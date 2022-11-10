use azure_core::{
    auth::TokenCredential,
    request_options::{LeaseId, Range},
};
use azure_identity::AzureCliCredential;
use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{
    BlobBlockType, BlobClient, BlobServiceClient, BlockList, ContainerClient,
};
use bytes::Bytes;
use futures::{future::try_join_all, StreamExt};
use reality::{
    wire::{ControlDevice, Data, Encoder, Frame, Interner, Protocol, ResourceId, WireObject},
    Keywords,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io::Cursor,
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tracing::{event, trace, Level};

/// Struct for uploading/fetching protocol data from azure storage,
///
pub struct Store {
    /// Protocol for stored wire objects,
    ///
    protocol: Protocol,
    /// Index of registered object names,
    ///
    index: HashMap<ResourceId, String>,
    /// Index of registered object names,
    ///
    reverse_index: BTreeMap<String, ResourceId>,
    /// Returns the etag for the last fetched store,
    ///
    last_fetched: Option<String>,
    /// Storage container client,
    ///
    container_client: Option<Arc<ContainerClient>>,
    /// Current lease id,
    ///
    lease_id: Option<LeaseId>,
}

impl Store {
    /// Returns a new empty store,
    ///
    pub fn empty() -> Self {
        Store::default()
    }

    /// Returns store with a container client,
    ///
    pub fn with_login(mut self, container_client: ContainerClient) -> Self {
        self.container_client = Some(Arc::new(container_client));
        self
    }

    /// Login to azure and return an authenticated store,
    ///
    pub async fn login_azcli(account_name: impl AsRef<str>, container: impl AsRef<str>) -> Self {
        let account_name = account_name.as_ref();
        let container_name = container.as_ref();
        trace!("Logging into store w/ account: {account_name}");

        let creds = AzureCliCredential::default()
            .get_token(format!("https://{account_name}.blob.core.windows.net/").as_str())
            .await
            .expect("should be able to get token");

        let client = StorageCredentials::bearer_token(creds.token.secret());
        let client = BlobServiceClient::new(account_name, client);
        let client = client.container_client(container_name);

        Self::empty().with_login(client)
    }

    /// Return objects from the store,
    ///
    pub fn objects<W>(&mut self) -> Vec<W>
    where
        W: WireObject,
    {
        {
            let object = self.encoder_mut::<W>().unwrap();
            object.frame_index = W::build_index(&object.interner, &object.frames);
        }
        self.protocol.decode::<W>()
    }

    /// Registers a wire object w/ the store,
    ///
    pub fn register<W>(&mut self, name: impl AsRef<str>)
    where
        W: WireObject,
    {
        self.index
            .insert(W::resource_id(), name.as_ref().to_string());
        self.reverse_index
            .insert(name.as_ref().to_string(), W::resource_id());
        self.protocol.ensure_encoder::<W>();
    }

    /// Returns an encoder for a wire object,
    ///
    pub fn encoder_mut<W>(&mut self) -> Option<&mut Encoder>
    where
        W: WireObject,
    {
        if self.index.contains_key(&W::resource_id()) {
            Some(self.protocol.ensure_encoder::<W>())
        } else {
            None
        }
    }

    pub async fn take(&mut self, prefix: impl AsRef<str>, timeout: Option<Duration>) -> bool {
        let container_client = self
            .container_client
            .as_ref()
            .clone()
            .expect("should be authenticated to commit the store");
        let prefix = format!("{}/store", prefix.as_ref());
        let blob_client = container_client.blob_client(&prefix);
        let blob_client = Arc::new(blob_client);

        let lease = if let Some(timeout) = timeout {
            blob_client.acquire_lease(timeout).await
        } else {
            blob_client.acquire_lease(Duration::from_secs(300)).await
        };

        match lease {
            Ok(lease) => {
                self.lease_id = Some(lease.lease_id);
            }
            Err(err) => {
                event!(Level::ERROR, "Could not acuire lease, {err}");
                return false;
            }
        }

        if self.fetch(prefix).await {
            let lease_id = self.lease_id.take().expect("should have a lease id");
            match blob_client.delete().lease_id(lease_id).await {
                Ok(_) => {}
                Err(err) => {
                    event!(Level::ERROR, "Could not delete blob, {err}");
                }
            }

            true
        } else {
            let lease_id = self.lease_id.take().expect("should have a lease id");
            match blob_client.blob_lease_client(lease_id).release().await {
                Ok(_) => {
                    
                },
                Err(err) => {
                    event!(Level::ERROR, "Could not release lease {err}");
                },
            }
            false
        }
    }

    /// Returns the latest version of the store,
    ///
    pub async fn fetch(&mut self, prefix: impl AsRef<str>) -> bool {
        let container_client = self
            .container_client
            .as_ref()
            .clone()
            .expect("should be authenticated to commit the store");
        let prefix = format!("{}/store", prefix.as_ref());
        let blob_client = container_client.blob_client(prefix);
        let blob_client = Arc::new(blob_client);

        if let Some(etag) = self.last_fetched.as_ref() {
            let current = Self::etag(blob_client.clone(), self.lease_id).await;
            if *etag == current {
                event!(Level::DEBUG, "Already up to date, skipping fetch");
                return false;
            }
        }

        let mut block_list = blob_client.get_block_list();
        if let Some(lease_id) = self.lease_id {
            block_list = block_list.lease_id(lease_id);
        }

        if let Some(block_list) = block_list.await.ok() {
            let mut interner = Interner::default();
            interner.add_ident("store");
            interner.add_ident("control");

            let mut encoder_map = HashMap::<ResourceId, Encoder>::default();
            for (id, _) in self.protocol.iter_encoders() {
                encoder_map.insert(id.clone(), Encoder::default());
            }

            // Build control device
            for block in block_list.block_with_size_list.blocks.iter() {
                match &block.block_list_type {
                    BlobBlockType::Committed(frame) => {
                        let frame = Frame::from(frame.as_ref());
                        match (frame.name(&interner), frame.symbol(&interner)) {
                            (Some(name), Some(symbol)) => match (name.as_str(), symbol.as_str()) {
                                ("store", "control") => {
                                    let mut reader = Self::pull_byte_range(
                                        blob_client.clone(),
                                        0..block.size_in_bytes,
                                        self.lease_id,
                                    );

                                    let mut control_device = ControlDevice::default();

                                    let mut buffer = [0; 64];
                                    while let Ok(r) = reader.read_exact(&mut buffer).await {
                                        assert_eq!(r, 64);
                                        let frame = Frame::from(buffer.as_ref());
                                        if frame.op() == 0x00 {
                                            control_device.data.push(frame.clone());
                                        } else if frame.op() > 0x00 && frame.op() < 0x06 {
                                            control_device.read.push(frame.clone());
                                        } else if frame.op() >= 0xC1 && frame.op() <= 0xC6 {
                                            assert!(
                                                frame.op() >= 0xC1 && frame.op() <= 0xC6,
                                                "Index frames have a specific op code range"
                                            );
                                            control_device.index.push(frame.clone());
                                        }
                                        buffer = [0; 64];
                                    }

                                    interner = interner.merge(&control_device.into());
                                }
                                _ => {
                                    // All control device blocks are in the front
                                    break;
                                }
                            },
                            _ => {
                                break;
                            }
                        }
                    }
                    _ => {
                        break;
                    }
                }
            }

            // Build objects
            let mut offset = 0;
            let mut current_encoder = None::<ResourceId>;
            for block in block_list.block_with_size_list.blocks.iter() {
                match &block.block_list_type {
                    BlobBlockType::Committed(frame) => {
                        let frame = Frame::from(frame.as_ref());
                        match (frame.name(&interner), frame.symbol(&interner)) {
                            (Some(name), Some(symbol))
                                if frame.keyword() == Keywords::Extension =>
                            {
                                match (name.as_ref(), symbol.as_ref()) {
                                    ("store", "control") => {}
                                    ("store", symbol) => {
                                        if let Some((id, Some(encoder))) =
                                            self.reverse_index.get(symbol).and_then(|id| {
                                                Some((
                                                    id,
                                                    self.protocol.encoder_mut_by_id(id.clone()),
                                                ))
                                            })
                                        {
                                            let mut reader = Self::pull_byte_range(
                                                blob_client.clone(),
                                                offset..offset + block.size_in_bytes,
                                                self.lease_id,
                                            );

                                            let mut buffer = [0; 64];
                                            while let Ok(r) = reader.read_exact(&mut buffer).await {
                                                assert_eq!(r, 64);
                                                let frame = Frame::from(buffer);
                                                encoder.frames.push(frame);
                                                buffer = [0; 64];
                                            }

                                            encoder.interner = interner.clone();

                                            current_encoder = Some(id.clone());
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            _ if frame.is_extent() => {
                                if let Some(encoder) = current_encoder
                                    .as_ref()
                                    .and_then(|id| self.protocol.encoder_mut_by_id(id.clone()))
                                {
                                    let mut reader = Self::pull_byte_range(
                                        blob_client.clone(),
                                        offset..offset + block.size_in_bytes,
                                        self.lease_id,
                                    );

                                    match tokio::io::copy(&mut reader, &mut encoder.blob_device)
                                        .await
                                    {
                                        Ok(copied) => {
                                            assert_eq!(copied, block.size_in_bytes);
                                            event!(Level::TRACE, "Copied, {copied}");
                                        }
                                        Err(err) => {
                                            event!(
                                                Level::ERROR,
                                                "Could not copy bytes into blob device, {err}"
                                            );
                                        }
                                    };
                                }
                            }
                            _ => panic!("Unrecognized frame"),
                        }
                    }
                    _ => {}
                }
                // Keep track of offset, so that blob reads can do range queries
                offset += block.size_in_bytes;
            }

            self.last_fetched = Some(Self::etag(blob_client, self.lease_id).await);
            true
        } else {
            false
        }
    }

    /// Uploading store,
    ///
    /// # Wire object layout
    ///
    /// ## Storing control device
    /// - All wire objects share the same control device/interner
    /// 0x0E    store   control
    ///
    /// - If the block storing the control device exceeds limits, an additional block is appended
    ///
    /// 0x0E    store   control
    ///
    /// - Since entropy is enabled on extension frames, the same namespace/symbol identifier can be used
    ///
    /// ## Wire objects
    ///
    /// - Frames for each wire object type are stored in their own block
    /// - Any extent frames in the stored wire object are replicated and added after the object block
    ///
    /// <Block with frames>         0x0E        store           <object_name>
    /// <If there are any extents>  0x0A/0x0D   <extents get their own block>
    ///
    ///
    pub async fn upload(&self, prefix: impl AsRef<str>) {
        let container_client = self
            .container_client
            .as_ref()
            .clone()
            .expect("should be authenticated to commit the store");

        let blob_client = container_client.blob_client(format!("{}/store", prefix.as_ref()));

        let mut interner = Interner::default();

        let mut objects = vec![];
        let mut upload_block_futures = vec![];

        for (resource_id, encoder) in self
            .protocol
            .iter_encoders()
            .filter(|(_, e)| !e.frames.is_empty())
        {
            if let Some(name) = self.index.get(resource_id) {
                /*
                  ## Wire objects
                   - Frames for each wire object type are stored in their own block
                   - Any extent frames in the stored wire object are replicated and added after the object block
                   <Block with frames>         0x0E        store           <object_name>
                */
                let encoder_frame = Frame::extension("store", name);

                let mut block_list = VecDeque::default();
                let mut buffer = Cursor::new(vec![]);

                interner.add_ident(name);
                interner = interner.merge(&encoder.interner);

                for frame in encoder.frames.iter() {
                    if frame.is_extent() {
                        if let Data::Extent {
                            length,
                            cursor: Some(cursor),
                        } = frame.value().expect("should be an extent")
                        {
                            /*
                            extents get their own block and are added after the object's store frame in the block list
                            */
                            let start = cursor as usize;
                            let end = start + length as usize;
                            let block_id = Bytes::copy_from_slice(frame.bytes());

                            let mut task = blob_client.put_block(
                                block_id.clone(),
                                Bytes::copy_from_slice(&encoder.blob_device.get_ref()[start..end]),
                            );
                            if let Some(lease_id) = self.lease_id.as_ref() {
                                task = task.lease_id(*lease_id);
                            }
                            upload_block_futures.push(task.into_future());
                            block_list.push_back(BlobBlockType::new_uncommitted(block_id));
                        }
                    }

                    match std::io::Write::write_all(&mut buffer, frame.bytes()) {
                        Ok(_) => {}
                        Err(err) => {
                            event!(Level::ERROR, "Could not write to buffer, {err}");
                        }
                    }
                }

                // Prepend the object's store frame to the block list and upload it's frames
                let block_id = Bytes::copy_from_slice(encoder_frame.bytes());
                let mut upload =
                    blob_client.put_block(block_id.clone(), Bytes::from(buffer.into_inner()));
                if let Some(lease_id) = self.lease_id.as_ref() {
                    upload = upload.lease_id(*lease_id);
                }
                upload_block_futures.push(upload.into_future());
                block_list.push_front(BlobBlockType::new_uncommitted(block_id));

                objects.push(block_list.make_contiguous().to_vec());
            }
        }

        // Handle control_device
        let control_device = ControlDevice::new(interner);
        let control_frame = Frame::extension("store", "control");
        let mut buffer = Cursor::new(vec![]);
        for d in control_device.data {
            std::io::Write::write_all(&mut buffer, d.bytes()).expect("should be able to write");
        }
        for d in control_device.read {
            std::io::Write::write_all(&mut buffer, d.bytes()).expect("should be able to write");
        }
        for d in control_device.index {
            std::io::Write::write_all(&mut buffer, d.bytes()).expect("should be able to write");
        }
        let control_block_id = Bytes::copy_from_slice(control_frame.bytes());
        let mut upload = blob_client.put_block(control_block_id.clone(), buffer.into_inner());
        if let Some(lease_id) = self.lease_id {
            upload = upload.lease_id(lease_id)
        }
        upload_block_futures.push(upload.into_future());

        // Finish upload blobs
        match try_join_all(upload_block_futures).await {
            Ok(_) => {}
            Err(err) => {
                event!(Level::ERROR, "Could not upload store objects, {err}");
            }
        }

        let mut block_list = BlockList {
            blocks: objects.concat(),
        };
        block_list
            .blocks
            .insert(0, BlobBlockType::new_uncommitted(control_block_id));

        let mut request = blob_client.put_block_list(block_list);
        if let Some(lease_id) = self.lease_id {
            request = request.lease_id(lease_id)
        }
        match request.await {
            Ok(_) => {}
            Err(err) => {
                event!(Level::ERROR, "Could not put block list, {err}");
            }
        }
    }

    /// Commits the store blob,
    ///
    pub async fn commit(&self, prefix: impl AsRef<str>) {
        let container_client = self
            .container_client
            .as_ref()
            .clone()
            .expect("should be authenticated to commit the store");

        let blob_client = container_client.blob_client(format!("{}/store", prefix.as_ref()));

        let mut request = blob_client.snapshot();
        if let Some(lease_id) = self.lease_id {
            request = request.lease_id(lease_id)
        }

        match request.await {
            Ok(_) => {}
            Err(err) => {
                event!(Level::ERROR, "Could not take commit store, {err}");
            }
        }
    }
}

/// Functions to work w/ blobs,
///
impl Store {
    /// Returns current etag,
    ///
    async fn etag(blob_client: Arc<BlobClient>, lease_id: Option<LeaseId>) -> String {
        let mut current = blob_client.get_metadata();
        if let Some(lease_id) = lease_id.as_ref() {
            current = current.lease_id(*lease_id);
        }
        current.await.expect("should have metadata").etag
    }

    /// Returns the other end of a duplex stream to read bytes from,
    ///
    fn pull_byte_range(
        blob_client: Arc<BlobClient>,
        range: impl Into<Range>,
        lease_id: Option<LeaseId>,
    ) -> DuplexStream {
        let range = range.into();

        let size = range.end - range.start;

        let (mut writer, reader) = tokio::io::duplex(size as usize);

        tokio::spawn(async move {
            let blob_client = blob_client;
            let mut request = blob_client.get();
            if let Some(lease_id) = lease_id.as_ref() {
                request = request.lease_id(*lease_id);
            }

            let mut stream = request.range(range).into_stream();
            while let Some(resp) = stream.next().await {
                match resp {
                    Ok(r) => {
                        let reader = r.data.collect().await.expect("should be able to read");
                        match writer.write_all(reader.as_ref()).await {
                            Ok(_) => {}
                            Err(err) => {
                                event!(Level::ERROR, "Could not write bytes {err}");
                            }
                        }
                    }
                    Err(err) => {
                        event!(Level::ERROR, "Error reading range, {err}")
                    }
                }
            }
        });

        reader
    }
}

impl Default for Store {
    fn default() -> Self {
        Self {
            protocol: Protocol::empty(),
            index: Default::default(),
            reverse_index: Default::default(),
            last_fetched: None,
            container_client: None,
            lease_id: None,
        }
    }
}
