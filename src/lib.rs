use azure_core::{
    auth::TokenCredential,
    request_options::{LeaseId, Range},
};
use azure_identity::AzureCliCredential;
use azure_storage::StorageCredentials;
use azure_storage_blobs::{
    blob::{BlockWithSizeList, operations::PutBlock},
    prelude::{BlobBlockType, BlobClient, BlobServiceClient, BlockList, ContainerClient, Snapshot},
};
use bytes::Bytes;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use futures::{future::try_join_all, StreamExt};
use reality::{
    wire::{ControlDevice, Data, Frame, Interner, Protocol, ResourceId},
    Keywords,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io::{Cursor, Read, Seek, Write},
    sync::Arc,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tracing::{event, trace, Level};

pub use reality::wire::Decoder;
pub use reality::wire::Encoder;
pub use reality::wire::WireObject;

mod index;
pub use index::StoreIndex;

mod filesystem;
pub use filesystem::Filesystem;

mod streamer;
pub use streamer::Blob;
pub use streamer::Streamer;

mod store_stream;
pub use store_stream::StoreStream;

/// Struct for uploading/fetching protocol data from azure storage,
///
pub struct Store<BlobImpl = Cursor<Vec<u8>>>
where
    BlobImpl: Read + Write + Seek + Clone + Default,
{
    /// Protocol for stored wire objects,
    ///
    protocol: Protocol<BlobImpl>,
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
    /// Current snapshot,
    ///
    snapshot: Option<Snapshot>,
    /// Store name, defaults to "store"
    ///
    store_name: &'static str,
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
    /// Ensures the wire object has an encoder,
    ///
    pub fn register<W>(&mut self, name: impl AsRef<str>)
    where
        W: WireObject,
    {
        self.register_with(W::resource_id(), name);
        self.protocol.ensure_encoder::<W>();
    }

    /// Registers a resource id w/ the store,
    ///
    pub fn register_with(&mut self, resource_id: ResourceId, name: impl AsRef<str>) {
        self.index
            .insert(resource_id.clone(), name.as_ref().to_string());
        self.reverse_index
            .insert(name.as_ref().to_string(), resource_id.clone());

        self.set_encoder(resource_id.clone(), Encoder::default());
    }

    /// Sets the encoder for resource id, the id must be registered,
    ///
    pub fn set_encoder(&mut self, resource_id: ResourceId, encoder: Encoder) {
        if self.index.contains_key(&resource_id) {
            self.protocol.set_encoder(resource_id, encoder);
        }
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

    /// Takes an encoder and replaces with an empty encoder,
    ///
    pub fn take_encoder<W>(&mut self) -> Option<Encoder>
    where
        W: WireObject,
    {
        if let Some(mut encoder) = self.protocol.take_encoder(W::resource_id()) {
            self.protocol.ensure_encoder::<W>();

            encoder.frame_index = W::build_index(&encoder.interner, &encoder.frames);

            Some(encoder)
        } else {
            None
        }
    }

    /// Takes an encoder by id,
    ///
    pub fn take_encoder_by_id(&mut self, resource_id: ResourceId) -> Option<Encoder> {
        self.protocol.take_encoder(resource_id)
    }

    /// Takes the uploaded store blob data if successfully fetched,
    ///
    pub async fn take(&mut self, prefix: impl AsRef<str>, timeout: Option<Duration>) -> bool {
        let container_client = self
            .container_client
            .as_ref()
            .clone()
            .expect("should be authenticated to commit the store");
        let blob_client =
            container_client.blob_client(&format!("{}/{}", prefix.as_ref(), self.store_name));
        let blob_client = Arc::new(blob_client);

        match blob_client.exists().await {
            Ok(exists) => {
                if !exists {
                    return false;
                }
            }
            Err(err) => {
                event!(Level::ERROR, "Could not check if blob exists, {err}");
                return false;
            }
        }

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
            match blob_client.blob_lease_client(lease_id).release().await {
                Ok(_) => {}
                Err(err) => {
                    event!(Level::ERROR, "Could not release lease {err}");
                }
            }

            match blob_client.delete().await {
                Ok(_) => {}
                Err(err) => {
                    event!(Level::ERROR, "Could not delete blob {err}");
                }
            }

            true
        } else {
            let lease_id = self.lease_id.take().expect("should have a lease id");
            match blob_client.blob_lease_client(lease_id).release().await {
                Ok(_) => {}
                Err(err) => {
                    event!(Level::ERROR, "Could not release lease {err}");
                }
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
        let prefix = format!("{}/{}", prefix.as_ref(), self.store_name);
        let blob_client = container_client.blob_client(prefix);
        let blob_client = Arc::new(blob_client);

        if let Some(etag) = self.last_fetched.as_ref() {
            match self.etag(blob_client.clone()).await {
                Some(current) => {
                    if etag == &current {
                        event!(Level::DEBUG, "Already up to date, skipping fetch");
                        return false;
                    }
                }
                None => {}
            }
        }

        if let Some(block_list) = self.block_list(blob_client.clone()).await {
            let mut interner = Interner::default();
            interner.add_ident("store");
            interner.add_ident("control");

            let mut encoder_map = HashMap::<ResourceId, Encoder>::default();
            for (id, _) in self.protocol.iter_encoders() {
                encoder_map.insert(id.clone(), Encoder::default());
            }

            // Build control device
            for block in block_list.blocks.iter() {
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
                                        self.snapshot.clone(),
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
            for block in block_list.blocks.iter() {
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
                                        current_encoder.take();
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
                                                self.snapshot.clone(),
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
                                if let Some(id) = current_encoder.as_ref() {
                                    if let Some(encoder) =
                                        self.protocol.encoder_mut_by_id(id.clone())
                                    {
                                        let mut reader = Self::pull_byte_range(
                                            blob_client.clone(),
                                            offset..offset + block.size_in_bytes,
                                            self.lease_id,
                                            self.snapshot.clone(),
                                        );

                                        match reader
                                            .read_to_end(&mut encoder.blob_device.get_mut())
                                            .await
                                        {
                                            Ok(read) => {
                                                if let Some(Data::Extent { length, .. }) =
                                                    frame.value()
                                                {
                                                    assert_eq!(length as usize, read);
                                                    event!(Level::TRACE, "Read blob, {length}");
                                                }
                                            }
                                            Err(err) => {
                                                event!(Level::ERROR, "Could not read blob, {err}");
                                            }
                                        }
                                    }
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

            self.last_fetched = self.etag(blob_client).await;
            true
        } else {
            false
        }
    }

    /// Returns a blob client for a blob at prefix/{store_name} within the container,
    /// 
    pub fn get_blob_client(&self, prefix: impl AsRef<str>) -> BlobClient {
        let container_client = self
            .container_client
            .as_ref()
            .clone()
            .expect("should be authenticated to commit the store");

        container_client.blob_client(format!("{}/{}", prefix.as_ref(), self.store_name))
    }

    /// Uploading store, returns true if upload completed
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
    pub async fn upload(&self, prefix: impl AsRef<str>) -> bool {
        let container_client = self
            .container_client
            .as_ref()
            .clone()
            .expect("should be authenticated to commit the store");

        let blob_client =
            container_client.blob_client(format!("{}/{}", prefix.as_ref(), self.store_name));

        let blob_client = Arc::new(blob_client);

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
                let mut extension_encoder = Encoder::new();
                let mut extension = extension_encoder.start_extension("store", name);

                let mut block_list = VecDeque::default();
                let mut buffer = GzEncoder::new(vec![], Compression::fast());

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

                            let mut gz_encoder = GzEncoder::new(vec![], Compression::fast());

                            match gz_encoder.write_all(&encoder.blob_device.get_ref()[start..end]) {
                                Ok(_) => {}
                                Err(err) => {
                                    event!(Level::ERROR, "Error wrting to gz encoder, {err}");
                                }
                            }

                            let mut task = blob_client.put_block(
                                block_id.clone(),
                                Bytes::from(
                                    gz_encoder.finish().expect("should be able to compress"),
                                ),
                            );
                            if let Some(lease_id) = self.lease_id.as_ref() {
                                task = task.lease_id(*lease_id);
                            }
                            upload_block_futures.push(task.into_future());
                            block_list.push_back(BlobBlockType::new_uncommitted(block_id));

                            extension.as_mut().frames.push(frame.clone());
                        }
                    }

                    match std::io::Write::write_all(&mut buffer, frame.bytes()) {
                        Ok(_) => {}
                        Err(err) => {
                            event!(Level::ERROR, "Could not write to buffer, {err}");
                        }
                    }
                }

                let pos = extension.insert_at();
                drop(extension);

                let encoder_frame = &extension_encoder.frames[pos];

                // Prepend the object's store frame to the block list and upload it's frames
                let block_id = Bytes::copy_from_slice(encoder_frame.bytes());
                let mut upload = blob_client.put_block(
                    block_id.clone(),
                    Bytes::from(buffer.finish().expect("should be able to complete")),
                );
                if let Some(lease_id) = self.lease_id.as_ref() {
                    upload = upload.lease_id(*lease_id);
                }
                upload_block_futures.push(upload.into_future());
                block_list.push_front(BlobBlockType::new_uncommitted(block_id));

                objects.push(block_list.make_contiguous().to_vec());
            }
        }

        // Handle control_device
        
        let (upload, control_block) = self.put_control_device(interner, blob_client.clone()).await;

        upload_block_futures.push(upload);

        // Finish upload blobs
        match try_join_all(upload_block_futures).await {
            Ok(_) => {}
            Err(err) => {
                event!(Level::ERROR, "Could not upload store objects, {err}");
                return false;
            }
        }

        let mut block_list = BlockList {
            blocks: objects.concat(),
        };
        block_list
            .blocks
            .insert(0, control_block);

        let mut request = blob_client.put_block_list(block_list);
        if let Some(lease_id) = self.lease_id {
            request = request.lease_id(lease_id)
        }
        match request.await {
            Ok(_) => true,
            Err(err) => {
                event!(Level::ERROR, "Could not put block list, {err}");
                false
            }
        }
    }

    /// Start a stream upload of the store,
    /// 
    /// Returns a StoreStream which can be configured by calling .start(), and passing in a select fn,
    ///
    pub fn start_stream(&mut self, prefix: impl AsRef<str>) -> StoreStream {
        StoreStream::new(self, prefix)
    }

    /// Commits the store blob to a snapshot and returns the snapshot,
    ///
    /// Snapshots are only meant for the store instance that took the snapshot, if a snapshot exists when
    /// this function is called again, it will be removed before creating a new snapshot.
    ///
    /// If the snapshot cannot be removed, this function will panic to prevent this client from continuing to create snapshots.
    ///
    pub async fn commit(&mut self, prefix: impl AsRef<str>) -> bool {
        let container_client = self
            .container_client
            .as_ref()
            .clone()
            .expect("should be authenticated to commit the store");

        let blob_client =
            container_client.blob_client(format!("{}/{}", prefix.as_ref(), self.store_name));
        let blob_client = Arc::new(blob_client);

        let etag = self.etag(blob_client.clone()).await;

        if let Some(snapshot) = self.snapshot.take() {
            let current = self.etag(blob_client.clone()).await;

            if current == etag {
                // Skip, creating a snapshot if the data would be the same
                event!(
                    Level::TRACE,
                    "Skip creating a snapshot if the etag is still the same"
                );
                self.snapshot = Some(snapshot);
                return false;
            }

            match blob_client.delete_snapshot(snapshot.clone()).await {
                Ok(resp) => {
                    event!(
                        Level::TRACE,
                        "Deleted previous snapshot, {:?}, request_id: {}",
                        snapshot,
                        resp.request_id
                    );
                }
                Err(err) => {
                    event!(
                        Level::ERROR,
                        "Could not release previous snapshot, will panic to prevent leaks. {err}"
                    );
                    panic!("Could not delete previous snapshot, {err}");
                }
            }
        }

        let mut request = blob_client.snapshot();
        if let Some(lease_id) = self.lease_id {
            request = request.lease_id(lease_id)
        }

        match request.await {
            Ok(resp) => {
                self.snapshot = Some(resp.snapshot);
                true
            }
            Err(err) => {
                event!(Level::ERROR, "Could not take commit store, {err}");
                false
            }
        }
    }

    /// Returns a store index,
    ///
    pub async fn index(&self, prefix: impl AsRef<str>) -> Option<StoreIndex> {
        let container_client = self
            .container_client
            .as_ref()
            .clone()
            .expect("should be authenticated to commit the store");

        let blob_client =
            container_client.blob_client(format!("{}/{}", prefix.as_ref(), self.store_name));
        let blob_client = Arc::new(blob_client);

        if let Some(block_list) = self.block_list(blob_client.clone()).await {
            let mut index = StoreIndex::empty(blob_client, self.lease_id, self.snapshot.clone());

            index.index(block_list).await;

            Some(index)
        } else {
            None
        }
    }
}

impl Store {
    /// Registers a filesystem by name,
    /// 
    pub fn register_filesystem(&mut self, name: impl AsRef<str>) -> ResourceId {
        let resource_id = reality::wire::ResourceId::new_with_dynamic_id::<Filesystem>(
           Interner::default().add_ident(name.as_ref()),
        );

        self.register_with(resource_id.clone(), name);

        resource_id
    }

    /// Returns an interner after parsing control device frames,
    ///
    pub async fn load_interner(
        mut reader: impl AsyncReadExt + tokio::io::AsyncRead + Unpin,
    ) -> Interner {
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

        control_device.into()
    }
}

/// Functions to work w/ blobs,
///
impl Store {
    /// Upload a control device block, returns the request future and block type,
    /// 
    pub async fn put_control_device(&self, interner: Interner, blob_client: Arc<BlobClient>) -> (PutBlock, BlobBlockType)  {
        // Handle control_device
        let control_device = ControlDevice::new(interner);
        let control_frame = Frame::extension("store", "control");
        let mut buffer = GzEncoder::new(vec![], Compression::fast());
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
        let mut upload = blob_client.put_block(
            control_block_id.clone(),
            buffer.finish().expect("should be able to compress"),
        );
        if let Some(lease_id) = self.lease_id {
            upload = upload.lease_id(lease_id)
        }

        (upload.into_future(), BlobBlockType::new_uncommitted(control_block_id))
    }

    /// Returns the current block list,
    ///
    async fn block_list(&self, blob_client: Arc<BlobClient>) -> Option<BlockWithSizeList> {
        let mut block_list = blob_client.get_block_list();
        if let Some(lease_id) = self.lease_id {
            block_list = block_list.lease_id(lease_id);
        }

        if let Some(snapshot) = self.snapshot.as_ref() {
            block_list = block_list.blob_versioning(snapshot.clone());
        }

        match block_list.await {
            Ok(resp) => Some(resp.block_with_size_list),
            Err(err) => {
                event!(Level::ERROR, "Could not get block list {}", err);
                None
            }
        }
    }

    /// Returns current etag,
    ///
    async fn etag(&self, blob_client: Arc<BlobClient>) -> Option<String> {
        let mut current = blob_client.get_metadata();

        if let Some(lease_id) = self.lease_id {
            current = current.lease_id(lease_id);
        }

        if let Some(snapshot) = self.snapshot.as_ref() {
            current = current.blob_versioning(snapshot.clone());
        }

        match current.await {
            Ok(resp) => Some(resp.etag),
            Err(err) => {
                event!(Level::ERROR, "Could not get etag, {err}");
                None
            }
        }
    }

    /// Returns the other end of a duplex stream to read bytes from,
    ///
    pub fn pull_byte_range(
        blob_client: Arc<BlobClient>,
        range: impl Into<Range>,
        lease_id: Option<LeaseId>,
        snapshot: Option<Snapshot>,
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

            if let Some(snapshot) = snapshot.as_ref() {
                request = request.blob_versioning(snapshot.clone());
            }

            let mut stream = request.range(range).into_stream();
            while let Some(resp) = stream.next().await {
                match resp {
                    Ok(r) => {
                        let reader = r.data.collect().await.expect("should be able to read");
                        let mut reader = GzDecoder::new(reader.as_ref());
                        let mut buffer = vec![];

                        match reader.read_to_end(&mut buffer) {
                            Ok(read) => {
                                event!(Level::TRACE, "Decoded {read} bytes");
                            }
                            Err(err) => {
                                event!(Level::ERROR, "Could not decode bytes, {err}");
                            }
                        }

                        match writer.write_all(buffer.as_slice()).await {
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
            snapshot: None,
            store_name: "store",
        }
    }
}
