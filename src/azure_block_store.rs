use azure_storage_blobs::prelude::{BlobBlockType, BlockList};
use bytes::{BufMut, Bytes, BytesMut};
use reality::wire::{
    block_tasks::{FinishStore, PutBlock},
    BlockBuilder, BlockClient, BlockEntry, BlockStore, BlockStoreBuilder, ControlDevice, Encoder,
    Frame, Interner,
};
use std::{collections::HashMap, io::ErrorKind, time::Duration};
use tokio::task::JoinSet;
use tracing::{event, Level};

use crate::{AzureBlockClient, Store};

/// Converts the interner into a control device, uploads, and returns the frame representing the control device
///
pub async fn put_control_device(block_client: &AzureBlockClient, interner: &Interner) -> Frame {
    let control_device = ControlDevice::new(interner.clone());

    let mut buffer = BytesMut::with_capacity(control_device.size());
    for d in control_device.data {
        buffer.put(d.bytes());
    }

    for r in control_device.read {
        buffer.put(r.bytes());
    }

    for i in control_device.index {
        buffer.put(i.bytes());
    }

    let buffer = buffer.freeze();
    let control_frame = Frame::extension("store", "control");

    match block_client.put_block(control_frame.bytes(), reality::store::Blob::Binary(buffer).compress().await).await {
        Ok(resp) => {
            event!(Level::TRACE, "{:?}", resp);
        }
        Err(err) => {
            event!(Level::ERROR, "Could not put control device, {err}");
        }
    }

    control_frame
}

/// Struct for a block store in Azure Storage
///
pub struct AzureBlockStore {
    /// Blob client to store data,
    ///
    block_client: AzureBlockClient,
    /// Main interner,
    ///
    interner: Interner,
    /// Map of block builders,
    ///
    builders: Option<HashMap<u64, AzureBlockBuilder>>,
}

pub struct AzureBlockBuilder {
    /// Name of the object being streamed,
    ///
    name: String,
    /// Frame data that will be uploaded for this block,
    ///
    frame_data: BytesMut,
    /// Block client,
    ///
    block_client: AzureBlockClient,
    /// Block list to upload,
    ///
    block_list: Vec<Frame>,
    /// All block uploads,
    ///
    uploads: JoinSet<Duration>,
}

impl AzureBlockStore {
    /// Returns a new azure block store,
    ///
    pub fn new(client: AzureBlockClient) -> Self {
        let mut interner = Interner::default();
        interner.add_ident("store");
        interner.add_ident("control");
        interner.add_ident("");

        Self {
            block_client: client.clone(),
            interner,
            builders: None,
        }
    }

    pub async fn load_interner(&mut self) {
        if let Some(client) = self.client() {
            match client.list_blocks().await.expect("should be able to join") {
                Ok(resp) => {
                    if let Some(entry) = resp.iter().find(|e| {
                        e.frame().name(&self.interner) == Some(String::from("store"))
                            && e.frame().symbol(&self.interner) == Some(String::from("control"))
                    }) {
                        let control_device = client.pull_byte_range(0..entry.size());
                        let interner = Store::load_interner(control_device).await;
                        self.interner = self.interner.merge(&interner);
                    }
                }
                Err(err) => {
                    event!(Level::ERROR, "Could not list blocks, {err}");
                },
            }
        }
    }
}

impl AzureBlockBuilder {
    /// Returns a new block builder
    ///
    fn new(block_client: AzureBlockClient, name: impl AsRef<str>) -> Self {
        Self {
            name: name.as_ref().to_string(),
            frame_data: BytesMut::new(),
            block_client,
            block_list: vec![],
            uploads: JoinSet::new(),
        }
    }

    /// Waits for all uploads to complete,
    ///
    async fn wait_for_uploads(&mut self) {
        while let Some(Ok(elapsed)) = self.uploads.join_next().await {
            event!(Level::TRACE, "Uploaded block, elapsed: {:?}", elapsed);
        }
    }
}

impl BlockBuilder for AzureBlockBuilder {
    fn name(&self) -> &String {
        &self.name
    }

    fn put_frame(&mut self, frame: &Frame) {
        self.frame_data.put(frame.bytes());
    }

    fn put_block(&mut self, frame: &Frame, blob: impl Into<bytes::Bytes>) -> PutBlock {
        self.put_frame(frame);

        self.block_list.push(frame.clone());

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        self.uploads.spawn(async {
            let started = tokio::time::Instant::now();

            match rx.await {
                Ok(_) => {}
                Err(err) => {
                    event!(Level::ERROR, "error waiting for signal, {err}");
                }
            }

            started.elapsed()
        });

        let block_id = frame.bytes();
        let blob: Bytes = blob.into().clone();
        let block_client = self.block_client.clone();
        tokio::spawn(async move {
            match block_client.put_block(block_id, blob).await {
                Ok(resp) => {
                    event!(Level::TRACE, "Put blob, {:?}", resp);
                    tx.send(()).expect("should be able to complete task");
                    Ok(())
                }
                Err(err) => {
                    event!(Level::ERROR, "Could not put block, {err}");
                    Err(std::io::Error::new(ErrorKind::Other, err))
                }
            }
        })
    }

    fn frame_block_data(&self) -> bytes::Bytes {
        self.frame_data.clone().freeze()
    }

    fn ordered_block_list(&self) -> Vec<Frame> {
        let mut encoder = Encoder::new();
        {
            let mut ext = encoder.start_extension("store", self.name());
            for f in self.block_list.iter().cloned() {
                ext.as_mut().frames.push(f);
            }
        }

        encoder.frames
    }
}

impl BlockStoreBuilder for AzureBlockStore {
    type Store = AzureBlockStore;

    type Builder = AzureBlockBuilder;

    fn include_interner(&mut self, interner: &reality::wire::Interner) {
        self.interner = self.interner.merge(interner);
    }

    fn build_block(&mut self, name: impl AsRef<str>) -> &mut Self::Builder {
        let key = self.interner.add_ident(name.as_ref());

        if let Some(builders) = self.builders.as_mut() {
            if !builders.contains_key(&key) {
                builders.insert(
                    key,
                    AzureBlockBuilder::new(self.block_client.clone(), name.as_ref()),
                );
            }

            builders.get_mut(&key).expect("should exist")
        } else {
            panic!("Store is not in build mode")
        }
    }

    fn finish(
        &mut self,
        mut block_order: Option<Vec<impl AsRef<str>>>,
    ) -> FinishStore<Self::Store> {
        let mut builders = self.builders.take().expect("should be in progress");
        let block_client = self.block_client.clone();
        let mut interner = self.interner.clone();
        interner.add_ident("store");
        interner.add_ident("control");

        let mut block_order = {
            if let Some(block_order) = block_order.take() {
                Some(
                    block_order
                        .iter()
                        .map(|s| s.as_ref().to_string())
                        .collect::<Vec<_>>(),
                )
            } else {
                None
            }
        };

        tokio::spawn(async move {
            let mut block_list = vec![];

            if let Some(block_order) = block_order.take() {
                for name in block_order {
                    if let Some(mut builder) = builders.remove(&interner.add_ident(name)) {
                        builder.wait_for_uploads().await;

                        let builder_block_list = builder.ordered_block_list();

                        let ext = builder_block_list.get(0).expect("should have an ext frame");

                        block_client
                            .put_block(ext.bytes(), builder.frame_data.freeze())
                            .await
                            .expect("should be able to put block");

                        block_list.push(
                            builder_block_list
                                .iter()
                                .map(|f| BlobBlockType::new_uncommitted(f.bytes()))
                                .collect::<Vec<_>>(),
                        );
                    }
                }
            }

            for (_, mut builder) in builders.drain() {
                builder.wait_for_uploads().await;

                let builder_block_list = builder.ordered_block_list();

                let ext = builder_block_list.get(0).expect("should have an ext frame");

                block_client
                    .put_block(ext.bytes(), builder.frame_data.freeze())
                    .await
                    .expect("should be able to put block");

                block_list.push(
                    builder_block_list
                        .iter()
                        .map(|f| BlobBlockType::new_uncommitted(f.bytes()))
                        .collect::<Vec<_>>(),
                );
            }

            let mut blocks = block_list.concat();

            let control_device = put_control_device(&block_client, &interner).await;

            blocks.insert(0, BlobBlockType::new_uncommitted(control_device.bytes()));

            match block_client.put_block_list(BlockList { blocks }).await {
                Ok(_) => {
                    // let block_client = block_client.snapshot().await;
                    Ok(AzureBlockStore {
                        block_client,
                        interner,
                        builders: None,
                    })
                }
                Err(err) => {
                    event!(
                        Level::ERROR,
                        "Could not put block list, {err}, {:?}",
                        err.kind()
                    );
                    Err(std::io::Error::new(std::io::ErrorKind::Other, err))
                }
            }
        })
    }
}

impl BlockStore for AzureBlockStore {
    type Client = AzureBlockClient;

    type Builder = AzureBlockStore;

    fn client(&self) -> Option<Self::Client> {
        if self.block_client.exists() {
            Some(self.block_client.clone())
        } else {
            None
        }
    }

    fn builder(&self) -> Option<Self::Builder> {
        if self.block_client.snapshot_id.is_none() {
            Some(AzureBlockStore {
                block_client: self.block_client.clone(),
                interner: self.interner.clone(),
                builders: Some(HashMap::default()),
            })
        } else {
            // If there is a snapshot id, then it is readonly
            None
        }
    }

    fn interner(&self) -> &reality::wire::Interner {
        &self.interner
    }
}
