use std::{sync::Arc, ops::Range, io::ErrorKind};
use tokio_stream::StreamExt;
use azure_core::{request_options::LeaseId, Body};
use azure_storage_blobs::{prelude::{BlobClient, Snapshot, BlockId, BlockList}, blob::{BlobBlockWithSize, operations::{PutBlockBuilder, PutBlockListBuilder}}};
use reality::wire::{BlockEntry, Frame, block_tasks::ListBlocks};
use tokio::io::DuplexStream;
use tracing::{event, Level};

/// Wrapper struct over an azure blob client,
///
#[derive(Clone)]
pub struct AzureBlockClient {
    /// Client
    ///  
    pub client: Arc<BlobClient>,
    /// Snapshot id,
    ///
    pub snapshot_id: Option<Snapshot>,
    /// Lease id
    ///
    pub lease_id: Option<LeaseId>,
    /// True if the underlying blob exists,
    /// 
    /// If set to false, then the value will be checked on .exists()
    /// 
    pub exists: bool,
}

/// Wrapper struct to implement block entry for BlobBlockWithSize
/// 
pub struct AzureBlockEntry(BlobBlockWithSize);

impl BlockEntry for AzureBlockEntry {
    fn frame(&self) -> reality::wire::Frame {
        match &self.0.block_list_type {
            azure_storage_blobs::prelude::BlobBlockType::Committed(block_id) => {
                Frame::from(block_id.bytes())
            },
            _ => {
                // This can mean that the wrong list block operation was used to creat this entry
                panic!("Invalid blob block type")
            }
        }
    }

    fn size(&self) -> usize {
        self.0.size_in_bytes as usize
    }
}

impl AzureBlockClient {
    /// Returns the other end of a duplex stream to read bytes from,
    ///
    pub fn pull_byte_range(
        &self,
        range: impl Into<Range<usize>>,
    ) -> DuplexStream {
        let Self {
            client,
            snapshot_id,
            lease_id,
            ..
        } = self.clone();

        let range = range.into();

        let size = range.end - range.start;

        let (mut writer, reader) = tokio::io::duplex(size as usize);

        tokio::spawn(async move {
            let blob_client = client;
            let mut request = blob_client.get();
            if let Some(lease_id) = lease_id.as_ref() {
                request = request.lease_id(*lease_id);
            }

            if let Some(snapshot) = snapshot_id.as_ref() {
                request = request.blob_versioning(snapshot.clone());
            }

            let mut stream = request.range(range).into_stream();
            while let Some(resp) = stream.next().await {
                match resp {
                    Ok(r) => {
                        // Note: Since this is a ranged query, there shouldn't be an additional call
                        let reader = r.data.collect().await.expect("should be able to read");
                        let mut decoder =
                            async_compression::tokio::write::GzipDecoder::new(&mut writer);

                        match tokio::io::copy(&mut reader.as_ref(), &mut decoder).await {
                            Ok(copied) => {
                                event!(Level::TRACE, "Copied {copied} bytes to decoder");
                            }
                            Err(err) => {
                                event!(Level::ERROR, "Error copying bytes to decoder {err}");
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


    /// Returns a put block request builder,
    /// 
    pub fn put_block(&self, block_id: impl Into<BlockId>, block: impl Into<Body>) -> PutBlockBuilder {
        let mut put_block = self.client.put_block(block_id, block);

        if let Some(lease_id) = self.lease_id.as_ref() {
            put_block = put_block.lease_id(*lease_id);
        }

        put_block
    }

    /// Returns a put block list builder,
    /// 
    pub fn put_block_list(&self, block_list: BlockList) -> PutBlockListBuilder {
        let mut put_block_list = self.client.put_block_list(block_list);

        if let Some(lease_id) = self.lease_id.as_ref() {
            put_block_list = put_block_list.lease_id(*lease_id);
        }

        put_block_list
    }

    /// Returns a readonly snapshot of the underlying blob,
    /// 
    pub async fn snapshot(&self) -> Self {
        let resp = self.client.snapshot().await.expect("should be able to take snapshot");

        Self {
            client: self.client.clone(),
            snapshot_id: Some(resp.snapshot),
            lease_id: None,
            exists: true,
        }
    }

    /// Returns true if the underlying blob is assumed to exist,
    /// 
    pub fn exists(&self) -> bool {
        if !self.exists { 
            tokio::runtime::Handle::current().block_on(async {
                self.client.exists().await.unwrap_or_default()
            })
        } else {
            true
        }
    }
}

impl reality::wire::BlockClient for AzureBlockClient {
    type Stream = DuplexStream;
    
    type Entry = AzureBlockEntry;

    fn stream_range(&self, range: Range<usize>) -> Self::Stream {
       self.pull_byte_range(range)
    }

    fn list_blocks(&self) -> ListBlocks<Self::Entry> {
        let client = self.client.clone();
        let mut lease_id = self.lease_id.clone();
        let mut snapshot_id = self.snapshot_id.clone();
        tokio::spawn(async move {
            let mut get_block_list = client.get_block_list();
            if let Some(lease_id) = lease_id.take() {
                get_block_list = get_block_list.lease_id(lease_id);
            }

            if let Some(snapshot_id) = snapshot_id.take() {
                get_block_list = get_block_list.blob_versioning(snapshot_id);
            }


            match get_block_list.await {
                Ok(list) =>{
                    Ok(list.block_with_size_list.blocks.iter().map(|b| AzureBlockEntry(b.clone())).collect())
                },
                Err(err) => {
                    event!(Level::ERROR, "Could not get block list, {err}");

                    Err(std::io::Error::new(ErrorKind::Other, err))
                },
            }
        })
    }
}

