use std::{sync::Arc, ops::Range};

use azure_core::request_options::LeaseId;
use azure_storage_blobs::prelude::{BlobClient, Snapshot};
use tokio::io::DuplexStream;

use crate::Store;

/// Wrapper struct over an azure blob client,
///
#[derive(Clone)]
pub struct AzureBlobClient {
    /// Client
    ///  
    pub client: Arc<BlobClient>,
    /// Snapshot id,
    ///
    pub snapshot_id: Option<Snapshot>,
    /// Lease id
    ///
    pub lease_id: Option<LeaseId>,
}

impl reality::wire::BlobClient for AzureBlobClient {
    type Stream = DuplexStream;

    fn stream_range(&self, range: Range<usize>) -> Self::Stream {
        Store::pull_byte_range(
            self.client.clone(),
            range,
            self.lease_id.clone(),
            self.snapshot_id.clone(),
        )
    }
}