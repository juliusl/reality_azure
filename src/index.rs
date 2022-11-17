use azure_core::request_options::LeaseId;
use azure_storage_blobs::{
    blob::{BlobBlockWithSize, BlockWithSizeList},
    prelude::{BlobClient, Snapshot},
};
use reality::wire::{Frame, Interner};
use std::{collections::HashMap, ops::Range, sync::Arc};
use tokio::io::DuplexStream;

use crate::Store;

/// Struct for a store index,
///
/// Indexes all interned content,
///
pub struct StoreIndex {
    /// Interner for decoding string references,
    ///
    interner: Interner,
    /// Blob client for the store,
    ///
    blob_client: Arc<BlobClient>,
    /// Snapshot id,
    ///
    snapshot_id: Option<Snapshot>,
    /// Lease id
    ///
    lease_id: Option<LeaseId>,
    /// Map of byte ranges,
    ///
    map: HashMap<StoreKey, Range<usize>>,
}

/// Struct for a store key,
///
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct StoreKey {
    /// Name key,
    ///
    name: u64,
    /// Symbol key,
    ///
    symbol: u64,
}

/// Struct for an entry in the index,
///
pub struct Entry<'a> {
    /// Parent index of this entry,
    ///
    index: &'a StoreIndex,
    /// Store key of this entry,
    ///
    store_key: StoreKey,
    /// Interner,
    ///
    interner: &'a Interner,
    /// Start byte range,
    ///
    start: usize,
    /// End byte range,
    ///
    end: usize,
}

impl StoreIndex {
    /// Creates a new empty store index,
    ///
    pub fn empty(
        blob_client: Arc<BlobClient>,
        lease_id: Option<LeaseId>,
        snapshot_id: Option<Snapshot>,
    ) -> Self {
        let mut interner = Interner::default();
        interner.add_ident("store");
        interner.add_ident("control");
        interner.add_ident("");

        let index = StoreIndex {
            blob_client,
            snapshot_id,
            lease_id,
            interner,
            map: HashMap::default(),
        };

        index
    }

    /// Index a block list,
    ///
    pub async fn index(&mut self, block_list: BlockWithSizeList) {
        let mut offset = 0;
        for block in block_list.blocks {
            self.add_entry(offset, &block);
            offset += block.size_in_bytes as usize;
        }

        let mut interner = Interner::default();
        if let Some(entry) = self.entries().find(|e| {
            e.name() == Some(&String::from("store")) && e.symbol() == Some(&String::from("control"))
        }) {
            let _interner = Store::load_interner(entry.pull().await).await;
            interner = self.interner.merge(&_interner);
        }

        self.interner = interner;
    }

    /// Returns entries,
    ///
    pub fn entries(&self) -> impl Iterator<Item = Entry> {
        self.map.iter().map(|(key, range)| Entry {
            index: self,
            store_key: *key,
            interner: &self.interner,
            start: range.start,
            end: range.end,
        })
    }

    /// Adds an entry to the store index,
    ///
    fn add_entry(&mut self, offset: usize, block: &BlobBlockWithSize) {
        match &block.block_list_type {
            azure_storage_blobs::prelude::BlobBlockType::Committed(block_id) => {
                let frame = Frame::from(block_id.as_ref());
                let key = StoreKey {
                    name: frame.name_key(),
                    symbol: frame.symbol_key(),
                };

                self.map
                    .insert(key, offset..(block.size_in_bytes as usize) + offset);
            }
            _ => {}
        }
    }
}

impl StoreKey {
    /// Returns the name,
    ///
    pub fn name<'a>(&'a self, interner: &'a Interner) -> Option<&'a String> {
        interner.strings().get(&self.name)
    }

    /// Returns the symbol,
    ///
    pub fn symbol<'a>(&'a self, interner: &'a Interner) -> Option<&'a String> {
        interner.strings().get(&self.symbol)
    }
}

impl<'a> Entry<'a> {
    /// Returns the name of the entry,
    ///
    pub fn key(&self) -> &StoreKey {
        &self.store_key
    }

    /// Returns the name of this entry,
    ///
    pub fn name(&self) -> Option<&String> {
        self.key().name(self.interner)
    }

    /// Returns the symbol of this entry,
    ///
    pub fn symbol(&self) -> Option<&String> {
        self.key().symbol(self.interner)
    }

    /// Returns a reader to pull bytes for this entry,
    ///
    pub async fn pull(&self) -> DuplexStream {
        Store::pull_byte_range(
            self.index.blob_client.clone(),
            self.start..self.end,
            self.index.lease_id,
            self.index.snapshot_id.clone(),
        )
    }
}
