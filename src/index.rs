use azure_core::request_options::LeaseId;
use azure_storage_blobs::{
    blob::{BlobBlockWithSize, BlockWithSizeList},
    prelude::{BlobClient, Snapshot},
};
use reality::wire::{Decoder, Encoder, Frame, Interner};
use std::{
    collections::HashMap,
    ops::Range,
    sync::Arc,
};
use tokio::io::{AsyncReadExt, DuplexStream};
use tokio_tar::{Archive, Header};
use tracing::{event, Level};

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
    /// Map of blob device ranges,
    ///
    blob_device_map: HashMap<String, Vec<StoreKey>>,
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
            blob_device_map: HashMap::default(),
        };

        index
    }

    /// Index a block list, returns an encoder that represents the frames from the block list,
    ///
    pub async fn index(&mut self, block_list: BlockWithSizeList) {
        let mut offset = 0;
        let mut encoder = Encoder::default();

        for block in block_list.blocks {
            if let Some(frame) = self.add_entry(offset, &block) {
                encoder.frames.push(frame);
            }
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

        encoder.interner = self.interner.clone();
        let mut decoder = Decoder::new(&encoder);

        let store_map = decoder.decode_namespace("store");
        for (symbol, decoder) in store_map.iter() {
            let mut keys = vec![];
            for frame in decoder.frames() {
                keys.push(StoreKey {
                    name: frame.name_key(),
                    symbol: frame.symbol_key(),
                });
            }

            self.blob_device_map.insert(symbol.to_string(), keys);
        }
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

    /// Returns an entry for a key,
    /// 
    pub fn entry(&self, key: StoreKey) -> Option<Entry> {
        if let Some(range) = self.map.get(&key) {
            Some(Entry {
                index: self,
                store_key: key,
                interner: &self.interner,
                start: range.start,
                end: range.end,
            })
        } else {
            None 
        }
    }

    /// Adds an entry to the store index,
    ///
    fn add_entry(&mut self, offset: usize, block: &BlobBlockWithSize) -> Option<Frame> {
        match &block.block_list_type {
            azure_storage_blobs::prelude::BlobBlockType::Committed(block_id) => {
                let frame = Frame::from(block_id.as_ref());
                let key = StoreKey {
                    name: frame.name_key(),
                    symbol: frame.symbol_key(),
                };

                self.map
                    .insert(key, offset..(block.size_in_bytes as usize) + offset);

                Some(frame)
            }
            _ => None,
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

    /// If name is "tar", unpacks this as a file to path,
    ///
    pub async fn unpack(&self, path: impl AsRef<str>) {
        if self.name() == Some(&String::from("tar")) {
            let mut bytes = self.pull().await;

            let mut buf = vec![];

            match bytes.read_to_end(&mut buf).await {
                Ok(read) => {
                    event!(Level::TRACE, "Read {read} bytes");
                }
                Err(err) => {
                    event!(Level::ERROR, "Could not read blob, {err}");
                }
            }

            match Archive::new(buf.as_slice()).unpack(path.as_ref()).await {
                Ok(_) => {}
                Err(err) => {
                    event!(Level::ERROR, "Could not unpack as archive, {err}");
                }
            }
        } else {
            event!(Level::WARN, "Tried to unpack an entry that is not a tar");
        }
    }

    /// Returns a file header if name is "tar",
    ///
    pub async fn file_header(&self) -> Option<Header> {
        if self.name() == Some(&String::from("tar")) {
            let mut bytes = self.pull().await;

            let mut buf = vec![0; 512];

            match bytes.read_exact(&mut buf).await {
                Ok(read) => {
                    event!(Level::TRACE, "Read {read} bytes");
                }
                Err(err) => {
                    event!(Level::ERROR, "Could not read blob, {err}");
                }
            }

            Some(Header::from_byte_slice(buf.as_slice()).clone())
        } else {
            event!(Level::WARN, "Tried to unpack an entry that is not a tar");
            None
        }
    }

    /// Returns true if this entry has a blob device,
    ///
    pub fn has_blob_device(&self) -> bool {
        if let Some(symbol) = self.symbol() {
            self.index.blob_device_map.contains_key(symbol)
        } else {
            false
        }
    }

    /// Returns a fully loaded encoder for this entry,
    /// 
    pub async fn encoder(&self) -> Option<Encoder> {
        if let Some(blocks) = self
            .symbol()
            .and_then(|s| self.index.blob_device_map.get(s))
        {
            let mut encoder = Encoder::default();
        
            for b in blocks.iter().filter_map(|b| self.index.entry(*b)) {
                let mut stream = b.pull().await;

                match stream.read_to_end(encoder.blob_device.get_mut()).await {
                    Ok(_) => {
                    },
                    Err(err) => {
                        event!(Level::ERROR, "Error reading bytes for blob device, {err}");
                    },
                }
            }

            encoder.interner = self.index.interner.clone();
            
            let mut frames = self.pull().await;

            let mut buffer = [0; 64];
            while let Ok(read) = frames.read_exact(&mut buffer).await {
                assert_eq!(read, 64);
                encoder.frames.push(Frame::from(buffer));
            }

           Some(encoder)
        } else {
            None
        }
    }
}
