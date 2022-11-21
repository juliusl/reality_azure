use azure_core::request_options::LeaseId;
use azure_storage_blobs::{
    blob::{BlobBlockWithSize, BlockWithSizeList},
    prelude::{BlobClient, Snapshot},
};
use bytes::{Bytes, BytesMut};
use futures::{Future, StreamExt};
use reality::wire::{Decoder, Encoder, Frame, Interner};
use std::{collections::HashMap, ops::Range, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio_tar::{Archive, Header};
use tracing::{event, Level};

use crate::Store;

/// Struct for a store index,
///
/// Indexes all interned content,
///
#[derive(Clone)]
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
#[derive(Default, Debug, PartialEq, Eq, Hash, Clone)]
pub struct StoreKey {
    /// Name key,
    ///
    name: u64,
    /// Symbol key,
    ///
    symbol: u64,
    /// Original frame,
    ///
    frame: Frame,
}

/// Struct for an entry in the index,
///
pub struct Entry {
    /// Parent index of this entry,
    ///
    index: Arc<StoreIndex>,
    /// Store key of this entry,
    ///
    store_key: StoreKey,
    /// Interner,
    ///
    interner: Arc<Interner>,
    /// Start byte range,
    ///
    start: usize,
    /// End byte range,
    ///
    end: usize,
    /// Cached bytes for this entry,
    ///
    cache: Option<Bytes>,
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

    /// Index a block list,
    ///
    pub async fn index(&mut self, block_list: BlockWithSizeList) {
        let mut offset = 0;
        let mut encoder = Encoder::new();

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
            let _interner = Store::load_interner(entry.pull()).await;
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
                    frame: frame.clone(),
                });
            }

            self.blob_device_map.insert(symbol.to_string(), keys);
        }
    }

    /// Returns entries,
    ///
    pub fn entries(&self) -> impl Iterator<Item = Entry> + '_ {
        let index = self.clone();
        let index = Arc::new(index);
        let interner = self.interner.clone();
        let interner = Arc::new(interner);
        self.map.iter().map(move |(key, range)| Entry {
            index: index.clone(),
            store_key: key.clone(),
            interner: interner.clone(),
            start: range.start,
            end: range.end,
            cache: None,
        })
    }

    /// Returns entries in order,
    ///
    pub fn entries_ordered(&self) -> Vec<Entry> {
        let index = self.clone();
        let index = Arc::new(index);
        let interner = self.interner.clone();
        let interner = Arc::new(interner);
        let mut entries = self
            .map
            .iter()
            .map(move |(key, range)| Entry {
                index: index.clone(),
                store_key: key.clone(),
                interner: interner.clone(),
                start: range.start,
                end: range.end,
                cache: None,
            })
            .collect::<Vec<_>>();

        entries.sort_by(|a, b| a.start.cmp(&b.start));

        entries
    }

    /// Returns an entry for a key,
    ///
    pub fn entry(&self, key: StoreKey) -> Option<Entry> {
        let index = self.clone();
        let index = Arc::new(index);
        let interner = self.interner.clone();
        let interner = Arc::new(interner);

        if let Some(range) = self.map.get(&key) {
            Some(Entry {
                index,
                store_key: key,
                interner,
                start: range.start,
                end: range.end,
                cache: None,
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
                    frame: frame.clone(),
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

    /// Returns the hash code for this store key,
    ///
    /// This is jsut key ^ symbol
    ///
    pub fn hash_code(&self) -> u64 {
        self.name ^ self.symbol
    }
}

impl Entry {
    /// Returns the name of the entry,
    ///
    pub fn key(&self) -> &StoreKey {
        &self.store_key
    }

    /// Returns the stored size of this entry,
    ///
    pub fn size(&self) -> usize {
        self.end - self.start
    }

    /// Returns the name of this entry,
    ///
    pub fn name(&self) -> Option<&String> {
        self.key().name(&self.interner)
    }

    /// Returns the symbol of this entry,
    ///
    pub fn symbol(&self) -> Option<&String> {
        self.key().symbol(&self.interner)
    }

    /// Returns a reader to pull bytes for this entry,
    ///
    pub fn pull(&self) -> DuplexStream {
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
            let buf = self.bytes().await;
            match Archive::new(buf.as_ref()).unpack(path.as_ref()).await {
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
            let mut bytes = self.pull();

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
        if let Some(keys) = self
            .symbol()
            .and_then(|s| self.index.blob_device_map.get(s))
        {
            !keys.is_empty()
        } else {
            false
        }
    }

    /// Returns a fully loaded encoder for this entry,
    ///
    pub async fn encoder(&self) -> Option<Encoder> {
        if let Some(mut stream) = self.stream_blob_device(4096) {
            let mut encoder = Encoder::new();

            match stream.read_to_end(encoder.blob_device.get_mut()).await {
                Ok(read) => {
                    event!(Level::TRACE, "Read {read} bytes");
                }
                Err(err) => {
                    event!(Level::ERROR, "Error reading blob device stream, {err}");
                }
            }

            encoder.interner = self.index.interner.clone();

            let mut frames = self.pull();

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

    /// Return blob device entries that belong to this entry,
    ///
    pub fn iter_blob_entries(&self) -> impl Iterator<Item = Entry> + '_ {
        let keys = if self.has_blob_device() {
            let key = self.symbol().expect("should have a symbol");
            self.index
                .blob_device_map
                .get(key)
                .expect("should have keys")
                .clone()
        } else {
            vec![]
        };

        keys.into_iter().filter_map(|k| self.index.entry(k))
    }

    /// If entry has child blob entries, concatenates each into a single stream,
    ///
    pub fn stream_blob_device(&self, buffer_size: usize) -> Option<DuplexStream> {
        if self.has_blob_device() {
            let (mut sender, receiver) = tokio::io::duplex(buffer_size);

            let mut blocks = futures::stream::FuturesOrdered::new();

            for entry in self.iter_blob_entries() {
                blocks.push_back(async move { entry.bytes().await });
            }

            tokio::task::spawn(async move {
                while let Some(next) = blocks.next().await {
                    let next_len = next.len();

                    match sender.write_all(next.as_ref()).await {
                        Ok(_) => {
                            event!(Level::TRACE, "Sent {next_len} bytes");
                        }
                        Err(err) => {
                            event!(Level::ERROR, "Error sending bytes, {err}");
                        }
                    }
                }
            });

            Some(receiver)
        } else {
            None
        }
    }

    /// Manually join's blob devices with an accumalator `T`,
    ///
    /// Call on_blob on each blob read from blob device entries. Blobs are processed in order,
    /// but are fetched in parallel.
    ///
    pub async fn join_blob_device<T, F>(&self, mut acc: T, on_blob: impl Fn(T, Entry, Bytes) -> F) -> T
    where
        F: Future<Output = T>,
    {
        if self.has_blob_device() {
            let mut blocks = futures::stream::FuturesOrdered::new();

            for entry in self.iter_blob_entries() {
                blocks.push_back(async move {
                    let bytes = entry.bytes().await;
                    (entry, bytes)
                });
            }

            while let Some((entry, next)) = blocks.next().await {
                acc = on_blob(acc, entry, next).await;
            }
        }

        acc
    }

    /// Caches bytes for this entry,
    ///
    /// This can be useful if this entry isn't being unpacked to the filesystem, but this entry
    /// will be around long enough to be called multiple times for bytes.
    ///  
    pub async fn cache(&mut self) {
        let mut size = self.end - self.start;
        let mut bytes = BytesMut::with_capacity(self.end - self.start);

        while size > 0 {
            match self.pull().read_buf(&mut bytes).await {
                Ok(read) => {
                    event!(Level::TRACE, "Read {read} bytes");
                    size -= read;
                }
                Err(err) => {
                    event!(Level::ERROR, "Could not cache entry, {err}");
                }
            }
        }

        self.cache = Some(bytes.into());
    }

    /// Returns bytes for this entry,
    ///
    /// If a cache exists, returns the cached version
    ///
    pub async fn bytes(&self) -> Bytes {
        if let Some(cached) = self.cache.as_ref() {
            cached.clone()
        } else {
            let mut bytes = self.pull();

            let mut buf = vec![];

            match bytes.read_to_end(&mut buf).await {
                Ok(read) => {
                    event!(Level::TRACE, "Read {read} bytes");
                }
                Err(err) => {
                    event!(Level::ERROR, "Could not read blob, {err}");
                }
            }

            buf.into()
        }
    }

    /// Returns a hash code for this entry's key,
    ///
    pub fn hash_code(&self) -> u64 {
        self.key().hash_code()
    }
}
