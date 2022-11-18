use std::io::{Read, Seek, Write};

use reality::wire::{Decoder, Encoder};
use tokio::io::AsyncReadExt;
use tokio_stream::StreamExt;
use tokio_tar::{Archive, Builder};
use tracing::{event, Level};

/// Struct for encoding/decoding a filesystem to/from the store,
///
/// A TAR is used to represent and encode the filesystem contents.
///
pub struct Filesystem {
    /// Root archive with the filesystem,
    ///
    builder: Option<Builder<Vec<u8>>>,
}

impl Filesystem {
    /// Load archive from the filesystem,
    ///
    pub async fn load_tar(path: impl AsRef<str>) -> Option<Self> {
        match tokio::fs::read(path.as_ref()).await {
            Ok(stream) => Self::new(Builder::new(stream)).await,
            Err(_) => None,
        }
    }

    /// Returns a new filesystem from a builder
    ///
    pub async fn new(mut builder: Builder<Vec<u8>>) -> Option<Self> {
        match builder.finish().await {
            Ok(_) => Some(Self {
                builder: Some(builder),
            }),
            Err(err) => {
                event!(Level::ERROR, "Could not load archive, {err}");
                None
            }
        }
    }

    /// Returns an empty filesystem,4
    /// 
    pub fn empty() -> Self {
        Self { builder: None }
    }

    /// Exports the filesystem as an encoder,
    ///
    pub async fn export(&mut self) -> Option<Encoder> {
        if let Some(builder) = self.builder.take() {
            let mut encoder = Encoder::default();

            let archive = builder
                .into_inner()
                .await
                .expect("should be able to take inner stream");
            let mut archive = Archive::new(archive.as_slice());

            match archive.entries() {
                Ok(mut entries) => {
                    while let Some(entry) = entries.next().await {
                        match entry {
                            Ok(mut entry) => {
                                let header = entry.header();
                                let path = header
                                    .path()
                                    .expect("should be a path")
                                    .to_str()
                                    .expect("should be a string")
                                    .to_string();

                                let mut buf = entry.header().as_bytes().to_vec();

                                match entry.read_to_end(&mut buf).await {
                                    Ok(_) => {
                                        encoder.define_binary("tar", path, buf);
                                    }
                                    Err(err) => {
                                        event!(Level::ERROR, "Could not read entry {err}");
                                    }
                                }
                            }
                            Err(err) => {
                                event!(Level::ERROR, "Could not get next entry, {err}");
                            }
                        }
                    }
                }
                Err(err) => {
                    event!(Level::ERROR, "Could not iterate over entries, {err}");
                }
            }

            encoder.blob_device.set_position(0);

            Some(encoder)
        } else {
            None
        }
    }

    /// Imports an archive from a decoder,
    ///
    pub async fn import<BlobImpl>(&mut self, decoder: &mut Decoder<'_, BlobImpl>)
    where
        BlobImpl: Read + Write + Seek + Clone + Default,
    {
        let mut builder = Builder::new(vec![]);

        let files = decoder.decode_properties("tar");

        for (name, value) in files.iter_properties() {
            match value {
                reality::BlockProperty::Single(value) => {
                    if let Some(bin) = value.binary() {
                        let mut archive = Archive::new(bin.as_slice());
                        let mut entries = archive.entries().unwrap();

                        if let Some(entry) = entries.next().await {
                            let entry = entry.unwrap();
                            match builder
                                .append_data(&mut entry.header().clone(), name, entry)
                                .await
                            {
                                Ok(_) => {}
                                Err(err) => {
                                    event!(Level::ERROR, "Error writing tar entry, {err}");
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        match builder.finish().await {
            Ok(_) => {
                self.builder = Some(builder);
            }
            Err(err) => event!(Level::ERROR, "Could not finish building archive, {err}"),
        }
    }

    /// Writes the current archive to disk,
    ///
    pub async fn write_disk(&mut self, path: impl AsRef<str>) {
        if let Some(builder) = self.builder.take() {
            match builder.into_inner().await {
                Ok(data) => match tokio::fs::write(path.as_ref(), data).await {
                    Ok(_) => {}
                    Err(err) => {
                        event!(Level::ERROR, "could not write to disk, {err}");
                    }
                },
                Err(err) => {
                    event!(Level::ERROR, "could not take builder, {err}");
                }
            }
        }
    }

    /// Unpack an archive to the specified destination,
    ///
    pub async fn unpack(&mut self, path: impl AsRef<str>) {
        if let Some(builder) = self.builder.take() {
            match builder.into_inner().await {
                Ok(data) => {
                    let mut archive = Archive::new(data.as_slice());
                    match archive.unpack(path.as_ref()).await {
                        Ok(_) => {}
                        Err(err) => {
                            event!(Level::ERROR, "Could not unpack, {err}");
                        }
                    }
                }
                Err(err) => {
                    event!(Level::ERROR, "could not take builder, {err}");
                }
            }
        }
    }
}

impl WireObject for Filesystem {
    fn encode<BlobImpl>(&self, _: &reality::wire::World, _: &mut Encoder<BlobImpl>)
    where
        BlobImpl: Read + Write + Seek + Clone + Default,
    {
        unimplemented!()
    }

    fn decode(
        _: &reality::wire::Protocol,
        _: &reality::wire::Interner,
        _: &std::io::Cursor<Vec<u8>>,
        _: &[reality::wire::Frame],
    ) -> Self {
        unimplemented!()
    }

    fn build_index(
        _: &reality::wire::Interner,
        _: &[reality::wire::Frame],
    ) -> reality::wire::FrameIndex {
        FrameIndex::default()
    }

    fn resource_id() -> reality::wire::ResourceId {
        ResourceId::new::<Filesystem>()
    }
}
