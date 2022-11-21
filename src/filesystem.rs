use std::{
    io::{Cursor, Read, Seek, Write},
    path::PathBuf,
    pin::Pin,
};

use bytes::Bytes;
use reality::{
    wire::{Decoder, Encoder, Frame, Interner},
    Value,
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, DuplexStream},
};
use tokio_stream::StreamExt;
use tokio_tar::{Archive, Builder};
use tracing::{event, Level};

use crate::{Blob, Streamer};

/// Struct for encoding/decoding a filesystem to/from the store,
///
/// A TAR is used to represent and encode the filesystem contents.
///
pub struct Filesystem {
    /// Root archive source,
    ///
    archive: Option<ArchiveSource>,
}

impl Filesystem {
    /// Load archive from the filesystem,
    ///
    pub async fn load_tar(path: impl AsRef<str>) -> Option<Self> {
        match tokio::fs::OpenOptions::new()
            .read(true)
            .open(path.as_ref())
            .await
        {
            Ok(stream) => Some(Self {
                archive: Some(ArchiveSource::File(stream)),
            }),
            Err(_) => None,
        }
    }

    /// Returns filesystem from a streamed tar file,
    ///
    pub fn stream_tar(stream: DuplexStream) -> Self {
        Self {
            archive: Some(ArchiveSource::Stream(stream)),
        }
    }

    /// Returns an empty filesystem,
    ///
    pub fn empty() -> Self {
        Self { archive: None }
    }

    /// Consumes and returns the archive from archive source,
    ///
    pub fn take(&mut self) -> Option<Archive<impl AsyncRead + Unpin>> {
        if let Some(archive) = self.archive.take() {
            Some(Archive::new(archive))
        } else {
            None
        }
    }

    /// Exports the filesystem as an encoder,
    ///
    pub async fn export<BlobImpl>(&mut self) -> Option<Encoder<BlobImpl>>
    where
        BlobImpl: Read + Write + Seek + Clone + Default,
    {
        if let Some(mut archive) = self.take() {
            let mut encoder = Encoder::<BlobImpl>::default();

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

            encoder
                .blob_device
                .seek(std::io::SeekFrom::Start(0))
                .expect("should be able to seek to start");

            Some(encoder)
        } else {
            None
        }
    }

    /// Imports an archive from a decoder,
    ///
    pub async fn import<BlobImpl>(decoder: &mut Decoder<'_, BlobImpl>) -> std::io::Result<Filesystem>
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
                let inner = builder
                    .into_inner()
                    .await
                    .expect("should be able to get inner");

                Ok(Self { 
                    archive: Some(ArchiveSource::Memory(Cursor::new(inner.into())))
                })
            }
            Err(err) => {
                event!(Level::ERROR, "Could not finish building archive, {err}");
                Err(err)
            },
        }
    }

    /// Writes the current archive to disk,
    ///
    pub async fn write_disk(&mut self, path: impl AsRef<str>) {
        if let Some(builder) = self.take() {
            let path = PathBuf::from(path.as_ref());

            tokio::fs::create_dir_all(&path.parent().unwrap())
                .await
                .expect("should be able to create dirs");

            match tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(path)
                .await
            {
                Ok(mut file) => match builder.into_inner() {
                    Ok(mut r) => {
                        tokio::io::copy(&mut r, &mut file)
                            .await
                            .expect("should be able to copy");
                    }
                    Err(_) => todo!(),
                },
                Err(err) => {
                    event!(Level::ERROR, "Error opening file, {err}");
                }
            }
        }
    }

    /// Unpack an archive to the specified destination,
    ///
    pub async fn unpack(&mut self, path: impl AsRef<str>) {
        if let Some(mut archive) = self.take() {
            match archive.unpack(path.as_ref()).await {
                Ok(_) => {}
                Err(err) => {
                    event!(Level::ERROR, "Could not unpack, {err}");
                }
            }
        }
    }

    /// Consumes the inner-archive and stream's w/ streamer,
    ///
    pub async fn stream(&mut self, streamer: &mut Streamer) -> Interner {
        let mut interner = Interner::default();
        interner.add_ident("tar");
        interner.add_ident("EOF");

        if let Some(mut archive) = self.take() {
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
                                let size = header.entry_size().expect("should have a size");
                                interner.add_ident(&path);

                                let mut buf = entry.header().as_bytes().to_vec();
                                buf.reserve(size as usize);

                                match entry.read_to_end(&mut buf).await {
                                    Ok(_) => {
                                        streamer
                                            .submit_frame(
                                                Frame::define(
                                                    "tar",
                                                    path,
                                                    &Value::Empty,
                                                    &mut Cursor::<[u8; 1]>::default(),
                                                ),
                                                Some(Blob::Binary(buf.into())),
                                            )
                                            .await;
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
        }

        interner
    }
}

/// Enumeration of archive sources,
///
enum ArchiveSource {
    /// Archive source from a stream,
    ///
    Stream(DuplexStream),
    /// Archive sourced from a file,
    ///
    File(File),
    /// Archive sourced from memory,
    ///
    Memory(Cursor<Bytes>),
}

impl AsyncRead for ArchiveSource {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match &mut *self {
            ArchiveSource::Stream(stream) => {
                let stream = Pin::new(stream);

                stream.poll_read(cx, buf)
            }
            ArchiveSource::File(file) => {
                let stream = Pin::new(file);

                stream.poll_read(cx, buf)
            }
            ArchiveSource::Memory(bytes) => {
                let stream = Pin::new(bytes);

                stream.poll_read(cx, buf)
            }
        }
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
