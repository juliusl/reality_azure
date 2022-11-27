use std::{
    io::SeekFrom,
    path::PathBuf,
    sync::Arc,
};

use bytes::{BufMut, Bytes, BytesMut};
use reality::wire::{Frame, block_tasks::ListBlocks};
use tokio::io::{AsyncReadExt, AsyncSeekExt, DuplexStream};
use tokio_stream::StreamExt;
use tracing::{event, Level};

/// Struct pointing to a file that is suitable to be read from an ArchiveBlockClient,
///
/// This means that the entries are ordered as they would in an upstream source, and each entry
/// has a path that is a base64 encoded frame
///
#[derive(Clone)]
pub struct ArchiveBlockClient {
    src: Arc<PathBuf>,
}

/// Struct for a block entry within an archive, derives from a tar header
/// 
#[derive(Clone)]
pub struct ArchiveBlockEntry {
    /// Reference to source file path,
    ///
    frame: Frame,
    /// Starting position of header
    ///
    size: u64,
}

impl reality::wire::BlockEntry for ArchiveBlockEntry {
    fn frame(&self) -> reality::wire::Frame {
        self.frame.clone()
    }

    fn size(&self) -> usize {
        self.size as usize
    }
}

impl reality::wire::BlockClient for ArchiveBlockClient {
    type Stream = DuplexStream;

    type Entry = ArchiveBlockEntry;

    fn stream_range(&self, range: std::ops::Range<usize>) -> Self::Stream {
        let (mut tx, rx) = tokio::io::duplex(range.len());

        let src = self.src.clone();
        tokio::spawn(async move {
            match tokio::fs::File::open(src.as_path()).await {
                Ok(mut file) => {
                    file.seek(SeekFrom::Start(range.start as u64))
                        .await
                        .expect("should be able to seek to range");

                    let mut buf = BytesMut::with_capacity(range.len());
                    buf.put_bytes(0, range.len());

                    file.read_exact(buf.as_mut())
                        .await
                        .expect("should be able to read bytes");

                    match tokio::io::copy(&mut buf.freeze().as_ref(), &mut tx).await {
                        Ok(copied) => {
                            event!(Level::TRACE, "Copied {copied} bytes");
                        }
                        Err(err) => {
                            event!(Level::ERROR, "Could not copy bytes, {err}");
                        }
                    }
                }
                Err(err) => {
                    event!(Level::ERROR, "Could not open src file, {err}");
                }
            }
        });

        rx
    }

    fn list_blocks(&self) -> ListBlocks<Self::Entry> {
        let src = self.src.clone();
        tokio::spawn(async move {
            let file = tokio::fs::File::open(src.as_path())
                .await
                .expect("should be able to open file");

            let mut archive = tokio_tar::Archive::new(file);

            let mut entries = archive
                .entries()
                .expect("should be able to create entries stream");

            let mut returns = vec![];
            while let Some(Ok(entry)) = entries.next().await {
                match entry.header().size() {
                    Ok(size) => {
                        let path = entry.path().expect("shoild have a path").to_str().expect("should be a string").to_string();
                        let bytes = base64::decode(path).expect("should be valid base64");

                        let frame = Frame::from(Bytes::from(bytes));

                        returns.push(ArchiveBlockEntry { size, frame });
                    }
                    Err(err) => {
                        event!(Level::ERROR, "Could not read size, {err}");
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, err));
                    }
                }
            }

            Ok(returns)
        })
    }
}
