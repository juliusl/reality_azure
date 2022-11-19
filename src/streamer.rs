use std::io::Write;

use azure_core::Body;
use bytes::Bytes;
use flate2::{write::GzEncoder, Compression};
use reality::{
    wire::{Data, Encoder, Frame, ResourceId},
    Attributes,
};
use tracing::{event, Level};

use crate::store_stream::FrameStream;

/// Struct w/ channels to stream upload a store,
///
pub struct Streamer {
    /// Channel for receiving frames that need to be encoded,
    ///
    frames: FrameStream,
    /// Resource id of this streamer,
    ///
    resource_id: ResourceId,
    /// Current bytes submitted by this streamer,
    ///
    bytes_submitted: usize,
}

impl Streamer {
    /// Returns a new streamer,
    /// 
    pub fn new(resource_id: ResourceId, frames: FrameStream) -> Self {
        Streamer {
            frames,
            resource_id,
            bytes_submitted: 0,
        }
    }

    /// Submits a frame,
    ///
    /// If a blob is provided, the frame will have it's extent information set now,
    /// this allows extent frames to be passed with Value::Empty initially, and w/ the actual bytes as a parameter.
    ///
    pub async fn submit_frame(&mut self, mut frame: Frame, blob: Option<Blob>) {
        if let Some(blob) = blob.as_ref() {
            let cursor = self.bytes_submitted;
            self.bytes_submitted += blob.len();

            match blob {
                Blob::Text(_) => {
                    frame = frame.set_text_extent(blob.len() as u64, cursor as u64);
                }
                Blob::Binary(_) => {
                    frame = frame.set_binary_extent(blob.len() as u64, cursor as u64);
                }
                _ => {}
            }
        }

        match self
            .frames
            .send((self.resource_id.clone(), frame.clone(), blob))
        {
            Ok(_) => {
                event!(
                    Level::TRACE,
                    "Submitted frame, {:#?} for {:?}",
                    frame,
                    self.resource_id
                );
            }
            Err(err) => {
                event!(Level::ERROR, "Could not send frame {err}");
            }
        }
    }

    /// Submits an entire encoder,
    /// 
    /// Used in case there aren't really any complicated blobs being transmitted. For example, in the Filesystem case, submit_frame gives more 
    /// flexibility over using just an encoder.
    ///
    pub async fn submit_encoder(&mut self, encoder: Encoder) {
        for frame in encoder.frames {
            if frame.is_extent() {
                if let Some(Data::Extent {
                    length,
                    cursor: Some(cursor),
                }) = frame.value()
                {
                    let start = cursor as usize;
                    let end = start + length as usize;
                    let blob = &encoder.blob_device.get_ref()[start..end];

                    match frame.attribute() {
                        Some(Attributes::BinaryVector) => {
                            self.submit_frame(frame, Some(Blob::Binary(Bytes::copy_from_slice(blob))))
                                .await;
                        }
                        Some(Attributes::Text) => {
                            self.submit_frame(
                                frame,
                                Some(Blob::Text(
                                    String::from_utf8(blob.to_vec()).expect("should be valid utf8"),
                                )),
                            )
                            .await;
                        }
                        _ => {
                            panic!("Invalid frame type")
                        }
                    }
                }
            } else {
                self.submit_frame(frame, None).await;
            }
        }
    }
}

/// Enumeration of blob types that can be streamed,
///
pub enum Blob {
    /// UTF8 encoded bytes,
    ///
    Text(String),
    /// Raw u8 bytes,
    ///
    Binary(Bytes),
    /// Compressed bytes,
    /// 
    Compressed(Bytes),
}

impl Blob {
    /// Returns the length in bytes of this blob,
    ///
    pub fn len(&self) -> usize {
        match self {
            Blob::Text(text) => text.len(),
            Blob::Binary(bin) => bin.len(),
            Blob::Compressed(c) => c.len(),
        }
    }

    /// Compresses self returning the resulting blob,
    /// 
    pub fn compress(self) -> Blob {
        match self {
            Blob::Text(bin) => {
                let mut gz_encoder = GzEncoder::new(vec![], Compression::fast());

                match gz_encoder.write_all(&mut bin.as_bytes()) {
                    Ok(_) => {
                        gz_encoder.flush().expect("should be able to flush");
                        let bytes = gz_encoder.finish().expect("should be able to finish encoding");

                        Blob::Compressed(bytes.into())
                    }
                    Err(err) => {
                        panic!("Error wrting to gz encoder, {err}");
                    }
                }
            }
            Blob::Binary(mut bin) => {
                let mut gz_encoder = GzEncoder::new(vec![], Compression::fast());

                match gz_encoder.write_all(&mut bin) {
                    Ok(_) => {
                        gz_encoder.flush().expect("should be able to flush");
                        let bytes = gz_encoder.finish().expect("should be able to finish encoding");

                        Blob::Compressed(bytes.into())
                    }
                    Err(err) => {
                        panic!("Error wrting to gz encoder, {err}");
                    }
                }
            },
            Blob::Compressed(_) => self,
        }
    }
}

impl Into<Body> for Blob {
    fn into(self) -> Body {
        match self {
            Blob::Text(text) => Body::Bytes(text.as_bytes().to_vec().into()),
            Blob::Binary(bytes) |  Blob::Compressed(bytes) => Body::Bytes(bytes),
        }
    }
}