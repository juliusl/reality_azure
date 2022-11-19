use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    sync::Arc,
};

use azure_storage_blobs::prelude::{BlobBlockType, BlobClient, BlockList};
use bytes::Bytes;
use flate2::{write::GzEncoder, Compression};
use futures::{future::join_all, Future};
use reality::wire::{Encoder, Frame, Interner, ResourceId};
use tokio::{select, task::JoinSet};
use tracing::{event, Level};

use crate::{Blob, Store, Streamer};

/// Type alias for frame being streamed
///
type StreamFrame = (ResourceId, Frame, Option<Blob>);

/// Type alias for the receiving end of the streams,
///
type FrameStreamRecv = tokio::sync::mpsc::UnboundedReceiver<StreamFrame>;

/// Type alias for sending end of frame streams,
///
pub type FrameStream = tokio::sync::mpsc::UnboundedSender<StreamFrame>;

/// Struct to represent a streaming upload of the store,
///
pub struct StoreStream<'a> {
    /// Recv of frames to publish,
    ///
    rx: FrameStreamRecv,
    /// Sender of frames,
    ///
    tx: FrameStream,
    /// Store that is being streamed,
    ///
    store: &'a mut Store,
    /// Interner that will be uploaded on completion,
    ///
    interner: Interner,
    /// Blob client,
    ///
    blob_client: Arc<BlobClient>,
}

impl<'a> StoreStream<'a> {
    /// Returns a new empty store stream
    ///
    pub fn new(store: &'a mut Store, prefix: impl AsRef<str>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(ResourceId, Frame, Option<Blob>)>();
        let client = store.get_blob_client(prefix);
        Self {
            rx,
            tx,
            store,
            interner: Interner::default(),
            blob_client: Arc::new(client),
        }
    }

    /// Returns a new streamer w/ resource_id
    ///
    pub fn streamer(&self, resource_id: ResourceId) -> Streamer {
        Streamer::new(resource_id, self.tx.clone())
    }

    /// Returns a new streamer w/ resource_id looking up the id by name from the reverse index,
    ///
    pub fn find_streamer(&self, name: impl AsRef<str>) -> Option<Streamer> {
        if let Some(id) = self.store.reverse_index.get(name.as_ref()) {
            Some(self.streamer(id.clone()))
        } else {
            None
        }
    }

    /// Starts the stream, calls select on all registered encoders, if a future is returned, then it will be added to a joinset to await completion,
    ///
    /// When all tasks in the joinset complete, the upload will be finalized
    ///
    pub async fn start<F>(
        mut self,
        select: impl Fn(&ResourceId, &Encoder, &StoreStream) -> Option<F>,
    ) where
        F: Future<Output = Option<Interner>> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let mut join_set = JoinSet::<Option<Interner>>::new();

        for (id, enc) in self.store.protocol.iter_encoders() {
            if let Some(future) = select(id, enc, &self) {
                join_set.spawn(future);
            }
        }

        tokio::spawn(async move {
            let mut interner = Interner::default();
            while let Some(result) = join_set.join_next().await {
                event!(Level::TRACE, "Completed stream task");
                match result {
                    Ok(Some(_interner)) => {
                        interner = interner.merge(&_interner);
                    }
                    Ok(None) => {
                        event!(Level::WARN, "Stream skipped");
                    }
                    Err(err) => {
                        event!(Level::ERROR, "Could not join next streaming result, {err}");
                    }
                }
            }
            match tx.send(interner) {
                Ok(_) => {
                    event!(Level::TRACE, "Completing stream");
                }
                Err(_) => {
                    event!(Level::ERROR, "Could not complete stream");
                }
            }
        });

        let future = self.begin_streaming(rx);
        future.await;
    }

    /// Begin receiving frames for stream uploading,
    ///
    async fn begin_streaming(&mut self, finished: tokio::sync::oneshot::Receiver<Interner>) {
        let mut stream_states = HashMap::<ResourceId, StreamState>::default();
        let mut upload_block_futures = vec![];
        let mut objects = vec![];

        let blob_client = self.blob_client.clone();

        let mut finished = Some(finished);

        while let Some((resource_id, frame, mut blob)) = self.try_receive(&mut finished).await {
            if let Some(name) = self.store.index.get(&resource_id) {
                if !stream_states.contains_key(&resource_id) {
                    stream_states.insert(
                        resource_id.clone(),
                        StreamState {
                            name: name.to_string(),
                            frames: vec![],
                            block_list: VecDeque::default(),
                        },
                    );
                }

                if let Some(blob) = blob.take() {
                    let block_id = Bytes::copy_from_slice(frame.bytes());

                    let put_block_request =
                        blob_client.put_block(block_id.clone(), blob.compress());

                    upload_block_futures.push(put_block_request.into_future());

                    if let Some(StreamState { block_list, .. }) =
                        stream_states.get_mut(&resource_id)
                    {
                        block_list.push_back(BlobBlockType::new_uncommitted(block_id));
                    }
                }

                if let Some(StreamState { frames, .. }) = stream_states.get_mut(&resource_id) {
                    frames.push(frame);
                }
            } else {
                event!(
                    Level::WARN,
                    "Unregistered object type is trying to stream w/ this store stream"
                )
            }
        }

        let mut interner = self.interner.clone();
        interner.add_ident("store");
        interner.add_ident("control");
        interner.add_ident("");
        for (
            resource_id,
            StreamState {
                name,
                mut frames,
                mut block_list,
            },
        ) in stream_states.drain()
        {
            let mut buffer = GzEncoder::new(vec![], Compression::fast());
            let mut extension_encoder = Encoder::new();
            let mut extension = extension_encoder.start_extension("store", name);

            for frame in frames.drain(..) {
                match buffer.write_all(frame.bytes()) {
                    Ok(_) => {
                        extension.as_mut().frames.push(frame);
                    }
                    Err(err) => {
                        event!(Level::ERROR, "Could not write frame to buffer, {err}");
                    }
                }
            }

            let pos = extension.insert_at();
            drop(extension);
            let encoder_frame = &extension_encoder.frames[pos];
            // Prepend the object's store frame to the block list and upload it's frames
            let block_id = Bytes::copy_from_slice(encoder_frame.bytes());
            let upload = blob_client.put_block(
                block_id.clone(),
                Bytes::from(buffer.finish().expect("should be able to complete")),
            );

            upload_block_futures.push(upload.into_future());
            block_list.push_front(BlobBlockType::new_uncommitted(block_id));
            objects.push(block_list.make_contiguous().to_vec());

            if let Some(encoder) = self.store.protocol.encoder_by_id(resource_id) {
                interner = interner.merge(&encoder.interner);
            }

            interner = interner.merge(&extension_encoder.interner);
        }

        let (upload, control_id) = self
            .store
            .put_control_device(interner, blob_client.clone())
            .await;

        upload_block_futures.push(upload);
        join_all(upload_block_futures).await;

        let mut block_list = objects.concat();
        block_list.insert(0, control_id);

        match blob_client
            .put_block_list(BlockList { blocks: block_list })
            .await
        {
            Ok(resp) => {
                event!(
                    Level::TRACE,
                    "Put block list, request_id {}",
                    resp.request_id
                );
            }
            Err(err) => {
                event!(Level::ERROR, "Could not put block list, {err}");
            }
        }
    }

    /// Try to receive the next messsage,
    ///
    pub async fn try_receive(
        &mut self,
        finished: &mut Option<tokio::sync::oneshot::Receiver<Interner>>,
    ) -> Option<StreamFrame> {
        if let Some(f) = finished.as_mut() {
            select! {
                next = self.rx.recv() => {
                    next
                }
                interner = f => {
                    match interner {
                        Ok(interner) => {
                            self.interner = self.interner.merge(&interner);
                        },
                        Err(err) => {
                            event!(Level::ERROR, "Error trying to receive interner, {err}");
                        },
                    }
                    finished.take();
                    self.rx.close();
                    self.rx.recv().await
                }
            }
        } else {
            self.rx.recv().await
        }
    }
}

/// Stream state for a resource id
///
struct StreamState {
    /// Name of the object being streamed,
    ///
    pub name: String,
    /// Frames being uploaded,
    ///
    pub frames: Vec<Frame>,
    /// Block list to upload,
    ///
    pub block_list: VecDeque<BlobBlockType>,
}
