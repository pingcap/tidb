// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Batched timestamp allocation on a PD stream. Each sent batch has the PD
//! deadline watcher's lifetime; completion disarms that deadline without
//! closing the shared stream.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures::prelude::*;
use tokio::sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use super::batch::Controller;
use super::deadline::{DeadlineDone, Watcher};
use crate::async_util::Cancellation;
use crate::internal_err;
use crate::proto::pdpb::pd_client::PdClient;
use crate::proto::pdpb::*;
use crate::{Error, Result};

/// Go newTSODispatcher uses defaultMaxTSOBatchSize * 2 for both the
/// collector and request queue. Its default RPC concurrency is one.
const MAX_BATCH_SIZE: usize = 20_000;
const DEFAULT_RPC_CONCURRENCY: usize = 1;
/// `clients/tso.newTSODispatcher` uses the same deadline channel capacity.
const DEADLINE_CAPACITY: usize = 64;

type TimestampRequest = oneshot::Sender<Timestamp>;

struct OracleInner {
    request_tx: mpsc::Sender<TimestampRequest>,
    cancellation: Cancellation,
    worker: Mutex<Option<JoinHandle<Result<()>>>>,
}

impl Drop for OracleInner {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// The timestamp oracle (TSO) which provides monotonically increasing timestamps.
#[derive(Clone)]
pub(crate) struct TimestampOracle {
    inner: Arc<OracleInner>,
}

impl TimestampOracle {
    pub(crate) fn new(
        cluster_id: u64,
        pd_client: &PdClient<Channel>,
        timeout: Duration,
    ) -> Result<TimestampOracle> {
        let (request_tx, request_rx) = mpsc::channel(MAX_BATCH_SIZE);
        let cancellation = Cancellation::default();
        let worker = tokio::spawn(run_tso(
            cluster_id,
            pd_client.clone(),
            request_rx,
            timeout,
            cancellation.clone(),
        ));
        Ok(TimestampOracle {
            inner: Arc::new(OracleInner {
                request_tx,
                cancellation,
                worker: Mutex::new(Some(worker)),
            }),
        })
    }

    pub(crate) async fn get_timestamp(self) -> Result<Timestamp> {
        let (request, response) = oneshot::channel();
        tokio::select! {
            // Retiring a stream must not revoke a batch result already delivered
            // to this request. Go Request waits independently of stream context.
            biased;
            result = async {
                self.inner.request_tx.send(request).await
                    .map_err(|_| internal_err!("TimestampRequest channel is closed"))?;
                Ok(response.await?)
            } => result,
            _ = self.inner.cancellation.cancelled() => Err(Error::ContextCanceled),
        }
    }

    pub(crate) fn cancellation(&self) -> Cancellation {
        self.inner.cancellation.clone()
    }

    pub(crate) async fn close(&self) {
        self.inner.cancellation.cancel();
        let mut worker = self.inner.worker.lock().await;
        if let Some(handle) = worker.as_mut() {
            // Keep ownership through the await so a cancelled close can be retried.
            match handle.await {
                Ok(Ok(())) | Ok(Err(Error::ContextCanceled)) => {}
                Ok(Err(error)) => log::debug!("TSO stream stopped: {error}"),
                Err(error) => log::error!("TSO worker failed: {error}"),
            }
            worker.take();
        }
    }
}

async fn run_tso(
    cluster_id: u64,
    mut pd_client: PdClient<Channel>,
    request_rx: mpsc::Receiver<TimestampRequest>,
    timeout: Duration,
    cancellation: Cancellation,
) -> Result<()> {
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let watcher = Watcher::new(&cancellation, DEADLINE_CAPACITY, "tso");
    let request_stream = request_stream(
        cluster_id,
        request_rx,
        pending_requests.clone(),
        watcher.clone(),
        timeout,
        cancellation.clone(),
    );
    let result = tokio::select! {
        _ = cancellation.cancelled() => Err(Error::ContextCanceled),
        result = async {
            // Include response-header establishment in the stream cancellation
            // scope. The request stream starts each deadline before yielding its RPC.
            let mut responses = pd_client.tso(request_stream).await?.into_inner();
            while let Some(response) = responses.message().await? {
                allocate_timestamps(&response, &mut *pending_requests.lock().await)?;
            }
            Err(super::errs::ERR_CLIENT_TSO_STREAM_CLOSED.error().with_stack().into())
        } => result,
    };
    cancellation.cancel();
    watcher.close().await;
    pending_requests.lock().await.clear();
    result
}

struct RequestGroup {
    // Rust drops fields in declaration order. Go returns the RPC token before
    // invoking finishers, including when an entire pending queue is discarded.
    _permit: OwnedSemaphorePermit,
    count: u32,
    requests: RequestBatch,
    done: DeadlineDone,
}

// Go keeps each in-flight controller in its batchBufferPool until completion.
// With the native default one-token mode, this holds at most the in-flight
// buffer and the next collector. No pool lock crosses an await or callback.
type BatchPool = Arc<StdMutex<Vec<Controller<TimestampRequest>>>>;

struct RequestBatch {
    controller: Option<Controller<TimestampRequest>>,
    pool: BatchPool,
}

impl RequestBatch {
    fn new(pool: BatchPool) -> Self {
        let reused = pool.lock().expect("TSO batch pool poisoned").pop();
        let controller = reused.unwrap_or_else(|| {
            Controller::new(
                MAX_BATCH_SIZE,
                Some(Box::new(|_, request, _| drop(request))),
                Some(crate::stats::pd_tso_best_batch_size_observer()),
            )
        });
        Self {
            controller: Some(controller),
            pool,
        }
    }
}

impl Deref for RequestBatch {
    type Target = Controller<TimestampRequest>;
    fn deref(&self) -> &Self::Target {
        self.controller.as_ref().unwrap()
    }
}

impl DerefMut for RequestBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.controller.as_mut().unwrap()
    }
}

impl Drop for RequestBatch {
    fn drop(&mut self) {
        if let Some(mut controller) = self.controller.take() {
            controller.finish_collected_requests(None, Some(&Error::ContextCanceled));
            self.pool
                .lock()
                .expect("TSO batch pool poisoned")
                .push(controller);
        }
    }
}

fn request_stream(
    cluster_id: u64,
    request_rx: mpsc::Receiver<TimestampRequest>,
    pending_requests: Arc<Mutex<VecDeque<RequestGroup>>>,
    watcher: Watcher,
    timeout: Duration,
    cancellation: Cancellation,
) -> impl Stream<Item = TsoRequest> + Send + 'static {
    let pending_capacity = Arc::new(Semaphore::new(DEFAULT_RPC_CONCURRENCY));
    let batch_pool = Arc::new(StdMutex::new(Vec::new()));
    futures::stream::unfold(request_rx, move |mut request_rx| {
        let pending_requests = pending_requests.clone();
        let pending_capacity = pending_capacity.clone();
        let batch_pool = batch_pool.clone();
        let watcher = watcher.clone();
        let cancellation = cancellation.clone();
        async move {
            let prepare = async {
                let mut requests = RequestBatch::new(batch_pool);
                let permit = requests
                    .fetch_pending_requests(
                        &cancellation,
                        &mut request_rx,
                        Some(&pending_capacity),
                        Duration::ZERO,
                    )
                    .await
                    .ok()??;
                requests.adjust_best_batch_size();
                let stream_cancellation = cancellation.clone();
                let done = watcher
                    .start(&cancellation, timeout, move || {
                        stream_cancellation.cancel();
                    })
                    .await?;
                let count = requests.get_collected_request_count() as u32;
                let mut pending = pending_requests.lock().await;
                // A cancelled stream must not publish more pending work after
                // the receiver has drained it during shutdown.
                if cancellation.is_cancelled() {
                    return None;
                }
                pending.push_back(RequestGroup {
                    count,
                    requests,
                    done,
                    _permit: permit,
                });
                Some(TsoRequest {
                    header: Some(RequestHeader {
                        cluster_id,
                        ..Default::default()
                    }),
                    count,
                    dc_location: String::new(),
                })
            };
            let request = tokio::select! {
                _ = cancellation.cancelled() => None,
                request = prepare => request,
            }?;
            Some((request, request_rx))
        }
    })
}

fn allocate_timestamps(
    resp: &TsoResponse,
    pending_requests: &mut VecDeque<RequestGroup>,
) -> Result<()> {
    let RequestGroup {
        count,
        mut requests,
        done,
        _permit,
    } = pending_requests
        .pop_front()
        .ok_or_else(|| internal_err!("PD gives more TsoResponse than expected"))?;
    // Go completes the deadline on both successful and failed batch callbacks.
    done.complete();
    let tail_ts = resp
        .timestamp
        .as_ref()
        .ok_or_else(|| internal_err!("No timestamp in TsoResponse"))?;
    if count != resp.count {
        return Err(super::errs::ERR_TSO_LENGTH.error().with_stack().into());
    }
    // Go doneCollectedRequests returns the token before request callbacks.
    drop(_permit);
    requests.finish_collected_requests(
        Some(&mut |index, request, _| {
            let ts = Timestamp {
                physical: tail_ts.physical,
                logical: tail_ts.logical - (i64::from(resp.count) - 1 - index as i64),
                suffix_bits: tail_ts.suffix_bits,
            };
            let _ = request.send(ts);
        }),
        None,
    );
    Ok(())
}

#[cfg(test)]
#[path = "timestamp_tests.rs"]
mod tests;
