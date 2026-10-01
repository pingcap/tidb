// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Batched timestamp allocation on a PD stream. Each sent batch has the PD
//! deadline watcher's lifetime; completion disarms that deadline without
//! closing the shared stream.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures::prelude::*;
use tokio::sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use super::deadline::{DeadlineDone, Watcher};
use crate::async_util::Cancellation;
use crate::internal_err;
use crate::proto::pdpb::pd_client::PdClient;
use crate::proto::pdpb::*;
use crate::{Error, Result};

/// Existing native batching bounds; the full Go TSO dispatcher remains a
/// separate package acceptance unit.
const MAX_BATCH_SIZE: usize = 64;
const MAX_PENDING_COUNT: usize = 1 << 16;
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
            Err(internal_err!("TSO stream terminated"))
        } => result,
    };
    cancellation.cancel();
    watcher.close().await;
    pending_requests.lock().await.clear();
    result
}

struct RequestGroup {
    count: u32,
    requests: Vec<TimestampRequest>,
    done: DeadlineDone,
    _permit: OwnedSemaphorePermit,
}

fn request_stream(
    cluster_id: u64,
    request_rx: mpsc::Receiver<TimestampRequest>,
    pending_requests: Arc<Mutex<VecDeque<RequestGroup>>>,
    watcher: Watcher,
    timeout: Duration,
    cancellation: Cancellation,
) -> impl Stream<Item = TsoRequest> + Send + 'static {
    let pending_capacity = Arc::new(Semaphore::new(MAX_PENDING_COUNT));
    futures::stream::unfold(request_rx, move |mut request_rx| {
        let pending_requests = pending_requests.clone();
        let pending_capacity = pending_capacity.clone();
        let watcher = watcher.clone();
        let cancellation = cancellation.clone();
        async move {
            let prepare = async {
                let permit = pending_capacity.acquire_owned().await.ok()?;
                let first = request_rx.recv().await?;
                let mut requests = vec![first];
                while requests.len() < MAX_BATCH_SIZE {
                    match request_rx.try_recv() {
                        Ok(request) => requests.push(request),
                        Err(_) => break,
                    }
                }
                let stream_cancellation = cancellation.clone();
                let done = watcher
                    .start(&cancellation, timeout, move || {
                        stream_cancellation.cancel();
                    })
                    .await?;
                let count = requests.len() as u32;
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
        requests,
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
        return Err(internal_err!(
            "PD gives different number of timestamps than expected"
        ));
    }
    let mut offset = resp.count;
    for request in requests {
        offset -= 1;
        let ts = Timestamp {
            physical: tail_ts.physical,
            logical: tail_ts.logical - offset as i64,
            suffix_bits: tail_ts.suffix_bits,
        };
        let _ = request.send(ts);
    }
    Ok(())
}

#[cfg(test)]
#[path = "timestamp_tests.rs"]
mod tests;
