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
use super::tso_batch::{TimestampParts, TimestampTracker, TsoBatch};
use crate::async_util::Cancellation;
use crate::internal_err;
#[cfg(test)]
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
    routes: Option<tokio::sync::watch::Sender<Vec<(super::service_discovery::TsoRoute, Channel)>>>,
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

enum Transport {
    #[cfg(test)]
    Pd(PdClient<Channel>),
    Discovered(
        tokio::sync::watch::Receiver<Vec<(super::service_discovery::TsoRoute, Channel)>>,
        super::service_discovery::TsoForwarding,
    ),
}

impl TimestampOracle {
    pub(crate) fn discovered(
        cluster_id: u64,
        routes: Vec<(super::service_discovery::TsoRoute, Channel)>,
        forwarding: super::service_discovery::TsoForwarding,
        options: Arc<super::opt::Options>,
        timeout: Duration,
        order: TimestampTracker,
    ) -> Result<Self> {
        let (sender, receiver) = tokio::sync::watch::channel(routes);
        let mut oracle = Self::with_transport(
            cluster_id,
            Transport::Discovered(receiver, forwarding),
            options,
            timeout,
            order,
        )?;
        Arc::get_mut(&mut oracle.inner).unwrap().routes = Some(sender);
        Ok(oracle)
    }

    pub(crate) fn update_routes(&self, routes: Vec<(super::service_discovery::TsoRoute, Channel)>) {
        if let Some(sender) = &self.inner.routes {
            sender.send_if_modified(|current| {
                if current
                    .iter()
                    .map(|(r, _)| r)
                    .eq(routes.iter().map(|(r, _)| r))
                {
                    return false;
                }
                *current = routes;
                true
            });
        }
    }

    fn with_transport(
        cluster_id: u64,
        transport: Transport,
        options: Arc<super::opt::Options>,
        timeout: Duration,
        order: TimestampTracker,
    ) -> Result<Self> {
        let (request_tx, request_rx) = mpsc::channel(MAX_BATCH_SIZE);
        let cancellation = Cancellation::default();
        let worker = tokio::spawn(run_tso(
            cluster_id,
            transport,
            request_rx,
            options,
            timeout,
            cancellation.clone(),
            order,
        ));
        Ok(Self {
            inner: Arc::new(OracleInner {
                request_tx,
                cancellation,
                worker: Mutex::new(Some(worker)),
                routes: None,
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn new(
        cluster_id: u64,
        pd_client: &PdClient<Channel>,
        timeout: Duration,
    ) -> Result<TimestampOracle> {
        Self::with_transport(
            cluster_id,
            Transport::Pd(pd_client.clone()),
            Arc::new(super::opt::Options::new()),
            timeout,
            TimestampTracker::default(),
        )
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
        // Retained request handles must not retain discovery channels after close.
        if let Some(routes) = &self.inner.routes {
            routes.send_replace(Vec::new());
        }
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
    transport: Transport,
    request_rx: mpsc::Receiver<TimestampRequest>,
    options: Arc<super::opt::Options>,
    timeout: Duration,
    cancellation: Cancellation,
    order: TimestampTracker,
) -> Result<()> {
    let pending_requests = Arc::new(Mutex::new(VecDeque::new()));
    let watcher = Watcher::new(&cancellation, DEADLINE_CAPACITY, "tso");
    let request_stream = request_stream_with_options(
        cluster_id,
        request_rx,
        pending_requests.clone(),
        watcher.clone(),
        options,
        timeout,
        cancellation.clone(),
        order,
    );
    let result = tokio::select! {
        _ = cancellation.cancelled() => Err(Error::ContextCanceled),
        result = async {
            // Include response-header establishment in the stream cancellation
            // scope. The request stream starts each deadline before yielding its RPC.
            match transport {
                #[cfg(test)]
                Transport::Pd(mut client) => {
                    let mut responses = client.tso(request_stream).await?.into_inner();
                    while let Some(response) = responses.next().await {
                        allocate_timestamps(&response?, &mut *pending_requests.lock().await)?;
                    }
                    Err(super::errs::ERR_CLIENT_TSO_STREAM_CLOSED.error().with_stack().into())
                }
                Transport::Discovered(routes, forwarding) => {
                    run_discovered(request_stream, routes, pending_requests.clone(), forwarding).await
                }
            }
        } => result,
    };
    cancellation.cancel();
    watcher.close().await;
    pending_requests.lock().await.clear();
    result
}

async fn run_discovered(
    requests: impl Stream<Item = TsoRequest>,
    mut routes: tokio::sync::watch::Receiver<Vec<(super::service_discovery::TsoRoute, Channel)>>,
    pending: Arc<Mutex<VecDeque<RequestGroup>>>,
    forwarding: super::service_discovery::TsoForwarding,
) -> Result<()> {
    use super::service_discovery::{pick_stream_route, TsoStreamSet};
    let mut streams = TsoStreamSet::default();
    tokio::pin!(requests);
    loop {
        let snapshot = routes.borrow_and_update().clone();
        streams.retain_routes(
            &snapshot
                .iter()
                .map(|(route, _)| route.clone())
                .collect::<Vec<_>>(),
        );
        let request = tokio::select! {
            biased;
            changed = routes.changed() => { if changed.is_err() { return Err(Error::ContextCanceled); } continue; },
            request = requests.next() => request.ok_or(Error::ContextCanceled)?,
        };
        let candidates = snapshot.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>();
        let Some(route) = pick_stream_route(&candidates).cloned() else {
            // Fail this collected batch; future batches can use newly healthy routes.
            for group in pending.lock().await.drain(..) {
                group.done.complete();
            }
            continue;
        };
        let endpoint = route.endpoint.clone();
        let channel = snapshot
            .iter()
            .find(|(r, _)| r == &route)
            .unwrap()
            .1
            .clone();
        let exchange = streams.request(route.clone(), channel, request, &forwarding);
        let response = {
            tokio::pin!(exchange);
            loop {
                tokio::select! {
                    biased;
                    response = &mut exchange => break Some(response),
                    changed = routes.changed() => {
                        if changed.is_err() { return Err(Error::ContextCanceled); }
                        if !routes.borrow().iter().any(|(current, _)| current == &route) {
                            break None;
                        }
                    }
                }
            }
        };
        match response {
            Some(Ok(response)) => allocate_timestamps(&response, &mut *pending.lock().await)?,
            Some(Err(_)) | None => {
                streams.remove(&endpoint);
                for group in pending.lock().await.drain(..) {
                    group.done.complete();
                }
            }
        }
    }
}

struct RequestGroup {
    // Rust drops fields in declaration order. Go returns the RPC token before
    // invoking finishers, including when an entire pending queue is discarded.
    _permit: OwnedSemaphorePermit,
    count: u32,
    requests: RequestBatch,
    done: DeadlineDone,
    order: TimestampTracker,
    before_request: Option<TimestampParts>,
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

#[cfg(test)]
fn request_stream(
    cluster_id: u64,
    request_rx: mpsc::Receiver<TimestampRequest>,
    pending_requests: Arc<Mutex<VecDeque<RequestGroup>>>,
    watcher: Watcher,
    timeout: Duration,
    cancellation: Cancellation,
) -> impl Stream<Item = TsoRequest> + Send + 'static {
    request_stream_with_options(
        cluster_id,
        request_rx,
        pending_requests,
        watcher,
        Arc::new(super::opt::Options::new()),
        timeout,
        cancellation,
        TimestampTracker::default(),
    )
}

fn request_stream_with_options(
    cluster_id: u64,
    request_rx: mpsc::Receiver<TimestampRequest>,
    pending_requests: Arc<Mutex<VecDeque<RequestGroup>>>,
    watcher: Watcher,
    options: Arc<super::opt::Options>,
    timeout: Duration,
    cancellation: Cancellation,
    order: TimestampTracker,
) -> impl Stream<Item = TsoRequest> + Send + 'static {
    let pending_capacity = Arc::new(Semaphore::new(DEFAULT_RPC_CONCURRENCY));
    let batch_pool = Arc::new(StdMutex::new(Vec::new()));
    futures::stream::unfold(request_rx, move |mut request_rx| {
        let pending_requests = pending_requests.clone();
        let pending_capacity = pending_capacity.clone();
        let batch_pool = batch_pool.clone();
        let watcher = watcher.clone();
        let options = options.clone();
        let order = order.clone();
        let cancellation = cancellation.clone();
        async move {
            let prepare = async {
                let mut requests = RequestBatch::new(batch_pool);
                let permit = requests
                    .fetch_pending_requests(
                        &cancellation,
                        &mut request_rx,
                        Some(&pending_capacity),
                        options.get_max_tso_batch_wait_interval(),
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
                    before_request: order.snapshot(),
                    order,
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
        order,
        before_request,
    } = pending_requests
        .pop_front()
        .ok_or_else(|| internal_err!("PD gives more TsoResponse than expected"))?;
    // Go completes the deadline on both successful and failed batch callbacks.
    done.complete();
    let batch = TsoBatch::from_response(resp, count).map_err(|error| {
        if error.kind == "tso_count_mismatch" {
            Error::from(super::errs::ERR_TSO_LENGTH.error().with_stack())
        } else {
            internal_err!("{}: {}", error.kind, error.message)
        }
    })?;
    order
        .accept(batch, before_request)
        .map_err(|error| internal_err!("{}: {}", error.kind, error.message))?;
    // Go doneCollectedRequests returns the token before request callbacks.
    drop(_permit);
    requests.finish_collected_requests(
        Some(&mut |index, request, _| {
            let ts = batch.timestamp(index as u32);
            let _ = request.send(ts);
        }),
        None,
    );
    Ok(())
}

#[cfg(test)]
#[path = "timestamp_tests.rs"]
mod tests;
