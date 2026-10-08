// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Batched timestamp allocation on a PD stream. Each sent batch has the PD
//! deadline watcher's lifetime; completion disarms that deadline without
//! closing the shared stream.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

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
/// Go `dispatcherCheckRPCConcurrencyInterval`
/// (`pd/client/clients/tso/dispatcher.go:62`).
const DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL: Duration = Duration::from_secs(5);
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
    // Go hands a batch to the stream and collects responses in a separate
    // loop, so a second batch leaves while the first is unanswered whenever
    // the RPC concurrency allows it. This is that shape: the select below
    // either takes a published response or accepts the next batch, and every
    // branch is cancellation-safe -- the response is read from the
    // collector's channel, never from the stream, because the losing branches
    // of a select are dropped mid-poll.
    //
    // Pairing stays positional, as in Go: a gRPC stream answers in the order
    // it was asked, `pending` is pushed in the order batches are yielded, and
    // `allocate_timestamps` pops the front. Ordering therefore does not depend
    // on how many batches are outstanding.
    let mut in_flight: usize = 0;
    let mut current: Option<(super::service_discovery::TsoRoute, String)> = None;

    // Ends every outstanding batch. Their responses can no longer be paired,
    // so completing them is what Go does when its recv loop fails.
    async fn abandon(pending: &Arc<Mutex<VecDeque<RequestGroup>>>) {
        for group in pending.lock().await.drain(..) {
            group.done.complete();
        }
    }

    loop {
        let snapshot = routes.borrow_and_update().clone();
        streams.retain_routes(
            &snapshot
                .iter()
                .map(|(route, _)| route.clone())
                .collect::<Vec<_>>(),
        );
        // A route that left the snapshot takes its outstanding batches with it.
        if let Some((route, endpoint)) = current.clone() {
            if !snapshot.iter().any(|(live, _)| live == &route) {
                streams.remove(&endpoint);
                abandon(&pending).await;
                in_flight = 0;
                current = None;
            }
        }

        enum Step {
            Routes(bool),
            Collected(Option<std::result::Result<TsoResponse, tonic::Status>>),
            Accepted(Option<TsoRequest>),
        }

        let step = {
            let collect = async {
                match current.as_ref() {
                    Some((_, endpoint)) if in_flight > 0 => streams.recv(endpoint).await,
                    // Nothing outstanding: stay pending so the select waits on
                    // the other branches rather than spinning.
                    _ => std::future::pending().await,
                }
            };
            tokio::pin!(collect);
            tokio::select! {
                biased;
                changed = routes.changed() => Step::Routes(changed.is_ok()),
                response = &mut collect => Step::Collected(response),
                request = requests.next() => Step::Accepted(request),
            }
        };

        match step {
            Step::Routes(false) => return Err(Error::ContextCanceled),
            Step::Routes(true) => continue,
            Step::Collected(Some(Ok(response))) => {
                in_flight -= 1;
                allocate_timestamps(&response, &mut *pending.lock().await)?;
            }
            Step::Collected(Some(Err(_)) | None) => {
                if let Some((_, endpoint)) = current.take() {
                    streams.remove(&endpoint);
                }
                abandon(&pending).await;
                in_flight = 0;
            }
            Step::Accepted(None) => return Err(Error::ContextCanceled),
            Step::Accepted(Some(request)) => {
                let candidates = snapshot.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>();
                let Some(route) = pick_stream_route(&candidates).cloned() else {
                    // Fail this collected batch; later batches can use newly
                    // healthy routes.
                    abandon(&pending).await;
                    in_flight = 0;
                    continue;
                };
                // Batches outstanding on another route cannot be answered by
                // this one, so they end here rather than mispairing.
                if let Some((previous, endpoint)) = current.clone() {
                    if previous != route {
                        streams.remove(&endpoint);
                        abandon(&pending).await;
                        in_flight = 0;
                    }
                }
                let endpoint = route.endpoint.clone();
                let channel = snapshot
                    .iter()
                    .find(|(r, _)| r == &route)
                    .expect("the picked route comes from this snapshot")
                    .1
                    .clone();
                match streams
                    .send(route.clone(), channel, request, &forwarding)
                    .await
                {
                    Ok(()) => {
                        current = Some((route, endpoint));
                        in_flight += 1;
                    }
                    Err(_) => {
                        streams.remove(&endpoint);
                        abandon(&pending).await;
                        in_flight = 0;
                        current = None;
                    }
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

/// Go `tsoDispatcher`'s RPC-concurrency decision
/// (`pd/client/clients/tso/dispatcher.go::checkTSORPCConcurrency`).
///
/// Concurrent TSO RPCs aim at the opposite goal from follower proxy and from
/// an additional collection wait, so Go forces the concurrency to 1 whenever
/// either is enabled, however the operator configured it. The decision is
/// normally rate-limited to one check per
/// [`DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL`], with one exception Go spells
/// out: when concurrency is above 1 and a collection wait has just been
/// enabled, the check runs immediately so the two never overlap.
///
/// The permit count is Go's `tokenCount`: one token at concurrency 1, and
/// twice the concurrency above it, because an RPC's duration jitters and Go
/// deliberately lets the number of ongoing requests fluctuate around the
/// target.
struct RpcConcurrency {
    capacity: Arc<Semaphore>,
    /// Go `td.rpcConcurrency`.
    concurrency: isize,
    /// Go `td.tokenCount`.
    token_count: usize,
    /// Go `td.lastCheckConcurrencyTime`; `None` until the first check, so the
    /// first dispatcher circle evaluates the option as Go's zero time does.
    last_check: Option<Instant>,
}

impl RpcConcurrency {
    fn new(capacity: Arc<Semaphore>) -> Self {
        // Go's dispatcher starts with no tokens and adopts the option on its
        // first circle; this starts at that settled state instead, which is
        // the same steady state for the default concurrency of 1.
        Self {
            capacity,
            concurrency: DEFAULT_RPC_CONCURRENCY as isize,
            token_count: DEFAULT_RPC_CONCURRENCY,
            last_check: None,
        }
    }

    async fn check(
        &mut self,
        options: &super::opt::Options,
        max_batch_wait: Duration,
        cancellation: &Cancellation,
        now: Instant,
    ) {
        let immediately_update = self.concurrency > 1 && !max_batch_wait.is_zero();
        if !immediately_update
            && self
                .last_check
                .is_some_and(|last| now.duration_since(last) < DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL)
        {
            return;
        }
        self.last_check = Some(now);
        let mut new_concurrency = options.get_tso_client_rpc_concurrency();
        if !max_batch_wait.is_zero() || options.get_enable_tso_follower_proxy() {
            new_concurrency = 1;
        }
        if new_concurrency == self.concurrency {
            return;
        }
        self.concurrency = new_concurrency;
        // Go keeps `int` throughout, so its arithmetic carries a nonpositive
        // option into the token count unchanged and the dispatcher starves.
        // A permit count is unsigned here, so the conversion has to answer for
        // that value: Go's own default of one ongoing request is the answer,
        // and every value TiDB can actually publish (1, 2 or 4 from
        // `tidb_tso_client_rpc_mode`) is unaffected by the floor.
        let effective = new_concurrency.max(1) as usize;
        let new_token_count = if effective > 1 {
            effective * 2
        } else {
            effective
        };
        if new_token_count > self.token_count {
            self.capacity.add_permits(new_token_count - self.token_count);
            self.token_count = new_token_count;
        } else if new_token_count < self.token_count {
            let drain = (self.token_count - new_token_count) as u32;
            // Go drains returned tokens one at a time under `select` on the
            // dispatcher context; cancellation abandons the reduction and
            // leaves the count as it stands.
            let capacity = Arc::clone(&self.capacity);
            tokio::select! {
                acquired = capacity.acquire_many_owned(drain) => {
                    if let Ok(permits) = acquired {
                        permits.forget();
                        self.token_count = new_token_count;
                    }
                }
                () = cancellation.cancelled() => {}
            }
        }
    }
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
    let rpc_concurrency = Arc::new(Mutex::new(RpcConcurrency::new(Arc::clone(
        &pending_capacity,
    ))));
    let batch_pool = Arc::new(StdMutex::new(Vec::new()));
    futures::stream::unfold(request_rx, move |mut request_rx| {
        let pending_requests = pending_requests.clone();
        let pending_capacity = pending_capacity.clone();
        let rpc_concurrency = rpc_concurrency.clone();
        let batch_pool = batch_pool.clone();
        let watcher = watcher.clone();
        let options = options.clone();
        let order = order.clone();
        let cancellation = cancellation.clone();
        async move {
            let prepare = async {
                let mut requests = RequestBatch::new(batch_pool);
                // Go loads the collection wait once per circle and passes it
                // to the concurrency check, because the two settings are
                // decided together.
                let max_batch_wait = options.get_max_tso_batch_wait_interval();
                rpc_concurrency
                    .lock()
                    .await
                    .check(&options, max_batch_wait, &cancellation, Instant::now())
                    .await;
                let permit = requests
                    .fetch_pending_requests(
                        &cancellation,
                        &mut request_rx,
                        Some(&pending_capacity),
                        max_batch_wait,
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
