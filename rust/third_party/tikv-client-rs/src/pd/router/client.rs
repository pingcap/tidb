// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client `clients/router/client.go`: the router client. Region queries
//! are collected into batches by one dispatcher and sent as one
//! `QueryRegionRequest` on a `QueryRegion` stream -- to a router service
//! node when every request of the batch allows it, otherwise to the PD
//! leader, or to any PD member when follower handling is enabled and every
//! request allows it. A connection daemon keeps the streams in step with the
//! leader and the members.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use futures::FutureExt;
use tokio::sync::{mpsc, Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use super::request::Request;
use super::stream::{QueryRegionStream, StreamKind};
use super::{Region, ServiceDiscovery, DEFAULT_MAX_ROUTER_REQUEST_BATCH_SIZE};
use crate::async_util::Cancellation;
use crate::pd::backoff::Backoffer;
use crate::pd::batch::{Controller, Finisher};
use crate::pd::connectionctx::{ConnectionCtx, Manager};
use crate::pd::errs;
use crate::pd::opt::{GetRegionOp, Options};
use crate::pd::service_discovery::MEMBER_UPDATE_INTERVAL;
use crate::proto::pdpb;
use crate::{Error, Result};

/// Go `servicediscovery.UpdateMemberBackOffBaseTime`.
const UPDATE_MEMBER_BACKOFF_BASE_TIME: Duration = Duration::from_millis(20);
/// Go `servicediscovery.UpdateMemberMaxBackoffTime`.
const UPDATE_MEMBER_MAX_BACKOFF_TIME: Duration = Duration::from_millis(100);
/// Go `servicediscovery.UpdateMemberTimeout`.
const UPDATE_MEMBER_TIMEOUT: Duration = Duration::from_secs(1);

type StreamCtx = ConnectionCtx<Arc<QueryRegionStream>>;

/// Go `router.Cli`.
pub(crate) struct Cli {
    inner: Arc<Inner>,
    /// Go `wg`: the connection daemon and the dispatcher.
    workers: Mutex<Vec<JoinHandle<()>>>,
}

struct Inner {
    /// Go `ctx`.
    ctx: Cancellation,
    option: Arc<Options>,
    svc_discovery: Arc<dyn ServiceDiscovery>,
    /// Go `leaderURL`.
    leader_url: RwLock<String>,
    /// Go `conCtxMgr`: the router stream connection(s) to PD.
    con_ctx_mgr: Manager<Arc<QueryRegionStream>>,
    /// Go `updateConnectionCh` (capacity one: a stored permit).
    update_connection: Notify,
    /// Go `msDiscovery`.
    ms_discovery: Option<Arc<dyn ServiceDiscovery>>,
    /// Go `msConCtxMgr`: the router service stream connections.
    ms_con_ctx_mgr: Manager<Arc<QueryRegionStream>>,
    /// Go `bo`.
    bo: AsyncMutex<Backoffer>,
    /// Go `requestCh`.
    request_tx: mpsc::Sender<Request>,
}

impl Cli {
    /// Go `NewClient`: registers the leader-switched and members-changed
    /// callbacks and starts the connection daemon and the dispatcher.
    pub(crate) fn new(
        parent: &Cancellation,
        svc_discovery: Arc<dyn ServiceDiscovery>,
        ms_discovery: Option<Arc<dyn ServiceDiscovery>>,
        option: Arc<Options>,
    ) -> Arc<Self> {
        let (request_tx, request_rx) = mpsc::channel(DEFAULT_MAX_ROUTER_REQUEST_BATCH_SIZE * 2);
        let inner = Arc::new(Inner {
            ctx: parent.child(),
            option,
            leader_url: RwLock::new(svc_discovery.get_serving_url()),
            svc_discovery,
            con_ctx_mgr: Manager::new(),
            update_connection: Notify::new(),
            ms_discovery,
            ms_con_ctx_mgr: Manager::new(),
            bo: AsyncMutex::new(Backoffer::new(
                UPDATE_MEMBER_BACKOFF_BASE_TIME.as_nanos() as i64,
                UPDATE_MEMBER_MAX_BACKOFF_TIME.as_nanos() as i64,
                UPDATE_MEMBER_TIMEOUT.as_nanos() as i64,
            )),
            request_tx,
        });
        // The discoveries keep their callbacks for their whole life, as Go's
        // do; a callback of a dropped client does nothing.
        let leader_switched = Arc::downgrade(&inner);
        inner
            .svc_discovery
            .exec_and_add_leader_switched_callback(Arc::new(
                move |url: &str| match leader_switched.upgrade() {
                    Some(inner) => inner.update_leader_url(url),
                    None => Ok(()),
                },
            ));
        inner
            .svc_discovery
            .add_members_changed_callback(schedule_update_connection_callback(&inner));
        if let Some(ms_discovery) = &inner.ms_discovery {
            ms_discovery.add_members_changed_callback(schedule_update_connection_callback(&inner));
        }
        let daemon = tokio::spawn(go_goroutine(Arc::clone(&inner).connection_daemon()));
        let dispatcher = tokio::spawn(go_goroutine(Arc::clone(&inner).dispatcher(request_rx)));
        Arc::new(Self {
            inner,
            workers: Mutex::new(vec![daemon, dispatcher]),
        })
    }

    /// Go `Close`.
    pub(crate) async fn close(&self) {
        log::info!("[router] closing router client");
        self.inner.ctx.cancel();
        self.join().await;
        log::info!("[router] router client is closed");
    }

    /// Waits for the workers once the client's ctx is cancelled (Go's
    /// `wg.Wait()` after its parent ctx ends, which a closing PD client does
    /// without closing the router client).
    pub(crate) async fn join(&self) {
        let workers = std::mem::take(&mut *self.workers.lock().unwrap());
        for worker in workers {
            let _ = worker.await;
        }
    }

    /// Go `GetRegion`. `timeout` bounds the request ctx (`client.GetRegion`
    /// applies the client timeout).
    pub(crate) async fn get_region(
        &self,
        key: Vec<u8>,
        options: GetRegionOp,
        timeout: Duration,
    ) -> Result<Option<Region>> {
        let (mut request, wait) = Request::new(options);
        request.key = Some(key);
        self.dispatch(request, wait, timeout).await
    }

    /// Go `GetPrevRegion`.
    pub(crate) async fn get_prev_region(
        &self,
        key: Vec<u8>,
        options: GetRegionOp,
        timeout: Duration,
    ) -> Result<Option<Region>> {
        let (mut request, wait) = Request::new(options);
        request.prev_key = Some(key);
        self.dispatch(request, wait, timeout).await
    }

    /// Go `GetRegionByID`.
    pub(crate) async fn get_region_by_id(
        &self,
        region_id: u64,
        options: GetRegionOp,
        timeout: Duration,
    ) -> Result<Option<Region>> {
        let (mut request, wait) = Request::new(options);
        request.id = region_id;
        self.dispatch(request, wait, timeout).await
    }

    /// Go `c.requestCh <- req; return req.wait()`.
    async fn dispatch(
        &self,
        request: Request,
        wait: super::request::RequestWait,
        timeout: Duration,
    ) -> Result<Option<Region>> {
        let deadline = tokio::time::Instant::now() + timeout;
        let ctx = &self.inner.ctx;
        // A closed queue means the dispatcher exited (its client's ctx
        // ended); the dropped request leaves the waiter to its ctxs, as Go's
        // buffered send followed by `wait()` does.
        let _ = self.inner.request_tx.send(request).await;
        wait.wait(deadline, ctx).await
    }

    #[cfg(test)]
    pub(super) fn connection_urls_for_test(&self) -> (Vec<String>, Vec<String>) {
        let mut pd = self.inner.con_ctx_mgr.urls();
        pd.sort();
        let mut ms = self.inner.ms_con_ctx_mgr.urls();
        ms.sort();
        (pd, ms)
    }

    #[cfg(test)]
    pub(super) fn leader_url_for_test(&self) -> String {
        self.inner.get_leader_url()
    }
}

/// Runs a client goroutine. Go's runtime ends the process on a panic no
/// goroutine recovers (the dispatcher panics on an invalid request, and on
/// a response whose ID maps are shorter than the batch); the same panic here
/// takes the same effect, after Rust's panic hook has printed it.
async fn go_goroutine(task: impl std::future::Future<Output = ()>) {
    if AssertUnwindSafe(task).catch_unwind().await.is_err() {
        crate::logutil::unrecovered_panic_exit();
    }
}

fn schedule_update_connection_callback(
    inner: &Arc<Inner>,
) -> crate::pd::service_discovery::MembersChangedCallback {
    let inner = Arc::downgrade(inner);
    Arc::new(move || {
        if let Some(inner) = Weak::upgrade(&inner) {
            inner.schedule_update_connection();
        }
    })
}

/// Go `requestFinisher(resp)`: with a response, each request takes its region
/// (keys and previous keys through the response's ID maps, in order); without
/// one, every request fails -- with the given error, or as a connection
/// timeout when there is none.
fn request_finisher(
    response: Option<pdpb::QueryRegionResponse>,
) -> impl FnMut(usize, Request, Option<&Error>) + Send {
    let (mut key_idx, mut prev_key_idx) = (0, 0);
    move |_, mut request, error| {
        if let Some(error) = error {
            request.try_done(Err(crate::pd::tso::dup_error(error)));
            return;
        }
        let Some(response) = &response else {
            request.try_done(Err(errs::ERR_CLIENT_ROUTER_CONNECTION_TIMEOUT
                .error()
                .into()));
            return;
        };
        let id = if request.key.is_some() {
            let id = response.key_id_map[key_idx];
            key_idx += 1;
            id
        } else if request.prev_key.is_some() {
            let id = response.prev_key_id_map[prev_key_idx];
            prev_key_idx += 1;
            id
        } else {
            request.id
        };
        let region = response
            .regions_by_id
            .get(&id)
            .cloned()
            .and_then(Region::from_response)
            .map(|mut region| {
                // NeedBuckets is batch-wide; drop the buckets a request did
                // not ask for, as the unary path would not return them.
                if !request.options.need_buckets {
                    region.buckets = None;
                }
                region
            });
        request.try_done(Ok(region));
    }
}

/// Whether every collected request allows what `allowed` checks.
fn all_requests_allow(controller: &Controller<Request>, allowed: fn(&GetRegionOp) -> bool) -> bool {
    let mut all = true;
    controller.iter_collected_requests(|request| {
        all = allowed(&request.options);
        all
    });
    all
}

fn finish_collected_requests(
    controller: &mut Controller<Request>,
    response: Option<pdpb::QueryRegionResponse>,
    error: Option<&Error>,
) {
    let mut finisher = request_finisher(response);
    controller.finish_collected_requests(Some(&mut finisher as &mut Finisher<'_, Request>), error);
}

impl Inner {
    /// Go `getLeaderURL`.
    fn get_leader_url(&self) -> String {
        self.leader_url.read().unwrap().clone()
    }

    /// Go `updateLeaderURL`, the leader-switched callback.
    fn update_leader_url(&self, url: &str) -> Result<()> {
        let old_url = self.get_leader_url();
        if old_url == url {
            return Ok(());
        }
        *self.leader_url.write().unwrap() = url.to_owned();
        self.schedule_update_connection();
        log::info!(
            "old-url" = old_url.as_str(),
            "new-url" = url;
            "[router] switch the router leader serving url"
        );
        Ok(())
    }

    /// Go `getLeaderClientConn`.
    fn get_leader_client_conn(&self) -> (Option<Channel>, String) {
        let url = self.get_leader_url();
        if url.is_empty() {
            self.svc_discovery.schedule_check_member_changed();
            return (None, String::new());
        }
        let conn = self
            .svc_discovery
            .get_client_conns()
            .into_iter()
            .find(|(conn_url, _)| *conn_url == url)
            .map(|(_, conn)| conn);
        (conn, url)
    }

    /// Go `getAllClientConns`.
    fn get_all_client_conns(&self) -> HashMap<String, Channel> {
        self.svc_discovery
            .get_client_conns()
            .into_iter()
            .filter(|(url, _)| !url.is_empty())
            .collect()
    }

    /// Go `getAllMsClientConns`.
    fn get_all_ms_client_conns(&self) -> HashMap<String, Channel> {
        match &self.ms_discovery {
            Some(ms_discovery) => ms_discovery
                .get_client_conns()
                .into_iter()
                .filter(|(url, _)| !url.is_empty())
                .collect(),
            None => HashMap::new(),
        }
    }

    /// Go `scheduleUpdateConnection`.
    fn schedule_update_connection(&self) {
        self.update_connection.notify_one();
    }

    /// Opens a stream and its connection context under `ctx`.
    async fn open_stream(
        &self,
        kind: StreamKind,
        conn: Channel,
        url: &str,
        ctx: &Cancellation,
    ) -> (Cancellation, Result<Arc<StreamCtx>>) {
        let cctx = ctx.child();
        let stream = QueryRegionStream::open(kind, conn, &cctx, self.option.timeout).await;
        let cancel = cctx.clone();
        let stream = stream.map(|stream| {
            Arc::new(ConnectionCtx::new(
                cctx.clone(),
                move || cancel.cancel(),
                url.to_owned(),
                Arc::new(stream),
            ))
        });
        (cctx, stream)
    }

    /// Go `connectionDaemon`.
    async fn connection_daemon(self: Arc<Self>) {
        let updater_ctx = self.ctx.child();
        let mut update_ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + MEMBER_UPDATE_INTERVAL,
            MEMBER_UPDATE_INTERVAL,
        );
        update_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        log::info!("[router] connection daemon is started");
        loop {
            self.update_connection(&updater_ctx).await;
            self.update_ms_connection(&updater_ctx).await;
            let follower_handle_changed = async {
                self.option
                    .enable_follower_handle_ch
                    .receiver
                    .lock()
                    .await
                    .recv()
                    .await
            };
            tokio::select! {
                _ = updater_ctx.cancelled() => {
                    log::info!("[router] connection daemon is exiting");
                    updater_ctx.cancel();
                    return;
                }
                _ = follower_handle_changed => {
                    let enable_follower_handle = self.option.get_enable_follower_handle();
                    log::info!(
                        enable = enable_follower_handle;
                        "[router] follower handle status changed"
                    );
                }
                _ = update_ticker.tick() => {}
                _ = self.update_connection.notified() => {}
            }
        }
    }

    /// Go `updateMsConnection`.
    async fn update_ms_connection(&self, ctx: &Cancellation) {
        if self.ms_discovery.is_none() {
            return;
        }
        let conns = self.get_all_ms_client_conns();
        if conns.is_empty() {
            log::debug!("[router] no router service node found");
            return;
        }
        for (url, conn) in &conns {
            if self.ms_con_ctx_mgr.exist(url) {
                log::debug!(url = url.as_str(); "[router] the router service remains unchanged");
                continue;
            }
            let (cctx, stream) = self
                .open_stream(StreamKind::Router, conn.clone(), url, ctx)
                .await;
            match stream {
                Err(error) => {
                    log::warn!(
                        error:% = error;
                        "[router] failed to create the router service stream connection"
                    );
                    cctx.cancel();
                }
                Ok(stream) => {
                    if !self.ms_con_ctx_mgr.store(&stream, false) {
                        log::info!(
                            url = url.as_str();
                            "[router] already exists router service stream connection"
                        );
                        cctx.cancel();
                        continue;
                    }
                    log::info!(
                        url = url.as_str();
                        "[router] successfully established the router service stream connection"
                    );
                }
            }
        }
        self.ms_con_ctx_mgr.gc(|url| {
            if !conns.contains_key(url) {
                log::info!(url = url; "[router] release the stale router service stream connection");
                return true;
            }
            false
        });
    }

    /// Go `updateConnection`.
    async fn update_connection(&self, ctx: &Cancellation) {
        let (conn, url) = self.get_leader_client_conn();
        match conn {
            None => {
                log::warn!(url = url.as_str(); "[router] got an invalid leader client connection");
            }
            Some(_) if url.is_empty() => {
                log::warn!(url = url.as_str(); "[router] got an invalid leader client connection");
            }
            Some(_) if self.con_ctx_mgr.exist(&url) => {
                log::debug!(url = url.as_str(); "[router] the router leader remains unchanged");
            }
            Some(conn) => {
                let (cctx, stream) = self.open_stream(StreamKind::Pd, conn, &url, ctx).await;
                match stream {
                    Err(error) => {
                        log::error!(
                            error:% = error;
                            "[router] failed to create the leader router stream connection"
                        );
                        log::warn!("[router] failed to create the leader router stream connection");
                        cctx.cancel();
                    }
                    Ok(stream) => {
                        if !self.con_ctx_mgr.store(&stream, false) {
                            log::info!(
                                url = url.as_str();
                                "[router] already exists leader router stream connection"
                            );
                            cctx.cancel();
                            return;
                        }
                        log::info!(
                            url = url.as_str();
                            "[router] successfully established the leader router stream connection"
                        );
                    }
                }
            }
        }
        if self.option.get_enable_follower_handle() {
            let conns = self.get_all_client_conns();
            if conns.is_empty() {
                log::warn!("[router] no router node found");
                return;
            }
            for (url, conn) in &conns {
                if self.con_ctx_mgr.exist(url) {
                    log::debug!(url = url.as_str(); "[router] the router node remains unchanged");
                    continue;
                }
                let (cctx, stream) = self
                    .open_stream(StreamKind::Pd, conn.clone(), url, ctx)
                    .await;
                match stream {
                    Err(error) => {
                        log::error!(
                            error:% = error;
                            "[router] failed to create the router stream connection"
                        );
                        cctx.cancel();
                    }
                    Ok(stream) => {
                        if !self.con_ctx_mgr.store(&stream, false) {
                            log::info!(
                                url = url.as_str();
                                "[router] already exists router stream connection"
                            );
                            cctx.cancel();
                            continue;
                        }
                        log::info!(
                            url = url.as_str();
                            "[router] successfully established the router stream connection"
                        );
                    }
                }
            }
            self.con_ctx_mgr.gc(|url| {
                if !conns.contains_key(url) {
                    log::info!(url = url; "[router] release the stale router stream connection");
                    return true;
                }
                false
            });
        } else {
            let leader_url = self.get_leader_url();
            self.con_ctx_mgr.gc(|url| {
                if url != leader_url {
                    log::info!(url = url; "[router] release the non-leader router stream connection");
                    return true;
                }
                false
            });
        }
    }

    /// Go `dispatcher`.
    async fn dispatcher(self: Arc<Self>, mut request_rx: mpsc::Receiver<Request>) {
        let ctx = self.ctx.child();
        let mut controller = Controller::new(
            DEFAULT_MAX_ROUTER_REQUEST_BATCH_SIZE,
            Some(Box::new(request_finisher(None)) as Box<Finisher<'static, Request>>),
            Some(
                crate::pd::metrics::global_metrics()
                    .query_region_best_batch_size
                    .clone(),
            ),
        );
        log::info!("[router] dispatcher is started");
        'batch: loop {
            if ctx.is_cancelled() {
                break;
            }

            // Step 1: Fetch the pending router requests in batch.
            if let Err(error) = controller
                .fetch_pending_requests(&ctx, &mut request_rx, None, Duration::ZERO)
                .await
            {
                if matches!(error, Error::ContextCanceled) {
                    log::info!(
                        "[router] stop fetching the pending router requests due to context canceled"
                    );
                } else {
                    log::error!(
                        error:% = error;
                        "[router] failed to fetch the pending router requests"
                    );
                }
                break;
            }

            // Step 2: Choose a stream connection to send the router request.
            let timeout = tokio::time::Instant::now() + self.option.timeout;
            let (stream, stream_url) = loop {
                if ctx.is_cancelled() {
                    break 'batch;
                }
                if tokio::time::Instant::now() >= timeout {
                    log::error!(
                        "[router] router stream connection is not ready until timeout, abort the batch"
                    );
                    self.svc_discovery.schedule_check_member_changed();
                    let error = errs::ERR_CLIENT_ROUTER_CONNECTION_TIMEOUT.error().into();
                    finish_collected_requests(&mut controller, None, Some(&error));
                    continue 'batch;
                }
                let router_service_allowed =
                    all_requests_allow(&controller, |options| options.allow_router_service_handle);
                if let Some(chosen) = self.send_to_ms(&ctx, router_service_allowed).await {
                    break chosen;
                }
                let follower_allowed =
                    all_requests_allow(&controller, |options| options.allow_follower_handle);
                match self.send_to_pd(&ctx, follower_allowed).await {
                    Some(chosen) => break chosen,
                    // Go retries at once; yield so the retry loop cannot hold
                    // a runtime thread.
                    None => tokio::task::yield_now().await,
                }
            };

            // Step 3: Dispatch the router requests to the stream connection.
            if let Err(error) = self.process_requests_inner(&mut controller, &stream).await {
                if !self
                    .handle_process_request_error(&ctx, &mut controller, &stream_url, error)
                    .await
                {
                    break;
                }
            }
        }
        log::info!("[router] dispatcher is exiting");
        ctx.cancel();
        log::info!("[router] dispatcher exited");
    }

    /// Go `sendToPD`: the stream to use, or `None` to choose again.
    /// `batch_allows_follower` is whether every request of the batch allows
    /// a follower to handle it.
    async fn send_to_pd(
        &self,
        ctx: &Cancellation,
        batch_allows_follower: bool,
    ) -> Option<(Arc<QueryRegionStream>, String)> {
        let allow_follower_handle =
            self.option.get_enable_follower_handle() && batch_allows_follower;
        let connection_ctx = if allow_follower_handle {
            self.con_ctx_mgr.randomly_pick()
        } else {
            self.con_ctx_mgr.get_connection_ctx(&self.get_leader_url())
        };
        let Some(connection_ctx) = connection_ctx else {
            log::info!("[router] router stream connection is not ready");
            self.update_connection(ctx).await;
            return None;
        };
        if connection_ctx.ctx.is_cancelled() {
            log::info!(
                "stream-url" = connection_ctx.stream_url.as_str();
                "[router] router stream connection is canceled"
            );
            self.con_ctx_mgr.release(&connection_ctx.stream_url);
            return None;
        }
        Some((
            Arc::clone(&connection_ctx.stream),
            connection_ctx.stream_url.clone(),
        ))
    }

    /// Go `sendToMs`: a router service stream when every request of the
    /// batch allows the router service to handle it
    /// (`batch_allows_router_service`).
    async fn send_to_ms(
        &self,
        ctx: &Cancellation,
        batch_allows_router_service: bool,
    ) -> Option<(Arc<QueryRegionStream>, String)> {
        let allow_router_service_handle =
            !self.ms_con_ctx_mgr.is_empty() && batch_allows_router_service;
        if !(allow_router_service_handle && !self.ms_con_ctx_mgr.is_empty()) {
            return None;
        }
        let Some(connection_ctx) = self.ms_con_ctx_mgr.randomly_pick() else {
            log::info!("[router] router service stream connection is not ready");
            self.update_ms_connection(ctx).await;
            return None;
        };
        if connection_ctx.ctx.is_cancelled() {
            log::info!(
                "stream-url" = connection_ctx.stream_url.as_str();
                "[router] router service stream connection is canceled"
            );
            self.ms_con_ctx_mgr.release(&connection_ctx.stream_url);
            return None;
        }
        Some((
            Arc::clone(&connection_ctx.stream),
            connection_ctx.stream_url.clone(),
        ))
    }

    /// Go `processRequestsInner`.
    async fn process_requests_inner(
        &self,
        controller: &mut Controller<Request>,
        stream: &QueryRegionStream,
    ) -> Result<()> {
        let metrics = crate::pd::metrics::global_metrics();
        let requests = controller.get_collected_requests();
        let mut query = pdpb::QueryRegionRequest {
            header: Some(pdpb::RequestHeader {
                cluster_id: self.svc_discovery.get_cluster_id(),
                ..Default::default()
            }),
            keys: Vec::with_capacity(requests.len()),
            prev_keys: Vec::with_capacity(requests.len()),
            ids: Vec::with_capacity(requests.len()),
            need_buckets: false,
        };
        for request in requests {
            if !query.need_buckets && request.options.need_buckets {
                query.need_buckets = true;
            }
            if let Some(key) = &request.key {
                query.keys.push(key.clone());
            } else if let Some(prev_key) = &request.prev_key {
                query.prev_keys.push(prev_key.clone());
            } else if request.id != 0 {
                query.ids.push(request.id);
            } else {
                panic!("invalid region query request received");
            }
        }
        let request_count = requests.len();
        let (keys_len, prev_keys_len, ids_len) =
            (query.keys.len(), query.prev_keys.len(), query.ids.len());
        let start = Instant::now();
        if let Err(error) = stream.send(query) {
            metrics
                .request_failed_duration_query_region
                .observe(start.elapsed().as_secs_f64());
            return Err(error);
        }
        if let Some(batch_start) = controller.get_extra_batching_start_time() {
            metrics
                .query_region_batch_send_latency
                .observe(batch_start.elapsed().as_secs_f64());
        }
        let response = match stream.recv().await {
            Ok(response) => response,
            Err(error) => {
                metrics
                    .request_failed_duration_query_region
                    .observe(start.elapsed().as_secs_f64());
                return Err(error);
            }
        };
        metrics
            .request_duration_query_region
            .observe(start.elapsed().as_secs_f64());
        metrics
            .query_region_batch_size_total
            .observe(request_count as f64);
        // Header errors come from an unready PD leader or follower
        // (`NOT_BOOTSTRAPPED`, `REGION_NOT_FOUND`).
        if let Some(header_error) = response.header.as_ref().and_then(|h| h.error.as_ref()) {
            return Err(Error::StringError(crate::error::protobuf_text(
                header_error,
            )));
        }
        if keys_len > 0 {
            metrics
                .query_region_batch_size_by_keys
                .observe(keys_len as f64);
        }
        if prev_keys_len > 0 {
            metrics
                .query_region_batch_size_by_prev_keys
                .observe(prev_keys_len as f64);
        }
        if ids_len > 0 {
            metrics
                .query_region_batch_size_by_i_ds
                .observe(ids_len as f64);
        }
        finish_collected_requests(controller, Some(response), None);
        Ok(())
    }

    /// Go `handleProcessRequestError`: whether the dispatcher continues.
    async fn handle_process_request_error(
        &self,
        ctx: &Cancellation,
        controller: &mut Controller<Request>,
        stream_url: &str,
        error: Error,
    ) -> bool {
        log::error!(
            "stream-url" = stream_url,
            error:% = error;
            "[router] failed to process the router requests"
        );
        finish_collected_requests(controller, None, Some(&error));
        if ctx.is_cancelled() {
            return false;
        }
        self.con_ctx_mgr.release(stream_url);
        self.ms_con_ctx_mgr.release(stream_url);
        if errs::is_leader_change(Some(&error as &(dyn std::error::Error + 'static))) {
            // A new leader: check the members blockingly, so the next batch
            // goes to it.
            let mut bo = self.bo.lock().await;
            let context_done = async {
                ctx.cancelled().await;
                Error::ContextCanceled
            };
            let svc_discovery = &self.svc_discovery;
            if bo
                .exec(context_done, || svc_discovery.check_member_changed())
                .await
                .is_err()
                && ctx.is_cancelled()
            {
                return false;
            }
        } else {
            self.svc_discovery.schedule_check_member_changed();
            if let Some(ms_discovery) = &self.ms_discovery {
                ms_discovery.schedule_check_member_changed();
            }
        }
        true
    }
}
