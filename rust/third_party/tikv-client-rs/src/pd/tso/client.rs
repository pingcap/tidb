// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client `clients/tso/client.go`: the TSO client (`Cli`). It owns the
//! connection contexts (one stream per TSO server it talks to), keeps them
//! current from its service discovery's leader and members, falls back to a
//! follower forwarding to the leader after repeated network errors, and runs
//! one request dispatcher.

use std::sync::{Arc, Mutex, OnceLock, RwLock};

use async_trait::async_trait;
use rand::Rng;
use tokio::task::JoinHandle;

use super::dispatcher::{TsoDispatcher, TsoServiceProvider};
use super::request::Request;
use super::stream::{StreamBuilderFactory, TsoStream, TsoStreamBuilder};
use super::{
    is_network_or_canceled, trim_http_prefix, ServiceDiscovery, MAX_RETRY_TIMES, RETRY_INTERVAL,
};
use crate::async_util::Cancellation;
use crate::pd::connectionctx::{ConnectionCtx, Manager};
use crate::pd::errs;
use crate::pd::opt::Options;
use crate::proto::pdpb::Timestamp;
use crate::{Error, Result};

/// Go `servicediscovery.MemberUpdateInterval`.
const MEMBER_UPDATE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
/// Go health check interval in `checkLeader`.
const CHECK_LEADER_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
/// gRPC health `SERVING`.
const HEALTH_SERVING: i32 = 1;

/// The state Go's `Cli` shares with its dispatcher and background loops
/// (everything but the dispatcher itself, so they hold no cycle).
pub(crate) struct CliCore {
    ctx: Cancellation,
    option: Arc<Options>,
    /// Go `option.Timeout`. The owning client's request timeout, which a
    /// shared `Options` does not carry for every owner.
    timeout: std::time::Duration,
    svc_discovery: Arc<dyn ServiceDiscovery>,
    factory: StreamBuilderFactory,
    /// Go `leaderURL`.
    leader_url: RwLock<String>,
    /// Go `conCtxMgr`.
    con_ctx_mgr: Manager<Arc<TsoStream>>,
    /// Go `updateConCtxsCh`: one pending wake-up.
    update_con_ctxs: tokio::sync::Notify,
    /// Go `checkLeader` goroutines, joined on close.
    background: Mutex<Vec<JoinHandle<()>>>,
    self_ref: std::sync::Weak<CliCore>,
}

/// Go `tso.Cli`.
pub(crate) struct Cli {
    core: Arc<CliCore>,
    /// Go `dispatcher` (set once, `CompareAndSwap(nil, ...)`).
    dispatcher: OnceLock<Arc<TsoDispatcher>>,
    /// Go `wg`: the dispatcher loop and the connection-contexts updater.
    workers: Mutex<Vec<JoinHandle<()>>>,
    #[cfg(test)]
    close_gate: tokio::sync::Mutex<()>,
}

impl Cli {
    /// Go `NewClient`. The owner then calls [`Self::update_tso_leader_url`]
    /// with the current serving URL (the `Exec` of
    /// `ExecAndAddLeaderSwitchedCallback`) and [`Self::setup`].
    pub(crate) fn new(
        parent: &Cancellation,
        option: Arc<Options>,
        timeout: std::time::Duration,
        svc_discovery: Arc<dyn ServiceDiscovery>,
        factory: StreamBuilderFactory,
    ) -> Arc<Self> {
        Arc::new(Self {
            core: Arc::new_cyclic(|self_ref| CliCore {
                ctx: parent.child(),
                option,
                timeout,
                svc_discovery,
                factory,
                leader_url: RwLock::new(String::new()),
                con_ctx_mgr: Manager::new(),
                update_con_ctxs: tokio::sync::Notify::new(),
                background: Mutex::new(Vec::new()),
                self_ref: self_ref.clone(),
            }),
            dispatcher: OnceLock::new(),
            workers: Mutex::new(Vec::new()),
            #[cfg(test)]
            close_gate: tokio::sync::Mutex::new(()),
        })
    }

    /// The client's ctx (Go `c.ctx`, every request's `clientCtx`).
    pub(crate) fn ctx(&self) -> &Cancellation {
        &self.core.ctx
    }

    /// Go `Setup`.
    pub(crate) async fn setup(self: &Arc<Self>) {
        let core = Arc::clone(&self.core);
        self.workers
            .lock()
            .unwrap()
            .push(tokio::spawn(core.connection_ctxs_updater()));
        // Go checks the members synchronously here. The owner publishes a
        // client from inside its own membership refresh, so the check runs
        // beside it instead of waiting on that refresh; closing the client
        // abandons it.
        let check_core = Arc::clone(&self.core);
        self.workers.lock().unwrap().push(tokio::spawn(async move {
            let result = tokio::select! {
                result = check_core.svc_discovery.check_member_changed() => result,
                _ = check_core.ctx.cancelled() => return,
            };
            if let Err(error) = result {
                log::warn!(error:% = error; "[tso] failed to check member changed");
            }
        }));
        self.try_create_tso_dispatcher();
    }

    /// Go `Close`.
    pub(crate) async fn close(&self) {
        log::info!("[tso] closing tso client");
        self.core.ctx.cancel();
        let workers = std::mem::take(&mut *self.workers.lock().unwrap());
        for worker in workers {
            let _ = worker.await;
        }
        let background = std::mem::take(&mut *self.core.background.lock().unwrap());
        for task in background {
            let _ = task.await;
        }
        log::info!("[tso] close tso client");
        #[cfg(test)]
        drop(self.close_gate.lock().await);
        if let Some(dispatcher) = self.dispatcher.get() {
            dispatcher.close().await;
        }
        log::info!("[tso] tso client is closed");
    }

    /// Go `scheduleUpdateTSOConnectionCtxs` (the members-changed callback).
    pub(crate) fn schedule_update_tso_connection_ctxs(&self) {
        self.core.schedule_update_tso_connection_ctxs();
    }

    /// Go `updateTSOLeaderURL` (the leader-switched callback).
    pub(crate) fn update_tso_leader_url(self: &Arc<Self>, url: &str) {
        *self.core.leader_url.write().unwrap() = url.to_owned();
        log::info!("new-url" = url; "[tso] switch the tso leader serving url");
        self.try_create_tso_dispatcher();
        self.core.schedule_update_tso_connection_ctxs();
    }

    /// Go `getLeaderURL`.
    pub(crate) fn leader_url(&self) -> String {
        self.core.leader_url()
    }

    /// Go `tryCreateTSODispatcher`.
    fn try_create_tso_dispatcher(self: &Arc<Self>) {
        if self.dispatcher.get().is_some() || self.core.leader_url().is_empty() {
            return;
        }
        let provider: Arc<dyn TsoServiceProvider> = Arc::clone(&self.core) as _;
        let dispatcher = TsoDispatcher::new(&self.core.ctx, provider);
        if self.dispatcher.set(Arc::clone(&dispatcher)).is_err() {
            return;
        }
        self.workers
            .lock()
            .unwrap()
            .push(tokio::spawn(dispatcher.handle_dispatcher()));
    }

    /// Go `DispatchRequest`: `Err((retryable, error, request))`. The request
    /// comes back when it never reached the dispatcher; after the hand-off
    /// it belongs to the dispatcher, which finishes it.
    pub(crate) async fn dispatch_request(
        &self,
        request: Request,
    ) -> std::result::Result<(), (bool, Error, Option<Request>)> {
        let Some(dispatcher) = self.dispatcher.get() else {
            let error = errs::client_get_tso("tso dispatcher is not ready");
            log::warn!(error:% = error; "[tso] dispatch tso request error");
            self.core.svc_discovery.schedule_check_member_changed();
            // A new dispatcher could be created in the meantime.
            return Err((true, error.into(), Some(request)));
        };
        if request.request_ctx.is_cancelled() {
            // The caller cancelled: no retry.
            return Err((false, Error::ContextCanceled, Some(request)));
        }
        if self.core.ctx.is_cancelled() {
            // Closed for a service-mode switch: retryable.
            return Err((true, Error::ContextCanceled, Some(request)));
        }
        let request_ctx = request.request_ctx.clone();
        if let Err(request) = dispatcher.push(request).await {
            return Err((true, Error::ContextCanceled, Some(request)));
        }
        // The request may have reached a closed dispatcher; never retry then.
        if request_ctx.is_cancelled() || self.core.ctx.is_cancelled() {
            return Err((false, Error::ContextCanceled, None));
        }
        Ok(())
    }

    /// pd-client `GetTSAsync(ctx).Wait()` on this client alone.
    #[cfg(test)]
    pub(crate) async fn get_timestamp(self: &Arc<Self>) -> Result<Timestamp> {
        let client = Arc::clone(self);
        get_ts_async(move || Some(Arc::clone(&client))).await
    }

    #[cfg(test)]
    pub(crate) fn release_all_for_test(&self) {
        self.core.con_ctx_mgr.release_all();
    }

    #[cfg(test)]
    pub(crate) fn is_closed_for_test(&self) -> bool {
        self.core.ctx.is_cancelled() && self.workers.lock().unwrap().is_empty()
    }

    #[cfg(test)]
    pub(crate) fn close_gate_for_test(&self) -> &tokio::sync::Mutex<()> {
        &self.close_gate
    }

    #[cfg(test)]
    pub(crate) fn dispatcher_for_test(&self) -> Option<Arc<TsoDispatcher>> {
        self.dispatcher.get().cloned()
    }

    #[cfg(test)]
    pub(crate) fn connection_urls_for_test(&self) -> Vec<String> {
        self.core.con_ctx_mgr.urls()
    }
}

/// pd-client `innerClient.dispatchTSORequestWithRetry` + `Request.Wait`, over
/// the TSO client the caller's `current` yields on each attempt (the client
/// can be replaced in between by a service-mode switch).
pub(crate) async fn get_ts_async<F>(mut current: F) -> Result<Timestamp>
where
    F: FnMut() -> Option<Arc<Cli>>,
{
    const DISPATCH_RETRY_COUNT: usize = 2;
    const DISPATCH_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
    let (request, future) = Request::new();
    // pd-client `GetTSAsync`: its span covers handing the request to the
    // dispatcher; waiting on the future opens none.
    let dispatched = crate::trace::detached_client_span("pdclient.GetTSAsync", async move {
        let mut request = Some(request);
        let mut last: Option<(Arc<Cli>, Option<Error>)> = None;
        for attempt in 0..DISPATCH_RETRY_COUNT {
            if attempt > 0 {
                tokio::time::sleep(DISPATCH_RETRY_DELAY).await;
            }
            // The client may be created or switched meanwhile.
            let Some(client) = current() else {
                last = None;
                continue;
            };
            let Some(pending) = request.take() else {
                break;
            };
            match client.dispatch_request(pending).await {
                Ok(()) => return Some((client, None)),
                Err((retryable, error, returned)) => {
                    request = returned;
                    last = Some((client, Some(error)));
                    if !retryable {
                        break;
                    }
                }
            }
        }
        match (last, request) {
            // Go `req.TryDone(err)`: the request carries the last error.
            (Some((client, Some(error))), Some(mut request)) => {
                request.try_done(Err(error));
                Some((client, None))
            }
            (Some((client, error)), None) => Some((client, error)),
            _ => None,
        }
    })
    .await;
    match dispatched {
        Some((client, _)) => future.wait(client.ctx()).await,
        None => Err(errs::client_get_tso("tso client is nil").into()),
    }
}

impl CliCore {
    fn leader_url(&self) -> String {
        self.leader_url.read().unwrap().clone()
    }

    fn schedule_update_tso_connection_ctxs(&self) {
        self.update_con_ctxs.notify_one();
    }

    /// Go `getTSOLeaderClientConn`.
    async fn get_tso_leader_client_conn(&self) -> (Option<tonic::transport::Channel>, String) {
        let url = self.leader_url();
        if url.is_empty() {
            log::error!("[tso] the tso leader should exist");
            return (None, url);
        }
        match self.svc_discovery.get_or_create_grpc_conn(&url).await {
            Ok(channel) => (Some(channel), url),
            Err(error) => {
                log::warn!(
                    "serving-url" = url.as_str(),
                    error:% = error;
                    "[tso] failed to get tso leader client connection"
                );
                (None, url)
            }
        }
    }

    async fn health_serving(&self, channel: tonic::transport::Channel) -> bool {
        crate::pd::service_discovery::health_check(channel, self.timeout)
            .await
            .is_ok_and(|status| status == HEALTH_SERVING)
    }

    /// Go `backupClientConn`: a random healthy backup endpoint.
    async fn backup_client_conn(&self) -> Option<(tonic::transport::Channel, String)> {
        let urls = self.svc_discovery.get_backup_urls();
        if urls.is_empty() {
            return None;
        }
        for _ in 0..urls.len() {
            let url = urls[rand::thread_rng().gen_range(0..urls.len())].clone();
            let Ok(channel) = self.svc_discovery.get_or_create_grpc_conn(&url).await else {
                continue;
            };
            if self.health_serving(channel.clone()).await {
                return Some((channel, url));
            }
        }
        None
    }

    fn connection(
        ctx: Cancellation,
        url: &str,
        stream: Arc<TsoStream>,
    ) -> Arc<ConnectionCtx<Arc<TsoStream>>> {
        let cancel = ctx.clone();
        Arc::new(ConnectionCtx::new(
            ctx,
            move || cancel.cancel(),
            url.to_owned(),
            stream,
        ))
    }

    /// Go `connectionCtxsUpdater`.
    async fn connection_ctxs_updater(self: Arc<Self>) {
        log::info!("[tso] start tso connection contexts updater");
        let ctx = self.ctx.child();
        let mut ticker: Option<tokio::time::Interval> = None;
        let new_ticker = || {
            let mut ticker = tokio::time::interval_at(
                tokio::time::Instant::now() + MEMBER_UPDATE_INTERVAL,
                MEMBER_UPDATE_INTERVAL,
            );
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker
        };
        if self.option.get_enable_tso_follower_proxy() {
            ticker = Some(new_ticker());
        }
        loop {
            self.update_connection_ctxs(&ctx).await;
            let proxy_changed = async {
                self.option
                    .enable_tso_follower_proxy_ch
                    .receiver
                    .lock()
                    .await
                    .recv()
                    .await
            };
            let tick = async {
                match ticker.as_mut() {
                    Some(ticker) => {
                        ticker.tick().await;
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = ctx.cancelled() => {
                    log::info!("[tso] exit tso connection contexts updater");
                    return;
                }
                _ = proxy_changed => {
                    let enable = self.option.get_enable_tso_follower_proxy();
                    log::info!(enable = enable; "[tso] tso follower proxy status changed");
                    if enable && ticker.is_none() {
                        ticker = Some(new_ticker());
                    } else if !enable && ticker.is_some() {
                        ticker = None;
                    }
                }
                _ = tick => {}
                _ = self.update_con_ctxs.notified() => {}
            }
        }
    }

    /// Go `tryConnectToTSO`: the leader's stream, or after `MaxRetryTimes`
    /// network failures a healthy follower forwarding to it.
    async fn try_connect_to_tso(self: &Arc<Self>, ctx: &Cancellation) -> Result<()> {
        let mut network_err_num = 0_u64;
        let mut last_error: Option<Error> = None;
        let mut url = String::new();
        for attempt in 0..MAX_RETRY_TIMES {
            if attempt > 0 {
                tokio::select! {
                    _ = ctx.cancelled() => {
                        return match last_error {
                            Some(error) => Err(error),
                            None => Ok(()),
                        };
                    }
                    _ = tokio::time::sleep(RETRY_INTERVAL) => {}
                }
            }
            self.svc_discovery.schedule_check_member_changed();
            let (channel, leader) = self.get_tso_leader_client_conn().await;
            url = leader;
            if self.con_ctx_mgr.exist(&url) {
                // Only clean up the stale connection contexts.
                let current = url.clone();
                self.con_ctx_mgr.gc(|stream_url| stream_url != current);
                return Ok(());
            }
            let Some(channel) = channel else {
                network_err_num += 1;
                continue;
            };
            let cctx = ctx.child();
            match self
                .factory
                .make_builder(channel, &url)
                .build(&cctx, None, self.timeout)
                .await
            {
                Ok(stream) => {
                    let connection = Self::connection(cctx.clone(), &url, stream);
                    if !self.con_ctx_mgr.clean_all_and_store(&connection) {
                        cctx.cancel();
                    }
                    return Ok(());
                }
                Err(error) => {
                    // A stream whose construction was cancelled by its own
                    // timeout counts as a network error too.
                    if self.option.enable_forwarding && is_network_or_canceled(&error) {
                        network_err_num += 1;
                    }
                    last_error = Some(error);
                    cctx.cancel();
                }
            }
        }

        if network_err_num == MAX_RETRY_TIMES {
            if let Some((backup, backup_url)) = self.backup_client_conn().await {
                log::info!(
                    "follower-url" = backup_url.as_str();
                    "[tso] fall back to use follower to forward tso stream"
                );
                let forwarded_host = self.leader_url();
                if forwarded_host.is_empty() {
                    return Err(Error::StringError("cannot find the tso leader".to_owned()));
                }
                let cctx = ctx.child();
                match self
                    .factory
                    .make_builder(backup, &backup_url)
                    .build(&cctx, Some(forwarded_host.clone()), self.timeout)
                    .await
                {
                    Ok(stream) => {
                        let forwarded_host_trim = trim_http_prefix(&forwarded_host).to_owned();
                        let addr = trim_http_prefix(&backup_url).to_owned();
                        let connection = Self::connection(cctx.clone(), &backup_url, stream);
                        if !self.con_ctx_mgr.clean_all_and_store(&connection) {
                            cctx.cancel();
                            return Ok(());
                        }
                        let core = Arc::clone(self);
                        let check_ctx = ctx.clone();
                        let leader_url = url.clone();
                        let handle = tokio::spawn(async move {
                            core.check_leader(
                                check_ctx,
                                cctx,
                                forwarded_host_trim,
                                addr,
                                leader_url,
                            )
                            .await
                        });
                        self.background.lock().unwrap().push(handle);
                        crate::pd::metrics::global_metrics()
                            .request_forwarded
                            .with_label_values(&[
                                trim_http_prefix(&forwarded_host),
                                trim_http_prefix(&backup_url),
                            ])
                            .set(1.0);
                        return Ok(());
                    }
                    Err(error) => {
                        cctx.cancel();
                        last_error = Some(error);
                    }
                }
            }
        }
        match last_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Go `checkLeader`: switch back to the leader's own stream once it is
    /// healthy, or stop when the leader changes.
    async fn check_leader(
        self: Arc<Self>,
        ctx: Cancellation,
        forward_ctx: Cancellation,
        forwarded_host_trim: String,
        addr: String,
        url: String,
    ) {
        let (mut channel, mut current) = self.get_tso_leader_client_conn().await;
        loop {
            if current != url {
                log::info!(
                    origin = url.as_str(),
                    new = current.as_str();
                    "[tso] the tso leader is changed"
                );
                break;
            }
            if let Some(leader) = channel.clone() {
                if self.health_serving(leader.clone()).await {
                    let cctx = ctx.child();
                    if let Ok(stream) = self
                        .factory
                        .make_builder(leader, &url)
                        .build(&cctx, None, self.timeout)
                        .await
                    {
                        log::info!(
                            url = url.as_str();
                            "[tso] recover the original tso stream since the network has become normal"
                        );
                        let connection = Self::connection(cctx.clone(), &url, stream);
                        if !self.con_ctx_mgr.clean_all_and_store(&connection) {
                            cctx.cancel();
                        }
                        break;
                    }
                }
            }
            tokio::select! {
                _ = ctx.cancelled() => break,
                _ = tokio::time::sleep(CHECK_LEADER_INTERVAL) => {
                    (channel, current) = self.get_tso_leader_client_conn().await;
                }
            }
        }
        // Go's deferred clean-up: cancel the forward stream.
        forward_ctx.cancel();
        crate::pd::metrics::global_metrics()
            .request_forwarded
            .with_label_values(&[&forwarded_host_trim, &addr])
            .set(0.0);
    }

    /// Go `tryConnectToTSOWithProxy`: a stream to every healthy service
    /// endpoint, followers forwarding to the leader.
    async fn try_connect_to_tso_with_proxy(self: &Arc<Self>, ctx: &Cancellation) -> Result<()> {
        let builders = self.get_all_tso_stream_builders().await;
        let leader_addr = self.svc_discovery.get_serving_url();
        let forwarded_host = self.leader_url();
        if forwarded_host.is_empty() {
            return Err(Error::StringError("cannot find the tso leader".to_owned()));
        }
        // GC the stale ones.
        self.con_ctx_mgr.gc(|addr| {
            let stale = !builders.iter().any(|(url, _)| url == addr);
            if stale {
                log::info!(addr = addr; "[tso] remove the stale tso stream");
            }
            stale
        });
        // Create the missing ones.
        for (addr, builder) in builders {
            if self.con_ctx_mgr.exist(&addr) {
                continue;
            }
            log::info!(addr = addr.as_str(); "[tso] try to create tso stream");
            let cctx = ctx.child();
            // Do not proxy the leader client.
            let forward = (addr != leader_addr).then(|| {
                log::info!(
                    addr = addr.as_str();
                    "[tso] use follower to forward tso stream to do the proxy"
                );
                forwarded_host.clone()
            });
            match builder.build(&cctx, forward.clone(), self.timeout).await {
                Ok(stream) => {
                    if forward.is_some() {
                        crate::pd::metrics::global_metrics()
                            .request_forwarded
                            .with_label_values(&[
                                trim_http_prefix(&forwarded_host),
                                trim_http_prefix(&addr),
                            ])
                            .set(1.0);
                    }
                    let connection = Self::connection(cctx.clone(), &addr, stream);
                    if !self.con_ctx_mgr.store(&connection, false) {
                        log::info!(addr = addr.as_str(); "[tso] already exists tso stream");
                        cctx.cancel();
                    }
                }
                Err(error) => {
                    log::error!(
                        addr = addr.as_str(),
                        error:% = error;
                        "[tso] create the tso stream failed"
                    );
                    cctx.cancel();
                }
            }
        }
        Ok(())
    }

    /// Go `getAllTSOStreamBuilders`: a builder for every healthy endpoint.
    async fn get_all_tso_stream_builders(&self) -> Vec<(String, TsoStreamBuilder)> {
        let mut builders: Vec<(String, TsoStreamBuilder)> = Vec::new();
        for addr in self.svc_discovery.get_service_urls() {
            if builders.iter().any(|(url, _)| url == &addr) {
                continue;
            }
            let Ok(channel) = self.svc_discovery.get_or_create_grpc_conn(&addr).await else {
                continue;
            };
            if self.health_serving(channel.clone()).await {
                let builder = self.factory.make_builder(channel, &addr);
                builders.push((addr, builder));
            }
        }
        builders
    }
}

#[async_trait]
impl TsoServiceProvider for CliCore {
    fn get_option(&self) -> &Options {
        &self.option
    }

    fn get_timeout(&self) -> std::time::Duration {
        self.timeout
    }

    fn get_service_discovery(&self) -> &Arc<dyn ServiceDiscovery> {
        &self.svc_discovery
    }

    fn get_connection_ctx_mgr(&self) -> &Manager<Arc<TsoStream>> {
        &self.con_ctx_mgr
    }

    /// Go `updateConnectionCtxs`.
    async fn update_connection_ctxs(&self, ctx: &Cancellation) -> bool {
        // The connect paths spawn `checkLeader` with the core, so they need
        // it shared; the provider is only ever reached through an Arc.
        let core = self.arc();
        let result = if self.option.get_enable_tso_follower_proxy() {
            core.try_connect_to_tso_with_proxy(ctx).await
        } else {
            core.try_connect_to_tso(ctx).await
        };
        if let Err(error) = result {
            log::warn!(error:% = error; "[tso] update connection contexts failed");
            return false;
        }
        true
    }
}

impl CliCore {
    fn arc(&self) -> Arc<Self> {
        self.self_ref
            .upgrade()
            .expect("a CliCore is always owned by its Cli")
    }
}
