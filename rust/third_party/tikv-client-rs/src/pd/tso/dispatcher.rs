// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client `clients/tso/dispatcher.go`: one dispatcher per TSO client
//! collects requests into batches, picks a stream from the connection-context
//! manager, starts the batch's deadline and hands it to the stream; the
//! stream's receive loop finishes the batch.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use tokio::sync::{mpsc, Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};

use super::request::Request;
use super::stream::{TsoRequestResult, TsoStream};
use super::{dup_error, ServiceDiscovery, DEFAULT_MAX_TSO_BATCH_SIZE, RETRY_INTERVAL};
use crate::async_util::Cancellation;
use crate::pd::batch::Controller;
use crate::pd::connectionctx::Manager;
use crate::pd::deadline::{DeadlineDone, Watcher};
use crate::pd::errs;
use crate::pd::opt::Options;
use crate::pd::tso_batch::TsoBatch;
use crate::proto::pdpb;
use crate::Error;

/// Go `tsoServiceProvider`, implemented by the TSO client.
#[async_trait]
pub(crate) trait TsoServiceProvider: Send + Sync {
    fn get_option(&self) -> &Options;
    /// Go `getOption().Timeout`.
    fn get_timeout(&self) -> Duration;
    fn get_service_discovery(&self) -> &Arc<dyn ServiceDiscovery>;
    fn get_connection_ctx_mgr(&self) -> &Manager<Arc<TsoStream>>;
    async fn update_connection_ctxs(&self, ctx: &Cancellation) -> bool;
}

/// Go `tsoInfo`.
#[derive(Clone, Debug)]
pub(crate) struct TsoInfo {
    pub(crate) tso_server: String,
    req_keyspace_group_id: u32,
    resp_keyspace_group_id: u32,
    resp_received_at: SystemTime,
    pub(crate) physical: i64,
    pub(crate) logical: i64,
    source_stream_id: String,
}

#[cfg(test)]
impl TsoInfo {
    pub(crate) fn for_test(physical: i64, logical: i64, resp_keyspace_group_id: u32) -> Self {
        Self {
            tso_server: "test".to_owned(),
            req_keyspace_group_id: resp_keyspace_group_id,
            resp_keyspace_group_id,
            resp_received_at: SystemTime::now(),
            physical,
            logical,
            source_stream_id: "test-1".to_owned(),
        }
    }
}

/// Go `dispatcherCheckRPCConcurrencyInterval`.
pub(crate) const DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL: Duration = Duration::from_secs(5);
/// Go `tokenChCapacity`.
const TOKEN_CH_CAPACITY: usize = 64;

type BatchController = Controller<Request>;

/// Go `tsoDispatcher`.
pub(crate) struct TsoDispatcher {
    ctx: Cancellation,
    provider: Arc<dyn TsoServiceProvider>,
    request_tx: mpsc::Sender<Request>,
    request_rx: AsyncMutex<mpsc::Receiver<Request>>,
    deadline_watcher: Watcher,
    latest_tso_info: Mutex<Option<TsoInfo>>,
    /// Go `batchBufferPool`.
    batch_buffer_pool: Mutex<Vec<BatchController>>,
    /// Go `tokenCh`: a token must be held while an RPC is in flight.
    tokens: Arc<Semaphore>,
    concurrency: AsyncMutex<RpcConcurrency>,
}

/// Go `tsoDispatcher`'s `lastCheckConcurrencyTime`, `tokenCount` and
/// `rpcConcurrency`, which only the dispatcher loop touches.
pub(crate) struct RpcConcurrency {
    pub(crate) last_check_concurrency_time: Option<Instant>,
    pub(crate) token_count: usize,
    pub(crate) rpc_concurrency: isize,
}

fn new_batch_controller() -> BatchController {
    Controller::new(
        DEFAULT_MAX_TSO_BATCH_SIZE * 2,
        Some(Box::new(|index, request, error| {
            tso_request_finisher(0, 0, None)(index, request, error)
        })),
        Some(crate::stats::pd_tso_best_batch_size_observer()),
    )
}

/// Go `tsoRequestFinisher(physical, firstLogical, streamID)`.
fn tso_request_finisher(
    physical: i64,
    first_logical: i64,
    suffix_bits: Option<u32>,
) -> impl FnMut(usize, Request, Option<&Error>) + Send {
    move |index, mut request, error| {
        let result = match error {
            Some(error) => Err(dup_error(error)),
            None => Ok(pdpb::Timestamp {
                physical,
                logical: first_logical + index as i64,
                suffix_bits: suffix_bits.unwrap_or_default(),
            }),
        };
        request.try_done(result);
    }
}

impl TsoDispatcher {
    /// Go `newTSODispatcher`.
    pub(crate) fn new(ctx: &Cancellation, provider: Arc<dyn TsoServiceProvider>) -> Arc<Self> {
        let ctx = ctx.child();
        let (request_tx, request_rx) = mpsc::channel(DEFAULT_MAX_TSO_BATCH_SIZE * 2);
        Arc::new(Self {
            deadline_watcher: Watcher::new(&ctx, TOKEN_CH_CAPACITY, "tso"),
            ctx,
            provider,
            request_tx,
            request_rx: AsyncMutex::new(request_rx),
            latest_tso_info: Mutex::new(None),
            batch_buffer_pool: Mutex::new(Vec::new()),
            tokens: Arc::new(Semaphore::new(0)),
            concurrency: AsyncMutex::new(RpcConcurrency {
                last_check_concurrency_time: None,
                token_count: 0,
                rpc_concurrency: 0,
            }),
        })
    }

    /// Go `revokePendingRequests`.
    fn revoke_pending_requests(request_rx: &mut mpsc::Receiver<Request>, error: &Error) {
        while let Ok(mut request) = request_rx.try_recv() {
            request.try_done(Err(dup_error(error)));
        }
    }

    /// Go `close`.
    pub(crate) async fn close(&self) {
        self.ctx.cancel();
        let error: Error = errs::ERR_CLOSING.error().with_stack().into();
        // The loop revokes on exit when it still holds the queue.
        if let Ok(mut request_rx) = self.request_rx.try_lock() {
            Self::revoke_pending_requests(&mut request_rx, &error);
        }
        self.deadline_watcher.close().await;
    }

    /// Go `push`.
    pub(crate) async fn push(&self, request: Request) -> std::result::Result<(), Request> {
        self.request_tx.send(request).await.map_err(|error| error.0)
    }

    fn get_batch_controller(&self) -> BatchController {
        self.batch_buffer_pool
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(new_batch_controller)
    }

    fn put_batch_controller(&self, controller: BatchController) {
        self.batch_buffer_pool.lock().unwrap().push(controller);
    }

    /// Go `handleDispatcher`.
    pub(crate) async fn handle_dispatcher(self: Arc<Self>) {
        let ctx = self.ctx.clone();
        let provider = Arc::clone(&self.provider);
        let option = provider.get_option();
        let svc_discovery = provider.get_service_discovery();
        let con_ctx_mgr = provider.get_connection_ctx_mgr();
        log::info!("[tso] tso dispatcher created");
        let mut request_rx = self.request_rx.lock().await;
        let mut tso_batch_controller: Option<BatchController> = None;
        let mut bo = crate::pd::backoff::Backoffer::new(
            super::UPDATE_MEMBER_BACKOFF_BASE_TIME.as_nanos() as i64,
            super::UPDATE_MEMBER_MAX_BACKOFF_TIME.as_nanos() as i64,
            super::UPDATE_MEMBER_TIMEOUT.as_nanos() as i64,
        );
        let mut stream: Option<Arc<crate::pd::connectionctx::ConnectionCtx<Arc<TsoStream>>>> = None;

        'tso_batch_loop: loop {
            if ctx.is_cancelled() {
                break;
            }
            let controller =
                tso_batch_controller.get_or_insert_with(|| self.get_batch_controller());
            let max_batch_wait_interval = option.get_max_tso_batch_wait_interval();
            let current_batch_start_time = Instant::now();
            if self
                .check_tso_rpc_concurrency(&ctx, max_batch_wait_interval, current_batch_start_time)
                .await
                .is_err()
            {
                log::info!(
                    error = "context canceled";
                    "[tso] stop checking tso rpc concurrency configurations due to context canceled"
                );
                break;
            }
            let permit = match controller
                .fetch_pending_requests(
                    &ctx,
                    &mut *request_rx,
                    Some(&self.tokens),
                    max_batch_wait_interval,
                )
                .await
            {
                Ok(permit) => permit,
                Err(Error::ContextCanceled) => {
                    log::info!(
                        "[tso] stop fetching the pending tso requests due to context canceled"
                    );
                    break;
                }
                Err(error) => {
                    log::error!(
                        error:% = errs::client_get_tso(&error);
                        "[tso] fetch pending tso requests error"
                    );
                    break;
                }
            };
            controller.adjust_best_batch_size();
            let stream_loop_deadline = tokio::time::Instant::now() + provider.get_timeout();

            // Choose a stream to send the TSO gRPC request.
            let connection = 'stream_choosing_loop: loop {
                if let Some(connection) = con_ctx_mgr.randomly_pick() {
                    stream = Some(connection);
                }
                let Some(connection) = stream.clone() else {
                    log::info!("[tso] tso stream is not ready");
                    if provider.update_connection_ctxs(&ctx).await {
                        continue 'stream_choosing_loop;
                    }
                    tokio::select! {
                        _ = ctx.cancelled() => {
                            self.cancel_collected_requests(controller, permit, Error::ContextCanceled);
                            break 'tso_batch_loop;
                        }
                        _ = tokio::time::sleep_until(stream_loop_deadline) => {
                            let error = errs::client_create_tso_stream(errs::RETRY_TIMEOUT_ERR);
                            log::warn!(error:% = error; "[tso] create tso stream error");
                            svc_discovery.schedule_check_member_changed();
                            self.cancel_collected_requests(controller, permit, error.with_stack().into());
                            continue 'tso_batch_loop;
                        }
                        _ = tokio::time::sleep(RETRY_INTERVAL) => continue 'stream_choosing_loop,
                    }
                };
                if connection.ctx.is_cancelled() {
                    log::info!(
                        "stream-url" = connection.stream_url.as_str();
                        "[tso] tso stream is canceled"
                    );
                    con_ctx_mgr.release(&connection.stream_url);
                    stream = None;
                    continue;
                }
                // An error the stream met receiving asynchronously.
                if let Some(error) = connection.stream.get_recv_error() {
                    let exit = !self
                        .handle_process_request_error(&ctx, &mut bo, &connection.stream_url, &error)
                        .await;
                    stream = None;
                    if exit {
                        self.cancel_collected_requests(controller, permit, error);
                        break 'tso_batch_loop;
                    }
                    continue;
                }
                break connection;
            };

            // With concurrent RPCs, each batch collects for estimated RPC
            // latency / concurrency, counted from the start of collection.
            let rpc_concurrency = self.concurrency.lock().await.rpc_concurrency;
            if rpc_concurrency > 1 {
                let estimated_latency = connection.stream.estimated_rpc_latency();
                let goal_batch_time = estimated_latency / rpc_concurrency as u32;
                let remaining_batch_time =
                    goal_batch_time.saturating_sub(current_batch_start_time.elapsed());
                if !remaining_batch_time.is_zero() {
                    let timer = tokio::time::sleep(remaining_batch_time);
                    tokio::pin!(timer);
                    if let Err(error) = controller
                        .fetch_requests_with_timer(&ctx, &mut *request_rx, timer)
                        .await
                    {
                        log::info!(
                            error:% = error;
                            "[tso] stop fetching the pending tso requests due to context canceled"
                        );
                        self.cancel_collected_requests(controller, permit, error);
                        break 'tso_batch_loop;
                    }
                }
            }

            let deadline_target = Arc::clone(&connection);
            let Some(done) = self
                .deadline_watcher
                .start(&ctx, provider.get_timeout(), move || {
                    deadline_target.cancel()
                })
                .await
            else {
                self.cancel_collected_requests(controller, permit, Error::ContextCanceled);
                break;
            };
            let controller = tso_batch_controller
                .take()
                .expect("a batch controller collected this batch");
            // processRequests guarantees the collected requests finish.
            if let Err(error) = self.process_requests(&connection.stream, controller, permit, done)
            {
                let exit = !self
                    .handle_process_request_error(&ctx, &mut bo, &connection.stream_url, &error)
                    .await;
                stream = None;
                if exit {
                    break;
                }
            }
        }

        // Go's deferred clean-up.
        log::info!("[tso] exit tso dispatcher");
        con_ctx_mgr.release_all();
        if let Some(mut controller) = tso_batch_controller.take() {
            if controller.get_collected_request_count() != 0 {
                controller.finish_collected_requests(None, Some(&Error::ContextCanceled));
            }
        }
        let error: Error = errs::ERR_CLOSING.error().with_stack().into();
        Self::revoke_pending_requests(&mut request_rx, &error);
    }

    /// Go `handleProcessRequestError`: true when the loop may continue.
    async fn handle_process_request_error(
        &self,
        ctx: &Cancellation,
        bo: &mut crate::pd::backoff::Backoffer,
        stream_url: &str,
        error: &Error,
    ) -> bool {
        log::error!(
            "stream-url" = stream_url,
            error:% = errs::client_get_tso(error);
            "[tso] getTS error after processing requests"
        );
        if ctx.is_cancelled() {
            return false;
        }
        let con_ctx_mgr = self.provider.get_connection_ctx_mgr();
        con_ctx_mgr.release(stream_url);
        let svc_discovery = Arc::clone(self.provider.get_service_discovery());
        let error_ref: &(dyn std::error::Error + 'static) = error;
        if errs::is_callee_mismatch(Some(error_ref)) {
            svc_discovery.remove_client_conn(stream_url);
        } else if errs::is_leader_change(Some(error_ref)) {
            let ctx_done = {
                let ctx = ctx.clone();
                async move {
                    ctx.cancelled().await;
                    Error::ContextCanceled
                }
            };
            if let Err(error) = bo
                .exec(ctx_done, || {
                    let svc_discovery = Arc::clone(&svc_discovery);
                    let ctx = ctx.clone();
                    // Closing the client must not wait on a membership check.
                    async move {
                        tokio::select! {
                            result = svc_discovery.check_member_changed() => result,
                            _ = ctx.cancelled() => Err(Error::ContextCanceled),
                        }
                    }
                })
                .await
            {
                log::error!(
                    error:% = error;
                    "[tso] check member changed error after the leader changed"
                );
            }
        } else {
            svc_discovery.schedule_check_member_changed();
        }
        !ctx.is_cancelled()
    }

    /// Go `processRequests`: after this call the batch's requests are
    /// finished, synchronously on error or by the stream's callback.
    fn process_requests(
        self: &Arc<Self>,
        stream: &Arc<TsoStream>,
        controller: BatchController,
        permit: Option<OwnedSemaphorePermit>,
        done: DeadlineDone,
    ) -> crate::Result<()> {
        // `pdclient.processRequests` spans under every traced request's ctx
        // span, finished when the batch has been handed over.
        let mut spans = Vec::new();
        controller.iter_collected_requests(|request| {
            if let Some(context) = &request.client_spans {
                spans.push(crate::trace::ClientSpan::start(
                    context,
                    std::borrow::Cow::Borrowed("pdclient.processRequests"),
                ));
            }
            true
        });
        let count = controller.get_collected_request_count() as i64;
        let svc_discovery = self.provider.get_service_discovery();
        let cluster_id = svc_discovery.get_cluster_id();
        let keyspace_id = svc_discovery.get_keyspace_id();
        let req_keyspace_group_id = svc_discovery.get_keyspace_group_id();
        let tso_info_before_req = self.latest_tso_info.lock().unwrap().clone();
        let batch_start_time = controller.get_extra_batching_start_time().map(Into::into);

        let dispatcher = Arc::clone(self);
        let callback_stream = Arc::clone(stream);
        let callback: super::stream::OnFinishedCallback = Box::new(
            move |result: TsoRequestResult, req_keyspace_group_id, error| {
                done.complete();
                let mut controller = controller;
                let stream_id = callback_stream.stream_id.clone();
                if let Some(error) = error {
                    dispatcher.cancel_collected_requests(&mut controller, permit, error);
                    dispatcher.put_batch_controller(controller);
                    return;
                }
                let cur_tso_info = TsoInfo {
                    tso_server: callback_stream.server_url().to_owned(),
                    req_keyspace_group_id,
                    resp_keyspace_group_id: result.resp_keyspace_group_id,
                    resp_received_at: SystemTime::now(),
                    physical: result.physical,
                    logical: result.logical,
                    source_stream_id: stream_id,
                };
                // `logical` is the batch's largest; the first is count - 1 below.
                let first_logical = result.logical - i64::from(result.count) + 1;
                let checked = check_batch(&result).and_then(|()| {
                    dispatcher.check_monotonicity(tso_info_before_req, cur_tso_info, first_logical)
                });
                match checked {
                    Ok(()) => dispatcher.done_collected_requests(
                        &mut controller,
                        permit,
                        result.physical,
                        first_logical,
                        result.suffix_bits,
                    ),
                    Err(error) => {
                        dispatcher.cancel_collected_requests(&mut controller, permit, error)
                    }
                }
                dispatcher.put_batch_controller(controller);
            },
        );
        let result = stream.process_requests(
            cluster_id,
            keyspace_id,
            req_keyspace_group_id,
            count,
            batch_start_time,
            callback,
        );
        drop(spans);
        match result {
            Ok(()) => Ok(()),
            Err((error, callback)) => {
                // Go closes `done` and cancels the batch synchronously.
                callback(
                    TsoRequestResult::default(),
                    req_keyspace_group_id,
                    Some(dup_error(&error)),
                );
                Err(error)
            }
        }
    }

    /// Go `cancelCollectedRequests`: return the token, then fail the batch.
    fn cancel_collected_requests(
        &self,
        controller: &mut BatchController,
        permit: Option<OwnedSemaphorePermit>,
        error: Error,
    ) {
        drop(permit);
        controller
            .finish_collected_requests(Some(&mut tso_request_finisher(0, 0, None)), Some(&error));
    }

    /// Go `doneCollectedRequests`: return the token, then finish the batch.
    fn done_collected_requests(
        &self,
        controller: &mut BatchController,
        permit: Option<OwnedSemaphorePermit>,
        physical: i64,
        first_logical: i64,
        suffix_bits: u32,
    ) {
        drop(permit);
        controller.finish_collected_requests(
            Some(&mut tso_request_finisher(
                physical,
                first_logical,
                Some(suffix_bits),
            )),
            None,
        );
    }

    /// Go `checkMonotonicity`: the batch must start after the latest ts
    /// allocated before its request began; `latestTSOInfo` keeps the max.
    pub(crate) fn check_monotonicity(
        &self,
        last_tso_info: Option<TsoInfo>,
        cur_tso_info: TsoInfo,
        first_logical: i64,
    ) -> crate::Result<()> {
        let keyspace_id = self.provider.get_service_discovery().get_keyspace_id();
        if let Some(last) = &last_tso_info {
            if last.resp_keyspace_group_id != cur_tso_info.resp_keyspace_group_id {
                log::info!(
                    "old-group-id" = last.resp_keyspace_group_id,
                    "new-group-id" = cur_tso_info.resp_keyspace_group_id;
                    "[tso] keyspace group changed"
                );
            }
            if ts_less_equal(
                cur_tso_info.physical,
                first_logical,
                last.physical,
                last.logical,
            ) {
                // Go `log.Panic`s the process here. The batch fails instead:
                // its callers see the error and no fallen-back ts is handed out.
                log::error!(
                    keyspace = keyspace_id,
                    "last-ts":% = format!("({}, {})", last.physical, last.logical),
                    "cur-ts":% = format!("({}, {})", cur_tso_info.physical, first_logical),
                    "last-tso-server" = last.tso_server.as_str(),
                    "cur-tso-server" = cur_tso_info.tso_server.as_str(),
                    "last-keyspace-group-in-request" = last.req_keyspace_group_id,
                    "cur-keyspace-group-in-request" = cur_tso_info.req_keyspace_group_id,
                    "last-keyspace-group-in-response" = last.resp_keyspace_group_id,
                    "cur-keyspace-group-in-response" = cur_tso_info.resp_keyspace_group_id,
                    "last-response-received-at":% = crate::logutil::zap_time(last.resp_received_at),
                    "cur-response-received-at":% = crate::logutil::zap_time(cur_tso_info.resp_received_at),
                    "last-stream-id" = last.source_stream_id.as_str(),
                    "cur-stream-id" = cur_tso_info.source_stream_id.as_str();
                    "[tso] timestamp fallback"
                );
                return Err(Error::StringError(format!(
                    "[tso] timestamp fallback: last-ts ({}, {}), cur-ts ({}, {})",
                    last.physical, last.logical, cur_tso_info.physical, first_logical
                )));
            }
        }
        let mut latest = self.latest_tso_info.lock().unwrap();
        let replace = latest.as_ref().is_none_or(|old| {
            !ts_less_equal(
                cur_tso_info.physical,
                cur_tso_info.logical,
                old.physical,
                old.logical,
            )
        });
        if replace {
            *latest = Some(cur_tso_info);
        }
        Ok(())
    }

    /// Go `checkTSORPCConcurrency`. Concurrent RPCs and either follower proxy
    /// or an extra batch wait aim at opposite goals, so either forces one.
    /// Rate-limited to one check per [`DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL`],
    /// except that enabling a batch wait while concurrent takes effect at once.
    /// One token at concurrency 1, twice the concurrency above it.
    pub(crate) async fn check_tso_rpc_concurrency(
        &self,
        ctx: &Cancellation,
        max_batch_wait_interval: Duration,
        now: Instant,
    ) -> std::result::Result<(), Error> {
        let mut state = self.concurrency.lock().await;
        let immediately_update = state.rpc_concurrency > 1 && !max_batch_wait_interval.is_zero();
        if !immediately_update
            && state.last_check_concurrency_time.is_some_and(|last| {
                now.duration_since(last) < DISPATCHER_CHECK_RPC_CONCURRENCY_INTERVAL
            })
        {
            return Ok(());
        }
        state.last_check_concurrency_time = Some(now);
        let option = self.provider.get_option();
        let mut new_concurrency = option.get_tso_client_rpc_concurrency();
        if !max_batch_wait_interval.is_zero() || option.get_enable_tso_follower_proxy() {
            new_concurrency = 1;
        }
        // Go's int arithmetic carries a nonpositive option into the token
        // count and the dispatcher starves; a permit count is unsigned, so it
        // answers with Go's default of one ongoing request instead. Every
        // value TiDB publishes (1, 2 or 4) is unaffected.
        let new_token_count = if new_concurrency > 1 {
            new_concurrency as usize * 2
        } else {
            1
        };
        if new_concurrency == state.rpc_concurrency && new_token_count == state.token_count {
            return Ok(());
        }
        if new_concurrency != state.rpc_concurrency {
            log::info!(
                old = state.rpc_concurrency,
                new = new_concurrency;
                "[tso] switching tso rpc concurrency"
            );
        }
        state.rpc_concurrency = new_concurrency;
        if new_token_count > state.token_count {
            self.tokens.add_permits(new_token_count - state.token_count);
            state.token_count = new_token_count;
        } else {
            while state.token_count > new_token_count {
                tokio::select! {
                    acquired = Arc::clone(&self.tokens).acquire_owned() => match acquired {
                        Ok(permit) => permit.forget(),
                        Err(_) => return Err(Error::ContextCanceled),
                    },
                    _ = ctx.cancelled() => return Err(Error::ContextCanceled),
                }
                state.token_count -= 1;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn concurrency_for_test(&self) -> (isize, usize) {
        let state = self.concurrency.lock().await;
        (state.rpc_concurrency, state.token_count)
    }

    #[cfg(test)]
    pub(crate) fn latest_tso_info_for_test(&self) -> Option<TsoInfo> {
        self.latest_tso_info.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn pooled_controllers_for_test(&self) -> usize {
        self.batch_buffer_pool.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(crate) fn pooled_requests_for_test(&self) -> usize {
        self.batch_buffer_pool
            .lock()
            .unwrap()
            .iter()
            .map(|controller| controller.get_collected_request_count())
            .sum()
    }

    #[cfg(test)]
    pub(crate) async fn hold_tokens_for_test(&self, count: u32) -> OwnedSemaphorePermit {
        Arc::clone(&self.tokens)
            .acquire_many_owned(count)
            .await
            .unwrap()
    }

    #[cfg(test)]
    pub(crate) fn available_tokens_for_test(&self) -> usize {
        self.tokens.available_permits()
    }
}

/// Go `tsoutil.TSLessEqual`.
fn ts_less_equal(physical: i64, logical: i64, other_physical: i64, other_logical: i64) -> bool {
    physical < other_physical || (physical == other_physical && logical <= other_logical)
}

/// Rust additionally rejects a range that cannot compose into timestamps
/// before handing any of it out.
fn check_batch(result: &TsoRequestResult) -> crate::Result<()> {
    let response = pdpb::TsoResponse {
        header: None,
        count: result.count,
        timestamp: Some(pdpb::Timestamp {
            physical: result.physical,
            logical: result.logical,
            suffix_bits: result.suffix_bits,
        }),
    };
    TsoBatch::from_response(&response, result.count)
        .map(|_| ())
        .map_err(|error| {
            if error.kind == "tso_count_mismatch" {
                Error::from(errs::ERR_TSO_LENGTH.error().with_stack())
            } else {
                Error::StringError(format!("{}: {}", error.kind, error.message))
            }
        })
}
