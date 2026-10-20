// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `global_controller.go`: the process-wide resource groups controller,
//! its main loop, the resource manager provider interface and the KV
//! interceptor interface.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, Weak};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use prost::Message;
use tokio::sync::mpsc;

use super::config::{
    default_config, generate_ru_config, Config, RUConfig, RUVersion, RequestUnitConfig,
    DEFAULT_GROUP_CLEANUP_INTERVAL, DEFAULT_GROUP_STATE_UPDATE_INTERVAL, DEFAULT_RU_VERSION,
    DEFAULT_TARGET_PERIOD,
};
use super::context::{CancelFunc, Context, ContextError};
use super::group_controller::{
    new_group_cost_controller, new_request_source_metrics_state, GroupCostController,
    RequestSourceMetricsState,
};
use super::limiter::NotifyMsg;
use super::model::{
    equal_ru, new_kv_calculator, new_sql_calculator, RequestInfo, ResourceCalculator, ResponseInfo,
};
use super::util::{duration_nanos, go_duration_string};
use crate::pd::errs::{self, ClientGetResourceGroupError};
use crate::pd::metastorage::{self, WatchResponse};
use crate::pd::opt::{with_prefix, with_prev_kv, with_rev};
use crate::pd::resource_manager_client::{
    group_settings_path_prefix_bytes, GetResourceGroupOption, CONTROLLER_CONFIG_PATH_PREFIX_BYTES,
};
use crate::proto::meta_storagepb::event::EventType;
use crate::proto::resource_manager::{
    keyspace_id_value, BackgroundSettings, Consumption, GroupMode, GroupRequestUnitSettings,
    KeyspaceIdValue, ResourceGroup, TokenBucketRequest, TokenBucketResponse, TokenBucketsRequest,
};

pub(crate) const DEFAULT_RESOURCE_GROUP_NAME: &str = "default";
pub(crate) const CONTROLLER_CONFIG_PATH: &str = "resource_group/controller";
pub(crate) const MAX_NOTIFICATION_CHAN_LEN: usize = 200;
pub(crate) const NEED_TOKENS_AMPLIFICATION: f64 = 1.1;
pub(crate) const TRICKLE_RESERVE_DURATION: Duration = Duration::from_millis(1250);
pub(crate) const SLOW_NOTIFY_FILTER_DURATION: Duration = Duration::from_millis(10);
pub(crate) const WATCH_RETRY_INTERVAL: Duration = Duration::from_secs(30);
/// 4 MiB, which is 16 RRU.
pub(crate) const BIG_REQUEST_THRESHOLD: u64 = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SelectType {
    PeriodicReport = 0,
    LowToken = 1,
}

static ENABLE_CONTROLLER_TRACE_LOG: AtomicBool = AtomicBool::new(false);

pub(crate) fn enable_controller_trace_log() -> bool {
    ENABLE_CONTROLLER_TRACE_LOG.load(Ordering::SeqCst)
}

/// Logs at info level only when controller trace logging is enabled.
pub(crate) fn log_controller_trace(message: impl FnOnce() -> String) {
    if enable_controller_trace_log() {
        log::info!("{}", message());
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// The quota limit controller of resource groups for a KV store.
#[async_trait]
pub trait ResourceGroupKVInterceptor: Send + Sync {
    /// Checks whether the resource group has enough tokens, possibly waiting.
    /// Answers the consumption delta, the store penalty, the wait duration
    /// and the group priority.
    async fn on_request_wait(
        &self,
        resource_group_name: &str,
        info: &dyn RequestInfo,
    ) -> crate::Result<(Consumption, Consumption, Duration, u32)>;
    /// Consumes tokens after receiving a response.
    fn on_response(
        &self,
        resource_group_name: &str,
        req: &dyn RequestInfo,
        resp: &dyn ResponseInfo,
    ) -> crate::Result<Consumption>;
    /// Consumes tokens after receiving a response, waiting when the response
    /// needs many tokens so the debt stays small and smooth.
    async fn on_response_wait(
        &self,
        resource_group_name: &str,
        req: &dyn RequestInfo,
        resp: &dyn ResponseInfo,
    ) -> crate::Result<(Consumption, Duration)>;
    /// Background jobs of the group are neither recorded nor waited for.
    async fn is_background_request(
        &self,
        resource_group_name: &str,
        request_resource: &str,
    ) -> bool;
    /// The current RU calculation version for this keyspace.
    fn get_ru_version(&self) -> RUVersion;
}

/// The resource manager server API, including the meta storage client.
#[async_trait]
pub trait ResourceGroupProvider: metastorage::Client {
    async fn get_resource_group(
        &self,
        resource_group_name: &str,
        opts: &[GetResourceGroupOption],
    ) -> crate::Result<Option<ResourceGroup>>;
    async fn list_resource_groups(
        &self,
        opts: &[GetResourceGroupOption],
    ) -> crate::Result<Vec<ResourceGroup>>;
    async fn add_resource_group(&self, meta_group: ResourceGroup) -> crate::Result<String>;
    async fn modify_resource_group(&self, meta_group: ResourceGroup) -> crate::Result<String>;
    async fn delete_resource_group(&self, resource_group_name: &str) -> crate::Result<String>;
    async fn acquire_token_buckets(
        &self,
        request: TokenBucketsRequest,
    ) -> crate::Result<Vec<TokenBucketResponse>>;
    async fn load_resource_groups(&self) -> crate::Result<(Vec<ResourceGroup>, i64)>;
}

/// The controller settings that create options adjust. Options are applied
/// at creation and again after every controller config change.
pub struct ControllerOptions {
    pub(crate) ru_config: RUConfig,
    pub(crate) degraded_ru_settings: Option<GroupRequestUnitSettings>,
}

/// Creates a `ResourceGroupsController` with optional settings.
pub type ResourceControlCreateOption = Arc<dyn Fn(&mut ControllerOptions) + Send + Sync>;

/// Enables the single group by keyspace feature.
pub fn enable_single_group_by_keyspace() -> ResourceControlCreateOption {
    Arc::new(|options| options.ru_config.is_single_group_by_keyspace = true)
}

/// Sets the max duration to wait for acquiring token buckets.
pub fn with_max_wait_duration(d: Duration) -> ResourceControlCreateOption {
    Arc::new(move |options| options.ru_config.ltb_max_wait_duration = d)
}

/// Sets the retry interval when waiting for the token.
pub fn with_wait_retry_interval(d: Duration) -> ResourceControlCreateOption {
    Arc::new(move |options| options.ru_config.wait_retry_interval = d)
}

/// Sets the times to retry when waiting for the token.
pub fn with_wait_retry_times(times: i64) -> ResourceControlCreateOption {
    Arc::new(move |options| options.ru_config.wait_retry_times = times)
}

/// Sets the wait duration before entering degraded mode.
pub fn with_degraded_mode_wait_duration(d: Duration) -> ResourceControlCreateOption {
    Arc::new(move |options| options.ru_config.degraded_mode_wait_duration = d)
}

/// Sets the RU settings of the degraded resource group used while the
/// resource manager is transiently unavailable.
pub fn with_degraded_ru_settings(
    degraded_ru_settings: Option<GroupRequestUnitSettings>,
) -> ResourceControlCreateOption {
    Arc::new(move |options| options.degraded_ru_settings = degraded_ru_settings.clone())
}

/// Go `ResourceGroupsController.run`, apart from the degraded-mode flag.
#[derive(Default)]
pub(crate) struct ControllerRunState {
    /// When the armed response deadline fires; `None` once it fired or was
    /// stopped.
    pub(crate) response_deadline: Option<Instant>,
    /// Go's non-nil `responseDeadlineCh`: a deadline was armed by a token
    /// request and no response has arrived since.
    pub(crate) response_deadline_armed: bool,
    /// The outstanding token bucket requests. Only one `AcquireTokenBuckets`
    /// is outstanding at a time.
    pub(crate) current_requests: Vec<TokenBucketRequest>,
}

struct LoopReceivers {
    low_token_notify: mpsc::Receiver<NotifyMsg>,
    token_response: mpsc::Receiver<Option<Vec<TokenBucketResponse>>>,
    token_bucket_update: mpsc::Receiver<Arc<GroupCostController>>,
}

/// The resource groups controller; it implements
/// `ResourceGroupKVInterceptor` and client-rust's resource control
/// interceptor.
pub struct ResourceGroupsController {
    pub(crate) client_unique_id: u64,
    pub(crate) provider: Arc<dyn ResourceGroupProvider>,
    pub(crate) groups_controller: RwLock<HashMap<String, Arc<GroupCostController>>>,
    pub(crate) request_source_states: Mutex<HashMap<String, Arc<RequestSourceMetricsState>>>,
    /// Go's `ruConfig` and its concurrency-safe copy `safeRuConfig`: the
    /// configuration is immutable once published, so one snapshot serves both.
    pub(crate) ru_config: ArcSwap<RUConfig>,
    pub(crate) keyspace_id: u32,

    loop_ctx: Mutex<Option<(Context, CancelFunc)>>,

    pub(crate) calculators: Vec<Box<dyn ResourceCalculator>>,

    /// Signals that a resource group is low on tokens.
    pub(crate) low_token_notify_chan: mpsc::Sender<NotifyMsg>,
    /// Token bucket responses from the server.
    pub(crate) token_response_chan: mpsc::Sender<Option<Vec<TokenBucketResponse>>>,
    /// Groups whose token bucket was updated.
    pub(crate) token_bucket_update_chan: mpsc::Sender<Arc<GroupCostController>>,
    receivers: Mutex<Option<LoopReceivers>>,

    pub(crate) run: Mutex<ControllerRunState>,
    pub(crate) in_degraded_mode: AtomicBool,

    pub(crate) opts: Vec<ResourceControlCreateOption>,

    pub(crate) degraded_ru_settings: RwLock<Option<GroupRequestUnitSettings>>,

    /// The RU calculation version for this keyspace; 0 means not loaded or
    /// not configured, which is treated as v1.
    pub(crate) ru_version: AtomicI32,

    wg: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    self_ref: Weak<ResourceGroupsController>,
}

/// Go `NewResourceGroupController`.
pub async fn new_resource_group_controller(
    client_unique_id: u64,
    provider: Arc<dyn ResourceGroupProvider>,
    request_unit_config: Option<RequestUnitConfig>,
    keyspace_id: u32,
    opts: Vec<ResourceControlCreateOption>,
) -> crate::Result<Arc<ResourceGroupsController>> {
    let mut config = load_server_config(provider.as_ref()).await?;
    if let Some(request_unit_config) = request_unit_config {
        config.base_config.request_unit = request_unit_config;
    }
    let controller = ResourceGroupsController::build(
        client_unique_id,
        provider,
        generate_ru_config(&config),
        keyspace_id,
        opts,
    );
    log::info!(
        "load resource controller config, config: {:?}, ru-config: {:?}, keyspace-id: {}",
        config,
        controller.ru_config.load_full(),
        keyspace_id
    );
    ENABLE_CONTROLLER_TRACE_LOG.store(
        config.base_config.enable_controller_trace_log,
        Ordering::SeqCst,
    );
    // The initial RU version comes from the controller config's policy.
    controller.update_ru_version_from_config(&config);
    Ok(controller)
}

async fn load_server_config(provider: &dyn ResourceGroupProvider) -> crate::Result<Config> {
    let resp = provider.get(CONTROLLER_CONFIG_PATH.as_bytes(), &[]).await?;
    let mut config = default_config();
    if resp.kvs.is_empty() {
        log::warn!("[resource group controller] server does not save config, load config failed");
        config.adjust();
        return Ok(config);
    }
    config
        .unmarshal_json(&resp.kvs[0].value)
        .map_err(crate::Error::StringError)?;
    config.adjust();
    Ok(config)
}

/// Source list.
pub const FROM_PERIOD_REPORT: &str = "period_report";
pub const FROM_LOW_RU: &str = "low_ru";

enum LoopEvent {
    Cleanup,
    StateUpdate,
    WatchRetry,
    EmergencyTokenAcquisition,
    Done,
    ResponseDeadline,
    TokenResponse(Option<Option<Vec<TokenBucketResponse>>>),
    LowTokenNotify(Option<NotifyMsg>),
    WatchMeta(Option<WatchResponse>),
    WatchConfig(Option<WatchResponse>),
    TokenBucketUpdate(Option<Arc<GroupCostController>>),
}

fn new_ticker(period: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    // Go's ticker delivers one late tick and keeps its original phase.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker
}

async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await,
        None => std::future::pending::<()>().await,
    }
}

/// Receives from an optional channel; a missing channel, like Go's nil
/// channel, never becomes ready. `None` means the channel was closed.
async fn recv_opt<T>(rx: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

fn watch_retry_after_error() -> Instant {
    if fail::eval("watchStreamError", |_| ()).is_some() {
        return Instant::now() + Duration::from_millis(20);
    }
    Instant::now() + WATCH_RETRY_INTERVAL
}

impl ResourceGroupsController {
    pub(crate) fn build(
        client_unique_id: u64,
        provider: Arc<dyn ResourceGroupProvider>,
        ru_config: RUConfig,
        keyspace_id: u32,
        opts: Vec<ResourceControlCreateOption>,
    ) -> Arc<ResourceGroupsController> {
        let mut options = ControllerOptions {
            ru_config,
            degraded_ru_settings: None,
        };
        for opt in &opts {
            (opt.as_ref())(&mut options);
        }
        let ru_config = Arc::new(options.ru_config);
        let (low_token_notify_chan, low_token_notify) = mpsc::channel(1);
        let (token_response_chan, token_response) = mpsc::channel(1);
        let (token_bucket_update_chan, token_bucket_update) =
            mpsc::channel(MAX_NOTIFICATION_CHAN_LEN);
        Arc::new_cyclic(|self_ref| ResourceGroupsController {
            client_unique_id,
            provider,
            groups_controller: RwLock::new(HashMap::new()),
            request_source_states: Mutex::new(HashMap::new()),
            calculators: vec![
                Box::new(new_kv_calculator(ru_config.clone())),
                Box::new(new_sql_calculator(ru_config.clone())),
            ],
            ru_config: ArcSwap::new(ru_config),
            keyspace_id,
            loop_ctx: Mutex::new(None),
            low_token_notify_chan,
            token_response_chan,
            token_bucket_update_chan,
            receivers: Mutex::new(Some(LoopReceivers {
                low_token_notify,
                token_response,
                token_bucket_update,
            })),
            run: Mutex::new(ControllerRunState::default()),
            in_degraded_mode: AtomicBool::new(false),
            opts,
            degraded_ru_settings: RwLock::new(options.degraded_ru_settings),
            ru_version: AtomicI32::new(0),
            wg: tokio::sync::Mutex::new(None),
            self_ref: self_ref.clone(),
        })
    }

    /// The controller's RU configuration.
    pub fn get_config(&self) -> Arc<RUConfig> {
        self.ru_config.load_full()
    }

    /// The current RU calculation version for this keyspace, or
    /// `DEFAULT_RU_VERSION` when none is configured or loaded. A pure memory
    /// read.
    pub fn get_ru_version(&self) -> RUVersion {
        let v = self.ru_version.load(Ordering::SeqCst);
        if v <= 0 {
            return DEFAULT_RU_VERSION;
        }
        v
    }

    /// Extracts this keyspace's RU version from the controller config.
    pub(crate) fn update_ru_version_from_config(&self, config: &Config) {
        match &config.base_config.ru_version_policy {
            Some(policy) => {
                if let Some(&v) = policy.overrides.get(&self.keyspace_id) {
                    let old = self.ru_version.swap(v, Ordering::SeqCst);
                    if old != v {
                        log::info!(
                            "ru version updated from controller config, old: {old}, new: {v}, keyspace-id: {}",
                            self.keyspace_id
                        );
                    }
                } else if policy.default > 0 {
                    let old = self.ru_version.swap(policy.default, Ordering::SeqCst);
                    if old != policy.default {
                        log::info!(
                            "ru version updated to default from controller config, old: {old}, new: {}, keyspace-id: {}",
                            policy.default,
                            self.keyspace_id
                        );
                    }
                } else {
                    self.ru_version.store(0, Ordering::SeqCst);
                }
            }
            // No policy means no override; `get_ru_version` answers the default.
            None => self.ru_version.store(0, Ordering::SeqCst),
        }
    }

    /// Starts the controller's main loop.
    pub fn start(self: &Arc<Self>) {
        let Some(receivers) = lock(&self.receivers).take() else {
            log::warn!("[resource group controller] the controller is already started");
            return;
        };
        let (loop_ctx, cancel) = Context::with_cancel(&Context::background());
        *lock(&self.loop_ctx) = Some((loop_ctx.clone(), cancel));
        let this = self.clone();
        let handle = tokio::spawn(async move { this.main_loop(loop_ctx, receivers).await });
        match self.wg.try_lock() {
            Ok(mut wg) => *wg = Some(handle),
            Err(_) => log::warn!("[resource group controller] start raced with stop"),
        }
    }

    async fn main_loop(self: Arc<Self>, ctx: Context, mut rx: LoopReceivers) {
        let mut cleanup_period = DEFAULT_GROUP_CLEANUP_INTERVAL;
        let mut state_update_period = DEFAULT_GROUP_STATE_UPDATE_INTERVAL;
        if fail::eval("fastCleanup", |_| ()).is_some() {
            cleanup_period = Duration::from_millis(100);
            // The cleanup compares `run.consumption`, so the state update
            // must speed up too.
            state_update_period = Duration::from_millis(200);
        }
        if fail::eval("acceleratedReportingPeriod", |_| ()).is_some() {
            state_update_period = Duration::from_millis(100);
        }
        let mut cleanup_ticker = new_ticker(cleanup_period);
        let mut state_update_ticker = new_ticker(state_update_period);
        let mut emergency_token_acquisition_ticker = new_ticker(DEFAULT_TARGET_PERIOD);

        let mut cfg_revision = match self
            .provider
            .get(CONTROLLER_CONFIG_PATH.as_bytes(), &[])
            .await
        {
            Ok(resp) => resp.header.as_ref().map_or(0, |header| header.revision),
            Err(error) => {
                log::warn!("load resource group revision failed, error: {error}");
                0
            }
        };
        if let Some(Some(value)) = fail::eval("staleRevision", |value| value) {
            if let Ok(revision) = value.trim().parse::<i64>() {
                cfg_revision = revision;
            }
        }
        let single_group_by_keyspace = self.ru_config.load().is_single_group_by_keyspace;
        let mut watch_meta_channel: Option<mpsc::Receiver<WatchResponse>> = None;
        let mut watch_config_channel: Option<mpsc::Receiver<WatchResponse>> = None;
        if !single_group_by_keyspace {
            // The previous key-value pair identifies the group of a delete event.
            let prefix = group_settings_path_prefix_bytes(self.keyspace_id);
            match self
                .provider
                .watch(&prefix, &[with_prefix(), with_prev_kv()])
                .await
            {
                Ok(channel) => watch_meta_channel = Some(channel),
                Err(error) => log::warn!("watch resource group meta failed, error: {error}"),
            }
        }
        match self
            .provider
            .watch(
                CONTROLLER_CONFIG_PATH_PREFIX_BYTES,
                &[with_rev(cfg_revision)],
            )
            .await
        {
            Ok(channel) => watch_config_channel = Some(channel),
            Err(error) => log::warn!("watch resource group config failed, error: {error}"),
        }
        let mut watch_retry_at = Some(Instant::now() + WATCH_RETRY_INTERVAL);

        loop {
            let response_deadline = lock(&self.run).response_deadline;
            let event = tokio::select! {
                _ = cleanup_ticker.tick() => LoopEvent::Cleanup,
                _ = state_update_ticker.tick() => LoopEvent::StateUpdate,
                _ = sleep_until_opt(watch_retry_at) => LoopEvent::WatchRetry,
                _ = emergency_token_acquisition_ticker.tick() => LoopEvent::EmergencyTokenAcquisition,
                _ = ctx.done() => LoopEvent::Done,
                _ = sleep_until_opt(response_deadline) => LoopEvent::ResponseDeadline,
                resp = rx.token_response.recv() => LoopEvent::TokenResponse(resp),
                msg = rx.low_token_notify.recv() => LoopEvent::LowTokenNotify(msg),
                resp = recv_opt(&mut watch_meta_channel) => LoopEvent::WatchMeta(resp),
                resp = recv_opt(&mut watch_config_channel) => LoopEvent::WatchConfig(resp),
                gc = rx.token_bucket_update.recv() => LoopEvent::TokenBucketUpdate(gc),
            };
            match event {
                LoopEvent::Cleanup => self.clean_up_resource_group(),
                LoopEvent::StateUpdate => {
                    self.execute_on_all_groups(|gc| gc.update_run_state());
                    self.execute_on_all_groups(|gc| gc.update_avg_request_resource_per_sec());
                    let idle = lock(&self.run).current_requests.is_empty();
                    if idle {
                        // Groups due for their periodic report.
                        self.collect_token_bucket_requests(
                            &ctx,
                            FROM_PERIOD_REPORT,
                            SelectType::PeriodicReport,
                            NotifyMsg::default(),
                        );
                    }
                }
                LoopEvent::WatchRetry => {
                    watch_retry_at = None;
                    if !single_group_by_keyspace && watch_meta_channel.is_none() {
                        let prefix = group_settings_path_prefix_bytes(self.keyspace_id);
                        match self
                            .provider
                            .watch(&prefix, &[with_prefix(), with_prev_kv()])
                            .await
                        {
                            Ok(channel) => watch_meta_channel = Some(channel),
                            Err(error) => {
                                log::warn!("watch resource group meta failed, error: {error}");
                                watch_retry_at = Some(watch_retry_after_error());
                            }
                        }
                    }
                    if watch_config_channel.is_none() {
                        match self
                            .provider
                            .watch(
                                CONTROLLER_CONFIG_PATH_PREFIX_BYTES,
                                &[with_rev(cfg_revision)],
                            )
                            .await
                        {
                            Ok(channel) => watch_config_channel = Some(channel),
                            Err(error) => {
                                log::warn!("watch resource group config failed, error: {error}");
                                watch_retry_at = Some(Instant::now() + WATCH_RETRY_INTERVAL);
                            }
                        }
                    }
                }
                LoopEvent::EmergencyTokenAcquisition => {
                    self.execute_on_all_groups(|gc| gc.reset_emergency_token_acquisition());
                }
                LoopEvent::Done => {
                    super::metrics::global_metrics()
                        .resource_group_status_gauge
                        .reset();
                    let states: Vec<_> = lock(&self.request_source_states)
                        .values()
                        .cloned()
                        .collect();
                    for state in states {
                        state.cleanup();
                    }
                    return;
                }
                LoopEvent::ResponseDeadline => {
                    lock(&self.run).response_deadline = None;
                    self.in_degraded_mode.store(true, Ordering::SeqCst);
                    self.execute_on_all_groups(|gc| gc.apply_degraded_mode());
                    log::warn!("[resource group controller] enter degraded mode");
                }
                LoopEvent::TokenResponse(resp) => {
                    if let Some(Some(resp)) = resp {
                        self.execute_on_all_groups(|gc| gc.update_run_state());
                        self.handle_token_bucket_response(resp);
                    }
                    lock(&self.run).current_requests.clear();
                }
                LoopEvent::LowTokenNotify(msg) => {
                    let Some(msg) = msg else {
                        continue;
                    };
                    self.execute_on_all_groups(|gc| gc.update_run_state());
                    self.execute_on_all_groups(|gc| gc.update_avg_request_resource_per_sec());
                    // Groups low on tokens.
                    self.collect_token_bucket_requests(
                        &ctx,
                        FROM_LOW_RU,
                        SelectType::LowToken,
                        msg,
                    );
                    if self.is_degraded() {
                        self.execute_on_all_groups(|gc| gc.apply_degraded_mode());
                    }
                }
                LoopEvent::WatchMeta(resp) => {
                    if fail::eval("disableWatch", |_| ()).is_some() && single_group_by_keyspace {
                        panic!("disableWatch");
                    }
                    let Some(resp) = resp else {
                        watch_meta_channel = None;
                        watch_retry_at = Some(watch_retry_after_error());
                        continue;
                    };
                    self.handle_meta_watch_events(resp).await;
                }
                LoopEvent::WatchConfig(resp) => {
                    let Some(resp) = resp else {
                        watch_config_channel = None;
                        watch_retry_at = Some(watch_retry_after_error());
                        continue;
                    };
                    if resp.compact_revision > cfg_revision {
                        cfg_revision = resp.compact_revision;
                    }
                    for item in &resp.events {
                        let Some(kv) = &item.kv else {
                            continue;
                        };
                        cfg_revision = kv.mod_revision;
                        let mut config = default_config();
                        if config.unmarshal_json(&kv.value).is_err() {
                            continue;
                        }
                        config.adjust();
                        self.apply_controller_config(&config);
                        log::info!(
                            "load resource controller config after config changed, config: {:?}, ruConfig: {:?}",
                            config,
                            self.ru_config.load_full()
                        );
                    }
                }
                LoopEvent::TokenBucketUpdate(gc) => {
                    if let Some(gc) = gc {
                        let ctx = ctx.clone();
                        tokio::spawn(async move { gc.handle_token_bucket_update_event(ctx).await });
                    }
                }
            }
        }
    }

    /// Publishes a changed controller config, re-applying the create options
    /// (for compatibility with serverless).
    fn apply_controller_config(&self, config: &Config) {
        let mut options = ControllerOptions {
            ru_config: generate_ru_config(config),
            degraded_ru_settings: self
                .degraded_ru_settings
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        };
        for opt in &self.opts {
            (opt.as_ref())(&mut options);
        }
        self.ru_config.store(Arc::new(options.ru_config));
        *self
            .degraded_ru_settings
            .write()
            .unwrap_or_else(|e| e.into_inner()) = options.degraded_ru_settings;
        if enable_controller_trace_log() != config.base_config.enable_controller_trace_log {
            ENABLE_CONTROLLER_TRACE_LOG.store(
                config.base_config.enable_controller_trace_log,
                Ordering::SeqCst,
            );
        }
        self.update_ru_version_from_config(config);
    }

    async fn handle_meta_watch_events(&self, resp: WatchResponse) {
        for item in &resp.events {
            if item.r#type == EventType::Put as i32 {
                let Some(kv) = &item.kv else {
                    continue;
                };
                let Ok(group) = ResourceGroup::decode(kv.value.as_slice()) else {
                    continue;
                };
                let name = group.name.clone();
                let Some(gc) = self.load_group_controller(&name) else {
                    continue;
                };
                if !gc.tombstone.load(Ordering::SeqCst) {
                    gc.modify_meta(Arc::new(group));
                    continue;
                }
                // A group that was tombstoned before is created again.
                let new_gc = match new_group_cost_controller(
                    Arc::new(group),
                    self.ru_config.load_full(),
                    self.low_token_notify_chan.clone(),
                    self.token_bucket_update_chan.clone(),
                    Some(self.get_or_create_request_source_metrics_state(&name)),
                ) {
                    Ok(new_gc) => new_gc,
                    Err(error) => {
                        log::warn!(
                            "[resource group controller] re-create resource group cost controller for tombstone failed, name: {name}, error: {error}"
                        );
                        continue;
                    }
                };
                if self.compare_and_swap_group_controller(&name, &gc, new_gc) {
                    log::info!(
                        "[resource group controller] re-create resource group cost controller for tombstone, name: {name}"
                    );
                }
            } else if item.r#type == EventType::Delete as i32 {
                // A compacted prev-kv means an earlier delete event already
                // happened, so this one is a duplicate.
                let Some(prev_kv) = &item.prev_kv else {
                    let (key, value) = item
                        .kv
                        .as_ref()
                        .map(|kv| {
                            (
                                String::from_utf8_lossy(&kv.key).into_owned(),
                                String::from_utf8_lossy(&kv.value).into_owned(),
                            )
                        })
                        .unwrap_or_default();
                    log::info!(
                        "[resource group controller] previous key-value pair has been compacted, required-key: {key}, value: {value}"
                    );
                    continue;
                };
                let Ok(group) = ResourceGroup::decode(prev_kv.value.as_slice()) else {
                    continue;
                };
                self.tombstone_group_cost_controller(&group.name).await;
            }
        }
    }

    /// Stops the controller's main loop and waits for it to exit.
    pub async fn stop(&self) -> crate::Result<()> {
        let cancel = lock(&self.loop_ctx)
            .as_ref()
            .map(|(_, cancel)| cancel.clone());
        let Some(cancel) = cancel else {
            return Err(crate::Error::StringError(
                "resource groups controller does not start".to_owned(),
            ));
        };
        cancel.cancel();
        let handle = self.wg.lock().await.take();
        if let Some(handle) = handle {
            if let Err(error) = handle.await {
                log::error!("[resource group controller] main loop failed: {error}");
            }
        }
        Ok(())
    }

    pub(crate) fn load_group_controller(&self, name: &str) -> Option<Arc<GroupCostController>> {
        self.groups_controller
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
    }

    /// Go `sync.Map.LoadOrStore`: the stored controller and whether it was
    /// already present.
    pub(crate) fn load_or_store_group_controller(
        &self,
        name: &str,
        gc: Arc<GroupCostController>,
    ) -> (Arc<GroupCostController>, bool) {
        let mut groups = self
            .groups_controller
            .write()
            .unwrap_or_else(|e| e.into_inner());
        match groups.get(name) {
            Some(existing) => (existing.clone(), true),
            None => {
                groups.insert(name.to_owned(), gc.clone());
                (gc, false)
            }
        }
    }

    pub(crate) fn store_group_controller(&self, name: &str, gc: Arc<GroupCostController>) {
        self.groups_controller
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_owned(), gc);
    }

    pub(crate) fn delete_group_controller(&self, name: &str) {
        self.groups_controller
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(name);
    }

    /// Go `sync.Map.CompareAndSwap` by controller identity.
    pub(crate) fn compare_and_swap_group_controller(
        &self,
        name: &str,
        old: &Arc<GroupCostController>,
        new: Arc<GroupCostController>,
    ) -> bool {
        let mut groups = self
            .groups_controller
            .write()
            .unwrap_or_else(|e| e.into_inner());
        match groups.get(name) {
            Some(current) if Arc::ptr_eq(current, old) => {
                groups.insert(name.to_owned(), new);
                true
            }
            _ => false,
        }
    }

    fn snapshot_groups(&self) -> Vec<(String, Arc<GroupCostController>)> {
        self.groups_controller
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(name, gc)| (name.clone(), gc.clone()))
            .collect()
    }

    pub(crate) fn get_or_create_request_source_metrics_state(
        &self,
        name: &str,
    ) -> Arc<RequestSourceMetricsState> {
        lock(&self.request_source_states)
            .entry(name.to_owned())
            .or_insert_with(|| new_request_source_metrics_state(name))
            .clone()
    }

    pub(crate) fn cleanup_request_source_metrics_state(&self, name: &str) {
        let state = lock(&self.request_source_states).remove(name);
        if let Some(state) = state {
            state.cleanup();
        }
    }

    pub(crate) fn should_use_degraded_resource_group(&self, error: &crate::Error) -> bool {
        if self
            .degraded_ru_settings
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
        {
            return false;
        }
        if is_resource_group_not_found_error(error) {
            return false;
        }
        if chain_any(error, is_context_canceled) || chain_any(error, is_context_deadline_exceeded) {
            return false;
        }
        let cause = unwrap_get_resource_group_error(error);
        if errs::is_leader_change(Some(cause)) {
            return true;
        }
        match grpc_code(cause) {
            tonic::Code::Cancelled => false,
            tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => true,
            _ => false,
        }
    }

    pub(crate) fn get_degraded_resource_group(&self, resource_group_name: &str) -> ResourceGroup {
        ResourceGroup {
            name: resource_group_name.to_owned(),
            mode: GroupMode::RuMode as i32,
            r_u_settings: self
                .degraded_ru_settings
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            ..ResourceGroup::default()
        }
    }

    /// The group's controller from the local cache, or from the server on a
    /// cache miss. With `use_tombstone`, a tombstoned controller is returned.
    pub(crate) async fn try_get_resource_group_controller(
        &self,
        name: &str,
        use_tombstone: bool,
    ) -> crate::Result<Arc<GroupCostController>> {
        // The local cache first.
        if let Some(gc) = self.load_group_controller(name) {
            if !use_tombstone && gc.tombstone.load(Ordering::SeqCst) {
                return Err(new_resource_group_not_exist_err(name));
            }
            return Ok(gc);
        }

        let mut is_use_degraded_resource_group = false;
        // Fetch the resource group info from the server.
        let group = match self.provider.get_resource_group(name, &[]).await {
            Ok(group) => group,
            Err(error) => {
                if self.should_use_degraded_resource_group(&error) {
                    is_use_degraded_resource_group = true;
                    Some(self.get_degraded_resource_group(name))
                } else {
                    return Err(normalize_get_resource_group_error(error));
                }
            }
        };
        let Some(group) = group else {
            return Err(new_resource_group_not_exist_err(name));
        };
        // Check again: another caller may have initialized the group.
        if let Some(gc) = self.load_group_controller(name) {
            return Ok(gc);
        }
        let group_name = group.name.clone();
        let gc = new_group_cost_controller(
            Arc::new(group),
            self.ru_config.load_full(),
            self.low_token_notify_chan.clone(),
            self.token_bucket_update_chan.clone(),
            Some(self.get_or_create_request_source_metrics_state(name)),
        )?;
        if !is_use_degraded_resource_group {
            let (_, loaded) = self.load_or_store_group_controller(name, gc.clone());
            if !loaded {
                super::metrics::global_metrics()
                    .resource_group_status_gauge
                    .with_label_values(&[name, &group_name])
                    .set(1.0);
                log::info!(
                    "[resource group controller] create resource group cost controller, name: {name}"
                );
            }
        }
        Ok(gc)
    }

    /// A deleted group is not dropped at once, which would interrupt ongoing
    /// requests: it is marked tombstone and served by a controller built from
    /// the default group.
    pub(crate) async fn tombstone_group_cost_controller(&self, name: &str) {
        if self.load_group_controller(name).is_none() {
            return;
        }
        // The default resource group controller is never deleted.
        if name == DEFAULT_RESOURCE_GROUP_NAME {
            return;
        }
        let default_gc = match self
            .try_get_resource_group_controller(DEFAULT_RESOURCE_GROUP_NAME, false)
            .await
        {
            Ok(default_gc) => default_gc,
            Err(error) => {
                log::warn!(
                    "[resource group controller] get default resource group meta for tombstone failed, name: {name}, error: {error}"
                );
                // Without the default group, drop the controller directly.
                self.cleanup_request_source_metrics_state(name);
                self.delete_group_controller(name);
                return;
            }
        };
        let gc = match new_group_cost_controller(
            default_gc.get_meta(),
            self.ru_config.load_full(),
            self.low_token_notify_chan.clone(),
            self.token_bucket_update_chan.clone(),
            Some(self.get_or_create_request_source_metrics_state(name)),
        ) {
            Ok(gc) => gc,
            Err(error) => {
                log::warn!(
                    "[resource group controller] create default resource group cost controller for tombstone failed, name: {name}, error: {error}"
                );
                self.cleanup_request_source_metrics_state(name);
                self.delete_group_controller(name);
                return;
            }
        };
        gc.tombstone.store(true, Ordering::SeqCst);
        self.store_group_controller(name, gc);
        // Its metrics are deleted by the cleanup.
        super::metrics::global_metrics()
            .resource_group_status_gauge
            .with_label_values(&[name, name])
            .set(2.0);
        log::info!(
            "[resource group controller] default resource group controller cost created for tombstone, name: {name}"
        );
    }

    /// Drops stale groups whose consumption stayed unchanged.
    pub(crate) fn clean_up_resource_group(&self) {
        for (resource_group_name, gc) in self.snapshot_groups() {
            let latest_consumption = lock(&gc.mu).consumption.clone();
            let unchanged = equal_ru(&latest_consumption, &lock(&gc.run).consumption);
            if unchanged {
                if gc.inactive.load(Ordering::SeqCst) || gc.tombstone.load(Ordering::SeqCst) {
                    self.cleanup_request_source_metrics_state(&resource_group_name);
                    self.delete_group_controller(&resource_group_name);
                    let _ = super::metrics::global_metrics()
                        .resource_group_status_gauge
                        .remove_label_values(&[&resource_group_name, &resource_group_name]);
                    gc.metrics.delete_paging_labels(&resource_group_name);
                    continue;
                }
                gc.inactive.store(true, Ordering::SeqCst);
            } else {
                gc.inactive.store(false, Ordering::SeqCst);
            }
        }
    }

    pub(crate) fn execute_on_all_groups(&self, f: impl Fn(&GroupCostController)) {
        for (_, gc) in self.snapshot_groups() {
            f(&gc);
        }
    }

    pub(crate) fn handle_token_bucket_response(&self, resp: Vec<TokenBucketResponse>) {
        {
            let mut run = lock(&self.run);
            if run.response_deadline_armed {
                run.response_deadline = None;
                run.response_deadline_armed = false;
            }
        }
        self.in_degraded_mode.store(false, Ordering::SeqCst);
        for res in &resp {
            let name = &res.resource_group_name;
            let Some(gc) = self.load_group_controller(name) else {
                log::warn!(
                    "[resource group controller] a non-existent resource group was found when handle token response, name: {name}"
                );
                continue;
            };
            gc.handle_token_bucket_response(res);
        }
    }

    pub(crate) fn collect_token_bucket_requests(
        &self,
        ctx: &Context,
        source: &'static str,
        typ: SelectType,
        notify_msg: NotifyMsg,
    ) {
        let mut requests = Vec::new();
        for (_, gc) in self.snapshot_groups() {
            if let Some(mut request) = gc.collect_request_and_consumption(typ) {
                request.keyspace_id = Some(KeyspaceIdValue {
                    keyspace: Some(keyspace_id_value::Keyspace::Value(self.keyspace_id)),
                });
                requests.push(request);
                gc.metrics.token_request_counter.inc();
            }
        }
        lock(&self.run).current_requests = requests.clone();
        if !requests.is_empty() {
            self.send_token_bucket_requests(ctx, requests, source, notify_msg);
        }
    }

    fn send_token_bucket_requests(
        &self,
        ctx: &Context,
        requests: Vec<TokenBucketRequest>,
        source: &'static str,
        notify_msg: NotifyMsg,
    ) {
        let now = Instant::now();
        let req = TokenBucketsRequest {
            requests,
            target_request_period_ms: DEFAULT_TARGET_PERIOD.as_millis() as u64,
            client_unique_id: self.client_unique_id,
        };
        let degraded_mode_wait_duration = self.ru_config.load().degraded_mode_wait_duration;
        {
            let mut run = lock(&self.run);
            if !degraded_mode_wait_duration.is_zero() && !run.response_deadline_armed {
                run.response_deadline = Some(Instant::now() + degraded_mode_wait_duration);
                run.response_deadline_armed = true;
            }
        }
        let provider = self.provider.clone();
        let token_response_chan = self.token_response_chan.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            log_controller_trace(|| {
                format!(
                    "[resource group controller] send token bucket request, now: {now:?}, req: {:?}, source: {source}",
                    req.requests
                )
            });
            let result = tokio::select! {
                result = provider.acquire_token_buckets(req) => result,
                _ = ctx.done() => Err(crate::Error::from(ctx.err().unwrap_or(ContextError::Canceled))),
            };
            let latency = now.elapsed();
            let metrics = super::metrics::global_metrics();
            let resp = match result {
                Ok(resp) => {
                    metrics
                        .successful_token_request_duration
                        .observe(latency.as_secs_f64());
                    Some(resp)
                }
                Err(error) => {
                    // Errors caused by the stopper cancelling the context are
                    // not logged.
                    if !matches!(error, crate::Error::ContextCanceled) {
                        log::warn!(
                            "[resource group controller] token bucket rpc error, error: {error}"
                        );
                    }
                    metrics
                        .failed_token_request_duration
                        .observe(latency.as_secs_f64());
                    None
                }
            };
            if let Some(start_time) = notify_msg.start_time {
                let cost = start_time.elapsed();
                if cost > SLOW_NOTIFY_FILTER_DURATION {
                    log::warn!(
                        "[resource group controller] slow token bucket request, source: {source}, cost: {}",
                        go_duration_string(duration_nanos(cost))
                    );
                }
            }
            log_controller_trace(|| {
                format!(
                    "[resource group controller] token bucket response, now: {:?}, resp: {resp:?}, source: {source}, latency: {}",
                    Instant::now(),
                    go_duration_string(duration_nanos(latency))
                )
            });
            let _ = token_response_chan.send(resp).await;
        });
    }

    /// Checks whether the resource group has enough tokens, possibly waiting.
    /// Answers the consumption delta, the store penalty, the wait duration
    /// and the group priority.
    pub async fn on_request_wait(
        &self,
        resource_group_name: &str,
        info: &dyn RequestInfo,
    ) -> crate::Result<(Consumption, Consumption, Duration, u32)> {
        let gc = self
            .try_get_resource_group_controller(resource_group_name, true)
            .await?;
        gc.on_request_wait_impl(&Context::background(), info).await
    }

    /// Consumes tokens after receiving a response.
    pub fn on_response(
        &self,
        resource_group_name: &str,
        req: &dyn RequestInfo,
        resp: &dyn ResponseInfo,
    ) -> crate::Result<Consumption> {
        let Some(gc) = self.load_group_controller(resource_group_name) else {
            log::warn!(
                "[resource group controller] resource group name does not exist, name: {resource_group_name}"
            );
            return Ok(Consumption::default());
        };
        gc.on_response_impl(req, resp)
    }

    /// Consumes tokens after receiving a response, waiting for large ones.
    pub async fn on_response_wait(
        &self,
        resource_group_name: &str,
        req: &dyn RequestInfo,
        resp: &dyn ResponseInfo,
    ) -> crate::Result<(Consumption, Duration)> {
        let Some(gc) = self.load_group_controller(resource_group_name) else {
            log::warn!(
                "[resource group controller] resource group name does not exist, name: {resource_group_name}"
            );
            return Ok((Consumption::default(), Duration::ZERO));
        };
        gc.on_response_wait_impl(&Context::background(), req, resp)
            .await
    }

    /// Background jobs of the resource group are neither recorded nor waited
    /// for.
    pub async fn is_background_request(
        &self,
        resource_group_name: &str,
        request_resource: &str,
    ) -> bool {
        let Ok(gc) = self
            .try_get_resource_group_controller(resource_group_name, false)
            .await
        else {
            return false;
        };
        let meta = gc.get_meta();
        self.check_background_settings(meta.background_settings.as_ref(), request_resource)
            .await
    }

    async fn check_background_settings(
        &self,
        bg: Option<&BackgroundSettings>,
        request_resource: &str,
    ) -> bool {
        // Fall back to the default resource group.
        let default_meta;
        let bg = match bg {
            Some(bg) => Some(bg),
            None => {
                let Ok(gc) = self
                    .try_get_resource_group_controller(DEFAULT_RESOURCE_GROUP_NAME, false)
                    .await
                else {
                    return false;
                };
                default_meta = gc.get_meta();
                default_meta.background_settings.as_ref()
            }
        };
        background_job_types_contain(bg, request_resource)
    }

    /// `is_background_request` over the local cache only, for the
    /// synchronous interceptor hook. A cache miss starts the fetch Go would
    /// perform and answers `false` until the group is cached.
    pub(crate) fn is_background_request_cached(
        &self,
        resource_group_name: &str,
        request_resource: &str,
    ) -> bool {
        let Some(gc) = self.cached_untombstoned(resource_group_name) else {
            return false;
        };
        let meta = gc.get_meta();
        if meta.background_settings.is_some() {
            return background_job_types_contain(
                meta.background_settings.as_ref(),
                request_resource,
            );
        }
        let Some(default_gc) = self.cached_untombstoned(DEFAULT_RESOURCE_GROUP_NAME) else {
            return false;
        };
        let default_meta = default_gc.get_meta();
        background_job_types_contain(default_meta.background_settings.as_ref(), request_resource)
    }

    fn cached_untombstoned(&self, name: &str) -> Option<Arc<GroupCostController>> {
        match self.load_group_controller(name) {
            Some(gc) if gc.tombstone.load(Ordering::SeqCst) => None,
            Some(gc) => Some(gc),
            None => {
                self.prefetch_resource_group(name);
                None
            }
        }
    }

    fn prefetch_resource_group(&self, name: &str) {
        let (Some(this), Ok(runtime)) = (
            self.self_ref.upgrade(),
            tokio::runtime::Handle::try_current(),
        ) else {
            return;
        };
        let name = name.to_owned();
        runtime.spawn(async move {
            let _ = this.try_get_resource_group_controller(&name, false).await;
        });
    }

    /// The meta setting of the given resource group.
    pub async fn get_resource_group(
        &self,
        resource_group_name: &str,
    ) -> crate::Result<Arc<ResourceGroup>> {
        let gc = self
            .try_get_resource_group_controller(resource_group_name, false)
            .await?;
        Ok(gc.get_meta())
    }

    /// The group's local runtime state, or `None` when the controller has no
    /// usable local state for it.
    pub fn get_resource_group_runtime_state(
        &self,
        resource_group_name: &str,
    ) -> Option<ResourceGroupRuntimeState> {
        let gc = self.load_group_controller(resource_group_name)?;
        if gc.tombstone.load(Ordering::SeqCst)
            || !gc.initial_request_completed.load(Ordering::SeqCst)
        {
            return None;
        }
        Some(ResourceGroupRuntimeState {
            has_limited_burst: !gc.burstable.load(Ordering::SeqCst),
        })
    }

    /// Reports RU consumption directly, such as the TiFlash MPP cost after a
    /// query finishes.
    pub fn report_consumption(&self, resource_group_name: &str, consumption: &Consumption) {
        let Some(gc) = self.load_group_controller(resource_group_name) else {
            log::warn!(
                "[resource group controller] resource group name does not exist, name: {resource_group_name}"
            );
            return;
        };
        gc.add_ru_consumption(Some(consumption));
    }

    /// Reports the experimental v2 RU consumption split by engine. RUv2 is
    /// only observed; no tokens are deducted.
    pub fn report_ruv2_consumption(
        &self,
        resource_group_name: &str,
        tikv_ruv2: f64,
        tidb_ruv2: f64,
        tiflash_ruv2: f64,
    ) {
        let Some(gc) = self.load_group_controller(resource_group_name) else {
            log::warn!(
                "[resource group controller] resource group name does not exist, name: {resource_group_name}"
            );
            return;
        };
        gc.add_ruv2_consumption(tikv_ruv2, tidb_ruv2, tiflash_ruv2);
    }

    /// Whether the controller is in degraded mode.
    pub fn is_degraded(&self) -> bool {
        self.in_degraded_mode.load(Ordering::SeqCst)
    }

    /// The active (not tombstoned) resource group. Used by tests.
    pub fn get_active_resource_group(
        &self,
        resource_group_name: &str,
    ) -> Option<Arc<ResourceGroup>> {
        let gc = self.load_group_controller(resource_group_name)?;
        if gc.tombstone.load(Ordering::SeqCst) {
            return None;
        }
        Some(gc.get_meta())
    }
}

/// Local runtime signals derived from token bucket responses.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResourceGroupRuntimeState {
    /// The request-unit token bucket has a finite burst limit.
    pub has_limited_burst: bool,
}

fn background_job_types_contain(bg: Option<&BackgroundSettings>, request_resource: &str) -> bool {
    let Some(bg) = bg else {
        return false;
    };
    if request_resource.is_empty() || bg.job_types.is_empty() {
        return false;
    }
    match request_resource.rfind('_') {
        Some(idx) => bg
            .job_types
            .iter()
            .any(|job_type| job_type == &request_resource[idx + 1..]),
        None => false,
    }
}

/// Go `NewResourceGroupNotExistErr`.
pub fn new_resource_group_not_exist_err(name: &str) -> crate::Error {
    crate::Error::PdResourceGroup(ClientGetResourceGroupError {
        resource_group_name: name.to_owned(),
        cause: "resource group does not exist".to_owned(),
        error: None,
    })
}

fn find_get_resource_group_error<'a>(
    mut error: &'a (dyn StdError + 'static),
) -> Option<&'a ClientGetResourceGroupError> {
    loop {
        if let Some(group_error) = error.downcast_ref::<ClientGetResourceGroupError>() {
            return Some(group_error);
        }
        error = error.source()?;
    }
}

/// The innermost cause wrapped by `ErrClientGetResourceGroup` values.
pub(crate) fn unwrap_get_resource_group_error<'a>(
    error: &'a (dyn StdError + 'static),
) -> &'a (dyn StdError + 'static) {
    let mut current = error;
    loop {
        let Some(group_error) = find_get_resource_group_error(current) else {
            return current;
        };
        let Some(inner) = group_error.error.as_deref() else {
            return current;
        };
        let inner: &(dyn StdError + 'static) = inner;
        if std::ptr::addr_eq(inner as *const dyn StdError, current as *const dyn StdError) {
            return current;
        }
        current = inner;
    }
}

fn chain_any(
    error: &(dyn StdError + 'static),
    predicate: fn(&(dyn StdError + 'static)) -> bool,
) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if predicate(error) {
            return true;
        }
        current = error.source();
    }
    false
}

fn is_context_canceled(error: &(dyn StdError + 'static)) -> bool {
    matches!(
        error.downcast_ref::<crate::Error>(),
        Some(crate::Error::ContextCanceled)
    ) || error.downcast_ref::<ContextError>() == Some(&ContextError::Canceled)
}

fn is_context_deadline_exceeded(error: &(dyn StdError + 'static)) -> bool {
    error.downcast_ref::<ContextError>() == Some(&ContextError::DeadlineExceeded)
}

/// Go `status.Code`: the gRPC code of the first status in the chain, or
/// `Unknown` for an error without one.
pub(crate) fn grpc_code(error: &(dyn StdError + 'static)) -> tonic::Code {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(status) = error.downcast_ref::<tonic::Status>() {
            return status.code();
        }
        if let Some(crate::Error::GrpcAPI(status)) = error.downcast_ref::<crate::Error>() {
            return status.code();
        }
        current = error.source();
    }
    tonic::Code::Unknown
}

/// Caller cancellation and deadlines surface as the context errors themselves.
pub(crate) fn normalize_get_resource_group_error(error: crate::Error) -> crate::Error {
    if chain_any(&error, is_context_canceled) {
        return crate::Error::ContextCanceled;
    }
    if chain_any(&error, is_context_deadline_exceeded) {
        return ContextError::DeadlineExceeded.into();
    }
    error
}

pub(crate) fn is_resource_group_not_found_error(error: &crate::Error) -> bool {
    let message = error.to_string();
    message.contains("PD:resourcemanager:ErrGroupNotExists")
        || message.contains("resource group does not exist")
}

#[async_trait]
impl ResourceGroupKVInterceptor for ResourceGroupsController {
    async fn on_request_wait(
        &self,
        resource_group_name: &str,
        info: &dyn RequestInfo,
    ) -> crate::Result<(Consumption, Consumption, Duration, u32)> {
        ResourceGroupsController::on_request_wait(self, resource_group_name, info).await
    }

    fn on_response(
        &self,
        resource_group_name: &str,
        req: &dyn RequestInfo,
        resp: &dyn ResponseInfo,
    ) -> crate::Result<Consumption> {
        ResourceGroupsController::on_response(self, resource_group_name, req, resp)
    }

    async fn on_response_wait(
        &self,
        resource_group_name: &str,
        req: &dyn RequestInfo,
        resp: &dyn ResponseInfo,
    ) -> crate::Result<(Consumption, Duration)> {
        ResourceGroupsController::on_response_wait(self, resource_group_name, req, resp).await
    }

    async fn is_background_request(
        &self,
        resource_group_name: &str,
        request_resource: &str,
    ) -> bool {
        ResourceGroupsController::is_background_request(self, resource_group_name, request_resource)
            .await
    }

    fn get_ru_version(&self) -> RUVersion {
        ResourceGroupsController::get_ru_version(self)
    }
}

/// client-go installs this controller with `tikv.SetResourceControlInterceptor`;
/// client-rust's dispatch reaches it through this interceptor.
#[async_trait]
impl crate::resource_control::ResourceGroupController for ResourceGroupsController {
    async fn on_request_wait(
        &self,
        resource_group_name: &str,
        request: crate::resource_control::RequestInfo,
    ) -> crate::Result<crate::resource_control::RequestWaitResult> {
        let (consumption, penalty, wait_duration, priority) =
            ResourceGroupsController::on_request_wait(self, resource_group_name, &request).await?;
        Ok(crate::resource_control::RequestWaitResult {
            consumption,
            penalty: Some(penalty),
            wait_duration,
            priority: u64::from(priority),
        })
    }

    /// The synchronous settlement is Go's non-waiting `OnResponse`, which
    /// client-go uses for txn-file commits.
    fn on_response_wait(
        &self,
        resource_group_name: &str,
        request: crate::resource_control::RequestInfo,
        response: crate::resource_control::ResponseInfo,
    ) -> crate::Result<crate::resource_control::ResponseWaitResult> {
        let consumption =
            ResourceGroupsController::on_response(self, resource_group_name, &request, &response)?;
        Ok(crate::resource_control::ResponseWaitResult {
            consumption,
            wait_duration: Duration::ZERO,
        })
    }

    /// Go's `OnResponseWait`, used for dispatched TiKV RPCs.
    async fn on_response_wait_async(
        &self,
        resource_group_name: &str,
        request: crate::resource_control::RequestInfo,
        response: crate::resource_control::ResponseInfo,
    ) -> crate::Result<crate::resource_control::ResponseWaitResult> {
        let (consumption, wait_duration) = ResourceGroupsController::on_response_wait(
            self,
            resource_group_name,
            &request,
            &response,
        )
        .await?;
        Ok(crate::resource_control::ResponseWaitResult {
            consumption,
            wait_duration,
        })
    }

    fn is_background_request(&self, resource_group_name: &str, request_source: &str) -> bool {
        self.is_background_request_cached(resource_group_name, request_source)
    }
}
