// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `group_controller.go`: per-resource-group cost accounting, the token
//! counter and its limiter, and per-group metrics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use prometheus::{Counter, Gauge, Histogram};
use tokio::sync::mpsc;

use super::config::{
    RUConfig, CONSUMPTIONS_REPORTING_THRESHOLD, DEFAULT_GROUP_STATE_UPDATE_INTERVAL,
    DEFAULT_TARGET_PERIOD, EXTENDED_REPORTING_PERIOD_FACTOR, MOVING_AVG_FACTOR, NOTIFY_FRACTION,
    TOKEN_RESERVE_FRACTION,
};
use super::context::Context;
use super::global_controller::{
    enable_controller_trace_log, log_controller_trace, SelectType, BIG_REQUEST_THRESHOLD,
    NEED_TOKENS_AMPLIFICATION, TRICKLE_RESERVE_DURATION,
};
use super::limiter::{
    is_throttled_error, reset_low_process, wait_reservations, Limiter, NotifyMsg, Reservation,
    TokenBucketReconfigureArgs,
};
use super::model::{
    add, get_ru_value_from_consumption, get_sql_process_cpu_time, new_kv_calculator,
    new_sql_calculator, paging_read_estimate, reported_request_consumption,
    reported_response_consumption, request_source, sub, update_delta_consumption, KVCalculator,
    RequestInfo, ResourceCalculator, ResponseInfo,
};
use crate::async_util::Cancellation;
use crate::pd::errs;
use crate::proto::resource_manager::{
    self, token_bucket_request, Consumption, RequestUnitItem, RequestUnitType, ResourceGroup,
    TokenBucket, TokenBucketRequest, TokenBucketResponse,
};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Go `groupCostController`.
pub(crate) struct GroupCostController {
    // Invariant attributes.
    pub(crate) name: String,
    pub(crate) mode: i32,
    pub(crate) main_cfg: Arc<RUConfig>,
    // Meta info.
    pub(crate) meta: RwLock<Arc<ResourceGroup>>,

    // Used by the token limiter.
    pub(crate) calculators: Vec<Box<dyn ResourceCalculator>>,

    pub(crate) metrics: GroupMetricsCollection,
    pub(crate) mu: Mutex<GroupCostMu>,

    /// Fast path: one token limit with an unlimited burst.
    pub(crate) burstable: AtomicBool,
    pub(crate) is_throttled: AtomicBool,

    pub(crate) low_ru_notify_chan: mpsc::Sender<NotifyMsg>,
    pub(crate) token_bucket_update_chan: mpsc::Sender<Arc<GroupCostController>>,

    /// The state updated by the main loop.
    pub(crate) run: Mutex<GroupRunState>,
    /// Go `run.requestUnitTokens`, fixed when the run state is initialized.
    pub(crate) request_unit_tokens: Arc<TokenCounter>,

    /// Set when the first token bucket request completes successfully.
    pub(crate) initial_request_completed: AtomicBool,
    /// Set when the resource group is deleted.
    pub(crate) tombstone: AtomicBool,
    /// Set when the resource group has not been updated for a long time.
    pub(crate) inactive: AtomicBool,

    self_ref: std::sync::Weak<GroupCostController>,
}

/// Go `groupCostController.mu`.
#[derive(Default)]
pub(crate) struct GroupCostMu {
    pub(crate) consumption: Consumption,
    pub(crate) store_counter: HashMap<u64, Consumption>,
    pub(crate) global_counter: Consumption,
}

/// Go `groupCostController.run`.
pub(crate) struct GroupRunState {
    pub(crate) now: Instant,
    pub(crate) last_request_time: Instant,
    /// Set while a token bucket request is outstanding; a pending request is
    /// retried on the next tick.
    pub(crate) request_in_progress: bool,
    /// The target period setting at the last update.
    pub(crate) target_period: Duration,
    /// The last value of `mu.consumption`.
    pub(crate) consumption: Consumption,
    pub(crate) last_request_consumption: Consumption,
}

/// Go `groupMetricsCollection`.
pub(crate) struct GroupMetricsCollection {
    pub(crate) successful_request_duration: Histogram,
    pub(crate) failed_limit_reserve_duration: Histogram,
    pub(crate) request_retry_counter: Counter,
    pub(crate) failed_request_counter_with_others: Counter,
    pub(crate) failed_request_counter_with_throttled: Counter,
    pub(crate) token_request_counter: Counter,
    pub(crate) running_kv_request_counter: Gauge,
    pub(crate) consume_token_histogram: Histogram,
    pub(crate) source_state: Option<Arc<RequestSourceMetricsState>>,

    // Paging pre-charge observers, cached per resource group.
    pub(crate) precharge_counter: Counter,
    pub(crate) precharge_bytes_counter: Counter,
    pub(crate) actual_bytes_counter: Counter,
    pub(crate) prediction_residual_bytes: Histogram,
    pub(crate) no_precharge_counter: Counter,
}

pub(crate) const REQUEST_SOURCE_RU_TYPE_RRU: &str = "rru";
pub(crate) const REQUEST_SOURCE_RU_TYPE_WRU: &str = "wru";
pub(crate) const REQUEST_SOURCE_DIRECTION_CONSUME: &str = "consume";
pub(crate) const REQUEST_SOURCE_DIRECTION_REFUND: &str = "refund";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestSourceMetricKey {
    pub(crate) request_source: String,
    pub(crate) ru_type: &'static str,
    pub(crate) direction: &'static str,
}

/// Go `requestSourceMetricsState`.
pub(crate) struct RequestSourceMetricsState {
    pub(crate) resource_group_name: String,
    pub(crate) mu: RwLock<RequestSourceMetricsInner>,
}

#[derive(Default)]
pub(crate) struct RequestSourceMetricsInner {
    pub(crate) closed: bool,
    pub(crate) items: HashMap<RequestSourceMetricKey, Counter>,
}

pub(crate) fn new_request_source_metrics_state(
    resource_group_name: &str,
) -> Arc<RequestSourceMetricsState> {
    Arc::new(RequestSourceMetricsState {
        resource_group_name: resource_group_name.to_owned(),
        mu: RwLock::new(RequestSourceMetricsInner::default()),
    })
}

impl RequestSourceMetricsState {
    pub(crate) fn cleanup(&self) {
        let mut inner = self.mu.write().unwrap_or_else(|e| e.into_inner());
        inner.closed = true;
        // This assumes one active controller per process: the counter vector
        // is process-global while this state is controller-local.
        let metrics = super::metrics::global_metrics();
        for key in inner.items.keys() {
            let _ = metrics.request_source_ru_counter.remove_label_values(&[
                &self.resource_group_name,
                &key.request_source,
                key.ru_type,
                key.direction,
            ]);
        }
        inner.items.clear();
    }
}

pub(crate) fn init_metrics(
    old_name: &str,
    name: &str,
    source_state: Option<Arc<RequestSourceMetricsState>>,
) -> GroupMetricsCollection {
    const OTHER_TYPE: &str = "others";
    const THROTTLED_TYPE: &str = "throttled";
    let metrics = super::metrics::global_metrics();
    GroupMetricsCollection {
        successful_request_duration: metrics
            .successful_request_duration
            .with_label_values(&[old_name, name]),
        failed_limit_reserve_duration: metrics
            .failed_limit_reserve_duration
            .with_label_values(&[old_name, name]),
        failed_request_counter_with_others: metrics
            .failed_request_counter
            .with_label_values(&[old_name, name, OTHER_TYPE]),
        failed_request_counter_with_throttled: metrics.failed_request_counter.with_label_values(&[
            old_name,
            name,
            THROTTLED_TYPE,
        ]),
        request_retry_counter: metrics
            .request_retry_counter
            .with_label_values(&[old_name, name]),
        token_request_counter: metrics
            .resource_group_token_request_counter
            .with_label_values(&[old_name, name]),
        running_kv_request_counter: metrics
            .group_running_kv_request_counter
            .with_label_values(&[name]),
        consume_token_histogram: metrics.token_consumed_histogram.with_label_values(&[name]),
        source_state,
        precharge_counter: metrics
            .cop_read_precharge_counter
            .with_label_values(&[name]),
        precharge_bytes_counter: metrics
            .paging_precharge_bytes_counter
            .with_label_values(&[name]),
        actual_bytes_counter: metrics
            .paging_actual_bytes_counter
            .with_label_values(&[name]),
        prediction_residual_bytes: metrics
            .paging_prediction_residual_bytes
            .with_label_values(&[name]),
        no_precharge_counter: metrics
            .cop_read_no_precharge_counter
            .with_label_values(&[name]),
    }
}

impl GroupMetricsCollection {
    pub(crate) fn get_or_create_request_source_metric(
        &self,
        key: RequestSourceMetricKey,
    ) -> Option<Counter> {
        let state = self.source_state.as_ref()?;
        {
            let inner = state.mu.read().unwrap_or_else(|e| e.into_inner());
            if let Some(metric) = inner.items.get(&key) {
                return Some(metric.clone());
            }
            if inner.closed {
                return None;
            }
        }
        let mut inner = state.mu.write().unwrap_or_else(|e| e.into_inner());
        if inner.closed {
            return None;
        }
        if let Some(metric) = inner.items.get(&key) {
            return Some(metric.clone());
        }
        let metric = super::metrics::global_metrics()
            .request_source_ru_counter
            .with_label_values(&[
                &state.resource_group_name,
                &key.request_source,
                key.ru_type,
                key.direction,
            ]);
        inner.items.insert(key, metric.clone());
        Some(metric)
    }

    pub(crate) fn add_request_source_ru_delta(
        &self,
        request_source: &str,
        consumption: Option<&Consumption>,
    ) {
        let Some(consumption) = consumption else {
            return;
        };
        self.add_request_source_ru_value(
            request_source,
            REQUEST_SOURCE_RU_TYPE_RRU,
            consumption.r_r_u,
        );
        self.add_request_source_ru_value(
            request_source,
            REQUEST_SOURCE_RU_TYPE_WRU,
            consumption.w_r_u,
        );
    }

    pub(crate) fn add_request_source_ru_value(
        &self,
        request_source: &str,
        ru_type: &'static str,
        mut value: f64,
    ) {
        if value == 0.0 {
            return;
        }
        let mut direction = REQUEST_SOURCE_DIRECTION_CONSUME;
        if value < 0.0 {
            direction = REQUEST_SOURCE_DIRECTION_REFUND;
            value = -value;
        }
        let metric = self.get_or_create_request_source_metric(RequestSourceMetricKey {
            request_source: request_source.to_owned(),
            ru_type,
            direction,
        });
        if let Some(metric) = metric {
            metric.inc_by(value);
        }
    }

    /// Removes the per-group paging series of a deleted or tombstoned group.
    /// Keep this list in sync with `init_metrics`.
    pub(crate) fn delete_paging_labels(&self, name: &str) {
        let metrics = super::metrics::global_metrics();
        let _ = metrics
            .cop_read_precharge_counter
            .remove_label_values(&[name]);
        let _ = metrics
            .cop_read_no_precharge_counter
            .remove_label_values(&[name]);
        let _ = metrics
            .paging_precharge_bytes_counter
            .remove_label_values(&[name]);
        let _ = metrics
            .paging_actual_bytes_counter
            .remove_label_values(&[name]);
        let _ = metrics
            .paging_prediction_residual_bytes
            .remove_label_values(&[name]);
    }

    /// Request-boundary cop read pre-charge counters. Callers gate this on
    /// `paging_read_estimate(..).1`, scoping it to coprocessor reads.
    pub(crate) fn observe_paging_request(&self, bytes_for_est: u64) {
        if bytes_for_est == 0 {
            self.no_precharge_counter.inc();
            return;
        }
        self.precharge_counter.inc();
        self.precharge_bytes_counter.inc_by(bytes_for_est as f64);
    }

    /// Response-boundary paging metrics for pre-charged coprocessor RPCs.
    pub(crate) fn observe_paging_response(&self, bytes_for_est: u64, actual: u64) {
        if bytes_for_est == 0 {
            return;
        }
        self.actual_bytes_counter.inc_by(actual as f64);
        self.prediction_residual_bytes
            .observe(actual as f64 - bytes_for_est as f64);
    }
}

/// Go `tokenCounter`.
pub(crate) struct TokenCounter {
    pub(crate) fill_rate: u64,
    pub(crate) state: Mutex<TokenCounterState>,
    pub(crate) notify: Mutex<TokenCounterNotify>,
    pub(crate) limiter: Arc<Limiter>,
}

pub(crate) struct TokenCounterState {
    /// An exponentially weighted moving average of RU consumption per second,
    /// used to estimate the next request's needs.
    pub(crate) avg_ru_per_sec: f64,
    /// The consumption RU value when `avg_ru_per_sec` was last updated.
    pub(crate) avg_ru_per_sec_last_ru: f64,
    pub(crate) avg_last_time: Instant,
    /// `None` is Go's zero time.
    pub(crate) last_deadline: Option<Instant>,
    pub(crate) last_rate: f64,
    pub(crate) in_degraded_mode: bool,
}

/// Go `tokenCounter.notify`. The setup timer and its channel are one
/// deadline; `generation` identifies one armed timer and cancel channel.
#[derive(Default)]
pub(crate) struct TokenCounterNotify {
    pub(crate) setup_notification_deadline: Option<Instant>,
    pub(crate) setup_notification_threshold: f64,
    /// Wakes `handle_token_bucket_update_event` when the timer is stopped.
    pub(crate) cancel_ch: Option<Arc<Cancellation>>,
    pub(crate) generation: u64,
}

static NOTIFY_GENERATION: AtomicU64 = AtomicU64::new(1);

impl TokenCounterNotify {
    /// Arms a new timer and cancel channel.
    pub(crate) fn arm(&mut self, deadline: Instant, threshold: f64) {
        self.setup_notification_deadline = Some(deadline);
        self.setup_notification_threshold = threshold;
        self.cancel_ch = Some(Arc::new(Cancellation::default()));
        self.generation = NOTIFY_GENERATION.fetch_add(1, Ordering::Relaxed);
    }
}

/// Go `newGroupCostController`.
pub(crate) fn new_group_cost_controller(
    group: Arc<ResourceGroup>,
    main_cfg: Arc<RUConfig>,
    low_ru_notify_chan: mpsc::Sender<NotifyMsg>,
    token_bucket_update_chan: mpsc::Sender<Arc<GroupCostController>>,
    source_state: Option<Arc<RequestSourceMetricsState>>,
) -> crate::Result<Arc<GroupCostController>> {
    let configured = group
        .r_u_settings
        .as_ref()
        .and_then(|settings| settings.r_u.as_ref())
        .is_some_and(|bucket| bucket.settings.is_some());
    if group.mode == resource_manager::GroupMode::RuMode as i32 {
        if !configured {
            return Err(crate::Error::Pd(
                errs::client_resource_group_config_unavailable("not configured"),
            ));
        }
    } else {
        return Err(crate::Error::Pd(
            errs::client_resource_group_config_unavailable("not supports the resource type"),
        ));
    }
    let metrics = init_metrics(&group.name, &group.name, source_state);
    let now = Instant::now();
    let (run, request_unit_tokens, burstable) =
        init_run_state(&group, &main_cfg, now, &low_ru_notify_chan);
    let name = group.name.clone();
    let mode = group.mode;
    Ok(Arc::new_cyclic(|self_ref| GroupCostController {
        name,
        mode,
        calculators: vec![
            Box::new(new_kv_calculator(main_cfg.clone())),
            Box::new(new_sql_calculator(main_cfg.clone())),
        ],
        main_cfg,
        meta: RwLock::new(group),
        metrics,
        mu: Mutex::new(GroupCostMu::default()),
        burstable: AtomicBool::new(burstable),
        is_throttled: AtomicBool::new(false),
        low_ru_notify_chan,
        token_bucket_update_chan,
        run: Mutex::new(run),
        request_unit_tokens,
        initial_request_completed: AtomicBool::new(false),
        tombstone: AtomicBool::new(false),
        inactive: AtomicBool::new(false),
        self_ref: self_ref.clone(),
    }))
}

/// Go `initRunState`.
fn init_run_state(
    meta: &ResourceGroup,
    main_cfg: &RUConfig,
    now: Instant,
    low_ru_notify_chan: &mpsc::Sender<NotifyMsg>,
) -> (GroupRunState, Arc<TokenCounter>, bool) {
    let run = GroupRunState {
        now,
        last_request_time: now.checked_sub(DEFAULT_TARGET_PERIOD).unwrap_or(now),
        request_in_progress: false,
        target_period: DEFAULT_TARGET_PERIOD,
        consumption: Consumption::default(),
        last_request_consumption: Consumption {
            sql_layer_cpu_time_ms: get_sql_process_cpu_time(main_cfg.is_single_group_by_keyspace),
            ..Consumption::default()
        },
    };

    let mut is_burstable = true;
    let bucket = meta
        .r_u_settings
        .as_ref()
        .and_then(|settings| settings.r_u.clone())
        .unwrap_or_default();
    let settings = bucket.settings.clone().unwrap_or_default();
    let initial_token = settings.fill_rate as f64;
    let mut cfg = TokenBucketReconfigureArgs {
        new_tokens: initial_token,
        new_burst: settings.burst_limit,
        // Trigger token requests as soon as the group starts consuming.
        new_notify_threshold: (initial_token * TOKEN_RESERVE_FRACTION).max(1.0),
        ..TokenBucketReconfigureArgs::default()
    };
    if cfg.new_burst >= 0 {
        cfg.new_burst = 0;
    }
    if settings.burst_limit >= 0 {
        is_burstable = false;
    }

    let limiter =
        Limiter::new_limiter_with_cfg(&meta.name, now, cfg, Some(low_ru_notify_chan.clone()));
    let counter = Arc::new(TokenCounter {
        fill_rate: settings.fill_rate,
        state: Mutex::new(TokenCounterState {
            avg_ru_per_sec: 0.0,
            avg_ru_per_sec_last_ru: 0.0,
            avg_last_time: now,
            last_deadline: None,
            last_rate: 0.0,
            in_degraded_mode: false,
        }),
        notify: Mutex::new(TokenCounterNotify::default()),
        limiter,
    });
    (run, counter, is_burstable)
}

impl GroupCostController {
    fn arc(&self) -> Option<Arc<GroupCostController>> {
        self.self_ref.upgrade()
    }

    /// Applies degraded mode to a group in the low-token process.
    pub(crate) fn apply_degraded_mode(&self) {
        let counter = &self.request_unit_tokens;
        if !counter.limiter.is_low_tokens() {
            return;
        }
        let now = lock(&self.run).now;
        {
            let mut state = lock(&counter.state);
            if state.in_degraded_mode {
                return;
            }
            state.in_degraded_mode = true;
        }
        init_counter_notify(counter);
        let mut cfg = TokenBucketReconfigureArgs {
            new_burst: counter.fill_rate as i64,
            new_fill_rate: counter.fill_rate as f64,
            ..TokenBucketReconfigureArgs::default()
        };
        if fail::eval("degradedModeRU", |_| ()).is_some() {
            cfg.new_fill_rate = 99999999.0;
        }
        counter
            .limiter
            .reconfigure(now, cfg, &[reset_low_process()]);
        log::info!(
            "[resource group controller] resource token bucket enter degraded mode, name: {}",
            self.name
        );
    }

    pub(crate) fn update_run_state(&self) {
        let new_time = Instant::now();
        let mut run = lock(&self.run);
        {
            let mut mu = lock(&self.mu);
            for calc in &self.calculators {
                calc.trickle(&mut mu.consumption);
            }
            run.consumption = mu.consumption.clone();
        }
        log_controller_trace(|| {
            format!(
                "[resource group controller] update run state, name: {}, request-unit-consumption: {:?}, is-throttled: {}",
                self.name,
                run.consumption,
                self.is_throttled.load(Ordering::SeqCst)
            )
        });
        run.now = new_time;
    }

    pub(crate) fn update_avg_request_resource_per_sec(&self) {
        let counter = &self.request_unit_tokens;
        let is_burstable = counter.limiter.get_burst() < 0;
        self.burstable.store(is_burstable, Ordering::SeqCst);
        let run = lock(&self.run);
        if !self.calc_avg(
            &run,
            counter,
            get_ru_value_from_consumption(Some(&run.consumption)),
        ) {
            return;
        }
        log_controller_trace(|| {
            format!(
                "[resource group controller] update avg ru per sec, name: {}, avg-ru-per-sec: {}, is-throttled: {}",
                self.name,
                lock(&counter.state).avg_ru_per_sec,
                self.is_throttled.load(Ordering::SeqCst)
            )
        });
    }

    pub(crate) fn reset_emergency_token_acquisition(&self) {
        self.request_unit_tokens
            .limiter
            .reset_remaining_notify_times();
    }

    /// Waits for the trickle timer and then re-enables low-token notification.
    pub(crate) async fn handle_token_bucket_update_event(&self, ctx: Context) {
        let counter = &self.request_unit_tokens;
        let (deadline, cancel_ch, generation) = {
            let notify = lock(&counter.notify);
            match (notify.setup_notification_deadline, notify.cancel_ch.clone()) {
                (Some(deadline), Some(cancel_ch)) => (deadline, cancel_ch, notify.generation),
                _ => return,
            }
        };
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                let cancel_ch = {
                    let mut notify = lock(&counter.notify);
                    let same_cancel = notify
                        .cancel_ch
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(current, &cancel_ch));
                    if notify.generation != generation
                        || notify.setup_notification_deadline.is_none()
                        || !same_cancel
                    {
                        return;
                    }
                    let threshold = notify.setup_notification_threshold;
                    let cancel_ch = reset_counter_notify_locked(&mut notify);
                    counter.limiter.setup_notification_threshold(threshold);
                    cancel_ch
                };
                if let Some(cancel_ch) = cancel_ch {
                    cancel_ch.cancel();
                }
            }
            _ = cancel_ch.cancelled() => {}
            _ = ctx.done() => {}
        }
    }

    fn calc_avg(&self, run: &GroupRunState, counter: &TokenCounter, new: f64) -> bool {
        let mut state = lock(&counter.state);
        let mut delta_duration = signed_seconds(run.now, state.avg_last_time);
        if fail::eval("acceleratedReportingPeriod", |_| ()).is_some() {
            delta_duration = 0.1;
        }
        // A controller published between the `update_run_state` and
        // `update_avg_request_resource_per_sec` passes of one tick still has
        // `run.now == avg_last_time`; 0/0 would poison the average with NaN.
        if delta_duration <= 0.0 {
            return false;
        }
        let delta = (new - state.avg_ru_per_sec_last_ru) / delta_duration;
        state.avg_ru_per_sec =
            MOVING_AVG_FACTOR * state.avg_ru_per_sec + (1.0 - MOVING_AVG_FACTOR) * delta;
        if fail::eval("acceleratedSpeedTrend", |_| ()).is_some() {
            state.avg_ru_per_sec = if delta > 0.0 { 1000.0 } else { 0.0 };
        }
        state.avg_last_time = run.now;
        state.avg_ru_per_sec_last_ru = new;
        true
    }

    fn should_report_consumption(&self, run: &GroupRunState) -> bool {
        if !self.initial_request_completed.load(Ordering::SeqCst) {
            return true;
        }
        let mut time_since_last_request = run.now.saturating_duration_since(run.last_request_time);
        if fail::eval("acceleratedReportingPeriod", |_| ()).is_some() {
            time_since_last_request = EXTENDED_REPORTING_PERIOD_FACTOR * DEFAULT_TARGET_PERIOD;
        }
        // `last_request_time` is updated late in this logic, so the measured
        // period is slightly shorter than the tick; half a tick of slack keeps
        // the actual report period at the target period.
        if time_since_last_request + DEFAULT_GROUP_STATE_UPDATE_INTERVAL / 2
            >= DEFAULT_TARGET_PERIOD
        {
            if time_since_last_request >= EXTENDED_REPORTING_PERIOD_FACTOR * DEFAULT_TARGET_PERIOD {
                return true;
            }
            if get_ru_value_from_consumption(Some(&run.consumption))
                - get_ru_value_from_consumption(Some(&run.last_request_consumption))
                >= CONSUMPTIONS_REPORTING_THRESHOLD
            {
                return true;
            }
        }
        false
    }

    pub(crate) fn handle_token_bucket_response(&self, resp: &TokenBucketResponse) {
        lock(&self.run).request_in_progress = false;
        self.handle_ru_token_response(resp);
        self.initial_request_completed.store(true, Ordering::SeqCst);
    }

    fn handle_ru_token_response(&self, resp: &TokenBucketResponse) {
        for granted_tb in &resp.granted_r_u_tokens {
            let bucket = granted_tb.granted_tokens.clone().unwrap_or_default();
            self.modify_token_counter(
                &self.request_unit_tokens,
                &bucket,
                granted_tb.trickle_time_ms,
            );
        }
    }

    pub(crate) fn modify_token_counter(
        &self,
        counter: &TokenCounter,
        bucket: &TokenBucket,
        trickle_time_ms: i64,
    ) {
        let now = lock(&self.run).now;
        let settings = bucket.settings.clone().unwrap_or_default();
        let mut granted = bucket.tokens;
        let mut state = lock(&counter.state);
        if let Some(last_deadline) = state.last_deadline {
            // RUs of the last trickle not yet made available are delivered
            // together with the newly granted RUs.
            if last_deadline > now {
                granted += state.last_rate * (last_deadline - now).as_secs_f64();
            }
        }
        init_counter_notify(counter);
        state.in_degraded_mode = false;
        let mut cfg = TokenBucketReconfigureArgs {
            new_burst: settings.burst_limit,
            ..TokenBucketReconfigureArgs::default()
        };
        // A zero trickle time means the server has enough tokens: all granted
        // tokens are available immediately.
        if trickle_time_ms == 0 {
            cfg.new_tokens = granted;
            cfg.new_fill_rate = settings.fill_rate as f64;
            state.last_deadline = None;
            cfg.new_notify_threshold = (granted + counter.limiter.available_tokens(now))
                .min(state.avg_ru_per_sec * DEFAULT_TARGET_PERIOD.as_secs_f64())
                * NOTIFY_FRACTION;
            if cfg.new_burst < 0 {
                cfg.new_tokens = counter.fill_rate as f64;
            }
            self.is_throttled.store(false, Ordering::SeqCst);
        } else {
            // Otherwise the granted tokens are delivered at the fill rate.
            cfg.new_tokens = 0.0;
            let trickle_duration = Duration::from_millis(trickle_time_ms.max(0) as u64);
            let deadline = now + trickle_duration;
            cfg.new_fill_rate =
                settings.fill_rate as f64 + granted / trickle_duration.as_secs_f64();

            let timer_duration = if trickle_duration > TRICKLE_RESERVE_DURATION {
                trickle_duration - TRICKLE_RESERVE_DURATION
            } else {
                (trickle_duration + TRICKLE_RESERVE_DURATION) / 2
            };
            lock(&counter.notify).arm(Instant::now() + timer_duration, 1.0);
            state.last_deadline = Some(deadline);
            self.is_throttled.store(true, Ordering::SeqCst);
            if let Some(this) = self.arc() {
                let _ = self.token_bucket_update_chan.try_send(this);
            }
        }

        state.last_rate = cfg.new_fill_rate;
        drop(state);
        counter
            .limiter
            .reconfigure(now, cfg, &[reset_low_process()]);
    }

    pub(crate) fn collect_request_and_consumption(
        &self,
        select_typ: SelectType,
    ) -> Option<TokenBucketRequest> {
        let mut run = lock(&self.run);
        // Collect the requested resource.
        let mut selected = run.request_in_progress;
        if fail::eval("triggerUpdate", |_| ()).is_some() {
            selected = true;
        }
        let counter = &self.request_unit_tokens;
        if select_typ == SelectType::PeriodicReport {
            selected = selected || self.should_report_consumption(&run);
            if let Some(value) = fail::eval("triggerPeriodicReport", |value| value) {
                selected = value.as_deref() == Some(self.name.as_str());
            }
        }
        // Go falls through from the periodic case into the low-token case.
        if counter.limiter.is_low_tokens() {
            selected = true;
        }
        if let Some(value) = fail::eval("triggerLowRUReport", |value| value) {
            if select_typ == SelectType::LowToken {
                selected = value.as_deref() == Some(self.name.as_str());
            }
        }
        let request = RequestUnitItem {
            r#type: RequestUnitType::Ru as i32,
            value: self.calc_request(&run, counter),
        };
        if !selected {
            return None;
        }
        let run = &mut *run;
        let consumption_since_last_request =
            update_delta_consumption(&mut run.last_request_consumption, &run.consumption);
        run.last_request_time = Instant::now();
        run.request_in_progress = true;
        Some(TokenBucketRequest {
            resource_group_name: self.name.clone(),
            request: Some(token_bucket_request::Request::RuItems(
                token_bucket_request::RequestRu {
                    request_r_u: vec![request],
                },
            )),
            consumption_since_last_request: Some(consumption_since_last_request),
            ..TokenBucketRequest::default()
        })
    }

    pub(crate) fn get_meta(&self) -> Arc<ResourceGroup> {
        self.meta.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub(crate) fn modify_meta(&self, new_meta: Arc<ResourceGroup>) {
        *self.meta.write().unwrap_or_else(|e| e.into_inner()) = new_meta;
    }

    fn calc_request(&self, run: &GroupRunState, counter: &TokenCounter) -> f64 {
        // The server's grants set the average consumption speed, so the need
        // is amplified to let the client follow a raised fill rate.
        let avg_ru_per_sec = lock(&counter.state).avg_ru_per_sec;
        let mut value =
            avg_ru_per_sec * run.target_period.as_secs_f64() * NEED_TOKENS_AMPLIFICATION;
        value -= counter.limiter.available_tokens(run.now);
        if value < 0.0 {
            value = 0.0;
        }
        value
    }

    /// Go `acquireTokens`: returns the reservation wait and the result, and
    /// adds retry waits to `wait_duration`.
    pub(crate) async fn acquire_tokens(
        &self,
        ctx: &Context,
        delta: &Consumption,
        wait_duration: &mut Duration,
        allow_debt: bool,
    ) -> (Duration, crate::Result<()>) {
        struct Running<'a>(&'a Gauge);
        impl Drop for Running<'_> {
            fn drop(&mut self) {
                self.0.dec();
            }
        }
        self.metrics.running_kv_request_counter.inc();
        let _running = Running(&self.metrics.running_kv_request_counter);

        let mut d = Duration::ZERO;
        let mut result: crate::Result<()> = Ok(());
        for _ in 0..self.main_cfg.wait_retry_times.max(0) {
            let counter = &self.request_unit_tokens;
            let reconfigured_ch = counter.limiter.get_reconfigured_ch();
            let now = Instant::now();
            let mut reservation: Option<Reservation> = None;
            let v = get_ru_value_from_consumption(Some(delta));
            if v > 0.0 {
                // Record the consumed tokens in controller trace mode.
                if enable_controller_trace_log() {
                    self.metrics.consume_token_histogram.observe(v);
                }
                // Small requests, or requests of an unthrottled group, may go
                // into debt: remove the tokens directly.
                if allow_debt {
                    counter.limiter.remove_tokens(now, v);
                    break;
                }
                reservation = Some(counter.limiter.reserve(
                    ctx,
                    self.main_cfg.ltb_max_wait_duration,
                    now,
                    v,
                ));
            }
            let (waited, waited_result) =
                wait_reservations(ctx, now, &[reservation.as_ref()]).await;
            d = waited;
            result = waited_result;
            match &result {
                Ok(()) => break,
                Err(error) if !is_throttled_error(error) => break,
                Err(_) => {}
            }
            self.metrics.request_retry_counter.inc();
            let wait_start = Instant::now();
            tokio::select! {
                _ = ctx.done() => {
                    *wait_duration += wait_start.elapsed();
                    let error = ctx.err().unwrap_or(super::context::ContextError::Canceled);
                    return (d, Err(error.into()));
                }
                _ = reconfigured_ch.cancelled() => {}
                _ = tokio::time::sleep(self.main_cfg.wait_retry_interval) => {}
            }
            *wait_duration += wait_start.elapsed();
        }
        (d, result)
    }

    /// Go `onRequestWaitImpl`.
    pub(crate) async fn on_request_wait_impl(
        &self,
        ctx: &Context,
        info: &dyn RequestInfo,
    ) -> crate::Result<(Consumption, Consumption, Duration, u32)> {
        let mut delta = Consumption::default();
        for calc in &self.calculators {
            calc.before_kv_request(&mut delta, info);
        }
        let reported_delta =
            reported_request_consumption(&self.calculators, info, &delta).into_owned();

        add(Some(&mut lock(&self.mu).consumption), Some(&reported_delta));

        /// A dropped admission is Go's cancelled context: it fails as
        /// "others" and its consumption is taken back.
        struct Admission<'a> {
            gc: &'a GroupCostController,
            reported_delta: &'a Consumption,
            pending: bool,
        }
        impl Drop for Admission<'_> {
            fn drop(&mut self) {
                if self.pending {
                    self.gc.metrics.failed_request_counter_with_others.inc();
                    sub(
                        Some(&mut lock(&self.gc.mu).consumption),
                        Some(self.reported_delta),
                    );
                }
            }
        }
        let mut admission = Admission {
            gc: self,
            reported_delta: &reported_delta,
            pending: true,
        };

        let mut wait_duration = Duration::ZERO;
        if !self.burstable.load(Ordering::SeqCst) {
            let (d, result) = self
                .acquire_tokens(ctx, &delta, &mut wait_duration, false)
                .await;
            if let Err(error) = result {
                admission.pending = false;
                if is_throttled_error(&error) {
                    self.metrics.failed_request_counter_with_throttled.inc();
                    self.metrics
                        .failed_limit_reserve_duration
                        .observe(d.as_secs_f64());
                } else {
                    self.metrics.failed_request_counter_with_others.inc();
                }
                sub(Some(&mut lock(&self.mu).consumption), Some(&reported_delta));
                if fail::eval("triggerUpdate", |_| ()).is_some() {
                    let _ = self.low_ru_notify_chan.send(NotifyMsg::default()).await;
                }
                return Err(error);
            }
            self.metrics
                .successful_request_duration
                .observe(d.as_secs_f64());
            wait_duration += d;
        }
        admission.pending = false;
        let (bytes_for_est, ok) = paging_read_estimate(info);
        if ok {
            self.metrics.observe_paging_request(bytes_for_est);
        }

        let penalty = {
            let mut mu = lock(&self.mu);
            let mu = &mut *mu;
            // The penalty of the store.
            let mut penalty = Consumption::default();
            let store_id = info.store_id();
            match mu.store_counter.get(&store_id) {
                Some(store_counter) => {
                    penalty = mu.global_counter.clone();
                    sub(Some(&mut penalty), Some(store_counter));
                }
                None => {
                    mu.store_counter.insert(store_id, Consumption::default());
                }
            }
            // Resetting on success would let concurrent requests piggyback a
            // large delta and inflate the penalty; failures are rare, so the
            // counter is reset here.
            mu.store_counter.insert(store_id, mu.global_counter.clone());
            penalty
        };
        self.metrics
            .add_request_source_ru_delta(request_source(info), Some(&reported_delta));

        Ok((delta, penalty, wait_duration, self.get_meta().priority))
    }

    /// Go `onResponseImpl`.
    pub(crate) fn on_response_impl(
        &self,
        req: &dyn RequestInfo,
        resp: &dyn ResponseInfo,
    ) -> crate::Result<Consumption> {
        let mut delta = Consumption::default();
        for calc in &self.calculators {
            calc.after_kv_request(&mut delta, req, resp);
        }
        let reported_delta =
            reported_response_consumption(&self.calculators, req, &delta).into_owned();
        // `count` is the full per-request consumption (before + after).
        let mut count = delta.clone();
        for calc in &self.calculators {
            calc.before_kv_request(&mut count, req);
        }
        let (bytes_for_est, is_paging_read) = paging_read_estimate(req);
        if is_paging_read {
            self.metrics
                .observe_paging_response(bytes_for_est, resp.read_bytes());
        }
        if !self.burstable.load(Ordering::SeqCst) {
            let counter = &self.request_unit_tokens;
            let v = get_ru_value_from_consumption(Some(&delta));
            if v > 0.0 {
                counter.limiter.remove_tokens(Instant::now(), v);
            } else if v < 0.0 && is_paging_read && bytes_for_est > 0 {
                // Paging over-estimate: refund the excess pre-charge.
                counter.limiter.refund_tokens(Instant::now(), -v);
            }
        }

        self.account_response(req, &reported_delta, &count);
        self.metrics
            .add_request_source_ru_delta(request_source(req), Some(&reported_delta));

        Ok(delta)
    }

    /// Go `onResponseWaitImpl`.
    pub(crate) async fn on_response_wait_impl(
        &self,
        ctx: &Context,
        req: &dyn RequestInfo,
        resp: &dyn ResponseInfo,
    ) -> crate::Result<(Consumption, Duration)> {
        let mut delta = Consumption::default();
        for calc in &self.calculators {
            calc.after_kv_request(&mut delta, req, resp);
        }
        let reported_delta =
            reported_response_consumption(&self.calculators, req, &delta).into_owned();
        // `count` is the full per-request consumption (before + after).
        let mut count = delta.clone();
        for calc in &self.calculators {
            calc.before_kv_request(&mut count, req);
        }
        let (bytes_for_est, is_paging_read) = paging_read_estimate(req);
        let mut wait_duration = Duration::ZERO;
        if !self.burstable.load(Ordering::SeqCst) {
            let v = get_ru_value_from_consumption(Some(&delta));
            if v > 0.0 && is_paging_read && bytes_for_est > 0 {
                // The pre-charged request already consumed TiKV resources:
                // debit the positive settlement now so later requests see the
                // debt, instead of queueing this completed request.
                self.request_unit_tokens
                    .limiter
                    .remove_tokens(Instant::now(), v);
                self.metrics.successful_request_duration.observe(0.0);
            } else if v > 0.0 {
                let allow_debt = delta.read_bytes + delta.write_bytes
                    < BIG_REQUEST_THRESHOLD as f64
                    || !self.is_throttled.load(Ordering::SeqCst);
                let (d, result) = self
                    .acquire_tokens(ctx, &delta, &mut wait_duration, allow_debt)
                    .await;
                if let Err(error) = result {
                    if is_throttled_error(&error) {
                        self.metrics.failed_request_counter_with_throttled.inc();
                        self.metrics
                            .failed_limit_reserve_duration
                            .observe(d.as_secs_f64());
                    } else {
                        self.metrics.failed_request_counter_with_others.inc();
                    }
                    return Err(error);
                }
                self.metrics
                    .successful_request_duration
                    .observe(d.as_secs_f64());
                wait_duration += d;
            } else if v < 0.0 && is_paging_read && bytes_for_est > 0 {
                // Paging over-estimate: refund the excess pre-charge.
                self.request_unit_tokens
                    .limiter
                    .refund_tokens(Instant::now(), -v);
                self.metrics.successful_request_duration.observe(0.0);
            } else {
                self.metrics.successful_request_duration.observe(0.0);
            }
        }
        if is_paging_read {
            self.metrics
                .observe_paging_response(bytes_for_est, resp.read_bytes());
        }

        self.account_response(req, &reported_delta, &count);
        self.metrics
            .add_request_source_ru_delta(request_source(req), Some(&reported_delta));

        Ok((delta, wait_duration))
    }

    /// The `mu` updates shared by both response paths. Go adds to the store
    /// counter only when the request path created it (a nil map entry is
    /// skipped by `add`).
    fn account_response(
        &self,
        req: &dyn RequestInfo,
        reported_delta: &Consumption,
        count: &Consumption,
    ) {
        let mut mu = lock(&self.mu);
        let mu = &mut *mu;
        add(Some(&mut mu.consumption), Some(reported_delta));
        add(mu.store_counter.get_mut(&req.store_id()), Some(count));
        add(Some(&mut mu.global_counter), Some(count));
    }

    pub(crate) fn add_ru_consumption(&self, consumption: Option<&Consumption>) {
        add(Some(&mut lock(&self.mu).consumption), consumption);
    }

    pub(crate) fn add_ruv2_consumption(&self, tikv_ruv2: f64, tidb_ruv2: f64, tiflash_ruv2: f64) {
        let mut mu = lock(&self.mu);
        mu.consumption.tikv_r_u_v2 += tikv_ruv2;
        mu.consumption.tidb_r_u_v2 += tidb_ruv2;
        mu.consumption.tiflash_r_u_v2 += tiflash_ruv2;
    }

    /// Test-only lookup of the KV calculator.
    pub(crate) fn get_kv_calculator(&self) -> Option<&KVCalculator> {
        self.calculators
            .iter()
            .find_map(|calc| calc.as_kv_calculator())
    }
}

/// Stops the setup timer and closes its cancel channel.
pub(crate) fn init_counter_notify(counter: &TokenCounter) {
    let cancel_ch = {
        let mut notify = lock(&counter.notify);
        reset_counter_notify_locked(&mut notify)
    };
    if let Some(cancel_ch) = cancel_ch {
        cancel_ch.cancel();
    }
}

pub(crate) fn reset_counter_notify_locked(
    notify: &mut TokenCounterNotify,
) -> Option<Arc<Cancellation>> {
    let cancel_ch = notify.cancel_ch.take();
    notify.setup_notification_deadline = None;
    cancel_ch
}

/// `a - b` in seconds, negative when `a` is before `b`.
fn signed_seconds(a: Instant, b: Instant) -> f64 {
    match a.checked_duration_since(b) {
        Some(d) => d.as_secs_f64(),
        None => -(b - a).as_secs_f64(),
    }
}
