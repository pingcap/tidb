// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go master `pkg/mvservice/service_settings.go`.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_timer::go_time::SECOND;

/// Go `time.Millisecond`, in nanoseconds.
const MILLISECOND: i64 = 1_000_000;

use crate::backpressure::CpuMemBackpressureController;
use crate::executor::TaskExecutor;
use crate::service::{
    Helper, MVService, TaskBackpressureConfig, DEFAULT_CH_REPLICAS, DEFAULT_MLOG_ANALYZE_TASK_CONCURRENCY,
    DEFAULT_MV_BASIC_INTERVAL, DEFAULT_MV_FETCH_INTERVAL, DEFAULT_MV_HISTORY_GC_INTERVAL, DEFAULT_MV_HISTORY_GC_RETENTION,
    DEFAULT_MV_PURGE_TASK_TIMEOUT, DEFAULT_MV_REFRESH_TASK_CONCURRENCY_RATIO, DEFAULT_MV_REFRESH_TASK_TIMEOUT,
    DEFAULT_MV_TASK_RETRY_BASE, DEFAULT_MV_TASK_RETRY_MAX, DEFAULT_SERVER_REFRESH_INTERVAL,
};
use crate::task_handler::SessionPool;
use crate::time_proxy::{mvs_now, unix_milli};

/// Go `Config`: how an [`MVService`] is built. Durations are nanoseconds.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Config {
    /// Go `TaskMaxConcurrency`.
    pub task_max_concurrency: i64,
    /// Go `RefreshTaskConcurrencyRatio`.
    pub refresh_task_concurrency_ratio: f64,
    /// Go `RefreshTaskTimeout`.
    pub refresh_task_timeout: i64,
    /// Go `PurgeTaskTimeout`.
    pub purge_task_timeout: i64,
    /// Go `MLogAnalyzeTaskConcurrency`.
    pub mlog_analyze_task_concurrency: i64,
    /// Go `FetchInterval`.
    pub fetch_interval: i64,
    /// Go `BasicInterval`.
    pub basic_interval: i64,
    /// Go `ServerRefreshInterval`.
    pub server_refresh_interval: i64,
    /// Go `RetryBaseDelay`.
    pub retry_base_delay: i64,
    /// Go `RetryMaxDelay`.
    pub retry_max_delay: i64,
    /// Go `MViewRefreshHistRetention`.
    pub mview_refresh_hist_retention: i64,
    /// Go `MLogPurgeHistRetention`.
    pub mlog_purge_hist_retention: i64,
    /// Go `ServerConsistentHashReplicas`.
    pub server_consistent_hash_replicas: i64,
    /// Go `TaskBackpressure`.
    pub task_backpressure: TaskBackpressureConfig,
}

/// Go `DefaultMVServiceConfig`.
#[must_use]
pub fn default_mv_service_config() -> Config {
    Config {
        task_max_concurrency: default_mv_task_max_concurrency(),
        refresh_task_concurrency_ratio: DEFAULT_MV_REFRESH_TASK_CONCURRENCY_RATIO,
        refresh_task_timeout: DEFAULT_MV_REFRESH_TASK_TIMEOUT,
        purge_task_timeout: DEFAULT_MV_PURGE_TASK_TIMEOUT,
        mlog_analyze_task_concurrency: DEFAULT_MLOG_ANALYZE_TASK_CONCURRENCY,
        fetch_interval: DEFAULT_MV_FETCH_INTERVAL,
        basic_interval: DEFAULT_MV_BASIC_INTERVAL,
        server_refresh_interval: DEFAULT_SERVER_REFRESH_INTERVAL,
        retry_base_delay: DEFAULT_MV_TASK_RETRY_BASE,
        retry_max_delay: DEFAULT_MV_TASK_RETRY_MAX,
        mview_refresh_hist_retention: DEFAULT_MV_HISTORY_GC_RETENTION,
        mlog_purge_hist_retention: DEFAULT_MV_HISTORY_GC_RETENTION,
        server_consistent_hash_replicas: DEFAULT_CH_REPLICAS,
        task_backpressure: TaskBackpressureConfig::default(),
    }
}

/// Go `defaultMVTaskMaxConcurrency`: `max(2, GOMAXPROCS)`.
pub(crate) fn default_mv_task_max_concurrency() -> i64 {
    let max_procs = std::thread::available_parallelism().map_or(0, |n| n.get() as i64);
    if max_procs > 0 {
        return max_procs.max(2);
    }
    2
}

/// Go `normalizeMVServiceConfig`: invalid values clamp to safe defaults.
#[must_use]
pub fn normalize_mv_service_config(mut cfg: Config) -> Config {
    let def = default_mv_service_config();
    if cfg.task_max_concurrency <= 0 {
        cfg.task_max_concurrency = def.task_max_concurrency;
    }
    if cfg.task_max_concurrency < 2 {
        cfg.task_max_concurrency = 2;
    }
    if cfg.refresh_task_concurrency_ratio <= 0.0 || cfg.refresh_task_concurrency_ratio >= 1.0 {
        cfg.refresh_task_concurrency_ratio = def.refresh_task_concurrency_ratio;
    }
    if cfg.refresh_task_timeout < 0 {
        cfg.refresh_task_timeout = 0;
    }
    if cfg.purge_task_timeout < 0 {
        cfg.purge_task_timeout = 0;
    }
    if cfg.mlog_analyze_task_concurrency <= 0 {
        cfg.mlog_analyze_task_concurrency = def.mlog_analyze_task_concurrency;
    }
    if cfg.fetch_interval <= 0 {
        cfg.fetch_interval = def.fetch_interval;
    }
    if cfg.basic_interval <= 0 {
        cfg.basic_interval = def.basic_interval;
    }
    if cfg.server_refresh_interval <= 0 {
        cfg.server_refresh_interval = def.server_refresh_interval;
    }
    if cfg.retry_base_delay <= 0 {
        cfg.retry_base_delay = def.retry_base_delay;
    }
    if cfg.retry_max_delay <= 0 {
        cfg.retry_max_delay = def.retry_max_delay;
    }
    if cfg.mview_refresh_hist_retention <= 0 {
        cfg.mview_refresh_hist_retention = def.mview_refresh_hist_retention;
    }
    if cfg.mlog_purge_hist_retention <= 0 {
        cfg.mlog_purge_hist_retention = def.mlog_purge_hist_retention;
    }
    if cfg.server_consistent_hash_replicas <= 0 {
        cfg.server_consistent_hash_replicas = def.server_consistent_hash_replicas;
    }
    cfg
}

/// Go `splitTaskConcurrency`: the refresh and purge shares of `total`.
#[must_use]
pub fn split_task_concurrency(total: i64, refresh_ratio: f64) -> (i64, i64) {
    let total = total.max(2);
    let refresh_ratio = if refresh_ratio <= 0.0 || refresh_ratio >= 1.0 {
        DEFAULT_MV_REFRESH_TASK_CONCURRENCY_RATIO
    } else {
        refresh_ratio
    };
    // Go `math.Round`: half away from zero, as `f64::round`.
    let mut refresh = (total as f64 * refresh_ratio).round() as i64;
    if refresh < 1 {
        refresh = 1;
    }
    if refresh >= total {
        refresh = total - 1;
    }
    let mut purge = total - refresh;
    if purge < 1 {
        purge = 1;
        refresh = total - purge;
    }
    (refresh, purge)
}

/// Go `NewMVService`.
///
/// # Panics
/// A configuration the setters refuse, as Go's constructor panics.
#[must_use]
pub fn new_mv_service(ctx: Context, se: Arc<dyn SessionPool>, helper: Arc<dyn Helper>, cfg: Config) -> Arc<MVService> {
    let cfg = normalize_mv_service_config(cfg);
    let (refresh_concurrency, purge_concurrency) =
        split_task_concurrency(cfg.task_max_concurrency, cfg.refresh_task_concurrency_ratio);
    let mgr = Arc::new(MVService::construct(
        ctx,
        se,
        helper,
        cfg.server_consistent_hash_replicas,
        (
            TaskExecutor::new(refresh_concurrency, cfg.refresh_task_timeout),
            TaskExecutor::new(purge_concurrency, cfg.purge_task_timeout),
            TaskExecutor::new(cfg.mlog_analyze_task_concurrency, 0),
        ),
        cfg.basic_interval,
        cfg.server_refresh_interval,
    ));
    mgr.refresh_task_concurrency_ratio_bits.store(cfg.refresh_task_concurrency_ratio.to_bits(), Ordering::SeqCst);
    if let Err(err) = mgr.set_fetch_interval(cfg.fetch_interval) {
        panic!("invalid MV service fetch interval config: fetch_interval={} err={err}", cfg.fetch_interval);
    }
    if let Err(err) = mgr.set_mview_refresh_hist_retention(cfg.mview_refresh_hist_retention) {
        panic!(
            "invalid MV service mview refresh history retention config: retention={} err={err}",
            cfg.mview_refresh_hist_retention
        );
    }
    if let Err(err) = mgr.set_mlog_purge_hist_retention(cfg.mlog_purge_hist_retention) {
        panic!("invalid MV service mlog purge history retention config: retention={} err={err}", cfg.mlog_purge_hist_retention);
    }
    if let Err(err) = mgr.set_retry_delay_config(cfg.retry_base_delay, cfg.retry_max_delay) {
        panic!("invalid MV service retry config: base={} max={} err={err}", cfg.retry_base_delay, cfg.retry_max_delay);
    }
    if let Err(err) = mgr.set_task_backpressure_config(cfg.task_backpressure) {
        panic!(
            "invalid MV service backpressure config: cpu_threshold={} mem_threshold={} delay={} err={err}",
            cfg.task_backpressure.cpu_threshold, cfg.task_backpressure.mem_threshold, cfg.task_backpressure.delay
        );
    }
    mgr.next_history_gc_at_millis
        .store(unix_milli(&mvs_now().add(mgr.history_gc_interval())), Ordering::SeqCst);
    mgr
}

impl MVService {
    /// Go `SetTaskMaxConcurrency`.
    pub fn set_task_max_concurrency(&self, max_concurrency: i64) {
        let max_concurrency = if max_concurrency == 0 { default_mv_task_max_concurrency() } else { max_concurrency };
        let max_concurrency = max_concurrency.max(2);
        let (refresh, purge) = split_task_concurrency(max_concurrency, self.get_refresh_task_concurrency_ratio());
        self.refresh_executor.set_max_concurrency(refresh);
        self.purge_executor.set_max_concurrency(purge);
    }

    /// Go `SetRefreshTaskConcurrencyRatio`.
    pub fn set_refresh_task_concurrency_ratio(&self, ratio: f64) {
        let ratio = if ratio <= 0.0 || ratio >= 1.0 { DEFAULT_MV_REFRESH_TASK_CONCURRENCY_RATIO } else { ratio };
        self.refresh_task_concurrency_ratio_bits.store(ratio.to_bits(), Ordering::SeqCst);
        let mut total = self.refresh_executor.max_concurrency.load(Ordering::SeqCst)
            + self.purge_executor.max_concurrency.load(Ordering::SeqCst);
        if total < 2 {
            total = default_mv_task_max_concurrency();
        }
        let (refresh, purge) = split_task_concurrency(total, ratio);
        self.refresh_executor.set_max_concurrency(refresh);
        self.purge_executor.set_max_concurrency(purge);
    }

    /// Go `GetRefreshTaskConcurrencyRatio`.
    #[must_use]
    pub fn get_refresh_task_concurrency_ratio(&self) -> f64 {
        let ratio = f64::from_bits(self.refresh_task_concurrency_ratio_bits.load(Ordering::SeqCst));
        if ratio <= 0.0 || ratio >= 1.0 {
            return DEFAULT_MV_REFRESH_TASK_CONCURRENCY_RATIO;
        }
        ratio
    }

    /// Go `setFetchInterval`.
    pub(crate) fn set_fetch_interval(&self, interval: i64) -> Result<(), String> {
        if interval <= 0 {
            return Err("fetch interval must be positive".to_owned());
        }
        if interval < MILLISECOND {
            return Err("fetch interval must be at least 1ms".to_owned());
        }
        self.fetch_interval_millis.store(interval / MILLISECOND, Ordering::SeqCst);
        Ok(())
    }

    /// Go `fetchInterval`.
    pub(crate) fn fetch_interval(&self) -> i64 {
        let interval = self.fetch_interval_millis.load(Ordering::SeqCst) * MILLISECOND;
        if interval <= 0 {
            return DEFAULT_MV_FETCH_INTERVAL;
        }
        interval
    }

    /// Go `setRetryDelayConfig`.
    pub(crate) fn set_retry_delay_config(&self, base_delay: i64, max_delay: i64) -> Result<(), String> {
        if base_delay <= 0 || max_delay <= 0 {
            return Err("retry delay must be positive".to_owned());
        }
        if base_delay < MILLISECOND || max_delay < MILLISECOND {
            return Err("retry delay must be at least 1ms".to_owned());
        }
        if base_delay > max_delay {
            return Err("retry base delay must be less than or equal to max delay".to_owned());
        }
        self.retry_base_delay_millis.store(base_delay / MILLISECOND, Ordering::SeqCst);
        self.retry_max_delay_millis.store(max_delay / MILLISECOND, Ordering::SeqCst);
        Ok(())
    }

    /// Go `retryDelayConfig`.
    pub(crate) fn retry_delay_config(&self) -> (i64, i64) {
        let mut base_delay = self.retry_base_delay_millis.load(Ordering::SeqCst) * MILLISECOND;
        let mut max_delay = self.retry_max_delay_millis.load(Ordering::SeqCst) * MILLISECOND;
        if base_delay <= 0 {
            base_delay = DEFAULT_MV_TASK_RETRY_BASE;
        }
        if max_delay <= 0 {
            max_delay = DEFAULT_MV_TASK_RETRY_MAX;
        }
        if base_delay > max_delay {
            max_delay = base_delay;
        }
        (base_delay, max_delay)
    }

    /// Go `SetTaskBackpressureConfig`.
    ///
    /// # Errors
    /// A threshold outside [0, 1] or a negative delay.
    pub fn set_task_backpressure_config(&self, cfg: TaskBackpressureConfig) -> Result<(), String> {
        if !(0.0..=1.0).contains(&cfg.cpu_threshold) {
            return Err("cpu threshold out of range".to_owned());
        }
        if !(0.0..=1.0).contains(&cfg.mem_threshold) {
            return Err("memory threshold out of range".to_owned());
        }
        if cfg.delay < 0 {
            return Err("backpressure delay must be non-negative".to_owned());
        }
        if cfg.cpu_threshold > 0.0 || cfg.mem_threshold > 0.0 {
            self.set_task_backpressure_controller(Some(Arc::new(CpuMemBackpressureController::new(
                cfg.cpu_threshold,
                cfg.mem_threshold,
                cfg.delay,
            ))));
        } else {
            self.set_task_backpressure_controller(None);
        }
        *self.backpressure_cfg.write().unwrap() = cfg;
        Ok(())
    }

    /// Go `GetTaskBackpressureConfig`.
    #[must_use]
    pub fn get_task_backpressure_config(&self) -> TaskBackpressureConfig {
        *self.backpressure_cfg.read().unwrap()
    }

    /// Go `historyGCInterval`.
    pub(crate) fn history_gc_interval(&self) -> i64 {
        let (mview_refresh_retention, mlog_purge_retention) = self.history_gc_retention_config();
        derive_history_gc_interval(mview_refresh_retention, mlog_purge_retention)
    }

    /// Go `SetMViewRefreshHistRetention`: the retention of
    /// `mysql.tidb_mview_refresh_hist`.
    ///
    /// # Errors
    /// A retention under 1ms.
    pub fn set_mview_refresh_hist_retention(&self, retention: i64) -> Result<(), String> {
        if retention <= 0 {
            return Err("history gc retention must be positive".to_owned());
        }
        if retention < MILLISECOND {
            return Err("history gc retention must be at least 1ms".to_owned());
        }
        self.mview_refresh_hist_retention_millis.store(retention / MILLISECOND, Ordering::SeqCst);
        self.reschedule_history_gc_earlier(&mvs_now(), self.history_gc_interval());
        Ok(())
    }

    /// Go `SetMLogPurgeHistRetention`: the retention of
    /// `mysql.tidb_mlog_purge_hist`.
    ///
    /// # Errors
    /// A retention under 1ms.
    pub fn set_mlog_purge_hist_retention(&self, retention: i64) -> Result<(), String> {
        if retention <= 0 {
            return Err("history gc retention must be positive".to_owned());
        }
        if retention < MILLISECOND {
            return Err("history gc retention must be at least 1ms".to_owned());
        }
        self.mlog_purge_hist_retention_millis.store(retention / MILLISECOND, Ordering::SeqCst);
        self.reschedule_history_gc_earlier(&mvs_now(), self.history_gc_interval());
        Ok(())
    }

    /// Go `historyGCRetentionConfig`.
    pub(crate) fn history_gc_retention_config(&self) -> (i64, i64) {
        let mut mview_refresh_retention = self.mview_refresh_hist_retention_millis.load(Ordering::SeqCst) * MILLISECOND;
        let mut mlog_purge_retention = self.mlog_purge_hist_retention_millis.load(Ordering::SeqCst) * MILLISECOND;
        if mview_refresh_retention <= 0 {
            mview_refresh_retention = DEFAULT_MV_HISTORY_GC_RETENTION;
        }
        if mlog_purge_retention <= 0 {
            mlog_purge_retention = DEFAULT_MV_HISTORY_GC_RETENTION;
        }
        (mview_refresh_retention, mlog_purge_retention)
    }

    /// Go `retryDelay`: the current retry delay.
    pub(crate) fn retry_delay(&self, retry_count: i64) -> i64 {
        let (base_delay, max_delay) = self.retry_delay_config();
        calc_retry_delay(retry_count, base_delay, max_delay)
    }
}

/// Go `deriveHistoryGCInterval`.
#[must_use]
pub fn derive_history_gc_interval(mview_refresh_retention: i64, mlog_purge_retention: i64) -> i64 {
    let interval = mview_refresh_retention.min(mlog_purge_retention) / 100;
    if interval < SECOND {
        return SECOND;
    }
    interval.min(DEFAULT_MV_HISTORY_GC_INTERVAL)
}

/// Go `calcRetryDelay`: exponential backoff with an upper bound.
#[must_use]
pub fn calc_retry_delay(retry_count: i64, base_delay: i64, max_delay: i64) -> i64 {
    if retry_count <= 0 {
        return base_delay;
    }
    let mut delay = base_delay;
    let mut i = 1;
    while i < retry_count && delay < max_delay {
        delay = delay.saturating_mul(2);
        if delay >= max_delay {
            delay = max_delay;
            break;
        }
        i += 1;
    }
    delay
}
