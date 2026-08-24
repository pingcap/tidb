//! Stateful retry budgeting adapted from client-go's `config/retry` package.
//!
//! This is intentionally distinct from the legacy public [`crate::Backoff`]
//! attempt-count helper. A `RetryBackoffer` owns the source-compatible shared
//! cumulative budget, per-error-class exponential state, cancellation, and
//! retry diagnostics needed by higher-level client paths.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::async_util::Cancellation;
use crate::error::{QueryInterruptedWithSignalError, StaticError, MISMATCH_CLUSTER_ID};
use crate::kv::{Variables, DEFAULT_VARIABLES};
use crate::proto::errorpb;
use rand::{thread_rng, Rng};

/// Maximum number of recent retry errors retained for exhaustion diagnostics.
pub const MAX_RECORD_BACKOFF_ERR_COUNT: usize = 3;

/// Source jitter modes used by a retry class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Jitter {
    No,
    Full,
    Equal,
    Decorrelated,
}

/// The concrete source error selected when the cumulative retry budget is
/// exhausted. PD timeout is not a singleton in client-go, so it must remain
/// distinct from the TiKV RPC timeout class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryTerminal {
    Static(StaticError),
    PdServerTimeout,
}

impl fmt::Display for RetryTerminal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Static(error) => error.fmt(formatter),
            // `BoPDRPC` constructs `NewErrPDServerTimeout("")` in client-go.
            Self::PdServerTimeout => Ok(()),
        }
    }
}

impl std::error::Error for RetryTerminal {}

/// Immutable source retry-class definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryConfig {
    pub name: &'static str,
    pub base_ms: u64,
    pub cap_ms: u64,
    pub jitter: Jitter,
    pub terminal_error: RetryTerminal,
    pub excluded_budget_limit_ms: Option<u64>,
    metric_label: Option<&'static str>,
}

impl RetryConfig {
    pub const fn new(
        name: &'static str,
        base_ms: u64,
        cap_ms: u64,
        jitter: Jitter,
        terminal_error: StaticError,
    ) -> Self {
        Self {
            name,
            base_ms,
            cap_ms,
            jitter,
            terminal_error: RetryTerminal::Static(terminal_error),
            excluded_budget_limit_ms: None,
            metric_label: None,
        }
    }

    pub const fn new_pd_timeout(
        name: &'static str,
        base_ms: u64,
        cap_ms: u64,
        jitter: Jitter,
    ) -> Self {
        Self {
            name,
            base_ms,
            cap_ms,
            jitter,
            terminal_error: RetryTerminal::PdServerTimeout,
            excluded_budget_limit_ms: None,
            metric_label: None,
        }
    }

    pub const fn excluding_budget(mut self, limit_ms: u64) -> Self {
        self.excluded_budget_limit_ms = Some(limit_ms);
        self
    }

    /// Returns the initial sleep selected by this retry class.
    pub const fn base(self) -> u64 {
        self.base_ms
    }

    /// Replaces the exponential-backoff parameters for a locally owned copy.
    ///
    /// This is Rust's value-semantic counterpart of client-go's mutable
    /// `SetBackoffFnCfg`: the process-wide constants remain race-free while a
    /// caller can install the same test- or request-specific policy.
    pub const fn with_backoff_fn(mut self, base_ms: u64, cap_ms: u64, jitter: Jitter) -> Self {
        self.base_ms = base_ms;
        self.cap_ms = cap_ms;
        self.jitter = jitter;
        self
    }

    /// Replaces the terminal error for a locally owned copy.
    pub const fn with_terminal(mut self, terminal_error: RetryTerminal) -> Self {
        self.terminal_error = terminal_error;
        self
    }

    const fn with_metric_label(mut self, metric_label: &'static str) -> Self {
        self.metric_label = Some(metric_label);
        self
    }
}

impl fmt::Display for RetryConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name)
    }
}

pub const BO_TIKV_RPC: RetryConfig = RetryConfig::new(
    "tikvRPC",
    100,
    2_000,
    Jitter::Equal,
    StaticError::TiKvServerTimeout,
)
.with_metric_label("tikvRPC");
pub const BO_TIFLASH_RPC: RetryConfig = RetryConfig::new(
    "tiflashRPC",
    100,
    2_000,
    Jitter::Equal,
    StaticError::TiFlashServerTimeout,
)
.with_metric_label("tikvRPC");
pub const BO_TXN_LOCK: RetryConfig = RetryConfig::new(
    "txnLock",
    100,
    3_000,
    Jitter::Equal,
    StaticError::ResolveLockTimeout,
)
.with_metric_label("txnLock");
pub const BO_PD_RPC: RetryConfig =
    RetryConfig::new_pd_timeout("pdRPC", 500, 3_000, Jitter::Equal).with_metric_label("pdRPC");
pub const BO_REGION_MISS: RetryConfig = RetryConfig::new(
    "regionMiss",
    2,
    500,
    Jitter::No,
    StaticError::RegionUnavailable,
)
.with_metric_label("regionMiss");
pub const BO_REGION_SCHEDULING: RetryConfig = RetryConfig::new(
    "regionScheduling",
    2,
    500,
    Jitter::No,
    StaticError::RegionUnavailable,
)
.with_metric_label("regionScheduling");
pub const BO_TIKV_SERVER_BUSY: RetryConfig = RetryConfig::new(
    "tikvServerBusy",
    2_000,
    10_000,
    Jitter::Equal,
    StaticError::TiKvServerBusy,
)
.with_metric_label("serverBusy")
.excluding_budget(600_000);
pub const BO_TIKV_DISK_FULL: RetryConfig = RetryConfig::new(
    "tikvDiskFull",
    500,
    5_000,
    Jitter::No,
    StaticError::TiKvDiskFull,
)
.with_metric_label("tikvDiskFull");
pub const BO_REGION_RECOVERY_IN_PROGRESS: RetryConfig = RetryConfig::new(
    "regionRecoveryInProgress",
    100,
    10_000,
    Jitter::Equal,
    StaticError::RegionRecoveryInProgress,
)
.with_metric_label("regionRecoveryInProgress");
pub const BO_TIFLASH_SERVER_BUSY: RetryConfig = RetryConfig::new(
    "tiflashServerBusy",
    2_000,
    10_000,
    Jitter::Equal,
    StaticError::TiFlashServerBusy,
)
.with_metric_label("serverBusy");
pub const BO_TXN_NOT_FOUND: RetryConfig = RetryConfig::new(
    "txnNotFound",
    2,
    500,
    Jitter::No,
    StaticError::ResolveLockTimeout,
)
.with_metric_label("");
pub const BO_STALE_CMD: RetryConfig = RetryConfig::new(
    "staleCommand",
    2,
    1_000,
    Jitter::No,
    StaticError::TiKvStaleCommand,
)
.with_metric_label("staleCommand");
pub const BO_MAX_TS_NOT_SYNCED: RetryConfig = RetryConfig::new(
    "maxTsNotSynced",
    2,
    500,
    Jitter::No,
    StaticError::TiKvMaxTimestampNotSynced,
)
.with_metric_label("");
pub const BO_COMMIT_TS_LAG: RetryConfig = RetryConfig::new(
    "commitTSLag",
    2,
    500,
    Jitter::No,
    StaticError::CommitTimestampLag,
)
.with_metric_label("");
pub const BO_MAX_REGION_NOT_INITIALIZED: RetryConfig = RetryConfig::new(
    "regionNotInitialized",
    2,
    1_000,
    Jitter::No,
    StaticError::RegionNotInitialized,
)
.with_metric_label("");
pub const BO_IS_WITNESS: RetryConfig = RetryConfig::new(
    "isWitness",
    1_000,
    10_000,
    Jitter::Equal,
    StaticError::IsWitness,
)
.with_metric_label("isWitness");
pub const BO_TXN_LOCK_FAST: RetryConfig = RetryConfig::new(
    "txnLockFast",
    2,
    3_000,
    Jitter::Equal,
    StaticError::ResolveLockTimeout,
)
.with_metric_label("tikvLockFast");

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackoffRecord {
    pub reason: String,
    pub time: SystemTime,
}

impl fmt::Display for BackoffRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} at {}",
            self.reason,
            format_rfc3339_nanos(self.time)
        )
    }
}

#[derive(Debug)]
pub enum RetryError {
    /// client-go returns the triggering error unchanged when its context is
    /// already cancelled before a backoff begins.
    Cancelled {
        reason: String,
    },
    /// Source noop backoffers return their triggering error directly.
    Noop {
        reason: String,
    },
    /// A cluster mismatch is fatal to this retry owner. Rust returns it to the
    /// embedding process instead of terminating the process from a library.
    ClusterIdMismatch {
        reason: String,
    },
    Interrupted(QueryInterruptedWithSignalError),
    Exhausted {
        max_sleep_ms: u64,
        /// The non-excluded class that consumed the most sleep. `None`
        /// preserves client-go's fallback to the triggering error when only
        /// excluded classes have consumed the budget.
        terminal: Option<RetryTerminal>,
        reason: String,
        recent_errors: Vec<BackoffRecord>,
    },
    KillHandler(crate::Error),
}

impl fmt::Display for RetryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled { reason } | Self::Noop { reason } => formatter.write_str(reason),
            Self::ClusterIdMismatch { reason } => write!(formatter, "critical error: {reason}"),
            Self::Interrupted(error) => error.fmt(formatter),
            Self::Exhausted {
                terminal: Some(terminal),
                ..
            } => terminal.fmt(formatter),
            Self::Exhausted { reason, .. } => formatter.write_str(reason),
            Self::KillHandler(error) => write!(formatter, "kill-signal handler failed: {error}"),
        }
    }
}

impl std::error::Error for RetryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Interrupted(error) => Some(error),
            Self::Exhausted {
                terminal: Some(terminal),
                ..
            } => Some(terminal),
            Self::KillHandler(error) => Some(error),
            _ => None,
        }
    }
}

impl From<QueryInterruptedWithSignalError> for RetryError {
    fn from(error: QueryInterruptedWithSignalError) -> Self {
        Self::Interrupted(error)
    }
}

#[derive(Clone, Debug)]
struct BackoffFnState {
    attempts: u32,
    last_sleep_ms: u64,
}

/// A cumulative retry budget. A zero max sleep is unlimited, matching Go.
pub struct RetryBackoffer {
    id: u64,
    ancestors: Vec<u64>,
    ancestor_types: Vec<&'static str>,
    cancellation: Cancellation,
    max_sleep_ms: u64,
    total_sleep_ms: u64,
    excluded_sleep_ms: u64,
    noop: bool,
    variables: Arc<Variables>,
    functions: BTreeMap<&'static str, BackoffFnState>,
    errors: Vec<BackoffRecord>,
    errors_num: usize,
    sleep_by_type: BTreeMap<&'static str, u64>,
    times_by_type: BTreeMap<&'static str, u64>,
    configs: Vec<RetryConfig>,
}

static NEXT_BACKOFFER_ID: AtomicU64 = AtomicU64::new(1);

impl RetryBackoffer {
    pub fn new(cancellation: Cancellation, max_sleep_ms: u64) -> Self {
        Self::with_variables(cancellation, max_sleep_ms, DEFAULT_VARIABLES.clone())
    }

    pub fn with_variables(
        cancellation: Cancellation,
        max_sleep_ms: u64,
        variables: Arc<Variables>,
    ) -> Self {
        let max_sleep_ms = Self::weighted_max_sleep(max_sleep_ms, &variables);
        Self {
            id: NEXT_BACKOFFER_ID.fetch_add(1, Ordering::Relaxed),
            ancestors: Vec::new(),
            ancestor_types: Vec::new(),
            cancellation,
            max_sleep_ms,
            total_sleep_ms: 0,
            excluded_sleep_ms: 0,
            noop: false,
            variables,
            functions: BTreeMap::new(),
            errors: Vec::new(),
            errors_num: 0,
            sleep_by_type: BTreeMap::new(),
            times_by_type: BTreeMap::new(),
            configs: Vec::new(),
        }
    }

    pub fn noop(cancellation: Cancellation) -> Self {
        Self {
            noop: true,
            ..Self::new(cancellation, 0)
        }
    }

    pub fn total_sleep_ms(&self) -> u64 {
        self.total_sleep_ms
    }

    #[cfg(test)]
    pub(crate) fn max_sleep_ms(&self) -> u64 {
        self.max_sleep_ms
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }
    pub fn excluded_sleep_ms(&self) -> u64 {
        self.excluded_sleep_ms
    }
    pub fn errors_num(&self) -> usize {
        self.errors_num
    }
    pub fn latest_errors(&self) -> &[BackoffRecord] {
        &self.errors
    }
    pub fn times_by_type(&self) -> &BTreeMap<&'static str, u64> {
        &self.times_by_type
    }
    pub fn sleep_by_type(&self) -> &BTreeMap<&'static str, u64> {
        &self.sleep_by_type
    }
    pub fn total_backoff_times(&self) -> u64 {
        self.times_by_type.values().sum()
    }
    pub fn variables(&self) -> &Arc<Variables> {
        &self.variables
    }

    /// Returns the cancellation scope bound to this retry owner.
    pub fn cancellation(&self) -> &Cancellation {
        &self.cancellation
    }

    /// Replaces the cancellation scope while preserving retry accounting.
    ///
    /// This is the native async equivalent of client-go's `SetCtx`; unrelated
    /// context values are passed explicitly by Rust callers.
    pub fn set_cancellation(&mut self, cancellation: Cancellation) {
        self.cancellation = cancellation;
    }

    /// Returns this backoffer's types followed by every ancestor's types.
    ///
    /// A fork first copies the parent's configs, so—as in client-go—the
    /// parent portion can intentionally duplicate earlier type names.
    pub fn types(&self) -> Vec<&'static str> {
        self.configs
            .iter()
            .map(|config| config.name)
            .chain(self.ancestor_types.iter().copied())
            .collect()
    }

    pub async fn backoff(
        &mut self,
        config: RetryConfig,
        reason: impl Into<String>,
    ) -> Result<(), RetryError> {
        self.backoff_with_config_and_max_sleep(config, None, reason)
            .await
    }

    /// Source `BackoffWithMaxSleepTxnLockFast`: preserves the class's
    /// exponential progression while capping only this individual sleep.
    pub async fn backoff_with_max_sleep_txn_lock_fast(
        &mut self,
        max_sleep_ms: u64,
        reason: impl Into<String>,
    ) -> Result<(), RetryError> {
        self.backoff_with_config_and_max_sleep(BO_TXN_LOCK_FAST, Some(max_sleep_ms), reason)
            .await
    }

    /// Sleeps according to `config`, optionally capping this one sleep without
    /// resetting the class's exponential state.
    ///
    /// `None` maps to client-go's `-1` sentinel in
    /// `BackoffWithCfgAndMaxSleep`.
    pub async fn backoff_with_config_and_max_sleep(
        &mut self,
        config: RetryConfig,
        max_single_sleep_ms: Option<u64>,
        reason: impl Into<String>,
    ) -> Result<(), RetryError> {
        let reason = reason.into();
        if reason.contains(MISMATCH_CLUSTER_ID) {
            log::error!("critical error: {reason}");
            return Err(RetryError::ClusterIdMismatch { reason });
        }
        if self.cancellation.is_cancelled() {
            return Err(RetryError::Cancelled { reason });
        }
        if self.noop {
            return Err(RetryError::Noop { reason });
        }
        if self.budget_exhausted(config) {
            let error = self.exhausted(reason);
            log::warn!(
                "{} backoffer.maxSleep {}ms is exceeded; total-backoff-times: {}; backoff-detail: {:?}; recent-errors: {:?}",
                config,
                self.max_sleep_ms,
                self.total_backoff_times(),
                self.times_by_type,
                self.errors
            );
            return Err(error);
        }
        self.push_error(reason.clone());
        self.configs.push(config);
        let selected_delay_ms = self.next_delay_ms(config);
        let delay_ms =
            max_single_sleep_ms.map_or(selected_delay_ms, |limit| selected_delay_ms.min(limit));
        // Go checks its context before constructing the backoff function. If
        // the context is cancelled while `time.After` is already pending, the
        // function returns a zero sleep and this call still records that
        // retry; the following Backoff call observes cancellation. Preserve
        // that transition rather than returning early from this wait.
        let interrupted = if fail::eval("fastBackoffBySkipSleep", |_| ()).is_some() {
            false
        } else {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => false,
                _ = self.cancellation.cancelled() => true,
            }
        };
        let real_sleep_ms = (!interrupted).then_some(delay_ms).unwrap_or(0);
        if !interrupted {
            // The source keeps the uncapped jitter result as `lastSleep`; the
            // one-call cap changes only wall-clock/accounting duration.
            self.complete_delay(config, selected_delay_ms);
        }
        self.total_sleep_ms = self.total_sleep_ms.saturating_add(real_sleep_ms);
        if config.excluded_budget_limit_ms.is_some() {
            self.excluded_sleep_ms = self.excluded_sleep_ms.saturating_add(real_sleep_ms);
        }
        *self.sleep_by_type.entry(config.name).or_default() += real_sleep_ms;
        *self.times_by_type.entry(config.name).or_default() += 1;
        if let Some(metric_label) = config.metric_label {
            crate::stats::observe_retry_backoff(metric_label, Duration::from_millis(real_sleep_ms));
        }
        self.check_killed()?;
        log::debug!(
            "retry later: reason={reason}; totalSleep={}; excludedSleep={}; maxSleep={}; type={config}",
            self.total_sleep_ms,
            self.excluded_sleep_ms,
            self.max_sleep_ms
        );
        Ok(())
    }

    pub fn check_killed(&self) -> Result<(), RetryError> {
        let signal = self.variables.killed.load(Ordering::Acquire);
        if signal != 0 {
            log::info!("backoff stops because a killed signal is received: signal={signal}");
            return Err(QueryInterruptedWithSignalError { signal }.into());
        }
        if let Some(handler) = &self.variables.kill_signal_handler {
            handler.handle_signal().map_err(RetryError::KillHandler)?;
        }
        Ok(())
    }

    pub fn reset(&mut self) {
        self.functions.clear();
        self.total_sleep_ms = 0;
        self.excluded_sleep_ms = 0;
    }

    /// Mirrors `MayBackoffForRegionError`: a real epoch mismatch already
    /// carries replacement regions and retries immediately; every other
    /// region error (including an empty/fake epoch mismatch) is backoffable.
    pub async fn may_backoff_region_error(
        &mut self,
        error: Option<&errorpb::Error>,
    ) -> Result<(), RetryError> {
        let Some(error) = error else {
            return Ok(());
        };
        if error.epoch_not_match.is_some() && !is_fake_region_error(Some(error)) {
            Ok(())
        } else {
            self.backoff(BO_REGION_MISS, format!("{error:?}")).await
        }
    }

    /// Source `ResetMaxSleep`: starts a new budget stage while retaining
    /// diagnostic accounting that belongs to the broader operation.
    pub fn reset_max_sleep(&mut self, max_sleep_ms: u64) {
        self.reset();
        self.max_sleep_ms = Self::weighted_max_sleep(max_sleep_ms, &self.variables);
    }

    /// Creates a cancellable child with copied accounting and fresh per-class
    /// exponential functions, matching client-go's `Fork`.
    pub fn fork(&self) -> (Self, Cancellation) {
        let cancellation = self.cancellation.child();
        let mut ancestors = self.ancestors.clone();
        ancestors.push(self.id);
        let ancestor_types = self.types();
        (
            Self {
                id: NEXT_BACKOFFER_ID.fetch_add(1, Ordering::Relaxed),
                ancestors,
                ancestor_types,
                cancellation: cancellation.clone(),
                max_sleep_ms: self.max_sleep_ms,
                total_sleep_ms: self.total_sleep_ms,
                excluded_sleep_ms: self.excluded_sleep_ms,
                // client-go deliberately omits `noop` from Clone and Fork.
                noop: false,
                variables: self.variables.clone(),
                functions: BTreeMap::new(),
                errors: self.errors.clone(),
                errors_num: self.errors_num,
                sleep_by_type: self.sleep_by_type.clone(),
                times_by_type: self.times_by_type.clone(),
                configs: self.configs.clone(),
            },
            cancellation,
        )
    }

    /// Merges mutable accounting from a no-longer-used fork into this exact
    /// ancestor. Unrelated backoffers are ignored like client-go's parent walk.
    pub fn update_using_forked(&mut self, forked: &Self) {
        if !forked.ancestors.contains(&self.id) {
            return;
        }
        self.total_sleep_ms = forked.total_sleep_ms;
        self.excluded_sleep_ms = forked.excluded_sleep_ms;
        self.errors = forked.errors.clone();
        self.errors_num = forked.errors_num;
        self.sleep_by_type = forked.sleep_by_type.clone();
        self.times_by_type = forked.times_by_type.clone();
    }

    fn budget_exhausted(&self, config: RetryConfig) -> bool {
        if self.max_sleep_ms == 0 {
            return false;
        }
        if self.total_sleep_ms.saturating_sub(self.excluded_sleep_ms) >= self.max_sleep_ms {
            return true;
        }
        config.excluded_budget_limit_ms.is_some_and(|limit| {
            self.excluded_sleep_ms >= limit && self.excluded_sleep_ms >= self.max_sleep_ms
        })
    }

    /// client-go applies `BackOffWeight` only when doing so remains within
    /// its signed 32-bit maximum. Large values represent intentionally long
    /// (effectively unbounded) retry budgets and must remain unscaled.
    fn weighted_max_sleep(max_sleep_ms: u64, variables: &Variables) -> u64 {
        let weight = variables.backoff_weight.max(1) as u64;
        let max_weighted_input = (i32::MAX as u64) / weight;
        if max_sleep_ms > 0 && max_sleep_ms <= max_weighted_input {
            max_sleep_ms.saturating_mul(weight)
        } else {
            max_sleep_ms
        }
    }

    fn exhausted(&self, reason: String) -> RetryError {
        let terminal = self
            .configs
            .iter()
            .copied()
            .filter(|config| config.excluded_budget_limit_ms.is_none())
            .max_by_key(|config| {
                self.sleep_by_type
                    .get(config.name)
                    .copied()
                    .unwrap_or_default()
            })
            .map(|config| config.terminal_error);
        RetryError::Exhausted {
            max_sleep_ms: self.max_sleep_ms,
            terminal,
            reason,
            recent_errors: self.errors.clone(),
        }
    }

    fn push_error(&mut self, reason: String) {
        self.errors_num += 1;
        self.errors.push(BackoffRecord {
            reason,
            time: SystemTime::now(),
        });
        if self.errors.len() > MAX_RECORD_BACKOFF_ERR_COUNT {
            self.errors.remove(0);
        }
    }

    fn next_delay_ms(&mut self, config: RetryConfig) -> u64 {
        let base = if config.name == BO_TXN_LOCK_FAST.name {
            self.variables.backoff_lock_fast.max(2) as u64
        } else {
            config.base_ms.max(2)
        };
        let state = self.functions.entry(config.name).or_insert(BackoffFnState {
            attempts: 0,
            last_sleep_ms: base,
        });
        let exponential = base
            .saturating_mul(2u64.saturating_pow(state.attempts))
            .min(config.cap_ms);
        let mut rng = thread_rng();
        let delay = match config.jitter {
            Jitter::No => exponential,
            Jitter::Full => rng.gen_range(0..exponential),
            Jitter::Equal => exponential / 2 + rng.gen_range(0..(exponential / 2).max(1)),
            Jitter::Decorrelated => rng
                .gen_range(base..state.last_sleep_ms.saturating_mul(3).max(base + 1))
                .min(config.cap_ms),
        };
        log::debug!(
            "backoff: base={base}; sleep={delay}; attempts={}",
            state.attempts
        );
        delay
    }

    fn complete_delay(&mut self, config: RetryConfig, delay_ms: u64) {
        let state = self
            .functions
            .get_mut(config.name)
            .expect("backoff function state is created before sleeping");
        state.attempts += 1;
        state.last_sleep_ms = delay_ms;
    }
}

/// Returns whether TiKV supplied an empty/fake `EpochNotMatch` region error.
pub fn is_fake_region_error(error: Option<&errorpb::Error>) -> bool {
    error
        .and_then(|error| error.epoch_not_match.as_ref())
        .is_some_and(|mismatch| mismatch.current_regions.is_empty())
}

fn format_rfc3339_nanos(time: SystemTime) -> String {
    let nanoseconds = match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
        }
        Err(error) => {
            let duration = error.duration();
            -(i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos()))
        }
    };
    let seconds = nanoseconds.div_euclid(1_000_000_000);
    let subsecond = nanoseconds.rem_euclid(1_000_000_000) as u32;
    let days = seconds.div_euclid(86_400) as i64;
    let seconds_of_day = seconds.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_date_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = seconds_of_day % 3_600 / 60;
    let second = seconds_of_day % 60;
    let fraction = if subsecond == 0 {
        String::new()
    } else {
        format!(".{subsecond:09}").trim_end_matches('0').to_owned()
    };
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{fraction}Z")
}

fn civil_date_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let shifted = days_since_epoch + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

impl Clone for RetryBackoffer {
    fn clone(&self) -> Self {
        Self {
            id: NEXT_BACKOFFER_ID.fetch_add(1, Ordering::Relaxed),
            ancestors: self.ancestors.clone(),
            ancestor_types: self.ancestor_types.clone(),
            cancellation: self.cancellation.clone(),
            max_sleep_ms: self.max_sleep_ms,
            total_sleep_ms: self.total_sleep_ms,
            excluded_sleep_ms: self.excluded_sleep_ms,
            // client-go deliberately omits `noop` from Clone and Fork.
            noop: false,
            variables: self.variables.clone(),
            functions: BTreeMap::new(),
            errors: self.errors.clone(),
            errors_num: self.errors_num,
            sleep_by_type: self.sleep_by_type.clone(),
            times_by_type: self.times_by_type.clone(),
            configs: self.configs.clone(),
        }
    }
}

impl fmt::Display for RetryBackoffer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.total_sleep_ms == 0 {
            return Ok(());
        }
        write!(formatter, " backoff({}ms [", self.total_sleep_ms)?;
        for (index, config) in self.configs.iter().enumerate() {
            if index != 0 {
                formatter.write_str(" ")?;
            }
            formatter.write_str(config.name)?;
        }
        formatter.write_str("])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, AtomicUsize};

    struct CountingKillHandler {
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    impl crate::kv::KillSignalHandler for CountingKillHandler {
        fn handle_signal(&self) -> crate::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(crate::Error::StringError("killed by handler".to_owned()))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn source_retry_config_matrix_and_value_semantic_setters_are_complete() {
        let configs = [
            (
                BO_TIKV_RPC,
                "tikvRPC",
                100,
                2_000,
                Jitter::Equal,
                RetryTerminal::Static(StaticError::TiKvServerTimeout),
            ),
            (
                BO_TIFLASH_RPC,
                "tiflashRPC",
                100,
                2_000,
                Jitter::Equal,
                RetryTerminal::Static(StaticError::TiFlashServerTimeout),
            ),
            (
                BO_TXN_LOCK,
                "txnLock",
                100,
                3_000,
                Jitter::Equal,
                RetryTerminal::Static(StaticError::ResolveLockTimeout),
            ),
            (
                BO_PD_RPC,
                "pdRPC",
                500,
                3_000,
                Jitter::Equal,
                RetryTerminal::PdServerTimeout,
            ),
            (
                BO_REGION_MISS,
                "regionMiss",
                2,
                500,
                Jitter::No,
                RetryTerminal::Static(StaticError::RegionUnavailable),
            ),
            (
                BO_REGION_SCHEDULING,
                "regionScheduling",
                2,
                500,
                Jitter::No,
                RetryTerminal::Static(StaticError::RegionUnavailable),
            ),
            (
                BO_TIKV_SERVER_BUSY,
                "tikvServerBusy",
                2_000,
                10_000,
                Jitter::Equal,
                RetryTerminal::Static(StaticError::TiKvServerBusy),
            ),
            (
                BO_TIKV_DISK_FULL,
                "tikvDiskFull",
                500,
                5_000,
                Jitter::No,
                RetryTerminal::Static(StaticError::TiKvDiskFull),
            ),
            (
                BO_REGION_RECOVERY_IN_PROGRESS,
                "regionRecoveryInProgress",
                100,
                10_000,
                Jitter::Equal,
                RetryTerminal::Static(StaticError::RegionRecoveryInProgress),
            ),
            (
                BO_TIFLASH_SERVER_BUSY,
                "tiflashServerBusy",
                2_000,
                10_000,
                Jitter::Equal,
                RetryTerminal::Static(StaticError::TiFlashServerBusy),
            ),
            (
                BO_TXN_NOT_FOUND,
                "txnNotFound",
                2,
                500,
                Jitter::No,
                RetryTerminal::Static(StaticError::ResolveLockTimeout),
            ),
            (
                BO_STALE_CMD,
                "staleCommand",
                2,
                1_000,
                Jitter::No,
                RetryTerminal::Static(StaticError::TiKvStaleCommand),
            ),
            (
                BO_MAX_TS_NOT_SYNCED,
                "maxTsNotSynced",
                2,
                500,
                Jitter::No,
                RetryTerminal::Static(StaticError::TiKvMaxTimestampNotSynced),
            ),
            (
                BO_COMMIT_TS_LAG,
                "commitTSLag",
                2,
                500,
                Jitter::No,
                RetryTerminal::Static(StaticError::CommitTimestampLag),
            ),
            (
                BO_MAX_REGION_NOT_INITIALIZED,
                "regionNotInitialized",
                2,
                1_000,
                Jitter::No,
                RetryTerminal::Static(StaticError::RegionNotInitialized),
            ),
            (
                BO_IS_WITNESS,
                "isWitness",
                1_000,
                10_000,
                Jitter::Equal,
                RetryTerminal::Static(StaticError::IsWitness),
            ),
            (
                BO_TXN_LOCK_FAST,
                "txnLockFast",
                2,
                3_000,
                Jitter::Equal,
                RetryTerminal::Static(StaticError::ResolveLockTimeout),
            ),
        ];

        for (config, name, base, cap, jitter, terminal) in configs {
            assert_eq!(config.to_string(), name);
            assert_eq!(config.base(), base);
            assert_eq!(config.cap_ms, cap);
            assert_eq!(config.jitter, jitter);
            assert_eq!(config.terminal_error, terminal);
        }
        assert_eq!(BO_TIKV_SERVER_BUSY.excluded_budget_limit_ms, Some(600_000));
        assert_eq!(BO_TIFLASH_SERVER_BUSY.excluded_budget_limit_ms, None);
        assert_eq!(
            [
                BO_TIKV_RPC.metric_label,
                BO_TIFLASH_RPC.metric_label,
                BO_TXN_LOCK.metric_label,
                BO_PD_RPC.metric_label,
                BO_REGION_MISS.metric_label,
                BO_REGION_SCHEDULING.metric_label,
                BO_TIKV_SERVER_BUSY.metric_label,
                BO_TIKV_DISK_FULL.metric_label,
                BO_REGION_RECOVERY_IN_PROGRESS.metric_label,
                BO_TIFLASH_SERVER_BUSY.metric_label,
                BO_TXN_NOT_FOUND.metric_label,
                BO_STALE_CMD.metric_label,
                BO_MAX_TS_NOT_SYNCED.metric_label,
                BO_COMMIT_TS_LAG.metric_label,
                BO_MAX_REGION_NOT_INITIALIZED.metric_label,
                BO_IS_WITNESS.metric_label,
                BO_TXN_LOCK_FAST.metric_label,
            ],
            [
                Some("tikvRPC"),
                Some("tikvRPC"),
                Some("txnLock"),
                Some("pdRPC"),
                Some("regionMiss"),
                Some("regionScheduling"),
                Some("serverBusy"),
                Some("tikvDiskFull"),
                Some("regionRecoveryInProgress"),
                Some("serverBusy"),
                Some(""),
                Some("staleCommand"),
                Some(""),
                Some(""),
                Some(""),
                Some("isWitness"),
                Some("tikvLockFast"),
            ]
        );

        let custom = BO_REGION_MISS
            .with_backoff_fn(7, 11, Jitter::Decorrelated)
            .with_terminal(RetryTerminal::Static(StaticError::Unknown));
        assert_eq!(custom.base(), 7);
        assert_eq!(custom.cap_ms, 11);
        assert_eq!(custom.jitter, Jitter::Decorrelated);
        assert_eq!(
            custom.terminal_error,
            RetryTerminal::Static(StaticError::Unknown)
        );
        assert_eq!(
            RetryConfig::new("custom", 2, 4, Jitter::No, StaticError::Unknown).metric_label,
            None
        );
        assert_eq!(BO_REGION_MISS.base(), 2);
    }

    #[test]
    fn check_killed_prefers_the_signal_then_runs_the_handler() {
        let killed = Arc::new(AtomicU32::new(7));
        let calls = Arc::new(AtomicUsize::new(0));
        let mut variables = Variables::new(killed.clone());
        variables.kill_signal_handler = Some(Arc::new(CountingKillHandler {
            calls: calls.clone(),
            fail: true,
        }));
        let backoffer =
            RetryBackoffer::with_variables(Cancellation::default(), 1, Arc::new(variables));

        assert!(matches!(
            backoffer.check_killed(),
            Err(RetryError::Interrupted(QueryInterruptedWithSignalError {
                signal: 7
            }))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        killed.store(0, Ordering::SeqCst);
        let error = backoffer.check_killed().unwrap_err();
        assert!(matches!(error, RetryError::KillHandler(_)));
        assert_eq!(
            error.to_string(),
            "kill-signal handler failed: killed by handler"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn budget_history_exclusion_and_terminal_class_match_client_go() {
        let vars = Arc::new(Variables::new(Arc::new(AtomicU32::new(0))));
        // Default source variables double the caller's max sleep budget.
        let mut backoffer = RetryBackoffer::with_variables(Cancellation::default(), 1, vars);
        backoffer
            .backoff(BO_REGION_MISS, "region miss")
            .await
            .unwrap();
        let error = backoffer
            .backoff(BO_REGION_MISS, "region miss again")
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RetryError::Exhausted {
                terminal: Some(RetryTerminal::Static(StaticError::RegionUnavailable)),
                ..
            }
        ));
        assert_eq!(backoffer.errors_num(), 1);
        assert_eq!(backoffer.total_sleep_ms(), 2);

        let mut excluded = RetryBackoffer::new(Cancellation::default(), 1);
        excluded.backoff(BO_TIKV_SERVER_BUSY, "busy").await.unwrap();
        assert!(excluded.total_sleep_ms() >= excluded.excluded_sleep_ms());
    }

    #[test]
    fn source_backoff_weight_does_not_expand_large_budgets() {
        let variables = Arc::new(Variables::new(Arc::new(AtomicU32::new(0))));
        let large = i32::MAX as u64;
        let backoffer =
            RetryBackoffer::with_variables(Cancellation::default(), large, variables.clone());
        assert_eq!(backoffer.max_sleep_ms, large);

        let small = RetryBackoffer::with_variables(Cancellation::default(), 10, variables);
        assert_eq!(small.max_sleep_ms, 20);
    }

    #[tokio::test]
    async fn source_backoffer_string_reports_total_sleep_and_type_history() {
        let mut backoffer = RetryBackoffer::new(Cancellation::default(), 10);
        assert_eq!(backoffer.to_string(), "");
        backoffer
            .backoff(BO_REGION_MISS, "region miss")
            .await
            .unwrap();
        assert_eq!(backoffer.to_string(), " backoff(2ms [regionMiss])");
    }

    #[tokio::test]
    async fn cancellation_and_kill_signal_stop_before_or_after_sleep() {
        let cancelled = Cancellation::default();
        cancelled.cancel();
        let error = RetryBackoffer::new(cancelled, 10)
            .backoff(BO_REGION_MISS, "cancelled")
            .await
            .unwrap_err();
        assert!(matches!(error, RetryError::Cancelled { .. }));
        assert_eq!(error.to_string(), "cancelled");

        let cancellation = Cancellation::default();
        let mut backoffer = RetryBackoffer::new(cancellation.clone(), 10);
        let retry = tokio::spawn(async move {
            backoffer
                .backoff(BO_TIKV_RPC, "cancelled while sleeping")
                .await?;
            Ok::<RetryBackoffer, RetryError>(backoffer)
        });
        tokio::time::sleep(Duration::from_millis(1)).await;
        cancellation.cancel();
        let backoffer = retry.await.unwrap().unwrap();
        assert_eq!(backoffer.total_sleep_ms(), 0);
        assert_eq!(backoffer.times_by_type().get("tikvRPC"), Some(&1));

        let killed = Arc::new(AtomicU32::new(7));
        let mut backoffer = RetryBackoffer::with_variables(
            Cancellation::default(),
            10,
            Arc::new(Variables::new(killed)),
        );
        let error = backoffer
            .backoff(BO_REGION_MISS, "killed")
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RetryError::Interrupted(QueryInterruptedWithSignalError { signal: 7 })
        ));
    }

    #[tokio::test]
    async fn cancellation_during_sleep_does_not_advance_the_exponential_sequence() {
        let config = RetryConfig::new(
            "cancelProgression",
            20,
            100,
            Jitter::No,
            StaticError::RegionUnavailable,
        );
        let cancellation = Cancellation::default();
        let mut backoffer = RetryBackoffer::new(cancellation.clone(), 1_000);
        let retry = tokio::spawn(async move {
            backoffer.backoff(config, "cancel first sleep").await?;
            Ok::<RetryBackoffer, RetryError>(backoffer)
        });
        tokio::time::sleep(Duration::from_millis(1)).await;
        cancellation.cancel();
        let mut backoffer = retry.await.unwrap().unwrap();
        assert_eq!(backoffer.total_sleep_ms(), 0);
        assert_eq!(backoffer.times_by_type().get(config.name), Some(&1));

        let replacement = Cancellation::default();
        backoffer.set_cancellation(replacement.clone());
        assert!(!backoffer.cancellation().is_cancelled());
        backoffer.backoff(config, "retry from base").await.unwrap();
        assert_eq!(backoffer.total_sleep_ms(), 20);
        assert_eq!(backoffer.times_by_type().get(config.name), Some(&2));
    }

    #[tokio::test]
    async fn longest_non_excluded_sleep_selects_the_terminal_error() {
        let short = RetryConfig::new("short", 2, 2, Jitter::No, StaticError::RegionUnavailable);
        let long = RetryConfig::new("long", 4, 4, Jitter::No, StaticError::ResolveLockTimeout);
        let variables = Arc::new(Variables::new(Arc::new(AtomicU32::new(0))));
        let mut backoffer = RetryBackoffer::with_variables(Cancellation::default(), 4, variables);

        backoffer.backoff(short, "short").await.unwrap();
        backoffer.backoff(long, "long one").await.unwrap();
        backoffer.backoff(long, "long two").await.unwrap();
        let error = backoffer.backoff(short, "exhausted").await.unwrap_err();
        assert_eq!(
            error.to_string(),
            StaticError::ResolveLockTimeout.to_string()
        );
        assert_eq!(
            std::error::Error::source(&error).unwrap().to_string(),
            StaticError::ResolveLockTimeout.to_string()
        );
        assert!(matches!(
            error,
            RetryError::Exhausted {
                terminal: Some(RetryTerminal::Static(StaticError::ResolveLockTimeout)),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn cluster_id_mismatch_is_immediately_non_retryable() {
        let mut backoffer = RetryBackoffer::new(Cancellation::default(), 10);
        let reason = format!("PD response: {MISMATCH_CLUSTER_ID}");
        let error = backoffer
            .backoff(BO_PD_RPC, reason.clone())
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            RetryError::ClusterIdMismatch { reason: ref actual } if actual == &reason
        ));
        assert_eq!(error.to_string(), format!("critical error: {reason}"));
        assert_eq!(backoffer.total_sleep_ms(), 0);
        assert_eq!(backoffer.errors_num(), 0);
    }

    #[tokio::test]
    async fn fork_inherits_budget_cancels_independently_and_merges_only_to_its_parent() {
        let mut parent = RetryBackoffer::new(Cancellation::default(), 4);
        parent.backoff(BO_REGION_MISS, "parent").await.unwrap();
        let (mut fork, fork_cancellation) = parent.fork();
        fork.backoff(BO_REGION_MISS, "fork").await.unwrap();
        let clone = fork.clone();
        parent.update_using_forked(&fork);
        assert_eq!(parent.total_sleep_ms(), clone.total_sleep_ms());
        assert_eq!(parent.times_by_type(), clone.times_by_type());

        let mut unrelated = RetryBackoffer::new(Cancellation::default(), 4);
        unrelated.update_using_forked(&fork);
        assert_eq!(unrelated.total_sleep_ms(), 0);

        fork_cancellation.cancel();
        assert!(matches!(
            fork.backoff(BO_REGION_MISS, "child cancel").await,
            Err(RetryError::Cancelled { .. })
        ));
        assert!(!parent.cancellation.is_cancelled());
    }

    #[tokio::test]
    async fn region_errors_only_skip_backoff_for_a_real_epoch_mismatch() {
        let mut backoffer = RetryBackoffer::new(Cancellation::default(), 10);
        let real = errorpb::Error {
            epoch_not_match: Some(errorpb::EpochNotMatch {
                current_regions: vec![crate::proto::metapb::Region {
                    id: 1,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        };
        backoffer
            .may_backoff_region_error(Some(&real))
            .await
            .unwrap();
        assert_eq!(backoffer.total_sleep_ms(), 0);

        let fake = errorpb::Error {
            epoch_not_match: Some(errorpb::EpochNotMatch::default()),
            ..Default::default()
        };
        backoffer
            .may_backoff_region_error(Some(&fake))
            .await
            .unwrap();
        assert_eq!(backoffer.total_sleep_ms(), 2);

        for error in [
            errorpb::Error {
                not_leader: Some(errorpb::NotLeader::default()),
                ..Default::default()
            },
            errorpb::Error {
                server_is_busy: Some(errorpb::ServerIsBusy::default()),
                ..Default::default()
            },
            errorpb::Error {
                max_timestamp_not_synced: Some(errorpb::MaxTimestampNotSynced::default()),
                ..Default::default()
            },
        ] {
            let before = backoffer.total_backoff_times();
            backoffer
                .may_backoff_region_error(Some(&error))
                .await
                .unwrap();
            assert_eq!(backoffer.total_backoff_times(), before + 1);
        }
        backoffer.may_backoff_region_error(None).await.unwrap();

        assert!(!is_fake_region_error(None));
        assert!(is_fake_region_error(Some(&fake)));
        assert!(!is_fake_region_error(Some(&real)));

        let cancelled = Cancellation::default();
        cancelled.cancel();
        let mut cancelled_backoffer = RetryBackoffer::new(cancelled, 10);
        let expected_reason = format!("{fake:?}");
        let error = cancelled_backoffer
            .may_backoff_region_error(Some(&fake))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RetryError::Cancelled { reason } if reason == expected_reason
        ));
    }

    #[tokio::test]
    async fn noop_backoffer_returns_the_current_reason_without_sleeping() {
        let error = RetryBackoffer::noop(Cancellation::default())
            .backoff(BO_REGION_MISS, "original reason")
            .await
            .unwrap_err();
        assert!(matches!(error, RetryError::Noop { ref reason } if reason == "original reason"));
        assert_eq!(error.to_string(), "original reason");
    }

    #[tokio::test]
    async fn txn_lock_fast_caps_each_sleep_without_resetting_its_exponential_state() {
        let mut backoffer = RetryBackoffer::new(Cancellation::default(), 100);
        backoffer
            .backoff_with_max_sleep_txn_lock_fast(1, "fast lock")
            .await
            .unwrap();
        assert_eq!(backoffer.total_sleep_ms(), 1);
        backoffer
            .backoff_with_max_sleep_txn_lock_fast(5, "fast lock")
            .await
            .unwrap();
        assert_eq!(backoffer.total_sleep_ms(), 6);
        backoffer.reset_max_sleep(1);
        assert_eq!(backoffer.total_sleep_ms(), 0);
    }

    #[tokio::test]
    async fn one_sleep_cap_does_not_replace_decorrelated_jitter_history() {
        let config = RetryConfig::new(
            "decorrelatedCap",
            20,
            100,
            Jitter::Decorrelated,
            StaticError::RegionUnavailable,
        );
        let mut backoffer = RetryBackoffer::new(Cancellation::default(), 100);
        backoffer
            .backoff_with_config_and_max_sleep(config, Some(1), "capped")
            .await
            .unwrap();

        assert_eq!(backoffer.total_sleep_ms(), 1);
        let state = backoffer.functions.get(config.name).unwrap();
        assert_eq!(state.attempts, 1);
        assert!((20..=59).contains(&state.last_sleep_ms));
    }

    #[tokio::test]
    async fn pd_timeout_remains_a_distinct_terminal_error_class() {
        let mut backoffer = RetryBackoffer::new(Cancellation::default(), 1);
        backoffer
            .backoff(BO_PD_RPC, "PD unavailable")
            .await
            .unwrap();
        let error = backoffer
            .backoff(BO_PD_RPC, "PD unavailable")
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RetryError::Exhausted {
                terminal: Some(RetryTerminal::PdServerTimeout),
                ..
            }
        ));
        assert_eq!(RetryTerminal::PdServerTimeout.to_string(), "");
    }

    #[tokio::test]
    async fn error_count_is_lifetime_accounting_while_diagnostics_retain_three_records() {
        let mut backoffer = RetryBackoffer::new(Cancellation::default(), 0);
        for index in 0..5 {
            backoffer
                .backoff(BO_REGION_MISS, format!("region miss {index}"))
                .await
                .unwrap();
        }

        assert_eq!(backoffer.errors_num(), 5);
        assert_eq!(
            backoffer.latest_errors().len(),
            MAX_RECORD_BACKOFF_ERR_COUNT
        );
        assert_eq!(backoffer.latest_errors()[0].reason, "region miss 2");
        assert_eq!(backoffer.latest_errors()[2].reason, "region miss 4");

        let cloned = backoffer.clone();
        let (forked, _) = backoffer.fork();
        assert_eq!(cloned.errors_num(), 5);
        assert_eq!(forked.errors_num(), 5);
    }

    #[tokio::test]
    async fn excluded_retry_class_still_obeys_its_own_maximum_budget() {
        let excluded = RetryConfig::new(
            "testExcluded",
            2,
            2,
            Jitter::No,
            StaticError::TiKvServerBusy,
        )
        .excluding_budget(1);
        let mut backoffer = RetryBackoffer::new(Cancellation::default(), 1);

        backoffer.backoff(excluded, "busy").await.unwrap();
        let error = backoffer.backoff(excluded, "busy").await.unwrap_err();

        assert!(matches!(
            error,
            RetryError::Exhausted {
                terminal: None,
                reason,
                ..
            } if reason == "busy"
        ));
        assert_eq!(backoffer.total_sleep_ms(), 2);
        assert_eq!(backoffer.excluded_sleep_ms(), 2);
    }

    #[tokio::test]
    async fn clone_and_fork_of_noop_are_normal_backoffers_like_client_go() {
        let noop = RetryBackoffer::noop(Cancellation::default());
        let mut cloned = noop.clone();
        let (mut forked, _) = noop.fork();

        cloned.backoff(BO_REGION_MISS, "clone").await.unwrap();
        forked.backoff(BO_REGION_MISS, "fork").await.unwrap();
    }

    #[tokio::test]
    async fn fork_type_history_includes_the_copied_configs_and_parent_history() {
        let mut parent = RetryBackoffer::new(Cancellation::default(), 0);
        parent.backoff(BO_REGION_MISS, "parent").await.unwrap();
        let (mut fork, _) = parent.fork();
        fork.backoff(BO_TXN_NOT_FOUND, "fork").await.unwrap();

        assert_eq!(parent.types(), ["regionMiss"]);
        assert_eq!(fork.types(), ["regionMiss", "txnNotFound", "regionMiss"]);
        assert_eq!(fork.total_backoff_times(), 2);
        assert_eq!(
            fork.variables().backoff_weight,
            crate::kv::DEF_BACKOFF_WEIGHT
        );
    }

    #[test]
    fn backoff_record_formats_rfc3339_nanoseconds() {
        let record = BackoffRecord {
            reason: "mockErr".to_owned(),
            time: SystemTime::UNIX_EPOCH
                + Duration::from_secs(4 * 3_600 + 1_234)
                + Duration::from_nanos(123_400_000),
        };
        assert_eq!(record.to_string(), "mockErr at 1970-01-01T04:20:34.1234Z");
    }
}
