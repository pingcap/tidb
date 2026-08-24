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

use rand::{thread_rng, Rng};
use thiserror::Error;

use crate::async_util::Cancellation;
use crate::error::{QueryInterruptedWithSignalError, StaticError};
use crate::kv::{Variables, DEFAULT_VARIABLES};
use crate::proto::errorpb;

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

/// Immutable source retry-class definition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryConfig {
    pub name: &'static str,
    pub base_ms: u64,
    pub cap_ms: u64,
    pub jitter: Jitter,
    pub terminal_error: RetryTerminal,
    pub excluded_budget_limit_ms: Option<u64>,
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
        }
    }

    pub const fn excluding_budget(mut self, limit_ms: u64) -> Self {
        self.excluded_budget_limit_ms = Some(limit_ms);
        self
    }

    fn metric_label(self) -> &'static str {
        match self.name {
            "tikvRPC" | "tiflashRPC" => "tikvRPC",
            "txnLock" => "txnLock",
            "txnLockFast" => "tikvLockFast",
            "pdRPC" => "pdRPC",
            "regionMiss" => "regionMiss",
            "regionScheduling" => "regionScheduling",
            "tikvServerBusy" | "tiflashServerBusy" => "serverBusy",
            "tikvDiskFull" => "tikvDiskFull",
            "regionRecoveryInProgress" => "regionRecoveryInProgress",
            "staleCommand" => "staleCommand",
            "isWitness" => "isWitness",
            _ => "",
        }
    }
}

pub const BO_TIKV_RPC: RetryConfig = RetryConfig::new(
    "tikvRPC",
    100,
    2_000,
    Jitter::Equal,
    StaticError::TiKvServerTimeout,
);
pub const BO_TIFLASH_RPC: RetryConfig = RetryConfig::new(
    "tiflashRPC",
    100,
    2_000,
    Jitter::Equal,
    StaticError::TiFlashServerTimeout,
);
pub const BO_TXN_LOCK: RetryConfig = RetryConfig::new(
    "txnLock",
    100,
    3_000,
    Jitter::Equal,
    StaticError::ResolveLockTimeout,
);
pub const BO_PD_RPC: RetryConfig = RetryConfig::new_pd_timeout("pdRPC", 500, 3_000, Jitter::Equal);
pub const BO_REGION_MISS: RetryConfig = RetryConfig::new(
    "regionMiss",
    2,
    500,
    Jitter::No,
    StaticError::RegionUnavailable,
);
pub const BO_REGION_SCHEDULING: RetryConfig = RetryConfig::new(
    "regionScheduling",
    2,
    500,
    Jitter::No,
    StaticError::RegionUnavailable,
);
pub const BO_TIKV_SERVER_BUSY: RetryConfig = RetryConfig::new(
    "tikvServerBusy",
    2_000,
    10_000,
    Jitter::Equal,
    StaticError::TiKvServerBusy,
)
.excluding_budget(600_000);
pub const BO_TIKV_DISK_FULL: RetryConfig = RetryConfig::new(
    "tikvDiskFull",
    500,
    5_000,
    Jitter::No,
    StaticError::TiKvDiskFull,
);
pub const BO_REGION_RECOVERY_IN_PROGRESS: RetryConfig = RetryConfig::new(
    "regionRecoveryInProgress",
    100,
    10_000,
    Jitter::Equal,
    StaticError::RegionRecoveryInProgress,
);
pub const BO_TIFLASH_SERVER_BUSY: RetryConfig = RetryConfig::new(
    "tiflashServerBusy",
    2_000,
    10_000,
    Jitter::Equal,
    StaticError::TiFlashServerBusy,
);
pub const BO_TXN_NOT_FOUND: RetryConfig = RetryConfig::new(
    "txnNotFound",
    2,
    500,
    Jitter::No,
    StaticError::ResolveLockTimeout,
);
pub const BO_STALE_CMD: RetryConfig = RetryConfig::new(
    "staleCommand",
    2,
    1_000,
    Jitter::No,
    StaticError::TiKvStaleCommand,
);
pub const BO_MAX_TS_NOT_SYNCED: RetryConfig = RetryConfig::new(
    "maxTsNotSynced",
    2,
    500,
    Jitter::No,
    StaticError::TiKvMaxTimestampNotSynced,
);
pub const BO_COMMIT_TS_LAG: RetryConfig = RetryConfig::new(
    "commitTSLag",
    2,
    500,
    Jitter::No,
    StaticError::CommitTimestampLag,
);
pub const BO_MAX_REGION_NOT_INITIALIZED: RetryConfig = RetryConfig::new(
    "regionNotInitialized",
    2,
    1_000,
    Jitter::No,
    StaticError::RegionNotInitialized,
);
pub const BO_IS_WITNESS: RetryConfig = RetryConfig::new(
    "isWitness",
    1_000,
    10_000,
    Jitter::Equal,
    StaticError::IsWitness,
);
pub const BO_TXN_LOCK_FAST: RetryConfig = RetryConfig::new(
    "txnLockFast",
    2,
    3_000,
    Jitter::Equal,
    StaticError::ResolveLockTimeout,
);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackoffRecord {
    pub reason: String,
    pub time: SystemTime,
}

#[derive(Debug, Error)]
pub enum RetryError {
    /// client-go returns the triggering error unchanged when its context is
    /// already cancelled before a backoff begins.
    #[error("{reason}")]
    Cancelled { reason: String },
    /// Source noop backoffers return their triggering error directly.
    #[error("{reason}")]
    Noop { reason: String },
    #[error(transparent)]
    Interrupted(#[from] QueryInterruptedWithSignalError),
    #[error("backoff budget of {max_sleep_ms}ms exhausted; terminal class {terminal:?}; reason: {reason}")]
    Exhausted {
        max_sleep_ms: u64,
        /// The non-excluded class that consumed the most sleep. `None`
        /// preserves client-go's fallback to the triggering error when only
        /// excluded classes have consumed the budget.
        terminal: Option<RetryTerminal>,
        reason: String,
        recent_errors: Vec<BackoffRecord>,
    },
    #[error("kill-signal handler failed: {0}")]
    KillHandler(#[source] crate::Error),
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

    pub(crate) fn max_sleep_ms(&self) -> u64 {
        self.max_sleep_ms
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
        self.backoff_with_max_sleep(config, None, reason).await
    }

    /// Source `BackoffWithMaxSleepTxnLockFast`: preserves the class's
    /// exponential progression while capping only this individual sleep.
    pub async fn backoff_with_max_sleep_txn_lock_fast(
        &mut self,
        max_sleep_ms: u64,
        reason: impl Into<String>,
    ) -> Result<(), RetryError> {
        self.backoff_with_max_sleep(BO_TXN_LOCK_FAST, Some(max_sleep_ms), reason)
            .await
    }

    async fn backoff_with_max_sleep(
        &mut self,
        config: RetryConfig,
        max_single_sleep_ms: Option<u64>,
        reason: impl Into<String>,
    ) -> Result<(), RetryError> {
        let reason = reason.into();
        if self.cancellation.is_cancelled() {
            return Err(RetryError::Cancelled { reason });
        }
        if self.noop {
            return Err(RetryError::Noop { reason });
        }
        if self.budget_exhausted(config) {
            return Err(self.exhausted(reason));
        }
        self.push_error(reason.clone());
        self.configs.push(config);
        let delay_ms = self.next_delay_ms(config);
        let delay_ms = max_single_sleep_ms.map_or(delay_ms, |limit| delay_ms.min(limit));
        // Go checks its context before constructing the backoff function. If
        // the context is cancelled while `time.After` is already pending, the
        // function returns a zero sleep and this call still records that
        // retry; the following Backoff call observes cancellation. Preserve
        // that transition rather than returning early from this wait.
        let interrupted = tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => false,
            _ = self.cancellation.cancelled() => true,
        };
        let real_sleep_ms = (!interrupted).then_some(delay_ms).unwrap_or(0);
        self.total_sleep_ms = self.total_sleep_ms.saturating_add(real_sleep_ms);
        if config.excluded_budget_limit_ms.is_some() {
            self.excluded_sleep_ms = self.excluded_sleep_ms.saturating_add(real_sleep_ms);
        }
        *self.sleep_by_type.entry(config.name).or_default() += real_sleep_ms;
        *self.times_by_type.entry(config.name).or_default() += 1;
        crate::stats::observe_retry_backoff(
            config.metric_label(),
            Duration::from_millis(real_sleep_ms),
        );
        self.check_killed()
    }

    pub fn check_killed(&self) -> Result<(), RetryError> {
        let signal = self.variables.killed.load(Ordering::Acquire);
        if signal != 0 {
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
        let real_epoch_mismatch = error
            .epoch_not_match
            .as_ref()
            .is_some_and(|mismatch| !mismatch.current_regions.is_empty());
        if real_epoch_mismatch {
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
        if self.errors.len() > 3 {
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
        state.attempts += 1;
        state.last_sleep_ms = delay;
        delay
    }
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
    use std::sync::atomic::AtomicU32;

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
        assert_eq!(backoffer.latest_errors().len(), 3);
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
}
