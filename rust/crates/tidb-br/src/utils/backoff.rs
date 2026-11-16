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

//! Go `br/pkg/utils/backoff.go`: BR's retry strategies.
//!
//! Go keys its retryable and fatal errors by identity after
//! `errors.Cause`; here a [`Sentinel`] names each such error, and a BR
//! normalized error matches by its ID.

use std::error::Error as StdError;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use tidb_log::{Field, Value};
use tidb_terror::TerrorError;

use super::error_handling::{handle_unknown_backup_error, ErrorContext, ErrorHandlingStrategy, CONTEXT_CANCELLED_MSG};
use crate::errors;

const IMPORT_SST_RETRY_TIMES: i32 = 16;
const IMPORT_SST_WAIT_INTERVAL: Duration = Duration::from_millis(40);
const IMPORT_SST_MAX_WAIT_INTERVAL: Duration = Duration::from_secs(10);
const DOWNLOAD_SST_RETRY_TIMES: i32 = 8;
const DOWNLOAD_SST_WAIT_INTERVAL: Duration = Duration::from_secs(1);
const DOWNLOAD_SST_MAX_WAIT_INTERVAL: Duration = Duration::from_secs(4);
const BACKUP_SST_RETRY_TIMES: i32 = 5;
const BACKUP_SST_WAIT_INTERVAL: Duration = Duration::from_secs(2);
const BACKUP_SST_MAX_WAIT_INTERVAL: Duration = Duration::from_secs(3);
const RESET_TS_RETRY_TIME: i32 = 32;
const RESET_TS_WAIT_INTERVAL: Duration = Duration::from_millis(50);
const RESET_TS_MAX_WAIT_INTERVAL: Duration = Duration::from_secs(2);
const RESET_TS_RETRY_TIME_EXT: i32 = 600;
const RESET_TS_WAIT_INTERVAL_EXT: Duration = Duration::from_millis(500);
const RESET_TS_MAX_WAIT_INTERVAL_EXT: Duration = Duration::from_secs(300);
/// Go `FlashbackRetryTime`.
pub const FLASHBACK_RETRY_TIME: i32 = 3;
/// Go `FlashbackWaitInterval`.
pub const FLASHBACK_WAIT_INTERVAL: Duration = Duration::from_secs(3);
/// Go `FlashbackMaxWaitInterval`.
pub const FLASHBACK_MAX_WAIT_INTERVAL: Duration = Duration::from_secs(15);
/// Go `ChecksumRetryTime`.
pub const CHECKSUM_RETRY_TIME: i32 = 8;
/// Go `ChecksumWaitInterval`.
pub const CHECKSUM_WAIT_INTERVAL: Duration = Duration::from_secs(1);
/// Go `ChecksumMaxWaitInterval`.
pub const CHECKSUM_MAX_WAIT_INTERVAL: Duration = Duration::from_secs(30);
const RECOVERY_MAX_ATTEMPTS: i32 = 16;
const RECOVERY_DELAY_TIME: Duration = Duration::from_secs(30);
const RAW_CLIENT_MAX_ATTEMPTS: i32 = 5;
const RAW_CLIENT_DELAY_TIME: Duration = Duration::from_millis(500);
const RAW_CLIENT_MAX_DELAY_TIME: Duration = Duration::from_secs(5);

/// Go `BackoffStrategy`.
pub trait BackoffStrategy: Send {
    /// Go `NextBackoff`.
    fn next_backoff(&mut self, err: &errors::Error) -> Duration;
    /// Go `RemainingAttempts`.
    fn remaining_attempts(&self) -> i32;
}

/// Go `ConstantBackoff`.
pub struct ConstantBackoff(pub Duration);

impl BackoffStrategy for ConstantBackoff {
    fn next_backoff(&mut self, _: &errors::Error) -> Duration {
        self.0
    }
    fn remaining_attempts(&self) -> i32 {
        i32::from(i16::MAX)
    }
}

/// Go `RetryState`.
#[derive(Clone, Copy, Debug)]
pub struct RetryState {
    max_retry: i32,
    retry_times: i32,
    max_backoff: Duration,
    next_backoff: Duration,
}

impl RetryState {
    /// Go `InitialRetryState`.
    #[must_use]
    pub fn new(max_retry_times: i32, initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self { max_retry: max_retry_times, retry_times: 0, max_backoff, next_backoff: initial_backoff }
    }
    /// Go `ShouldRetry`.
    #[must_use]
    pub fn should_retry(&self) -> bool {
        self.retry_times < self.max_retry
    }
    /// Go `ExponentialBackoff`.
    pub fn exponential_backoff(&mut self) -> Duration {
        self.retry_times += 1;
        if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/utils/set-remaining-attempts-to-one").is_some() {
            self.retry_times = self.max_retry;
        }
        let backoff = self.next_backoff;
        self.next_backoff = (self.next_backoff * 2).min(self.max_backoff);
        backoff
    }
    /// Go `GiveUp`.
    pub fn give_up(&mut self) {
        self.retry_times = self.max_retry;
    }
    /// Go `ReduceRetry`.
    pub fn reduce_retry(&mut self) {
        self.retry_times -= 1;
    }
}

impl BackoffStrategy for RetryState {
    fn next_backoff(&mut self, _: &errors::Error) -> Duration {
        self.exponential_backoff()
    }
    fn remaining_attempts(&self) -> i32 {
        self.max_retry - self.retry_times
    }
}

/// An error Go's strategies name by identity.
#[derive(Clone, Copy)]
pub enum Sentinel {
    /// A BR normalized error (`berrors.ErrX`), matched by ID.
    Terror(&'static LazyLock<TerrorError>),
    /// Go `context.Canceled`.
    ContextCanceled,
    /// Go `context.DeadlineExceeded`.
    ContextDeadlineExceeded,
    /// Go `io.EOF`.
    IoEof,
}

impl Sentinel {
    /// Whether `cause` (an `errors.Cause` result) is this error.
    #[must_use]
    pub fn matches(&self, cause: &(dyn StdError + 'static)) -> bool {
        match self {
            Self::Terror(t) => cause.downcast_ref::<TerrorError>().is_some_and(|e| e.id() == t.id()),
            Self::ContextCanceled => {
                matches!(cause.downcast_ref::<tidb_gocontext::CtxError>(), Some(tidb_gocontext::CtxError::Canceled))
            }
            Self::ContextDeadlineExceeded => matches!(
                cause.downcast_ref::<tidb_gocontext::CtxError>(),
                Some(tidb_gocontext::CtxError::DeadlineExceeded)
            ),
            Self::IoEof => cause.downcast_ref::<std::io::Error>().is_some_and(|e| e.kind() == std::io::ErrorKind::UnexpectedEof),
        }
    }
}

/// Go `func(error) bool` error classifiers.
pub type ErrFn = Arc<dyn Fn(&(dyn StdError + 'static)) -> bool + Send + Sync>;

/// Go `backoffStrategyImpl`.
pub struct BackoffStrategyImpl {
    remaining_attempts: i32,
    delay_time: Duration,
    max_delay_time: Duration,
    err_context: Arc<ErrorContext>,
    is_retry_err: ErrFn,
    is_non_retry_err: ErrFn,
    retry_grpc_canceled: bool,
}

/// Go `BackoffOption`.
pub type BackoffOption = Box<dyn FnOnce(&mut BackoffStrategyImpl)>;

/// Go `WithRemainingAttempts`.
pub fn with_remaining_attempts(attempts: i32) -> BackoffOption {
    Box::new(move |b| b.remaining_attempts = attempts)
}
/// Go `WithDelayTime`.
pub fn with_delay_time(delay: Duration) -> BackoffOption {
    Box::new(move |b| b.delay_time = delay)
}
/// Go `WithMaxDelayTime`.
pub fn with_max_delay_time(max: Duration) -> BackoffOption {
    Box::new(move |b| b.max_delay_time = max)
}
/// Go `WithErrorContext`.
pub fn with_error_context(ec: Arc<ErrorContext>) -> BackoffOption {
    Box::new(move |b| b.err_context = ec)
}
/// Go `WithRetryErrorFunc`.
pub fn with_retry_error_func(f: ErrFn) -> BackoffOption {
    Box::new(move |b| b.is_retry_err = f)
}
/// Go `WithNonRetryErrorFunc`.
pub fn with_non_retry_error_func(f: ErrFn) -> BackoffOption {
    Box::new(move |b| b.is_non_retry_err = f)
}
/// Go `WithRetryableGRPCCanceled`.
pub fn with_retryable_grpc_canceled() -> BackoffOption {
    Box::new(|b| b.retry_grpc_canceled = true)
}

/// Go `NewBackoffStrategy`.
#[must_use]
pub fn new_backoff_strategy(opts: Vec<BackoffOption>) -> BackoffStrategyImpl {
    let mut bs = BackoffStrategyImpl {
        remaining_attempts: 1,
        delay_time: Duration::from_secs(1),
        max_delay_time: Duration::from_secs(10),
        err_context: Arc::new(ErrorContext::new_zero_retry("default")),
        is_retry_err: always_true(),
        is_non_retry_err: always_false(),
        retry_grpc_canceled: false,
    };
    for opt in opts {
        opt(&mut bs);
    }
    bs
}

/// Go `NewBackoffRetryAllErrorStrategy`.
#[must_use]
pub fn new_backoff_retry_all_error_strategy(remaining: i32, delay: Duration, max_delay: Duration) -> BackoffStrategyImpl {
    new_backoff_strategy(vec![
        with_remaining_attempts(remaining),
        with_delay_time(delay),
        with_max_delay_time(max_delay),
        with_error_context(Arc::new(ErrorContext::new_zero_retry("retry all errors"))),
        with_retry_error_func(always_true()),
        with_non_retry_error_func(always_false()),
    ])
}

/// Go `NewBackoffRetryAllExceptStrategy`.
#[must_use]
pub fn new_backoff_retry_all_except_strategy(
    remaining: i32,
    delay: Duration,
    max_delay: Duration,
    is_non_retry: ErrFn,
) -> BackoffStrategyImpl {
    new_backoff_strategy(vec![
        with_remaining_attempts(remaining),
        with_delay_time(delay),
        with_max_delay_time(max_delay),
        with_error_context(Arc::new(ErrorContext::new_zero_retry("retry all except"))),
        with_retry_error_func(always_true()),
        with_non_retry_error_func(is_non_retry),
    ])
}

/// Go `NewTiKVStoreBackoffStrategy`.
#[must_use]
pub fn new_tikv_store_backoff_strategy(
    max_retry: i32,
    delay: Duration,
    max_delay: Duration,
    ec: Arc<ErrorContext>,
    opts: Vec<BackoffOption>,
) -> BackoffStrategyImpl {
    let retry_errs = vec![
        Sentinel::Terror(&errors::ERR_KV_EPOCH_NOT_MATCH),
        Sentinel::Terror(&errors::ERR_KV_DOWNLOAD_FAILED),
        Sentinel::Terror(&errors::ERR_KV_INGEST_FAILED),
        Sentinel::Terror(&errors::ERR_PD_LEADER_NOT_FOUND),
    ];
    let grpc_retry_codes = vec![
        tonic::Code::Cancelled,
        tonic::Code::Unavailable,
        tonic::Code::Aborted,
        tonic::Code::DeadlineExceeded,
        tonic::Code::ResourceExhausted,
        tonic::Code::Internal,
    ];
    let non_retry_errs = vec![
        Sentinel::ContextCanceled,
        Sentinel::Terror(&errors::ERR_KV_RANGE_IS_EMPTY),
        Sentinel::Terror(&errors::ERR_KV_REWRITE_RULE_NOT_FOUND),
    ];
    let mut all = vec![
        with_remaining_attempts(max_retry),
        with_delay_time(delay),
        with_max_delay_time(max_delay),
        with_error_context(ec),
        with_retry_error_func(build_is_retry_err_func(retry_errs, grpc_retry_codes)),
        with_non_retry_error_func(build_is_non_retry_err_func(non_retry_errs)),
    ];
    all.extend(opts);
    new_backoff_strategy(all)
}

/// Go `NewImportSSTBackoffStrategy`.
#[must_use]
pub fn new_import_sst_backoff_strategy() -> BackoffStrategyImpl {
    new_tikv_store_backoff_strategy(
        IMPORT_SST_RETRY_TIMES,
        IMPORT_SST_WAIT_INTERVAL,
        IMPORT_SST_MAX_WAIT_INTERVAL,
        Arc::new(ErrorContext::new("import sst", 3)),
        vec![],
    )
}

/// Go `NewDownloadSSTBackoffStrategy`.
#[must_use]
pub fn new_download_sst_backoff_strategy() -> BackoffStrategyImpl {
    new_tikv_store_backoff_strategy(
        DOWNLOAD_SST_RETRY_TIMES,
        DOWNLOAD_SST_WAIT_INTERVAL,
        DOWNLOAD_SST_MAX_WAIT_INTERVAL,
        Arc::new(ErrorContext::new("download sst", 3)),
        vec![],
    )
}

/// Go `NewPeerDownloadSSTBackoffStrategy`.
#[must_use]
pub fn new_peer_download_sst_backoff_strategy() -> BackoffStrategyImpl {
    new_tikv_store_backoff_strategy(
        DOWNLOAD_SST_RETRY_TIMES,
        DOWNLOAD_SST_WAIT_INTERVAL,
        DOWNLOAD_SST_MAX_WAIT_INTERVAL,
        Arc::new(ErrorContext::new("peer download sst", 3)),
        vec![with_retryable_grpc_canceled()],
    )
}

/// Go `NewBackupSSTBackoffStrategy`.
#[must_use]
pub fn new_backup_sst_backoff_strategy() -> BackoffStrategyImpl {
    new_tikv_store_backoff_strategy(
        BACKUP_SST_RETRY_TIMES,
        BACKUP_SST_WAIT_INTERVAL,
        BACKUP_SST_MAX_WAIT_INTERVAL,
        Arc::new(ErrorContext::new("backup sst", 3)),
        vec![],
    )
}

/// Go `NewPDBackoffStrategy`.
#[must_use]
pub fn new_pd_backoff_strategy(max_retry: i32, delay: Duration, max_delay: Duration) -> BackoffStrategyImpl {
    let retry_errs = vec![Sentinel::Terror(&errors::ERR_RESTORE_TOTAL_KV_MISMATCH), Sentinel::IoEof];
    let grpc_retry_codes = vec![
        tonic::Code::Cancelled,
        tonic::Code::DeadlineExceeded,
        tonic::Code::NotFound,
        tonic::Code::AlreadyExists,
        tonic::Code::PermissionDenied,
        tonic::Code::ResourceExhausted,
        tonic::Code::Aborted,
        tonic::Code::OutOfRange,
        tonic::Code::Unavailable,
        tonic::Code::DataLoss,
        tonic::Code::Unknown,
    ];
    // Go's `sql.ErrNoRows` is a `database/sql` sentinel BR's PD calls never
    // return here.
    let non_retry_errs = vec![Sentinel::ContextCanceled, Sentinel::ContextDeadlineExceeded];
    new_backoff_strategy(vec![
        with_remaining_attempts(max_retry),
        with_delay_time(delay),
        with_max_delay_time(max_delay),
        with_error_context(Arc::new(ErrorContext::new_zero_retry("connect PD"))),
        with_retry_error_func(build_is_retry_err_func(retry_errs, grpc_retry_codes)),
        with_non_retry_error_func(build_is_non_retry_err_func(non_retry_errs)),
    ])
}

/// Go `NewAggressivePDBackoffStrategy`.
#[must_use]
pub fn new_aggressive_pd_backoff_strategy() -> BackoffStrategyImpl {
    new_pd_backoff_strategy(RESET_TS_RETRY_TIME, RESET_TS_WAIT_INTERVAL, RESET_TS_MAX_WAIT_INTERVAL)
}

/// Go `NewConservativePDBackoffStrategy`.
#[must_use]
pub fn new_conservative_pd_backoff_strategy() -> BackoffStrategyImpl {
    new_pd_backoff_strategy(RESET_TS_RETRY_TIME_EXT, RESET_TS_WAIT_INTERVAL_EXT, RESET_TS_MAX_WAIT_INTERVAL_EXT)
}

/// Go `NewDiskCheckBackoffStrategy`.
#[must_use]
pub fn new_disk_check_backoff_strategy() -> BackoffStrategyImpl {
    let retry_errs =
        vec![Sentinel::Terror(&errors::ERR_PD_INVALID_RESPONSE), Sentinel::Terror(&errors::ERR_KV_DISK_FULL)];
    new_backoff_strategy(vec![
        with_remaining_attempts(RESET_TS_RETRY_TIME),
        with_delay_time(RESET_TS_WAIT_INTERVAL),
        with_error_context(Arc::new(ErrorContext::new_zero_retry("disk check"))),
        with_retry_error_func(build_is_retry_err_func(retry_errs, vec![])),
        with_non_retry_error_func(always_false()),
    ])
}

/// Go `NewRecoveryBackoffStrategy`.
#[must_use]
pub fn new_recovery_backoff_strategy(is_retry: ErrFn) -> BackoffStrategyImpl {
    new_backoff_strategy(vec![
        with_remaining_attempts(RECOVERY_MAX_ATTEMPTS),
        with_delay_time(RECOVERY_DELAY_TIME),
        with_error_context(Arc::new(ErrorContext::new_zero_retry("recovery"))),
        with_retry_error_func(is_retry),
        with_non_retry_error_func(always_false()),
    ])
}

/// Go `NewFlashBackBackoffStrategy`.
#[must_use]
pub fn new_flashback_backoff_strategy() -> BackoffStrategyImpl {
    new_backoff_strategy(vec![
        with_remaining_attempts(FLASHBACK_RETRY_TIME),
        with_delay_time(FLASHBACK_WAIT_INTERVAL),
        with_error_context(Arc::new(ErrorContext::new_zero_retry("flashback"))),
        with_retry_error_func(always_true()),
        with_non_retry_error_func(always_false()),
    ])
}

/// Go `NewChecksumBackoffStrategy`.
#[must_use]
pub fn new_checksum_backoff_strategy() -> BackoffStrategyImpl {
    new_backoff_strategy(vec![
        with_remaining_attempts(CHECKSUM_RETRY_TIME),
        with_delay_time(CHECKSUM_WAIT_INTERVAL),
        with_error_context(Arc::new(ErrorContext::new_zero_retry("checksum"))),
        with_retry_error_func(always_true()),
        with_non_retry_error_func(always_false()),
    ])
}

/// Go `NewRawClientBackoffStrategy`.
#[must_use]
pub fn new_raw_client_backoff_strategy() -> BackoffStrategyImpl {
    new_backoff_strategy(vec![
        with_remaining_attempts(RAW_CLIENT_MAX_ATTEMPTS),
        with_delay_time(RAW_CLIENT_DELAY_TIME),
        with_max_delay_time(RAW_CLIENT_MAX_DELAY_TIME),
        with_error_context(Arc::new(ErrorContext::new_zero_retry("raw client"))),
        with_retry_error_func(always_true()),
        with_non_retry_error_func(always_false()),
    ])
}

impl BackoffStrategy for BackoffStrategyImpl {
    /// Go `NextBackoff`.
    fn next_backoff(&mut self, err: &errors::Error) -> Duration {
        let errs = super::retry::multierr_errors(err);
        let last_err = errs.last().cloned().unwrap_or_else(|| err.clone());
        let res = handle_unknown_backup_error(&last_err.to_string(), 0, &self.err_context);
        if res.strategy == ErrorHandlingStrategy::Retry {
            self.do_backoff();
        } else if res.reason == CONTEXT_CANCELLED_MSG {
            let e = errors::cause(last_err.as_ref());
            let std_canceled = Sentinel::ContextCanceled.matches(e);
            if self.retry_grpc_canceled && !std_canceled && errors::grpc_code(e) == tonic::Code::Cancelled {
                self.do_backoff();
            } else {
                self.stop_backoff();
            }
        } else {
            let e = errors::cause(last_err.as_ref());
            if (self.is_non_retry_err)(e) {
                self.stop_backoff();
            } else if (self.is_retry_err)(e) {
                self.do_backoff();
            } else {
                tidb_log::warn("stop retrying on error", &[Field::new("error", Value::Str(err.to_string()))]);
                self.stop_backoff();
            }
        }
        if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/utils/set-remaining-attempts-to-one").is_some()
            && self.remaining_attempts > 1
        {
            self.remaining_attempts = 1;
        }
        self.delay_time.min(self.max_delay_time)
    }

    fn remaining_attempts(&self) -> i32 {
        self.remaining_attempts
    }
}

impl BackoffStrategyImpl {
    fn do_backoff(&mut self) {
        self.delay_time *= 2;
        self.remaining_attempts -= 1;
    }

    fn stop_backoff(&mut self) {
        self.delay_time = Duration::ZERO;
        self.remaining_attempts = 0;
    }
}

/// Go `buildIsRetryErrFunc`.
fn build_is_retry_err_func(retry_errs: Vec<Sentinel>, grpc_codes: Vec<tonic::Code>) -> ErrFn {
    Arc::new(move |e| {
        retry_errs.iter().any(|s| s.matches(e)) || grpc_status_code(e).is_some_and(|c| grpc_codes.contains(&c))
    })
}

/// Go `status.Code(err)` on an `errors.Cause` result: `OK` for nil is not
/// reachable, `Unknown` for a non-status error.
fn grpc_status_code(e: &(dyn StdError + 'static)) -> Option<tonic::Code> {
    Some(errors::grpc_code(e))
}

/// Go `buildIsNonRetryErrFunc`.
fn build_is_non_retry_err_func(non_retry_errs: Vec<Sentinel>) -> ErrFn {
    Arc::new(move |e| non_retry_errs.iter().any(|s| s.matches(e)))
}

/// Go `alwaysTrueFunc`.
#[must_use]
pub fn always_true() -> ErrFn {
    Arc::new(|_| true)
}

/// Go `alwaysFalseFunc`.
#[must_use]
pub fn always_false() -> ErrFn {
    Arc::new(|_| false)
}
