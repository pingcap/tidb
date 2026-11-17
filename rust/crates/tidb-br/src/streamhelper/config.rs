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


//! Go `streamhelper/config`: the advancer's intervals. TiDB uses
//! `TiDBConfig`, whose lag limit is `tidb_advancer_check_point_lag_limit`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Go `DefaultTryAdvanceThreshold`.
pub const DEFAULT_TRY_ADVANCE_THRESHOLD: Duration = Duration::from_secs(4 * 60);
/// Go `DefaultCheckPointLagLimit`.
pub const DEFAULT_CHECK_POINT_LAG_LIMIT: Duration = Duration::from_secs(48 * 3600);
/// Go `DefaultBackOffTime`.
pub const DEFAULT_BACKOFF_TIME: Duration = Duration::from_secs(5);
/// Go `DefaultTickInterval`.
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_secs(12);
/// Go `DefaultMaxConcurrencyAdvance`.
pub const DEFAULT_MAX_CONCURRENCY_ADVANCE: usize = 8;

/// Go `vardef.AdvancerCheckPointLagLimit`, in nanoseconds: set by
/// `tidb_advancer_check_point_lag_limit`.
pub static ADVANCER_CHECK_POINT_LAG_LIMIT: AtomicU64 = AtomicU64::new(48 * 3600 * 1_000_000_000);

/// Go `config.Config`.
pub trait Config: Send + Sync {
    /// Go `GetBackoffTime`.
    fn get_backoff_time(&self) -> Duration;
    /// Go `TickTimeout`.
    fn tick_timeout(&self) -> Duration;
    /// Go `GetDefaultStartPollThreshold`.
    fn get_default_start_poll_threshold(&self) -> Duration;
    /// Go `GetSubscriberErrorStartPollThreshold`.
    fn get_subscriber_error_start_poll_threshold(&self) -> Duration;
    /// Go `GetResolveLockInterval`.
    fn get_resolve_lock_interval(&self) -> Duration;
    /// Go `GetCheckPointLagLimit`.
    fn get_check_point_lag_limit(&self) -> Duration;
}

/// Go `CommandConfig`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandConfig {
    /// Go `BackoffTime`.
    pub backoff_time: Duration,
    /// Go `TickDuration`.
    pub tick_duration: Duration,
    /// Go `TryAdvanceThreshold`.
    pub try_advance_threshold: Duration,
    /// Go `CheckPointLagLimit`.
    pub check_point_lag_limit: Duration,
}

impl Default for CommandConfig {
    /// Go `defaultCommandConfig`.
    fn default() -> Self {
        Self {
            backoff_time: DEFAULT_BACKOFF_TIME,
            tick_duration: DEFAULT_TICK_INTERVAL,
            try_advance_threshold: DEFAULT_TRY_ADVANCE_THRESHOLD,
            check_point_lag_limit: DEFAULT_CHECK_POINT_LAG_LIMIT,
        }
    }
}

impl Config for CommandConfig {
    fn get_backoff_time(&self) -> Duration {
        self.backoff_time
    }
    /// A tick blocking longer than the interval is broken and retried.
    fn tick_timeout(&self) -> Duration {
        self.tick_duration
    }
    fn get_default_start_poll_threshold(&self) -> Duration {
        self.try_advance_threshold
    }
    /// 0.45x of the normal threshold.
    fn get_subscriber_error_start_poll_threshold(&self) -> Duration {
        self.try_advance_threshold * 9 / 20
    }
    fn get_resolve_lock_interval(&self) -> Duration {
        self.tick_duration * 2
    }
    fn get_check_point_lag_limit(&self) -> Duration {
        self.check_point_lag_limit
    }
}

/// Go `TiDBConfig`: the command defaults with the lag limit of the
/// system variable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TiDBConfig {
    /// Go's embedded `*CommandConfig`.
    pub command: CommandConfig,
}

impl Config for TiDBConfig {
    fn get_backoff_time(&self) -> Duration {
        self.command.get_backoff_time()
    }
    fn tick_timeout(&self) -> Duration {
        self.command.tick_timeout()
    }
    fn get_default_start_poll_threshold(&self) -> Duration {
        self.command.get_default_start_poll_threshold()
    }
    fn get_subscriber_error_start_poll_threshold(&self) -> Duration {
        self.command.get_subscriber_error_start_poll_threshold()
    }
    fn get_resolve_lock_interval(&self) -> Duration {
        self.command.get_resolve_lock_interval()
    }
    fn get_check_point_lag_limit(&self) -> Duration {
        Duration::from_nanos(ADVANCER_CHECK_POINT_LAG_LIMIT.load(Ordering::Relaxed))
    }
}
