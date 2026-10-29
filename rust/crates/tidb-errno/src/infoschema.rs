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

//! Instance error and warning summaries from `pkg/errno/infoschema.go`.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::SystemTime;

/// Error and warning counters for one MySQL error code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorSummary {
    /// Number of errors observed for this key.
    pub error_count: isize,
    /// Number of warnings observed for this key.
    pub warning_count: isize,
    /// Time at which the key was first inserted.
    pub first_seen: SystemTime,
    /// Time of the most recent increment, or Go's zero `time.Time` before one.
    pub last_seen: Option<SystemTime>,
}

impl ErrorSummary {
    fn first_seen(at: SystemTime) -> Self {
        Self {
            error_count: 0,
            warning_count: 0,
            first_seen: at,
            last_seen: None,
        }
    }
}

/// Error summaries indexed by MySQL error code.
pub type ErrorStats = HashMap<u16, ErrorSummary>;

/// Per-user or per-host error summaries.
pub type ScopedErrorStats = HashMap<String, ErrorStats>;

#[derive(Default)]
struct InstanceStatistics {
    global: ErrorStats,
    users: ScopedErrorStats,
    hosts: ScopedErrorStats,
}

fn instance_statistics() -> &'static Mutex<InstanceStatistics> {
    static STATS: OnceLock<Mutex<InstanceStatistics>> = OnceLock::new();
    STATS.get_or_init(|| Mutex::new(InstanceStatistics::default()))
}

fn lock_statistics() -> MutexGuard<'static, InstanceStatistics> {
    instance_statistics()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Resets errors and warnings across global, user, and host summaries.
pub fn flush_stats() {
    *lock_statistics() = InstanceStatistics::default();
}

/// Returns a deep snapshot of summaries across all users and hosts.
pub fn global_stats() -> ErrorStats {
    lock_statistics().global.clone()
}

/// Returns a deep snapshot of per-user summaries.
pub fn user_stats() -> ScopedErrorStats {
    lock_statistics().users.clone()
}

/// Returns a deep snapshot of per-host summaries.
pub fn host_stats() -> ScopedErrorStats {
    lock_statistics().hosts.clone()
}

fn init_counters(error_code: u16, user: &str, host: &str) {
    let seen = SystemTime::now();
    let mut statistics = lock_statistics();
    statistics
        .global
        .entry(error_code)
        .or_insert_with(|| ErrorSummary::first_seen(seen));
    statistics
        .users
        .entry(user.to_owned())
        .or_default()
        .entry(error_code)
        .or_insert_with(|| ErrorSummary::first_seen(seen));
    statistics
        .hosts
        .entry(host.to_owned())
        .or_default()
        .entry(error_code)
        .or_insert_with(|| ErrorSummary::first_seen(seen));
}

fn increment_summary(summary: &mut ErrorSummary, seen: SystemTime, warning: bool) {
    if warning {
        summary.warning_count = summary.warning_count.wrapping_add(1);
    } else {
        summary.error_count = summary.error_count.wrapping_add(1);
    }
    summary.last_seen = Some(seen);
}

/// Increments global, user, and host error statistics for `error_code`.
pub fn increment_error(error_code: u16, user: &str, host: &str) {
    let seen = SystemTime::now();
    init_counters(error_code, user, host);

    let mut statistics = lock_statistics();
    increment_summary(
        statistics
            .global
            .get_mut(&error_code)
            .expect("init_counters creates the global summary"),
        seen,
        false,
    );
    increment_summary(
        statistics
            .users
            .get_mut(user)
            .and_then(|summaries| summaries.get_mut(&error_code))
            .expect("init_counters creates the user summary"),
        seen,
        false,
    );
    increment_summary(
        statistics
            .hosts
            .get_mut(host)
            .and_then(|summaries| summaries.get_mut(&error_code))
            .expect("init_counters creates the host summary"),
        seen,
        false,
    );
}

/// Increments global, user, and host warning statistics for `error_code`.
pub fn increment_warning(error_code: u16, user: &str, host: &str) {
    let seen = SystemTime::now();
    init_counters(error_code, user, host);

    let mut statistics = lock_statistics();
    increment_summary(
        statistics
            .global
            .get_mut(&error_code)
            .expect("init_counters creates the global summary"),
        seen,
        true,
    );
    increment_summary(
        statistics
            .users
            .get_mut(user)
            .and_then(|summaries| summaries.get_mut(&error_code))
            .expect("init_counters creates the user summary"),
        seen,
        true,
    );
    increment_summary(
        statistics
            .hosts
            .get_mut(host)
            .and_then(|summaries| summaries.get_mut(&error_code))
            .expect("init_counters creates the host summary"),
        seen,
        true,
    );
}
