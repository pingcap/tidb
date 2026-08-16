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

//! The mocks and helpers Go declares directly inside `aggregator_test.go` and
//! `stmtstats_test.go`, shared here because Rust splits the package's tests
//! across module files.

use std::sync::{Mutex, MutexGuard};

use crate::topsql_state::GLOBAL_TEST_LOCK;

use super::aggregator::{Collector, RuCollector};
use super::rustats::{RuIncrementMap, RuKey, RuVersion, RuVersionProvider};
use super::stmtstats::{
    BinaryDigest, ExecBeginInfo, ExecFinishInfo, SqlPlanDigest, StatementStatsMap,
};
use crate::topsql_state::{disable_top_ru, disable_top_sql, top_ru_enabled};

/// Go runs a package's tests sequentially; Rust runs them in parallel. Both the
/// Top-SQL/Top-RU state and the global aggregator are process-global, so every
/// test in this package takes `topsql_state`'s lock first — the same one that
/// package's own tests take, since both mutate the same flags.
pub(super) fn global_test_guard() -> MutexGuard<'static, ()> {
    GLOBAL_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Go's repeated `state.DisableTopSQL()` / `for state.TopRUEnabled() {
/// state.DisableTopRU() }` preamble.
pub(super) fn reset_topsql_state() {
    disable_top_sql();
    while top_ru_enabled() {
        disable_top_ru();
    }
}

/// Go's `require.InDelta(t, expected, actual, 1e-9)`.
#[track_caller]
pub(super) fn assert_in_delta(expected: f64, actual: f64) {
    assert!(
        (expected - actual).abs() <= 1e-9,
        "expected {expected}, got {actual}"
    );
}

/// Go's `SQLPlanDigest{SQLDigest: sql, PlanDigest: plan}`.
pub(super) fn sql_plan_digest(sql: &str, plan: &str) -> SqlPlanDigest {
    SqlPlanDigest {
        sql_digest: BinaryDigest::from(sql),
        plan_digest: BinaryDigest::from(plan),
    }
}

/// Go's `RUKey{User: user, SQLDigest: sql, PlanDigest: plan}`.
pub(super) fn ru_key(user: &str, sql: &str, plan: &str) -> RuKey {
    RuKey {
        user: user.to_owned(),
        sql_digest: BinaryDigest::from(sql),
        plan_digest: BinaryDigest::from(plan),
    }
}

/// Go's `&ExecBeginInfo{InNetworkBytes: n}`.
pub(super) fn exec_begin(in_network_bytes: u64) -> ExecBeginInfo {
    ExecBeginInfo {
        in_network_bytes,
        ..ExecBeginInfo::default()
    }
}

/// Go's `&ExecFinishInfo{ExecDuration: d}`.
pub(super) fn exec_finish(exec_duration_ns: i64) -> ExecFinishInfo {
    ExecFinishInfo {
        exec_duration_ns,
        ..ExecFinishInfo::default()
    }
}

/// Go's `mockCollector` / `newMockCollector`.
pub(super) struct MockCollector {
    f: Box<dyn Fn(&StatementStatsMap) + Send + Sync>,
}

impl MockCollector {
    pub(super) fn new(f: impl Fn(&StatementStatsMap) + Send + Sync + 'static) -> Self {
        Self { f: Box::new(f) }
    }
}

impl Collector for MockCollector {
    fn collect_stmt_stats_map(&self, data: &StatementStatsMap) {
        (self.f)(data);
    }
}

/// Go's `mockRUCollector.f` field type.
type RuIncrementFn = Box<dyn Fn(&RuIncrementMap) + Send + Sync>;
/// Go's `mockRUCollector.fWithVersion` field type.
type RuIncrementVersionFn = Box<dyn Fn(&RuIncrementMap, RuVersion) + Send + Sync>;
/// Go's `mockRUCollector.onChange` field type.
type RuVersionChangeFn = Box<dyn Fn(RuVersion) + Send + Sync>;

/// Go's `mockRUCollector`.
pub(super) struct MockRuCollector {
    f: Option<RuIncrementFn>,
    f_with_version: Option<RuIncrementVersionFn>,
    on_change: Option<RuVersionChangeFn>,
}

impl MockRuCollector {
    /// Go's `&mockRUCollector{f: ...}`.
    pub(super) fn new(f: impl Fn(&RuIncrementMap) + Send + Sync + 'static) -> Self {
        Self {
            f: Some(Box::new(f)),
            f_with_version: None,
            on_change: None,
        }
    }

    /// Go's `&mockRUCollector{fWithVersion: ...}`.
    pub(super) fn with_version(
        f: impl Fn(&RuIncrementMap, RuVersion) + Send + Sync + 'static,
    ) -> Self {
        Self {
            f: None,
            f_with_version: Some(Box::new(f)),
            on_change: None,
        }
    }

    /// Go's `onChange: ...` field.
    pub(super) fn on_change(mut self, f: impl Fn(RuVersion) + Send + Sync + 'static) -> Self {
        self.on_change = Some(Box::new(f));
        self
    }
}

impl RuCollector for MockRuCollector {
    fn collect_ru_increments(&self, data: &RuIncrementMap, version: RuVersion) {
        if let Some(f) = &self.f_with_version {
            f(data, version);
            return;
        }
        if let Some(f) = &self.f {
            f(data);
        }
    }

    fn on_ru_version_change(&self, version: RuVersion) {
        if let Some(f) = &self.on_change {
            f(version);
        }
    }
}

/// Go's `mockRUVersionProvider`.
pub(super) struct MockRuVersionProvider {
    version: Mutex<RuVersion>,
}

impl MockRuVersionProvider {
    pub(super) fn new(version: RuVersion) -> Self {
        Self {
            version: Mutex::new(version),
        }
    }

    /// Go's `provider.version = ...` field write.
    pub(super) fn set(&self, version: RuVersion) {
        *self.version.lock().unwrap_or_else(|e| e.into_inner()) = version;
    }
}

impl RuVersionProvider for MockRuVersionProvider {
    fn get_ru_version(&self) -> RuVersion {
        *self.version.lock().unwrap_or_else(|e| e.into_inner())
    }
}
