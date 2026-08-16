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

//! Go `stmtstats.go`: the per-session statement statistics counter.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use super::aggregator::global_aggregator;
use super::ru_details::RuDetails;
use super::rustats::{normalize_ru_version, ExecutionContext, RuIncrementMap, RuKey, RuVersion};
use super::ruv2_metrics::{self, RuV2Metrics, RuV2Weights};

/// Go `BinaryDigest`, converted from `parser.Digest.Bytes()` so that it can be
/// used as a map key.
///
/// Go's `type BinaryDigest string` is a byte string, not text, so the bytes are
/// kept verbatim here rather than going through `String`.
#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BinaryDigest(pub Vec<u8>);

impl BinaryDigest {
    /// The raw digest bytes, Go's `[]byte(d)`.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for BinaryDigest {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl From<&str> for BinaryDigest {
    fn from(text: &str) -> Self {
        Self(text.as_bytes().to_vec())
    }
}

impl std::fmt::Debug for BinaryDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}", String::from_utf8_lossy(&self.0))
    }
}

/// Go `SQLPlanDigest`: the key of [`StatementStatsMap`], distinguishing
/// different SQL.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SqlPlanDigest {
    /// Go `SQLPlanDigest.SQLDigest`.
    pub sql_digest: BinaryDigest,
    /// Go `SQLPlanDigest.PlanDigest`.
    pub plan_digest: BinaryDigest,
}

/// Go `newSQLPlanDigest`.
#[must_use]
pub fn new_sql_plan_digest(sql_digest: &[u8], plan_digest: &[u8]) -> SqlPlanDigest {
    SqlPlanDigest {
        sql_digest: BinaryDigest::from(sql_digest),
        plan_digest: BinaryDigest::from(plan_digest),
    }
}

/// Go `KvStatementStatsItem`: the part of [`StatementStatsItem`] holding kv
/// layer indicators.
///
/// Go distinguishes a nil `KvExecCount` map from an empty one only inside
/// `Merge`, where a nil destination adopts the source map wholesale. With owned
/// maps both paths produce the same contents, so `Default` and
/// [`KvStatementStatsItem::new`] coincide.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KvStatementStatsItem {
    /// Go `KvStatementStatsItem.KvExecCount`: the number of SQL executions of
    /// TiKV, per target.
    pub kv_exec_count: HashMap<String, u64>,
}

impl KvStatementStatsItem {
    /// Go `NewKvStatementStatsItem`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `KvStatementStatsItem.Merge`.
    pub fn merge(&mut self, other: &KvStatementStatsItem) {
        for (target, count) in &other.kv_exec_count {
            *self.kv_exec_count.entry(target.clone()).or_default() += count;
        }
    }
}

/// Go `StatementStatsItem`: a set of mergeable statistics for a certain
/// [`SqlPlanDigest`] under a certain timestamp.
///
/// If more indicators are added, their aggregation belongs in
/// [`StatementStatsItem::merge`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatementStatsItem {
    /// Go `StatementStatsItem.KvStatsItem`: all indicators of the kv layer.
    pub kv_stats_item: KvStatementStatsItem,
    /// Go `StatementStatsItem.ExecCount`: the number of SQL executions of TiDB.
    pub exec_count: u64,
    /// Go `StatementStatsItem.SumDurationNs`: the total duration in
    /// nanoseconds.
    pub sum_duration_ns: u64,
    /// Go `StatementStatsItem.DurationCount`: the number of SQL executions
    /// used to calculate SQL duration.
    pub duration_count: u64,
    /// Go `StatementStatsItem.NetworkInBytes`: total network bytes in from the
    /// client.
    pub network_in_bytes: u64,
    /// Go `StatementStatsItem.NetworkOutBytes`: total network bytes out to the
    /// client.
    pub network_out_bytes: u64,
}

impl StatementStatsItem {
    /// Go `NewStatementStatsItem`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `StatementStatsItem.Merge`.
    pub fn merge(&mut self, other: &StatementStatsItem) {
        self.exec_count += other.exec_count;
        self.sum_duration_ns += other.sum_duration_ns;
        self.duration_count += other.duration_count;
        self.network_in_bytes += other.network_in_bytes;
        self.network_out_bytes += other.network_out_bytes;
        self.kv_stats_item.merge(&other.kv_stats_item);
    }
}

/// Go `StatementStatsMap`: the local data type of [`StatementStats`].
///
/// Go's map of `*StatementStatsItem` pointers is a map of owned values here, so
/// the aliasing Go's `Merge` doc warns about cannot arise.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatementStatsMap(pub HashMap<SqlPlanDigest, StatementStatsItem>);

impl StatementStatsMap {
    /// An empty map, Go's `StatementStatsMap{}`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `StatementStatsMap.Merge`: merges `other` in, combining values that
    /// share a [`SqlPlanDigest`].
    ///
    /// Go's nil-receiver / nil-argument early return has no Rust counterpart:
    /// an absent map is the empty map, which merges to a no-op either way.
    pub fn merge(&mut self, other: &StatementStatsMap) {
        for (digest, new_item) in other.iter() {
            match self.0.get_mut(digest) {
                None => {
                    self.0.insert(digest.clone(), new_item.clone());
                }
                Some(item) => item.merge(new_item),
            }
        }
    }

    /// Go `StatementStats.GetOrCreateStatementStatsItem`, which only ever
    /// touches `s.data`.
    ///
    /// Go documents it as "**not** thread-safe"; here the borrow of the map
    /// makes that structural — callers reach it through
    /// [`StatementStats::lock`].
    pub fn get_or_create_statement_stats_item(
        &mut self,
        sql_digest: &[u8],
        plan_digest: &[u8],
    ) -> &mut StatementStatsItem {
        self.0
            .entry(new_sql_plan_digest(sql_digest, plan_digest))
            .or_default()
    }
}

impl Deref for StatementStatsMap {
    type Target = HashMap<SqlPlanDigest, StatementStatsItem>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StatementStatsMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Go `ExecBeginInfo`: optional execution-begin context for extensible stats
/// collection.
///
/// Go carries a `context.Context` whose only use is one
/// `Ctx.Value(util.RUDetailsCtxKey)` lookup at begin time; that lookup is
/// hoisted into the [`ExecBeginInfo::ru_details`] field here, since Rust has no
/// `context.Context` and the value is the sole reason the context is passed.
#[derive(Debug, Default)]
pub struct ExecBeginInfo {
    /// Go's `Ctx.Value(util.RUDetailsCtxKey)`, resolved by the caller.
    pub ru_details: Option<Arc<RuDetails>>,
    /// Go `ExecBeginInfo.RUV2Metrics`.
    pub ruv2_metrics: Option<Arc<RuV2Metrics>>,
    /// Go `ExecBeginInfo.User`.
    pub user: String,
    /// Go `ExecBeginInfo.RUV2Weights`.
    pub ruv2_weights: RuV2Weights,
    /// Go `ExecBeginInfo.InNetworkBytes`.
    pub in_network_bytes: u64,
    /// Go `ExecBeginInfo.RUVersion`.
    pub ru_version: RuVersion,
    /// Go `ExecBeginInfo.TopRUEnabled`.
    pub top_ru_enabled: bool,
}

/// Go `ExecFinishInfo`: optional execution-finish context for extensible stats
/// collection.
#[derive(Debug, Default)]
pub struct ExecFinishInfo {
    /// Go `ExecFinishInfo.RUDetails`.
    pub ru_details: Option<Arc<RuDetails>>,
    /// Go `ExecFinishInfo.User`.
    pub user: String,
    /// Go `ExecFinishInfo.OutNetworkBytes`.
    pub out_network_bytes: u64,
    /// Go `ExecFinishInfo.ExecDuration`, as a signed nanosecond count.
    ///
    /// Go's `time.Duration` is a signed integer and the finish path explicitly
    /// treats a negative duration as "no measurement"; `std::time::Duration`
    /// cannot be negative, so the nanoseconds are carried directly.
    pub exec_duration_ns: i64,
    /// Go `ExecFinishInfo.TopRUEnabled`.
    pub top_ru_enabled: bool,
}

/// Go `StatementObserver`: an abstract callback interface hooked into the
/// corresponding positions of TiDB's SQL statement execution process.
///
/// [`StatementStats`] implements it and performs the counting internally; the
/// caller is only responsible for calling the methods at the right places.
pub trait StatementObserver {
    /// Go `StatementObserver.OnExecutionBegin`, called before statement
    /// execution.
    fn on_execution_begin(
        &self,
        sql_digest: &[u8],
        plan_digest: &[u8],
        info: Option<&ExecBeginInfo>,
    );

    /// Go `StatementObserver.OnExecutionFinished`, called after the statement
    /// is executed.
    ///
    /// WARNING: these callbacks are used by both the Top-SQL and Top-RU
    /// collection paths, and begin/finish are not guaranteed to be paired for
    /// every statement across Top-SQL/Top-RU toggle windows.
    fn on_execution_finished(
        &self,
        sql_digest: &[u8],
        plan_digest: &[u8],
        info: Option<&ExecFinishInfo>,
    );
}

/// The mutex-guarded half of [`StatementStats`], holding exactly the fields
/// Go's `StatementStats.mu` protects.
#[derive(Debug, Default)]
pub struct StatementStatsInner {
    /// Go `StatementStats.data`.
    pub data: StatementStatsMap,
    /// Go `StatementStats.finishedRUBuffer`: completed SQL RU deltas drained
    /// by aggregator ticks.
    pub finished_ru_buffer: RuIncrementMap,
    /// Go `StatementStats.execCtx`: the currently active SQL execution in this
    /// session. TiDB session execution is serialized, so at most one active
    /// context is kept.
    pub exec_ctx: Option<ExecutionContext>,
}

/// Go `StatementStats`: a counter used locally in each session.
///
/// It counts data such as "the number of SQL executions", with the expectation
/// that the statistics are eventually collected and merged in the background.
#[derive(Debug, Default)]
pub struct StatementStats {
    inner: Mutex<StatementStatsInner>,
    finished: AtomicBool,
}

/// Go `CreateStatementStats`: creates and registers a [`StatementStats`].
///
/// Go returns a `*StatementStats` that the global aggregator also holds; the
/// shared ownership is an `Arc` here.
#[must_use]
pub fn create_statement_stats() -> Arc<StatementStats> {
    let stats = Arc::new(StatementStats::default());
    global_aggregator().register(&stats);
    stats
}

impl StatementStats {
    /// The mutex-guarded fields. Go reaches them through `s.mu`.
    pub fn lock(&self) -> MutexGuard<'_, StatementStatsInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go's `addRUOnBeginLocked`.
    fn add_ru_on_begin_locked(
        inner: &mut StatementStatsInner,
        info: &ExecBeginInfo,
        sql_digest: &[u8],
        plan_digest: &[u8],
    ) {
        let key = RuKey {
            user: info.user.clone(),
            sql_digest: BinaryDigest::from(sql_digest),
            plan_digest: BinaryDigest::from(plan_digest),
        };
        // Replace stale execution context defensively.
        inner.exec_ctx = Some(ExecutionContext {
            ru_details: info.ru_details.clone(),
            ruv2_metrics: info.ruv2_metrics.clone(),
            ruv2_weights: info.ruv2_weights,
            ru_version: normalize_ru_version(info.ru_version),
            key: key.clone(),
            last_ru_total: 0.0,
        });
        // ExecCount is begin-based, aligned with Top-SQL semantics.
        inner.finished_ru_buffer.get_or_create(key).exec_count += 1;
    }

    /// Go's `addRUOnFinishLocked`.
    fn add_ru_on_finish_locked(
        inner: &mut StatementStatsInner,
        user: &str,
        sql_digest: &[u8],
        plan_digest: &[u8],
        ru: Option<&RuDetails>,
        exec_duration_ns: i64,
    ) {
        let key = RuKey {
            user: user.to_owned(),
            sql_digest: BinaryDigest::from(sql_digest),
            plan_digest: BinaryDigest::from(plan_digest),
        };
        let delta_ru = {
            let Some(exec_ctx) = inner.exec_ctx.as_mut() else {
                // No matching begin was recorded, so delta cannot be computed
                // correctly.
                return;
            };
            if exec_ctx.key != key {
                // A newer execution has replaced the active context.
                return;
            }

            // Go arms `defer s.clearRUExecCtxLocked()` from here on, so every
            // path below leaves the execution context cleared.
            let current_total_ru = current_ru_total(Some(exec_ctx), ru);
            if current_total_ru <= 0.0 {
                inner.exec_ctx = None;
                return;
            }

            let last_total_ru = exec_ctx.last_ru_total;
            exec_ctx.last_ru_total = current_total_ru;
            current_total_ru - last_total_ru
        };
        inner.exec_ctx = None;
        if delta_ru <= 0.0 {
            // Counter reset or the value was already sampled.
            // Expected behavior: when no new RU is observed, do not add
            // ExecDuration.
            return;
        }
        let incr = inner.finished_ru_buffer.get_or_create(key);
        incr.total_ru += delta_ru;
        incr.exec_duration += exec_duration_ns.unsigned_abs();
    }

    /// Go `StatementStats.ResetRUStateOnVersionChange`: resets RU state for an
    /// RU version handover without touching regular stmt stats.
    pub fn reset_ru_state_on_version_change(&self, current_ru_version: RuVersion) {
        let mut inner = self.lock();
        inner.finished_ru_buffer = RuIncrementMap::new();
        let Some(exec_ctx) = inner.exec_ctx.as_ref() else {
            return;
        };
        if normalize_ru_version(exec_ctx.ru_version) != normalize_ru_version(current_ru_version) {
            inner.exec_ctx = None;
        }
    }

    /// Go `StatementStats.ClearRUExecContext`: discards the active RU
    /// execution context without touching accumulated stmtstats or finished RU
    /// increments.
    pub fn clear_ru_exec_context(&self) {
        self.lock().exec_ctx = None;
    }

    /// Go's `sampleActiveRUDeltaLocked`.
    fn sample_active_ru_delta_locked(
        inner: &mut StatementStatsInner,
        mut result: RuIncrementMap,
    ) -> RuIncrementMap {
        let Some(exec_ctx) = inner.exec_ctx.as_mut() else {
            return result;
        };

        let ru_details = exec_ctx.ru_details.clone();
        let current_total_ru = current_ru_total(Some(exec_ctx), ru_details.as_deref());
        let delta_ru = current_total_ru - exec_ctx.last_ru_total;
        if delta_ru > 0.0 {
            result.get_or_create(exec_ctx.key.clone()).total_ru += delta_ru;
        }
        // Keep last_ru_total in sync even when delta <= 0 (e.g. counter reset).
        exec_ctx.last_ru_total = current_total_ru;
        result
    }

    /// Go `StatementStats.addKvExecCount`: counts the executions of a certain
    /// [`SqlPlanDigest`] for a certain target. Thread-safe.
    pub fn add_kv_exec_count(&self, sql_digest: &[u8], plan_digest: &[u8], target: &str, n: u64) {
        let mut inner = self.lock();
        let item = inner
            .data
            .get_or_create_statement_stats_item(sql_digest, plan_digest);
        *item
            .kv_stats_item
            .kv_exec_count
            .entry(target.to_owned())
            .or_default() += n;
    }

    /// Go `StatementStats.Take`: takes out all existing [`StatementStatsMap`]
    /// data. Thread-safe.
    #[must_use]
    pub fn take(&self) -> StatementStatsMap {
        std::mem::take(&mut self.lock().data)
    }

    /// Go `StatementStats.SetFinished`: marks this [`StatementStats`] as
    /// finished so that no more counting or aggregation happens.
    ///
    /// As the stats are created when a session starts, this is called when the
    /// session ends.
    pub fn set_finished(&self) {
        self.finished.store(true, Ordering::SeqCst);
    }

    /// Go `StatementStats.Finished`.
    #[must_use]
    pub fn finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst)
    }

    /// Go `StatementStats.MergeRUInto`: drains the finished RU buffer and
    /// returns the accumulated RU increments. In-flight RU is sampled in the
    /// same call.
    #[must_use]
    pub fn merge_ru_into(&self) -> RuIncrementMap {
        let mut inner = self.lock();
        let result = std::mem::take(&mut inner.finished_ru_buffer);
        Self::sample_active_ru_delta_locked(&mut inner, result)
    }
}

impl StatementObserver for StatementStats {
    fn on_execution_begin(
        &self,
        sql_digest: &[u8],
        plan_digest: &[u8],
        info: Option<&ExecBeginInfo>,
    ) {
        let mut inner = self.lock();
        {
            let item = inner
                .data
                .get_or_create_statement_stats_item(sql_digest, plan_digest);
            item.exec_count += 1;
            if let Some(info) = info {
                item.network_in_bytes += info.in_network_bytes;
            }
        }
        if let Some(info) = info {
            if info.top_ru_enabled {
                Self::add_ru_on_begin_locked(&mut inner, info, sql_digest, plan_digest);
            }
        }
        // Count more data here.
    }

    fn on_execution_finished(
        &self,
        sql_digest: &[u8],
        plan_digest: &[u8],
        info: Option<&ExecFinishInfo>,
    ) {
        let Some(info) = info else {
            return;
        };
        let ns = info.exec_duration_ns;
        if ns < 0 {
            self.lock().exec_ctx = None;
            return;
        }

        let mut inner = self.lock();
        {
            let item = inner
                .data
                .get_or_create_statement_stats_item(sql_digest, plan_digest);
            item.sum_duration_ns += ns.unsigned_abs();
            item.duration_count += 1;
            item.network_out_bytes += info.out_network_bytes;
        }
        if info.top_ru_enabled {
            let ru = info.ru_details.clone();
            Self::add_ru_on_finish_locked(
                &mut inner,
                &info.user,
                sql_digest,
                plan_digest,
                ru.as_deref(),
                ns,
            );
        } else {
            inner.exec_ctx = None;
        }
        // Count more data here.
    }
}

/// Go's `currentRUTotal`.
fn current_ru_total(exec_ctx: Option<&ExecutionContext>, ru_details: Option<&RuDetails>) -> f64 {
    let Some(exec_ctx) = exec_ctx else {
        return 0.0;
    };

    if normalize_ru_version(exec_ctx.ru_version) == RuVersion::V2 {
        let mut tikv_ru = 0.0;
        let mut tiflash_ru = 0.0;
        if let Some(ru) = ru_details {
            tikv_ru = ru.tikv_ru_v2();
            tiflash_ru = ru.tiflash_ru();
        }
        return ruv2_metrics::total_ru(
            exec_ctx.ruv2_metrics.as_deref(),
            exec_ctx.ruv2_weights,
            tikv_ru,
            tiflash_ru,
        );
    }

    match ru_details {
        None => 0.0,
        Some(ru) => ru.rru() + ru.wru(),
    }
}

#[cfg(test)]
impl StatementStats {
    /// Go's `&StatementStats{...}` struct literals in `stmtstats_test.go` and
    /// `aggregator_test.go`, which deliberately skip `register`.
    pub(super) fn detached() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Go's struct literal with a pre-set `finished` flag.
    pub(super) fn detached_finished(finished: bool) -> Arc<Self> {
        let stats = Self::default();
        stats.finished.store(finished, Ordering::SeqCst);
        Arc::new(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::super::rustats::RuIncrement;
    use super::super::test_support::{
        assert_in_delta, exec_begin, exec_finish, global_test_guard, ru_key, sql_plan_digest,
    };
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    const SECOND_NS: u64 = 1_000_000_000;
    const MILLISECOND_NS: u64 = 1_000_000;

    // Go `TestKvStatementStatsItemMerge`.
    #[test]
    fn kv_statement_stats_item_merge() {
        let mut item1 = KvStatementStatsItem {
            kv_exec_count: HashMap::from([
                ("127.0.0.1:10001".to_owned(), 1),
                ("127.0.0.1:10002".to_owned(), 2),
            ]),
        };
        let item2 = KvStatementStatsItem {
            kv_exec_count: HashMap::from([
                ("127.0.0.1:10002".to_owned(), 2),
                ("127.0.0.1:10003".to_owned(), 3),
            ]),
        };
        assert_eq!(item1.kv_exec_count.len(), 2);
        assert_eq!(item2.kv_exec_count.len(), 2);
        item1.merge(&item2);
        assert_eq!(item1.kv_exec_count.len(), 3);
        assert_eq!(item2.kv_exec_count.len(), 2);
        assert_eq!(item1.kv_exec_count["127.0.0.1:10001"], 1);
        assert_eq!(item1.kv_exec_count["127.0.0.1:10003"], 3);
    }

    // Go `TestStatementsStatsItemMerge`.
    #[test]
    fn statements_stats_item_merge() {
        let mut item1 = StatementStatsItem {
            exec_count: 1,
            sum_duration_ns: 100,
            kv_stats_item: KvStatementStatsItem::new(),
            network_in_bytes: 10,
            network_out_bytes: 20,
            ..StatementStatsItem::new()
        };
        let item2 = StatementStatsItem {
            exec_count: 2,
            sum_duration_ns: 50,
            kv_stats_item: KvStatementStatsItem::new(),
            network_in_bytes: 50,
            network_out_bytes: 60,
            ..StatementStatsItem::new()
        };
        item1.merge(&item2);
        assert_eq!(item1.exec_count, 3);
        assert_eq!(item1.sum_duration_ns, 150);
        assert_eq!(item1.network_in_bytes, 60);
        assert_eq!(item1.network_out_bytes, 80);
    }

    // Go `TestStatementStatsMapMerge`.
    #[test]
    fn statement_stats_map_merge() {
        let kv = || KvStatementStatsItem {
            kv_exec_count: HashMap::from([("KV-1".to_owned(), 1), ("KV-2".to_owned(), 2)]),
        };
        let mut m1 = StatementStatsMap(HashMap::from([
            (
                sql_plan_digest("SQL-1", ""),
                StatementStatsItem {
                    exec_count: 1,
                    sum_duration_ns: 100,
                    kv_stats_item: kv(),
                    ..StatementStatsItem::new()
                },
            ),
            (
                sql_plan_digest("SQL-2", ""),
                StatementStatsItem {
                    exec_count: 1,
                    sum_duration_ns: 200,
                    kv_stats_item: kv(),
                    ..StatementStatsItem::new()
                },
            ),
        ]));
        let m2 = StatementStatsMap(HashMap::from([
            (
                sql_plan_digest("SQL-2", ""),
                StatementStatsItem {
                    exec_count: 1,
                    sum_duration_ns: 100,
                    kv_stats_item: kv(),
                    ..StatementStatsItem::new()
                },
            ),
            (
                sql_plan_digest("SQL-3", ""),
                StatementStatsItem {
                    exec_count: 1,
                    sum_duration_ns: 50,
                    kv_stats_item: kv(),
                    ..StatementStatsItem::new()
                },
            ),
        ]));
        assert_eq!(m1.len(), 2);
        assert_eq!(m2.len(), 2);
        m1.merge(&m2);
        assert_eq!(m1.len(), 3);
        assert_eq!(m2.len(), 2);
        assert_eq!(m1[&sql_plan_digest("SQL-1", "")].exec_count, 1);
        assert_eq!(m1[&sql_plan_digest("SQL-2", "")].exec_count, 2);
        assert_eq!(m1[&sql_plan_digest("SQL-3", "")].exec_count, 1);
        assert_eq!(m1[&sql_plan_digest("SQL-1", "")].sum_duration_ns, 100);
        assert_eq!(m1[&sql_plan_digest("SQL-2", "")].sum_duration_ns, 300);
        assert_eq!(m1[&sql_plan_digest("SQL-3", "")].sum_duration_ns, 50);
        let kv_of = |digest: &str, target: &str| {
            m1[&sql_plan_digest(digest, "")].kv_stats_item.kv_exec_count[target]
        };
        assert_eq!(kv_of("SQL-1", "KV-1"), 1);
        assert_eq!(kv_of("SQL-1", "KV-2"), 2);
        assert_eq!(kv_of("SQL-2", "KV-1"), 2);
        assert_eq!(kv_of("SQL-2", "KV-2"), 4);
        assert_eq!(kv_of("SQL-3", "KV-1"), 1);
        assert_eq!(kv_of("SQL-3", "KV-2"), 2);
        // Go's `m1.Merge(nil)`: the empty map is the nil map's Rust shape.
        m1.merge(&StatementStatsMap::new());
        assert_eq!(m1.len(), 3);
    }

    // Go `TestCreateStatementStats`.
    #[test]
    fn create_statement_stats_registers_and_finishes() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        assert!(global_aggregator().contains_stats(&stats));
        assert!(!stats.finished());
        stats.set_finished();
        assert!(stats.finished());
    }

    // Go `TestStatementStatsRUV2Sampling`.
    #[test]
    fn statement_stats_ruv2_sampling() {
        let _guard = global_test_guard();
        let key = ru_key("u1", "sql", "plan");

        // Go subtest "with ru details".
        {
            let stats = StatementStats::detached();
            let ru = Arc::new(RuDetails::new());
            ru.add_tikv_ru_v2(11.0);
            let metrics = Arc::new(RuV2Metrics::new());
            metrics.add_plan_cnt(3);
            let weights = RuV2Weights {
                ru_scale: 1.0,
                plan_cnt: 2.0,
                ..RuV2Weights::default()
            };
            let info = ExecBeginInfo {
                ru_details: Some(ru.clone()),
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_version: RuVersion::V2,
                ruv2_metrics: Some(metrics.clone()),
                ruv2_weights: weights,
                ..ExecBeginInfo::default()
            };
            stats.on_execution_begin(b"sql", b"plan", Some(&info));

            let first = stats.merge_ru_into();
            assert_eq!(first[&key].exec_count, 1);
            assert_in_delta(17.0, first[&key].total_ru);

            metrics.add_plan_cnt(1);
            ru.add_tikv_ru_v2(5.0);
            let second = stats.merge_ru_into();
            assert_in_delta(7.0, second[&key].total_ru);

            metrics.add_plan_cnt(2);
            ru.add_tikv_ru_v2(4.0);
            stats.on_execution_finished(
                b"sql",
                b"plan",
                Some(&ExecFinishInfo {
                    ru_details: Some(ru.clone()),
                    user: "u1".to_owned(),
                    exec_duration_ns: SECOND_NS as i64,
                    top_ru_enabled: true,
                    ..ExecFinishInfo::default()
                }),
            );
            let finish = stats.merge_ru_into();
            assert_in_delta(8.0, finish[&key].total_ru);
            assert_eq!(finish[&key].exec_duration, SECOND_NS);
        }

        // Go subtest "without ru details still counts tidb ru".
        {
            let stats = StatementStats::detached();
            let metrics = Arc::new(RuV2Metrics::new());
            metrics.add_plan_cnt(3);
            let weights = RuV2Weights {
                ru_scale: 1.0,
                plan_cnt: 2.0,
                ..RuV2Weights::default()
            };
            stats.on_execution_begin(
                b"sql",
                b"plan",
                Some(&ExecBeginInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_version: RuVersion::V2,
                    ruv2_metrics: Some(metrics.clone()),
                    ruv2_weights: weights,
                    ..ExecBeginInfo::default()
                }),
            );

            let first = stats.merge_ru_into();
            assert_eq!(first[&key].exec_count, 1);
            assert_in_delta(6.0, first[&key].total_ru);

            metrics.add_plan_cnt(1);
            let second = stats.merge_ru_into();
            assert_in_delta(2.0, second[&key].total_ru);

            metrics.add_plan_cnt(2);
            stats.on_execution_finished(
                b"sql",
                b"plan",
                Some(&ExecFinishInfo {
                    user: "u1".to_owned(),
                    exec_duration_ns: SECOND_NS as i64,
                    top_ru_enabled: true,
                    ..ExecFinishInfo::default()
                }),
            );
            let finish = stats.merge_ru_into();
            assert_in_delta(4.0, finish[&key].total_ru);
            assert_eq!(finish[&key].exec_duration, SECOND_NS);
        }

        // Go subtest "v2 with nil metrics falls back to external ru".
        {
            let stats = StatementStats::detached();
            let ru = Arc::new(RuDetails::new());
            ru.add_tikv_ru_v2(11.0);

            stats.on_execution_begin(
                b"sql",
                b"plan",
                Some(&ExecBeginInfo {
                    ru_details: Some(ru.clone()),
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_version: RuVersion::V2,
                    ..ExecBeginInfo::default()
                }),
            );

            let first = stats.merge_ru_into();
            assert_eq!(first[&key].exec_count, 1);
            assert_in_delta(11.0, first[&key].total_ru);

            ru.add_tikv_ru_v2(4.0);
            let second = stats.merge_ru_into();
            assert_in_delta(4.0, second[&key].total_ru);
        }
    }

    // Go `TestStatementStatsRUV2InFlightSamplingExcludesDrainOnlyFields`:
    // ResourceManager{Read,Write}Cnt are invisible to in-flight Top-RU samples
    // until the end-of-statement drain, and the in-flight + finalize deltas
    // telescope to the full per-statement total.
    #[test]
    fn statement_stats_ruv2_in_flight_sampling_excludes_drain_only_fields() {
        let _guard = global_test_guard();
        let stats = StatementStats::detached();
        let ru = Arc::new(RuDetails::new());
        let metrics = Arc::new(RuV2Metrics::new());
        let weights = RuV2Weights {
            ru_scale: 1.0,
            plan_cnt: 1.0,
            resource_manager_read_cnt: 0.02,
            resource_manager_write_cnt: 0.07,
            ..RuV2Weights::default()
        };
        metrics.add_plan_cnt(1); // live field, not drain-fed

        stats.on_execution_begin(
            b"sql",
            b"plan",
            Some(&ExecBeginInfo {
                ru_details: Some(ru.clone()),
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_version: RuVersion::V2,
                ruv2_metrics: Some(metrics.clone()),
                ruv2_weights: weights,
                ..ExecBeginInfo::default()
            }),
        );
        let key = ru_key("u1", "sql", "plan");

        // In-flight sample: only PlanCnt visible, drain-fed fields still zero.
        let in_flight = stats.merge_ru_into();
        assert_in_delta(1.0, in_flight[&key].total_ru);
        assert_eq!(in_flight[&key].exec_count, 1);

        // Equivalent of finalizeStatementRUV2Metrics's drain; bypassing kvproto.
        metrics.add_resource_manager_read_cnt(5);
        metrics.add_resource_manager_write_cnt(3);

        stats.on_execution_finished(
            b"sql",
            b"plan",
            Some(&ExecFinishInfo {
                ru_details: Some(ru.clone()),
                user: "u1".to_owned(),
                exec_duration_ns: SECOND_NS as i64,
                top_ru_enabled: true,
                ..ExecFinishInfo::default()
            }),
        );
        let finish = stats.merge_ru_into();
        assert_in_delta(0.31, finish[&key].total_ru); // 5*0.02 + 3*0.07
        assert_in_delta(1.31, in_flight[&key].total_ru + finish[&key].total_ru);
    }

    // Go `TestStatementStatsResetRUStateOnVersionChangePreservesStmtStats`.
    #[test]
    fn statement_stats_reset_ru_state_on_version_change_preserves_stmt_stats() {
        let _guard = global_test_guard();
        struct Case {
            name: &'static str,
            exec_ctx_version: RuVersion,
            current_version: RuVersion,
            expect_nil_exec: bool,
        }
        let cases = [
            Case {
                name: "clear old version exec context",
                exec_ctx_version: RuVersion::V1,
                current_version: RuVersion::V2,
                expect_nil_exec: true,
            },
            Case {
                name: "keep current version exec context",
                exec_ctx_version: RuVersion::V2,
                current_version: RuVersion::V2,
                expect_nil_exec: false,
            },
        ];

        for case in cases {
            let stats = StatementStats::detached();
            {
                let mut inner = stats.lock();
                inner.data.insert(
                    new_sql_plan_digest(b"sql", b"plan"),
                    StatementStatsItem::new(),
                );
                inner.finished_ru_buffer.insert(
                    ru_key("u1", "sql", "plan"),
                    RuIncrement {
                        total_ru: 1.0,
                        ..RuIncrement::default()
                    },
                );
                inner.exec_ctx = Some(ExecutionContext {
                    key: ru_key("u1", "sql", "plan"),
                    ru_version: case.exec_ctx_version,
                    ..ExecutionContext::default()
                });
            }

            stats.reset_ru_state_on_version_change(case.current_version);

            assert_eq!(
                stats.lock().exec_ctx.is_none(),
                case.expect_nil_exec,
                "{}",
                case.name
            );
            assert!(stats.lock().finished_ru_buffer.is_empty(), "{}", case.name);
            assert_eq!(stats.take().len(), 1, "{}", case.name);
        }
    }

    // Go `TestExecCounterAddExecCountTake`.
    #[test]
    fn exec_counter_add_exec_count_take() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let m = stats.take();
        assert_eq!(m.len(), 0);
        for _ in 0..1 {
            stats.on_execution_begin(b"SQL-1", b"", Some(&exec_begin(0)));
        }
        for _ in 0..2 {
            stats.on_execution_begin(b"SQL-2", b"", Some(&exec_begin(0)));
            stats.on_execution_finished(b"SQL-2", b"", Some(&exec_finish(SECOND_NS as i64)));
        }
        for _ in 0..3 {
            stats.on_execution_begin(b"SQL-3", b"", Some(&exec_begin(0)));
            stats.on_execution_finished(b"SQL-3", b"", Some(&exec_finish(MILLISECOND_NS as i64)));
        }
        stats.on_execution_finished(b"SQL-3", b"", Some(&exec_finish(-(MILLISECOND_NS as i64))));
        let m = stats.take();
        assert_eq!(m.len(), 3);
        assert_eq!(m[&sql_plan_digest("SQL-1", "")].exec_count, 1);
        assert_eq!(m[&sql_plan_digest("SQL-1", "")].sum_duration_ns, 0);
        assert_eq!(m[&sql_plan_digest("SQL-2", "")].exec_count, 2);
        assert_eq!(
            m[&sql_plan_digest("SQL-2", "")].sum_duration_ns,
            2 * SECOND_NS
        );
        assert_eq!(m[&sql_plan_digest("SQL-3", "")].exec_count, 3);
        assert_eq!(
            m[&sql_plan_digest("SQL-3", "")].sum_duration_ns,
            3 * MILLISECOND_NS
        );
        let m = stats.take();
        assert_eq!(m.len(), 0);
    }

    // Go `TestNetworkBytesAccumulation`.
    #[test]
    fn network_bytes_accumulation() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let sql_digest = b"SQL-1";
        let plan_digest = b"PLAN-1";

        // NetworkInBytes accumulation in OnExecutionBegin.
        for bytes in [100, 200, 300] {
            stats.on_execution_begin(sql_digest, plan_digest, Some(&exec_begin(bytes)));
        }

        let m = stats.take();
        assert_eq!(m.len(), 1);
        let key = sql_plan_digest("SQL-1", "PLAN-1");
        let item = &m[&key];
        // 100 + 200 + 300 = 600
        assert_eq!(item.network_in_bytes, 600);
        assert_eq!(item.exec_count, 3);

        // NetworkOutBytes accumulation in OnExecutionFinished.
        for bytes in [50, 150, 250] {
            stats.on_execution_finished(
                sql_digest,
                plan_digest,
                Some(&ExecFinishInfo {
                    exec_duration_ns: SECOND_NS as i64,
                    out_network_bytes: bytes,
                    ..ExecFinishInfo::default()
                }),
            );
        }

        let m = stats.take();
        assert_eq!(m.len(), 1);
        let item = &m[&key];
        // 50 + 150 + 250 = 450
        assert_eq!(item.network_out_bytes, 450);
        assert_eq!(item.duration_count, 3);
    }

    // Go `TestOnExecutionBeginFinishRU`: one begin/finish pair emits exactly
    // one RU key with the expected exec-count, RU total, and duration.
    #[test]
    fn on_execution_begin_finish_ru() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        stats.on_execution_begin(
            b"sql1",
            b"plan1",
            Some(&ExecBeginInfo {
                user: "user1".to_owned(),
                top_ru_enabled: true,
                ..ExecBeginInfo::default()
            }),
        );
        let ru = Arc::new(RuDetails::new_with(10.0, 20.0, Duration::from_millis(1)));
        stats.on_execution_finished(
            b"sql1",
            b"plan1",
            Some(&ExecFinishInfo {
                user: "user1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru),
                exec_duration_ns: SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );

        let m = stats.merge_ru_into();
        assert_eq!(m.len(), 1);
        let incr = &m[&ru_key("user1", "sql1", "plan1")];
        assert_eq!(incr.exec_count, 1);
        assert_eq!(incr.total_ru, 30.0);
        assert_eq!(incr.exec_duration, SECOND_NS);
    }

    // Go `TestMergeRUIntoInFlightSamplingAndFinishDedup`: tick sampling plus
    // finish reporting merge into one total without double-counting RU growth.
    #[test]
    fn merge_ru_into_in_flight_sampling_and_finish_dedup() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
        let key = ru_key("user1", "sql1", "plan1");

        stats.on_execution_begin(
            b"sql1",
            b"plan1",
            Some(&ExecBeginInfo {
                user: "user1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                ..ExecBeginInfo::default()
            }),
        );

        let mut total = RuIncrementMap::new();

        ru.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));
        total.merge(&stats.merge_ru_into());

        ru.merge(&RuDetails::new_with(5.0, 0.0, Duration::ZERO));
        total.merge(&stats.merge_ru_into());

        ru.merge(&RuDetails::new_with(7.0, 0.0, Duration::ZERO));
        stats.on_execution_finished(
            b"sql1",
            b"plan1",
            Some(&ExecFinishInfo {
                user: "user1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                exec_duration_ns: 2 * SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        total.merge(&stats.merge_ru_into());

        let incr = &total[&key];
        assert_eq!(incr.exec_count, 1);
        assert_in_delta(22.0, incr.total_ru);
        assert_eq!(incr.exec_duration, 2 * SECOND_NS);

        assert!(stats.lock().exec_ctx.is_none());
        ru.merge(&RuDetails::new_with(3.0, 0.0, Duration::ZERO));
        assert_eq!(stats.merge_ru_into().len(), 0);
    }

    // Go `TestMergeRUIntoHandlesRUResetAndNilRUDetails`: RU counter resets do
    // not emit negative deltas and nil finish RUDetails only clear exec
    // context.
    #[test]
    fn merge_ru_into_handles_ru_reset_and_nil_ru_details() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let ru = Arc::new(RuDetails::new_with(10.0, 0.0, Duration::ZERO));
        let key = ru_key("user2", "sql2", "plan2");

        stats.on_execution_begin(
            b"sql2",
            b"plan2",
            Some(&ExecBeginInfo {
                user: "user2".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                ..ExecBeginInfo::default()
            }),
        );
        let first = stats.merge_ru_into();
        assert_eq!(first.len(), 1);
        assert_in_delta(10.0, first[&key].total_ru);

        stats.lock().exec_ctx.as_mut().unwrap().last_ru_total = 100.0;
        assert_eq!(stats.merge_ru_into().len(), 0);

        ru.merge(&RuDetails::new_with(5.0, 0.0, Duration::ZERO));
        let next = stats.merge_ru_into();
        assert_eq!(next.len(), 1);
        assert_in_delta(5.0, next[&key].total_ru);
        assert!(next[&key].total_ru >= 0.0);

        stats.on_execution_finished(
            b"sql2",
            b"plan2",
            Some(&ExecFinishInfo {
                user: "user2".to_owned(),
                top_ru_enabled: true,
                ru_details: None,
                exec_duration_ns: SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        assert!(stats.lock().exec_ctx.is_none());
    }

    // Go `TestExecCountBeginBasedLongRunningAcrossTicks`.
    #[test]
    fn exec_count_begin_based_long_running_across_ticks() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
        let key = ru_key("u1", "sql", "plan");

        stats.on_execution_begin(
            b"sql",
            b"plan",
            Some(&ExecBeginInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                ..ExecBeginInfo::default()
            }),
        );

        ru.merge(&RuDetails::new_with(4.0, 0.0, Duration::ZERO));
        let tick1 = stats.merge_ru_into();
        assert_eq!(tick1.len(), 1);
        assert_in_delta(4.0, tick1[&key].total_ru);
        assert_eq!(tick1[&key].exec_count, 1);

        ru.merge(&RuDetails::new_with(6.0, 0.0, Duration::ZERO));
        let tick2 = stats.merge_ru_into();
        assert_eq!(tick2.len(), 1);
        assert_in_delta(6.0, tick2[&key].total_ru);
        assert_eq!(tick2[&key].exec_count, 0);

        ru.merge(&RuDetails::new_with(5.0, 0.0, Duration::ZERO));
        stats.on_execution_finished(
            b"sql",
            b"plan",
            Some(&ExecFinishInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                exec_duration_ns: 3 * SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        let finish = stats.merge_ru_into();
        assert_eq!(finish.len(), 1);
        assert_in_delta(5.0, finish[&key].total_ru);
        assert_eq!(finish[&key].exec_count, 0);

        let mut total = RuIncrementMap::new();
        total.merge(&tick1);
        total.merge(&tick2);
        total.merge(&finish);
        assert_eq!(total[&key].exec_count, 1);
        assert_in_delta(15.0, total[&key].total_ru);
    }

    // Go `TestTopRUToggleMidExecutionMatrix`.
    #[test]
    fn top_ru_toggle_mid_execution_matrix() {
        let _guard = global_test_guard();
        let key = ru_key("u1", "sql", "plan");

        // Go subtest "begin-on-finish-off-no-tick".
        {
            let stats = create_statement_stats();
            let ru = Arc::new(RuDetails::new_with(3.0, 0.0, Duration::ZERO));

            stats.on_execution_begin(
                b"sql",
                b"plan",
                Some(&ExecBeginInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_details: Some(ru.clone()),
                    ..ExecBeginInfo::default()
                }),
            );
            stats.on_execution_finished(
                b"sql",
                b"plan",
                Some(&ExecFinishInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: false,
                    ru_details: Some(ru.clone()),
                    exec_duration_ns: SECOND_NS as i64,
                    ..ExecFinishInfo::default()
                }),
            );

            assert!(stats.lock().exec_ctx.is_none());
            let m = stats.merge_ru_into();
            assert_eq!(m.len(), 1);
            let incr = &m[&key];
            assert_eq!(incr.exec_count, 1);
            assert_in_delta(0.0, incr.total_ru);

            ru.merge(&RuDetails::new_with(2.0, 0.0, Duration::ZERO));
            assert_eq!(stats.merge_ru_into().len(), 0);
        }

        // Go subtest "begin-off-finish-on-late-enable".
        {
            let stats = create_statement_stats();
            let ru = Arc::new(RuDetails::new_with(20.0, 0.0, Duration::ZERO));

            stats.on_execution_begin(
                b"sql",
                b"plan",
                Some(&ExecBeginInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: false,
                    ..ExecBeginInfo::default()
                }),
            );
            // Tick while Top-RU is off: no RU data.
            assert_eq!(stats.merge_ru_into().len(), 0);

            stats.on_execution_finished(
                b"sql",
                b"plan",
                Some(&ExecFinishInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_details: Some(ru),
                    exec_duration_ns: SECOND_NS as i64,
                    ..ExecFinishInfo::default()
                }),
            );
            // No begin baseline => skip to avoid cumulative spike.
            assert_eq!(stats.merge_ru_into().len(), 0);
        }

        // Go subtest "begin-on-tick-then-finish-off".
        {
            let stats = create_statement_stats();
            let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));

            stats.on_execution_begin(
                b"sql",
                b"plan",
                Some(&ExecBeginInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_details: Some(ru.clone()),
                    ..ExecBeginInfo::default()
                }),
            );
            ru.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));
            let m1 = stats.merge_ru_into();
            assert_in_delta(10.0, m1[&key].total_ru);

            ru.merge(&RuDetails::new_with(5.0, 0.0, Duration::ZERO));
            stats.on_execution_finished(
                b"sql",
                b"plan",
                Some(&ExecFinishInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: false,
                    ru_details: Some(ru.clone()),
                    exec_duration_ns: SECOND_NS as i64,
                    ..ExecFinishInfo::default()
                }),
            );
            assert_eq!(stats.merge_ru_into().len(), 0);
            assert!(stats.lock().exec_ctx.is_none());
            assert_in_delta(10.0, m1[&key].total_ru);
        }

        // Go subtest "toggle-no-double-count-across-two-sqls".
        {
            let stats = create_statement_stats();
            let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));

            // SQL 1: begin+finish with Top-RU on.
            stats.on_execution_begin(
                b"sql",
                b"plan",
                Some(&ExecBeginInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_details: Some(ru.clone()),
                    ..ExecBeginInfo::default()
                }),
            );
            ru.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));
            stats.on_execution_finished(
                b"sql",
                b"plan",
                Some(&ExecFinishInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_details: Some(ru.clone()),
                    exec_duration_ns: SECOND_NS as i64,
                    ..ExecFinishInfo::default()
                }),
            );

            // SQL 2: begin with Top-RU off, finish with Top-RU on.
            let ru2 = Arc::new(RuDetails::new_with(20.0, 0.0, Duration::ZERO));
            stats.on_execution_begin(
                b"sql",
                b"plan",
                Some(&ExecBeginInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: false,
                    ..ExecBeginInfo::default()
                }),
            );
            stats.on_execution_finished(
                b"sql",
                b"plan",
                Some(&ExecFinishInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_details: Some(ru2),
                    exec_duration_ns: SECOND_NS as i64,
                    ..ExecFinishInfo::default()
                }),
            );

            let m = stats.merge_ru_into();
            assert_eq!(m.len(), 1);
            assert_in_delta(10.0, m[&key].total_ru);
            assert_eq!(m[&key].exec_count, 1);
        }
    }

    // Go `TestExecCountBeginBasedRUZeroNoNoise`: zero-RU executions still keep
    // begin-based exec-count but do not produce extra RU deltas at finish.
    #[test]
    fn exec_count_begin_based_ru_zero_no_noise() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
        let key = ru_key("u3", "sql", "plan");

        stats.on_execution_begin(
            b"sql",
            b"plan",
            Some(&ExecBeginInfo {
                user: "u3".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                ..ExecBeginInfo::default()
            }),
        );
        let m = stats.merge_ru_into();
        assert_eq!(m.len(), 1);
        let incr = &m[&key];
        assert_eq!(incr.exec_count, 1);
        assert_in_delta(0.0, incr.total_ru);

        stats.on_execution_finished(
            b"sql",
            b"plan",
            Some(&ExecFinishInfo {
                user: "u3".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru),
                exec_duration_ns: SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        assert_eq!(stats.merge_ru_into().len(), 0);
    }

    // Go `TestExecCountBeginBasedBucketMergeSameTick`: the same-tick merge
    // combines finished and in-flight RU into one bucket with deterministic
    // exec-count accumulation.
    #[test]
    fn exec_count_begin_based_bucket_merge_same_tick() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let key = ru_key("u1", "sql", "plan");

        // Execution 1: finish first, data stays in finished_ru_buffer before
        // the next tick.
        let ru1 = Arc::new(RuDetails::new_with(6.0, 0.0, Duration::ZERO));
        stats.on_execution_begin(
            b"sql",
            b"plan",
            Some(&ExecBeginInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru1.clone()),
                ..ExecBeginInfo::default()
            }),
        );
        stats.on_execution_finished(
            b"sql",
            b"plan",
            Some(&ExecFinishInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru1),
                exec_duration_ns: SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );

        // Execution 2: active with a positive delta before the same tick drains.
        let ru2 = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
        stats.on_execution_begin(
            b"sql",
            b"plan",
            Some(&ExecBeginInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru2.clone()),
                ..ExecBeginInfo::default()
            }),
        );
        ru2.merge(&RuDetails::new_with(4.0, 0.0, Duration::ZERO));

        let m = stats.merge_ru_into();
        assert_eq!(m.len(), 1);
        assert_in_delta(10.0, m[&key].total_ru);
        assert_eq!(m[&key].exec_count, 2);
    }

    // Go `TestExecCountBeginBasedFinishAndTickConcurrent`: begin-based
    // ExecCount must remain 1 even when finish and tick race.
    #[test]
    fn exec_count_begin_based_finish_and_tick_concurrent() {
        let _guard = global_test_guard();
        const ROUNDS: usize = 100;
        let key = ru_key("u1", "sql", "plan");

        for _ in 0..ROUNDS {
            let stats = create_statement_stats();
            let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
            stats.on_execution_begin(
                b"sql",
                b"plan",
                Some(&ExecBeginInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_details: Some(ru.clone()),
                    ..ExecBeginInfo::default()
                }),
            );
            ru.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));

            let start = Arc::new(std::sync::Barrier::new(2));
            let (tick_tx, tick_rx) = mpsc::channel();

            let tick_handle = {
                let (stats, start) = (stats.clone(), start.clone());
                std::thread::spawn(move || {
                    start.wait();
                    tick_tx.send(stats.merge_ru_into()).unwrap();
                })
            };
            let finish_handle = {
                let (stats, start, ru) = (stats.clone(), start.clone(), ru.clone());
                std::thread::spawn(move || {
                    start.wait();
                    stats.on_execution_finished(
                        b"sql",
                        b"plan",
                        Some(&ExecFinishInfo {
                            user: "u1".to_owned(),
                            top_ru_enabled: true,
                            ru_details: Some(ru),
                            exec_duration_ns: SECOND_NS as i64,
                            ..ExecFinishInfo::default()
                        }),
                    );
                })
            };

            let tick_result = tick_rx.recv().unwrap();
            tick_handle.join().unwrap();
            finish_handle.join().unwrap();
            let tail_result = stats.merge_ru_into();
            assert_eq!(tick_result.len(), 1);
            assert_eq!(tail_result.len(), 0);

            let mut total = RuIncrementMap::new();
            total.merge(&tick_result);
            total.merge(&tail_result);

            assert_eq!(total.len(), 1);
            let incr = &total[&key];
            assert_in_delta(10.0, incr.total_ru);
            assert_eq!(incr.exec_count, 1);
            assert!(stats.lock().exec_ctx.is_none());
        }
    }

    // Go `TestExecCountBeginBasedFinishTickBucketSemantics`: whether tick or
    // finish happens first, exactly one bucket gets the delta and the next
    // bucket remains empty.
    #[test]
    fn exec_count_begin_based_finish_tick_bucket_semantics() {
        let _guard = global_test_guard();
        let key = ru_key("u1", "sql", "plan");

        // Go subtests "tick-first" and "finish-first".
        for tick_first in [true, false] {
            let stats = create_statement_stats();
            let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
            stats.on_execution_begin(
                b"sql",
                b"plan",
                Some(&ExecBeginInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_details: Some(ru.clone()),
                    ..ExecBeginInfo::default()
                }),
            );
            ru.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));

            let mut bucket_a = RuIncrementMap::new();
            if tick_first {
                bucket_a = stats.merge_ru_into();
            }
            stats.on_execution_finished(
                b"sql",
                b"plan",
                Some(&ExecFinishInfo {
                    user: "u1".to_owned(),
                    top_ru_enabled: true,
                    ru_details: Some(ru.clone()),
                    exec_duration_ns: SECOND_NS as i64,
                    ..ExecFinishInfo::default()
                }),
            );
            if !tick_first {
                bucket_a = stats.merge_ru_into();
            }
            let bucket_b = stats.merge_ru_into();

            assert_eq!(bucket_a.len(), 1, "tick_first={tick_first}");
            let incr = &bucket_a[&key];
            assert_in_delta(10.0, incr.total_ru);
            assert_eq!(incr.exec_count, 1);
            assert_eq!(bucket_b.len(), 0);
            assert!(stats.lock().exec_ctx.is_none());
        }
    }

    // Go `TestExecCountBeginBasedTickThenGrow`: the first tick emits
    // begin-based count=1; a later finish only emits tail RU/duration with
    // count=0.
    #[test]
    fn exec_count_begin_based_tick_then_grow() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let key = ru_key("u1", "sql", "plan");
        let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
        stats.on_execution_begin(
            b"sql",
            b"plan",
            Some(&ExecBeginInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                ..ExecBeginInfo::default()
            }),
        );

        ru.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));
        let bucket_a = stats.merge_ru_into();
        assert_eq!(bucket_a.len(), 1);
        let incr_a = &bucket_a[&key];
        assert_in_delta(10.0, incr_a.total_ru);
        assert_eq!(incr_a.exec_count, 1);
        assert_eq!(incr_a.exec_duration, 0);

        ru.merge(&RuDetails::new_with(5.0, 0.0, Duration::ZERO));
        stats.on_execution_finished(
            b"sql",
            b"plan",
            Some(&ExecFinishInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                exec_duration_ns: 2 * SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        let bucket_b = stats.merge_ru_into();
        assert_eq!(bucket_b.len(), 1);
        let incr_b = &bucket_b[&key];
        assert_in_delta(5.0, incr_b.total_ru);
        assert_eq!(incr_b.exec_count, 0);
        assert_eq!(incr_b.exec_duration, 2 * SECOND_NS);

        let mut total = RuIncrementMap::new();
        total.merge(&bucket_a);
        total.merge(&bucket_b);
        assert_in_delta(15.0, total[&key].total_ru);
        assert_eq!(total[&key].exec_count, 1);
        assert_eq!(total[&key].exec_duration, 2 * SECOND_NS);
        assert_eq!(stats.merge_ru_into().len(), 0);
        assert!(stats.lock().exec_ctx.is_none());
    }

    // Go `TestExecCountBeginBasedTickThen`.
    #[test]
    fn exec_count_begin_based_tick_then() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let key = ru_key("u1", "sql", "plan");
        let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
        stats.on_execution_begin(
            b"sql",
            b"plan",
            Some(&ExecBeginInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                ..ExecBeginInfo::default()
            }),
        );

        ru.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));
        let bucket_a = stats.merge_ru_into();
        assert_eq!(bucket_a.len(), 1);
        let incr_a = &bucket_a[&key];
        assert_in_delta(10.0, incr_a.total_ru);
        assert_eq!(incr_a.exec_count, 1);

        ru.merge(&RuDetails::new_with(5.0, 0.0, Duration::ZERO));
        stats.on_execution_finished(
            b"sql",
            b"plan",
            Some(&ExecFinishInfo {
                user: "u1".to_owned(),
                top_ru_enabled: false,
                ru_details: Some(ru),
                exec_duration_ns: 2 * SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        let bucket_b = stats.merge_ru_into();
        assert_eq!(bucket_b.len(), 0);
        assert!(stats.lock().exec_ctx.is_none());
    }

    // Go `TestExecCountBeginBasedTickThenReset`: the reset-after-tick path does
    // not emit negative tail deltas and still clears the execution context.
    #[test]
    fn exec_count_begin_based_tick_then_reset() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let key = ru_key("u1", "sql", "plan");
        let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
        stats.on_execution_begin(
            b"sql",
            b"plan",
            Some(&ExecBeginInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                ..ExecBeginInfo::default()
            }),
        );

        ru.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));
        let bucket_a = stats.merge_ru_into();
        assert_eq!(bucket_a.len(), 1);
        let incr_a = &bucket_a[&key];
        assert_in_delta(10.0, incr_a.total_ru);
        assert_eq!(incr_a.exec_count, 1);

        stats.lock().exec_ctx.as_mut().unwrap().last_ru_total = 100.0;
        ru.merge(&RuDetails::new_with(5.0, 0.0, Duration::ZERO));

        stats.on_execution_finished(
            b"sql",
            b"plan",
            Some(&ExecFinishInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru),
                exec_duration_ns: 2 * SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        let bucket_b = stats.merge_ru_into();
        assert_eq!(bucket_b.len(), 0);
        assert!(stats.lock().exec_ctx.is_none());
    }

    // Go `TestExecCountBeginBasedKeySwitchNoCrossPollution`: a stale finish for
    // keyA must not write into keyB, even when keyB is the active execution
    // context.
    #[test]
    fn exec_count_begin_based_key_switch_no_cross_pollution() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let key_a = ru_key("u1", "sqlA", "planA");
        let key_b = ru_key("u1", "sqlB", "planB");

        let ru_a = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
        stats.on_execution_begin(
            b"sqlA",
            b"planA",
            Some(&ExecBeginInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru_a.clone()),
                ..ExecBeginInfo::default()
            }),
        );

        ru_a.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));
        let bucket_a = stats.merge_ru_into();
        assert_eq!(bucket_a.len(), 1);
        let incr_a = &bucket_a[&key_a];
        assert_in_delta(10.0, incr_a.total_ru);
        assert_eq!(incr_a.exec_count, 1);

        let ru_b = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));
        stats.on_execution_begin(
            b"sqlB",
            b"planB",
            Some(&ExecBeginInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru_b.clone()),
                ..ExecBeginInfo::default()
            }),
        );

        ru_a.merge(&RuDetails::new_with(5.0, 0.0, Duration::ZERO));
        stats.on_execution_finished(
            b"sqlA",
            b"planA",
            Some(&ExecFinishInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru_a),
                exec_duration_ns: 2 * SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        let pseudo_b = stats.merge_ru_into();
        assert_eq!(pseudo_b.len(), 1);
        // Stale finish for keyA should not contaminate keyB.
        assert!(!pseudo_b.contains_key(&key_a));
        let incr_pseudo_b = &pseudo_b[&key_b];
        assert_eq!(incr_pseudo_b.exec_count, 1);
        assert_in_delta(0.0, incr_pseudo_b.total_ru);

        ru_b.merge(&RuDetails::new_with(7.0, 0.0, Duration::ZERO));
        stats.on_execution_finished(
            b"sqlB",
            b"planB",
            Some(&ExecFinishInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru_b),
                exec_duration_ns: SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        let bucket_b = stats.merge_ru_into();
        assert_eq!(bucket_b.len(), 1);
        let incr_b = &bucket_b[&key_b];
        assert_in_delta(7.0, incr_b.total_ru);
        assert_eq!(incr_b.exec_count, 0);
        // Across buckets, keyB still has exactly one begin-based ExecCount.
        let mut total = RuIncrementMap::new();
        total.merge(&pseudo_b);
        total.merge(&bucket_b);
        assert_eq!(total[&key_b].exec_count, 1);
        assert_in_delta(7.0, total[&key_b].total_ru);

        assert!(stats.lock().exec_ctx.is_none());
    }

    // Go `TestMultiTickDeltaSumEqualsFinalTotal`: multiple ticks sample active
    // SQL deltas, then finish samples the remaining delta; sum(all deltas)
    // equals the final total RU.
    #[test]
    fn multi_tick_delta_sum_equals_final_total() {
        let _guard = global_test_guard();
        let stats = create_statement_stats();
        let key = ru_key("u1", "sql", "plan");
        let ru = Arc::new(RuDetails::new_with(0.0, 0.0, Duration::ZERO));

        stats.on_execution_begin(
            b"sql",
            b"plan",
            Some(&ExecBeginInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru.clone()),
                ..ExecBeginInfo::default()
            }),
        );

        // Tick 1: RU grows to 10.
        ru.merge(&RuDetails::new_with(10.0, 0.0, Duration::ZERO));
        let m1 = stats.merge_ru_into();
        assert_in_delta(10.0, m1[&key].total_ru);
        let mut all_deltas = m1;

        // Tick 2: RU grows to 25.
        ru.merge(&RuDetails::new_with(15.0, 0.0, Duration::ZERO));
        let m2 = stats.merge_ru_into();
        assert_in_delta(15.0, m2[&key].total_ru);
        all_deltas.merge(&m2);

        // Tick 3: RU grows to 33.
        ru.merge(&RuDetails::new_with(8.0, 0.0, Duration::ZERO));
        let m3 = stats.merge_ru_into();
        assert_in_delta(8.0, m3[&key].total_ru);
        all_deltas.merge(&m3);

        // Finish: RU grows to 50.
        ru.merge(&RuDetails::new_with(17.0, 0.0, Duration::ZERO));
        stats.on_execution_finished(
            b"sql",
            b"plan",
            Some(&ExecFinishInfo {
                user: "u1".to_owned(),
                top_ru_enabled: true,
                ru_details: Some(ru),
                exec_duration_ns: 5 * SECOND_NS as i64,
                ..ExecFinishInfo::default()
            }),
        );
        let m_final = stats.merge_ru_into();
        all_deltas.merge(&m_final);

        // sum(all deltas) must equal the final cumulative total.
        assert_in_delta(50.0, all_deltas[&key].total_ru);
        assert_eq!(all_deltas[&key].exec_count, 1);
        assert!(stats.lock().exec_ctx.is_none());
        assert_eq!(stats.merge_ru_into().len(), 0);
    }
}
