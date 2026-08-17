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

//! Complete transcreation of Go `pkg/util/topsql/reporter/datamodel.go`: the
//! in-memory Top-SQL collection data model, with all 26 test functions of
//! `datamodel_test.go`.
//!
//! Data naming and relationship, quoting Go's own header comment:
//!
//! ```text
//! tsItem:  timestamp + cpuTime + stmtStats(execCount, durationSum, ...)
//! tsItems: [ tsItem | tsItem | ... ]
//! record:  tsItems + tsIndex { 1640500000 => 0 | 1640500001 => 1 | ... }
//! records: [ record | record | ... ]
//! collecting: records { sqlPlanDigest => record } + evicted { sqlPlanDigest }
//! cpuRecords: [ SQLCPUTimeRecord | SQLCPUTimeRecord | ... ]
//! normalizedSQLMap:  { sqlDigest => normalizedSQL }
//! normalizedPlanMap: { planDigest => normalizedPlan }
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::topsql_state::{self, DEF_TIDB_TOP_SQL_REPORT_INTERVAL_SECONDS};
use crate::topsql_stmtstats::StatementStatsItem;

/// Go `keyOthers`: the key that stores the aggregation of all records that
/// fall out of Top N. Go's key type is `string`; here the collection key is
/// the raw concatenation of the two digests, so "others" is the empty key.
pub const KEY_OTHERS: &[u8] = b"";

/// Go `maxTsItemsCapacity`: a protection against excessive memory usage
/// caused by an incorrect configuration.
pub const MAX_TS_ITEMS_CAPACITY: i64 = 1000;

/// boundary: Go `reporter/metrics.IgnoreExceedSQLCounter`, a prometheus
/// counter. `tidb-util` carries no metric registry for the reporter, so the
/// two ignore counters become process counters with the same names and the
/// same increment points.
pub static IGNORE_EXCEED_SQL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// boundary: Go `reporter/metrics.IgnoreExceedPlanCounter`.
pub static IGNORE_EXCEED_PLAN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// boundary: Go `collector.SQLCPUTimeRecord`, declared locally rather than
/// pulling in `pkg/util/topsql/collector` (whose profiler machinery is not
/// part of this port). The four fields, and only they, are what the reporter
/// data model consumes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqlCpuTimeRecord {
    /// Go `SQLCPUTimeRecord.SQLDigest`.
    pub sql_digest: Vec<u8>,
    /// Go `SQLCPUTimeRecord.PlanDigest`.
    pub plan_digest: Vec<u8>,
    /// Go `SQLCPUTimeRecord.CPUTimeMs`.
    pub cpu_time_ms: u32,
}

/// boundary: Go `tipb.TopSQLRecordItem`. `tidb-proto` generates no tipb
/// top-sql messages, so the flat repeated-message shapes this file converts
/// into are declared locally instead of adding a proto-generation dependency.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TopSqlRecordItem {
    /// Go `TimestampSec`.
    pub timestamp_sec: u64,
    /// Go `CpuTimeMs`.
    pub cpu_time_ms: u32,
    /// Go `StmtExecCount`.
    pub stmt_exec_count: u64,
    /// Go `StmtKvExecCount`.
    pub stmt_kv_exec_count: HashMap<String, u64>,
    /// Go `StmtDurationSumNs`.
    pub stmt_duration_sum_ns: u64,
    /// Go `StmtDurationCount`.
    pub stmt_duration_count: u64,
    /// Go `StmtNetworkInBytes`.
    pub stmt_network_in_bytes: u64,
    /// Go `StmtNetworkOutBytes`.
    pub stmt_network_out_bytes: u64,
}

/// boundary: Go `tipb.TopSQLRecord`, declared locally (see
/// [`TopSqlRecordItem`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TopSqlRecordProto {
    /// Go `KeyspaceName`.
    pub keyspace_name: Vec<u8>,
    /// Go `SqlDigest`.
    pub sql_digest: Vec<u8>,
    /// Go `PlanDigest`.
    pub plan_digest: Vec<u8>,
    /// Go `Items`.
    pub items: Vec<TopSqlRecordItem>,
}

impl TopSqlRecordProto {
    /// Go's generated `GetKeyspaceName`.
    #[must_use]
    pub fn get_keyspace_name(&self) -> &[u8] {
        &self.keyspace_name
    }
}

/// boundary: Go `tipb.SQLMeta`, declared locally (see [`TopSqlRecordItem`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqlMetaProto {
    /// Go `KeyspaceName`.
    pub keyspace_name: Vec<u8>,
    /// Go `SqlDigest`.
    pub sql_digest: Vec<u8>,
    /// Go `NormalizedSql`.
    pub normalized_sql: String,
    /// Go `IsInternalSql`.
    pub is_internal_sql: bool,
}

/// boundary: Go `tipb.PlanMeta`, declared locally (see [`TopSqlRecordItem`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanMetaProto {
    /// Go `KeyspaceName`.
    pub keyspace_name: Vec<u8>,
    /// Go `PlanDigest`.
    pub plan_digest: Vec<u8>,
    /// Go `NormalizedPlan`.
    pub normalized_plan: String,
    /// Go `EncodedNormalizedPlan`.
    pub encoded_normalized_plan: String,
}

/// Go `tsItem`: a self-contained complete piece of data for a certain
/// timestamp.
///
/// Go's `zeroTsItem` exists only to allocate the nil `KvExecCount` map;
/// [`Default`] covers it, since an owned empty map and Go's freshly made map
/// behave identically.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TsItem {
    /// Go `tsItem.stmtStats`.
    pub stmt_stats: StatementStatsItem,
    /// Go `tsItem.timestamp`.
    pub timestamp: u64,
    /// Go `tsItem.cpuTimeMs`.
    pub cpu_time_ms: u32,
}

impl TsItem {
    /// Go `tsItem.toProto`.
    #[must_use]
    pub fn to_proto(&self) -> TopSqlRecordItem {
        TopSqlRecordItem {
            timestamp_sec: self.timestamp,
            cpu_time_ms: self.cpu_time_ms,
            stmt_exec_count: self.stmt_stats.exec_count,
            stmt_kv_exec_count: self.stmt_stats.kv_stats_item.kv_exec_count.clone(),
            stmt_duration_sum_ns: self.stmt_stats.sum_duration_ns,
            stmt_duration_count: self.stmt_stats.duration_count,
            stmt_network_in_bytes: self.stmt_stats.network_in_bytes,
            stmt_network_out_bytes: self.stmt_stats.network_out_bytes,
            // Convert more indicators here.
        }
    }
}

/// Go `tsItems`: a list of [`TsItem`] sortable by timestamp (asc).
///
/// Go implements `sort.Interface` on it; `Less`/`Swap`/`Len` exist only to
/// feed `sort.Sort`, which here is `sort_by_key(|item| item.timestamp)` (and
/// [`Record::sort_by_timestamp`] when the owning record's index must follow).
pub type TsItems = Vec<TsItem>;

/// Go `tsItems.sorted`.
#[must_use]
pub fn ts_items_sorted(items: &[TsItem]) -> bool {
    items
        .windows(2)
        .all(|pair| pair[0].timestamp <= pair[1].timestamp)
}

/// Go `tsItems.toProto`. Go returns nil for an empty list, which is the empty
/// slice on the wire either way.
#[must_use]
pub fn ts_items_to_proto(items: &[TsItem]) -> Vec<TopSqlRecordItem> {
    items.iter().map(TsItem::to_proto).collect()
}

/// Go `record`: the cumulative [`TsItem`]s in the current minute window.
///
/// `ts_items` is not guaranteed sorted by timestamp when time jumps backward.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Record {
    /// Go `record.tsIndex`: finds the [`TsItem`] index for a timestamp.
    pub ts_index: HashMap<u64, usize>,
    /// Go `record.sqlDigest`.
    pub sql_digest: Vec<u8>,
    /// Go `record.planDigest`.
    pub plan_digest: Vec<u8>,
    /// Go `record.tsItems`.
    pub ts_items: TsItems,
    /// Go `record.totalCPUTimeMs`.
    pub total_cpu_time_ms: u64,
}

impl Record {
    /// Go `newRecord`.
    #[must_use]
    pub fn new(sql_digest: Vec<u8>, plan_digest: Vec<u8>) -> Self {
        let precision = topsql_state::GLOBAL_STATE
            .precision_seconds
            .load(Ordering::SeqCst)
            .max(1);
        let list_cap = (DEF_TIDB_TOP_SQL_REPORT_INTERVAL_SECONDS / precision + 1)
            .clamp(0, MAX_TS_ITEMS_CAPACITY) as usize;
        Self {
            ts_index: HashMap::with_capacity(list_cap),
            sql_digest,
            plan_digest,
            ts_items: Vec::with_capacity(list_cap),
            total_cpu_time_ms: 0,
        }
    }

    /// Go's `sort.Sort(&record)`: sorts `ts_items` by timestamp ascending and
    /// keeps `ts_index` consistent.
    ///
    /// Go threads the index update through `record.Swap`, which — starting
    /// from an index that agrees with `ts_items` — leaves exactly the mapping
    /// [`Record::rebuild_ts_index`] produces, so the rebuild is used directly.
    pub fn sort_by_timestamp(&mut self) {
        self.ts_items.sort_by_key(|item| item.timestamp);
        self.rebuild_ts_index();
    }

    /// Go `record.appendCPUTime`: appends `cpu_time_ms` under `timestamp`,
    /// adding to an existing [`TsItem`] when that timestamp is already known.
    pub fn append_cpu_time(&mut self, timestamp: u64, cpu_time_ms: u32) {
        if let Some(&index) = self.ts_index.get(&timestamp) {
            // appendStmtStatsItem may already have created this item with a
            // zero cpuTimeMs, so add rather than overwrite.
            self.ts_items[index].cpu_time_ms += cpu_time_ms;
        } else {
            let new_item = TsItem {
                timestamp,
                cpu_time_ms,
                ..TsItem::default()
            };
            self.ts_index.insert(timestamp, self.ts_items.len());
            self.ts_items.push(new_item);
        }
        self.total_cpu_time_ms += u64::from(cpu_time_ms);
    }

    /// Go `record.appendStmtStatsItem`: appends a [`StatementStatsItem`] under
    /// `timestamp`, merging into an existing [`TsItem`] when that timestamp is
    /// already known.
    pub fn append_stmt_stats_item(&mut self, timestamp: u64, item: StatementStatsItem) {
        if let Some(&index) = self.ts_index.get(&timestamp) {
            self.ts_items[index].stmt_stats.merge(&item);
        } else {
            let new_item = TsItem {
                timestamp,
                stmt_stats: item,
                cpu_time_ms: 0,
            };
            self.ts_index.insert(timestamp, self.ts_items.len());
            self.ts_items.push(new_item);
        }
    }

    /// Go `record.merge`: merges `other` into `self`.
    ///
    /// Depends on `self` being sorted, and sorts `other` by timestamp — which
    /// is why Go takes a pointer and this takes `&mut`.
    pub fn merge(&mut self, other: &mut Record) {
        if other.ts_items.is_empty() {
            return;
        }
        if !ts_items_sorted(&other.ts_items) {
            other.sort_by_timestamp(); // this may never happen
        }
        if self.ts_items.is_empty() {
            self.total_cpu_time_ms = other.total_cpu_time_ms;
            self.ts_items = std::mem::take(&mut other.ts_items);
            self.ts_index = std::mem::take(&mut other.ts_index);
            return;
        }
        let length = self.ts_items.len() + other.ts_items.len();
        let mut new_ts_items: TsItems = Vec::with_capacity(length);
        let (mut i, mut j) = (0, 0);
        while i < self.ts_items.len() && j < other.ts_items.len() {
            if self.ts_items[i].timestamp == other.ts_items[j].timestamp {
                let mut new_item = TsItem {
                    timestamp: self.ts_items[i].timestamp,
                    cpu_time_ms: self.ts_items[i].cpu_time_ms + other.ts_items[j].cpu_time_ms,
                    ..TsItem::default()
                };
                self.ts_items[i]
                    .stmt_stats
                    .merge(&other.ts_items[j].stmt_stats);
                new_item.stmt_stats = self.ts_items[i].stmt_stats.clone();
                new_ts_items.push(new_item);
                i += 1;
                j += 1;
            } else if self.ts_items[i].timestamp < other.ts_items[j].timestamp {
                new_ts_items.push(self.ts_items[i].clone());
                i += 1;
            } else {
                new_ts_items.push(other.ts_items[j].clone());
                j += 1;
            }
        }
        if i < self.ts_items.len() {
            new_ts_items.extend_from_slice(&self.ts_items[i..]);
        }
        if j < other.ts_items.len() {
            new_ts_items.extend_from_slice(&other.ts_items[j..]);
        }
        self.ts_items = new_ts_items;
        self.total_cpu_time_ms += other.total_cpu_time_ms;
        self.rebuild_ts_index();
    }

    /// Go `record.rebuildTsIndex`.
    pub fn rebuild_ts_index(&mut self) {
        if self.ts_items.is_empty() {
            self.ts_index = HashMap::new();
            return;
        }
        self.ts_index = HashMap::with_capacity(self.ts_items.len());
        for (index, item) in self.ts_items.iter().enumerate() {
            self.ts_index.insert(item.timestamp, index);
        }
    }

    /// Go `record.toProto`.
    #[must_use]
    pub fn to_proto(&self, keyspace_name: &[u8]) -> TopSqlRecordProto {
        TopSqlRecordProto {
            keyspace_name: keyspace_name.to_vec(),
            sql_digest: self.sql_digest.clone(),
            plan_digest: self.plan_digest.clone(),
            items: ts_items_to_proto(&self.ts_items),
        }
    }
}

/// Go `records`: a list of [`Record`] sortable by `totalCPUTimeMs` (desc).
pub type Records = Vec<Record>;

/// Go's `sort.Sort(records)`: orders by `total_cpu_time_ms` **DESC**.
pub fn sort_records(rs: &mut Records) {
    rs.sort_by_key(|r| std::cmp::Reverse(r.total_cpu_time_ms));
}

/// Go `records.topN`: returns the largest `n` records by `total_cpu_time_ms`,
/// with the rest returned as evicted.
///
/// boundary: Go uses `quickselect.QuickSelect`, which only partitions — the
/// order *within* each part is unspecified, and on the small inputs the tests
/// use it happens to come out sorted. A full descending sort satisfies the
/// same partition contract and additionally pins the order the tests observe,
/// so the ordering is not left to an implementation detail.
#[must_use]
pub fn records_top_n(mut rs: Records, n: usize) -> (Records, Records) {
    if rs.len() <= n {
        return (rs, Vec::new());
    }
    sort_records(&mut rs);
    let evicted = rs.split_off(n);
    (rs, evicted)
}

/// Go `records.toProto`.
#[must_use]
pub fn records_to_proto(rs: &Records, keyspace_name: &[u8]) -> Vec<TopSqlRecordProto> {
    rs.iter().map(|r| r.to_proto(keyspace_name)).collect()
}

/// Go `cpuRecords`: a list of [`SqlCpuTimeRecord`] sortable by `CPUTimeMs`
/// (desc).
pub type CpuRecords = Vec<SqlCpuTimeRecord>;

/// Go's `sort.Sort(cpuRecords)`: orders by `cpu_time_ms` **DESC**.
pub fn sort_cpu_records(rs: &mut CpuRecords) {
    rs.sort_by_key(|r| std::cmp::Reverse(r.cpu_time_ms));
}

/// Go `cpuRecords.topN`, with the same quickselect narrowing as
/// [`records_top_n`].
#[must_use]
pub fn cpu_records_top_n(mut rs: CpuRecords, n: usize) -> (CpuRecords, CpuRecords) {
    if rs.len() <= n {
        return (rs, Vec::new());
    }
    sort_cpu_records(&mut rs);
    let evicted = rs.split_off(n);
    (rs, evicted)
}

/// Go `encodeKey`: the record key is the concatenation of the two digests.
///
/// boundary: Go threads a reusable `*bytes.Buffer` through every call to
/// avoid an allocation; the buffer is not part of the semantics and is
/// dropped here.
#[must_use]
pub fn encode_key(sql_digest: &[u8], plan_digest: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(sql_digest.len() + plan_digest.len());
    key.extend_from_slice(sql_digest);
    key.extend_from_slice(plan_digest);
    key
}

/// Go `collecting`: the collection of data being collected by the reporter.
#[derive(Debug, Default)]
pub struct Collecting {
    /// Go `collecting.records`: sqlPlanDigest => record.
    pub records: HashMap<Vec<u8>, Record>,
    /// Go `collecting.evicted`: timestamp => { sqlPlanDigest }.
    pub evicted: HashMap<u64, HashSet<Vec<u8>>>,
}

impl Collecting {
    /// Go `newCollecting`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `collecting.getOrCreateRecord`.
    pub fn get_or_create_record(&mut self, sql_digest: &[u8], plan_digest: &[u8]) -> &mut Record {
        let key = encode_key(sql_digest, plan_digest);
        self.records
            .entry(key)
            .or_insert_with(|| Record::new(sql_digest.to_vec(), plan_digest.to_vec()))
    }

    /// Go `collecting.markAsEvicted`.
    pub fn mark_as_evicted(&mut self, timestamp: u64, sql_digest: &[u8], plan_digest: &[u8]) {
        self.evicted
            .entry(timestamp)
            .or_default()
            .insert(encode_key(sql_digest, plan_digest));
    }

    /// Go `collecting.hasEvicted`.
    #[must_use]
    pub fn has_evicted(&self, timestamp: u64, sql_digest: &[u8], plan_digest: &[u8]) -> bool {
        self.evicted
            .get(&timestamp)
            .is_some_and(|digests| digests.contains(&encode_key(sql_digest, plan_digest)))
    }

    /// Go `collecting.appendOthersCPUTime`.
    pub fn append_others_cpu_time(&mut self, timestamp: u64, total_cpu_time_ms: u32) {
        if total_cpu_time_ms == 0 {
            return;
        }
        self.records
            .entry(KEY_OTHERS.to_vec())
            .or_insert_with(|| Record::new(Vec::new(), Vec::new()))
            .append_cpu_time(timestamp, total_cpu_time_ms);
    }

    /// Go `collecting.appendOthersStmtStatsItem`.
    pub fn append_others_stmt_stats_item(&mut self, timestamp: u64, item: StatementStatsItem) {
        self.records
            .entry(KEY_OTHERS.to_vec())
            .or_insert_with(|| Record::new(Vec::new(), Vec::new()))
            .append_stmt_stats_item(timestamp, item);
    }

    /// Go `collecting.removeInvalidPlanRecord`: removes the `""` plan record
    /// when a SQL has exactly one other valid plan, folding its data into that
    /// plan. Called once at the end of a collection, from
    /// [`Collecting::get_report_records`].
    pub fn remove_invalid_plan_record(&mut self) {
        let mut sql_to_plans: HashMap<Vec<u8>, Vec<Vec<u8>>> =
            HashMap::with_capacity(self.records.len());
        for value in self.records.values() {
            sql_to_plans
                .entry(value.sql_digest.clone())
                .or_default()
                .push(value.plan_digest.clone());
        }
        for (sql_digest, plans) in sql_to_plans {
            if plans.len() != 2 {
                continue;
            }
            if !plans[0].is_empty() && !plans[1].is_empty() {
                continue;
            }
            let key0 = encode_key(&sql_digest, &plans[0]);
            let key1 = encode_key(&sql_digest, &plans[1]);
            if !self.records.contains_key(&key0) || !self.records.contains_key(&key1) {
                continue;
            }
            // The record with the empty plan is merged away; Go merges through
            // two map pointers, which here means taking one record out first.
            let (into_key, from_key) = if plans[0].is_empty() {
                (key1, key0)
            } else {
                (key0, key1)
            };
            let Some(mut from) = self.records.remove(&from_key) else {
                continue;
            };
            if let Some(into) = self.records.get_mut(&into_key) {
                into.merge(&mut from);
            }
        }
    }

    /// Go `collecting.getReportRecords`: all records, with the "others" record
    /// packed and appended at the end.
    pub fn get_report_records(&mut self) -> Records {
        let others = self.records.remove(KEY_OTHERS);

        self.remove_invalid_plan_record();

        let mut rs: Records = Vec::with_capacity(self.records.len());
        for value in self.records.values() {
            rs.push(value.clone());
        }
        if let Some(others) = others {
            rs.push(others);
        }
        rs
    }

    /// Go `collecting.take`: takes all data out, returning it in a new
    /// `collecting`.
    pub fn take(&mut self) -> Collecting {
        Collecting {
            records: std::mem::take(&mut self.records),
            evicted: std::mem::take(&mut self.evicted),
        }
    }
}

/// Go `sqlMeta`: a normalized SQL string plus a flag distinguishing internal
/// SQL.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SqlMeta {
    /// Go `sqlMeta.normalizedSQL`.
    pub normalized_sql: String,
    /// Go `sqlMeta.isInternal`.
    pub is_internal: bool,
}

/// Go `planMeta`: a binary normalized plan plus `isLarge`, which marks a plan
/// too large to decode quickly.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanMeta {
    /// Go `planMeta.binaryNormalizedPlan`.
    pub binary_normalized_plan: String,
    /// Go `planMeta.isLarge`.
    pub is_large: bool,
}

/// Go `planBinaryDecodeFunc`: decodes a binary normalized plan for the
/// protobuf conversion.
pub type PlanBinaryDecodeFunc<'a> = &'a dyn Fn(&str) -> Result<String, String>;

/// Go `planBinaryCompressFunc`: compresses a large normalized plan into its
/// encoded form.
pub type PlanBinaryCompressFunc<'a> = &'a dyn Fn(&[u8]) -> String;

/// Go `normalizedSQLMap`: a wrapped map used to register normalized SQL.
///
/// Go pairs an `atomic.Pointer[sync.Map]` with an atomic length so that
/// `take` can swap the whole map in one store; a `Mutex` around the map plus
/// the same atomic length gives the identical externally visible behavior,
/// with `take` swapping under the lock.
#[derive(Debug, Default)]
pub struct NormalizedSqlMap {
    data: Mutex<HashMap<Vec<u8>, SqlMeta>>,
    length: AtomicI64,
}

impl NormalizedSqlMap {
    /// Go `newNormalizedSQLMap`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go's `m.length.Load()`.
    #[must_use]
    pub fn len(&self) -> i64 {
        self.length.load(Ordering::SeqCst)
    }

    /// Whether no SQL meta is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go's `m.data.Load().Load(digest)`.
    #[must_use]
    pub fn get(&self, sql_digest: &[u8]) -> Option<SqlMeta> {
        self.data.lock().unwrap().get(sql_digest).cloned()
    }

    /// Go `normalizedSQLMap.register`: saves the sqlDigest => normalizedSQL
    /// relationship, discarding it once the map exceeds `MaxCollect`.
    pub fn register(&self, sql_digest: &[u8], normalized_sql: &str, is_internal: bool) {
        if self.length.load(Ordering::SeqCst)
            >= topsql_state::GLOBAL_STATE
                .max_collect
                .load(Ordering::SeqCst)
        {
            IGNORE_EXCEED_SQL_COUNTER.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut data = self.data.lock().unwrap();
        if !data.contains_key(sql_digest) {
            data.insert(
                sql_digest.to_vec(),
                SqlMeta {
                    normalized_sql: normalized_sql.to_owned(),
                    is_internal,
                },
            );
            self.length.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Go `normalizedSQLMap.take`.
    #[must_use]
    pub fn take(&self) -> NormalizedSqlMap {
        let mut data = self.data.lock().unwrap();
        let taken = std::mem::take(&mut *data);
        let length = self.length.swap(0, Ordering::SeqCst);
        NormalizedSqlMap {
            data: Mutex::new(taken),
            length: AtomicI64::new(length),
        }
    }

    /// Go `normalizedSQLMap.toProto`.
    #[must_use]
    pub fn to_proto(&self, keyspace_name: &[u8]) -> Vec<SqlMetaProto> {
        self.data
            .lock()
            .unwrap()
            .iter()
            .map(|(digest, meta)| SqlMetaProto {
                keyspace_name: keyspace_name.to_vec(),
                sql_digest: digest.clone(),
                normalized_sql: meta.normalized_sql.clone(),
                is_internal_sql: meta.is_internal,
            })
            .collect()
    }
}

/// Go `normalizedPlanMap`: a wrapped map used to register normalized plans.
#[derive(Debug, Default)]
pub struct NormalizedPlanMap {
    data: Mutex<HashMap<Vec<u8>, PlanMeta>>,
    length: AtomicI64,
}

impl NormalizedPlanMap {
    /// Go `newNormalizedPlanMap`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go's `m.length.Load()`.
    #[must_use]
    pub fn len(&self) -> i64 {
        self.length.load(Ordering::SeqCst)
    }

    /// Whether no plan meta is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Go's `m.data.Load().Load(digest)`.
    #[must_use]
    pub fn get(&self, plan_digest: &[u8]) -> Option<PlanMeta> {
        self.data.lock().unwrap().get(plan_digest).cloned()
    }

    /// Go `normalizedPlanMap.register`: saves the planDigest =>
    /// normalizedPlan relationship, discarding it once the map exceeds
    /// `MaxCollect`.
    pub fn register(&self, plan_digest: &[u8], normalized_plan: &str, is_large: bool) {
        if self.length.load(Ordering::SeqCst)
            >= topsql_state::GLOBAL_STATE
                .max_collect
                .load(Ordering::SeqCst)
        {
            IGNORE_EXCEED_PLAN_COUNTER.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut data = self.data.lock().unwrap();
        if !data.contains_key(plan_digest) {
            data.insert(
                plan_digest.to_vec(),
                PlanMeta {
                    binary_normalized_plan: normalized_plan.to_owned(),
                    is_large,
                },
            );
            self.length.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Go `normalizedPlanMap.take`.
    #[must_use]
    pub fn take(&self) -> NormalizedPlanMap {
        let mut data = self.data.lock().unwrap();
        let taken = std::mem::take(&mut *data);
        let length = self.length.swap(0, Ordering::SeqCst);
        NormalizedPlanMap {
            data: Mutex::new(taken),
            length: AtomicI64::new(length),
        }
    }

    /// Go `normalizedPlanMap.toProto`: large plans are compressed, the rest
    /// decoded; a plan whose decode fails is logged and skipped.
    #[must_use]
    pub fn to_proto(
        &self,
        keyspace_name: &[u8],
        decode_plan: PlanBinaryDecodeFunc<'_>,
        compress_plan: PlanBinaryCompressFunc<'_>,
    ) -> Vec<PlanMetaProto> {
        let data = self.data.lock().unwrap();
        let mut metas = Vec::with_capacity(data.len());
        for (digest, original_meta) in data.iter() {
            let mut proto_meta = PlanMetaProto {
                keyspace_name: keyspace_name.to_vec(),
                plan_digest: digest.clone(),
                ..PlanMetaProto::default()
            };
            if original_meta.is_large {
                proto_meta.encoded_normalized_plan =
                    compress_plan(original_meta.binary_normalized_plan.as_bytes());
            } else {
                match decode_plan(&original_meta.binary_normalized_plan) {
                    Ok(plan) => proto_meta.normalized_plan = plan,
                    Err(error) => {
                        tracing::warn!(category = "top-sql", %error, "decode plan failed");
                        continue;
                    }
                }
            }
            metas.push(proto_meta);
        }
        metas
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topsql_stmtstats::KvStatementStatsItem;

    /// `topsqlstate.GlobalState.MaxCollect` is process-global, and Go's test
    /// binary runs these cases sequentially; Rust's runs them in parallel, so
    /// the cases that set it are serialized here.
    static MAX_COLLECT_GUARD: Mutex<()> = Mutex::new(());

    fn kv_exec_count(pairs: &[(&str, u64)]) -> KvStatementStatsItem {
        let mut item = KvStatementStatsItem::new();
        for (target, count) in pairs {
            item.kv_exec_count.insert((*target).to_owned(), *count);
        }
        item
    }

    #[test]
    fn test_ts_item_to_proto() {
        let item = TsItem {
            timestamp: 1,
            cpu_time_ms: 2,
            stmt_stats: StatementStatsItem {
                exec_count: 3,
                sum_duration_ns: 50000,
                duration_count: 2,
                kv_stats_item: kv_exec_count(&[("", 4)]),
                ..StatementStatsItem::default()
            },
        };
        let pb = item.to_proto();
        assert_eq!(1, pb.timestamp_sec);
        assert_eq!(2, pb.cpu_time_ms);
        assert_eq!(3, pb.stmt_exec_count);
        assert_eq!(50000, pb.stmt_duration_sum_ns);
        assert_eq!(2, pb.stmt_duration_count);
        assert_eq!(4, pb.stmt_kv_exec_count[""]);
    }

    #[test]
    fn test_ts_items_sort() {
        let mut items: TsItems = Vec::new();
        assert!(ts_items_sorted(&items));
        items = vec![
            TsItem {
                timestamp: 2,
                ..TsItem::default()
            },
            TsItem {
                timestamp: 3,
                ..TsItem::default()
            },
            TsItem {
                timestamp: 1,
                ..TsItem::default()
            },
        ];
        assert!(!ts_items_sorted(&items));
        items.sort_by_key(|item| item.timestamp);
        assert!(ts_items_sorted(&items));
        assert_eq!(1, items[0].timestamp);
        assert_eq!(2, items[1].timestamp);
        assert_eq!(3, items[2].timestamp);
    }

    #[test]
    fn test_ts_items_to_proto() {
        let items: TsItems = vec![TsItem::default(), TsItem::default(), TsItem::default()];
        assert_eq!(3, ts_items_to_proto(&items).len());
    }

    #[test]
    fn test_record_sort() {
        let mut r = Record {
            ts_items: vec![
                TsItem {
                    timestamp: 2,
                    ..TsItem::default()
                },
                TsItem {
                    timestamp: 3,
                    ..TsItem::default()
                },
                TsItem {
                    timestamp: 1,
                    ..TsItem::default()
                },
            ],
            ts_index: HashMap::from([(2, 0), (3, 1), (1, 2)]),
            ..Record::default()
        };
        r.sort_by_timestamp();
        assert_eq!(1, r.ts_items[0].timestamp);
        assert_eq!(2, r.ts_items[1].timestamp);
        assert_eq!(3, r.ts_items[2].timestamp);
        assert_eq!(0, r.ts_index[&1]);
        assert_eq!(1, r.ts_index[&2]);
        assert_eq!(2, r.ts_index[&3]);
    }

    #[test]
    fn test_record_append() {
        let mut r = Record::new(Vec::new(), Vec::new());
        r.append_cpu_time(1, 1);
        r.append_stmt_stats_item(
            1,
            StatementStatsItem {
                exec_count: 1,
                sum_duration_ns: 10000,
                ..StatementStatsItem::default()
            },
        );
        r.append_cpu_time(2, 1);
        r.append_cpu_time(3, 1);
        r.append_stmt_stats_item(
            3,
            StatementStatsItem {
                exec_count: 1,
                sum_duration_ns: 30000,
                ..StatementStatsItem::default()
            },
        );
        r.append_stmt_stats_item(
            2,
            StatementStatsItem {
                exec_count: 1,
                sum_duration_ns: 20000,
                ..StatementStatsItem::default()
            },
        );

        assert_eq!(3, r.ts_items.len());
        assert_eq!(3, r.ts_index.len());
        assert_eq!(3, r.total_cpu_time_ms);
        assert_eq!(1, r.ts_items[0].timestamp);
        assert_eq!(2, r.ts_items[1].timestamp);
        assert_eq!(3, r.ts_items[2].timestamp);
        assert_eq!(1, r.ts_items[0].cpu_time_ms);
        assert_eq!(1, r.ts_items[1].cpu_time_ms);
        assert_eq!(1, r.ts_items[2].cpu_time_ms);
        assert_eq!(1, r.ts_items[0].stmt_stats.exec_count);
        assert_eq!(1, r.ts_items[1].stmt_stats.exec_count);
        assert_eq!(1, r.ts_items[2].stmt_stats.exec_count);
        assert_eq!(10000, r.ts_items[0].stmt_stats.sum_duration_ns);
        assert_eq!(20000, r.ts_items[1].stmt_stats.sum_duration_ns);
        assert_eq!(30000, r.ts_items[2].stmt_stats.sum_duration_ns);
    }

    #[test]
    fn test_record_merge() {
        let item = |timestamp: u64, cpu: u32| TsItem {
            timestamp,
            cpu_time_ms: cpu,
            stmt_stats: StatementStatsItem::new(),
        };
        let mut r1 = Record {
            total_cpu_time_ms: 1 + 2 + 3,
            ts_items: vec![item(1, 1), item(2, 2), item(3, 3)],
            ..Record::default()
        };
        r1.rebuild_ts_index();
        let mut r2 = Record {
            total_cpu_time_ms: 6 + 5 + 4,
            ts_items: vec![item(6, 6), item(5, 5), item(4, 4)],
            ..Record::default()
        };
        r2.rebuild_ts_index();
        r1.merge(&mut r2);
        assert_eq!(4, r2.ts_items[0].timestamp);
        assert_eq!(5, r2.ts_items[1].timestamp);
        assert_eq!(6, r2.ts_items[2].timestamp);
        assert_eq!(6, r1.ts_items.len());
        assert_eq!(6, r1.ts_index.len());
        for (index, expected) in [1u64, 2, 3, 4, 5, 6].iter().enumerate() {
            assert_eq!(*expected, r1.ts_items[index].timestamp);
        }
        assert_eq!(1 + 2 + 3 + 4 + 5 + 6, r1.total_cpu_time_ms);
    }

    #[test]
    fn test_record_rebuild_ts_index() {
        let mut r = Record {
            ts_index: HashMap::from([(1, 1)]),
            ..Record::default()
        };
        r.rebuild_ts_index();
        assert!(r.ts_index.is_empty());
        r.ts_items = vec![
            TsItem {
                timestamp: 1,
                cpu_time_ms: 1,
                ..TsItem::default()
            },
            TsItem {
                timestamp: 2,
                cpu_time_ms: 2,
                ..TsItem::default()
            },
            TsItem {
                timestamp: 3,
                cpu_time_ms: 3,
                ..TsItem::default()
            },
        ];
        r.rebuild_ts_index();
        assert_eq!(3, r.ts_index.len());
        assert_eq!(0, r.ts_index[&1]);
        assert_eq!(1, r.ts_index[&2]);
        assert_eq!(2, r.ts_index[&3]);
    }

    #[test]
    fn test_record_to_proto() {
        let r = Record {
            sql_digest: b"SQL-1".to_vec(),
            plan_digest: b"PLAN-1".to_vec(),
            total_cpu_time_ms: 123,
            ts_items: vec![TsItem::default(), TsItem::default(), TsItem::default()],
            ..Record::default()
        };
        let name = b"123";
        let pb = r.to_proto(name);
        assert_eq!(name, pb.get_keyspace_name());
        assert_eq!(b"SQL-1".to_vec(), pb.sql_digest);
        assert_eq!(b"PLAN-1".to_vec(), pb.plan_digest);
        assert_eq!(3, pb.items.len());
    }

    fn cpu_record(total_cpu_time_ms: u64) -> Record {
        Record {
            total_cpu_time_ms,
            ..Record::default()
        }
    }

    #[test]
    fn test_records_sort() {
        let mut rs: Records = vec![cpu_record(1), cpu_record(3), cpu_record(2)];
        sort_records(&mut rs);
        assert_eq!(3, rs[0].total_cpu_time_ms);
        assert_eq!(2, rs[1].total_cpu_time_ms);
        assert_eq!(1, rs[2].total_cpu_time_ms);
    }

    #[test]
    fn test_records_top_n() {
        let rs: Records = vec![cpu_record(1), cpu_record(3), cpu_record(2)];
        let (top, evicted) = records_top_n(rs, 2);
        assert_eq!(2, top.len());
        assert_eq!(1, evicted.len());
        assert_eq!(3, top[0].total_cpu_time_ms);
        assert_eq!(2, top[1].total_cpu_time_ms);
        assert_eq!(1, evicted[0].total_cpu_time_ms);
    }

    #[test]
    fn test_records_to_proto() {
        let rs: Records = vec![Record::default(), Record::default()];
        assert_eq!(2, records_to_proto(&rs, b"").len());
    }

    #[test]
    fn test_collecting_get_or_create_record() {
        let mut c = Collecting::new();
        let r1 = c.get_or_create_record(b"SQL-1", b"PLAN-1").clone();
        let r2 = c.get_or_create_record(b"SQL-1", b"PLAN-1").clone();
        assert_eq!(r1, r2);
        assert_eq!(1, c.records.len());
    }

    #[test]
    fn test_collecting_mark_as_evicted_has_evicted() {
        let mut c = Collecting::new();
        c.mark_as_evicted(1, b"SQL-1", b"PLAN-1");
        assert!(c.has_evicted(1, b"SQL-1", b"PLAN-1"));
        assert!(!c.has_evicted(1, b"SQL-2", b"PLAN-2"));
        assert!(!c.has_evicted(2, b"SQL-1", b"PLAN-1"));
    }

    #[test]
    fn test_collecting_append_others() {
        let mut c = Collecting::new();
        c.append_others_cpu_time(1, 1);
        c.append_others_cpu_time(2, 2);
        c.append_others_stmt_stats_item(
            1,
            StatementStatsItem {
                exec_count: 1,
                sum_duration_ns: 1000,
                ..StatementStatsItem::default()
            },
        );
        c.append_others_stmt_stats_item(
            2,
            StatementStatsItem {
                exec_count: 2,
                sum_duration_ns: 2000,
                ..StatementStatsItem::default()
            },
        );
        let r = &c.records[KEY_OTHERS];
        assert_eq!(2, r.ts_items.len());
        assert_eq!(2, r.ts_index.len());
        assert_eq!(1, r.ts_items[0].timestamp);
        assert_eq!(2, r.ts_items[1].timestamp);
        assert_eq!(1, r.ts_items[0].cpu_time_ms);
        assert_eq!(2, r.ts_items[1].cpu_time_ms);
        assert_eq!(1, r.ts_items[0].stmt_stats.exec_count);
        assert_eq!(2, r.ts_items[1].stmt_stats.exec_count);
        assert_eq!(1000, r.ts_items[0].stmt_stats.sum_duration_ns);
        assert_eq!(2000, r.ts_items[1].stmt_stats.sum_duration_ns);
    }

    #[test]
    fn test_collecting_get_report_records() {
        let mut c = Collecting::new();
        c.get_or_create_record(b"SQL-1", b"PLAN-1")
            .append_cpu_time(1, 1);
        c.get_or_create_record(b"SQL-2", b"PLAN-2")
            .append_cpu_time(1, 2);
        c.get_or_create_record(b"SQL-3", b"PLAN-3")
            .append_cpu_time(1, 3);
        c.get_or_create_record(KEY_OTHERS, KEY_OTHERS)
            .append_cpu_time(1, 10);
        let rs = c.get_report_records();
        assert_eq!(4, rs.len());
        assert_eq!(10, rs[3].ts_items[0].cpu_time_ms);
        assert_eq!(10, rs[3].total_cpu_time_ms);
    }

    #[test]
    fn test_collecting_take() {
        // Go additionally asserts the two `keyBuf` buffers differ; the buffer
        // is a Go-only allocation reuse that this port drops, so that
        // assertion has no counterpart.
        let mut c1 = Collecting::new();
        c1.get_or_create_record(b"SQL-1", b"PLAN-1")
            .append_cpu_time(1, 1);
        let c2 = c1.take();
        assert!(c1.records.is_empty());
        assert_eq!(1, c2.records.len());
    }

    fn cpu_time_record(cpu_time_ms: u32) -> SqlCpuTimeRecord {
        SqlCpuTimeRecord {
            cpu_time_ms,
            ..SqlCpuTimeRecord::default()
        }
    }

    #[test]
    fn test_cpu_records_sort() {
        let mut rs: CpuRecords = vec![cpu_time_record(1), cpu_time_record(3), cpu_time_record(2)];
        sort_cpu_records(&mut rs);
        assert_eq!(3, rs[0].cpu_time_ms);
        assert_eq!(2, rs[1].cpu_time_ms);
        assert_eq!(1, rs[2].cpu_time_ms);
    }

    #[test]
    fn test_cpu_records_top_n() {
        let rs: CpuRecords = vec![cpu_time_record(1), cpu_time_record(3), cpu_time_record(2)];
        let (top, evicted) = cpu_records_top_n(rs, 2);
        assert_eq!(2, top.len());
        assert_eq!(1, evicted.len());
        assert_eq!(3, top[0].cpu_time_ms);
        assert_eq!(2, top[1].cpu_time_ms);
        assert_eq!(1, evicted[0].cpu_time_ms);
    }

    #[test]
    fn test_normalized_sql_map_register() {
        let _guard = MAX_COLLECT_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        topsql_state::GLOBAL_STATE
            .max_collect
            .store(2, Ordering::SeqCst);
        let m = NormalizedSqlMap::new();
        m.register(b"SQL-1", "SQL-1", true);
        m.register(b"SQL-2", "SQL-2", false);
        m.register(b"SQL-3", "SQL-3", true);
        assert_eq!(2, m.len());
        let meta = m.get(b"SQL-1").unwrap();
        assert_eq!("SQL-1", meta.normalized_sql);
        assert!(meta.is_internal);
        let meta = m.get(b"SQL-2").unwrap();
        assert_eq!("SQL-2", meta.normalized_sql);
        assert!(!meta.is_internal);
        assert!(m.get(b"SQL-3").is_none());
    }

    #[test]
    fn test_normalized_sql_map_take() {
        let _guard = MAX_COLLECT_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        topsql_state::GLOBAL_STATE
            .max_collect
            .store(999, Ordering::SeqCst);
        let m1 = NormalizedSqlMap::new();
        m1.register(b"SQL-1", "SQL-1", true);
        m1.register(b"SQL-2", "SQL-2", false);
        m1.register(b"SQL-3", "SQL-3", true);
        let m2 = m1.take();
        assert_eq!(0, m1.len());
        assert_eq!(3, m2.len());
        for digest in [b"SQL-1".as_slice(), b"SQL-2", b"SQL-3"] {
            assert!(m1.get(digest).is_none());
            assert!(m2.get(digest).is_some());
        }
    }

    #[test]
    fn test_normalized_sql_map_to_proto() {
        let _guard = MAX_COLLECT_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        topsql_state::GLOBAL_STATE
            .max_collect
            .store(999, Ordering::SeqCst);
        let m = NormalizedSqlMap::new();
        m.register(b"SQL-1", "SQL-1", true);
        m.register(b"SQL-2", "SQL-2", false);
        m.register(b"SQL-3", "SQL-3", true);
        let name = b"12345";
        let pb = m.to_proto(name);
        assert_eq!(3, pb.len());
        let hash: HashMap<String, SqlMetaProto> = pb
            .into_iter()
            .map(|meta| (meta.normalized_sql.clone(), meta))
            .collect();
        assert_eq!(
            SqlMetaProto {
                keyspace_name: name.to_vec(),
                sql_digest: b"SQL-1".to_vec(),
                normalized_sql: "SQL-1".to_owned(),
                is_internal_sql: true,
            },
            hash["SQL-1"]
        );
        assert_eq!(
            SqlMetaProto {
                keyspace_name: name.to_vec(),
                sql_digest: b"SQL-2".to_vec(),
                normalized_sql: "SQL-2".to_owned(),
                is_internal_sql: false,
            },
            hash["SQL-2"]
        );
        assert_eq!(
            SqlMetaProto {
                keyspace_name: name.to_vec(),
                sql_digest: b"SQL-3".to_vec(),
                normalized_sql: "SQL-3".to_owned(),
                is_internal_sql: true,
            },
            hash["SQL-3"]
        );
    }

    #[test]
    fn test_normalized_plan_map_register() {
        let _guard = MAX_COLLECT_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        topsql_state::GLOBAL_STATE
            .max_collect
            .store(2, Ordering::SeqCst);
        let m = NormalizedPlanMap::new();
        m.register(b"PLAN-1", "PLAN-1", false);
        m.register(b"PLAN-2", "PLAN-2", true);
        m.register(b"PLAN-3", "PLAN-3", false);
        assert_eq!(2, m.len());
        assert_eq!(
            Some(PlanMeta {
                binary_normalized_plan: "PLAN-1".to_owned(),
                is_large: false,
            }),
            m.get(b"PLAN-1")
        );
        assert_eq!(
            Some(PlanMeta {
                binary_normalized_plan: "PLAN-2".to_owned(),
                is_large: true,
            }),
            m.get(b"PLAN-2")
        );
        assert!(m.get(b"PLAN-3").is_none());
    }

    #[test]
    fn test_normalized_plan_map_take() {
        let _guard = MAX_COLLECT_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        topsql_state::GLOBAL_STATE
            .max_collect
            .store(999, Ordering::SeqCst);
        let m1 = NormalizedPlanMap::new();
        m1.register(b"PLAN-1", "PLAN-1", false);
        m1.register(b"PLAN-2", "PLAN-2", false);
        m1.register(b"PLAN-3", "PLAN-3", false);
        let m2 = m1.take();
        assert_eq!(0, m1.len());
        assert_eq!(3, m2.len());
        for digest in [b"PLAN-1".as_slice(), b"PLAN-2", b"PLAN-3"] {
            assert!(m1.get(digest).is_none());
            assert!(m2.get(digest).is_some());
        }
    }

    #[test]
    fn test_normalized_plan_map_to_proto() {
        let _guard = MAX_COLLECT_GUARD.lock().unwrap_or_else(|e| e.into_inner());
        topsql_state::GLOBAL_STATE
            .max_collect
            .store(999, Ordering::SeqCst);
        let m = NormalizedPlanMap::new();
        m.register(b"PLAN-1", "PLAN-1", false);
        m.register(b"PLAN-2", "PLAN-2", true);
        m.register(b"PLAN-3", "PLAN-3", false);
        let name = b"12345";
        let pb = m.to_proto(
            name,
            &|plan: &str| Ok(format!("[decoded] {plan}")),
            &|plan: &[u8]| format!("[encoded] {}", String::from_utf8_lossy(plan)),
        );
        assert_eq!(3, pb.len());
        let hash: HashMap<Vec<u8>, PlanMetaProto> = pb
            .into_iter()
            .map(|meta| (meta.plan_digest.clone(), meta))
            .collect();
        assert_eq!(
            PlanMetaProto {
                keyspace_name: name.to_vec(),
                plan_digest: b"PLAN-1".to_vec(),
                normalized_plan: "[decoded] PLAN-1".to_owned(),
                encoded_normalized_plan: String::new(),
            },
            hash[b"PLAN-1".as_slice()]
        );
        assert_eq!(
            PlanMetaProto {
                keyspace_name: name.to_vec(),
                plan_digest: b"PLAN-2".to_vec(),
                normalized_plan: String::new(),
                encoded_normalized_plan: "[encoded] PLAN-2".to_owned(),
            },
            hash[b"PLAN-2".as_slice()]
        );
        assert_eq!(
            PlanMetaProto {
                keyspace_name: name.to_vec(),
                plan_digest: b"PLAN-3".to_vec(),
                normalized_plan: "[decoded] PLAN-3".to_owned(),
                encoded_normalized_plan: String::new(),
            },
            hash[b"PLAN-3".as_slice()]
        );
    }

    #[test]
    fn test_encode_key() {
        assert_eq!(b"SP".to_vec(), encode_key(b"S", b"P"));
    }

    #[test]
    fn test_remove_invalid_plan_record() {
        let mut c1 = Collecting::new();
        let rs: &[(&[u8], &[u8], &[u64])] = &[
            (b"SQL-1", b"PLAN-1", &[1, 2, 3, 5]),
            (b"SQL-1", b"PLAN-2", &[1, 2, 5, 6]),
            (b"SQL-2", b"PLAN-1", &[1, 2, 3, 5]),
            (b"SQL-2", b"", &[1, 2, 3, 4, 6]),
            (b"SQL-3", b"", &[2, 3, 5]),
            (b"SQL-3", b"PLAN-1", &[1, 2, 3, 4, 6]),
        ];
        for (sql, plan, timestamps) in rs {
            let record = c1.get_or_create_record(sql, plan);
            for timestamp in *timestamps {
                record.append_cpu_time(*timestamp, 1);
            }
        }

        c1.remove_invalid_plan_record();

        type ExpectedRecord<'a> = (&'a [u8], &'a [u8], &'a [u64], &'a [u32]);
        let result: &[ExpectedRecord<'_>] = &[
            (b"SQL-1", b"PLAN-1", &[1, 2, 3, 5], &[1, 1, 1, 1]),
            (b"SQL-1", b"PLAN-2", &[1, 2, 5, 6], &[1, 1, 1, 1]),
            (
                b"SQL-2",
                b"PLAN-1",
                &[1, 2, 3, 4, 5, 6],
                &[2, 2, 2, 1, 1, 1],
            ),
            (
                b"SQL-3",
                b"PLAN-1",
                &[1, 2, 3, 4, 5, 6],
                &[1, 2, 2, 1, 1, 1],
            ),
        ];
        assert_eq!(result.len(), c1.records.len());
        for (sql, plan, timestamps, cpus) in result {
            let key = encode_key(sql, plan);
            let record = c1.records.get(&key).expect("record must exist");
            assert_eq!(sql.to_vec(), record.sql_digest);
            assert_eq!(plan.to_vec(), record.plan_digest);
            assert_eq!(timestamps.len(), record.ts_items.len());
            for (i, timestamp) in timestamps.iter().enumerate() {
                assert_eq!(*timestamp, record.ts_items[i].timestamp);
                assert_eq!(cpus[i], record.ts_items[i].cpu_time_ms);
            }
        }
    }
}
