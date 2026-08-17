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

//! Complete transcreation of Go
//! `pkg/util/topsql/reporter/ru_window_aggregator.go`: the online 15s bucket
//! ring behind Top-RU reporting, with all test functions of
//! `ru_window_aggregator_test.go` and the data-model half of the generated
//! golden-case harness (`topru_case_runner_test.go` +
//! `topru_generated_cases_test.go`).
//!
//! RU batches land in 15-second buckets, every point in a bucket collapsing to
//! the bucket's start timestamp. A bucket is *rotated* — compacted to
//! `MAX_TOP_USERS` x `MAX_TOP_SQLS_PER_USER` and closed to further writes — as
//! soon as a write or a report crosses its end. One aligned, closed 60-second
//! window is reported at most once; at report time its buckets are regrouped
//! into item intervals of 15, 30, or 60 seconds, each interval compacted to
//! [`RU_REPORT_TOP_N_USERS`] x [`RU_REPORT_TOP_N_SQLS_PER_USER`].
//!
//! Late data — a batch whose bucket falls inside an already reported window —
//! is shifted forward to the earliest still-open window rather than dropped.
//! That is a *best-effort* contract: if the remapped bucket has itself already
//! been compacted (a concurrent report got there first), the batch is dropped
//! and the loss is counted in
//! [`IGNORE_LATE_COMPACTED_RU_KEYS_COUNTER`] /
//! [`IGNORE_LATE_COMPACTED_RU_TOTAL_COUNTER`].

use std::collections::HashMap;
use std::sync::Mutex;

use super::ru_datamodel::{
    RuCollecting, TopRuRecord, MAX_PRE_TOP_N_SQLS_PER_USER, MAX_PRE_TOP_N_USERS,
    MAX_TOP_SQLS_PER_USER, MAX_TOP_USERS,
};
use crate::topsql_stmtstats::{normalize_ru_version, RuIncrementMap, RuVersion};

/// Go `ruBaseBucketSeconds`.
pub const RU_BASE_BUCKET_SECONDS: u64 = 15;

/// Go `ruReportWindowSeconds`.
pub const RU_REPORT_WINDOW_SECONDS: u64 = 60;

/// Go `ruReportTopNUsers`: the per-item-interval output cap on users.
///
/// Each 15s/30s slice is compacted to at most
/// `ruReportTopNUsers x ruReportTopNSQLsPerUser`. One 60s report may contain
/// several such slices (4 for 15s, 2 for 30s), so the total user count can
/// exceed 100.
pub const RU_REPORT_TOP_N_USERS: usize = 100;

/// Go `ruReportTopNSQLsPerUser`: the per-item-interval output cap on SQLs per
/// user.
pub const RU_REPORT_TOP_N_SQLS_PER_USER: usize = 100;

/// boundary: Go `reporter/metrics` prometheus `Counter`s are float counters
/// read back by the tests through `readCounter`. `tidb-util` carries no
/// reporter metric registry, so the two late-drop counters keep their names
/// and their float semantics as process counters.
#[derive(Debug)]
pub struct FloatCounter(Mutex<f64>);

impl FloatCounter {
    /// A counter starting at zero.
    #[must_use]
    pub const fn new() -> Self {
        Self(Mutex::new(0.0))
    }

    /// Prometheus `Counter.Add`.
    pub fn add(&self, delta: f64) {
        *self.0.lock().unwrap() += delta;
    }

    /// The test-side `readCounter`.
    #[must_use]
    pub fn get(&self) -> f64 {
        *self.0.lock().unwrap()
    }
}

impl Default for FloatCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// boundary: Go `reporter/metrics.IgnoreLateCompactedRUKeysCounter`.
pub static IGNORE_LATE_COMPACTED_RU_KEYS_COUNTER: FloatCounter = FloatCounter::new();

/// boundary: Go `reporter/metrics.IgnoreLateCompactedRUTotalCounter`.
pub static IGNORE_LATE_COMPACTED_RU_TOTAL_COUNTER: FloatCounter = FloatCounter::new();

/// boundary: Go `ruBatch` lives in `reporter.go`, which is gRPC-bound and out
/// of scope here; the three fields the window aggregator reads are declared
/// locally. The `version` field is `rmclient.RUVersion` in Go, already
/// narrowed to [`RuVersion`] by `topsql_stmtstats` — a plain integer enum, not
/// a PD client dependency.
#[derive(Clone, Debug, Default)]
pub struct RuBatch {
    /// Go `ruBatch.data`.
    pub data: RuIncrementMap,
    /// Go `ruBatch.timestamp`: stamped at enqueue time so bucket attribution
    /// does not depend on downstream scheduling delay.
    pub timestamp: u64,
    /// Go `ruBatch.version`.
    pub version: RuVersion,
}

/// Go `ruPointBucket`.
#[derive(Debug, Default)]
pub struct RuPointBucket {
    /// Go `ruPointBucket.collecting`: `Some` while actively collecting, `None`
    /// once compacted.
    pub collecting: Option<RuCollecting>,
    /// Go `ruPointBucket.compactedCollecting`: a read-only snapshot, valid
    /// only while `collecting` is `None`.
    pub compacted_collecting: Option<RuCollecting>,
    /// Go `ruPointBucket.startTs`.
    pub start_ts: u64,
}

/// Go `alignToInterval`.
#[must_use]
pub fn align_to_interval(ts: u64, interval: u64) -> u64 {
    if interval == 0 {
        return ts;
    }
    ts - ts % interval
}

/// The state Go guards with `ruWindowAggregator.mu`.
#[derive(Debug, Default)]
struct AggregatorState {
    /// Go `buckets`: 15s startTs -> bucket.
    buckets: HashMap<u64, RuPointBucket>,
    /// Go `currentVersion`.
    current_version: RuVersion,
    /// Go `dropUntilTs`.
    drop_until_ts: u64,
    /// Go `lastReportedEndTs`.
    last_reported_end_ts: u64,
}

/// Go `ruWindowAggregator`: keeps the online 15s buckets for Top-RU
/// reporting.
///
/// Go's `mu sync.Mutex` plus bare fields become one `Mutex` over the whole
/// state, so every method takes `&self` exactly as the Go pointer receivers
/// do and the aggregator can be shared across threads.
#[derive(Debug, Default)]
pub struct RuWindowAggregator {
    state: Mutex<AggregatorState>,
}

impl RuWindowAggregator {
    /// Go `newRUWindowAggregator`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `ruWindowAggregator.addBatch`.
    pub fn add_batch(&self, batch: &RuBatch) {
        if batch.data.is_empty() {
            return;
        }
        let mut bucket_start = align_to_interval(batch.timestamp, RU_BASE_BUCKET_SECONDS);

        let mut state = self.state.lock().unwrap();

        if state.current_version == RuVersion::UNSPECIFIED {
            // The first accepted batch establishes the reporter-side initial
            // RU version.
            state.current_version = normalize_ru_version(batch.version);
        }
        if batch.version != state.current_version
            || (state.drop_until_ts > 0 && bucket_start < state.drop_until_ts)
        {
            return;
        }

        // Best-effort contract: late batches are shifted to the earliest
        // still-open report window when possible. Under concurrent reporting
        // they may still be dropped if the remapped bucket has already been
        // compacted, and that path is tracked via dedicated metrics.
        let mut was_late_batch = false;
        if state.last_reported_end_ts > 0 && bucket_start < state.last_reported_end_ts {
            bucket_start = state.last_reported_end_ts;
            was_late_batch = true;
        }

        Self::rotate_buckets_before(&mut state, bucket_start);

        let bucket = state
            .buckets
            .entry(bucket_start)
            .or_insert_with(|| RuPointBucket {
                start_ts: bucket_start,
                collecting: Some(RuCollecting::with_caps(
                    MAX_PRE_TOP_N_USERS,
                    MAX_PRE_TOP_N_SQLS_PER_USER,
                )),
                compacted_collecting: None,
            });
        let Some(collecting) = bucket.collecting.as_mut() else {
            // Best-effort contract: a remapped late batch may still hit an
            // already compacted bucket under concurrent reporting. Keep this
            // observable.
            if was_late_batch {
                let dropped_ru: f64 = batch.data.values().map(|incr| incr.total_ru).sum();
                IGNORE_LATE_COMPACTED_RU_KEYS_COUNTER.add(batch.data.len() as f64);
                IGNORE_LATE_COMPACTED_RU_TOTAL_COUNTER.add(dropped_ru);
            }
            return;
        };

        // Collapse all points in this 15s bucket to the bucket start
        // timestamp.
        collecting.add_batch(bucket_start, &batch.data);
    }

    /// Go `ruWindowAggregator.resetForHandover`.
    pub fn reset_for_handover(&self, version: RuVersion, now_ts: u64) {
        let mut state = self.state.lock().unwrap();
        state.current_version = version;
        state.buckets = HashMap::new();
        state.drop_until_ts = align_to_interval(now_ts, RU_REPORT_WINDOW_SECONDS);
        if !now_ts.is_multiple_of(RU_REPORT_WINDOW_SECONDS) {
            state.drop_until_ts += RU_REPORT_WINDOW_SECONDS;
        }
    }

    /// Go `ruWindowAggregator.takeReportRecords`: emits one aligned closed 60s
    /// window for `now_ts`, dropping older windows when called late.
    /// `item_interval` must be 15, 30, or 60.
    ///
    /// Go's `nil` slice — window not ready, already reported, or no data — is
    /// `None` here; a reported window with data is `Some`.
    #[must_use]
    pub fn take_report_records(
        &self,
        now_ts: u64,
        item_interval: u64,
        keyspace_name: &[u8],
    ) -> Option<Vec<TopRuRecord>> {
        let window_end = align_to_interval(now_ts, RU_REPORT_WINDOW_SECONDS);
        if window_end < RU_REPORT_WINDOW_SECONDS {
            return None;
        }

        // Step 1: take buckets under lock.
        let taken_buckets = self.take_buckets_for_window(window_end)?;

        // Step 2: build report records outside the lock.
        let window_start = window_end - RU_REPORT_WINDOW_SECONDS;
        let records = build_report_records(
            &taken_buckets,
            window_start,
            window_end,
            item_interval,
            keyspace_name,
        );
        if records.is_empty() {
            return None;
        }
        Some(records)
    }

    /// Go `ruWindowAggregator.takeBucketsForWindow`: extracts the buckets of a
    /// window under lock, or `None` when the window has already been reported.
    fn take_buckets_for_window(&self, window_end: u64) -> Option<HashMap<u64, RuPointBucket>> {
        let mut state = self.state.lock().unwrap();

        if window_end <= state.last_reported_end_ts {
            return None;
        }

        // Rotate buckets that are no longer writable for this report-window
        // boundary.
        Self::rotate_buckets_before(&mut state, window_end);

        let window_start = window_end - RU_REPORT_WINDOW_SECONDS;

        let mut taken_buckets =
            HashMap::with_capacity((RU_REPORT_WINDOW_SECONDS / RU_BASE_BUCKET_SECONDS) as usize);
        let mut ts = window_start;
        while ts < window_end {
            if let Some(bucket) = state.buckets.remove(&ts) {
                taken_buckets.insert(ts, bucket);
            }
            ts += RU_BASE_BUCKET_SECONDS;
        }
        state.last_reported_end_ts = window_end;

        // Clean stale buckets.
        state.buckets.retain(|ts, _| *ts >= window_start);

        Some(taken_buckets)
    }

    /// Go `ruWindowAggregator.rotateBucketsBefore`.
    fn rotate_buckets_before(state: &mut AggregatorState, boundary_start: u64) {
        for bucket in state.buckets.values_mut() {
            if bucket.collecting.is_none() {
                continue;
            }
            if bucket.start_ts + RU_BASE_BUCKET_SECONDS <= boundary_start {
                // Compact to an internal snapshot.
                let collecting = bucket.collecting.take().expect("checked above");
                bucket.compacted_collecting =
                    collecting.compact_with_limits(MAX_TOP_USERS, MAX_TOP_SQLS_PER_USER);
            }
        }
    }
}

/// Go `buildReportRecords`: merges the taken buckets and produces the final
/// proto records. Requires no lock.
#[must_use]
pub fn build_report_records(
    buckets: &HashMap<u64, RuPointBucket>,
    window_start: u64,
    window_end: u64,
    item_interval: u64,
    keyspace_name: &[u8],
) -> Vec<TopRuRecord> {
    let single_interval = window_end - window_start <= item_interval;

    let buckets_per_interval = item_interval.div_ceil(RU_BASE_BUCKET_SECONDS) as usize;
    let interval_pre_cap_users = buckets_per_interval * MAX_TOP_USERS;
    let interval_pre_cap_sqls_per_user = buckets_per_interval * MAX_TOP_SQLS_PER_USER;

    let intervals_per_window = (window_end - window_start).div_ceil(item_interval) as usize;
    let merged_pre_cap_users = intervals_per_window * RU_REPORT_TOP_N_USERS;
    let merged_pre_cap_sqls_per_user = intervals_per_window * RU_REPORT_TOP_N_SQLS_PER_USER;
    let mut merged_output = if single_interval {
        None
    } else {
        Some(RuCollecting::with_caps(
            merged_pre_cap_users,
            merged_pre_cap_sqls_per_user,
        ))
    };

    let mut interval_start = window_start;
    while interval_start < window_end {
        let mut interval_collecting =
            RuCollecting::with_caps(interval_pre_cap_users, interval_pre_cap_sqls_per_user);
        let mut bucket_start = interval_start;
        while bucket_start < interval_start + item_interval {
            if let Some(compacted) = buckets
                .get(&bucket_start)
                .and_then(|bucket| bucket.compacted_collecting.as_ref())
            {
                // Merge the internal structure directly.
                interval_collecting.merge_from(compacted, interval_start, true);
            }
            bucket_start += RU_BASE_BUCKET_SECONDS;
        }
        // Apply Top-N and merge into the output.
        let interval_compacted = interval_collecting
            .compact_with_limits(RU_REPORT_TOP_N_USERS, RU_REPORT_TOP_N_SQLS_PER_USER);
        if single_interval {
            return match interval_compacted {
                None => Vec::new(),
                Some(compacted) => compacted.to_top_ru_records(keyspace_name),
            };
        }
        if let Some(compacted) = interval_compacted {
            if let Some(merged) = merged_output.as_mut() {
                merged.merge_from(&compacted, 0, false);
            }
        }
        interval_start += item_interval;
    }
    // Convert to proto at output.
    merged_output
        .map(|merged| merged.to_top_ru_records(keyspace_name))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topsql_reporter::datamodel::{
        NormalizedPlanMap, NormalizedSqlMap, PlanMetaProto, SqlMetaProto,
    };
    use crate::topsql_reporter::ru_datamodel::{TopRuRecordItem, OTHERS_USER_WIRE_LABEL};
    use crate::topsql_state;
    use crate::topsql_stmtstats::{default_ru_version, BinaryDigest, RuIncrement, RuKey};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    /// The two late-drop counters are process-global and the Go test binary
    /// runs these cases sequentially; the cases that read counter deltas are
    /// serialized here so Rust's parallel test runner cannot interleave them.
    static LATE_DROP_METRICS_GUARD: Mutex<()> = Mutex::new(());

    fn ru_key(user: &str, sql: &str, plan: &str) -> RuKey {
        RuKey {
            user: user.to_owned(),
            sql_digest: BinaryDigest::from(sql),
            plan_digest: BinaryDigest::from(plan),
        }
    }

    fn incr(total_ru: f64, exec_count: u64, exec_duration: u64) -> RuIncrement {
        RuIncrement {
            total_ru,
            exec_count,
            exec_duration,
        }
    }

    fn one(key: RuKey, incr: RuIncrement) -> RuIncrementMap {
        let mut map = RuIncrementMap::new();
        map.insert(key, incr);
        map
    }

    impl RuWindowAggregator {
        /// Go's test-only `(*ruWindowAggregator).addBatchToBucket`.
        fn add_batch_to_bucket(&self, ts: u64, increments: RuIncrementMap) {
            if increments.is_empty() {
                return;
            }
            let version = {
                let state = self.state.lock().unwrap();
                state.current_version
            };
            let version = if version == RuVersion::UNSPECIFIED {
                default_ru_version()
            } else {
                version
            };
            self.add_batch(&RuBatch {
                timestamp: ts,
                data: increments,
                version,
            });
        }
    }

    /// Go `makeRUBatch`.
    fn make_ru_batch(num_users: usize, num_sqls_per_user: usize) -> RuIncrementMap {
        let mut batch = RuIncrementMap::with_capacity(num_users * num_sqls_per_user);
        for u in 0..num_users {
            for s in 0..num_sqls_per_user {
                let key = RuKey {
                    user: format!("u{u:04}"),
                    sql_digest: BinaryDigest::from(format!("sql{u:04}_{s:04}").as_str()),
                    plan_digest: BinaryDigest::from("plan"),
                };
                let total_ru = (num_users * num_sqls_per_user - u * num_sqls_per_user - s) as f64;
                batch.insert(key, incr(total_ru, 1, 1));
            }
        }
        batch
    }

    /// Go `sumTopRUItems`.
    fn sum_top_ru_items(items: &[TopRuRecordItem]) -> f64 {
        items.iter().map(|item| item.total_ru).sum()
    }

    /// Go `totalRUFromTopRURecords`.
    fn total_ru_from_top_ru_records(records: &[TopRuRecord]) -> f64 {
        records.iter().map(|rec| sum_top_ru_items(&rec.items)).sum()
    }

    /// Go `findRURecordByDigest`.
    fn find_ru_record_by_digest<'a>(
        records: &'a [TopRuRecord],
        user: &str,
        sql_digest: &str,
        plan_digest: &str,
    ) -> Option<&'a TopRuRecord> {
        records.iter().find(|rec| {
            rec.user == user
                && rec.sql_digest == sql_digest.as_bytes()
                && rec.plan_digest == plan_digest.as_bytes()
        })
    }

    /// Go `findRURecord`, whose only difference is failing the test when the
    /// record is missing.
    fn find_ru_record<'a>(
        records: &'a [TopRuRecord],
        user: &str,
        sql_digest: &str,
        plan_digest: &str,
    ) -> &'a TopRuRecord {
        find_ru_record_by_digest(records, user, sql_digest, plan_digest).unwrap_or_else(|| {
            panic!("record not found: user={user} sql={sql_digest} plan={plan_digest}")
        })
    }

    #[test]
    fn test_ru_window_aggregator_report_granularity() {
        // Contract: the same four 15s buckets are regrouped by item interval.
        let run = |interval: u64, expected_ts: &[u64], expected_ru: &[f64]| {
            let agg = RuWindowAggregator::new();
            let key = ru_key("u1", "sql1", "plan1");
            agg.add_batch_to_bucket(1, one(key.clone(), incr(1.0, 1, 10)));
            agg.add_batch_to_bucket(16, one(key.clone(), incr(2.0, 1, 20)));
            agg.add_batch_to_bucket(31, one(key.clone(), incr(3.0, 1, 30)));
            agg.add_batch_to_bucket(46, one(key, incr(4.0, 1, 40)));

            let records = agg
                .take_report_records(60, interval, b"ks")
                .expect("records");
            assert!(!records.is_empty());

            let rec = find_ru_record(&records, "u1", "sql1", "plan1");
            assert_eq!(expected_ts.len(), rec.items.len());
            for (i, ts) in expected_ts.iter().enumerate() {
                assert_eq!(*ts, rec.items[i].timestamp_sec);
                assert!((expected_ru[i] - rec.items[i].total_ru).abs() < 1e-9);
            }
        };

        run(15, &[0, 15, 30, 45], &[1.0, 2.0, 3.0, 4.0]);
        run(30, &[0, 30], &[3.0, 7.0]);
        run(60, &[0], &[10.0]);
    }

    #[test]
    fn test_ru_window_aggregator_compact_to_200() {
        // Build >200 users in one 15s bucket, then force rotation by writing
        // to the next bucket.
        let agg = RuWindowAggregator::new();
        let mut batch = RuIncrementMap::with_capacity(250);
        for i in 0..250 {
            batch.insert(
                ru_key(&format!("u{i:03}"), "sql", "plan"),
                incr(f64::from(250 - i), 1, 1), // keep deterministic ranking
            );
        }
        agg.add_batch_to_bucket(1, batch);
        agg.add_batch_to_bucket(16, one(ru_key("next", "sql", "plan"), incr(1.0, 0, 0)));

        let state = agg.state.lock().unwrap();
        let bucket = state.buckets.get(&0).expect("bucket 0");
        assert!(bucket.collecting.is_none());
        let compacted = bucket.compacted_collecting.as_ref().expect("compacted");
        assert!(compacted.users.len() <= MAX_TOP_USERS);
    }

    #[test]
    fn test_ru_window_aggregator_take_once_per_window() {
        // Contract: one aligned 60s window is reported at most once. 59 is
        // still open, 60 closes [0,60), and later calls do not re-emit it.
        let agg = RuWindowAggregator::new();
        agg.add_batch_to_bucket(1, one(ru_key("u1", "sql1", "plan1"), incr(1.0, 1, 1)));

        assert!(agg.take_report_records(59, 60, b"ks").is_none());
        assert!(agg.take_report_records(60, 60, b"ks").is_some());
        assert!(agg.take_report_records(61, 60, b"ks").is_none());
    }

    #[test]
    fn test_reset_current_window_first_batch_establishes_initial_version() {
        let agg = RuWindowAggregator::new();
        assert_eq!(
            RuVersion::UNSPECIFIED,
            agg.state.lock().unwrap().current_version
        );

        agg.add_batch(&RuBatch {
            timestamp: 1,
            version: RuVersion::V2,
            data: one(ru_key("u-init", "sql-init", "plan-init"), incr(1.0, 1, 1)),
        });

        let state = agg.state.lock().unwrap();
        assert_eq!(RuVersion::V2, state.current_version);
        assert_eq!(1, state.buckets.len());
    }

    #[test]
    fn test_reset_current_window_drops_until_next_boundary() {
        let agg = RuWindowAggregator::new();
        let key = ru_key("u-reset", "sql-reset", "plan-reset");
        agg.add_batch_to_bucket(121, one(key.clone(), incr(3.0, 1, 1)));

        agg.reset_for_handover(RuVersion::V2, 125);
        agg.add_batch_to_bucket(126, one(key.clone(), incr(5.0, 1, 1)));
        assert!(agg.take_report_records(180, 60, b"ks").is_none());

        agg.add_batch_to_bucket(181, one(key, incr(7.0, 1, 1)));
        let records = agg.take_report_records(240, 60, b"ks").expect("records");
        let rec = find_ru_record(&records, "u-reset", "sql-reset", "plan-reset");
        assert_eq!(1, rec.items.len());
        assert_eq!(180, rec.items[0].timestamp_sec);
        assert!((7.0 - rec.items[0].total_ru).abs() < 1e-9);
    }

    #[test]
    fn test_reset_current_window_aligned_boundary_keeps_current_window() {
        let agg = RuWindowAggregator::new();
        let key = ru_key("u-aligned", "sql-aligned", "plan-aligned");

        agg.reset_for_handover(RuVersion::V2, 180);
        agg.add_batch_to_bucket(181, one(key, incr(9.0, 1, 1)));

        let records = agg.take_report_records(240, 60, b"ks").expect("records");
        let rec = find_ru_record(&records, "u-aligned", "sql-aligned", "plan-aligned");
        assert_eq!(1, rec.items.len());
        assert_eq!(180, rec.items[0].timestamp_sec);
        assert!((9.0 - rec.items[0].total_ru).abs() < 1e-9);
    }

    #[test]
    fn test_ru_window_aggregator_concurrent_pressure() {
        // Under contention, addBatchToBucket must not panic or lose
        // structural integrity: records still produce valid reports.
        const NUM_WRITERS: usize = 16;
        const BATCHES_PER_WRITER: usize = 100;
        const NUM_USERS: usize = 50;
        let agg = Arc::new(RuWindowAggregator::new());

        let mut handles = Vec::with_capacity(NUM_WRITERS);
        for writer_id in 0..NUM_WRITERS {
            let agg = Arc::clone(&agg);
            handles.push(std::thread::spawn(move || {
                for i in 0..BATCHES_PER_WRITER {
                    let ts = (i % 15) as u64;
                    let mut batch = RuIncrementMap::with_capacity(NUM_USERS);
                    for u in 0..NUM_USERS {
                        batch.insert(
                            ru_key(&format!("u{u}"), &format!("sql{writer_id}_{i}"), "plan"),
                            incr(1.0, 1, 1),
                        );
                    }
                    agg.add_batch_to_bucket(ts, batch);
                }
            }));
        }
        for handle in handles {
            handle.join().expect("writer thread");
        }

        // Filler points for later buckets so [0,60) is a complete window.
        let mut ts = 15;
        while ts < 60 {
            agg.add_batch_to_bucket(ts, one(ru_key("u0", "sql_filler", "plan"), incr(1.0, 1, 1)));
            ts += 15;
        }

        let records = agg
            .take_report_records(60, 60, b"ks")
            .expect("concurrent writes should produce a non-nil report");

        // Only structural integrity is asserted, not exact totals, to avoid
        // coupling this stress test to internal compaction choices.
        let mut total_ru = 0.0;
        for rec in &records {
            for item in &rec.items {
                assert!(item.total_ru >= 0.0, "negative RU in output");
                total_ru += item.total_ru;
            }
        }
        assert!(total_ru > 0.0, "total reported RU should be positive");
    }

    #[test]
    fn test_shifts_late_data_after_window_reported() {
        // A closed [0,60) window is reported only once; late writes to it are
        // shifted to the earliest still-open report window.
        let agg = RuWindowAggregator::new();

        agg.add_batch_to_bucket(1, one(ru_key("u1", "sql-a", "plan-a"), incr(10.0, 1, 10)));
        let first = agg.take_report_records(60, 60, b"ks").expect("first");
        assert!(find_ru_record_by_digest(&first, "u1", "sql-a", "plan-a").is_some());

        agg.add_batch_to_bucket(
            10,
            one(ru_key("u1", "sql-late", "plan-late"), incr(999.0, 1, 1)),
        );
        agg.add_batch_to_bucket(
            61,
            one(ru_key("u1", "sql-cur", "plan-cur"), incr(1.0, 1, 1)),
        );

        let second = agg.take_report_records(120, 60, b"ks").expect("second");
        assert!(!second.is_empty());
        let late = find_ru_record_by_digest(&second, "u1", "sql-late", "plan-late").expect("late");
        assert_eq!(1, late.items.len());
        assert!((999.0 - late.items[0].total_ru).abs() < 1e-9);
        let cur = find_ru_record_by_digest(&second, "u1", "sql-cur", "plan-cur").expect("cur");
        assert_eq!(1, cur.items.len());
        assert!((1.0 - cur.items[0].total_ru).abs() < 1e-9);
        assert!((1000.0 - total_ru_from_top_ru_records(&second)).abs() < 1e-9);
    }

    #[test]
    fn test_drop_on_compacted_late_target_is_tracked() {
        let _guard = LATE_DROP_METRICS_GUARD
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let agg = RuWindowAggregator::new();
        let before_dropped_keys = IGNORE_LATE_COMPACTED_RU_KEYS_COUNTER.get();
        let before_dropped_ru = IGNORE_LATE_COMPACTED_RU_TOTAL_COUNTER.get();

        agg.add_batch_to_bucket(1, one(ru_key("u1", "sql-a", "plan-a"), incr(10.0, 1, 10)));
        assert!(agg.take_report_records(60, 60, b"ks").is_some());

        agg.add_batch_to_bucket(
            61,
            one(ru_key("u1", "sql-cur", "plan-cur"), incr(1.0, 1, 1)),
        );
        agg.add_batch_to_bucket(
            76,
            one(ru_key("u1", "sql-cur-2", "plan-cur-2"), incr(2.0, 1, 1)),
        );
        agg.add_batch_to_bucket(
            10,
            one(ru_key("u1", "sql-late", "plan-late"), incr(999.0, 1, 1)),
        );

        assert!(
            (1.0 - (IGNORE_LATE_COMPACTED_RU_KEYS_COUNTER.get() - before_dropped_keys)).abs()
                < 1e-9
        );
        assert!(
            (999.0 - (IGNORE_LATE_COMPACTED_RU_TOTAL_COUNTER.get() - before_dropped_ru)).abs()
                < 1e-9
        );
    }

    #[test]
    fn test_late_data_under_concurrent_reporting() {
        // Concurrent report plus late writes are best-effort: late writes can
        // land in the second or third window, or be dropped if they race with
        // bucket compaction, but dropped RU must stay observable in metrics.
        let _guard = LATE_DROP_METRICS_GUARD
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let agg = Arc::new(RuWindowAggregator::new());
        let before_dropped_keys = IGNORE_LATE_COMPACTED_RU_KEYS_COUNTER.get();
        let before_dropped_ru = IGNORE_LATE_COMPACTED_RU_TOTAL_COUNTER.get();
        let key_a = ru_key("u-a", "sql-a", "plan-a");
        let key_b = ru_key("u-b", "sql-b", "plan-b");
        let late_key = ru_key("u-late", "sql-late", "plan-late");

        for ts in [1, 16, 31, 46] {
            agg.add_batch_to_bucket(ts, one(key_a.clone(), incr(1.0, 1, 1)));
        }
        for ts in [61, 76, 91, 106] {
            agg.add_batch_to_bucket(ts, one(key_b.clone(), incr(2.0, 1, 1)));
        }

        let first = agg.take_report_records(60, 60, b"ks").expect("first");
        assert!(find_ru_record_by_digest(&first, "u-a", "sql-a", "plan-a").is_some());

        let late_agg = Arc::clone(&agg);
        let late_key_thread = late_key.clone();
        let late_handle = std::thread::spawn(move || {
            for _ in 0..200 {
                late_agg.add_batch_to_bucket(10, one(late_key_thread.clone(), incr(999.0, 1, 1)));
            }
        });
        let report_agg = Arc::clone(&agg);
        let report_handle =
            std::thread::spawn(move || report_agg.take_report_records(120, 60, b"ks"));
        late_handle.join().expect("late thread");
        let second = report_handle
            .join()
            .expect("report thread")
            .unwrap_or_default();

        assert!(!second.is_empty());
        let rec_b = find_ru_record_by_digest(&second, "u-b", "sql-b", "plan-b").expect("u-b");
        assert_eq!(1, rec_b.items.len());
        assert!((8.0 - rec_b.items[0].total_ru).abs() < 1e-9);

        assert!(agg.take_report_records(120, 60, b"ks").is_none());

        for ts in [121, 136, 151, 166] {
            agg.add_batch_to_bucket(ts, one(ru_key("u-c", "sql-c", "plan-c"), incr(3.0, 1, 1)));
        }
        let third = agg.take_report_records(180, 60, b"ks").expect("third");
        assert!(find_ru_record_by_digest(&third, "u-c", "sql-c", "plan-c").is_some());

        let mut late_total = 0.0;
        if let Some(rec) = find_ru_record_by_digest(&second, "u-late", "sql-late", "plan-late") {
            late_total += sum_top_ru_items(&rec.items);
        }
        if let Some(rec) = find_ru_record_by_digest(&third, "u-late", "sql-late", "plan-late") {
            late_total += sum_top_ru_items(&rec.items);
        }
        let dropped_keys = IGNORE_LATE_COMPACTED_RU_KEYS_COUNTER.get() - before_dropped_keys;
        let dropped_ru = IGNORE_LATE_COMPACTED_RU_TOTAL_COUNTER.get() - before_dropped_ru;
        assert!((200.0 * 999.0 - (late_total + dropped_ru)).abs() < 1e-6);
        assert!((dropped_ru / 999.0 - dropped_keys).abs() < 1e-9);
    }

    /// Go's shared shape check: real users and their SQL counts, ignoring the
    /// others-user record, which must always carry empty digests.
    fn count_real_users(records: &[TopRuRecord]) -> HashMap<String, usize> {
        let mut real_users: HashMap<String, usize> = HashMap::new();
        for rec in records {
            if rec.user == OTHERS_USER_WIRE_LABEL {
                assert!(rec.sql_digest.is_empty());
                assert!(rec.plan_digest.is_empty());
                continue;
            }
            if !rec.sql_digest.is_empty() || !rec.plan_digest.is_empty() {
                *real_users.entry(rec.user.clone()).or_default() += 1;
            } else {
                real_users.entry(rec.user.clone()).or_default();
            }
        }
        real_users
    }

    #[test]
    fn test_ru_window_aggregator_final_report_capped_to_100x100() {
        // The final 60s output enforces 100 users and 100 SQLs per user.
        let agg = RuWindowAggregator::new();
        const NUM_USERS: usize = 120;
        const NUM_SQLS_PER_USER: usize = 120;
        let mut batch = RuIncrementMap::with_capacity(NUM_USERS * NUM_SQLS_PER_USER);
        for u in 0..NUM_USERS {
            for s in 0..NUM_SQLS_PER_USER {
                // Keep deterministic ranking and avoid ties.
                let total_ru = ((NUM_USERS - u) * 1_000_000 + (NUM_SQLS_PER_USER - s)) as f64;
                batch.insert(
                    ru_key(&format!("u{u:03}"), &format!("sql_{u:03}_{s:03}"), "plan"),
                    incr(total_ru, 1, 1),
                );
            }
        }
        agg.add_batch_to_bucket(1, batch);

        let records = agg.take_report_records(60, 60, b"ks").expect("records");
        assert!(!records.is_empty());

        let others_user_total_ru: f64 = records
            .iter()
            .filter(|rec| {
                rec.user == OTHERS_USER_WIRE_LABEL
                    && rec.sql_digest.is_empty()
                    && rec.plan_digest.is_empty()
            })
            .map(|rec| sum_top_ru_items(&rec.items))
            .sum();
        let real_users = count_real_users(&records);
        assert!(real_users.len() <= RU_REPORT_TOP_N_USERS);
        for (user, sql_count) in &real_users {
            assert!(*sql_count <= RU_REPORT_TOP_N_SQLS_PER_USER, "user={user}");
        }
        assert!(others_user_total_ru > 0.0);
    }

    #[test]
    fn test_regroup_sparse_buckets_no_phantom_points() {
        for (interval, expected_ts, expected_ru) in [
            (30u64, vec![0u64, 30], vec![2.0f64, 3.0]),
            (60, vec![0], vec![5.0]),
        ] {
            let agg = RuWindowAggregator::new();
            let key = ru_key("u-sparse", "sql-sparse", "plan-sparse");
            agg.add_batch_to_bucket(1, one(key.clone(), incr(2.0, 1, 1)));
            agg.add_batch_to_bucket(31, one(key, incr(3.0, 1, 1)));

            let records = agg
                .take_report_records(60, interval, b"ks")
                .expect("records");
            assert_eq!(1, records.len());
            let rec = find_ru_record_by_digest(&records, "u-sparse", "sql-sparse", "plan-sparse")
                .expect("sparse record");
            assert_eq!(expected_ts.len(), rec.items.len());
            for (i, ts) in expected_ts.iter().enumerate() {
                assert_eq!(*ts, rec.items[i].timestamp_sec);
                assert!((expected_ru[i] - rec.items[i].total_ru).abs() < 1e-9);
            }
            assert!((5.0 - total_ru_from_top_ru_records(&records)).abs() < 1e-9);
        }
    }

    #[test]
    fn test_over_cap_behavior_keeps_hot_keys() {
        // Under over-cap cardinality the final report stays within the 100x100
        // limits, preserves hot keys, and does not leak already reported
        // window data into the next report window.
        let agg = RuWindowAggregator::new();
        const NUM_USERS: usize = 130;
        const NUM_SQLS_PER_USER: usize = 130;
        const HOT_RU: f64 = 1e9;
        let mut batch = make_ru_batch(NUM_USERS, NUM_SQLS_PER_USER);
        batch.insert(ru_key("u-hot", "sql-hot", "plan-hot"), incr(HOT_RU, 1, 1));
        for ts in [1, 16, 31, 46] {
            agg.add_batch_to_bucket(ts, batch.clone());
        }

        let records = agg.take_report_records(60, 60, b"ks").expect("records");
        assert!(!records.is_empty());

        let real_users = count_real_users(&records);
        assert!(real_users.len() <= RU_REPORT_TOP_N_USERS);
        for (user, sql_count) in &real_users {
            assert!(*sql_count <= RU_REPORT_TOP_N_SQLS_PER_USER, "user={user}");
        }

        let hot = find_ru_record_by_digest(&records, "u-hot", "sql-hot", "plan-hot").expect("hot");
        assert_eq!(1, hot.items.len());
        assert!((HOT_RU * 4.0 - hot.items[0].total_ru).abs() < 1e-6);

        agg.add_batch_to_bucket(
            61,
            one(ru_key("u-next", "sql-next", "plan-next"), incr(7.0, 1, 1)),
        );
        let next = agg.take_report_records(120, 60, b"ks").expect("next");
        assert!(!next.is_empty());
        assert!(find_ru_record_by_digest(&next, "u-hot", "sql-hot", "plan-hot").is_none());
        let next_rec =
            find_ru_record_by_digest(&next, "u-next", "sql-next", "plan-next").expect("u-next");
        assert_eq!(1, next_rec.items.len());
        assert!((7.0 - next_rec.items[0].total_ru).abs() < 1e-9);
    }

    // ---------------------------------------------------------------------
    // Go `topru_case_runner_test.go` + `topru_generated_cases_test.go`.
    //
    // boundary: the Go runner drives a `RemoteTopSQLReporter` and waits for a
    // `DataSink` to receive the payload. `reporter.go` and `datasink.go` are
    // gRPC-bound and out of scope, so the round trip is replaced by building
    // the same `ReportData` directly out of the pieces this layer owns: the
    // window aggregator's report records and the normalized SQL/plan maps.
    // Every semantic assertion of the generated cases is kept.
    // ---------------------------------------------------------------------

    /// Go `caseSpec`.
    struct CaseSpec {
        goal_id: &'static str,
        require_send: bool,
        ru_records_min: usize,
        exec_count_min: u64,
        exec_count_sum_min: u64,
        total_ru_min: f64,
        sql_meta_match_marker: &'static str,
        plan_meta_required: Option<bool>,
    }

    /// boundary: Go `ReportData` lives in `datasink.go`; only the three
    /// payload fields the generated cases assert on are declared here.
    struct ReportData {
        ru_records: Vec<TopRuRecord>,
        sql_metas: Vec<SqlMetaProto>,
        plan_metas: Vec<PlanMetaProto>,
    }

    /// Go `mockPlanBinaryDecoderFunc` / `mockPlanBinaryCompressFunc`.
    fn mock_decode_plan(plan: &str) -> Result<String, String> {
        Ok(plan.to_owned())
    }

    fn mock_compress_plan(plan: &[u8]) -> String {
        String::from_utf8_lossy(plan).into_owned()
    }

    /// Go `topRUGeneratedCaseSpecs`.
    const TOP_RU_GENERATED_CASE_SPECS: &[CaseSpec] = &[
        CaseSpec {
            goal_id: "sqlmeta_present",
            require_send: false,
            ru_records_min: 0,
            exec_count_min: 0,
            exec_count_sum_min: 0,
            total_ru_min: 0.0,
            sql_meta_match_marker: "topru_gen_sqlmeta",
            plan_meta_required: None,
        },
        CaseSpec {
            goal_id: "planmeta_present",
            require_send: false,
            ru_records_min: 0,
            exec_count_min: 0,
            exec_count_sum_min: 0,
            total_ru_min: 0.0,
            sql_meta_match_marker: "",
            plan_meta_required: Some(true),
        },
        CaseSpec {
            goal_id: "multi_records_batch",
            require_send: false,
            ru_records_min: 2,
            exec_count_min: 0,
            exec_count_sum_min: 2,
            total_ru_min: 0.0,
            sql_meta_match_marker: "",
            plan_meta_required: None,
        },
        CaseSpec {
            goal_id: "total_ru_threshold",
            require_send: false,
            ru_records_min: 0,
            exec_count_min: 0,
            exec_count_sum_min: 0,
            total_ru_min: 1.5,
            sql_meta_match_marker: "",
            plan_meta_required: None,
        },
        CaseSpec {
            goal_id: "key_aggregation_by_user_sql_plan",
            require_send: true,
            ru_records_min: 0,
            exec_count_min: 0,
            exec_count_sum_min: 0,
            total_ru_min: 0.0,
            sql_meta_match_marker: "",
            plan_meta_required: None,
        },
        CaseSpec {
            goal_id: "same_timestamp_multiple_finish_accumulate",
            require_send: true,
            ru_records_min: 1,
            exec_count_min: 0,
            exec_count_sum_min: 2,
            total_ru_min: 0.0,
            sql_meta_match_marker: "",
            plan_meta_required: None,
        },
        CaseSpec {
            goal_id: "internal_sql_empty_user_handling",
            require_send: true,
            ru_records_min: 0,
            exec_count_min: 0,
            exec_count_sum_min: 0,
            total_ru_min: 0.0,
            sql_meta_match_marker: "",
            plan_meta_required: None,
        },
        CaseSpec {
            goal_id: "short_exec_time_lt_1s_handling",
            require_send: true,
            ru_records_min: 0,
            exec_count_min: 0,
            exec_count_sum_min: 0,
            total_ru_min: 0.0,
            sql_meta_match_marker: "",
            plan_meta_required: None,
        },
    ];

    /// Go `runTopRUCase`: builds one deterministic closed 60s window so each
    /// generated case validates a semantic contract without clock flakiness.
    fn run_top_ru_case(cs: &CaseSpec) {
        topsql_state::GLOBAL_STATE
            .max_collect
            .store(5000, Ordering::SeqCst);
        let aggregator = RuWindowAggregator::new();
        let normalized_sql_map = NormalizedSqlMap::new();
        let normalized_plan_map = NormalizedPlanMap::new();

        let record_count = cs.ru_records_min.max(1);
        let marker = if cs.sql_meta_match_marker.is_empty() {
            format!("topru_gen_{}", cs.goal_id.to_lowercase())
        } else {
            cs.sql_meta_match_marker.to_owned()
        };
        let total_ru_baseline = if cs.total_ru_min <= 0.0 {
            0.001
        } else {
            cs.total_ru_min
        };
        let required_sum = (cs.exec_count_sum_min as usize).max(record_count);

        let mut exec_counts = vec![1u64; record_count];
        exec_counts[0] += (required_sum - record_count) as u64;
        if exec_counts[0] < cs.exec_count_min {
            exec_counts[0] = cs.exec_count_min;
        }

        const SAMPLE_TS: u64 = 1_700_000_000;
        match cs.goal_id {
            "key_aggregation_by_user_sql_plan" => {
                // The same SQL/plan under different users stays isolated by
                // RUKey.User.
                normalized_sql_map.register(b"S_G7", &format!("/* {marker} */ select 7"), false);
                normalized_plan_map.register(b"P_G7", &format!("plan_{marker}_7"), false);
                let mut batch = RuIncrementMap::new();
                batch.insert(ru_key("u1", "S_G7", "P_G7"), incr(7.0, 1, 1000));
                batch.insert(ru_key("u2", "S_G7", "P_G7"), incr(9.0, 1, 1000));
                aggregator.add_batch_to_bucket(SAMPLE_TS, batch);
            }
            "same_timestamp_multiple_finish_accumulate" => {
                // The same key in the same bucket accumulates RU / ExecCount /
                // Duration.
                normalized_sql_map.register(b"S_G8", &format!("/* {marker} */ select 8"), false);
                normalized_plan_map.register(b"P_G8", &format!("plan_{marker}_8"), false);
                let key = ru_key("root", "S_G8", "P_G8");
                aggregator.add_batch_to_bucket(SAMPLE_TS, one(key.clone(), incr(3.0, 1, 1000)));
                aggregator.add_batch_to_bucket(SAMPLE_TS, one(key, incr(4.0, 2, 2000)));
            }
            "internal_sql_empty_user_handling" => {
                // The empty user is valid and is not rewritten to the
                // others-user sentinel.
                normalized_sql_map.register(b"S_G10", &format!("/* {marker} */ select 10"), false);
                normalized_plan_map.register(b"P_G10", &format!("plan_{marker}_10"), false);
                aggregator.add_batch_to_bucket(
                    SAMPLE_TS,
                    one(ru_key("", "S_G10", "P_G10"), incr(10.0, 1, 1000)),
                );
            }
            "short_exec_time_lt_1s_handling" => {
                // A sub-second duration is kept in nanoseconds and reported
                // as-is.
                normalized_sql_map.register(b"S_G11", &format!("/* {marker} */ select 11"), false);
                normalized_plan_map.register(b"P_G11", &format!("plan_{marker}_11"), false);
                aggregator.add_batch_to_bucket(
                    SAMPLE_TS,
                    one(ru_key("root", "S_G11", "P_G11"), incr(11.0, 1, 500_000_000)),
                );
            }
            _ => {
                let mut batch = RuIncrementMap::with_capacity(record_count);
                for (i, exec_count) in exec_counts.iter().enumerate() {
                    let sql_digest = format!("S_{}_{i}", cs.goal_id);
                    let plan_digest = format!("P_{}_{i}", cs.goal_id);
                    normalized_sql_map.register(
                        sql_digest.as_bytes(),
                        &format!("/* {marker} */ select {i}"),
                        false,
                    );
                    normalized_plan_map.register(
                        plan_digest.as_bytes(),
                        &format!("plan_{marker}_{i}"),
                        false,
                    );
                    batch.insert(
                        ru_key("root", &sql_digest, &plan_digest),
                        incr(
                            total_ru_baseline + (i + 1) as f64,
                            *exec_count,
                            (1000 + i * 100) as u64,
                        ),
                    );
                }
                aggregator.add_batch_to_bucket(SAMPLE_TS, batch);
            }
        }

        // Emit exactly one aligned closed [start, start+60) window.
        let report_ts =
            align_to_interval(SAMPLE_TS, RU_REPORT_WINDOW_SECONDS) + RU_REPORT_WINDOW_SECONDS;
        let payload = ReportData {
            ru_records: aggregator
                .take_report_records(report_ts, 60, b"topru-gen-keyspace")
                .unwrap_or_default(),
            sql_metas: normalized_sql_map.take().to_proto(b"topru-gen-keyspace"),
            plan_metas: normalized_plan_map.take().to_proto(
                b"topru-gen-keyspace",
                &mock_decode_plan,
                &mock_compress_plan,
            ),
        };

        if cs.require_send {
            assert!(
                !payload.ru_records.is_empty(),
                "missing payload for goal {}",
                cs.goal_id
            );
        }
        if cs.ru_records_min > 0 {
            assert!(payload.ru_records.len() >= cs.ru_records_min);
        }

        let mut max_exec_count = 0u64;
        let mut sum_exec_count = 0u64;
        let mut max_total_ru = 0.0f64;
        for rec in &payload.ru_records {
            for item in &rec.items {
                max_exec_count = max_exec_count.max(item.exec_count);
                sum_exec_count += item.exec_count;
                max_total_ru = max_total_ru.max(item.total_ru);
            }
        }
        if cs.exec_count_min > 0 {
            assert!(max_exec_count >= cs.exec_count_min);
        }
        if cs.exec_count_sum_min > 0 {
            assert!(sum_exec_count >= cs.exec_count_sum_min);
        }
        if cs.total_ru_min > 0.0 {
            assert!(max_total_ru >= cs.total_ru_min);
        }
        if !cs.sql_meta_match_marker.is_empty() {
            assert!(
                payload
                    .sql_metas
                    .iter()
                    .any(|meta| meta.normalized_sql.contains(cs.sql_meta_match_marker)),
                "missing SQLMeta marker: {}",
                cs.sql_meta_match_marker
            );
        }
        if cs.plan_meta_required == Some(true) {
            assert!(!payload.plan_metas.is_empty());
        }
        assert_top_ru_case_payload(cs.goal_id, &payload);
    }

    /// Go `assertTopRUCasePayload`.
    fn assert_top_ru_case_payload(goal_id: &str, payload: &ReportData) {
        match goal_id {
            "key_aggregation_by_user_sql_plan" => {
                let mut users: Vec<&str> = payload
                    .ru_records
                    .iter()
                    .filter(|rec| rec.sql_digest == b"S_G7" && rec.plan_digest == b"P_G7")
                    .map(|rec| rec.user.as_str())
                    .collect();
                users.sort_unstable();
                users.dedup();
                assert_eq!(vec!["u1", "u2"], users);
            }
            "same_timestamp_multiple_finish_accumulate" => {
                let rec = find_ru_record_by_digest(&payload.ru_records, "root", "S_G8", "P_G8")
                    .expect("S_G8 record");
                assert_eq!(1, rec.items.len());
                assert!((7.0 - rec.items[0].total_ru).abs() < 1e-9);
                assert_eq!(3, rec.items[0].exec_count);
                assert_eq!(3000, rec.items[0].exec_duration);
            }
            "internal_sql_empty_user_handling" => {
                let rec = find_ru_record_by_digest(&payload.ru_records, "", "S_G10", "P_G10")
                    .expect("S_G10 record");
                assert!(!rec.items.is_empty());
            }
            "short_exec_time_lt_1s_handling" => {
                let rec = find_ru_record_by_digest(&payload.ru_records, "root", "S_G11", "P_G11")
                    .expect("S_G11 record");
                assert!(!rec.items.is_empty());
                assert_eq!(500_000_000, rec.items[0].exec_duration);
            }
            _ => {}
        }
    }

    /// Go `TestTopRUGeneratedCases`, whose subtests become one case each.
    #[test]
    fn test_top_ru_generated_cases() {
        for cs in TOP_RU_GENERATED_CASE_SPECS {
            run_top_ru_case(cs);
        }
    }
}
