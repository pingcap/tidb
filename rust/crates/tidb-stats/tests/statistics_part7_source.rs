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

//! testport batch b048 — `pkg/statistics` (part7) unit tests ported from
//! origin/master Go sources into the `tidb-stats` crate.
//!
//! Part7 scope (deterministic derivation, matching batches b042/b044):
//! all `func Test*` under `pkg/statistics/**`, sorted by (file path, line),
//! chunked into groups of 60; part7 = items 361–420.
//!
//! Portable tests live here; go-parity gaps carry `#[ignore]` markers below.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tidb_datatype::{
    BinaryLiteral, CoreTime, Datum, Decimal, MySqlDuration, Time, TimeType,
};
use tidb_stats::index_usage::{
    IndexUsageCollector, SessionIndexUsageCollector as _, StmtIndexUsageCollector,
};
use tidb_stats::{enum_range_values, new_index_usage_sample, IndexColumnInfo};

// -------------------------------------------------------------------------
// scalar_test.go::TestCalcFraction
// -------------------------------------------------------------------------

/// scalar_test.go::TestCalcFraction — the full master matrix. The Go test
/// builds a one-bucket histogram over `[lower, upper]` and asserts
/// `calcFraction(0, value)` within 1e-9. The Rust histogram computes the
/// fraction from the stored bound datums directly (`calc_fraction_from_datums`),
/// so no separate scalar precalculation step exists to drive.
#[test]
fn source_calc_fraction_matches_go_matrix() {
    const EPS: f64 = 1e-9;

    fn decimal(value: f64) -> Datum {
        Datum::Decimal(Decimal::from_f64(value).expect("decimal from float"))
    }

    fn duration(hours: i64) -> Datum {
        Datum::Duration(MySqlDuration::from_nanoseconds(hours * 3_600_000_000_000, 0).unwrap())
    }

    fn time(kind: TimeType, month: u8, day: u8) -> Datum {
        Datum::Time(
            Time::new(
                CoreTime::from_date(2017, month, day, 0, 0, 0, 0),
                kind,
                0,
            )
            .unwrap(),
        )
    }

    fn bit(value: u64, byte_size: usize) -> Datum {
        Datum::BinaryLiteral(BinaryLiteral::from_uint(value, Some(2_u8.try_into().unwrap())))
    }

    // (lower, upper, value, expected fraction)
    let cases: Vec<(Datum, Datum, Datum, f64)> = vec![
        (Datum::Int(0), Datum::Int(4), Datum::Int(1), 0.25),
        (Datum::Int(0), Datum::Int(4), Datum::Int(4), 1.0),
        (Datum::Int(0), Datum::Int(4), Datum::Int(-1), 0.0),
        (
            Datum::UInt(0),
            Datum::UInt(4),
            Datum::UInt(1),
            0.25,
        ),
        (Datum::Real(0.0), Datum::Real(4.0), Datum::Real(1.0), 0.25),
        (
            Datum::Float32(0.0),
            Datum::Float32(4.0),
            Datum::Float32(1.0),
            0.25,
        ),
        (decimal(0.0), decimal(4.0), decimal(1.0), 0.25),
        // BIT values fall through Go calcFraction4Datums' kind switch to the
        // 0.5 fallback.
        (bit(0, 1), bit(4, 1), bit(1, 1), 0.5),
        (duration(0), duration(4), duration(1), 0.25),
        (
            time(TimeType::Timestamp, 1, 1),
            time(TimeType::Timestamp, 4, 1),
            time(TimeType::Timestamp, 2, 1),
            0.344_444_444_444_444_44,
        ),
        (
            time(TimeType::DateTime, 1, 1),
            time(TimeType::DateTime, 4, 1),
            time(TimeType::DateTime, 2, 1),
            0.344_444_444_444_444_44,
        ),
        (
            time(TimeType::Date, 1, 1),
            time(TimeType::Date, 4, 1),
            time(TimeType::Date, 2, 1),
            0.344_444_444_444_444_44,
        ),
        (
            Datum::new_string("aasad"),
            Datum::new_string("addad"),
            Datum::new_string("abfsd"),
            0.322_802_539_840_637_45,
        ),
        (
            Datum::new_bytes(b"aasad".to_vec()),
            Datum::new_bytes(b"asdff".to_vec()),
            Datum::new_bytes(b"abfsd".to_vec()),
            0.052_921_680_221_726_9,
        ),
    ];

    for (lower, upper, value, expected) in cases {
        let mut hg = tidb_stats::Histogram::new(0, 0, 0, 0, 1, 0);
        hg.append_bucket(lower.clone(), upper.clone(), 0, 0);
        let fraction = hg.calc_fraction(0, &value);
        assert!(
            (expected - fraction).abs() < EPS,
            "lower={lower:?} upper={upper:?} value={value:?}: {fraction} !~ {expected}"
        );
    }
}

// -------------------------------------------------------------------------
// scalar_test.go::TestEnumRangeValues
// -------------------------------------------------------------------------

/// scalar_test.go::TestEnumRangeValues — the exact master table. Go asserts
/// formatted strings (`"(0, 1, 2)"`) or `""`; here the enumerated datums are
/// compared elementwise, which pins the same sequence. A Go result of `""`
/// corresponds to `None`.
#[test]
fn source_enum_range_values_matches_go_table() {
    fn duration(seconds: i64) -> Datum {
        Datum::Duration(MySqlDuration::from_nanoseconds(seconds * 1_000_000_000, 0).unwrap())
    }

    fn time(kind: TimeType, day: u8, second: u8) -> Datum {
        Datum::Time(
            Time::new(
                CoreTime::from_date(2017, 1, day, 0, 0, second, 0),
                kind,
                0,
            )
            .unwrap(),
        )
    }

    // int 0..5, high excluded -> "(0, 1, 2, 3, 4)"
    assert_eq!(
        enum_range_values(&Datum::Int(0), &Datum::Int(5), false, true).unwrap(),
        (0..5).map(Datum::Int).collect::<Vec<_>>()
    );

    // int MinInt64..MaxInt64 inclusive -> ""
    assert!(
        enum_range_values(&Datum::Int(i64::MIN), &Datum::Int(i64::MAX), false, false).is_none()
    );

    // uint 0..5, high excluded -> "(0, 1, 2, 3, 4)"
    assert_eq!(
        enum_range_values(&Datum::UInt(0), &Datum::UInt(5), false, true).unwrap(),
        (0..5).map(Datum::UInt).collect::<Vec<_>>()
    );

    // duration 0s..5s, high excluded -> "(00:00:00 .. 00:00:04)"
    assert_eq!(
        enum_range_values(&duration(0), &duration(5), false, true).unwrap(),
        (0..5).map(duration).collect::<Vec<_>>()
    );

    // date 2017-01-01..2017-01-05, high excluded -> 4 dates
    let dates = enum_range_values(
        &time(TimeType::Date, 1, 0),
        &time(TimeType::Date, 5, 0),
        false,
        true,
    )
    .unwrap();
    assert_eq!(dates.len(), 4);
    assert_eq!(dates[0], time(TimeType::Date, 1, 0));
    assert_eq!(dates[3], time(TimeType::Date, 4, 0));

    // timestamp / datetime 00:00:00..00:00:05, high excluded -> seconds 0..4
    for kind in [TimeType::Timestamp, TimeType::DateTime] {
        let values = enum_range_values(
            &time(kind, 1, 0),
            &time(kind, 1, 5),
            false,
            true,
        )
        .unwrap();
        assert_eq!(values.len(), 5);
        assert_eq!(values[0], time(kind, 1, 0));
        assert_eq!(values[4], time(kind, 1, 4));
    }

    // issue 11610: int MinInt64..0 inclusive -> ""
    assert!(enum_range_values(&Datum::Int(i64::MIN), &Datum::Int(0), false, false).is_none());

    // same-point range with both ends excluded -> ""
    assert!(
        enum_range_values(
            &time(TimeType::Date, 1, 0),
            &time(TimeType::Date, 1, 0),
            true,
            true
        )
        .is_none()
    );
}

// -------------------------------------------------------------------------
// indexusage/collector_test.go
// -------------------------------------------------------------------------

/// collector_test.go::TestUpdateIndex — one session collector accumulates a
/// full scan, a partial scan, and a zero-total-row scan against the same
/// index, folding counts and percentage buckets.
#[test]
fn source_update_index_accumulates_full_partial_and_zero_total_scans() {
    let global = IndexUsageCollector::new();
    global.start_worker();
    let mut collector = global.spawn_session_collector();

    // Report a normal full scan.
    collector.update(1, 1, &new_index_usage_sample(1, 1, 1, 1));
    let usage = collector.get_index_usage(1, 1).expect("entry recorded");
    assert_eq!(usage.query_total, 1);
    assert_eq!(usage.kv_req_total, 1);
    assert_eq!(usage.row_access_total, 1);
    assert_eq!(usage.percentage_access, [0, 0, 0, 0, 0, 0, 1]);

    // Report a partial scan.
    collector.update(1, 1, &new_index_usage_sample(10, 10, 5, 50));
    let usage = collector.get_index_usage(1, 1).expect("entry recorded");
    assert_eq!(usage.query_total, 11);
    assert_eq!(usage.kv_req_total, 11);
    assert_eq!(usage.row_access_total, 6);
    assert_eq!(usage.percentage_access, [0, 0, 0, 1, 0, 0, 1]);

    // Report a 0 total row scan.
    collector.update(1, 1, &new_index_usage_sample(10, 10, 5, 0));
    let usage = collector.get_index_usage(1, 1).expect("entry recorded");
    assert_eq!(usage.query_total, 21);
    assert_eq!(usage.kv_req_total, 21);
    assert_eq!(usage.row_access_total, 11);
    assert_eq!(usage.percentage_access, [0, 0, 0, 1, 0, 0, 2]);

    global.close();
}

/// Deterministic stand-in for Go's math/rand-driven op generator in
/// collector_test.go: an xorshift64* PRNG seeded identically for the serial
/// expectation and the concurrent run.
struct OpGenerator(u64);

impl OpGenerator {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// `(table_id, index_id, query_total, kv_req_total, total_rows, row_access)`
    fn next_op(&mut self) -> (i64, i64, u64, u64, u64, u64) {
        let table_id = (self.next_u64() % 10) as i64;
        let index_id = (self.next_u64() % 10) as i64;
        let query_total = self.next_u64() % 10_000;
        let kv_req_total = self.next_u64() % 10_000;
        let total_rows = self.next_u64() % 10_000;
        let row_access = if total_rows > 0 {
            self.next_u64() % total_rows
        } else {
            0
        };
        (table_id, index_id, query_total, kv_req_total, total_rows, row_access)
    }
}

/// collector_test.go::TestFlushConcurrentIndexCollector — 64 concurrent
/// session collectors apply the same operation stream (with interleaved
/// `Report()` calls, roughly one in four as in Go) and must produce exactly
/// the map a single serial session collector produces. The Go fixture uses
/// 100_000 ops per session; 20_000 keeps the property under test (serial ==
/// concurrent after flush/close) while staying fast.
#[test]
fn source_flush_concurrent_index_collector_matches_serial_expectation() {
    const SESSION_COUNT: usize = 64;
    const OPS_PER_SESSION: usize = 20_000;

    // Serial expectation.
    let mut generator = OpGenerator(0x1234_5678_9abc_def0);
    let expect_collector = IndexUsageCollector::new();
    expect_collector.start_worker();
    let mut expect_session = expect_collector.spawn_session_collector();
    let mut ops = Vec::with_capacity(SESSION_COUNT * OPS_PER_SESSION);
    for _ in 0..SESSION_COUNT * OPS_PER_SESSION {
        let (t, i, q, k, rows, access) = generator.next_op();
        let sample = new_index_usage_sample(q, k, access, rows);
        ops.push(((t, i), sample.clone()));
        expect_session.update(t, i, &sample);
    }
    expect_session.flush();

    // Concurrent application.
    let iuc = Arc::new(IndexUsageCollector::new());
    iuc.start_worker();
    let chunk = OPS_PER_SESSION;
    thread::scope(|scope| {
        for session_ops in ops.chunks(chunk) {
            let iuc = Arc::clone(&iuc);
            let session_ops: Vec<_> = session_ops.to_vec();
            scope.spawn(move || {
                let mut local = iuc.spawn_session_collector();
                for (counter, ((t, i), sample)) in session_ops.into_iter().enumerate() {
                    local.update(t, i, &sample);
                    if counter % 4 == 1 {
                        local.report();
                    }
                }
                local.flush();
            });
        }
    });

    expect_collector.close();
    iuc.close();
    assert_eq!(
        expect_collector.index_usage_snapshot(),
        iuc.index_usage_snapshot(),
        "concurrent collectors must fold to the serial expectation"
    );
}

/// collector_test.go::TestStmtIndexUsageCollector — a statement-level
/// collector reports through its session collector asynchronously; duplicate
/// updates inside one statement are collapsed and an explicit zero
/// `query_total` is forced up to 1.
#[test]
fn source_stmt_index_usage_collector_dedups_and_forces_query_total() {
    fn eventually(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        predicate()
    }

    let iuc = IndexUsageCollector::new();
    iuc.start_worker();
    let session = Arc::new(Mutex::new(iuc.spawn_session_collector()));

    let statement =
        StmtIndexUsageCollector::new(Arc::clone(&session));

    statement.update(1, 1, &new_index_usage_sample(10, 0, 0, 0));
    session.lock().expect("session lock").flush();
    // Go polls `iuc.GetIndexUsage(1,1) != Sample{}`; compare on query_total,
    // because a Rust-constructed zero sample carries a fresh wall-clock stamp
    // rather than Go's comparable zero time.Time.
    assert!(
        eventually(Duration::from_secs(1), || {
            iuc.get_index_usage(1, 1).query_total == 1
        }),
        "wait for report"
    );

    // A duplicated index update within the same statement is ignored.
    statement.update(1, 1, &new_index_usage_sample(10, 0, 0, 0));
    session.lock().expect("session lock").flush();
    assert!(
        eventually(Duration::from_secs(1), || {
            iuc.get_index_usage(1, 1).query_total == 1
        }),
        "wait for report"
    );

    statement.update(1, 2, &new_index_usage_sample(10, 0, 0, 0));
    session.lock().expect("session lock").flush();
    assert!(
        eventually(Duration::from_secs(1), || {
            iuc.get_index_usage(1, 2).query_total == 1
        }),
        "wait for report"
    );

    // `query_total` will be 1 even when set to 0.
    statement.update(1, 3, &new_index_usage_sample(0, 0, 0, 0));
    session.lock().expect("session lock").flush();
    assert!(
        eventually(Duration::from_secs(1), || {
            iuc.get_index_usage(1, 3).query_total == 1
        }),
        "wait for report"
    );

    iuc.close();
}

/// index_usage_integration_test.go::TestGCIndexUsage — usage recorded for ten
/// tables × ten indexes survives until GC; dropping indexes with ID ≥ 5
/// removes their usage, and subsequently dropping whole tables removes the
/// rest beyond the surviving five tables. The Go test creates the objects via
/// DDL through a testkit session; the collector-level contract exercised here
/// is the same `Collector.GCIndexUsage` retention rule driven by a table
/// metadata lookup.
#[test]
fn source_gc_index_usage_removes_dropped_indexes_and_tables() {
    const TABLE_COUNT: i64 = 10;
    const INDEX_COUNT: i64 = 10;

    // Index IDs are globally increasing across tables, mirroring TiDB's
    // allocation: table `t` owns ids `t * INDEX_COUNT .. (t + 1) * INDEX_COUNT`.
    let collector = IndexUsageCollector::new();
    collector.start_worker();
    let mut session = collector.spawn_session_collector();
    for t in 0..TABLE_COUNT {
        for i in 0..INDEX_COUNT {
            session.update(t, t * INDEX_COUNT + i, &new_index_usage_sample(1, 2, 3, 4));
        }
    }
    session.flush();
    // Close it. It'll no longer receive any updates.
    collector.close();

    // Live metadata snapshot: which index IDs remain per table.
    let live_indexes = |tables_alive: i64, max_index_id: i64| move |table_id: i64| {
        if table_id >= tables_alive {
            return None; // table dropped
        }
        let ids: Vec<i64> = (0..INDEX_COUNT)
            .map(|i| table_id * INDEX_COUNT + i)
            .filter(|id| *id < max_index_id)
            .collect();
        if ids.is_empty() { None } else { Some(ids) }
    };

    let verify = |collector: &IndexUsageCollector, tables_alive: i64, max_index_id: i64| {
        for t in 0..TABLE_COUNT {
            for i in 0..INDEX_COUNT {
                let id = t * INDEX_COUNT + i;
                let info = collector.get_index_usage(t, id);
                if t < tables_alive && id < max_index_id {
                    // Compare counts, not the whole sample: each observation
                    // carries its own creation wall-clock stamp.
                    assert_eq!(info.query_total, 1, "table {t} index {id}");
                    assert_eq!(info.kv_req_total, 2, "table {t} index {id}");
                    assert_eq!(info.row_access_total, 3, "table {t} index {id}");
                    assert_eq!(
                        info.percentage_access,
                        [0, 0, 0, 0, 0, 1, 0],
                        "table {t} index {id}"
                    );
                } else {
                    // Go's zero `Sample{}`; the Rust default carries the epoch
                    // stamp in place of Go's zero time.Time.
                    assert_eq!(
                        info,
                        tidb_stats::index_usage::IndexUsageSample::default(),
                        "table {t} index {id}"
                    );
                }
            }
        }
    };

    // Before GC everything is retained.
    verify(&collector, TABLE_COUNT, i64::MAX);

    // Drop every index whose ID >= 5, then GC.
    collector.gc_index_usage(live_indexes(TABLE_COUNT, 5));
    verify(&collector, TABLE_COUNT, 5);

    // Drop every table whose position >= 5, then GC.
    collector.gc_index_usage(live_indexes(5, 5));
    verify(&collector, 5, 5);
}

// -------------------------------------------------------------------------
// util_test.go::TestIsSpecialGlobalIndex
// -------------------------------------------------------------------------

/// util_test.go::TestIsSpecialGlobalIndex — the six-index DDL fixture
/// classifies as follows: plain unique/local indexes (`b`, `c`) and their
/// non-global expression/prefix twins (`b_s`, `d_s`) are NOT special, while
/// the global expression index (`ub_s`) and the global prefix index (`ud_s`)
/// ARE. The Go test derives the column facts from DDL'd table info; the
/// classification primitive takes those facts directly.
#[test]
fn source_is_special_global_index_classifies_ddl_fixture() {
    // b: unique global index on a regular column -> not special.
    assert!(!tidb_stats::is_special_global_index(
        true,
        &[IndexColumnInfo::regular()]
    ));
    // c: local index on a regular column -> not special.
    assert!(!tidb_stats::is_special_global_index(
        false,
        &[IndexColumnInfo::regular()]
    ));
    // b_s: local expression index (virtual generated column) -> not special.
    assert!(!tidb_stats::is_special_global_index(
        false,
        &[IndexColumnInfo::virtual_generated()]
    ));
    // d_s: local prefix index -> not special.
    assert!(!tidb_stats::is_special_global_index(
        false,
        &[IndexColumnInfo::prefix()]
    ));
    // ub_s: UNIQUE global index on an expression -> special.
    assert!(tidb_stats::is_special_global_index(
        true,
        &[IndexColumnInfo::virtual_generated()]
    ));
    // ud_s: UNIQUE global index on a prefix -> special.
    assert!(tidb_stats::is_special_global_index(
        true,
        &[IndexColumnInfo::prefix()]
    ));
}

// -------------------------------------------------------------------------
// go-parity-gap markers. Each `#[ignore]`d test names one Go test from the
// part7 slice whose behavior has no Rust surface yet.
// -------------------------------------------------------------------------

// usage/predicate_column_test.go::TestCleanupPredicateColumns
#[test]
#[ignore = "go-parity-gap: drives DELETE/DML cleanup of mysql.column_stats_usage through a testkit session and stats handle; outside tidb-stats"]
fn source_cleanup_predicate_columns() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/predicate_column_test.go::TestAnalyzeTableWithPredicateColumns
#[test]
#[ignore = "go-parity-gap: runs ANALYZE through a session and inspects predicate-column stats via the handle; outside tidb-stats"]
fn source_analyze_table_with_predicate_columns() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/predicate_column_test.go::TestAnalyzeTableWithTiDBPersistAnalyzeOptionsEnabled
#[test]
#[ignore = "go-parity-gap: session-variable-gated ANALYZE persistence behavior through testkit; outside tidb-stats"]
fn source_analyze_table_with_tidb_persist_analyze_options_enabled() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/predicate_column_test.go::TestAnalyzeTableWithTiDBPersistAnalyzeOptionsDisabled
#[test]
#[ignore = "go-parity-gap: session-variable-gated ANALYZE persistence behavior through testkit; outside tidb-stats"]
fn source_analyze_table_with_tidb_persist_analyze_options_disabled() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/predicate_column_test.go::TestAnalyzeNoPredicateColumnsWithIndexes
#[test]
#[ignore = "go-parity-gap: ANALYZE-through-session behavior on tables without predicate columns; outside tidb-stats"]
fn source_analyze_no_predicate_columns_with_indexes() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/predicate_column_test.go::TestAnalyzeWithNoPredicateColumnsAndNoIndexes
#[test]
#[ignore = "go-parity-gap: ANALYZE-through-session behavior on tables without predicate columns; outside tidb-stats"]
fn source_analyze_with_no_predicate_columns_and_no_indexes() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/predicate_column_test.go::TestAnalyzeNoPredicateColumnsWithPrimaryKey
#[test]
#[ignore = "go-parity-gap: ANALYZE-through-session behavior on tables without predicate columns; outside tidb-stats"]
fn source_analyze_no_predicate_columns_with_primary_key() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/session_stats_collect_test.go::TestPredicateUsage_FirstTouchCreatesRow
#[test]
#[ignore = "go-parity-gap: last_used_at NULL->set transition is observed through SQL over mysql.column_stats_usage via a stats handle; outside tidb-stats"]
fn source_predicate_usage_first_touch_creates_row() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/session_stats_collect_test.go::TestPredicateUsage_NoBumpWithinThrottle
#[test]
#[ignore = "go-parity-gap: throttle-window suppression is observed through SQL over mysql.column_stats_usage via a stats handle; outside tidb-stats"]
fn source_predicate_usage_no_bump_within_throttle() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/session_stats_collect_test.go::TestPredicateUsage_BumpAfterOldStoredValue
#[test]
#[ignore = "go-parity-gap: stale-last-used bump is observed through SQL over mysql.column_stats_usage via a stats handle; outside tidb-stats"]
fn source_predicate_usage_bump_after_old_stored_value() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/session_stats_collect_test.go::TestDumpStatsDeltaPersistsInitTime
#[test]
#[ignore = "go-parity-gap: dumpStatsMaxDuration/dumpStatsDeltaRatio timing behavior lives in the handle's usage writer with KV storage; outside tidb-stats"]
fn source_dump_stats_delta_persists_init_time() {
    unreachable!("gated by go-parity-gap ignore")
}

// usage/session_stats_collect_test.go::TestDumpStatsDeltaMergeKeepsEarliestInitTime
#[test]
#[ignore = "go-parity-gap: pessimistic-txn-blocked dump merge timing lives in the handle's usage writer with KV storage; outside tidb-stats"]
fn source_dump_stats_delta_merge_keeps_earliest_init_time() {
    unreachable!("gated by go-parity-gap ignore")
}

// handle/util/util_test.go::TestCallSCtxFailed
#[test]
#[ignore = "go-parity-gap: CallWithSCtx session-pool acquire/release semantics need the session layer; outside tidb-stats"]
fn source_call_sctx_failed() {
    unreachable!("gated by go-parity-gap ignore")
}

// handle/util/util_test.go::TestCallWithSCtxSyncsStmtCtxTimeZone
#[test]
#[ignore = "go-parity-gap: CallWithSCtx stmt-context timezone sync needs the session layer; outside tidb-stats"]
fn source_call_with_sctx_syncs_stmt_ctx_time_zone() {
    unreachable!("gated by go-parity-gap ignore")
}

// handle/util/util_test.go::TestTableItemByIDForInitStatsAvoidsV1PartitionScan
#[test]
#[ignore = "go-parity-gap: TableInfoGetter.TableItemByIDForInitStats over a mock infoschema is not ported into tidb-stats (no tidb-model dependency)"]
fn source_table_item_by_id_for_init_stats_avoids_v1_partition_scan() {
    unreachable!("gated by go-parity-gap ignore")
}

// merge_global_cases_test.go::TestMergePartTopNAndHistToGlobalErrors
#[test]
#[ignore = "go-parity-gap: MergePartTopNAndHistToGlobal (combined partition TopN+histogram global merge) is not ported to tidb-stats; only the TopN-only merge lives in global_topn.rs"]
fn source_merge_part_topn_and_hist_to_global_errors() {
    unreachable!("gated by go-parity-gap ignore")
}

// merge_global_cases_test.go::TestMergePartTopNAndHistToGlobalVirtualHistChunking
#[test]
#[ignore = "go-parity-gap: MergePartTopNAndHistToGlobal combined merge (incl. virtual-histogram chunking) is not ported to tidb-stats"]
fn source_merge_part_topn_and_hist_to_global_virtual_hist_chunking() {
    unreachable!("gated by go-parity-gap ignore")
}

// merge_global_cases_test.go::TestMergePartTopNAndHistToGlobalSingletonFilter
#[test]
#[ignore = "go-parity-gap: MergePartTopNAndHistToGlobal combined merge (incl. singleton filtering) is not ported to tidb-stats"]
fn source_merge_part_topn_and_hist_to_global_singleton_filter() {
    unreachable!("gated by go-parity-gap ignore")
}

// merge_global_types_test.go::TestMergeRebuildsNonPromotedTopNAcrossTypes
#[test]
#[ignore = "go-parity-gap: drives MergePartTopNAndHistToGlobal across the full column-type matrix (codec.EncodeKey + Datum.Compare per type); the combined merge is not ported to tidb-stats"]
fn source_merge_rebuilds_non_promoted_topn_across_types() {
    unreachable!("gated by go-parity-gap ignore")
}

// merge_global_types_test.go::TestMergeIndexPathAcrossTypes
#[test]
#[ignore = "go-parity-gap: drives MergePartTopNAndHistToGlobal's index path across encoded-bound types; the combined merge is not ported to tidb-stats"]
fn source_merge_index_path_across_types() {
    unreachable!("gated by go-parity-gap ignore")
}

// merge_global_types_test.go::TestMergeAggregatesBothValuesAcrossTypes
#[test]
#[ignore = "go-parity-gap: stresses TopN-stream ordering inside MergePartTopNAndHistToGlobal; the combined merge is not ported to tidb-stats"]
fn source_merge_aggregates_both_values_across_types() {
    unreachable!("gated by go-parity-gap ignore")
}

// merge_global_types_test.go::TestTopNOrderingMatchesDatumOrder
#[test]
#[ignore = "go-parity-gap: pins sortTopNEntries' decoded-value ordering inside MergePartTopNAndHistToGlobal; the combined merge is not ported to tidb-stats"]
fn source_topn_ordering_matches_datum_order() {
    unreachable!("gated by go-parity-gap ignore")
}

// merge_global_types_test.go::TestTypeMatrixCoversAllColumnTypes
#[test]
#[ignore = "go-parity-gap: guards the type matrix consumed by MergePartTopNAndHistToGlobal fixtures; the combined merge and its FieldType registry are not ported to tidb-stats"]
fn source_type_matrix_covers_all_column_types() {
    unreachable!("gated by go-parity-gap ignore")
}

// sample_test.go::TestWeightedSampling
#[test]
#[ignore = "go-parity-gap: RowSampleBuilder over a sqlexec.RecordSet (reservoir sampling with Chernoff-bound frequency assertions) is not ported; only reservoir slot primitives live in weighted_reservoir.rs"]
fn source_weighted_sampling() {
    unreachable!("gated by go-parity-gap ignore")
}

// sample_test.go::TestDistributedWeightedSampling
#[test]
#[ignore = "go-parity-gap: distributed reservoir merge via RowSampleBuilder over RecordSets plus NewReservoirRowSampleCollector is not ported"]
fn source_distributed_weighted_sampling() {
    unreachable!("gated by go-parity-gap ignore")
}

// sample_test.go::TestSampleSerial (+ SubTestCollectColumnStats,
// SubTestMergeSampleCollector, SubTestCollectorProtoConversion)
#[test]
#[ignore = "go-parity-gap: SampleBuilder over a sqlexec.RecordSet (column-stats collection, collector merge, SampleCollector proto conversion) is not ported; row-sample protos live in row_sample_collector.rs but the RecordSet-driven path does not"]
fn source_sample_serial() {
    unreachable!("gated by go-parity-gap ignore")
}
