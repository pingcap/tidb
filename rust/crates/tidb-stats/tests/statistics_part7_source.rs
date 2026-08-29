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

use tidb_datatype::{
    BinaryLiteral, CoreTime, Datum, Decimal, MySqlDuration, Time, TimeType,
};
use tidb_stats::enum_range_values;

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
