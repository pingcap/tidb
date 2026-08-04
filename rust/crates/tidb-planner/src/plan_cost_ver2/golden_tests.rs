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

//! The GOLDEN COST TABLE: every `estCost` TiDB prints in
//! `tests/integrationtest/r/planner/core/plan_cost_ver2.result`, rebuilt
//! operator by operator from [`super`].
//!
//! # Why these numbers are an oracle and not a fit
//!
//! `explain format='verbose'` prints Go's own per-operator ver2 cost, and the
//! recorded file fixes the session (`tidb_distsql_scan_concurrency`,
//! `tidb_executor_concurrency` and `tidb_index_lookup_concurrency` are pinned
//! to 1 for the middle block, defaults elsewhere) and the statistics (a
//! 100-row `analyze`d table, and a pseudo-stats pair for the index-join
//! block). Nothing here is tuned: each case supplies the rows Go's
//! `getCardinality` would have read -- which the recording ALSO prints as
//! `estRows` -- and the row sizes derived independently by
//! [`crate::cardinality::row_size`], then asserts the printed cost.
//!
//! Two inputs are not printed by `EXPLAIN` and are therefore MEASURED from
//! the recording rather than derived, and both are called out at their case:
//! the costing child of an operator that a later rule re-parented, and
//! `len(PhysicalIndexJoin.LeftJoinKeys)`.

use super::*;
use crate::cardinality::row_size::{RowSizeColumn, RowSizeColumnStats, RowSizeType};
use crate::cost_usage::COST_FLAG_TRACE;

/// A child cost taken verbatim from a recording, expressed through the source
/// constructor so no test-only way to build a [`CostVer2`] is introduced.
fn recorded_child(cost: f64) -> CostVer2 {
    new_cost_ver2(
        None,
        &CostVer2Factor::new("recorded_child", 1.0),
        cost,
        String::new,
    )
}

/// How `EXPLAIN` renders a cost.
fn printed(cost: &CostVer2) -> String {
    format!("{:.2}", cost.value())
}

/// An analyzed 8-byte integer column of the 100-row table `t`.
fn int_col(rows: f64) -> RowSizeColumn {
    RowSizeColumn::with_stats(
        RowSizeColumnStats::new(RowSizeType::Long, rows as i64 * 8, 0, rows, false),
        8.0,
    )
}

/// The clustered handle column of `t`.
fn handle_col(rows: f64) -> RowSizeColumn {
    RowSizeColumn::with_stats(
        RowSizeColumnStats::new(RowSizeType::Long, rows as i64 * 8, 0, rows, true),
        8.0,
    )
}

/// An `int` column with no loaded statistics (the pseudo tables `t1`/`t2`).
fn pseudo_int_col() -> RowSizeColumn {
    RowSizeColumn::without_stats(8.0)
}

/// A `sum(...)` output column: a decimal, whose chunk width is 40.
fn decimal_col() -> RowSizeColumn {
    RowSizeColumn::without_stats(40.0)
}

/// The 100-row analyzed table used by most of the recording.
const T_ROWS: f64 = 100.0;

/// `getAvgRowSize` over `n` analyzed integer columns of `t`.
fn int_row_size(count: usize) -> f64 {
    let columns: Vec<RowSizeColumn> = (0..count).map(|_| int_col(T_ROWS)).collect();
    plan_avg_row_size(&columns, Some((false, T_ROWS as i64)))
}

/// The session block the recording pins to 1 for the scan/join cases.
fn serial_session() -> CostSessionOpts {
    CostSessionOpts {
        distsql_scan_concurrency: 1.0,
        index_lookup_concurrency: 1.0,
        index_lookup_join_concurrency: 1.0,
        projection_concurrency: 1.0,
        hashagg_final_concurrency: 1.0,
        union_concurrency: 1.0,
        ..CostSessionOpts::default()
    }
}

/// The penalty inputs for a scan of the analyzed 100-row `t`.
fn t_penalty(has_index_force: bool) -> TableScanPenaltyInput {
    TableScanPenaltyInput {
        has_range_info: false,
        allow_prefer_range_scan: true,
        pseudo_stats: false,
        analyze_row_count: 100,
        modify_count: 0,
        has_partition_scan: false,
        has_index_force,
    }
}

// ---------------------------------------------------------------------------
// Factors and primitives
// ---------------------------------------------------------------------------

#[test]
fn test_default_factor_values_match_source() {
    let factors = Ver2Factors::default();
    assert_eq!(factors.tikv_scan.value(), 40.70);
    assert_eq!(factors.tikv_desc_scan.value(), 61.05);
    assert_eq!(factors.tiflash_scan.value(), 11.60);
    assert_eq!(factors.tidb_cpu.value(), 49.90);
    assert_eq!(factors.tikv_cpu.value(), 49.90);
    assert_eq!(factors.tiflash_cpu.value(), 2.40);
    assert_eq!(factors.tidb_to_kv_net.value(), 3.96);
    assert_eq!(factors.tidb_to_flash_net.value(), 2.20);
    assert_eq!(factors.tiflash_mpp_net.value(), 1.00);
    assert_eq!(factors.tidb_mem.value(), 0.20);
    assert_eq!(factors.tikv_mem.value(), 0.20);
    assert_eq!(factors.tiflash_mem.value(), 0.05);
    assert_eq!(factors.tidb_disk.value(), 200.00);
    assert_eq!(factors.tidb_request.value(), 6000000.00);
    assert_eq!(factors.tidb_temp.value(), 0.00);
    assert_eq!(factors.inverted_index_search.value(), 139.2);
    // The trace text the recording prints for the scan factor.
    assert_eq!(factors.tikv_scan.to_string(), "tikv_scan_factor(40.7)");
}

#[test]
fn test_task_factor_selection_matches_source() {
    let factors = Ver2Factors::default();
    assert_eq!(factors.task_cpu(TaskType::Root), &factors.tidb_cpu);
    assert_eq!(factors.task_cpu(TaskType::Mpp), &factors.tiflash_cpu);
    assert_eq!(factors.task_cpu(TaskType::CopSingleRead), &factors.tikv_cpu);
    assert_eq!(factors.task_cpu(TaskType::CopMultiRead), &factors.tikv_cpu);
    assert_eq!(factors.task_mem(TaskType::Root), &factors.tidb_mem);
    assert_eq!(factors.task_mem(TaskType::Mpp), &factors.tiflash_mem);
    assert_eq!(factors.task_mem(TaskType::CopSingleRead), &factors.tikv_mem);

    // A descending TiKV scan uses the desc factor; a TiFlash store never does.
    let scan = |desc| factors.task_scan(false, StoreType::TiKv, TaskType::CopSingleRead, desc);
    assert_eq!(scan(false), &factors.tikv_scan);
    assert_eq!(scan(true), &factors.tikv_desc_scan);
    assert_eq!(
        factors.task_scan(false, StoreType::TiFlash, TaskType::CopSingleRead, true),
        &factors.tiflash_scan
    );
    assert_eq!(
        factors.task_scan(false, StoreType::TiKv, TaskType::Mpp, true),
        &factors.tiflash_scan
    );
    // A temporary table short-circuits every one of them to the zero factor.
    assert_eq!(
        factors.task_scan(true, StoreType::TiKv, TaskType::CopSingleRead, true),
        &factors.tidb_temp
    );
    assert_eq!(
        factors.task_net(true, NetOwner::TiDbToTiKv),
        &factors.tidb_temp
    );
    assert_eq!(factors.task_request(true), &factors.tidb_temp);
    assert_eq!(
        factors.task_net(false, NetOwner::TiFlashMpp),
        &factors.tiflash_mpp_net
    );
    assert_eq!(
        factors.task_net(false, NetOwner::TiDbToTiFlash),
        &factors.tidb_to_flash_net
    );
}

#[test]
fn test_num_functions_counts_columns_at_one_percent() {
    assert_eq!(num_functions(&[]), 0.0);
    assert_eq!(num_functions(&[true]), 1.0);
    assert_eq!(num_functions(&[false]), 0.01);
    assert_eq!(num_functions(&[true, false]), 1.01);
}

#[test]
fn test_cardinality_floors_non_positive_estimates() {
    assert_eq!(cardinality(0.0), 1.0);
    assert_eq!(cardinality(-3.0), 1.0);
    assert_eq!(cardinality(0.5), 0.5);
    assert_eq!(cardinality(12.0), 12.0);
}

#[test]
fn test_row_sizes_are_derived_not_assumed() {
    // One analyzed 8-byte column costs 8 bytes plus the 8-byte record header.
    assert_eq!(int_row_size(1), 16.0);
    assert_eq!(int_row_size(2), 32.0);
    assert_eq!(int_row_size(3), 48.0);
    // An aggregate output has no HistColl, so only the type width counts.
    assert_eq!(plan_avg_row_size(&[decimal_col()], None), 40.0);
    // Pseudo statistics keep the record header.
    assert_eq!(
        plan_avg_row_size(&[pseudo_int_col()], Some((true, 10000))),
        16.0
    );
}

// ---------------------------------------------------------------------------
// TopN: `select /*+ limit_to_cop() */ * from t where a=1 order by a limit N`
// over the one-row table `t (a int)`, with DEFAULT concurrencies.
// ---------------------------------------------------------------------------

/// Rebuilds the recorded TopN plan and returns `(cop TopN, root TopN)`.
fn topn_plan(limit: u64) -> (String, String) {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let session = CostSessionOpts::default();
    // `t (a int)`, one analyzed row: the scan reads `a` and `_tidb_rowid`.
    let scan_row_size = plan_avg_row_size(&[int_col(1.0), handle_col(1.0)], Some((false, 1)));
    assert_eq!(scan_row_size, 32.0);
    // Everything above the scan sees only `a`.
    let visible_row_size = plan_avg_row_size(&[int_col(1.0)], Some((false, 1)));
    assert_eq!(visible_row_size, 16.0);

    let scan = table_scan_cost(
        None,
        TableScanInput {
            rows: 1.0,
            row_size: scan_row_size,
            is_child_of_inl: None,
            has_full_range_scan: true,
            penalty: TableScanPenaltyInput {
                analyze_row_count: 1,
                allow_prefer_range_scan: true,
                ..TableScanPenaltyInput::default()
            },
        },
        &factors.tikv_scan,
        &vars,
    );
    assert_eq!(printed(&scan), "203.50");

    let selection = selection_cost(None, 1.0, &[true], &factors.tikv_cpu, &scan);
    assert_eq!(printed(&selection), "253.40");

    let cop_topn = top_n_cost(
        None,
        1.0,
        (limit, 0),
        visible_row_size,
        &[false],
        (&factors.tikv_cpu, &factors.tikv_mem, vars.topn),
        &selection,
    );
    let reader = reader_cost(
        None,
        1.0,
        visible_row_size,
        &factors.tidb_to_kv_net,
        session.distsql_scan_concurrency,
        &cop_topn,
        vars.table_reader,
    );
    let root_topn = top_n_cost(
        None,
        1.0,
        (limit, 0),
        visible_row_size,
        &[false],
        (&factors.tidb_cpu, &factors.tidb_mem, vars.topn),
        &reader,
    );
    (
        format!("{} {}", printed(&cop_topn), printed(&reader)),
        printed(&root_topn),
    )
}

#[test]
fn test_golden_topn_limit_one() {
    let (below, root) = topn_plan(1);
    assert_eq!(below, "256.60 21.33");
    assert_eq!(root, "24.53");
}

#[test]
fn test_golden_topn_limit_one_billion_uses_the_hundred_row_floor() {
    // `n` is clamped to 100 -- neither the billion asked for nor the single
    // row estimated -- which is what makes the two recordings differ.
    let (below, root) = topn_plan(1_000_000_000);
    assert_eq!(below, "904.93 64.55");
    assert_eq!(root, "716.08");
}

// ---------------------------------------------------------------------------
// Single-table reads over the analyzed `t (a int primary key, b int, c int,
// key(b))`, with every concurrency pinned to 1.
// ---------------------------------------------------------------------------

/// `select /*+ use_index(t, primary) */ a from t where a < ...`.
fn table_range_scan_plan(rows: f64) -> (String, String) {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let session = serial_session();
    let scan = table_scan_cost(
        None,
        TableScanInput {
            rows,
            row_size: int_row_size(3),
            is_child_of_inl: None,
            has_full_range_scan: false,
            penalty: t_penalty(true),
        },
        &factors.tikv_scan,
        &vars,
    );
    let reader = reader_cost(
        None,
        rows,
        int_row_size(1),
        &factors.tidb_to_kv_net,
        session.distsql_scan_concurrency,
        &scan,
        vars.table_reader,
    );
    (printed(&scan), printed(&reader))
}

#[test]
fn test_golden_table_range_scan() {
    assert_eq!(
        table_range_scan_plan(2.0),
        ("454.62".into(), "581.34".into())
    );
    assert_eq!(
        table_range_scan_plan(11.0),
        ("2500.39".into(), "3197.35".into())
    );
    assert_eq!(
        table_range_scan_plan(100.0),
        ("22730.80".into(), "29066.80".into())
    );
}

/// `select /*+ use_index(t, b) */ b from t where b < ...`.
fn index_range_scan_plan(rows: f64) -> (String, String) {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let session = serial_session();
    let scan = index_scan_cost(
        None,
        rows,
        int_row_size(1),
        &factors.tikv_scan,
        vars.index_scan,
        // The tie-breaker is deliberately below the printed precision.
        Some(1),
    );
    let reader = reader_cost(
        None,
        rows,
        int_row_size(1),
        &factors.tidb_to_kv_net,
        session.distsql_scan_concurrency,
        &scan,
        vars.index_reader,
    );
    (printed(&scan), printed(&reader))
}

#[test]
fn test_golden_index_range_scan() {
    assert_eq!(
        index_range_scan_plan(2.0),
        ("325.60".into(), "452.32".into())
    );
    assert_eq!(
        index_range_scan_plan(11.0),
        ("1790.80".into(), "2487.76".into())
    );
    assert_eq!(
        index_range_scan_plan(100.0),
        ("16280.00".into(), "22616.00".into())
    );
}

/// `select /*+ use_index(t, primary) */ <cols> from t`: a full scan whose
/// `USE INDEX` hint triggers the 1000-row penalty.
fn table_full_scan_forced() -> CostVer2 {
    table_scan_cost(
        None,
        TableScanInput {
            rows: T_ROWS,
            row_size: int_row_size(3),
            is_child_of_inl: None,
            has_full_range_scan: true,
            penalty: t_penalty(true),
        },
        &Ver2Factors::default().tikv_scan,
        &CostFactorVars::default(),
    )
}

#[test]
fn test_golden_full_scan_penalty_is_charged_only_when_forced() {
    // With the hint: scan(100 rows) + scan(1000 penalty rows).
    assert_eq!(printed(&table_full_scan_forced()), "250038.77");
    // The same scan without `USE INDEX` -- the join cases below -- pays none.
    let unforced = table_scan_cost(
        None,
        TableScanInput {
            rows: T_ROWS,
            row_size: int_row_size(3),
            is_child_of_inl: None,
            has_full_range_scan: true,
            penalty: t_penalty(false),
        },
        &Ver2Factors::default().tikv_scan,
        &CostFactorVars::default(),
    );
    assert_eq!(printed(&unforced), "22730.80");
}

#[test]
fn test_golden_table_reader_width_tracks_the_projected_columns() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let scan = table_full_scan_forced();
    for (columns, expected) in [(1, "256374.77"), (2, "262710.77"), (3, "269046.77")] {
        let reader = reader_cost(
            None,
            T_ROWS,
            int_row_size(columns),
            &factors.tidb_to_kv_net,
            1.0,
            &scan,
            vars.table_reader,
        );
        assert_eq!(printed(&reader), expected, "{columns} columns");
    }
}

#[test]
fn test_golden_selection_cost_is_linear_in_the_conditions() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let scan = table_full_scan_forced();
    let expected = [
        (1, "255028.77", "260097.57"),
        (2, "260018.77", "265087.57"),
        (3, "265008.77", "270077.57"),
    ];
    for (conditions, expected_selection, expected_reader) in expected {
        let filters = vec![true; conditions];
        let selection = selection_cost(None, T_ROWS, &filters, &factors.tikv_cpu, &scan);
        assert_eq!(printed(&selection), expected_selection);
        // The selection cuts the estimate to 80 rows before the network hop.
        let reader = reader_cost(
            None,
            80.0,
            int_row_size(1),
            &factors.tidb_to_kv_net,
            1.0,
            &selection,
            vars.table_reader,
        );
        assert_eq!(printed(&reader), expected_reader);
    }
}

#[test]
fn test_golden_projection_cost_is_linear_in_the_expressions() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let reader = reader_cost(
        None,
        T_ROWS,
        int_row_size(1),
        &factors.tidb_to_kv_net,
        1.0,
        &table_full_scan_forced(),
        vars.table_reader,
    );
    for (exprs, expected) in [(1, "261364.77"), (2, "266354.77"), (3, "271344.77")] {
        let projection = projection_cost(
            None,
            T_ROWS,
            &vec![true; exprs],
            &factors.tidb_cpu,
            1.0,
            &reader,
        );
        assert_eq!(printed(&projection), expected, "{exprs} expressions");
    }
}

// ---------------------------------------------------------------------------
// Aggregates
// ---------------------------------------------------------------------------

/// The `TableReader` both aggregate blocks read from: `a` and `b`.
fn agg_input_reader() -> CostVer2 {
    reader_cost(
        None,
        T_ROWS,
        int_row_size(2),
        &Ver2Factors::default().tidb_to_kv_net,
        1.0,
        &table_full_scan_forced(),
        CostFactorVars::default().table_reader,
    )
}

#[test]
fn test_golden_hash_agg() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let reader = agg_input_reader();
    assert_eq!(printed(&reader), "262710.77");

    // (aggregate count, group-by items, expected cost). The group-by items
    // are `b`, `b+1`, `b+2`: one column and then scalar functions.
    let cases: [(usize, &[bool], &str); 5] = [
        (1, &[false], "290007.67"),
        (2, &[false], "295797.67"),
        (3, &[false], "301587.67"),
        (1, &[false, true], "304977.67"),
        (1, &[false, true, true], "319947.67"),
    ];
    for (num_aggs, group_items, expected) in cases {
        let output_columns: Vec<RowSizeColumn> = (0..num_aggs).map(|_| decimal_col()).collect();
        let cost = hash_agg_cost(
            None,
            HashAggInput {
                input_rows: T_ROWS,
                output_rows: T_ROWS,
                output_row_size: plan_avg_row_size(&output_columns, None),
                num_agg_funcs: num_aggs,
                // The child is a `TableReader`, so a StreamAgg alternative
                // existed and the hash table's memory IS charged.
                child_can_provide_order: true,
            },
            group_items,
            (&factors.tidb_cpu, &factors.tidb_mem, vars.hash_agg),
            1.0,
            TaskType::Root,
            &reader,
        );
        assert_eq!(printed(&cost), expected, "{num_aggs} aggs {group_items:?}");
    }
}

#[test]
fn test_golden_hash_agg_memory_penalty_is_conditional() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let input = HashAggInput {
        input_rows: T_ROWS,
        output_rows: T_ROWS,
        output_row_size: 40.0,
        num_agg_funcs: 1,
        child_can_provide_order: false,
    };
    let cost = hash_agg_cost(
        None,
        input,
        &[false],
        (&factors.tidb_cpu, &factors.tidb_mem, vars.hash_agg),
        1.0,
        TaskType::Root,
        &agg_input_reader(),
    );
    // 800.00 cheaper than the recorded 290007.67: exactly the hash memory
    // term `1 * 100 * 40 * 0.20` the ordered child would have paid.
    assert_eq!(printed(&cost), "289207.67");
}

#[test]
fn test_golden_sort_and_stream_agg() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let session = serial_session();
    let reader = agg_input_reader();

    let sort = sort_cost(
        None,
        (T_ROWS, int_row_size(2)),
        &[false],
        (&factors, &vars),
        &session,
        TaskType::Root,
        &reader,
    );
    assert_eq!(printed(&sort), "296503.61");

    // MEASURED, not printed: the StreamAgg was costed over the Sort, before
    // the recorded `Projection_17` was injected between them. The recorded
    // projection cost is `Sort + projCost`, so both print 301543.51.
    let projection = projection_cost(None, T_ROWS, &[true, false], &factors.tidb_cpu, 1.0, &sort);
    assert_eq!(printed(&projection), "301543.51");

    for (num_aggs, expected) in [(1, "301543.51"), (2, "306533.51"), (3, "311523.51")] {
        let cost = stream_agg_cost(
            None,
            T_ROWS,
            num_aggs,
            &[false],
            (&factors.tidb_cpu, vars.stream_agg),
            &sort,
        );
        assert_eq!(printed(&cost), expected, "{num_aggs} aggs");
    }
}

// ---------------------------------------------------------------------------
// IndexLookUp: `select /*+ use_index(t, b) */ * from t where b < ...`
// ---------------------------------------------------------------------------

#[test]
fn test_golden_index_lookup_reader() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let session = serial_session();
    // The index side is read as an encoded key in chunk format: `b` plus the
    // handle, 8 bytes each plus an eighth of a byte of null bitmap.
    let index_row_size = crate::cardinality::row_size::get_avg_row_size(
        &[int_col(T_ROWS), handle_col(T_ROWS)],
        false,
        T_ROWS as i64,
        true,
        false,
        true,
    );
    assert_eq!(index_row_size, 16.25);
    let table_row_size = crate::cardinality::row_size::get_avg_row_size(
        &[int_col(T_ROWS), int_col(T_ROWS), int_col(T_ROWS)],
        false,
        T_ROWS as i64,
        false,
        false,
        true,
    );
    assert_eq!(table_row_size, 24.375);

    let cases = [
        (2.0, "407.00", "454.62", "20483.17"),
        (11.0, "2238.50", "2500.39", "112657.41"),
        (100.0, "20350.00", "22730.80", "1024158.30"),
    ];
    for (rows, expected_index, expected_table, expected_total) in cases {
        // The index scan reads `b` and the handle: two analyzed columns.
        let index_scan = index_scan_cost(
            None,
            rows,
            int_row_size(2),
            &factors.tikv_scan,
            vars.index_scan,
            Some(1),
        );
        assert_eq!(printed(&index_scan), expected_index);
        // The row-id scan is the ONE table scan that keeps its raw row count
        // and row size and skips the penalty, because `isChildOfINL` is set.
        let table_scan = table_scan_cost(
            None,
            TableScanInput {
                rows,
                row_size: int_row_size(3),
                is_child_of_inl: Some(true),
                has_full_range_scan: true,
                penalty: t_penalty(true),
            },
            &factors.tikv_scan,
            &vars,
        );
        assert_eq!(printed(&table_scan), expected_table);

        let cost = index_lookup_reader_cost(
            None,
            IndexLookUpInput {
                index_rows: rows,
                table_rows: rows,
                index_row_size,
                table_row_size,
                pushed_limit: None,
                expected_cnt: f64::MAX,
            },
            (&index_scan, &table_scan),
            (&factors, &vars),
            &session,
            TaskType::Root,
        );
        assert_eq!(printed(&cost), expected_total, "{rows} rows");
    }
}

// ---------------------------------------------------------------------------
// HashJoin: `select /*+ hash_join_build(tN) */ * from t t1, t t2 ...`
// ---------------------------------------------------------------------------

/// The two recorded join inputs: an 11-row range scan and a 100-row full scan,
/// each behind a `not(isnull(b))` selection and a `TableReader`.
fn join_inputs() -> (CostVer2, CostVer2) {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let side = |rows: f64, full_range: bool| {
        let scan = table_scan_cost(
            None,
            TableScanInput {
                rows,
                row_size: int_row_size(3),
                is_child_of_inl: None,
                has_full_range_scan: full_range,
                penalty: t_penalty(false),
            },
            &factors.tikv_scan,
            &vars,
        );
        let selection = selection_cost(None, rows, &[true], &factors.tikv_cpu, &scan);
        reader_cost(
            None,
            rows,
            int_row_size(3),
            &factors.tidb_to_kv_net,
            1.0,
            &selection,
            vars.table_reader,
        )
    };
    (side(11.0, false), side(T_ROWS, true))
}

#[test]
fn test_golden_hash_join() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let (small, large) = join_inputs();
    assert_eq!(printed(&small), "5140.17");
    assert_eq!(printed(&large), "46728.80");

    let join = |build: &CostVer2, probe: &CostVer2, rows: (f64, f64), keys: usize| {
        printed(&hash_join_cost(
            None,
            HashJoinInput {
                build_rows: rows.0,
                probe_rows: rows.1,
                build_row_size: int_row_size(3),
                num_build_keys: keys,
                num_probe_keys: keys,
                tidb_concurrency: 1.0,
            },
            (&[], &[]),
            (&factors.tidb_cpu, &factors.tidb_mem, vars.hash_join),
            TaskType::Root,
            (build, probe),
        ))
    };
    // `hash_join_build(t1)`: the 11-row side builds.
    assert_eq!(join(&small, &large, (11.0, T_ROWS), 1), "64549.37");
    // `hash_join_build(t2)`: the 100-row side builds -- strictly worse.
    assert_eq!(join(&large, &small, (T_ROWS, 11.0), 1), "65403.77");
    // Both sides full, one key, then two keys.
    assert_eq!(join(&large, &large, (T_ROWS, T_ROWS), 1), "115874.59");
    assert_eq!(join(&large, &large, (T_ROWS, T_ROWS), 2), "125854.59");
}

#[test]
fn test_golden_hash_join_over_a_derived_table_from_a_live_binary() {
    // A SECOND oracle, from `gorun` rather than the recorded file: the
    // statement at `join_reorder_through_projection.result:1169`, whose
    // recorded plan is an `IndexJoin` and whose `gorun` plan is a `HashJoin`
    // because the two environments load different statistics. What is being
    // checked here is the ARITHMETIC, which both environments share.
    //
    //   HashJoin_16          2.00  2492.68  root
    //   |-TableReader_30(B)  2.00    44.03  root
    //   `-Projection_51(P)   4.80   643.44  root
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let build = recorded_child(44.03);
    let probe = recorded_child(643.44);
    let cost = hash_join_cost(
        None,
        HashJoinInput {
            build_rows: 2.0,
            probe_rows: 4.8,
            // `expr_small(a int primary key, b int)`: two 8-byte columns.
            build_row_size: 32.0,
            num_build_keys: 1,
            num_probe_keys: 1,
            // `tidb_executor_concurrency` at its default of 5.
            tidb_concurrency: 5.0,
        },
        (&[], &[]),
        (&factors.tidb_cpu, &factors.tidb_mem, vars.hash_join),
        TaskType::Root,
        (&build, &probe),
    );
    assert_eq!(printed(&cost), "2492.68");
}

// ---------------------------------------------------------------------------
// IndexJoin: `select /*+ tidb_inlj(t1, t2) */ * from t1, t2 where t1.a=t2.a`
// over two pseudo-stats tables, with DEFAULT concurrencies.
// ---------------------------------------------------------------------------

#[test]
fn test_golden_index_join_and_its_double_read_penalty() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let pseudo_row_size = plan_avg_row_size(&[pseudo_int_col()], Some((true, 10000)));
    assert_eq!(pseudo_row_size, 16.0);

    // Build side: a full index scan of `t1`, 9990 rows after `not(isnull)`.
    let build_scan = index_scan_cost(
        None,
        9990.0,
        pseudo_row_size,
        &factors.tikv_scan,
        vars.index_scan,
        None,
    );
    assert_eq!(printed(&build_scan), "1626372.00");
    let build = reader_cost(
        None,
        9990.0,
        pseudo_row_size,
        &factors.tidb_to_kv_net,
        15.0,
        &build_scan,
        vars.index_reader,
    );
    assert_eq!(printed(&build), "150622.56");

    // Probe side: the recorded `estRows` are TOTALS; the cost model reads the
    // per-outer-row estimate, which is the total divided by the 9990 build
    // rows -- 12500/9990 for the scan and 12487.5/9990 = 1.25 after the
    // selection.
    let probe_scan_rows = 12500.0 / 9990.0;
    let probe_rows_one = 12487.5 / 9990.0;
    let probe_scan = index_scan_cost(
        None,
        probe_scan_rows,
        pseudo_row_size,
        &factors.tikv_scan,
        vars.index_scan,
        None,
    );
    assert_eq!(printed(&probe_scan), "203.70");
    let probe_selection = selection_cost(
        None,
        probe_scan_rows,
        &[true],
        &factors.tikv_cpu,
        &probe_scan,
    );
    assert_eq!(printed(&probe_selection), "266.14");
    let probe = reader_cost(
        None,
        probe_rows_one,
        pseudo_row_size,
        &factors.tidb_to_kv_net,
        15.0,
        &probe_selection,
        vars.index_reader,
    );
    assert_eq!(printed(&probe), "23.02");

    // MEASURED, not printed: `PhysicalIndexJoin` leaves `LeftJoinKeys` and
    // `RightJoinKeys` empty -- only the hash and merge join constructors fill
    // them -- so the hash table pays no per-key cost.
    let input = IndexJoinInput {
        build_rows: 9990.0,
        build_row_size: pseudo_row_size,
        probe_rows_one,
        probe_row_size: pseudo_row_size,
        num_right_join_keys: 0,
        num_left_join_keys: 0,
        num_ranges: 1.0,
        is_semi_join: false,
        kind: IndexJoinKind::IndexJoin,
    };
    for (rate, expected) in [
        (0.0, "5277413.38"),
        (0.5, "250791653.38"),
        (1.0, "496305893.38"),
    ] {
        let session = CostSessionOpts {
            index_join_double_read_penalty_cost_rate: rate,
            ..CostSessionOpts::default()
        };
        let cost = index_join_cost(
            None,
            input,
            (&[], &[]),
            (&factors, &vars),
            &session,
            TaskType::Root,
            (&build, &probe),
        );
        assert_eq!(printed(&cost), expected, "penalty rate {rate}");
    }
}

// ---------------------------------------------------------------------------
// The cost TRACE, from `explain analyze format='true_card_cost'`
// ---------------------------------------------------------------------------

#[test]
fn test_golden_true_card_cost_trace() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let option = PlanCostOption::new().with_cost_flag(COST_FLAG_TRACE);
    // `t (a int)` with pseudo statistics, and the true cardinality of a
    // never-executed scan: zero rows, floored to one, plus the 1000-row
    // penalty a pseudo-stats full scan always pays.
    let row_size = plan_avg_row_size(&[pseudo_int_col(), pseudo_int_col()], Some((true, 10000)));
    assert_eq!(row_size, 32.0);
    let scan = table_scan_cost(
        Some(&option),
        TableScanInput {
            rows: 0.0,
            row_size,
            is_child_of_inl: None,
            has_full_range_scan: true,
            penalty: TableScanPenaltyInput {
                allow_prefer_range_scan: true,
                pseudo_stats: true,
                ..TableScanPenaltyInput::default()
            },
        },
        &factors.tikv_scan,
        &vars,
    );
    assert_eq!(printed(&scan), "203703.50");
    assert_eq!(
        scan.trace().unwrap().formula(),
        "((scan(1*logrowsize(32)*tikv_scan_factor(40.7))) \
         + (scan(1000*logrowsize(32)*tikv_scan_factor(40.7))))*1.00"
    );

    let selection = selection_cost(Some(&option), 0.0, &[true], &factors.tikv_cpu, &scan);
    assert_eq!(printed(&selection), "203703.50");
    assert_eq!(
        selection.trace().unwrap().formula(),
        "(cpu(0*filters(1)*tikv_cpu_factor(49.9))) \
         + (((scan(1*logrowsize(32)*tikv_scan_factor(40.7))) \
         + (scan(1000*logrowsize(32)*tikv_scan_factor(40.7))))*1.00)"
    );

    let reader = reader_cost(
        Some(&option),
        0.0,
        16.0,
        &factors.tidb_to_kv_net,
        15.0,
        &selection,
        vars.table_reader,
    );
    assert_eq!(printed(&reader), "13580.23");
    assert_eq!(
        reader.trace().unwrap().formula(),
        "((((cpu(0*filters(1)*tikv_cpu_factor(49.9))) \
         + (((scan(1*logrowsize(32)*tikv_scan_factor(40.7))) \
         + (scan(1000*logrowsize(32)*tikv_scan_factor(40.7))))*1.00)) \
         + (net(0*rowsize(16)*tidb_kv_net_factor(3.96))))/15.00)*1.00"
    );
}

// ---------------------------------------------------------------------------
// compareTaskCost
// ---------------------------------------------------------------------------

#[test]
fn test_compare_task_cost_tie_direction_and_invalid_tasks() {
    let cheap = TaskPlanCost::valid(10.0);
    let dear = TaskPlanCost::valid(20.0);
    assert!(compare_task_cost(cheap, dear));
    assert!(!compare_task_cost(dear, cheap));
    // A TIE keeps the incumbent: enumeration order, not cost, breaks it.
    assert!(!compare_task_cost(cheap, cheap));
    // An invalid current task never wins, even against an invalid best.
    assert!(!compare_task_cost(TaskPlanCost::invalid(), cheap));
    assert!(!compare_task_cost(
        TaskPlanCost::invalid(),
        TaskPlanCost::invalid()
    ));
    // An invalid best loses to any valid task, however expensive.
    assert!(compare_task_cost(
        TaskPlanCost::valid(f64::MAX / 2.0),
        TaskPlanCost::invalid()
    ));
    assert_eq!(TaskPlanCost::invalid().cost, f64::MAX);
}

// ---------------------------------------------------------------------------
// Source-derived cases: the branches the recording never reaches
//
// Everything above is anchored to a printed TiDB cost. The recording exercises
// neither TiFlash/MPP, nor paging, nor a multi-range index-join probe, nor any
// operand small enough to hit a clamp, so those branches are asserted directly
// against `plan_cost_ver2.go` instead. They are labelled so no reader mistakes
// them for measured output.
// ---------------------------------------------------------------------------

#[test]
fn test_source_session_and_cost_factor_defaults() {
    let session = CostSessionOpts::default();
    assert_eq!(session.distsql_scan_concurrency, 15.0);
    // `ConcurrencyUnset` resolves to `tidb_executor_concurrency`, default 5.
    assert_eq!(session.index_lookup_concurrency, 5.0);
    assert_eq!(session.index_lookup_join_concurrency, 5.0);
    assert_eq!(session.projection_concurrency, 5.0);
    assert_eq!(session.hashagg_final_concurrency, 5.0);
    assert_eq!(session.union_concurrency, 5.0);
    assert_eq!(session.index_lookup_size, 20000.0);
    assert_eq!(session.index_join_batch_size, 25000.0);
    assert_eq!(session.index_join_double_read_penalty_cost_rate, 0.0);
    assert_eq!(session.mem_quota, 0);
    assert!(!session.mpp_enforced);

    // Every `tidb_opt_*_cost_factor` defaults to a no-op multiplier.
    let vars = CostFactorVars::default();
    for factor in [
        vars.index_scan,
        vars.table_row_id_scan,
        vars.table_range_scan,
        vars.table_full_scan,
        vars.table_tiflash_scan,
        vars.index_reader,
        vars.table_reader,
        vars.index_lookup,
        vars.index_merge,
        vars.limit,
        vars.sort,
        vars.topn,
        vars.stream_agg,
        vars.hash_agg,
        vars.merge_join,
        vars.hash_join,
        vars.index_join,
    ] {
        assert_eq!(factor, 1.0);
    }
}

#[test]
fn test_source_scan_clamps_apply_to_every_scan_but_a_row_id_scan() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let input = |is_child_of_inl| TableScanInput {
        rows: 0.4,
        row_size: 1.5,
        is_child_of_inl,
        has_full_range_scan: false,
        penalty: TableScanPenaltyInput::default(),
    };
    // Not a row-id scan: 0.4 rows become 1 and 1.5 bytes become MIN_ROW_SIZE.
    let clamped = table_scan_cost(None, input(None), &factors.tikv_scan, &vars);
    assert_eq!(printed(&clamped), "40.70");
    assert_eq!(
        printed(&table_scan_cost(
            None,
            input(Some(false)),
            &factors.tikv_scan,
            &vars
        )),
        "40.70"
    );
    // A row-id scan keeps both: 0.4 * log2(1.5) * 40.70.
    let raw = table_scan_cost(None, input(Some(true)), &factors.tikv_scan, &vars);
    assert_eq!(printed(&raw), "9.52");
}

#[test]
fn test_source_projection_treats_zero_concurrency_as_serial() {
    let factors = Ver2Factors::default();
    let child = recorded_child(0.0);
    let serial = projection_cost(None, 10.0, &[true], &factors.tidb_cpu, 1.0, &child);
    let unset = projection_cost(None, 10.0, &[true], &factors.tidb_cpu, 0.0, &child);
    assert_eq!(serial.value(), 499.0);
    assert_eq!(unset.value(), serial.value());
}

#[test]
fn test_source_index_join_seeking_cost() {
    let factors = Ver2Factors::default();
    // A single range, or a single build row, is not charged at all.
    assert_eq!(
        index_join_seeking_cost(None, 2.0, 1.0, &factors.tikv_scan).value(),
        0.0
    );
    assert_eq!(
        index_join_seeking_cost(None, 1.0, 5.0, &factors.tikv_scan).value(),
        0.0
    );
    // Otherwise a seek is ten 8-byte rows: 2 * 10 * log2(8) * 3 * 40.70.
    let cost = index_join_seeking_cost(None, 2.0, 3.0, &factors.tikv_scan);
    assert_eq!(cost.value(), 2.0 * 10.0 * 3.0 * 3.0 * 40.70);
    assert_eq!(INDEX_JOIN_BATCH_RATIO, 6.0);
    assert_eq!(PAGING_THRESHOLD, 960);
    assert_eq!(TIFLASH_STARTUP_ROW_PENALTY, 10000.0);
    assert_eq!(MAX_PENALTY_ROW_COUNT, 1000.0);
    assert_eq!(MIN_NUM_ROWS, 1.0);
    assert_eq!(MIN_ROW_SIZE, 2.0);
    assert_eq!(printed(&cost), "7326.00");
}

#[test]
fn test_source_index_scan_tie_breaker_separates_equal_indexes() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let scan = |index_id| {
        index_scan_cost(
            None,
            10.0,
            16.0,
            &factors.tikv_scan,
            vars.index_scan,
            index_id,
        )
        .value()
    };
    let base = scan(None);
    // Only the last two digits of the index id count, at one part in 1e6.
    assert_eq!(scan(Some(0)), base);
    assert!((scan(Some(137)) - base - 37.0 / 1_000_000.0).abs() < 1e-12);
    // Only the last two digits: 173 and 73 agree, 173 and 23 do not.
    assert_eq!(scan(Some(173)), scan(Some(73)));
    assert!(scan(Some(173)) != scan(Some(23)));
    // ...and it is far below what EXPLAIN prints, by design.
    assert_eq!(format!("{base:.2}"), format!("{:.2}", scan(Some(99))));
}

#[test]
fn test_source_index_lookup_paging_discount_and_its_threshold() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let session = CostSessionOpts::default();
    let child = recorded_child(1000.0);
    let cost = |expected_cnt| {
        index_lookup_reader_cost(
            None,
            IndexLookUpInput {
                index_rows: 10.0,
                table_rows: 10.0,
                index_row_size: 16.0,
                table_row_size: 16.0,
                pushed_limit: None,
                expected_cnt,
            },
            (&child, &child),
            (&factors, &vars),
            &session,
            TaskType::Root,
        )
        .value()
    };
    let full = cost(961.0);
    // At the paging threshold and below, the whole lookup costs 60%.
    assert_eq!(cost(PAGING_THRESHOLD as f64), full * 0.6);
    assert_eq!(cost(1.0), full * 0.6);
    // Above it, and at the "no expected count" sentinel, nothing is deducted.
    assert_eq!(cost(0.0), full);
    assert_eq!(cost(f64::MAX), full);
}

#[test]
fn test_source_index_merge_reader_biases_a_pushed_down_limit() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let side = |rows: f64| IndexMergeSide {
        rows,
        row_size: 16.0,
        child_cost: recorded_child(100.0),
    };
    let table = side(20.0);
    let indexes = [side(30.0), side(40.0)];
    let plain = index_merge_reader_cost(
        None,
        Some(&table),
        &indexes,
        &factors.tidb_to_kv_net,
        (1.0, false, &vars),
    );
    // Table side plus BOTH partial index sides, each child + net.
    assert_eq!(
        plain.value(),
        3.0 * 100.0 + (20.0 + 30.0 + 40.0) * 16.0 * 3.96
    );
    // The pushed-down limit earns a 1% discount over the identical plan that
    // keeps the limit outside.
    let pushed = index_merge_reader_cost(
        None,
        Some(&table),
        &indexes,
        &factors.tidb_to_kv_net,
        (1.0, true, &vars),
    );
    assert_eq!(pushed.value(), plain.value() * 0.99);
    // With no table side, only the index sides are summed.
    let index_only = index_merge_reader_cost(
        None,
        None,
        &indexes,
        &factors.tidb_to_kv_net,
        (1.0, false, &vars),
    );
    assert_eq!(index_only.value(), 2.0 * 100.0 + 70.0 * 16.0 * 3.96);
}

#[test]
fn test_source_merge_join_charges_other_conditions_to_both_sides() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let zero = recorded_child(0.0);
    let cost = merge_join_cost(
        None,
        (2.0, 3.0),
        (&[], &[], &[true]),
        (0, 0),
        (&factors.tidb_cpu, vars.merge_join),
        (&zero, &zero),
    );
    // (2 + 3) rows * one function * the TiDB CPU factor, and no group cost
    // because neither side has a join key.
    assert_eq!(cost.value(), 5.0 * 49.90);
    // Join keys are COLUMNS, so each costs `numFunctions`' 0.01 per row.
    let keyed = merge_join_cost(
        None,
        (2.0, 3.0),
        (&[], &[], &[]),
        (1, 2),
        (&factors.tidb_cpu, vars.merge_join),
        (&zero, &zero),
    );
    assert_eq!(keyed.value(), (2.0 * 0.01 + 3.0 * 0.02) * 49.90);
    // Both sides are floored at one row.
    let tiny = merge_join_cost(
        None,
        (0.0, 0.0),
        (&[true], &[true], &[]),
        (0, 0),
        (&factors.tidb_cpu, vars.merge_join),
        (&zero, &zero),
    );
    assert_eq!(tiny.value(), 2.0 * 49.90);
}

#[test]
fn test_source_tiflash_and_mpp_branches() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    // A TiFlash scan always pays for 10000 startup rows on top of its own.
    let scan = tiflash_table_scan_cost(None, 1.0, 4.0, &factors.tiflash_scan, 1.0);
    assert_eq!(
        printed(&scan),
        format!("{:.2}", (1.0 + TIFLASH_STARTUP_ROW_PENALTY) * 2.0 * 11.60)
    );
    assert_eq!(printed(&scan), "232023.20");

    // An MPP hash join divides ALL four cpu terms by the empirical 3, and
    // pays no start cost; a TiDB one divides only the two probe terms.
    let zero = recorded_child(0.0);
    let input = HashJoinInput {
        build_rows: 10.0,
        probe_rows: 20.0,
        build_row_size: 16.0,
        num_build_keys: 1,
        num_probe_keys: 1,
        tidb_concurrency: 3.0,
    };
    let mpp = hash_join_cost(
        None,
        input,
        (&[], &[]),
        (&factors.tiflash_cpu, &factors.tiflash_mem, vars.hash_join),
        TaskType::Mpp,
        (&zero, &zero),
    );
    let build_hash = 10.0 * 2.40 + 10.0 * 16.0 * 0.05 + 10.0 * 2.40;
    let probe_hash = 20.0 * 2.40 + 20.0 * 2.40;
    assert_eq!(MPP_CONCURRENCY, 3.0);
    assert_eq!(mpp.value(), (build_hash + probe_hash) / 3.0);

    // A broadcast exchange multiplies its network cost by the three nodes.
    let point = exchange_receiver_cost(None, 5.0, 16.0, &factors.tiflash_mpp_net, false, &zero);
    let bcast = exchange_receiver_cost(None, 5.0, 16.0, &factors.tiflash_mpp_net, true, &zero);
    assert_eq!(point.value(), 5.0 * 16.0);
    assert_eq!(bcast.value(), point.value() * 3.0);
}

#[test]
fn test_source_remaining_leaf_operators() {
    let factors = Ver2Factors::default();
    let zero = recorded_child(0.0);
    // A point get is pure network; the fast-plan path with no access columns
    // is free.
    assert_eq!(
        point_get_cost(None, 1.0, 16.0, &factors.tidb_to_kv_net, true).value(),
        16.0 * 3.96
    );
    assert_eq!(
        point_get_cost(None, 7.0, 16.0, &factors.tidb_to_kv_net, false).value(),
        0.0
    );
    // A CTE consumer pays only for its own schema columns, at 0.01 each.
    assert_eq!(
        cte_cost(None, 100.0, 3, &factors.tidb_cpu).value(),
        100.0 * 0.03 * 49.90
    );
    // UNION ALL divides the summed children by union concurrency, and an
    // enforced-MPP one is then discounted so it still compares by cost.
    let children = [recorded_child(300.0), recorded_child(200.0)];
    assert_eq!(union_all_cost(&children, 5.0, false).value(), 100.0);
    assert_eq!(MPP_ENFORCED_DISCOUNT, 1_000_000_000.0);
    assert_eq!(
        union_all_cost(&children, 5.0, true).value(),
        100.0 / 1_000_000_000.0
    );
    // Apply re-runs its probe once per build row -- no batching discount.
    let probe = recorded_child(10.0);
    assert_eq!(
        apply_cost(
            None,
            4.0,
            5.0,
            (&[], &[true]),
            &factors.tidb_cpu,
            (&zero, &probe)
        )
        .value(),
        4.0 * 10.0 + 20.0 * 49.90
    );
}

#[test]
fn test_source_sort_spills_only_on_a_root_task_over_quota() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let zero = recorded_child(0.0);
    let spilling = CostSessionOpts {
        mem_quota: 100,
        ..CostSessionOpts::default()
    };
    let sort = |session: &CostSessionOpts, task_type| {
        sort_cost(
            None,
            (10.0, 16.0),
            &[false],
            (&factors, &vars),
            session,
            task_type,
            &zero,
        )
        .value()
    };
    let cpu = 10.0 * 10.0_f64.log2() * 49.90;
    // 10 rows * 16 bytes = 160 > the 100-byte quota, so a root task spills:
    // memory is charged at the quota and the rows are charged to disk.
    assert_eq!(
        sort(&spilling, TaskType::Root),
        cpu + 100.0 * 0.2 + 10.0 * 16.0 * 200.0
    );
    // Only TiDB can spill; a cop task pays the in-memory cost.
    let in_memory = cpu + 10.0 * 16.0 * 0.2;
    assert_eq!(sort(&spilling, TaskType::CopSingleRead), in_memory);
    // ...and so does a root task with no quota set.
    assert_eq!(sort(&CostSessionOpts::default(), TaskType::Root), in_memory);
    // Disabling the temporary-storage variable also disables spilling.
    let no_tmp = CostSessionOpts {
        enable_tmp_storage_on_oom: false,
        ..spilling
    };
    assert_eq!(sort(&no_tmp, TaskType::Root), in_memory);
}

#[test]
fn test_source_index_join_kinds_differ_only_in_the_hash_table() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let zero = recorded_child(0.0);
    let session = CostSessionOpts::default();
    let input = IndexJoinInput {
        build_rows: 10.0,
        build_row_size: 32.0,
        probe_rows_one: 4.0,
        probe_row_size: 16.0,
        num_right_join_keys: 2,
        num_left_join_keys: 1,
        num_ranges: 1.0,
        is_semi_join: false,
        kind: IndexJoinKind::IndexJoin,
    };
    let cost = |kind| {
        index_join_cost(
            None,
            IndexJoinInput { kind, ..input },
            (&[], &[]),
            (&factors, &vars),
            &session,
            TaskType::Root,
            (&zero, &zero),
        )
        .value()
    };
    let shared = 10.0 * 3.0 * 49.90 + 10.0 * 10.0 * 49.90;
    // IndexMergeJoin builds no hash table at all.
    assert_eq!(cost(IndexJoinKind::IndexMergeJoin), shared);
    // IndexHashJoin hashes the BUILD side on the right-hand keys.
    let build_table = 10.0 * 2.0 * 49.90 + 10.0 * 32.0 * 0.2 + 10.0 * 49.90;
    assert_eq!(
        cost(IndexJoinKind::IndexHashJoin),
        shared + build_table / 5.0
    );
    // IndexJoin hashes ALL 40 probe rows on the left-hand keys.
    let probe_table = 40.0 * 1.0 * 49.90 + 40.0 * 16.0 * 0.2 + 40.0 * 49.90;
    assert_eq!(cost(IndexJoinKind::IndexJoin), shared + probe_table / 5.0);
}

#[test]
fn test_source_semi_index_join_pays_for_the_rows_it_cannot_stop_reading() {
    let factors = Ver2Factors::default();
    let vars = CostFactorVars::default();
    let session = CostSessionOpts::default();
    let probe_child = recorded_child(60.0);
    let zero = recorded_child(0.0);
    let input = |probe_rows_one, is_semi_join| IndexJoinInput {
        build_rows: 10.0,
        build_row_size: 16.0,
        probe_rows_one,
        probe_row_size: 16.0,
        num_right_join_keys: 0,
        num_left_join_keys: 0,
        num_ranges: 1.0,
        is_semi_join,
        kind: IndexJoinKind::IndexMergeJoin,
    };
    let cost = |probe_rows_one, is_semi_join| {
        index_join_cost(
            None,
            input(probe_rows_one, is_semi_join),
            (&[], &[]),
            (&factors, &vars),
            &session,
            TaskType::Root,
            (&zero, &probe_child),
        )
        .value()
    };
    let shared = 10.0 * 3.0 * 49.90 + 10.0 * 10.0 * 49.90;
    let probe = 60.0 * 10.0 / INDEX_JOIN_BATCH_RATIO;
    assert_eq!(cost(4.0, false), shared + probe / 5.0);
    // A semi join re-reads the whole key group, so its probe is scaled by the
    // per-key row count...
    assert_eq!(cost(4.0, true), shared + probe * 4.0 / 5.0);
    // ...but only when there IS more than one row per key.
    assert_eq!(cost(1.0, true), cost(1.0, false));
}
