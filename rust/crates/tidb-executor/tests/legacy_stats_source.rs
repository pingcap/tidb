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

//! Golden `estRows` fixtures for the driver's `legacy_stats` model
//! (formerly `tidb_planner::cardinality::derive_stats`).
//!
//! Every number asserted here was read off a real TiDB `EXPLAIN`, on the exact
//! schema of `tests/integrationtest/t/planner/core/join_reorder_through_projection.test`:
//!
//! ```sql
//! create table tN(a int, b int, c varchar(32), primary key (a), key(b));
//! ```
//!
//! with no `ANALYZE`, so every table carries pseudo statistics
//! (`RealtimeCount` 10000, every column NDV 8000).
//!
//! The recorded `.result` file itself is **not** the oracle: all 94 of its
//! `EXPLAIN`s use `format = 'plan_tree'`, which prints no `estRows` column at
//! all. The oracle is a default-format `EXPLAIN` of the same statements.

use tidb_executor::driver::legacy_stats::{
    calc_join_cum_cost, derive_stats, ColumnId, DeriveStatsContext, DerivedNode, LogicalNode,
    ProjectionExpr,
};
use tidb_planner::cardinality::ndv::GroupNdv;

const T1_A: ColumnId = 1;
const T1_B: ColumnId = 2;
const T1_C: ColumnId = 3;
const T2_A: ColumnId = 11;
const T2_B: ColumnId = 12;
const T2_C: ColumnId = 13;
const T3_A: ColumnId = 21;
const T4_A: ColumnId = 41;
const T5_A: ColumnId = 51;
const DT_KEY_A: ColumnId = 101;
const DT_DOUBLED_B: ColumnId = 102;
const DT_UPPER_C: ColumnId = 103;
const DT2_KEY_A: ColumnId = 111;
const DT2_ADJUSTED: ColumnId = 112;

/// `tidb_opt_join_reorder_threshold = 10`, the value the target statements set.
const THRESHOLD: i32 = 10;

/// A pseudo-statistics base table with every predicate already accounted for by
/// `selectivity`.
fn table(columns: &[ColumnId], selectivity: f64) -> LogicalNode {
    LogicalNode::DataSource {
        realtime_count: 10000.0,
        column_ndvs: columns.iter().map(|id| (*id, 8000.0)).collect(),
        group_ndvs: Vec::new(),
        selectivity,
    }
}

/// `cardinality.EstimateColumnNDV` installs analyzed NDVs in the data-source
/// stats before predicate selectivity is applied.
#[test]
fn a_datasource_preserves_analyzed_column_ndvs() {
    let source = LogicalNode::DataSource {
        realtime_count: 300_000.0,
        column_ndvs: [(T1_A, 10.0), (T1_B, 30_000.0)].into_iter().collect(),
        group_ndvs: Vec::new(),
        selectivity: 0.1,
    };
    let derived = derive_stats(&source, &ctx());
    assert_row_counts(&derived, &[30_000.0]);
    assert!((derived.stats.col_ndvs()[&T1_A] - 1.0).abs() < 1e-9);
    assert!((derived.stats.col_ndvs()[&T1_B] - 3_000.0).abs() < 1e-9);
}

fn join(
    left: LogicalNode,
    right: LogicalNode,
    left_keys: &[ColumnId],
    right_keys: &[ColumnId],
) -> LogicalNode {
    LogicalNode::Join {
        left: Box::new(left),
        right: Box::new(right),
        left_keys: left_keys.to_vec(),
        right_keys: right_keys.to_vec(),
        kind: tidb_executor::driver::legacy_stats::JoinKind::Inner,
    }
}

fn projection(child: LogicalNode, exprs: &[(ColumnId, &[ColumnId])]) -> LogicalNode {
    LogicalNode::Projection {
        child: Box::new(child),
        injected: false,
        exprs: exprs
            .iter()
            .map(|(output, inputs)| ProjectionExpr {
                output: *output,
                inputs: inputs.to_vec(),
                direct_input: (inputs.len() == 1).then_some(inputs[0]),
            })
            .collect(),
    }
}

fn ctx() -> DeriveStatsContext {
    DeriveStatsContext::with_join_reorder_threshold(THRESHOLD)
}

#[track_caller]
fn assert_row_counts(node: &DerivedNode, expected: &[f64]) {
    let actual = node.row_counts();
    assert_eq!(
        actual.len(),
        expected.len(),
        "node count differs: {actual:?} vs {expected:?}"
    );
    for (index, (got, want)) in actual.iter().zip(expected).enumerate() {
        assert!(
            (got - want).abs() < 1e-9,
            "node {index}: got {got}, want {want} (full: {actual:?})"
        );
    }
}

// ---------------------------------------------------------------------------
// Base cases: the simplest shapes, so a later diff can tell a leaf rule from a
// composition rule.
// ---------------------------------------------------------------------------

/// `explain select * from t1` -> `TableFullScan 10000.00`.
#[test]
fn full_scan_of_a_pseudo_table_is_ten_thousand_rows() {
    let derived = derive_stats(&table(&[T1_A, T1_B, T1_C], 1.0), &ctx());
    assert_row_counts(&derived, &[10000.0]);
    assert_eq!(derived.stats.col_ndvs()[&T1_A], 8000.0);
}

/// `explain select * from t1 where c = 'x'` -> `Selection 10.00`, the
/// `1/pseudoEqualRate` selectivity the caller supplies.
#[test]
fn a_datasource_scales_by_the_supplied_selectivity() {
    let derived = derive_stats(&table(&[T1_A, T1_B, T1_C], 1.0 / 1000.0), &ctx());
    assert_row_counts(&derived, &[10.0]);
    // `Scale` re-derives the NDV; at the default skew ratio of 1.0 that is
    // `8000 * 10 / 10000`.
    assert!((derived.stats.col_ndvs()[&T1_C] - 8.0).abs() < 1e-9);
}

/// `explain select * from t1, t2 where t1.a = t2.a` -> `MergeJoin 12500.00`.
#[test]
fn a_two_table_equi_join_is_twelve_thousand_five_hundred_rows() {
    let plan = join(
        table(&[T1_A, T1_B, T1_C], 1.0),
        table(&[T2_A, T2_B, T2_C], 1.0),
        &[T1_A],
        &[T2_A],
    );
    assert_row_counts(&derive_stats(&plan, &ctx()), &[12500.0, 10000.0, 10000.0]);
}

/// Exact composite index NDVs remove Go's multi-key correlation fallback.
/// This is the cardinality contract used by TPCC condition 05's clustered
/// `(warehouse, district, order)` primary-key join.
#[test]
fn a_composite_group_ndv_bounds_an_outer_join_by_its_preserved_side() {
    let left = LogicalNode::DataSource {
        realtime_count: 300_000.0,
        column_ndvs: [(T1_A, 10.0), (T1_B, 10.0), (T1_C, 3_000.0)]
            .into_iter()
            .collect(),
        group_ndvs: vec![GroupNdv {
            columns: vec![T1_A as i64, T1_B as i64, T1_C as i64],
            ndv: 299_264.0,
        }],
        selectivity: 0.1,
    };
    let right = LogicalNode::DataSource {
        realtime_count: 90_000.0,
        column_ndvs: [(T2_A, 10.0), (T2_B, 10.0), (T2_C, 900.0)]
            .into_iter()
            .collect(),
        group_ndvs: vec![GroupNdv {
            columns: vec![T2_A as i64, T2_B as i64, T2_C as i64],
            ndv: 89_168.0,
        }],
        selectivity: 0.1,
    };
    let plan = LogicalNode::Join {
        left: Box::new(left),
        right: Box::new(right),
        left_keys: vec![T1_A, T1_B, T1_C],
        right_keys: vec![T2_A, T2_B, T2_C],
        kind: tidb_executor::driver::legacy_stats::JoinKind::LeftOuter,
    };

    let derived = derive_stats(&plan, &ctx());
    assert_row_counts(&derived, &[30_000.0, 30_000.0, 9_000.0]);
}

/// `explain select * from t1, t2 where t1.b = t2.b` -> `Selection 9990.00`
/// under each side and `HashJoin 12487.50`. The `not(isnull(col))` rewrite is
/// a `0.999` factor *per column*, which is why the join is not `12500`.
#[test]
fn the_derived_not_null_condition_costs_one_thousandth_per_column() {
    let plan = join(
        table(&[T1_A, T1_B, T1_C], 0.999),
        table(&[T2_A, T2_B, T2_C], 0.999),
        &[T1_B],
        &[T2_B],
    );
    assert_row_counts(&derive_stats(&plan, &ctx()), &[12487.5, 9990.0, 9990.0]);
}

/// `explain select * from t1 join t2 on t1.a=t2.a join t3 on t2.a=t3.a`
/// -> `MergeJoin 15625.00` over `MergeJoin 12500.00`.
#[test]
fn a_three_table_chain_join_is_fifteen_thousand_six_hundred_and_twenty_five() {
    let plan = join(
        join(
            table(&[T1_A, T1_B, T1_C], 1.0),
            table(&[T2_A, T2_B, T2_C], 1.0),
            &[T1_A],
            &[T2_A],
        ),
        table(&[T3_A], 1.0),
        &[T2_A],
        &[T3_A],
    );
    assert_row_counts(
        &derive_stats(&plan, &ctx()),
        &[15625.0, 12500.0, 10000.0, 10000.0, 10000.0],
    );
}

/// A `LogicalSelection` that could not be pushed into its `DataSource` is a
/// flat `SelectionFactor`, whatever it filters
/// (`logicalop/logical_selection.go:234`).
#[test]
fn a_logical_selection_is_a_flat_zero_point_eight() {
    let plan = LogicalNode::Selection {
        child: Box::new(table(&[T1_A], 1.0)),
    };
    assert_row_counts(&derive_stats(&plan, &ctx()), &[8000.0, 10000.0]);
}

/// `LogicalAggregation.DeriveStats` uses the group-key NDV as both its row
/// count and every output column's NDV. A pseudo equality leaves ten rows and
/// an NDV of eight on the grouped column, matching TPCC condition 04's inner
/// `GROUP BY ol_d_id` fixture.
#[test]
fn a_grouped_aggregation_uses_the_group_key_ndv_for_every_output() {
    let plan = LogicalNode::Aggregation {
        child: Box::new(table(&[T1_A, T1_B], 1.0 / 1000.0)),
        group_by: vec![T1_B],
        columns: vec![DT_KEY_A, DT_DOUBLED_B],
    };
    let derived = derive_stats(&plan, &ctx());
    assert_row_counts(&derived, &[8.0, 10.0]);
    assert_eq!(derived.stats.col_ndvs()[&DT_KEY_A], 8.0);
    assert_eq!(derived.stats.col_ndvs()[&DT_DOUBLED_B], 8.0);
}

/// `LogicalJoin.DeriveStats` clamps every inherited column NDV to the join's
/// own row count (`logicalop/logical_join.go:604-610`), and once a join has
/// produced fewer rows than a key's NDV the clamp is what the *next* join
/// divides by.
///
/// Oracle, with `tidb_opt_join_reorder_threshold = 0` so no correlation factor
/// is applied:
///
/// ```sql
/// explain select /*+ leading(t1, t2, t3, t4) */ * from t1, t2, t3, t4
///   where t1.a = t2.a and t3.a = t4.a and t1.a = t3.a
///     and t2.c = 'x' and t4.c = 'y';
/// ```
/// ```text
/// IndexJoin_17    10.00   t3.a = t4.a
/// ├─IndexJoin_74  15.62   t1.a = t3.a
/// │ ├─IndexJoin_95 12.50  t2.a = t1.a
/// ```
///
/// The last join is the discriminating one. `t3.a` enters it with an NDV of
/// `8000` and leaves the clamp with `15.625`; dividing by `15.625` gives the
/// recorded `10.00`, dividing by the unclamped `8000` would give `0.02`.
#[test]
fn a_join_clamps_inherited_ndvs_to_its_own_row_count() {
    let context = DeriveStatsContext::default();
    let plan = join(
        join(
            join(
                table(&[T2_A, T2_B, T2_C], 1.0 / 1000.0),
                table(&[T1_A, T1_B, T1_C], 1.0),
                &[T2_A],
                &[T1_A],
            ),
            table(&[T3_A], 1.0),
            &[T1_A],
            &[T3_A],
        ),
        table(&[T4_A, 42, 43], 1.0 / 1000.0),
        &[T3_A],
        &[T4_A],
    );
    let derived = derive_stats(&plan, &context);
    assert_row_counts(
        &derived,
        &[
            10.0,    // IndexJoin_17
            15.625,  // IndexJoin_74
            12.5,    // IndexJoin_95
            10.0,    // t2 after c = 'x'
            10000.0, // t1
            10000.0, // t3
            10.0,    // t4 after c = 'y'
        ],
    );
}

// ---------------------------------------------------------------------------
// The derived-table group node shared by every target statement.
// ---------------------------------------------------------------------------

/// `(select t2.a as key_a, t2.b * 2 as doubled_b from t2 join t3 on t2.a = t3.a) dt`.
///
/// `t2_selectivity` carries whatever was pushed into `t2` -- `1.0` when nothing
/// was, `0.8` when a condition over the *expression* `t2.b * 2` was, since
/// `Selectivity` cannot resolve an expression to a column and charges one
/// `SelectionFactor` for the whole leftover mask.
fn dt(t2_selectivity: f64) -> LogicalNode {
    projection(
        join(
            table(&[T2_A, T2_B, T2_C], t2_selectivity),
            table(&[T3_A], 1.0),
            &[T2_A],
            &[T3_A],
        ),
        &[(DT_KEY_A, &[T2_A]), (DT_DOUBLED_B, &[T2_B])],
    )
}

/// The unfiltered `t5` group node, shared by every candidate.
fn t5() -> LogicalNode {
    table(&[T5_A], 1.0)
}

/// Statement 1: `where t1.a = dt.key_a and dt.key_a = t5.a`.
///
/// Oracle (`explain`, `tidb_opt_join_reorder_through_proj = off`):
/// ```text
/// MergeJoin_22          19531.25   inner join, left key:t2.a, right key:t5.a
/// ├─MergeJoin_35        15625.00   inner join, left key:t1.a, right key:t2.a
/// │ ├─TableReader_43    10000.00   t1
/// │ └─Projection_44     12500.00   t2.a, mul(t2.b, 2)
/// │   └─MergeJoin_45    12500.00   inner join, left key:t2.a, right key:t3.a
/// │     ├─TableReader_53 10000.00  t2
/// │     └─TableReader_55 10000.00  t3
/// └─TableReader_61      10000.00   t5
/// ```
#[test]
fn statement_1_plain_key_join_matches_the_recorded_est_rows() {
    let plan = join(
        join(
            table(&[T1_A, T1_B, T1_C], 1.0),
            dt(1.0),
            &[T1_A],
            &[DT_KEY_A],
        ),
        table(&[T5_A], 1.0),
        &[DT_KEY_A],
        &[T5_A],
    );
    assert_row_counts(
        &derive_stats(&plan, &ctx()),
        &[
            19531.25, // MergeJoin_22
            15625.0,  // MergeJoin_35
            10000.0,  // t1
            12500.0,  // Projection_44
            12500.0,  // MergeJoin_45
            10000.0,  // t2
            10000.0,  // t3
            10000.0,  // t5
        ],
    );
}

/// Statement 2: `where t1.b = dt.doubled_b and dt.key_a = t5.a`.
///
/// Oracle: `HashJoin_20 15625.00`, `HashJoin_61 12500.00`,
/// `Projection_93 10000.00`, `MergeJoin_96 10000.00`, `Selection_53 8000.00`
/// (`not(isnull(mul(t2.b, 2)))`), `IndexReader_76 9990.00`
/// (`not(isnull(t1.b))`).
///
/// The two `not(isnull(...))` rewrites are charged differently and both numbers
/// are load-bearing: over a bare column it is `0.999`, over an expression it is
/// the `0.8` leftover-mask factor.
#[test]
fn statement_2_computed_join_key_matches_the_recorded_est_rows() {
    let plan = join(
        join(
            table(&[T1_A, T1_B, T1_C], 0.999),
            dt(0.8),
            &[T1_B],
            &[DT_DOUBLED_B],
        ),
        table(&[T5_A], 1.0),
        &[DT_KEY_A],
        &[T5_A],
    );
    assert_row_counts(
        &derive_stats(&plan, &ctx()),
        &[
            15625.0, // HashJoin_20
            12500.0, // HashJoin_61
            9990.0,  // t1 after not(isnull(t1.b))
            10000.0, // Projection_93
            10000.0, // MergeJoin_96
            8000.0,  // t2 after not(isnull(mul(t2.b, 2)))
            10000.0, // t3
            10000.0, // t5
        ],
    );
}

/// Statement 3: `where t1.b = dt.doubled_b and t1.c = dt.upper_c and dt.key_a = t5.a`.
///
/// Oracle: `HashJoin_20 14062.50`, `HashJoin_63 11250.00`,
/// `Selection_78 9980.01` (`not(isnull(t1.b)), not(isnull(t1.c))`),
/// `Projection_97 10000.00`, `Selection_55 8000.00`.
///
/// Two equi keys, so `EstimateFullJoinRowCount` applies `0.9^(2-1)`: the join
/// would be `12500` without it.
#[test]
fn statement_3_two_computed_keys_apply_one_correlation_factor() {
    let dt3 = projection(
        join(
            table(&[T2_A, T2_B, T2_C], 0.8),
            table(&[T3_A], 1.0),
            &[T2_A],
            &[T3_A],
        ),
        &[
            (DT_KEY_A, &[T2_A]),
            (DT_DOUBLED_B, &[T2_B]),
            (DT_UPPER_C, &[T2_C]),
        ],
    );
    let plan = join(
        join(
            table(&[T1_A, T1_B, T1_C], 0.999 * 0.999),
            dt3,
            &[T1_B, T1_C],
            &[DT_DOUBLED_B, DT_UPPER_C],
        ),
        table(&[T5_A], 1.0),
        &[DT_KEY_A],
        &[T5_A],
    );
    assert_row_counts(
        &derive_stats(&plan, &ctx()),
        &[
            14062.5, // HashJoin_20
            11250.0, // HashJoin_63
            9980.01, // t1
            10000.0, // Projection_97
            10000.0, // MergeJoin_100
            8000.0,  // t2
            10000.0, // t3
            10000.0, // t5
        ],
    );
}

/// Statement 4: `where t1.a = dt.key_a and dt.key_a = t5.a and dt.doubled_b > 100`.
///
/// Oracle: `MergeJoin_22 15625.00`, `MergeJoin_35 12500.00`,
/// `MergeJoin_45 10000.00`, `Selection_53 8000.00` (`gt(mul(t2.b, 2), 100)`).
///
/// This is the statement that pins `ScaleNDV`: `t2` keeps `8000` rows but its
/// `a` NDV drops to `6400`, which is what makes the `t2 join t3` estimate
/// `10000` instead of `12500`.
#[test]
fn statement_4_a_filtered_derived_table_rescales_the_key_ndv() {
    let plan = join(
        join(
            table(&[T1_A, T1_B, T1_C], 1.0),
            dt(0.8),
            &[T1_A],
            &[DT_KEY_A],
        ),
        table(&[T5_A], 1.0),
        &[DT_KEY_A],
        &[T5_A],
    );
    assert_row_counts(
        &derive_stats(&plan, &ctx()),
        &[
            15625.0, // MergeJoin_22
            12500.0, // MergeJoin_35
            10000.0, // t1
            10000.0, // Projection
            10000.0, // MergeJoin_45
            8000.0,  // t2 after gt(mul(t2.b, 2), 100)
            10000.0, // t3
            10000.0, // t5
        ],
    );
}

/// Statement 5: the nested derived table,
/// `where t1.b = dt2.adjusted and dt2.key_a = t5.a`.
///
/// Oracle: `HashJoin_25 19531.25`, `HashJoin_79 15625.00`,
/// `Projection_143 12500.00`, `MergeJoin_146 12500.00`,
/// `Projection_57 10000.00`, `MergeJoin_58 10000.00`, `Selection_66 8000.00`
/// (`not(isnull(plus(mul(t2.b, 2), 10)))`), `IndexReader_93 9990.00`.
#[test]
fn statement_5_nested_derived_table_matches_the_recorded_est_rows() {
    let dt2 = projection(
        join(dt(0.8), table(&[T4_A], 1.0), &[DT_KEY_A], &[T4_A]),
        &[(DT2_KEY_A, &[DT_KEY_A]), (DT2_ADJUSTED, &[DT_DOUBLED_B])],
    );
    let plan = join(
        join(
            table(&[T1_A, T1_B, T1_C], 0.999),
            dt2,
            &[T1_B],
            &[DT2_ADJUSTED],
        ),
        table(&[T5_A], 1.0),
        &[DT2_KEY_A],
        &[T5_A],
    );
    assert_row_counts(
        &derive_stats(&plan, &ctx()),
        &[
            19531.25, // HashJoin_25
            15625.0,  // HashJoin_79
            9990.0,   // t1
            12500.0,  // Projection_143
            12500.0,  // MergeJoin_146
            10000.0,  // Projection_57
            10000.0,  // MergeJoin_58
            8000.0,   // t2
            10000.0,  // t3
            10000.0,  // t4
            10000.0,  // t5
        ],
    );
}

// ---------------------------------------------------------------------------
// The DP tie.
// ---------------------------------------------------------------------------

/// The two candidates `dpGraph` reaches at `nodeBitmap = 0b111` for a join
/// group of `[t1, dt, t5]` whose only edges are `t1--dt` and `dt--t5`.
///
/// `bestPlan[0b101]` (`{t1, t5}`) stays nil because those two are not
/// connected, and the `sub > remain` guard drops the mirrored halves, so
/// exactly two survive:
///
/// * `C1`, from `sub = 0b011`: `(t1 join dt) join t5`
/// * `C2`, from `sub = 0b001`: `t1 join (dt join t5)`
struct Candidates {
    c1: f64,
    c2: f64,
}

fn dp_candidate_costs(
    t1: &LogicalNode,
    dt: &LogicalNode,
    t5: &LogicalNode,
    t1_dt_keys: (&[ColumnId], &[ColumnId]),
    dt_t5_keys: (&[ColumnId], &[ColumnId]),
) -> Candidates {
    let context = ctx();
    let cum_t1 = derive_stats(t1, &context).cum_cost();
    let cum_dt = derive_stats(dt, &context).cum_cost();
    let cum_t5 = derive_stats(t5, &context).cum_cost();

    let t1_dt = join(t1.clone(), dt.clone(), t1_dt_keys.0, t1_dt_keys.1);
    let t1_dt_derived = derive_stats(&t1_dt, &context);
    let cum_t1_dt = calc_join_cum_cost(&t1_dt_derived, cum_t1, cum_dt);
    let c1_top = derive_stats(
        &join(t1_dt, t5.clone(), dt_t5_keys.0, dt_t5_keys.1),
        &context,
    );
    let c1 = calc_join_cum_cost(&c1_top, cum_t1_dt, cum_t5);

    let dt_t5 = join(dt.clone(), t5.clone(), dt_t5_keys.0, dt_t5_keys.1);
    let dt_t5_derived = derive_stats(&dt_t5, &context);
    let cum_dt_t5 = calc_join_cum_cost(&dt_t5_derived, cum_dt, cum_t5);
    let c2_top = derive_stats(
        &join(t1.clone(), dt_t5, t1_dt_keys.0, t1_dt_keys.1),
        &context,
    );
    let c2 = calc_join_cum_cost(&c2_top, cum_t1, cum_dt_t5);

    Candidates { c1, c2 }
}

/// Under pseudo statistics the two candidates cost *exactly* the same -- for
/// four of the five target statements. Statement 3 is the exception and gets
/// its own test below.
///
/// This is the acceptance criterion, and it is not a near miss: the assertion
/// is on bit-equal `f64`s. Go's update test is a strict `>`
/// (`rule_join_reorder_dp.go`, `bestPlan[nodeBitmap].cumCost > curCost`), so a
/// tie leaves the first enumerated candidate in place -- `C1`, the left-deep
/// `(t1 join dt) join t5`, which is the shape every recorded plan shows.
///
/// The tie is structural rather than coincidental. Writing out both costs,
///
/// ```text
/// cost(C1) = rows(top) + rows(t1 join dt) + cum(t1) + cum(dt) + cum(t5)
/// cost(C2) = rows(top) + cum(t1) + rows(dt join t5) + cum(dt) + cum(t5)
/// ```
///
/// every term cancels except `rows(t1 join dt)` against `rows(dt join t5)`,
/// and with one equi key per edge both reduce to
/// `10000 * rows(dt) / 8000` -- the two edges' key NDVs are both pinned at
/// `8000` by the unfiltered outer table, whatever the derived table did to its
/// own key NDV.
#[test]
fn the_two_dp_candidates_tie_exactly_for_four_of_the_five_statements() {
    // Statement 1 and statement 4 differ only in what was pushed into t2.
    for (name, t2_selectivity) in [("statement 1", 1.0), ("statement 4", 0.8)] {
        let candidates = dp_candidate_costs(
            &table(&[T1_A, T1_B, T1_C], 1.0),
            &dt(t2_selectivity),
            &t5(),
            (&[T1_A], &[DT_KEY_A]),
            (&[DT_KEY_A], &[T5_A]),
        );
        assert_eq!(
            candidates.c1, candidates.c2,
            "{name}: {} vs {}",
            candidates.c1, candidates.c2
        );
    }

    // Statement 2: the computed join key.
    let candidates = dp_candidate_costs(
        &table(&[T1_A, T1_B, T1_C], 0.999),
        &dt(0.8),
        &t5(),
        (&[T1_B], &[DT_DOUBLED_B]),
        (&[DT_KEY_A], &[T5_A]),
    );
    assert_eq!(candidates.c1, candidates.c2, "statement 2");

    // Statement 5: the nested derived table.
    let dt2 = projection(
        join(dt(0.8), table(&[T4_A], 1.0), &[DT_KEY_A], &[T4_A]),
        &[(DT2_KEY_A, &[DT_KEY_A]), (DT2_ADJUSTED, &[DT_DOUBLED_B])],
    );
    let candidates = dp_candidate_costs(
        &table(&[T1_A, T1_B, T1_C], 0.999),
        &dt2,
        &t5(),
        (&[T1_B], &[DT2_ADJUSTED]),
        (&[DT2_KEY_A], &[T5_A]),
    );
    assert_eq!(candidates.c1, candidates.c2, "statement 5");
}

/// Statement 3 is the one target statement whose two DP candidates do **not**
/// tie, and the cancellation above says exactly why.
///
/// The `t1--dt` edge carries two equi keys, so `EstimateFullJoinRowCount`
/// applies `0.9^(len(leftJoinKeys) - max(leftColCnt, rightColCnt))` = `0.9`
/// to it; the `dt--t5` edge carries one key and gets `0.9^0` = `1`. The two
/// terms that were supposed to cancel are therefore `11250` and `12500`, and
/// `C1` comes out exactly `1250` cheaper.
///
/// `C1` is also the candidate `dpGraph` enumerates first, so the recorded
/// left-deep plan is unchanged -- but it wins on cost here, not on the
/// tie-break, and any claim that statement 3 ties is wrong.
#[test]
fn statement_3_does_not_tie_because_only_one_edge_takes_the_correlation_factor() {
    let dt3 = projection(
        join(
            table(&[T2_A, T2_B, T2_C], 0.8),
            table(&[T3_A], 1.0),
            &[T2_A],
            &[T3_A],
        ),
        &[
            (DT_KEY_A, &[T2_A]),
            (DT_DOUBLED_B, &[T2_B]),
            (DT_UPPER_C, &[T2_C]),
        ],
    );
    let candidates = dp_candidate_costs(
        &table(&[T1_A, T1_B, T1_C], 0.999 * 0.999),
        &dt3,
        &t5(),
        (&[T1_B, T1_C], &[DT_DOUBLED_B, DT_UPPER_C]),
        (&[DT_KEY_A], &[T5_A]),
    );
    assert!(
        candidates.c1 < candidates.c2,
        "expected C1 to win outright: {} vs {}",
        candidates.c1,
        candidates.c2
    );
    // 12500 (dt join t5, one key) - 11250 (t1 join dt, two keys and one 0.9).
    assert_eq!(candidates.c2 - candidates.c1, 1250.0);
}

/// The tie is not an artifact of both candidates being computed by the same
/// code path: break the symmetry and the costs separate.
///
/// With `t5` filtered down, `dt join t5` is cheaper than `t1 join dt`, so `C2`
/// wins -- which is what makes the tie in the test above a real measurement
/// rather than a tautology.
#[test]
fn an_asymmetric_group_does_not_tie() {
    let candidates = dp_candidate_costs(
        &table(&[T1_A, T1_B, T1_C], 1.0),
        &dt(1.0),
        &table(&[T5_A], 0.001),
        (&[T1_A], &[DT_KEY_A]),
        (&[DT_KEY_A], &[T5_A]),
    );
    assert!(
        candidates.c2 < candidates.c1,
        "expected C2 to win: {} vs {}",
        candidates.c1,
        candidates.c2
    );
}

/// `baseNodeCumCost` sums the whole subtree, so the derived table alone already
/// costs more than either base table.
#[test]
fn the_derived_table_cum_cost_sums_its_whole_subtree() {
    // 12500 (projection) + 12500 (join) + 10000 (t2) + 10000 (t3).
    assert_eq!(derive_stats(&dt(1.0), &ctx()).cum_cost(), 45000.0);
}

#[test]
fn selection_factor_matches_the_planner_constant() {
    // The legacy model's default must track the package-owned constant, not
    // a second literal that can drift independently.
    assert_eq!(
        tidb_executor::driver::legacy_stats::DeriveStatsContext::default().selection_factor,
        tidb_planner::cost_factors::SELECTION_FACTOR
    );
}
