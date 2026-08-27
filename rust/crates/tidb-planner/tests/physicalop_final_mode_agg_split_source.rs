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

//! `pkg/planner.part14` ports of `pkg/planner/core/plan_test.go`'s
//! `BuildFinalModeAggregation` items over the REAL transcreated split in
//! [`tidb_planner::final_mode_agg`]:
//!
//! * `plan_test.go:534 TestBuildFinalModeAggregation` — RUNS for real.
//! * `plan_test.go:682 TestBuildFinalModeAggregationMaxMinCountSchema` —
//!   honest `#[ignore]` gap port (the `max_count`/`min_count` aggregate
//!   family is not transcreated on the Rust side).

use tidb_datatype::FieldType;
use tidb_datatype::FieldTypeCode;
use tidb_expr::aggregation::names;
use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode, ByItems};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::{SessionTimeZone, ZonedNoColumns};

use tidb_planner::expression_rewriter::ColumnIdAllocator;
use tidb_planner::final_mode_agg::{build_final_mode_aggregation, AggInfo};

/// Go's expression context is consulted only for argument types during the
/// split's `TypeInfer` calls, exactly as in the crate's own `final_mode_agg`
/// unit tests.
fn ctx() -> ZonedNoColumns {
    ZonedNoColumns(SessionTimeZone::utc())
}

/// Go's `aggCol` (:588): a `TypeLonglong` column.
fn bigint_col(unique_id: i64) -> Column {
    Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
}

/// Go's `isFinalAggMode` (:545): Final or Complete.
fn is_final_mode(mode: AggFunctionMode) -> bool {
    mode == AggFunctionMode::Final || mode == AggFunctionMode::Complete
}

/// Go `NewAggFuncDesc` over one column argument.
fn agg_func(name: &str, arg: &Column, distinct: bool) -> AggFuncDesc {
    AggFuncDesc::new(&ctx(), name, vec![Expression::Column(arg.clone())], distinct)
        .expect("NewAggFuncDesc")
}

/// Go's `aggSchemaBuilder` (:535): one schema column per aggregate, carrying
/// the aggregate's inferred return type. Unique ids are arbitrary (Go's are
/// session-allocated); the split only reads the type.
fn original_schema(agg_funcs: &[AggFuncDesc]) -> Schema {
    Schema::new(
        agg_funcs
            .iter()
            .enumerate()
            .map(|(i, f)| Column::new(1000 + i as i64, f.base.ret_type.clone()))
            .collect(),
    )
}

/// GO PORT of `pkg/planner/core/plan_test.go:534
/// TestBuildFinalModeAggregation`.
///
/// Go builds five aggregates without distinct (max, firstrow, count, sum,
/// avg), two with distinct (avg, count), four group_concat variants
/// (±distinct × ±order-by) plus mixed sets, and for EVERY set × {empty,
/// [gbyCol]} group-by × `partialIsCop` × `isMPPTask` calls
/// `physicalop.BuildFinalModeAggregation`
/// (`pkg/planner/core/operator/physicalop/base_physical_agg.go:614`).
/// Whenever the partial half exists its modes must be non-final under a cop
/// split and final-mode otherwise, and the final half's modes must ALWAYS be
/// final-mode; when the split returns one phase (`partial == nil`), the
/// final half IS the original whose descriptors stay in `CompleteMode`.
///
/// This port replays exactly that matrix over
/// [`tidb_planner::final_mode_agg::build_final_mode_aggregation`], the
/// transcreated body whose module header cites `base_physical_agg.go:600`.
#[test]
fn build_final_mode_aggregation_splits_modes_across_cop_and_mpp_matrix() {
    let agg_col = bigint_col(1);
    let gby_col = bigint_col(2);
    let order_col = bigint_col(3);

    // Go :596-603 — empty group-by vs group-by over one column.
    let empty_group_by: Vec<Expression> = Vec::new();
    let group_by_items: Vec<Expression> = vec![Expression::Column(gby_col.clone())];

    // Go :612-635 — five aggregates without distinct.
    let agg_funcs: Vec<AggFuncDesc> = [names::MAX, names::FIRST_ROW, names::COUNT, names::SUM, names::AVG]
        .iter()
        .map(|name| agg_func(name, &agg_col, false))
        .collect();

    // Go :637-645 — avg and count WITH distinct.
    let agg_funcs_with_distinct: Vec<AggFuncDesc> = [names::AVG, names::COUNT]
        .iter()
        .map(|name| agg_func(name, &agg_col, true))
        .collect();

    // Go :647-665 — four group_concat variants (arg, separator) ×
    // ±distinct × ±order-by; the order-by item is `orderCol DESC`.
    let order_by_items = vec![ByItems::new(Expression::Column(order_col.clone()), true)];
    // Go builds each group_concat with BOTH arguments (value + separator) at
    // once so type inference sees the full signature.
    let group_concat = |distinct: bool| {
        AggFuncDesc::new(
            &ctx(),
            names::GROUP_CONCAT,
            vec![
                Expression::Column(agg_col.clone()),
                Expression::Column(agg_col.clone()),
            ],
            distinct,
        )
        .expect("NewAggFuncDesc(group_concat)")
    };
    let mut group_concat_aggs: Vec<AggFuncDesc> = Vec::new();
    for distinct in [false, true] {
        for with_order in [false, true] {
            let mut desc = group_concat(distinct);
            if with_order {
                desc.order_by_items = order_by_items.clone();
            }
            group_concat_aggs.push(desc);
        }
    }

    // Go `checkResult` (:546-574).
    fn check_result(agg_funcs: &[AggFuncDesc], group_by_items: &[Expression]) {
        for partial_is_cop in [true, false] {
            for is_mpp_task in [true, false] {
                let alloc = ColumnIdAllocator::new();
                let original = AggInfo {
                    agg_funcs: agg_funcs.to_vec(),
                    group_by_items: group_by_items.to_vec(),
                    schema: original_schema(agg_funcs),
                };
                match build_final_mode_aggregation(&ctx(), &alloc, &original, partial_is_cop, is_mpp_task) {
                    Some(split) => {
                        for agg_func in &split.partial.agg_funcs {
                            if partial_is_cop {
                                assert!(
                                    !is_final_mode(agg_func.mode),
                                    "cop partial must be non-final, got {:?}",
                                    agg_func.mode
                                );
                            } else {
                                assert!(
                                    is_final_mode(agg_func.mode),
                                    "non-cop partial must be final-mode, got {:?}",
                                    agg_func.mode
                                );
                            }
                        }
                        for agg_func in &split.final_agg.agg_funcs {
                            assert!(
                                is_final_mode(agg_func.mode),
                                "final half must be final-mode, got {:?}",
                                agg_func.mode
                            );
                        }
                    }
                    // Go's `partial == nil` return hands back
                    // `final = original` (:763-766, :799-802): the original
                    // descriptors are untouched `CompleteMode`.
                    None => {
                        for agg_func in &original.agg_funcs {
                            assert!(is_final_mode(agg_func.mode));
                        }
                    }
                }
            }
        }
    }

    // Case 1: aggregates without distinct.
    check_result(&agg_funcs, &empty_group_by);
    check_result(&agg_funcs, &group_by_items);
    // Case 2: aggregates with distinct.
    check_result(&agg_funcs_with_distinct, &empty_group_by);
    check_result(&agg_funcs_with_distinct, &group_by_items);
    // Case 3: mixed distinct and non-distinct.
    let mut mixed: Vec<AggFuncDesc> = agg_funcs.clone();
    mixed.extend(agg_funcs_with_distinct.clone());
    check_result(&mixed, &empty_group_by);
    check_result(&mixed, &group_by_items);
    // Case 4: group_concat, each variant alone and all four together. The
    // order-by-without-distinct variants must come back one-phase (`None`).
    for group_concat_agg in &group_concat_aggs {
        let expect_none = !group_concat_agg.has_distinct && !group_concat_agg.order_by_items.is_empty();
        for group_by in [&empty_group_by, &group_by_items] {
            let alloc = ColumnIdAllocator::new();
            let original = AggInfo {
                agg_funcs: vec![group_concat_agg.clone()],
                group_by_items: group_by.clone(),
                schema: original_schema(std::slice::from_ref(group_concat_agg)),
            };
            let split = build_final_mode_aggregation(&ctx(), &alloc, &original, true, false);
            assert_eq!(
                split.is_none(),
                expect_none,
                "group_concat distinct={} order_by={} one-phase={}",
                group_concat_agg.has_distinct,
                !group_concat_agg.order_by_items.is_empty(),
                expect_none
            );
        }
    }
    check_result(&group_concat_aggs, &empty_group_by);
    check_result(&group_concat_aggs, &group_by_items);
    // Case 5: mixed group_concat + plain + distinct sets.
    for group_concat_agg in &group_concat_aggs {
        let mut funcs: Vec<AggFuncDesc> = vec![group_concat_agg.clone()];
        funcs.extend(agg_funcs.iter().cloned());
        check_result(&funcs, &empty_group_by);
        check_result(&funcs, &group_by_items);
        funcs.extend(agg_funcs_with_distinct.iter().cloned());
        check_result(&funcs, &empty_group_by);
        check_result(&funcs, &group_by_items);
    }
    mixed.extend(group_concat_aggs.iter().cloned());
    check_result(&mixed, &empty_group_by);
    check_result(&mixed, &group_by_items);
}

/// GO PARITY GAP port of `pkg/planner/core/plan_test.go:682
/// TestBuildFinalModeAggregationMaxMinCountSchema`.
///
/// go-parity-gap: the `max_count` aggregate family is not transcreated —
/// Go's `ast.AggFuncMaxCount = "max_count"` (pkg/parser/ast/functions.go:837)
/// has no entry in `tidb_expr::aggregation::names`, `need_value`
/// (tidb-expr/src/aggregation/mod.rs:333) excludes it, and the split's
/// `NeedValue` arm keeps the ARGUMENT type for the partial value column
/// (base_physical_agg.go:803-809: "max_count/min_count partial result
/// contains [count, extrema value]"). Go pins, for a `max_count(varchar
/// utf8mb4_general_ci NOT NULL-stripped)` split under (cop, non-MPP):
/// partial schema exactly `[TypeLonglong, TypeString utf8mb4_general_ci]`
/// and final args typed `[Longlong, String utf8mb4_general_ci]`. Running
/// that descriptor through the Rust split would take the un-gated value
/// arm and produce the wrong schema, so the test is documented, not
/// approximated.
#[test]
#[ignore = "go-parity-gap: max_count/min_count aggregate family untranscreated (names/need_value/split value-type arm base_physical_agg.go:803-809)"]
fn build_final_mode_aggregation_max_min_count_schema_two_column_partial() {}
