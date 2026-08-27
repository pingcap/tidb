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

//! Port of `pkg/planner.part13` items exercised against the GENERATED
//! `Hash64`/`Equals` identities of four plan operators:
//! `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go`
//! `TestLogicalExpandHash64Equals` (:585), `TestLogicalApplyHash64Equals`
//! (:676), `TestLogicalJoinHash64Equals` (:734) and
//! `TestLogicalAggregationHash64Equals` (:804) on `origin/master`.
//!
//! Every Go assertion sequence ("build two equal operators; mutate ONE field
//! group; hash and equality must flip; restoring must flip them back") is
//! replayed against the real operators' transcribed hash bodies
//! (`pkg/planner/core/operator/logicalop/hash64_equals_generated.go`, read
//! from origin/master):
//! * `LogicalExpand.Hash64` (:249) folds the producer schema, the distinct
//!   group-by columns/exprs, `DistinctSize`, the rollup grouping sets, the
//!   level projections and `GID`/`GPos` — mirrored by
//!   [`tidb_planner::logical::expand::LogicalExpand::hash64`].
//! * `LogicalJoin.Hash64` (:25) folds the producer schema, `JoinType`,
//!   `EqualConditions`/`NAEQConditions` and left/right/other conditions —
//!   mirrored by [`tidb_planner::logical::join::LogicalJoin::hash64`].
//! * `LogicalAggregation.Hash64` (:138) folds the producer schema, the agg
//!   descriptors, `GroupByItems` and `PossibleProperties` — whose Go body
//!   delegates to `PossiblePropertiesInfo.Hash64`
//!   (`pkg/planner/core/base/plan_base.go:391-408`: `Orders` folded NESTEDLY,
//!   `HasTiFlash` deliberately excluded, see its field comment :387).
//! * `LogicalApply.Hash64` (:200) exists in Go only — the Rust operator does
//!   not carry a hash surface yet (gap-documented below).
//!
//! Only equality RELATIONS are pinned, never absolute digests. Deviations are
//! documented per test.

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::aggregation::{AggFuncDesc, AggFunctionMode, BaseFuncDesc};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;

use tidb_planner::find_best_task::LogicalJoinType;
use tidb_planner::logical::aggregation::LogicalAggregation;
use tidb_planner::logical::expand::{LogicalExpand, RollupGroupingSet};
use tidb_planner::logical::join::LogicalJoin;
use tidb_planner::logical::BaseLogicalPlan;
use tidb_planner::plan_base::PossiblePropertiesInfo;

/// Go `&expression.Column{ID: n, Index: i, RetType: TypeLonglong}`.
fn column(unique_id: i64, index: i64) -> Column {
    let mut col = Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
    col.id = unique_id;
    col.index = index;
    col
}

fn col_expr(unique_id: i64, index: i64) -> Expression {
    Expression::Column(column(unique_id, index))
}

/// Go `expression.NewFunction(ctx, ast.EQ, TypeLonglong, lhs, rhs)` as a bare
/// scalar function.
fn eq(lhs: Column, rhs: Column) -> ScalarFunction {
    ScalarFunction::new(
        tidb_ast::CiString::new("eq"),
        FieldType::new(FieldTypeCode::LongLong),
        vec![Expression::Column(lhs), Expression::Column(rhs)],
    )
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:585
/// TestLogicalExpandHash64Equals`.
///
/// Sequence re-derived from the source: two `LogicalExpand`s over
/// `[col1]`/size 1/nil sets/nil levels/GID=GPos=col1 match (:616-619);
/// mutating `DistinctGroupByCol` → col2 (:621-625), `DistinctGbyExprs` → col2
/// (:627-632), `DistinctSize` → 2 (:634-639), `RollupGroupingSets` → one set
/// (:641-646), `LevelExprs` → one level (:648-653), `GID` → col2 (:655-660)
/// and `GPos` → col2 (:662-667) each flip hash AND equality independently;
/// restoring everything restores both (:669-673). Generated field order:
/// `hash64_equals_generated.go:249-310`.
#[test]
fn expand_hash64_equals_tracks_distinct_gby_size_sets_levels_and_generated_cols() {
    // Go builds bare struct literals with a NIL producer schema; the crate's
    // hash takes that state as `None`.
    let expand = |gby_col: i64,
                  gby_expr: i64,
                  distinct_size: i64,
                  sets: Vec<RollupGroupingSet>,
                  levels: Option<Vec<Vec<Expression>>>,
                  gid: Option<Box<Column>>,
                  gpos: Option<Box<Column>>| {
        LogicalExpand {
            base: BaseLogicalPlan::default(),
            distinct_group_by_col: vec![column(gby_col, 0)],
            distinct_gby_col_names: Vec::new(),
            distinct_gby_exprs: vec![col_expr(gby_expr, 0)],
            distinct_size,
            rollup_grouping_sets: sets,
            level_exprs: levels,
            gid,
            gpos,
            ..LogicalExpand::default()
        }
    };
    let p1 = expand(
        1,
        1,
        1,
        Vec::new(),
        None,
        Some(Box::new(column(1, 0))),
        Some(Box::new(column(1, 0))),
    );
    let p2 = expand(
        1,
        1,
        1,
        Vec::new(),
        None,
        Some(Box::new(column(1, 0))),
        Some(Box::new(column(1, 0))),
    );
    assert_eq!(p1.hash64(None), p2.hash64(None));
    assert!(p1.equals(None, &p2, None));

    // DistinctGroupByCol -> col2 (:621-623).
    let p2 = expand(
        2,
        1,
        1,
        Vec::new(),
        None,
        Some(Box::new(column(1, 0))),
        Some(Box::new(column(1, 0))),
    );
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // Restore column; DistinctGbyExprs -> col2 (:627-630).
    let p2 = expand(
        1,
        2,
        1,
        Vec::new(),
        None,
        Some(Box::new(column(1, 0))),
        Some(Box::new(column(1, 0))),
    );
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // Restore exprs; DistinctSize -> 2 (:634-637).
    let p2 = expand(
        1,
        1,
        2,
        Vec::new(),
        None,
        Some(Box::new(column(1, 0))),
        Some(Box::new(column(1, 0))),
    );
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // Restore size; RollupGroupingSets -> {set{col1}} vs nil (:641-644). Go
    // hashes nil vs a NON-nil single-set slice; the content change alone moves
    // the digest here as well.
    let p2 = expand(
        1,
        1,
        1,
        vec![RollupGroupingSet::new([1])],
        None,
        Some(Box::new(column(1, 0))),
        Some(Box::new(column(1, 0))),
    );
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // Restore sets; LevelExprs -> [[col1]] vs nil (:648-651). This is the
    // load-bearing NIL-vs-present distinction the generated body makes.
    let p2 = expand(
        1,
        1,
        1,
        Vec::new(),
        Some(vec![vec![col_expr(1, 0)]]),
        Some(Box::new(column(1, 0))),
        Some(Box::new(column(1, 0))),
    );
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // Restore levels; GID -> col2 (:655-658).
    let p2 = expand(
        1,
        1,
        1,
        Vec::new(),
        None,
        Some(Box::new(column(2, 0))),
        Some(Box::new(column(1, 0))),
    );
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // GPos -> col2 (:662-665).
    let p2 = expand(
        1,
        1,
        1,
        Vec::new(),
        None,
        Some(Box::new(column(1, 0))),
        Some(Box::new(column(2, 0))),
    );
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // Everything restored -> equal again (:667-670+).
    let p2 = expand(
        1,
        1,
        1,
        Vec::new(),
        None,
        Some(Box::new(column(1, 0))),
        Some(Box::new(column(1, 0))),
    );
    assert_eq!(p1.hash64(None), p2.hash64(None));
    assert!(p1.equals(None, &p2, None));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:734
/// TestLogicalJoinHash64Equals`, first FOUR arms.
///
/// Sequence re-derived from the source: two inner joins keyed `eq(col1,col2)`
/// match (:758-761); flipping `JoinType` to `AntiSemiJoin` (:763-767),
/// swapping the key sides to `eq(col2,col1)` (:772-776), or replacing the keys
/// with an OTHER-condition list (:778-785) each flip hash AND equality. The
/// final nil-vs-empty arms (:787-801) live in the ignored companion below.
///
/// DEVIATION: Go passes NIL condition slices for `LeftConditions` /
/// `RightConditions` / `NAEQConditions`; the Rust operator carries plain
/// `Vec`s, folded by length only — irrelevant here because those buckets stay
/// empty on BOTH sides throughout.
#[test]
fn join_hash64_equals_tracks_join_type_condition_order_and_other_conditions() {
    let build = |join_type: LogicalJoinType,
                 equal_lhs: i64,
                 equal_rhs: i64,
                 has_equal: bool,
                 others: Vec<Expression>| {
        let equal_conditions = if has_equal {
            vec![eq(column(equal_lhs, 0), column(equal_rhs, 1))]
        } else {
            Vec::new()
        };
        LogicalJoin {
            base: BaseLogicalPlan::default(),
            join_type,
            equal_conditions,
            other_conditions: others,
            ..LogicalJoin::default()
        }
    };
    // la1 / la2: InnerJoin, EqualConditions=[eq(col1,col2)] (:748-761).
    let p1 = build(LogicalJoinType::Inner, 1, 2, true, Vec::new());
    let p2 = build(LogicalJoinType::Inner, 1, 2, true, Vec::new());
    assert_eq!(p1.hash64(None), p2.hash64(None));
    assert!(p1.equals(None, &p2, None));

    // JoinType -> AntiSemiJoin (:763-767).
    let p2 = build(LogicalJoinType::AntiSemi, 1, 2, true, Vec::new());
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // Key sides swapped -> eq(col2,col1) (:772-776). Digests move because the
    // ARGUMENT ORDER of the scalar function changes its hash code.
    let p2 = build(LogicalJoinType::Inner, 2, 1, true, Vec::new());
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // EqualConditions emptied, OtherConditions=[eq(col1,col2)] (:778-785).
    // (Go names the variable `gt`; its function is `ast.EQ`, read at :780.)
    let p2 = build(
        LogicalJoinType::Inner,
        1,
        2,
        false,
        vec![Expression::ScalarFunction(eq(column(1, 0), column(2, 1)))],
    );
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // Both restored -> equal again (mirrors the :800-801 outcome with
    // non-empty keys; the []-vs-nil arm itself is the gap below).
    let p2 = build(LogicalJoinType::Inner, 1, 2, true, Vec::new());
    assert_eq!(p1.hash64(None), p2.hash64(None));
    assert!(p1.equals(None, &p2, None));
}

/// GO PARITY GAP port of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:787-801`
/// (the tail of `TestLogicalJoinHash64Equals`).
///
/// go-parity-gap: Go pins that a NON-NIL EMPTY `EqualConditions` slice hashes
/// and equals DIFFERENTLY from a NIL one
/// (`hash64_equals_generated.go:31-34` writes NotNilFlag+len before any
/// element), with `la1.EqualConditions=[]` vs `la2.EqualConditions=nil`
/// requiring NOT-equal digests (:794-795). The Rust `LogicalJoin` stores plain
/// `Vec`s with no absent marker, so both states fold identically and the
/// assertion cannot be expressed without inventing production surface.
#[test]
#[ignore]
fn logical_join_hash64_equals_pins_nil_versus_empty_equal_conditions() {
    // p1.equal_conditions = [] (Go: non-nil empty), p2.equal_conditions = nil.
    //
    // What a faithful port would assert once the operator gains Go's
    // nil-marker framing (generated Hash64 :31-34, Equals :88-91):
    //     assert_ne!(p1.hash64(None), p2.hash64(None));
    //     assert!(!p1.equals(None, &p2, None));
    //     // restore p2.equal_conditions = [] (empty):
    //     assert_eq!(p1.hash64(None), p2.hash64(None));
    //     assert!(p1.equals(None, &p2, None));
}

/// GO PARITY GAP port of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:676
/// TestLogicalApplyHash64Equals`.
///
/// go-parity-gap: the embedded-JOIN half of the Go sequence replays through
/// `LogicalJoin::hash64`/`equals` exactly as the preceding test pins it, but
/// the APPLY-SPECIFIC arms — `CorCols` swapped col3->col2 (:713-717),
/// `NoDecorrelate` toggled (:719-723), and their restoration (:725-728), hashed
/// by `LogicalApply.Hash64` (`hash64_equals_generated.go:200-215`) — have NO
/// Rust counterpart: `logical::apply::LogicalApply` exposes neither `Hash64`
/// nor `Equals`, and adding one would be production code outside this batch's
/// scope.
#[test]
#[ignore]
fn logical_apply_hash64_equals_tracks_correlated_columns_and_no_decorrelate_flag() {
    // What a faithful port would run once the operator carries the generated
    // identity:
    //   two Applies sharing Join{Inner, [eq(col1,col2)]} and CorCols=[col3]
    //   are equal; CorCols=[col2] breaks both halves; restoring col3 and
    //   flipping NoDecorrelate=true breaks both; restoring restores both.
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:804
/// TestLogicalAggregationHash64Equals`, GROUP-BY arms.
///
/// Sequence re-derived from the source: two aggregations over AVG(DISTINCT
/// col)+group-by-[col]+possible-orders-[[col]] match (:828-831); emptying
/// `GroupByItems` to a non-nil empty slice flips hash AND equality (:833-837).
///
/// The POSSIBLE-PROPERTIES arms (:839-855) live in the ignored companion
/// below: they hinge on how `PossiblePropertiesInfo` enters the digest, and
/// the Rust body folds the OPPOSITE half of that struct.
#[test]
fn aggregation_hash64_equals_tracks_group_by_items() {
    let agg_desc = || AggFuncDesc {
        base: BaseFuncDesc {
            name: "avg".to_owned(),
            args: vec![col_expr(10, 0)],
            ret_type: FieldType::new(FieldTypeCode::LongLong),
        },
        mode: AggFunctionMode::Complete,
        has_distinct: true,
        order_by_items: Vec::new(),
        grouping_id: 0,
    };
    let possible = |orders: Vec<Vec<Column>>| PossiblePropertiesInfo {
        orders,
        has_tiflash: false,
    };
    let build = |group_by: Vec<Expression>, orders: Vec<Vec<Column>>| LogicalAggregation {
        base: BaseLogicalPlan::default(),
        agg_funcs: vec![agg_desc()],
        group_by_items: group_by,
        possible_properties: possible(orders),
        ..LogicalAggregation::default()
    };
    // la1 / la2 (:812-831): AggFuncs=[avg(distinct col)], GroupByItems=[col],
    // PossibleProperties.Orders=[[col]].
    let p1 = build(vec![col_expr(20, 0)], vec![vec![column(20, 0)]]);
    let p2 = build(vec![col_expr(20, 0)], vec![vec![column(20, 0)]]);
    assert_eq!(p1.hash64(None), p2.hash64(None));
    assert!(p1.equals(None, &p2, None));

    // GroupByItems -> empty slice (:833-837): content leaves the digest, and
    // Equals' length guard fails.
    let p2 = build(Vec::new(), vec![vec![column(20, 0)]]);
    assert_ne!(p1.hash64(None), p2.hash64(None));
    assert!(!p1.equals(None, &p2, None));

    // Restored (:839-841): parity again.
    let p2 = build(vec![col_expr(20, 0)], vec![vec![column(20, 0)]]);
    assert_eq!(p1.hash64(None), p2.hash64(None));
    assert!(p1.equals(None, &p2, None));
}

/// GO PARITY GAP port of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:839-855`
/// (the `PossibleProperties` tail of `TestLogicalAggregationHash64Equals`).
///
/// go-parity-gap: Go folds `PossiblePropertiesInfo.Orders` (nested order
/// lists, `plan_base.go:400-408`) into the digest and DELIBERATELY EXCLUDES
/// `HasTiFlash` (field comment `plan_base.go:387`) — hence Orders->[[]] must
/// flip (:844-846) while Orders->[[col]] WITH HasTiFlash:true must hash EQUAL
/// (:854-855). The Rust `LogicalAggregation::hash64`
/// (`src/logical/aggregation.rs`, `PossibleProperties` section) folds ONLY
/// `has_tiflash` and NO orders — the inverted choice — so neither assertion
/// can pass against it as written:
///     // Orders -> [[]]: Go hash flips, Rust digest would stay put.
///     // Orders -> [[col]], HasTiFlash=true: Go hashes EQUAL, Rust would flip.
///
/// WHAT WOULD CLOSE IT: transcription-correcting the operator's hash body to
/// fold `possible_properties.orders` (nested lists of column identities) and
/// drop `has_tiflash` — a production-code change outside this batch.
#[test]
#[ignore]
fn aggregation_possible_properties_orders_belong_to_the_identity_excluding_has_tiflash() {}
