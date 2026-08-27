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

//! Ports for `pkg/planner/core/rule_join_reorder_dp_test.go`
//! (`pkg/planner.part15` items 861–864 on `origin/master`; production sources
//! `rule_join_reorder.go`, `joinorder/join_order.go`,
//! `rule_join_reorder_projection_inline.go`).
//!
//! | Go function (`rule_join_reorder_dp_test.go`) | Rust test |
//! | --- | --- |
//! | `:166 TestDPReorderTPCHQ5` | [`dp_reorder_tpch_q5_bushy_shape_under_mock_stats`] (gap) |
//! | `:215 TestDPReorderAllCartesian` | [`dp_reorder_all_cartesian_balances_pairs`] (gap) |
//! | `:243 TestInjectedJoinExprMaterializationSafety` | [`injected_join_expr_materialization_keeps_rand_independent_and_child_space`] (gap) |
//! | `:315 TestJoinReorderInlineSafetyGates` | [`join_reorder_inline_basic_gate_rejects_nondeterministic_and_accepts_cross_leaf_expr`] (running, basic-gate halves) + [`join_reorder_inline_leaf_gates_group_extraction_and_multi_leaf_filters`] (gap remainder) |
//!
//! The DP solver itself (`joinReorderDPSolver.solve`, its `newJoin` mock
//! harness and `baseSingleGroupJoinOrderSolver.injectExpr`) is not
//! transcreated; the closest Rust artifact lives in `tidb-executor`'s legacy
//! driver, outside this crate's gate scope. The projection-inline BASIC gate
//! IS transcreated (`tidb_planner::join_reorder_projection_inline`), so its
//! share of `TestJoinReorderInlineSafetyGates` runs here for real: Rust keeps
//! the caller-supplied effect metadata (`non_deterministic`, `correlated`,
//! …) that Go derives inside the expression framework — FoundRows is a
//! non-deterministic builtin on the Go side (its evaluation reads session
//! state), and that fact feeds the shape exactly as Go's own
//! `CheckNonDeterministic(expr)` would observe it.

use tidb_planner::join_reorder_projection_inline::{
    can_inline_projection_basic, ProjectionInlineExpr, ProjectionInlineShape,
};

/// GO PORT of the subtests of
/// `pkg/planner/core/rule_join_reorder_dp_test.go:315
/// TestJoinReorderInlineSafetyGates` that exercise `canInlineProjectionBasic`
/// (`rule_join_reorder_projection_inline.go:128-160`).
///
/// Re-derived contract from the two relevant subtests:
/// - "projection basic gate rejects non-deterministic expressions" (:367-382):
///   proj expr = `plus(left.col, found_rows())`. `found_rows()` carries no
///   columns but IS non-deterministic, so the expression still references one
///   column and consists only of supported nodes; the gate must fail SOLELY
///   on the non-determinism check → `false`.
/// - "projection leaf gate rejects cross-leaf expressions" (:384-397): proj
///   expr = `plus(left.col, right.col)` over an inner join; its FIRST
///   assertion (:397) requires the basic gate to ACCEPT it (`true`) — only
///   the later leaf-attribution check may reject it.
#[test]
fn join_reorder_inline_basic_gate_rejects_nondeterministic_and_accepts_cross_leaf_expr() {
    // Sub-test 1 construction (:370-377), mapped onto the transcreated gate:
    // `plus(Column, ScalarFunction(found_rows))` where `found_rows` exposes
    // zero column references and non-deterministic evaluation metadata.
    let nondeterministic_proj = ProjectionInlineShape::new(
        false,
        vec![ProjectionInlineExpr::ScalarFunction {
            args: vec![
                ProjectionInlineExpr::Column,
                ProjectionInlineExpr::ScalarFunction {
                    args: Vec::new(),
                    mutable_effects: false,
                    non_deterministic: true,
                    correlated: false,
                },
            ],
            mutable_effects: false,
            non_deterministic: true,
            correlated: false,
        }],
    );
    assert!(
        !can_inline_projection_basic(&nondeterministic_proj),
        ":381 require.False: a projection over plus(col, found_rows()) must be rejected as non-deterministic"
    );

    // Sub-test 2 construction (:387-394): `plus(left_col, right_col)` over an
    // inner join. Deterministic, uncorrelated, supported nodes, references
    // columns → the basic gate accepts, deferring to the leaf gate.
    let cross_leaf_proj = ProjectionInlineShape::new(
        false,
        vec![ProjectionInlineExpr::ScalarFunction {
            args: vec![ProjectionInlineExpr::Column, ProjectionInlineExpr::Column],
            mutable_effects: false,
            non_deterministic: false,
            correlated: false,
        }],
    );
    assert!(
        can_inline_projection_basic(&cross_leaf_proj),
        ":397 require.True: cross-leaf plus(col,col) passes the BASIC safety gate"
    );

    // Source quirk pinned alongside (:136-146): constant-only projections are
    // rejected because ExtractColumns finds zero columns even when the
    // constants are deterministic.
    let constant_only_proj =
        ProjectionInlineShape::new(false, vec![ProjectionInlineExpr::Constant { deferred: false }]);
    assert!(!can_inline_projection_basic(&constant_only_proj));
}

/// GO PORT of `pkg/planner/core/rule_join_reorder_dp_test.go:166
/// TestDPReorderTPCHQ5`.
///
/// Re-derived contract: a mock `LogicalJoin` fabric (`mockLogicalJoin`,
/// :34-79) keys extra stats by involved-node-set bitmask; six single-column
/// dataSources named lineitem/orders/customer/supplier/nation/region
/// (:60M/15M/1.5M/100k/25/5 rows via `newDataSource` :141-149) are connected
/// by seven TPC-H Q5 equality edges. After `joinReorderDPSolver.solve` the
/// stringified tree MUST be exactly `"MockJoin{supplier, MockJoin{lineitem,
/// MockJoin{orders, MockJoin{customer, MockJoin{nation, region}}}}}"`
/// (:211-212).
#[test]
#[ignore = "go-parity-gap: joinReorderDPSolver/injectExpr machinery not in this crate (legacy driver copy lives in tidb-executor)"]
fn dp_reorder_tpch_q5_bushy_shape_under_mock_stats() {}

/// GO PORT of `pkg/planner/core/rule_join_reorder_dp_test.go:215
/// TestDPReorderAllCartesian`.
///
/// Four 100-row dataSources a/b/c/d with NO edges through the same DP solver;
/// expected shape `"MockJoin{MockJoin{a, b}, MockJoin{c, d}}"` (:239-240),
/// pinning DP's balanced-pair preference under equal row counts.
#[test]
#[ignore = "go-parity-gap: joinReorderDPSolver not in this crate"]
fn dp_reorder_all_cartesian_balances_pairs() {}

/// GO PORT of the subtests of
/// `pkg/planner/core/rule_join_reorder_dp_test.go:243
/// TestInjectedJoinExprMaterializationSafety`.
///
/// - "injectExpr keeps repeated rand expressions independent" (:255-274):
///   two structurally-equal `plus(rand(), col)` trees both inject into a
///   DataSource → LogicalProjection grows to THREE exprs and the injected
///   columns carry DIFFERENT UniqueIDs (each materialized once, never
///   deduplicated against the other).
/// - "injectExpr keeps appended expression in child space for existing
///   projections" (:276-312): injecting `plus(derivedCol, 1)` into a
///   projection already owning derivedCol appends
///   `plus(plus(baseCol,1),1)` whose extracted columns reference ONLY
///   baseCol's UniqueID (child-space assertions :305-310).
#[test]
#[ignore = "go-parity-gap: baseSingleGroupJoinOrderSolver.injectExpr + AppendExpr pipeline not in this crate"]
fn injected_join_expr_materialization_keeps_rand_independent_and_child_space() {}

/// GO PORT of the REMAINING subtests of
/// `pkg/planner/core/rule_join_reorder_dp_test.go:315
/// TestJoinReorderInlineSafetyGates` (the halves beyond the basic gate, which
/// run in [`join_reorder_inline_basic_gate_rejects_nondeterministic_and_accepts_cross_leaf_expr`]).
///
/// - "selection rejects non-deterministic predicates" (:346-365): with
///   `TiDBOptJoinReorderThroughSel = true`, a Selection above the join whose
///   condition is `eq(found_rows(), 1)` makes `extractJoinGroup`
///   (`rule_join_reorder.go:38`) stop there: group has length ONE and holds
///   the Selection itself (:362-364).
/// - leaf-gate parts of the cross-leaf subtest (:398-402) and null-extended
///   subtest (:404-424): `canInlineProjection`
///   (`rule_join_reorder_projection_inline.go:164-238`) rejects the
///   cross-leaf expr and the right-side `plus(right.col, 1)` over a LEFT
///   outer join whose `nullExtendedCols` schema covers right columns.
/// - "tryInlineProjectionForJoinGroup keeps safe single-leaf derived
///   columns" (:426-445): returns handled=true, group grows to TWO leaves and
///   colExprMap maps the projected UniqueID to `plus(left.col, 1)`.
/// - "tryInlineProjectionForJoinGroup keeps cross-leaf projection atomic"
///   (:447-465): handled=true, group stays length ONE holding the projection.
/// - "outer join side filters touching multiple leaves block reorder"
///   (:467-495): a LeftConditions filter `gt(plus(left0.col, left1.col), 0)`
///   makes `OuterJoinSideFiltersTouchMultipleLeaves`
///   (`pkg/planner/core/joinorder/util.go:275-315`) return TRUE (:488).
#[test]
#[ignore = "go-parity-gap: extractJoinGroup/canInlineProjection/tryInlineProjectionForJoinGroup and OuterJoinSideFiltersTouchMultipleLeaves are not in this crate"]
fn join_reorder_inline_leaf_gates_group_extraction_and_multi_leaf_filters() {}
