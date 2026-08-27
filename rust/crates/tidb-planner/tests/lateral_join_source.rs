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

//! Documentary gap ports for `pkg/planner/core/lateral_join_test.go`
//! (`pkg/planner.part12` items 661-662 on `origin/master`):
//! `TestRecursiveCTEWithLateralOrderByLimit` (:627) and
//! `TestLateralJoinMySQLCompatibility` (:790).
//!
//! Both tests parse SQL with `s.GetParser().ParseOneStmt`, wrap it in
//! `resolve.NewNodeW`, and run the FULL statement-to-logical-plan build via
//! `BuildLogicalPlanForTest(ctx, s.GetSCtx(), nodeW, s.GetIS())`
//! (session context + mock InfoSchema; loop body :766-786 and :830-846).
//! The behavioral core lives in `PlanBuilder.buildSelect`
//! (`pkg/planner/core/logical_plan_builder.go:4295-4300`: while building the
//! recursive part of a CTE, ORDER BY/LIMIT are only accepted inside LATERAL
//! subqueries — enforced by `b.buildingLateralSubquery`) plus name-resolution
//! visibility rules for derived-table aliases into LATERAL scopes. The Rust
//! workspace has no SQL→plan builder driven over parsed statements yet
//! (`plan_builder/cte.rs` covers validation helpers, not FROM-clause scope
//! construction), so these are recorded gaps, not approximations.

/// GO PORT of
/// `pkg/planner/core/lateral_join_test.go:627
/// TestRecursiveCTEWithLateralOrderByLimit`.
///
/// Eight single-statement cases against table `t(a, b)`:
/// allowed — RECURSIVE `WITH` whose UNION ALL arm uses CROSS JOIN LATERAL
/// with ORDER BY+LIMIT (:639-653), LIMIT-only (:657), ORDER BY-only (:673),
/// comma join + LATERAL with both (:713), and two chained comma-LATERAL
/// sources where the second references the first's alias n1 (:730-745);
/// rejected — plain parenthesized recursive blocks carrying ORDER BY
/// (:689-697) or LIMIT (:701-709), and an identical block wrapped in a
/// derived-table alias `sub` (:752-766), each failing with the
/// `ErrNotSupportedYet` text "ORDER BY / LIMIT in recursive query block"
/// (message source: `logical_plan_builder.go:4298`). Every case additionally
/// requires parsing to SUCCEED — all eight rejections are semantic gates in
/// plan building, not grammar errors.
#[test]
#[ignore = "go-parity-gap: needs the SQL->logical-plan builder's recursive-CTE arm and its buildingLateralSubquery gate"]
fn recursive_cte_lateral_order_by_limit_gating() {}

/// GO PORT of
/// `pkg/planner/core/lateral_join_test.go:790 TestLateralJoinMySQLCompatibility`.
///
/// Four compatibility cases: RIGHT JOIN LATERAL must be rejected with
/// "RIGHT JOIN is not supported with LATERAL" (:803-808); LATERAL MAY read a
/// column of an enclosing DERIVED table's alias (`FROM (SELECT a FROM t) AS j,
/// LATERAL (SELECT j.a) AS dt` builds cleanly, :810-812); but aliases of
/// tables INSIDE a derived table must NOT leak into the LATERAL scope —
/// `FROM (SELECT t1.a FROM t AS t1 JOIN t AS t2 USING(a)) AS j, LATERAL
/// (SELECT t1.a) AS dt` errors "Unknown column", twice, once per leaked inner
/// alias t1 (:816-819) and t2 (:823-826). Pins that LATERAL correlation
/// resolves against derived-table OUTPUT columns only.
#[test]
#[ignore = "go-parity-gap: lateral-scope name resolution and JOIN-type gating live in the unported PlanBuilder"]
fn lateral_join_mysql_compatibility_scopes() {}
