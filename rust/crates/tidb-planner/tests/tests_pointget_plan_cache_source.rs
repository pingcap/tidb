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

//! Ports for `pkg/planner/core/tests/pointget` on `origin/master`
//! (`pkg/planner.part19`, items 1136–1140 of all 1278 `Test*`/`Benchmark*`
//! declarations under `pkg/planner/`; package `pointget`). Item 1136,
//! `main_test.go:25 TestMain` (testsetup bootstrap + goleak filter list), is
//! recorded as skipped-reason in the batch receipt — no behavior to assert —
//! following crate precedent for bootstrap-only families.
//!
//! Family inventory from `point_get_plan_test.go`:
//!
//! * [`point_get_plan_cache_select_update_delete_hit_matrix`] /
//!   [`point_get_plan_cache_for_next_gen_lock_suffix_variant`] pin the
//!   prepared-plan-cache hit matrix over `metrics.PlanCacheCounter` while
//!   PointGet plans serve select/update/delete through prepared statements on
//!   `t(a bigint unsigned primary key, b int, c int, key idx_bc(b,c))`. Go
//!   splits the behavior across kernel gates (`kerneltype.IsNextGen` skip at
//!   :35, inverse at :156) that also change two golden rows: under next-gen
//!   the Update/Delete children read `└─Point_Get root table:t handle:1, lock`
//!   instead of `handle:1` (:196-197/:203-204 vs :141/:146).
//! * [`point_get_id_fresh_statement_build_reallocates_ids_from_one`] runs for
//!   real and pins what this crate owns of `point_get_plan_test.go:277
//!   TestPointGetId` ("Test that the plan id will be reset before optimization
//!   every time"). Go drives `session.Parse` → `core.Preprocess` →
//!   `planner.Optimize` twice against the session-plan-id counter and asserts
//!   `p.ID() == 1` both times; that works because every pass resets the
//!   counter before building — `buildLogicalPlan`
//!   (`pkg/planner/optimize.go:904`) `sctx.GetSessionVars().PlanID.Store(0)`
//!   before `builder.Build`, and again inside `TryFastPlan`
//!   (`pkg/planner/core/point_get_plan.go:97`) right before the fast-path
//!   conversion that decides the whole query. This crate has no
//!   session/optimizer driver yet: the counter itself is an explicit
//!   [`PlanIdAllocator`](tidb_planner::plan_base::PlanIdAllocator)
//!   (`plan_base.rs`: fresh allocator hands out `1`, monotonic afterwards),
//!   which the builder consumes once per statement. The running test pins the
//!   equivalent observable at that seam — two independent
//!   `build_select("select c2 from t where c1 = 1")` passes over
//!   `t (c1 int primary key, c2 int)` allocate identical id sets starting at
//!   1, ids are unique inside one pass, and a SHARED allocator provably does
//!   not restart (min(second) > max(first)), which is exactly the regression
//!   the Go-side resets prevent.
//! * [`point_get_id_full_pipeline_reset_site_documentary`] is the ignored
//!   documentary twin for the tail that has no Rust owner yet: parse→
//!   preprocess→Optimize over a live session, TryFastPlan admission, and the
//!   returned PhysicalPointGet carrying `ID()==1`.
//! * [`issue_20692_pessimistic_write_chain_blocks_conflicting_update`]
//!   (Go :302) interleaves three pessimistic transactions so a row delete,
//!   then an insert of the same PK, makes a third txn's conflicting UPDATE
//!   block until both commit (`select * from t` → `10 20 30 40`).
//! * [`issue_18042_max_execution_time_memory_quota_hints_reach_stmt_ctx`]
//!   (Go :342) pins that `MAX_EXECUTION_TIME(100), MEMORY_QUOTA(1 MB)` land on
//!   `StmtCtx.MaxExecutionTime == 100` and `StmtCtx.MemQuotaQuery == 1<<20`.

use tidb_ast::{QueryStmt, Stmt};
use tidb_datatype::FieldTypeCode;
use tidb_expr::{SessionTimeZone, ZonedNoColumns};
use tidb_planner::expression_rewriter::ColumnIdAllocator;
use tidb_planner::logical::LogicalPlan;
use tidb_planner::plan_base::PlanIdAllocator;
use tidb_planner::plan_builder::PlanBuilder;
use tidb_planner::plan_builder::catalog::{SourceColumn, SourceTable, TableSource};

/// Go `point_get_plan_test.go:274`: `create table t (c1 int primary key, c2 int)`.
struct PointGetCatalog {
    t: SourceTable,
}

impl TableSource for PointGetCatalog {
    fn current_database(&self) -> &str {
        "test"
    }

    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable> {
        if db_name.eq_ignore_ascii_case("test")
            && table_name.eq_ignore_ascii_case(&self.t.table_name)
        {
            Some(&self.t)
        } else {
            None
        }
    }

    fn database_exists(&self, db_name: &str) -> bool {
        db_name.eq_ignore_ascii_case("test")
    }
}

fn point_get_column(offset: usize, name: &str, primary: bool) -> SourceColumn {
    let mut ret_type = tidb_datatype::FieldType::new(FieldTypeCode::Long);
    ret_type.set_flen(11);
    ret_type.set_decimal(0);
    SourceColumn {
        id: (offset + 1) as i64,
        name: name.to_owned(),
        is_primary_key: primary,
        offset,
        ret_type,
        is_public: true,
        is_hidden: false,
        is_virtual_generated: false,
        generated_expr: None,
    }
}

/// `t (c1 int primary key, c2 int)` as declared at `point_get_plan_test.go:273`.
fn point_get_catalog() -> PointGetCatalog {
    let mut t = SourceTable::default();
    t.table_id = 201;
    t.db_name = "test".to_owned();
    t.table_name = "t".to_owned();
    t.physical_table_id = 201;
    t.columns = vec![
        point_get_column(0, "c1", true),
        point_get_column(1, "c2", false),
    ];
    PointGetCatalog { t }
}

/// Go `point_get_plan_test.go:275`.
const POINT_GET_QUERY: &str = "select c2 from t where c1 = 1";

fn parse_point_get_query(sql: &str) -> tidb_ast::SelectStmt {
    match tidb_parser::parse(sql).expect("the point-get SQL parses") {
        Stmt::Query(query) => match query.into_inner() {
            QueryStmt::Select(select) => *select,
            other => panic!("expected a SELECT, got {other:?}"),
        },
        other => panic!("expected a SELECT, got {other:?}"),
    }
}

/// Every plan id in the subtree, pre-order.
fn collect_plan_ids(plan: &LogicalPlan, out: &mut Vec<i32>) {
    out.push(plan.id());
    for child in plan.base().children() {
        collect_plan_ids(child, out);
    }
}

/// Builds `select c2 from t where c1 = 1` and returns its sorted plan-id set.
///
/// One call == one Go optimize pass: the caller decides whether the allocator
/// is fresh (`optimize.go:904` reset) or shared with earlier passes.
fn build_once(catalog: &PointGetCatalog) -> Vec<i32> {
    let ctx = ZonedNoColumns(SessionTimeZone::utc());
    let plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let mut builder = PlanBuilder::new(
        catalog,
        &ctx,
        &plan_ids,
        &column_ids,
        SessionTimeZone::utc(),
    );
    let select = parse_point_get_query(POINT_GET_QUERY);
    let (plan, _) = builder
        .build_select(&select)
        .expect("the point-get query builds");
    let mut ids = Vec::new();
    collect_plan_ids(&plan, &mut ids);
    ids.sort_unstable();
    ids
}

/// Rust side of `pkg/planner/core/tests/pointget/point_get_plan_test.go:277
/// TestPointGetId` — the plan-id counter restarts from 1 for every top-level
/// statement build, never inside one.
#[test]
fn point_get_id_fresh_statement_build_reallocates_ids_from_one() {
    let catalog = point_get_catalog();

    // Two passes, each mirroring one Optimize: `buildLogicalPlan` stores 0 into
    // PlanID before Build (optimize.go:904) and `TryFastPlan` repeats it before
    // the point-get conversion (point_get_plan.go:97). A fresh allocator is
    // this crate's model of that reset.
    let mut first = build_once(&catalog);
    let second = build_once(&catalog);

    assert!(
        !first.is_empty(),
        "the built statement carries at least one plan id"
    );
    assert_eq!(first.first(), Some(&1), "ids start at 1 after the reset");
    assert_eq!(first, second, "each pass reallocates the identical id set");
    let len = first.len();
    first.dedup();
    assert_eq!(first.len(), len, "ids are unique inside one statement");

    // Contrast arm: WITHOUT the reset nothing restarts — a shared counter keeps
    // counting past the first pass. This is the exact failure mode the Go test
    // guards against; pass two would break if reset were dropped upstream.
    let ctx = ZonedNoColumns(SessionTimeZone::utc());
    let shared_plan_ids = PlanIdAllocator::new();
    let column_ids = ColumnIdAllocator::new();
    let select = parse_point_get_query(POINT_GET_QUERY);
    let build_shared = |catalog: &'_ PointGetCatalog| {
        let mut builder = PlanBuilder::new(
            catalog,
            &ctx,
            &shared_plan_ids,
            &column_ids,
            SessionTimeZone::utc(),
        );
        builder
            .build_select(&select)
            .expect("the point-get query builds")
            .0
            .id()
    };
    let _first_root = build_shared(&catalog);
    let second_root_shared = build_shared(&catalog);
    assert!(
        second_root_shared > 1,
        "a shared allocator keeps allocating: root of pass two is id {second_root_shared}, not 1"
    );
}

/// Documentary twin for the pipeline part of Go TestPointGetId that has no
/// Rust owner: session Parse + Preprocess + planner.Optimize ending in a
/// PhysicalPointGet whose `ID()` equals 1 on every pass.
#[test]
#[ignore = "go-parity-gap: no session/Optimize/TryFastPlan driver exists, so the physical Point_Get plan carrying ID()==1 cannot be produced"]
fn point_get_id_full_pipeline_reset_site_documentary() {}

/// Go `point_get_plan_test.go:34 TestPointGetPlanCache` (classic-kernel gate
/// at :35 skips it under next-gen): prepared-plan-cache hit matrix over
/// `metrics.PlanCacheCounter` ("prepare" label) across select/update/delete
/// point-get statements plus the bigint-unsigned negative-param arm
/// (`@p1=-1` yields zero rows, `@p2=1` yields `1`, hit count stays at 2).
#[test]
#[ignore = "go-parity-gap: prepare/execute round-trips and metrics.PlanCacheCounter need the session+executor stack"]
fn point_get_plan_cache_select_update_delete_hit_matrix() {}

/// Go `point_get_plan_test.go:155 TestPointGetPlanCacheForNextGen` — next-gen
/// gate twin of the previous family; only the Update/Delete explain rows grow
/// a `, lock` suffix (:196-197, :203-204).
#[test]
#[ignore = "go-parity-gap: same session+executor boundary, plus Go's kerneltype.IsClassic gate has no Rust counterpart"]
fn point_get_plan_cache_for_next_gen_lock_suffix_variant() {}

/// Go `point_get_plan_test.go:302 TestIssue20692`: three pessimistic
/// transactions; tk1 deletes `(1,1,1)`, tk2 inserts `(1,2,3,4)` (blocking on
/// tk1), tk3's conflicting UPDATE on `(1,2,3)` must stay blocked while tk2
/// holds the lock; final committed state is `10 20 30 40`.
#[test]
#[ignore = "go-parity-gap: cross-session pessimistic lock ordering needs transactional execution"]
fn issue_20692_pessimistic_write_chain_blocks_conflicting_update() {}

/// Go `point_get_plan_test.go:342 TestIssue18042`: `MAX_EXECUTION_TIME(100),
/// MEMORY_QUOTA(1 MB)` hints leave `StmtCtx.MemQuotaQuery == 1<<20` and
/// `StmtCtx.MaxExecutionTime == 100` on the session after the statement runs.
#[test]
#[ignore = "go-parity-gap: statement-hint application writes StmtCtx fields that no Rust surface carries yet"]
fn issue_18042_max_execution_time_memory_quota_hints_reach_stmt_ctx() {}
