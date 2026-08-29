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

//! Ownership guard for prepared SELECT cache hits.

#[test]
fn cached_select_execution_is_borrowed_across_the_statement_retry_boundary() {
    let server = include_str!("../src/cluster_session_node/mod.rs");
    let session = include_str!("../../tidb-session/src/dispatch.rs");
    let executor = include_str!("../../tidb-executor/src/driver/access.rs");

    assert!(server.contains(".execute_prepared_select(cached, &sql)"));
    assert!(session.contains("execution: &tidb_executor::PreparedSelectExecution"));
    assert!(!executor.contains("#[derive(Clone, Debug)]\npub struct PreparedSelectExecution"));
}

#[test]
fn cached_select_hit_does_not_build_a_second_statement_context() {
    let source = include_str!("../../tidb-session/src/prepared_ast.rs");
    let function = source
        .split_once("pub fn bind_cached_prepared_select_for_statement(")
        .expect("prepared SELECT cache binder")
        .1;
    let cache_probe = function
        .find("plan.bind_cached_for_statement(")
        .expect("retained physical tree is probed without a planner context");
    let planner_context = function
        .find("let ctx = self.statement_context(false);")
        .expect("a cache miss still gets a planner context");
    let cache_fill = function[planner_context..]
        .find("plan.bind_for_statement(")
        .map(|offset| planner_context + offset)
        .expect("a cache miss fills the retained physical tree");

    assert!(cache_probe < planner_context && planner_context < cache_fill);
}

#[test]
fn cached_select_hit_rebinds_the_retained_ast_in_place() {
    let access = include_str!("../../tidb-executor/src/driver/access.rs");
    let planner = include_str!("../../tidb-executor/src/driver/planner_bridge.rs");
    let bind_inner = access
        .split_once("fn bind_inner(")
        .expect("prepared SELECT cache binder")
        .1
        .split_once("fn stats_version_hash(")
        .expect("cache binder boundary")
        .0;
    let cache_hit_path = bind_inner
        .split_once("            None => {")
        .expect("cache miss arm")
        .0;

    assert!(!cache_hit_path.contains("bind_prepared_statement"));
    assert!(bind_inner.contains("bind_prepared_statement"));
    assert!(planner.contains("bind_prepared_statement_in_place(&mut self.statement, values)"));
    assert!(!access.contains("select: tidb_ast::SelectStmt,\n    decision:"));
}

#[test]
fn cached_select_key_reuses_the_typed_session_environment() {
    let source = include_str!("../../tidb-session/src/prepared_ast.rs");
    let binder = source
        .split_once("pub fn bind_cached_prepared_select_for_statement(")
        .expect("prepared SELECT cache binder")
        .1;

    assert!(binder.contains("self.prepared_plan_cache_environment_for_binding(binding_sql)"));
    assert!(!binder.contains("PreparedPlanCacheEnvironment::new("));
    assert!(!binder.contains("get_system(\"sql_select_limit\")"));
    assert!(!binder.contains("TIDB_SNAPSHOT"));
    assert!(!binder.contains("TIDB_READ_STALENESS"));
}

#[test]
fn cached_execution_trusts_the_shared_historical_read_admission() {
    let source = include_str!("../../tidb-session/src/dispatch.rs");
    let cached_execution = source
        .split_once("pub fn execute_prepared_point_get(")
        .expect("cached point-get execution")
        .1
        .split_once("fn execute_parsed_statement(")
        .expect("ordinary execution boundary")
        .0;
    let ordinary_execution = source
        .split_once("fn execute_parsed_statement(")
        .expect("ordinary execution")
        .1
        .split_once("fn refuse_pinned_historical_read(")
        .expect("historical-read helper boundary")
        .0;
    let binder = include_str!("../../tidb-session/src/prepared_ast.rs");
    let dml_binder = binder
        .split_once("pub fn bind_cached_prepared_dml_for_statement(")
        .expect("cached DML binder")
        .1
        .split_once("pub fn bind_cached_prepared_select(")
        .expect("cached DML binder boundary")
        .0;

    assert!(!cached_execution.contains("refuse_pinned_historical_read"));
    assert!(ordinary_execution.contains("self.refuse_pinned_historical_read()?"));
    assert!(
        dml_binder.contains("self.prepared_plan_cache_environment_for_binding(binding_sql)")
    );
}

#[test]
fn prepared_execution_reuses_the_typed_session_time_zone() {
    let source = include_str!("../../tidb-session/src/stmt_ctx.rs");
    let accessor = source
        .split_once("pub fn session_time_zone(")
        .expect("session time-zone accessor")
        .1
        .split_once("fn resolve_session_time_zone(")
        .expect("typed time-zone resolver")
        .0;

    assert!(accessor.contains("self.statement_var_snapshot().time_zone.clone()"));
    assert!(!accessor.contains("get_system(\"time_zone\")"));
    assert!(!accessor.contains("system_location()"));
}

#[test]
fn prepared_prelock_classification_borrows_the_retained_ast() {
    let classify = include_str!("../../tidb-session/src/classify.rs");
    let access = include_str!("../../tidb-executor/src/access_path.rs");
    let server = include_str!("../src/cluster_session_node/mod.rs");

    assert!(!classify.contains("pub fn prepared_statement_prelock_keys"));
    assert!(!classify.contains("bind_statement(stmt.clone()"));
    assert!(access.contains(
        "pessimistic_statement_prelock_keys(\n    stmt: &tidb_ast::Stmt,\n    params: &[Datum],"
    ));
    assert!(!server.contains(".prepared_statement_prelock_keys"));
}
