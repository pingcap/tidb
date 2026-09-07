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

//! Source guards for Go's common INSERT execution path.

#[test]
fn inserts_do_not_use_a_workload_specific_literal_execution_path() {
    let dml = include_str!("../src/driver/dml.rs");
    assert!(
        !dml.contains("fast_literal_shape") && !dml.contains("literal_value_rows"),
        "integer VALUES rows must use the ordinary INSERT row builder"
    );

    let table = include_str!("../src/kv_table.rs");
    assert!(
        !table.contains("if self.indexes.iter().any(|index| !index.clustered_primary)"),
        "the generic row writer must keep one index-maintenance path"
    );
}

#[test]
fn prepared_dml_uses_the_ordinary_statement_executor() {
    let dml = include_str!("../src/driver/dml.rs");
    for private_executor in [
        "PreparedInsertPlan",
        "PreparedPointUpdatePlan",
        "PreparedPointDeletePlan",
        "PreparedDmlPlanInner",
        "run_prepared_dml",
    ] {
        assert!(
            !dml.contains(private_executor),
            "prepared DML must not retain the private executor path {private_executor}"
        );
    }

    let session = include_str!("../../tidb-session/src/dispatch.rs");
    assert!(
        session.contains("execute_parsed_statement_with_dml_plan")
            && session.contains("run_insert_stmt_with_physical")
            && session.contains("run_update_stmt_with_physical")
            && session.contains("run_delete_stmt_with_physical"),
        "cached prepared DML must enter the ordinary statement funnel and DML executors"
    );
    assert!(
        dml.contains("PhysicalDmlRoot")
            && dml.contains("CachedDmlPlan")
            && dml.contains("rebuild_plan_for_cache_in_place"),
        "prepared DML must retain and recursively rebuild an ordinary physical DML root"
    );
    assert!(
        !dml.contains("PreparedDmlCacheKey")
            && !dml.contains("cached_keys")
            && !dml.contains("mark_cache_ready"),
        "prepared DML must not report hits from a seen-key pseudo-cache"
    );
    let statement_context = include_str!("../../tidb-session/src/stmt_ctx.rs");
    assert!(
        !statement_context.contains("fast_statement_context"),
        "the deleted cache-only write path must not retain a private statement context"
    );
    let sql_prepare = include_str!("../../tidb-session/src/prepared_statements.rs");
    assert!(
        !sql_prepare.contains("bind_parameters("),
        "SQL PREPARE must bind its retained AST instead of restoring and reparsing SQL"
    );
    assert!(
        sql_prepare.contains("execute_cached_prepared_dml"),
        "SQL and binary prepared statements must share the retained DML root and ordinary executor"
    );

    let server = include_str!("../../tidb-server/src/cluster_session_node/mod.rs");
    assert!(
        !server.contains("prepared DML cache was invalidated"),
        "cache misses must be typed admission decisions, not error-string fallbacks"
    );
}

#[test]
fn fresh_dml_builds_the_same_physical_select_child_as_prepared_dml() {
    let dml = include_str!("../src/driver/dml.rs");
    // Each ordinary DML entrypoint delegates to the shared stats variant,
    // which plans a fresh source root through `physical_dml_plan` when no
    // physical plan was handed in (a cache hit or a retaining caller).
    for entrypoint in [
        "pub fn run_insert_stmt_with_physical(",
        "pub fn run_update_stmt_with_physical(",
        "pub fn run_delete_stmt_with_physical(",
    ] {
        assert!(
            dml.contains(entrypoint),
            "missing ordinary DML entrypoint {entrypoint}"
        );
    }
    for delegation in [
        "run_insert_stmt_with_physical_and_stats(insert, catalog, current_db, ctx, physical_plan, None)",
        "run_update_stmt_with_physical_and_stats(update, catalog, current_db, ctx, physical_plan, None)",
        "run_delete_stmt_with_physical_and_stats(delete, catalog, current_db, ctx, physical_plan, None)",
    ] {
        assert!(
            dml.contains(delegation),
            "the ordinary DML entrypoint must reach the shared fresh-planning variant: {delegation}"
        );
    }
    assert!(
        dml.matches("physical_dml_plan(").count() >= 3,
        "each DML kind plans its fresh source root through physical_dml_plan"
    );
    assert!(
        dml.contains("insert.source.as_deref()"),
        "INSERT must retain and execute every QueryStmt source, including set operations"
    );
    assert!(
        !dml.contains("Fresh statements pass `None`"),
        "fresh DML must not be documented as bypassing Go's retained SelectPlan boundary"
    );
}
