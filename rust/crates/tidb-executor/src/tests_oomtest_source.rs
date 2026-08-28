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

//! Ports of Go `pkg/executor/test/oomtest/oom_test.go` (batch items
//! 1046–1050): which write executors charge the session query quota
//! (`MemQuotaQuery`) and trip the OOM action when it is exceeded.
//!
//! OBSERVATION SEAM. Go observes the trip through a zap log hook
//! (`oomCapture.Write` parses the `[8001] ... holds` tracker dump and filters
//! the `expensive_query during bootstrap phase` message); this tier has no
//! log-capture surface, so the same trip is observed through the typed
//! verdict the action produces — `MemoryExceedForQuery` (8175) — plus an
//! ACCEPT-CONTROL: the identical statement under the shipped 1GiB default
//! must succeed and write exactly what the cancelled run must not. Quotas
//! use `1` (any accounted row crosses it) rather than Go's 244/500: per the
//! `WriteMemory` divergence note in `mem_quota.rs`, this tier accounts datum
//! rows it actually holds instead of Go's chunk capacity, so only quotas far
//! from the boundary classify identically, which 1 and the 1GiB default are.

use tidb_datatype::Datum;
use tidb_util::memory::DEF_LOG_PRIORITY;

use crate::mem_quota::{OomAction, SessionMemory};
use crate::{
    run_create_table_on, run_delete_on, run_insert_on, run_select_on, run_update_on, Catalog,
    StmtContext,
};

fn permitting() -> StmtContext {
    StmtContext::for_query()
}

/// A context whose quota cancels any statement that accounts a single row
/// (Go: `tk.Session().GetSessionVars().MemQuotaQuery = 1`).
fn cancelling() -> StmtContext {
    StmtContext::for_query().with_mem_quota(1, OomAction::Cancel)
}

fn is_memory_exceeded(error: &crate::DriverError) -> bool {
    matches!(error, crate::DriverError::MemoryExceedForQuery { .. })
}

fn rows(catalog: &Catalog, sql: &str) -> Vec<Vec<Datum>> {
    run_select_on(sql, catalog, &permitting()).unwrap()
}

/// Go `pkg/executor/test/oomtest/oom_test.go:50::TestMemTracker4UpdateExec`:
/// the UPDATE executor's tracker is wired to the session query quota. Go
/// sets `MemQuotaQuery = 244` and requires the OOM hook to fire for the
/// 3-row update; here the same statement over a tiny quota yields the 8175
/// memory-exceeded verdict and changes nothing, while the default quota
/// completes it.
#[test]
fn update_exec_charges_the_session_query_quota() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t_MemTracker4UpdateExec (id int, a int, b int, index idx_a(a))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into t_MemTracker4UpdateExec values (1,1,1), (2,2,2), (3,3,3)",
        &mut catalog,
        &permitting(),
    )
    .unwrap();

    let error = run_update_on(
        "update t_MemTracker4UpdateExec set a = 4",
        &mut catalog,
        &cancelling(),
    )
    .unwrap_err();
    assert!(is_memory_exceeded(&error), "expected 8175, got {error:?}");
    assert_eq!(
        rows(&catalog, "select id, a, b from t_MemTracker4UpdateExec order by id"),
        vec![
            vec![Datum::Int(1), Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(3), Datum::Int(3), Datum::Int(3)],
        ],
        "a cancelled UPDATE must leave every row as it found it"
    );

    // Accept-control: the same statement under the default quota (Go resets
    // the quota to -1 between arms).
    assert_eq!(
        run_update_on(
            "update t_MemTracker4UpdateExec set a = 4",
            &mut catalog,
            &permitting()
        )
        .unwrap(),
        3
    );
}

/// Go `pkg/executor/test/oomtest/oom_test.go:70
/// ::TestMemTracker4InsertAndReplaceExec`: plain INSERT, REPLACE and
/// INSERT...SELECT all charge the session query quota (over a tiny quota
/// each trips the OOM action; at the default each completes), and REPLACE
/// over distinct keys at the default writes without tripping. Go's
/// batch-insert arms (`DMLBatchSize=1`, `BatchInsert=true`) need the
/// session-variable surface and are not reproduced here.
#[test]
fn insert_replace_and_insert_select_charge_the_session_query_quota() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t_MemTracker4InsertAndReplaceExec (id int, a int, b int, index idx_a(a))",
        &mut catalog,
    )
    .unwrap();
    // Go's source table `t` (same shape, 3 rows) for the INSERT...SELECT arms.
    run_create_table_on("create table t_src (id int, a int, b int)", &mut catalog).unwrap();
    run_insert_on("insert into t_src values (1,1,1), (2,2,2), (3,3,3)", &mut catalog, &permitting()).unwrap();

    // INSERT over the quota: 8175, nothing written.
    let error = run_insert_on(
        "insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)",
        &mut catalog,
        &cancelling(),
    )
    .unwrap_err();
    assert!(is_memory_exceeded(&error), "expected 8175, got {error:?}");
    // (Full-row selects, not `count(*)`: the planner covers `count(*)` from
    // idx_a, and the covering-index read decode is a pre-existing gap this
    // batch must not exercise.)
    assert_eq!(
        rows(&catalog, "select id, a, b from t_MemTracker4InsertAndReplaceExec"),
        Vec::<Vec<Datum>>::new()
    );

    // Accept-control (Go: quota back to -1).
    run_insert_on(
        "insert into t_MemTracker4InsertAndReplaceExec values (1,1,1), (2,2,2), (3,3,3)",
        &mut catalog,
        &permitting(),
    )
    .unwrap();

    // REPLACE over the quota: 8175 (Go expects the hook to fire), then the
    // default-quota REPLACE over fresh keys completes.
    let error = run_insert_on(
        "replace into t_MemTracker4InsertAndReplaceExec values (9,9,9)",
        &mut catalog,
        &cancelling(),
    )
    .unwrap_err();
    assert!(is_memory_exceeded(&error), "expected 8175, got {error:?}");
    run_insert_on(
        "replace into t_MemTracker4InsertAndReplaceExec values (9,9,9)",
        &mut catalog,
        &permitting(),
    )
    .unwrap();

    // INSERT ... SELECT over the quota (Go's `insert ... select * from t`
    // arm): 8175, nothing copied.
    let error = run_insert_on(
        "insert into t_MemTracker4InsertAndReplaceExec select * from t_src",
        &mut catalog,
        &cancelling(),
    )
    .unwrap_err();
    assert!(is_memory_exceeded(&error), "expected 8175, got {error:?}");
    assert_eq!(
        rows(&catalog, "select id, a, b from t_MemTracker4InsertAndReplaceExec order by id"),
        vec![
            vec![Datum::Int(1), Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(3), Datum::Int(3), Datum::Int(3)],
            vec![Datum::Int(9), Datum::Int(9), Datum::Int(9)],
        ]
    );
    // Accept-control for the select source. (Row ORDER after the mixed
    // insert/replace sequence is an allocation artifact -- Go's test checks
    // the OOM hook, not row order -- so compare as a multiset.)
    run_insert_on(
        "insert into t_MemTracker4InsertAndReplaceExec select * from t_src",
        &mut catalog,
        &permitting(),
    )
    .unwrap();
    let mut final_rows =
        rows(&catalog, "select id, a, b from t_MemTracker4InsertAndReplaceExec");
    // Datum does not implement Ord; the rendered text form sorts stably
    // for this integer-only fixture.
    final_rows.sort_by_key(|row: &Vec<Datum>| {
        row.iter()
            .map(|d| match d {
                Datum::Int(value) => format!("{value:012}"),
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
            .join(",")
    });
    assert_eq!(
        final_rows,
        vec![
            vec![Datum::Int(1), Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(1), Datum::Int(1), Datum::Int(1)],
            vec![Datum::Int(2), Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(2), Datum::Int(2), Datum::Int(2)],
            vec![Datum::Int(3), Datum::Int(3), Datum::Int(3)],
            vec![Datum::Int(3), Datum::Int(3), Datum::Int(3)],
            vec![Datum::Int(9), Datum::Int(9), Datum::Int(9)],
        ]
    );
}

/// Go `pkg/executor/test/oomtest/oom_test.go:164::TestMemTracker4DeleteExec`
/// (single-table arm): DELETE charges the session query quota — at the
/// default it removes every row without tripping; over a tiny quota it
/// yields 8175 and removes nothing.
#[test]
fn delete_exec_charges_the_session_query_quota() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table MemTracker4DeleteExec1 (id int, a int, b int, index idx_a(a), index idx_b(b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "insert into MemTracker4DeleteExec1 values (1,1,1), (2,2,2), (3,3,3)",
        &mut catalog,
        &permitting(),
    )
    .unwrap();

    // Go first deletes at the default quota and requires NO trip.
    run_delete_on("delete from MemTracker4DeleteExec1", &mut catalog, &permitting()).unwrap();
    assert_eq!(
        rows(&catalog, "select id from MemTracker4DeleteExec1"),
        Vec::<Vec<Datum>>::new()
    );

    // Re-seed, then the same statement over a tiny quota must trip and
    // remove nothing.
    run_insert_on(
        "insert into MemTracker4DeleteExec1 values (1,1,1), (2,2,2), (3,3,3)",
        &mut catalog,
        &permitting(),
    )
    .unwrap();
    let error =
        run_delete_on("delete from MemTracker4DeleteExec1", &mut catalog, &cancelling()).unwrap_err();
    assert!(is_memory_exceeded(&error), "expected 8175, got {error:?}");
    assert_eq!(
        rows(&catalog, "select id from MemTracker4DeleteExec1"),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)], vec![Datum::Int(3)]]
    );
}

/// Go `oom_test.go`'s multi-table-delete arm (`delete t1, t2 from t1 join t2
/// ...` with `EnabledRateLimitAction`, the `disableFixedRowCountHint`
/// failpoint and quota 500): the multi-table DELETE syntax, the coprocessor
/// failpoint and the rateLimitAction-delegates-to-fallback log message have
/// no Rust surface.
#[test]
#[ignore = "go-parity-gap: multi-table DELETE, the disableFixedRowCountHint failpoint and rateLimitAction fallback logging (oom_test.go:186-220) are unported"]
fn multi_table_delete_rate_limit_action_delegates_to_fallback() {}

/// Go `pkg/executor/test/oomtest/oom_test.go:292::TestOOMActionPriority`:
/// after a five-way join completes, the surviving OOM action is the
/// log-level one — Go reads `StmtCtx.MemTracker.GetFallbackForTest(true)`
/// and requires `GetPriority() == DefLogPriority` ("all actions are finished
/// and removed"); here the session root's `get_fallback_for_test(true)`
/// returns the installed log action with the same `DEF_LOG_PRIORITY`, over a
/// join that returns Go's checked row `1 1 1 1 1`.
#[test]
fn after_the_query_the_surviving_oom_action_is_the_log_level_one() {
    let session = SessionMemory::new(-1, OomAction::Log, 7);
    let mut catalog = Catalog::default();
    for name in ["t0", "t1", "t2", "t3", "t4"] {
        run_create_table_on(&format!("create table {name}(a int)"), &mut catalog).unwrap();
        run_insert_on(&format!("insert into {name} values(1)"), &mut catalog, &permitting())
            .unwrap();
    }

    let statement = session.statement();
    let ctx = StmtContext::for_query().with_statement_memory(statement.clone());
    let got = run_select_on(
        "select * from t0 join t1 join t2 join t3 join t4 order by t0.a",
        &catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        got,
        vec![vec![Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Int(1), Datum::Int(1)]],
        "Go: .Check(testkit.Rows(\"1 1 1 1 1\"))"
    );

    // The surviving action after the statement: the log-level one.
    let survivor = statement
        .session_tracker()
        .get_fallback_for_test(true)
        .expect("the configured oom action survives the statement");
    assert_eq!(
        survivor.get_priority(),
        DEF_LOG_PRIORITY,
        "Go: action.GetPriority() == DefLogPriority"
    );
    statement.finish_statement();
}

/// Go `pkg/executor/test/oomtest/oom_test.go:37::TestMain`: goleak, the
/// `oomCapture` zap hook registration and config bootstrap only.
#[test]
#[ignore = "go-parity-gap: oomtest TestMain is goleak/log-hook suite bootstrap; no statement behavior"]
fn oomtest_main_is_bootstrap_only() {}
