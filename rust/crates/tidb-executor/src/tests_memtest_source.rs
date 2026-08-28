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

//! Ports of Go `pkg/executor/test/memtest/mem_test.go` (batch items
//! 1043–1045): the statement memory tracker's release-on-cleanup contract
//! and the global memory arbitrator's variable surface.

use crate::mem_quota::{OomAction, SessionMemory};
use crate::{
    run_create_table_on, run_insert_on, run_select_on, run_update_on, Catalog, StatementMemory,
    StmtContext,
};

/// Go `pkg/executor/test/memtest/mem_test.go:26
/// ::TestInsertUpdateTrackerOnCleanUp`: after each completed INSERT and
/// UPDATE statement, the session's memory accounting is back to the
/// pre-statement level — the per-statement tracker tree is attached for the
/// statement and released at cleanup, so a batch of writes leaves no
/// residual consumption behind (Go compares
/// `StmtCtx.MemTracker.BytesConsumed()` before and after the batch).
#[test]
fn insert_update_tracker_bytes_return_to_baseline_on_cleanup() {
    let session = SessionMemory::new(-1, OomAction::Cancel, 7);
    let mut catalog = Catalog::default();
    run_create_table_on("create table t (id int)", &mut catalog).unwrap();

    let statement = |memory: &SessionMemory| -> StatementMemory { memory.statement() };

    // Go: three `insert t (id) values (...)`, then the consumed bytes must
    // equal the pre-batch reading.
    let baseline = session.bytes_consumed();
    for id in 1..=3 {
        let memory = statement(&session);
        let ctx = StmtContext::for_query().with_statement_memory(memory.clone());
        run_insert_on(&format!("insert into t (id) values ({id})"), &mut catalog, &ctx).unwrap();
        memory.finish_statement();
        assert_eq!(
            session.bytes_consumed(),
            baseline,
            "insert {id}: statement cleanup must release every accounted byte"
        );
    }

    // Go: the same contract for three UPDATEs.
    for id in 4..=6 {
        let memory = statement(&session);
        let ctx = StmtContext::for_query().with_statement_memory(memory.clone());
        run_update_on(
            &format!("update t set id = {id} where id = {}", id - 3),
            &mut catalog,
            &ctx,
        )
        .unwrap();
        memory.finish_statement();
        assert_eq!(
            session.bytes_consumed(),
            baseline,
            "update {}: statement cleanup must release every accounted byte",
            id - 3
        );
    }

    // The writes actually landed.
    let rows = run_select_on("select id from t order by id", &catalog, &StmtContext::for_query())
        .unwrap();
    assert_eq!(rows.len(), 3);
}

/// Go `pkg/executor/test/memtest/mem_test.go:50::TestGlobalMemArbitrator`:
/// the process-global arbitrator driven through `SET GLOBAL
/// tidb_mem_arbitrator_mode` / `tidb_mem_arbitrator_soft_limit` /
/// `tidb_mem_arbitrator_wait_averse` / `tidb_mem_arbitrator_query_reserved`
/// (session-scope refusals, `default` resets, `auto`/float-rate soft-limit
/// parsing, resource-group hints, `[executor:8180]` cancellation reasons and
/// the arbitrator's ExecMetrics counters). The sysvar SET/SELECT surface and
/// the process-global singleton plus resource groups live above this tier;
/// the arbitrator CORE those variables drive is separately pinned by
/// `mem_quota.rs`'s unit tests (`a_statement_root_registers_and_releases_its_global_arbitrator_pool`,
/// `nolimit_skips_global_memory_arbitration_but_reserve_promotes_immediately`)
/// over `tidb-util`'s `MemArbitrator`.
#[test]
#[ignore = "go-parity-gap: SET GLOBAL/@ arbitrator variables, the global-arbitrator singleton, resource groups and 8180 cancellation text are session/process surfaces unported at this tier"]
fn global_mem_arbitrator_modes_limits_and_cancel_reasons() {}

/// Go `pkg/executor/test/memtest/main_test.go:26::TestMain`: goleak and
/// config bootstrap only.
#[test]
#[ignore = "go-parity-gap: memtest TestMain is goleak/config suite bootstrap; no statement behavior"]
fn memtest_main_is_bootstrap_only() {}
