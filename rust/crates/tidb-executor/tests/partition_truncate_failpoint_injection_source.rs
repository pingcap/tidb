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

//! Port of `pkg/ddl/tests/partition/error_injection_test.go`:
//! `:65::TestTruncatePartitionListFailuresWithGlobalIndex` and
//! `:92::TestTruncatePartitionListFailures`.
//!
//! Both drive `testDDLWithInjectedErrors` (`:159`): after loading a fixed
//! DML history on a LIST-partitioned table, the TRUNCATE PARTITION job is
//! re-run once per injected `truncatePart{Cancel1,Fail1,Fail2,Fail3}`
//! failpoint (`truncateTests`, `:27-:59`). A ROLLBACK injection must answer
//! "Injected error by github.com/pingcap/tidb/pkg/ddl/truncatePart<name>"
//! and leave EVERY id stable — partition ids, index ids, `AddingDefinitions`
//! /`DroppingDefinitions`/`NewPartitionIDs` all empty, `SHOW CREATE TABLE`
//! byte-identical — while a RECOVERABLE injection must SUCCEED with fresh
//! partition ids (`:196-:206`). The follow-up DML battery (`:209-:216`)
//! then re-proves the surviving partitions' contents.
// go-parity-gap: this tier has neither failpoints nor an online-DDL job
// queue — `ALTER TABLE ... TRUNCATE PARTITION` applies synchronously
// (`crate::ddl::alter_table`), so injections cannot interleave and the
// rollback/recover distinction, the id-stability assertions and the
// `tidb_enable_global_index` session variable the GLOBAL variant needs
// (`:71`, unported) are all unobservable.
use tidb_executor::{run_create_table_on, Catalog};

#[test]
#[ignore]
fn truncate_partition_list_failures_with_global_index_injected_rollback_and_recovery() {
    let mut catalog = Catalog::default();
    // Go :67-:70 — the GLOBAL-index variant; the tier refuses the GLOBAL
    // unique index with 8264-style reasoning, so even the fixture cannot
    // build (measured for the plain-key spelling).
    let _ = run_create_table_on(
        "create table t (a int unsigned primary key nonclustered global, b int not null, \
         c varchar(255), unique index (c) global) partition by list(b) (\
         partition p0 values in (1,2,3), partition p1 values in (4,5,6), \
         partition p2 values in (7,8,9))",
        &mut catalog,
    );
    // Go :71-:150: beforeDML, injected `truncate partition p0,p2` per
    // failpoint, before/after result batteries.
}

/// Go `error_injection_test.go:92::TestTruncatePartitionListFailures`: the
/// plain (non-global) variant over `partition by list(a)`; the
/// `Cancel1/Fail1/Fail2/Fail3` matrix runs with `Fail1..3` skipped, so
/// `Cancel1`'s rollback path (error text, id stability, admin check clean,
/// afterDML battery ending in `afterResult` :141-:147) is the whole body.
// go-parity-gap: same missing carriers as the GLOBAL variant — no
// failpoints, no job queue, no cancel-then-rollback lifecycle; the
// truncate's successful path alone is pinned by the landed
// tests_ddl_partition_operations_sql port of TestDropAndTruncatePartition.
#[test]
#[ignore]
fn truncate_partition_list_failures_injected_rollback_keeps_ids_stable() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "create table t (a int unsigned primary key, b int not null, c varchar(255)) \
         partition by list(a) (partition p0 values in (1,2,3), \
         partition p1 values in (4,5,6), partition p2 values in (7,8,9))",
        &mut catalog,
    )
    .expect("the plain LIST fixture builds on this tier");
    // Go :107-:150 runs the injected-failure matrix over
    // `alter table t truncate partition p0,p2`.
}
