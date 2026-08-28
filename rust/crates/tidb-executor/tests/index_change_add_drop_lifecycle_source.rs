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

//! Ports of the `pkg/ddl/index_change_test.go` family (part6 items 331–334)
//! and `pkg/ddl/index_cop_test.go` (item 335) of the package's `func
//! Test*`/`func Benchmark*` declarations sorted by file and line, read from
//! `origin/master`.
//!
//! Go's TestIndexChange observes the ADD/DROP INDEX jobs THROUGH their schema
//! states, running raw-table DML against the delete-only, write-only and
//! public versions of the table mid-job. This tier has no schema states, so
//! the state-machine halves are `#[ignore]`d documentaries; the serialized
//! outer contract — add an index over populated rows, read through it, drop
//! it, and find the meta clean — is asserted live against the storage-backed
//! catalog.

use tidb_datatype::Datum;
use tidb_executor::driver::Catalog;
use tidb_executor::{admin_check, ddl, run_insert_on, run_select_on, KvTable, RowDecodeContext, StmtContext, TableEntry};

fn kv_table(catalog: &Catalog, database: &str, name: &str) -> KvTable {
    match catalog.table_in(database, name) {
        Some(TableEntry::Kv(table)) => table.clone(),
        _ => panic!("expected a storage-backed table {database}.{name}"),
    }
}

fn datum_text(value: &Datum) -> String {
    match value {
        Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
        Datum::Int(i) => i.to_string(),
        Datum::UInt(u) => u.to_string(),
        other => panic!("unexpected datum {other:?}"),
    }
}

// --- TestIndexChange (pkg/ddl/index_change_test.go:39) ---
//
// Go creates `t (c1 int primary key, c2 int)` with rows (1,1),(2,2),(3,3),
// adds index `c2(c2)` and requires the job's row count at StatePublic to be
// exactly 3 (the backfill indexed every existing row); then drops the index
// and requires the meta to end with none. The port runs the same statements
// serialized: the rebuilt index must serve the three rows, and after the
// drop the meta must carry no index — Go's state-machine probes
// (checkAddWriteOnlyForAddIndex / checkDropWriteOnly /
// checkDropDeleteOnly) are registered separately below.
#[test]
fn index_change_add_then_drop_rebuilds_and_clears_the_index() {
    let mut catalog = Catalog::default();
    let ctx = StmtContext::for_query();
    ddl::run_create_table_in(
        "create table t (c1 int primary key, c2 int)",
        &mut catalog,
        "test",
        ddl::CreateTableSettings::default(),
        &ctx,
    )
    .unwrap();
    run_insert_on("insert t values (1, 1), (2, 2), (3, 3)", &mut catalog, &ctx).unwrap();

    ddl::run_alter_table_in("alter table t add index c2(c2)", &mut catalog, "test", &ctx).unwrap();
    // Go: job.GetRowCount() == 3 at StatePublic — every row was backfilled.
    let table = kv_table(&catalog, "test", "t");
    let indexed = table
        .indexes()
        .iter()
        .find(|index| index.name == "c2")
        .expect("index c2 exists after add");
    assert_eq!(indexed.column_offsets, vec![1], "index covers c2");
    let mut table = table;
    admin_check::check_table(&mut table, None, &RowDecodeContext::for_query(&ctx))
        .expect("index entries match rows after the add");
    let rows = run_select_on("select c1 from t where c2 >= 1 order by c1", &mut catalog, &ctx).unwrap();
    assert_eq!(
        rows.iter().map(|row| datum_text(&row[0])).collect::<Vec<_>>(),
        vec!["1", "2", "3"]
    );

    ddl::run_alter_table_in("alter table t drop index c2", &mut catalog, "test", &ctx).unwrap();
    let table = kv_table(&catalog, "test", "t");
    assert!(
        table.indexes().is_empty(),
        "Go: index should have been dropped (pkg/ddl/index_change_test.go:165)"
    );
}

// The write-only halves of Go's TestIndexChange
// (pkg/ddl/index_change_test.go:52-165): against the DeleteOnly/WriteOnly/
// Public table versions captured mid-job, Go requires — insert (4,4) on the
// delete-only table writes NO index entry; insert (5,5) and update (4,4)->
// (4,1) on the write-only table DO write entries; the public backfill
// completes the missing ones; and on the way down, drop-write-only keeps
// entries readable while drop-delete-only stops writing them.
//
// go-parity-gap: schema states and the dual-version table views they need do
// not exist in this tier.
#[test]
#[ignore = "go-parity-gap: DeleteOnly/WriteOnly schema-state views of a table under ADD/DROP INDEX need the DDL job queue"]
fn index_change_schema_state_probes() {
    // Contract (pkg/ddl/index_change_test.go:172-345
    // checkAddWriteOnlyForAddIndex/checkAddPublicForAddIndex/
    // checkDropWriteOnly/checkDropDeleteOnly): per-state index-entry
    // visibility exactly as the Go helpers assert them.
}

// --- TestAddIndexRowCountUpdate (pkg/ddl/index_change_test.go:394) ---
//
// Go backfills an ADD INDEX with one reorg worker, fast reorg off and
// dist-task off, and via the afterHandleBackfillTask failpoint requires
// `admin show ddl jobs` to report a monotonically growing row count (> 0)
// for the running job.
//
// go-parity-gap: there is no backfill progress tracking, no `admin show ddl
// jobs`, and no failpoint hook in this tier.
#[test]
#[ignore = "go-parity-gap: backfill row-count progress and `admin show ddl jobs` are not transcreated"]
fn add_index_row_count_update_is_visible_mid_backfill() {
    // Contract (pkg/ddl/index_change_test.go:394-436): while the add-index
    // job runs, its row count column grows past zero.
}

// --- TestFastReOrgAlwaysEnabledOnNextGen (pkg/ddl/index_change_test.go:438)
//     and TestReadOnlyVarsInNextGen (:449) ---
//
// Both tests skip themselves unless the binary is a NEXT-GEN kernel build
// (`if kerneltype.IsClassic() { t.Skip }`), so on a classic checkout —
// which this workspace is — they execute nothing. They pin that
// `tidb_ddl_enable_fast_reorg`, `tidb_max_dist_task_nodes`,
// `tidb_ddl_reorg_max_write_speed` and `tidb_ddl_disk_quota` are read-only
// next-gen globals whose SET fails with "setting ... is not supported in the
// next generation of TiDB".
#[test]
#[ignore = "go-parity-gap: nextgen-only var guards; Go itself skips these tests on a classic kernel"]
fn fast_reorg_and_ddl_vars_are_read_only_on_nextgen() {
    // Contract (pkg/ddl/index_change_test.go:438-460, nextgen builds only):
    // SET GLOBAL on those four variables fails with the not-supported error.
}

// --- TestAddIndexFetchRowsFromCoprocessor
//     (pkg/ddl/index_cop_test.go:35) ---
//
// Go builds a single-index reorg cop context for three table shapes
// (non-clustered, pk-is-handle clustered, common-handle clustered), fetches
// the table's rows through the coprocessor between the record-prefix bounds,
// and requires each row to convert to (handle, index datums) with the
// expected handle values: _tidb_rowid 1..8 for the non-clustered shape, the
// a-values 0..7 for pk-is-handle.
//
// go-parity-gap: the cop CONTEXT is transcreated (tidb-executor::ddl_copr,
// with copr_ctx.go's own tests ported there), but the storage fetch and
// row->(handle, index datum) conversion half (FetchChunk4Test /
// ConvertRowToHandleAndIndexDatum against a live store) is not.
#[test]
#[ignore = "go-parity-gap: the coprocessor row fetch (FetchChunk4Test + ConvertRowToHandleAndIndexDatum) is not transcreated"]
fn add_index_fetch_rows_from_coprocessor_reads_expected_handles() {
    // Contract (pkg/ddl/index_cop_test.go:35-107): one (handle, index
    // datum) pair per row, handles 1..8 (non-clustered) / 0..7
    // (pk-is-handle) / 8 common-handle rows.
}
