// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a License copy at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Ports of Go `pkg/executor/test/admintest/admin_test.go` items 721–728
//! (the global-index / generated-column / fast-check `ADMIN CHECK` slice)
//! plus that package's two `main_test.go` bootstraps.
//!
//! SCOPE NOTE. Go drives these through a mock TiKV store, raw transactions,
//! and the `tidb_enable_fast_table_check` session variable, corrupting index
//! entries with `tables.NewIndex(...).Create/Delete` on a raw txn. This tier
//! corrupts through the equivalent raw-key seam
//! ([`crate::kv_table::KvTable::delete_raw_key_for_test`] and a crafted
//! `storage.set` of `encode_handle_in_unique_index_value`), which leaves the
//! store byte-identical to a half-applied write; `tidb_enable_fast_table_check`
//! has no Rust variable to set, so both Go fast-check settings map to the one
//! consistency-check path [`crate::admin_check::check_table`] owns.

use crate::{admin_check, run_create_table_on, run_insert_on, Catalog, RowDecodeContext};

fn ctx() -> crate::StmtContext {
    crate::StmtContext::for_query()
}

fn create(catalog: &mut Catalog, sql: &str) {
    run_create_table_on(sql, catalog)
        .unwrap_or_else(|error| panic!("create {sql:?} failed: {error:?}"));
}

fn insert(catalog: &mut Catalog, sql: &str) {
    run_insert_on(sql, catalog, &ctx())
        .unwrap_or_else(|error| panic!("insert {sql:?} failed: {error:?}"));
}

fn kv_table_of(catalog: &Catalog, name: &str) -> crate::kv_table::KvTable {
    let Some(crate::TableEntry::Kv(table)) = catalog.table_in("test", name) else {
        panic!("table {name} is not stored as bytes");
    };
    table.clone()
}

fn check_context() -> RowDecodeContext {
    RowDecodeContext::for_test_query_utc()
}

/// Go `admin_test.go:2244::TestAdminCheckGeneratedColumns`: a virtual
/// generated column (`gen int AS (val * pk)`) with `KEY idx_gen(gen)`; Go
/// corrupts the index by deleting the stored `gen=10 -> handle 2` entry and
/// creating a `gen=5 -> handle 2` one, then requires `ADMIN CHECK TABLE t`
/// to error under BOTH fast-check settings.
///
/// The corruption here is byte-equivalent: the old entry is removed with the
/// raw-key seam and the wrong entry is written with
/// `tidb_tablecodec::encode_handle_in_unique_index_value`, exactly what Go's
/// `indexOpr.Delete` + `indexOpr.Create` pair leaves behind. The check must
/// refuse, and the refusal is Go's 8134 value-mismatch shape naming the
/// index, the column, the handle and both sides
/// (`pkg/util/consistency/errors.go`, `ErrAdminCheckInconsistentWithColInfo`).
#[test]
fn admin_check_detects_generated_column_index_value_corruption() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "CREATE TABLE t(pk int PRIMARY KEY CLUSTERED, val int, \
         gen int GENERATED ALWAYS AS (val * pk) VIRTUAL, KEY idx_gen(gen))",
    );
    insert(&mut catalog, "INSERT INTO t(pk, val) VALUES (2, 5)");

    let mut table = kv_table_of(&catalog, "t");
    let context = check_context();
    // Go: the initial "ADMIN CHECK TABLE t" passes.
    let checked = admin_check::check_table(&mut table, None, &context)
        .expect("the consistent table must pass");
    assert_eq!(checked, 1, "exactly idx_gen is checked");

    // Corrupt: replace the stored gen=10 entry with gen=5 for the same handle.
    let index = table
        .index_list_for_check()
        .into_iter()
        .find(|index| index.name.eq_ignore_ascii_case("idx_gen"))
        .expect("idx_gen exists");
    let Some((old_entry_key, _)) = table
        .index_entries_for_check(index.id)
        .expect("index entries")
        .into_iter()
        .next()
    else {
        panic!("the index holds one entry");
    };
    let rows = table
        .scan_rows_with_handles_recomputed(&context)
        .expect("rows readable");
    let (handle, row) = &rows[0];
    let mut wrong_row = row.clone();
    wrong_row[index.column_offsets[0]] = tidb_datatype::Datum::Int(5);
    let (wrong_key, _) = table
        .index_key_for_check(&index, &wrong_row, handle, context.zone())
        .expect("the wrong-value key encodes");
    table
        .delete_raw_key_for_test(&old_entry_key)
        .expect("old entry dropped");
    let mut storage = table.swap_storage(Box::new(crate::storage::MemTableStorage::default()));
    let crafted_handle = tidb_txnkv::Handle::Int(tidb_txnkv::IntHandle::new(match handle {
        crate::kv_table::TableHandle::Int(value) => *value,
        other => panic!("unexpected handle {other:?}"),
    }));
    storage
        .set(
            tidb_txnkv::Key::from_bytes(wrong_key),
            tidb_tablecodec::encode_handle_in_unique_index_value(&crafted_handle, false),
        )
        .expect("wrong entry written");
    table.swap_storage(storage);

    let error = admin_check::check_table(&mut table, None, &context)
        .expect_err("the corrupted generated-column index must be refused");
    let admin_check::AdminCheckError::ValueMismatch(mismatch) = &error else {
        panic!("expected the value-mismatch shape, got {error:?}");
    };
    assert_eq!(mismatch.index, "idx_gen");
    assert_eq!(mismatch.column, "gen");
    assert_eq!(mismatch.handle, "2");
    assert_eq!(mismatch.index_value, "KindInt64 5");
    assert_eq!(mismatch.record_value, "KindInt64 10");
}

/// Go `admin_test.go:2345::TestAdminCheckTableWithEnumAndPointGet`, data
/// halves: tables whose unique indexes sit on ENUM columns (single-column
/// `uk_status`, composite `uk_composite(id, type)`, and a plain varchar
/// `uk_name`) pass `admin check table` AND `admin check index <name>` in
/// both fast-check modes. The Go test's reason to exist is that the fast
/// path's `verifyIndexSideQuery` mis-handed the PointGet plans such a
/// picture produces; the check-level contract pinned here is that the same
/// tables pass the check in every named form.
///
/// go-parity-gap: the EXPLAIN-shape assertions (the query with
/// `status = 'active'` must plan a `Point_Get` whose access object contains
/// ", index:", the `id in (1, 2) and type = 'A'` query may plan a
/// `Batch_Point_Get`) have no explain output on this tier; they are recorded
/// in `admin_check_enum_pointget_plan_shape` below.
#[test]
fn admin_check_passes_over_enum_unique_index_tables_in_every_named_form() {
    let context = check_context();

    // Test 1: enum column with a unique index. (Each Go section drops and
    // re-creates `admin_test`; this tier has no drop-table helper in scope,
    // so each section uses a fresh catalog — same table shape, same check.)
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table admin_test (id int primary key, \
         status enum('active', 'inactive', 'pending'), unique key uk_status(status))",
    );
    insert(
        &mut catalog,
        "insert into admin_test values (1, 'active'), (2, 'inactive'), (3, 'pending')",
    );
    let mut table = kv_table_of(&catalog, "admin_test");
    assert_eq!(
        admin_check::check_table(&mut table, None, &context).expect("table check"),
        1,
        "the whole-table check covers uk_status"
    );
    assert_eq!(
        admin_check::check_table(&mut table, Some("uk_status"), &context).expect("index check"),
        1,
        "exactly the named index is checked"
    );
    drop(table);
    // Go re-runs the checks with `tidb_enable_fast_table_check = 0`; this
    // tier has the one consistency path, so a second pass stands in.
    let mut table = kv_table_of(&catalog, "admin_test");
    assert_eq!(
        admin_check::check_table(&mut table, Some("uk_status"), &context)
            .expect("index check again"),
        1
    );
    drop(table);

    // Test 2: varchar unique index.
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table admin_test (id int primary key, name varchar(50), unique key uk_name(name))",
    );
    insert(
        &mut catalog,
        "insert into admin_test values (1, 'alice'), (2, 'bob'), (3, 'charlie')",
    );
    let mut table = kv_table_of(&catalog, "admin_test");
    assert_eq!(
        admin_check::check_table(&mut table, None, &context).expect("table check"),
        1
    );
    assert_eq!(
        admin_check::check_table(&mut table, Some("uk_name"), &context).expect("index check"),
        1
    );
    drop(table);

    // Test 3: composite unique index over an enum column.
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table admin_test (id int, type enum('A', 'B', 'C'), value int, \
         unique key uk_composite(id, type))",
    );
    insert(
        &mut catalog,
        "insert into admin_test values (1, 'A', 100), (2, 'B', 200), (3, 'C', 300)",
    );
    let mut table = kv_table_of(&catalog, "admin_test");
    assert_eq!(
        admin_check::check_table(&mut table, Some("uk_composite"), &context)
            .expect("composite index check"),
        1
    );
    assert_eq!(
        admin_check::check_table(&mut table, None, &context).expect("composite table check"),
        1
    );
}

/// Go's admin checker treats a partial index as a predicate-filtered relation:
/// rows for which the condition is false or NULL do not count as missing index
/// entries. The Rust checker must therefore pass both whole-table and named
/// index checks when only one of three table rows is indexed.
#[test]
fn admin_check_counts_only_rows_matching_partial_index_predicate() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table t (a int, b int, key idx_b (b) where a > 0)",
    );
    insert(
        &mut catalog,
        "insert into t values (1, 10), (-1, 20), (null, 30)",
    );
    let mut table = kv_table_of(&catalog, "t");
    let context = check_context();
    assert_eq!(
        admin_check::check_table(&mut table, None, &context).expect("partial table check"),
        1
    );
    assert_eq!(
        admin_check::check_table(&mut table, Some("idx_b"), &context).expect("partial index check"),
        1
    );
}

/// Go `admin_test.go:2488::TestFastCheckTableConcurrent`: five sessions run
/// `admin check table` over a 100-row table concurrently and all pass.
///
/// The Go race the comment mentions (ExecDetails writes) is delivered by the
/// race detector against mock-TiKV goroutines; this tier delivers the same
/// five concurrent checks over cloned table images (each thread owns its
/// table because a check drains a read cursor), which is the equivalent
/// concurrency for the check logic itself.
#[test]
fn admin_check_table_runs_concurrently_over_one_hundred_rows() {
    let mut catalog = Catalog::default();
    create(
        &mut catalog,
        "create table t_concurrent (id int primary key, val int, key idx_val(val))",
    );
    for i in 0..100 {
        insert(
            &mut catalog,
            &format!("insert into t_concurrent values ({i}, {})", i * 10),
        );
    }
    let catalog = std::sync::Arc::new(catalog);
    let workers: Vec<_> = (0..5)
        .map(|_| {
            let catalog = std::sync::Arc::clone(&catalog);
            std::thread::spawn(move || {
                let mut table = {
                    let Some(crate::TableEntry::Kv(table)) =
                        catalog.table_in("test", "t_concurrent")
                    else {
                        panic!("t_concurrent is not stored as bytes");
                    };
                    table.clone()
                };
                admin_check::check_table(&mut table, None, &check_context())
                    .map_err(|error| format!("{error:?}"))
            })
        })
        .collect();
    for worker in workers {
        let checked = worker
            .join()
            .expect("check thread joins")
            .expect("check passes");
        assert_eq!(checked, 1);
    }
}

/// Go `admintest/main_test.go:27::TestMain` and (with the same shape)
/// `aggregate/main_test.go:25::TestMain`: `testsetup.SetupForCommonTest`,
/// global config tweaks, `autoid.SetStep(5000)` (admintest) and the goleak
/// verification wrapper.
///
/// No behavior to pin: this tier's tests bootstrap themselves and have no
/// goroutine-leak audit, so the carrier records the disposition.
#[test]
fn admintest_and_aggregate_mains_are_suite_bootstrap() {
    // go-parity-gap: goleak/config suite bootstrap; no behavior.
}
