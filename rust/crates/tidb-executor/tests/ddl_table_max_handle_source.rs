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

//! Ports of Go `pkg/ddl/primary_key_handle_test.go` (pkg/ddl batch). Four of
//! the five tests scan table storage through `ddl.GetTableMaxHandle`
//! (`pkg/ddl/reorg.go:780`), which has no Rust carrier; the fifth,
//! `TestCreateClusteredIndex`, pins CREATE TABLE's handle-kind decision and
//! IS ported over the executor's create-table carrier (`run_create_table_on`
//! / `run_create_table_in` with `CreateTableSettings::clustered_index_mode`,
//! the Rust stand-in for Go reading `@@tidb_enable_clustered_index` in
//! `pkg/ddl/metabuild.go`).

use tidb_executor::{Catalog, StmtContext, TableEntry};

/// Go `TestCreateClusteredIndex` (`pkg/ddl/primary_key_handle_test.go:262`):
/// with `@@tidb_enable_clustered_index = ON` (the stock default),
/// - a single-column INTEGER primary key becomes the row handle
///   (`TableInfo.PKIsHandle`),
/// - a VARCHAR primary key and a composite primary key become common
///   handles (`TableInfo.IsCommonHandle`),
/// - no primary key means neither,
/// - `NONCLUSTERED` on the key demotes both shapes to neither,
/// - `CREATE TABLE ... LIKE` inherits the common-handle shape, and
/// with `@@tidb_enable_clustered_index = INT_ONLY` a VARCHAR primary key is
/// NOT clustered (neither flag).
///
/// The Rust carriers of the two Go flags are the predicates
/// `KvTable::pk_handle_offset().is_some()` (PKIsHandle) and
/// `!KvTable::common_handle_offsets().is_empty()` (IsCommonHandle); the DDL
// module itself states this mapping at `src/ddl/alter_metadata.rs:226`.
#[test]
fn create_clustered_index_pins_pk_is_handle_and_common_handle_flags() {
    let mut catalog = Catalog::default();
    // Stock session: ClusteredIndexDefModeOn (CreateTableSettings::default).
    for (name, ddl, pk_is_handle, is_common_handle) in [
        ("t1", "CREATE TABLE t1 (a int primary key, b int)", true, false),
        ("t2", "CREATE TABLE t2 (a varchar(255) primary key, b int)", false, true),
        ("t3", "CREATE TABLE t3 (a int, b int, c int, primary key (a, b))", false, true),
        ("t4", "CREATE TABLE t4 (a int, b int, c int)", false, false),
        (
            "t5",
            "CREATE TABLE t5 (a varchar(255) primary key nonclustered, b int)",
            false,
            false,
        ),
        (
            "t6",
            "CREATE TABLE t6 (a int, b int, c int, primary key (a, b) nonclustered)",
            false,
            false,
        ),
    ] {
        tidb_executor::run_create_table_on(ddl, &mut catalog)
            .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        let table = stored_table(&catalog, name);
        assert_eq!(
            table.pk_handle_offset().is_some(),
            pk_is_handle,
            "{name}: PKIsHandle mismatch"
        );
        assert_eq!(
            !table.common_handle_offsets().is_empty(),
            is_common_handle,
            "{name}: IsCommonHandle mismatch"
        );
    }

    // LIKE copies the handle shape: t21 inherits t2's common handle.
    tidb_executor::run_create_table_on("CREATE TABLE t21 like t2", &mut catalog).unwrap();
    tidb_executor::run_create_table_on("CREATE TABLE t31 like t3", &mut catalog).unwrap();
    assert!(!stored_table(&catalog, "t21").common_handle_offsets().is_empty());
    assert!(!stored_table(&catalog, "t31").common_handle_offsets().is_empty());

    // INT_ONLY: a VARCHAR primary key stays non-clustered.
    let mut catalog = Catalog::default();
    let settings = tidb_executor::CreateTableSettings {
        clustered_index_mode: tidb_vardef::modes::ClusteredIndexDefMode::INT_ONLY,
        ..tidb_executor::CreateTableSettings::default()
    };
    tidb_executor::run_create_table_in(
        "CREATE TABLE t7 (a varchar(255) primary key, b int)",
        &mut catalog,
        "test",
        settings,
        &StmtContext::default().with_strict(true),
    )
    .unwrap();
    let table = stored_table(&catalog, "t7");
    assert!(table.pk_handle_offset().is_none(), "INT_ONLY: no PK handle");
    assert!(table.common_handle_offsets().is_empty(), "INT_ONLY: no common handle");
}

fn stored_table<'a>(catalog: &'a Catalog, name: &str) -> &'a tidb_executor::KvTable {
    match catalog.get_table_for_test(name) {
        Some(TableEntry::Kv(table)) => table,
        _ => panic!("{name} is not a storage-backed table"),
    }
}

/// Go `TestMultiRegionGetTableEndHandle`
/// (`pkg/ddl/primary_key_handle_test.go:59`): over a 1000-row table split
/// into ~100-key regions, `ddl.GetTableMaxHandle` (`pkg/ddl/reorg.go:780`)
/// scans every region and answers `kv.IntHandle(999)`; after
/// `insert into t values(10000, 1000)` it answers 10000; after
/// `insert into t values(-1, 1000)` it STILL answers 10000 (the max, not
/// the last write).
// go-parity-gap: GetTableMaxHandle and the mockstore region-split cluster
// are not transcreated; the Rust tier has no reorg-context storage scan.
#[test]
#[ignore]
fn multi_region_get_table_end_handle_scans_every_region_for_the_max() {
}

/// Go `TestGetTableEndHandle` (`pkg/ddl/primary_key_handle_test.go:97`):
/// GetTableMaxHandle over PK-handle shapes -- an empty table answers
/// `emptyTable=true` with a nil handle; IntHandle boundaries -1,
/// 9223372036854775806 and i64::MAX are all answered exactly; later
/// smaller inserts do not lower the max; a 1000-row t1 answers 999; and
/// for a non-handle-PK table (`t2 varchar`) the max `_tidb_rowid` queried
/// by SQL equals GetTableMaxHandle's IntHandle answer, including at
/// MaxInt64-1 and MaxInt64 row ids.
// go-parity-gap: GetTableMaxHandle carrier missing (storage scan through
// the reorg context).
#[test]
#[ignore]
fn get_table_end_handle_answers_the_max_row_id_across_pk_shapes() {
}

/// Go `TestMultiRegionGetTableEndCommonHandle`
/// (`pkg/ddl/primary_key_handle_test.go:187`): over a clustered table with
/// PRIMARY KEY (a varchar(20), b int, c float) split across regions,
/// GetTableMaxHandle answers the COMMON handle of the lexicographically
/// greatest row ('999',999,999.0), 'a' after `(a,1,1,1)` is inserted, and
/// still 'a' after the SMALLER '0000' row arrives -- common-handle ordering
/// is the encoded column order, not insertion order.
// go-parity-gap: GetTableMaxHandle carrier missing (storage scan through
// the reorg context).
#[test]
#[ignore]
fn multi_region_get_table_end_common_handle_orders_by_encoded_columns() {
}

/// Go `TestGetTableEndCommonHandle`
/// (`pkg/ddl/primary_key_handle_test.go:227`): common-handle max-row scans
/// for `(a varchar(15), b bigint)` and for the PREFIXED primary key
/// `a(2)` -- the prefixed table's max handle truncates the column to the
// index prefix length ('abccccc' -> "ab", 'azzzz' -> "az"), and an empty
// table answers empty.
// go-parity-gap: GetTableMaxHandle carrier missing (storage scan through
// the reorg context).
#[test]
#[ignore]
fn get_table_end_common_handle_truncates_prefixed_primary_keys() {
}
