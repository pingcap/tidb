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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

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
