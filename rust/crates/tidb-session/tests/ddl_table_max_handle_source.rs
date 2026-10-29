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

use crate::support::testkit::TestKit;
use tidb_infoschema::TableEntry;
use tidb_table::kv_table::KvTable;

/// Go `TestCreateClusteredIndex` (`pkg/ddl/primary_key_handle_test.go:262`):
/// with `@@tidb_enable_clustered_index = ON`,
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
/// Go reads the flags off `dom.InfoSchema().TableByName(...).Meta()`; the
/// Rust carriers of the two flags are the predicates
/// `KvTable::pk_handle_offset().is_some()` (PKIsHandle) and
/// `!KvTable::common_handle_offsets().is_empty()` (IsCommonHandle).
#[test]
fn create_clustered_index_pins_pk_is_handle_and_common_handle_flags() {
    let mut tk = TestKit::new();
    // Go: tk.Session().GetSessionVars().EnableClusteredIndex = ClusteredIndexDefModeOn.
    tk.must_exec("set @@tidb_enable_clustered_index = ON");
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
        tk.must_exec(ddl);
        let table = stored_table(&tk, name);
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
    tk.must_exec("CREATE TABLE t21 like t2");
    tk.must_exec("CREATE TABLE t31 like t3");
    assert!(!stored_table(&tk, "t21").common_handle_offsets().is_empty());
    assert!(!stored_table(&tk, "t31").common_handle_offsets().is_empty());

    // Go: EnableClusteredIndex = ClusteredIndexDefModeIntOnly -- a VARCHAR
    // primary key stays non-clustered.
    tk.must_exec("set @@tidb_enable_clustered_index = INT_ONLY");
    tk.must_exec("CREATE TABLE t7 (a varchar(255) primary key, b int)");
    let table = stored_table(&tk, "t7");
    assert!(table.pk_handle_offset().is_none(), "INT_ONLY: no PK handle");
    assert!(table.common_handle_offsets().is_empty(), "INT_ONLY: no common handle");
}

/// Go `dom.InfoSchema().TableByName(ctx, "test", name).Meta()`.
fn stored_table(tk: &TestKit, name: &str) -> KvTable {
    match tk.get_table_by_name("test", name) {
        TableEntry::Kv(table) => (*table).clone(),
        _ => panic!("{name} is not a storage-backed table"),
    }
}
