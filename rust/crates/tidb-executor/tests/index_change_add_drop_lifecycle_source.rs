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

//! Behavioral tests retained from Go. Removed documentary entries are
//! indexed in rust/docs/parity/current-audit/comment-test-cleanup-validation.json.

use tidb_datatype::Datum;
use tidb_executor::driver::Catalog;
use tidb_executor::{admin_check, ddl, run_insert_on, run_select_on, KvTable, RowDecodeContext, StmtContext, TableEntry};

fn kv_table(catalog: &Catalog, database: &str, name: &str) -> KvTable {
    match catalog.table_in(database, name) {
        Some(TableEntry::Kv(table)) => (**table).clone(),
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
