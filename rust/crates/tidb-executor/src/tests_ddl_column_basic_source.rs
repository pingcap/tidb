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

#![allow(missing_docs)]

//! GO PORT of `pkg/ddl/column_test.go` items 57-60 of the pkg/ddl.part1
//! slice, read from `origin/master`: TestColumnBasic, TestAddColumn,
//! TestAddColumns and TestDropColumnInColumnTest.
//!
//! The Go tests observe add/drop column through the DDL job history
//! (`GetHistoryJobByID` via `testCheckJobDone`) and raw record iteration
//! (`tables.IterRecords`/`tables.RowWithCols` over the TiKV row bytes).
//! This tier's transcreation of Go `pkg/ddl/column.go`
//! (`AddColumn`/`DropColumn` via the `ddl_api.go` `AlterTable` action loop)
//! exposes the same schema changes through the catalog and the wired
//! engine, so each port below pins the identical row/column facts with
//! SELECTs and catalog reads; the job-history halves are the documented
//! gap.

use crate::ddl::{run_alter_table_in, run_create_table_on, run_drop_table_in};
use crate::driver::{run_insert_on, run_select_on};
use crate::{Catalog, StmtContext};

fn ctx() -> StmtContext {
    StmtContext::for_query()
}

fn rows(catalog: &Catalog, sql: &str) -> Vec<Vec<tidb_datatype::Datum>> {
    run_select_on(sql, catalog, &ctx()).unwrap()
}

fn alter(catalog: &mut Catalog, sql: &str) -> Result<(), crate::driver::DriverError> {
    run_alter_table_in(sql, catalog, crate::driver::DEFAULT_DATABASE, &ctx())
}

fn int(value: i64) -> tidb_datatype::Datum {
    tidb_datatype::Datum::Int(value)
}

/// GO PORT of `pkg/ddl/column_test.go:154 TestColumnBasic`.
///
/// Go's sequence over `t1 (c1 int, c2 int, c3 int)` with rows `(i, 10i,
/// 100*i)` for i in 0..10 (column_test.go:167-169):
/// 1. `add column c4 int default 100 after c3` — every stored row reads
///    100 (column_test.go:176-206), a new 4-value row reads its own 14
///    (column_test.go:208-224);
/// 2. `drop column c4` — rows shrink back to three values ending in 13
///    (column_test.go:227-238);
/// 3. re-add `c4` default 111, add `c5` default 101, add `c6` default 202
///    `first` — the column order becomes c6,c1,c2,c3,c4,c5 with offsets
///    0..5, the first value reads 202 and the last 101
///    (column_test.go:240-300);
/// 4. drop c2, c1, c3, c4 — two values remain, 202 first and 101 last
///    (column_test.go:302-332);
/// 5. add index `c5_idx (c5)`, drop `c5` — Go's DROP COLUMN removes the
///    column (and with it the index) without error
///    (column_test.go:334-344);
/// 6. drop `c6` — now the only column, refused
///    (testDropColumnInternal with isError=true, column_test.go:346-348).
///
/// The job-id/history plumbing Go wraps every step in is the gap; the
/// schema/row facts above are each asserted exactly.
#[test]
fn column_basic_add_drop_reposition_sequence() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t1 (c1 INT, c2 INT, c3 INT)", &mut catalog).unwrap();
    let mut values = Vec::new();
    for i in 0..10i64 {
        values.push(format!("({i}, {}, {})", 10 * i, 100 * i));
    }
    run_insert_on(
        &format!("INSERT INTO t1 VALUES {}", values.join(", ")),
        &mut catalog,
        &ctx(),
    )
    .unwrap();

    // 1. add c4 default 100 after c3.
    alter(&mut catalog, "ALTER TABLE t1 ADD COLUMN c4 INT DEFAULT 100 AFTER c3").unwrap();
    let all = rows(&catalog, "SELECT * FROM t1 ORDER BY c1");
    assert_eq!(all.len(), 10);
    for (i, row) in all.iter().enumerate() {
        assert_eq!(row.len(), 4, "10 columns after add: {row:?}");
        assert_eq!(row[0], int(i as i64));
        assert_eq!(row[1], int(10 * i as i64));
        assert_eq!(row[2], int(100 * i as i64));
        assert_eq!(row[3], int(100), "stored rows read the DEFAULT 100");
    }
    run_insert_on("INSERT INTO t1 VALUES (11, 12, 13, 14)", &mut catalog, &ctx()).unwrap();
    assert_eq!(
        rows(&catalog, "SELECT * FROM t1 WHERE c1 = 11"),
        vec![vec![int(11), int(12), int(13), int(14)]],
        "the new row reads its own c4 = 14"
    );

    // 2. drop c4.
    alter(&mut catalog, "ALTER TABLE t1 DROP COLUMN c4").unwrap();
    let all = rows(&catalog, "SELECT * FROM t1 WHERE c1 = 11");
    assert_eq!(all, vec![vec![int(11), int(12), int(13)]], "c4 gone");

    // 3. re-add c4 (111), add c5 (101), add c6 (202) FIRST.
    alter(&mut catalog, "ALTER TABLE t1 ADD COLUMN c4 INT DEFAULT 111").unwrap();
    alter(&mut catalog, "ALTER TABLE t1 ADD COLUMN c5 INT DEFAULT 101").unwrap();
    alter(&mut catalog, "ALTER TABLE t1 ADD COLUMN c6 INT DEFAULT 202 FIRST").unwrap();
    {
        let Some(crate::TableEntry::Kv(table)) = catalog.get_table_for_test("t1") else {
            panic!("t1 missing");
        };
        let names: Vec<&str> = table.columns.as_ref().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["c6", "c1", "c2", "c3", "c4", "c5"],
            "FIRST puts c6 ahead (column_test.go:280-299)"
        );
        let ids: Vec<i64> = table.columns.as_ref().iter().map(|c| c.id).collect();
        assert_eq!(ids.len(), 6);
    }
    let all = rows(&catalog, "SELECT * FROM t1 WHERE c1 = 11");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].len(), 6);
    assert_eq!(all[0][0], int(202), "values[0] reads the FIRST column's 202");
    assert_eq!(all[0][5], int(101), "values[5] reads c5's 101");

    // 4. drop c2, c1, c3, c4 (Go's order). Every stored row - including the
    // handle-11 row Go reads by handle - now reads just the two remaining
    // columns.
    for column in ["c2", "c1", "c3", "c4"] {
        alter(&mut catalog, &format!("ALTER TABLE t1 DROP COLUMN {column}")).unwrap();
    }
    let all = rows(&catalog, "SELECT * FROM t1");
    assert_eq!(all.len(), 11, "row count unchanged by column drops");
    assert!(
        all.iter().all(|row| row == &vec![int(202), int(101)]),
        "only c6, c5 remain, all rows (202, 101): {all:?}"
    );

    // 5. index on c5, then drop c5: Go's DROP COLUMN succeeds and takes the
    // index with it (column_test.go:334-344).
    alter(&mut catalog, "ALTER TABLE t1 ADD INDEX c5_idx (c5)").unwrap();
    alter(&mut catalog, "ALTER TABLE t1 DROP COLUMN c5").unwrap();
    {
        let Some(crate::TableEntry::Kv(table)) = catalog.get_table_for_test("t1") else {
            panic!("t1 missing");
        };
        assert!(
            table.indexes().iter().all(|index| index.name != "c5_idx"),
            "the dropped column's index must be gone"
        );
    }

    // 6. dropping the last remaining column is refused (1090).
    let error = alter(&mut catalog, "ALTER TABLE t1 DROP COLUMN c6")
        .expect_err("Go: dropping the only column errors");
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1090, "{mysql:?}");

    run_drop_table_in(
        "DROP TABLE t1",
        &mut catalog,
        crate::driver::DEFAULT_DATABASE,
        tidb_parser::SqlMode::default(),
        true,
    )
    .unwrap();
}

/// GO PORT of `pkg/ddl/column_test.go:651 TestAddColumn`.
///
/// Go stores one row `(1, 2, 3)` directly through `tbl.AddRecord`, runs
/// `testCreateColumn` for `c4` with default 4, and its per-state hook
/// `checkAddColumn` walks every schema state until the row reads
/// `(1, 2, 3, 4)` (column_test.go:675-696). The state walk is the gap; the
/// end state — including that the pre-DDL row sees the new default — is
/// pinned.
#[test]
fn add_column_fills_default_on_stored_rows() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t1 (c1 INT, c2 INT, c3 INT)", &mut catalog).unwrap();
    run_insert_on("INSERT INTO t1 VALUES (1, 2, 3)", &mut catalog, &ctx()).unwrap();

    alter(&mut catalog, "ALTER TABLE t1 ADD COLUMN c4 INT DEFAULT 4").unwrap();

    assert_eq!(
        rows(&catalog, "SELECT * FROM t1"),
        vec![vec![int(1), int(2), int(3), int(4)]],
        "the stored row reads the new column's default 4 (column_test.go:696)"
    );
}

/// GO PORT of `pkg/ddl/column_test.go:705 TestAddColumns`.
///
/// Go's `testCreateColumns` adds `c4`, `c5`, `c6` (no positions) in ONE
/// multi-schema-change job with default 4 over a stored row `(1, 2, 3)`,
/// the hook `checkAddColumn` verifying each new column per state until the
/// row reads six values. The state walk is the gap; the multi-add end state
/// is pinned.
#[test]
fn add_columns_fills_default_on_stored_rows() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t1 (c1 INT, c2 INT, c3 INT)", &mut catalog).unwrap();
    run_insert_on("INSERT INTO t1 VALUES (1, 2, 3)", &mut catalog, &ctx()).unwrap();

    alter(
        &mut catalog,
        "ALTER TABLE t1 ADD COLUMN c4 INT DEFAULT 4, ADD COLUMN c5 INT DEFAULT 4, \
         ADD COLUMN c6 INT DEFAULT 4",
    )
    .unwrap();

    assert_eq!(
        rows(&catalog, "SELECT * FROM t1"),
        vec![vec![int(1), int(2), int(3), int(4), int(4), int(4)]],
        "every added column defaults the stored row to 4 (column_test.go:753-766)"
    );
}

/// GO PORT of `pkg/ddl/column_test.go:774 TestDropColumnInColumnTest`.
///
/// Go stores a 4-value row `(1, 2, 3, 4)` and drops `c4`; the hook waits for
/// the column to disappear and the job history to record it
/// (column_test.go:802-815). The job half is the gap; the row now reads
/// three values.
#[test]
fn drop_column_removes_only_the_named_column() {
    let mut catalog = Catalog::default();
    run_create_table_on("CREATE TABLE t1 (c1 INT, c2 INT, c3 INT, c4 INT)", &mut catalog).unwrap();
    run_insert_on("INSERT INTO t1 VALUES (1, 2, 3, 4)", &mut catalog, &ctx()).unwrap();

    alter(&mut catalog, "ALTER TABLE t1 DROP COLUMN c4").unwrap();

    assert_eq!(
        rows(&catalog, "SELECT * FROM t1"),
        vec![vec![int(1), int(2), int(3)]],
        "c4 is gone, the stored values untouched (column_test.go:811)"
    );
}
