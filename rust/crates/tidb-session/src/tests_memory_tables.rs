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

//! Memory tables are read where the plan reads them, not where the statement
//! text names them (Go `buildMemTable` + `MemTableReaderExec`). Every expected
//! answer below was captured from Go TiDB.

use crate::tests_support::*;
use crate::*;

fn error_of(session: &mut Session, sql: &str) -> String {
    session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_string()
}

/// A write whose read half scans a memory table: Go retrieves the rows when
/// the scan is read, whatever statement the scan sits under. The statement
/// text decided which tables were read before, and only a SELECT was walked,
/// so every one of these failed with a missing memory table.
#[test]
fn writes_read_memory_tables() {
    let mut session = Session::new();
    session.run("create database memtbl").unwrap();
    session.run("use memtbl").unwrap();
    session
        .run("create table t (a bigint primary key, n varchar(64))")
        .unwrap();

    // INSERT ... SELECT.
    session
        .run(
            "insert into t select 1, schema_name from information_schema.schemata \
             where schema_name = 'memtbl'",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["1", "memtbl"]]);

    // An UPDATE assigning a scalar subquery, which runs while it is planned.
    session
        .run(
            "update t set a = (select count(*) from information_schema.schemata \
             where schema_name = 'memtbl') + 10",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["11", "memtbl"]]);

    // A DELETE filtered by an IN subquery.
    session
        .run(
            "delete from t where n in (select schema_name from information_schema.schemata \
             where schema_name = 'memtbl')",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select count(*) from t")), [["0"]]);

    // REPLACE ... SELECT, then a multi-table UPDATE joining the memory table.
    session
        .run(
            "replace into t select 5, schema_name from information_schema.schemata \
             where schema_name = 'memtbl'",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["5", "memtbl"]]);
    session
        .run(
            "update t join information_schema.schemata s on s.schema_name = t.n \
             set t.a = 6",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["6", "memtbl"]]);

    // ON DUPLICATE KEY UPDATE assigning a subquery over a memory table.
    session
        .run(
            "insert into t values (6, 'x') on duplicate key update n = \
             (select max(schema_name) from information_schema.schemata \
             where schema_name = 'memtbl')",
        )
        .unwrap();
    assert_eq!(row_text(session.run("select * from t")), [["6", "memtbl"]]);

    // A locking read whose subquery scans one.
    assert_eq!(
        row_text(session.run(
            "select a from t where exists (select 1 from information_schema.schemata \
             where schema_name = 'memtbl') for update"
        )),
        [["6"]]
    );
}

/// The plan, not the statement text, resolves a memory table, so a missing one
/// is reported as the planner reports every missing table: the tables of a
/// FROM clause resolve left to right, and the schema is named in lower case
/// (captured: `INFORMATION_SCHEMA.nonexistent` answers
/// `information_schema.nonexistent`, a bare name under `USE PERFORMANCE_SCHEMA`
/// answers `performance_schema.NonExistent`).
#[test]
fn a_missing_memory_table_is_reported_as_the_planner_resolves_it() {
    let mut session = Session::new();
    for (sql, expected) in [
        (
            "select * from INFORMATION_SCHEMA.nonexistent",
            "Table 'information_schema.nonexistent' doesn't exist",
        ),
        (
            "select * from Information_Schema.NonExistent",
            "Table 'information_schema.NonExistent' doesn't exist",
        ),
        (
            "select * from PERFORMANCE_SCHEMA.nonexistent",
            "Table 'performance_schema.nonexistent' doesn't exist",
        ),
        (
            "select * from metrics_SCHEMA.nonexistent",
            "Table 'metrics_schema.nonexistent' doesn't exist",
        ),
        (
            "select * from MySQL.nonexistent",
            "Table 'mysql.nonexistent' doesn't exist",
        ),
        (
            "select * from information_schema.nonexistent join test.x",
            "Table 'information_schema.nonexistent' doesn't exist",
        ),
        (
            "select * from test.x join information_schema.nonexistent",
            "Table 'test.x' doesn't exist",
        ),
    ] {
        assert_eq!(error_of(&mut session, sql), expected, "{sql}");
    }
    session.run("use information_schema").unwrap();
    assert_eq!(
        error_of(&mut session, "select * from NonExistent"),
        "Table 'information_schema.NonExistent' doesn't exist"
    );
    session.run("use PERFORMANCE_SCHEMA").unwrap();
    assert_eq!(
        error_of(&mut session, "select * from NonExistent"),
        "Table 'performance_schema.NonExistent' doesn't exist"
    );
}

/// A multi-table UPDATE or DELETE joining a memory table: the plan reads the
/// memory table's own columns, with no hidden handle (it stores no rows), so
/// the write splits each read row by those widths. `7 columns, expected 9`
/// was the width of a stored table's part applied to the virtual one. The
/// answers are Go's, for a heap table and a clustered one as the target.
#[test]
fn multi_table_writes_join_memory_tables() {
    let mut session = Session::new();
    session.run("create database memtbl").unwrap();
    session.run("use memtbl").unwrap();
    session
        .run("create table t5 (a int, n varchar(64))")
        .unwrap();
    session
        .run("create table t6 (a bigint primary key, n varchar(64))")
        .unwrap();
    session
        .run("insert into t5 values (1, 'memtbl'), (2, 'nomatch'), (3, 'memtbl')")
        .unwrap();
    session
        .run("insert into t6 values (1, 'memtbl'), (2, 'nomatch')")
        .unwrap();
    let rows = |session: &mut Session, table: &str| {
        row_text(session.run(&format!("select * from {table} order by a")))
    };
    session
        .run("delete t5 from t5 join information_schema.schemata s on s.schema_name = t5.n")
        .unwrap();
    assert_eq!(rows(&mut session, "t5"), [["2", "nomatch"]]);
    session
        .run("insert into t5 values (1, 'memtbl'), (4, 'memtbl')")
        .unwrap();
    session
        .run(
            "delete t5 from information_schema.schemata s join t5 on s.schema_name = t5.n \
             where t5.a = 4",
        )
        .unwrap();
    assert_eq!(rows(&mut session, "t5"), [["1", "memtbl"], ["2", "nomatch"]]);
    session
        .run(
            "update t5 join information_schema.schemata s on s.schema_name = t5.n \
             set t5.a = t5.a + 100",
        )
        .unwrap();
    assert_eq!(rows(&mut session, "t5"), [["2", "nomatch"], ["101", "memtbl"]]);
    session
        .run(
            "delete t6, t5 from t6 join t5 on t5.a = 101 + 0 * t6.a \
             join information_schema.schemata s on s.schema_name = t6.n",
        )
        .unwrap();
    assert_eq!(rows(&mut session, "t5"), [["2", "nomatch"]]);
    assert_eq!(rows(&mut session, "t6"), [["2", "nomatch"]]);
    session
        .run("delete from t6 using t6 join information_schema.schemata s on s.schema_name = t6.n")
        .unwrap();
    assert_eq!(rows(&mut session, "t6"), [["2", "nomatch"]]);
}
