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

//! `_tidb_rowid`, Go's extra handle column.

use crate::tests_support::row_text;
use crate::Session;

fn fixture() -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int, b int, KEY ia(a))").unwrap();
    session
        .run("INSERT INTO t VALUES (10, 1), (20, 2), (30, 3)")
        .unwrap();
    session
}

/// Go appends the extra handle column to a heap table's `DataSource` schema
/// (`buildDataSource`'s `NewExtraHandleSchemaCol`), where it reports the
/// record HANDLE rather than any stored column, and `unfoldWildStar` skips it
/// so `*` never carries it.
#[test]
fn the_extra_handle_column_reports_the_record_handle() {
    let mut session = fixture();

    assert_eq!(
        row_text(session.run("SELECT _tidb_rowid FROM t")),
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, _tidb_rowid FROM t")),
        vec![vec!["10", "1"], vec!["20", "2"], vec!["30", "3"]]
    );
    // Qualified, and through an alias.
    assert_eq!(
        row_text(session.run("SELECT t._tidb_rowid FROM t")),
        vec![vec!["1"], vec!["2"], vec!["3"]]
    );

    // `*` expands to the stored columns alone, even beside the name.
    assert_eq!(
        row_text(session.run("SELECT * FROM t")),
        vec![vec!["10", "1"], vec!["20", "2"], vec!["30", "3"]]
    );
    assert_eq!(
        row_text(session.run("SELECT *, _tidb_rowid FROM t")),
        vec![
            vec!["10", "1", "1"],
            vec!["20", "2", "2"],
            vec!["30", "3", "3"]
        ]
    );

    // It is an ordinary column everywhere else: an ORDER BY key, a predicate.
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY _tidb_rowid DESC")),
        vec![vec!["30"], vec!["20"], vec!["10"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE _tidb_rowid = 2")),
        vec![vec!["20"]]
    );

    // And through an index reader, whose entries carry the same handle.
    assert_eq!(
        row_text(session.run(
            "SELECT a, _tidb_rowid FROM t USE INDEX (ia) WHERE a > 10 ORDER BY a, _tidb_rowid"
        )),
        vec![vec!["20", "2"], vec!["30", "3"]]
    );
}

/// A table whose handle IS a declared column has no extra one: Go's
/// `buildDataSource` appends it only when `handleCols == nil` and the table is
/// not `IsCommonHandle`, so TiDB answers "Unknown column" for the name there.
#[test]
fn a_clustered_table_has_no_extra_handle_column() {
    let mut session = Session::new();
    for ddl in [
        "CREATE TABLE pk (a int PRIMARY KEY, b int)",
        "CREATE TABLE ch (a varchar(8), b int, PRIMARY KEY (a) CLUSTERED)",
    ] {
        session.run(ddl).unwrap();
    }
    for table in ["pk", "ch"] {
        let error = session
            .run(&format!("SELECT _tidb_rowid FROM {table}"))
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(error.code, 1054, "{table}");
    }
}
