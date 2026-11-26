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

//! What a stored NULL becomes when its column is rewritten to `NOT NULL`.

use crate::tests_support::row_text;
use crate::Session;

/// Go `updateColumnWorker.getRowRecord` substitutes the CURRENT TIMESTAMP for
/// a NULL before it casts, guarded on exactly three things: the old value is
/// NULL, the new type is `mysql.TypeTimestamp`, and the new column carries
/// `NotNullFlag` -- "convert null value to timestamp should be substituted
/// with current timestamp if NOT_NULL flag is set".
///
/// Without it the rewrite failed outright, so the column stayed `int` and the
/// row stayed NULL. Source rows: `tests/integrationtest/t/ddl/db_change.test`,
/// where the recorded answer is that the stored instant is within two seconds
/// of the reading statement's own clock.
#[test]
fn a_null_becomes_the_current_timestamp_when_the_column_turns_not_null() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int, b int)").unwrap();
    session.run("INSERT INTO t VALUES (NULL, NULL)").unwrap();
    session
        .run("ALTER TABLE t MODIFY COLUMN a timestamp not null")
        .unwrap();

    assert_eq!(
        row_text(session.run("SELECT floor((unix_timestamp() - unix_timestamp(a)) / 2) FROM t")),
        vec![vec!["0"]],
        "the stored instant is the statement clock, not NULL and not the zero time"
    );

    // Go names `mysql.TypeTimestamp` and nothing else, so the sibling
    // temporal type still refuses the NULL rather than inventing a value.
    let error = session
        .run("ALTER TABLE t MODIFY COLUMN b datetime not null")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1265);
    // And the refusal left the column alone.
    assert_eq!(row_text(session.run("SELECT b FROM t")), vec![vec!["NULL"]]);
}

#[test]
fn modify_not_null_preserves_go_precheck_error_and_timestamp_boundary() {
    let mut session = Session::new();
    for (name, old_type, new_type) in [
        ("same_int", "INT", "INT"),
        ("same_timestamp", "TIMESTAMP NULL", "TIMESTAMP"),
    ] {
        session
            .run(&format!("CREATE TABLE {name} (id INT, B {old_type})"))
            .unwrap();
        session
            .run(&format!("INSERT INTO {name} VALUES (1, NULL), (2, NULL)"))
            .unwrap();
        let error = session
            .run(&format!("ALTER TABLE {name} MODIFY B {new_type} NOT NULL"))
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(error.code, 1265);
        assert_eq!(error.message, "Data truncated for column 'b' at row 1");
        assert_eq!(
            row_text(session.run(&format!("SELECT B FROM {name} ORDER BY id"))),
            [["NULL"], ["NULL"]]
        );
    }
    session
        .run("CREATE TABLE renamed (id INT PRIMARY KEY, OldName INT)")
        .unwrap();
    session
        .run("INSERT INTO renamed VALUES (1, 10), (2, NULL)")
        .unwrap();
    let error = session
        .run("ALTER TABLE renamed CHANGE OldName NewName INT NOT NULL")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1265);
    assert_eq!(
        error.message,
        "Data truncated for column 'newname' at row 1"
    );
    assert_eq!(
        row_text(session.run("SELECT OldName FROM renamed ORDER BY id")),
        [["10"], ["NULL"]]
    );
}

/// Go `TestModifyColumnRollbackKeepsOriginalNotNull` (Go master
/// 3eee6459185): a failed MODIFY COLUMN keeps the column's original NOT NULL;
/// the rollback only clears the in-progress markers. (Go's NullToNotNull
/// case needs the `beforeDoModifyColumnSkipReorgCheck` failpoint, which has no
/// Rust site.)
#[test]
fn modify_column_rollback_keeps_original_not_null() {
    let mut tk = Session::new();
    tk.run("set session sql_mode = 'STRICT_TRANS_TABLES'").unwrap();
    let assert_not_null_preserved = |tk: &mut Session, column: &str, data_type: &str| {
        assert_eq!(
            row_text(tk.run(&format!(
                "select data_type, is_nullable from information_schema.columns \
                 where table_schema = 'test' and table_name = 't' and column_name = '{column}'"
            ))),
            [[data_type, "NO"]]
        );
        let error = tk.run(&format!("insert into t (id, {column}) values (100, null)")).unwrap_err();
        assert_eq!(error.to_string(), format!("Column '{column}' cannot be null"));
    };

    // NoReorgWithCheck
    tk.run("drop table if exists t").unwrap();
    tk.run("create table t (id int primary key, c int not null)").unwrap();
    tk.run("insert into t values (1, 1000), (2, 2000)").unwrap();
    let error = tk.run("alter table t modify column c tinyint not null").unwrap_err();
    assert!(error.to_string().contains("Data truncated for column 'c'"), "{error}");
    assert_not_null_preserved(&mut tk, "c", "int");

    // IndexReorg
    tk.run("drop table if exists t").unwrap();
    tk.run("create table t (id int primary key, c char(20) collate utf8mb4_bin not null, index idx(c))")
        .unwrap();
    tk.run("insert into t values (1, 'abcdefghijklmnop')").unwrap();
    let error = tk.run("alter table t modify column c varchar(10) collate utf8mb4_bin not null").unwrap_err();
    assert!(error.to_string().contains("Data truncated for column 'c'"), "{error}");
    assert_not_null_preserved(&mut tk, "c", "char");
}
