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

//! DDL Go refuses for its names and declarations: the preprocessor's
//! grammar checks (`planner/core/preprocess.go`) and the DDL builder's
//! identifier, duplicate and count checks (`checkTableInfoValidExtra`), as
//! `ddl/db_integration.test`, `executor/ddl.test` and `ddl/db.test` record.

use crate::tests_support::row_text;
use crate::Session;

fn error_of(session: &mut Session, sql: &str) -> String {
    session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_string()
}

#[test]
fn identifiers_longer_than_64_characters_are_refused() {
    let mut session = Session::new();
    let long = |c: char| c.to_string().repeat(65);
    let too_long = |name: &str| format!("Identifier name '{name}' is too long");
    assert_eq!(
        error_of(&mut session, &format!("create database {}", long('a'))),
        too_long(&long('a'))
    );
    assert_eq!(
        error_of(&mut session, &format!("create table {}(c int)", long('b'))),
        too_long(&long('b'))
    );
    assert_eq!(
        error_of(
            &mut session,
            &format!("create table t(c1 int, {} int)", long('c'))
        ),
        too_long(&long('c'))
    );
    assert_eq!(
        error_of(
            &mut session,
            &format!("create table t(c int, index {}(c))", long('d'))
        ),
        too_long(&long('d'))
    );
    assert_eq!(
        error_of(
            &mut session,
            &format!("create view v(`{}`) as select 1", long('e'))
        ),
        too_long(&long('e'))
    );
    session.run("create table t(c1 int)").unwrap();
    assert_eq!(
        error_of(
            &mut session,
            &format!("alter table t add column {} int", long('f'))
        ),
        too_long(&long('f'))
    );
    assert_eq!(
        error_of(
            &mut session,
            &format!("create index {} on t(c1)", long('g'))
        ),
        too_long(&long('g'))
    );
}

#[test]
fn names_go_refuses_on_their_face() {
    let mut session = Session::new();
    session.run("create table t(c1 int)").unwrap();
    for (sql, expected) in [
        (
            "alter table t add column `a ` int",
            "Incorrect column name 'a '",
        ),
        (
            "alter table t add column `_tidb_rowid` int",
            "Incorrect column name '_tidb_rowid'",
        ),
        ("create table t2(xxx.t2.a bigint)", "Incorrect database name 'xxx'"),
        ("create table t2(c1.c2 blob default null)", "Incorrect table name 'c1'"),
        (
            "alter table t modify testx.t.c1 bigint",
            "Incorrect database name 'testx'",
        ),
        ("alter table t modify t2.c1 bigint", "Incorrect table name 't2'"),
        (
            "create table t3(`id` int, key `primary`(`id`))",
            "Incorrect index name 'primary'",
        ),
        (
            "create table t4(a int) partition by range (a) (partition p0 values less than (0), partition `p1 ` values less than (3))",
            "Incorrect partition name",
        ),
        (
            "create table t5(a varbinary(70000))",
            "Column length too big for column 'a' (max = 65535); use BLOB or TEXT instead",
        ),
        (
            "create view v1 as select 1 as t, 1 as t",
            "Duplicate column name 't'",
        ),
        (
            "alter table t add column c int auto_increment",
            "unsupported add column 'c' constraint AUTO_INCREMENT when altering 'test.t'",
        ),
    ] {
        assert_eq!(error_of(&mut session, sql), expected, "{sql}");
    }
    // A column COMMENT is an ordinary ADD COLUMN option.
    session
        .run("alter table t add column f int comment 'test'")
        .unwrap();
}

#[test]
fn a_table_has_at_most_64_indexes() {
    let mut session = Session::new();
    let columns = (0..65)
        .map(|i| format!("c{i} int"))
        .collect::<Vec<_>>()
        .join(", ");
    let indexes = (0..65)
        .map(|i| format!("key k{i}(c{i})"))
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(
        error_of(
            &mut session,
            &format!("create table t({columns}, {indexes})")
        ),
        "Too many keys specified; max 64 keys allowed"
    );
    let indexes = (0..64)
        .map(|i| format!("key k{i}(c{i})"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .run(&format!("create table t({columns}, {indexes})"))
        .unwrap();
    assert_eq!(
        error_of(&mut session, "create index k64 on t (c64)"),
        "Too many keys specified; max 64 keys allowed"
    );
}

/// Go `adjustOverlongViewColname`: an unnamed view column longer than 64
/// characters is named `name_exp_<offset>`.
#[test]
fn an_overlong_view_column_is_renamed() {
    let mut session = Session::new();
    let long = "b".repeat(65);
    session
        .run(&format!("create view v as select 1 as a, 2 as {long}"))
        .unwrap();
    assert_eq!(
        row_text(session.run("select * from v")),
        vec![vec!["1", "2"]]
    );
    let shown = row_text(session.run("show create view v"));
    assert!(shown[0][1].contains("(`a`, `name_exp_2`)"), "{shown:?}");
}

/// Go `validateCommentLength`: a column or index comment holds 1024 bytes;
/// strict mode refuses a longer one, otherwise it warns and is cut.
#[test]
fn a_column_or_index_comment_holds_1024_bytes() {
    let mut session = Session::new();
    let long = "b".repeat(1025);
    session.run("create table t(c1 int)").unwrap();
    assert_eq!(
        error_of(
            &mut session,
            &format!("alter table t add column c2 int comment '{long}'")
        ),
        "Comment for field 'c2' is too long (max = 1024)"
    );
    assert_eq!(
        error_of(
            &mut session,
            &format!("create index i on t(c1) comment '{long}'")
        ),
        "Comment for index 'i' is too long (max = 1024)"
    );
    assert_eq!(
        error_of(&mut session, "alter table t change c1 _tidb_rowid bigint"),
        "Incorrect column name '_tidb_rowid'"
    );
    session.run("set sql_mode = ''").unwrap();
    session
        .run(&format!("create table t2(c int comment '{long}')"))
        .unwrap();
    assert_eq!(
        crate::tests_support::warnings_of(&session),
        vec![(
            1629,
            "Comment for field 'c' is too long (max = 1024)".to_owned()
        )]
    );
}

/// Go `getColDefaultExprValue`: an expression default is cast into the
/// column with `CastColumnValue`, so a value the column cannot hold fails a
/// strict INSERT (an ENUM's bare `ErrTruncated`) and is stored with a warning
/// otherwise; and `checkDefaultValue` refuses one on AUTO_INCREMENT.
#[test]
fn an_expression_default_is_cast_into_the_column() {
    let mut session = Session::new();
    session
        .run("create table t2 (c int, c1 enum('y','n') default (date_format(now(),'%Y-%m-%d')))")
        .unwrap();
    assert_eq!(
        error_of(&mut session, "insert into t2 values ()"),
        "Data truncated for column '%s' at row %d"
    );
    assert_eq!(
        error_of(
            &mut session,
            "create table t0 (c int, c1 int auto_increment default (str_to_date('1980-01-01','%Y-%m-%d')))"
        ),
        "Invalid default value for 'c1'"
    );
}
