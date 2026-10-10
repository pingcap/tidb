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

//! DDL checks and table metadata Go keeps, as `ddl/db.test`,
//! `ddl/db_integration.test` and `executor/ddl.test` record: TTL and
//! foreign-key validation, temporary-table allocators, column auto-convert,
//! and key-part checks.

use crate::tests_support::row_text;
use crate::Session;

fn code(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session
        .run(sql)
        .err()
        .unwrap_or_else(|| panic!("{sql} was accepted"))
        .to_mysql_error();
    (error.code, error.message)
}

fn warnings(session: &mut Session) -> Vec<Vec<String>> {
    row_text(session.run("show warnings"))
}

fn create_text(session: &mut Session, table: &str) -> String {
    row_text(session.run(&format!("show create table {table}")))[0][1].clone()
}

/// Go `handleTableOptions` and `checkTTLInfoValid`, in Go's order, and
/// `updateTTLInfoWhenModifyColumn`.
#[test]
fn ttl_options_are_checked_and_follow_a_renamed_column() {
    let mut session = Session::new();
    assert_eq!(
        code(&mut session, "create table t (id int) ttl_enable = 'ON'"),
        (
            8150,
            "Cannot set TTL_ENABLE on a table without TTL config".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "create table t (id int) ttl_job_interval = '1h'"
        )
        .0,
        8150
    );
    session
        .run("create table t (created_at datetime, updated_at datetime) ttl = `updated_at` + interval 2 year")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t change updated_at updated_at_new int"
        ),
        (
            8148,
            "Field 'updated_at_new' is of a not supported type for TTL config, expect DATETIME, DATE or TIMESTAMP".to_owned()
        )
    );
    session
        .run("alter table t rename column updated_at to updated_at_2")
        .unwrap();
    assert!(create_text(&mut session, "t").contains("TTL=`updated_at_2` + INTERVAL 2 YEAR"));
    session.run("create table c like t").unwrap();
    assert!(create_text(&mut session, "c").contains("TTL=`updated_at_2` + INTERVAL 2 YEAR"));
}

/// Go `checkTableForeignKey` and `checkTTLInfoValid`: a TTL table cannot be
/// referenced by a foreign key, nor gain a TTL while referenced.
#[test]
fn a_ttl_table_and_a_foreign_key_parent_exclude_each_other() {
    let mut session = Session::new();
    let refused = (
        8152,
        "Set TTL for a table referenced by foreign key is not allowed".to_owned(),
    );
    session
        .run("create table t (id int primary key, created_at datetime)")
        .unwrap();
    session
        .run("create table c (t_id int, foreign key fk_t_id(t_id) references t(id))")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t ttl = created_at + interval 5 year"
        ),
        refused
    );
    session.run("drop table c, t").unwrap();
    session
        .run("create table t (id int primary key, created_at datetime) ttl = created_at + interval 5 year")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "create table c (t_id int, foreign key fk_t_id(t_id) references t(id))"
        ),
        refused
    );
    session.run("create table c (t_id int)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table c add foreign key fk_t_id(t_id) references t(id)"
        ),
        refused
    );
}

/// Go `buildFKInfo`: a writing referential action may not change the base
/// column of a stored generated column.
#[test]
fn a_foreign_key_writing_a_stored_generated_base_is_refused() {
    let mut session = Session::new();
    session.run("create table t2 (a int primary key)").unwrap();
    for action in ["on update cascade", "on delete set null"] {
        assert_eq!(
            code(
                &mut session,
                &format!(
                    "create table t1 (a int, b int generated always as (a+1) stored, foreign key (a) references t2(a) {action})"
                )
            ),
            (1215, "Cannot add foreign key constraint".to_owned())
        );
    }
    session
        .run("create table t1 (a int, b int generated always as (a+1) stored)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t1 add foreign key (a) references t2(a) on delete cascade"
        )
        .0,
        1215
    );
    session
        .run("alter table t1 add foreign key (a) references t2(a) on delete restrict")
        .unwrap();
}

/// Go `checkGeneratedColumn` on ADD COLUMN, naming the column in lowercase.
#[test]
fn an_added_generated_column_reading_auto_increment_is_refused() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key auto_increment, b int)")
        .unwrap();
    assert_eq!(
        code(&mut session, "alter table t add column C int as (a + 1)"),
        (
            3109,
            "Generated column 'c' cannot refer to auto-increment column.".to_owned()
        )
    );
}

/// Go names a view's columns as its body's plan does, and records the
/// session's connection charset and collation on the view.
#[test]
fn a_view_takes_its_column_names_and_collation_from_the_session() {
    let mut session = Session::new();
    session
        .run("set names utf8mb4 collate utf8mb4_general_ci")
        .unwrap();
    session
        .run("create view v as select 'cccccccccc', 'a' as b")
        .unwrap();
    let crate::StmtOutput::Rows { columns, .. } =
        session.run_with_columns("select * from v").unwrap()
    else {
        panic!("expected rows");
    };
    let names: Vec<_> = columns.into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, vec!["cccccccccc", "b"]);
    assert_eq!(
        row_text(session.run(
            "select character_set_client, collation_connection from information_schema.views where table_name = 'v'"
        )),
        vec![vec!["utf8mb4", "utf8mb4_general_ci"]]
    );
}

/// Go `NewAllocatorFromTempTblInfo`: a temporary table allocates one id at a
/// time, a global one afresh in each transaction, and SHOW TABLE NEXT_ROW_ID
/// reads the domain's infoschema, which holds no local temporary table.
#[test]
fn temporary_tables_allocate_in_memory_one_id_at_a_time() {
    let mut session = Session::new();
    let next_row_id = |session: &mut Session, table: &str| {
        row_text(session.run(&format!("show table {table} next_row_id")))[0][3].clone()
    };
    session
        .run("create global temporary table g (id int primary key auto_increment) auto_increment = 100 on commit delete rows")
        .unwrap();
    assert_eq!(next_row_id(&mut session, "g"), "100");
    session.run("begin").unwrap();
    session.run("insert into g values (null)").unwrap();
    assert_eq!(next_row_id(&mut session, "g"), "101");
    session.run("commit").unwrap();
    assert_eq!(next_row_id(&mut session, "g"), "100");

    session
        .run("create temporary table l (id int primary key auto_increment) auto_increment = 100")
        .unwrap();
    assert_eq!(code(&mut session, "show table l next_row_id").0, 1146);
    session.run("insert into l values (null)").unwrap();
    assert_eq!(
        row_text(session.run("select @@last_insert_id")),
        vec![vec!["100"]]
    );
    assert!(create_text(&mut session, "l").contains("AUTO_INCREMENT=101"));
}

/// Go `checkTooBigFieldLengthAndTryAutoConvert` (issue #30328): outside strict
/// mode an over-long VARCHAR becomes TEXT/BLOB with a warning.
#[test]
fn an_over_long_varchar_converts_outside_strict_mode() {
    let mut session = Session::new();
    session
        .run("set @@sql_mode = 'NO_ENGINE_SUBSTITUTION'")
        .unwrap();
    session
        .run("create table t (a varbinary(70000), b varchar(70000000))")
        .unwrap();
    assert_eq!(
        warnings(&mut session),
        vec![
            vec![
                "Warning",
                "1246",
                "Converting column 'a' from VARBINARY to BLOB"
            ],
            vec![
                "Warning",
                "1246",
                "Converting column 'b' from VARCHAR to TEXT"
            ],
        ]
    );
    session.run("create table u (a varchar(200))").unwrap();
    session
        .run("alter table u modify a varchar(70000000)")
        .unwrap();
    assert_eq!(
        warnings(&mut session),
        vec![vec![
            "Warning",
            "1246",
            "Converting column 'a' from VARCHAR to TEXT"
        ]]
    );
    assert!(create_text(&mut session, "u").contains("`a` longtext"));
}

/// Go raises a truncated index comment's warning once per ALTER.
#[test]
fn a_long_index_comment_warns_once() {
    let mut session = Session::new();
    session.run("set sql_mode = ''").unwrap();
    session.run("create table t (c int, e int)").unwrap();
    session
        .run(&format!(
            "alter table t add key (e) comment '{}'",
            "a".repeat(1025)
        ))
        .unwrap();
    assert_eq!(
        warnings(&mut session),
        vec![vec![
            "Warning",
            "1688",
            "Comment for index 'e' is too long (max = 1024)"
        ]]
    );
}

/// Go `checkModifyTypes` refuses a primary key change only when it needs
/// reorganization: widening an INT handle is metadata-only.
#[test]
fn a_primary_key_column_may_widen_without_reorganization() {
    let mut session = Session::new();
    session
        .run("create table t (k int primary key, v int)")
        .unwrap();
    session
        .run("alter table t change column k k bigint")
        .unwrap();
    assert_eq!(row_text(session.run("show columns from t"))[0][1], "bigint");
    assert_eq!(
        code(&mut session, "alter table t modify k int").1,
        "Unsupported modify column: this column has primary key flag"
    );
}

/// Go keeps `FlagIgnoreZeroDateErr` on a write, so a zero date written as
/// `0` is decided by `NO_ZERO_DATE` alone, and UNIX_TIMESTAMP of a zero
/// TIMESTAMP is 0.
#[test]
fn a_zero_timestamp_is_written_and_read_as_go_does() {
    let mut session = Session::new();
    session
        .run("set session sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE'")
        .unwrap();
    session
        .run("create table t (a timestamp default 0)")
        .unwrap();
    session.run("insert into t values (0)").unwrap();
    assert_eq!(
        row_text(session.run("select a, unix_timestamp(a) from t")),
        vec![vec!["0000-00-00 00:00:00", "0"]]
    );
    session
        .run("set session sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE'")
        .unwrap();
    assert_eq!(
        code(&mut session, "update t set a = 0 where a = 0"),
        (1292, "Incorrect timestamp value: '0'".to_owned())
    );
}

/// Go `checkIndexColumn` reaches inline keys, and `checkIsDroppableColumn`
/// looks only at visible columns.
#[test]
fn inline_keys_and_hidden_columns_are_checked() {
    let mut session = Session::new();
    for sql in [
        "create table t (a text primary key)",
        "create table t (a text unique)",
    ] {
        assert_eq!(
            code(&mut session, sql),
            (
                1170,
                "BLOB/TEXT column 'a' used in key specification without a key length".to_owned()
            )
        );
    }
    session.run("create table t (a int, b int)").unwrap();
    session.run("alter table t add index ((a + 1))").unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t drop column _V$_expression_index_0"
        ),
        (
            1091,
            "Can't DROP '_V$_expression_index_0'; check that column/key exists".to_owned()
        )
    );
}
