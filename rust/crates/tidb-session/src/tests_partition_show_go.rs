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

//! HASH/KEY partition management, partition metadata, collation changes on
//! indexed columns and SHOW output, as `ddl/db_partition.test`,
//! `ddl/serial.test` and `executor/show.test` record.

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

fn create_text(session: &mut Session, table: &str) -> String {
    row_text(session.run(&format!("show create table {table}")))[0][1].clone()
}

/// Go `buildHashPartitionDefinitions`: a HASH reorganization keeps every
/// existing partition's name and options, takes the written definitions
/// next and names the rest `p{i}`; the names must stay unique (1517).
#[test]
fn hash_partition_management_keeps_names_and_options() {
    let mut session = Session::new();
    session
        .run("create table t (a int) partition by hash (a) partitions 2")
        .unwrap();
    session
        .run("insert into t values (1), (2), (3), (4)")
        .unwrap();
    session
        .run("alter table t add partition (partition pp2 comment 'c2')")
        .unwrap();
    assert!(create_text(&mut session, "t").contains("PARTITION `pp2` COMMENT 'c2'"));
    session
        .run("alter table t add partition partitions 1")
        .unwrap();
    assert_eq!(
        row_text(session.run("show warnings")),
        vec![vec![
            "Warning",
            "1105",
            "The statistics of related partitions will be outdated after reorganizing partitions. Please use 'ANALYZE TABLE' statement if you want to update it now"
        ]]
    );
    assert!(create_text(&mut session, "t").contains("PARTITION `p3`"));
    session.run("alter table t coalesce partition 2").unwrap();
    assert!(create_text(&mut session, "t").ends_with("PARTITION BY HASH (`a`) PARTITIONS 2"));
    session
        .run("alter table t add partition (partition p3 comment 'three')")
        .unwrap();
    // The generated name is `p{count}`, so `p3` is taken.
    assert_eq!(
        code(&mut session, "alter table t add partition partitions 1"),
        (1517, "Duplicate partition name p3".to_owned())
    );
    assert_eq!(
        code(
            &mut session,
            "alter table t add partition (partition q values less than (10))"
        )
        .0,
        1480
    );
    assert_eq!(
        row_text(session.run("select count(*) from t")),
        vec![vec!["4"]]
    );
}

/// Go `setDataFromPartitions` keeps the description and comment as strings,
/// and `checkUniqueKeyIncludePartKey` does not count a prefix key part.
#[test]
fn partition_metadata_and_prefix_keys_follow_go() {
    let mut session = Session::new();
    session
        .run("create table h (a int) partition by hash (a) partitions 2")
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "select partition_description, partition_comment from information_schema.partitions where table_name = 'h' and partition_name = 'p0'"
        )),
        vec![vec!["", ""]]
    );
    let needs_global = |name: &str| {
        (
            8264,
            format!("Global Index is needed for index '{name}', since the unique index is not including all partitioning columns, and GLOBAL is not given as IndexOption"),
        )
    };
    assert_eq!(
        code(
            &mut session,
            "create table p (a varchar(20), unique index (a(5))) partition by range columns (a) (partition p0 values less than ('aaaaa'))"
        ),
        needs_global("a")
    );
    session
        .run("create table p (a varchar(20)) partition by range columns (a) (partition p0 values less than ('aaaaa'))")
        .unwrap();
    assert_eq!(
        code(&mut session, "alter table p add unique index (a(5))"),
        needs_global("a")
    );
}

/// Go `checkModifyCharsetAndCollation` with the column's index membership:
/// an indexed column cannot change to an incompatible collation unless the
/// type change reorganizes the column anyway.
#[test]
fn an_indexed_column_keeps_a_compatible_collation() {
    let mut session = Session::new();
    session
        .run("create table t (b varchar(10) collate utf8_bin, c varchar(10) collate utf8_general_ci, index (b), index (c)) collate utf8_bin")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "alter table t modify b varchar(10) collate utf8_general_ci"
        ),
        (
            8200,
            "Unsupported modifying collation of column 'b' from 'utf8_bin' to 'utf8_general_ci' when index is defined on it.".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "alter table t convert to charset utf8 collate utf8_general_ci"
        ),
        (
            8200,
            "Unsupported converting collation of column 'b' from 'utf8_bin' to 'utf8_general_ci' when index is defined on it.".to_owned()
        )
    );
    session
        .run("create table u (a text, unique index idx (a(2)))")
        .unwrap();
    session.run("alter table u modify column a int").unwrap();
}

/// Go escapes SHOW CREATE identifiers by the session's ANSI_QUOTES mode.
#[test]
fn show_create_follows_ansi_quotes() {
    let mut session = Session::new();
    session
        .run("create table t (a int, b varchar(255)) partition by list columns (a, b) (partition p0 values in ((1, '1')))")
        .unwrap();
    session.run("set sql_mode = 'ANSI_QUOTES'").unwrap();
    let text = create_text(&mut session, "t");
    assert!(
        text.starts_with("CREATE TABLE \"t\" (\n  \"a\" int"),
        "{text}"
    );
    assert!(
        text.contains("PARTITION BY LIST COLUMNS(\"a\",\"b\")"),
        "{text}"
    );
    assert!(text.contains("(PARTITION \"p0\" VALUES IN"), "{text}");
    assert!(row_text(session.run("show create database test"))[0][1]
        .starts_with("CREATE DATABASE \"test\""));
}

/// Go's SHOW surfaces: `SHOW OPEN TABLES`' four columns, a lowercased LIKE
/// pattern, and a masked LDAP bind password.
#[test]
fn show_statements_follow_go() {
    let mut session = Session::new();
    let crate::StmtOutput::Rows { columns, .. } =
        session.run_with_columns("show open tables").unwrap()
    else {
        panic!("expected rows");
    };
    let names: Vec<_> = columns.into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, vec!["Database", "Table", "In_use", "Name_locked"]);
    assert_eq!(
        row_text(session.run("show collation like 'UTF8MB4_BI%'"))
            .into_iter()
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec!["utf8mb4_bin"]
    );
    session
        .run("set global authentication_ldap_sasl_bind_root_pwd = 'password'")
        .unwrap();
    assert_eq!(
        row_text(session.run("show variables like 'AUTHENTICATION_LDAP_SASL_BIND_ROOT_PWD'")),
        vec![vec!["authentication_ldap_sasl_bind_root_pwd", "******"]]
    );
}
