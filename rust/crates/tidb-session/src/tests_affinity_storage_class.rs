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

//! Table AFFINITY and the ENGINE_ATTRIBUTE storage classes, as
//! `ddl/affinity.test` and `ddl/storage_class.test` record.

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

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// Go `handleTableOptions`, `validateTableAffinity` and the partition DDL
/// refusals for a table with AFFINITY.
#[test]
fn affinity_reads_back_and_refuses_what_go_refuses() {
    let mut session = Session::new();
    session
        .run("create table t1 (a int) affinity = 'TABLE'")
        .unwrap();
    session
        .run("create table tp1 (a int) affinity = 'partition' partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))")
        .unwrap();
    assert!(create_text(&mut session, "t1").contains(" /*T![affinity] AFFINITY='table' */"));
    assert_eq!(
        rows(
            &mut session,
            "select table_name, tidb_affinity from information_schema.tables where table_schema = database() and table_name in ('t1', 'tp1') order by table_name"
        ),
        vec![vec!["t1", "table"], vec!["tp1", "partition"]]
    );
    assert_eq!(
        rows(
            &mut session,
            "select table_name, partition_name, tidb_affinity from information_schema.partitions where table_schema = database() and table_name in ('t1', 'tp1') order by table_name, partition_name"
        ),
        vec![
            vec!["t1", "NULL", "table"],
            vec!["tp1", "p0", "partition"],
            vec!["tp1", "p1", "partition"],
        ]
    );
    assert_eq!(
        code(
            &mut session,
            "create table tx (a int) affinity = 'invalid_affinity'"
        ),
        (8266, "Invalid AFFINITY 'invalid_affinity'".to_owned())
    );
    assert_eq!(
        code(&mut session, "alter table t1 affinity = 'partition'"),
        (
            8266,
            "Can not set AFFINITY='partition' on a non-partition table.".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "create temporary table temp1 (a int) affinity = 'table'"
        ),
        (
            8266,
            "Can not set AFFINITY on a temporary table.".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            "alter table tp1 add partition (partition p2 values less than (30))"
        ),
        (
            8200,
            "Unsupported DDL operation: ADD PARTITION of a table with AFFINITY option".to_owned()
        )
    );
    session.run("alter table t1 affinity = 'none'").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "select tidb_affinity from information_schema.tables where table_schema = database() and table_name = 't1'"
        ),
        vec![vec!["NULL"]]
    );
}

/// Go `handleEngineAttributeForCreateTable`, `onModifyTableEngineAttribute`
/// and `GetSimpleTableStorageClassForShowCreate`.
#[test]
fn table_storage_class_follows_the_engine_attribute() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();
    let table_class = |session: &mut Session| {
        rows(
            session,
            "select tidb_storage_class from information_schema.tables where table_schema = database() and table_name = 't'",
        )[0][0]
            .clone()
    };
    assert_eq!(table_class(&mut session), "");
    session
        .run(r#"alter table t ENGINE_ATTRIBUTE = '{"storage_class": "ia"}'"#)
        .unwrap();
    assert_eq!(table_class(&mut session), "IA");
    assert!(create_text(&mut session, "t").contains(") ENGINE=InnoDB STORAGE_CLASS='IA' DEFAULT"));
    session
        .run(r#"alter table t ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"STANDARD", "transitions":[{"tier":"IA", "after_days":30}]}}'"#)
        .unwrap();
    assert_eq!(
        table_class(&mut session),
        r#"{"tier":"STANDARD","transitions":[{"tier":"IA","after_days":30}]}"#
    );
    assert!(create_text(&mut session, "t").contains(
        r#"ENGINE=InnoDB ENGINE_ATTRIBUTE='{"storage_class": {"tier":"STANDARD", "transitions":[{"tier":"IA", "after_days":30}]}}' DEFAULT"#
    ));
    session
        .run("alter table t storage_class = 'standard'")
        .unwrap();
    assert_eq!(table_class(&mut session), "STANDARD");
    assert_eq!(
        code(
            &mut session,
            r#"create table u (a int) ENGINE_ATTRIBUTE = '{"storage_class": "AI"}'"#
        ),
        (
            8271,
            "Invalid storage class: invalid storage class tier: AI".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            r#"create table u (a int) ENGINE_ATTRIBUTE = '{' ENGINE_ATTRIBUTE = '{"storage_class": "IA"}'"#
        ),
        (
            8270,
            "Invalid engine attribute format: 'unexpected end of JSON input'".to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            r#"alter table t STORAGE_CLASS = 'STANDARD', ENGINE_ATTRIBUTE = '{"storage_class": "IA"}'"#
        ),
        (
            8271,
            "Invalid storage class: can not specify 'ENGINE_ATTRIBUTE' and 'STORAGE_CLASS' together"
                .to_owned()
        )
    );
}

/// Go `BuildStorageClassForPartitions`: scoped definitions pick partitions
/// by name, RANGE bound or LIST value, and an added partition resolves
/// against the table's attribute.
#[test]
fn partition_storage_classes_follow_their_scope() {
    let mut session = Session::new();
    let classes = |session: &mut Session| {
        rows(
            session,
            "select partition_name, tidb_storage_class from information_schema.partitions where table_schema = database() and table_name = 't' order by partition_ordinal_position",
        )
    };
    session
        .run(r#"create table t (id int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "names_in":["p0", "P1"]}}' partition by range (id) (partition p0 values less than (100))"#)
        .unwrap();
    session
        .run("alter table t add partition (partition p1 values less than (200), partition p2 values less than (300))")
        .unwrap();
    assert_eq!(
        classes(&mut session),
        vec![vec!["p0", "IA"], vec!["p1", "IA"], vec!["p2", "STANDARD"]]
    );
    assert_eq!(
        code(
            &mut session,
            r#"alter table t ENGINE_ATTRIBUTE = '[{"tier":"IA"}]'"#
        ),
        (
            8270,
            "Invalid engine attribute format: 'json: cannot unmarshal array into Go value of type model.EngineAttribute'".to_owned()
        )
    );
    session
        .run(r#"alter table t ENGINE_ATTRIBUTE = '{"storage_class": [{"tier":"IA", "less_than":"200"}, {"tier":"STANDARD"}]}'"#)
        .unwrap();
    assert_eq!(
        classes(&mut session),
        vec![vec!["p0", "IA"], vec!["p1", "IA"], vec!["p2", "STANDARD"]]
    );

    session.run("drop table t").unwrap();
    session
        .run(r#"create table t (a int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "values_in":["2"]}}' partition by list (a) (partition p0 values in (1), partition p1 values in (2))"#)
        .unwrap();
    assert_eq!(
        classes(&mut session),
        vec![vec!["p0", "STANDARD"], vec!["p1", "IA"]]
    );
    session.run("drop table t").unwrap();
    assert_eq!(
        code(
            &mut session,
            r#"create table t (a int, b int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"(2,2)"}}' partition by range columns (a, b) (partition p0 values less than (1,1), partition p1 values less than (2,2))"#
        ),
        (
            8271,
            "Invalid storage class: 'less_than' only supports single-column RANGE partitions"
                .to_owned()
        )
    );
    assert_eq!(
        code(
            &mut session,
            r#"create table t (a int) ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "names_in":["p0"]}}' partition by hash (a) partitions 2"#
        ),
        (
            8271,
            "Invalid storage class: partition-scoped storage_class does not support HASH or KEY partitions"
                .to_owned()
        )
    );
    session
        .run(r#"create table t (d date) STORAGE_CLASS = 'IA' partition by range columns (d) (partition p0 values less than ('2025-01-01'), partition p1 values less than (maxvalue))"#)
        .unwrap();
    assert_eq!(
        classes(&mut session),
        vec![vec!["p0", "IA"], vec!["p1", "IA"]]
    );
    session
        .run(r#"alter table t ENGINE_ATTRIBUTE = '{"storage_class": {"tier":"IA", "less_than":"2025-01-01"}}'"#)
        .unwrap();
    assert_eq!(
        classes(&mut session),
        vec![vec!["p0", "IA"], vec!["p1", "STANDARD"]]
    );
}
