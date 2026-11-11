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

//! Go's masking-policy DDL (`ddl/masking_policy.go` and its executor
//! functions): the statements, their refusals and what the other DDL jobs do
//! to a table's policies, pinned by a Go testkit run of the same statements.

use super::table_mode::refusal;
use super::*;

fn text(session: &mut Session, sql: &str) -> Vec<String> {
    displayed(rows(session, sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect()
}

fn refused(session: &mut Session, sql: &str) -> String {
    refusal(session, sql).unwrap_or_else(|| panic!("{sql} ran"))
}

fn run(session: &mut Session, sql: &str) {
    assert_eq!(refusal(session, sql), None, "{sql}");
}

/// Go's `CreateMaskingPolicy`, `AddMaskingPolicy`, `AlterTableMaskingPolicyState`,
/// `ModifyMaskingPolicyExpression` and `ModifyMaskingPolicyRestrictOn` with
/// `buildMaskingPolicyInfo`'s and `createMaskingPolicyWithInfo`'s refusals.
#[test]
fn masking_policy_statements_check_and_store_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2131)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    run(&mut session, "create table t(c varchar(20), b int, g int as (b+1), j json)");
    for (sql, want) in [
        (
            "create masking policy p on t(c) as nosuch enable",
            "1054|Unknown column 'nosuch' in 'expression'",
        ),
        (
            "create masking policy p on t(c) as upper(nosuch) enable",
            "1054|Unknown column 'nosuch' in 'expression'",
        ),
        (
            "create masking policy p on t(c) as concat(c, t.nosuch) enable",
            "1054|Unknown column 'nosuch' in 'expression'",
        ),
        (
            "create masking policy p on t(c) as concat(c, x.c) enable",
            "1054|Unknown column 'x.c' in 'expression'",
        ),
        (
            "create masking policy p on t(c) as b enable",
            "8275|masking policy expression can only reference the target column 'c'",
        ),
        (
            "create masking policy p on t(nosuch) as c enable",
            "1054|Unknown column 'nosuch' in 't'",
        ),
        (
            "create masking policy p on t(g) as g enable",
            "3106|'masking policy on generated column' is not supported for generated columns.",
        ),
        (
            "create masking policy p on t(j) as j enable",
            "8200|Unsupported masking policy on unsupported column type",
        ),
        (
            "create masking policy p on nosuch(c) as c",
            "1146|Table 'test.nosuch' doesn't exist",
        ),
        (
            "create masking policy p on mysql.user(user) as user",
            "8200|Unsupported masking policy on system table",
        ),
        (
            "alter table t add masking policy p3 on (b) as mask_full(b) restrict on (insert_into_select, ctas) disable",
            "1305|FUNCTION test.mask_full does not exist",
        ),
    ] {
        assert_eq!(refused(&mut session, sql), want, "{sql}");
    }
    run(&mut session, "create masking policy p on t(c) as concat(c) enable");
    for (sql, want) in [
        (
            "create masking policy p on t(c) as c enable",
            "1105|masking policy p already exists",
        ),
        (
            "create masking policy if not exists p on t(b) as b enable",
            "1105|masking policy p already exists on another column",
        ),
        (
            "create masking policy p2 on t(c) as c enable",
            "1105|masking policy already exists on column c",
        ),
        (
            "create or replace masking policy p on t(b) as b",
            "1105|masking policy p already exists on another column",
        ),
        (
            "alter table t enable masking policy nosuch",
            "1105|masking policy nosuch doesn't exist",
        ),
        (
            "alter table t drop masking policy nosuch",
            "1105|masking policy nosuch doesn't exist",
        ),
    ] {
        assert_eq!(refused(&mut session, sql), want, "{sql}");
    }
    // IF NOT EXISTS on the policy's own column files the note and no job.
    run(&mut session, "create masking policy if not exists p on t(c) as c enable");
    assert_eq!(
        text(&mut session, "show warnings"),
        ["Note|1105|masking policy p already exists"]
    );
    run(&mut session, "alter table t add masking policy p3 on (b) as b disable");
    let policies = "select policy_id, policy_name, db_name, table_name, column_name, column_id, \
                    expression, status, masking_type, restrict_on, created_by \
                    from mysql.tidb_masking_policy order by policy_id";
    // `created_by` is Go's `sessVars.User.String()`; the Go run's testkit
    // session has no user and stores ''.
    assert_eq!(
        text(&mut session, policies),
        [
            "1|p|test|t|c|1|CONCAT(`c`)|ENABLED|CUSTOM|NONE|root@%",
            "2|p3|test|t|b|2|`b`|DISABLED|CUSTOM|NONE|root@%",
        ]
    );
    assert_eq!(
        refused(&mut session, "alter table t modify masking policy p set expression = b"),
        "8275|masking policy expression can only reference the target column 'c'"
    );
    run(&mut session, "alter table t modify masking policy p3 set restrict on none");
    run(
        &mut session,
        "alter table t modify masking policy p3 set restrict on (ctas, delete_select)",
    );
    run(&mut session, "alter table t disable masking policy p");
    assert_eq!(
        text(&mut session, policies),
        [
            "1|p|test|t|c|1|CONCAT(`c`)|DISABLED|CUSTOM|NONE|root@%",
            "2|p3|test|t|b|2|`b`|DISABLED|CUSTOM|DELETE_SELECT,CTAS|root@%",
        ]
    );
    assert_eq!(
        refused(
            &mut session,
            "alter table t add masking policy p4 on (b) as b, add column z int"
        ),
        "8200|Unsupported multi schema change for masking policy"
    );
    run(&mut session, "create view v as select * from t");
    assert_eq!(
        refused(&mut session, "create masking policy pv on v(c) as c"),
        "1347|'test.v' is not BASE TABLE"
    );
    run(&mut session, "create temporary table tt(c int)");
    assert_eq!(
        refused(&mut session, "create masking policy pt on tt(c) as c"),
        "1146|Table 'test.tt' doesn't exist"
    );
    // Go `checkMaskingPolicyOnModifyColumn`.
    for sql in [
        "alter table t modify column c json",
        "alter table t change column c c2 json",
    ] {
        assert_eq!(
            refused(&mut session, sql),
            "8200|Unsupported masking policy on unsupported column type",
            "{sql}"
        );
    }
    run(&mut session, "alter table t rename column c to c_new");
    run(&mut session, "alter table t modify column c_new varchar(30)");
    assert_eq!(
        text(
            &mut session,
            "select policy_name, column_name, column_id, expression \
             from mysql.tidb_masking_policy order by policy_id"
        ),
        ["p|c_new|1|CONCAT(`c_new`)", "p3|b|2|`b`"]
    );
}

/// What Go's other DDL jobs do to a table's policies: `OR REPLACE` keeps the
/// policy's row, DROP COLUMN / DROP TABLE / DROP DATABASE delete, TRUNCATE
/// and RENAME TABLE follow the table, MODIFY / CHANGE COLUMN follow the
/// column's name.
#[test]
fn ddl_jobs_keep_masking_policies_in_step_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2132)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    run(&mut session, "create table h(a varchar(10), b varchar(10), c int)");
    run(&mut session, "create masking policy p1 on h(a) as a");
    run(&mut session, "create masking policy p2 on h(b) as b");
    run(
        &mut session,
        "create or replace masking policy p1 on h(a) as upper(a) restrict on (ctas) disable",
    );
    assert_eq!(
        text(
            &mut session,
            "select policy_id, policy_name, column_name, expression, status, restrict_on \
             from mysql.tidb_masking_policy order by policy_id"
        ),
        ["1|p1|a|UPPER(`a`)|DISABLED|CTAS", "2|p2|b|`b`|ENABLED|NONE"]
    );
    run(&mut session, "alter table h drop column b");
    assert_eq!(
        text(
            &mut session,
            "select policy_name, table_name, column_name from mysql.tidb_masking_policy"
        ),
        ["p1|h|a"]
    );
    run(&mut session, "truncate table h");
    assert_eq!(
        text(
            &mut session,
            "select policy_name, table_id = (select tidb_table_id from information_schema.tables \
             where table_name='h') from mysql.tidb_masking_policy"
        ),
        ["p1|1"]
    );
    run(&mut session, "rename table h to h2");
    let names = "select policy_name, db_name, table_name from mysql.tidb_masking_policy";
    assert_eq!(text(&mut session, names), ["p1|test|h2"]);
    run(&mut session, "create database d2");
    run(&mut session, "rename table h2 to d2.h3");
    assert_eq!(text(&mut session, names), ["p1|d2|h3"]);
    let columns = "select policy_name, column_name, expression from mysql.tidb_masking_policy";
    run(&mut session, "alter table d2.h3 modify column a char(5)");
    assert_eq!(text(&mut session, columns), ["p1|a|UPPER(`a`)"]);
    run(&mut session, "alter table d2.h3 change column a a2 varchar(20)");
    assert_eq!(text(&mut session, columns), ["p1|a2|UPPER(`a2`)"]);
    run(&mut session, "alter table d2.h3 change column a2 a3 int");
    assert_eq!(text(&mut session, columns), ["p1|a3|UPPER(`a3`)"]);
    // The reorganization gave the column a new ID and the policy's row
    // follows it. Go's infoschema keeps the policy it cached under the old
    // column ID past this job and answers "masking policy p1 doesn't exist"
    // here although its row is the one above; this session reads the row.
    assert_eq!(
        text(
            &mut session,
            "select column_id = (select max(column_id) from mysql.tidb_masking_policy)              from mysql.tidb_masking_policy"
        ),
        ["1"]
    );
    assert_eq!(
        text(&mut session, "select column_id from mysql.tidb_masking_policy"),
        ["4"]
    );
    run(&mut session, "alter table d2.h3 enable masking policy p1");
    assert_eq!(
        refused(&mut session, "create masking policy p1 on d2.h3(a3) as a3"),
        "1105|masking policy p1 already exists"
    );
    let count = "select count(*) from mysql.tidb_masking_policy";
    assert_eq!(text(&mut session, count), ["1"]);
    run(&mut session, "alter table d2.h3 drop masking policy p1");
    assert_eq!(text(&mut session, count), ["0"]);
    run(&mut session, "create masking policy p1 on d2.h3(a3) as a3");
    run(&mut session, "drop table d2.h3");
    assert_eq!(text(&mut session, count), ["0"]);
    run(&mut session, "create table d2.k(a varchar(10))");
    run(&mut session, "create masking policy pk on d2.k(a) as a");
    run(&mut session, "drop database d2");
    assert_eq!(text(&mut session, count), ["0"]);
}
