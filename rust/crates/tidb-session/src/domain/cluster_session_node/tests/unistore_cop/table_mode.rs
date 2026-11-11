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

//! Go `executor.AlterTableMode` and `executor.RefreshMeta`: the internal
//! jobs IMPORT INTO and BR queue by ID.

use super::*;
use tidb_ddl::cluster_ddl::DdlStatement;
use tidb_model::TableMode;

/// The last job's id and its type, schema state and state.
fn last_job(session: &mut Session) -> (i64, String) {
    let row = displayed(rows(session, "admin show ddl jobs 1")).remove(0);
    (
        row[0].parse().unwrap(),
        [3, 4, 11].map(|column| row[column].clone()).join("|"),
    )
}

/// Go `TestTableModeBasic`'s `SetTableMode` cases: a transition Go allows
/// runs as an `ActionAlterTableMode` job, the mode a table already has
/// queues nothing, and Restore to Import is refused with Go's text.
#[test]
fn alter_table_mode_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2111)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    rows(
        &mut session,
        "create table t1_restore_import(id int, c1 int, c2 int, index idx1(c1))",
    );
    let stored = || {
        let (schema, table) = stack
            .factory
            .catalog_snapshot()
            .table_info_by_name("test", "t1_restore_import")
            .unwrap();
        let ids = (schema.read().id, table.read().id, table.read().mode);
        ids
    };
    let (schema_id, table_id, mode) = stored();
    assert_eq!(mode, TableMode::NORMAL);
    let set = |table_mode, schema_id, table_id| {
        stack
            .factory
            .ddl
            .execute(
                &DdlStatement::AlterTableMode {
                    table_mode,
                    schema_id,
                    table_id,
                },
                &tidb_sessionctx::StmtContext::for_query(),
            )
            .map(|_| ())
            .map_err(|error| format!("{error} {error:?}"))
    };

    set(TableMode::RESTORE, schema_id, table_id).unwrap();
    let (restored, kind) = last_job(&mut session);
    assert_eq!(kind, "alter table mode|public|synced");
    assert_eq!(stored().2, TableMode::RESTORE);

    let refused = set(TableMode::IMPORT, schema_id, table_id).unwrap_err();
    assert!(
        refused.contains(
            "Invalid mode set from (or by default) Restore to Import for table t1_restore_import"
        ),
        "{refused}"
    );
    assert_eq!(stored().2, TableMode::RESTORE);

    set(TableMode::RESTORE, schema_id, table_id).unwrap();
    assert_eq!(last_job(&mut session).0, restored, "the mode already holds: no job");

    set(TableMode::NORMAL, schema_id, table_id).unwrap();
    assert_eq!(stored().2, TableMode::NORMAL);
    set(TableMode::IMPORT, schema_id, table_id).unwrap();
    assert_eq!(stored().2, TableMode::IMPORT);
    assert!(last_job(&mut session).0 > restored);

    // Go `executor.AlterTableMode` resolves both IDs in the latest schema.
    let schema = set(TableMode::NORMAL, schema_id + 1000, table_id).unwrap_err();
    assert!(
        schema.contains(&format!("Unknown database 'SchemaID: {}'", schema_id + 1000)),
        "{schema}"
    );
    let table = set(TableMode::NORMAL, schema_id, table_id + 1000).unwrap_err();
    assert!(
        table.contains(&format!("Table 'test.TableID: {}' doesn't exist", table_id + 1000)),
        "{table}"
    );
}

/// What `sql` is refused with (`code|message`), `None` when it runs.
pub(super) fn refusal(session: &mut Session, sql: &str) -> Option<String> {
    use crate::domain::resultset_source::ResultSetSource;
    let mut result = match session.node_conn().execute(sql) {
        Ok(result) => result,
        Err(error) => return Some(format!("{}|{}", error.code, error.message)),
    };
    let source = result.source();
    loop {
        match source.next_batch(8) {
            Ok(batch) if batch.is_empty() => break,
            Ok(_) => {}
            Err(error) => return Some(format!("{}|{}", error.code, error.message)),
        }
    }
    source.finish().expect("finish");
    source.close().expect("close");
    None
}

/// Sets the mode of `test.<table>` through an `ActionAlterTableMode` job.
fn set_table_mode(stack: &UnistoreClusterStack, table: &str, table_mode: TableMode) {
    let (schema, info) = stack
        .factory
        .catalog_snapshot()
        .table_info_by_name("test", table)
        .unwrap();
    let (schema_id, table_id) = (schema.read().id, info.read().id);
    stack
        .factory
        .ddl
        .execute(
            &DdlStatement::AlterTableMode {
                table_mode,
                schema_id,
                table_id,
            },
            &tidb_sessionctx::StmtContext::for_query(),
        )
        .unwrap();
}

/// Go `TestTableModeBasic`'s statements on a table in `TableModeRestore`
/// (and `TableModeImport`): the statements that only read its metadata run,
/// every other is `ErrProtectedTableMode` -- from `core.CheckTableMode`, or
/// from the DDL executor for DROP TABLE and the renames.
#[test]
fn a_table_not_in_normal_mode_is_protected_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2113)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    for create in [
        "create table t1(id int, c1 int, c2 int, index idx1(c1))",
        "create table t2(id int, c1 int, c2 int, index idx1(c1))",
        "create table t1_foreign_key(id int, pid INT, INDEX idx_pid (pid),FOREIGN KEY fk_1 (pid) REFERENCES t1(c1) ON UPDATE SET NULL)",
        "create table t1_restore_import(id int, c1 int, c2 int, index idx1(c1))",
    ] {
        rows(&mut session, create);
    }
    set_table_mode(&stack, "t1_foreign_key", TableMode::IMPORT);
    assert_eq!(
        refusal(&mut session, "ALTER TABLE t1_foreign_key DROP FOREIGN KEY fk_1").as_deref(),
        Some("8258|Table t1_foreign_key is in mode Import"),
    );
    set_table_mode(&stack, "t1_restore_import", TableMode::RESTORE);

    for allowed in [
        "show create table t1_restore_import",
        "show table status where Name = 't1_restore_import'",
        "show columns from t1_restore_import",
        "show index from t1_restore_import",
        "describe t1_restore_import",
        "create table t1_restore_import_2 like t1_restore_import",
        "create view t1_restore_import_view as select * from t1_restore_import",
        "create table foreign_key_child(id int, pid INT, INDEX idx_pid (pid),FOREIGN KEY (pid) REFERENCES t1_restore_import(c1) ON DELETE CASCADE)",
        "drop table foreign_key_child",
        "admin checksum table t1_restore_import",
        // A table in `TableModeNormal` beside it is untouched.
        "select * from t1",
        "insert into t2 values(1, 1, 1)",
    ] {
        assert_eq!(refusal(&mut session, allowed), None, "{allowed}");
    }
    for protected in [
        "select * from t1_restore_import",
        "explain select * from t1_restore_import",
        "desc select * from t1_restore_import",
        "insert into t1_restore_import values(1, 1, 1)",
        "replace into t1_restore_import values(1,1,1)",
        "update t1_restore_import set id = 2 where id = 1",
        "delete from t1_restore_import where id = 2",
        "truncate table t1_restore_import",
        "drop table t1_restore_import",
        "alter table t1_restore_import rename to t1_new",
        "rename table t1_restore_import to t1_new",
        "rename table t1_restore_import to t1_new, t2 to t2_new",
        "alter table t1_restore_import modify column c2 bigint",
        "alter table t1_restore_import add column c3 int",
        "alter table t1_restore_import drop column c2",
        "alter table t1_restore_import drop index idx1",
        "alter table t1_restore_import add index idx2(c2)",
        "alter table t1_restore_import partition by range(id) (partition p0 values less than (100))",
        "alter table t1_restore_import comment='new comment'",
        "alter table t1_restore_import convert to character set utf8mb4",
        "alter table t1_restore_import rename column c1 to c1_new",
        "alter table t1_restore_import alter column c1 set default 100",
        "alter table t1_restore_import add foreign key fk_1 (c2) REFERENCES t1(c1) ON UPDATE SET NULL ",
        // Statements Go's test leaves out, whose table name its
        // preprocessor resolves the same way.
        "select * from t1 where id in (select id from t1_restore_import)",
        "insert into t2 select * from t1_restore_import",
        "explain insert into t1_restore_import values(1, 1, 1)",
        "explain update t1_restore_import set id = 2 where id = 1",
        "create index idx2 on t1_restore_import(c2)",
        "drop index idx1 on t1_restore_import",
        "analyze table t1_restore_import",
        "admin check table t1_restore_import",
    ] {
        assert_eq!(
            refusal(&mut session, protected).as_deref(),
            Some("8258|Table t1_restore_import is in mode Restore"),
            "{protected}"
        );
    }
    rows(&mut session, "begin");
    assert_eq!(
        refusal(&mut session, "insert into t1_restore_import values(1,1,1)").as_deref(),
        Some("8258|Table t1_restore_import is in mode Restore"),
    );
    rows(&mut session, "rollback");

    // Back in `TableModeNormal` the table is an ordinary one.
    set_table_mode(&stack, "t1_restore_import", TableMode::NORMAL);
    assert_eq!(refusal(&mut session, "insert into t1_restore_import values(1, 1, 1)"), None);
    assert_eq!(
        displayed(rows(&mut session, "select * from t1_restore_import")),
        [["1", "1", "1"]]
    );
}

/// Go `TestForeignKeyAndTableMode` (`pkg/ddl/tests/fk`): a parent's write
/// that would check or cascade into a child table not in `TableModeNormal`
/// is `ErrProtectedTableMode` (`buildOnDeleteOrUpdateFKTrigger`).
#[test]
fn a_parent_write_into_a_protected_child_is_refused_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2114)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test");
    rows(&mut session, "USE test");
    for sql in [
        "create table parent_1(id int primary key, name varchar(20))",
        "create table parent_2(id int primary key, name varchar(20))",
        "create table child_delete_cascade(id int primary key, parent_id int, constraint fk_cascade foreign key (parent_id) references parent_1(id) on delete cascade)",
        "create table child_update_cascade(id int primary key, parent_id int, constraint fk_cascade foreign key (parent_id) references parent_2(id) on update cascade)",
        "insert into parent_1 values(1, 'parent_1')",
        "insert into child_delete_cascade values(111, 1)",
        "insert into parent_2 values(2, 'parent_2'), (22222, 'parent_22')",
        "insert into child_update_cascade values(222, 2)",
    ] {
        rows(&mut session, sql);
    }
    let children = ["child_delete_cascade", "child_update_cascade"];
    for child in children {
        set_table_mode(&stack, child, TableMode::IMPORT);
    }
    for (sql, child) in [
        ("delete from parent_1 where id = 1", children[0]),
        ("update parent_2 set id = 22 where id = 2", children[1]),
        (
            "insert into parent_2 values (2, 'parent_11') on duplicate key update id =22",
            children[1],
        ),
    ] {
        assert_eq!(
            refusal(&mut session, sql),
            Some(format!("8258|Table {child} is in mode Import")),
            "{sql}"
        );
    }
    for child in children {
        set_table_mode(&stack, child, TableMode::NORMAL);
    }
    rows(&mut session, "delete from parent_1 where id = 1");
    rows(&mut session, "update parent_2 set id = 22 where id = 2");
    assert!(rows(&mut session, "select * from child_delete_cascade").is_empty());
    assert_eq!(
        displayed(rows(&mut session, "select * from child_update_cascade")),
        [["222", "22"]]
    );
}

/// Go `TestRefreshMetaBasic`: an `ActionRefreshMeta` job publishes exactly
/// one schema version.
#[test]
fn refresh_meta_runs_as_a_persisted_job_like_go() {
    let (stack, _users) = cop_backed_stack();
    let mut session = stack.factory.open_session(session_context(2112)).unwrap();
    rows(&mut session, "CREATE DATABASE IF NOT EXISTS test1");
    rows(&mut session, "USE test1");
    rows(&mut session, "create table t1(id int)");
    let (schema, table) = stack
        .factory
        .catalog_snapshot()
        .table_info_by_name("test1", "t1")
        .unwrap();
    let (schema_id, table_id) = (schema.read().id, table.read().id);
    let schema_version = |session: &mut Session| -> i64 {
        displayed(rows(session, "ADMIN SHOW DDL"))[0][0].parse().unwrap()
    };
    let before = schema_version(&mut session);
    let (created, _) = last_job(&mut session);
    stack
        .factory
        .ddl
        .execute(
            &DdlStatement::RefreshMeta {
                schema_id,
                table_id,
                involved_db: "test1".to_owned(),
                involved_table: "t1".to_owned(),
            },
            &tidb_sessionctx::StmtContext::for_query(),
        )
        .expect("the refresh runs");
    assert_eq!(schema_version(&mut session), before + 1);
    let (refreshed, kind) = last_job(&mut session);
    assert!(refreshed > created);
    assert_eq!(kind, "refresh meta|public|synced");
}
