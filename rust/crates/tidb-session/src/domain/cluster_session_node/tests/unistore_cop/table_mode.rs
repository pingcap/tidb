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
