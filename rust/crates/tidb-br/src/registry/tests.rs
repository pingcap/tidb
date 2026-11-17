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

//! Go `br/pkg/registry/registration_test.go`, and the registration
//! transaction over a scripted session.

use std::collections::VecDeque;
use std::sync::Arc;

use tidb_ast::CiString;
use tidb_model::{ColumnInfo, IndexColumn, IndexInfo, SchemaState, TableInfo};

use super::*;

/// A session answering each statement with the next scripted rows and
/// recording the statements.
#[derive(Clone, Default)]
struct ScriptSession {
    log: Arc<Mutex<Vec<String>>>,
    rows: Arc<Mutex<VecDeque<Vec<Vec<Datum>>>>>,
}

impl ScriptSession {
    fn answer(&self, sql: &str) -> Vec<Vec<Datum>> {
        self.log
            .lock()
            .unwrap()
            .push(sql.split_whitespace().collect::<Vec<_>>().join(" "));
        self.rows.lock().unwrap().pop_front().unwrap_or_default()
    }

    fn script(&self, rows: Vec<Vec<Vec<Datum>>>) {
        *self.rows.lock().unwrap() = rows.into();
    }

    /// The first two words of each statement.
    fn statements(&self) -> Vec<String> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.split(' ').take(2).collect::<Vec<_>>().join(" "))
            .collect()
    }
}

impl Session for ScriptSession {
    fn execute(&self, _: &Context, sql: &str) -> Result<()> {
        self.answer(sql);
        Ok(())
    }
    fn execute_internal(&self, _: &Context, sql: &str, _: &[SqlArg<'_>]) -> Result<()> {
        self.answer(sql);
        Ok(())
    }
    fn create_database_on_exist_error(&self, _: &Context, _: &tidb_model::DBInfo) -> Result<()> {
        unimplemented!()
    }
    fn create_table(
        &self,
        _: &Context,
        _: &str,
        _: &TableInfo,
        _: crate::glue::CreateTableConfig,
    ) -> Result<()> {
        unimplemented!()
    }
    fn create_tables(
        &self,
        _: &Context,
        _: &[(String, Vec<TableInfo>)],
        _: crate::glue::CreateTableConfig,
    ) -> Option<Result<()>> {
        None
    }
    fn create_placement_policy(&self, _: &Context, _: &tidb_model::PolicyInfo) -> Result<()> {
        unimplemented!()
    }
    fn get_global_variable(&self, _: &str) -> Result<String> {
        unimplemented!()
    }
    fn get_global_sys_var(&self, _: &str) -> Result<String> {
        unimplemented!()
    }
    fn get_all_ddl_jobs(&self, _: &Context) -> Result<Vec<tidb_model::Job>> {
        unimplemented!()
    }
    fn alter_table_mode(
        &self,
        _: &Context,
        _: i64,
        _: i64,
        _: tidb_model::TableMode,
    ) -> Result<()> {
        unimplemented!()
    }
    fn exec_restricted_sql(
        &self,
        _: &Context,
        sql: &str,
        _: &[SqlArg<'_>],
    ) -> Result<Vec<Vec<Datum>>> {
        Ok(self.answer(sql))
    }
}

fn registry(se: &ScriptSession) -> Registry {
    Registry {
        se: Mutex::new(Box::new(se.clone())),
        heartbeat_session: Arc::new(Mutex::new(Box::new(ScriptSession::default()))),
        heartbeat_manager: Mutex::new(None),
        wait_ids: Mutex::new(Vec::new()),
        table_exists: true,
        route_schema_ready: false,
        stale_check_tick: Duration::from_secs(60),
    }
}

fn info() -> RegistrationInfo {
    RegistrationInfo {
        filter_strings: vec!["*.*".into()],
        restored_ts: 0,
        upstream_cluster_id: 42,
        with_sys_table: true,
        cmd: "Restore".into(),
        ..Default::default()
    }
}

/// A new registration: looked up and inserted in one pessimistic
/// transaction, its ID from `LAST_INSERT_ID()`.
#[test]
fn creates_a_new_registration() {
    let se = ScriptSession::default();
    // The conflict lookup, BEGIN, the lookup, the insert, then the ID.
    se.script(vec![
        vec![],
        vec![],
        vec![],
        vec![],
        vec![vec![Datum::UInt(7)]],
    ]);
    let r = registry(&se);
    let (id, ts) = r
        .resume_or_create_registration(&Context::background(), info(), false)
        .unwrap();
    assert_eq!((id, ts), (7, 0));
    assert_eq!(
        se.statements(),
        [
            "SELECT id,",
            "BEGIN PESSIMISTIC",
            "SELECT id,",
            "INSERT INTO",
            "SELECT LAST_INSERT_ID()",
            "COMMIT",
            "SELECT id"
        ]
    );
}

/// A paused registration with the same parameters is resumed.
#[test]
fn resumes_a_paused_registration() {
    let se = ScriptSession::default();
    se.script(vec![
        vec![],
        vec![],
        vec![vec![Datum::UInt(3), Datum::new_string_datum("paused")]],
    ]);
    let r = registry(&se);
    let (id, _) = r
        .resume_or_create_registration(&Context::background(), info(), false)
        .unwrap();
    assert_eq!(id, 3);
    assert_eq!(
        se.statements(),
        [
            "SELECT id,",
            "BEGIN PESSIMISTIC",
            "SELECT id,",
            "UPDATE mysql.tidb_restore_registry",
            "COMMIT",
            "SELECT id"
        ]
    );
}

/// A running registration fails the restore and rolls back.
#[test]
fn refuses_a_running_registration() {
    let se = ScriptSession::default();
    se.script(vec![
        vec![],
        vec![],
        vec![vec![Datum::UInt(3), Datum::new_string_datum("running")]],
    ]);
    let r = registry(&se);
    let err = r
        .resume_or_create_registration(&Context::background(), info(), false)
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("task with ID 3 already exists and is running"),
        "{err}"
    );
    assert_eq!(se.statements().last().unwrap(), "ROLLBACK");
}

/// Routes are refused, as `--rename` has no SQL form.
#[test]
fn refuses_routes() {
    let se = ScriptSession::default();
    let r = registry(&se);
    let mut i = info();
    i.route_strings = vec!["a.*->b.*".into()];
    r.resume_or_create_registration(&Context::background(), i, false)
        .unwrap_err();
    assert!(se.statements().is_empty());
}

/// Go `TestRestoreRegistryRouteSchemaReadiness`.
#[test]
fn route_schema_readiness() {
    let names = [
        "filter_strings",
        "filter_hash",
        "source_filter_strings",
        "route_strings",
        "route_hash",
        "start_ts",
        "restored_ts",
        "upstream_cluster_id",
        "with_sys_table",
        "cmd",
    ];
    let columns = names
        .iter()
        .enumerate()
        .map(|(offset, n)| ColumnInfo {
            name: CiString::new(*n),
            offset: offset as i64,
            state: SchemaState::PUBLIC,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    let index = |cols: &[(&str, i64)], unique: bool| IndexInfo {
        name: CiString::new(RESTORE_REGISTRY_ROUTE_INDEX_NAME),
        state: SchemaState::PUBLIC,
        unique,
        columns: cols
            .iter()
            .map(|(n, l)| IndexColumn {
                name: CiString::new(*n),
                length: *l,
                ..Default::default()
            })
            .collect::<Vec<_>>()
            .into(),
        ..Default::default()
    };
    let table = |indices: Vec<IndexInfo>, columns: Vec<ColumnInfo>| TableInfo {
        columns: columns.into(),
        indices: indices.into(),
        ..Default::default()
    };
    let good = RESTORE_REGISTRY_ROUTE_INDEX_COLUMNS;
    assert!(has_restore_registry_route_schema(&table(
        vec![index(&good, true)],
        columns.clone()
    )));
    let legacy = IndexInfo {
        name: CiString::new(RESTORE_REGISTRY_LEGACY_INDEX_NAME),
        state: SchemaState::PUBLIC,
        unique: true,
        ..Default::default()
    };
    assert!(!has_restore_registry_route_schema(&table(
        vec![index(&good, true), legacy],
        columns.clone()
    )));
    let mut renamed = good;
    renamed[1].0 = "start_ts";
    assert!(!has_restore_registry_route_schema(&table(
        vec![index(&renamed, true)],
        columns.clone()
    )));
    let mut unspecified = good;
    unspecified[6].1 = -1;
    assert!(!has_restore_registry_route_schema(&table(
        vec![index(&unspecified, true)],
        columns.clone()
    )));
    assert!(!has_restore_registry_route_schema(&table(
        vec![index(&good, false)],
        columns.clone()
    )));
    let mut write_only = columns;
    write_only[4].state = SchemaState::WRITE_ONLY;
    assert!(!has_restore_registry_route_schema(&table(
        vec![index(&good, true)],
        write_only
    )));
}

/// Go `checkForTableConflicts`: a table another task's filter covers is
/// refused.
#[test]
fn table_conflicts_with_a_registered_task() {
    let db = tidb_model::DBInfo {
        id: 1,
        name: CiString::new("test"),
        ..Default::default()
    };
    let t = metautil::Table::with_schema(
        db.clone(),
        TableInfo {
            id: 2,
            name: CiString::new("t"),
            ..Default::default()
        },
    );
    let reg = RegistrationInfoWithID {
        info: RegistrationInfo {
            filter_strings: vec!["test.*".into()],
            cmd: "Restore".into(),
            ..Default::default()
        },
        restore_id: 5,
    };
    let f = tidb_table_filter::parse(&["test.*"]).unwrap();
    let err =
        check_for_table_conflicts(&[], std::slice::from_ref(&t), &reg, f.as_ref(), 6).unwrap_err();
    assert!(
        err.to_string()
            .contains("table test.t cannot be restored by current task with ID 6"),
        "{err}"
    );
    let other = tidb_table_filter::parse(&["other.*"]).unwrap();
    check_for_table_conflicts(&[], &[t], &reg, other.as_ref(), 6).unwrap();
}
