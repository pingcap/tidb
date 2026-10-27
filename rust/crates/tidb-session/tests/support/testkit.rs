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

//! Go `pkg/testkit` (`testkit.go`, `result.go`) over a mock-store
//! [`Session`]: the tests read like their Go originals -- settings are
//! `tk.must_exec("set ...")`, warnings are `show warnings`, ids are
//! `select last_insert_id()`, and metadata is read the way Go's
//! `external.GetTableByName` reads it, from the domain's current schema.

#![allow(dead_code)]

use std::sync::{Mutex, MutexGuard, PoisonError};

use tidb_datatype::Datum;
use tidb_executor::{Catalog, MysqlError, TableEntry};
use tidb_session::{Session, StmtResult};

/// Go `testkit.TestKit`: one connection to a mock store.
pub struct TestKit {
    session: Session,
    affected_rows: u64,
}

impl TestKit {
    /// Go `testkit.CreateMockStore(t)` + `testkit.NewTestKit(t, store)` +
    /// `tk.MustExec("use test")`, the prologue nearly every Go test opens
    /// with.
    #[must_use]
    pub fn new() -> Self {
        let mut tk = Self::from_session(Session::new());
        tk.must_exec("use test");
        tk
    }

    /// Go `testkit.NewTestKit(t, testkit.CreateMockStore(t,
    /// mockstore.WithMockTiFlash(nodes)))`.
    #[must_use]
    pub fn new_with_mock_tiflash(nodes: usize) -> Self {
        let mut tk = Self::from_session(Session::new_with_mock_tiflash(nodes));
        tk.must_exec("use test");
        tk
    }

    /// Go `testkit.NewTestKit(t, store)` on this kit's store: another
    /// connection, with no current database (Go tests `use` one
    /// explicitly).
    #[must_use]
    pub fn new_tk(&self) -> Self {
        Self::from_session(Session::with_catalog(self.session.shared_catalog()))
    }

    /// Go `testkit.NewTestKitWithSession`.
    #[must_use]
    pub fn from_session(session: Session) -> Self {
        Self {
            session,
            affected_rows: 0,
        }
    }

    /// Go `tk.Session()`.
    pub fn session(&mut self) -> &mut Session {
        &mut self.session
    }

    /// Go `tk.Exec(sql)`.
    pub fn exec(&mut self, sql: &str) -> Result<StmtResult, MysqlError> {
        let result = self.session.run(sql).map_err(|error| error.to_mysql_error());
        self.affected_rows = match &result {
            Ok(StmtResult::Affected(rows)) => *rows,
            _ => 0,
        };
        result
    }

    /// Go `tk.MustExec(sql)`.
    pub fn must_exec(&mut self, sql: &str) {
        if let Err(error) = self.exec(sql) {
            panic!("sql: {sql}, error: {}", error.error_string());
        }
    }

    /// Go `tk.MustQuery(sql)`.
    pub fn must_query(&mut self, sql: &str) -> TkResult {
        match self.exec(sql) {
            Ok(StmtResult::Rows(rows)) => TkResult::new(sql, &rows),
            Ok(_) => TkResult::new(sql, &[]),
            Err(error) => panic!("sql: {sql}, error: {}", error.error_string()),
        }
    }

    /// Go `tk.QueryToErr(sql)` / `tk.ExecToErr(sql)`: the error, or `None`.
    pub fn exec_to_err(&mut self, sql: &str) -> Option<MysqlError> {
        self.exec(sql).err()
    }

    /// Go `tk.MustExecToErr(sql)` / `tk.MustQueryToErr(sql)`: the statement
    /// fails, and the failure is returned for further assertions.
    pub fn must_exec_to_err(&mut self, sql: &str) -> MysqlError {
        match self.exec(sql) {
            Ok(result) => panic!("sql: {sql}, expected an error, got {result:?}"),
            Err(error) => error,
        }
    }

    /// Go `tk.MustGetErrCode(sql, code)`: the wire code of the failure
    /// (`terror.ToSQLError`), returned for further assertions.
    pub fn must_get_err_code(&mut self, sql: &str, code: u16) -> MysqlError {
        let error = self.must_exec_to_err(sql);
        assert_eq!(
            error.code,
            code,
            "sql: {sql}, Assertion failed, origin err: {}",
            error.error_string()
        );
        error
    }

    /// Go `tk.MustGetErrMsg(sql, msg)`: `err.Error()`, which for a
    /// `*terror.Error` is `[<class>:<code>]<message>`.
    pub fn must_get_err_msg(&mut self, sql: &str, msg: &str) {
        let error = self.must_exec_to_err(sql);
        assert_eq!(error.error_string(), msg, "sql: {sql}");
    }

    /// Go `tk.MustContainErrMsg(sql, msg)`.
    pub fn must_contain_err_msg(&mut self, sql: &str, msg: &str) {
        let error = self.must_exec_to_err(sql);
        let text = error.error_string();
        assert!(text.contains(msg), "sql: {sql}, {text:?} does not contain {msg:?}");
    }

    /// Go `tk.MustHavePlan(sql, plan)`: some `explain` row's `id` contains
    /// `plan`.
    pub fn must_have_plan(&mut self, sql: &str, plan: &str) {
        let (has, rs) = self.has_plan(sql, plan);
        assert!(has, "{sql} doesn't have plan {plan}, full plan {:?}", rs.rows());
    }

    /// Go `tk.MustNotHavePlan(sql, plan)`.
    pub fn must_not_have_plan(&mut self, sql: &str, plan: &str) {
        let (has, rs) = self.has_plan(sql, plan);
        assert!(!has, "{sql} shouldn't have plan {plan}, full plan {:?}", rs.rows());
    }

    fn has_plan(&mut self, sql: &str, plan: &str) -> (bool, TkResult) {
        let rs = self.must_query(&format!("explain {sql}"));
        let has = rs.rows.iter().any(|row| row[0].contains(plan));
        (has, rs)
    }

    /// Go `tk.Session().AffectedRows()`: the last statement's affected rows.
    #[must_use]
    pub fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Go `tk.Session().LastInsertID()`'s session-visible value, which
    /// `select last_insert_id()` reads.
    #[must_use]
    pub fn last_insert_id(&self) -> u64 {
        self.session.last_insert_id()
    }

    /// Go `tk.CheckExecResult(affectedRows, insertID)`.
    pub fn check_exec_result(&self, affected_rows: u64, insert_id: u64) {
        assert_eq!(self.affected_rows(), affected_rows, "affected rows");
        assert_eq!(self.session.statement_insert_id(), insert_id, "insert id");
    }

    /// Go `tk.Session().GetSessionVars().ClientCapability |=
    /// mysql.ClientFoundRows` (or `&^=`): the connection's negotiated
    /// `CLIENT_FOUND_ROWS` bit.
    pub fn set_client_found_rows(&mut self, enabled: bool) {
        self.session.set_client_found_rows(enabled);
    }

    /// Go `dom.InfoSchema()` / `tk.Session().GetLatestInfoSchema()`: the
    /// domain's current schema, read through a new connection so it is never
    /// a snapshot this kit cached.
    #[must_use]
    pub fn info_schema(&self) -> Catalog {
        let fresh = Session::with_catalog(self.session.shared_catalog());
        let catalog = fresh
            .shared_catalog()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        catalog
    }

    /// Go `dom.InfoSchema().SchemaMetaVersion()`: the schema version the
    /// session's domain serves.
    #[must_use]
    pub fn schema_version(&self) -> u64 {
        let version = tidb_session::domain::mock_store::domain_of(&self.session)
            .expect("a mock-store session")
            .factory()
            .catalog_snapshot()
            .schema_version;
        u64::try_from(version).unwrap_or(0)
    }

    /// Go `external.GetTableByName(t, tk, db, table)`.
    #[must_use]
    pub fn get_table_by_name(&self, db: &str, table: &str) -> TableEntry {
        self.info_schema()
            .table_in(db, table)
            .unwrap_or_else(|| panic!("table {db}.{table} does not exist"))
            .clone()
    }

    /// Go `domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo`.
    pub fn update_table_replica_info(&mut self, physical_id: i64, available: bool) {
        tidb_session::domain::mock_store::domain_of(&self.session)
            .expect("a mock-store session")
            .factory()
            .update_table_replica_info(physical_id, available)
            .unwrap_or_else(|error| panic!("UpdateTableReplicaInfo({physical_id}): {error:?}"));
    }
}

/// Go `testkit.Result`: a query's rows rendered as Go's
/// `session.ResultSetToStringSlice` renders them (`Datum.ToString`, NULL as
/// `<nil>`).
#[derive(Debug, Clone)]
pub struct TkResult {
    sql: String,
    rows: Vec<Vec<String>>,
}

/// Go `ResultSetToStringSlice`'s rendering of one datum.
#[must_use]
pub fn go_string(datum: &Datum) -> String {
    if datum.is_null() {
        return "<nil>".to_owned();
    }
    let bytes = datum
        .sql_bytes()
        .unwrap_or_else(|error| panic!("Datum.ToString of {datum:?}: {error:?}"));
    String::from_utf8_lossy(&bytes).into_owned()
}

impl TkResult {
    fn new(sql: &str, rows: &[Vec<Datum>]) -> Self {
        Self {
            sql: sql.to_owned(),
            rows: rows
                .iter()
                .map(|row| row.iter().map(go_string).collect())
                .collect(),
        }
    }

    /// Go `res.Check(testkit.Rows(...))`: each expected row is its cells
    /// joined by single spaces, as `testkit.Rows` splits them.
    #[track_caller]
    pub fn check<S: AsRef<str>>(&self, expected: impl IntoIterator<Item = S>) {
        let got: Vec<String> = self.rows.iter().map(|row| row.join(" ")).collect();
        let want: Vec<String> = expected
            .into_iter()
            .map(|row| row.as_ref().to_owned())
            .collect();
        assert_eq!(got, want, "sql: {}", self.sql);
    }

    /// Go `res.Check(testkit.Rows())`: no rows.
    #[track_caller]
    pub fn check_empty(&self) {
        self.check(std::iter::empty::<&str>());
    }

    /// Go `res.Check(testkit.RowsWithSep(sep, ...))`.
    #[track_caller]
    pub fn check_with_sep<S: AsRef<str>>(&self, sep: &str, expected: impl IntoIterator<Item = S>) {
        let got: Vec<Vec<String>> = self.rows.clone();
        let want: Vec<Vec<String>> = expected
            .into_iter()
            .map(|row| row.as_ref().split(sep).map(str::to_owned).collect())
            .collect();
        assert_eq!(got, want, "sql: {}", self.sql);
    }

    /// Go `res.Sort()`.
    #[must_use]
    pub fn sort(mut self) -> Self {
        self.rows.sort();
        self
    }

    /// Go `res.Rows()`.
    #[must_use]
    pub fn rows(&self) -> &[Vec<String>] {
        &self.rows
    }

    /// Go `res.CheckAt(cols, expected)`.
    #[track_caller]
    pub fn check_at<S: AsRef<str>>(&self, cols: &[usize], expected: impl IntoIterator<Item = S>) {
        let got: Vec<String> = self
            .rows
            .iter()
            .map(|row| {
                cols.iter()
                    .map(|&col| row[col].as_str())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        let want: Vec<String> = expected
            .into_iter()
            .map(|row| row.as_ref().to_owned())
            .collect();
        assert_eq!(got, want, "sql: {}", self.sql);
    }

    /// Go `res.CheckContain(expected)`: some cell contains `expected`.
    #[track_caller]
    pub fn check_contain(&self, expected: &str) {
        assert!(
            self.rows.iter().flatten().any(|cell| cell.contains(expected)),
            "sql: {}, {:?} not found in {:?}",
            self.sql,
            expected,
            self.rows
        );
    }

    /// Go `res.CheckNotContain(unexpected)`.
    #[track_caller]
    pub fn check_not_contain(&self, unexpected: &str) {
        assert!(
            !self.rows.iter().flatten().any(|cell| cell.contains(unexpected)),
            "sql: {}, {:?} found in {:?}",
            self.sql,
            unexpected,
            self.rows
        );
    }
}

/// Serializes every test that mutates process-wide state (Go marks such
/// tests serial): the global config, and the process-wide sysvar atomics a
/// `SET GLOBAL` stores.
static SERIAL: Mutex<()> = Mutex::new(());

/// Go `defer config.RestoreFunc()()` after `config.UpdateGlobal(...)`, holding
/// the process-wide serial lock until the config is restored.
pub struct ConfigGuard {
    restore: Box<dyn Fn()>,
    _serial: MutexGuard<'static, ()>,
}

impl Drop for ConfigGuard {
    fn drop(&mut self) {
        (self.restore)();
    }
}

/// Go `testfailpoint.Enable(t, name, actions)`: the failpoint is removed
/// when the guard drops (Go's `t.Cleanup`).
pub struct FailpointGuard(&'static str);

impl Drop for FailpointGuard {
    fn drop(&mut self) {
        fail::remove(self.0);
    }
}

/// Go `testfailpoint.Enable(t, name, actions)`.
#[must_use]
pub fn enable_failpoint(name: &'static str, actions: &str) -> FailpointGuard {
    fail::cfg(name, actions).expect("enable failpoint");
    FailpointGuard(name)
}

/// Go `config.UpdateGlobal(update)` + `defer config.RestoreFunc()()`.
#[must_use]
pub fn update_global_config(
    update: impl FnOnce(&mut tidb_config::config_tree::config::Config),
) -> ConfigGuard {
    let serial = SERIAL.lock().unwrap_or_else(PoisonError::into_inner);
    let restore = Box::new(tidb_config::config_tree::config::restore_func());
    tidb_config::config_tree::config::update_global(update);
    ConfigGuard {
        restore,
        _serial: serial,
    }
}

/// The process-wide serial lock alone, for a serial test that mutates
/// process-wide state other than the config (a `SET GLOBAL` of a sysvar Go
/// keeps in a process atomic).
#[must_use]
pub fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}
