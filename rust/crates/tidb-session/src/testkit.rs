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
//! [`Session`], for this crate's tests (`cfg(test)`) and the integration
//! suites (the `testkit` feature): the tests read like their Go originals -- settings are
//! `tk.must_exec("set ...")`, warnings are `show warnings`, ids are
//! `select last_insert_id()`, and metadata is read the way Go's
//! `external.GetTableByName` reads it, from the domain's current schema.

#![allow(dead_code)]

use crate::domain::cluster_session_node::DomainTestSessions as _;
use std::sync::{Mutex, MutexGuard, PoisonError};

use tidb_datatype::Datum;
use tidb_terror::MysqlError;
use tidb_infoschema::{SessionExtendedInfoSchema, TableEntry};
use crate::{Session, StmtResult};

/// Go `testkit.TestKit`: one connection to a mock store.
pub struct TestKit {
    session: Session,
    affected_rows: u64,
    /// Go `SessionVars.PreparedStmts`, by statement id (1-based).
    prepared: Vec<Option<crate::domain::session_api::PreparedGeneral>>,
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

    /// Go `testkit.NewTestKitWithSession`; the session gets the next
    /// connection id, as Go's `newSession` does
    /// (`se.SetConnectionID(testKitIDGenerator.Inc())`).
    #[must_use]
    pub fn from_session(mut session: Session) -> Self {
        static TEST_KIT_ID_GENERATOR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        session.set_connection_id(
            TEST_KIT_ID_GENERATOR.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1,
        );
        Self {
            session,
            affected_rows: 0,
            prepared: Vec::new(),
        }
    }

    /// Go `tk.Session().ShowProcess().ID`: this connection's id.
    #[must_use]
    pub fn connection_id(&self) -> u64 {
        self.session.connection_id()
    }

    /// Go `tk.Session()`.
    pub fn session(&mut self) -> &mut Session {
        &mut self.session
    }

    /// Go `tk.Exec(sql)`.
    pub fn exec(&mut self, sql: &str) -> Result<StmtResult, MysqlError> {
        // Go `ExecWithContext`: every statement of the text runs in order;
        // the first one's result set is the answer.
        let statements = self
            .session
            .split_statements(sql, true)
            .map_err(|error| error.to_mysql_error())?;
        if statements.len() > 1 {
            let mut first = None;
            for statement in &statements {
                let result = self.exec(statement)?;
                first.get_or_insert(result);
            }
            return Ok(first.expect("at least one statement"));
        }
        let result = self.session.run(sql).map_err(|error| error.to_mysql_error());
        self.affected_rows = match &result {
            Ok(StmtResult::Affected(rows)) => *rows,
            _ => 0,
        };
        result
    }

    /// Go `tk.Session().PrepareStmt(sql)`: `(stmtID, paramCount, fields,
    /// err)`. Statement ids count from 1 per session, as Go's
    /// `SessionVars.GetNextPreparedStmtID` does.
    pub fn prepare_stmt(
        &mut self,
        sql: &str,
    ) -> (u32, usize, Vec<tidb_executor::adapter::ResultFieldMetadata>, Option<MysqlError>) {
        match self.session.prepare_stmt(sql) {
            Ok(prepared) => {
                let params = prepared.parameter_count();
                let fields = prepared.result_fields().to_vec();
                self.prepared.push(Some(prepared));
                (u32::try_from(self.prepared.len()).expect("stmt id"), params, fields, None)
            }
            Err(error) => (0, 0, Vec::new(), Some(error.to_mysql_error())),
        }
    }

    /// Go `tk.Session().ExecutePreparedStmt(ctx, stmtID, args)`: `(rs, err)`;
    /// a statement without a result set answers an empty, non-nil result.
    pub fn execute_prepared_stmt(
        &mut self,
        stmt_id: u32,
        args: &[tidb_param::PreparedValue],
    ) -> (TkResult, Option<MysqlError>) {
        let prepared = self
            .prepared
            .get(stmt_id as usize - 1)
            .and_then(Option::clone)
            .unwrap_or_else(|| panic!("prepared statement {stmt_id} not found"));
        let sql = prepared.sql().to_owned();
        match self.session.execute_prepared_stmt(&prepared, args) {
            Ok(tidb_executor::StmtOutput::Rows { rows, .. }) => (TkResult::new(&sql, &rows), None),
            Ok(tidb_executor::StmtOutput::Affected(rows)) => {
                self.affected_rows = rows;
                (TkResult::nil(), None)
            }
            Ok(_) => (TkResult::nil(), None),
            Err(error) => (TkResult::nil(), Some(error.to_mysql_error())),
        }
    }

    /// Go `tk.Session().DropPreparedStmt(stmtID)`.
    pub fn drop_prepared_stmt(&mut self, stmt_id: u32) -> Option<MysqlError> {
        match self.prepared.get_mut(stmt_id as usize - 1) {
            Some(slot @ Some(_)) => {
                // Go `DropPreparedStmt` queues the id, and the next
                // statement's `cleanRetryInfo` deletes its cached plan.
                if let Some(ast) = slot.as_ref().and_then(|prepared| prepared.prepared_ast()) {
                    self.session.close_prepared(ast);
                }
                *slot = None;
                None
            }
            _ => Some(MysqlError::from_parts(8113, *b"HY000", format!("statement not found, id: {stmt_id}"))),
        }
    }

    /// Go `tk.Exec(sql, args...)` with arguments: `PrepareStmt`,
    /// `ExecutePreparedStmt` with `Args2Expressions4Test(args...)`, then
    /// `DropPreparedStmt`.
    pub fn exec_args(
        &mut self,
        sql: &str,
        args: &[tidb_param::PreparedValue],
    ) -> (TkResult, Option<MysqlError>) {
        let (stmt_id, _, _, err) = self.prepare_stmt(sql);
        if err.is_some() {
            return (TkResult::nil(), err);
        }
        let (rs, err) = self.execute_prepared_stmt(stmt_id, args);
        if err.is_some() {
            return (rs, err);
        }
        (rs, self.drop_prepared_stmt(stmt_id))
    }

    /// Go `tk.MustQuery(sql, args...)` with arguments.
    pub fn must_query_args(&mut self, sql: &str, args: &[tidb_param::PreparedValue]) -> TkResult {
        match self.exec_args(sql, args) {
            (rs, None) => rs,
            (_, Some(error)) => panic!("sql: {sql}, args: {args:?}, error: {}", error.error_string()),
        }
    }

    /// Go `tk.MustExec(sql, args...)` with arguments.
    pub fn must_exec_args(&mut self, sql: &str, args: &[tidb_param::PreparedValue]) {
        if let (_, Some(error)) = self.exec_args(sql, args) {
            panic!("sql: {sql}, args: {args:?}, error: {}", error.error_string());
        }
    }

    /// Go `tk.Session().GetSessionVars().PreparedStmts[id].(*PlanCacheStmt).StmtCacheable = cacheable`.
    pub fn set_prepared_stmt_cacheable(&mut self, stmt_id: u32, cacheable: bool) {
        let prepared = self
            .prepared
            .get_mut(stmt_id as usize - 1)
            .and_then(Option::as_mut)
            .unwrap_or_else(|| panic!("prepared statement {stmt_id} not found"));
        prepared.set_stmt_cacheable(cacheable);
    }

    /// Go `tk.Exec(sql)` read as `(rs, err)`.
    pub fn exec_rs(&mut self, sql: &str) -> (TkResult, Option<MysqlError>) {
        let multi = self.session.split_statements(sql, true).is_ok_and(|statements| statements.len() > 1);
        if multi {
            return match self.exec(sql) {
                Ok(StmtResult::Rows(rows)) => (TkResult::new(sql, &rows), None),
                Ok(_) => (TkResult::nil(), None),
                Err(error) => (TkResult::nil(), Some(error)),
            };
        }
        match self.session.run_with_fields(sql) {
            Ok((fields, tidb_executor::StmtOutput::Rows { rows, .. })) => {
                let mut rs = TkResult::new(sql, &rows);
                rs.fields = fields;
                (rs, None)
            }
            Ok((_, tidb_executor::StmtOutput::Affected(rows))) => {
                self.affected_rows = rows;
                (TkResult::nil(), None)
            }
            Ok(_) => (TkResult::nil(), None),
            Err(error) => (TkResult::nil(), Some(error.to_mysql_error())),
        }
    }

    /// Go `tk.CheckLastMessage(msg)`: the last statement's info text.
    pub fn check_last_message(&self, msg: &str) {
        assert_eq!(self.session.statement_message(), msg);
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

    /// Go `tk.MustIndexLookup(sql)`: the plan reads through an
    /// `IndexLookUp`, and the query's result.
    pub fn must_index_lookup(&mut self, sql: &str) -> TkResult {
        self.must_have_plan(sql, "IndexLookUp");
        self.must_query(sql)
    }

    /// Go `tk.MustPartition(sql, partitions)`: some `explain` row's
    /// `access object` ends with `partition:<partitions>` (or, for an empty
    /// `partitions`, none names a partition), and the query's result.
    pub fn must_partition(&mut self, sql: &str, partitions: &str) -> TkResult {
        let rs = self.must_query(&format!("explain {sql}"));
        let mut ok = partitions.is_empty();
        for row in &rs.rows {
            if partitions.is_empty() && row[3].contains("partition:") {
                ok = false;
            }
            if !partitions.is_empty() && row[3].ends_with(&format!("partition:{partitions}")) {
                ok = true;
            }
        }
        assert!(ok, "{sql} does not read partitions {partitions}: {:?}", rs.rows);
        self.must_query(sql)
    }

    /// Go `tk.MustPointGet(sql)`: the plan is a single `Point_Get`, and the
    /// query's result.
    pub fn must_point_get(&mut self, sql: &str) -> TkResult {
        let rs = self.must_query(&format!("explain {sql}"));
        assert_eq!(rs.rows.len(), 1, "{sql}: {:?}", rs.rows);
        assert!(rs.rows[0][0].contains("Point_Get"), "plan {}", rs.rows[0][0]);
        self.must_query(sql)
    }

    /// Go `tk.HasTiFlashPlan(sql)`: some `explain` row's task is a TiFlash
    /// one.
    pub fn has_tiflash_plan(&mut self, sql: &str) -> bool {
        let rs = self.must_query(&format!("explain {sql}"));
        rs.rows.iter().any(|row| row[2].contains("tiflash"))
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
    pub fn set_client_capability(&mut self, capability: u32) {
        self.session.set_client_capability(capability);
    }

    /// Go `dom.InfoSchema()` / `tk.Session().GetLatestInfoSchema()`: the
    /// domain's current schema, read through a new connection so it is never
    /// a snapshot this kit cached.
    #[must_use]
    pub fn info_schema(&self) -> SessionExtendedInfoSchema {
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
        let version = crate::domain::mock_store::domain_of(&self.session)
            .expect("a mock-store session")
            .factory()
            .catalog_snapshot()
            .schema_meta_version();
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
    /// Go `dom.StatsHandle().GetPhysicalTableStats(id, tblInfo)`.
    pub fn stats_table_for_test(&self, physical_id: i64) -> Option<std::sync::Arc<tidb_stats::Table>> {
        self.session.stats_table_for_test(physical_id)
    }

    pub fn update_table_replica_info(&mut self, physical_id: i64, available: bool) {
        crate::domain::mock_store::domain_of(&self.session)
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
    /// Go `rs.Fields()`, when the result came from a record set.
    fields: Vec<tidb_executor::adapter::ResultFieldMetadata>,
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
            fields: Vec::new(),
        }
    }

    /// Go `rs.Fields()`.
    #[must_use]
    pub fn fields(&self) -> &[tidb_executor::adapter::ResultFieldMetadata] {
        &self.fields
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
        // Go `Check` compares each row as `fmt.Sprintf("%s", row)`, so the
        // cells' boundaries are not part of the comparison.
        let got: Vec<String> = self.rows.iter().map(|row| row.join(" ")).collect();
        let want: Vec<String> = expected
            .into_iter()
            .map(|row| row.as_ref().split(sep).collect::<Vec<_>>().join(" "))
            .collect();
        assert_eq!(got, want, "sql: {}", self.sql);
    }

    /// Go `res.Sort()`.
    #[must_use]
    pub fn sort(mut self) -> Self {
        self.rows.sort();
        self
    }

    /// Go `result.Check(other.Rows())`: the same rows as another result.
    pub fn check_result(&self, expected: &TkResult) {
        assert_eq!(self.rows(), expected.rows());
    }

    /// Go `var r [][]any`: a result no query has filled yet (`r == nil`).
    #[must_use]
    pub fn nil() -> Self {
        Self { sql: String::new(), rows: Vec::new(), fields: Vec::new() }
    }

    /// Go `r == nil` for a [`TkResult::nil`] not yet assigned.
    #[must_use]
    pub fn is_nil(&self) -> bool {
        self.sql.is_empty()
    }

    /// Go `len(r)`: the row count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
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

/// Go `testkit.SetTiFlashReplica(t, dom, db, table)`: the domain's table
/// meta gets an available one-replica TiFlash replica, in place.
pub fn set_tiflash_replica(tk: &TestKit, db: &str, table: &str) {
    let TableEntry::Kv(tbl) = tk.get_table_by_name(db, table) else {
        panic!("{db}.{table} is not a storage-backed table");
    };
    tbl.meta().write().tiflash_replica = Some(tidb_model::GoShared::new(tidb_model::TiFlashReplicaInfo {
        count: 1,
        available: true,
        ..Default::default()
    }));
}

/// Go `testfailpoint.Enable(t, name, actions)`: the failpoint is removed
/// when the guard drops (Go's `t.Cleanup`).
#[must_use]
pub fn enable_failpoint(name: &str, actions: &str) -> tidb_failpoint::Guard {
    tidb_failpoint::enable_for_test(name, actions)
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

/// Go `rand.Intn(n)` for the ported tests' generated data.
pub fn rand_intn(n: i64) -> i64 {
    rand::random_range(0..n)
}

/// Go `fmt.Sprintf(format, args...)` for a format held in a variable: each
/// `%v` / `%d` / `%s` takes the next argument's `Display`, `%%` is `%`.
pub fn go_sprintf(format: &str, args: &[&dyn std::fmt::Display]) -> String {
    let mut out = String::with_capacity(format.len());
    let mut args = args.iter();
    let mut chars = format.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        match chars.next() {
            Some('%') => out.push('%'),
            Some('v' | 'd' | 's') => {
                let arg = args.next().expect("fmt.Sprintf: missing argument");
                out.push_str(&arg.to_string());
            }
            other => panic!("fmt.Sprintf: unsupported verb %{other:?}"),
        }
    }
    out
}

/// Go `fmt.Sprintf("%v", x)` of a test value: a row prints as `[a b c]`.
pub trait GoV {
    fn go_v(&self) -> String;
}

impl GoV for Vec<String> {
    fn go_v(&self) -> String {
        format!("[{}]", self.join(" "))
    }
}

impl GoV for String {
    fn go_v(&self) -> String {
        self.clone()
    }
}

impl GoV for &str {
    fn go_v(&self) -> String {
        (*self).to_owned()
    }
}

macro_rules! go_v_display {
    ($($t:ty),*) => {$(
        impl GoV for $t {
            fn go_v(&self) -> String {
                self.to_string()
            }
        }
    )*};
}
go_v_display!(i64, i32, u64, usize, f64, bool);

/// Go `%v` formatting of `value`.
pub fn go_v<T: GoV + ?Sized>(value: &T) -> String {
    value.go_v()
}

/// Go `for _, row := range res.Rows()`.
impl IntoIterator for TkResult {
    type Item = Vec<String>;
    type IntoIter = std::vec::IntoIter<Vec<String>>;
    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

/// Go `require.Equal(t, a.Rows(), b.Rows())`: the rows, not the SQL.
impl PartialEq for TkResult {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
    }
}

/// Go `expression.Args2Expressions4Test`'s value kinds: a Go value as the
/// binary-protocol parameter `types.NewDatum` makes of it.
pub trait GoArg {
    fn go_arg(self) -> tidb_param::PreparedValue;
}

impl GoArg for i32 {
    fn go_arg(self) -> tidb_param::PreparedValue {
        tidb_param::PreparedValue::SignedLongLong(i64::from(self))
    }
}

impl GoArg for i64 {
    fn go_arg(self) -> tidb_param::PreparedValue {
        tidb_param::PreparedValue::SignedLongLong(self)
    }
}

impl GoArg for u64 {
    fn go_arg(self) -> tidb_param::PreparedValue {
        tidb_param::PreparedValue::UnsignedLongLong(self)
    }
}

impl GoArg for f64 {
    fn go_arg(self) -> tidb_param::PreparedValue {
        tidb_param::PreparedValue::Double(self)
    }
}

impl GoArg for &str {
    fn go_arg(self) -> tidb_param::PreparedValue {
        tidb_param::PreparedValue::String(self.as_bytes().to_vec())
    }
}

impl GoArg for String {
    fn go_arg(self) -> tidb_param::PreparedValue {
        tidb_param::PreparedValue::String(self.into_bytes())
    }
}

/// [`GoArg::go_arg`] as a function, the shape the ported calls use.
pub fn go_arg(value: impl GoArg) -> tidb_param::PreparedValue {
    value.go_arg()
}

/// Go `strconv.Atoi(s)`: `(value, err)`.
pub fn go_atoi(text: &str) -> (i64, Option<String>) {
    match text.parse::<i64>() {
        Ok(value) => (value, None),
        Err(error) => (0, Some(format!("strconv.Atoi: parsing {text:?}: {error}"))),
    }
}

/// Unpacks `tests/integrationtest/s.zip` into `s/` once, the way
/// `run-tests.sh` does before starting mysql-tester.
///
/// Concurrency is the reason this is not a plain `unzip -o`: the full replay
/// runs topics in parallel and `replay_in_child` adds separate PROCESSES, so
/// two racers must never read each other's half-written files. Each racer
/// extracts into its own scratch directory and installs with one `rename`;
/// the loser of the rename finds `s/` present and discards its copy.
pub fn ensure_stats_fixtures_unzipped(dir: &std::path::Path) {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let target = dir.join("s");
        if target.exists() {
            return;
        }
        let scratch = dir.join(format!(".s_unzip_{}", std::process::id()));
        let unzipped = std::process::Command::new("unzip")
            .arg("-qq")
            .arg(dir.join("s.zip"))
            .arg("-d")
            .arg(&scratch)
            .status();
        match unzipped {
            Ok(status) if status.success() => {
                // A losing rename means another process installed `s/` while
                // we extracted; its copy is identical, so ours just goes.
                if std::fs::rename(scratch.join("s"), &target).is_err() && !target.exists() {
                    eprintln!("could not install {}", target.display());
                }
                let _ = std::fs::remove_dir_all(&scratch);
            }
            outcome => {
                // Leave the statement to fail on the missing file, which the
                // report then counts and names instead of hiding.
                eprintln!("unzip s.zip failed: {outcome:?}");
                let _ = std::fs::remove_dir_all(&scratch);
            }
        }
    });
}

/// A stats dump from `tests/integrationtest/s/`, unpacked from `s.zip` first
/// (the directory is git-ignored, so a fresh worktree has only the zip).
pub fn integrationtest_stats_fixture(name: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../tests/integrationtest");
    ensure_stats_fixtures_unzipped(&dir);
    dir.join("s").join(name)
}
