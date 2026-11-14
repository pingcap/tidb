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

//! Go's `LOCK TABLES` (enabled by `enable-table-lock`): the session's lock
//! set (`session.go` `AddTableLock` and its siblings), the `LOCK`/`UNLOCK
//! TABLES` executors (`executor/ddl.go`,
//! `ddl/executor.go`) and the edits their jobs make to `TableInfo.Lock`
//! (`ddl/table_lock.go`). `lock.Checker` (`pkg/lock`) is
//! `tidb_lock`.
//!
//! This tier applies a lock job's steps in one catalog mutation, so a lock
//! goes from absent straight to Go's final `public` state; the `pre-lock`
//! state between Go's two job steps is never observable here.

use std::collections::HashSet;

use tidb_ast::TableLockType;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_config::config_tree::config::table_lock_enabled;
use tidb_ddl_build::table_lock;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_driver_error::DriverError;
use tidb_infoschema::TableEntry;
use tidb_table::tables::TableCommon;
use tidb_model::{SessionInfo, TableLockTpInfo};
use tidb_hack::go_to_lower;

use tidb_mysql::PrivilegeType;
use tidb_executor::table_privilege::TablePrivilegeRequest;
use crate::Session;

/// Go `ddl.checkTableLocked` over an in-process table.
fn check_table_locked(
    table: &TableCommon,
    tp: TableLockType,
    info: &SessionInfo,
) -> Result<(), DriverError> {
    table_lock::check_table_locked(&go_to_lower(&table.name()), table.lock().as_ref(), tp, info)
        .map_err(DriverError::from)
}

impl Session {
    /// Go `session.AddTableLock`.
    pub(crate) fn add_table_lock(&mut self, locks: &[TableLockTpInfo]) {
        for lock in locks {
            // A READ ONLY lock is session-unrelated.
            if lock.tp != TableLockType::READ_ONLY {
                self.locked_tables.insert(lock.table_id, *lock);
            }
        }
    }

    /// Go `session.ReleaseTableLocks`.
    pub(crate) fn release_table_locks(&mut self, locks: &[TableLockTpInfo]) {
        for lock in locks {
            self.locked_tables.remove(&lock.table_id);
        }
    }

    /// Go `session.ReleaseTableLockByTableIDs`.
    pub(crate) fn release_table_lock_by_table_ids(&mut self, table_ids: &[i64]) {
        for table_id in table_ids {
            self.locked_tables.remove(table_id);
        }
    }

    /// Go `session.CheckTableLocked`.
    pub(crate) fn check_table_locked(&self, table_id: i64) -> (bool, TableLockType) {
        match self.locked_tables.get(&table_id) {
            Some(lock) => (true, lock.tp),
            None => (false, TableLockType::NONE),
        }
    }

    /// Go `session.GetAllTableLocks`.
    pub(crate) fn get_all_table_locks(&self) -> Vec<TableLockTpInfo> {
        self.locked_tables.values().copied().collect()
    }

    /// Go `session.HasLockedTables`.
    pub(crate) fn has_locked_tables(&self) -> bool {
        !self.locked_tables.is_empty()
    }

    /// Go `session.ReleaseAllTableLocks`.
    pub(crate) fn release_all_table_locks(&mut self) {
        self.locked_tables.clear();
    }

    /// Go's `SessionInfo{ServerID: e.uuid, SessionID: ConnectionID}`. The
    /// DDL uuid is the server's ID on a node (Go's domain hands the server
    /// info's ID to the DDL), and this process's own otherwise.
    fn table_lock_session_info(&self) -> SessionInfo {
        // Go's DDL takes one uuid when the process starts its domain.
        static DDL_UUID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        let server_id = match &self.server_info_syncer {
            Some(syncer) => syncer.get_local_server_info().static_info.id,
            None => DDL_UUID
                .get_or_init(tidb_domain_serverinfo::serverinfo_syncer::new_node_id)
                .clone(),
        };
        SessionInfo {
            server_id,
            session_id: self.connection_id(),
        }
    }

    /// Whether `CheckTableLock` can find anything: with the feature off, or
    /// no lock held by this session nor recorded on any table, every arm of
    /// `lock.Checker` answers nil.
    fn table_lock_check_needed(&self, catalog: &SessionExtendedInfoSchema) -> bool {
        table_lock_enabled() && (self.has_locked_tables() || catalog.has_table_locks())
    }

    /// [`Self::table_lock_check_needed`] against the statement's catalog.
    pub(crate) fn table_lock_check_pending(&self) -> Result<bool, DriverError> {
        if !table_lock_enabled() {
            return Ok(false);
        }
        if self.has_locked_tables() {
            return Ok(true);
        }
        self.with_txn_catalog(SessionExtendedInfoSchema::has_table_locks)
    }

    /// Go `core.CheckTableLock` over one statement's visit infos, after
    /// `CheckPrivilege` (`planner/optimize.go`).
    pub(crate) fn check_table_lock(
        &self,
        requests: &[TablePrivilegeRequest],
    ) -> Result<(), DriverError> {
        if !table_lock_enabled() {
            return Ok(());
        }
        let shared = self.lock_catalog()?;
        let catalog = self.txn_ctx_stale_catalog().unwrap_or(&*shared);
        if !self.table_lock_check_needed(catalog) {
            return Ok(());
        }
        let visits: Vec<(&str, &str, PrivilegeType, bool)> = requests
            .iter()
            .filter(|request| request.dynamic_privileges.is_empty())
            .map(|request| {
                (
                    request.database.as_str(),
                    request.table.as_str(),
                    request.privilege,
                    request.alter_writable,
                )
            })
            .collect();
        tidb_lock::check_visits(
            &tidb_lock::Checker::new(&self.locked_tables, catalog),
            &visits,
        )
    }

    /// Go `TableLockReadContext`: the table locks this session holds, handed
    /// to the executor (`StmtContext::locked_tables`); `None` with the
    /// feature off.
    pub(crate) fn statement_locked_tables(&self) -> Option<tidb_sessionctx::LockedTables> {
        if !table_lock_enabled() {
            return None;
        }
        Some(std::sync::Arc::new(self.locked_tables.clone()))
    }

    /// Go `DDLExec.executeLockTables`' local-temporary refusal and
    /// `executor.LockTables`' checks before the job: the session's identity,
    /// the `(SchemaID, TableID, Tp)` of each table, and the first table's
    /// schema.
    fn lock_tables_args(
        &self,
        locks: &[tidb_ast::TableLock],
    ) -> Result<(SessionInfo, Vec<TableLockTpInfo>, String), DriverError> {
        let names: Vec<(String, String)> = locks
            .iter()
            .map(|lock| self.table_lock_name(&lock.table))
            .collect::<Result<_, _>>()?;
        for (db, table) in &names {
            if self.is_local_temporary_table(db, table) {
                return Err(DriverError::from(
                    tidb_dbterror::ERR_UNSUPPORTED_LOCAL_TEMP_TABLE_DDL
                        .gen_with_stack_by_args(&["LOCK TABLES".into()]),
                ));
            }
        }
        let login = self
            .vars.login_user
            .as_deref()
            .and_then(|login| login.split_once('@'))
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        let info = self.table_lock_session_info();
        let catalog = self.lock_catalog()?;
        let mut lock_tables = Vec::with_capacity(locks.len());
        let mut unique_table_id = HashSet::new();
        for (lock, (db, name)) in locks.iter().zip(&names) {
            table_lock::throw_err_if_in_mem_or_sys_db(
                login.as_ref().map(|(user, host)| (user.as_str(), host.as_str())),
                db,
            )
            .map_err(DriverError::Mysql)?;
            let schema_id = catalog.database_id(db).ok_or_else(|| {
                DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                        .gen_with_stack_by_args(&[(db.clone()).into()]),
                )
            })?;
            let table = match catalog.table_in(db, name) {
                Some(TableEntry::Kv(table)) => table,
                Some(_) => {
                    return Err(DriverError::from(
                        tidb_table::table_errors::ERR_UNSUPPORTED_OP
                            .gen_with_stack_by_args(&[]),
                    ))
                }
                None => {
                    return Err(DriverError::from(
                        tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                            .gen_with_stack_by_args(&[(&db).into(), (&name).into()]),
                    ))
                }
            };
            check_table_locked(table, lock.lock_type, &info)?;
            if !unique_table_id.insert(table.table_id()) {
                return Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_NONUNIQ_TABLE
                        .gen_with_stack_by_args(&[table.name().into()]),
                ));
            }
            lock_tables.push(TableLockTpInfo {
                schema_id,
                table_id: table.table_id(),
                tp: lock.lock_type,
            });
        }
        let first_schema = names.first().map(|(db, _)| db.clone()).unwrap_or_default();
        Ok((info, lock_tables, first_schema))
    }

    /// Go `executor.LockTables`: validate names and build its typed job.
    pub fn cluster_lock_tables_job(&mut self, locks: &[tidb_ast::TableLock], sql: &str)
        -> Result<tidb_ddl::JobWrapper, DriverError> {
        let unlock_tables = self.get_all_table_locks();
        let (session_info, lock_tables, _) = self.lock_tables_args(locks)?;
        let context = self.ddl_statement_context().with_ddl_query(sql);
        let catalog = self.lock_catalog()?;
        tidb_ddl::cluster_ddl::table_lock::lock_tables_job(&catalog,
            tidb_model::LockTablesArgs { lock_tables, unlock_tables, session_info, ..Default::default() },
            &context)
            .map_err(|e| DriverError::Mysql(tidb_terror::MysqlError::from_sql_error(e.to_sql_error())))
    }

    /// Go `executor.UnlockTables`; no job when the session holds no locks.
    pub fn cluster_unlock_tables_job(&self) -> Result<Option<tidb_ddl::JobWrapper>, DriverError> {
        let unlock_tables = self.get_all_table_locks();
        if unlock_tables.is_empty() { return Ok(None); }
        let context = self.ddl_statement_context();
        let catalog = self.lock_catalog()?;
        tidb_ddl::cluster_ddl::table_lock::unlock_tables_job(&catalog,
            tidb_model::LockTablesArgs { unlock_tables, session_info: self.table_lock_session_info(),
                ..Default::default() }, &context)
            .map(Some).map_err(|e| DriverError::Mysql(tidb_terror::MysqlError::from_sql_error(e.to_sql_error())))
    }

    /// Go `executor.CleanupTableLock`; no job when all named tables are unlocked.
    pub fn cluster_cleanup_table_lock_job(&self, tables: &[Vec<String>], sql: &str)
        -> Result<Option<tidb_ddl::JobWrapper>, DriverError> {
        let Some(unlock_tables) = self.cleanup_table_lock_args(tables)? else { return Ok(None); };
        let context = self.ddl_statement_context().with_ddl_query(sql);
        let catalog = self.lock_catalog()?;
        tidb_ddl::cluster_ddl::table_lock::unlock_tables_job(&catalog,
            tidb_model::LockTablesArgs { unlock_tables, is_cleanup: true, ..Default::default() },
            &context)
            .map(Some).map_err(|e| DriverError::Mysql(tidb_terror::MysqlError::from_sql_error(e.to_sql_error())))
    }

    /// Go `executor.LockTables` before `doDDLJob2` protects locks if the session is killed.
    pub(crate) fn begin_lock_tables_args(&mut self, args: &tidb_model::LockTablesArgs) {
        self.add_table_lock(&args.lock_tables);
    }

    /// Go `executor.LockTables`, `UnlockTables`, `CleanupTableLock` after success.
    pub(crate) fn finish_lock_tables_args(&mut self, args: &tidb_model::LockTablesArgs) {
        if !args.lock_tables.is_empty() {
            self.release_table_locks(&args.unlock_tables);
            self.add_table_lock(&args.lock_tables);
        } else if args.is_cleanup {
            self.release_table_locks(&args.unlock_tables);
        } else {
            self.release_all_table_locks();
        }
    }





    /// Go `dropTableObject`, `DropSchema`, and `HandleLockTablesOnFinish`:
    /// capture the affected locks before the executor publishes new metadata.
    pub(crate) fn cluster_executor_lock_effect(&self, statement: &tidb_ast::Stmt) -> ClusterDdlLockEffect {
        if !table_lock_enabled() || !self.has_locked_tables() { return ClusterDdlLockEffect::None; }
        let tidb_ast::Stmt::Ddl(ddl) = statement else { return ClusterDdlLockEffect::None; };
        let Ok(catalog) = self.lock_catalog() else { return ClusterDdlLockEffect::None; };
        let held = |schema: &str, table: &str| catalog.stored_table_id(schema, table)
            .filter(|id| self.check_table_locked(*id).0);
        match &**ddl {
            tidb_ast::DdlStmt::TruncateTable(path) => {
                let Ok((schema, table)) = tidb_infoschema::split_table_path_pub(path, &self.vars.current_db) else {
                    return ClusterDdlLockEffect::None;
                };
                match (held(schema, table), catalog.database_id(schema)) {
                    (Some(old_id), Some(schema_id)) => ClusterDdlLockEffect::Move { schema_id, old_id },
                    _ => ClusterDdlLockEffect::None,
                }
            }
            tidb_ast::DdlStmt::DropTable(drop) => ClusterDdlLockEffect::Release(drop.names.iter()
                .filter_map(|path| tidb_infoschema::split_table_path_pub(path, &self.vars.current_db).ok())
                .filter_map(|(schema, table)| held(schema, table)).collect()),
            tidb_ast::DdlStmt::DropDatabase { name, .. } => ClusterDdlLockEffect::Release(
                catalog.table_names(name).unwrap_or_default().iter().filter_map(|table| held(name, table)).collect()),
            _ => ClusterDdlLockEffect::None,
        }
    }

    /// Applies [`Self::cluster_ddl_lock_effect`] after the DDL succeeded;
    /// `created_id` is the truncated table's new ID.
    pub fn finish_cluster_ddl_lock_effect(
        &mut self,
        effect: ClusterDdlLockEffect,
        created_id: Option<i64>,
    ) {
        match effect {
            ClusterDdlLockEffect::None => {}
            ClusterDdlLockEffect::Release(table_ids) => {
                self.release_table_lock_by_table_ids(&table_ids);
            }
            ClusterDdlLockEffect::Move { schema_id, old_id } => {
                if let Some(new_id) = created_id {
                    self.move_truncated_table_lock(schema_id, old_id, new_id);
                }
            }
        }
    }


    /// Go `DDLExec.executeCleanupTableLock`'s local-temporary refusal and
    /// `executor.CleanupTableLock`'s checks: the tables to clean, `None`
    /// when none is locked (Go submits no job).
    fn cleanup_table_lock_args(
        &self,
        tables: &[Vec<String>],
    ) -> Result<Option<Vec<TableLockTpInfo>>, DriverError> {
        let names: Vec<(String, String)> = tables
            .iter()
            .map(|path| self.table_lock_name(path))
            .collect::<Result<_, _>>()?;
        for (db, table) in &names {
            if self.is_local_temporary_table(db, table) {
                return Err(DriverError::from(
                    tidb_dbterror::ERR_UNSUPPORTED_LOCAL_TEMP_TABLE_DDL
                        .gen_with_stack_by_args(&["ADMIN CLEANUP TABLE LOCK".into()]),
                ));
            }
        }
        let login = self
            .vars.login_user
            .as_deref()
            .and_then(|login| login.split_once('@'))
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        let catalog = self.lock_catalog()?;
        let mut cleanup = Vec::with_capacity(names.len());
        let mut unique_table_id = HashSet::new();
        let mut unlocked = 0;
        for (db, name) in &names {
            table_lock::throw_err_if_in_mem_or_sys_db(
                login.as_ref().map(|(user, host)| (user.as_str(), host.as_str())),
                db,
            )
            .map_err(DriverError::Mysql)?;
            let schema_id = catalog.database_id(db).ok_or_else(|| {
                DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                        .gen_with_stack_by_args(&[(db.clone()).into()]),
                )
            })?;
            let table = match catalog.table_in(db, name) {
                Some(TableEntry::Kv(table)) => table,
                Some(_) => {
                    return Err(DriverError::from(
                        tidb_table::table_errors::ERR_UNSUPPORTED_OP
                            .gen_with_stack_by_args(&[]),
                    ))
                }
                None => {
                    return Err(DriverError::from(
                        tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                            .gen_with_stack_by_args(&[(&db).into(), (&name).into()]),
                    ))
                }
            };
            if !table.is_locked() {
                unlocked += 1;
            }
            if !unique_table_id.insert(table.table_id()) {
                return Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_NONUNIQ_TABLE
                        .gen_with_stack_by_args(&[table.name().into()]),
                ));
            }
            cleanup.push(TableLockTpInfo {
                schema_id,
                table_id: table.table_id(),
                tp: TableLockType::NONE,
            });
        }
        if cleanup.is_empty() || cleanup.len() == unlocked {
            return Ok(None);
        }
        Ok(Some(cleanup))
    }


    /// Go `HandleLockTablesOnSuccessSubmit`/`HandleLockTablesOnFinish` for a
    /// successful `TRUNCATE TABLE`: the session's lock moves to the new
    /// table ID.
    pub(crate) fn move_truncated_table_lock(&mut self, schema_id: i64, old_id: i64, new_id: i64) {
        if let (true, tp) = self.check_table_locked(old_id) {
            self.add_table_lock(&[TableLockTpInfo {
                schema_id,
                table_id: new_id,
                tp,
            }]);
            self.release_table_lock_by_table_ids(&[old_id]);
        }
    }

    /// A `LOCK TABLES` name, its schema defaulted to the current database as
    /// Go's preprocessor does.
    fn table_lock_name(&self, path: &[String]) -> Result<(String, String), DriverError> {
        match path {
            [_] if self.vars.current_db.is_empty() => Err(DriverError::from(
                tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
            )),
            [table] => Ok((self.vars.current_db.clone(), table.clone())),
            [db, table] => Ok((db.clone(), table.clone())),
            _ => Err(DriverError::unsupported(
                "a LOCK TABLES name has one or two parts",
            )),
        }
    }
}

/// What a cluster DDL does to the session's table locks once it succeeds.
#[derive(Debug)]
pub enum ClusterDdlLockEffect {
    /// Nothing the session holds is involved.
    None,
    /// The dropped tables the session held locks on.
    Release(Vec<i64>),
    /// The truncated table's lock moves to its new ID.
    Move {
        /// The table's schema ID.
        schema_id: i64,
        /// The table ID before the truncate.
        old_id: i64,
    },
}
