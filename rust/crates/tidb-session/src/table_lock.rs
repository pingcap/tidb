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

use tidb_ast::TableLockType;
use tidb_config::config_tree::config::table_lock_enabled;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_driver_error::DriverError;
use tidb_model::TableLockTpInfo;

use tidb_mysql::PrivilegeType;
use tidb_executor::table_privilege::TablePrivilegeRequest;
use crate::Session;

impl Session {
    /// Go `session.AddTableLock`.
    pub(crate) fn add_table_lock(&mut self, locks: &[TableLockTpInfo]) {
        self.locked_tables.add_table_lock(locks);
    }

    /// Go `session.ReleaseTableLockByTableIDs`.
    pub(crate) fn release_table_lock_by_table_ids(&mut self, table_ids: &[i64]) {
        self.locked_tables.release_table_lock_by_table_ids(table_ids);
    }

    /// Go `session.CheckTableLocked`.
    pub(crate) fn check_table_locked(&self, table_id: i64) -> (bool, TableLockType) {
        self.locked_tables.check_table_locked(table_id)
    }

    /// Go `session.GetAllTableLocks`.
    pub(crate) fn get_all_table_locks(&self) -> Vec<TableLockTpInfo> {
        self.locked_tables.get_all_table_locks()
    }

    /// Go `session.HasLockedTables`.
    pub(crate) fn has_locked_tables(&self) -> bool {
        self.locked_tables.has_locked_tables()
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
        let held = self.locked_tables.snapshot();
        tidb_lock::check_visits(&tidb_lock::Checker::new(&held, catalog), &visits)
    }

    /// Go `TableLockReadContext`: the table locks this session holds, handed
    /// to the executor (`StmtContext::locked_tables`); `None` with the
    /// feature off.
    pub(crate) fn statement_locked_tables(&self) -> Option<tidb_sessionctx::LockedTables> {
        if !table_lock_enabled() {
            return None;
        }
        Some(std::sync::Arc::new(self.locked_tables.snapshot()))
    }

    /// Go `DDLExec.executeLockTables` / `executeCleanupTableLock`: a LOCAL
    /// temporary table among the names is refused before the DDL executor
    /// runs (`statement` names the statement in the error).
    pub fn refuse_local_temporary_lock_targets(
        &self,
        tables: &[Vec<String>],
        statement: &str,
    ) -> Result<(), DriverError> {
        for path in tables {
            let (db, table) = self.table_lock_name(path)?;
            if self.is_local_temporary_table(&db, &table) {
                return Err(DriverError::from(
                    tidb_dbterror::ERR_UNSUPPORTED_LOCAL_TEMP_TABLE_DDL
                        .gen_with_stack_by_args(&[statement.into()]),
                ));
            }
        }
        Ok(())
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
