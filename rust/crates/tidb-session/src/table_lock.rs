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
//! set (`session.go` `AddTableLock` and its siblings), `lock.Checker`
//! (`pkg/lock`), the `LOCK`/`UNLOCK TABLES` executors (`executor/ddl.go`,
//! `ddl/executor.go`) and the edits their jobs make to `TableInfo.Lock`
//! (`ddl/table_lock.go`).
//!
//! This tier applies a lock job's steps in one catalog mutation, so a lock
//! goes from absent straight to Go's final `public` state; the `pre-lock`
//! state between Go's two job steps is never observable here.

use std::collections::{BTreeMap, HashSet};

use tidb_ast::TableLockType;
use tidb_config::config_tree::config::table_lock_enabled;
use tidb_executor::ddl::table_lock;
use tidb_executor::{Catalog, DriverError, KvTable, SchemaErrorKind, TableEntry};
use tidb_model::{SessionInfo, TableLockTpInfo};
use tidb_util::stringutil::go_to_lower;

use crate::privilege::GlobalPriv;
use crate::table_privilege::TablePrivilegeRequest;
use crate::{Session, StmtOutput};

/// Go `ddl.checkTableLocked` over an in-process table.
fn check_table_locked(
    table: &KvTable,
    tp: TableLockType,
    info: &SessionInfo,
) -> Result<(), DriverError> {
    table_lock::check_table_locked(&go_to_lower(&table.name), table.lock(), tp, info)
}

/// Go `ddl.unlockTable` over an in-process table.
fn unlock_table(table: &mut KvTable, info: &SessionInfo, is_cleanup: bool) {
    if let Some(next) = table_lock::unlock_table(table.lock(), info, is_cleanup) {
        table.set_lock(next);
    }
}

/// Go `ddl.unlockTables` over the job's `UnlockTables`: a table dropped since
/// it was locked is skipped.
fn unlock_tables(
    catalog: &mut Catalog,
    unlock: &[TableLockTpInfo],
    info: &SessionInfo,
    is_cleanup: bool,
) {
    for entry in unlock {
        let Some((schema, name)) = catalog.table_names_by_id(entry.table_id) else {
            continue;
        };
        if let Some(TableEntry::Kv(table)) = catalog.table_mut_in(&schema, &name) {
            unlock_table(std::sync::Arc::make_mut(table), info, is_cleanup);
        }
    }
}

/// Go `lock.ErrLockedTableDropped`: the session dropped a table it holds a
/// WRITE lock on, after which `CheckTableLock` lets the statement's other
/// tables through (a `RENAME`).
enum LockCheckError {
    LockedTableDropped,
    Error(DriverError),
}

impl From<DriverError> for LockCheckError {
    fn from(error: DriverError) -> Self {
        Self::Error(error)
    }
}

/// Go `lock.Checker`: one statement's table accesses against the locks.
struct Checker<'a> {
    /// Go `TableLockReadContext`: the session's own lock set.
    locked_tables: &'a BTreeMap<i64, TableLockTpInfo>,
    catalog: &'a Catalog,
}

impl Checker<'_> {
    fn has_locked_tables(&self) -> bool {
        !self.locked_tables.is_empty()
    }

    /// Go `Checker.CheckTableLock`.
    fn check_table_lock(
        &self,
        db: &str,
        table: &str,
        privilege: GlobalPriv,
        alter_writable: bool,
    ) -> Result<(), LockCheckError> {
        if (db.is_empty() && table.is_empty()) || privilege == GlobalPriv::LockTables {
            return Ok(());
        }
        // System and memory schemas take no table lock.
        if tidb_metadef::is_mem_or_sys_db(&go_to_lower(db)) {
            return Ok(());
        }
        if !alter_writable && table.is_empty() {
            return self.check_lock_in_db(db, privilege);
        }
        match privilege {
            GlobalPriv::ShowDatabases => return Ok(()),
            GlobalPriv::Create | GlobalPriv::CreateView => {
                if self.has_locked_tables() {
                    return Err(DriverError::TableNotLocked(table.to_owned()).into());
                }
                return Ok(());
            }
            _ => {}
        }
        // An absent table is not an error here ("drop table if exists").
        let Some(TableEntry::Kv(tb)) = self.catalog.table_in(db, table) else {
            return Ok(());
        };
        let Some(lock) = tb.lock() else {
            return Ok(());
        };
        if privilege == GlobalPriv::Drop && tb.name == table && self.has_locked_tables() {
            if self.locked_tables.contains_key(&tb.table_id) {
                if lock.tp == TableLockType::WRITE {
                    return Err(LockCheckError::LockedTableDropped);
                }
                if matches!(
                    lock.tp,
                    TableLockType::READ | TableLockType::WRITE_LOCAL | TableLockType::READ_ONLY
                ) {
                    return Err(DriverError::TableNotLockedForWrite(tb.name.clone()).into());
                }
            }
        }
        if !alter_writable && self.has_locked_tables() {
            if let Some(held) = self.locked_tables.get(&tb.table_id) {
                if check_lock_tp_meet_privilege(held.tp, privilege) {
                    return Ok(());
                }
                return Err(DriverError::TableNotLockedForWrite(tb.name.clone()).into());
            }
            return Err(DriverError::TableNotLocked(tb.name.clone()).into());
        }
        if privilege == GlobalPriv::Select
            && matches!(
                lock.tp,
                TableLockType::READ | TableLockType::WRITE_LOCAL | TableLockType::READ_ONLY
            )
        {
            return Ok(());
        }
        if alter_writable && lock.tp == TableLockType::READ_ONLY {
            return Ok(());
        }
        Err(table_lock::table_locked_error(&go_to_lower(&tb.name), lock).into())
    }

    /// Go `Checker.CheckLockInDB`.
    fn check_lock_in_db(&self, db: &str, privilege: GlobalPriv) -> Result<(), LockCheckError> {
        if self.has_locked_tables()
            && matches!(
                privilege,
                GlobalPriv::Create | GlobalPriv::Drop | GlobalPriv::Alter
            )
        {
            return Err(DriverError::LockOrActiveTransaction.into());
        }
        if privilege == GlobalPriv::Create {
            return Ok(());
        }
        // Go checks every locked table's NAME in `db`, whichever schema the
        // locked table itself lives in.
        for (_, table) in self.catalog.locked_tables() {
            self.check_table_lock(db, &go_to_lower(&table.name), privilege, false)?;
        }
        Ok(())
    }
}

/// Go `lock.checkLockTpMeetPrivilege`.
fn check_lock_tp_meet_privilege(tp: TableLockType, privilege: GlobalPriv) -> bool {
    // READ ONLY is session-unrelated and never reaches here.
    match tp {
        TableLockType::WRITE | TableLockType::WRITE_LOCAL => true,
        // ShowDB, Create and CreateView were answered before; a READ lock
        // admits nothing else but SELECT.
        TableLockType::READ => privilege == GlobalPriv::Select,
        _ => false,
    }
}

/// Go `core.CheckTableLock`'s loop over the visit infos.
fn check_visits(
    checker: &Checker<'_>,
    visits: &[(&str, &str, GlobalPriv)],
) -> Result<(), DriverError> {
    for (db, table, privilege) in visits {
        // `alterWritable` is set only by `ALTER TABLE ... READ ONLY`'s
        // writeable spec, which this tier does not execute.
        match checker.check_table_lock(db, table, *privilege, false) {
            Ok(()) => {}
            // With the session's WRITE-locked table dropped, its other
            // tables may be accessed (`rename`).
            Err(LockCheckError::LockedTableDropped) => break,
            Err(LockCheckError::Error(error)) => return Err(error),
        }
    }
    Ok(())
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
            Some(syncer) => syncer.local_server_info().static_info.id,
            None => DDL_UUID
                .get_or_init(tidb_domain::serverinfo_syncer::new_node_id)
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
    fn table_lock_check_needed(&self, catalog: &Catalog) -> bool {
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
        Ok(match &self.txn {
            Some(txn) => txn.working.has_table_locks(),
            None => self.lock_catalog()?.has_table_locks(),
        })
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
        let shared;
        let catalog = match &self.txn {
            Some(txn) => &txn.working,
            None => {
                shared = self.lock_catalog()?;
                &*shared
            }
        };
        if !self.table_lock_check_needed(catalog) {
            return Ok(());
        }
        let visits: Vec<(&str, &str, GlobalPriv)> = requests
            .iter()
            .filter(|request| request.dynamic_privileges.is_empty())
            .map(|request| {
                (
                    request.database.as_str(),
                    request.table.as_str(),
                    request.privilege,
                )
            })
            .collect();
        check_visits(
            &Checker {
                locked_tables: &self.locked_tables,
                catalog,
            },
            &visits,
        )
    }

    /// The foreign-key cascade's `CheckTableLock`, handed to the executor
    /// (`StmtContext::fk_cascade_table_lock`); `None` with the feature off.
    pub(crate) fn fk_cascade_table_lock_check(
        &self,
    ) -> Option<tidb_executor::ForeignKeyCascadeLockCheck> {
        if !table_lock_enabled() {
            return None;
        }
        let locked_tables = self.locked_tables.clone();
        Some(std::sync::Arc::new(move |catalog, db, table, delete| {
            if locked_tables.is_empty() && !catalog.has_table_locks() {
                return Ok(());
            }
            // The cascade's `DELETE`/`UPDATE` reads the child table and then
            // writes it, in `buildDelete`/`buildUpdate`'s visit order.
            let write = if delete {
                GlobalPriv::Delete
            } else {
                GlobalPriv::Update
            };
            check_visits(
                &Checker {
                    locked_tables: &locked_tables,
                    catalog,
                },
                &[(db, table, GlobalPriv::Select), (db, table, write)],
            )
        }))
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
                return Err(DriverError::UnsupportedLocalTempTableDDL("LOCK TABLES"));
            }
        }
        let login = self
            .login_user
            .as_deref()
            .and_then(|login| login.split_once('@'))
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        let info = self.table_lock_session_info();
        let catalog = self.lock_catalog()?;
        let mut lock_tables = Vec::with_capacity(locks.len());
        let mut unique_table_id = HashSet::new();
        for (lock, (db, name)) in locks.iter().zip(&names) {
            throw_err_if_in_mem_or_sys_db(login.as_ref(), db)?;
            let schema_id = catalog
                .database_id(db)
                .ok_or_else(|| DriverError::Schema(SchemaErrorKind::UnknownDatabase(db.clone())))?;
            let table = match catalog.table_in(db, name) {
                Some(TableEntry::Kv(table)) => table,
                Some(_) => return Err(DriverError::UnsupportedOp),
                None => {
                    return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                        "{db}.{name}"
                    ))))
                }
            };
            check_table_locked(table, lock.lock_type, &info)?;
            if !unique_table_id.insert(table.table_id) {
                return Err(DriverError::NonuniqTable(table.name.clone()));
            }
            lock_tables.push(TableLockTpInfo {
                schema_id,
                table_id: table.table_id,
                tp: lock.lock_type,
            });
        }
        let first_schema = names.first().map(|(db, _)| db.clone()).unwrap_or_default();
        Ok((info, lock_tables, first_schema))
    }

    /// The cluster lock job for `LOCK TABLES`: `executor.LockTables`' checks
    /// against this node's catalog, then Go's `LockTablesArgs` with the
    /// session's current locks to release
    /// ([`Self::begin_cluster_lock_job`] adds the new ones).
    pub fn cluster_lock_tables_statement(
        &mut self,
        locks: &[tidb_ast::TableLock],
    ) -> Result<tidb_exec::cluster_ddl::DdlStatement, DriverError> {
        let unlock_tables = self.get_all_table_locks();
        let (session, lock_tables, _) = self.lock_tables_args(locks)?;
        Ok(tidb_exec::cluster_ddl::DdlStatement::LockTables {
            lock_tables,
            unlock_tables,
            session,
        })
    }

    /// The cluster lock job that releases every lock this session holds
    /// (`UNLOCK TABLES`, and `session.Close`); `None` with none held, where Go's
    /// `UnlockTables` submits nothing.
    pub fn cluster_unlock_tables_statement(&self) -> Option<tidb_exec::cluster_ddl::DdlStatement> {
        let unlock_tables = self.get_all_table_locks();
        if unlock_tables.is_empty() {
            return None;
        }
        Some(tidb_exec::cluster_ddl::DdlStatement::UnlockTables {
            unlock_tables,
            session: self.table_lock_session_info(),
            is_cleanup: false,
        })
    }

    /// Go `executor.LockTables`' `AddTableLock` before the job, once the
    /// statement passed its privilege check.
    pub fn begin_cluster_lock_job(&mut self, statement: &tidb_exec::cluster_ddl::DdlStatement) {
        if let tidb_exec::cluster_ddl::DdlStatement::LockTables { lock_tables, .. } = statement {
            self.add_table_lock(lock_tables);
        }
    }

    /// What a cluster DROP TABLE, DROP DATABASE or TRUNCATE TABLE does to
    /// this session's locks once it succeeds, read from this node's catalog
    /// before it runs (Go `dropTableObject`, `DropSchema` and
    /// `HandleLockTablesOnFinish`).
    pub fn cluster_ddl_lock_effect(
        &self,
        statement: &tidb_exec::cluster_ddl::DdlStatement,
    ) -> ClusterDdlLockEffect {
        use tidb_exec::cluster_ddl::DdlStatement;
        if !table_lock_enabled() || !self.has_locked_tables() {
            return ClusterDdlLockEffect::None;
        }
        let Ok(catalog) = self.lock_catalog() else {
            return ClusterDdlLockEffect::None;
        };
        let held = |schema: &str, table: &str| {
            catalog
                .stored_table_id(schema, table)
                .filter(|table_id| self.check_table_locked(*table_id).0)
        };
        match statement {
            DdlStatement::DropTable { schema, table, .. } => {
                ClusterDdlLockEffect::Release(held(schema, table).into_iter().collect())
            }
            DdlStatement::DropTables { names, .. } => ClusterDdlLockEffect::Release(
                names
                    .iter()
                    .filter_map(|(schema, table)| held(schema, table))
                    .collect(),
            ),
            DdlStatement::DropDatabase { name, .. } => ClusterDdlLockEffect::Release(
                catalog
                    .table_names(name)
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|table| held(name, table))
                    .collect(),
            ),
            DdlStatement::TruncateTable { schema, table } => {
                match (held(schema, table), catalog.database_id(schema)) {
                    (Some(old_id), Some(schema_id)) => {
                        ClusterDdlLockEffect::Move { schema_id, old_id }
                    }
                    _ => ClusterDdlLockEffect::None,
                }
            }
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

    /// Go `executor.LockTables`/`UnlockTables` after a successful cluster
    /// lock job: the released locks leave the session, the new ones stay.
    pub fn finish_cluster_lock_job(&mut self, statement: &tidb_exec::cluster_ddl::DdlStatement) {
        match statement {
            tidb_exec::cluster_ddl::DdlStatement::LockTables {
                lock_tables,
                unlock_tables,
                ..
            } => {
                self.release_table_locks(unlock_tables);
                self.add_table_lock(lock_tables);
            }
            tidb_exec::cluster_ddl::DdlStatement::UnlockTables {
                is_cleanup: false, ..
            } => {
                self.release_all_table_locks();
            }
            // Go `CleanupTableLock` releases only the tables it cleaned.
            tidb_exec::cluster_ddl::DdlStatement::UnlockTables { unlock_tables, .. } => {
                self.release_table_locks(unlock_tables);
            }
            _ => {}
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
                return Err(DriverError::UnsupportedLocalTempTableDDL(
                    "ADMIN CLEANUP TABLE LOCK",
                ));
            }
        }
        let login = self
            .login_user
            .as_deref()
            .and_then(|login| login.split_once('@'))
            .map(|(user, host)| (user.to_owned(), host.to_owned()));
        let catalog = self.lock_catalog()?;
        let mut cleanup = Vec::with_capacity(names.len());
        let mut unique_table_id = HashSet::new();
        let mut unlocked = 0;
        for (db, name) in &names {
            throw_err_if_in_mem_or_sys_db(login.as_ref(), db)?;
            let schema_id = catalog
                .database_id(db)
                .ok_or_else(|| DriverError::Schema(SchemaErrorKind::UnknownDatabase(db.clone())))?;
            let table = match catalog.table_in(db, name) {
                Some(TableEntry::Kv(table)) => table,
                Some(_) => return Err(DriverError::UnsupportedOp),
                None => {
                    return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                        "{db}.{name}"
                    ))))
                }
            };
            if !table.is_locked() {
                unlocked += 1;
            }
            if !unique_table_id.insert(table.table_id) {
                return Err(DriverError::NonuniqTable(table.name.clone()));
            }
            cleanup.push(TableLockTpInfo {
                schema_id,
                table_id: table.table_id,
                tp: TableLockType::NONE,
            });
        }
        if cleanup.is_empty() || cleanup.len() == unlocked {
            return Ok(None);
        }
        Ok(Some(cleanup))
    }

    /// Go `DDLExec.executeCleanupTableLock` and `executor.CleanupTableLock`,
    /// with the `IsCleanup` job `onUnlockTables` runs; the session then lets
    /// go of the cleaned tables.
    pub(crate) fn execute_cleanup_table_lock(
        &mut self,
        tables: &[Vec<String>],
    ) -> Result<StmtOutput, DriverError> {
        let Some(cleanup) = self.cleanup_table_lock_args(tables)? else {
            return Ok(StmtOutput::Done(false));
        };
        let context = self.ddl_statement_context();
        let info = self.table_lock_session_info();
        self.with_persistent_catalog_mut(|catalog| {
            let schema = catalog
                .table_names_by_id(cleanup[0].table_id)
                .map(|(schema, _)| schema)
                .unwrap_or_default();
            tidb_executor::ddl::submit_ddl_job(
                catalog,
                &context,
                &schema,
                "",
                tidb_model::ActionType::ACTION_UNLOCK_TABLE,
            )?;
            unlock_tables(catalog, &cleanup, &info, true);
            Ok(())
        })?;
        self.release_table_locks(&cleanup);
        Ok(StmtOutput::Done(false))
    }

    /// The cluster `IsCleanup` unlock job for `ADMIN CLEANUP TABLE LOCK`,
    /// `None` when no named table is locked.
    pub fn cluster_cleanup_table_lock_statement(
        &self,
        tables: &[Vec<String>],
    ) -> Result<Option<tidb_exec::cluster_ddl::DdlStatement>, DriverError> {
        Ok(self.cleanup_table_lock_args(tables)?.map(|unlock_tables| {
            tidb_exec::cluster_ddl::DdlStatement::UnlockTables {
                unlock_tables,
                session: self.table_lock_session_info(),
                is_cleanup: true,
            }
        }))
    }

    /// Go `executor.UnlockTables`.
    fn unlock_tables(&mut self, unlock: Vec<TableLockTpInfo>) -> Result<(), DriverError> {
        if unlock.is_empty() {
            return Ok(());
        }
        let context = self.ddl_statement_context();
        let info = self.table_lock_session_info();
        self.with_persistent_catalog_mut(|catalog| {
            let schema = catalog
                .table_names_by_id(unlock[0].table_id)
                .map(|(schema, _)| schema)
                .unwrap_or_default();
            tidb_executor::ddl::submit_ddl_job(
                catalog,
                &context,
                &schema,
                "",
                tidb_model::ActionType::ACTION_UNLOCK_TABLE,
            )?;
            unlock_tables(catalog, &unlock, &info, false);
            Ok(())
        })?;
        self.release_all_table_locks();
        Ok(())
    }

    /// Go `session.Close`: a session's table locks are released when it ends.
    pub(crate) fn release_table_locks_on_close(&mut self) {
        if self.has_locked_tables() && table_lock_enabled() {
            // Go logs and moves on when the release fails.
            let _ = self.unlock_tables(self.get_all_table_locks());
        }
    }

    /// Go's DDL executors release the session's lock on a table they drop
    /// (`dropTableObject`) or on every table of a dropped schema
    /// (`DropSchema`).
    pub(crate) fn release_dropped_table_locks(&mut self, table_ids: &[i64]) {
        if !table_lock_enabled() {
            return;
        }
        let held: Vec<i64> = table_ids
            .iter()
            .copied()
            .filter(|table_id| self.check_table_locked(*table_id).0)
            .collect();
        self.release_table_lock_by_table_ids(&held);
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
            [_] if self.current_db.is_empty() => {
                Err(DriverError::Schema(SchemaErrorKind::NoDatabaseSelected))
            }
            [table] => Ok((self.current_db.clone(), table.clone())),
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

/// Go `ddl.throwErrIfInMemOrSysDB`.
fn throw_err_if_in_mem_or_sys_db(
    login: Option<&(String, String)>,
    db: &str,
) -> Result<(), DriverError> {
    if !tidb_metadef::is_mem_or_sys_db(&go_to_lower(db)) {
        return Ok(());
    }
    let (user, host) = login.map_or(("", ""), |(user, host)| (user.as_str(), host.as_str()));
    // Go passes two of the message's three verbs.
    Err(DriverError::Mysql(tidb_executor::MysqlError::terror(
        &tidb_error::catalog::privileges::ERR_ACCESS_DENIED,
        format!("Access denied for user '{user}'@'{host}' (using password: %!s(MISSING))"),
    )))
}
