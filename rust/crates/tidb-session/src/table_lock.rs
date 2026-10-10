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
use tidb_executor::{Catalog, DriverError, KvTable, SchemaErrorKind, TableEntry};
use tidb_model::{SessionInfo, TableLockInfo, TableLockState, TableLockTpInfo};
use tidb_util::stringutil::go_to_lower;

use crate::privilege::GlobalPriv;
use crate::table_privilege::TablePrivilegeRequest;
use crate::{Session, StmtOutput};

/// Go `findSessionInfoIndex`.
fn find_session_info_index(lock: &TableLockInfo, info: &SessionInfo) -> Option<usize> {
    lock.sessions.iter().position(|session| {
        session.server_id == info.server_id && session.session_id == info.session_id
    })
}

/// Go `infoschema.ErrTableLocked.GenWithStackByArgs(tbInfo.Name.L,
/// tbInfo.Lock.Tp, tbInfo.Lock.Sessions[0])`.
fn table_locked_error(table: &KvTable, lock: &TableLockInfo) -> DriverError {
    DriverError::TableLocked {
        table: go_to_lower(&table.name),
        lock_type: lock.tp,
        holder: lock
            .sessions
            .iter()
            .next()
            .map(ToString::to_string)
            .unwrap_or_default(),
    }
}

/// Go `ddl.checkTableLocked`: whether `info` may take a `tp` lock on `table`.
fn check_table_locked(
    table: &KvTable,
    tp: TableLockType,
    info: &SessionInfo,
) -> Result<(), DriverError> {
    if !table.is_locked() {
        return Ok(());
    }
    let lock = table.lock().expect("is_locked has a lock");
    if lock.state == TableLockState::PRE_LOCK {
        return Ok(());
    }
    if (lock.tp == TableLockType::READ && tp == TableLockType::READ)
        || (lock.tp == TableLockType::READ_ONLY && tp == TableLockType::READ_ONLY)
    {
        return Ok(());
    }
    // The request session already locked the table: a repeat lock.
    if find_session_info_index(lock, info).is_some() {
        if lock.tp == tp {
            return Ok(());
        }
        // No other session holds it, and it is not the session-unrelated
        // READ ONLY lock.
        if lock.sessions.len() == 1 && lock.tp != TableLockType::READ_ONLY {
            return Ok(());
        }
    }
    Err(table_locked_error(table, lock))
}

/// Go `ddl.lockTable`, then the state step `onLockTables` takes: absent ->
/// `pre-lock` -> `public` for a fresh lock, `public` for one more READ
/// holder.
fn lock_table(
    table: &mut KvTable,
    tp: TableLockType,
    info: &SessionInfo,
) -> Result<(), DriverError> {
    let mut lock = match table.lock().filter(|_| table.is_locked()) {
        None => TableLockInfo {
            tp,
            sessions: vec![info.clone()].into(),
            ..TableLockInfo::default()
        },
        Some(lock) => {
            let mut lock = lock.clone();
            if lock.state != TableLockState::PRE_LOCK {
                let shared = (lock.tp == TableLockType::READ && tp == TableLockType::READ)
                    || (lock.tp == TableLockType::READ_ONLY && tp == TableLockType::READ_ONLY);
                if !shared {
                    // Unlock runs before lock, so Go never expects to get here.
                    return Err(table_locked_error(table, &lock));
                }
                if find_session_info_index(&lock, info).is_none() {
                    let mut sessions: Vec<SessionInfo> = lock.sessions.iter().cloned().collect();
                    sessions.push(info.clone());
                    lock.sessions = sessions.into();
                }
            }
            lock
        }
    };
    lock.state = TableLockState::PUBLIC;
    table.set_lock(Some(lock));
    Ok(())
}

/// Go `ddl.unlockTable`: drops `info` from the holders, or the whole lock
/// for `ADMIN CLEANUP TABLE LOCK` (`is_cleanup`).
fn unlock_table(table: &mut KvTable, info: &SessionInfo, is_cleanup: bool) {
    if !table.is_locked() {
        return;
    }
    if is_cleanup {
        table.set_lock(None);
        return;
    }
    let lock = table.lock().expect("is_locked has a lock");
    // A session cleaning its locks may name a table it no longer holds.
    let Some(index) = find_session_info_index(lock, info) else {
        return;
    };
    let mut lock = lock.clone();
    let mut sessions: Vec<SessionInfo> = lock.sessions.iter().cloned().collect();
    sessions.remove(index);
    if sessions.is_empty() {
        table.set_lock(None);
    } else {
        lock.sessions = sessions.into();
        table.set_lock(Some(lock));
    }
}

/// Go `ddl.unlockTables` over the job's `UnlockTables`: a table dropped since
/// it was locked is skipped.
fn unlock_tables(catalog: &mut Catalog, unlock: &[TableLockTpInfo], info: &SessionInfo) {
    for entry in unlock {
        let Some((schema, name)) = catalog.table_names_by_id(entry.table_id) else {
            continue;
        };
        if let Some(TableEntry::Kv(table)) = catalog.table_mut_in(&schema, &name) {
            unlock_table(std::sync::Arc::make_mut(table), info, false);
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
        Err(table_locked_error(tb, lock).into())
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
    /// info's ID to the DDL), and this in-process domain's own otherwise.
    fn table_lock_session_info(&self, catalog: &Catalog) -> SessionInfo {
        let server_id = match &self.server_info_syncer {
            Some(syncer) => syncer.local_server_info().static_info.id,
            None => catalog
                .ddl_uuid(tidb_domain::serverinfo_syncer::new_node_id)
                .to_owned(),
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

    /// Go `DDLExec.executeLockTables` and `executor.LockTables`, with the job
    /// `onLockTables` runs.
    pub(crate) fn execute_lock_tables(
        &mut self,
        locks: &[tidb_ast::TableLock],
    ) -> Result<StmtOutput, DriverError> {
        if !table_lock_enabled() {
            let warning =
                DriverError::FuncNotEnabled("LOCK TABLES", "enable-table-lock").to_mysql_error();
            self.append_warning(crate::WarningLevel::Warning, warning.code, warning.message);
            return Ok(StmtOutput::Done(false));
        }
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
        let cdc_write_source = self.ddl_cdc_write_source();
        let unlock = self.get_all_table_locks();
        let (info, lock_tables) = {
            let catalog = self.lock_catalog()?;
            let info = self.table_lock_session_info(&catalog);
            let mut lock_tables = Vec::with_capacity(locks.len());
            let mut unique_table_id = HashSet::new();
            for (lock, (db, name)) in locks.iter().zip(&names) {
                throw_err_if_in_mem_or_sys_db(login.as_ref(), db)?;
                let schema_id = catalog.database_id(db).ok_or_else(|| {
                    DriverError::Schema(SchemaErrorKind::UnknownDatabase(db.clone()))
                })?;
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
            (info, lock_tables)
        };
        // Go adds the locks before the job, so a session killed after the
        // job succeeded still releases them on close.
        self.add_table_lock(&lock_tables);
        let first_schema = names[0].0.clone();
        self.with_persistent_catalog_mut(|catalog| {
            tidb_executor::ddl::admit_bdr_job(
                catalog,
                cdc_write_source,
                &first_schema,
                tidb_model::ActionType::ACTION_LOCK_TABLE,
            )?;
            // `onLockTables`: the session's earlier locks are released first.
            unlock_tables(catalog, &unlock, &info);
            let targets: Vec<(String, String)> = lock_tables
                .iter()
                .map(|lock| {
                    catalog.table_names_by_id(lock.table_id).ok_or_else(|| {
                        DriverError::Schema(SchemaErrorKind::UnknownTable(
                            lock.table_id.to_string(),
                        ))
                    })
                })
                .collect::<Result<_, _>>()?;
            // Checked once more at the job's first step.
            for ((schema, name), lock) in targets.iter().zip(&lock_tables) {
                if let Some(TableEntry::Kv(table)) = catalog.table_in(schema, name) {
                    check_table_locked(table, lock.tp, &info)?;
                }
            }
            for ((schema, name), lock) in targets.iter().zip(&lock_tables) {
                if let Some(TableEntry::Kv(table)) = catalog.table_mut_in(schema, name) {
                    lock_table(std::sync::Arc::make_mut(table), lock.tp, &info)?;
                }
            }
            catalog.mark_has_table_locks();
            Ok(())
        })?;
        self.release_table_locks(&unlock);
        self.add_table_lock(&lock_tables);
        Ok(StmtOutput::Done(false))
    }

    /// Go `DDLExec.executeUnlockTables` and `executor.UnlockTables`, with the
    /// job `onUnlockTables` runs.
    pub(crate) fn execute_unlock_tables(&mut self) -> Result<StmtOutput, DriverError> {
        if !table_lock_enabled() {
            let warning =
                DriverError::FuncNotEnabled("UNLOCK TABLES", "enable-table-lock").to_mysql_error();
            self.append_warning(crate::WarningLevel::Warning, warning.code, warning.message);
            return Ok(StmtOutput::Done(false));
        }
        self.unlock_tables(self.get_all_table_locks())?;
        Ok(StmtOutput::Done(false))
    }

    /// Go `executor.UnlockTables`.
    fn unlock_tables(&mut self, unlock: Vec<TableLockTpInfo>) -> Result<(), DriverError> {
        if unlock.is_empty() {
            return Ok(());
        }
        let cdc_write_source = self.ddl_cdc_write_source();
        let info = {
            let catalog = self.lock_catalog()?;
            self.table_lock_session_info(&catalog)
        };
        self.with_persistent_catalog_mut(|catalog| {
            let schema = catalog
                .table_names_by_id(unlock[0].table_id)
                .map(|(schema, _)| schema)
                .unwrap_or_default();
            tidb_executor::ddl::admit_bdr_job(
                catalog,
                cdc_write_source,
                &schema,
                tidb_model::ActionType::ACTION_UNLOCK_TABLE,
            )?;
            unlock_tables(catalog, &unlock, &info);
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
    Err(DriverError::Mysql(tidb_executor::MysqlError::new(
        tidb_error::tidb::errcode::ErrAccessDenied,
        format!("Access denied for user '{user}'@'{host}' (using password: %!s(MISSING))"),
    )))
}
