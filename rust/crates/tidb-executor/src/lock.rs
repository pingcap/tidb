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

//! Go `pkg/lock`: `Checker`, one statement's table accesses against the
//! session's `LOCK TABLES` set and the tables' locks.

use std::collections::BTreeMap;

use tidb_ast::TableLockType;
use tidb_ddl_build::table_lock;
use tidb_model::TableLockTpInfo;
use tidb_mysql::PrivilegeType;
use tidb_util::stringutil::go_to_lower;

use crate::{Catalog, DriverError, TableEntry};

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
pub struct Checker<'a> {
    /// Go `TableLockReadContext`: the session's own lock set.
    locked_tables: &'a BTreeMap<i64, TableLockTpInfo>,
    catalog: &'a Catalog,
}

impl<'a> Checker<'a> {
    /// A checker over the session's lock set and the statement's catalog.
    pub fn new(locked_tables: &'a BTreeMap<i64, TableLockTpInfo>, catalog: &'a Catalog) -> Self {
        Self {
            locked_tables,
            catalog,
        }
    }

    fn has_locked_tables(&self) -> bool {
        !self.locked_tables.is_empty()
    }

    /// Go `Checker.CheckTableLock`.
    fn check_table_lock(
        &self,
        db: &str,
        table: &str,
        privilege: PrivilegeType,
        alter_writable: bool,
    ) -> Result<(), LockCheckError> {
        if (db.is_empty() && table.is_empty()) || privilege == tidb_mysql::LockTablesPriv {
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
            tidb_mysql::ShowDBPriv => return Ok(()),
            tidb_mysql::CreatePriv | tidb_mysql::CreateViewPriv => {
                if self.has_locked_tables() {
                    return Err(DriverError::from(
                        tidb_error::catalog::infoschema::ERR_TABLE_NOT_LOCKED
                            .generate_with_stack_by_args(&[table.to_owned().into()]),
                    )
                    .into());
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
        if privilege == tidb_mysql::DropPriv && tb.name() == table && self.has_locked_tables() {
            if self.locked_tables.contains_key(&tb.table_id()) {
                if lock.tp == TableLockType::WRITE {
                    return Err(LockCheckError::LockedTableDropped);
                }
                if matches!(
                    lock.tp,
                    TableLockType::READ | TableLockType::WRITE_LOCAL | TableLockType::READ_ONLY
                ) {
                    return Err(DriverError::from(
                        tidb_error::catalog::infoschema::ERR_TABLE_NOT_LOCKED_FOR_WRITE
                            .generate_with_stack_by_args(&[tb.name().into()]),
                    )
                    .into());
                }
            }
        }
        if !alter_writable && self.has_locked_tables() {
            if let Some(held) = self.locked_tables.get(&tb.table_id()) {
                if check_lock_tp_meet_privilege(held.tp, privilege) {
                    return Ok(());
                }
                return Err(DriverError::from(
                    tidb_error::catalog::infoschema::ERR_TABLE_NOT_LOCKED_FOR_WRITE
                        .generate_with_stack_by_args(&[tb.name().into()]),
                )
                .into());
            }
            return Err(DriverError::from(
                tidb_error::catalog::infoschema::ERR_TABLE_NOT_LOCKED
                    .generate_with_stack_by_args(&[tb.name().into()]),
            )
            .into());
        }
        if privilege == tidb_mysql::SelectPriv
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
        Err(DriverError::from(table_lock::table_locked_error(&go_to_lower(&tb.name()), &lock)).into())
    }

    /// Go `Checker.CheckLockInDB`.
    fn check_lock_in_db(&self, db: &str, privilege: PrivilegeType) -> Result<(), LockCheckError> {
        if self.has_locked_tables()
            && matches!(
                privilege,
                tidb_mysql::CreatePriv | tidb_mysql::DropPriv | tidb_mysql::AlterPriv
            )
        {
            return Err(DriverError::from(
                tidb_error::catalog::table::ERR_LOCK_OR_ACTIVE_TRANSACTION
                    .generate_with_stack_by_args(&[]),
            )
            .into());
        }
        if privilege == tidb_mysql::CreatePriv {
            return Ok(());
        }
        // Go checks every locked table's NAME in `db`, whichever schema the
        // locked table itself lives in.
        for (_, table) in self.catalog.locked_tables() {
            self.check_table_lock(db, &go_to_lower(&table.name()), privilege, false)?;
        }
        Ok(())
    }
}

/// Go `lock.checkLockTpMeetPrivilege`.
fn check_lock_tp_meet_privilege(tp: TableLockType, privilege: PrivilegeType) -> bool {
    // READ ONLY is session-unrelated and never reaches here.
    match tp {
        TableLockType::WRITE | TableLockType::WRITE_LOCAL => true,
        // ShowDB, Create and CreateView were answered before; a READ lock
        // admits nothing else but SELECT.
        TableLockType::READ => privilege == tidb_mysql::SelectPriv,
        _ => false,
    }
}

/// Go `core.CheckTableLock`'s loop over the visit infos.
pub fn check_visits(
    checker: &Checker<'_>,
    visits: &[(&str, &str, PrivilegeType)],
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
