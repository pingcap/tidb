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

//! Go `ddl/table_lock.go`'s edits to `TableInfo.Lock`, shared by the
//! in-process catalog (a `KvTable`'s lock) and the cluster planner (a stored
//! `TableInfo`'s): `checkTableLocked`, `lockTable` with the state step
//! `onLockTables` takes, and `unlockTable`.

use tidb_ast::TableLockType;
use tidb_model::{SessionInfo, TableLockInfo, TableLockState};

use crate::DriverError;

/// Go `TableInfo.IsLocked`.
#[must_use]
pub fn is_locked(lock: Option<&TableLockInfo>) -> bool {
    lock.is_some_and(|lock| !lock.sessions.is_empty())
}

/// Go `findSessionInfoIndex`.
fn find_session_info_index(lock: &TableLockInfo, info: &SessionInfo) -> Option<usize> {
    lock.sessions.iter().position(|session| {
        session.server_id == info.server_id && session.session_id == info.session_id
    })
}

/// Go `infoschema.ErrTableLocked.GenWithStackByArgs(tbInfo.Name.L,
/// tbInfo.Lock.Tp, tbInfo.Lock.Sessions[0])`; `table` is the lowercase name.
#[must_use]
pub fn table_locked_error(table: &str, lock: &TableLockInfo) -> DriverError {
    DriverError::TableLocked {
        table: table.to_owned(),
        lock_type: lock.tp,
        holder: lock
            .sessions
            .iter()
            .next()
            .map(ToString::to_string)
            .unwrap_or_default(),
    }
}

/// Go `ddl.checkTableLocked`: whether `info` may take a `tp` lock on the
/// table `table` (lowercase) whose lock is `lock`.
///
/// # Errors
///
/// `ErrTableLocked` when another session's lock conflicts.
pub fn check_table_locked(
    table: &str,
    lock: Option<&TableLockInfo>,
    tp: TableLockType,
    info: &SessionInfo,
) -> Result<(), DriverError> {
    let Some(lock) = lock.filter(|_| is_locked(lock)) else {
        return Ok(());
    };
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
/// holder, `TS` the job's meta transaction start. A caller that applies the
/// job in one step lands on the final `public` state.
///
/// # Errors
///
/// `ErrTableLocked` when a conflicting lock is still held (Go never expects
/// it: the job unlocks first).
pub fn lock_table(
    table: &str,
    lock: Option<&TableLockInfo>,
    tp: TableLockType,
    info: &SessionInfo,
    ts: u64,
) -> Result<TableLockInfo, DriverError> {
    let mut next = match lock.filter(|_| is_locked(lock)) {
        None => TableLockInfo {
            tp,
            sessions: vec![info.clone()].into(),
            ..TableLockInfo::default()
        },
        Some(lock) => {
            let mut next = lock.clone();
            if next.state != TableLockState::PRE_LOCK {
                let shared = (next.tp == TableLockType::READ && tp == TableLockType::READ)
                    || (next.tp == TableLockType::READ_ONLY && tp == TableLockType::READ_ONLY);
                if !shared {
                    return Err(table_locked_error(table, lock));
                }
                if find_session_info_index(&next, info).is_none() {
                    let mut sessions: Vec<SessionInfo> = next.sessions.iter().cloned().collect();
                    sessions.push(info.clone());
                    next.sessions = sessions.into();
                }
            }
            next
        }
    };
    next.state = TableLockState::PUBLIC;
    next.ts = ts;
    Ok(next)
}

/// Go `ddl.unlockTable`: the lock after `info` lets go of it (or after
/// `ADMIN CLEANUP TABLE LOCK`, `is_cleanup`), `None` when nothing changes
/// (Go's `needUpdateTableInfo == false`).
#[must_use]
pub fn unlock_table(
    lock: Option<&TableLockInfo>,
    info: &SessionInfo,
    is_cleanup: bool,
) -> Option<Option<TableLockInfo>> {
    let lock = lock.filter(|_| is_locked(lock))?;
    if is_cleanup {
        return Some(None);
    }
    // A session cleaning its locks may name a table it no longer holds.
    let index = find_session_info_index(lock, info)?;
    let mut sessions: Vec<SessionInfo> = lock.sessions.iter().cloned().collect();
    sessions.remove(index);
    if sessions.is_empty() {
        return Some(None);
    }
    let mut next = lock.clone();
    next.sessions = sessions.into();
    Some(Some(next))
}
