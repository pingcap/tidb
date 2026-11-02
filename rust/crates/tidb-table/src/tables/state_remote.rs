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

//! Go `pkg/table/tables/state_remote.go`: the cached table's lock meta in
//! `mysql.table_cache_meta`, read and written through internal SQL.

use std::time::Duration;

use tidb_datatype::Datum;
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::{SqlExecError, SqlExecutor};

/// Go `CachedTableLockType`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CachedTableLockType {
    /// Go `CachedTableLockNone`: there is no lock.
    #[default]
    None,
    /// Go `CachedTableLockRead`.
    Read,
    /// Go `CachedTableLockIntend`: the write intention, while a READ lock
    /// changes to WRITE and the READ lease has not expired.
    Intend,
    /// Go `CachedTableLockWrite`.
    Write,
}

impl CachedTableLockType {
    fn from_enum_offset(offset: u64) -> Self {
        match offset {
            0 => Self::None,
            1 => Self::Read,
            2 => Self::Intend,
            3 => Self::Write,
            _ => panic!("invalid CachedTableLockType value"),
        }
    }
}

impl std::fmt::Display for CachedTableLockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::None => "NONE",
            Self::Read => "READ",
            Self::Intend => "INTEND",
            Self::Write => "WRITE",
        })
    }
}

/// Go `StateRemote`: the remote state of the cached table's lock meta.
/// IMPORTANT: it is not thread-safe; the caller should be aware of that.
pub trait StateRemote: Send {
    /// Go `Load`: the lock type and lease of the table.
    ///
    /// # Errors
    ///
    /// The internal SQL's error, or a missing meta row.
    fn load(&mut self, tid: i64) -> Result<(CachedTableLockType, u64), SqlExecError>;

    /// Go `LockForRead`: on success, by the protocol, the TiKV data is not
    /// modified until the lease expires, so the caller may load, cache and
    /// use it.
    ///
    /// # Errors
    ///
    /// The internal SQL's error.
    fn lock_for_read(&mut self, tid: i64, lease: u64) -> Result<bool, SqlExecError>;

    /// Go `LockForWrite`: the write lock's lease.
    ///
    /// # Errors
    ///
    /// The internal SQL's error.
    fn lock_for_write(&mut self, tid: i64, lease_duration: Duration) -> Result<u64, SqlExecError>;

    /// Go `RenewReadLease`: the current lease on success, 0 on failure.
    ///
    /// # Errors
    ///
    /// The internal SQL's error.
    fn renew_read_lease(
        &mut self,
        tid: i64,
        old_local_lease: u64,
        new_value: u64,
    ) -> Result<u64, SqlExecError>;

    /// Go `RenewWriteLease`.
    ///
    /// # Errors
    ///
    /// The internal SQL's error.
    fn renew_write_lease(&mut self, tid: i64, new_ts: u64) -> Result<bool, SqlExecError>;
}

/// Go `stateRemoteHandle`.
pub struct StateRemoteHandle {
    exec: std::sync::Arc<dyn SqlExecutor>,

    // Local state, which could be stale. The handle is used by one thread at
    // a time, so every operation may check it first to avoid an unnecessary
    // remote access.
    lock_type: CachedTableLockType,
    lease: u64,
    old_read_lease: u64,
}

impl StateRemoteHandle {
    /// Go `NewStateRemote`.
    #[must_use]
    pub fn new(exec: std::sync::Arc<dyn SqlExecutor>) -> Self {
        Self {
            exec,
            lock_type: CachedTableLockType::None,
            lease: 0,
            old_read_lease: 0,
        }
    }

    fn lock_for_write_once(
        &mut self,
        tid: i64,
        lease_duration: Duration,
    ) -> Result<(Duration, u64), SqlExecError> {
        let mut wait_and_retry = Duration::ZERO;
        let mut ts = 0;
        let mut update_local = None;
        self.run_in_txn(true, |handle, now| {
            let (lock_type, lease, old_read_lease) = handle.load_row(tid, true)?;
            ts = lease_from_ts(now, lease_duration);
            // The lease is outdated, so the lock is invalid: clear an orphan
            // lock of any kind.
            if now > lease {
                handle.update_row(tid, "WRITE", ts)?;
                return Ok(());
            }

            // The lease is valid.
            match lock_type {
                CachedTableLockType::None => {
                    handle.update_row(tid, "WRITE", ts)?;
                    update_local = Some((CachedTableLockType::Write, ts, 0));
                }
                CachedTableLockType::Read => {
                    let new_lease = ts.max(lease);
                    // Change from READ to INTEND.
                    handle.exec_sql(
                        "update mysql.table_cache_meta set lock_type='INTEND', oldReadLease=%?, lease=%? where tid=%?",
                        &[SqlArg::Unsigned(lease), SqlArg::Unsigned(new_lease), SqlArg::Signed(tid)],
                    )?;
                    // Wait for the lease to expire, and then retry.
                    wait_and_retry = wait_for_lease_expire(lease, now);
                    update_local = Some((CachedTableLockType::Intend, ts, lease));
                }
                CachedTableLockType::Intend => {
                    // `now` past `oldReadLease` means the READ lease is over:
                    // it is safe to write.
                    if now > old_read_lease {
                        handle.update_row(tid, "WRITE", ts)?;
                        update_local = Some((CachedTableLockType::Write, ts, 0));
                        return Ok(());
                    }
                    // Otherwise the WRITE waits for the READ lease to expire,
                    // then retries changing the lock to WRITE.
                    wait_and_retry = wait_for_lease_expire(old_read_lease, now);
                }
                CachedTableLockType::Write => {
                    // Never decrease the lease.
                    if ts > lease {
                        handle.update_row(tid, "WRITE", ts)?;
                        update_local = Some((CachedTableLockType::Write, ts, 0));
                    }
                }
            }
            Ok(())
        })?;
        if let Some((lock_type, lease, old_read_lease)) = update_local {
            self.lock_type = lock_type;
            self.lease = lease;
            self.old_read_lease = old_read_lease;
        }
        Ok((wait_and_retry, ts))
    }

    fn begin_txn(&mut self, pessimistic: bool) -> Result<(), SqlExecError> {
        if pessimistic {
            self.exec_sql("begin pessimistic", &[])?;
        } else {
            self.exec_sql("begin optimistic", &[])?;
        }
        Ok(())
    }

    fn commit_txn(&mut self) -> Result<(), SqlExecError> {
        self.exec_sql("commit", &[]).map(drop)
    }

    fn rollback_txn(&mut self) -> Result<(), SqlExecError> {
        self.exec_sql("rollback", &[]).map(drop)
    }

    fn run_in_txn(
        &mut self,
        pessimistic: bool,
        f: impl FnOnce(&mut Self, u64) -> Result<(), SqlExecError>,
    ) -> Result<(), SqlExecError> {
        self.begin_txn(pessimistic)?;
        self.exec_sql("set @@session.tidb_retry_limit = 0", &[])?;
        let rows = self.exec_sql("select @@tidb_current_ts", &[])?;
        let result = match rows.first().and_then(|row| row.first()) {
            Some(Datum::String(text)) => String::from_utf8_lossy(text.bytes()).into_owned(),
            Some(Datum::Bytes(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
            Some(other) => format!("{other:?}"),
            None => String::new(),
        };
        let txn_ts: u64 = result.parse()?;
        if let Err(error) = f(self, txn_ts) {
            if let Err(rollback) = self.rollback_txn() {
                tracing::error!(%rollback, "rollback cached table lock meta");
            }
            return Err(error);
        }
        self.commit_txn()
    }

    fn load_row(
        &mut self,
        tid: i64,
        for_update: bool,
    ) -> Result<(CachedTableLockType, u64, u64), SqlExecError> {
        let rows = if for_update {
            self.exec_sql(
                "select lock_type, lease, oldReadLease from mysql.table_cache_meta where tid = %? for update",
                &[SqlArg::Signed(tid)],
            )?
        } else {
            self.exec_sql(
                "select lock_type, lease, oldReadLease from mysql.table_cache_meta where tid = %?",
                &[SqlArg::Signed(tid)],
            )?
        };
        if rows.len() != 1 {
            return Err(format!("table_cache_meta tid not exist {tid}").into());
        }
        let row = &rows[0];
        // Note: a MySQL enum value starts from 1, not 0.
        let lock_type = match &row[0] {
            Datum::Enum(value, _) => CachedTableLockType::from_enum_offset(value.value() - 1),
            other => return Err(format!("table_cache_meta lock_type is {other:?}").into()),
        };
        let lease = get_uint64(&row[1]);
        let old_read_lease = get_uint64(&row[2]);

        // Also keep a local copy after loadRow().
        self.lock_type = lock_type;
        self.lease = lease;
        self.old_read_lease = old_read_lease;

        Ok((lock_type, lease, old_read_lease))
    }

    fn update_row(&mut self, tid: i64, lock_type: &str, lease: u64) -> Result<(), SqlExecError> {
        self.exec_sql(
            "update mysql.table_cache_meta set lock_type = %?, lease = %? where tid = %?",
            &[
                SqlArg::String(lock_type.as_bytes()),
                SqlArg::Unsigned(lease),
                SqlArg::Signed(tid),
            ],
        )
        .map(drop)
    }

    fn exec_sql(&mut self, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, SqlExecError> {
        match self.exec.execute_internal(&(), sql, args)? {
            Some(mut record_set) => {
                tidb_sqlexec::drain_record_set_and_close(&(), record_set.as_mut(), 1)
            }
            None => Ok(Vec::new()),
        }
    }
}

impl StateRemote for StateRemoteHandle {
    fn load(&mut self, tid: i64) -> Result<(CachedTableLockType, u64), SqlExecError> {
        let (lock_type, lease, _) = self.load_row(tid, false)?;
        Ok((lock_type, lease))
    }

    fn lock_for_read(&mut self, tid: i64, new_lease: u64) -> Result<bool, SqlExecError> {
        // A write lock or intention: no read lock.
        if self.lease >= new_lease
            && matches!(
                self.lock_type,
                CachedTableLockType::Intend | CachedTableLockType::Write
            )
        {
            return Ok(false);
        }
        let mut succ = false;
        self.run_in_txn(false, |handle, now| {
            let (lock_type, lease, _) = handle.load_row(tid, false)?;
            // The old lock is outdated: clear the orphan lock.
            if now > lease {
                // Never decrease the lease.
                if new_lease > now {
                    succ = true;
                    handle.update_row(tid, "READ", new_lease)?;
                }
                return Ok(());
            }
            match lock_type {
                CachedTableLockType::None | CachedTableLockType::Read => {}
                CachedTableLockType::Write | CachedTableLockType::Intend => return Ok(()),
            }
            succ = true;
            // Never decrease the lease.
            if new_lease > lease {
                handle.update_row(tid, "READ", new_lease)?;
            }
            Ok(())
        })?;
        Ok(succ)
    }

    fn lock_for_write(&mut self, tid: i64, lease_duration: Duration) -> Result<u64, SqlExecError> {
        if self.lock_type == CachedTableLockType::Write {
            let safe = go_time_to_ts(unix_millis_now() + duration_millis(lease_duration / 2));
            // The remote is already write locked, and the lock stays valid
            // for a while.
            if self.lease > safe {
                return Ok(self.lease);
            }
        }
        loop {
            let (wait_and_retry, lease) = self.lock_for_write_once(tid, lease_duration)?;
            if wait_and_retry.is_zero() {
                return Ok(lease);
            }
            std::thread::sleep(wait_and_retry);
        }
    }

    fn renew_read_lease(
        &mut self,
        tid: i64,
        old_local_lease: u64,
        new_value: u64,
    ) -> Result<u64, SqlExecError> {
        let mut new_lease = 0;
        self.run_in_txn(false, |handle, now| {
            let (lock_type, remote_lease, _) = handle.load_row(tid, false)?;
            // The read lock already expired: fail to renew.
            if now >= remote_lease {
                return Ok(());
            }
            // Not a read lock: fail to renew.
            if lock_type != CachedTableLockType::Read {
                return Ok(());
            }
            // Another TiDB instance already changed the lease.
            if old_local_lease != remote_lease {
                // 1. Data in [cacheDataTS -------- oldLocalLease) is immutable.
                // 2. Data in [now -------- remoteLease) is immutable.
                //
                // With now < oldLocalLease the whole range is immutable, so
                // the old cache data is still usable. Otherwise a write may
                // have happened between oldLocalLease and the new
                // remoteLease: the renewal fails.
                if now < old_local_lease {
                    new_lease = remote_lease;
                }
                return Ok(());
            }
            // The lease never decreases.
            if new_value > remote_lease {
                handle.update_row(tid, "READ", new_value)?;
                new_lease = new_value;
            } else {
                new_lease = remote_lease;
            }
            Ok(())
        })?;
        Ok(new_lease)
    }

    fn renew_write_lease(&mut self, tid: i64, new_lease: u64) -> Result<bool, SqlExecError> {
        let mut succ = false;
        self.run_in_txn(true, |handle, now| {
            let (lock_type, old_lease, _) = handle.load_row(tid, true)?;
            // The write lock already expired: fail to renew.
            if now >= old_lease {
                return Ok(());
            }
            // Not a write lock: fail to renew.
            if lock_type != CachedTableLockType::Write {
                return Ok(());
            }
            // The lease never decreases.
            if new_lease > old_lease {
                handle.update_row(tid, "WRITE", new_lease)?;
            }
            succ = true;
            Ok(())
        })?;
        if succ {
            self.lock_type = CachedTableLockType::Write;
            self.lease = new_lease;
        }
        Ok(succ)
    }
}

/// Go `chunk.Row.GetUint64` over a stored BIGINT.
fn get_uint64(datum: &Datum) -> u64 {
    match datum {
        Datum::UInt(value) => *value,
        Datum::Int(value) => *value as u64,
        _ => 0,
    }
}

fn wait_for_lease_expire(old_read_lease: u64, now: u64) -> Duration {
    if old_read_lease >= now {
        let t1 = get_time_from_ts(old_read_lease);
        let t2 = get_time_from_ts(now);
        if t1 > t2 {
            return Duration::from_millis(t1 - t2);
        }
        return Duration::from_micros(1);
    }
    Duration::ZERO
}

/// Go `leaseFromTS` (`cache.go`).
pub(crate) fn lease_from_ts(ts: u64, lease_duration: Duration) -> u64 {
    go_time_to_ts(get_time_from_ts(ts) + duration_millis(lease_duration))
}

/// Go `oracle.GetTimeFromTS`, as Unix milliseconds.
pub(crate) fn get_time_from_ts(ts: u64) -> u64 {
    ts >> 18
}

/// Go `oracle.GoTimeToTS`, from Unix milliseconds.
pub(crate) fn go_time_to_ts(unix_millis: u64) -> u64 {
    unix_millis << 18
}

pub(crate) fn unix_millis_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

pub(crate) fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis() as u64
}
