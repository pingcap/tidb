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

//! Go `pkg/session/advisory_locks.go`: each advisory lock is a session of
//! its own, holding `INSERT INTO mysql.advisory_locks` inside a pessimistic
//! transaction it never commits; releasing the lock rolls it back.

use std::sync::Weak;
use std::time::Duration;

use tidb_domain_core::domain::Domain;
use tidb_sessionctx::advisory_lock_state::{
    AdvisoryLockError, AdvisoryLockLease, AdvisoryLockService,
};

use super::DomainSessions as _;
use crate::Session;

/// Go `mysql.ErrLockWaitTimeout`.
const ERR_LOCK_WAIT_TIMEOUT: u16 = 1205;
/// Go `mysql.ErrLockDeadlock`.
const ERR_LOCK_DEADLOCK: u16 = 1213;

/// Go `session.GetAdvisoryLock` / `IsUsedAdvisoryLock` over the domain's
/// internal sessions (`getInternalSession`).
pub(crate) struct SessionAdvisoryLockService {
    pub(crate) domain: Weak<Domain>,
}

/// Go `advisoryLock`: the lock's session.
struct AdvisoryLock {
    session: Session,
}

impl AdvisoryLock {
    fn execute(
        &mut self,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        let escaped = tidb_sqlescape::escape_sql(sql, args).map_err(|error| {
            tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(error.to_string())
        })?;
        self.session
            .node_conn()
            .execute_internal(&String::from_utf8_lossy(&escaped))
            .map(|_| ())
    }

    /// Go `advisoryLock.Close`: the transaction rolls back, so its lock goes.
    fn close(mut self) {
        if let Err(error) = self.execute("ROLLBACK", &[]) {
            // Go `terror.Log(err)`.
            tidb_logutil::bg_logger().error(
                "encountered error",
                &[tidb_logutil::zap_error(error.message)],
            );
        }
    }

    /// Go `advisoryLock.GetLock` / `IsUsedLock`'s statements: the lock wait
    /// budget, the pessimistic transaction, and the row insert whose lock is
    /// the advisory lock.
    fn lock(
        &mut self,
        lock_name: &str,
        timeout_secs: u64,
    ) -> Result<(), tidb_store_driver_error::sql_query_error::SqlQueryError> {
        use tidb_sqlescape::SqlArg;
        self.execute(
            "SET innodb_lock_wait_timeout = %?",
            &[SqlArg::Unsigned(timeout_secs)],
        )?;
        self.execute("BEGIN PESSIMISTIC", &[])?;
        self.execute(
            "INSERT INTO mysql.advisory_locks (lock_name) VALUES (%?)",
            &[SqlArg::from(lock_name)],
        )
    }
}

impl SessionAdvisoryLockService {
    /// Go `getInternalSession`.
    fn new_lock(&self) -> Result<AdvisoryLock, AdvisoryLockError> {
        let domain = self
            .domain
            .upgrade()
            .ok_or_else(|| AdvisoryLockError::Internal("the node's domain is closed".to_owned()))?;
        let session = domain
            .open_system_session()
            .map_err(|error| AdvisoryLockError::Internal(error.message))?;
        Ok(AdvisoryLock { session })
    }
}

impl AdvisoryLockService for SessionAdvisoryLockService {
    /// Go `advisoryLock.GetLock`.
    fn acquire(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError> {
        let mut lock = self.new_lock()?;
        if let Err(error) = lock.lock(name, timeout.as_secs()) {
            // The lock was not acquired: the session closes cleanly and the
            // caller reads a lock wait timeout or a deadlock.
            lock.close();
            return Err(match error.code {
                ERR_LOCK_WAIT_TIMEOUT => AdvisoryLockError::Timeout,
                ERR_LOCK_DEADLOCK => AdvisoryLockError::Deadlock,
                _ => AdvisoryLockError::Internal(error.message),
            });
        }
        Ok(Box::new(lock))
    }

    /// Go `IsUsedAdvisoryLock`'s second half (`advisoryLock.IsUsedLock`):
    /// any failure to take the lock within a second reads as used.
    fn is_used(&self, name: &str) -> bool {
        let Ok(mut lock) = self.new_lock() else {
            return false;
        };
        let used = lock.lock(name, 1).is_err();
        lock.close();
        used
    }
}

impl AdvisoryLockLease for AdvisoryLock {
    fn release(self: Box<Self>) {
        (*self).close();
    }
}
