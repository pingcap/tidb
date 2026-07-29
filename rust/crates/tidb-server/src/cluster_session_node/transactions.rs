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

//! The transaction seam: one fresh read snapshot per autocommit statement,
//! one publication of its staged writes, and the single transaction an
//! explicit `BEGIN` holds open. Split out of `cluster_session_node` because
//! it is one of the independent seams that accreted there; see that module's
//! doc comment for the statement lifecycle this seam is exercised by.

use std::sync::Arc;
use std::time::Duration;

use tidb_exec::cluster_table_storage::{
    commit_staged_buffer, SessionTransaction, StatementSnapshot,
};
use tidb_exec::pessimistic_lock_error::LockSqlError;
use tidb_exec::real_tikv_read::RealOptimisticTransactionOpener;
use tidb_executor::cluster_storage::{ClusterSnapshot, MutationBuffer};

use crate::sql_node::SqlQueryError;

/// Carries a commit's own client-visible triple onto the wire, so a 9007 stays
/// a 9007 instead of collapsing into the generic 1105.
pub(crate) fn sql_error(error: LockSqlError) -> SqlQueryError {
    SqlQueryError::new(error.code, error.state, error.message)
}

/// Everything a connection needs from the cluster's transaction tier: one
/// fresh read snapshot per autocommit statement, one publication of its staged
/// writes, and the single transaction an explicit `BEGIN` holds open.
///
/// The seam exists so the statement lifecycle -- which is the correctness core
/// of this mode -- is exercised without a cluster. The production
/// implementation is [`RealClusterTransactions`]; the tests drive the same
/// lifecycle against an in-memory committed store.
pub trait ClusterTransactions: Send + Sync {
    /// Opens one autocommit statement's read snapshot at its own timestamp.
    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Publishes one autocommit statement's staged writes as its own
    /// transaction, then empties the buffer. An empty buffer publishes nothing.
    ///
    /// The error is the client-visible one, because a publication TiKV refused
    /// has a code of its own: a lost race is 9007, not a generic failure.
    fn commit(&self, buffer: &MutationBuffer) -> Result<(), SqlQueryError>;

    /// Opens the one transaction an explicit `BEGIN` holds until `COMMIT` or
    /// `ROLLBACK`.
    fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String>;
}

/// The transaction an explicit `BEGIN` holds open across its statements.
///
/// Every statement of the transaction reads through [`Self::snapshot`], so they
/// all share the timestamp `BEGIN` took, and [`Self::commit`] prewrites at that
/// same timestamp -- which is what makes a racing writer a write conflict
/// instead of a silent overwrite.
pub trait OpenClusterTransaction: Send {
    /// One statement's read handle. Dropping it ends the statement, never the
    /// transaction.
    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Publishes the staged writes at the transaction's own start timestamp and
    /// empties the buffer.
    ///
    /// The error is the client-visible one: a transaction whose prewrite lost
    /// the race against a newer commit reports 9007, as Go's does.
    fn commit(self: Box<Self>, buffer: &MutationBuffer) -> Result<(), SqlQueryError>;

    /// Ends the transaction without publishing anything.
    fn rollback(self: Box<Self>) -> Result<(), String>;
}

/// The production transaction tier: real read-only transactions and the
/// optimistic 2PC, both over the node's one process authority.
pub struct RealClusterTransactions {
    opener: Arc<RealOptimisticTransactionOpener>,
    timeout: Duration,
}

impl RealClusterTransactions {
    /// Binds the tier to an already-connected authority's write capability.
    #[must_use]
    pub fn new(opener: RealOptimisticTransactionOpener, timeout: Duration) -> Self {
        Self {
            opener: Arc::new(opener),
            timeout,
        }
    }
}

impl ClusterTransactions for RealClusterTransactions {
    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        StatementSnapshot::open(Arc::clone(&self.opener), self.timeout)
            .map(|snapshot| Box::new(snapshot) as Box<dyn ClusterSnapshot>)
            .map_err(|error| error.to_string())
    }

    fn commit(&self, buffer: &MutationBuffer) -> Result<(), SqlQueryError> {
        commit_staged_buffer(&self.opener, buffer, self.timeout)
            .map(|_| ())
            .map_err(sql_error)
    }

    fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String> {
        SessionTransaction::begin(Arc::clone(&self.opener), self.timeout)
            .map(|transaction| Box::new(transaction) as Box<dyn OpenClusterTransaction>)
            .map_err(|error| error.to_string())
    }
}

impl OpenClusterTransaction for SessionTransaction {
    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::snapshot(self).map_err(|error| error.to_string())
    }

    fn commit(self: Box<Self>, buffer: &MutationBuffer) -> Result<(), SqlQueryError> {
        SessionTransaction::commit(*self, buffer)
            .map(|_| ())
            .map_err(sql_error)
    }

    fn rollback(self: Box<Self>) -> Result<(), String> {
        SessionTransaction::rollback(*self)
    }
}
