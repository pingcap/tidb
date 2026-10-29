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

//! The pieces of Go's one `session` object that run a statement over a
//! store: the error mapping a client sees, the commit-hooks factory, and the
//! `TxnManagerOps` view of the transaction manager, snapshot slot and catalog
//! follower the cluster connection holds alongside its [`Session`].

use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_kv::cluster_storage::BufferCheckpoint;
use tidb_driver_error::DriverError;

use crate::statement_lifecycle::{finish_snapshot, CatalogFollowing, SnapshotSlot};
use crate::txn::driver_error_of;
use crate::txn_manager::{CommitHooks, CommitTxnRegions, TxnManager};
use crate::{Session, TxnManagerOps};

/// A driver error as the client sees it: Go's code, SQLSTATE and message, with
/// the error's own identity and its typed cause (Go `errors.Cause`) carried
/// across.
#[must_use]
pub fn sql_query_error(error: tidb_driver_error::DriverError) -> SqlQueryError {
    let mapped = error.clone().to_mysql_error();
    SqlQueryError::from(mapped).with_cause(error)
}

/// Builds what a commit reads off the server, for the schema version the
/// statement planned against.
pub type CommitHooksFactory = Box<dyn Fn(&Session, i64) -> Box<dyn CommitHooks> + Send>;

/// Go `sessiontxn.GetTxnManager(sctx)` as the `SimpleExec` transaction arms
/// reach it: the store session's [`TxnManager`] with the snapshot slot, the
/// catalog follower and the commit hooks it needs.
pub(crate) struct StoreTxnOps<'a, F: CatalogFollowing> {
    pub(crate) txn: &'a mut TxnManager,
    pub(crate) slot: &'a SnapshotSlot,
    pub(crate) follower: &'a mut F,
    pub(crate) hooks: Box<dyn CommitHooks>,
}

impl<F: CatalogFollowing> TxnManagerOps for StoreTxnOps<'_, F> {
    fn commit_txn(&mut self, session: &mut Session) -> Result<(), DriverError> {
        let _commit_txn = CommitTxnRegions::begin(true);
        finish_snapshot(self.slot).map_err(driver_error_of)?;
        self.txn
            .commit(session, self.hooks.as_ref())
            .map_err(driver_error_of)
    }

    fn rollback_txn(&mut self, session: &mut Session) -> Result<(), DriverError> {
        finish_snapshot(self.slot).map_err(driver_error_of)?;
        self.txn.rollback(session).map_err(driver_error_of)
    }

    fn enter_new_txn_with_begin(&mut self, session: &mut Session) -> Result<(), DriverError> {
        // The refresh runs before the transaction pins its schema view: Go
        // activates a transaction with the LATEST schema at start, on a
        // connection of any age.
        self.follower.rebuild(session);
        let resource_group = session.current_resource_group().to_owned();
        self.txn
            .open(session, &resource_group, self.follower.schema_version())
            .map_err(driver_error_of)
    }

    fn savepoint_checkpoint(
        &mut self,
        session: &mut Session,
    ) -> Result<Option<BufferCheckpoint>, DriverError> {
        let resource_group = session.current_resource_group().to_owned();
        self.txn
            .savepoint_checkpoint(session, &resource_group, self.follower.schema_version())
            .map_err(driver_error_of)
    }

    fn rollback_to_checkpoint(&mut self, checkpoint: &BufferCheckpoint) {
        self.txn.rollback_to_checkpoint(checkpoint);
    }
}
