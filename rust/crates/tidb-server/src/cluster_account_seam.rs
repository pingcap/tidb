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

//! The route an account statement takes when the accounts live in the
//! cluster, not in this process.
//!
//! This is [`crate::cluster_session_node`]'s [`ClusterDdl`] seam's twin, and
//! it exists for the same reason: a `CREATE USER` run against the node's own
//! in-memory account table would change a *read* of somebody else's state and
//! answer `OK` to a client whose account does not exist anywhere.
//!
//! # Which half runs first, and what a failure leaves behind
//!
//! The registry already knows what every account statement MEANS -- it
//! validates `GRANT SELECT ON *.*`, refuses a `DROP USER` of an account that
//! is not there, decides that `CREATE USER IF NOT EXISTS` is a no-op. Deriving
//! the same rules a second time from the AST, in the persist layer, would be
//! two implementations of `GRANT` that drift.
//!
//! So the registry runs first -- but not the LIVE one. One account statement
//! is:
//!
//! 1. open one transaction and read the cluster's own `mysql.*` through it,
//!    building a *scratch* registry that is the cluster's current truth (not
//!    this node's possibly-stale copy);
//! 2. run the statement against that scratch registry, which validates it and
//!    applies its meaning;
//! 3. plan the row mutations that make `mysql.*` equal the scratch registry's
//!    new state, and commit them on that same transaction;
//! 4. only then publish the scratch registry into the node's live one, and
//!    announce the change on etcd.
//!
//! The failure invariant falls straight out of the order: nothing the node
//! serves from is touched until the 2PC has committed. A statement that fails
//! to validate never reaches storage; a statement whose commit is rejected --
//! including a write conflict with a Go TiDB that granted something at the
//! same time -- leaves the live registry exactly as it was, and the client is
//! told. There is no rollback path to get wrong, because there is nothing to
//! roll back.
//!
//! Reading the cluster at step 1 is also what makes the whole-image persist
//! safe: the diff is between the cluster's rows and those same rows plus this
//! statement, so it can never revert a change another node made.
//!
//! [`ClusterDdl`]: crate::cluster_session_node::ClusterDdl

use std::sync::Arc;
use std::time::Duration;

use tidb_exec::cluster_account_write::plan_account_write;
use tidb_exec::cluster_catalog::{load_cluster_catalog, ClusterCatalog};
use tidb_exec::cluster_privilege_load::{load_cluster_privileges, read_bootstrap_state};
use tidb_exec::mysql_bootstrap::utc_now_timestamp;
use tidb_exec::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_pd_client::EtcdClient;
use tidb_session::privilege::PrivilegeRegistry;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, ProductionOptimisticTransaction, RealOptimisticTransactionOpener,
    MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::cluster_privileges::{cluster_image_from_registry, registry_from_cluster};

/// This node's one route to the cluster's stored accounts.
///
/// The seam exists so the routing decision -- what a session does with an
/// account statement, and what happens when the persist fails -- is exercised
/// without a cluster. The production implementation is
/// [`RealClusterAccountWriter`].
pub trait ClusterAccountWriter: Send + Sync {
    /// Reads the cluster's accounts and hands back the scratch table one
    /// statement is to be applied to.
    fn begin(&self) -> Result<Box<dyn PendingAccountChange>, String>;
}

/// One account statement in flight: the cluster read that opened it, waiting
/// for the statement's effect to be persisted.
///
/// Dropping one without committing abandons the change, which is exactly what
/// a statement that failed to validate needs.
pub trait PendingAccountChange {
    /// The scratch account table the statement is to run against. It holds
    /// the cluster's accounts as of this change's own snapshot.
    fn registry(&self) -> PrivilegeRegistry;

    /// Persists whatever the statement did to [`Self::registry`], publishes it
    /// into the node's live table, and announces it.
    ///
    /// The `'user'@'host'` identities whose rows changed are answered for the
    /// log; an empty answer means the statement was a no-op the cluster
    /// already satisfied, and nothing was written or announced.
    fn commit(self: Box<Self>) -> Result<Vec<String>, String>;
}

/// The production account writer: one real transaction per statement, the
/// optimistic 2PC, then the live table and the etcd announcement.
pub struct RealClusterAccountWriter {
    opener: Arc<RealOptimisticTransactionOpener>,
    /// The node's LIVE account table -- the one every session, the login path
    /// and `SHOW GRANTS` already hold a clone of. Published into only after a
    /// commit.
    live: PrivilegeRegistry,
    timeout: Duration,
    /// The etcd client this node announces account changes through, so peers'
    /// privilege watches fire promptly. `None` leaves them to their reload
    /// tick; a failed announcement is a warning, never a failed statement.
    notifier: Option<Arc<EtcdClient>>,
}

impl RealClusterAccountWriter {
    /// Binds the writer to an already-connected authority and the live
    /// account table a successful change publishes into.
    #[must_use]
    pub fn new(
        opener: Arc<RealOptimisticTransactionOpener>,
        live: PrivilegeRegistry,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
    ) -> Self {
        Self {
            opener,
            live,
            timeout,
            notifier,
        }
    }
}

impl ClusterAccountWriter for RealClusterAccountWriter {
    fn begin(&self) -> Result<Box<dyn PendingAccountChange>, String> {
        let mut transaction = self
            .opener
            .begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
            .map_err(|error| error.to_string())?;
        let (catalog, scratch) = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            let bootstrap =
                read_bootstrap_state(&mut snapshot, &catalog).map_err(|error| error.to_string())?;
            if !bootstrap.already_bootstrapped() {
                return Err(format!(
                    "this node cannot change the accounts of a cluster that reports {bootstrap}"
                ));
            }
            let loaded =
                load_cluster_privileges(&mut snapshot, &catalog).map_err(|e| e.to_string())?;
            // Writing the table back writes what this node modelled, so a row
            // it could not model would be dropped from the cluster. Refusing
            // is the only honest answer, and it names what stopped it.
            let unwritable = crate::cluster_privileges::unwritable_account_rows(&loaded);
            if !unwritable.is_empty() {
                return Err(format!(
                    "this node cannot write the cluster's accounts back: {}",
                    unwritable.join("; ")
                ));
            }
            (catalog, registry_from_cluster(&loaded).registry)
        };
        Ok(Box::new(RealPendingAccountChange {
            transaction: Some(transaction),
            catalog,
            scratch,
            live: self.live.clone(),
            timeout: self.timeout,
            notifier: self.notifier.clone(),
        }))
    }
}

struct RealPendingAccountChange {
    /// `None` only after [`PendingAccountChange::commit`] has taken it.
    transaction: Option<ProductionOptimisticTransaction>,
    catalog: ClusterCatalog,
    scratch: PrivilegeRegistry,
    live: PrivilegeRegistry,
    timeout: Duration,
    notifier: Option<Arc<EtcdClient>>,
}

impl PendingAccountChange for RealPendingAccountChange {
    fn registry(&self) -> PrivilegeRegistry {
        self.scratch.clone()
    }

    fn commit(mut self: Box<Self>) -> Result<Vec<String>, String> {
        let mut transaction = self
            .transaction
            .take()
            .ok_or_else(|| "the account change was already committed".to_owned())?;
        let desired = cluster_image_from_registry(&self.scratch);
        let plan = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            plan_account_write(&mut snapshot, &self.catalog, &desired, utc_now_timestamp())
                .map_err(|error| error.to_string())?
        };
        if plan.is_empty() {
            // A statement the cluster already satisfies -- `CREATE USER IF NOT
            // EXISTS` for an account that exists, a `GRANT` of a privilege
            // already held -- spends no commit and announces nothing, exactly
            // as an `IF EXISTS` DDL no-op does.
            transaction
                .finish_without_writes()
                .map_err(|error| error.to_string())?;
            return Ok(Vec::new());
        }
        let changed = plan.changed_users;
        let call = UnaryCallContext::with_timeout(self.timeout);
        match transaction
            .commit(plan.mutations, &call)
            .map_err(|error| error.to_string())?
        {
            OptimisticCommitOutcome::Committed(_) => {}
            other => {
                return Err(format!(
                    "the account change was not committed: {:?}",
                    other.state()
                ))
            }
        }
        // Published only now: every clone of the live table -- every open
        // session, the login path -- sees the change the instant it is durable
        // in the cluster, and never before.
        self.live.replace_from(&self.scratch);
        notify_privilege_update(self.notifier.as_deref(), &changed);
        Ok(changed)
    }
}

/// Announces a committed account change, downgrading every failure to a
/// warning.
///
/// Go's `Domain.notifyUpdatePrivilege` logs and carries on when the PUT fails
/// for the same reason this does: the rows are already durable, and every
/// reader's own reload interval still finds them. Only a committed change is
/// ever announced.
pub fn notify_privilege_update(notifier: Option<&EtcdClient>, changed: &[String]) {
    let Some(notifier) = notifier else {
        return;
    };
    let users = serde_json::to_string(changed).unwrap_or_else(|_| "[]".to_owned());
    match notifier.notify_privilege_update() {
        Ok(()) => eprintln!("{{\"event\":\"privilege_update_notified\",\"users\":{users}}}"),
        Err(error) => eprintln!(
            "{{\"event\":\"privilege_update_notify_failed\",\"level\":\"warning\",\"users\":{users},\"error\":{}}}",
            serde_json::to_string(&error.to_string()).unwrap_or_else(|_| "\"unprintable\"".to_owned())
        ),
    }
}
