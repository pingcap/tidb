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
//! This is tidb-session's `domain::cluster_session_node` `ClusterDdl` seam's twin, and
//! it exists for the same reason: a `CREATE USER` run against the node's own
//! in-memory account table would change a *read* of somebody else's state and
//! answer `OK` to a client whose account does not exist anywhere.
//!
//! # What one account statement does, as Go's executors do it
//!
//! Go's `executeCreateUser`, `GrantExec` and the other account executors take
//! a system session, run their own `mysql.*` SQL in one `BEGIN PESSIMISTIC`
//! .. `COMMIT`, then call `Domain.NotifyUpdatePrivilege`, which reloads the
//! privilege cache from the tables and tells the peers. Here:
//!
//! 1. [`ClusterAccountWriter::load_accounts`] reads the cluster's accounts,
//!    which the session's executor validates the statement against and
//!    applies its meaning to (the registry knows what `GRANT SELECT ON *.*`
//!    and `CREATE USER IF NOT EXISTS` mean);
//! 2. the executor writes the statement's own rows on a system session
//!    (`tidb_executor::user_table`, the SQL Go's executors issue);
//! 3. after the commit, [`ClusterAccountWriter::notify_update_privilege`]
//!    reloads the node's live table from the tables and announces the change
//!    on etcd.
//!
//! Nothing the node serves from is touched before the commit: a statement
//! that fails to validate or to commit leaves the live table as it was.
//!
//! `ClusterDdl`: tidb_ddl::worker::ClusterDdl

use std::sync::Arc;
use std::time::Duration;
use tidb_store_driver_txn::transaction::StoreClient;

use tidb_infoschema::cluster_catalog::load_info_schema;
use tidb_privilege::cluster_privilege_load::{load_cluster_privileges, read_bootstrap_state};
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_pd_client::EtcdClient;
use tidb_privilege::PrivilegeRegistry;

use crate::cluster_privileges::{priv_reload_event, PrivilegeEvent};
use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;

use tidb_privilege::load::registry_from_cluster;

/// This node's one route to the cluster's stored accounts.
///
/// The seam exists so the routing decision -- what a session does with an
/// account statement, and what happens when the persist fails -- is exercised
/// without a cluster. The production implementation is
/// [`RealClusterAccountWriter`].
pub trait ClusterAccountWriter: Send + Sync {
    /// The cluster's accounts as of now: what one statement validates against.
    fn load_accounts(&self) -> Result<PrivilegeRegistry, String>;
    /// Go `Domain.NotifyUpdatePrivilege(users)` after a committed account
    /// statement: peers are told and the live table reloads those users.
    fn notify_update_privilege(&self, users: &[String]) -> Result<(), String>;
    /// Go `NotifyUpdateAllUsersPrivilege` after FLUSH PRIVILEGES: peers are
    /// told and the live table reloads every user.
    fn notify_all_privileges_changed(&self) -> Result<(), String>;
}

/// The production account writer: reads of the cluster's tables, the live
/// table's reload and the etcd announcement.
pub struct RealClusterAccountWriter<S = tikv_client::PdRpcClient>
where
    S: StoreClient,
{
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    /// The node's LIVE account table -- the one every session, the login path
    /// and `SHOW GRANTS` already hold a clone of. Reloaded only after a
    /// commit.
    live: PrivilegeRegistry,
    timeout: Duration,
    /// The etcd client this node announces account changes through, so peers'
    /// privilege watches fire promptly. `None` leaves them to their reload
    /// tick; a failed announcement is a warning, never a failed statement.
    notifier: Option<Arc<EtcdClient>>,
}

impl<S> RealClusterAccountWriter<S>
where
    S: StoreClient,
{
    /// Binds the writer to an already-connected authority and the live
    /// account table a successful change reloads.
    #[must_use]
    pub fn new(
        opener: Arc<RealOptimisticTransactionOpener<S>>,
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

impl<S> ClusterAccountWriter for RealClusterAccountWriter<S>
where
    S: StoreClient,
{
    fn notify_all_privileges_changed(&self) -> Result<(), String> {
        self.notify_update(&PrivilegeEvent {
            all: true,
            ..PrivilegeEvent::default()
        })
    }

    fn load_accounts(&self) -> Result<PrivilegeRegistry, String> {
        let mut transaction = self.opener.begin().map_err(|error| error.to_string())?;
        let registry = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            let catalog = load_info_schema(&mut snapshot).map_err(|error| error.to_string())?;
            let bootstrap =
                read_bootstrap_state(&mut snapshot, &catalog).map_err(|error| error.to_string())?;
            if !bootstrap.already_bootstrapped() {
                return Err(format!(
                    "this node cannot change the accounts of a cluster that reports {bootstrap}"
                ));
            }
            let loaded =
                load_cluster_privileges(&mut snapshot, &catalog).map_err(|e| e.to_string())?;
            registry_from_cluster(&loaded).registry
        };
        transaction
            .finish_without_writes()
            .map_err(|error| error.to_string())?;
        Ok(registry)
    }

    fn notify_update_privilege(&self, users: &[String]) -> Result<(), String> {
        self.notify_update(&PrivilegeEvent {
            user_list: users.to_vec(),
            ..PrivilegeEvent::default()
        })
    }
}

impl<S> RealClusterAccountWriter<S>
where
    S: StoreClient,
{
    /// Go `Domain.notifyUpdatePrivilege`: the etcd announcement, then
    /// `privReloadEvent` on this node's handle. This node allocates no
    /// global server ID, so its events carry Go's zero `ServerID`.
    fn notify_update(&self, event: &PrivilegeEvent) -> Result<(), String> {
        notify_privilege_update(self.notifier.as_deref(), event);
        priv_reload_event(&self.live, event)
    }
}

/// Announces a committed account change, downgrading every failure to a
/// warning.
///
/// Go's `Domain.notifyUpdatePrivilege` logs and carries on when the PUT fails
/// for the same reason this does: the rows are already durable, and every
/// reader's own reload interval still finds them. Only a committed change is
/// ever announced.
pub fn notify_privilege_update(notifier: Option<&EtcdClient>, event: &PrivilegeEvent) {
    let Some(notifier) = notifier else {
        return;
    };
    // Go `Domain.notifyUpdatePrivilege` is silent on success.
    if let Err(error) = notifier.notify_privilege_update(&event.encode()) {
        tidb_logutil::bg_logger()
            .warn("notify update privilege failed", &[tidb_logutil::zap_error(error)]);
    }
}

