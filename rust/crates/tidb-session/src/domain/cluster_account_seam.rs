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
//! This is [`crate::domain::cluster_session_node`]'s [`ClusterDdl`] seam's twin, and
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
//!    (`crate::user_table`, the SQL Go's executors issue);
//! 3. after the commit, [`ClusterAccountWriter::notify_update_privilege`]
//!    reloads the node's live table from the tables and announces the change
//!    on etcd.
//!
//! Nothing the node serves from is touched before the commit: a statement
//! that fails to validate or to commit leaves the live table as it was.
//!
//! [`ClusterDdl`]: crate::domain::cluster_session_node::ClusterDdl

use std::sync::Arc;
use std::time::Duration;
use tidb_txnkv::transaction::StoreClient;

use tidb_exec::cluster_account_write::{plan_account_write, AccountWriteError};
use tidb_exec::cluster_auto_id::ClusterAutoIdStore;
use tidb_exec::cluster_catalog::{load_cluster_catalog, ClusterCatalog};
use tidb_exec::cluster_privilege_load::{load_cluster_privileges, read_bootstrap_state};
use tidb_exec::mysql_bootstrap::utc_now_timestamp;
use tidb_exec::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_executor::kv_table::AutoIdStore;
use tidb_pd_client::EtcdClient;
use crate::privilege::PrivilegeRegistry;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;

use crate::domain::cluster_privileges::{cluster_image_from_registry, registry_from_cluster};
use crate::domain::session_api::{cluster_commit_error, SqlQueryError};

/// Reserves `count` `_tidb_rowid`s of a `mysql.*` table in their own
/// transaction (Go's autoid allocator, `RunInNewTxn`), never below
/// `minimum`, answering the first. Shared by every system-table writer.
pub fn reserve_system_row_ids<S>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    db: i64,
    table: &tidb_model::TableInfo,
    minimum: i64,
    count: usize,
) -> Result<i64, String>
where
    S: StoreClient,
{
    reserve_account_ids(opener, timeout, db, table, minimum, count)
        .map_err(|error| error.to_string())
}

fn reserve_account_ids<S>(
    opener: &RealOptimisticTransactionOpener<S>,
    timeout: Duration,
    db: i64,
    table: &tidb_model::TableInfo,
    minimum: i64,
    count: usize,
) -> Result<i64, AccountWriteError>
where
    S: StoreClient,
{
    let store = ClusterAutoIdStore::new(opener.clone(), db, table, timeout);
    let failure = |error: tidb_executor::kv_table::AutoIdStoreError| {
        AccountWriteError::Unsupported(format!("account row-ID reservation: {error}"))
    };
    // Preserve bootstrap rows that predate a persisted allocator watermark.
    store
        .rebase(minimum.max(0) as u64, false)
        .map_err(failure)?;
    let (base, end) = store.reserve(count as u64, false).map_err(failure)?;
    if end - base != count as u64 || end > i64::MAX as u64 {
        return Err(AccountWriteError::Unsupported(
            "account row-ID range exhausted".to_owned(),
        ));
    }
    Ok((base + 1) as i64)
}

/// This node's one route to the cluster's stored accounts.
///
/// The seam exists so the routing decision -- what a session does with an
/// account statement, and what happens when the persist fails -- is exercised
/// without a cluster. The production implementation is
/// [`RealClusterAccountWriter`].
pub trait ClusterAccountWriter: Send + Sync {
    /// The cluster's accounts as of now: what one statement validates against.
    fn load_accounts(&self) -> Result<PrivilegeRegistry, String>;
    /// Go `Domain.NotifyUpdatePrivilege` after a committed account statement:
    /// the live table reloads from the cluster's tables and peers are told.
    fn notify_update_privilege(&self) -> Result<(), String>;
    /// Announce a committed automatic lock/unlock; counter-only writes stay local.
    fn notify_login_lock_change(&self, _user: &str, _host: &str) {}
    /// Go `NotifyUpdateAllUsersPrivilege`'s etcd PUT after FLUSH PRIVILEGES:
    /// every peer's privilege watch reloads all accounts.
    fn notify_all_privileges_changed(&self) {}
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
    fn notify_login_lock_change(&self, user: &str, host: &str) {
        notify_privilege_update(self.notifier.as_deref(), &[format!("'{user}'@'{host}'")]);
    }

    fn notify_all_privileges_changed(&self) {
        notify_privilege_update(self.notifier.as_deref(), &[]);
    }

    fn load_accounts(&self) -> Result<PrivilegeRegistry, String> {
        let mut transaction = self.opener.begin().map_err(|error| error.to_string())?;
        let registry = {
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
            registry_from_cluster(&loaded).registry
        };
        transaction
            .finish_without_writes()
            .map_err(|error| error.to_string())?;
        Ok(registry)
    }

    fn notify_update_privilege(&self) -> Result<(), String> {
        let loaded = self.load_accounts()?;
        self.live.replace_from(&loaded);
        notify_privilege_update(self.notifier.as_deref(), &[]);
        Ok(())
    }
}

/// Announces a committed account change, downgrading every failure to a
/// warning.
///
/// Go's `Domain.notifyUpdatePrivilege` logs and carries on when the PUT fails
/// for the same reason this does: the rows are already durable, and every
/// reader's own reload interval still finds them. Only a committed change is
/// ever announced.
pub fn notify_privilege_update(notifier: Option<&EtcdClient>, _changed: &[String]) {
    let Some(notifier) = notifier else {
        return;
    };
    // Go `Domain.notifyUpdatePrivilege` is silent on success.
    if let Err(error) = notifier.notify_privilege_update() {
        tidb_util::logutil::bg_logger()
            .warn("notify update privilege failed", &[crate::domain::zap_error(error)]);
    }
}

/// Reconciles `live`'s accounts into the cluster's `mysql.user` at boot.
///
/// Go's bootstrap INSERTs `root@%` into `mysql.user` with an EMPTY
/// `authentication_string` (`bootstrap.go:493`), and the cluster is then the
/// single source of accounts: every later statement reads that set, edits it,
/// and writes it back. This port also provisions a startup identity from
/// `--auth-file`, WITH a password, into the node's live table only — so the
/// two disagreed about root's credential. The first `CREATE USER` opened its
/// scratch from the cluster, added one row, committed, and the live table was
/// then REPLACED with what the cluster held. Root's password reverted to the bootstrap's empty one and the
/// administrator could no longer authenticate: one `CREATE USER` locked the
/// server's owner out of it.
///
/// Writing the auth-file's accounts into `mysql.user` at boot restores Go's
/// invariant -- the credential a session authenticates against IS the
/// cluster's row -- so rewriting the set preserves it. `plan_account_write`
/// diffs, so a cluster that already agrees costs no commit.
///
/// This treats `live` as the WHOLE intended account set, which is true only
/// where the node's own `--auth-file` defines it over a store that starts
/// empty each boot (`--store unistore`). A node joining a populated cluster
/// must not call this: it would delete accounts it never knew about.
pub fn seed_cluster_accounts<S>(
    opener: &RealOptimisticTransactionOpener<S>,
    live: &PrivilegeRegistry,
    timeout: Duration,
) -> Result<Vec<String>, SqlQueryError>
where
    S: StoreClient,
{
    let mut transaction = opener
        .begin()
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    // `cluster_image_from_registry` reads through `export`, so the live table
    // is not disturbed by being written out.
    let desired = cluster_image_from_registry(live);
    let plan = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|e| SqlQueryError::unknown(e.to_string()))?;
        Some(
            plan_account_write(
                &mut snapshot,
                &catalog,
                &desired,
                utc_now_timestamp(),
                &mut |db, table, minimum, count| {
                    reserve_account_ids(opener, timeout, db, table, minimum, count)
                },
            )
            .map_err(|e| SqlQueryError::unknown(e.to_string()))?,
        )
    };
    let Some(plan) = plan.filter(|plan| !plan.is_empty()) else {
        transaction
            .finish_without_writes()
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        return Ok(Vec::new());
    };
    let changed = plan.changed_users;
    let call = UnaryCallContext::with_timeout(timeout);
    let outcome = transaction
        .commit(plan.mutations, &call)
        .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
    if let Some(error) = cluster_commit_error(&outcome, "account seeding") {
        return Err(error);
    }
    Ok(changed)
}

#[cfg(test)]
mod seed_tests {
    use crate::privilege::PrivilegeRegistry;

    /// The startup identity's CREDENTIAL, not merely its name, is what an
    /// account statement used to discard: the cluster's bootstrap row carries
    /// Go's empty `authentication_string`, so replacing the live table with
    /// the cluster's view reverted root's password and locked the owner out.
    ///
    /// `cluster_image_from_registry` is the exact projection
    /// [`seed_cluster_accounts`] writes, so asserting the password survives it
    /// pins the half that broke, without needing a cluster to write into.
    #[test]
    fn the_seeded_image_carries_the_startup_password() {
        let registry = PrivilegeRegistry::bootstrapped_from([(
            "root".to_owned(),
            "%".to_owned(),
            "*61CAB5BD10C9AE60B985E8821DCBCBA0125FE290".to_owned(),
        )]);
        let image = super::cluster_image_from_registry(&registry);
        let root = image
            .users
            .iter()
            .find(|user| user.user == "root" && user.host == "%")
            .expect("the startup identity is in the image");
        assert_eq!(
            root.authentication_string, "*61CAB5BD10C9AE60B985E8821DCBCBA0125FE290",
            "the password must reach mysql.user, or the next account statement reverts it"
        );
    }
}
