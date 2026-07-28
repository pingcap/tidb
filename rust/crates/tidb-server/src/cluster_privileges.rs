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

//! Turning a cluster's `mysql.*` rows into this node's live account table.
//!
//! [`tidb_exec::cluster_privilege_load`] reads the rows; this turns them into
//! the one [`PrivilegeRegistry`] that the login path, `SHOW GRANTS`, and every
//! privilege check share. The two halves are deliberately separate crates:
//! `tidb-exec` owns cluster reads and has no idea what a registry is, and this
//! crate owns the registry and never speaks to TiKV.
//!
//! What crosses the bridge is exactly what this node can act on. A privilege
//! name the registry does not model — a dynamic privilege it has never heard
//! of, a table-scope privilege outside its grant vocabulary — is collected as
//! a named skip rather than dropped silently or turned into a startup failure:
//! a node that refuses to start because a Go TiDB granted `RESTRICTED_TABLES_ADMIN`
//! is useless, and one that silently swallows it is dishonest.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::{Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_exec::cluster_privilege_load::{ClusterPrivileges, LoadedUser};
use tidb_exec::real_tikv_privileges::{load_accounts_from_cluster, ClusterAccounts};
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;

use tidb_session::privilege::{is_dynamic_privilege, Account, GlobalPriv, PrivilegeRegistry};

use crate::auth_identity::DEFAULT_AUTH_PLUGIN;

/// One privilege a cluster row granted that this node does not model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkippedGrant {
    /// `mysql.<table>` the row came from.
    pub source: &'static str,
    /// The account the row named, as `'user'@'host'`.
    pub account: String,
    /// The privilege name, exactly as stored.
    pub privilege: String,
}

impl SkippedGrant {
    fn new(source: &'static str, user: &str, host: &str, privilege: &str) -> Self {
        Self {
            source,
            account: format!("'{user}'@'{host}'"),
            privilege: privilege.to_owned(),
        }
    }
}

/// What one cluster privilege load produced.
#[derive(Clone, Debug)]
pub struct LoadedRegistry {
    /// The live account table, ready to be shared with the session factory.
    pub registry: PrivilegeRegistry,
    /// Accounts loaded, including roles.
    pub account_count: usize,
    /// Grants named by the cluster that this node does not model.
    pub skipped: Vec<SkippedGrant>,
}

/// Builds one live account table from a cluster's `mysql.*` rows.
///
/// Go's loader keeps a `mysql.user` row's privileges on the row itself; this
/// registry stores an account's global mask, so the two are the same state
/// reached differently. A row whose `User` is empty is Go's anonymous-user
/// row, which this node's host-matching login path has no way to select, so it
/// is not created at all rather than created and never reachable.
#[must_use]
pub fn registry_from_cluster(loaded: &ClusterPrivileges) -> LoadedRegistry {
    let registry = PrivilegeRegistry::bootstrapped_from(Vec::new());
    let mut skipped = Vec::new();
    let mut account_count = 0;

    for user in &loaded.users {
        if user.user.is_empty() {
            continue;
        }
        create_account(&registry, user);
        account_count += 1;
        let mut mask = 0u64;
        for privilege in &user.privileges {
            match GlobalPriv::from_grant_name(privilege) {
                Some(global) => mask |= priv_mask(global),
                None => skipped.push(SkippedGrant::new(
                    "mysql.user",
                    &user.user,
                    &user.host,
                    privilege,
                )),
            }
        }
        registry.grant(&user.user, &user.host, mask);
    }

    for grant in &loaded.db_grants {
        let mut mask = 0u64;
        for privilege in &grant.privileges {
            match GlobalPriv::from_grant_name(privilege) {
                Some(global) => mask |= priv_mask(global),
                None => skipped.push(SkippedGrant::new(
                    "mysql.db",
                    &grant.user,
                    &grant.host,
                    privilege,
                )),
            }
        }
        registry.grant_db(&grant.user, &grant.host, &grant.database, mask);
    }

    for grant in &loaded.table_grants {
        let mask = set_mask(
            "mysql.tables_priv",
            &grant.user,
            &grant.host,
            &grant.privileges,
            &mut skipped,
        );
        registry.grant_table(
            &grant.user,
            &grant.host,
            &grant.database,
            &grant.table,
            mask,
        );
    }

    for grant in &loaded.column_grants {
        let mask = set_mask(
            "mysql.columns_priv",
            &grant.user,
            &grant.host,
            &grant.privileges,
            &mut skipped,
        );
        registry.grant_column(
            &grant.user,
            &grant.host,
            &grant.database,
            &grant.table,
            &grant.column,
            mask,
        );
    }

    for grant in &loaded.dynamic_grants {
        if is_dynamic_privilege(&grant.privilege) {
            registry.grant_dynamic(
                &grant.user,
                &grant.host,
                &grant.privilege,
                grant.with_grant_option,
            );
        } else {
            skipped.push(SkippedGrant::new(
                "mysql.global_grants",
                &grant.user,
                &grant.host,
                &grant.privilege,
            ));
        }
    }

    for edge in &loaded.role_edges {
        registry.grant_role(
            &account(&edge.role_user, &edge.role_host),
            &account(&edge.grantee_user, &edge.grantee_host),
        );
    }

    // Go stores one `mysql.default_roles` row per default role, and
    // `SET DEFAULT ROLE` replaces every row for the account. Grouping the
    // rows back into one set per account before writing is what keeps
    // `set_default_roles`'s replace semantics from making each row erase the
    // one before it.
    let mut grouped: Vec<(Account, Vec<Account>)> = Vec::new();
    for default in &loaded.default_roles {
        let owner = account(&default.user, &default.host);
        let role = account(&default.role_user, &default.role_host);
        match grouped.iter_mut().find(|(existing, _)| *existing == owner) {
            Some((_, roles)) => roles.push(role),
            None => grouped.push((owner, vec![role])),
        }
    }
    for (owner, roles) in grouped {
        registry.set_default_roles(&owner, &roles);
    }

    LoadedRegistry {
        registry,
        account_count,
        skipped,
    }
}

/// Creates one account row, preserving the distinction Go encodes in
/// `Account_locked`: a locked, passwordless row IS a role.
fn create_account(registry: &PrivilegeRegistry, user: &LoadedUser) {
    if user.account_locked && user.authentication_string.is_empty() {
        registry.create_role(&user.user, &user.host);
        return;
    }
    let plugin = if user.plugin.is_empty() {
        DEFAULT_AUTH_PLUGIN
    } else {
        user.plugin.as_str()
    };
    registry.create_user_with_plugin(&user.user, &user.host, &user.authentication_string, plugin);
    if user.account_locked {
        registry.set_locked(&user.user, &user.host, true);
    }
}

/// Resolves a `SET`-valued privilege list, naming anything unmodelled.
fn set_mask(
    source: &'static str,
    user: &str,
    host: &str,
    privileges: &[String],
    skipped: &mut Vec<SkippedGrant>,
) -> u64 {
    let mut mask = 0u64;
    for privilege in privileges {
        match GlobalPriv::from_grant_name(&set_element_grant_name(privilege)) {
            Some(global) => mask |= priv_mask(global),
            None => skipped.push(SkippedGrant::new(source, user, host, privilege)),
        }
    }
    mask
}

/// Spells one `SET` element the way `from_grant_name` resolves it.
///
/// `mysql.tables_priv`.`Table_priv` and `mysql.columns_priv`.`Column_priv`
/// declare their elements in title case (`Create View`, verified against a
/// live cluster's stored `FieldType.Elems`), so uppercasing is enough for all
/// but one: the element is spelled `Grant`, while the privilege it names is
/// `GRANT OPTION`.
fn set_element_grant_name(element: &str) -> String {
    let upper = element.to_uppercase();
    if upper == "GRANT" {
        return "GRANT OPTION".to_owned();
    }
    upper
}

fn priv_mask(global: GlobalPriv) -> u64 {
    global.mask()
}

fn account(user: &str, host: &str) -> Account {
    (user.to_owned(), host.to_owned())
}

/// Keeps a node's live [`PrivilegeRegistry`] following the cluster's own
/// `mysql.*` on a cadence, so a `GRANT`/`CREATE USER`/`DROP USER` a Go node
/// commits reaches this node without a restart.
///
/// Go source of truth: `pkg/domain/domain.go`'s privilege-reload goroutine,
/// which re-reads `mysql.*` on a `notifyupdateprivilege` etcd event and on its
/// own interval. This tier has no etcd notification wired up (the same
/// documented gap [`tidb_exec::catalog_watch`] states for the schema
/// catalog), so this is the lease-cadence backstop alone: a re-scan every
/// `interval`, diffed against nothing and simply applied whole via
/// [`PrivilegeRegistry::replace_from`].
///
/// One-shot loading (`--load-privileges`'s startup read,
/// [`crate::real_tikv_node::node_accounts`]) already reuses
/// [`load_accounts_from_cluster`] and [`registry_from_cluster`]; this
/// reloader is the same two calls run again on a timer against the SAME live
/// registry, rather than a new one the caller has to go re-wire in.
pub struct PrivilegeReloader {
    signal: Arc<(Mutex<PrivilegeReloadSignal>, Condvar)>,
    stats: Arc<PrivilegeReloadCounters>,
    worker: Option<JoinHandle<()>>,
}

#[derive(Debug, Default)]
struct PrivilegeReloadSignal {
    shutdown: bool,
}

#[derive(Debug, Default)]
struct PrivilegeReloadCounters {
    passes: AtomicU64,
    reloads: AtomicU64,
    failures: AtomicU64,
}

/// What the reload thread has done so far, for tests and for operators.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PrivilegeReloadStats {
    /// Passes that ran to a decision, successful or not.
    pub passes: u64,
    /// Passes that read a bootstrapped cluster and published its accounts.
    pub reloads: u64,
    /// Passes whose read failed, or found an unbootstrapped cluster; the
    /// previously published table stays in force.
    pub failures: u64,
}

/// Why a privilege reload thread could not be started or stopped.
#[derive(Debug)]
pub enum PrivilegeReloadError {
    /// A zero tick would spin the reload thread against PD without pause.
    ZeroInterval,
    /// The reload thread could not be created.
    Spawn(std::io::Error),
    /// The reload thread panicked.
    WorkerPanicked,
}

impl std::fmt::Display for PrivilegeReloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroInterval => formatter.write_str("privilege reload interval must be nonzero"),
            Self::Spawn(error) => {
                write!(formatter, "failed to spawn privilege reloader: {error}")
            }
            Self::WorkerPanicked => formatter.write_str("privilege reloader panicked"),
        }
    }
}

impl std::error::Error for PrivilegeReloadError {}

/// One reload pass's read step, answering one cluster snapshot or a reason it
/// could not be read.
///
/// Kept as an injectable closure -- Go's own reload loop separates "read one
/// snapshot" from "publish it" the same way -- so the thread's condvar/
/// shutdown machinery can run against a fake snapshot provider in tests,
/// without a real PD/TiKV connection.
pub type PrivilegeReloadRead = Box<dyn FnMut() -> Result<ClusterAccounts, String> + Send + 'static>;

impl PrivilegeReloader {
    /// Starts the reload thread ticking every `interval` against `registry`,
    /// reading through `opener`.
    ///
    /// `registry` is the node's LIVE table -- the same one every session, the
    /// login path, and `SHOW GRANTS` already hold a clone of. Every tick
    /// builds a throwaway registry from one fresh cluster snapshot and
    /// publishes it into `registry` with
    /// [`PrivilegeRegistry::replace_from`], which every existing clone
    /// observes immediately.
    pub fn spawn(
        registry: PrivilegeRegistry,
        opener: RealOptimisticTransactionOpener,
        interval: Duration,
        timeout: Duration,
    ) -> Result<Self, PrivilegeReloadError> {
        Self::spawn_with_read(
            registry,
            interval,
            Box::new(move || {
                load_accounts_from_cluster(&opener, timeout).map_err(|error| error.to_string())
            }),
        )
    }

    /// Starts the reload thread with an injectable read step; production
    /// code should use [`Self::spawn`], which supplies the real cluster read.
    pub fn spawn_with_read(
        registry: PrivilegeRegistry,
        interval: Duration,
        mut read: PrivilegeReloadRead,
    ) -> Result<Self, PrivilegeReloadError> {
        if interval.is_zero() {
            return Err(PrivilegeReloadError::ZeroInterval);
        }
        let signal = Arc::new((Mutex::new(PrivilegeReloadSignal::default()), Condvar::new()));
        let stats = Arc::new(PrivilegeReloadCounters::default());
        let worker_signal = Arc::clone(&signal);
        let worker_stats = Arc::clone(&stats);
        let worker = std::thread::Builder::new()
            .name("privilege-reloader".to_owned())
            .spawn(move || {
                let (lock, condvar) = &*worker_signal;
                loop {
                    // Waiting on the condvar rather than sleeping is what
                    // makes shutdown prompt: a stop does not wait out the
                    // interval.
                    let mut state = match lock.lock() {
                        Ok(state) => state,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if !state.shutdown {
                        state = match condvar.wait_timeout(state, interval) {
                            Ok((state, _)) => state,
                            Err(poisoned) => poisoned.into_inner().0,
                        };
                    }
                    let stopping = state.shutdown;
                    drop(state);
                    if stopping {
                        return;
                    }
                    run_one_privilege_reload_pass(&registry, read.as_mut(), &worker_stats);
                }
            })
            .map_err(PrivilegeReloadError::Spawn)?;
        Ok(Self {
            signal,
            stats,
            worker: Some(worker),
        })
    }

    /// What the thread has done so far.
    #[must_use]
    pub fn stats(&self) -> PrivilegeReloadStats {
        PrivilegeReloadStats {
            passes: self.stats.passes.load(Ordering::Acquire),
            reloads: self.stats.reloads.load(Ordering::Acquire),
            failures: self.stats.failures.load(Ordering::Acquire),
        }
    }

    /// Stops the thread and waits for it, reporting a panicking worker.
    ///
    /// Idempotent: [`Drop`] calls it, so an explicit call is only needed when
    /// the caller wants to observe the failure.
    pub fn shutdown(&mut self) -> Result<(), PrivilegeReloadError> {
        let (lock, condvar) = &*self.signal;
        {
            let mut state = match lock.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.shutdown = true;
        }
        condvar.notify_all();
        match self.worker.take() {
            Some(worker) => worker
                .join()
                .map_err(|_| PrivilegeReloadError::WorkerPanicked),
            None => Ok(()),
        }
    }
}

impl Drop for PrivilegeReloader {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Runs one pass: read one fresh cluster snapshot, build its registry, and
/// publish it -- or leave the currently published table alone on any
/// failure, exactly as Go's reload loop logs and retries.
///
/// An unbootstrapped cluster is treated the same as a read failure rather
/// than as "zero accounts": a cluster that was bootstrapped when this node
/// started and then somehow reports unbootstrapped is a read anomaly, not a
/// real account wipe, and publishing an empty table would lock out every
/// account this node currently admits.
fn run_one_privilege_reload_pass(
    registry: &PrivilegeRegistry,
    read: &mut dyn FnMut() -> Result<ClusterAccounts, String>,
    stats: &PrivilegeReloadCounters,
) {
    stats.passes.fetch_add(1, Ordering::AcqRel);
    match read() {
        Ok(accounts) if accounts.bootstrap.already_bootstrapped() => {
            let built = registry_from_cluster(&accounts.privileges);
            registry.replace_from(&built.registry);
            stats.reloads.fetch_add(1, Ordering::AcqRel);
        }
        Ok(accounts) => {
            stats.failures.fetch_add(1, Ordering::AcqRel);
            eprintln!(
                "{{\"event\":\"privilege_reload_failed\",\"error\":{:?}}}",
                format!("cluster reports {}", accounts.bootstrap)
            );
        }
        Err(error) => {
            stats.failures.fetch_add(1, Ordering::AcqRel);
            eprintln!("{{\"event\":\"privilege_reload_failed\",\"error\":{error:?}}}");
        }
    }
}

#[cfg(test)]
mod tests {
    use tidb_exec::cluster_privilege_load::{
        LoadedDbGrant, LoadedDefaultRole, LoadedDynamicGrant, LoadedRoleEdge, LoadedTableGrant,
    };

    use super::*;

    fn user(
        user: &str,
        host: &str,
        auth: &str,
        locked: bool,
        privs: Vec<&'static str>,
    ) -> LoadedUser {
        LoadedUser {
            host: host.to_owned(),
            user: user.to_owned(),
            authentication_string: auth.to_owned(),
            plugin: "mysql_native_password".to_owned(),
            account_locked: locked,
            password_expired: false,
            privileges: privs,
        }
    }

    #[test]
    fn a_cluster_account_arrives_with_the_credential_and_privileges_its_row_carried() {
        let loaded = ClusterPrivileges {
            users: vec![user(
                "bridge",
                "%",
                "*756304EBF9898407899144B374EECB2CC1B94597",
                false,
                vec!["SELECT", "SUPER"],
            )],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert_eq!(built.account_count, 1);
        assert!(built.skipped.is_empty());
        assert_eq!(
            built.registry.auth_string("bridge", "%").as_deref(),
            Some("*756304EBF9898407899144B374EECB2CC1B94597")
        );
        assert!(built
            .registry
            .has_global_priv("bridge", "%", GlobalPriv::Select));
        assert!(built
            .registry
            .has_global_priv("bridge", "%", GlobalPriv::Super));
        assert!(!built
            .registry
            .has_global_priv("bridge", "%", GlobalPriv::Delete));
    }

    #[test]
    fn both_a_role_and_a_locked_user_arrive_locked_out_of_login() {
        // Go stores a ROLE and an `ACCOUNT LOCK`ed user under the SAME
        // `mysql.user.account_locked = 'Y'`, and the registry keeps that one
        // flag rather than inventing a second one. Loading must therefore
        // reach the locked state by either construction path, and the two
        // rows stay distinguishable only by the credential they carry.
        let loaded = ClusterPrivileges {
            users: vec![
                user("analyst", "%", "", true, Vec::new()),
                user("frozen", "%", "*ABC", true, Vec::new()),
            ],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert!(built.registry.is_role("analyst", "%"));
        assert!(built.registry.is_role("frozen", "%"));
        assert_eq!(
            built.registry.auth_string("analyst", "%").as_deref(),
            Some("")
        );
        assert_eq!(
            built.registry.auth_string("frozen", "%").as_deref(),
            Some("*ABC")
        );
    }

    #[test]
    fn the_anonymous_row_is_not_created_at_all() {
        // A `User = ''` row is Go's anonymous account; this node's host
        // matching cannot select it, so creating it would only leave a row
        // nothing can ever reach.
        let loaded = ClusterPrivileges {
            users: vec![user("", "localhost", "", false, Vec::new())],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert_eq!(built.account_count, 0);
        assert!(built.registry.accounts().is_empty());
    }

    #[test]
    fn a_table_grants_set_element_named_grant_becomes_grant_option() {
        // `mysql.tables_priv`.`Table_priv` spells it `Grant`, while the
        // privilege it names is `GRANT OPTION`; uppercasing alone would drop
        // it as unrecognized.
        let loaded = ClusterPrivileges {
            users: vec![user("bridge", "%", "", false, Vec::new())],
            table_grants: vec![LoadedTableGrant {
                host: "%".to_owned(),
                user: "bridge".to_owned(),
                database: "test".to_owned(),
                table: "rows".to_owned(),
                privileges: vec!["Select".to_owned(), "Grant".to_owned()],
            }],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert!(
            built.skipped.is_empty(),
            "unexpected skips: {:?}",
            built.skipped
        );
        assert!(built
            .registry
            .table_grant_row_exists("bridge", "%", "test", "rows"));
    }

    #[test]
    fn a_database_grant_arrives_at_database_scope_only() {
        let loaded = ClusterPrivileges {
            users: vec![user("bridge", "%", "", false, Vec::new())],
            db_grants: vec![LoadedDbGrant {
                host: "%".to_owned(),
                user: "bridge".to_owned(),
                database: "test".to_owned(),
                privileges: vec!["SELECT"],
            }],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert!(built.registry.db_grant_row_exists("bridge", "%", "test"));
        assert!(!built
            .registry
            .has_global_priv("bridge", "%", GlobalPriv::Select));
    }

    #[test]
    fn a_dynamic_privilege_this_node_does_not_model_is_named_rather_than_dropped() {
        let loaded = ClusterPrivileges {
            users: vec![user("bridge", "%", "", false, Vec::new())],
            dynamic_grants: vec![
                LoadedDynamicGrant {
                    host: "%".to_owned(),
                    user: "bridge".to_owned(),
                    privilege: "SYSTEM_VARIABLES_ADMIN".to_owned(),
                    with_grant_option: false,
                },
                LoadedDynamicGrant {
                    host: "%".to_owned(),
                    user: "bridge".to_owned(),
                    privilege: "NOT_A_REAL_PRIVILEGE".to_owned(),
                    with_grant_option: false,
                },
            ],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        assert!(built
            .registry
            .has_dynamic_priv("bridge", "%", "SYSTEM_VARIABLES_ADMIN", false));
        assert_eq!(built.skipped.len(), 1);
        assert_eq!(built.skipped[0].privilege, "NOT_A_REAL_PRIVILEGE");
        assert_eq!(built.skipped[0].account, "'bridge'@'%'");
    }

    #[test]
    fn every_default_role_row_for_one_account_survives_the_replace() {
        // `set_default_roles` replaces an account's whole set, so writing one
        // stored row at a time would leave only the last.
        let loaded = ClusterPrivileges {
            users: vec![
                user("bridge", "%", "", false, Vec::new()),
                user("r1", "%", "", true, Vec::new()),
                user("r2", "%", "", true, Vec::new()),
            ],
            role_edges: vec![
                LoadedRoleEdge {
                    role_host: "%".to_owned(),
                    role_user: "r1".to_owned(),
                    grantee_host: "%".to_owned(),
                    grantee_user: "bridge".to_owned(),
                },
                LoadedRoleEdge {
                    role_host: "%".to_owned(),
                    role_user: "r2".to_owned(),
                    grantee_host: "%".to_owned(),
                    grantee_user: "bridge".to_owned(),
                },
            ],
            default_roles: vec![
                LoadedDefaultRole {
                    host: "%".to_owned(),
                    user: "bridge".to_owned(),
                    role_host: "%".to_owned(),
                    role_user: "r1".to_owned(),
                },
                LoadedDefaultRole {
                    host: "%".to_owned(),
                    user: "bridge".to_owned(),
                    role_host: "%".to_owned(),
                    role_user: "r2".to_owned(),
                },
            ],
            ..ClusterPrivileges::default()
        };
        let built = registry_from_cluster(&loaded);
        let account = ("bridge".to_owned(), "%".to_owned());
        assert_eq!(built.registry.default_roles(&account).len(), 2);
        assert_eq!(built.registry.granted_roles(&account).len(), 2);
    }

    use std::sync::mpsc;
    use std::time::Instant;

    use tidb_exec::cluster_privilege_load::ClusterBootstrapState;
    use tidb_exec::real_tikv_privileges::ClusterAccounts;

    fn bootstrapped(privileges: ClusterPrivileges) -> ClusterAccounts {
        ClusterAccounts {
            bootstrap: ClusterBootstrapState::Bootstrapped { version: Some(1) },
            privileges,
        }
    }

    fn snapshot_with_users(users: Vec<LoadedUser>) -> ClusterAccounts {
        bootstrapped(ClusterPrivileges {
            users,
            ..ClusterPrivileges::default()
        })
    }

    #[test]
    fn a_zero_interval_is_refused_rather_than_spinning() {
        let registry = PrivilegeRegistry::bootstrapped_from(Vec::new());
        let result = PrivilegeReloader::spawn_with_read(
            registry,
            Duration::ZERO,
            Box::new(|| Ok(snapshot_with_users(Vec::new()))),
        );
        assert!(matches!(result, Err(PrivilegeReloadError::ZeroInterval)));
    }

    #[test]
    fn a_reload_picks_up_a_new_user_a_new_grant_a_revoke_and_a_drop() {
        let registry = PrivilegeRegistry::bootstrapped_from(Vec::new());
        registry.create_user("stale", "%", "");
        registry.grant("stale", "%", GlobalPriv::Select.mask());

        let (sender, receiver) = mpsc::channel();
        let mut tick = 0u32;
        let mut reloader = PrivilegeReloader::spawn_with_read(
            registry.clone(),
            Duration::from_millis(5),
            Box::new(move || {
                tick += 1;
                let snapshot = match tick {
                    // Pass 1: `new_user` arrives with SELECT, `stale` is
                    // dropped entirely -- this is what a cluster-side
                    // `CREATE USER new_user` + `GRANT SELECT` + `DROP USER
                    // stale` produces by the next tick.
                    1 => {
                        snapshot_with_users(vec![user("new_user", "%", "", false, vec!["SELECT"])])
                    }
                    // Pass 2: a `REVOKE SELECT` on `new_user` leaves it with
                    // no privileges.
                    _ => snapshot_with_users(vec![user("new_user", "%", "", false, Vec::new())]),
                };
                sender.send(tick).unwrap();
                Ok(snapshot)
            }),
        )
        .unwrap();

        assert_eq!(receiver.recv().unwrap(), 1);
        assert_eq!(receiver.recv().unwrap(), 2);
        reloader.shutdown().unwrap();

        assert!(registry.user_exists("new_user", "%"));
        assert!(!registry.has_global_priv("new_user", "%", GlobalPriv::Select));
        assert!(!registry.user_exists("stale", "%"));
        let stats = reloader.stats();
        assert_eq!(stats.reloads, 2);
        assert_eq!(stats.failures, 0);
    }

    #[test]
    fn an_unbootstrapped_read_is_counted_as_a_failure_and_keeps_the_previous_table() {
        let registry = PrivilegeRegistry::bootstrapped_from(Vec::new());
        registry.create_user("kept", "%", "");

        let mut reloader = PrivilegeReloader::spawn_with_read(
            registry.clone(),
            Duration::from_millis(5),
            Box::new(|| {
                Ok(ClusterAccounts {
                    bootstrap: ClusterBootstrapState::Unflagged,
                    privileges: ClusterPrivileges::default(),
                })
            }),
        )
        .unwrap();

        // Give the thread a few ticks to run, then confirm it never
        // published the unbootstrapped snapshot's empty table.
        std::thread::sleep(Duration::from_millis(60));
        reloader.shutdown().unwrap();

        assert!(registry.user_exists("kept", "%"));
        let stats = reloader.stats();
        assert!(stats.passes > 0);
        assert_eq!(stats.reloads, 0);
        assert_eq!(stats.failures, stats.passes);
    }

    #[test]
    fn dropping_the_reloader_stops_its_thread_promptly() {
        let registry = PrivilegeRegistry::bootstrapped_from(Vec::new());
        let (sender, receiver) = mpsc::channel();
        let reloader = PrivilegeReloader::spawn_with_read(
            registry,
            Duration::from_millis(5),
            Box::new(move || {
                let _ = sender.send(());
                Ok(snapshot_with_users(Vec::new()))
            }),
        )
        .unwrap();
        receiver.recv().unwrap();

        let stopping = Instant::now();
        drop(reloader);
        assert!(stopping.elapsed() < Duration::from_secs(5));

        while receiver.recv_timeout(Duration::from_millis(200)).is_ok() {}
        assert!(receiver.recv_timeout(Duration::from_millis(200)).is_err());
    }

    #[test]
    fn a_role_dropped_by_a_reload_stops_granting_though_a_session_still_names_it_active() {
        // Same rule `PrivilegeRegistry::replace_from` documents, exercised
        // through the reload thread rather than a direct call: Go's
        // `FindAllUserEffectiveRoles` never reaches into a live session to
        // deactivate a dropped role, it just stops finding it in the
        // reloaded role graph on the next check.
        let registry = PrivilegeRegistry::bootstrapped_from(Vec::new());
        registry.create_user("bridge", "%", "");
        registry.create_role("r1", "%");
        let bridge: Account = ("bridge".to_owned(), "%".to_owned());
        let role: Account = ("r1".to_owned(), "%".to_owned());
        registry.grant_role(&role, &bridge);
        registry.grant("r1", "%", GlobalPriv::Select.mask());
        let active_roles = vec![role];
        assert!(registry.has_global_priv_with_roles(
            "bridge",
            "%",
            &active_roles,
            GlobalPriv::Select
        ));

        let (sender, receiver) = mpsc::channel();
        let mut reloader = PrivilegeReloader::spawn_with_read(
            registry.clone(),
            Duration::from_millis(5),
            Box::new(move || {
                let _ = sender.send(());
                // `r1` is gone: no row, no edge -- a cluster-wide `DROP ROLE
                // r1`.
                Ok(snapshot_with_users(vec![user(
                    "bridge",
                    "%",
                    "",
                    false,
                    Vec::new(),
                )]))
            }),
        )
        .unwrap();
        receiver.recv().unwrap();
        reloader.shutdown().unwrap();

        assert!(!registry.has_global_priv_with_roles(
            "bridge",
            "%",
            &active_roles,
            GlobalPriv::Select
        ));
    }
}
