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

//! The route a `SET GLOBAL` statement takes when the cluster's stored
//! sysvars live in `mysql.global_variables`, not just in this process.
//!
//! This is [`crate::cluster_account_seam`]'s twin for the same reason: a
//! `SET GLOBAL` run only against this node's in-memory
//! [`tidb_session::vars::GlobalSysvars`] would answer `OK` to a client while
//! every other node in the cluster -- and this node itself, after a restart
//! -- never saw the change.
//!
//! # Ordering, mirroring the account seam exactly
//!
//! 1. open one transaction and read the cluster's own `mysql.global_variables`
//!    through it, building a *scratch* [`GlobalSysvars`] that is the
//!    cluster's current truth (not this node's possibly-stale copy);
//! 2. run the statement's assignments against that scratch table, which
//!    validates them (unknown name, wrong scope, wrong type/value) exactly as
//!    the in-process path already does;
//! 3. plan the row mutations that make `mysql.global_variables` equal the
//!    scratch table's new state, and commit them on that same transaction;
//! 4. only then publish the scratch table into the node's live one, and
//!    announce the change on etcd.
//!
//! The failure invariant is the same as the account seam's: nothing the node
//! serves reads from is touched until the 2PC has committed, and a statement
//! whose commit is rejected -- including a write conflict with a Go TiDB that
//! ran `SET GLOBAL` on the same variable at the same time -- leaves the live
//! table exactly as it was.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_exec::cluster_catalog::{load_cluster_catalog, ClusterCatalog};
use tidb_exec::cluster_sysvar_load::load_cluster_sysvars;
use tidb_exec::cluster_sysvar_write::plan_sysvar_write;
use tidb_exec::mysql_bootstrap::utc_now_timestamp;
use tidb_exec::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_pd_client::EtcdClient;
use tidb_session::vars::GlobalSysvars;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    ProductionOptimisticTransaction, RealOptimisticTransactionOpener, MAX_OPTIMISTIC_MUTATIONS,
    MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::sql_node::{cluster_commit_error, SqlQueryError};

/// This node's one route to the cluster's stored `SET GLOBAL` overrides.
///
/// The seam exists so the routing decision -- what a session does with a
/// `SET GLOBAL`, and what happens when the persist fails -- is exercised
/// without a cluster. The production implementation is
/// [`RealClusterSysvarWriter`].
pub trait ClusterSysvarWriter: Send + Sync {
    /// Reads the cluster's sysvar overrides and hands back the scratch table
    /// one statement is to be applied to.
    fn begin(&self) -> Result<Box<dyn PendingSysvarChange>, String>;
}

/// One `SET GLOBAL` in flight: the cluster read that opened it, waiting for
/// the statement's effect to be persisted.
///
/// Dropping one without committing abandons the change, which is exactly
/// what a statement that failed to validate needs.
pub trait PendingSysvarChange {
    /// The scratch sysvar table the statement is to run against. It holds
    /// the cluster's overrides as of this change's own snapshot.
    fn table(&self) -> GlobalSysvars;

    /// Persists whatever the statement did to [`Self::table`], publishes it
    /// into the node's live table, and announces it.
    ///
    /// The variable names this change touched are answered for the log; an
    /// empty answer means the statement stored the value the cluster already
    /// had (or reset a variable with no stored row), and nothing was written
    /// or announced.
    fn commit(self: Box<Self>) -> Result<Vec<String>, SqlQueryError>;
}

/// Orders publications into one node's live sysvar table.
///
/// A reload reads the cluster without holding this fence, then publishes only
/// if no local `SET GLOBAL` published while that read was in flight. This
/// keeps a slow, earlier cluster read from finishing after a local commit and
/// replacing the just-committed image with its stale snapshot.
#[derive(Clone, Default)]
pub struct SysvarPublicationFence {
    state: Arc<Mutex<SysvarPublicationState>>,
    secure_transport_floor: Arc<Mutex<SecureTransportFloor>>,
}

#[derive(Default)]
struct SysvarPublicationState {
    epoch: u64,
    local_commit_ts: std::collections::BTreeMap<String, u64>,
    instance_commit_ts: std::collections::BTreeMap<String, u64>,
}

#[derive(Default)]
struct SecureTransportFloor {
    pending_on_publications: u64,
}

impl SysvarPublicationFence {
    fn observed_epoch(&self) -> u64 {
        self.lock_state().epoch
    }

    fn prepublish_secure_transport_on(
        &self,
        live: &GlobalSysvars,
        scratch: &GlobalSysvars,
    ) -> bool {
        let mut floor = self.lock_secure_transport_floor();
        if scratch.get("require_secure_transport").as_deref() == Ok("ON") {
            floor.pending_on_publications = floor
                .pending_on_publications
                .checked_add(1)
                .expect("pending secure-transport publications overflowed");
            live.set("require_secure_transport", "ON".to_owned())
                .expect("require_secure_transport=ON is a validated global value");
            true
        } else {
            false
        }
    }

    fn publish_global_image(
        &self,
        live: &GlobalSysvars,
        fresh: &GlobalSysvars,
        completes_secure_transport_on: bool,
    ) {
        let mut floor = self.lock_secure_transport_floor();
        let remaining_pending_on_publications = floor
            .pending_on_publications
            .checked_sub(u64::from(completes_secure_transport_on))
            .expect("completed an unregistered secure-transport publication");
        if remaining_pending_on_publications != 0
            && fresh.get("require_secure_transport").as_deref() != Ok("ON")
        {
            fresh
                .set("require_secure_transport", "ON".to_owned())
                .expect("require_secure_transport=ON is a validated global value");
        }
        live.replace_from(fresh);
        floor.pending_on_publications = remaining_pending_on_publications;
    }

    fn finish_secure_transport_on(&self, registered: bool) {
        if !registered {
            return;
        }
        let mut floor = self.lock_secure_transport_floor();
        floor.pending_on_publications = floor
            .pending_on_publications
            .checked_sub(1)
            .expect("completed an unregistered secure-transport publication");
    }

    fn publish_reload_if_current(
        &self,
        observed_epoch: u64,
        live: &GlobalSysvars,
        fresh: &GlobalSysvars,
    ) -> bool {
        let mut state = self.lock_state();
        if state.epoch != observed_epoch {
            return false;
        }
        self.publish_global_image(live, fresh, false);
        state.epoch = state.epoch.wrapping_add(1);
        true
    }

    fn publish_local_after_commit_with_read(
        &self,
        live: &GlobalSysvars,
        scratch: &GlobalSysvars,
        global_changed: &[String],
        commit_ts: Option<u64>,
        read: impl FnOnce() -> Result<Vec<(String, String)>, String>,
    ) -> Option<String> {
        // Handshakes read the live table without taking this publication
        // fence. Once the durable commit has been confirmed, close that gate
        // before either this statement's best-effort reread or an earlier
        // publisher holding the fence can block us. Only ON is safe to
        // publish here: OFF must wait for a confirmed fresh image. The same
        // fail-closed rule intentionally lets an uncertain older ON
        // temporarily cover a newer OFF until a successful reread converges.
        let registered_secure_transport_on = self.prepublish_secure_transport_on(live, scratch);
        let mut state = self.lock_state();
        // Invalidate every reload that started before this committed change.
        // The lock stays held through the reread and fallback publication, so
        // no reload can observe the new epoch without also observing the new
        // live image.
        state.epoch = state.epoch.wrapping_add(1);
        remember_local_commits(&mut state, global_changed, commit_ts);
        match read() {
            Ok(rows) => {
                self.publish_global_image(
                    live,
                    &GlobalSysvars::from_cluster_rows(rows),
                    registered_secure_transport_on,
                );
            }
            Err(error) => {
                // A confirmed commit must still answer OK if this best-effort
                // cache rebuild fails. Publish this statement's durable keys
                // only; a late response from an older local commit must not
                // overwrite a newer value whose commit timestamp this fence
                // already observed.
                let publishable: Vec<String> = global_changed
                    .iter()
                    .filter(|name| {
                        let is_secure_transport =
                            name.eq_ignore_ascii_case("require_secure_transport");
                        let would_disable_secure_transport =
                            is_secure_transport && scratch.get(name).as_deref() != Ok("ON");
                        !would_disable_secure_transport
                            && commit_ts.is_none_or(|commit_ts| {
                                state
                                    .local_commit_ts
                                    .get(*name)
                                    .copied()
                                    .unwrap_or_default()
                                    <= commit_ts
                            })
                    })
                    .cloned()
                    .collect();
                live.publish_global_changes_from(scratch, &publishable);
                publish_instance_changes(&mut state, live, scratch, commit_ts);
                self.finish_secure_transport_on(registered_secure_transport_on);
                return Some(error);
            }
        }
        publish_instance_changes(&mut state, live, scratch, commit_ts);
        None
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, SysvarPublicationState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn lock_secure_transport_floor(&self) -> std::sync::MutexGuard<'_, SecureTransportFloor> {
        match self.secure_transport_floor.lock() {
            Ok(floor) => floor,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn publish_instance_changes(
    state: &mut SysvarPublicationState,
    live: &GlobalSysvars,
    scratch: &GlobalSysvars,
    commit_ts: Option<u64>,
) {
    let published = live.publish_instance_changes_from_if(scratch, |name| {
        commit_ts.is_none_or(|commit_ts| {
            state
                .instance_commit_ts
                .get(name)
                .copied()
                .unwrap_or_default()
                <= commit_ts
        })
    });
    let Some(commit_ts) = commit_ts else {
        return;
    };
    for name in published {
        state
            .instance_commit_ts
            .entry(name)
            .and_modify(|known| *known = (*known).max(commit_ts))
            .or_insert(commit_ts);
    }
}

fn remember_local_commits(
    state: &mut SysvarPublicationState,
    changed: &[String],
    commit_ts: Option<u64>,
) {
    let Some(commit_ts) = commit_ts else {
        return;
    };
    for name in changed {
        state
            .local_commit_ts
            .entry(name.clone())
            .and_modify(|known| *known = (*known).max(commit_ts))
            .or_insert(commit_ts);
    }
}

/// The production sysvar writer: one real transaction per statement, the
/// optimistic 2PC, then the live table and the etcd announcement.
pub struct RealClusterSysvarWriter {
    opener: Arc<RealOptimisticTransactionOpener>,
    /// The node's LIVE sysvar table -- the one every session shares a clone
    /// of. Published into only after a commit.
    live: GlobalSysvars,
    timeout: Duration,
    /// The etcd client this node announces sysvar changes through, so peers'
    /// sysvar watches fire promptly. `None` leaves them to their reload tick;
    /// a failed announcement is a warning, never a failed statement.
    notifier: Option<Arc<EtcdClient>>,
    publication_fence: SysvarPublicationFence,
}

impl RealClusterSysvarWriter {
    /// Binds the writer to an already-connected authority and the live
    /// sysvar table a successful change publishes into.
    #[must_use]
    pub fn new(
        opener: Arc<RealOptimisticTransactionOpener>,
        live: GlobalSysvars,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
        publication_fence: SysvarPublicationFence,
    ) -> Self {
        Self {
            opener,
            live,
            timeout,
            notifier,
            publication_fence,
        }
    }
}

impl ClusterSysvarWriter for RealClusterSysvarWriter {
    fn begin(&self) -> Result<Box<dyn PendingSysvarChange>, String> {
        let mut transaction = self
            .opener
            .begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
            .map_err(|error| error.to_string())?;
        let (catalog, scratch) = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            let loaded =
                load_cluster_sysvars(&mut snapshot, &catalog).map_err(|error| error.to_string())?;
            let scratch = GlobalSysvars::from_cluster_rows(loaded);
            (catalog, scratch)
        };
        Ok(Box::new(RealPendingSysvarChange {
            transaction: Some(transaction),
            catalog,
            scratch,
            live: self.live.clone(),
            timeout: self.timeout,
            notifier: self.notifier.clone(),
            publication_fence: self.publication_fence.clone(),
            reload_opener: Arc::clone(&self.opener),
        }))
    }
}

struct RealPendingSysvarChange {
    /// `None` only after [`PendingSysvarChange::commit`] has taken it.
    transaction: Option<ProductionOptimisticTransaction>,
    catalog: ClusterCatalog,
    scratch: GlobalSysvars,
    live: GlobalSysvars,
    timeout: Duration,
    notifier: Option<Arc<EtcdClient>>,
    publication_fence: SysvarPublicationFence,
    reload_opener: Arc<RealOptimisticTransactionOpener>,
}

impl PendingSysvarChange for RealPendingSysvarChange {
    fn table(&self) -> GlobalSysvars {
        self.scratch.clone()
    }

    fn commit(mut self: Box<Self>) -> Result<Vec<String>, SqlQueryError> {
        let mut transaction = self
            .transaction
            .take()
            .ok_or_else(|| SqlQueryError::unknown("the sysvar change was already committed"))?;
        // The scratch table's current overrides ARE the desired image: it was
        // seeded from this same snapshot's own stored rows and then the
        // statement ran against it, so a name the statement reset to DEFAULT
        // is simply absent here, and `plan_sysvar_write`'s whole-image diff
        // turns that absence into a delete of its stored row.
        let desired: std::collections::BTreeMap<String, String> =
            self.scratch.overrides().into_iter().collect();
        let plan = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            plan_sysvar_write(&mut snapshot, &self.catalog, &desired, utc_now_timestamp())
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?
        };
        let plan_is_empty = plan.is_empty();
        let changed = plan.changed;
        let commit_ts = if plan_is_empty {
            transaction
                .finish_without_writes()
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            None
        } else {
            let call = UnaryCallContext::with_timeout(self.timeout);
            let outcome = transaction
                .commit(plan.mutations, &call)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            if let Some(error) = cluster_commit_error(&outcome, "sysvar change") {
                return Err(error);
            }
            Some(outcome.receipt().commit_ts)
        };
        // Rebuild from durable cluster truth inside the same publication
        // boundary the periodic reloader uses. Two local commits may finish
        // in one order and reach this point in another; the fresh image makes
        // either arrival publish the latest cluster state. This reread is a
        // cache refresh after a confirmed commit, so its failure is warned
        // and never changes the client's successful SQL outcome.
        let refresh_error = self.publication_fence.publish_local_after_commit_with_read(
            &self.live,
            &self.scratch,
            &changed,
            commit_ts,
            || {
                tidb_exec::real_tikv_privileges::load_sysvars_from_cluster(
                    &self.reload_opener,
                    self.timeout,
                )
                .map_err(|error| error.to_string())
            },
        );
        if let Some(error) = refresh_error {
            eprintln!(
                "{{\"event\":\"sysvar_post_commit_refresh_failed\",\"level\":\"warning\",\"error\":{}}}",
                serde_json::to_string(&error).unwrap_or_else(|_| "\"unprintable\"".to_owned())
            );
        }
        notify_sysvar_update(self.notifier.as_deref(), &changed);
        Ok(changed)
    }
}

/// Announces a committed sysvar change, downgrading every failure to a
/// warning.
///
/// Go's `Domain.NotifyUpdateSysVarCache` logs and carries on when the PUT
/// fails for the same reason this does: the rows are already durable, and
/// every reader's own 30-second reload tick still finds them (see
/// `pkg/domain/domain.go` `LoadSysVarCacheLoop`). Only a committed change is
/// ever announced.
pub fn notify_sysvar_update(notifier: Option<&EtcdClient>, changed: &[String]) {
    let Some(notifier) = notifier else {
        return;
    };
    let names = serde_json::to_string(changed).unwrap_or_else(|_| "[]".to_owned());
    match notifier.notify_sysvar_update() {
        Ok(()) => eprintln!("{{\"event\":\"sysvar_update_notified\",\"variables\":{names}}}"),
        Err(error) => eprintln!(
            "{{\"event\":\"sysvar_update_notify_failed\",\"level\":\"warning\",\"variables\":{names},\"error\":{}}}",
            serde_json::to_string(&error.to_string()).unwrap_or_else(|_| "\"unprintable\"".to_owned())
        ),
    }
}

/// The background half of `mysql.global_variables` freshness: a thread that
/// periodically re-reads the whole table and republishes it into the node's
/// live [`GlobalSysvars`], plus an etcd-nudged wakeup.
///
/// Go source of truth: `pkg/domain/domain.go`'s `LoadSysVarCacheLoop`, which
/// selects between its own 30-second ticker and a `/tidb/sysvars` etcd watch
/// -- this is [`crate::cluster_privileges::PrivilegeReloader`]'s same
/// tick-plus-nudge shape, aimed at [`load_cluster_sysvars`] and
/// [`GlobalSysvars::replace_from`] instead of the account registry. Without
/// this, a `SET GLOBAL` a Go peer runs is durable in `mysql.global_variables`
/// the instant it commits but invisible to THIS node until it restarts; with
/// it, the node notices within one tick (or one round trip, if its own etcd
/// watch fires).
pub struct SysvarReloader {
    signal: Arc<(Mutex<SysvarReloadSignal>, Condvar)>,
    stats: Arc<SysvarReloadCounters>,
    worker: Option<JoinHandle<()>>,
}

#[derive(Debug, Default)]
struct SysvarReloadSignal {
    shutdown: bool,
    nudged: bool,
}

/// A handle the etcd sysvar watch uses to wake the reload thread before its
/// tick.
#[derive(Clone)]
pub struct SysvarReloadWaker {
    signal: Arc<(Mutex<SysvarReloadSignal>, Condvar)>,
}

impl SysvarReloadWaker {
    /// Asks for one reload pass as soon as the thread can run it.
    pub fn nudge(&self) {
        let (lock, condvar) = &*self.signal;
        {
            let mut state = match lock.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.nudged = true;
        }
        condvar.notify_all();
    }
}

#[derive(Debug, Default)]
struct SysvarReloadCounters {
    passes: AtomicU64,
    reloads: AtomicU64,
    failures: AtomicU64,
}

/// What the reload thread has done so far, for tests and for operators.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SysvarReloadStats {
    /// Passes that ran to a decision, successful or not.
    pub passes: u64,
    /// Passes that read the cluster and published its sysvars.
    pub reloads: u64,
    /// Passes whose read failed; the previously published table stays in
    /// force.
    pub failures: u64,
}

/// Why a sysvar reload thread could not be started or stopped.
#[derive(Debug)]
pub enum SysvarReloadError {
    /// A zero tick would spin the reload thread against PD without pause.
    ZeroInterval,
    /// The reload thread could not be created.
    Spawn(std::io::Error),
    /// The reload thread panicked.
    WorkerPanicked,
}

impl std::fmt::Display for SysvarReloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroInterval => formatter.write_str("sysvar reload interval must be nonzero"),
            Self::Spawn(error) => write!(formatter, "failed to spawn sysvar reloader: {error}"),
            Self::WorkerPanicked => formatter.write_str("sysvar reloader panicked"),
        }
    }
}

impl std::error::Error for SysvarReloadError {}

/// One reload pass's read step: the cluster's whole current sysvar image, or
/// a reason it could not be read. Injectable so the thread machinery can run
/// against a fake read in tests, without a real PD/TiKV connection.
pub type SysvarReloadRead =
    Box<dyn FnMut() -> Result<Vec<(String, String)>, String> + Send + 'static>;

impl SysvarReloader {
    /// Starts the reload thread ticking every `interval`, republishing into
    /// `live` (the node's LIVE table every session already holds a clone of).
    pub fn spawn(
        live: GlobalSysvars,
        opener: RealOptimisticTransactionOpener,
        interval: Duration,
        timeout: Duration,
        publication_fence: SysvarPublicationFence,
    ) -> Result<Self, SysvarReloadError> {
        Self::spawn_with_read(
            live,
            interval,
            publication_fence,
            Box::new(move || {
                tidb_exec::real_tikv_privileges::load_sysvars_from_cluster(&opener, timeout)
                    .map_err(|error| error.to_string())
            }),
        )
    }

    /// Starts the reload thread with an injectable read step; production
    /// code should use [`Self::spawn`], which supplies the real cluster read.
    pub fn spawn_with_read(
        live: GlobalSysvars,
        interval: Duration,
        publication_fence: SysvarPublicationFence,
        mut read: SysvarReloadRead,
    ) -> Result<Self, SysvarReloadError> {
        if interval.is_zero() {
            return Err(SysvarReloadError::ZeroInterval);
        }
        let signal = Arc::new((Mutex::new(SysvarReloadSignal::default()), Condvar::new()));
        let stats = Arc::new(SysvarReloadCounters::default());
        let worker_signal = Arc::clone(&signal);
        let worker_stats = Arc::clone(&stats);
        let worker = std::thread::Builder::new()
            .name("sysvar-reloader".to_owned())
            .spawn(move || {
                let (lock, condvar) = &*worker_signal;
                loop {
                    let mut state = match lock.lock() {
                        Ok(state) => state,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    if !state.shutdown && !state.nudged {
                        state = match condvar.wait_timeout(state, interval) {
                            Ok((state, _)) => state,
                            Err(poisoned) => poisoned.into_inner().0,
                        };
                    }
                    let stopping = state.shutdown;
                    state.nudged = false;
                    drop(state);
                    if stopping {
                        return;
                    }
                    worker_stats.passes.fetch_add(1, Ordering::AcqRel);
                    let observed_epoch = publication_fence.observed_epoch();
                    match read() {
                        Ok(rows) => {
                            let fresh = GlobalSysvars::from_cluster_rows(rows);
                            if publication_fence.publish_reload_if_current(
                                observed_epoch,
                                &live,
                                &fresh,
                            ) {
                                worker_stats.reloads.fetch_add(1, Ordering::AcqRel);
                            }
                        }
                        Err(_) => {
                            worker_stats.failures.fetch_add(1, Ordering::AcqRel);
                        }
                    }
                }
            })
            .map_err(SysvarReloadError::Spawn)?;
        Ok(Self {
            signal,
            stats,
            worker: Some(worker),
        })
    }

    /// A handle the etcd sysvar watch uses to wake this thread.
    #[must_use]
    pub fn waker(&self) -> SysvarReloadWaker {
        SysvarReloadWaker {
            signal: Arc::clone(&self.signal),
        }
    }

    /// What the thread has done so far.
    #[must_use]
    pub fn stats(&self) -> SysvarReloadStats {
        SysvarReloadStats {
            passes: self.stats.passes.load(Ordering::Acquire),
            reloads: self.stats.reloads.load(Ordering::Acquire),
            failures: self.stats.failures.load(Ordering::Acquire),
        }
    }

    /// Stops the thread and waits for it, reporting a panicking worker.
    ///
    /// Idempotent: [`Drop`] calls it, so an explicit call is only needed when
    /// the caller wants to observe the failure.
    pub fn shutdown(&mut self) -> Result<(), SysvarReloadError> {
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
            Some(worker) => worker.join().map_err(|_| SysvarReloadError::WorkerPanicked),
            None => Ok(()),
        }
    }
}

impl Drop for SysvarReloader {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod reloader_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration as StdDuration;

    #[test]
    fn a_reload_pass_publishes_a_fresh_read_into_the_live_table() {
        let live = GlobalSysvars::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let read_calls = Arc::clone(&calls);
        let mut reloader = SysvarReloader::spawn_with_read(
            live.clone(),
            StdDuration::from_millis(5),
            SysvarPublicationFence::default(),
            Box::new(move || {
                read_calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![(
                    "require_secure_transport".to_owned(),
                    "ON".to_owned(),
                )])
            }),
        )
        .expect("a nonzero interval spawns");
        // Wait for at least one pass rather than a fixed sleep: the interval
        // is short, but a loaded CI box can still be slower than one tick.
        let deadline = std::time::Instant::now() + StdDuration::from_secs(2);
        while calls.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(StdDuration::from_millis(2));
        }
        reloader.shutdown().expect("shutdown joins cleanly");
        assert!(calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
        assert!(reloader.stats().reloads >= 1);
    }

    #[test]
    fn a_failed_read_leaves_the_live_table_untouched() {
        let live = GlobalSysvars::new();
        live.set("autocommit", "OFF".to_owned())
            .expect("a known GLOBAL-scoped variable sets cleanly");
        let mut reloader = SysvarReloader::spawn_with_read(
            live.clone(),
            StdDuration::from_millis(5),
            SysvarPublicationFence::default(),
            Box::new(|| Err("cluster unreachable".to_owned())),
        )
        .expect("a nonzero interval spawns");
        let deadline = std::time::Instant::now() + StdDuration::from_secs(2);
        while reloader.stats().failures == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(StdDuration::from_millis(2));
        }
        reloader.shutdown().expect("shutdown joins cleanly");
        assert_eq!(live.get("autocommit").as_deref(), Ok("OFF"));
        assert!(reloader.stats().failures >= 1);
    }

    #[test]
    fn zero_interval_is_refused() {
        let live = GlobalSysvars::new();
        match SysvarReloader::spawn_with_read(
            live,
            Duration::ZERO,
            SysvarPublicationFence::default(),
            Box::new(|| Ok(Vec::new())),
        ) {
            Err(SysvarReloadError::ZeroInterval) => {}
            Err(other) => panic!("wrong refusal reason: {other}"),
            Ok(_) => panic!("a zero interval must not spin the reload thread"),
        }
    }

    #[test]
    fn a_reload_read_started_before_a_local_commit_cannot_publish_after_it() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let (read_started_tx, read_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_read_tx, release_read_rx) = std::sync::mpsc::sync_channel(0);
        let mut reloader = SysvarReloader::spawn_with_read(
            live.clone(),
            StdDuration::from_secs(60),
            fence.clone(),
            Box::new(move || {
                read_started_tx
                    .send(())
                    .expect("the test observes the in-flight stale read");
                release_read_rx
                    .recv()
                    .expect("the local commit releases the stale read");
                Ok(vec![(
                    "require_secure_transport".to_owned(),
                    "OFF".to_owned(),
                )])
            }),
        )
        .expect("a nonzero interval spawns");

        reloader.waker().nudge();
        read_started_rx
            .recv_timeout(StdDuration::from_secs(2))
            .expect("the nudged reload starts its cluster read");
        let committed = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);
        let refresh_error = fence.publish_local_after_commit_with_read(
            &live,
            &committed,
            &["require_secure_transport".to_owned()],
            Some(10),
            || {
                Ok(vec![(
                    "require_secure_transport".to_owned(),
                    "ON".to_owned(),
                )])
            },
        );
        assert!(refresh_error.is_none());
        release_read_tx
            .send(())
            .expect("the stale read is still waiting");
        reloader.shutdown().expect("shutdown joins the stale pass");

        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
        assert_eq!(reloader.stats().reloads, 0);
    }

    #[test]
    fn secure_transport_on_is_live_before_the_post_commit_reread_finishes() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let committed = GlobalSysvars::from_cluster_rows([
            ("require_secure_transport".to_owned(), "ON".to_owned()),
            ("autocommit".to_owned(), "OFF".to_owned()),
        ]);
        let live_for_publish = live.clone();
        let fence_for_publish = fence.clone();
        let (read_started_tx, read_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_read_tx, release_read_rx) = std::sync::mpsc::sync_channel(0);

        let publisher = std::thread::spawn(move || {
            fence_for_publish.publish_local_after_commit_with_read(
                &live_for_publish,
                &committed,
                &[
                    "require_secure_transport".to_owned(),
                    "autocommit".to_owned(),
                ],
                Some(10),
                || {
                    read_started_tx
                        .send(())
                        .expect("the test observes the post-commit reread");
                    release_read_rx
                        .recv()
                        .expect("the test releases the post-commit reread");
                    Ok(vec![
                        ("require_secure_transport".to_owned(), "ON".to_owned()),
                        ("autocommit".to_owned(), "OFF".to_owned()),
                    ])
                },
            )
        });

        read_started_rx
            .recv_timeout(StdDuration::from_secs(2))
            .expect("the post-commit reread starts");
        let secure_transport_while_reading = live.get("require_secure_transport");
        let autocommit_while_reading = live.get("autocommit");
        release_read_tx
            .send(())
            .expect("the post-commit reread is still waiting");
        let warning = publisher.join().expect("the publisher does not panic");

        assert!(warning.is_none());
        assert_eq!(secure_transport_while_reading.as_deref(), Ok("ON"));
        assert_eq!(
            autocommit_while_reading.as_deref(),
            Ok("ON"),
            "ordinary globals wait for the fresh durable image"
        );
        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
        assert_eq!(live.get("autocommit").as_deref(), Ok("OFF"));
    }

    #[test]
    fn secure_transport_on_prepublishes_while_an_earlier_refresh_holds_the_fence() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let first_scratch = GlobalSysvars::new();
        let live_for_first = live.clone();
        let fence_for_first = fence.clone();
        let (first_read_started_tx, first_read_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_first_read_tx, release_first_read_rx) = std::sync::mpsc::sync_channel(0);
        let first_publisher = std::thread::spawn(move || {
            fence_for_first.publish_local_after_commit_with_read(
                &live_for_first,
                &first_scratch,
                &[],
                None,
                || {
                    first_read_started_tx
                        .send(())
                        .expect("the test observes the first post-commit reread");
                    release_first_read_rx
                        .recv()
                        .expect("the test releases the first post-commit reread");
                    Ok(vec![(
                        "require_secure_transport".to_owned(),
                        "OFF".to_owned(),
                    )])
                },
            )
        });
        first_read_started_rx
            .recv_timeout(StdDuration::from_secs(2))
            .expect("the first publisher holds the publication fence");

        let second_scratch = GlobalSysvars::from_cluster_rows([
            ("require_secure_transport".to_owned(), "ON".to_owned()),
            ("autocommit".to_owned(), "OFF".to_owned()),
        ]);
        let live_for_second = live.clone();
        let fence_for_second = fence.clone();
        let second_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let second_called_from_thread = Arc::clone(&second_called);
        let (second_read_started_tx, second_read_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_second_read_tx, release_second_read_rx) = std::sync::mpsc::sync_channel(0);
        let second_publisher = std::thread::spawn(move || {
            second_called_from_thread.store(true, Ordering::SeqCst);
            fence_for_second.publish_local_after_commit_with_read(
                &live_for_second,
                &second_scratch,
                &[
                    "require_secure_transport".to_owned(),
                    "autocommit".to_owned(),
                ],
                Some(20),
                || {
                    second_read_started_tx
                        .send(())
                        .expect("the test observes the second post-commit reread");
                    release_second_read_rx
                        .recv()
                        .expect("the test releases the second post-commit reread");
                    Ok(vec![
                        ("require_secure_transport".to_owned(), "ON".to_owned()),
                        ("autocommit".to_owned(), "OFF".to_owned()),
                    ])
                },
            )
        });

        let called_deadline = std::time::Instant::now() + StdDuration::from_secs(2);
        while !second_called.load(Ordering::SeqCst) && std::time::Instant::now() < called_deadline {
            std::thread::yield_now();
        }
        assert!(
            second_called.load(Ordering::SeqCst),
            "the second confirmed commit reaches publication"
        );
        let publish_deadline = std::time::Instant::now() + StdDuration::from_secs(2);
        let mut ordinary_while_secure = None;
        while std::time::Instant::now() < publish_deadline {
            if live.get("require_secure_transport").as_deref() == Ok("ON") {
                ordinary_while_secure = Some(live.get("autocommit"));
                break;
            }
            std::thread::yield_now();
        }

        release_first_read_tx
            .send(())
            .expect("the first post-commit reread is still waiting");
        second_read_started_rx
            .recv_timeout(StdDuration::from_secs(2))
            .expect("the second publisher starts after the first stale image publishes");
        assert_eq!(
            live.get("require_secure_transport").as_deref(),
            Ok("ON"),
            "the first stale OFF image cannot lower a confirmed ON while the second reread waits",
        );
        assert_eq!(
            live.get("autocommit").as_deref(),
            Ok("ON"),
            "the second statement's ordinary global still waits for durable truth",
        );
        release_second_read_tx
            .send(())
            .expect("the second post-commit reread is still waiting");
        assert!(first_publisher
            .join()
            .expect("the first publisher does not panic")
            .is_none());
        assert!(second_publisher
            .join()
            .expect("the second publisher does not panic")
            .is_none());

        assert_eq!(
            ordinary_while_secure
                .expect("secure transport is live before releasing the first reread")
                .as_deref(),
            Ok("ON"),
            "ordinary globals do not prepublish with the secure gate"
        );
        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
        assert_eq!(live.get("autocommit").as_deref(), Ok("OFF"));
    }

    #[test]
    fn secure_transport_off_waits_for_the_post_commit_reread() {
        let live = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);
        let fence = SysvarPublicationFence::default();
        let newer_off = GlobalSysvars::new();
        let live_for_newer = live.clone();
        let fence_for_newer = fence.clone();
        let (newer_read_started_tx, newer_read_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_newer_read_tx, release_newer_read_rx) = std::sync::mpsc::sync_channel(0);
        let newer_publisher = std::thread::spawn(move || {
            fence_for_newer.publish_local_after_commit_with_read(
                &live_for_newer,
                &newer_off,
                &["require_secure_transport".to_owned()],
                Some(20),
                || {
                    newer_read_started_tx
                        .send(())
                        .expect("the test observes the newer post-commit reread");
                    release_newer_read_rx
                        .recv()
                        .expect("the test releases the newer post-commit reread");
                    Ok(Vec::new())
                },
            )
        });

        newer_read_started_rx
            .recv_timeout(StdDuration::from_secs(2))
            .expect("the newer post-commit reread starts");
        let secure_transport_during_off_read = live.get("require_secure_transport");
        release_newer_read_tx
            .send(())
            .expect("the newer post-commit reread is still waiting");
        assert!(newer_publisher
            .join()
            .expect("the newer publisher does not panic")
            .is_none());
        assert_eq!(
            secure_transport_during_off_read.as_deref(),
            Ok("ON"),
            "OFF waits for the confirmed fresh image before opening the gate"
        );
        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("OFF"));
    }

    #[test]
    fn local_commits_from_the_same_old_image_merge_their_distinct_changes() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let first = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);
        let second =
            GlobalSysvars::from_cluster_rows([("autocommit".to_owned(), "OFF".to_owned())]);

        assert!(fence
            .publish_local_after_commit_with_read(
                &live,
                &first,
                &["require_secure_transport".to_owned()],
                Some(10),
                || {
                    Ok(vec![(
                        "require_secure_transport".to_owned(),
                        "ON".to_owned(),
                    )])
                },
            )
            .is_none());
        assert!(fence
            .publish_local_after_commit_with_read(
                &live,
                &second,
                &["autocommit".to_owned()],
                Some(20),
                || {
                    Ok(vec![
                        ("require_secure_transport".to_owned(), "ON".to_owned()),
                        ("autocommit".to_owned(), "OFF".to_owned()),
                    ])
                },
            )
            .is_none());

        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
        assert_eq!(live.get("autocommit").as_deref(), Ok("OFF"));
    }

    #[test]
    fn an_older_local_commit_publishing_last_rereads_the_newer_durable_value() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let older = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);
        let newer = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "OFF".to_owned(),
        )]);
        let durable = || {
            Ok(vec![(
                "require_secure_transport".to_owned(),
                "OFF".to_owned(),
            )])
        };

        assert!(fence
            .publish_local_after_commit_with_read(
                &live,
                &newer,
                &["require_secure_transport".to_owned()],
                Some(20),
                durable,
            )
            .is_none());
        assert!(fence
            .publish_local_after_commit_with_read(
                &live,
                &older,
                &["require_secure_transport".to_owned()],
                Some(10),
                durable,
            )
            .is_none());

        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("OFF"));
    }

    #[test]
    fn a_noop_local_set_refreshes_a_stale_live_table() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let scratch = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);

        let refresh_error =
            fence.publish_local_after_commit_with_read(&live, &scratch, &[], None, || {
                Ok(vec![(
                    "require_secure_transport".to_owned(),
                    "ON".to_owned(),
                )])
            });
        assert!(refresh_error.is_none());

        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
    }

    #[test]
    fn a_mixed_global_and_instance_set_publishes_both_after_the_durable_reread() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let scratch = GlobalSysvars::from_cluster_rows([]);
        scratch
            .set("autocommit", "OFF".to_owned())
            .expect("the GLOBAL assignment validates");
        scratch
            .set("tidb_general_log", "ON".to_owned())
            .expect("SET GLOBAL admits an instance-only variable");

        let warning = fence.publish_local_after_commit_with_read(
            &live,
            &scratch,
            &["autocommit".to_owned()],
            Some(10),
            || Ok(vec![("autocommit".to_owned(), "OFF".to_owned())]),
        );

        assert!(warning.is_none());
        assert_eq!(live.get("autocommit").as_deref(), Ok("OFF"));
        assert_eq!(live.get("tidb_general_log").as_deref(), Ok("ON"));
    }

    #[test]
    fn a_mixed_global_and_instance_set_publishes_both_when_the_reread_fails() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let scratch = GlobalSysvars::from_cluster_rows([]);
        scratch
            .set("autocommit", "OFF".to_owned())
            .expect("the GLOBAL assignment validates");
        scratch
            .set("tidb_general_log", "ON".to_owned())
            .expect("SET GLOBAL admits an instance-only variable");

        let warning = fence.publish_local_after_commit_with_read(
            &live,
            &scratch,
            &["autocommit".to_owned()],
            Some(10),
            || Err("post-commit read unavailable".to_owned()),
        );

        assert_eq!(warning.as_deref(), Some("post-commit read unavailable"));
        assert_eq!(live.get("autocommit").as_deref(), Ok("OFF"));
        assert_eq!(live.get("tidb_general_log").as_deref(), Ok("ON"));
    }

    #[test]
    fn an_instance_only_noop_commit_still_publishes_the_instance_change() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let scratch = GlobalSysvars::from_cluster_rows([]);
        scratch
            .set("tidb_general_log", "ON".to_owned())
            .expect("SET GLOBAL admits an instance-only variable");

        let warning =
            fence.publish_local_after_commit_with_read(&live, &scratch, &[], None, || {
                Ok(Vec::new())
            });

        assert!(warning.is_none());
        assert_eq!(live.get("tidb_general_log").as_deref(), Ok("ON"));
    }

    #[test]
    fn repeated_instance_changes_publish_in_statement_order() {
        let live = GlobalSysvars::new();
        live.set_instance("tidb_general_log", "ON".to_owned())
            .expect("the live instance value sets");
        let fence = SysvarPublicationFence::default();
        let scratch = GlobalSysvars::from_cluster_rows([]);
        scratch
            .set("tidb_general_log", "ON".to_owned())
            .expect("the first assignment validates");
        scratch
            .reset("tidb_general_log")
            .expect("the later DEFAULT validates");

        let warning =
            fence.publish_local_after_commit_with_read(&live, &scratch, &[], None, || {
                Ok(Vec::new())
            });

        assert!(warning.is_none());
        assert_eq!(live.get("tidb_general_log").as_deref(), Ok("OFF"));
    }

    #[test]
    fn an_older_mixed_commit_publishing_last_cannot_overwrite_a_newer_instance_value() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let older = GlobalSysvars::from_cluster_rows([]);
        older
            .set("tidb_general_log", "ON".to_owned())
            .expect("the older instance assignment validates");
        let newer = GlobalSysvars::from_cluster_rows([]);
        newer
            .set("tidb_general_log", "OFF".to_owned())
            .expect("the newer instance assignment validates");

        assert!(fence
            .publish_local_after_commit_with_read(&live, &newer, &[], Some(20), || Ok(Vec::new()))
            .is_none());
        assert!(fence
            .publish_local_after_commit_with_read(&live, &older, &[], Some(10), || Ok(Vec::new()))
            .is_none());

        assert_eq!(live.get("tidb_general_log").as_deref(), Ok("OFF"));
    }

    #[test]
    fn refresh_failure_publishes_changed_secure_transport_on_and_fences_stale_reload() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let stale_reload_epoch = fence.observed_epoch();
        let scratch = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);

        let warning = fence.publish_local_after_commit_with_read(
            &live,
            &scratch,
            &["require_secure_transport".to_owned()],
            Some(10),
            || Err("post-commit read unavailable".to_owned()),
        );

        assert_eq!(warning.as_deref(), Some("post-commit read unavailable"));
        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
        let stale = GlobalSysvars::new();
        assert!(!fence.publish_reload_if_current(stale_reload_epoch, &live, &stale,));
        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
    }

    #[test]
    fn refresh_failure_on_noop_secure_transport_fails_closed() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let scratch = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);

        let warning =
            fence.publish_local_after_commit_with_read(&live, &scratch, &[], None, || {
                Err("post-noop read unavailable".to_owned())
            });

        assert_eq!(warning.as_deref(), Some("post-noop read unavailable"));
        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
    }

    #[test]
    fn refresh_failure_publishes_an_ordinary_changed_global() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let scratch =
            GlobalSysvars::from_cluster_rows([("autocommit".to_owned(), "OFF".to_owned())]);

        let warning = fence.publish_local_after_commit_with_read(
            &live,
            &scratch,
            &["autocommit".to_owned()],
            Some(10),
            || Err("post-commit read unavailable".to_owned()),
        );

        assert_eq!(warning.as_deref(), Some("post-commit read unavailable"));
        assert_eq!(live.get("autocommit").as_deref(), Ok("OFF"));
    }

    #[test]
    fn refresh_failure_does_not_let_an_older_commit_overwrite_a_newer_fallback() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let newer = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "OFF".to_owned(),
        )]);
        let older = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);

        assert!(fence
            .publish_local_after_commit_with_read(
                &live,
                &newer,
                &["require_secure_transport".to_owned()],
                Some(20),
                || Err("post-commit read unavailable".to_owned()),
            )
            .is_some());
        assert!(fence
            .publish_local_after_commit_with_read(
                &live,
                &older,
                &["require_secure_transport".to_owned()],
                Some(10),
                || Err("post-commit read unavailable".to_owned()),
            )
            .is_some());

        assert_eq!(
            live.get("require_secure_transport").as_deref(),
            Ok("ON"),
            "an uncertain fallback never weakens the secure-transport gate"
        );
    }

    #[test]
    fn refresh_failure_with_an_unrelated_change_still_publishes_durable_secure_transport_on() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let scratch = GlobalSysvars::from_cluster_rows([
            ("require_secure_transport".to_owned(), "ON".to_owned()),
            ("autocommit".to_owned(), "OFF".to_owned()),
        ]);

        let warning = fence.publish_local_after_commit_with_read(
            &live,
            &scratch,
            &["autocommit".to_owned()],
            Some(10),
            || Err("post-commit read unavailable".to_owned()),
        );

        assert!(warning.is_some());
        assert_eq!(live.get("autocommit").as_deref(), Ok("OFF"));
        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
    }

    #[test]
    fn refresh_failure_from_an_old_local_off_cannot_undo_a_peer_reload_of_on() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let old_local_scratch = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "OFF".to_owned(),
        )]);
        let peer_on = GlobalSysvars::from_cluster_rows([(
            "require_secure_transport".to_owned(),
            "ON".to_owned(),
        )]);
        let reload_epoch = fence.observed_epoch();
        assert!(fence.publish_reload_if_current(reload_epoch, &live, &peer_on,));

        let warning = fence.publish_local_after_commit_with_read(
            &live,
            &old_local_scratch,
            &["require_secure_transport".to_owned()],
            Some(10),
            || Err("post-commit read unavailable".to_owned()),
        );

        assert!(warning.is_some());
        assert_eq!(live.get("require_secure_transport").as_deref(), Ok("ON"));
    }

    #[test]
    fn refresh_failure_skips_a_future_unknown_global_without_panicking() {
        let live = GlobalSysvars::new();
        let fence = SysvarPublicationFence::default();
        let scratch = GlobalSysvars::new();

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            fence.publish_local_after_commit_with_read(
                &live,
                &scratch,
                &["future_version_sysvar".to_owned()],
                Some(10),
                || Err("post-commit read unavailable".to_owned()),
            )
        }));

        let warning = outcome.expect("an unknown future sysvar must not panic this node");
        assert_eq!(warning.as_deref(), Some("post-commit read unavailable"));
        assert!(live.overrides().is_empty());
    }
}
