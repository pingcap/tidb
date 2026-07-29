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
    OptimisticCommitOutcome, ProductionOptimisticTransaction, RealOptimisticTransactionOpener,
    MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

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
    fn commit(self: Box<Self>) -> Result<Vec<String>, String>;
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
    ) -> Self {
        Self {
            opener,
            live,
            timeout,
            notifier,
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
            (catalog, GlobalSysvars::from_cluster_rows(loaded))
        };
        Ok(Box::new(RealPendingSysvarChange {
            transaction: Some(transaction),
            catalog,
            scratch,
            live: self.live.clone(),
            timeout: self.timeout,
            notifier: self.notifier.clone(),
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
}

impl PendingSysvarChange for RealPendingSysvarChange {
    fn table(&self) -> GlobalSysvars {
        self.scratch.clone()
    }

    fn commit(mut self: Box<Self>) -> Result<Vec<String>, String> {
        let mut transaction = self
            .transaction
            .take()
            .ok_or_else(|| "the sysvar change was already committed".to_owned())?;
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
                .map_err(|error| error.to_string())?
        };
        if plan.is_empty() {
            transaction
                .finish_without_writes()
                .map_err(|error| error.to_string())?;
            return Ok(Vec::new());
        }
        let changed = plan.changed;
        let call = UnaryCallContext::with_timeout(self.timeout);
        match transaction
            .commit(plan.mutations, &call)
            .map_err(|error| error.to_string())?
        {
            OptimisticCommitOutcome::Committed(_) => {}
            other => {
                return Err(format!(
                    "the sysvar change was not committed: {:?}",
                    other.state()
                ))
            }
        }
        // Published only now: every clone of the live table sees the change
        // the instant it is durable in the cluster, and never before.
        self.live.replace_from(&self.scratch);
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
    ) -> Result<Self, SysvarReloadError> {
        Self::spawn_with_read(
            live,
            interval,
            Box::new(move || {
                let mut transaction = opener
                    .begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
                    .map_err(|error| error.to_string())?;
                let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
                let catalog =
                    load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
                load_cluster_sysvars(&mut snapshot, &catalog).map_err(|error| error.to_string())
            }),
        )
    }

    /// Starts the reload thread with an injectable read step; production
    /// code should use [`Self::spawn`], which supplies the real cluster read.
    pub fn spawn_with_read(
        live: GlobalSysvars,
        interval: Duration,
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
                    match read() {
                        Ok(rows) => {
                            live.replace_from(&GlobalSysvars::from_cluster_rows(rows));
                            worker_stats.reloads.fetch_add(1, Ordering::AcqRel);
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
            Box::new(move || {
                read_calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![("max_connections".to_owned(), "500".to_owned())])
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
        assert_eq!(live.get("max_connections").as_deref(), Ok("500"));
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
            Box::new(|| Err("cluster unreachable".to_owned())),
        )
        .expect("a nonzero interval spawns");
        std::thread::sleep(StdDuration::from_millis(30));
        reloader.shutdown().expect("shutdown joins cleanly");
        assert_eq!(live.get("autocommit").as_deref(), Ok("OFF"));
        assert!(reloader.stats().failures >= 1);
    }

    #[test]
    fn zero_interval_is_refused() {
        let live = GlobalSysvars::new();
        match SysvarReloader::spawn_with_read(live, Duration::ZERO, Box::new(|| Ok(Vec::new()))) {
            Err(SysvarReloadError::ZeroInterval) => {}
            Err(other) => panic!("wrong refusal reason: {other}"),
            Ok(_) => panic!("a zero interval must not spin the reload thread"),
        }
    }
}
