// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Keeping a node's loaded catalog current while it serves queries.
//!
//! Go source of truth: `pkg/domain/domain.go`'s lease-driven reload goroutine
//! and `pkg/infoschema/issyncer`'s `Syncer.Reload`. Go ticks at `lease/2` so
//! that a node is never more than one lease behind; the same cadence is used
//! here, with the interval left configurable because this tier has no DDL
//! owner telling it what the lease is.
//!
//! # Consistency
//!
//! The published catalog is one `Arc`, replaced whole. A reader takes one
//! `Arc` clone at the start of a statement and holds it for that statement's
//! lifetime, so a statement is planned and executed against exactly one schema
//! version even if a reload lands mid-flight; the reload's own snapshot read
//! never observes a partially applied catalog either (see
//! [`crate::catalog_reload`]).
//!
//! What this tier does NOT do, and Go does: Go pairs the reload loop with a
//! *schema validator* holding a lease. A transaction that started at schema
//! version `v` and commits after a conflicting DDL is rejected with
//! `ErrInfoSchemaChanged`/`ErrInfoSchemaExpired` (error 8027) rather than
//! silently committing against a stale plan. This tier has no validator: a
//! statement that began just before a DDL runs to completion against its own
//! snapshot's catalog. That is safe for the read path (the snapshot's data
//! matches the snapshot's schema) and is a real gap for writes, which is why
//! error 8027 is deliberately deferred rather than approximated.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::cluster_catalog::ClusterCatalog;

/// The catalog every query reads, replaced whole by the reload thread.
///
/// A reader's [`load`](SharedCatalog::load) hands back an owned `Arc`, so a
/// statement in flight keeps the exact catalog it started with and a publish
/// never has to wait for readers.
pub struct SharedCatalog {
    published: RwLock<Arc<ClusterCatalog>>,
}

impl SharedCatalog {
    /// Publishes an initial catalog, normally the node's startup full load.
    #[must_use]
    pub fn new(catalog: ClusterCatalog) -> Self {
        Self {
            published: RwLock::new(Arc::new(catalog)),
        }
    }

    /// The catalog in force now. A poisoned lock still yields the value: a
    /// panicking publisher cannot leave the node unable to answer queries.
    #[must_use]
    pub fn load(&self) -> Arc<ClusterCatalog> {
        match self.published.read() {
            Ok(guard) => Arc::clone(&guard),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        }
    }

    /// Replaces the published catalog atomically.
    pub fn store(&self, catalog: ClusterCatalog) {
        let mut guard = match self.published.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = Arc::new(catalog);
    }
}

/// Why a reload thread could not be started or stopped.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogReloadError {
    /// A zero tick would spin the reload thread against PD without pause.
    ZeroInterval,
    /// The reload thread could not be created.
    Spawn(String),
    /// The reload thread panicked.
    WorkerPanicked,
}

impl std::fmt::Display for CatalogReloadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroInterval => formatter.write_str("catalog reload interval must be nonzero"),
            Self::Spawn(message) => {
                write!(formatter, "failed to spawn catalog reloader: {message}")
            }
            Self::WorkerPanicked => formatter.write_str("catalog reloader panicked"),
        }
    }
}

impl std::error::Error for CatalogReloadError {}

/// Counts of what the reload thread has done, for tests and for operators.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CatalogReloadStats {
    /// Passes that ran to a decision, successful or not.
    pub passes: u64,
    /// Passes that found the cluster still at the loaded version.
    pub unchanged: u64,
    /// Passes that published a catalog advanced by replaying diffs.
    pub diff_reloads: u64,
    /// Passes that published a freshly re-read catalog.
    pub full_reloads: u64,
    /// Passes whose read failed; the previous catalog stays published.
    pub failures: u64,
}

#[derive(Debug, Default)]
struct StatCounters {
    passes: AtomicU64,
    unchanged: AtomicU64,
    diff_reloads: AtomicU64,
    full_reloads: AtomicU64,
    failures: AtomicU64,
}

impl StatCounters {
    fn snapshot(&self) -> CatalogReloadStats {
        CatalogReloadStats {
            passes: self.passes.load(Ordering::Acquire),
            unchanged: self.unchanged.load(Ordering::Acquire),
            diff_reloads: self.diff_reloads.load(Ordering::Acquire),
            full_reloads: self.full_reloads.load(Ordering::Acquire),
            failures: self.failures.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Default)]
struct ReloadSignal {
    shutdown: bool,
}

/// One reload pass, as the caller performs it.
///
/// The closure is handed the currently published catalog and answers what to
/// publish. `Unchanged` and an error both leave the published catalog alone.
/// Keeping the storage access outside this module is what lets the thread be
/// tested without PD or TiKV.
pub type ReloadPass =
    Box<dyn FnMut(&ClusterCatalog) -> Result<CatalogReloadPass, String> + Send + 'static>;

/// The outcome one reload pass reports back to the thread.
#[derive(Clone, Debug)]
pub enum CatalogReloadPass {
    /// The cluster is still at the published version.
    Unchanged,
    /// Publish this catalog; it was reached by replaying diffs.
    Diffs(ClusterCatalog),
    /// Publish this catalog; it was re-read whole.
    Full(ClusterCatalog),
}

/// The running reload thread. Dropping it stops and joins the thread, so a
/// node cannot outlive its own reloader or leak it.
#[derive(Debug)]
pub struct CatalogReloader {
    signal: Arc<(Mutex<ReloadSignal>, Condvar)>,
    stats: Arc<StatCounters>,
    worker: Option<JoinHandle<()>>,
}

impl CatalogReloader {
    /// Starts the reload thread ticking every `interval`.
    ///
    /// Go ticks at `schemaLease/2`; a caller with a lease should pass half of
    /// it, which is why the interval is a parameter rather than a constant.
    pub fn spawn(
        catalog: Arc<SharedCatalog>,
        interval: Duration,
        mut pass: ReloadPass,
    ) -> Result<Self, CatalogReloadError> {
        if interval.is_zero() {
            return Err(CatalogReloadError::ZeroInterval);
        }
        let signal = Arc::new((Mutex::new(ReloadSignal::default()), Condvar::new()));
        let stats = Arc::new(StatCounters::default());
        let worker_signal = Arc::clone(&signal);
        let worker_stats = Arc::clone(&stats);
        let worker = std::thread::Builder::new()
            .name("catalog-reloader".to_owned())
            .spawn(move || {
                let (lock, condvar) = &*worker_signal;
                loop {
                    // Waiting on the condvar rather than sleeping is what makes
                    // shutdown prompt: a stop does not wait out the interval.
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
                    run_one_pass(&catalog, &worker_stats, &mut pass);
                }
            })
            .map_err(|error| CatalogReloadError::Spawn(error.to_string()))?;
        Ok(Self {
            signal,
            stats,
            worker: Some(worker),
        })
    }

    /// What the thread has done so far.
    #[must_use]
    pub fn stats(&self) -> CatalogReloadStats {
        self.stats.snapshot()
    }

    /// Stops the thread and waits for it, reporting a panicking worker.
    ///
    /// Idempotent: [`Drop`] calls it, so an explicit call is only needed when
    /// the caller wants to observe the failure.
    pub fn shutdown(&mut self) -> Result<(), CatalogReloadError> {
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
                .map_err(|_| CatalogReloadError::WorkerPanicked),
            None => Ok(()),
        }
    }
}

impl Drop for CatalogReloader {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Runs one pass and publishes its result, counting what happened.
///
/// A failed read is not fatal: the previously published catalog stays in force
/// and the next tick tries again, exactly as Go's reload loop logs and retries.
fn run_one_pass(catalog: &SharedCatalog, stats: &StatCounters, pass: &mut ReloadPass) {
    let current = catalog.load();
    let outcome = pass(&current);
    stats.passes.fetch_add(1, Ordering::AcqRel);
    match outcome {
        Ok(CatalogReloadPass::Unchanged) => {
            stats.unchanged.fetch_add(1, Ordering::AcqRel);
        }
        Ok(CatalogReloadPass::Diffs(next)) => {
            catalog.store(next);
            stats.diff_reloads.fetch_add(1, Ordering::AcqRel);
        }
        Ok(CatalogReloadPass::Full(next)) => {
            catalog.store(next);
            stats.full_reloads.fetch_add(1, Ordering::AcqRel);
        }
        Err(message) => {
            stats.failures.fetch_add(1, Ordering::AcqRel);
            eprintln!(
                "{{\"event\":\"catalog_reload_failed\",\"schema_version\":{},\"error\":{}}}",
                current.schema_version,
                serde_json::to_string(&message).unwrap_or_else(|_| "\"unprintable\"".to_owned())
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::Instant;

    use super::*;

    fn catalog_at(version: i64) -> ClusterCatalog {
        ClusterCatalog {
            schema_version: version,
            databases: Vec::new(),
        }
    }

    #[test]
    fn a_published_catalog_replaces_the_previous_one_whole() {
        let shared = SharedCatalog::new(catalog_at(1));
        let held = shared.load();
        shared.store(catalog_at(2));
        // The in-flight reader keeps its own version; the next reader sees the
        // new one. That is the whole consistency contract of the swap.
        assert_eq!(held.schema_version, 1);
        assert_eq!(shared.load().schema_version, 2);
    }

    #[test]
    fn a_zero_interval_is_refused_rather_than_spinning() {
        let error = CatalogReloader::spawn(
            Arc::new(SharedCatalog::new(catalog_at(1))),
            Duration::ZERO,
            Box::new(|_| Ok(CatalogReloadPass::Unchanged)),
        )
        .unwrap_err();
        assert_eq!(error, CatalogReloadError::ZeroInterval);
    }

    #[test]
    fn the_thread_publishes_each_pass_and_stops_promptly_on_shutdown() {
        let shared = Arc::new(SharedCatalog::new(catalog_at(1)));
        let (sender, receiver) = mpsc::channel();
        let mut version = 1;
        let mut reloader = CatalogReloader::spawn(
            Arc::clone(&shared),
            Duration::from_millis(5),
            Box::new(move |current| {
                assert_eq!(current.schema_version, version);
                version += 1;
                sender.send(version).unwrap();
                Ok(CatalogReloadPass::Diffs(catalog_at(version)))
            }),
        )
        .unwrap();

        assert_eq!(receiver.recv().unwrap(), 2);
        assert_eq!(receiver.recv().unwrap(), 3);

        let stopping = Instant::now();
        reloader.shutdown().unwrap();
        // A condvar-based stop must not wait out a tick; the bound is generous
        // enough for a loaded machine yet far below a long interval.
        assert!(stopping.elapsed() < Duration::from_secs(5));
        assert!(shared.load().schema_version >= 3);
        assert!(reloader.stats().diff_reloads >= 2);
        // The thread is gone: no further pass can arrive.
        drop(receiver);
    }

    #[test]
    fn a_failed_pass_keeps_the_previous_catalog_published() {
        let shared = Arc::new(SharedCatalog::new(catalog_at(7)));
        let stats = Arc::new(StatCounters::default());
        let mut pass: ReloadPass = Box::new(|_| Err("snapshot read failed".to_owned()));
        run_one_pass(&shared, &stats, &mut pass);
        assert_eq!(shared.load().schema_version, 7);
        assert_eq!(stats.snapshot().failures, 1);
    }

    #[test]
    fn an_unchanged_pass_publishes_nothing() {
        let shared = Arc::new(SharedCatalog::new(catalog_at(7)));
        let stats = Arc::new(StatCounters::default());
        let published = shared.load();
        let mut pass: ReloadPass = Box::new(|_| Ok(CatalogReloadPass::Unchanged));
        run_one_pass(&shared, &stats, &mut pass);
        assert!(Arc::ptr_eq(&published, &shared.load()));
        assert_eq!(stats.snapshot().unchanged, 1);
    }

    #[test]
    fn dropping_the_reloader_stops_its_thread() {
        let shared = Arc::new(SharedCatalog::new(catalog_at(1)));
        let (sender, receiver) = mpsc::channel();
        let reloader = CatalogReloader::spawn(
            Arc::clone(&shared),
            Duration::from_millis(5),
            Box::new(move |_| {
                let _ = sender.send(());
                Ok(CatalogReloadPass::Unchanged)
            }),
        )
        .unwrap();
        receiver.recv().unwrap();
        drop(reloader);
        // Drain what the thread had already queued, then prove it sent no more:
        // the sender is owned by the closure, so the channel closes with it.
        while receiver.recv_timeout(Duration::from_millis(200)).is_ok() {}
        assert!(receiver.recv_timeout(Duration::from_millis(200)).is_err());
    }
}
