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

//! Acknowledging loaded schema versions to a Go DDL owner.
//!
//! A node registered under `/tidb/server/info/<id>` is IN the owner's wait
//! set: Go's `WaitVersionSynced` (`pkg/ddl/schemaver/syncer.go`) holds every
//! DDL job until each registered node PUTs
//! `/tidb/ddl/all_schema_by_job_versions/<jobID>/<id>` at the job's schema
//! version. A node that registers but never acks therefore blocks EVERY DDL
//! issued through a Go tidb-server on the shared cluster — receipted by the
//! sysbench ladder's Go-control `CREATE DATABASE` hanging while the owner
//! logged this node's id as "someone is not synced", once per second.
//!
//! This module is Go's non-owner loop (`MDLCheckLoop` +
//! `refreshMDLCheckTableInfo`, `pkg/infoschema/issyncer/syncer.go`) reduced
//! to this node's shape: after the catalog reloader publishes version `V`,
//! read `mysql.tidb_mdl_info`, and for each job whose version is at most `V`
//! — and whose OLD schema no live local statement or transaction still uses
//! — report through `tidb-schemaver::Syncer`. The "still uses" gate is
//! [`SchemaPinRegistry`]: Go
//! removes a job from the ack set while a session on an older schema touches
//! the job's tables (`CheckOldRunningTxn`).

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_exec::catalog_watch::SharedCatalog;
use tidb_exec::mdl_info_load::{load_mdl_jobs, MdlJob};
use tidb_pd_client::EtcdClient;
use tidb_schemaver::etcd_syncer::new_etcd_syncer;
use tidb_schemaver::{Context as SchemaVersionContext, Syncer as SchemaVersionSyncer};
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
};

/// The tables live local work still reads at older schema versions.
///
/// Go's shape, ported from `RemoveLockDDLJobs`
/// (`pkg/sessionctx/variable/session.go:3935-3962`) and the recording in
/// `pkg/planner/core/preprocess.go:2243-2270`: each open transaction carries
/// a map `table id -> the domain schema version at the table's FIRST use in
/// this transaction` (`GetRelatedTableForMDL`), and a DDL job is blocked iff
/// some live map holds one of the job's tables at a version below the
/// job's. The map lives exactly as long as the transaction
/// (`TransactionContext.Cleanup` drops it, `session.go:451`).
///
/// One divergence, deliberately conservative: Go records tables while the
/// PLANNER resolves them, so a view's underlying tables are recorded too.
/// This port records from the parsed statement's names, which see the view,
/// not its bases -- so a name that does not resolve to a stored table marks
/// the connection `unresolved`, and an unresolved connection blocks every
/// job below its pinned version, the whole-transaction rule this replaces.
/// Blocking longer than Go is a slow ack; blocking shorter would let a DDL
/// publish under a transaction still reading the old schema.
#[derive(Debug, Default)]
pub struct SchemaPinRegistry {
    pins: Mutex<HashMap<u64, ConnPins>>,
}

/// One connection's live work.
#[derive(Debug, Default)]
struct ConnPins {
    /// Nesting count: a statement inside an explicit transaction holds too.
    count: u32,
    /// The catalog version of the OUTERMOST live hold -- the fallback bound
    /// when `unresolved` is set.
    version: i64,
    /// Go `GetRelatedTableForMDL`: table id -> version at first use.
    tables: HashMap<i64, i64>,
    /// A statement referenced a name this node could not resolve to a stored
    /// table id; block conservatively below `version`.
    unresolved: bool,
}

impl SchemaPinRegistry {
    /// Registers one unit of live work at `version`; released by dropping
    /// the guard.
    pub fn hold(self: &Arc<Self>, connection_id: u64, version: i64) -> SchemaPinGuard {
        {
            let mut pins = self.pins.lock().expect("schema pin registry poisoned");
            let entry = pins.entry(connection_id).or_default();
            if entry.count == 0 {
                entry.version = version;
            } else {
                // Nested holds keep the OLDER bound: an explicit
                // transaction's pin must not be masked by a statement
                // re-pinning after a rebuild that cannot have happened while
                // it was open.
                entry.version = entry.version.min(version);
            }
            entry.count += 1;
        }
        SchemaPinGuard {
            registry: Arc::clone(self),
            connection_id,
        }
    }

    /// Go `preprocess.go:2270`'s store: the table was bound at `version`;
    /// first use wins, exactly as Go only stores on a `Load` miss.
    pub fn record_table_use(&self, connection_id: u64, table_id: i64, version: i64) {
        let mut pins = self.pins.lock().expect("schema pin registry poisoned");
        if let Some(entry) = pins.get_mut(&connection_id) {
            entry.tables.entry(table_id).or_insert(version);
        }
    }

    /// A statement referenced a name that resolves to no stored table; the
    /// connection falls back to the whole-transaction rule.
    pub fn record_unresolved(&self, connection_id: u64) {
        let mut pins = self.pins.lock().expect("schema pin registry poisoned");
        if let Some(entry) = pins.get_mut(&connection_id) {
            entry.unresolved = true;
        }
    }

    /// Go `RemoveLockDDLJobs`'s per-session test, over every live
    /// connection: blocked iff some connection used one of the job's tables
    /// at a version below the job's -- or is `unresolved` below it.
    #[must_use]
    pub fn blocks(&self, job_version: i64, job_table_ids: &[i64]) -> bool {
        let pins = self.pins.lock().expect("schema pin registry poisoned");
        pins.values().any(|entry| {
            entry.count > 0
                && ((entry.unresolved && entry.version < job_version)
                    || job_table_ids.iter().any(|table| {
                        entry
                            .tables
                            .get(table)
                            .is_some_and(|&used| used < job_version)
                    }))
        })
    }

    fn release(&self, connection_id: u64) {
        let mut pins = self.pins.lock().expect("schema pin registry poisoned");
        if let Some(entry) = pins.get_mut(&connection_id) {
            entry.count = entry.count.saturating_sub(1);
            if entry.count == 0 {
                // Go `TransactionContext.Cleanup` (`session.go:451`): the
                // related-table map dies with the transaction.
                pins.remove(&connection_id);
            }
        }
    }
}

/// One held pin; dropping it releases the hold, disconnects included.
#[derive(Debug)]
pub struct SchemaPinGuard {
    registry: Arc<SchemaPinRegistry>,
    connection_id: u64,
}

impl Drop for SchemaPinGuard {
    fn drop(&mut self) {
        self.registry.release(self.connection_id);
    }
}

/// Which jobs are due an acknowledgement this pass.
///
/// Pure so the whole MDL decision is testable without etcd or TiKV. `acked`
/// is Go's `jobCache`: the versions already written, so an ack is sent once
/// per (job, version).
fn acks_due(
    loaded_version: i64,
    pins: &SchemaPinRegistry,
    jobs: &[MdlJob],
    acked: &BTreeMap<i64, i64>,
) -> Vec<MdlJob> {
    jobs.iter()
        .filter(|job| {
            // Go reads `... where version <= domainSchemaVer`: a job whose
            // version this node has not loaded yet cannot be acknowledged.
            job.version <= loaded_version
                // Go `RemoveLockDDLJobs`: live work that used one of the
                // job's tables on an older schema holds the job back.
                && !pins.blocks(job.version, &job.table_ids)
                // Go's `jobCache`: one ack per (job, version).
                && acked.get(&job.job_id).is_none_or(|&sent| sent < job.version)
        })
        .cloned()
        .collect()
}

/// The session-facing half of the registry: one connection's recorder,
/// handed to the driver session as its [`tidb_session::MdlRelatedTableSink`].
///
/// Recording lands only while the connection holds a pin (a live statement
/// or an open transaction) -- `record_table_use` is a no-op for an idle
/// connection, exactly as Go's map lives on the `TransactionContext` and a
/// session without one blocks nothing.
#[derive(Debug)]
pub struct ConnectionMdlSink {
    registry: Arc<SchemaPinRegistry>,
    connection_id: u64,
}

impl ConnectionMdlSink {
    /// Binds one connection's recorder to the node's registry.
    #[must_use]
    pub fn new(registry: Arc<SchemaPinRegistry>, connection_id: u64) -> Self {
        Self {
            registry,
            connection_id,
        }
    }
}

impl tidb_session::MdlRelatedTableSink for ConnectionMdlSink {
    fn record_table(&self, table_id: i64, version: i64) {
        self.registry
            .record_table_use(self.connection_id, table_id, version);
    }

    fn record_unresolved(&self) {
        self.registry.record_unresolved(self.connection_id);
    }
}

/// The background acknowledger; dropping it stops the thread.
pub struct SchemaSyncAck {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    syncer_context: SchemaVersionContext,
    syncer_thread: Option<std::thread::JoinHandle<()>>,
    syncer: Arc<dyn SchemaVersionSyncer>,
}

impl SchemaSyncAck {
    /// Starts the ack loop.
    ///
    /// `tick` is the reload cadence (the caller passes the catalog
    /// reloader's own `schema_lease / 2`); the loop only touches TiKV when
    /// the loaded version moved or an ack is still owed. Session, watch,
    /// monotonic-write, and owner-wait behavior comes from the shared
    /// `tidb-schemaver` package implementation.
    pub fn spawn<C, L, P>(
        catalog: Arc<SharedCatalog>,
        opener: RealOptimisticTransactionOpener<C, L, P>,
        pins: Arc<SchemaPinRegistry>,
        etcd: Arc<EtcdClient>,
        ddl_id: String,
        server_info: Arc<tidb_domain::serverinfo_syncer::Syncer>,
        tick: Duration,
        timeout: Duration,
    ) -> Result<Self, String>
    where
        C: StoreWriteClient + Send + 'static,
        L: StoreWriteLoader + Send + 'static,
        P: StorePdCapability + Send + 'static,
    {
        let etcd_ops = Arc::new(crate::serverinfo_etcd::EtcdClientOps::new(etcd));
        let syncer = Arc::new(new_etcd_syncer(etcd_ops, &ddl_id));
        syncer.set_server_info_syncer(Arc::new(move || {
            server_info
                .all_server_info()
                .map(|servers| servers.into_values().collect())
        }));
        let syncer_context = SchemaVersionContext::background();
        syncer.init(&syncer_context)?;
        let syncer_thread = match std::thread::Builder::new()
            .name("schema-version-mirror".to_owned())
            .spawn({
                let syncer = Arc::clone(&syncer);
                let context = syncer_context.clone();
                move || syncer.sync_job_schema_ver_loop(&context)
            }) {
            Ok(thread) => thread,
            Err(error) => {
                syncer_context.cancel();
                syncer.close();
                return Err(error.to_string());
            }
        };

        let syncer: Arc<dyn SchemaVersionSyncer> = syncer;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_seen = Arc::clone(&stop);
        let tick = tick.max(Duration::from_millis(100));
        let ack_syncer = Arc::clone(&syncer);
        let ack_context = syncer_context.clone();
        let thread = match std::thread::Builder::new()
            .name("schema-sync-ack".to_owned())
            .spawn(move || {
                run_ack_loop(
                    &catalog,
                    &opener,
                    &pins,
                    ack_syncer.as_ref(),
                    &ack_context,
                    tick,
                    timeout,
                    &stop_seen,
                );
            }) {
            Ok(thread) => thread,
            Err(error) => {
                syncer_context.cancel();
                let _ = syncer_thread.join();
                syncer.close();
                return Err(error.to_string());
            }
        };
        Ok(Self {
            stop,
            thread: Some(thread),
            syncer_context,
            syncer_thread: Some(syncer_thread),
            syncer,
        })
    }

    /// The package-parity schema-version syncer shared with the DDL owner.
    pub fn syncer(&self) -> Arc<dyn SchemaVersionSyncer> {
        Arc::clone(&self.syncer)
    }
}

impl Drop for SchemaSyncAck {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.syncer_context.cancel();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        if let Some(thread) = self.syncer_thread.take() {
            let _ = thread.join();
        }
        self.syncer.close();
    }
}

#[allow(clippy::too_many_arguments)]
fn run_ack_loop<C, L, P>(
    catalog: &SharedCatalog,
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    pins: &SchemaPinRegistry,
    syncer: &dyn SchemaVersionSyncer,
    syncer_context: &SchemaVersionContext,
    tick: Duration,
    timeout: Duration,
    stop: &AtomicBool,
) where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    // Go's `jobCache`: (job id -> version already acknowledged).
    let mut acked: BTreeMap<i64, i64> = BTreeMap::new();
    // The version whose mdl rows were last read, and whether any read job
    // is still owed its ack (a pin held it back, or a PUT failed).
    let mut scanned_version: Option<i64> = None;
    let mut reported_loaded_version: Option<i64> = None;
    let mut owed = false;
    while !stop.load(Ordering::SeqCst) {
        let loaded = catalog.load().schema_version;

        // Go's domain reload path reports every newly loaded version with
        // job id zero. The etcd syncer turns this into the leased self-key
        // update only when MDL is disabled; with MDL enabled it is a no-op.
        if reported_loaded_version != Some(loaded) {
            match syncer.update_self_version(syncer_context, 0, loaded) {
                Ok(()) => reported_loaded_version = Some(loaded),
                Err(error) => emit_warning("schema_sync_self_version_put_failed", &error),
            }
        }

        // The MDL acks. The table is only re-read when the loaded version
        // moved or something read before is still owed.
        if scanned_version != Some(loaded) || owed {
            match load_mdl_jobs(opener, timeout, &catalog.load()) {
                Ok(jobs) => {
                    scanned_version = Some(loaded);
                    // The owner deletes a finished job's row; forgetting its
                    // cache entry with it keeps the cache from growing for
                    // the process's life.
                    acked.retain(|job_id, _| jobs.iter().any(|job| job.job_id == *job_id));
                    let due = acks_due(loaded, pins, &jobs, &acked);
                    owed = jobs.iter().any(|job| {
                        acked
                            .get(&job.job_id)
                            .is_none_or(|&sent| sent < job.version)
                    });
                    for job in due {
                        match syncer.update_self_version(syncer_context, job.job_id, job.version) {
                            Ok(()) => {
                                eprintln!(
                                    "{{\"event\":\"schema_sync_acked\",\"job_id\":{},\"version\":{}}}",
                                    job.job_id, job.version
                                );
                                acked.insert(job.job_id, job.version);
                            }
                            Err(error) => {
                                emit_warning("schema_sync_ack_put_failed", &error);
                                owed = true;
                            }
                        }
                    }
                }
                Err(error) => emit_warning("schema_sync_mdl_read_failed", &error),
            }
        }

        // A sleep in small slices so a shutdown never waits a whole tick.
        let mut remaining = tick;
        while !stop.load(Ordering::SeqCst) && !remaining.is_zero() {
            let slice = remaining.min(Duration::from_millis(50));
            std::thread::sleep(slice);
            remaining = remaining.saturating_sub(slice);
        }
    }
}

fn emit_warning(event: &str, error: &impl std::fmt::Display) {
    eprintln!("{{\"event\":\"{event}\",\"error\":\"{error}\"}}");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(job_id: i64, version: i64, table_ids: &[i64]) -> MdlJob {
        MdlJob {
            job_id,
            version,
            table_ids: table_ids.to_vec(),
        }
    }

    /// Go reads `... where version <= domainSchemaVer`: a job ahead of the
    /// loaded catalog is not acknowledged yet.
    #[test]
    fn a_job_ahead_of_the_loaded_version_waits() {
        let pins = Arc::new(SchemaPinRegistry::default());
        let due = acks_due(
            5,
            &pins,
            &[job(1, 6, &[100]), job(2, 5, &[100])],
            &BTreeMap::new(),
        );
        assert_eq!(due, vec![job(2, 5, &[100])]);
    }

    /// Go `RemoveLockDDLJobs` (`pkg/sessionctx/variable/session.go:3944`):
    /// a job is blocked iff live work USED one of the job's tables at a
    /// version below the job's -- a transaction on an unrelated table does
    /// not hold it, which is the whole point of the per-table check.
    #[test]
    fn only_work_on_the_jobs_own_tables_holds_it_back() {
        let pins = Arc::new(SchemaPinRegistry::default());
        let txn = pins.hold(7, 4);
        pins.record_table_use(7, 100, 4);

        let unrelated = [job(1, 5, &[200])];
        assert_eq!(
            acks_due(5, &pins, &unrelated, &BTreeMap::new()),
            unrelated.to_vec(),
            "a transaction on table 100 must not hold a DDL on table 200"
        );

        let related = [job(2, 5, &[100, 300])];
        assert!(
            acks_due(5, &pins, &related, &BTreeMap::new()).is_empty(),
            "the same transaction must hold a DDL on table 100"
        );

        // Work AT the job's version already sees the new schema (Go's
        // `value.(int64) < jobMDL.Ver` is strict).
        pins.record_table_use(7, 300, 5);
        let at_version = [job(3, 5, &[300])];
        assert_eq!(
            acks_due(5, &pins, &at_version, &BTreeMap::new()),
            at_version.to_vec()
        );

        drop(txn);
        assert!(
            acks_due(5, &pins, &related, &BTreeMap::new()) == related.to_vec(),
            "the map dies with the transaction (Go TxnCtx.Cleanup)"
        );
    }

    /// Go records at FIRST use only (`preprocess.go:2247`: store on a Load
    /// miss), so a later re-bind at a newer version must not weaken the pin.
    #[test]
    fn first_use_wins_and_release_clears() {
        let pins = Arc::new(SchemaPinRegistry::default());
        let txn = pins.hold(7, 3);
        pins.record_table_use(7, 100, 3);
        pins.record_table_use(7, 100, 9);
        assert!(pins.blocks(5, &[100]), "first use at 3 still blocks 5");
        let statement = pins.hold(7, 3);
        drop(statement);
        assert!(pins.blocks(5, &[100]), "the transaction still holds");
        drop(txn);
        assert!(!pins.blocks(5, &[100]));
    }

    /// A name this node cannot resolve to a stored table (Go's planner sees
    /// through views; this port's statement names do not) falls back to the
    /// whole-transaction rule: block everything below the pinned version.
    #[test]
    fn an_unresolved_name_blocks_conservatively() {
        let pins = Arc::new(SchemaPinRegistry::default());
        let txn = pins.hold(7, 4);
        pins.record_unresolved(7);
        assert!(pins.blocks(5, &[999]), "unresolved blocks any table");
        assert!(!pins.blocks(4, &[999]), "but not a job at its own version");
        drop(txn);
        assert!(!pins.blocks(5, &[999]));
    }

    /// Go's `jobCache`: one ack per (job, version), but a job re-published
    /// at a HIGHER version is acknowledged again.
    #[test]
    fn an_acknowledged_job_is_not_resent_until_its_version_moves() {
        let pins = Arc::new(SchemaPinRegistry::default());
        let mut acked = BTreeMap::new();
        acked.insert(1, 5_i64);
        assert!(acks_due(5, &pins, &[job(1, 5, &[1])], &acked).is_empty());
        assert_eq!(
            acks_due(6, &pins, &[job(1, 6, &[1])], &acked),
            vec![job(1, 6, &[1])]
        );
    }
}
