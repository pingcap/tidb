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
//! — write the ack. The "still uses" gate is [`SchemaPinRegistry`]: Go
//! removes a job from the ack set while a session on an older schema touches
//! the job's tables (`CheckOldRunningTxn`); this port blocks on any older
//! pin regardless of table, which acks later than Go, never earlier.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_exec::catalog_watch::SharedCatalog;
use tidb_exec::mdl_info_load::{load_mdl_jobs, MdlJob};
use tidb_pd_client::EtcdClient;
use tidb_txnkv::transaction::{
    RealOptimisticTransactionOpener, StorePdCapability, StoreWriteClient, StoreWriteLoader,
};

/// Go `util.DDLAllSchemaVersions`: the MDL-off owner's wait keys, leased.
const DDL_ALL_SCHEMA_VERSIONS: &str = "/tidb/ddl/all_schema_versions";
/// Go `util.DDLAllSchemaVersionsByJob`: the MDL owner's per-job wait keys.
const DDL_ALL_SCHEMA_VERSIONS_BY_JOB: &str = "/tidb/ddl/all_schema_by_job_versions";
/// Go `util.SessionTTL`: the seconds the self-version lease lives without a
/// keepalive.
const SESSION_TTL_SECONDS: i64 = 90;

/// The catalog versions live local work still reads at.
///
/// A statement holds its session's version for its own duration; an explicit
/// transaction holds it from `BEGIN` to `COMMIT`/`ROLLBACK`. The oldest held
/// version is the MDL gate: a job may be acknowledged only when nothing
/// local still runs on a schema older than the job's.
#[derive(Debug, Default)]
pub struct SchemaPinRegistry {
    /// connection id -> (pinned version, nesting count). A statement inside
    /// an explicit transaction nests; both pin the SAME version because the
    /// session's catalog cannot move while the transaction is open.
    pins: Mutex<HashMap<u64, (i64, u32)>>,
}

impl SchemaPinRegistry {
    /// Registers one unit of live work at `version`; released by dropping
    /// the guard.
    pub fn hold(self: &Arc<Self>, connection_id: u64, version: i64) -> SchemaPinGuard {
        {
            let mut pins = self.pins.lock().expect("schema pin registry poisoned");
            let entry = pins.entry(connection_id).or_insert((version, 0));
            // Nested holds keep the OLDER version: an explicit transaction's
            // pin must not be masked by a statement re-pinning after a
            // rebuild that cannot have happened while it was open.
            entry.0 = entry.0.min(version);
            entry.1 += 1;
        }
        SchemaPinGuard {
            registry: Arc::clone(self),
            connection_id,
        }
    }

    /// The oldest version any live work still reads at, if any.
    #[must_use]
    pub fn oldest_pinned(&self) -> Option<i64> {
        self.pins
            .lock()
            .expect("schema pin registry poisoned")
            .values()
            .map(|(version, _)| *version)
            .min()
    }

    fn release(&self, connection_id: u64) {
        let mut pins = self.pins.lock().expect("schema pin registry poisoned");
        if let Some(entry) = pins.get_mut(&connection_id) {
            entry.1 = entry.1.saturating_sub(1);
            if entry.1 == 0 {
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
    oldest_pin: Option<i64>,
    jobs: &[MdlJob],
    acked: &BTreeMap<i64, i64>,
) -> Vec<MdlJob> {
    jobs.iter()
        .copied()
        .filter(|job| {
            // Go reads `... where version <= domainSchemaVer`: a job whose
            // version this node has not loaded yet cannot be acknowledged.
            job.version <= loaded_version
                // Go `CheckOldRunningTxn`: live work on an older schema
                // holds the job back. `>=` because work AT the job's version
                // already sees the new schema.
                && oldest_pin.is_none_or(|pin| pin >= job.version)
                // Go's `jobCache`: one ack per (job, version).
                && acked.get(&job.job_id).is_none_or(|&sent| sent < job.version)
        })
        .collect()
}

/// The background acknowledger; dropping it stops the thread.
pub struct SchemaSyncAck {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SchemaSyncAck {
    /// Starts the ack loop.
    ///
    /// `tick` is the reload cadence (the caller passes the catalog
    /// reloader's own `schema_lease / 2`); the loop only touches TiKV when
    /// the loaded version moved or an ack is still owed, so a quiet cluster
    /// costs nothing but the leased self-key's keepalive.
    pub fn spawn<C, L, P>(
        catalog: Arc<SharedCatalog>,
        opener: RealOptimisticTransactionOpener<C, L, P>,
        pins: Arc<SchemaPinRegistry>,
        etcd: Arc<EtcdClient>,
        ddl_id: String,
        tick: Duration,
        timeout: Duration,
    ) -> Self
    where
        C: StoreWriteClient + Send + 'static,
        L: StoreWriteLoader + Send + 'static,
        P: StorePdCapability + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_seen = Arc::clone(&stop);
        let tick = tick.max(Duration::from_millis(100));
        let thread = std::thread::Builder::new()
            .name("schema-sync-ack".to_owned())
            .spawn(move || {
                run_ack_loop(
                    &catalog, &opener, &pins, &etcd, &ddl_id, tick, timeout, &stop_seen,
                );
            })
            .expect("spawning the schema-sync ack thread");
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for SchemaSyncAck {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_ack_loop<C, L, P>(
    catalog: &SharedCatalog,
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    pins: &SchemaPinRegistry,
    etcd: &EtcdClient,
    ddl_id: &str,
    tick: Duration,
    timeout: Duration,
    stop: &AtomicBool,
) where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    // Go `Init` writes the self key at startup under a session lease so a
    // dead node's entry expires instead of blocking an MDL-off owner
    // forever. Everything etcd-side is best effort with a warning: a node
    // that cannot reach etcd still serves SQL, exactly like the watch and
    // the server-info syncer.
    let mut lease: Option<i64> = None;
    let mut lease_renewed = Instant::now();
    let mut published_self: Option<i64> = None;
    // Go's `jobCache`: (job id -> version already acknowledged).
    let mut acked: BTreeMap<i64, i64> = BTreeMap::new();
    // The version whose mdl rows were last read, and whether any read job
    // is still owed its ack (a pin held it back, or a PUT failed).
    let mut scanned_version: Option<i64> = None;
    let mut owed = false;
    while !stop.load(Ordering::SeqCst) {
        let loaded = catalog.load().schema_version;

        // The leased self key, kept CURRENT rather than Go's write-once
        // under MDL: only an MDL-off owner reads it, and for that reader a
        // stale value is the harmful shape (see the ExecPlan's decision
        // log, `docs/schema-sync-ack-execplan.md`).
        let lease_expired = lease_renewed.elapsed().as_secs() >= (SESSION_TTL_SECONDS as u64) / 3;
        if published_self != Some(loaded) || lease_expired {
            if lease.is_none() || lease_expired {
                if let (Some(id), true) = (lease, lease_expired) {
                    match etcd.lease_keep_alive_once(id) {
                        Ok(_) => lease_renewed = Instant::now(),
                        Err(_) => lease = None,
                    }
                }
                if lease.is_none() {
                    match etcd.lease_grant(SESSION_TTL_SECONDS) {
                        Ok((id, _ttl)) => {
                            lease = Some(id);
                            lease_renewed = Instant::now();
                            published_self = None;
                        }
                        Err(error) => emit_warning("schema_sync_lease_grant_failed", &error),
                    }
                }
            }
            if let Some(id) = lease {
                if published_self != Some(loaded) {
                    let key = format!("{DDL_ALL_SCHEMA_VERSIONS}/{ddl_id}");
                    match etcd.put_with_lease(key.as_bytes(), loaded.to_string().as_bytes(), id) {
                        Ok(()) => published_self = Some(loaded),
                        Err(error) => emit_warning("schema_sync_self_version_put_failed", &error),
                    }
                }
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
                    let oldest_pin = pins.oldest_pinned();
                    let due = acks_due(loaded, oldest_pin, &jobs, &acked);
                    owed = jobs.iter().any(|job| {
                        acked
                            .get(&job.job_id)
                            .is_none_or(|&sent| sent < job.version)
                    });
                    for job in due {
                        let key =
                            format!("{DDL_ALL_SCHEMA_VERSIONS_BY_JOB}/{}/{ddl_id}", job.job_id);
                        match etcd.put(key.as_bytes(), job.version.to_string().as_bytes()) {
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

    const fn job(job_id: i64, version: i64) -> MdlJob {
        MdlJob { job_id, version }
    }

    /// Go reads `... where version <= domainSchemaVer`: a job ahead of the
    /// loaded catalog is not acknowledged yet.
    #[test]
    fn a_job_ahead_of_the_loaded_version_waits() {
        let due = acks_due(5, None, &[job(1, 6), job(2, 5)], &BTreeMap::new());
        assert_eq!(due, vec![job(2, 5)]);
    }

    /// Go `CheckOldRunningTxn`: live work on an older schema holds the job
    /// back; work AT the job's version does not.
    #[test]
    fn an_older_pin_holds_the_ack_back() {
        let jobs = [job(1, 5)];
        assert!(acks_due(5, Some(4), &jobs, &BTreeMap::new()).is_empty());
        assert_eq!(
            acks_due(5, Some(5), &jobs, &BTreeMap::new()),
            vec![job(1, 5)]
        );
        assert_eq!(acks_due(5, None, &jobs, &BTreeMap::new()), vec![job(1, 5)]);
    }

    /// Go's `jobCache`: one ack per (job, version), but a job re-published
    /// at a HIGHER version is acknowledged again.
    #[test]
    fn an_acknowledged_job_is_not_resent_until_its_version_moves() {
        let mut acked = BTreeMap::new();
        acked.insert(1, 5_i64);
        assert!(acks_due(5, None, &[job(1, 5)], &acked).is_empty());
        assert_eq!(acks_due(6, None, &[job(1, 6)], &acked), vec![job(1, 6)]);
    }

    /// The registry reports the OLDEST held version, nested holds keep one
    /// entry per connection, and dropping every guard clears it.
    #[test]
    fn the_pin_registry_tracks_the_oldest_live_version() {
        let pins = Arc::new(SchemaPinRegistry::default());
        assert_eq!(pins.oldest_pinned(), None);
        let transaction = pins.hold(7, 10);
        let statement = pins.hold(7, 10);
        let other = pins.hold(8, 12);
        assert_eq!(pins.oldest_pinned(), Some(10));
        drop(statement);
        assert_eq!(
            pins.oldest_pinned(),
            Some(10),
            "the transaction still holds"
        );
        drop(transaction);
        assert_eq!(pins.oldest_pinned(), Some(12));
        drop(other);
        assert_eq!(pins.oldest_pinned(), None);
    }
}
