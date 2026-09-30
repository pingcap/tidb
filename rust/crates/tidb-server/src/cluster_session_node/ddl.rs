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

//! The DDL seam: this node's one route to the cluster's stored schema. Split
//! out of `cluster_session_node` because it is one of the independent seams
//! that accreted there; see that module's doc comment for how a DDL
//! statement is routed here and what happens to the connection's own
//! catalog afterwards.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tidb_pd_client::PdClient;
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};
use tidb_txnkv::PdRegionLoader;

use tidb_ddl_serverstate::{Context as ServerStateContext, EtcdSyncer, MemSyncer, Syncer};
use tidb_exec::catalog_watch::{CatalogReloadPass, SharedCatalog as SharedClusterCatalog};
use tidb_exec::cluster_ddl::{
    CheckConstraintValidation, DdlStatement, ExchangePartitionValidation, IndexBackfill,
};
use tidb_exec::ddl_job_scheduler::{must_reload_schemas, SchemaLoader};
use tidb_exec::ddl_job_table::DdlJobTable;
use tidb_exec::ddl_systable::MinJobIdRefresher;
use tidb_exec::pessimistic_lock_error::LockSqlError;
use tidb_exec::real_tikv_catalog::reload_catalog_from_cluster;
use tidb_exec::real_tikv_ddl::{
    commit_cluster_ddl_with_backfill, load_active_persisted_ddl_jobs_cached,
    load_history_persisted_ddl_job, load_min_persisted_ddl_job_id_cached,
    run_persisted_ddl_job_to_completion, submit_check_constraint_job_with_retry,
    CheckConstraintValidator, ClusterDdlReport, DdlSchemaSync, ExchangePartitionValidator,
    IndexBackfiller, SchemaVersionNotifier,
};

use tidb_exec::real_tikv_read::RealOptimisticTransactionOpener;
use tidb_exec::schema_validator::SchemaValidator;
use tidb_executor::cluster_storage::{ClusterSnapshot, ClusterTableStorage, MutationBuffer};
use tidb_executor::{RowDecodeContext, StmtContext};
use tidb_pd_client::EtcdClient;

use crate::cluster_session::{cluster_table, kv_index, AutoIdSource};
use crate::sql_node::{cluster_ddl_error, SqlQueryError};

/// This node's one route to the cluster's stored schema.
///
/// The seam exists for the same reason `ClusterTransactions` does: the
/// routing decision -- which statements become catalog changes, what happens
/// to an open transaction, when the connection's tables are rebuilt -- is
/// exercised without a cluster. The production implementation is
/// [`RealClusterDdl`].
pub trait ClusterDdl: Send + Sync {
    /// Publishes one admitted catalog change, then brings this node's own
    /// catalog up to it before answering.
    ///
    /// The two halves are one method because a caller that published without
    /// refreshing would answer the next statement from a catalog it knows to
    /// be stale.
    fn execute(&self, statement: &DdlStatement) -> Result<ClusterDdlReport, SqlQueryError>;
}

const DDL_OWNER_KEY: &str = "/tidb/ddl/fg/owner";
const ADDING_DDL_JOB_NOTIFY_KEY: &[u8] = b"/tidb/ddl/add_ddl_job_general";
const DDL_SCHEDULER_INTERVAL: Duration = Duration::from_secs(1);

fn handle_server_state_watch<T>(
    poll: Result<T, std::sync::mpsc::TryRecvError>,
    rewatch: impl FnOnce(),
) -> bool {
    match poll {
        Ok(_) => true,
        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
            rewatch();
            true
        }
        Err(std::sync::mpsc::TryRecvError::Empty) => false,
    }
}

struct ClusterSchemaSync<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    catalog: Arc<SharedClusterCatalog>,
    /// The node's schema validator, renewed by this reload like the reload
    /// thread's own passes renew it.
    schema_validator: Arc<SchemaValidator>,
    timeout: Duration,
    notifier: Option<Arc<EtcdClient>>,
    schema_version_syncer: Option<Arc<dyn tidb_schemaver::Syncer>>,
    owner_id: String,
}

struct SchedulerWorker {
    stop: Arc<AtomicBool>,
    stop_tx: Sender<()>,
    handle: JoinHandle<()>,
    _adding_job_watcher: Option<tidb_pd_client::EtcdWatcher>,
}

struct PersistedDdlScheduler<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    timeout: Duration,
    notifier: Option<Arc<EtcdClient>>,
    schema_sync: Arc<ClusterSchemaSync<C, L, P>>,
    wake: Arc<(Mutex<u64>, Condvar)>,
    server_state: Arc<dyn Syncer>,
    server_state_context: ServerStateContext,
    min_job_id_refresher: Arc<MinJobIdRefresher>,
    owner: Mutex<Option<Arc<dyn tidb_owner::Manager>>>,
    worker: Mutex<Option<SchedulerWorker>>,
}

impl<C, L, P> PersistedDdlScheduler<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn notify(&self) {
        let (generation, condvar) = &*self.wake;
        let mut generation = generation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *generation = generation.wrapping_add(1);
        condvar.notify_all();
    }

    fn stop(&self) {
        let worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(worker) = worker {
            worker.stop.store(true, Ordering::Release);
            let _ = worker.stop_tx.send(());
            self.notify();
            let _ = worker.handle.join();
        }
    }

    fn refresh_server_state(
        server_state: &dyn Syncer,
        context: &ServerStateContext,
        owner: &dyn tidb_owner::Manager,
    ) {
        match server_state.get_global_state(context) {
            Ok(state) => {
                let op = if state.state == tidb_ddl_serverstate::STATE_UPGRADING {
                    tidb_owner::OpType::SYNC_UPGRADING_STATE
                } else {
                    tidb_owner::OpType::NONE
                };
                if let Err(error) =
                    owner.set_owner_op_value(&tidb_owner::Context::background(), op)
                {
                    eprintln!(
                        "{{\"level\":\"warning\",\"event\":\"ddl_owner_state_update_failed\",\"error\":{}}}",
                        serde_json::to_string(&error)
                            .unwrap_or_else(|_| "\"unprintable\"".to_owned())
                    );
                }
            }
            Err(error) => eprintln!(
                "{{\"level\":\"warning\",\"event\":\"ddl_global_state_reload_failed\",\"error\":{}}}",
                serde_json::to_string(&error.to_string())
                    .unwrap_or_else(|_| "\"unprintable\"".to_owned())
            ),
        }
    }

    fn run_loop(
        opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
        schema_sync: Arc<ClusterSchemaSync<C, L, P>>,
        wake: Arc<(Mutex<u64>, Condvar)>,
        server_state: Arc<dyn Syncer>,
        server_state_context: ServerStateContext,
        min_job_id_refresher: Arc<MinJobIdRefresher>,
        owner: Arc<dyn tidb_owner::Manager>,
        stop: Arc<AtomicBool>,
        stopped: Receiver<()>,
    ) {
        must_reload_schemas(schema_sync.as_ref(), &stopped, DDL_SCHEDULER_INTERVAL);
        if stop.load(Ordering::Acquire) {
            return;
        }
        Self::refresh_server_state(server_state.as_ref(), &server_state_context, owner.as_ref());
        // The bootstrap `mysql.tidb_ddl_job` layout is located once and kept
        // across ticks; each tick still reads its rows from a fresh
        // snapshot. See `load_active_persisted_ddl_jobs_cached`.
        let mut cached_job_table: Option<DdlJobTable> = None;
        while !stop.load(Ordering::Acquire) {
            match load_active_persisted_ddl_jobs_cached(
                Arc::clone(&opener),
                timeout,
                min_job_id_refresher.current_min_job_id(),
                &mut cached_job_table,
            ) {
                Ok(jobs) => {
                    for job in jobs {
                        if stop.load(Ordering::Acquire) {
                            return;
                        }
                        let notifier_ref = notifier
                            .as_ref()
                            .map(|client| Arc::as_ref(client) as &dyn SchemaVersionNotifier);
                        if !tidb_exec::cluster_ddl::supports_persisted_ddl_job(job.type_) {
                            continue;
                        }
                        let check_owner = || {
                            if stop.load(Ordering::Acquire) || !owner.is_owner() {
                                Err("DDL worker stopped or ownership lost".to_owned())
                            } else {
                                Ok(())
                            }
                        };
                        if let Err(error) = run_persisted_ddl_job_to_completion(
                            Arc::clone(&opener),
                            job.id,
                            timeout,
                            notifier_ref,
                            &KvTableIndexBackfiller,
                            &KvTableIndexBackfiller,
                            &KvTableIndexBackfiller,
                            schema_sync.as_ref(),
                            &check_owner,
                        ) {
                            eprintln!("{{\"level\":\"warning\",\"event\":\"ddl_job_step_failed\",\"job_id\":{},\"error\":{}}}",
                                job.id, serde_json::to_string(&error.to_string()).unwrap_or_else(|_| "\"unprintable\"".to_owned()));
                        }
                    }
                }
                Err(error) => eprintln!(
                    "{{\"level\":\"warning\",\"event\":\"ddl_job_scan_failed\",\"error\":{}}}",
                    serde_json::to_string(&error.to_string())
                        .unwrap_or_else(|_| "\"unprintable\"".to_owned())
                ),
            }

            let (generation, condvar) = &*wake;
            let guard = generation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let observed = *guard;
            let (guard, _) = condvar
                .wait_timeout_while(guard, DDL_SCHEDULER_INTERVAL, |generation| {
                    !stop.load(Ordering::Acquire) && *generation == observed
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            drop(guard);

            let state_changed = server_state.watch_chan().is_some_and(|watch| {
                handle_server_state_watch(watch.try_recv(), || {
                    server_state.rewatch(&server_state_context);
                })
            });
            if state_changed {
                Self::refresh_server_state(
                    server_state.as_ref(),
                    &server_state_context,
                    owner.as_ref(),
                );
            }
        }
    }

}

impl<C, L, P> tidb_owner::Listener for PersistedDdlScheduler<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn on_become_owner(&self) {
        let mut worker = self
            .worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if worker.is_some() {
            return;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let (stop_tx, stopped) = channel();
        let adding_job_watcher = self.notifier.as_ref().and_then(|etcd| {
            let wake = Arc::clone(&self.wake);
            match etcd.watch_key(ADDING_DDL_JOB_NOTIFY_KEY, 0, move |_| {
                let (generation, condvar) = &*wake;
                let mut generation = generation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *generation = generation.wrapping_add(1);
                condvar.notify_all();
            }) {
                Ok(watcher) => Some(watcher),
                Err(error) => {
                    eprintln!(
                        "{{\"level\":\"warning\",\"event\":\"ddl_job_notify_watch_failed\",\"error\":{}}}",
                        serde_json::to_string(&error.to_string())
                            .unwrap_or_else(|_| "\"unprintable\"".to_owned())
                    );
                    None
                }
            }
        });
        let handle = std::thread::Builder::new()
            .name("ddl-job-scheduler".to_owned())
            .spawn({
                let opener = Arc::clone(&self.opener);
                let notifier = self.notifier.clone();
                let schema_sync = Arc::clone(&self.schema_sync);
                let wake = Arc::clone(&self.wake);
                let stop = Arc::clone(&stop);
                let timeout = self.timeout;
                let server_state = Arc::clone(&self.server_state);
                let server_state_context = self.server_state_context.clone();
                let min_job_id_refresher = Arc::clone(&self.min_job_id_refresher);
                let owner = self
                    .owner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .cloned()
                    .expect("DDL owner is bound before campaigning");
                move || {
                    Self::run_loop(
                        opener,
                        timeout,
                        notifier,
                        schema_sync,
                        wake,
                        server_state,
                        server_state_context,
                        min_job_id_refresher,
                        owner,
                        stop,
                        stopped,
                    )
                }
            })
            .expect("spawning the DDL owner scheduler");
        *worker = Some(SchedulerWorker {
            stop,
            stop_tx,
            handle,
            _adding_job_watcher: adding_job_watcher,
        });
    }

    fn on_retire_owner(&self) {
        self.stop();
    }
}

/// The production catalog writer: the optimistic 2PC over the node's one
/// process authority, followed by an inline reload of the node's own catalog.
pub struct RealClusterDdl<C = TonicCoprocessorClient, L = PdRegionLoader, P = PdClient>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    opener: Arc<RealOptimisticTransactionOpener<C, L, P>>,
    catalog: Arc<SharedClusterCatalog>,
    timeout: Duration,
    /// The etcd client this node announces its catalog changes through, so
    /// peers' watches fire promptly. `None` leaves them to their lease tick;
    /// a failed announcement is a warning, never a failed DDL.
    notifier: Option<Arc<EtcdClient>>,
    schema_sync: Arc<ClusterSchemaSync<C, L, P>>,
    scheduler: Arc<PersistedDdlScheduler<C, L, P>>,
    owner: Arc<dyn tidb_owner::Manager>,
    server_state: Arc<dyn Syncer>,
    server_state_context: ServerStateContext,
    min_job_id_refresher: Arc<MinJobIdRefresher>,
    min_job_id_stop: Sender<()>,
    min_job_id_worker: Option<JoinHandle<()>>,
}

impl<C, L, P> RealClusterDdl<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    /// Binds the writer to an already-connected authority and the catalog slot
    /// the reload thread publishes into.
    pub fn new(
        opener: RealOptimisticTransactionOpener<C, L, P>,
        catalog: Arc<SharedClusterCatalog>,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
        server_info: Arc<tidb_domain::serverinfo_syncer::Syncer>,
        schema_version_syncer: Option<Arc<dyn tidb_schemaver::Syncer>>,
        schema_validator: Arc<SchemaValidator>,
        campaign_owner: bool,
    ) -> Result<Self, String> {
        let owner_id = server_info.local_server_info().static_info.id;
        let server_state_context = ServerStateContext::background();
        let server_state: Arc<dyn Syncer> = match notifier.as_ref() {
            Some(etcd) => Arc::new(EtcdSyncer::new(
                Arc::clone(etcd),
                tidb_ddl_serverstate::SERVER_GLOBAL_STATE,
            )),
            None => Arc::new(MemSyncer::new()),
        };
        server_state
            .init(&server_state_context)
            .map_err(|error| error.to_string())?;
        let opener = Arc::new(opener);
        let min_job_id_refresher = Arc::new(MinJobIdRefresher::new());
        let (min_job_id_stop, min_job_id_stopped) = channel();
        let min_job_id_worker = std::thread::Builder::new()
            .name("ddl-min-job-id-refresher".to_owned())
            .spawn({
                let refresher = Arc::clone(&min_job_id_refresher);
                let opener = Arc::clone(&opener);
                move || {
                    let mut cached_job_table: Option<DdlJobTable> = None;
                    refresher.start(&min_job_id_stopped, |previous| {
                        load_min_persisted_ddl_job_id_cached(
                            Arc::clone(&opener),
                            timeout,
                            previous,
                            &mut cached_job_table,
                        )
                        .map_err(|error| error.to_string())
                    });
                }
            })
            .map_err(|error| error.to_string())?;
        let schema_sync = Arc::new(ClusterSchemaSync {
            opener: Arc::clone(&opener),
            catalog: Arc::clone(&catalog),
            schema_validator,
            timeout,
            notifier: notifier.clone(),
            schema_version_syncer,
            owner_id: owner_id.clone(),
        });
        let scheduler = Arc::new(PersistedDdlScheduler {
            opener: Arc::clone(&opener),
            timeout,
            notifier: notifier.clone(),
            schema_sync: Arc::clone(&schema_sync),
            wake: Arc::new((Mutex::new(0), Condvar::new())),
            server_state: Arc::clone(&server_state),
            server_state_context: server_state_context.clone(),
            min_job_id_refresher: Arc::clone(&min_job_id_refresher),
            owner: Mutex::new(None),
            worker: Mutex::new(None),
        });
        let owner: Arc<dyn tidb_owner::Manager> = match notifier.as_ref() {
            Some(etcd) => Arc::new(tidb_owner::OwnerManager::new(
                tidb_owner::Context::background(),
                Arc::clone(etcd) as Arc<dyn tidb_owner::OwnerStore>,
                "ddl",
                owner_id.clone(),
                DDL_OWNER_KEY,
            )),
            None => Arc::new(local_ddl_owner(owner_id, opener.authority_id())),
        };
        *scheduler
            .owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&owner));
        owner.set_listener(Arc::clone(&scheduler) as Arc<dyn tidb_owner::Listener>);
        // Go `ddl.Start` (`ddl.go:871`, `:926`): only `Instance.TiDBEnableDDL`
        // (`--run-ddl`) campaigns and runs the worker; a node with it off
        // submits its DDL to whichever node owns the cluster's queue. This
        // tier's owner loop runs the CHECK CONSTRAINT job types only, so the
        // switch is how a mixed cluster keeps every other job type on a
        // node that executes it.
        if campaign_owner {
            owner.campaign_owner(&[])?;
        }
        Ok(Self {
            opener,
            catalog,
            timeout,
            notifier,
            schema_sync,
            scheduler,
            owner,
            server_state,
            server_state_context,
            min_job_id_refresher,
            min_job_id_stop,
            min_job_id_worker: Some(min_job_id_worker),
        })
    }

    fn reload_catalog(&self) -> Result<(), String> {
        self.schema_sync.reload_catalog()
    }

    /// Runs one reload pass inline, on the statement's own thread.
    ///
    /// Go's DDL owner PUTs the new version to etcd so every *other* node's
    /// watch fires; this node is the one that just wrote the change, so it
    /// needs no notification -- it reloads at once instead of waiting up to
    /// `lease/2` for the reload thread's tick. Both publishers replace the
    /// catalog whole in the same slot, so neither can observe the other
    /// half-applied.
    ///
    /// A failed reload is not a failed DDL: the change is committed in the
    /// cluster, and the lease tick will pick it up. Reporting the statement as
    /// failed would be a lie about what the cluster now holds, so the failure
    /// is emitted and the statement stands.
    fn refresh_catalog(&self) {
        if let Err(error) = self.reload_catalog() {
            eprintln!(
                "{{\"event\":\"catalog_reload_after_ddl_failed\",\"schema_version\":{},\"error\":{:?}}}",
                self.catalog.load().schema_version,
                error
            );
        }
    }

    fn notify_new_job_submitted(&self) {
        if self.owner.is_owner() {
            self.scheduler.notify();
            return;
        }
        let Some(etcd) = self.notifier.as_ref() else {
            return;
        };
        if let Err(error) = etcd.put(ADDING_DDL_JOB_NOTIFY_KEY, b"0") {
            eprintln!(
                "{{\"level\":\"info\",\"event\":\"notify_new_ddl_job_failed\",\"error\":{}}}",
                serde_json::to_string(&error.to_string())
                    .unwrap_or_else(|_| "\"unprintable\"".to_owned())
            );
        }
    }

    fn wait_persisted_job(&self, ddl_job_id: i64) -> Result<ClusterDdlReport, SqlQueryError> {
        loop {
            if let Some(job) =
                load_history_persisted_ddl_job(Arc::clone(&self.opener), ddl_job_id, self.timeout)
                    .map_err(cluster_ddl_error)?
            {
                if let Some(error) = job.error.as_ref() {
                    let error = error.read();
                    let code = u16::try_from(error.code().value()).unwrap_or(1105);
                    return Err(crate::sql_node::lock_sql_error(&LockSqlError {
                        code,
                        state: *b"HY000",
                        message: error.message().to_owned(),
                    }));
                }
                if job.state.is_done() || job.state.is_synced() {
                    return Ok(ClusterDdlReport::Applied {
                        schema_version: job.last_schema_version,
                        created_id: None,
                        warnings: job
                            .warning
                            .as_ref()
                            .map(|warning| {
                                vec![(
                                    tidb_exec::real_tikv_ddl::DdlWarningLevel::Warning,
                                    1105_u16,
                                    warning.read().message().to_owned(),
                                )]
                            })
                            .unwrap_or_default(),
                    });
                }
                return Err(SqlQueryError::unknown(format!(
                    "DDL job {ddl_job_id} reached terminal state {} without an error",
                    job.state
                )));
            }

            let (generation, condvar) = &*self.scheduler.wake;
            let guard = generation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let observed = *guard;
            let (guard, _) = condvar
                .wait_timeout_while(guard, Duration::from_millis(100), |generation| {
                    *generation == observed
                })
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            drop(guard);
        }
    }
}

fn local_ddl_owner(owner_id: String, authority_id: u64) -> tidb_owner::MockManager {
    // Go NewMockManager uses store.UUID(), not the nil-store fallback.
    // Embedded stores each own one read authority; clones retain its ID.
    let store_id = format!("embedded-authority-{authority_id}");
    tidb_owner::MockManager::new(
        tidb_owner::Context::background(),
        owner_id,
        Some(&store_id),
        DDL_OWNER_KEY,
    )
}

#[cfg(test)]
mod schema_sync_tests {
    use super::*;

    #[test]
    fn persisted_worker_recovers_schema_barriers_before_history() {
        if crate::isolate_process_globals() {
            return;
        }
        use tidb_exec::real_tikv_catalog::TransactionMetaSnapshot;
        use tidb_exec::real_tikv_ddl::{load_active_persisted_ddl_jobs, ClusterDdlError};
        use tidb_model::{
            ActionType, CreateSchemaArgs, DBInfo, GoField, GoShared, HistoryInfo, Job, JobState,
            JobVersion, SchemaState,
        };
        use tidb_txnkv::transaction::OptimisticCommitOutcome;
        struct Barrier {
            fail: AtomicBool,
            waits: Mutex<Vec<(i64, i64)>>,
            cleaned: Mutex<Vec<i64>>,
        }
        impl DdlSchemaSync for Barrier {
            fn owner_id(&self) -> &str {
                "replacement-owner"
            }
            fn wait_version_synced(&self, id: i64, version: i64) -> Result<(), String> {
                self.waits.lock().unwrap().push((id, version));
                if self.fail.load(Ordering::Relaxed) {
                    Err("schema acknowledgement unavailable".into())
                } else {
                    Ok(())
                }
            }
            fn clean_job_versions(&self, id: i64) -> Result<(), String> {
                self.cleaned.lock().unwrap().push(id);
                Ok(())
            }
        }
        struct FailedNotifier;
        impl SchemaVersionNotifier for FailedNotifier {
            fn notify(&self, _: i64) -> Result<(), String> {
                Err("etcd unavailable".into())
            }
        }
        let (_authority, _pd, opener) = crate::unistore_node::in_process_write_stack().unwrap();
        let opener = Arc::new(opener);
        let timeout = Duration::from_secs(5);
        let (outcome, _) = crate::bootstrap_publish::publish_bootstrap(&opener, timeout).unwrap();
        assert!(matches!(outcome, OptimisticCommitOutcome::Committed(_)));
        let seed = |job: &mut Job| {
            let mut tx = opener.begin().unwrap();
            let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
            let catalog = tidb_exec::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
            let table = DdlJobTable::locate(&catalog).unwrap();
            let mut mutations = Vec::new();
            table
                .append_insert(job, false, "501", "0", false, &mut mutations)
                .unwrap();
            assert!(matches!(
                tx.commit(
                    mutations,
                    &tidb_txnkv::UnaryCallContext::with_timeout(timeout)
                )
                .unwrap(),
                OptimisticCommitOutcome::Committed(_)
            ));
        };
        let mut job = Job::default();
        job.id = 500;
        job.schema_id = 501;
        job.schema_name = "sync_database".into();
        job.type_ = ActionType::ACTION_CREATE_SCHEMA;
        job.state = JobState::QUEUEING;
        job.version = JobVersion::V2;
        job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
        job.fill_args(Some(GoShared::new(CreateSchemaArgs {
            db_info: GoField::new(Some(GoShared::new(DBInfo {
                id: 501,
                name: tidb_ast::CiString::new("sync_database"),
                ..Default::default()
            }))),
        })));
        seed(&mut job);
        let barrier = Barrier {
            fail: AtomicBool::new(true),
            waits: Mutex::new(Vec::new()),
            cleaned: Mutex::new(Vec::new()),
        };
        let run = |id,
                   notifier: Option<&dyn SchemaVersionNotifier>,
                   check: &dyn Fn() -> Result<(), String>| {
            run_persisted_ddl_job_to_completion(
                opener.clone(),
                id,
                timeout,
                notifier,
                &KvTableIndexBackfiller,
                &KvTableIndexBackfiller,
                &KvTableIndexBackfiller,
                &barrier,
                check,
            )
        };
        let before = tidb_exec::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout)
            .unwrap()
            .schema_version;
        let checks = std::sync::atomic::AtomicUsize::new(0);
        let lost_before_commit = || {
            if checks.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err("owner lost".into())
            }
        };
        assert!(run(500, None, &lost_before_commit).is_err());
        assert_eq!(
            tidb_exec::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout)
                .unwrap()
                .schema_version,
            before
        );
        assert_eq!(
            load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap()[0].state,
            JobState::QUEUEING
        );
        assert!(run(500, Some(&FailedNotifier), &|| Ok(())).is_err());
        assert!(
            barrier.waits.lock().unwrap().is_empty(),
            "notification failure must precede acknowledgement wait"
        );
        assert!(matches!(
            run(500, Some(&FailedNotifier), &|| Ok(())),
            Err(ClusterDdlError::SchemaSync(_))
        ));
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].state, JobState::DONE);
        assert_eq!(active[0].last_schema_version, before + 1);
        assert!(load_history_persisted_ddl_job(opener.clone(), 500, timeout)
            .unwrap()
            .is_none());
        assert!(barrier.cleaned.lock().unwrap().is_empty());
        barrier.fail.store(false, Ordering::Relaxed);
        run(500, Some(&FailedNotifier), &|| Ok(())).unwrap();
        let mut history = load_history_persisted_ddl_job(opener.clone(), 500, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::SYNCED);
        assert_eq!(history.last_schema_version, before + 1);
        assert_eq!(
            tidb_model::get_create_schema_args(&mut history)
                .unwrap()
                .unwrap()
                .read()
                .db_info
                .get()
                .unwrap()
                .read()
                .id,
            501
        );
        assert_eq!(
            *barrier.waits.lock().unwrap(),
            vec![(500, before + 1), (500, before + 1)]
        );
        assert_eq!(*barrier.cleaned.lock().unwrap(), vec![500]);
        assert!(load_active_persisted_ddl_jobs(opener.clone(), timeout, 0)
            .unwrap()
            .is_empty());
        // Every DROP transition must wait, not only the final one.
        job.id = 502;
        job.type_ = ActionType::ACTION_DROP_SCHEMA;
        job.state = JobState::QUEUEING;
        job.schema_state = SchemaState::PUBLIC;
        job.fill_args(Some(GoShared::new(tidb_model::DropSchemaArgs::default())));
        seed(&mut job);
        barrier.fail.store(true, Ordering::Relaxed);
        assert!(run(502, None, &|| Ok(())).is_err());
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active[0].schema_state, SchemaState::WRITE_ONLY);
        assert_eq!(active[0].state, JobState::RUNNING);
        barrier.fail.store(false, Ordering::Relaxed);
        run(502, None, &|| Ok(())).unwrap();
        let waits = barrier.waits.lock().unwrap();
        assert_eq!(
            &waits[2..],
            &[
                (502, before + 2),
                (502, before + 2),
                (502, before + 3),
                (502, before + 4)
            ]
        );
        assert_eq!(
            load_history_persisted_ddl_job(opener.clone(), 502, timeout)
                .unwrap()
                .unwrap()
                .state,
            JobState::SYNCED
        );
        let wait_count = waits.len();
        drop(waits);
        // A cancelled action also checks ownership before its single history
        // commit. Retrying after owner loss records the new failure only once
        // and must not publish or wait for an unchanged schema version.
        job.id = 503;
        job.state = JobState::QUEUEING;
        job.error_count = 2;
        seed(&mut job);
        let checks = std::sync::atomic::AtomicUsize::new(0);
        let lost_before_cancel_commit = || {
            if checks.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err("owner lost".into())
            }
        };
        assert!(run(503, None, &lost_before_cancel_commit).is_err());
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active[0].state, JobState::QUEUEING);
        assert_eq!(active[0].error_count, 2);
        assert!(load_history_persisted_ddl_job(opener.clone(), 503, timeout)
            .unwrap()
            .is_none());
        run(503, Some(&FailedNotifier), &|| Ok(())).unwrap();
        let history = load_history_persisted_ddl_job(opener.clone(), 503, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::CANCELLED);
        assert_eq!(history.error_count, 3);
        assert_eq!(
            history.error.as_ref().unwrap().read().code().value(),
            tidb_error::tidb::errcode::ErrDBDropExists as isize
        );
        assert_eq!(barrier.waits.lock().unwrap().len(), wait_count);
        assert_eq!(
            tidb_exec::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout)
                .unwrap()
                .schema_version,
            before + 4
        );
    }

    #[test]
    fn independent_store_authorities_can_both_own_ddl() {
        use tidb_owner::Manager;
        let first = local_ddl_owner("isolated-ddl-first".to_owned(), u64::MAX - 1);
        let second = local_ddl_owner("isolated-ddl-second".to_owned(), u64::MAX);
        first.campaign_owner(&[]).unwrap();
        second.campaign_owner(&[]).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !(first.is_owner() && second.is_owner()) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let both_owned = first.is_owner() && second.is_owner();
        let competing = local_ddl_owner("isolated-ddl-competing".to_owned(), u64::MAX - 1);
        competing.campaign_owner(&[]).unwrap();
        let same_store_exclusive = first.is_owner() && !competing.is_owner();
        competing.close();
        first.close();
        second.close();
        assert!(
            both_owned,
            "independent stores must not compete for the same DDL owner"
        );
        assert!(
            same_store_exclusive,
            "one store must retain a single DDL owner"
        );
    }

    #[test]
    fn tiflash_status_uses_the_shared_ddl_catalog_publication() {
        if crate::isolate_process_globals() {
            return;
        }
        use tidb_exec::tiflash_replica_manager::TiFlashReplicaControl;
        use tidb_meta::{key, value};
        use tidb_model::{DBInfo, GoShared, SchemaState, TableInfo, TiFlashReplicaInfo};
        use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome};

        let (_authority, _pd, opener) = crate::unistore_node::in_process_write_stack().unwrap();
        let timeout = Duration::from_secs(5);
        let database = DBInfo {
            id: 1,
            name: tidb_ast::CiString::new("test"),
            state: SchemaState::PUBLIC,
            ..Default::default()
        };
        let table = TableInfo {
            id: 7,
            name: tidb_ast::CiString::new("replica"),
            state: SchemaState::PUBLIC,
            tiflash_replica: Some(GoShared::new(TiFlashReplicaInfo {
                count: 1,
                ..Default::default()
            })),
            ..Default::default()
        };
        let seed = opener
            .begin()
            .unwrap()
            .commit(
                vec![
                    BufferMutation::set(
                        key::database_kv_key(1),
                        value::serialize_db_info(&database).unwrap(),
                    )
                    .unwrap(),
                    BufferMutation::set(
                        key::table_kv_key(1, 7),
                        value::serialize_table_info(&table).unwrap(),
                    )
                    .unwrap(),
                    BufferMutation::set(key::schema_version_kv_key(), b"1").unwrap(),
                ],
                &tidb_txnkv::UnaryCallContext::with_timeout(timeout),
            )
            .unwrap();
        assert!(matches!(seed, OptimisticCommitOutcome::Committed(_)));
        let catalog = Arc::new(SharedClusterCatalog::new(
            tidb_exec::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout).unwrap(),
        ));
        let mut info = tidb_domain::serverinfo::ServerInfo::default();
        info.static_info.id = tidb_domain::serverinfo_syncer::new_node_id();
        let server_info = Arc::new(tidb_domain::serverinfo_syncer::Syncer::new(info, None));
        let ddl = RealClusterDdl::new(
            opener,
            catalog.clone(),
            timeout,
            None,
            server_info,
            None,
            Arc::new(SchemaValidator::new(Duration::from_secs(45))),
            false,
        )
        .unwrap();
        assert!(!ddl.is_owner(), "run-ddl=false must not campaign");
        ddl.update_replica_status(7, true).unwrap();
        let published = catalog.load();
        assert_eq!(published.schema_version, 2);
        let (_, table) = published.find_table("test", "replica").unwrap();
        assert!(table.tiflash_replica.as_ref().unwrap().read().available);
        ddl.update_replica_status(7, true).unwrap();
        assert_eq!(
            catalog.load().schema_version,
            2,
            "repeated status does not advance the schema"
        );
        assert!(ddl.update_replica_status(8, true).is_err());
    }

    #[test]
    fn closed_server_state_watch_rewatches_and_reloads() {
        let rewatched = std::sync::atomic::AtomicBool::new(false);
        assert!(handle_server_state_watch::<()>(
            Err(std::sync::mpsc::TryRecvError::Disconnected),
            || rewatched.store(true, Ordering::Release),
        ));
        assert!(rewatched.load(Ordering::Acquire));

        assert!(!handle_server_state_watch::<()>(
            Err(std::sync::mpsc::TryRecvError::Empty),
            || panic!("an open idle watch must not be replaced"),
        ));
    }
}

impl<C, L, P> ClusterSchemaSync<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn reload_catalog(&self) -> Result<(), String> {
        let current = self.catalog.load();
        let at = reload_catalog_from_cluster(&self.opener, self.timeout, &current)
            .map_err(|error| error.to_string())?;
        match crate::real_tikv_node::note_reload(&self.schema_validator, current.schema_version, at)
        {
            CatalogReloadPass::Unchanged => {}
            CatalogReloadPass::Diffs(catalog) | CatalogReloadPass::Full(catalog) => {
                self.catalog.store(catalog);
            }
        }
        Ok(())
    }
}

impl<C, L, P> SchemaLoader for ClusterSchemaSync<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn reload(&self) -> Result<(), String> {
        self.reload_catalog()
    }
}

impl<C, L, P> DdlSchemaSync for ClusterSchemaSync<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn owner_id(&self) -> &str {
        &self.owner_id
    }

    fn wait_version_synced(&self, ddl_job_id: i64, version: i64) -> Result<(), String> {
        self.reload_catalog()?;
        let Some(syncer) = self.schema_version_syncer.as_ref() else {
            return Ok(());
        };
        let context = tidb_schemaver::Context::with_timeout(
            &tidb_schemaver::Context::background(),
            self.timeout,
        );
        syncer
            .wait_version_synced(&context, ddl_job_id, version, false)
            .map(|_summary| ())
    }

    fn clean_job_versions(&self, ddl_job_id: i64) -> Result<(), String> {
        let Some(etcd) = self.notifier.as_ref() else {
            return Ok(());
        };
        etcd.delete_prefix(format!("/tidb/ddl/all_schema_by_job_versions/{ddl_job_id}/").as_bytes())
            .map_err(|error| error.to_string())
    }
}

impl<C, L, P> ClusterDdl for RealClusterDdl<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn execute(&self, statement: &DdlStatement) -> Result<ClusterDdlReport, SqlQueryError> {
        if matches!(
            statement,
            DdlStatement::AddCheckConstraint { .. }
                | DdlStatement::DropCheckConstraint { .. }
                | DdlStatement::AlterCheckConstraint { .. }
        ) {
            let ddl_job_id = submit_check_constraint_job_with_retry(
                Arc::clone(&self.opener),
                statement,
                self.timeout,
                self.server_state.is_upgrading_state(),
                self.min_job_id_refresher.current_min_job_id(),
            )
            .map_err(cluster_ddl_error)?;
            self.notify_new_job_submitted();
            let report = self.wait_persisted_job(ddl_job_id)?;
            self.refresh_catalog();
            return Ok(report);
        }
        let notifier = self
            .notifier
            .as_ref()
            .map(|client| Arc::as_ref(client) as &dyn SchemaVersionNotifier);
        let report = commit_cluster_ddl_with_backfill(
            Arc::clone(&self.opener),
            statement,
            self.timeout,
            notifier,
            &KvTableIndexBackfiller,
            &KvTableIndexBackfiller,
            &KvTableIndexBackfiller,
            self.schema_sync.as_ref(),
        )
        .map_err(cluster_ddl_error)?;
        self.refresh_catalog();
        Ok(report)
    }
}

impl<C, L, P> tidb_exec::tiflash_replica_manager::TiFlashReplicaControl for RealClusterDdl<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn is_owner(&self) -> bool {
        self.owner.is_owner()
    }

    fn update_replica_status(&self, table_id: i64, available: bool) -> Result<(), String> {
        self.execute(&DdlStatement::UpdateTiFlashReplicaStatus {
            table_id,
            available,
        })
        .map(|_| ())
        .map_err(|error| error.message)
    }
}

impl<C, L, P> Drop for RealClusterDdl<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn drop(&mut self) {
        self.server_state_context.cancel();
        let _ = self.min_job_id_stop.send(());
        if let Some(worker) = self.min_job_id_worker.take() {
            let _ = worker.join();
        }
        self.owner.close();
        self.scheduler.stop();
    }
}

/// The index backfill, performed by the very code an `INSERT` uses.
///
/// `KvTable::create_index` is Go's reorg step expressed at this tier: it walks
/// the table's rows, computes each entry from the row, and refuses a UNIQUE
/// index whose existing rows already collide -- leaving the table without the
/// index, which is what TiDB answers too. Nothing about it is
/// cluster-specific; it is the same call the in-process tier makes, over the
/// same `TableStorage` seam, which here is bound to the DDL transaction's
/// snapshot and staging buffer. That reuse is the point: an index whose
/// entries were written by a second implementation would disagree with the one
/// the write path maintains from the next `INSERT` onwards.
struct KvTableIndexBackfiller;

impl IndexBackfiller for KvTableIndexBackfiller {
    fn stage(
        &self,
        plan: &IndexBackfill,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), String> {
        let storage = ClusterTableStorage::new(buffer.clone(), snapshot);
        // Built from the table as it was BEFORE the change, which is the shape
        // its stored rows have -- and, for a DROP, the state in which the
        // index being removed is still one of the table's own.
        //
        // With NO auto-increment counter, because a backfill allocates no id:
        // `create_index`/`drop_index` scan the rows that exist and write or
        // delete the index entries those rows produce, and neither reads the
        // allocator. Naming that absence is what keeps it honest -- the plan
        // carries no database id, so the alternative would be inventing one
        // and handing over a counter starting at zero, which against shared
        // cluster storage re-issues ids the table already holds.
        let mut table = cluster_table(&plan.table, &storage, &AutoIdSource::Unavailable)?
            .with_new_collation_mode(plan.use_new_collation);
        let index = {
            let index = plan.index.read();
            kv_index(&index, &table.columns)?
        };
        let name = index.name.clone();
        if plan.add {
            // This cluster DDL plan carries no session context, so its
            // backfill uses the DDL statement defaults while still
            // recomputing every generated column through RowDecoder.
            table
                .create_index_with_context(index, &StmtContext::default())
                .map_err(backfill_failure)?;
        } else if !table
            .drop_index(&name, &StmtContext::default().session_zone())
            .map_err(backfill_failure)?
        {
            // The plan found the index on the stored table, so the loader
            // dropping it can only mean the two disagree about what the table
            // has -- which must not end as a silent no-op.
            return Err(format!(
                "index {name} is on the stored table but not on the table this node built \
                 from it, so its entries cannot be removed"
            ));
        }
        Ok(())
    }
}

impl ExchangePartitionValidator for KvTableIndexBackfiller {
    fn validate(
        &self,
        plan: &ExchangePartitionValidation,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError> {
        let storage = ClusterTableStorage::new(buffer.clone(), snapshot);
        let mut standalone = cluster_table(&plan.standalone, &storage, &AutoIdSource::Unavailable)
            .map_err(exchange_validation_internal)?;
        let mut partitioned =
            cluster_table(&plan.partitioned, &storage, &AutoIdSource::Unavailable)
                .map_err(exchange_validation_internal)?;
        let context = StmtContext::for_query();
        let rows = standalone
            .scan_rows_with_context(&RowDecodeContext::for_query(&context))
            .map_err(exchange_validation_table_error)?;
        for row in rows {
            partitioned
                .validate_insert_partitions(&row, &[plan.partition_id], &context)
                .map_err(exchange_validation_table_error)?;
            partitioned
                .validate_check_constraints(&row, &context)
                .map_err(exchange_validation_table_error)?;
        }

        // Go's second restricted query reads the target partition and applies
        // the standalone table's writable constraints. Both scans share the
        // same transaction snapshot so neither direction can race the swap.
        partitioned.restrict_read_to_partitions(&[plan.partition_id]);
        let rows = partitioned
            .scan_rows_with_context(&RowDecodeContext::for_query(&context))
            .map_err(exchange_validation_table_error)?;
        for row in rows {
            standalone
                .validate_check_constraints(&row, &context)
                .map_err(exchange_validation_table_error)?;
        }
        Ok(())
    }
}

impl CheckConstraintValidator for KvTableIndexBackfiller {
    fn validate(
        &self,
        plan: &CheckConstraintValidation,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError> {
        let storage = ClusterTableStorage::new(buffer.clone(), snapshot);
        let mut table = cluster_table(&plan.table, &storage, &AutoIdSource::Unavailable)
            .map_err(check_constraint_validation_internal)?;
        let rows = table
            .scan_rows_with_context(&RowDecodeContext::for_query(&plan.context.0))
            .map_err(|error| {
                check_constraint_validation_table_error(error, &plan.constraint_name)
            })?;
        for row in rows {
            table
                .validate_check_constraints(&row, &plan.context.0)
                .map_err(|error| {
                    check_constraint_validation_table_error(error, &plan.constraint_name)
                })?;
        }
        Ok(())
    }
}

fn check_constraint_validation_internal(message: String) -> LockSqlError {
    LockSqlError {
        code: 1105,
        state: *b"HY000",
        message,
    }
}

fn check_constraint_validation_table_error(
    error: tidb_executor::kv_table::KvTableError,
    constraint_name: &str,
) -> LockSqlError {
    match error {
        tidb_executor::kv_table::KvTableError::CheckConstraintViolated(name) => LockSqlError {
            code: tidb_error::tidb::errcode::ErrCheckConstraintViolated,
            state: *b"HY000",
            message: format!("Check constraint '{name}' is violated."),
        },
        other => LockSqlError {
            code: 1105,
            state: *b"HY000",
            message: format!(
                "validation of check constraint '{constraint_name}' failed: {other:?}"
            ),
        },
    }
}

fn exchange_validation_internal(message: String) -> LockSqlError {
    LockSqlError {
        code: 1105,
        state: *b"HY000",
        message,
    }
}

fn exchange_validation_table_error(error: tidb_executor::kv_table::KvTableError) -> LockSqlError {
    match error {
        tidb_executor::kv_table::KvTableError::RowDoesNotMatchGivenPartitionSet
        | tidb_executor::kv_table::KvTableError::NoPartitionForValue(_)
        | tidb_executor::kv_table::KvTableError::CheckConstraintViolated(_) => LockSqlError {
            code: tidb_error::tidb::errcode::ErrRowDoesNotMatchPartition,
            state: *b"HY000",
            message: "Found a row that does not match the partition".to_owned(),
        },
        tidb_executor::kv_table::KvTableError::CheckConstraint {
            eval: Some(eval), ..
        } => {
            let error = tidb_executor::DriverError::Exec(tidb_executor::ExecError::Eval(eval))
                .to_mysql_error();
            LockSqlError {
                code: error.code,
                state: error.state,
                message: error.message,
            }
        }
        other => exchange_validation_internal(format!("{other:?}")),
    }
}

/// Renders a failed walk in the words the failure has, not as a Debug dump.
///
/// The one a user actually meets is the duplicate: `CREATE UNIQUE INDEX` over
/// rows that already collide is Go's 1062 naming `table.index`, and the
/// statement leaves the table WITHOUT the index -- which is exactly what
/// happens here, because the whole change is one transaction that does not
/// commit.
fn backfill_failure(error: tidb_executor::kv_table::KvTableError) -> String {
    match error {
        tidb_executor::kv_table::KvTableError::DuplicateEntry { value, key } => {
            format!("Duplicate entry '{value}' for key '{key}'")
        }
        other => format!("{other:?}"),
    }
}
