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

//! The DDL seam: this node's one route to the cluster's stored schema (Go's
//! `ddl` package from the submitting side's `DoDDLJob` to the owner's job
//! worker). It was split out of `tidb-server`'s `cluster_session_node` and now
//! sits below the session; see that module's doc comment for how a DDL
//! statement is routed here and what happens to the connection's own
//! catalog afterwards.

use tidb_ddl_build::partition_reorg::KvTablePartitionReorg;
use tidb_ddl_build::table_backfill::KvTableBackfill;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use tidb_txnkv::transaction::StoreClient;

use tidb_ddl_serverstate::{Context as ServerStateContext, EtcdSyncer, MemSyncer, Syncer};
use tidb_infoschema_issyncer::catalog_watch::{CatalogReloadPass, SharedCatalog as SharedClusterCatalog};
use crate::cluster_ddl::{
    CheckConstraintValidation, DdlPlanError, DdlStatement, ExchangePartitionValidation,
};
use crate::ddl_job_scheduler::{must_reload_schemas, SchemaLoader};
use crate::ddl_job_table::DdlJobTable;
use crate::ddl_systable::MinJobIdRefresher;
use tidb_store_driver::pessimistic_lock_error::LockSqlError;
use tidb_infoschema_issyncer::real_tikv_catalog::reload_catalog_from_cluster;
use crate::real_tikv_ddl::{
    answer_ddl_without_job, load_active_persisted_ddl_jobs_cached, load_history_persisted_ddl_job,
    load_min_persisted_ddl_job_id_cached, run_persisted_ddl_job,
    submit_check_constraint_job_with_retry, CheckConstraintValidator, ClusterDdlReport,
    DdlSchemaSync, IndexBackfiller, PersistedDdlJobOutcome, SchemaVersionNotifier,
};

use tidb_txnkv::transaction::RealOptimisticTransactionOpener;
use tidb_infoschema_isvalidator::SchemaValidator;
use tidb_kv::cluster_storage::{ClusterSnapshot, ClusterTableStorage, MutationBuffer};
use tidb_table::kv_table::RowDecodeContext;
use tidb_pd_client::EtcdClient;

use crate::real_tikv_ddl::cluster_ddl_error;
use tidb_store_driver::sql_query_error::SqlQueryError;
use tidb_table::table_from_meta::{cluster_table, AutoIdSource};

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
    /// be stale. `job_context` is what Go's submitter copies from the issuing
    /// session into the job: the statement text (`Job.Query`), SQL mode and
    /// `tidb_cdc_write_source`, which exempts a change TiCDC replicates from
    /// the BDR submit admission.
    fn execute(
        &self,
        statement: &DdlStatement,
        job_context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError>;

    /// Go `do.DDL().OwnerManager().IsOwner()`: whether this node owns the DDL
    /// job queue right now.
    ///
    /// The domain's cluster-wide writers consult this on every round rather
    /// than once, because ownership moves while a server runs. A DDL that has
    /// not started owns nothing, which is also what Go answers before
    /// `ddl.Start`, so that is the default.
    fn is_owner(&self) -> bool {
        false
    }

    /// Go `infoschema.GetTiFlashStoreCount(ctx.GetStore())`: the TiFlash
    /// stores PD lists, which `checkTiFlashReplicaCount` bounds a replica
    /// count by.
    fn tiflash_store_count(&self) -> Result<u64, SqlQueryError>;
}

/// A named thread with the SQL worker's stack, for every background worker
/// that runs a transaction or a catalog reload (the same stack the server's
/// `sql_worker_thread` gives its own).
pub(crate) fn sql_worker_thread(name: impl Into<String>) -> std::thread::Builder {
    std::thread::Builder::new()
        .name(name.into())
        .stack_size(tidb_worker_stack::WORKER_STACK_BYTES)
}

/// zap `zap.Error(err)`.
pub(crate) fn zap_error(error: impl std::fmt::Display) -> tidb_log::Field {
    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))
}

/// Go `jobV2FirstVer`: every TiDB at or above it runs job version 2.
const JOB_V2_FIRST_VER: (u64, u64, u64) = (8, 4, 0);
/// Go `globalIdxV1FirstVer`: every TiDB at or above it reads the global
/// index V1 key format (the partition ID in a non-unique global key).
const GLOBAL_IDX_V1_FIRST_VER: (u64, u64, u64) = (8, 5, 6);
/// Go `detectJobVerInterval`.
const DETECT_JOB_VER_INTERVAL: Duration = Duration::from_secs(10);

/// Go `semver.NewVersion` with the pre-release cleared, as
/// `detectAndUpdateJobVersionOnce` compares it: `MAJOR.MINOR.PATCH`, an
/// optional `-pre-release` and `+build`.
fn parse_release_version(text: &str) -> Option<(u64, u64, u64)> {
    let core = text.split('+').next()?.split('-').next()?;
    let mut parts = core.split('.');
    let version = (
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    );
    parts.next().is_none().then_some(version)
}

/// Whether Go's detection has nothing left to wait for.
fn job_version_settled() -> bool {
    tidb_model::get_job_ver_in_use() == tidb_model::JobVersion::V2
        && tidb_model::index::get_global_index_v1_supported()
}

/// Go `ddl.detectAndUpdateJobVersionOnce`: job version 2 once every TiDB in
/// the cluster is at least 8.4.0, global index V1 once every one is at least
/// 8.5.6, read from the servers' registered versions.
fn detect_and_update_job_version_once(
    server_info: &tidb_domain::serverinfo_syncer::Syncer,
) -> Result<(), String> {
    let infos = server_info.all_server_info()?;
    let mut all_support_v2 = true;
    let mut all_support_global_idx_v1 = true;
    for info in infos.values() {
        let tidb_version = &info.static_info.version_info.version;
        let Some(index) = tidb_version.find(tidb_mysql::consts::VersionSeparator) else {
            all_support_v2 = false;
            all_support_global_idx_v1 = false;
            break;
        };
        let release = tidb_version[index + tidb_mysql::consts::VersionSeparator.len()..]
            .trim_start_matches('v');
        let Some(version) = parse_release_version(release) else {
            all_support_v2 = false;
            all_support_global_idx_v1 = false;
            break;
        };
        if version < JOB_V2_FIRST_VER {
            all_support_v2 = false;
        }
        if version < GLOBAL_IDX_V1_FIRST_VER {
            all_support_global_idx_v1 = false;
        }
        if !all_support_v2 && !all_support_global_idx_v1 {
            break;
        }
    }
    let target = if all_support_v2 {
        tidb_model::JobVersion::V2
    } else {
        tidb_model::JobVersion::V1
    };
    if tidb_model::get_job_ver_in_use() != target {
        tidb_model::set_job_ver_in_use(target);
    }
    if tidb_model::index::get_global_index_v1_supported() != all_support_global_idx_v1 {
        tidb_model::index::set_global_index_v1_supported(all_support_global_idx_v1);
    }
    Ok(())
}

/// Go `ddl.detectAndUpdateJobVersion`: without etcd (a single node) job
/// version 2 and global index V1 hold at once; otherwise detect now and, while
/// a peer is older, re-detect every `detectJobVerInterval` until both hold.
fn detect_and_update_job_version(
    has_etcd: bool,
    server_info: &Arc<tidb_domain::serverinfo_syncer::Syncer>,
) -> Option<(Sender<()>, JoinHandle<()>)> {
    if !has_etcd {
        tidb_model::set_job_ver_in_use(tidb_model::JobVersion::V2);
        tidb_model::index::set_global_index_v1_supported(true);
        return None;
    }
    if let Err(error) = detect_and_update_job_version_once(server_info) {
        tidb_ddl_logutil::ddl_logger().warn(
            "detect job version failed",
            &[tidb_log::Field::new(
                "err",
                tidb_log::Value::Str(error.to_string()),
            )],
        );
    }
    if job_version_settled() {
        return None;
    }
    let (stop, stopped) = channel::<()>();
    let server_info = Arc::clone(server_info);
    let worker = sql_worker_thread("ddl-detect-job-version")
        .spawn(move || loop {
            match stopped.recv_timeout(DETECT_JOB_VER_INTERVAL) {
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                _ => return,
            }
            if let Err(error) = detect_and_update_job_version_once(&server_info) {
                tidb_ddl_logutil::sample_logger().warn(
                    "detect job version failed",
                    &[tidb_log::Field::new(
                        "err",
                        tidb_log::Value::Str(error.to_string()),
                    )],
                );
            }
            if job_version_settled() {
                return;
            }
        })
        .ok()?;
    Some((stop, worker))
}

const DDL_OWNER_KEY: &str = "/tidb/ddl/fg/owner";
const ADDING_DDL_JOB_NOTIFY_KEY: &[u8] = b"/tidb/ddl/add_ddl_job_general";
const DDL_SCHEDULER_INTERVAL: Duration = Duration::from_secs(1);

fn wait_ddl_retry(stopped: &Receiver<()>, delay: Duration) -> Result<(), String> {
    match stopped.recv_timeout(delay) {
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Ok(()),
        Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            Err("DDL worker stopped or ownership lost".to_owned())
        }
    }
}

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

struct ClusterSchemaSync<S>
where
    S: StoreClient,
{
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    catalog: Arc<SharedClusterCatalog>,
    /// The node's schema validator, renewed by this reload like the reload
    /// thread's own passes renew it.
    schema_validator: Arc<SchemaValidator>,
    timeout: Duration,
    notifier: Option<Arc<EtcdClient>>,
    schema_version_syncer: Option<Arc<dyn tidb_schemaver::Syncer>>,
    owner_id: String,
    /// The node's allocator registry (the scheduler's own), so the reload
    /// after a rebase drops the ranges reserved before it.
    row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
}

struct SchedulerWorker {
    stop: Arc<AtomicBool>,
    stop_tx: Sender<()>,
    handle: JoinHandle<()>,
    _adding_job_watcher: Option<tidb_pd_client::EtcdWatcher>,
}

struct PersistedDdlScheduler<S>
where
    S: StoreClient,
{
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    notifier: Option<Arc<EtcdClient>>,
    schema_sync: Arc<ClusterSchemaSync<S>>,
    wake: Arc<(Mutex<u64>, Condvar)>,
    server_state: Arc<dyn Syncer>,
    server_state_context: ServerStateContext,
    min_job_id_refresher: Arc<MinJobIdRefresher>,
    owner: Mutex<Option<Arc<dyn tidb_owner::Manager>>>,
    worker: Mutex<Option<SchedulerWorker>>,
    /// The node's row-ID allocators: Go's internal INSERTs into `mysql.*`
    /// tables allocate from them.
    row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
    /// Go `delRange`'s emulator, on a store without delete-range support.
    del_range: Arc<std::sync::OnceLock<crate::real_tikv_ddl::delete_range::DelRangeEmulator>>,
    /// Go `ddlJobDoneChMap`, shared by this node's submitting sessions.
    ddl_job_done_ch_map: Arc<crate::real_tikv_ddl::DdlJobDoneChMap>,
    /// Go `pkg/ddl/session.Pool`: the sessions the node hands DDL.
    session_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
}

impl<S> PersistedDdlScheduler<S>
where
    S: StoreClient,
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
                    tidb_ddl_logutil::ddl_logger().warn(
                        "the owner sets global state to owner operator value failed",
                        &[zap_error(error)],
                    );
                }
            }
            Err(error) => tidb_ddl_logutil::ddl_logger()
                .warn("get global state failed", &[zap_error(error)]),
        }
    }

    fn run_loop(
        opener: Arc<RealOptimisticTransactionOpener<S>>,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
        schema_sync: Arc<ClusterSchemaSync<S>>,
        wake: Arc<(Mutex<u64>, Condvar)>,
        server_state: Arc<dyn Syncer>,
        server_state_context: ServerStateContext,
        min_job_id_refresher: Arc<MinJobIdRefresher>,
        owner: Arc<dyn tidb_owner::Manager>,
        stop: Arc<AtomicBool>,
        stopped: Receiver<()>,
        row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
        del_range: Arc<
            std::sync::OnceLock<crate::real_tikv_ddl::delete_range::DelRangeEmulator>,
        >,
        ddl_job_done_ch_map: Arc<crate::real_tikv_ddl::DdlJobDoneChMap>,
        session_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
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
            // The wake generation is read BEFORE the jobs are loaded: a job
            // submitted while this pass runs bumps it, and the wait below then
            // returns at once instead of sleeping a whole interval with the
            // job queued (Go's buffered `ddlJobCh` keeps that notification).
            let observed = {
                let (generation, _) = &*wake;
                *generation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            };
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
                        if !crate::cluster_ddl::supports_persisted_ddl_job(job.type_) {
                            continue;
                        }
                        let check_owner = || {
                            if stop.load(Ordering::Acquire) || !owner.is_owner() {
                                Err("DDL worker stopped or ownership lost".to_owned())
                            } else {
                                Ok(())
                            }
                        };
                        let backfiller = KvTableIndexBackfiller::new(&row_ids, &session_pool);
                        // Go `getJobRunCtx`: the job context carries the
                        // submitter's done signal, when this node has one.
                        let job_done = ddl_job_done_ch_map.load(job.id);
                        match run_persisted_ddl_job(
                            Arc::clone(&opener),
                            job.id,
                            timeout,
                            job_done.as_deref(),
                            notifier_ref,
                            &backfiller,
                            &backfiller,
                            schema_sync.as_ref(),
                            &check_owner,
                            &|delay| wait_ddl_retry(&stopped, delay),
                        ) {
                            Ok(PersistedDdlJobOutcome::Finished) => {
                                // Go `addDelRangeJob` wakes the emulator.
                                if let Some(emulator) = del_range.get() {
                                    emulator.notify();
                                }
                            }
                            Ok(PersistedDdlJobOutcome::Paused) => {}
                            Err(error) => tidb_ddl_logutil::ddl_logger().info(
                                "transit one job step and wait sync failed",
                                &[
                                    zap_error(error),
                                    tidb_log::Field::new(
                                        "job",
                                        tidb_log::Value::Str(job.to_string()),
                                    ),
                                ],
                            ),
                        }
                    }
                }
                Err(error) => tidb_ddl_logutil::sample_logger()
                    .warn("load and deliver jobs failed", &[zap_error(error)]),
            }

            let (generation, condvar) = &*wake;
            let guard = generation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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

impl<S> tidb_owner::Listener for PersistedDdlScheduler<S>
where
    S: StoreClient,
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
                    // Not log content: Go's etcd watch cannot fail to start,
                    // so this degraded state has no Go line; kept on stderr.
                    eprintln!(
                        "{{\"level\":\"warning\",\"event\":\"ddl_job_notify_watch_failed\",\"error\":{}}}",
                        serde_json::to_string(&error.to_string())
                            .unwrap_or_else(|_| "\"unprintable\"".to_owned())
                    );
                    None
                }
            }
        });
        // The scheduler runs a job's whole course on this thread -- the
        // meta transaction's commit through client-rust's `block_on`, the
        // backfill's scans and evaluation -- the work a SQL worker does, on
        // the SQL worker's reserved stack. The default 2 MB overflowed in a
        // debug build when a job's commit had to resolve a leftover lock
        // (client-rust's `ResolveLock` retry chain on top of the commit
        // future): `ddl/constraint`, `ddl/bdr_mode` and `infoschema/tables`
        // aborted the whole-corpus replay over the embedded store.
        let handle = std::thread::Builder::new()
            .name("ddl-job-scheduler".to_owned())
            .stack_size(tidb_worker_stack::WORKER_STACK_BYTES)
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
                let row_ids = Arc::clone(&self.row_ids);
                let del_range = Arc::clone(&self.del_range);
                let ddl_job_done_ch_map = Arc::clone(&self.ddl_job_done_ch_map);
                let session_pool = Arc::clone(&self.session_pool);
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
                        row_ids,
                        del_range,
                        ddl_job_done_ch_map,
                        session_pool,
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
pub struct RealClusterDdl<S = tikv_client::PdRpcClient>
where
    S: StoreClient,
{
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    auto_ids: Option<Arc<tidb_autoid::auto_id_client::AutoIdClient>>,
    /// The etcd client this node announces its catalog changes through, so
    /// peers' watches fire promptly. `None` leaves them to their lease tick;
    /// a failed announcement is a warning, never a failed DDL.
    notifier: Option<Arc<EtcdClient>>,
    schema_sync: Arc<ClusterSchemaSync<S>>,
    scheduler: Arc<PersistedDdlScheduler<S>>,
    owner: Arc<dyn tidb_owner::Manager>,
    server_state: Arc<dyn Syncer>,
    server_state_context: ServerStateContext,
    min_job_id_refresher: Arc<MinJobIdRefresher>,
    min_job_id_stop: Sender<()>,
    min_job_id_worker: Option<JoinHandle<()>>,
    /// Go `detectAndUpdateJobVersion`'s background re-detection, while some
    /// peer is still older than job version 2 / global index V1.
    job_version_detector: Option<(Sender<()>, JoinHandle<()>)>,
}

impl<S> RealClusterDdl<S>
where
    S: StoreClient,
{
    /// Go `newDelRangeManager` for a store without delete-range support
    /// (`SupportDeleteRange` false, the embedded store): the owner works off
    /// `mysql.gc_delete_range` itself.
    pub fn with_delete_range_emulator(self) -> Result<Self, String> {
        let emulator = crate::real_tikv_ddl::delete_range::start_del_range_emulator(
            Arc::clone(&self.opener),
            self.timeout,
            Arc::clone(&self.scheduler.row_ids),
        )?;
        let _ = self.scheduler.del_range.set(emulator);
        Ok(self)
    }

    /// DDL rebases use the process AutoID service rather than stale IID metadata.
    pub fn with_auto_ids(mut self, client: Arc<tidb_autoid::auto_id_client::AutoIdClient>) -> Self {
        self.auto_ids = Some(client);
        self
    }

    /// Binds the writer to an already-connected authority and the catalog slot
    /// the reload thread publishes into.
    pub fn new(
        opener: RealOptimisticTransactionOpener<S>,
        catalog: Arc<SharedClusterCatalog>,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
        server_info: Arc<tidb_domain::serverinfo_syncer::Syncer>,
        schema_version_syncer: Option<Arc<dyn tidb_schemaver::Syncer>>,
        schema_validator: Arc<SchemaValidator>,
        campaign_owner: bool,
        session_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
        row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
    ) -> Result<Self, String> {
        Self::new_with_min_job_id_refresher(
            opener,
            catalog,
            timeout,
            notifier,
            server_info,
            schema_version_syncer,
            schema_validator,
            campaign_owner,
            Arc::new(MinJobIdRefresher::new()),
            session_pool,
            row_ids,
        )
    }

    /// Constructs the DDL owner with a refresher shared by the schema ACK
    /// loop. Go's domain owns one `MinJobIDRefresher`; both the scheduler and
    /// `refreshMDLCheckTableInfo` read that same monotonic lower bound.
    pub fn new_with_min_job_id_refresher(
        opener: RealOptimisticTransactionOpener<S>,
        catalog: Arc<SharedClusterCatalog>,
        timeout: Duration,
        notifier: Option<Arc<EtcdClient>>,
        server_info: Arc<tidb_domain::serverinfo_syncer::Syncer>,
        schema_version_syncer: Option<Arc<dyn tidb_schemaver::Syncer>>,
        schema_validator: Arc<SchemaValidator>,
        campaign_owner: bool,
        min_job_id_refresher: Arc<MinJobIdRefresher>,
        session_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
        row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
    ) -> Result<Self, String> {
        let owner_id = server_info.local_server_info().static_info.id;
        // Go `ddl.Start`: on the classic kernel, settle the job version and
        // the global index V1 key format before any job is submitted.
        let job_version_detector = if tidb_config::kerneltype::is_classic() {
            detect_and_update_job_version(notifier.is_some(), &server_info)
        } else {
            None
        };
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
        let (min_job_id_stop, min_job_id_stopped) = channel();
        let min_job_id_worker = sql_worker_thread("ddl-min-job-id-refresher")
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
            row_ids: Arc::clone(&row_ids),
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
            row_ids,
            del_range: Arc::new(std::sync::OnceLock::new()),
            ddl_job_done_ch_map: Arc::default(),
            session_pool,
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
            auto_ids: None,
            opener,
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
            job_version_detector,
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
    /// is dropped and the statement stands. Go has no such inline reload and
    /// so no log line for it.
    fn refresh_catalog(&self) {
        let _ = self.reload_catalog();
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
            tidb_ddl_logutil::ddl_logger()
                .info("notify new DDL job failed", &[zap_error(error)]);
        }
    }

    /// Go `DoDDLJobWrapper`'s wait: the done signal the scheduler closes
    /// once the job is done and its reorg handles are cleaned, or the check
    /// ticker (`getJobCheckInterval`, capped by ten leases), then the job's
    /// history. The signal is dropped when the wait ends (`delJobDoneCh`).
    fn wait_persisted_job(
        &self,
        ddl_job_id: i64,
        action: tidb_model::ActionType,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        let result = self.wait_persisted_job_done(ddl_job_id, action);
        self.scheduler.ddl_job_done_ch_map.delete(ddl_job_id);
        result
    }

    fn wait_persisted_job_done(
        &self,
        ddl_job_id: i64,
        action: tidb_model::ActionType,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        use crate::real_tikv_ddl::{choose_lease_time, get_job_check_interval};
        let lease = self.schema_sync.schema_validator.lease().saturating_mul(10);
        let mut tick = choose_lease_time(lease, get_job_check_interval(action, 0).0);
        let mut next_tick = std::time::Instant::now() + tick;
        let mut checks = 0;
        let mut notify = self.scheduler.ddl_job_done_ch_map.load(ddl_job_id);
        loop {
            match notify.as_ref() {
                // A closed signal is read once; later rounds wait on the
                // ticker alone.
                Some(done) if done.wait_until(next_tick) => notify = None,
                _ => {
                    if notify.is_none() {
                        let now = std::time::Instant::now();
                        if next_tick > now {
                            std::thread::sleep(next_tick - now);
                        }
                    }
                    checks += 1;
                    // Go `updateTickerInterval`: a new interval restarts the
                    // ticker; an unchanged one keeps its period.
                    let (interval, changed) = get_job_check_interval(action, checks);
                    let now = std::time::Instant::now();
                    if changed {
                        tick = choose_lease_time(lease, interval);
                        next_tick = now + tick;
                    } else {
                        while next_tick <= now {
                            next_tick += tick;
                        }
                    }
                }
            }
            if let Some(job) =
                load_history_persisted_ddl_job(Arc::clone(&self.opener), ddl_job_id, self.timeout)
                    .map_err(cluster_ddl_error)?
            {
                // Go `DoDDLJob`: a synced history job succeeded whatever
                // error a step recorded on it (a non-revertible sub-job that
                // failed and cancelled itself); only a rolled-back or
                // cancelled job answers its error.
                if job.state.is_synced() {
                    // TRUNCATE's new identity, which a session holding a lock
                    // on the old one moves its lock to and which is
                    // pre-split like a created table.
                    let created_id = matches!(
                        job.type_,
                        tidb_model::ActionType::ACTION_TRUNCATE_TABLE
                            | tidb_model::ActionType::ACTION_CREATE_TABLE
                    )
                    .then(|| {
                        job.binlog_info
                            .as_ref()
                            .and_then(|history| history.read().table_info.clone())
                            .map(|table| table.read().id)
                    })
                    .flatten();
                    return Ok(ClusterDdlReport::Applied {
                        schema_version: job.last_schema_version,
                        created_id,
                        warnings: synced_job_warnings(&job),
                    });
                }
                if let Some(error) = job.error.as_ref() {
                    let error = error.read();
                    return Err(persisted_job_error(&error));
                }
                return Err(SqlQueryError::unknown(format!(
                    "DDL job {ddl_job_id} reached terminal state {} without an error",
                    job.state
                )));
            }
        }
    }
}

fn persisted_job_error(error: &tidb_error::terror::TerrorError) -> SqlQueryError {
    let error = crate::cluster_ddl::ddl_job_error_to_sql_error(error);
    SqlQueryError::from(error)
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

    /// The backfiller the node builds, its data checks the executor's.
    fn test_backfiller() -> KvTableIndexBackfiller {
        KvTableIndexBackfiller::new(
            &(Arc::new(tidb_table::table_from_meta::LocalTableAutoIds::default())
                as Arc<dyn tidb_table::table_from_meta::TableAutoIds>),
            &(Arc::new(tidb_session::ddl_session_pool::ExecutorDdlSessionPool)
                as Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>),
        )
    }

    #[test]
    fn persisted_history_preserves_source_sqlstate() {
        use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError};
        for (code, state) in [
            (1049, *b"42000"),
            (1146, *b"42S02"),
            (1060, *b"42S21"),
            (3819, *b"HY000"),
        ] {
            // Read the same durable envelope as an owner replacement, including
            // legacy rows written before source RFC identities are preserved.
            for error in [
                TerrorError::compatible(TerrorCode::new(code), "original diagnostic"),
                TerrorError::registered(
                    if code == 3819 {
                        TerrorClass::Ddl
                    } else {
                        TerrorClass::Schema
                    },
                    TerrorCode::new(code),
                    "original diagnostic",
                ),
            ] {
                let encoded = serde_json::to_vec(&error).unwrap();
                let decoded = serde_json::from_slice(&encoded).unwrap();
                let actual = persisted_job_error(&decoded);
                assert_eq!(actual.code, code as u16);
                assert_eq!(actual.state, state);
                assert_eq!(actual.message, "original diagnostic");
            }
        }
        let unknown = TerrorError::synthesize(
            TerrorClass::Ddl,
            tidb_error::terror::CODE_UNKNOWN,
            "plain failure",
        );
        let actual = persisted_job_error(&unknown);
        assert_eq!((actual.code, actual.state), (1105, *b"HY000"));
        assert_eq!(actual.message, "plain failure");
    }

    #[test]
    fn check_validation_source_message() {
        let error = check_constraint_validation_table_error(
            tidb_table::kv_table::KvTableError::Mysql(tidb_error::mysql::MysqlError::from_terror(
                &tidb_error::catalog::table::ERR_CHECK_CONSTRAINT_VIOLATED
                    .fast_generate_by_args(&["MixedCheck".into()]),
            )),
            "MixedCheck",
        );
        let DdlPlanError::Source(source) = error else {
            panic!("CHECK validation must return its typed source error");
        };
        assert_eq!(source.rfc_code(), "ddl:3819");
        assert_eq!(
            source.message(),
            "Check constraint 'mixedcheck' is violated."
        );
        assert!(source.stack().is_some());
        let sql = persisted_job_error(&source);
        assert_eq!(sql.code, 3819);
        assert_eq!(sql.message, source.message());
    }

    #[test]
    fn worker_retry_wait_uses_owner_retirement() {
        let (stop, stopped) = channel();
        assert!(wait_ddl_retry(&stopped, Duration::ZERO).is_ok());
        // A queued retirement and a disconnected owner both interrupt even
        // an arbitrarily long retry delay; no elapsed-time assertion is needed.
        stop.send(()).unwrap();
        assert!(wait_ddl_retry(&stopped, Duration::from_secs(3600)).is_err());
        drop(stop);
        assert!(wait_ddl_retry(&stopped, Duration::from_secs(3600)).is_err());
    }

    fn worker_without_mdl(recover: bool, remove_mdl_table: bool) {
        use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
        use crate::real_tikv_ddl::load_active_persisted_ddl_jobs;
        use tidb_meta::{key, value};
        use tidb_model::{
            ActionType, CreateSchemaArgs, DBInfo, GoField, GoShared, HistoryInfo, Job, JobState,
            JobVersion, SchemaDiff, SchemaState,
        };
        use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome};

        struct Barrier(Mutex<Vec<(i64, i64)>>, AtomicBool);
        impl DdlSchemaSync for Barrier {
            fn owner_id(&self) -> &str {
                "non-mdl-owner"
            }
            fn wait_version_synced(&self, id: i64, version: i64) -> Result<(), String> {
                self.0.lock().unwrap().push((id, version));
                if self.1.load(Ordering::Relaxed) {
                    Err("pending schema version".into())
                } else {
                    Ok(())
                }
            }
            fn clean_job_versions(&self, _: i64) -> Result<(), String> {
                panic!("disabled MDL must not clean per-job acknowledgement keys")
            }
        }
        let (_pd, opener) = crate::embedded_store::in_process_write_stack().unwrap();
        let opener = Arc::new(opener);
        let timeout = Duration::from_secs(5);
        tidb_session::bootstrap_publish::publish_bootstrap(&opener, timeout).unwrap();
        tidb_vardef::set_enable_mdl(false);
        let mut tx = opener.begin().unwrap();
        let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
        let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
        let before = catalog.schema_version;
        let mut job = Job::default();
        job.id = 600;
        job.schema_id = 601;
        job.schema_name = "without_mdl".into();
        job.type_ = ActionType::ACTION_CREATE_SCHEMA;
        job.state = if recover {
            JobState::DONE
        } else {
            JobState::QUEUEING
        };
        job.last_schema_version = if recover { before + 1 } else { 0 };
        job.version = JobVersion::V2;
        job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
        let database = DBInfo {
            id: 601,
            name: tidb_ast::CiString::new("without_mdl"),
            state: SchemaState::PUBLIC,
            ..Default::default()
        };
        job.fill_args(Some(GoShared::new(CreateSchemaArgs {
            db_info: GoField::new(Some(GoShared::new(database.clone_like_go()))),
        })));
        let mut mutations = Vec::new();
        DdlJobTable::locate(&catalog)
            .unwrap()
            .append_insert(&mut job, false, "601", "0", false, &mut mutations)
            .unwrap();
        if remove_mdl_table {
            let (schema, table) = catalog.find_table("mysql", "tidb_mdl_info").unwrap();
            mutations.push(BufferMutation::delete(key::table_kv_key(schema.id, table.id)).unwrap());
        }
        if recover {
            // Another job published a newer diff; the newest reserved version
            // is still empty. Go recovers GetSchemaVersionWithNonEmptyDiff.
            mutations.push(
                BufferMutation::set(
                    key::database_kv_key(601),
                    value::serialize_db_info(&database).unwrap(),
                )
                .unwrap(),
            );
            mutations.push(
                BufferMutation::set(key::schema_version_kv_key(), (before + 3).to_string())
                    .unwrap(),
            );
            let diff = SchemaDiff {
                version: before + 2,
                action_type: ActionType::ACTION_CREATE_SCHEMA,
                schema_id: 601,
                ..Default::default()
            };
            mutations.push(
                BufferMutation::set(
                    key::schema_diff_kv_key(before + 2),
                    value::serialize_schema_diff(&diff).unwrap(),
                )
                .unwrap(),
            );
        }
        assert!(matches!(
            tx.commit(
                mutations,
                &tidb_txnkv::UnaryCallContext::with_timeout(timeout)
            )
            .unwrap(),
            OptimisticCommitOutcome::Committed(_)
        ));
        let barrier = Barrier(Mutex::new(Vec::new()), AtomicBool::new(true));
        struct FailedNotifier(Mutex<Vec<i64>>);
        impl SchemaVersionNotifier for FailedNotifier {
            fn notify(&self, version: i64) -> Result<(), String> {
                self.0.lock().unwrap().push(version);
                Err("notification unavailable".into())
            }
        }
        let notifier = FailedNotifier(Mutex::new(Vec::new()));
        let run = |job_id| {
            run_persisted_ddl_job(
                opener.clone(),
                job_id,
                timeout,
                None,
                Some(&notifier),
                &test_backfiller(),
                &test_backfiller(),
                &barrier,
                &|| Ok(()),
                &|_| panic!("no action error retry expected"),
            )
        };
        let outcome = run(job.id);
        assert_eq!(
            *barrier.0.lock().unwrap(),
            vec![(job.id, before + if recover { 2 } else { 1 })],
            "the worker must reach the schema-version barrier: {outcome:?}"
        );
        assert_eq!(
            *notifier.0.lock().unwrap(),
            vec![before + if recover { 2 } else { 1 }]
        );
        assert!(outcome.is_err());
        assert!(
            load_history_persisted_ddl_job(opener.clone(), job.id, timeout)
                .unwrap()
                .is_none()
        );
        if !remove_mdl_table {
            let mut tx = opener.begin_read_only().unwrap();
            let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
            assert!(
                matches!(
                    crate::ddl_systable::SystemTableManager::new(&catalog)
                        .get_mdl_version(&mut snapshot, job.id),
                    Err(crate::ddl_systable::SystemTableManagerError::NotFound)
                ),
                "disabled mode must never register an MDL row"
            );
            tx.finish_without_writes().unwrap();
        }
        barrier.1.store(false, Ordering::Relaxed);
        run(job.id).unwrap();
        assert!(load_active_persisted_ddl_jobs(opener.clone(), timeout, 0)
            .unwrap()
            .is_empty());
        assert_eq!(
            load_history_persisted_ddl_job(opener.clone(), job.id, timeout)
                .unwrap()
                .unwrap()
                .state,
            JobState::SYNCED
        );
        // Every committed DROP phase waits in non-MDL mode too, while
        // notification errors do not prevent the self-version acknowledgement.
        let start = tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout)
            .unwrap()
            .schema_version;
        job.id = 602;
        job.type_ = ActionType::ACTION_DROP_SCHEMA;
        job.state = JobState::QUEUEING;
        job.schema_state = SchemaState::PUBLIC;
        job.last_schema_version = 0;
        job.fill_args(Some(GoShared::new(tidb_model::DropSchemaArgs::default())));
        let mut tx = opener.begin().unwrap();
        let current = tidb_infoschema::cluster_catalog::load_cluster_catalog(
            &mut TransactionMetaSnapshot::new(&mut tx, timeout),
        )
        .unwrap();
        let mut mutations = Vec::new();
        DdlJobTable::locate(&current)
            .unwrap()
            .append_insert(&mut job, false, "601", "0", false, &mut mutations)
            .unwrap();
        tx.commit(
            mutations,
            &tidb_txnkv::UnaryCallContext::with_timeout(timeout),
        )
        .unwrap();
        let before_waits = barrier.0.lock().unwrap().len();
        run(job.id).unwrap();
        assert_eq!(
            &barrier.0.lock().unwrap()[before_waits..],
            &[(602, start + 1), (602, start + 2), (602, start + 3)]
        );
        assert!(
            load_history_persisted_ddl_job(opener.clone(), job.id, timeout)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn persisted_worker_without_mdl_skips_registration() {
        if crate::isolate_process_globals() {
            return;
        }
        worker_without_mdl(false, false);
    }

    #[test]
    fn persisted_worker_without_mdl_recovers_before_history() {
        if crate::isolate_process_globals() {
            return;
        }
        worker_without_mdl(true, false);
    }

    #[test]
    fn persisted_worker_without_mdl_needs_no_mdl_table() {
        if crate::isolate_process_globals() {
            return;
        }
        worker_without_mdl(false, true);
    }

    #[test]
    fn persisted_worker_recovers_schema_barriers_before_history() {
        if crate::isolate_process_globals() {
            return;
        }
        tidb_vardef::set_enable_mdl(true);
        use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
        use crate::real_tikv_ddl::{load_active_persisted_ddl_jobs, ClusterDdlError};
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
        let (_pd, opener) = crate::embedded_store::in_process_write_stack().unwrap();
        let opener = Arc::new(opener);
        let timeout = Duration::from_secs(5);
        let (outcome, _) = tidb_session::bootstrap_publish::publish_bootstrap(&opener, timeout).unwrap();
        assert!(matches!(outcome, OptimisticCommitOutcome::Committed(_)));
        let seed = |job: &mut Job| {
            let mut tx = opener.begin().unwrap();
            let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
            let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
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
            run_persisted_ddl_job(
                opener.clone(),
                id,
                timeout,
                None,
                notifier,
                &test_backfiller(),
                &test_backfiller(),
                &barrier,
                check,
                &|_| panic!("this job must not wait after an action error"),
            )
        };
        let before = tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout)
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
            tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout)
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
            tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout)
                .unwrap()
                .schema_version,
            before + 4
        );

        // Pausing releases the worker without finishing the SQL job. Owner
        // replacement must still observe PAUSED until an explicit resume.
        let mut paused_job = Job::default();
        paused_job.id = 504;
        paused_job.schema_id = 505;
        paused_job.schema_name = "pause_database".into();
        paused_job.type_ = ActionType::ACTION_CREATE_SCHEMA;
        paused_job.state = JobState::PAUSING;
        paused_job.version = JobVersion::V2;
        paused_job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
        paused_job.fill_args(Some(GoShared::new(CreateSchemaArgs {
            db_info: GoField::new(Some(GoShared::new(DBInfo {
                id: 505,
                name: tidb_ast::CiString::new("pause_database"),
                ..Default::default()
            }))),
        })));
        seed(&mut paused_job);
        let checks = std::sync::atomic::AtomicUsize::new(0);
        let lost_before_pause_commit = || {
            if checks.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err("owner lost".into())
            }
        };
        assert!(run(504, None, &lost_before_pause_commit).is_err());
        assert_eq!(
            load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap()[0].state,
            JobState::PAUSING
        );
        let clean_count = barrier.cleaned.lock().unwrap().len();
        assert_eq!(
            run(504, Some(&FailedNotifier), &|| Ok(())).unwrap(),
            PersistedDdlJobOutcome::Paused
        );
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].state, JobState::PAUSED);
        let pause_ts = active[0].real_start_ts;
        assert_ne!(pause_ts, 0);
        // A fresh worker has no in-memory continuation, so this also proves
        // the durable checkpoint drives recovery after owner replacement.
        assert_eq!(
            run(504, Some(&FailedNotifier), &|| Ok(())).unwrap(),
            PersistedDdlJobOutcome::Paused
        );
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active[0].state, JobState::PAUSED);
        assert_eq!(active[0].real_start_ts, pause_ts);
        assert_eq!(barrier.waits.lock().unwrap().len(), wait_count);
        assert_eq!(barrier.cleaned.lock().unwrap().len(), clean_count);
        assert!(load_history_persisted_ddl_job(opener.clone(), 504, timeout)
            .unwrap()
            .is_none());
        let catalog =
            tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout).unwrap();
        assert_eq!(catalog.schema_version, before + 4);
        assert!(!catalog
            .databases
            .iter()
            .any(|database| database.info.id == 505));

        // Another job can finish while the paused job remains in the queue.
        let mut other = paused_job.clone();
        other.id = 506;
        other.schema_id = 507;
        other.state = JobState::QUEUEING;
        other.schema_name = "other_database".into();
        other.fill_args(Some(GoShared::new(CreateSchemaArgs {
            db_info: GoField::new(Some(GoShared::new(DBInfo {
                id: 507,
                name: tidb_ast::CiString::new("other_database"),
                ..Default::default()
            }))),
        })));
        seed(&mut other);
        assert_eq!(
            run(506, None, &|| Ok(())).unwrap(),
            PersistedDdlJobOutcome::Finished
        );
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!((active[0].id, active[0].state), (504, JobState::PAUSED));

        // Resume to QUEUEING as Go does (this job has no pause reason or
        // prior error to clear). The next worker reads the original arguments
        // and follows the normal schema barrier.
        let mut tx = opener.begin().unwrap();
        let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
        let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
        let table = DdlJobTable::locate(&catalog).unwrap();
        let mut active = table.load_by_id(&mut snapshot, 504).unwrap().unwrap();
        active.job.state = JobState::QUEUEING;
        let mut mutations = Vec::new();
        table
            .append_update(&mut active, false, &mut mutations)
            .unwrap();
        assert!(matches!(
            tx.commit(
                mutations,
                &tidb_txnkv::UnaryCallContext::with_timeout(timeout)
            )
            .unwrap(),
            OptimisticCommitOutcome::Committed(_)
        ));
        assert_eq!(
            run(504, None, &|| Ok(())).unwrap(),
            PersistedDdlJobOutcome::Finished
        );
        let history = load_history_persisted_ddl_job(opener.clone(), 504, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::SYNCED);
        assert_eq!(history.real_start_ts, pause_ts);
        assert_eq!(history.error_count, 0);
        assert_eq!(barrier.waits.lock().unwrap().len(), wait_count + 2);
        assert_eq!(barrier.cleaned.lock().unwrap().len(), clean_count + 2);

        // A cancelling CREATE must never reach its valid forward arguments.
        let mut cancelled = paused_job.clone();
        cancelled.id = 600;
        cancelled.schema_id = 601;
        cancelled.schema_name = "never_created".into();
        cancelled.state = JobState::CANCELLING;
        cancelled.fill_args(Some(GoShared::new(CreateSchemaArgs {
            db_info: GoField::new(Some(GoShared::new(DBInfo {
                id: 601,
                name: tidb_ast::CiString::new("never_created"),
                ..Default::default()
            }))),
        })));
        seed(&mut cancelled);
        run(600, Some(&FailedNotifier), &|| Ok(())).unwrap();
        let history = load_history_persisted_ddl_job(opener.clone(), 600, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::CANCELLED);
        assert_eq!(history.error_count, 1);
        assert_eq!(
            history.error.as_ref().unwrap().read().code().value(),
            tidb_error::tidb::errcode::ErrCancelledDDLJob as isize
        );
        assert!(
            !tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout)
                .unwrap()
                .databases
                .iter()
                .any(|db| db.info.id == 601)
        );

        let set_limit = |limit: i64| {
            let mut tx = opener.begin().unwrap();
            let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
            let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
            let mut vars =
                tidb_syssession::cluster_sysvar_load::load_cluster_sysvars(&mut snapshot, &catalog)
                    .unwrap()
                    .into_iter()
                    .collect::<std::collections::BTreeMap<_, _>>();
            vars.insert("tidb_ddl_error_count_limit".into(), limit.to_string());
            let plan = tidb_syssession::cluster_sysvar_write::plan_sysvar_write(
                &mut snapshot,
                &catalog,
                &vars,
                tidb_datatype::Time::from_date_checked(
                    2026,
                    10,
                    1,
                    0,
                    0,
                    0,
                    0,
                    tidb_datatype::TimeType::Timestamp,
                    0,
                )
                .unwrap(),
                &mut |_, _, minimum, _| Ok(minimum + 1),
            )
            .unwrap();
            assert!(matches!(
                tx.commit(
                    plan.mutations,
                    &tidb_txnkv::UnaryCallContext::with_timeout(timeout)
                )
                .unwrap(),
                OptimisticCommitOutcome::Committed(_)
            ));
        };
        set_limit(1);
        let mut failed = Job::default();
        failed.id = 602;
        failed.schema_id = 603;
        failed.version = JobVersion::V2;
        failed.type_ = ActionType::ACTION_CREATE_SCHEMA;
        failed.state = JobState::QUEUEING;
        failed.fill_v2_arg(serde_json::from_str("{}").unwrap());
        // Committing an action error is a successful worker step. The same
        // worker must carry it through cancellation and durable history.
        set_limit(0);
        failed.id = 604;
        seed(&mut failed);
        assert_eq!(
            run(604, None, &|| Ok(())).unwrap(),
            PersistedDdlJobOutcome::Finished
        );
        let history = load_history_persisted_ddl_job(opener.clone(), 604, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::CANCELLED);
        assert_eq!(history.error_count, 2);
        assert!(history.error.is_some());
        failed.id = 602;
        set_limit(1);
        seed(&mut failed);
        let checks = std::sync::atomic::AtomicUsize::new(0);
        let lost_before_error_commit = || {
            if checks.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err("owner lost".into())
            }
        };
        assert!(run(602, None, &lost_before_error_commit).is_err());
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active[0].error_count, 0);
        assert_eq!(active[0].state, JobState::QUEUEING);
        let run_until_error_count = |count| {
            run(602, None, &|| {
                let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
                if active
                    .iter()
                    .any(|job| job.id == 602 && job.error_count >= count)
                {
                    Err("owner retired after error checkpoint".into())
                } else {
                    Ok(())
                }
            })
        };
        assert!(matches!(
            run_until_error_count(1),
            Err(ClusterDdlError::SchemaSync(_))
        ));
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active[0].error_count, 1);
        assert_eq!(active[0].state, JobState::RUNNING);
        // A peer changes the persisted limit; the next error refresh must
        // override even a different cached value, without restarting the node.
        set_limit(0);
        tidb_vardef::set_ddl_error_count_limit(100);
        assert!(matches!(
            run_until_error_count(2),
            Err(ClusterDdlError::SchemaSync(_))
        ));
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active[0].error_count, 2);
        assert_eq!(active[0].state, JobState::CANCELLING);
        assert_eq!(tidb_vardef::ddl_error_count_limit(), 0);
        run(602, None, &|| Ok(())).unwrap();
        let history = load_history_persisted_ddl_job(opener.clone(), 602, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::CANCELLED);
        assert_eq!(history.error_count, 3);
        assert!(history
            .error
            .as_ref()
            .unwrap()
            .read()
            .message()
            .starts_with("DDL job rollback, error msg:"));

        // A retry wait sees the committed error and current count. Retiring
        // the owner in that wait leaves a replacement the same durable job.
        for (id, interrupt) in [(605, true), (606, false)] {
            set_limit(3);
            failed.id = id;
            seed(&mut failed);
            let waits = std::cell::Cell::new(0);
            let result = run_persisted_ddl_job(
                opener.clone(),
                id,
                timeout,
                None,
                None,
                &test_backfiller(),
                &test_backfiller(),
                &barrier,
                &|| Ok(()),
                &|delay| {
                    assert_eq!(delay, Duration::from_secs(1));
                    waits.set(waits.get() + 1);
                    let active =
                        load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
                    let active = active.iter().find(|job| job.id == id).unwrap();
                    assert_eq!(active.error_count, 1, "wait must follow the first commit");
                    assert!(active.error.is_some());
                    assert!(load_history_persisted_ddl_job(opener.clone(), id, timeout)
                        .unwrap()
                        .is_none());
                    if interrupt {
                        Err("owner retired during retry wait".into())
                    } else {
                        // The next step reloads the new process limit; it
                        // must not keep a per-job copy of the old budget.
                        set_limit(2);
                        Ok(())
                    }
                },
            );
            assert_eq!(waits.get(), 1);
            if interrupt {
                assert!(matches!(result, Err(ClusterDdlError::SchemaSync(_))));
                set_limit(0);
                assert_eq!(
                    run(id, None, &|| Ok(())).unwrap(),
                    PersistedDdlJobOutcome::Finished
                );
            } else {
                assert_eq!(result.unwrap(), PersistedDdlJobOutcome::Finished);
            }
            let history = load_history_persisted_ddl_job(opener.clone(), id, timeout)
                .unwrap()
                .unwrap();
            assert_eq!(history.state, JobState::CANCELLED);
            assert_eq!(history.error_count, if interrupt { 3 } else { 4 });
            assert!(history.error.is_some());
        }

        // An ADMIN pause committed during validation must win over a stale
        // validation error. A detached checkpoint would overwrite PAUSING.
        struct PauseDuringValidation<F>(F);
        impl<F: Fn(&MutationBuffer)> CheckConstraintValidator for PauseDuringValidation<F> {
            fn validate(
                &self,
                _: &CheckConstraintValidation,
                _: Arc<Mutex<dyn ClusterSnapshot>>,
                buffer: &MutationBuffer,
            ) -> Result<(), DdlPlanError> {
                (self.0)(buffer);
                Err(tidb_dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED
                    .fast_generate_by_args(&["c".into()])
                    .into())
            }
        }
        let constraint = tidb_model::table::ConstraintInfo {
            name: tidb_ast::CiString::new("c"),
            state: SchemaState::WRITE_REORGANIZATION,
            enforced: true,
            ..Default::default()
        };
        let table = tidb_model::TableInfo {
            id: 701,
            name: tidb_ast::CiString::new("pause_during_validation"),
            state: SchemaState::PUBLIC,
            constraints: vec![constraint.clone()].into(),
            ..Default::default()
        };
        let tx = opener.begin().unwrap();
        assert!(matches!(
            tx.commit(
                vec![tidb_txnkv::transaction::BufferMutation::set(
                    tidb_meta::key::table_kv_key(505, 701),
                    tidb_meta::value::serialize_table_info(&table).unwrap(),
                )
                .unwrap()],
                &tidb_txnkv::UnaryCallContext::with_timeout(timeout)
            )
            .unwrap(),
            OptimisticCommitOutcome::Committed(_)
        ));
        let mut check_job = Job::default();
        check_job.id = 700;
        check_job.schema_id = 505;
        check_job.table_id = 701;
        check_job.type_ = ActionType::ACTION_ADD_CHECK_CONSTRAINT;
        check_job.state = JobState::RUNNING;
        check_job.schema_state = SchemaState::WRITE_REORGANIZATION;
        check_job.version = JobVersion::V2;
        check_job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
        let mut submitted = constraint;
        submitted.state = SchemaState::WRITE_ONLY;
        check_job.fill_args(Some(GoShared::new(tidb_model::AddCheckConstraintArgs {
            constraint: GoField::new(Some(GoShared::new(submitted))),
        })));
        seed(&mut check_job);
        let validator = PauseDuringValidation(|_: &MutationBuffer| {
            let mut tx = opener.begin().unwrap();
            let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
            let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
            let queue = DdlJobTable::locate(&catalog).unwrap();
            let mut active = queue.load_by_id(&mut snapshot, 700).unwrap().unwrap();
            active.job.state = JobState::PAUSING;
            let mut mutations = Vec::new();
            queue
                .append_update(&mut active, false, &mut mutations)
                .unwrap();
            assert!(matches!(
                tx.commit(
                    mutations,
                    &tidb_txnkv::UnaryCallContext::with_timeout(timeout)
                )
                .unwrap(),
                OptimisticCommitOutcome::Committed(_)
            ));
        });
        let result = run_persisted_ddl_job(
            opener.clone(),
            700,
            timeout,
            None,
            None,
            &test_backfiller(),
            &validator,
            &barrier,
            &|| Ok(()),
            &|_| panic!("this job must not wait after an action error"),
        );
        assert_eq!(result.unwrap(), PersistedDdlJobOutcome::Paused);
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active[0].state, JobState::PAUSED);
        assert_eq!(
            active[0].error_count, 0,
            "the losing error transaction must not count"
        );
        assert!(load_history_persisted_ddl_job(opener.clone(), 700, timeout)
            .unwrap()
            .is_none());
        let mut tx = opener.begin().unwrap();
        let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
        let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
        let queue = DdlJobTable::locate(&catalog).unwrap();
        let mut active = queue.load_by_id(&mut snapshot, 700).unwrap().unwrap();
        active.job.state = JobState::QUEUEING;
        let mut mutations = Vec::new();
        queue
            .append_update(&mut active, false, &mut mutations)
            .unwrap();
        assert!(matches!(
            tx.commit(
                mutations,
                &tidb_txnkv::UnaryCallContext::with_timeout(timeout)
            )
            .unwrap(),
            OptimisticCommitOutcome::Committed(_)
        ));
        // Without a racing control command, the very same error checkpoint
        // drives ordinary rollback and history, even with the retry limit 0.
        set_limit(0);
        let result = run_persisted_ddl_job(
            opener.clone(),
            700,
            timeout,
            None,
            None,
            &test_backfiller(),
            &PauseDuringValidation(|_: &MutationBuffer| {}),
            &barrier,
            &|| Ok(()),
            &|_| panic!("this job must not wait after an action error"),
        );
        assert_eq!(result.unwrap(), PersistedDdlJobOutcome::Finished);
        let history = load_history_persisted_ddl_job(opener.clone(), 700, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::ROLLBACK_DONE);
        assert_eq!(history.error_count, 1);
        assert_eq!(
            history.error.as_ref().unwrap().read().rfc_code(),
            "ddl:3819"
        );
        assert_eq!(
            history.error.as_ref().unwrap().read().code().value(),
            tidb_error::tidb::errcode::ErrCheckConstraintViolated as isize
        );
        let sql_error = persisted_job_error(&history.error.as_ref().unwrap().read());
        assert_eq!(sql_error.code, 3819);
        assert_eq!(sql_error.message, "Check constraint 'c' is violated.");

        // Owner replacement between rollback publication and acknowledgement
        // must produce exactly the same worker outcome and SQL history error.
        set_limit(5);
        let tx = opener.begin().unwrap();
        assert!(matches!(
            tx.commit(
                vec![tidb_txnkv::transaction::BufferMutation::set(
                    tidb_meta::key::table_kv_key(505, 701),
                    tidb_meta::value::serialize_table_info(&table).unwrap(),
                )
                .unwrap()],
                &tidb_txnkv::UnaryCallContext::with_timeout(timeout),
            )
            .unwrap(),
            OptimisticCommitOutcome::Committed(_)
        ));
        check_job.id = 704;
        seed(&mut check_job);
        barrier.fail.store(true, Ordering::Relaxed);
        let result = run_persisted_ddl_job(
            opener.clone(),
            704,
            timeout,
            None,
            None,
            &test_backfiller(),
            &PauseDuringValidation(|_: &MutationBuffer| {}),
            &barrier,
            &|| Ok(()),
            &|_| panic!("CHECK failure is not retryable"),
        );
        assert!(matches!(result, Err(ClusterDdlError::SchemaSync(_))));
        assert!(load_history_persisted_ddl_job(opener.clone(), 704, timeout)
            .unwrap()
            .is_none());
        barrier.fail.store(false, Ordering::Relaxed);
        assert_eq!(
            run(704, None, &|| Ok(())).unwrap(),
            PersistedDdlJobOutcome::Finished
        );
        let history = load_history_persisted_ddl_job(opener.clone(), 704, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::ROLLBACK_DONE);
        assert_eq!(history.error_count, 1);
        let recovered = persisted_job_error(&history.error.as_ref().unwrap().read());
        assert_eq!(
            history.error.as_ref().unwrap().read().rfc_code(),
            "ddl:3819"
        );
        assert_eq!(
            (recovered.code, recovered.state, recovered.message),
            (sql_error.code, sql_error.state, sql_error.message)
        );

        // Restore the CHECK intermediate state and force its validator to
        // panic. The worker must discard the incomplete action and persist
        // Go's cancellation outcome under the current limit.
        let panic_count = tidb_metrics::panic::PANIC_TOTAL
            .with_label_values(&["ddl-worker"])
            .get();
        set_limit(0);
        let tx = opener.begin().unwrap();
        assert!(matches!(
            tx.commit(
                vec![tidb_txnkv::transaction::BufferMutation::set(
                    tidb_meta::key::table_kv_key(505, 701),
                    tidb_meta::value::serialize_table_info(&table).unwrap(),
                )
                .unwrap()],
                &tidb_txnkv::UnaryCallContext::with_timeout(timeout),
            )
            .unwrap(),
            OptimisticCommitOutcome::Committed(_)
        ));
        check_job.id = 702;
        seed(&mut check_job);
        let before_panic_version =
            tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout)
                .unwrap()
                .schema_version;
        let marker = tidb_meta::key::table_kv_key(505, 999);
        let panicking = PauseDuringValidation(|buffer: &MutationBuffer| {
            buffer
                .set(marker.clone().into(), b"unfinished action".to_vec())
                .unwrap();
            panic!("DDL validation panic");
        });
        let checks = std::sync::atomic::AtomicUsize::new(0);
        let lost_owner = || {
            if checks.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(())
            } else {
                Err("owner lost after action panic".into())
            }
        };
        assert!(run_persisted_ddl_job(
            opener.clone(),
            702,
            timeout,
            None,
            None,
            &test_backfiller(),
            &panicking,
            &barrier,
            &lost_owner,
            &|_| panic!("lost owner must not wait"),
        )
        .is_err());
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active[0].error_count, 0);
        assert_eq!(active[0].state, JobState::RUNNING);
        let result = run_persisted_ddl_job(
            opener.clone(),
            702,
            timeout,
            None,
            None,
            &test_backfiller(),
            &panicking,
            &barrier,
            &|| Ok(()),
            &|_| panic!("this job must not wait after an action error"),
        );
        assert_eq!(result.unwrap(), PersistedDdlJobOutcome::Finished);
        let history = load_history_persisted_ddl_job(opener.clone(), 702, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::CANCELLED);
        assert_eq!(history.error_count, 1);
        let error = history.error.as_ref().unwrap().read();
        assert_eq!(error.class(), tidb_error::terror::TerrorClass::Ddl);
        assert_eq!(error.code(), tidb_error::terror::CODE_UNKNOWN);
        let mut tx = opener.begin_read_only().unwrap();
        assert!(tx
            .snapshot_get(
                &marker,
                &tidb_txnkv::UnaryCallContext::with_timeout(timeout)
            )
            .unwrap()
            .value
            .is_none());
        tx.finish_without_writes().unwrap();
        let after =
            tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout).unwrap();
        assert_eq!(after.schema_version, before_panic_version);
        let stored_table = after
            .databases
            .iter()
            .flat_map(|db| &db.tables)
            .find(|table| table.id == 701)
            .unwrap();
        assert_eq!(
            stored_table.constraints.get(0).unwrap().read().state,
            SchemaState::WRITE_REORGANIZATION
        );

        // The same panic must not overwrite an ADMIN pause committed while
        // validation was running. Retry reads the new control row instead.
        set_limit(5);
        check_job.id = 703;
        seed(&mut check_job);
        let set_job_state = |state| {
            let mut tx = opener.begin().unwrap();
            let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
            let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
            let queue = DdlJobTable::locate(&catalog).unwrap();
            let mut active = queue.load_by_id(&mut snapshot, 703).unwrap().unwrap();
            active.job.state = state;
            let mut mutations = Vec::new();
            queue
                .append_update(&mut active, false, &mut mutations)
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
        let pause_then_panic = PauseDuringValidation(|buffer: &MutationBuffer| {
            set_job_state(JobState::PAUSING);
            (panicking.0)(buffer);
        });
        assert_eq!(
            run_persisted_ddl_job(
                opener.clone(),
                703,
                timeout,
                None,
                None,
                &test_backfiller(),
                &pause_then_panic,
                &barrier,
                &|| Ok(()),
                &|_| panic!("this job must not wait after an action error"),
            )
            .unwrap(),
            PersistedDdlJobOutcome::Paused
        );
        let active = load_active_persisted_ddl_jobs(opener.clone(), timeout, 0).unwrap();
        assert_eq!(active[0].state, JobState::PAUSED);
        assert_eq!(active[0].error_count, 0);
        assert!(load_history_persisted_ddl_job(opener.clone(), 703, timeout)
            .unwrap()
            .is_none());
        set_job_state(JobState::QUEUEING);
        assert_eq!(
            run_persisted_ddl_job(
                opener.clone(),
                703,
                timeout,
                None,
                None,
                &test_backfiller(),
                &panicking,
                &barrier,
                &|| Ok(()),
                &|_| panic!("this job must not wait after an action error"),
            )
            .unwrap(),
            PersistedDdlJobOutcome::Finished
        );
        let history = load_history_persisted_ddl_job(opener.clone(), 703, timeout)
            .unwrap()
            .unwrap();
        assert_eq!(history.state, JobState::CANCELLED);
        assert_eq!(
            history.error_count, 2,
            "one panic, then normal cancellation"
        );
        assert_eq!(
            history.error.as_ref().unwrap().read().code().value(),
            tidb_error::tidb::errcode::ErrCancelledDDLJob as isize
        );
        let after =
            tidb_infoschema_issyncer::real_tikv_catalog::load_catalog_from_cluster(&opener, timeout).unwrap();
        let stored_table = after
            .databases
            .iter()
            .flat_map(|db| &db.tables)
            .find(|table| table.id == 701)
            .unwrap();
        assert!(
            stored_table.constraints.is_empty(),
            "cancellation must finish metadata rollback"
        );
        assert_eq!(
            tidb_metrics::panic::PANIC_TOTAL
                .with_label_values(&["ddl-worker"])
                .get(),
            panic_count + 4,
            "record every recovered action panic, including owner loss and a conflicting pause"
        );
    }

    /// Go `handleJobDone`: after the history commit the reorg handles are
    /// cleaned, and only then does the submitting session's done signal
    /// close. A delay injected between the history commit and the cleanup
    /// widens the window a session woken any earlier would read the job's
    /// leftover `mysql.tidb_ddl_reorg` row in
    /// (`add_index_runs_as_a_persisted_job_like_go`).
    #[test]
    fn job_done_signal_closes_after_reorg_handles_are_cleaned() {
        if crate::isolate_process_globals() {
            return;
        }
        tidb_vardef::set_enable_mdl(true);
        use crate::ddl_reorg_table::DdlReorgTable;
        use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
        use crate::real_tikv_ddl::{load_history_persisted_ddl_job, JobDoneCh};
        use tidb_model::{
            ActionType, CreateSchemaArgs, DBInfo, GoField, GoShared, HistoryInfo, Job, JobState,
            JobVersion,
        };
        use tidb_txnkv::transaction::OptimisticCommitOutcome;
        struct Synced;
        impl DdlSchemaSync for Synced {
            fn owner_id(&self) -> &str {
                "owner"
            }
            fn wait_version_synced(&self, _: i64, _: i64) -> Result<(), String> {
                Ok(())
            }
            fn clean_job_versions(&self, _: i64) -> Result<(), String> {
                Ok(())
            }
        }
        struct FailpointGuard;
        impl Drop for FailpointGuard {
            fn drop(&mut self) {
                fail::remove("beforeCleanupDDLReorgHandles");
            }
        }
        let (_pd, opener) = crate::embedded_store::in_process_write_stack().unwrap();
        let opener = Arc::new(opener);
        let timeout = Duration::from_secs(5);
        let (outcome, _) = tidb_session::bootstrap_publish::publish_bootstrap(&opener, timeout).unwrap();
        assert!(matches!(outcome, OptimisticCommitOutcome::Committed(_)));
        let mut job = Job::default();
        job.id = 700;
        job.schema_id = 701;
        job.schema_name = "done_signal".into();
        job.type_ = ActionType::ACTION_CREATE_SCHEMA;
        job.state = JobState::QUEUEING;
        job.version = JobVersion::V2;
        job.binlog_info = Some(GoShared::new(HistoryInfo::default()));
        job.fill_args(Some(GoShared::new(CreateSchemaArgs {
            db_info: GoField::new(Some(GoShared::new(DBInfo {
                id: 701,
                name: tidb_ast::CiString::new("done_signal"),
                ..Default::default()
            }))),
        })));
        // The job and a reorg handle of its own, as a backfill leaves one.
        {
            let mut tx = opener.begin().unwrap();
            let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
            let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
            let mut mutations = Vec::new();
            DdlJobTable::locate(&catalog)
                .unwrap()
                .append_insert(&mut job, false, "701", "0", false, &mut mutations)
                .unwrap();
            mutations.extend(
                DdlReorgTable::locate(&catalog)
                    .unwrap()
                    .init_mutations(
                        &[],
                        job.id,
                        &crate::cluster_ddl::add_index::ReorgHandle {
                            element_id: 1,
                            element_type: b"_idx_".to_vec(),
                            start_key: Vec::new(),
                            end_key: Vec::new(),
                            physical_id: 701,
                        },
                        1,
                    )
                    .unwrap(),
            );
            assert!(matches!(
                tx.commit(
                    mutations,
                    &tidb_txnkv::UnaryCallContext::with_timeout(timeout)
                )
                .unwrap(),
                OptimisticCommitOutcome::Committed(_)
            ));
        }
        let reorg_rows = move |opener: &Arc<
            tidb_txnkv::transaction::RealOptimisticTransactionOpener<
                tidb_unistore::mock::StorePdClient,
            >,
        >| {
            let mut tx = opener.begin().unwrap();
            let mut snapshot = TransactionMetaSnapshot::new(&mut tx, timeout);
            let catalog = tidb_infoschema::cluster_catalog::load_cluster_catalog(&mut snapshot).unwrap();
            DdlReorgTable::locate(&catalog)
                .unwrap()
                .load(&mut snapshot, 700)
                .unwrap()
                .len()
        };
        assert_eq!(reorg_rows(&opener), 1);

        fail::cfg("beforeCleanupDDLReorgHandles", "sleep(300)").unwrap();
        let _guard = FailpointGuard;
        let done = Arc::new(JobDoneCh::default());
        let waiter = std::thread::spawn({
            let done = Arc::clone(&done);
            let opener = Arc::clone(&opener);
            move || {
                assert!(done.wait_until(std::time::Instant::now() + Duration::from_secs(30)));
                (
                    load_history_persisted_ddl_job(Arc::clone(&opener), 700, timeout)
                        .unwrap()
                        .is_some(),
                    reorg_rows(&opener),
                )
            }
        });
        let outcome = run_persisted_ddl_job(
            opener.clone(),
            700,
            timeout,
            Some(&done),
            None,
            &test_backfiller(),
            &test_backfiller(),
            &Synced,
            &|| Ok(()),
            &|_| panic!("the job must not fail"),
        );
        assert!(matches!(outcome, Ok(PersistedDdlJobOutcome::Finished)));
        assert_eq!(waiter.join().unwrap(), (true, 0));
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

    /// Go `detectAndUpdateJobVersionOnce` reads `semver.NewVersion` with
    /// the pre-release cleared.
    #[test]
    fn release_versions_parse_as_go_semver_compares_them() {
        assert_eq!(parse_release_version("8.5.6"), Some((8, 5, 6)));
        assert_eq!(
            parse_release_version("8.4.0-this-is-a-placeholder"),
            Some((8, 4, 0))
        );
        assert_eq!(parse_release_version("9.0.0-beta.1+build"), Some((9, 0, 0)));
        assert_eq!(parse_release_version("8.5"), None);
        assert!(parse_release_version("8.5.5").unwrap() < GLOBAL_IDX_V1_FIRST_VER);
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

impl<S> ClusterSchemaSync<S>
where
    S: StoreClient,
{
    fn reload_catalog(&self) -> Result<(), String> {
        let _reload = self.catalog.lock_reload();
        let current = self.catalog.load();
        let at =
            reload_catalog_from_cluster(&self.opener, self.timeout, &current, &self.row_ids)
                .map_err(|error| error.to_string())?;
        match tidb_infoschema_issyncer::real_tikv_catalog::note_reload(
            &self.schema_validator,
            current.schema_version,
            at,
        ) {
            CatalogReloadPass::Unchanged => {}
            CatalogReloadPass::Diffs(catalog) | CatalogReloadPass::Full(catalog) => {
                self.catalog.store(catalog);
            }
        }
        Ok(())
    }
}

impl<S> SchemaLoader for ClusterSchemaSync<S>
where
    S: StoreClient,
{
    fn reload(&self) -> Result<(), String> {
        self.reload_catalog()
    }
}

impl<S> DdlSchemaSync for ClusterSchemaSync<S>
where
    S: StoreClient,
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

impl<S> ClusterDdl for RealClusterDdl<S>
where
    S: StoreClient,
{
    fn is_owner(&self) -> bool {
        self.owner.is_owner()
    }

    fn tiflash_store_count(&self) -> Result<u64, SqlQueryError> {
        self.opener
            .store()
            .tiflash_store_count()
            .map_err(SqlQueryError::unknown)
    }

    fn execute(
        &self,
        statement: &DdlStatement,
        job_context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        self.execute_targets(statement, job_context, &[])
    }
}

impl<S> RealClusterDdl<S>
where
    S: StoreClient,
{
    /// [`ClusterDdl::execute`] for one target of a statement naming
    /// `targets` (Go's `DropTableArgs.Identifiers`: every table the DROP
    /// names, which the owner's foreign-key check ignores).
    fn execute_targets(
        &self,
        statement: &DdlStatement,
        job_context: &dyn tidb_ddl_build::session_context::DdlSessionContext,
        targets: &[(String, String)],
    ) -> Result<ClusterDdlReport, SqlQueryError> {
        // Go `dropTableObject`: each target is its own job, a missing name is
        // collected and reported once the list ends.
        #[derive(Clone, Copy, PartialEq)]
        enum DropObject {
            Table,
            View,
            Sequence,
        }
        let drop_list = match statement {
            DdlStatement::DropTables { names, if_exists } => {
                Some((names, *if_exists, DropObject::Table))
            }
            DdlStatement::DropView { names, if_exists } if names.len() != 1 => {
                Some((names, *if_exists, DropObject::View))
            }
            DdlStatement::DropSequences { names, if_exists } => {
                Some((names, *if_exists, DropObject::Sequence))
            }
            _ => None,
        };
        if let Some((names, if_exists, object)) = drop_list {
            use tidb_ddl_build::table_lifecycle::{drop_objects, DropObjectsError};
            let mut last = ClusterDdlReport::AlreadySatisfied {
                detail: "no existing DROP targets".into(),
                warnings: Vec::new(),
            };
            let mut notes = Vec::new();
            // Go `dropTableObject`'s `dropExistErr`.
            let missing_error: &tidb_error::terror::TerrorError = if object == DropObject::Sequence
            {
                &tidb_error::catalog::infoschema::ERR_SEQUENCE_DROP_EXISTS
            } else {
                &tidb_error::catalog::infoschema::ERR_TABLE_DROP_EXISTS
            };
            let missing_raised =
                |names: &str| missing_error.generate_with_stack_by_args(&[names.into()]);
            let missing = drop_objects(names, if_exists, |schema, name| {
                let target = match object {
                    DropObject::View => DdlStatement::DropView {
                        names: vec![(schema.to_owned(), name.to_owned())],
                        if_exists: false,
                    },
                    DropObject::Table => DdlStatement::DropTable {
                        schema: schema.to_owned(),
                        table: name.to_owned(),
                        if_exists: false,
                    },
                    DropObject::Sequence => DdlStatement::DropSequence {
                        schema: schema.to_owned(),
                        name: name.to_owned(),
                    },
                };
                let statement_targets: &[(String, String)] = if object == DropObject::Table {
                    names
                } else {
                    &[]
                };
                match self.execute_targets(&target, job_context, statement_targets) {
                    Ok(report) => {
                        last = report;
                        Ok(true)
                    }
                    Err(error) if error.code == missing_error.to_sql_error().code => Ok(false),
                    // Go: a target that is not a sequence is a note under
                    // IF EXISTS, and the list goes on.
                    Err(error)
                        if object == DropObject::Sequence
                            && if_exists
                            && error.code == tidb_error::mysql::errcode::ErrWrongObject =>
                    {
                        notes.push(tidb_context::SqlWarn::from_error(
                            crate::real_tikv_ddl::DdlWarningLevel::Note,
                            error.code,
                            error.message,
                            error.identity,
                        ));
                        Ok(true)
                    }
                    Err(error) => Err(error),
                }
            })
            .map_err(|error| match error {
                DropObjectsError::Target(error) => error,
                DropObjectsError::Missing(names) => SqlQueryError::from(
                    tidb_error::mysql::MysqlError::from_terror(&missing_raised(&names.join(","))),
                ),
            })?;
            let (ClusterDdlReport::Applied { warnings, .. }
            | ClusterDdlReport::AlreadySatisfied { warnings, .. }) = &mut last;
            warnings.extend(notes);
            // The same error, appended as a note for each name IF EXISTS skipped.
            warnings.extend(
                missing
                    .into_iter()
                    .map(|name| crate::real_tikv_ddl::ddl_note(missing_raised(&name))),
            );
            return Ok(last);
        }
        if matches!(
            statement,
            DdlStatement::AddCheckConstraint { .. }
                | DdlStatement::DropCheckConstraint { .. }
                | DdlStatement::AlterCheckConstraint { .. }
        ) {
            let (ddl_job_id, action) = submit_check_constraint_job_with_retry(
                Arc::clone(&self.opener),
                statement,
                self.timeout,
                self.server_state.is_upgrading_state(),
                self.min_job_id_refresher.current_min_job_id(),
                Some(&self.scheduler.ddl_job_done_ch_map),
            )
            .map_err(cluster_ddl_error)?;
            self.notify_new_job_submitted();
            let report = self.wait_persisted_job(ddl_job_id, action)?;
            self.refresh_catalog();
            return Ok(report);
        }
        let single_view =
            matches!(statement, DdlStatement::DropView { names, .. } if names.len() == 1);
        if single_view
            || matches!(
                statement,
                DdlStatement::CreateIndex { .. }
                    | DdlStatement::DropIndex { .. }
                    | DdlStatement::DropPrimaryKey { .. }
                    | DdlStatement::DropTable { .. }
                    | DdlStatement::DropSequence { .. }
                    | DdlStatement::DropDatabase { .. }
                    | DdlStatement::TruncateTable { .. }
                    | DdlStatement::CreateTable { .. }
                    | DdlStatement::AddColumn { .. }
                    | DdlStatement::DropColumn { .. }
                    | DdlStatement::CreateDatabase { .. }
                    | DdlStatement::RenameTable { .. }
                    | DdlStatement::RenameTables { .. }
                    | DdlStatement::ModifyColumn { .. }
                    | DdlStatement::RenameColumn { .. }
                    | DdlStatement::MultiSchemaChange { .. }
                    | DdlStatement::AddForeignKey { .. }
                    | DdlStatement::CreateView { .. }
                    | DdlStatement::CreateSequence { .. }
                    | DdlStatement::CreateTableLike { .. }
                    | DdlStatement::AlterSequence { .. }
                    | DdlStatement::ModifySchemaCharsetAndCollate { .. }
                    | DdlStatement::ModifySchemaDefaultPlacement { .. }
                    | DdlStatement::CreatePlacementPolicy { .. }
                    | DdlStatement::AlterPlacementPolicy { .. }
                    | DdlStatement::DropPlacementPolicy { .. }
                    | DdlStatement::CreateResourceGroup { .. }
                    | DdlStatement::AlterResourceGroup { .. }
                    | DdlStatement::DropResourceGroup { .. }
                    | DdlStatement::LockTables { .. }
                    | DdlStatement::UnlockTables { .. }
                    | DdlStatement::AddPartitions { .. }
                    | DdlStatement::DropPartitions { .. }
                    | DdlStatement::TruncatePartitions { .. }
                    | DdlStatement::ExchangePartition { .. }
                    | DdlStatement::ReorganizePartitions { .. }
            )
            || crate::cluster_ddl::alter_table::is_alter_table_statement(statement)
        {
            // Go `createIndex` / `dropIndex` / `dropTableObject` /
            // `DropSchema`: a persisted job the owner steps through the
            // object's schema states.
            match crate::real_tikv_ddl::submit_persisted_job_with_retry(
                Arc::clone(&self.opener),
                statement,
                job_context,
                targets,
                self.timeout,
                self.server_state.is_upgrading_state(),
                self.min_job_id_refresher.current_min_job_id(),
                &KvTableIndexBackfiller::new(&self.scheduler.row_ids, &self.scheduler.session_pool),
                self.auto_ids.as_ref(),
                Some(&self.scheduler.ddl_job_done_ch_map),
            )
            .map_err(cluster_ddl_error)?
            {
                crate::real_tikv_ddl::AddIndexSubmitted::AlreadySatisfied(report) => {
                    return Ok(report)
                }
                crate::real_tikv_ddl::AddIndexSubmitted::Job(
                    ddl_job_id,
                    action,
                    executor_warnings,
                ) => {
                    self.notify_new_job_submitted();
                    let mut report = self.wait_persisted_job(ddl_job_id, action)?;
                    self.refresh_catalog();
                    // The executor's warnings precede the job's.
                    let (ClusterDdlReport::Applied { warnings, .. }
                    | ClusterDdlReport::AlreadySatisfied { warnings, .. }) = &mut report;
                    let job_warnings = std::mem::take(warnings);
                    *warnings = executor_warnings;
                    warnings.extend(job_warnings);
                    return Ok(report);
                }
            }
        }
        // A statement Go's executor finishes without a job.
        answer_ddl_without_job(Arc::clone(&self.opener), statement, self.timeout)
            .map_err(cluster_ddl_error)
    }
}

/// Go `DoDDLJobWrapper` on a synced job: the reorganization's warnings,
/// then a multi-schema change's sub-job warnings as notes
/// (`appendMultiChangeWarningsToOwnerCtx`). A job's own `Warning` never
/// reaches the client.
fn synced_job_warnings(job: &tidb_model::Job) -> Vec<tidb_context::SqlWarn> {
    let mut warnings = reorg_meta_warnings(job);
    if job.type_ == tidb_model::ActionType::ACTION_MULTI_SCHEMA_CHANGE {
        if let Some(info) = &job.multi_schema_info {
            for sub in info.read().sub_jobs.iter_deref() {
                if let Some(warning) = &sub.read().warning {
                    warnings.push(tidb_context::SqlWarn::new(
                        crate::real_tikv_ddl::DdlWarningLevel::Note,
                        tidb_context::WarnErr::Terror(warning.read().clone()),
                    ));
                }
            }
        }
    }
    warnings
}

/// Go `DoDDLJobWrapper` on a synced job: the reorganization's warnings,
/// each once or summarized by code.
fn reorg_meta_warnings(job: &tidb_model::Job) -> Vec<tidb_context::SqlWarn> {
    let Some(meta) = job.reorg_meta.as_ref() else {
        return Vec::new();
    };
    let meta = meta.read();
    let (Some(warnings), Some(counts)) = (meta.warnings.as_ref(), meta.warnings_count.as_ref())
    else {
        return Vec::new();
    };
    let (warnings, counts) = (warnings.read(), counts.read());
    if warnings.len() != counts.len() {
        return Vec::new();
    }
    warnings
        .iter()
        .filter_map(|(key, warning)| {
            let warning = warning.as_ref()?.read();
            let count = counts.get(key).copied().unwrap_or_default();
            let error = if count == 1 {
                warning.clone()
            } else {
                // Go `dbterror.ClassTypes.Synthesize(warning.Code(), newMsg)`.
                tidb_error::terror::TerrorError::synthesize(
                    tidb_error::terror::TerrorClass::Types,
                    warning.code(),
                    format!(
                        "{count} warnings with this error code, first warning: {}",
                        warning.message()
                    ),
                )
            };
            Some(tidb_context::SqlWarn::new(
                crate::real_tikv_ddl::DdlWarningLevel::Warning,
                tidb_context::WarnErr::Terror(error),
            ))
        })
        .collect()
}

impl<S> crate::tiflash_replica_manager::TiFlashReplicaControl for RealClusterDdl<S>
where
    S: StoreClient,
{
    fn is_owner(&self) -> bool {
        self.owner.is_owner()
    }

    fn update_replica_status(&self, table_id: i64, available: bool) -> Result<(), (u16, String)> {
        self.execute(
            &DdlStatement::UpdateTiFlashReplicaStatus {
                table_id,
                available,
            },
            // The domain's own job: a system session, no statement text.
            &*self.scheduler.session_pool.system_session(),
        )
        .map(|_| ())
        .map_err(|error| (error.code, error.message))
    }
}

impl<S> Drop for RealClusterDdl<S>
where
    S: StoreClient,
{
    fn drop(&mut self) {
        self.server_state_context.cancel();
        let _ = self.min_job_id_stop.send(());
        if let Some(worker) = self.min_job_id_worker.take() {
            let _ = worker.join();
        }
        if let Some((stop, worker)) = self.job_version_detector.take() {
            let _ = stop.send(());
            let _ = worker.join();
        }
        self.owner.close();
        self.scheduler.stop();
    }
}

/// The index backfill, performed by the very code an `INSERT` uses.
///
/// `KvTable::create_index_with_context` expresses Go's reorg step: it walks
/// the table's rows, computes each entry from the row, and refuses a UNIQUE
/// index whose existing rows already collide -- leaving the table without the
/// index, which is what TiDB answers too. Nothing about it is
/// cluster-specific; it is the same call the in-process tier makes, over the
/// same `TableStorage` seam, which here is bound to the DDL transaction's
/// snapshot and staging buffer. That reuse is the point: an index whose
/// entries were written by a second implementation would disagree with the one
/// the write path maintains from the next `INSERT` onwards.
/// Auto-id counters read and bumped inside the DDL transaction itself: what a
/// row move that must allocate a fresh `_tidb_rowid` allocates from.
#[derive(Debug)]
struct DdlTransactionAutoIds {
    storage: ClusterTableStorage,
}

use tidb_table::kv_table::TableAutoId;

impl tidb_table::table_from_meta::TableAutoIds for DdlTransactionAutoIds {
    fn allocator_for(&self, db_id: i64, table: &tidb_model::TableInfo) -> TableAutoId {
        self.over(tidb_autoid::cluster_auto_id::auto_id_key_for(db_id, table))
    }

    fn row_allocator_for(&self, db_id: i64, table: &tidb_model::TableInfo) -> TableAutoId {
        let db_id = if table.auto_id_schema_id == 0 {
            db_id
        } else {
            table.auto_id_schema_id
        };
        self.over(tidb_meta::key::auto_table_id_kv_key(db_id, table.id))
    }

    fn random_allocator_for(&self, db_id: i64, table: &tidb_model::TableInfo) -> TableAutoId {
        self.over(tidb_autoid::cluster_auto_id::auto_random_id_key_for(
            db_id, table,
        ))
    }
}

impl DdlTransactionAutoIds {
    fn over(&self, key: Vec<u8>) -> TableAutoId {
        TableAutoId::over(
            Arc::new(DdlTransactionAutoIdStore {
                storage: Mutex::new(self.storage.clone()),
                key: tidb_txnkv::Key::from_bytes(key),
            }),
            tidb_autoid::DEFAULT_AUTO_ID_STEP,
        )
    }
}

/// One counter key, as Go's `alloc4Signed` reads and bumps it, through the DDL
/// transaction's buffer.
#[derive(Debug)]
struct DdlTransactionAutoIdStore {
    storage: Mutex<ClusterTableStorage>,
    key: tidb_txnkv::Key,
}

impl DdlTransactionAutoIdStore {
    fn last(&self) -> Result<u64, tidb_autoid::AutoIdStoreError> {
        let mut storage = self.storage.lock().expect("auto id storage poisoned");
        match tidb_kv::storage::TableStorage::get(&mut *storage, &self.key) {
            Ok(value) => tidb_meta::value::parse_int_value(&value)
                .map(|value| value as u64)
                .map_err(|error| tidb_autoid::AutoIdStoreError(error.to_string())),
            Err(tidb_kv::storage::StorageError::NotFound) => Ok(0),
            Err(error) => Err(tidb_autoid::AutoIdStoreError(error.to_string())),
        }
    }

    fn store(&self, end: u64) -> Result<(), tidb_autoid::AutoIdStoreError> {
        let mut storage = self.storage.lock().expect("auto id storage poisoned");
        tidb_kv::storage::TableStorage::set(
            &mut *storage,
            self.key.clone(),
            tidb_meta::value::encode_int_value(end as i64),
        )
        .map_err(|error| tidb_autoid::AutoIdStoreError(error.to_string()))
    }
}

impl tidb_autoid::AutoIdStore for DdlTransactionAutoIdStore {
    fn reserve(
        &self,
        step: u64,
        unsigned: bool,
    ) -> Result<(u64, u64), tidb_autoid::AutoIdStoreError> {
        let base = self.last()?;
        let end = tidb_autoid::advance(base, step, unsigned);
        self.store(end)?;
        Ok((base, end))
    }

    fn next_global(&self) -> Result<u64, tidb_autoid::AutoIdStoreError> {
        Ok(self.last()?.wrapping_add(1))
    }

    fn rebase(
        &self,
        required: u64,
        unsigned: bool,
    ) -> Result<(), tidb_autoid::AutoIdStoreError> {
        if tidb_autoid::exceeds(required, self.last()?, unsigned) {
            self.store(required)?;
        }
        Ok(())
    }

    fn force_rebase(
        &self,
        required: u64,
        _unsigned: bool,
    ) -> Result<(), tidb_autoid::AutoIdStoreError> {
        self.store(required)
    }

    fn reset(&self) -> Result<(), tidb_autoid::AutoIdStoreError> {
        self.store(0)
    }
}

struct KvTableIndexBackfiller {
    /// The node's row-ID allocators, for the reorg handle's row.
    row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
    /// The data checks Go runs as restricted SQL through its session pool.
    session_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
}

impl KvTableIndexBackfiller {
    fn new(
        row_ids: &Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
        session_pool: &Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    ) -> Self {
        Self {
            row_ids: Arc::clone(row_ids),
            session_pool: Arc::clone(session_pool),
        }
    }
}

impl IndexBackfiller for KvTableIndexBackfiller {
    fn system_row_ids(&self) -> Arc<dyn tidb_table::table_from_meta::TableAutoIds> {
        Arc::clone(&self.row_ids)
    }

    /// Go's txn backfill workers over the table as the job holds it, with the
    /// indexes being added written as the table's own: the backfill writes
    /// their real keys (Go `FromBackfill`), and the merge replays the
    /// temporary entries onto them.
    fn has_null_value(
        &self,
        table: &tidb_model::TableInfo,
        column_id: i64,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<bool, LockSqlError> {
        let storage = ClusterTableStorage::new(buffer.clone(), snapshot);
        let mut kv_table = cluster_table(table, &storage, &AutoIdSource::Unavailable)
            .map_err(exchange_validation_internal)?;
        let Some(offset) = kv_table
            .columns()
            .iter()
            .position(|column| column.id == column_id)
        else {
            return Ok(false);
        };
        // A NULL is what a NOT NULL copy of the column refuses.
        let not_null = kv_table.columns()[offset]
            .field_type
            .clone()
            .with_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        let found = self
            .session_pool
            .modify_column_data(
                &mut kv_table,
                offset,
                &not_null,
                false,
                &tidb_table::context::StaticMutateContext::new(),
            )
            .map_err(backfill_failure)?;
        Ok(matches!(
            found,
            Some(tidb_table::kv_table::KvTableError::Mysql(error))
                if error.equal(&tidb_dbterror::ERR_INVALID_USE_OF_NULL)
        ))
    }

    fn reorg_add_index(
        &self,
        reorg: &crate::cluster_ddl::add_index::AddIndexReorg,
        work: crate::real_tikv_ddl::AddIndexReorgWork<'_>,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<crate::real_tikv_ddl::AddIndexReorgAnswer, LockSqlError> {
        use crate::real_tikv_ddl::{AddIndexReorgAnswer, AddIndexReorgWork};
        let storage = ClusterTableStorage::new(buffer.clone(), snapshot);
        let context = &reorg_table_mutate_context(&reorg.session_vars);
        // Go `reorgPartitionWorker.BackfillData`: the table as the job sees
        // it, with the counters a colliding heap row's fresh handle comes
        // from, read and bumped in this batch's transaction.
        if let AddIndexReorgWork::PartitionCopy {
            physical_id,
            start,
            end,
        } = &work
        {
            let ids = DdlTransactionAutoIds {
                storage: storage.clone(),
            };
            let mut table = cluster_table(
                &reorg.table,
                &storage,
                &AutoIdSource::In {
                    db_id: reorg.schema_id,
                    ids: &ids,
                },
            )
            .map_err(exchange_validation_internal)?
            .with_new_collation_mode(tidb_datatype::new_collation_enabled());
            return table
                .reorg_partition_batch(*physical_id, start, end, reorg.batch_size, context)
                .map(AddIndexReorgAnswer::Batch)
                .map_err(backfill_failure);
        }
        // Go `checkModifyColumnData` reads the table as it stands.
        if let AddIndexReorgWork::CheckColumn(check) = &work {
            let mut table = cluster_table(&check.table, &storage, &AutoIdSource::Unavailable)
                .map_err(exchange_validation_internal)?
                .with_new_collation_mode(tidb_datatype::new_collation_enabled());
            let offset = table
                .columns()
                .iter()
                .position(|column| column.id == check.old_column_id)
                .ok_or_else(|| {
                    exchange_validation_internal("the modified column is absent".to_owned())
                })?;
            let found = self
                .session_pool
                .modify_column_data(
                    &mut table,
                    offset,
                    &check.new_column.field_type,
                    check.check_value_range,
                    context,
                )
                .map_err(backfill_failure)?;
            return Ok(AddIndexReorgAnswer::Checked(found.map(|error| {
                let error = tidb_ddl_build::column_changes::modify_column_error(error);
                LockSqlError::from(error)
            })));
        }
        let info = reorg.table.clone_like_go();
        for index in info.indices.iter_deref() {
            let mut index = index.write();
            if reorg.index_ids.contains(&index.id) {
                index.state = tidb_model::SchemaState::PUBLIC;
                index.backfill_state = tidb_model::BackfillState::INAPPLICABLE;
            }
        }
        // A MODIFY COLUMN's changing indexes are backfilled from its changing
        // column, which the row rewrite filled: the backfill reads it as an
        // ordinary column.
        if reorg.column.is_none() {
            for column in info.columns.iter_deref() {
                let mut column = column.write();
                if column.change_state_info.is_some()
                    && column.state != tidb_model::SchemaState::PUBLIC
                {
                    column.state = tidb_model::SchemaState::PUBLIC;
                }
            }
        }
        let mut table = cluster_table(&info, &storage, &AutoIdSource::Unavailable)
            .map_err(exchange_validation_internal)?
            .with_new_collation_mode(tidb_datatype::new_collation_enabled());
        match work {
            AddIndexReorgWork::CheckColumn(_) | AddIndexReorgWork::PartitionCopy { .. } => {
                unreachable!("answered above")
            }
            // Go `updateColumnWorker.BackfillData`.
            AddIndexReorgWork::UpdateColumn { start, end, .. } => {
                let spec = reorg.column.as_ref().ok_or_else(|| {
                    exchange_validation_internal("a column rewrite names no column".to_owned())
                })?;
                let old_offset = table
                    .columns()
                    .iter()
                    .position(|column| column.id == spec.old_column_id)
                    .ok_or_else(|| {
                        exchange_validation_internal("the modified column is absent".to_owned())
                    })?;
                let changing = table
                    .nonpublic_columns()
                    .iter()
                    .position(|column| column.column.id == spec.changing_column_id)
                    .ok_or_else(|| {
                        exchange_validation_internal("the changing column is absent".to_owned())
                    })?;
                table
                    .update_column_batch(
                        old_offset,
                        changing,
                        start,
                        end,
                        reorg.batch_size,
                        &RowDecodeContext::for_ddl(context),
                        context,
                    )
                    .map(AddIndexReorgAnswer::Batch)
                    .map_err(|error| {
                        let error = tidb_ddl_build::column_changes::modify_column_error(error);
                        LockSqlError::from(error)
                    })
            }
            AddIndexReorgWork::TableRange { physical_id } => table
                .physical_record_range(physical_id)
                .map(AddIndexReorgAnswer::Range)
                .map_err(backfill_failure),
            AddIndexReorgWork::Backfill {
                physical_id,
                start,
                end,
            } => {
                // A partial index's entries are the rows its condition holds
                // for (Go `buildIndexConditionChecker` in the backfill).
                for index in info.indices.iter_deref() {
                    let index = index.read();
                    if !reorg.index_ids.contains(&index.id) || !index.has_condition() {
                        continue;
                    }
                    let condition = index
                        .condition_expr()
                        .map_err(|error| exchange_validation_internal(error.message))?;
                    table
                        .add_partial_index_condition(
                            index.id,
                            index.name.original(),
                            &condition,
                            &tidb_table::context::MutateContext::session_zone(context),
                            tidb_expr::Columns::like_default_escape(context),
                        )
                        .map_err(backfill_failure)?;
                }
                let decode = RowDecodeContext::for_write(context);
                let mut combined: Option<tidb_ddl_build::backfilling::ReorgBatch> = None;
                for index_id in &reorg.index_ids {
                    let batch = table
                        .backfill_index_batch(
                            *index_id,
                            physical_id,
                            start,
                            end,
                            reorg.batch_size,
                            &decode,
                        )
                        .map_err(backfill_failure)?;
                    combined = Some(match combined {
                        None => batch,
                        Some(mut combined) => {
                            // Every index walks the same rows. A partition
                            // reorganization counts each index's entries, as
                            // Go builds its indexes one element at a time.
                            combined.added = if reorg.partition_phase.is_some() {
                                combined.added + batch.added
                            } else {
                                combined.added.max(batch.added)
                            };
                            combined.locks.extend(batch.locks);
                            combined
                        }
                    });
                }
                let mut batch = combined.unwrap_or_default();
                batch.locks.sort();
                batch.locks.dedup();
                Ok(AddIndexReorgAnswer::Batch(batch))
            }
            AddIndexReorgWork::CleanUpGlobalIndex {
                physical_id,
                start,
                end,
            } => table
                .clean_up_global_index_batch(
                    &reorg.index_ids,
                    physical_id,
                    start,
                    end,
                    reorg.batch_size,
                    &RowDecodeContext::for_write(context),
                )
                .map(AddIndexReorgAnswer::Batch)
                .map_err(backfill_failure),
            AddIndexReorgWork::Merge {
                index_id,
                physical_id,
                start,
                end,
            } => table
                .merge_temp_index_batch(index_id, physical_id, start, end, reorg.batch_size)
                .map(AddIndexReorgAnswer::Batch)
                .map_err(backfill_failure),
        }
    }

    fn stage_exchange_partition_check(
        &self,
        plan: &ExchangePartitionValidation,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError> {
        self.validate_exchange_partition(plan, snapshot, buffer)
    }

    /// Go `checkForeignKeyConstrain`: the child's existing rows against its
    /// parent, both read through the DDL transaction's snapshot and buffer,
    /// by the same check the in-process `ADD FOREIGN KEY` runs.
    fn stage_foreign_key_check(
        &self,
        plan: &tidb_ddl_build::session_pool::ForeignKeyCheck,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError> {
        let storage = ClusterTableStorage::new(buffer.clone(), snapshot);
        self.session_pool
            .foreign_key(plan, &storage)
            .map_err(|error| LockSqlError::from(error))
    }
    fn stage_cache_table_check(
        &self,
        table: &tidb_model::TableInfo,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError> {
        let storage = ClusterTableStorage::new(buffer.clone(), snapshot);
        let mut probe = cluster_table(table, &storage, &AutoIdSource::Unavailable)
            .map_err(exchange_validation_internal)?;
        probe.enable_cache().map_err(|error| {
            let error = match error {
                tidb_table::kv_table::KvTableError::Mysql(error) => error,
                other => tidb_error::mysql::MysqlError::unknown(format!(
                    "cache table size check failed: {other:?}"
                )),
            };
            LockSqlError::from(error)
        })
    }
}

impl KvTableIndexBackfiller {
    /// Go `checkExchangePartitionRecordValidation`: every standalone-table
    /// row must route to the exchanged partition.
    fn validate_exchange_partition(
        &self,
        plan: &ExchangePartitionValidation,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), LockSqlError> {
        // Go `checkExchangePartitionRecordValidation`'s partition-type switch
        // refuses a KEY-partitioned table before any row is read.
        if plan
            .partitioned
            .partition
            .as_ref()
            .is_some_and(|partition| {
                partition.read().partition_type == tidb_ast::PartitionType::KEY
            })
        {
            return Err(LockSqlError::from(
                tidb_error::mysql::MysqlError::from_terror(
                    &tidb_dbterror::ERR_UNSUPPORTED_PARTITION_TYPE
                        .generate_with_stack_by_args(&[plan.partitioned.name.original().into()]),
                ),
            ));
        }
        let storage = ClusterTableStorage::new(buffer.clone(), snapshot);
        let mut standalone = cluster_table(&plan.standalone, &storage, &AutoIdSource::Unavailable)
            .map_err(exchange_validation_internal)?;
        let mut partitioned =
            cluster_table(&plan.partitioned, &storage, &AutoIdSource::Unavailable)
                .map_err(exchange_validation_internal)?;
        let ordinal = plan
            .partitioned
            .partition
            .as_ref()
            .and_then(|partition| {
                partition
                    .read()
                    .definitions
                    .snapshot()
                    .iter()
                    .position(|definition| definition.id == plan.partition_id)
            })
            .ok_or_else(|| {
                exchange_validation_internal(format!(
                    "partition {} is not in the partitioned table",
                    plan.partition_id
                ))
            })?;
        self.session_pool
            .exchange_partition_rows(
                &mut standalone,
                &mut partitioned,
                ordinal,
                plan.partition_id,
                plan.check_constraints,
            )
            .map_err(exchange_validation_table_error)
    }
}

impl CheckConstraintValidator for KvTableIndexBackfiller {
    fn validate(
        &self,
        plan: &CheckConstraintValidation,
        snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
        buffer: &MutationBuffer,
    ) -> Result<(), DdlPlanError> {
        let storage = ClusterTableStorage::new(buffer.clone(), snapshot);
        let mut table = cluster_table(&plan.table, &storage, &AutoIdSource::Unavailable)
            .map_err(DdlPlanError::Encode)?;
        self.session_pool
            .check_constraint_rows(&mut table)
            .map_err(|error| check_constraint_validation_table_error(error, &plan.constraint_name))
    }
}

fn check_constraint_validation_table_error(
    error: tidb_table::kv_table::KvTableError,
    constraint_name: &str,
) -> DdlPlanError {
    match error {
        tidb_table::kv_table::KvTableError::Mysql(error)
            if error.equal(&tidb_error::catalog::table::ERR_CHECK_CONSTRAINT_VIOLATED) =>
        {
            use tidb_hack::GoToLower;
            tidb_dbterror::ERR_CHECK_CONSTRAINT_IS_VIOLATED
                .generate_with_stack_by_args(&[tidb_error::mysql::FormatArg::from(
                    constraint_name.go_to_lower(),
                )])
                .into()
        }
        other => DdlPlanError::Encode(format!(
            "validation of check constraint '{constraint_name}' failed: {other:?}"
        )),
    }
}

fn exchange_validation_internal(message: String) -> LockSqlError {
    LockSqlError::from(tidb_error::mysql::MysqlError::unknown(message))
}

fn exchange_validation_table_error(error: tidb_table::kv_table::KvTableError) -> LockSqlError {
    match error {
        tidb_table::kv_table::KvTableError::Mysql(error)
            if error
                .equal(&tidb_error::catalog::table::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET)
                || error.equal(&tidb_error::catalog::table::ERR_NO_PARTITION_FOR_GIVEN_VALUE)
                || error.equal(&tidb_error::catalog::table::ERR_CHECK_CONSTRAINT_VIOLATED) =>
        {
            LockSqlError::from(tidb_error::mysql::MysqlError::from_terror(
                &tidb_dbterror::ERR_ROW_DOES_NOT_MATCH_PARTITION
                    .generate_with_stack_by_args(&[]),
            ))
        }
        tidb_table::kv_table::KvTableError::CheckConstraint {
            eval: Some(eval), ..
        } => {
            let error = eval.to_mysql_error();
            LockSqlError::from(error)
        }
        other => exchange_validation_internal(format!("{other:?}")),
    }
}

/// Preserve the shared table/expression error identity through DDL rollback.
fn backfill_failure(error: tidb_table::kv_table::KvTableError) -> LockSqlError {
    let error = tidb_ddl_build::table_backfill::index_backfill_error(error);
    LockSqlError::from(error)
}


/// Go `newReorgTableMutateContext(newReorgExprCtxWithReorgMeta(job.ReorgMeta))`:
/// the table context a reorganization batch writes under, the job's SQL
/// mode and location with no session.
#[must_use]
fn reorg_table_mutate_context(
    vars: &tidb_ddl_build::session_pool::ReorgSessionVars,
) -> tidb_table::context::StaticMutateContext {
    tidb_table::context::StaticMutateContext::for_reorg_meta(vars.sql_mode, vars.time_zone.clone())
}

#[cfg(test)]
mod reorg_context_tests {
    use super::*;
    use tidb_datatype::SessionTimeZone;
    use tidb_table::context::MutateContext;

    /// The statement context the worker built for a reorganization before it
    /// held Go's `reorgTableMutateContext` (Go `newReorgSessCtx`) over the
    /// job's SQL mode and time zone. Go's reorganization reads the current time
    /// in the job's zone, as a NULL bound for a `TIMESTAMP NOT NULL` column does.
    fn reorg_statement_context(vars: &tidb_ddl_build::session_pool::ReorgSessionVars) -> tidb_sessionctx::StmtContext {
        tidb_ddl_build::table_info_build::default_ddl_statement_context()
            .with_ddl_sql_mode(vars.sql_mode)
            .with_lazy_clock(None, vars.time_zone.clone())
            .for_reorg()
    }

    /// Every value the table layer and expression evaluation read from a
    /// reorganization's context, and from its `for_reorg` derivative.
    fn fingerprint(ctx: &dyn MutateContext) -> Vec<String> {
        let mut values = vec![
            format!("{:?}", ctx.write_conversion_flags()),
            format!("{:?}", ctx.strict()),
            format!("{:?}", ctx.ignore_err()),
            format!("{:?}", ctx.new_collation_enabled()),
            format!("{:?}", ctx.pessimistic_transaction()),
            format!("{:?}", ctx.pessimistic_check_in_prewrite()),
            format!("{:?}", ctx.optimistic_transaction()),
            format!("{:?}", ctx.writes_untouched_indices()),
            format!("{:?}", ctx.tmp_table_max_size()),
            format!("{:?}", ctx.reorg_default_conversion_flags()),
            format!("{:?}", ctx.query_default_conversion_flags()),
            format!("{:?}", ctx.show_default_conversion_flags()),
            format!("{:?}", ctx.session_zone()),
            format!("{:?}", ctx.type_flags()),
            format!("{:?}", ctx.truncate_level()),
            format!("{:?}", ctx.division_by_zero_level()),
            format!("{:?}", ctx.strict_sql_mode()),
            format!("{:?}", ctx.date_modes()),
            format!("{:?}", ctx.time_zone()),
            format!("{:?}", ctx.connection_charset_info()),
            format!("{:?}", ctx.like_default_escape()),
            format!("{:?}", ctx.no_unsigned_subtraction()),
            format!("{:?}", ctx.sysdate_is_now()),
            format!("{:?}", ctx.max_allowed_packet()),
            format!("{:?}", ctx.div_precision_increment()),
            format!("{:?}", ctx.default_week_format()),
            format!("{:?}", ctx.windowing_use_high_precision()),
            format!("{:?}", ctx.current_database()),
            format!("{:?}", ctx.connection_id()),
            // The clock's offset in the job's zone; its instant is "now".
            format!("{:?}", ctx.now().map(|(_, _, offset)| offset)),
        ];
        ctx.append_warning(1292, "w");
        values.push(format!("{:?}", ctx.take_warnings()));
        values
    }

    /// Go's `reorgTableMutateContext` over `newReorgExprCtxWithReorgMeta`
    /// reads as the statement context the worker used to build for a
    /// reorganization, for every SQL-mode shape and zone kind a job carries.
    #[test]
    fn the_reorg_table_context_reads_as_the_reorg_statement_did() {
        let modes = [
            tidb_mysql::DefaultSQLMode,
            "",
            "STRICT_ALL_TABLES",
            "ALLOW_INVALID_DATES",
            "NO_ZERO_DATE,NO_ZERO_IN_DATE",
            "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO",
        ];
        let zones = [
            SessionTimeZone::utc(),
            SessionTimeZone::Fixed {
                name: "+08:00".to_owned(),
                offset_secs: 8 * 3600,
            },
            SessionTimeZone::Named(chrono_tz::America::New_York),
        ];
        for mode in modes {
            let sql_mode = tidb_mysql::get_sql_mode(mode).expect("a valid SQL mode").0;
            for zone in &zones {
                let vars = tidb_ddl_build::session_pool::ReorgSessionVars {
                    sql_mode,
                    time_zone: zone.clone(),
                };
                let statement = reorg_statement_context(&vars);
                let table = reorg_table_mutate_context(&vars);
                assert_eq!(fingerprint(&table), fingerprint(&statement), "{mode} {zone:?}");
                assert_eq!(
                    fingerprint(&*MutateContext::for_reorg(&table)),
                    fingerprint(&*MutateContext::for_reorg(&statement)),
                    "{mode} {zone:?} for_reorg"
                );
            }
        }
    }
}

