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

//! Go `br/pkg/restore/snap_client/client.go`: `SnapClient`, its SST import
//! setup and its schema side (loading the backup schemas, preallocating
//! table IDs, the checkpoint, creating policies, databases and tables,
//! replaying DDL jobs, the checksum). Raw and txn KV restore and the PiTR
//! hooks (`InstallPiTRSupport`, `GetPartitionMap`) are not ported.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_model::{Job, PolicyInfo};
use tidb_proto::backup::{BackupMeta, CipherInfo, StorageBackend};
use tidb_proto::kvrpcpb;
use tikv_client::pd_http;

use super::import::{
    ImporterCallback, KvMode, RewriteMode, SnapFileImporter, SnapFileImporterOptions,
};
use crate::checkpoint::manager::SnapshotMetaManager;
use crate::checkpoint::restore::{
    start_checkpoint_runner_for_restore, PreallocIDs as CheckpointPreallocIDs, RestoreRunner,
};
use crate::checkpoint::ChecksumItem;
use crate::conn::{get_all_tikv_stores_with_retry, StoreBehavior};
use crate::errors::{self, Result};
use crate::glue::{Domain, Glue};
use crate::metautil;
use crate::pdclient::PdClient;
use crate::pdutil::{ClusterConfig, PdController};
use crate::restore::import_client::ImportClient;
use crate::restore::internal::prealloc_db::{new_db, PolicyMap, DB};
use crate::restore::internal::prealloc_table_id::{self, PreallocIDs};
use crate::restore::misc::UniqueTableName;
use crate::restore::restorer::{MultiTablesRestorer, SstRestorer};
use crate::restore::split::{self, new_client, SplitClient};
use crate::restore_utils::common::{CreatedTable, DatabaseRestorePlan, TableRestorePlan};
use crate::utils::store_manager::Keepalive;
use crate::utils::worker_pool::{ErrGroup, WorkerPool};

/// Go `resetSpeedLimitRetryTimes`.
const RESET_SPEED_LIMIT_RETRY_TIMES: usize = 3;
/// Go `resetSpeedLimitTimeout`.
const RESET_SPEED_LIMIT_TIMEOUT: Duration = Duration::from_secs(60);
/// Go `maxSplitKeysOnce`.
pub const MAX_SPLIT_KEYS_ONCE: usize = 10240;

/// Go `SnapClient`, its import fields.
pub struct SnapClient {
    restorer: Mutex<Option<Arc<dyn SstRestorer>>>,
    pub(crate) importer: Option<Arc<SnapFileImporter>>,
    pub(crate) pd_client: Arc<dyn PdClient>,
    pub(crate) pd_http_client: Option<pd_http::Client>,
    pub(crate) security: tidb_pd_client::ClusterSecurity,
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
    pub(crate) cipher: Option<CipherInfo>,
    concurrency_per_store: usize,
    region_scan_concurrency: usize,
    split_region_index_step: usize,
    coarse_scatter: bool,
    keepalive: Keepalive,
    rate_limit: u64,
    pub(crate) store_count: usize,
    worker_pool: Option<WorkerPool>,
    rewrite_mode: RewriteMode,
    /// Go `checkpointRunner`.
    pub checkpoint_runner: Option<Arc<RestoreRunner>>,
    pub(crate) support_policy: bool,
    no_schema: bool,
    pub(crate) databases: BTreeMap<String, metautil::Database>,
    ddl_jobs: Vec<Job>,
    rebased_tables_map: HashSet<UniqueTableName>,
    pub(crate) backup_meta: BackupMeta,
    db: Option<Arc<DB>>,
    db_pool: Vec<Arc<DB>>,
    prealloced_ids: Option<Arc<PreallocIDs>>,
    pub(crate) dom: Option<Arc<dyn Domain>>,
    policy_mode: String,
    policy_map: Option<Arc<PolicyMap>>,
    batch_ddl_size: usize,
    full_cluster_restore: bool,
    with_sys_table: bool,
    pub(crate) checkpoint_checksum: HashMap<i64, ChecksumItem>,
    pub(crate) temporary_system_tables_renamed: bool,
    restore_uuid: uuid::Uuid,
    pub(crate) privilege_table_rows_collate_compatibility: bool,
}

impl SnapClient {
    /// Go `NewRestoreClient`.
    #[must_use]
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        pd_http_client: Option<pd_http::Client>,
        security: tidb_pd_client::ClusterSecurity,
        keepalive: Keepalive,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            restorer: Mutex::new(None),
            importer: None,
            pd_client,
            pd_http_client,
            security,
            runtime,
            cipher: None,
            concurrency_per_store: 0,
            region_scan_concurrency: 0,
            split_region_index_step: split::DEFAULT_REGION_INDEX_STEP,
            coarse_scatter: false,
            keepalive,
            rate_limit: 0,
            store_count: 0,
            worker_pool: None,
            rewrite_mode: RewriteMode::Legacy,
            checkpoint_runner: None,
            support_policy: false,
            no_schema: false,
            databases: BTreeMap::new(),
            ddl_jobs: Vec::new(),
            rebased_tables_map: HashSet::new(),
            backup_meta: BackupMeta::default(),
            db: None,
            db_pool: Vec::new(),
            prealloced_ids: None,
            dom: None,
            policy_mode: String::new(),
            policy_map: None,
            batch_ddl_size: 0,
            full_cluster_restore: false,
            with_sys_table: false,
            checkpoint_checksum: HashMap::new(),
            temporary_system_tables_renamed: false,
            restore_uuid: uuid::Uuid::nil(),
            privilege_table_rows_collate_compatibility: false,
        }
    }

    /// Go `GetRestorer`: the restorer the import set up, made once.
    ///
    /// # Panics
    /// Before [`Self::init_clients`], as Go's nil `getRestorerFn`.
    pub fn get_restorer(
        &self,
        ctx: &Context,
        checkpoint_runner: Option<Arc<RestoreRunner>>,
    ) -> Arc<dyn SstRestorer> {
        let mut restorer = self.restorer.lock().unwrap();
        Arc::clone(restorer.get_or_insert_with(|| {
            let importer = Arc::clone(
                self.importer
                    .as_ref()
                    .expect("initClients sets the importer"),
            );
            let pool = self
                .worker_pool
                .clone()
                .expect("initClients sets the worker pool");
            Arc::new(MultiTablesRestorer::new(
                ctx,
                importer,
                pool,
                checkpoint_runner,
            ))
        }))
    }

    /// Go `Close`: the sessions and the restorer closed (a restorer failure
    /// is only logged).
    pub fn close(&self) {
        self.close_conn();
        if let Some(r) = self.restorer.lock().unwrap().as_ref() {
            if r.close().is_err() {
                tidb_log::warn("failed to close file restorer", &[]);
            }
        }
        tidb_log::info("Restore client closed", &[]);
    }

    /// Go `SetRateLimit`.
    pub fn set_rate_limit(&mut self, rate_limit: u64) {
        self.rate_limit = rate_limit;
    }

    /// Go `SetCrypter`.
    pub fn set_crypter(&mut self, crypter: Option<CipherInfo>) {
        self.cipher = crypter;
    }

    /// Go `updateConcurrency`.
    fn update_concurrency(&mut self) {
        const DOWNLOAD_WORKER_POOL_SIZE_PER_STORE: usize = 7186;
        let count = self.store_count * DOWNLOAD_WORKER_POOL_SIZE_PER_STORE;
        tidb_log::info(
            "download coarse worker pool",
            &[Field::new("size", Value::U64(count as u64))],
        );
        self.worker_pool = Some(WorkerPool::new(count, "file"));
    }

    /// Go `SetConcurrencyPerStore`.
    pub fn set_concurrency_per_store(&mut self, c: usize) {
        tidb_log::info(
            "per-store download worker pool",
            &[Field::new("size", Value::U64(c as u64))],
        );
        self.concurrency_per_store = c;
    }

    /// Go `SetRegionScanConcurrency`.
    pub fn set_region_scan_concurrency(&mut self, c: usize) {
        tidb_log::info(
            "region scan request concurrency",
            &[Field::new("size", Value::U64(c as u64))],
        );
        self.region_scan_concurrency = c;
    }

    /// Go `GetRegionScanConcurrency`.
    #[must_use]
    pub fn get_region_scan_concurrency(&self) -> usize {
        self.region_scan_concurrency
    }

    /// Go `SetSplitRegionIndexStep`.
    pub fn set_split_region_index_step(&mut self, step: usize) {
        tidb_log::info(
            "split region index step",
            &[Field::new("step", Value::U64(step as u64))],
        );
        self.split_region_index_step = split::normalize_region_index_step(step);
    }

    /// Go `GetSplitRegionIndexStep`.
    #[must_use]
    pub fn get_split_region_index_step(&self) -> usize {
        self.split_region_index_step
    }

    /// Go `SetCoarseScatter`.
    pub fn set_coarse_scatter(&mut self, coarse_scatter: bool) {
        self.coarse_scatter = coarse_scatter;
    }

    /// Go `GetCoarseScatter`.
    #[must_use]
    pub fn get_coarse_scatter(&self) -> bool {
        self.coarse_scatter
    }

    /// Go `SetRewriteMode`: keyspace rewrite when every TiKV is 6.6.0 or
    /// newer, else legacy.
    pub fn set_rewrite_mode(&mut self, ctx: &Context) {
        match crate::version::check_cluster_version(
            ctx,
            self.pd_client.as_ref(),
            &crate::version::check_version_for_keyspace_br,
        ) {
            Err(err) => {
                tidb_log::warn(
                    "Keyspace BR is not supported in this cluster, fallback to legacy restore",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
                self.rewrite_mode = RewriteMode::Legacy;
            }
            Ok(()) => self.rewrite_mode = RewriteMode::Keyspace,
        }
    }

    /// Go `GetRewriteMode`.
    #[must_use]
    pub fn get_rewrite_mode(&self) -> RewriteMode {
        self.rewrite_mode
    }

    /// The split client [`Self::init_clients`] and `SplitPoints` build.
    pub(crate) fn new_split_client(
        &self,
        split_concurrency: usize,
        on_split: Option<split::OnSplit>,
    ) -> split::PdSplitClient {
        new_client(
            Arc::clone(&self.pd_client),
            self.pd_http_client.clone(),
            self.security.clone(),
            Arc::clone(&self.runtime),
            MAX_SPLIT_KEYS_ONCE,
            split_concurrency,
            on_split,
        )
    }

    /// Go `InstallPiTRSupport`: with a log backup task running, the
    /// importer copies each batch's SSTs into the task's storage before
    /// ingesting it and commits them when it closes.
    ///
    /// # Errors
    /// The collector cannot be built, or the restore is incremental.
    pub fn install_pitr_support(&mut self, ctx: &Context, mut deps: super::pitr_collector::PitrCollDep) -> Result<()> {
        deps.load_max_copy_concurrency(ctx, self.concurrency_per_store)?;
        let collector = super::pitr_collector::new_pitr_coll(ctx, &deps)?;
        if !collector.enabled() {
            return Ok(());
        }
        if self.is_incremental() {
            // Even an error here would only confuse the user.
            let _ = collector.close();
            return Err(errors::annotatef(
                &errors::ERR_STREAM_LOG_TASK_EXIST,
                "it seems there is a log backup task exists, if an incremental restore were performed to such cluster, log backup cannot properly handle this, the restore will be aborted, you may stop the log backup task, then restore, finally restart the task",
            ));
        }
        let mut restore_uuid = self.restore_uuid;
        if restore_uuid.is_nil() {
            restore_uuid = uuid::Uuid::new_v4();
            tidb_log::warn(
                "UUID not found(checkpoint not enabled?), generating a new UUID for backup.",
                &[Field::new("uuid", Value::Str(restore_uuid.to_string()))],
            );
        }
        collector.set_restore_uuid(restore_uuid);
        let importer = self.importer.as_ref().ok_or_else(|| errors::errorf("the importer is not initialized"))?;
        let on_batch = collector.clone();
        importer.add_before_ingest_callback(Box::new(move |ctx, file_sets| on_batch.on_batch(ctx, file_sets)));
        importer.add_close_callback(Box::new(move |_| collector.close()));
        Ok(())
    }

    /// Go `initClients` for a TiDB (`TiDBFull`) restore: the store count,
    /// the worker pool, and the importer with its create and close
    /// callbacks. Raw and txn KV restore are not ported.
    ///
    /// # Errors
    /// The stores, or an importer create callback.
    pub fn init_clients(
        &mut self,
        ctx: &Context,
        backend: Option<StorageBackend>,
        api_version: kvrpcpb::ApiVersion,
    ) -> Result<()> {
        let stores = get_all_tikv_stores_with_retry(
            ctx,
            self.pd_client.as_ref(),
            StoreBehavior::SkipTiFlash,
        )
        .map_err(|e| errors::annotate(e, "failed to get stores"))?;
        self.store_count = stores.len();
        self.update_concurrency();

        let mut create_callbacks: Vec<ImporterCallback> = Vec::new();
        let mut close_callbacks: Vec<ImporterCallback> = Vec::new();
        let (c1, s1) = (ctx.clone(), stores.clone());
        create_callbacks.push(Box::new(move |importer| {
            importer.check_multi_ingest_support(&c1, &s1)
        }));
        let (c2, s2) = (ctx.clone(), stores.clone());
        create_callbacks.push(Box::new(move |importer| {
            importer.check_peer_download_retry_support(&c2, &s2)
        }));
        if self.rate_limit != 0 {
            let pool = self
                .worker_pool
                .clone()
                .expect("updateConcurrency sets the worker pool");
            let (create_cb, close_cb) =
                set_speed_limit_callbacks(ctx, Arc::clone(&self.pd_client), pool, self.rate_limit);
            create_callbacks.push(create_cb);
            close_callbacks.push(close_cb);
        }

        let meta_client: Arc<dyn SplitClient> =
            Arc::new(self.new_split_client(self.store_count + 1, None));
        let import_cli = Arc::new(ImportClient::new(
            Arc::clone(&meta_client),
            self.security.clone(),
            self.keepalive,
            Arc::clone(&self.runtime),
        ));
        let opt = SnapFileImporterOptions::new(
            self.cipher.clone(),
            meta_client,
            import_cli,
            backend,
            self.rewrite_mode,
            stores,
            self.concurrency_per_store,
            self.region_scan_concurrency,
            create_callbacks,
            close_callbacks,
        );
        self.importer = Some(SnapFileImporter::new(api_version, KvMode::TiDBFull, opt)?);
        Ok(())
    }
}

/// Go `SetSpeedLimitCallbacks`: the create callback sets the download speed
/// limit and refreshes it every 3 minutes; the close callback stops the
/// refresh and resets the limit, retried.
pub fn set_speed_limit_callbacks(
    ctx: &Context,
    pd_client: Arc<dyn PdClient>,
    pool: WorkerPool,
    rate_limit: u64,
) -> (ImporterCallback, ImporterCallback) {
    let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(0);
    let worker: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::default();
    let (ctx1, pd1, pool1, worker1) = (
        ctx.clone(),
        Arc::clone(&pd_client),
        pool.clone(),
        Arc::clone(&worker),
    );
    let create: ImporterCallback = Box::new(move |importer: &SnapFileImporter| {
        set_speed_limit_fn(&ctx1, pd1.as_ref(), &pool1, importer, rate_limit)
            .map_err(|e| errors::annotate(e, "failed to set download speed limit"))?;
        let refresher = SpeedLimitSetter::of(importer);
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_secs(180));
            loop {
                crossbeam_channel::select! {
                    recv(ctx1.done()) -> _ => return,
                    recv(stop_rx) -> _ => return,
                    recv(ticker) -> _ => {
                        if let Err(err) = refresher.set_all(&ctx1, pd1.as_ref(), &pool1, rate_limit) {
                            tidb_log::warn("failed to set download speed limit, retry it", &[Field::new("error", Value::Str(err.to_string()))]);
                        }
                    }
                }
            }
        });
        *worker1.lock().unwrap() = Some(handle);
        Ok(())
    });
    let close: ImporterCallback = Box::new(move |importer: &SnapFileImporter| {
        drop(stop_tx);
        if let Some(h) = worker.lock().unwrap().take() {
            let _ = h.join();
        }
        let mut reset_err = None;
        for retry in 0..RESET_SPEED_LIMIT_RETRY_TIMES {
            let (reset_ctx, cancel) =
                Context::with_timeout(&Context::background(), RESET_SPEED_LIMIT_TIMEOUT);
            let res = set_speed_limit_fn(&reset_ctx, pd_client.as_ref(), &pool, importer, 0);
            cancel.cancel();
            match res {
                Ok(()) => {
                    reset_err = None;
                    break;
                }
                Err(err) => {
                    tidb_log::warn(
                        "failed to reset speed limit, retry it",
                        &[
                            Field::new("retry time", Value::I64(retry as i64)),
                            Field::new("error", Value::Str(err.to_string())),
                        ],
                    );
                    reset_err = Some(err);
                    std::thread::sleep(Duration::from_secs(retry as u64 + 3));
                }
            }
        }
        if let Some(err) = reset_err {
            tidb_log::error(
                "failed to reset speed limit, please reset it manually",
                &[Field::new("error", Value::Str(err.to_string()))],
            );
            return Err(err);
        }
        Ok(())
    });
    (create, close)
}

/// What the speed limit refresher needs of the importer.
struct SpeedLimitSetter {
    task_id: String,
    import_client: Arc<dyn crate::restore::import_client::ImporterClient>,
}

impl SpeedLimitSetter {
    fn of(importer: &SnapFileImporter) -> Arc<Self> {
        Arc::new(Self {
            task_id: importer.task_id().to_owned(),
            import_client: Arc::clone(importer.import_client()),
        })
    }

    /// Go `SnapFileImporter.SetDownloadSpeedLimit`.
    fn set(&self, ctx: &Context, store_id: u64, rate_limit: u64) -> Result<()> {
        super::import::set_download_speed_limit(
            self.import_client.as_ref(),
            &self.task_id,
            ctx,
            store_id,
            rate_limit,
        )
    }

    /// Go `SetSpeedLimitFn(ctx, pdClient, pool)(importer, limit)`.
    fn set_all(
        self: &Arc<Self>,
        ctx: &Context,
        pd_client: &dyn PdClient,
        pool: &WorkerPool,
        limit: u64,
    ) -> Result<()> {
        let stores = get_all_tikv_stores_with_retry(ctx, pd_client, StoreBehavior::SkipTiFlash)
            .map_err(|e| errors::annotate(e, "failed to get stores"))?;
        let (eg, ectx) = ErrGroup::with_context(ctx);
        for store in stores {
            if let Some(err) = ectx.err() {
                return Err(Arc::new(err));
            }
            let (this, ectx) = (Arc::clone(self), ectx.clone());
            pool.apply_on_error_group(&eg, move || this.set(&ectx, store.id, limit));
        }
        eg.wait()
    }
}

/// Go `SetSpeedLimitFn`.
///
/// # Errors
/// The stores, or a store's error.
pub fn set_speed_limit_fn(
    ctx: &Context,
    pd_client: &dyn PdClient,
    pool: &WorkerPool,
    importer: &SnapFileImporter,
    limit: u64,
) -> Result<()> {
    SpeedLimitSetter::of(importer).set_all(ctx, pd_client, pool, limit)
}

/// Go `strictPlacementPolicyMode`.
const STRICT_PLACEMENT_POLICY_MODE: &str = "STRICT";
/// Go `ignorePlacementPolicyMode`.
const IGNORE_PLACEMENT_POLICY_MODE: &str = "IGNORE";
/// Go `defaultDDLConcurrency`.
const DEFAULT_DDL_CONCURRENCY: usize = 64;
/// Go `minBatchDdlSize`.
const MIN_BATCH_DDL_SIZE: usize = 1;

fn log_str(k: &'static str, v: impl ToString) -> Field {
    Field::new(k, Value::Str(v.to_string()))
}

/// Go `getMinUserTableID`: the smallest table or partition ID outside the
/// system databases.
fn get_min_user_table_id(tables: &[metautil::Table]) -> i64 {
    let mut min = i64::MAX;
    for table in tables {
        if crate::utils::misc::is_sys_or_temp_sys_db(table.db.name.original()) {
            continue;
        }
        min = min.min(table.info().id);
        if let Some(part) = &table.info().partition {
            for id in part.read().definitions.map_visible(|d| d.id) {
                min = min.min(id);
            }
        }
    }
    min
}

/// Go `makeDBPool`: up to `size` sessions; the ones made before an error
/// are kept.
fn make_db_pool(size: usize, db_factory: impl Fn() -> Result<DB>) -> (Vec<Arc<DB>>, Result<()>) {
    let mut pool = Vec::with_capacity(size);
    for _ in 0..size {
        match db_factory() {
            Ok(db) => pool.push(Arc::new(db)),
            Err(e) => return (pool, Err(e)),
        }
    }
    (pool, Ok(()))
}

/// Go `needLoadSchemas`.
fn need_load_schemas(backup_meta: &BackupMeta) -> bool {
    !(backup_meta.is_raw_kv || backup_meta.is_txn_kv)
}

/// Go `ApplyWithIDInErrorGroup` over a session pool: `items` run on
/// `pool.len()` workers, each holding one session at a time; the first
/// error stops the rest. The results keep the items' order.
fn run_on_pool<T: Sync, R: Send>(
    ctx: &Context,
    pool: &[Arc<DB>],
    items: &[T],
    f: impl Fn(&Context, &DB, &T) -> Result<R> + Sync,
) -> Result<Vec<R>> {
    let (ectx, cancel) = Context::with_cancel(ctx);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let first: Mutex<Option<errors::Error>> = Mutex::new(None);
    let results: Mutex<Vec<Option<R>>> = Mutex::new((0..items.len()).map(|_| None).collect());
    std::thread::scope(|s| {
        for db in pool.iter().take(items.len().max(1)) {
            let (ectx, next, first, results, f, cancel) =
                (&ectx, &next, &first, &results, &f, &cancel);
            s.spawn(move || loop {
                if ectx.err().is_some() {
                    return;
                }
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let Some(item) = items.get(i) else { return };
                match f(ectx, db, item) {
                    Ok(r) => results.lock().unwrap()[i] = Some(r),
                    Err(e) => {
                        let mut first = first.lock().unwrap();
                        if first.is_none() {
                            *first = Some(e);
                            cancel.cancel();
                        }
                        return;
                    }
                }
            });
        }
    });
    cancel.cancel();
    if let Some(e) = first.into_inner().unwrap() {
        return Err(e);
    }
    if let Some(e) = ctx.err() {
        return Err(errors::errorf(e.to_string()));
    }
    Ok(results
        .into_inner()
        .unwrap()
        .into_iter()
        .flatten()
        .collect())
}

/// Go `SortTablesBySchemaID`: by schema ID, then table ID.
#[must_use]
pub fn sort_tables_by_schema_id(tables: &[metautil::Table]) -> Vec<metautil::Table> {
    let mut ordered = tables.to_vec();
    ordered.sort_by_key(|t| (t.db.id, t.info().id));
    ordered
}

impl SnapClient {
    /// Go `CreatePreallocIDCheckpoint`.
    #[must_use]
    pub fn create_prealloc_id_checkpoint(&self) -> Option<CheckpointPreallocIDs> {
        self.prealloced_ids
            .as_ref()
            .and_then(|ids| ids.create_checkpoint())
    }

    /// Go `closeConn`: the sessions are dropped with the client.
    fn close_conn(&self) {}

    /// Go `CleanTablesIfTemporarySystemTablesRenamed`: once a checkpointed
    /// physical load renamed the temporary tables, they are not restored
    /// again.
    #[must_use]
    pub fn clean_tables_if_temporary_system_tables_renamed(
        &self,
        load_stats_physical: bool,
        load_sys_table_physical: bool,
        tables: Vec<metautil::Table>,
    ) -> Vec<metautil::Table> {
        if !self.temporary_system_tables_renamed {
            return tables;
        }
        let checker = super::systable_restore::TemporaryTableChecker {
            load_stats_physical,
            load_sys_table_physical,
        };
        tables
            .into_iter()
            .filter(|t| {
                checker
                    .check_temporary_tables(t.db.name.original(), t.info().name.original())
                    .is_none()
            })
            .collect()
    }

    /// Go `GetClusterID`: the downstream cluster's ID.
    #[must_use]
    pub fn get_cluster_id(&self, ctx: &Context) -> u64 {
        self.pd_client.get_cluster_id(ctx)
    }

    /// Go `GetDomain`.
    ///
    /// # Errors
    /// Before [`Self::init_connections`].
    pub fn dom(&self) -> Result<&Arc<dyn Domain>> {
        self.dom
            .as_ref()
            .ok_or_else(|| errors::errorf("the restore client has no domain"))
    }

    /// Go `rc.db`.
    pub(crate) fn db(&self) -> Result<&Arc<DB>> {
        self.db
            .as_ref()
            .ok_or_else(|| errors::errorf("the restore client has no session"))
    }

    /// Go `GetSupportPolicy`.
    #[must_use]
    pub fn get_support_policy(&self) -> bool {
        self.support_policy
    }

    /// Go `SetCheckPrivilegeTableRowsCollateCompatibility`.
    pub fn set_check_privilege_table_rows_collate_compatibility(&mut self, v: bool) {
        self.privilege_table_rows_collate_compatibility = v;
    }

    /// Go `GetCheckPrivilegeTableRowsCollateCompatibility`.
    #[must_use]
    pub fn get_check_privilege_table_rows_collate_compatibility(&self) -> bool {
        self.privilege_table_rows_collate_compatibility
    }

    /// Go `SetBatchDdlSize`.
    pub fn set_batch_ddl_size(&mut self, size: usize) {
        self.batch_ddl_size = size;
    }

    /// Go `GetBatchDdlSize`.
    #[must_use]
    pub fn get_batch_ddl_size(&self) -> usize {
        self.batch_ddl_size
    }

    /// Go `SetWithSysTable`.
    pub fn set_with_sys_table(&mut self, with_sys_table: bool) {
        self.with_sys_table = with_sys_table;
    }

    /// Go `rc.withSysTable`.
    #[must_use]
    pub fn with_sys_table(&self) -> bool {
        self.with_sys_table
    }

    /// Go `SetPlacementPolicyMode`: `STRICT` unless `IGNORE`.
    pub fn set_placement_policy_mode(&mut self, mode: &str) {
        self.policy_mode = if mode.eq_ignore_ascii_case(IGNORE_PLACEMENT_POLICY_MODE) {
            IGNORE_PLACEMENT_POLICY_MODE.to_owned()
        } else {
            STRICT_PLACEMENT_POLICY_MODE.to_owned()
        };
        tidb_log::info(
            "set placement policy mode",
            &[log_str("mode", &self.policy_mode)],
        );
    }

    /// Go `AllocTableIDs`: the backed-up table IDs reserved where free, so
    /// TiKV need not rewrite their keys, or the checkpoint's reservation
    /// reused. Whether a user table ID was not kept while a physical stats
    /// load needs it.
    ///
    /// # Errors
    /// The meta transaction's error, or a checkpoint that does not match.
    pub fn alloc_table_ids(
        &mut self,
        _ctx: &Context,
        tables: &[metautil::Table],
        load_stats_physical: bool,
        load_sys_table_physical: bool,
        reuse_prealloc_ids: Option<&CheckpointPreallocIDs>,
    ) -> Result<bool> {
        let ids = match reuse_prealloc_ids {
            None => {
                let mut ids = None;
                self.dom()?.run_in_new_meta_txn(&mut |m| {
                    ids = Some(prealloc_table_id::new_and_prealloc(tables, m)?);
                    Ok(())
                })?;
                ids.ok_or_else(|| errors::errorf("the meta transaction did not run"))?
            }
            Some(reuse) => prealloc_table_id::reuse_checkpoint(Some(reuse), tables)?,
        };
        let mut load_stats_physical = load_stats_physical;
        let mut user_table_id_not_reused = false;
        if load_stats_physical {
            let min_user_table_id = get_min_user_table_id(tables);
            let (start, _) = ids.get_id_range();
            if min_user_table_id != i64::MAX && min_user_table_id < start {
                user_table_id_not_reused = true;
                load_stats_physical = false;
            }
        }
        if reuse_prealloc_ids.is_some() && (load_stats_physical || load_sys_table_physical) {
            let checker = super::systable_restore::TemporaryTableChecker {
                load_stats_physical,
                load_sys_table_physical,
            };
            let is = self.dom()?.info_schema()?;
            for table in tables {
                if let Some(db_name) = checker
                    .check_temporary_tables(table.db.name.original(), table.info().name.original())
                {
                    let downstream_id = ids.alloc_id(table.info().id)?;
                    if let Some(info) = is.table_by_name(&db_name, table.info().name.original()) {
                        if info.id == downstream_id {
                            self.temporary_system_tables_renamed = true;
                        }
                        break;
                    }
                }
            }
        }
        tidb_log::info("registering the table IDs", &[log_str("ids", &ids)]);
        let ids = Arc::new(ids);
        for db in self.db_pool.iter().chain(self.db.iter()) {
            db.register_preallocated_ids(Arc::clone(&ids));
        }
        self.prealloced_ids = Some(ids);
        Ok(user_table_id_not_reused)
    }

    /// Go `GetPreAllocedTableIDRange`: `[0, 0]` for an empty range.
    ///
    /// # Errors
    /// Before [`Self::alloc_table_ids`].
    pub fn get_pre_alloced_table_id_range(&self) -> Result<[i64; 2]> {
        let ids = self
            .prealloced_ids
            .as_ref()
            .ok_or_else(|| errors::errorf("No preAlloced IDs"))?;
        let (start, end) = ids.get_id_range();
        if start >= end {
            tidb_log::warn("PreAlloced IDs range is empty, no table to restore", &[]);
            return Ok([0, 0]);
        }
        Ok([start, end])
    }
}

/// The result of Go `InitCheckpoint`.
#[derive(Debug, Default)]
pub struct CheckpointInit {
    /// Go `checkpointSetWithTableID`: the restored range keys by table.
    pub checkpoint_set_with_table_id: HashMap<i64, HashSet<String>>,
    /// Go `checkpointClusterConfig`: the schedulers config recorded by the
    /// first run.
    pub checkpoint_cluster_config: Option<ClusterConfig>,
    /// Go `newRestoreStartTS`.
    pub new_restore_start_ts: u64,
}

impl SnapClient {
    /// Go `InitCheckpoint`: a first run records the checkpoint metadata;
    /// a later one checks it against this restore and loads the restored
    /// ranges and checksums. Either way the checkpoint runner starts.
    ///
    /// # Errors
    /// The checkpoint does not match this restore, or a read or write error.
    #[allow(clippy::too_many_arguments)]
    pub fn init_checkpoint(
        &mut self,
        ctx: &Context,
        manager: &dyn SnapshotMetaManager,
        config: Option<&ClusterConfig>,
        restore_start_ts: u64,
        log_restored_ts: u64,
        hash: &[u8],
        checkpoint_exists: bool,
    ) -> Result<CheckpointInit> {
        let mut out = CheckpointInit::default();
        if checkpoint_exists {
            let meta = manager.load_checkpoint_metadata(ctx)?;
            self.restore_uuid = meta.restore_uuid;
            if meta.upstream_cluster_id != self.backup_meta.cluster_id {
                return Err(errors::errorf(format!(
                    "The upstream cluster id[{}] of the current snapshot restore does not match that[{}] recorded in checkpoint. \
                     Perhaps you should specify the last full backup storage instead, \
                     or just clean the checkpoint {manager} if the cluster has been cleaned up.",
                    self.backup_meta.cluster_id, meta.upstream_cluster_id
                )));
            }
            if meta.hash != hash {
                return Err(errors::errorf(format!(
                    "The hash of the current snapshot restore does not match that recorded in checkpoint. \
                     Please don't use the checkpoint, or use the the same restore command. checkpoint manager: {manager}"
                )));
            }
            if meta.restored_ts != self.backup_meta.end_version {
                return Err(errors::errorf(format!(
                    "The current snapshot restore want to restore cluster to the BackupTS[{}], which is different from that[{}] recorded in checkpoint. \
                     Perhaps you should specify the last full backup storage instead, \
                     or just clean the checkpoint {manager} if the cluster has been cleaned up.",
                    self.backup_meta.end_version, meta.restored_ts
                )));
            }
            // The filters depend on the PiTR restored ts, so the snapshot
            // checkpoint checks it did not change.
            if meta.log_restored_ts != log_restored_ts {
                return Err(errors::errorf(format!(
                    "The current PITR want to restore cluster to the log restored ts[{log_restored_ts}], which is different from that[{}] recorded in checkpoint. \
                     Perhaps you shoud specify the log restored ts instead, \
                     or just clean the checkpoint database[{manager}] if the cluster has been cleaned up.",
                    meta.log_restored_ts
                )));
            }
            out.new_restore_start_ts = meta.restore_start_ts;
            // Without a recorded config, the undo uses this run's removal.
            out.checkpoint_cluster_config = meta.schedulers_config;
            let sets = &mut out.checkpoint_set_with_table_id;
            let t1 = manager.load_checkpoint_data(ctx, &mut |table_id, v| {
                sets.entry(table_id).or_default().insert(v.range_key);
                Ok(())
            })?;
            let (checksum, t2) = manager.load_checkpoint_checksum(ctx)?;
            self.checkpoint_checksum = checksum;
            crate::summary::adjust_start_time_to_earlier_time(t1.max(t2));
        } else {
            let restore_id = uuid::Uuid::new_v4();
            let meta = crate::checkpoint::restore::CheckpointMetadataForSnapshotRestore {
                upstream_cluster_id: self.backup_meta.cluster_id,
                restore_start_ts,
                restored_ts: self.backup_meta.end_version,
                log_restored_ts,
                // A nil config means the undo function.
                schedulers_config: config.map(|c| ClusterConfig {
                    schedulers: c.schedulers.clone(),
                    schedule_cfg: c.schedule_cfg.clone(),
                    rule_id: c.rule_id.clone(),
                }),
                hash: hash.to_vec(),
                prealloc_ids: self.create_prealloc_id_checkpoint(),
                restore_uuid: restore_id,
            };
            self.restore_uuid = restore_id;
            out.new_restore_start_ts = restore_start_ts;
            manager.save_checkpoint_metadata(ctx, Some(&meta))?;
        }
        self.checkpoint_runner = Some(start_checkpoint_runner_for_restore(ctx, manager)?);
        Ok(out)
    }

    /// Go `WaitForFinishCheckpoint`.
    pub fn wait_for_finish_checkpoint(&self, ctx: &Context, flush: bool) {
        if let Some(r) = &self.checkpoint_runner {
            r.wait_for_finish(ctx, flush);
        }
    }

    /// Go `InitConnections`: the DDL session (after the placement mode is
    /// set), the domain, and a pool of 64 sessions; a pool error is logged
    /// and returned with the sessions made so far kept.
    ///
    /// # Errors
    /// A session cannot be created.
    pub fn init_connections(&mut self, g: &dyn Glue) -> Result<()> {
        let (db, support_policy) = new_db(g, &self.policy_mode)?;
        self.db = Some(Arc::new(db));
        self.support_policy = support_policy;
        self.dom = Some(g.get_domain()?);
        // Executing DDL is bound on enqueueing the jobs, so the pool size is
        // fixed.
        let (pool, res) = make_db_pool(DEFAULT_DDL_CONCURRENCY, || {
            new_db(g, &self.policy_mode).map(|(db, _)| db)
        });
        self.db_pool = pool;
        if let Err(err) = &res {
            tidb_log::warn(
                "create session pool failed, we will send DDLs only by created sessions",
                &[
                    log_str("error", err),
                    Field::new("sessionCount", Value::U64(self.db_pool.len() as u64)),
                ],
            );
        }
        res
    }

    /// Go `LoadSchemaIfNeededAndInitClient`: the backed-up schemas and DDL
    /// jobs, the import clients, and whether this is a full cluster restore.
    /// Raw and txn KV backups are not supported.
    ///
    /// # Errors
    /// The backup metadata cannot be read, or the import clients fail.
    #[allow(clippy::too_many_arguments)]
    pub fn load_schema_if_needed_and_init_client(
        &mut self,
        ctx: &Context,
        backup_meta: BackupMeta,
        backend: Option<StorageBackend>,
        reader: &metautil::MetaReader,
        load_stats: bool,
        api_version: kvrpcpb::ApiVersion,
        has_explicit_filter: bool,
        is_full_restore: bool,
        with_sys: bool,
    ) -> Result<()> {
        if !need_load_schemas(&backup_meta) {
            return Err(errors::errorf("raw and txn kv restore are not supported"));
        }
        self.databases =
            metautil::load_backup_tables(ctx, reader, load_stats).map_err(errors::errorf)?;
        let ddls = reader.read_ddls(ctx).map_err(errors::errorf)?;
        self.ddl_jobs = decode_ddl_jobs(&ddls)?;
        tidb_log::info(
            "loaded backup meta",
            &[
                Field::new("databases", Value::U64(self.databases.len() as u64)),
                Field::new("jobs", Value::U64(self.ddl_jobs.len() as u64)),
            ],
        );
        self.backup_meta = backup_meta;
        self.init_clients(ctx, backend, api_version)?;
        self.init_full_cluster_restore(has_explicit_filter, is_full_restore, with_sys);
        Ok(())
    }

    /// Go `IsRawKvMode`.
    #[must_use]
    pub fn is_raw_kv_mode(&self) -> bool {
        self.backup_meta.is_raw_kv
    }

    /// Go `ResetTS`: PD's TSO moved past the backup.
    ///
    /// # Errors
    /// PD's error after the aggressive PD backoff.
    pub fn reset_ts(&self, ctx: &Context, pd_ctrl: &PdController) -> Result<()> {
        let restore_ts = self.backup_meta.end_version;
        tidb_log::info(
            "reset pd timestamp",
            &[Field::new("ts", Value::U64(restore_ts))],
        );
        crate::utils::retry::with_retry(
            ctx,
            || pd_ctrl.reset_ts(ctx, restore_ts),
            &mut crate::utils::backoff::new_aggressive_pd_backoff_strategy(),
        )
    }

    /// Go `GetDatabases`.
    #[must_use]
    pub fn get_databases(&self) -> Vec<metautil::Database> {
        self.databases.values().cloned().collect()
    }

    /// Go `GetDatabaseMap`: by database ID.
    #[must_use]
    pub fn get_database_map(&self) -> HashMap<i64, &metautil::Database> {
        self.databases.values().map(|d| (d.info.id, d)).collect()
    }

    /// Go `GetTableMap`: by table ID.
    #[must_use]
    pub fn get_table_map(&self) -> HashMap<i64, &metautil::Table> {
        self.databases
            .values()
            .flat_map(|d| d.tables.iter())
            .filter_map(|t| t.info.as_ref().map(|i| (i.id, t)))
            .collect()
    }

    /// Go `HasBackedUpSysDB`: backups since BR 5.1.0 hold the system tables.
    #[must_use]
    pub fn has_backed_up_sys_db(&self) -> bool {
        use crate::utils::misc::{temporary_db_name, SYSTEM_DB, SYS_DB, WORKLOAD_SCHEMA};
        [SYSTEM_DB, SYS_DB, WORKLOAD_SCHEMA]
            .iter()
            .any(|db| self.databases.contains_key(&temporary_db_name(db)))
    }

    /// Go `GetPlacementPolicies`.
    ///
    /// # Errors
    /// A policy that is not valid JSON.
    pub fn get_placement_policies(&self) -> Result<PolicyMap> {
        let mut policies = HashMap::new();
        for p in &self.backup_meta.policies {
            let info: PolicyInfo =
                serde_json::from_slice(&p.info).map_err(|e| errors::errorf(e.to_string()))?;
            policies.insert(info.name.lowercase().to_owned(), info);
        }
        Ok(Mutex::new(policies))
    }

    /// Go `GetDDLJobs`.
    #[must_use]
    pub fn get_ddl_jobs(&self) -> &[Job] {
        &self.ddl_jobs
    }

    /// Go `SetPolicyMap`.
    pub fn set_policy_map(&mut self, p: Option<Arc<PolicyMap>>) {
        self.policy_map = p;
    }

    /// Go `CreatePolicies`: every backed-up policy, for a full restore.
    ///
    /// # Errors
    /// The DDL error.
    pub fn create_policies(&self, ctx: &Context, policy_map: &PolicyMap) -> Result<()> {
        let db = self.db()?;
        for policy in policy_map.lock().unwrap().values() {
            db.create_placement_policy(ctx, policy)?;
        }
        Ok(())
    }

    /// Go `CreateDatabases`.
    ///
    /// # Errors
    /// The DDL error.
    pub fn create_databases(&self, ctx: &Context, dbs: &[metautil::Database]) -> Result<()> {
        let mut plans: Vec<DatabaseRestorePlan> = dbs
            .iter()
            .map(|db| DatabaseRestorePlan {
                source: db.clone(),
                target: db.info.clone_like_go(),
                reused: false,
            })
            .collect();
        self.create_databases_with_plan(ctx, &mut plans)
    }

    /// Go `CreateDatabasesWithPlan`: the target databases, sequentially or
    /// on the session pool; an existing one is marked reused.
    ///
    /// # Errors
    /// The DDL error.
    pub fn create_databases_with_plan(
        &self,
        ctx: &Context,
        plans: &mut [DatabaseRestorePlan],
    ) -> Result<()> {
        if self.is_skip_create_sql() {
            tidb_log::info("skip create database", &[]);
            return Ok(());
        }
        let policy_map = self.policy_map.as_deref();
        if self.db_pool.is_empty() {
            tidb_log::info("create databases sequentially", &[]);
            let db = self.db()?;
            for plan in plans.iter_mut() {
                if db.create_database(ctx, &plan.target, self.support_policy, policy_map)? {
                    plan.reused = true;
                }
            }
            return Ok(());
        }
        tidb_log::info(
            "create databases in db pool",
            &[
                Field::new("pool size", Value::U64(self.db_pool.len() as u64)),
                Field::new("number of db", Value::U64(plans.len() as u64)),
            ],
        );
        let exists = run_on_pool(ctx, &self.db_pool, &*plans, |ectx, db, plan| {
            db.create_database(ectx, &plan.target, self.support_policy, policy_map)
        })?;
        for (plan, exists) in plans.iter_mut().zip(exists) {
            plan.reused |= exists;
        }
        Ok(())
    }
}

/// Go `json.Unmarshal(ddls, &ddlJobs)`.
fn decode_ddl_jobs(ddls: &[u8]) -> Result<Vec<Job>> {
    if ddls.is_empty() {
        return Ok(Vec::new());
    }
    let values: Vec<serde_json::Value> =
        serde_json::from_slice(ddls).map_err(|e| errors::errorf(e.to_string()))?;
    values
        .into_iter()
        .map(|v| {
            let bytes = serde_json::to_vec(&v).map_err(|e| errors::errorf(e.to_string()))?;
            let mut job = Job::default();
            job.decode(&bytes)
                .map_err(|e| errors::errorf(e.to_string()))?;
            Ok(job)
        })
        .collect()
}

impl SnapClient {
    /// Go `generateRebasedTables`: an incremental restore rebases the auto
    /// IDs of the tables it creates; a full one creates every table with
    /// its own.
    fn generate_rebased_tables(&mut self, plans: &[TableRestorePlan]) {
        self.rebased_tables_map = if self.is_incremental() {
            plans
                .iter()
                .map(|p| UniqueTableName {
                    db: p.target_db.name.to_string(),
                    table: p.target_info.name.to_string(),
                })
                .collect()
        } else {
            HashSet::new()
        };
    }

    /// Go `CreateTables`.
    ///
    /// # Errors
    /// The DDL error.
    pub fn create_tables(
        &mut self,
        ctx: &Context,
        tables: &[metautil::Table],
        new_ts: u64,
    ) -> Result<Vec<CreatedTable<metautil::Table>>> {
        let plans: Vec<TableRestorePlan> = tables
            .iter()
            .map(|t| TableRestorePlan {
                source: t.clone(),
                target_db: t.db.clone_like_go(),
                target_info: t.info().clone_like_go(),
            })
            .collect();
        self.create_tables_with_plan(ctx, &plans, new_ts)
    }

    /// Go `CreateTablesWithPlan`: in batches on the session pool when the
    /// batch size allows, falling back to one table at a time on
    /// `ErrInvalidDDLJob`.
    ///
    /// # Errors
    /// The DDL error.
    pub fn create_tables_with_plan(
        &mut self,
        ctx: &Context,
        plans: &[TableRestorePlan],
        new_ts: u64,
    ) -> Result<Vec<CreatedTable<metautil::Table>>> {
        tidb_log::info(
            "start create tables",
            &[Field::new("total count", Value::U64(plans.len() as u64))],
        );
        self.generate_rebased_tables(plans);
        if self.batch_ddl_size > MIN_BATCH_DDL_SIZE && !self.db_pool.is_empty() {
            match self.create_tables_batch(ctx, plans, new_ts) {
                Ok(tables) => return Ok(tables),
                Err(err) if !crate::utils::retry::fall_back_to_create_table(err.as_ref()) => {
                    return Err(err)
                }
                Err(_) => tidb_log::info("fall back to the sequential create table", &[]),
            }
        }
        let pool = if self.db_pool.is_empty() {
            vec![Arc::clone(self.db()?)]
        } else {
            self.db_pool.clone()
        };
        self.create_tables_single(ctx, &pool, plans, new_ts)
    }

    /// The created table read back: its new schema, the clustered index
    /// checked, and its rewrite rules.
    fn created_table(
        &self,
        plan: &TableRestorePlan,
        new_ts: u64,
    ) -> Result<CreatedTable<metautil::Table>> {
        let new_table_info = crate::restore::misc::get_table_schema(
            self.dom()?.as_ref(),
            plan.target_db.name.original(),
            plan.target_info.name.original(),
        )?;
        let source = plan.source.info();
        if new_table_info.is_common_handle != source.is_common_handle {
            return Err(errors::annotatef(
                &errors::ERR_RESTORE_MODE_MISMATCH,
                format!(
                    "Clustered index option mismatch. Restored cluster's @@tidb_enable_clustered_index should be {} (backup table = {}, created table = {}).",
                    crate::restore::misc::transfer_bool_to_value(source.is_common_handle),
                    source.is_common_handle,
                    new_table_info.is_common_handle
                ),
            ));
        }
        let rules = crate::restore_utils::rewrite_rule::get_rewrite_rules(
            &new_table_info,
            source,
            new_ts,
            true,
        );
        Ok(CreatedTable {
            rewrite_rule: Some(rules),
            table: Some(new_table_info),
            old_table: Some(plan.source.clone()),
            target_db: plan.target_db.name.clone(),
        })
    }

    /// Go `createTables`: one batch, then the merge options of its tables.
    fn create_tables_in(
        &self,
        ctx: &Context,
        db: &DB,
        plans: &[TableRestorePlan],
        new_ts: u64,
    ) -> Result<Vec<CreatedTable<metautil::Table>>> {
        tidb_log::info("client to create tables", &[]);
        if self.is_skip_create_sql() {
            tidb_log::info("skip create table and alter autoIncID", &[]);
        } else {
            let targets: Vec<metautil::Table> =
                plans.iter().map(TableRestorePlan::target_table).collect();
            db.create_tables(
                ctx,
                &targets,
                &self.rebased_tables_map,
                self.support_policy,
                self.policy_map.as_deref(),
            )?;
        }
        let mut cts = Vec::with_capacity(plans.len());
        let mut with_merge_option = Vec::new();
        for plan in plans {
            let ct = self.created_table(plan, new_ts)?;
            if plan.source.is_merge_option_allowed
                || !plan.source.partition_merge_option_allowed.is_empty()
            {
                with_merge_option.push(cts.len());
            }
            cts.push(ct);
        }
        if !with_merge_option.is_empty() {
            let tables: Vec<&CreatedTable<metautil::Table>> =
                with_merge_option.iter().map(|&i| &cts[i]).collect();
            self.set_merge_option_for_tables(&tables)
                .map_err(|e| errors::annotate(e, "failed to batch set merge_option for tables"))?;
        }
        Ok(cts)
    }

    /// Go `setMergeOptionForTables`: a `merge_option=allow` region label for
    /// each table or partition the backup allowed merging, by the new IDs,
    /// put to PD 50 rules at a time.
    fn set_merge_option_for_tables(
        &self,
        created_tables: &[&CreatedTable<metautil::Table>],
    ) -> Result<()> {
        if created_tables.is_empty() {
            return Ok(());
        }
        tidb_log::info(
            "batch setting merge_option for tables and partitions",
            &[Field::new("count", Value::U64(created_tables.len() as u64))],
        );
        let dom = self.dom()?;
        let codec = dom.label_codec();
        let new_rule = |db: &str, table: &str, part: &str, id: i64| {
            let mut rule = tidb_ddl_label::Rule::new();
            rule.labels = vec![tidb_ddl_label::RegionLabel {
                key: "merge_option".into(),
                value: "allow".into(),
                ..Default::default()
            }]
            .into();
            rule.reset(codec.as_ref(), db, table, part, &[id]);
            rule
        };
        let mut rules = Vec::new();
        for ct in created_tables {
            let table = ct
                .old_table
                .as_ref()
                .expect("a created table has its backup table");
            let new = ct.table.as_ref().expect("a created table has its schema");
            // Lowercase names match the DDL's case-insensitive rules.
            let db_name = ct.target_db_name();
            if table.is_merge_option_allowed {
                rules.push(new_rule(
                    db_name.lowercase(),
                    new.name.lowercase(),
                    "",
                    new.id,
                ));
            }
            let (Some(old_part), Some(new_part)) = (&table.info().partition, &new.partition) else {
                continue;
            };
            let old_defs = old_part.read().definitions.snapshot();
            let new_defs = new_part.read().definitions.snapshot();
            if table.partition_merge_option_allowed.is_empty()
                || old_defs.is_empty()
                || new_defs.is_empty()
            {
                continue;
            }
            let new_by_name: HashMap<&str, i64> =
                new_defs.iter().map(|d| (d.name.original(), d.id)).collect();
            for old_def in &old_defs {
                if !table
                    .partition_merge_option_allowed
                    .get(old_def.name.original())
                    .copied()
                    .unwrap_or(false)
                {
                    continue;
                }
                let Some(&new_id) = new_by_name.get(old_def.name.original()) else {
                    tidb_log::warn(
                        "partition not found in restored table, skipping merge_option",
                        &[log_str("partition", old_def.name.original())],
                    );
                    continue;
                };
                rules.push(new_rule(
                    db_name.lowercase(),
                    new.name.lowercase(),
                    old_def.name.lowercase(),
                    new_id,
                ));
            }
        }
        let total = rules.len();
        for batch in rules.chunks(crate::utils::misc::LABEL_RULE_BATCH_SIZE) {
            tidb_log::debug(
                "putting label rules batch",
                &[
                    Field::new("batch_size", Value::U64(batch.len() as u64)),
                    Field::new("total", Value::U64(total as u64)),
                ],
            );
            dom.update_label_rules(&tidb_ddl_label::new_rule_patch(batch.to_vec(), Vec::new())).map_err(|e| {
                errors::annotate(
                    e,
                    format!("failed to put label rules (batch_size={}, total={total}) for merge_option to PD", batch.len()),
                )
            })?;
        }
        tidb_log::info(
            "successfully put all label rules",
            &[Field::new("total_rules", Value::U64(total as u64))],
        );
        Ok(())
    }

    /// Go `createTablesBatch`: `batch_ddl_size` tables per batch, ordered
    /// by schema then table ID, on the session pool.
    fn create_tables_batch(
        &self,
        ctx: &Context,
        plans: &[TableRestorePlan],
        new_ts: u64,
    ) -> Result<Vec<CreatedTable<metautil::Table>>> {
        let mut ordered = plans.to_vec();
        ordered.sort_by_key(|p| (p.source.db.id, p.source.info().id));
        let batches: Vec<&[TableRestorePlan]> =
            ordered.chunks(self.batch_ddl_size.max(1)).collect();
        let created = run_on_pool(ctx, &self.db_pool, &batches, |ectx, db, batch| {
            tidb_log::info(
                "create tables",
                &[Field::new("count", Value::U64(batch.len() as u64))],
            );
            let mut res = self.create_tables_in(ectx, db, batch, new_ts);
            if tidb_failpoint::eval(
                "github.com/pingcap/tidb/br/pkg/restore/snap_client/restore-createtables-error",
            )
            .is_some_and(|v| v.bool())
            {
                res = Err(errors::errorf("sample error without extra message"));
            }
            match res {
                Err(err) => {
                    tidb_log::error("create tables fail", &[log_str("error", &err)]);
                    Err(err)
                }
                Ok(cts) => {
                    tidb_log::info(
                        "tables created",
                        &[Field::new("num", Value::U64(cts.len() as u64))],
                    );
                    Ok(cts)
                }
            }
        })?;
        Ok(created.into_iter().flatten().collect())
    }

    /// Go `createTable`.
    fn create_table(
        &self,
        ctx: &Context,
        db: &DB,
        plan: &TableRestorePlan,
        new_ts: u64,
    ) -> Result<CreatedTable<metautil::Table>> {
        if self.is_skip_create_sql() {
            tidb_log::info(
                "skip create table and alter autoIncID",
                &[log_str("table", &plan.target_info.name)],
            );
        } else {
            db.create_table(
                ctx,
                &plan.target_table(),
                &self.rebased_tables_map,
                self.support_policy,
                self.policy_map.as_deref(),
            )?;
        }
        self.created_table(plan, new_ts)
    }

    /// Go `createTablesSingle`: one table at a time on each session.
    fn create_tables_single(
        &self,
        ctx: &Context,
        db_pool: &[Arc<DB>],
        plans: &[TableRestorePlan],
        new_ts: u64,
    ) -> Result<Vec<CreatedTable<metautil::Table>>> {
        run_on_pool(ctx, db_pool, plans, |ectx, db, plan| {
            let fields = || {
                [
                    log_str("table", &plan.target_info.name),
                    log_str("database", &plan.target_db.name),
                ]
            };
            match self.create_table(ectx, db, plan, new_ts) {
                Err(err) => {
                    let mut f = fields().to_vec();
                    f.push(log_str("error", &err));
                    tidb_log::error("create table failed", &f);
                    Err(err)
                }
                Ok(ct) => {
                    tidb_log::info("table created", &fields());
                    Ok(ct)
                }
            }
        })
    }

    /// Go `InitFullClusterRestore`.
    pub fn init_full_cluster_restore(
        &mut self,
        explicit_filter: bool,
        is_full_restore: bool,
        with_sys: bool,
    ) {
        self.full_cluster_restore =
            !explicit_filter && !self.is_incremental() && is_full_restore && with_sys;
        tidb_log::info(
            "mark full cluster restore",
            &[Field::new("value", Value::Bool(self.full_cluster_restore))],
        );
    }

    /// Go `IsFullClusterRestore`.
    #[must_use]
    pub fn is_full_cluster_restore(&self) -> bool {
        self.full_cluster_restore
    }

    /// Go `IsIncremental`.
    #[must_use]
    pub fn is_incremental(&self) -> bool {
        if tidb_failpoint::eval(
            "github.com/pingcap/tidb/br/pkg/restore/snap_client/mock-incr-backup-data",
        )
        .is_some()
        {
            return true;
        }
        !(self.backup_meta.start_version == self.backup_meta.end_version
            || self.backup_meta.start_version == 0)
    }

    /// Go `NeedCheckFreshCluster`.
    #[must_use]
    pub fn need_check_fresh_cluster(
        &self,
        explicit_filter: bool,
        checkpoint_enabled_and_exists: bool,
    ) -> bool {
        !self.is_incremental() && !explicit_filter && !checkpoint_enabled_and_exists
    }

    /// Go `EnableSkipCreateSQL`.
    pub fn enable_skip_create_sql(&mut self) {
        self.no_schema = true;
    }

    /// Go `IsSkipCreateSQL`.
    #[must_use]
    pub fn is_skip_create_sql(&self) -> bool {
        self.no_schema
    }

    /// Go `EnsureNoUserTables`.
    ///
    /// # Errors
    /// [`errors::ERR_RESTORE_NOT_FRESH_CLUSTER`].
    pub fn ensure_no_user_tables(&self) -> Result<()> {
        tidb_log::info("checking whether cluster contains user dbs and tables", &[]);
        crate::restore::misc::assert_user_dbs_empty(self.dom()?.as_ref())
    }

    /// Go `ExecDDLs`: the jobs by schema version.
    ///
    /// # Errors
    /// A job's error.
    pub fn exec_ddls(&self, ctx: &Context, ddl_jobs: &mut [Job]) -> Result<()> {
        let version = |j: &Job| {
            j.binlog_info
                .as_ref()
                .map_or(0, |b| b.read().schema_version)
        };
        ddl_jobs.sort_by_key(version);
        let db = self.db()?;
        for job in ddl_jobs.iter() {
            db.exec_ddl(ctx, job)?;
            tidb_log::info(
                "execute ddl query",
                &[
                    log_str("db", &job.schema_name),
                    log_str("query", &job.query),
                    Field::new("historySchemaVersion", Value::I64(version(job))),
                ],
            );
        }
        Ok(())
    }

    /// Go `execAndValidateChecksum`: the table's checksum, from the
    /// checkpoint or scanned (and then checkpointed), against the backup's.
    ///
    /// # Errors
    /// [`errors::ERR_RESTORE_CHECKSUM_MISMATCH`], or the scan's error.
    pub fn exec_and_validate_checksum(
        &self,
        ctx: &Context,
        tbl: &CreatedTable<metautil::Table>,
        kv_client: &dyn tidb_txnkv::Client,
        concurrency: usize,
    ) -> Result<()> {
        let old = tbl
            .old_table
            .as_ref()
            .expect("a created table has its backup table");
        let table = tbl.table.as_ref().expect("a created table has its schema");
        let ids = [
            log_str("db", old.db.name.original()),
            log_str("table", old.info().name.original()),
        ];
        let expected = old.calculate_checksum_stats_on_files();
        if !expected.checksum_exists() {
            tidb_log::warn("table has no checksum, skipping checksum", &ids);
            return Ok(());
        }
        let item = match self.checkpoint_checksum.get(&table.id) {
            Some(item) => *item,
            None => {
                tidb_log::info(
                    "did not find checksum from checkpoint, scanning table to calculate checksum",
                    &ids,
                );
                let start_ts =
                    crate::restore::misc::get_ts_with_retry(ctx, self.pd_client.as_ref())?;
                let rules = tbl.rewrite_rule.clone().unwrap_or_default();
                let exe = crate::checksum::ExecutorBuilder::new(table, start_ts)
                    .set_old_table(Some(old.info()))
                    .set_concurrency(concurrency as u64)
                    .set_old_keyspace(rules.old_keyspace)
                    .set_new_keyspace(rules.new_keyspace)
                    .set_explicit_request_source_type(crate::backup::schema::EXPLICIT_TYPE_BR)
                    .build()?;
                let resp = exe.execute(ctx, kv_client, || {})?;
                let item = ChecksumItem {
                    table_id: table.id,
                    crc64xor: resp.checksum.unwrap_or_default(),
                    total_kvs: resp.total_kvs.unwrap_or_default(),
                    total_bytes: resp.total_bytes.unwrap_or_default(),
                };
                if let Some(r) = &self.checkpoint_runner {
                    r.flush_checksum_item(ctx, item)?;
                }
                item
            }
        };
        let mut matched = item.crc64xor == expected.crc64_xor
            && item.total_kvs == expected.total_kvs
            && item.total_bytes == expected.total_bytes;
        if tidb_failpoint::eval(
            "github.com/pingcap/tidb/br/pkg/restore/snap_client/full-restore-validate-checksum",
        )
        .is_some()
        {
            matched = false;
        }
        if !matched {
            tidb_log::error(
                "failed in validate checksum",
                &[
                    Field::new("expected tidb crc64", Value::U64(expected.crc64_xor)),
                    Field::new("calculated crc64", Value::U64(item.crc64xor)),
                    Field::new("expected tidb total kvs", Value::U64(expected.total_kvs)),
                    Field::new("calculated total kvs", Value::U64(item.total_kvs)),
                    Field::new(
                        "expected tidb total bytes",
                        Value::U64(expected.total_bytes),
                    ),
                    Field::new("calculated total bytes", Value::U64(item.total_bytes)),
                    Field::new("table_id", Value::I64(table.id)),
                    log_str("table_info", &table.name),
                ],
            );
            return Err(errors::annotatef(
                &errors::ERR_RESTORE_CHECKSUM_MISMATCH,
                format!(
                    "checksum mismatch for table '{}.{}' (ID: {}): crc64xor (expected: {}, actual: {}), totalKvs (expected: {}, actual: {}), totalBytes (expected: {}, actual: {})",
                    old.db.name.original(),
                    old.info().name.original(),
                    table.id,
                    expected.crc64_xor,
                    item.crc64xor,
                    expected.total_kvs,
                    item.total_kvs,
                    expected.total_bytes,
                    item.total_bytes
                ),
            ));
        }
        tidb_log::info("success in validating checksum", &ids);
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests;
