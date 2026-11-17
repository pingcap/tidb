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

//! Go `br/pkg/restore/snap_client/client.go`: the parts of `SnapClient`
//! that set up and run the SST import (`NewRestoreClient`, the concurrency
//! setters, `initClients`, the speed limit callbacks, `GetRestorer`,
//! `Close`). The schema side comes with the restore task.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::backup::{CipherInfo, StorageBackend};
use tidb_proto::kvrpcpb;
use tikv_client::pd_http;

use super::import::{
    ImporterCallback, KvMode, RewriteMode, SnapFileImporter, SnapFileImporterOptions,
};
use crate::checkpoint::restore::RestoreRunner;
use crate::conn::{get_all_tikv_stores_with_retry, StoreBehavior};
use crate::errors::{self, Result};
use crate::pdclient::PdClient;
use crate::restore::import_client::ImportClient;
use crate::restore::restorer::{MultiTablesRestorer, SstRestorer};
use crate::restore::split::{self, new_client, SplitClient};
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
    cipher: Option<CipherInfo>,
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

    /// Go `Close`: the restorer closed (a failure is only logged).
    pub fn close(&self) {
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
