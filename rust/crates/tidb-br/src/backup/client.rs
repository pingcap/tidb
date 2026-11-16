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

//! Go `br/pkg/backup/client.go`: the backup client. It picks the backup
//! TS and storage, builds the ranges and schemas to back up from the
//! catalog at that TS, and pushes the backup down to every store in rounds
//! until no range is left, resolving the locks TiKV meets in between.
//!
//! The repo layout's per-store adapters (`PerStoreBackupAdapter`) are not
//! reachable from SQL: the request and response files are used as they
//! are.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, select, unbounded, Receiver, Sender};
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_model::{DBInfo, Job, TableInfo};
use tidb_proto::backup::{BackupRequest, CipherInfo, PlacementPolicy, StorageBackend};
use tidb_proto::kvrpcpb;

use super::limit::ResourceConcurrentLimiter;
use super::schema::Schemas;
use super::store::{
    observe_store_changes_async, BackupClient, BackupRetryPolicy, BackupSender, MainBackupSender,
    ResponseAndStore,
};
use crate::checkpoint::backup::{self as cpbackup, BackupRunner, CheckpointMetadataForBackup};
use crate::conn::{get_all_tikv_stores_with_retry, StoreBehavior};
use crate::errors::{self, Result, ERR_INVALID_ARGUMENT, ERR_KV_STORAGE};
use crate::gc;
use crate::glue::{Glue, Storage};
use crate::metautil::{self, MetaItem, MetaWriter};
use crate::pdclient::{compose_ts, extract_physical, PdClient};
use crate::rtree::{KeyRange, ProgressRange, ProgressRangeTree, RangeTree};
use crate::utils::error_handling::{handle_backup_error, ErrorContext, ErrorHandlingStrategy};
use crate::utils::misc::{check_store_liveness, is_template_sys_db, need_auto_id};

/// Go `MaxResolveLocksbackupOffSleepMs`.
pub const MAX_RESOLVE_LOCKS_BACKOFF_SLEEP_MS: u64 = 600_000;
/// Go `IncompleteRangesUpdateInterval`.
pub const INCOMPLETE_RANGES_UPDATE_INTERVAL: Duration = Duration::from_secs(15);
/// Go `RangesSentThreshold`.
pub const RANGES_SENT_THRESHOLD: i64 = 30_000_000;

/// Go `ClientMgr`.
pub trait ClientMgr: Send + Sync {
    /// Go `GetBackupClient`.
    ///
    /// # Errors
    /// The connection error.
    fn get_backup_client(&self, ctx: &Context, store_id: u64) -> Result<BackupClient>;
    /// Go `ResetBackupClient`.
    ///
    /// # Errors
    /// The connection error.
    fn reset_backup_client(&self, ctx: &Context, store_id: u64) -> Result<BackupClient>;
    /// Go `GetPDClient`.
    fn get_pd_client(&self) -> Arc<dyn PdClient>;
    /// Go `GetStorage`.
    fn get_storage(&self) -> Arc<dyn Storage>;
    /// Go `GetGCManager`.
    fn get_gc_manager(&self) -> Arc<dyn gc::Manager>;
    /// Go `GetLockResolver().ResolveLocksForRead(bo, ts, locks, true)`: the
    /// start TSs of locks a read at `ts` may ignore, and the commit TSs it
    /// may access.
    ///
    /// # Errors
    /// The resolve error.
    fn resolve_locks_for_read(
        &self,
        ctx: &Context,
        ts: u64,
        locks: Vec<kvrpcpb::LockInfo>,
    ) -> Result<(Vec<u64>, Vec<u64>)>;
    /// The runtime the backup RPCs run on.
    fn runtime(&self) -> Arc<tokio::runtime::Runtime>;
}

impl ClientMgr for crate::conn::Mgr {
    fn get_backup_client(&self, ctx: &Context, store_id: u64) -> Result<BackupClient> {
        crate::conn::Mgr::get_backup_client(self, ctx, store_id)
    }

    fn reset_backup_client(&self, ctx: &Context, store_id: u64) -> Result<BackupClient> {
        self.store_manager.reset_backup_client(ctx, store_id)
    }

    fn get_pd_client(&self) -> Arc<dyn PdClient> {
        Arc::clone(crate::conn::Mgr::get_pd_client(self))
    }

    fn get_storage(&self) -> Arc<dyn Storage> {
        Arc::clone(crate::conn::Mgr::get_storage(self))
    }

    fn get_gc_manager(&self) -> Arc<dyn gc::Manager> {
        Arc::clone(crate::conn::Mgr::get_gc_manager(self))
    }

    fn resolve_locks_for_read(
        &self,
        _ctx: &Context,
        ts: u64,
        locks: Vec<kvrpcpb::LockInfo>,
    ) -> Result<(Vec<u64>, Vec<u64>)> {
        let storage = crate::conn::Mgr::get_storage(self);
        let tikv = storage.tikv().clone();
        let opts = tikv_client::txnkv::txnlock::ResolveLocksOptions {
            caller_start_ts: ts,
            locks,
            lite: true,
            for_read: true,
            ..Default::default()
        };
        let backoffer = Arc::new(tokio::sync::Mutex::new(
            tikv_client::retry::RetryBackoffer::new(
                Default::default(),
                MAX_RESOLVE_LOCKS_BACKOFF_SLEEP_MS,
            ),
        ));
        let store = tikv.clone();
        let res = tikv
            .block_on(async move {
                store
                    .store()
                    .resolve_locks_with_opts(&kvrpcpb::Context::default(), backoffer, opts)
                    .await
            })
            .map_err(crate::pdclient::from_client_error)?;
        Ok((res.ignore_locks, res.access_locks))
    }

    fn runtime(&self) -> Arc<tokio::runtime::Runtime> {
        Arc::clone(crate::conn::Mgr::get_storage(self).tikv().runtime())
    }
}

/// Go `ProgressUnit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgressUnit {
    /// Go `UnitRange`.
    Range,
    /// Go `UnitRegion`.
    Region,
}

impl ProgressUnit {
    /// Go's string value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::Region => "region",
        }
    }
}

/// Go `Client`.
pub struct Client {
    mgr: Arc<dyn ClientMgr>,
    cluster_id: u64,
    storage: Option<metautil::Storage>,
    backend: Option<StorageBackend>,
    api_version: Mutex<i32>,
    cipher: Option<CipherInfo>,
    checkpoint_meta: Option<CheckpointMetadataForBackup>,
    checkpoint_runner: Option<BackupRunner>,
    gc_ttl: i64,
    table_range: bool,
    skip_checksum: bool,
}

/// Go `GetBackupClientCallBack`: the store's backup client, reset when
/// asked.
pub type BackupClientCallBack =
    Arc<dyn Fn(&Context, u64, bool) -> Result<BackupClient> + Send + Sync>;

/// Go `MainBackupLoop`.
pub struct MainBackupLoop {
    /// Go's embedded `BackupSender`.
    pub sender: Arc<dyn BackupSender>,
    /// Go `BackupReq`.
    pub backup_req: BackupRequest,
    /// Go `Concurrency`.
    pub concurrency: usize,
    /// Go `GlobalProgressTree`. It stays on the loop's thread.
    pub global_progress_tree: Mutex<ProgressRangeTree>,
    /// Go `ReplicaReadLabel`.
    pub replica_read_label: BTreeMap<String, String>,
    /// Go `StateNotifier`.
    pub state_notifier: (Sender<BackupRetryPolicy>, Receiver<BackupRetryPolicy>),
    /// Go `Limiter`.
    pub limiter: Arc<ResourceConcurrentLimiter>,
    /// Go `ProgressCallBack`.
    pub progress_call_back: Arc<dyn Fn(ProgressUnit) + Send + Sync>,
    /// Go `GetBackupClientCallBack`.
    pub get_backup_client_call_back: BackupClientCallBack,
}

impl Client {
    /// Go `NewBackupClient`.
    #[must_use]
    pub fn new(ctx: &Context, mgr: Arc<dyn ClientMgr>) -> Self {
        tidb_log::info("new backup client", &[]);
        let cluster_id = mgr.get_pd_client().get_cluster_id(ctx);
        Self {
            mgr,
            cluster_id,
            storage: None,
            backend: None,
            api_version: Mutex::new(0),
            cipher: None,
            checkpoint_meta: None,
            checkpoint_runner: None,
            gc_ttl: 0,
            table_range: false,
            skip_checksum: false,
        }
    }

    /// Go `NewTableBackupClient`.
    #[must_use]
    pub fn new_table_backup_client(ctx: &Context, mgr: Arc<dyn ClientMgr>) -> Self {
        let mut c = Self::new(ctx, mgr);
        c.table_range = true;
        c
    }

    /// Go `SetCipher`.
    pub fn set_cipher(&mut self, cipher: &CipherInfo) {
        self.cipher = Some(cipher.clone());
    }

    /// Go `SetSkipChecksum`.
    pub fn set_skip_checksum(&mut self, skip: bool) {
        self.skip_checksum = skip;
    }

    /// Go `GetCurrentTS`.
    ///
    /// # Errors
    /// PD's error.
    pub fn get_current_ts(&self, ctx: &Context) -> Result<u64> {
        let (p, l) = self.mgr.get_pd_client().get_ts(ctx)?;
        Ok(compose_ts(p, l))
    }

    /// Go `GetTS`: the checkpoint's backup TS when resuming, else `ts`
    /// (not later than now) or now minus `duration`; it must be ahead of
    /// PD's GC safe point.
    ///
    /// # Errors
    /// A TS in the future, a negative or overflowing `duration`, PD's
    /// error, or the GC safe point past the TS.
    pub fn get_ts(&self, ctx: &Context, duration: i64, ts: u64) -> Result<u64> {
        if let Some(meta) = &self.checkpoint_meta {
            tidb_log::info(
                "reuse checkpoint BackupTS",
                &[Field::new("backup-ts", Value::U64(meta.backup_ts))],
            );
            return Ok(meta.backup_ts);
        }
        let backup_ts = if ts > 0 {
            let current = self.get_current_ts(ctx)?;
            if ts > current {
                return Err(errors::annotatef(
                    &ERR_INVALID_ARGUMENT,
                    format!(
                        "backup timestamp {ts}({}) must not be later than current timestamp {current}({})",
                        go_time_from_ts(ts),
                        go_time_from_ts(current)
                    ),
                ));
            }
            ts
        } else {
            let (p, l) = self.mgr.get_pd_client().get_ts(ctx)?;
            let mut backup_ts = compose_ts(p, l);
            if duration < 0 {
                return Err(errors::annotate(
                    errors::new(&ERR_INVALID_ARGUMENT),
                    "negative timeago is not allowed",
                ));
            }
            if duration > 0 {
                tidb_log::info(
                    "backup time ago",
                    &[Field::new("timeago", Value::Duration(duration))],
                );
                let ago_ms = extract_physical(backup_ts) - duration / 1_000_000;
                let ago_ts = compose_ts(ago_ms, l);
                if backup_ts < ago_ts {
                    return Err(errors::annotate(
                        errors::new(&ERR_INVALID_ARGUMENT),
                        "backup ts overflow please choose a smaller timeago",
                    ));
                }
                backup_ts = ago_ts;
            }
            backup_ts
        };
        gc::check_gc_safe_point(ctx, self.mgr.get_gc_manager().as_ref(), backup_ts)?;
        tidb_log::info(
            "backup encode timestamp",
            &[Field::new("BackupTS", Value::U64(backup_ts))],
        );
        Ok(backup_ts)
    }

    /// Go `SetLockFile`.
    ///
    /// # Errors
    /// The write error.
    pub fn set_lock_file(&self, ctx: &Context) -> Result<()> {
        self.get_storage()
            .write_file(
                ctx,
                metautil::LOCK_FILE,
                b"DO NOT DELETE\nThis file exists to remind other backup jobs won't use this path",
            )
            .map_err(|e| errors::errorf(e.to_string()))
    }

    /// Go `GetSafePointID`: the checkpoint's service ID when resuming.
    #[must_use]
    pub fn get_safe_point_id(&self) -> String {
        if let Some(meta) = &self.checkpoint_meta {
            tidb_log::info(
                "reuse the checkpoint gc-safepoint service id",
                &[Field::new(
                    "service-id",
                    Value::Str(meta.gc_service_id.clone()),
                )],
            );
            return meta.gc_service_id.clone();
        }
        gc::make_safe_point_id()
    }

    /// Go `SetGCTTL`: a non-positive TTL is the default.
    pub fn set_gc_ttl(&mut self, ttl: i64) {
        self.gc_ttl = if ttl <= 0 {
            gc::DEFAULT_BR_GC_SAFE_POINT_TTL
        } else {
            ttl
        };
    }

    /// Go `GetGCTTL`.
    #[must_use]
    pub fn get_gc_ttl(&self) -> i64 {
        self.gc_ttl
    }

    /// Go `GetStorageBackend`.
    #[must_use]
    pub fn get_storage_backend(&self) -> Option<&StorageBackend> {
        self.backend.as_ref()
    }

    /// Go `GetStorage`.
    ///
    /// # Panics
    /// The storage is not set.
    #[must_use]
    pub fn get_storage(&self) -> &metautil::Storage {
        self.storage.as_ref().expect("the backup storage is set")
    }

    /// Go `SetStorage`.
    ///
    /// # Errors
    /// The storage cannot be created.
    pub fn set_storage(
        &mut self,
        ctx: &Context,
        backend: StorageBackend,
        opts: &tidb_objstore::storeapi::Options,
    ) -> Result<()> {
        let storage = tidb_objstore::new(ctx, &backend, Some(opts))
            .map_err(|e| errors::errorf(e.to_string()))?;
        self.backend = Some(backend);
        self.storage = Some(storage);
        Ok(())
    }

    /// Go `SetStorageAndCheckNotInUse`: refuses a storage that holds a
    /// backup, loads its checkpoint when there is one, and otherwise refuses
    /// one locked by another backup.
    ///
    /// # Errors
    /// The storage is in use, or a storage error.
    pub fn set_storage_and_check_not_in_use(
        &mut self,
        ctx: &Context,
        backend: StorageBackend,
        opts: &tidb_objstore::storeapi::Options,
    ) -> Result<()> {
        self.set_storage(ctx, backend, opts)?;
        let s = self.get_storage().clone();
        let exist = s.file_exists(ctx, metautil::META_FILE).map_err(|e| {
            errors::annotate(
                errors::errorf(e.to_string()),
                format!("error occurred when checking {} file", metautil::META_FILE),
            )
        })?;
        if exist {
            return Err(errors::annotatef(
                &ERR_INVALID_ARGUMENT,
                format!(
                    "backup meta file exists in {}/{}, there may be some backup files in the path already, please specify a correct backup directory!",
                    s.uri(),
                    metautil::META_FILE
                ),
            ));
        }
        let path = cpbackup::CHECKPOINT_META_PATH_FOR_BACKUP;
        let exist = s.file_exists(ctx, path).map_err(|e| {
            errors::annotate(
                errors::errorf(e.to_string()),
                format!("error occurred when checking {path} file"),
            )
        })?;
        if exist {
            tidb_log::info(
                "load the checkpoint meta, so the existence of lockfile is allowed.",
                &[],
            );
            self.checkpoint_meta =
                Some(cpbackup::load_checkpoint_metadata(ctx, &s).map_err(|e| {
                    errors::annotate(e, format!("error occurred when loading {path} file"))
                })?);
        } else {
            check_backup_storage_is_locked(ctx, &s)?;
        }
        Ok(())
    }

    /// Go `CheckCheckpoint`: a checkpoint of another configuration refuses
    /// the backup.
    ///
    /// # Errors
    /// [`ERR_INVALID_ARGUMENT`].
    pub fn check_checkpoint(&self, hash: &[u8]) -> Result<()> {
        if let Some(meta) = &self.checkpoint_meta {
            if meta.config_hash != hash {
                return Err(errors::annotatef(
                    &ERR_INVALID_ARGUMENT,
                    format!(
                        "failed to backup to {}, because the checkpoint mode is used, but the hashs of the configs are not the same. Please check the config",
                        self.get_storage().uri()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Go `GetCheckpointRunner`.
    #[must_use]
    pub fn get_checkpoint_runner(&self) -> Option<&BackupRunner> {
        self.checkpoint_runner.as_ref()
    }

    /// Go `GetCheckpointMetadata`.
    #[must_use]
    pub fn get_checkpoint_metadata(&self) -> Option<&CheckpointMetadataForBackup> {
        self.checkpoint_meta.as_ref()
    }

    /// Go `StartCheckpointRunner`: writes the checkpoint metadata of a new
    /// backup (a resumed one reloads its ranges instead).
    ///
    /// # Errors
    /// A storage error, or the checkpoint lock is held.
    pub fn start_checkpoint_runner(
        &mut self,
        ctx: &Context,
        cfg_hash: &[u8],
        backup_ts: u64,
        backup_id: u64,
        safe_point_id: &str,
    ) -> Result<()> {
        match &mut self.checkpoint_meta {
            None => {
                let meta = CheckpointMetadataForBackup {
                    gc_service_id: safe_point_id.to_owned(),
                    config_hash: cfg_hash.to_vec(),
                    backup_ts,
                    backup_id,
                    ..Default::default()
                };
                cpbackup::save_checkpoint_metadata(ctx, self.storage.as_ref().unwrap(), &meta)?;
            }
            Some(meta) => meta.load_checkpoint_data_map = true,
        }
        let timer: Arc<dyn crate::checkpoint::GlobalTimer> =
            Arc::new(PdTimer(self.mgr.get_pd_client()));
        self.checkpoint_runner = Some(cpbackup::start_checkpoint_runner_for_backup(
            ctx,
            self.get_storage().clone(),
            self.cipher.clone(),
            timer,
        )?);
        Ok(())
    }

    /// Go `WaitForFinishCheckpoint`.
    pub fn wait_for_finish_checkpoint(&self, ctx: &Context, flush: bool) {
        if let Some(r) = &self.checkpoint_runner {
            r.wait_for_finish(ctx, flush);
        }
    }

    /// Go `getProgressRange`.
    fn get_progress_range(&self, r: &KeyRange) -> ProgressRange {
        let physical_id = if self.table_range {
            tidb_tablecodec::table_key::decode_table_id(&r.start_key)
        } else {
            0
        };
        ProgressRange {
            res: RangeTree::new_range_tree_with_free_list_g(physical_id),
            origin: r.clone(),
        }
    }

    /// Go `GetClusterID`.
    #[must_use]
    pub fn get_cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Go `GetApiVersion`.
    #[must_use]
    pub fn get_api_version(&self) -> i32 {
        *self.api_version.lock().unwrap()
    }

    /// Go `SetApiVersion`.
    pub fn set_api_version(&self, v: i32) {
        *self.api_version.lock().unwrap() = v;
    }

    /// Go `BuildBackupRangeAndSchema`: a resumed backup reuses the
    /// checkpoint's table checksums.
    ///
    /// # Errors
    /// A catalog read error, or a table too new to back up.
    pub fn build_backup_range_and_schema(
        &self,
        storage: &Arc<dyn Storage>,
        table_filter: Arc<dyn tidb_table_filter::Filter>,
        backup_ts: u64,
        is_full_backup: bool,
    ) -> Result<(Vec<KeyRange>, Option<Schemas>, Vec<PlacementPolicy>)> {
        let (ranges, mut schemas, policies) =
            build_backup_range_and_init_schema(storage, table_filter, backup_ts, is_full_backup)?;
        if let (Some(meta), Some(schemas)) = (&self.checkpoint_meta, schemas.as_mut()) {
            schemas.set_checkpoint_checksum(meta.checkpoint_checksum.clone());
        }
        Ok((ranges, schemas, policies))
    }

    /// Go `BuildProgressRangeTree`: the ranges as a progress tree, with a
    /// resumed backup's finished ranges and their files already in it.
    ///
    /// # Errors
    /// Overlapping ranges, or a checkpoint read error.
    pub fn build_progress_range_tree(
        &self,
        ctx: &Context,
        ranges: &[KeyRange],
        meta_writer: Option<Arc<MetaWriter>>,
        progress: Arc<dyn Fn(ProgressUnit) + Send + Sync>,
    ) -> Result<ProgressRangeTree> {
        let sink = meta_writer
            .as_ref()
            .map(|w| Box::new(MetaSink(Arc::clone(w))) as Box<dyn crate::rtree::MetaWriter>);
        let mut tree = ProgressRangeTree::new(sink, self.skip_checksum);
        for r in ranges {
            tree.insert(Arc::new(Mutex::new(self.get_progress_range(r))))
                .map_err(|e| errors::errorf(e.to_string()))?;
        }
        let p = Arc::clone(&progress);
        tree.set_call_back(move || p(ProgressUnit::Range));
        if self
            .checkpoint_meta
            .as_ref()
            .is_some_and(|m| m.load_checkpoint_data_map)
        {
            let skip_checksum = self.skip_checksum;
            let past = cpbackup::walk_checkpoint_file_for_backup(
                ctx,
                self.get_storage(),
                self.cipher.as_ref(),
                &mut |_, rg| {
                    let pr = tree
                        .find_contained(&rg.start_key, &rg.end_key)
                        .map_err(|e| errors::errorf(e.to_string()))?;
                    if let Some(pr) = pr {
                        let files: Vec<Arc<tidb_proto::backup::File>> =
                            rg.files.iter().cloned().map(Arc::new).collect();
                        let put = pr.lock().unwrap().res.put_force(
                            &rg.start_key,
                            &rg.end_key,
                            files,
                            false,
                        );
                        if put {
                            let (crc, kvs, bytes) = crate::utils::misc::summary_files(&rg.files);
                            if let Some(w) = &meta_writer {
                                w.send(MetaItem::DataFile(rg.files.clone()))
                                    .map_err(errors::errorf)?;
                            }
                            if !skip_checksum {
                                let id = pr.lock().unwrap().res.physical_id;
                                tree.update_checksum(id, crc, kvs, bytes);
                            }
                            progress(ProgressUnit::Region);
                        }
                    }
                    Ok(())
                },
            )?;
            tidb_log::info(
                "past cost time",
                &[Field::new("cost", Value::Duration(past.as_nanos() as i64))],
            );
            crate::summary::adjust_start_time_to_earlier_time(past);
        }
        Ok(tree)
    }

    /// Go `BackupRanges`: backs up the ranges and answers the checksum of
    /// each physical table from the backed-up files.
    ///
    /// # Errors
    /// A backup error, or ranges left after the loop.
    #[allow(clippy::too_many_arguments)]
    pub fn backup_ranges(
        &self,
        ctx: &Context,
        ranges: &[KeyRange],
        request: BackupRequest,
        concurrency: usize,
        range_limit: i64,
        replica_read_label: BTreeMap<String, String>,
        meta_writer: Arc<MetaWriter>,
        progress: Arc<dyn Fn(ProgressUnit) + Send + Sync>,
    ) -> Result<BTreeMap<i64, metautil::ChecksumStats>> {
        tidb_log::info(
            "Backup Ranges Started",
            &[Field::new("ranges", Value::I64(ranges.len() as i64))],
        );
        let init = Instant::now();
        let tree =
            self.build_progress_range_tree(ctx, ranges, Some(meta_writer), Arc::clone(&progress))?;
        let notifier = bounded::<BackupRetryPolicy>(0);
        observe_store_changes_async(ctx, notifier.0.clone(), self.mgr.get_pd_client());
        let mgr = Arc::clone(&self.mgr);
        let mut main_loop = MainBackupLoop {
            sender: Arc::new(MainBackupSender {
                runtime: self.mgr.runtime(),
            }),
            backup_req: request,
            concurrency,
            global_progress_tree: Mutex::new(tree),
            replica_read_label,
            state_notifier: notifier,
            limiter: Arc::new(ResourceConcurrentLimiter::new(range_limit)),
            progress_call_back: progress,
            get_backup_client_call_back: Arc::new(move |ctx, store_id, reset| {
                if reset {
                    mgr.reset_backup_client(ctx, store_id)
                } else {
                    mgr.get_backup_client(ctx, store_id)
                }
            }),
        };
        let res = self.run_loop(ctx, &mut main_loop);
        tidb_log::info(
            "Backup Ranges Completed",
            &[Field::new(
                "take",
                Value::Duration(init.elapsed().as_nanos() as i64),
            )],
        );
        res?;
        let tree = main_loop.global_progress_tree.into_inner().unwrap();
        if !tree.is_empty() {
            tidb_log::error(
                "backup ranges done but some ranges are in complete",
                &[Field::new("count", Value::I64(tree.len() as i64))],
            );
            return Err(errors::errorf(
                "backup ranges done but some ranges are in complete",
            ));
        }
        Ok(tree
            .get_checksum_map()
            .iter()
            .map(|(k, v)| {
                (
                    *k,
                    metautil::ChecksumStats {
                        crc64_xor: v.crc64_xor,
                        total_kvs: v.total_kvs,
                        total_bytes: v.total_bytes,
                    },
                )
            })
            .collect())
    }

    /// Go `getBackupStores`: every TiKV store, or the stores matching the
    /// replica read labels.
    fn get_backup_stores(
        &self,
        ctx: &Context,
        labels: &BTreeMap<String, String>,
    ) -> Result<Vec<tidb_proto::metapb::Store>> {
        let all = get_all_tikv_stores_with_retry(
            ctx,
            self.mgr.get_pd_client().as_ref(),
            StoreBehavior::SkipTiFlash,
        )?;
        if labels.is_empty() {
            return Ok(all);
        }
        let mut target = Vec::new();
        for store in all {
            for label in &store.labels {
                if labels.get(&label.key) == Some(&label.value) {
                    target.push(store.clone());
                }
            }
        }
        if target.is_empty() {
            return Err(errors::errorf(format!(
                "no store matches replica read label: map{}",
                go_map_text(labels)
            )));
        }
        Ok(target)
    }

    /// Go `RunLoop`: rounds of push-down backup. Each round sends the
    /// incomplete ranges to every live store and handles the responses;
    /// locks TiKV met are resolved once the round's stores are done, and a
    /// store change restarts the round or the store.
    ///
    /// # Errors
    /// `ctx` done, a progress tree error, or a response TiKV gives up on.
    pub fn run_loop(&self, ctx: &Context, l: &mut MainBackupLoop) -> Result<()> {
        let mut round = 0_u64;
        let mut reset = true;
        'main: loop {
            round += 1;
            std::thread::sleep(Duration::from_millis(200));
            tidb_log::info(
                "This round of backup starts...",
                &[Field::new("round", Value::U64(round))],
            );
            let err_context = ErrorContext::new("MainBackupLoop", 10);
            let (main_ctx, main_cancel) = Context::with_cancel(ctx);
            let start = Instant::now();
            let mut all_txn_locks: Vec<kvrpcpb::LockInfo> = Vec::new();
            if let Some(err) = ctx.err() {
                main_cancel.cancel();
                return Err(Arc::new(err));
            }
            l.backup_req.sub_ranges = l
                .global_progress_tree
                .lock()
                .unwrap()
                .get_incomplete_ranges()
                .map_err(|e| {
                    main_cancel.cancel();
                    errors::errorf(e.to_string())
                })?;
            if l.backup_req.sub_ranges.is_empty() {
                tidb_log::info(
                    "This round finished all backup ranges",
                    &[Field::new("round", Value::U64(round))],
                );
                main_cancel.cancel();
                return Ok(());
            }
            tidb_log::info(
                "backup ranges",
                &[
                    Field::new("round", Value::U64(round)),
                    Field::new(
                        "incomplete-ranges",
                        Value::I64(l.backup_req.sub_ranges.len() as i64),
                    ),
                    Field::new("cost", Value::Duration(start.elapsed().as_nanos() as i64)),
                ],
            );
            let all_stores = match self.get_backup_stores(&main_ctx, &l.replica_read_label) {
                Ok(s) => s,
                Err(err) => {
                    tidb_log::error(
                        "failed to get backup stores",
                        &[
                            Field::new("round", Value::U64(round)),
                            Field::new("error", Value::Str(err.to_string())),
                        ],
                    );
                    main_cancel.cancel();
                    reset = true;
                    continue 'main;
                }
            };
            let (global_tx, global_rx) = unbounded::<ResponseAndStore>();
            let mut store_count = 0;
            for store in &all_stores {
                if let Err(err) = check_store_liveness(store) {
                    tidb_log::warn(
                        "store not alive, skip backup it in this round",
                        &[
                            Field::new("round", Value::U64(round)),
                            Field::new("error", Value::Str(err.to_string())),
                        ],
                    );
                    continue;
                }
                let cli = match (l.get_backup_client_call_back)(&main_ctx, store.id, reset) {
                    Ok(c) => c,
                    Err(err) => {
                        tidb_log::error(
                            "failed to reset backup client",
                            &[
                                Field::new("round", Value::U64(round)),
                                Field::new("storeID", Value::U64(store.id)),
                                Field::new("error", Value::Str(err.to_string())),
                            ],
                        );
                        main_cancel.cancel();
                        reset = true;
                        continue 'main;
                    }
                };
                store_count += 1;
                l.sender.send_async(
                    &main_ctx,
                    round,
                    store.id,
                    Arc::clone(&l.limiter),
                    l.backup_req.clone(),
                    l.concurrency,
                    cli,
                    global_tx.clone(),
                    l.state_notifier.0.clone(),
                );
            }
            drop(global_tx);
            tidb_log::info(
                "start wait store backups",
                &[Field::new("remainingProducers", Value::I64(store_count))],
            );
            let mut ticker = crossbeam_channel::after(INCOMPLETE_RANGES_UPDATE_INTERVAL);
            let mut global_rx = global_rx;
            loop {
                select! {
                    recv(ctx.done()) -> _ => {
                        main_cancel.cancel();
                        return Err(Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled)));
                    }
                    recv(ticker) -> _ => {
                        let start_update = Instant::now();
                        l.backup_req.sub_ranges = l.global_progress_tree.lock().unwrap().get_incomplete_ranges().map_err(|e| {
                            main_cancel.cancel();
                            errors::errorf(e.to_string())
                        })?;
                        let elapsed = start_update.elapsed();
                        tidb_log::debug(
                            "update the incomplete ranges",
                            &[Field::new("incomplete-ranges", Value::I64(l.backup_req.sub_ranges.len() as i64)), Field::new("take", Value::Duration(elapsed.as_nanos() as i64))],
                        );
                        ticker = crossbeam_channel::after((elapsed * 5).max(INCOMPLETE_RANGES_UPDATE_INTERVAL));
                    }
                    recv(l.state_notifier.1) -> info => {
                        let Ok(info) = info else { continue };
                        if info.all {
                            tidb_log::info("cluster state changed. restart store backups", &[Field::new("round", Value::U64(round))]);
                            main_cancel.cancel();
                            reset = true;
                            continue 'main;
                        }
                        if info.one != 0 {
                            let store_id = info.one;
                            tidb_log::info("receive notifaction and retry backup on this store", &[Field::new("storeID", Value::U64(store_id)), Field::new("round", Value::U64(round))]);
                            let store = match self.mgr.get_pd_client().get_store(&main_ctx, store_id) {
                                Ok(s) => s,
                                Err(err) => {
                                    tidb_log::info("cannot get store from pd", &[Field::new("round", Value::U64(round)), Field::new("error", Value::Str(err.to_string()))]);
                                    main_cancel.cancel();
                                    reset = true;
                                    continue 'main;
                                }
                            };
                            if let Err(err) = check_store_liveness(&store) {
                                tidb_log::warn("store not alive, skip backup it in this round", &[Field::new("round", Value::U64(round)), Field::new("error", Value::Str(err.to_string()))]);
                                reset = true;
                                continue 'main;
                            }
                            let cli = match (l.get_backup_client_call_back)(&main_ctx, store_id, reset) {
                                Ok(c) => c,
                                Err(err) => {
                                    tidb_log::error("failed to reset backup client", &[Field::new("round", Value::U64(round)), Field::new("storeID", Value::U64(store_id)), Field::new("error", Value::Str(err.to_string()))]);
                                    main_cancel.cancel();
                                    reset = true;
                                    continue 'main;
                                }
                            };
                            // Go restarts the collector over every store's
                            // channel; here the store's responses join the
                            // round's channel.
                            let (tx, rx) = unbounded::<ResponseAndStore>();
                            let old_rx = std::mem::replace(&mut global_rx, rx);
                            let fwd = tx.clone();
                            std::thread::spawn(move || for r in old_rx { if fwd.send(r).is_err() { return; } });
                            l.sender.send_async(&main_ctx, round, store_id, Arc::clone(&l.limiter), l.backup_req.clone(), l.concurrency, cli, tx, l.state_notifier.0.clone());
                        }
                    }
                    recv(global_rx) -> r => match r {
                        Err(_) => {
                            tidb_log::info("all store backup goroutines exited", &[Field::new("round", Value::U64(round)), Field::new("store-count", Value::I64(store_count))]);
                            if !all_txn_locks.is_empty() {
                                match self.mgr.resolve_locks_for_read(&main_ctx, l.backup_req.end_version, std::mem::take(&mut all_txn_locks)) {
                                    Err(err) => tidb_log::warn(
                                        "failed to resolve locks, ignore and wait for next round to resolve",
                                        &[Field::new("round", Value::U64(round)), Field::new("error", Value::Str(err.to_string()))],
                                    ),
                                    Ok((ignore, access)) => {
                                        // The context is nil in raw and txn backup.
                                        if let Some(c) = l.backup_req.context.as_mut() {
                                            c.resolved_locks.extend(ignore);
                                            c.committed_locks.extend(access);
                                        }
                                    }
                                }
                                reset = false;
                            }
                            main_cancel.cancel();
                            continue 'main;
                        }
                        Ok(resp) => {
                            match self.on_backup_response(&main_ctx, resp, &err_context, &l.global_progress_tree) {
                                Err(err) => {
                                    main_cancel.cancel();
                                    return Err(err);
                                }
                                Ok(Some(lock)) => all_txn_locks.push(lock),
                                Ok(None) => {}
                            }
                            (l.progress_call_back)(ProgressUnit::Region);
                        }
                    }
                }
            }
        }
    }

    /// Go `OnBackupResponse`: puts the response's files in its range, or
    /// answers the lock TiKV met; a response error TiKV gives up on fails
    /// the backup.
    ///
    /// # Errors
    /// The response's range is outside every range, or an error TiKV gives
    /// up on.
    pub fn on_backup_response(
        &self,
        ctx: &Context,
        r: ResponseAndStore,
        err_context: &ErrorContext,
        tree: &Mutex<ProgressRangeTree>,
    ) -> Result<Option<kvrpcpb::LockInfo>> {
        let resp = r.resp;
        let store_id = r.store_id;
        match &resp.error {
            None => {
                let start = Instant::now();
                let pr = tree
                    .lock()
                    .unwrap()
                    .find_contained(&resp.start_key, &resp.end_key);
                tidb_log::debug(
                    "find the range tree contains response ranges",
                    &[Field::new(
                        "take",
                        Value::Duration(start.elapsed().as_nanos() as i64),
                    )],
                );
                let pr = pr.map_err(|e| {
                    tidb_log::error(
                        "failed to update the backup response",
                        &[Field::new("error", Value::Str(e.to_string()))],
                    );
                    errors::errorf(e.to_string())
                })?;
                if let Some(pr) = pr {
                    if let Some(runner) = &self.checkpoint_runner {
                        cpbackup::append_for_backup(
                            ctx,
                            runner,
                            &resp.start_key,
                            &resp.end_key,
                            &resp.files,
                        )
                        .inspect_err(|e| {
                            tidb_log::error(
                                "failed to flush checkpoint",
                                &[Field::new("error", Value::Str(e.to_string()))],
                            );
                        })?;
                        if let Some(v) = tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/backup/backup-response-error-after-checkpoint") {
                            let msg = v.string();
                            let msg = if msg.is_empty() { "failpoint: backup response error after checkpoint".to_owned() } else { msg };
                            return Err(errors::errorf(msg));
                        }
                    }
                    let files: Vec<Arc<tidb_proto::backup::File>> =
                        resp.files.iter().cloned().map(Arc::new).collect();
                    pr.lock()
                        .unwrap()
                        .res
                        .put(&resp.start_key, &resp.end_key, files);
                    self.set_api_version(resp.api_version);
                }
            }
            Some(err) => {
                if let Some(tidb_proto::backup::error::Detail::KvError(kv)) = &err.detail {
                    if let Some(lock) = &kv.locked {
                        return Ok(Some(lock.clone()));
                    }
                }
                let res = handle_backup_error(Some(err), store_id, err_context);
                if res.strategy == ErrorHandlingStrategy::GiveUp {
                    tidb_log::error(
                        "TiKV encountered an error, interrupting the backup.",
                        &[Field::new("error message", Value::Str(err.msg.clone()))],
                    );
                    return Err(errors::annotatef(
                        &ERR_KV_STORAGE,
                        format!(
                            "error happen in store {store_id}: {}, {}",
                            res.reason, err.msg
                        ),
                    ));
                }
            }
        }
        Ok(None)
    }
}

/// Go `map[string]string`'s `%v`.
fn go_map_text(m: &BTreeMap<String, String>) -> String {
    let parts: Vec<String> = m.iter().map(|(k, v)| format!("{k}:{v}")).collect();
    format!("[{}]", parts.join(" "))
}

/// Go `oracle.GetTimeFromTS(ts)`'s `%s`.
fn go_time_from_ts(ts: u64) -> String {
    tidb_go_time_layout::go_unix_milli_string(extract_physical(ts))
}

/// The progress tree's data file sink: Go `metaWriter.Send(files,
/// metautil.AppendDataFile)`.
struct MetaSink(Arc<MetaWriter>);

impl crate::rtree::MetaWriter for MetaSink {
    fn send(
        &mut self,
        files: &[Arc<tidb_proto::backup::File>],
    ) -> std::result::Result<(), crate::rtree::RtreeError> {
        self.0
            .send(MetaItem::DataFile(
                files.iter().map(|f| (**f).clone()).collect(),
            ))
            .map_err(crate::rtree::RtreeError::Meta)
    }
}

/// PD's TSO as the checkpoint lock's timer.
struct PdTimer(Arc<dyn PdClient>);

impl crate::checkpoint::GlobalTimer for PdTimer {
    fn get_ts(&self, ctx: &Context) -> Result<(i64, i64)> {
        self.0.get_ts(ctx)
    }
}

/// Go `CheckBackupStorageIsLocked`: a lock file next to SST files and no
/// checkpoint means another backup's data is there.
///
/// # Errors
/// [`ERR_INVALID_ARGUMENT`], or a storage error.
pub fn check_backup_storage_is_locked(ctx: &Context, s: &metautil::Storage) -> Result<()> {
    let exist = s.file_exists(ctx, metautil::LOCK_FILE).map_err(|e| {
        errors::annotate(
            errors::errorf(e.to_string()),
            format!("error occurred when checking {} file", metautil::LOCK_FILE),
        )
    })?;
    if exist {
        let uri = s.uri();
        s.walk_dir(ctx, None, &mut |path, _| {
            if path.ends_with(".sst") {
                return Err(tidb_objstore::Error::other(
                    errors::annotatef(
                        &ERR_INVALID_ARGUMENT,
                        format!(
                            "backup lock file and sst file exist in {uri}/{}, there are some backup files in the path already, but hasn't checkpoint metadata, please specify a correct backup directory!",
                            metautil::LOCK_FILE
                        ),
                    )
                    .to_string(),
                ));
            }
            Ok(())
        })
        .map_err(|e| errors::errorf(e.to_string()))?;
    }
    Ok(())
}

/// Go `BuildBackupRangeAndInitSchema`: the key ranges of every table the
/// filter matches (in the store's key space) and the schemas to back up;
/// a full backup also takes the placement policies. `None` schemas when
/// nothing matches.
///
/// # Errors
/// A catalog read error, or a table too new to back up.
pub fn build_backup_range_and_init_schema(
    storage: &Arc<dyn Storage>,
    table_filter: Arc<dyn tidb_table_filter::Filter>,
    backup_ts: u64,
    is_full_backup: bool,
) -> Result<(Vec<KeyRange>, Option<Schemas>, Vec<PlacementPolicy>)> {
    let m = crate::metareader::new_reader(storage.as_ref(), backup_ts);
    let meta_err = |e: tidb_meta::MetaError| errors::errorf(e.to_string());
    let mut policies = Vec::new();
    if is_full_backup {
        for p in m.policies().map_err(meta_err)? {
            let info = tidb_model::serde_helpers::to_go_json(&p)
                .map_err(|e| errors::errorf(e.to_string()))?;
            policies.push(PlacementPolicy { info });
        }
    }
    let mut ranges = Vec::new();
    let mut schemas_num = 0;
    for db in m.databases().map_err(meta_err)? {
        if !table_filter.match_schema(db.name.original())
            || tidb_metadef::db::is_mem_db(db.name.lowercase())
            || is_template_sys_db(db.name.original())
        {
            continue;
        }
        let mut tables = Vec::new();
        m.iter_tables(db.id, |t| {
            tables.push(t.clone_like_go());
            Ok(())
        })
        .map_err(meta_err)?;
        let mut has_table = false;
        for t in &tables {
            if t.version > crate::version::CURRENT_BACKUP_SUPPORT_TABLE_INFO_VERSION {
                return Err(errors::errorf(format!(
                    "backup doesn't not support table {} with version {}, maybe try a new version of br",
                    t.name.original(),
                    t.version
                )));
            }
            if !table_filter.match_table(db.name.original(), t.name.original()) {
                continue;
            }
            schemas_num += 1;
            has_table = true;
            for r in tidb_distsql::build_table_ranges(&table_range_spec(t))
                .map_err(|e| errors::errorf(format!("{e:?}")))?
            {
                let (start, end) = storage.encode_range(&r.start_key, &r.end_key);
                ranges.push(KeyRange::new(start, end));
            }
        }
        if !has_table {
            tidb_log::info(
                "backup empty database",
                &[Field::new("db", Value::Str(db.name.original().to_owned()))],
            );
            schemas_num += 1;
        }
    }
    if schemas_num == 0 {
        tidb_log::info("nothing to backup", &[]);
        return Ok((Vec::new(), None, Vec::new()));
    }
    let storage = Arc::clone(storage);
    let iter = move |f: &mut dyn FnMut(DBInfo, Option<TableInfo>)| -> Result<()> {
        build_backup_schemas(
            &storage,
            table_filter.as_ref(),
            backup_ts,
            is_full_backup,
            f,
        )
    };
    Ok((
        ranges,
        Some(Schemas::new(Box::new(iter), schemas_num)),
        policies,
    ))
}

/// The full-scan ranges' spec of a table (Go `distsql.BuildTableRanges`'s
/// reading of `model.TableInfo`).
#[must_use]
pub fn table_range_spec(t: &TableInfo) -> tidb_distsql::TableRangeSpec {
    tidb_distsql::TableRangeSpec {
        table_id: t.id,
        partition_ids: t
            .get_partition_info()
            .map_or_else(Vec::new, |p| p.read().definitions.map_visible(|d| d.id)),
        common_handle: t.is_common_handle,
        indexes: t
            .indices
            .iter_deref()
            .map(|i| {
                let i = i.read();
                tidb_distsql::TableIndexRangeSpec {
                    id: i.id,
                    public: i.state == tidb_model::SchemaState::PUBLIC,
                    global: i.global,
                }
            })
            .collect(),
    }
}

/// Go `BuildBackupSchemas`: each matching database and table as it is at
/// `backup_ts`, with its auto IDs one past the allocated ones, placement
/// cleared outside a full backup, the table cache off and only public
/// indexes; a database without tables is `f(db, None)`.
///
/// # Errors
/// A catalog read error.
pub fn build_backup_schemas(
    storage: &Arc<dyn Storage>,
    table_filter: &dyn tidb_table_filter::Filter,
    backup_ts: u64,
    is_full_backup: bool,
    f: &mut dyn FnMut(DBInfo, Option<TableInfo>),
) -> Result<()> {
    let m = crate::metareader::new_reader(storage.as_ref(), backup_ts);
    let meta_err = |e: tidb_meta::MetaError| errors::errorf(e.to_string());
    for mut db in m.databases().map_err(meta_err)? {
        if !table_filter.match_schema(db.name.original())
            || tidb_metadef::db::is_mem_db(db.name.lowercase())
            || is_template_sys_db(db.name.original())
        {
            continue;
        }
        if !is_full_backup {
            db.placement_policy_ref = None;
        }
        let mut tables = Vec::new();
        m.iter_tables(db.id, |t| {
            tables.push(t.clone_like_go());
            Ok(())
        })
        .map_err(meta_err)?;
        let mut has_table = false;
        for mut t in tables {
            if !table_filter.match_table(db.name.original(), t.name.original()) {
                continue;
            }
            let access = m.auto_ids(db.id, t.id);
            let global_auto_id = if t.is_sequence() {
                access.sequence_value().get().map_err(meta_err)?
            } else if t.is_view() || !need_auto_id(&t) {
                0
            } else if t.sep_auto_inc() {
                let id = access.increment_id(t.version).get().map_err(meta_err)?;
                match access.row_id().get() {
                    Ok(row_id) => t.auto_inc_id_extra = row_id + 1,
                    Err(e) => {
                        if id == 0 {
                            return Err(meta_err(e));
                        }
                        tidb_log::warn(
                            "get rowid error",
                            &[Field::new("error", Value::Str(e.to_string()))],
                        );
                    }
                }
                id
            } else {
                access.row_id().get().map_err(meta_err)?
            };
            t.auto_inc_id = global_auto_id + 1;
            if !is_full_backup {
                t.clear_placement();
            }
            t.table_cache_status_type = tidb_model::TableCacheStatusType::DISABLE;
            if t.contains_auto_random_bits() {
                let rand = access.random_id().get().map_err(meta_err)?;
                t.auto_rand_id = rand + 1;
                tidb_log::debug(
                    "change table AutoRandID",
                    &[
                        Field::new("db", Value::Str(db.name.original().into())),
                        Field::new("table", Value::Str(t.name.original().into())),
                        Field::new("AutoRandID", Value::I64(rand)),
                    ],
                );
            }
            tidb_log::debug(
                "change table AutoIncID",
                &[
                    Field::new("db", Value::Str(db.name.original().into())),
                    Field::new("table", Value::Str(t.name.original().into())),
                    Field::new("AutoIncID", Value::I64(global_auto_id)),
                ],
            );
            let public: Vec<_> = t
                .indices
                .iter_deref()
                .filter(|i| i.read().state == tidb_model::SchemaState::PUBLIC)
                .collect();
            t.indices = tidb_model::GoSharedPointerSlice::from_handles(
                public.into_iter().map(Some).collect(),
            );
            f(db.clone(), Some(t));
            has_table = true;
        }
        if !has_table {
            f(db, None);
        }
    }
    Ok(())
}

/// Go `skipUnsupportedDDLJob`: placement and attribute DDL is not
/// restored.
#[must_use]
pub fn skip_unsupported_ddl_job(job: &Job) -> bool {
    use tidb_model::ActionType as A;
    [
        A::ACTION_CREATE_PLACEMENT_POLICY,
        A::ACTION_ALTER_PLACEMENT_POLICY,
        A::ACTION_DROP_PLACEMENT_POLICY,
        A::ACTION_ALTER_TABLE_PARTITION_PLACEMENT,
        A::ACTION_MODIFY_SCHEMA_DEFAULT_PLACEMENT,
        A::ACTION_ALTER_TABLE_PLACEMENT,
        A::ACTION_ALTER_TABLE_ATTRIBUTES,
        A::ACTION_ALTER_TABLE_PARTITION_ATTRIBUTES,
    ]
    .contains(&job.type_)
}

/// Go `WriteBackupDDLJobs`: the finished DDL jobs between the two schema
/// versions (queued jobs first, then history from the newest), by job ID,
/// placement cleared, to the meta writer's DDL phase.
///
/// # Errors
/// A catalog read error, or the session's.
pub fn write_backup_ddl_jobs(
    meta_writer: &MetaWriter,
    g: &dyn Glue,
    store: &Arc<dyn Storage>,
    last_backup_ts: u64,
    backup_ts: u64,
    need_domain: bool,
) -> Result<()> {
    let meta_err = |e: tidb_meta::MetaError| errors::errorf(e.to_string());
    let snap_meta = crate::metareader::new_reader(store.as_ref(), backup_ts);
    let last_meta = crate::metareader::new_reader(store.as_ref(), last_backup_ts);
    let last_schema_version = last_meta
        .get_schema_version_with_non_empty_diff()
        .map_err(meta_err)?;
    let backup_schema_version = snap_meta
        .get_schema_version_with_non_empty_diff()
        .map_err(meta_err)?;
    let version = store.current_version()?;
    let append_jobs = |jobs: Vec<Job>| -> (Vec<Job>, bool) {
        let mut out = Vec::with_capacity(jobs.len());
        for job in jobs {
            if skip_unsupported_ddl_job(&job) {
                continue;
            }
            let schema_version = job.binlog_info.as_ref().map(|b| b.read().schema_version);
            if schema_version.is_some_and(|v| v <= last_schema_version) {
                return (out, true);
            }
            let done = job.state == tidb_model::JobState::DONE
                || job.state == tidb_model::JobState::SYNCED;
            if done
                && schema_version
                    .is_some_and(|v| v > last_schema_version && v <= backup_schema_version)
            {
                if let Some(info) = &job.binlog_info {
                    let info = info.read();
                    if let Some(db) = &info.db_info {
                        db.write().placement_policy_ref = None;
                    }
                    if let Some(t) = &info.table_info {
                        t.write().clear_placement();
                    }
                }
                out.push(job);
            }
        }
        (out, false)
    };
    let newest_meta = crate::metareader::new_reader(store.as_ref(), version);
    let mut all_jobs = Vec::new();
    g.use_one_shot_session(!need_domain, &mut |se| {
        all_jobs = se.get_all_ddl_jobs(&Context::background())?;
        tidb_log::debug(
            "get all jobs",
            &[Field::new("jobs", Value::I64(all_jobs.len() as i64))],
        );
        Ok(())
    })?;
    let (mut all_jobs, _) = append_jobs(all_jobs);
    let mut iter = newest_meta
        .get_last_history_ddl_jobs_iterator::<Job>()
        .map_err(meta_err)?;
    let mut count = all_jobs.len();
    loop {
        let mut cache = Vec::with_capacity(DEF_NUM_HISTORY_JOBS as usize);
        tidb_meta::transaction::LastJobIterator::get_last_jobs(
            &mut iter,
            DEF_NUM_HISTORY_JOBS,
            &mut cache,
        )
        .map_err(meta_err)?;
        if cache.is_empty() {
            break;
        }
        let (jobs, finished) = append_jobs(cache);
        count += jobs.len();
        all_jobs.extend(jobs);
        if finished {
            break;
        }
    }
    tidb_log::debug(
        "get complete jobs",
        &[Field::new("jobs", Value::I64(count as i64))],
    );
    all_jobs.sort_by_key(|j| j.id);
    for job in &all_jobs {
        let bytes = tidb_model::serde_helpers::to_go_json(job)
            .map_err(|e| errors::errorf(e.to_string()))?;
        meta_writer
            .send(MetaItem::Ddl(bytes))
            .map_err(errors::errorf)?;
    }
    Ok(())
}

/// Go `ddl.DefNumHistoryJobs`.
pub const DEF_NUM_HISTORY_JOBS: i32 = 10;

/// A store change notification sink that drops what it gets, for callers
/// that do not watch stores.
#[must_use]
pub fn new_state_notifier() -> (Sender<BackupRetryPolicy>, Receiver<BackupRetryPolicy>) {
    bounded(0)
}

/// Re-export for tests: the map Go keys checkpoint checksums by.
pub type ChecksumMap = HashMap<i64, crate::checkpoint::ChecksumItem>;
