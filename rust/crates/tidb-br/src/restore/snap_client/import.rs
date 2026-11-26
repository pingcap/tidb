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

//! Go `br/pkg/restore/snap_client/import.go`: `SnapFileImporter` downloads
//! a table's SST files onto every peer of each region they cover, then
//! ingests them on the leader.
//!
//! Only the `TiDBFull` path SQL restore takes is ported: the raw/txn KV
//! download, the compacted batch download and `BatchDownloadLatestMVCC`
//! serve `br` CLI and PiTR modes without a SQL caller.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender};
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::backup::{CipherInfo, File, StorageBackend};
use tidb_proto::{import_sstpb, kvrpcpb, metapb};

use crate::errors::{self, Error, Result};
use crate::restore::import_client::ImporterClient;
use crate::restore::restorer::{
    zap_batch_backup_file_set, BackupFileSet, BalancedFileImporter, FileImporter,
};
use crate::restore::split::{self, check_region_epoch, RegionInfo, SplitClient};
use crate::restore_utils::{
    encode_key_prefix, find_matched_rewrite_rule, get_rewrite_raw_keys, rewrite_and_encode_raw_key,
    set_time_range_filter, truncate_ts, RestoreError, RestoreErrorKind, RewriteRules,
    DEFAULT_CF_NAME, WRITE_CF_NAME,
};
use crate::utils::backoff::{
    new_download_sst_backoff_strategy, new_import_sst_backoff_strategy,
    new_peer_download_sst_backoff_strategy,
};
use crate::utils::retry::{verbose_retry, with_retry, with_retry_v2};

/// Go `KvMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvMode {
    /// Go `TiDBFull`.
    TiDBFull,
    /// Go `Raw`.
    Raw,
    /// Go `Txn`.
    Txn,
    /// Go `TiDBCompacted`.
    TiDBCompacted,
}

/// Go `gRPCTimeOut`.
const GRPC_TIMEOUT: Duration = Duration::from_secs(200 * 60);

/// Go `RewriteMode`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RewriteMode {
    /// Go `RewriteModeLegacy`: the old-style prefix only rewrite.
    #[default]
    Legacy,
    /// Go `RewriteModeKeyspace`: keys carry the keyspace.
    Keyspace,
}

/// Go `DownloadRateLimitTTLSeconds`.
pub const DOWNLOAD_RATE_LIMIT_TTL_SECONDS: u64 = 3600;

/// Go `kvutil.BuildRequestSource(true, kv.InternalTxnBR, kvutil.ExplicitTypeBR)`.
const BR_REQUEST_SOURCE: &str = "internal_br_br";

/// Go `utils.DefaultWorkerTokenChannelSize`.
const DEFAULT_WORKER_TOKEN_CHANNEL_SIZE: usize = 128;
/// Go `utils.MaxWorkerTokenChannelSize`.
const MAX_WORKER_TOKEN_CHANNEL_SIZE: usize = 30 * 1024 * 1024;

/// A token channel: Go's `chan struct{}` filled with its tokens.
#[derive(Clone)]
struct TokenCh {
    give: Sender<()>,
    take: Receiver<()>,
}

impl TokenCh {
    fn len(&self) -> usize {
        self.take.len()
    }
}

/// Go `utils.BuildWorkerTokenChannel`.
fn build_worker_token_channel(size: usize) -> TokenCh {
    let size = if size == 0 {
        tidb_log::warn(
            "build worker token channel: size is 0, set to default value",
            &[Field::new(
                "new size",
                Value::I64(DEFAULT_WORKER_TOKEN_CHANNEL_SIZE as i64),
            )],
        );
        DEFAULT_WORKER_TOKEN_CHANNEL_SIZE
    } else if size > MAX_WORKER_TOKEN_CHANNEL_SIZE {
        tidb_log::warn(
            "build worker token channel: size is greater than max value, set to max value",
            &[Field::new(
                "new size",
                Value::I64(MAX_WORKER_TOKEN_CHANNEL_SIZE as i64),
            )],
        );
        MAX_WORKER_TOKEN_CHANNEL_SIZE
    } else {
        size
    };
    let (give, take) = bounded(size);
    for _ in 0..size {
        give.send(()).expect("the channel has room for its tokens");
    }
    TokenCh { give, take }
}

/// Go `storeTokenChannelMap`.
struct StoreTokenChannelMap {
    tokens: RwLock<HashMap<u64, TokenCh>>,
}

impl StoreTokenChannelMap {
    /// Go `acquireTokenCh`.
    fn acquire_token_ch(&self, store_id: u64, buffer_size: usize) -> TokenCh {
        if let Some(ch) = self.tokens.read().unwrap().get(&store_id) {
            return ch.clone();
        }
        self.tokens
            .write()
            .unwrap()
            .entry(store_id)
            .or_insert_with(|| build_worker_token_channel(buffer_size))
            .clone()
    }

    /// Go `ShouldBlock`: every store's tokens are taken.
    fn should_block(&self) -> bool {
        let tokens = self.tokens.read().unwrap();
        !tokens.is_empty() && tokens.values().all(|ch| ch.len() == 0)
    }

    /// Go `newStoreTokenChannelMap`.
    fn new(stores: &[metapb::Store], buffer_size: usize) -> Self {
        let mut tokens = HashMap::new();
        if buffer_size > 0 {
            for store in stores {
                tokens.insert(store.id, build_worker_token_channel(buffer_size));
            }
        }
        Self {
            tokens: RwLock::new(tokens),
        }
    }
}

/// A callback run when the importer is created or closed.
pub type ImporterCallback = Box<dyn FnOnce(&SnapFileImporter) -> Result<()> + Send>;
/// Go `beforeIngestCallbacks`: runs before a batch is imported and may
/// return a callback to run after it is ingested.
pub type BeforeIngestCallback = Box<
    dyn Fn(&Context, &[BackupFileSet]) -> Result<Option<Box<dyn FnOnce() -> Result<()> + Send>>>
        + Send
        + Sync,
>;

/// Go `SnapFileImporter`.
pub struct SnapFileImporter {
    task_id: String,
    cipher: Option<CipherInfo>,
    api_version: kvrpcpb::ApiVersion,
    meta_client: Arc<dyn SplitClient>,
    import_client: Arc<dyn ImporterClient>,
    backend: Option<StorageBackend>,
    download_tokens_map: StoreTokenChannelMap,
    ingest_tokens_map: StoreTokenChannelMap,
    close_callbacks: Mutex<Vec<ImporterCallback>>,
    before_ingest_callbacks: RwLock<Vec<BeforeIngestCallback>>,
    concurrency_per_store: usize,
    pd_req_tokens: Option<TokenCh>,
    kv_mode: KvMode,
    rewrite_mode: RewriteMode,
    cache_key: String,
    cond: (Mutex<()>, Condvar),
    peer_download_retry: AtomicBool,
}

/// Go `SnapFileImporterOptions`.
pub struct SnapFileImporterOptions {
    cipher: Option<CipherInfo>,
    meta_client: Arc<dyn SplitClient>,
    import_client: Arc<dyn ImporterClient>,
    backend: Option<StorageBackend>,
    rewrite_mode: RewriteMode,
    tikv_stores: Vec<metapb::Store>,
    scan_concurrency: usize,
    concurrency_per_store: usize,
    create_callbacks: Vec<ImporterCallback>,
    close_callbacks: Vec<ImporterCallback>,
}

impl SnapFileImporterOptions {
    /// Go `NewSnapFileImporterOptions`; `retainLatestMVCCVersion` is only
    /// set by compacted restore and is not taken.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        cipher: Option<CipherInfo>,
        meta_client: Arc<dyn SplitClient>,
        import_client: Arc<dyn ImporterClient>,
        backend: Option<StorageBackend>,
        rewrite_mode: RewriteMode,
        tikv_stores: Vec<metapb::Store>,
        concurrency_per_store: usize,
        scan_concurrency: usize,
        create_callbacks: Vec<ImporterCallback>,
        close_callbacks: Vec<ImporterCallback>,
    ) -> Self {
        Self {
            cipher,
            meta_client,
            import_client,
            backend,
            rewrite_mode,
            tikv_stores,
            scan_concurrency,
            concurrency_per_store,
            create_callbacks,
            close_callbacks,
        }
    }
}

/// Go `fmt.Sprintf("BR-%s-%d", time.Now().Format("20060102150405"), rand.Int63())`.
fn new_cache_key() -> String {
    let mut buf = [0_u8; 8];
    getrandom::fill(&mut buf).expect("the OS has randomness");
    let n = u64::from_le_bytes(buf) >> 1;
    format!("BR-{}-{n}", chrono::Local::now().format("%Y%m%d%H%M%S"))
}

impl SnapFileImporter {
    /// Go `NewSnapFileImporter`.
    ///
    /// # Errors
    /// A zero `concurrencyPerStore`, or a create callback's error.
    pub fn new(
        api_version: kvrpcpb::ApiVersion,
        kv_mode: KvMode,
        options: SnapFileImporterOptions,
    ) -> Result<Arc<Self>> {
        if options.concurrency_per_store == 0 {
            return Err(errors::errorf("concurrencyPerStore must be greater than 0"));
        }
        let pd_req_tokens = (options.scan_concurrency > 0)
            .then(|| build_worker_token_channel(options.scan_concurrency));
        let importer = Arc::new(Self {
            task_id: uuid::Uuid::new_v4().to_string(),
            cipher: options.cipher,
            api_version,
            meta_client: options.meta_client,
            import_client: options.import_client,
            backend: options.backend,
            download_tokens_map: StoreTokenChannelMap::new(
                &options.tikv_stores,
                options.concurrency_per_store,
            ),
            ingest_tokens_map: StoreTokenChannelMap::new(
                &options.tikv_stores,
                options.concurrency_per_store,
            ),
            close_callbacks: Mutex::new(options.close_callbacks),
            before_ingest_callbacks: RwLock::default(),
            concurrency_per_store: options.concurrency_per_store,
            pd_req_tokens,
            kv_mode,
            rewrite_mode: options.rewrite_mode,
            cache_key: new_cache_key(),
            cond: (Mutex::new(()), Condvar::new()),
            peer_download_retry: AtomicBool::new(false),
        });
        for f in options.create_callbacks {
            f(&importer)?;
        }
        Ok(importer)
    }

    /// Go `importer.beforeIngestCallbacks = append(...)`.
    pub fn add_before_ingest_callback(&self, cb: BeforeIngestCallback) {
        self.before_ingest_callbacks.write().unwrap().push(cb);
    }

    /// Go `importer.closeCallbacks = append(...)`.
    pub fn add_close_callback(&self, cb: ImporterCallback) {
        self.close_callbacks.lock().unwrap().push(cb);
    }

    /// The import client.
    #[must_use]
    pub fn import_client(&self) -> &Arc<dyn ImporterClient> {
        &self.import_client
    }

    /// Go `ShouldBlock`.
    #[must_use]
    pub fn should_block(&self) -> bool {
        self.download_tokens_map.should_block() || self.ingest_tokens_map.should_block()
    }

    /// Go `releaseToken`.
    fn release_token(&self, ch: &TokenCh) {
        let _ = ch.give.send(());
        let _guard = self.cond.0.lock().unwrap();
        self.cond.1.notify_one();
    }

    /// Takes a token of `ch`, or `ctx`'s error.
    fn acquire(ctx: &Context, ch: &TokenCh) -> Result<()> {
        crossbeam_channel::select! {
            recv(ctx.done()) -> _ => Err(Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled))),
            recv(ch.take) -> _ => Ok(()),
        }
    }

    /// Go `paginateScanRegion`, at most `scanConcurrency` at once.
    fn paginate_scan_region(
        &self,
        ctx: &Context,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<RegionInfo>> {
        if let Some(tokens) = &self.pd_req_tokens {
            Self::acquire(ctx, tokens)?;
        }
        let res = split::paginate_scan_region(
            ctx,
            self.meta_client.as_ref(),
            start_key,
            end_key,
            split::SCAN_REGION_PAGINATION_LIMIT,
        );
        if let Some(tokens) = &self.pd_req_tokens {
            let _ = tokens.give.send(());
        }
        res
    }

    /// Go `PaginateScanRegionForTest`.
    #[cfg(test)]
    pub(crate) fn paginate_scan_region_for_test(
        &self,
        ctx: &Context,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<RegionInfo>> {
        self.paginate_scan_region(ctx, start_key, end_key)
    }

    /// Go `SetDownloadSpeedLimit`.
    ///
    /// # Errors
    /// The store's error.
    pub fn set_download_speed_limit(
        &self,
        ctx: &Context,
        store_id: u64,
        rate_limit: u64,
    ) -> Result<()> {
        set_download_speed_limit(
            self.import_client.as_ref(),
            &self.task_id,
            ctx,
            store_id,
            rate_limit,
        )
    }

    /// The task ID the download speed limit is set under.
    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Go `CheckPeerDownloadRetrySupport`: a store's error falls back to the
    /// legacy retry.
    ///
    /// # Errors
    /// Never; Go's signature has one.
    pub fn check_peer_download_retry_support(
        &self,
        ctx: &Context,
        tikv_stores: &[metapb::Store],
    ) -> Result<()> {
        let ids = up_store_ids(tikv_stores);
        let support = match self
            .import_client
            .is_batch_download_latest_mvcc_supported(ctx, &ids)
        {
            Ok(s) => s,
            Err(err) => {
                tidb_log::warn(
                    "failed to check peer download retry support, fallback to legacy download retry",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
                false
            }
        };
        self.peer_download_retry.store(support, Ordering::Relaxed);
        Ok(())
    }

    /// Go `CheckMultiIngestSupport`.
    ///
    /// # Errors
    /// A store without multi ingest.
    pub fn check_multi_ingest_support(
        &self,
        ctx: &Context,
        tikv_stores: &[metapb::Store],
    ) -> Result<()> {
        self.import_client
            .check_multi_ingest_support(ctx, &up_store_ids(tikv_stores))
    }

    /// Go `getKeyRangeForFiles`: the rewritten raw key range covering the
    /// files (Go `getKeyRangeByMode(TiDBFull)`).
    fn get_key_range_for_files(&self, files_group: &[BackupFileSet]) -> Result<(Vec<u8>, Vec<u8>)> {
        let (mut start_key, mut end_key): (Vec<u8>, Vec<u8>) = (Vec::new(), Vec::new());
        for files in files_group {
            for f in &files.sst_files {
                let (start, end) = match self.kv_mode {
                    KvMode::Raw => (f.start_key.clone(), f.end_key.clone()),
                    KvMode::Txn => {
                        let enc = |k: &[u8]| {
                            if k.is_empty() {
                                Vec::new()
                            } else {
                                let mut b = Vec::new();
                                tidb_codec::encode_bytes(&mut b, k);
                                b
                            }
                        };
                        (enc(&f.start_key), enc(&f.end_key))
                    }
                    _ => {
                        let (s, e) =
                            get_rewrite_raw_keys(f.as_ref(), files.rewrite_rules.as_deref())
                                .map_err(restore_err)?;
                        (s.unwrap_or_default(), e.unwrap_or_default())
                    }
                };
                if start_key.is_empty() || start < start_key {
                    start_key = start;
                }
                if end_key.is_empty() || end_key < end {
                    end_key = end;
                }
            }
        }
        Ok((start_key, end_key))
    }

    /// Go `download`: [`Self::download_sst`] retried; an SST that fails to
    /// decrypt is downloaded again without the cipher.
    fn download(
        &self,
        ctx: &Context,
        region: &RegionInfo,
        files_group: &[BackupFileSet],
    ) -> Result<Vec<import_sstpb::SstMeta>> {
        let mut metas = Vec::new();
        with_retry(
            ctx,
            || {
                let mut res = self.download_sst(ctx, region, files_group, self.cipher.as_ref());
                if let Some(v) = tidb_failpoint::eval(
                    "github.com/pingcap/tidb/br/pkg/restore/snap_client/restore-storage-error",
                ) {
                    let msg = v.string();
                    tidb_log::debug(
                        "failpoint restore-storage-error injected.",
                        &[Field::new("msg", Value::Str(msg.clone()))],
                    );
                    res = res.and_then(|_| Err(errors::errorf(msg)));
                }
                if tidb_failpoint::eval(
                    "github.com/pingcap/tidb/br/pkg/restore/snap_client/restore-gRPC-error",
                )
                .is_some()
                {
                    tidb_log::warn(
                        "the connection to TiKV has been cut by a neko, meow :3",
                        &[],
                    );
                    res = Err(errors::grpc(tonic::Status::unavailable(
                        "the connection to TiKV has been cut by a neko, meow :3",
                    )));
                }
                if res.as_ref().err().is_some_and(is_decrypt_sst_err) {
                    tidb_log::info(
                        "fail to decrypt when download sst, try again with no-crypt",
                        &[],
                    );
                    res = self.download_sst(ctx, region, files_group, None);
                }
                metas = res?;
                Ok(())
            },
            &mut new_download_sst_backoff_strategy(),
        )?;
        Ok(metas)
    }

    /// Go `buildDownloadRequest`: `None` when no rule matches the file or the
    /// file is outside the region.
    fn build_download_request(
        &self,
        file: &File,
        rewrite_rules: Option<&RewriteRules>,
        region: &RegionInfo,
        cipher: Option<&CipherInfo>,
    ) -> Result<Option<(import_sstpb::DownloadRequest, import_sstpb::SstMeta)>> {
        let Some(file_rule) = find_matched_rewrite_rule(file, rewrite_rules) else {
            tidb_log::warn(
                "download file skipped",
                &[
                    region_field(&region.region),
                    Field::new(
                        "error",
                        Value::Str(errors::ERR_KV_REWRITE_RULE_NOT_FOUND.to_string()),
                    ),
                ],
            );
            return Ok(None);
        };
        let encoded_start_key = rewrite_and_encode_raw_key(&file.start_key, Some(file_rule));
        if !region.region.end_key.is_empty() && encoded_start_key >= region.region.end_key {
            return Ok(None);
        }
        let encoded_end_key = rewrite_and_encode_raw_key(&file.end_key, Some(file_rule));
        if encoded_end_key <= region.region.start_key {
            return Ok(None);
        }
        let mut rule = file_rule.clone();
        if let Some(rules) = rewrite_rules {
            set_time_range_filter(rules, &mut rule, &file.cf).map_err(restore_err)?;
        }
        if self.rewrite_mode == RewriteMode::Legacy {
            rule.old_key_prefix = encode_key_prefix(&file_rule.old_key_prefix);
            rule.new_key_prefix = encode_key_prefix(&file_rule.new_key_prefix);
        }
        let sst_meta = get_sst_meta_from_file(file, &region.region, &rule, self.rewrite_mode)?;
        let req = import_sstpb::DownloadRequest {
            sst: Some(sst_meta.clone()),
            storage_backend: self.backend.clone(),
            name: file.name.clone(),
            rewrite_rule: Some(rule),
            cipher_info: cipher.cloned(),
            storage_cache_id: self.cache_key.clone(),
            request_type: import_sstpb::DownloadRequestType::Keyspace as i32,
            context: Some(kvrpcpb::Context {
                resource_control_context: Some(kvrpcpb::ResourceControlContext::default()),
                request_source: BR_REQUEST_SOURCE.to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(Some((req, sst_meta)))
    }

    /// Go `downloadWithOptionalPeerRetry`.
    fn download_with_optional_peer_retry(
        &self,
        ctx: &Context,
        f: impl FnMut(&Context) -> Result<import_sstpb::DownloadResponse>,
    ) -> Result<import_sstpb::DownloadResponse> {
        if self.peer_download_retry.load(Ordering::Relaxed) {
            with_retry_v2(ctx, &mut new_peer_download_sst_backoff_strategy(), f)
        } else {
            with_retry_v2(ctx, &mut new_download_sst_backoff_strategy(), f)
        }
    }

    /// Go `downloadSST`: every file onto every peer of the region; the SST
    /// metas of the files that are not empty there.
    fn download_sst(
        &self,
        ctx: &Context,
        region: &RegionInfo,
        files_group: &[BackupFileSet],
        cipher: Option<&CipherInfo>,
    ) -> Result<Vec<import_sstpb::SstMeta>> {
        let mut download_metas: HashMap<String, import_sstpb::SstMeta> = HashMap::new();
        let mut download_reqs: HashMap<String, import_sstpb::DownloadRequest> = HashMap::new();
        for files in files_group {
            for file in &files.sst_files {
                let Some((req, mut meta)) = self.build_download_request(
                    file,
                    files.rewrite_rules.as_deref(),
                    region,
                    cipher,
                )?
                else {
                    continue;
                };
                meta.api_version = self.api_version as i32;
                download_metas.insert(file.name.clone(), meta);
                download_reqs.insert(file.name.clone(), req);
            }
        }
        let result: Mutex<HashMap<String, import_sstpb::SstMeta>> = Mutex::default();
        let (ectx, ecancel) = Context::with_cancel(ctx);
        let first: Mutex<Option<Error>> = Mutex::new(None);
        std::thread::scope(|scope| {
            for peer in &region.region.peers {
                let (ectx, ecancel, first, result, reqs, metas) = (
                    &ectx,
                    &ecancel,
                    &first,
                    &result,
                    &download_reqs,
                    &download_metas,
                );
                scope.spawn(move || {
                    let res = (|| -> Result<()> {
                        let token = self
                            .download_tokens_map
                            .acquire_token_ch(peer.store_id, self.concurrency_per_store);
                        Self::acquire(ectx, &token)?;
                        let res = self.download_from_peer(ectx, region, peer, reqs, metas, result);
                        self.release_token(&token);
                        res
                    })();
                    if let Err(err) = res {
                        let mut first = first.lock().unwrap();
                        if first.is_none() {
                            *first = Some(err);
                            ecancel.cancel();
                        }
                    }
                });
            }
        });
        ecancel.cancel();
        if let Some(err) = first.into_inner().unwrap() {
            return Err(err);
        }
        Ok(result.into_inner().unwrap().into_values().collect())
    }

    /// One peer's loop of Go `downloadSST`.
    fn download_from_peer(
        &self,
        ctx: &Context,
        region: &RegionInfo,
        peer: &metapb::Peer,
        reqs: &HashMap<String, import_sstpb::DownloadRequest>,
        metas: &HashMap<String, import_sstpb::SstMeta>,
        result: &Mutex<HashMap<String, import_sstpb::SstMeta>>,
    ) -> Result<()> {
        for (file_name, req) in reqs {
            let resp = self.download_with_optional_peer_retry(ctx, |ctx| {
                let (dctx, cancel) = Context::with_timeout(ctx, GRPC_TIMEOUT);
                let res = self
                    .import_client
                    .download_sst(&dctx, peer.store_id, req.clone());
                cancel.cancel();
                res
            })?;
            if let Some(err) = &resp.error {
                return Err(errors::annotatef(
                    &errors::ERR_KV_DOWNLOAD_FAILED,
                    err.message.clone(),
                ));
            }
            if resp.is_empty {
                tidb_log::warn(
                    "download file skipped",
                    &[
                        Field::new("filename", Value::Str(file_name.clone())),
                        region_field(&region.region),
                        Field::new(
                            "error",
                            Value::Str(errors::ERR_KV_RANGE_IS_EMPTY.to_string()),
                        ),
                    ],
                );
                continue;
            }
            let mut result = result.lock().unwrap();
            let Some(meta) = metas.get(file_name) else {
                return Err(errors::errorf(format!(
                    "not found file {file_name} for download sstMeta"
                )));
            };
            let mut meta = meta.clone();
            let range = resp.range.clone().unwrap_or_default();
            meta.range = Some(import_sstpb::Range {
                start: truncate_ts(&range.start).to_vec(),
                end: truncate_ts(&range.end).to_vec(),
            });
            result.insert(file_name.clone(), meta);
            tidb_log::debug(
                "download from peer",
                &[
                    Field::new("filename", Value::Str(file_name.clone())),
                    region_field(&region.region),
                    Field::new(
                        "peer",
                        Value::Str(format!("{{id:{} store_id:{}}}", peer.id, peer.store_id)),
                    ),
                    Field::new("resp-isempty", Value::Bool(resp.is_empty)),
                    Field::new("resp-crc32", Value::U64(u64::from(resp.crc32))),
                ],
            );
        }
        Ok(())
    }

    /// Go `ingest`: the SSTs ingested on the region's leader, following a
    /// new leader.
    fn ingest(
        &self,
        ctx: &Context,
        info: &RegionInfo,
        download_metas: Vec<import_sstpb::SstMeta>,
    ) -> Result<()> {
        if download_metas.is_empty() {
            return Ok(());
        }
        let leader_store = info.leader.as_ref().map_or(0, |l| l.store_id);
        let token = self
            .ingest_tokens_map
            .acquire_token_ch(leader_store, self.concurrency_per_store);
        Self::acquire(ctx, &token)?;
        let res = self.ingest_loop(ctx, info.clone(), download_metas);
        self.release_token(&token);
        res
    }

    fn ingest_loop(
        &self,
        ctx: &Context,
        mut info: RegionInfo,
        download_metas: Vec<import_sstpb::SstMeta>,
    ) -> Result<()> {
        loop {
            let resp = self.ingest_ssts(ctx, &download_metas, &info)?;
            let Some(err_pb) = resp.error else {
                tidb_log::info(
                    "finish ingesting into a region.",
                    &[
                        region_field(&info.region),
                        Field::new("sst", Value::I64(download_metas.len() as i64)),
                    ],
                );
                return Ok(());
            };
            if let Some(not_leader) = &err_pb.not_leader {
                let new_info = if let Some(leader) = &not_leader.leader {
                    RegionInfo {
                        leader: Some(*leader),
                        region: info.region.clone(),
                        ..Default::default()
                    }
                } else {
                    loop {
                        if let Some(r) = self.meta_client.get_region(ctx, &info.region.start_key)? {
                            break r;
                        }
                        tidb_log::warn(
                            "ingest get region by key return nil",
                            &[region_field(&info.region)],
                        );
                        if ctx.sleep(Duration::from_secs(1)) {
                            return Err(Arc::new(
                                ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled),
                            ));
                        }
                    }
                };
                if !check_region_epoch(&new_info, &info) {
                    return Err(errors::new(&errors::ERR_KV_EPOCH_NOT_MATCH));
                }
                tidb_log::debug(
                    "ingest sst returns not leader error, retry it",
                    &[
                        region_field(&info.region),
                        Field::new("newLeader", Value::Str(format!("{:?}", new_info.leader))),
                    ],
                );
                info = new_info;
            } else if err_pb.epoch_not_match.is_some() {
                return Err(errors::new(&errors::ERR_KV_EPOCH_NOT_MATCH));
            } else if err_pb.key_not_in_region.is_some() {
                return Err(errors::new(&errors::ERR_KV_KEY_NOT_IN_REGION));
            } else {
                return Err(errors::annotatef(
                    &errors::ERR_KV_INGEST_FAILED,
                    format!("ingest error {err_pb:?}"),
                ));
            }
        }
    }

    /// Go `ingestSSTs`.
    fn ingest_ssts(
        &self,
        ctx: &Context,
        sst_metas: &[import_sstpb::SstMeta],
        region: &RegionInfo,
    ) -> Result<import_sstpb::IngestResponse> {
        let Some(leader) = &region.leader else {
            return Err(errors::annotatef(
                &errors::ERR_PD_LEADER_NOT_FOUND,
                format!("region id {} has no leader", region.region.id),
            ));
        };
        let req = import_sstpb::MultiIngestRequest {
            context: Some(kvrpcpb::Context {
                region_id: region.region.id,
                region_epoch: region.region.region_epoch,
                peer: Some(*leader),
                resource_control_context: Some(kvrpcpb::ResourceControlContext::default()),
                request_source: BR_REQUEST_SOURCE.to_owned(),
                ..Default::default()
            }),
            ssts: sst_metas.to_vec(),
        };
        self.import_client.multi_ingest(ctx, leader.store_id, req)
    }
}

/// Go `SetDownloadSpeedLimit`'s request for `task_id`.
pub(crate) fn set_download_speed_limit(
    client: &dyn ImporterClient,
    task_id: &str,
    ctx: &Context,
    store_id: u64,
    rate_limit: u64,
) -> Result<()> {
    let req = import_sstpb::SetDownloadSpeedLimitRequest {
        task_id: task_id.to_owned(),
        speed_limit: rate_limit,
        ttl_seconds: DOWNLOAD_RATE_LIMIT_TTL_SECONDS,
    };
    client
        .set_download_speed_limit(ctx, store_id, req)
        .map(|_| ())
}

/// The IDs of the stores that are up.
fn up_store_ids(stores: &[metapb::Store]) -> Vec<u64> {
    stores
        .iter()
        .filter(|s| s.state == metapb::StoreState::Up as i32)
        .map(|s| s.id)
        .collect()
}

fn region_field(region: &metapb::Region) -> Field {
    Field::new(
        "region",
        Value::Str(format!(
            "{{ID: {}, startKey: {}, endKey: {}}}",
            region.id,
            tidb_redact::key(&region.start_key),
            tidb_redact::key(&region.end_key)
        )),
    )
}

/// A `br/pkg/restore/utils` error as a BR error with its sentinel.
pub(crate) fn restore_err(err: RestoreError) -> Error {
    let sentinel = match err.kind() {
        RestoreErrorKind::InvalidRange => &*errors::ERR_INVALID_RANGE,
        RestoreErrorKind::RestoreTableIdMismatch => &*errors::ERR_RESTORE_TABLE_ID_MISMATCH,
        RestoreErrorKind::RestoreInvalidRewrite => &*errors::ERR_RESTORE_INVALID_REWRITE,
        RestoreErrorKind::RestoreInvalidBackup => &*errors::ERR_RESTORE_INVALID_BACKUP,
        RestoreErrorKind::Plain => return errors::errorf(err.to_string()),
    };
    let text = err.to_string();
    let annotation = text
        .strip_suffix(&format!(": {}", err.kind().message()))
        .unwrap_or_default();
    if annotation.is_empty() {
        errors::new(sentinel)
    } else {
        errors::annotatef(sentinel, annotation)
    }
}

/// Go `getSSTMetaFromFile`: the SST's range is the rule's new prefix
/// clipped to the region.
///
/// # Errors
/// A region key that fails to decode in keyspace mode.
///
/// # Panics
/// The range start exceeds its end, as Go's `log.Panic`.
pub fn get_sst_meta_from_file(
    file: &File,
    region: &metapb::Region,
    region_rule: &import_sstpb::RewriteRule,
    rewrite_mode: RewriteMode,
) -> Result<import_sstpb::SstMeta> {
    let (mut start, mut end) = (region.start_key.clone(), region.end_key.clone());
    if rewrite_mode == RewriteMode::Keyspace {
        let decode = |k: &[u8]| {
            tidb_codec::decode_bytes(k)
                .map(|(_, raw)| raw)
                .map_err(|e| errors::errorf(e.to_string()))
        };
        if !start.is_empty() {
            start = decode(&start)?;
        }
        if !end.is_empty() {
            end = decode(&end)?;
        }
    }
    let cf_name = if file.name.contains(DEFAULT_CF_NAME) {
        DEFAULT_CF_NAME.to_owned()
    } else if file.name.contains(WRITE_CF_NAME) {
        WRITE_CF_NAME.to_owned()
    } else {
        file.cf.clone()
    };
    let mut range_start = region_rule.new_key_prefix.clone();
    if range_start < start {
        range_start = start;
    }
    let mut range_end = region_rule.new_key_prefix.clone();
    range_end.extend_from_slice(&[0xff; 10]);
    if !end.is_empty() && range_end > end {
        range_end = end;
    }
    assert!(
        range_start <= range_end,
        "range start exceed range end: file {}, startKey {}, endKey {}",
        file.name,
        tidb_redact::key(&range_start),
        tidb_redact::key(&range_end)
    );
    Ok(import_sstpb::SstMeta {
        uuid: uuid::Uuid::new_v4().as_bytes().to_vec(),
        cf_name,
        range: Some(import_sstpb::Range {
            start: range_start,
            end: range_end,
        }),
        length: file.size,
        region_id: region.id,
        region_epoch: region.region_epoch,
        cipher_iv: file.cipher_iv.clone(),
        ..Default::default()
    })
}

/// Go `isDecryptSstErr`.
fn is_decrypt_sst_err(err: &Error) -> bool {
    let msg = err.to_string();
    msg.contains("Engine Engine") && msg.contains("Corruption: Bad table magic number")
}

impl FileImporter for SnapFileImporter {
    /// Go `Import`: each region the files cover downloaded and ingested,
    /// all retried until the regions take them.
    fn import(&self, ctx: &Context, backup_file_sets: &[BackupFileSet]) -> Result<()> {
        let mut delay_cbs = Vec::new();
        for (i, cb) in self
            .before_ingest_callbacks
            .read()
            .unwrap()
            .iter()
            .enumerate()
        {
            match cb(ctx, backup_file_sets) {
                Err(err) => {
                    return Err(errors::annotate(
                        err,
                        format!("failed to executing the callback #{i}"),
                    ))
                }
                Ok(Some(d)) => delay_cbs.push(d),
                Ok(None) => {}
            }
        }
        let import_begin = Instant::now();
        let (start_key, end_key) = self.get_key_range_for_files(backup_file_sets)?;
        let res = with_retry(
            ctx,
            || {
                let regions = self.paginate_scan_region(ctx, &start_key, &end_key)?;
                // Go sizes its worker pool by the regions only for the merged
                // and latest-MVCC downloads; for TiDBFull the pool has one
                // worker, so the regions go one after another and the first
                // error ends the round.
                tidb_log::debug(
                    "scan regions",
                    &[
                        Field::new("start key", Value::Str(tidb_redact::key_owned(&start_key))),
                        Field::new("end key", Value::Str(tidb_redact::key_owned(&end_key))),
                        Field::new("count", Value::I64(regions.len() as i64)),
                        Field::new("pool size", Value::I64(1)),
                    ],
                );
                for info in &regions {
                    let metas = self
                        .download(ctx, info, backup_file_sets)
                        .inspect_err(|err| {
                            tidb_log::warn(
                                "download file failed, retry later",
                                &[
                                    region_field(&info.region),
                                    Field::new("error", Value::Str(err.to_string())),
                                ],
                            );
                        })?;
                    self.ingest(ctx, info, metas).inspect_err(|err| {
                        tidb_log::warn(
                            "ingest file failed, retry later",
                            &[
                                region_field(&info.region),
                                Field::new("error", Value::Str(err.to_string())),
                            ],
                        );
                    })?;
                }
                Ok(())
            },
            &mut verbose_retry(new_import_sst_backoff_strategy()),
        );
        if let Err(err) = res {
            tidb_log::error(
                "import sst file failed after retry, stop the whole progress",
                &[
                    zap_batch_backup_file_set(backup_file_sets),
                    Field::new("error", Value::Str(err.to_string())),
                ],
            );
            return Err(err);
        }
        tidb_log::debug(
            "import files done",
            &[Field::new(
                "take",
                Value::Duration(import_begin.elapsed().as_nanos() as i64),
            )],
        );
        for (i, cb) in delay_cbs.into_iter().enumerate() {
            cb().map_err(|err| {
                errors::annotate(err, format!("failed to execute the delaied callback #{i}"))
            })?;
        }
        for files in backup_file_sets {
            for f in &files.sst_files {
                crate::summary::collect_success_unit(
                    crate::summary::TOTAL_KV,
                    1,
                    crate::summary::SuccessArg::Data(f.total_kvs),
                );
                crate::summary::collect_success_unit(
                    crate::summary::TOTAL_BYTES,
                    1,
                    crate::summary::SuccessArg::Data(f.total_bytes),
                );
            }
        }
        Ok(())
    }

    /// Go `Close`: the close callbacks (failures logged), then the
    /// connections.
    fn close(&self) -> Result<()> {
        let cbs: Vec<ImporterCallback> = std::mem::take(&mut *self.close_callbacks.lock().unwrap());
        for f in cbs {
            if let Err(err) = f(self) {
                tidb_log::warn(
                    "failed on close snap importer",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
            }
        }
        self.import_client.close_grpc_client()
    }
}

impl BalancedFileImporter for SnapFileImporter {
    /// Go `PauseForBackpressure`.
    fn pause_for_backpressure(&self) {
        let mut guard = self.cond.0.lock().unwrap();
        while self.should_block() {
            guard = self.cond.1.wait(guard).unwrap();
        }
    }
}
