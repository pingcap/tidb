// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/region_job.go`: peer writes and leader ingest.
use crate::{
    Error, Result, engine::Engine, local::BackendConfig, rate_limiter::IngestLimiter,
    rate_limiter_param,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tidb_gocontext::Context;
use tidb_proto::{errorpb, import_sstpb as sst, kvrpcpb, metapb};
use tikv_client::request::{EncodeKeyspace, KeyMode};

/// Go `split.RegionInfo`.
#[derive(Clone, Debug)]
pub struct RegionInfo {
    /// Region metadata with raw logical range bounds.
    pub region: metapb::Region,
    /// Current leader.
    pub leader: Option<metapb::Peer>,
}
/// Go `importClientFactory` and `split.SplitClient` operations used by jobs.
/// The production implementation below uses PD and the generated ImportSST client.
pub trait ImportClient: Send + Sync {
    /// Go `SplitClient.GetRegion`.
    fn get_region(&self, key: &[u8]) -> Result<RegionInfo>;
    /// Go region-cache invalidation before rescanning changed peers or epochs.
    fn invalidate_region(&self, region: &RegionInfo);
    /// Go `ImportSST.Write` to one peer, including the metadata first.
    fn write(&self, store_id: u64, requests: Vec<sst::WriteRequest>) -> Result<sst::WriteResponse>;
    /// Go `ImportSST.MultiIngest`.
    fn multi_ingest(
        &self,
        store_id: u64,
        request: sst::MultiIngestRequest,
    ) -> Result<sst::IngestResponse>;
    /// Go `ImportSST.Ingest` for older servers.
    fn ingest(&self, store_id: u64, request: sst::IngestRequest) -> Result<sst::IngestResponse>;
    /// Go `ImportSST.DuplicateDetect`, consuming the streamed responses.
    fn duplicate_detect(
        &self,
        store_id: u64,
        request: sst::DuplicateDetectRequest,
        consume: &mut (dyn FnMut(sst::DuplicateDetectResponse) -> Result<()> + Send),
    ) -> Result<()>;
    /// Go `tikvCodec.GetAPIVersion`.
    fn api_version(&self) -> i32;
    /// Go `tikvCodec.EncodeKey`, before the import protocol's memcomparable encoding.
    fn encode_key(&self, key: &[u8]) -> Vec<u8>;
    /// Go `Backend.GetTS` (`pdCli.GetTS`), composed into one TSO.
    fn get_ts(&self) -> Result<u64> {
        Err(Error::Other("this import client cannot allocate a timestamp".into()))
    }
    /// Go `pdHTTPCli.GetStores`: each store's address and Go `StoreState`
    /// value (Up 0, Offline 1, Tombstone 2).
    fn get_stores(&self) -> Result<Vec<(String, i64)>> {
        Err(Error::Other("this import client has no PD HTTP client".into()))
    }
    /// Go `tikv.SwitchMode`: `ImportSST.SwitchMode` on the store at
    /// `address` over a new connection.
    fn switch_mode(&self, address: &str, request: sst::SwitchModeRequest) -> Result<()> {
        let _ = (address, request);
        Err(Error::Other("this import client cannot switch TiKV mode".into()))
    }
    /// Go `pdHTTPCli.SetRegionLabelRule`.
    fn set_region_label_rule(&self, rule: &tikv_client::pd_http::LabelRule) -> Result<()> {
        let _ = rule;
        Err(Error::Other("this import client has no PD HTTP client".into()))
    }
    /// Go `pdHTTPCli.PatchRegionLabelRules`.
    fn patch_region_label_rules(&self, patch: &tikv_client::pd_http::LabelRulePatch) -> Result<()> {
        let _ = patch;
        Err(Error::Other("this import client has no PD HTTP client".into()))
    }
}
/// Go `importClientFactory`, over the connected store's PD client and runtime.
pub struct RemoteImportClient<S: tikv_client::PdClient> {
    store: tidb_txnkv::driver::tikv_store::TikvStore<S>,
    security: tidb_pd_client::ClusterSecurity,
    clients:
        Mutex<HashMap<u64, sst::import_sst_client::ImportSstClient<tonic::transport::Channel>>>,
}
impl<S: tikv_client::PdClient> RemoteImportClient<S> {
    /// Go `newImportClientFactory`.
    pub fn new(
        store: tidb_txnkv::driver::tikv_store::TikvStore<S>,
        security: tidb_pd_client::ClusterSecurity,
    ) -> Self {
        Self {
            store,
            security,
            clients: Mutex::new(HashMap::new()),
        }
    }
    fn client(
        &self,
        store_id: u64,
    ) -> Result<sst::import_sst_client::ImportSstClient<tonic::transport::Channel>> {
        if let Some(client) = self.clients.lock().unwrap().get(&store_id) {
            return Ok(client.clone());
        }
        let pd = self.store.store().pd_client();
        let stores = self
            .store
            .block_on(async move { pd.get_all_stores().await })
            .map_err(|e| Error::Other(e.to_string()))?;
        let address = stores
            .into_iter()
            .find(|s| s.id == store_id)
            .ok_or_else(|| Error::Other(format!("store {store_id} not found")))?
            .address;
        let client = self.connect(&address)?;
        self.clients
            .lock()
            .unwrap()
            .insert(store_id, client.clone());
        Ok(client)
    }
}
impl<S: tikv_client::PdClient> RemoteImportClient<S> {
    /// An ImportSST client of the store at `address`.
    fn connect(&self, address: &str) -> Result<sst::import_sst_client::ImportSstClient<tonic::transport::Channel>> {
        let endpoint = tidb_pd_client::secure_endpoint(address, &self.security)
            .map_err(|e| Error::Other(e.to_string()))?
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(600));
        let channel = self
            .store
            .block_on(endpoint.connect())
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(sst::import_sst_client::ImportSstClient::new(channel).max_decoding_message_size(usize::MAX))
    }

    fn pd_http(&self) -> Result<tikv_client::pd_http::Client> {
        self.store
            .pd_http_client()
            .ok_or_else(|| Error::Other("the store has no PD HTTP client".into()))
    }
}
impl<S: tikv_client::PdClient> ImportClient for RemoteImportClient<S> {
    fn get_stores(&self) -> Result<Vec<(String, i64)>> {
        let pd = self.pd_http()?;
        let stores = self
            .store
            .block_on(async move { pd.get_stores().await })
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(stores.stores.into_iter().map(|s| (s.store.address, s.store.state)).collect())
    }
    fn switch_mode(&self, address: &str, request: sst::SwitchModeRequest) -> Result<()> {
        let mut client = self.connect(address)?;
        match self.store.block_on(async move { client.switch_mode(request).await }) {
            Ok(_) => Ok(()),
            // Go `ignoreUnimplementedError`: a TiFlash store.
            Err(status) if status.code() == tonic::Code::Unimplemented => Ok(()),
            Err(status) => Err(Error::Other(status.to_string())),
        }
    }
    fn set_region_label_rule(&self, rule: &tikv_client::pd_http::LabelRule) -> Result<()> {
        let pd = self.pd_http()?;
        let rule = rule.clone();
        self.store
            .block_on(async move { pd.set_region_label_rule(&rule).await })
            .map_err(|e| Error::Other(e.to_string()))
    }
    fn patch_region_label_rules(&self, patch: &tikv_client::pd_http::LabelRulePatch) -> Result<()> {
        let pd = self.pd_http()?;
        let patch = patch.clone();
        self.store
            .block_on(async move { pd.patch_region_label_rules(&patch).await })
            .map_err(|e| Error::Other(e.to_string()))
    }
    fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
        let pd = self.store.store().pd_client();
        let key = tikv_client::Key::from(self.encode_key(key));
        let region = self
            .store
            .block_on(async move { pd.region_for_key(&key).await })
            .map_err(|e| Error::Other(e.to_string()))?;
        let mut metadata = region.region;
        if let tikv_client::request::Keyspace::Enable { keyspace_id } =
            self.store.store().keyspace()
        {
            let codec = tikv_client::request::ApiV2Codec::new(KeyMode::Txn, keyspace_id)
                .map_err(|e| Error::Other(e.to_string()))?;
            (metadata.start_key, metadata.end_key) = codec
                .decode_range(&metadata.start_key, &metadata.end_key)
                .map_err(|e| Error::Other(e.to_string()))?;
        }
        Ok(RegionInfo {
            region: metadata,
            leader: region.leader,
        })
    }
    fn invalidate_region(&self, region: &RegionInfo) {
        let epoch = region.region.region_epoch.unwrap_or_default();
        let ver = tikv_client::RegionVerId {
            id: region.region.id,
            conf_ver: epoch.conf_ver,
            ver: epoch.version,
        };
        let pd = self.store.store().pd_client();
        self.store
            .block_on(async move { pd.invalidate_region_cache(ver).await });
    }
    fn write(&self, id: u64, requests: Vec<sst::WriteRequest>) -> Result<sst::WriteResponse> {
        let mut client = self.client(id)?;
        self.store
            .block_on(async move { client.write(tokio_stream::iter(requests)).await })
            .map(|r| r.into_inner())
            .map_err(|e| Error::Other(e.to_string()))
    }
    fn multi_ingest(
        &self,
        id: u64,
        request: sst::MultiIngestRequest,
    ) -> Result<sst::IngestResponse> {
        let mut client = self.client(id)?;
        self.store
            .block_on(async move { client.multi_ingest(request).await })
            .map(|r| r.into_inner())
            .map_err(|e| Error::Other(e.to_string()))
    }
    fn ingest(&self, id: u64, request: sst::IngestRequest) -> Result<sst::IngestResponse> {
        let mut client = self.client(id)?;
        self.store
            .block_on(async move { client.ingest(request).await })
            .map(|r| r.into_inner())
            .map_err(|e| Error::Other(e.to_string()))
    }
    fn duplicate_detect(
        &self,
        id: u64,
        request: sst::DuplicateDetectRequest,
        consume: &mut (dyn FnMut(sst::DuplicateDetectResponse) -> Result<()> + Send),
    ) -> Result<()> {
        let mut client = self.client(id)?;
        self.store.block_on(async move {
            let mut stream = client
                .duplicate_detect(request)
                .await
                .map_err(|e| Error::Other(e.to_string()))?
                .into_inner();
            while let Some(response) = stream
                .message()
                .await
                .map_err(|e| Error::Other(e.to_string()))?
            {
                consume(response)?;
            }
            Ok(())
        })
    }
    fn api_version(&self) -> i32 {
        self.store.store().keyspace().api_version() as i32
    }
    fn encode_key(&self, key: &[u8]) -> Vec<u8> {
        tikv_client::Key::from(key.to_vec())
            .encode_keyspace(self.store.store().keyspace(), KeyMode::Txn)
            .into()
    }
    fn get_ts(&self) -> Result<u64> {
        self.store
            .get_timestamp_async()
            .wait()
            .map_err(Error::Other)
    }
}
/// Go `newWriteRequest`.
pub fn new_write_request(
    meta: sst::SstMeta,
    resource_group_name: &str,
    task_type: &str,
) -> sst::WriteRequest {
    sst::WriteRequest {
        context: Some(kvrpcpb::Context {
            resource_control_context: Some(kvrpcpb::ResourceControlContext {
                resource_group_name: resource_group_name.into(),
                ..Default::default()
            }),
            request_source: format!("internal_lightning_{task_type}"),
            txn_source: 1 << 16,
            ..Default::default()
        }),
        chunk: Some(sst::write_request::Chunk::Meta(meta)),
    }
}
/// Go `Backend.doImport`, bounded by the region's range and split thresholds.
pub fn do_import(
    ctx: &Context,
    engine: &Engine,
    client: &dyn ImportClient,
    cfg: &BackendConfig,
    split_size: u64,
    split_keys: usize,
) -> Result<()> {
    let limiter = IngestLimiter::new(
        ctx.clone(),
        rate_limiter_param::get_max_ingest_concurrency() as usize,
        rate_limiter_param::get_max_ingest_per_sec(),
    );
    let mut start = Vec::new();
    loop {
        if let Some(e) = ctx.err() {
            return Err(Error::Other(e.to_string()));
        }
        let mut probe = engine.new_iter(&start, &[])?;
        let Some((first, _)) = probe.next_pair()? else {
            return Ok(());
        };
        drop(probe);
        let mut last_error = None;
        let mut completed = None;
        for _ in 0..20 {
            let region = client.get_region(&first)?;
            let mut it = engine.new_iter(&first, &region.region.end_key)?;
            let mut pairs = Vec::new();
            let mut bytes = 0u64;
            let mut next = Vec::new();
            while let Some((key, value)) = it.next_pair()? {
                if !pairs.is_empty() && (bytes >= split_size || pairs.len() >= split_keys) {
                    next = key;
                    break;
                }
                bytes += (key.len() + value.len()) as u64;
                pairs.push(sst::Pair {
                    key,
                    value,
                    ..Default::default()
                });
            }
            if pairs.is_empty() {
                return Err(Error::Other("region does not contain ingest key".into()));
            }
            if next.is_empty() {
                next = region.region.end_key.clone();
            }
            match write_and_ingest(ctx, client, cfg, &limiter, &region, &pairs, engine.get_ts()) {
                Ok(()) => {
                    engine.add_imported_kvs(pairs.len() as i64);
                    completed = Some(next);
                    break;
                }
                Err(e) => {
                    if tidb_ingestor_errdef::is_kv_disk_full_error(&e) {
                        return Err(e);
                    }
                    client.invalidate_region(&region);
                    last_error = Some(e);
                    if ctx.sleep(Duration::from_secs(1)) {
                        return Err(Error::Other(ctx.err().unwrap().to_string()));
                    }
                }
            }
        }
        match completed {
            Some(next) if next.is_empty() => return Ok(()),
            Some(next) => start = next,
            None => return Err(last_error.unwrap()),
        }
    }
}
/// Go `Backend.doWrite` followed by `Backend.ingest`.
fn write_and_ingest(
    ctx: &Context,
    client: &dyn ImportClient,
    cfg: &BackendConfig,
    limiter: &IngestLimiter,
    region: &RegionInfo,
    pairs: &[sst::Pair],
    ts: u64,
) -> Result<()> {
    if ts == 0 {
        return Err(Error::Other("data commitTS is 0".into()));
    }
    let leader = region.leader.as_ref().ok_or_else(|| {
        Error::Ingest(
            tidb_ingestor_errdef::ERR_NO_LEADER
                .gen_with_stack_by_args(&[(region.region.id as i64).into()]),
        )
    })?;
    let mut first = Vec::new();
    tidb_codec::encode_bytes(&mut first, &client.encode_key(&pairs[0].key));
    let mut last = Vec::new();
    tidb_codec::encode_bytes(&mut last, &client.encode_key(&pairs[pairs.len() - 1].key));
    let meta = sst::SstMeta {
        uuid: uuid::Uuid::new_v4().as_bytes().to_vec(),
        region_id: region.region.id,
        region_epoch: region.region.region_epoch,
        range: Some(sst::Range {
            start: first,
            end: last,
        }),
        api_version: client.api_version(),
        ..Default::default()
    };
    let mut requests = vec![new_write_request(
        meta,
        &cfg.resource_group_name,
        &cfg.task_type,
    )];
    let mut batch = Vec::new();
    let mut size = 0;
    for pair in pairs {
        let pair = sst::Pair {
            key: client.encode_key(&pair.key),
            value: pair.value.clone(),
            ..Default::default()
        };
        size += pair.key.len() + pair.value.len();
        batch.push(pair);
        if size >= cfg.kv_write_batch_size {
            requests.push(sst::WriteRequest {
                context: requests[0].context.clone(),
                chunk: Some(sst::write_request::Chunk::Batch(sst::WriteBatch {
                    commit_ts: ts,
                    pairs: std::mem::take(&mut batch),
                })),
            });
            size = 0;
        }
    }
    if !batch.is_empty() {
        requests.push(sst::WriteRequest {
            context: requests[0].context.clone(),
            chunk: Some(sst::write_request::Chunk::Batch(sst::WriteBatch {
                commit_ts: ts,
                pairs: batch,
            })),
        });
    }
    let mut metas = Vec::new();
    for peer in &region.region.peers {
        let response = client.write(peer.store_id, requests.clone())?;
        if let Some(error) = response.error {
            return Err(Error::Other(format!("resp error: {}", error.message)));
        }
        if peer.id == leader.id {
            metas = response.metas;
        }
    }
    if metas.is_empty() {
        return Err(Error::Ingest(
            tidb_ingestor_errdef::ERR_NO_LEADER
                .gen_with_stack_by_args(&[(region.region.id as i64).into()]),
        ));
    }
    let mut last_error = None;
    for _ in 0..20 {
        if let Some(e) = ctx.err() {
            return Err(Error::Other(e.to_string()));
        }
        match do_ingest(ctx, client, cfg, limiter, region, &mut metas) {
            Ok(()) => return Ok(()),
            Err(e) => {
                if get_next_stage_on_ingest_error(&e) != JobStage::Wrote {
                    return Err(e);
                }
                let busy = tidb_ingestor_errdef::ERR_KV_SERVER_IS_BUSY.equal(Some(&e));
                last_error = Some(e);
                if busy && ctx.sleep(Duration::from_secs(1)) {
                    return Err(Error::Other(ctx.err().unwrap().to_string()));
                }
            }
        }
    }
    Err(last_error.unwrap())
}
/// Go `Backend.doIngest`: remove successfully imported SSTs before retrying.
fn do_ingest(
    ctx: &Context,
    client: &dyn ImportClient,
    cfg: &BackendConfig,
    limiter: &IngestLimiter,
    region: &RegionInfo,
    metas: &mut Vec<sst::SstMeta>,
) -> Result<()> {
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/diskFullOnIngest")
        .is_some()
    {
        return Err(ingest_api_error(errorpb::Error {
            message: "propose failed: tikv disk full, cmd diskFullOpt={:?}, leader diskUsage={:?}"
                .into(),
            disk_full: Some(errorpb::DiskFull {
                store_id: vec![1],
                ..Default::default()
            }),
            ..Default::default()
        }));
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/doIngestFailed")
        .is_some()
    {
        return Err(Error::Other("injected error".into()));
    }
    if cfg.should_check_write_stall && limiter.no_limit() {
        for peer in &region.region.peers {
            let response =
                client.multi_ingest(peer.store_id, sst::MultiIngestRequest::default())?;
            if let Some(error) = response.error.filter(|e| e.server_is_busy.is_some()) {
                return Err(ingest_api_error(error));
            }
        }
    }
    let leader = region.leader.as_ref().ok_or_else(|| {
        Error::Ingest(
            tidb_ingestor_errdef::ERR_NO_LEADER
                .gen_with_stack_by_args(&[(region.region.id as i64).into()]),
        )
    })?;
    let request_context = kvrpcpb::Context {
        region_id: region.region.id,
        region_epoch: region.region.region_epoch,
        peer: Some(*leader),
        resource_control_context: Some(kvrpcpb::ResourceControlContext {
            resource_group_name: cfg.resource_group_name.clone(),
            ..Default::default()
        }),
        request_source: format!("internal_lightning_{}", cfg.task_type),
        ..Default::default()
    };
    while !metas.is_empty() {
        if let Some(e) = ctx.err() {
            return Err(Error::Other(e.to_string()));
        }
        let count = metas.len().min(limiter.burst().max(1));
        limiter
            .acquire(leader.store_id, count)
            .map_err(|e| Error::Other(e.to_string()))?;
        let response = client.multi_ingest(
            leader.store_id,
            sst::MultiIngestRequest {
                context: Some(request_context.clone()),
                ssts: metas[..count].to_vec(),
            },
        );
        limiter.release(leader.store_id, count);
        if let Some(error) = response?.error {
            return Err(ingest_api_error(error));
        }
        metas.drain(..count);
    }
    Ok(())
}
/// Go `jobStageTp`, the retry boundary after an import failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStage {
    /// Go `regionScanned`.
    RegionScanned,
    /// Go `wrote`.
    Wrote,
    /// Go `needRescan`.
    NeedRescan,
}
/// Go `getNextStageOnIngestError` (without a replacement region).
pub fn get_next_stage_on_ingest_error(error: &Error) -> JobStage {
    if let Error::Ingest(e) = error {
        if tidb_ingestor_errdef::ERR_KV_INGEST_FAILED.equal(Some(e)) {
            JobStage::RegionScanned
        } else if tidb_ingestor_errdef::ERR_KV_SERVER_IS_BUSY.equal(Some(e)) {
            JobStage::Wrote
        } else {
            JobStage::NeedRescan
        }
    } else {
        JobStage::Wrote
    }
}
/// Go `ingestcli.NewIngestAPIError`, retaining disk-full identity for DXF.
pub fn ingest_api_error(error: errorpb::Error) -> Error {
    use tidb_ingestor_errdef::*;
    let kind = if error.not_leader.is_some() {
        &*ERR_KV_NOT_LEADER
    } else if error.epoch_not_match.is_some() {
        &*ERR_KV_EPOCH_NOT_MATCH
    } else if error.message.contains("raft: proposal dropped") {
        &*ERR_KV_RAFT_PROPOSAL_DROPPED
    } else if error.server_is_busy.is_some() {
        &*ERR_KV_SERVER_IS_BUSY
    } else if error.region_not_found.is_some() {
        &*ERR_KV_REGION_NOT_FOUND
    } else if error.read_index_not_ready.is_some() {
        &*ERR_KV_READ_INDEX_NOT_READY
    } else if error.disk_full.is_some() {
        &*ERR_KV_DISK_FULL
    } else {
        &*ERR_KV_INGEST_FAILED
    };
    let message = if kind.equal(Some(&*ERR_KV_INGEST_FAILED)) {
        get_ingest_failed_msg(&error)
    } else {
        error.message
    };
    Error::Ingest(kind.gen_with_stack(message))
}
/// Go `ingestcli.getIngestFailedMsg`.
fn get_ingest_failed_msg(error: &errorpb::Error) -> String {
    let tp = if error.key_not_in_region.is_some() {
        "KeyNotInRegion"
    } else if error.stale_command.is_some() {
        "StaleCommand"
    } else if error.store_not_match.is_some() {
        "StoreNotMatch"
    } else if error.raft_entry_too_large.is_some() {
        "RaftEntryTooLarge"
    } else if error.max_timestamp_not_synced.is_some() {
        "MaxTimestampNotSynced"
    } else if error.proposal_in_merging_mode.is_some() {
        "ProposalInMergingMode"
    } else if error.data_is_not_ready.is_some() {
        "DataIsNotReady"
    } else if error.region_not_initialized.is_some() {
        "RegionNotInitialized"
    } else if error.recovery_in_progress.is_some() {
        "RecoveryInProgress"
    } else if error.flashback_in_progress.is_some() {
        "FlashbackInProgress"
    } else if error.flashback_not_prepared.is_some() {
        "FlashbackNotPrepared"
    } else if error.is_witness.is_some() {
        "IsWitness"
    } else if error.mismatch_peer_id.is_some() {
        "MismatchPeerId"
    } else if error.bucket_version_not_match.is_some() {
        "BucketVersionNotMatch"
    } else if error.undetermined_result.is_some() {
        "UndeterminedResult"
    } else {
        ""
    };
    match (tp.is_empty(), error.message.is_empty()) {
        (true, _) => error.message.clone(),
        (false, true) => tp.into(),
        (false, false) => format!("{tp} {}", error.message),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    /// Go `region_job_test.go:TestNewWriteRequest`.
    #[test]
    fn test_new_write_request() {
        assert_eq!(
            new_write_request(sst::SstMeta::default(), "", "")
                .context
                .unwrap()
                .txn_source,
            1 << 16
        );
    }
    /// Go `region_job_test.go:TestGetNextStageOnIngestError`, cases with no replacement region.
    #[test]
    fn test_get_next_stage_on_ingest_error() {
        use tidb_ingestor_errdef::*;
        assert_eq!(
            get_next_stage_on_ingest_error(&Error::Other("DNS timeout".into())),
            JobStage::Wrote
        );
        for (kind, stage) in [
            (&*ERR_KV_NOT_LEADER, JobStage::NeedRescan),
            (&*ERR_KV_EPOCH_NOT_MATCH, JobStage::NeedRescan),
            (&*ERR_KV_RAFT_PROPOSAL_DROPPED, JobStage::NeedRescan),
            (&*ERR_KV_SERVER_IS_BUSY, JobStage::Wrote),
            (&*ERR_KV_REGION_NOT_FOUND, JobStage::NeedRescan),
            (&*ERR_KV_READ_INDEX_NOT_READY, JobStage::NeedRescan),
            (&*ERR_KV_INGEST_FAILED, JobStage::RegionScanned),
        ] {
            assert_eq!(
                get_next_stage_on_ingest_error(&Error::Ingest(kind.gen_with_stack(""))),
                stage
            );
        }
    }
    /// Go `ingestcli/ingest_err_test.go:TestConvertPBError2Error`.
    #[test]
    fn test_convert_pb_error2_error() {
        use tidb_ingestor_errdef::*;
        for (error, kind) in [
            (
                errorpb::Error {
                    not_leader: Some(Default::default()),
                    ..Default::default()
                },
                &*ERR_KV_NOT_LEADER,
            ),
            (
                errorpb::Error {
                    epoch_not_match: Some(Default::default()),
                    ..Default::default()
                },
                &*ERR_KV_EPOCH_NOT_MATCH,
            ),
            (
                errorpb::Error {
                    message: "raft: proposal dropped".into(),
                    ..Default::default()
                },
                &*ERR_KV_RAFT_PROPOSAL_DROPPED,
            ),
            (
                errorpb::Error {
                    server_is_busy: Some(Default::default()),
                    ..Default::default()
                },
                &*ERR_KV_SERVER_IS_BUSY,
            ),
            (
                errorpb::Error {
                    region_not_found: Some(Default::default()),
                    ..Default::default()
                },
                &*ERR_KV_REGION_NOT_FOUND,
            ),
            (
                errorpb::Error {
                    read_index_not_ready: Some(Default::default()),
                    ..Default::default()
                },
                &*ERR_KV_READ_INDEX_NOT_READY,
            ),
            (
                errorpb::Error {
                    disk_full: Some(Default::default()),
                    ..Default::default()
                },
                &*ERR_KV_DISK_FULL,
            ),
            (
                errorpb::Error {
                    stale_command: Some(Default::default()),
                    ..Default::default()
                },
                &*ERR_KV_INGEST_FAILED,
            ),
        ] {
            assert!(kind.equal(Some(&ingest_api_error(error))));
        }
    }
    struct MockImportClient {
        writes: Mutex<Vec<u64>>,
        ingests: Mutex<Vec<u64>>,
        busy: AtomicBool,
    }
    impl ImportClient for MockImportClient {
        fn get_region(&self, _: &[u8]) -> Result<RegionInfo> {
            let peers = (1..=3)
                .map(|id| metapb::Peer {
                    id,
                    store_id: id,
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            Ok(RegionInfo {
                leader: Some(peers[0]),
                region: metapb::Region {
                    id: 1,
                    peers,
                    ..Default::default()
                },
            })
        }
        fn invalidate_region(&self, _: &RegionInfo) {}
        fn write(&self, id: u64, requests: Vec<sst::WriteRequest>) -> Result<sst::WriteResponse> {
            self.writes.lock().unwrap().push(id);
            let Some(sst::write_request::Chunk::Meta(meta)) = requests[0].chunk.clone() else {
                panic!("missing metadata");
            };
            assert!(requests[1..].iter().all(
                |r| matches!(&r.chunk,Some(sst::write_request::Chunk::Batch(b)) if b.commit_ts==42)
            ));
            Ok(sst::WriteResponse {
                metas: vec![meta],
                ..Default::default()
            })
        }
        fn multi_ingest(&self, id: u64, _: sst::MultiIngestRequest) -> Result<sst::IngestResponse> {
            self.ingests.lock().unwrap().push(id);
            Ok(sst::IngestResponse {
                error: if self.busy.swap(false, Ordering::SeqCst) {
                    Some(errorpb::Error {
                        server_is_busy: Some(Default::default()),
                        ..Default::default()
                    })
                } else {
                    None
                },
            })
        }
        fn ingest(&self, _: u64, _: sst::IngestRequest) -> Result<sst::IngestResponse> {
            unreachable!()
        }
        fn duplicate_detect(
            &self,
            _: u64,
            _: sst::DuplicateDetectRequest,
            _: &mut (dyn FnMut(sst::DuplicateDetectResponse) -> Result<()> + Send),
        ) -> Result<()> {
            Ok(())
        }
        fn api_version(&self) -> i32 {
            0
        }
        fn encode_key(&self, key: &[u8]) -> Vec<u8> {
            key.to_vec()
        }
    }
    /// Go `local_test.go:TestPartialWriteIngestBusy`: a busy response must
    /// retry the existing SST, then import the remaining partial range.
    #[test]
    fn test_partial_write_ingest_busy() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::open(dir.path().join("engine"), 42, false, 1024).unwrap();
        engine
            .append_rows(
                0,
                &[
                    (b"a".to_vec(), b"a".to_vec(), vec![]),
                    (b"a2".to_vec(), b"a2".to_vec(), vec![]),
                ],
            )
            .unwrap();
        engine.flush().unwrap();
        let client = MockImportClient {
            writes: Mutex::new(vec![]),
            ingests: Mutex::new(vec![]),
            busy: AtomicBool::new(true),
        };
        let cfg = BackendConfig {
            local_store_dir: dir.path().into(),
            resource_group_name: String::new(),
            task_type: String::new(),
            worker_concurrency: 1,
            mem_table_size: 1024,
            local_writer_mem_cache_size: 1024,
            dupe_detect_enabled: false,
            kv_write_batch_size: 16 * 1024,
            should_check_write_stall: false,
            pause_pd_scheduler_scope: String::new(),
            raft_kv2_switch_mode_duration: Duration::ZERO,
        };
        do_import(&Context::background(), &engine, &client, &cfg, 1, 960000).unwrap();
        assert_eq!(*client.writes.lock().unwrap(), [1, 2, 3, 1, 2, 3]);
        assert_eq!(*client.ingests.lock().unwrap(), [1, 1, 1]);
    }
    /// Go `ingestcli/ingest_err_test.go:TestGetIngestFailedMsg`.
    #[test]
    fn test_get_ingest_failed_msg() {
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                key_not_in_region: Some(Default::default()),
                ..Default::default()
            }),
            "KeyNotInRegion"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                stale_command: Some(Default::default()),
                ..Default::default()
            }),
            "StaleCommand"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                store_not_match: Some(Default::default()),
                ..Default::default()
            }),
            "StoreNotMatch"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                raft_entry_too_large: Some(Default::default()),
                ..Default::default()
            }),
            "RaftEntryTooLarge"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                max_timestamp_not_synced: Some(Default::default()),
                ..Default::default()
            }),
            "MaxTimestampNotSynced"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                proposal_in_merging_mode: Some(Default::default()),
                ..Default::default()
            }),
            "ProposalInMergingMode"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                data_is_not_ready: Some(Default::default()),
                ..Default::default()
            }),
            "DataIsNotReady"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                region_not_initialized: Some(Default::default()),
                ..Default::default()
            }),
            "RegionNotInitialized"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                recovery_in_progress: Some(Default::default()),
                ..Default::default()
            }),
            "RecoveryInProgress"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                flashback_in_progress: Some(Default::default()),
                ..Default::default()
            }),
            "FlashbackInProgress"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                flashback_not_prepared: Some(Default::default()),
                ..Default::default()
            }),
            "FlashbackNotPrepared"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                is_witness: Some(Default::default()),
                ..Default::default()
            }),
            "IsWitness"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                mismatch_peer_id: Some(Default::default()),
                ..Default::default()
            }),
            "MismatchPeerId"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                bucket_version_not_match: Some(Default::default()),
                ..Default::default()
            }),
            "BucketVersionNotMatch"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                undetermined_result: Some(Default::default()),
                ..Default::default()
            }),
            "UndeterminedResult"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                region_not_initialized: Some(Default::default()),
                message: "the message".into(),
                ..Default::default()
            }),
            "RegionNotInitialized the message"
        );
        assert_eq!(
            get_ingest_failed_msg(&errorpb::Error {
                message: "the message".into(),
                ..Default::default()
            }),
            "the message"
        );
    }
}
