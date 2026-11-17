// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/region_job.go`: peer writes and leader ingest.
use crate::{
    Error, Result, local::BackendConfig, rate_limiter::IngestLimiter,
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
pub trait ImportClient: Send + Sync + crate::localhelper::SplitClient {
    /// Go `SplitClient.GetRegion`.
    fn get_region(&self, key: &[u8]) -> Result<RegionInfo>;
    /// Go region-cache invalidation before rescanning changed peers or epochs.
    fn invalidate_region(&self, region: &RegionInfo);
    /// Go `ImportSST.Write` to one peer, including the metadata first.
    fn write(&self, store_id: u64, requests: Vec<sst::WriteRequest>) -> Result<sst::WriteResponse>;
    /// Go `Backend.doWrite`: throttle each batch before sending it to a peer.
    fn write_limited(
        &self,
        ctx: &Context,
        store_id: u64,
        requests: Vec<sst::WriteRequest>,
        limiter: &crate::localhelper::StoreWriteLimiter,
    ) -> Result<sst::WriteResponse> {
        for request in &requests {
            if let Some(sst::write_request::Chunk::Batch(batch)) = &request.chunk {
                limiter.wait_n(
                    ctx,
                    store_id,
                    batch
                        .pairs
                        .iter()
                        .map(|p| p.key.len() + p.value.len())
                        .sum(),
                )?;
            }
        }
        self.write(store_id, requests)
    }
    /// Go `checkMultiIngestSupport`, initialized by `NewBackend`.
    fn check_multi_ingest_support(&self, _: &Context) -> Result<bool> {
        Ok(true)
    }
    /// Go `local.pdCli.GetTS` composed into a timestamp.
    fn get_ts(&self) -> Result<u64> {
        Err(Error::Other("the import client has no PD".into()))
    }
    /// Go `Backend.supportMultiIngest` after the capability probe.
    fn supports_multi_ingest(&self) -> bool {
        true
    }
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
    /// Go `pdCli.GetAllStores(ctx, opt.WithExcludeTombstone())`.
    fn all_stores(&self, _: &Context) -> Result<Vec<metapb::Store>> {
        Ok(Vec::new())
    }
    /// Go `getSplitConfFromStore`: TiKV's `coprocessor.region-split-size`
    /// and `region-split-keys` from its status port's `/config`.
    fn split_conf_from_store(&self, _: &Context, host: &str) -> Result<(i64, i64)> {
        Err(Error::Other(format!("no status port at {host}")))
    }
    /// Go `ImportSSTClient.AddForcePartitionRange` on one store.
    fn add_force_partition_range(
        &self,
        _store_id: u64,
        _: sst::AddPartitionRangeRequest,
    ) -> Result<()> {
        Ok(())
    }
    /// Go `ImportSSTClient.RemoveForcePartitionRange` on one store.
    fn remove_force_partition_range(
        &self,
        _store_id: u64,
        _: sst::RemovePartitionRangeRequest,
    ) -> Result<()> {
        Ok(())
    }
    /// Go `Backend.pdHTTPCli`, as `PauseSchedulersByKeyRange` calls it.
    fn pd_http_client(&self) -> Option<std::sync::Arc<dyn tidb_br::pdutil::LabelRuleClient>> {
        None
    }
    /// Go `tikvCodec.GetAPIVersion`.
    fn api_version(&self) -> i32;
    /// Go `tikvCodec.EncodeKey`, before the import protocol's memcomparable encoding.
    fn encode_key(&self, key: &[u8]) -> Vec<u8>;
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
}
/// Go `importClientFactory`, over the connected store's PD client and runtime.
pub struct RemoteImportClient<S: tikv_client::PdClient> {
    store: tidb_txnkv::driver::tikv_store::TikvStore<S>,
    security: tidb_pd_client::ClusterSecurity,
    support_multi_ingest: std::sync::OnceLock<bool>,
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
            support_multi_ingest: std::sync::OnceLock::new(),
            clients: Mutex::new(HashMap::new()),
        }
    }
    /// Go `importClientFactoryImpl.create`.
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
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))?;
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
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))?
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(600));
        let channel = self
            .store
            .block_on(endpoint.connect())
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))?;
        Ok(sst::import_sst_client::ImportSstClient::new(channel).max_decoding_message_size(usize::MAX))
    }
}
impl<S: tikv_client::PdClient> crate::localhelper::SplitClient for RemoteImportClient<S> {
    /// Go `SplitClient.SplitKeysAndScatter`, through TiKV's split RPC and PD scatter.
    fn split_keys_and_scatter(&self, ctx: &Context, keys: &[Vec<u8>]) -> Result<Vec<u64>> {
        if let Some(error) = ctx.err() {
            return Err(Error::Other(error.to_string()));
        }
        let keys = keys.iter().map(|key| self.encode_key(key)).collect();
        let (ids, result) = self
            .store
            .block_on(self.store.store().split_regions(keys, true, None));
        result.map_err(|error| Error::Other(error.to_string()))?;
        Ok(ids)
    }
    /// Go `SplitClient.WaitRegionsScattered`.
    fn wait_regions_scattered(&self, ctx: &Context, regions: &[u64]) -> Result<usize> {
        let mut remaining = regions.len();
        for id in regions {
            if let Some(error) = ctx.err() {
                return Err(Error::Other(error.to_string()));
            }
            if self
                .store
                .block_on(self.store.store().wait_scatter_region_finish(*id, 120_000))
                .is_ok()
            {
                remaining -= 1;
            }
        }
        Ok(remaining)
    }
}
impl<S: tikv_client::PdClient> ImportClient for RemoteImportClient<S> {
    /// Go `pdHTTPCli.GetStores`: each store's address and state.
    fn get_stores(&self) -> Result<Vec<(String, i64)>> {
        let pd = self
            .store
            .pd_http_client()
            .ok_or_else(|| Error::Other("pd http client is not available".into()))?;
        let stores = self
            .store
            .block_on(async move { pd.get_stores().await })
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(stores.stores.into_iter().map(|s| (s.store.address, s.store.state)).collect())
    }
    /// Go `tikv.SwitchMode` over a new connection to the store.
    fn switch_mode(&self, address: &str, request: sst::SwitchModeRequest) -> Result<()> {
        let mut client = self.connect(address)?;
        match self.store.block_on(async move { client.switch_mode(request).await }) {
            Ok(_) => Ok(()),
            // Go `ignoreUnimplementedError`: a TiFlash store.
            Err(status) if status.code() == tonic::Code::Unimplemented => Ok(()),
            Err(status) => Err(Error::Other(status.to_string())),
        }
    }
    /// Go `pdCli.GetAllStores(ctx, opt.WithExcludeTombstone())`.
    fn all_stores(&self, ctx: &Context) -> Result<Vec<metapb::Store>> {
        if let Some(error) = ctx.err() {
            return Err(Error::Other(error.to_string()));
        }
        let pd = self.store.store().pd_client();
        let stores = self
            .store
            .block_on(async move { pd.get_all_stores().await })
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))?;
        Ok(stores
            .into_iter()
            .filter(|store| store.state != metapb::StoreState::Tombstone as i32)
            .collect())
    }
    /// Go `getSplitConfFromStore`: `tls.WithHost(host).GetJSON(ctx, "/config")`
    /// and `units.FromHumanSize` of the split size.
    fn split_conf_from_store(&self, _: &Context, host: &str) -> Result<(i64, i64)> {
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct Coprocessor {
            #[serde(rename = "region-split-size")]
            region_split_size: String,
            #[serde(rename = "region-split-keys")]
            region_split_keys: i64,
        }
        #[derive(serde::Deserialize, Default)]
        #[serde(default)]
        struct Nested {
            coprocessor: Coprocessor,
        }
        let host = host.trim_start_matches("http://").trim_start_matches("https://");
        let scheme = if self.security.is_tls_enabled() { "https" } else { "http" };
        let client = tidb_store_driver_txn::cluster_http::internal_http_client(&self.security)
            .map_err(Error::Other)?;
        let response = client
            .get(format!("{scheme}://{host}/config"))
            .send()
            .map_err(|error| Error::Other(error.to_string()))?;
        if !response.status().is_success() {
            return Err(Error::Other(format!(
                "get {scheme}://{host}/config failed: {}",
                response.status()
            )));
        }
        let nested: Nested = response.json().map_err(|error| Error::Other(error.to_string()))?;
        let split_size = tidb_config_configtypes::from_human_size(&nested.coprocessor.region_split_size)
            .map_err(Error::Other)?;
        Ok((split_size, nested.coprocessor.region_split_keys))
    }
    /// Go `ImportSSTClient.AddForcePartitionRange`.
    fn add_force_partition_range(
        &self,
        store_id: u64,
        request: sst::AddPartitionRangeRequest,
    ) -> Result<()> {
        let mut client = self.client(store_id)?;
        self.store
            .block_on(async move { client.add_force_partition_range(request).await })
            .map(|_| ())
            .map_err(|e| Error::Other(e.to_string()))
    }
    /// Go `ImportSSTClient.RemoveForcePartitionRange`.
    fn remove_force_partition_range(
        &self,
        store_id: u64,
        request: sst::RemovePartitionRangeRequest,
    ) -> Result<()> {
        let mut client = self.client(store_id)?;
        self.store
            .block_on(async move { client.remove_force_partition_range(request).await })
            .map(|_| ())
            .map_err(|e| Error::Other(e.to_string()))
    }
    /// Go `Backend.pdHTTPCli`.
    fn pd_http_client(&self) -> Option<std::sync::Arc<dyn tidb_br::pdutil::LabelRuleClient>> {
        self.store
            .pd_http_client()
            .map(|client| std::sync::Arc::new(client) as std::sync::Arc<dyn tidb_br::pdutil::LabelRuleClient>)
    }
    fn get_ts(&self) -> Result<u64> {
        self.store
            .get_timestamp_async()
            .wait()
            .map_err(|e| Error::Other(e.to_string()))
    }
    /// Go `split.SplitClient.GetRegion`.
    fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
        let pd = self.store.store().pd_client();
        let key = tikv_client::Key::from(self.encode_key(key));
        let region = self
            .store
            .block_on(async move { pd.region_for_key(&key).await })
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))?;
        let mut metadata = region.region;
        if let tikv_client::request::Keyspace::Enable { keyspace_id } =
            self.store.store().keyspace()
        {
            let codec = tikv_client::request::ApiV2Codec::new(KeyMode::Txn, keyspace_id)
                .map_err(|e| Error::Source(std::sync::Arc::new(e)))?;
            (metadata.start_key, metadata.end_key) = codec
                .decode_range(&metadata.start_key, &metadata.end_key)
                .map_err(|e| Error::Source(std::sync::Arc::new(e)))?;
        }
        Ok(RegionInfo {
            region: metadata,
            leader: region.leader,
        })
    }
    /// Go `Backend.generateJobForRange region-cache invalidation`.
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
    /// Go `ImportSST.Write`.
    fn write(&self, id: u64, requests: Vec<sst::WriteRequest>) -> Result<sst::WriteResponse> {
        let mut client = self.client(id)?;
        self.store
            .block_on(async move { client.write(tokio_stream::iter(requests)).await })
            .map(|r| r.into_inner())
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))
    }
    /// Go `Backend.doWrite`: the bounded stream applies live rate changes
    /// between network batches, rather than delaying and then bursting the file.
    fn write_limited(
        &self,
        ctx: &Context,
        id: u64,
        requests: Vec<sst::WriteRequest>,
        limiter: &crate::localhelper::StoreWriteLimiter,
    ) -> Result<sst::WriteResponse> {
        let mut client = self.client(id)?;
        let (ctx, cancel) = Context::with_cancel(ctx);
        std::thread::scope(|scope| {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let write_ctx = ctx.clone();
            let (stop_watch, stopped) = crossbeam_channel::bounded::<()>(0);
            let (interrupt, interrupted) = tokio::sync::oneshot::channel();
            let watch_ctx = ctx.clone();
            scope.spawn(move || { crossbeam_channel::select! { recv(watch_ctx.done()) -> _ => { let _ = interrupt.send(()); },recv(stopped) -> _ => {} } });
            let producer = scope.spawn(move || -> Result<()> {
                for request in requests {
                    if let Some(sst::write_request::Chunk::Batch(batch)) = &request.chunk {
                        limiter.wait_n(
                            &ctx,
                            id,
                            batch
                                .pairs
                                .iter()
                                .map(|p| p.key.len() + p.value.len())
                                .sum(),
                        )?;
                    }
                    if tx.blocking_send(request).is_err() {
                        break;
                    }
                }
                Ok(())
            });
            let result = self.store.block_on(async move {
                tokio::select! {
                    result = client.write(tokio_stream::wrappers::ReceiverStream::new(rx)) => result.map(|r| r.into_inner()).map_err(|e| Error::Source(std::sync::Arc::new(e))),
                    _ = interrupted => Err(Error::Source(std::sync::Arc::new(write_ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled)))),
                }
            });
            drop(stop_watch);
            cancel.cancel();
            let produced = producer
                .join()
                .map_err(|_| Error::Other("write producer panicked".into()))?;
            match result {
                Err(error) => Err(error),
                Ok(response) => {
                    produced?;
                    Ok(response)
                }
            }
        })
    }
    /// Go `checkMultiIngestSupport`, retaining the result for all region workers.
    fn check_multi_ingest_support(&self, ctx: &Context) -> Result<bool> {
        if let Some(supported) = self.support_multi_ingest.get() {
            return Ok(*supported);
        }
        let pd = self.store.store().pd_client();
        let stores = self
            .store
            .block_on(pd.get_all_stores())
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))?;
        let supported = crate::local::check_multi_ingest_support(ctx, &stores, |id| {
            self.multi_ingest(id, sst::MultiIngestRequest::default())
                .map(|_| ())
        })?;
        let _ = self.support_multi_ingest.set(supported);
        Ok(supported)
    }
    /// Go `Backend.supportMultiIngest`.
    fn supports_multi_ingest(&self) -> bool {
        self.support_multi_ingest.get().copied().unwrap_or(true)
    }
    /// Go `ImportSST.MultiIngest`.
    fn multi_ingest(
        &self,
        id: u64,
        request: sst::MultiIngestRequest,
    ) -> Result<sst::IngestResponse> {
        let mut client = self.client(id)?;
        self.store
            .block_on(async move { client.multi_ingest(request).await })
            .map(|r| r.into_inner())
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))
    }
    /// Go `ImportSST.Ingest`.
    fn ingest(&self, id: u64, request: sst::IngestRequest) -> Result<sst::IngestResponse> {
        let mut client = self.client(id)?;
        self.store
            .block_on(async move { client.ingest(request).await })
            .map(|r| r.into_inner())
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))
    }
    /// Go `ImportSST.DuplicateDetect`.
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
                .map_err(|e| Error::Source(std::sync::Arc::new(e)))?
                .into_inner();
            while let Some(response) = stream
                .message()
                .await
                .map_err(|e| Error::Source(std::sync::Arc::new(e)))?
            {
                consume(response)?;
            }
            Ok(())
        })
    }
    /// Go `tikv.Codec.GetAPIVersion`.
    fn api_version(&self) -> i32 {
        self.store.store().keyspace().api_version() as i32
    }
    /// Go `tikv.Codec.EncodeKey`.
    fn encode_key(&self, key: &[u8]) -> Vec<u8> {
        tikv_client::Key::from(key.to_vec())
            .encode_keyspace(self.store.store().keyspace(), KeyMode::Txn)
            .into()
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
/// An engine error as a backend error: the backend's own error as it was, a
/// Go `*errors.Error` kept as one.
pub(crate) fn engine_error(e: tidb_ingestor_engineapi::Error) -> Error {
    let e = match e.downcast::<Error>() {
        Ok(e) => return *e,
        Err(e) => e,
    };
    if let Some(t) = e.downcast_ref::<tidb_terror::TerrorError>() {
        return Error::Ingest(t.clone());
    }
    match e.downcast_ref::<tidb_ingestor_simplesst::Error>() {
        Some(tidb_ingestor_simplesst::Error::Terror(t)) => return Error::Ingest(t.clone()),
        // Go keeps `ErrFoundDuplicateKeys`'s key and value arguments.
        Some(dup @ tidb_ingestor_simplesst::Error::FoundDuplicateKeys { .. }) => {
            return Error::Source(std::sync::Arc::new(dup.clone()));
        }
        _ => {}
    }
    Error::Other(e.to_string())
}

/// Go `execute.Collector.Processed`: the bytes and pairs written.
pub type Collector = std::sync::Arc<dyn Fn(i64, i64) + Send + Sync>;

/// Go `Backend.doImport` after `prepareAndSendJob`'s split and scatter: the
/// region jobs of every range `engine` loads (`generateAndSendJob`), run by
/// the job workers through the store balancer, the dispatcher and the
/// retryer, over the backend's per-store write limiter.
#[allow(clippy::too_many_arguments)]
pub fn do_import(
    ctx: &Context,
    engine: &mut dyn tidb_ingestor_engineapi::Engine,
    client: &dyn ImportClient,
    cfg: &BackendConfig,
    split_size: u64,
    split_keys: usize,
    write_limiter: &crate::localhelper::StoreWriteLimiter,
    collector: Option<&Collector>,
) -> Result<()> {
    let (ctx, cancel) = Context::with_cancel(ctx);
    let limiter = IngestLimiter::new(
        ctx.clone(),
        rate_limiter_param::get_max_ingest_concurrency() as usize,
        rate_limiter_param::get_max_ingest_per_sec(),
    );
    let tracker = JobTracker::default();
    let generating = tracker.acquire();
    let errors = Mutex::new(None);
    let fail = |error: Error| {
        errors.lock().unwrap().get_or_insert(error);
        cancel.cancel();
    };
    let (to_jobs, jobs) = crossbeam_channel::bounded(0);
    let (to_worker, worker_jobs) = crossbeam_channel::bounded(0);
    let (from_worker, done_jobs) = crossbeam_channel::bounded(0);
    let retryer = RegionJobRetryer::new(&ctx, to_jobs.clone());
    let balancer = StoreBalancer::default();
    let write = |ctx: &Context, job: &mut RegionJob<'_>| {
        let data = job.ingest_data.clone().expect("a region job has its ingest data");
        let mut iter = data.new_iter(&job.key_range.0, &job.key_range.1).map_err(engine_error)?;
        let mut pairs = Vec::new();
        let mut bytes = 0;
        let mut remaining = None;
        while let Some((key, value)) = iter.next_pair().map_err(engine_error)? {
            if !pairs.is_empty() && (bytes >= split_size || pairs.len() >= split_keys) {
                remaining = Some(key);
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
            return Ok(TiKVWriteResult {
                empty_job: true,
                ..Default::default()
            });
        }
        let region = job.region.as_ref().unwrap();
        let mut result = do_write(
            ctx,
            client,
            cfg,
            region,
            &pairs,
            data.get_ts(),
            write_limiter,
            collector,
        )
        .inspect_err(|_| client.invalidate_region(region))?;
        result.remaining_start_key = remaining;
        Ok(result)
    };
    let ingest = |ctx: &Context, job: &mut RegionJob<'_>| {
        let region = job.region.as_ref().unwrap();
        do_ingest(
            ctx,
            client,
            cfg,
            &limiter,
            region,
            &mut job.write_result.as_mut().unwrap().sst_meta,
        )
        .inspect_err(|error| {
            if get_next_stage_on_ingest_error(error) == JobStage::NeedRescan {
                client.invalidate_region(region);
            }
        })
    };
    let pre = |_: &Context, _: &mut RegionJob<'_>| {
        if tidb_failpoint::eval(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/WriteToTiKVNotEnoughDiskSpace",
        )
        .is_some()
        {
            return Err(Error::Ingest(tidb_ingestor_errdef::ERR_KV_DISK_FULL.gen_with_stack("the remaining storage capacity of TiKV is less than 10%; please increase the storage capacity of TiKV and try again")));
        }
        Ok(())
    };
    let after = |peers: &[metapb::Peer]| balancer.release_store_load(peers);
    let regenerate = |ctx: &Context, job: &RegionJob<'static>| {
        generate_region_jobs(
            ctx,
            job.ingest_data.as_ref().expect("a region job has its ingest data"),
            client,
            &job.key_range,
            split_size,
            split_keys as u64,
        )
    };
    let worker = crate::job_worker::RegionJobBaseWorker {
        ctx: ctx.clone(),
        write_fn: &write,
        ingest_fn: &ingest,
        pre_run_job_fn: &pre,
        after_run_job_fn: Some(&after),
        regenerate_jobs_fn: &regenerate,
    };
    std::thread::scope(|scope| {
        scope.spawn(|| retryer.run());
        scope.spawn(|| balancer.run(&ctx, jobs, to_worker));
        scope.spawn(|| {
            if let Err(error) = (Dispatcher {
                ctx: ctx.clone(),
                input: done_jobs,
                retryer: &retryer,
            })
            .run()
            {
                fail(error);
            }
        });
        for _ in 0..cfg.worker_concurrency.max(1) {
            let input = worker_jobs.clone();
            let output = from_worker.clone();
            let worker = &worker;
            let fail = &fail;
            let ctx = &ctx;
            scope.spawn(move || loop {
                let job = crossbeam_channel::select! { recv(ctx.done()) -> _ => return,recv(input) -> job => match job { Ok(job) => job,Err(_) => return } };
                let result = worker.handle_task(job,&mut |job| { crossbeam_channel::select! { recv(ctx.done()) -> _ => {},send(output,job) -> _ => {} } });
                if let Err(error) = result { fail(error);return; }
            });
        }
        let generated = engine.load_ingest_data(&ctx, &mut |batch| {
            tidb_failpoint::inject(
                "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/beforeGenerateJob",
                |_| {},
            );
            if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/sendDummyJob")
                .is_some()
            {
                let dummy = RegionJob::new(
                    RegionInfo {
                        region: metapb::Region::default(),
                        leader: None,
                    },
                    None,
                    vec![],
                    vec![],
                    0,
                    0,
                );
                send_region_jobs(&ctx, &tracker, vec![dummy], &to_jobs);
                ctx.sleep(Duration::from_secs(5));
            }
            // Go `generateJobForRange`: nothing to ingest in the batch.
            let ranges = &batch.sorted_ranges;
            let (start_of_all, end_of_all) = (&ranges[0].start, &ranges[ranges.len() - 1].end);
            if batch.data.get_first_and_last_key(start_of_all, end_of_all)?.is_none() {
                tidb_log::warn(
                    "There is no pairs in range",
                    &[
                        tidb_log::Field::new("startOfAllRanges", tidb_log::Value::Str(hex::encode(start_of_all))),
                        tidb_log::Field::new("endOfAllRanges", tidb_log::Value::Str(hex::encode(end_of_all))),
                    ],
                );
                return Ok(());
            }
            let data: std::sync::Arc<dyn tidb_ingestor_engineapi::IngestData> = batch.data;
            for range in ranges {
                let jobs = generate_region_jobs(
                    &ctx,
                    &data,
                    client,
                    &(range.start.clone(), range.end.clone()),
                    split_size,
                    split_keys as u64,
                )?;
                send_region_jobs(&ctx, &tracker, jobs, &to_jobs);
            }
            Ok(())
        });
        let generated = generated.map_err(engine_error);
        if let Err(error) = generated {
            fail(error);
        }
        drop(generating);
        tracker.wait(&ctx);
        cancel.cancel();
    });
    // The receivers own any buffered jobs not accepted before cancellation.
    drop(worker_jobs);
    drop(from_worker);
    drop(to_jobs);
    if let Some(error) = errors.into_inner().unwrap() {
        return Err(error);
    }
    Ok(())
}
/// Go `Backend.generateAndSendJob`: ref every job before publishing any job;
/// ownership releases unsent references on cancellation.
fn send_region_jobs<'a>(
    ctx: &Context,
    tracker: &JobTracker,
    mut jobs: Vec<RegionJob<'a>>,
    output: &crossbeam_channel::Sender<RegionJob<'a>>,
) {
    for job in &mut jobs {
        job.completion = Some(tracker.acquire());
    }
    for job in jobs {
        crossbeam_channel::select! {
            recv(ctx.done()) -> _ => return,
            send(output, job) -> result => if result.is_err() { return; },
        }
    }
}

/// Go `Backend.generateJobForIngestData`, scanning fresh region metadata for
/// both initial jobs and a stale region's remaining range.
fn generate_region_jobs<'a>(
    ctx: &Context,
    data: &std::sync::Arc<dyn tidb_ingestor_engineapi::IngestData + 'a>,
    client: &dyn ImportClient,
    range: &(Vec<u8>, Vec<u8>),
    size: u64,
    keys: u64,
) -> Result<Vec<RegionJob<'a>>> {
    let mut jobs = Vec::new();
    let mut start = range.0.clone();
    while crate::localhelper::before_end(&start, &range.1) {
        if let Some(error) = ctx.err() {
            return Err(Error::Source(std::sync::Arc::new(error)));
        }
        let mut region = None;
        let mut error = None;
        for attempt in 0..20 {
            match client.get_region(&start) {
                Ok(found) => {
                    region = Some(found);
                    break;
                }
                Err(err) => error = Some(err),
            }
            if attempt != 19 && ctx.sleep(Duration::from_secs(1)) {
                return Err(Error::Source(std::sync::Arc::new(ctx.err().unwrap())));
            }
        }
        let region = region.ok_or_else(|| error.unwrap())?;
        let end = if region.region.end_key.is_empty() {
            range.1.clone()
        } else if range.1.is_empty() {
            region.region.end_key.clone()
        } else {
            region.region.end_key.clone().min(range.1.clone())
        };
        if !end.is_empty() && end <= start {
            return Err(Error::Other("region does not contain ingest key".into()));
        }
        jobs.push(RegionJob::new(
            region,
            Some(std::sync::Arc::clone(data)),
            start,
            end.clone(),
            size,
            keys,
        ));
        if end.is_empty() {
            break;
        }
        start = end;
    }
    Ok(jobs)
}
/// Go `Backend.doWrite`, writing all peers and retaining the leader SST metadata.
fn do_write(
    ctx: &Context,
    client: &dyn ImportClient,
    cfg: &BackendConfig,
    region: &RegionInfo,
    pairs: &[sst::Pair],
    ts: u64,
    write_limiter: &crate::localhelper::StoreWriteLimiter,
    collector: Option<&Collector>,
) -> Result<TiKVWriteResult> {
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
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/mockWritePeerErr").is_some() {
            let epoch = region.region.region_epoch.unwrap_or_default();
            return Err(Error::Other(format!(
                "peer {}, store {}, region {}, epoch conf_ver:{} version:{} , when open write stream: mock write peer error",
                peer.id, peer.store_id, region.region.id, epoch.conf_ver, epoch.version
            )));
        }
        let response = client.write_limited(ctx, peer.store_id, requests.clone(), write_limiter)?;
        if let Some(error) = response.error {
            return Err(Error::Other(format!("resp error: {}", error.message)));
        }
        if peer.id == leader.id {
            metas = response.metas;
        }
    }
    // Go `local.collector.Processed` after each batch reached every peer.
    if let Some(collector) = collector {
        for request in &requests {
            if let Some(sst::write_request::Chunk::Batch(batch)) = &request.chunk {
                let size: usize = batch.pairs.iter().map(|pair| pair.key.len() + pair.value.len()).sum();
                collector(size as i64, batch.pairs.len() as i64);
            }
        }
    }
    if metas.is_empty() {
        return Err(Error::Ingest(
            tidb_ingestor_errdef::ERR_NO_LEADER
                .gen_with_stack_by_args(&[(region.region.id as i64).into()]),
        ));
    }
    Ok(TiKVWriteResult {
        count: pairs.len() as u64,
        total_bytes: pairs
            .iter()
            .map(|pair| (pair.key.len() + pair.value.len()) as u64)
            .sum(),
        sst_meta: metas,
        ..Default::default()
    })
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
    if cfg.should_check_write_stall && limiter.no_limit() && client.supports_multi_ingest() {
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
        let count = if client.supports_multi_ingest() {
            metas.len().min(limiter.burst().max(1))
        } else {
            1
        };
        limiter
            .acquire(leader.store_id, count)
            .map_err(|e| Error::Source(std::sync::Arc::new(e)))?;
        let response = if client.supports_multi_ingest() {
            client.multi_ingest(
                leader.store_id,
                sst::MultiIngestRequest {
                    context: Some(request_context.clone()),
                    ssts: metas[..count].to_vec(),
                },
            )
        } else {
            client.ingest(
                leader.store_id,
                sst::IngestRequest {
                    context: Some(request_context.clone()),
                    sst: Some(metas[0].clone()),
                },
            )
        };
        limiter.release(leader.store_id, count);
        if let Some(error) = response?.error {
            let replacement = error.epoch_not_match.as_ref().and_then(|epoch| {
                epoch
                    .current_regions
                    .iter()
                    .find(|candidate| crate::localhelper::inside_region(candidate, metas))
                    .and_then(|candidate| {
                        candidate
                            .peers
                            .iter()
                            .find(|peer| peer.store_id == leader.store_id)
                            .map(|peer| RegionInfo {
                                region: candidate.clone(),
                                leader: Some(*peer),
                            })
                    })
            });
            let error = ingest_api_error(error);
            return Err(match (error, replacement) {
                (Error::Ingest(error), Some(region)) => Error::IngestRegion { error, region },
                (error, _) => error,
            });
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
    /// Go `ingested`.
    Ingested,
}
/// Go `getNextStageOnIngestError` (without a replacement region).
pub fn get_next_stage_on_ingest_error(error: &Error) -> JobStage {
    if matches!(error, Error::IngestRegion { .. }) {
        return JobStage::RegionScanned;
    }
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
    use crate::engine::Engine;
    use std::sync::atomic::{AtomicBool, Ordering};
    /// Go `doImport` over a local engine, as `ImportEngine` hands it.
    fn do_import(
        ctx: &Context,
        engine: &std::sync::Arc<Engine>,
        client: &dyn ImportClient,
        cfg: &BackendConfig,
        split_size: u64,
        split_keys: usize,
    ) -> Result<()> {
        let mut local = crate::local::LocalEngine::new(std::sync::Arc::clone(engine), split_size, split_keys)?;
        super::do_import(
            ctx,
            &mut local,
            client,
            cfg,
            split_size,
            split_keys,
            &crate::localhelper::StoreWriteLimiter::new(0),
            None,
        )
    }
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
    #[derive(Default)]
    struct MockImportClient {
        writes: Mutex<Vec<u64>>,
        ingests: Mutex<Vec<u64>>,
        busy: AtomicBool,
        not_leader: AtomicBool,
        changed_peers: AtomicBool,
        single_ingest: bool,
        busy_store: Option<u64>,
        region_boundaries: Vec<Vec<u8>>,
        scans: std::sync::atomic::AtomicUsize,
        write_errors: Mutex<HashMap<Vec<u8>, std::collections::VecDeque<Error>>>,
        ingest_errors: Mutex<HashMap<Vec<u8>, std::collections::VecDeque<errorpb::Error>>>,
        region_fail_after: Option<usize>,
    }
    impl crate::localhelper::SplitClient for MockImportClient {
        /// Go `split.SplitClient.SplitKeysAndScatter`.
        fn split_keys_and_scatter(&self, _: &Context, _: &[Vec<u8>]) -> Result<Vec<u64>> {
            Ok(Vec::new())
        }
        /// Go `split.SplitClient.WaitRegionsScattered`.
        fn wait_regions_scattered(&self, _: &Context, _: &[u64]) -> Result<usize> {
            Ok(0)
        }
    }
    impl ImportClient for MockImportClient {
        /// Go `split.SplitClient.GetRegion`.
        fn get_region(&self, key: &[u8]) -> Result<RegionInfo> {
            let scan = self.scans.fetch_add(1, Ordering::SeqCst);
            if self.region_fail_after.is_some_and(|count| scan >= count) {
                return Err(Error::Other("meet error when generateJobForRange".into()));
            }
            let end_key = self
                .region_boundaries
                .iter()
                .find(|end| end.as_slice() > key)
                .cloned()
                .unwrap_or_default();
            let offset = if self.changed_peers.load(Ordering::SeqCst) {
                10
            } else {
                0
            };
            let peers = (1 + offset..=3 + offset)
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
                    end_key,
                    ..Default::default()
                },
            })
        }
        /// Go `Backend.generateJobForRange region-cache invalidation`.
        fn invalidate_region(&self, _: &RegionInfo) {}
        /// Go `ImportSST.Write`.
        fn write(&self, id: u64, requests: Vec<sst::WriteRequest>) -> Result<sst::WriteResponse> {
            self.writes.lock().unwrap().push(id);
            if let Some(sst::write_request::Chunk::Batch(batch)) = &requests[1].chunk {
                if let Some(error) = self
                    .write_errors
                    .lock()
                    .unwrap()
                    .get_mut(&batch.pairs[0].key)
                    .and_then(|errors| errors.pop_front())
                {
                    return Err(error);
                }
            }
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
        /// Go `Backend.supportMultiIngest`.
        fn supports_multi_ingest(&self) -> bool {
            !self.single_ingest
        }
        /// Go `ImportSST.MultiIngest`.
        fn multi_ingest(
            &self,
            id: u64,
            request: sst::MultiIngestRequest,
        ) -> Result<sst::IngestResponse> {
            assert!(!self.single_ingest);
            self.ingests.lock().unwrap().push(id);
            if let Some(range) = request.ssts.first().and_then(|meta| meta.range.as_ref()) {
                let (_, key) = tidb_codec::decode_bytes(&range.start).unwrap();
                if let Some(error) = self
                    .ingest_errors
                    .lock()
                    .unwrap()
                    .get_mut(&key)
                    .and_then(|errors| errors.pop_front())
                {
                    return Ok(sst::IngestResponse { error: Some(error) });
                }
            }
            if !request.ssts.is_empty() && self.not_leader.swap(false, Ordering::SeqCst) {
                self.changed_peers.store(true, Ordering::SeqCst);
                return Ok(sst::IngestResponse {
                    error: Some(errorpb::Error {
                        not_leader: Some(errorpb::NotLeader {
                            leader: Some(metapb::Peer {
                                store_id: 11,
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                });
            }
            Ok(sst::IngestResponse {
                error: if self.busy_store.is_none_or(|store| store == id)
                    && self.busy.swap(false, Ordering::SeqCst)
                {
                    Some(errorpb::Error {
                        server_is_busy: Some(Default::default()),
                        ..Default::default()
                    })
                } else {
                    None
                },
            })
        }
        /// Go `ImportSST.Ingest`.
        fn ingest(&self, id: u64, request: sst::IngestRequest) -> Result<sst::IngestResponse> {
            assert!(self.single_ingest);
            assert!(request.sst.is_some());
            self.ingests.lock().unwrap().push(id);
            Ok(sst::IngestResponse::default())
        }
        /// Go `ImportSST.DuplicateDetect`.
        fn duplicate_detect(
            &self,
            _: u64,
            _: sst::DuplicateDetectRequest,
            _: &mut (dyn FnMut(sst::DuplicateDetectResponse) -> Result<()> + Send),
        ) -> Result<()> {
            Ok(())
        }
        /// Go `tikv.Codec.GetAPIVersion`.
        fn api_version(&self) -> i32 {
            0
        }
        /// Go `tikv.Codec.EncodeKey`.
        fn encode_key(&self, key: &[u8]) -> Vec<u8> {
            key.to_vec()
        }
    }
    /// Go `local_test.go:TestPartialWriteIngestBusy`: a busy response must
    /// retry the existing SST, then import the remaining partial range.
    #[test]
    fn test_partial_write_ingest_busy() {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(dir.path().join("engine"), 42, false, 1024).unwrap());
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
            ..Default::default()
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
            checkpoint_enabled: false,
            pause_pd_scheduler_scope: crate::local::PAUSE_PD_SCHEDULER_SCOPE_GLOBAL.into(),
            raft_kv2_switch_mode_duration: Duration::ZERO,
        };
        do_import(&Context::background(), &engine, &client, &cfg, 1, 960000).unwrap();
        assert_eq!(*client.writes.lock().unwrap(), [1, 2, 3, 1, 2, 3]);
        assert_eq!(*client.ingests.lock().unwrap(), [1, 1, 1]);
    }
    /// Go `TestNotLeaderErrorNeedUpdatePeers` and partial-write test setup.
    fn import_test_config(path: &std::path::Path) -> BackendConfig {
        BackendConfig {
            local_store_dir: path.into(),
            resource_group_name: String::new(),
            task_type: String::new(),
            worker_concurrency: 1,
            mem_table_size: 1024,
            local_writer_mem_cache_size: 1024,
            dupe_detect_enabled: false,
            kv_write_batch_size: 16 * 1024,
            should_check_write_stall: false,
            checkpoint_enabled: false,
            pause_pd_scheduler_scope: crate::local::PAUSE_PD_SCHEDULER_SCOPE_GLOBAL.into(),
            raft_kv2_switch_mode_duration: std::time::Duration::ZERO,
        }
    }
    /// Go `TestNotLeaderErrorNeedUpdatePeers`: write and busy-check the new peers
    /// after rescanning, including replacement of the old leader.
    #[test]
    fn test_not_leader_error_need_update_peers() {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(dir.path().join("engine"), 42, false, 1024).unwrap());
        engine
            .append_rows(0, &[(b"a".to_vec(), b"a".to_vec(), vec![])])
            .unwrap();
        engine.flush().unwrap();
        let client = MockImportClient {
            not_leader: AtomicBool::new(true),
            ..Default::default()
        };
        let mut cfg = import_test_config(dir.path());
        cfg.should_check_write_stall = true;
        do_import(
            &Context::background(),
            &engine,
            &client,
            &cfg,
            96 * 1024 * 1024,
            960_000,
        )
        .unwrap();
        assert_eq!(*client.writes.lock().unwrap(), [1, 2, 3, 11, 12, 13]);
        assert_eq!(
            *client.ingests.lock().unwrap(),
            [1, 2, 3, 1, 11, 12, 13, 11]
        );
        assert_eq!(engine.imported_statistics(), (2, 1));
    }
    /// Go `TestPartialWriteIngestErrorWontPanic`: a not-leader response after a
    /// partial write must rescan without dereferencing the cleared write result.
    #[test]
    fn test_partial_write_ingest_error_wont_panic() {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(dir.path().join("engine"), 42, false, 1024).unwrap());
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
            not_leader: AtomicBool::new(true),
            ..Default::default()
        };
        do_import(
            &Context::background(),
            &engine,
            &client,
            &import_test_config(dir.path()),
            1,
            960_000,
        )
        .unwrap();
        assert_eq!(
            *client.writes.lock().unwrap(),
            [1, 2, 3, 11, 12, 13, 11, 12, 13]
        );
        assert_eq!(*client.ingests.lock().unwrap(), [1, 11, 11]);
        assert_eq!(engine.imported_statistics(), (6, 2));
    }
    /// Go `TestMultiIngest`'s unsupported-cluster fallback through `doIngest`:
    /// single-SST imports remove completed entries and bypass MultiIngest probes.
    #[test]
    fn test_multi_ingest_fallback() {
        let client = MockImportClient {
            single_ingest: true,
            ..Default::default()
        };
        let mut cfg = import_test_config(std::path::Path::new("unused"));
        cfg.should_check_write_stall = true;
        let ctx = Context::background();
        let limiter = IngestLimiter::new(ctx.clone(), 0, 0.0);
        let mut metas = vec![sst::SstMeta::default(); 3];
        do_ingest(
            &ctx,
            &client,
            &cfg,
            &limiter,
            &client.get_region(&[]).unwrap(),
            &mut metas,
        )
        .unwrap();
        assert!(metas.is_empty());
        assert_eq!(*client.ingests.lock().unwrap(), [1, 1, 1]);
    }
    /// Go `TestCtxCancelIsIgnored`: cancellation from a failed worker must not
    /// replace the disk-full cause while the generator prepares its next range.
    #[test]
    fn test_ctx_cancel_is_ignored() {
        let _delay = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/beforeGenerateJob",
            "sleep(1000)",
        );
        let _disk = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/WriteToTiKVNotEnoughDiskSpace",
            "return()",
        );
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(dir.path().join("engine"), 42, false, 1024).unwrap());
        for key in [b'a', b'b', b'c'] {
            engine
                .append_rows(0, &[(vec![key], vec![key], vec![])])
                .unwrap();
            engine.flush().unwrap();
        }
        assert_eq!(
            engine.get_region_split_keys(1, 1).unwrap(),
            [vec![b'a'], vec![b'b'], vec![b'c', 0]]
        );
        let client = MockImportClient {
            region_boundaries: vec![vec![b'b'], vec![b'c']],
            ..Default::default()
        };
        let error = do_import(
            &Context::background(),
            &engine,
            &client,
            &import_test_config(dir.path()),
            1,
            1,
        )
        .unwrap_err();
        assert!(tidb_ingestor_errdef::ERR_KV_DISK_FULL.equal(Some(&error)));
        assert_eq!(
            client.scans.load(Ordering::SeqCst),
            1,
            "cancelled generator must not scan the next range"
        );
    }
    /// Go `TestWorkerFailedWhenGeneratingJobs`: worker failure before the first
    /// PD scan cancels the generator and releases all outstanding references.
    #[test]
    fn test_worker_failed_when_generating_jobs() {
        let _dummy = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/sendDummyJob",
            "return()",
        );
        let _disk = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/WriteToTiKVNotEnoughDiskSpace",
            "return()",
        );
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(dir.path().join("engine"), 42, false, 1024).unwrap());
        engine
            .append_rows(0, &[(b"a".to_vec(), b"a".to_vec(), vec![])])
            .unwrap();
        engine.flush().unwrap();
        let client = MockImportClient::default();
        let error = do_import(
            &Context::background(),
            &engine,
            &client,
            &import_test_config(dir.path()),
            96 * 1024 * 1024,
            960_000,
        )
        .unwrap_err();
        assert!(tidb_ingestor_errdef::ERR_KV_DISK_FULL.equal(Some(&error)));
        assert_eq!(client.scans.load(Ordering::SeqCst), 0);
    }
    /// Go `TestCheckPeersBusy`: follower 12 stalls only its own region; region
    /// 21/22/23 still ingests, and the unfinished range retains its retry count.
    #[test]
    fn test_check_peers_busy() {
        let client = MockImportClient {
            busy: AtomicBool::new(true),
            busy_store: Some(12),
            ..Default::default()
        };
        let mut cfg = import_test_config(std::path::Path::new("unused"));
        cfg.should_check_write_stall = true;
        let ctx = Context::background();
        let limiter = IngestLimiter::new(ctx.clone(), 0, 0.0);
        let write_limiter = crate::localhelper::StoreWriteLimiter::new(0);
        let mut retry = None;
        for (base, start, end) in [(10, b'a', b'b'), (20, b'b', b'c')] {
            let peers: Vec<_> = (1..=3)
                .map(|i| metapb::Peer {
                    id: base + i,
                    store_id: base + i,
                    ..Default::default()
                })
                .collect();
            let region = RegionInfo {
                region: metapb::Region {
                    id: base,
                    peers: peers.clone(),
                    ..Default::default()
                },
                leader: Some(peers[0]),
            };
            let mut result = do_write(
                &ctx,
                &client,
                &cfg,
                &region,
                &[sst::Pair {
                    key: vec![start],
                    value: vec![start],
                    ..Default::default()
                }],
                42,
                &write_limiter,
                None,
            )
            .unwrap();
            let mut job = RegionJob::new(region.clone(), None, vec![start], vec![end], 0, 0);
            job.retry_count = 20;
            job.stage = JobStage::Wrote;
            if let Err(error) =
                do_ingest(&ctx, &client, &cfg, &limiter, &region, &mut result.sst_meta)
            {
                job.last_retryable_error = Some(error);
                retry = Some(job);
            } else {
                assert!(result.sst_meta.is_empty());
            }
        }
        assert_eq!(*client.writes.lock().unwrap(), [11, 12, 13, 21, 22, 23]);
        assert_eq!(*client.ingests.lock().unwrap(), [11, 12, 21, 22, 23, 21]);
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(retry.unwrap()).unwrap();
        drop(tx);
        let (out, _) = crossbeam_channel::bounded(0);
        let retryer = RegionJobRetryer::new(&ctx, out);
        Dispatcher {
            ctx,
            input: rx,
            retryer: &retryer,
        }
        .run()
        .unwrap();
        let queue = retryer.queue.lock().unwrap();
        let job = &queue.2.first_key_value().unwrap().1;
        assert_eq!(job.stage, JobStage::Wrote);
        assert_eq!(job.retry_count, 21);
        assert_eq!(job.key_range, (b"a".to_vec(), b"b".to_vec()));
    }
    /// Go `TestDoImport`: write retry, partial writes, ingest/write rescans,
    /// initial-generation failure, rescan-generation failure and fatal write.
    #[test]
    fn test_do_import() {
        let dir = tempfile::tempdir().unwrap();
        let engine = std::sync::Arc::new(Engine::open(dir.path().join("engine"), 42, false, 1024).unwrap());
        let rows: Vec<_> = ["a", "b", "b2", "c", "c2"]
            .map(|key| (key.as_bytes().to_vec(), key.as_bytes().to_vec(), vec![]))
            .into();
        engine.append_rows(0, &rows).unwrap();
        engine.flush().unwrap();
        let client = MockImportClient {
            region_boundaries: vec![b"b".to_vec(), b"c".to_vec(), b"c2".to_vec(), b"d".to_vec()],
            ..Default::default()
        };
        for (key, message) in [
            (b"a".as_slice(), "RequestTooNew"),
            (b"c2".as_slice(), "is not fully replicated"),
        ] {
            client.write_errors.lock().unwrap().insert(
                key.to_vec(),
                [Error::Source(std::sync::Arc::new(tonic::Status::unknown(
                    message,
                )))]
                .into(),
            );
        }
        client.ingest_errors.lock().unwrap().insert(
            b"c".to_vec(),
            [errorpb::Error {
                epoch_not_match: Some(Default::default()),
                ..Default::default()
            }]
            .into(),
        );
        let mut cfg = import_test_config(dir.path());
        cfg.worker_concurrency = 2;
        do_import(&Context::background(), &engine, &client, &cfg, 1, 960_000).unwrap();
        assert_eq!(engine.imported_statistics().1, 5);
        assert!(
            client
                .write_errors
                .lock()
                .unwrap()
                .values()
                .all(|errors| errors.is_empty())
        );
        assert!(
            client
                .ingest_errors
                .lock()
                .unwrap()
                .values()
                .all(|errors| errors.is_empty())
        );
        for rescan in [false, true] {
            let client = MockImportClient {
                region_fail_after: Some(usize::from(rescan)),
                not_leader: AtomicBool::new(rescan),
                ..Default::default()
            };
            let error =
                do_import(&Context::background(), &engine, &client, &cfg, 1, 960_000).unwrap_err();
            assert_eq!(error.to_string(), "meet error when generateJobForRange");
        }
        let client = MockImportClient::default();
        client
            .write_errors
            .lock()
            .unwrap()
            .insert(b"a".to_vec(), [Error::Other("fatal error".into())].into());
        let error =
            do_import(&Context::background(), &engine, &client, &cfg, 1, 960_000).unwrap_err();
        assert_eq!(error.to_string(), "fatal error");
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


/// Go `tikvWriteResult`.
#[derive(Default, Clone)]
pub struct TiKVWriteResult {
    /// Go `emptyJob`.
    pub empty_job: bool,
    /// Go `count`.
    pub count: u64,
    /// Go `totalBytes`.
    pub total_bytes: u64,
    /// Go `remainingStartKey`.
    pub remaining_start_key: Option<Vec<u8>>,
    /// Go `sstMeta`.
    pub sst_meta: Vec<sst::SstMeta>,
}
/// Go `regionJob`; Rust's lifetime keeps the ingest data alive through retries.
pub struct RegionJob<'a> {
    /// Go `keyRange`.
    pub key_range: (Vec<u8>, Vec<u8>),
    /// Go `region`.
    pub region: Option<RegionInfo>,
    /// Go `stage`.
    pub stage: JobStage,
    /// Go `writeResult`.
    pub write_result: Option<TiKVWriteResult>,
    /// Go `ingestData`.
    pub ingest_data: Option<std::sync::Arc<dyn tidb_ingestor_engineapi::IngestData + 'a>>,
    /// Go `regionSplitSize`.
    pub region_split_size: u64,
    /// Go `regionSplitKeys`.
    pub region_split_keys: u64,
    /// Go `retryCount`.
    pub retry_count: usize,
    /// Go `waitUntil`.
    pub wait_until: std::time::Instant,
    /// Go `lastRetryableErr`.
    pub last_retryable_error: Option<Error>,
    pub(crate) completion: Option<JobRef>,
}
impl<'a> RegionJob<'a> {
    /// Go `newRegionJob`.
    pub fn new(
        region: RegionInfo,
        data: Option<std::sync::Arc<dyn tidb_ingestor_engineapi::IngestData + 'a>>,
        start: Vec<u8>,
        end: Vec<u8>,
        size: u64,
        keys: u64,
    ) -> Self {
        Self {
            key_range: (start, end),
            region: Some(region),
            stage: JobStage::RegionScanned,
            write_result: None,
            ingest_data: data,
            region_split_size: size,
            region_split_keys: keys,
            retry_count: 0,
            wait_until: std::time::Instant::now(),
            last_retryable_error: None,
            completion: None,
        }
    }
    /// Go `regionJob.convertStageTo`.
    pub fn convert_stage_to(&mut self, stage: JobStage) {
        self.stage = stage;
        match stage {
            JobStage::RegionScanned => self.write_result = None,
            JobStage::NeedRescan => self.region = None,
            JobStage::Ingested => {
                if let (Some(data), Some(result)) = (&self.ingest_data, self.write_result.as_ref()) {
                    data.finish(result.total_bytes as i64, result.count as i64);
                }
            }
            JobStage::Wrote => {}
        }
    }
}
/// Go `newRegionJobs`. RegionInfo already contains decoded logical bounds.
pub fn new_region_jobs<'a>(
    regions: &[RegionInfo],
    data: Option<std::sync::Arc<dyn tidb_ingestor_engineapi::IngestData + 'a>>,
    ranges: &[(Vec<u8>, Vec<u8>)],
    size: u64,
    keys: u64,
) -> Vec<RegionJob<'a>> {
    let mut jobs = Vec::new();
    for range in ranges {
        for region in regions {
            let start = range.0.clone().max(region.region.start_key.clone());
            let end = if region.region.end_key.is_empty() {
                range.1.clone()
            } else if range.1.is_empty() {
                region.region.end_key.clone()
            } else {
                range.1.clone().min(region.region.end_key.clone())
            };
            if crate::localhelper::before_end(&start, &end) {
                jobs.push(RegionJob::new(region.clone(), data.clone(), start, end, size, keys));
            }
        }
    }
    jobs
}
/// Go `extractRegionFromErr`, retaining the leader store if SSTs still fit.
pub fn extract_region_from_err(
    job: &RegionJob<'_>,
    regions: &[metapb::Region],
    nextgen: bool,
) -> Option<RegionInfo> {
    if nextgen {
        return None;
    }
    let metas = &job.write_result.as_ref()?.sst_meta;
    let store = job.region.as_ref()?.leader?.store_id;
    for region in regions {
        if crate::localhelper::inside_region(region, metas) {
            let leader = region
                .peers
                .iter()
                .find(|peer| peer.store_id == store)
                .copied()?;
            return Some(RegionInfo {
                region: region.clone(),
                leader: Some(leader),
            });
        }
    }
    None
}

#[cfg(test)]
mod job_tests {
    use super::*;
    /// Go `TestNewRegionJobs`, all six range-intersection cases.
    #[test]
    fn test_new_region_jobs() {
        for (regions, ranges, expected) in [
            (vec![1, 0], vec![2, 3, 4], vec![2, 3, 4]),
            (vec![1, 4], vec![1, 2, 3, 4], vec![1, 2, 3, 4]),
            (vec![1, 2, 3, 4], vec![1, 4], vec![1, 2, 3, 4]),
            (vec![1, 3, 5, 7], vec![2, 4, 6], vec![2, 3, 4, 5, 6]),
            (vec![1, 4, 7], vec![2, 3, 4, 5, 6], vec![2, 3, 4, 5, 6]),
            (
                vec![1, 5, 6, 7, 8, 12],
                vec![1, 2, 3, 4, 9, 10, 12],
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12],
            ),
        ] {
            let regions: Vec<_> = regions
                .windows(2)
                .map(|keys| RegionInfo {
                    region: metapb::Region {
                        start_key: vec![keys[0]],
                        end_key: if keys[1] == 0 { vec![] } else { vec![keys[1]] },
                        ..Default::default()
                    },
                    leader: None,
                })
                .collect();
            let ranges: Vec<_> = ranges
                .windows(2)
                .map(|keys| (vec![keys[0]], vec![keys[1]]))
                .collect();
            let jobs = new_region_jobs(&regions, None, &ranges, 0, 0);
            assert_eq!(
                jobs.into_iter()
                    .map(|job| job.key_range)
                    .collect::<Vec<_>>(),
                expected
                    .windows(2)
                    .map(|keys| (vec![keys[0]], vec![keys[1]]))
                    .collect::<Vec<_>>()
            );
        }
    }
    /// Go `TestExtractRegionFromErrForNextGen`.
    #[test]
    fn test_extract_region_from_err_for_next_gen() {
        let region = RegionInfo {
            region: metapb::Region {
                id: 1,
                ..Default::default()
            },
            leader: Some(metapb::Peer {
                store_id: 1,
                ..Default::default()
            }),
        };
        let job = RegionJob::new(region.clone(), None, vec![1], vec![3], 0, 0);
        assert!(extract_region_from_err(&job, &[region.region], true).is_none());
    }
}

/// Go `regionJobRetryer`; owned jobs replace Go's reference-counted queue.
/// The queue lock is never held while delivering a job to a blocked worker.
pub struct RegionJobRetryer<'a> {
    ctx: Context,
    cancel: tidb_gocontext::CancelFunc,
    queue: Mutex<(
        bool,
        u64,
        std::collections::BTreeMap<(std::time::Instant, u64), RegionJob<'a>>,
    )>,
    reload: crossbeam_channel::Sender<()>,
    wake: crossbeam_channel::Receiver<()>,
    output: Mutex<Option<crossbeam_channel::Sender<RegionJob<'a>>>>,
}
impl<'a> RegionJobRetryer<'a> {
    /// Go `newRegionJobRetryer`.
    pub fn new(ctx: &Context, output: crossbeam_channel::Sender<RegionJob<'a>>) -> Self {
        let (ctx, cancel) = Context::with_cancel(ctx);
        let (reload, wake) = crossbeam_channel::bounded(1);
        Self {
            ctx,
            cancel,
            queue: Mutex::new((false, 0, Default::default())),
            reload,
            wake,
            output: Mutex::new(Some(output)),
        }
    }
    /// Go `regionJobRetryer.push`, returning ownership when the queue is closed.
    pub fn push(&self, job: RegionJob<'a>) -> std::result::Result<(), RegionJob<'a>> {
        let mut queue = self.queue.lock().unwrap();
        if queue.0 {
            return Err(job);
        }
        let id = queue.1;
        queue.1 += 1;
        queue.2.insert((job.wait_until, id), job);
        let _ = self.reload.try_send(());
        Ok(())
    }
    /// Go `regionJobRetryer.run`; cancellation drops queued and in-flight jobs.
    pub fn run(&self) {
        let Some(output) = self.output.lock().unwrap().clone() else {
            return;
        };
        loop {
            if self.ctx.err().is_some() {
                break;
            }
            let deadline = self
                .queue
                .lock()
                .unwrap()
                .2
                .first_key_value()
                .map(|(key, _)| key.0);
            match deadline {
                None => {
                    crossbeam_channel::select! { recv(self.ctx.done()) -> _ => break,recv(self.wake) -> _ => {} }
                }
                Some(deadline) => {
                    let wait = crossbeam_channel::after(
                        deadline.saturating_duration_since(std::time::Instant::now()),
                    );
                    crossbeam_channel::select! { recv(self.ctx.done()) -> _ => break,recv(self.wake) -> _ => continue,recv(wait) -> _ => {} }
                    let Some((_, job)) = self.queue.lock().unwrap().2.pop_first() else {
                        continue;
                    };
                    crossbeam_channel::select! { recv(self.ctx.done()) -> _ => break,send(output,job) -> result => if result.is_err() { break; } }
                }
            }
        }
        self.cleanup_unprocessed_jobs();
    }
    /// Go `regionJobRetryer.close`, after all jobs have completed.
    pub fn close(&self) {
        self.cancel.cancel();
        self.output.lock().unwrap().take();
    }
    /// Go `regionJobRetryer.cleanupUnprocessedJobs`.
    fn cleanup_unprocessed_jobs(&self) {
        let mut queue = self.queue.lock().unwrap();
        queue.0 = true;
        queue.2.clear();
    }
}
/// Go `dispatcher`: stage-driven completion and capped exponential retries.
pub struct Dispatcher<'a, 'b> {
    /// Go `workerCtx`.
    pub ctx: Context,
    /// Go `jobFromWorkerCh`.
    pub input: crossbeam_channel::Receiver<RegionJob<'a>>,
    /// Go `retryer`.
    pub retryer: &'b RegionJobRetryer<'a>,
}
impl Dispatcher<'_, '_> {
    /// Go `dispatcher.run`.
    pub fn run(&self) -> Result<()> {
        loop {
            let mut job = crossbeam_channel::select! { recv(self.ctx.done()) -> _ => return Ok(()),recv(self.input) -> job => match job { Ok(job) => job,Err(_) => { self.retryer.close();return Ok(()); } } };
            match job.stage {
                JobStage::RegionScanned | JobStage::Wrote => {
                    job.retry_count += 1;
                    if job.retry_count > crate::local::MAX_WRITE_AND_INGEST_RETRY_TIMES.load(std::sync::atomic::Ordering::SeqCst) {
                        return Err(job
                            .last_retryable_error
                            .take()
                            .unwrap_or_else(|| Error::Other("retry limit exceeded".into())));
                    }
                    let mut backoff =
                        Duration::from_secs((1_u64 << job.retry_count.min(5)).min(30));
                    tidb_failpoint::inject_call(
                        "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/adjustRegionJobRetryBackoff",
                        &mut backoff,
                    );
                    job.wait_until = std::time::Instant::now() + backoff;
                    let _ = self.retryer.push(job);
                }
                JobStage::Ingested => {}
                JobStage::NeedRescan => panic!("should not reach here"),
            }
        }
    }
}
struct BalanceState<'a> {
    jobs: std::collections::BTreeMap<u64, RegionJob<'a>>,
    next: u64,
    load: HashMap<u64, usize>,
}
/// Go `storeBalancer`; a single mutex makes selection and peer-load accounting
/// atomic, including repeated peer store IDs.
pub struct StoreBalancer<'a> {
    state: Mutex<BalanceState<'a>>,
}
impl<'a> Default for StoreBalancer<'a> {
    /// Go `newStoreBalancer`.
    fn default() -> Self {
        Self {
            state: Mutex::new(BalanceState {
                jobs: Default::default(),
                next: 0,
                load: Default::default(),
            }),
        }
    }
}
impl<'a> StoreBalancer<'a> {
    /// Go `storeBalancer.runReadToWorkerCh` queue insertion.
    pub fn push(&self, job: RegionJob<'a>) {
        let mut state = self.state.lock().unwrap();
        let id = state.next;
        state.next += 1;
        state.jobs.insert(id, job);
    }
    /// Go `storeBalancer.jobLen`.
    pub fn job_len(&self) -> usize {
        self.state.lock().unwrap().jobs.len()
    }
    /// Go `storeBalancer.pickJob`.
    pub fn pick_job(&self) -> Option<RegionJob<'a>> {
        let mut state = self.state.lock().unwrap();
        let id = *state
            .jobs
            .iter()
            .min_by_key(|(_, job)| {
                job.region.as_ref().map_or(0, |region| {
                    region
                        .region
                        .peers
                        .iter()
                        .map(|p| state.load.get(&p.store_id).copied().unwrap_or(0))
                        .sum::<usize>()
                })
            })
            .map(|(id, _)| id)?;
        let job = state.jobs.remove(&id).unwrap();
        if let Some(region) = &job.region {
            for peer in &region.region.peers {
                *state.load.entry(peer.store_id).or_default() += 1;
            }
        }
        Some(job)
    }
    /// Go `storeBalancer.releaseStoreLoad`.
    pub fn release_store_load(&self, peers: &[metapb::Peer]) {
        let mut state = self.state.lock().unwrap();
        for peer in peers {
            if let Some(load) = state.load.get_mut(&peer.store_id) {
                assert!(*load > 0);
                *load -= 1;
            }
        }
    }
    /// Go `storeBalancer.run`: read while the delivery worker is blocked.
    pub fn run(
        &self,
        ctx: &Context,
        input: crossbeam_channel::Receiver<RegionJob<'a>>,
        output: crossbeam_channel::Sender<RegionJob<'a>>,
    ) {
        let (wake_tx, wake) = crossbeam_channel::bounded(1);
        let (ctx, cancel) = Context::with_cancel(ctx);
        std::thread::scope(|scope| {
            scope.spawn(|| loop {
                crossbeam_channel::select! {
                    recv(ctx.done()) -> _ => return,
                    recv(wake) -> _ => {}
                }
                while let Some(job) = self.pick_job() {
                    let peers = job.region.as_ref().map(|r| r.region.peers.clone()).unwrap_or_default();
                    crossbeam_channel::select! {
                        recv(ctx.done()) -> _ => { self.release_store_load(&peers);return; },
                        send(output,job) -> sent => if sent.is_err() { self.release_store_load(&peers);cancel.cancel();return; }
                    }
                }
            });
            loop {
                crossbeam_channel::select! {
                    recv(ctx.done()) -> _ => break,
                    recv(input) -> job => match job {
                        Ok(job) => { self.push(job);let _ = wake_tx.try_send(()); },
                        Err(_) => break,
                    }
                }
            }
            cancel.cancel();
        });
        self.state.lock().unwrap().jobs.clear();
    }
}
#[cfg(test)]
mod scheduling_tests {
    use super::*;
    /// Go `TestStoreBalancerPick region-job fixture`.
    fn job(id: u64, stores: &[u64]) -> RegionJob<'static> {
        RegionJob::new(
            RegionInfo {
                region: metapb::Region {
                    id,
                    peers: stores
                        .iter()
                        .enumerate()
                        .map(|(n, id)| metapb::Peer {
                            id: n as u64 + 1,
                            store_id: *id,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
                leader: None,
            },
            None,
            vec![],
            vec![],
            0,
            0,
        )
    }
    /// Go `TestRegionJobRetryer`, wake earlier deadline, blocked-send push,
    /// cancel cleanup, and happy-path channel closure.
    #[test]
    fn test_region_job_retryer() {
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let (tx, rx) = crossbeam_channel::bounded(0);
        let queue = RegionJobRetryer::new(&ctx, tx);
        std::thread::scope(|scope| {
            scope.spawn(|| queue.run());
            for id in 0..8 {
                let mut job = job(id, &[]);
                job.wait_until += Duration::from_secs(3600);
                assert!(queue.push(job).is_ok());
            }
            assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
            assert!(queue.push(job(123, &[])).is_ok());
            assert_eq!(
                rx.recv_timeout(Duration::from_secs(1))
                    .unwrap()
                    .region
                    .unwrap()
                    .region
                    .id,
                123
            );
            assert!(queue.push(job(456, &[])).is_ok());
            std::thread::sleep(Duration::from_millis(20));
            assert!(queue.push(job(789, &[])).is_ok());
            cancel.cancel();
        });
        assert!(queue.queue.lock().unwrap().2.is_empty());
        assert!(queue.push(job(1, &[])).is_err());
        let (tx, rx) = crossbeam_channel::bounded(0);
        let queue = RegionJobRetryer::new(&Context::background(), tx);
        std::thread::scope(|scope| {
            scope.spawn(|| queue.run());
            assert!(queue.push(job(1, &[])).is_ok());
            rx.recv().unwrap();
            queue.close();
        });
        assert!(rx.recv().is_err());
    }
    /// Go `TestStoreBalancerPick`, including two peers on the same busy store.
    #[test]
    fn test_store_balancer_pick() {
        let b = StoreBalancer::default();
        b.push(job(1, &[1, 2]));
        let first = b.pick_job().unwrap();
        b.push(job(2, &[2, 2]));
        b.push(job(3, &[3, 4]));
        let idle = b.pick_job().unwrap();
        let busy = b.pick_job().unwrap();
        assert_eq!(idle.region.as_ref().unwrap().region.id, 3);
        assert_eq!(busy.region.as_ref().unwrap().region.id, 2);
        for j in [first, idle, busy] {
            b.release_store_load(&j.region.unwrap().region.peers);
        }
        assert!(b.state.lock().unwrap().load.values().all(|n| *n == 0));
    }
    /// Go `TestCancelBalancer`.
    #[test]
    fn test_cancel_balancer() {
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let (tx, rx) = crossbeam_channel::bounded(0);
        let (out, _rx) = crossbeam_channel::bounded(0);
        let b = StoreBalancer::default();
        std::thread::scope(|scope| {
            scope.spawn(|| b.run(&ctx, rx, out));
            for n in 0..20 {
                tx.send(job(n, &[n % 10])).unwrap();
            }
            cancel.cancel();
        });
        assert_eq!(b.job_len(), 0);
        assert!(b.state.lock().unwrap().load.values().all(|n| *n == 0));
    }
    /// Go `TestStoreBalancerNoRace`, concurrent load release while picking.
    #[test]
    fn test_store_balancer_no_race() {
        let b = StoreBalancer::default();
        for n in 0..200 {
            b.push(job(n, &[n % 10, (n * 7) % 10]));
        }
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let b = &b;
                scope.spawn(move || {
                    while let Some(j) = b.pick_job() {
                        b.release_store_load(&j.region.unwrap().region.peers);
                    }
                });
            }
        });
        assert_eq!(b.job_len(), 0);
        assert!(b.state.lock().unwrap().load.values().all(|n| *n == 0));
    }
    /// Go `TestUpdateAndGetLimiterConcurrencySafety`.
    #[test]
    fn test_update_and_get_limiter_concurrency_safety() {
        let limiter = crate::localhelper::StoreWriteLimiter::new(0);
        std::thread::scope(|scope| {
            for n in 0..100 {
                let limiter = &limiter;
                scope.spawn(move || limiter.update_limit(n));
                scope.spawn(move || {
                    let _ = limiter.limit();
                });
            }
        });
    }
}

/// Go `regionJob.ref/done` and `jobWg`, with scoped ownership instead of
/// manually pairing WaitGroup.Add/Done across every cancellation branch.
#[derive(Clone, Default)]
pub(crate) struct JobTracker(std::sync::Arc<(Mutex<usize>, std::sync::Condvar)>);
pub(crate) struct JobRef(JobTracker);
impl JobTracker {
    /// Go `regionJob.ref`.
    pub(crate) fn acquire(&self) -> JobRef {
        *self.0.0.lock().unwrap() += 1;
        JobRef(self.clone())
    }
    /// Go `jobWg.Wait`, interrupted by worker failure/cancellation.
    pub(crate) fn wait(&self, ctx: &Context) {
        let mut count = self.0.0.lock().unwrap();
        while *count != 0 && ctx.err().is_none() {
            count = self
                .0
                .1
                .wait_timeout(count, Duration::from_millis(20))
                .unwrap()
                .0;
        }
    }
}
impl JobRef {
    /// Go rescan's references for every regenerated job before publication.
    pub(crate) fn acquire(&self) -> Self {
        self.0.acquire()
    }
}
impl Drop for JobRef {
    /// Go `regionJob.done`.
    fn drop(&mut self) {
        let mut count = self.0.0.0.lock().unwrap();
        *count -= 1;
        if *count == 0 {
            self.0.0.1.notify_all();
        }
    }
}

#[cfg(test)]
mod job_reference_tests {
    use super::*;
    /// Go `TestRefAllJobsBeforeSending` job ranges, including empty jobs.
    fn jobs(count: usize) -> Vec<RegionJob<'static>> {
        (0..count)
            .map(|index| {
                RegionJob::new(
                    RegionInfo {
                        region: metapb::Region::default(),
                        leader: None,
                    },
                    None,
                    vec![b'a' + index as u8],
                    vec![b'b' + index as u8],
                    0,
                    0,
                )
            })
            .collect()
    }
    /// Go `TestRefAllJobsBeforeSending`: no early cleanup when an empty first job finishes.
    #[test]
    fn test_ref_all_jobs_before_sending() {
        let tracker = JobTracker::default();
        let (sender, receiver) = crossbeam_channel::bounded(0);
        std::thread::scope(|scope| {
            scope.spawn(|| send_region_jobs(&Context::background(), &tracker, jobs(5), &sender));
            for remaining in (1..=5).rev() {
                let job = receiver.recv().unwrap();
                assert_eq!(*tracker.0.0.lock().unwrap(), remaining);
                drop(job);
            }
        });
        assert_eq!(*tracker.0.0.lock().unwrap(), 0);
    }
    /// Go `TestGenerateAndSendJobDoneAllRefedJobsOnCancel`: cancel after first job.
    #[test]
    fn test_generate_and_send_job_done_all_refed_jobs_on_cancel() {
        let tracker = JobTracker::default();
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let (sender, receiver) = crossbeam_channel::bounded(0);
        std::thread::scope(|scope| {
            scope.spawn(|| send_region_jobs(&ctx, &tracker, jobs(4), &sender));
            let job = receiver.recv().unwrap();
            assert_eq!(*tracker.0.0.lock().unwrap(), 4);
            drop(job);
            cancel.cancel();
        });
        assert_eq!(*tracker.0.0.lock().unwrap(), 0);
    }
}
