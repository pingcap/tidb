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

//! A TiKV cluster's regions as SQL reaches them: client-go's
//! `KVStore.SplitRegions` / `WaitScatterRegionFinish` /
//! `CheckRegionInScattering` and `RegionCache.LoadRegionsInKeyRange`
//! through the native client's `KvStore`, and Go `getRegionInfo`'s PD HTTP
//! region statistics.
//!
//! It runs on the node's one client-rust store, as Go's `tikvStore` hands its
//! one `KVStore` to region splitting, and asks PD through that store's PD
//! HTTP client (Go `store.GetPDHTTPClient()`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tikv_client::async_util::Cancellation;
use tikv_client::proto::kvrpcpb;
use tikv_client::retry::{RetryBackoffer, BO_REGION_MISS, BO_TIKV_RPC};
use tikv_client::tikv::KvStore;
use tikv_client::transaction::range_task::{RangeTaskHandler, Runner, TaskStat};

use crate::region_map::{RegionMeta, RegionStore};
use tidb_txnkv::driver::tikv_store::TikvStore;

/// Go `helper.MaxBackoffTimeoutForMvccGet`.
const MAX_BACKOFF_TIMEOUT_FOR_MVCC_GET: u64 = 5000;

/// Go `ddl.flashbackMaxBackoff`.
const FLASHBACK_MAX_BACKOFF_MS: u64 = 1_800_000;

/// Go `ddl.flashbackTimeout`.
const FLASHBACK_TIMEOUT: Duration = Duration::from_secs(3 * 60);

/// Go `tikv.NewBackofferWithVars(ctx, 20000, nil)` in `getTableRegions`.
const LOAD_REGIONS_BACKOFF_MS: u64 = 20_000;

/// [`RegionStore`] over a node's store: a TiKV cluster's, or the embedded
/// store's (which has no PD HTTP API, so no region statistics, as Go's
/// `getRegionInfo` asks PD only for an etcd-backed store).
pub struct TikvRegionStore<S: tikv_client::PdClient = tikv_client::PdRpcClient> {
    pd_endpoints: Vec<String>,
    store: TikvStore<S>,
}

impl<S: tikv_client::PdClient> std::fmt::Debug for TikvRegionStore<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TikvRegionStore")
            .field("pd_endpoints", &self.pd_endpoints)
            .finish_non_exhaustive()
    }
}

impl<S: tikv_client::PdClient> TikvRegionStore<S> {
    /// Region access over the node's store, whose PD answers at
    /// `pd_endpoints` (none for the embedded store); region statistics come
    /// from the store's PD HTTP client.
    #[must_use]
    pub fn new(store: TikvStore<S>, pd_endpoints: Vec<String>) -> Self {
        Self {
            pd_endpoints,
            store,
        }
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: std::future::Future + Send,
        F::Output: Send,
    {
        self.store.block_on(future)
    }

    /// Go `getRegionInfo`: PD's statistics for one region, through
    /// `helper.TryGetPDHTTPClient` (the store's PD HTTP client with caller ID
    /// `tidb-store-helper`) and `GetRegionByID`. A store without PD
    /// addresses keeps the regions' zero statistics.
    fn region_info(&self, region_id: u64) -> Result<tikv_client::pd_http::RegionInfo, String> {
        if self.pd_endpoints.is_empty() {
            return Ok(tikv_client::pd_http::RegionInfo::default());
        }
        let pd_cli = self
            .store
            .pd_http_client()
            .ok_or_else(|| "pd http client unavailable".to_owned())?
            .with_caller_id("tidb-store-helper");
        self.block_on(pd_cli.get_region_by_id(region_id))
            .map_err(|error| error.to_string())
    }
}

impl<S: tikv_client::PdClient> TikvRegionStore<S> {
    /// Go `ddl.flashbackToVersion`: a `flashback-to-version-runner` range
    /// task over `[start_key, end_key)` whose handler sends `rpc` region by
    /// region and adds the regions it completed.
    fn flashback_to_version_on_range(
        &self,
        rpc: FlashbackRpc,
        flashback_ts: u64,
        start_ts: u64,
        start_key: &[u8],
        end_key: &[u8],
        concurrency: usize,
    ) -> Result<u64, String> {
        let total_regions = Arc::new(AtomicU64::new(0));
        let handler = FlashbackToVersionHandler {
            store: self.store.store().clone(),
            rpc,
            flashback_ts,
            start_ts,
            total_regions: Arc::clone(&total_regions),
        };
        let runner = Runner::new(
            "flashback-to-version-runner",
            self.store.store().pd_client(),
            concurrency,
            handler,
        );
        self.block_on(runner.run_on_range(start_key.to_vec(), end_key.to_vec()))
            .map_err(|error| error.to_string())?;
        Ok(total_regions.load(Ordering::Acquire))
    }
}

/// Which of TiKV's two flashback commands a range task sends.
#[derive(Clone, Copy)]
enum FlashbackRpc {
    /// Go `SendPrepareFlashbackToVersionRPC`.
    Prepare,
    /// Go `SendFlashbackToVersionRPC`.
    Flashback { commit_ts: u64 },
}

/// The range-task handler of Go's `flashbackCluster` steps.
struct FlashbackToVersionHandler<S: tikv_client::PdClient> {
    store: KvStore<S>,
    rpc: FlashbackRpc,
    flashback_ts: u64,
    start_ts: u64,
    total_regions: Arc<AtomicU64>,
}

impl<S: tikv_client::PdClient> Clone for FlashbackToVersionHandler<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            rpc: self.rpc,
            flashback_ts: self.flashback_ts,
            start_ts: self.start_ts,
            total_regions: Arc::clone(&self.total_regions),
        }
    }
}

#[async_trait::async_trait]
impl<S: tikv_client::PdClient> RangeTaskHandler for FlashbackToVersionHandler<S> {
    async fn handle(&self, cancellation: Cancellation, range: (Vec<u8>, Vec<u8>)) -> (TaskStat, tikv_client::Result<()>) {
        let (stats, result) = match self.rpc {
            FlashbackRpc::Prepare => {
                send_prepare_flashback_to_version_rpc(&cancellation, &self.store, self.flashback_ts, self.start_ts, range)
                    .await
            }
            FlashbackRpc::Flashback { commit_ts } => {
                send_flashback_to_version_rpc(
                    &cancellation,
                    &self.store,
                    self.flashback_ts,
                    self.start_ts,
                    commit_ts,
                    range,
                )
                .await
            }
        };
        self.total_regions
            .fetch_add(stats.completed_regions as u64, Ordering::AcqRel);
        (stats, result)
    }
}

fn new_flashback_backoffer(cancellation: &Cancellation) -> Arc<tokio::sync::Mutex<RetryBackoffer>> {
    Arc::new(tokio::sync::Mutex::new(RetryBackoffer::new(
        cancellation.clone(),
        FLASHBACK_MAX_BACKOFF_MS,
    )))
}

async fn flashback_backoff(
    backoffer: &Arc<tokio::sync::Mutex<RetryBackoffer>>,
    config: tikv_client::retry::RetryConfig,
    reason: String,
) -> tikv_client::Result<()> {
    backoffer
        .lock()
        .await
        .backoff(config, reason)
        .await
        .map_err(tikv_client::Error::from)
}

/// Go `ddlutil.SendPrepareFlashbackToVersionRPC`: prepares the regions of
/// `range` for a flashback, one request per region, blocking their
/// scheduling, reads and writes.
async fn send_prepare_flashback_to_version_rpc<S: tikv_client::PdClient>(
    cancellation: &Cancellation,
    store: &KvStore<S>,
    flashback_ts: u64,
    start_ts: u64,
    range: (Vec<u8>, Vec<u8>),
) -> (TaskStat, tikv_client::Result<()>) {
    let (mut start_key, range_end_key) = range;
    let mut task_stat = TaskStat::default();
    let mut bo = new_flashback_backoffer(cancellation);
    loop {
        if !range_end_key.is_empty() && start_key >= range_end_key {
            break;
        }
        let loc = match store.pd_client().region_for_key(&start_key.clone().into()).await {
            Ok(loc) => loc,
            Err(error) => return (task_stat, Err(error)),
        };
        let mut end_key = loc.region.end_key.clone();
        let is_last = end_key.is_empty() || (!range_end_key.is_empty() && end_key >= range_end_key);
        // If it is the last region.
        if is_last {
            end_key.clone_from(&range_end_key);
        }
        let request = kvrpcpb::PrepareFlashbackToVersionRequest {
            start_key: start_key.clone(),
            end_key: end_key.clone(),
            start_ts,
            version: flashback_ts,
            ..Default::default()
        };
        let sent = store
            .send_req(Arc::clone(&bo), request, &loc.ver_id(), FLASHBACK_TIMEOUT)
            .await;
        let response = match sent {
            Ok(response) => response,
            Err(tikv_client::Error::RegionError(region_error)) => {
                if let Err(error) = flashback_backoff(&bo, BO_REGION_MISS, format!("{region_error:?}")).await {
                    return (task_stat, Err(error));
                }
                continue;
            }
            Err(error) => return (task_stat, Err(error)),
        };
        if !response.error.is_empty() {
            let reason = format!("unexpected prepare flashback to version err: {}", response.error);
            if let Err(error) = flashback_backoff(&bo, BO_TIKV_RPC, reason).await {
                return (task_stat, Err(error));
            }
            continue;
        }
        task_stat.completed_regions += 1;
        if is_last {
            break;
        }
        bo = new_flashback_backoffer(cancellation);
        start_key = end_key;
    }
    (task_stat, Ok(()))
}

/// Go `ddlutil.SendFlashbackToVersionRPC`: flashes the regions of `range`
/// back to `flashback_ts`, one request per region. A region TiKV reports as
/// not prepared was flashed back by an earlier run of this step.
async fn send_flashback_to_version_rpc<S: tikv_client::PdClient>(
    cancellation: &Cancellation,
    store: &KvStore<S>,
    flashback_ts: u64,
    start_ts: u64,
    commit_ts: u64,
    range: (Vec<u8>, Vec<u8>),
) -> (TaskStat, tikv_client::Result<()>) {
    let (mut start_key, range_end_key) = range;
    let mut task_stat = TaskStat::default();
    let mut bo = new_flashback_backoffer(cancellation);
    loop {
        if !range_end_key.is_empty() && start_key >= range_end_key {
            break;
        }
        let loc = match store.pd_client().region_for_key(&start_key.clone().into()).await {
            Ok(loc) => loc,
            Err(error) => return (task_stat, Err(error)),
        };
        let mut end_key = loc.region.end_key.clone();
        let is_last = end_key.is_empty() || (!range_end_key.is_empty() && end_key >= range_end_key);
        // If it is the last region.
        if is_last {
            end_key.clone_from(&range_end_key);
        }
        let request = kvrpcpb::FlashbackToVersionRequest {
            version: flashback_ts,
            start_key: start_key.clone(),
            end_key: end_key.clone(),
            start_ts,
            commit_ts,
            ..Default::default()
        };
        let sent = store
            .send_req(Arc::clone(&bo), request, &loc.ver_id(), FLASHBACK_TIMEOUT)
            .await;
        match sent {
            Ok(response) => {
                if !response.error.is_empty() {
                    if let Err(error) = flashback_backoff(&bo, BO_TIKV_RPC, response.error).await {
                        return (task_stat, Err(error));
                    }
                    continue;
                }
            }
            Err(tikv_client::Error::RegionError(region_error)) => {
                if let Err(error) = flashback_backoff(&bo, BO_REGION_MISS, format!("{region_error:?}")).await {
                    return (task_stat, Err(error));
                }
                continue;
            }
            Err(error) => {
                if error.to_string() != format!("region {} is not prepared for the flashback", loc.id()) {
                    return (task_stat, Err(error));
                }
            }
        }
        task_stat.completed_regions += 1;
        if is_last {
            break;
        }
        bo = new_flashback_backoffer(cancellation);
        start_key = end_key;
    }
    (task_stat, Ok(()))
}

impl<S: tikv_client::PdClient> RegionStore for TikvRegionStore<S> {
    fn split_regions(
        &self,
        keys: Vec<Vec<u8>>,
        scatter: bool,
        table_id: Option<i64>,
    ) -> (Vec<u64>, Option<String>) {
        let (region_ids, result) =
            self.block_on(self.store.store().split_regions(keys, scatter, table_id));
        (region_ids, result.err().map(|error| error.to_string()))
    }

    fn wait_scatter_region_finish(&self, region_id: u64, backoff_ms: i64) -> Result<(), String> {
        self.block_on(
            self.store
                .store()
                .wait_scatter_region_finish(region_id, backoff_ms),
        )
        .map_err(|error| error.to_string())
    }

    fn check_region_in_scattering(&self, region_id: u64) -> Result<bool, String> {
        self.block_on(self.store.store().check_region_in_scattering(region_id))
            .map_err(|error| error.to_string())
    }

    fn load_regions_in_key_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<RegionMeta>, String> {
        let pd = self.store.store().pd_client();
        let mut backoffer = tikv_client::retry::RetryBackoffer::new(
            tikv_client::async_util::Cancellation::default(),
            LOAD_REGIONS_BACKOFF_MS,
        );
        let (start, end) = (start.to_vec().into(), end.to_vec().into());
        let regions = self
            .block_on(async move {
                pd.load_regions_in_key_range(start, end, &mut backoffer)
                    .await
            })
            .map_err(|error| error.to_string())?;
        let mut metas = Vec::with_capacity(regions.len());
        for region in regions {
            let leader = region.leader.as_ref();
            let info = self.region_info(region.region.id)?;
            metas.push(RegionMeta {
                id: region.region.id,
                peer_id: leader.map_or(0, |peer| peer.id),
                store_id: leader.map_or(0, |peer| peer.store_id),
                peers: region.region.peers.iter().map(|peer| peer.id).collect(),
                start_key: region.region.start_key.clone(),
                end_key: region.region.end_key.clone(),
                scattering: false,
                written_bytes: info.written_bytes,
                read_bytes: info.read_bytes,
                approximate_size: info.approximate_size,
                approximate_keys: info.approximate_keys,
            });
        }
        Ok(metas)
    }

    fn batch_load_regions_with_key_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let pd = self.store.store().pd_client();
        // Go `loadTableRanges`: `tikv.NewBackofferWithVars(ctx, 10000, nil)`.
        let mut backoffer = tikv_client::retry::RetryBackoffer::new(
            tikv_client::async_util::Cancellation::default(),
            10_000,
        );
        let (start, end) = (start.to_vec().into(), end.to_vec().into());
        let regions = self
            .block_on(async move {
                pd.batch_load_regions_with_key_range(start, end, limit, &mut backoffer)
                    .await
            })
            .map_err(|error| error.to_string())?;
        Ok(regions
            .into_iter()
            .map(|region| (region.region.start_key.clone(), region.region.end_key.clone()))
            .collect())
    }

    fn tikv_store_count(&self) -> Option<usize> {
        self.store
            .store()
            .pd_client()
            .get_stores_by_type(tikv_client::EndpointType::TiKv)
            .map(|stores| stores.len())
    }

    fn pd_region_by_id(&self, region_id: u64) -> Result<crate::region_map::PdRegion, String> {
        let pd = self.store.store().pd_client();
        let region = self
            .block_on(async move { pd.region_for_id(region_id).await })
            .map_err(|error| error.to_string())?;
        Ok(pd_region(region))
    }

    fn get_pd_addrs(&self) -> Result<Vec<String>, String> {
        Ok(self.pd_endpoints.clone())
    }

    fn pd_get_all_stores(&self) -> Result<Vec<crate::region_map::PdStoreMeta>, String> {
        let pd = self.store.store().pd_client();
        let stores = self
            .block_on(async move { pd.get_all_stores().await })
            .map_err(|error| error.to_string())?;
        Ok(stores
            .into_iter()
            .map(|store| crate::region_map::PdStoreMeta {
                id: store.id,
                labels: store
                    .labels
                    .into_iter()
                    .map(|label| (label.key, label.value))
                    .collect(),
            })
            .collect())
    }

    fn pd_batch_scan_regions(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<crate::region_map::PdRegion>, String> {
        let pd = self.store.store().pd_client();
        let ranges = vec![tikv_client::proto::pdpb::KeyRange {
            start_key: start.to_vec(),
            end_key: end.to_vec(),
        }];
        let options = tikv_client::RegionScanOptions {
            allow_follower_handle: true,
            ..Default::default()
        };
        let regions = self
            .block_on(async move { pd.batch_scan_regions(ranges, 0, options).await })
            .map_err(|error| error.to_string())?;
        Ok(regions.into_iter().map(pd_region).collect())
    }

    fn pd_scan_regions(&self, start: &[u8], end: &[u8]) -> Result<Vec<crate::region_map::PdRegion>, String> {
        let pd = self.store.store().pd_client();
        let mut backoffer = tikv_client::retry::RetryBackoffer::new(
            tikv_client::async_util::Cancellation::default(),
            LOAD_REGIONS_BACKOFF_MS,
        );
        let (start, end) = (start.to_vec().into(), end.to_vec().into());
        let regions = self
            .block_on(async move { pd.load_regions_in_key_range(start, end, &mut backoffer).await })
            .map_err(|error| error.to_string())?;
        Ok(regions.into_iter().map(pd_region).collect())
    }

    fn mvcc_get_by_key(
        &self,
        encoded_key: &[u8],
    ) -> Result<tikv_client::proto::kvrpcpb::MvccGetByKeyResponse, String> {
        let backoffer = Arc::new(tokio::sync::Mutex::new(tikv_client::retry::RetryBackoffer::new(
            tikv_client::async_util::Cancellation::default(),
            MAX_BACKOFF_TIMEOUT_FOR_MVCC_GET,
        )));
        let pd = self.store.store().pd_client();
        loop {
            let key = encoded_key.to_vec();
            let located = Arc::clone(&pd);
            let key_location = self
                .block_on(async move { located.region_for_key(&key.into()).await })
                .map_err(|error| error.to_string())?;
            let request = tikv_client::transaction::lowering::new_mvcc_get_by_key_request(encoded_key.to_vec().into());
            let sent = self.block_on(self.store.store().send_req(
                Arc::clone(&backoffer),
                request,
                &key_location.ver_id(),
                Duration::from_secs(60),
            ));
            let mvcc_resp = match sent {
                Ok(response) => response,
                Err(tikv_client::Error::RegionError(region_error)) => {
                    let backoffer = Arc::clone(&backoffer);
                    self.block_on(async move {
                        backoffer
                            .lock()
                            .await
                            .backoff(tikv_client::retry::BO_REGION_MISS, format!("{region_error:?}"))
                            .await
                    })
                    .map_err(|error| error.to_string())?;
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            };
            if !mvcc_resp.error.is_empty() {
                return Err(mvcc_resp.error);
            }
            if mvcc_resp.info.is_none() {
                return Err("Invalid mvcc response result, the info field is nil".to_owned());
            }
            return Ok(mvcc_resp);
        }
    }

    fn mvcc_get_by_start_ts(
        &self,
        start_ts: u64,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Option<(Vec<u8>, u64, tikv_client::proto::kvrpcpb::MvccGetByKeyResponse)>, String> {
        let backoffer = Arc::new(tokio::sync::Mutex::new(tikv_client::retry::RetryBackoffer::new(
            tikv_client::async_util::Cancellation::default(),
            5000,
        )));
        let pd = self.store.store().pd_client();
        let mut start_key = start_key.to_vec();
        loop {
            let key = start_key.clone();
            let located = Arc::clone(&pd);
            let cur_region = self
                .block_on(async move { located.region_for_key(&key.into()).await })
                .map_err(|error| error.to_string())?;
            let mut request = tikv_client::transaction::lowering::new_mvcc_get_by_start_ts_request(
                <tikv_client::Timestamp as tikv_client::TimestampExt>::from_version(start_ts),
            );
            request.context.get_or_insert_with(Default::default).priority =
                tikv_client::proto::kvrpcpb::CommandPri::Low as i32;
            let sent = self.block_on(self.store.store().send_req(
                Arc::clone(&backoffer),
                request,
                &cur_region.ver_id(),
                Duration::from_secs(60 * 60),
            ));
            let data = match sent {
                Ok(data) => data,
                // Go logs the region error and locates again.
                Err(tikv_client::Error::RegionError(_)) => continue,
                Err(error) => return Err(error.to_string()),
            };
            if !data.error.is_empty() {
                return Err(data.error);
            }
            if !data.key.is_empty() {
                let response = tikv_client::proto::kvrpcpb::MvccGetByKeyResponse {
                    info: data.info,
                    region_error: data.region_error,
                    error: data.error,
                };
                return Ok(Some((data.key, cur_region.id(), response)));
            }
            if !end_key.is_empty() && cur_region.contains(&end_key.to_vec().into()) {
                return Ok(None);
            }
            if cur_region.region.end_key.is_empty() {
                return Ok(None);
            }
            start_key = cur_region.region.end_key.clone();
        }
    }

    fn prepare_flashback_to_version(
        &self,
        flashback_ts: u64,
        start_ts: u64,
        start_key: &[u8],
        end_key: &[u8],
        concurrency: usize,
    ) -> Result<u64, String> {
        self.flashback_to_version_on_range(
            FlashbackRpc::Prepare,
            flashback_ts,
            start_ts,
            start_key,
            end_key,
            concurrency,
        )
    }

    fn flashback_to_version(
        &self,
        flashback_ts: u64,
        start_ts: u64,
        commit_ts: u64,
        start_key: &[u8],
        end_key: &[u8],
        concurrency: usize,
    ) -> Result<u64, String> {
        self.flashback_to_version_on_range(
            FlashbackRpc::Flashback { commit_ts },
            flashback_ts,
            start_ts,
            start_key,
            end_key,
            concurrency,
        )
    }

    fn region_id_by_key(&self, key: &[u8]) -> Result<u64, String> {
        let pd = self.store.store().pd_client();
        let key = key.to_vec();
        self.block_on(async move { pd.region_for_key(&key.into()).await })
            .map(|region| region.id())
            .map_err(|error| error.to_string())
    }

    fn locate_key_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
        let pd = self.store.store().pd_client();
        // Go `splitRawKeyRanges`' `tikv.NewBackofferWithVars(ctx, maxSleep,
        // nil)`: 20000ms, reduced to 500ms in test.
        let max_sleep = if tidb_intest::IN_TEST { 500 } else { 20_000 };
        let mut backoffer = tikv_client::retry::RetryBackoffer::new(
            tikv_client::async_util::Cancellation::default(),
            max_sleep,
        );
        let (start, end) = (start.to_vec().into(), end.to_vec().into());
        let regions = self
            .block_on(async move { pd.locate_key_range(start, end, &mut backoffer).await })
            .map_err(|error| error.to_string())?;
        Ok(regions
            .into_iter()
            .map(|region| (region.region.start_key.clone(), region.region.end_key.clone()))
            .collect())
    }

    /// Go `store.GetPDHTTPClient()`: the store's PD HTTP client when the
    /// store has PD addresses (the embedded store has none).
    fn pd_http_client(&self) -> Option<tikv_client::pd_http::Client> {
        if self.pd_endpoints.is_empty() {
            return None;
        }
        self.store.pd_http_client()
    }

    fn locate_region_by_id(&self, region_id: u64) -> Result<(Vec<u8>, Vec<u8>), String> {
        let pd = self.store.store().pd_client();
        let region = self
            .block_on(async move { pd.region_for_id(region_id).await })
            .map_err(|error| error.to_string())?;
        Ok((region.region.start_key, region.region.end_key))
    }
}

fn pd_peer(peer: &tikv_client::proto::metapb::Peer) -> crate::region_map::PdPeer {
    crate::region_map::PdPeer {
        id: peer.id,
        store_id: peer.store_id,
        role: peer.role,
        is_witness: peer.is_witness,
    }
}

/// client-go `pd.Region` of a cached region.
fn pd_region(region: tikv_client::RegionWithLeader) -> crate::region_map::PdRegion {
    crate::region_map::PdRegion {
        id: region.region.id,
        leader: region.leader.as_ref().map(pd_peer),
        peers: region.region.peers.iter().map(pd_peer).collect(),
        region_epoch: region
            .region
            .region_epoch
            .as_ref()
            .map(|epoch| (epoch.conf_ver, epoch.version)),
        start_key: region.region.start_key,
        end_key: region.region.end_key,
    }
}
