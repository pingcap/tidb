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

use std::sync::Arc;
use std::time::Duration;

use crate::region_map::{RegionMeta, RegionStore};
use tidb_txnkv::driver::tikv_store::TikvStore;

/// Go `helper.MaxBackoffTimeoutForMvccGet`.
const MAX_BACKOFF_TIMEOUT_FOR_MVCC_GET: u64 = 5000;

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
