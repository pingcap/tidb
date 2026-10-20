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

use tidb_executor::region_map::{RegionMeta, RegionStore};
use tidb_txnkv::driver::tikv_store::TikvStore;

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
