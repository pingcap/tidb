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
//! The store connects to PD on first use: a node that never splits or lists
//! regions never opens the extra client.

use std::sync::OnceLock;

use tidb_executor::region_map::{RegionMeta, RegionStore};
use tidb_pd_client::ClusterSecurity;

/// Go `tikv.NewBackofferWithVars(ctx, 20000, nil)` in `getTableRegions`.
const LOAD_REGIONS_BACKOFF_MS: u64 = 20_000;

/// [`RegionStore`] over a TiKV cluster.
pub struct TikvRegionStore {
    pd_endpoints: Vec<String>,
    security: ClusterSecurity,
    runtime: OnceLock<Result<tokio::runtime::Runtime, String>>,
    store: OnceLock<Result<tikv_client::tikv::KvStore, String>>,
    http: OnceLock<Result<reqwest::blocking::Client, String>>,
}

impl std::fmt::Debug for TikvRegionStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TikvRegionStore")
            .field("pd_endpoints", &self.pd_endpoints)
            .finish_non_exhaustive()
    }
}

/// PD's `RegionInfo`, the fields Go's `getRegionInfo` copies.
#[derive(serde::Deserialize, Default)]
struct PdRegionInfo {
    #[serde(default)]
    written_bytes: u64,
    #[serde(default)]
    read_bytes: u64,
    #[serde(default)]
    approximate_size: i64,
    #[serde(default)]
    approximate_keys: i64,
}

impl TikvRegionStore {
    /// A store over the cluster whose PD answers at `pd_endpoints`.
    #[must_use]
    pub fn new(pd_endpoints: Vec<String>, security: ClusterSecurity) -> Self {
        Self {
            pd_endpoints,
            security,
            runtime: OnceLock::new(),
            store: OnceLock::new(),
            http: OnceLock::new(),
        }
    }

    fn runtime(&self) -> Result<&tokio::runtime::Runtime, String> {
        self.runtime
            .get_or_init(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("region-store")
                    .enable_all()
                    .build()
                    .map_err(|error| error.to_string())
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    fn store(&self) -> Result<&tikv_client::tikv::KvStore, String> {
        let runtime = self.runtime()?;
        self.store
            .get_or_init(|| {
                let mut config = tikv_client::Config::default();
                if self.security.is_tls_enabled() {
                    config = config.with_security(
                        self.security.ca_path(),
                        self.security.cert_path(),
                        self.security.key_path(),
                    );
                }
                runtime
                    .block_on(tikv_client::tikv::KvStore::new_with_config(
                        self.pd_endpoints.clone(),
                        config,
                    ))
                    .map_err(|error| error.to_string())
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    /// Go `getRegionInfo`: PD's statistics for one region.
    fn region_info(&self, region_id: u64) -> Result<PdRegionInfo, String> {
        let Some(endpoint) = self.pd_endpoints.first() else {
            return Ok(PdRegionInfo::default());
        };
        let http = self
            .http
            .get_or_init(|| crate::cluster_http::cluster_http_client(&self.security))
            .as_ref()
            .map_err(Clone::clone)?;
        let scheme = if self.security.is_tls_enabled() {
            "https"
        } else {
            "http"
        };
        let host = endpoint
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        http.get(format!("{scheme}://{host}/pd/api/v1/region/id/{region_id}"))
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map_err(|error| error.to_string())?
            .json::<PdRegionInfo>()
            .map_err(|error| error.to_string())
    }
}

impl RegionStore for TikvRegionStore {
    fn split_regions(
        &self,
        keys: Vec<Vec<u8>>,
        scatter: bool,
        table_id: Option<i64>,
    ) -> (Vec<u64>, Option<String>) {
        let store = match self.store() {
            Ok(store) => store,
            Err(error) => return (Vec::new(), Some(error)),
        };
        let runtime = self.runtime().expect("store() built the runtime");
        let (region_ids, result) = runtime.block_on(store.split_regions(keys, scatter, table_id));
        (region_ids, result.err().map(|error| error.to_string()))
    }

    fn wait_scatter_region_finish(&self, region_id: u64, backoff_ms: i64) -> Result<(), String> {
        let store = self.store()?;
        self.runtime()?
            .block_on(store.wait_scatter_region_finish(region_id, backoff_ms))
            .map_err(|error| error.to_string())
    }

    fn check_region_in_scattering(&self, region_id: u64) -> Result<bool, String> {
        let store = self.store()?;
        self.runtime()?
            .block_on(store.check_region_in_scattering(region_id))
            .map_err(|error| error.to_string())
    }

    fn load_regions_in_key_range(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<RegionMeta>, String> {
        let store = self.store()?;
        let cache = store.region_cache();
        let mut backoffer = tikv_client::retry::RetryBackoffer::new(
            tikv_client::async_util::Cancellation::default(),
            LOAD_REGIONS_BACKOFF_MS,
        );
        let regions = self
            .runtime()?
            .block_on(cache.load_regions_in_key_range(
                start.to_vec().into(),
                end.to_vec().into(),
                &mut backoffer,
            ))
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
}
