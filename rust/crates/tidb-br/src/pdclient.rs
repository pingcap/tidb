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

//! The part of `github.com/tikv/pd/client`'s `pd.Client` that BR calls,
//! as a synchronous trait (Go's calls block their goroutine), and its
//! implementation over the node's TiKV store. Go's BR tests mock `pd.Client`
//! the same way.

use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_proto::metapb;
use tikv_client::PdClient as _;

use crate::errors::{self, Result};

/// Go `pd.Client`, the methods BR uses.
pub trait PdClient: Send + Sync {
    /// Go `GetClusterID`.
    fn get_cluster_id(&self, ctx: &Context) -> u64;
    /// Go `GetAllStores(ctx)`, with `opt.WithExcludeTombstone()` when
    /// `exclude_tombstone`.
    ///
    /// # Errors
    /// PD's error.
    fn get_all_stores(&self, ctx: &Context, exclude_tombstone: bool) -> Result<Vec<metapb::Store>>;
    /// Go `GetStore`.
    ///
    /// # Errors
    /// PD's error, or an unknown store.
    fn get_store(&self, ctx: &Context, store_id: u64) -> Result<metapb::Store> {
        self.get_all_stores(ctx, false)?
            .into_iter()
            .find(|s| s.id == store_id)
            .ok_or_else(|| errors::errorf(format!("invalid store ID {store_id}, not found")))
    }
    /// Go `GetTS`: the physical and logical parts.
    ///
    /// # Errors
    /// PD's error.
    fn get_ts(&self, ctx: &Context) -> Result<(i64, i64)>;
    /// Go `UpdateGCSafePoint`: answers the new GC safe point.
    ///
    /// # Errors
    /// PD's error.
    fn update_gc_safe_point(&self, ctx: &Context, safe_point: u64) -> Result<u64>;
    /// Go `UpdateServiceGCSafePoint`: answers the minimal service safe
    /// point.
    ///
    /// # Errors
    /// PD's error.
    fn update_service_gc_safe_point(&self, ctx: &Context, service_id: &str, ttl: i64, safe_point: u64)
        -> Result<u64>;
    /// Go `GetGCStatesClient(keyspaceID).GetGCState`'s `GCSafePoint`.
    ///
    /// # Errors
    /// PD's error.
    fn get_keyspace_gc_safe_point(&self, ctx: &Context, keyspace_id: u32) -> Result<u64>;
    /// Go `GetGCStatesClient(keyspaceID).SetGCBarrier`: the barrier's ID,
    /// timestamp and TTL.
    ///
    /// # Errors
    /// PD's error.
    fn set_gc_barrier(
        &self,
        ctx: &Context,
        keyspace_id: u32,
        barrier_id: &str,
        barrier_ts: u64,
        ttl: std::time::Duration,
    ) -> Result<(String, u64, std::time::Duration)>;
    /// Go `GetGCStatesClient(keyspaceID).DeleteGCBarrier`.
    ///
    /// # Errors
    /// PD's error.
    fn delete_gc_barrier(&self, ctx: &Context, keyspace_id: u32, barrier_id: &str) -> Result<()>;
    /// Go `GetRegion(key)` from PD, with its pending and down peers; `None`
    /// for no region.
    ///
    /// # Errors
    /// PD's error.
    fn get_region(&self, _ctx: &Context, _key: &[u8]) -> Result<Option<PdRegion>> {
        Err(errors::errorf("GetRegion is not implemented by this PD client"))
    }
    /// Go `GetRegionByID` from PD.
    ///
    /// # Errors
    /// PD's error.
    fn get_region_by_id(&self, _ctx: &Context, _region_id: u64) -> Result<Option<PdRegion>> {
        Err(errors::errorf("GetRegionByID is not implemented by this PD client"))
    }
    /// Go `ScanRegions(key, endKey, limit)`.
    ///
    /// # Errors
    /// PD's error.
    fn scan_regions(&self, _ctx: &Context, _key: &[u8], _end_key: &[u8], _limit: usize) -> Result<Vec<PdRegion>> {
        Err(errors::errorf("ScanRegions is not implemented by this PD client"))
    }
    /// Go `ScatterRegions(regionIDs, opt.WithSkipStoreLimit())`.
    ///
    /// # Errors
    /// PD's error.
    fn scatter_regions(&self, _ctx: &Context, _region_ids: &[u64]) -> Result<tidb_proto::pdpb::ScatterRegionResponse> {
        Err(errors::errorf("ScatterRegions is not implemented by this PD client"))
    }
    /// Go `ScatterRegion(regionID)`: the legacy one-region scatter.
    ///
    /// # Errors
    /// PD's error.
    fn scatter_region(&self, _ctx: &Context, _region_id: u64) -> Result<()> {
        Err(errors::errorf("ScatterRegion is not implemented by this PD client"))
    }
    /// Go `GetOperator(regionID)`.
    ///
    /// # Errors
    /// PD's error.
    fn get_operator(&self, _ctx: &Context, _region_id: u64) -> Result<tidb_proto::pdpb::GetOperatorResponse> {
        Err(errors::errorf("GetOperator is not implemented by this PD client"))
    }
}

/// Go `pd.Region`: a region PD reports, with its leader and the peers that
/// are pending or down.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PdRegion {
    /// Go `Meta`.
    pub meta: metapb::Region,
    /// Go `Leader`.
    pub leader: Option<metapb::Peer>,
    /// Go `PendingPeers`.
    pub pending_peers: Vec<metapb::Peer>,
    /// Go `DownPeers`.
    pub down_peers: Vec<metapb::Peer>,
}

impl From<tikv_client::RegionWithLeader> for PdRegion {
    fn from(r: tikv_client::RegionWithLeader) -> Self {
        Self {
            meta: r.region,
            leader: r.leader,
            pending_peers: r.pending_peers,
            down_peers: r.down_peers.into_iter().filter_map(|p| p.peer).collect(),
        }
    }
}

/// Go `oracle.ComposeTS`.
#[must_use]
pub fn compose_ts(physical: i64, logical: i64) -> u64 {
    ((physical << 18) + logical) as u64
}

/// Go `oracle.ExtractPhysical`.
#[must_use]
pub fn extract_physical(ts: u64) -> i64 {
    (ts >> 18) as i64
}

/// A `tikv_client` error as a BR error. A gRPC failure keeps its
/// `tonic::Status` as the cause, so Go's `status.Code(err)` checks see it.
pub fn from_client_error(err: tikv_client::Error) -> errors::Error {
    match err {
        tikv_client::Error::GrpcAPI(status) => errors::grpc(status),
        err => Arc::new(err),
    }
}

/// [`PdClient`] over the node's TiKV store's PD client.
pub struct TikvPdClient {
    store: tidb_txnkv::driver::tikv_store::TikvStore,
}

impl TikvPdClient {
    /// The PD client of `store`.
    #[must_use]
    pub fn new(store: tidb_txnkv::driver::tikv_store::TikvStore) -> Self {
        Self { store }
    }

    /// The store the client was built from.
    #[must_use]
    pub fn store(&self) -> &tidb_txnkv::driver::tikv_store::TikvStore {
        &self.store
    }
}

impl PdClient for TikvPdClient {
    fn get_cluster_id(&self, _: &Context) -> u64 {
        self.store.cluster_id()
    }

    fn get_all_stores(&self, _: &Context, exclude_tombstone: bool) -> Result<Vec<metapb::Store>> {
        let pd = self.store.store().pd_client();
        let stores = self.store.block_on(async move { pd.get_all_stores().await }).map_err(from_client_error)?;
        Ok(stores
            .into_iter()
            .filter(|s| !exclude_tombstone || s.state != metapb::StoreState::Tombstone as i32)
            .collect())
    }

    fn get_ts(&self, _: &Context) -> Result<(i64, i64)> {
        let pd = self.store.store().pd_client();
        let ts = self.store.block_on(async move { pd.get_timestamp().await }).map_err(from_client_error)?;
        Ok((ts.physical, ts.logical))
    }

    fn update_gc_safe_point(&self, _: &Context, safe_point: u64) -> Result<u64> {
        let pd = self.store.store().pd_client();
        self.store.block_on(async move { pd.update_safepoint_value(safe_point).await }).map_err(from_client_error)
    }

    fn update_service_gc_safe_point(
        &self,
        _: &Context,
        service_id: &str,
        ttl: i64,
        safe_point: u64,
    ) -> Result<u64> {
        let pd = self.store.store().pd_client();
        let id = service_id.to_owned();
        self.store
            .block_on(async move { pd.update_service_gc_safe_point(&id, ttl, safe_point).await })
            .map_err(from_client_error)
    }

    /// The store's PD client is bound to the store's keyspace, the one Go's
    /// `gc.NewManager` passes (`storage.GetCodec().GetKeyspaceID()`).
    fn get_keyspace_gc_safe_point(&self, _: &Context, _keyspace_id: u32) -> Result<u64> {
        let pd = self.store.store().pd_client();
        self.store
            .block_on(async move { pd.get_gc_state().await })
            .map(|state| state.gc_safe_point)
            .map_err(from_client_error)
    }

    fn set_gc_barrier(
        &self,
        _: &Context,
        _keyspace_id: u32,
        barrier_id: &str,
        barrier_ts: u64,
        ttl: std::time::Duration,
    ) -> Result<(String, u64, std::time::Duration)> {
        let pd = self.store.store().pd_client();
        let id = barrier_id.to_owned();
        let ttl_seconds = ttl.as_secs() as i64;
        let info = self
            .store
            .block_on(async move { pd.set_gc_barrier(&id, barrier_ts, ttl_seconds).await })
            .map_err(from_client_error)?;
        Ok((info.barrier_id, info.barrier_ts, std::time::Duration::from_secs(info.ttl_seconds.max(0) as u64)))
    }

    fn delete_gc_barrier(&self, _: &Context, _keyspace_id: u32, barrier_id: &str) -> Result<()> {
        let pd = self.store.store().pd_client();
        let id = barrier_id.to_owned();
        self.store.block_on(async move { pd.delete_gc_barrier(&id).await }).map_err(from_client_error)
    }

    fn get_store(&self, _: &Context, store_id: u64) -> Result<metapb::Store> {
        let pd = self.store.store().pd_client();
        self.store
            .block_on(async move { pd.pd_get_store(store_id).await })
            .map_err(from_client_error)?
            .ok_or_else(|| errors::errorf(format!("invalid store ID {store_id}, not found")))
    }

    fn get_region(&self, _: &Context, key: &[u8]) -> Result<Option<PdRegion>> {
        let pd = self.store.store().pd_client();
        let key = key.to_vec();
        Ok(self.store.block_on(async move { pd.pd_get_region(key).await }).map_err(from_client_error)?.map(PdRegion::from))
    }

    fn get_region_by_id(&self, _: &Context, region_id: u64) -> Result<Option<PdRegion>> {
        let pd = self.store.store().pd_client();
        Ok(self.store.block_on(async move { pd.pd_get_region_by_id(region_id).await }).map_err(from_client_error)?.map(PdRegion::from))
    }

    fn scan_regions(&self, _: &Context, key: &[u8], end_key: &[u8], limit: usize) -> Result<Vec<PdRegion>> {
        let pd = self.store.store().pd_client();
        let (key, end_key) = (key.to_vec(), end_key.to_vec());
        Ok(self
            .store
            .block_on(async move { pd.pd_scan_regions(key, end_key, limit).await })
            .map_err(from_client_error)?
            .into_iter()
            .map(PdRegion::from)
            .collect())
    }

    fn scatter_regions(&self, _: &Context, region_ids: &[u64]) -> Result<tidb_proto::pdpb::ScatterRegionResponse> {
        let pd = self.store.store().pd_client();
        let ids = region_ids.to_vec();
        self.store.block_on(async move { pd.pd_scatter_regions_skip_store_limit(ids).await }).map_err(from_client_error)
    }

    fn scatter_region(&self, _: &Context, region_id: u64) -> Result<()> {
        let pd = self.store.store().pd_client();
        self.store.block_on(async move { pd.pd_scatter_region(region_id).await }).map(|_| ()).map_err(from_client_error)
    }

    fn get_operator(&self, _: &Context, region_id: u64) -> Result<tidb_proto::pdpb::GetOperatorResponse> {
        let pd = self.store.store().pd_client();
        self.store.block_on(async move { pd.get_operator(region_id).await }).map_err(from_client_error)
    }
}
