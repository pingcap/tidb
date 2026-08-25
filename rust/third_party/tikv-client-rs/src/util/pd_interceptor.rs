// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use crate::kv::ReplicaReadConfig;
use crate::locate::ReplicaSelectorState;
use crate::pd::PdClient;
use crate::proto::{keyspacepb, metapb};
use crate::region::{RegionId, RegionVerId, RegionWithLeader, StoreId};
use crate::retry::RetryBackoffer;
use crate::store::{RegionStore, Store};
use crate::{Key, Result, Timestamp};

use super::execdetails::current_exec_details;

/// Transparent PD client decorator that records caller-scoped PD wait time.
pub struct InterceptedPdClient<C> {
    inner: Arc<C>,
}

impl<C> InterceptedPdClient<C> {
    pub fn new(inner: Arc<C>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &Arc<C> {
        &self.inner
    }
}

async fn record_wait<F>(future: F) -> F::Output
where
    F: Future,
{
    let started = Instant::now();
    let output = future.await;
    if let Some(details) = current_exec_details() {
        details.add_wait_pd_response(started.elapsed());
    }
    output
}

#[async_trait]
impl<C> PdClient for InterceptedPdClient<C>
where
    C: PdClient,
{
    type KvClient = C::KvClient;

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        self.inner.clone().map_region_to_store(region).await
    }

    async fn map_region_to_store_with_replica(
        self: Arc<Self>,
        region: RegionWithLeader,
        config: ReplicaReadConfig,
        selector_state: ReplicaSelectorState,
        is_read_request: bool,
    ) -> Result<RegionStore> {
        self.inner
            .clone()
            .map_region_to_store_with_replica(region, config, selector_state, is_read_request)
            .await
    }

    async fn map_region_to_tiflash_store(
        self: Arc<Self>,
        region: RegionWithLeader,
        load_balance: bool,
        labels: &[metapb::StoreLabel],
    ) -> Result<RegionStore> {
        self.inner
            .clone()
            .map_region_to_tiflash_store(region, load_balance, labels)
            .await
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        record_wait(self.inner.region_for_key(key)).await
    }

    async fn region_for_end_key(&self, key: &Key) -> Result<RegionWithLeader> {
        record_wait(self.inner.region_for_end_key(key)).await
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        record_wait(self.inner.region_for_id(id)).await
    }

    async fn batch_load_regions_from_key(
        &self,
        key: &Key,
        count: usize,
        backoffer: &mut RetryBackoffer,
    ) -> Result<Vec<RegionWithLeader>> {
        record_wait(
            self.inner
                .batch_load_regions_from_key(key, count, backoffer),
        )
        .await
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        record_wait(self.inner.clone().get_timestamp()).await
    }

    async fn split_regions(
        self: Arc<Self>,
        split_keys: Vec<Vec<u8>>,
        retry_limit: u64,
    ) -> Result<Vec<u64>> {
        self.inner
            .clone()
            .split_regions(split_keys, retry_limit)
            .await
    }

    async fn cluster_id(&self) -> u64 {
        self.inner.cluster_id().await
    }

    async fn get_min_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        self.inner.clone().get_min_timestamp().await
    }

    async fn set_external_timestamp(self: Arc<Self>, timestamp: u64) -> Result<()> {
        self.inner.clone().set_external_timestamp(timestamp).await
    }

    async fn get_external_timestamp(self: Arc<Self>) -> Result<u64> {
        self.inner.clone().get_external_timestamp().await
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
        self.inner.clone().update_safepoint(safepoint).await
    }

    async fn update_safepoint_value(self: Arc<Self>, safepoint: u64) -> Result<u64> {
        self.inner.clone().update_safepoint_value(safepoint).await
    }

    async fn get_gc_state(self: Arc<Self>) -> Result<crate::proto::pdpb::GcState> {
        self.inner.clone().get_gc_state().await
    }

    async fn advance_transaction_safe_point(
        self: Arc<Self>,
        target: u64,
    ) -> Result<crate::proto::pdpb::AdvanceTxnSafePointResponse> {
        self.inner
            .clone()
            .advance_transaction_safe_point(target)
            .await
    }

    async fn advance_gc_safe_point(
        self: Arc<Self>,
        target: u64,
    ) -> Result<crate::proto::pdpb::AdvanceGcSafePointResponse> {
        self.inner.clone().advance_gc_safe_point(target).await
    }

    async fn scatter_regions(
        self: Arc<Self>,
        region_ids: Vec<u64>,
        group: Option<String>,
    ) -> Result<crate::proto::pdpb::ScatterRegionResponse> {
        self.inner.clone().scatter_regions(region_ids, group).await
    }

    async fn get_operator(
        self: Arc<Self>,
        region_id: u64,
    ) -> Result<crate::proto::pdpb::GetOperatorResponse> {
        self.inner.clone().get_operator(region_id).await
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.inner.load_keyspace(keyspace).await
    }

    async fn get_keyspace_meta(&self, name: &str) -> Result<keyspacepb::KeyspaceMeta> {
        self.inner.get_keyspace_meta(name).await
    }

    async fn get_keyspace_id(&self, name: &str) -> Result<u32> {
        self.inner.get_keyspace_id(name).await
    }

    async fn store_for_key(self: Arc<Self>, key: &Key) -> Result<RegionStore> {
        record_wait(self.inner.clone().store_for_key(key)).await
    }

    async fn store_for_id(self: Arc<Self>, id: RegionId) -> Result<RegionStore> {
        record_wait(self.inner.clone().store_for_id(id)).await
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        self.inner.all_stores().await
    }

    async fn update_leader(&self, id: RegionVerId, leader: metapb::Peer) -> Result<()> {
        self.inner.update_leader(id, leader).await
    }

    async fn update_region_cache(&self, regions: Vec<RegionWithLeader>) -> Result<()> {
        self.inner.update_region_cache(regions).await
    }

    async fn update_buckets(&self, id: RegionVerId, version: u64, keys: Vec<Vec<u8>>) {
        self.inner.update_buckets(id, version, keys).await;
    }

    fn record_server_load(&self, store_id: StoreId, estimated_wait_ms: u32) {
        self.inner.record_server_load(store_id, estimated_wait_ms);
    }

    async fn record_forwarding_proxy(&self, id: RegionVerId, store_id: StoreId) {
        self.inner.record_forwarding_proxy(id, store_id).await;
    }

    async fn on_send_failure(self: Arc<Self>, route: Option<&RegionStore>) -> bool {
        self.inner.clone().on_send_failure(route).await
    }

    async fn invalidate_region_cache(&self, id: RegionVerId) {
        self.inner.invalidate_region_cache(id).await;
    }

    async fn invalidate_store_cache(&self, store_id: StoreId) {
        self.inner.invalidate_store_cache(store_id).await;
    }

    async fn close_kv_client_addr_ver(&self, address: &str, version: u64) {
        self.inner.close_kv_client_addr_ver(address, version).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::MockPdClient;
    use crate::util::{with_exec_details, ExecDetails};

    #[tokio::test]
    async fn selected_pd_calls_accumulate_only_inside_the_scoped_details() {
        let inner = Arc::new(MockPdClient::default());
        let client = Arc::new(InterceptedPdClient::new(inner));
        let details = Arc::new(ExecDetails::default());
        with_exec_details(details.clone(), async {
            client.region_for_key(&Key::from(vec![1])).await.unwrap();
            client.clone().get_timestamp().await.unwrap();
        })
        .await;
        assert!(details.snapshot().wait_pd_response_duration_ns > 0);

        let before = details.snapshot();
        client.region_for_id(1).await.unwrap();
        assert_eq!(details.snapshot(), before);
    }
}
