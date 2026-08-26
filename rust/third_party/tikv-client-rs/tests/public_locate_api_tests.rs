// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Ordinary downstream-crate gate for client-go's public region-cache API.

use std::sync::Arc;
use std::time::Duration;

use tikv_client::proto::metapb;
use tikv_client::{
    get_store_liveness_timeout, set_store_liveness_timeout, Cluster, CodecPdClient, PdRpcClient,
    RegionCache, RegionWithLeader, RetryClient, TiFlashRpcContextUnavailableReason,
    TiFlashSelectionError,
};

type LiveRegionCache = RegionCache<CodecPdClient<RetryClient<Cluster>>>;

fn downstream_can_get_the_live_region_cache(client: &PdRpcClient) {
    let _: Arc<LiveRegionCache> = client.region_cache();
}

#[allow(dead_code)]
async fn downstream_can_drive_tiflash_routing(
    cache: &Arc<LiveRegionCache>,
    region: &RegionWithLeader,
) {
    let failure: TiFlashSelectionError = cache
        .select_tiflash_peer(region, false, &[])
        .await
        .unwrap_err();
    let _: TiFlashRpcContextUnavailableReason = failure.detail().reason;
    let _ = cache.valid_tiflash_store_ids(region, 0, &[]).await;
    let _ = cache
        .on_send_fail_for_tiflash(&region.ver_id(), 0, region.region.peers.len(), true, true)
        .await;
    let _: Arc<CodecPdClient<RetryClient<Cluster>>> = cache.pd_client();
    cache.update_buckets_if_needed(region.ver_id(), 0, 1);
}

#[allow(dead_code)]
fn downstream_store_facades_expose_the_same_live_cache(
    transaction_client: &tikv_client::TransactionClient,
    store: &tikv_client::tikv::KvStore,
) {
    let pd: Arc<PdRpcClient> = transaction_client.pd_client();
    let _: Arc<LiveRegionCache> = pd.region_cache();
    let _: Arc<LiveRegionCache> = store.region_cache();
}

#[test]
fn downstream_can_control_the_process_liveness_timeout() {
    let original = get_store_liveness_timeout();
    set_store_liveness_timeout(Duration::from_millis(321));
    assert_eq!(get_store_liveness_timeout(), Duration::from_millis(321));
    set_store_liveness_timeout(original);
}

#[allow(dead_code)]
fn public_signatures_compile(
    cache_accessor: fn(&PdRpcClient),
    tiflash_driver: fn(&LiveRegionCache, &RegionWithLeader),
    _label: metapb::StoreLabel,
) {
    let _ = (cache_accessor, tiflash_driver);
    let _ = downstream_can_get_the_live_region_cache;
}
