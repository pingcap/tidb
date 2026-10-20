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

//! client-go reaches Go's unistore through `unistore.New`'s RPC and PD
//! clients (`tikv.NewTestTiKVStore`); client-rust's own `KvStore` reaches the
//! embedded store the same way: transactions, region splits answered by the
//! store's region errors, PD's GC-state calls, and GC lock resolution.

use std::sync::Arc;

use tidb_unistore::cluster::{bootstrap_with_single_store, Cluster};
use tidb_unistore::mock::StorePdClient;
use tikv_client::async_util::Cancellation;
use tikv_client::proto::kvrpcpb;
use tikv_client::tikv::{KvStore, MockSafePointKv};
use tikv_client::{Config, PdClient, TimestampExt};

async fn store() -> (KvStore<StorePdClient>, Arc<Cluster>) {
    let (rpc, pd, cluster) = tidb_unistore::mock::new(Vec::new()).unwrap();
    bootstrap_with_single_store(&cluster);
    let pd = tidb_unistore::mock::store_pd_client(&rpc, pd, Config::default())
        .await
        .unwrap();
    let store = KvStore::new_with_pd_client(
        Arc::new(pd),
        Arc::new(MockSafePointKv::new()),
        Config::default(),
    )
    .await
    .unwrap();
    (store, cluster)
}

async fn put_all(store: &KvStore<StorePdClient>, pairs: &[(&[u8], &[u8])]) {
    let mut txn = store.begin_optimistic().await.unwrap();
    for (key, value) in pairs {
        txn.put(key.to_vec(), value.to_vec()).await.unwrap();
    }
    txn.commit().await.unwrap();
}

async fn scan_keys(store: &KvStore<StorePdClient>) -> Vec<Vec<u8>> {
    let mut txn = store.begin_optimistic().await.unwrap();
    let keys = txn
        .scan(b"k".to_vec()..b"l".to_vec(), 100)
        .await
        .unwrap()
        .map(|pair| pair.0.into())
        .collect();
    txn.rollback().await.unwrap();
    keys
}

#[tokio::test]
async fn a_client_rust_store_runs_transactions_across_region_splits() {
    let (store, cluster) = store().await;
    put_all(
        &store,
        &[(b"k1", b"1"), (b"k2", b"2"), (b"k3", b"3"), (b"k4", b"4")],
    )
    .await;
    let mut reader = store.begin_optimistic().await.unwrap();
    assert_eq!(
        reader.get(b"k2".to_vec()).await.unwrap(),
        Some(b"2".to_vec())
    );
    reader.rollback().await.unwrap();

    // The store answers requests routed by the old region with a stale
    // epoch; the client re-routes over the new regions.
    cluster.split_arbitrary(&[b"k3".to_vec()]);
    assert_eq!(
        scan_keys(&store).await,
        vec![
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec()
        ]
    );
    put_all(&store, &[(b"k2", b"22"), (b"k5", b"5")]).await;
    let mut reader = store.begin_optimistic().await.unwrap();
    assert_eq!(
        reader.get(b"k2".to_vec()).await.unwrap(),
        Some(b"22".to_vec())
    );
    assert_eq!(
        reader.get(b"k5".to_vec()).await.unwrap(),
        Some(b"5".to_vec())
    );
    reader.rollback().await.unwrap();
}

#[tokio::test]
async fn pds_gc_state_calls_reach_the_embedded_stores_simulator() {
    let (store, _cluster) = store().await;
    let pd = store.pd_client();
    let response = pd
        .clone()
        .advance_transaction_safe_point(100)
        .await
        .unwrap();
    assert_eq!(
        (response.old_txn_safe_point, response.new_txn_safe_point),
        (0, 100)
    );
    assert_eq!(
        pd.clone()
            .update_service_gc_safe_point("br", 600, 150)
            .await
            .unwrap(),
        100
    );
    let blocked = pd
        .clone()
        .advance_transaction_safe_point(200)
        .await
        .unwrap();
    assert_eq!(blocked.new_txn_safe_point, 150);
    assert!(blocked.blocker_description.contains("br"));
    let error = pd
        .clone()
        .advance_transaction_safe_point(10)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("PD:gc:ErrDecreasingTxnSafePoint"),
        "{error}"
    );
    let response = pd.clone().advance_gc_safe_point(120).await.unwrap();
    assert_eq!(response.new_gc_safe_point, 120);
    let state = pd.clone().get_gc_state().await.unwrap();
    assert_eq!((state.txn_safe_point, state.gc_safe_point), (150, 120));
    assert_eq!(state.gc_barriers.len(), 1);
    let stores = pd.all_stores().await.unwrap();
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0].target, "store1");
}

#[tokio::test]
async fn gc_lock_resolution_clears_an_abandoned_lock_through_the_embedded_store() {
    let (store, cluster) = store().await;
    put_all(&store, &[(b"k1", b"1")]).await;
    cluster.split_arbitrary(&[b"k5".to_vec()]);

    // A transaction that prewrote and vanished: its lock's TTL has passed.
    let start_ts = store.current_timestamp().await.unwrap().version();
    // The embedded store's own client, behind the mock store's redirector.
    let tidb_unistore::redirector::RedirectedClient::Mock(rpc) =
        store.pd_client().kv_client("store1").await.unwrap()
    else {
        panic!("store1 is the embedded store, not a TiDB peer");
    };
    {
        let mut handler = rpc.handler().lock().unwrap();
        let context = kvrpcpb::Context {
            region_id: cluster
                .get_region_by_key(&tidb_unistore::region_manager::encode_region_key(b"k7"))
                .unwrap()
                .0
                .id,
            region_epoch: Some(tikv_client::proto::metapb::RegionEpoch {
                conf_ver: 1,
                version: 1,
            }),
            ..kvrpcpb::Context::default()
        };
        let response = handler.kv_prewrite(&kvrpcpb::PrewriteRequest {
            context: Some(context),
            mutations: vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key: b"k7".to_vec(),
                value: b"7".to_vec(),
                ..kvrpcpb::Mutation::default()
            }],
            primary_lock: b"k7".to_vec(),
            start_version: start_ts,
            lock_ttl: 1,
            ..kvrpcpb::PrewriteRequest::default()
        });
        assert!(response.errors.is_empty() && response.region_error.is_none());
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let max_version = store.current_timestamp().await.unwrap();
    let (stat, result) = store
        .resolve_locks_for_range(
            Cancellation::default(),
            &max_version,
            (Vec::new(), Vec::new()),
            16,
        )
        .await;
    result.unwrap();
    assert_eq!(stat.completed_regions, 2);
    assert!(rpc
        .handler()
        .lock()
        .unwrap()
        .store
        .scan_lock(b"", b"", u64::MAX, 16)
        .is_empty());
    let mut reader = store.begin_optimistic().await.unwrap();
    assert_eq!(
        reader.get(b"k7".to_vec()).await.unwrap(),
        None,
        "rolled back"
    );
    assert_eq!(
        reader.get(b"k1".to_vec()).await.unwrap(),
        Some(b"1".to_vec())
    );
    reader.rollback().await.unwrap();
}
