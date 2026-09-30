// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Downstream-crate behavioral checks for the public mock TiKV test facade.

use std::sync::Arc;

use tikv_client::testutils::{bootstrap_with_single_store, new_mock_tikv, Keyspace, MockPdClient};
use tikv_client::{FlagsOp, Timestamp, TimestampExt, Transaction, TransactionOptions};

#[tokio::test]
async fn transactional_get_distinguishes_missing_from_empty_value() {
    fn assert_pd_client<T: tikv_client::PdClient>() {}
    assert_pd_client::<MockPdClient>();

    let (_client, cluster, pd) = new_mock_tikv("", None).unwrap();
    bootstrap_with_single_store(&cluster);
    let mut transaction = Transaction::new(
        Timestamp::from_version(2),
        Arc::new(pd),
        TransactionOptions::new_optimistic().read_only(),
        Keyspace::Disable,
    );

    assert_eq!(transaction.get("missing".to_owned()).await.unwrap(), None);
}

#[tokio::test]
async fn transaction_commits_direct_staged_memdb_writes_without_a_drain() {
    let (_client, cluster, pd) = new_mock_tikv("", None).unwrap();
    bootstrap_with_single_store(&cluster);
    let pd = Arc::new(pd);
    let mut transaction = Transaction::new(
        Timestamp::from_version(2),
        pd.clone(),
        TransactionOptions::new_optimistic(),
        Keyspace::Disable,
    );

    let discarded = transaction.get_mem_buffer().staging();
    transaction
        .get_mem_buffer()
        .set(b"discarded", b"value")
        .unwrap();
    assert_eq!(
        transaction.get("discarded".to_owned()).await.unwrap(),
        Some(b"value".to_vec())
    );
    transaction.get_mem_buffer().cleanup(discarded);

    let released = transaction.get_mem_buffer().staging();
    transaction
        .get_mem_buffer()
        .set_with_flags(
            b"committed",
            b"value",
            &[
                FlagsOp::SetAssertNotExist,
                FlagsOp::SetNeedConstraintCheckInPrewrite,
            ],
        )
        .unwrap();
    transaction.get_mem_buffer().release(released);
    assert_eq!(transaction.len(), 1);
    assert_eq!(transaction.size(), b"committed".len() + b"value".len());
    assert_eq!(
        transaction.get("committed".to_owned()).await.unwrap(),
        Some(b"value".to_vec())
    );
    transaction.commit().await.unwrap();

    let mut reader = Transaction::new(
        Timestamp::from_version(u64::MAX),
        pd,
        TransactionOptions::new_optimistic().read_only(),
        Keyspace::Disable,
    );
    assert_eq!(reader.get("discarded".to_owned()).await.unwrap(), None);
    assert_eq!(
        reader.get("committed".to_owned()).await.unwrap(),
        Some(b"value".to_vec())
    );
}

#[tokio::test]
async fn pd_routed_requests_share_the_mock_coprocessor_handler() {
    use tikv_client::proto::{coprocessor, kvrpcpb};
    use tikv_client::testutils::{CoprRpcHandler, RpcSession};
    use tikv_client::tikv::Client;
    use tikv_client::PdClient;

    struct Echo;
    impl CoprRpcHandler for Echo {
        fn handle(
            &self,
            _context: &kvrpcpb::Context,
            _session: &RpcSession,
            request: &coprocessor::Request,
        ) -> coprocessor::Response {
            coprocessor::Response {
                data: request.data.clone(),
                ..Default::default()
            }
        }
    }

    let (client, cluster, pd) = new_mock_tikv("", Some(Arc::new(Echo))).unwrap();
    bootstrap_with_single_store(&cluster);
    let pd = Arc::new(pd);
    let region = pd.region_for_key(&b"key".to_vec().into()).await.unwrap();
    let request = coprocessor::Request {
        context: Some(kvrpcpb::Context {
            region_id: region.id(),
            region_epoch: region.region.region_epoch.clone(),
            peer: region.leader.clone(),
            ..Default::default()
        }),
        data: b"same handler".to_vec(),
        ..Default::default()
    };
    let direct = client.dispatch(&request).await.unwrap();
    let routed = pd.map_region_to_store(region).await.unwrap();
    let indirect = routed.client.dispatch(&request).await.unwrap();
    let direct = direct.downcast::<coprocessor::Response>().unwrap();
    let indirect = indirect.downcast::<coprocessor::Response>().unwrap();
    assert_eq!(direct.data, request.data);
    assert_eq!(indirect.data, request.data);
}

#[test]
fn synchronous_injected_transaction_commits_and_rolls_back_the_native_buffer() {
    use tikv_client::{PdClient, SyncTransaction};

    let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let (_client, cluster, pd) = new_mock_tikv("", None).unwrap();
    bootstrap_with_single_store(&cluster);
    let pd = Arc::new(pd);
    let begin = || {
        let timestamp = runtime.block_on(pd.clone().get_timestamp()).unwrap();
        SyncTransaction::new(
            Transaction::new(
                timestamp,
                pd.clone(),
                TransactionOptions::new_optimistic(),
                Keyspace::Disable,
            ),
            runtime.clone(),
        )
    };
    let mut writer = begin();
    writer.get_mem_buffer().set(b"committed", b"value").unwrap();
    let checkpoint = writer.get_mem_buffer().staging();
    writer.get_mem_buffer().set(b"discarded", b"value").unwrap();
    writer.get_mem_buffer().cleanup(checkpoint);
    writer.commit().unwrap();

    let mut rolled_back = begin();
    rolled_back
        .get_mem_buffer()
        .set(b"rolled_back", b"value")
        .unwrap();
    rolled_back.rollback().unwrap();

    let mut reader = begin();
    assert_eq!(
        reader.get(b"committed".to_vec()).unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(reader.get(b"discarded".to_vec()).unwrap(), None);
    assert_eq!(reader.get(b"rolled_back".to_vec()).unwrap(), None);
    reader.rollback().unwrap();
}

#[tokio::test]
async fn transaction_scans_do_not_populate_the_point_read_cache() {
    use tikv_client::PdClient;
    let (_, cluster, pd) = new_mock_tikv("", None).unwrap();
    bootstrap_with_single_store(&cluster);
    let pd = Arc::new(pd);
    let mut writer = Transaction::new(
        pd.clone().get_timestamp().await.unwrap(),
        pd.clone(),
        TransactionOptions::new_optimistic(),
        Keyspace::Disable,
    );
    writer
        .put(b"key".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    writer.commit().await.unwrap();
    let mut reader = Transaction::new(
        pd.clone().get_timestamp().await.unwrap(),
        pd,
        TransactionOptions::new_optimistic(),
        Keyspace::Disable,
    );
    assert_eq!(
        reader
            .scan(b"a".to_vec()..b"z".to_vec(), 10)
            .await
            .unwrap()
            .count(),
        1
    );
    assert_eq!(
        reader.snapshot_cache_size(),
        0,
        "Go Scanner does not populate KVSnapshot's point cache"
    );
    assert_eq!(
        reader.get(b"key".to_vec()).await.unwrap(),
        Some(b"value".to_vec())
    );
    assert_eq!(reader.snapshot_cache_size(), 1);
    reader.rollback().await.unwrap();
}
