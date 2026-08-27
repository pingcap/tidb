// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

// Direct ports of client-go's root integration_tests package live in this
// include so they retain access to transaction probes without widening the
// production API. Shared setup stops below the action/assertion boundary: each
// source identity remains independently selectable and owns its observations.

fn source_integration_store() -> (
    crate::mock::mocktikv::Cluster,
    Arc<crate::mock::mocktikv::MockPdClient>,
) {
    let (_client, cluster, pd) = crate::mock::mocktikv::new_tikv_and_pd_client("", None).unwrap();
    crate::mock::mocktikv::bootstrap_with_single_store(&cluster);
    (cluster, Arc::new(pd))
}

#[derive(Clone)]
struct SourceDeleteRangeHandler {
    engine: unistore::MockEngine,
}

#[async_trait::async_trait]
impl crate::transaction::range_task::RangeTaskHandler for SourceDeleteRangeHandler {
    async fn handle(
        &self,
        _cancellation: crate::async_util::Cancellation,
        range: (Vec<u8>, Vec<u8>),
    ) -> (
        crate::transaction::range_task::TaskStat,
        crate::Result<()>,
    ) {
        self.engine.delete_range(&range.0, &range.1);
        (
            crate::transaction::range_task::TaskStat {
                completed_regions: 1,
                ..Default::default()
            },
            Ok(()),
        )
    }
}

async fn source_delete_range_data(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut reader = source_integration_transaction(pd, false).await;
    reader
        .scan(b"~delete_range/a".to_vec().., u32::MAX)
        .await
        .unwrap()
        .map(|pair| {
            (
                <Key as Into<Vec<u8>>>::into(pair.key().clone()),
                pair.value().to_vec(),
            )
        })
        .collect()
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_delete_range_test_TestDeleteRange() {
    let (_client, cluster, pd) =
        crate::mock::mocktikv::new_tikv_and_pd_client("", None).unwrap();
    let key = |suffix: &[u8]| {
        let mut key = b"~delete_range/".to_vec();
        key.extend_from_slice(suffix);
        key
    };
    crate::mock::mocktikv::bootstrap_with_multi_regions(
        &cluster,
        &[key(b"b"), key(b"c"), key(b"d")],
    );
    let pd = Arc::new(pd);
    let mut expected = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut writer = source_integration_transaction(&pd, false).await;
    for first in b'a'..=b'd' {
        for second in b'0'..=b'9' {
            let full_key = key(&[first, second]);
            let value = vec![first, second];
            writer.put(full_key.clone(), value.clone()).await.unwrap();
            expected.insert(full_key, value);
        }
    }
    let buffered = writer.buffer.to_proto_mutations();
    assert_eq!(buffered.len(), expected.len());
    assert!(buffered.iter().all(|mutation| {
        expected
            .get(&mutation.key)
            .is_some_and(|value| value == &mutation.value)
    }));
    Box::pin(writer.commit()).await.unwrap();

    assert_eq!(source_delete_range_data(&pd).await, expected);

    for (start, end, completed) in [
        (key(b"b"), key(b"c0"), 2usize),
        (key(b"d0"), key(b"d0"), 0),
        (key(b"d0\0"), key(b"d1\0"), 1),
        (key(b"c5"), key(b"d5"), 2),
        (key(b"a"), key(b"z"), 4),
        (Vec::new(), Vec::new(), 4),
    ] {
        let mut runner = crate::transaction::range_task::Runner::new(
            "integration-delete-range",
            Arc::clone(&pd),
            1,
            SourceDeleteRangeHandler {
                engine: cluster.engine(),
            },
        );
        runner.set_regions_per_task(1);
        runner.run_on_range(start.clone(), end.clone()).await.unwrap();
        expected.retain(|candidate, _| {
            candidate < &start || (!end.is_empty() && candidate >= &end)
        });
        assert_eq!(source_delete_range_data(&pd).await, expected);
        assert_eq!(runner.completed_regions(), completed);
    }
}

fn source_integration_options(one_pc: bool) -> TransactionOptions {
    let options = TransactionOptions::new_optimistic()
        .heartbeat_option(HeartbeatOption::NoHeartbeat)
        .drop_check(CheckLevel::None);
    if one_pc {
        options.try_one_pc()
    } else {
        options
    }
}

async fn source_integration_transaction(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    one_pc: bool,
) -> Transaction<crate::mock::mocktikv::MockPdClient> {
    source_integration_transaction_with_options(pd, source_integration_options(one_pc)).await
}

async fn source_integration_transaction_with_options(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    options: TransactionOptions,
) -> Transaction<crate::mock::mocktikv::MockPdClient> {
    let timestamp = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
        .await
        .unwrap();
    Transaction::new(timestamp, Arc::clone(pd), options, Keyspace::Disable)
}

async fn source_async_commit_transaction(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
) -> Transaction<crate::mock::mocktikv::MockPdClient> {
    source_integration_transaction_with_options(
        pd,
        TransactionOptions::new_optimistic()
            .use_async_commit()
            .drop_check(CheckLevel::None),
    )
    .await
}

async fn source_async_commit_put(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    key: &[u8],
    value: &[u8],
) -> u64 {
    let mut transaction = source_async_commit_transaction(pd).await;
    transaction.put(key.to_vec(), value.to_vec()).await.unwrap();
    Box::pin(transaction.commit())
        .await
        .unwrap()
        .unwrap()
        .version()
}

fn source_commit_info_probe() -> (
    Arc<Mutex<Option<serde_json::Value>>>,
    impl Fn(String, Option<String>) + Send + Sync + 'static,
) {
    let info = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&info);
    let callback = move |value: String, error: Option<String>| {
        assert_eq!(error, None);
        *captured.lock().unwrap() = Some(serde_json::from_str(&value).unwrap());
    };
    (info, callback)
}

fn source_engine_value(
    cluster: &crate::mock::mocktikv::Cluster,
    key: &[u8],
    timestamp: u64,
) -> Option<(Vec<u8>, u64)> {
    cluster
        .engine()
        .get(
            key,
            timestamp,
            unistore::IsolationLevel::SnapshotIsolation,
            &[],
        )
        .unwrap()
}

fn source_assertion_key(prefix: &str, index: u8) -> Vec<u8> {
    format!("~assertion/{prefix}_k_{index}").into_bytes()
}

async fn source_prepare_assertion_keys(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    prefix: &str,
) -> (u64, u64) {
    let mut transaction = source_integration_transaction(pd, false).await;
    let start_ts = transaction.timestamp.version();
    for (index, value) in [(1, b"v1"), (3, b"v3"), (7, b"v7")] {
        transaction
            .put(source_assertion_key(prefix, index), value.to_vec())
            .await
            .unwrap();
    }
    let commit_ts = Box::pin(transaction.commit())
        .await
        .unwrap()
        .unwrap()
        .version();
    (start_ts, commit_ts)
}

async fn source_assertion_attempt(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    prefix: &str,
    pessimistic: bool,
    lock_keys: bool,
    assertion_level: kvrpcpb::AssertionLevel,
    assertion: super::MutationAssertion,
    indices: &[u8],
) -> (u64, crate::Result<Option<Timestamp>>) {
    let options = if pessimistic {
        TransactionOptions::new_pessimistic()
    } else {
        TransactionOptions::new_optimistic()
    }
    .heartbeat_option(HeartbeatOption::NoHeartbeat)
    .drop_check(CheckLevel::None);
    let mut transaction = source_integration_transaction_with_options(pd, options).await;
    transaction.set_assertion_level(assertion_level);
    let keys = indices
        .iter()
        .map(|index| source_assertion_key(prefix, *index))
        .collect::<Vec<_>>();
    if lock_keys {
        let for_update_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
            .await
            .unwrap()
            .version();
        let mut context = LockContext::new(for_update_ts, 1_000, SystemTime::now());
        context.init_check_existence(keys.len());
        transaction
            .lock_keys_with_context(&mut context, keys.clone())
            .await
            .unwrap();
    } else if pessimistic {
        transaction
            .lock_keys([format!("~assertion/{prefix}_primary").into_bytes()])
            .await
            .unwrap();
    }
    for key in &keys[..keys.len() - 1] {
        transaction
            .put(
                key.clone(),
                std::iter::once(b'v')
                    .chain(key.iter().copied())
                    .collect::<Vec<_>>(),
            )
            .await
            .unwrap();
    }
    let last = keys.last().unwrap();
    transaction
        .put_with_options(
            last.clone(),
            std::iter::once(b'v')
                .chain(last.iter().copied())
                .collect::<Vec<_>>(),
            super::MutationOptions::default().assertion(assertion),
        )
        .await
        .unwrap();
    let start_ts = transaction.timestamp.version();
    let result = Box::pin(transaction.commit()).await;
    (start_ts, result)
}

fn source_assertion_failure_fields(error: &Error) -> (u64, Vec<u8>, i32, u64, u64) {
    let Error::AssertionFailed(error) = error else {
        panic!("expected assertion failure, got {error:?}");
    };
    let failure = &error.assertion_failed;
    (
        failure.start_ts,
        failure.key.clone(),
        failure.assertion,
        failure.existing_start_ts,
        failure.existing_commit_ts,
    )
}

struct SourceAtomicU32Restore(&'static std::sync::atomic::AtomicU32, u32);

impl Drop for SourceAtomicU32Restore {
    fn drop(&mut self) {
        self.0.store(self.1, Ordering::SeqCst);
    }
}

struct SourceAtomicU64Restore(&'static std::sync::atomic::AtomicU64, u64);

impl Drop for SourceAtomicU64Restore {
    fn drop(&mut self) {
        self.0.store(self.1, Ordering::SeqCst);
    }
}

fn source_run_async_on_large_stack<F, Fut>(name: &str, test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name(name.to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(16 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(test());
        })
        .unwrap()
        .join()
        .unwrap();
}

fn source_ticlient_key(prefix: &str, index: usize) -> Vec<u8> {
    format!("~ticlient/{prefix}/key{index:08}").into_bytes()
}

async fn source_isolation_write(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    key: &[u8],
    value: Vec<u8>,
) -> (u64, u64) {
    loop {
        let mut transaction = source_integration_transaction(pd, false).await;
        let start_ts = transaction.timestamp.version();
        transaction.put(key.to_vec(), value.clone()).await.unwrap();
        match Box::pin(transaction.commit()).await {
            Ok(Some(commit_ts)) => return (start_ts, commit_ts.version()),
            Ok(None) => panic!("a source isolation write must have a commit timestamp"),
            Err(_) => tokio::task::yield_now().await,
        }
    }
}

async fn source_shared_transaction(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
) -> Transaction<crate::mock::mocktikv::MockPdClient> {
    source_integration_transaction_with_options(
        pd,
        TransactionOptions::new_pessimistic().drop_check(CheckLevel::None),
    )
    .await
}

async fn source_shared_lock_primary(
    transaction: &mut Transaction<crate::mock::mocktikv::MockPdClient>,
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    key: &[u8],
) {
    let for_update_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
        .await
        .unwrap()
        .version();
    let mut context = LockContext::new(for_update_ts, 1_000, SystemTime::now());
    transaction
        .lock_keys_with_context(&mut context, [key.to_vec()])
        .await
        .unwrap();
    assert_eq!(
        transaction
            .buffer
            .get_primary_key()
            .map(|key| <&[u8]>::from(&key).to_vec()),
        Some(key.to_vec())
    );
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_test_TestRollbackAsyncCommitEnforcesFallback() {
    let (client, cluster, _pd) = crate::mock::mocktikv::new_tikv_and_pd_client("", None).unwrap();
    let (store_id, peer_id, region_id) =
        crate::mock::mocktikv::bootstrap_with_single_store(&cluster);
    let (region, _) = cluster.region(region_id).unwrap();
    let context = kvrpcpb::Context {
        region_id,
        region_epoch: region.region_epoch,
        peer: Some(crate::proto::metapb::Peer {
            id: peer_id,
            store_id,
            ..Default::default()
        }),
        ..Default::default()
    };
    let primary = b"~async_commit/fallback-expiry/primary".to_vec();
    let secondary = b"~async_commit/fallback-expiry/secondary".to_vec();
    let now_ms = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let start_ts = now_ms << 18;
    let max_commit_ts = (now_ms + 50) << 18;
    let request = |key: Vec<u8>, secondaries: Vec<Vec<u8>>| kvrpcpb::PrewriteRequest {
        context: Some(context.clone()),
        mutations: vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::Put as i32,
            key,
            value: b"value".to_vec(),
            ..Default::default()
        }],
        primary_lock: primary.clone(),
        start_version: start_ts,
        lock_ttl: 1_000,
        min_commit_ts: start_ts + 1,
        max_commit_ts,
        use_async_commit: true,
        secondaries,
        ..Default::default()
    };

    let primary_request = request(primary.clone(), vec![secondary.clone()]);
    let primary_response = crate::store::KvClient::dispatch(&client, &primary_request)
        .await
        .unwrap()
        .downcast::<kvrpcpb::PrewriteResponse>()
        .unwrap();
    assert!(primary_response.errors.is_empty());
    assert!(primary_response.min_commit_ts > 0);
    assert!(
        cluster
            .engine()
            .mvcc_get_by_key(&primary)
            .lock
            .unwrap()
            .use_async_commit
    );

    tokio::time::sleep(Duration::from_millis(70)).await;
    let secondary_request = request(secondary.clone(), Vec::new());
    let secondary_response = crate::store::KvClient::dispatch(&client, &secondary_request)
        .await
        .unwrap()
        .downcast::<kvrpcpb::PrewriteResponse>()
        .unwrap();
    assert!(secondary_response.errors.is_empty());
    assert_eq!(secondary_response.min_commit_ts, 0);
    assert!(
        !cluster
            .engine()
            .mvcc_get_by_key(&secondary)
            .lock
            .unwrap()
            .use_async_commit
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_fail_test_TestFailAsyncCommitPrewriteRpcErrors() {
    let _scenario = FailScenario::setup();
    crate::util::enable_failpoints();
    let (_cluster, pd) = source_integration_store();
    let key = b"~async_commit_fail/prewrite-rpc/a".to_vec();
    let mut transaction = source_async_commit_transaction(&pd).await;
    transaction.put(key.clone(), b"a1".to_vec()).await.unwrap();

    fail::cfg("tikvclient/noRetryOnRpcError", "return(true)").unwrap();
    fail::cfg("tikvclient/rpcPrewriteTimeout", "return(true)").unwrap();
    let error = Box::pin(transaction.commit()).await.unwrap_err();
    assert!(crate::error::is_error_undetermined(&error), "{error:?}");
    assert!(matches!(
        transaction.rollback().await.unwrap_err(),
        Error::Static(crate::error::StaticError::InvalidTransaction)
    ));
    fail::remove("tikvclient/rpcPrewriteTimeout");
    fail::remove("tikvclient/noRetryOnRpcError");

    let mut reader = source_async_commit_transaction(&pd).await;
    assert_eq!(reader.get(key).await.unwrap(), Some(b"a1".to_vec()));
    reader.rollback().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_fail_test_TestAsyncCommitPrewriteCancelled() {
    let _scenario = FailScenario::setup();
    crate::util::enable_failpoints();
    let (cluster, pd) = source_integration_store();
    let split_key = b"~async_commit_fail/prewrite-cancel/s";
    let region_id = cluster.region_by_key(split_key).unwrap().0.id;
    let new_region_id = cluster.alloc_id();
    let new_peer_id = cluster.alloc_id();
    cluster.split(
        region_id,
        new_region_id,
        split_key,
        &[new_peer_id],
        new_peer_id,
    );

    fail::cfg(
        "tikvclient/rpcPrewriteResult",
        "1*return(writeConflict)->sleep(50)",
    )
    .unwrap();
    let mut transaction = source_async_commit_transaction(&pd).await;
    transaction
        .put(
            b"~async_commit_fail/prewrite-cancel/a".to_vec(),
            b"a".to_vec(),
        )
        .await
        .unwrap();
    transaction
        .put(
            b"~async_commit_fail/prewrite-cancel/z".to_vec(),
            b"z".to_vec(),
        )
        .await
        .unwrap();
    let error = Box::pin(transaction.commit()).await.unwrap_err();
    assert!(crate::error::is_write_conflict(&error), "{error:?}");
    fail::remove("tikvclient/rpcPrewriteResult");
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_async_commit_fail_test_TestPointGetWithAsyncCommit() {
    let _scenario = FailScenario::setup();
    crate::util::enable_failpoints();
    source_run_async_on_large_stack("client-go-TestPointGetWithAsyncCommit", || async {
        let (cluster, pd) = source_integration_store();
        let key_a = b"~async_commit_fail/point-get/a".to_vec();
        let key_b = b"~async_commit_fail/point-get/b".to_vec();
        let mut initial = source_integration_transaction(&pd, false).await;
        initial.put(key_a.clone(), b"a".to_vec()).await.unwrap();
        initial.put(key_b.clone(), b"b".to_vec()).await.unwrap();
        Box::pin(initial.commit()).await.unwrap();

        let background_done = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&background_done);
        let mut transaction = source_async_commit_transaction(&pd).await;
        transaction.set_background_task_lifecycle_hooks(super::LifecycleHooks {
            pre: None,
            post: Some(Arc::new(move || notify.notify_one())),
        });
        transaction
            .put(key_a.clone(), b"v1".to_vec())
            .await
            .unwrap();
        transaction
            .put(key_b.clone(), b"v2".to_vec())
            .await
            .unwrap();
        fail::cfg("tikvclient/asyncCommitDoNothing", "return").unwrap();
        Box::pin(transaction.commit()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), background_done.notified())
            .await
            .unwrap();
        assert!(cluster.engine().mvcc_get_by_key(&key_a).lock.is_some());
        assert!(cluster.engine().mvcc_get_by_key(&key_b).lock.is_some());

        let mut point_reader = source_integration_transaction(&pd, false).await;
        assert_eq!(
            point_reader.get(key_a.clone()).await.unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            point_reader.get(key_b.clone()).await.unwrap(),
            Some(b"v2".to_vec())
        );
        fail::remove("tikvclient/asyncCommitDoNothing");
        let mut snapshot_reader = source_async_commit_transaction(&pd).await;
        assert_eq!(
            snapshot_reader.get(key_a).await.unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            snapshot_reader.get(key_b).await.unwrap(),
            Some(b"v2".to_vec())
        );
        snapshot_reader.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_async_commit_fail_test_TestSecondaryListInPrimaryLock() {
    let _scenario = FailScenario::setup();
    crate::util::enable_failpoints();
    source_run_async_on_large_stack("client-go-TestSecondaryListInPrimaryLock", || async {
        async fn run_case(
            cluster: &crate::mock::mocktikv::Cluster,
            pd: &Arc<crate::mock::mocktikv::MockPdClient>,
            case: usize,
            names: &[&str],
        ) {
            let keys = names
                .iter()
                .map(|name| format!("~async_commit_fail/secondaries/{name}_{case}").into_bytes())
                .collect::<Vec<_>>();
            let background_done = Arc::new(tokio::sync::Notify::new());
            let notify = Arc::clone(&background_done);
            let mut transaction = source_async_commit_transaction(pd).await;
            transaction.set_background_task_lifecycle_hooks(super::LifecycleHooks {
                pre: None,
                post: Some(Arc::new(move || notify.notify_one())),
            });
            for key in &keys {
                transaction
                    .put(key.clone(), b"value".to_vec())
                    .await
                    .unwrap();
            }
            let start_ts = transaction.start_timestamp().version();
            fail::cfg("tikvclient/asyncCommitDoNothing", "return").unwrap();
            Box::pin(transaction.commit()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), background_done.notified())
                .await
                .unwrap();

            let primary = transaction
                .buffer
                .get_primary_key()
                .expect("committed transaction must retain its selected primary");
            let primary = <&[u8]>::from(&primary).to_vec();
            let lock = cluster
                .engine()
                .mvcc_get_by_key(&primary)
                .lock
                .expect("async-commit primary lock must remain");
            assert_eq!(lock.start_ts, start_ts);
            assert!(lock.use_async_commit);
            let mut expected = keys
                .iter()
                .filter(|key| **key != primary)
                .cloned()
                .collect::<Vec<_>>();
            expected.sort();
            let mut actual = lock.secondaries;
            actual.sort();
            assert_eq!(actual, expected);

            cluster.engine().rollback(&keys, start_ts).unwrap();
            fail::remove("tikvclient/asyncCommitDoNothing");
        }

        let (cluster, pd) = source_integration_store();
        for split in ["h", "o", "u"] {
            let split_key = format!("~async_commit_fail/secondaries/{split}").into_bytes();
            let region_id = cluster.region_by_key(&split_key).unwrap().0.id;
            let new_region_id = cluster.alloc_id();
            let new_peer_id = cluster.alloc_id();
            cluster.split(
                region_id,
                new_region_id,
                &split_key,
                &[new_peer_id],
                new_peer_id,
            );
        }

        run_case(&cluster, &pd, 1, &["a"]).await;
        run_case(&cluster, &pd, 2, &["a", "b"]).await;
        run_case(&cluster, &pd, 3, &["a", "b", "d"]).await;
        run_case(&cluster, &pd, 4, &["a", "b", "h", "i", "u"]).await;
        run_case(&cluster, &pd, 5, &["i", "a", "z", "u", "b"]).await;
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_fail_test_TestAsyncCommitContextCancelCausingUndetermined(
) {
    let _scenario = FailScenario::setup();
    crate::util::enable_failpoints();
    let (_cluster, pd) = source_integration_store();
    let mut transaction = source_async_commit_transaction(&pd).await;
    transaction
        .put(
            b"~async_commit_fail/context-cancel/a".to_vec(),
            b"va".to_vec(),
        )
        .await
        .unwrap();
    fail::cfg("tikvclient/rpcContextCancelErr", "return(true)").unwrap();
    let error = Box::pin(transaction.commit()).await.unwrap_err();
    assert!(crate::error::is_error_undetermined(&error), "{error:?}");
    fail::remove("tikvclient/rpcContextCancelErr");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_fail_test_TestPrewriteFailWithUndeterminedResult()
{
    let _scenario = FailScenario::setup();
    crate::util::enable_failpoints();
    let (_cluster, pd) = source_integration_store();
    let mut transaction = source_async_commit_transaction(&pd).await;
    transaction
        .put(
            b"~async_commit_fail/undetermined/key".to_vec(),
            b"value".to_vec(),
        )
        .await
        .unwrap();
    fail::cfg(
        "tikvclient/rpcPrewriteResult",
        "1*return(undeterminedResult)->return()",
    )
    .unwrap();
    let error = Box::pin(transaction.commit()).await.unwrap_err();
    assert!(crate::error::is_error_undetermined(&error), "{error:?}");
    fail::remove("tikvclient/rpcPrewriteResult");
}

async fn source_shared_lock_key(
    transaction: &mut Transaction<crate::mock::mocktikv::MockPdClient>,
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    key: &[u8],
) -> crate::Result<()> {
    let for_update_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
        .await?
        .version();
    let mut context = LockContext::new(for_update_ts, 1_000, SystemTime::now());
    context.in_share_mode = true;
    transaction
        .lock_keys_with_context(&mut context, [key.to_vec()])
        .await
}

fn source_shared_locks(
    cluster: &crate::mock::mocktikv::Cluster,
    key: &[u8],
    max_ts: u64,
) -> Vec<unistore::LockInfo> {
    let mut end = key.to_vec();
    end.push(0);
    cluster.engine().scan_locks(key, &end, max_ts).unwrap()
}

async fn source_wait_shared_locks(
    cluster: &crate::mock::mocktikv::Cluster,
    key: &[u8],
    max_ts: u64,
    expected: usize,
) -> Vec<unistore::LockInfo> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let locks = source_shared_locks(cluster, key, max_ts);
            if locks.len() == expected {
                return locks;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected {expected} shared locks for {key:?}"))
}

fn source_proto_shared_lock(lock: unistore::LockInfo) -> kvrpcpb::LockInfo {
    kvrpcpb::LockInfo {
        primary_lock: lock.primary,
        lock_version: lock.start_ts,
        key: lock.key,
        lock_ttl: lock.ttl,
        txn_size: lock.txn_size,
        lock_type: match lock.lock_type {
            unistore::Op::SharedLock => kvrpcpb::Op::Lock as i32,
            unistore::Op::SharedPessimisticLock => kvrpcpb::Op::PessimisticLock as i32,
            unistore::Op::Put => kvrpcpb::Op::Put as i32,
            unistore::Op::Delete => kvrpcpb::Op::Del as i32,
            unistore::Op::Lock => kvrpcpb::Op::Lock as i32,
            unistore::Op::Rollback => kvrpcpb::Op::Rollback as i32,
            unistore::Op::Insert => kvrpcpb::Op::Insert as i32,
            unistore::Op::PessimisticLock => kvrpcpb::Op::PessimisticLock as i32,
            unistore::Op::CheckNotExists => kvrpcpb::Op::CheckNotExists as i32,
        },
        lock_for_update_ts: lock.for_update_ts,
        min_commit_ts: lock.min_commit_ts,
        use_async_commit: lock.use_async_commit,
        secondaries: lock.secondaries,
        ..Default::default()
    }
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_1pc_test_Test1PC() {
    std::thread::Builder::new()
        .name("client-go-Test1PC".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let (cluster, pd) = source_integration_store();
                    let mut first = source_integration_transaction(&pd, true).await;
                    let first_start = first.timestamp.version();
                    let (first_info, first_callback) = source_commit_info_probe();
                    first.set_commit_callback(first_callback);
                    first
                        .put(b"~1pc/k1".to_vec(), b"v1".to_vec())
                        .await
                        .unwrap();
                    let first_commit = Box::pin(first.commit()).await.unwrap().unwrap().version();
                    assert!(first_commit > first_start);
                    assert_eq!(first.commit_timestamp().unwrap().version(), first_commit);
                    assert_eq!(
                        first_info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
                        "1pc"
                    );
                    assert_eq!(
                        source_engine_value(&cluster, b"~1pc/k1", first_commit),
                        Some((b"v1".to_vec(), first_commit))
                    );

                    let mut ordinary = source_integration_transaction(&pd, false).await;
                    let ordinary_start = ordinary.timestamp.version();
                    let (ordinary_info, ordinary_callback) = source_commit_info_probe();
                    ordinary.set_commit_callback(ordinary_callback);
                    ordinary
                        .put(b"~1pc/k2".to_vec(), b"v2".to_vec())
                        .await
                        .unwrap();
                    let ordinary_commit = Box::pin(ordinary.commit())
                        .await
                        .unwrap()
                        .unwrap()
                        .version();
                    assert!(ordinary_commit > ordinary_start);
                    assert_eq!(
                        ordinary_info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
                        "2pc"
                    );

                    let mut multiple = source_integration_transaction(&pd, true).await;
                    let multiple_start = multiple.timestamp.version();
                    let (multiple_info, multiple_callback) = source_commit_info_probe();
                    multiple.set_commit_callback(multiple_callback);
                    for (key, value) in [
                        (b"~1pc/k3", b"v3"),
                        (b"~1pc/k4", b"v4"),
                        (b"~1pc/k5", b"v5"),
                    ] {
                        multiple.put(key.to_vec(), value.to_vec()).await.unwrap();
                    }
                    let multiple_commit = Box::pin(multiple.commit())
                        .await
                        .unwrap()
                        .unwrap()
                        .version();
                    assert!(multiple_commit > multiple_start);
                    assert_eq!(
                        multiple_info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
                        "1pc"
                    );
                    for (key, value) in [
                        (b"~1pc/k3", b"v3"),
                        (b"~1pc/k4", b"v4"),
                        (b"~1pc/k5", b"v5"),
                    ] {
                        assert_eq!(
                            source_engine_value(&cluster, key, multiple_commit),
                            Some((value.to_vec(), multiple_commit))
                        );
                        assert_eq!(
                            source_engine_value(&cluster, key, multiple_commit - 1),
                            None
                        );
                    }

                    let mut overwrite = source_integration_transaction(&pd, true).await;
                    let (overwrite_info, overwrite_callback) = source_commit_info_probe();
                    overwrite.set_commit_callback(overwrite_callback);
                    overwrite
                        .put(b"~1pc/k5".to_vec(), b"v5new".to_vec())
                        .await
                        .unwrap();
                    let overwrite_commit = Box::pin(overwrite.commit())
                        .await
                        .unwrap()
                        .unwrap()
                        .version();
                    assert_eq!(
                        overwrite_info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
                        "1pc"
                    );
                    assert_eq!(
                        source_engine_value(&cluster, b"~1pc/k5", overwrite_commit),
                        Some((b"v5new".to_vec(), overwrite_commit))
                    );
                    assert_eq!(
                        source_engine_value(&cluster, b"~1pc/k5", overwrite_commit - 1),
                        Some((b"v5".to_vec(), multiple_commit))
                    );
                    for (key, value) in [
                        (b"~1pc/k1".as_slice(), b"v1".as_slice()),
                        (b"~1pc/k2".as_slice(), b"v2".as_slice()),
                        (b"~1pc/k3".as_slice(), b"v3".as_slice()),
                        (b"~1pc/k4".as_slice(), b"v4".as_slice()),
                        (b"~1pc/k5".as_slice(), b"v5new".as_slice()),
                    ] {
                        assert_eq!(
                            source_engine_value(&cluster, key, u64::MAX).unwrap().0,
                            value
                        );
                    }
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_1pc_test_Test1PCIsolation() {
    let (cluster, pd) = source_integration_store();
    let key = b"~1pc/isolation_k".to_vec();
    let mut initial = source_integration_transaction(&pd, true).await;
    initial.put(key.clone(), b"v1".to_vec()).await.unwrap();
    let initial_commit = Box::pin(initial.commit()).await.unwrap().unwrap().version();

    let mut writer = source_integration_transaction(&pd, true).await;
    writer.put(key.clone(), b"v2".to_vec()).await.unwrap();
    for _ in 0..10 {
        let _ = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
            .await
            .unwrap();
    }
    let mut reader = source_integration_transaction(&pd, true).await;
    let reader_start = reader.timestamp.version();
    assert_eq!(reader.get(key.clone()).await.unwrap(), Some(b"v1".to_vec()));

    let writer_commit = Box::pin(writer.commit()).await.unwrap().unwrap().version();
    assert!(writer_commit > reader_start);
    assert_eq!(reader.get(key.clone()).await.unwrap(), Some(b"v1".to_vec()));
    reader.rollback().await.unwrap();
    assert_eq!(
        source_engine_value(&cluster, &key, writer_commit),
        Some((b"v2".to_vec(), writer_commit))
    );
    assert_eq!(
        source_engine_value(&cluster, &key, writer_commit - 1),
        Some((b"v1".to_vec(), initial_commit))
    );
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_1pc_test_Test1PCDisallowMultiRegion() {
    let (cluster, pd) = source_integration_store();
    let mut initial = source_integration_transaction(&pd, true).await;
    initial
        .put(b"~1pc/k0".to_vec(), b"v0".to_vec())
        .await
        .unwrap();
    initial
        .put(b"~1pc/k3".to_vec(), b"v3".to_vec())
        .await
        .unwrap();
    Box::pin(initial.commit()).await.unwrap();

    let region_id = cluster.region_by_key(b"~1pc/k2").unwrap().0.id;
    let new_region_id = cluster.alloc_id();
    let new_peer_id = cluster.alloc_id();
    cluster.split(
        region_id,
        new_region_id,
        b"~1pc/k2",
        &[new_peer_id],
        new_peer_id,
    );
    let mut transaction = source_integration_transaction(&pd, true).await;
    let (info, callback) = source_commit_info_probe();
    transaction.set_commit_callback(callback);
    transaction
        .put(b"~1pc/k1".to_vec(), b"v1".to_vec())
        .await
        .unwrap();
    transaction
        .put(b"~1pc/k2".to_vec(), b"v2".to_vec())
        .await
        .unwrap();
    let commit_ts = Box::pin(transaction.commit())
        .await
        .unwrap()
        .unwrap()
        .version();
    assert_eq!(
        info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
        "2pc"
    );
    assert_eq!(
        info.lock().unwrap().as_ref().unwrap()["one_pc_fallback"],
        true
    );
    let mut reader = Transaction::new(
        crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
            .await
            .unwrap(),
        Arc::clone(&pd),
        source_integration_options(false).read_only(),
        Keyspace::Disable,
    );
    for (key, value) in [
        (b"~1pc/k0", b"v0"),
        (b"~1pc/k1", b"v1"),
        (b"~1pc/k2", b"v2"),
        (b"~1pc/k3", b"v3"),
    ] {
        assert_eq!(reader.get(key.to_vec()).await.unwrap().unwrap(), value);
        assert_eq!(
            source_engine_value(&cluster, key, commit_ts).unwrap().0,
            value
        );
    }
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_1pc_test_Test1PCLinearizability() {
    let (_cluster, pd) = source_integration_store();
    let mut first = source_integration_transaction(&pd, false).await;
    let mut second = source_integration_transaction(&pd, false).await;
    first
        .put(b"~1pc/linear_a".to_vec(), b"a1".to_vec())
        .await
        .unwrap();
    second
        .put(b"~1pc/linear_b".to_vec(), b"b1".to_vec())
        .await
        .unwrap();
    let second_commit = Box::pin(second.commit()).await.unwrap().unwrap().version();
    let first_commit = Box::pin(first.commit()).await.unwrap().unwrap().version();
    assert!(second_commit < first_commit);
    assert_eq!(second.commit_timestamp().unwrap().version(), second_commit);
    assert_eq!(first.commit_timestamp().unwrap().version(), first_commit);
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_1pc_test_Test1PCWithMultiDC() {
    let (_cluster, pd) = source_integration_store();
    let mut local = source_integration_transaction(&pd, true).await;
    let (local_info, local_callback) = source_commit_info_probe();
    local.set_commit_callback(local_callback);
    local.set_scope("bj");
    local
        .put(b"~1pc/multi_dc_a".to_vec(), b"a1".to_vec())
        .await
        .unwrap();
    Box::pin(local.commit()).await.unwrap();
    assert_eq!(
        local_info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
        "2pc"
    );
    assert_eq!(
        local_info.lock().unwrap().as_ref().unwrap()["txn_scope"],
        "bj"
    );

    let mut global = source_integration_transaction(&pd, true).await;
    let (global_info, global_callback) = source_commit_info_probe();
    global.set_commit_callback(global_callback);
    global.set_scope(crate::oracle::GLOBAL_TXN_SCOPE);
    global
        .put(b"~1pc/multi_dc_b".to_vec(), b"b1".to_vec())
        .await
        .unwrap();
    Box::pin(global.commit()).await.unwrap();
    assert_eq!(
        global_info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
        "1pc"
    );
    assert_eq!(
        global_info.lock().unwrap().as_ref().unwrap()["txn_scope"],
        crate::oracle::GLOBAL_TXN_SCOPE
    );
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_1pc_test_TestTxnCommitCounter() {
    let (_cluster, pd) = source_integration_store();
    let initial = crate::metrics::get_txn_commit_counter();

    let mut two_pc = source_integration_transaction(&pd, false).await;
    two_pc
        .put(b"~1pc/counter_k".to_vec(), b"v".to_vec())
        .await
        .unwrap();
    Box::pin(two_pc.commit()).await.unwrap();
    let after_two_pc = crate::metrics::get_txn_commit_counter().subtract(initial);
    assert_eq!(after_two_pc.two_pc, 1);
    assert_eq!(after_two_pc.async_commit, 0);
    assert_eq!(after_two_pc.one_pc, 0);

    let mut async_commit = Transaction::new(
        crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
            .await
            .unwrap(),
        Arc::clone(&pd),
        TransactionOptions::new_optimistic()
            .use_async_commit()
            .heartbeat_option(HeartbeatOption::NoHeartbeat)
            .drop_check(CheckLevel::None),
        Keyspace::Disable,
    );
    async_commit
        .put(b"~1pc/counter_k1".to_vec(), b"v1".to_vec())
        .await
        .unwrap();
    Box::pin(async_commit.commit()).await.unwrap();
    let after_async = crate::metrics::get_txn_commit_counter().subtract(initial);
    assert_eq!(after_async.two_pc, 1);
    assert_eq!(after_async.async_commit, 1);
    assert_eq!(after_async.one_pc, 0);

    let mut one_pc = source_integration_transaction(&pd, true).await;
    one_pc
        .put(b"~1pc/counter_k2".to_vec(), b"v2".to_vec())
        .await
        .unwrap();
    Box::pin(one_pc.commit()).await.unwrap();
    let after_one_pc = crate::metrics::get_txn_commit_counter().subtract(initial);
    assert_eq!(after_one_pc.two_pc, 1);
    assert_eq!(after_one_pc.async_commit, 1);
    assert_eq!(after_one_pc.one_pc, 1);
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_1pc_test_TestFailWithUndeterminedResult() {
    let scenario = FailScenario::setup();
    let (_cluster, pd) = source_integration_store();
    let mut transaction = source_integration_transaction(&pd, true).await;
    transaction
        .put(b"~1pc/undetermined_key".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    crate::util::enable_failpoints();
    fail::cfg("tikvclient/rpcPrewriteResult", "return(undeterminedResult)").unwrap();
    let error = Box::pin(transaction.commit()).await.unwrap_err();
    assert!(matches!(error, Error::UndeterminedError(_)));
    assert!(error.to_string().contains("undetermined"));
    fail::remove("tikvclient/rpcPrewriteResult");
    drop(scenario);
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_assertion_test_TestPrewriteAssertion() {
    std::thread::Builder::new()
        .name("client-go-TestPrewriteAssertion".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let _scenario = FailScenario::setup();
                    crate::util::enable_failpoints();
                    fail::cfg("tikvclient/assertionSkipCheckFromLock", "return").unwrap();

                    for (suffix, pessimistic, lock_keys) in
                        [("a", false, false), ("b", true, false), ("c", true, true)]
                    {
                        let (_cluster, pd) = source_integration_store();
                        let prefix = format!("strict-{suffix}");
                        let (prepare_start, prepare_commit) =
                            Box::pin(source_prepare_assertion_keys(&pd, &prefix)).await;

                        let (_, result) = Box::pin(source_assertion_attempt(
                            &pd,
                            &prefix,
                            pessimistic,
                            lock_keys,
                            kvrpcpb::AssertionLevel::Strict,
                            super::MutationAssertion::Exist,
                            &[1],
                        ))
                        .await;
                        assert!(result.is_ok());
                        let (_, result) = Box::pin(source_assertion_attempt(
                            &pd,
                            &prefix,
                            pessimistic,
                            lock_keys,
                            kvrpcpb::AssertionLevel::Strict,
                            super::MutationAssertion::NotExist,
                            &[2],
                        ))
                        .await;
                        assert!(result.is_ok());

                        let (start_ts, result) = Box::pin(source_assertion_attempt(
                            &pd,
                            &prefix,
                            pessimistic,
                            lock_keys,
                            kvrpcpb::AssertionLevel::Strict,
                            super::MutationAssertion::NotExist,
                            &[3],
                        ))
                        .await;
                        assert_eq!(
                            source_assertion_failure_fields(&result.unwrap_err()),
                            (
                                start_ts,
                                source_assertion_key(&prefix, 3),
                                kvrpcpb::Assertion::NotExist as i32,
                                prepare_start,
                                prepare_commit,
                            )
                        );
                        let (start_ts, result) = Box::pin(source_assertion_attempt(
                            &pd,
                            &prefix,
                            pessimistic,
                            lock_keys,
                            kvrpcpb::AssertionLevel::Strict,
                            super::MutationAssertion::Exist,
                            &[4],
                        ))
                        .await;
                        assert_eq!(
                            source_assertion_failure_fields(&result.unwrap_err()),
                            (
                                start_ts,
                                source_assertion_key(&prefix, 4),
                                kvrpcpb::Assertion::Exist as i32,
                                0,
                                0,
                            )
                        );
                        let (start_ts, result) = Box::pin(source_assertion_attempt(
                            &pd,
                            &prefix,
                            pessimistic,
                            lock_keys,
                            kvrpcpb::AssertionLevel::Strict,
                            super::MutationAssertion::NotExist,
                            &[5, 6, 7],
                        ))
                        .await;
                        assert_eq!(
                            source_assertion_failure_fields(&result.unwrap_err()),
                            (
                                start_ts,
                                source_assertion_key(&prefix, 7),
                                kvrpcpb::Assertion::NotExist as i32,
                                prepare_start,
                                prepare_commit,
                            )
                        );
                        let (start_ts, result) = Box::pin(source_assertion_attempt(
                            &pd,
                            &prefix,
                            pessimistic,
                            lock_keys,
                            kvrpcpb::AssertionLevel::Strict,
                            super::MutationAssertion::Exist,
                            &[8, 9, 10],
                        ))
                        .await;
                        assert_eq!(
                            source_assertion_failure_fields(&result.unwrap_err()),
                            (
                                start_ts,
                                source_assertion_key(&prefix, 10),
                                kvrpcpb::Assertion::Exist as i32,
                                0,
                                0,
                            )
                        );
                    }

                    fail::remove("tikvclient/assertionSkipCheckFromLock");
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_assertion_test_TestPrewriteAssertionWithTxnFileEnabled() {
    let (_cluster, pd) = source_integration_store();
    let key = b"~assertion/txn-file".to_vec();
    let mut prepare = source_integration_transaction(&pd, false).await;
    prepare
        .put(key.clone(), b"existing".to_vec())
        .await
        .unwrap();
    Box::pin(prepare.commit()).await.unwrap();

    let restore = crate::config::update_global(|config| {
        config.tikv_client.txn_chunk_writer_addr = "127.0.0.1".to_owned();
        config.tikv_client.txn_file_min_mutation_size = 1;
    });

    let mut transaction = source_integration_transaction(&pd, false).await;
    let start_ts = transaction.timestamp.version();
    transaction.set_assertion_level(kvrpcpb::AssertionLevel::Strict);
    transaction
        .put_with_options(
            key.clone(),
            b"updated".to_vec(),
            super::MutationOptions::default().assertion(super::MutationAssertion::NotExist),
        )
        .await
        .unwrap();
    let buffered = transaction.buffer.to_proto_mutations();
    assert_eq!(buffered.len(), 1);
    assert_eq!(buffered[0].assertion, kvrpcpb::Assertion::NotExist as i32);
    let error = Box::pin(transaction.commit()).await.unwrap_err();
    let fields = source_assertion_failure_fields(&error);
    assert_eq!(fields.0, start_ts);
    assert_eq!(fields.1, key);
    assert_eq!(fields.2, kvrpcpb::Assertion::NotExist as i32);
    assert!(!matches!(error, Error::StringError(ref message) if message.contains("txn chunk")));
    restore();
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_assertion_test_TestFastAssertion() {
    std::thread::Builder::new()
        .name("client-go-TestFastAssertion".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let _scenario = FailScenario::setup();
                    crate::util::enable_failpoints();
                    fail::cfg("tikvclient/assertionSkipCheckFromPrewrite", "return").unwrap();

                    for (suffix, pessimistic, lock_keys) in
                        [("a", false, false), ("b", true, false), ("c", true, true)]
                    {
                        let (_cluster, pd) = source_integration_store();
                        let prefix = format!("fast-{suffix}");
                        Box::pin(source_prepare_assertion_keys(&pd, &prefix)).await;
                        for (assertion, indices, should_fail) in [
                            (super::MutationAssertion::Exist, vec![1], false),
                            (super::MutationAssertion::NotExist, vec![2], false),
                            (super::MutationAssertion::NotExist, vec![3], lock_keys),
                            (super::MutationAssertion::Exist, vec![4], lock_keys),
                            (super::MutationAssertion::NotExist, vec![5, 6, 7], lock_keys),
                            (super::MutationAssertion::Exist, vec![8, 9, 10], lock_keys),
                        ] {
                            let (start_ts, result) = Box::pin(source_assertion_attempt(
                                &pd,
                                &prefix,
                                pessimistic,
                                lock_keys,
                                kvrpcpb::AssertionLevel::Fast,
                                assertion,
                                &indices,
                            ))
                            .await;
                            if should_fail {
                                let last = *indices.last().unwrap();
                                let fields = source_assertion_failure_fields(&result.unwrap_err());
                                assert_eq!(fields.0, start_ts);
                                assert_eq!(fields.1, source_assertion_key(&prefix, last));
                                assert_eq!(fields.2, assertion.to_proto() as i32);
                                assert_eq!((fields.3, fields.4), (0, 0));
                            } else {
                                assert!(
                    result.is_ok(),
                    "fast assertion case {suffix}/{indices:?} unexpectedly failed: {result:?}"
                );
                            }
                        }
                    }

                    fail::remove("tikvclient/assertionSkipCheckFromPrewrite");
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
#[allow(non_snake_case)]
fn source_go_integration_tests_assertion_test_TestAssertionErrorLessPriorToOtherError() {
    let assertion = || -> Error {
        kvrpcpb::KeyError {
            assertion_failed: Some(kvrpcpb::AssertionFailed {
                start_ts: 1,
                key: b"asserted".to_vec(),
                assertion: kvrpcpb::Assertion::NotExist as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
        .into()
    };
    let conflict = || -> Error {
        kvrpcpb::KeyError {
            conflict: Some(kvrpcpb::WriteConflict {
                start_ts: 1,
                conflict_ts: 2,
                conflict_commit_ts: 3,
                key: b"conflict".to_vec(),
                ..Default::default()
            }),
            ..Default::default()
        }
        .into()
    };
    for errors in [
        vec![assertion(), conflict()],
        vec![conflict(), assertion()],
        vec![assertion(), assertion(), conflict()],
        vec![assertion(), conflict(), assertion()],
    ] {
        let selected = super::normalize_prewrite_error(Error::MultipleKeyErrors(errors));
        assert!(matches!(selected, Error::WriteConflict(_)));
        assert!(!selected.to_string().to_lowercase().contains("assertion"));
    }
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_ticlient_test_TestSingleKey() {
    let (_cluster, pd) = source_integration_store();
    let key = b"~ticlient/single/key".to_vec();

    let mut writer = source_integration_transaction(&pd, false).await;
    writer.put(key.clone(), b"value".to_vec()).await.unwrap();
    writer.lock_keys([key.clone()]).await.unwrap();
    Box::pin(writer.commit()).await.unwrap();

    let mut reader = source_integration_transaction(&pd, false).await;
    assert_eq!(
        reader.get(key.clone()).await.unwrap(),
        Some(b"value".to_vec())
    );
    reader.rollback().await.unwrap();

    let mut deleter = source_integration_transaction(&pd, false).await;
    deleter.delete(key.clone()).await.unwrap();
    Box::pin(deleter.commit()).await.unwrap();
    let mut after_delete = source_integration_transaction(&pd, false).await;
    assert_eq!(after_delete.get(key).await.unwrap(), None);
    after_delete.rollback().await.unwrap();
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_ticlient_test_TestMultiKeys() {
    const KEY_COUNT: usize = 100;
    let (_cluster, pd) = source_integration_store();

    let mut writer = source_integration_transaction(&pd, false).await;
    for index in 0..KEY_COUNT {
        writer
            .put(
                source_ticlient_key("multi", index),
                format!("value{index:08}").into_bytes(),
            )
            .await
            .unwrap();
    }
    Box::pin(writer.commit()).await.unwrap();

    let mut reader = source_integration_transaction(&pd, false).await;
    for index in 0..KEY_COUNT {
        assert_eq!(
            reader
                .get(source_ticlient_key("multi", index))
                .await
                .unwrap(),
            Some(format!("value{index:08}").into_bytes())
        );
    }
    reader.rollback().await.unwrap();

    let mut deleter = source_integration_transaction(&pd, false).await;
    for index in 0..KEY_COUNT {
        deleter
            .delete(source_ticlient_key("multi", index))
            .await
            .unwrap();
    }
    Box::pin(deleter.commit()).await.unwrap();

    let mut after_delete = source_integration_transaction(&pd, false).await;
    for index in 0..KEY_COUNT {
        assert_eq!(
            after_delete
                .get(source_ticlient_key("multi", index))
                .await
                .unwrap(),
            None
        );
    }
    after_delete.rollback().await.unwrap();
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_ticlient_test_TestNotExist() {
    let (_cluster, pd) = source_integration_store();
    let mut transaction = source_integration_transaction(&pd, false).await;
    assert_eq!(
        transaction
            .get(b"~ticlient/not-exist/no-such-key".to_vec())
            .await
            .unwrap(),
        None
    );
    transaction.rollback().await.unwrap();
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_ticlient_test_TestLargeRequest() {
    let (_cluster, pd) = source_integration_store();
    let mut transaction = source_integration_transaction(&pd, false).await;
    transaction
        .get_mem_buffer()
        .set_entry_size_limit(1024 * 1024, 100 * 1024 * 1024);
    let error = transaction
        .put(
            b"~ticlient/large-request/key".to_vec(),
            vec![0; 9 * 1024 * 1024],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::StringError(ref message) if message.contains("entry size too large")
    ));
    assert_eq!(Box::pin(transaction.commit()).await.unwrap(), None);
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_ticlient_test_TestSplitRegionIn2PC() {
    std::thread::Builder::new()
        .name("client-go-TestSplitRegionIn2PC".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    const THRESHOLD: usize = 500;
                    let old_detect =
                        super::PRE_SPLIT_DETECT_THRESHOLD.swap(THRESHOLD as u32, Ordering::SeqCst);
                    let _detect_restore =
                        SourceAtomicU32Restore(&super::PRE_SPLIT_DETECT_THRESHOLD, old_detect);
                    let old_size = super::PRE_SPLIT_SIZE_THRESHOLD.swap(5_000, Ordering::SeqCst);
                    let _size_restore =
                        SourceAtomicU32Restore(&super::PRE_SPLIT_SIZE_THRESHOLD, old_size);

                    for (prefix, pessimistic) in [("optimistic", false), ("pessimistic", true)] {
                        let (_cluster, pd) = source_integration_store();
                        let start = source_ticlient_key(prefix, 0);
                        let end = source_ticlient_key(prefix, THRESHOLD);
                        let before_start = crate::pd::PdClient::region_for_key(
                            pd.as_ref(),
                            &Key::from(start.clone()),
                        )
                        .await
                        .unwrap()
                        .id();
                        let before_end = crate::pd::PdClient::region_for_key(
                            pd.as_ref(),
                            &Key::from(end.clone()),
                        )
                        .await
                        .unwrap()
                        .id();
                        assert_eq!(before_start, before_end);

                        let options = if pessimistic {
                            TransactionOptions::new_pessimistic()
                        } else {
                            TransactionOptions::new_optimistic()
                        }
                        .heartbeat_option(HeartbeatOption::NoHeartbeat)
                        .drop_check(CheckLevel::None);
                        let mut transaction =
                            source_integration_transaction_with_options(&pd, options).await;
                        if pessimistic {
                            transaction
                                .lock_keys(
                                    (0..THRESHOLD).map(|index| source_ticlient_key(prefix, index)),
                                )
                                .await
                                .unwrap();
                            let locked_start = crate::pd::PdClient::region_for_key(
                                pd.as_ref(),
                                &Key::from(start.clone()),
                            )
                            .await
                            .unwrap()
                            .id();
                            let locked_end = crate::pd::PdClient::region_for_key(
                                pd.as_ref(),
                                &Key::from(end.clone()),
                            )
                            .await
                            .unwrap()
                            .id();
                            assert_ne!(locked_start, locked_end);
                        }
                        for index in 0..THRESHOLD {
                            transaction
                                .put(
                                    source_ticlient_key(prefix, index),
                                    format!("value{index:08}").into_bytes(),
                                )
                                .await
                                .unwrap();
                        }
                        Box::pin(transaction.commit()).await.unwrap();

                        let after_start =
                            crate::pd::PdClient::region_for_key(pd.as_ref(), &Key::from(start))
                                .await
                                .unwrap()
                                .id();
                        let after_end =
                            crate::pd::PdClient::region_for_key(pd.as_ref(), &Key::from(end))
                                .await
                                .unwrap()
                                .id();
                        assert_ne!(after_start, after_end);
                    }
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_isolation_test_TestWriteWriteConflict() {
    std::thread::Builder::new()
        .name("client-go-TestWriteWriteConflict".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(16 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    const THREAD_COUNT: usize = 10;
                    const SETS_PER_THREAD: usize = 50;
                    let (_cluster, pd) = source_integration_store();
                    let key = b"~isolation/write-write/k".to_vec();
                    let mut workers = Vec::with_capacity(THREAD_COUNT);
                    for _ in 0..THREAD_COUNT {
                        let pd = Arc::clone(&pd);
                        let key = key.clone();
                        workers.push(tokio::spawn(async move {
                            let mut writes = Vec::with_capacity(SETS_PER_THREAD);
                            for _ in 0..SETS_PER_THREAD {
                                writes.push(source_isolation_write(&pd, &key, b"v".to_vec()).await);
                            }
                            writes
                        }));
                    }
                    let mut writes = Vec::with_capacity(THREAD_COUNT * SETS_PER_THREAD);
                    for worker in workers {
                        writes.extend(worker.await.unwrap());
                    }
                    writes.sort_unstable_by_key(|(start_ts, _)| *start_ts);
                    assert_eq!(writes.len(), THREAD_COUNT * SETS_PER_THREAD);
                    for pair in writes.windows(2) {
                        assert!(
                            pair[0].1 < pair[1].0,
                            "committed transactions overlap: {:?} then {:?}",
                            pair[0],
                            pair[1]
                        );
                    }
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_isolation_test_TestReadWriteConflict() {
    const READER_COUNT: usize = 10;
    const WRITE_COUNT: usize = 10;
    let (_cluster, pd) = source_integration_store();
    let key = b"~isolation/read-write/k".to_vec();
    let initial = source_isolation_write(&pd, &key, b"0".to_vec()).await;
    let done = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(tokio::sync::Barrier::new(READER_COUNT + 1));
    let mut readers = Vec::with_capacity(READER_COUNT);
    for _ in 0..READER_COUNT {
        let pd = Arc::clone(&pd);
        let key = key.clone();
        let done = Arc::clone(&done);
        let barrier = Arc::clone(&barrier);
        readers.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut reads = Vec::new();
            while !done.load(Ordering::Acquire) {
                let mut transaction = source_integration_transaction(&pd, false).await;
                let start_ts = transaction.timestamp.version();
                if let Ok(Some(entry)) = transaction
                    .get_with_options(key.clone(), &[GetOption::ReturnCommitTs])
                    .await
                {
                    reads.push((start_ts, entry.value, entry.commit_ts));
                }
                tokio::task::yield_now().await;
            }
            reads
        }));
    }

    barrier.wait().await;
    let mut versions = vec![(initial.1, b"0".to_vec())];
    for index in 1..=WRITE_COUNT {
        let (_, commit_ts) =
            source_isolation_write(&pd, &key, index.to_string().into_bytes()).await;
        versions.push((commit_ts, index.to_string().into_bytes()));
        tokio::time::sleep(Duration::from_micros(10)).await;
    }
    done.store(true, Ordering::Release);

    let mut reads = Vec::new();
    for reader in readers {
        reads.extend(reader.await.unwrap());
    }
    assert!(!reads.is_empty());
    for (start_ts, value, commit_ts) in reads {
        let expected = versions
            .iter()
            .filter(|(version, _)| *version < start_ts)
            .max_by_key(|(version, _)| *version)
            .expect("the initial value precedes every reader");
        assert_eq!(commit_ts, expected.0);
        assert_eq!(value, expected.1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestSharedLockBlockExclusiveLock() {
    for commit in [true, false] {
        let (cluster, pd) = source_integration_store();
        let suffix = if commit { "commit" } else { "rollback" };
        let key = format!("~shared_lock/shared-block-exclusive/{suffix}/key").into_bytes();
        let mut first = source_shared_transaction(&pd).await;
        let mut second = source_shared_transaction(&pd).await;
        let mut exclusive = source_shared_transaction(&pd).await;
        source_shared_lock_primary(
            &mut first,
            &pd,
            format!("~shared_lock/shared-block-exclusive/{suffix}/p1").as_bytes(),
        )
        .await;
        source_shared_lock_primary(
            &mut second,
            &pd,
            format!("~shared_lock/shared-block-exclusive/{suffix}/p2").as_bytes(),
        )
        .await;
        source_shared_lock_key(&mut first, &pd, &key).await.unwrap();
        source_shared_lock_key(&mut second, &pd, &key)
            .await
            .unwrap();
        assert!(second.buffer.is_shared_locked(&Key::from(key.clone())));
        source_shared_lock_primary(
            &mut exclusive,
            &pd,
            format!("~shared_lock/shared-block-exclusive/{suffix}/p3").as_bytes(),
        )
        .await;

        let blocker = tokio::spawn(async move {
            let result = exclusive
                .lock_keys_with_wait_time(1_000, [key.clone()])
                .await;
            (exclusive, result)
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!blocker.is_finished());
        if commit {
            Box::pin(first.commit()).await.unwrap();
            Box::pin(second.commit()).await.unwrap();
        } else {
            first.rollback().await.unwrap();
            second.rollback().await.unwrap();
        }
        let (mut exclusive, result) = tokio::time::timeout(Duration::from_secs(3), blocker)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_err(), "exclusive lock wait must report conflict");
        exclusive.rollback().await.unwrap();
        assert!(source_shared_locks(&cluster, b"", u64::MAX).is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestExclusiveLockBlockSharedLock() {
    for commit in [true, false] {
        let (_cluster, pd) = source_integration_store();
        let suffix = if commit { "commit" } else { "rollback" };
        let key = format!("~shared_lock/exclusive-block-shared/{suffix}/key").into_bytes();
        let mut exclusive = source_shared_transaction(&pd).await;
        source_shared_lock_primary(
            &mut exclusive,
            &pd,
            format!("~shared_lock/exclusive-block-shared/{suffix}/p1").as_bytes(),
        )
        .await;
        exclusive
            .lock_keys_with_wait_time(1_000, [key.clone()])
            .await
            .unwrap();

        let mut first_shared = source_shared_transaction(&pd).await;
        let mut second_shared = source_shared_transaction(&pd).await;
        source_shared_lock_primary(
            &mut first_shared,
            &pd,
            format!("~shared_lock/exclusive-block-shared/{suffix}/p2").as_bytes(),
        )
        .await;
        source_shared_lock_primary(
            &mut second_shared,
            &pd,
            format!("~shared_lock/exclusive-block-shared/{suffix}/p3").as_bytes(),
        )
        .await;
        let first_pd = pd.clone();
        let first_key = key.clone();
        let first = tokio::spawn(async move {
            let result = source_shared_lock_key(&mut first_shared, &first_pd, &first_key).await;
            (first_shared, result)
        });
        let second_pd = pd.clone();
        let second_key = key.clone();
        let second = tokio::spawn(async move {
            let result = source_shared_lock_key(&mut second_shared, &second_pd, &second_key).await;
            (second_shared, result)
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!first.is_finished());
        assert!(!second.is_finished());
        if commit {
            Box::pin(exclusive.commit()).await.unwrap();
        } else {
            exclusive.rollback().await.unwrap();
        }
        let (mut first_shared, first_result) = first.await.unwrap();
        let (mut second_shared, second_result) = second.await.unwrap();
        assert!(first_result.is_err());
        assert!(second_result.is_err());
        first_shared.rollback().await.unwrap();
        second_shared.rollback().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestResolveSharedLock() {
    let (cluster, pd) = source_integration_store();
    let primary = b"~shared_lock/resolve/p1".to_vec();
    let key = b"~shared_lock/resolve/key".to_vec();
    let mut first = source_shared_transaction(&pd).await;
    source_shared_lock_primary(&mut first, &pd, &primary).await;
    source_shared_lock_key(&mut first, &pd, &key).await.unwrap();
    let start_ts = first.timestamp.version();
    let super::TransactionKind::Pessimistic(for_update_ts) = &first.options.kind else {
        unreachable!()
    };
    let for_update_ts = for_update_ts.version();
    assert_eq!(
        cluster.engine().prewrite(&unistore::PrewriteRequest {
            mutations: vec![
                unistore::TxnMutation {
                    op: unistore::Op::Lock,
                    key: primary.clone(),
                    value: Vec::new(),
                    assertion: unistore::Assertion::None,
                },
                unistore::TxnMutation {
                    op: unistore::Op::SharedLock,
                    key: key.clone(),
                    value: Vec::new(),
                    assertion: unistore::Assertion::None,
                },
            ],
            primary: primary.clone(),
            start_ts,
            ttl: 3_000,
            for_update_ts,
            min_commit_ts: start_ts + 1,
            pessimistic_actions: vec![
                unistore::PessimisticAction::DoCheck,
                unistore::PessimisticAction::DoCheck,
            ],
            ..Default::default()
        }),
        [None, None]
    );
    let commit_ts = crate::pd::PdClient::get_timestamp(pd.clone())
        .await
        .unwrap()
        .version();
    cluster
        .engine()
        .commit(std::slice::from_ref(&primary), start_ts, commit_ts)
        .unwrap();
    let locks = source_shared_locks(&cluster, &key, u64::MAX);
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].start_ts, start_ts);
    assert_eq!(locks[0].lock_type, unistore::Op::SharedLock);

    let second_primary = b"~shared_lock/resolve/p2".to_vec();
    let mut second = source_shared_transaction(&pd).await;
    source_shared_lock_primary(&mut second, &pd, &second_primary).await;
    second
        .lock_keys_with_wait_time(1_000, [key.clone()])
        .await
        .unwrap();
    let locks = source_shared_locks(&cluster, &key, u64::MAX);
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].start_ts, second.timestamp.version());
    assert_eq!(locks[0].lock_type, unistore::Op::PessimisticLock);
    second.rollback().await.unwrap();
    assert!(source_shared_locks(&cluster, &key, u64::MAX).is_empty());
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestScanSharedLock() {
    let (cluster, pd) = source_integration_store();
    let key = b"~shared_lock/scan/key".to_vec();
    let mut transactions = Vec::new();
    for index in 0..3 {
        let mut transaction = source_shared_transaction(&pd).await;
        source_shared_lock_primary(
            &mut transaction,
            &pd,
            format!("~shared_lock/scan/p{index}").as_bytes(),
        )
        .await;
        source_shared_lock_key(&mut transaction, &pd, &key)
            .await
            .unwrap();
        transactions.push(transaction);
    }
    for (index, transaction) in transactions.iter().enumerate() {
        let locks = source_shared_locks(&cluster, &key, transaction.timestamp.version());
        assert_eq!(locks.len(), index + 1);
        assert!(locks.iter().all(|lock| {
            lock.key == key
                && lock.start_ts <= transaction.timestamp.version()
                && lock.lock_type == unistore::Op::SharedPessimisticLock
        }));
    }
    assert!(
        source_shared_locks(&cluster, &key, transactions[0].timestamp.version() - 1).is_empty()
    );
    for transaction in &mut transactions {
        transaction.rollback().await.unwrap();
    }
    assert!(source_shared_locks(&cluster, &key, u64::MAX).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestSharedLockCommitAndRollback() {
    for commit in [true, false] {
        let (cluster, pd) = source_integration_store();
        let suffix = if commit { "commit" } else { "rollback" };
        let key = format!("~shared_lock/finish/{suffix}/key").into_bytes();
        let mut transactions = Vec::new();
        for index in 0..3 {
            let mut transaction = source_shared_transaction(&pd).await;
            source_shared_lock_primary(
                &mut transaction,
                &pd,
                format!("~shared_lock/finish/{suffix}/p{index}").as_bytes(),
            )
            .await;
            source_shared_lock_key(&mut transaction, &pd, &key)
                .await
                .unwrap();
            transactions.push(transaction);
        }
        source_wait_shared_locks(&cluster, &key, u64::MAX, 3).await;
        for (index, transaction) in transactions.iter_mut().enumerate() {
            let finished_start_ts = transaction.timestamp.version();
            if commit {
                Box::pin(transaction.commit()).await.unwrap();
            } else {
                transaction.rollback().await.unwrap();
            }
            let locks = source_wait_shared_locks(&cluster, &key, u64::MAX, 2 - index).await;
            assert!(locks.iter().all(|lock| lock.start_ts != finished_start_ts));
        }
        assert!(source_shared_locks(&cluster, &key, u64::MAX).is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestGCSharedLock() {
    let old_ttl = super::MANAGED_LOCK_TTL.swap(1_000, Ordering::SeqCst);
    let _ttl_restore = SourceAtomicU64Restore(&super::MANAGED_LOCK_TTL, old_ttl);
    let (cluster, pd) = source_integration_store();
    let key = b"~shared_lock/gc/key".to_vec();
    let mut transactions = Vec::new();
    for index in 0..3 {
        let mut transaction = source_shared_transaction(&pd).await;
        source_shared_lock_primary(
            &mut transaction,
            &pd,
            format!("~shared_lock/gc/p{index}").as_bytes(),
        )
        .await;
        source_shared_lock_key(&mut transaction, &pd, &key)
            .await
            .unwrap();
        transactions.push(transaction);
    }
    transactions[0].reset_auto_heartbeat();
    transactions[1].reset_auto_heartbeat();

    let locks = source_wait_shared_locks(&cluster, &key, u64::MAX, 3).await;
    assert!(locks
        .iter()
        .all(|lock| lock.lock_type == unistore::Op::SharedPessimisticLock));
    tokio::time::sleep(Duration::from_millis(1_200)).await;

    let live = crate::transaction::resolve_locks_with_context(
        locks.into_iter().map(source_proto_shared_lock).collect(),
        Timestamp::from_version(0),
        Arc::clone(&pd),
        Keyspace::Disable,
        None,
        ResolveLocksContext::default(),
    )
    .await
    .unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].lock_version, transactions[2].timestamp.version());
    let remaining = source_wait_shared_locks(&cluster, &key, u64::MAX, 1).await;
    assert_eq!(remaining[0].start_ts, transactions[2].timestamp.version());
    transactions[2].rollback().await.unwrap();
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_shared_lock_test_TestPrewriteResolveExpiredSharedLock() {
    std::thread::Builder::new()
        .name("client-go-TestPrewriteResolveExpiredSharedLock".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(16 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let old_ttl = super::MANAGED_LOCK_TTL.swap(500, Ordering::SeqCst);
                    let _ttl_restore = SourceAtomicU64Restore(&super::MANAGED_LOCK_TTL, old_ttl);
                    let (cluster, pd) = source_integration_store();
                    let key = b"~shared_lock/prewrite-expired/key".to_vec();
                    let mut expired = source_shared_transaction(&pd).await;
                    source_shared_lock_primary(
                        &mut expired,
                        &pd,
                        b"~shared_lock/prewrite-expired/primary",
                    )
                    .await;
                    source_shared_lock_key(&mut expired, &pd, &key)
                        .await
                        .unwrap();
                    source_wait_shared_locks(&cluster, &key, u64::MAX, 1).await;
                    expired.reset_auto_heartbeat();
                    tokio::time::sleep(Duration::from_millis(700)).await;
                    assert_eq!(source_shared_locks(&cluster, &key, u64::MAX).len(), 1);

                    let value = b"value_from_contender".to_vec();
                    let mut contender = source_integration_transaction(&pd, false).await;
                    contender.put(key.clone(), value.clone()).await.unwrap();
                    let commit_ts = Box::pin(contender.commit())
                        .await
                        .unwrap()
                        .unwrap()
                        .version();
                    assert_eq!(
                        source_engine_value(&cluster, &key, commit_ts),
                        Some((value, commit_ts))
                    );
                    source_wait_shared_locks(&cluster, &key, u64::MAX, 0).await;
                    expired.rollback().await.unwrap();
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_shared_lock_test_TestPrewriteResolveExpiredSharedLockWithActiveHolder(
) {
    std::thread::Builder::new()
        .name("client-go-TestPrewriteResolveExpiredSharedLockWithActiveHolder".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .thread_stack_size(16 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let old_ttl = super::MANAGED_LOCK_TTL.swap(500, Ordering::SeqCst);
                    let _ttl_restore = SourceAtomicU64Restore(&super::MANAGED_LOCK_TTL, old_ttl);
                    let (cluster, pd) = source_integration_store();
                    let key = b"~shared_lock/prewrite-mixed/key".to_vec();
                    let mut expired = source_shared_transaction(&pd).await;
                    let mut active = source_shared_transaction(&pd).await;
                    source_shared_lock_primary(
                        &mut expired,
                        &pd,
                        b"~shared_lock/prewrite-mixed/expired",
                    )
                    .await;
                    source_shared_lock_key(&mut expired, &pd, &key)
                        .await
                        .unwrap();
                    source_shared_lock_primary(
                        &mut active,
                        &pd,
                        b"~shared_lock/prewrite-mixed/active",
                    )
                    .await;
                    source_shared_lock_key(&mut active, &pd, &key)
                        .await
                        .unwrap();
                    source_wait_shared_locks(&cluster, &key, u64::MAX, 2).await;
                    expired.reset_auto_heartbeat();
                    tokio::time::sleep(Duration::from_millis(700)).await;

                    let value = b"contender-value".to_vec();
                    let mut contender = source_integration_transaction(&pd, false).await;
                    contender.put(key.clone(), value.clone()).await.unwrap();
                    let commit_done = tokio::spawn(async move {
                        let result = Box::pin(contender.commit()).await;
                        (contender, result)
                    });
                    let remaining = source_wait_shared_locks(&cluster, &key, u64::MAX, 1).await;
                    assert_eq!(remaining[0].start_ts, active.timestamp.version());
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    assert!(!commit_done.is_finished());

                    active.rollback().await.unwrap();
                    let (_contender, result) =
                        tokio::time::timeout(Duration::from_secs(5), commit_done)
                            .await
                            .unwrap()
                            .unwrap();
                    let commit_ts = result.unwrap().unwrap().version();
                    assert_eq!(
                        source_engine_value(&cluster, &key, commit_ts),
                        Some((value, commit_ts))
                    );
                    expired.rollback().await.unwrap();
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestForceLockRetryOnSharedLock() {
    let (cluster, pd) = source_integration_store();
    let key = b"~shared_lock/force/key".to_vec();
    let mut shared = source_shared_transaction(&pd).await;
    source_shared_lock_primary(&mut shared, &pd, b"~shared_lock/force/p1").await;
    source_shared_lock_key(&mut shared, &pd, &key)
        .await
        .unwrap();
    source_wait_shared_locks(&cluster, &key, u64::MAX, 1).await;

    let mut exclusive = source_shared_transaction(&pd).await;
    source_shared_lock_primary(&mut exclusive, &pd, b"~shared_lock/force/p2").await;
    exclusive.start_aggressive_locking();
    let force_key = key.clone();
    let force = tokio::spawn(async move {
        let result = exclusive.lock_keys_with_wait_time(1_000, [force_key]).await;
        (exclusive, result)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!force.is_finished());

    shared.rollback().await.unwrap();
    let (mut exclusive, result) = tokio::time::timeout(Duration::from_secs(5), force)
        .await
        .unwrap()
        .unwrap();
    result.unwrap();
    exclusive.done_aggressive_locking().await.unwrap();
    let locks = source_wait_shared_locks(&cluster, &key, u64::MAX, 1).await;
    assert_eq!(locks[0].start_ts, exclusive.timestamp.version());
    assert_eq!(locks[0].lock_type, unistore::Op::PessimisticLock);
    exclusive.rollback().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_test_TestRepeatableRead() {
    for pessimistic in [false, true] {
        let (cluster, pd) = source_integration_store();
        let key = if pessimistic {
            b"~async_commit/repeatable/pessimistic".to_vec()
        } else {
            b"~async_commit/repeatable/optimistic".to_vec()
        };
        source_async_commit_put(&pd, &key, b"v1").await;

        let mut first = source_async_commit_transaction(&pd).await;
        let (commit_info, commit_callback) = source_commit_info_probe();
        first.set_commit_callback(commit_callback);
        first.set_pessimistic(pessimistic);
        assert_eq!(first.get(key.clone()).await.unwrap(), Some(b"v1".to_vec()));
        first.put(key.clone(), b"v2".to_vec()).await.unwrap();
        for _ in 0..20 {
            crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
                .await
                .unwrap();
        }

        let mut snapshot = source_async_commit_transaction(&pd).await;
        assert_eq!(
            snapshot.get(key.clone()).await.unwrap(),
            Some(b"v1".to_vec())
        );
        Box::pin(first.commit()).await.unwrap();
        assert_eq!(
            commit_info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
            "async_commit"
        );
        assert_eq!(
            snapshot.get(key.clone()).await.unwrap(),
            Some(b"v1".to_vec())
        );
        snapshot.rollback().await.unwrap();

        let mut latest = source_async_commit_transaction(&pd).await;
        let latest_value = latest.get(key.clone()).await.unwrap();
        assert_eq!(
            latest_value,
            Some(b"v2".to_vec()),
            "pessimistic={pessimistic}, mvcc={:?}",
            cluster.engine().mvcc_get_by_key(&key)
        );
        latest.rollback().await.unwrap();
    }
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_test_TestAsyncCommitLinearizability() {
    let (_cluster, pd) = source_integration_store();
    let mut first = source_async_commit_transaction(&pd).await;
    let mut second = source_async_commit_transaction(&pd).await;
    first.set_causal_consistency(false);
    second.set_causal_consistency(false);
    first
        .put(b"~async_commit/linear/a".to_vec(), b"a1".to_vec())
        .await
        .unwrap();
    second
        .put(b"~async_commit/linear/b".to_vec(), b"b1".to_vec())
        .await
        .unwrap();
    let second_commit = Box::pin(second.commit()).await.unwrap().unwrap().version();
    let first_commit = Box::pin(first.commit()).await.unwrap().unwrap().version();
    assert!(second_commit < first_commit);
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_test_TestAsyncCommitWithMultiDC() {
    let (_cluster, pd) = source_integration_store();
    let mut local = source_async_commit_transaction(&pd).await;
    let (local_info, local_callback) = source_commit_info_probe();
    local.set_commit_callback(local_callback);
    local.set_scope("bj");
    local
        .put(b"~async_commit/scope/local".to_vec(), b"v1".to_vec())
        .await
        .unwrap();
    Box::pin(local.commit()).await.unwrap();
    assert_eq!(
        local_info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
        "2pc"
    );

    let mut global = source_async_commit_transaction(&pd).await;
    let (global_info, global_callback) = source_commit_info_probe();
    global.set_commit_callback(global_callback);
    global.set_scope(crate::oracle::GLOBAL_TXN_SCOPE);
    global
        .put(b"~async_commit/scope/global".to_vec(), b"v2".to_vec())
        .await
        .unwrap();
    Box::pin(global.commit()).await.unwrap();
    assert_eq!(
        global_info.lock().unwrap().as_ref().unwrap()["txn_commit_mode"],
        "async_commit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_test_TestAsyncCommitLifecycleHooks() {
    let (_cluster, pd) = source_integration_store();
    let reached_pre = Arc::new(AtomicBool::new(false));
    let reached_post = Arc::new(AtomicBool::new(false));
    let post_done = Arc::new(tokio::sync::Notify::new());
    let mut transaction = source_async_commit_transaction(&pd).await;
    let pre = Arc::clone(&reached_pre);
    let post_pre = Arc::clone(&reached_pre);
    let post = Arc::clone(&reached_post);
    let notify = Arc::clone(&post_done);
    transaction.set_background_task_lifecycle_hooks(super::LifecycleHooks {
        pre: Some(Arc::new(move || {
            pre.store(true, Ordering::SeqCst);
        })),
        post: Some(Arc::new(move || {
            assert!(post_pre.load(Ordering::SeqCst));
            post.store(true, Ordering::SeqCst);
            notify.notify_one();
        })),
    });
    transaction
        .put(b"~async_commit/hooks/a".to_vec(), b"a".to_vec())
        .await
        .unwrap();
    transaction
        .put(b"~async_commit/hooks/z".to_vec(), b"z".to_vec())
        .await
        .unwrap();
    Box::pin(transaction.commit()).await.unwrap();
    assert!(reached_pre.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_secs(5), post_done.notified())
        .await
        .unwrap();
    assert!(reached_post.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_async_commit_test_TestCheckSecondaries() {
    async fn prepare(
        cluster: &crate::mock::mocktikv::Cluster,
        pd: &Arc<crate::mock::mocktikv::MockPdClient>,
        prefix: &str,
        secondary_count: usize,
    ) -> (Vec<Vec<u8>>, u64, u64) {
        let mut keys = (0..secondary_count)
            .map(|index| format!("~async_commit/check/{prefix}/s{index}").into_bytes())
            .collect::<Vec<_>>();
        keys.push(format!("~async_commit/check/{prefix}/primary").into_bytes());
        let primary = keys.last().unwrap().clone();
        let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
            .await
            .unwrap()
            .version();
        let min_commit_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
            .await
            .unwrap()
            .version();
        assert!(cluster
            .engine()
            .prewrite(&unistore::PrewriteRequest {
                mutations: keys
                    .iter()
                    .map(|key| unistore::TxnMutation::put(key.clone(), b"new".to_vec()))
                    .collect(),
                primary,
                start_ts,
                ttl: 3_000,
                min_commit_ts,
                use_async_commit: true,
                secondaries: keys[..secondary_count].to_vec(),
                ..Default::default()
            })
            .iter()
            .all(Option::is_none));
        (keys, start_ts, min_commit_ts)
    }

    async fn wait_resolved(cluster: &crate::mock::mocktikv::Cluster, keys: &[Vec<u8>]) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if keys
                    .iter()
                    .all(|key| cluster.engine().mvcc_get_by_key(key).lock.is_none())
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("client-go CheckSecondaries cleanup did not finish");
    }

    let (cluster, pd) = source_integration_store();
    let (keys, _start_ts, min_commit_ts) = prepare(&cluster, &pd, "primary-only", 0).await;
    let mut reader = source_integration_transaction(&pd, false).await;
    assert_eq!(
        reader.get(keys[0].clone()).await.unwrap(),
        Some(b"new".to_vec())
    );
    wait_resolved(&cluster, &keys).await;
    assert_eq!(
        source_engine_value(&cluster, &keys[0], u64::MAX),
        Some((b"new".to_vec(), min_commit_ts))
    );

    let (keys, start_ts, min_commit_ts) = prepare(&cluster, &pd, "committed", 2).await;
    let commit_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version()
        .max(min_commit_ts);
    cluster
        .engine()
        .commit(std::slice::from_ref(&keys[1]), start_ts, commit_ts)
        .unwrap();
    let mut reader = source_integration_transaction(&pd, false).await;
    assert_eq!(
        reader.get(keys[0].clone()).await.unwrap(),
        Some(b"new".to_vec())
    );
    wait_resolved(&cluster, &keys).await;
    for key in &keys {
        assert_eq!(
            source_engine_value(&cluster, key, u64::MAX),
            Some((b"new".to_vec(), commit_ts))
        );
    }

    let (keys, start_ts, _min_commit_ts) = prepare(&cluster, &pd, "rolled-back", 2).await;
    cluster
        .engine()
        .rollback(std::slice::from_ref(&keys[1]), start_ts)
        .unwrap();
    let mut reader = source_integration_transaction(&pd, false).await;
    assert_eq!(reader.get(keys[0].clone()).await.unwrap(), None);
    wait_resolved(&cluster, &keys).await;
    for key in &keys {
        assert!(cluster.engine().mvcc_get_by_key(key).lock.is_none());
        assert_eq!(source_engine_value(&cluster, key, u64::MAX), None);
    }
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_async_commit_test_TestPessimisticTxnResolveAsyncCommitLock() {
    source_run_async_on_large_stack(
        "client-go-TestPessimisticTxnResolveAsyncCommitLock",
        || async {
            let (cluster, pd) = source_integration_store();
            let key = b"~async_commit/pessimistic-resolve/key".to_vec();
            let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
                .await
                .unwrap()
                .version();
            let min_commit_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
                .await
                .unwrap()
                .version();
            assert_eq!(
                cluster.engine().prewrite(&unistore::PrewriteRequest {
                    mutations: vec![unistore::TxnMutation::put(
                        key.clone(),
                        b"async-value".to_vec(),
                    )],
                    primary: key.clone(),
                    start_ts,
                    ttl: 3_000,
                    min_commit_ts,
                    use_async_commit: true,
                    ..Default::default()
                }),
                [None]
            );

            let mut transaction = source_shared_transaction(&pd).await;
            source_shared_lock_primary(
                &mut transaction,
                &pd,
                b"~async_commit/pessimistic-resolve/primary",
            )
            .await;
            transaction
                .put(key.clone(), b"pessimistic-value".to_vec())
                .await
                .unwrap();
            Box::pin(transaction.commit()).await.unwrap();
            let mut reader = source_integration_transaction(&pd, false).await;
            assert_eq!(
                reader.get(key).await.unwrap(),
                Some(b"pessimistic-value".to_vec())
            );
        },
    );
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_async_commit_test_TestResolveTxnFallbackFromAsyncCommit() {
    source_run_async_on_large_stack(
        "client-go-TestResolveTxnFallbackFromAsyncCommit",
        || async {
            for (index, (fallback_primary, fallback_secondary, read_secondary_first)) in [
                (true, false, false),
                (true, false, true),
                (false, true, false),
                (false, true, true),
                (true, true, false),
                (true, true, true),
            ]
            .into_iter()
            .enumerate()
            {
                let (cluster, pd) = source_integration_store();
                let primary = format!("~async_commit/fallback/{index}/primary").into_bytes();
                let secondary = format!("~async_commit/fallback/{index}/secondary").into_bytes();
                let initial_start = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
                    .await
                    .unwrap()
                    .version();
                assert_eq!(
                    cluster.engine().prewrite(&unistore::PrewriteRequest {
                        mutations: vec![
                            unistore::TxnMutation::put(primary.clone(), b"p0".to_vec()),
                            unistore::TxnMutation::put(secondary.clone(), b"s0".to_vec()),
                        ],
                        primary: primary.clone(),
                        start_ts: initial_start,
                        ttl: 3_000,
                        ..Default::default()
                    }),
                    [None, None]
                );
                let initial_commit = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
                    .await
                    .unwrap()
                    .version();
                cluster
                    .engine()
                    .commit(
                        &[primary.clone(), secondary.clone()],
                        initial_start,
                        initial_commit,
                    )
                    .unwrap();

                let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
                    .await
                    .unwrap()
                    .version();
                let min_commit_ts = start_ts + 1;
                let primary_secondaries = if fallback_primary {
                    Vec::new()
                } else {
                    vec![secondary.clone()]
                };
                assert_eq!(
                    cluster.engine().prewrite(&unistore::PrewriteRequest {
                        mutations: vec![unistore::TxnMutation::put(
                            primary.clone(),
                            b"p1".to_vec(),
                        )],
                        primary: primary.clone(),
                        start_ts,
                        ttl: 1,
                        min_commit_ts,
                        use_async_commit: !fallback_primary,
                        secondaries: primary_secondaries,
                        ..Default::default()
                    }),
                    [None]
                );
                assert_eq!(
                    cluster.engine().prewrite(&unistore::PrewriteRequest {
                        mutations: vec![unistore::TxnMutation::put(
                            secondary.clone(),
                            b"s1".to_vec(),
                        )],
                        primary: primary.clone(),
                        start_ts,
                        ttl: 1,
                        min_commit_ts,
                        use_async_commit: !fallback_secondary,
                        ..Default::default()
                    }),
                    [None]
                );
                tokio::time::sleep(Duration::from_millis(3)).await;

                let order = if read_secondary_first {
                    [(&secondary, b"s0".as_slice()), (&primary, b"p0".as_slice())]
                } else {
                    [(&primary, b"p0".as_slice()), (&secondary, b"s0".as_slice())]
                };
                for (key, expected) in order {
                    let mut reader = source_integration_transaction(&pd, false).await;
                    assert_eq!(
                    reader.get(key.clone()).await.unwrap(),
                    Some(expected.to_vec()),
                    "case {index}, fallback_primary={fallback_primary}, fallback_secondary={fallback_secondary}, read_secondary_first={read_secondary_first}"
                );
                }
            }
        },
    );
}
