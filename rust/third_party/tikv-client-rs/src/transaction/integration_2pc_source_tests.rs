// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

// Direct ports of client-go/integration_tests/2pc_test.go. Keep every source
// method independently selectable while sharing only mechanical setup.

fn source_2pc_store_with_splits(
    split_keys: &[Vec<u8>],
) -> (
    crate::mock::mocktikv::Cluster,
    Arc<crate::mock::mocktikv::MockPdClient>,
) {
    let (_client, cluster, pd) = crate::mock::mocktikv::new_tikv_and_pd_client("", None).unwrap();
    crate::mock::mocktikv::bootstrap_with_multi_regions(&cluster, split_keys);
    (cluster, Arc::new(pd))
}

fn source_2pc_committer(
    rpc: Arc<crate::mock::mocktikv::MockPdClient>,
    start_version: u64,
    primary_key: Option<Key>,
    mutations: Vec<kvrpcpb::Mutation>,
    options: TransactionOptions,
) -> Committer<crate::mock::mocktikv::MockPdClient> {
    let write_size = mutations.iter().fold(0_u64, |total, mutation| {
        total.saturating_add((mutation.key.len() + mutation.value.len()) as u64)
    });
    Committer::new(
        primary_key,
        mutations,
        Timestamp::from_version(start_version),
        rpc,
        options,
        CommitSettings::default(),
        Keyspace::Disable,
        None,
        None,
        None,
        None,
        None,
        ResolveLocksContext::default(),
        PipelinedTransactionState::default(),
        write_size,
        write_size,
        std::time::Instant::now(),
    )
}

fn source_2pc_run<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + 'static,
{
    std::thread::Builder::new()
        .name("client-go-2pc-port".to_owned())
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_stack_size(64 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(test());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn source_2pc_assert_values(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    pairs: &[(&[u8], &[u8])],
) {
    let mut transaction = source_integration_transaction(pd, false).await;
    for (key, value) in pairs {
        assert_eq!(
            transaction.get(key.to_vec()).await.unwrap().as_deref(),
            Some(*value)
        );
    }
}

async fn source_2pc_pessimistic_transaction(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
) -> Transaction<crate::mock::mocktikv::MockPdClient> {
    source_integration_transaction_with_options(
        pd,
        TransactionOptions::new_pessimistic()
            .heartbeat_option(HeartbeatOption::NoHeartbeat)
            .drop_check(CheckLevel::None),
    )
    .await
}

fn source_2pc_key(prefix: &str, name: &str) -> Vec<u8> {
    format!("~2pc/{prefix}/{name}").into_bytes()
}

fn source_2pc_is_locked(cluster: &crate::mock::mocktikv::Cluster, key: &[u8]) -> bool {
    let info = cluster.engine().mvcc_get_by_key(key);
    info.lock.is_some() || !info.shared_locks.is_empty()
}

fn source_2pc_assert_primary(
    cluster: &crate::mock::mocktikv::Cluster,
    key: &[u8],
    primary: &[u8],
) {
    let lock = cluster.engine().mvcc_get_by_key(key).lock.unwrap();
    assert_eq!(lock.op, unistore::Op::PessimisticLock);
    assert_eq!(lock.primary, primary);
}

struct SourceManagedTtlGuard(u64);

impl SourceManagedTtlGuard {
    fn set(value: u64) -> Self {
        Self(super::MANAGED_LOCK_TTL.swap(value, Ordering::SeqCst))
    }
}

impl Drop for SourceManagedTtlGuard {
    fn drop(&mut self) {
        super::MANAGED_LOCK_TTL.store(self.0, Ordering::SeqCst);
    }
}

struct SourceAtomicU64Guard(&'static std::sync::atomic::AtomicU64, u64);

impl SourceAtomicU64Guard {
    fn set(value: &'static std::sync::atomic::AtomicU64, replacement: u64) -> Self {
        Self(value, value.swap(replacement, Ordering::SeqCst))
    }
}

impl Drop for SourceAtomicU64Guard {
    fn drop(&mut self) {
        self.0.store(self.1, Ordering::SeqCst);
    }
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestCommitRollback() {
    source_2pc_run(|| async move {
    let (_cluster, pd) = source_integration_store();
    let keys = [b"~2pc/rollback/a".to_vec(), b"~2pc/rollback/b".to_vec(), b"~2pc/rollback/c".to_vec()];
    let mut initial = source_integration_transaction(&pd, false).await;
    for key in &keys {
        initial.put(key.clone(), key.clone()).await.unwrap();
    }
    Box::pin(initial.commit()).await.unwrap();

    let mut stale = source_integration_transaction(&pd, false).await;
    for key in &keys {
        stale.put(key.clone(), b"stale".to_vec()).await.unwrap();
    }
    let mut winner = source_integration_transaction(&pd, false).await;
    winner.put(keys[2].clone(), b"winner".to_vec()).await.unwrap();
    Box::pin(winner.commit()).await.unwrap();
    assert!(Box::pin(stale.commit()).await.is_err());

    source_2pc_assert_values(
        &pd,
        &[
            (&keys[0], &keys[0]),
            (&keys[1], &keys[1]),
            (&keys[2], b"winner"),
        ],
    )
        .await;
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestCommitOnTiKVDiskFullOpt() {
    source_2pc_run(|| async move {
    let scenario = FailScenario::setup();
    crate::util::enable_failpoints();
    // fail-rs uses an unquoted boolean action for the same client-go failpoint.
    fail::cfg("tikvclient/rpcAllowedOnAlmostFull", "return(true)").unwrap();
    let (_cluster, pd) = source_integration_store();
    let allowed_key = b"~2pc/disk-full/allowed".to_vec();
    let mut allowed = source_integration_transaction(&pd, false).await;
    allowed.set_disk_full_option(kvrpcpb::DiskFullOpt::AllowedOnAlmostFull);
    allowed
        .put(allowed_key.clone(), b"allowed".to_vec())
        .await
        .unwrap();
    Box::pin(allowed.commit()).await.unwrap();
    source_2pc_assert_values(&pd, &[(&allowed_key, b"allowed")]).await;

    // Rust's transaction API does not accept a per-commit Context. Use a
    // zero-retry owner to exercise the same terminal result without waiting
    // for client-go's three-second context deadline.
    let mut denied = Transaction::new(
        crate::pd::PdClient::get_timestamp(pd.clone())
            .await
            .unwrap(),
        pd.clone(),
        TransactionOptions::new_optimistic().retry_options(RetryOptions::none()),
        Keyspace::Disable,
    );
    denied
        .put(b"~2pc/disk-full/denied".to_vec(), b"denied".to_vec())
        .await
        .unwrap();
    let error = Box::pin(denied.commit()).await.unwrap_err();
    assert!(matches!(
        error,
        crate::Error::RegionError(ref region_error) if region_error.disk_full.is_some()
    ), "{error:?}");
        scenario.teardown();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestIllegalTso() {
    source_2pc_run(|| async move {
    let (_cluster, pd) = source_integration_store();
    let mut transaction = Transaction::new(
        Timestamp::from_version(u64::MAX),
        pd,
        TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
        Keyspace::Disable,
    );
    transaction
        .put(b"~2pc/illegal-tso".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    let error = Box::pin(transaction.commit()).await.unwrap_err();
        assert!(error.to_string().contains("invalid txnStartTS"), "{error:?}");
    });
}

#[test]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestCommitBeforePrewrite() {
    let engine = unistore::MockEngine::new();
    let key = b"~2pc/commit-before-prewrite".to_vec();
    let start_ts = 10;
    engine.rollback(std::slice::from_ref(&key), start_ts).unwrap();
    let error = engine.prewrite(&unistore::PrewriteRequest {
        mutations: vec![unistore::TxnMutation::put(key.clone(), b"value".to_vec())],
        primary: key,
        start_ts,
        ttl: 3_000,
        min_commit_ts: start_ts + 1,
        ..Default::default()
    });
    assert!(matches!(
        error.as_slice(),
        [Some(unistore::MockError::AlreadyRolledBack { .. })]
    ));
}

#[test]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPessimisticPrewriteRequest() {
    source_2pc_run(|| async move {
        let key = b"~2pc/pessimistic-prewrite".to_vec();
        let requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PrewriteRequest>::new()));
        let captured = Arc::clone(&requests);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured.lock().unwrap().push(request.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected request in pessimistic prewrite metadata test");
            },
        )));
        let mut options = TransactionOptions::new_pessimistic()
            .heartbeat_option(HeartbeatOption::NoHeartbeat)
            .drop_check(CheckLevel::None);
        options.kind = super::TransactionKind::Pessimistic(Timestamp::from_version(100));
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(key.clone())),
            vec![source_test_mutation(key, kvrpcpb::Op::Put)],
            options,
            CommitSettings::default(),
        );
        committer.prewrite().await.unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].for_update_ts, 100);
        assert_eq!(
            requests[0].pessimistic_actions,
            [kvrpcpb::prewrite_request::PessimisticAction::SkipPessimisticCheck as i32]
        );
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPrewriteTxnSize() {
    source_2pc_run(|| async move {
    let split = b"~2pc/txn-size/m".to_vec();
    let (_cluster, pd) = source_2pc_store_with_splits(std::slice::from_ref(&split));
    let requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PrewriteRequest>::new()));
    let captured = Arc::clone(&requests);
    let interceptor = crate::new_rpc_interceptor("capture-2pc-txn-size", move |_, request, next| {
        if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::PrewriteRequest>() {
            captured.lock().unwrap().push(request.clone());
        }
        Box::pin(async move { next().await })
    });
    let mut transaction = source_integration_transaction(&pd, false).await;
    transaction.set_rpc_interceptor(interceptor);
    for index in 0..70 {
        let side = if index < 50 { 'a' } else { 'z' };
        transaction
            .put(
                format!("~2pc/txn-size/{side}{index:03}").into_bytes(),
                vec![b'v'; 1_024],
            )
            .await
            .unwrap();
    }
    Box::pin(transaction.commit()).await.unwrap();
    let requests = requests.lock().unwrap();
        let mut sizes = std::collections::BTreeMap::<u64, usize>::new();
        for request in requests.iter() {
            assert!(matches!(request.txn_size, 20 | 50));
            *sizes.entry(request.txn_size).or_default() += request.mutations.len();
        }
        assert_eq!(sizes, [(20, 20), (50, 50)].into_iter().collect());
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestDeleteYourWritesTTL() {
    source_2pc_run(|| async move {
        let restore = crate::config::update_global(|config| {
            config.tikv_client.ttl_refreshed_txn_size = 0;
        });
        let scenario = FailScenario::setup();
        crate::util::enable_failpoints();
        fail::cfg("after-prewrite", "sleep(50)").unwrap();
        let heartbeats = Arc::new(AtomicUsize::new(0));
        let captured_heartbeats = Arc::clone(&heartbeats);
        let prewrites = Arc::new(Mutex::new(Vec::new()));
        let captured_prewrites = Arc::clone(&prewrites);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured_prewrites.lock().unwrap().push(request.clone());
                    return Ok(Box::<kvrpcpb::PrewriteResponse>::default() as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::TxnHeartBeatRequest>() {
                    captured_heartbeats.fetch_add(1, Ordering::SeqCst);
                    return Ok(Box::new(kvrpcpb::TxnHeartBeatResponse {
                        lock_ttl: 3_000,
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                if request.is::<kvrpcpb::CommitRequest>() {
                    return Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>);
                }
                panic!("unexpected delete-your-write TTL request");
            },
        )));
        rpc.set_timestamp(Timestamp::from_version(10));
        for suffix in ["bb", "dd"] {
            let mut transaction = Transaction::new(
                Timestamp::from_version(1),
                Arc::clone(&rpc),
                TransactionOptions::new_optimistic()
                    .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(5)))
                    .drop_check(CheckLevel::None),
                Keyspace::Disable,
            );
            let deleted = format!("~2pc/delete-your-write/{suffix}").into_bytes();
            transaction
                .insert(deleted.clone(), b"inserted".to_vec())
                .await
                .unwrap();
            transaction.delete(deleted).await.unwrap();
            transaction
                .put(
                    format!("~2pc/delete-your-write/{suffix}-live").into_bytes(),
                    b"live".to_vec(),
                )
                .await
                .unwrap();
            Box::pin(transaction.commit()).await.unwrap();
        }
        assert!(heartbeats.load(Ordering::SeqCst) > 0);
        let prewrites = prewrites.lock().unwrap();
        assert_eq!(prewrites.len(), 2);
        for request in prewrites.iter() {
            let mut operations = request
                .mutations
                .iter()
                .map(|mutation| kvrpcpb::Op::try_from(mutation.op).unwrap())
                .collect::<Vec<_>>();
            operations.sort_by_key(|operation| *operation as i32);
            assert_eq!(operations, [kvrpcpb::Op::Put, kvrpcpb::Op::CheckNotExists]);
        }
        fail::remove("after-prewrite");
        scenario.teardown();
        restore();
    });
}

#[test]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPrewriteRollback() {
    let engine = unistore::MockEngine::new();
    let keys = [b"~2pc/prewrite-rollback/a".to_vec(), b"~2pc/prewrite-rollback/b".to_vec()];
    let initial = unistore::PrewriteRequest {
        mutations: keys
            .iter()
            .map(|key| unistore::TxnMutation::put(key.clone(), b"old".to_vec()))
            .collect(),
        primary: keys[0].clone(),
        start_ts: 1,
        ttl: 3_000,
        min_commit_ts: 2,
        ..Default::default()
    };
    assert!(engine.prewrite(&initial).iter().all(Option::is_none));
    engine.commit(&keys, 1, 2).unwrap();

    let update = unistore::PrewriteRequest {
        mutations: keys
            .iter()
            .map(|key| unistore::TxnMutation::put(key.clone(), b"new".to_vec()))
            .collect(),
        primary: keys[0].clone(),
        start_ts: 3,
        ttl: 3_000,
        min_commit_ts: 4,
        ..Default::default()
    };
    assert!(engine.prewrite(&update).iter().all(Option::is_none));
    assert_eq!(
        engine
            .get(
                &keys[0],
                u64::MAX,
                unistore::IsolationLevel::ReadCommitted,
                &[],
            )
            .unwrap()
            .unwrap()
            .0,
        b"old"
    );
    assert!(engine.prewrite(&update).iter().all(Option::is_none));
    engine.commit(&keys, 3, 4).unwrap();
    assert_eq!(
        engine
            .get(
                &keys[1],
                u64::MAX,
                unistore::IsolationLevel::SnapshotIsolation,
                &[],
            )
            .unwrap()
            .unwrap()
            .0,
        b"new"
    );
}

#[test]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestContextCancel() {
    source_2pc_run(|| async move {
        #[derive(Clone)]
        struct PendingPrewrite;

        #[async_trait::async_trait]
        impl crate::request::Plan for PendingPrewrite {
            type Result = ();

            async fn execute(&self) -> crate::Result<Self::Result> {
                futures::future::pending().await
            }
        }

        let canceller = crate::RpcCanceller::new();
        canceller.cancel_all();
        let plan = crate::request::RpcCancellable {
            inner: PendingPrewrite,
            canceller,
        };
        let error = crate::request::Plan::execute(&plan).await.unwrap_err();
        assert_eq!(error.to_string(), "context canceled");
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestContextCancel2() {
    source_2pc_run(|| async move {
        let (cluster, pd) = source_2pc_store_with_splits(&[b"~2pc/context-cancel2/b".to_vec()]);
        let keys = [b"~2pc/context-cancel2/a".to_vec(), b"~2pc/context-cancel2/c".to_vec()];
        let secondary_done = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&secondary_done);
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction.set_background_task_lifecycle_hooks(super::LifecycleHooks {
            pre: None,
            post: Some(Arc::new(move || notify.notify_one())),
        });
        for key in &keys {
            transaction.put(key.clone(), key.clone()).await.unwrap();
        }
        Box::pin(transaction.commit()).await.unwrap();
        let cancelled = crate::async_util::Cancellation::default();
        cancelled.cancel();
        tokio::time::timeout(Duration::from_secs(2), secondary_done.notified())
            .await
            .unwrap();
        assert!(keys
            .iter()
            .all(|key| cluster.engine().mvcc_get_by_key(key).lock.is_none()));
        source_2pc_assert_values(&pd, &[(&keys[0], &keys[0]), (&keys[1], &keys[1])]).await;
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestContextCancelRetryable() {
    source_2pc_run(|| async move {
        let (cluster, pd) = source_2pc_store_with_splits(&[
            b"~2pc/context-cancel-retry/b".to_vec(),
            b"~2pc/context-cancel-retry/c".to_vec(),
        ]);
        let key_a = b"~2pc/context-cancel-retry/a".to_vec();
        let key_b = b"~2pc/context-cancel-retry/b1".to_vec();
        let key_c = b"~2pc/context-cancel-retry/c1".to_vec();
        let lock_start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
            .await
            .unwrap()
            .version();
        let mut transaction = source_integration_transaction(&pd, false).await;
        let mut winner = source_integration_transaction(&pd, false).await;
        assert!(cluster
            .engine()
            .prewrite(&unistore::PrewriteRequest {
                mutations: vec![unistore::TxnMutation::put(key_b.clone(), b"locked".to_vec())],
                primary: key_b.clone(),
                start_ts: lock_start_ts,
                ttl: 3_000,
                min_commit_ts: lock_start_ts + 1,
                ..Default::default()
            })
            .iter()
            .all(Option::is_none));
        winner.put(key_c.clone(), b"winner".to_vec()).await.unwrap();
        Box::pin(winner.commit()).await.unwrap();
        for key in [&key_a, &key_b, &key_c] {
            transaction.put(key.clone(), b"loser".to_vec()).await.unwrap();
        }
        let error = tokio::time::timeout(Duration::from_secs(2), Box::pin(transaction.commit()))
            .await
            .expect("first shard error must cancel a sibling waiting on a lock")
            .unwrap_err();
        assert!(crate::error::is_write_conflict(&error), "{error:?}");
        cluster
            .engine()
            .rollback(std::slice::from_ref(&key_b), lock_start_ts)
            .unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestContextCancelCausingUndetermined() {
    source_2pc_run(|| async move {
        let scenario = FailScenario::setup();
        crate::util::enable_failpoints();
        let (_cluster, pd) = source_integration_store();
        let key = b"~2pc/context-cancel-undetermined/a".to_vec();
        let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
            .await
            .unwrap()
            .version();
        let mut committer = source_2pc_committer(
            Arc::clone(&pd),
            start_ts,
            Some(Key::from(key.clone())),
            vec![source_test_mutation(key, kvrpcpb::Op::Put)],
            source_integration_options(false),
        );
        committer.prewrite().await.unwrap();
        fail::cfg("tikvclient/rpcContextCancelErr", "return(true)").unwrap();
        let error = committer.commit_primary_with_retry().await.unwrap_err();
        assert_eq!(error.to_string(), "context canceled");
        assert!(committer.undetermined);
        fail::remove("tikvclient/rpcContextCancelErr");
        scenario.teardown();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPrewriteCancel() {
    source_2pc_run(|| async move {
        let (cluster, pd) = source_2pc_store_with_splits(&[
            b"~2pc/prewrite-cancel/b".to_vec(),
            b"~2pc/prewrite-cancel/c".to_vec(),
        ]);
        let keys = [
            b"~2pc/prewrite-cancel/a".to_vec(),
            b"~2pc/prewrite-cancel/b1".to_vec(),
            b"~2pc/prewrite-cancel/c1".to_vec(),
        ];
        let mut loser = source_integration_transaction(&pd, false).await;
        let mut winner = source_integration_transaction(&pd, false).await;
        winner.put(keys[1].clone(), b"winner".to_vec()).await.unwrap();
        Box::pin(winner.commit()).await.unwrap();
        let cleanup_done = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&cleanup_done);
        loser.set_background_task_lifecycle_hooks(super::LifecycleHooks {
            pre: None,
            post: Some(Arc::new(move || notify.notify_one())),
        });
        for key in &keys {
            loser.put(key.clone(), b"loser".to_vec()).await.unwrap();
        }
        assert!(Box::pin(loser.commit()).await.is_err());
        tokio::time::timeout(Duration::from_secs(2), cleanup_done.notified())
            .await
            .unwrap();
        assert!(keys
            .iter()
            .all(|key| cluster.engine().mvcc_get_by_key(key).lock.is_none()));
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPrewritePrimaryKeyFailed() {
    source_2pc_run(|| async move {
        let (cluster, pd) = source_integration_store();
        let keys = [b"~2pc/primary-failed/a".to_vec(), b"~2pc/primary-failed/b".to_vec()];
        let mut stale = source_integration_transaction(&pd, false).await;
        let stale_start_ts = stale.start_timestamp().version();
        let mut winner = source_integration_transaction(&pd, false).await;
        winner.put(keys[0].clone(), b"a1".to_vec()).await.unwrap();
        Box::pin(winner.commit()).await.unwrap();
        let cleanup_done = Arc::new(tokio::sync::Notify::new());
        let notify = Arc::clone(&cleanup_done);
        stale.set_background_task_lifecycle_hooks(super::LifecycleHooks {
            pre: None,
            post: Some(Arc::new(move || notify.notify_one())),
        });
        stale.put(keys[0].clone(), b"a2".to_vec()).await.unwrap();
        stale.put(keys[1].clone(), b"b2".to_vec()).await.unwrap();
        assert!(Box::pin(stale.commit()).await.is_err());
        tokio::time::timeout(Duration::from_secs(2), cleanup_done.notified())
            .await
            .unwrap();
        cluster.engine().rollback(&keys, stale_start_ts).unwrap();
        assert_eq!(source_engine_value(&cluster, &keys[0], u64::MAX).unwrap().0, b"a1");
        assert_eq!(source_engine_value(&cluster, &keys[1], u64::MAX), None);
        let mut update = source_integration_transaction(&pd, false).await;
        update.put(keys[0].clone(), b"a3".to_vec()).await.unwrap();
        Box::pin(update.commit()).await.unwrap();
        assert_eq!(source_engine_value(&cluster, &keys[0], u64::MAX).unwrap().0, b"a3");
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestWrittenKeysOnConflict() {
    source_2pc_run(|| async move {
        let split = b"~2pc/written-conflict/y".to_vec();
        let (cluster, pd) = source_2pc_store_with_splits(std::slice::from_ref(&split));
        let key_x = b"~2pc/written-conflict/x1".to_vec();
        let key_y = b"~2pc/written-conflict/y1".to_vec();
        for round in 0..10_u8 {
            let mut loser = source_integration_transaction(&pd, false).await;
            let mut winner = source_integration_transaction(&pd, false).await;
            winner.put(key_x.clone(), vec![round]).await.unwrap();
            Box::pin(winner.commit()).await.unwrap();
            let cleanup_done = Arc::new(tokio::sync::Notify::new());
            let notify = Arc::clone(&cleanup_done);
            loser.set_background_task_lifecycle_hooks(super::LifecycleHooks {
                pre: None,
                post: Some(Arc::new(move || notify.notify_one())),
            });
            loser.put(key_x.clone(), b"loser".to_vec()).await.unwrap();
            loser.put(key_y.clone(), b"written".to_vec()).await.unwrap();
            assert!(Box::pin(loser.commit()).await.is_err());
            tokio::time::timeout(Duration::from_secs(2), cleanup_done.notified())
                .await
                .unwrap();
            assert!(cluster.engine().mvcc_get_by_key(&key_y).lock.is_none());
        }
    });
}

#[test]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestRejectCommitTS() {
    source_2pc_run(|| async move {
        let versions = Arc::new(Mutex::new(Vec::new()));
        let captured_versions = Arc::clone(&versions);
        let attempts = Arc::new(AtomicUsize::new(0));
        let captured_attempts = Arc::clone(&attempts);
        let rpc = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request
                    .downcast_ref::<kvrpcpb::CommitRequest>()
                    .expect("reject-commit-ts sends Commit");
                captured_versions.lock().unwrap().push(request.commit_version);
                if captured_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Ok(Box::new(kvrpcpb::CommitResponse {
                        error: Some(kvrpcpb::KeyError {
                            commit_ts_expired: Some(kvrpcpb::CommitTsExpired {
                                start_ts: request.start_version,
                                attempted_commit_ts: request.commit_version,
                                key: request.keys[0].clone(),
                                min_commit_ts: request.commit_version + 1,
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }) as Box<dyn Any>);
                }
                Ok(Box::<kvrpcpb::CommitResponse>::default() as Box<dyn Any>)
            },
        )));
        rpc.set_timestamp_sequence([
            Timestamp::from_version(9),
            Timestamp::from_version(10),
        ]);
        let key = b"~2pc/reject-commit-ts/x".to_vec();
        let mut committer = source_test_committer(
            rpc,
            Some(Key::from(key.clone())),
            vec![source_test_mutation(key, kvrpcpb::Op::Put)],
            source_integration_options(false),
            CommitSettings::default(),
        );
        let commit_ts = committer.commit_primary_with_retry().await.unwrap().version();
        let versions = versions.lock().unwrap();
        assert_eq!(versions.len(), 2);
        assert!(versions[1] > versions[0]);
        assert_eq!(commit_ts, versions[1]);
    });
}

#[test]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPessimisticLockedKeysDedup() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let keys = [b"~2pc/dedup/abc".to_vec(), b"~2pc/dedup/def".to_vec()];
        let mut transaction = source_integration_transaction_with_options(
            &pd,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
        )
        .await;
        transaction.lock_keys(keys.clone()).await.unwrap();
        transaction.lock_keys(keys.clone()).await.unwrap();
        assert_eq!(transaction.buffer.pessimistic_lock_keys(), keys.into_iter().collect());
        transaction.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestUnsetPrimaryKey() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let key = b"~2pc/unset-primary/key".to_vec();
        let key2 = b"~2pc/unset-primary/key2".to_vec();
        let mut initial = source_integration_transaction(&pd, false).await;
        initial.put(key.clone(), key.clone()).await.unwrap();
        Box::pin(initial.commit()).await.unwrap();

        let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
        assert_eq!(
            transaction.get(key.clone()).await.unwrap(),
            Some(key.clone())
        );
        transaction
            .get_mem_buffer()
            .set_with_flags(
                &key,
                b"duplicate",
                &[crate::FlagsOp::SetPresumeKeyNotExists],
            )
            .unwrap();
        let error = transaction.lock_keys([key.clone()]).await.unwrap_err();
        assert!(
            matches!(error, Error::KeyExists(_) | Error::AssertionFailed(_)),
            "unexpected pessimistic duplicate error: {error:?}"
        );
        assert!(transaction.buffer.get_primary_key().is_none());
        transaction.delete(key).await.unwrap();
        transaction.put(key2.clone(), key2.clone()).await.unwrap();
        Box::pin(transaction.commit()).await.unwrap();
        source_2pc_assert_values(&pd, &[(&key2, &key2)]).await;
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPessimisticTTL() {
    source_2pc_run(|| async move {
        let (cluster, pd) = source_integration_store();
        let key = b"~2pc/pessimistic-ttl/key".to_vec();
        let key2 = b"~2pc/pessimistic-ttl/key2".to_vec();
        let mut transaction = source_integration_transaction_with_options(
            &pd,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(20)))
                .drop_check(CheckLevel::None),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        transaction.lock_keys([key.clone()]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        transaction.lock_keys([key2.clone()]).await.unwrap();
        let lock_ttl = cluster.engine().mvcc_get_by_key(&key).lock.unwrap().ttl;
        let start_ts = transaction.start_timestamp().version();
        let now = crate::pd::PdClient::get_timestamp(pd.clone())
            .await
            .unwrap()
            .version();
        let remaining = crate::oracle::extract_physical(start_ts)
            .saturating_add(lock_ttl as i64)
            .saturating_sub(crate::oracle::extract_physical(now));
        assert!(remaining >= 100, "primary lock expires in only {remaining}ms");
        let status_ttl = cluster
            .engine()
            .check_txn_status(&key, start_ts, start_ts, 0, true, true)
            .unwrap()
            .0;
        assert!(status_ttl >= lock_ttl);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let updated_ttl = cluster.engine().mvcc_get_by_key(&key).lock.unwrap().ttl;
                if updated_ttl > lock_ttl {
                    let now = crate::pd::PdClient::get_timestamp(pd.clone())
                        .await
                        .unwrap()
                        .version();
                    let expiry = crate::oracle::extract_physical(start_ts)
                        .saturating_add(updated_ttl as i64);
                    let now = crate::oracle::extract_physical(now);
                    assert!(expiry > now);
                    assert!((expiry - now) as u64 <= super::managed_lock_ttl());
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("managed heartbeat did not extend the primary lock TTL");
        transaction.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPessimisticLockReturnValues() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let keys = [b"~2pc/lock-values/key".to_vec(), b"~2pc/lock-values/key2".to_vec()];
        let mut initial = source_integration_transaction(&pd, false).await;
        for key in &keys {
            initial.put(key.clone(), key.clone()).await.unwrap();
        }
        Box::pin(initial.commit()).await.unwrap();
        let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
        let mut context = LockContext::new(
            transaction.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        context.init_return_values(2);
        transaction
            .lock_keys_with_context(&mut context, keys.clone())
            .await
            .unwrap();
        assert_eq!(context.returned_values_len(), 2);
        for key in &keys {
            assert_eq!(context.returned_value(key).unwrap().value, *key);
        }
        transaction.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPessimisticLockIfExists() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let key0 = b"~2pc/lock-if-exists/jkey".to_vec();
        let key = b"~2pc/lock-if-exists/key".to_vec();
        let key2 = b"~2pc/lock-if-exists/key2".to_vec();
        let key3 = b"~2pc/lock-if-exists/key3".to_vec();
        let mut initial = source_integration_transaction(&pd, false).await;
        initial.put(key.clone(), key.clone()).await.unwrap();
        initial.put(key3.clone(), key3.clone()).await.unwrap();
        Box::pin(initial.commit()).await.unwrap();

        for (requested, expected_locked) in [
            (vec![key.clone()], vec![key0.clone(), key.clone()]),
            (vec![key2.clone()], vec![key0.clone()]),
            (
                vec![key.clone(), key2.clone(), key3.clone()],
                vec![key0.clone(), key.clone(), key3.clone()],
            ),
            (
                vec![key2.clone(), key.clone(), key3.clone()],
                vec![key0.clone(), key.clone(), key3.clone()],
            ),
        ] {
            let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
            transaction.lock_keys([key0.clone()]).await.unwrap();
            let mut context = LockContext::new(
                transaction.start_timestamp().version(),
                0,
                SystemTime::now(),
            );
            context.init_return_values(requested.len());
            context.lock_only_if_exists = true;
            transaction
                .lock_keys_with_context(&mut context, requested.clone())
                .await
                .unwrap();
            assert_eq!(transaction.buffer.get_primary_key().unwrap().as_ref(), key0);
            assert_eq!(
                transaction.buffer.pessimistic_lock_keys(),
                expected_locked.into_iter().collect()
            );
            for requested_key in requested {
                let returned = context.returned_value(&requested_key).unwrap();
                if requested_key == key2 {
                    assert!(!returned.exists);
                    assert!(returned.value.is_empty());
                } else {
                    assert!(returned.exists);
                    assert_eq!(returned.value, requested_key);
                }
            }
            transaction.rollback().await.unwrap();
        }

        let mut already_primary = source_2pc_pessimistic_transaction(&pd).await;
        already_primary.lock_keys([key0.clone()]).await.unwrap();
        let mut context = LockContext::new(
            already_primary.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        context.init_return_values(3);
        context.lock_only_if_exists = true;
        already_primary
            .lock_keys_with_context(&mut context, [key0.clone(), key.clone(), key3.clone()])
            .await
            .unwrap();
        assert!(context.returned_value(&key0).unwrap().already_locked);
        assert_eq!(already_primary.buffer.pessimistic_lock_keys().len(), 3);
        already_primary.rollback().await.unwrap();

        let mut one_existing = source_2pc_pessimistic_transaction(&pd).await;
        let mut context = LockContext::new(
            one_existing.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        context.init_return_values(1);
        context.lock_only_if_exists = true;
        one_existing
            .lock_keys_with_context(&mut context, [key.clone()])
            .await
            .unwrap();
        assert_eq!(one_existing.buffer.get_primary_key().unwrap().as_ref(), key);
        Box::pin(one_existing.commit()).await.unwrap();

        let mut one_missing = source_2pc_pessimistic_transaction(&pd).await;
        let mut context = LockContext::new(
            one_missing.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        context.init_return_values(1);
        context.lock_only_if_exists = true;
        one_missing
            .lock_keys_with_context(&mut context, [key2.clone()])
            .await
            .unwrap();
        assert!(!context.returned_value(&key2).unwrap().exists);
        assert!(one_missing.buffer.get_primary_key().is_none());
        assert!(one_missing.buffer.pessimistic_lock_keys().is_empty());
        Box::pin(one_missing.commit()).await.unwrap();

        let mut no_primary = source_2pc_pessimistic_transaction(&pd).await;
        let mut context = LockContext::new(
            no_primary.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        context.init_return_values(2);
        context.lock_only_if_exists = true;
        assert!(matches!(
            no_primary
                .lock_keys_with_context(&mut context, [key.clone(), key2.clone()])
                .await
                .unwrap_err(),
            Error::LockOnlyIfExistsNoPrimaryKey(_)
        ));
        no_primary.rollback().await.unwrap();

        let mut no_values = source_2pc_pessimistic_transaction(&pd).await;
        no_values.lock_keys([key.clone()]).await.unwrap();
        let mut context = LockContext::new(
            no_values.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        context.lock_only_if_exists = true;
        assert!(matches!(
            no_values
                .lock_keys_with_context(&mut context, [key2.clone()])
                .await
                .unwrap_err(),
            Error::LockOnlyIfExistsNoReturnValue(_)
        ));
        no_values
            .lock_keys_with_context(&mut context, Vec::<Vec<u8>>::new())
            .await
            .unwrap();
        no_values.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPessimisticLockCheckExistence() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let key = b"~2pc/check-existence/key".to_vec();
        let missing = b"~2pc/check-existence/missing".to_vec();
        let mut initial = source_integration_transaction(&pd, false).await;
        initial.put(key.clone(), key.clone()).await.unwrap();
        Box::pin(initial.commit()).await.unwrap();
        for return_values in [false, true] {
            let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
            let mut context = LockContext::new(
                transaction.start_timestamp().version(),
                0,
                SystemTime::now(),
            );
            context.init_check_existence(2);
            if return_values {
                context.init_return_values(2);
            }
            transaction
                .lock_keys_with_context(&mut context, [key.clone(), missing.clone()])
                .await
                .unwrap();
            let existing = context.returned_value(&key).unwrap();
            let absent = context.returned_value(&missing).unwrap();
            assert!(existing.exists);
            assert!(!absent.exists);
            assert_eq!(existing.value, if return_values { key.clone() } else { Vec::new() });
            assert!(absent.value.is_empty());
            transaction.rollback().await.unwrap();
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPessimisticLockAllowLockWithConflict() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let key = b"~2pc/allow-conflict/key".to_vec();
        let mut seed = source_integration_transaction(&pd, false).await;
        seed.put(key.clone(), key.clone()).await.unwrap();
        Box::pin(seed.commit()).await.unwrap();

        for return_values in [false, true] {
            for check_existence in [false, true] {
                let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
                transaction.start_aggressive_locking();
                let mut context = LockContext::new(
                    transaction.start_timestamp().version(),
                    0,
                    SystemTime::now(),
                );
                if check_existence {
                    context.init_check_existence(1);
                }
                if return_values {
                    context.init_return_values(1);
                }
                transaction
                    .lock_keys_with_context(&mut context, [key.clone()])
                    .await
                    .unwrap();
                if return_values || check_existence {
                    let returned = context.returned_value(&key).unwrap();
                    assert!(returned.exists);
                    assert_eq!(
                        returned.value,
                        if return_values { key.clone() } else { Vec::new() }
                    );
                } else {
                    assert_eq!(context.returned_values_len(), 0);
                }
                assert_eq!(context.max_locked_with_conflict_ts, 0);
                transaction.done_aggressive_locking().await.unwrap();
                transaction.rollback().await.unwrap();
            }
        }

        for return_values in [false, true] {
            for check_existence in [false, true] {
                let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
                let value = format!("value-{return_values}-{check_existence}").into_bytes();
                let mut writer = source_integration_transaction(&pd, false).await;
                writer.put(key.clone(), value.clone()).await.unwrap();
                let writer_commit_ts = Box::pin(writer.commit())
                    .await
                    .unwrap()
                    .unwrap()
                    .version();
                assert!(writer_commit_ts > transaction.start_timestamp().version());

                transaction.start_aggressive_locking();
                let mut context = LockContext::new(
                    transaction.start_timestamp().version(),
                    0,
                    SystemTime::now(),
                );
                if check_existence {
                    context.init_check_existence(1);
                }
                if return_values {
                    context.init_return_values(1);
                }
                transaction
                    .lock_keys_with_context(&mut context, [key.clone()])
                    .await
                    .unwrap();
                assert_eq!(context.max_locked_with_conflict_ts, writer_commit_ts);
                let returned = context.returned_value(&key).unwrap();
                assert_eq!(returned.locked_with_conflict_ts, writer_commit_ts);
                assert!(returned.exists);
                assert_eq!(returned.value, value);
                transaction.cancel_aggressive_locking().await.unwrap();
                transaction.rollback().await.unwrap();
            }
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPessimisticLockAllowLockWithConflictError() {
    source_2pc_run(|| async move {
        crate::util::enable_failpoints();
        let (_cluster, pd) = source_integration_store();
        for return_values in [false, true] {
            for check_existence in [false, true] {
                let key = format!(
                    "~2pc/allow-conflict-error/{return_values}/{check_existence}"
                )
                .into_bytes();
                let mut blocker = source_2pc_pessimistic_transaction(&pd).await;
                blocker.lock_keys([key.clone()]).await.unwrap();

                let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
                transaction.start_aggressive_locking();
                let mut context = LockContext::new(
                    transaction.start_timestamp().version(),
                    10,
                    SystemTime::now(),
                );
                if check_existence {
                    context.init_check_existence(1);
                }
                if return_values {
                    context.init_return_values(1);
                }
                let error = transaction
                    .lock_keys_with_context(&mut context, [key.clone()])
                    .await
                    .unwrap_err();
                assert!(crate::error::is_lock_wait_timeout(&error), "{error:?}");
                assert!(!transaction.is_in_aggressive_locking_stage(key.clone()));
                blocker.rollback().await.unwrap();

                fail::cfg("rpcPessimisticLockResult", "1*return(\"notLeader\")").unwrap();
                transaction
                    .lock_keys_with_context(&mut context, [key.clone()])
                    .await
                    .unwrap();
                fail::remove("rpcPessimisticLockResult");
                assert!(transaction.is_in_aggressive_locking_stage(key));
                transaction.cancel_aggressive_locking().await.unwrap();
                transaction.rollback().await.unwrap();
            }
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAggressiveLocking() {
    source_2pc_run(|| async move {
        for final_is_done in [false, true] {
            let (cluster, pd) = source_integration_store();
            let prefix = format!("aggressive-basic/{final_is_done}");
            let keys = (1..=6)
                .map(|index| source_2pc_key(&prefix, &format!("k{index}")))
                .collect::<Vec<_>>();
            let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
            assert!(!transaction.is_in_aggressive_locking_mode());

            let mut context = LockContext::new(
                transaction.start_timestamp().version(),
                0,
                SystemTime::now(),
            );
            transaction
                .lock_keys_with_context(&mut context, [keys[0].clone(), keys[1].clone()])
                .await
                .unwrap();
            assert!(source_2pc_is_locked(&cluster, &keys[0]));
            assert!(source_2pc_is_locked(&cluster, &keys[1]));

            transaction.start_aggressive_locking();
            for key in [&keys[1], &keys[2], &keys[3]] {
                transaction
                    .lock_keys_with_context(&mut context, [key.clone()])
                    .await
                    .unwrap();
                assert!(source_2pc_is_locked(&cluster, key));
            }
            assert!(!transaction.is_in_aggressive_locking_stage(keys[1].clone()));
            assert!(transaction.is_in_aggressive_locking_stage(keys[2].clone()));
            assert!(transaction.is_in_aggressive_locking_stage(keys[3].clone()));

            transaction.retry_aggressive_locking().await.unwrap();
            for key in &keys[..4] {
                assert!(source_2pc_is_locked(&cluster, key));
            }
            transaction
                .lock_keys_with_context(&mut context, [keys[3].clone()])
                .await
                .unwrap();
            transaction
                .lock_keys_with_context(&mut context, [keys[4].clone()])
                .await
                .unwrap();
            assert!(source_2pc_is_locked(&cluster, &keys[3]));
            assert!(source_2pc_is_locked(&cluster, &keys[4]));

            transaction.retry_aggressive_locking().await.unwrap();
            assert!(source_2pc_is_locked(&cluster, &keys[0]));
            assert!(source_2pc_is_locked(&cluster, &keys[1]));
            assert!(!source_2pc_is_locked(&cluster, &keys[2]));
            assert!(source_2pc_is_locked(&cluster, &keys[3]));
            assert!(source_2pc_is_locked(&cluster, &keys[4]));

            for key in [&keys[1], &keys[4], &keys[5]] {
                transaction
                    .lock_keys_with_context(&mut context, [key.clone()])
                    .await
                    .unwrap();
            }
            if final_is_done {
                transaction.done_aggressive_locking().await.unwrap();
                for (index, expected) in [true, true, false, false, true, true]
                    .into_iter()
                    .enumerate()
                {
                    assert_eq!(source_2pc_is_locked(&cluster, &keys[index]), expected);
                }
            } else {
                transaction.cancel_aggressive_locking().await.unwrap();
                for (index, expected) in [true, true, false, false, false, false]
                    .into_iter()
                    .enumerate()
                {
                    assert_eq!(source_2pc_is_locked(&cluster, &keys[index]), expected);
                }
            }
            transaction.rollback().await.unwrap();
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAggressiveLockingInsert() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let keys = (1..=8)
            .map(|index| source_2pc_key("aggressive-insert", &format!("k{index}")))
            .collect::<Vec<_>>();
        let mut seed = source_integration_transaction(&pd, false).await;
        for index in [0_usize, 2, 5, 7] {
            seed.put(keys[index].clone(), format!("v{}", index + 1).into_bytes())
                .await
                .unwrap();
        }
        Box::pin(seed.commit()).await.unwrap();

        let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
        let mut context = LockContext::new(
            transaction.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        context.init_return_values(2);
        transaction
            .lock_keys_with_context(&mut context, [keys[0].clone(), keys[1].clone()])
            .await
            .unwrap();
        transaction.put(keys[4].clone(), b"v5".to_vec()).await.unwrap();
        transaction.delete(keys[5].clone()).await.unwrap();

        transaction.start_aggressive_locking();
        transaction
            .get_mem_buffer()
            .update_flags(&keys[0], &[crate::FlagsOp::SetPresumeKeyNotExists]);
        let error = transaction
            .lock_keys([keys[0].clone()])
            .await
            .unwrap_err();
        assert!(crate::error::is_key_exists(&error), "{error:?}");

        transaction
            .get_mem_buffer()
            .update_flags(&keys[1], &[crate::FlagsOp::SetPresumeKeyNotExists]);
        transaction.lock_keys([keys[1].clone()]).await.unwrap();

        transaction
            .get_mem_buffer()
            .update_flags(&keys[2], &[crate::FlagsOp::SetPresumeKeyNotExists]);
        let error = transaction
            .lock_keys([keys[2].clone()])
            .await
            .unwrap_err();
        assert!(crate::error::is_key_exists(&error), "{error:?}");

        transaction
            .get_mem_buffer()
            .update_flags(&keys[3], &[crate::FlagsOp::SetPresumeKeyNotExists]);
        transaction.lock_keys([keys[3].clone()]).await.unwrap();

        let mut writer = source_integration_transaction(&pd, false).await;
        writer.put(keys[6].clone(), b"v7".to_vec()).await.unwrap();
        writer.delete(keys[7].clone()).await.unwrap();
        let writer_commit_ts = Box::pin(writer.commit())
            .await
            .unwrap()
            .unwrap()
            .version();

        let mut conflict_context = LockContext::new(
            transaction.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        transaction
            .get_mem_buffer()
            .update_flags(&keys[6], &[crate::FlagsOp::SetPresumeKeyNotExists]);
        let error = transaction
            .lock_keys_with_context(&mut conflict_context, [keys[6].clone()])
            .await
            .unwrap_err();
        assert!(crate::error::is_write_conflict(&error), "{error:?}");

        let mut deleted_context = LockContext::new(
            transaction.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        transaction
            .get_mem_buffer()
            .update_flags(&keys[7], &[crate::FlagsOp::SetPresumeKeyNotExists]);
        transaction
            .lock_keys_with_context(&mut deleted_context, [keys[7].clone()])
            .await
            .unwrap();
        assert_eq!(deleted_context.max_locked_with_conflict_ts, writer_commit_ts);
        assert_eq!(
            deleted_context
                .returned_value(&keys[7])
                .unwrap()
                .locked_with_conflict_ts,
            writer_commit_ts
        );

        let for_update_ts = crate::pd::PdClient::get_timestamp(pd.clone())
            .await
            .unwrap()
            .version();
        assert!(for_update_ts >= writer_commit_ts);
        let mut retry_context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .get_mem_buffer()
            .update_flags(&keys[6], &[crate::FlagsOp::SetPresumeKeyNotExists]);
        let error = transaction
            .lock_keys_with_context(&mut retry_context, [keys[6].clone()])
            .await
            .unwrap_err();
        assert!(crate::error::is_key_exists(&error), "{error:?}");

        transaction
            .get_mem_buffer()
            .update_flags(&keys[7], &[crate::FlagsOp::SetPresumeKeyNotExists]);
        transaction
            .lock_keys_with_context(&mut retry_context, [keys[7].clone()])
            .await
            .unwrap();
        transaction.cancel_aggressive_locking().await.unwrap();
        transaction.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAggressiveLockingLockOnlyIfExists() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let key = source_2pc_key("aggressive-lock-if-exists", "k1");

        let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
        let mut writer = source_integration_transaction(&pd, false).await;
        writer.put(key.clone(), b"v1".to_vec()).await.unwrap();
        let writer_commit_ts = Box::pin(writer.commit())
            .await
            .unwrap()
            .unwrap()
            .version();
        transaction.start_aggressive_locking();
        let mut context = LockContext::new(
            transaction.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        context.init_return_values(1);
        context.lock_only_if_exists = true;
        transaction
            .lock_keys_with_context(&mut context, [key.clone()])
            .await
            .unwrap();
        let returned = context.returned_value(&key).unwrap();
        assert!(returned.exists);
        assert!(!returned.already_locked);
        assert_eq!(returned.value, b"v1");
        assert_eq!(returned.locked_with_conflict_ts, writer_commit_ts);
        assert!(transaction.is_in_aggressive_locking_stage(key.clone()));
        transaction.cancel_aggressive_locking().await.unwrap();
        transaction.rollback().await.unwrap();

        let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
        let mut writer = source_integration_transaction(&pd, false).await;
        writer.delete(key.clone()).await.unwrap();
        Box::pin(writer.commit()).await.unwrap();
        transaction.start_aggressive_locking();
        let mut context = LockContext::new(
            transaction.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        context.init_return_values(1);
        context.lock_only_if_exists = true;
        let error = transaction
            .lock_keys_with_context(&mut context, [key.clone()])
            .await
            .unwrap_err();
        assert!(crate::error::is_write_conflict(&error), "{error:?}");
        assert!(context.returned_value(&key).is_none());
        assert!(!transaction.is_in_aggressive_locking_stage(key));
        transaction.cancel_aggressive_locking().await.unwrap();
        transaction.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAggressiveLockingSwitchPrimary() {
    source_2pc_run(|| async move {
        let (cluster, pd) = source_integration_store();
        let keys = (1..=7)
            .map(|index| source_2pc_key("aggressive-primary", &format!("k{index}")))
            .collect::<Vec<_>>();
        let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
        let mut for_update_ts = transaction.start_timestamp().version();
        transaction.start_aggressive_locking();
        let mut context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut context, [keys[0].clone()])
            .await
            .unwrap();
        transaction
            .lock_keys_with_context(&mut context, [keys[1].clone()])
            .await
            .unwrap();
        source_2pc_assert_primary(&cluster, &keys[0], &keys[0]);
        source_2pc_assert_primary(&cluster, &keys[1], &keys[0]);

        for_update_ts += 1;
        transaction.retry_aggressive_locking().await.unwrap();
        context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut context, [keys[0].clone()])
            .await
            .unwrap();
        transaction
            .lock_keys_with_context(&mut context, [keys[2].clone()])
            .await
            .unwrap();
        source_2pc_assert_primary(&cluster, &keys[0], &keys[0]);
        source_2pc_assert_primary(&cluster, &keys[2], &keys[0]);

        for_update_ts += 1;
        transaction.retry_aggressive_locking().await.unwrap();
        context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut context, [keys[3].clone()])
            .await
            .unwrap();
        transaction
            .lock_keys_with_context(&mut context, [keys[4].clone()])
            .await
            .unwrap();
        source_2pc_assert_primary(&cluster, &keys[3], &keys[3]);
        source_2pc_assert_primary(&cluster, &keys[4], &keys[3]);
        assert!(!source_2pc_is_locked(&cluster, &keys[1]));

        for_update_ts += 1;
        transaction.retry_aggressive_locking().await.unwrap();
        context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut context, [keys[4].clone()])
            .await
            .unwrap();
        transaction
            .lock_keys_with_context(&mut context, [keys[5].clone()])
            .await
            .unwrap();
        source_2pc_assert_primary(&cluster, &keys[4], &keys[4]);
        source_2pc_assert_primary(&cluster, &keys[5], &keys[4]);
        assert!(!source_2pc_is_locked(&cluster, &keys[0]));
        assert!(!source_2pc_is_locked(&cluster, &keys[2]));

        for_update_ts += 1;
        transaction.retry_aggressive_locking().await.unwrap();
        context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut context, [keys[6].clone()])
            .await
            .unwrap();
        for_update_ts += 1;
        transaction.retry_aggressive_locking().await.unwrap();
        context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut context, [keys[5].clone()])
            .await
            .unwrap();
        transaction
            .lock_keys_with_context(&mut context, [keys[4].clone()])
            .await
            .unwrap();
        source_2pc_assert_primary(&cluster, &keys[4], &keys[5]);
        source_2pc_assert_primary(&cluster, &keys[5], &keys[5]);
        transaction.cancel_aggressive_locking().await.unwrap();
        for key in &keys {
            assert!(!source_2pc_is_locked(&cluster, key));
        }
        transaction.rollback().await.unwrap();

        let prefix = "aggressive-primary-preselected";
        let keys = (1..=4)
            .map(|index| source_2pc_key(prefix, &format!("k{index}")))
            .collect::<Vec<_>>();
        let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
        for_update_ts = transaction.start_timestamp().version();
        context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut context, [keys[0].clone(), keys[1].clone()])
            .await
            .unwrap();
        source_2pc_assert_primary(&cluster, &keys[0], &keys[0]);
        source_2pc_assert_primary(&cluster, &keys[1], &keys[0]);
        transaction.start_aggressive_locking();
        transaction
            .lock_keys_with_context(&mut context, [keys[1].clone()])
            .await
            .unwrap();
        transaction
            .lock_keys_with_context(&mut context, [keys[2].clone()])
            .await
            .unwrap();
        source_2pc_assert_primary(&cluster, &keys[1], &keys[0]);
        source_2pc_assert_primary(&cluster, &keys[2], &keys[0]);
        for_update_ts += 1;
        transaction.retry_aggressive_locking().await.unwrap();
        context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut context, [keys[2].clone()])
            .await
            .unwrap();
        transaction
            .lock_keys_with_context(&mut context, [keys[3].clone()])
            .await
            .unwrap();
        source_2pc_assert_primary(&cluster, &keys[2], &keys[0]);
        source_2pc_assert_primary(&cluster, &keys[3], &keys[0]);
        transaction.cancel_aggressive_locking().await.unwrap();
        assert!(source_2pc_is_locked(&cluster, &keys[0]));
        assert!(source_2pc_is_locked(&cluster, &keys[1]));
        assert!(!source_2pc_is_locked(&cluster, &keys[2]));
        assert!(!source_2pc_is_locked(&cluster, &keys[3]));
        transaction.rollback().await.unwrap();
        assert!(!source_2pc_is_locked(&cluster, &keys[0]));
        assert!(!source_2pc_is_locked(&cluster, &keys[1]));
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAggressiveLockingLoadValueOptionChanges() {
    source_2pc_run(|| async move {
        for first_attempt_locked_with_conflict in [false, true] {
            let (_cluster, pd) = source_integration_store();
            let prefix = format!("aggressive-load/{first_attempt_locked_with_conflict}");
            let key0 = source_2pc_key(&prefix, "k0");
            let key1 = source_2pc_key(&prefix, "k1");
            let key2 = source_2pc_key(&prefix, "k2");
            let mut seed = source_integration_transaction(&pd, false).await;
            seed.put(key2.clone(), b"v2".to_vec()).await.unwrap();
            Box::pin(seed.commit()).await.unwrap();

            let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
            let mut context = LockContext::new(
                transaction.start_timestamp().version(),
                0,
                SystemTime::now(),
            );
            transaction
                .lock_keys_with_context(&mut context, [key0.clone()])
                .await
                .unwrap();
            transaction.start_aggressive_locking();
            let mut for_update_ts = transaction.start_timestamp().version();
            context = LockContext::new(for_update_ts, 0, SystemTime::now());

            let writer_commit_ts = if first_attempt_locked_with_conflict {
                let mut writer = source_integration_transaction(&pd, false).await;
                writer.delete(key1.clone()).await.unwrap();
                writer.put(key2.clone(), b"v2".to_vec()).await.unwrap();
                Some(
                    Box::pin(writer.commit())
                        .await
                        .unwrap()
                        .unwrap()
                        .version(),
                )
            } else {
                None
            };
            transaction
                .lock_keys_with_context(&mut context, [key1.clone()])
                .await
                .unwrap();
            transaction
                .lock_keys_with_context(&mut context, [key2.clone()])
                .await
                .unwrap();
            if let Some(writer_commit_ts) = writer_commit_ts {
                assert_eq!(context.max_locked_with_conflict_ts, writer_commit_ts);
                assert_eq!(
                    context
                        .returned_value(&key1)
                        .unwrap()
                        .locked_with_conflict_ts,
                    writer_commit_ts
                );
                assert_eq!(
                    context
                        .returned_value(&key2)
                        .unwrap()
                        .locked_with_conflict_ts,
                    writer_commit_ts
                );
                for_update_ts = writer_commit_ts + 1;
            } else {
                assert_eq!(context.returned_values_len(), 0);
                for_update_ts += 1;
            }

            transaction.retry_aggressive_locking().await.unwrap();
            context = LockContext::new(for_update_ts, 0, SystemTime::now());
            context.init_check_existence(2);
            transaction
                .lock_keys_with_context(&mut context, [key1.clone()])
                .await
                .unwrap();
            transaction
                .lock_keys_with_context(&mut context, [key2.clone()])
                .await
                .unwrap();
            assert_eq!(context.max_locked_with_conflict_ts, 0);
            assert!(!context.returned_value(&key1).unwrap().exists);
            assert!(context.returned_value(&key2).unwrap().exists);
            assert!(context.returned_value(&key1).unwrap().value.is_empty());
            assert!(context.returned_value(&key2).unwrap().value.is_empty());

            for_update_ts += 1;
            transaction.retry_aggressive_locking().await.unwrap();
            context = LockContext::new(for_update_ts, 0, SystemTime::now());
            context.init_return_values(2);
            transaction
                .lock_keys_with_context(&mut context, [key1.clone()])
                .await
                .unwrap();
            transaction
                .lock_keys_with_context(&mut context, [key2.clone()])
                .await
                .unwrap();
            assert_eq!(context.max_locked_with_conflict_ts, 0);
            assert!(!context.returned_value(&key1).unwrap().exists);
            assert!(context.returned_value(&key2).unwrap().exists);
            assert_eq!(context.returned_value(&key2).unwrap().value, b"v2");
            transaction.cancel_aggressive_locking().await.unwrap();
            transaction.rollback().await.unwrap();
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAggressiveLockingExitIfInapplicable() {
    source_2pc_run(|| async move {
        let (cluster, pd) = source_integration_store();
        let keys = (1..=4)
            .map(|index| source_2pc_key("aggressive-exit", &format!("k{index}")))
            .collect::<Vec<_>>();
        let mut transaction = source_2pc_pessimistic_transaction(&pd).await;
        transaction.start_aggressive_locking();
        let mut context = LockContext::new(
            transaction.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        transaction
            .lock_keys_with_context(&mut context, [keys[0].clone()])
            .await
            .unwrap();
        transaction.retry_aggressive_locking().await.unwrap();
        transaction
            .lock_keys_with_context(&mut context, [keys[1].clone()])
            .await
            .unwrap();
        assert!(transaction.is_in_aggressive_locking_mode());
        transaction
            .lock_keys_with_context(&mut context, [keys[2].clone(), keys[3].clone()])
            .await
            .unwrap();
        assert!(!transaction.is_in_aggressive_locking_mode());
        assert!(!source_2pc_is_locked(&cluster, &keys[0]));
        for key in &keys[1..] {
            assert!(source_2pc_is_locked(&cluster, key));
        }
        transaction.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAggressiveLockingResetTTLManager() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let options = || {
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(10)))
                .drop_check(CheckLevel::None)
        };
        let key = source_2pc_key("aggressive-reset-ttl", "k1");
        let mut transaction = source_integration_transaction_with_options(&pd, options()).await;
        transaction.start_aggressive_locking();
        assert!(!transaction.committer_initialized);
        let mut context = LockContext::new(
            transaction.start_timestamp().version(),
            0,
            SystemTime::now(),
        );
        transaction
            .lock_keys_with_context(&mut context, [key.clone()])
            .await
            .unwrap();
        assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));
        transaction.cancel_aggressive_locking().await.unwrap();
        assert!(!transaction.is_in_aggressive_locking_mode());
        assert!(!transaction.is_heartbeat_started.load(Ordering::Acquire));
        transaction.rollback().await.unwrap();

        let mut transaction = source_integration_transaction_with_options(&pd, options()).await;
        transaction.start_aggressive_locking();
        assert!(!transaction.committer_initialized);
        let mut blocker = source_2pc_pessimistic_transaction(&pd).await;
        blocker.lock_keys([key.clone()]).await.unwrap();
        let mut context = LockContext::new(
            transaction.start_timestamp().version(),
            crate::kv::LOCK_ALWAYS_WAIT,
            SystemTime::now(),
        );
        let mut lock = Box::pin(
            transaction.lock_keys_with_context(&mut context, [key.clone()]),
        );
        tokio::select! {
            result = &mut lock => panic!("blocked lock completed early: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
        blocker.put(key.clone(), b"v1".to_vec()).await.unwrap();
        let blocker_commit_ts = Box::pin(blocker.commit())
            .await
            .unwrap()
            .unwrap()
            .version();
        tokio::time::timeout(Duration::from_secs(1), &mut lock)
            .await
            .expect("aggressive lock did not resume after blocker commit")
            .unwrap();
        drop(lock);
        assert_eq!(context.max_locked_with_conflict_ts, blocker_commit_ts);
        assert!(blocker_commit_ts > transaction.start_timestamp().version());
        assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));

        transaction.retry_aggressive_locking().await.unwrap();
        assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));
        let for_update_ts = crate::pd::PdClient::get_timestamp(pd.clone())
            .await
            .unwrap()
            .version();
        let mut retry_context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut retry_context, [key])
            .await
            .unwrap();
        assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));
        transaction.cancel_aggressive_locking().await.unwrap();
        assert!(!transaction.is_heartbeat_started.load(Ordering::Acquire));
        assert_eq!(transaction.pessimistic_lock_count, 0);
        transaction.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAggressiveLockingKeepAliveAfterMultiLockKeysOperation() {
    source_2pc_run(|| async move {
        for (is_retried, is_primary_changed) in
            [(false, false), (true, false), (true, true)]
        {
            let (_cluster, pd) = source_integration_store();
            let prefix = format!("aggressive-keepalive/{is_retried}/{is_primary_changed}");
            let key0 = source_2pc_key(&prefix, "k0");
            let key1 = source_2pc_key(&prefix, "k1");
            let key2 = source_2pc_key(&prefix, "k2");
            let mut transaction = source_integration_transaction_with_options(
                &pd,
                TransactionOptions::new_pessimistic()
                    .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(10)))
                    .drop_check(CheckLevel::None),
            )
            .await;
            assert!(!transaction.committer_initialized);
            transaction.start_aggressive_locking();
            let mut context = LockContext::new(
                transaction.start_timestamp().version(),
                1_000,
                SystemTime::now(),
            );
            if is_retried {
                let last_primary = if is_primary_changed {
                    key0.clone()
                } else {
                    key1.clone()
                };
                transaction
                    .lock_keys_with_context(&mut context, [last_primary.clone()])
                    .await
                    .unwrap();
                assert_eq!(transaction.buffer.get_primary_key().unwrap().as_ref(), last_primary);
                transaction.retry_aggressive_locking().await.unwrap();
                assert_eq!(
                    transaction
                        .aggressive_locking
                        .as_ref()
                        .unwrap()
                        .last_primary_key
                        .as_ref()
                        .unwrap()
                        .as_ref(),
                    last_primary
                );
            }
            transaction
                .lock_keys_with_context(&mut context, [key1.clone()])
                .await
                .unwrap();
            assert!(transaction.is_in_aggressive_locking_stage(key1.clone()));
            assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));
            assert_eq!(transaction.buffer.get_primary_key().unwrap().as_ref(), key1);
            transaction
                .lock_keys_with_context(&mut context, [key2.clone()])
                .await
                .unwrap();
            assert!(transaction.is_in_aggressive_locking_stage(key1.clone()));
            assert!(transaction.is_in_aggressive_locking_stage(key2.clone()));
            assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));
            assert_eq!(transaction.buffer.get_primary_key().unwrap().as_ref(), key1);
            transaction.done_aggressive_locking().await.unwrap();
            assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));

            let mut competitor = source_2pc_pessimistic_transaction(&pd).await;
            for key in [&key1, &key2] {
                let mut blocked = LockContext::new(
                    competitor.start_timestamp().version(),
                    100,
                    SystemTime::now(),
                );
                let error = competitor
                    .lock_keys_with_context(&mut blocked, [key.clone()])
                    .await
                    .unwrap_err();
                assert!(crate::error::is_lock_wait_timeout(&error), "{error:?}");
            }
            competitor.rollback().await.unwrap();
            transaction.rollback().await.unwrap();
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAggressiveLockingResetPrimaryAndTTLManagerAfterExit() {
    source_2pc_run(|| async move {
        for done in [false, true] {
            for exit_phase in 0_u8..4 {
                for retry_different_key in [false, true] {
                    let (_cluster, pd) = source_integration_store();
                    let prefix = format!(
                        "aggressive-reset-exit/{done}/{exit_phase}/{retry_different_key}"
                    );
                    let key1 = source_2pc_key(&prefix, "k1");
                    let key2 = source_2pc_key(&prefix, "k2");
                    let mut transaction = source_integration_transaction_with_options(
                        &pd,
                        TransactionOptions::new_pessimistic()
                            .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_secs(10)))
                            .drop_check(CheckLevel::None),
                    )
                    .await;
                    transaction.start_aggressive_locking();
                    assert!(!transaction.committer_initialized);

                    if exit_phase == 0 {
                        if done {
                            transaction.done_aggressive_locking().await.unwrap();
                        } else {
                            transaction.cancel_aggressive_locking().await.unwrap();
                        }
                        assert!(!transaction.is_in_aggressive_locking_mode());
                        assert_eq!(transaction.pessimistic_lock_count, 0);
                        assert!(transaction.buffer.get_primary_key().is_none());
                        assert!(!transaction.is_heartbeat_started.load(Ordering::Acquire));
                        transaction.rollback().await.unwrap();
                        continue;
                    }

                    let mut context = LockContext::new(
                        transaction.start_timestamp().version(),
                        0,
                        SystemTime::now(),
                    );
                    transaction
                        .lock_keys_with_context(&mut context, [key1.clone()])
                        .await
                        .unwrap();
                    assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));
                    assert_eq!(transaction.pessimistic_lock_count, 1);
                    if exit_phase == 1 {
                        if done {
                            transaction.done_aggressive_locking().await.unwrap();
                            assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));
                            assert_eq!(transaction.pessimistic_lock_count, 1);
                        } else {
                            transaction.cancel_aggressive_locking().await.unwrap();
                            assert!(!transaction.is_heartbeat_started.load(Ordering::Acquire));
                            assert_eq!(transaction.pessimistic_lock_count, 0);
                        }
                        assert!(!transaction.is_in_aggressive_locking_mode());
                        transaction.rollback().await.unwrap();
                        continue;
                    }

                    transaction.retry_aggressive_locking().await.unwrap();
                    if exit_phase == 2 {
                        if done {
                            transaction.done_aggressive_locking().await.unwrap();
                        } else {
                            transaction.cancel_aggressive_locking().await.unwrap();
                        }
                        assert!(!transaction.is_in_aggressive_locking_mode());
                        assert_eq!(transaction.pessimistic_lock_count, 0);
                        assert!(transaction.buffer.get_primary_key().is_none());
                        assert!(!transaction.is_heartbeat_started.load(Ordering::Acquire));
                        transaction.rollback().await.unwrap();
                        continue;
                    }

                    let for_update_ts = crate::pd::PdClient::get_timestamp(pd.clone())
                        .await
                        .unwrap()
                        .version();
                    context = LockContext::new(for_update_ts, 0, SystemTime::now());
                    let retry_key = if retry_different_key {
                        key2.clone()
                    } else {
                        key1.clone()
                    };
                    transaction
                        .lock_keys_with_context(&mut context, [retry_key.clone()])
                        .await
                        .unwrap();
                    assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));
                    assert_eq!(
                        transaction.buffer.get_primary_key().unwrap().as_ref(),
                        retry_key
                    );
                    assert_eq!(
                        transaction.pessimistic_lock_count,
                        if retry_different_key { 2 } else { 1 }
                    );
                    if done {
                        transaction.done_aggressive_locking().await.unwrap();
                        assert!(transaction.is_heartbeat_started.load(Ordering::Acquire));
                        assert_eq!(transaction.pessimistic_lock_count, 1);
                    } else {
                        transaction.cancel_aggressive_locking().await.unwrap();
                        assert!(!transaction.is_heartbeat_started.load(Ordering::Acquire));
                        assert_eq!(transaction.pessimistic_lock_count, 0);
                    }
                    assert!(!transaction.is_in_aggressive_locking_mode());
                    transaction.rollback().await.unwrap();
                }
            }
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestElapsedTTL() {
    source_2pc_run(|| async move {
        let (cluster, pd) = source_integration_store();
        let key = source_2pc_key("elapsed-ttl", "key");
        let start_ts = crate::oracle::system_time_to_timestamp(
            SystemTime::now() + Duration::from_secs(10),
        ) + 1;
        let mut transaction = Transaction::new(
            Timestamp::from_version(start_ts),
            pd,
            TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        let for_update_ts = crate::oracle::compose_timestamp(
            crate::oracle::extract_physical(start_ts) + 100,
            1,
        );
        let mut context = LockContext::new(for_update_ts, 0, SystemTime::now());
        transaction
            .lock_keys_with_context(&mut context, [key.clone()])
            .await
            .unwrap();
        let lock_ttl = cluster.engine().mvcc_get_by_key(&key).lock.unwrap().ttl;
        let elapsed = lock_ttl - super::managed_lock_ttl();
        assert!(elapsed >= 100, "elapsed TTL was only {elapsed}ms");
        assert!(elapsed < 150, "elapsed TTL was {elapsed}ms");
        transaction.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestDeleteYourWriteCauseGhostPrimary() {
    source_2pc_run(|| async move {
        let k1 = source_2pc_key("ghost-primary", "a");
        let k2 = source_2pc_key("ghost-primary", "b");
        let k3 = source_2pc_key("ghost-primary", "c");
        let split = source_2pc_key("ghost-primary", "b");
        let (cluster, pd) = source_2pc_store_with_splits(&[split]);
        let start_ts = crate::pd::PdClient::get_timestamp(pd.clone())
            .await
            .unwrap()
            .version();
        let mutations = vec![
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::CheckNotExists as i32,
                key: k1.clone(),
                ..Default::default()
            },
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key: k2.clone(),
                value: vec![1],
                ..Default::default()
            },
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key: k3.clone(),
                value: vec![2],
                ..Default::default()
            },
        ];
        let mut committer = source_2pc_committer(
            pd.clone(),
            start_ts,
            Some(Key::from(k2.clone())),
            mutations,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
        );
        committer.prewrite().await.unwrap();
        assert!(cluster.engine().mvcc_get_by_key(&k1).lock.is_none());
        for key in [&k2, &k3] {
            let lock = cluster.engine().mvcc_get_by_key(key).lock.unwrap();
            assert_eq!(lock.op, unistore::Op::Put);
            assert_eq!(lock.primary, k2);
        }
        committer.commit_primary().await.unwrap();
        assert!(cluster.engine().mvcc_get_by_key(&k2).lock.is_none());
        assert!(cluster.engine().mvcc_get_by_key(&k3).lock.is_some());

        let mut reader = source_integration_transaction(&pd, false).await;
        assert_eq!(reader.get(k3.clone()).await.unwrap(), Some(vec![2]));
        assert!(cluster.engine().mvcc_get_by_key(&k3).lock.is_none());
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestDeleteAllYourWrites() {
    source_2pc_run(|| async move {
        let keys = [
            source_2pc_key("delete-all-writes", "a"),
            source_2pc_key("delete-all-writes", "b"),
            source_2pc_key("delete-all-writes", "c"),
        ];
        let (cluster, pd) = source_2pc_store_with_splits(&[keys[1].clone()]);
        let mut transaction = source_integration_transaction(&pd, false).await;
        for (index, key) in keys.iter().enumerate() {
            transaction
                .get_mem_buffer()
                .set_with_flags(
                    key,
                    &[index as u8],
                    &[crate::FlagsOp::SetPresumeKeyNotExists],
                )
                .unwrap();
            transaction.delete(key.clone()).await.unwrap();
        }
        Box::pin(transaction.commit()).await.unwrap();
        for key in &keys {
            let info = cluster.engine().mvcc_get_by_key(key);
            assert!(info.lock.is_none());
            let mut reader = source_integration_transaction(&pd, false).await;
            assert_eq!(reader.get(key.clone()).await.unwrap(), None);
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestDeleteAllYourWritesWithSFU() {
    source_2pc_run(|| async move {
        let k1 = source_2pc_key("delete-all-sfu", "a");
        let k2 = source_2pc_key("delete-all-sfu", "b");
        let k3 = source_2pc_key("delete-all-sfu", "c");
        let (cluster, pd) = source_2pc_store_with_splits(&[k2.clone()]);
        let start_ts = crate::pd::PdClient::get_timestamp(pd.clone())
            .await
            .unwrap()
            .version();
        let mutations = vec![
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::CheckNotExists as i32,
                key: k1.clone(),
                ..Default::default()
            },
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Lock as i32,
                key: k2.clone(),
                ..Default::default()
            },
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Lock as i32,
                key: k3.clone(),
                ..Default::default()
            },
        ];
        let mut committer = source_2pc_committer(
            pd.clone(),
            start_ts,
            Some(Key::from(k2.clone())),
            mutations,
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat),
        );
        committer.prewrite().await.unwrap();
        assert!(cluster.engine().mvcc_get_by_key(&k1).lock.is_none());
        for key in [&k2, &k3] {
            let lock = cluster.engine().mvcc_get_by_key(key).lock.unwrap();
            assert_eq!(lock.op, unistore::Op::Lock);
            assert_eq!(lock.primary, k2);
        }
        committer.commit_primary().await.unwrap();

        let mut writer = source_integration_transaction(&pd, false).await;
        writer.put(k3.clone(), vec![33]).await.unwrap();
        Box::pin(writer.commit()).await.unwrap();
        source_2pc_assert_values(&pd, &[(&k3, &[33])]).await;
        assert!(cluster.engine().mvcc_get_by_key(&k3).lock.is_none());
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAcquireFalseTimeoutLock() {
    source_2pc_run(|| async move {
        let _ttl = SourceManagedTtlGuard::set(1_000);
        let (_cluster, pd) = source_integration_store();
        let k1 = source_2pc_key("false-timeout", "k1");
        let k2 = source_2pc_key("false-timeout", "k2");
        let mut holder = source_integration_transaction_with_options(
            &pd,
            TransactionOptions::new_pessimistic().drop_check(CheckLevel::None),
        )
        .await;
        holder.lock_keys([k1.clone()]).await.unwrap();
        holder.lock_keys([k2.clone()]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        let mut waiter = source_2pc_pessimistic_transaction(&pd).await;
        let mut no_wait = LockContext::new(
            waiter.start_timestamp().version(),
            crate::kv::LOCK_NO_WAIT,
            SystemTime::now(),
        );
        let error = waiter
            .lock_keys_with_context(&mut no_wait, [k2.clone()])
            .await
            .unwrap_err();
        assert!(
            crate::error::is_lock_acquire_fail_and_no_wait_set(&error),
            "{error:?}"
        );
        let mut limited = LockContext::new(
            waiter.start_timestamp().version(),
            200,
            SystemTime::now(),
        );
        let error = waiter
            .lock_keys_with_context(&mut limited, [k2])
            .await
            .unwrap_err();
        assert!(crate::error::is_lock_wait_timeout(&error), "{error:?}");
        waiter.rollback().await.unwrap();
        holder.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPkNotFound() {
    source_2pc_run(|| async move {
        let _ttl = SourceManagedTtlGuard::set(100);
        let (cluster, pd) = source_integration_store();
        let k1 = source_2pc_key("pk-not-found", "k1");
        let k2 = source_2pc_key("pk-not-found", "k2");
        let k3 = source_2pc_key("pk-not-found", "k3");
        let mut holder = source_2pc_pessimistic_transaction(&pd).await;
        holder.lock_keys([k1.clone()]).await.unwrap();
        holder
            .lock_keys([k2.clone(), k3.clone()])
            .await
            .unwrap();
        let start_ts = holder.start_timestamp().version();
        let current_ts = crate::oracle::system_time_to_timestamp(
            SystemTime::now() + Duration::from_millis(200),
        );
        let first = cluster
            .engine()
            .check_txn_status(&k1, start_ts, start_ts, current_ts, true, true)
            .unwrap();
        assert_eq!(first.2, unistore::Action::TtlExpirePessimisticRollback);
        let second = cluster
            .engine()
            .check_txn_status(&k1, start_ts, start_ts, current_ts, true, true)
            .unwrap();
        assert_eq!(second.2, unistore::Action::LockNotExistDoNothing);
        // The source constructs lockKey2 with TTL=0, which means resolve it
        // unconditionally. The ordinary Rust lock path re-reads the stored
        // lock metadata, so let its equivalent 100 ms TTL expire first.
        tokio::time::sleep(Duration::from_millis(110)).await;
        let secondary = cluster.engine().mvcc_get_by_key(&k2).lock.unwrap();
        let now = crate::pd::PdClient::get_timestamp(pd.clone()).await.unwrap();
        let remaining = crate::transaction::lock::lock_until_expired_ms(
            secondary.start_ts,
            secondary.ttl,
            now,
        );
        assert!(remaining <= 0, "secondary still has {remaining}ms TTL");

        let mut transaction2 = source_2pc_pessimistic_transaction(&pd).await;
        transaction2.lock_keys([k2.clone()]).await.unwrap();

        let rollback_errors = cluster.engine().pessimistic_rollback(
            &[],
            &[],
            std::slice::from_ref(&k3),
            start_ts,
            start_ts - 1,
        );
        assert!(rollback_errors.into_iter().all(|error| error.is_none()));

        let mut transaction3 = source_2pc_pessimistic_transaction(&pd).await;
        let mut no_wait = LockContext::new(
            transaction3.start_timestamp().version(),
            crate::kv::LOCK_NO_WAIT,
            SystemTime::now(),
        );
        transaction3
            .lock_keys_with_context(&mut no_wait, [k3.clone()])
            .await
            .unwrap();
        let third = cluster
            .engine()
            .check_txn_status(&k1, start_ts, start_ts, current_ts, true, true)
            .unwrap();
        assert_eq!(third.2, unistore::Action::LockNotExistDoNothing);
        transaction2.rollback().await.unwrap();
        transaction3.rollback().await.unwrap();
        holder.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPessimisticLockPrimary() {
    source_2pc_run(|| async move {
        let k1 = source_2pc_key("lock-primary", "a");
        let k2 = source_2pc_key("lock-primary", "b");
        let (cluster, pd) = source_2pc_store_with_splits(std::slice::from_ref(&k2));
        let mut holder = source_2pc_pessimistic_transaction(&pd).await;
        holder.lock_keys([k1.clone()]).await.unwrap();

        let mut blocked = source_2pc_pessimistic_transaction(&pd).await;
        let mut blocked_context = LockContext::new(
            blocked.start_timestamp().version(),
            200,
            SystemTime::now(),
        );
        let mut blocked_lock = Box::pin(blocked.lock_keys_with_context(
            &mut blocked_context,
            [k1.clone(), k2.clone()],
        ));
        tokio::select! {
            result = &mut blocked_lock => panic!("primary-blocked lock completed early: {result:?}"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }

        let mut third = source_2pc_pessimistic_transaction(&pd).await;
        let mut no_wait = LockContext::new(
            third.start_timestamp().version(),
            crate::kv::LOCK_NO_WAIT,
            SystemTime::now(),
        );
        third
            .lock_keys_with_context(&mut no_wait, [k2.clone()])
            .await
            .unwrap();
        assert_eq!(
            cluster
                .engine()
                .mvcc_get_by_key(&k2)
                .lock
                .as_ref()
                .unwrap()
                .start_ts,
            third.start_timestamp().version()
        );
        let error = tokio::time::timeout(Duration::from_secs(1), &mut blocked_lock)
            .await
            .expect("primary-blocked lock did not respect its wait deadline")
            .unwrap_err();
        assert!(crate::error::is_lock_wait_timeout(&error), "{error:?}");
        drop(blocked_lock);
        blocked.rollback().await.unwrap();
        third.rollback().await.unwrap();
        holder.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestResolvePessimisticLock() {
    source_2pc_run(|| async move {
        #[derive(Clone)]
        struct Filter;
        impl super::KvFilter for Filter {
            fn is_unnecessary_key_value(
                &self,
                key: &[u8],
                _value: &[u8],
                flags: super::MutationFlags,
            ) -> crate::Result<bool> {
                let untouched = key == b"t00000001_i000000001";
                if untouched && flags.presumes_key_not_exists() {
                    return Err(Error::StringError(
                        "unexpected untouched PresumeKeyNotExists path".to_owned(),
                    ));
                }
                Ok(untouched)
            }
        }

        let (_cluster, pd) = source_integration_store();
        let untouched = b"t00000001_i000000001".to_vec();
        let untouched_value = vec![0, 0, 0, 0, 0, 0, 0, 1, 49];
        let no_value = b"t00000001_i000000002".to_vec();
        let filter = Arc::new(Filter);
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction.set_kv_filter(filter.clone());
        transaction
            .put(untouched.clone(), untouched_value.clone())
            .await
            .unwrap();
        let mut context = LockContext::new(
            transaction.start_timestamp().version(),
            crate::kv::LOCK_NO_WAIT,
            SystemTime::now(),
        );
        transaction
            .lock_keys_with_context(&mut context, [untouched.clone(), no_value.clone()])
            .await
            .unwrap();
        let mutations = transaction
            .buffer
            .to_proto_mutations_with_filter(filter.as_ref())
            .unwrap();
        assert_eq!(mutations.len(), 2);
        assert_eq!(mutations[0].op, kvrpcpb::Op::Lock as i32);
        assert_eq!(mutations[0].key, untouched);
        assert_eq!(mutations[0].value, untouched_value);
        assert_eq!(mutations[1].op, kvrpcpb::Op::Lock as i32);
        assert_eq!(mutations[1].key, no_value);
        assert!(mutations[1].value.is_empty());
        transaction.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestCommitDeadLock() {
    source_2pc_run(|| async move {
        let restore = crate::config::update_global(|config| {
            config.tikv_client.ttl_refreshed_txn_size = 0;
        });
        let k1 = source_2pc_key("commit-deadlock", "a");
        let k2 = source_2pc_key("commit-deadlock", "y");
        let (cluster, pd) = source_2pc_store_with_splits(std::slice::from_ref(&k2));
        let region1 = crate::pd::PdClient::region_for_key(pd.as_ref(), &Key::from(k1.clone()))
            .await
            .unwrap()
            .region
            .id;
        let region2 = crate::pd::PdClient::region_for_key(pd.as_ref(), &Key::from(k2.clone()))
            .await
            .unwrap()
            .region
            .id;
        assert_ne!(region1, region2);

        let options = || {
            TransactionOptions::new_optimistic()
                .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(1)))
                .drop_check(CheckLevel::None)
        };
        let mut transaction1 =
            source_integration_transaction_with_options(&pd, options()).await;
        transaction1.put(k1.clone(), b"t1".to_vec()).await.unwrap();
        transaction1.put(k2.clone(), b"t1".to_vec()).await.unwrap();
        transaction1.buffer.primary_key_or(&Key::from(k1.clone()));

        let mut transaction2 =
            source_integration_transaction_with_options(&pd, options()).await;
        transaction2.put(k1.clone(), b"t2".to_vec()).await.unwrap();
        transaction2.put(k2.clone(), b"t2".to_vec()).await.unwrap();
        transaction2.buffer.primary_key_or(&Key::from(k2.clone()));

        cluster.schedule_delay(
            transaction2.start_timestamp().version(),
            region1,
            Duration::from_millis(5),
        );
        cluster.schedule_delay(
            transaction1.start_timestamp().version(),
            region2,
            Duration::from_millis(5),
        );
        let (first, second) = tokio::join!(
            Box::pin(transaction1.commit()),
            Box::pin(transaction2.commit())
        );
        assert_eq!(usize::from(first.is_err()) + usize::from(second.is_err()), 1);
        restore();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPushPessimisticLock() {
    source_2pc_run(|| async move {
        let (cluster, pd) = source_integration_store();
        let k1 = source_2pc_key("push-pessimistic", "a");
        let k2 = source_2pc_key("push-pessimistic", "b");
        let mut transaction1 = source_2pc_pessimistic_transaction(&pd).await;
        transaction1
            .lock_keys([k1.clone(), k2.clone()])
            .await
            .unwrap();
        transaction1.put(k2.clone(), b"v2".to_vec()).await.unwrap();

        let start_ts = transaction1.start_timestamp().version();
        let mut committer = source_2pc_committer(
            pd.clone(),
            start_ts,
            Some(Key::from(k1.clone())),
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key: k2.clone(),
                value: b"v2".to_vec(),
                ..Default::default()
            }],
            transaction1.options.clone(),
        )
        .with_pessimistic_lock_keys(BTreeSet::from([k2.clone()]));
        let cleanup = committer.clone();
        committer.prewrite().await.unwrap();

        let primary = cluster.engine().mvcc_get_by_key(&k1).lock.unwrap();
        assert_eq!(primary.op, unistore::Op::PessimisticLock);
        assert_eq!(primary.primary, k1);
        let secondary = cluster.engine().mvcc_get_by_key(&k2).lock.unwrap();
        assert_eq!(secondary.op, unistore::Op::Put);
        assert_eq!(secondary.primary, k1);

        let mut transaction2 = source_integration_transaction(&pd, false).await;
        let started = Instant::now();
        let value = tokio::time::timeout(
            Duration::from_millis(500),
            transaction2.get(k2.clone()),
        )
        .await
        .expect("optimistic lock should not block a read")
        .unwrap();
        assert_eq!(value, None);
        assert!(started.elapsed() < Duration::from_millis(500));

        cleanup.rollback(true).await.unwrap();
        transaction1.rollback().await.unwrap();
        transaction2.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestResolveMixed() {
    source_2pc_run(|| async move {
        let _ttl = SourceManagedTtlGuard::set(100);
        let (cluster, pd) = source_integration_store();
        let primary = source_2pc_key("resolve-mixed", "pk");
        let pessimistic = source_2pc_key("resolve-mixed", "pessimistic");
        let threshold = crate::config::get_global_config()
            .tikv_client
            .resolve_lock_lite_threshold as usize;
        let secondaries = (0..threshold)
            .map(|index| source_2pc_key("resolve-mixed", &format!("optimistic-{index:04}")))
            .collect::<Vec<_>>();

        let mut transaction1 = source_2pc_pessimistic_transaction(&pd).await;
        transaction1.lock_keys([primary.clone()]).await.unwrap();
        let start_ts = transaction1.start_timestamp().version();
        let mut mutations = vec![kvrpcpb::Mutation {
            op: kvrpcpb::Op::Lock as i32,
            key: primary.clone(),
            ..Default::default()
        }];
        mutations.extend(secondaries.iter().enumerate().map(|(index, key)| {
            kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key: key.clone(),
                value: format!("v{index}").into_bytes(),
                ..Default::default()
            }
        }));
        let mut committer = source_2pc_committer(
            pd.clone(),
            start_ts,
            Some(Key::from(primary.clone())),
            mutations,
            transaction1.options.clone(),
        )
        .with_pessimistic_lock_keys(BTreeSet::from([primary.clone()]));
        committer.write_size = crate::kv::TXN_COMMIT_BATCH_SIZE.load(Ordering::SeqCst);
        committer.prewrite().await.unwrap();

        transaction1
            .lock_keys([pessimistic.clone()])
            .await
            .unwrap();
        source_2pc_assert_primary(&cluster, &pessimistic, &primary);
        let optimistic = secondaries[0].clone();
        let optimistic_lock = cluster.engine().mvcc_get_by_key(&optimistic).lock.unwrap();
        assert_eq!(optimistic_lock.op, unistore::Op::Put);
        assert_eq!(optimistic_lock.primary, primary);
        assert!(optimistic_lock.txn_size >= threshold as u64);

        cluster
            .engine()
            .rollback(std::slice::from_ref(&primary), start_ts)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(110)).await;
        let read_ts = crate::pd::PdClient::get_timestamp(pd.clone()).await.unwrap();
        assert!(crate::transaction::resolve_locks_with_context(
            vec![source_lock_info(&cluster, &optimistic)],
            read_ts,
            pd.clone(),
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
        )
        .await
        .unwrap()
        .is_empty());
        assert!(!source_2pc_is_locked(&cluster, &pessimistic));

        let mut transaction2 = source_2pc_pessimistic_transaction(&pd).await;
        let mut no_wait = LockContext::new(
            transaction2.start_timestamp().version(),
            crate::kv::LOCK_NO_WAIT,
            SystemTime::now(),
        );
        transaction2
            .lock_keys_with_context(&mut no_wait, [pessimistic.clone()])
            .await
            .unwrap();
        transaction1.rollback().await.unwrap();
        transaction2.rollback().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestPrewriteSecondaryKeys() {
    source_2pc_run(|| async move {
        let split = source_2pc_key("prewrite-secondaries", "100");
        let (_cluster, pd) = source_2pc_store_with_splits(std::slice::from_ref(&split));
        let requests = Arc::new(Mutex::new(Vec::<kvrpcpb::PrewriteRequest>::new()));
        let captured = Arc::clone(&requests);
        let interceptor = crate::new_rpc_interceptor(
            "capture-2pc-async-secondary-keys",
            move |_, request, next| {
                if let Some(request) = request.as_any().downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    captured.lock().unwrap().push(request.clone());
                }
                Box::pin(async move { next().await })
            },
        );
        let mut transaction = source_async_commit_transaction(&pd).await;
        transaction.set_rpc_interceptor(interceptor);
        let expected = (50..120)
            .map(|index| source_2pc_key("prewrite-secondaries", &format!("{index:03}")))
            .collect::<Vec<_>>();
        for key in &expected {
            transaction
                .put(key.clone(), vec![0; 1_024])
                .await
                .unwrap();
        }
        for index in (50..120).step_by(10) {
            transaction
                .put(
                    source_2pc_key("prewrite-secondaries", &format!("{index:03}")),
                    vec![0; 188],
                )
                .await
                .unwrap();
        }
        Box::pin(transaction.commit()).await.unwrap();

        let requests = requests.lock().unwrap();
        let primary_requests = requests
            .iter()
            .filter(|request| {
                request
                    .mutations
                    .iter()
                    .any(|mutation| mutation.key == request.primary_lock)
            })
            .collect::<Vec<_>>();
        let secondary_requests = requests
            .iter()
            .filter(|request| {
                request
                    .mutations
                    .iter()
                    .all(|mutation| mutation.key != request.primary_lock)
            })
            .collect::<Vec<_>>();
        assert!(!primary_requests.is_empty());
        assert!(!secondary_requests.is_empty());
        for request in primary_requests {
            assert!(request.use_async_commit);
            assert!(!request
                .secondaries
                .iter()
                .any(|key| key == &request.primary_lock));
            let unique = request
                .secondaries
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            assert_eq!(unique.len(), request.secondaries.len());
            assert_eq!(
                unique,
                expected
                    .iter()
                    .filter(|key| *key != &request.primary_lock)
                    .cloned()
                    .collect()
            );
        }
        for request in secondary_requests {
            assert!(request.use_async_commit);
            assert!(request.secondaries.is_empty());
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAsyncCommit() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let primary = source_2pc_key("async-commit", "tpk");
        let secondary = source_2pc_key("async-commit", "tk1");
        let mut transaction = source_async_commit_transaction(&pd).await;
        transaction
            .put(primary.clone(), b"pkVal".to_vec())
            .await
            .unwrap();
        transaction
            .put(secondary.clone(), b"k1Val".to_vec())
            .await
            .unwrap();
        Box::pin(transaction.commit()).await.unwrap();
        source_2pc_assert_values(
            &pd,
            &[(&primary, b"pkVal"), (&secondary, b"k1Val")],
        )
        .await;
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestRetryPushTTL() {
    source_2pc_run(|| async move {
        let _ttl = SourceManagedTtlGuard::set(3_000);
        let (_cluster, pd) = source_integration_store();
        let key = source_2pc_key("retry-push-ttl", "a");

        let mut transaction1 = source_2pc_pessimistic_transaction(&pd).await;
        transaction1.lock_keys([key.clone()]).await.unwrap();
        let transaction2 = source_2pc_pessimistic_transaction(&pd).await;
        let key_for_second = key.clone();
        let second = tokio::spawn(async move {
            let mut transaction2 = transaction2;
            transaction2.lock_keys([key_for_second]).await.unwrap();
            transaction2
        });
        tokio::time::sleep(Duration::from_secs(2)).await;
        transaction1.rollback().await.unwrap();
        let mut transaction2 = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("second transaction should acquire the released lock")
            .unwrap();

        let transaction3 = source_2pc_pessimistic_transaction(&pd).await;
        let key_for_third = key.clone();
        let (acquired, mut acquired_rx) = tokio::sync::mpsc::channel(1);
        let third = tokio::spawn(async move {
            let mut transaction3 = transaction3;
            transaction3.lock_keys([key_for_third]).await.unwrap();
            acquired.send(()).await.unwrap();
            transaction3.rollback().await.unwrap();
        });
        assert!(tokio::time::timeout(Duration::from_secs(2), acquired_rx.recv())
            .await
            .is_err());
        transaction2.rollback().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), acquired_rx.recv())
            .await
            .expect("third transaction should acquire the released lock")
            .expect("third transaction dropped its acquisition signal");
        third.await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestAsyncCommitCheck() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let keys = (0..16)
            .map(|index| source_2pc_key("async-check", &format!("{index:02}")))
            .collect::<Vec<_>>();
        let mutations = keys
            .iter()
            .cloned()
            .map(|key| kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key,
                value: b"v".to_vec(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let total_key_size = keys.iter().map(Vec::len).sum::<usize>() as u64;
        let committer = source_2pc_committer(
            pd,
            1,
            Some(Key::from(keys[0].clone())),
            mutations,
            TransactionOptions::new_optimistic().use_async_commit(),
        );

        let restore = crate::config::update_global(|config| {
            config.tikv_client.async_commit.keys_limit = 16;
            config.tikv_client.async_commit.total_key_size_limit = total_key_size;
        });
        assert!(committer.check_async_commit());
        let _restore_keys_limit = crate::config::update_global(|config| {
            config.tikv_client.async_commit.keys_limit = 15;
        });
        assert!(!committer.check_async_commit());
        let _restore_size_limit = crate::config::update_global(|config| {
            config.tikv_client.async_commit.keys_limit = 20;
            config.tikv_client.async_commit.total_key_size_limit = total_key_size - 1;
        });
        assert!(!committer.check_async_commit());
        restore();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestFailCommitPrimaryRpcErrors() {
    source_2pc_run(|| async move {
        let _backoff = SourceAtomicU64Guard::set(&super::COMMIT_MAX_BACKOFF, 20);
        let scenario = FailScenario::setup();
        crate::util::enable_failpoints();
        fail::cfg("tikvclient/rpcCommitResult", "return(timeout)").unwrap();
        let (_cluster, pd) = source_integration_store();
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction
            .put(source_2pc_key("fail-primary-rpc", "a"), b"a1".to_vec())
            .await
            .unwrap();
        let error = Box::pin(transaction.commit()).await.unwrap_err();
        assert!(crate::error::is_error_undetermined(&error), "{error:?}");
        assert!(matches!(
            transaction.rollback().await.unwrap_err(),
            Error::Static(crate::error::StaticError::InvalidTransaction)
        ));
        fail::remove("tikvclient/rpcCommitResult");
        scenario.teardown();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestFailCommitPrimaryRegionError() {
    source_2pc_run(|| async move {
        let _backoff = SourceAtomicU64Guard::set(&super::COMMIT_MAX_BACKOFF, 20);
        let scenario = FailScenario::setup();
        crate::util::enable_failpoints();
        fail::cfg("tikvclient/rpcCommitResult", "return(notLeader)").unwrap();
        let (_cluster, pd) = source_integration_store();
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction
            .put(source_2pc_key("fail-primary-region", "b"), b"b1".to_vec())
            .await
            .unwrap();
        let error = Box::pin(transaction.commit()).await.unwrap_err();
        assert!(!crate::error::is_error_undetermined(&error), "{error:?}");
        fail::remove("tikvclient/rpcCommitResult");
        scenario.teardown();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestFailCommitPrimaryRPCErrorThenRegionError() {
    source_2pc_run(|| async move {
        let _backoff = SourceAtomicU64Guard::set(&super::COMMIT_MAX_BACKOFF, 20);
        let scenario = FailScenario::setup();
        crate::util::enable_failpoints();
        fail::cfg(
            "tikvclient/rpcCommitResult",
            "1*return(timeout)->return(notLeader)",
        )
        .unwrap();
        let (_cluster, pd) = source_integration_store();
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction
            .put(
                source_2pc_key("fail-primary-rpc-region", "a"),
                b"a1".to_vec(),
            )
            .await
            .unwrap();
        let error = Box::pin(transaction.commit()).await.unwrap_err();
        assert!(crate::error::is_error_undetermined(&error), "{error:?}");
        fail::remove("tikvclient/rpcCommitResult");
        scenario.teardown();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestFailCommitPrimaryKeyError() {
    source_2pc_run(|| async move {
        let _backoff = SourceAtomicU64Guard::set(&super::COMMIT_MAX_BACKOFF, 20);
        let scenario = FailScenario::setup();
        crate::util::enable_failpoints();
        fail::cfg("tikvclient/rpcCommitResult", "return(keyError)").unwrap();
        let (_cluster, pd) = source_integration_store();
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction
            .put(source_2pc_key("fail-primary-key", "c"), b"c1".to_vec())
            .await
            .unwrap();
        let error = Box::pin(transaction.commit()).await.unwrap_err();
        assert!(!crate::error::is_error_undetermined(&error), "{error:?}");
        fail::remove("tikvclient/rpcCommitResult");
        scenario.teardown();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestFailCommitPrimaryRPCErrorThenKeyError() {
    source_2pc_run(|| async move {
        let _backoff = SourceAtomicU64Guard::set(&super::COMMIT_MAX_BACKOFF, 20);
        let scenario = FailScenario::setup();
        crate::util::enable_failpoints();
        fail::cfg(
            "tikvclient/rpcCommitResult",
            "1*return(timeout)->return(keyError)",
        )
        .unwrap();
        let (_cluster, pd) = source_integration_store();
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction
            .put(
                source_2pc_key("fail-primary-rpc-key", "c"),
                b"c1".to_vec(),
            )
            .await
            .unwrap();
        let error = Box::pin(transaction.commit()).await.unwrap_err();
        assert!(!crate::error::is_error_undetermined(&error), "{error:?}");
        fail::remove("tikvclient/rpcCommitResult");
        scenario.teardown();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestFailCommitTimeout() {
    source_2pc_run(|| async move {
        let _backoff = SourceAtomicU64Guard::set(&super::COMMIT_MAX_BACKOFF, 20);
        let scenario = FailScenario::setup();
        crate::util::enable_failpoints();
        fail::cfg("tikvclient/rpcCommitTimeout", "return(true)").unwrap();
        let (_cluster, pd) = source_integration_store();
        let keys = [
            source_2pc_key("fail-commit-timeout", "a"),
            source_2pc_key("fail-commit-timeout", "b"),
            source_2pc_key("fail-commit-timeout", "c"),
        ];
        let mut transaction = source_integration_transaction(&pd, false).await;
        for (index, key) in keys.iter().enumerate() {
            transaction
                .put(key.clone(), format!("value-{index}").into_bytes())
                .await
                .unwrap();
        }
        assert!(Box::pin(transaction.commit()).await.is_err());
        fail::remove("tikvclient/rpcCommitTimeout");

        let mut reader = source_integration_transaction(&pd, false).await;
        assert!(reader.get(keys[0].clone()).await.unwrap().is_some());
        assert!(reader.get(keys[1].clone()).await.unwrap().is_some());
        scenario.teardown();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestCommitMultipleRegions() {
    source_2pc_run(|| async move {
        let splits = [
            source_2pc_key("multi-region", "025"),
            source_2pc_key("multi-region", "050"),
            source_2pc_key("multi-region", "075"),
        ];
        let (_cluster, pd) = source_2pc_store_with_splits(&splits);

        let small = (0..100)
            .map(|index| {
                (
                    source_2pc_key("multi-region", &format!("{index:03}")),
                    format!("value-{index:03}").into_bytes(),
                )
            })
            .collect::<Vec<_>>();
        let mut transaction = source_integration_transaction(&pd, false).await;
        for (key, value) in &small {
            transaction.put(key.clone(), value.clone()).await.unwrap();
        }
        Box::pin(transaction.commit()).await.unwrap();
        let mut reader = source_integration_transaction(&pd, false).await;
        for (key, value) in &small {
            assert_eq!(reader.get(key.clone()).await.unwrap().as_ref(), Some(value));
        }

        let large_value_len =
            crate::kv::TXN_COMMIT_BATCH_SIZE.load(Ordering::SeqCst) as usize / 7;
        let large = (0..50)
            .map(|index| {
                (
                    source_2pc_key("multi-region-large", &format!("{index:03}")),
                    vec![b'a' + (index % 3) as u8; large_value_len],
                )
            })
            .collect::<Vec<_>>();
        let mut transaction = source_integration_transaction(&pd, false).await;
        for (key, value) in &large {
            transaction.put(key.clone(), value.clone()).await.unwrap();
        }
        Box::pin(transaction.commit()).await.unwrap();
        let mut reader = source_integration_transaction(&pd, false).await;
        for (key, value) in &large {
            assert_eq!(reader.get(key.clone()).await.unwrap().as_ref(), Some(value));
        }
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestNewlyInsertedMemDBFlag() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let k0 = source_2pc_key("newly-inserted", "k0");
        let k1 = source_2pc_key("newly-inserted", "k1");
        let k2 = source_2pc_key("newly-inserted", "k2");
        let mut transaction = source_2pc_pessimistic_transaction(&pd).await;

        transaction.delete(k0.clone()).await.unwrap();
        transaction.put(k0.clone(), b"v0".to_vec()).await.unwrap();
        assert!(!transaction
            .get_mem_buffer()
            .get_flags(&k0)
            .unwrap()
            .has_newly_inserted());

        transaction.lock_keys([k1.clone()]).await.unwrap();
        transaction
            .get_mem_buffer()
            .set_with_flags(&k1, b"v1", &[crate::FlagsOp::SetNewlyInserted])
            .unwrap();
        assert!(transaction
            .get_mem_buffer()
            .get_flags(&k1)
            .unwrap()
            .has_newly_inserted());

        transaction.lock_keys([k2.clone()]).await.unwrap();
        transaction.delete(k2.clone()).await.unwrap();
        assert!(!transaction
            .get_mem_buffer()
            .get_flags(&k2)
            .unwrap()
            .has_newly_inserted());
        transaction.put(k2.clone(), b"v2".to_vec()).await.unwrap();
        assert!(!transaction
            .get_mem_buffer()
            .get_flags(&k2)
            .unwrap()
            .has_newly_inserted());
        Box::pin(transaction.commit()).await.unwrap();
    });
}

#[test]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestFlagsInMemBufferMutations() {
    let mut transaction = Transaction::new(
        Timestamp::from_version(1),
        Arc::new(MockPdClient::default()),
        TransactionOptions::new_optimistic().drop_check(CheckLevel::None),
        Keyspace::Disable,
    );
    let operations = [
        kvrpcpb::Op::Put,
        kvrpcpb::Op::Del,
        kvrpcpb::Op::CheckNotExists,
    ];
    let mut cases = Vec::new();
    for operation in operations {
        for bits in 0..8 {
            let key = source_2pc_key("flags", &format!("{:05}", cases.len()));
            let value = format!("v{:05}", cases.len()).into_bytes();
            let pessimistic = bits & 0b100 != 0;
            let assert_exist = bits & 0b010 != 0;
            let assert_not_exist = bits & 0b001 != 0;
            let memdb = transaction.get_mem_buffer();
            match operation {
                kvrpcpb::Op::Put => memdb.set(&key, &value).unwrap(),
                kvrpcpb::Op::Del => memdb.delete(&key).unwrap(),
                kvrpcpb::Op::CheckNotExists => {
                    memdb
                        .delete_with_flags(
                            &key,
                            &[crate::FlagsOp::SetPresumeKeyNotExists],
                        )
                        .unwrap()
                }
                _ => unreachable!(),
            }
            let mut flags = Vec::new();
            if pessimistic {
                flags.push(crate::FlagsOp::SetKeyLocked);
            }
            flags.push(match (assert_exist, assert_not_exist) {
                (true, true) => crate::FlagsOp::SetAssertUnknown,
                (true, false) => crate::FlagsOp::SetAssertExist,
                (false, true) => crate::FlagsOp::SetAssertNotExist,
                (false, false) => crate::FlagsOp::SetAssertNone,
            });
            memdb.update_flags(&key, &flags);
            cases.push((
                key,
                operation,
                pessimistic,
                assert_exist,
                assert_not_exist,
            ));
        }
    }

    let mutations = transaction.buffer.to_proto_mutations();
    let pessimistic_keys = transaction.buffer.pessimistic_lock_keys();
    assert_eq!(mutations.len(), cases.len());
    for (index, (key, operation, pessimistic, assert_exist, assert_not_exist)) in
        cases.iter().enumerate()
    {
        let flags = transaction
            .get_mem_buffer()
            .get_flags(key)
            .unwrap();
        assert_eq!(mutations[index].key, *key);
        assert_eq!(kvrpcpb::Op::try_from(mutations[index].op).unwrap(), *operation);
        assert_eq!(pessimistic_keys.contains(key), *pessimistic);
        assert_eq!(flags.has_assert_exist(), *assert_exist && !*assert_not_exist);
        assert_eq!(flags.has_assert_not_exist(), *assert_not_exist && !*assert_exist);
        assert_eq!(flags.has_assert_unknown(), *assert_exist && *assert_not_exist);
        let expected_assertion = match (*assert_exist, *assert_not_exist) {
            (true, false) => kvrpcpb::Assertion::Exist,
            (false, true) => kvrpcpb::Assertion::NotExist,
            _ => kvrpcpb::Assertion::None,
        };
        assert_eq!(mutations[index].assertion, expected_assertion as i32);
    }
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestExtractKeyExistsErr() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let key = source_2pc_key("extract-key-exists", "de");
        let mut initial = source_integration_transaction(&pd, false).await;
        initial.put(key.clone(), b"ef".to_vec()).await.unwrap();
        Box::pin(initial.commit()).await.unwrap();

        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction
            .get_mem_buffer()
            .set_with_flags(
                &key,
                b"fg",
                &[crate::FlagsOp::SetPresumeKeyNotExists],
            )
            .unwrap();
        let mutations = transaction.buffer.to_proto_mutations();
        assert_eq!(mutations[0].op, kvrpcpb::Op::Insert as i32);
        transaction
            .get_mem_buffer()
            .update_flags(&key, &[crate::FlagsOp::DelPresumeKeyNotExists]);
        let current_presume_keys = transaction.buffer.presume_key_not_exists_keys();
        assert!(current_presume_keys.is_empty());
        let mut committer = source_2pc_committer(
            pd,
            transaction.start_timestamp().version(),
            Some(Key::from(key.clone())),
            mutations,
            transaction.options.clone(),
        )
        .with_presume_key_not_exists_keys(current_presume_keys);
        let error = committer.prewrite().await.unwrap_err();
        assert!(error.to_string().contains("existErr for key"), "{error:?}");
        assert!(transaction.get_mem_buffer().get_flags(&key).is_ok());
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestKillSignal() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let killed = Arc::new(std::sync::atomic::AtomicU32::new(2));
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction.set_variables(Arc::new(crate::Variables::new(killed)));
        transaction
            .put(source_2pc_key("kill-signal", "key"), b"value".to_vec())
            .await
            .unwrap();
        let error = Box::pin(transaction.commit()).await.unwrap_err();
        assert!(error.to_string().contains("query interrupted"), "{error:?}");
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestUninterruptibleAction() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();

        let killed = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let key = source_2pc_key("uninterruptible-cleanup", "k1");
        let start_ts = crate::pd::PdClient::get_timestamp(pd.clone())
            .await
            .unwrap()
            .version();
        let mut cleanup = source_2pc_committer(
            pd.clone(),
            start_ts,
            Some(Key::from(key.clone())),
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key,
                value: b"v1".to_vec(),
                ..Default::default()
            }],
            TransactionOptions::new_optimistic(),
        );
        cleanup.settings.variables = Arc::new(crate::Variables::new(killed.clone()));
        cleanup.prewrite().await.unwrap();
        killed.store(2, Ordering::SeqCst);
        cleanup.rollback(true).await.unwrap();

        let killed = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let key = source_2pc_key("uninterruptible-pessimistic", "k2");
        let mut pessimistic = source_2pc_pessimistic_transaction(&pd).await;
        pessimistic.set_variables(Arc::new(crate::Variables::new(killed.clone())));
        let mut no_wait = LockContext::new(
            pessimistic.start_timestamp().version(),
            crate::kv::LOCK_NO_WAIT,
            SystemTime::now(),
        );
        pessimistic
            .lock_keys_with_context(&mut no_wait, [key])
            .await
            .unwrap();
        killed.store(2, Ordering::SeqCst);
        pessimistic.rollback().await.unwrap();

        let killed = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let key = source_2pc_key("uninterruptible-commit", "k1");
        let start_ts = crate::pd::PdClient::get_timestamp(pd.clone())
            .await
            .unwrap()
            .version();
        let mut commit = source_2pc_committer(
            pd,
            start_ts,
            Some(Key::from(key.clone())),
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key,
                value: b"v1".to_vec(),
                ..Default::default()
            }],
            TransactionOptions::new_optimistic(),
        );
        commit.settings.variables = Arc::new(crate::Variables::new(killed.clone()));
        commit.prewrite().await.unwrap();
        killed.store(2, Ordering::SeqCst);
        commit.commit_primary().await.unwrap();
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_Test2PCLifecycleHooks() {
    source_2pc_run(|| async move {
        let split = source_2pc_key("2pc-hooks", "m");
        let (_cluster, pd) = source_2pc_store_with_splits(std::slice::from_ref(&split));
        let reached_pre = Arc::new(AtomicBool::new(false));
        let reached_post = Arc::new(AtomicBool::new(false));
        let done = Arc::new(tokio::sync::Notify::new());
        let mut transaction = source_integration_transaction(&pd, false).await;
        let pre = reached_pre.clone();
        let post_pre = reached_pre.clone();
        let post = reached_post.clone();
        let notify = done.clone();
        transaction.set_background_task_lifecycle_hooks(super::LifecycleHooks {
            pre: Some(Arc::new(move || pre.store(true, Ordering::SeqCst))),
            post: Some(Arc::new(move || {
                assert!(post_pre.load(Ordering::SeqCst));
                post.store(true, Ordering::SeqCst);
                notify.notify_one();
            })),
        });
        transaction
            .put(source_2pc_key("2pc-hooks", "a"), b"a".to_vec())
            .await
            .unwrap();
        transaction
            .put(source_2pc_key("2pc-hooks", "z"), b"z".to_vec())
            .await
            .unwrap();
        Box::pin(transaction.commit()).await.unwrap();
        tokio::task::yield_now().await;
        assert!(reached_pre.load(Ordering::SeqCst));
        tokio::time::timeout(Duration::from_secs(1), done.notified())
            .await
            .unwrap();
        assert!(reached_post.load(Ordering::SeqCst));
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_Test2PCCleanupLifecycleHooks() {
    source_2pc_run(|| async move {
        let (_cluster, pd) = source_integration_store();
        let reached_pre = Arc::new(AtomicBool::new(false));
        let reached_post = Arc::new(AtomicBool::new(false));
        let done = Arc::new(tokio::sync::Notify::new());
        let key = source_2pc_key("2pc-cleanup-hooks", "a");
        let mut committer = source_2pc_committer(
            pd,
            1,
            Some(Key::from(key.clone())),
            vec![kvrpcpb::Mutation {
                op: kvrpcpb::Op::Put as i32,
                key,
                value: b"a".to_vec(),
                ..Default::default()
            }],
            TransactionOptions::new_optimistic(),
        );
        let pre = reached_pre.clone();
        let post_pre = reached_pre.clone();
        let post = reached_post.clone();
        let notify = done.clone();
        committer.settings.lifecycle_hooks = super::LifecycleHooks {
            pre: Some(Arc::new(move || pre.store(true, Ordering::SeqCst))),
            post: Some(Arc::new(move || {
                assert!(post_pre.load(Ordering::SeqCst));
                post.store(true, Ordering::SeqCst);
                notify.notify_one();
            })),
        };
        committer.cleanup_without_wait(true);
        tokio::task::yield_now().await;
        assert!(reached_pre.load(Ordering::SeqCst));
        tokio::time::timeout(Duration::from_secs(1), done.notified())
            .await
            .unwrap();
        assert!(reached_post.load(Ordering::SeqCst));
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_2pc_test_TestFailWithUndeterminedResult() {
    source_2pc_run(|| async move {
        let _backoff = SourceAtomicU64Guard::set(&super::PREWRITE_MAX_BACKOFF, 20);
        let scenario = FailScenario::setup();
        crate::util::enable_failpoints();
        let (_cluster, pd) = source_integration_store();
        let key = source_2pc_key("undetermined-result", "key");

        fail::cfg(
            "tikvclient/rpcPrewriteResult",
            "1*return(undeterminedResult)->return()",
        )
        .unwrap();
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction.put(key.clone(), b"value".to_vec()).await.unwrap();
        Box::pin(transaction.commit()).await.unwrap();
        fail::remove("tikvclient/rpcPrewriteResult");

        fail::cfg(
            "tikvclient/rpcCommitResult",
            "1*return(undeterminedResult)->return()",
        )
        .unwrap();
        let mut transaction = source_integration_transaction(&pd, false).await;
        transaction.put(key, b"value".to_vec()).await.unwrap();
        let error = Box::pin(transaction.commit()).await.unwrap_err();
        assert!(crate::error::is_error_undetermined(&error), "{error:?}");
        fail::remove("tikvclient/rpcCommitResult");
        scenario.teardown();
    });
}
