// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

// Direct ports of client-go/integration_tests/lock_test.go. The helpers keep
// setup mechanical while every source test retains a separately selectable
// identity and its own behavioral assertions.

struct SourceLockTxn {
    start_ts: u64,
    min_commit_ts: u64,
    commit_ts: u64,
}

async fn source_lock_put(
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    key: Vec<u8>,
    value: Vec<u8>,
) -> (u64, u64) {
    let mut transaction = source_integration_transaction(pd, false).await;
    let start_ts = transaction.start_timestamp().version();
    transaction.put(key, value).await.unwrap();
    let commit_ts = Box::pin(transaction.commit())
        .await
        .unwrap()
        .unwrap()
        .version();
    (start_ts, commit_ts)
}

async fn source_lock_prewrite(
    cluster: &crate::mock::mocktikv::Cluster,
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    mutations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    primary: Vec<u8>,
    ttl: u64,
    async_commit: bool,
    commit_primary: bool,
) -> SourceLockTxn {
    let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
        .await
        .unwrap()
        .version();
    let min_commit_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
        .await
        .unwrap()
        .version();
    let secondaries = mutations
        .iter()
        .map(|(key, _)| key)
        .filter(|key| **key != primary)
        .cloned()
        .collect::<Vec<_>>();
    let request = unistore::PrewriteRequest {
        mutations: mutations
            .into_iter()
            .map(|(key, value)| match value {
                Some(value) => unistore::TxnMutation::put(key, value),
                None => unistore::TxnMutation::delete(key),
            })
            .collect(),
        primary: primary.clone(),
        start_ts,
        ttl,
        min_commit_ts,
        use_async_commit: async_commit,
        secondaries,
        ..Default::default()
    };
    assert!(cluster
        .engine()
        .prewrite(&request)
        .into_iter()
        .all(|error| error.is_none()));
    let commit_ts = if commit_primary {
        let commit_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
            .await
            .unwrap()
            .version()
            .max(min_commit_ts);
        cluster
            .engine()
            .commit(std::slice::from_ref(&primary), start_ts, commit_ts)
            .unwrap();
        commit_ts
    } else {
        0
    };
    SourceLockTxn {
        start_ts,
        min_commit_ts,
        commit_ts,
    }
}

async fn source_lock_seed_alphabet(
    cluster: &crate::mock::mocktikv::Cluster,
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    prefix: &str,
) {
    let mut transaction = source_integration_transaction(pd, false).await;
    for byte in b'a'..=b'z' {
        transaction
            .put(
                format!("{prefix}{}", char::from(byte)).into_bytes(),
                vec![byte],
            )
            .await
            .unwrap();
    }
    Box::pin(transaction.commit()).await.unwrap();
    let end = format!("{prefix}\u{10ffff}").into_bytes();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cluster
                .engine()
                .scan_locks(prefix.as_bytes(), &end, u64::MAX)
                .unwrap()
                .is_empty()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("alphabet seed secondaries did not commit");
}

async fn source_lock_prepare_alphabet(
    cluster: &crate::mock::mocktikv::Cluster,
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    prefix: &str,
) {
    source_lock_seed_alphabet(cluster, pd, prefix).await;
    let key_c = format!("{prefix}c").into_bytes();
    let primary_c = format!("{prefix}z1").into_bytes();
    source_lock_prewrite(
        cluster,
        pd,
        vec![
            (key_c, Some(b"c".to_vec())),
            (primary_c.clone(), Some(b"z1".to_vec())),
        ],
        primary_c,
        3_000,
        false,
        true,
    )
    .await;
    let key_d = format!("{prefix}d").into_bytes();
    let primary_d = format!("{prefix}z2").into_bytes();
    source_lock_prewrite(
        cluster,
        pd,
        vec![
            (key_d, Some(b"dd".to_vec())),
            (primary_d.clone(), Some(b"z2".to_vec())),
        ],
        primary_d,
        3_000,
        false,
        false,
    )
    .await;
}

async fn source_lock_assert_alphabet_reads(mode: u8) {
    let (cluster, pd) = source_integration_store();
    let prefix = format!("~lock/scan-{mode}/");
    source_lock_prepare_alphabet(&cluster, &pd, &prefix).await;
    let mut transaction = source_integration_transaction(&pd, false).await;
    match mode {
        0 => {
            for byte in b'a'..=b'z' {
                let key = format!("{prefix}{}", char::from(byte)).into_bytes();
                assert_eq!(transaction.get(key).await.unwrap(), Some(vec![byte]));
            }
        }
        1 => {
            let start = format!("{prefix}a").into_bytes();
            let end = format!("{prefix}{{").into_bytes();
            let pairs = transaction
                .scan(start..end, 64)
                .await
                .unwrap()
                .collect::<Vec<_>>();
            assert!(pairs.len() >= 26);
            for (index, pair) in pairs.into_iter().take(26).enumerate() {
                let byte = b'a' + index as u8;
                assert_eq!(
                    pair.0,
                    Key::from(format!("{prefix}{}", char::from(byte)).into_bytes())
                );
                assert_eq!(pair.1, vec![byte]);
            }
        }
        2 => {
            transaction.set_snapshot_key_only(true);
            let start = format!("{prefix}a").into_bytes();
            let end = format!("{prefix}{{").into_bytes();
            let keys = transaction
                .scan_keys(start..end, 64)
                .await
                .unwrap()
                .collect::<Vec<_>>();
            assert!(keys.len() >= 26);
            for (index, key) in keys.into_iter().take(26).enumerate() {
                let byte = b'a' + index as u8;
                assert_eq!(
                    key,
                    Key::from(format!("{prefix}{}", char::from(byte)).into_bytes())
                );
            }
        }
        3 => {
            let keys = (b'a'..=b'z')
                .map(|byte| format!("{prefix}{}", char::from(byte)).into_bytes())
                .collect::<Vec<_>>();
            let mut pairs = transaction
                .batch_get(keys)
                .await
                .unwrap()
                .collect::<Vec<_>>();
            pairs.sort_by(|left, right| left.0.cmp(&right.0));
            assert_eq!(pairs.len(), 26);
            for (index, pair) in pairs.into_iter().enumerate() {
                assert_eq!(pair.1, vec![b'a' + index as u8]);
            }
        }
        _ => unreachable!(),
    }
    transaction.rollback().await.unwrap();
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_test_TestScanLockResolveWithGet() {
    source_run_async_on_large_stack("client-go-TestScanLockResolveWithGet", || async {
        source_lock_assert_alphabet_reads(0).await;
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_test_TestScanLockResolveWithSeek() {
    source_run_async_on_large_stack("client-go-TestScanLockResolveWithSeek", || async {
        source_lock_assert_alphabet_reads(1).await;
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_test_TestScanLockResolveWithSeekKeyOnly() {
    source_run_async_on_large_stack("client-go-TestScanLockResolveWithSeekKeyOnly", || async {
        source_lock_assert_alphabet_reads(2).await;
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_test_TestScanLockResolveWithBatchGet() {
    source_run_async_on_large_stack("client-go-TestScanLockResolveWithBatchGet", || async {
        source_lock_assert_alphabet_reads(3).await;
    });
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_test_TestCleanLock() {
    source_run_async_on_large_stack("client-go-TestCleanLock", || async {
        let (cluster, pd) = source_integration_store();
        let prefix = "~lock/clean/";
        for byte in b'a'..=b'z' {
            let key = format!("{prefix}{}", char::from(byte)).into_bytes();
            source_lock_prewrite(
                &cluster,
                &pd,
                vec![(key.clone(), Some(vec![byte]))],
                key,
                0,
                false,
                false,
            )
            .await;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        let mut writer = source_integration_transaction(&pd, false).await;
        for byte in b'a'..=b'z' {
            writer
                .put(
                    format!("{prefix}{}", char::from(byte)).into_bytes(),
                    vec![byte + 1],
                )
                .await
                .unwrap();
        }
        Box::pin(writer.commit()).await.unwrap();
        let mut reader = source_integration_transaction(&pd, false).await;
        for byte in b'a'..=b'z' {
            assert_eq!(
                reader
                    .get(format!("{prefix}{}", char::from(byte)).into_bytes())
                    .await
                    .unwrap(),
                Some(vec![byte + 1])
            );
        }
    });
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestGetTxnStatus() {
    let (cluster, pd) = source_integration_store();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    let committed_key = b"~lock/status/committed".to_vec();
    let (start_ts, commit_ts) =
        source_lock_put(&pd, committed_key.clone(), b"value".to_vec()).await;
    let current_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let status = resolver
        .check_txn_status(
            Arc::clone(&pd),
            Keyspace::Disable,
            None,
            start_ts,
            committed_key,
            start_ts,
            current_ts,
            false,
            false,
            false,
            false,
        )
        .await
        .unwrap();
    assert!(status.is_committed());
    assert_eq!(status.commit_ts(), commit_ts);

    let live_key = b"~lock/status/live".to_vec();
    let live = source_lock_prewrite(
        &cluster,
        &pd,
        vec![(live_key.clone(), Some(b"value".to_vec()))],
        live_key.clone(),
        3_000,
        false,
        false,
    )
    .await;
    let status = resolver
        .check_txn_status(
            Arc::clone(&pd),
            Keyspace::Disable,
            None,
            live.start_ts,
            live_key,
            live.start_ts,
            current_ts,
            false,
            false,
            false,
            false,
        )
        .await
        .unwrap();
    assert!(!status.is_committed());
    assert!(status.ttl() > 0);
    resolver.close().await;
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestCheckTxnStatusTTL() {
    let (cluster, pd) = source_integration_store();
    let key = b"~lock/status-ttl/key".to_vec();
    let lock = source_lock_prewrite(
        &cluster,
        &pd,
        vec![(key.clone(), Some(b"value".to_vec()))],
        key.clone(),
        1_000,
        false,
        false,
    )
    .await;
    let caller_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    let status = resolver
        .check_txn_status(
            Arc::clone(&pd),
            Keyspace::Disable,
            None,
            lock.start_ts,
            key.clone(),
            caller_ts,
            caller_ts,
            false,
            false,
            false,
            false,
        )
        .await
        .unwrap();
    assert!(status.ttl() > 0);
    assert_eq!(status.commit_ts(), 0);
    cluster
        .engine()
        .rollback(std::slice::from_ref(&key), lock.start_ts)
        .unwrap();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    let status = resolver
        .check_txn_status(
            Arc::clone(&pd),
            Keyspace::Disable,
            None,
            lock.start_ts,
            key,
            caller_ts,
            caller_ts,
            false,
            false,
            false,
            false,
        )
        .await
        .unwrap();
    assert!(status.is_rolled_back());
    assert_eq!(status.ttl(), 0);
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestTxnHeartBeat() {
    let (cluster, pd) = source_integration_store();
    let key = b"~lock/heartbeat/key".to_vec();
    let lock = source_lock_prewrite(
        &cluster,
        &pd,
        vec![(key.clone(), Some(b"value".to_vec()))],
        key.clone(),
        3_000,
        false,
        false,
    )
    .await;
    assert_eq!(
        cluster.engine().txn_heartbeat(&key, lock.start_ts, 6_666),
        Ok(6_666)
    );
    assert_eq!(
        cluster.engine().txn_heartbeat(&key, lock.start_ts, 5_555),
        Ok(6_666)
    );
    cluster
        .engine()
        .rollback(std::slice::from_ref(&key), lock.start_ts)
        .unwrap();
    assert!(cluster
        .engine()
        .txn_heartbeat(&key, lock.start_ts, 6_666)
        .is_err());
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestCheckTxnStatus() {
    let (cluster, pd) = source_integration_store();
    let primary = b"~lock/check-status/primary".to_vec();
    let secondary = b"~lock/check-status/secondary".to_vec();
    let lock = source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (primary.clone(), Some(b"value".to_vec())),
            (secondary.clone(), Some(b"second".to_vec())),
        ],
        primary.clone(),
        1_000,
        false,
        false,
    )
    .await;
    let current_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    let status = resolver
        .check_txn_status(
            Arc::clone(&pd),
            Keyspace::Disable,
            None,
            lock.start_ts,
            primary.clone(),
            current_ts,
            current_ts,
            true,
            false,
            false,
            false,
        )
        .await
        .unwrap();
    assert!(status.ttl() > 0);
    assert_eq!(status.action(), kvrpcpb::Action::MinCommitTsPushed);
    cluster
        .engine()
        .rollback(&[primary.clone(), secondary], lock.start_ts)
        .unwrap();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    let status = resolver
        .check_txn_status(
            Arc::clone(&pd),
            Keyspace::Disable,
            None,
            lock.start_ts,
            primary,
            current_ts,
            0,
            true,
            false,
            false,
            false,
        )
        .await
        .unwrap();
    assert!(status.is_rolled_back());
    assert_eq!(status.action(), kvrpcpb::Action::NoAction);
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestCheckTxnStatusNoWait() {
    let (cluster, pd) = source_integration_store();
    let primary = b"~lock/status-no-wait/primary".to_vec();
    let secondary = b"~lock/status-no-wait/secondary".to_vec();
    let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let min_commit_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    assert!(cluster
        .engine()
        .prewrite(&unistore::PrewriteRequest {
            mutations: vec![unistore::TxnMutation::put(
                secondary.clone(),
                b"secondary".to_vec(),
            )],
            primary: primary.clone(),
            start_ts,
            ttl: 100_000,
            min_commit_ts,
            ..Default::default()
        })
        .into_iter()
        .all(|error| error.is_none()));
    let current_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    let missing_primary = resolver
        .check_txn_status(
            Arc::clone(&pd),
            Keyspace::Disable,
            None,
            start_ts,
            primary.clone(),
            current_ts,
            current_ts,
            false,
            false,
            false,
            false,
        )
        .await;
    let missing_primary = missing_primary.unwrap_err();
    assert!(resolver.is_error_not_found(&missing_primary));
    assert!(cluster
        .engine()
        .prewrite(&unistore::PrewriteRequest {
            mutations: vec![unistore::TxnMutation::put(
                primary.clone(),
                b"primary".to_vec(),
            )],
            primary: primary.clone(),
            start_ts,
            ttl: 100_000,
            min_commit_ts,
            ..Default::default()
        })
        .into_iter()
        .all(|error| error.is_none()));
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    let status = resolver
        .check_txn_status(
            Arc::clone(&pd),
            Keyspace::Disable,
            None,
            start_ts,
            primary.clone(),
            current_ts,
            current_ts,
            false,
            false,
            false,
            false,
        )
        .await
        .unwrap();
    assert!(status.ttl() > 0);
    cluster
        .engine()
        .rollback(&[primary.clone(), secondary], start_ts)
        .unwrap();

    let missing_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    let missing = resolver
        .check_txn_status(
            pd,
            Keyspace::Disable,
            None,
            missing_ts,
            b"~lock/status-no-wait/missing".to_vec(),
            current_ts,
            current_ts,
            true,
            false,
            false,
            false,
        )
        .await
        .unwrap();
    assert!(missing.is_rolled_back());
    assert_eq!(missing.action(), kvrpcpb::Action::LockNotExistRollback);
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_test_TestLockTTL() {
    let old_threshold = crate::kv::TXN_COMMIT_BATCH_SIZE.swap(u64::MAX, Ordering::SeqCst);
    let old_managed_ttl = super::MANAGED_LOCK_TTL.swap(20_000, Ordering::SeqCst);
    let rpc = Arc::new(MockPdClient::default());
    let mut committer = source_test_committer(
        rpc,
        Some(Key::from(b"~lock/ttl/key".to_vec())),
        vec![source_test_mutation("~lock/ttl/key", kvrpcpb::Op::Put)],
        TransactionOptions::new_optimistic(),
        CommitSettings::default(),
    );
    committer.write_size = 1024 * 1024;
    assert_eq!(committer.calc_txn_lock_ttl(), super::DEFAULT_LOCK_TTL);
    crate::kv::TXN_COMMIT_BATCH_SIZE.store(1, Ordering::SeqCst);
    assert_eq!(committer.calc_txn_lock_ttl(), 6_000);
    committer.write_size = 16 * 1024 * 1024;
    assert_eq!(committer.calc_txn_lock_ttl(), 20_000);
    crate::kv::TXN_COMMIT_BATCH_SIZE.store(old_threshold, Ordering::SeqCst);
    super::MANAGED_LOCK_TTL.store(old_managed_ttl, Ordering::SeqCst);
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestBatchResolveLocks() {
    let (cluster, pd) = source_integration_store();
    let normal_primary = b"~lock/batch-resolve/k1".to_vec();
    let normal_secondary = b"~lock/batch-resolve/k2".to_vec();
    source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (normal_primary.clone(), Some(b"v1".to_vec())),
            (normal_secondary.clone(), Some(b"v2".to_vec())),
        ],
        normal_primary,
        20_000,
        false,
        false,
    )
    .await;
    let async_primary = b"~lock/batch-resolve/k3".to_vec();
    let async_secondary = b"~lock/batch-resolve/k4".to_vec();
    source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (async_primary.clone(), Some(b"v3".to_vec())),
            (async_secondary.clone(), Some(b"v4".to_vec())),
        ],
        async_primary,
        20_000,
        true,
        false,
    )
    .await;
    let locks = cluster
        .engine()
        .scan_locks(b"~lock/batch-resolve/", b"~lock/batch-resolve0", u64::MAX)
        .unwrap()
        .into_iter()
        .map(source_proto_shared_lock)
        .collect::<Vec<_>>();
    assert_eq!(locks.len(), 4);
    let store = crate::pd::PdClient::store_for_key(
        Arc::clone(&pd),
        &Key::from(b"~lock/batch-resolve/k1".to_vec()),
    )
    .await
    .unwrap();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    resolver
        .cleanup_locks(store, locks, Arc::clone(&pd), Keyspace::Disable, None)
        .await
        .unwrap();
    let mut reader = source_integration_transaction(&pd, false).await;
    assert_eq!(reader.get(normal_secondary).await.unwrap(), None);
    assert_eq!(
        reader.get(async_secondary).await.unwrap(),
        Some(b"v4".to_vec())
    );
    resolver.close().await;
}

#[test]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_test_TestNewLockZeroTTL() {
    let lock = crate::transaction::Lock::from_lock_info(&kvrpcpb::LockInfo::default());
    assert_eq!(lock.ttl, 0);
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestZeroMinCommitTS() {
    let (cluster, pd) = source_integration_store();
    let key = b"~lock/zero-min-commit/key".to_vec();
    let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    assert!(cluster
        .engine()
        .prewrite(&unistore::PrewriteRequest {
            mutations: vec![unistore::TxnMutation::put(key.clone(), b"value".to_vec())],
            primary: key.clone(),
            start_ts,
            ttl: 1_000,
            min_commit_ts: 0,
            ..Default::default()
        })
        .into_iter()
        .all(|error| error.is_none()));
    assert_eq!(
        cluster
            .engine()
            .mvcc_get_by_key(&key)
            .lock
            .as_ref()
            .unwrap()
            .min_commit_ts,
        0
    );
    let (ttl, commit_ts, action) = cluster
        .engine()
        .check_txn_status(&key, start_ts, u64::MAX, start_ts, false, false)
        .unwrap();
    assert!(ttl > 0);
    assert_eq!(commit_ts, 0);
    assert_eq!(action, unistore::Action::MinCommitTsPushed);
    cluster
        .engine()
        .rollback(std::slice::from_ref(&key), start_ts)
        .unwrap();
}

async fn source_lock_prepare_fallback_async_commit(
    cluster: &crate::mock::mocktikv::Cluster,
    pd: &Arc<crate::mock::mocktikv::MockPdClient>,
    prefix: &str,
) -> (Vec<u8>, Vec<u8>, u64) {
    let primary = format!("{prefix}/fb1").into_bytes();
    let secondary = format!("{prefix}/fb2").into_bytes();
    let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
        .await
        .unwrap()
        .version();
    let min_commit_ts = crate::pd::PdClient::get_timestamp(Arc::clone(pd))
        .await
        .unwrap()
        .version();
    assert_eq!(
        cluster.engine().prewrite(&unistore::PrewriteRequest {
            mutations: vec![unistore::TxnMutation::put(primary.clone(), b"1".to_vec(),)],
            primary: primary.clone(),
            start_ts,
            ttl: 0,
            min_commit_ts,
            use_async_commit: true,
            secondaries: vec![secondary.clone()],
            ..Default::default()
        }),
        vec![None]
    );
    assert_eq!(
        cluster.engine().prewrite(&unistore::PrewriteRequest {
            mutations: vec![unistore::TxnMutation::put(secondary.clone(), b"2".to_vec(),)],
            primary: primary.clone(),
            start_ts,
            ttl: 0,
            min_commit_ts,
            use_async_commit: false,
            ..Default::default()
        }),
        vec![None]
    );
    (primary, secondary, start_ts)
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestCheckLocksFallenBackFromAsyncCommit() {
    let (cluster, pd) = source_integration_store();
    let (primary, secondary, start_ts) =
        source_lock_prepare_fallback_async_commit(&cluster, &pd, "~lock/check-fallback").await;
    let primary_lock = cluster.engine().mvcc_get_by_key(&primary).lock.unwrap();
    assert!(primary_lock.use_async_commit);
    assert_eq!(primary_lock.secondaries, vec![secondary.clone()]);
    let secondary_lock = cluster.engine().mvcc_get_by_key(&secondary).lock.unwrap();
    assert!(!secondary_lock.use_async_commit);
    let (locks, commit_ts) = cluster
        .engine()
        .check_secondary_locks(std::slice::from_ref(&secondary), start_ts)
        .unwrap();
    assert_eq!(commit_ts, 0);
    assert_eq!(locks.len(), 1);
    assert!(!locks[0].use_async_commit);
    let (_, _, action) = cluster
        .engine()
        .check_txn_status_with_force_sync(&primary, start_ts, 0, u64::MAX, true, true, false)
        .unwrap();
    assert_eq!(action, unistore::Action::TtlExpireRollback);
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_test_TestResolveTxnFallenBackFromAsyncCommit() {
    source_run_async_on_large_stack(
        "client-go-TestResolveTxnFallenBackFromAsyncCommit",
        || async {
            let (cluster, pd) = source_integration_store();
            let (primary, secondary, _) =
                source_lock_prepare_fallback_async_commit(&cluster, &pd, "~lock/resolve-fallback")
                    .await;
            tokio::time::sleep(Duration::from_millis(2)).await;
            let mut primary_end = primary.clone();
            primary_end.push(0);
            let lock = source_proto_shared_lock(
                cluster
                    .engine()
                    .scan_locks(&primary, &primary_end, u64::MAX)
                    .unwrap()
                    .into_iter()
                    .next()
                    .unwrap(),
            );
            crate::transaction::resolve_locks_with_context(
                vec![lock],
                Timestamp::from_version(0),
                Arc::clone(&pd),
                Keyspace::Disable,
                None,
                ResolveLocksContext::default(),
            )
            .await
            .unwrap();
            assert!(cluster.engine().mvcc_get_by_key(&primary).lock.is_none());
            let mut reader = source_integration_transaction(&pd, false).await;
            assert_eq!(reader.get(primary).await.unwrap(), None);
            assert_eq!(reader.get(secondary).await.unwrap(), None);
        },
    );
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestBatchResolveTxnFallenBackFromAsyncCommit() {
    let (cluster, pd) = source_integration_store();
    let (primary, secondary, _) =
        source_lock_prepare_fallback_async_commit(&cluster, &pd, "~lock/batch-fallback").await;
    let locks = cluster
        .engine()
        .scan_locks(b"~lock/batch-fallback/", b"~lock/batch-fallback0", u64::MAX)
        .unwrap()
        .into_iter()
        .map(source_proto_shared_lock)
        .collect::<Vec<_>>();
    let store = crate::pd::PdClient::store_for_key(Arc::clone(&pd), &Key::from(primary.clone()))
        .await
        .unwrap();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    resolver
        .cleanup_locks(store, locks, Arc::clone(&pd), Keyspace::Disable, None)
        .await
        .unwrap();
    assert!(cluster.engine().mvcc_get_by_key(&primary).lock.is_none());
    assert!(cluster.engine().mvcc_get_by_key(&secondary).lock.is_none());
    let mut reader = source_integration_transaction(&pd, false).await;
    assert_eq!(reader.get(primary).await.unwrap(), None);
    assert_eq!(reader.get(secondary).await.unwrap(), None);
}

fn source_deadlock_context(for_update_ts: u64, tag: String) -> LockContext {
    let mut context = LockContext::new(for_update_ts, 1_000, SystemTime::now());
    context.resource_group_tag = tag.into_bytes();
    context
}

fn source_assert_wait_chain_entry(
    entry: &crate::proto::deadlock::WaitForEntry,
    transaction: u64,
    wait_for_transaction: u64,
    key: &[u8],
    resource_group_tag: &str,
) {
    assert_eq!(entry.txn, transaction);
    assert_eq!(entry.wait_for_txn, wait_for_transaction);
    assert_eq!(entry.key, key);
    assert_eq!(entry.resource_group_tag, resource_group_tag.as_bytes());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestDeadlockReportWaitChain() {
    async fn prepare(
        pd: &Arc<crate::mock::mocktikv::MockPdClient>,
        prefix: &str,
        count: usize,
    ) -> Vec<Transaction<crate::mock::mocktikv::MockPdClient>> {
        let mut transactions = Vec::with_capacity(count);
        for index in 0..count {
            let mut transaction = source_shared_transaction(pd).await;
            let key = format!("{prefix}{index:02}").into_bytes();
            let timestamp = transaction.start_timestamp().version();
            let mut context = source_deadlock_context(timestamp, format!("tag-init{index}"));
            transaction
                .lock_keys_with_context(&mut context, [key])
                .await
                .unwrap();
            transactions.push(transaction);
        }
        transactions
    }

    async fn try_lock(
        transaction: &mut Transaction<crate::mock::mocktikv::MockPdClient>,
        key: Vec<u8>,
        tag: String,
    ) -> crate::Result<()> {
        let mut context =
            source_deadlock_context(transaction.start_timestamp().version(), tag);
        transaction
            .lock_keys_with_context(&mut context, [key])
            .await
    }

    let (_cluster, pd) = source_integration_store();

    let prefix = "~lock/deadlock-chain/two/";
    let mut transactions = prepare(&pd, prefix, 2).await;
    let mut transaction_0 = transactions.remove(0);
    let transaction_0_ts = transaction_0.start_timestamp().version();
    let key_1 = format!("{prefix}{:02}", 1).into_bytes();
    let wait_0_for_1 = tokio::spawn(async move {
        let result = try_lock(&mut transaction_0, key_1, "tag-0-1".to_owned()).await;
        (transaction_0, result)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let transaction_1 = &mut transactions[0];
    let transaction_1_ts = transaction_1.start_timestamp().version();
    let key_0 = format!("{prefix}{:02}", 0).into_bytes();
    let error = try_lock(transaction_1, key_0.clone(), "tag-1-0".to_owned())
        .await
        .unwrap_err();
    let crate::Error::Deadlock(deadlock) = error else {
        panic!("expected deadlock, got {error:?}");
    };
    assert_eq!(deadlock.deadlock.wait_chain.len(), 2);
    source_assert_wait_chain_entry(
        &deadlock.deadlock.wait_chain[0],
        transaction_0_ts,
        transaction_1_ts,
        format!("{prefix}{:02}", 1).as_bytes(),
        "tag-0-1",
    );
    source_assert_wait_chain_entry(
        &deadlock.deadlock.wait_chain[1],
        transaction_1_ts,
        transaction_0_ts,
        &key_0,
        "tag-1-0",
    );
    transaction_1.rollback().await.unwrap();
    let (mut transaction_0, wait_result) = wait_0_for_1.await.unwrap();
    assert!(wait_result.is_err());
    transaction_0.rollback().await.unwrap();

    let prefix = "~lock/deadlock-chain/four/";
    let mut transactions = prepare(&pd, prefix, 4).await;
    let timestamps = transactions
        .iter()
        .map(|transaction| transaction.start_timestamp().version())
        .collect::<Vec<_>>();
    let mut transaction_0 = transactions.remove(0);
    let mut transaction_1 = transactions.remove(0);
    let mut transaction_2 = transactions.remove(0);
    let mut transaction_3 = transactions.remove(0);
    let key = move |index: usize| format!("{prefix}{index:02}").into_bytes();
    let wait_0_for_1 = tokio::spawn(async move {
        let result = try_lock(&mut transaction_0, key(1), "tag-0-1".to_owned()).await;
        (transaction_0, result)
    });
    let key = move |index: usize| format!("{prefix}{index:02}").into_bytes();
    let wait_2_for_0 = tokio::spawn(async move {
        let result = try_lock(&mut transaction_2, key(0), "tag-2-0".to_owned()).await;
        (transaction_2, result)
    });
    let key = move |index: usize| format!("{prefix}{index:02}").into_bytes();
    let wait_1_for_3 = tokio::spawn(async move {
        let result = try_lock(&mut transaction_1, key(3), "tag-1-3".to_owned()).await;
        (transaction_1, result)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let key = |index: usize| format!("{prefix}{index:02}").into_bytes();
    let error = try_lock(&mut transaction_3, key(2), "tag-3-2".to_owned())
        .await
        .unwrap_err();
    let crate::Error::Deadlock(deadlock) = error else {
        panic!("expected deadlock, got {error:?}");
    };
    assert_eq!(deadlock.deadlock.wait_chain.len(), 4);
    for (entry, (transaction, wait_for, key_index, tag)) in
        deadlock.deadlock.wait_chain.iter().zip([
            (timestamps[2], timestamps[0], 0, "tag-2-0"),
            (timestamps[0], timestamps[1], 1, "tag-0-1"),
            (timestamps[1], timestamps[3], 3, "tag-1-3"),
            (timestamps[3], timestamps[2], 2, "tag-3-2"),
        ])
    {
        source_assert_wait_chain_entry(entry, transaction, wait_for, &key(key_index), tag);
    }
    transaction_3.rollback().await.unwrap();
    let (mut transaction_1, result) = wait_1_for_3.await.unwrap();
    assert!(result.is_err());
    transaction_1.rollback().await.unwrap();
    let (mut transaction_0, result) = wait_0_for_1.await.unwrap();
    assert!(result.is_err());
    transaction_0.rollback().await.unwrap();
    let (mut transaction_2, result) = wait_2_for_0.await.unwrap();
    assert!(result.is_err());
    transaction_2.rollback().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestStartHeartBeatAfterLockingPrimary() {
    let old_ttl = super::MANAGED_LOCK_TTL.swap(100, Ordering::SeqCst);
    let (cluster, pd) = source_integration_store();
    let mut transaction = source_integration_transaction_with_options(
        &pd,
        TransactionOptions::new_pessimistic()
            .heartbeat_option(HeartbeatOption::FixedTime(Duration::from_millis(10)))
            .drop_check(CheckLevel::None),
    )
    .await;
    let primary = b"~lock/heartbeat-start/a".to_vec();
    let secondary = b"~lock/heartbeat-start/b".to_vec();
    let for_update_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let mut context = LockContext::new(for_update_ts, 1_000, SystemTime::now());
    transaction
        .lock_keys_with_context(&mut context, [primary.clone(), secondary])
        .await
        .unwrap();
    let initial_ttl = cluster.engine().mvcc_get_by_key(&primary).lock.unwrap().ttl;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let updated_ttl = cluster.engine().mvcc_get_by_key(&primary).lock.unwrap().ttl;
    assert!(updated_ttl > initial_ttl, "{initial_ttl} -> {updated_ttl}");
    transaction.rollback().await.unwrap();
    super::MANAGED_LOCK_TTL.store(old_ttl, Ordering::SeqCst);
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_test_TestPrewriteEncountersLargerTsLock() {
    source_run_async_on_large_stack("client-go-TestPrewriteEncountersLargerTsLock", || async {
        let (cluster, pd) = source_integration_store();
        let key = b"~lock/larger-ts/k2".to_vec();
        let missing_primary = b"~lock/larger-ts/k1".to_vec();
        let mut older = source_integration_transaction(&pd, false).await;
        older.put(key.clone(), b"old".to_vec()).await.unwrap();
        let newer = source_lock_prewrite(
            &cluster,
            &pd,
            vec![(key.clone(), Some(b"new".to_vec()))],
            missing_primary,
            20_000,
            false,
            false,
        )
        .await;
        assert!(newer.start_ts > older.start_timestamp().version());
        let error = Box::pin(older.commit()).await.unwrap_err();
        assert!(crate::error::is_write_conflict(&error), "{error:?}");
        cluster
            .engine()
            .rollback(std::slice::from_ref(&key), newer.start_ts)
            .unwrap();
    });
}

fn source_lock_info(
    cluster: &crate::mock::mocktikv::Cluster,
    key: &[u8],
) -> kvrpcpb::LockInfo {
    let mut end = key.to_vec();
    end.push(0);
    let mut locks = cluster
        .engine()
        .scan_locks(key, &end, u64::MAX)
        .unwrap();
    assert_eq!(locks.len(), 1, "expected one lock for {key:?}");
    source_proto_shared_lock(locks.remove(0))
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestResolveLocksForRead() {
    let (cluster, pd) = source_integration_store();
    let key = |name: &str| format!("~lock/for-read/{name}").into_bytes();
    let mut expected_resolved = Vec::new();
    let mut expected_committed = Vec::new();
    let mut locks = Vec::new();

    let committed = source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (key("k1"), Some(b"v1".to_vec())),
            (key("k11"), Some(b"v11".to_vec())),
        ],
        key("k11"),
        3_000,
        false,
        true,
    )
    .await;
    expected_committed.push(committed.start_ts);
    locks.push(source_lock_info(&cluster, &key("k1")));

    let rolled_back = source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (key("k2"), Some(b"v2".to_vec())),
            (key("k22"), Some(b"v22".to_vec())),
        ],
        key("k22"),
        3_000,
        false,
        false,
    )
    .await;
    cluster
        .engine()
        .rollback(&[key("k22")], rolled_back.start_ts)
        .unwrap();
    expected_resolved.push(rolled_back.start_ts);
    locks.push(source_lock_info(&cluster, &key("k2")));

    let pushed = source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (key("k3"), Some(b"v3".to_vec())),
            (key("k33"), Some(b"v33".to_vec())),
        ],
        key("k33"),
        3_000,
        false,
        false,
    )
    .await;
    expected_resolved.push(pushed.start_ts);
    locks.push(source_lock_info(&cluster, &key("k3")));

    let active_async = source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (key("k4"), Some(b"v4".to_vec())),
            (key("k44"), Some(b"v44".to_vec())),
        ],
        key("k44"),
        3_000,
        true,
        false,
    )
    .await;
    locks.push(source_lock_info(&cluster, &key("k4")));

    let expired_async = source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (key("k5"), Some(b"v5".to_vec())),
            (key("k55"), Some(b"v55".to_vec())),
        ],
        key("k55"),
        1,
        true,
        false,
    )
    .await;
    expected_committed.push(expired_async.start_ts);
    locks.push(source_lock_info(&cluster, &key("k5")));

    let later_commit = source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (key("k6"), Some(b"v6".to_vec())),
            (key("k66"), Some(b"v66".to_vec())),
        ],
        key("k66"),
        3_000,
        false,
        false,
    )
    .await;
    expected_resolved.push(later_commit.start_ts);
    let read_timestamp = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap();
    let later_commit_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    assert!(later_commit_ts > read_timestamp.version());
    cluster
        .engine()
        .commit(&[key("k66")], later_commit.start_ts, later_commit_ts)
        .unwrap();
    locks.push(source_lock_info(&cluster, &key("k6")));

    tokio::time::sleep(Duration::from_millis(20)).await;
    let read_locks = crate::transaction::ReadLockContext::default();
    let result = crate::transaction::resolve_locks_for_read_with_context_result(
        locks,
        read_timestamp,
        Arc::clone(&pd),
        Keyspace::Disable,
        None,
        ResolveLocksContext::default(),
        &read_locks,
    )
    .await
    .unwrap();
    assert!(result.ms_before_expired > 0);
    assert_eq!(result.live_locks.len(), 1);
    assert_eq!(result.live_locks[0].lock_version, active_async.start_ts);
    let (mut resolved, mut committed) = read_locks.snapshot();
    resolved.sort_unstable();
    committed.sort_unstable();
    expected_resolved.sort_unstable();
    expected_committed.sort_unstable();
    assert_eq!(resolved, expected_resolved);
    assert_eq!(committed, expected_committed);
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestBatchLiteResolveLocksByRegion() {
    for for_read in [false, true] {
        let requests = Arc::new(Mutex::new(Vec::<kvrpcpb::ResolveLockRequest>::new()));
        let captured = Arc::clone(&requests);
        let kv = crate::mock::MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
                return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                    commit_version: request.lock_ts + 1_000,
                    ..Default::default()
                }) as Box<dyn Any>);
            }
            let request = request
                .downcast_ref::<kvrpcpb::ResolveLockRequest>()
                .expect("lock resolver should only issue status and resolve requests");
            captured.lock().unwrap().push(request.clone());
            Ok(Box::<kvrpcpb::ResolveLockResponse>::default() as Box<dyn Any>)
        });
        let pd = Arc::new(crate::mock::MockPdClient::with_client_and_regions(
            kv,
            vec![
                crate::mock::MockPdClient::region1(),
                crate::mock::MockPdClient::region2(),
                crate::mock::MockPdClient::region3(),
            ],
        ));
        pd.set_timestamp(Timestamp {
            physical: 100,
            logical: 0,
            ..Default::default()
        });
        let threshold = crate::config::get_global_config()
            .tikv_client
            .resolve_lock_lite_threshold;
        let lock = |transaction: u64, key: Vec<u8>, txn_size: u64| kvrpcpb::LockInfo {
            primary_lock: vec![5],
            lock_version: transaction,
            key,
            lock_ttl: 3_000,
            txn_size,
            lock_type: kvrpcpb::Op::Put as i32,
            ..Default::default()
        };
        let locks = vec![
            lock(101, vec![1], threshold - 1),
            lock(101, vec![2], threshold - 1),
            lock(101, vec![11], threshold - 1),
            lock(102, vec![12], threshold - 1),
            lock(103, vec![13], threshold + 1),
        ];
        let context = ResolveLocksContext::default();
        let context_owner = context.clone();
        if for_read {
            let read_locks = crate::transaction::ReadLockContext::default();
            crate::transaction::resolve_locks_for_read_with_context_result(
                locks,
                Timestamp::from_version(5_000),
                pd,
                Keyspace::Disable,
                None,
                context,
                &read_locks,
            )
            .await
            .unwrap();
            let mut committed = read_locks.snapshot().1;
            committed.sort_unstable();
            assert_eq!(committed, [101, 102, 103]);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if requests.lock().unwrap().len() >= 4 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("detached read cleanup did not dispatch all region requests");
            context_owner.close().await;
        } else {
            crate::transaction::resolve_locks_with_context_result(
                locks,
                Timestamp::default(),
                pd,
                Keyspace::Disable,
                None,
                context,
            )
            .await
            .unwrap();
        }

        let requests = requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 4, "for_read={for_read}: {requests:?}");
        let by_transaction = requests.into_iter().fold(
            HashMap::<u64, Vec<kvrpcpb::ResolveLockRequest>>::new(),
            |mut grouped, request| {
                grouped
                    .entry(request.start_version)
                    .or_default()
                    .push(request);
                grouped
            },
        );
        let multi = &by_transaction[&101];
        assert_eq!(multi.len(), 2);
        let mut multi_keys = multi
            .iter()
            .flat_map(|request| request.keys.clone())
            .collect::<Vec<_>>();
        multi_keys.sort();
        assert_eq!(multi_keys, [vec![1], vec![2], vec![11]]);
        assert!(multi.iter().all(|request| !request.is_async));
        assert_eq!(by_transaction[&102][0].keys, [vec![12]]);
        assert!(by_transaction[&103][0].keys.is_empty());
    }
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestBatchLiteResolveLocksForReadIgnoresInlineCleanupError(
) {
    let resolve_attempts = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::clone(&resolve_attempts);
    let pd = Arc::new(crate::mock::MockPdClient::new(
        crate::mock::MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
            if request.is::<kvrpcpb::CheckTxnStatusRequest>() {
                return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                    commit_version: 200,
                    ..Default::default()
                }) as Box<dyn Any>);
            }
            assert!(request.is::<kvrpcpb::ResolveLockRequest>());
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(crate::Error::StringError(
                "injected inline cleanup timeout".to_owned(),
            ))
        }),
    ));
    pd.set_timestamp(Timestamp {
        physical: 100,
        logical: 0,
        ..Default::default()
    });
    let transaction_id = 100;
    let lock = kvrpcpb::LockInfo {
        primary_lock: b"~lock/inline-error/primary".to_vec(),
        lock_version: transaction_id,
        key: b"~lock/inline-error/secondary".to_vec(),
        lock_ttl: 3_000,
        txn_size: 1,
        lock_type: kvrpcpb::Op::Put as i32,
        ..Default::default()
    };
    let mut context = ResolveLocksContext::default();
    context.set_async_resolve_pool_size(0);
    let read_locks = crate::transaction::ReadLockContext::default();
    let result = crate::transaction::resolve_locks_for_read_with_context_result(
        vec![lock],
        Timestamp::from_version(300),
        pd,
        Keyspace::Disable,
        None,
        context,
        &read_locks,
    )
    .await
    .unwrap();
    assert!(result.live_locks.is_empty());
    assert_eq!(read_locks.snapshot().1, [transaction_id]);
    assert_eq!(resolve_attempts.load(Ordering::SeqCst), 1);
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_with_tikv_test_TestPrewriteCheckForUpdateTS() {
    source_run_async_on_large_stack("client-go-TestPrewriteCheckForUpdateTS", || async {
        for (async_commit, one_pc, causal_consistency) in [
            (false, false, false),
            (true, false, false),
            (true, true, false),
            (true, false, true),
        ] {
            let (cluster, pd) = source_integration_store();
            let suffix = format!("{async_commit}-{one_pc}-{causal_consistency}");
            let failed_key = format!("~lock/prewrite-update/fail/{suffix}").into_bytes();
            let mut options = TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None);
            if async_commit {
                options = options.use_async_commit();
            }
            if one_pc {
                options = options.try_one_pc();
            }
            let mut transaction = source_integration_transaction_with_options(&pd, options).await;
            transaction.set_causal_consistency(causal_consistency);
            transaction.start_aggressive_locking();
            transaction
                .lock_keys([failed_key.clone()])
                .await
                .unwrap();
            transaction.done_aggressive_locking().await.unwrap();
            transaction
                .put(failed_key.clone(), b"v1".to_vec())
                .await
                .unwrap();
            let start_ts = transaction.start_timestamp().version();
            let expected_for_update_ts = cluster
                .engine()
                .mvcc_get_by_key(&failed_key)
                .lock
                .unwrap()
                .for_update_ts;
            cluster
                .engine()
                .pessimistic_rollback(
                    b"",
                    b"",
                    std::slice::from_ref(&failed_key),
                    start_ts,
                    u64::MAX,
                );
            let (_, conflicting_commit_ts) =
                source_lock_put(&pd, failed_key.clone(), b"v2".to_vec()).await;
            assert!(conflicting_commit_ts > expected_for_update_ts);
            let (errors, _) = cluster.engine().pessimistic_lock(&unistore::PessimisticLockRequest {
                mutations: vec![unistore::TxnMutation {
                    op: unistore::Op::PessimisticLock,
                    key: failed_key.clone(),
                    value: Vec::new(),
                    assertion: unistore::Assertion::None,
                }],
                primary: failed_key.clone(),
                start_ts,
                for_update_ts: conflicting_commit_ts,
                ttl: 3_000,
                min_commit_ts: conflicting_commit_ts.saturating_add(1),
                wait_timeout: 0,
                wake_up_mode: unistore::PessimisticWakeUpMode::ForceLock,
                ..Default::default()
            });
            assert_eq!(errors, [None]);
            let error = Box::pin(transaction.commit()).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("pessimistic lock not found"),
                "{error:?}"
            );
            assert_eq!(
                source_engine_value(&cluster, &failed_key, u64::MAX)
                    .unwrap()
                    .0,
                b"v2"
            );

            let passing_key = format!("~lock/prewrite-update/pass/{suffix}").into_bytes();
            let mut options = TransactionOptions::new_pessimistic()
                .heartbeat_option(HeartbeatOption::NoHeartbeat)
                .drop_check(CheckLevel::None);
            if async_commit {
                options = options.use_async_commit();
            }
            if one_pc {
                options = options.try_one_pc();
            }
            let mut passing = source_integration_transaction_with_options(&pd, options).await;
            passing.set_causal_consistency(causal_consistency);
            passing.start_aggressive_locking();
            passing.lock_keys([passing_key.clone()]).await.unwrap();
            passing.done_aggressive_locking().await.unwrap();
            passing
                .put(passing_key.clone(), b"v1".to_vec())
                .await
                .unwrap();
            Box::pin(passing.commit()).await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if source_engine_value(&cluster, &passing_key, u64::MAX).is_some() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "passing async commit did not finish for {suffix}: {:?}",
                    cluster.engine().mvcc_get_by_key(&passing_key)
                )
            });
            let value = source_engine_value(&cluster, &passing_key, u64::MAX).unwrap_or_else(|| {
                panic!(
                    "missing passing value for {suffix}: {:?}",
                    cluster.engine().mvcc_get_by_key(&passing_key)
                )
            });
            assert_eq!(value.0, b"v1", "{suffix}");
        }
    });
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_with_tikv_test_TestCheckTxnStatusSentToSecondary() {
    let (cluster, pd) = source_integration_store();
    let key1 = b"~lock/status-secondary/k1".to_vec();
    let key2 = b"~lock/status-secondary/k2".to_vec();
    let key3 = b"~lock/status-secondary/k3".to_vec();
    let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let for_update_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let lock_request = |keys: Vec<Vec<u8>>, primary: Vec<u8>| {
        unistore::PessimisticLockRequest {
            mutations: keys
                .into_iter()
                .map(|key| unistore::TxnMutation {
                    op: unistore::Op::PessimisticLock,
                    key,
                    value: Vec::new(),
                    assertion: unistore::Assertion::None,
                })
                .collect(),
            primary,
            start_ts,
            for_update_ts,
            ttl: 3_000,
            min_commit_ts: for_update_ts.saturating_add(1),
            wait_timeout: 0,
            ..Default::default()
        }
    };
    assert!(cluster
        .engine()
        .pessimistic_lock(&lock_request(
            vec![key1.clone(), key2.clone()],
            key1.clone(),
        ))
        .0
        .into_iter()
        .all(|error| error.is_none()));
    assert!(cluster
        .engine()
        .pessimistic_lock(&lock_request(vec![key3.clone()], key3.clone()))
        .0
        .into_iter()
        .all(|error| error.is_none()));
    assert!(cluster
        .engine()
        .prewrite(&unistore::PrewriteRequest {
            mutations: vec![
                unistore::TxnMutation::put(key1.clone(), b"v1-1".to_vec()),
                unistore::TxnMutation::put(key3.clone(), b"v3-1".to_vec()),
            ],
            primary: key3.clone(),
            start_ts,
            ttl: 3_000,
            for_update_ts,
            min_commit_ts: for_update_ts.saturating_add(1),
            pessimistic_actions: vec![
                unistore::PessimisticAction::DoCheck,
                unistore::PessimisticAction::DoCheck,
            ],
            ..Default::default()
        })
        .into_iter()
        .all(|error| error.is_none()));
    let commit_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    cluster
        .engine()
        .commit(std::slice::from_ref(&key3), start_ts, commit_ts)
        .unwrap();

    let stale_secondary = source_lock_info(&cluster, &key2);
    assert_eq!(stale_secondary.primary_lock, key1);
    let result = crate::transaction::resolve_locks_with_context_result(
        vec![stale_secondary],
        Timestamp::from_version(commit_ts.saturating_add(1)),
        Arc::clone(&pd),
        Keyspace::Disable,
        None,
        ResolveLocksContext::default(),
    )
    .await
    .unwrap();
    assert!(result.live_locks.is_empty());
    assert!(cluster.engine().mvcc_get_by_key(&key2).lock.is_none());

    let mut reader = source_integration_transaction(&pd, false).await;
    assert_eq!(reader.get(key3).await.unwrap(), Some(b"v3-1".to_vec()));
    assert_eq!(reader.get(key2).await.unwrap(), None);
    assert_eq!(reader.get(key1).await.unwrap(), Some(b"v1-1".to_vec()));
}

#[tokio::test]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_with_tikv_test_TestBatchResolveLocks() {
    let (cluster, pd) = source_integration_store();
    let key1 = b"~lock/tikv-batch/k1".to_vec();
    let key2 = b"~lock/tikv-batch/k2".to_vec();
    let key3 = b"~lock/tikv-batch/k3".to_vec();
    let key4 = b"~lock/tikv-batch/k4".to_vec();
    for key in [&key1, &key4] {
        let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
            .await
            .unwrap()
            .version();
        assert_eq!(
            cluster
                .engine()
                .pessimistic_lock(&unistore::PessimisticLockRequest {
                    mutations: vec![unistore::TxnMutation {
                        op: unistore::Op::PessimisticLock,
                        key: key.clone(),
                        value: Vec::new(),
                        assertion: unistore::Assertion::None,
                    }],
                    primary: key.clone(),
                    start_ts,
                    for_update_ts: start_ts,
                    ttl: 10,
                    min_commit_ts: start_ts.saturating_add(1),
                    wait_timeout: 0,
                    ..Default::default()
                })
                .0,
            [None]
        );
    }
    let committed = source_lock_prewrite(
        &cluster,
        &pd,
        vec![
            (key2.clone(), Some(b"v2".to_vec())),
            (key3.clone(), Some(b"v3".to_vec())),
        ],
        key2.clone(),
        3_000,
        false,
        true,
    )
    .await;
    assert!(committed.commit_ts > committed.start_ts);

    let locks = cluster
        .engine()
        .scan_locks(b"~lock/tikv-batch/", b"~lock/tikv-batch0", u64::MAX)
        .unwrap()
        .into_iter()
        .map(source_proto_shared_lock)
        .collect::<Vec<_>>();
    assert_eq!(locks.len(), 3);
    assert_eq!(
        locks
            .iter()
            .map(|lock| kvrpcpb::Op::try_from(lock.lock_type).unwrap())
            .collect::<Vec<_>>(),
        [
            kvrpcpb::Op::PessimisticLock,
            kvrpcpb::Op::Put,
            kvrpcpb::Op::PessimisticLock,
        ]
    );
    let store = crate::pd::PdClient::store_for_key(Arc::clone(&pd), &Key::from(key1.clone()))
        .await
        .unwrap();
    let mut resolver = crate::transaction::LockResolver::new(ResolveLocksContext::default());
    resolver
        .cleanup_locks(store, locks, Arc::clone(&pd), Keyspace::Disable, None)
        .await
        .unwrap();
    assert!(cluster
        .engine()
        .scan_locks(b"~lock/tikv-batch/", b"~lock/tikv-batch0", u64::MAX)
        .unwrap()
        .is_empty());
    let mut reader = source_integration_transaction(&pd, false).await;
    assert_eq!(reader.get(key1).await.unwrap(), None);
    assert_eq!(reader.get(key2).await.unwrap(), Some(b"v2".to_vec()));
    assert_eq!(reader.get(key3).await.unwrap(), Some(b"v3".to_vec()));
    assert_eq!(reader.get(key4).await.unwrap(), None);
    resolver.close().await;
}

#[test]
#[serial_test::serial]
#[allow(non_snake_case)]
fn source_go_integration_tests_lock_with_tikv_test_TestPessimisticRollbackWithRead() {
    source_run_async_on_large_stack("client-go-TestPessimisticRollbackWithRead", || async {
        for (case, pessimistic_count, prewrite_count) in [("basic", 3, 0), ("large", 500, 500)] {
            let (cluster, pd) = source_integration_store();
            let prefix = format!("~lock/pess-region/{case}/");
            let start_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
                .await
                .unwrap()
                .version();
            let pessimistic_keys = (0..pessimistic_count)
                .map(|index| format!("{prefix}p{index:04}").into_bytes())
                .collect::<Vec<_>>();
            let (errors, _) = cluster.engine().pessimistic_lock(&unistore::PessimisticLockRequest {
                mutations: pessimistic_keys
                    .iter()
                    .cloned()
                    .map(|key| unistore::TxnMutation {
                        op: unistore::Op::PessimisticLock,
                        key,
                        value: Vec::new(),
                        assertion: unistore::Assertion::None,
                    })
                    .collect(),
                primary: pessimistic_keys[0].clone(),
                start_ts,
                for_update_ts: start_ts,
                ttl: 10,
                min_commit_ts: start_ts.saturating_add(1),
                wait_timeout: 0,
                ..Default::default()
            });
            assert!(errors.into_iter().all(|error| error.is_none()));

            let prewrite_keys = (0..prewrite_count)
                .map(|index| format!("{prefix}w{index:04}").into_bytes())
                .collect::<Vec<_>>();
            if !prewrite_keys.is_empty() {
                assert!(cluster
                    .engine()
                    .prewrite(&unistore::PrewriteRequest {
                        mutations: prewrite_keys
                            .iter()
                            .cloned()
                            .map(|key| unistore::TxnMutation::put(key, b"value".to_vec()))
                            .collect(),
                        primary: prewrite_keys[0].clone(),
                        start_ts,
                        ttl: 10,
                        min_commit_ts: start_ts.saturating_add(1),
                        ..Default::default()
                    })
                    .into_iter()
                    .all(|error| error.is_none()));
            }

            let mut context = ResolveLocksContext::default();
            context.pessimistic_region_resolve = true;
            let result = crate::transaction::resolve_locks_with_context_result(
                vec![kvrpcpb::LockInfo {
                    key: pessimistic_keys[1].clone(),
                    primary_lock: pessimistic_keys[0].clone(),
                    lock_version: start_ts,
                    lock_ttl: 0,
                    lock_type: kvrpcpb::Op::PessimisticLock as i32,
                    lock_for_update_ts: start_ts,
                    ..Default::default()
                }],
                Timestamp::from_version(start_ts.saturating_add(1)),
                Arc::clone(&pd),
                Keyspace::Disable,
                None,
                context,
            )
            .await
            .unwrap();
            assert!(result.live_locks.is_empty());
            let mut end = prefix.as_bytes().to_vec();
            end.push(0xff);
            let locks = cluster
                .engine()
                .scan_locks(prefix.as_bytes(), &end, u64::MAX)
                .unwrap();
            assert_eq!(locks.len(), prewrite_count, "{case}");
            assert!(locks.iter().all(|lock| lock.lock_type == unistore::Op::Put));
            if !prewrite_keys.is_empty() {
                cluster
                    .engine()
                    .rollback(&prewrite_keys, start_ts)
                    .unwrap();
            }
        }
    });
}

fn source_assert_max_execution_error(error: &crate::Error) {
    assert!(matches!(
        error,
        crate::Error::QueryInterruptedWithSignal(
            crate::error::QueryInterruptedWithSignalError { signal: 2 }
        )
    ), "{error:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_with_tikv_test_TestPessimisticLockMaxExecutionTime() {
    let (_cluster, pd) = source_integration_store();
    let key1 = b"~lock/max-execution/k1".to_vec();
    let key2 = b"~lock/max-execution/k2".to_vec();
    let mut holder = source_shared_transaction(&pd).await;
    let holder_ts = holder.start_timestamp().version();
    let mut holder_context =
        LockContext::new(holder_ts, crate::kv::LOCK_ALWAYS_WAIT, SystemTime::now());
    holder
        .lock_keys_with_context(&mut holder_context, [key1.clone()])
        .await
        .unwrap();

    let mut expired = source_shared_transaction(&pd).await;
    let started = SystemTime::now();
    let mut context = LockContext::new(expired.start_timestamp().version(), 800, started);
    context.max_execution_deadline = Some(started - Duration::from_millis(100));
    let error = expired
        .lock_keys_with_context(&mut context, [key1.clone()])
        .await
        .unwrap_err();
    source_assert_max_execution_error(&error);
    assert!(started.elapsed().unwrap() < Duration::from_millis(100));

    let mut execution_limited = source_shared_transaction(&pd).await;
    let started = SystemTime::now();
    let mut context =
        LockContext::new(execution_limited.start_timestamp().version(), 800, started);
    context.max_execution_deadline = Some(started + Duration::from_millis(200));
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        execution_limited.lock_keys_with_context(&mut context, [key1.clone()]),
    )
        .await
        .expect("max execution deadline must bound lock acquisition")
        .unwrap_err();
    source_assert_max_execution_error(&error);
    let elapsed = started.elapsed().unwrap();
    assert!(elapsed >= Duration::from_millis(180), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(450), "{elapsed:?}");

    let mut lock_limited = source_shared_transaction(&pd).await;
    let started = SystemTime::now();
    let mut context = LockContext::new(lock_limited.start_timestamp().version(), 150, started);
    context.max_execution_deadline = Some(started + Duration::from_millis(600));
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        lock_limited.lock_keys_with_context(&mut context, [key1.clone()]),
    )
        .await
        .expect("lock wait timeout must bound lock acquisition")
        .unwrap_err();
    assert!(crate::error::is_lock_wait_timeout(&error), "{error:?}");
    let elapsed = started.elapsed().unwrap();
    assert!(elapsed >= Duration::from_millis(120), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(350), "{elapsed:?}");

    let mut tikv_limited = source_shared_transaction(&pd).await;
    let started = SystemTime::now();
    let mut context = LockContext::new(tikv_limited.start_timestamp().version(), 900, started);
    context.max_execution_deadline = Some(started + Duration::from_millis(1_200));
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        tikv_limited.lock_keys_with_context(&mut context, [key1.clone()]),
    )
        .await
        .expect("TiKV lock wait must finish before the execution deadline")
        .unwrap_err();
    assert!(crate::error::is_lock_wait_timeout(&error), "{error:?}");
    let elapsed = started.elapsed().unwrap();
    assert!(elapsed >= Duration::from_millis(800), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(1_250), "{elapsed:?}");

    let mut no_wait = source_shared_transaction(&pd).await;
    let mut context = LockContext::new(
        no_wait.start_timestamp().version(),
        crate::kv::LOCK_NO_WAIT,
        SystemTime::now(),
    );
    let error = no_wait
        .lock_keys_with_context(&mut context, [key1.clone()])
        .await
        .unwrap_err();
    assert!(
        crate::error::is_lock_acquire_fail_and_no_wait_set(&error),
        "{error:?}"
    );

    holder.rollback().await.unwrap();
    let mut succeeds = source_shared_transaction(&pd).await;
    let started = SystemTime::now();
    let mut context = LockContext::new(
        succeeds.start_timestamp().version(),
        crate::kv::LOCK_ALWAYS_WAIT,
        started,
    );
    context.max_execution_deadline = Some(started + Duration::from_millis(100));
    succeeds
        .lock_keys_with_context(&mut context, [key2])
        .await
        .unwrap();

    expired.rollback().await.unwrap();
    execution_limited.rollback().await.unwrap();
    lock_limited.rollback().await.unwrap();
    tikv_limited.rollback().await.unwrap();
    no_wait.rollback().await.unwrap();
    succeeds.rollback().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestResolveLockWithTiKVSideAsync() {
    for commit_primary in [true, false] {
        let (cluster, pd) = source_integration_store();
        let case = if commit_primary { "commit" } else { "rollback" };
        let prefix = format!("~lock/tikv-async/{case}/");
        let keys = (0..20)
            .map(|index| format!("{prefix}{index:03}").into_bytes())
            .collect::<Vec<_>>();
        let values = (0..20)
            .map(|index| format!("value-{case}-{index:03}").into_bytes())
            .collect::<Vec<_>>();
        let transaction = source_lock_prewrite(
            &cluster,
            &pd,
            keys.iter()
                .cloned()
                .zip(values.iter().cloned().map(Some))
                .collect(),
            keys[0].clone(),
            if commit_primary { 3_000 } else { 1 },
            false,
            commit_primary,
        )
        .await;
        if !commit_primary {
            cluster
                .engine()
                .rollback(std::slice::from_ref(&keys[0]), transaction.start_ts)
                .unwrap();
        }
        let mut end = prefix.as_bytes().to_vec();
        end.push(0xff);
        let threshold = crate::config::get_global_config()
            .tikv_client
            .resolve_lock_lite_threshold;
        let locks = cluster
            .engine()
            .scan_locks(prefix.as_bytes(), &end, u64::MAX)
            .unwrap()
            .into_iter()
            .map(source_proto_shared_lock)
            .map(|mut lock| {
                lock.txn_size = threshold.saturating_add(1);
                lock
            })
            .collect::<Vec<_>>();
        assert_eq!(locks.len(), keys.len() - 1);

        let requests = Arc::new(Mutex::new(Vec::<kvrpcpb::ResolveLockRequest>::new()));
        let captured = Arc::clone(&requests);
        let interceptor = crate::new_rpc_interceptor(
            format!("capture-tikv-async-{case}"),
            move |_, request, next| {
                if let Some(request) = request
                    .as_any()
                    .downcast_ref::<kvrpcpb::ResolveLockRequest>()
                {
                    captured.lock().unwrap().push(request.clone());
                }
                Box::pin(async move { next().await })
            },
        );
        let mut interceptor_chain = crate::RpcInterceptorChain::new();
        interceptor_chain.link(interceptor);
        let mut context = ResolveLocksContext::default();
        context.rpc_interceptor = Some(interceptor_chain);
        let context_owner = context.clone();
        let read_locks = crate::transaction::ReadLockContext::default();
        let read_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
            .await
            .unwrap();
        let result = crate::transaction::resolve_locks_for_read_with_context_result(
            locks,
            read_ts,
            Arc::clone(&pd),
            Keyspace::Disable,
            None,
            context,
            &read_locks,
        )
        .await
        .unwrap();
        assert!(result.live_locks.is_empty());
        if commit_primary {
            assert_eq!(read_locks.snapshot().1, [transaction.start_ts]);
        } else {
            assert_eq!(read_locks.snapshot().0, [transaction.start_ts]);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if cluster
                    .engine()
                    .scan_locks(prefix.as_bytes(), &end, u64::MAX)
                    .unwrap()
                    .is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("TiKV-side read cleanup did not clear the region");
        context_owner.close().await;

        let requests = requests.lock().unwrap();
        assert!(!requests.is_empty());
        assert!(requests.iter().all(|request| request.keys.is_empty()));
        assert!(requests
            .iter()
            .all(|request| request.commit_version == transaction.commit_ts));
        assert!(requests
            .iter()
            .all(|request| request.is_async == crate::config::NEXT_GEN));
        drop(requests);
        for (key, value) in keys.iter().zip(&values) {
            let actual = source_engine_value(&cluster, key, u64::MAX).map(|value| value.0);
            if commit_primary {
                assert_eq!(actual.as_deref(), Some(value.as_slice()));
            } else {
                assert_eq!(actual, None);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_lock_test_TestLockWaitTimeLimit() {
    let (_cluster, pd) = source_integration_store();
    let key1 = b"~lock/wait-limit/k1".to_vec();
    let key2 = b"~lock/wait-limit/k2".to_vec();
    let mut holder = source_shared_transaction(&pd).await;
    let holder_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let mut holder_context = LockContext::new(holder_ts, 1_000, SystemTime::now());
    holder
        .lock_keys_with_context(&mut holder_context, [key1.clone(), key2.clone()])
        .await
        .unwrap();

    let mut waiter = source_shared_transaction(&pd).await;
    let no_wait_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let no_wait_started = SystemTime::now();
    let mut no_wait = LockContext::new(no_wait_ts, crate::kv::LOCK_NO_WAIT, no_wait_started);
    let error = waiter
        .lock_keys_with_context(&mut no_wait, [key1])
        .await
        .unwrap_err();
    assert!(
        crate::error::is_lock_acquire_fail_and_no_wait_set(&error),
        "{error:?}"
    );
    assert!(no_wait_started.elapsed().unwrap() < Duration::from_millis(500));

    let wait_ts = crate::pd::PdClient::get_timestamp(Arc::clone(&pd))
        .await
        .unwrap()
        .version();
    let wait_started = SystemTime::now();
    let mut limited = LockContext::new(wait_ts, 200, wait_started);
    let error = waiter
        .lock_keys_with_context(&mut limited, [key2])
        .await
        .unwrap_err();
    assert!(crate::error::is_lock_wait_timeout(&error));
    let elapsed = wait_started.elapsed().unwrap();
    assert!(elapsed >= Duration::from_millis(180), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(800), "{elapsed:?}");
    holder.rollback().await.unwrap();
    waiter.rollback().await.unwrap();
}
