// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Waiter assertions from client-go integration_tests/shared_lock_test.go and
//! lock_test.go require a server wait queue. The mocktikv backend deliberately
//! returns Locked after 5 ms and must never emulate these server transitions.
//! Selected by integration_tests.rs's existing integration-tests feature.

use super::common::pd_addrs;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tikv_client::{LockContext, Timestamp, TimestampExt, Transaction, TransactionClient};

fn assert_write_conflict(result: tikv_client::Result<()>) {
    let error = result.expect_err("normal wake-up must return WriteConflict");
    assert!(
        tikv_client::error::is_write_conflict(&error),
        "expected WriteConflict, got {error:?}"
    );
}

async fn source_real_wait_store() -> (Arc<TransactionClient>, Arc<TransactionClient>) {
    // These cases only create and release their own locks; do not clear a
    // caller's cluster through the broad integration-test init() helper.
    let client = Arc::new(TransactionClient::new(pd_addrs()).await.unwrap());
    (client.clone(), client)
}

async fn source_shared_transaction(client: &Arc<TransactionClient>) -> Transaction {
    client.begin_pessimistic().await.unwrap()
}

async fn source_shared_lock_primary(
    txn: &mut Transaction,
    client: &Arc<TransactionClient>,
    key: &[u8],
) {
    let ts = client.current_timestamp().await.unwrap().version();
    let mut ctx = LockContext::new(ts, 1000, SystemTime::now());
    txn.lock_keys_with_context(&mut ctx, [key.to_vec()])
        .await
        .unwrap();
    let locks = source_shared_locks(client, key, txn.start_timestamp().version()).await;
    let own_lock = locks
        .iter()
        .find(|lock| lock.lock_version == txn.start_timestamp().version())
        .unwrap();
    assert_eq!(own_lock.primary_lock, key);
}

async fn source_shared_lock_key(
    txn: &mut Transaction,
    client: &Arc<TransactionClient>,
    key: &[u8],
) -> tikv_client::Result<()> {
    let ts = client.current_timestamp().await?.version();
    let mut ctx = LockContext::new(ts, 1000, SystemTime::now());
    ctx.in_share_mode = true;
    txn.lock_keys_with_context(&mut ctx, [key.to_vec()]).await
}

async fn source_shared_locks(
    client: &TransactionClient,
    key: &[u8],
    max_ts: u64,
) -> Vec<tikv_client::proto::kvrpcpb::LockInfo> {
    let locks = client
        .scan_locks(&Timestamp::from_version(max_ts), key.to_vec().., 1024)
        .await
        .unwrap();
    locks
        .into_iter()
        .filter(|lock| key.is_empty() || lock.key == key)
        .collect()
}

async fn source_wait_shared_locks(
    client: &TransactionClient,
    key: &[u8],
    max_ts: u64,
    expected: usize,
) -> Vec<tikv_client::proto::kvrpcpb::LockInfo> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let locks = source_shared_locks(client, key, max_ts).await;
            if locks.len() == expected {
                return locks;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("server did not reach the expected lock state")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestSharedLockBlockExclusiveLock() {
    for commit in [true, false] {
        let (cluster, pd) = source_real_wait_store().await;
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
        assert!(second
            .get_mem_buffer_readonly()
            .get_flags_readonly(&key)
            .unwrap()
            .has_locked_in_share_mode());
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
        assert_write_conflict(result);
        exclusive.rollback().await.unwrap();
        assert!(source_shared_locks(&cluster, b"", u64::MAX)
            .await
            .is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestExclusiveLockBlockSharedLock() {
    for commit in [true, false] {
        let (_cluster, pd) = source_real_wait_store().await;
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
        assert_write_conflict(first_result);
        assert_write_conflict(second_result);
        first_shared.rollback().await.unwrap();
        second_shared.rollback().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
#[allow(non_snake_case)]
async fn source_go_integration_tests_shared_lock_test_TestForceLockRetryOnSharedLock() {
    let (cluster, pd) = source_real_wait_store().await;
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
    assert_eq!(locks[0].lock_version, exclusive.start_timestamp().version());
    assert_eq!(
        locks[0].lock_type,
        tikv_client::proto::kvrpcpb::Op::PessimisticLock as i32
    );
    exclusive.rollback().await.unwrap();
}

fn source_deadlock_context(for_update_ts: u64, tag: String) -> LockContext {
    let mut context = LockContext::new(for_update_ts, 1_000, SystemTime::now());
    context.resource_group_tag = tag.into_bytes();
    context
}

fn source_assert_wait_chain_entry(
    entry: &tikv_client::proto::deadlock::WaitForEntry,
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
    async fn prepare(pd: &Arc<TransactionClient>, prefix: &str, count: usize) -> Vec<Transaction> {
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
        transaction: &mut Transaction,
        key: Vec<u8>,
        tag: String,
    ) -> tikv_client::Result<()> {
        let mut context = source_deadlock_context(transaction.start_timestamp().version(), tag);
        transaction
            .lock_keys_with_context(&mut context, [key])
            .await
    }

    let (_cluster, pd) = source_real_wait_store().await;

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
    let tikv_client::Error::Deadlock(deadlock) = error else {
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
    assert_write_conflict(wait_result);
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
    let tikv_client::Error::Deadlock(deadlock) = error else {
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
    assert_write_conflict(result);
    transaction_1.rollback().await.unwrap();
    let (mut transaction_0, result) = wait_0_for_1.await.unwrap();
    assert_write_conflict(result);
    transaction_0.rollback().await.unwrap();
    let (mut transaction_2, result) = wait_2_for_0.await.unwrap();
    assert_write_conflict(result);
    transaction_2.rollback().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn txn_exclusive_rollback_wakeup_distinguishes_normal_and_force_lock() {
    for force in [false, true] {
        let (_, client) = source_real_wait_store().await;
        let suffix = if force { "force" } else { "normal" };
        let key = format!("~lock/exclusive-wakeup/{suffix}/key").into_bytes();
        let mut holder = source_shared_transaction(&client).await;
        source_shared_lock_primary(&mut holder, &client, &key).await;
        let mut waiter = source_shared_transaction(&client).await;
        source_shared_lock_primary(
            &mut waiter,
            &client,
            format!("~lock/exclusive-wakeup/{suffix}/primary").as_bytes(),
        )
        .await;
        if force {
            waiter.start_aggressive_locking();
        }
        let waiting = tokio::spawn(async move {
            let result = waiter.lock_keys_with_wait_time(1000, [key]).await;
            (waiter, result)
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!waiting.is_finished());
        holder.rollback().await.unwrap();
        let (mut waiter, result) = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
        if force {
            result.unwrap();
            waiter.done_aggressive_locking().await.unwrap();
        } else {
            assert_write_conflict(result);
        }
        waiter.rollback().await.unwrap();
    }
}
