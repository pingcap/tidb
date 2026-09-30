// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Ordinary downstream-crate gate for client-go's injected client path.

use std::any::Any;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use tikv_client::proto::{keyspacepb, metapb};
use tikv_client::tikv::{
    Client as KvClient, Keyspace, RegionId, RegionStore, RegionVerId, RegionWithLeader, Request,
    Store, StoreId,
};
use tikv_client::{
    Error, Key, PdClient, RawClient, Result, Timestamp, TimestampExt, Transaction,
    TransactionOptions,
};

#[derive(Clone)]
struct InProcessKvClient;

#[async_trait]
impl KvClient for InProcessKvClient {
    async fn dispatch(&self, _request: &dyn Request) -> Result<Box<dyn Any>> {
        Err(Error::StringError("no request expected".to_owned()))
    }
}

struct InProcessPdClient;

fn no_region() -> Error {
    Error::StringError("no region expected".to_owned())
}

#[async_trait]
impl PdClient for InProcessPdClient {
    type KvClient = InProcessKvClient;

    async fn map_region_to_store(
        self: Arc<Self>,
        _region: RegionWithLeader,
    ) -> Result<RegionStore> {
        Err(Error::StringError("no route expected".to_owned()))
    }

    async fn region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
        Err(no_region())
    }

    async fn region_for_id(&self, _id: RegionId) -> Result<RegionWithLeader> {
        Err(no_region())
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(Timestamp::from_version(42))
    }

    async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
        Ok(true)
    }

    async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        Err(Error::StringError("no keyspace expected".to_owned()))
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(Vec::new())
    }

    async fn update_leader(&self, _ver_id: RegionVerId, _leader: metapb::Peer) -> Result<()> {
        Ok(())
    }

    async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {}

    async fn invalidate_store_cache(&self, _store_id: StoreId) {}
}

#[test]
fn ordinary_downstream_build_can_construct_an_injected_transaction() {
    let transaction = Transaction::new(
        Timestamp::from_version(42),
        Arc::new(InProcessPdClient),
        TransactionOptions::new_optimistic().read_only(),
        Keyspace::Disable,
    );

    assert_eq!(transaction.start_timestamp().version(), 42);
}

#[test]
fn ordinary_downstream_build_can_name_transaction_test_controls() {
    assert_eq!(
        tikv_client::transaction::PESSIMISTIC_LOCK_MAX_BACKOFF,
        20_000
    );
    assert_eq!(tikv_client::transaction::DEFAULT_LOCK_TTL, 3_000);
    assert_eq!(tikv_client::transaction::TTL_FACTOR, 6_000.0);
    assert_eq!(tikv_client::transaction::RESOLVED_CACHE_SIZE, 2_048);
    assert_eq!(
        tikv_client::transaction::ASYNC_RESOLVE_LOCK_SEMAPHORE_LIMIT,
        10_000
    );
    assert_eq!(
        tikv_client::transaction::PRE_SPLIT_DETECT_THRESHOLD.load(Ordering::Relaxed),
        100_000
    );
    assert_eq!(
        tikv_client::transaction::PRE_SPLIT_SIZE_THRESHOLD.load(Ordering::Relaxed),
        32 << 20
    );
}

#[test]
fn ordinary_downstream_build_can_construct_an_injected_raw_client() {
    let pd = Arc::new(InProcessPdClient);
    let client = RawClient::new_with_pd_client(pd.clone(), 17, Keyspace::Disable, None);

    assert_eq!(client.cluster_id(), 17);
    assert!(Arc::ptr_eq(&client.pd_client(), &pd));
    assert_eq!(tikv_client::raw::RAW_BATCH_PUT_SIZE, 16 * 1024);
}

#[test]
fn synchronous_injected_transaction_uses_the_native_buffer_and_runtime_guard() {
    let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let native = Transaction::new(
        Timestamp::from_version(42),
        Arc::new(InProcessPdClient),
        TransactionOptions::new_optimistic(),
        Keyspace::Disable,
    );
    let mut transaction = tikv_client::SyncTransaction::new(native, runtime.clone());
    let checkpoint = transaction.get_mem_buffer().staging();
    transaction
        .get_mem_buffer()
        .set(b"staged", b"value")
        .unwrap();
    assert_eq!(transaction.inner().get_mem_buffer_readonly().len(), 1);
    assert_eq!(
        transaction.get(b"staged".to_vec()).unwrap(),
        Some(b"value".to_vec())
    );
    transaction.inner_mut().get_mem_buffer().cleanup(checkpoint);
    assert_eq!(transaction.inner().get_mem_buffer_readonly().len(), 0);

    runtime.block_on(async {
        assert!(matches!(
            transaction.block_on(|inner| inner.put(b"nested".to_vec(), b"bad".to_vec())),
            Err(Error::NestedRuntimeError(_))
        ));
    });
    assert_eq!(transaction.inner().get_mem_buffer_readonly().len(), 0);
    transaction.block_on(|inner| inner.rollback()).unwrap();
}

#[tokio::test]
async fn empty_resolver_pass_does_not_consult_cancelled_caller_or_pd() {
    use tikv_client::async_util::Cancellation;
    use tikv_client::retry::RetryBackoffer;
    use tikv_client::txnkv::txnlock::{
        LockResolver, ResolveLockResult, ResolveLocksContext, ResolveLocksOptions,
    };
    let cancellation = Cancellation::default();
    cancellation.cancel();
    let owner = Arc::new(tokio::sync::Mutex::new(RetryBackoffer::new(
        cancellation,
        1,
    )));
    let result = LockResolver::new(ResolveLocksContext::default())
        .resolve_locks_with_opts(
            Arc::new(InProcessPdClient),
            Keyspace::Disable,
            None,
            owner,
            ResolveLocksOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(result, ResolveLockResult::default());
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn resolver_api_preserves_input_order_and_shared_status_cache_for_mixed_locks() {
    use tikv_client::async_util::Cancellation;
    use tikv_client::mock::{MockKvClient, MockPdClient};
    use tikv_client::proto::kvrpcpb;
    use tikv_client::retry::RetryBackoffer;
    use tikv_client::txnkv::txnlock::{
        LockResolver, ResolveLocksContext, ResolveLocksOptions, ResolvingLocksGuard,
    };

    let checks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = checks.clone();
    let client = MockKvClient::with_dispatch_hook(move |request| {
        if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
            recorded.lock().unwrap().push(request.lock_ts);
            return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse {
                commit_version: if request.lock_ts == 2 { 5 } else { 0 },
                action: kvrpcpb::Action::LockNotExistRollback as i32,
                ..Default::default()
            }));
        }
        if request.is::<kvrpcpb::ResolveLockRequest>() {
            return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()));
        }
        if request.is::<kvrpcpb::PessimisticRollbackRequest>() {
            return Ok(Box::new(kvrpcpb::PessimisticRollbackResponse::default()));
        }
        panic!("unexpected resolver RPC");
    });
    let pd = Arc::new(MockPdClient::new(client));
    let context = ResolveLocksContext::default();
    let resolver = LockResolver::new(context.clone());
    let locks: Vec<_> = [
        (2, kvrpcpb::Op::Put),
        (3, kvrpcpb::Op::PessimisticLock),
        (2, kvrpcpb::Op::Put),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (txn, op))| kvrpcpb::LockInfo {
        key: vec![b'a' + index as u8],
        primary_lock: b"a".to_vec(),
        lock_version: txn,
        lock_type: op as i32,
        txn_size: 32,
        ..Default::default()
    })
    .collect();
    let guard = ResolvingLocksGuard::new(context.clone(), &locks, 10);
    assert_eq!(context.resolving_locks().await.len(), 3);
    for _ in 0..2 {
        let owner = Arc::new(tokio::sync::Mutex::new(RetryBackoffer::new(
            Cancellation::default(),
            100,
        )));
        let result = resolver
            .resolve_locks_with_opts(
                pd.clone(),
                Keyspace::Disable,
                None,
                owner,
                ResolveLocksOptions {
                    caller_start_ts: 10,
                    locks: locks.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(result.ttl, 0);
        assert_eq!(result.access_locks, vec![2, 2]);
        assert_eq!(result.ignore_locks, vec![3]);
    }
    assert_eq!(*checks.lock().unwrap(), vec![2, 3]);
    drop(guard);
    assert!(context.resolving_locks().await.is_empty());
}

#[tokio::test]
async fn resolver_api_uses_exact_request_hints_and_existing_retry_owner() {
    use tikv_client::async_util::Cancellation;
    use tikv_client::proto::kvrpcpb;
    use tikv_client::retry::{RetryBackoffer, BO_REGION_MISS};
    use tikv_client::txnkv::txnlock::{
        LockHintsInRequest, LockResolver, ResolveLocksContext, ResolveLocksOptions,
    };
    let mut owner = RetryBackoffer::new(Cancellation::default(), 1);
    owner
        .backoff(BO_REGION_MISS, "previous region retry")
        .await
        .unwrap();
    let owner = Arc::new(tokio::sync::Mutex::new(owner));
    let error = LockResolver::new(ResolveLocksContext::default())
        .resolve_locks_with_opts(
            Arc::new(InProcessPdClient),
            Keyspace::Disable,
            None,
            owner.clone(),
            ResolveLocksOptions {
                for_read: true,
                locks: vec![kvrpcpb::LockInfo {
                    lock_version: 42,
                    ..Default::default()
                }],
                lock_hints_in_request: LockHintsInRequest::new(&[42], &[42]),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Static(tikv_client::error::StaticError::RegionUnavailable)
    ));
    let owner = owner.lock().await;
    assert_eq!(owner.total_backoff_times(), 1);
    assert_eq!(owner.total_sleep_ms(), 2);
}

#[tokio::test]
async fn externally_driven_waits_and_resolver_share_one_budget_and_fork_history() {
    use tikv_client::async_util::Cancellation;
    use tikv_client::retry::{RetryBackoffer, RetryError, BO_REGION_MISS, BO_STALE_CMD};
    let mut owner = RetryBackoffer::new(Cancellation::default(), 3);
    let wait = owner
        .prepare_backoff(BO_REGION_MISS, Some(1), "caller wait")
        .unwrap();
    assert_eq!(wait.duration().as_millis(), 1);
    owner.finish_backoff(wait, true).unwrap();
    let wait = owner
        .prepare_backoff(BO_REGION_MISS, None, "interrupted")
        .unwrap();
    assert_eq!(wait.duration().as_millis(), 4);
    owner.finish_backoff(wait, false).unwrap();
    let (mut child, _) = owner.fork();
    child
        .backoff(BO_STALE_CMD, "resolver region retry")
        .await
        .unwrap();
    owner.update_using_forked(&child);
    assert_eq!(owner.total_sleep_ms(), 3);
    assert_eq!(owner.total_backoff_times(), 3);
    let wait = owner
        .prepare_backoff(BO_REGION_MISS, None, "caller again")
        .unwrap();
    assert_eq!(wait.duration().as_millis(), 4);
    owner.finish_backoff(wait, true).unwrap();
    assert!(matches!(
        owner.prepare_backoff(BO_REGION_MISS, None, "exhausted"),
        Err(RetryError::Exhausted { .. })
    ));
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn resolver_read_cleanup_owns_its_lifetime_and_copies_only_source_metadata() {
    use tikv_client::async_util::Cancellation;
    use tikv_client::mock::{MockKvClient, MockPdClient};
    use tikv_client::proto::kvrpcpb;
    use tikv_client::retry::RetryBackoffer;
    use tikv_client::txnkv::txnlock::{LockResolver, ResolveLocksContext, ResolveLocksOptions};
    let contexts = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = contexts.clone();
    let completed = Arc::new(tokio::sync::Notify::new());
    let done = completed.clone();
    let client = MockKvClient::with_dispatch_hook(move |request| {
        if let Some(request) = request.downcast_ref::<kvrpcpb::CheckTxnStatusRequest>() {
            recorded
                .lock()
                .unwrap()
                .push(request.context.clone().unwrap());
            return Ok(Box::new(kvrpcpb::CheckTxnStatusResponse::default()));
        }
        if let Some(request) = request.downcast_ref::<kvrpcpb::ResolveLockRequest>() {
            recorded
                .lock()
                .unwrap()
                .push(request.context.clone().unwrap());
            done.notify_one();
            return Ok(Box::new(kvrpcpb::ResolveLockResponse::default()));
        }
        panic!("unexpected RPC");
    });
    let context = ResolveLocksContext::default();
    let cancellation = Cancellation::default();
    let owner = Arc::new(tokio::sync::Mutex::new(RetryBackoffer::new(
        cancellation.clone(),
        10,
    )));
    let resolver = LockResolver::new(context.clone()).with_request_context(&kvrpcpb::Context {
        request_source: "internal_test".to_owned(),
        resource_control_context: Some(kvrpcpb::ResourceControlContext {
            resource_group_name: "test-group".to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    });
    let detail = Arc::new(std::sync::Mutex::new(
        tikv_client::util::ResolveLockDetail::default(),
    ));
    let result = resolver
        .resolve_locks_with_opts(
            Arc::new(MockPdClient::new(client)),
            Keyspace::Disable,
            None,
            owner,
            ResolveLocksOptions {
                caller_start_ts: 10,
                for_read: true,
                detail: Some(detail.clone()),
                locks: vec![kvrpcpb::LockInfo {
                    key: b"b".to_vec(),
                    primary_lock: b"a".to_vec(),
                    lock_version: 1,
                    txn_size: 32,
                    lock_type: kvrpcpb::Op::Put as i32,
                    ..Default::default()
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    cancellation.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(2), completed.notified())
        .await
        .unwrap();
    assert_eq!(result.ignore_locks, vec![1]);
    assert!(detail.lock().unwrap().resolve_lock_time_ns > 0);
    let contexts = contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    assert_eq!(contexts[0].request_source, "internal_test");
    assert_eq!(
        contexts[0]
            .resource_control_context
            .as_ref()
            .unwrap()
            .resource_group_name,
        "test-group"
    );
    assert_eq!(contexts[1].request_source, "internal_test");
    assert!(contexts[1]
        .resource_control_context
        .as_ref()
        .is_none_or(|value| value.resource_group_name.is_empty()));
}
