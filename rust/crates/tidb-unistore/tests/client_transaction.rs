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

//! The client-rust transaction engine must work over TiDB's actual embedded
//! transport, not only client-rust's separate mocktikv implementation.
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use tidb_txnkv::{
    driver::client_bridge::ClientPd, lock::TimestampSource, region::RegionCache, Key,
    SharedReadRuntime, TikvTransactionDriver, UnaryCallContext,
};
use tidb_unistore::{client::InProcessClient, region_loader::InProcessRegionLoader};
use tikv_client::{pd::PdClient, Transaction, TransactionOptions};

#[derive(Debug, Clone)]
struct Oracle(Arc<tidb_unistore::tso::Tso>);
impl TimestampSource for Oracle {
    fn current_ts(&self) -> Result<u64, String> {
        Ok(self.0.get_composed_ts())
    }
}
struct Store {
    pd: Arc<ClientPd>,
    storage: SharedReadRuntime<InProcessClient, InProcessRegionLoader>,
    oracle: Oracle,
    runtime: Arc<tokio::runtime::Runtime>,
}
impl Store {
    fn new() -> Self {
        let oracle = Arc::new(tidb_unistore::tso::Tso::new());
        let client = InProcessClient::over(tidb_unistore::kv_handler::KvHandler {
            store: tidb_unistore::mvcc_store::MvccStore::with_pd(oracle.clone()),
        });
        let storage = SharedReadRuntime::new_injected(
            client.clone(),
            RegionCache::new(InProcessRegionLoader),
        );
        let oracle = Oracle(oracle);
        let pd = ClientPd::new(storage.clone(), oracle.clone());
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap(),
        );
        Self {
            pd,
            runtime,
            storage,
            oracle,
        }
    }
    fn begin(&self, pessimistic: bool) -> TikvTransactionDriver<ClientPd> {
        let timestamp = self
            .runtime
            .block_on(self.pd.clone().get_timestamp())
            .unwrap();
        let options = if pessimistic {
            TransactionOptions::new_pessimistic()
        } else {
            TransactionOptions::new_optimistic()
        };
        let transaction = Transaction::new(
            timestamp,
            self.pd.clone(),
            options,
            tikv_client::request::Keyspace::Disable,
        );
        TikvTransactionDriver::new(transaction, self.runtime.clone())
    }
    fn facade(
        &self,
    ) -> tidb_txnkv::transaction::client::ClientTransaction<
        InProcessClient,
        InProcessRegionLoader,
        Oracle,
    > {
        tidb_txnkv::transaction::client::ClientTransaction::new_injected(
            self.storage.clone(),
            self.oracle.clone(),
            Duration::from_secs(2),
            self.oracle.current_ts().unwrap(),
            std::time::Instant::now(),
            10,
            4096,
        )
        .unwrap()
    }
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut reader = self.begin(false);
        let value = reader.get(&Key::from(key.to_vec())).unwrap();
        reader.finish_without_writes().unwrap();
        value
    }
}

#[test]
fn shared_engine_commits_and_rolls_back_staged_statements() {
    let store = Store::new();
    let key = Key::from(b"a".to_vec());
    let mut txn = store.begin(false);
    txn.set(key.clone(), b"old".to_vec()).unwrap();
    let stage = txn.staging();
    txn.set(key.clone(), b"aborted".to_vec()).unwrap();
    txn.cleanup(stage);
    assert_eq!(txn.get(&key).unwrap(), Some(b"old".to_vec()));
    assert_eq!(store.get(b"a"), None);
    txn.commit().unwrap();
    assert_eq!(store.get(b"a"), Some(b"old".to_vec()));
    let mut txn = store.begin(false);
    txn.delete(key).unwrap();
    txn.rollback().unwrap();
    assert_eq!(store.get(b"a"), Some(b"old".to_vec()));
}

#[test]
fn shared_engine_detects_first_committer_wins() {
    let store = Store::new();
    let mut first = store.begin(false);
    let mut second = store.begin(false);
    first
        .set(Key::from(b"a".to_vec()), b"first".to_vec())
        .unwrap();
    second
        .set(Key::from(b"a".to_vec()), b"second".to_vec())
        .unwrap();
    second.commit().unwrap();
    let result = first.commit_staged();
    assert!(
        matches!(
            result,
            Ok(tidb_txnkv::transaction::OptimisticCommitOutcome::RolledBack(_))
        ),
        "{result:?}"
    );
    assert_eq!(store.get(b"a"), Some(b"second".to_vec()));
}

#[test]
fn shared_engine_owns_pessimistic_locks_until_transaction_end() {
    let store = Store::new();
    let key = Key::from(b"a".to_vec());
    let mut writer = store.begin(true);
    writer.lock_keys(&[key.clone()]).unwrap();
    writer.set(key.clone(), b"committed".to_vec()).unwrap();
    writer.commit().unwrap();
    assert_eq!(store.get(b"a"), Some(b"committed".to_vec()));
    let mut writer = store.begin(true);
    writer.lock_keys(&[key.clone()]).unwrap();
    writer.rollback().unwrap();
    let mut next = store.begin(true);
    next.lock_keys(&[key]).unwrap();
    next.rollback().unwrap();
}

#[test]
fn shared_engine_reads_ordered_snapshot_without_staged_writes() {
    let store = Store::new();
    let mut txn = store.begin(false);
    txn.set(Key::from(b"a".to_vec()), b"one".to_vec()).unwrap();
    txn.set(Key::from(b"b".to_vec()), b"two".to_vec()).unwrap();
    txn.commit().unwrap();
    let mut reader = store.begin(false);
    let pairs = reader
        .scan(&Key::from(b"a".to_vec()), &Key::from(b"z".to_vec()), 10)
        .unwrap();
    assert_eq!(
        pairs,
        vec![
            (Key::from(b"a".to_vec()), b"one".to_vec()),
            (Key::from(b"b".to_vec()), b"two".to_vec())
        ]
    );
    reader.finish_without_writes().unwrap();
}

#[test]
fn shared_engine_never_rolls_back_an_ambiguous_primary() {
    use tikv_client::proto::kvrpcpb::{BatchRollbackRequest, CommitRequest};
    let store = Store::new();
    let mut txn = store.begin(false);
    let rollbacks = Arc::new(AtomicU64::new(0));
    let observed = rollbacks.clone();
    txn.transaction_mut()
        .set_rpc_interceptor(tikv_client::new_rpc_interceptor(
            "lose-primary-response",
            move |_, request, next| {
                let is_primary = request
                    .as_any()
                    .downcast_ref::<CommitRequest>()
                    .is_some_and(|request| request.keys.contains(&b"a".to_vec()));
                let observed = observed.clone();
                Box::pin(async move {
                    if request.as_any().is::<BatchRollbackRequest>() {
                        observed.fetch_add(1, Ordering::SeqCst);
                    }
                    let response = next().await?;
                    if is_primary {
                        return Err(tikv_client::Error::StringError(
                            "context canceled".to_owned(),
                        ));
                    }
                    Ok(response)
                })
            },
        ));
    txn.set(Key::from(b"a".to_vec()), b"committed".to_vec())
        .unwrap();
    let result = txn.commit_staged().unwrap();
    assert!(
        matches!(
            result,
            tidb_txnkv::transaction::OptimisticCommitOutcome::Undetermined(_)
        ),
        "{result:?}"
    );
    assert_eq!(store.get(b"a"), Some(b"committed".to_vec()));
    assert_eq!(rollbacks.load(Ordering::SeqCst), 0);
}

#[test]
fn shared_engine_cancellation_before_dispatch_does_not_publish() {
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(1));
    call.cancellation().cancel();
    store.pd.set_call(&call);
    let mut txn = store.begin(false);
    txn.set(Key::from(b"a".to_vec()), b"cancelled".to_vec())
        .unwrap();
    assert!(txn.commit().is_err());
    store
        .pd
        .set_call(&UnaryCallContext::with_timeout(Duration::from_secs(1)));
    assert_eq!(store.get(b"a"), None);
}

#[test]
fn shared_engine_fast_commit_permissions_preserve_results() {
    let store = Store::new();
    for (i, (one_pc, async_commit)) in [(true, false), (false, true), (true, true)]
        .into_iter()
        .enumerate()
    {
        let mut txn = store.begin(false);
        txn.transaction_mut().inner_mut().set_enable_one_pc(one_pc);
        txn.transaction_mut()
            .inner_mut()
            .set_enable_async_commit(async_commit);
        let key = format!("key{i}").into_bytes();
        txn.set(Key::from(key.clone()), b"value".to_vec()).unwrap();
        txn.commit().unwrap();
        assert_eq!(store.get(&key), Some(b"value".to_vec()));
    }
}

#[test]
fn shared_engine_statement_cleanup_retains_ordinary_pessimistic_locks() {
    // Go LazyTxn.cleanup cleans the statement MemBuffer staging handle;
    // successful ordinary LockKeys locks remain until the transaction ends.
    let store = Store::new();
    let mut txn = store.begin(true);
    let key = Key::from(b"a".to_vec());
    txn.lock_keys(&[key.clone()]).unwrap();
    let stage = txn.staging();
    txn.set(key.clone(), b"discarded".to_vec()).unwrap();
    txn.cleanup(stage);
    assert_eq!(txn.locked_keys(), vec![key]);
    txn.rollback().unwrap();
    assert_eq!(store.get(b"a"), None);
}

#[test]
fn shared_engine_fair_statement_cancel_keeps_prior_locks() {
    let store = Store::new();
    let mut txn = store.begin(true);
    let prior = Key::from(b"a".to_vec());
    let current = Key::from(b"b".to_vec());
    txn.lock_keys(&[prior.clone()]).unwrap();
    txn.start_statement_locking();
    txn.lock_statement_keys(&[current], false, txn.start_ts(), -1, None)
        .unwrap();
    txn.cancel_statement_locking().unwrap();
    assert_eq!(txn.locked_keys(), vec![prior]);
    txn.rollback().unwrap();
}

#[test]
fn store_facade_preserves_snapshot_and_commit_timestamps() {
    use tidb_txnkv::transaction::{OptimisticCommitOutcome, OptimisticMutation};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut old = store.facade();
    assert_eq!(old.snapshot_get(b"a", &call).unwrap().value, None);
    let writer = store.facade();
    let start_ts = writer.start_ts();
    let outcome = writer
        .commit(
            vec![OptimisticMutation::meta_put(b"a".to_vec(), b"value".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    assert!(
        matches!(outcome, OptimisticCommitOutcome::Committed(_)),
        "{outcome:?}"
    );
    assert_eq!(outcome.receipt().start_ts, start_ts);
    assert!(outcome.receipt().commit_ts > start_ts);
    assert_eq!(old.snapshot_get(b"a", &call).unwrap().value, None);
    old.finish_without_writes().unwrap();
    assert_eq!(store.get(b"a"), Some(b"value".to_vec()));
}

#[test]
fn store_facade_preserves_schema_error_and_never_publishes_invalid_schema() {
    use tidb_txnkv::transaction::{
        OptimisticCommitOutcome, OptimisticMutation, SchemaLease, SchemaLeaseChecker,
        SchemaLeaseError, TransactionCause,
    };
    struct Changed;
    impl SchemaLeaseChecker for Changed {
        fn check_by_schema_ver(
            &self,
            check_ts: u64,
            table_ids: &[i64],
        ) -> Result<(), SchemaLeaseError> {
            assert!(check_ts > 0);
            assert_eq!(table_ids, &[42]);
            Err(SchemaLeaseError {
                code: 8028,
                message: "schema changed".to_owned(),
            })
        }
    }
    let store = Store::new();
    let mut txn = store.facade();
    txn.set_schema_lease(SchemaLease {
        checker: Arc::new(Changed),
        related_physical_table_ids: vec![42],
    });
    let outcome = txn
        .commit(
            vec![OptimisticMutation::meta_put(b"a".to_vec(), b"invalid".to_vec()).unwrap()],
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        )
        .unwrap();
    assert!(
        matches!(outcome, OptimisticCommitOutcome::RolledBack(ref failure) if matches!(failure.cause, TransactionCause::SchemaLease { code: 8028, .. })),
        "{outcome:?}"
    );
    assert_eq!(store.get(b"a"), None);
}

#[test]
fn store_facade_fair_retry_and_statement_cancel_keep_transaction_locks() {
    use tidb_txnkv::transaction::{client::ClientPessimisticTransaction, LockWaitTime};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut txn =
        ClientPessimisticTransaction::from_transaction(store.facade(), std::time::Instant::now())
            .unwrap();
    let absent = std::collections::BTreeSet::new();
    txn.acquire_locks(&[b"prior".to_vec()], &absent, LockWaitTime::NoWait, &call)
        .unwrap();
    txn.set_fair_locking(true);
    txn.acquire_locks(&[b"stale".to_vec()], &absent, LockWaitTime::NoWait, &call)
        .unwrap();
    txn.advance_for_update_ts().unwrap();
    txn.acquire_locks(&[b"wanted".to_vec()], &absent, LockWaitTime::NoWait, &call)
        .unwrap();
    txn.finish_statement(true).unwrap();
    assert_eq!(
        txn.locked_keys(),
        vec![b"prior".to_vec(), b"wanted".to_vec()]
    );
    txn.acquire_locks(
        &[b"cancelled".to_vec()],
        &absent,
        LockWaitTime::NoWait,
        &call,
    )
    .unwrap();
    txn.finish_statement(false).unwrap();
    assert_eq!(
        txn.locked_keys(),
        vec![b"prior".to_vec(), b"wanted".to_vec()]
    );
    txn.rollback(&call).unwrap();
}

#[test]
fn store_facade_lock_values_distinguish_missing_and_previously_locked_keys() {
    use tidb_txnkv::transaction::{client::ClientPessimisticTransaction, LockWaitTime};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut txn =
        ClientPessimisticTransaction::from_transaction(store.facade(), std::time::Instant::now())
            .unwrap();
    let absent = std::collections::BTreeSet::new();
    let result = txn
        .acquire_locks_returning_values(
            &[b"missing".to_vec()],
            &absent,
            LockWaitTime::NoWait,
            &call,
        )
        .unwrap();
    assert_eq!(result.values.get(b"missing".as_slice()), Some(&None));
    assert_eq!(result.primary_key, b"missing");
    let result = txn
        .acquire_locks_returning_values(
            &[b"missing".to_vec()],
            &absent,
            LockWaitTime::NoWait,
            &call,
        )
        .unwrap();
    assert!(result.values.is_empty());
    assert!(result.keys.is_empty());
    txn.rollback(&call).unwrap();
}

#[test]
fn store_facade_scan_regions_uses_actual_serving_responses() {
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut writer = store.begin(false);
    writer
        .set(Key::from(b"a".to_vec()), b"one".to_vec())
        .unwrap();
    writer
        .set(Key::from(b"b".to_vec()), b"two".to_vec())
        .unwrap();
    writer.commit().unwrap();
    let mut txn = store.facade();
    let regions = txn.snapshot_scan_regions(b"a", b"z", &call).unwrap();
    assert_eq!(regions.len(), 1);
    assert_eq!(
        regions[0].pairs,
        vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"b".to_vec(), b"two".to_vec())
        ]
    );
    assert_eq!(regions[0].region.id, 1);
    txn.finish_without_writes().unwrap();
}

#[test]
fn client_store_invalidation_refreshes_the_shared_cache() {
    use tidb_txnkv::region::StoreResolveState;
    let store = Store::new();
    let key = tikv_client::Key::from(b"a".to_vec());
    store
        .runtime
        .block_on(store.pd.region_for_key(&key))
        .unwrap();
    let before = store
        .storage
        .with_region_cache(|cache| cache.store_state(1).unwrap().epoch())
        .unwrap();
    store.runtime.block_on(store.pd.invalidate_store_cache(1));
    store
        .storage
        .with_region_cache(|cache| {
            let state = cache.store_state(1).unwrap();
            assert_eq!(state.resolve_state(), StoreResolveState::NeedCheck);
            assert!(state.epoch() > before);
        })
        .unwrap();
    store
        .runtime
        .block_on(store.pd.region_for_key(&key))
        .unwrap();
    store
        .storage
        .with_region_cache(|cache| {
            assert_eq!(
                cache.store_state(1).unwrap().resolve_state(),
                StoreResolveState::Resolved
            )
        })
        .unwrap();
}
