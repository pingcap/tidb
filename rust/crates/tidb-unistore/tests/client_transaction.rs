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
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::{Key, TikvTransactionDriver, UnaryCallContext};
use tidb_unistore::mock::StorePdClient;
use tikv_client::proto::kvrpcpb;
use tikv_client::TimestampExt;

/// Go `mockstore`'s `tikv.NewTestTiKVStore` over unistore: client-rust's
/// store over unistore's PD and RPC clients, which reach one handler.
struct Store {
    tikv_store: TikvStore<StorePdClient>,
    runtime: Arc<tokio::runtime::Runtime>,
    regions: Arc<tidb_unistore_tikv::region_manager::InProcessRegionManager>,
}
impl Store {
    fn new() -> Self {
        let oracle = Arc::new(tidb_unistore_tikv::tso::Tso::new());
        let regions = Arc::new(tidb_unistore::cluster::new_region_manager());
        let handler = Arc::new(std::sync::Mutex::new(
            tidb_unistore_tikv::kv_handler::KvHandler::new(
                tidb_unistore_tikv::mvcc_store::MvccStore::with_pd(Arc::clone(&oracle)),
                Arc::clone(&regions),
            ),
        ));
        let (rpc, pd, _cluster) =
            tidb_unistore::mock::over(handler, Arc::clone(&regions), oracle, Vec::new()).unwrap();
        let tikv_store =
            tidb_unistore::mock::new_test_tikv_store(&rpc, pd, tikv_client::Config::default())
                .unwrap();
        Self {
            runtime: Arc::clone(tikv_store.runtime()),
            tikv_store,
            regions,
        }
    }
    /// Go `store.Begin()`: a transaction at a fresh oracle timestamp.
    fn begin(&self, pessimistic: bool) -> TikvTransactionDriver<StorePdClient> {
        let options = if pessimistic {
            tikv_client::TransactionOptions::new_pessimistic()
        } else {
            tikv_client::TransactionOptions::new_optimistic()
        };
        let transaction = self
            .tikv_store
            .block_on(self.tikv_store.store().begin_with_options(options))
            .unwrap();
        TikvTransactionDriver::new(transaction, self.runtime.clone())
    }
    fn facade(&self) -> tidb_txnkv::transaction::client::ClientTransaction<StorePdClient> {
        tidb_txnkv::transaction::client::ClientTransaction::new_opened(
            &self.tikv_store,
            self.timestamp(),
            false,
            self.tikv_store.authority_id(),
        )
        .unwrap()
    }
    fn timestamp(&self) -> u64 {
        self.tikv_store
            .block_on(self.tikv_store.store().current_timestamp())
            .unwrap()
            .version()
    }
    /// One raw request to the store holding `key`, outside any transaction:
    /// how a test leaves the store in a state no transaction API produces.
    fn send_raw<R, Resp>(&self, key: &[u8], build: impl FnOnce(kvrpcpb::Context) -> R) -> Resp
    where
        R: tikv_client::tikv::Request,
        Resp: 'static,
    {
        use tikv_client::PdClient as _;
        let store = self.tikv_store.store();
        let pd = store.pd_client();
        let region = self
            .tikv_store
            .block_on(pd.region_for_key(&key.to_vec().into()))
            .unwrap();
        let address = self
            .tikv_store
            .block_on(pd.all_stores())
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .target;
        let mut request = build(kvrpcpb::Context {
            region_id: region.region.id,
            region_epoch: region.region.region_epoch.clone(),
            peer: region.leader.clone(),
            ..Default::default()
        });
        *self
            .runtime
            .block_on(store.send_request(&address, &mut request, Duration::from_secs(2)))
            .unwrap()
            .downcast::<Resp>()
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
                        return Err(tikv_client::Error::ContextCanceled);
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
fn store_facade_cancellation_before_dispatch_does_not_publish() {
    use tidb_txnkv::transaction::BufferMutation;
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(1));
    call.cancellation().cancel();
    let txn = store.facade();
    // Go's committer fails the prewrite on the cancelled context: a
    // determinate rollback that published nothing.
    let outcome = txn
        .commit(
            vec![BufferMutation::set(b"a".to_vec(), b"cancelled".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    let tidb_txnkv::transaction::OptimisticCommitOutcome::RolledBack(rolled_back) = outcome else {
        panic!("a cancelled commit must roll back: {outcome:?}");
    };
    assert!(
        matches!(
            &rolled_back.cause,
            tidb_txnkv::transaction::TransactionCause::Transport { detail }
                if detail == "context canceled"
        ),
        "{:?}",
        rolled_back.cause
    );
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
    use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut old = store.facade();
    assert_eq!(old.snapshot_get(b"a", &call).unwrap().value, None);
    let writer = store.facade();
    let start_ts = writer.start_ts();
    let outcome = writer
        .commit(
            vec![BufferMutation::set(b"a".to_vec(), b"value".to_vec()).unwrap()],
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
        BufferMutation, OptimisticCommitOutcome, SchemaLease, SchemaLeaseChecker, SchemaLeaseError,
        TransactionCause,
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
                error: tidb_domain::domain_errors::ERR_INFO_SCHEMA_CHANGED
                    .generate_with_stack("schema changed"),
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
            vec![BufferMutation::set(b"a".to_vec(), b"invalid".to_vec()).unwrap()],
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        )
        .unwrap();
    assert!(
        matches!(outcome, OptimisticCommitOutcome::RolledBack(ref failure) if matches!(failure.cause, TransactionCause::SchemaLease { ref error } if error.code().value() == 8028)),
        "{outcome:?}"
    );
    assert_eq!(store.get(b"a"), None);
}

#[test]
fn store_facade_fair_retry_and_statement_cancel_keep_transaction_locks() {
    use tidb_txnkv::transaction::LockWaitTime;
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut txn = pessimistic(&store);
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
    use tidb_txnkv::transaction::LockWaitTime;
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut txn = pessimistic(&store);
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
    // Go's `MockRegionManager` bootstraps store 1, region 2, peer 3.
    assert_eq!(regions[0].region.id, 2);
    txn.finish_without_writes().unwrap();
}

/// A scan pair that met a lock is resolved and read again by a point Get
/// (Go `scanner.go` `getData`); that answer serves the row, so the region
/// fragments still hold it.
#[test]
fn store_facade_scan_regions_serves_a_row_its_scan_found_locked() {
    let store = Store::new();
    // A committed transaction whose secondary still holds its lock, as a
    // reader meets one while client-go commits secondaries after returning.
    let start_ts = store.timestamp();
    let put = |key: &[u8], value: &[u8]| kvrpcpb::Mutation {
        op: kvrpcpb::Op::Put as i32,
        key: key.to_vec(),
        value: value.to_vec(),
        ..Default::default()
    };
    let prewrite: kvrpcpb::PrewriteResponse =
        store.send_raw(b"a", |context| kvrpcpb::PrewriteRequest {
            context: Some(context),
            mutations: vec![put(b"a", b"one"), put(b"b", b"two")],
            primary_lock: b"a".to_vec(),
            start_version: start_ts,
            lock_ttl: 20_000,
            txn_size: 2,
            ..Default::default()
        });
    assert!(
        prewrite.errors.is_empty() && prewrite.region_error.is_none(),
        "{prewrite:?}"
    );
    let commit_ts = store.timestamp();
    let commit: kvrpcpb::CommitResponse = store.send_raw(b"a", |context| kvrpcpb::CommitRequest {
        context: Some(context),
        keys: vec![b"a".to_vec()],
        start_version: start_ts,
        commit_version: commit_ts,
        ..Default::default()
    });
    assert!(
        commit.error.is_none() && commit.region_error.is_none(),
        "{commit:?}"
    );

    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
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
    txn.finish_without_writes().unwrap();
}

/// A split the region cache has not seen: the scan meets the store's
/// stale-epoch answer, and client-go's sender retries it on the new regions
/// before the scanner sees a page. It must not read as an empty region.
#[test]
fn shared_engine_scan_retries_a_split_its_cache_has_not_seen() {
    let store = Store::new();
    let mut txn = store.begin(false);
    for key in [b"a", b"n", b"z"] {
        txn.set(Key::from(key.to_vec()), b"v".to_vec()).unwrap();
    }
    txn.commit().unwrap();
    let scan = |store: &Store| {
        let mut reader = store.begin(false);
        let keys = reader
            .scan(&Key::from(b"a".to_vec()), &Key::from(b"zz".to_vec()), 10)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        reader.finish_without_writes().unwrap();
        keys
    };
    let all = vec![
        Key::from(b"a".to_vec()),
        Key::from(b"n".to_vec()),
        Key::from(b"z".to_vec()),
    ];
    assert_eq!(scan(&store), all);
    store.regions.split_arbitrary(&[b"m".to_vec()]);
    assert_eq!(scan(&store), all);
}

/// The requests a transaction sent, as an interceptor added through Go's
/// `kv.RPCInterceptor` option saw them.
#[derive(Default)]
struct Sent {
    gets: Vec<kvrpcpb::GetRequest>,
    batch_gets: Vec<kvrpcpb::BatchGetRequest>,
    scans: Vec<kvrpcpb::ScanRequest>,
    locks: Vec<kvrpcpb::PessimisticLockRequest>,
    prewrites: Vec<kvrpcpb::PrewriteRequest>,
    commits: Vec<kvrpcpb::CommitRequest>,
}

fn record_requests() -> (
    Arc<std::sync::Mutex<Sent>>,
    tikv_client::RpcInterceptorHandle,
) {
    let sent = Arc::new(std::sync::Mutex::new(Sent::default()));
    let recorder = Arc::clone(&sent);
    let interceptor =
        tikv_client::new_rpc_interceptor("record-requests", move |_, request, next| {
            {
                let mut sent = recorder.lock().unwrap();
                let request = request.as_any();
                if let Some(request) = request.downcast_ref::<kvrpcpb::GetRequest>() {
                    sent.gets.push(request.clone());
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::BatchGetRequest>() {
                    sent.batch_gets.push(request.clone());
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::ScanRequest>() {
                    sent.scans.push(request.clone());
                } else if let Some(request) =
                    request.downcast_ref::<kvrpcpb::PessimisticLockRequest>()
                {
                    sent.locks.push(request.clone());
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::PrewriteRequest>() {
                    sent.prewrites.push(request.clone());
                } else if let Some(request) = request.downcast_ref::<kvrpcpb::CommitRequest>() {
                    sent.commits.push(request.clone());
                }
            }
            next()
        });
    (sent, interceptor)
}

fn resource_group(context: Option<&kvrpcpb::Context>) -> Option<String> {
    context
        .and_then(|context| context.resource_control_context.as_ref())
        .map(|resource| resource.resource_group_name.clone())
}

/// Go `onTxnActiveFunc`: the session's transaction in pessimistic mode.
fn pessimistic(store: &Store) -> tidb_txnkv::transaction::client::ClientTransaction<StorePdClient> {
    let mut transaction = store.facade();
    transaction.set_pessimistic(true);
    transaction
}

/// Go's session holds one `kv.Transaction` from `BEGIN` to `COMMIT`: its
/// reads are served at that transaction's `start_ts`, and client-go's
/// `2pc.go` sends the same `start_ts` as the prewrite's `start_version`.
#[test]
fn every_read_and_the_prewrite_carry_the_transactions_one_start_ts() {
    use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let (sent, recorder) = record_requests();
    let mut txn = store.facade();
    txn.add_rpc_interceptor(recorder);
    let start_ts = txn.start_ts();
    txn.snapshot_get(b"a", &call).unwrap();
    txn.snapshot_batch_get(&[b"b".to_vec(), b"c".to_vec()], &call)
        .unwrap();
    txn.snapshot_scan(b"d", b"z", None, &call).unwrap();
    let outcome = txn
        .commit(
            vec![BufferMutation::set(b"a".to_vec(), b"value".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    assert!(
        matches!(outcome, OptimisticCommitOutcome::Committed(_)),
        "{outcome:?}"
    );

    let sent = sent.lock().unwrap();
    assert!(!sent.gets.is_empty() && !sent.batch_gets.is_empty() && !sent.scans.is_empty());
    assert!(sent.gets.iter().all(|get| get.version == start_ts));
    assert!(sent.batch_gets.iter().all(|get| get.version == start_ts));
    assert!(sent.scans.iter().all(|scan| scan.version == start_ts));
    assert!(!sent.prewrites.is_empty());
    assert!(sent
        .prewrites
        .iter()
        .all(|prewrite| prewrite.start_version == start_ts));
    assert!(sent
        .commits
        .iter()
        .all(|commit| commit.start_version == start_ts));
}

/// TiKV refuses a prewrite when the key has a commit newer than its
/// `start_version`: the `WriteConflict` a transaction that lost a
/// read-then-write race reports. A fresh write timestamp would hide it.
#[test]
fn a_commit_newer_than_the_start_ts_makes_the_prewrite_a_write_conflict() {
    use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome, TransactionCause};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut loser = store.facade();
    assert_eq!(loser.snapshot_get(b"a", &call).unwrap().value, None);
    let winner = store.facade();
    let won = winner
        .commit(
            vec![BufferMutation::set(b"a".to_vec(), b"winner".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    assert!(
        matches!(won, OptimisticCommitOutcome::Committed(_)),
        "{won:?}"
    );
    // The loser's read still answers at its start_ts.
    assert_eq!(loser.snapshot_get(b"a", &call).unwrap().value, None);
    let lost = loser
        .commit(
            vec![BufferMutation::set(b"a".to_vec(), b"loser".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    let OptimisticCommitOutcome::RolledBack(lost) = lost else {
        panic!("the stale write must not publish: {lost:?}");
    };
    assert!(
        matches!(lost.cause, TransactionCause::WriteConflict { .. }),
        "{:?}",
        lost.cause
    );
    assert_eq!(store.get(b"a"), Some(b"winner".to_vec()));
}

/// A racing commit on another key is no conflict: only the keys this
/// transaction writes are checked against its `start_ts`.
#[test]
fn a_racing_commit_on_another_key_does_not_refuse_the_prewrite() {
    use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut first = store.facade();
    assert_eq!(first.snapshot_get(b"a", &call).unwrap().value, None);
    let other = store.facade();
    let other = other
        .commit(
            vec![BufferMutation::set(b"b".to_vec(), b"other".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    assert!(
        matches!(other, OptimisticCommitOutcome::Committed(_)),
        "{other:?}"
    );
    let outcome = first
        .commit(
            vec![BufferMutation::set(b"a".to_vec(), b"first".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    assert!(
        matches!(outcome, OptimisticCommitOutcome::Committed(_)),
        "{outcome:?}"
    );
    assert_eq!(store.get(b"a"), Some(b"first".to_vec()));
    assert_eq!(store.get(b"b"), Some(b"other".to_vec()));
}

/// Go's `kv.ResourceGroupName` transaction option labels every request the
/// transaction sends, prewrite and commit included.
#[test]
fn txn_resource_group_is_attached_to_prewrite_and_commit_rpc_contexts() {
    use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome};
    use tidb_txnkv::TxnResourceGroup;
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let (sent, recorder) = record_requests();
    let mut txn = store.facade();
    txn.add_rpc_interceptor(recorder);
    txn.set_resource_group_name("analytics");
    let outcome = txn
        .commit(
            vec![BufferMutation::set(b"a".to_vec(), b"value".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    assert!(
        matches!(outcome, OptimisticCommitOutcome::Committed(_)),
        "{outcome:?}"
    );
    let sent = sent.lock().unwrap();
    assert!(!sent.prewrites.is_empty() && !sent.commits.is_empty());
    for context in sent
        .prewrites
        .iter()
        .map(|prewrite| prewrite.context.as_ref())
        .chain(sent.commits.iter().map(|commit| commit.context.as_ref()))
    {
        assert_eq!(resource_group(context).as_deref(), Some("analytics"));
    }
}

/// The same label rides the pessimistic lock request.
#[test]
fn txn_resource_group_is_attached_to_pessimistic_lock_rpc_context() {
    use tidb_txnkv::transaction::LockWaitTime;
    use tidb_txnkv::TxnResourceGroup;
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let (sent, recorder) = record_requests();
    let mut txn = pessimistic(&store);
    txn.add_rpc_interceptor(recorder);
    txn.set_resource_group_name("analytics");
    txn.acquire_locks(
        &[b"a".to_vec()],
        &std::collections::BTreeSet::new(),
        LockWaitTime::AlwaysWait,
        &call,
    )
    .unwrap();
    let sent = sent.lock().unwrap();
    assert!(!sent.locks.is_empty());
    for lock in &sent.locks {
        assert_eq!(
            resource_group(lock.context.as_ref()).as_deref(),
            Some("analytics")
        );
    }
}

/// A key the statement presumes absent is locked with TiKV's `NotExist`
/// assertion; the others carry none.
#[test]
fn a_presumed_absent_key_carries_the_not_exists_assertion() {
    use tidb_txnkv::transaction::LockWaitTime;
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let (sent, recorder) = record_requests();
    let mut txn = pessimistic(&store);
    txn.add_rpc_interceptor(recorder);
    let presumed = std::collections::BTreeSet::from([b"a".to_vec()]);
    txn.acquire_locks(
        &[b"a".to_vec(), b"b".to_vec()],
        &presumed,
        LockWaitTime::AlwaysWait,
        &call,
    )
    .unwrap();
    let sent = sent.lock().unwrap();
    let assertion_for = |key: &[u8]| {
        sent.locks
            .iter()
            .flat_map(|lock| lock.mutations.iter())
            .find(|mutation| mutation.key == key)
            .expect("every requested key is sent")
            .assertion
    };
    assert_eq!(assertion_for(b"a"), kvrpcpb::Assertion::NotExist as i32);
    assert_eq!(assertion_for(b"b"), kvrpcpb::Assertion::None as i32);
}

/// Prewrite declares a pessimistic check only for the keys a statement
/// actually locked; a key first written at commit -- an index entry -- skips
/// it, and only a lock request advances the committer's `for_update_ts`.
#[test]
fn commit_declares_a_pessimistic_check_only_for_keys_it_actually_locked() {
    use tidb_txnkv::transaction::{BufferMutation, LockWaitTime, OptimisticTransactionState};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let (sent, recorder) = record_requests();
    let mut txn = pessimistic(&store);
    txn.add_rpc_interceptor(recorder);
    txn.acquire_locks(
        &[b"locked".to_vec()],
        &std::collections::BTreeSet::new(),
        LockWaitTime::AlwaysWait,
        &call,
    )
    .unwrap();
    let lock_for_update_ts = sent.lock().unwrap().locks[0].for_update_ts;
    let statement_ts = txn.advance_for_update_ts().unwrap();
    assert!(statement_ts > lock_for_update_ts);
    let outcome = txn
        .commit(
            vec![
                BufferMutation::set(b"locked".to_vec(), b"row".to_vec()).unwrap(),
                BufferMutation::set(b"unlocked".to_vec(), b"index".to_vec()).unwrap(),
            ],
            &call,
        )
        .unwrap();
    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);

    let sent = sent.lock().unwrap();
    let prewrite = sent
        .prewrites
        .iter()
        .find(|prewrite| prewrite.mutations.len() == 2)
        .expect("one prewrite carries both keys");
    assert_eq!(prewrite.for_update_ts, lock_for_update_ts);
    assert_eq!(
        prewrite.pessimistic_actions.len(),
        prewrite.mutations.len(),
        "TiKV matches actions to mutations by index"
    );
    let action_for = |key: &[u8]| {
        let index = prewrite
            .mutations
            .iter()
            .position(|mutation| mutation.key == key)
            .expect("every mutation is sent");
        prewrite.pessimistic_actions[index]
    };
    assert_eq!(
        action_for(b"locked"),
        kvrpcpb::prewrite_request::PessimisticAction::DoPessimisticCheck as i32
    );
    assert_eq!(
        action_for(b"unlocked"),
        kvrpcpb::prewrite_request::PessimisticAction::SkipPessimisticCheck as i32
    );
}

/// A value-returning lock asks TiKV for the rows and answers them: bytes for
/// an existing key, `None` for an absent one.
#[test]
fn a_returning_value_lock_carries_the_rows_answer() {
    use tidb_txnkv::transaction::{BufferMutation, LockWaitTime};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    store
        .facade()
        .commit(
            vec![BufferMutation::set(b"row".to_vec(), b"row-value".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    let (sent, recorder) = record_requests();
    let mut txn = pessimistic(&store);
    txn.add_rpc_interceptor(recorder);
    let acquired = txn
        .acquire_locks_returning_values(
            &[b"row".to_vec(), b"absent".to_vec()],
            &std::collections::BTreeSet::new(),
            LockWaitTime::AlwaysWait,
            &call,
        )
        .unwrap();
    assert_eq!(
        acquired.values.get(b"row".as_slice()).map(|v| v.as_deref()),
        Some(Some(b"row-value".as_slice()))
    );
    assert_eq!(
        acquired
            .values
            .get(b"absent".as_slice())
            .map(|v| v.as_deref()),
        Some(None)
    );
    assert!(sent
        .lock()
        .unwrap()
        .locks
        .iter()
        .all(|lock| lock.return_values));
}

/// A plain lock does not ask TiKV for values.
#[test]
fn a_plain_lock_still_omits_return_values() {
    use tidb_txnkv::transaction::LockWaitTime;
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let (sent, recorder) = record_requests();
    let mut txn = pessimistic(&store);
    txn.add_rpc_interceptor(recorder);
    txn.acquire_locks(
        &[b"row".to_vec()],
        &std::collections::BTreeSet::new(),
        LockWaitTime::AlwaysWait,
        &call,
    )
    .unwrap();
    let sent = sent.lock().unwrap();
    assert!(!sent.locks.is_empty());
    assert!(sent.locks.iter().all(|lock| !lock.return_values));
}

/// Go `SetOption(kv.ResourceGroupName)` on a snapshot read labels its Get,
/// BatchGet and Scan, and clearing the option clears the label.
#[test]
fn snapshot_read_options_reach_native_requests_and_reset() {
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let (sent, recorder) = record_requests();
    let mut txn = store.facade();
    txn.add_rpc_interceptor(recorder);
    // Distinct keys per round: the snapshot caches what it read (client-go
    // `KVSnapshot.mu.cached`), and a cached key sends nothing.
    for (round, group) in [Some("read-group"), None].into_iter().enumerate() {
        txn.set_snapshot_read_options(&tidb_txnkv::SnapshotReadOptions {
            resource_group_name: group.map(str::to_owned),
            ..Default::default()
        });
        let key = |name: &str| format!("{name}{round}").into_bytes();
        txn.snapshot_get(&key("a"), &call).unwrap();
        txn.snapshot_batch_get(&[key("b"), key("c")], &call)
            .unwrap();
        txn.snapshot_scan(b"d", b"z", None, &call).unwrap();
        let mut sent = sent.lock().unwrap();
        let labelled = |context: Option<&kvrpcpb::Context>| {
            resource_group(context).filter(|name| !name.is_empty())
        };
        let expected = group.map(str::to_owned);
        assert_eq!(
            labelled(sent.gets.last().unwrap().context.as_ref()),
            expected
        );
        assert_eq!(
            labelled(sent.batch_gets.last().unwrap().context.as_ref()),
            expected
        );
        assert_eq!(
            labelled(sent.scans.last().unwrap().context.as_ref()),
            expected
        );
        *sent = Sent::default();
    }
    txn.finish_without_writes().unwrap();
}

/// Go `snapshot.SetOption(kv.CollectRuntimeStats, stats)`: a point reader's
/// own `SnapshotRuntimeStats` collect the snapshot's Get/BatchGet RPCs while
/// installed, and clearing the option returns collection to the
/// transaction's own statistics, untouched meanwhile.
#[test]
fn reader_snapshot_runtime_stats_collect_only_while_installed() {
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
    let mut txn = store.facade();
    let stats = Arc::new(tikv_client::SnapshotRuntimeStats::default());
    txn.set_snapshot_read_options(&tidb_txnkv::SnapshotReadOptions {
        runtime_stats: Some(tidb_txnkv::SnapshotRuntimeStatsHandle(Arc::clone(&stats))),
        ..Default::default()
    });
    txn.snapshot_get(b"stats-a", &call).unwrap();
    txn.snapshot_batch_get(&[b"stats-b".to_vec(), b"stats-c".to_vec()], &call)
        .unwrap();
    assert_eq!(stats.rpc_count(tikv_client::SnapshotRpcCommand::Get), 1);
    assert_eq!(
        stats.rpc_count(tikv_client::SnapshotRpcCommand::BatchGet),
        1
    );
    let text = stats.to_string();
    assert!(text.starts_with("Get:{num_rpc:1, total_time:"), "{text}");
    assert!(text.contains("BatchGet:{num_rpc:1, total_time:"), "{text}");
    assert_eq!(txn.snapshot_point_rpc_counts(), (1, 1));

    txn.set_snapshot_read_options(&tidb_txnkv::SnapshotReadOptions::default());
    assert_eq!(txn.snapshot_point_rpc_counts(), (0, 0));
    txn.snapshot_get(b"stats-d", &call).unwrap();
    assert_eq!(txn.snapshot_point_rpc_counts(), (1, 0));
    assert_eq!(stats.rpc_count(tikv_client::SnapshotRpcCommand::Get), 1);
    txn.finish_without_writes().unwrap();
}

/// `NOWAIT` fails the statement at once instead of queueing behind a live
/// owner.
#[test]
fn nowait_fails_immediately_rather_than_queueing_behind_a_live_owner() {
    use tidb_txnkv::transaction::{LockWaitTime, PessimisticLockFailure};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(5));
    let none = std::collections::BTreeSet::new();
    let mut owner = pessimistic(&store);
    owner
        .acquire_locks(&[b"k".to_vec()], &none, LockWaitTime::AlwaysWait, &call)
        .unwrap();
    let mut waiter = pessimistic(&store);
    let started = std::time::Instant::now();
    let failure = waiter
        .acquire_locks(&[b"k".to_vec()], &none, LockWaitTime::NoWait, &call)
        .unwrap_err();
    assert!(
        matches!(&failure, PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key } if key == b"k"),
        "{failure:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    waiter.rollback(&call).unwrap();
    owner.rollback(&call).unwrap();
}

/// A statement's lock-wait budget ends in a lock-wait timeout while the
/// owner is still alive.
#[test]
fn an_exhausted_lock_wait_budget_reports_a_timeout() {
    use tidb_txnkv::transaction::{LockWaitTime, PessimisticLockFailure};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(5));
    let none = std::collections::BTreeSet::new();
    let mut owner = pessimistic(&store);
    owner
        .acquire_locks(&[b"k".to_vec()], &none, LockWaitTime::AlwaysWait, &call)
        .unwrap();
    let mut waiter = pessimistic(&store);
    let failure = waiter
        .acquire_locks(
            &[b"k".to_vec()],
            &none,
            LockWaitTime::Timeout(Duration::from_millis(100)),
            &call,
        )
        .unwrap_err();
    assert!(
        matches!(&failure, PessimisticLockFailure::LockWaitTimeout { key } if key == b"k"),
        "{failure:?}"
    );
    waiter.rollback(&call).unwrap();
    owner.rollback(&call).unwrap();
}

/// A commit newer than the statement's `for_update_ts` is a statement-scoped
/// write conflict; the statement retried at an advanced `for_update_ts`
/// locks the key and reads the newer row.
#[test]
fn a_write_conflict_is_statement_scoped_and_a_newer_timestamp_retries() {
    use tidb_txnkv::transaction::{BufferMutation, LockWaitTime, PessimisticLockFailure};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(5));
    let none = std::collections::BTreeSet::new();
    let mut txn = pessimistic(&store);
    store
        .facade()
        .commit(
            vec![BufferMutation::set(b"k".to_vec(), b"newer".to_vec()).unwrap()],
            &call,
        )
        .unwrap();
    let failure = txn
        .acquire_locks_returning_values(&[b"k".to_vec()], &none, LockWaitTime::AlwaysWait, &call)
        .unwrap_err();
    assert!(
        matches!(failure, PessimisticLockFailure::WriteConflict { .. }),
        "{failure:?}"
    );
    txn.advance_for_update_ts().unwrap();
    let acquired = txn
        .acquire_locks_returning_values(&[b"k".to_vec()], &none, LockWaitTime::AlwaysWait, &call)
        .unwrap();
    assert_eq!(
        acquired.values.get(b"k".as_slice()).map(|v| v.as_deref()),
        Some(Some(b"newer".as_slice()))
    );
    txn.rollback(&call).unwrap();
}

/// TiKV's deadlock detector proves the cycle two transactions form; the
/// statement that closes it is aborted. The waiter woken by the victim's
/// rollback gets a write conflict (TiKV's legacy wake-up, Go unistore's
/// `KvPessimisticLock`) and locks on the statement's retry.
#[test]
fn a_detected_deadlock_aborts_the_statement_that_closes_the_cycle() {
    use tidb_txnkv::transaction::{LockWaitTime, PessimisticLockFailure};
    let store = Store::new();
    let call = UnaryCallContext::with_timeout(Duration::from_secs(10));
    let none = std::collections::BTreeSet::new();
    let mut first = pessimistic(&store);
    let mut second = pessimistic(&store);
    first
        .acquire_locks(&[b"k1".to_vec()], &none, LockWaitTime::AlwaysWait, &call)
        .unwrap();
    second
        .acquire_locks(&[b"k2".to_vec()], &none, LockWaitTime::AlwaysWait, &call)
        .unwrap();
    let waiter = std::thread::spawn({
        let call = call.clone();
        move || {
            let result = first.acquire_locks(
                &[b"k2".to_vec()],
                &std::collections::BTreeSet::new(),
                LockWaitTime::AlwaysWait,
                &call,
            );
            (first, result)
        }
    });
    // The first transaction waits on k2 before the second closes the cycle.
    std::thread::sleep(Duration::from_millis(200));
    let failure = second
        .acquire_locks(&[b"k1".to_vec()], &none, LockWaitTime::AlwaysWait, &call)
        .unwrap_err();
    assert!(
        matches!(failure, PessimisticLockFailure::Deadlock(_)),
        "{failure:?}"
    );
    second.rollback(&call).unwrap();
    let (mut first, result) = waiter.join().unwrap();
    let woken = result.unwrap_err();
    assert!(
        matches!(woken, PessimisticLockFailure::WriteConflict { .. }),
        "{woken:?}"
    );
    first.advance_for_update_ts().unwrap();
    first
        .acquire_locks(&[b"k2".to_vec()], &none, LockWaitTime::AlwaysWait, &call)
        .unwrap();
    first.rollback(&call).unwrap();
}
