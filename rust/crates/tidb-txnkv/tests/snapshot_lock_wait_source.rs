// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A snapshot read blocked behind another transaction's live lock WAITS the
//! lock out — Go's `KVSnapshot.get` retries under a 20-second TIME budget
//! (`getMaxBackoff`), sleeping `BoTxnLockFast` (2ms base, exponential) capped
//! by the lock's remaining TTL, with NO attempt cap.
//!
//! The regression: this port used to cap lock retries at FOUR ATTEMPTS, which
//! sysbench's concurrent `UPDATE ... WHERE id=?` exhausted in milliseconds on
//! a hot row — statements failed with "snapshot lock retry budget exhausted"
//! (and, through the write path's error mapping of the day, reached the
//! client as a 1064 SYNTAX error) where Go simply waits and answers.
//!
//! The client is mocked at the trait seam ([`TransactionCommandClient`] +
//! [`LockRecoveryClient`]), the same seam `tidb-unistore`'s in-process client
//! implements, because a lock that stays alive for exactly N probes cannot be
//! produced on demand against a live cluster.

// aggregate-test: standalone (mutates the process configuration)

#![allow(missing_docs)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_proto::{
    KvrpcBatchGetRequest, KvrpcBatchGetResponse, KvrpcBatchRollbackRequest,
    KvrpcBatchRollbackResponse, KvrpcCheckSecondaryLocksRequest, KvrpcCheckSecondaryLocksResponse,
    KvrpcCheckTxnStatusRequest, KvrpcCheckTxnStatusResponse, KvrpcCommitRequest,
    KvrpcCommitResponse, KvrpcContext, KvrpcGetRequest, KvrpcGetResponse, KvrpcKeyError,
    KvrpcLockInfo, KvrpcPessimisticLockRequest, KvrpcPessimisticLockResponse,
    KvrpcPessimisticRollbackRequest, KvrpcPessimisticRollbackResponse, KvrpcPrewriteRequest,
    KvrpcPrewriteResponse, KvrpcResolveLockRequest, KvrpcResolveLockResponse, KvrpcScanRequest,
    KvrpcScanResponse, KvrpcTxnHeartBeatRequest, KvrpcTxnHeartBeatResponse,
};
use tidb_txnkv::lock::{LockRecoveryClient, TimestampSource};
use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation,
    RegionMetadata, RegionRecoveryLoader, RegionVerId, Store,
};
use tidb_txnkv::rpc::{
    BatchCommandTag, DirectUnaryClientError, TransactionBatchPublication, TransactionBatchResponse,
    UnaryCallContext,
};
use tidb_txnkv::transaction::{
    PublishedCommand, RealOptimisticTransaction, TransactionCommandClient,
};
use tidb_txnkv::SharedReadRuntime;

// Every test holds this guard until its request workers have joined. The
// standalone test process prevents global flag changes reaching other suites.
fn snapshot_test_config() -> std::sync::MutexGuard<'static, ()> {
    static CONFIG_LOCK: Mutex<()> = Mutex::new(());
    let guard = CONFIG_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    tidb_config::config_tree::config::store_global_config(tidb_config::config_tree::new_config());
    guard
}

const START_TS: u64 = 100;
/// The blocking transaction's own start timestamp, older than the reader's.
const LOCK_TS: u64 = 90;
const CALL_TIMEOUT: Duration = Duration::from_secs(30);
const ROW_KEY: &[u8] = b"row-1";
const REGION: u64 = 62;
const ADDRESS: &str = "in-process-lock-wait";

/// More times than the removed four-attempt cap: the old code failed on the
/// fifth lock encounter, so serving the value only after the seventh probe is
/// what separates the time budget from the counter.
const LOCKED_RESPONSES: u64 = 7;

#[derive(Clone)]
struct OneRegion;

impl RegionLoader for OneRegion {
    fn cluster_id(&self) -> u64 {
        11
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Ok(RegionLocation {
            region: RegionVerId {
                id: REGION,
                epoch: RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                },
            },
            start_key: Vec::new(),
            end_key: Vec::new(),
            peers: vec![Peer {
                id: 620,
                store_id: 6200,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 1,
            }],
            leader_peer_id: Some(620),
            stores: vec![Store {
                id: 6200,
                address: ADDRESS.to_owned(),
                epoch: 1,
            }],
            ..RegionLocation::default()
        })
    }
}

impl RegionRecoveryLoader for OneRegion {
    fn hydrate_region(
        &mut self,
        _metadata: &RegionMetadata,
        _leader_store_id: u64,
        _resolved_stores: &mut std::collections::BTreeMap<
            u64,
            Option<tidb_txnkv::region::StoreMetadata>,
        >,
    ) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-hydration",
            "this regression never takes the EpochNotMatch hydration path",
        ))
    }
}

/// A TSO that ticks on every call: each status-check round asks for a fresh
/// current timestamp, and seven lock rounds need seven of them.
#[derive(Debug)]
struct TickingTimestamps(std::sync::atomic::AtomicU64);

impl TimestampSource for TickingTimestamps {
    fn current_ts(&self) -> Result<u64, String> {
        Ok(self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

#[derive(Debug, Default)]
struct Recorded {
    get_versions: Vec<u64>,
    get_contexts: Vec<KvrpcContext>,
    scans: Vec<(KvrpcScanRequest, KvrpcContext)>,
    batch_requests: Vec<KvrpcBatchGetRequest>,
    async_batch_calls: usize,
    batch_contexts: Vec<KvrpcContext>,
    status_checks: Vec<KvrpcCheckTxnStatusRequest>,
    status_get_attempts: Vec<(u64, usize)>,
}

/// A store holding one value behind a lock that stays alive for the first
/// [`LOCKED_RESPONSES`] probes; the mock lives at the trait seam, so no
/// transport runs at all.
#[derive(Clone)]
struct LockingClient {
    reject_async: bool,
    initial_batch_barrier: Option<Arc<(Mutex<usize>, std::sync::Condvar)>>,
    keyed_batch_responses:
        Option<Arc<Mutex<std::collections::BTreeMap<Vec<u8>, KvrpcBatchGetResponse>>>>,
    remaining_locked: u64,
    request_ids: u64,
    recorded: Arc<Mutex<Recorded>>,
    batch_responses: std::collections::VecDeque<KvrpcBatchGetResponse>,
    get_responses: std::collections::VecDeque<KvrpcGetResponse>,
    scan_responses: std::collections::VecDeque<KvrpcScanResponse>,
    status_response: Option<KvrpcCheckTxnStatusResponse>,
    status_responses: std::collections::VecDeque<KvrpcCheckTxnStatusResponse>,
    recovery_barrier: Option<Arc<(Mutex<usize>, std::sync::Condvar)>>,
    status_hold: Option<Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>>,
    retry_hold: Option<Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>>,
    lock_batch_keys_once: Option<Arc<Mutex<std::collections::BTreeSet<Vec<u8>>>>>,
    first_batch_gate: Option<Arc<Mutex<Option<futures::channel::oneshot::Receiver<()>>>>>,
    status_ready: Option<Arc<Mutex<Option<futures::channel::oneshot::Sender<()>>>>>,
}

impl LockingClient {
    fn new(recorded: Arc<Mutex<Recorded>>) -> Self {
        Self {
            reject_async: false,
            initial_batch_barrier: None,
            keyed_batch_responses: None,
            remaining_locked: LOCKED_RESPONSES,
            request_ids: 0,
            recorded,
            batch_responses: Default::default(),
            get_responses: Default::default(),
            scan_responses: Default::default(),
            status_response: None,
            status_responses: Default::default(),
            recovery_barrier: None,
            status_hold: None,
            retry_hold: None,
            lock_batch_keys_once: None,
            first_batch_gate: None,
            status_ready: None,
        }
    }

    fn live_lock() -> KvrpcLockInfo {
        KvrpcLockInfo {
            primary_lock: ROW_KEY.to_vec(),
            lock_version: LOCK_TS,
            key: ROW_KEY.to_vec(),
            // Short, so the TTL-capped waits keep the whole test fast while
            // still exercising the `min(backoff, ttl)` arm.
            lock_ttl: 20,
            ..KvrpcLockInfo::default()
        }
    }

    fn respond<R>(&mut self, tag: BatchCommandTag, response: R) -> PublishedCommand<R> {
        self.request_ids += 1;
        PublishedCommand::Response(TransactionBatchResponse {
            response,
            publication: TransactionBatchPublication::in_process(tag, ADDRESS, self.request_ids),
        })
    }
}

/// Every command this regression never publishes answers as a pre-publication
/// refusal, so an unexpected call fails the test with its own name instead of
/// silently shaping the outcome.
macro_rules! never_published {
    ($self:ident, $name:literal) => {
        PublishedCommand::BeforePublication(
            concat!("this regression never publishes ", $name).to_owned(),
        )
    };
}

impl TransactionCommandClient for LockingClient {
    fn publish_transaction_get(
        &mut self,
        _address: &str,
        request: &KvrpcGetRequest,
        context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcGetResponse> {
        self.recorded
            .lock()
            .unwrap()
            .get_versions
            .push(request.version);
        self.recorded
            .lock()
            .unwrap()
            .get_contexts
            .push(context.clone());
        if request.key == b"transport-error" {
            return PublishedCommand::BeforePublication("injected transport failure".to_owned());
        }
        if let Some(response) = self.get_responses.pop_front() {
            return self.respond(BatchCommandTag::Get, response);
        }
        let response = if self.remaining_locked > 0 {
            self.remaining_locked -= 1;
            KvrpcGetResponse {
                error: Some(KvrpcKeyError {
                    locked: Some(Self::live_lock()),
                    ..KvrpcKeyError::default()
                }),
                ..KvrpcGetResponse::default()
            }
        } else if request.key == b"error" {
            KvrpcGetResponse {
                error: Some(KvrpcKeyError {
                    abort: "injected read failure".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }
        } else if request.key == b"missing" {
            KvrpcGetResponse::default()
        } else {
            KvrpcGetResponse {
                value: b"waited-out-value".to_vec(),
                ..KvrpcGetResponse::default()
            }
        };
        self.respond(BatchCommandTag::Get, response)
    }

    fn publish_transaction_batch_get(
        &mut self,
        _address: &str,
        request: &KvrpcBatchGetRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcBatchGetResponse> {
        self.recorded
            .lock()
            .unwrap()
            .batch_requests
            .push(request.clone());
        self.recorded
            .lock()
            .unwrap()
            .batch_contexts
            .push(context.clone());
        // Mirror production BatchGet admission: an RPC started after a held
        // status reply must still observe cancellation/deadline expiry.
        if call.cancellation().is_cancelled() || call.timeout().is_zero() {
            return PublishedCommand::BeforePublication("BatchGet admission cancelled".to_owned());
        }
        if let Some(barrier) = &self.initial_batch_barrier {
            let mut entered = barrier.0.lock().unwrap();
            *entered += 1;
            barrier.1.notify_all();
            let (entered, _) = barrier
                .1
                .wait_timeout_while(entered, Duration::from_secs(2), |entered| *entered < 2)
                .unwrap();
            if *entered < 2 {
                return PublishedCommand::BeforePublication(
                    "synchronous initial BatchGet workers did not overlap".to_owned(),
                );
            }
        }
        let keyed_response = self
            .keyed_batch_responses
            .as_ref()
            .and_then(|responses| responses.lock().unwrap().remove(&request.keys[0]));
        if let Some(response) = keyed_response {
            return self.respond(BatchCommandTag::BatchGet, response);
        }
        if let Some(response) = self.batch_responses.pop_front() {
            return self.respond(BatchCommandTag::BatchGet, response);
        }
        if let Some(hold) = &self.retry_hold {
            if let Err(error) = wait_for_worker_release(hold) {
                return PublishedCommand::BeforePublication(error.to_string());
            }
            if call.cancellation().is_cancelled() || call.timeout().is_zero() {
                return PublishedCommand::BeforePublication("held retry was cancelled".to_owned());
            }
        }
        if let Some(seen) = &self.lock_batch_keys_once {
            if seen.lock().unwrap().insert(request.keys[0].clone()) {
                return self.respond(
                    BatchCommandTag::BatchGet,
                    KvrpcBatchGetResponse {
                        error: Some(KvrpcKeyError {
                            locked: Some(KvrpcLockInfo {
                                key: request.keys[0].clone(),
                                primary_lock: request.keys[0].clone(),
                                lock_version: if request.keys[0].as_slice() < b"m".as_slice() {
                                    90
                                } else {
                                    91
                                },
                                ..Self::live_lock()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                );
            }
        }
        if request.keys.iter().any(|key| key == b"error") {
            return self.respond(
                BatchCommandTag::BatchGet,
                KvrpcBatchGetResponse {
                    error: Some(KvrpcKeyError {
                        abort: "injected read failure".to_owned(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }
        let pairs = request
            .keys
            .iter()
            .filter(|key| key.as_slice() != b"missing")
            .map(|key| (key.clone(), b"batch-value".to_vec()))
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_iter()
            .map(|(key, value)| tidb_proto::KvrpcKvPair {
                key,
                value,
                ..Default::default()
            })
            .collect();
        self.respond(
            BatchCommandTag::BatchGet,
            KvrpcBatchGetResponse {
                pairs,
                ..Default::default()
            },
        )
    }

    fn begin_transaction_batch_gets(
        &mut self,
        requests: &[tidb_txnkv::transaction::TransactionBatchGetRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<tidb_txnkv::transaction::TransactionBatchGetFuture> {
        self.recorded.lock().unwrap().async_batch_calls += 1;
        if self.reject_async {
            return requests
                .iter()
                .map(|_| {
                    Box::pin(futures::future::ready(PublishedCommand::BeforePublication(
                        "async BatchGet was disabled by the published configuration".to_owned(),
                    ))) as tidb_txnkv::transaction::TransactionBatchGetFuture
                })
                .collect();
        }
        let mut gate = self
            .first_batch_gate
            .as_ref()
            .and_then(|gate| gate.lock().unwrap().take());
        self.publish_transaction_batch_gets(requests, call)
            .into_iter()
            .map(|response| {
                let gate = gate.take();
                Box::pin(async move {
                    if let Some(gate) = gate {
                        let _ = gate.await;
                    }
                    response
                }) as tidb_txnkv::transaction::TransactionBatchGetFuture
            })
            .collect()
    }

    fn publish_transaction_scan(
        &mut self,
        _address: &str,
        request: &KvrpcScanRequest,
        context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcScanResponse> {
        self.recorded
            .lock()
            .unwrap()
            .scans
            .push((request.clone(), context.clone()));
        match self.scan_responses.pop_front() {
            Some(response) => self.respond(BatchCommandTag::Scan, response),
            None => never_published!(self, "Scan"),
        }
    }

    fn publish_prewrite(
        &mut self,
        _address: &str,
        _request: &KvrpcPrewriteRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPrewriteResponse> {
        never_published!(self, "Prewrite")
    }

    fn publish_commit(
        &mut self,
        _address: &str,
        _request: &KvrpcCommitRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcCommitResponse> {
        never_published!(self, "Commit")
    }

    fn publish_batch_rollback(
        &mut self,
        _address: &str,
        _request: &KvrpcBatchRollbackRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcBatchRollbackResponse> {
        never_published!(self, "BatchRollback")
    }

    fn publish_pessimistic_lock(
        &mut self,
        _address: &str,
        _request: &KvrpcPessimisticLockRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPessimisticLockResponse> {
        never_published!(self, "PessimisticLock")
    }

    fn publish_pessimistic_rollback(
        &mut self,
        _address: &str,
        _request: &KvrpcPessimisticRollbackRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPessimisticRollbackResponse> {
        never_published!(self, "PessimisticRollback")
    }

    fn publish_txn_heart_beat(
        &mut self,
        _address: &str,
        _request: &KvrpcTxnHeartBeatRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcTxnHeartBeatResponse> {
        never_published!(self, "TxnHeartBeat")
    }
}

impl LockRecoveryClient for LockingClient {
    fn check_txn_status_for_lock(
        &mut self,
        _address: &str,
        request: &KvrpcCheckTxnStatusRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        {
            let mut recorded = self.recorded.lock().unwrap();
            let attempts = recorded.get_versions.len();
            recorded
                .status_get_attempts
                .push((request.lock_ts, attempts));
            recorded.status_checks.push(request.clone());
        }
        if let Some(barrier) = &self.recovery_barrier {
            let mut entered = barrier.0.lock().unwrap();
            *entered += 1;
            barrier.1.notify_all();
            let (entered, _) = barrier
                .1
                .wait_timeout_while(entered, Duration::from_secs(2), |entered| *entered < 2)
                .unwrap();
            if *entered < 2 {
                return Err(DirectUnaryClientError::InvalidRequest(
                    "BatchGet retry workers did not overlap".to_owned(),
                ));
            }
        }
        if let Some(ready) = &self.status_ready {
            if let Some(ready) = ready.lock().unwrap().take() {
                let _ = ready.send(());
            }
        }
        if let Some(hold) = &self.status_hold {
            wait_for_worker_release(hold)?;
        }
        if let Some(response) = self.status_responses.pop_front() {
            return Ok(response);
        }
        if let Some(response) = &self.status_response {
            return Ok(response.clone());
        }
        // The blocking transaction is alive: a positive `lock_ttl` and no
        // commit version, which is what makes the reader WAIT rather than
        // resolve.
        Ok(KvrpcCheckTxnStatusResponse {
            lock_ttl: 20,
            ..KvrpcCheckTxnStatusResponse::default()
        })
    }

    fn check_secondary_locks_for_lock(
        &mut self,
        _address: &str,
        _request: &KvrpcCheckSecondaryLocksRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
        panic!("a live primary lock never needs its secondaries checked here");
    }

    fn resolve_lock_for_read(
        &mut self,
        _address: &str,
        _request: &KvrpcResolveLockRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
        assert!(
            self.status_response.is_some(),
            "an alive lock is waited out, never resolved"
        );
        Ok(KvrpcResolveLockResponse::default())
    }

    fn pessimistic_rollback_for_lock(
        &mut self,
        _address: &str,
        _request: &KvrpcPessimisticRollbackRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        panic!("no pessimistic lock exists in this regression");
    }
}

/// A live lock that outlasts the removed four-attempt cap is WAITED OUT: the
/// read keeps probing under Go's time budget and answers the value the lock
/// released. Before the `BoTxnLockFast` port this failed on the fifth probe
/// with "snapshot lock retry budget exhausted".
#[test]
fn a_snapshot_read_waits_out_a_live_lock_beyond_four_attempts() {
    let _config = snapshot_test_config();
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let runtime = SharedReadRuntime::new_injected(
        LockingClient::new(Arc::clone(&recorded)),
        RegionCache::new(OneRegion),
    );
    let mut transaction = RealOptimisticTransaction::new_injected(
        runtime,
        TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4 * 1024,
    )
    .unwrap();
    let call = UnaryCallContext::with_timeout(CALL_TIMEOUT);

    let read = transaction
        .snapshot_get(ROW_KEY, &call)
        .expect("the read waits the lock out instead of exhausting a counter");
    assert_eq!(read.value.as_deref(), Some(b"waited-out-value".as_slice()));

    let observations = recorded.lock().unwrap();
    assert_eq!(
        observations.get_versions.len() as u64,
        LOCKED_RESPONSES + 1,
        "one probe per locked answer plus the final read"
    );
    assert!(
        observations
            .get_versions
            .iter()
            .all(|version| *version == START_TS),
        "every probe reads at the transaction's one timestamp"
    );
    assert!(
        !observations.status_checks.is_empty(),
        "a locked probe consults the blocking transaction's status"
    );
    assert!(
        observations
            .status_checks
            .iter()
            .all(|check| check.lock_ts == LOCK_TS && check.primary_key == ROW_KEY),
        "the status question names the lock's own transaction and primary"
    );
    drop(observations);

    // Go KVSnapshot.Get/BatchGet share a timestamp-scoped cache, including
    // not-found results. A lock retry only fills it after the final read.
    let mut cached = transaction.snapshot_get(ROW_KEY, &call).unwrap();
    assert_eq!(
        cached.rpc_count, 0,
        "a repeated snapshot read must not publish another Get"
    );
    assert!(cached.region.is_none() && cached.publication.is_none());
    cached.value.as_mut().unwrap().clear();
    assert_eq!(
        transaction.snapshot_get(ROW_KEY, &call).unwrap().value,
        read.value
    );
    let keys = vec![
        ROW_KEY.to_vec(),
        b"other".to_vec(),
        b"missing".to_vec(),
        b"other".to_vec(),
    ];
    let values = transaction
        .snapshot_batch_get(&keys, &call)
        .unwrap()
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(values.get(ROW_KEY), Some(&b"waited-out-value".to_vec()));
    assert_eq!(
        values.get(b"other".as_slice()),
        Some(&b"batch-value".to_vec())
    );
    assert!(!values.contains_key(b"missing".as_slice()));
    assert_eq!(
        recorded.lock().unwrap().batch_requests[0].keys,
        vec![b"missing".to_vec(), b"other".to_vec()]
    );
    let counts = transaction.snapshot_point_rpc_counts();
    transaction.snapshot_batch_get(&keys, &call).unwrap();
    assert_eq!(
        transaction.snapshot_get(b"missing", &call).unwrap().value,
        None
    );
    assert_eq!(
        transaction.snapshot_get(b"other", &call).unwrap().value,
        Some(b"batch-value".to_vec())
    );
    assert_eq!(transaction.snapshot_point_rpc_counts(), counts);
    assert_eq!(
        transaction.snapshot_batch_get(&keys, &call).unwrap().len(),
        2
    );
    // A failed mixed batch must not return only its cached portion.
    for _ in 0..2 {
        assert!(transaction.snapshot_get(b"error", &call).is_err());
        assert!(transaction
            .snapshot_batch_get(&[ROW_KEY.to_vec(), b"error".to_vec()], &call)
            .is_err());
    }
    assert_eq!(
        transaction.snapshot_point_rpc_counts(),
        (counts.0 + 2, counts.1 + 2)
    );
    transaction
        .snapshot_scan_at(b"a", b"z", Some(0), START_TS + 1, &call)
        .unwrap();
    assert_eq!(
        transaction.snapshot_get(ROW_KEY, &call).unwrap().rpc_count,
        1
    );

    // For-update/read timestamps may move in either direction. Neither image
    // may reuse the other, and MaxUint64 reads never populate the cache.
    for ts in [START_TS + 1, START_TS, u64::MAX, u64::MAX] {
        assert_eq!(
            transaction
                .snapshot_get_at(ROW_KEY, ts, &call)
                .unwrap()
                .rpc_count,
            1
        );
    }
    let before = transaction.snapshot_point_rpc_counts();
    for _ in 0..2 {
        transaction
            .snapshot_batch_get_at(&keys, u64::MAX, &call)
            .unwrap();
    }
    assert_eq!(transaction.snapshot_point_rpc_counts().1, before.1 + 2);
    transaction
        .snapshot_get_at(b"missing", START_TS, &call)
        .unwrap();
    assert_eq!(
        transaction
            .snapshot_get_at(b"missing", START_TS, &call)
            .unwrap()
            .rpc_count,
        0
    );
    assert_batch_read_limits_and_pending_retries();
}

fn assert_batch_read_limits_and_pending_retries() {
    // Go snapshot.go: batchGetSize, collectBatchGetResponseData, and
    // batchGetSingleRegion. Reuse the existing injected client/region seam.
    let fixture = |responses: Vec<KvrpcBatchGetResponse>| {
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let mut client = LockingClient::new(Arc::clone(&recorded));
        client.remaining_locked = 0;
        client.batch_responses = responses.into();
        let transaction = RealOptimisticTransaction::new_injected(
            SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion)),
            TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
            CALL_TIMEOUT,
            START_TS,
            Instant::now(),
            4,
            4096,
        )
        .unwrap();
        (transaction, recorded)
    };
    let call = UnaryCallContext::with_timeout(CALL_TIMEOUT);
    let wide_keys: Vec<_> = (0..5121).map(|i| format!("{i:064}").into_bytes()).collect();
    let (mut transaction, recorded) = fixture(Vec::new());
    assert_eq!(
        transaction
            .snapshot_batch_get(&wide_keys, &call)
            .unwrap()
            .len(),
        5121
    );
    assert_eq!(
        recorded
            .lock()
            .unwrap()
            .batch_requests
            .iter()
            .map(|request| request.keys.len())
            .collect::<Vec<_>>(),
        vec![5120, 1],
        "snapshot reads use Go's key-count limit, not commit's byte limit"
    );
    // Short keys expose the opposite bug: a byte cap can admit MORE than
    // Go's maximum key count in one request.
    let short_keys: Vec<_> = (0..5121_u16).map(|i| i.to_be_bytes().to_vec()).collect();
    let (mut transaction, recorded) = fixture(Vec::new());
    assert_eq!(
        transaction
            .snapshot_batch_get(&short_keys, &call)
            .unwrap()
            .len(),
        5121
    );
    assert_eq!(
        recorded
            .lock()
            .unwrap()
            .batch_requests
            .iter()
            .map(|request| request.keys.len())
            .collect::<Vec<_>>(),
        vec![5120, 1]
    );

    let keys = vec![b"a".to_vec(), b"b".to_vec(), b"missing".to_vec()];
    let lock = KvrpcKeyError {
        locked: Some(KvrpcLockInfo {
            key: b"b".to_vec(),
            ..LockingClient::live_lock()
        }),
        ..Default::default()
    };
    for response_level in [false, true] {
        let mut pairs = vec![tidb_proto::KvrpcKvPair {
            key: b"a".to_vec(),
            value: b"first-value".to_vec(),
            ..Default::default()
        }];
        if !response_level {
            // A pair can omit its outer key; the lock owns the retry key.
            pairs.push(tidb_proto::KvrpcKvPair {
                error: Some(lock.clone()),
                ..Default::default()
            });
        }
        let (mut transaction, recorded) = fixture(vec![KvrpcBatchGetResponse {
            error: response_level.then(|| lock.clone()),
            pairs,
            ..Default::default()
        }]);
        let values = transaction
            .snapshot_batch_get(&keys, &call)
            .unwrap()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(values.len(), 2);
        assert_eq!(
            values.get(b"a".as_slice()).unwrap().as_slice(),
            if response_level {
                b"batch-value".as_slice()
            } else {
                b"first-value".as_slice()
            }
        );
        let requests = &recorded.lock().unwrap().batch_requests;
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1].keys,
            if response_level {
                keys.clone()
            } else {
                vec![b"b".to_vec()]
            }
        );
    }

    // A retryable region error in the first physical batch must not throw
    // away the successful second batch from the same publication round.
    let (mut transaction, recorded) = fixture(vec![
        KvrpcBatchGetResponse {
            region_error: Some(tidb_proto::errorpb::Error {
                server_is_busy: Some(tidb_proto::errorpb::ServerIsBusy {
                    reason: "injected retry".to_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        KvrpcBatchGetResponse {
            pairs: vec![tidb_proto::KvrpcKvPair {
                key: wide_keys[5120].clone(),
                value: b"completed-batch".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        },
    ]);
    let values = transaction
        .snapshot_batch_get(&wide_keys, &call)
        .unwrap()
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(values.len(), wide_keys.len());
    assert_eq!(
        values.get(&wide_keys[5120]),
        Some(&b"completed-batch".to_vec())
    );
    assert_eq!(
        recorded
            .lock()
            .unwrap()
            .batch_requests
            .iter()
            .map(|request| request.keys.len())
            .collect::<Vec<_>>(),
        vec![5120, 1, 5120]
    );
    assert_eq!(transaction.snapshot_point_rpc_counts(), (0, 3));
    assert_eq!(
        transaction
            .snapshot_batch_get(&wide_keys, &call)
            .unwrap()
            .len(),
        5121
    );
    assert_eq!(
        transaction.snapshot_point_rpc_counts(),
        (0, 3),
        "cache every completed batch, not only the last retry"
    );

    // A fatal later batch must not populate the cache with a successful
    // prefix from an operation that returned an error.
    let (mut transaction, _) = fixture(vec![
        KvrpcBatchGetResponse {
            pairs: vec![tidb_proto::KvrpcKvPair {
                key: wide_keys[0].clone(),
                value: b"uncommitted-read-result".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        },
        KvrpcBatchGetResponse {
            error: Some(KvrpcKeyError {
                abort: "injected fatal batch".to_owned(),
                ..Default::default()
            }),
            ..Default::default()
        },
    ]);
    assert!(transaction.snapshot_batch_get(&wide_keys, &call).is_err());
    assert_eq!(
        transaction
            .snapshot_get(&wide_keys[0], &call)
            .unwrap()
            .rpc_count,
        1
    );
}

#[test]
fn scan_pair_locks_use_point_get_without_replaying_clean_rows() {
    let _config = snapshot_test_config();
    for shared in [false, true] {
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let mut client = LockingClient::new(Arc::clone(&recorded));
        client.remaining_locked = 0;
        client.status_response = Some(KvrpcCheckTxnStatusResponse {
            commit_version: START_TS,
            ..Default::default()
        });
        let lock = LockingClient::live_lock();
        let lock = if shared {
            KvrpcLockInfo {
                key: ROW_KEY.to_vec(),
                shared_lock_infos: vec![lock],
                ..Default::default()
            }
        } else {
            lock
        };
        client.scan_responses.push_back(KvrpcScanResponse {
            pairs: vec![
                tidb_proto::KvrpcKvPair {
                    key: b"a".to_vec(),
                    value: b"clean-prefix".to_vec(),
                    ..Default::default()
                },
                tidb_proto::KvrpcKvPair {
                    error: Some(KvrpcKeyError {
                        locked: Some(KvrpcLockInfo {
                            key: b"missing".to_vec(),
                            ..lock.clone()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                tidb_proto::KvrpcKvPair {
                    error: Some(KvrpcKeyError {
                        locked: Some(lock),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                tidb_proto::KvrpcKvPair {
                    key: b"z".to_vec(),
                    value: b"clean-suffix".to_vec(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
        let mut transaction = RealOptimisticTransaction::new_injected(
            SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion)),
            TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
            CALL_TIMEOUT,
            START_TS,
            Instant::now(),
            4,
            4096,
        )
        .unwrap();
        assert_eq!(
            transaction
                .snapshot_scan(
                    b"a",
                    b"zz",
                    None,
                    &UnaryCallContext::with_timeout(CALL_TIMEOUT)
                )
                .unwrap(),
            vec![
                (b"a".to_vec(), b"clean-prefix".to_vec()),
                (ROW_KEY.to_vec(), b"waited-out-value".to_vec()),
                (b"z".to_vec(), b"clean-suffix".to_vec()),
            ]
        );
        let recorded = recorded.lock().unwrap();
        assert_eq!(recorded.scans.len(), 1, "keep the original clean rows");
        assert_eq!(recorded.get_versions, vec![START_TS; 2]);
        assert!(
            recorded.status_checks.is_empty(),
            "pair errors are reread through Get before resolving"
        );
    }
}

#[test]
fn snapshot_get_and_batch_get_back_off_ignored_request_hints() {
    let _config = snapshot_test_config();
    for (batch, fail) in [(false, false), (false, true), (true, false), (true, true)] {
        for committed in [false, true] {
            let recorded = Arc::new(Mutex::new(Recorded::default()));
            let mut client = LockingClient::new(Arc::clone(&recorded));
            client.remaining_locked = 0;
            client.status_response = Some(KvrpcCheckTxnStatusResponse {
                commit_version: if committed { START_TS } else { START_TS + 1 },
                ..Default::default()
            });
            let error = KvrpcKeyError {
                locked: Some(LockingClient::live_lock()),
                ..Default::default()
            };
            for _ in 0..7 {
                client.get_responses.push_back(KvrpcGetResponse {
                    error: Some(error.clone()),
                    ..Default::default()
                });
                client.batch_responses.push_back(KvrpcBatchGetResponse {
                    error: Some(error.clone()),
                    ..Default::default()
                });
            }
            if fail {
                let error = Some(KvrpcKeyError {
                    abort: "after backoff".into(),
                    ..Default::default()
                });
                client.get_responses.push_back(KvrpcGetResponse {
                    error: error.clone(),
                    ..Default::default()
                });
                client.batch_responses.push_back(KvrpcBatchGetResponse {
                    error,
                    ..Default::default()
                });
            }
            let mut transaction = RealOptimisticTransaction::new_injected(
                SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion)),
                TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
                CALL_TIMEOUT,
                START_TS,
                Instant::now(),
                4,
                4096,
            )
            .unwrap();
            let stats = Arc::new(tikv_client::SnapshotRuntimeStats::new());
            transaction.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));
            let started = Instant::now();
            if batch {
                let result = transaction.snapshot_batch_get(
                    &[ROW_KEY.to_vec()],
                    &UnaryCallContext::with_timeout(CALL_TIMEOUT),
                );
                if fail {
                    assert!(result.is_err());
                } else {
                    assert_eq!(result.unwrap().len(), 1);
                }
            } else {
                let result = transaction
                    .snapshot_get(ROW_KEY, &UnaryCallContext::with_timeout(CALL_TIMEOUT));
                if fail {
                    assert!(result.is_err());
                } else {
                    assert!(result.unwrap().value.is_some());
                }
            }
            assert_eq!(stats.backoff_count("txnLockFast"), 6);
            assert!(stats.backoff_duration("txnLockFast") >= Duration::from_millis(63));
            assert!(stats.resolve_lock_duration() > Duration::ZERO);
            assert!(
                stats.rpc_duration(tikv_client::SnapshotRpcCommand::ResolveLock)
                    >= stats.resolve_lock_duration() + stats.backoff_duration("txnLockFast")
            );
            let recorded = recorded.lock().unwrap();
            let contexts = if batch {
                &recorded.batch_contexts
            } else {
                &recorded.get_contexts
            };
            assert_eq!(contexts.len(), 8);
            assert!(
                contexts[0].resolved_locks.is_empty() && contexts[0].committed_locks.is_empty()
            );
            assert!(contexts[1..].iter().all(|ctx| if committed {
                ctx.committed_locks == [LOCK_TS]
            } else {
                ctx.resolved_locks == [LOCK_TS]
            }));
            // Six equal-jitter draws have lower bounds 1+2+4+8+16+32ms.
            assert!(started.elapsed() >= Duration::from_millis(63), "ignored hints must consume the actual retry wait: batch={batch}, committed={committed}");
        }
    }
}

#[test]
fn batch_get_retries_keep_physical_response_boundaries() {
    let _config = snapshot_test_config();
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let mut client = LockingClient::new(Arc::clone(&recorded));
    client.remaining_locked = 0;
    client.status_response = Some(KvrpcCheckTxnStatusResponse {
        commit_version: START_TS,
        ..Default::default()
    });
    let keys: Vec<_> = (0..5121).map(|i| format!("{i:05}").into_bytes()).collect();
    let locked_pair = |key: Vec<u8>| tidb_proto::KvrpcKvPair {
        error: Some(KvrpcKeyError {
            locked: Some(KvrpcLockInfo {
                key,
                ..LockingClient::live_lock()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut first: Vec<_> = keys[1..5120]
        .iter()
        .map(|key| tidb_proto::KvrpcKvPair {
            key: key.clone(),
            value: b"clean".to_vec(),
            ..Default::default()
        })
        .collect();
    first.push(locked_pair(keys[0].clone()));
    client.batch_responses = [
        KvrpcBatchGetResponse {
            pairs: first,
            ..Default::default()
        },
        KvrpcBatchGetResponse {
            pairs: vec![locked_pair(keys[5120].clone())],
            ..Default::default()
        },
    ]
    .into();
    let mut transaction = RealOptimisticTransaction::new_injected(
        SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion)),
        TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4096,
    )
    .unwrap();
    let values = transaction
        .snapshot_batch_get(&keys, &UnaryCallContext::with_timeout(CALL_TIMEOUT))
        .unwrap();
    assert_eq!(values.len(), keys.len());
    assert_eq!(
        recorded
            .lock()
            .unwrap()
            .batch_requests
            .iter()
            .map(|r| r.keys.len())
            .collect::<Vec<_>>(),
        vec![5120, 1, 1, 1]
    );
    assert_eq!(values.iter().filter(|(_, v)| v == b"clean").count(), 5119);
}

#[test]
fn scan_response_locks_wait_without_stamping_read_hints() {
    let _config = snapshot_test_config();
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let mut client = LockingClient::new(Arc::clone(&recorded));
    client.remaining_locked = 0;
    client.status_response = Some(KvrpcCheckTxnStatusResponse {
        lock_ttl: 20,
        action: tidb_proto::KvrpcTxnAction::MinCommitTsPushed as i32,
        ..Default::default()
    });
    client.scan_responses = [
        KvrpcScanResponse {
            error: Some(KvrpcKeyError {
                locked: Some(LockingClient::live_lock()),
                ..Default::default()
            }),
            ..Default::default()
        },
        KvrpcScanResponse {
            pairs: vec![tidb_proto::KvrpcKvPair {
                key: ROW_KEY.to_vec(),
                value: b"resolved".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        },
    ]
    .into();
    let mut transaction = RealOptimisticTransaction::new_injected(
        SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion)),
        TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4096,
    )
    .unwrap();
    let stats = Arc::new(tikv_client::SnapshotRuntimeStats::new());
    transaction.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));
    assert_eq!(
        transaction
            .snapshot_scan(
                b"a",
                b"z",
                None,
                &UnaryCallContext::with_timeout(CALL_TIMEOUT)
            )
            .unwrap(),
        vec![(ROW_KEY.to_vec(), b"resolved".to_vec())]
    );
    assert_eq!(
        stats.rpc_count(tikv_client::SnapshotRpcCommand::ResolveLock),
        0
    );
    assert_eq!(stats.backoff_count("txnLockFast"), 0);
    assert_eq!(stats.resolve_lock_duration(), Duration::ZERO);
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.scans.len(), 2);
    assert!(recorded
        .scans
        .iter()
        .all(|(_, ctx)| ctx.resolved_locks.is_empty() && ctx.committed_locks.is_empty()));
    assert!(
        recorded.get_versions.is_empty(),
        "a response-level error retries the full scan page"
    );
}

#[test]
fn max_ts_get_only_skips_new_unhinted_transactions_after_its_first_lock() {
    let _config = snapshot_test_config();
    for scan_pair in [false, true] {
        for read_ts in [START_TS, u64::MAX] {
            let recorded = Arc::new(Mutex::new(Recorded::default()));
            let mut client = LockingClient::new(Arc::clone(&recorded));
            client.remaining_locked = 0;
            client.status_response = Some(KvrpcCheckTxnStatusResponse {
                commit_version: START_TS,
                ..Default::default()
            });
            for lock_ts in [90, 91, 91, 90, 92] {
                client.get_responses.push_back(KvrpcGetResponse {
                    error: Some(KvrpcKeyError {
                        locked: Some(KvrpcLockInfo {
                            lock_version: lock_ts,
                            ..LockingClient::live_lock()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
            }
            if scan_pair {
                client.scan_responses.push_back(KvrpcScanResponse {
                    pairs: vec![tidb_proto::KvrpcKvPair {
                        key: ROW_KEY.to_vec(),
                        error: Some(KvrpcKeyError {
                            locked: Some(LockingClient::live_lock()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                });
            }
            let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion));
            let mut transaction = RealOptimisticTransaction::new_injected(
                runtime.clone(),
                TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
                CALL_TIMEOUT,
                START_TS,
                Instant::now(),
                4,
                4096,
            )
            .unwrap();
            let call = UnaryCallContext::with_timeout(CALL_TIMEOUT);
            if scan_pair {
                assert_eq!(
                    transaction
                        .snapshot_scan_at(b"a", b"z", None, read_ts, &call)
                        .unwrap(),
                    vec![(ROW_KEY.to_vec(), b"waited-out-value".to_vec())]
                );
            } else {
                assert_eq!(
                    transaction
                        .snapshot_get_at(ROW_KEY, read_ts, &call)
                        .unwrap()
                        .value,
                    Some(b"waited-out-value".to_vec())
                );
            }
            {
                let recorded = recorded.lock().unwrap();
                assert_eq!(recorded.get_contexts.len(), 6);
                assert_eq!(recorded.get_contexts[1].committed_locks, vec![90]);
                if read_ts == u64::MAX {
                    assert_eq!(
                        recorded.get_contexts[2].resolved_locks,
                        vec![91],
                        "new lock is skipped before consulting its status"
                    );
                    assert_eq!(recorded.get_contexts[2].committed_locks, vec![90]);
                    assert_eq!(
                        recorded.get_contexts[3].committed_locks,
                        vec![90, 91],
                        "ignored sent hint must fall through to resolution"
                    );
                    assert_eq!(recorded.get_contexts[5].resolved_locks, vec![91, 92]);
                    assert!(recorded.status_get_attempts.contains(&(90, 1)));
                    assert!(recorded.status_get_attempts.contains(&(91, 3)));
                    assert!(!recorded.status_get_attempts.iter().any(|(ts, _)| *ts == 92));
                } else {
                    assert!(recorded
                        .get_contexts
                        .iter()
                        .all(|ctx| ctx.resolved_locks.is_empty()));
                    assert!(recorded.status_get_attempts.contains(&(91, 2)));
                    assert!(recorded.status_get_attempts.contains(&(92, 5)));
                }
            }
            // A later Get chooses its own first transaction, even while the
            // snapshot retains resolved/committed sets from the preceding Get.
            runtime
                .client()
                .lock()
                .unwrap()
                .get_responses
                .push_back(KvrpcGetResponse {
                    error: Some(KvrpcKeyError {
                        locked: Some(KvrpcLockInfo {
                            lock_version: 93,
                            ..LockingClient::live_lock()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
            transaction
                .snapshot_get_at(b"another-key", read_ts, &call)
                .unwrap();
            assert!(recorded
                .lock()
                .unwrap()
                .status_get_attempts
                .contains(&(93, 7)));
        }
    }
}

#[test]
fn batch_get_lock_recovery_workers_make_independent_progress() {
    let _config = snapshot_test_config();
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let mut client = LockingClient::new(Arc::clone(&recorded));
    client.remaining_locked = 0;
    client.status_response = Some(KvrpcCheckTxnStatusResponse {
        commit_version: START_TS,
        ..Default::default()
    });
    client.recovery_barrier = Some(Arc::new((Mutex::new(0), std::sync::Condvar::new())));
    let keys: Vec<_> = (0..5121).map(|i| format!("{i:05}").into_bytes()).collect();
    for (key, lock_version) in [(&keys[0], 90), (&keys[5120], 91)] {
        client.batch_responses.push_back(KvrpcBatchGetResponse {
            error: Some(KvrpcKeyError {
                locked: Some(KvrpcLockInfo {
                    key: key.clone(),
                    primary_lock: key.clone(),
                    lock_version,
                    ..LockingClient::live_lock()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    let mut transaction = RealOptimisticTransaction::new_injected(
        SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion)),
        TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4096,
    )
    .unwrap();
    let values = transaction
        .snapshot_batch_get(&keys, &UnaryCallContext::with_timeout(CALL_TIMEOUT))
        .unwrap();
    assert_eq!(values.len(), keys.len());
    assert_eq!(transaction.snapshot_point_rpc_counts(), (0, 4));
    let mut requests = recorded
        .lock()
        .unwrap()
        .batch_requests
        .iter()
        .map(|request| request.keys.len())
        .collect::<Vec<_>>();
    requests.sort_unstable();
    assert_eq!(requests, vec![1, 1, 5120, 5120]);
}

fn wait_for_worker_release(
    hold: &Arc<(Mutex<(bool, bool)>, std::sync::Condvar)>,
) -> Result<(), DirectUnaryClientError> {
    let mut state = hold.0.lock().unwrap();
    state.0 = true;
    hold.1.notify_all();
    // Retain the RPC even after cancellation, as Go's original paused worker
    // does, to distinguish joining from merely sending a cancel signal.
    let (state, _) = hold
        .1
        .wait_timeout_while(state, Duration::from_secs(2), |state| !state.1)
        .unwrap();
    if !state.1 {
        return Err(DirectUnaryClientError::InvalidRequest(
            "worker release timed out".to_owned(),
        ));
    }
    Ok(())
}

fn two_batch_worker_fixture() -> (LockingClient, Vec<Vec<u8>>) {
    let mut client = LockingClient::new(Arc::new(Mutex::new(Recorded::default())));
    client.remaining_locked = 0;
    client.status_response = Some(KvrpcCheckTxnStatusResponse {
        commit_version: START_TS,
        ..Default::default()
    });
    let keys: Vec<_> = (0..5121).map(|i| format!("{i:05}").into_bytes()).collect();
    client.keyed_batch_responses = Some(Arc::new(Mutex::new(
        [
            (keys[0].clone(), KvrpcBatchGetResponse::default()),
            (
                keys[5120].clone(),
                KvrpcBatchGetResponse {
                    error: Some(KvrpcKeyError {
                        locked: Some(KvrpcLockInfo {
                            key: keys[5120].clone(),
                            primary_lock: keys[5120].clone(),
                            ..LockingClient::live_lock()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
        ]
        .into(),
    )));
    (client, keys)
}

#[test]
fn batch_get_recovery_starts_before_an_earlier_rpc_completes() {
    let _config = snapshot_test_config();
    let (mut client, keys) = two_batch_worker_fixture();
    let (ready, gate) = futures::channel::oneshot::channel();
    client.first_batch_gate = Some(Arc::new(Mutex::new(Some(gate))));
    client.status_ready = Some(Arc::new(Mutex::new(Some(ready))));
    let mut transaction = RealOptimisticTransaction::new_injected(
        SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion)),
        TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4096,
    )
    .unwrap();
    // Batch 1 completes only after batch 2's retry enters CheckTxnStatus.
    // Waiting in request order consumes the deadline without making progress.
    let values = transaction
        .snapshot_batch_get(
            &keys,
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        )
        .unwrap();
    assert_eq!(values, vec![(keys[5120].clone(), b"batch-value".to_vec())]);
    assert_eq!(transaction.snapshot_point_rpc_counts(), (0, 3));
}

#[test]
fn batch_get_joins_retry_workers_before_returning_cancellation_or_sibling_error() {
    let _config = snapshot_test_config();
    for enable_async in [false, true] {
        let mut config = tidb_config::config_tree::new_config();
        config.performance.enable_async_batch_get = enable_async;
        tidb_config::config_tree::config::store_global_config(config);
        for hold_retry in [false, true] {
            for mode in ["cancel", "deadline", "sibling-error"] {
                let (mut client, keys) = two_batch_worker_fixture();
                let hold = Arc::new((Mutex::new((false, false)), std::sync::Condvar::new()));
                if hold_retry {
                    client.retry_hold = Some(Arc::clone(&hold));
                } else {
                    client.status_hold = Some(Arc::clone(&hold));
                }
                if mode == "sibling-error" {
                    client
                        .keyed_batch_responses
                        .as_ref()
                        .unwrap()
                        .lock()
                        .unwrap()
                        .get_mut(&keys[0])
                        .unwrap()
                        .error = Some(KvrpcKeyError {
                        abort: "fatal sibling response".to_owned(),
                        ..Default::default()
                    });
                }
                let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion));
                let mut transaction = RealOptimisticTransaction::new_injected(
                    runtime.clone(),
                    TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
                    CALL_TIMEOUT,
                    START_TS,
                    Instant::now(),
                    4,
                    4096,
                )
                .unwrap();
                let call = UnaryCallContext::with_timeout(if mode == "deadline" {
                    Duration::from_millis(100)
                } else {
                    CALL_TIMEOUT
                });
                let cancel = call.cancellation().clone();
                let (done, completed) = std::sync::mpsc::channel();
                let worker = std::thread::spawn(move || {
                    let result = transaction.snapshot_batch_get(&keys, &call);
                    done.send(()).unwrap();
                    (transaction, result)
                });
                let state = hold.0.lock().unwrap();
                let (state, _) = hold
                    .1
                    .wait_timeout_while(state, Duration::from_secs(2), |state| !state.0)
                    .unwrap();
                assert!(
                    state.0,
                    "retry worker must enter the held status RPC: {mode}"
                );
                drop(state);
                let active_locks = runtime.resolving_locks();
                if mode == "cancel" {
                    cancel.cancel();
                }
                let wait = if mode == "deadline" {
                    Duration::from_millis(150)
                } else {
                    Duration::from_millis(30)
                };
                let early = completed.recv_timeout(wait);
                hold.0.lock().unwrap().1 = true;
                hold.1.notify_all();
                let (mut transaction, result) = worker.join().unwrap();
                assert_eq!(active_locks.len(), 1,
                "the worker retains one resolving record through status, backoff and retry RPC: retry={hold_retry}, mode={mode}, async={enable_async}");
                assert!(
                    early.is_err(),
                    "BatchGet returned while its retry worker still owned request state: {mode}"
                );
                assert!(result.is_err(), "{mode}");
                if mode == "sibling-error" {
                    assert!(result
                        .unwrap_err()
                        .to_string()
                        .contains("fatal sibling response"));
                    assert_eq!(
                        transaction.snapshot_point_rpc_counts(),
                        (0, 3),
                        "sibling recovery was allowed to finish"
                    );
                }
                assert!(
                    runtime.resolving_locks().is_empty(),
                    "worker guard released before return"
                );
                let before = transaction.snapshot_point_rpc_counts();
                assert_eq!(
                    transaction
                        .snapshot_get(b"05120", &UnaryCallContext::with_timeout(CALL_TIMEOUT))
                        .unwrap()
                        .rpc_count,
                    1,
                    "an unsuccessful BatchGet cannot populate the snapshot cache"
                );
                assert_eq!(transaction.snapshot_point_rpc_counts().0, before.0 + 1);
            }
        }
    }
}

#[test]
fn batch_get_split_retries_run_children_independently() {
    let _config = snapshot_test_config();
    struct SplitRegion;
    impl RegionLoader for SplitRegion {
        fn cluster_id(&self) -> u64 {
            11
        }
        fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
            OneRegion.load_region(key)
        }
    }
    impl RegionRecoveryLoader for SplitRegion {
        fn hydrate_region(
            &mut self,
            metadata: &RegionMetadata,
            _: u64,
            _: &mut std::collections::BTreeMap<u64, Option<tidb_txnkv::region::StoreMetadata>>,
        ) -> Result<RegionLocation, RegionLoadError> {
            let mut region = OneRegion.load_region(b"")?;
            region.region = metadata.region;
            let decode = |key: &[u8]| {
                if key.is_empty() {
                    Vec::new()
                } else {
                    tidb_codec::decode_bytes(key).unwrap().1
                }
            };
            region.start_key = decode(&metadata.encoded_start_key);
            region.end_key = decode(&metadata.encoded_end_key);
            Ok(region)
        }
    }
    let (mut client, _) = two_batch_worker_fixture();
    client.lock_batch_keys_once = Some(Arc::new(Mutex::new(Default::default())));
    client.recovery_barrier = Some(Arc::new((Mutex::new(0), std::sync::Condvar::new())));
    let mut middle = Vec::new();
    tidb_codec::encode_bytes(&mut middle, b"m");
    let region = |id, start_key, end_key| tidb_proto::metapb::Region {
        id,
        start_key,
        end_key,
        region_epoch: Some(tidb_proto::metapb::RegionEpoch {
            conf_ver: 1,
            version: 2,
        }),
        peers: vec![tidb_proto::metapb::Peer {
            id: 620,
            store_id: 6200,
            ..Default::default()
        }],
        ..Default::default()
    };
    client.batch_responses = [KvrpcBatchGetResponse {
        region_error: Some(tidb_proto::errorpb::Error {
            epoch_not_match: Some(tidb_proto::errorpb::EpochNotMatch {
                current_regions: vec![
                    region(63, Vec::new(), middle.clone()),
                    region(64, middle, Vec::new()),
                ],
            }),
            ..Default::default()
        }),
        ..Default::default()
    }]
    .into();
    let mut transaction = RealOptimisticTransaction::new_injected(
        SharedReadRuntime::new_injected(client, RegionCache::new(SplitRegion)),
        TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4096,
    )
    .unwrap();
    let values = transaction
        .snapshot_batch_get(
            &[b"a".to_vec(), b"z".to_vec()],
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .unwrap()
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(values.len(), 2);
    assert_eq!(values[b"a".as_slice()], b"batch-value");
    assert_eq!(values[b"z".as_slice()], b"batch-value");
    assert_eq!(transaction.snapshot_point_rpc_counts(), (0, 5));
}

#[test]
fn batch_get_reads_the_published_async_setting_on_every_call() {
    let _config = snapshot_test_config();
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let client = LockingClient::new(Arc::clone(&recorded));
    let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion));
    let mut transaction = RealOptimisticTransaction::new_injected(
        runtime.clone(),
        TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4096,
    )
    .unwrap();
    let keys: Vec<_> = (0..5121).map(|i| format!("{i:05}").into_bytes()).collect();
    let mut read_ts = START_TS;
    let mut expected_async_calls = 0;
    for enable_async in [false, true, false] {
        // Publish through the same owner as server startup, after the
        // transaction exists. Each operation must observe the current flag.
        let mut config = tidb_config::config_tree::new_config();
        config.performance.enable_async_batch_get = enable_async;
        tidb_config::config_tree::config::store_global_config(config);
        let barrier = Arc::new((Mutex::new(0), std::sync::Condvar::new()));
        {
            let mut client = runtime.client().lock().unwrap();
            client.reject_async = !enable_async;
            client.initial_batch_barrier = (!enable_async).then(|| Arc::clone(&barrier));
        }
        let values = transaction
            .snapshot_batch_get_at(
                &keys,
                read_ts,
                &UnaryCallContext::with_timeout(CALL_TIMEOUT),
            )
            .unwrap();
        assert_eq!(values.len(), keys.len());
        if enable_async {
            expected_async_calls += 1;
        } else {
            assert_eq!(*barrier.0.lock().unwrap(), 2);
        }
        assert_eq!(
            recorded.lock().unwrap().async_batch_calls,
            expected_async_calls
        );
        runtime.client().lock().unwrap().initial_batch_barrier = None;
        read_ts += 1;
        let single = transaction
            .snapshot_batch_get_at(
                &keys[..1],
                read_ts,
                &UnaryCallContext::with_timeout(CALL_TIMEOUT),
            )
            .unwrap();
        assert_eq!(single, vec![(keys[0].clone(), b"batch-value".to_vec())]);
        assert_eq!(
            recorded.lock().unwrap().async_batch_calls,
            expected_async_calls,
            "a single physical batch bypasses async admission in either mode"
        );
        read_ts += 1;
    }
    assert_eq!(transaction.snapshot_point_rpc_counts(), (0, 9));
}

fn response_exec_detail(
    total: u64,
    processed: u64,
    size: u64,
) -> tidb_proto::kvrpcpb::ExecDetailsV2 {
    use tikv_client::proto::kvrpcpb;
    kvrpcpb::ExecDetailsV2 {
        scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
            total_versions: total,
            processed_versions: processed,
            processed_versions_size: size,
            ..Default::default()
        }),
        time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
            process_wall_time_ns: 29,
            ..Default::default()
        }),
        read_pool_task_details: Some(kvrpcpb::PoolTaskDetails {
            poll_count: 3,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn snapshot_response_stats_get_retries_cache_and_optional_collection() {
    let _config = snapshot_test_config();
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let mut client = LockingClient::new(recorded);
    client.remaining_locked = 0;
    client.status_response = Some(KvrpcCheckTxnStatusResponse {
        commit_version: START_TS,
        ..Default::default()
    });
    client.get_responses = [
        KvrpcGetResponse {
            error: Some(KvrpcKeyError {
                locked: Some(LockingClient::live_lock()),
                ..Default::default()
            }),
            value: b"ignored".to_vec(),
            exec_details_v2: Some(response_exec_detail(2, 1, 10)),
            ..Default::default()
        },
        KvrpcGetResponse {
            value: b"abc".to_vec(),
            exec_details_v2: Some(response_exec_detail(3, 2, 20)),
            ..Default::default()
        },
        KvrpcGetResponse::default(),
        KvrpcGetResponse {
            error: Some(KvrpcKeyError {
                abort: "fatal".to_owned(),
                ..Default::default()
            }),
            value: b"ignored".to_vec(),
            ..Default::default()
        },
    ]
    .into();
    let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion));
    let mut transaction = RealOptimisticTransaction::new_injected(
        runtime.clone(),
        TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4096,
    )
    .unwrap();
    assert!(!transaction.snapshot_point_response_stats().is_valid());
    let stats = Arc::new(tikv_client::SnapshotRuntimeStats::new());
    transaction.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));
    assert_eq!(
        transaction.snapshot_point_response_stats(),
        tikv_client::util::PointResponseStats::default()
    );
    let call = UnaryCallContext::with_timeout(CALL_TIMEOUT);
    assert_eq!(
        transaction.snapshot_get(ROW_KEY, &call).unwrap().value,
        Some(b"abc".to_vec())
    );
    let point = transaction.snapshot_point_response_stats();
    assert!(point.scan_detail_complete());
    assert!(point.payload_complete());
    assert_eq!(
        point.scan_detail,
        tikv_client::util::PointReadScanDetail {
            total_keys: 5,
            processed_keys: 3,
            processed_keys_size: 30
        }
    );
    assert_eq!(
        point.payload_bytes, 3,
        "Get excludes key bytes and error payload"
    );
    assert_eq!(stats.rpc_count(tikv_client::SnapshotRpcCommand::Get), 2);
    assert_eq!(
        stats.rpc_count(tikv_client::SnapshotRpcCommand::ResolveLock),
        1
    );
    assert_eq!(stats.time_detail().process_time, Duration::from_nanos(58));
    assert_eq!(stats.read_pool_task_details().unwrap().task_count, 2);
    assert_eq!(
        transaction.snapshot_get(ROW_KEY, &call).unwrap().rpc_count,
        0
    );
    assert_eq!(
        stats.point_response_stats(),
        point,
        "cache hits are not physical responses"
    );
    assert_eq!(stats.rpc_count(tikv_client::SnapshotRpcCommand::Get), 2);
    assert!(transaction
        .snapshot_get(b"miss", &call)
        .unwrap()
        .value
        .is_none());
    assert!(transaction.snapshot_get(b"fatal", &call).is_err());
    assert!(!stats.point_response_stats().scan_detail_complete());
    assert_eq!(stats.point_response_stats().payload_bytes, 3);
    assert_eq!(stats.rpc_count(tikv_client::SnapshotRpcCommand::Get), 4);
    let saved = stats.point_response_stats();
    transaction.set_snapshot_runtime_stats(None);
    transaction.snapshot_get(b"uncollected", &call).unwrap();
    assert!(!transaction.snapshot_point_response_stats().is_valid());
    assert_eq!(stats.point_response_stats(), saved);
    assert_eq!(stats.rpc_count(tikv_client::SnapshotRpcCommand::Get), 4);
    let empty = Arc::new(tikv_client::SnapshotRuntimeStats::new());
    transaction.set_snapshot_runtime_stats(Some(Arc::clone(&empty)));
    transaction.snapshot_get(ROW_KEY, &call).unwrap();
    assert!(transaction.snapshot_get(b"transport-error", &call).is_err());
    runtime
        .client()
        .lock()
        .unwrap()
        .get_responses
        .push_back(KvrpcGetResponse {
            region_error: Some(tidb_proto::errorpb::Error {
                raft_entry_too_large: Some(Default::default()),
                ..Default::default()
            }),
            exec_details_v2: Some(response_exec_detail(99, 99, 99)),
            ..Default::default()
        });
    assert!(transaction.snapshot_get(b"region-error", &call).is_err());
    assert_eq!(
        empty.point_response_stats(),
        tikv_client::util::PointResponseStats::default()
    );
    assert_eq!(empty.rpc_count(tikv_client::SnapshotRpcCommand::Get), 2);
    // Scanner pair errors are reread with Get; the enclosing Scan is not a
    // recognized point response and must not create missing-detail coverage.
    runtime
        .client()
        .lock()
        .unwrap()
        .scan_responses
        .push_back(KvrpcScanResponse {
            pairs: vec![tidb_proto::KvrpcKvPair {
                error: Some(KvrpcKeyError {
                    locked: Some(LockingClient::live_lock()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
    runtime
        .client()
        .lock()
        .unwrap()
        .get_responses
        .push_back(KvrpcGetResponse {
            value: b"scan-value".to_vec(),
            exec_details_v2: Some(response_exec_detail(1, 1, 10)),
            ..Default::default()
        });
    transaction
        .snapshot_scan(b"a", b"z", Some(1), &call)
        .unwrap();
    assert!(empty.point_response_stats().scan_detail_complete());
    assert_eq!(empty.point_response_stats().payload_bytes, 10);
    assert_eq!(empty.rpc_count(tikv_client::SnapshotRpcCommand::Get), 3);
    assert_eq!(
        empty.rpc_count(tikv_client::SnapshotRpcCommand::ResolveLock),
        0
    );
    {
        let mut client = runtime.client().lock().unwrap();
        client.status_response = Some(KvrpcCheckTxnStatusResponse {
            error: Some(KvrpcKeyError {
                abort: "lock resolution failed".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        // A fresh transaction avoids the status cache of the earlier success.
        client.get_responses.push_back(KvrpcGetResponse {
            error: Some(KvrpcKeyError {
                locked: Some(KvrpcLockInfo {
                    lock_version: 80,
                    ..LockingClient::live_lock()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    let prior_resolve_time = empty.resolve_lock_duration();
    assert!(transaction.snapshot_get(b"failed-lock", &call).is_err());
    assert!(empty.resolve_lock_duration() > prior_resolve_time);
    assert_eq!(empty.rpc_count(tikv_client::SnapshotRpcCommand::Get), 4);
    assert_eq!(
        empty.rpc_count(tikv_client::SnapshotRpcCommand::ResolveLock),
        1
    );
}

#[test]
fn snapshot_response_stats_batch_modes_retries_and_pair_errors() {
    let _config = snapshot_test_config();
    for enable_async in [false, true] {
        let mut config = tidb_config::config_tree::new_config();
        config.performance.enable_async_batch_get = enable_async;
        tidb_config::config_tree::config::store_global_config(config);
        let (client, keys) = two_batch_worker_fixture();
        client
            .keyed_batch_responses
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .get_mut(&keys[5120])
            .unwrap()
            .exec_details_v2 = Some(response_exec_detail(2, 1, 10));
        let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion));
        let mut transaction = RealOptimisticTransaction::new_injected(
            runtime.clone(),
            TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
            CALL_TIMEOUT,
            START_TS,
            Instant::now(),
            4,
            4096,
        )
        .unwrap();
        let stats = Arc::new(tikv_client::SnapshotRuntimeStats::new());
        transaction.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));
        let call = UnaryCallContext::with_timeout(CALL_TIMEOUT);
        let values = transaction.snapshot_batch_get(&keys, &call).unwrap();
        assert_eq!(values, vec![(keys[5120].clone(), b"batch-value".to_vec())]);
        let point = stats.point_response_stats();
        assert_eq!(
            stats.rpc_count(tikv_client::SnapshotRpcCommand::BatchGet),
            3
        );
        assert_eq!(
            stats.rpc_count(tikv_client::SnapshotRpcCommand::ResolveLock),
            1
        );
        assert_eq!(point.scan_detail.total_keys, 2);
        assert_eq!(
            point.payload_bytes,
            (keys[5120].len() + b"batch-value".len()) as u64
        );
        assert!(point.payload_complete());
        assert!(
            !point.scan_detail_complete(),
            "missing detail on either worker or retry is sticky"
        );
        transaction.snapshot_batch_get(&keys, &call).unwrap();
        assert_eq!(stats.point_response_stats(), point);
        assert_eq!(
            stats.rpc_count(tikv_client::SnapshotRpcCommand::BatchGet),
            3
        );
        let pair = |key: &[u8], value: &[u8]| tidb_proto::KvrpcKvPair {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        };
        runtime
            .client()
            .lock()
            .unwrap()
            .batch_responses
            .push_back(KvrpcBatchGetResponse {
                pairs: vec![
                    pair(b"aa", b"bbb"),
                    pair(b"missing", b""),
                    tidb_proto::KvrpcKvPair {
                        error: Some(KvrpcKeyError {
                            abort: "pair error".to_owned(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    pair(b"zz", b"q"),
                ],
                exec_details_v2: Some(response_exec_detail(3, 2, 20)),
                ..Default::default()
            });
        assert!(transaction
            .snapshot_batch_get(&[b"aa".to_vec()], &call)
            .is_err());
        assert_eq!(
            stats.point_response_stats().payload_bytes,
            point.payload_bytes + 15,
            "account every successful pair before handling the first pair error"
        );
        assert_eq!(stats.point_response_stats().scan_detail.total_keys, 5);
        assert_eq!(
            stats.rpc_count(tikv_client::SnapshotRpcCommand::BatchGet),
            4
        );
        assert_eq!(
            stats.rpc_count(tikv_client::SnapshotRpcCommand::ResolveLock),
            1
        );
    }
}

#[test]
fn snapshot_batch_backoff_keeps_one_completed_workers_history() {
    let _config = snapshot_test_config();
    for enable_async in [false, true] {
        let mut config = tidb_config::config_tree::new_config();
        config.performance.enable_async_batch_get = enable_async;
        tidb_config::config_tree::config::store_global_config(config);
        let mut client = LockingClient::new(Arc::new(Mutex::new(Recorded::default())));
        client.remaining_locked = 0;
        client.status_response = Some(KvrpcCheckTxnStatusResponse {
            commit_version: START_TS,
            ..Default::default()
        });
        let mut keys: Vec<_> = (0..5120).map(|i| format!("a{i:05}").into_bytes()).collect();
        keys.push(b"z".to_vec());
        client.keyed_batch_responses = Some(Arc::new(Mutex::new(
            [(&keys[0], 90), (&keys[5120], 91)]
                .into_iter()
                .map(|(key, lock_version)| {
                    (
                        key.clone(),
                        KvrpcBatchGetResponse {
                            error: Some(KvrpcKeyError {
                                locked: Some(KvrpcLockInfo {
                                    key: key.clone(),
                                    primary_lock: key.clone(),
                                    lock_version,
                                    ..LockingClient::live_lock()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )
                })
                .collect(),
        )));
        // Each worker sees its own already-hinted lock once on the retry, so
        // each independently sleeps 1ms. Summing workers would report 2ms.
        client.lock_batch_keys_once = Some(Arc::new(Mutex::new(Default::default())));
        let mut transaction = RealOptimisticTransaction::new_injected(
            SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion)),
            TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
            CALL_TIMEOUT,
            START_TS,
            Instant::now(),
            4,
            4096,
        )
        .unwrap();
        let stats = Arc::new(tikv_client::SnapshotRuntimeStats::new());
        transaction.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));
        let values = transaction
            .snapshot_batch_get(&keys, &UnaryCallContext::with_timeout(CALL_TIMEOUT))
            .unwrap();
        assert_eq!(values.len(), keys.len());
        assert_eq!(
            stats.rpc_count(tikv_client::SnapshotRpcCommand::BatchGet),
            6
        );
        assert_eq!(stats.backoff_count("txnLockFast"), 1);
        assert_eq!(
            stats.backoff_duration("txnLockFast"),
            Duration::from_millis(1)
        );
        // The completed history is recorded once per uncached operation.
        transaction
            .snapshot_batch_get(&keys, &UnaryCallContext::with_timeout(CALL_TIMEOUT))
            .unwrap();
        assert_eq!(stats.backoff_count("txnLockFast"), 1);
    }
}

#[test]
fn snapshot_nested_status_retries_share_history_across_lock_encounters() {
    let _config = snapshot_test_config();
    for batch in [false, true] {
        let recorded = Arc::new(Mutex::new(Recorded::default()));
        let mut client = LockingClient::new(Arc::clone(&recorded));
        client.remaining_locked = 0;
        for txn_id in [90, 91] {
            let error = KvrpcKeyError {
                locked: Some(KvrpcLockInfo {
                    lock_version: txn_id,
                    ..LockingClient::live_lock()
                }),
                ..Default::default()
            };
            client.get_responses.push_back(KvrpcGetResponse {
                error: Some(error.clone()),
                ..Default::default()
            });
            client.batch_responses.push_back(KvrpcBatchGetResponse {
                error: Some(error),
                ..Default::default()
            });
            client.status_responses.extend([
                KvrpcCheckTxnStatusResponse {
                    error: Some(KvrpcKeyError {
                        txn_not_found: Some(Default::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                KvrpcCheckTxnStatusResponse {
                    commit_version: 95,
                    ..Default::default()
                },
            ]);
        }
        client.get_responses.push_back(KvrpcGetResponse {
            value: b"value".to_vec(),
            ..Default::default()
        });
        client.batch_responses.push_back(KvrpcBatchGetResponse {
            pairs: vec![tidb_proto::KvrpcKvPair {
                key: ROW_KEY.to_vec(),
                value: b"value".to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(OneRegion));
        let mut transaction = RealOptimisticTransaction::new_injected(
            runtime,
            TickingTimestamps(std::sync::atomic::AtomicU64::new(2_000)),
            CALL_TIMEOUT,
            START_TS,
            Instant::now(),
            4,
            4096,
        )
        .unwrap();
        let stats = Arc::new(tikv_client::SnapshotRuntimeStats::new());
        transaction.set_snapshot_runtime_stats(Some(Arc::clone(&stats)));
        let call = UnaryCallContext::with_timeout(CALL_TIMEOUT);
        if batch {
            assert_eq!(
                transaction
                    .snapshot_batch_get(&[ROW_KEY.to_vec()], &call)
                    .unwrap(),
                vec![(ROW_KEY.to_vec(), b"value".to_vec())]
            );
        } else {
            assert_eq!(
                transaction.snapshot_get(ROW_KEY, &call).unwrap().value,
                Some(b"value".to_vec())
            );
        }
        assert_eq!(recorded.lock().unwrap().status_checks.len(), 4);
        assert_eq!(stats.backoff_count("txnNotFound"), 2);
        assert_eq!(
            stats.backoff_duration("txnNotFound"),
            Duration::from_millis(6),
            "both lock encounters share the 2ms then 4ms schedule"
        );
    }
}
