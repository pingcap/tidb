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
    batch_contexts: Vec<KvrpcContext>,
    status_checks: Vec<KvrpcCheckTxnStatusRequest>,
    status_get_attempts: Vec<(u64, usize)>,
}

/// A store holding one value behind a lock that stays alive for the first
/// [`LOCKED_RESPONSES`] probes; the mock lives at the trait seam, so no
/// transport runs at all.
struct LockingClient {
    remaining_locked: u64,
    request_ids: u64,
    recorded: Arc<Mutex<Recorded>>,
    batch_responses: std::collections::VecDeque<KvrpcBatchGetResponse>,
    get_responses: std::collections::VecDeque<KvrpcGetResponse>,
    scan_responses: std::collections::VecDeque<KvrpcScanResponse>,
    status_response: Option<KvrpcCheckTxnStatusResponse>,
}

impl LockingClient {
    fn new(recorded: Arc<Mutex<Recorded>>) -> Self {
        Self {
            remaining_locked: LOCKED_RESPONSES,
            request_ids: 0,
            recorded,
            batch_responses: Default::default(),
            get_responses: Default::default(),
            scan_responses: Default::default(),
            status_response: None,
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
        _call: &UnaryCallContext,
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
        if let Some(response) = self.batch_responses.pop_front() {
            return self.respond(BatchCommandTag::BatchGet, response);
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
    for batch in [false, true] {
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
            let started = Instant::now();
            if batch {
                assert_eq!(
                    transaction
                        .snapshot_batch_get(
                            &[ROW_KEY.to_vec()],
                            &UnaryCallContext::with_timeout(CALL_TIMEOUT)
                        )
                        .unwrap()
                        .len(),
                    1
                );
            } else {
                assert!(transaction
                    .snapshot_get(ROW_KEY, &UnaryCallContext::with_timeout(CALL_TIMEOUT))
                    .unwrap()
                    .value
                    .is_some());
            }
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
