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
    status_checks: Vec<KvrpcCheckTxnStatusRequest>,
}

/// A store holding one value behind a lock that stays alive for the first
/// [`LOCKED_RESPONSES`] probes; the mock lives at the trait seam, so no
/// transport runs at all.
struct LockingClient {
    remaining_locked: u64,
    request_ids: u64,
    recorded: Arc<Mutex<Recorded>>,
}

impl LockingClient {
    fn new(recorded: Arc<Mutex<Recorded>>) -> Self {
        Self {
            remaining_locked: LOCKED_RESPONSES,
            request_ids: 0,
            recorded,
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
        PublishedCommand::BeforePublication(concat!(
            "this regression never publishes ",
            $name
        )
        .to_owned())
    };
}

impl TransactionCommandClient for LockingClient {
    fn publish_transaction_get(
        &mut self,
        _address: &str,
        request: &KvrpcGetRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcGetResponse> {
        self.recorded
            .lock()
            .unwrap()
            .get_versions
            .push(request.version);
        let response = if self.remaining_locked > 0 {
            self.remaining_locked -= 1;
            KvrpcGetResponse {
                error: Some(KvrpcKeyError {
                    locked: Some(Self::live_lock()),
                    ..KvrpcKeyError::default()
                }),
                ..KvrpcGetResponse::default()
            }
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
        _request: &KvrpcBatchGetRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcBatchGetResponse> {
        never_published!(self, "BatchGet")
    }

    fn publish_transaction_scan(
        &mut self,
        _address: &str,
        _request: &KvrpcScanRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcScanResponse> {
        never_published!(self, "Scan")
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
        self.recorded
            .lock()
            .unwrap()
            .status_checks
            .push(request.clone());
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
        panic!("an alive lock is waited out, never resolved");
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

    let recorded = recorded.lock().unwrap();
    assert_eq!(
        recorded.get_versions.len() as u64,
        LOCKED_RESPONSES + 1,
        "one probe per locked answer plus the final read"
    );
    assert!(
        recorded
            .get_versions
            .iter()
            .all(|version| *version == START_TS),
        "every probe reads at the transaction's one timestamp"
    );
    assert!(
        !recorded.status_checks.is_empty(),
        "a locked probe consults the blocking transaction's status"
    );
    assert!(
        recorded
            .status_checks
            .iter()
            .all(|check| check.lock_ts == LOCK_TS && check.primary_key == ROW_KEY),
        "the status question names the lock's own transaction and primary"
    );
}
