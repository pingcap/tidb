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

//! The regression: one logical snapshot Scan shared ONE absolute deadline
//! across every region page, so a table-wide ANALYZE sample -- tens of
//! thousands of honest pages -- saturated that deadline to zero mid-scan and
//! every later page answered instantly with "timed out after 0ms" (or,
//! through another producer of the same expiry, "completion execution deadline
//! exceeded"). Go bounds EACH region page by its own fresh `ReadTimeoutMedium`
//! (`SendReq(bo, req, loc.Region, ReadTimeoutMedium)` per iteration of
//! `KVSnapshot.scan`) and keeps only the caller's cancellation alive across
//! pages; so does this port now.
//!
//! The client is mocked at the trait seam ([`TransactionCommandClient`]), the
//! same seam `tidb-unistore`'s in-process client implements: a scan whose
//! every page carries a full page of pairs cannot be produced on demand
//! against a live cluster, and recording each page's remaining call budget is
//! exactly what distinguishes a per-page budget from a shared absolute one.

#![allow(missing_docs)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_proto::{
    KvrpcBatchGetRequest, KvrpcBatchGetResponse, KvrpcBatchRollbackRequest,
    KvrpcBatchRollbackResponse, KvrpcCheckSecondaryLocksRequest, KvrpcCheckSecondaryLocksResponse,
    KvrpcCheckTxnStatusRequest, KvrpcCheckTxnStatusResponse, KvrpcCommitRequest,
    KvrpcCommitResponse, KvrpcContext, KvrpcGetRequest, KvrpcGetResponse, KvrpcKvPair,
    KvrpcPessimisticLockRequest, KvrpcPessimisticLockResponse, KvrpcPessimisticRollbackRequest,
    KvrpcPessimisticRollbackResponse, KvrpcPrewriteRequest, KvrpcPrewriteResponse,
    KvrpcResolveLockRequest, KvrpcResolveLockResponse, KvrpcScanRequest, KvrpcScanResponse,
    KvrpcTxnHeartBeatRequest, KvrpcTxnHeartBeatResponse,
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
/// The caller's own budget for the WHOLE logical scan. Under the old
/// shared-deadline behavior this expires between the first and second page;
/// under Go's per-page behavior it never touches any page's RPC budget.
const CALLER_BUDGET: Duration = Duration::from_millis(150);
/// Each full page carries exactly one scan limit of pairs; three full pages
/// plus one drained page make four region round trips in ONE logical Scan.
const FULL_PAGES: usize = 3;
const REGION: u64 = 62;
const ADDRESS: &str = "in-process-scan-pages";

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

/// A TSO the scan never needs (no lock is ever met), but the type requires.
#[derive(Debug)]
struct FixedTimestamps(std::sync::atomic::AtomicU64);

impl TimestampSource for FixedTimestamps {
    fn current_ts(&self) -> Result<u64, String> {
        Ok(self.0.load(std::sync::atomic::Ordering::Relaxed))
    }
}

#[derive(Debug, Default)]
struct Recorded {
    /// Remaining caller budget each page observed at admission time. The
    /// shared-deadline bug shows up as zeros from the second page onward.
    page_budgets_ms: Vec<u64>,
}

/// A store whose scan range holds [`FULL_PAGES`] full pages followed by a
/// drained page, served at the mock seam with no transport at all.
struct PagingScanClient {
    remaining_full_pages: usize,
    next_key: u64,
    request_ids: u64,
    recorded: Arc<Mutex<Recorded>>,
}

impl PagingScanClient {
    fn new(recorded: Arc<Mutex<Recorded>>) -> Self {
        Self {
            remaining_full_pages: FULL_PAGES,
            next_key: 0,
            request_ids: 0,
            recorded,
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

impl TransactionCommandClient for PagingScanClient {
    fn publish_transaction_get(
        &mut self,
        _address: &str,
        _request: &KvrpcGetRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcGetResponse> {
        never_published!(self, "Get")
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
        request: &KvrpcScanRequest,
        _context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcScanResponse> {
        self.recorded
            .lock()
            .unwrap()
            .page_budgets_ms
            .push(u64::try_from(call.timeout().as_millis()).unwrap_or(u64::MAX));
        let pairs = if self.remaining_full_pages > 0 {
            self.remaining_full_pages -= 1;
            (0..request.limit)
                .map(|_| {
                    self.next_key += 1;
                    KvrpcKvPair {
                        key: format!("k-{:06}", self.next_key).into_bytes(),
                        value: b"page-value".to_vec(),
                        ..KvrpcKvPair::default()
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        self.respond(
            BatchCommandTag::Scan,
            KvrpcScanResponse {
                pairs,
                ..KvrpcScanResponse::default()
            },
        )
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

impl LockRecoveryClient for PagingScanClient {
    fn check_txn_status_for_lock(
        &mut self,
        _address: &str,
        _request: &KvrpcCheckTxnStatusRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "this regression never publishes CheckTxnStatus".to_owned(),
        ))
    }

    fn check_secondary_locks_for_lock(
        &mut self,
        _address: &str,
        _request: &KvrpcCheckSecondaryLocksRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "this regression never publishes CheckSecondaryLocks".to_owned(),
        ))
    }

    fn resolve_lock_for_read(
        &mut self,
        _address: &str,
        _request: &KvrpcResolveLockRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "this regression never publishes ResolveLock".to_owned(),
        ))
    }

    fn pessimistic_rollback_for_lock(
        &mut self,
        _address: &str,
        _request: &KvrpcPessimisticRollbackRequest,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        Err(DirectUnaryClientError::InvalidRequest(
            "this regression never publishes PessimisticRollback-for-lock".to_owned(),
        ))
    }
}

#[test]
fn a_multi_page_scan_bounds_each_page_not_the_whole_range() {
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let runtime = SharedReadRuntime::new_injected(
        PagingScanClient::new(Arc::clone(&recorded)),
        RegionCache::new(OneRegion),
    );
    let mut transaction = RealOptimisticTransaction::new_injected(
        runtime,
        FixedTimestamps(std::sync::atomic::AtomicU64::new(START_TS)),
        CALLER_BUDGET,
        START_TS,
        Instant::now(),
        1,
        1024,
    )
    .unwrap();
    let call = UnaryCallContext::with_timeout(CALLER_BUDGET);

    let pairs = transaction
        .snapshot_scan(b"a", b"z", None, &call)
        .unwrap_or_else(|error| panic!("scan failed: {error}"));

    assert_eq!(
        pairs.len(),
        FULL_PAGES * 256,
        "three full pages come back whole"
    );
    let budgets = recorded.lock().unwrap().page_budgets_ms.clone();
    assert_eq!(
        budgets.len(),
        FULL_PAGES + 1,
        "one round trip per full page plus the drained closer"
    );
    // The caller's 150 ms budget must not be what any page runs under: each
    // page opens its own fresh `ReadTimeoutMedium`. A floor below the constant
    // keeps the assertion honest about "fresh" without re-deriving the exact
    // instant arithmetic.
    for (index, budget) in budgets.iter().enumerate() {
        assert!(
            *budget >= 55_000,
            "page {index} ran under a {budget}ms budget, not its own fresh one"
        );
    }
}
