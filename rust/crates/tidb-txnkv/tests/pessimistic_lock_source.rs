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

//! Pessimistic locking sequences a live cluster cannot produce on demand.
//!
//! Deadlock detection, a blocker whose lock was refreshed milliseconds ago, and
//! an exhausted lock-wait budget are all timing-dependent against real TiKV.
//! Here the transport, BatchCommands framing, publication identity, region
//! routing, and the coordinator itself are the real production paths; only
//! TiKV's answers are scripted.

#![allow(missing_docs)]

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use prost::Message;
use tidb_proto::errorpb;
use tidb_proto::tikvpb::batch_commands_request::request::Cmd as RequestCmd;
use tidb_proto::tikvpb::batch_commands_response::response::Cmd as ResponseCmd;
use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
use tidb_proto::tikvpb::{batch_commands_response, BatchCommandsRequest, BatchCommandsResponse};
use tidb_proto::{
    CoprocessorRequest, CoprocessorResponse, KvrpcCommitRequest, KvrpcCommitResponse, KvrpcDeadlock,
    KvrpcKeyError, KvrpcLockInfo, KvrpcOp, KvrpcPessimisticAction, KvrpcPessimisticLockKeyResult,
    KvrpcPessimisticLockRequest, KvrpcPessimisticLockResponse, KvrpcPessimisticLockWakeUpMode,
    KvrpcPessimisticRollbackRequest, KvrpcPessimisticRollbackResponse, KvrpcPrewriteRequest,
    KvrpcPrewriteResponse, KvrpcWaitForEntry, KvrpcWriteConflict,
};
use tidb_txnkv::lock::TimestampSource;
use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation,
    RegionMetadata, RegionRecoveryLoader, RegionVerId, Store,
};
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext};
use tidb_txnkv::transaction::{
    LockWaitTime, OptimisticMutation, PessimisticLockFailure, RealOptimisticTransaction,
    RealPessimisticTransaction, TransactionCause,
};
use tidb_txnkv::SharedReadRuntime;

const START_TS: u64 = 400;
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Lexicographically smallest key, so it becomes the transaction's primary.
const PRIMARY_KEY: &[u8] = b"a";
const SECOND_KEY: &[u8] = b"b";
/// Blocking transaction's start timestamp, older than ours.
const BLOCKER_TS: u64 = 300;

const REGION_ID: u64 = 41;

type ScriptedTransaction =
    RealPessimisticTransaction<TonicCoprocessorClient, SingleRegion, MonotonicTimestamps>;

/// A PD stand-in that never returns the same timestamp twice.
///
/// A pessimistic transaction consumes timestamps repeatedly — once per
/// statement retry and again for the commit — so a one-shot source cannot
/// drive these sequences.
#[derive(Debug, Default)]
struct MonotonicTimestamps {
    next: std::cell::Cell<u64>,
}

impl TimestampSource for MonotonicTimestamps {
    fn current_ts(&self) -> Result<u64, String> {
        let previous = self.next.get().max(START_TS);
        let timestamp = previous + 1;
        self.next.set(timestamp);
        Ok(timestamp)
    }
}

// -----------------------------------------------------------------------------
// Scripted topology
// -----------------------------------------------------------------------------

/// One region covering the whole key space, always routable.
#[derive(Clone)]
struct SingleRegion {
    address: String,
    loads: Arc<AtomicU64>,
}

impl SingleRegion {
    fn new(address: String) -> Self {
        Self {
            address,
            loads: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl RegionLoader for SingleRegion {
    fn cluster_id(&self) -> u64 {
        7
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(RegionLocation {
            region: RegionVerId {
                id: REGION_ID,
                epoch: RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                },
            },
            start_key: Vec::new(),
            end_key: Vec::new(),
            peers: vec![Peer {
                id: 410,
                store_id: 4_100,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 1,
            }],
            leader_peer_id: Some(410),
            stores: vec![Store {
                id: 4_100,
                address: self.address.clone(),
                epoch: 1,
            }],
            ..RegionLocation::default()
        })
    }
}

impl RegionRecoveryLoader for SingleRegion {
    fn hydrate_region(
        &mut self,
        _metadata: &RegionMetadata,
        _leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-hydration",
            "these sequences never take the EpochNotMatch hydration path",
        ))
    }
}

// -----------------------------------------------------------------------------
// Scripted TiKV
// -----------------------------------------------------------------------------

/// One scripted answer to one PessimisticLock attempt.
#[derive(Clone, Debug)]
enum LockOutcome {
    /// Locks acquired.
    Granted,
    /// A retryable region error, which must make the client relocate.
    RegionError,
    /// TiKV's detector proved a cycle.
    Deadlock,
    /// A newer commit invalidated this statement's `for_update_ts`.
    WriteConflict,
    /// Blocked by a lock the owner refreshed `refreshed_ms` ago.
    BlockedByLiveLock { refreshed_ms: u64 },
    /// A Normal wake-up answered with the ForceLock-only `results` field.
    ForceLockResults,
}

#[derive(Debug, Default)]
struct Recorded {
    locks: Vec<KvrpcPessimisticLockRequest>,
    pessimistic_rollbacks: Vec<KvrpcPessimisticRollbackRequest>,
    prewrites: Vec<KvrpcPrewriteRequest>,
    commits: Vec<KvrpcCommitRequest>,
}

#[derive(Clone)]
struct ScriptedTikv {
    locks: Arc<Mutex<Vec<LockOutcome>>>,
    recorded: Arc<Mutex<Recorded>>,
}

impl ScriptedTikv {
    fn new(locks: Vec<LockOutcome>) -> Self {
        Self {
            locks: Arc::new(Mutex::new(locks)),
            recorded: Arc::new(Mutex::new(Recorded::default())),
        }
    }

    /// Later attempts than the script describes are granted, which models a
    /// blocker that finally went away.
    fn next_lock(&self) -> LockOutcome {
        let mut script = self.locks.lock().unwrap();
        if script.is_empty() {
            return LockOutcome::Granted;
        }
        script.remove(0)
    }
}

fn live_pessimistic_lock(key: &[u8], refreshed_ms: u64) -> KvrpcKeyError {
    KvrpcKeyError {
        locked: Some(KvrpcLockInfo {
            primary_lock: PRIMARY_KEY.to_vec(),
            lock_version: BLOCKER_TS,
            key: key.to_vec(),
            lock_ttl: 3_000,
            lock_type: KvrpcOp::PessimisticLock as i32,
            lock_for_update_ts: BLOCKER_TS,
            duration_to_last_update_ms: refreshed_ms,
            ..KvrpcLockInfo::default()
        }),
        ..KvrpcKeyError::default()
    }
}

fn lock_response(outcome: &LockOutcome, request: &KvrpcPessimisticLockRequest) -> Vec<u8> {
    let blocked_key = request
        .mutations
        .first()
        .map(|mutation| mutation.key.clone())
        .unwrap_or_default();
    let response = match outcome {
        LockOutcome::Granted => KvrpcPessimisticLockResponse::default(),
        LockOutcome::RegionError => KvrpcPessimisticLockResponse {
            region_error: Some(errorpb::Error {
                recovery_in_progress: Some(errorpb::RecoveryInProgress { region_id: REGION_ID }),
                ..errorpb::Error::default()
            }),
            ..KvrpcPessimisticLockResponse::default()
        },
        LockOutcome::Deadlock => KvrpcPessimisticLockResponse {
            errors: vec![KvrpcKeyError {
                deadlock: Some(KvrpcDeadlock {
                    lock_ts: BLOCKER_TS,
                    lock_key: blocked_key,
                    deadlock_key_hash: 99,
                    deadlock_key: PRIMARY_KEY.to_vec(),
                    wait_chain: vec![
                        KvrpcWaitForEntry {
                            txn: START_TS,
                            wait_for_txn: BLOCKER_TS,
                            ..KvrpcWaitForEntry::default()
                        },
                        KvrpcWaitForEntry {
                            txn: BLOCKER_TS,
                            wait_for_txn: START_TS,
                            ..KvrpcWaitForEntry::default()
                        },
                    ],
                }),
                ..KvrpcKeyError::default()
            }],
            ..KvrpcPessimisticLockResponse::default()
        },
        LockOutcome::WriteConflict => KvrpcPessimisticLockResponse {
            errors: vec![KvrpcKeyError {
                conflict: Some(KvrpcWriteConflict {
                    start_ts: START_TS,
                    conflict_ts: BLOCKER_TS,
                    conflict_commit_ts: START_TS + 5,
                    key: blocked_key,
                    ..KvrpcWriteConflict::default()
                }),
                ..KvrpcKeyError::default()
            }],
            ..KvrpcPessimisticLockResponse::default()
        },
        LockOutcome::BlockedByLiveLock { refreshed_ms } => KvrpcPessimisticLockResponse {
            errors: vec![live_pessimistic_lock(&blocked_key, *refreshed_ms)],
            ..KvrpcPessimisticLockResponse::default()
        },
        LockOutcome::ForceLockResults => KvrpcPessimisticLockResponse {
            results: vec![KvrpcPessimisticLockKeyResult::default()],
            ..KvrpcPessimisticLockResponse::default()
        },
    };
    response.encode_to_vec()
}

#[tonic::async_trait]
impl Tikv for ScriptedTikv {
    type BatchCommandsStream =
        tokio_stream::wrappers::ReceiverStream<Result<BatchCommandsResponse, tonic::Status>>;

    async fn coprocessor(
        &self,
        _request: tonic::Request<CoprocessorRequest>,
    ) -> Result<tonic::Response<CoprocessorResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "these sequences publish transaction commands only",
        ))
    }

    async fn batch_commands(
        &self,
        request: tonic::Request<tonic::Streaming<BatchCommandsRequest>>,
    ) -> Result<tonic::Response<Self::BatchCommandsStream>, tonic::Status> {
        let service = self.clone();
        let mut inbound = request.into_inner();
        let (responses, response_rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            while let Ok(Some(packet)) = inbound.message().await {
                for (request_id, request) in packet.request_ids.into_iter().zip(packet.requests) {
                    let Some(cmd) = request.cmd else { continue };
                    let response = match service.answer(cmd) {
                        Ok(response) => response,
                        Err(status) => {
                            let _ = responses.send(Err(status)).await;
                            return;
                        }
                    };
                    let packet = BatchCommandsResponse {
                        request_ids: vec![request_id],
                        responses: vec![batch_commands_response::Response {
                            cmd: Some(response),
                        }],
                        transport_layer_load: 0,
                        ..BatchCommandsResponse::default()
                    };
                    if responses.send(Ok(packet)).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(response_rx),
        ))
    }
}

impl ScriptedTikv {
    fn answer(&self, cmd: RequestCmd) -> Result<ResponseCmd, tonic::Status> {
        match cmd {
            RequestCmd::PessimisticLock(body) => {
                let request = KvrpcPessimisticLockRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                let outcome = self.next_lock();
                let encoded = lock_response(&outcome, &request);
                self.recorded.lock().unwrap().locks.push(request);
                Ok(ResponseCmd::PessimisticLock(encoded))
            }
            RequestCmd::PessimisticRollback(body) => {
                let request = KvrpcPessimisticRollbackRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                self.recorded
                    .lock()
                    .unwrap()
                    .pessimistic_rollbacks
                    .push(request);
                Ok(ResponseCmd::PessimisticRollback(
                    KvrpcPessimisticRollbackResponse::default().encode_to_vec(),
                ))
            }
            RequestCmd::Prewrite(body) => {
                let request = KvrpcPrewriteRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                self.recorded.lock().unwrap().prewrites.push(request);
                Ok(ResponseCmd::Prewrite(
                    KvrpcPrewriteResponse::default().encode_to_vec(),
                ))
            }
            RequestCmd::Commit(body) => {
                let request = KvrpcCommitRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                self.recorded.lock().unwrap().commits.push(request);
                Ok(ResponseCmd::Commit(
                    KvrpcCommitResponse::default().encode_to_vec(),
                ))
            }
            other => Err(tonic::Status::unimplemented(format!(
                "unexpected command in a pessimistic sequence: {other:?}"
            ))),
        }
    }
}

struct TestServer {
    address: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl TestServer {
    fn start(service: ScriptedTikv) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let server = tonic::transport::Server::builder()
                    .add_service(TikvServer::new(service))
                    .serve_with_shutdown(address, async {
                        let _ = shutdown_rx.await;
                    });
                started_tx.send(()).unwrap();
                server.await.unwrap();
            });
        });
        started_rx.recv().unwrap();
        for _ in 0..200 {
            if std::net::TcpStream::connect(address).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Self {
            address: format!("http://{address}"),
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }

    fn store_address(&self) -> String {
        self.address.clone()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

// -----------------------------------------------------------------------------
// Fixture
// -----------------------------------------------------------------------------

fn transaction(topology: SingleRegion) -> ScriptedTransaction {
    let client = TonicCoprocessorClient::new().unwrap();
    let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(topology));
    let two_pc = RealOptimisticTransaction::new_injected(
        runtime,
        MonotonicTimestamps::default(),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4 * 1024,
    )
    .unwrap();
    RealPessimisticTransaction::from_transaction(two_pc, Instant::now()).unwrap()
}

fn fixture(locks: Vec<LockOutcome>) -> (TestServer, Arc<Mutex<Recorded>>, ScriptedTransaction) {
    let service = ScriptedTikv::new(locks);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let transaction = transaction(SingleRegion::new(server.store_address()));
    (server, recorded, transaction)
}

fn call() -> UnaryCallContext {
    UnaryCallContext::with_timeout(CALL_TIMEOUT)
}

fn no_presumption() -> BTreeSet<Vec<u8>> {
    BTreeSet::new()
}

// -----------------------------------------------------------------------------
// Request shaping
// -----------------------------------------------------------------------------

/// A granted lock carries the statement timestamp, not just the transaction's.
///
/// Source contract (`actionPessimisticLock.handleSingleBatch`): every mutation
/// is `Op_PessimisticLock`, `start_version` stays the transaction's while
/// `for_update_ts` is the statement's, `min_commit_ts` is `for_update_ts + 1`,
/// and the primary chosen by the first statement is reused by later ones with
/// `is_first_lock` cleared.
#[test]
fn a_granted_lock_carries_the_statement_timestamp_and_a_stable_primary() {
    let (_server, recorded, mut transaction) = fixture(Vec::new());

    let first = transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect("an unblocked lock is granted");
    assert_eq!(first.for_update_ts, START_TS);
    assert_eq!(first.primary_key, PRIMARY_KEY.to_vec());
    assert_eq!(first.keys, vec![PRIMARY_KEY.to_vec()]);

    let statement_ts = transaction
        .advance_for_update_ts()
        .expect("a new statement gets its own timestamp");
    assert!(statement_ts > START_TS);

    let second = transaction
        .acquire_locks(
            &[SECOND_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect("the second statement is granted too");
    assert_eq!(second.for_update_ts, statement_ts);
    // The primary must not move; the whole transaction's recovery points at it.
    assert_eq!(second.primary_key, PRIMARY_KEY.to_vec());
    assert_eq!(
        transaction.locked_keys(),
        vec![PRIMARY_KEY.to_vec(), SECOND_KEY.to_vec()]
    );

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.locks.len(), 2);
    let opening = &recorded.locks[0];
    assert_eq!(opening.start_version, START_TS);
    assert_eq!(opening.for_update_ts, START_TS);
    assert_eq!(opening.min_commit_ts, START_TS + 1);
    assert_eq!(opening.primary_lock, PRIMARY_KEY.to_vec());
    assert!(opening.is_first_lock);
    assert_eq!(
        opening.wake_up_mode,
        KvrpcPessimisticLockWakeUpMode::WakeUpModeNormal as i32
    );
    assert!(!opening.return_values && !opening.check_existence);
    assert_eq!(opening.mutations.len(), 1);
    assert_eq!(opening.mutations[0].op, KvrpcOp::PessimisticLock as i32);
    assert!(
        opening.mutations[0].value.is_empty(),
        "a pessimistic lock reserves a key, it does not write a value"
    );
    // A lock must outlive the statement that took it, so its TTL is at least
    // one managed interval.
    assert!(opening.lock_ttl >= 20_000);

    let later = &recorded.locks[1];
    assert_eq!(later.start_version, START_TS);
    assert_eq!(later.for_update_ts, statement_ts);
    assert_eq!(later.min_commit_ts, statement_ts + 1);
    assert_eq!(later.primary_lock, PRIMARY_KEY.to_vec());
    assert!(
        !later.is_first_lock,
        "only the very first lock may skip deadlock detection"
    );
}

/// An INSERT's expected-absent keys are checked while locking, not at commit.
#[test]
fn a_presumed_absent_key_carries_the_not_exists_assertion() {
    let (_server, recorded, mut transaction) = fixture(Vec::new());
    let presumed = BTreeSet::from([PRIMARY_KEY.to_vec()]);

    transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec(), SECOND_KEY.to_vec()],
            &presumed,
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect("locks are granted");

    let recorded = recorded.lock().unwrap();
    let mutations = &recorded.locks[0].mutations;
    let assertion_for = |key: &[u8]| {
        mutations
            .iter()
            .find(|mutation| mutation.key == key)
            .expect("every requested key is sent")
            .assertion
    };
    assert_eq!(
        assertion_for(PRIMARY_KEY),
        tidb_proto::KvrpcAssertion::NotExist as i32
    );
    assert_eq!(
        assertion_for(SECOND_KEY),
        tidb_proto::KvrpcAssertion::None as i32
    );
}

// -----------------------------------------------------------------------------
// Blocked statements
// -----------------------------------------------------------------------------

/// A proven deadlock ends the statement immediately; retrying recreates it.
#[test]
fn a_detected_deadlock_aborts_the_statement_without_retrying() {
    let (_server, recorded, mut transaction) = fixture(vec![LockOutcome::Deadlock]);

    let failure = transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect_err("a deadlock cannot be waited out");

    let PessimisticLockFailure::Deadlock(detail) = failure else {
        panic!("a deadlock must keep its own identity, got {failure:?}");
    };
    assert_eq!(detail.lock_ts, BLOCKER_TS);
    assert_eq!(detail.deadlock_key, PRIMARY_KEY.to_vec());
    assert_eq!(detail.deadlock_key_hash, 99);
    assert_eq!(
        detail.wait_chain,
        vec![(START_TS, BLOCKER_TS), (BLOCKER_TS, START_TS)],
        "the whole proven cycle must reach the SQL layer"
    );
    assert_eq!(
        recorded.lock().unwrap().locks.len(),
        1,
        "a deadlock must not be retried"
    );
    // The transaction itself survives: only this statement is dead.
    assert!(transaction.locked_keys().is_empty());
}

/// A write conflict costs the statement, not the transaction: a fresh
/// `for_update_ts` lets the same statement run again over the same `start_ts`.
#[test]
fn a_write_conflict_is_statement_scoped_and_a_newer_timestamp_retries() {
    let (_server, recorded, mut transaction) = fixture(vec![LockOutcome::WriteConflict]);

    let failure = transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect_err("a stale for_update_ts cannot take the lock");
    assert!(
        matches!(failure, PessimisticLockFailure::WriteConflict { .. }),
        "got {failure:?}"
    );
    assert!(
        failure.is_statement_scoped(),
        "a write conflict must not end a pessimistic transaction"
    );

    let statement_ts = transaction.advance_for_update_ts().expect("a newer TSO");
    let retry = transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect("the retry sees the newer committed version and succeeds");

    assert_eq!(retry.for_update_ts, statement_ts);
    assert_eq!(transaction.start_ts(), START_TS, "start_ts must not move");
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.locks.len(), 2);
    assert_eq!(recorded.locks[0].for_update_ts, START_TS);
    assert_eq!(recorded.locks[1].for_update_ts, statement_ts);
    assert_eq!(recorded.locks[1].start_version, START_TS);
}

/// `NOWAIT` fails the statement instead of queueing behind a live owner.
#[test]
fn nowait_fails_immediately_rather_than_queueing_behind_a_live_owner() {
    let (_server, recorded, mut transaction) = fixture(vec![LockOutcome::BlockedByLiveLock {
        // Refreshed well inside the skip-resolve threshold: the owner is
        // demonstrably alive, so no status RPC can change the answer.
        refreshed_ms: 10,
    }]);

    let failure = transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::NoWait,
            &call(),
        )
        .expect_err("NOWAIT cannot wait");

    let PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key } = failure else {
        panic!("NOWAIT must have its own identity, got {failure:?}");
    };
    assert_eq!(key, PRIMARY_KEY.to_vec());
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.locks.len(), 1, "NOWAIT must not retry");
    assert_eq!(
        recorded.locks[0].wait_timeout, -1,
        "NOWAIT is sent to TiKV so it does not queue the request either"
    );
}

/// An exhausted lock-wait budget is a timeout, distinct from `NOWAIT`.
#[test]
fn an_exhausted_lock_wait_budget_reports_a_timeout() {
    let (_server, recorded, mut transaction) = fixture(vec![LockOutcome::BlockedByLiveLock {
        refreshed_ms: 10,
    }]);

    let failure = transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::Timeout(Duration::ZERO),
            &call(),
        )
        .expect_err("a spent budget cannot wait further");

    assert!(
        matches!(failure, PessimisticLockFailure::LockWaitTimeout { .. }),
        "a spent budget is a timeout, not a NOWAIT failure: {failure:?}"
    );
    assert_eq!(recorded.lock().unwrap().locks.len(), 1);
}

/// A blocker that goes away between wake-ups is simply retried.
#[test]
fn a_blocker_that_disappears_is_retried_and_the_lock_is_taken() {
    let (_server, recorded, mut transaction) = fixture(vec![
        LockOutcome::BlockedByLiveLock { refreshed_ms: 10 },
        LockOutcome::Granted,
    ]);

    let acquired = transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::Timeout(Duration::from_secs(5)),
            &call(),
        )
        .expect("the second attempt takes the released lock");

    assert_eq!(acquired.keys, vec![PRIMARY_KEY.to_vec()]);
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.locks.len(), 2);
    // The residual budget must shrink, or every retry would restart the wait.
    assert!(recorded.locks[1].wait_timeout <= recorded.locks[0].wait_timeout);
}

/// A region error relocates and retries instead of failing the statement.
#[test]
fn a_region_error_relocates_the_batch_and_retries() {
    let (_server, recorded, mut transaction) =
        fixture(vec![LockOutcome::RegionError, LockOutcome::Granted]);

    transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect("the relocated attempt is granted");

    assert_eq!(recorded.lock().unwrap().locks.len(), 2);
}

/// A Normal wake-up answered in the ForceLock protocol is refused, not guessed.
#[test]
fn force_lock_results_in_a_normal_wake_up_are_refused() {
    let (_server, _recorded, mut transaction) = fixture(vec![LockOutcome::ForceLockResults]);

    let failure = transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect_err("a protocol this client did not speak cannot be interpreted");

    assert!(
        matches!(
            failure,
            PessimisticLockFailure::Transaction(TransactionCause::InvalidResponse { .. })
        ),
        "got {failure:?}"
    );
}

// -----------------------------------------------------------------------------
// Releasing locks and committing
// -----------------------------------------------------------------------------

/// A statement that fails after locking must not leave its locks behind.
#[test]
fn a_pessimistic_rollback_releases_the_locks_and_forgets_them() {
    let (_server, recorded, mut transaction) = fixture(Vec::new());
    transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec(), SECOND_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect("locks are granted");

    transaction
        .pessimistic_rollback(&[SECOND_KEY.to_vec()], &call())
        .expect("releasing an own lock succeeds");

    assert_eq!(
        transaction.locked_keys(),
        vec![PRIMARY_KEY.to_vec()],
        "a released lock must not be claimed at Prewrite"
    );
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.pessimistic_rollbacks.len(), 1);
    let rollback = &recorded.pessimistic_rollbacks[0];
    assert_eq!(rollback.start_version, START_TS);
    assert_eq!(rollback.for_update_ts, START_TS);
    assert_eq!(rollback.keys, vec![SECOND_KEY.to_vec()]);
}

/// Prewrite verifies the locks this transaction holds and only those.
///
/// Source contract (`buildPrewriteRequest`): a key whose pessimistic lock is
/// held takes `DO_PESSIMISTIC_CHECK`, so TiKV validates the lock instead of
/// redoing a conflict check; a key that was never locked keeps
/// `SKIP_PESSIMISTIC_CHECK`. The request also carries the transaction's
/// `for_update_ts`.
#[test]
fn commit_declares_a_pessimistic_check_only_for_keys_it_actually_locked() {
    let (_server, recorded, mut transaction) = fixture(Vec::new());
    transaction
        .acquire_locks(
            &[PRIMARY_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect("the row is locked");
    let statement_ts = transaction.advance_for_update_ts().expect("a newer TSO");

    let outcome = transaction
        .commit(
            vec![
                OptimisticMutation::put_existing(PRIMARY_KEY.to_vec(), b"locked".to_vec()).unwrap(),
                // Never locked: an index entry written only at commit time.
                OptimisticMutation::index_put(SECOND_KEY.to_vec(), b"index".to_vec()).unwrap(),
            ],
            &call(),
        )
        .expect("the two-phase commit runs to a terminal outcome");
    assert_eq!(
        outcome.state(),
        tidb_txnkv::transaction::OptimisticTransactionState::Committed
    );

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.prewrites.len(), 1);
    let prewrite = &recorded.prewrites[0];
    assert_eq!(prewrite.start_version, START_TS);
    assert_eq!(prewrite.for_update_ts, statement_ts);
    assert_eq!(prewrite.min_commit_ts, statement_ts + 1);
    assert_eq!(prewrite.mutations.len(), 2);
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
        action_for(PRIMARY_KEY),
        KvrpcPessimisticAction::DoPessimisticCheck as i32
    );
    assert_eq!(
        action_for(SECOND_KEY),
        KvrpcPessimisticAction::SkipPessimisticCheck as i32
    );
    assert!(!recorded.commits.is_empty());
}

/// A transaction that locked nothing still commits as a plain optimistic one.
#[test]
fn committing_without_any_lock_keeps_every_key_on_the_optimistic_check() {
    let (_server, recorded, transaction) = fixture(Vec::new());

    transaction
        .commit(
            vec![OptimisticMutation::insert(PRIMARY_KEY.to_vec(), b"v".to_vec()).unwrap()],
            &call(),
        )
        .expect("the two-phase commit runs");

    let recorded = recorded.lock().unwrap();
    assert!(recorded.locks.is_empty());
    let prewrite = &recorded.prewrites[0];
    assert_eq!(prewrite.for_update_ts, START_TS);
    assert_eq!(
        prewrite.pessimistic_actions,
        vec![KvrpcPessimisticAction::SkipPessimisticCheck as i32]
    );
}
