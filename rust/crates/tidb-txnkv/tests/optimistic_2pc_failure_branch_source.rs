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

//! Normal optimistic 2PC failure branches that a live cluster cannot produce
//! on demand.
//!
//! The transport, BatchCommands framing, publication identity, region-error
//! recovery, and regrouping are the real production paths. Only the TiKV
//! responses and the region topology are scripted, because both branches
//! require a region error to be followed by a region that can no longer be
//! routed. Real-cluster acceptance stays in `optimistic_2pc_realtikv_source`.

#![allow(missing_docs)]

use std::collections::HashMap;
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
    CoprocessorRequest, CoprocessorResponse, KvrpcAlreadyExist, KvrpcBatchRollbackRequest,
    KvrpcBatchRollbackResponse, KvrpcCommitRequest, KvrpcCommitResponse, KvrpcCommitRole,
    KvrpcKeyError, KvrpcPrewriteRequest, KvrpcPrewriteResponse,
};
use tidb_txnkv::lock::FixedTimestampSource;
use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation,
    RegionMetadata, RegionRecoveryLoader, RegionVerId, Store,
};
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext};
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticMutation, OptimisticTransactionState,
    RealOptimisticTransaction, TransactionAttemptPhase, TransactionAttemptResult, TransactionCause,
};
use tidb_txnkv::SharedReadRuntime;

const START_TS: u64 = 100;
const COMMIT_TS: u64 = 200;
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Primary key: lexicographically smallest, so it selects `LOW_REGION`.
const PRIMARY_KEY: &[u8] = b"a";
/// Secondary key: routed to `HIGH_REGION` by the split at `m`.
const SECONDARY_KEY: &[u8] = b"m";
const SPLIT_KEY: &[u8] = b"m";

const LOW_REGION: u64 = 41;
const HIGH_REGION: u64 = 42;

// -----------------------------------------------------------------------------
// Scripted region topology
// -----------------------------------------------------------------------------

/// Region loader that answers the first load of each region with a routable
/// leader and every later load without one.
///
/// A region error invalidates its region, so the coordinator's regroup is the
/// second load and fails in `route_from_location`. This models a region whose
/// leader is unknown at exactly the moment cleanup or secondary commit needs
/// it, without racing the scripted TiKV thread.
#[derive(Clone)]
struct SplitTopology {
    address: String,
    loads: Arc<Mutex<HashMap<u64, u64>>>,
}

impl SplitTopology {
    fn new(address: String) -> Self {
        Self {
            address,
            loads: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn loads_for(&self, region_id: u64) -> u64 {
        *self.loads.lock().unwrap().get(&region_id).unwrap_or(&0)
    }

    fn region_for(key: &[u8]) -> (u64, Vec<u8>, Vec<u8>) {
        if key < SPLIT_KEY {
            (LOW_REGION, Vec::new(), SPLIT_KEY.to_vec())
        } else {
            (HIGH_REGION, SPLIT_KEY.to_vec(), Vec::new())
        }
    }

    fn location(
        &self,
        region_id: u64,
        start: Vec<u8>,
        end: Vec<u8>,
        routable: bool,
    ) -> RegionLocation {
        let peer_id = region_id * 10;
        let store_id = region_id * 100;
        RegionLocation {
            region: RegionVerId {
                id: region_id,
                epoch: RegionEpoch {
                    conf_ver: 1,
                    version: 1,
                },
            },
            start_key: start,
            end_key: end,
            peers: vec![Peer {
                id: peer_id,
                store_id,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 1,
            }],
            // Dropping the leader is what makes the second load unroutable.
            leader_peer_id: routable.then_some(peer_id),
            stores: vec![Store {
                id: store_id,
                address: self.address.clone(),
                epoch: 1,
            }],
            ..RegionLocation::default()
        }
    }
}

impl RegionLoader for SplitTopology {
    fn cluster_id(&self) -> u64 {
        7
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        let (region_id, start, end) = Self::region_for(key);
        let loads = {
            let mut loads = self.loads.lock().unwrap();
            let counter = loads.entry(region_id).or_insert(0);
            *counter += 1;
            *counter
        };
        Ok(self.location(region_id, start, end, loads == 1))
    }
}

impl RegionRecoveryLoader for SplitTopology {
    fn hydrate_region(
        &mut self,
        _metadata: &RegionMetadata,
        _leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-hydration",
            "these branches never take the EpochNotMatch hydration path",
        ))
    }
}

// -----------------------------------------------------------------------------
// Scripted TiKV
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommandOutcome {
    Ok,
    /// Invalidates the region and yields a retryable rebuild disposition.
    RecoveryInProgress,
    /// Definitive key error that ends the prewrite phase.
    AlreadyExists,
}

#[derive(Debug, Default)]
struct Recorded {
    prewrites: Vec<KvrpcPrewriteRequest>,
    commits: Vec<KvrpcCommitRequest>,
    rollbacks: Vec<KvrpcBatchRollbackRequest>,
}

/// Scripts one outcome per command occurrence, keyed by command kind.
#[derive(Clone)]
struct ScriptedTikv {
    prewrites: Arc<Mutex<Vec<CommandOutcome>>>,
    commits: Arc<Mutex<Vec<CommandOutcome>>>,
    rollbacks: Arc<Mutex<Vec<CommandOutcome>>>,
    recorded: Arc<Mutex<Recorded>>,
}

impl ScriptedTikv {
    fn new(
        prewrites: Vec<CommandOutcome>,
        commits: Vec<CommandOutcome>,
        rollbacks: Vec<CommandOutcome>,
    ) -> Self {
        Self {
            prewrites: Arc::new(Mutex::new(prewrites)),
            commits: Arc::new(Mutex::new(commits)),
            rollbacks: Arc::new(Mutex::new(rollbacks)),
            recorded: Arc::new(Mutex::new(Recorded::default())),
        }
    }

    fn next(script: &Mutex<Vec<CommandOutcome>>) -> CommandOutcome {
        let mut script = script.lock().unwrap();
        if script.is_empty() {
            return CommandOutcome::Ok;
        }
        script.remove(0)
    }
}

fn region_error(outcome: CommandOutcome) -> Option<errorpb::Error> {
    match outcome {
        CommandOutcome::RecoveryInProgress => Some(errorpb::Error {
            recovery_in_progress: Some(errorpb::RecoveryInProgress { region_id: 0 }),
            ..errorpb::Error::default()
        }),
        CommandOutcome::Ok | CommandOutcome::AlreadyExists => None,
    }
}

fn already_exists_error(key: &[u8]) -> KvrpcKeyError {
    KvrpcKeyError {
        already_exist: Some(KvrpcAlreadyExist { key: key.to_vec() }),
        ..KvrpcKeyError::default()
    }
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
            "these branches publish transaction commands only",
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
                    let cmd = match request.cmd {
                        Some(cmd) => cmd,
                        None => continue,
                    };
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
            RequestCmd::Prewrite(body) => {
                let request = KvrpcPrewriteRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                let outcome = Self::next(&self.prewrites);
                let errors = match outcome {
                    CommandOutcome::AlreadyExists => vec![already_exists_error(
                        request
                            .mutations
                            .first()
                            .map(|mutation| mutation.key.clone())
                            .unwrap_or_default()
                            .as_slice(),
                    )],
                    CommandOutcome::Ok | CommandOutcome::RecoveryInProgress => Vec::new(),
                };
                self.recorded.lock().unwrap().prewrites.push(request);
                Ok(ResponseCmd::Prewrite(
                    KvrpcPrewriteResponse {
                        region_error: region_error(outcome),
                        errors,
                        ..KvrpcPrewriteResponse::default()
                    }
                    .encode_to_vec(),
                ))
            }
            RequestCmd::Commit(body) => {
                let request = KvrpcCommitRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                let outcome = Self::next(&self.commits);
                self.recorded.lock().unwrap().commits.push(request);
                Ok(ResponseCmd::Commit(
                    KvrpcCommitResponse {
                        region_error: region_error(outcome),
                        ..KvrpcCommitResponse::default()
                    }
                    .encode_to_vec(),
                ))
            }
            RequestCmd::BatchRollback(body) => {
                let request = KvrpcBatchRollbackRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                let outcome = Self::next(&self.rollbacks);
                self.recorded.lock().unwrap().rollbacks.push(request);
                Ok(ResponseCmd::BatchRollback(
                    KvrpcBatchRollbackResponse {
                        region_error: region_error(outcome),
                        ..KvrpcBatchRollbackResponse::default()
                    }
                    .encode_to_vec(),
                ))
            }
            other => Err(tonic::Status::unimplemented(format!(
                "these branches publish only Prewrite, Commit, and BatchRollback: {other:?}"
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

    /// Physical address exactly as the scripted topology publishes it.
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

static NEXT_TIMESTAMP: AtomicU64 = AtomicU64::new(COMMIT_TS);

fn transaction(
    server: &TestServer,
    topology: SplitTopology,
) -> RealOptimisticTransaction<TonicCoprocessorClient, SplitTopology, FixedTimestampSource> {
    let client = TonicCoprocessorClient::new().unwrap();
    let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(topology));
    assert_eq!(runtime.cluster_id(), 7);
    let _ = server;
    RealOptimisticTransaction::new_injected(
        runtime,
        FixedTimestampSource::new(NEXT_TIMESTAMP.fetch_add(1, Ordering::Relaxed)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4 * 1024,
    )
    .unwrap()
}

fn two_region_mutations() -> Vec<OptimisticMutation> {
    vec![
        OptimisticMutation::insert(PRIMARY_KEY.to_vec(), b"primary-value".to_vec()).unwrap(),
        OptimisticMutation::insert(SECONDARY_KEY.to_vec(), b"secondary-value".to_vec()).unwrap(),
    ]
}

// -----------------------------------------------------------------------------
// Regressions
// -----------------------------------------------------------------------------

/// A confirmed primary commit stays committed when the secondary batch cannot
/// be regrouped after its region error.
///
/// Source contract: a successful primary response makes the transaction
/// committed. Unresolved secondary keys are reported on the receipt; they must
/// never become rollback, cleanup failure, or undetermined, and cleanup must
/// not run.
#[test]
fn secondary_commit_regroup_failure_keeps_a_determinate_committed_outcome() {
    let service = ScriptedTikv::new(
        vec![CommandOutcome::Ok, CommandOutcome::Ok],
        // Primary batch commits; the secondary batch hits a region error whose
        // recovery invalidates the region, and the regroup then finds no leader.
        vec![CommandOutcome::Ok, CommandOutcome::RecoveryInProgress],
        Vec::new(),
    );
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let topology = SplitTopology::new(server.store_address());
    let transaction = transaction(&server, topology.clone());

    let outcome = transaction
        .commit(
            two_region_mutations(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("the coordinator must return a terminal outcome, not a caller error");

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("a confirmed primary commit must stay Committed");
    };

    // The secondary failure is reported with full physical identity.
    assert_eq!(committed.secondary_failures.len(), 1);
    let failure = &committed.secondary_failures[0];
    assert_eq!(failure.keys, vec![SECONDARY_KEY.to_vec()]);
    assert_eq!(failure.region.map(|region| region.id), Some(HIGH_REGION));
    assert_eq!(
        failure.address.as_deref(),
        Some(server.store_address().as_str())
    );
    assert!(
        failure.publication.is_some(),
        "a decoded secondary response must retain its publication identity"
    );
    let TransactionCause::Region { detail } = &failure.cause else {
        panic!(
            "a failed regroup is a region cause, got {:?}",
            failure.cause
        );
    };
    assert!(
        detail.contains("secondary Commit regroup failed"),
        "unexpected regroup detail: {detail}"
    );

    // The primary is committed and no secondary was confirmed.
    let receipt = &committed.receipt;
    assert_eq!(receipt.start_ts, START_TS);
    assert!(receipt.commit_ts > START_TS);
    assert_eq!(receipt.primary_key, PRIMARY_KEY.to_vec());
    assert_eq!(receipt.primary_publications.len(), 1);
    assert!(receipt.secondary_publications.is_empty());
    assert_eq!(receipt.secondary_attempt_publications.len(), 1);

    let secondary_attempts = receipt
        .attempt_history
        .iter()
        .filter(|attempt| attempt.phase == TransactionAttemptPhase::SecondaryCommit)
        .collect::<Vec<_>>();
    assert_eq!(secondary_attempts.len(), 1);
    assert!(matches!(
        secondary_attempts[0].result,
        TransactionAttemptResult::DefinitiveFailure(TransactionCause::Region { .. })
    ));

    // A committed transaction never cleans up.
    let recorded = recorded.lock().unwrap();
    assert!(
        recorded.rollbacks.is_empty(),
        "a committed transaction must not publish BatchRollback"
    );
    assert_eq!(recorded.prewrites.len(), 2);
    assert_eq!(recorded.commits.len(), 2);
    assert_eq!(
        recorded.commits[0].commit_role,
        KvrpcCommitRole::Primary as i32,
        "the primary batch must commit before any secondary"
    );
    assert_eq!(recorded.commits[0].keys, vec![PRIMARY_KEY.to_vec()]);
    assert_eq!(
        recorded.commits[1].commit_role,
        KvrpcCommitRole::Secondary as i32
    );
    assert_eq!(recorded.commits[1].keys, vec![SECONDARY_KEY.to_vec()]);
    assert!(recorded
        .commits
        .iter()
        .all(|commit| commit.primary_key == PRIMARY_KEY.to_vec()));
    // Exactly one reload proves the regroup consulted the invalidated region.
    assert_eq!(topology.loads_for(HIGH_REGION), 2);
    assert_eq!(topology.loads_for(LOW_REGION), 1);
}

/// A rollback batch that cannot be regrouped after its region error reports
/// `CleanupFailed`, keeping the original cause and the outstanding keys.
#[test]
fn rollback_regroup_failure_reports_cleanup_failed_with_outstanding_keys() {
    let service = ScriptedTikv::new(
        // The second prewrite is definitively rejected, so cleanup must run for
        // every possibly-prewritten key.
        vec![CommandOutcome::Ok, CommandOutcome::AlreadyExists],
        Vec::new(),
        // The primary region's rollback hits a region error; its regroup then
        // finds no leader. The other batch is cleaned normally.
        vec![CommandOutcome::RecoveryInProgress, CommandOutcome::Ok],
    );
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let topology = SplitTopology::new(server.store_address());
    let transaction = transaction(&server, topology.clone());

    let outcome = transaction
        .commit(
            two_region_mutations(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("the coordinator must return a terminal outcome, not a caller error");

    assert_eq!(outcome.state(), OptimisticTransactionState::CleanupFailed);
    let OptimisticCommitOutcome::CleanupFailed(cleanup_failed) = outcome else {
        panic!("incomplete cleanup must not be reported as a clean rollback");
    };

    // The original prewrite rejection survives cleanup.
    assert!(
        matches!(
            &cleanup_failed.cause,
            TransactionCause::AlreadyExists { key, .. } if key == SECONDARY_KEY
        ),
        "cleanup must not overwrite the original cause: {:?}",
        cleanup_failed.cause
    );

    assert_eq!(cleanup_failed.cleanup_failures.len(), 1);
    let failure = &cleanup_failed.cleanup_failures[0];
    assert_eq!(failure.keys, vec![PRIMARY_KEY.to_vec()]);
    assert_eq!(failure.region.map(|region| region.id), Some(LOW_REGION));
    assert_eq!(
        failure.address.as_deref(),
        Some(server.store_address().as_str())
    );
    assert!(
        failure.publication.is_some(),
        "a decoded rollback response must retain its publication identity"
    );
    let TransactionCause::Region { detail } = &failure.cause else {
        panic!(
            "a failed regroup is a region cause, got {:?}",
            failure.cause
        );
    };
    assert!(
        detail.contains("BatchRollback regroup failed"),
        "unexpected regroup detail: {detail}"
    );

    // The other batch was still cleaned, and no commit was ever published.
    let receipt = &cleanup_failed.receipt;
    assert_eq!(receipt.commit_ts, 0);
    assert!(receipt.primary_publications.is_empty());
    assert_eq!(receipt.rollback_publications.len(), 1);
    assert_eq!(receipt.rollback_attempt_publications.len(), 2);

    let rollback_results = receipt
        .attempt_history
        .iter()
        .filter(|attempt| attempt.phase == TransactionAttemptPhase::BatchRollback)
        .map(|attempt| attempt.result.clone())
        .collect::<Vec<_>>();
    assert_eq!(rollback_results.len(), 2);
    assert!(matches!(
        rollback_results[0],
        TransactionAttemptResult::DefinitiveFailure(TransactionCause::Region { .. })
    ));
    assert!(matches!(
        rollback_results[1],
        TransactionAttemptResult::Confirmed
    ));

    let recorded = recorded.lock().unwrap();
    assert!(recorded.commits.is_empty(), "cleanup must never commit");
    assert_eq!(recorded.rollbacks.len(), 2);
    assert!(recorded
        .rollbacks
        .iter()
        .all(|rollback| rollback.start_version == START_TS));
    assert_eq!(topology.loads_for(LOW_REGION), 2);
    assert_eq!(topology.loads_for(HIGH_REGION), 1);
}
