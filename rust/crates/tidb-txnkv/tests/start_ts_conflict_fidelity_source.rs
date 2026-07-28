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

//! One transaction, one `start_ts`: the conflict-detection contract a
//! multi-statement transaction depends on.
//!
//! Go's session holds one `kv.Transaction` from `BEGIN` to `COMMIT`. Its reads
//! are served at that transaction's `start_ts`, and client-go's `2pc.go` sends
//! that same `start_ts` as the prewrite's `start_version`. TiKV then refuses
//! the prewrite when the key has a commit newer than `start_version`, which is
//! the `WriteConflict` a transaction that lost a read-then-write race reports.
//!
//! These regressions pin both halves through the real transport, BatchCommands
//! framing and publication identity: only the region topology and the TiKV
//! responses are scripted, because a racing commit cannot be produced on demand
//! against a live cluster. Real-cluster acceptance stays in
//! `optimistic_2pc_realtikv_source`.

#![allow(missing_docs)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use prost::Message;
use tidb_proto::tikvpb::batch_commands_request::request::Cmd as RequestCmd;
use tidb_proto::tikvpb::batch_commands_response::response::Cmd as ResponseCmd;
use tidb_proto::tikvpb::tikv_server::{Tikv, TikvServer};
use tidb_proto::tikvpb::{batch_commands_response, BatchCommandsRequest, BatchCommandsResponse};
use tidb_proto::{
    CoprocessorRequest, CoprocessorResponse, KvrpcBatchRollbackRequest, KvrpcBatchRollbackResponse,
    KvrpcCommitRequest, KvrpcCommitResponse, KvrpcGetRequest, KvrpcGetResponse, KvrpcKeyError,
    KvrpcPrewriteRequest, KvrpcPrewriteResponse, KvrpcWriteConflict,
};
use tidb_txnkv::lock::FixedTimestampSource;
use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation,
    RegionMetadata, RegionRecoveryLoader, RegionVerId, Store,
};
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext};
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticMutation, OptimisticTransactionState,
    RealOptimisticTransaction, TransactionCause,
};
use tidb_txnkv::SharedReadRuntime;

/// The timestamp `BEGIN` takes. Every read and the prewrite must carry it.
const START_TS: u64 = 100;
/// A commit another transaction made after `BEGIN`, which is what makes the
/// prewrite a conflict.
const RACING_COMMIT_TS: u64 = 150;
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

const ROW_KEY: &[u8] = b"row-1";
const OTHER_ROW_KEY: &[u8] = b"row-2";
const REGION: u64 = 61;

// -----------------------------------------------------------------------------
// Scripted single-region topology
// -----------------------------------------------------------------------------

/// One routable region covering every key, so nothing here depends on the
/// grouping or region-recovery paths those other sources already pin.
#[derive(Clone)]
struct OneRegion {
    address: String,
}

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
                id: 610,
                store_id: 6100,
                role: PeerRole::Voter,
                is_witness: false,
                store_epoch: 1,
            }],
            leader_peer_id: Some(610),
            stores: vec![Store {
                id: 6100,
                address: self.address.clone(),
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
            "these regressions never take the EpochNotMatch hydration path",
        ))
    }
}

// -----------------------------------------------------------------------------
// Scripted TiKV
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Recorded {
    gets: Vec<KvrpcGetRequest>,
    prewrites: Vec<KvrpcPrewriteRequest>,
    commits: Vec<KvrpcCommitRequest>,
    rollbacks: Vec<KvrpcBatchRollbackRequest>,
}

/// One key a racing writer committed, with the timestamp it committed at.
type CommittedKey = (Vec<u8>, u64);

/// A store whose keys hold one value, and which enforces TiKV's own prewrite
/// rule: a key committed after the prewrite's `start_version` is a conflict.
#[derive(Clone)]
struct ScriptedTikv {
    value: Vec<u8>,
    /// Keys a racing writer committed, with the timestamp it committed at. A
    /// prewrite whose `start_version` predates one is refused.
    committed_after: Arc<Mutex<Vec<CommittedKey>>>,
    recorded: Arc<Mutex<Recorded>>,
}

impl ScriptedTikv {
    fn new(committed_after: Vec<CommittedKey>) -> Self {
        Self {
            value: b"snapshot-value".to_vec(),
            committed_after: Arc::new(Mutex::new(committed_after)),
            recorded: Arc::new(Mutex::new(Recorded::default())),
        }
    }

    /// TiKV's prewrite conflict check, in one line: any mutation whose key was
    /// committed after `start_version` loses.
    fn conflict(&self, request: &KvrpcPrewriteRequest) -> Option<KvrpcKeyError> {
        let committed = self.committed_after.lock().unwrap();
        request.mutations.iter().find_map(|mutation| {
            committed
                .iter()
                .find(|(key, commit_ts)| *key == mutation.key && *commit_ts > request.start_version)
                .map(|(key, commit_ts)| KvrpcKeyError {
                    conflict: Some(KvrpcWriteConflict {
                        start_ts: request.start_version,
                        conflict_ts: *commit_ts,
                        conflict_commit_ts: *commit_ts,
                        key: key.clone(),
                        ..KvrpcWriteConflict::default()
                    }),
                    ..KvrpcKeyError::default()
                })
        })
    }

    fn answer(&self, cmd: RequestCmd) -> Result<ResponseCmd, tonic::Status> {
        match cmd {
            RequestCmd::Get(body) => {
                let request = KvrpcGetRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                let value = self.value.clone();
                self.recorded.lock().unwrap().gets.push(request);
                Ok(ResponseCmd::Get(
                    KvrpcGetResponse {
                        value,
                        ..KvrpcGetResponse::default()
                    }
                    .encode_to_vec(),
                ))
            }
            RequestCmd::Prewrite(body) => {
                let request = KvrpcPrewriteRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                let errors = self.conflict(&request).into_iter().collect();
                self.recorded.lock().unwrap().prewrites.push(request);
                Ok(ResponseCmd::Prewrite(
                    KvrpcPrewriteResponse {
                        errors,
                        ..KvrpcPrewriteResponse::default()
                    }
                    .encode_to_vec(),
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
            RequestCmd::BatchRollback(body) => {
                let request = KvrpcBatchRollbackRequest::decode(body.as_slice())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                self.recorded.lock().unwrap().rollbacks.push(request);
                Ok(ResponseCmd::BatchRollback(
                    KvrpcBatchRollbackResponse::default().encode_to_vec(),
                ))
            }
            other => Err(tonic::Status::unimplemented(format!(
                "these regressions publish only Get, Prewrite, Commit and BatchRollback: {other:?}"
            ))),
        }
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
            "these regressions publish transaction commands only",
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

static NEXT_COMMIT_TS: AtomicU64 = AtomicU64::new(1_000);

fn transaction(
    topology: OneRegion,
) -> RealOptimisticTransaction<TonicCoprocessorClient, OneRegion, FixedTimestampSource> {
    let client = TonicCoprocessorClient::new().unwrap();
    let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(topology));
    RealOptimisticTransaction::new_injected(
        runtime,
        FixedTimestampSource::new(NEXT_COMMIT_TS.fetch_add(1, Ordering::Relaxed)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4 * 1024,
    )
    .unwrap()
}

// -----------------------------------------------------------------------------
// Regressions
// -----------------------------------------------------------------------------

/// Several statements' reads and the final prewrite all carry the one timestamp
/// the transaction opened at.
///
/// Source contract: client-go's `2pc.go` prewrites at `txn.startTS`, and every
/// snapshot read of that transaction uses the same version. A per-statement
/// timestamp would show up here as a `version` or `start_version` that is not
/// `START_TS`, which is exactly the divergence this pins shut.
#[test]
fn every_read_and_the_prewrite_carry_the_transactions_one_start_ts() {
    let service = ScriptedTikv::new(Vec::new());
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let topology = OneRegion {
        address: server.store_address(),
    };
    let mut transaction = transaction(topology);
    let call = UnaryCallContext::with_timeout(CALL_TIMEOUT);

    // Three statements of one transaction: read, read again, then write.
    for _ in 0..3 {
        let read = transaction
            .snapshot_get(ROW_KEY, &call)
            .expect("a re-entered read phase serves the next statement");
        assert_eq!(read.start_ts, START_TS);
        assert_eq!(read.value.as_deref(), Some(b"snapshot-value".as_slice()));
    }

    let outcome = transaction
        .commit(
            vec![OptimisticMutation::put_existing(ROW_KEY.to_vec(), b"new".to_vec()).unwrap()],
            &call,
        )
        .expect("the coordinator must return a terminal outcome");
    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    assert_eq!(outcome.receipt().start_ts, START_TS);

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.gets.len(), 3);
    assert!(
        recorded.gets.iter().all(|get| get.version == START_TS),
        "every statement of one transaction reads at its start timestamp: {:?}",
        recorded.gets.iter().map(|get| get.version).collect::<Vec<_>>()
    );
    assert_eq!(recorded.prewrites.len(), 1);
    assert_eq!(
        recorded.prewrites[0].start_version, START_TS,
        "the prewrite must carry the timestamp the reads used, not a newer one"
    );
    assert_eq!(recorded.commits.len(), 1);
    assert_eq!(recorded.commits[0].start_version, START_TS);
    assert!(recorded.commits[0].commit_version > START_TS);
}

/// A transaction whose key was committed by someone else after it opened is
/// refused at prewrite and reports a write conflict.
///
/// Source contract: TiKV rejects a prewrite whose key has a commit newer than
/// `start_version` with `KeyError.conflict`, and `kv.ErrWriteConflict` (9007)
/// is what the client sees. The transaction rolls back every possibly
/// prewritten key rather than reporting a partial write.
#[test]
fn a_commit_newer_than_the_start_ts_makes_the_prewrite_a_write_conflict() {
    let service = ScriptedTikv::new(vec![(ROW_KEY.to_vec(), RACING_COMMIT_TS)]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let topology = OneRegion {
        address: server.store_address(),
    };
    let mut transaction = transaction(topology);
    let call = UnaryCallContext::with_timeout(CALL_TIMEOUT);

    // The read happens before the racing commit lands, so the transaction has
    // no way to learn of it until it tries to publish.
    transaction
        .snapshot_get(ROW_KEY, &call)
        .expect("the statement's read succeeds");

    let outcome = transaction
        .commit(
            vec![OptimisticMutation::put_existing(ROW_KEY.to_vec(), b"new".to_vec()).unwrap()],
            &call,
        )
        .expect("the coordinator must return a terminal outcome, not a caller error");

    assert_eq!(outcome.state(), OptimisticTransactionState::RolledBack);
    let OptimisticCommitOutcome::RolledBack(rolled_back) = outcome else {
        panic!("a lost race must not be reported as committed");
    };
    let TransactionCause::WriteConflict { detail } = &rolled_back.cause else {
        panic!(
            "a commit newer than start_ts is a write conflict, got {:?}",
            rolled_back.cause
        );
    };
    assert!(
        detail.contains(&RACING_COMMIT_TS.to_string()),
        "the conflicting commit timestamp must survive into the diagnostic: {detail}"
    );

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.prewrites[0].start_version, START_TS);
    assert!(
        recorded.commits.is_empty(),
        "a refused prewrite never commits"
    );
    assert_eq!(
        recorded.rollbacks.len(),
        1,
        "every possibly prewritten key is cleaned up"
    );
    assert_eq!(recorded.rollbacks[0].start_version, START_TS);
}

/// A key nobody raced is published normally even when another key of the same
/// cluster was committed after this transaction opened.
///
/// The conflict check is per key, so a transaction that touched only untouched
/// rows must not be punished for someone else's commit elsewhere.
#[test]
fn a_racing_commit_on_another_key_does_not_refuse_the_prewrite() {
    let service = ScriptedTikv::new(vec![(OTHER_ROW_KEY.to_vec(), RACING_COMMIT_TS)]);
    let server = TestServer::start(service);
    let topology = OneRegion {
        address: server.store_address(),
    };
    let mut transaction = transaction(topology);
    let call = UnaryCallContext::with_timeout(CALL_TIMEOUT);
    transaction.snapshot_get(ROW_KEY, &call).expect("read");
    let outcome = transaction
        .commit(
            vec![OptimisticMutation::put_existing(ROW_KEY.to_vec(), b"new".to_vec()).unwrap()],
            &call,
        )
        .expect("terminal outcome");
    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
}
