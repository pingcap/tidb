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

//! Async commit and 1PC decisions, and every fallback back to normal 2PC.
//!
//! The transport, BatchCommands framing, publication identity, region grouping,
//! and the coordinator are the real production paths. Only the TiKV responses
//! and the region topology are scripted, because whether TiKV grants 1PC or a
//! nonzero `min_commit_ts` is exactly the decision under test and a live
//! cluster will not make it on demand. Real-cluster acceptance stays in
//! `run-realtikv-optimistic-2pc.sh`.

#![allow(missing_docs)]

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    KvrpcCommitRequest, KvrpcCommitResponse, KvrpcCommitRole, KvrpcPrewriteRequest,
    KvrpcPrewriteResponse,
};
use tidb_txnkv::lock::TimestampSource;
use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation,
    RegionMetadata, RegionRecoveryLoader, RegionVerId, Store,
};
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext};
use tidb_txnkv::transaction::{
    CommitProtocol, CommittedProtocol, OptimisticCommitOutcome, OptimisticMutation,
    OptimisticTransactionState, RealOptimisticTransaction, TransactionAttemptPhase,
};
use tidb_txnkv::{set_txn_resource_group, SharedReadRuntime};

const START_TS: u64 = 1_000 << 18;
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

/// A stable two-region split at `m`, always routable: these branches are about
/// the commit protocol, not about region recovery.
#[derive(Clone)]
struct SplitTopology {
    address: String,
}

fn encode_boundary(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return Vec::new();
    }
    let mut encoded = Vec::new();
    tidb_codec::encode_bytes(&mut encoded, key);
    encoded
}
fn decode_boundary(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return Vec::new();
    }
    tidb_codec::decode_bytes(key).unwrap().1
}
impl RegionLoader for SplitTopology {
    fn cluster_id(&self) -> u64 {
        7
    }

    fn load_region(&mut self, key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        let (region_id, start, end) = if key < SPLIT_KEY {
            (LOW_REGION, Vec::new(), SPLIT_KEY.to_vec())
        } else {
            (HIGH_REGION, SPLIT_KEY.to_vec(), Vec::new())
        };
        let peer_id = region_id * 10;
        let store_id = region_id * 100;
        Ok(RegionLocation {
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
            leader_peer_id: Some(peer_id),
            stores: vec![Store {
                id: store_id,
                address: self.address.clone(),
                epoch: 1,
            }],
            ..RegionLocation::default()
        })
    }
}

impl RegionRecoveryLoader for SplitTopology {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        _leader_store_id: u64,
        _resolved_stores: &mut std::collections::BTreeMap<
            u64,
            Option<tidb_txnkv::region::StoreMetadata>,
        >,
    ) -> Result<RegionLocation, RegionLoadError> {
        let mut location = self.load_region(&decode_boundary(&metadata.encoded_start_key))?;
        location.region = metadata.region;
        location.start_key = decode_boundary(&metadata.encoded_start_key);
        location.end_key = decode_boundary(&metadata.encoded_end_key);
        location.peers = metadata
            .peers
            .iter()
            .map(|peer| Peer {
                id: peer.id,
                store_id: peer.store_id,
                role: peer.role,
                is_witness: peer.is_witness,
                store_epoch: 1,
            })
            .collect();
        location.stores = location
            .peers
            .iter()
            .map(|peer| Store {
                id: peer.store_id,
                address: self.address.clone(),
                epoch: 1,
            })
            .collect();
        location.leader_peer_id = location.peers.first().map(|peer| peer.id);
        Ok(location)
    }
}

// -----------------------------------------------------------------------------
// Timestamp authority that reports whether it was ever consulted
// -----------------------------------------------------------------------------

/// A PD stand-in that counts allocations.
///
/// Counts the prewrite consistency check separately from a fallback commit TSO.
#[derive(Debug)]
struct CountingTimestampSource {
    next: Cell<u64>,
    calls: Arc<AtomicU64>,
}

impl CountingTimestampSource {
    fn new(first: u64) -> (Self, Arc<AtomicU64>) {
        let calls = Arc::new(AtomicU64::new(0));
        (
            Self {
                next: Cell::new(first),
                calls: Arc::clone(&calls),
            },
            calls,
        )
    }
}

impl TimestampSource for CountingTimestampSource {
    fn current_ts(&self) -> Result<u64, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let timestamp = self.next.get();
        self.next.set(timestamp + (1 << 18));
        Ok(timestamp)
    }
}

// -----------------------------------------------------------------------------
// Scripted TiKV
// -----------------------------------------------------------------------------

/// What TiKV answers one prewrite with, beyond an empty success.
#[derive(Clone, Copy, Debug)]
struct PrewriteAnswer {
    min_commit_ts: u64,
    one_pc_commit_ts: u64,
}

#[derive(Debug, Default)]
struct Recorded {
    prewrites: Vec<KvrpcPrewriteRequest>,
    commits: Vec<KvrpcCommitRequest>,
    rollbacks: Vec<KvrpcBatchRollbackRequest>,
}

/// Answers prewrites from a script, in publication order.
#[derive(Clone)]
struct ScriptedTikv {
    prewrites: Arc<Mutex<Vec<PrewriteAnswer>>>,
    recorded: Arc<Mutex<Recorded>>,
    hold_commit: Option<(u64, tokio::sync::watch::Receiver<bool>)>,
    split_secondary: Arc<AtomicBool>,
}

impl ScriptedTikv {
    fn new(prewrites: Vec<PrewriteAnswer>) -> Self {
        Self {
            prewrites: Arc::new(Mutex::new(prewrites)),
            recorded: Arc::new(Mutex::new(Recorded::default())),
            hold_commit: None,
            split_secondary: Arc::new(AtomicBool::new(false)),
        }
    }

    fn next_prewrite(&self) -> PrewriteAnswer {
        let mut script = self.prewrites.lock().unwrap();
        assert!(
            !script.is_empty(),
            "the coordinator published more prewrites than the script answers"
        );
        script.remove(0)
    }
}

#[tonic::async_trait]
impl Tikv for ScriptedTikv {
    type BatchCommandsStream =
        tokio_stream::wrappers::ReceiverStream<Result<BatchCommandsResponse, tonic::Status>>;

    // Go parity: `pkg/store/mockstore/unistore/tikv/server.go`'s `Server`
    // embeds `tikvpb.UnimplementedTikvServer` so a `tikvpb.proto` addition
    // (like this MPP RPC group) never breaks compilation; Rust has no
    // embedding equivalent for a required trait method, so each method
    // delegates to the shared stub in `tidb_proto::unimplemented_tikv`.
    type EstablishMPPConnectionStream = tidb_proto::unimplemented_tikv::NeverStream;

    async fn dispatch_mpp_task(
        &self,
        request: tonic::Request<tidb_proto::mpp::DispatchTaskRequest>,
    ) -> Result<tonic::Response<tidb_proto::mpp::DispatchTaskResponse>, tonic::Status> {
        tidb_proto::unimplemented_tikv::dispatch_mpp_task(request).await
    }

    async fn cancel_mpp_task(
        &self,
        request: tonic::Request<tidb_proto::mpp::CancelTaskRequest>,
    ) -> Result<tonic::Response<tidb_proto::mpp::CancelTaskResponse>, tonic::Status> {
        tidb_proto::unimplemented_tikv::cancel_mpp_task(request).await
    }

    async fn establish_mpp_connection(
        &self,
        request: tonic::Request<tidb_proto::mpp::EstablishMppConnectionRequest>,
    ) -> Result<tonic::Response<Self::EstablishMPPConnectionStream>, tonic::Status> {
        tidb_proto::unimplemented_tikv::establish_mpp_connection(request).await
    }

    async fn report_mpp_task_status(
        &self,
        request: tonic::Request<tidb_proto::mpp::ReportTaskStatusRequest>,
    ) -> Result<tonic::Response<tidb_proto::mpp::ReportTaskStatusResponse>, tonic::Status> {
        tidb_proto::unimplemented_tikv::report_mpp_task_status(request).await
    }

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
                    let Some(cmd) = request.cmd else {
                        continue;
                    };
                    let mut held = service
                        .hold_commit
                        .as_ref()
                        .and_then(|(start_ts, release)| match &cmd {
                            RequestCmd::Commit(body)
                                if KvrpcCommitRequest::decode(body.as_ref())
                                    .unwrap()
                                    .start_version
                                    == *start_ts
                                    && KvrpcCommitRequest::decode(body.as_ref())
                                        .unwrap()
                                        .commit_role
                                        == KvrpcCommitRole::Secondary as i32 =>
                            {
                                Some(release.clone())
                            }
                            _ => None,
                        });
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
                    if let Some(release) = held.as_mut() {
                        while !*release.borrow() {
                            if release.changed().await.is_err() {
                                break;
                            }
                        }
                    }
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
                let request = KvrpcPrewriteRequest::decode(body.as_ref())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                let answer = self.next_prewrite();
                self.recorded.lock().unwrap().prewrites.push(request);
                Ok(ResponseCmd::Prewrite(
                    KvrpcPrewriteResponse {
                        min_commit_ts: answer.min_commit_ts,
                        one_pc_commit_ts: answer.one_pc_commit_ts,
                        ..KvrpcPrewriteResponse::default()
                    }
                    .encode_to_vec()
                    .into(),
                ))
            }
            RequestCmd::Commit(body) => {
                let request = KvrpcCommitRequest::decode(body.as_ref())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                let split = request.commit_role == KvrpcCommitRole::Secondary as i32
                    && self.split_secondary.swap(false, Ordering::SeqCst);
                self.recorded.lock().unwrap().commits.push(request);
                if split {
                    let regions = [
                        (HIGH_REGION, b"m".to_vec(), b"t".to_vec()),
                        (HIGH_REGION + 1, b"t".to_vec(), Vec::new()),
                    ]
                    .into_iter()
                    .map(|(id, start_key, end_key)| tidb_proto::metapb::Region {
                        id,
                        start_key: encode_boundary(&start_key),
                        end_key: encode_boundary(&end_key),
                        region_epoch: Some(tidb_proto::metapb::RegionEpoch {
                            conf_ver: 1,
                            version: 2,
                        }),
                        peers: vec![tidb_proto::metapb::Peer {
                            id: id * 10,
                            store_id: HIGH_REGION * 100,
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .collect();
                    return Ok(ResponseCmd::Commit(
                        KvrpcCommitResponse {
                            region_error: Some(tidb_proto::RegionError {
                                epoch_not_match: Some(tidb_proto::errorpb::EpochNotMatch {
                                    current_regions: regions,
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }
                        .encode_to_vec()
                        .into(),
                    ));
                }
                Ok(ResponseCmd::Commit(
                    KvrpcCommitResponse::default().encode_to_vec().into(),
                ))
            }
            RequestCmd::BatchRollback(body) => {
                let request = KvrpcBatchRollbackRequest::decode(body.as_ref())
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                self.recorded.lock().unwrap().rollbacks.push(request);
                Ok(ResponseCmd::BatchRollback(
                    KvrpcBatchRollbackResponse::default().encode_to_vec().into(),
                ))
            }
            other => Err(tonic::Status::unimplemented(format!(
                "these branches publish only Prewrite, Commit, and BatchRollback: {other:?}"
            ))),
        }
    }
}

// Go spawnWithStorePool allows another transaction's secondaries to progress
// while beforeCommitSecondaries or a slow response holds the first task.
#[test]
fn detached_secondary_flushes_progress_independently() {
    let (release, held) = tokio::sync::watch::channel(false);
    let mut service = ScriptedTikv::new(vec![ok(0), ok(0), ok(0), ok(0)]);
    service.hold_commit = Some((10, held));
    let server = TestServer::start(service);
    let (mut first, _) = transaction_at(server.store_address(), CommitProtocol::default(), 10);
    let first_observed = first.observe_detached_commits();
    assert!(matches!(
        first
            .commit(
                two_region_mutations(),
                &UnaryCallContext::with_timeout(CALL_TIMEOUT)
            )
            .unwrap(),
        OptimisticCommitOutcome::Committed(_)
    ));
    let (mut second, _) = transaction_at(server.store_address(), CommitProtocol::default(), 20);
    let second_observed = second.observe_detached_commits();
    assert!(matches!(
        second
            .commit(
                two_region_mutations(),
                &UnaryCallContext::with_timeout(CALL_TIMEOUT)
            )
            .unwrap(),
        OptimisticCommitOutcome::Committed(_)
    ));
    let second_response = second_observed.recv_timeout(Duration::from_secs(2));
    let first_is_held = matches!(first_observed.try_recv(), Err(mpsc::TryRecvError::Empty));
    release.send(true).unwrap();
    let response = second_response
        .expect("another transaction's secondary must progress")
        .unwrap();
    assert!(response.response.error.is_none());
    assert!(
        first_is_held,
        "the first secondary is still waiting for its response"
    );
    assert!(first_observed
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap()
        .response
        .error
        .is_none());
}

/// Go commit.go relocates and regroups the detached secondary batch after a
/// split; 2pc.go keeps that retry inside the store-owned background task.
#[test]
fn detached_secondary_commit_regroups_after_split() {
    for async_commit in [false, true] {
        let service = ScriptedTikv::new(vec![ok(1_100 << 18), ok(1_200 << 18)]);
        service.split_secondary.store(true, Ordering::SeqCst);
        let recorded = Arc::clone(&service.recorded);
        let server = TestServer::start(service);
        let (mut transaction, _) = transaction(
            server.store_address(),
            CommitProtocol {
                async_commit,
                one_pc: false,
            },
        );
        set_txn_resource_group(&mut transaction, "split-recovery");
        let observed = transaction.observe_detached_commits();
        let mut mutations = two_region_mutations();
        mutations.push(OptimisticMutation::insert(b"z".to_vec(), b"tail".to_vec()).unwrap());
        let outcome = transaction
            .commit(mutations, &UnaryCallContext::with_timeout(CALL_TIMEOUT))
            .unwrap();
        assert!(matches!(outcome, OptimisticCommitOutcome::Committed(_)));
        for _ in 0..if async_commit { 3 } else { 2 } {
            let response = observed
                .recv_timeout(CALL_TIMEOUT)
                .unwrap_or_else(|e| {
                    panic!(
                        "async={async_commit} observer={e:?} commits={:?}",
                        recorded.lock().unwrap().commits
                    )
                })
                .unwrap();
            assert!(
                response.response.region_error.is_none(),
                "a split must regroup, not end the detached commit"
            );
            assert!(response.response.error.is_none());
        }
        let commits = &recorded.lock().unwrap().commits;
        assert_eq!(
            commits.len(),
            4,
            "primary, rejected secondary, and two regrouped batches"
        );
        let mut retried: Vec<_> = commits[2..]
            .iter()
            .map(|request| request.keys.clone())
            .collect();
        retried.sort();
        assert_eq!(retried, vec![vec![b"m".to_vec()], vec![b"z".to_vec()]]);
        for request in &commits[2..] {
            assert_eq!(request.primary_key, PRIMARY_KEY);
            assert_eq!(request.use_async_commit, async_commit);
            assert_eq!(request.commit_role, KvrpcCommitRole::Secondary as i32);
            assert_eq!(request.start_version, START_TS);
            assert_eq!(request.commit_version, outcome.receipt().commit_ts);
            let context = request.context.as_ref().unwrap();
            assert!(context.is_retry_request);
            assert_eq!(context.region_epoch.as_ref().unwrap().version, 2);
            assert_eq!(
                context.region_id,
                if request.keys[0] == b"m" {
                    HIGH_REGION
                } else {
                    HIGH_REGION + 1
                }
            );
            assert_eq!(
                context
                    .resource_control_context
                    .as_ref()
                    .unwrap()
                    .resource_group_name,
                "split-recovery"
            );
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

/// The commit timestamp PD would hand out, well past every scripted
/// `min_commit_ts`, so a normal-2PC fallback is unmistakable in the receipt.
const PD_COMMIT_TS: u64 = START_TS + (10 << 18);

fn transaction(
    address: String,
    protocol: CommitProtocol,
) -> (
    RealOptimisticTransaction<TonicCoprocessorClient, SplitTopology, CountingTimestampSource>,
    Arc<AtomicU64>,
) {
    transaction_at(address, protocol, START_TS)
}

fn transaction_at(
    address: String,
    protocol: CommitProtocol,
    start_ts: u64,
) -> (
    RealOptimisticTransaction<TonicCoprocessorClient, SplitTopology, CountingTimestampSource>,
    Arc<AtomicU64>,
) {
    // The injected client must stay alive for the whole test even though the
    // transaction that consumes the runtime drops at `commit`: the detached
    // async-commit secondary flush runs on its own thread holding only a
    // non-owning clone, and dropping the owner would close the transport
    // under it — production keeps this owner for the process lifetime.
    let client = TonicCoprocessorClient::new().unwrap();
    let runtime =
        SharedReadRuntime::new_injected(client, RegionCache::new(SplitTopology { address }));
    let (timestamps, calls) = CountingTimestampSource::new(PD_COMMIT_TS);
    let mut transaction = RealOptimisticTransaction::new_injected(
        runtime,
        timestamps,
        CALL_TIMEOUT,
        start_ts,
        Instant::now(),
        8,
        64 * 1024,
    )
    .unwrap();
    transaction.set_commit_protocol(protocol);
    (transaction, calls)
}

fn single_region_mutation() -> Vec<OptimisticMutation> {
    vec![OptimisticMutation::insert(PRIMARY_KEY.to_vec(), b"primary-value".to_vec()).unwrap()]
}

fn two_region_mutations() -> Vec<OptimisticMutation> {
    vec![
        OptimisticMutation::insert(PRIMARY_KEY.to_vec(), b"primary-value".to_vec()).unwrap(),
        OptimisticMutation::insert(SECONDARY_KEY.to_vec(), b"secondary-value".to_vec()).unwrap(),
    ]
}

fn commit(
    address: String,
    protocol: CommitProtocol,
    mutations: Vec<OptimisticMutation>,
) -> (OptimisticCommitOutcome, u64) {
    let (transaction, calls) = transaction(address, protocol);
    let outcome = transaction
        .commit(mutations, &UnaryCallContext::with_timeout(CALL_TIMEOUT))
        .expect("the coordinator must return a terminal outcome, not a caller error");
    let calls = calls.load(Ordering::Relaxed);
    (outcome, calls)
}

fn wait_for_commits(recorded: &Arc<Mutex<Recorded>>, count: usize) {
    // A classic two-phase commit hands its secondary Commit to the detached
    // flush thread once the primary lands (`2pc.go`, `spawnWithStorePool`), so
    // the caller must wait for that flush instead of assuming it landed before
    // `commit` returned.
    let deadline = Instant::now() + CALL_TIMEOUT;
    while recorded.lock().unwrap().commits.len() < count {
        assert!(
            Instant::now() < deadline,
            "the detached secondary flush never published its Commits"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn ok(min_commit_ts: u64) -> PrewriteAnswer {
    PrewriteAnswer {
        min_commit_ts,
        one_pc_commit_ts: 0,
    }
}

fn one_pc(commit_ts: u64) -> PrewriteAnswer {
    PrewriteAnswer {
        min_commit_ts: 0,
        one_pc_commit_ts: commit_ts,
    }
}

// -----------------------------------------------------------------------------
// 1PC
// -----------------------------------------------------------------------------

/// A single-region transaction TiKV accepts for 1PC commits inside its prewrite
/// and publishes no Commit at all.
///
/// Source contract (`twoPhaseCommitter.execute`): when `isOnePC()` survives
/// prewrite, `c.commitTS = c.onePCCommitTS` and execute returns — there is no
/// `GetTimestampForCommit` and no `commitMutations`.
#[test]
fn one_pc_has_no_commit_rpc_or_post_prewrite_tso() {
    let one_pc_commit_ts = 1_500 << 18;
    let service = ScriptedTikv::new(vec![one_pc(one_pc_commit_ts)]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);

    let (outcome, timestamp_calls) = commit(
        server.store_address(),
        CommitProtocol {
            async_commit: true,
            one_pc: true,
        },
        single_region_mutation(),
    );

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    let receipt = outcome.receipt();
    assert_eq!(receipt.commit_protocol, CommittedProtocol::OnePc);
    assert_eq!(
        receipt.commit_ts, one_pc_commit_ts,
        "the commit timestamp is TiKV's, not PD's"
    );
    assert_eq!(
        timestamp_calls, 1,
        "Go checks external consistency once before fast prewrite, with no post-prewrite TSO"
    );
    assert!(receipt
        .attempt_history
        .iter()
        .all(|attempt| attempt.phase == TransactionAttemptPhase::Prewrite));

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.prewrites.len(), 1);
    assert!(recorded.prewrites[0].try_one_pc);
    assert!(
        recorded.commits.is_empty(),
        "a 1PC transaction must never publish a Commit"
    );
    assert!(recorded.rollbacks.is_empty());
}

/// 1PC is withdrawn the moment the mutations need more than one region, before
/// any prewrite is published.
///
/// Source contract (`checkOnePCFallBack`): 1PC is a single-region protocol, so
/// a multi-batch grouping disables it rather than letting TiKV refuse it once
/// per batch.
#[test]
fn a_multi_region_transaction_never_asks_for_one_pc() {
    let service = ScriptedTikv::new(vec![ok(1_100 << 18), ok(1_200 << 18)]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);

    let (outcome, timestamp_calls) = commit(
        server.store_address(),
        CommitProtocol {
            async_commit: false,
            one_pc: true,
        },
        two_region_mutations(),
    );

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    assert_eq!(
        outcome.receipt().commit_protocol,
        CommittedProtocol::TwoPhase
    );
    assert_eq!(outcome.receipt().commit_ts, PD_COMMIT_TS + (1 << 18));
    assert_eq!(
        timestamp_calls, 2,
        "fallback takes a commit TSO after the fast-protocol consistency check"
    );

    {
        let recorded = recorded.lock().unwrap();
        assert_eq!(recorded.prewrites.len(), 2);
        assert!(
            recorded.prewrites.iter().all(|request| !request.try_one_pc),
            "a two-region transaction must not ask any batch for 1PC"
        );
    }
    wait_for_commits(&recorded, 2);
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.commits.len(), 2);
}

/// `pkg/kv.SetTxnResourceGroup` reaches every Prewrite and Commit request,
/// including the primary and secondary batches of a multi-region 2PC.
#[test]
fn txn_resource_group_is_attached_to_prewrite_and_commit_rpc_contexts() {
    let service = ScriptedTikv::new(vec![ok(1_100 << 18), ok(1_200 << 18)]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let (mut transaction, _) =
        transaction(server.store_address(), CommitProtocol::two_phase_only());
    set_txn_resource_group(&mut transaction, "analytics");

    let outcome = transaction
        .commit(
            two_region_mutations(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("the scripted two-phase commit completes");
    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);

    wait_for_commits(&recorded, 2);
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.prewrites.len(), 2);
    assert_eq!(recorded.commits.len(), 2);
    for context in recorded
        .prewrites
        .iter()
        .map(|request| {
            request
                .context
                .as_ref()
                .expect("the transport attaches the Prewrite context")
        })
        .chain(recorded.commits.iter().map(|request| {
            request
                .context
                .as_ref()
                .expect("the transport attaches the Commit context")
        }))
    {
        assert_eq!(
            context
                .resource_control_context
                .as_ref()
                .map(|resource| resource.resource_group_name.as_str()),
            Some("analytics")
        );
    }
}

/// TiKV refusing 1PC — a zero `one_pc_commit_ts` — falls back to normal 2PC,
/// and takes async commit down with it.
///
/// Source contract (`handleSingleBatchSucceed`): the fallback sets both
/// `setOnePC(false)` and `setAsyncCommit(false)`, and TiKV must zero
/// `min_commit_ts` when it declines.
#[test]
fn a_refused_one_pc_falls_back_to_normal_two_phase_commit() {
    let service = ScriptedTikv::new(vec![ok(0)]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);

    let (outcome, timestamp_calls) = commit(
        server.store_address(),
        CommitProtocol {
            async_commit: true,
            one_pc: true,
        },
        single_region_mutation(),
    );

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    let receipt = outcome.receipt();
    assert_eq!(receipt.commit_protocol, CommittedProtocol::TwoPhase);
    assert_eq!(receipt.commit_ts, PD_COMMIT_TS + (1 << 18));
    assert_eq!(timestamp_calls, 2);

    let recorded = recorded.lock().unwrap();
    assert!(recorded.prewrites[0].try_one_pc);
    assert_eq!(recorded.commits.len(), 1);
    assert_eq!(
        recorded.commits[0].commit_role,
        KvrpcCommitRole::Primary as i32
    );
    assert!(!recorded.commits[0].use_async_commit);
}

/// A 1PC response that reports both a commit timestamp and a nonzero
/// `min_commit_ts` contradicts itself and rolls the transaction back.
#[test]
fn a_one_pc_fallback_that_still_reports_a_min_commit_ts_is_rejected() {
    let service = ScriptedTikv::new(vec![PrewriteAnswer {
        min_commit_ts: 1_100 << 18,
        one_pc_commit_ts: 0,
    }]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);

    let (outcome, _) = commit(
        server.store_address(),
        CommitProtocol {
            async_commit: false,
            one_pc: true,
        },
        single_region_mutation(),
    );

    assert_eq!(outcome.state(), OptimisticTransactionState::RolledBack);
    let recorded = recorded.lock().unwrap();
    assert!(recorded.commits.is_empty());
    assert!(
        recorded.rollbacks.is_empty(),
        "Go skips optimistic cleanup while 1PC is still enabled"
    );
}

// -----------------------------------------------------------------------------
// Async commit
// -----------------------------------------------------------------------------

/// An async-commit transaction commits at the largest `min_commit_ts` its
/// prewrites returned, with no second PD round trip.
///
/// Source contract (`twoPhaseCommitter.execute`): `commitTS =
/// c.minCommitTSMgr.get()` when `isAsyncCommit()`, and the commit itself is
/// spawned only to make the already-taken decision visible.
#[test]
fn an_async_commit_uses_the_largest_min_commit_ts_and_takes_no_second_tso() {
    let service = ScriptedTikv::new(vec![ok(1_100 << 18), ok(1_700 << 18)]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);

    let (outcome, timestamp_calls) = commit(
        server.store_address(),
        CommitProtocol {
            async_commit: true,
            one_pc: false,
        },
        two_region_mutations(),
    );

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    let receipt = outcome.receipt();
    assert_eq!(receipt.commit_protocol, CommittedProtocol::AsyncCommit);
    assert_eq!(
        receipt.commit_ts,
        1_700 << 18,
        "the commit timestamp is max(min_commit_ts), not the first or the last"
    );
    assert_eq!(
        timestamp_calls, 1,
        "Go checks external consistency before prewrite, then uses the maximum min_commit_ts"
    );

    // Go answers the session at the prewrite and flushes the follow-up
    // secondary Commits on a goroutine (`2pc.go`), so this node hands them to
    // a detached transport thread too: wait for that flush instead of assuming
    // it landed before `commit` returned.
    let deadline = Instant::now() + CALL_TIMEOUT;
    while recorded.lock().unwrap().commits.len() < 2 {
        assert!(
            Instant::now() < deadline,
            "the detached async-commit secondary flush never published its Commits"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.prewrites.len(), 2);
    assert!(recorded
        .prewrites
        .iter()
        .all(|request| request.use_async_commit));
    // Only the primary lock names the secondaries, and it names all of them.
    let primary_prewrite = recorded
        .prewrites
        .iter()
        .find(|request| request.mutations[0].key == PRIMARY_KEY)
        .expect("the primary batch was published");
    assert_eq!(primary_prewrite.secondaries, vec![SECONDARY_KEY.to_vec()]);
    let secondary_prewrite = recorded
        .prewrites
        .iter()
        .find(|request| request.mutations[0].key == SECONDARY_KEY)
        .expect("the secondary batch was published");
    assert!(secondary_prewrite.secondaries.is_empty());
    // Every prewrite bounds the commit timestamp it is willing to be granted.
    assert!(recorded.prewrites.iter().all(
        |request| request.max_commit_ts > START_TS && request.min_commit_ts == PD_COMMIT_TS + 1
    ));

    // The commits only publish the decision; they carry the async-commit flag
    // and the primary's batch keeps the primary role.
    assert_eq!(recorded.commits.len(), 2);
    assert!(recorded
        .commits
        .iter()
        .all(|request| request.use_async_commit && request.commit_version == 1_700 << 18));
    assert_eq!(
        recorded
            .commits
            .iter()
            .find(|request| request.keys.contains(&PRIMARY_KEY.to_vec()))
            .unwrap()
            .commit_role,
        KvrpcCommitRole::Primary as i32
    );
    assert_eq!(
        recorded
            .commits
            .iter()
            .find(|request| request.keys.contains(&SECONDARY_KEY.to_vec()))
            .unwrap()
            .commit_role,
        KvrpcCommitRole::Secondary as i32
    );
}

/// One prewrite answering with a zero `min_commit_ts` withdraws async commit
/// for the whole transaction, which then commits normally.
///
/// Source contract: a zero `min_commit_ts` means TiKV could not make the
/// async-commit guarantee, and one key that cannot is enough to sink it.
#[test]
fn a_zero_min_commit_ts_from_any_batch_falls_back_to_normal_two_phase_commit() {
    let service = ScriptedTikv::new(vec![ok(1_100 << 18), ok(0)]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);

    let (outcome, timestamp_calls) = commit(
        server.store_address(),
        CommitProtocol {
            async_commit: true,
            one_pc: false,
        },
        two_region_mutations(),
    );

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    let receipt = outcome.receipt();
    assert_eq!(receipt.commit_protocol, CommittedProtocol::TwoPhase);
    assert_eq!(receipt.commit_ts, PD_COMMIT_TS + (1 << 18));
    assert_eq!(timestamp_calls, 2);

    wait_for_commits(&recorded, 2);
    let recorded = recorded.lock().unwrap();
    assert!(recorded
        .commits
        .iter()
        .all(|request| !request.use_async_commit));
}

/// A transaction whose keys exceed the async-commit size limit never asks for
/// the protocol in the first place.
///
/// Source contract (`checkAsyncCommit`): the primary lock has to carry every
/// secondary key, so the total key size is capped at 4 KiB; past that the saved
/// round trip costs more than it saves.
#[test]
fn a_transaction_over_the_key_size_limit_never_asks_for_async_commit() {
    let service = ScriptedTikv::new(vec![ok(1_100 << 18), ok(1_200 << 18)]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);

    // Two keys that together pass the 4 KiB total-key-size limit; each on its
    // own is inside the per-key limit, so only the aggregate rules this out.
    let mut primary = PRIMARY_KEY.to_vec();
    primary.resize(2_100, b'z');
    let mut secondary = SECONDARY_KEY.to_vec();
    secondary.resize(2_100, b'z');
    let mutations = vec![
        OptimisticMutation::insert(primary, b"value".to_vec()).unwrap(),
        OptimisticMutation::insert(secondary, b"value".to_vec()).unwrap(),
    ];

    let (transaction, timestamp_calls) = transaction(
        server.store_address(),
        CommitProtocol {
            async_commit: true,
            one_pc: false,
        },
    );
    let outcome = transaction
        .commit(mutations, &UnaryCallContext::with_timeout(CALL_TIMEOUT))
        .unwrap();

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    assert_eq!(
        outcome.receipt().commit_protocol,
        CommittedProtocol::TwoPhase
    );
    assert_eq!(timestamp_calls.load(Ordering::Relaxed), 1);

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.prewrites.len(), 2);
    assert!(recorded.prewrites.iter().all(|request| {
        !request.use_async_commit
            && request.secondaries.is_empty()
            // A normal 2PC prewrite places no upper bound on its commit
            // timestamp, because a PD timestamp will decide it.
            && request.max_commit_ts == 0
    }));
}

/// A transaction permitted neither protocol behaves exactly as before: no
/// async-commit flag, no 1PC request, one PD commit timestamp.
#[test]
fn a_transaction_permitted_neither_protocol_still_commits_in_two_phases() {
    let service = ScriptedTikv::new(vec![ok(1_100 << 18), ok(1_200 << 18)]);
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);

    let (outcome, timestamp_calls) = commit(
        server.store_address(),
        CommitProtocol::two_phase_only(),
        two_region_mutations(),
    );

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    assert_eq!(
        outcome.receipt().commit_protocol,
        CommittedProtocol::TwoPhase
    );
    assert_eq!(timestamp_calls, 1);

    {
        let recorded = recorded.lock().unwrap();
        assert!(recorded
            .prewrites
            .iter()
            .all(|request| !request.use_async_commit && !request.try_one_pc));
    }
    wait_for_commits(&recorded, 2);
    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.commits.len(), 2);
}

impl tidb_txnkv::region::RegionQueryLoader for SplitTopology {
    fn query_region(
        &mut self,
        query: tidb_txnkv::region::RegionQuery<'_>,
        _options: tidb_txnkv::region::RegionQueryOptions,
    ) -> Result<RegionLocation, RegionLoadError> {
        match query {
            tidb_txnkv::region::RegionQuery::Id(id) => {
                let key = if id == HIGH_REGION {
                    SPLIT_KEY.to_vec()
                } else {
                    Vec::new()
                };
                let location = self.load_region(&key)?;
                if location.region.id == id {
                    Ok(location)
                } else {
                    Err(RegionLoadError::new(
                        "unknown-region",
                        "unknown fixture region",
                    ))
                }
            }
            tidb_txnkv::region::RegionQuery::Key(key) => self.load_region(key),
            tidb_txnkv::region::RegionQuery::EndKey(key) => self.load_region_by_end_key(key),
        }
    }
    fn scan_regions_once(
        &mut self,
        range: &tidb_txnkv::region::KeyRange,
        _limit: usize,
        _options: tidb_txnkv::region::RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        self.load_region(&range.start).map(|r| vec![r])
    }
    fn load_store(
        &mut self,
        _id: u64,
    ) -> Result<Option<tidb_txnkv::region::StoreMetadata>, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-store-query",
            "fixture expects cached store metadata",
        ))
    }
}
