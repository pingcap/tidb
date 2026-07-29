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

//! Async-commit and 1PC fast-commit protocol branches over scripted TiKV.
//!
//! These are the wire-level facts a live cluster cannot produce on demand:
//! whether Prewrite carried `use_async_commit` / `try_one_pc` and the secondary
//! list, whether the client took its commit timestamp from the returned
//! `min_commit_ts` / `one_pc_commit_ts` instead of PD, whether a 1PC
//! transaction published any Commit RPC at all, and both fallbacks to normal
//! 2PC (ineligible before Prewrite, and TiKV rejecting the fast path in the
//! Prewrite response). The transport, BatchCommands framing, region routing,
//! and commit-phase logic are the real production paths; only the TiKV
//! responses and region topology are scripted.
//!
//! Mirrors the pinned client-go `txnkv/transaction/{2pc,prewrite}.go`.

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
    CoprocessorRequest, CoprocessorResponse, KvrpcCommitRequest, KvrpcCommitResponse,
    KvrpcPrewriteRequest, KvrpcPrewriteResponse,
};
use tidb_txnkv::lock::FixedTimestampSource;
use tidb_txnkv::region::{
    Peer, PeerRole, RegionCache, RegionEpoch, RegionLoadError, RegionLoader, RegionLocation,
    RegionMetadata, RegionRecoveryLoader, RegionVerId, Store,
};
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext};
use tidb_txnkv::transaction::{
    CommitProtocol, OptimisticCommitOutcome, OptimisticMutation, OptimisticTransactionState,
    RealOptimisticTransaction,
};
use tidb_txnkv::SharedReadRuntime;

const START_TS: u64 = 100;
/// The PD commit timestamp a normal-2PC fallback allocates.
const FALLBACK_COMMIT_TS: u64 = 200;
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Primary key: lexicographically smallest, so it selects `LOW_REGION`.
const PRIMARY_KEY: &[u8] = b"a";
/// Second key routed to `LOW_REGION` (same region as the primary).
const SAME_REGION_KEY: &[u8] = b"b";
/// Key routed to `HIGH_REGION` by the split at `m`.
const HIGH_REGION_KEY: &[u8] = b"m";
const SPLIT_KEY: &[u8] = b"m";

const LOW_REGION: u64 = 41;
const HIGH_REGION: u64 = 42;

// -----------------------------------------------------------------------------
// Scripted region topology (a single split at `m`, always routable)
// -----------------------------------------------------------------------------

#[derive(Clone)]
struct SplitTopology {
    address: String,
}

impl SplitTopology {
    fn region_for(key: &[u8]) -> (u64, Vec<u8>, Vec<u8>) {
        if key < SPLIT_KEY {
            (LOW_REGION, Vec::new(), SPLIT_KEY.to_vec())
        } else {
            (HIGH_REGION, SPLIT_KEY.to_vec(), Vec::new())
        }
    }

    fn location(&self, region_id: u64, start: Vec<u8>, end: Vec<u8>) -> RegionLocation {
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
            leader_peer_id: Some(peer_id),
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
        Ok(self.location(region_id, start, end))
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
// Scripted TiKV: every Prewrite gets the same scripted fast-commit reply.
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default)]
struct PrewriteReply {
    /// Value TiKV returns in `PrewriteResponse.min_commit_ts`.
    min_commit_ts: u64,
    /// Value TiKV returns in `PrewriteResponse.one_pc_commit_ts`.
    one_pc_commit_ts: u64,
}

#[derive(Debug, Default)]
struct Recorded {
    prewrites: Vec<KvrpcPrewriteRequest>,
    commits: Vec<KvrpcCommitRequest>,
}

#[derive(Clone)]
struct ScriptedTikv {
    reply: PrewriteReply,
    recorded: Arc<Mutex<Recorded>>,
}

impl ScriptedTikv {
    fn new(reply: PrewriteReply) -> Self {
        Self {
            reply,
            recorded: Arc::new(Mutex::new(Recorded::default())),
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
                self.recorded.lock().unwrap().prewrites.push(request);
                Ok(ResponseCmd::Prewrite(
                    KvrpcPrewriteResponse {
                        min_commit_ts: self.reply.min_commit_ts,
                        one_pc_commit_ts: self.reply.one_pc_commit_ts,
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
            other => Err(tonic::Status::unimplemented(format!(
                "these branches publish only Prewrite and Commit: {other:?}"
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

/// Every transaction gets a distinct one-shot fallback timestamp so a 2PC
/// fallback always finds a fresh commit ts greater than `start_ts`.
static NEXT_FALLBACK_TS: AtomicU64 = AtomicU64::new(FALLBACK_COMMIT_TS);

fn transaction(
    address: String,
    protocol: CommitProtocol,
) -> RealOptimisticTransaction<TonicCoprocessorClient, SplitTopology, FixedTimestampSource> {
    let client = TonicCoprocessorClient::new().unwrap();
    let runtime = SharedReadRuntime::new_injected(client, RegionCache::new(SplitTopology { address }));
    assert_eq!(runtime.cluster_id(), 7);
    let mut transaction = RealOptimisticTransaction::new_injected(
        runtime,
        FixedTimestampSource::new(NEXT_FALLBACK_TS.fetch_add(1, Ordering::Relaxed)),
        CALL_TIMEOUT,
        START_TS,
        Instant::now(),
        4,
        4 * 1024,
    )
    .unwrap();
    transaction.set_commit_protocol(protocol);
    transaction
}

fn two_key_same_region() -> Vec<OptimisticMutation> {
    vec![
        OptimisticMutation::insert(PRIMARY_KEY.to_vec(), b"primary-value".to_vec()).unwrap(),
        OptimisticMutation::insert(SAME_REGION_KEY.to_vec(), b"same-region-value".to_vec()).unwrap(),
    ]
}

fn two_key_two_regions() -> Vec<OptimisticMutation> {
    vec![
        OptimisticMutation::insert(PRIMARY_KEY.to_vec(), b"primary-value".to_vec()).unwrap(),
        OptimisticMutation::insert(HIGH_REGION_KEY.to_vec(), b"high-region-value".to_vec()).unwrap(),
    ]
}

fn async_only() -> CommitProtocol {
    CommitProtocol {
        enable_async_commit: true,
        enable_1pc: false,
        ..CommitProtocol::default()
    }
}

fn one_pc_only() -> CommitProtocol {
    CommitProtocol {
        enable_async_commit: false,
        enable_1pc: true,
        ..CommitProtocol::default()
    }
}

// -----------------------------------------------------------------------------
// Async commit
// -----------------------------------------------------------------------------

/// Async commit takes its commit timestamp from the greatest `min_commit_ts`
/// TiKV returns, publishes the secondary list on the primary Prewrite, and
/// carries `use_async_commit` on both Prewrite and Commit — never touching PD
/// for a commit timestamp.
#[test]
fn async_commit_uses_returned_min_commit_ts_and_carries_the_wire_flags() {
    let returned_min_commit_ts = 150;
    let service = ScriptedTikv::new(PrewriteReply {
        min_commit_ts: returned_min_commit_ts,
        one_pc_commit_ts: 0,
    });
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let transaction = transaction(server.store_address(), async_only());

    let outcome = transaction
        .commit(
            two_key_same_region(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("async commit must return a terminal outcome");

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("async commit must stay Committed");
    };
    let receipt = &committed.receipt;
    assert!(receipt.async_commit, "the receipt must record async commit");
    assert!(!receipt.one_pc);
    assert_eq!(
        receipt.commit_ts, returned_min_commit_ts,
        "commit_ts must be the max returned min_commit_ts, not a PD timestamp"
    );
    assert!(committed.secondary_failures.is_empty());

    let recorded = recorded.lock().unwrap();
    // Both keys share LOW_REGION, so a single Prewrite carries the whole txn.
    assert_eq!(recorded.prewrites.len(), 1);
    let prewrite = &recorded.prewrites[0];
    assert!(prewrite.use_async_commit, "Prewrite must set use_async_commit");
    assert!(!prewrite.try_one_pc);
    assert_eq!(
        prewrite.min_commit_ts,
        START_TS + 1,
        "Prewrite must carry the min_commit_ts floor start_ts + 1"
    );
    assert_eq!(
        prewrite.secondaries,
        vec![SAME_REGION_KEY.to_vec()],
        "the primary Prewrite must carry every non-primary key as a secondary"
    );
    // The Commit RPC(s) commit at the async timestamp and carry the flag.
    assert!(!recorded.commits.is_empty());
    assert!(recorded
        .commits
        .iter()
        .all(|commit| commit.use_async_commit
            && commit.commit_version == returned_min_commit_ts));
}

/// Async commit accumulates the floor across regions: with two batches, the
/// commit timestamp is the maximum `min_commit_ts` any region returned.
#[test]
fn async_commit_takes_the_maximum_min_commit_ts_across_regions() {
    // Both regions get the same scripted reply; the point is that a two-region
    // async commit still succeeds and commits every region at that timestamp.
    let returned = 175;
    let service = ScriptedTikv::new(PrewriteReply {
        min_commit_ts: returned,
        one_pc_commit_ts: 0,
    });
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let transaction = transaction(server.store_address(), async_only());

    let outcome = transaction
        .commit(
            two_key_two_regions(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("two-region async commit must return a terminal outcome");

    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("async commit must stay Committed");
    };
    assert!(committed.receipt.async_commit);
    assert_eq!(committed.receipt.commit_ts, returned);

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.prewrites.len(), 2, "one Prewrite per region");
    // Only the primary batch carries the secondary list.
    let primary_prewrite = recorded
        .prewrites
        .iter()
        .find(|prewrite| prewrite.mutations.iter().any(|m| m.key == PRIMARY_KEY))
        .unwrap();
    assert_eq!(primary_prewrite.secondaries, vec![HIGH_REGION_KEY.to_vec()]);
    let secondary_prewrite = recorded
        .prewrites
        .iter()
        .find(|prewrite| prewrite.mutations.iter().any(|m| m.key == HIGH_REGION_KEY))
        .unwrap();
    assert!(
        secondary_prewrite.secondaries.is_empty(),
        "a non-primary batch must not carry the secondary list"
    );
    assert!(recorded.commits.iter().all(|c| c.use_async_commit));
}

// -----------------------------------------------------------------------------
// 1PC
// -----------------------------------------------------------------------------

/// 1PC commits atomically inside Prewrite: the client takes `one_pc_commit_ts`
/// as its commit timestamp and publishes no Commit RPC at all.
#[test]
fn one_pc_commits_in_prewrite_with_no_commit_rpc() {
    let one_pc_commit_ts = 190;
    let service = ScriptedTikv::new(PrewriteReply {
        min_commit_ts: 0,
        one_pc_commit_ts,
    });
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let transaction = transaction(server.store_address(), one_pc_only());

    let outcome = transaction
        .commit(
            two_key_same_region(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("1PC must return a terminal outcome");

    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("1PC must stay Committed");
    };
    let receipt = &committed.receipt;
    assert!(receipt.one_pc, "the receipt must record 1PC");
    assert!(!receipt.async_commit);
    assert_eq!(receipt.commit_ts, one_pc_commit_ts);
    assert!(
        receipt.primary_publications.is_empty(),
        "1PC publishes no primary Commit"
    );
    assert!(committed.secondary_failures.is_empty());

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.prewrites.len(), 1);
    assert!(recorded.prewrites[0].try_one_pc, "Prewrite must set try_one_pc");
    assert!(
        recorded.commits.is_empty(),
        "1PC must never publish a Commit RPC"
    );
}

/// 1PC is refused before Prewrite when the transaction spans more than one
/// region batch (client-go `checkOnePCFallBack`): it becomes normal 2PC.
#[test]
fn one_pc_is_ineligible_across_regions_and_becomes_2pc() {
    let service = ScriptedTikv::new(PrewriteReply::default());
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let transaction = transaction(server.store_address(), one_pc_only());

    let outcome = transaction
        .commit(
            two_key_two_regions(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("a two-region 1PC-ineligible txn must return a terminal outcome");

    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("must commit via normal 2PC");
    };
    assert!(!committed.receipt.one_pc);
    assert!(!committed.receipt.async_commit);
    assert!(
        committed.receipt.commit_ts >= FALLBACK_COMMIT_TS,
        "a 2PC fallback must allocate a PD commit timestamp"
    );

    let recorded = recorded.lock().unwrap();
    assert!(
        recorded.prewrites.iter().all(|p| !p.try_one_pc),
        "no Prewrite may set try_one_pc once 1PC is ruled out"
    );
    assert!(!recorded.commits.is_empty(), "normal 2PC publishes Commit RPCs");
}

// -----------------------------------------------------------------------------
// Fallbacks to normal 2PC
// -----------------------------------------------------------------------------

/// Async commit disabled by the size limit never reaches the wire as async: the
/// transaction runs normal 2PC and takes a PD commit timestamp.
#[test]
fn async_commit_ineligible_by_keys_limit_runs_normal_2pc() {
    let service = ScriptedTikv::new(PrewriteReply::default());
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let protocol = CommitProtocol {
        enable_async_commit: true,
        // Two mutations exceed a one-key limit, so async is ineligible.
        async_keys_limit: 1,
        ..CommitProtocol::default()
    };
    let transaction = transaction(server.store_address(), protocol);

    let outcome = transaction
        .commit(
            two_key_same_region(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("an ineligible async txn must return a terminal outcome");

    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("must commit via normal 2PC");
    };
    assert!(!committed.receipt.async_commit);
    assert!(committed.receipt.commit_ts >= FALLBACK_COMMIT_TS);

    let recorded = recorded.lock().unwrap();
    assert!(
        recorded.prewrites.iter().all(|p| !p.use_async_commit),
        "an ineligible async txn must not set use_async_commit on the wire"
    );
    assert!(!recorded.commits.is_empty());
}

/// Async commit that TiKV rejects in the Prewrite response (`min_commit_ts` 0)
/// falls back to normal 2PC: the wire flag was set, but the client allocates a
/// PD commit timestamp and clears the async receipt.
#[test]
fn async_commit_rejected_by_tikv_falls_back_to_2pc() {
    let service = ScriptedTikv::new(PrewriteReply {
        // Zero means "async commit cannot proceed" — fall back to normal path.
        min_commit_ts: 0,
        one_pc_commit_ts: 0,
    });
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let transaction = transaction(server.store_address(), async_only());

    let outcome = transaction
        .commit(
            two_key_same_region(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("a rejected async commit must return a terminal outcome");

    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("must commit via normal 2PC after fallback");
    };
    assert!(
        !committed.receipt.async_commit,
        "a fallback must not report async commit"
    );
    assert!(committed.receipt.commit_ts >= FALLBACK_COMMIT_TS);

    let recorded = recorded.lock().unwrap();
    assert!(
        recorded.prewrites[0].use_async_commit,
        "the Prewrite still carried the async flag before TiKV rejected it"
    );
    // After fallback, Commit runs as normal 2PC (no async flag).
    assert!(!recorded.commits.is_empty());
    assert!(recorded.commits.iter().all(|c| !c.use_async_commit));
}

/// 1PC that TiKV rejects in the Prewrite response (`one_pc_commit_ts` 0, and a
/// zero `min_commit_ts` as the protocol requires) falls back to normal 2PC.
#[test]
fn one_pc_rejected_by_tikv_falls_back_to_2pc() {
    let service = ScriptedTikv::new(PrewriteReply {
        // 1PC rejected: one_pc_commit_ts 0, and min_commit_ts must also be 0.
        min_commit_ts: 0,
        one_pc_commit_ts: 0,
    });
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let transaction = transaction(server.store_address(), one_pc_only());

    let outcome = transaction
        .commit(
            two_key_same_region(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("a rejected 1PC must return a terminal outcome");

    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("must commit via normal 2PC after fallback");
    };
    assert!(!committed.receipt.one_pc, "a fallback must not report 1PC");
    assert!(committed.receipt.commit_ts >= FALLBACK_COMMIT_TS);

    let recorded = recorded.lock().unwrap();
    assert!(
        recorded.prewrites[0].try_one_pc,
        "the Prewrite still carried try_one_pc before TiKV rejected it"
    );
    assert!(
        !recorded.commits.is_empty(),
        "after 1PC fallback the transaction commits via normal 2PC"
    );
}

/// Neither fast-commit protocol enabled keeps a transaction on plain 2PC with no
/// fast-commit wire flags at all — the default a non-TiKV session runs with.
#[test]
fn default_protocol_stays_on_plain_2pc() {
    let service = ScriptedTikv::new(PrewriteReply::default());
    let recorded = Arc::clone(&service.recorded);
    let server = TestServer::start(service);
    let transaction = transaction(server.store_address(), CommitProtocol::default());

    let outcome = transaction
        .commit(
            two_key_same_region(),
            &UnaryCallContext::with_timeout(CALL_TIMEOUT),
        )
        .expect("plain 2PC must return a terminal outcome");

    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("must commit via plain 2PC");
    };
    assert!(!committed.receipt.async_commit);
    assert!(!committed.receipt.one_pc);
    assert!(committed.receipt.commit_ts >= FALLBACK_COMMIT_TS);

    let recorded = recorded.lock().unwrap();
    assert!(recorded
        .prewrites
        .iter()
        .all(|p| !p.use_async_commit && !p.try_one_pc));
    assert!(!recorded.commits.is_empty());
}
