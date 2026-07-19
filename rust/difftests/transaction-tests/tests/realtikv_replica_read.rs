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

#![allow(missing_docs)]

use std::cell::RefCell;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use tidb_datatype::FieldType;
use tidb_distsql::{
    decode_select_response, DirectUnaryClient, DirectUnaryClientError, DirectUnaryQueryTransport,
    DirectUnaryRequest, DirectUnaryResponse, DirectUnaryRuntimeConfig, InjectedQueryRuntime,
    KvRequestMetadata, QueryResultContext, ReplicaReadType, RequestKeyRange, RequestKeyRanges,
    RequestType, SelectInput, StoreType, TransportRequest, WarningCollector,
};
use tidb_txnkv::region::{PeerRole, RegionCache, StoreLiveness};
use tidb_txnkv::region::{ReadPolicy, RequestSelection, StoreFailureOutcome};
use tidb_txnkv::rpc::{
    completion_pair, CompletionError, CompletionRunLoop, TonicCoprocessorClient,
};
use tidb_txnkv::{
    BatchCommandEntry, BatchCommandTag, ClientReplicaReadType, EndpointType, OpaqueBatchCommand,
    PdRegionLoader, SharedReadRuntime, UnaryCallContext,
};

const TABLE_START: &[u8] = b"t\x80\0\0\0\0\0\0*_r";
const TABLE_END: &[u8] = b"t\x80\0\0\0\0\0\0+";
const TABLE_SCAN_DAG: &[u8] = &[0x12, 0x04, 0x12, 0x02, 0x08, 0x2a];
const CAMPAIGN18_SPLIT_KEY: &[u8] = b"t\x80\0\0\0\0\0\0*_r\x80\0\0\0\0\0\0\0";
const CAMPAIGN18_PHASE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Debug)]
struct ObservedDispatch {
    address: String,
    forwarded_host: Option<String>,
    peer_id: u64,
    store_id: u64,
    replica_read_type: ClientReplicaReadType,
    replica_read: bool,
    stale_read: bool,
}

struct RecordingClient {
    inner: TonicCoprocessorClient,
    dispatches: Rc<RefCell<Vec<ObservedDispatch>>>,
}

impl RecordingClient {
    fn record_dispatch(
        &self,
        address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
    ) {
        let peer = request
            .context
            .peer
            .as_ref()
            .expect("production transport must attach one selected peer");
        self.dispatches.borrow_mut().push(ObservedDispatch {
            address: address.to_owned(),
            forwarded_host: forwarded_host.map(str::to_owned),
            peer_id: peer.id,
            store_id: peer.store_id,
            replica_read_type: request.replica_read_type,
            replica_read: request.context.replica_read,
            stale_read: request.context.stale_read,
        });
    }
}

impl DirectUnaryClient for RecordingClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.record_dispatch(address, None, request);
        self.inner.send_request(address, request, timeout)
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.record_dispatch(address, None, request);
        self.inner.send_request_with_context(address, request, call)
    }

    fn send_request_with_route(
        &mut self,
        address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.record_dispatch(address, forwarded_host, request);
        self.inner
            .send_request_with_route(address, forwarded_host, request, call)
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        self.inner.close_address(address)
    }

    fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.inner.close_address_version(address, version)
    }

    fn liveness(
        &self,
        address: &str,
        timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        self.inner.liveness(address, timeout)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        self.inner.close()
    }
}

impl tidb_distsql::LockRecoveryClient for RecordingClient {
    fn check_txn_status_for_lock(
        &mut self,
        address: &str,
        request: &tidb_proto::KvrpcCheckTxnStatusRequest,
        context: &tidb_proto::KvrpcContext,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        self.inner.check_txn_status(address, request, context, call)
    }

    fn resolve_lock_for_read(
        &mut self,
        address: &str,
        request: &tidb_proto::KvrpcResolveLockRequest,
        context: &tidb_proto::KvrpcContext,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcResolveLockResponse, DirectUnaryClientError> {
        self.inner.resolve_lock(address, request, context, call)
    }
}

fn execute_live_empty_query(
    runtime: &mut InjectedQueryRuntime<DirectUnaryQueryTransport<RecordingClient, PdRegionLoader>>,
    source: &str,
) {
    let mut metadata = KvRequestMetadata {
        request_type: RequestType::Dag,
        data: Some(TABLE_SCAN_DAG.to_vec()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![
            RequestKeyRange {
                start_key: TABLE_START.to_vec(),
                end_key: TABLE_END.to_vec(),
            },
        ])),
        keep_order: true,
        store_type: StoreType::TiKv,
        start_ts: 1,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        ..KvRequestMetadata::default()
    };
    metadata.session.replica_read = ReplicaReadType::Leader;
    metadata.session.request_source.explicit_source_type = source.to_owned();
    let request = TransportRequest::new(
        metadata,
        std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
    );
    let mut result = runtime
        .select_with_runtime_stats(
            &request,
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .expect("enter through production direct unary transport");
    let raw = result
        .next_raw()
        .expect("live route dispatch must complete")
        .expect("one live region must publish one result");
    assert_eq!(result.next_raw().expect("finish live response"), None);
    let select = decode_select_response(&raw).expect("decode returned tipb SelectResponse");
    assert!(
        select.error.is_none(),
        "live SelectResponse returned application error: {:?}",
        select.error
    );
    assert!(
        select.rows.is_empty()
            && select.chunks.iter().all(|chunk| chunk
                .rows_data
                .as_deref()
                .unwrap_or_default()
                .is_empty()),
        "the known empty table-42 range must return no row payload"
    );
}

#[test]
#[ignore = "requires the cleanup-safe Campaign 13 three-TiKV runner"]
fn follower_policy_reaches_a_live_nonleader_voter() {
    let pd_address = std::env::var("C13_PD_ADDR")
        .expect("C13_PD_ADDR must be supplied by run-campaign13-replica-read.sh");
    let loader = PdRegionLoader::connect(pd_address, Duration::from_secs(5))
        .expect("bootstrap live PD region loader");
    let mut cache = RegionCache::new(loader);
    let location = cache
        .locate_key(TABLE_START)
        .expect("discover table region from PD")
        .clone();
    let leader_peer_id = location
        .leader_peer_id
        .expect("live region must expose a leader");
    assert!(
        location.peers.iter().any(|peer| {
            peer.id != leader_peer_id
                && matches!(
                    peer.role,
                    PeerRole::Voter | PeerRole::IncomingVoter | PeerRole::DemotingVoter
                )
                && !peer.is_witness
        }),
        "runner must expose a nonleader voter"
    );

    let dispatches = Rc::new(RefCell::new(Vec::new()));
    let shared_runtime = SharedReadRuntime::new_with_maintenance(
        RecordingClient {
            inner: TonicCoprocessorClient::new().expect("construct live unary client"),
            dispatches: Rc::clone(&dispatches),
        },
        cache,
    )
    .expect("start production region-cache maintenance");
    let inspection_runtime = shared_runtime.clone();
    let transport = DirectUnaryQueryTransport::with_shared_runtime(
        shared_runtime,
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(5),
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_distsql::FixedTimestampSource::new(1 << 18),
    )
    .expect("construct production direct unary transport");
    let mut runtime = InjectedQueryRuntime::new(transport);
    let mut metadata = KvRequestMetadata {
        request_type: RequestType::Dag,
        data: Some(TABLE_SCAN_DAG.to_vec()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![
            RequestKeyRange {
                start_key: TABLE_START.to_vec(),
                end_key: TABLE_END.to_vec(),
            },
        ])),
        keep_order: true,
        store_type: StoreType::TiKv,
        start_ts: 1,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        ..KvRequestMetadata::default()
    };
    metadata.session.replica_read = ReplicaReadType::Follower;
    metadata.session.request_source.explicit_source_type = "campaign13".to_owned();
    let request = TransportRequest::new(
        metadata,
        std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
    );
    let mut result = runtime
        .select_with_runtime_stats(
            &request,
            SelectInput::default(),
            QueryResultContext::new(Vec::<FieldType>::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .expect("enter through InjectedQueryRuntime and production transport");
    assert!(
        dispatches.borrow().is_empty(),
        "production response must remain lazy until first pull"
    );
    let raw = result
        .next_raw()
        .expect("live follower dispatch must complete")
        .expect("one live region must publish one result");
    assert_eq!(result.next_raw().expect("finish live response"), None);
    let select = decode_select_response(&raw).expect("decode returned tipb SelectResponse");
    assert!(
        select.error.is_none(),
        "live follower SelectResponse returned application error: {:?}",
        select.error
    );
    assert!(
        select.rows.is_empty()
            && select.chunks.iter().all(|chunk| chunk
                .rows_data
                .as_deref()
                .unwrap_or_default()
                .is_empty()),
        "the known empty table-42 range must return no row payload"
    );
    drop(result);

    let post_location = inspection_runtime
        .with_region_cache(|cache| {
            cache
                .locate_key(TABLE_START)
                .expect("inspect the retained live region after follower success")
                .clone()
        })
        .expect("lock the maintained live cache");
    let post_leader_peer_id = post_location
        .leader_peer_id
        .expect("successful follower dispatch must retain a cached leader");
    assert_eq!(
        post_leader_peer_id, leader_peer_id,
        "successful follower dispatch must not promote or replace the cached leader"
    );

    let dispatches = dispatches.borrow();
    assert_eq!(dispatches.len(), 1, "one logical region dispatch expected");
    let selected = &dispatches[0];
    let selected_peer = location
        .peers
        .iter()
        .find(|peer| peer.id == selected.peer_id)
        .expect("recorded peer must come from PD topology");
    assert_ne!(selected.peer_id, leader_peer_id);
    assert!(matches!(
        selected_peer.role,
        PeerRole::Voter | PeerRole::IncomingVoter | PeerRole::DemotingVoter
    ));
    assert!(!selected_peer.is_witness);
    assert_eq!(selected_peer.store_id, selected.store_id);
    assert_eq!(selected.replica_read_type, ClientReplicaReadType::Follower);
    assert!(selected.replica_read);
    assert!(!selected.stale_read);

    println!(
        "campaign13_replica_read region_id={} leader_peer_id={} post_leader_peer_id={} selected_peer_id={} selected_store_id={} selected_address={} replica_read={} stale_read={} usable_response=true",
        location.region.id,
        leader_peer_id,
        post_leader_peer_id,
        selected.peer_id,
        selected.store_id,
        selected.address,
        selected.replica_read,
        selected.stale_read,
    );
}

#[test]
#[ignore = "requires the cleanup-safe Campaign 14 three-TiKV runner"]
fn adaptive_forwarding_reuses_proxy_then_recovers_direct() {
    let pd_address = std::env::var("C14_PD_ADDR")
        .expect("C14_PD_ADDR must be supplied by run-campaign14-adaptive-forwarding.sh");
    let loader = PdRegionLoader::connect(pd_address, Duration::from_secs(5))
        .expect("bootstrap live PD region loader");
    let mut cache = RegionCache::new(loader);
    let location = cache
        .locate_key(TABLE_START)
        .expect("discover table region from PD")
        .clone();
    let region = location.region;
    let leader_peer_id = location
        .leader_peer_id
        .expect("live region must expose a leader");
    assert_eq!(
        location.peers.len(),
        3,
        "runner must expose exactly three region peers"
    );

    let busy_now = Duration::from_secs(1);
    let mut busy_selector = cache
        .request_selector(region, ReadPolicy::default())
        .expect("build live busy selector");
    busy_selector.set_busy_threshold(Duration::from_millis(50));
    let nonleader_peer_ids = location
        .peers
        .iter()
        .filter_map(|peer| (peer.id != leader_peer_id).then_some(peer.id))
        .collect::<Vec<_>>();
    assert_eq!(nonleader_peer_ids.len(), 2);
    let expected_busy_peers = [leader_peer_id, nonleader_peer_ids[0], nonleader_peer_ids[1]];
    for (expected_peer_id, estimated_wait_ms) in
        expected_busy_peers.into_iter().zip([500, 800, 150])
    {
        let RequestSelection::Attempt(selected) = cache
            .select_request_at(&mut busy_selector, busy_now)
            .expect("select live idle replica")
        else {
            panic!("an idle replica must remain before the final busy response")
        };
        assert_eq!(selected.target().peer_id, expected_peer_id);
        assert!(busy_selector.record_attempt_result(selected.target(), Duration::from_millis(1)));
        cache
            .on_server_busy(
                &mut busy_selector,
                selected.target(),
                estimated_wait_ms,
                busy_now,
            )
            .expect("apply live store load observation");
        assert!(busy_selector.acknowledge_server_busy(selected.target()));
    }
    let RequestSelection::Attempt(busy_fallback) = cache
        .select_request_at(&mut busy_selector, busy_now)
        .expect("retry leader after every replica reports busy")
    else {
        panic!("no-idle fallback must retain the region")
    };
    assert_eq!(busy_fallback.target().peer_id, leader_peer_id);
    assert_eq!(busy_selector.busy_threshold(), Duration::ZERO);
    assert_eq!(cache.len(), 1, "busy fallback must not invalidate topology");

    let mut fresh_busy_selector = cache
        .request_selector(region, ReadPolicy::default())
        .expect("build fresh live busy selector");
    fresh_busy_selector.set_busy_threshold(Duration::from_millis(50));
    let RequestSelection::Attempt(least_busy) = cache
        .select_request_at(
            &mut fresh_busy_selector,
            busy_now + Duration::from_millis(120),
        )
        .expect("select decayed least-busy live replica")
    else {
        panic!("the 150ms replica must become eligible after 120ms")
    };
    assert_eq!(least_busy.target().peer_id, nonleader_peer_ids[1]);
    assert!(least_busy.replica_read);

    let forwarding_policy = ReadPolicy {
        forwarding: true,
        ..ReadPolicy::default()
    };
    let mut forwarding_selector = cache
        .request_selector(region, forwarding_policy)
        .expect("build forwarding selector");
    let RequestSelection::Attempt(direct) = cache
        .select_request(&mut forwarding_selector)
        .expect("select direct leader before foreground failure")
    else {
        panic!("leader must be tried directly first")
    };
    assert_eq!(direct.target().peer_id, leader_peer_id);
    assert!(direct.proxy().is_none());
    assert_eq!(
        cache
            .on_route_send_failure(&direct, StoreLiveness::Unreachable)
            .expect("preserve unreachable logical leader for forwarding"),
        StoreFailureOutcome::ForwardingRequired { epoch: 0 }
    );
    let target_address = direct.target().address.clone();

    let dispatches = Rc::new(RefCell::new(Vec::new()));
    let shared_runtime = SharedReadRuntime::new_with_maintenance(
        RecordingClient {
            inner: TonicCoprocessorClient::new().expect("construct live unary client"),
            dispatches: Rc::clone(&dispatches),
        },
        cache,
    )
    .expect("start production region-cache maintenance");
    let inspection_runtime = shared_runtime.clone();
    let transport = DirectUnaryQueryTransport::with_shared_runtime(
        shared_runtime,
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(5),
            enable_forwarding: true,
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_distsql::FixedTimestampSource::new(1 << 18),
    )
    .expect("construct production forwarding transport");
    let mut runtime = InjectedQueryRuntime::new(transport);

    execute_live_empty_query(&mut runtime, "campaign14-forwarded-first");
    let published_proxy = inspection_runtime
        .with_region_cache(|cache| {
            cache
                .preferred_proxy(region)
                .expect("usable forwarded response must publish its physical proxy")
                .clone()
        })
        .expect("lock the maintained live cache");
    execute_live_empty_query(&mut runtime, "campaign14-forwarded-reuse");
    inspection_runtime
        .with_region_cache(|cache| {
            assert_eq!(
                cache.preferred_proxy(region),
                Some(&published_proxy),
                "fresh selector must retain the proven proxy"
            );
        })
        .expect("lock the maintained live cache");

    inspection_runtime
        .with_region_cache(|cache| cache.on_send_failure(direct.target(), StoreLiveness::Reachable))
        .expect("lock the maintained live cache")
        .expect("record foreground leader recovery");
    execute_live_empty_query(&mut runtime, "campaign14-direct-recovery");
    inspection_runtime
        .with_region_cache(|cache| {
            assert!(
                cache.preferred_proxy(region).is_none(),
                "usable direct recovery must clear the proxy preference"
            );
        })
        .expect("lock the maintained live cache");

    let dispatches = dispatches.borrow();
    assert_eq!(
        dispatches.len(),
        3,
        "two forwarded and one direct RPC expected"
    );
    let first = &dispatches[0];
    let reused = &dispatches[1];
    let recovered = &dispatches[2];
    assert_eq!(first.peer_id, leader_peer_id);
    assert_eq!(first.store_id, direct.target().store_id);
    assert_ne!(first.address, target_address);
    assert_eq!(
        first.forwarded_host.as_deref(),
        Some(target_address.as_str())
    );
    assert_eq!(reused.address, first.address);
    assert_eq!(reused.forwarded_host, first.forwarded_host);
    assert_eq!(recovered.address, target_address);
    assert!(recovered.forwarded_host.is_none());

    println!(
        "campaign14_adaptive_forwarding region_id={} target_peer_id={} target_store_id={} target_address={} proxy_address={} forwarded_header=tikv-forwarded-host forwarded_host={} first_usable_response=true proxy_reused=true reused_usable_response=true busy_sequence=500,800,150 busy_fallback_peer_id={} least_busy_peer_id={} direct_recovery_address={} direct_usable_response=true preference_cleared=true",
        region.id,
        leader_peer_id,
        direct.target().store_id,
        target_address,
        first.address,
        first.forwarded_host.as_deref().expect("forwarded host"),
        busy_fallback.target().peer_id,
        least_busy.target().peer_id,
        recovered.address,
    );
}

#[test]
#[ignore = "requires the cleanup-safe Campaign 18 three-PD/three-TiKV runner"]
fn live_pd_prev_region_and_forwarded_batch_survive_same_address_restart() {
    use prost::Message;
    use tidb_codec::encode_bytes;
    use tidb_proto::{
        CoprocessorKeyRange, CoprocessorRequest, KvrpcContext, KvrpcPeer, KvrpcRegionEpoch,
    };
    use tidb_txnkv::region::RegionLoader;
    use tidb_txnkv::rpc::{AsyncRequestDispatcher, PendingRequest};

    let pd_seed = std::env::var("C18_PD_SEED")
        .expect("C18_PD_SEED must be supplied by run-campaign18-pd-batch-topology.sh");
    let phase_dir = PathBuf::from(
        std::env::var("C18_PHASE_DIR")
            .expect("C18_PHASE_DIR must be supplied by run-campaign18-pd-batch-topology.sh"),
    );
    assert!(phase_dir.is_dir(), "phase directory must already exist");

    let mut loader = PdRegionLoader::connect(pd_seed, Duration::from_secs(5))
        .expect("bootstrap production PD region loader");
    let cluster_id = loader.cluster_id();
    let split_source = loader
        .load_region(TABLE_START)
        .expect("discover live region containing the split key");
    let mut encoded_split_key = Vec::new();
    encode_bytes(&mut encoded_split_key, CAMPAIGN18_SPLIT_KEY);
    write_campaign18_phase(
        &phase_dir,
        "split-source",
        &format!(
            "region_id={}\nsplit_key_hex={}\n",
            split_source.region.id,
            hex_bytes(&encoded_split_key),
        ),
    );
    wait_for_campaign18_phase(&phase_dir, "split-complete");

    // Loading an exact split boundary by inclusive end must transit through
    // production PdRegionLoader's GetPrevRegion branch and return the left
    // neighbor, while ordinary key lookup returns the right neighbor.
    let left = loader
        .load_region_by_end_key(CAMPAIGN18_SPLIT_KEY)
        .expect("GetPrevRegion must resolve the left split neighbor");
    let right = loader
        .load_region(CAMPAIGN18_SPLIT_KEY)
        .expect("GetRegion must resolve the right split neighbor");
    assert_ne!(left.region, right.region);
    assert_eq!(left.end_key, CAMPAIGN18_SPLIT_KEY);
    assert_eq!(right.start_key, CAMPAIGN18_SPLIT_KEY);
    assert_eq!(
        left.end_key, right.start_key,
        "split neighbors must be adjacent"
    );

    let leader_peer_id = left.leader_peer_id.expect("left region must have a leader");
    let leader = left
        .peers
        .iter()
        .find(|peer| peer.id == leader_peer_id)
        .expect("PD leader must be one projected peer");
    let logical_peer = left
        .peers
        .iter()
        .find(|peer| peer.id != leader_peer_id && peer.role == PeerRole::Voter && !peer.is_witness)
        .expect("three-TiKV topology must expose a stable nonleader voter target");
    let logical_store = left
        .stores
        .iter()
        .find(|store| store.id == logical_peer.store_id)
        .expect("follower store must have a resolved address");
    let physical_store = left
        .stores
        .iter()
        .find(|store| store.id == leader.store_id)
        .expect("proxy store must have a resolved address");
    assert_ne!(physical_store.address, logical_store.address);

    let context = KvrpcContext {
        region_id: left.region.id,
        region_epoch: Some(KvrpcRegionEpoch {
            conf_ver: left.region.epoch.conf_ver,
            version: left.region.epoch.version,
        }),
        peer: Some(KvrpcPeer {
            id: logical_peer.id,
            store_id: logical_peer.store_id,
            role: logical_peer.role.as_i32(),
            is_witness: logical_peer.is_witness,
        }),
        replica_read: true,
        cluster_id,
        request_source: "campaign18-live-pd-batch".to_owned(),
        ..KvrpcContext::default()
    };
    let request = DirectUnaryRequest {
        endpoint: EndpointType::TiKv,
        replica_read_type: ClientReplicaReadType::Follower,
        replica_read: true,
        stale_read: false,
        input_request_source: "campaign18-live-pd-batch".to_owned(),
        predicted_read_bytes: 0,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        context: context.clone(),
        encoded_request: CoprocessorRequest {
            context: Some(context),
            tp: 103,
            data: TABLE_SCAN_DAG.to_vec(),
            start_ts: 1,
            ranges: vec![CoprocessorKeyRange {
                start: TABLE_START.to_vec(),
                end: CAMPAIGN18_SPLIT_KEY.to_vec(),
            }],
            ..CoprocessorRequest::default()
        }
        .encode_to_vec(),
    };

    let mut client = TonicCoprocessorClient::new().expect("construct production tonic client");
    let direct_call = UnaryCallContext::with_timeout(Duration::from_secs(10));
    let (direct_completion, mut direct_pull) = completion_pair(CompletionRunLoop::new(), || {});
    let direct_receipts = client
        .submit_batch_commands(
            &physical_store.address,
            vec![BatchCommandEntry::new(
                OpaqueBatchCommand::new(BatchCommandTag::Empty, Vec::new()),
                direct_completion,
            )],
        )
        .expect("publish production direct BatchCommands request");
    assert_eq!(direct_receipts.len(), 1);
    let direct_raw = direct_pull
        .complete(&direct_call)
        .expect("drive production direct BatchCommands completion")
        .expect("direct BatchCommands request must succeed");
    assert_eq!(direct_raw.tag(), BatchCommandTag::Empty);
    let direct_generation = client
        .batch_stream_generation(&physical_store.address, None)
        .expect("successful direct request must retain its independent stream");

    let first_call = UnaryCallContext::with_timeout(Duration::from_secs(10));
    let mut first = client
        .begin(
            &physical_store.address,
            Some(&logical_store.address),
            &request,
            &first_call,
        )
        .expect("begin first production forwarded BatchCommands request");
    let first_raw = first
        .complete(&first_call)
        .expect("drive first production BatchCommands completion")
        .expect("first forwarded BatchCommands request must succeed");
    assert_usable_campaign18_response(&first_raw);
    let generation_n = client
        .batch_stream_generation(&physical_store.address, Some(&logical_store.address))
        .expect("successful forwarded request must retain generation N");
    let forwarded_channel_version = client
        .connection_version(&physical_store.address)
        .expect("forwarded request must retain its physical channel");

    let mut route_phase = format!(
        "left_region_id={}\nright_region_id={}\nboundary_hex={}\nphysical_store_id={}\nphysical_address={}\nlogical_store_id={}\nlogical_peer_id={}\nlogical_address={}\ngeneration_n={}\nforwarded_channel_version={}\ndirect_generation={}\n",
        left.region.id,
        right.region.id,
        hex_bytes(CAMPAIGN18_SPLIT_KEY),
        physical_store.id,
        physical_store.address,
        logical_store.id,
        logical_peer.id,
        logical_store.address,
        generation_n,
        forwarded_channel_version,
        direct_generation,
    );
    for store in &left.stores {
        route_phase.push_str(&format!("store_address={}\n", store.address));
    }
    write_campaign18_phase(&phase_dir, "route-ready", &route_phase);
    wait_for_campaign18_phase(&phase_dir, "logical-target-frozen");

    // The runner freezes the exact logical target process, so begin publishes
    // through the live physical proxy into generation N but cannot receive a
    // response before the runner kills that target.
    let failed_call = UnaryCallContext::with_timeout(Duration::from_secs(15));
    let mut failed = client
        .begin(
            &physical_store.address,
            Some(&logical_store.address),
            &request,
            &failed_call,
        )
        .expect("publish one request into the frozen generation N stream");
    assert_eq!(
        client.batch_stream_generation(&physical_store.address, Some(&logical_store.address)),
        Some(generation_n),
    );
    let failed_request_watermark = client.batch_request_id_watermark();
    write_campaign18_phase(&phase_dir, "request-published", "published=1\n");
    let mut failure_count = 0;
    let failure = match failed
        .complete(&failed_call)
        .expect("generation failure must use the pending completion")
    {
        Ok(_) => panic!("killed logical target must fail forwarded generation N"),
        Err(error) => {
            failure_count += 1;
            error
        }
    };
    let connection = failure
        .connection()
        .expect("BatchCommands stream failure must retain address generation");
    assert_eq!(connection.address(), physical_store.address);
    assert_eq!(connection.version(), forwarded_channel_version);
    assert!(failed
        .try_complete()
        .expect("duplicate completion probe")
        .is_none());
    assert_eq!(failure_count, 1);

    let direct_survival_call = UnaryCallContext::with_timeout(Duration::from_secs(10));
    let (direct_survival_completion, mut direct_survival_pull) =
        completion_pair(CompletionRunLoop::new(), || {});
    let direct_survival_receipts = client
        .submit_batch_commands(
            &physical_store.address,
            vec![BatchCommandEntry::new(
                OpaqueBatchCommand::new(BatchCommandTag::Empty, Vec::new()),
                direct_survival_completion,
            )],
        )
        .expect("forwarded failure must not retire the sibling direct stream");
    assert_eq!(direct_survival_receipts.len(), 1);
    let direct_survival_raw = direct_survival_pull
        .complete(&direct_survival_call)
        .expect("drive direct isolation completion")
        .expect("direct stream must survive forwarded stream failure");
    assert_eq!(direct_survival_raw.tag(), BatchCommandTag::Empty);
    assert_eq!(
        client.batch_stream_generation(&physical_store.address, None),
        Some(direct_generation),
        "forwarded-host failure must preserve the same-physical direct route generation",
    );
    assert_ne!(
        client.batch_stream_generation(&physical_store.address, Some(&logical_store.address)),
        Some(generation_n),
        "failed forwarded generation must retire before caller retry",
    );
    let post_isolation_watermark = client.batch_request_id_watermark();
    let scheduled_after_failure = post_isolation_watermark.saturating_sub(failed_request_watermark);
    assert_eq!(
        scheduled_after_failure, 1,
        "only the explicit direct-survival request may allocate after failure",
    );
    let transport_scheduled_resends = scheduled_after_failure.saturating_sub(1);
    write_campaign18_phase(
        &phase_dir,
        "failure-observed",
        &format!(
            "failed_address={}\nfailed_route_generation={}\nfailed_channel_version={}\nfailure_count={}\ntransport_scheduled_resends={}\ndirect_generation={}\ndirect_survived=true\n",
            connection.address(),
            generation_n,
            connection.version(),
            failure_count,
            transport_scheduled_resends,
            direct_generation,
        ),
    );
    wait_for_campaign18_phase(&phase_dir, "tikv-restarted");

    // Separate direct clients prove that the exact restarted peer can serve
    // this follower read without warming the measured physical proxy's
    // forwarding path. The main-client call below remains the first forwarded
    // attempt after restart.
    let readiness_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut readiness_client =
            TonicCoprocessorClient::new().expect("construct restarted-peer readiness client");
        let readiness_call = UnaryCallContext::with_timeout(Duration::from_secs(2));
        let readiness_result =
            match readiness_client.begin(&logical_store.address, None, &request, &readiness_call) {
                Ok(mut pending) => match pending.complete(&readiness_call) {
                    Ok(Ok(response)) => Ok(response),
                    Ok(Err(error)) => Err((
                        error.connection().is_some()
                            || matches!(error, DirectUnaryClientError::Timeout { .. }),
                        error.to_string(),
                    )),
                    Err(CompletionError::DeadlineExceeded) => {
                        Err((true, "direct readiness attempt timed out".to_owned()))
                    }
                    Err(error) => Err((false, error.to_string())),
                },
                Err(error) => Err((
                    error.connection().is_some()
                        || matches!(error, DirectUnaryClientError::Timeout { .. }),
                    error.to_string(),
                )),
            };
        readiness_client
            .close()
            .expect("close restarted-peer readiness client");

        let retry_reason = match readiness_result {
            Ok(response) => {
                let response =
                    tidb_proto::CoprocessorResponse::decode(response.encoded_response.as_slice())
                        .expect("decode restarted-peer readiness response");
                if let Some(region_error) = response.region_error {
                    Some(format!("region not ready: {region_error:?}"))
                } else {
                    assert!(response.other_error.is_empty());
                    break;
                }
            }
            Err((true, reason)) => Some(reason),
            Err((false, reason)) => {
                panic!("restarted-peer readiness failed permanently: {reason}")
            }
        };
        assert!(
            Instant::now() < readiness_deadline,
            "restarted follower did not become ready: {}",
            retry_reason.expect("every readiness retry retains a reason"),
        );
        thread::sleep(Duration::from_millis(100));
    }

    let retry_call = UnaryCallContext::with_timeout(Duration::from_secs(15));
    let mut retry = client
        .begin(
            &physical_store.address,
            Some(&logical_store.address),
            &request,
            &retry_call,
        )
        .expect("caller retry must create a new forwarded stream");
    let retry_raw = retry
        .complete(&retry_call)
        .expect("drive explicit caller retry")
        .expect("same-address restarted TiKV must serve caller retry");
    assert_usable_campaign18_response(&retry_raw);
    let retry_generation = client
        .batch_stream_generation(&physical_store.address, Some(&logical_store.address))
        .expect("caller retry must retain its new generation");
    assert!(retry_generation > generation_n);
    let retry_channel_version = client
        .connection_version(&physical_store.address)
        .expect("caller retry must retain its physical channel");
    client.close().expect("close production tonic client");

    write_campaign18_phase(
        &phase_dir,
        "completed",
        &format!(
            "left_region_id={}\nright_region_id={}\nadjacent=true\nphysical_address={}\nlogical_address={}\ninitial_route_generation={}\nfailed_route_generation={}\nretry_route_generation={}\ninitial_channel_version={}\nfailed_channel_version={}\nretry_channel_version={}\nfailure_count={}\ntransport_scheduled_resends={}\ndirect_generation={}\ndirect_survived=true\nexact_peer_readiness=true\nretry_usable=true\nplaintext_only=true\n",
            left.region.id,
            right.region.id,
            physical_store.address,
            logical_store.address,
            generation_n,
            generation_n,
            retry_generation,
            forwarded_channel_version,
            forwarded_channel_version,
            retry_channel_version,
            failure_count,
            transport_scheduled_resends,
            direct_generation,
        ),
    );
}

fn assert_usable_campaign18_response(response: &DirectUnaryResponse) {
    use prost::Message;

    let response = tidb_proto::CoprocessorResponse::decode(response.encoded_response.as_slice())
        .expect("decode live BatchCommands Coprocessor response");
    assert!(response.region_error.is_none());
    assert!(response.other_error.is_empty());
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn write_campaign18_phase(directory: &Path, name: &str, body: &str) {
    let temporary = directory.join(format!(".{name}.tmp"));
    fs::write(&temporary, body).expect("write temporary Campaign 18 phase file");
    fs::rename(temporary, directory.join(name)).expect("publish Campaign 18 phase atomically");
}

fn wait_for_campaign18_phase(directory: &Path, name: &str) {
    let path = directory.join(name);
    let deadline = Instant::now() + CAMPAIGN18_PHASE_TIMEOUT;
    while !path.is_file() {
        assert!(Instant::now() < deadline, "timed out waiting for {name}");
        thread::sleep(Duration::from_millis(100));
    }
}
