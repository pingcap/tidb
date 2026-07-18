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
use std::rc::Rc;
use std::time::Duration;

use tidb_datatype::FieldType;
use tidb_distsql::{
    decode_select_response, DirectUnaryClient, DirectUnaryClientError, DirectUnaryQueryTransport,
    DirectUnaryRequest, DirectUnaryResponse, DirectUnaryRuntimeConfig, InjectedQueryRuntime,
    KvRequestMetadata, QueryResultContext, ReplicaReadType, RequestKeyRange, RequestKeyRanges,
    RequestType, SelectInput, StoreType, TransportRequest, WarningCollector,
};
use tidb_txnkv::region::{PeerRole, RegionCache, StoreLiveness};
use tidb_txnkv::region::{ReadPolicy, RequestSelection, StoreFailureOutcome};
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::{ClientReplicaReadType, PdRegionLoader, SharedReadRuntime};

const TABLE_START: &[u8] = b"t\x80\0\0\0\0\0\0*_r";
const TABLE_END: &[u8] = b"t\x80\0\0\0\0\0\0+";
const TABLE_SCAN_DAG: &[u8] = &[0x12, 0x04, 0x12, 0x02, 0x08, 0x2a];

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
    let shared_runtime = SharedReadRuntime::new(
        RecordingClient {
            inner: TonicCoprocessorClient::new().expect("construct live unary client"),
            dispatches: Rc::clone(&dispatches),
        },
        cache,
    );
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
        .region_cache()
        .borrow_mut()
        .locate_key(TABLE_START)
        .expect("inspect the retained live region after follower success")
        .clone();
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
    let shared_runtime = SharedReadRuntime::new(
        RecordingClient {
            inner: TonicCoprocessorClient::new().expect("construct live unary client"),
            dispatches: Rc::clone(&dispatches),
        },
        cache,
    );
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
        .region_cache()
        .borrow()
        .preferred_proxy(region)
        .expect("usable forwarded response must publish its physical proxy")
        .clone();
    execute_live_empty_query(&mut runtime, "campaign14-forwarded-reuse");
    assert_eq!(
        inspection_runtime
            .region_cache()
            .borrow()
            .preferred_proxy(region),
        Some(&published_proxy),
        "fresh selector must retain the proven proxy"
    );

    inspection_runtime
        .region_cache()
        .borrow_mut()
        .on_send_failure(direct.target(), StoreLiveness::Reachable)
        .expect("record foreground leader recovery");
    execute_live_empty_query(&mut runtime, "campaign14-direct-recovery");
    assert!(
        inspection_runtime
            .region_cache()
            .borrow()
            .preferred_proxy(region)
            .is_none(),
        "usable direct recovery must clear the proxy preference"
    );

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
