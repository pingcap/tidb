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
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::{ClientReplicaReadType, PdRegionLoader, SharedReadRuntime};

const TABLE_START: &[u8] = b"t\x80\0\0\0\0\0\0*_r";
const TABLE_END: &[u8] = b"t\x80\0\0\0\0\0\0+";
const TABLE_SCAN_DAG: &[u8] = &[0x12, 0x04, 0x12, 0x02, 0x08, 0x2a];

#[derive(Clone, Debug)]
struct ObservedDispatch {
    address: String,
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
    fn record_dispatch(&self, address: &str, request: &DirectUnaryRequest) {
        let peer = request
            .context
            .peer
            .as_ref()
            .expect("production transport must attach one selected peer");
        self.dispatches.borrow_mut().push(ObservedDispatch {
            address: address.to_owned(),
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
        self.record_dispatch(address, request);
        self.inner.send_request(address, request, timeout)
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &tidb_txnkv::UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.record_dispatch(address, request);
        self.inner.send_request_with_context(address, request, call)
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
