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

use std::time::{Duration, Instant};

use prost::Message;
use tidb_codec::encode_bytes;
use tidb_proto::{
    CoprocessorKeyRange, CoprocessorRequest, CoprocessorResponse, KvrpcContext, KvrpcPeer,
    KvrpcRegionEpoch,
};
use tidb_txnkv::region::{PeerRole, ReadPolicy, RegionCache, ReplicaReadMode, RequestSelection};
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::{
    ClientReplicaReadType, DirectUnaryClient, DirectUnaryRequest, EndpointType, PdRegionLoader,
};

const TABLE_START: &[u8] = b"t\x80\0\0\0\0\0\0*_r";
const TABLE_END: &[u8] = b"t\x80\0\0\0\0\0\0+";
const TABLE_SCAN_DAG: &[u8] = &[0x12, 0x04, 0x12, 0x02, 0x08, 0x2a];

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

    let mut selector = cache
        .request_selector(
            location.region,
            ReadPolicy {
                mode: ReplicaReadMode::Follower,
                ..ReadPolicy::default()
            },
        )
        .expect("construct request-scoped follower selector");
    let RequestSelection::Attempt(selected) = cache
        .select_request(&mut selector)
        .expect("select follower request")
    else {
        panic!("fresh live region must have a follower candidate")
    };
    assert_ne!(selected.attempt.peer_id, leader_peer_id);
    assert!(matches!(
        selected.role,
        PeerRole::Voter | PeerRole::IncomingVoter | PeerRole::DemotingVoter
    ));
    assert!(!selected.is_witness);
    assert!(selected.replica_read);
    assert!(!selected.stale_read);
    assert!(!selected.cached_leader);

    let mut encoded_start = Vec::new();
    let mut encoded_end = Vec::new();
    encode_bytes(&mut encoded_start, TABLE_START);
    encode_bytes(&mut encoded_end, TABLE_END);
    let context = KvrpcContext {
        region_id: location.region.id,
        region_epoch: Some(KvrpcRegionEpoch {
            conf_ver: location.region.epoch.conf_ver,
            version: location.region.epoch.version,
        }),
        peer: Some(KvrpcPeer {
            id: selected.attempt.peer_id,
            store_id: selected.attempt.store_id,
            role: selected.role.as_i32(),
            is_witness: selected.is_witness,
        }),
        replica_read: true,
        stale_read: false,
        cluster_id: cache.cluster_id(),
        request_source: "external_campaign13".to_owned(),
        ..KvrpcContext::default()
    };
    let request = DirectUnaryRequest {
        endpoint: EndpointType::TiKv,
        replica_read_type: ClientReplicaReadType::Follower,
        replica_read: true,
        stale_read: false,
        input_request_source: "external_campaign13".to_owned(),
        predicted_read_bytes: 0,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        context,
        encoded_request: CoprocessorRequest {
            tp: 103,
            data: TABLE_SCAN_DAG.to_vec(),
            ranges: vec![CoprocessorKeyRange {
                start: encoded_start,
                end: encoded_end,
            }],
            start_ts: 1,
            ..CoprocessorRequest::default()
        }
        .encode_to_vec(),
    };

    let mut client = TonicCoprocessorClient::new().expect("construct live unary client");
    let started = Instant::now();
    let raw = client
        .send_request(&selected.attempt.address, &request, Duration::from_secs(5))
        .expect("send follower read to Rust-selected nonleader voter");
    assert!(selector.record_attempt_result(&selected.attempt, started.elapsed()));
    let response = CoprocessorResponse::decode(raw.encoded_response.as_slice())
        .expect("decode live coprocessor response");
    assert!(
        response.region_error.is_none(),
        "follower route returned region error: {:?}",
        response.region_error
    );
    assert!(
        response.other_error.is_empty(),
        "follower route returned application error: {}",
        response.other_error
    );

    println!(
        "campaign13_replica_read region_id={} leader_peer_id={} selected_peer_id={} selected_store_id={} selected_address={} replica_read={} stale_read={} usable_response=true",
        location.region.id,
        leader_peer_id,
        selected.attempt.peer_id,
        selected.attempt.store_id,
        selected.attempt.address,
        selected.replica_read,
        selected.stale_read,
    );
}
