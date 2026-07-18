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

use prost::Message;
use tidb_proto::{metapb, pdpb};

#[test]
fn pd_request_projection_keeps_source_field_numbers_and_presence() {
    let members = pdpb::GetMembersRequest { header: None };
    assert!(members.encode_to_vec().is_empty());

    let header = pdpb::RequestHeader {
        cluster_id: 42,
        sender_id: 0,
        caller_id: String::new(),
        caller_component: String::new(),
    };
    let region = pdpb::GetRegionRequest {
        header: Some(header.clone()),
        region_key: vec![0xaa, 0xbb],
        need_buckets: true,
    };
    assert_eq!(
        region.encode_to_vec(),
        vec![0x0a, 0x02, 0x08, 0x2a, 0x12, 0x02, 0xaa, 0xbb, 0x18, 0x01]
    );
    let store = pdpb::GetStoreRequest {
        header: Some(header),
        store_id: 7,
    };
    assert_eq!(
        store.encode_to_vec(),
        vec![0x0a, 0x02, 0x08, 0x2a, 0x10, 0x07]
    );
}

#[test]
fn region_peer_store_projection_round_trips_sparse_source_tags() {
    let region = metapb::Region {
        id: 11,
        start_key: vec![1],
        end_key: vec![2],
        region_epoch: Some(metapb::RegionEpoch {
            conf_ver: 3,
            version: 4,
        }),
        peers: vec![metapb::Peer {
            id: 5,
            store_id: 6,
            role: metapb::PeerRole::DemotingVoter as i32,
            is_witness: true,
        }],
    };
    let wire = region.encode_to_vec();
    assert!(wire.contains(&0x22)); // region_epoch, field 4
    assert!(wire.contains(&0x2a)); // peers, field 5
    assert_eq!(metapb::Region::decode(wire.as_slice()).unwrap(), region);

    let store = metapb::Store {
        id: 6,
        address: "127.0.0.1:20160".to_owned(),
        state: metapb::StoreState::Offline as i32,
        node_state: metapb::NodeState::Removing as i32,
    };
    let wire = store.encode_to_vec();
    assert!(wire.contains(&0x68)); // node_state, field 13
    assert_eq!(metapb::Store::decode(wire.as_slice()).unwrap(), store);
}
