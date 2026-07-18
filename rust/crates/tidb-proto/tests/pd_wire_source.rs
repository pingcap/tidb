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
    // GetRegion and GetPrevRegion intentionally share this exact request wire.
    let previous_region = pdpb::GetRegionRequest {
        header: Some(header.clone()),
        region_key: vec![0xaa, 0xbb],
        need_buckets: true,
    };
    assert_eq!(previous_region.encode_to_vec(), region.encode_to_vec());
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
        labels: vec![metapb::StoreLabel {
            key: "zone".to_owned(),
            value: "z1".to_owned(),
        }],
        node_state: metapb::NodeState::Removing as i32,
    };
    let wire = store.encode_to_vec();
    assert!(wire.contains(&0x22)); // labels, field 4
    assert!(wire.contains(&0x68)); // node_state, field 13
    assert_eq!(metapb::Store::decode(wire.as_slice()).unwrap(), store);
}

#[test]
fn bucket_and_scan_projection_keeps_every_pinned_source_tag() {
    let stats = metapb::BucketStats {
        read_bytes: vec![1],
        write_bytes: vec![2],
        read_qps: vec![3],
        write_qps: vec![4],
        read_keys: vec![5],
        write_keys: vec![6],
    };
    assert_eq!(
        stats.encode_to_vec(),
        [
            0x0a, 0x01, 0x01, 0x12, 0x01, 0x02, 0x1a, 0x01, 0x03, 0x22, 0x01, 0x04, 0x2a, 0x01,
            0x05, 0x32, 0x01, 0x06,
        ]
    );
    let buckets = metapb::Buckets {
        region_id: 7,
        version: 9,
        keys: vec![b"a".to_vec(), b"m".to_vec(), Vec::new()],
        stats: Some(stats),
        period_in_ms: 1_000,
    };
    let bucket_wire = buckets.encode_to_vec();
    assert!(bucket_wire.contains(&0x08)); // region_id, field 1
    assert!(bucket_wire.contains(&0x10)); // version, field 2
    assert!(bucket_wire.contains(&0x1a)); // keys, field 3
    assert!(bucket_wire.contains(&0x22)); // stats, field 4
    assert!(bucket_wire.contains(&0x28)); // period_in_ms, field 5
    assert_eq!(
        metapb::Buckets::decode(bucket_wire.as_slice()).unwrap(),
        buckets
    );

    let response = pdpb::GetRegionResponse {
        buckets: Some(buckets),
        ..pdpb::GetRegionResponse::default()
    };
    assert!(response.encode_to_vec().contains(&0x3a)); // buckets, field 7

    let extended = pdpb::Region {
        region: Some(metapb::Region::default()),
        leader: Some(metapb::Peer::default()),
        down_peers: vec![pdpb::PeerStats::default()],
        pending_peers: vec![metapb::Peer::default()],
        buckets: Some(metapb::Buckets::default()),
    };
    assert_eq!(
        extended.encode_to_vec(),
        [0x0a, 0x00, 0x12, 0x00, 0x1a, 0x00, 0x22, 0x00, 0x2a, 0x00]
    );
    let batch_response = pdpb::BatchScanRegionsResponse {
        header: None,
        regions: vec![extended],
    };
    assert_eq!(batch_response.encode_to_vec()[0], 0x12); // regions, field 2

    let by_id = pdpb::GetRegionByIdRequest {
        header: None,
        region_id: 17,
        need_buckets: true,
    };
    assert_eq!(by_id.encode_to_vec(), [0x10, 0x11, 0x18, 0x01]);

    let scan = pdpb::ScanRegionsRequest {
        header: None,
        start_key: b"a".to_vec(),
        limit: 128,
        end_key: b"z".to_vec(),
    };
    assert_eq!(
        scan.encode_to_vec(),
        [0x12, 0x01, b'a', 0x18, 0x80, 0x01, 0x22, 0x01, b'z']
    );

    let batch = pdpb::BatchScanRegionsRequest {
        header: None,
        need_buckets: true,
        ranges: vec![pdpb::KeyRange {
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
        }],
        limit: 128,
        contain_all_key_range: true,
    };
    let wire = batch.encode_to_vec();
    assert!(wire.contains(&0x10)); // need_buckets, field 2
    assert!(wire.contains(&0x1a)); // ranges, field 3
    assert!(wire.contains(&0x20)); // limit, field 4
    assert!(wire.contains(&0x28)); // contain_all_key_range, field 5
    assert_eq!(
        pdpb::BatchScanRegionsRequest::decode(wire.as_slice()).unwrap(),
        batch
    );
}
