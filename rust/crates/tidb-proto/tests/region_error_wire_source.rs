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
use tidb_proto::{errorpb, metapb};

#[test]
fn not_leader_restores_exact_nested_peer_field() {
    let error = errorpb::NotLeader {
        region_id: 7,
        leader: Some(metapb::Peer {
            id: 11,
            store_id: 101,
            role: 99,
            is_witness: true,
        }),
    };

    // field 1 region_id, field 2 length-delimited metapb.Peer; the nested
    // peer keeps fields 1, 2, 3, and 4, including an unknown role value.
    let go_wire = [
        0x08, 0x07, 0x12, 0x08, 0x08, 0x0b, 0x10, 0x65, 0x18, 0x63, 0x20, 0x01,
    ];
    assert_eq!(error.encode_to_vec(), go_wire);
    assert_eq!(
        errorpb::NotLeader::decode(go_wire.as_slice()).unwrap(),
        error
    );
}

#[test]
fn epoch_not_match_restores_exact_repeated_current_regions_field() {
    let region = metapb::Region {
        id: 7,
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        region_epoch: Some(metapb::RegionEpoch {
            conf_ver: 3,
            version: 4,
        }),
        peers: vec![metapb::Peer {
            id: 11,
            store_id: 101,
            role: 0,
            is_witness: false,
        }],
    };
    let error = errorpb::EpochNotMatch {
        current_regions: vec![region.clone(), region],
    };
    let wire = error.encode_to_vec();

    // Each current region is one occurrence of field 1, not an invented
    // wrapper or flattened region identity.
    assert_eq!(wire.iter().filter(|byte| **byte == 0x0a).count(), 2);
    assert_eq!(
        errorpb::EpochNotMatch::decode(wire.as_slice()).unwrap(),
        error
    );
}
