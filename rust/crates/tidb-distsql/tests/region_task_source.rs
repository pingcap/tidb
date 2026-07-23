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

//! Source-contract checks for the pre-RPC region child-task envelope.

use prost::Message;
use tidb_distsql::{
    RegionTaskEnvelope, RegionTaskEpoch, RegionTaskPeer, RequestKeyRange, VersionedRegionKeyRange,
};
use tidb_proto::StoreBatchTask;

#[test]
fn store_batch_task_preserves_region_epoch_peer_ranges_and_bucket_version() {
    // `pkg/store/copr/coprocessor.go:317-335` maps `copTask.batchTaskList`
    // children to `StoreBatchTask`; `coprocessor_test.go:1055-1092`
    // specifically asserts that each child bucket version survives.
    let envelope = RegionTaskEnvelope {
        region_id: 9,
        region_epoch: Some(RegionTaskEpoch {
            conf_ver: 3,
            version: 4,
        }),
        peer: Some(RegionTaskPeer {
            id: 5,
            store_id: 7,
            role: 1,
            is_witness: true,
        }),
        ranges: vec![RequestKeyRange {
            start_key: b"a".to_vec().into(),
            end_key: b"b".to_vec().into(),
        }],
        task_id: 42,
        versioned_ranges: vec![VersionedRegionKeyRange {
            range: RequestKeyRange {
                start_key: b"p".to_vec().into(),
                end_key: b"p".to_vec().into(),
            },
            read_ts: 99,
        }],
        buckets_version: 202,
        ..Default::default()
    };

    let encoded = envelope.encode_to_vec();
    let decoded = StoreBatchTask::decode(encoded.as_slice()).expect("store task wire");
    assert_eq!(decoded.region_id, 9);
    assert_eq!(
        decoded.region_epoch.as_ref().map(|epoch| epoch.conf_ver),
        Some(3)
    );
    assert_eq!(
        decoded.region_epoch.as_ref().map(|epoch| epoch.version),
        Some(4)
    );
    assert_eq!(decoded.peer.as_ref().map(|peer| peer.id), Some(5));
    assert_eq!(decoded.peer.as_ref().map(|peer| peer.store_id), Some(7));
    assert_eq!(decoded.peer.as_ref().map(|peer| peer.role), Some(1));
    assert_eq!(
        decoded.peer.as_ref().map(|peer| peer.is_witness),
        Some(true)
    );
    assert_eq!(decoded.ranges[0].start, b"a");
    assert_eq!(decoded.ranges[0].end, b"b");
    assert_eq!(decoded.task_id, 42);
    assert_eq!(decoded.versioned_ranges[0].read_ts, 99);
    assert_eq!(
        decoded.versioned_ranges[0].range.as_ref().unwrap().start,
        b"p"
    );
    assert_eq!(decoded.buckets_version, 202);

    // StoreBatchTask field numbers 1..7 are intentionally sparse only by
    // message payload; this anchor catches accidental field renumbering.
    assert!(encoded.windows(2).any(|window| window == [0x08, 0x09])); // region_id = 1
    assert!(encoded.contains(&0x12)); // region_epoch = 2
    assert!(encoded.contains(&0x1a)); // peer = 3
    assert!(encoded.contains(&0x22)); // ranges = 4
    assert!(encoded.contains(&0x28)); // task_id = 5
    assert!(encoded.contains(&0x32)); // versioned_ranges = 6
    assert!(encoded.contains(&0x38)); // buckets_version = 7
}

#[test]
fn absent_region_metadata_stays_absent_and_ranges_keep_order() {
    let envelope = RegionTaskEnvelope {
        region_id: 17,
        ranges: vec![
            RequestKeyRange {
                start_key: vec![2].into(),
                end_key: vec![3].into(),
            },
            RequestKeyRange {
                start_key: vec![0].into(),
                end_key: vec![1].into(),
            },
        ],
        ..Default::default()
    };
    let decoded = StoreBatchTask::decode(envelope.encode_to_vec().as_slice()).unwrap();
    assert!(decoded.region_epoch.is_none());
    assert!(decoded.peer.is_none());
    assert_eq!(decoded.ranges.len(), 2);
    assert_eq!(decoded.ranges[0].start, vec![2]);
    assert_eq!(decoded.ranges[1].start, vec![0]);
    assert_eq!(decoded.task_id, 0);
    assert_eq!(decoded.buckets_version, 0);
}
