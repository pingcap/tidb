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

//! Focused source checks for the raw `kv.Request` envelope ownership seam.

use tidb_distsql::{KvRequestBuilder, PartitionIdAndRanges, RequestKeyRange, RequestType};

#[test]
fn go_request_builder_preserves_raw_payload_bytes() {
    // `pkg/kv/kv.go:568-574` owns `Request.Data` as an opaque byte slice;
    // `pkg/distsql/request_builder.go:189-253` fills it with the exact
    // marshalled DAG/analyze/checksum request.  This leaf must not decode,
    // normalize, or synthesize those bytes before a protobuf owner exists.
    let payload = vec![0x18, 0x00, 0x20, 0x01, 0xff];
    let mut builder = KvRequestBuilder::new();
    builder
        .set_request_type(RequestType::Dag)
        .set_data(payload.clone());

    let request = builder.build().expect("raw request build");
    assert_eq!(request.request_type, RequestType::Dag);
    assert_eq!(request.data.as_deref(), Some(payload.as_slice()));
}

#[test]
fn go_partition_table_scan_keeps_partition_ids_next_to_ranges() {
    // `pkg/kv/kv.go:579-581,678-682` and
    // `pkg/distsql/request_builder.go:311-314` keep TiFlash partition
    // ranges separate from ordinary `KeyRanges`.  Preserve source order and
    // bytes here; region splitting and RPC conversion remain unimplemented.
    let mut builder = KvRequestBuilder::new();
    builder.set_partition_id_and_ranges(vec![
        PartitionIdAndRanges {
            id: 41,
            key_ranges: vec![RequestKeyRange {
                start_key: vec![1, 2].into(),
                end_key: vec![3].into(),
            }],
        },
        PartitionIdAndRanges {
            id: 42,
            key_ranges: vec![RequestKeyRange {
                start_key: vec![4].into(),
                end_key: vec![5, 6].into(),
            }],
        },
    ]);

    let request = builder.build().expect("partition request build");
    assert_eq!(request.partition_id_and_ranges.len(), 2);
    assert_eq!(request.partition_id_and_ranges[0].id, 41);
    assert_eq!(
        request.partition_id_and_ranges[0].key_ranges[0].start_key,
        vec![1, 2]
    );
    assert_eq!(request.partition_id_and_ranges[1].id, 42);
    assert_eq!(
        request.partition_id_and_ranges[1].key_ranges[0].end_key,
        vec![5, 6]
    );
    assert!(request.key_ranges.is_some());
}
