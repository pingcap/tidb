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

//! Source-contract tests for the pre-region coprocessor request wire leaf.

use prost::Message;
use tidb_distsql::{
    CoprocessorRequestEnvelope, KvRequestBuilder, RequestKeyRange, RequestType, TransportBinding,
    TransportRequest, TransportRequestError,
};
use tidb_proto::CoprocessorRequest;

#[test]
fn coprocessor_request_uses_source_field_numbers_and_preserves_payload() {
    // `pkg/distsql/request_builder.go:189-195` marshals the DAG into
    // `kv.Request.Data`; `pkg/kv/kv.go:568-580` keeps that payload and the
    // ranges as request metadata. `pkg/store/copr/coprocessor.go:1745-1757`
    // projects them into this exact coprocessor.Request field set.
    let payload = vec![0xde, 0xad, 0x00];
    let mut builder = KvRequestBuilder::new();
    builder
        .set_request_type(RequestType::Dag)
        .set_start_ts(42)
        .set_data(payload.clone());
    let metadata = builder.build().expect("metadata");

    let envelope = CoprocessorRequestEnvelope::from_metadata(
        &metadata,
        vec![RequestKeyRange {
            start_key: vec![1],
            end_key: vec![2, 3],
        }],
    )
    .with_context(vec![0x99])
    .with_paging_size(7)
    .with_cache_version(17)
    .with_max_keys_read(257);

    let encoded = envelope.encode_to_vec();
    let expected = vec![
        0x0a, 0x01, 0x99, // context = 1
        0x10, 0x67, // tp = 2
        0x1a, 0x03, 0xde, 0xad, 0x00, // data = 3
        0x22, 0x07, 0x0a, 0x01, 0x01, 0x12, 0x02, 0x02, 0x03, // ranges = 4
        0x28, 0x01, // is_cache_enabled = 5
        0x30, 0x11, // cache_if_match_version = 6
        0x38, 0x2a, // start_ts = 7
        0x50, 0x07, // paging_size = 10
        0x80, 0x01, 0x81, 0x02, // max_keys_read = 16, 257
    ];
    assert_eq!(encoded, expected);

    let decoded = CoprocessorRequest::decode(encoded.as_slice()).expect("coprocessor wire");
    assert_eq!(decoded.context.as_deref(), Some([0x99].as_slice()));
    assert_eq!(decoded.tp, RequestType::Dag as i64);
    assert_eq!(decoded.data, payload);
    assert_eq!(decoded.ranges[0].start, vec![1]);
    assert_eq!(decoded.ranges[0].end, vec![2, 3]);
    assert_eq!(decoded.start_ts, 42);
    assert_eq!(decoded.paging_size, 7);
    assert_eq!(decoded.max_keys_read, 257);
    // The source leaves field 11/14/15 for transport-owned task metadata;
    // this projection does not fabricate those messages.
}

#[test]
fn transport_request_rejects_unbound_serialization_and_allows_bound_snapshot() {
    let mut builder = KvRequestBuilder::new();
    builder
        .set_request_type(RequestType::Checksum)
        .set_data(vec![0xaa, 0xbb]);
    let request = TransportRequest::new(builder.build().expect("metadata"));
    let ranges = vec![RequestKeyRange {
        start_key: vec![4],
        end_key: vec![5],
    }];

    assert!(matches!(
        request.encode_coprocessor_request(ranges.clone()),
        Err(TransportRequestError::Unbound)
    ));

    let bound = request
        .bind(TransportBinding::new())
        .expect("first transport owner");
    let decoded = CoprocessorRequest::decode(
        bound
            .encode_coprocessor_request(ranges)
            .expect("bound wire")
            .as_slice(),
    )
    .expect("decode bound wire");
    assert_eq!(decoded.tp, RequestType::Checksum as i64);
    assert_eq!(decoded.data, vec![0xaa, 0xbb]);
    assert_eq!(decoded.ranges.len(), 1);
}
