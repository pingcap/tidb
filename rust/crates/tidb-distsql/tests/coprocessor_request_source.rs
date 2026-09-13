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
use tidb_proto::{CoprocessorRequest, KvrpcContext};

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
        .set_data(payload.clone())
        .set_allow_batch_task_data_merge(true)
        .set_execute_batch_tasks_serially(true);
    let metadata = builder.build().expect("metadata");

    let envelope = CoprocessorRequestEnvelope::from_metadata(
        &metadata,
        vec![RequestKeyRange {
            start_key: vec![1].into(),
            end_key: vec![2, 3].into(),
        }],
    )
    .with_context(KvrpcContext {
        region_id: 9,
        ..KvrpcContext::default()
    })
    .with_paging_size(7)
    .with_cache_version(17)
    .with_max_keys_read(257);

    let encoded = envelope.encode_to_vec();
    let expected = vec![
        0x0a, 0x02, 0x08, 0x09, // context.region_id = 9
        0x10, 0x67, // tp = 2
        0x1a, 0x03, 0xde, 0xad, 0x00, // data = 3
        0x22, 0x07, 0x0a, 0x01, 0x01, 0x12, 0x02, 0x02, 0x03, // ranges = 4
        0x28, 0x01, // is_cache_enabled = 5
        0x30, 0x11, // cache_if_match_version = 6
        0x38, 0x2a, // start_ts = 7
        0x50, 0x07, // paging_size = 10
        0x80, 0x01, 0x81, 0x02, // max_keys_read = 16, 257
        0x90, 0x01, 0x01, // allow_batch_task_data_merge = 18
        0x98, 0x01, 0x01, // execute_batch_tasks_serially = 19
    ];
    assert_eq!(encoded, expected);

    let decoded = CoprocessorRequest::decode(encoded.as_slice()).expect("coprocessor wire");
    assert_eq!(decoded.context.unwrap().region_id, 9);
    assert_eq!(decoded.tp, RequestType::Dag.raw());
    assert_eq!(decoded.data, payload);
    assert_eq!(decoded.ranges[0].start, vec![1]);
    assert_eq!(decoded.ranges[0].end, vec![2, 3]);
    assert_eq!(decoded.start_ts, 42);
    assert_eq!(decoded.paging_size, 7);
    assert_eq!(decoded.max_keys_read, 257);
    assert!(decoded.allow_batch_task_data_merge);
    assert!(decoded.execute_batch_tasks_serially);
    assert!(encoded.windows(2).any(|window| window == [0x90, 0x01]));
    assert!(encoded.windows(2).any(|window| window == [0x98, 0x01]));
    // The source leaves field 11/14/15 for transport-owned task metadata;
    // this projection does not fabricate those messages.
}

#[test]
fn coprocessor_request_encoding_matches_the_derived_message_for_every_range_shape() {
    // The envelope writes its bytes without materialising a
    // `coprocessor.Request`; an unbounded range's empty boundary and a
    // populated context must encode exactly as `prost` derives them.
    let mut builder = KvRequestBuilder::new();
    builder
        .set_request_type(RequestType::Dag)
        .set_start_ts(7)
        .set_data(vec![9; 40]);
    let metadata = builder.build().expect("metadata");
    let ranges = vec![
        RequestKeyRange {
            start_key: vec![1, 2].into(),
            end_key: vec![].into(),
        },
        RequestKeyRange {
            start_key: vec![].into(),
            end_key: vec![3].into(),
        },
        RequestKeyRange {
            start_key: vec![0x80; 300].into(),
            end_key: vec![0x81; 300].into(),
        },
    ];
    let envelope = CoprocessorRequestEnvelope::from_metadata(&metadata, ranges.clone())
        .with_context(KvrpcContext {
            region_id: 300,
            ..KvrpcContext::default()
        })
        .with_paging_size(128);
    let derived = CoprocessorRequest {
        context: Some(KvrpcContext {
            region_id: 300,
            ..KvrpcContext::default()
        }),
        tp: RequestType::Dag.raw(),
        data: vec![9; 40],
        ranges: ranges
            .iter()
            .map(|range| tidb_proto::CoprocessorKeyRange {
                start: range.start_key.to_vec(),
                end: range.end_key.to_vec(),
            })
            .collect(),
        start_ts: 7,
        paging_size: 128,
        ..CoprocessorRequest::default()
    }
    .encode_to_vec();
    assert_eq!(envelope.encode_to_vec(), derived);
}

#[test]
fn transport_request_rejects_unbound_serialization_and_allows_bound_snapshot() {
    let mut builder = KvRequestBuilder::new();
    builder
        .set_request_type(RequestType::Checksum)
        .set_data(vec![0xaa, 0xbb]);
    let request = TransportRequest::new(
        builder.build().expect("metadata"),
        std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
    );
    let ranges = vec![RequestKeyRange {
        start_key: vec![4].into(),
        end_key: vec![5].into(),
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
    assert_eq!(decoded.tp, RequestType::Checksum.raw());
    assert_eq!(decoded.data, vec![0xaa, 0xbb]);
    assert_eq!(decoded.ranges.len(), 1);
}
