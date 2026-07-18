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

//! Direct transit of pinned BatchCommands conversion and encoded-body tests.

use std::sync::Arc;
use std::thread;

use tidb_txnkv::rpc::batch::{
    BatchCommandTag, BatchEnvelopeKind, BatchWireError, BatchWireRequest, BatchWireResponse,
    OpaqueBatchCommand,
};

// client-go/tikvrpc/tikvrpc_test.go:177 TestTiDB51921 and
// client-go/tikvrpc/tikvrpc.go:644-713,768-840.
#[test]
fn every_pinned_command_tag_round_trips_as_opaque_bytes() {
    let fields: Vec<u32> = BatchCommandTag::ALL
        .iter()
        .map(|tag| tag.field_number())
        .collect();
    assert_eq!(
        fields,
        vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 33, 34, 35, 36, 37, 38, 39, 40, 255,
        ]
    );

    for (index, tag) in BatchCommandTag::ALL.iter().copied().enumerate() {
        let body = vec![0x08, index as u8];
        let request = BatchWireRequest::new(
            vec![OpaqueBatchCommand::new(tag, body.clone())],
            vec![index as u64 + 1],
            17,
        )
        .unwrap();
        let decoded = BatchWireRequest::decode(&request.encode_to_vec()).unwrap();
        assert_eq!(decoded, request);
        assert_eq!(decoded.commands()[0].body(), body);

        let response = BatchWireResponse::new(
            vec![OpaqueBatchCommand::new(tag, body)],
            vec![index as u64 + 1],
            280,
            Some(vec![0x08, 0x03]),
            19,
        )
        .unwrap();
        assert_eq!(
            BatchWireResponse::decode(&response.encode_to_vec()).unwrap(),
            response
        );
    }
}

// client-go/internal/client/client_batch_test.go:27-220 encoded command,
// reuse, double-return, and concurrent-pool tests. Rust ownership removes the
// released-buffer state while preserving exact repeated wire bytes.
#[test]
fn encoded_body_remains_owned_after_repeated_and_concurrent_encoding() {
    let request = Arc::new(
        BatchWireRequest::new(
            vec![OpaqueBatchCommand::new(
                BatchCommandTag::Get,
                b"pre-encoded-get".to_vec(),
            )],
            vec![1],
            23,
        )
        .unwrap(),
    );
    let expected = request.encode_to_vec();
    assert_eq!(request.encode_to_vec(), expected);
    assert_eq!(request.commands()[0].body(), b"pre-encoded-get");

    let threads: Vec<_> = (0..12)
        .map(|_| {
            let request = Arc::clone(&request);
            let expected = expected.clone();
            thread::spawn(move || {
                for _ in 0..100 {
                    assert_eq!(request.encode_to_vec(), expected);
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(request.commands()[0].body(), b"pre-encoded-get");
}

// A malformed response must not reach client_batch.go:1295-1298's indexed
// response delivery; the Rust boundary rejects it before publication.
#[test]
fn cardinality_is_rejected_before_any_dispatch_state_exists() {
    assert_eq!(
        BatchWireRequest::new(
            vec![OpaqueBatchCommand::new(BatchCommandTag::Empty, Vec::new())],
            Vec::new(),
            0,
        ),
        Err(BatchWireError::Cardinality {
            kind: BatchEnvelopeKind::Request,
            commands: 1,
            request_ids: 0,
        })
    );
    assert_eq!(
        BatchWireResponse::new(Vec::new(), vec![9], 0, None, 0),
        Err(BatchWireError::Cardinality {
            kind: BatchEnvelopeKind::Response,
            commands: 0,
            request_ids: 1,
        })
    );
}

#[test]
fn zero_and_duplicate_request_ids_are_rejected_at_wire_construction() {
    let commands = || {
        vec![
            OpaqueBatchCommand::new(BatchCommandTag::Empty, Vec::new()),
            OpaqueBatchCommand::new(BatchCommandTag::Empty, Vec::new()),
        ]
    };

    assert_eq!(
        BatchWireRequest::new(commands(), vec![1, 0], 0),
        Err(BatchWireError::ZeroRequestId {
            kind: BatchEnvelopeKind::Request,
            index: 1,
        })
    );
    assert_eq!(
        BatchWireRequest::new(commands(), vec![7, 7], 0),
        Err(BatchWireError::DuplicateRequestId {
            kind: BatchEnvelopeKind::Request,
            request_id: 7,
        })
    );
    assert_eq!(
        BatchWireResponse::new(commands(), vec![0, 2], 0, None, 0),
        Err(BatchWireError::ZeroRequestId {
            kind: BatchEnvelopeKind::Response,
            index: 0,
        })
    );
    assert_eq!(
        BatchWireResponse::new(commands(), vec![8, 8], 0, None, 0),
        Err(BatchWireError::DuplicateRequestId {
            kind: BatchEnvelopeKind::Response,
            request_id: 8,
        })
    );
}

// client-go/tikvrpc/tikvrpc_test.go:55 TestBatchResponse.
#[test]
fn nil_command_response_is_a_typed_error() {
    // response[0] is a present message with no command; request_ids is packed.
    let bytes = [0x0a, 0x00, 0x12, 0x01, 0x07];
    assert_eq!(
        BatchWireResponse::decode(&bytes),
        Err(BatchWireError::MissingCommand {
            kind: BatchEnvelopeKind::Response,
            index: 0,
        })
    );
}

#[test]
fn response_metadata_preserves_presence_and_exact_values() {
    let response = BatchWireResponse::new(
        vec![OpaqueBatchCommand::new(
            BatchCommandTag::Coprocessor,
            vec![1, 2, 3],
        )],
        vec![42],
        280,
        Some(Vec::new()),
        123,
    )
    .unwrap();
    let decoded = BatchWireResponse::decode(&response.encode_to_vec()).unwrap();
    assert_eq!(decoded.request_ids(), &[42]);
    assert_eq!(decoded.transport_layer_load(), 280);
    assert_eq!(decoded.health_feedback(), Some(&[][..]));
    assert_eq!(decoded.tikv_send_time_ns(), 123);
}
