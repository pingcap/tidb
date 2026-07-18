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

//! Source-derived checks for streamed coprocessor response ownership.
//!
//! These tests stop at the protobuf envelope.  They do not decode the nested
//! default/chunk/CHBlock payload, attach MPP semantics, or synthesize an
//! intermediate output channel.

use prost::Message;
use tidb_distsql::{decode_stream_response, RawStreamResponse};
use tidb_proto::{Error, StreamResponse};

#[test]
fn stream_response_preserves_payload_and_all_metadata() {
    let response = StreamResponse {
        error: Some(Error {
            code: Some(1105),
            msg: Some("cop stream error".to_owned()),
        }),
        data: Some(b"serialized-chunk".to_vec()),
        warnings: vec![
            Error {
                code: Some(1265),
                msg: Some("truncated".to_owned()),
            },
            Error {
                code: Some(1287),
                msg: Some("deprecated".to_owned()),
            },
        ],
        output_counts: vec![3, 5, -1],
        warning_count: Some(2),
        ndvs: vec![41, 43, -2],
    };

    let decoded = decode_stream_response(&response.encode_to_vec()).expect("valid stream wire");
    assert_eq!(decoded.error, response.error);
    assert_eq!(decoded.data(), Some(b"serialized-chunk".as_slice()));
    assert_eq!(decoded.warnings, response.warnings);
    assert_eq!(decoded.output_counts, response.output_counts);
    assert_eq!(decoded.warning_count, response.warning_count);
    assert_eq!(decoded.ndvs, response.ndvs);
    assert_eq!(decoded.clone().into_proto(), response);
}

#[test]
fn stream_response_preserves_absent_vs_present_empty_data() {
    let absent = decode_stream_response(&StreamResponse::default().encode_to_vec())
        .expect("empty protobuf is valid");
    assert_eq!(absent, RawStreamResponse::default());
    assert_eq!(absent.data(), None);

    let present_empty = StreamResponse {
        data: Some(Vec::new()),
        ..Default::default()
    };
    let encoded = present_empty.encode_to_vec();
    assert_eq!(encoded, vec![0x1a, 0x00]); // field 3, length-delimited
    let decoded = decode_stream_response(&encoded).expect("present empty data is valid");
    assert_eq!(decoded.data, Some(Vec::new()));
    assert_ne!(decoded, absent);
}

#[test]
fn stream_response_rejects_invalid_protobuf_without_guessing_payload() {
    // Field 3 declares two bytes but only one is present.  The decoder must
    // reject the envelope instead of returning partial data or interpreting
    // the byte as a typed row.
    let malformed = [0x1a, 0x02, 0x01];
    assert!(decode_stream_response(&malformed).is_err());
}
