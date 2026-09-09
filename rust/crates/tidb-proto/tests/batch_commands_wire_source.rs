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

//! Exact pinned kvproto wire checks for opaque BatchCommands envelopes.

use prost::Message;
use tidb_proto::tikvpb::{
    batch_commands_request, batch_commands_response, BatchCommandsRequest, BatchCommandsResponse,
};

#[test]
fn coprocessor_request_body_keeps_pinned_tag_22() {
    let request = BatchCommandsRequest {
        requests: vec![batch_commands_request::Request {
            cmd: Some(batch_commands_request::request::Cmd::Coprocessor(vec![
                0x08, 0x01,
            ])),
        }],
        request_ids: vec![7],
        client_send_time_ns: 9,
    };

    assert_eq!(
        request.encode_to_vec(),
        vec![
            0x0a, 0x05, 0xb2, 0x01, 0x02, 0x08, 0x01, // request / command 22
            0x12, 0x01, 0x07, // packed request_ids
            0x18, 0x09, // client_send_time_ns
        ]
    );
    assert_eq!(
        BatchCommandsRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
}

#[test]
fn empty_response_and_feedback_presence_keep_pinned_fields() {
    let response = BatchCommandsResponse {
        responses: vec![batch_commands_response::Response {
            cmd: Some(batch_commands_response::response::Cmd::Empty(Vec::new())),
        }],
        request_ids: vec![9],
        transport_layer_load: 7,
        health_feedback: Some(Vec::new()),
        tikv_send_time_ns: 11,
    };

    assert_eq!(
        response.encode_to_vec(),
        vec![
            0x0a, 0x03, 0xfa, 0x0f, 0x00, // response / command 255
            0x12, 0x01, 0x09, // packed request_ids
            0x18, 0x07, // transport_layer_load
            0x22, 0x00, // present, empty health_feedback
            0x28, 0x0b, // tikv_send_time_ns
        ]
    );
    let decoded = BatchCommandsResponse::decode(response.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.health_feedback, Some(Vec::new()));
    assert_eq!(decoded, response);
}

#[test]
fn missing_command_presence_remains_observable() {
    let request = BatchCommandsRequest {
        requests: vec![batch_commands_request::Request { cmd: None }],
        request_ids: vec![1],
        client_send_time_ns: 0,
    };

    let decoded = BatchCommandsRequest::decode(request.encode_to_vec().as_slice()).unwrap();
    assert!(decoded.requests[0].cmd.is_none());
}
