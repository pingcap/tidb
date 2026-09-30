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
            cmd: Some(batch_commands_request::request::Cmd::Coprocessor(
                vec![0x08, 0x01].into(),
            )),
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
            cmd: Some(batch_commands_response::response::Cmd::Empty(
                prost::bytes::Bytes::new(),
            )),
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

#[test]
fn batch_decode_shares_the_encoded_command_buffer() {
    let wire = prost::bytes::Bytes::from_static(&[0x0a, 0x05, 0xb2, 0x01, 0x02, 0x08, 0x01]);
    let decoded = BatchCommandsRequest::decode(wire.clone()).unwrap();
    let Some(batch_commands_request::request::Cmd::Coprocessor(body)) = &decoded.requests[0].cmd
    else {
        panic!("expected coprocessor command");
    };
    assert_eq!(body.as_ptr(), wire[5..].as_ptr());
    assert_eq!(body.as_ref(), &[0x08, 0x01]);
}

#[test]
fn complete_tikv_service_and_messages_preserve_the_upstream_contract() {
    use prost_types::{field_descriptor_proto::Type, FileDescriptorSet};

    let upstream = FileDescriptorSet::decode(tikv_client_kvproto::FILE_DESCRIPTOR_SET).unwrap();
    let transport = FileDescriptorSet::decode(tidb_proto::TIKV_TRANSPORT_DESCRIPTOR_SET).unwrap();
    let upstream = upstream
        .file
        .iter()
        .find(|file| file.package.as_deref() == Some("tikvpb"))
        .unwrap();
    assert_eq!(transport.file.len(), 1);
    let transport = &transport.file[0];
    assert_eq!(transport.service, upstream.service);
    assert_eq!(transport.enum_type, upstream.enum_type);
    assert_eq!(transport.message_type.len(), upstream.message_type.len());
    for (actual, expected) in transport.message_type.iter().zip(&upstream.message_type) {
        let mut normalized = actual.clone();
        if matches!(
            expected.name.as_deref(),
            Some("BatchCommandsRequest" | "BatchCommandsResponse")
        ) {
            assert_eq!(actual.nested_type.len(), expected.nested_type.len());
            for (nested, original) in normalized.nested_type.iter_mut().zip(&expected.nested_type) {
                assert_eq!(nested.field.len(), original.field.len());
                for (field, original) in nested.field.iter_mut().zip(&original.field) {
                    if original.oneof_index.is_some() {
                        assert_eq!(field.r#type, Some(Type::Bytes as i32));
                        assert_eq!(field.type_name, None);
                        field.r#type = original.r#type;
                        field.type_name = original.type_name.clone();
                    }
                }
            }
        }
        if expected.name.as_deref() == Some("BatchCommandsResponse") {
            let field = normalized
                .field
                .iter_mut()
                .find(|field| field.name.as_deref() == Some("health_feedback"))
                .unwrap();
            let original = expected
                .field
                .iter()
                .find(|field| field.name.as_deref() == Some("health_feedback"))
                .unwrap();
            assert_eq!(field.r#type, Some(Type::Bytes as i32));
            assert_eq!(field.type_name, None);
            assert_eq!(field.proto3_optional, Some(true));
            assert_eq!(field.oneof_index, Some(expected.oneof_decl.len() as i32));
            field.r#type = original.r#type;
            field.type_name = original.type_name.clone();
            field.oneof_index = original.oneof_index;
            field.proto3_optional = original.proto3_optional;
            assert_eq!(
                normalized.oneof_decl.pop().unwrap().name.as_deref(),
                Some("_health_feedback")
            );
        }
        assert_eq!(
            &normalized, expected,
            "complete message {:?}",
            expected.name
        );
    }
}
