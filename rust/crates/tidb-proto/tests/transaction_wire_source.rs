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

//! Wire compatibility checks for transactional BatchCommands messages.

use prost::Message;
use tidb_proto::errorpb::Error as RegionError;
use tidb_proto::kvrpcpb::{
    AlreadyExist, Assertion, AssertionFailed, AssertionLevel, BatchRollbackRequest,
    BatchRollbackResponse, CommitRequest, CommitResponse, CommitRole, CommitTsExpired,
    CommitTsTooLarge, GetRequest, GetResponse, KeyError, Mutation, Op, PrewriteRequest,
    PrewriteResponse, WriteConflict,
};
use tidb_proto::tikvpb::{batch_commands_request, batch_commands_response};
use tidb_proto::tikvpb::{BatchCommandsRequest, BatchCommandsResponse};

#[test]
fn transaction_requests_match_pinned_kvproto_wire() {
    let get = GetRequest {
        key: b"k".to_vec(),
        version: 7,
        need_commit_ts: true,
        ..GetRequest::default()
    };
    assert_eq!(
        get.encode_to_vec(),
        [0x12, 0x01, b'k', 0x18, 0x07, 0x20, 0x01]
    );

    let mutation = Mutation {
        op: Op::Insert as i32,
        key: b"k".to_vec(),
        value: b"v".to_vec(),
        assertion: Assertion::NotExist as i32,
    };
    let mutation_wire = [0x08, 0x04, 0x12, 0x01, b'k', 0x1a, 0x01, b'v', 0x20, 0x02];
    assert_eq!(mutation.encode_to_vec(), mutation_wire);

    let prewrite = PrewriteRequest {
        mutations: vec![mutation],
        primary_lock: b"k".to_vec(),
        start_version: 11,
        lock_ttl: 3_000,
        txn_size: 1,
        assertion_level: AssertionLevel::Strict as i32,
        ..PrewriteRequest::default()
    };
    let mut prewrite_wire = vec![0x12, mutation_wire.len() as u8];
    prewrite_wire.extend_from_slice(&mutation_wire);
    prewrite_wire.extend_from_slice(&[
        0x1a, 0x01, b'k', 0x20, 0x0b, 0x28, 0xb8, 0x17, 0x40, 0x01, 0x78, 0x02,
    ]);
    assert_eq!(prewrite.encode_to_vec(), prewrite_wire);

    let commit = CommitRequest {
        start_version: 11,
        keys: vec![b"k".to_vec()],
        commit_version: 12,
        commit_role: CommitRole::Primary as i32,
        primary_key: b"k".to_vec(),
        ..CommitRequest::default()
    };
    assert_eq!(
        commit.encode_to_vec(),
        [0x10, 0x0b, 0x1a, 0x01, b'k', 0x20, 0x0c, 0x30, 0x01, 0x3a, 0x01, b'k',]
    );

    let rollback = BatchRollbackRequest {
        start_version: 13,
        keys: vec![b"k".to_vec()],
        ..BatchRollbackRequest::default()
    };
    assert_eq!(rollback.encode_to_vec(), [0x10, 0x0d, 0x1a, 0x01, b'k']);
}

#[test]
fn transaction_responses_preserve_region_and_key_error_presence() {
    let get = GetResponse {
        region_error: Some(RegionError {
            message: "r".to_owned(),
            ..RegionError::default()
        }),
        not_found: true,
        commit_ts: 17,
        ..GetResponse::default()
    };
    assert_eq!(
        get.encode_to_vec(),
        [0x0a, 0x03, 0x0a, 0x01, b'r', 0x20, 0x01, 0x38, 0x11]
    );

    let prewrite = PrewriteResponse {
        errors: vec![KeyError {
            retryable: "x".to_owned(),
            ..KeyError::default()
        }],
        ..PrewriteResponse::default()
    };
    assert_eq!(prewrite.encode_to_vec(), [0x12, 0x03, 0x12, 0x01, b'x']);

    let commit = CommitResponse {
        error: Some(KeyError {
            abort: "a".to_owned(),
            ..KeyError::default()
        }),
        commit_version: 17,
        ..CommitResponse::default()
    };
    assert_eq!(
        CommitResponse::decode(commit.encode_to_vec().as_slice()).unwrap(),
        commit
    );

    let rollback = BatchRollbackResponse::default();
    assert!(rollback.encode_to_vec().is_empty());

    let classified = KeyError {
        conflict: Some(WriteConflict {
            start_ts: 1,
            conflict_ts: 2,
            key: b"k".to_vec(),
            primary: b"p".to_vec(),
            conflict_commit_ts: 3,
            reason: tidb_proto::kvrpcpb::write_conflict::Reason::Optimistic as i32,
        }),
        already_exist: Some(AlreadyExist { key: b"e".to_vec() }),
        commit_ts_expired: Some(CommitTsExpired {
            start_ts: 4,
            attempted_commit_ts: 5,
            key: b"x".to_vec(),
            min_commit_ts: 6,
        }),
        commit_ts_too_large: Some(CommitTsTooLarge { commit_ts: 7 }),
        assertion_failed: Some(AssertionFailed {
            start_ts: 8,
            key: b"a".to_vec(),
            assertion: Assertion::Exist as i32,
            existing_start_ts: 9,
            existing_commit_ts: 10,
        }),
        ..KeyError::default()
    };
    assert_eq!(
        classified.encode_to_vec(),
        [
            0x22, 0x0e, 0x08, 0x01, 0x10, 0x02, 0x1a, 0x01, b'k', 0x22, 0x01, b'p', 0x28, 0x03,
            0x30, 0x01, 0x2a, 0x03, 0x0a, 0x01, b'e', 0x3a, 0x09, 0x08, 0x04, 0x10, 0x05, 0x1a,
            0x01, b'x', 0x20, 0x06, 0x4a, 0x02, 0x08, 0x07, 0x52, 0x0b, 0x08, 0x08, 0x12, 0x01,
            b'a', 0x18, 0x01, 0x20, 0x09, 0x28, 0x0a,
        ]
    );
}

#[test]
fn transaction_messages_keep_exact_batchcommands_tags() {
    let bodies = [
        (1_u32, GetRequest::default().encode_to_vec()),
        (3, PrewriteRequest::default().encode_to_vec()),
        (4, CommitRequest::default().encode_to_vec()),
        (8, BatchRollbackRequest::default().encode_to_vec()),
    ];
    let request = BatchCommandsRequest {
        requests: vec![
            batch_commands_request::Request {
                cmd: Some(batch_commands_request::request::Cmd::Get(
                    bodies[0].1.clone(),
                )),
            },
            batch_commands_request::Request {
                cmd: Some(batch_commands_request::request::Cmd::Prewrite(
                    bodies[1].1.clone(),
                )),
            },
            batch_commands_request::Request {
                cmd: Some(batch_commands_request::request::Cmd::Commit(
                    bodies[2].1.clone(),
                )),
            },
            batch_commands_request::Request {
                cmd: Some(batch_commands_request::request::Cmd::BatchRollback(
                    bodies[3].1.clone(),
                )),
            },
        ],
        request_ids: vec![11, 13, 17, 19],
        ..BatchCommandsRequest::default()
    };
    let encoded = request.encode_to_vec();
    for (field, _) in bodies {
        assert!(encoded.contains(&((field << 3 | 2) as u8)));
    }

    let response = BatchCommandsResponse {
        responses: vec![batch_commands_response::Response {
            cmd: Some(batch_commands_response::response::Cmd::Commit(
                CommitResponse::default().encode_to_vec(),
            )),
        }],
        request_ids: vec![17],
        ..BatchCommandsResponse::default()
    };
    assert_eq!(
        BatchCommandsResponse::decode(response.encode_to_vec().as_slice()).unwrap(),
        response
    );
}
