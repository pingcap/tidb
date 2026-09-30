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

use prost::Message;
use tidb_proto::{coprocessor, mpp};

#[test]
fn complete_coprocessor_requests_retain_batch_and_versioned_ranges() {
    // Request.tasks=11 (task_id=5), versioned_ranges=15 (read_ts=2).
    let wire = [0x5a, 2, 0x28, 7, 0x7a, 2, 0x10, 9];
    let decoded = coprocessor::Request::decode(wire.as_slice()).unwrap();
    assert_eq!(decoded.encode_to_vec(), wire);
    // BatchRequest.start_ts=5; the old local projection retained only regions.
    let wire = [0x28, 9];
    assert_eq!(
        coprocessor::BatchRequest::decode(wire.as_slice())
            .unwrap()
            .encode_to_vec(),
        wire
    );
}

#[test]
fn complete_mpp_dispatch_retains_partition_table_regions() {
    // DispatchTaskRequest.table_regions=6, physical_table_id=1.
    let wire = [0x32, 2, 0x08, 42];
    assert_eq!(
        mpp::DispatchTaskRequest::decode(wire.as_slice())
            .unwrap()
            .encode_to_vec(),
        wire
    );
}

#[test]
fn complete_coprocessor_response_retains_merged_task_marker() {
    // StoreBatchTaskResponse.data_merged_into_response=7.
    let wire = [0x38, 1];
    assert_eq!(
        coprocessor::StoreBatchTaskResponse::decode(wire.as_slice())
            .unwrap()
            .encode_to_vec(),
        wire
    );
}

#[test]
fn mpp_preserves_keyspace_presence_and_shares_the_native_type() {
    // Explicit zero is present in Go's keyspace oneof, unlike an absent ID.
    let wire = [0x50, 0];
    let meta: tikv_client_kvproto::mpp::TaskMeta = mpp::TaskMeta::decode(wire.as_slice()).unwrap();
    assert_eq!(meta.encode_to_vec(), wire);
    assert!(matches!(
        meta.keyspace,
        Some(mpp::task_meta::Keyspace::KeyspaceId(0))
    ));
    // Namespace 1 / keyspace 2 replaces the legacy ID, as the last oneof arm.
    let wire = [0x50, 0, 0xb2, 1, 4, 8, 1, 16, 2];
    let meta = mpp::TaskMeta::decode(wire.as_slice()).unwrap();
    assert_eq!(meta.encode_to_vec(), wire[2..]);
    let request: tikv_client_kvproto::coprocessor::Request = coprocessor::Request::default();
    assert!(request.tasks.is_empty());
}
