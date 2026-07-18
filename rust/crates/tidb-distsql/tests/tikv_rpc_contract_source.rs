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

#![allow(missing_docs)]

use std::time::Duration;

use prost::Message;
use tidb_distsql::cop_paging::{
    build_tikv_unary_request, decode_tikv_unary_response, CopReadTaskRuntime,
};
use tidb_distsql::{
    CopPagingState, KvPriority, KvRequestMetadata, ReadEngineGeneration, RegionTaskEpoch,
    RegionTaskPeer, RegionTaskTopology, ReplicaReadType, RequestKeyRange, RequestKeyRanges,
    RequestSource, RequestType, StoreType,
};
use tidb_proto::{
    CoprocessorRequest, CoprocessorResponse, KvrpcContext, KvrpcLockInfo, KvrpcRequestOrigin,
    RegionError, StoreBatchTaskResponse,
};
use tidb_txnkv::{ClientReplicaReadType, EndpointType, TraceInfo};

fn range(start: &str, end: &str) -> RequestKeyRange {
    RequestKeyRange {
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
    }
}

fn metadata() -> KvRequestMetadata {
    let mut metadata = KvRequestMetadata {
        request_type: RequestType::Dag,
        data: Some(b"dag".to_vec()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![range("a", "z")])),
        keep_order: true,
        cacheable: true,
        store_type: StoreType::TiKv,
        start_ts: 100,
        schema_version: 7,
        connection_id: 9,
        connection_alias: "session".to_owned(),
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        is_staleness: true,
        ..KvRequestMetadata::default()
    };
    metadata.session.paging.enabled = true;
    metadata.session.paging.min_size = 2;
    metadata.session.paging.max_size = 8;
    metadata.session.replica_read = ReplicaReadType::Follower;
    metadata.session.priority = KvPriority::High;
    metadata.session.not_fill_cache = true;
    metadata.session.task_id = 42;
    metadata.session.resource_group_name = "rg1".to_owned();
    metadata.session.store_busy_threshold_ms = u64::from(u32::MAX) + 2;
    metadata.session.tikv_client_read_timeout_ms = 777;
    metadata.session.request_source = RequestSource {
        internal: true,
        source_type: "stats".to_owned(),
        explicit_source_type: "analyze".to_owned(),
    };
    metadata
}

fn topology() -> RegionTaskTopology {
    RegionTaskTopology {
        region_id: 8,
        region_epoch: Some(RegionTaskEpoch {
            conf_ver: 2,
            version: 3,
        }),
        peer: Some(RegionTaskPeer {
            id: 11,
            store_id: 12,
            role: 1,
            is_witness: false,
        }),
        start_key: b"a".to_vec(),
        end_key: b"z".to_vec(),
        buckets_version: 13,
        ..RegionTaskTopology::default()
    }
}

fn runtime(metadata: &KvRequestMetadata) -> CopReadTaskRuntime {
    CopPagingState::prepare_read_tasks(
        metadata,
        &[topology()],
        None,
        ReadEngineGeneration::Classic,
        4096,
    )
    .unwrap()
}

#[test]
fn checked_task_becomes_wire_ready_request_with_exact_context_and_wrapper_metadata() {
    let metadata = metadata();
    let runtime = runtime(&metadata);
    let prepared = runtime.prepared_attempt(1).unwrap();
    let request = build_tikv_unary_request(
        prepared,
        &metadata,
        runtime.predicted_read_bytes(),
        Some(&TraceInfo {
            connection_id: 123,
            session_alias: "trace-alias".to_owned(),
        }),
    );

    assert_eq!(request.endpoint, EndpointType::TiKv);
    assert_eq!(request.replica_read_type, ClientReplicaReadType::Mixed);
    assert!(!request.replica_read);
    assert!(request.stale_read);
    assert_eq!(request.input_request_source, "internal_stats_analyze");
    assert_eq!(request.predicted_read_bytes, 4096);
    assert_eq!(request.read_replica_scope, "global");
    assert_eq!(request.txn_scope, "global");
    assert_eq!(request.timeout_override_ms, Some(777));

    let wire = CoprocessorRequest::decode(request.encoded_request.as_slice()).unwrap();
    assert_eq!(wire.tp, RequestType::Dag as i64);
    assert_eq!(wire.data, b"dag");
    assert_eq!(wire.start_ts, 100);
    assert_eq!(wire.schema_ver, 7);
    assert_eq!(wire.paging_size, 2);
    assert_eq!(wire.connection_id, 9);
    assert_eq!(wire.connection_alias, "session");
    assert_eq!(wire.ranges.len(), 1);

    let context = KvrpcContext::decode(wire.context.unwrap().as_slice()).unwrap();
    assert_eq!(context, request.context);
    assert_eq!(context.region_id, 8);
    assert_eq!(context.region_epoch.unwrap().version, 3);
    assert_eq!(context.peer.unwrap().store_id, 12);
    assert_eq!(context.priority, KvPriority::High as i32);
    assert!(context.not_fill_cache);
    assert!(context.record_time_stat);
    assert!(context.record_scan_stat);
    assert!(!context.replica_read);
    assert!(context.stale_read);
    assert_eq!(context.task_id, 42);
    assert_eq!(context.busy_threshold_ms, 1);
    assert_eq!(context.buckets_version, 13);
    assert_eq!(context.request_source, "internal_stats_analyze");
    assert_eq!(context.request_origin, KvrpcRequestOrigin::TiDb as i32);
    assert_eq!(
        context
            .resource_control_context
            .unwrap()
            .resource_group_name,
        "rg1"
    );
    let source_stmt = context.source_stmt.unwrap();
    assert_eq!(source_stmt.connection_id, 123);
    assert_eq!(source_stmt.session_alias, "trace-alias");
}

fn assert_response_error(response: CoprocessorResponse, expected: &str) {
    let metadata = metadata();
    let mut runtime = runtime(&metadata);
    let decoded = decode_tikv_unary_response(response.encode_to_vec().as_slice()).unwrap();
    let error = runtime
        .accept_response(1, decoded, None, Duration::from_secs(1))
        .unwrap_err();
    assert_eq!(error.kind(), expected);
    assert_eq!(runtime.in_flight_attempt_ids(), [1]);
}

#[test]
fn raw_response_decode_classifies_errors_before_coordinator_mutation() {
    assert_response_error(
        CoprocessorResponse {
            region_error: Some(RegionError {
                message: "not leader".to_owned(),
                ..RegionError::default()
            }),
            ..CoprocessorResponse::default()
        },
        "region_error",
    );
    assert_response_error(
        CoprocessorResponse {
            locked: Some(KvrpcLockInfo {
                key: b"locked".to_vec(),
                ..KvrpcLockInfo::default()
            }),
            ..CoprocessorResponse::default()
        },
        "lock_error",
    );
    assert_response_error(
        CoprocessorResponse {
            other_error: "boom".to_owned(),
            ..CoprocessorResponse::default()
        },
        "other_error",
    );
    assert_response_error(
        CoprocessorResponse {
            batch_responses: vec![StoreBatchTaskResponse::default()],
            ..CoprocessorResponse::default()
        },
        "batch_response",
    );
    assert_response_error(
        CoprocessorResponse {
            region_error: Some(RegionError {
                message: "top-level wins".to_owned(),
                ..RegionError::default()
            }),
            batch_responses: vec![StoreBatchTaskResponse::default()],
            ..CoprocessorResponse::default()
        },
        "region_error",
    );

    let metadata = metadata();
    let runtime = runtime(&metadata);
    assert!(decode_tikv_unary_response(&[0x0a, 0x02, 0x01]).is_err());
    assert_eq!(runtime.in_flight_attempt_ids(), [1]);
}

#[test]
fn decoded_success_is_accepted_only_after_the_full_message_exists() {
    let metadata = metadata();
    let mut runtime = runtime(&metadata);
    let decoded = decode_tikv_unary_response(
        CoprocessorResponse {
            data: b"rows".to_vec(),
            ..CoprocessorResponse::default()
        }
        .encode_to_vec()
        .as_slice(),
    )
    .unwrap();
    runtime
        .accept_response(1, decoded, None, Duration::from_secs(1))
        .unwrap();
    assert!(runtime.in_flight_attempt_ids().is_empty());
}
