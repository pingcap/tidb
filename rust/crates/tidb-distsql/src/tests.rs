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

use std::sync::{atomic::AtomicU64, Arc};

use super::*;

#[test]
fn test_context_detach_preserves_handles_and_copies_owned_state() {
    let killer = Arc::new(KillHandle::default());
    let cancel = Arc::new(CancelHandle::default());
    let mut execution = ExecutionState::with_handles(Arc::clone(&killer), Arc::clone(&cancel));
    execution.cpu_usage = CpuUsage::from_samples(vec![3, 5, 8]);
    execution.kv_vars.backoff_lock_fast = 11;
    execution.kv_vars.backoff_weight = 13;
    execution.max_keys_read_counter = Some(Arc::new(AtomicU64::new(7)));

    let warning_handler = WarningCollector::new();
    let context = DistSqlContext {
        request: RequestContext {
            warning_handler: warning_handler.clone(),
            original_sql: "select 1".to_owned(),
            enable_chunk_rpc: true,
            session: SessionContext {
                connection_id: 42,
                alias: "reader".to_owned(),
            },
            replica_read: ReplicaReadType::Follower,
            priority: Priority::High,
            paging: PagingConfig {
                enabled: true,
                min_size: 4,
                max_size: 64,
                size_bytes: 4096,
            },
            max_execution_time_ms: 250,
            max_keys_read: 100,
            ..RequestContext::default()
        },
        execution,
    };
    context.append_warning("source warning");

    let mut detached = context.detach();
    assert!(detached.execution.detached);
    assert!(context
        .request
        .warning_handler
        .shares_handler_with(&detached.request.warning_handler));
    assert!(Arc::ptr_eq(
        &context.execution.killer,
        &detached.execution.killer
    ));
    assert!(Arc::ptr_eq(
        &context.execution.cancel,
        &detached.execution.cancel
    ));
    assert!(detached
        .execution
        .kv_vars
        .shares_killer_with(&context.execution.killer));

    assert_eq!(context.execution.cpu_usage.samples(), &[3, 5, 8]);
    detached.execution.cpu_usage.push_sample(13);
    assert_eq!(context.execution.cpu_usage.samples(), &[3, 5, 8]);
    assert_eq!(detached.execution.kv_vars.backoff_lock_fast, 11);
    assert_eq!(detached.execution.kv_vars.backoff_weight, 13);
    assert_eq!(context.execution.max_keys_read_count(), Some(7));
    assert_eq!(detached.execution.max_keys_read_count(), Some(0));
    assert!(!std::ptr::eq(
        context
            .execution
            .max_keys_read_counter
            .as_ref()
            .expect("source counter"),
        detached
            .execution
            .max_keys_read_counter
            .as_ref()
            .expect("detached counter")
    ));

    detached.request.original_sql = "select 2".to_owned();
    assert_eq!(context.request.original_sql, "select 1");
    assert!(context.request.enable_chunk_rpc);
    assert!(detached.request.enable_chunk_rpc);
    detached.request.session.alias = "background".to_owned();
    assert_eq!(context.request.session.alias, "reader");

    detached.append_note("detached note");
    assert_eq!(warning_handler.len(), 2);
    assert_eq!(context.warnings()[1].level, WarningLevel::Note);
}

#[test]
fn test_context_detach_preserves_source_routing_values() {
    assert!(!ReplicaReadType::Leader.is_follower_read());
    assert!(ReplicaReadType::PreferLeader.is_follower_read());
    assert!(ReplicaReadType::Closest.is_closest_read());
    assert!(!ReplicaReadType::ClosestAdaptive.is_closest_read());
    assert_eq!(ReplicaReadType::Learner.raw(), 5);
    assert_eq!(Priority::NoPriority as u8, 0);
    assert_eq!(Priority::Low as u8, 1);
    assert_eq!(Priority::High as u8, 2);
    assert_eq!(Priority::Delayed as u8, 3);
}

#[test]
fn test_context_detach_shared_kill_and_cancel_state_remains_observable() {
    let context = DistSqlContext::new();
    let detached = context.detach();

    assert!(context.execution.killer.request_kill(4));
    assert_eq!(detached.execution.killer.signal(), 4);
    detached.execution.cancel.cancel();
    assert!(context.execution.cancel.is_cancelled());
}

#[test]
fn test_cancel_handle_cancel_before_carrier_acquisition_shares_identity() {
    let cancel = CancelHandle::default();

    cancel.cancel();
    let first = cancel.unary_cancellation();
    let second = cancel.unary_cancellation();

    assert!(cancel.is_cancelled());
    assert!(first.is_cancelled());
    assert!(first.shares_state_with(&second));
}

#[test]
fn test_cancel_handle_cancel_after_carrier_acquisition_shares_identity() {
    let cancel = CancelHandle::default();
    let first = cancel.unary_cancellation();
    let second = cancel.unary_cancellation();

    assert!(first.shares_state_with(&second));
    assert!(!first.is_cancelled());
    cancel.cancel();
    assert!(cancel.is_cancelled());
    assert!(first.is_cancelled());
    assert!(second.is_cancelled());
}

#[test]
fn test_request_builder_maps_session_settings_without_transport() {
    let mut context = DistSqlContext::new();
    context.request.dist_sql_concurrency = 15;
    context.request.in_restricted_sql = true;
    context.request.original_sql = "select * from t".to_owned();
    context.request.replica_read = ReplicaReadType::Follower;
    context.request.task_id = 19;
    context.request.priority = Priority::Low;
    context.request.paging = PagingConfig {
        enabled: false,
        min_size: 128,
        max_size: 50_000,
        size_bytes: 4 * 1024 * 1024,
    };
    context.request.request_source_type = "internal".to_owned();
    context.request.explicit_request_source_type = "client".to_owned();
    context.request.store_batch_size = 32;
    context.request.resource_group_name = "olap".to_owned();
    context.request.load_based_replica_read_threshold_ms = 7;
    context.request.tikv_client_read_timeout_ms = 100;
    context.request.max_execution_time_ms = 250;
    context.request.max_keys_read = 1_000;
    context.execution.max_keys_read_counter = Some(Arc::new(AtomicU64::new(3)));

    let metadata = ReadRequestMetadata::from_context(&context);
    assert_eq!(metadata.concurrency, 15);
    assert_eq!(metadata.isolation_level, IsolationLevel::Snapshot);
    assert_eq!(metadata.priority, KvPriority::Low);
    assert_eq!(metadata.replica_read, ReplicaReadType::Follower);
    assert!(!metadata.paging.enabled);
    assert_eq!(metadata.paging.size_bytes, 4 * 1024 * 1024);
    assert_eq!(metadata.task_id, 19);
    assert!(metadata.request_source.internal);
    assert_eq!(metadata.request_source.source_type, "internal");
    assert_eq!(metadata.request_source.explicit_source_type, "client");
    assert_eq!(metadata.store_batch_size, 32);
    assert_eq!(metadata.resource_group_name, "olap");
    assert_eq!(metadata.store_busy_threshold_ms, 7);
    assert_eq!(metadata.tikv_client_read_timeout_ms, 100);
    assert_eq!(metadata.max_execution_time_ms, 250);
    assert_eq!(metadata.max_keys_read, 1_000);
    assert!(metadata.max_keys_read_counter.is_some());
}

#[test]
fn test_request_builder_preserves_go_priority_and_isolation_precedence() {
    for (priority, expected) in [
        (Priority::NoPriority, KvPriority::Normal),
        (Priority::Delayed, KvPriority::Normal),
        (Priority::Low, KvPriority::Low),
        (Priority::High, KvPriority::High),
    ] {
        let mut context = DistSqlContext::new();
        context.request.priority = priority;
        let metadata = ReadRequestMetadata::from_context(&context);
        assert_eq!(metadata.priority, expected);
    }

    let mut context = DistSqlContext::new();
    context.request.replica_read = ReplicaReadType::Follower;
    context.request.rc_check_ts = true;
    let metadata = ReadRequestMetadata::from_context(&context);
    assert_eq!(metadata.isolation_level, IsolationLevel::RcCheckTs);
    assert_eq!(metadata.replica_read, ReplicaReadType::Leader);

    context.request.weak_consistency = true;
    context.request.replica_read = ReplicaReadType::Follower;
    let metadata = ReadRequestMetadata::from_context(&context);
    assert_eq!(metadata.isolation_level, IsolationLevel::ReadCommitted);
    assert_eq!(metadata.replica_read, ReplicaReadType::Follower);
}

#[test]
fn test_request_builder_clamps_explicit_concurrency_and_shares_counter() {
    let mut context = DistSqlContext::new();
    context.request.dist_sql_concurrency = 15;
    context.execution.max_keys_read_counter = Some(Arc::new(AtomicU64::new(2)));

    let metadata = ReadRequestBuilder::new()
        .set_concurrency(50)
        .from_context(&context)
        .build();
    assert_eq!(metadata.concurrency, 15);

    let second = ReadRequestMetadata::from_context(&context);
    assert!(metadata.shares_max_keys_counter_with(&second));
}

#[test]
fn test_request_builder_scan_limit_concurrency_source_vectors() {
    let default_concurrency = 15;
    for (kind, limit, expected) in [
        (ExecutorKind::TableScan, 1, 1),
        (ExecutorKind::IndexScan, 1, 1),
        (ExecutorKind::TableScan, 1_000_000, default_concurrency),
        (ExecutorKind::IndexScan, 1_000_000, default_concurrency),
    ] {
        let mut envelope = RequestEnvelope::new(vec![
            ExecutorShape::new(kind),
            ExecutorShape::limit(limit, None),
        ]);
        envelope.concurrency = default_concurrency;
        assert_eq!(
            envelope.effective_concurrency(default_concurrency),
            expected
        );
        assert_eq!(envelope.limit_size(), Some(limit));
    }
}

#[test]
fn test_request_builder_index_lookup_pushdown_scan_concurrency_source_vectors() {
    let default_concurrency = 15;
    for (limit, expected) in [(1, 1), (1_000_000, default_concurrency)] {
        let mut envelope = RequestEnvelope::new(vec![
            ExecutorShape::new(ExecutorKind::IndexScan),
            ExecutorShape::limit(limit, Some(3)),
            ExecutorShape::new(ExecutorKind::TableScan),
            ExecutorShape::new(ExecutorKind::IndexLookup),
        ]);
        envelope.concurrency = default_concurrency;
        assert_eq!(
            envelope.effective_concurrency(default_concurrency),
            expected
        );
    }
}

#[test]
fn test_request_envelope_ordered_scan_and_partition_defaults() {
    let mut envelope = RequestEnvelope::new(vec![ExecutorShape::new(ExecutorKind::TableScan)]);
    envelope.keep_order = true;
    assert_eq!(envelope.effective_concurrency(15), 2);

    envelope.partition_count = 4;
    envelope.executors = vec![
        ExecutorShape::new(ExecutorKind::TableScan),
        ExecutorShape::limit(1, None),
    ];
    assert_eq!(envelope.effective_concurrency(15), 4);

    envelope.partition_count = 50;
    assert_eq!(envelope.effective_concurrency(15), 15);
}

#[test]
fn test_kv_request_build_initializes_defaults_and_is_one_use() {
    let mut builder = KvRequestBuilder::new();
    let request = builder.build().expect("first build");

    assert_eq!(request.request_type, RequestType::Unknown);
    assert_eq!(request.read_replica_scope, GLOBAL_REPLICA_SCOPE);
    // Go RequestBuilder.Build leaves concurrency unset. Session projection or
    // DAG policy owns any non-zero value; Build must not invent a default.
    assert_eq!(request.concurrency, 0);
    assert_eq!(
        request
            .key_ranges
            .as_ref()
            .map(RequestKeyRanges::partition_count),
        Some(1)
    );
    assert!(request
        .key_ranges
        .as_ref()
        .is_some_and(RequestKeyRanges::is_non_partitioned));
    assert_eq!(request.match_store_labels, Vec::new());
    assert!(matches!(
        builder.build(),
        Err(KvRequestBuildError::AlreadyBuilt)
    ));

    let mut context = DistSqlContext::new();
    context.request.resource_group_name = "test".to_owned();
    let request = KvRequestBuilder::from_context(&context)
        .build()
        .expect("context build");
    assert_eq!(request.resource_group_name, "test");
}

#[test]
fn test_kv_request_build_closest_scope_and_range_metadata() {
    let mut builder = KvRequestBuilder::new();
    builder
        .set_replica_read(ReplicaReadType::Closest)
        .set_read_replica_scope("az-a")
        .set_key_ranges(RequestKeyRanges::new_partitioned(vec![vec![
            RequestKeyRange {
                start_key: vec![1].into(),
                end_key: vec![2].into(),
            },
        ]]));

    let request = builder.build().expect("closest build");
    assert_eq!(request.read_replica_scope, "az-a");
    assert_eq!(
        request.match_store_labels,
        vec![StoreLabel {
            key: DC_LABEL_KEY.to_owned(),
            value: "az-a".to_owned(),
        }]
    );
    assert_eq!(request.key_ranges.as_ref().unwrap().partition_count(), 1);
    assert!(!request.key_ranges.as_ref().unwrap().is_non_partitioned());
}

#[test]
fn test_kv_request_build_projects_dag_limit_and_partition_concurrency() {
    let mut builder = KvRequestBuilder::new();
    let mut envelope = RequestEnvelope::new(vec![
        ExecutorShape::new(ExecutorKind::TableScan),
        ExecutorShape::limit(1, None),
    ]);
    envelope.partition_count = 3;
    builder.set_dag_envelope(envelope);

    let request = builder.build().expect("DAG build");
    assert_eq!(request.request_type, RequestType::Dag);
    assert!(request.cacheable);
    assert_eq!(request.limit_size, 1);
    assert_eq!(request.concurrency, 3);
}
