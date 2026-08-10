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

//! Direct structural translations of the complete original
//! `pkg/distsql/request_builder_test.go` obligation inventory.

use prost::Message;
use tidb_codec::{encode_int, encode_key, encode_row_key};
use tidb_datatype::Datum;
use tidb_distsql::{
    build_table_ranges, index_ranges_to_kv_ranges, table_handles_to_kv_ranges,
    table_ranges_to_kv_ranges, DatumRange, DistSqlContext, ExecutorKind, ExecutorShape,
    IsolationLevel, KvPriority, KvRequestMetadata, PagingConfig, ReplicaReadType, RequestBuilder,
    RequestEnvelope, RequestKeyRange, RequestSource, RequestType, StoreLabel, StoreType,
    TableRangeSpec, DC_LABEL_KEY, DEFAULT_DIST_SQL_CONCURRENCY, GLOBAL_REPLICA_SCOPE,
};
use tidb_proto::ResourceGroupTag;
use tidb_txnkv::{Handle, IntHandle, Key, ResourceGroupTagBuilder};

const DAG_BYTES: &[u8] = &[0x18, 0, 0x20, 0, 0x40, 0, 0x5a, 0];

fn source_ranges() -> Vec<DatumRange> {
    vec![
        DatumRange::inclusive(vec![Datum::Int(1)], vec![Datum::Int(2)]),
        DatumRange {
            low: vec![Datum::Int(2)],
            high: vec![Datum::Int(4)],
            low_exclude: true,
            high_exclude: true,
        },
        DatumRange {
            low: vec![Datum::Int(4)],
            high: vec![Datum::Int(19)],
            low_exclude: false,
            high_exclude: true,
        },
        DatumRange {
            low: vec![Datum::Int(19)],
            high: vec![Datum::Int(32)],
            low_exclude: true,
            high_exclude: false,
        },
        DatumRange {
            low: vec![Datum::Int(34)],
            high: vec![Datum::Int(34)],
            low_exclude: true,
            high_exclude: false,
        },
    ]
}

fn encoded_int(value: i64) -> Vec<u8> {
    let mut out = Vec::new();
    encode_int(&mut out, value);
    out
}

#[test]
fn table_and_index_ranges_match_source_exclusion_vectors() {
    // TestTableRangesToKVRanges and TestIndexRangesToKVRanges use the same
    // codec path; assert every adjusted boundary.
    let table = table_ranges_to_kv_ranges(13, &source_ranges()).expect("table ranges");
    let expected = [(1, 3), (3, 4), (4, 19), (20, 33), (35, 35)];
    for (range, (low, high)) in table.iter().zip(expected) {
        assert_eq!(range.start_key, encode_row_key(13, &encoded_int(low)));
        assert_eq!(range.end_key, encode_row_key(13, &encoded_int(high)));
    }

    let index = index_ranges_to_kv_ranges(&[12], 15, &source_ranges())
        .expect("index ranges")
        .remove(0);
    for (range, (low, high)) in index.iter().zip(expected) {
        let low = encode_key(&[Datum::Int(low)]).expect("low");
        let high = encode_key(&[Datum::Int(high)]).expect("high");
        assert!(range.start_key.ends_with(&low));
        assert!(range.end_key.ends_with(&high));
    }
}

// Go pkg/distsql/request_builder_test.go::TestTableRangesToKVRangesWithFbs.
#[test]
fn test_table_ranges_to_kv_ranges_with_fbs() {
    let fbs = DatumRange::inclusive(vec![Datum::Int(1)], vec![Datum::Int(4)]);
    let table = table_ranges_to_kv_ranges(0, std::slice::from_ref(&fbs)).expect("table FBS");
    assert_eq!(
        table,
        [RequestKeyRange {
            start_key: encode_row_key(0, &encoded_int(1)).into(),
            end_key: encode_row_key(0, &encoded_int(5)).into(),
        }]
    );
}

// Go pkg/distsql/request_builder_test.go::TestIndexRangesToKVRangesWithFbs.
#[test]
fn test_index_ranges_to_kv_ranges_with_fbs() {
    let fbs = DatumRange::inclusive(vec![Datum::Int(1)], vec![Datum::Int(4)]);
    let index = index_ranges_to_kv_ranges(&[0], 0, &[fbs]).expect("index FBS");
    let low = encode_key(&[Datum::Int(1)]).expect("low");
    let high = encode_key(&[Datum::Int(5)]).expect("high");
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].len(), 1);
    assert!(index[0][0].start_key.ends_with(&low));
    assert!(index[0][0].end_key.ends_with(&high));
}

fn finish_default_dag_request(builder: &mut RequestBuilder) -> KvRequestMetadata {
    builder
        .set_dag_request(RequestEnvelope::new(Vec::new()), DAG_BYTES)
        .set_desc(false)
        .set_keep_order(false)
        .set_from_context(&DistSqlContext::new());
    let request = builder.build().expect("DAG request");
    assert_eq!(request.request_type, RequestType::Dag);
    assert_eq!(request.data.as_deref(), Some(DAG_BYTES));
    assert!(request.cacheable);
    assert!(!request.keep_order);
    assert!(!request.desc);
    assert_eq!(
        request.concurrency,
        isize::try_from(DEFAULT_DIST_SQL_CONCURRENCY).unwrap()
    );
    assert_eq!(request.read_replica_scope, GLOBAL_REPLICA_SCOPE);
    request
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilder1.
#[test]
fn test_request_builder_1() {
    let expected = table_ranges_to_kv_ranges(12, &source_ranges()).unwrap();
    let mut builder = RequestBuilder::new();
    builder.set_table_ranges(12, &source_ranges());
    let request = finish_default_dag_request(&mut builder);
    assert_eq!(
        request.key_ranges.as_ref().unwrap().partitions(),
        [expected]
    );
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilder2.
#[test]
fn test_request_builder_2() {
    let expected = index_ranges_to_kv_ranges(&[12], 15, &source_ranges()).unwrap();
    let mut builder = RequestBuilder::new();
    builder.set_index_ranges(12, 15, &source_ranges());
    let request = finish_default_dag_request(&mut builder);
    assert_eq!(
        request.key_ranges.as_ref().unwrap().partitions(),
        expected
    );
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilder3.
#[test]
fn test_request_builder_3() {
    let handles = [0, 2, 3, 4, 5, 10, 11, 100].map(|value| Handle::Int(IntHandle::new(value)));
    let (expected_ranges, expected_hints) = table_handles_to_kv_ranges(15, &handles);
    let mut builder = RequestBuilder::new();
    builder.set_table_handles(15, &handles);
    let request = finish_default_dag_request(&mut builder);
    let ranges = request.key_ranges.as_ref().unwrap();
    assert_eq!(ranges.partitions(), [expected_ranges]);
    assert_eq!(ranges.row_count_hints(), [expected_hints]);
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilder4.
#[test]
fn test_request_builder_4() {
    let handles = [0, 2, 3, 4, 5, 10, 11, 100].map(|value| Handle::Int(IntHandle::new(value)));
    let (raw, _) = table_handles_to_kv_ranges(15, &handles);
    let mut builder = RequestBuilder::new();
    builder.set_non_partitioned_key_ranges(raw.clone());
    let request = finish_default_dag_request(&mut builder);
    let ranges = request.key_ranges.as_ref().unwrap();
    assert_eq!(ranges.partitions(), [raw]);
    assert!(ranges.row_count_hints().is_empty());
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilder5.
#[test]
fn test_request_builder_5() {
    let mut analyze = RequestBuilder::new();
    analyze
        .set_analyze_request([0x08, 0, 0x18, 0, 0x20, 0], IsolationLevel::ReadCommitted)
        .set_keep_order(true)
        .set_concurrency(15);
    let analyze = analyze.build().expect("analyze");
    assert_eq!(analyze.request_type, RequestType::Analyze);
    assert_eq!(
        analyze.isolation_level,
        IsolationLevel::ReadCommitted
    );
    assert_eq!(analyze.priority, KvPriority::Low);
    assert!(analyze.not_fill_cache);
    assert!(analyze.keep_order);
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilder6.
#[test]
fn test_request_builder_6() {
    let mut checksum = RequestBuilder::new();
    checksum
        .set_checksum_request([0x10, 0, 0x18, 0])
        .set_concurrency(10);
    let checksum = checksum.build().expect("checksum");
    assert_eq!(checksum.request_type, RequestType::Checksum);
    assert!(checksum.not_fill_cache);
    assert_eq!(checksum.concurrency, 10);
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilder7.
#[test]
fn test_request_builder_7() {
    for replica in [
        ReplicaReadType::Leader,
        ReplicaReadType::Follower,
        ReplicaReadType::Mixed,
    ] {
        let mut context = DistSqlContext::new();
        context.request.replica_read = replica;
        let mut builder = RequestBuilder::from_context(&context);
        builder.set_concurrency(10);
        let request = builder.build().expect("replica request");
        assert_eq!(request.replica_read, replica);
        assert_eq!(request.concurrency, 10);
        assert_eq!(
            request
                .key_ranges
                .as_ref()
                .expect("default ranges")
                .partitions(),
            vec![vec![]]
        );
    }
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilder8.
#[test]
fn test_request_builder_8() {
    let mut context = DistSqlContext::new();
    context.request.resource_group_name = "test".to_owned();
    let mut builder = RequestBuilder::from_context(&context);
    let request = builder.build().expect("default request");
    assert_eq!(request.resource_group_name, "test");
    assert_eq!(
        request.concurrency,
        isize::try_from(DEFAULT_DIST_SQL_CONCURRENCY).unwrap()
    );
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilderKeepsPagingSizeBytesWhenPagingDisabled.
#[test]
fn test_request_builder_keeps_paging_size_bytes_when_paging_disabled() {
    let mut context = DistSqlContext::new();
    context.request.paging = PagingConfig {
        enabled: false,
        size_bytes: 4 * 1024 * 1024,
        ..PagingConfig::source_defaults()
    };
    let mut builder = RequestBuilder::from_context(&context);
    let request = builder.build().expect("session projection");
    assert!(!request.paging.enabled);
    assert_eq!(request.paging.size_bytes, 4 * 1024 * 1024);
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilderTiKVClientReadTimeout.
#[test]
fn test_request_builder_tikv_client_read_timeout() {
    let mut context = DistSqlContext::new();
    context.request.tikv_client_read_timeout_ms = 100;
    let mut builder = RequestBuilder::from_context(&context);
    let request = builder.build().expect("session projection");
    assert_eq!(request.tikv_client_read_timeout_ms, 100);
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilderMaxExecutionTime.
#[test]
fn test_request_builder_max_execution_time() {
    let mut context = DistSqlContext::new();
    context.request.max_execution_time_ms = 100;
    let mut builder = RequestBuilder::from_context(&context);
    let request = builder.build().expect("session projection");
    assert_eq!(request.max_execution_time_ms, 100);
}

// Go pkg/distsql/request_builder_test.go::TestScanLimitConcurrency.
#[test]
fn test_scan_limit_concurrency() {
    for (kind, limit, expected) in [
        (ExecutorKind::TableScan, 1, 1),
        (ExecutorKind::IndexScan, 1, 1),
        (ExecutorKind::TableScan, 1_000_000, 15),
        (ExecutorKind::IndexScan, 1_000_000, 15),
    ] {
        let dag = RequestEnvelope::new(vec![
            ExecutorShape::new(kind),
            ExecutorShape::limit(limit, None),
        ]);
        let mut builder = RequestBuilder::new();
        builder
            .set_dag_request(dag, DAG_BYTES)
            .set_from_context(&DistSqlContext::new());
        let request = builder.build().expect("scan limit");
        assert_eq!(request.concurrency, expected);
        assert_eq!(request.limit_size, limit);
    }
}

// Go pkg/distsql/request_builder_test.go::TestIndexLookUpPushDownScanConcurrency.
#[test]
fn test_index_look_up_push_down_scan_concurrency() {
    for (limit, expected) in [(1, 1), (1_000_000, 15)] {
        let dag = RequestEnvelope::new(vec![
            ExecutorShape::new(ExecutorKind::IndexScan),
            ExecutorShape::limit(limit, Some(3)),
            ExecutorShape::new(ExecutorKind::TableScan),
            ExecutorShape::new(ExecutorKind::IndexLookup),
        ]);
        let mut builder = RequestBuilder::new();
        builder
            .set_dag_request(dag, DAG_BYTES)
            .set_from_context(&DistSqlContext::new());
        assert_eq!(
            builder.build().expect("lookup").concurrency,
            expected
        );
    }
}

#[test]
fn dag_concurrency_preserves_builder_call_order() {
    let ranges = vec![vec![], vec![], vec![], vec![], vec![], vec![]];
    let small_limit = || {
        RequestEnvelope::new(vec![
            ExecutorShape::new(ExecutorKind::TableScan),
            ExecutorShape::limit(1, None),
        ])
    };
    let mut context = DistSqlContext::new();
    context.request.dist_sql_concurrency = 4;

    let mut dag_then_context = RequestBuilder::new();
    dag_then_context
        .set_partition_key_ranges(ranges.clone())
        .set_dag_request(small_limit(), DAG_BYTES)
        .set_from_context(&context);
    assert_eq!(
        dag_then_context.build().unwrap().concurrency,
        4,
        "a later session projection caps SetDAGRequest's partition count"
    );

    let mut context_then_dag = RequestBuilder::new();
    context_then_dag
        .set_from_context(&context)
        .set_partition_key_ranges(ranges)
        .set_dag_request(small_limit(), DAG_BYTES);
    assert_eq!(
        context_then_dag.build().unwrap().concurrency,
        6,
        "a later SetDAGRequest owns the small-limit concurrency mutation"
    );

    let mut zero = RequestBuilder::new();
    assert_eq!(zero.build().unwrap().concurrency, 0);

    let ordered_scan = || {
        let mut dag = RequestEnvelope::new(vec![ExecutorShape::new(ExecutorKind::TableScan)]);
        dag.keep_order = true;
        dag
    };
    let mut default_scan = RequestBuilder::new();
    default_scan
        .set_keep_order(true)
        .set_dag_request(ordered_scan(), DAG_BYTES)
        .set_from_context(&DistSqlContext::new());
    assert_eq!(default_scan.build().unwrap().concurrency, 2);

    let mut explicit_context = DistSqlContext::new();
    explicit_context.request.dist_sql_concurrency = 4;
    let mut explicit_scan = RequestBuilder::new();
    explicit_scan
        .set_keep_order(true)
        .set_dag_request(ordered_scan(), DAG_BYTES)
        .set_from_context(&explicit_context);
    assert_eq!(explicit_scan.build().unwrap().concurrency, 4);
}

#[test]
fn complete_metadata_setters_and_read_consistency_reach_transport_snapshot() {
    let mut context = DistSqlContext::new();
    context.request.weak_consistency = true;
    context.request.rc_check_ts = true;
    context.request.replica_read = ReplicaReadType::Follower;
    context.request.paging.size_bytes = 4096;

    let mut builder = RequestBuilder::from_context(&context);
    builder
        .set_start_ts(42)
        .set_store_type(StoreType::TiFlash)
        .set_allow_batch_cop(true)
        .set_tidb_server_id(7)
        .set_schema_version(9)
        .set_txn_scope("zone-a")
        .set_read_replica_scope("zone-a")
        .set_is_staleness(true)
        .set_connection(11, "client")
        .set_resource_group_name("rg")
        .set_request_source(RequestSource {
            internal: true,
            source_type: "internal".to_owned(),
            explicit_source_type: "old".to_owned(),
        })
        .set_explicit_request_source_type("explicit")
        .set_paging(false)
        .set_replica_read(ReplicaReadType::Closest);
    let transport = builder
        .build_transport_request(std::sync::Arc::new(tidb_distsql::CancelHandle::default()))
        .unwrap();
    let request = transport.metadata();
    assert_eq!(request.start_ts, 42);
    assert_eq!(request.store_type, StoreType::TiFlash);
    assert!(request.batch_cop);
    assert_eq!((request.tidb_server_id, request.schema_version), (7, 9));
    assert_eq!(
        (
            request.txn_scope.as_str(),
            request.read_replica_scope.as_str()
        ),
        ("zone-a", "zone-a")
    );
    assert!(request.is_staleness);
    assert_eq!(
        (request.connection_id, request.connection_alias.as_str()),
        (11, "client")
    );
    assert_eq!(request.resource_group_name, "rg");
    assert!(request.request_source.internal);
    assert_eq!(request.request_source.source_type, "internal");
    assert_eq!(
        request.request_source.explicit_source_type,
        "explicit"
    );
    assert!(!request.paging.enabled);
    assert_eq!(request.paging.size_bytes, 4096);
    assert_eq!(
        request.isolation_level,
        IsolationLevel::ReadCommitted
    );
    assert_eq!(
        request.match_store_labels,
        vec![StoreLabel {
            key: DC_LABEL_KEY.to_owned(),
            value: "zone-a".to_owned()
        }]
    );

    let mut rc_check = DistSqlContext::new();
    rc_check.request.rc_check_ts = true;
    rc_check.request.replica_read = ReplicaReadType::Follower;
    let request = RequestBuilder::from_context(&rc_check).build().unwrap();
    assert_eq!(request.isolation_level, IsolationLevel::RcCheckTs);
    assert_eq!(request.replica_read, ReplicaReadType::Leader);

    let mut analyze = RequestBuilder::new();
    analyze
        .set_analyze_request([], IsolationLevel::ReadCommitted)
        .set_from_context(&DistSqlContext::new());
    assert_eq!(
        analyze.build().unwrap().isolation_level,
        IsolationLevel::ReadCommitted
    );
}

fn assert_full_table_ranges(common_handle: bool, low: &[u8], high: &[u8]) {
    for ids in [vec![1], vec![1, 2, 3], vec![1, 3]] {
        let ranges = build_table_ranges(&TableRangeSpec {
            table_id: 0,
            partition_ids: ids.clone(),
            common_handle,
            indexes: Vec::new(),
        })
        .expect("partition ranges");
        let expected = ids
            .into_iter()
            .map(|id| RequestKeyRange {
                start_key: encode_row_key(id, low).into(),
                end_key: encode_row_key(id, high).into(),
            })
            .collect::<Vec<_>>();
        assert_eq!(ranges, expected);
    }

    let ranges = build_table_ranges(&TableRangeSpec {
        table_id: 7,
        common_handle,
        ..TableRangeSpec::default()
    })
    .expect("nonpartitioned ranges");
    assert_eq!(
        ranges,
        [RequestKeyRange {
            start_key: encode_row_key(7, low).into(),
            end_key: encode_row_key(7, high).into(),
        }]
    );
}

// Go pkg/distsql/request_builder_test.go::TestBuildTableRangeIntHandle.
#[test]
fn test_build_table_range_int_handle() {
    let low = encoded_int(i64::MIN);
    let high = Key::from_bytes(encoded_int(i64::MAX))
        .prefix_next()
        .into_bytes();
    assert_full_table_ranges(false, &low, &high);
}

// Go pkg/distsql/request_builder_test.go::TestBuildTableRangeCommonHandle.
#[test]
fn test_build_table_range_common_handle() {
    let low = encode_key(&[Datum::MinNotNull]).expect("minimum common handle");
    let high = Key::from_bytes(encode_key(&[Datum::MaxValue]).expect("maximum common handle"))
        .prefix_next()
        .into_bytes();
    assert_full_table_ranges(true, &low, &high);
}

// Go pkg/distsql/request_builder_test.go::TestRequestBuilderHandle.
#[test]
fn test_request_builder_handle() {
    let handles = [0, 2, 3, 4, 5, 10, 11, 100].map(|value| Handle::Int(IntHandle::new(value)));
    let mut builder = RequestBuilder::new();
    builder
        .set_table_handles(15, &handles)
        .set_dag_request(RequestEnvelope::new(Vec::new()), DAG_BYTES)
        .set_resource_group_tagger(ResourceGroupTagBuilder::new(None));
    let transport = builder
        .build_transport_request(std::sync::Arc::new(tidb_distsql::CancelHandle::default()))
        .expect("transport envelope");
    let encoded = transport.resource_group_tag().expect("resource tag");
    let tag = ResourceGroupTag::decode(encoded.as_slice()).expect("valid tag");
    assert_eq!(tag.table_id, Some(15));
}

#[test]
fn complete_original_obligation_inventory_is_visible() {
    // This prevents consolidation from silently dropping generated subtests
    // or the file-level obligation while unavailable TiKV layers stay PARTIAL.
    let obligations = [
        "file",
        "handles",
        "partition-handles",
        "table-ranges",
        "index-ranges",
        "builder-1",
        "builder-2",
        "builder-3",
        "builder-4",
        "builder-5",
        "builder-6",
        "builder-7",
        "builder-7-generated",
        "builder-8",
        "paging-bytes",
        "read-timeout",
        "max-exec-time",
        "table-ranges-fbs",
        "index-ranges-fbs",
        "scan-limit",
        "scan-limit-generated",
        "index-lookup",
        "index-lookup-generated",
        "table-int",
        "table-common",
        "resource-tag",
    ];
    assert_eq!(obligations.len(), 26);
    assert_eq!(Key::from_bytes(vec![0xff]).as_bytes(), &[0xff]);
}
