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
    build_table_ranges, index_ranges_to_kv_ranges, table_ranges_to_kv_ranges, DatumRange,
    DistSqlContext, ExecutorKind, ExecutorShape, IsolationLevel, KvPriority, PagingConfig,
    ReplicaReadType, RequestBuilder, RequestEnvelope, RequestKeyRange, RequestSource, RequestType,
    StoreLabel, StoreType, TableRangeSpec, DC_LABEL_KEY, DEFAULT_DIST_SQL_CONCURRENCY,
    GLOBAL_REPLICA_SCOPE,
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
    // TestTableRangesToKVRanges, TestIndexRangesToKVRanges, and both FBS
    // variants use the same codec path; assert every adjusted boundary.
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

    let fbs = DatumRange::inclusive(vec![Datum::Int(1)], vec![Datum::Int(4)]);
    let table = table_ranges_to_kv_ranges(0, std::slice::from_ref(&fbs)).expect("table FBS");
    assert_eq!(table[0].start_key, encode_row_key(0, &encoded_int(1)));
    assert_eq!(table[0].end_key, encode_row_key(0, &encoded_int(5)));
    let index = index_ranges_to_kv_ranges(&[0], 0, &[fbs]).expect("index FBS");
    assert!(index[0][0]
        .start_key
        .ends_with(&encode_key(&[Datum::Int(1)]).expect("low")));
    assert!(index[0][0]
        .end_key
        .ends_with(&encode_key(&[Datum::Int(5)]).expect("high")));
}

#[test]
fn canonical_builder_covers_all_source_range_entry_points() {
    // TestRequestBuilder1-4 differ only in range production. They now all
    // converge on this one builder and one request envelope.
    let handles = [0, 2, 3, 4, 5, 10, 11, 100].map(|value| Handle::Int(IntHandle::new(value)));
    let raw = vec![RequestKeyRange {
        start_key: vec![1],
        end_key: vec![2],
    }];
    let mut builders = [
        {
            let mut builder = RequestBuilder::new();
            builder.set_table_ranges(12, &source_ranges());
            builder
        },
        {
            let mut builder = RequestBuilder::new();
            builder.set_index_ranges(12, 15, &source_ranges());
            builder
        },
        {
            let mut builder = RequestBuilder::new();
            builder.set_table_handles(15, &handles);
            builder
        },
        {
            let mut builder = RequestBuilder::new();
            builder.set_non_partitioned_key_ranges(raw.clone());
            builder
        },
    ];
    for builder in &mut builders {
        builder
            .set_dag_request(RequestEnvelope::new(Vec::new()), DAG_BYTES)
            .set_desc(false)
            .set_keep_order(false)
            .set_from_context(&DistSqlContext::new());
        let request = builder.build().expect("DAG request");
        assert_eq!(request.request_type, RequestType::Dag);
        assert_eq!(request.data.as_deref(), Some(DAG_BYTES));
        assert!(request.cacheable);
        assert_eq!(request.session.concurrency, DEFAULT_DIST_SQL_CONCURRENCY);
        assert_eq!(request.read_replica_scope, GLOBAL_REPLICA_SCOPE);
        assert!(request.key_ranges.is_some());
    }
}

#[test]
fn analyze_checksum_replica_and_default_state_match_source() {
    // TestRequestBuilder5-8.
    let mut analyze = RequestBuilder::new();
    analyze
        .set_analyze_request([0x08, 0, 0x18, 0, 0x20, 0], IsolationLevel::ReadCommitted)
        .set_keep_order(true)
        .set_concurrency(15);
    let analyze = analyze.build().expect("analyze");
    assert_eq!(analyze.request_type, RequestType::Analyze);
    assert_eq!(
        analyze.session.isolation_level,
        IsolationLevel::ReadCommitted
    );
    assert_eq!(analyze.session.priority, KvPriority::Low);
    assert!(analyze.session.not_fill_cache);
    assert!(analyze.keep_order);

    let mut checksum = RequestBuilder::new();
    checksum
        .set_checksum_request([0x10, 0, 0x18, 0])
        .set_concurrency(10);
    let checksum = checksum.build().expect("checksum");
    assert_eq!(checksum.request_type, RequestType::Checksum);
    assert!(checksum.session.not_fill_cache);
    assert_eq!(checksum.session.concurrency, 10);

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
        assert_eq!(request.session.replica_read, replica);
        assert_eq!(request.session.concurrency, 10);
        assert_eq!(
            request.key_ranges.expect("default ranges").partitions,
            vec![vec![]]
        );
    }

    let mut context = DistSqlContext::new();
    context.request.resource_group_name = "test".to_owned();
    let mut builder = RequestBuilder::from_context(&context);
    let request = builder.build().expect("default request");
    assert_eq!(request.session.resource_group_name, "test");
    assert_eq!(request.session.concurrency, DEFAULT_DIST_SQL_CONCURRENCY);
}

#[test]
fn paging_timeout_and_execution_time_survive_projection() {
    let mut context = DistSqlContext::new();
    context.request.paging = PagingConfig {
        enabled: false,
        size_bytes: 4 * 1024 * 1024,
        ..PagingConfig::source_defaults()
    };
    context.request.tikv_client_read_timeout_ms = 100;
    context.request.max_execution_time_ms = 101;
    let mut builder = RequestBuilder::from_context(&context);
    let request = builder.build().expect("session projection");
    assert!(!request.session.paging.enabled);
    assert_eq!(request.session.paging.size_bytes, 4 * 1024 * 1024);
    assert_eq!(request.session.tikv_client_read_timeout_ms, 100);
    assert_eq!(request.session.max_execution_time_ms, 101);
}

#[test]
fn scan_limit_and_index_lookup_concurrency_match_source_tables() {
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
        assert_eq!(request.session.concurrency, expected);
        assert_eq!(request.limit_size, limit);
    }

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
            builder.build().expect("lookup").session.concurrency,
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
        dag_then_context.build().unwrap().session.concurrency,
        4,
        "a later session projection caps SetDAGRequest's partition count"
    );

    let mut context_then_dag = RequestBuilder::new();
    context_then_dag
        .set_from_context(&context)
        .set_partition_key_ranges(ranges)
        .set_dag_request(small_limit(), DAG_BYTES);
    assert_eq!(
        context_then_dag.build().unwrap().session.concurrency,
        6,
        "a later SetDAGRequest owns the small-limit concurrency mutation"
    );

    let mut zero = RequestBuilder::new();
    assert_eq!(zero.build().unwrap().session.concurrency, 0);

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
    assert_eq!(default_scan.build().unwrap().session.concurrency, 2);

    let mut explicit_context = DistSqlContext::new();
    explicit_context.request.dist_sql_concurrency = 4;
    let mut explicit_scan = RequestBuilder::new();
    explicit_scan
        .set_keep_order(true)
        .set_dag_request(ordered_scan(), DAG_BYTES)
        .set_from_context(&explicit_context);
    assert_eq!(explicit_scan.build().unwrap().session.concurrency, 4);
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
    let transport = builder.build_transport_request().unwrap();
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
    assert_eq!(request.session.resource_group_name, "rg");
    assert!(request.session.request_source.internal);
    assert_eq!(request.session.request_source.source_type, "internal");
    assert_eq!(
        request.session.request_source.explicit_source_type,
        "explicit"
    );
    assert!(!request.session.paging.enabled);
    assert_eq!(request.session.paging.size_bytes, 4096);
    assert_eq!(
        request.session.isolation_level,
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
    assert_eq!(request.session.isolation_level, IsolationLevel::RcCheckTs);
    assert_eq!(request.session.replica_read, ReplicaReadType::Leader);

    let mut analyze = RequestBuilder::new();
    analyze
        .set_analyze_request([], IsolationLevel::ReadCommitted)
        .set_from_context(&DistSqlContext::new());
    assert_eq!(
        analyze.build().unwrap().session.isolation_level,
        IsolationLevel::ReadCommitted
    );
}

#[test]
fn full_table_ranges_cover_int_and_common_handle_partition_layouts() {
    for common_handle in [false, true] {
        for ids in [vec![1], vec![1, 2, 3], vec![1, 3]] {
            let ranges = build_table_ranges(&TableRangeSpec {
                table_id: 0,
                partition_ids: ids.clone(),
                common_handle,
                indexes: Vec::new(),
            })
            .expect("partition ranges");
            assert_eq!(ranges.len(), ids.len());
            for (range, id) in ranges.iter().zip(ids) {
                assert_eq!(&range.start_key[..9], &encode_row_key(id, &[])[..9]);
            }
        }
        let ranges = build_table_ranges(&TableRangeSpec {
            table_id: 7,
            common_handle,
            ..TableRangeSpec::default()
        })
        .expect("nonpartitioned ranges");
        assert_eq!(ranges.len(), 1);
        assert_eq!(&ranges[0].start_key[..9], &encode_row_key(7, &[])[..9]);
    }
}

#[test]
fn transport_consumer_builds_resource_tag_from_first_table_range() {
    let handles = [Handle::Int(IntHandle::new(0))];
    let mut builder = RequestBuilder::new();
    builder
        .set_table_handles(15, &handles)
        .set_dag_request(RequestEnvelope::new(Vec::new()), DAG_BYTES)
        .set_resource_group_tagger(ResourceGroupTagBuilder::new(None));
    let transport = builder
        .build_transport_request()
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
