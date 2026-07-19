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

use prost::Message;
use tidb_distsql::EncodeType as DistSqlEncodeType;
use tidb_exec::dag_request::{
    construct_dag_req, DagRequestBuildError, DagRequestContext, TiKvScanPlan,
    DEFAULT_DIV_PRECISION_INCREMENT,
};
use tidb_planner::{
    access_path::{DataSourceAccessPath, IndexAccessPath, PointGetAdmission},
    cardinality::{
        index_range_policy::{IndexRangeShape, RangeBoundKind},
        live_index_optimizer::{IndexPointStatistics, LiveIndexCandidate},
    },
    logical_data_source::LogicalDataSource,
    logical_data_source_task::IndexTaskProperty,
    physical_index_scan::PhysicalIndexScanPlan,
    physical_table_scan::PhysicalTableScanPlan,
    scan_pushdown::{
        check_cover_index, IndexPushdownMetadataError, ResolvedIndexDescriptor, ScanColumnInfo,
        TiKvIndexScanSpec, TiKvTableScanSpec, UnsupportedScanFeature,
    },
    task_type::TaskType,
};
use tidb_proto::tipb::{DagRequest, EncodeType, Endian, EngineType, ExecType};

fn value_range(width: usize) -> IndexRangeShape {
    IndexRangeShape::new(
        vec![RangeBoundKind::Value; width],
        vec![RangeBoundKind::Value; width],
        false,
        false,
    )
}

fn candidate(index_id: i64, ranges: Vec<IndexRangeShape>) -> LiveIndexCandidate {
    LiveIndexCandidate {
        index_id,
        ranges,
        proven_equality_range: false,
        point_statistics: IndexPointStatistics {
            topn_count: None,
            cms_count: None,
            histogram_count: 1,
        },
        row_size: 8.0,
        scan_factor: 1.0,
        index_scan_cost_factor: 1.0,
    }
}

fn default_context() -> DagRequestContext {
    DagRequestContext::new("UTC", 0, 32, DistSqlEncodeType::Default)
}

#[test]
fn timezone_and_empty_table_scan_match_go_wire() {
    // pkg/executor/test/executor/executor_test.go:84-113 TestTimezonePushDown
    // pkg/executor/table_readers_required_rows_test.go:149-158 buildMockDAGRequest
    let table = PhysicalTableScanPlan::init(1, 0, TiKvTableScanSpec::new(12345, vec![]));
    let context = DagRequestContext::new("Asia/Shanghai", 28_800, 32, DistSqlEncodeType::Default);
    let request = construct_dag_req(&context, &[TiKvScanPlan::Table(&table)]).unwrap();

    assert_eq!(request.time_zone_name.as_deref(), Some("Asia/Shanghai"));
    assert_eq!(request.time_zone_offset, Some(28_800));
    assert_eq!(request.flags, Some(32));
    assert_eq!(request.encode_type, Some(EncodeType::TypeDefault as i32));
    assert_eq!(request.collect_execution_summaries, None);
    assert_eq!(request.chunk_memory_layout, None);
    assert_eq!(request.div_precision_increment, None);
    let executor = &request.executors[0];
    assert_eq!(executor.tp, Some(ExecType::TypeTableScan as i32));
    assert_eq!(executor.executor_id.as_deref(), Some(""));
    let scan = executor.tbl_scan.as_ref().unwrap();
    assert_eq!(scan.table_id, Some(12345));
    assert_eq!(scan.next_read_engine, Some(EngineType::Local as i32));
    assert_eq!(scan.keep_order, Some(false));
    assert_eq!(scan.is_fast_scan, Some(false));
    assert_eq!(scan.max_wait_time_ms, Some(0));

    let go_wire = vec![
        0x12, 0x13, 0x08, 0x00, 0x12, 0x0d, 0x08, 0xb9, 0x60, 0x18, 0x00, 0x28, 0x00, 0x40, 0x00,
        0x48, 0x00, 0x60, 0x00, 0x52, 0x00, 0x18, 0x80, 0xe1, 0x01, 0x20, 0x20, 0x40, 0x00, 0x5a,
        0x0d, b'A', b's', b'i', b'a', b'/', b'S', b'h', b'a', b'n', b'g', b'h', b'a', b'i',
    ];
    assert_eq!(request.encode_to_vec(), go_wire);
    assert_eq!(DagRequest::decode(go_wire.as_slice()).unwrap(), request);
}

#[test]
fn table_scan_preserves_pre_resolved_column_and_common_handle_metadata() {
    // physical_table_scan.go:780-784,809-822 and tables.BuildTableScanFromInfos
    let column = ScanColumnInfo {
        column_id: 7,
        tp: 8,
        collation: -46,
        column_len: 20,
        decimal: 3,
        flag: 2,
        elems: vec!["red".to_owned(), "blue".to_owned()],
        default_val: Some(vec![1, 42]),
        pk_handle: true,
        array: false,
    };
    let stored_column = ScanColumnInfo {
        column_id: 8,
        tp: 8,
        collation: 63,
        column_len: 20,
        decimal: 0,
        flag: 1,
        pk_handle: false,
        ..ScanColumnInfo::default()
    };
    let mut spec = TiKvTableScanSpec::new(42, vec![column, stored_column]);
    spec.desc = true;
    spec.keep_order = true;
    spec.primary_column_ids = vec![7, 9];
    spec.primary_prefix_column_ids = vec![7];
    let table = PhysicalTableScanPlan::init(2, 0, spec);
    let request = construct_dag_req(&default_context(), &[TiKvScanPlan::Table(&table)]).unwrap();
    assert_eq!(request.output_offsets, [0, 1]);
    let scan = request.executors[0].tbl_scan.as_ref().unwrap();

    assert_eq!(scan.desc, Some(true));
    assert_eq!(scan.keep_order, Some(true));
    assert_eq!(scan.primary_column_ids, vec![7, 9]);
    assert_eq!(scan.primary_prefix_column_ids, vec![7]);
    let column = &scan.columns[0];
    assert_eq!(column.column_id, Some(7));
    assert_eq!(column.tp, Some(8));
    assert_eq!(column.collation, Some(-46));
    assert_eq!(column.column_len, Some(20));
    assert_eq!(column.decimal, Some(3));
    assert_eq!(column.flag, Some(2));
    assert_eq!(column.elems, vec!["red", "blue"]);
    assert_eq!(column.default_val.as_deref(), Some([1, 42].as_slice()));
    assert_eq!(column.pk_handle, Some(true));
    assert_eq!(column.array, Some(false));
    let stored_column = &scan.columns[1];
    assert_eq!(stored_column.column_id, Some(8));
    assert_eq!(stored_column.flag, Some(1));
    assert_eq!(stored_column.pk_handle, Some(false));
    assert_eq!(
        DagRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
}

#[test]
fn check_cover_index_ports_source_width_and_null_rules() {
    // physical_index_scan.go:579-603 checkCoverIndex
    let covered = value_range(2);
    assert!(check_cover_index(true, 2, std::slice::from_ref(&covered)));
    assert!(!check_cover_index(false, 2, std::slice::from_ref(&covered)));
    assert!(!check_cover_index(true, 1, std::slice::from_ref(&covered)));

    let low_null = IndexRangeShape::new(
        [RangeBoundKind::Null, RangeBoundKind::Value],
        [RangeBoundKind::Value, RangeBoundKind::Value],
        false,
        false,
    );
    let high_null = IndexRangeShape::new(
        [RangeBoundKind::Value, RangeBoundKind::Value],
        [RangeBoundKind::Value, RangeBoundKind::Null],
        false,
        false,
    );
    assert!(!check_cover_index(true, 2, &[low_null]));
    assert!(!check_cover_index(true, 2, &[high_null]));

    // The source loop is vacuously true for a declared unique index with no
    // ranges; empty-range task conversion itself returns TableDual upstream.
    assert!(check_cover_index(true, 2, &[]));
}

#[test]
fn live_index_task_reaches_index_scan_dag_payload() {
    let ranges = vec![value_range(2)];
    let pushdown = TiKvIndexScanSpec::new(
        100,
        11,
        vec![ScanColumnInfo {
            column_id: 3,
            tp: 3,
            ..ScanColumnInfo::default()
        }],
        true,
        2,
    );
    let descriptor = ResolvedIndexDescriptor {
        index_id: 11,
        declared_unique: true,
        index_column_count: 2,
    };
    let path = IndexAccessPath::from_source_index_scan(
        candidate(11, ranges),
        5.0,
        PointGetAdmission::NotEligible,
    )
    .with_pushdown(descriptor, pushdown)
    .unwrap();
    let source = LogicalDataSource::new(9, 0, [DataSourceAccessPath::Index(path)]);
    let task = source.build_index_task(IndexTaskProperty::new(TaskType::CopSingleRead));
    let plan = task.index_plan().expect("source-admitted index task");
    let request = construct_dag_req(&default_context(), &[TiKvScanPlan::Index(plan)]).unwrap();
    let executor = &request.executors[0];
    assert_eq!(executor.tp, Some(ExecType::TypeIndexScan as i32));
    assert_eq!(executor.executor_id, None);
    let scan = executor.idx_scan.as_ref().unwrap();
    assert_eq!(scan.table_id, Some(100));
    assert_eq!(scan.index_id, Some(11));
    assert_eq!(scan.desc, Some(false));
    assert_eq!(scan.unique, Some(true));
    assert_eq!(scan.columns[0].column_id, Some(3));
    assert_eq!(
        DagRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
}

#[test]
fn request_optional_fields_follow_construct_dag_req_presence_rules() {
    let table = PhysicalTableScanPlan::init(1, 0, TiKvTableScanSpec::new(1, vec![]));
    let mut context = default_context();
    context.collect_execution_summaries = true;
    context.div_precision_increment = 7;
    context.encode_type = DistSqlEncodeType::Chunk;
    let request = construct_dag_req(&context, &[TiKvScanPlan::Table(&table)]).unwrap();
    assert_eq!(request.collect_execution_summaries, Some(true));
    assert_eq!(request.div_precision_increment, Some(7));
    assert_eq!(request.encode_type, Some(EncodeType::TypeChunk as i32));
    let expected_endian = if cfg!(target_endian = "big") {
        Endian::BigEndian
    } else {
        Endian::LittleEndian
    };
    assert_eq!(
        request
            .chunk_memory_layout
            .as_ref()
            .and_then(|layout| layout.endian),
        Some(expected_endian as i32)
    );

    context.collect_execution_summaries = false;
    context.div_precision_increment = DEFAULT_DIV_PRECISION_INCREMENT;
    let request = construct_dag_req(&context, &[TiKvScanPlan::Table(&table)]).unwrap();
    assert_eq!(request.collect_execution_summaries, None);
    assert_eq!(request.div_precision_increment, None);
    assert!(request.chunk_memory_layout.is_some());
}

#[test]
fn index_pushdown_rejects_split_identity_and_shape_authority() {
    let candidate = candidate(11, vec![value_range(2)]);
    let descriptor = ResolvedIndexDescriptor {
        index_id: 11,
        declared_unique: true,
        index_column_count: 2,
    };

    let error = PhysicalIndexScanPlan::init(1, 0, &candidate, 1.0)
        .try_with_pushdown(
            ResolvedIndexDescriptor {
                index_id: 12,
                ..descriptor
            },
            TiKvIndexScanSpec::new(1, 12, vec![], true, 2),
        )
        .unwrap_err();
    assert_eq!(
        error,
        IndexPushdownMetadataError::CandidateIndexId {
            candidate: 11,
            descriptor: 12,
        }
    );

    let error = PhysicalIndexScanPlan::init(1, 0, &candidate, 1.0)
        .try_with_pushdown(descriptor, TiKvIndexScanSpec::new(1, 12, vec![], true, 2))
        .unwrap_err();
    assert_eq!(
        error,
        IndexPushdownMetadataError::PushdownIndexId {
            descriptor: 11,
            pushdown: 12,
        }
    );

    let error = PhysicalIndexScanPlan::init(1, 0, &candidate, 1.0)
        .try_with_pushdown(descriptor, TiKvIndexScanSpec::new(1, 11, vec![], false, 2))
        .unwrap_err();
    assert_eq!(
        error,
        IndexPushdownMetadataError::DeclaredUnique {
            descriptor: true,
            pushdown: false,
        }
    );

    let error =
        IndexAccessPath::from_source_index_scan(candidate, 1.0, PointGetAdmission::NotEligible)
            .with_pushdown(descriptor, TiKvIndexScanSpec::new(1, 11, vec![], true, 1))
            .unwrap_err();
    assert_eq!(
        error,
        IndexPushdownMetadataError::IndexColumnCount {
            descriptor: 2,
            pushdown: 1,
        }
    );
}

#[test]
fn unsupported_or_incomplete_scans_fail_closed() {
    assert_eq!(
        construct_dag_req(&default_context(), &[]),
        Err(DagRequestBuildError::PlanCount { actual: 0 })
    );
    let table = PhysicalTableScanPlan::init(1, 0, TiKvTableScanSpec::new(1, vec![]));
    assert_eq!(
        construct_dag_req(
            &default_context(),
            &[TiKvScanPlan::Table(&table), TiKvScanPlan::Table(&table)],
        ),
        Err(DagRequestBuildError::PlanCount { actual: 2 })
    );

    let candidate = candidate(1, vec![value_range(1)]);
    let missing = PhysicalIndexScanPlan::init(1, 0, &candidate, 1.0);
    assert_eq!(
        construct_dag_req(&default_context(), &[TiKvScanPlan::Index(&missing)]),
        Err(DagRequestBuildError::MissingIndexPushdown)
    );

    let array_index = PhysicalIndexScanPlan::init(1, 0, &candidate, 1.0)
        .try_with_pushdown(
            ResolvedIndexDescriptor {
                index_id: 1,
                declared_unique: false,
                index_column_count: 1,
            },
            TiKvIndexScanSpec::new(
                1,
                1,
                vec![ScanColumnInfo {
                    array: true,
                    ..ScanColumnInfo::default()
                }],
                false,
                1,
            ),
        )
        .unwrap();
    assert_eq!(
        construct_dag_req(&default_context(), &[TiKvScanPlan::Index(&array_index)]),
        Err(DagRequestBuildError::UnsupportedScanFeature(
            UnsupportedScanFeature::MultiValuedIndex
        ))
    );

    for feature in [
        UnsupportedScanFeature::Partition,
        UnsupportedScanFeature::UnresolvedDefaultValue,
        UnsupportedScanFeature::LateMaterialization,
        UnsupportedScanFeature::RuntimeFilter,
        UnsupportedScanFeature::ColumnarIndex,
        UnsupportedScanFeature::MultiValuedIndex,
        UnsupportedScanFeature::FullTextSearch,
        UnsupportedScanFeature::VectorSearch,
    ] {
        let rejected = PhysicalTableScanPlan::init(
            1,
            0,
            TiKvTableScanSpec::new(1, vec![]).with_unsupported(feature),
        );
        assert_eq!(
            construct_dag_req(&default_context(), &[TiKvScanPlan::Table(&rejected)]),
            Err(DagRequestBuildError::UnsupportedScanFeature(feature))
        );
    }
}
