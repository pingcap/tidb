// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use tidb_planner::{
    access_path::{
        AccessPathStore, DataSourceAccessPath, IndexAccessPath, PointGetAdmission, TableAccessPath,
    },
    cardinality::{
        index_range_policy::{IndexRangeShape, RangeBoundKind},
        live_index_optimizer::{IndexPointStatistics, LiveIndexCandidate},
    },
    index_task::{IndexTaskRejection, ScanReadTask, ScanReadTaskRejection, TableTaskRejection},
    logical_data_source::LogicalDataSource,
    logical_data_source_task::IndexTaskProperty,
    physical_property::IndexOrderingRequirement,
    scan_pushdown::{ScanColumnInfo, TiKvTableScanSpec, UnsupportedScanFeature},
    task_type::TaskType,
};

fn source(paths: impl IntoIterator<Item = DataSourceAccessPath>) -> LogicalDataSource {
    LogicalDataSource::new(91, -4, paths)
}

fn table_path(spec: TiKvTableScanSpec) -> TableAccessPath {
    TableAccessPath::from_source_table_scan(spec, PointGetAdmission::NotEligible, 12.0)
}

fn index_path() -> DataSourceAccessPath {
    let candidate = LiveIndexCandidate {
        index_id: 7,
        ranges: vec![IndexRangeShape::new(
            [RangeBoundKind::Value],
            [RangeBoundKind::Value],
            false,
            false,
        )],
        proven_equality_range: true,
        point_statistics: IndexPointStatistics {
            topn_count: Some(2),
            cms_count: None,
            histogram_count: 4,
        },
        row_size: 16.0,
        scan_factor: 1.0,
        index_scan_cost_factor: 1.0,
    };
    DataSourceAccessPath::Index(IndexAccessPath::from_source_index_scan(
        candidate,
        2.0,
        PointGetAdmission::NotEligible,
    ))
}

fn table_rejection(
    path: TableAccessPath,
    property: IndexTaskProperty,
) -> Option<ScanReadTaskRejection> {
    source([DataSourceAccessPath::Table(path)])
        .build_scan_read_task(property)
        .rejection()
}

#[test]
fn source_admitted_table_path_becomes_one_root_table_reader() {
    // pkg/planner/core/find_best_task.go:2829 convertToTableScan
    // pkg/planner/core/operator/physicalop/task_base.go:509
    // pkg/planner/core/operator/physicalop/physical_table_reader.go:87
    let column = ScanColumnInfo {
        column_id: 3,
        tp: 8,
        ..ScanColumnInfo::default()
    };
    let mut spec = TiKvTableScanSpec::new(42, vec![column]);
    spec.primary_column_ids = vec![3];

    let task = source([DataSourceAccessPath::Table(table_path(spec.clone()))])
        .build_scan_read_task(IndexTaskProperty::new(TaskType::Root));
    let reader = task
        .table_reader()
        .expect("one admitted table Cop task must convert to a root reader");
    let scan = reader
        .table_scan_plan()
        .expect("the reader must own the exact physical table scan");

    assert_eq!(reader.plan_type(), "TableReader");
    assert_eq!(reader.read_req_name(), "cop");
    assert_eq!(reader.query_block_offset(), -4);
    assert_eq!(reader.table_plans_len(), 1);
    assert_eq!(reader.table_scan_count(), 1);
    assert!(reader.table_plan_is_first_flattened());
    assert_eq!(reader.table_plan_explain(), Some("TableScan"));
    assert_eq!(reader.explain_info(), "data:TableScan");
    assert_eq!(scan.plan().operator(), "TableScan");
    assert_eq!(scan.plan().id(), 91);
    assert_eq!(scan.plan().query_block_offset(), -4);
    assert_eq!(scan.estimated_rows(), Some(12.0));
    assert_eq!(scan.pushdown(), &spec);
    assert!(task.index_plan().is_none());

    let cloned = reader.clone_plan();
    assert_eq!(cloned.table_scan_plan(), reader.table_scan_plan());
    assert_eq!(
        cloned
            .table_scan_plan()
            .expect("clone retains the owned scan")
            .plan()
            .id(),
        91
    );
    assert!(cloned.table_plan_is_first_flattened());
}

#[test]
fn empty_table_ranges_return_dual_before_any_other_admission() {
    // pkg/planner/core/find_best_task.go:2180-2188
    let path = TableAccessPath::from_source_table_scan(
        TiKvTableScanSpec::new(1, vec![]).with_unsupported(UnsupportedScanFeature::Partition),
        PointGetAdmission::Unproven,
        f64::NAN,
    )
    .with_store(AccessPathStore::TiFlash)
    .with_filters(true)
    .with_empty_ranges();
    let task = source([
        DataSourceAccessPath::Table(path),
        DataSourceAccessPath::IndexMerge,
    ])
    .build_scan_read_task(IndexTaskProperty::new(TaskType::Mpp));

    assert_eq!(
        task.table_dual()
            .expect("empty ranger result dominates task construction")
            .row_count(),
        0
    );
    assert_eq!(task.table_dual().unwrap().query_block_offset(), -4);
    assert!(task.rejection().is_none());
}

#[test]
fn unified_scan_task_preserves_existing_index_only_behavior() {
    let task = source([index_path()])
        .build_scan_read_task(IndexTaskProperty::new(TaskType::CopSingleRead));
    let scan = task
        .index_plan()
        .expect("index-only input delegates to the established builder");
    assert_eq!(scan.index_id(), 7);
    assert_eq!(scan.estimated_rows(), 2.0);
    assert!(matches!(task, ScanReadTask::Index(_)));

    assert_eq!(
        source([]).build_scan_read_task(IndexTaskProperty::new(TaskType::CopSingleRead)),
        ScanReadTask::Invalid(ScanReadTaskRejection::Index(
            IndexTaskRejection::NoAccessPaths
        ))
    );
}

#[test]
fn table_path_fails_closed_on_unowned_planner_and_executor_shapes() {
    let root = IndexTaskProperty::new(TaskType::Root);
    let spec = TiKvTableScanSpec::new(1, vec![]);

    assert_eq!(
        table_rejection(
            TableAccessPath::from_source_table_scan(spec.clone(), PointGetAdmission::Unproven, 1.0,),
            root,
        ),
        Some(ScanReadTaskRejection::Table(
            TableTaskRejection::UnprovenPointGetAdmission
        ))
    );
    assert_eq!(
        table_rejection(
            TableAccessPath::from_source_table_scan(spec.clone(), PointGetAdmission::Eligible, 1.0,),
            root,
        ),
        Some(ScanReadTaskRejection::Table(
            TableTaskRejection::PointGetRequired
        ))
    );
    assert_eq!(
        table_rejection(
            table_path(spec.clone()).with_store(AccessPathStore::TiFlash),
            root,
        ),
        Some(ScanReadTaskRejection::Table(
            TableTaskRejection::TiFlashStore
        ))
    );
    assert_eq!(
        table_rejection(table_path(spec.clone()).with_filters(true), root),
        Some(ScanReadTaskRejection::Table(TableTaskRejection::Filters))
    );
    assert_eq!(
        table_rejection(table_path(spec.clone()).with_partitioned(true), root),
        Some(ScanReadTaskRejection::Table(TableTaskRejection::Partition))
    );
    assert_eq!(
        table_rejection(table_path(spec.clone()).with_table_sample(true), root),
        Some(ScanReadTaskRejection::Table(
            TableTaskRejection::TableSample
        ))
    );
    assert_eq!(
        table_rejection(
            table_path(spec.clone()),
            root.with_ordering(IndexOrderingRequirement::KeepOrder),
        ),
        Some(ScanReadTaskRejection::Table(
            TableTaskRejection::RequiredOrdering
        ))
    );
    assert_eq!(
        table_rejection(
            table_path(spec.clone()),
            IndexTaskProperty::new(TaskType::CopSingleRead),
        ),
        Some(ScanReadTaskRejection::Table(
            TableTaskRejection::UnsupportedTaskType
        ))
    );
    assert_eq!(
        table_rejection(table_path(spec.clone()), root.with_expected_cnt(1.0),),
        Some(ScanReadTaskRejection::Table(
            TableTaskRejection::ExpectedCountUnsupported
        ))
    );

    for count_after_access in [f64::NAN, f64::INFINITY, -1.0] {
        assert_eq!(
            table_rejection(
                TableAccessPath::from_source_table_scan(
                    spec.clone(),
                    PointGetAdmission::NotEligible,
                    count_after_access,
                ),
                root,
            ),
            Some(ScanReadTaskRejection::Table(
                TableTaskRejection::InvalidCountAfterAccess
            ))
        );
    }

    assert_eq!(
        table_rejection(
            table_path(
                spec.clone()
                    .with_unsupported(UnsupportedScanFeature::Partition),
            ),
            root,
        ),
        Some(ScanReadTaskRejection::Table(TableTaskRejection::Partition))
    );

    for feature in [
        UnsupportedScanFeature::UnresolvedDefaultValue,
        UnsupportedScanFeature::LateMaterialization,
        UnsupportedScanFeature::RuntimeFilter,
        UnsupportedScanFeature::ColumnarIndex,
        UnsupportedScanFeature::MultiValuedIndex,
        UnsupportedScanFeature::FullTextSearch,
        UnsupportedScanFeature::VectorSearch,
    ] {
        assert_eq!(
            table_rejection(table_path(spec.clone().with_unsupported(feature)), root,),
            Some(ScanReadTaskRejection::Table(
                TableTaskRejection::UnsupportedScanFeature(feature)
            ))
        );
    }

    for (desc, keep_order) in [(true, false), (false, true)] {
        let mut ordered = spec.clone();
        ordered.desc = desc;
        ordered.keep_order = keep_order;
        assert_eq!(
            table_rejection(table_path(ordered), root),
            Some(ScanReadTaskRejection::Table(
                TableTaskRejection::RequiredOrdering
            ))
        );
    }
}

#[test]
fn table_path_rejects_candidate_sets_that_need_cost_or_index_merge() {
    let table = || DataSourceAccessPath::Table(table_path(TiKvTableScanSpec::new(1, vec![])));
    let root = IndexTaskProperty::new(TaskType::Root);

    assert_eq!(
        source([table(), table()])
            .build_scan_read_task(root)
            .rejection(),
        Some(ScanReadTaskRejection::MultipleTablePaths)
    );
    assert_eq!(
        source([table(), index_path()])
            .build_scan_read_task(root)
            .rejection(),
        Some(ScanReadTaskRejection::MixedAccessPaths)
    );
    assert_eq!(
        source([table(), DataSourceAccessPath::IndexMerge])
            .build_scan_read_task(root)
            .rejection(),
        Some(ScanReadTaskRejection::IndexMergePath)
    );
}
