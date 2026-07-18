// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use tidb_planner::cardinality::index_range_policy::{IndexRangeShape, RangeBoundKind};
use tidb_planner::{
    access_path::{
        AccessPathStore, DataSourceAccessPath, ExpectedCountRows, IndexAccessPath, IndexReadShape,
        PointEstimateAdmissionError, PointGetAdmission,
    },
    cardinality::live_index_optimizer::{IndexPointStatistics, LiveIndexCandidate},
    index_task::{IndexTask, IndexTaskRejection},
    logical_data_source::LogicalDataSource,
    logical_data_source_task::IndexTaskProperty,
    physical_index_scan::PhysicalIndexScanPlan,
    physical_property::IndexOrderingRequirement,
    task_type::TaskType,
};

fn range(low: RangeBoundKind, high: RangeBoundKind) -> IndexRangeShape {
    IndexRangeShape::new([low], [high], false, false)
}

fn candidate(index_id: i64) -> LiveIndexCandidate {
    LiveIndexCandidate {
        index_id,
        ranges: vec![range(RangeBoundKind::Value, RangeBoundKind::Value)],
        proven_equality_range: true,
        point_statistics: IndexPointStatistics {
            topn_count: Some(3),
            cms_count: Some(7),
            histogram_count: 11,
        },
        row_size: 32.0,
        scan_factor: 40.7,
        index_scan_cost_factor: 1.0,
    }
}

fn source(paths: impl IntoIterator<Item = DataSourceAccessPath>) -> LogicalDataSource {
    LogicalDataSource::new(71, -3, paths)
}

fn point_path(candidate: LiveIndexCandidate) -> DataSourceAccessPath {
    DataSourceAccessPath::Index(
        IndexAccessPath::from_proven_point_estimate(candidate, PointGetAdmission::NotEligible)
            .expect("test candidate is an upstream-proven equality range"),
    )
}

fn source_path(candidate: LiveIndexCandidate, rows: f64) -> DataSourceAccessPath {
    DataSourceAccessPath::Index(IndexAccessPath::from_source_index_scan(
        candidate,
        rows,
        PointGetAdmission::NotEligible,
    ))
}

fn rejection(paths: impl IntoIterator<Item = DataSourceAccessPath>) -> Option<IndexTaskRejection> {
    source(paths)
        .build_index_task(IndexTaskProperty::new(TaskType::CopSingleRead))
        .rejection()
}

#[test]
fn source_proven_point_statistics_flow_through_an_admitted_index_task() {
    // pkg/statistics/histogram_test.go:537 TestIndexQueryBytes
    // pkg/planner/core/operator/physicalop/physical_index_scan.go:644-695
    let topn_path =
        IndexAccessPath::from_proven_point_estimate(candidate(12), PointGetAdmission::NotEligible)
            .expect("proven equality is the only local stats adapter");
    let topn_source = source([DataSourceAccessPath::Index(topn_path.clone())]);
    let topn = topn_source.build_index_task(IndexTaskProperty::new(TaskType::CopSingleRead));
    let topn_scan = topn
        .index_plan()
        .expect("admitted index path must become a cop task");
    assert_eq!(topn_path.count_after_access(), Some(3.0));
    assert_eq!(topn_scan.estimated_rows(), 3.0);
    assert_eq!(topn_scan.plan().estimated_rows(), Some(3.0));
    assert_eq!(topn_scan.plan().operator(), "IndexScan");
    assert!((topn_scan.cost() - 610.500_012).abs() < f64::EPSILON);
    assert!(matches!(topn, IndexTask::CopSingleRead(_)));

    let mut cms = candidate(13);
    cms.point_statistics.topn_count = None;
    let cms =
        source([point_path(cms)]).build_index_task(IndexTaskProperty::new(TaskType::CopSingleRead));
    assert_eq!(
        cms.index_plan()
            .expect("cms path is valid")
            .estimated_rows(),
        7.0
    );

    let mut histogram = candidate(14);
    histogram.point_statistics.topn_count = None;
    histogram.point_statistics.cms_count = None;
    let histogram = source([point_path(histogram)])
        .build_index_task(IndexTaskProperty::new(TaskType::CopSingleRead));
    assert_eq!(
        histogram
            .index_plan()
            .expect("histogram path is valid")
            .estimated_rows(),
        11.0
    );
}

#[test]
fn source_count_after_access_is_precomputed_for_non_point_ranges() {
    // pkg/planner/core/stats.go:203-264
    // pkg/planner/cardinality/selectivity_test.go:541 TestCanSkipIndexEstimation
    let mut full = candidate(1);
    full.ranges = vec![range(RangeBoundKind::Null, RangeBoundKind::MaxValue)];
    full.proven_equality_range = false;
    assert_eq!(
        IndexAccessPath::from_proven_point_estimate(full.clone(), PointGetAdmission::NotEligible),
        Err(PointEstimateAdmissionError::UnsupportedRange)
    );

    let task = source([source_path(full, 60.0)])
        .build_index_task(IndexTaskProperty::new(TaskType::CopSingleRead));
    assert_eq!(
        task.index_plan()
            .expect("upstream cardinality permits the source range")
            .estimated_rows(),
        60.0
    );
}

#[test]
fn source_empty_range_returns_table_dual_before_candidate_enumeration() {
    // pkg/planner/core/find_best_task.go:2180-2188
    let mut empty = candidate(1);
    empty.ranges.clear();
    let task = source([
        DataSourceAccessPath::Index(IndexAccessPath::new(empty)),
        point_path(candidate(2)),
    ])
    .build_index_task(IndexTaskProperty::new(TaskType::CopSingleRead));
    assert_eq!(
        task.table_dual()
            .expect("empty range is TableDual")
            .row_count(),
        0
    );
    assert_eq!(
        task.table_dual()
            .expect("empty range is TableDual")
            .query_block_offset(),
        -3
    );
    assert!(task.index_plan().is_none());
}

#[test]
fn source_rejects_unproven_or_point_get_admitted_index_paths() {
    // pkg/planner/core/find_best_task.go:2189-2268
    assert_eq!(
        rejection([DataSourceAccessPath::Index(IndexAccessPath::new(
            candidate(1)
        ))]),
        Some(IndexTaskRejection::UnprovenPointGetAdmission)
    );
    assert_eq!(
        rejection([DataSourceAccessPath::Index(
            IndexAccessPath::from_source_index_scan(candidate(1), 3.0, PointGetAdmission::Eligible,)
        )]),
        Some(IndexTaskRejection::PointGetRequired)
    );
}

#[test]
fn source_datasource_index_task_rejects_unimplemented_go_path_forms() {
    // pkg/planner/core/find_best_task.go:2156-2327,2571-2728
    assert_eq!(rejection([]), Some(IndexTaskRejection::NoAccessPaths));
    assert_eq!(
        rejection([DataSourceAccessPath::Table]),
        Some(IndexTaskRejection::TablePath)
    );
    assert_eq!(
        rejection([DataSourceAccessPath::IndexMerge]),
        Some(IndexTaskRejection::IndexMergePath)
    );
    assert_eq!(
        rejection([DataSourceAccessPath::Index(
            IndexAccessPath::from_source_index_scan(
                candidate(1),
                3.0,
                PointGetAdmission::NotEligible
            )
            .with_store(AccessPathStore::TiFlash),
        )]),
        Some(IndexTaskRejection::TiFlashStore)
    );
    assert_eq!(
        rejection([DataSourceAccessPath::Index(
            IndexAccessPath::from_source_index_scan(
                candidate(1),
                3.0,
                PointGetAdmission::NotEligible
            )
            .with_read_shape(IndexReadShape::DoubleRead),
        )]),
        Some(IndexTaskRejection::DoubleRead)
    );
    assert_eq!(
        rejection([DataSourceAccessPath::Index(
            IndexAccessPath::from_source_index_scan(
                candidate(1),
                3.0,
                PointGetAdmission::NotEligible
            )
            .with_multi_valued(true),
        )]),
        Some(IndexTaskRejection::MultiValuedIndex)
    );

    for ordering in [
        IndexOrderingRequirement::KeepOrder,
        IndexOrderingRequirement::PartialOrder,
        IndexOrderingRequirement::MergeSort,
    ] {
        assert_eq!(
            source([point_path(candidate(1))]).build_index_task(
                IndexTaskProperty::new(TaskType::CopSingleRead).with_ordering(ordering),
            ),
            IndexTask::Invalid(IndexTaskRejection::RequiredOrdering)
        );
    }
    assert_eq!(
        source([point_path(candidate(1))])
            .build_index_task(IndexTaskProperty::new(TaskType::CopMultiRead))
            .rejection(),
        Some(IndexTaskRejection::UnsupportedTaskType)
    );
    assert_eq!(
        source([point_path(candidate(1))])
            .build_index_task(IndexTaskProperty::new(TaskType::Root))
            .rejection(),
        Some(IndexTaskRejection::RootIndexReaderUnsupported)
    );
}

#[test]
fn source_expected_count_requires_exact_upstream_cardinality() {
    // pkg/planner/core/operator/physicalop/physical_index_scan.go:668-691
    let property = IndexTaskProperty::new(TaskType::CopSingleRead).with_expected_cnt(5.0);
    assert_eq!(
        source([point_path(candidate(1))])
            .build_index_task(property)
            .rejection(),
        Some(IndexTaskRejection::ExpectedCountUnsupported)
    );

    let path =
        IndexAccessPath::from_source_index_scan(candidate(1), 3.0, PointGetAdmission::NotEligible)
            .with_expected_count_rows(ExpectedCountRows::new(5.0, 2.0));
    let task = source([DataSourceAccessPath::Index(path)]).build_index_task(property);
    assert_eq!(
        task.index_plan()
            .expect("exact upstream adjustment is representable")
            .estimated_rows(),
        2.0
    );
    assert_eq!(
        source([point_path(candidate(1))])
            .build_index_task(
                IndexTaskProperty::new(TaskType::CopSingleRead).with_expected_cnt(f64::NAN),
            )
            .rejection(),
        Some(IndexTaskRejection::ExpectedCountUnsupported)
    );
}

#[test]
fn source_index_scan_cost_uses_go_row_size_boundaries() {
    // pkg/planner/core/plan_cost_ver2.go:1102-1111
    let mut below_one = candidate(0);
    below_one.row_size = 0.5;
    let below_one = PhysicalIndexScanPlan::init(74, 0, &below_one, 3.0);
    assert_eq!(below_one.cost(), 0.0);

    let mut one = candidate(0);
    one.row_size = 1.0;
    let one = PhysicalIndexScanPlan::init(75, 0, &one, 3.0);
    assert_eq!(one.cost(), 0.0);

    let mut two = candidate(0);
    two.row_size = 2.0;
    let two = PhysicalIndexScanPlan::init(76, 0, &two, 3.0);
    assert!((two.cost() - 3.0 * 40.7).abs() < f64::EPSILON);

    let mut forty_eight = candidate(-12);
    forty_eight.row_size = 48.0;
    forty_eight.point_statistics.topn_count = Some(1);
    forty_eight.index_scan_cost_factor = 1.25;
    let forty_eight = PhysicalIndexScanPlan::init(77, 0, &forty_eight, 1.0);
    assert!(
        (forty_eight.cost() - (48.0_f64.log2() * 40.7 * 1.25 - 0.000_012)).abs() < f64::EPSILON
    );

    let current = PhysicalIndexScanPlan::init(78, 0, &candidate(12), 3.0);
    let equal_cost = PhysicalIndexScanPlan::init(79, 0, &candidate(12), 3.0);
    assert_eq!(
        PhysicalIndexScanPlan::choose_lower_cost(current, equal_cost)
            .plan()
            .id(),
        78
    );
}
