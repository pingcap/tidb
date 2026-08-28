// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use tidb_planner::{
    access_path::{
        DataSourceAccessPath, IndexAccessPath, PointGetAdmission, ResolvedTableDescriptor,
        ResolvedTableScanKind, TableAccessPath, TableScanExplainIdSuffix,
    },
    cardinality::{
        index_range_policy::{IndexRangeShape, RangeBoundKind},
        live_index_optimizer::{IndexPointStatistics, LiveIndexCandidate},
    },
    logical_data_source::LogicalDataSource,
    logical_data_source_task::IndexTaskProperty,
    physical::{PhysicalIndexScan, PhysicalPlan, PhysicalTableReader, PhysicalTableScan},
    task_type::TaskType,
    tikv_scan_spec::TiKvTableScanSpec,
};

#[test]
fn datasource_table_task_uses_the_wired_reader_and_scan_tree() {
    let spec = TiKvTableScanSpec::new(42, vec![]);
    let path = TableAccessPath::from_source_table_scan(
        ResolvedTableDescriptor::new(
            42,
            false,
            ResolvedTableScanKind::Range,
            TableScanExplainIdSuffix::IncludePlanId,
        ),
        spec.clone(),
        PointGetAdmission::NotEligible,
        12.0,
    )
    .expect("descriptor and pushdown identify the same table");
    let source = LogicalDataSource::new(91, -4, [DataSourceAccessPath::Table(path)]);

    let task = source.build_scan_read_task(IndexTaskProperty::new(TaskType::Root));
    let reader: &PhysicalTableReader = task
        .table_reader()
        .expect("an admitted table path becomes one root reader");
    let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref() else {
        panic!("the reader must own its wired physical table scan");
    };
    let scan: &PhysicalTableScan = scan;

    assert_eq!(scan.base.base.id(), 91);
    assert_eq!(scan.base.base.query_block_offset(), -4);
    assert_eq!(scan.pushdown(), Some(&spec));

    let cloned = PhysicalPlan::TableReader(reader.clone()).clone_plan();
    let PhysicalPlan::TableReader(cloned_reader) = cloned else {
        unreachable!();
    };
    let cloned_scan = cloned_reader
        .table_scan_plan()
        .expect("cache cloning retains the wired scan tree");
    assert_eq!(cloned_scan.pushdown(), Some(&spec));
    assert_eq!(cloned_scan.descriptor(), scan.descriptor());
}

#[test]
fn datasource_index_task_uses_the_wired_index_scan() {
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
            topn_count: Some(1),
            cms_count: None,
            histogram_count: 0,
        },
        row_size: 8.0,
        scan_factor: 1.0,
        index_scan_cost_factor: 1.0,
    };
    let path =
        IndexAccessPath::from_source_index_scan(candidate, 1.0, PointGetAdmission::NotEligible);
    let source = LogicalDataSource::new(92, -5, [DataSourceAccessPath::Index(path)]);

    let task = source.build_index_task(IndexTaskProperty::new(TaskType::CopSingleRead));
    let scan: &PhysicalIndexScan = task
        .index_plan()
        .expect("an admitted index path becomes one cop index scan");

    assert_eq!(scan.base.base.id(), 92);
    assert_eq!(scan.estimated_rows(), 1.0);

    let cloned = PhysicalPlan::IndexScan(scan.clone()).clone_plan();
    let PhysicalPlan::IndexScan(cloned_scan) = cloned else {
        unreachable!();
    };
    assert_eq!(cloned_scan.estimated_rows(), scan.estimated_rows());
    assert_eq!(cloned_scan.cost(), scan.cost());
    assert_eq!(cloned_scan.ranges(), scan.ranges());
}
