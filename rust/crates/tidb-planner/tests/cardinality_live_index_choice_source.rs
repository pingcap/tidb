// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use tidb_planner_cardinality::index_range_policy::{IndexRangeShape, RangeBoundKind};

use tidb_planner_cardinality::live_index_optimizer::{IndexPointStatistics, LiveIndexCandidate};

use tidb_planner_core_operator_physicalop::PhysicalIndexScan;

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

#[test]
fn source_index_scan_cost_uses_go_row_size_boundaries() {
    // pkg/planner/core/plan_cost_ver2.go:1102-1111
    let mut below_one = candidate(0);
    below_one.row_size = 0.5;
    let below_one = PhysicalIndexScan::init(74, 0, &below_one, 3.0);
    assert_eq!(below_one.cost(), 0.0);

    let mut one = candidate(0);
    one.row_size = 1.0;
    let one = PhysicalIndexScan::init(75, 0, &one, 3.0);
    assert_eq!(one.cost(), 0.0);

    let mut two = candidate(0);
    two.row_size = 2.0;
    let two = PhysicalIndexScan::init(76, 0, &two, 3.0);
    assert!((two.cost() - 3.0 * 40.7).abs() < f64::EPSILON);

    let mut forty_eight = candidate(-12);
    forty_eight.row_size = 48.0;
    forty_eight.point_statistics.topn_count = Some(1);
    forty_eight.index_scan_cost_factor = 1.25;
    let forty_eight = PhysicalIndexScan::init(77, 0, &forty_eight, 1.0);
    assert!(
        (forty_eight.cost() - (48.0_f64.log2() * 40.7 * 1.25 - 0.000_012)).abs() < f64::EPSILON
    );
}
