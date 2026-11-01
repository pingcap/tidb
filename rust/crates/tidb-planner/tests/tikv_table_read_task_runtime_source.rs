// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#![allow(missing_docs)]

use tidb_planner_core_operator_physicalop::{PhysicalTableReader, PhysicalTableScan};

use tidb_planner_core_operator_logicalop::tikv_scan_spec::TiKvTableScanSpec;

#[test]
fn raw_dag_table_scan_cannot_become_a_table_reader() {
    let raw = PhysicalTableScan::init(3, 0, TiKvTableScanSpec::new(7, vec![]));
    assert_eq!(raw.descriptor(), None);
    assert_eq!(raw.resolved_explain_id(false), None);
    assert_eq!(raw.resolved_is_common_handle(), None);
    assert_eq!(
        PhysicalTableReader::from_table_scan(raw)
            .expect_err("raw DAG scans must fail closed at reader conversion")
            .to_string(),
        "table reader requires a source-resolved table descriptor"
    );
}

