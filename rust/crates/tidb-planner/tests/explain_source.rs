// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//! Planner-leaf assertions for EXPLAIN formatting and LOAD DATA fields.
//!
//! SQL-to-plan construction is intentionally absent: that consumer depends on
//! the real PlanBuilder, physical optimizer, and FlattenPhysicalPlan bridge.

use tidb_ast::{DmlStmt, Stmt};
use tidb_planner::explain::{
    new_line_fields_info, Explain, ExplainContext, ExplainFormat, ExplainOperator, ExplainTask,
};

#[test]
fn explain_format_schema_and_context_leaf() {
    let cases = [
        ("brief", 5),
        ("dot", 1),
        ("hint", 1),
        ("row", 5),
        ("verbose", 6),
        ("traditional", 5),
        ("binary", 1),
        ("tidb_json", 1),
        ("cost_trace", 7),
        ("plan_cache", 5),
        ("plan_tree", 4),
    ];
    for (written, field_count) in cases {
        let format = ExplainFormat::parse(written).expect("valid Go format spelling");
        let explain = Explain::new(format, false, ExplainOperator::new("TableReader", 1));
        let mut context = ExplainContext {
            in_explain_stmt: false,
            explain_format: written.to_owned(),
        };
        let schema = explain
            .prepare_schema(&mut context)
            .expect("format owns a non-analyze schema");
        assert!(context.in_explain_stmt);
        assert_eq!(context.explain_format, written);
        assert_eq!(schema.field_names.len(), field_count, "{written}");
    }
}

#[test]
fn plan_tree_renderer_leaf() {
    let task = ExplainTask::Cop {
        request: "cop".to_owned(),
        store: "tikv".to_owned(),
    };
    let mut build = ExplainOperator::new("IndexRangeScan", 2)
        .with_task(task.clone())
        .with_access_object("table:t, index:idx(a)")
        .with_operator_info("range:[5,5], keep order:false, stats:pseudo");
    build.label = "(Build)".to_owned();
    let mut probe = ExplainOperator::new("TableRowIDScan", 3)
        .with_task(task)
        .with_access_object("table:t")
        .with_operator_info("keep order:false, stats:pseudo");
    probe.label = "(Probe)".to_owned();
    let root = ExplainOperator::new("IndexLookUp", 1).with_children([build, probe]);
    let mut explain = Explain::new(ExplainFormat::PlanTree, false, root);
    let rows = explain
        .render_result(&mut ExplainContext::default())
        .expect("render preplanned tree");

    assert!(rows.iter().all(|row| row.len() == 4));
    assert_eq!(rows[0][0], "IndexLookUp");
    assert_eq!(rows[1][0], "├─IndexRangeScan(Build)");
    assert_eq!(rows[2][0], "└─TableRowIDScan(Probe)");
    assert_eq!(rows[1][1], "cop[tikv]");
    assert_eq!(rows[1][2], "table:t, index:idx(a)");
    assert_eq!(rows[1][3], "range:[5,5], keep order:false, stats:pseudo");

    let analyze = Explain::new(
        ExplainFormat::PlanTree,
        true,
        ExplainOperator::new("TableReader", 1),
    );
    let error = analyze
        .prepare_schema(&mut ExplainContext::default())
        .expect_err("PLAN_TREE analyze must fail");
    assert!(error.to_string().contains("plan_tree"));
}

#[test]
fn new_line_fields_info_all_go_vectors() {
    let cases = [
        (
            "load data infile 'a' into table t",
            ("\t", "", "\\", false, "", "\n"),
        ),
        (
            "load data infile 'a' into table t fields terminated by 'a'",
            ("a", "", "\\", false, "", "\n"),
        ),
        (
            "load data infile 'a' into table t fields optionally enclosed by 'a'",
            ("\t", "a", "\\", true, "", "\n"),
        ),
        (
            "load data infile 'a' into table t fields enclosed by 'a'",
            ("\t", "a", "\\", false, "", "\n"),
        ),
        (
            "load data infile 'a' into table t fields escaped by 'a'",
            ("\t", "", "a", false, "", "\n"),
        ),
        (
            "load data infile 'a' into table t lines starting by 'a'",
            ("\t", "", "\\", false, "a", "\n"),
        ),
        (
            "load data infile 'a' into table t lines terminated by 'aa'",
            ("\t", "", "\\", false, "", "aa"),
        ),
    ];

    for (sql, expected) in cases {
        let Stmt::Dml(dml) = tidb_parser::parse(sql).expect("parse source LOAD DATA") else {
            panic!("expected DML")
        };
        let DmlStmt::LoadData(load) = dml.as_ref() else {
            panic!("expected LOAD DATA")
        };
        let actual = new_line_fields_info(Some(&load.fields), Some(&load.lines));
        assert_eq!(actual.fields_terminated_by, expected.0, "{sql}");
        assert_eq!(actual.fields_enclosed_by, expected.1, "{sql}");
        assert_eq!(actual.fields_escaped_by, expected.2, "{sql}");
        assert_eq!(actual.fields_opt_enclosed, expected.3, "{sql}");
        assert_eq!(actual.lines_starting_by, expected.4, "{sql}");
        assert_eq!(actual.lines_terminated_by, expected.5, "{sql}");
    }
}
