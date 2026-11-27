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

//! Go master `pkg/planner/core/casetest/mview/mv_refresh_test.go`, the cases
//! that plan `RefreshMaterializedViewImplementStmt` over a mock schema: the
//! refresh plans this tier builds (`plan_mview_delta_merge`,
//! `plan_mview_complete_delta_apply`) rendered as Go's brief EXPLAIN.

use tidb_datatype::FieldTypeCode;
use tidb_model::TableInfo;
use tidb_planner::common_plans::MViewDeltaMerge;
use tidb_planner::mview::{AggKind, BuildOptions};
use tidb_sessionctx::explain_format::ExplainFormat;

use super::planner_mview_tests::{base_and_mlog, col_at, index, mk_col, mock_info_schema, mview_table, mview_test_context};

/// Go `mviewRefreshTestContext`'s mock transaction start TS, which bounds
/// the retained mlog window when no transaction manager provides one.
const MOCK_START_TS: u64 = 1 << 18;

/// One EXPLAIN row as Go's `explain.Rows` holds it.
fn row_cells(row: &[tidb_datatype::Datum]) -> Vec<String> {
    row.iter()
        .map(|datum| match datum {
            tidb_datatype::Datum::Null => "<nil>".to_owned(),
            tidb_datatype::Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
            tidb_datatype::Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
            other => format!("{other:?}"),
        })
        .collect()
}

/// Go `plannercore.Explain{TargetPlan: <FAST implement plan>, Format: brief}.RenderResult()`.
fn explain_fast_refresh(tables: &[&TableInfo], view: &str, options: BuildOptions, mlog_retained_lower_tso: u64) -> Vec<Vec<String>> {
    let is = mock_info_schema(tables);
    let mut plan = MViewDeltaMerge {
        database: "test".to_owned(),
        name: view.to_owned(),
        options,
        mlog_retained_lower_tso,
        registered_mlog_ids: Vec::new(),
        planned: None,
    };
    // Go's mock store under the mock context.
    let ctx = mview_test_context()
        .with_txn_store(Box::new(tidb_store_driver_txn::cluster_storage_test_util::empty_mock_store()));
    let no_snapshot = |_: u64| -> Result<tidb_planner::common_plans::DataReaderSnapshot, super::DriverError> {
        panic!("the test reads no AS OF snapshot")
    };
    super::mview_delta_merge::optimize_mview_delta_merge(&mut plan, &is, "test", &ctx, MOCK_START_TS, &no_snapshot).unwrap();
    let (_, rows) = crate::explain_mview_delta_merge(&plan, &is, &ctx, ExplainFormat::Brief).unwrap();
    rows.iter().map(|row| row_cells(row)).collect()
}

fn rows(expected: &[[&str; 5]]) -> Vec<Vec<String>> {
    expected.iter().map(|row| row.iter().map(|cell| (*cell).to_owned()).collect()).collect()
}

#[test]
fn test_explain_refresh_mv_fast_plan_tree() {
    let (base_id, mlog_id, mv_id) = (101, 102, 103);
    let (mut base, mut mlog) = base_and_mlog(base_id, mlog_id);
    base.columns = vec![mk_col(1, "a", 0, FieldTypeCode::Long)].into();
    mlog.columns = vec![
        mk_col(1, "a", 0, FieldTypeCode::Long),
        mk_col(2, tidb_model::table::MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME, 1, FieldTypeCode::Varchar),
        mk_col(3, tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME, 2, FieldTypeCode::Tiny),
    ]
    .into();
    let mv = mview_table(
        mv_id,
        "mv_tbl_explain",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        vec![base_id],
        "select a, count(1) from t group by a",
    );
    let got = explain_fast_refresh(&[&base, &mlog, &mv], "mv_tbl_explain", BuildOptions::default(), 0);
    assert_eq!(
        got,
        rows(&[
            ["MViewDeltaMerge", "N/A", "root", "", "agg_deps:[count(*)@1->[0]]"],
            ["└─HashJoin", "8000.00", "root", "", "left outer join, left side:HashAgg, equal:[nulleq(test.$mlog$t.a, test.mv_tbl_explain.a)]"],
            ["  ├─HashAgg(Build)", "6400.00", "root", "", "group by:test.$mlog$t.a, funcs:sum_int(Column#11)->Column#6, funcs:firstrow(test.$mlog$t.a)->test.$mlog$t.a"],
            ["  │ └─TableReader", "6400.00", "root", "", "data:HashAgg"],
            ["  │   └─HashAgg", "6400.00", "cop[tikv]", "", "group by:test.$mlog$t.a, funcs:sum_int(test.$mlog$t._mlog$_old_new)->Column#11"],
            ["  │     └─Selection", "8000.00", "cop[tikv]", "", "gt(test.$mlog$t._tidb_commit_ts, 0)"],
            ["  │       └─TableFullScan", "10000.00", "cop[tikv]", "table:$mlog$t", "keep order:false, stats:pseudo"],
            ["  └─TableReader(Probe)", "10000.00", "root", "", "data:TableFullScan"],
            ["    └─TableFullScan", "10000.00", "cop[tikv]", "table:mv", "keep order:false, stats:pseudo"],
        ])
    );
}

#[test]
fn test_explain_refresh_mv_fast_plan_tree_min_max() {
    let (base_id, mlog_id, mv_id) = (201, 202, 203);
    let (mut base, mlog) = base_and_mlog(base_id, mlog_id);
    base.indices = vec![index(1, "idx_a", &[("a", 0)], false)].into();
    col_at(&base, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    col_at(&mlog, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let mv = mview_table(
        mv_id,
        "mv_tbl_explain_minmax",
        vec![
            mk_col(1, "a", 0, FieldTypeCode::Long),
            mk_col(2, "cnt", 1, FieldTypeCode::LongLong),
            mk_col(3, "mx", 2, FieldTypeCode::Long),
            mk_col(4, "mn", 3, FieldTypeCode::Long),
        ],
        vec![base_id],
        "select a, count(1), max(b), min(b) from t group by a",
    );
    let got = explain_fast_refresh(&[&base, &mlog, &mv], "mv_tbl_explain_minmax", BuildOptions::default(), 0);
    assert_eq!(
        got,
        rows(&[
            ["MViewDeltaMerge", "N/A", "root", "", "agg_deps:[count(*)@1->[0], max(b)@2->[1,2,3,4], min(b)@3->[5,6,7,8]], full_update:index_lookup"],
            ["├─HashJoin", "8000.00", "root", "", "left outer join, left side:HashAgg, equal:[nulleq(test.$mlog$t.a, test.mv_tbl_explain_minmax.a)]"],
            ["│ ├─HashAgg(Build)", "6400.00", "root", "", "group by:Column#31, funcs:sum_int(Column#22)->Column#7, funcs:max(Column#23)->Column#8, funcs:max_count(Column#24)->Column#9, funcs:max(Column#25)->Column#10, funcs:max_count(Column#26)->Column#11, funcs:min(Column#27)->Column#12, funcs:min_count(Column#28)->Column#13, funcs:min(Column#29)->Column#14, funcs:min_count(Column#30)->Column#15, funcs:firstrow(Column#31)->test.$mlog$t.a"],
            ["│ │ └─Projection", "8000.00", "root", "", "test.$mlog$t._mlog$_old_new->Column#22, if(eq(test.$mlog$t._mlog$_old_new, 1), test.$mlog$t.b, <nil>)->Column#23, if(eq(test.$mlog$t._mlog$_old_new, 1), test.$mlog$t.b, <nil>)->Column#24, if(eq(test.$mlog$t._mlog$_old_new, -1), test.$mlog$t.b, <nil>)->Column#25, if(eq(test.$mlog$t._mlog$_old_new, -1), test.$mlog$t.b, <nil>)->Column#26, if(eq(test.$mlog$t._mlog$_old_new, 1), test.$mlog$t.b, <nil>)->Column#27, if(eq(test.$mlog$t._mlog$_old_new, 1), test.$mlog$t.b, <nil>)->Column#28, if(eq(test.$mlog$t._mlog$_old_new, -1), test.$mlog$t.b, <nil>)->Column#29, if(eq(test.$mlog$t._mlog$_old_new, -1), test.$mlog$t.b, <nil>)->Column#30, test.$mlog$t.a->Column#31"],
            ["│ │   └─TableReader", "8000.00", "root", "", "data:Selection"],
            ["│ │     └─Selection", "8000.00", "cop[tikv]", "", "gt(test.$mlog$t._tidb_commit_ts, 0)"],
            ["│ │       └─TableFullScan", "10000.00", "cop[tikv]", "table:$mlog$t", "keep order:false, stats:pseudo"],
            ["│ └─TableReader(Probe)", "10000.00", "root", "", "data:TableFullScan"],
            ["│   └─TableFullScan", "10000.00", "cop[tikv]", "table:mv", "keep order:false, stats:pseudo"],
            ["└─HashAgg", "1.00", "root", "", "group by:test.t.a, funcs:max(test.t.b)->Column#40, funcs:min(test.t.b)->Column#41, funcs:firstrow(test.t.a)->test.t.a"],
            ["  └─IndexLookUp", "1.00", "root", "", ""],
            ["    ├─IndexRangeScan(Build)", "1.00", "cop[tikv]", "table:t, index:idx_a(a)", "range: decided by [eq(test.t.a, test.t.a)], keep order:false, stats:pseudo"],
            ["    └─TableRowIDScan(Probe)", "1.00", "cop[tikv]", "table:t", "keep order:false, stats:pseudo"],
        ])
    );
}

/// Go `plannercore.Explain{TargetPlan: <COMPLETE DELTA APPLY implement plan>, Format: brief}.RenderResult()`.
fn explain_complete_delta_apply(tables: &[&TableInfo], mv: &TableInfo) -> Vec<Vec<String>> {
    let is = mock_info_schema(tables);
    let diff = tidb_planner::mview::build_complete_diff_source(&super::planner_mview_tests::parse, &is, mv).unwrap();
    let mut plan = tidb_planner::common_plans::MViewCompleteDeltaApply {
        database: "test".to_owned(),
        name: mv.name.original().to_owned(),
        diff,
        root: None,
    };
    let ctx = mview_test_context()
        .with_txn_store(Box::new(tidb_store_driver_txn::cluster_storage_test_util::empty_mock_store()));
    super::mview_delta_apply::optimize_mview_complete_delta_apply(&mut plan, &is, "test", &ctx).unwrap();
    let (_, rows) = crate::explain_mview_complete_delta_apply(&plan, &is, &ctx, ExplainFormat::Brief).unwrap();
    rows.iter().map(|row| row_cells(row)).collect()
}

#[test]
fn test_explain_refresh_m_view_complete_delta_plan_tree() {
    let (base_id, mv_id) = (1001, 1002);
    let base = super::planner_mview_tests::table(
        base_id,
        "t",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "b", 1, FieldTypeCode::Long)],
    );
    col_at(&base, 0).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let mut mv = mview_table(
        mv_id,
        "mv_tbl_complete_diff",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        vec![base_id],
        "select a, count(1) from t group by a",
    );
    mv.pk_is_handle = true;
    col_at(&mv, 0)
        .write()
        .field_type
        .add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL | tidb_datatype::FieldTypeFlags::PRI_KEY);
    let got = explain_complete_delta_apply(&[&base, &mv], &mv);
    assert!(!got.is_empty());
    let (mut has_apply, mut has_projection, mut has_selection, mut has_full_outer_join) = (false, false, false, false);
    for row in &got {
        if row[0].ends_with("MViewCompleteDeltaApply") {
            has_apply = true;
        } else if row[0].ends_with("Projection") {
            has_projection = true;
        } else if row[0].ends_with("Selection") {
            has_selection = true;
        } else if row[0].ends_with("HashJoin") && row[4].contains("full outer join") {
            has_full_outer_join = true;
        }
    }
    assert!(has_apply && has_projection && has_selection && has_full_outer_join, "{got:#?}");
}

/// Go `builder.Build(<FAST implement stmt>)`: this tier's merge plan (the
/// build, then the optimized merge source with its full-update lookup).
fn build_fast_refresh(
    is: &tidb_infoschema::SessionExtendedInfoSchema,
    view: &str,
    retained_upper_tso: u64,
) -> Result<(tidb_planner::mview::BuildResult, bool, Option<tidb_planner::common_plans::MViewFullUpdateLookup>), super::DriverError> {
    let mut plan = MViewDeltaMerge {
        database: "test".to_owned(),
        name: view.to_owned(),
        options: BuildOptions::default(),
        mlog_retained_lower_tso: 0,
        registered_mlog_ids: Vec::new(),
        planned: None,
    };
    let ctx = mview_test_context()
        .with_txn_store(Box::new(tidb_store_driver_txn::cluster_storage_test_util::empty_mock_store()));
    let no_snapshot = |_: u64| -> Result<tidb_planner::common_plans::DataReaderSnapshot, super::DriverError> {
        panic!("the test reads no AS OF snapshot")
    };
    super::mview_delta_merge::optimize_mview_delta_merge(&mut plan, is, "test", &ctx, retained_upper_tso, &no_snapshot)?;
    let planned = *plan.planned.expect("the optimizer plans the merge");
    Ok((planned.build, planned.mlog_empty, planned.full_update))
}

#[test]
fn test_build_refresh_mv_fast_plan() {
    let (base_id, mlog_id, mv_id) = (1, 2, 3);
    let (base, mlog) = base_and_mlog(base_id, mlog_id);
    let mv = mview_table(
        mv_id,
        "mv_tbl",
        vec![
            mk_col(1, "a", 0, FieldTypeCode::Long),
            mk_col(2, "cnt", 1, FieldTypeCode::LongLong),
            mk_col(3, "cnt_b", 2, FieldTypeCode::LongLong),
            mk_col(4, "s", 3, FieldTypeCode::LongLong),
        ],
        vec![base_id],
        "select a, count(1), count(b), sum(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let (merge_plan, _, _) = build_fast_refresh(&is, "mv_tbl", MOCK_START_TS).unwrap();
    assert_eq!(mv_id, merge_plan.mv_table_id);
    assert_eq!(base_id, merge_plan.base_table_id);
    assert_eq!(mlog_id, merge_plan.mlog_table_id);
    assert_eq!(mv.columns.len(), merge_plan.mv_column_count);
    assert_eq!(vec![0], merge_plan.group_key_mv_offsets);
    assert_eq!(1, merge_plan.count_star_mv_offset);
    assert_eq!(merge_plan.agg_infos.len(), 3);
    let (mut has_count_star, mut has_count_expr, mut has_sum) = (false, false, false);
    for agg_info in &merge_plan.agg_infos {
        match agg_info.kind {
            AggKind::CountStar => {
                has_count_star = true;
                assert_eq!(vec![0], agg_info.dependencies);
            }
            AggKind::Count => {
                has_count_expr = true;
                assert_eq!("b", agg_info.arg_col_name);
                assert_eq!(vec![1], agg_info.dependencies);
            }
            AggKind::Sum => {
                has_sum = true;
                assert_eq!("b", agg_info.arg_col_name);
                assert_eq!(vec![2, 5], agg_info.dependencies);
            }
            _ => {}
        }
    }
    assert!(has_count_star && has_count_expr && has_sum);

    let rows = explain_fast_refresh(&[&base, &mlog, &mv], "mv_tbl", BuildOptions::default(), 0);
    assert_eq!(
        rows[0],
        ["MViewDeltaMerge", "N/A", "root", "", "agg_deps:[count(*)@1->[0], count(b)@2->[1], sum(b)@3->[2,5]]"]
    );

    // Go `mviewRefreshZeroStartTSContext`: a transaction whose start TS is
    // zero (this tier's retained upper bound is the transaction's TS).
    let Err(err) = build_fast_refresh(&is, "mv_tbl", 0) else {
        panic!("a zero start TS plans");
    };
    assert_eq!(err.to_string(), "RefreshMaterializedViewImplementStmt: invalid transaction start tso");
}

#[test]
fn test_build_refresh_mv_fast_as_of_timestamp_plan_with_min_max_carries_full_update_snapshot() {
    let (base_id, mlog_id, mv_id) = (1, 2, 3);
    let (mut base, mlog) = base_and_mlog(base_id, mlog_id);
    base.indices = vec![index(1, "idx_a", &[("a", 0)], false)].into();
    for table in [&base, &mlog] {
        for i in 0..2 {
            col_at(table, i).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        }
    }
    let mv = mview_table(
        mv_id,
        "mv_refresh_asof_plan",
        vec![
            mk_col(1, "a", 0, FieldTypeCode::Long),
            mk_col(2, "cnt", 1, FieldTypeCode::LongLong),
            mk_col(3, "mx", 2, FieldTypeCode::Long),
            mk_col(4, "mn", 3, FieldTypeCode::Long),
        ],
        vec![base_id],
        "select a, count(1), max(b), min(b) from t group by a",
    );
    let tables = [&base, &mlog, &mv];
    let is = mock_info_schema(&tables);
    // Go `oracle.GoTimeToTS(time.Now().UTC().Add(time.Second))`.
    let target_tso = compose_ts(START_PHYSICAL + 1000);
    let mut plan = MViewDeltaMerge {
        database: "test".to_owned(),
        name: "mv_refresh_asof_plan".to_owned(),
        options: BuildOptions { from_ts: 1, to_ts: target_tso },
        mlog_retained_lower_tso: 0,
        registered_mlog_ids: Vec::new(),
        planned: None,
    };
    let ctx = mview_test_context()
        .with_txn_store(Box::new(tidb_store_driver_txn::cluster_storage_test_util::empty_mock_store()));
    // The session's `staleread.GetSessionSnapshotInfoSchema` at the target.
    let requested = std::cell::Cell::new(0);
    let snapshot_at = |to_ts: u64| -> Result<tidb_planner::common_plans::DataReaderSnapshot, super::DriverError> {
        requested.set(to_ts);
        Ok(tidb_planner::common_plans::DataReaderSnapshot { ts: to_ts, info_schema: mock_info_schema(&tables) })
    };
    super::mview_delta_merge::optimize_mview_delta_merge(&mut plan, &is, "test", &ctx, compose_ts(START_PHYSICAL), &snapshot_at)
        .unwrap();
    let planned = plan.planned.expect("the optimizer plans the merge");
    let full_update = planned.full_update.as_ref().expect("FullUpdateInnerSource");
    let snapshot = full_update.snapshot.as_ref().expect("FullUpdateSnapshot");
    assert_eq!(target_tso, requested.get());
    assert_eq!(target_tso, snapshot.ts);
    assert!(snapshot.info_schema.table_info_by_id(base_id).is_some());
}

#[test]
fn test_build_refresh_mv_fast_plan_with_min_max_has_full_update() {
    let (base_id, mlog_id, mv_id) = (10001, 10002, 10003);
    let (mut base, mlog) = base_and_mlog(base_id, mlog_id);
    base.indices = vec![index(1, "idx_a", &[("a", 0)], false)].into();
    col_at(&base, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    col_at(&mlog, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let mv = mview_table(
        mv_id,
        "mv_tbl_minmax",
        vec![
            mk_col(1, "a", 0, FieldTypeCode::Long),
            mk_col(2, "cnt", 1, FieldTypeCode::LongLong),
            mk_col(3, "mx", 2, FieldTypeCode::Long),
            mk_col(4, "mn", 3, FieldTypeCode::Long),
            mk_col(5, "s", 4, FieldTypeCode::LongLong),
        ],
        vec![base_id],
        "select a, count(1), max(b), min(b), sum(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let (merge_plan, mlog_empty, full_update) = build_fast_refresh(&is, "mv_tbl_minmax", MOCK_START_TS).unwrap();
    let full_update = full_update.expect("FullUpdateInnerSource");
    assert!(!mlog_empty);
    let min_max_count =
        merge_plan.agg_infos.iter().filter(|ai| matches!(ai.kind, AggKind::Min | AggKind::Max)).count();
    assert_eq!(merge_plan.group_key_mv_offsets.len() + min_max_count, full_update.inner_column_count());
    assert_eq!(full_update.key_off2_idx_off().len(), merge_plan.group_key_mv_offsets.len());
    assert_eq!(full_update.key_result_col_idxes.len(), merge_plan.group_key_mv_offsets.len());
    assert_eq!(full_update.output_mv_offsets.len(), full_update.inner_column_count());
    for (i, &key_result_col_idx) in full_update.key_result_col_idxes.iter().enumerate() {
        assert!(key_result_col_idx < full_update.inner_column_count());
        assert_eq!(merge_plan.group_key_mv_offsets[i], full_update.output_mv_offsets[key_result_col_idx]);
    }
    for ai in &merge_plan.agg_infos {
        if matches!(ai.kind, AggKind::Min | AggKind::Max) {
            assert_eq!(1, full_update.output_mv_offsets.iter().filter(|&&offset| offset == ai.mv_offset).count());
        }
    }
    let rows = explain_fast_refresh(&[&base, &mlog, &mv], "mv_tbl_minmax", BuildOptions::default(), 0);
    assert!(rows[0][4].contains("full_update:index_lookup"), "{rows:?}");

    for (predicate, empty) in [
        ("false", true),
        ("a = 1 and a = 2", true),
        // "Contradictory predicates not replaced by Dual keep the existing lookup path."
        ("a > 1 and a < 0", false),
        ("a is null and a is not null", false),
    ] {
        mv.materialized_view.as_ref().unwrap().write().sql_content =
            format!("select a, count(1), max(b), min(b), sum(b) from t where {predicate} group by a");
        let is = mock_info_schema(&[&base, &mlog, &mv]);
        let (_, mlog_empty, full_update) = build_fast_refresh(&is, "mv_tbl_minmax", MOCK_START_TS).unwrap();
        assert_eq!(empty, mlog_empty, "{predicate}");
        assert_eq!(empty, full_update.is_none(), "{predicate}");
    }
}

/// Go `builder.Build(<COMPLETE DELTA APPLY implement stmt>)`.
fn build_complete_delta_apply(tables: &[&TableInfo], mv: &TableInfo, ctx: &crate::StmtContext) -> (tidb_planner::common_plans::MViewCompleteDeltaApply, String) {
    let is = mock_info_schema(tables);
    let diff = tidb_planner::mview::build_complete_diff_source(&super::planner_mview_tests::parse, &is, mv).unwrap();
    let mut plan = tidb_planner::common_plans::MViewCompleteDeltaApply {
        database: "test".to_owned(),
        name: mv.name.original().to_owned(),
        diff,
        root: None,
    };
    super::mview_delta_apply::optimize_mview_complete_delta_apply(&mut plan, &is, "test", ctx).unwrap();
    let physical = plan.root.clone().expect("the optimizer plans the source");
    let tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(root) = &physical else {
        panic!("not an MViewCompleteDeltaApply root");
    };
    assert!(root.select_plan.is_some(), "Source");
    let explain_info = root.explain_info.clone().unwrap_or_default();
    (plan, explain_info)
}

fn complete_apply_tables(base_id: i64, mv_id: i64, mv_name: &str, pk_is_handle: bool) -> (TableInfo, TableInfo) {
    let base = super::planner_mview_tests::table(
        base_id,
        "t",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "b", 1, FieldTypeCode::Long)],
    );
    let mut mv = mview_table(
        mv_id,
        mv_name,
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        vec![base_id],
        "select a, count(1) from t group by a",
    );
    if pk_is_handle {
        col_at(&base, 0).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        mv.pk_is_handle = true;
        col_at(&mv, 0)
            .write()
            .field_type
            .add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL | tidb_datatype::FieldTypeFlags::PRI_KEY);
    }
    col_at(&mv, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    (base, mv)
}

#[test]
fn test_build_refresh_m_view_complete_delta_apply_plan() {
    let (base, mv) = complete_apply_tables(1101, 1102, "mv_tbl_complete_apply", true);
    let (apply_plan, explain_info) = build_complete_delta_apply(&[&base, &mv], &mv, &mview_test_context());
    let diff = &apply_plan.diff;
    assert_eq!(1102, diff.mv_table_id);
    assert_eq!(mv.columns.len(), diff.mv_column_count);
    assert_eq!(0, diff.op_col_offset);
    assert_eq!(0, diff.marker_mv_offset);
    assert_eq!(vec![0], diff.group_key_mv_offsets);
    assert_eq!(vec![1, 2], diff.current_row_offsets);
    assert_eq!(vec![3, 4], diff.recomputed_row_offsets);
    assert_eq!(1, diff.current_handle_cols.num_cols());
    assert_eq!(1, diff.current_handle_cols.get_col(0).index);
    assert_eq!(
        "op_offset:0, current_marker_offset:1, recomputed_marker_offset:3, current_group_keys_offset:[1], recomputed_group_keys_offset:[3], current_handle_offset:[1], current_row_offset:[1,2], recomputed_row_offset:[3,4]",
        explain_info
    );
}

#[test]
fn test_build_refresh_m_view_complete_delta_apply_plan_with_cascades_enabled() {
    let (base, mv) = complete_apply_tables(1101, 1102, "mv_tbl_complete_apply_cascades", true);
    let ctx = mview_test_context().with_cascades_planner(true);
    build_complete_delta_apply(&[&base, &mv], &mv, &ctx);
    // Go restores the session's cascades switches after planning the diff
    // source; this tier plans it under a copy of the statement context.
    assert!(ctx.enable_cascades_planner());
}

#[test]
fn test_build_refresh_m_view_complete_delta_apply_plan_nullable_group_key() {
    let (base, mv) = complete_apply_tables(1201, 1202, "mv_tbl_complete_apply_nullable_gk", false);
    let (apply_plan, explain_info) = build_complete_delta_apply(&[&base, &mv], &mv, &mview_test_context());
    let diff = &apply_plan.diff;
    assert_eq!(1202, diff.mv_table_id);
    assert_eq!(mv.columns.len(), diff.mv_column_count);
    assert_eq!(0, diff.op_col_offset);
    assert_eq!(1, diff.marker_mv_offset);
    assert_eq!(vec![0], diff.group_key_mv_offsets);
    assert_eq!(vec![2, 3], diff.current_row_offsets);
    assert_eq!(vec![4, 5], diff.recomputed_row_offsets);
    assert_eq!(1, diff.current_handle_cols.num_cols());
    assert_eq!(tidb_model::column::EXTRA_HANDLE_ID, diff.current_handle_cols.get_col(0).id);
    assert_eq!(1, diff.current_handle_cols.get_col(0).index);
    assert_eq!(
        "op_offset:0, current_marker_offset:3, recomputed_marker_offset:5, current_group_keys_offset:[2], recomputed_group_keys_offset:[4], current_handle_offset:[1], current_row_offset:[2,3], recomputed_row_offset:[4,5]",
        explain_info
    );
}

#[test]
fn test_build_refresh_mv_fast_sum_not_null_no_count_expr() {
    let (base_id, mlog_id, mv_id) = (11, 22, 33);
    let (base, mlog) = base_and_mlog(base_id, mlog_id);
    col_at(&base, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    col_at(&mlog, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let mv = mview_table(
        mv_id,
        "mv_tbl_not_null",
        vec![
            mk_col(1, "x", 0, FieldTypeCode::Long),
            mk_col(2, "cnt", 1, FieldTypeCode::LongLong),
            mk_col(3, "s", 2, FieldTypeCode::LongLong),
        ],
        vec![base_id],
        "select a, count(1), sum(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let (merge_plan, _, _) = build_fast_refresh(&is, "mv_tbl_not_null", MOCK_START_TS).unwrap();
    assert_eq!(mv_id, merge_plan.mv_table_id);
    assert_eq!(base_id, merge_plan.base_table_id);
    assert_eq!(mlog_id, merge_plan.mlog_table_id);
    assert_eq!(mv.columns.len(), merge_plan.mv_column_count);
    assert_eq!(vec![0], merge_plan.group_key_mv_offsets);
    assert_eq!(1, merge_plan.count_star_mv_offset);
    assert_eq!(merge_plan.agg_infos.len(), 2);
    let (mut has_count_star, mut has_sum) = (false, false);
    for agg_info in &merge_plan.agg_infos {
        match agg_info.kind {
            AggKind::CountStar => {
                has_count_star = true;
                assert_eq!(vec![0], agg_info.dependencies);
            }
            AggKind::Sum => {
                has_sum = true;
                assert_eq!("b", agg_info.arg_col_name);
                assert_eq!(vec![1], agg_info.dependencies);
            }
            _ => {}
        }
    }
    assert!(has_count_star && has_sum);
}

/// Go `handleColDef`.
struct HandleColDef {
    id: i64,
    name: &'static str,
    tp: FieldTypeCode,
}

/// Go `buildBaseAndMLogForHandleTest`.
fn build_base_and_m_log_for_handle_test(base_id: i64, mlog_id: i64, cols: &[HandleColDef]) -> (TableInfo, TableInfo) {
    let mut base_cols = Vec::new();
    let mut mlog_cols = Vec::new();
    let mut mlog_payload_cols = Vec::new();
    let mut max_col_id = 0;
    for (i, col) in cols.iter().enumerate() {
        max_col_id = max_col_id.max(col.id);
        base_cols.push(mk_col(col.id, col.name, i as i64, col.tp));
        mlog_cols.push(mk_col(col.id, col.name, i as i64, col.tp));
        mlog_payload_cols.push(tidb_ast::CiString::new(col.name));
    }
    mlog_cols.push(mk_col(
        max_col_id + 1,
        tidb_model::table::MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME,
        cols.len() as i64,
        FieldTypeCode::Varchar,
    ));
    mlog_cols.push(mk_col(
        max_col_id + 2,
        tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME,
        cols.len() as i64 + 1,
        FieldTypeCode::Tiny,
    ));
    let mut base = super::planner_mview_tests::table(base_id, "t", base_cols);
    base.materialized_view_base = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewBaseInfo {
        mlog_id,
        ..Default::default()
    }));
    let mut mlog = super::planner_mview_tests::table(mlog_id, "$mlog$t", mlog_cols);
    mlog.materialized_view_log = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewLogInfo {
        base_table_id: base_id,
        columns: mlog_payload_cols.into(),
        ..Default::default()
    }));
    (base, mlog)
}

#[test]
fn test_m_view_build_result_handle_cols() {
    struct HandleCase {
        name: &'static str,
        ids: (i64, i64, i64),
        base_cols: Vec<HandleColDef>,
        build_mv: fn(i64, i64) -> TableInfo,
        expect_handle_is_int: bool,
        expect_handle_idxs: Vec<i64>,
        expect_handle_layout: Vec<&'static str>,
    }
    let a = || HandleColDef { id: 1, name: "a", tp: FieldTypeCode::Long };
    let cases = vec![
        HandleCase {
            name: "tidb_rowid",
            ids: (1001, 1002, 1003),
            base_cols: vec![a()],
            build_mv: |base_id, mv_id| {
                mview_table(
                    mv_id,
                    "mv_tidb_rowid",
                    vec![mk_col(1, "x", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
                    vec![base_id],
                    "select a, count(1) from t group by a",
                )
            },
            expect_handle_is_int: true,
            expect_handle_idxs: vec![3],
            expect_handle_layout: vec!["__mview_mv_rowid"],
        },
        HandleCase {
            name: "int_handle",
            ids: (2001, 2002, 2003),
            base_cols: vec![a()],
            build_mv: |base_id, mv_id| {
                let mut pk_col = mk_col(2, "x", 1, FieldTypeCode::Long);
                pk_col.field_type.add_flags(tidb_datatype::FieldTypeFlags::PRI_KEY);
                pk_col.field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
                let mut mv = mview_table(
                    mv_id,
                    "mv_int_handle",
                    vec![mk_col(1, "cnt", 0, FieldTypeCode::LongLong), pk_col],
                    vec![base_id],
                    "select count(1), a from t group by a",
                );
                mv.pk_is_handle = true;
                mv
            },
            expect_handle_is_int: true,
            expect_handle_idxs: vec![2],
            expect_handle_layout: vec!["x"],
        },
        HandleCase {
            name: "common_handle",
            ids: (3001, 3002, 3003),
            base_cols: vec![a(), HandleColDef { id: 2, name: "b", tp: FieldTypeCode::Long }],
            build_mv: |base_id, mv_id| {
                let mut mv = mview_table(
                    mv_id,
                    "mv_common_handle",
                    vec![
                        mk_col(1, "x", 0, FieldTypeCode::Long),
                        mk_col(2, "cnt", 1, FieldTypeCode::LongLong),
                        mk_col(3, "y", 2, FieldTypeCode::Long),
                    ],
                    vec![base_id],
                    "select a, count(1), b from t group by a, b",
                );
                mv.is_common_handle = true;
                let mut primary = index(1, "PRIMARY", &[("y", 2), ("x", 0)], false);
                primary.primary = true;
                primary.unique = true;
                mv.indices = vec![primary].into();
                mv
            },
            expect_handle_is_int: false,
            expect_handle_idxs: vec![3, 1],
            expect_handle_layout: vec!["y", "x"],
        },
    ];
    for tc in cases {
        let (base, mlog) = build_base_and_m_log_for_handle_test(tc.ids.0, tc.ids.1, &tc.base_cols);
        let mv = (tc.build_mv)(tc.ids.0, tc.ids.2);
        // Go `buildMViewResultForHandleTest`.
        let is = mock_info_schema(&[&base, &mlog, &mv]);
        let res = tidb_planner::mview::build(
            &super::planner_mview_tests::parse,
            &is,
            &mv,
            BuildOptions { from_ts: 1, to_ts: 0 },
            None,
        )
        .unwrap();
        let (_, output_names) = super::planner_mview_tests::optimize_for_test(&is, &res.merge_source_select);
        let handle_cols = &res.mv_table_pk_cols;
        assert_eq!(tc.expect_handle_is_int, handle_cols.is_int(), "{}", tc.name);
        assert_eq!(tc.expect_handle_idxs.len(), handle_cols.num_cols(), "{}", tc.name);
        for (i, &idx) in tc.expect_handle_idxs.iter().enumerate() {
            assert_eq!(idx, handle_cols.get_col(i).index, "{}", tc.name);
        }
        // Go `assertHandleColsMatchMergeSourceLayout`.
        let fields = res.merge_source_select.fields.fields();
        assert_eq!(fields.len(), res.source_column_count, "{}", tc.name);
        assert_eq!(output_names.len(), res.source_column_count, "{}", tc.name);
        assert_eq!(tc.expect_handle_layout.len(), handle_cols.num_cols(), "{}", tc.name);
        for (i, expected_col) in tc.expect_handle_layout.iter().enumerate() {
            let idx = usize::try_from(handle_cols.get_col(i).index).unwrap();
            assert!(idx < fields.len(), "{}", tc.name);
            let tidb_ast::SelectField::Expr { alias, .. } = &fields[idx] else {
                panic!("{}: not an expression field", tc.name);
            };
            assert_eq!(Some(*expected_col), alias.as_deref(), "{}", tc.name);
            assert_eq!(*expected_col, output_names[idx].names.column.original, "{}", tc.name);
        }
    }
}

/// Go `oracle.ComposeTS(physical, 0)`.
const fn compose_ts(physical: u64) -> u64 {
    physical << 18
}

/// Go `requireMLogCommitTSSelectionEstRows`.
fn require_m_log_commit_ts_selection_est_rows(rows: &[Vec<String>], commit_ts_info: &str, est_rows: &str) {
    let row = rows.iter().find(|row| row[4].contains(commit_ts_info)).unwrap_or_else(|| {
        panic!("mlog commit-ts selection not found: commitTSInfo={commit_ts_info} rows={rows:?}")
    });
    assert_eq!(est_rows, row[1], "{rows:?}");
}

/// Go `requireMLogCommitTSSelectionContains`.
fn require_m_log_commit_ts_selection_contains(rows: &[Vec<String>], commit_ts_info: &str, contains: &[String]) {
    let row = rows.iter().find(|row| row[4].contains(commit_ts_info)).unwrap_or_else(|| {
        panic!("mlog commit-ts selection not found: commitTSInfo={commit_ts_info} rows={rows:?}")
    });
    for expected in contains {
        assert!(row[4].contains(expected.as_str()), "{} lacks {expected}", row[4]);
    }
}

/// The Go explain-selectivity cases' tables: `t(a, b)` NOT NULL, its log, and
/// `select a, count(1) from t group by a`.
fn commit_ts_estimation_tables() -> (TableInfo, TableInfo, TableInfo) {
    let (base, mlog) = base_and_mlog(1, 2);
    for table in [&base, &mlog] {
        for i in 0..2 {
            col_at(table, i).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        }
    }
    let mv = mview_table(
        3,
        "mv",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        vec![1],
        "select a, count(1) from t group by a",
    );
    (base, mlog, mv)
}

/// [`explain_fast_refresh`] with the statement's retained upper bound
/// (Go `txnManager.GetStmtForUpdateTS()` / the transaction's start TS).
fn explain_fast_refresh_at(
    tables: &[&TableInfo],
    view: &str,
    options: BuildOptions,
    mlog_retained_lower_tso: u64,
    retained_upper_tso: u64,
) -> Vec<Vec<String>> {
    let is = mock_info_schema(tables);
    let mut plan = MViewDeltaMerge {
        database: "test".to_owned(),
        name: view.to_owned(),
        options,
        mlog_retained_lower_tso,
        registered_mlog_ids: Vec::new(),
        planned: None,
    };
    let ctx = mview_test_context()
        .with_txn_store(Box::new(tidb_store_driver_txn::cluster_storage_test_util::empty_mock_store()));
    let no_snapshot = |_: u64| -> Result<tidb_planner::common_plans::DataReaderSnapshot, super::DriverError> {
        panic!("the test reads no AS OF snapshot")
    };
    super::mview_delta_merge::optimize_mview_delta_merge(&mut plan, &is, "test", &ctx, retained_upper_tso, &no_snapshot)
        .unwrap();
    let (_, rows) = crate::explain_mview_delta_merge(&plan, &is, &ctx, ExplainFormat::Brief).unwrap();
    rows.iter().map(|row| row_cells(row)).collect()
}

/// Go's statement start (physical milliseconds) the window cases are built
/// around.
const START_PHYSICAL: u64 = 1_000_000;

#[test]
fn test_explain_refresh_mv_fast_plan_uses_m_log_commit_ts_window_selectivity() {
    let (base, mlog, mv) = commit_ts_estimation_tables();
    let retained_lower_tso = compose_ts(START_PHYSICAL - 10000);
    let last_success_tso = compose_ts(START_PHYSICAL - 5000);
    let target_tso = compose_ts(START_PHYSICAL - 1000);
    let retained_upper_tso = compose_ts(START_PHYSICAL);
    let expected_rows = format!(
        "{:.2}",
        10000.0 * (START_PHYSICAL - 1000 - (START_PHYSICAL - 5000)) as f64 / (START_PHYSICAL - (START_PHYSICAL - 10000)) as f64
    );
    let rows = explain_fast_refresh_at(
        &[&base, &mlog, &mv],
        "mv",
        BuildOptions { from_ts: last_success_tso, to_ts: target_tso },
        retained_lower_tso,
        retained_upper_tso,
    );
    require_m_log_commit_ts_selection_est_rows(&rows, "test.$mlog$t._tidb_commit_ts", &expected_rows);
}

#[test]
fn test_explain_refresh_mv_fast_plan_clamps_m_log_commit_ts_filter_to_retained_window() {
    let (base, mlog, mv) = commit_ts_estimation_tables();
    let retained_lower_tso = compose_ts(START_PHYSICAL - 10000);
    let last_success_tso = compose_ts(START_PHYSICAL - 20000);
    let target_tso = compose_ts(START_PHYSICAL + 10000);
    let rows = explain_fast_refresh_at(
        &[&base, &mlog, &mv],
        "mv",
        BuildOptions { from_ts: last_success_tso, to_ts: target_tso },
        retained_lower_tso,
        compose_ts(START_PHYSICAL),
    );
    require_m_log_commit_ts_selection_est_rows(&rows, "test.$mlog$t._tidb_commit_ts", "10000.00");
    require_m_log_commit_ts_selection_contains(
        &rows,
        "test.$mlog$t._tidb_commit_ts",
        &[
            format!("gt(test.$mlog$t._tidb_commit_ts, {last_success_tso})"),
            format!("le(test.$mlog$t._tidb_commit_ts, {target_tso})"),
        ],
    );
}

#[test]
fn test_explain_refresh_mv_fast_plan_estimates_zero_rows_for_disjoint_m_log_commit_ts_filter() {
    let (base, mlog, mv) = commit_ts_estimation_tables();
    let retained_lower_tso = compose_ts(START_PHYSICAL - 10000);
    let last_success_tso = compose_ts(START_PHYSICAL - 20000);
    let target_tso = compose_ts(START_PHYSICAL - 15000);
    let rows = explain_fast_refresh_at(
        &[&base, &mlog, &mv],
        "mv",
        BuildOptions { from_ts: last_success_tso, to_ts: target_tso },
        retained_lower_tso,
        compose_ts(START_PHYSICAL),
    );
    require_m_log_commit_ts_selection_est_rows(&rows, "test.$mlog$t._tidb_commit_ts", "0.00");
    require_m_log_commit_ts_selection_contains(
        &rows,
        "test.$mlog$t._tidb_commit_ts",
        &[
            format!("gt(test.$mlog$t._tidb_commit_ts, {last_success_tso})"),
            format!("le(test.$mlog$t._tidb_commit_ts, {target_tso})"),
        ],
    );
}
