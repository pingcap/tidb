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
use tidb_planner::mview::BuildOptions;
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
    let plan = MViewDeltaMerge {
        database: "test".to_owned(),
        name: view.to_owned(),
        options,
        mlog_retained_lower_tso,
        registered_mlog_ids: Vec::new(),
    };
    // Go's mock store under the mock context.
    let ctx = mview_test_context()
        .with_txn_store(Box::new(tidb_store_driver_txn::cluster_storage_test_util::SharedMemStorage::default()));
    let no_snapshot = |_: u64| -> Result<super::mview_delta_merge::FullUpdateSnapshot, super::DriverError> {
        panic!("the test reads no AS OF snapshot")
    };
    let (_, rows) =
        crate::explain_mview_delta_merge(&plan, &is, "test", &ctx, ExplainFormat::Brief, MOCK_START_TS, &no_snapshot)
            .unwrap();
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
            ["└─StreamAgg", "1.00", "root", "", "group by:test.t.a, funcs:max(test.t.b)->Column#40, funcs:min(test.t.b)->Column#41, funcs:firstrow(test.t.a)->test.t.a"],
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
    let plan = tidb_planner::common_plans::MViewCompleteDeltaApply {
        database: "test".to_owned(),
        name: mv.name.original().to_owned(),
        diff,
    };
    let ctx = mview_test_context()
        .with_txn_store(Box::new(tidb_store_driver_txn::cluster_storage_test_util::SharedMemStorage::default()));
    let (_, rows) = crate::explain_mview_complete_delta_apply(&plan, &is, "test", &ctx, ExplainFormat::Brief).unwrap();
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
