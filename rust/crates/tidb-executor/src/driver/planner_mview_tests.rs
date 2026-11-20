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

//! Go master `pkg/planner/mview/mview_test.go`: the merge and diff sources
//! `tidb_planner::mview` builds, optimized as Go's `optimizeForTest` does
//! (here through the executor's planner bridge, where this tier's optimizer
//! runs).

use tidb_ast::{Expr, JoinNode, QueryStmt, SelectStmt, Stmt};
use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_model::table::{MaterializedViewBaseInfo, MaterializedViewInfo, MaterializedViewLogInfo};
use tidb_model::{ColumnInfo, GoShared, SchemaState, TableInfo};
use tidb_planner::mview::{self, AggInfo, AggKind, BuildOptions};
use tidb_planner_core_operator_physicalop::PhysicalPlan;

const DELTA_TABLE_ALIAS: &str = "delta";
const MV_TABLE_ALIAS: &str = "mv";
const DELTA_CNT_STAR_NAME: &str = "__mview_delta_cnt_star";

/// Go `mviewTestContext` with the parser it plans under.
pub(super) fn parse(sql: &str) -> Result<Stmt, String> {
    tidb_parser::parse(sql).map_err(|error| error.compatibility_message(sql))
}

/// Go `infoschema.MockInfoSchema(tables)`: the tables, public, in `test`.
pub(super) fn mock_info_schema(tables: &[&TableInfo]) -> SessionExtendedInfoSchema {
    let mut db = tidb_model::db::DBInfo {
        id: 1,
        name: tidb_ast::CiString::new("test"),
        state: SchemaState::PUBLIC,
        ..Default::default()
    };
    for table in tables {
        let mut table = table.clone_like_go();
        table.db_id = 1;
        db.deprecated_tables.push_go(table);
    }
    let db = GoShared::new(db);
    let mut is = SessionExtendedInfoSchema::init_with_db_infos(vec![db.clone()], Vec::new(), Vec::new(), 1);
    // The domain's builder: each table's `table.Table` (`cluster_session_catalog`).
    for table in db.read().deprecated_tables.iter_deref() {
        let table = table.read();
        let kv = tidb_table::table_from_meta::table_from_meta(&table, &tidb_table::table_from_meta::AutoIdSource::Unavailable)
            .unwrap();
        is.register_kv_in("test", table.name.original(), kv).unwrap();
    }
    is
}

/// Go `optimizeForTest`: the SELECT optimized against `is`, with its
/// output names.
fn optimize_for_test(is: &SessionExtendedInfoSchema, sel: &SelectStmt) -> (PhysicalPlan, Vec<tidb_datatype::FieldName>) {
    optimize_for_test_in(&mview_test_context(), is, sel)
}

/// Go `mviewTestContext`: materialized views on.
pub(super) fn mview_test_context() -> crate::StmtContext {
    crate::StmtContext::for_query().with_enable_mview(true)
}

/// [`optimize_for_test`] under `ctx` (a test that sets session variables).
fn optimize_for_test_in(
    ctx: &crate::StmtContext,
    is: &SessionExtendedInfoSchema,
    sel: &SelectStmt,
) -> (PhysicalPlan, Vec<tidb_datatype::FieldName>) {
    let (plan_ids, column_ids) =
        (tidb_planner_core_base::PlanIdAllocator::new(), tidb_planner_core_base::ColumnIdAllocator::new());
    let query = QueryStmt::Select(Box::new(sel.clone()));
    let (logical, physical) = super::planner_bridge::planner_optimized_query_with_allocators_for_test(
        &query,
        is,
        // Go `coretestsdk.MockContext`'s `CurrentDB`.
        "test",
        ctx,
        &plan_ids,
        &column_ids,
    )
    .unwrap_or_else(|error| panic!("optimize: {error:?}"));
    (physical, logical.output_names().to_vec())
}

/// Go `mkCol`.
pub(super) fn mk_col(id: i64, name: &str, offset: i64, tp: FieldTypeCode) -> ColumnInfo {
    ColumnInfo {
        id,
        name: tidb_ast::CiString::new(name),
        offset,
        state: SchemaState::PUBLIC,
        field_type: FieldType::new(tp),
        ..Default::default()
    }
}

pub(super) fn table(id: i64, name: &str, columns: Vec<ColumnInfo>) -> TableInfo {
    TableInfo {
        id,
        name: tidb_ast::CiString::new(name),
        state: SchemaState::PUBLIC,
        columns: columns.into(),
        ..Default::default()
    }
}

/// The base table `t(a, b)` with its log, the shape most Go cases share.
pub(super) fn base_and_mlog(base_id: i64, mlog_id: i64) -> (TableInfo, TableInfo) {
    let mut base = table(
        base_id,
        "t",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "b", 1, FieldTypeCode::Long)],
    );
    base.materialized_view_base = Some(GoShared::new(MaterializedViewBaseInfo { mlog_id, ..Default::default() }));
    let mut mlog = table(
        mlog_id,
        "$mlog$t",
        vec![
            mk_col(1, "a", 0, FieldTypeCode::Long),
            mk_col(2, "b", 1, FieldTypeCode::Long),
            mk_col(3, tidb_model::table::MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME, 2, FieldTypeCode::Varchar),
            mk_col(4, tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME, 3, FieldTypeCode::Tiny),
        ],
    );
    mlog.materialized_view_log = Some(GoShared::new(MaterializedViewLogInfo {
        base_table_id: base_id,
        columns: vec![tidb_ast::CiString::new("a"), tidb_ast::CiString::new("b")].into(),
        ..Default::default()
    }));
    (base, mlog)
}

pub(super) fn mview_table(id: i64, name: &str, columns: Vec<ColumnInfo>, base_ids: Vec<i64>, sql: &str) -> TableInfo {
    let mut mv = table(id, name, columns);
    mv.materialized_view = Some(GoShared::new(MaterializedViewInfo {
        base_table_ids: base_ids.into(),
        sql_content: sql.to_owned(),
        ..Default::default()
    }));
    mv
}

/// Go `requireDependencies`.
fn require_dependencies(ai: &AggInfo, expected: &[usize]) {
    assert_eq!(ai.dependencies, expected, "{ai:?}");
}

/// Go `fieldNameInfo`.
#[derive(Debug, Default, PartialEq, Eq)]
struct FieldNameInfo {
    pos: usize,
    hidden: bool,
    db: String,
    tbl: String,
    col: String,
    orig_tbl: String,
    orig_col: String,
}

fn info(pos: usize, db: &str, tbl: &str, col: &str, orig_tbl: &str, orig_col: &str) -> FieldNameInfo {
    FieldNameInfo {
        pos,
        hidden: false,
        db: db.to_owned(),
        tbl: tbl.to_owned(),
        col: col.to_owned(),
        orig_tbl: orig_tbl.to_owned(),
        orig_col: orig_col.to_owned(),
    }
}

/// Go `nameSliceInfo`.
fn name_slice_info(names: &[tidb_datatype::FieldName]) -> Vec<FieldNameInfo> {
    names
        .iter()
        .enumerate()
        .map(|(pos, n)| FieldNameInfo {
            pos,
            hidden: n.hidden,
            db: n.names.database.original.clone(),
            tbl: n.names.table.original.clone(),
            col: n.names.column.original.clone(),
            orig_tbl: n.names.original_table.original.clone(),
            orig_col: n.names.original_column.original.clone(),
        })
        .collect()
}

/// Go `requireMergePlanOutputNames`.
fn require_merge_plan_output_names(
    plan: &PhysicalPlan,
    output_names: &[tidb_datatype::FieldName],
    expected: Vec<FieldNameInfo>,
) {
    assert_eq!(output_names.len(), plan.schema().map_or(0, |schema| schema.columns.len()));
    assert_eq!(expected.len(), output_names.len());
    assert_eq!(name_slice_info(output_names), expected);
}

/// Go's `res.MergeSourceSelect.From.TableRefs.Left.(*ast.TableSource).Source.(*ast.SelectStmt)`.
fn left_derived_select(sel: &SelectStmt) -> &SelectStmt {
    let from = sel.from.as_ref().expect("from");
    let JoinNode::Derived { subquery, .. } = &from.left else {
        panic!("left side is not a derived table");
    };
    let QueryStmt::Select(sel) = &**subquery else {
        panic!("derived table is not a select");
    };
    sel
}

/// Go `collectAndPredicates`.
fn collect_and_predicates(expr: &Expr) -> Vec<(&tidb_ast::BinaryOp, &Expr, &Expr)> {
    let Expr::Binary(op, l, r) = expr else {
        panic!("not a binary operation: {expr:?}");
    };
    if *op != tidb_ast::BinaryOp::LogicAnd {
        return vec![(op, &**l, &**r)];
    }
    let mut out = collect_and_predicates(l);
    out.extend(collect_and_predicates(r));
    out
}

#[test]
fn test_build_count_sum() {
    let (base_id, mlog_id, mv_id) = (1, 2, 3);
    let (base, mlog) = base_and_mlog(base_id, mlog_id);
    let mv = mview_table(
        mv_id,
        "mv_tbl",
        vec![
            mk_col(1, "x", 0, FieldTypeCode::Long),
            mk_col(2, "cnt", 1, FieldTypeCode::LongLong),
            mk_col(3, "cnt_b", 2, FieldTypeCode::LongLong),
            mk_col(4, "s", 3, FieldTypeCode::LongLong),
        ],
        vec![base_id],
        "select a, COUNT(1), CoUnT(b), sUm(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);

    let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 10, to_ts: 0 }, None).unwrap();
    let (plan, output_names) = optimize_for_test(&is, &res.merge_source_select);
    assert_eq!(mv.columns.len(), res.mv_column_count);
    assert_eq!(1, res.count_star_mv_offset);

    let (mut has_count_star, mut has_count_expr, mut has_sum) = (false, false, false);
    for ai in &res.agg_infos {
        match ai.kind {
            AggKind::CountStar => {
                has_count_star = true;
                require_dependencies(ai, &[0]);
            }
            AggKind::Sum => {
                has_sum = true;
                assert_eq!("b", ai.arg_col_name);
                require_dependencies(ai, &[2, 5]);
            }
            AggKind::Count => {
                has_count_expr = true;
                assert_eq!("b", ai.arg_col_name);
                require_dependencies(ai, &[1]);
            }
            _ => {}
        }
    }
    assert!(has_count_star && has_count_expr && has_sum);

    let mv_db_name = is.schema_name_by_table_id(mv.id).unwrap();
    assert!(!mv_db_name.is_empty());
    require_merge_plan_output_names(
        &plan,
        &output_names,
        vec![
            info(0, "", DELTA_TABLE_ALIAS, DELTA_CNT_STAR_NAME, "", ""),
            info(1, "", DELTA_TABLE_ALIAS, "__mview_delta_cnt_2", "", ""),
            info(2, "", DELTA_TABLE_ALIAS, "__mview_delta_sum_3", "", ""),
            info(3, "", DELTA_TABLE_ALIAS, "x", mlog.name.original(), "a"),
            info(4, &mv_db_name, MV_TABLE_ALIAS, "cnt", mv.name.original(), "cnt"),
            info(5, &mv_db_name, MV_TABLE_ALIAS, "cnt_b", mv.name.original(), "cnt_b"),
            info(6, &mv_db_name, MV_TABLE_ALIAS, "s", mv.name.original(), "s"),
            info(7, &mv_db_name, MV_TABLE_ALIAS, "__mview_mv_rowid", "", "_tidb_rowid"),
        ],
    );
}

#[test]
fn test_build_count_expr_sum_expr() {
    let (base_id, mlog_id, mv_id) = (11, 22, 33);
    let (base, mlog) = base_and_mlog(base_id, mlog_id);
    let mv = mview_table(
        mv_id,
        "mv_expr_tbl",
        vec![
            mk_col(1, "x", 0, FieldTypeCode::Long),
            mk_col(2, "cnt_star", 1, FieldTypeCode::LongLong),
            mk_col(3, "cnt_b", 2, FieldTypeCode::LongLong),
            mk_col(4, "s_expr", 3, FieldTypeCode::LongLong),
        ],
        vec![base_id],
        "select a, count(1), count(a+b), sum((a+b)) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);

    let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 10, to_ts: 0 }, None).unwrap();
    let (plan, output_names) = optimize_for_test(&is, &res.merge_source_select);
    assert_eq!(1, res.count_star_mv_offset);

    let (mut has_count_star, mut has_count, mut has_sum) = (false, false, false);
    for ai in &res.agg_infos {
        match ai.kind {
            AggKind::CountStar => {
                has_count_star = true;
                require_dependencies(ai, &[0]);
            }
            AggKind::Count => {
                has_count = true;
                assert!(ai.arg_col_name.is_empty());
                require_dependencies(ai, &[1]);
            }
            AggKind::Sum => {
                has_sum = true;
                assert!(ai.arg_col_name.is_empty());
                require_dependencies(ai, &[2, 5]);
            }
            _ => {}
        }
    }
    assert!(has_count_star && has_count && has_sum);

    let mv_db_name = is.schema_name_by_table_id(mv.id).unwrap();
    require_merge_plan_output_names(
        &plan,
        &output_names,
        vec![
            info(0, "", DELTA_TABLE_ALIAS, DELTA_CNT_STAR_NAME, "", ""),
            info(1, "", DELTA_TABLE_ALIAS, "__mview_delta_cnt_2", "", ""),
            info(2, "", DELTA_TABLE_ALIAS, "__mview_delta_sum_3", "", ""),
            info(3, "", DELTA_TABLE_ALIAS, "x", mlog.name.original(), "a"),
            info(4, &mv_db_name, MV_TABLE_ALIAS, "cnt_star", mv.name.original(), "cnt_star"),
            info(5, &mv_db_name, MV_TABLE_ALIAS, "cnt_b", mv.name.original(), "cnt_b"),
            info(6, &mv_db_name, MV_TABLE_ALIAS, "s_expr", mv.name.original(), "s_expr"),
            info(7, &mv_db_name, MV_TABLE_ALIAS, "__mview_mv_rowid", "", "_tidb_rowid"),
        ],
    );
}

#[test]
fn test_build_m_log_delta_select_ti_flash_hint() {
    let (base_id, mlog_id, mv_id) = (1011, 2022, 3033);
    let (base, mlog) = base_and_mlog(base_id, mlog_id);
    let mv = mview_table(
        mv_id,
        "mv_tbl",
        vec![mk_col(1, "x", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        vec![base_id],
        "select a, count(1) from t group by a",
    );
    let cases = [
        ("available replica keeps hint", Some(tidb_model::table::TiFlashReplicaInfo { count: 1, available: true, ..Default::default() })),
        ("missing replica still keeps hint", None),
        ("unavailable replica still keeps hint", Some(tidb_model::table::TiFlashReplicaInfo { count: 1, available: false, ..Default::default() })),
    ];
    for (name, tiflash_replica) in cases {
        let mut mlog_clone = mlog.clone_like_go();
        mlog_clone.tiflash_replica = tiflash_replica.map(GoShared::new);
        let is = mock_info_schema(&[&base, &mlog_clone, &mv]);

        let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 10, to_ts: 0 }, None).unwrap();
        let delta_sel = left_derived_select(&res.merge_source_select);
        assert_eq!(delta_sel.hints.len(), 1, "{name}");
        let table_hint = &delta_sel.hints[0];
        assert_eq!(tidb_hack::go_to_lower(&table_hint.name), "read_from_storage" /* Go hint.HintReadFromStorage */, "{name}");
        let tidb_ast::HintKind::ReadFromStorage { groups, .. } = &table_hint.kind else {
            panic!("{name}: not a READ_FROM_STORAGE hint: {table_hint:?}");
        };
        assert_eq!(groups.len(), 1, "{name}");
        assert_eq!(tidb_hack::go_to_lower(&groups[0].0), "tiflash" /* Go hint.HintTiFlash */, "{name}");
        assert_eq!(groups[0].1.len(), 1, "{name}");
        assert_eq!(tidb_hack::go_to_lower(&groups[0].1[0].name), mlog_clone.name.lowercase(), "{name}");
        let db = is.schema_name_by_table_id(mlog_clone.id).unwrap();
        assert_eq!(groups[0].1[0].db_name.as_deref().map(tidb_hack::go_to_lower), Some(tidb_hack::go_to_lower(&db)), "{name}");
    }
}

#[test]
fn test_build_m_log_delta_select_commit_ts_window() {
    let (base_id, mlog_id, mv_id) = (1111, 2222, 3333);
    let (base, mlog) = base_and_mlog(base_id, mlog_id);
    let mv = mview_table(
        mv_id,
        "mv_tbl",
        vec![mk_col(1, "x", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        vec![base_id],
        "select a, count(1) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);

    let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 10, to_ts: 25 }, None).unwrap();
    let delta_sel = left_derived_select(&res.merge_source_select);
    let r#where = delta_sel.where_clause.as_ref().expect("where");

    let mut ts_value_by_op = std::collections::HashMap::new();
    for (op, l, r) in collect_and_predicates(r#where) {
        let Expr::Column(path) = l else {
            continue;
        };
        if tidb_hack::go_to_lower(path.last().unwrap()) != tidb_model::column::EXTRA_COMMIT_TS_NAME {
            continue;
        }
        let Expr::Int(value) = r else {
            panic!("not a value: {r:?}");
        };
        ts_value_by_op.insert(*op, value.parse::<u64>().unwrap());
    }
    assert_eq!(ts_value_by_op.get(&tidb_ast::BinaryOp::Gt), Some(&10));
    assert_eq!(ts_value_by_op.get(&tidb_ast::BinaryOp::Le), Some(&25));
}

#[test]
fn test_build_materialized_view_source() {
    let (parent_id, mlog_id, child_id) = (1, 2, 3);
    let mut parent = table(
        parent_id,
        "mv_parent",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
    );
    parent.materialized_view = Some(GoShared::new(MaterializedViewInfo::default()));
    parent.materialized_view_base = Some(GoShared::new(MaterializedViewBaseInfo { mlog_id, ..Default::default() }));
    let mut mlog = table(
        mlog_id,
        "$mlog$mv_parent",
        vec![
            mk_col(1, "a", 0, FieldTypeCode::Long),
            mk_col(2, tidb_model::table::MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME, 1, FieldTypeCode::Varchar),
            mk_col(3, tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME, 2, FieldTypeCode::Tiny),
        ],
    );
    mlog.materialized_view_log = Some(GoShared::new(MaterializedViewLogInfo {
        base_table_id: parent_id,
        columns: vec![tidb_ast::CiString::new("a")].into(),
        ..Default::default()
    }));
    let child = mview_table(
        child_id,
        "mv_child",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        vec![parent_id],
        "select a, count(1) from mv_parent group by a",
    );

    let build = |parent: &TableInfo, mlog: &TableInfo| {
        let is = mock_info_schema(&[parent, mlog, &child]);
        mview::build(&parse, &is, &child, BuildOptions { from_ts: 10, to_ts: 0 }, None)
    };
    let resolve = |parent: &TableInfo, mlog: &TableInfo| {
        let is = mock_info_schema(&[parent, mlog, &child]);
        mview::resolve_fast_refresh_source(&is, &child)
    };
    let res = build(&parent, &mlog).unwrap();
    assert_eq!(parent_id, res.base_table_id);
    assert_eq!(mlog_id, res.mlog_table_id);
    assert_eq!(vec!["a".to_owned()], res.group_key_base_cols);

    // "source not ready"
    for state in [tidb_model::table::MViewInitBuildState::DEFERRED, tidb_model::table::MViewInitBuildState::BUILDING] {
        parent.materialized_view.as_ref().unwrap().write().init_build_state = state;
        let err = build(&parent, &mlog).unwrap_err();
        assert!(err.contains("source materialized view mv_parent is not ready"), "{err}");
    }
    parent.materialized_view.as_ref().unwrap().write().init_build_state = tidb_model::table::MViewInitBuildState::READY;
    // "source not public"
    {
        let mut parent = parent.clone_like_go();
        parent.state = SchemaState::WRITE_ONLY;
        let err = resolve(&parent, &mlog).unwrap_err();
        assert!(err.contains("source object mv_parent is not public"), "{err}");
    }
    // "log not public"
    {
        let mut mlog = mlog.clone_like_go();
        mlog.state = SchemaState::WRITE_ONLY;
        let err = resolve(&parent, &mlog).unwrap_err();
        assert!(err.contains("materialized view log $mlog$mv_parent is not public"), "{err}");
    }
    // "log owner mismatch"
    mlog.materialized_view_log.as_ref().unwrap().write().base_table_id = child_id;
    let err = resolve(&parent, &mlog).unwrap_err();
    assert!(err.contains("does not belong to base table"), "{err}");
}

/// Go `columnNameRef`: a column reference's table and name.
fn column_name_ref(expr: &Expr) -> (String, String) {
    let Expr::Column(path) = expr else {
        panic!("not a column: {expr:?}");
    };
    let table = if path.len() > 1 { path[path.len() - 2].clone() } else { String::new() };
    (table, path.last().unwrap().clone())
}

/// Go `findIndexJoinPlan`.
fn find_index_join_plan(plan: &PhysicalPlan) -> Option<&tidb_planner_core_operator_physicalop::PhysicalIndexJoin> {
    match plan {
        PhysicalPlan::IndexJoin(join) => Some(join),
        PhysicalPlan::IndexHashJoin(join) => Some(&join.physical_index_join),
        _ => plan.children().iter().find_map(find_index_join_plan),
    }
}

/// Go `findHashJoinPlan`.
fn find_hash_join_plan(plan: &PhysicalPlan) -> Option<&tidb_planner_core_operator_physicalop::PhysicalHashJoin> {
    match plan {
        PhysicalPlan::HashJoin(join) => Some(join),
        _ => plan.children().iter().find_map(find_hash_join_plan),
    }
}

/// Go `requireOutputColNames`.
fn require_output_col_names(output_names: &[tidb_datatype::FieldName], expected: &[&str]) {
    assert_eq!(output_names.len(), expected.len());
    for (name, col) in output_names.iter().zip(expected) {
        assert_eq!(name.names.column.original, *col);
    }
}

/// Go `collectPayloadComparisonOps`.
fn collect_payload_comparison_ops(expr: &Expr) -> std::collections::HashMap<String, tidb_ast::BinaryOp> {
    fn walk(node: &Expr, ops: &mut std::collections::HashMap<String, tidb_ast::BinaryOp>) {
        match node {
            Expr::Binary(op, l, r) => {
                if matches!(op, tidb_ast::BinaryOp::NullEq | tidb_ast::BinaryOp::Eq) {
                    let (left_table, left_col) = column_name_ref(l);
                    let (right_table, right_col) = column_name_ref(r);
                    assert_eq!(mview::DIFF_RECOMPUTED_ALIAS, left_table);
                    assert_eq!(mview::DIFF_CURRENT_ALIAS, right_table);
                    assert_eq!(left_col, right_col);
                    ops.insert(right_col, *op);
                }
                walk(l, ops);
                walk(r, ops);
            }
            Expr::Unary(_, v) | Expr::Paren(v) => walk(v, ops),
            Expr::Is { expr, .. } => walk(expr, ops),
            _ => {}
        }
    }
    let mut ops = std::collections::HashMap::new();
    walk(expr, &mut ops);
    ops
}

/// Go `t.Columns[i]`.
pub(super) fn col_at(t: &TableInfo, i: usize) -> GoShared<ColumnInfo> {
    t.columns.iter_deref().nth(i).unwrap()
}

pub(super) fn index(id: i64, name: &str, columns: &[(&str, i64)], invisible: bool) -> tidb_model::IndexInfo {
    tidb_model::IndexInfo {
        id,
        name: tidb_ast::CiString::new(name),
        state: SchemaState::PUBLIC,
        columns: columns
            .iter()
            .map(|(name, offset)| tidb_model::IndexColumn {
                name: tidb_ast::CiString::new(*name),
                offset: *offset,
                ..Default::default()
            })
            .collect::<Vec<_>>()
            .into(),
        invisible,
        ..Default::default()
    }
}

/// The `select a, ... from t group by a` view over [`base_and_mlog`] whose
/// columns follow `x` with `rest`, built at `opt`.
fn build_over_t(
    ids: (i64, i64, i64),
    mv_name: &str,
    rest: &[(&str, FieldTypeCode)],
    sql: &str,
) -> (TableInfo, TableInfo, TableInfo) {
    let (base, mlog) = base_and_mlog(ids.0, ids.1);
    let mut columns = vec![mk_col(1, "x", 0, FieldTypeCode::Long)];
    for (i, (name, tp)) in rest.iter().enumerate() {
        columns.push(mk_col(i as i64 + 2, name, i as i64 + 1, *tp));
    }
    let mv = mview_table(ids.2, mv_name, columns, vec![ids.0], sql);
    (base, mlog, mv)
}

#[test]
fn test_build_merge_source_select_join_operator_by_mv_nullability() {
    let (base, mlog, mut mv) = build_over_t(
        (4011, 4022, 4033),
        "mv_tbl",
        &[("y", FieldTypeCode::Long), ("cnt", FieldTypeCode::LongLong)],
        "select a, b, count(1) from t group by a, b",
    );
    col_at(&mv, 0).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let is = mock_info_schema(&[&base, &mlog, &mv]);

    let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 10, to_ts: 0 }, None).unwrap();
    let on = res.merge_source_select.from.as_ref().unwrap().on.as_ref().expect("on");
    let predicates = collect_and_predicates(on);
    assert_eq!(predicates.len(), 2);

    let mut op_by_mv_col = std::collections::HashMap::new();
    for (op, l, r) in predicates {
        let (left_table, left_col) = column_name_ref(l);
        let (right_table, right_col) = column_name_ref(r);
        assert_eq!(DELTA_TABLE_ALIAS, left_table);
        assert_eq!(MV_TABLE_ALIAS, right_table);
        assert_eq!(left_col, right_col);
        op_by_mv_col.insert(right_col, *op);
    }
    assert_eq!(op_by_mv_col["x"], tidb_ast::BinaryOp::Eq);
    assert_eq!(op_by_mv_col["y"], tidb_ast::BinaryOp::NullEq);
}

#[test]
fn test_build_min_max_has_removed_gate() {
    let ctx = mview_test_context().with_inl_join_inner_multi_pattern(true);
    let (mut base, mlog, mv) = build_over_t(
        (10, 20, 30),
        "mv_minmax_tbl",
        &[("cnt", FieldTypeCode::LongLong), ("mx", FieldTypeCode::Long), ("mn", FieldTypeCode::Long)],
        "select a, count(1), max(b), min(b) from t group by a",
    );
    base.indices = vec![index(1, "idx_a", &[("a", 0)], false)].into();
    col_at(&base, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    col_at(&mlog, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let is = mock_info_schema(&[&base, &mlog, &mv]);

    let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 1, to_ts: 0 }, None).unwrap();
    let (plan, output_names) = optimize_for_test_in(&ctx, &is, &res.merge_source_select);
    assert_eq!(1, res.count_star_mv_offset);
    let template = res.full_update_lookup_template_select.as_ref().expect("full-update lookup template");
    let (full_plan, full_output_names) = optimize_for_test_in(&ctx, &is, template);
    assert!(find_index_join_plan(&full_plan).is_some(), "lookup template best plan: {full_plan:?}");
    assert_eq!(res.full_update_lookup_column_count, full_plan.schema().map_or(0, |schema| schema.columns.len()));
    assert_eq!(res.full_update_lookup_column_count, full_output_names.len());
    require_output_col_names(&full_output_names, &["x", "mx", "mn"]);

    let (mut has_max, mut has_min) = (false, false);
    for ai in &res.agg_infos {
        match ai.kind {
            AggKind::Max => {
                has_max = true;
                require_dependencies(ai, &[1, 2, 3, 4]);
            }
            AggKind::Min => {
                has_min = true;
                require_dependencies(ai, &[5, 6, 7, 8]);
            }
            AggKind::CountStar => require_dependencies(ai, &[0]),
            _ => {}
        }
    }
    assert!(has_max && has_min);

    let mv_db_name = is.schema_name_by_table_id(mv.id).unwrap();
    assert!(!mv_db_name.is_empty());
    let d = |pos, col: &str| info(pos, "", DELTA_TABLE_ALIAS, col, "", "");
    let m = |pos, col: &str| info(pos, &mv_db_name, MV_TABLE_ALIAS, col, mv.name.original(), col);
    require_merge_plan_output_names(
        &plan,
        &output_names,
        vec![
            d(0, DELTA_CNT_STAR_NAME),
            d(1, "__mview_max_in_added_2"),
            d(2, "__mview_max_cnt_in_added_2"),
            d(3, "__mview_max_in_removed_2"),
            d(4, "__mview_max_cnt_in_removed_2"),
            d(5, "__mview_min_in_added_3"),
            d(6, "__mview_min_cnt_in_added_3"),
            d(7, "__mview_min_in_removed_3"),
            d(8, "__mview_min_cnt_in_removed_3"),
            info(9, "", DELTA_TABLE_ALIAS, "x", mlog.name.original(), "a"),
            m(10, "cnt"),
            m(11, "mx"),
            m(12, "mn"),
            info(13, &mv_db_name, MV_TABLE_ALIAS, "__mview_mv_rowid", "", "_tidb_rowid"),
        ],
    );
}

#[test]
fn test_full_update_lookup_index_hint_uses_all_supporting_indexes() {
    let (mut base, mlog, mv) = build_over_t(
        (101, 102, 103),
        "mv_minmax_hint_tbl",
        &[("cnt", FieldTypeCode::LongLong), ("mx", FieldTypeCode::Long)],
        "select a, count(1), max(b) from t group by a",
    );
    base.indices = vec![
        index(1, "idx_a", &[("a", 0)], false),
        index(2, "idx_a_b", &[("a", 0), ("b", 1)], false),
        index(3, "idx_a_hidden", &[("a", 0)], true),
        index(4, "idx_b", &[("b", 1)], false),
    ]
    .into();
    let is = mock_info_schema(&[&base, &mlog, &mv]);

    let mut res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 1, to_ts: 0 }, None).unwrap();
    let index_names = mview::find_visible_indexes_with_prefix_covering_columns(&base, &res.group_key_base_cols);
    assert_eq!(index_names, vec!["idx_a".to_owned(), "idx_a_b".to_owned()]);
    let template = res.full_update_lookup_template_select.as_mut().expect("full-update lookup template");
    mview::set_full_update_lookup_index_hint(template, &index_names).unwrap();

    let from = template.from.as_ref().unwrap();
    let Some(JoinNode::Derived { subquery, .. }) = &from.right else {
        panic!("inner side is not a derived table");
    };
    let QueryStmt::Select(inner_sel) = &**subquery else {
        panic!("inner side is not a select");
    };
    let JoinNode::Table(base_table_name) = &inner_sel.from.as_ref().unwrap().left else {
        panic!("inner base side is not a table");
    };
    assert_eq!(base_table_name.hints.len(), 1);
    assert_eq!(base_table_name.hints[0].kind, tidb_ast::IndexHintKind::Use);
    assert_eq!(base_table_name.hints[0].scope, tidb_ast::IndexHintScope::All);
    assert_eq!(base_table_name.hints[0].indexes, index_names);
}

#[test]
fn test_build_min_max_nullable_dependency_order() {
    let (base, mlog, mv) = build_over_t(
        (110, 120, 130),
        "mv_minmax_nullable_tbl",
        &[
            ("cnt", FieldTypeCode::LongLong),
            ("cnt_b", FieldTypeCode::LongLong),
            ("mx", FieldTypeCode::Long),
            ("mn", FieldTypeCode::Long),
        ],
        "select a, count(1), count(b), max(b), min(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 1, to_ts: 0 }, None).unwrap();
    for ai in &res.agg_infos {
        match ai.kind {
            AggKind::CountStar => require_dependencies(ai, &[0]),
            AggKind::Count => {
                assert_eq!("b", ai.arg_col_name);
                require_dependencies(ai, &[1]);
            }
            AggKind::Max => require_dependencies(ai, &[2, 3, 4, 5, 12]),
            AggKind::Min => require_dependencies(ai, &[6, 7, 8, 9, 12]),
            AggKind::Sum => {}
        }
    }
}

#[test]
fn test_build_min_max_nullable_without_count_expr() {
    let (base, mlog, mv) = build_over_t(
        (210, 220, 230),
        "mv_minmax_nullable_no_count_tbl",
        &[("cnt", FieldTypeCode::LongLong), ("mx", FieldTypeCode::Long), ("mn", FieldTypeCode::Long)],
        "select a, count(1), max(b), min(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 1, to_ts: 0 }, None).unwrap();
    for ai in &res.agg_infos {
        match ai.kind {
            AggKind::CountStar => require_dependencies(ai, &[0]),
            AggKind::Max => require_dependencies(ai, &[1, 2, 3, 4]),
            AggKind::Min => require_dependencies(ai, &[5, 6, 7, 8]),
            _ => {}
        }
    }
}

#[test]
fn test_build_sum_nullable_with_duplicate_count_expr() {
    let (base, mlog, mv) = build_over_t(
        (211, 221, 231),
        "mv_sum_nullable_dup_count_tbl",
        &[
            ("cnt", FieldTypeCode::LongLong),
            ("cnt_b1", FieldTypeCode::LongLong),
            ("cnt_b2", FieldTypeCode::LongLong),
            ("s", FieldTypeCode::LongLong),
        ],
        "select a, count(1), count(b), count(b), sum(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 1, to_ts: 0 }, None).unwrap();
    for ai in &res.agg_infos {
        match ai.kind {
            AggKind::CountStar => require_dependencies(ai, &[0]),
            AggKind::Count => {
                assert_eq!("b", ai.arg_col_name);
                if ai.mv_offset == 2 {
                    require_dependencies(ai, &[1]);
                } else {
                    assert_eq!(3, ai.mv_offset);
                    require_dependencies(ai, &[2]);
                }
            }
            AggKind::Sum => require_dependencies(ai, &[3, 6]),
            _ => {}
        }
    }
}

#[test]
fn test_build_min_max_nullable_with_duplicate_count_expr() {
    let (base, mlog, mv) = build_over_t(
        (212, 222, 232),
        "mv_minmax_nullable_dup_count_tbl",
        &[
            ("cnt", FieldTypeCode::LongLong),
            ("cnt_b1", FieldTypeCode::LongLong),
            ("cnt_b2", FieldTypeCode::LongLong),
            ("mx", FieldTypeCode::Long),
            ("mn", FieldTypeCode::Long),
        ],
        "select a, count(1), count(b), count(b), max(b), min(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let res = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 1, to_ts: 0 }, None).unwrap();
    for ai in &res.agg_infos {
        match ai.kind {
            AggKind::CountStar => require_dependencies(ai, &[0]),
            AggKind::Count => {
                assert_eq!("b", ai.arg_col_name);
                if ai.mv_offset == 2 {
                    require_dependencies(ai, &[1]);
                } else {
                    assert_eq!(3, ai.mv_offset);
                    require_dependencies(ai, &[2]);
                }
            }
            AggKind::Max => require_dependencies(ai, &[3, 4, 5, 6, 13]),
            AggKind::Min => require_dependencies(ai, &[7, 8, 9, 10, 13]),
            AggKind::Sum => {}
        }
    }
}

#[test]
fn test_build_sum_without_count_expr() {
    let (base, mlog, mv) = build_over_t(
        (101, 202, 303),
        "mv_sum_only_tbl",
        &[("cnt", FieldTypeCode::LongLong), ("s", FieldTypeCode::LongLong)],
        "select a, count(1), sum(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let err = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 1, to_ts: 0 }, None).unwrap_err();
    assert!(err.contains("requires matching COUNT(expr)"), "{err}");
}

#[test]
fn test_build_missing_count_star() {
    let (base, mlog, mv) = build_over_t(
        (111, 222, 333),
        "mv_no_cnt_star_tbl",
        &[("cnt_b", FieldTypeCode::LongLong), ("s", FieldTypeCode::LongLong)],
        "select a, count(b), sum(b) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let err = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 1, to_ts: 0 }, None).unwrap_err();
    assert!(err.contains("must include COUNT(*)"), "{err}");
}

#[test]
fn test_build_missing_old_new() {
    let (base_id, mlog_id, mv_id) = (1000, 2000, 3000);
    let mut base = table(base_id, "t", vec![mk_col(1, "a", 0, FieldTypeCode::Long)]);
    base.materialized_view_base = Some(GoShared::new(MaterializedViewBaseInfo { mlog_id, ..Default::default() }));
    let mut mlog = table(
        mlog_id,
        "$mlog$t",
        vec![
            mk_col(1, "a", 0, FieldTypeCode::Long),
            mk_col(3, tidb_model::table::MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME, 1, FieldTypeCode::Varchar),
        ],
    );
    mlog.materialized_view_log = Some(GoShared::new(MaterializedViewLogInfo {
        base_table_id: base_id,
        columns: vec![tidb_ast::CiString::new("a")].into(),
        ..Default::default()
    }));
    let mv = mview_table(
        mv_id,
        "mv",
        vec![mk_col(1, "x", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        vec![base_id],
        "select a, count(1) from t group by a",
    );
    let is = mock_info_schema(&[&base, &mlog, &mv]);
    let err = mview::build(&parse, &is, &mv, BuildOptions { from_ts: 1, to_ts: 0 }, None).unwrap_err();
    assert!(err.contains(tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME), "{err}");
}

/// The base table `t(a, b)` and the diff tests' view over it.
fn diff_tables(base_id: i64, mv_id: i64, mv_name: &str, mv_columns: Vec<ColumnInfo>, sql: &str) -> (TableInfo, TableInfo) {
    let base = table(base_id, "t", vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "b", 1, FieldTypeCode::Long)]);
    let mv = mview_table(mv_id, mv_name, mv_columns, vec![base_id], sql);
    (base, mv)
}

#[test]
fn test_build_complete_diff_source_layout() {
    let (base, mut mv) = diff_tables(
        11,
        12,
        "mv_tbl_diff",
        vec![
            mk_col(1, "a", 0, FieldTypeCode::Long),
            mk_col(2, "cnt", 1, FieldTypeCode::LongLong),
            mk_col(3, "s", 2, FieldTypeCode::LongLong),
        ],
        "select a, count(1), sum(b) from t group by a",
    );
    col_at(&base, 0).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    mv.pk_is_handle = true;
    col_at(&mv, 0).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL | tidb_datatype::FieldTypeFlags::PRI_KEY);
    col_at(&mv, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let is = mock_info_schema(&[&base, &mv]);

    let res = mview::build_complete_diff_source(&parse, &is, &mv).unwrap();
    let db = tidb_hack::go_to_lower(&is.schema_name_by_table_id(mv.id).unwrap());
    let hints = &res.diff_source_select.hints;
    assert_eq!(hints.len(), 2);
    let storage_hint = &hints[0];
    assert_eq!(tidb_hack::go_to_lower(&storage_hint.name), "read_from_storage" /* Go hint.HintReadFromStorage */);
    let tidb_ast::HintKind::ReadFromStorage { groups, .. } = &storage_hint.kind else {
        panic!("not a READ_FROM_STORAGE hint: {storage_hint:?}");
    };
    assert_eq!(groups.len(), 1);
    assert_eq!(tidb_hack::go_to_lower(&groups[0].0), "tiflash" /* Go hint.HintTiFlash */);
    assert_eq!(groups[0].1.len(), 1);
    assert_eq!(tidb_hack::go_to_lower(&groups[0].1[0].name), mview::DIFF_CURRENT_ALIAS);
    assert_eq!(groups[0].1[0].db_name.as_deref().map(tidb_hack::go_to_lower), Some(db.clone()));
    let probe_hint = &hints[1];
    assert_eq!(tidb_hack::go_to_lower(&probe_hint.name), "hash_join_probe" /* Go hint.HintHashJoinProbe */);
    let tidb_ast::HintKind::Tables { tables, .. } = &probe_hint.kind else {
        panic!("not a table hint: {probe_hint:?}");
    };
    assert_eq!(tables.len(), 1);
    assert_eq!(tidb_hack::go_to_lower(&tables[0].name), mview::DIFF_CURRENT_ALIAS);
    assert_eq!(tables[0].db_name.as_deref().map(tidb_hack::go_to_lower), Some(db));

    let ctx = mview_test_context().with_full_outer_join(true);
    let (plan, _) = optimize_for_test_in(&ctx, &is, &res.diff_source_select);
    let hash_join = find_hash_join_plan(&plan).expect("hash join");
    assert_eq!(hash_join.join_type, tidb_planner_core_base::LogicalJoinType::FullOuter);
    // Go `RightIsBuildSide`.
    let right_is_build_side =
        if hash_join.use_outer_to_build { hash_join.inner_child_idx == 0 } else { hash_join.inner_child_idx != 0 };
    assert!(!right_is_build_side);
    assert_eq!(12, res.mv_table_id);
    assert_eq!(mv.columns.len(), res.mv_column_count);
    assert_eq!(0, res.op_col_offset);
    assert_eq!(0, res.marker_mv_offset);
    assert_eq!(1, res.current_handle_cols.num_cols());
    assert_eq!(res.current_row_offsets.len(), mv.columns.len());
    assert_eq!(res.recomputed_row_offsets.len(), mv.columns.len());
    assert_eq!(1 + 2 * mv.columns.len(), res.source_column_count);
    res.validate_source_layout(res.source_column_count).unwrap();
    assert_eq!(res.current_row_offsets[0] as i64, res.current_handle_cols.get_col(0).index);

    let join = res.diff_source_select.from.as_ref().unwrap();
    assert_eq!(join.tp, tidb_ast::JoinType::Full);
    let join_predicates = collect_and_predicates(join.on.as_ref().expect("on"));
    assert_eq!(join_predicates.len(), 1);
    let (left_table, left_col) = column_name_ref(join_predicates[0].1);
    let (right_table, right_col) = column_name_ref(join_predicates[0].2);
    assert_eq!(mview::DIFF_RECOMPUTED_ALIAS, left_table);
    assert_eq!(mview::DIFF_CURRENT_ALIAS, right_table);
    assert_eq!(left_col, right_col);
    assert_eq!("a", right_col);
    assert_eq!(*join_predicates[0].0, tidb_ast::BinaryOp::Eq);
    let payload_cmp_ops = collect_payload_comparison_ops(res.diff_source_select.where_clause.as_ref().expect("where"));
    assert_eq!(
        payload_cmp_ops,
        std::collections::HashMap::from([
            ("cnt".to_owned(), tidb_ast::BinaryOp::Eq),
            ("s".to_owned(), tidb_ast::BinaryOp::NullEq),
        ])
    );
}

#[test]
fn test_build_complete_diff_source_nullable_group_key_uses_null_eq() {
    let (base, mv) = diff_tables(
        21,
        22,
        "mv_tbl_diff_nullable_gk",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        "select a, count(1) from t group by a",
    );
    col_at(&mv, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let is = mock_info_schema(&[&base, &mv]);

    let res = mview::build_complete_diff_source(&parse, &is, &mv).unwrap();
    assert_eq!(1, res.marker_mv_offset);
    assert_eq!(1, res.current_handle_cols.num_cols());
    assert_eq!(1, res.current_handle_cols.get_col(0).index);
    assert_eq!(1 + 1 + 2 * mv.columns.len(), res.source_column_count);
    let join_predicates = collect_and_predicates(res.diff_source_select.from.as_ref().unwrap().on.as_ref().expect("on"));
    assert_eq!(join_predicates.len(), 1);
    let (left_table, left_col) = column_name_ref(join_predicates[0].1);
    let (right_table, right_col) = column_name_ref(join_predicates[0].2);
    assert_eq!(mview::DIFF_RECOMPUTED_ALIAS, left_table);
    assert_eq!(mview::DIFF_CURRENT_ALIAS, right_table);
    assert_eq!(left_col, right_col);
    assert_eq!("a", right_col);
    assert_eq!(*join_predicates[0].0, tidb_ast::BinaryOp::NullEq);
}

#[test]
fn test_build_complete_diff_source_common_handle_reuses_old_row_image() {
    let (base, mut mv) = diff_tables(
        31,
        32,
        "mv_tbl_diff_common_handle",
        vec![
            mk_col(1, "a", 0, FieldTypeCode::Long),
            mk_col(2, "b", 1, FieldTypeCode::Long),
            mk_col(3, "cnt", 2, FieldTypeCode::LongLong),
        ],
        "select a, b, count(1) from t group by a, b",
    );
    col_at(&base, 0).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    col_at(&base, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    mv.is_common_handle = true;
    let mut primary = index(1, "PRIMARY", &[("b", 1), ("a", 0)], false);
    primary.primary = true;
    primary.unique = true;
    mv.indices = vec![primary].into();
    for col in mv.columns.iter_deref() {
        col.write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    }
    let is = mock_info_schema(&[&base, &mv]);

    let res = mview::build_complete_diff_source(&parse, &is, &mv).unwrap();
    assert_eq!(1 + 2 * mv.columns.len(), res.source_column_count);
    assert_eq!(2, res.current_handle_cols.num_cols());
    res.validate_source_layout(res.source_column_count).unwrap();
    assert_eq!(res.current_row_offsets[1] as i64, res.current_handle_cols.get_col(0).index);
    assert_eq!(res.current_row_offsets[0] as i64, res.current_handle_cols.get_col(1).index);
}

#[test]
fn test_build_complete_diff_source_extra_handle_keeps_row_id_projection() {
    let (base, mv) = diff_tables(
        41,
        42,
        "mv_tbl_diff_rowid_handle",
        vec![mk_col(1, "a", 0, FieldTypeCode::Long), mk_col(2, "cnt", 1, FieldTypeCode::LongLong)],
        "select a, count(1) from t group by a",
    );
    col_at(&base, 0).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    col_at(&mv, 0).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    col_at(&mv, 1).write().field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
    let is = mock_info_schema(&[&base, &mv]);

    let res = mview::build_complete_diff_source(&parse, &is, &mv).unwrap();
    assert_eq!(2 + 2 * mv.columns.len(), res.source_column_count);
    assert_eq!(1, res.current_handle_cols.num_cols());
    res.validate_source_layout(res.source_column_count).unwrap();
    assert_eq!(tidb_model::column::EXTRA_HANDLE_ID, res.current_handle_cols.get_col(0).id);
    assert_eq!(1, res.current_handle_cols.get_col(0).index);
    assert!(!res.current_row_offsets.contains(&(res.current_handle_cols.get_col(0).index as usize)));
}
