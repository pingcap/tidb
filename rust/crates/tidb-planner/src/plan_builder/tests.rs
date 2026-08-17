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

//! The seam tests. WRITTEN, not transcreated: Go's builder tests run against a
//! live session, a real `infoschema` and a `testkit` cluster, none of which
//! exist here. What is proven is the SEAM — that a parsed statement, a
//! [`TableSource`] and the already-ported logical tree compose into a plan
//! with the schema and output names Go's own build would produce, and that
//! [`crate::expression_rewriter`] is callable from the builder UNCHANGED.

use std::collections::BTreeMap;

use tidb_ast::{Expr, SelectStmt, Stmt};
use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone};
use tidb_expr::expression::Expression;
use tidb_expr::ZonedNoColumns;

use super::catalog::{SourceColumn, SourceIndex, SourceIndexColumn, SourceTable, TableSource};
use super::marker::{MarkerKind, PlanMarker};
use super::{
    constant_is_always_false, snapshot_schema_and_names, PlanBuilder, EXTRA_COMMIT_TS_ID,
    EXTRA_COMMIT_TS_NAME,
};
use crate::expression_rewriter::ColumnIdAllocator;
use crate::logical::rule::flags;
use crate::logical::LogicalPlan;
use crate::plan_base::PlanIdAllocator;

// ***** a TableSource implementation, which is what a downstream crate writes *****

struct TestCatalog {
    current_database: String,
    tables: Vec<SourceTable>,
}

impl TableSource for TestCatalog {
    fn current_database(&self) -> &str {
        &self.current_database
    }

    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable> {
        self.tables.iter().find(|table| {
            table.db_name.eq_ignore_ascii_case(db_name)
                && table.table_name.eq_ignore_ascii_case(table_name)
        })
    }

    fn database_exists(&self, db_name: &str) -> bool {
        self.tables
            .iter()
            .any(|table| table.db_name.eq_ignore_ascii_case(db_name))
    }
}

fn column(offset: usize, name: &str, primary: bool) -> SourceColumn {
    SourceColumn {
        id: offset as i64 + 1,
        name: name.to_owned(),
        is_primary_key: primary,
        offset,
        ret_type: FieldType::new(FieldTypeCode::LongLong),
        is_public: true,
        is_hidden: false,
        is_virtual_generated: false,
    }
}

/// `CREATE TABLE test.t (a BIGINT PRIMARY KEY, b BIGINT, KEY idx_b(b))`.
fn catalog() -> TestCatalog {
    TestCatalog {
        current_database: "test".to_owned(),
        tables: vec![SourceTable {
            table_id: 100,
            table_name: "t".to_owned(),
            db_name: "test".to_owned(),
            physical_table_id: 100,
            columns: vec![column(0, "a", true), column(1, "b", false)],
            indexes: vec![SourceIndex {
                id: 1,
                name: "idx_b".to_owned(),
                columns: vec![SourceIndexColumn {
                    name: "b".to_owned(),
                    offset: 1,
                    length: -1,
                }],
                is_public: true,
                is_visible: true,
                ..SourceIndex::default()
            }],
            pk_is_handle: true,
            handle_col_offsets: vec![0],
            ..SourceTable::default()
        }],
    }
}

struct Harness {
    catalog: TestCatalog,
    ctx: ZonedNoColumns,
    plan_ids: PlanIdAllocator,
    column_ids: ColumnIdAllocator,
}

impl Harness {
    fn new() -> Self {
        Self {
            catalog: catalog(),
            ctx: ZonedNoColumns(SessionTimeZone::utc()),
            plan_ids: PlanIdAllocator::default(),
            column_ids: ColumnIdAllocator::new(),
        }
    }

    fn builder(&self) -> PlanBuilder<'_, TestCatalog, ZonedNoColumns> {
        PlanBuilder::new(
            &self.catalog,
            &self.ctx,
            &self.plan_ids,
            &self.column_ids,
            SessionTimeZone::utc(),
        )
    }
}

fn parse_select(sql: &str) -> SelectStmt {
    match tidb_parser::parse(sql).expect("the seam's SQL parses") {
        Stmt::Query(query) => match query.into_inner() {
            tidb_ast::QueryStmt::Select(select) => *select,
            other => panic!("expected a SELECT, got {other:?}"),
        },
        other => panic!("expected a SELECT, got {other:?}"),
    }
}

fn column_names(plan: &LogicalPlan) -> Vec<String> {
    plan.output_names()
        .iter()
        .map(|name| name.names.column.original.clone())
        .collect()
}

// ***** the seam, end to end *****

#[test]
fn test_the_seam_builds_a_real_logical_plan() {
    // THE SEAM TEST. One statement, through every spine piece:
    // buildResultSetNode -> buildDataSource -> buildSelection -> buildProjection
    // -> buildSort -> buildLimit.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT a+1 FROM t WHERE b>1 ORDER BY a LIMIT 3");
    let (plan, opt_flag) = builder.build_select(&select).expect("the seam builds");

    // The shape Go builds, top down:
    //   Projection(trim) -> Limit -> Sort -> Projection -> Selection -> DataSource
    // The OUTER projection is `buildSelect`'s `:4640` trailing trim: `ORDER BY
    // a` needs `a`, which `SELECT a+1` does not project, so the inner
    // projection carries it as a hidden extra column and this one drops it.
    let LogicalPlan::Projection(trim) = &plan else {
        panic!(
            "expected the trailing trim Projection at the root, got {}",
            plan.tp()
        );
    };
    assert_eq!(trim.exprs.len(), 1);

    let LogicalPlan::Limit(limit) = &trim.base.children()[0] else {
        panic!("expected a Limit under the trim Projection");
    };
    assert_eq!(limit.count, 3);
    assert_eq!(limit.offset, 0);

    let LogicalPlan::Sort(sort) = &limit.base.children()[0] else {
        panic!("expected a Sort under the Limit");
    };
    assert_eq!(sort.by_items.len(), 1);
    assert!(!sort.by_items[0].desc);

    let LogicalPlan::Projection(projection) = &sort.base.children()[0] else {
        panic!("expected a Projection under the Sort");
    };
    // `a+1` plus the hidden `a` the ORDER BY needed.
    assert_eq!(projection.exprs.len(), 2);
    assert!(projection.base.base.output_names()[1].hidden);
    // `a+1` is a scalar function over the data source's `a`, not a bare column.
    assert!(matches!(projection.exprs[0], Expression::ScalarFunction(_)));
    assert!(matches!(projection.exprs[1], Expression::Column(_)));

    // The ORDER BY item bound through a `#order#1` marker to that hidden
    // column, which is what proves the marker scheme carries a real clause.
    let Expression::Column(sort_column) = &sort.by_items[0].expr else {
        panic!("the ORDER BY item must resolve to the hidden projection column");
    };
    assert_eq!(sort_column.index, 1);

    let LogicalPlan::Selection(selection) = &projection.base.children()[0] else {
        panic!("expected a Selection under the Projection");
    };
    assert_eq!(selection.conditions.len(), 1);

    let LogicalPlan::DataSource(data_source) = &selection.base.children()[0] else {
        panic!("expected a DataSource at the leaf");
    };
    assert_eq!(data_source.table_id, 100);
    assert_eq!(data_source.db_name, "test");
    assert!(data_source.pk_is_handle);

    // The plan's SCHEMA and OUTPUT NAMES are the projection's, seen through
    // the Sort and the Limit, which have neither of their own — Go's
    // own-then-first-child rule.
    assert_eq!(plan.schema().expect("a schema").len(), 1);
    assert_eq!(column_names(&plan), vec!["a+1".to_owned()]);

    // opt_flag accumulated across the clauses: WHERE set predicate push-down,
    // the projection set projection elimination, LIMIT set TopN push-down.
    assert_ne!(opt_flag & flags::PREDICATE_PUSH_DOWN, 0);
    assert_ne!(opt_flag & flags::ELIMINATE_PROJECTION, 0);
    assert_ne!(opt_flag & flags::PUSH_DOWN_TOPN, 0);
    // Nothing on this statement asks for GC substitution or the partition
    // processor, so neither bit is set by accident.
    assert_eq!(opt_flag & flags::GC_SUBSTITUTE, 0);
    assert_eq!(opt_flag & flags::PARTITION_PROCESSOR, 0);
}

#[test]
fn test_expression_rewriter_is_callable_from_the_builder_unchanged() {
    // The batch-5 rewriter's INTERFACE requirement: the builder hands it a
    // `RewriterEnv` and an `ExprRewriterPlanCtx` and nothing in that module
    // changes. Building a MaxOneRow over a built plan exercises both.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let (inner, _) = builder
        .build_select(&parse_select("SELECT a FROM t"))
        .expect("the inner plan builds");

    builder.cur_clause = crate::expression_rewriter::ClauseCode::Where;
    builder.outer_schemas.push(
        inner
            .schema()
            .cloned()
            .expect("the projection carries a schema"),
    );
    builder.outer_names.push(inner.output_names().to_vec());

    let rewriter = builder.expression_rewriter();
    assert_eq!(
        rewriter.clause(),
        crate::expression_rewriter::ClauseCode::Where
    );
    let max_one_row = rewriter.build_max_one_row(inner);
    assert_eq!(max_one_row.tp(), "MaxOneRow");
    assert_eq!(max_one_row.base().children().len(), 1);
}

// ***** each spine piece *****

#[test]
fn test_table_source_drives_data_source_construction() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT * FROM t");
    let plan = builder
        .build_result_set_node(&tidb_ast::JoinNode::Join(Box::new(
            select.from.clone().expect("a FROM clause"),
        )))
        .expect("the data source builds");

    let LogicalPlan::DataSource(data_source) = &plan else {
        panic!("expected a DataSource");
    };
    // Two declared columns plus the extra commit-ts column Go appends at
    // `logical_plan_builder.go:5244`. No extra HANDLE column: `a` is the
    // int handle already.
    assert_eq!(data_source.columns.len(), 3);
    assert_eq!(data_source.columns[2].id, EXTRA_COMMIT_TS_ID);
    assert_eq!(data_source.columns[2].name, EXTRA_COMMIT_TS_NAME);
    assert_eq!(data_source.handle_cols.len(), 1);
    assert!(data_source.handle_is_int);
    assert_eq!(plan.schema().expect("a schema").len(), 3);
    assert_eq!(
        column_names(&plan),
        vec![
            "a".to_owned(),
            "b".to_owned(),
            EXTRA_COMMIT_TS_NAME.to_owned()
        ]
    );

    // The handle map reached the helper, keyed by the LOGICAL table id.
    let tail = builder.handle_helper.tail_map().expect("a pushed map");
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[&100].len(), 1);
    assert!(tail[&100][0].is_int());
}

#[test]
fn test_handle_less_table_gets_the_extra_row_id_handle() {
    let mut harness = Harness::new();
    harness.tables_mut()[0].pk_is_handle = false;
    harness.tables_mut()[0].handle_col_offsets.clear();
    let mut builder = harness.builder();
    let plan = builder
        .build_data_source(&tidb_ast::TableRef {
            name: vec!["t".to_owned()],
            partitions: Vec::new(),
            alias: None,
            as_of: None,
            hints: Vec::new(),
            sample: None,
        })
        .expect("the data source builds");

    let LogicalPlan::DataSource(data_source) = &plan else {
        panic!("expected a DataSource");
    };
    // `a`, `b`, `_tidb_rowid`, `_tidb_commit_ts` — the extra handle is
    // appended BEFORE the commit-ts column, as Go does.
    assert_eq!(data_source.columns.len(), 4);
    assert_eq!(
        data_source.columns[2].id,
        crate::logical::data_source::EXTRA_HANDLE_ID
    );
    assert!(data_source.handle_is_int);
    assert_eq!(data_source.handle_cols.len(), 1);
}

#[test]
fn test_wildcard_expansion_skips_the_extra_columns() {
    // `unfoldWildStar` (`:4115`) excludes by column ID, which is why
    // `build_data_source` may append `_tidb_rowid` / `_tidb_commit_ts` at all.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT * FROM t"))
        .expect("the wildcard builds");
    assert_eq!(column_names(&plan), vec!["a".to_owned(), "b".to_owned()]);
    assert_eq!(plan.schema().expect("a schema").len(), 2);
}

#[test]
fn test_selection_folds_an_always_false_predicate_to_a_dual() {
    // `buildSelection`'s `:1381` arm. The dual keeps the SOURCE's schema and
    // names, which is the read-after-move rule's first customer.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT a FROM t WHERE 0"))
        .expect("the always-false predicate builds");

    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected a Projection at the root");
    };
    let LogicalPlan::TableDual(dual) = &projection.base.children()[0] else {
        panic!("expected the Selection to have folded to a TableDual");
    };
    assert_eq!(dual.row_count, 0);
    // The dual carries the data source's own schema, not an empty one.
    assert_eq!(dual.base.base.schema().expect("a schema").len(), 3);

    // An always-TRUE conjunct is dropped instead, leaving no Selection at all.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT a FROM t WHERE 1"))
        .expect("the always-true predicate builds");
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected a Projection at the root");
    };
    assert!(matches!(
        projection.base.children()[0],
        LogicalPlan::DataSource(_)
    ));
}

#[test]
fn test_limit_zero_becomes_a_zero_row_dual() {
    // `buildLimit`'s `:2588` arm, the read-after-move rule's second customer.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT a FROM t LIMIT 0"))
        .expect("LIMIT 0 builds");
    let LogicalPlan::TableDual(dual) = &plan else {
        panic!("expected a TableDual, got {}", plan.tp());
    };
    assert_eq!(dual.row_count, 0);
    // Rule 3: the projection's schema and names survived the move.
    assert_eq!(dual.base.base.schema().expect("a schema").len(), 1);
    assert_eq!(column_names(&plan), vec!["a".to_owned()]);
}

#[test]
fn test_sort_resolves_a_positional_order_by() {
    // `itemTransformer` (`:2380`): a bare integer is a select-list POSITION,
    // not a constant.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT b, a FROM t ORDER BY 2"))
        .expect("the positional ORDER BY builds");
    let LogicalPlan::Sort(sort) = &plan else {
        panic!("expected a Sort at the root");
    };
    let Expression::Column(column) = &sort.by_items[0].expr else {
        panic!("a position must resolve to a column, not a constant");
    };
    assert_eq!(column.index, 1);

    // Out of range is Go's `ErrUnknownColumn`.
    let mut builder = harness.builder();
    assert!(builder
        .build_select(&parse_select("SELECT a FROM t ORDER BY 9"))
        .is_err());
}

#[test]
fn test_projection_names_follow_go_s_two_rules() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    // A bare column keeps its origin names (`:1537`); an alias replaces only
    // ColName; an expression is named by its restored text (`:1445`).
    let (plan, _) = builder
        .build_select(&parse_select("SELECT a, b AS bee, a+1 FROM t"))
        .expect("the projection builds");
    assert_eq!(
        column_names(&plan),
        vec!["a".to_owned(), "bee".to_owned(), "a+1".to_owned()]
    );
    let names = plan.output_names();
    // The aliased column keeps its ORIGIN table and column.
    assert_eq!(names[1].names.original_column.original, "b");
    assert_eq!(names[1].names.table.original, "t");
    // The computed column has no table qualifier at all, as in Go.
    assert!(names[2].names.table.original.is_empty());
}

#[test]
fn test_table_alias_renames_the_output_names() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT x.a FROM t AS x"))
        .expect("the aliased table builds");
    assert_eq!(column_names(&plan), vec!["a".to_owned()]);
    assert_eq!(plan.output_names()[0].names.table.original, "x");
    // The ORIGIN table name survives the alias.
    assert_eq!(plan.output_names()[0].names.original_table.original, "t");
}

#[test]
fn test_no_from_clause_builds_a_one_row_dual() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT 1"))
        .expect("a FROM-less select builds");
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected a Projection");
    };
    let LogicalPlan::TableDual(dual) = &projection.base.children()[0] else {
        panic!("expected a one-row TableDual under it");
    };
    assert_eq!(dual.row_count, 1);
    // `buildTableDual` pushes an empty handle map (`:4659`).
    assert_eq!(builder.handle_helper.depth(), 1);
}

#[test]
fn test_generated_column_index_sets_the_gc_substitute_flag() {
    let mut harness = Harness::new();
    harness.tables_mut()[0].columns[1].is_virtual_generated = true;
    let mut builder = harness.builder();
    builder
        .build_select(&parse_select("SELECT a FROM t"))
        .expect("the plan builds");
    // `logical_plan_builder.go:5102`: only an INDEX on a virtual generated
    // column enables the substitution.
    assert_ne!(builder.opt_flag & flags::GC_SUBSTITUTE, 0);
}

#[test]
fn test_partitioned_table_sets_the_partition_processor_flag() {
    let mut harness = Harness::new();
    harness.tables_mut()[0].partition_definition_names = vec!["p0".to_owned(), "p1".to_owned()];
    harness.tables_mut()[0].partition_def_idx = Some(1);
    let mut builder = harness.builder();
    builder
        .build_select(&parse_select("SELECT a FROM t"))
        .expect("the plan builds");
    assert_ne!(builder.opt_flag & flags::PARTITION_PROCESSOR, 0);
}

#[test]
fn test_unknown_database_and_table_are_distinguished() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let error = builder
        .build_select(&parse_select("SELECT a FROM nosuchdb.t"))
        .expect_err("an unknown database is an error");
    assert!(error.message().contains("Unknown database"));

    let mut builder = harness.builder();
    let error = builder
        .build_select(&parse_select("SELECT a FROM nosuchtable"))
        .expect_err("an unknown table is an error");
    assert!(error.message().contains("doesn't exist"));
}

#[test]
fn test_unported_clauses_name_their_go_symbol() {
    // 6c landed GROUP BY, HAVING and DISTINCT and 6d landed `WITH`
    // (`buildWith`), so `WINDOW` is the one clause whose builder is still a
    // later batch.
    let sql = "SELECT a FROM t WINDOW w AS (ORDER BY a)";
    let symbol = "buildWindowFunctions";
    let harness = Harness::new();
    let mut builder = harness.builder();
    let error = builder
        .build_select(&parse_select(sql))
        .expect_err("an unported clause is an explicit error");
    assert!(
        error.message().contains(symbol),
        "`{sql}` must name {symbol}, said: {}",
        error.message()
    );
}

// ***** the marker scheme, through the resolver *****

#[test]
fn test_a_marker_round_trips_through_a_clause_rewrite() {
    // The producing half: a pass substitutes a marker into the clause. The
    // reading half: `PlanScopeResolver` binds it to the producer's column.
    let harness = Harness::new();
    let builder = harness.builder();
    let (schema, names) = {
        let mut builder = harness.builder();
        let (plan, _) = builder
            .build_select(&parse_select("SELECT a FROM t"))
            .expect("the producer builds");
        snapshot_schema_and_names(&plan)
    };

    let produced = tidb_expr::column::Column::new(9_999, FieldType::new(FieldTypeCode::LongLong));
    let mut markers = BTreeMap::new();
    markers.insert(MarkerKind::Agg, vec![produced.clone()]);

    // `HAVING count(*) > 1` after 6c's substitution: the aggregate call has
    // become `#agg#0`.
    let mut clause = Expr::Column(vec!["count(*)".to_owned()]);
    super::marker::substitute(&mut clause, PlanMarker::new(MarkerKind::Agg, 0));
    let built = builder
        .rewrite_scalar(&clause, &schema, &names, &markers)
        .expect("the marker resolves");
    let Expression::Column(column) = built else {
        panic!("a marker must resolve to the producer's column");
    };
    assert_eq!(column.unique_id, produced.unique_id);

    // The SAME index under a different kind is not this marker.
    let mut other = Expr::Null;
    super::marker::substitute(&mut other, PlanMarker::new(MarkerKind::Window, 0));
    assert!(builder
        .rewrite_scalar(&other, &schema, &names, &markers)
        .is_err());
}

#[test]
fn test_an_unbound_marker_falls_through_to_name_resolution() {
    // Rule 6 / the collision note: an undecodable-to-a-column marker is
    // resolved as an ordinary name, so a user-written `#agg#0` is not hijacked.
    let harness = Harness::new();
    let builder = harness.builder();
    let (schema, names) = {
        let mut builder = harness.builder();
        let (plan, _) = builder
            .build_select(&parse_select("SELECT a FROM t"))
            .expect("the producer builds");
        snapshot_schema_and_names(&plan)
    };
    let markers = BTreeMap::new();
    let clause = PlanMarker::new(MarkerKind::Agg, 0).as_expr();
    // No column is named `#agg#0`, so it is an unresolved column reference,
    // not a panic and not a silent wrong binding.
    assert!(builder
        .rewrite_scalar(&clause, &schema, &names, &markers)
        .is_err());
}

// ***** the small decisions *****

#[test]
fn test_constant_is_always_false_matches_go_s_eval_bool() {
    use tidb_datatype::Datum;
    use tidb_expr::constant::Constant;

    let typed = |value: Datum| Constant::new(value, FieldType::new(FieldTypeCode::LongLong));
    assert_eq!(constant_is_always_false(&typed(Datum::Int(0))), Some(true));
    assert_eq!(constant_is_always_false(&typed(Datum::Int(1))), Some(false));
    // Go treats a NULL predicate as filtering every row.
    assert_eq!(constant_is_always_false(&typed(Datum::Null)), Some(true));
    // A string is not decided here; it is kept as a condition.
    assert_eq!(
        constant_is_always_false(&typed(Datum::Bytes(b"x".to_vec()))),
        None
    );
}

#[test]
fn test_opt_flag_accumulates_and_is_returned_beside_the_plan() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    assert_eq!(builder.get_opt_flag(), 0);
    builder.add_opt_flag(flags::DECORRELATE);
    builder.add_opt_flag(flags::JOIN_REORDER);
    assert_eq!(
        builder.get_opt_flag(),
        flags::DECORRELATE | flags::JOIN_REORDER
    );
    // The build ORs its own flags on top and hands the total back.
    let (_, returned) = builder
        .build_select(&parse_select("SELECT a FROM t LIMIT 1"))
        .expect("the plan builds");
    assert_eq!(returned, builder.get_opt_flag());
    assert_ne!(returned & flags::DECORRELATE, 0);
    assert_ne!(returned & flags::PUSH_DOWN_TOPN, 0);
}

#[test]
fn test_select_offset_follows_the_query_block_stack() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    assert_eq!(builder.select_offset(), -1);
    builder.qb_offset.push(1);
    builder.qb_offset.push(2);
    assert_eq!(builder.select_offset(), 2);
    builder.qb_offset.pop();
    assert_eq!(builder.select_offset(), 1);
}

#[test]
fn test_rewrite_error_flattens_into_plan_error() {
    use crate::expression_rewriter::RewriteError;
    use crate::plan_base::PlanError;

    let error: PlanError = RewriteError::OperandColumns(2).into();
    assert!(error.message().contains("2 column(s)"));
}

impl Harness {
    fn tables_mut(&mut self) -> &mut Vec<SourceTable> {
        &mut self.catalog.tables
    }
}
