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
use tidb_datatype::{
    Datum, FieldName, FieldNameMetadata, FieldType, FieldTypeCode, IdentifierMetadata,
    SessionTimeZone,
};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::{Columns, EvalError, ZonedNoColumns};

use super::catalog::{SourceColumn, SourceIndex, SourceIndexColumn, SourceTable, TableSource};
use super::marker::{MarkerKind, PlanMarker};
use super::{
    constant_is_always_false, snapshot_schema_and_names, PlanBuilder, PlanScopeResolver,
    EXTRA_COMMIT_TS_ID, EXTRA_COMMIT_TS_NAME,
};
use crate::expression_rewriter::ColumnIdAllocator;
use crate::logical::rule::flags;
use crate::logical::rule::logical_optimize;
use crate::logical::rule_tests::test_context;
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
        generated_expr: None,
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

#[derive(Default)]
struct WarningColumns(std::sync::Mutex<Vec<(u16, String)>>);

impl Columns for WarningColumns {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn append_warning(&self, code: u16, message: &str) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((code, message.to_owned()));
    }
}

#[derive(Clone, Copy)]
struct LikeEscapeColumns(u8);

impl Columns for LikeEscapeColumns {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn time_zone(&self) -> SessionTimeZone {
        SessionTimeZone::utc()
    }

    fn like_default_escape(&self) -> u8 {
        self.0
    }
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

fn parse_select_with_sql_mode(sql: &str, sql_mode: tidb_parser::SqlMode) -> SelectStmt {
    match tidb_parser::parse_with_sql_mode(sql, sql_mode).expect("the seam's SQL parses") {
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

#[test]
fn test_like_rewrite_uses_the_statement_default_escape() {
    let catalog = catalog();
    let context = LikeEscapeColumns(0);
    let plan_ids = PlanIdAllocator::default();
    let column_ids = ColumnIdAllocator::new();
    let mut builder = PlanBuilder::new(
        &catalog,
        &context,
        &plan_ids,
        &column_ids,
        SessionTimeZone::utc(),
    );
    let select = parse_select_with_sql_mode(
        r"SELECT a FROM t WHERE b LIKE 'a\b'",
        tidb_parser::SqlMode {
            no_backslash_escapes: true,
            ..tidb_parser::SqlMode::default()
        },
    );
    let (plan, _) = builder.build_select(&select).expect("the LIKE plan builds");
    let mut like_escape = None;
    plan.walk_preorder(&mut |node| {
        let LogicalPlan::Selection(selection) = node else {
            return;
        };
        let Expression::ScalarFunction(like) = &selection.conditions[0] else {
            return;
        };
        let tidb_expr::expression::Expression::Constant(constant) = &like.args[2] else {
            return;
        };
        like_escape = Some(constant.value.clone());
    });
    assert_eq!(like_escape, Some(Datum::Int(0)));
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
            identity: Default::default(),
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
fn indexed_virtual_generated_expression_is_substituted_by_the_ordinary_rule() {
    let mut harness = Harness::new();
    let generated_select = parse_select("SELECT a + 1 FROM t");
    let tidb_ast::SelectField::Expr {
        expr: generated_expr,
        ..
    } = &generated_select.fields.fields()[0]
    else {
        panic!("expected generated expression")
    };
    let generated = &mut harness.tables_mut()[0].columns[1];
    generated.is_virtual_generated = true;
    generated.generated_expr = Some(generated_expr.clone());

    let mut builder = harness.builder();
    let (plan, flags) = builder
        .build_select(&parse_select("SELECT a + 1 FROM t WHERE a + 1 = 3"))
        .expect("the generated-column query builds");
    assert_ne!(flags & flags::GC_SUBSTITUTE, 0);

    let optimized = logical_optimize(&test_context(&harness.plan_ids), flags::GC_SUBSTITUTE, plan)
        .expect("generated-column substitution succeeds")
        .plan;
    let mut substituted = false;
    optimized.walk_preorder(&mut |plan| {
        let LogicalPlan::Selection(selection) = plan else {
            return;
        };
        let Expression::ScalarFunction(comparison) = &selection.conditions[0] else {
            return;
        };
        substituted = comparison
            .args
            .iter()
            .any(|argument| matches!(argument, Expression::Column(column) if column.id == 2));
    });
    assert!(substituted, "a + 1 must be replaced by generated column b");
}

#[test]
fn test_partitioned_table_sets_the_partition_processor_flag() {
    let mut harness = Harness::new();
    harness.tables_mut()[0].is_partitioned = true;
    harness.tables_mut()[0].partition_definition_names = vec!["p0".to_owned(), "p1".to_owned()];
    harness.tables_mut()[0].partition_def_idx = Some(1);
    let mut builder = harness.builder();
    builder
        .build_select(&parse_select("SELECT a FROM t"))
        .expect("the plan builds");
    assert_ne!(builder.opt_flag & flags::PARTITION_PROCESSOR, 0);
}

#[test]
fn test_dynamic_partition_pruning_retains_global_index_paths() {
    let mut harness = Harness::new();
    let table = &mut harness.tables_mut()[0];
    table.is_partitioned = true;
    table.partition_definition_names = vec!["p0".to_owned(), "p1".to_owned()];
    table.indexes[0].global = true;

    let mut builder = harness.builder();
    builder.set_partition_processor_enabled(false);
    let (plan, flags) = builder
        .build_select(&parse_select("SELECT a FROM t"))
        .expect("the dynamic-pruning plan builds");
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected projection");
    };
    let LogicalPlan::DataSource(source) = &projection.base.children()[0] else {
        panic!("expected data source");
    };
    assert_eq!(flags & flags::PARTITION_PROCESSOR, 0);
    assert!(source
        .enumerated_paths
        .contains(&crate::access_path::PossiblePath::Index { index: 0 }));

    let mut builder = harness.builder();
    let (plan, flags) = builder
        .build_select(&parse_select("SELECT a FROM t"))
        .expect("the static-pruning plan builds");
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected projection");
    };
    let LogicalPlan::DataSource(source) = &projection.base.children()[0] else {
        panic!("expected data source");
    };
    assert_ne!(flags & flags::PARTITION_PROCESSOR, 0);
    assert!(!source
        .enumerated_paths
        .contains(&crate::access_path::PossiblePath::Index { index: 0 }));
}

#[test]
fn test_partition_clause_matches_partition_metadata() {
    let mut harness = Harness::new();
    let mut builder = harness.builder();
    let error = builder
        .build_select(&parse_select("SELECT a FROM t PARTITION (p0)"))
        .expect_err("a partition clause on a nonpartitioned table is rejected");
    assert_eq!(
        error.kind(),
        &crate::plan_base::PlanErrorKind::PartitionClauseOnNonpartitioned
    );

    harness.tables_mut()[0].is_partitioned = true;
    harness.tables_mut()[0].partition_definition_names = vec!["p0".to_owned()];
    let mut builder = harness.builder();
    builder
        .build_select(&parse_select("SELECT a FROM t PARTITION (P0)"))
        .expect("partition names match case-insensitively");

    let mut builder = harness.builder();
    let error = builder
        .build_select(&parse_select("SELECT a FROM t PARTITION (Missing)"))
        .expect_err("an unknown partition is rejected");
    assert_eq!(
        error.kind(),
        &crate::plan_base::PlanErrorKind::UnknownPartition {
            partition: "missing".to_owned(),
            table: "t".to_owned(),
        }
    );
}

#[test]
fn test_table_index_hints_filter_ordinary_planner_paths() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT a FROM t USE INDEX (idx_b)"))
        .expect("the hinted plan builds");
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected projection");
    };
    let LogicalPlan::DataSource(source) = &projection.base.children()[0] else {
        panic!("expected data source");
    };
    assert_eq!(
        source.enumerated_paths,
        vec![crate::access_path::PossiblePath::Index { index: 0 }]
    );
    assert_eq!(
        source.forced_index_ids,
        std::collections::BTreeSet::from([1])
    );

    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT /*+ USE_INDEX(t, idx) */ a FROM t"))
        .expect("Go accepts a unique index-name prefix in a comment hint");
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected projection");
    };
    let LogicalPlan::DataSource(source) = &projection.base.children()[0] else {
        panic!("expected data source");
    };
    assert_eq!(
        source.enumerated_paths,
        vec![crate::access_path::PossiblePath::Index { index: 0 }]
    );
    assert_eq!(
        source.forced_index_ids,
        std::collections::BTreeSet::from([1])
    );

    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select(
            "SELECT /*+ ORDER_INDEX(t, PRIMARY) */ a FROM t",
        ))
        .expect("ORDER_INDEX may name the clustered PRIMARY table path");
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected projection");
    };
    let LogicalPlan::DataSource(source) = &projection.base.children()[0] else {
        panic!("expected data source");
    };
    assert!(matches!(
        source.enumerated_paths.as_slice(),
        [crate::access_path::PossiblePath::Table { .. }]
    ));
    assert!(source.force_keep_order_table_path);

    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select("SELECT a FROM t USE INDEX ()"))
        .expect("empty USE INDEX keeps only the table path");
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected projection");
    };
    let LogicalPlan::DataSource(source) = &projection.base.children()[0] else {
        panic!("expected data source");
    };
    assert!(matches!(
        source.enumerated_paths.as_slice(),
        [crate::access_path::PossiblePath::Table { .. }]
    ));

    let mut builder = harness.builder();
    let error = builder
        .build_select(&parse_select("SELECT a FROM t IGNORE INDEX (missing)"))
        .expect_err("table-syntax unknown indexes are errors");
    assert_eq!(
        error.kind(),
        &crate::plan_base::PlanErrorKind::KeyNotExists {
            key: "missing".to_owned(),
            table: "t".to_owned(),
        }
    );
}

#[test]
fn test_comment_index_hint_warnings_match_go() {
    let harness = Harness::new();
    let warnings = WarningColumns::default();
    let mut builder = PlanBuilder::new(
        &harness.catalog,
        &warnings,
        &harness.plan_ids,
        &harness.column_ids,
        SessionTimeZone::utc(),
    );
    builder
        .build_select(&parse_select(
            "SELECT /*+ USE_INDEX(t, missing) */ a FROM t",
        ))
        .expect("an unknown comment-style index is only a warning");
    assert_eq!(
        *warnings
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![(1176, "Key 'missing' doesn't exist in table 't'".to_owned())]
    );

    warnings
        .0
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
    builder
        .build_select(&parse_select(
            "SELECT /*+ ORDER_INDEX(missing, idx_b) */ a FROM t",
        ))
        .expect("an unmatched comment-style index hint is only a warning");
    assert_eq!(
        *warnings
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![(
            1815,
            "(test.missing, idx_b) is inapplicable, check whether the table(test.missing) exists"
                .to_owned()
        )]
    );
}

#[test]
fn test_read_from_storage_is_resolved_from_plan_hints_like_go() {
    use crate::logical::data_source::{PREFER_TIFLASH, PREFER_TIKV};

    let mut harness = Harness::new();
    harness.tables_mut()[0].has_tiflash_replica = true;
    let warnings = WarningColumns::default();

    let build = |sql: &str, engines: &str| {
        warnings
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        let mut builder = PlanBuilder::new(
            &harness.catalog,
            &warnings,
            &harness.plan_ids,
            &harness.column_ids,
            SessionTimeZone::utc(),
        );
        builder.set_isolation_read_engines(engines);
        let (plan, _) = builder
            .build_select(&parse_select(sql))
            .expect("the storage hint builds");
        let mut source = None;
        plan.walk_preorder(&mut |node| {
            if let LogicalPlan::DataSource(data_source) = node {
                source = Some(data_source.clone());
            }
        });
        (
            source.expect("the SELECT contains a DataSource"),
            warnings
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        )
    };

    let (source, warning) = build(
        "SELECT /*+ READ_FROM_STORAGE(TIKV[x]) */ a FROM t AS x",
        "tikv,tiflash,tidb",
    );
    assert_eq!(source.prefer_store_type, PREFER_TIKV);
    assert_eq!(source.prefer_partitions.get(&PREFER_TIKV), Some(&vec![]));
    assert!(warning.is_empty());

    let (source, warning) = build(
        "SELECT /*+ READ_FROM_STORAGE(TIFLASH[x]) */ a FROM t AS x",
        "tikv,tidb",
    );
    assert_eq!(source.prefer_store_type, 0);
    assert_eq!(
        warning,
        vec![(
            1815,
            "No available path for table test.t with the store type tiflash of the hint /*+ read_from_storage */, please check the status of the table replica and variable value of tidb_isolation_read_engines(map[0:{} 2:{}])".to_owned(),
        )]
    );

    let (source, warning) = build(
        "SELECT /*+ READ_FROM_STORAGE(TIKV[x], TIFLASH[x]) */ a FROM t AS x",
        "tikv,tiflash,tidb",
    );
    assert_eq!(source.prefer_store_type, 0);
    assert_eq!(source.prefer_partitions.get(&PREFER_TIKV), Some(&vec![]));
    assert_eq!(
        warning,
        vec![(
            1815,
            "Storage hints are conflict, you can only specify one storage type of table test.x"
                .to_owned(),
        )]
    );

    let (source, warning) = build(
        "SELECT /*+ READ_FROM_STORAGE(TIFLASH[x]) */ a FROM t AS x",
        "tikv,tiflash,tidb",
    );
    assert_eq!(source.prefer_store_type, PREFER_TIFLASH);
    assert_eq!(source.prefer_partitions.get(&PREFER_TIFLASH), Some(&vec![]));
    assert!(warning.is_empty());
}

#[test]
fn test_index_hints_respect_tikv_isolation_like_go() {
    use crate::access_path::PossiblePath;

    let mut harness = Harness::new();
    harness.tables_mut()[0].has_tiflash_replica = true;
    let warnings = WarningColumns::default();
    let build = |sql: &str| {
        warnings
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        let mut builder = PlanBuilder::new(
            &harness.catalog,
            &warnings,
            &harness.plan_ids,
            &harness.column_ids,
            SessionTimeZone::utc(),
        );
        builder.set_isolation_read_engines("tiflash");
        builder.build_select(&parse_select(sql))
    };

    let (plan, _) = build("SELECT a FROM t USE INDEX ()")
        .expect("an empty USE INDEX does not force TiKV when TiKV is disabled");
    let mut source = None;
    plan.walk_preorder(&mut |node| {
        if let LogicalPlan::DataSource(data_source) = node {
            source = Some(data_source.clone());
        }
    });
    assert_eq!(
        source
            .expect("the SELECT contains a DataSource")
            .enumerated_paths,
        vec![PossiblePath::TiFlashTable]
    );

    let error = build("SELECT a FROM t USE INDEX (idx_b)")
        .expect_err("a table-syntax TiKV index hint is an error without TiKV");
    assert_eq!(
        error.message(),
        "TiDB doesn't support index 'idx_b' in the isolation read engines(value: 'tiflash')"
    );

    let (plan, _) = build("SELECT /*+ USE_INDEX(t, idx_b) */ a FROM t")
        .expect("a comment-style TiKV index hint is downgraded to a warning");
    let mut source = None;
    plan.walk_preorder(&mut |node| {
        if let LogicalPlan::DataSource(data_source) = node {
            source = Some(data_source.clone());
        }
    });
    assert_eq!(
        source
            .expect("the SELECT contains a DataSource")
            .enumerated_paths,
        vec![PossiblePath::TiFlashTable]
    );
    assert_eq!(
        *warnings
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![(
            1105,
            "TiDB doesn't support index 'idx_b' in the isolation read engines(value: 'tiflash')"
                .to_owned(),
        )]
    );
}

#[test]
fn test_no_index_lookup_pushdown_matches_and_overrides_positive_hint() {
    let harness = Harness::new();
    let warnings = WarningColumns::default();
    let mut builder = PlanBuilder::new(
        &harness.catalog,
        &warnings,
        &harness.plan_ids,
        &harness.column_ids,
        SessionTimeZone::utc(),
    );
    let (plan, _) = builder
        .build_select(&parse_select(
            "SELECT /*+ INDEX_LOOKUP_PUSHDOWN(t, idx_b) NO_INDEX_LOOKUP_PUSHDOWN(t) */ a FROM t",
        ))
        .expect("conflicting lookup hints fall back to ordinary paths");
    let LogicalPlan::Projection(projection) = plan else {
        panic!("expected projection");
    };
    let LogicalPlan::DataSource(source) = &projection.base.children()[0] else {
        panic!("expected data source");
    };
    assert!(source.force_no_index_lookup_push_down);
    assert!(source.index_lookup_push_down_by.is_empty());
    assert!(matches!(
        source.enumerated_paths.as_slice(),
        [crate::access_path::PossiblePath::Table { .. }]
    ));
    assert_eq!(
        *warnings
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![(
            1815,
            "hint INDEX_LOOKUP_PUSHDOWN cannot be inapplicable, NO_INDEX_LOOKUP_PUSHDOWN is specified"
                .to_owned(),
        )]
    );
}

#[test]
fn test_lookup_pushdown_rejects_go_unsupported_index_metadata() {
    for (global, multi_valued, reason) in [
        (
            true,
            false,
            "the global index in partition table is not supported",
        ),
        (false, true, "multi-valued index is not supported"),
    ] {
        let mut harness = Harness::new();
        harness.tables_mut()[0].indexes[0].global = global;
        harness.tables_mut()[0].indexes[0].is_multi_valued = multi_valued;
        let warnings = WarningColumns::default();
        let mut builder = PlanBuilder::new(
            &harness.catalog,
            &warnings,
            &harness.plan_ids,
            &harness.column_ids,
            SessionTimeZone::utc(),
        );
        let (plan, _) = builder
            .build_select(&parse_select(
                "SELECT /*+ INDEX_LOOKUP_PUSHDOWN(t, idx_b) */ a FROM t",
            ))
            .expect("unsupported lookup-pushdown metadata is a warning");
        let LogicalPlan::Projection(projection) = plan else {
            panic!("expected projection");
        };
        let LogicalPlan::DataSource(source) = &projection.base.children()[0] else {
            panic!("expected data source");
        };
        assert!(source.index_lookup_push_down_by.is_empty());
        assert!(matches!(
            source.enumerated_paths.as_slice(),
            [crate::access_path::PossiblePath::Table { .. }]
        ));
        assert_eq!(
            *warnings
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![(
                1815,
                format!("hint INDEX_LOOKUP_PUSHDOWN is inapplicable, {reason}"),
            )]
        );
    }
}

#[test]
fn test_lookup_pushdown_support_gates_and_auto_policy_match_go() {
    use crate::access_path::{
        apply_table_index_hints, get_possible_access_paths, IndexLookupPushDownBy,
        IndexLookupPushDownPolicy, IndexLookupPushDownSession,
    };
    use crate::logical::data_source::DataSourceIndexHint;

    let hint = DataSourceIndexHint {
        kind: tidb_ast::IndexHintKind::Use,
        index_names: vec!["idx_b".to_owned()],
        partitions: Vec::new(),
        push_down_lookup: true,
        force_keep_order: false,
        force_no_keep_order: false,
        restored: "INDEX_LOOKUP_PUSHDOWN(t, idx_b)".to_owned(),
    };
    let resolve = |table: &SourceTable, session| {
        let paths = get_possible_access_paths(table, false, None, None, true, false).unwrap();
        apply_table_index_hints(
            table,
            &paths,
            &[],
            std::slice::from_ref(&hint),
            false,
            false,
            session,
            true,
            "tikv,tiflash,tidb",
        )
        .expect("lookup-pushdown hint resolution")
    };

    let cases: [(fn(&mut SourceTable, &mut IndexLookupPushDownSession), &str); 10] = [
        (
            |table: &mut SourceTable, _: &mut IndexLookupPushDownSession| {
                table.is_common_handle = true;
                table.common_handle_version = 0;
            },
            "common handle table with old encoding version is not supported",
        ),
        (
            |table, _| table.indexes[0].global = true,
            "the global index in partition table is not supported",
        ),
        (
            |table, _| table.is_temporary = true,
            "temporary table is not supported",
        ),
        (
            |table, _| table.is_cached = true,
            "cached table is not supported",
        ),
        (
            |table, _| table.indexes[0].is_multi_valued = true,
            "multi-valued index is not supported",
        ),
        (
            |_, session| session.repeatable_read = false,
            "transaction isolation level is not REPEATABLE-READ",
        ),
        (
            |_, session| session.leader_read = false,
            "only leader read is supported",
        ),
        (
            |_, session| session.staleness = true,
            "stale read is not supported",
        ),
        (
            |_, session| session.historical_read = true,
            "historical read is not supported",
        ),
        (
            |_, session| session.max_keys_read = 1,
            "tidb_max_keys_read is set",
        ),
    ];
    for (mutate, reason) in cases {
        let mut table = catalog().tables.remove(0);
        let mut session = IndexLookupPushDownSession::default();
        mutate(&mut table, &mut session);
        let result = resolve(&table, session);
        assert!(result.index_lookup_push_down_by.is_empty());
        assert_eq!(
            result.hint_warnings,
            vec![format!(
                "hint INDEX_LOOKUP_PUSHDOWN is inapplicable, {reason}"
            )]
        );
    }

    let table = catalog().tables.remove(0);
    let forced = resolve(
        &table,
        IndexLookupPushDownSession {
            policy: IndexLookupPushDownPolicy::Force,
            ..Default::default()
        },
    );
    assert_eq!(
        forced.index_lookup_push_down_by.get(&1),
        Some(&IndexLookupPushDownBy::Hint),
        "an explicit hint overrides the system-policy origin"
    );

    let paths = get_possible_access_paths(&table, false, None, None, true, false).unwrap();
    let auto = apply_table_index_hints(
        &table,
        &paths,
        &[],
        &[],
        false,
        false,
        IndexLookupPushDownSession {
            policy: IndexLookupPushDownPolicy::Force,
            ..Default::default()
        },
        true,
        "tikv,tiflash,tidb",
    )
    .unwrap();
    assert_eq!(
        auto.index_lookup_push_down_by.get(&1),
        Some(&IndexLookupPushDownBy::SysVar)
    );
    let affinity_only = apply_table_index_hints(
        &table,
        &paths,
        &[],
        &[],
        false,
        false,
        IndexLookupPushDownSession {
            policy: IndexLookupPushDownPolicy::AffinityForce,
            ..Default::default()
        },
        true,
        "tikv,tiflash,tidb",
    )
    .unwrap();
    assert!(affinity_only.index_lookup_push_down_by.is_empty());
}

#[test]
fn test_fast_index_hint_check_keeps_go_smaller_boundary() {
    let select = parse_select("SELECT /*+ USE_INDEX(t, idx) */ a FROM t");
    let table_hints = match &select.from.as_ref().expect("FROM").left {
        tidb_ast::JoinNode::Table(table) => &table.hints,
        other => panic!("expected table, got {other:?}"),
    };
    assert!(
        !crate::access_path::fast_index_is_available_by_hints(
            "test",
            "test",
            "t",
            Some("idx_b"),
            &select.hints,
            table_hints,
        ),
        "Go's fast path matches names exactly; ordinary planning alone accepts a unique prefix"
    );

    let select = parse_select("SELECT /*+ ORDER_INDEX(t, idx_b) */ a FROM t");
    let table_hints = match &select.from.as_ref().expect("FROM").left {
        tidb_ast::JoinNode::Table(table) => &table.hints,
        other => panic!("expected table, got {other:?}"),
    };
    assert!(
        crate::access_path::fast_index_is_available_by_hints(
            "test",
            "test",
            "t",
            None,
            &select.hints,
            table_hints,
        ),
        "pinned indexIsAvailableByHints ignores ORDER_INDEX in the fast path"
    );

    let select = parse_select("SELECT a FROM t IGNORE INDEX (idx_b)");
    let table_hints = match &select.from.as_ref().expect("FROM").left {
        tidb_ast::JoinNode::Table(table) => &table.hints,
        other => panic!("expected table, got {other:?}"),
    };
    assert!(crate::access_path::fast_index_is_available_by_hints(
        "test",
        "test",
        "t",
        None,
        &select.hints,
        table_hints,
    ));
    assert!(!crate::access_path::fast_index_is_available_by_hints(
        "test",
        "test",
        "t",
        Some("idx_b"),
        &select.hints,
        table_hints,
    ));
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
    // 6c landed GROUP BY, HAVING and DISTINCT, 6d landed `WITH`
    // (`buildWith`) and 6e landed `WINDOW` (`buildWindowFunctions`). What
    // remains refused inside the window stage is the `windowAggMap` half of
    // `resolveWindowFunction`; see [`super::window`]'s section 3.
    let sql = "SELECT ROW_NUMBER() OVER (ORDER BY SUM(a)) FROM t GROUP BY b";
    let symbol = "resolveWindowFunction";
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

#[test]
fn an_eval_error_keeps_its_typed_plan_kind() {
    use crate::plan_base::{PlanError, PlanErrorKind};

    let eval = EvalError::CollationCharsetMismatch {
        collation: "latin1_bin".to_owned(),
        charset: "utf8mb4".to_owned(),
    };
    let error: PlanError = eval.clone().into();
    assert_eq!(error.kind(), &PlanErrorKind::Eval(eval));
}

#[test]
fn plan_scope_resolver_uses_the_statement_connection_collation() {
    let schema = Schema::default();
    let names = [];
    let markers = BTreeMap::new();
    let resolver = PlanScopeResolver::new(&schema, &names, &markers, SessionTimeZone::utc())
        .with_connection_charset_info(("utf8mb4", "utf8mb4_general_ci"));
    let expression =
        tidb_expr::rewriter::rewrite_expr_resolved(&Expr::String("a".to_owned()), &resolver)
            .expect("literal rewrite");
    assert_eq!(
        expression
            .static_type()
            .expect("literal type")
            .collation_name(),
        "utf8mb4_general_ci"
    );
}

#[test]
fn plan_scope_resolver_uses_no_unsigned_subtraction_mode() {
    let select = parse_select("SELECT CAST(0 AS UNSIGNED) - 1");
    let tidb_ast::SelectField::Expr { expr, .. } = &select.fields.fields()[0] else {
        panic!("expected subtraction expression")
    };
    let schema = Schema::default();
    let names = [];
    let markers = BTreeMap::new();
    let resolver = PlanScopeResolver::new(&schema, &names, &markers, SessionTimeZone::utc())
        .with_no_unsigned_subtraction(true);
    let rewritten =
        tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver).expect("subtraction rewrite");
    let Expression::ScalarFunction(function) = rewritten else {
        panic!("subtraction should remain a scalar function")
    };
    assert!(!function
        .ret_type
        .as_ref()
        .expect("subtraction return type")
        .is_unsigned());
}

#[test]
fn plan_scope_resolver_keeps_char_using_for_runtime_warnings() {
    let select = parse_select("SELECT CHAR(65, -1, 67.5 USING utf8)");
    let tidb_ast::SelectField::Expr { expr, .. } = &select.fields.fields()[0] else {
        panic!("expected CHAR expression")
    };
    let schema = Schema::default();
    let names = [];
    let markers = BTreeMap::new();
    let resolver = PlanScopeResolver::new(&schema, &names, &markers, SessionTimeZone::utc());
    let rewritten = tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver)
        .expect("CHAR expression rewrite");
    let Expression::ScalarFunction(function) = rewritten else {
        panic!("CHAR USING must remain executable at statement time")
    };
    assert_eq!(function.func_name.lowercase(), "char_func");
}

#[test]
fn plan_scope_resolver_refines_integer_string_with_live_warning_context() {
    let mut column = tidb_expr::column::Column::new(1, FieldType::new(FieldTypeCode::LongLong));
    column.index = 0;
    let schema = Schema::new(vec![column]);
    let names = [FieldName::new(FieldNameMetadata {
        original_table: IdentifierMetadata::new("t"),
        original_column: IdentifierMetadata::new("a"),
        database: IdentifierMetadata::new("test"),
        table: IdentifierMetadata::new("t"),
        column: IdentifierMetadata::new("a"),
    })];
    let markers = BTreeMap::new();
    let warnings = WarningColumns::default();
    let resolver = PlanScopeResolver::new(&schema, &names, &markers, SessionTimeZone::utc())
        .with_warning_context(&warnings);
    let select = parse_select("SELECT a > '10ab'");
    let tidb_ast::SelectField::Expr { expr, .. } = &select.fields.fields()[0] else {
        panic!("expected comparison expression")
    };
    let rewritten =
        tidb_expr::rewriter::rewrite_expr_resolved(expr, &resolver).expect("comparison rewrite");
    let Expression::ScalarFunction(function) = rewritten else {
        panic!("comparison should remain a scalar function")
    };
    assert!(matches!(function.args[1], Expression::Constant(_)));
    assert_eq!(warnings.0.lock().unwrap().len(), 2);
}

impl Harness {
    fn tables_mut(&mut self) -> &mut Vec<SourceTable> {
        &mut self.catalog.tables
    }
}

#[test]
fn a_built_data_source_enumerates_its_access_paths() {
    // Go `getPossibleAccessPaths` (`planbuilder.go:1320`) runs during
    // `buildDataSource`: the fixture table (`a BIGINT PRIMARY KEY, KEY
    // idx_b(b)`) yields the int-handle table path first, then `idx_b`.
    use crate::access_path::PossiblePath;
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
    assert_eq!(
        data_source.enumerated_paths,
        vec![
            PossiblePath::Table {
                is_int_handle: true,
                primary_index: None
            },
            PossiblePath::Index { index: 0 },
        ]
    );
    // The GROWN lists stay empty at build time; the costing seam fills them.
    assert!(data_source.possible_access_paths.is_empty());
}

#[test]
fn use_index_merge_is_attached_to_the_matching_data_source() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT /*+ USE_INDEX_MERGE(x, idx_b) */ * FROM test.t AS x");
    let (plan, _) = builder.build_select(&select).expect("the SELECT builds");

    let mut matched = 0;
    plan.walk_preorder(&mut |node| {
        if let LogicalPlan::DataSource(source) = node {
            matched += 1;
            assert_eq!(source.index_merge_hints.len(), 1);
            assert_eq!(source.index_merge_hints[0].index_names, ["idx_b"]);
            assert!(source.index_merge_hints[0].partitions.is_empty());
        }
    });
    assert_eq!(matched, 1);
}

#[test]
fn use_index_merge_keeps_its_partition_scope() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select =
        parse_select("SELECT /*+ USE_INDEX_MERGE(x PARTITION(p1), idx_b) */ * FROM test.t AS x");
    let (plan, _) = builder.build_select(&select).expect("the SELECT builds");

    let mut matched = 0;
    plan.walk_preorder(&mut |node| {
        if let LogicalPlan::DataSource(source) = node {
            matched += 1;
            assert_eq!(source.index_merge_hints.len(), 1);
            assert_eq!(source.index_merge_hints[0].index_names, ["idx_b"]);
            assert_eq!(source.index_merge_hints[0].partitions, ["p1"]);
            assert_eq!(
                source.index_merge_hints[0].restored,
                "/*+ USE_INDEX_MERGE(x PARTITION(p1) idx_b) */"
            );
        }
    });
    assert_eq!(matched, 1);
}
