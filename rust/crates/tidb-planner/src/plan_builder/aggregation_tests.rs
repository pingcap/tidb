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

//! 6c's seam tests. WRITTEN, not transcreated, for the reason
//! [`super::tests`]' header gives: Go's builder tests need a live session and
//! a `testkit` cluster.
//!
//! What each group proves is named on the group.

use std::collections::BTreeMap;

use tidb_ast::{Expr, SelectStmt, Stmt};
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::ZonedNoColumns;

use super::catalog::{SourceColumn, SourceIndex, SourceIndexColumn, SourceTable, TableSource};
use super::marker::{MarkerKind, PlanMarker};
use super::{PlanBuilder, ProjectionField};
use crate::expression_rewriter::ColumnIdAllocator;
use crate::logical::LogicalPlan;
use crate::plan_base::PlanIdAllocator;

// ***** the catalogue *****

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
    let mut ret_type = FieldType::new(FieldTypeCode::LongLong);
    if primary {
        ret_type.set_flags(ret_type.flags() | FieldTypeFlags::NOT_NULL | FieldTypeFlags::PRI_KEY);
    }
    SourceColumn {
        id: offset as i64 + 1,
        name: name.to_owned(),
        is_primary_key: primary,
        offset,
        ret_type,
        is_public: true,
        is_hidden: false,
        is_virtual_generated: false,
    }
}

/// `CREATE TABLE test.t (a BIGINT PRIMARY KEY, b BIGINT, c BIGINT)`.
fn catalog() -> TestCatalog {
    TestCatalog {
        current_database: "test".to_owned(),
        tables: vec![SourceTable {
            table_id: 100,
            table_name: "t".to_owned(),
            db_name: "test".to_owned(),
            physical_table_id: 100,
            columns: vec![
                column(0, "a", true),
                column(1, "b", false),
                column(2, "c", false),
            ],
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

fn build(sql: &str) -> LogicalPlan {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select(sql);
    builder
        .build_select(&select)
        .unwrap_or_else(|error| panic!("{sql} should build: {}", error.message()))
        .0
}

fn build_err(sql: &str) -> String {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select(sql);
    match builder.build_select(&select) {
        Ok(_) => panic!("{sql} should have been refused"),
        Err(error) => error.message().to_owned(),
    }
}

/// The first operator of the given kind on the way down from the root.
fn find<'a>(plan: &'a LogicalPlan, tp: &str) -> Option<&'a LogicalPlan> {
    if plan.tp() == tp {
        return Some(plan);
    }
    plan.children().iter().find_map(|child| find(child, tp))
}

// ***** GROUP BY + aggregate builds a LogicalAggregation *****

#[test]
fn test_group_by_builds_an_aggregation_with_gos_schema() {
    // Go's `buildAggregation` schema is: one column per DISTINCT aggregate,
    // then one `firstrow()` per CHILD column. The projection above it reports
    // only the select list.
    let plan = build("SELECT b, count(a) FROM t GROUP BY b");
    let aggregation = find(&plan, "Aggregation").expect("an Aggregation is built");
    let LogicalPlan::Aggregation(agg) = aggregation else {
        unreachable!()
    };
    assert_eq!(agg.group_by_items.len(), 1);
    assert!(matches!(agg.group_by_items[0], Expression::Column(_)));

    // `count(a)` plus one `firstrow` per data-source column (a, b, c and the
    // extra commit-ts column `build_data_source` appends).
    assert_eq!(agg.agg_funcs[0].name(), "count");
    assert!(agg.agg_funcs[1..]
        .iter()
        .all(|func| func.name() == "firstrow"));
    let schema = agg
        .base
        .base
        .schema()
        .expect("the aggregation has a schema");
    assert_eq!(schema.columns.len(), agg.agg_funcs.len());

    // The projection above reports exactly the two select fields.
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected a Projection at the root, got {}", plan.tp());
    };
    assert_eq!(projection.exprs.len(), 2);
}

#[test]
fn test_identical_aggregates_are_combined_onto_one_column() {
    // Go `:322` "combine identical aggregate functions". `aggIndexMap` then
    // points both markers at the same output column, which is why
    // `agg_marker_columns` exists.
    let plan = build("SELECT count(a), count(a)+1 FROM t GROUP BY b");
    let LogicalPlan::Aggregation(agg) = find(&plan, "Aggregation").expect("an Aggregation") else {
        unreachable!()
    };
    assert_eq!(
        agg.agg_funcs
            .iter()
            .filter(|func| func.name() == "count")
            .count(),
        1
    );
}

#[test]
fn test_an_aggregate_without_group_by_still_builds_an_aggregation() {
    // `select count(*) from t` has no GROUP BY at all, and Go still builds the
    // aggregation — `detectSelectAgg` is what decides, not the GROUP BY.
    let plan = build("SELECT count(*) FROM t");
    let LogicalPlan::Aggregation(agg) = find(&plan, "Aggregation").expect("an Aggregation") else {
        unreachable!()
    };
    assert!(agg.group_by_items.is_empty());
    assert_eq!(agg.agg_funcs[0].name(), "count");
}

#[test]
fn test_group_by_a_select_list_alias_resolves_to_the_fields_expression() {
    // `gbyResolver.Leave`: `group by x` over `select b+1 as x` groups by
    // `b+1`, NOT by an output column. So the group-by item is a scalar
    // function over the source, and the aggregation sits below the projection.
    let plan = build("SELECT b+1 AS x, count(a) FROM t GROUP BY x");
    let LogicalPlan::Aggregation(agg) = find(&plan, "Aggregation").expect("an Aggregation") else {
        unreachable!()
    };
    assert!(matches!(
        agg.group_by_items[0],
        Expression::ScalarFunction(_)
    ));
}

#[test]
fn test_group_by_position_names_a_select_field() {
    let plan = build("SELECT b, count(a) FROM t GROUP BY 1");
    let LogicalPlan::Aggregation(agg) = find(&plan, "Aggregation").expect("an Aggregation") else {
        unreachable!()
    };
    assert!(matches!(agg.group_by_items[0], Expression::Column(_)));

    // A position naming an aggregate field is Go's `ErrWrongGroupField`.
    let message = build_err("SELECT count(a) FROM t GROUP BY 1");
    assert!(message.contains("Can't group on"), "{message}");
    let message = build_err("SELECT b FROM t GROUP BY 9");
    assert!(message.contains("in 'group statement'"), "{message}");
}

// ***** HAVING becomes a Selection ABOVE the Projection *****

#[test]
fn test_having_is_a_selection_above_the_projection() {
    // Go `:4533`. The shape, top down, is
    //   Selection -> Projection -> Aggregation -> DataSource
    // and the Selection's condition reads the PROJECTION's column, which is
    // what makes an alias usable in HAVING at all. There is no trailing trim
    // here because `n` is a WRITTEN select field, so nothing auxiliary was
    // appended and `oldLen` is already the projection's width.
    let plan = build("SELECT b, count(a) AS n FROM t GROUP BY b HAVING n > 1");
    let LogicalPlan::Selection(selection) = &plan else {
        panic!("expected a Selection at the root, got {}", plan.tp());
    };
    assert_eq!(selection.conditions.len(), 1);
    let projection = &selection.base.children()[0];
    assert_eq!(projection.tp(), "Projection");
    assert_eq!(
        selection.base.children()[0].children()[0].tp(),
        "Aggregation"
    );
}

#[test]
fn test_having_over_an_aggregate_not_in_the_select_list() {
    // `having sum(b) > 0` must build `sum(b)` even though nothing projects it:
    // the resolver lifts it into an auxiliary select field, so the aggregation
    // computes it and the trailing projection trims it back off.
    let plan = build("SELECT b FROM t GROUP BY b HAVING sum(a) > 0");
    let LogicalPlan::Aggregation(agg) = find(&plan, "Aggregation").expect("an Aggregation") else {
        unreachable!()
    };
    assert!(agg.agg_funcs.iter().any(|func| func.name() == "sum"));

    // The root trims back to the one written select field.
    let LogicalPlan::Projection(trim) = &plan else {
        panic!("expected the trailing trim Projection, got {}", plan.tp());
    };
    assert_eq!(trim.exprs.len(), 1);
    assert!(find(&plan, "Selection").is_some());
}

#[test]
fn test_having_resolves_an_alias_to_the_fields_expression_inside_an_aggregate() {
    // Go's `:2896` arm: `having sum(x) < 0` over `select a+1 as x` builds
    // `sum(a+1)`, which is only possible before the projection exists.
    let plan = build("SELECT a+1 AS x FROM t HAVING sum(x) < 100");
    let LogicalPlan::Aggregation(agg) = find(&plan, "Aggregation").expect("an Aggregation") else {
        unreachable!()
    };
    let sum = agg
        .agg_funcs
        .iter()
        .find(|func| func.name() == "sum")
        .expect("sum is built");
    // Its argument is `a+1`, a scalar function — not the projection's output.
    assert!(matches!(sum.args()[0], Expression::ScalarFunction(_)));
}

// ***** the marker scheme round-trips through the agg / having maps *****

#[test]
fn test_the_agg_marker_round_trips_through_build_aggregation() {
    // The producing half: `extract_agg_funcs_in_select_fields` substitutes
    // `#agg#i`. The reading half: `agg_marker_columns` binds `i` through
    // `aggIndexMap` to the aggregation's schema, and `build_projection`
    // resolves the marker to that column.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT count(a), count(a) FROM t GROUP BY b");
    let plan = builder
        .build_table_refs(select.from.as_ref())
        .expect("FROM");
    let (schema, names) = super::snapshot_schema_and_names(&plan);

    let mut fields =
        PlanBuilder::<TestCatalog, ZonedNoColumns>::expand_fields(&select.fields, &schema, &names);
    let aggs = builder.extract_agg_funcs_in_select_fields(&mut fields);
    assert_eq!(aggs.len(), 2, "both calls are extracted separately");
    // Rule 2: the marker occupies the WHOLE field expression.
    assert_eq!(
        PlanMarker::from_expr(&fields[0].expr),
        Some(PlanMarker::new(MarkerKind::Agg, 0))
    );
    assert_eq!(
        PlanMarker::from_expr(&fields[1].expr),
        Some(PlanMarker::new(MarkerKind::Agg, 1))
    );

    let markers = BTreeMap::new();
    let (aggregated, agg_index_map) = builder
        .build_aggregation(plan, &aggs, Vec::new(), &markers)
        .expect("the aggregation builds");
    // Both extracted aggregates land on ONE output column, which is Go's
    // combine step; the map is what recovers that.
    assert_eq!(agg_index_map, vec![0, 0]);
    let agg_schema = aggregated.schema().cloned().unwrap_or_default();
    let columns = super::aggregation::agg_marker_columns(&agg_index_map, &agg_schema);
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].unique_id, columns[1].unique_id);
}

#[test]
fn test_the_having_marker_is_never_read_as_the_agg_marker() {
    // Spec rule 3, at this batch's own boundary: HAVING's aggregates and the
    // select list's occupy disjoint kinds, so a HAVING marker cannot bind to
    // an `Agg` column and vice versa.
    let having = PlanMarker::new(MarkerKind::Having, 0).as_expr();
    assert_eq!(
        PlanMarker::index_of_kind(&having, MarkerKind::Agg),
        None,
        "a Having marker must not read as an Agg marker"
    );
    assert_eq!(
        PlanMarker::index_of_kind(&having, MarkerKind::Having),
        Some(0)
    );
}

#[test]
fn test_a_having_column_reference_becomes_a_column_marker() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT b AS x FROM t GROUP BY b HAVING x > 1");
    let plan = builder
        .build_table_refs(select.from.as_ref())
        .expect("FROM");
    let (schema, names) = super::snapshot_schema_and_names(&plan);
    let mut fields =
        PlanBuilder::<TestCatalog, ZonedNoColumns>::expand_fields(&select.fields, &schema, &names);

    let mut having = select.having.clone().expect("a HAVING clause");
    let aggregates = builder
        .resolve_having_and_order_by(&mut having, &mut fields, &names)
        .expect("HAVING resolves");
    assert!(aggregates.is_empty());
    // `x` is the select list's field 0, so the marker is `#col#0`.
    let Expr::Binary(_, left, _) = &having else {
        panic!("expected a comparison, got {having:?}");
    };
    assert_eq!(
        PlanMarker::from_expr(left),
        Some(PlanMarker::new(MarkerKind::Column, 0))
    );
}

// ***** a correlated aggregate resolves to the right scope *****

#[test]
fn test_a_correlated_aggregate_is_lifted_into_the_outer_select_list() {
    // `select (select count(a)) from t`: the inner `count(a)` reads only the
    // OUTER block's `a`, so Go evaluates it in the OUTER block and records it
    // in `correlatedAggMapper`. Here it becomes an auxiliary outer field and a
    // `#corragg#0` marker inside the subquery.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT (SELECT count(a)) FROM t");
    let plan = builder
        .build_table_refs(select.from.as_ref())
        .expect("FROM");
    let (schema, names) = super::snapshot_schema_and_names(&plan);
    let mut fields =
        PlanBuilder::<TestCatalog, ZonedNoColumns>::expand_fields(&select.fields, &schema, &names);
    let before = fields.len();

    let lifted = builder
        .resolve_correlated_aggregates(&mut fields, None, &mut Vec::new(), &names)
        .expect("correlated aggregates resolve");
    assert_eq!(lifted.len(), 1, "count(a) belongs to the outer block");
    assert_eq!(fields.len(), before + 1);
    let appended = fields.last().expect("the auxiliary field");
    assert!(appended.hidden);
    assert_eq!(appended.alias.as_deref(), Some("sel_subq_agg_1"));
    assert_eq!(builder.correlated_agg_columns.len(), 1);

    // Inside the subquery, the aggregate is now the marker.
    let Expr::Subquery(subquery) = &fields[0].expr else {
        panic!("expected a subquery field");
    };
    let tidb_ast::QueryStmt::Select(inner) = &**subquery else {
        panic!("expected a SELECT subquery");
    };
    let tidb_ast::SelectField::Expr { expr, .. } = &inner.fields.fields()[0] else {
        panic!("expected an expression field");
    };
    assert_eq!(
        PlanMarker::from_expr(expr),
        Some(PlanMarker::new(MarkerKind::CorrelatedAgg, 0))
    );
}

#[test]
fn test_an_aggregate_over_the_subquerys_own_columns_is_not_lifted() {
    // The other side of the same rule: `count(x.a)` over the subquery's own
    // FROM stays where it is written.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT (SELECT count(x.a) FROM t x) FROM t");
    let plan = builder
        .build_table_refs(select.from.as_ref())
        .expect("FROM");
    let (schema, names) = super::snapshot_schema_and_names(&plan);
    let mut fields =
        PlanBuilder::<TestCatalog, ZonedNoColumns>::expand_fields(&select.fields, &schema, &names);
    let lifted = builder
        .resolve_correlated_aggregates(&mut fields, None, &mut Vec::new(), &names)
        .expect("correlated aggregates resolve");
    assert!(
        lifted.is_empty(),
        "count(x.a) names the subquery's own table"
    );
}

// ***** WITH ROLLUP builds a LogicalExpand *****

#[test]
fn test_with_rollup_builds_an_expand_over_a_projection() {
    // Go `buildExpand`: Aggregation -> Expand -> Projection(proj4Expand) ->
    // DataSource, and the aggregation additionally groups by the Expand's
    // `gid`.
    let plan = build("SELECT b, c, count(a) FROM t GROUP BY b, c WITH ROLLUP");
    let LogicalPlan::Aggregation(agg) = find(&plan, "Aggregation").expect("an Aggregation") else {
        unreachable!()
    };
    // Two written group-by items plus the generated `gid`.
    assert_eq!(agg.group_by_items.len(), 3);

    let expand = find(&plan, "Expand").expect("an Expand is built");
    let LogicalPlan::Expand(expand) = expand else {
        unreachable!()
    };
    // `<b, c>` rolls up to `{}`, `{b}`, `{b,c}`.
    assert_eq!(expand.rollup_grouping_sets.len(), 3);
    assert_eq!(expand.distinct_size, 3);
    assert_eq!(expand.distinct_group_by_col.len(), 2);
    assert!(expand.gid.is_some());
    assert!(
        expand.gpos.is_none(),
        "rollup prefixes are always distinct, so no gpos is needed"
    );
    // The grouping ids are the bitmasks of the prefixes: {}, {b}, {b,c}.
    assert_eq!(expand.rollup_grouping_ids, vec![0, 1, 3]);

    // The projection below materialises the group-by expressions.
    let projection = &expand.base.children()[0];
    let LogicalPlan::Projection(projection) = projection else {
        panic!(
            "expected a Projection under the Expand, got {}",
            projection.tp()
        );
    };
    assert!(projection.proj4_expand);
}

#[test]
fn test_rollup_makes_the_grouping_columns_nullable() {
    // `AdjustNullabilityFromGroupingSets`: a set that does not group by a
    // column projects NULL there, so NOT NULL must come off across the Expand.
    let plan = build("SELECT a, count(b) FROM t GROUP BY a WITH ROLLUP");
    let LogicalPlan::Expand(expand) = find(&plan, "Expand").expect("an Expand") else {
        unreachable!()
    };
    let schema = expand.base.base.schema().expect("the Expand has a schema");
    let grouping_id = expand.distinct_group_by_col[0].unique_id;
    let grouping_column = schema
        .columns
        .iter()
        .find(|column| column.unique_id == grouping_id)
        .expect("the grouping column is in the Expand's schema");
    let flags = grouping_column
        .ret_type
        .as_ref()
        .expect("the column is typed")
        .flags();
    assert_eq!(
        flags & FieldTypeFlags::NOT_NULL,
        0,
        "a rolled-up grouping column must be nullable"
    );
}

#[test]
fn test_grouping_marks_resolve_against_the_expands_columns() {
    // `GROUPING(b)` is answered from the grouping id: in ModeBitAnd its mark
    // is the single bit of that column. This is the resolution half of
    // `driver/grouping.rs:65-164`, over the operator this batch builds.
    let plan = build("SELECT b, c, count(a) FROM t GROUP BY b, c WITH ROLLUP");
    let LogicalPlan::Expand(expand) = find(&plan, "Expand").expect("an Expand") else {
        unreachable!()
    };
    let marks = expand.generate_grouping_marks(&expand.distinct_group_by_col);
    assert_eq!(marks.len(), 2);
    assert!(marks[0].contains(&1), "the first grouping column is bit 0");
    assert!(marks[1].contains(&2), "the second grouping column is bit 1");

    // And a `GROUPING()` argument that IS a group-by column resolves.
    let resolved = expand
        .resolve_grouping_func_args_in_group_by(&[Expression::Column(
            expand.distinct_group_by_col[0].clone(),
        )])
        .expect("a group-by column resolves");
    assert_eq!(resolved.len(), 1);
    // While one that is not is Go's `ErrFieldInGroupingNotGroupBy`.
    let mut stranger = expand.distinct_group_by_col[0].clone();
    stranger.unique_id = i64::MAX;
    let error = expand
        .resolve_grouping_func_args_in_group_by(&[Expression::Column(stranger)])
        .expect_err("a non-group-by argument is refused");
    assert!(error.message().contains("ErrFieldInGroupingNotGroupBy"));
}

// ***** DISTINCT *****

#[test]
fn test_distinct_builds_an_aggregation_grouping_by_the_select_list() {
    let plan = build("SELECT DISTINCT b FROM t");
    let LogicalPlan::Aggregation(agg) = &plan else {
        panic!("expected an Aggregation at the root, got {}", plan.tp());
    };
    assert_eq!(agg.group_by_items.len(), 1);
    assert!(agg.agg_funcs.iter().all(|func| func.name() == "firstrow"));
    // The schema is the child's, which is what makes DISTINCT transparent.
    let schema = agg.base.base.schema().expect("a schema");
    assert_eq!(schema.columns.len(), agg.agg_funcs.len());
}

// ***** ONLY_FULL_GROUP_BY refusals match Go's error codes *****

#[test]
fn test_only_full_group_by_refuses_an_ungrouped_column() {
    // Go 1055 `ErrFieldNotInGroupBy`.
    let message = build_err("SELECT b, c FROM t GROUP BY b");
    assert!(
        message.contains("Expression #2 of SELECT list is not in GROUP BY clause"),
        "{message}"
    );
    assert!(
        message.contains("only_full_group_by") && message.contains("test.t.c"),
        "{message}"
    );
}

#[test]
fn test_only_full_group_by_refuses_a_bare_column_in_an_aggregated_query() {
    // Go 8123 `ErrMixOfGroupFuncAndFields`.
    let message = build_err("SELECT b, count(a) FROM t");
    assert!(
        message.contains("In aggregated query without GROUP BY, expression #1"),
        "{message}"
    );
}

#[test]
fn test_only_full_group_by_refuses_an_order_by_aggregate_over_a_non_aggregated_query() {
    // Go 3029 `ErrAggregateOrderNonAggQuery`, which is reported BEFORE 8123.
    let message = build_err("SELECT b FROM t ORDER BY count(a)");
    assert!(
        message.contains("Expression #1 of ORDER BY contains aggregate function"),
        "{message}"
    );
}

#[test]
fn test_a_primary_key_in_the_group_by_justifies_every_other_column() {
    // `checkColFuncDepend`: grouping by the PRIMARY KEY determines the whole
    // row, so `b` and `c` are legal without being named.
    build("SELECT a, b, c FROM t GROUP BY a");
}

#[test]
fn test_a_where_equality_pins_a_column_for_the_whole_query() {
    // MySQL's documented single-value relaxation, Go's
    // `extractSingeValueColNamesFromWhere`.
    build("SELECT b, c FROM t WHERE c = 3 GROUP BY b");
}

#[test]
fn test_an_aggregate_and_a_grouped_column_are_both_justified() {
    build("SELECT b, count(a), sum(c) FROM t GROUP BY b");
}

#[test]
fn test_clearing_the_sql_mode_restores_the_permissive_behaviour() {
    // The whole rule is gated on `SQLMode.HasOnlyFullGroupBy()`, which is what
    // clearing the mode turns off.
    let harness = Harness::new();
    let mut builder = harness.builder();
    builder.only_full_group_by = false;
    let select = parse_select("SELECT b, c FROM t GROUP BY b");
    builder
        .build_select(&select)
        .expect("without the mode, the query is accepted");
}

#[test]
fn test_distinct_refuses_an_order_by_the_select_list_does_not_report() {
    // Go 3065 `ErrFieldInOrderNotSelect`, MySQL #12442.
    let message = build_err("SELECT DISTINCT b FROM t ORDER BY c");
    assert!(
        message.contains("Expression #1 of ORDER BY clause is not in SELECT list"),
        "{message}"
    );
    // Ordering by a reported field is fine.
    build("SELECT DISTINCT b FROM t ORDER BY b");
}

// ***** the pieces, in isolation *****

#[test]
fn test_resolve_from_select_fields_precedence() {
    let fields = vec![
        ProjectionField {
            expr: Expr::Column(vec!["b".to_owned()]),
            alias: Some("x".to_owned()),
            text: None,
            hidden: false,
        },
        ProjectionField {
            expr: Expr::Column(vec!["c".to_owned()]),
            alias: None,
            text: None,
            hidden: false,
        },
        ProjectionField {
            expr: Expr::Column(vec!["hidden".to_owned()]),
            alias: None,
            text: None,
            hidden: true,
        },
    ];
    // 1. an alias wins.
    assert_eq!(
        super::aggregation::resolve_from_select_fields(&["x".to_owned()], &fields, false),
        Some(0)
    );
    // 2. a field that IS that column.
    assert_eq!(
        super::aggregation::resolve_from_select_fields(&["c".to_owned()], &fields, false),
        Some(1)
    );
    // 3. an AUXILIARY field is never matched (Go's `field.Auxiliary` skip).
    assert_eq!(
        super::aggregation::resolve_from_select_fields(&["hidden".to_owned()], &fields, false),
        None
    );
    // `ignoreAsName` looks past the alias at the underlying column.
    assert_eq!(
        super::aggregation::resolve_from_select_fields(&["b".to_owned()], &fields, true),
        Some(0)
    );
}

#[test]
fn test_rollup_grouping_sets_are_the_prefixes() {
    let columns: Vec<Expression> = (0..3)
        .map(|id| {
            let mut column = tidb_expr::column::Column::default();
            column.unique_id = id;
            Expression::Column(column)
        })
        .collect();
    let sets = super::expand::rollup_grouping_sets(&columns);
    assert_eq!(sets.len(), 4);
    assert!(sets[0].col_ids.is_empty());
    assert_eq!(sets[3].col_ids.len(), 3);
}

#[test]
fn test_deduplicate_and_restore_gby_expressions_round_trip() {
    // `group by a, b, a` has two distinct expressions and puts the rebuilt
    // column back in all three written positions.
    let make = |id: i64| {
        let mut column = tidb_expr::column::Column::default();
        column.unique_id = id;
        column
    };
    let items = vec![
        Expression::Column(make(1)),
        Expression::Column(make(2)),
        Expression::Column(make(1)),
    ];
    let (distinct, positions) = super::expand::deduplicate_gby_expression(&items);
    assert_eq!(distinct.len(), 2);
    assert_eq!(positions, vec![0, 1, 0]);

    let projected = vec![make(10), make(20)];
    let restored = super::expand::restore_gby_expression(&projected, &positions);
    assert_eq!(restored.len(), 3);
    let ids: Vec<i64> = restored
        .iter()
        .map(|expr| match expr {
            Expression::Column(column) => column.unique_id,
            _ => panic!("expected a column"),
        })
        .collect();
    assert_eq!(ids, vec![10, 20, 10]);
}

#[test]
fn test_add_alias_name_gives_every_field_an_explicit_alias() {
    let mut fields = vec![
        ProjectionField {
            expr: Expr::Column(vec!["t".to_owned(), "b".to_owned()]),
            alias: None,
            text: None,
            hidden: false,
        },
        ProjectionField {
            expr: Expr::Int("1".to_owned()),
            alias: None,
            text: Some("1".to_owned()),
            hidden: false,
        },
    ];
    PlanBuilder::<TestCatalog, ZonedNoColumns>::add_alias_name(&mut fields, &[]);
    assert_eq!(fields[0].alias.as_deref(), Some("b"));
    assert_eq!(fields[1].alias.as_deref(), Some("1"));
}

#[test]
fn test_build_distinct_reports_every_child_column() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT * FROM t");
    let child = builder
        .build_table_refs(select.from.as_ref())
        .expect("FROM");
    let child_width = child
        .schema()
        .map(|schema: &Schema| schema.columns.len())
        .unwrap_or_default();
    let plan = builder.build_distinct(child, 1).expect("DISTINCT builds");
    let LogicalPlan::Aggregation(agg) = &plan else {
        panic!("expected an Aggregation, got {}", plan.tp());
    };
    assert_eq!(agg.group_by_items.len(), 1, "grouped on the first column");
    assert_eq!(agg.agg_funcs.len(), child_width);
}
