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

//! Aggregation seam tests. WRITTEN, not transcreated: Go's builder tests need
//! a live session and a `testkit` cluster.
//!
//! What each group proves is named on the group.

use std::collections::BTreeMap;

use tidb_ast::{Expr, SelectStmt, Stmt};
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone};
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_exprctx::ZonedNoColumns;
use tidb_intset::FastIntSet;

use tidb_infoschema::table_source::SourceTable;
use tidb_infoschema::table_source::TableSource;
use tidb_infoschema::table_source::SourceColumn;
use super::marker::{MarkerKind, PlanMarker};
use super::{PlanBuilder, ProjectionField};
use tidb_planner_core_base::ColumnIdAllocator;
use tidb_planner_core_operator_logicalop::{prepare_possible_properties, LogicalPlan};
use tidb_planner_core_base::PlanIdAllocator;

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
    tidb_table::column::Column {
        info: tidb_model::GoShared::new({
            let mut info = tidb_model::ColumnInfo {
                id: offset as i64 + 1,
                name: tidb_ast::CiString::new(name.to_owned()),
                offset: (offset) as i64,
                field_type: ret_type,
                state: tidb_model::SchemaState::PUBLIC,
                ..tidb_model::ColumnInfo::default()
            };
            if primary {
                info.add_flag(u64::from(tidb_datatype::FieldTypeFlags::PRI_KEY));
            }
            info
        }),
        generated_expr: None,
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
            indexes: vec![tidb_model::GoShared::new(tidb_model::IndexInfo {
                id: 1,
                name: tidb_ast::CiString::new("idx_b"),
                columns: vec![tidb_model::IndexColumn {
                    name: tidb_ast::CiString::new("b"),
                    offset: 1,
                    length: -1,
                    ..tidb_model::IndexColumn::default()
                }].into(),
                state: tidb_model::SchemaState::PUBLIC,
                ..tidb_model::IndexInfo::default()
            })],
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

#[test]
fn extract_fd_source_projection_and_aggregation_cases() {
    let build_with_fd_expression_ids = |sql: &str| {
        let harness = Harness::new();
        let mut builder = harness.builder();
        builder.new_only_full_group_by_check = true;
        builder
            .build_select(&parse_select(sql))
            .unwrap_or_else(|error| panic!("{sql} should build: {}", error.message()))
            .0
    };

    let projection = build_with_fd_expression_ids("SELECT a, c, b + 1 FROM t");
    let projection_schema = projection.schema().expect("projection schema");
    let projection_fd = projection.extract_fd();
    assert!(projection_fd.in_closure(
        &FastIntSet::new([1]),
        &FastIntSet::new(
            projection_schema
                .columns
                .iter()
                .map(|column| column.unique_id),
        ),
    ));
    assert!(projection_fd.in_closure(
        &FastIntSet::new([2]),
        &FastIntSet::new([projection_schema.columns[2].unique_id]),
    ));

    let grouped = build_with_fd_expression_ids("SELECT b + 1, SUM(a) FROM t GROUP BY b");
    let grouped_schema = grouped.schema().expect("grouped projection schema");
    let grouped_fd = grouped.extract_fd();
    assert!(grouped_fd.in_closure(
        &FastIntSet::new([2]),
        &FastIntSet::new(grouped_schema.columns.iter().map(|column| column.unique_id),),
    ));
    assert!(grouped_fd.group_by_cols().has(2));

    let harness = Harness::new();
    harness.catalog.tables[0].indexes[0].write().unique = true;
    let mut builder = harness.builder();
    let (filtered, _) = builder
        .build_select(&parse_select("SELECT a, c FROM t WHERE b = 1"))
        .expect("nullable unique key query builds");
    let filtered_fd = filtered.extract_fd();
    assert!(filtered_fd.in_closure(&FastIntSet::new([2]), &FastIntSet::new([1, 3])));
}

/// The first operator of the given kind on the way down from the root.
fn find<'a>(plan: &'a LogicalPlan, tp: &str) -> Option<&'a LogicalPlan> {
    if plan.tp() == tp {
        return Some(plan);
    }
    plan.children().iter().find_map(|child| find(child, tp))
}

#[test]
fn possible_properties_use_equalities_above_the_data_source() {
    let mut harness = Harness::new();
    harness.catalog.tables[0].indexes.push(tidb_model::GoShared::new(tidb_model::IndexInfo {
        id: 2,
        name: tidb_ast::CiString::new("idx_bc"),
        columns: vec![
            tidb_model::IndexColumn {
                name: tidb_ast::CiString::new("b"),
                offset: 1,
                length: -1,
                ..tidb_model::IndexColumn::default()
            },
            tidb_model::IndexColumn {
                name: tidb_ast::CiString::new("c"),
                offset: 2,
                length: -1,
                ..tidb_model::IndexColumn::default()
            },
        ].into(),
        state: tidb_model::SchemaState::PUBLIC,
        ..tidb_model::IndexInfo::default()
    }));
    let mut builder = harness.builder();
    let (plan, _) = builder
        .build_select(&parse_select(
            "SELECT c, COUNT(*) FROM t WHERE b = 1 GROUP BY c",
        ))
        .expect("the grouped select builds");
    let (plan, _) = prepare_possible_properties(plan);
    let LogicalPlan::Aggregation(aggregation) =
        find(&plan, "Aggregation").expect("the plan contains an aggregation")
    else {
        panic!("the located operator is not an aggregation");
    };
    assert!(aggregation
        .possible_properties
        .orders
        .iter()
        .any(|order| { order.len() == 1 && order[0].id == 3 }));
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
    let Expression::Column(group_expr) = &projection.exprs[0] else {
        panic!("the grouped select field resolves to an aggregation column");
    };
    let group_output = projection
        .base
        .base
        .schema()
        .and_then(|schema| schema.columns.first())
        .expect("the projection output column");
    assert_eq!(
        group_output.unique_id, group_expr.unique_id,
        "Go buildProjectionField preserves an already-rewritten Column identity"
    );
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
    // ONLY_FULL_GROUP_BY refuses the nonaggregated `a`, so it is off here.
    let harness = Harness::new();
    let mut builder = harness.builder();
    builder.only_full_group_by = false;
    let plan = builder
        .build_select(&parse_select("SELECT a+1 AS x FROM t HAVING sum(x) < 100"))
        .unwrap_or_else(|error| panic!("should build: {}", error.message()))
        .0;
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
    let gby_exprs = builder
        .resolve_gby_exprs(&select.group_by, &fields, &names)
        .expect("GROUP BY resolves");
    let aggregates = builder
        .resolve_having_and_order_by(&mut having, &mut fields, &names, &gby_exprs)
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
fn test_a_having_group_by_column_resolves_through_the_source_plan() {
    // Go `havingWindowAndOrderbyExprResolver.Leave`: a HAVING name matching a
    // GROUP BY item sets `resolveFieldsFirst = false`, so it resolves through
    // the SOURCE plan and `resolveFromPlan` appends an auxiliary select field.
    // Without that, `select count(*) from t group by a having a > 1` was 1054
    // even though `a` is grouped, because `a` is not in the select list.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT COUNT(*) FROM t GROUP BY a HAVING a > 1");
    let plan = builder
        .build_table_refs(select.from.as_ref())
        .expect("FROM");
    let (schema, names) = super::snapshot_schema_and_names(&plan);
    let mut fields =
        PlanBuilder::<TestCatalog, ZonedNoColumns>::expand_fields(&select.fields, &schema, &names);
    let old_len = fields.len();

    let mut having = select.having.clone().expect("a HAVING clause");
    let gby_exprs = builder
        .resolve_gby_exprs(&select.group_by, &fields, &names)
        .expect("GROUP BY resolves");
    let aggregates = builder
        .resolve_having_and_order_by(&mut having, &mut fields, &names, &gby_exprs)
        .expect("HAVING resolves a grouped column");
    assert!(aggregates.is_empty(), "HAVING has no aggregate call");
    assert_eq!(
        fields.len(),
        old_len + 1,
        "the grouped column is appended as an auxiliary field"
    );
    let appended = &fields[old_len];
    assert!(appended.hidden, "the auxiliary field is hidden");
    assert!(
        matches!(&appended.expr, Expr::Column(path) if path.last().is_some_and(|c| c.as_str() == "a"))
    );
    let Expr::Binary(_, left, _) = &having else {
        panic!("expected a comparison, got {having:?}");
    };
    assert_eq!(
        PlanMarker::from_expr(left),
        Some(PlanMarker::new(MarkerKind::Column, old_len))
    );
}

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
        .resolve_correlated_aggregates(&mut fields, None, &mut Vec::new(), &names, &schema)
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
        .resolve_correlated_aggregates(&mut fields, None, &mut Vec::new(), &names, &schema)
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

fn find_grouping(plan: &LogicalPlan) -> Option<&tidb_expr::scalar_function::ScalarFunction> {
    if let LogicalPlan::Projection(projection) = plan {
        for expr in &projection.exprs {
            if let Expression::ScalarFunction(function) = expr {
                if function.func_name.lowercase() == "grouping" {
                    return Some(function);
                }
            }
        }
    }
    plan.base().children().iter().find_map(find_grouping)
}

#[test]
fn grouping_sql_installs_metadata_and_uses_the_expand_gid() {
    let plan = build("SELECT GROUPING(b) FROM t GROUP BY b, c WITH ROLLUP");
    let LogicalPlan::Expand(expand) = find(&plan, "Expand").unwrap() else {
        unreachable!()
    };
    let function = find_grouping(&plan).expect("GROUPING must retain its planner metadata");
    assert_eq!(function.func_name.lowercase(), "grouping");
    assert_eq!(function.args.len(), 1);
    let Expression::Column(argument) = &function.args[0] else {
        panic!("GROUPING evaluates the generated gid")
    };
    assert_eq!(argument.unique_id, expand.gid.as_ref().unwrap().unique_id);
    let metadata = function.grouping_metadata().unwrap();
    assert_eq!(metadata.mode(), tidb_expr::GroupingMode::BitAnd);
    assert_eq!(
        metadata.grouping_marks()[0],
        std::collections::BTreeSet::from([1])
    );
}

#[test]
fn grouping_sql_preserves_argument_order_and_wide_rollup_mode() {
    let plan = build("SELECT GROUPING(c,b) FROM t GROUP BY b,c WITH ROLLUP");
    let metadata = find_grouping(&plan).unwrap().grouping_metadata().unwrap();
    assert_eq!(
        metadata.grouping_marks(),
        &[
            std::collections::BTreeSet::from([2]),
            std::collections::BTreeSet::from([1]),
        ]
    );
    assert_eq!(metadata.eval(1), 2);
    assert_eq!(metadata.eval(0), 3);

    let group_items = vec!["b"; 65].join(",");
    let plan = build(&format!(
        "SELECT GROUPING(b) FROM t GROUP BY {group_items} WITH ROLLUP"
    ));
    let LogicalPlan::Expand(expand) = find(&plan, "Expand").unwrap() else {
        unreachable!()
    };
    let metadata = find_grouping(&plan).unwrap().grouping_metadata().unwrap();
    assert_eq!(metadata.mode(), tidb_expr::GroupingMode::NumericSet);
    assert_eq!(
        metadata.grouping_marks(),
        expand.generate_grouping_marks(&expand.distinct_group_by_col)
    );
}

#[test]
fn grouping_sql_retains_source_validation_errors() {
    let harness = Harness::new();
    let args = vec!["b"; 65].join(",");
    for (sql, expected) in [
        (
            "SELECT GROUPING(b) FROM t".to_owned(),
            tidb_exprctx::EvalError::InvalidGroupFuncUse,
        ),
        (
            "SELECT GROUPING(c) FROM t GROUP BY b WITH ROLLUP".to_owned(),
            tidb_exprctx::EvalError::FieldInGroupingNotGroupBy(0),
        ),
        (
            format!("SELECT GROUPING({args}) FROM t GROUP BY b WITH ROLLUP"),
            tidb_exprctx::EvalError::TooManyGroupingArguments,
        ),
    ] {
        let error = harness
            .builder()
            .build_select(&parse_select(&sql))
            .unwrap_err();
        assert!(
            matches!(error.kind(), tidb_planner_core_base::PlanErrorKind::Eval(actual) if *actual == expected),
            "{sql}: {error:?}"
        );
    }
}

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
fn test_a_group_by_expression_justifies_the_same_expression_written_again() {
    // Go `ExpressionDeepEqual` clears every node's source position and
    // compares function names by their lower-case spelling, so the select
    // item matches the GROUP BY item although it sits elsewhere in the text.
    build("SELECT DATE_FORMAT(c, '%Y'), count(a) FROM t GROUP BY date_format(c, '%Y')");
    build("SELECT DISTINCT upper(b), lower(b) FROM t ORDER BY upper(b)");
}

#[test]
fn test_a_cte_column_is_not_reported() {
    // Go `tblInfoFromCol` resolves no table for a CTE reference, so its
    // column owes the grouping nothing.
    build("WITH qn AS (SELECT a, b FROM t) SELECT b FROM qn GROUP BY a");
}

#[test]
fn test_a_wildcard_is_checked_column_by_column_without_the_extra_handle() {
    // Go checks the select list after `unfoldWildStar`, which expands to the
    // visible columns only (no `_tidb_rowid`), each owing its own
    // justification.
    let message = build_err("SELECT * FROM t GROUP BY b");
    assert!(
        message.contains("of SELECT list is not in GROUP BY clause"),
        "{message}"
    );
    let message = build_err("SELECT *, count(a) FROM t");
    assert!(
        message.contains("In aggregated query without GROUP BY, expression #1"),
        "{message}"
    );
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
            default_name: None,
            window_spec_column: false,
            expr: Expr::Column(vec!["b".to_owned()]),
            column_reference: true,
            alias: Some("x".to_owned()),
            text: None,
            hidden: false,
        },
        ProjectionField {
            default_name: None,
            window_spec_column: false,
            expr: Expr::Column(vec!["c".to_owned()]),
            column_reference: true,
            alias: None,
            text: None,
            hidden: false,
        },
        ProjectionField {
            default_name: None,
            window_spec_column: false,
            expr: Expr::Column(vec!["hidden".to_owned()]),
            column_reference: true,
            alias: None,
            text: None,
            hidden: true,
        },
    ];
    // 1. an alias wins.
    assert_eq!(
        super::aggregation::resolve_from_select_fields(&["x".to_owned()], &fields, false).unwrap(),
        Some(0)
    );
    // 2. a field that IS that column.
    assert_eq!(
        super::aggregation::resolve_from_select_fields(&["c".to_owned()], &fields, false).unwrap(),
        Some(1)
    );
    // 3. an AUXILIARY field is never matched (Go's `field.Auxiliary` skip).
    assert_eq!(
        super::aggregation::resolve_from_select_fields(&["hidden".to_owned()], &fields, false)
            .unwrap(),
        None
    );
    // `ignoreAsName` looks past the alias at the underlying column.
    assert_eq!(
        super::aggregation::resolve_from_select_fields(&["b".to_owned()], &fields, true).unwrap(),
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
    // Go `addAliasName`: a column takes its written name, any other field
    // the name `buildProjectionFieldNameFromExpressions` gives its text, and
    // under `AsViewSchema` a repeated anonymous name is renamed.
    let mut select = parse_select("SELECT t.b, 1, 1, (b), b+1 AS x FROM t");
    select.as_view_schema = true;
    let mut fields = PlanBuilder::<TestCatalog, ZonedNoColumns>::expand_fields(
        &select.fields,
        &Schema::new(Vec::new()),
        &[],
    );
    let harness = Harness::new();
    harness.builder().add_alias_name(&select, &mut fields);
    let aliases: Vec<_> = fields.iter().map(|field| field.alias.as_deref()).collect();
    assert_eq!(
        aliases,
        vec![
            Some("b"),
            Some("1"),
            Some("Name_exp_1"),
            Some("(b)"),
            Some("x")
        ]
    );
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

#[test]
fn aggregation_hints_reach_the_logical_aggregation() {
    use tidb_hint::{PREFER_HASH_AGG, PREFER_STREAM_AGG};

    for (hint, expected) in [
        ("HASH_AGG", PREFER_HASH_AGG),
        ("STREAM_AGG", PREFER_STREAM_AGG),
    ] {
        let plan = build(&format!("SELECT /*+ {hint}() */ COUNT(*) FROM t"));
        let mut found = None;
        plan.walk_preorder(&mut |node| {
            if let LogicalPlan::Aggregation(aggregation) = node {
                found = Some(aggregation.prefer_agg_type);
            }
        });
        assert_eq!(found, Some(expected), "{hint} must reach physical search");
    }
}

#[test]
fn test_an_aggregate_order_by_term_is_appended_as_a_hidden_field() {
    // Go `resolveHavingAndOrderBy`'s AggregateFuncExpr case
    // (`logical_plan_builder.go:2789`) appends EVERY aggregate order-by term
    // as a hidden auxiliary select field — even when the same aggregate
    // already appears in the select list (q42's
    // `select ... sum(x) ... order by sum(x) desc`). The widened field list
    // is what later fires the :4620 trim, whose fresh-id schema renders the
    // agg output as `Column#77->Column#81`. Reusing the select field here
    // keeps the schema narrow and the trim never fires.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let select = parse_select("SELECT a, SUM(b) FROM t GROUP BY a ORDER BY SUM(b) DESC");
    let plan = builder
        .build_table_refs(select.from.as_ref())
        .expect("FROM");
    let (schema, names) = super::snapshot_schema_and_names(&plan);
    let mut fields =
        PlanBuilder::<TestCatalog, ZonedNoColumns>::expand_fields(&select.fields, &schema, &names);
    assert_eq!(fields.len(), 2, "two select fields before ORDER BY");
    PlanBuilder::<TestCatalog, ZonedNoColumns>::resolve_order_by(
        &select.order_by,
        &mut fields,
        &names,
        None,
    )
    .expect("ORDER BY resolves");
    assert_eq!(
        fields.len(),
        3,
        "the aggregate order-by term is appended as a hidden field"
    );
    let appended = &fields[2];
    assert!(appended.hidden, "the appended aggregate field is hidden");
    assert!(
        matches!(&appended.expr, Expr::Aggregate { .. }),
        "the appended field holds the aggregate expression, got {:?}",
        appended.expr
    );
}

#[test]
fn aggregation_build_initializes_declared_index_keys_before_statistics() {
    let harness = Harness::new();
    let (plan, _) = harness
        .builder()
        .build_select(&parse_select("SELECT b, COUNT(*) FROM t GROUP BY b"))
        .unwrap();
    let mut seen = false;
    plan.walk_preorder(&mut |node| {
        if let LogicalPlan::DataSource(source) = node {
            seen = true;
            let path = &source.derived_index_paths[&1];
            let declared = path.declared_columns.as_ref().unwrap();
            assert_eq!(declared.len(), 1);
            assert_eq!(declared[0].as_ref().unwrap().0.id, 2);
            assert!(path.row_estimate.is_none());
        }
    });
    assert!(seen);
}

#[test]
fn single_table_delete_prunes_the_read_to_the_handle_and_index_columns() {
    // Go `pruneAndBuildColPositionInfoForDelete`: removing a record needs the
    // handle's and the indexes' columns, so `c` leaves the read of
    // `t(a BIGINT PRIMARY KEY, b BIGINT, c BIGINT, KEY idx_b (b))`.
    let build_delete = |harness: &Harness, sql: &str, has_fk: bool| {
        let mut builder = harness.builder();
        builder.delete_prune_has_fk = Some(has_fk);
        builder.allocate_delete_root = true;
        let select = parse_select(sql);
        let (plan, _) = builder
            .build_dml_source(&select, false)
            .unwrap_or_else(|error| panic!("{sql} should build: {}", error.message()));
        let width = plan.schema().map(|schema| schema.columns.len());
        (
            builder
                .delete_col_pos
                .map(|position| (position.kept, position.extra_handle)),
            width,
        )
    };
    let harness = Harness::new();
    assert_eq!(
        build_delete(&harness, "SELECT * FROM t WHERE b = 1", false),
        (Some((vec![0, 1], false)), Some(2))
    );
    // The alias is the name the kept columns are matched under.
    assert_eq!(
        build_delete(&harness, "SELECT * FROM t AS x WHERE b = 1", false),
        (Some((vec![0, 1], false)), Some(2))
    );
    // A foreign key referencing the table declines the pruning.
    assert_eq!(
        build_delete(&harness, "SELECT * FROM t", true),
        (None, Some(3))
    );
    // A partitioned table and a partial index decline it too.
    let mut partitioned = Harness::new();
    partitioned.catalog.tables[0].is_partitioned = true;
    assert_eq!(
        build_delete(&partitioned, "SELECT * FROM t", false),
        (None, Some(3))
    );
    let partial = Harness::new();
    partial.catalog.tables[0].indexes[0].write().condition_expr_string = "`c` > 0".to_owned();
    assert_eq!(
        build_delete(&partial, "SELECT * FROM t", false),
        (None, Some(3))
    );
    // A heap table (no integer primary key, no common handle) also reads
    // `_tidb_rowid`.
    let mut heap = Harness::new();
    heap.catalog.tables[0].pk_is_handle = false;
    heap.catalog.tables[0].handle_col_offsets = Vec::new();
    heap.catalog.tables[0].columns[0]
        .info
        .write()
        .and_flag(!u64::from(tidb_datatype::FieldTypeFlags::PRI_KEY));
    assert_eq!(
        build_delete(&heap, "SELECT * FROM t", false),
        (Some((vec![1], true)), Some(2))
    );
}

#[test]
fn multi_table_delete_lays_out_each_target_in_the_pruned_read() {
    // Go `pruneAndBuildColPositionInfoForDelete` + `TblColPosInfos`: every
    // target keeps the handle's and the indexes' columns while the other
    // sources are read whole, and each target's `DeleteTablePos` says where
    // its columns and handle sit in a row of the pruned read.
    use super::delete_pruning::{DeleteMultiInput, DeleteTarget};
    use tidb_planner_core_operator_physicalop::DeleteHandle;
    let target = |visible: &str| DeleteTarget {
        database: "test".to_owned(),
        table: "t".to_owned(),
        visible: visible.to_owned(),
    };
    let build = |targets: Vec<DeleteTarget>, has_fk: bool| {
        let harness = Harness::new();
        let mut builder = harness.builder();
        builder.delete_multi = Some(DeleteMultiInput { targets, has_fk });
        builder.allocate_delete_root = true;
        let select = parse_select("SELECT * FROM t AS t1, t AS t2");
        let (plan, _) = builder
            .build_dml_source(&select, false)
            .unwrap_or_else(|error| panic!("should build: {}", error.message()));
        (
            plan.schema().map(|schema| schema.columns.len()),
            builder.delete_tables,
        )
    };
    // One target: t1 loses `c`, t2 is read whole.
    let (width, tables) = build(vec![target("t1")], false);
    assert_eq!(width, Some(5));
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].table, "t");
    assert_eq!(tables[0].stored_width, 3);
    assert_eq!(tables[0].columns, vec![(0, 0), (1, 1)]);
    assert_eq!(tables[0].handle, DeleteHandle::Int(0));
    // Both targets are pruned and the second starts after the first.
    let (width, tables) = build(vec![target("t1"), target("t2")], false);
    assert_eq!(width, Some(4));
    assert_eq!(tables[1].columns, vec![(2, 0), (3, 1)]);
    assert_eq!(tables[1].handle, DeleteHandle::Int(2));
    // A foreign key anywhere declines every pruning.
    let (width, tables) = build(vec![target("t1"), target("t2")], true);
    assert_eq!(width, Some(6));
    assert_eq!(tables[1].columns, vec![(3, 0), (4, 1), (5, 2)]);
    assert_eq!(tables[1].handle, DeleteHandle::Int(3));
}

#[test]
fn update_lowers_every_assignment_over_the_read_not_only_those_with_subqueries() {
    // Go `buildUpdateLists` rewrites each SET expression against the frozen
    // UPDATE input, so `SET b = c + 1` is a planner expression over the read's
    // row as every other SET expression is; an entry the caller leaves out
    // (a generated column's own DEFAULT) has none.
    let harness = Harness::new();
    let value = |sql: &str| {
        let select = parse_select(&format!("SELECT {sql}"));
        match select.fields.iter().next() {
            Some(tidb_ast::SelectField::Expr { expr, .. }) => expr.clone(),
            other => panic!("unexpected field {other:?}"),
        }
    };
    let mut builder = harness.builder();
    let select = parse_select("SELECT * FROM t WHERE a = 1");
    let values = vec![Some(value("c + 1")), None, Some(value("b * 2"))];
    let (plan, expressions, _) = builder
        .build_update_dml_source(&select, &values, false)
        .unwrap_or_else(|error| panic!("should build: {}", error.message()));
    assert_eq!(expressions.len(), 3);
    let plain = expressions[0].as_ref().expect("a plain assignment is lowered");
    let columns = tidb_expr::simple_expr::extract_columns(plain);
    assert_eq!(columns.len(), 1, "`c + 1` reads one column");
    assert_eq!(
        columns[0].unique_id,
        plan.schema().unwrap().columns[2].unique_id,
        "`c + 1` reads the read's `c`"
    );
    assert!(expressions[1].is_none());
    assert!(expressions[2].is_some());
}

#[test]
fn fast_point_update_ordered_list_rewrites_over_the_point_schema() {
    // Go `buildOrderedList`: each SET value is rewritten over the point plan's
    // own schema (the table's columns in order) and cast to its target column;
    // a generated column's own DEFAULT has no expression.
    let harness = Harness::new();
    let select = parse_select("SELECT * FROM t");
    let table_ref = match &select.from.as_ref().expect("a FROM").left {
        tidb_ast::JoinNode::Table(table_ref) => table_ref.clone(),
        other => panic!("unexpected source {other:?}"),
    };
    let mut reader = harness.builder();
    let (plan, _) = reader.build_select(&select).unwrap();
    let schema = plan.schema().unwrap().clone();
    let value = |sql: &str| {
        let select = parse_select(&format!("SELECT {sql}"));
        match select.fields.iter().next() {
            Some(tidb_ast::SelectField::Expr { expr, .. }) => expr.clone(),
            other => panic!("unexpected field {other:?}"),
        }
    };
    let builder = harness.builder();
    let list = builder
        .build_point_ordered_list(
            &table_ref,
            &schema,
            &[
                (vec!["b".to_owned()], Some(value("c + 1"))),
                (vec!["B".to_owned()], None),
            ],
        )
        .unwrap_or_else(|error| panic!("should build: {}", error.message()));
    assert_eq!(list.len(), 2);
    let columns = tidb_expr::simple_expr::extract_columns(list[0].as_ref().unwrap());
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].unique_id, schema.columns[2].unique_id);
    assert!(list[1].is_none());
}

#[test]
fn multi_table_delete_targets_resolve_as_collect_table_name_does() {
    // Go `buildDelete` over `collectTableName`: a target names a source by its
    // alias, or `schema.table` when the source has none; only a base table is
    // updatable.
    let harness = Harness::new();
    let resolve = |sql: &str| {
        let statement = tidb_parser::parse(sql).expect("the DELETE parses");
        let Stmt::Dml(dml) = statement else {
            panic!("not a DML statement");
        };
        let tidb_ast::DmlStmt::Delete(delete) = &*dml else {
            panic!("not a DELETE");
        };
        harness.builder().resolve_delete_targets(delete)
    };
    let names = |targets: Vec<super::delete_pruning::DeleteTarget>| {
        targets
            .into_iter()
            .map(|target| (target.database, target.table, target.visible))
            .collect::<Vec<_>>()
    };
    let t = |visible: &str| ("test".to_owned(), "t".to_owned(), visible.to_owned());
    assert_eq!(
        names(resolve("DELETE a FROM t AS a, t AS b").unwrap()),
        vec![t("a")]
    );
    // An unaliased source is named by its table, with or without the schema,
    // and a name listed twice is one target.
    assert_eq!(
        names(resolve("DELETE t, test.t FROM t").unwrap()),
        vec![t("t")]
    );
    // An alias replaces the table name as the only name.
    let error = resolve("DELETE t FROM t AS a").err().expect("refused");
    assert_eq!(error.message(), "Unknown table 't' in MULTI DELETE");
    // A derived table is not updatable (1288), a missing name is 1109.
    let error = resolve("DELETE d FROM (SELECT * FROM t) AS d, t")
        .err()
        .expect("refused");
    assert_eq!(
        error.message(),
        "The target table d of the DELETE is not updatable"
    );
    let error = resolve("DELETE x FROM t").err().expect("refused");
    assert_eq!(error.message(), "Unknown table 'x' in MULTI DELETE");
}

#[test]
fn update_set_columns_resolve_to_the_updatable_base_tables() {
    // Go `buildUpdateLists`: each SET column is found among the read's output
    // names and its source must be a base table of the outer FROM.
    let harness = Harness::new();
    let resolve = |sql: &str| {
        let statement = tidb_parser::parse(sql).expect("the UPDATE parses");
        let Stmt::Dml(dml) = statement else {
            panic!("not a DML statement");
        };
        let tidb_ast::DmlStmt::Update(update) = &*dml else {
            panic!("not an UPDATE");
        };
        harness.builder().resolve_update_targets(update)
    };
    let targets = resolve("UPDATE t AS x, t AS y SET x.b = 1, y.c = 2, x.b = 3").unwrap();
    assert_eq!(
        targets
            .iter()
            .map(|target| (target.database.as_str(), target.table.as_str(), target.column.as_str()))
            .collect::<Vec<_>>(),
        vec![("test", "t", "b"), ("test", "t", "c")]
    );
    let error = resolve("UPDATE (SELECT * FROM t) AS d, t SET d.b = 1")
        .err()
        .expect("refused");
    assert_eq!(
        error.message(),
        "The target table d of the UPDATE is not updatable"
    );
    let error = resolve("UPDATE t AS x, t AS y SET z = 1")
        .err()
        .expect("refused");
    assert_eq!(error.message(), "Unknown column 'z' in 'field list'");
    let error = resolve("UPDATE t AS x, t AS y SET b = 1")
        .err()
        .expect("refused");
    assert_eq!(error.message(), "Column 'b' in field list is ambiguous");
}

#[test]
fn multi_table_update_keeps_updated_aliases_and_what_set_reads() {
    // Go `buildUpdate`: the read keeps every column of an updated alias and
    // only the columns the SET expressions read from the others;
    // `buildUpdateTblColPosInfos` says where each updated table's row sits.
    use tidb_planner_core_operator_physicalop::{DeleteHandle, UpdateAssign};
    let harness = Harness::new();
    let build = |sql: &str| {
        let statement = tidb_parser::parse(sql).expect("the UPDATE parses");
        let Stmt::Dml(dml) = statement else {
            panic!("not a DML statement");
        };
        let tidb_ast::DmlStmt::Update(update) = &*dml else {
            panic!("not an UPDATE");
        };
        let tidb_ast::UpdateKind::Multi { from, .. } = &update.kind else {
            panic!("not a multi-table UPDATE");
        };
        let select = super::dml_source::dml_source_select(
            update.with.as_ref(),
            &update.hints,
            (**from).clone(),
            update.where_clause.as_ref(),
            &update.order_by,
            update.limit.as_ref(),
        );
        let mut builder = harness.builder();
        let (plan, expressions, _) =
            builder.build_multi_update_dml_source(&select, update, false)?;
        Ok::<_, tidb_planner_core_base::PlanError>((
            plan.schema().map(|schema| schema.columns.len()),
            expressions.len(),
            builder.update_tables,
            builder.update_assigns,
        ))
    };
    let (width, expressions, tables, assigns) =
        build("UPDATE t AS t1, t AS t2 SET t1.b = t2.c + 1").unwrap();
    assert_eq!(width, Some(4), "t1 whole, plus t2.c");
    assert_eq!(expressions, 1);
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].visible, "t1");
    assert_eq!(tables[0].columns, vec![(0, 0), (1, 1), (2, 2)]);
    assert_eq!(tables[0].handle, DeleteHandle::Int(0));
    assert_eq!(
        assigns,
        vec![UpdateAssign {
            table: 0,
            column: 1,
            expr: 0
        }]
    );
    let (width, _, tables, assigns) =
        build("UPDATE t AS t1, t AS t2 SET t2.b = 1, t1.c = 2").unwrap();
    assert_eq!(width, Some(6));
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0].visible, "t1", "the read's order, not the SET order");
    assert_eq!(tables[1].columns, vec![(3, 0), (4, 1), (5, 2)]);
    assert_eq!(assigns[0].table, 1);
    assert_eq!(assigns[1].table, 0);
    let error = build("UPDATE t AS t1, t AS t2 SET t1.nope = 1").err().unwrap();
    assert_eq!(error.message(), "Unknown column 'nope' in 'field list'");
}

#[test]
fn write_visits_follow_the_targets_the_builder_resolved() {
    // Go `buildUpdateLists` appends `UpdatePriv` per assignment's base table
    // (no authErr); `buildDelete` appends `DeletePriv` per target with the
    // `ErrTableaccessDenied` authErr; a CTE target is `ErrNonUpdatableTable`.
    use super::visit_info::VisitAuthError;
    let harness = Harness::new();
    let dml = |sql: &str| {
        let statement = tidb_parser::parse(sql).expect("the statement parses");
        let Stmt::Dml(dml) = statement else {
            panic!("not a DML statement");
        };
        (*dml).clone()
    };
    let tidb_ast::DmlStmt::Update(update) = dml("UPDATE t AS x, t AS y SET x.b = 1, y.c = 2") else {
        panic!("not an UPDATE");
    };
    let visits = harness.builder().update_write_visits(&update).unwrap();
    assert_eq!(visits.len(), 1, "both aliases are the one base table");
    assert_eq!(
        (visits[0].database.as_str(), visits[0].table.as_str()),
        ("test", "t")
    );
    assert_eq!(visits[0].privilege, tidb_mysql::UpdatePriv);
    assert_eq!(visits[0].error, VisitAuthError::None);

    let tidb_ast::DmlStmt::Delete(delete) = dml("DELETE x FROM t AS x, t AS y WHERE x.a = y.a")
    else {
        panic!("not a DELETE");
    };
    let visits = harness.builder().delete_write_visits(&delete).unwrap();
    assert_eq!(visits.len(), 1);
    assert_eq!(visits[0].privilege, tidb_mysql::DeletePriv);
    assert_eq!(
        visits[0].error,
        VisitAuthError::TableAccessDenied {
            command: "DELETE",
            table: "t".to_owned()
        }
    );

    let tidb_ast::DmlStmt::Delete(delete) = dml("DELETE FROM t WHERE a = 1") else {
        panic!("not a DELETE");
    };
    let visits = harness.builder().delete_write_visits(&delete).unwrap();
    assert_eq!(visits.len(), 1);
    assert_eq!(visits[0].table, "t");

    let tidb_ast::DmlStmt::Delete(delete) = dml("WITH c AS (SELECT 1) DELETE FROM c") else {
        panic!("not a DELETE");
    };
    let error = harness.builder().delete_write_visits(&delete).err().unwrap();
    assert_eq!(error.message(), "The target table c of the DELETE is not updatable");
}

#[test]
fn insert_values_are_expressions_over_the_target() {
    // Go `buildInsert`: `getAffectCols` names the columns written and
    // `buildValuesListOfInsert` rewrites each value over `mockTablePlan`; a
    // generated column accepts only DEFAULT, which leaves it no expression.
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    let parse = |sql: &str| {
        let statement = tidb_parser::parse(sql).expect("the INSERT parses");
        let Stmt::Dml(dml) = statement else {
            panic!("not a DML statement");
        };
        let tidb_ast::DmlStmt::Insert(insert) = (*dml).clone() else {
            panic!("not an INSERT");
        };
        insert
    };
    let default = |_: &str, _: &str, _: &str| {
        Ok(tidb_expr::expression::Expression::Constant(
            tidb_expr::constant::Constant::new(Datum::Int(7), FieldType::new(FieldTypeCode::LongLong)),
        ))
    };
    let build = |harness: &Harness, sql: &str| {
        let insert = parse(sql);
        let mut builder = harness.builder().with_insert_default_evaluator(&default);
        let target = builder.resolve_insert_target(&insert)?;
        let values = builder.build_insert_values(&insert, &target)?;
        Ok::<_, tidb_planner_core_base::PlanError>((target, values))
    };
    let harness = Harness::new();
    let (target, values) = build(&harness, "INSERT INTO t (c, a) VALUES (1 + 1, DEFAULT), (3, 4)").unwrap();
    assert_eq!(target.offsets, vec![2, 0]);
    assert_eq!(values.lists.len(), 2);
    assert!(values.lists.iter().flatten().all(Option::is_some));
    let (target, values) = build(&harness, "INSERT INTO t VALUES (1, a + 1, DEFAULT)").unwrap();
    assert_eq!(target.offsets, vec![0, 1, 2]);
    let reads = tidb_expr::simple_expr::extract_columns(values.lists[0][1].as_ref().unwrap());
    assert_eq!(reads.len(), 1, "`a + 1` reads the row's `a`");

    let error = build(&harness, "INSERT INTO t VALUES (1, 2, 3), (1)").err().unwrap();
    assert_eq!(error.message(), "Column count doesn't match value count at row 2");
    let error = build(&harness, "INSERT INTO t (nope) VALUES (1)").err().unwrap();
    assert_eq!(error.message(), "Unknown column 'nope' in 'field list'");

    let generated = Harness::new();
    generated.catalog.tables[0].columns[2].info.write().generated_expr_string = "`a` + 1".to_owned();
    let (_, values) = build(&generated, "INSERT INTO t VALUES (1, 2, DEFAULT)").unwrap();
    assert!(values.lists[0][2].is_none(), "a generated column's own DEFAULT");
    let error = build(&generated, "INSERT INTO t VALUES (1, 2, 3)").err().unwrap();
    assert_eq!(
        error.message(),
        "The value specified for generated column 'c' in table 't' is not allowed."
    );
    let error = build(&generated, "INSERT INTO t VALUES (DEFAULT(c), 2, 3)").err().unwrap();
    assert_eq!(
        error.message(),
        "The value specified for generated column 'a' in table 't' is not allowed."
    );
}

#[test]
fn insert_on_duplicate_assignments_resolve_over_schema4_on_duplicate() {
    // Go `ResolveOnDuplicate`: each assignment is built once over the target's
    // columns; `VALUES(col)` names a target column; a generated column's own
    // DEFAULT leaves no assignment.
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    let parse = |sql: &str| {
        let statement = tidb_parser::parse(sql).expect("the INSERT parses");
        let Stmt::Dml(dml) = statement else {
            panic!("not a DML statement");
        };
        let tidb_ast::DmlStmt::Insert(insert) = (*dml).clone() else {
            panic!("not an INSERT");
        };
        insert
    };
    let default = |_: &str, _: &str, _: &str| {
        Ok(tidb_expr::expression::Expression::Constant(
            tidb_expr::constant::Constant::new(Datum::Int(7), FieldType::new(FieldTypeCode::LongLong)),
        ))
    };
    let build = |harness: &Harness, sql: &str| {
        let insert = parse(sql);
        let mut builder = harness.builder().with_default_evaluator(&default);
        let target = builder.resolve_insert_target(&insert)?;
        builder.build_on_duplicate(&insert, &target, None)
    };
    let harness = Harness::new();
    let assigns = build(
        &harness,
        "INSERT INTO t VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE b = b + VALUES(c), c = DEFAULT",
    )
    .unwrap();
    assert_eq!(assigns.iter().map(|assign| assign.offset).collect::<Vec<_>>(), vec![1, 2]);
    assert!(assigns.iter().all(|assign| assign.lazy_error.is_none()));
    let error = build(
        &harness,
        "INSERT INTO t VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE b = VALUES(nope)",
    )
    .err()
    .unwrap();
    assert_eq!(error.message(), "Unknown column 'nope' in 'field list'");
    let error = build(
        &harness,
        "INSERT INTO t VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE nope = 1",
    )
    .err()
    .unwrap();
    assert_eq!(error.message(), "Unknown column 'nope' in 'field list'");

    let generated = Harness::new();
    generated.catalog.tables[0].columns[2].info.write().generated_expr_string = "`a` + 1".to_owned();
    let assigns = build(
        &generated,
        "INSERT INTO t VALUES (1, 2, DEFAULT) ON DUPLICATE KEY UPDATE c = DEFAULT, b = 1",
    )
    .unwrap();
    assert_eq!(assigns.len(), 1, "the generated column's own DEFAULT is dropped");
    let error = build(
        &generated,
        "INSERT INTO t VALUES (1, 2, DEFAULT) ON DUPLICATE KEY UPDATE c = 1",
    )
    .err()
    .unwrap();
    assert_eq!(
        error.message(),
        "The value specified for generated column 'c' in table 't' is not allowed."
    );
}

#[test]
fn single_table_update_assignments_resolve_to_table_columns() {
    // Go `buildUpdateLists`: each SET item is found among the table's names
    // (an alias replaces the table name as the only qualifier), a bare DEFAULT
    // names its own column, and a generated column accepts only DEFAULT.
    let parse = |sql: &str| {
        let statement = tidb_parser::parse(sql).expect("the UPDATE parses");
        let Stmt::Dml(dml) = statement else {
            panic!("not a DML statement");
        };
        let tidb_ast::DmlStmt::Update(update) = (*dml).clone() else {
            panic!("not an UPDATE");
        };
        update
    };
    let harness = Harness::new();
    let resolve = |harness: &Harness, sql: &str| {
        harness
            .builder()
            .resolve_single_update_assignments(&parse(sql), false)
    };
    let resolved = resolve(&harness, "UPDATE t SET c = DEFAULT, b = 1").unwrap();
    assert_eq!(resolved.iter().map(|item| item.column).collect::<Vec<_>>(), vec![2, 1]);
    assert!(matches!(
        &resolved[0].value,
        Some(tidb_ast::Expr::Default(Some(path))) if path == &vec!["c".to_owned()]
    ));
    assert!(resolve(&harness, "UPDATE t AS x SET x.b = 1").is_ok());
    let error = resolve(&harness, "UPDATE t AS x SET t.b = 1").err().unwrap();
    assert_eq!(error.message(), "Unknown column 't.b' in 'field list'");
    let error = resolve(&harness, "UPDATE t SET nope = 1").err().unwrap();
    assert_eq!(error.message(), "Unknown column 'nope' in 'field list'");

    let generated = Harness::new();
    generated.catalog.tables[0].columns[2].info.write().generated_expr_string = "`a` + 1".to_owned();
    let resolved = resolve(&generated, "UPDATE t SET c = DEFAULT").unwrap();
    assert!(resolved[0].value.is_none(), "left to the generation expression");
    let error = resolve(&generated, "UPDATE t SET c = 1").err().unwrap();
    assert_eq!(
        error.message(),
        "The value specified for generated column 'c' in table 't' is not allowed."
    );

    let mut heap = Harness::new();
    heap.catalog.tables[0].pk_is_handle = false;
    heap.catalog.tables[0].handle_col_offsets = Vec::new();
    let error = resolve(&heap, "UPDATE t SET _tidb_rowid = 1").err().unwrap();
    assert_eq!(
        error.message(),
        "insert, update and replace statements for _tidb_rowid are not supported"
    );
    assert!(heap
        .builder()
        .resolve_single_update_assignments(&parse("UPDATE t SET _tidb_rowid = 1"), true)
        .is_ok());
}

#[test]
fn insert_on_duplicate_columns_follow_the_resolved_assignments() {
    // Go `buildOnDuplicateUpdateColumns` reads `Insert.OnDuplicate`, which
    // `ResolveOnDuplicate` leaves without a generated column's own DEFAULT.
    let parse = |sql: &str| {
        let statement = tidb_parser::parse(sql).expect("the INSERT parses");
        let Stmt::Dml(dml) = statement else {
            panic!("not a DML statement");
        };
        let tidb_ast::DmlStmt::Insert(insert) = (*dml).clone() else {
            panic!("not an INSERT");
        };
        insert
    };
    let generated = Harness::new();
    generated.catalog.tables[0].columns[2].info.write().generated_expr_string = "`a` + 1".to_owned();
    let insert = parse("INSERT INTO t VALUES (1, 2, DEFAULT) ON DUPLICATE KEY UPDATE C = DEFAULT, b = 1");
    let builder = generated.builder();
    let target = builder.resolve_insert_target(&insert).unwrap();
    assert_eq!(
        builder.on_duplicate_columns(&insert, &target).unwrap(),
        vec!["b".to_owned()]
    );
}

#[test]
fn insert_generated_columns_are_expressions_over_the_whole_row() {
    // Go `resolveGeneratedColumns`: each generated column's expression is
    // rewritten over the table's own schema, so its columns index the stored
    // row; a column that is not generated has none.
    let mut generated = Harness::new();
    let expression = {
        let select = parse_select("SELECT a + b");
        match select.fields.iter().next() {
            Some(tidb_ast::SelectField::Expr { expr, .. }) => expr.clone(),
            other => panic!("unexpected field {other:?}"),
        }
    };
    generated.catalog.tables[0].columns[2].info.write().generated_expr_string = "`a` + 1".to_owned();
    generated.catalog.tables[0].columns[2].generated_expr = Some(expression);
    let statement = tidb_parser::parse("INSERT INTO t VALUES (1, 2, DEFAULT)").unwrap();
    let Stmt::Dml(dml) = statement else {
        panic!("not a DML statement");
    };
    let tidb_ast::DmlStmt::Insert(insert) = (*dml).clone() else {
        panic!("not an INSERT");
    };
    let mut builder = generated.builder();
    let target = builder.resolve_insert_target(&insert).unwrap();
    let resolved = builder.resolve_generated_columns(&target).unwrap();
    assert_eq!(resolved.len(), 3);
    assert!(resolved[0].is_none() && resolved[1].is_none());
    let columns = tidb_expr::simple_expr::extract_columns(resolved[2].as_ref().unwrap());
    let mut indexes: Vec<i64> = columns.iter().map(|column| column.index).collect();
    indexes.sort_unstable();
    assert_eq!(indexes, vec![0, 1], "`a + b` reads the stored row's first two columns");

    let plain = Harness::new();
    let mut builder = plain.builder();
    let target = builder.resolve_insert_target(&insert).unwrap();
    assert!(builder.resolve_generated_columns(&target).unwrap().is_empty());
}
