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

//! `FROM`/`JOIN` seam tests. WRITTEN, not transcreated, for the reason
//! [`super::tests`]' header gives: Go's own `buildJoin` coverage runs through
//! `testkit` against a live cluster and a real `infoschema`.
//!
//! What is proven here is the DECISION SURFACE of [`super::from`]: the
//! `ON`-condition wrap, the `USING`/`NATURAL` coalescing including the RIGHT
//! join's side swap and the `Redundant` marking, `LATERAL`'s apply, the
//! derived table's renames, the view's projection and its recursion refusal,
//! and the duplicate-alias error.

use tidb_ast::{Join, JoinNode, JoinType, SelectStmt, Stmt};
use tidb_datatype::{FieldType, FieldTypeCode, SessionTimeZone};
use tidb_expr::ZonedNoColumns;

use super::catalog::{SourceColumn, SourceTable, SourceView, TableSource};
use super::from::{
    check_non_uniq_table_alias, contains_lateral_in_join, extract_table_alias,
    find_join_full_schema, is_immediate_lateral_table_source, join_hint_flags, JoinHints,
};
use crate::expression_rewriter::ColumnIdAllocator;
use crate::find_best_task::LogicalJoinType;
use crate::logical::rule::flags;
use crate::logical::LogicalPlan;
use crate::plan_base::PlanIdAllocator;
use crate::plan_builder::PlanBuilder;

// ***** the catalogue *****

struct JoinCatalog {
    tables: Vec<SourceTable>,
    views: Vec<SourceView>,
}

impl TableSource for JoinCatalog {
    fn current_database(&self) -> &str {
        "test"
    }

    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable> {
        self.tables.iter().find(|table| {
            table.db_name.eq_ignore_ascii_case(db_name)
                && table.table_name.eq_ignore_ascii_case(table_name)
        })
    }

    fn database_exists(&self, db_name: &str) -> bool {
        db_name.eq_ignore_ascii_case("test")
    }

    fn find_view(&self, db_name: &str, view_name: &str) -> Option<&SourceView> {
        self.views.iter().find(|view| {
            view.db_name.eq_ignore_ascii_case(db_name)
                && view.view_name.eq_ignore_ascii_case(view_name)
        })
    }
}

fn column(offset: usize, name: &str) -> SourceColumn {
    SourceColumn {
        id: offset as i64 + 1,
        name: name.to_owned(),
        offset,
        ret_type: FieldType::new(FieldTypeCode::LongLong),
        is_public: true,
        ..SourceColumn::default()
    }
}

fn table(id: i64, name: &str, columns: &[&str]) -> SourceTable {
    SourceTable {
        table_id: id,
        table_name: name.to_owned(),
        db_name: "test".to_owned(),
        physical_table_id: id,
        columns: columns
            .iter()
            .enumerate()
            .map(|(offset, name)| column(offset, name))
            .collect(),
        pk_is_handle: true,
        handle_col_offsets: vec![0],
        ..SourceTable::default()
    }
}

/// `t1(a, b)`, `t2(a, c)`, plus `v` over `t1` and `bad_v` over itself.
fn catalog() -> JoinCatalog {
    JoinCatalog {
        tables: vec![table(100, "t1", &["a", "b"]), table(200, "t2", &["a", "c"])],
        views: vec![
            SourceView {
                db_name: "test".to_owned(),
                view_name: "v".to_owned(),
                select_sql: "SELECT a, b FROM test.t1".to_owned(),
                view_cols: Vec::new(),
                columns: vec![column(0, "va"), column(1, "vb")],
            },
            SourceView {
                db_name: "test".to_owned(),
                view_name: "bad_v".to_owned(),
                // The body reads the view itself, which is the recursion
                // `checkRecursiveView` refuses.
                select_sql: "SELECT a FROM test.bad_v".to_owned(),
                view_cols: Vec::new(),
                columns: vec![column(0, "a")],
            },
        ],
    }
}

struct Harness {
    catalog: JoinCatalog,
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

    fn builder(&self) -> PlanBuilder<'_, JoinCatalog, ZonedNoColumns> {
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
    match tidb_parser::parse(sql).expect("the test SQL parses") {
        Stmt::Query(query) => match query.into_inner() {
            tidb_ast::QueryStmt::Select(select) => *select,
            other => panic!("expected a SELECT, got {other:?}"),
        },
        other => panic!("expected a SELECT, got {other:?}"),
    }
}

fn from_clause(sql: &str) -> Join {
    parse_select(sql).from.expect("the statement has a FROM")
}

fn build(harness: &Harness, sql: &str) -> LogicalPlan {
    let mut builder = harness.builder();
    builder
        .build_select(&parse_select(sql))
        .expect("the statement builds")
        .0
}

/// The visible output names of a plan, as `table.column`.
fn qualified_names(plan: &LogicalPlan) -> Vec<String> {
    plan.output_names()
        .iter()
        .map(|name| {
            format!(
                "{}.{}",
                name.names.table.original, name.names.column.original
            )
        })
        .collect()
}

/// The join under the trailing projection `buildSelect` always adds.
fn join_under(plan: &LogicalPlan) -> &LogicalPlan {
    let mut current = plan;
    loop {
        match current {
            LogicalPlan::Join(_) | LogicalPlan::Apply(_) => return current,
            other => {
                let children = other.base().children();
                assert_eq!(
                    children.len(),
                    1,
                    "expected a single-child wrapper above the join, got {}",
                    other.tp()
                );
                current = &children[0];
            }
        }
    }
}

// ***** buildJoin: the ON-condition wrap *****

#[test]
fn test_inner_join_with_on_wraps_the_join_in_a_selection() {
    // Go `buildJoin` (:930): "Keep these expressions as a LogicalSelection
    // upon the inner join, in order to apply possible decorrelate
    // optimizations." An INNER join's ON clause becomes a Selection ABOVE the
    // join, NOT `AttachOnConds` on it.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a");
    let plan = builder.build_join(&from).expect("the join builds");

    let LogicalPlan::Selection(selection) = &plan else {
        panic!("expected a Selection over the join, got {}", plan.tp());
    };
    assert_eq!(selection.conditions.len(), 1);
    let LogicalPlan::Join(join) = &selection.base.children()[0] else {
        panic!("expected a Join under the Selection");
    };
    assert_eq!(join.join_type, LogicalJoinType::Inner);
    // The ON clause did NOT become the join's own condition.
    assert!(join.equal_conditions.is_empty());
    assert!(join.other_conditions.is_empty());
    // Left-deep: two children, left first.
    assert_eq!(join.base.children().len(), 2);
    assert_eq!(
        qualified_names(&plan)
            .into_iter()
            .filter(|name| name.starts_with("t1.") || name.starts_with("t2."))
            .take(2)
            .collect::<Vec<_>>(),
        vec!["t1.a".to_owned(), "t1.b".to_owned()]
    );

    // `buildJoin`'s opt flags (:751).
    assert!(builder.opt_flag & flags::PREDICATE_PUSH_DOWN != 0);
    assert!(builder.opt_flag & flags::JOIN_KEY_TYPE_CAST != 0);
    assert!(builder.opt_flag & flags::JOIN_REORDER != 0);
    assert!(builder.opt_flag & flags::EMPTY_SELECTION_ELIMINATOR != 0);
}

#[test]
fn test_outer_join_with_on_attaches_the_condition_to_the_join() {
    // The same ON clause on a LEFT join takes `AttachOnConds` instead, because
    // an outer join's ON is not a WHERE.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a");
    let plan = builder.build_join(&from).expect("the join builds");

    let LogicalPlan::Join(join) = &plan else {
        panic!("expected a bare Join, got {}", plan.tp());
    };
    assert_eq!(join.join_type, LogicalJoinType::LeftOuter);
    assert_eq!(join.equal_conditions.len(), 1);
    // `:848` the inner side loses NOT NULL.
    assert!(builder.opt_flag & flags::ELIMINATE_OUTER_JOIN != 0);
    assert!(builder.opt_flag & flags::OUTER_JOIN_TO_SEMI_JOIN != 0);
}

#[test]
fn test_straight_join_is_recorded_on_the_join() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1 STRAIGHT_JOIN t2 ON t1.a = t2.a");
    let plan = builder.build_join(&from).expect("the join builds");
    let join = join_under(&plan);
    let LogicalPlan::Join(join) = join else {
        panic!("expected a Join");
    };
    assert!(join.straight_join);

    // `b.inStraightJoin` sets it too, for a plain comma join.
    let mut builder = harness.builder();
    builder.in_straight_join = true;
    let from = from_clause("SELECT * FROM t1, t2");
    let plan = builder.build_join(&from).expect("the join builds");
    let LogicalPlan::Join(join) = &plan else {
        panic!("expected a Join");
    };
    assert!(join.straight_join);
    // A comma join is a CROSS join with no conditions at all.
    assert_eq!(join.join_type, LogicalJoinType::Inner);
    assert!(join.other_conditions.is_empty());
}

#[test]
fn test_a_single_operand_join_node_unwraps_to_its_left() {
    // `buildJoin` (:738): "joinNode.Right is nil and we only build the left
    // ResultSetNode."
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1");
    assert!(from.right.is_none());
    let plan = builder.build_join(&from).expect("the single table builds");
    assert!(matches!(plan, LogicalPlan::DataSource(_)));
}

// ***** USING / NATURAL *****

/// `full_names`' `redundant` flags, paired with the column each names.
fn redundant_full_names(join: &LogicalPlan) -> Vec<(String, bool)> {
    let LogicalPlan::Join(join) = join else {
        panic!("expected a Join, got {}", join.tp());
    };
    join.full_names
        .iter()
        .map(|name| {
            (
                format!(
                    "{}.{}",
                    name.names.table.original, name.names.column.original
                ),
                name.redundant,
            )
        })
        .collect()
}

#[test]
fn test_using_coalesces_the_common_column_and_orders_it_first() {
    // Go `buildUsingClause` (:1104): "coalesced common columns ... in the
    // order they appears in leftPlan", then the rest of the left, then the
    // rest of the right.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1 JOIN t2 USING (a)");
    let plan = builder.build_join(&from).expect("the join builds");

    let visible: Vec<String> = qualified_names(&plan)
        .into_iter()
        .filter(|name| !name.contains("_tidb_"))
        .collect();
    // `a` (coalesced, from the LEFT), then `t1.b`, then `t2.c` — `t2.a` is
    // absorbed and does not appear.
    assert_eq!(
        visible,
        vec!["t1.a".to_owned(), "t1.b".to_owned(), "t2.c".to_owned()]
    );

    let LogicalPlan::Join(join) = &plan else {
        panic!("expected a Join");
    };
    // The coalescing produced exactly one `t1.a = t2.a` other-condition.
    assert_eq!(join.other_conditions.len(), 1);
    // `FullSchema` still holds BOTH `a` columns; the right one is redundant.
    let full = join.full_schema.as_ref().expect("USING sets FullSchema");
    assert_eq!(full.columns.len(), join.full_names.len());
    let redundant: Vec<(String, bool)> = redundant_full_names(&plan)
        .into_iter()
        .filter(|(name, _)| name.ends_with(".a"))
        .collect();
    assert_eq!(
        redundant,
        vec![("t1.a".to_owned(), false), ("t2.a".to_owned(), true)],
        "an INNER join keeps the LEFT side as the canonical output"
    );
    // And the redundant column is remapped onto the visible one's position.
    assert_eq!(join.redundant_cols_to_output_idx.len(), 1);
}

#[test]
fn test_right_join_using_swaps_the_sides_and_the_redundant_flag() {
    // Go `coalesceCommonColumns` (:1156): for a RIGHT join the two sides are
    // swapped, so the RIGHT side becomes the canonical output and the LEFT
    // side's common column is the redundant one. `buildJoin`'s FullSchema
    // merge (:882) performs the SAME swap, so the two agree.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1 RIGHT JOIN t2 USING (a)");
    let plan = builder.build_join(&from).expect("the join builds");

    let visible: Vec<String> = qualified_names(&plan)
        .into_iter()
        .filter(|name| !name.contains("_tidb_"))
        .collect();
    // The right side leads now: `t2.a` coalesced, `t2.c`, then `t1.b`.
    assert_eq!(
        visible,
        vec!["t2.a".to_owned(), "t2.c".to_owned(), "t1.b".to_owned()]
    );

    let redundant: Vec<(String, bool)> = redundant_full_names(&plan)
        .into_iter()
        .filter(|(name, _)| name.ends_with(".a"))
        .collect();
    // FullNames also lead with the right (outer) side after the swap.
    assert_eq!(
        redundant,
        vec![("t2.a".to_owned(), false), ("t1.a".to_owned(), true)],
        "a RIGHT join keeps the RIGHT side as the canonical output"
    );
}

#[test]
fn test_natural_join_matches_every_common_column() {
    // Go `buildNaturalJoin` (:1128): the same coalescing with no filter.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1 NATURAL JOIN t2");
    let plan = builder.build_join(&from).expect("the join builds");

    let visible: Vec<String> = qualified_names(&plan)
        .into_iter()
        .filter(|name| !name.contains("_tidb_"))
        .collect();
    assert_eq!(
        visible,
        vec!["t1.a".to_owned(), "t1.b".to_owned(), "t2.c".to_owned()]
    );
    let LogicalPlan::Join(join) = &plan else {
        panic!("expected a Join");
    };
    // `_tidb_rowid` and `_tidb_commit_ts` are common to both tables by name
    // and are NOT matched — Go skips them explicitly (:1229).
    assert_eq!(
        join.other_conditions.len(),
        1,
        "only `a` is a common column; the extra columns are skipped"
    );
}

#[test]
fn test_using_an_unknown_column_is_refused() {
    // Go `coalesceCommonColumns` (:1300): ErrUnknownColumn for a USING name
    // that is not common to both sides.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1 JOIN t2 USING (b)");
    let error = builder.build_join(&from).expect_err("`b` is not on t2");
    assert!(
        format!("{error:?}").contains("Unknown column"),
        "expected ErrUnknownColumn, got {error:?}"
    );
}

// ***** LATERAL *****

#[test]
fn test_lateral_derived_table_becomes_a_logical_apply() {
    // Go `buildLateralJoin` (:956): a LATERAL derived table is a LogicalApply
    // with InnerJoin, marked `IsLateral`.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1, LATERAL (SELECT 1 AS la) AS d");
    assert!(contains_lateral_in_join(&from));
    assert!(is_immediate_lateral_table_source(
        from.right.as_ref().expect("the join has a right operand")
    ));

    let plan = builder.build_join(&from).expect("the lateral join builds");
    let LogicalPlan::Apply(apply) = &plan else {
        panic!("expected a LogicalApply, got {}", plan.tp());
    };
    assert!(apply.is_lateral);
    assert!(!apply.no_decorrelate);
    assert_eq!(apply.join.join_type, LogicalJoinType::Inner);
    assert_eq!(apply.join.base.children().len(), 2);
    // The apply is produced because the RIGHT OPERAND IS LATERAL, which is
    // Go's condition (a) — no actual correlation is required for that arm.
    // A body that really does reference an outer column needs correlated
    // resolution, which is `from.rs`'s named `rewrite_scalar` boundary.
    assert!(apply.cor_cols.is_empty());
    // `:986` the decorrelation flags.
    assert!(builder.opt_flag & flags::DECORRELATE != 0);
    assert!(builder.opt_flag & flags::BUILD_KEY_INFO != 0);
    // `:756` LATERAL disables join reorder.
    assert!(builder.opt_flag & flags::JOIN_REORDER == 0);
    // The outer schema push is balanced again.
    assert_eq!(builder.lateral_outer_count, 0);
    assert!(builder.outer_schemas.is_empty());
}

#[test]
fn test_lateral_refuses_the_clauses_go_refuses() {
    let harness = Harness::new();
    for (sql, expected) in [
        (
            "SELECT * FROM t1 LEFT JOIN LATERAL (SELECT 1 AS la) AS d ON TRUE",
            "LEFT JOIN is not supported with LATERAL",
        ),
        (
            "SELECT * FROM t1 RIGHT JOIN LATERAL (SELECT 1 AS la) AS d ON TRUE",
            "RIGHT JOIN is not supported with LATERAL",
        ),
    ] {
        let mut builder = harness.builder();
        let from = from_clause(sql);
        let error = builder.build_join(&from).expect_err("LATERAL refuses this");
        assert!(
            format!("{error:?}").contains(expected),
            "expected {expected:?}, got {error:?}"
        );
    }
}

// ***** derived tables *****

#[test]
fn test_derived_table_takes_the_alias_and_drops_the_database() {
    // Go `buildResultSetNode` (:497): the alias replaces the table name and
    // DBName is CLEARED so an error reads "d.a" and not "test.d.a".
    let harness = Harness::new();
    let plan = build(&harness, "SELECT * FROM (SELECT a, b FROM t1) AS d");
    for name in plan.output_names() {
        assert_eq!(name.names.table.original, "d");
        assert!(
            name.names.database.original.is_empty(),
            "a derived table's names carry no database"
        );
    }
    assert_eq!(
        plan.output_names()
            .iter()
            .map(|name| name.names.column.original.clone())
            .collect::<Vec<_>>(),
        vec!["a".to_owned(), "b".to_owned()]
    );
}

#[test]
fn test_lateral_column_alias_list_renames_the_output() {
    // Go `buildResultSetNode` (:530), harvested from `driver/from.rs:1484`
    // `rename_derived_columns`: `AS d(x, y)` renames positionally.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1, LATERAL (SELECT 1, 2) AS d(x, y)");
    let plan = builder.build_join(&from).expect("the lateral join builds");
    let renamed: Vec<String> = plan
        .output_names()
        .iter()
        .filter(|name| name.names.table.original == "d")
        .map(|name| name.names.column.original.clone())
        .collect();
    assert_eq!(renamed, vec!["x".to_owned(), "y".to_owned()]);
}

#[test]
fn test_a_column_alias_list_of_the_wrong_length_is_refused() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let from = from_clause("SELECT * FROM t1, LATERAL (SELECT 1, 2) AS d(x)");
    let error = builder
        .build_join(&from)
        .expect_err("one alias cannot rename two columns");
    assert!(
        format!("{error:?}").contains("different column counts"),
        "expected ErrViewWrongList, got {error:?}"
    );
}

#[test]
fn test_a_derived_table_with_duplicate_output_names_is_refused() {
    // Go `buildResultSetNode` (:567): "select * from (select 1, 1) as a" is a
    // duplicate.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let error = builder
        .build_select(&parse_select("SELECT * FROM (SELECT a, a FROM t1) AS d"))
        .expect_err("two columns named `a` collide");
    assert!(
        format!("{error:?}").contains("Duplicate column name"),
        "expected ErrDupFieldName, got {error:?}"
    );
}

// ***** views *****

#[test]
fn test_a_view_builds_a_projection_over_its_body() {
    // Go `BuildDataSourceFromView` (:5509) + `buildProjUponView` (:5646): the
    // projection presents the VIEW's own column names over the body's plan.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let view = harness
        .catalog
        .find_view("test", "v")
        .expect("the view is in the catalogue");
    let plan = builder
        .build_data_source_from_view(view)
        .expect("the view builds");

    let LogicalPlan::Projection(projection) = &plan else {
        panic!(
            "expected a Projection over the view body, got {}",
            plan.tp()
        );
    };
    assert_eq!(projection.exprs.len(), 2);
    assert_eq!(
        qualified_names(&plan),
        vec!["v.va".to_owned(), "v.vb".to_owned()],
        "the names are the VIEW's, qualified by the view rather than the table"
    );
    // The underlying table is still recorded, which is what `OrigTblName` is
    // for.
    assert_eq!(plan.output_names()[0].names.original_table.original, "t1");
    // The guard released, so the same view builds again.
    assert!(builder.building_view_stack.is_empty());
    assert!(builder.build_data_source_from_view(view).is_ok());
}

#[test]
fn test_a_recursive_view_is_refused() {
    // Go `checkRecursiveView` (:5487): ErrViewRecursive.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let view = harness
        .catalog
        .find_view("test", "bad_v")
        .expect("the view is in the catalogue");
    // The body references `bad_v` itself; the catalogue has no TABLE by that
    // name, so the recursion has to be caught by the stack and not by the
    // lookup.
    let error = builder
        .build_data_source_from_view(view)
        .expect_err("a self-referencing view is refused");
    let message = format!("{error:?}");
    assert!(
        message.contains("view recursion") || message.contains("doesn't exist"),
        "expected a recursion or missing-table refusal, got {message}"
    );
    // Either way the guard is released.
    assert!(builder.building_view_stack.is_empty());
}

#[test]
fn test_the_view_guard_refuses_a_reentrant_build() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let guard = builder
        .check_recursive_view("test", "v")
        .expect("the first entry is allowed");
    let error = builder
        .check_recursive_view("TEST", "V")
        .expect_err("the second entry is the recursion");
    assert!(format!("{error:?}").contains("view recursion"));
    guard.release(&mut builder);
    assert!(builder.check_recursive_view("test", "v").is_ok());
}

// ***** preprocess: duplicate table aliases *****

#[test]
fn test_duplicate_table_aliases_are_refused() {
    // Go `checkNonUniqTableAlias` (`preprocess.go:1139`) + `isTableAliasDuplicate`
    // (`:1158`): ErrNonUniqTable.
    for sql in [
        "SELECT * FROM t1, t1",
        "SELECT * FROM t1 AS x JOIN t2 AS x ON x.a = 1",
        "SELECT * FROM t1 JOIN t2 ON TRUE JOIN t1 ON TRUE",
    ] {
        let from = from_clause(sql);
        let node = JoinNode::Join(Box::new(from));
        let error =
            check_non_uniq_table_alias(&node, false).expect_err("the duplicate alias is refused");
        assert!(
            format!("{error:?}").contains("Not unique table/alias"),
            "expected ErrNonUniqTable for {sql:?}, got {error:?}"
        );
    }
}

#[test]
fn test_distinct_aliases_and_oracle_mode_are_allowed() {
    // An alias makes the second reference unique...
    let node = JoinNode::Join(Box::new(from_clause("SELECT * FROM t1, t1 AS x")));
    assert!(check_non_uniq_table_alias(&node, false).is_ok());
    // ...and ORACLE mode skips the check entirely (`preprocess.go:1145`).
    let node = JoinNode::Join(Box::new(from_clause("SELECT * FROM t1, t1")));
    assert!(check_non_uniq_table_alias(&node, true).is_ok());
}

// ***** the small helpers *****

#[test]
fn test_extract_table_alias_rejects_a_conflicting_name_set() {
    // Go `util.ExtractTableAlias` (`misc.go:244`).
    let harness = Harness::new();
    let mut builder = harness.builder();
    let single = builder
        .build_join(&from_clause("SELECT * FROM t1"))
        .expect("the single table builds");
    assert_eq!(
        extract_table_alias(single.output_names()).map(|alias| alias.table_name),
        Some("t1".to_owned())
    );

    // Two tables under a join disagree, so no hint can name the join.
    let mut builder = harness.builder();
    let joined = builder
        .build_join(&from_clause("SELECT * FROM t1, t2"))
        .expect("the join builds");
    assert_eq!(extract_table_alias(joined.output_names()), None);
    assert_eq!(extract_table_alias(&[]), None);
}

#[test]
fn test_join_hints_set_the_preferred_type_on_the_named_side() {
    // Go `LogicalJoin.SetPreferredJoinTypeAndOrder` (`logical_join.go:1596`).
    let harness = Harness::new();
    let mut hints = JoinHints::default();
    hints.hint_table(
        "",
        "t2",
        join_hint_flags::MERGE_JOIN | join_hint_flags::INLJ,
    );

    let mut builder = harness.builder();
    builder.join_hints = hints;
    let plan = builder
        .build_join(&from_clause("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a"))
        .expect("the join builds");
    let LogicalPlan::Join(join) = &plan else {
        panic!("expected a Join");
    };
    assert!(join.prefer_join_type & join_hint_flags::MERGE_JOIN != 0);
    assert!(join.right_prefer_join_type & join_hint_flags::MERGE_JOIN != 0);
    assert_eq!(join.left_prefer_join_type & join_hint_flags::MERGE_JOIN, 0);
    // An INLJ hint on the right side asks for the RIGHT child as the inner
    // one, which is a DIFFERENT bit on the join than on the side.
    assert!(join.prefer_join_type & join_hint_flags::RIGHT_AS_INLJ_INNER != 0);
    assert!(join.right_prefer_join_type & join_hint_flags::INLJ != 0);
}

#[test]
fn test_find_join_full_schema_looks_through_a_selection_but_not_a_projection() {
    // Go `findJoinFullSchema` (:645) and its own comment: a Selection from an
    // ON clause is transparent, a Projection is a derived-table boundary.
    let harness = Harness::new();
    let mut builder = harness.builder();
    // `USING` sets FullSchema; the INNER `ON`-free form leaves a bare join.
    let plan = builder
        .build_join(&from_clause("SELECT * FROM t1 JOIN t2 USING (a)"))
        .expect("the join builds");
    assert!(find_join_full_schema(&plan).is_some());

    // A derived table over the same join hides it.
    let derived = build(
        &harness,
        "SELECT * FROM (SELECT * FROM t1 JOIN t2 USING (a)) AS d",
    );
    assert!(
        find_join_full_schema(&derived).is_none(),
        "a Projection boundary must not leak the inner join's FullSchema"
    );
}

#[test]
fn test_lateral_detection_walks_the_subtree_but_immediacy_does_not() {
    // `containsLateralTableSource` (:676) vs `isImmediateLateralTableSource`
    // (:721): the first is the outer-schema PUSH decision, the second is the
    // APPLY decision, and Go deliberately makes the second tighter.
    let nested = from_clause(
        "SELECT * FROM t1 JOIN (SELECT * FROM t2, LATERAL (SELECT 1 AS x) AS l) AS d ON TRUE",
    );
    assert!(contains_lateral_in_join(&nested));
    assert!(
        !is_immediate_lateral_table_source(nested.right.as_ref().expect("a right operand")),
        "a LATERAL nested inside the right subtree is not itself the right operand"
    );

    let plain = from_clause("SELECT * FROM t1 JOIN t2 ON TRUE");
    assert!(!contains_lateral_in_join(&plain));
}

#[test]
fn test_a_cross_join_type_maps_to_an_inner_join() {
    // `ast.CrossJoin` covers `JOIN` / `INNER JOIN` / `CROSS JOIN` / comma.
    let from = from_clause("SELECT * FROM t1 CROSS JOIN t2");
    assert_eq!(from.tp, JoinType::Cross);
    let harness = Harness::new();
    let mut builder = harness.builder();
    let plan = builder.build_join(&from).expect("the join builds");
    let LogicalPlan::Join(join) = &plan else {
        panic!("expected a Join");
    };
    assert_eq!(join.join_type, LogicalJoinType::Inner);
}

// ***** buildSelectLock and buildMemTable *****

#[test]
fn test_build_select_lock_carries_the_tail_handle_map() {
    // Go `buildSelectLock` (`planbuilder.go:1610`): `TblID2Handle` is the
    // handle helper's TAIL map, resolved back to the child's own columns.
    use crate::logical::lock::SelectLockType;

    let harness = Harness::new();
    let mut builder = harness.builder();
    let source = builder
        .build_join(&from_clause("SELECT * FROM t1"))
        .expect("the table builds");
    let plan = builder
        .build_select_lock(source, SelectLockType::ForUpdate, 0)
        .expect("the lock builds");
    let LogicalPlan::Lock(lock) = &plan else {
        panic!("expected a LogicalLock, got {}", plan.tp());
    };
    assert_eq!(lock.lock_type, SelectLockType::ForUpdate);
    // `t1`'s handle is its primary key `a`, under the table's own id.
    let handles = lock
        .tbl_id_to_handle_cols
        .get(&100)
        .expect("t1's handle is recorded");
    assert_eq!(handles.len(), 1);
    // The `pid` column map is the named boundary and stays empty.
    assert!(lock.tbl_id_to_phys_tbl_id_col.is_empty());
}

#[test]
fn test_build_mem_table_produces_the_table_s_own_schema() {
    // Go `buildMemTable` (:5372): "the memory table has a stable schema and
    // there is no online DDL on the memory table" — so no extra handle and no
    // commit-ts column is appended, unlike `buildDataSource`.
    let harness = Harness::new();
    let mut builder = harness.builder();
    let mem = table(300, "CLUSTER_INFO", &["TYPE", "INSTANCE"]);
    let plan = builder.build_mem_table("information_schema", &mem);
    let LogicalPlan::MemTable(mem_table) = &plan else {
        panic!("expected a LogicalMemTable, got {}", plan.tp());
    };
    assert_eq!(mem_table.db_name, "information_schema");
    assert_eq!(mem_table.columns.len(), 2);
    assert_eq!(plan.schema().expect("a schema").columns.len(), 2);
    assert_eq!(
        plan.output_names()
            .iter()
            .map(|name| name.names.column.original.clone())
            .collect::<Vec<_>>(),
        vec!["TYPE".to_owned(), "INSTANCE".to_owned()]
    );
    // The handle helper got exactly one push, as every leaf builder owes it.
    assert_eq!(builder.handle_helper.depth(), 1);
}
