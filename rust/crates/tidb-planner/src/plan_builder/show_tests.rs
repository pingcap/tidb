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

//! [`PlanBuilder::build_show`]'s plan shapes against Go's `buildShow`.

use tidb_ast::Stmt;
use tidb_datatype::SessionTimeZone;
use tidb_expr::ZonedNoColumns;

use super::catalog::{SourceTable, TableSource};
use super::PlanBuilder;
use crate::expression_rewriter::ColumnIdAllocator;
use crate::common_plans::Plan;
use crate::logical::LogicalPlan;
use crate::plan_base::PlanIdAllocator;

/// A catalog with no tables: the SHOW kinds tested here read none.
struct NoTables;

impl TableSource for NoTables {
    fn current_database(&self) -> &str {
        ""
    }

    fn find_table(&self, _db_name: &str, _table_name: &str) -> Option<&SourceTable> {
        None
    }

    fn database_exists(&self, _db_name: &str) -> bool {
        false
    }
}

struct Harness {
    catalog: NoTables,
    ctx: ZonedNoColumns,
    plan_ids: PlanIdAllocator,
    column_ids: ColumnIdAllocator,
}

impl Harness {
    fn new() -> Self {
        Self {
            catalog: NoTables,
            ctx: ZonedNoColumns(SessionTimeZone::utc()),
            plan_ids: PlanIdAllocator::default(),
            column_ids: ColumnIdAllocator::new(),
        }
    }

    fn builder(&self) -> PlanBuilder<'_, NoTables, ZonedNoColumns> {
        PlanBuilder::new(
            &self.catalog,
            &self.ctx,
            &self.plan_ids,
            &self.column_ids,
            SessionTimeZone::utc(),
        )
    }
}

fn column_names(plan: &LogicalPlan) -> Vec<String> {
    plan.output_names()
        .iter()
        .map(|name| name.names.column.original.clone())
        .collect()
}

fn parse_show(sql: &str, current_db: &str) -> tidb_ast::ShowStmt {
    match tidb_parser::parse(sql).expect("the SHOW parses") {
        Stmt::Admin(admin) => match admin.into_inner() {
            tidb_ast::AdminStmt::Show(show) => {
                let mut show = *show;
                crate::preprocess::resolve_show_stmt(&mut show, current_db, None)
                    .expect("the SHOW preprocesses");
                show
            }
            other => panic!("expected a SHOW, got {other:?}"),
        },
        other => panic!("expected a SHOW, got {other:?}"),
    }
}

fn plan_shape(plan: &LogicalPlan) -> Vec<&'static str> {
    let mut shape = Vec::new();
    let mut node = plan;
    loop {
        shape.push(match node {
            LogicalPlan::Show(_) => "Show",
            LogicalPlan::Selection(_) => "Selection",
            LogicalPlan::Projection(_) => "Projection",
            LogicalPlan::Sort(_) => "Sort",
            LogicalPlan::Limit(_) => "Limit",
            _ => "other",
        });
        match node.children().first() {
            Some(child) => node = child,
            None => return shape,
        }
    }
}

/// Go `buildShow`: a literal LIKE is the extractor's, so SHOW TABLES stays a
/// bare `LogicalShow` named after the database and pattern.
#[test]
fn show_tables_literal_like_is_extracted_not_selected() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let plan = builder
        .build_show(&parse_show("show tables like 'T%'", "test"))
        .expect("SHOW TABLES plans");
    let Plan::Logical(plan) = plan else { panic!("a logical SHOW plan") };
    assert_eq!(plan_shape(&plan), ["Show"]);
    assert_eq!(column_names(&plan), ["Tables_in_test (T%)"]);
    let LogicalPlan::Show(show) = &plan else { unreachable!() };
    let Some(crate::logical::ShowPredicateExtractor::Base(extractor)) = &show.extractor else {
        panic!("the base extractor claimed the pattern");
    };
    assert_eq!(extractor.field_pattern, "t%");
}

/// Go `buildShow`: WHERE is a selection under the closing projection, and
/// SHOW VARIABLES sorts on its first column above everything.
#[test]
fn show_where_selects_under_a_projection_and_variables_sort() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let plan = builder
        .build_show(&parse_show("show variables where variable_name = 'x'", ""))
        .expect("SHOW VARIABLES plans");
    let Plan::Logical(plan) = plan else { panic!("a logical SHOW plan") };
    assert_eq!(plan_shape(&plan), ["Sort", "Projection", "Selection", "Show"]);
    assert_eq!(column_names(&plan), ["Variable_name", "Value"]);
    assert_ne!(builder.opt_flag & crate::logical::rule::flags::ELIMINATE_PROJECTION, 0);
}

/// Go `buildShow`: SHOW WARNINGS has no extractor, so LIKE tests `Level`.
#[test]
fn show_warnings_like_is_a_selection_on_the_first_column() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let plan = builder
        .build_show(&parse_show("show warnings like 'Note'", ""))
        .expect("SHOW WARNINGS plans");
    let Plan::Logical(plan) = plan else { panic!("a logical SHOW plan") };
    assert_eq!(plan_shape(&plan), ["Projection", "Selection", "Show"]);
    assert_eq!(column_names(&plan), ["Level", "Code", "Message"]);
}

/// Go `buildShow`: SHOW TABLES with no database is `ErrNoDB`.
#[test]
fn show_tables_without_a_database_is_no_db() {
    let harness = Harness::new();
    let mut builder = harness.builder();
    let error = builder
        .build_show(&parse_show("show tables", ""))
        .expect_err("no database is selected");
    assert_eq!(error.kind(), &crate::plan_base::PlanErrorKind::NoDB);
}
