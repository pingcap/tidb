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

//! Port of `pkg/parser/ast/ddl_test.go::TestDDLVisitorCover` (origin/master).

#![cfg(test)]

use crate::{
    AlterTableAction, AlterTableStmt, ColumnDef, ColumnOption, ColumnPosition, CreateIndexStmt,
    CreateTableStmt, CreateTableTemporary, DdlStmt, DropIndexLock, DropIndexStmt, DropTableStmt,
    DropTemporary, Expr, ForeignKeyConstraintDefinition, ForeignKeyMatch, ForeignKeyReference,
    IndexConstraintDefinition, IndexConstraintKind, IndexKind, IndexPart, NodeBox, Stmt,
    TableConstraint, ViewCheckOption, ViewSecurity,
};
use crate::{CreateViewStmt, RenameTableStmt, UserSpec};

/// Go's `checkExpr` sentinel: the visitor counts only visits reaching this
/// distinctive column expression.
fn check_expr() -> Expr {
    Expr::Column(vec!["__check".to_string()])
}

/// Counts visits reaching the [`check_expr`] marker; everything else passes
/// through exactly like Go's `checkVisitor`.
#[derive(Default)]
struct CheckVisitor {
    enter_count: usize,
    leave_count: usize,
}

impl crate::Visitor for CheckVisitor {
    fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
        if let Some(Expr::Column(path)) = node.downcast_ref::<Expr>() {
            if path == &vec!["__check".to_string()] {
                self.enter_count += 1;
                // Go's checkExpr.Accept returns skip-children=true after its
                // own Enter; the marker expression has no children to skip.
                return true;
            }
        }
        false
    }

    fn leave(&mut self, node: &mut dyn std::any::Any) -> bool {
        if let Some(Expr::Column(path)) = node.downcast_ref::<Expr>() {
            if path == &vec!["__check".to_string()] {
                self.leave_count += 1;
            }
        }
        true
    }
}

/// A row target: either a whole statement (like most Go rows) or one of the
/// bare payload nodes Go's table also drives directly.
enum Target {
    Statement(Stmt),
    Option(ColumnOption),
    Position(ColumnPosition),
    Constraint(TableConstraint),
    Part(IndexPart),
}

fn stmt_ddl(ddl: DdlStmt) -> Target {
    Target::Statement(Stmt::Ddl(NodeBox::new(ddl)))
}

fn named_part(name: &str) -> IndexPart {
    IndexPart::Column {
        name: name.to_string(),
        prefix_len: None,
        desc: false,
    }
}

fn key_constraint() -> IndexConstraintDefinition {
    IndexConstraintDefinition {
        kind: IndexConstraintKind::Index,
        if_not_exists: false,
        name: Some("par_ind".to_string()),
        is_empty_index: false,
        parts: vec![named_part("c1"), named_part("c2")],
        options: Default::default(),
    }
}

fn reference_def() -> ForeignKeyReference {
    ForeignKeyReference {
        table: Some(vec!["parent".to_string()]),
        parts: Some(vec![named_part("id"), named_part("hello")]),
        match_type: ForeignKeyMatch::default(),
        on_delete: None,
        on_update: None,
    }
}

fn plain_column(name: &str) -> ColumnDef {
    ColumnDef {
        qualifier: Vec::new(),
        name: name.to_string(),
        ty: crate::ColumnType {
            name: String::new(),
            args: Vec::new(),
            unsigned: false,
            zerofill: false,
            binary: false,
            charset: None,
        },
        options: Vec::new(),
    }
}

fn empty_select() -> crate::SelectStmt {
    crate::SelectStmt {
        kind: Default::default(),
        is_in_braces: false,
        with: None,
        hints: Vec::new(),
        priority: Default::default(),
        sql_small_result: false,
        sql_big_result: false,
        sql_buffer_result: false,
        sql_no_cache: false,
        straight_join: false,
        calc_found_rows: false,
        distinct: false,
        all: false,
        fields: Default::default(),
        values: Vec::new(),
        from: None,
        where_clause: None,
        group_by: Vec::new(),
        rollup: false,
        having: None,
        windows: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        lock: None,
        into_outfile: None,
        into_vars: Vec::new(),
    }
}

fn empty_user_spec() -> UserSpec {
    UserSpec {
        current_user: false,
        user: String::new(),
        host: String::new(),
    }
}

fn bare_create_table(
    columns: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
) -> CreateTableStmt {
    CreateTableStmt {
        temporary: CreateTableTemporary::None,
        on_commit_delete: false,
        if_not_exists: false,
        name: vec!["t".to_string()],
        like_table: None,
        columns,
        table_constraints: constraints,
        table_options: Vec::new(),
        partitioning: None,
        splits: Vec::new(),
        ctas: None,
    }
}

/// `pkg/parser/ast/ddl_test.go::TestDDLVisitorCover`.
///
/// Go constructs zero-value nodes (some embedding `checkExpr` sentinels) and
/// requires balanced Enter/Leave counts reaching each sentinel exactly once,
/// then replays every node through the generic `visitor1`. Rust's closed
/// expression enum cannot embed a custom counter type, so the marker is a
/// distinctive column expression, and each row embeds as many markers as the
/// Go row had reachable `checkExpr` children.
#[test]
fn ddl_visitor_cover() {
    let ce = check_expr();
    let rows: Vec<(Target, usize)> = vec![
        // {&CreateDatabaseStmt{}}
        (
            stmt_ddl(DdlStmt::CreateDatabase {
                if_not_exists: false,
                name: String::new(),
                options: Vec::new(),
            }),
            0,
        ),
        // {&AlterDatabaseStmt{}}
        (
            stmt_ddl(DdlStmt::AlterDatabase {
                name: None,
                options: Vec::new(),
            }),
            0,
        ),
        // {&DropDatabaseStmt{}}
        (
            stmt_ddl(DdlStmt::DropDatabase {
                if_exists: false,
                name: String::new(),
            }),
            0,
        ),
        // {&DropIndexStmt{Table: &TableName{}}}
        (
            stmt_ddl(DdlStmt::DropIndex(Box::new(DropIndexStmt {
                is_hypo: false,
                if_exists: false,
                name: String::new(),
                table: vec![String::new()],
                algorithm: None,
                lock: None::<DropIndexLock>,
            }))),
            0,
        ),
        // {&DropTableStmt{Tables: []*TableName{{}, {}}}}
        (
            stmt_ddl(DdlStmt::DropTable(Box::new(DropTableStmt {
                temporary: DropTemporary::None,
                if_exists: false,
                names: vec![Vec::new(), Vec::new()],
            }))),
            0,
        ),
        // {&RenameTableStmt{TableToTables: []*TableToTable{}}}
        (
            stmt_ddl(DdlStmt::RenameTable(Box::new(RenameTableStmt {
                pairs: Vec::new(),
            }))),
            0,
        ),
        // {&TruncateTableStmt{Table: &TableName{}}}
        (stmt_ddl(DdlStmt::TruncateTable(Box::default())), 0),
        // {&AlterTableStmt{Table, Specs:[full spec]}} — index/FK constraints,
        // grouped columns, single column with position, attributes spec.
        (
            stmt_ddl(DdlStmt::AlterTable(Box::new(AlterTableStmt {
                name: vec!["t".to_string()],
                actions: vec![
                    AlterTableAction::AddIndexConstraint(key_constraint()),
                    AlterTableAction::AddForeignKey(ForeignKeyConstraintDefinition {
                        name: None,
                        if_not_exists: false,
                        parts: vec![named_part("parent_id")],
                        reference: reference_def(),
                    }),
                    AlterTableAction::AddColumns {
                        if_not_exists: false,
                        columns: vec![plain_column("a")],
                        constraints: Vec::new(),
                    },
                    AlterTableAction::AddColumn {
                        if_not_exists: false,
                        column: plain_column("old"),
                        position: ColumnPosition::After("b".to_string()),
                    },
                    AlterTableAction::SetAttributes(crate::AttributesSpec { attributes: None }),
                ],
            }))),
            0,
        ),
        // {&CreateIndexStmt{Table: &TableName{}}}
        (
            stmt_ddl(DdlStmt::CreateIndex(Box::new(CreateIndexStmt {
                kind: IndexKind::Ordinary,
                if_not_exists: false,
                name: "idx".to_string(),
                table: vec!["t".to_string()],
                parts: Vec::new(),
                options: Default::default(),
                online: Default::default(),
            }))),
            0,
        ),
        // {&CreateTableStmt{Table, ReferTable}} (LIKE form)
        (
            stmt_ddl(DdlStmt::CreateTable(Box::new({
                let mut create = bare_create_table(Vec::new(), Vec::new());
                create.like_table = Some(vec!["src".to_string()]);
                create
            }))),
            0,
        ),
        // {&CreateViewStmt{ViewName, Select: &SelectStmt{}}}
        (
            stmt_ddl(DdlStmt::CreateView(Box::new(CreateViewStmt {
                or_replace: false,
                algorithm: Default::default(),
                definer: empty_user_spec(),
                security: ViewSecurity::DEFINER,
                name: vec!["v".to_string()],
                columns: Vec::new(),
                query: NodeBox::new(crate::QueryStmt::Select(Box::new(empty_select()))),
                query_parenthesized: false,
                check_option: ViewCheckOption::default(),
            }))),
            0,
        ),
        // {&AlterTableSpec{}} — a payload-free specification.
        (
            stmt_ddl(DdlStmt::AlterTable(Box::new(AlterTableStmt {
                name: vec!["t".to_string()],
                actions: vec![AlterTableAction::Force],
            }))),
            0,
        ),
        // {&ColumnDef{Name, Options:[{Expr: ce}]}}
        (
            stmt_ddl(DdlStmt::CreateTable(Box::new(bare_create_table(
                vec![ColumnDef {
                    qualifier: Vec::new(),
                    name: "col".to_string(),
                    ty: plain_column("").ty,
                    options: vec![ColumnOption::Default(ce.clone())],
                }],
                Vec::new(),
            )))),
            1,
        ),
        // {&ColumnOption{Expr: ce}}
        (Target::Option(ColumnOption::Default(ce.clone())), 1),
        // {&ColumnPosition{RelativeColumn: &ColumnName{}}}
        (Target::Position(ColumnPosition::After("c".to_string())), 0),
        // {&Constraint{Keys, Refer, Option}}
        (
            Target::Constraint(TableConstraint::Index(key_constraint())),
            0,
        ),
        // Same shape through the foreign-key constraint class.
        (
            Target::Constraint(TableConstraint::ForeignKey(
                ForeignKeyConstraintDefinition {
                    name: None,
                    if_not_exists: false,
                    parts: vec![named_part("a"), named_part("b")],
                    reference: reference_def(),
                },
            )),
            0,
        ),
        // {&IndexPartSpecification{Column: &ColumnName{}}}
        (Target::Part(named_part("k")), 0),
        // {&AlterTableSpec{NewConstraints: [constraint, constraint]}}
        (
            stmt_ddl(DdlStmt::AlterTable(Box::new(AlterTableStmt {
                name: Vec::new(),
                actions: vec![
                    AlterTableAction::AddIndexConstraint(key_constraint()),
                    AlterTableAction::AddIndexConstraint(key_constraint()),
                ],
            }))),
            0,
        ),
        // {&AlterTableSpec{NewConstraints:[c], NewColumns:[def]}}
        (
            stmt_ddl(DdlStmt::AlterTable(Box::new(AlterTableStmt {
                name: Vec::new(),
                actions: vec![
                    AlterTableAction::AddIndexConstraint(key_constraint()),
                    AlterTableAction::AddColumns {
                        if_not_exists: false,
                        columns: vec![plain_column("n")],
                        constraints: Vec::new(),
                    },
                ],
            }))),
            0,
        ),
    ];

    for (target, expected) in rows {
        let mut visitor = CheckVisitor::default();
        match target {
            Target::Statement(mut statement) => {
                assert!(crate::Visitable::accept(&mut statement, &mut visitor));
            }
            Target::Option(mut value) => {
                assert!(crate::Visitable::accept(&mut value, &mut visitor));
            }
            Target::Position(mut value) => {
                assert!(crate::Visitable::accept(&mut value, &mut visitor));
            }
            Target::Constraint(mut value) => {
                assert!(crate::Visitable::accept(&mut value, &mut visitor));
            }
            Target::Part(mut value) => {
                assert!(crate::Visitable::accept(&mut value, &mut visitor));
            }
        }
        assert_eq!(visitor.enter_count, expected);
        assert_eq!(visitor.leave_count, expected);
    }
}
