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

//! Port of `pkg/parser/ast/dml_test.go::TestDMLVisitorCover` (origin/master).
//!
//! See `tests_ddl_package_source` for the marker-expression translation of
//! Go's `checkExpr`.

#![cfg(test)]

use crate::{
    Assignment, DeleteKind, DeleteStmt, DmlStmt, Expr, ImportIntoStmt, ImportSource, JoinNode,
    Limit, LoadDataOnDuplicate, LoadDataStmt, NodeBox, OrderItem, QueryStmt, SelectField,
    SetOprStmt, Stmt, TableRef, UpdateKind, UpdateStmt,
};

fn check_expr() -> Expr {
    Expr::Column(vec!["__check".to_string()])
}

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

/// A row target: a whole statement or one of the bare payload nodes Go's
/// table drives directly.
enum Target {
    Statement(Stmt),
    Assignment(Assignment),
    Item(OrderItem),
    Join(Box<crate::Join>),
}

fn stmt_dml(dml: DmlStmt) -> Target {
    Target::Statement(Stmt::Dml(NodeBox::new(dml)))
}

fn plain_table() -> TableRef {
    TableRef {
        identity: Default::default(),
        name: vec!["t".to_string()],
        partitions: Vec::new(),
        alias: None,
        as_of: None,
        hints: Vec::new(),
        sample: None,
    }
}

fn table_node() -> JoinNode {
    JoinNode::Table(plain_table())
}

fn join_with_on(on: Option<Expr>) -> Box<crate::Join> {
    Box::new(crate::Join {
        left: table_node(),
        right: None,
        tp: crate::JoinType::Cross,
        straight: false,
        on,
        using: Vec::new(),
        natural: false,
        explicit_parens: false,
    })
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

fn query(select: crate::SelectStmt) -> Target {
    Target::Statement(Stmt::Query(NodeBox::new(QueryStmt::Select(Box::new(
        select,
    )))))
}

/// Runs one row through the counting visitor and requires balanced counts.
fn check_row(name: &str, mut target: Target, expected: usize) {
    let mut visitor = CheckVisitor::default();
    match &mut target {
        Target::Statement(statement) => {
            assert!(crate::Visitable::accept(statement, &mut visitor));
        }
        Target::Assignment(value) => {
            assert!(crate::Visitable::accept(value, &mut visitor));
        }
        Target::Item(value) => {
            assert!(crate::Visitable::accept(value, &mut visitor));
        }
        Target::Join(value) => {
            assert!(crate::Visitable::accept(value.as_mut(), &mut visitor));
        }
    }
    assert_eq!(visitor.enter_count, expected, "{name}");
    assert_eq!(visitor.leave_count, expected, "{name}");
}

/// `pkg/parser/ast/dml_test.go::TestDMLVisitorCover`.
///
/// Each row mirrors one Go row's reachable-`checkExpr` count; statement
/// wrappers also replay the generic traversal, matching the trailing
/// `Accept(visitor1{})` in Go.
#[test]
fn dml_visitor_cover() {
    let ce = check_expr();

    // {&DeleteStmt{TableRefs(ON=ce), Tables, Where: ce, Limit{ce, ce}}} → 4.
    let delete = DeleteStmt {
        hints: Vec::new(),
        priority: Default::default(),
        quick: false,
        ignore: false,
        kind: DeleteKind::Multi {
            targets: vec![vec!["t1".to_string()]],
            using: false,
            from: join_with_on(Some(ce.clone())),
        },
        where_clause: Some(ce.clone()),
        order_by: Vec::new(),
        limit: Some(Limit {
            offset: Some(ce.clone()),
            count: ce.clone(),
        }),
        returning: Default::default(),
    };
    check_row("delete", stmt_dml(DmlStmt::Delete(Box::new(delete))), 4);

    // {&ShowStmt{...}} — Rust owns no generic show node family with embedded
    // expressions; an admin SHOW carries none either way (Go expects 3
    // placeholder visits that never reach its sentinel).
    let show = crate::AdminStmt::ShowTables(Box::new(crate::ShowTablesStmt {
        full: false,
        database: None,
        filter: None,
    }));
    check_row(
        "show",
        Target::Statement(Stmt::Admin(NodeBox::new(show))),
        0,
    );

    // {&LoadDataStmt{Table, Columns, FieldsInfo, LinesInfo}}
    let load = LoadDataStmt {
        low_priority: false,
        local: false,
        path: String::new(),
        format: None,
        on_duplicate: LoadDataOnDuplicate::Error,
        table: vec!["t".to_string()],
        charset: None,
        fields: Default::default(),
        lines: Default::default(),
        ignore_lines: None,
        columns_and_user_vars: Vec::new(),
        column_assignments: Vec::new(),
        options: Vec::new(),
    };
    check_row("load data", stmt_dml(DmlStmt::LoadData(Box::new(load))), 0);

    // {&ImportIntoStmt{Table}}
    let import = ImportIntoStmt {
        table: vec!["t".to_string()],
        columns_and_user_vars: Vec::new(),
        column_assignments: Vec::new(),
        source: ImportSource::File {
            path: String::new(),
            format: None,
        },
        options: Vec::new(),
    };
    check_row(
        "import into",
        stmt_dml(DmlStmt::ImportInto(Box::new(import))),
        0,
    );

    // {&Assignment{Column, Expr: ce}}
    check_row(
        "assignment",
        Target::Assignment(Assignment {
            col: vec!["c".to_string()],
            value: ce.clone(),
        }),
        1,
    );

    // {&ByItem{Expr: ce}}
    check_row(
        "by item",
        Target::Item(OrderItem {
            expr: ce.clone(),
            desc: false,
        }),
        1,
    );

    // {&GroupByClause{Items:[ce, ce]}} — clause children live on SELECT.
    let mut grouped = empty_select();
    grouped.group_by = vec![
        crate::GroupByItem {
            expr: ce.clone(),
            desc: None,
        },
        crate::GroupByItem {
            expr: ce.clone(),
            desc: None,
        },
    ];
    check_row("group by", query(grouped), 2);

    // {&HavingClause{Expr: ce}}
    let mut having = empty_select();
    having.having = Some(ce.clone());
    check_row("having", query(having), 1);

    // {&Join{Left: &TableSource{Source: &TableName{}}}}
    check_row("join left only", Target::Join(join_with_on(None)), 0);

    // {&Limit{Count: ce, Offset: ce}}
    let limited = empty_select();
    let mut limited = query(limited);
    if let Target::Statement(Stmt::Query(boxed)) = &mut limited {
        if let QueryStmt::Select(select) = &mut **boxed {
            select.limit = Some(Limit {
                offset: Some(ce.clone()),
                count: ce.clone(),
            });
        }
    }
    check_row("limit", limited, 2);

    // {&OnCondition{Expr: ce}} — ON clauses live on the join node.
    check_row(
        "on condition",
        Target::Join(join_with_on(Some(ce.clone()))),
        1,
    );

    // {&OrderByClause{Items:[ce, ce]}}
    let mut ordered = empty_select();
    ordered.order_by = vec![
        OrderItem {
            expr: ce.clone(),
            desc: false,
        },
        OrderItem {
            expr: ce.clone(),
            desc: false,
        },
    ];
    check_row("order by", query(ordered), 2);

    // {&SelectField{Expr: ce, WildCard}} — one projected expression field.
    let mut fielded = empty_select();
    fielded.fields.push(SelectField::Expr {
        expr: ce.clone(),
        alias: None,
    });
    check_row("select field", query(fielded), 1);

    // {&TableName{}}, {&TableSource{}}, {&WildCardField{}} — zero-value rows.
    let wildcard_fielded = empty_select();
    let mut wildcard_fielded = query(wildcard_fielded);
    if let Target::Statement(Stmt::Query(boxed)) = &mut wildcard_fielded {
        if let QueryStmt::Select(select) = &mut **boxed {
            select.fields.push(SelectField::Wildcard(Vec::new()));
            select.from = Some(*join_with_on(None));
        }
    }
    check_row("wildcard/table source", wildcard_fielded, 0);

    // {&InsertStmt{Table: tableRefsClause}} — nested ON=ce stays reachable.
    let inserted = crate::InsertStmt {
        hints: Vec::new(),
        priority: Default::default(),
        ignore: false,
        table: vec!["t".to_string()],
        partitions: Vec::new(),
        columns: Vec::new(),
        columns_specified: false,
        set_columns: Vec::new(),
        rows: Vec::new(),
        source: None,
        source_parenthesized: false,
        on_duplicate: Vec::new(),
        row_alias: None,
        column_aliases: Vec::new(),
        returning: Default::default(),
        set_syntax: false,
        replace: false,
    };
    let _ = inserted;
    let insert_values = empty_select();
    let mut insert_values = query(insert_values);
    if let Target::Statement(Stmt::Query(boxed)) = &mut insert_values {
        if let QueryStmt::Select(select) = &mut **boxed {
            select.values = vec![vec![ce.clone()]];
        }
    }
    check_row("insert values", insert_values, 1);

    // {&SetOprStmt{}}, {&SetOprSelectList{}}
    let set_opr = SetOprStmt {
        with: None,
        is_in_braces: false,
        terms: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        lock: None,
        outer_order_by: Vec::new(),
        outer_limit: None,
        outer_lock: None,
    };
    check_row(
        "set operation",
        Target::Statement(Stmt::Query(NodeBox::new(QueryStmt::SetOpr(Box::new(
            set_opr,
        ))))),
        0,
    );

    // {&UpdateStmt{TableRefs}} with the embedded ON=ce reachable.
    let update = UpdateStmt {
        hints: Vec::new(),
        priority: Default::default(),
        ignore: false,
        kind: UpdateKind::Multi {
            from: join_with_on(Some(check_expr())),
            comma_join: false,
        },
        assignments: Vec::new(),
        where_clause: None,
        order_by: Vec::new(),
        limit: None,
        returning: Default::default(),
    };
    check_row("update", stmt_dml(DmlStmt::Update(Box::new(update))), 1);

    // {&SelectStmt{}}, {&FieldList{}}
    check_row("empty select", query(empty_select()), 0);

    // {&WindowSpec{}}, {&PartitionByClause{}}, {&FrameClause{}},
    // {&FrameBound{}} — zero-expression window payloads traverse untouched.
    let mut windows = empty_select();
    windows.windows = vec![(
        "w".to_string(),
        crate::WindowDef {
            base: None,
            spec: crate::WindowSpec {
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: Some(crate::WindowFrame {
                    kind: crate::FrameKind::Rows,
                    start: crate::FrameBound::UnboundedPreceding,
                    end: crate::FrameBound::CurrentRow,
                }),
            },
        },
    )];
    check_row("window spec", query(windows), 0);
}
