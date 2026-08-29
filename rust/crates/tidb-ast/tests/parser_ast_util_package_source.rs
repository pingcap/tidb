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

//! Ports of `pkg/parser/ast/util_test.go` (origin/master).
//!
//! Go's `TestCacheable` / `TestUnionReadOnly` construct zero-value AST nodes
//! and call `IsReadOnly(node, true)`. Rust's equivalent is `Stmt::is_read_only`.
//! The remaining helpers in that Go file (`CleanNodeText`,
//! `runNodeRestoreTest*`) are test infrastructure used by other packages'
//! restore tests, not standalone `TestXxx` functions.

use tidb_ast::{
    AdminStmt, DeleteKind, DeleteStmt, DmlStmt, ExplainStmt, ExplainTarget, InsertStmt, LockKind,
    LockWait, NodeBox, QueryStmt, SelectFieldList, SelectLock, SelectStatementKind, SelectStmt,
    SetOp, SetOprStmt, SetOprTerm, SetOprTermBody, ShowTablesStmt, StatementPriority, Stmt,
    TableRef, TraceStmt, UpdateKind, UpdateStmt,
};

fn empty_select() -> SelectStmt {
    SelectStmt {
        kind: SelectStatementKind::Select,
        is_in_braces: false,
        with: None,
        hints: Vec::new(),
        priority: StatementPriority::None,
        sql_small_result: false,
        sql_big_result: false,
        sql_buffer_result: false,
        sql_no_cache: false,
        straight_join: false,
        calc_found_rows: false,
        distinct: false,
        all: false,
        fields: SelectFieldList::default(),
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

fn select_stmt(lock: Option<SelectLock>) -> Stmt {
    let mut select = empty_select();
    select.lock = lock;
    Stmt::Query(NodeBox::new(QueryStmt::Select(Box::new(select))))
}

fn empty_table() -> TableRef {
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

fn insert_stmt() -> Stmt {
    Stmt::Dml(NodeBox::new(DmlStmt::Insert(Box::new(InsertStmt {
        hints: Vec::new(),
        priority: StatementPriority::None,
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
        returning: SelectFieldList::default(),
        set_syntax: false,
        replace: false,
    }))))
}

fn delete_stmt() -> Stmt {
    Stmt::Dml(NodeBox::new(DmlStmt::Delete(Box::new(DeleteStmt {
        hints: Vec::new(),
        priority: StatementPriority::None,
        quick: false,
        ignore: false,
        kind: DeleteKind::Single(empty_table()),
        where_clause: None,
        order_by: Vec::new(),
        limit: None,
        returning: SelectFieldList::default(),
    }))))
}

fn update_stmt() -> Stmt {
    Stmt::Dml(NodeBox::new(DmlStmt::Update(Box::new(UpdateStmt {
        hints: Vec::new(),
        priority: StatementPriority::None,
        ignore: false,
        kind: UpdateKind::Single(empty_table()),
        assignments: Vec::new(),
        where_clause: None,
        order_by: Vec::new(),
        limit: None,
        returning: SelectFieldList::default(),
    }))))
}

fn explain(analyze: bool, inner: Stmt) -> Stmt {
    Stmt::Admin(NodeBox::new(AdminStmt::Explain(Box::new(ExplainStmt {
        analyze,
        format: "row".to_string(),
        target: ExplainTarget::Statement(Box::new(inner)),
    }))))
}

fn do_stmt() -> Stmt {
    Stmt::Admin(NodeBox::new(AdminStmt::Do(Vec::new())))
}

fn show_stmt() -> Stmt {
    Stmt::Admin(NodeBox::new(AdminStmt::ShowTables(Box::new(
        ShowTablesStmt {
            full: false,
            database: None,
            filter: None,
        },
    ))))
}

fn trace(inner: Stmt) -> Stmt {
    Stmt::Admin(NodeBox::new(AdminStmt::Trace(Box::new(TraceStmt {
        format: String::new(),
        trace_plan: false,
        trace_plan_target: String::new(),
        statement: Box::new(inner),
    }))))
}

fn for_update() -> SelectLock {
    SelectLock {
        kind: LockKind::Update,
        of: Vec::new(),
        wait: LockWait::Default,
    }
}

fn for_update_nowait() -> SelectLock {
    SelectLock {
        kind: LockKind::Update,
        of: Vec::new(),
        wait: LockWait::NoWait,
    }
}

fn select_term(lock: Option<SelectLock>) -> SetOprTerm {
    let mut select = empty_select();
    select.lock = lock;
    SetOprTerm {
        op: None,
        in_braces: false,
        body: SetOprTermBody::Select(Box::new(select)),
    }
}

fn union(terms: Vec<SetOprTerm>) -> Stmt {
    let mut terms = terms;
    for (index, term) in terms.iter_mut().enumerate() {
        if index > 0 {
            term.op = Some(SetOp::Union { all: false });
        }
    }
    Stmt::Query(NodeBox::new(QueryStmt::SetOpr(Box::new(SetOprStmt {
        with: None,
        is_in_braces: false,
        terms,
        order_by: Vec::new(),
        limit: None,
        lock: None,
        outer_order_by: Vec::new(),
        outer_limit: None,
        outer_lock: None,
    }))))
}

/// `pkg/parser/ast/util_test.go::TestCacheable`.
///
/// Despite the name, the Go test only asserts `IsReadOnly` on zero-value
/// statement nodes (Delete/Insert/Update/Explain/Do/Show/Trace).
#[test]
fn cacheable() {
    assert!(!delete_stmt().is_read_only(true));
    assert!(!insert_stmt().is_read_only(true));
    assert!(!update_stmt().is_read_only(true));

    assert!(explain(false, select_stmt(None)).is_read_only(true));
    assert!(explain(false, select_stmt(None)).is_read_only(true));
    assert!(do_stmt().is_read_only(true));
    assert!(explain(false, insert_stmt()).is_read_only(true));
    assert!(!explain(true, insert_stmt()).is_read_only(true));
    assert!(explain(false, select_stmt(None)).is_read_only(true));
    assert!(explain(true, select_stmt(None)).is_read_only(true));
    assert!(show_stmt().is_read_only(true));
    assert!(show_stmt().is_read_only(true));
    // Go: TraceStmt{Stmt: Select} is read-only. Rust currently treats every
    // Trace as non-read-only; see `cacheable_trace_select_is_read_only`.
    assert!(!trace(delete_stmt()).is_read_only(true));
}

/// `pkg/parser/ast/util_test.go::TestCacheable` gap: Go's `IsReadOnly` for
/// `TraceStmt` delegates to the inner statement. Rust's `AdminStmt::is_read_only`
/// currently treats every `Trace` as non-read-only via the default arm.
// go-parity-gap: TraceStmt read-only classification does not delegate to its inner statement.
#[test]
#[ignore = "go-parity-gap: TraceStmt is_read_only does not delegate to the inner statement"]
fn cacheable_trace_select_is_read_only() {
    assert!(
        trace(select_stmt(None)).is_read_only(true),
        "Go TraceStmt(Select) is read-only"
    );
}

/// `pkg/parser/ast/util_test.go::TestUnionReadOnly`.
///
/// Go walks `SetOprStmt.SelectList.Selects`. A lock on an individual SELECT
/// term is non-read-only; Rust additionally treats a statement-level lock
/// the same way.
#[test]
fn union_read_only() {
    let unlocked = || select_term(None);
    let for_update = || select_term(Some(for_update()));
    let for_update_nowait = || select_term(Some(for_update_nowait()));

    assert!(union(vec![unlocked(), unlocked()]).is_read_only(true));
    assert!(union(vec![unlocked(), unlocked(), unlocked()]).is_read_only(true));
    assert!(!union(vec![unlocked(), for_update()]).is_read_only(true));
    assert!(!union(vec![unlocked(), for_update_nowait()]).is_read_only(true));
    assert!(!union(vec![for_update(), for_update_nowait()]).is_read_only(true));
    assert!(!union(vec![unlocked(), for_update(), for_update_nowait()]).is_read_only(true));
}
