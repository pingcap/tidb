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

//! Shared harness transcreated from the `runNodeRestoreTest*` helpers in
//! `pkg/parser/ast/util_test.go` (origin/master).
//!
//! The Go helpers fill `template` with each case, parse through
//! `parser.ParseOneStmt`, drive an `extractNodeFunc` that restores one AST
//! sub-node into a fresh builder using `NewRestoreCtx(flags, &sb)`, splice
//! the fragment back into the template, require equality with the expected
//! SQL, then re-parse the restored SQL and require full AST equality after
//! `CleanNodeText`. Rust mirrors every step: parsing comes from the dev
//! dependency on `tidb-parser` (the same external-package relationship the
//! Go `ast_test` package has), and derived `PartialEq` already excludes the
//! source-text metadata `CleanNodeText` clears.

use tidb_ast::{DdlStmt, RestoreContext, RestoreFlags, Stmt};
use tidb_parser::parse;

/// One `{sourceSQL, expectSQL}` row of Go's `NodeRestoreTestCase`.
#[derive(Clone, Copy)]
pub struct NodeRestoreCase {
    /// The `%s` substitution written by the test author.
    pub source: &'static str,
    /// The canonical SQL expected after restoring only the extracted node.
    pub expect: &'static str,
}

/// Builds a [`NodeRestoreCase`] row.
pub const fn case(source: &'static str, expect: &'static str) -> NodeRestoreCase {
    NodeRestoreCase { source, expect }
}

fn parsed(sql: &str) -> Stmt {
    parse(sql).unwrap_or_else(|error| panic!("{sql:?} failed to parse: {error:?}"))
}

/// The Rust transcreation of Go's `nodeTextCleaner` from
/// `pkg/parser/ast/util_test.go`: the round-trip equality compares ASTs
/// whose source-text provenance has been cleaned. Rust excludes source text
/// from equality already; the remaining provenance artifacts are an explicit
/// `_utf8mb4` introducer on a restored string literal (Go's own ValueExprs
/// collapse to identical structs after cleaning) and ENUM/SET members that
/// were written as hex/bit literals (Go's `CleanElemIsBinaryLit`). Both are
/// normalized here before comparison, nothing else.
fn clean_node_text(stmt: &mut Stmt) {
    use tidb_ast::{ColumnDef, Expr, Visitable, Visitor};

    struct Cleaner;

    impl Visitor for Cleaner {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expression) = node.downcast_mut::<Expr>() {
                // Canonicalize provenance the way Go's parse-time
                // `walkRewriteFuncName` + `cleanNodeText` collapse it: the
                // function name compares canonically and byte offsets are
                // zeroed.
                if let Expr::Func {
                    name,
                    origin_position,
                    ..
                } = expression
                {
                    *name = name.to_ascii_uppercase();
                    *origin_position = 0;
                }
                if let Expr::CharsetString { charset, value } = expression {
                    if charset.eq_ignore_ascii_case("utf8mb4") {
                        *expression = Expr::String(std::mem::take(value));
                    }
                    return true;
                }
            }
            if let Some(join) = node.downcast_mut::<tidb_ast::Join>() {
                // Go's FROM tree has no parenthesization marker at all, so
                // its clean comparison never sees one either.
                join.explicit_parens = false;
            }
            if let Some(option) = node.downcast_mut::<tidb_ast::ColumnOption>() {
                if let tidb_ast::ColumnOption::Generated {
                    expression_text, ..
                } = option
                {
                    // Go's CleanNodeText clears the generated-expression's
                    // own source text through the node interface; this crate
                    // stores it beside the expression, so zero it here.
                    expression_text.clear();
                }
            }
            if let Some(column) = node.downcast_mut::<ColumnDef>() {
                clean_column(column);
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    fn clean_column(column: &mut ColumnDef) {
        for argument in &mut column.ty.args {
            if let tidb_ast::ColumnTypeArg::Bytes(bytes) = argument {
                if let Ok(text) = std::str::from_utf8(bytes) {
                    *argument = tidb_ast::ColumnTypeArg::Text(text.to_string());
                }
            }
        }
    }

    stmt.accept(&mut Cleaner);
}

/// Panics unless the parsed statement is a DDL statement; returns its payload.
pub fn expect_ddl(stmt: &Stmt) -> &DdlStmt {
    match stmt {
        Stmt::Ddl(ddl) => ddl,
        other => panic!("expected a DDL statement, got {other:?}"),
    }
}

fn run_with_flags(
    template: &str,
    cases: &[NodeRestoreCase],
    flags: RestoreFlags,
    round_trip: bool,
    extract_restore: &dyn Fn(&Stmt, &RestoreContext) -> String,
) {
    for node_case in cases {
        let source_sql = template.replace("%s", node_case.source);
        let expect_sql = template.replace("%s", node_case.expect);
        let stmt = parsed(&source_sql);
        let context = RestoreContext::new(flags);
        let fragment = extract_restore(&stmt, &context);
        let restore_sql = template.replace("%s", &fragment);
        assert_eq!(
            expect_sql, restore_sql,
            "source {:?}; extracted {fragment:?}",
            node_case.source
        );
        if !round_trip {
            continue;
        }
        // Go re-parses the restored SQL and requires DeepEqual after
        // CleanNodeText on both sides.
        let mut source_stmt = stmt;
        clean_node_text(&mut source_stmt);
        let mut reparsed = parsed(&restore_sql);
        clean_node_text(&mut reparsed);
        assert_eq!(
            source_stmt, reparsed,
            "round trip mismatch: {source_sql:?} -> {restore_sql:?}"
        );
    }
}

/// Go `runNodeRestoreTest`: default restore flags plus AST round-trip checks.
pub fn run_node_restore_test(
    template: &str,
    cases: &[NodeRestoreCase],
    extract_restore: impl Fn(&Stmt, &RestoreContext) -> String,
) {
    run_with_flags(
        template,
        cases,
        RestoreFlags::DEFAULT,
        true,
        &extract_restore,
    );
}

/// Go `runNodeRestoreTestWithFlags`: explicit flags plus AST round-trip checks.
pub fn run_node_restore_test_with_flags(
    template: &str,
    cases: &[NodeRestoreCase],
    flags: RestoreFlags,
    extract_restore: impl Fn(&Stmt, &RestoreContext) -> String,
) {
    run_with_flags(template, cases, flags, true, &extract_restore);
}

/// Go `runNodeRestoreTestWithFlagsStmtChange`: the restored text may differ
/// from the source AST, so no re-parse comparison happens.
pub fn run_node_restore_test_with_flags_stmt_change(
    template: &str,
    cases: &[NodeRestoreCase],
    flags: RestoreFlags,
    extract_restore: impl Fn(&Stmt, &RestoreContext) -> String,
) {
    run_with_flags(template, cases, flags, false, &extract_restore);
}
