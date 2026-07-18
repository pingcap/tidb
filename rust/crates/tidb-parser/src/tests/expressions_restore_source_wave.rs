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

//! Source-owned restore/error vectors for the expression AST leaves covered by
//! `pkg/parser/ast/expressions_test.go`'s `Test*ExprRestore` helpers.

use super::*;

/// Mirrors Go's `TestUnaryOperationExprRestore` (expressions_test.go:103).
/// The helper extracts the expression node from `SELECT`, while `r` below
/// checks the same canonical expression text in its statement envelope.
#[test]
fn unary_operation_expr_restore_source_vectors() {
    for (source, expected) in [
        ("++1", "++1"),
        ("--1", "--1"),
        ("-+1", "-+1"),
        ("-1", "-1"),
        ("not true", "NOT TRUE"),
        ("~3", "~3"),
        ("!true", "!TRUE"),
    ] {
        assert_eq!(
            r(&format!("select {source}")),
            format!("SELECT {expected}"),
            "source SQL: {source}"
        );
    }
}

/// Mirrors Go's `TestColumnNameExprRestore` (expressions_test.go:119).
/// Each path component is quoted independently, and an embedded backtick is
/// doubled exactly as `ast.ColumnName.Restore` does.
#[test]
fn column_name_expr_restore_source_vectors() {
    for (source, expected) in [
        ("abc", "`abc`"),
        ("`abc`", "`abc`"),
        ("`ab``c`", "`ab``c`"),
        ("sabc.tABC", "`sabc`.`tABC`"),
        ("dabc.sabc.tabc", "`dabc`.`sabc`.`tabc`"),
        ("dabc.`sabc`.tabc", "`dabc`.`sabc`.`tabc`"),
        ("`dABC`.`sabc`.tabc", "`dABC`.`sabc`.`tabc`"),
    ] {
        assert_eq!(
            r(&format!("select {source}")),
            format!("SELECT {expected}"),
            "source SQL: {source}"
        );
    }
}

/// Mirrors Go's `TestIsNullExprRestore` (expressions_test.go:135).
#[test]
fn is_null_expr_restore_source_vectors() {
    for (source, expected) in [
        ("a is null", "`a` IS NULL"),
        ("a is not null", "`a` IS NOT NULL"),
    ] {
        assert_eq!(
            r(&format!("select {source}")),
            format!("SELECT {expected}"),
            "source SQL: {source}"
        );
    }
}

/// Keep the parse-error boundary beside the restore vectors. These are the
/// incomplete operator/predicate forms rejected by Go's expression parser;
/// accepting them would turn a malformed source row into a different AST.
#[test]
fn expression_restore_source_error_vectors() {
    for source in [
        "select +",
        "select not",
        "select ~",
        "select !",
        "select a is",
        "select a is not",
        "select a is maybe",
        "select .a",
        "select `a",
        "select abc.def.ghi.jkl",
    ] {
        assert!(parse(source).is_err(), "Go rejects source SQL: {source}");
    }
}
