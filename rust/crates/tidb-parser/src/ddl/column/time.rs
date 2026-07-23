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

//! DDL-only time/default column-option grammar.
//!
//! This leaf directly owns Go's `normalizeDDLFuncName` and
//! `HandParser.parseNowSymOptionFraction` from `ddl_table_parser.go`.  It is
//! deliberately not a general expression parser: DEFAULT has its own
//! parenthesis-unwrapping and alias normalization contract, while ON UPDATE
//! accepts only the narrow NOW/CURDATE family production.

use tidb_ast::Expr;
use tidb_lexer::TokenKind;

use crate::{prec, PResult, Parser};

impl Parser {
    /// Parses and canonicalizes a column `DEFAULT` expression according to
    /// Go's `parseColumnOptions` DDL-only contract.
    ///
    /// This is intentionally distinct from a general expression slot and
    /// from `ALTER ... SET DEFAULT`: Go strips only the column-default
    /// expression's redundant outer parentheses and normalizes its time
    /// aliases before the common `ast.ColumnOption.Restore` formatting rule
    /// chooses whether to add parentheses back. In particular, the bare
    /// `LOCALTIME`/`LOCALTIMESTAMP` grammar forms are valid DDL defaults even
    /// though they are reserved words outside this narrow context.
    pub(super) fn parse_column_default_expression(&mut self) -> PResult<Expr> {
        let expression = if (self.is_kw("LOCALTIME") || self.is_kw("LOCALTIMESTAMP"))
            && !self.is_op_at(1, "(")
        {
            Expr::Func {
                name: self.bump().text,
                args: Vec::new(),
                origin_position: 0,
            }
        } else {
            // Go's column DEFAULT grammar deliberately parses one prefix
            // expression, not a full infix expression. This leaves the next
            // `NOT NULL`/other column option for the option loop.
            self.parse_prefix(prec::NONE)?
        };

        // `parseColumnOptions` rejects a bare identifier directly following
        // DEFAULT. Preserve Go's exact timing: parenthesized identifiers
        // have already taken its parenthesized-expression grammar path and
        // are not reclassified here.
        if matches!(expression, Expr::Column(_)) {
            return Err(self.err_here("invalid default value"));
        }

        Ok(normalize_column_default_expression(expression))
    }

    /// Direct port of Go `parseNowSymOptionFraction`, the deliberately narrow
    /// grammar accepted after a column's `ON UPDATE`. It is not a generic
    /// expression slot: only the NOW and CURDATE families are legal, and Go
    /// normalizes their aliases before AST restore.
    pub(super) fn parse_on_update_expr(&mut self) -> PResult<Expr> {
        let (name, requires_parens, accepts_fraction) = if self.is_kw("CURRENT_TIMESTAMP")
            || self.is_kw("LOCALTIME")
            || self.is_kw("LOCALTIMESTAMP")
        {
            ("CURRENT_TIMESTAMP", false, true)
        } else if self.is_kw("NOW") {
            ("CURRENT_TIMESTAMP", true, true)
        } else if self.is_kw("CURRENT_DATE") {
            ("CURRENT_DATE", false, false)
        } else if self.is_kw("CURDATE") {
            ("CURRENT_DATE", true, false)
        } else {
            return Err(
                self.err_here("expected a NOW-family or CURDATE-family ON UPDATE expression")
            );
        };
        self.bump();

        let mut args = Vec::new();
        if self.is_op("(") {
            self.bump();
            if accepts_fraction && self.peek().kind == TokenKind::IntLit {
                args.push(Expr::Int(self.bump().text));
            }
            self.expect_op(")")?;
        } else if requires_parens {
            return Err(self.err_here("expected '(' after ON UPDATE time function"));
        }
        Ok(Expr::Func {
            name: name.to_owned(),
            args,
            origin_position: 0,
        })
    }
}

/// Canonicalizes the AST payload Go leaves in a `ColumnOptionDefaultValue`.
///
/// `parseColumnOptions` strips all redundant outer parenthesized-expression
/// nodes and then normalizes the DDL-only time aliases. Keeping this as a
/// small generic AST transformation means CREATE TABLE and all ALTER actions
/// that reuse `parse_column_def` get identical behavior.
fn normalize_column_default_expression(mut expression: Expr) -> Expr {
    while let Expr::Paren(inner) = expression {
        expression = *inner;
    }
    if let Expr::Func { name, .. } = &mut expression {
        match name.to_ascii_uppercase().as_str() {
            "NOW" | "LOCALTIME" | "LOCALTIMESTAMP" => *name = "CURRENT_TIMESTAMP".to_string(),
            "CURDATE" => *name = "CURRENT_DATE".to_string(),
            "CURTIME" => *name = "CURRENT_TIME".to_string(),
            _ => {}
        }
    }
    expression
}
