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

//! SQL-binding grammar translated from `pkg/parser/binding_parser.go` and its
//! CREATE/DROP/SET/SHOW dispatch branches.

use tidb_ast::{
    BindingScope, BindingStatementTarget, BindingStatus, BindingValue, CreateBindingSource,
    CreateBindingStmt, DmlStmt, DropBindingStmt, DropBindingTarget, SetBindingStmt,
    SetBindingTarget, ShowBindingsFilter, ShowBindingsStmt, Stmt,
};
use tidb_lexer::TokenKind;

use crate::{decode_at_name, decode_string, prec, PResult, Parser};

impl Parser {
    /// Consumes the optional scope between a binding command and `BINDING`.
    /// Go treats an omitted scope as SESSION and makes it explicit on restore.
    fn parse_binding_scope(&mut self) -> BindingScope {
        if self.is_kw("GLOBAL") {
            self.bump();
            BindingScope::Global
        } else {
            if self.is_kw("SESSION") {
                self.bump();
            }
            BindingScope::Session
        }
    }

    /// Parses a digest list which, in Go, accepts only a string literal or a
    /// user variable. This must not reuse expression parsing: accepting a
    /// computed expression would add a binding grammar form TiDB does not
    /// support.
    fn parse_binding_values(&mut self) -> PResult<Vec<BindingValue>> {
        let mut values = vec![self.parse_binding_value()?];
        while self.is_op(",") {
            self.bump();
            values.push(self.parse_binding_value()?);
        }
        Ok(values)
    }

    fn parse_binding_value(&mut self) -> PResult<BindingValue> {
        let token = self.peek().clone();
        match token.kind {
            TokenKind::Str => {
                self.bump();
                Ok(BindingValue::String(decode_string(&token.text)))
            }
            TokenKind::UserVar => {
                self.bump();
                Ok(BindingValue::UserVar(decode_at_name(&token.text)))
            }
            _ => Err(self.err_here("expected a string or @variable binding digest")),
        }
    }

    /// Parses the statement payload owned by a SQL binding. TiDB's binding
    /// preprocessor admits only SELECT/set-operation, INSERT/REPLACE,
    /// UPDATE, and DELETE targets (`pkg/planner/core/preprocess.go`'s
    /// `bindableStmtType`). Keep the same typed boundary here: otherwise a
    /// newly added top-level command can accidentally become valid inside a
    /// binding merely because the generic statement parser learnt it.
    fn parse_binding_statement(&mut self) -> PResult<Box<Stmt>> {
        let statement = self.parse_statement()?;
        match &statement {
            Stmt::Query(_) => Ok(Box::new(statement)),
            Stmt::Dml(dml)
                if matches!(
                    dml.as_ref(),
                    DmlStmt::Insert(_) | DmlStmt::Update(_) | DmlStmt::Delete(_)
                ) =>
            {
                Ok(Box::new(statement))
            }
            // Go attaches a leading CTE directly to UpdateStmt/DeleteStmt
            // (`pkg/parser/ast/dml.go`), so those statements remain bindable
            // under `bindableStmtType`.  The Rust AST keeps the same ownership
            // explicit as a DmlStmt::With envelope; only admit envelopes whose
            // inner statement is one of those bindable DML families.  Do not
            // turn arbitrary WITH-prefixed commands into binding payloads.
            Stmt::Dml(dml)
                if matches!(
                    dml.as_ref(),
                    DmlStmt::With {
                        statement,
                        ..
                    } if matches!(
                        statement.as_ref(),
                        DmlStmt::Insert(_) | DmlStmt::Update(_) | DmlStmt::Delete(_)
                    )
                ) =>
            {
                Ok(Box::new(statement))
            }
            _ => Err(self.err_here("unsupported SQL binding statement")),
        }
    }

    /// Direct translation of Go's `parseCreateBindingStmt` plus CREATE's
    /// scope dispatch. Nested SQL uses the ordinary statement parser—never a
    /// raw-text compatibility path—so unsupported inner statements remain
    /// unsupported.
    pub(crate) fn parse_create_binding(&mut self) -> PResult<CreateBindingStmt> {
        self.expect_kw("CREATE")?;
        let scope = self.parse_binding_scope();
        self.expect_kw("BINDING")?;

        let source = if self.is_kw("FOR") {
            self.bump();
            let origin = self.parse_binding_statement()?;
            self.expect_kw("USING")?;
            let hinted = self.parse_binding_statement()?;
            CreateBindingSource::Statement {
                target: BindingStatementTarget {
                    origin,
                    hinted: Some(hinted),
                },
            }
        } else if self.is_kw("FROM") {
            self.bump();
            self.expect_kw("HISTORY")?;
            self.expect_kw("USING")?;
            // `PLAN` and `DIGEST` are optional keywords in Go's hand parser.
            if self.is_kw("PLAN") {
                self.bump();
            }
            if self.is_kw("DIGEST") {
                self.bump();
            }
            CreateBindingSource::History {
                plan_digests: self.parse_binding_values()?,
            }
        } else if self.is_kw("USING") {
            self.bump();
            let hinted = self.parse_binding_statement()?;
            // Go's wildcard form stores the same statement as both origin and
            // hint, then restores the canonical explicit `FOR ... USING ...`.
            CreateBindingSource::Statement {
                target: BindingStatementTarget {
                    origin: hinted.clone(),
                    hinted: Some(hinted),
                },
            }
        } else {
            return Err(self.err_here("expected FOR, FROM, or USING after BINDING"));
        };

        Ok(CreateBindingStmt { scope, source })
    }

    /// Direct translation of Go's `parseDropBindingStmt` plus DROP's scope
    /// dispatch.
    pub(crate) fn parse_drop_binding(&mut self) -> PResult<DropBindingStmt> {
        self.expect_kw("DROP")?;
        let scope = self.parse_binding_scope();
        self.expect_kw("BINDING")?;
        self.expect_kw("FOR")?;

        let target = if self.is_kw("SQL") && self.is_kw_at(1, "DIGEST") {
            self.bump();
            self.bump();
            DropBindingTarget::SqlDigests(self.parse_binding_values()?)
        } else {
            let origin = self.parse_binding_statement()?;
            let hinted = if self.is_kw("USING") {
                self.bump();
                Some(self.parse_binding_statement()?)
            } else {
                None
            };
            DropBindingTarget::Statement(BindingStatementTarget { origin, hinted })
        };

        Ok(DropBindingStmt { scope, target })
    }

    /// Direct translation of Go's `parseSetBindingStmt` branch.
    pub(crate) fn parse_set_binding(&mut self) -> PResult<SetBindingStmt> {
        self.expect_kw("SET")?;
        self.expect_kw("BINDING")?;
        let status = if self.is_kw("ENABLED") {
            self.bump();
            BindingStatus::Enabled
        } else if self.is_kw("DISABLED") {
            self.bump();
            BindingStatus::Disabled
        } else {
            return Err(self.err_here("expected ENABLED or DISABLED after SET BINDING"));
        };
        self.expect_kw("FOR")?;

        let target = if self.is_kw("SQL") && self.is_kw_at(1, "DIGEST") {
            self.bump();
            self.bump();
            let token = self.peek().clone();
            if token.kind != TokenKind::Str {
                return Err(self.err_here("expected a string SQL digest"));
            }
            self.bump();
            SetBindingTarget::SqlDigest(decode_string(&token.text))
        } else {
            let origin = self.parse_binding_statement()?;
            let hinted = if self.is_kw("USING") {
                self.bump();
                Some(self.parse_binding_statement()?)
            } else {
                None
            };
            SetBindingTarget::Statement(BindingStatementTarget { origin, hinted })
        };

        Ok(SetBindingStmt { status, target })
    }

    /// Direct translation of the `BINDINGS` arm in Go's
    /// `parseShowScopedStmt`, including its shared LIKE/WHERE grammar.
    pub(crate) fn parse_show_bindings(&mut self) -> PResult<ShowBindingsStmt> {
        self.expect_kw("SHOW")?;
        let scope = self.parse_binding_scope();
        self.expect_kw("BINDINGS")?;
        let filter = if self.is_kw("LIKE") {
            self.bump();
            Some(ShowBindingsFilter::Like(self.parse_expr(prec::UNARY)?))
        } else if self.is_kw("WHERE") {
            self.bump();
            Some(ShowBindingsFilter::Where(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        Ok(ShowBindingsStmt { scope, filter })
    }
}
