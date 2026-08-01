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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The `IN` / `BETWEEN` / `LIKE` / `REGEXP` / `IS` predicates, mirroring Go's
//! `PredicateExpr` production family in `pkg/parser/parser.y`.

use super::*;

impl Parser {
    // ---- predicates ----

    pub(crate) fn parse_predicate(&mut self, left: Expr, not: bool) -> PResult<Expr> {
        if self.is_kw("IN") {
            self.bump();
            // `IN (SELECT ...)` / `IN (WITH ... SELECT ...)` is a
            // subquery; otherwise a value list — see the `"(" =>` prefix
            // arm's own doc (this crate's other widened lookahead) for
            // why only this position and that one need the extra `WITH`
            // check. Unlike scalar and `ANY`/`ALL` positions, this one
            // parses via `parse_select_or_setopr` directly instead —
            // confirmed via `godump restore` that `x IN (SELECT ...
            // UNION SELECT ...)` is real, additive grammar. `EXISTS` has
            // its own `parse_query_subquery` path for the same set-operation
            // shape; scalar and `ANY`/`ALL` remain `SelectStmt`-only here.
            if self.is_op("(") && self.is_query_start_at(1) {
                self.bump(); // (
                let start = self.peek().offset;
                let subquery = if self.is_kw("WITH") {
                    self.parse_with_select()?
                } else {
                    self.parse_select_or_setopr()?
                };
                let end = self.peek().offset;
                let mut subquery = tidb_ast::NodeBox::new(subquery);
                if end > start {
                    subquery.set_text(None, self.source.as_bytes()[start..end].to_vec());
                }
                self.expect_op(")")?;
                return Ok(Expr::InSubquery {
                    expr: Box::new(left),
                    subquery,
                    not,
                });
            }
            self.expect_op("(")?;
            let mut list = vec![self.parse_expr(prec::NONE)?];
            while self.is_op(",") {
                self.bump();
                list.push(self.parse_expr(prec::NONE)?);
            }
            self.expect_op(")")?;
            Ok(Expr::In {
                expr: Box::new(left),
                list,
                not,
            })
        } else if self.is_kw("LIKE") || self.is_kw("ILIKE") {
            let ilike = self.is_kw("ILIKE");
            self.bump();
            // The pattern is a `SimpleExpr`, NOT a `bit_expr` — the source
            // production is `BitExpr LikeOrNotOp SimpleExpr
            // LikeEscapeOpt`, and `pkg/parser/expr_parser.go`'s
            // `parseLikeExpr` renders that as `parseExpression(precUnary)`
            // with the note "precUnary excludes all binary arithmetic/
            // bitwise operators". So a binary operator after the pattern
            // applies to the WHOLE predicate: `'a' LIKE 'a' + 0` is
            // `('a' LIKE 'a') + 0`, not `'a' LIKE ('a' + 0)`.
            let pattern = self.parse_expr(prec::UNARY)?;
            let escape = self.parse_opt_escape_clause()?;
            Ok(Expr::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                not,
                ilike,
                escape,
            })
        } else if self.is_kw("REGEXP") || self.is_kw("RLIKE") {
            self.bump();
            // Same `SimpleExpr` pattern precedence as `LIKE` above:
            // `pkg/parser/expr_parser.go`'s `parseRegexpExpr` is
            // `parseExpression(precUnary)` under the note "yacc: BitExpr
            // RegexpOrNotOp SimpleExpr — pattern is SimpleExpr".
            let pattern = self.parse_expr(prec::UNARY)?;
            Ok(Expr::Regexp {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                not,
            })
        } else if self.is_kw("BETWEEN") {
            self.bump();
            let low = self.parse_expr(prec::PREDICATE + 1)?;
            self.expect_kw("AND")?;
            let high = self.parse_expr(prec::PREDICATE)?;
            Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                not,
            })
        } else {
            Err(self.err_here("expected IN / LIKE / REGEXP / BETWEEN after NOT"))
        }
    }

    /// Parses an OPTIONAL `ESCAPE 'char'` clause following `LIKE`'s own
    /// pattern operand — read directly from real TiDB's own
    /// `parseLikeExpr` (`pkg/parser/expr_parser.go`): the escape
    /// argument is a MANDATORY string literal of BYTE length 0 or 1
    /// (any other length is a genuine `ParseError`,
    /// `ErrWrongArguments`) — folds the "explicit but matches the
    /// default backslash" case into `None` directly, matching
    /// `tidb_ast::Expr::Like`'s own doc for why that's safe.
    fn parse_opt_escape_clause(&mut self) -> PResult<Option<u8>> {
        if !self.is_kw("ESCAPE") {
            return Ok(None);
        }
        self.bump();
        if self.peek().kind != TokenKind::Str {
            return Err(self.err_here("expected a string literal after ESCAPE"));
        }
        let decoded = self.bumped_string();
        match decoded.as_bytes() {
            [] => Ok(Some(0)),
            [b'\\'] => Ok(None),
            [b] => Ok(Some(*b)),
            _ => Err(self.err_here("[parser:1210]Incorrect arguments to ESCAPE")),
        }
    }

    pub(crate) fn parse_is(&mut self, left: Expr) -> PResult<Expr> {
        self.expect_kw("IS")?;
        let not = self.is_kw("NOT");
        if not {
            self.bump();
        }
        let target = if self.is_kw("NULL") {
            self.bump();
            IsTarget::Null
        } else if self.is_kw("TRUE") {
            self.bump();
            IsTarget::True
        } else if self.is_kw("FALSE") {
            self.bump();
            IsTarget::False
        } else if self.is_kw("UNKNOWN") {
            // `IS [NOT] UNKNOWN` is the same node as `IS [NOT] NULL` in the Go
            // AST (IsNullExpr), and restores as NULL.
            self.bump();
            IsTarget::Null
        } else {
            return Err(self.err_here("expected NULL / TRUE / FALSE / UNKNOWN after IS"));
        };
        Ok(Expr::Is {
            expr: Box::new(left),
            target,
            not,
        })
    }
}
