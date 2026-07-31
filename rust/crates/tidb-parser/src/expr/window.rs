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

//! Aggregates and window functions: `parse_aggregate` (which detects a
//! trailing `OVER` and dispatches to `Expr::Window`), the ranking/value/
//! distribution window families, the `OVER (...)` clause and its explicit
//! `ROWS`/`RANGE` frames, and `GROUP_CONCAT`.
//!
//! This mirrors Go's `SumExpr` and `WindowFuncCall` production families in
//! `pkg/parser/parser.y`.

use super::*;

impl Parser {
    /// Parses an aggregate call `NAME([DISTINCT|ALL] arg [, arg ...])`;
    /// `COUNT(*)` is modelled as the single-element literal `1` (matching
    /// the Go AST's `COUNT(1)` restore). A comma-separated arg list is
    /// syntactically accepted for any aggregate name here, then rejected
    /// (a `ParseError`) unless the name allows it — see
    /// [`tidb_ast::Expr::Aggregate`]'s own doc for the exact name-keyed
    /// rule, read directly from `pkg/parser/expr_func_parser.go`'s
    /// `parseAggregateFuncCall` rather than guessed. If `OVER` follows,
    /// this is instead a window AGGREGATE ([`Expr::Window`], sharing this
    /// same argument-parsing shape and preserving `DISTINCT`).
    pub(crate) fn parse_aggregate(&mut self) -> PResult<Expr> {
        let name = agg_canonical(&self.bump().text)
            .expect("aggregate name")
            .to_string();
        self.expect_op("(")?;
        if self.is_op("*") {
            self.bump();
            self.expect_op(")")?;
            let arg = Expr::Int("1".to_string());
            if self.is_kw("OVER") {
                self.bump();
                let over = self.parse_over_clause()?;
                return Ok(Expr::Window {
                    name,
                    args: vec![arg],
                    distinct: false,
                    ignore_nulls: false,
                    from_last: false,
                    over,
                });
            }
            return Ok(Expr::Aggregate {
                name,
                distinct: false,
                args: vec![arg],
            });
        }
        let distinct = if self.is_kw("DISTINCT") || self.is_kw("DISTINCTROW") {
            self.bump();
            true
        } else {
            if self.is_kw("ALL") {
                self.bump(); // ALL is the default
            }
            false
        };
        if distinct && self.is_kw("ALL") {
            if name == "COUNT" {
                return Err(self.err_here("COUNT does not accept DISTINCT ALL"));
            }
            self.bump();
        }
        if distinct
            && matches!(
                name.as_str(),
                "BIT_AND" | "BIT_OR" | "BIT_XOR" | "JSON_ARRAYAGG" | "JSON_OBJECTAGG"
            )
        {
            return Err(self.err_here("this aggregate does not accept DISTINCT"));
        }
        // Go's argument loop accepts a redundant `ALL` before EVERY argument,
        // not just the first (`JSON_OBJECTAGG(c1, ALL c2)` parses).
        let mut args = vec![self.parse_expr(prec::NONE)?];
        while self.is_op(",") {
            self.bump();
            if self.is_kw("ALL") {
                self.bump();
            }
            args.push(self.parse_expr(prec::NONE)?);
        }
        self.expect_op(")")?;
        if args.len() > 1 {
            let multi_arg_allowed = match name.as_str() {
                "COUNT" => distinct,
                "APPROX_COUNT_DISTINCT" | "APPROX_PERCENTILE" => true,
                // Go's grammar spells JSON_OBJECTAGG with exactly two
                // arguments, so two is the ONLY legal count -- one is a
                // syntax error the same way three is.
                "JSON_OBJECTAGG" => args.len() == 2,
                _ => false,
            };
            if !multi_arg_allowed {
                return Err(self.err_here("this aggregate does not accept multiple arguments"));
            }
        }
        if name == "JSON_OBJECTAGG" && args.len() != 2 {
            return Err(self.err_here("JSON_OBJECTAGG takes exactly two arguments"));
        }
        if self.is_kw("OVER") {
            self.bump();
            let over = self.parse_over_clause()?;
            return Ok(Expr::Window {
                name,
                args,
                distinct,
                ignore_nulls: false,
                from_last: false,
                over,
            });
        }
        Ok(Expr::Aggregate {
            name,
            distinct,
            args,
        })
    }

    /// Parses `NAME(args...) OVER (window_spec)` for every window function
    /// that ISN'T an aggregate name (so not sharing `parse_aggregate`'s own
    /// `OVER`-detection path): the zero-argument ranking functions
    /// `ROW_NUMBER`/`RANK`/`DENSE_RANK`; `FIRST_VALUE`/`LAST_VALUE` (one
    /// argument) and `NTH_VALUE` (two: value, then a 1-based position);
    /// and `LAG`/`LEAD` (one to three: value, an optional offset, an
    /// optional out-of-range default) — see [`tidb_ast::Expr::Window`]'s
    /// own doc for the exact arity of each. Optional `FROM FIRST|LAST` and
    /// `IGNORE|RESPECT NULLS` modifiers are preserved in the AST.
    pub(crate) fn parse_window_func(&mut self) -> PResult<Expr> {
        let name = window_func_canonical(&self.bump().text)
            .expect("window function name")
            .to_string();
        self.expect_op("(")?;
        let args = match name.as_str() {
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" => Vec::new(),
            "FIRST_VALUE" | "LAST_VALUE" | "NTILE" => vec![self.parse_expr(prec::NONE)?],
            "NTH_VALUE" => {
                let value = self.parse_expr(prec::NONE)?;
                self.expect_op(",")?;
                let pos = self.parse_expr(prec::NONE)?;
                vec![value, pos]
            }
            // LAG/LEAD.
            _ => {
                let mut args = vec![self.parse_expr(prec::NONE)?];
                if self.is_op(",") {
                    self.bump();
                    args.push(self.parse_expr(prec::NONE)?);
                    if self.is_op(",") {
                        self.bump();
                        args.push(self.parse_expr(prec::NONE)?);
                    }
                }
                args
            }
        };
        self.expect_op(")")?;
        let from_last = if self.is_kw("FROM") && self.is_kw_at(1, "LAST") {
            self.bump();
            self.bump();
            true
        } else {
            if self.is_kw("FROM") && self.is_kw_at(1, "FIRST") {
                self.bump();
                self.bump();
            }
            false
        };
        let ignore_nulls = if self.is_kw("IGNORE") && self.is_kw_at(1, "NULLS") {
            self.bump();
            self.bump();
            true
        } else {
            if self.is_kw("RESPECT") && self.is_kw_at(1, "NULLS") {
                self.bump();
                self.bump();
            }
            false
        };
        self.expect_kw("OVER")?;
        let over = self.parse_over_clause()?;
        Ok(Expr::Window {
            name,
            args,
            distinct: false,
            ignore_nulls,
            from_last,
            over,
        })
    }

    /// Parses the `OVER` clause itself (the `OVER` keyword is already
    /// consumed by the caller): a bare window name (`OVER w`, no
    /// parentheses at all) or a parenthesized definition (`OVER (...)`,
    /// covering a fully inline spec, a bare name reference, and a name
    /// plus its own extension alike) — see [`WindowOver`]'s own doc for
    /// why the two restore differently even when equivalent.
    fn parse_over_clause(&mut self) -> PResult<WindowOver> {
        if self.is_op("(") {
            Ok(WindowOver::Def(self.parse_window_def_body()?))
        } else {
            Ok(WindowOver::Name(self.parse_name()?))
        }
    }

    /// Parses a window definition's own body, INCLUDING its enclosing
    /// parentheses: `(PARTITION BY expr, ... ORDER BY expr [ASC|DESC],
    /// ... [ROWS ...])`, with an OPTIONAL leading base-window name before
    /// any of those clauses — shared by a parenthesized `OVER (...)`
    /// reference and one entry of a top-level `WINDOW name AS (...)`
    /// clause alike (`crate::select::Parser::parse_window_clause`). A
    /// leading identifier is read as the base name whenever the CURRENT
    /// token isn't already `PARTITION`/`ORDER`/`ROWS`/`RANGE`/`)` — those
    /// are all keywords or an operator, never a plain `Ident`, so no
    /// separate lookahead check is needed to tell them apart. What an
    /// extension's own `spec` may add on top of a named `base` (never
    /// `PARTITION BY`; `ORDER BY`/a frame only if the base doesn't
    /// already have one) is a real MySQL/TiDB restriction (confirmed via
    /// `gorun`) but NOT a parse-time one — the grammar accepts any
    /// combination syntactically here, validated only when
    /// `tidb_exec::window` resolves a window function's `OVER` clause.
    /// `RANGE BETWEEN ...` parses via the exact same [`Parser::parse_window_frame`]
    /// as `ROWS`, just tagged [`FrameKind::Range`] — validating that a
    /// numeric offset bound needs EXACTLY one `ORDER BY` column, and the
    /// DESC-aware sign flip on that offset, both happen only when
    /// `tidb_exec::window` resolves a window function's `OVER` clause
    /// (see [`WindowFrame`]'s own doc), not here.
    pub(crate) fn parse_window_def_body(&mut self) -> PResult<WindowDef> {
        self.expect_op("(")?;
        let base = if self.peek().kind == TokenKind::Ident {
            Some(self.parse_name()?)
        } else {
            None
        };
        let mut partition_by = Vec::new();
        if self.is_kw("PARTITION") {
            self.bump();
            self.expect_kw("BY")?;
            partition_by = self.parse_expr_list()?;
        }
        let mut order_by = Vec::new();
        if self.is_kw("ORDER") {
            self.bump();
            self.expect_kw("BY")?;
            order_by = self.parse_order_list()?;
        }
        let frame = if self.is_kw("ROWS") {
            self.bump();
            Some(self.parse_window_frame(FrameKind::Rows)?)
        } else if self.is_kw("RANGE") {
            self.bump();
            Some(self.parse_window_frame(FrameKind::Range)?)
        } else {
            None
        };
        self.expect_op(")")?;
        Ok(WindowDef {
            base,
            spec: WindowSpec {
                partition_by,
                order_by,
                frame,
            },
        })
    }

    /// Parses the body of a `ROWS`/`RANGE` frame clause AFTER the
    /// `ROWS`/`RANGE` keyword itself (`kind` says which): either `BETWEEN
    /// <bound> AND <bound>`, or the single-bound shorthand `<bound>`
    /// alone, normalized to `BETWEEN <bound> AND CURRENT ROW` (matching
    /// real TiDB's own restore — confirmed via `godump`, there is only
    /// one shape past this point, for EITHER kind).
    fn parse_window_frame(&mut self, kind: FrameKind) -> PResult<WindowFrame> {
        if self.is_kw("BETWEEN") {
            self.bump();
            let start = self.parse_frame_bound()?;
            self.expect_kw("AND")?;
            let end = self.parse_frame_bound()?;
            Ok(WindowFrame { kind, start, end })
        } else {
            let start = self.parse_frame_bound()?;
            Ok(WindowFrame {
                kind,
                start,
                end: FrameBound::CurrentRow,
            })
        }
    }

    /// Parses one frame boundary: `UNBOUNDED PRECEDING`/`UNBOUNDED
    /// FOLLOWING`, `CURRENT ROW`, or `expr PRECEDING`/`expr FOLLOWING`.
    /// The offset expression is parsed at `PREDICATE + 1` — ABOVE
    /// `AND`/`OR`'s own precedence — the same technique
    /// `parse_predicate`'s own `BETWEEN low AND high` already uses so the
    /// bound's own `AND` separator is never swallowed as part of a
    /// (nonsensical) boolean expression.
    fn parse_frame_bound(&mut self) -> PResult<FrameBound> {
        if self.is_kw("UNBOUNDED") {
            self.bump();
            if self.is_kw("PRECEDING") {
                self.bump();
                return Ok(FrameBound::UnboundedPreceding);
            }
            self.expect_kw("FOLLOWING")?;
            return Ok(FrameBound::UnboundedFollowing);
        }
        if self.is_kw("CURRENT") {
            self.bump();
            self.expect_kw("ROW")?;
            return Ok(FrameBound::CurrentRow);
        }
        let n = self.parse_expr(prec::PREDICATE + 1)?;
        if self.is_kw("PRECEDING") {
            self.bump();
            return Ok(FrameBound::Preceding(Box::new(n)));
        }
        self.expect_kw("FOLLOWING")?;
        Ok(FrameBound::Following(Box::new(n)))
    }

    /// Parses `GROUP_CONCAT([DISTINCT] expr [, expr ...] [SEPARATOR 'str'])`,
    /// with an optional trailing `OVER (...)`. Go's grammar accepts
    /// `GROUP_CONCAT(...) OVER (...)` unconditionally at parse time (any
    /// aggregate name may take an `OVER` suffix — `pkg/parser`'s
    /// `parseFuncCall`) and rejects it later at plan time with 1235 `This
    /// version of TiDB doesn't yet support 'group_concat as window
    /// function'` (see `tidb_exec::window`'s `build_call`, which raises the
    /// same 1235 by name). So this parses the `OVER` clause too rather than
    /// erroring on the stray token, and returns `Expr::Window` instead of
    /// `Expr::GroupConcat` — the `ORDER BY`/`SEPARATOR` already consumed
    /// above that clause are then dropped, mirroring Go's
    /// `parseWindowFuncExpr`, which copies only `Args`/`Distinct` from the
    /// aggregate node into the window node, never `Order`.
    pub(crate) fn parse_group_concat(&mut self) -> PResult<Expr> {
        self.bump(); // GROUP_CONCAT
        self.expect_op("(")?;
        let distinct = if self.is_kw("DISTINCT") || self.is_kw("DISTINCTROW") {
            self.bump();
            true
        } else {
            false
        };
        let mut args = vec![self.parse_expr(prec::NONE)?];
        while self.is_op(",") {
            self.bump();
            args.push(self.parse_expr(prec::NONE)?);
        }
        // `ORDER BY` controls the row order within the concatenated
        // result — reuses the SAME item grammar (including positional
        // references) the outer `SELECT`'s own `ORDER BY` does, via the
        // same `parse_order_list`, confirmed via `godump restore`:
        // `GROUP_CONCAT(a, b ORDER BY 1 DESC, a)`.
        let order_by = if self.is_kw("ORDER") {
            self.bump();
            self.expect_kw("BY")?;
            self.parse_order_list()?
        } else {
            Vec::new()
        };
        let separator = if self.is_kw("SEPARATOR") {
            self.bump();
            let t = self.peek().clone();
            if t.kind != TokenKind::Str {
                return Err(self.err_here("expected a string literal after SEPARATOR"));
            }
            self.bump();
            self.decode_string(&t.text)
        } else {
            ",".to_string()
        };
        self.expect_op(")")?;
        if self.is_kw("OVER") {
            self.bump();
            let over = self.parse_over_clause()?;
            // `order_by`/`separator` were consumed above to stay on-grammar
            // (Go's own hand-written parser parses them into the aggregate
            // node before converting it), but neither survives the
            // aggregate-to-window conversion — see this function's own doc.
            let _ = (order_by, separator);
            return Ok(Expr::Window {
                name: "GROUP_CONCAT".to_string(),
                args,
                distinct,
                ignore_nulls: false,
                from_last: false,
                over,
            });
        }
        Ok(Expr::GroupConcat {
            distinct,
            args,
            order_by,
            separator: tidb_ast::TypedString::new(
                separator,
                self.connection_charset.clone(),
                self.connection_collation.clone(),
            ),
        })
    }
}

/// Returns the canonical restore name for a (supported) window function, or
/// `None`. Scope: only the zero-argument ranking functions — a frame-based
/// window function (`SUM(x) OVER (...)`, `LEAD`/`LAG`/...) is not modelled.
pub(super) fn window_func_canonical(name: &str) -> Option<&'static str> {
    match name.to_ascii_uppercase().as_str() {
        "ROW_NUMBER" => Some("ROW_NUMBER"),
        "RANK" => Some("RANK"),
        "DENSE_RANK" => Some("DENSE_RANK"),
        "LAG" => Some("LAG"),
        "LEAD" => Some("LEAD"),
        "FIRST_VALUE" => Some("FIRST_VALUE"),
        "LAST_VALUE" => Some("LAST_VALUE"),
        "NTH_VALUE" => Some("NTH_VALUE"),
        "NTILE" => Some("NTILE"),
        "PERCENT_RANK" => Some("PERCENT_RANK"),
        "CUME_DIST" => Some("CUME_DIST"),
        _ => None,
    }
}

/// Returns the canonical restore name for an aggregate function, or `None` if
/// the name is not a (supported) aggregate. The `STD*`/`VAR*` synonyms fold to
/// their canonical population forms, matching the Go AST.
pub(super) fn agg_canonical(name: &str) -> Option<&'static str> {
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => Some("COUNT"),
        "SUM" => Some("SUM"),
        "AVG" => Some("AVG"),
        "MAX" => Some("MAX"),
        "MIN" => Some("MIN"),
        "STD" | "STDDEV" | "STDDEV_POP" => Some("STDDEV_POP"),
        "STDDEV_SAMP" => Some("STDDEV_SAMP"),
        "VARIANCE" | "VAR_POP" => Some("VAR_POP"),
        "VAR_SAMP" => Some("VAR_SAMP"),
        "BIT_AND" => Some("BIT_AND"),
        "BIT_OR" => Some("BIT_OR"),
        "BIT_XOR" => Some("BIT_XOR"),
        // Restores with a `DISTINCT` modifier just like every other
        // aggregate here (`APPROX_COUNT_DISTINCT(DISTINCT x)`, confirmed
        // via `godump restore`), matching `Expr::Aggregate`'s own shape
        // exactly — unlike `COLLATION`/`WEIGHT_STRING` above, which are
        // plain scalar-shaped calls. Evaluation is `Unsupported` beyond
        // this parse-time recognition (`tidb_exec`'s own aggregate
        // dispatch has no entry for it), same "parse and restore only"
        // boundary.
        "APPROX_COUNT_DISTINCT" => Some("APPROX_COUNT_DISTINCT"),
        // Go's `parseAggregateFuncCall` routes these three by name exactly
        // like the aggregates above: `JSON_ARRAYAGG`/`JSON_OBJECTAGG` reject
        // DISTINCT and fix their arity (1 and 2), `APPROX_PERCENTILE` accepts
        // a multi-argument list unconditionally.
        "JSON_ARRAYAGG" => Some("JSON_ARRAYAGG"),
        "JSON_OBJECTAGG" => Some("JSON_OBJECTAGG"),
        "APPROX_PERCENTILE" => Some("APPROX_PERCENTILE"),
        _ => None,
    }
}
