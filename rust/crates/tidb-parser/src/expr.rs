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

//! The expression parser: precedence climbing over the exact precedence
//! table from `pkg/parser/prec.go` (see [`crate::prec`]), covering columns,
//! variables, literals, unary/binary operators, function calls, aggregates,
//! `GROUP_CONCAT`, window functions (the ranking functions `ROW_NUMBER`/
//! `RANK`/`DENSE_RANK`; the frame-based window aggregates `COUNT`/`SUM`/
//! `AVG`/`MAX`/`MIN` — `parse_aggregate` itself detects a trailing `OVER`
//! and dispatches to `Expr::Window`; the "value function" family
//! `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`/`LAG`/`LEAD`; and the
//! "distribution function" family `NTILE`/`PERCENT_RANK`/`CUME_DIST`,
//! each with its own arity parsed by `parse_window_func` — all sharing
//! `parse_over_clause`/`parse_window_def_body` for the `OVER (...)` clause
//! itself (including named-window references and `ROWS`/`RANGE` explicit
//! frames); see
//! [`tidb_ast::Expr::Window`]'s own doc for the exact scope), and the
//! `IN`/`BETWEEN`/`LIKE`/`IS` predicates. Called from `crate::ddl`,
//! `crate::dml`, and `crate::select`
//! wherever an expression is expected (`WHERE`, `DEFAULT`, assignment
//! values, select
//! fields, ...).

use tidb_ast::{
    BinaryOp, CastExpr, CastStyle, CastType, Expr, FrameBound, FrameKind, GetFormatSelector,
    IsTarget, MatchModifier, SysVarScope, TrimDirection, UnaryOp, WeightStringType, WindowDef,
    WindowFrame, WindowOver, WindowSpec,
};
use tidb_lexer::{canonical_charset, canonical_collation, canonical_legacy_charset, TokenKind};

use crate::{prec, PResult, Parser};

mod func;
use func::{is_datetime_precision_func, is_scalar_kw_func};
mod predicate;
mod window;
use window::{agg_canonical, window_func_canonical};

impl Parser {
    pub(crate) fn parse_expr(&mut self, min_prec: u8) -> PResult<Expr> {
        let mut left = self.parse_prefix(min_prec)?;
        loop {
            // Keyword predicates: IN / LIKE / BETWEEN / IS, and NOT-prefixed
            // predicates, all at the predicate/comparison precedence levels.
            if self.is_kw("NOT")
                && (self.is_kw_at(1, "IN")
                    || self.is_kw_at(1, "LIKE")
                    || self.is_kw_at(1, "ILIKE")
                    || self.is_kw_at(1, "BETWEEN")
                    || self.is_kw_at(1, "REGEXP")
                    || self.is_kw_at(1, "RLIKE"))
            {
                if min_prec > prec::PREDICATE {
                    break;
                }
                self.bump(); // NOT
                left = self.parse_predicate(left, true)?;
                continue;
            }
            if self.is_kw("NOT") && min_prec <= prec::PREDICATE {
                // Go shifts NOT before discovering that the following token
                // cannot complete a NOT IN/LIKE/BETWEEN/REGEXP predicate.
                // Consume it so the reported yacc boundary belongs to the
                // actual unexpected token, not to NOT itself.
                self.bump();
                return Err(self.err_here("expected a predicate after NOT"));
            }
            if self.is_kw("IN")
                || self.is_kw("LIKE")
                || self.is_kw("ILIKE")
                || self.is_kw("BETWEEN")
                || self.is_kw("REGEXP")
                || self.is_kw("RLIKE")
            {
                if min_prec > prec::PREDICATE {
                    break;
                }
                left = self.parse_predicate(left, false)?;
                continue;
            }
            if self.is_kw("IS") {
                if min_prec > prec::COMPARISON {
                    break;
                }
                left = self.parse_is(left)?;
                continue;
            }
            // `expr COLLATE name` — chains left-to-right with itself (a
            // repeat visit through this same loop iteration handles the
            // chain, matching real TiDB's own hand-written parser,
            // `pkg/parser/expr_parser.go`'s `case collate:` arm — see
            // `tidb_ast::Expr::Collate`'s own doc for the precedence
            // rationale). The collation name accepts a QUOTED STRING
            // too, not just a bare identifier (`parseCollateExpr`'s own
            // `p.next()` takes literally any next token — see
            // `Parser::parse_using_charset_name`'s own doc, reused
            // here), confirmed via `godump restore`: `expr COLLATE
            // 'binary'` restores identically to the bare-identifier
            // form.
            if self.is_kw("COLLATE") {
                if min_prec > prec::COLLATE {
                    break;
                }
                self.bump(); // COLLATE
                let raw = self.parse_using_charset_name()?;
                let collation = canonical_collation(&raw)
                    .ok_or_else(|| self.err_here(&format!("[ddl:1273]Unknown collation: '{raw}'")))?
                    .to_owned();
                left = Expr::Collate {
                    expr: Box::new(left),
                    collation,
                };
                continue;
            }
            // `expr -> path` / `expr ->> path` — JSON extraction, DESUGARS
            // at parse time to `JSON_EXTRACT(expr, path)` /
            // `JSON_UNQUOTE(JSON_EXTRACT(expr, path))` — real TiDB's own
            // grammar has no dedicated AST node for this operator at all
            // (`pkg/parser/expr_parser.go`'s `case jss, juss:` builds a
            // plain `FuncCallExpr`), matching the SAME "operator desugars
            // to a function-call AST shape" precedent
            // `fold_interval_arith`'s own doc already established for
            // `date_expr +/- INTERVAL`. Needs NO new evaluation code:
            // `JSON_EXTRACT`/`JSON_UNQUOTE` are unrecognized function
            // names to `tidb_expr::eval_func`'s own existing wildcard,
            // which already returns `Unsupported` (this crate has no
            // JSON value domain yet). ONLY valid when the left operand is
            // a BARE COLUMN reference (confirmed via `godump restore`:
            // `(a+b)->'$.x'` is a genuine `ParseError` — real TiDB's own
            // grammar restricts this to `SimpleIdent`, not arbitrary
            // `SimpleExpr`) — if `left` isn't a column, this deliberately
            // does NOT consume the operator, so the surrounding
            // expression falls through to whatever generic "unexpected
            // token" error real TiDB would ALSO give there. Binds at the
            // SAME tight precedence as `COLLATE` (confirmed: `pkg/parser/
            // expr_parser.go` uses `precCollate` for both). The right
            // side MUST be a plain string-literal token (real grammar,
            // not a full sub-expression — confirmed via `godump restore`:
            // `a->(1+1)` is a genuine `ParseError`).
            if (self.is_op("->") || self.is_op("->>")) && matches!(left, Expr::Column(_)) {
                if min_prec > prec::COLLATE {
                    break;
                }
                let unquote = self.is_op("->>");
                self.bump();
                if self.peek().kind != TokenKind::Str {
                    return Err(
                        self.err_here("-> requires a string literal JSON path on the right side")
                    );
                }
                let path = Expr::String(self.bumped_string());
                let extract = Expr::Func {
                    name: "JSON_EXTRACT".to_string(),
                    args: vec![left, path],
                    origin_position: 0,
                };
                left = if unquote {
                    Expr::Func {
                        name: "JSON_UNQUOTE".to_string(),
                        args: vec![extract],
                        origin_position: 0,
                    }
                } else {
                    extract
                };
                continue;
            }
            // `expr MEMBER OF (array)` — see `tidb_ast::Expr::MemberOf`'s
            // own doc. `tidb_lexer` already merges `MEMBER OF` into ONE
            // keyword token (the SAME `AS OF`-style two-word merge —
            // confirmed via a direct `tidb_lexer::Lexer` probe, not
            // assumed, matching the lesson from `AS OF TIMESTAMP`'s own
            // implementation). Binds at `prec::PREDICATE` (matching real
            // TiDB's own `precPredicate`); `array` parses at
            // `prec::UNARY` (real TiDB's own `SimpleExpr` restriction —
            // confirmed via `godump restore`: `MEMBER OF(1 OR 2)` is a
            // genuine `ParseError`), while `left` (the candidate element)
            // has no type restriction at all, unlike `->`/`->>` just
            // above.
            if self.is_kw("MEMBER OF") {
                if min_prec > prec::PREDICATE {
                    break;
                }
                self.bump();
                self.expect_op("(")?;
                let array = self.parse_expr(prec::UNARY)?;
                self.expect_op(")")?;
                left = Expr::MemberOf {
                    expr: Box::new(left),
                    array: Box::new(array),
                };
                continue;
            }

            // Under `PIPES_AS_CONCAT` the scanner keeps Go's `pipes` token,
            // which `pkg/parser/expr_parser.go:216` compiles to a `CONCAT()`
            // CALL rather than a `BinaryExpr` -- at `precConcat`, well above
            // the `precOr` the same spelling carries by default.
            if self.pipes_as_concat && self.is_op("||") {
                if prec::CONCAT < min_prec {
                    break;
                }
                self.bump();
                let right = self.parse_expr(prec::CONCAT + 1)?; // left-associative
                left = Expr::Func {
                    name: "concat".to_string(),
                    args: vec![left, right],
                    origin_position: 0,
                };
                continue;
            }

            // Standard binary operators.
            let Some((op, p)) = self.infix_op() else {
                break;
            };
            if p < min_prec {
                break;
            }
            self.bump(); // operator

            // `expr <cmp> ANY|SOME|ALL (subquery)` at comparison precedence.
            if p == prec::COMPARISON
                && (self.is_kw("ANY") || self.is_kw("SOME") || self.is_kw("ALL"))
            {
                let all = self.is_kw("ALL");
                self.bump(); // ANY / SOME / ALL
                let subquery = self.parse_query_subquery()?;
                left = Expr::CompareSubquery {
                    op,
                    left: Box::new(left),
                    all,
                    subquery,
                };
                continue;
            }

            let right = self.parse_expr(p + 1)?; // left-associative
            left = self.fold_interval_arith(op, left, right)?;
        }
        Ok(left)
    }

    /// `date_expr + INTERVAL amount unit` / `date_expr - INTERVAL amount
    /// unit` desugar to `DATE_ADD(date_expr, INTERVAL amount unit)` /
    /// `DATE_SUB(...)` — confirmed via `godump restore`, not a
    /// `tidb-exec`-side rewrite, since `DATE_ADD`/`DATE_SUB` are ALREADY
    /// fully implemented and this is PURELY a parse-time desugaring
    /// (real TiDB's own grammar, not a semantic pass). `+` is
    /// commutative here (`INTERVAL ... + date_expr` ALSO desugars, the
    /// non-`Interval` operand always becomes `DATE_ADD`'s FIRST
    /// argument regardless of which side it was written on), but `-` is
    /// NOT (`INTERVAL ... - date_expr` is a genuine `ParseError`, and so
    /// is `INTERVAL ... + INTERVAL ...`) — both confirmed via `godump
    /// restore`. A bare parenthesized `INTERVAL` is rejected while parsing
    /// the parentheses, matching Go's rule that `INTERVAL` is not a
    /// standalone expression.
    /// Runs INSIDE the main precedence-climbing loop (not as a one-shot
    /// post-pass), so a chain like `a + INTERVAL 5 DAY + INTERVAL 3
    /// DAY` builds on the ALREADY-desugared result at each step
    /// (`DATE_ADD(DATE_ADD(a, INTERVAL 5 DAY), INTERVAL 3 DAY)`),
    /// matching real TiDB's own left-associative nesting exactly.
    fn fold_interval_arith(&self, op: BinaryOp, left: Expr, right: Expr) -> PResult<Expr> {
        let (left_is_interval, right_is_interval) = (
            matches!(left, Expr::Interval { .. }),
            matches!(right, Expr::Interval { .. }),
        );
        match op {
            BinaryOp::Plus if left_is_interval && right_is_interval => {
                Err(self.err_here("INTERVAL cannot be added to INTERVAL"))
            }
            BinaryOp::Plus if left_is_interval => Ok(Expr::Func {
                name: "DATE_ADD".to_string(),
                args: vec![right, left],
                origin_position: 0,
            }),
            BinaryOp::Plus if right_is_interval => Ok(Expr::Func {
                name: "DATE_ADD".to_string(),
                args: vec![left, right],
                origin_position: 0,
            }),
            BinaryOp::Minus if left_is_interval => {
                Err(self.err_here("INTERVAL cannot be the left operand of -"))
            }
            BinaryOp::Minus if right_is_interval => Ok(Expr::Func {
                name: "DATE_SUB".to_string(),
                args: vec![left, right],
                origin_position: 0,
            }),
            _ => Ok(Expr::Binary(op, Box::new(left), Box::new(right))),
        }
    }

    /// Returns the binary operator and its precedence for the current token,
    /// mirroring `tokenPrecedence` + `tokenToOp` in `pkg/parser/prec.go`.
    fn infix_op(&self) -> Option<(BinaryOp, u8)> {
        let t = self.peek();
        match t.kind {
            TokenKind::Op => match t.text.as_str() {
                "|" => Some((BinaryOp::BitOr, prec::BIT_OR)),
                "&" => Some((BinaryOp::BitAnd, prec::BIT_AND)),
                "^" => Some((BinaryOp::BitXor, prec::BIT_XOR)),
                "<<" => Some((BinaryOp::LeftShift, prec::SHIFT)),
                ">>" => Some((BinaryOp::RightShift, prec::SHIFT)),
                "+" => Some((BinaryOp::Plus, prec::ADD_SUB)),
                "-" => Some((BinaryOp::Minus, prec::ADD_SUB)),
                "*" => Some((BinaryOp::Mul, prec::MUL_DIV)),
                "/" => Some((BinaryOp::Div, prec::MUL_DIV)),
                "%" => Some((BinaryOp::Mod, prec::MUL_DIV)),
                "=" => Some((BinaryOp::Eq, prec::COMPARISON)),
                "<=>" => Some((BinaryOp::NullEq, prec::COMPARISON)),
                ">=" => Some((BinaryOp::Ge, prec::COMPARISON)),
                ">" => Some((BinaryOp::Gt, prec::COMPARISON)),
                "<=" => Some((BinaryOp::Le, prec::COMPARISON)),
                "<" => Some((BinaryOp::Lt, prec::COMPARISON)),
                "!=" | "<>" => Some((BinaryOp::Ne, prec::COMPARISON)),
                "&&" => Some((BinaryOp::LogicAnd, prec::AND)),
                "||" => Some((BinaryOp::LogicOr, prec::OR)), // default mode: pipes-as-or
                _ => None,
            },
            TokenKind::Keyword => match t.text.to_ascii_uppercase().as_str() {
                "OR" => Some((BinaryOp::LogicOr, prec::OR)),
                "XOR" => Some((BinaryOp::LogicXor, prec::XOR)),
                "AND" => Some((BinaryOp::LogicAnd, prec::AND)),
                "DIV" => Some((BinaryOp::IntDiv, prec::MUL_DIV)),
                "MOD" => Some((BinaryOp::Mod, prec::MUL_DIV)),
                _ => None,
            },
            _ => None,
        }
    }

    pub(crate) fn parse_prefix(&mut self, min_prec: u8) -> PResult<Expr> {
        let t = self.peek().clone();
        match t.kind {
            TokenKind::Op if t.text == "?" => {
                self.bump();
                Ok(Expr::ParamMarker {
                    offset: t.offset,
                    order: self.next_param_marker_position(),
                    in_execute: false,
                    projection_offset: 0,
                })
            }
            TokenKind::IntLit => {
                self.bump();
                Ok(Expr::Int(t.text))
            }
            TokenKind::DecLit => {
                self.bump();
                Ok(Expr::Decimal(t.text))
            }
            TokenKind::FloatLit => {
                self.bump();
                let f = t
                    .text
                    .parse::<f64>()
                    .map_err(|_| self.err_here("invalid float literal"))?;
                // Rust's own float parser saturates an overflowing literal
                // (e.g. `1e400`) to infinity rather than erroring, but real
                // TiDB rejects it at PARSE time (confirmed via `godump
                // restore`, not assumed — the boundary is exactly
                // `f64::MAX`, `1.7976931348623157e308` parses, `1.8e308`
                // doesn't), so this must too.
                if !f.is_finite() {
                    return Err(self.err_here("float literal out of range"));
                }
                Ok(Expr::Float(f))
            }
            TokenKind::HexLit => {
                // Go LEXES `0X11` as hexLit (`startWithNumber`, 'x' || 'X' in
                // one arm) and then FAILS at literal construction: the parser
                // calls `ast.NewHexLiteral` -> `ParseHexStr`, whose number
                // form checks `strings.HasPrefix(s, "0x")` -- LOWERCASE ONLY
                // (test_driver_datum.go:384) -- so `select 0X11` is a
                // statement error while `select 0x11` and `X'11'` both parse
                // (parser_test.go:5237,5240). Reproduce the constructor half
                // here; the quoted `x'..'`/`X'..'` forms accept either case.
                if t.text.starts_with("0X") {
                    return Err(self.err_here("invalid hexadecimal format"));
                }
                self.bump();
                Ok(Expr::Hex(normalize_hex(&t.text)))
            }
            TokenKind::BitLit => {
                self.bump();
                Ok(Expr::Bit(normalize_bit(&t.text)))
            }
            // MySQL/TiDB concatenates adjacent bare string-literal tokens:
            // `'a' 'b'` parses as the SINGLE value `'ab'`, no operator
            // between them (read directly from `pkg/parser/expr_parser.go`'s
            // `parseLiteral`, `case stringLit:` — `for p.peek().Tp ==
            // stringLit { val += p.next().Lit }`). Each token's OWN escapes
            // are decoded independently before concatenating (mirrors Go's
            // `Lit` already being per-token-decoded) — confirmed via
            // `godump restore` that a literal space token folded in this
            // way contributes its own character, not a separator (`'a' ' '
            // 'b'` restores as `'a b'`). This applies ONLY to plain,
            // un-introduced string tokens: an `N'...'`/`_charset'...'`
            // literal is a genuinely different token/parse path (not
            // `TokenKind::Str`) that this loop never reaches, so a
            // following bare string token there is left for the caller's
            // own implicit-alias parsing instead (confirmed via `godump
            // restore`: `N'a' 'b'` restores as `_UTF8'a' AS \`b\``, NOT a
            // concatenation) — real TiDB's own behavior exactly, since its
            // `case stringLit:` arm is likewise never reached for an
            // `N`/charset-introduced literal either.
            TokenKind::Str => {
                self.bump();
                let mut val = self.decode_string(&t.text);
                while self.peek().kind == TokenKind::Str {
                    let next = self.bump();
                    val.push_str(&self.decode_string(&next.text));
                }
                Ok(Expr::String(val))
            }
            // `_charset'x'` / `N'x'` / `n'x'` — a character-set-introduced
            // string/hex/bit literal (see
            // `tidb_lexer::TokenKind::CharsetIntroducer` and
            // `tidb_ast::Expr::CharsetString`'s own docs). Real TiDB's own
            // grammar requires one of those literal forms immediately after
            // the introducer — confirmed via `godump restore` that a bare
            // `_latin1` with nothing following is a genuine `ParseError`
            // there too (the LEXER itself recognizes the introducer
            // unconditionally, regardless of what follows; only the GRAMMAR
            // enforces the following literal). No adjacent-
            // string-literal concatenation here (unlike the plain
            // `TokenKind::Str` case above) — confirmed via `godump
            // restore` that `_latin1'a' 'b'` does NOT concatenate,
            // restoring as `_LATIN1'a' AS \`b\`` instead (the bare `'b'`
            // token is left for the caller's own implicit-alias parsing).
            TokenKind::CharsetIntroducer => {
                self.bump();
                // The scanner recognizes every name in TiDB's charset table,
                // but Go's `parseCharsetIntroducer` immediately validates it
                // through `charset.GetDefaultCollationLegacy`. Keep that
                // narrower parser boundary: GBK/UJIS/other registered legacy
                // names are lexer tokens but unsupported introducers.
                let charset = canonical_legacy_charset(&t.text)
                    .ok_or_else(|| self.err_here("unsupported character introducer"))?;
                // Go preserves the canonical lowercase name for an explicit
                // `_charset` introducer, while national strings (`N'...'`)
                // restore with their fixed `UTF8` spelling.
                let restored_charset = if t.text.eq_ignore_ascii_case("N") {
                    "UTF8".to_owned()
                } else {
                    charset.to_owned()
                };
                match self.peek().kind {
                    // Go's UNDERSCORE_CHARSET production accepts a string,
                    // hex, or bit literal after the introducer.  Hex/bit
                    // payloads deliberately use the ordinary literal AST:
                    // TiDB's ValueExpr restore drops the charset wrapper and
                    // emits x'...' / b'...' (the same canonical form for
                    // `_binary 0x...` and `_utf8mb4 0x...`).
                    TokenKind::Str => {
                        let value = self.bumped_string();
                        if charset == "utf8mb4" {
                            Ok(Expr::String(value))
                        } else {
                            Ok(Expr::CharsetString {
                                charset: restored_charset,
                                value,
                            })
                        }
                    }
                    TokenKind::HexLit => {
                        // Same constructor rule as the bare-literal arm:
                        // Go's ParseHexStr accepts only a LOWERCASE `0x`
                        // number prefix, so `_utf8 0XD0B1` is a statement
                        // error while `_utf8 0xD0B1` parses.
                        if self.peek().text.starts_with("0X") {
                            return Err(self.err_here("invalid hexadecimal format"));
                        }
                        let token = self.bump();
                        let value = Expr::Hex(normalize_hex(&token.text));
                        if matches!(charset, "binary" | "utf8mb4") {
                            Ok(value)
                        } else {
                            Ok(Expr::CharsetBinary {
                                charset: restored_charset,
                                value: Box::new(value),
                            })
                        }
                    }
                    TokenKind::BitLit => {
                        let token = self.bump();
                        let value = Expr::Bit(normalize_bit(&token.text));
                        if matches!(charset, "binary" | "utf8mb4") {
                            Ok(value)
                        } else {
                            Ok(Expr::CharsetBinary {
                                charset: restored_charset,
                                value: Box::new(value),
                            })
                        }
                    }
                    _ => Err(self.err_here("expected a string, hex, or bit literal")),
                }
            }
            // An aggregate-function keyword directly before `(` is an aggregate
            // call (`COUNT(...)`, `SUM(...)`).
            TokenKind::Keyword
                if self.peek_n(1).kind == TokenKind::Op
                    && self.peek_n(1).text == "("
                    && agg_canonical(&t.text).is_some() =>
            {
                self.parse_aggregate()
            }
            // `GROUP_CONCAT` has its own shape (multiple args, an optional
            // `SEPARATOR`), distinct from the single-arg aggregates above.
            TokenKind::Keyword
                if self.peek_n(1).kind == TokenKind::Op
                    && self.peek_n(1).text == "("
                    && t.text.eq_ignore_ascii_case("GROUP_CONCAT") =>
            {
                self.parse_group_concat()
            }
            // A ranking window-function keyword directly before `()` is a
            // window function call (`ROW_NUMBER() OVER (...)`).
            TokenKind::Keyword
                if self.peek_n(1).kind == TokenKind::Op
                    && self.peek_n(1).text == "("
                    && window_func_canonical(&t.text).is_some() =>
            {
                self.parse_window_func()
            }
            // A plain scalar-function keyword before `(` (e.g. `IF`, `COALESCE`)
            // is an ordinary function call.
            TokenKind::Keyword
                if self.peek_n(1).kind == TokenKind::Op
                    && self.peek_n(1).text == "("
                    && is_datetime_precision_func(&t.text) =>
            {
                self.parse_datetime_precision_func()
            }
            // CURDATE is the one current-date keyword whose Go production
            // accepts only the empty parenthesized form. Keep it ahead of the
            // generic keyword-function arm so `CURDATE(1)` cannot become a
            // generic expression call.
            TokenKind::Keyword
                if t.text.eq_ignore_ascii_case("CURDATE")
                    && self.peek_n(1).kind == TokenKind::Op
                    && self.peek_n(1).text == "(" =>
            {
                let name = self.bump().text;
                self.expect_op("(")?;
                self.expect_op(")")?;
                Ok(Expr::Func {
                    name,
                    args: vec![],
                    origin_position: t.offset,
                })
            }
            // A plain scalar-function keyword before `(` (e.g. `IF`, `COALESCE`)
            // is an ordinary function call.
            TokenKind::Keyword
                if self.peek_n(1).kind == TokenKind::Op
                    && self.peek_n(1).text == "("
                    && is_scalar_kw_func(&t.text) =>
            {
                self.parse_named_func()
            }
            TokenKind::Keyword => match t.text.to_ascii_uppercase().as_str() {
                "NULL" => {
                    self.bump();
                    Ok(Expr::Null)
                }
                "TRUE" => {
                    self.bump();
                    Ok(Expr::Bool(true))
                }
                "FALSE" => {
                    self.bump();
                    Ok(Expr::Bool(false))
                }
                // These nine (unlike `NOW`/`CURDATE`/`CURTIME`) have a
                // special MySQL grammar rule allowing them bare, with no
                // `()` at all — confirmed via `godump restore`
                // (`select current_timestamp` parses and restores as
                // `CURRENT_TIMESTAMP()`, and likewise for the other six,
                // `CURRENT_ROLE` included; `LOCALTIME` and
                // `LOCALTIMESTAMP` restore with their own names too). The
                // parenthesized form
                // (`CURRENT_TIMESTAMP(...)`, ...) never reaches this
                // arm — it's caught earlier by `is_scalar_kw_func`.
                "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "UTC_DATE" | "UTC_TIME"
                | "UTC_TIMESTAMP" | "CURRENT_ROLE" | "CURRENT_USER" | "LOCALTIME"
                | "LOCALTIMESTAMP"
                    if !(self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(") =>
                {
                    let name = t.text.to_ascii_uppercase();
                    self.bump();
                    Ok(Expr::Func {
                        name,
                        args: vec![],
                        origin_position: t.offset,
                    })
                }
                "NOT" => {
                    let precedence = if self.high_not_precedence {
                        prec::UNARY
                    } else {
                        prec::NOT
                    };
                    if min_prec > precedence {
                        return Err(self.err_here("NOT not allowed at this precedence"));
                    }
                    self.bump();
                    let e = self.parse_expr(precedence)?;
                    if let Expr::Exists { subquery, not } = e {
                        Ok(Expr::Exists {
                            subquery,
                            not: !not,
                        })
                    } else {
                        Ok(Expr::Unary(
                            if self.high_not_precedence {
                                UnaryOp::Not
                            } else {
                                UnaryOp::NotKeyword
                            },
                            Box::new(e),
                        ))
                    }
                }
                // `[NOT] EXISTS (subquery)`; `NOT` is handled by the unary path,
                // so restore of `NOT EXISTS (...)` matches.
                "EXISTS" => {
                    self.bump();
                    let subquery = self.parse_query_subquery()?;
                    Ok(Expr::Exists {
                        subquery,
                        not: false,
                    })
                }
                // `INTERVAL value unit` — a general prefix expression (matching
                // real MySQL grammar), most commonly `DATE_ADD`/`DATE_SUB`'s
                // second argument, but parsed here rather than special-cased
                // by caller function name. The unit is any keyword token,
                // captured as text; only some units are evaluated (see
                // `tidb_expr::date_fn`).
                // `INTERVAL(N, N1, N2, ...)` (immediately followed by `(`)
                // is a totally unrelated GENERIC scalar function (an
                // index-lookup among a sorted numeric list) — NOT
                // `INTERVAL value unit`'s own date-arithmetic
                // prefix-expression grammar at all. Read directly from
                // `pkg/parser/expr_prefix_parser.go`'s
                // `parsePrefixKeywordExpr` (`case interval:`): `if
                // p.peekN(1).Tp != '(' { ...date-arith form... } else {
                // return p.parseIdentOrFuncCall() }` — found via a
                // restore-mismatch surfaced by adding `Expr::Row`'s own
                // bare-paren-comma-list grammar, which had been silently
                // absorbing `INTERVAL(...)`'s own argument list into
                // `parse_interval`'s single `value` expression (previously
                // a harmless `ParseError`, now a genuinely wrong parse
                // without this check).
                "INTERVAL"
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" =>
                {
                    self.parse_named_func()
                }
                "INTERVAL" => self.parse_interval(),
                // `CASE`/`BINARY`/`MATCH` are genuinely reserved, expression-
                // introducing keywords with NO bare-identifier fallback in
                // real TiDB either (confirmed via `godump restore`: `SELECT
                // case FROM t`/`SELECT binary FROM t`/`SELECT match FROM t`
                // are ALL genuine `ParseError`s there too) — unlike the
                // builtin-function-NAME keywords immediately below, these
                // three correctly dispatch unconditionally, no guard needed.
                "CASE" => self.parse_case(),
                // `EXTRACT`/`CAST`/`TIMESTAMPADD`/`TIMESTAMPDIFF`/
                // `GET_FORMAT`/`ADDDATE`/`SUBDATE` are builtin-function-name
                // keywords with a bare, no-parens identifier meaning TOO
                // (the SAME class of bug `TRIM`/`CHAR`/`JSON_SUM_CRC32`/
                // `POSITION`/`SUBSTR`/`SUBSTRING` were already fixed for) —
                // confirmed via `godump restore` that bare `SELECT extract
                // FROM t`/`cast`/`timestampadd`/`timestampdiff`/
                // `get_format`/`adddate`/`subdate` are all valid real SQL,
                // genuinely rejected here before this fix. All seven are
                // non-reserved (confirmed in both `tidb_lexer::reserved`
                // and `pkg/parser/reserved_words.go`), so — like `TRIM`/
                // `JSON_SUM_CRC32` — they fall through cleanly to the
                // shared `_ if !is_clause_keyword(...) => parse_ident_or_func()`
                // arm below once guarded, no bespoke fallback needed.
                "EXTRACT" if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" => {
                    self.parse_extract()
                }
                "CAST" if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" => {
                    self.parse_cast()
                }
                // `CONVERT`, unlike the seven above, IS a reserved keyword
                // (confirmed in both `tidb_lexer::reserved` and real
                // TiDB's own `pkg/parser/reserved_words.go`) — so, exactly
                // like `CHAR`, it needs its own bespoke fallback rather
                // than the shared non-reserved arm (`parse_ident_or_func`
                // delegates to `parse_name_or_keyword`, which explicitly
                // rejects a reserved keyword as a column name; real TiDB's
                // own `tryBuiltinFunc` wrapper has no such reserved-word
                // check at all, confirmed via `godump restore` that bare
                // `SELECT convert FROM t` parses there).
                "CONVERT" => {
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" {
                        self.parse_convert()
                    } else {
                        Ok(Expr::Column(vec![self.bump().text]))
                    }
                }
                "BINARY" => self.parse_binary_operator(),
                "MATCH" => self.parse_match_against(),
                "TIMESTAMPADD"
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" =>
                {
                    self.parse_timestamp_add()
                }
                "TIMESTAMPDIFF"
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" =>
                {
                    self.parse_timestamp_diff()
                }
                "GET_FORMAT"
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" =>
                {
                    self.parse_get_format()
                }
                "ADDDATE" if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" => {
                    self.parse_adddate_or_subdate("ADDDATE")
                }
                "SUBDATE" if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" => {
                    self.parse_adddate_or_subdate("SUBDATE")
                }
                // `NEXT VALUE FOR seq_name` — SQL-standard sequence syntax,
                // sugar for `NEXTVAL(seq_name)` (real TiDB desugars it at
                // PARSE time too, read directly from
                // `pkg/parser/expr_prefix_parser.go`'s own
                // `parsePrefixExpr`: `if tok.Tp == next && peekN(1) ==
                // value && peekN(2) == forKwd { ... FnName = "nextval" }`
                // — confirmed via `godump restore`: `NEXT VALUE FOR seq1`
                // restores as `NEXTVAL(\`seq1\`)`, byte-identical to
                // writing `NEXTVAL(seq1)` directly). `NEXTVAL`/`LASTVAL`/
                // `SETVAL` themselves need no dedicated AST shape at all —
                // they already parse as an ordinary `Expr::Func` via the
                // ambient function-call path, with the sequence-name
                // argument an ordinary (possibly schema-qualified)
                // `Expr::Column` — confirmed via `godump restore` that
                // this ALREADY restores identically to real TiDB's own
                // dedicated `TableNameExpr`-based argument, since both
                // unconditionally back-quote every path segment. `NEXT`/
                // `VALUE` are both non-reserved (ordinary `GENERAL_KEYWORDS`
                // entries), so this guard must check the SAME 3-token
                // sequence real TiDB's own parser does before consuming
                // anything — a bare `SELECT next, value FROM t` (unrelated
                // uses of these names) is unaffected.
                "NEXT" if self.is_kw_at(1, "VALUE") && self.is_kw_at(2, "FOR") => {
                    self.bump(); // NEXT
                    self.bump(); // VALUE
                    self.bump(); // FOR
                    let path = self.parse_name_path()?;
                    Ok(Expr::Func {
                        name: "NEXTVAL".to_string(),
                        args: vec![Expr::Column(path)],
                        origin_position: t.offset,
                    })
                }
                "ROW" => self.parse_row_constructor(),
                "DEFAULT" => self.parse_default_expr(),
                // `CHAR`/`JSON_SUM_CRC32`/`TRIM`/`POSITION`/`SUBSTR`/
                // `SUBSTRING` are all builtin-function-name keywords with a
                // bare, no-parens identifier meaning TOO — real TiDB's own
                // hand-written parser gates EVERY one of them on an
                // immediately-following `(`, falling back to an ordinary
                // identifier/function-call otherwise (`pkg/parser/
                // parser.go`'s `tryBuiltinFunc`: `if p.peekN(1).Tp == '('
                // { return parseFn() } return p.parseIdentOrFuncCall()`,
                // used for `TRIM`/`POSITION`/`SUBSTRING`/`JSON_SUM_CRC32`;
                // `CHAR`/`CHARACTER` get the SAME check inlined directly
                // in `parsePrefixKeywordExpr`'s own switch, since they're
                // reserved words with an explicit carve-out) — confirmed
                // via `godump restore`/direct probes that bare `SELECT
                // trim FROM t`/`SELECT char FROM t`/`SELECT json_sum_crc32
                // FROM t` (no parens) are all valid real SQL, genuinely
                // rejected here before this fix. `CHAR` alone needs its
                // own bespoke fallback (not `parse_ident_or_func`, and not
                // the shared `_ if !is_clause_keyword(...)` arm below) because,
                // unlike the other five, `CHAR` IS a reserved keyword
                // (confirmed in both `tidb_lexer::reserved` and real
                // TiDB's own `pkg/parser/reserved_words.go`) —
                // `parse_ident_or_func` delegates to
                // `parse_name_path`/`parse_name_or_keyword`, which
                // explicitly REJECT a reserved keyword as a column name
                // (the general rule this ONE keyword is a deliberate,
                // narrow exception to, mirroring real TiDB's own
                // `case charType, character:` calling
                // `p.parseIdentOrFuncCall()` directly, bypassing the
                // normal reserved-word dispatch entirely) — so this builds
                // a single-component `Expr::Column` straight from the
                // token text, the SAME shape `parse_name_or_keyword` would
                // have produced were `CHAR` non-reserved.
                "CHAR" => {
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" {
                        self.parse_char_func()
                    } else {
                        Ok(Expr::Column(vec![self.bump().text]))
                    }
                }
                "JSON_SUM_CRC32"
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" =>
                {
                    self.parse_json_sum_crc32()
                }
                "TRIM" if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" => {
                    self.parse_trim()
                }
                "POSITION"
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" =>
                {
                    self.parse_position_func()
                }
                "SUBSTR" | "SUBSTRING"
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" =>
                {
                    self.parse_substring_func(&t.text.to_ascii_uppercase())
                }
                "WEIGHT_STRING"
                    if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" =>
                {
                    self.parse_weight_string()
                }
                // `MOD(a, b)` desugars to the `%` binary operator, NOT an
                // `Expr::Func` call — real TiDB's own hand-written parser
                // (`pkg/parser/expr_func_parser.go`'s own
                // `parseScalarFuncCall`, checked BEFORE ever allocating a
                // `FuncCallExpr`: "MOD returns a BinaryOperationExpr, not
                // a FuncCallExpr") builds a plain `BinaryOperationExpr`
                // with `opcode.Mod` instead, confirmed via `godump
                // restore`: `MOD(a, 5)` restores as `` `a`%5 ``, byte-
                // identical to `a % 5` written directly. Exactly two
                // arguments — real TiDB's own `parseModFuncCall` parses
                // left, expects a comma, parses right, expects `)`, with
                // no comma-list loop at all, so `MOD(a, b, c)`/`MOD(a)`
                // are both genuine `ParseError`s here too. Caught before
                // this project's own general reserved-keyword-function-
                // call fallback (below) could otherwise wrongly desugar
                // it to a plain `Expr::Func` call, which restores subtly
                // differently.
                "MOD" => {
                    self.bump();
                    self.expect_op("(")?;
                    let left = self.parse_expr(prec::NONE)?;
                    self.expect_op(",")?;
                    let right = self.parse_expr(prec::NONE)?;
                    self.expect_op(")")?;
                    Ok(Expr::Binary(BinaryOp::Mod, Box::new(left), Box::new(right)))
                }
                // `INSERT(str, pos, len, newstr)` — the string-replacement
                // scalar function, sharing its own keyword with the
                // `INSERT` statement. Real TiDB's own hand-written parser
                // (`pkg/parser/expr_func_parser.go`'s
                // `parseKeywordFuncCall`) renames it to `insert_func` at
                // parse time (confirmed via `godump restore`), the SAME
                // "desugars to a renamed `Expr::Func` call" pattern
                // `CHAR`/`CHAR_FUNC` already established. `IF`/`COALESCE`
                // get the SAME special routing in real TiDB but keep
                // their own name unchanged — already covered by
                // `is_scalar_kw_func` above, needing no rename here.
                "INSERT" => {
                    self.bump();
                    self.expect_op("(")?;
                    let mut args = Vec::new();
                    if !self.is_op(")") {
                        args.push(self.parse_expr(prec::NONE)?);
                        while self.is_op(",") {
                            self.bump();
                            args.push(self.parse_expr(prec::NONE)?);
                        }
                    }
                    self.expect_op(")")?;
                    Ok(Expr::Func {
                        name: "INSERT_FUNC".to_string(),
                        args,
                        origin_position: t.offset,
                    })
                }
                // `DATE`/`TIME`/`TIMESTAMP 'literal'` — an ODBC-style typed
                // literal, ONLY recognized when the token immediately
                // following the keyword is a string literal (confirmed via
                // `godump restore`: read directly from
                // `pkg/parser/expr_prefix_parser.go`'s
                // `parsePrefixTimeLiteral`, `if p.peekN(1).Tp ==
                // stringLit { ... } return p.parseIdentOrFuncCall()`) —
                // otherwise these are non-reserved keywords like any
                // other, falling through to the SAME generic
                // `parse_ident_or_func` the `_ if !is_clause_keyword(...)` arm
                // below already uses for a bare column reference (`SELECT
                // date FROM t`) or a scalar function call (`DATE(expr)`).
                // See `tidb_ast::CastStyle::DateLiteral`'s own doc for why
                // evaluation is deliberately `Unsupported` here rather
                // than reusing `CAST(... AS DATE)`'s existing logic.
                "DATE" if self.peek_n(1).kind == TokenKind::Str => {
                    self.parse_typed_literal(CastStyle::DateLiteral, CastType::Date)
                }
                "TIME" if self.peek_n(1).kind == TokenKind::Str => {
                    self.parse_typed_literal(CastStyle::TimeLiteral, CastType::Time { fsp: None })
                }
                "TIMESTAMP" if self.peek_n(1).kind == TokenKind::Str => self.parse_typed_literal(
                    CastStyle::TimestampLiteral,
                    CastType::DateTime { fsp: None },
                ),
                // ANY keyword — reserved or not — that does not introduce a
                // clause is a real MySQL/TiDB identifier here: a bare column
                // reference, or a (possibly user-defined) function call if
                // immediately followed by `(`. Mirrors real TiDB's own
                // hand-written parser EXACTLY: `pkg/parser/
                // expr_prefix_parser.go`'s `parsePrefixKeywordExpr`, its own
                // final fallback — `if tok.Tp >= identifier &&
                // !isReservedClauseKeyword(tok.Tp) { if p.peekN(1).Tp == '('
                // { ...function call... } else { ...column ref... } }` — is a
                // SINGLE check gated only on `isReservedClauseKeyword` (13
                // keywords: FROM/WHERE/GROUP/ORDER/LIMIT/HAVING/UNION/INTO/
                // FOR/LOCK/SELECT/SET/ON), never on the much larger
                // `IsReserved` (233 keywords) used elsewhere for alias/CTE
                // admission. Confirmed via `godump restore`: `SELECT rows
                // FROM t` and `SELECT database.table.column` both parse in
                // real TiDB even though `ROWS`/`DATABASE` are reserved —
                // gating this arm on `is_reserved` (as a prior revision did)
                // wrongly rejects ~220 reserved-but-not-clause keywords used
                // bare, which is why it was reverted (see
                // `rust/docs/parser-lexer-divergence.md` findings #4/#5).
                // This single arm subsumes the old two-arm split (a
                // `!is_reserved` bare-column arm plus a separate
                // `peek == "(" && !is_clause_keyword` function-call arm):
                // `parse_ident_or_func` already dispatches to
                // `parse_named_func` when `(` follows, so the two arms
                // always agreed on RESERVED-but-not-clause keywords like
                // `REPEAT(...)`/`REPLACE(...)` (real MySQL string functions
                // that share a name with a reserved keyword) and only
                // disagreed on the bare (no-paren) case Go actually allows.
                _ if !is_clause_keyword(&t.text) => self.parse_ident_or_func(),
                _ => Err(self.err_here("unsupported keyword in expression")),
            },
            TokenKind::Op => match t.text.as_str() {
                "(" => {
                    // `(SELECT ...)` / `(WITH ... SELECT ...)` is a scalar
                    // subquery; otherwise a parenthesized expression OR —
                    // if a comma follows the first element — the SAME
                    // bare row-constructor shape `parse_row_constructor`'s
                    // own `ROW(...)` form builds (see `tidb_ast::Expr::Row`'s
                    // own doc: `(1)` stays `Paren`, `(1,2)` becomes `Row`,
                    // both source syntaxes restore identically to
                    // `ROW(...)`). The `WITH` alternative here mirrors
                    // `EXISTS`/`ANY`/`SOME`/`ALL`'s own subquery position,
                    // which needed no lookahead widening at all (they
                    // dispatch to `parse_query_subquery` unconditionally,
                    // no disambiguation against a competing shape) — this
                    // position and `IN`'s own (below) are the only two
                    // that need this SAME one-token lookahead widened,
                    // confirmed via a direct probe before this was added:
                    // `SELECT (WITH q AS (...) SELECT ...)` and `... IN
                    // (WITH ... SELECT ...)` were genuine `ParseError`s.
                    if self.is_query_start_at(1) {
                        // Scalar subqueries use the complete query envelope,
                        // just like `IN`/`EXISTS`: a top-level UNION body is
                        // valid here in TiDB's source grammar and must not be
                        // prematurely narrowed to `SelectStmt`.
                        let sub = self.parse_query_subquery()?;
                        Ok(Expr::Subquery(sub))
                    } else {
                        self.bump();
                        let e = self.parse_expr(prec::NONE)?;
                        if matches!(e, Expr::Interval { .. }) {
                            return Err(self.err_here("INTERVAL is not a standalone expression"));
                        }
                        if self.is_op(",") {
                            let mut values = vec![e];
                            while self.is_op(",") {
                                self.bump();
                                let value = self.parse_expr(prec::NONE)?;
                                if matches!(value, Expr::Interval { .. }) {
                                    return Err(
                                        self.err_here("INTERVAL is not a standalone expression")
                                    );
                                }
                                values.push(value);
                            }
                            self.expect_op(")")?;
                            Ok(Expr::Row(values))
                        } else {
                            self.expect_op(")")?;
                            Ok(Expr::Paren(Box::new(e)))
                        }
                    }
                }
                "{" => self.parse_odbc_escape(),
                "+" => self.unary(UnaryOp::Plus),
                "-" => self.unary(UnaryOp::Minus),
                "~" => self.unary(UnaryOp::BitNeg),
                "!" => self.unary(UnaryOp::Not),
                _ => Err(self.err_here("unexpected operator in expression")),
            },
            TokenKind::Ident => self.parse_ident_or_func(),
            TokenKind::UserVar => {
                self.bump();
                let var = self
                    .parse_variable(&t.text)
                    .ok_or_else(|| self.err_here("malformed variable"))?;
                // `:=` following ANY variable atom is an inline
                // assignment expression, ALWAYS targeting a plain user
                // variable by its own bare name regardless of whether
                // the atom itself was written `@name` or `@@[scope.]name`
                // — see `tidb_ast::Expr::Assign`'s own doc for the exact
                // quirk this replicates. `value` is parsed at the LOWEST
                // precedence (`prec::NONE`), matching real TiDB's own
                // `p.parseExpression(precNone)`, so a further nested
                // `:=` naturally chains right-associatively.
                if self.is_op(":=") {
                    self.bump();
                    let name = match var {
                        Expr::UserVar(name) | Expr::SysVar { name, .. } => name,
                        _ => unreachable!("parse_variable only ever returns UserVar or SysVar"),
                    };
                    let value = self.parse_expr(prec::NONE)?;
                    return Ok(Expr::Assign {
                        name,
                        value: Box::new(value),
                    });
                }
                Ok(var)
            }
            _ => Err(self.err_here("unexpected token in expression")),
        }
    }

    /// Unary `+ - ~ !` bind at `precUnary` (see `parseUnaryOp`).
    fn unary(&mut self, op: UnaryOp) -> PResult<Expr> {
        self.bump();
        let e = self.parse_expr(prec::UNARY)?;
        Ok(Expr::Unary(op, Box::new(e)))
    }
}

/// Whether `name` is one of the small set of clause-introducing reserved
/// keywords that must NEVER be consumed as an identifier or function name in
/// expression context, even directly followed by `(` — see `parse_prefix`'s
/// own final reserved-keyword-function-call fallback for why this list
/// exists at all. Mirrors real TiDB's own `pkg/parser/
/// expr_prefix_parser.go`'s `isReservedClauseKeyword` exactly (confirmed via
/// `godump restore`: `SELECT LOCK()`/`SELECT FROM()`/etc. are all genuine
/// `ParseError`s in real TiDB too, not merely unrecognized-as-a-function).
fn is_clause_keyword(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "FROM"
            | "WHERE"
            | "GROUP"
            | "ORDER"
            | "LIMIT"
            | "HAVING"
            | "UNION"
            | "INTO"
            | "FOR"
            | "LOCK"
            | "SELECT"
            | "SET"
            | "ON"
    )
}

/// Normalizes a hex literal token (`0xFF`, `x'1a2b'`) to lowercase, even-length
/// hex digits, matching the Go AST's `x'..'` restore.
fn normalize_hex(text: &str) -> String {
    let digits = if let Some(rest) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        rest.to_string()
    } else {
        // x'..' / X'..'
        text[1..].trim_matches('\'').to_string()
    };
    let mut lower = digits.to_ascii_lowercase();
    if lower.len() % 2 != 0 {
        lower.insert(0, '0');
    }
    lower
}

/// Normalizes a bit literal token (`0b101`, `b'0101'`) to its leading-zero-
/// stripped bit digits, matching the Go AST's `b'..'` restore. A genuinely
/// EMPTY quoted literal (`b''`/`B''` — confirmed via `godump restore` to be
/// real, valid grammar, lexed as a `BitLit` with a zero-length digit span
/// between the quotes; the bare `0b` form can never reach here empty, since
/// the lexer only emits `BitLit` for `0b` when at least one binary digit
/// follows, otherwise treating the whole span as an identifier) stays empty
/// (`b''`) rather than being folded into `b'0'` — those are DIFFERENT values
/// in real TiDB (confirmed via `goeval`: `LENGTH(b'')` is `0`, `LENGTH(b'0')`
/// is `1`, even though both evaluate to `0` under arithmetic), so only a
/// digit string that had leading zeros STRIPPED DOWN TO empty (`0`, `00`,
/// ...) — genuinely non-empty to begin with — falls back to `"0"`.
fn normalize_bit(text: &str) -> String {
    let digits = if let Some(rest) = text.strip_prefix("0b").or_else(|| text.strip_prefix("0B")) {
        rest.to_string()
    } else {
        // b'..' / B'..'
        text[1..].trim_matches('\'').to_string()
    };
    if digits.is_empty() {
        return digits;
    }
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

impl Parser {
    /// Parses a variable token's text (`@name`, `@@name`, `@@scope.name`) into the
    /// AST. Returns `None` for shapes this phase does not model (e.g. quoted names).
    fn parse_variable(&self, text: &str) -> Option<Expr> {
        if let Some(rest) = text.strip_prefix("@@") {
            // System variable, with an optional GLOBAL/SESSION/LOCAL scope.
            if let Some((prefix, name)) = rest.split_once('.') {
                let scope = match prefix.to_ascii_uppercase().as_str() {
                    "GLOBAL" => Some(SysVarScope::Global),
                    "SESSION" | "LOCAL" => Some(SysVarScope::Session),
                    "INSTANCE" => Some(SysVarScope::Instance),
                    _ => None,
                };
                if scope.is_some() {
                    return Some(Expr::SysVar {
                        scope,
                        name: self.decode_variable_name(name).to_ascii_lowercase(),
                    });
                }
                // A dotted name with an unknown prefix is not a scope; keep it whole.
                return Some(Expr::SysVar {
                    scope: None,
                    name: self.decode_variable_name(rest).to_ascii_lowercase(),
                });
            }
            return Some(Expr::SysVar {
                scope: None,
                name: self.decode_variable_name(rest).to_ascii_lowercase(),
            });
        }
        let name = text.strip_prefix('@')?;
        Some(Expr::UserVar(self.decode_variable_name(name)))
    }

    fn decode_variable_name(&self, raw: &str) -> String {
        if matches!(raw.as_bytes().first(), Some(b'\'') | Some(b'"')) {
            self.decode_string(raw)
        } else if raw.starts_with('`') && raw.ends_with('`') && raw.len() >= 2 {
            raw[1..raw.len() - 1].replace("``", "`")
        } else {
            raw.to_string()
        }
    }
}
