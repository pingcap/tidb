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
use tidb_lexer::{
    canonical_charset, canonical_collation, canonical_legacy_charset, is_reserved, TokenKind,
};

use crate::{decode_string, prec, PResult, Parser};

impl Parser {
    pub(crate) fn parse_expr(&mut self, min_prec: u8) -> PResult<Expr> {
        let mut left = self.parse_prefix(min_prec)?;
        loop {
            // Keyword predicates: IN / LIKE / BETWEEN / IS, and NOT-prefixed
            // predicates, all at the predicate/comparison precedence levels.
            if self.is_kw("NOT")
                && (self.is_kw_at(1, "IN")
                    || self.is_kw_at(1, "LIKE")
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
            if self.is_kw("IN")
                || self.is_kw("LIKE")
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
                    .ok_or_else(|| self.err_here("unknown collation"))?
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
                let path = Expr::String(decode_string(&self.bump().text));
                let extract = Expr::Func {
                    name: "JSON_EXTRACT".to_string(),
                    args: vec![left, path],
                };
                left = if unquote {
                    Expr::Func {
                        name: "JSON_UNQUOTE".to_string(),
                        args: vec![extract],
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
                    subquery: Box::new(subquery),
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
    /// restore`. Real TiDB ALSO rejects a PARENTHESIZED `INTERVAL`
    /// operand outright (`(INTERVAL x) + y` is a genuine `ParseError`
    /// there too, confirmed via `godump restore` — a bare `(INTERVAL
    /// ...)` isn't a valid standalone expression in real TiDB's own
    /// grammar at all) — a narrower, KNOWN divergence this project does
    /// NOT replicate: `Expr::Interval` is parsed here as an ordinary
    /// primary expression (reachable from `parse_prefix`, hence also
    /// from inside a `(...)`), so `(INTERVAL x) + y` parses successfully
    /// here as a plain `Expr::Binary` (this check deliberately does NOT
    /// unwrap `Expr::Paren` before testing for `Expr::Interval`, so a
    /// parenthesized one is intentionally left ALONE rather than
    /// desugared) rather than replicating real TiDB's rejection —
    /// accepted as out of scope since it never appeared in the
    /// real-TiDB-test-suite corpus that surfaced this whole feature.
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
            }),
            BinaryOp::Plus if right_is_interval => Ok(Expr::Func {
                name: "DATE_ADD".to_string(),
                args: vec![left, right],
            }),
            BinaryOp::Minus if left_is_interval => {
                Err(self.err_here("INTERVAL cannot be the left operand of -"))
            }
            BinaryOp::Minus if right_is_interval => Ok(Expr::Func {
                name: "DATE_SUB".to_string(),
                args: vec![left, right],
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

    fn parse_prefix(&mut self, min_prec: u8) -> PResult<Expr> {
        let t = self.peek().clone();
        match t.kind {
            TokenKind::Op if t.text == "?" => {
                self.bump();
                Ok(Expr::ParamMarker {
                    position: self.next_param_marker_position(),
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
                let mut val = decode_string(&t.text);
                while self.peek().kind == TokenKind::Str {
                    let next = self.bump();
                    val.push_str(&decode_string(&next.text));
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
                match self.peek().kind {
                    // Go's UNDERSCORE_CHARSET production accepts a string,
                    // hex, or bit literal after the introducer.  Hex/bit
                    // payloads deliberately use the ordinary literal AST:
                    // TiDB's ValueExpr restore drops the charset wrapper and
                    // emits x'...' / b'...' (the same canonical form for
                    // `_binary 0x...` and `_utf8mb4 0x...`).
                    TokenKind::Str => {
                        let value = decode_string(&self.bump().text);
                        if charset == "utf8mb4" {
                            Ok(Expr::String(value))
                        } else {
                            Ok(Expr::CharsetString {
                                charset: charset.to_ascii_uppercase(),
                                value,
                            })
                        }
                    }
                    TokenKind::HexLit => {
                        let token = self.bump();
                        Ok(Expr::Hex(normalize_hex(&token.text)))
                    }
                    TokenKind::BitLit => {
                        let token = self.bump();
                        Ok(Expr::Bit(normalize_bit(&token.text)))
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
                Ok(Expr::Func { name, args: vec![] })
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
                    Ok(Expr::Func { name, args: vec![] })
                }
                "NOT" => {
                    if min_prec > prec::NOT {
                        return Err(self.err_here("NOT not allowed at this precedence"));
                    }
                    self.bump();
                    let e = self.parse_expr(prec::NOT)?;
                    if let Expr::Exists { subquery, not } = e {
                        Ok(Expr::Exists {
                            subquery,
                            not: !not,
                        })
                    } else {
                        Ok(Expr::Unary(UnaryOp::NotKeyword, Box::new(e)))
                    }
                }
                // `[NOT] EXISTS (subquery)`; `NOT` is handled by the unary path,
                // so restore of `NOT EXISTS (...)` matches.
                "EXISTS" => {
                    self.bump();
                    let subquery = self.parse_query_subquery()?;
                    Ok(Expr::Exists {
                        subquery: Box::new(subquery),
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
                // shared `_ if !is_reserved(...) => parse_ident_or_func()`
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
                // the shared `_ if !is_reserved(...)` arm below) because,
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
                // `parse_ident_or_func` the `_ if !is_reserved(...)` arm
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
                // A NON-RESERVED keyword not otherwise recognized above is a
                // real MySQL/TiDB identifier here — a bare column reference,
                // or a (possibly user-defined) function call if immediately
                // followed by `(` — confirmed via `godump restore`: `SELECT
                // uuid FROM t` parses (`UUID` is a non-reserved keyword),
                // even though it isn't a recognized aggregate/window/scalar
                // function name. A genuinely RESERVED keyword (`SELECT`,
                // `WHERE`, ...) can never validly start an expression this
                // way, so it stays a `ParseError`.
                _ if !is_reserved(&t.text) => self.parse_ident_or_func(),
                // A RESERVED keyword immediately followed by `(` is ALSO a
                // function call, unless it's one of a small set of
                // clause-introducing keywords that must never be consumed
                // as an identifier/function name (`SELECT FROM(1)` can't
                // mean anything). Read directly from real TiDB's own
                // hand-written parser (`pkg/parser/
                // expr_prefix_parser.go`'s `parsePrefixKeywordExpr`, its
                // own final fallback): `if tok.Tp >= identifier &&
                // !isReservedClauseKeyword(tok.Tp) { if
                // p.peekN(1).Tp == '(' { ...function call... } }` — this
                // rule is NOT gated on the keyword being individually
                // recognized as a scalar-function name at all (unlike
                // `is_scalar_kw_func` above, this project's own earlier,
                // narrower, one-keyword-at-a-time allowlist), which is
                // exactly why `REPEAT(...)`/`REPLACE(...)` (real MySQL
                // string functions that happen to share a name with a
                // reserved keyword, confirmed via `godump restore`) were
                // genuine `ParseError`s here before this arm existed,
                // despite plainly parsing in real TiDB. New reserved
                // keywords that are ALSO real function names should need
                // NO further individual entries anywhere once they reach
                // this point — this arm subsumes that whole class of gap.
                _ if self.peek_n(1).kind == TokenKind::Op
                    && self.peek_n(1).text == "("
                    && !is_clause_keyword(&t.text) =>
                {
                    self.parse_named_func()
                }
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
                    if self.is_kw_at(1, "SELECT") || self.is_kw_at(1, "WITH") {
                        // Scalar subqueries use the complete query envelope,
                        // just like `IN`/`EXISTS`: a top-level UNION body is
                        // valid here in TiDB's source grammar and must not be
                        // prematurely narrowed to `SelectStmt`.
                        let sub = self.parse_query_subquery()?;
                        Ok(Expr::Subquery(Box::new(sub)))
                    } else {
                        self.bump();
                        let e = self.parse_expr(prec::NONE)?;
                        if self.is_op(",") {
                            let mut values = vec![e];
                            while self.is_op(",") {
                                self.bump();
                                values.push(self.parse_expr(prec::NONE)?);
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
                let var =
                    parse_variable(&t.text).ok_or_else(|| self.err_here("malformed variable"))?;
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

    /// Parses `INTERVAL value unit`. The unit keyword naturally stops the
    /// value's own expression parse (it isn't a recognized infix operator),
    /// so no special precedence handling is needed to separate the two.
    fn parse_interval(&mut self) -> PResult<Expr> {
        self.bump(); // INTERVAL
        let value = self.parse_expr(prec::NONE)?;
        if self.peek().kind != TokenKind::Keyword {
            return Err(self.err_here("expected an INTERVAL unit"));
        }
        let unit = self.bump().text.to_ascii_uppercase();
        Ok(Expr::Interval {
            value: Box::new(value),
            unit,
        })
    }

    /// Parses `EXTRACT(unit FROM expr)` — the unit keyword comes FIRST
    /// (unlike `INTERVAL value unit`'s own order), so it's read directly
    /// off the token stream rather than via `parse_expr`.
    fn parse_extract(&mut self) -> PResult<Expr> {
        self.bump(); // EXTRACT
        self.expect_op("(")?;
        if self.peek().kind != TokenKind::Keyword {
            return Err(self.err_here("expected an EXTRACT unit"));
        }
        let unit = self.bump().text.to_ascii_uppercase();
        self.expect_kw("FROM")?;
        let value = self.parse_expr(prec::NONE)?;
        self.expect_op(")")?;
        Ok(Expr::Extract {
            unit,
            value: Box::new(value),
        })
    }

    /// Reads a bare unit keyword token (same simple "any keyword, just
    /// capture and uppercase the text" convention `INTERVAL`/`EXTRACT`
    /// already use — see their own docs).
    fn parse_bare_time_unit(&mut self) -> PResult<String> {
        if self.peek().kind != TokenKind::Keyword {
            return Err(self.err_here("expected a time unit"));
        }
        Ok(self.bump().text.to_ascii_uppercase())
    }

    /// `TIMESTAMPADD(unit, interval, datetime_expr)` — see
    /// `tidb_ast::Expr::TimestampAdd`'s own doc for why `unit` is a
    /// dedicated field, not an ordinary parsed argument expression.
    fn parse_timestamp_add(&mut self) -> PResult<Expr> {
        self.bump(); // TIMESTAMPADD
        self.expect_op("(")?;
        let unit = self.parse_bare_time_unit()?;
        self.expect_op(",")?;
        let interval = self.parse_expr(prec::NONE)?;
        self.expect_op(",")?;
        let expr = self.parse_expr(prec::NONE)?;
        self.expect_op(")")?;
        Ok(Expr::TimestampAdd {
            unit,
            interval: Box::new(interval),
            expr: Box::new(expr),
        })
    }

    /// `TIMESTAMPDIFF(unit, expr1, expr2)` — see
    /// `tidb_ast::Expr::TimestampDiff`'s own doc.
    fn parse_timestamp_diff(&mut self) -> PResult<Expr> {
        self.bump(); // TIMESTAMPDIFF
        self.expect_op("(")?;
        let unit = self.parse_bare_time_unit()?;
        self.expect_op(",")?;
        let expr1 = self.parse_expr(prec::NONE)?;
        self.expect_op(",")?;
        let expr2 = self.parse_expr(prec::NONE)?;
        self.expect_op(")")?;
        Ok(Expr::TimestampDiff {
            unit,
            expr1: Box::new(expr1),
            expr2: Box::new(expr2),
        })
    }

    /// `ADDDATE`/`SUBDATE(date, interval_or_days)` — a genuinely dual-form
    /// MySQL grammar: the second argument is EITHER an explicit `INTERVAL
    /// n unit` (identical to `DATE_ADD`/`DATE_SUB`'s own only form) OR a
    /// bare number, silently meaning `INTERVAL n DAY` — confirmed via
    /// `godump restore`: `ADDDATE(d, -1)` restores as `ADDDATE(d, INTERVAL
    /// -1 DAY)`, and the function NAME as written is preserved (never
    /// rewritten to `DATE_ADD`, unlike this crate's own `date_expr ±
    /// INTERVAL amount unit` desugaring). `DATE_ADD`/`DATE_SUB` do NOT
    /// have this bare-number shorthand (a bare number there is a genuine
    /// `ParseError`, confirmed via `godump restore`). Both forms restore
    /// as an ordinary 2-arg `Expr::Func` call with the second argument
    /// NORMALIZED to always be `Expr::Interval` — `tidb_expr::func`'s own
    /// `DATE_ADD`/`DATE_SUB` evaluation dispatch already expects exactly
    /// this shape (evaluation itself stays scoped to those two names,
    /// unaffected here).
    fn parse_adddate_or_subdate(&mut self, name: &str) -> PResult<Expr> {
        self.bump(); // ADDDATE / SUBDATE
        self.expect_op("(")?;
        let date = self.parse_expr(prec::NONE)?;
        self.expect_op(",")?;
        let amount = self.parse_expr(prec::NONE)?;
        self.expect_op(")")?;
        let interval = match amount {
            Expr::Interval { .. } => amount,
            other => Expr::Interval {
                value: Box::new(other),
                unit: "DAY".to_string(),
            },
        };
        Ok(Expr::Func {
            name: name.to_string(),
            args: vec![date, interval],
        })
    }

    /// `GET_FORMAT(DATE|TIME|DATETIME|TIMESTAMP, format_expr)` — see
    /// `tidb_ast::Expr::GetFormat`'s own doc for why `TIMESTAMP` collapses
    /// into the SAME selector as `DATETIME`.
    fn parse_get_format(&mut self) -> PResult<Expr> {
        self.bump(); // GET_FORMAT
        self.expect_op("(")?;
        let selector = if self.is_kw("DATE") {
            self.bump();
            GetFormatSelector::Date
        } else if self.is_kw("TIME") {
            self.bump();
            GetFormatSelector::Time
        } else if self.is_kw("DATETIME") || self.is_kw("TIMESTAMP") {
            self.bump();
            GetFormatSelector::Datetime
        } else {
            return Err(self.err_here("expected DATE, TIME, DATETIME, or TIMESTAMP"));
        };
        self.expect_op(",")?;
        let expr = self.parse_expr(prec::NONE)?;
        self.expect_op(")")?;
        Ok(Expr::GetFormat {
            selector,
            expr: Box::new(expr),
        })
    }

    /// `CASE [value] (WHEN cond THEN result)+ [ELSE result] END`. At least
    /// one `WHEN` clause is required — real MySQL rejects `CASE END`/
    /// `CASE 1 END` at parse time (confirmed via `godump restore`, not
    /// assumed), so this errors the same way rather than accepting an
    /// empty `when_clauses` the evaluator would need to special-case.
    fn parse_case(&mut self) -> PResult<Expr> {
        self.bump(); // CASE
        let value = if self.is_kw("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_expr(prec::NONE)?))
        };
        let mut when_clauses = Vec::new();
        while self.is_kw("WHEN") {
            self.bump();
            let cond = self.parse_expr(prec::NONE)?;
            self.expect_kw("THEN")?;
            let result = self.parse_expr(prec::NONE)?;
            when_clauses.push((cond, result));
        }
        if when_clauses.is_empty() {
            return Err(self.err_here("CASE requires at least one WHEN clause"));
        }
        let else_clause = if self.is_kw("ELSE") {
            self.bump();
            Some(Box::new(self.parse_expr(prec::NONE)?))
        } else {
            None
        };
        self.expect_kw("END")?;
        Ok(Expr::Case {
            value,
            when_clauses,
            else_clause,
        })
    }

    /// `ROW(expr, expr, ...)` — an explicit row/tuple constructor, requiring
    /// 2+ elements (confirmed via `godump restore`: `ROW(1)`/`ROW()` are
    /// both genuine `ParseError`s, read from
    /// `pkg/parser/expr_prefix_parser.go`'s `parsePrefixKeywordExpr`,
    /// `case row:`). See [`tidb_ast::Expr::Row`]'s own doc for why a bare
    /// `(expr, expr, ...)` (no `ROW` keyword) builds the identical node —
    /// that shape is handled by the primary `"("` case in `parse_prefix`,
    /// not here.
    fn parse_row_constructor(&mut self) -> PResult<Expr> {
        self.bump(); // ROW
        self.expect_op("(")?;
        let mut values = vec![self.parse_expr(prec::NONE)?];
        while self.is_op(",") {
            self.bump();
            values.push(self.parse_expr(prec::NONE)?);
        }
        self.expect_op(")")?;
        if values.len() < 2 {
            return Err(self.err_here("ROW constructor requires at least 2 elements"));
        }
        Ok(Expr::Row(values))
    }

    /// `DEFAULT(col)` — the column's own `DEFAULT` value. `col` is a
    /// DOTTED COLUMN-NAME PATH specifically,
    /// NOT an arbitrary expression (confirmed via `godump restore`:
    /// `DEFAULT(1+1)`/`DEFAULT()`/`DEFAULT(a,b)` are all genuine
    /// `ParseError`s) — reuses [`Parser::parse_name_path`] rather than
    /// [`Parser::parse_expr`]. A bare `DEFAULT` (no parens) is ALSO a
    /// genuine `ParseError` in this general-expression context (real
    /// TiDB's own grammar only allows it in `VALUES`/`SET`-assignment
    /// positions, a separate, unmodelled scope), so this function is
    /// dispatched to UNCONDITIONALLY for the `DEFAULT` keyword, not
    /// gated on a lookahead.
    fn parse_default_expr(&mut self) -> PResult<Expr> {
        self.bump(); // DEFAULT
        self.expect_op("(")?;
        let path = self.parse_name_path()?;
        self.expect_op(")")?;
        Ok(Expr::Default(Some(path)))
    }

    /// `CHAR(expr, ...)` / `CHAR(expr, ... USING charset)` — desugars to a
    /// plain [`Expr::Func`] call (`name: "CHAR_FUNC"`) rather than a
    /// dedicated AST node: real TiDB's own hand-written parser
    /// (`pkg/parser/expr_func_parser.go`'s `parseCharFuncCall`) ALWAYS
    /// renames the function to `char_func` and appends one extra
    /// argument — the charset name as a string literal if `USING` was
    /// given, else a `NULL` literal — confirmed via `godump restore`:
    /// `CHAR(97, 100)` restores as `CHAR_FUNC(97, 100, NULL)`, `CHAR(1
    /// USING gbk)` as `CHAR_FUNC(1, 'gbk')`. `CHAR()` with ZERO
    /// arguments is a real, EARLIER short-circuit in real TiDB's own
    /// `parseScalarFuncCall` (an empty-parens check that runs BEFORE
    /// dispatching to any function-specific parser) — it restores as
    /// bare `CHAR()`, the name UNCHANGED and no `NULL` sentinel
    /// appended, so that case is handled separately here, before any
    /// argument parsing. `CHAR_FUNC(...)` typed directly by a user (the
    /// desugared name itself) is NOT a lexer keyword and needs no
    /// dispatch arm at all — it already reaches the generic
    /// [`Parser::parse_named_func`] path via [`Parser::parse_ident_or_func`],
    /// which appends no sentinel (confirmed via `godump restore`).
    /// `USING` accepts the same identifier/keyword or quoted-string token
    /// shape as Go's `parseCharFuncCall`, then validates/canonicalizes it
    /// through the shared charset registry (`GetCharsetInfo` in Go).
    fn parse_char_func(&mut self) -> PResult<Expr> {
        self.bump(); // CHAR
        self.expect_op("(")?;
        if self.is_op(")") {
            self.bump();
            return Ok(Expr::Func {
                name: "CHAR".to_string(),
                args: vec![],
            });
        }
        let mut args = vec![self.parse_expr(prec::NONE)?];
        while self.is_op(",") {
            self.bump();
            args.push(self.parse_expr(prec::NONE)?);
        }
        if self.is_kw("USING") {
            self.bump();
            let raw = self.parse_using_charset_name()?;
            let charset =
                canonical_charset(&raw).ok_or_else(|| self.err_here("unknown character set"))?;
            args.push(Expr::RawString(charset.to_string()));
        } else {
            args.push(Expr::Null);
        }
        self.expect_op(")")?;
        Ok(Expr::Func {
            name: "CHAR_FUNC".to_string(),
            args,
        })
    }

    /// `WEIGHT_STRING(expr [AS {CHAR|CHARACTER|BINARY}(len)])` — read
    /// directly from real TiDB's own hand-written parser
    /// (`pkg/parser/expr_func_parser.go`'s `parseWeightStringFuncCall`).
    /// `CHARACTER` is a real synonym for `CHAR` here, collapsing to the
    /// SAME `WeightStringType::Char` — see `tidb_ast::Expr::WeightString`'s
    /// own doc for the exact restore shape this produces. `len` is a
    /// plain integer literal with no range check beyond fitting `u64`
    /// (real TiDB's own grammar only accepts `LengthNum = intLit`, no
    /// general expression here).
    fn parse_weight_string(&mut self) -> PResult<Expr> {
        self.bump(); // WEIGHT_STRING
        self.expect_op("(")?;
        let expr = self.parse_expr(prec::NONE)?;
        let as_type = if self.is_kw("AS") {
            self.bump();
            let ty = if self.is_kw("CHAR") || self.is_kw("CHARACTER") {
                self.bump();
                WeightStringType::Char
            } else if self.is_kw("BINARY") {
                self.bump();
                WeightStringType::Binary
            } else {
                return Err(self.err_here("expected CHAR or BINARY"));
            };
            self.expect_op("(")?;
            if self.peek().kind != TokenKind::IntLit {
                return Err(self.err_here("expected integer type argument"));
            }
            let len: u64 = self
                .bump()
                .text
                .parse()
                .map_err(|_| self.err_here("invalid integer type argument"))?;
            self.expect_op(")")?;
            Some((ty, len))
        } else {
            None
        };
        self.expect_op(")")?;
        Ok(Expr::WeightString {
            expr: Box::new(expr),
            as_type,
        })
    }

    /// `POSITION(substr IN str)` — read directly from real TiDB's own
    /// hand-written parser (`pkg/parser/expr_cast_parser.go`'s
    /// `parsePositionFunc`). `substr` parses at `prec::PREDICATE + 1` so
    /// the mandatory `IN` keyword is never swallowed as the SQL `IN`
    /// predicate operator while parsing `substr` itself — see
    /// `tidb_ast::Expr::Position`'s own doc.
    fn parse_position_func(&mut self) -> PResult<Expr> {
        self.bump(); // POSITION
        self.expect_op("(")?;
        let substr = self.parse_expr(prec::PREDICATE + 1)?;
        self.expect_kw("IN")?;
        let str = self.parse_expr(prec::NONE)?;
        self.expect_op(")")?;
        Ok(Expr::Position {
            substr: Box::new(substr),
            str: Box::new(str),
        })
    }

    /// `SUBSTR`/`SUBSTRING(str, pos[, len])` / `SUBSTR`/`SUBSTRING(str
    /// FROM pos [FOR len])` — read directly from real TiDB's own
    /// hand-written parser (`pkg/parser/expr_cast_parser.go`'s
    /// `parseSubstringFunc`): the `FROM`-based form DESUGARS at parse
    /// time into the SAME comma-separated-args `Expr::Func` shape the
    /// ordinary call already produces (confirmed via `godump restore`:
    /// `SUBSTR('foobarbar' FROM 4)` restores as `SUBSTR('foobarbar', 4)`,
    /// byte-identical to the comma form) — no dedicated AST node needed,
    /// following this session's own established "desugars to an
    /// existing AST shape" pattern. The plain comma form was ALREADY
    /// reachable via the generic non-reserved-keyword function-call
    /// path before this function existed; this REPLACES that generic
    /// dispatch for `SUBSTR`/`SUBSTRING` specifically so the `FROM`/`FOR`
    /// alternative is recognized too, in the SAME function (matching
    /// real TiDB's own `parseSubstringFunc`, which owns both forms).
    /// Whichever length-separator was used for `pos` (`FROM` vs `,`)
    /// MUST be reused for the optional `len` (`FOR` after `FROM`, `,`
    /// after `,`) — real TiDB's own grammar never mixes the two within
    /// one call (confirmed by reading `parseSubstringFunc` directly: the
    /// length separator is chosen from `usesFor`, not re-detected).
    /// `name` is the ORIGINAL keyword spelling, canonically uppercased —
    /// `SUBSTR` stays `SUBSTR`, `SUBSTRING` stays `SUBSTRING` in restore
    /// (confirmed via `godump restore`, NOT normalized to one name).
    fn parse_substring_func(&mut self, name: &str) -> PResult<Expr> {
        self.bump(); // SUBSTR / SUBSTRING
        self.expect_op("(")?;
        let str = self.parse_expr(prec::NONE)?;
        let uses_for = if self.is_kw("FROM") {
            self.bump();
            true
        } else {
            self.expect_op(",")?;
            false
        };
        let pos = self.parse_expr(prec::NONE)?;
        let mut args = vec![str, pos];
        let has_len = if uses_for {
            self.is_kw("FOR")
        } else {
            self.is_op(",")
        };
        if has_len {
            self.bump();
            args.push(self.parse_expr(prec::NONE)?);
        }
        self.expect_op(")")?;
        Ok(Expr::Func {
            name: name.to_string(),
            args,
        })
    }

    /// `TRIM(expr)` / `TRIM([remstr] FROM expr)` / `TRIM(direction
    /// [remstr] FROM expr)` — read directly from real TiDB's own
    /// hand-written parser (`pkg/parser/expr_cast_parser.go`'s
    /// `parseTrimFunc`), see `tidb_ast::Expr::Trim`'s own doc for the
    /// exact restore rules this must produce. `direction FROM expr`
    /// (no `remstr` written) defaults `remstr` to a single-space STRING
    /// literal — confirmed via `godump restore` to restore WITH the
    /// usual `_UTF8MB4` charset introducer (`TRIM(BOTH FROM x)` restores
    /// as `` TRIM(BOTH _UTF8MB4' ' FROM x) ``), so a plain
    /// [`Expr::String`], not [`Expr::RawString`].
    fn parse_trim(&mut self) -> PResult<Expr> {
        self.bump(); // TRIM
        self.expect_op("(")?;
        let direction = if self.is_kw("BOTH") {
            self.bump();
            Some(TrimDirection::Both)
        } else if self.is_kw("LEADING") {
            self.bump();
            Some(TrimDirection::Leading)
        } else if self.is_kw("TRAILING") {
            self.bump();
            Some(TrimDirection::Trailing)
        } else {
            None
        };
        if let Some(direction) = direction {
            let remstr = if self.is_kw("FROM") {
                Expr::String(" ".to_string())
            } else {
                self.parse_expr(prec::NONE)?
            };
            self.expect_kw("FROM")?;
            let expr = self.parse_expr(prec::NONE)?;
            self.expect_op(")")?;
            return Ok(Expr::Trim {
                expr: Box::new(expr),
                remstr: Some(Box::new(remstr)),
                direction: Some(direction),
            });
        }
        let first = self.parse_expr(prec::NONE)?;
        if self.is_kw("FROM") {
            self.bump();
            let expr = self.parse_expr(prec::NONE)?;
            self.expect_op(")")?;
            return Ok(Expr::Trim {
                expr: Box::new(expr),
                remstr: Some(Box::new(first)),
                direction: None,
            });
        }
        self.expect_op(")")?;
        Ok(Expr::Trim {
            expr: Box::new(first),
            remstr: None,
            direction: None,
        })
    }

    /// `DATE`/`TIME`/`TIMESTAMP 'literal'` — an ODBC-style typed literal,
    /// only ever called once the caller has already confirmed the
    /// keyword is immediately followed by a string-literal token. See
    /// `tidb_ast::CastStyle::DateLiteral`'s own doc for the exact shape
    /// this builds and reuses `tidb_ast::Expr::Cast` for.
    fn parse_typed_literal(&mut self, style: CastStyle, cast_type: CastType) -> PResult<Expr> {
        self.bump(); // DATE / TIME / TIMESTAMP
        let lit = self.bump();
        Ok(Expr::Cast(CastExpr {
            expr: Box::new(Expr::String(decode_string(&lit.text))),
            cast_type,
            style,
            array: false,
        }))
    }

    /// `{d 'literal'}` / `{t 'literal'}` / `{ts 'literal'}` — an ODBC
    /// escape-sequence literal, read directly from
    /// `pkg/parser/expr_parser.go`'s `parsePrefixExpr` (`case '{':` arm):
    /// the type identifier's TEXT (not its token kind) decides the shape
    /// — only the exact, case-insensitive strings `"d"`/`"t"`/`"ts"`
    /// produce a typed literal, reusing the SAME
    /// `tidb_ast::CastStyle::DateLiteral`/`TimeLiteral`/`TimestampLiteral`
    /// shape `DATE`/`TIME`/`TIMESTAMP 'literal'` already builds
    /// (confirmed via `godump restore`: both forms restore
    /// byte-identically). Real TiDB's own grammar parses a full
    /// expression here (`p.parseExpression(precNone)`), but only clears
    /// the literal's charset — the thing that makes the restore match
    /// `DATE 'literal'` exactly — when that expression is itself a bare
    /// `ValueExpr`; every real-corpus statement is a plain string
    /// literal, so (mirroring `parse_typed_literal`'s own established,
    /// narrower scope) this requires one directly rather than modelling
    /// the fully general expression case. Any OTHER type identifier
    /// (`fn`, `date`, `time`, `timestamp`, ...) is a pass-through: the
    /// braces are discarded and the inner expression alone survives —
    /// real TiDB's own `default:` arm.
    fn parse_odbc_escape(&mut self) -> PResult<Expr> {
        self.bump(); // '{'
        if !matches!(self.peek().kind, TokenKind::Ident | TokenKind::Keyword) {
            return Err(self.err_here("expected an ODBC escape type identifier"));
        }
        let typ = self.bump().text.to_ascii_lowercase();
        let typed = match typ.as_str() {
            "d" => Some((CastStyle::DateLiteral, CastType::Date)),
            "t" => Some((CastStyle::TimeLiteral, CastType::Time { fsp: None })),
            "ts" => Some((
                CastStyle::TimestampLiteral,
                CastType::DateTime { fsp: None },
            )),
            _ => None,
        };
        if let Some((style, cast_type)) = typed {
            if self.peek().kind != TokenKind::Str {
                return Err(self.err_here("expected a string literal"));
            }
            let lit = self.bump();
            self.expect_op("}")?;
            return Ok(Expr::Cast(CastExpr {
                expr: Box::new(Expr::String(decode_string(&lit.text))),
                cast_type,
                style,
                array: false,
            }));
        }
        let inner = self.parse_expr(prec::NONE)?;
        self.expect_op("}")?;
        Ok(inner)
    }

    /// `MATCH(col, ...) AGAINST(expr [search_modifier])` — read directly from
    /// real TiDB's own hand-written parser (`pkg/parser/expr_parser.go`'s
    /// `parseMatchAgainstExpr`), see `tidb_ast::Expr::MatchAgainst`'s own
    /// doc. The `AGAINST` argument parses at `prec::PREDICATE + 1` (the
    /// SAME bound `BETWEEN`'s low operand and `LIKE`/`REGEXP`'s pattern
    /// already use), so a following `IN`/`WITH` modifier keyword is left
    /// for this function's own modifier parsing rather than being
    /// swallowed as part of the `AGAINST` expression itself.
    fn parse_match_against(&mut self) -> PResult<Expr> {
        self.bump(); // MATCH
        self.expect_op("(")?;
        let mut columns = vec![self.parse_name_path()?];
        while self.is_op(",") {
            self.bump();
            columns.push(self.parse_name_path()?);
        }
        self.expect_op(")")?;
        self.expect_kw("AGAINST")?;
        self.expect_op("(")?;
        let against = self.parse_expr(prec::PREDICATE + 1)?;
        let modifier = if self.is_kw("IN") {
            self.bump();
            if self.is_kw("BOOLEAN") {
                self.bump();
                self.expect_kw("MODE")?;
                // Real TiDB rejects this specific combination at parse
                // time (confirmed by reading `parseMatchAgainstExpr`),
                // rather than silently accepting a modifier restore can't
                // represent (`MatchAgainst.Restore` would need to print
                // both suffixes, which its own real grammar never allows).
                if self.is_kw("WITH") {
                    return Err(
                        self.err_here("IN BOOLEAN MODE WITH QUERY EXPANSION is not supported")
                    );
                }
                MatchModifier::BooleanMode
            } else if self.is_kw("NATURAL") {
                self.bump();
                self.expect_kw("LANGUAGE")?;
                self.expect_kw("MODE")?;
                if self.is_kw("WITH") {
                    self.bump();
                    self.expect_kw("QUERY")?;
                    self.expect_kw("EXPANSION")?;
                    MatchModifier::QueryExpansion
                } else {
                    MatchModifier::None
                }
            } else {
                return Err(self.err_here("expected BOOLEAN or NATURAL LANGUAGE after IN"));
            }
        } else if self.is_kw("WITH") {
            self.bump();
            self.expect_kw("QUERY")?;
            self.expect_kw("EXPANSION")?;
            MatchModifier::QueryExpansion
        } else {
            MatchModifier::None
        };
        self.expect_op(")")?;
        Ok(Expr::MatchAgainst {
            columns,
            against: Box::new(against),
            modifier,
        })
    }

    /// A bare identifier is a column reference, or a function call when directly
    /// followed by `(`.
    fn parse_ident_or_func(&mut self) -> PResult<Expr> {
        // A schema-qualified GENERIC function call (`schema.func(...)`)
        // — checked FIRST, before falling through to a plain qualified
        // column reference, since `ident '.' ident` alone is otherwise
        // ambiguous with `table.column` until the 4th token (`(`) is
        // seen — read directly from real TiDB's own
        // `parseIdentOrFuncCall` (`pkg/parser/expr_parser.go`); see
        // `tidb_ast::Expr::GenericFuncCall`'s own doc for the restore
        // asymmetry.
        if self.peek_n(1).kind == TokenKind::Op
            && self.peek_n(1).text == "."
            && matches!(self.peek_n(2).kind, TokenKind::Ident | TokenKind::Keyword)
            && self.peek_n(3).kind == TokenKind::Op
            && self.peek_n(3).text == "("
        {
            let schema = self.bump().text;
            self.bump(); // .
            let name = self.bump().text;
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
            return Ok(Expr::GenericFuncCall { schema, name, args });
        }
        if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" {
            return self.parse_named_func();
        }
        Ok(Expr::Column(self.parse_name_path()?))
    }

    /// Parses `name ( arg, ... )` where the current token is the function name.
    fn parse_named_func(&mut self) -> PResult<Expr> {
        let name = self.bump().text;
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
        Ok(Expr::Func { name, args })
    }

    /// Parses the datetime functions whose optional argument is an integer
    /// literal precision. Go's `parseCurrentFunc`/`parseCurDateFunc` and the
    /// `FuncDatetimePrecListOpt` grammar reject expressions such as `-1` or
    /// `1+1` here; routing these names through the generic expression parser
    /// would incorrectly accept them as unary/binary expressions.
    fn parse_datetime_precision_func(&mut self) -> PResult<Expr> {
        let name = self.bump().text;
        self.expect_op("(")?;
        let mut args = Vec::new();
        if !self.is_op(")") {
            if self.peek().kind != TokenKind::IntLit {
                return Err(self.err_here("expected integer datetime precision"));
            }
            args.push(Expr::Int(self.bump().text));
        }
        self.expect_op(")")?;
        Ok(Expr::Func { name, args })
    }

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
    fn parse_aggregate(&mut self) -> PResult<Expr> {
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
            self.bump();
        }
        let mut args = vec![self.parse_expr(prec::NONE)?];
        while self.is_op(",") {
            self.bump();
            args.push(self.parse_expr(prec::NONE)?);
        }
        self.expect_op(")")?;
        if args.len() > 1 {
            let multi_arg_allowed = match name.as_str() {
                "COUNT" => distinct,
                "APPROX_COUNT_DISTINCT" => true,
                _ => false,
            };
            if !multi_arg_allowed {
                return Err(self.err_here("this aggregate does not accept multiple arguments"));
            }
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
    fn parse_window_func(&mut self) -> PResult<Expr> {
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

    /// Parses `GROUP_CONCAT([DISTINCT] expr [, expr ...] [SEPARATOR 'str'])`.
    /// An `ORDER BY` before `SEPARATOR` is a real MySQL construct but is not
    /// modelled — an honest `ParseError` (the closing `)` won't match), not a
    /// silent skip. `separator` defaults to `,` and is always restored
    /// explicitly, matching the Go AST's normalization.
    fn parse_group_concat(&mut self) -> PResult<Expr> {
        self.bump(); // GROUP_CONCAT
        self.expect_op("(")?;
        let distinct = if self.is_kw("DISTINCT") {
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
            decode_string(&t.text)
        } else {
            ",".to_string()
        };
        self.expect_op(")")?;
        Ok(Expr::GroupConcat {
            distinct,
            args,
            order_by,
            separator,
        })
    }

    // ---- predicates ----

    fn parse_predicate(&mut self, left: Expr, not: bool) -> PResult<Expr> {
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
            if self.is_op("(") && (self.is_kw_at(1, "SELECT") || self.is_kw_at(1, "WITH")) {
                self.bump(); // (
                let subquery = if self.is_kw("WITH") {
                    self.parse_with_select()?
                } else {
                    self.parse_select_or_setopr()?
                };
                self.expect_op(")")?;
                return Ok(Expr::InSubquery {
                    expr: Box::new(left),
                    subquery: Box::new(subquery),
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
        } else if self.is_kw("LIKE") {
            self.bump();
            // The pattern is a bit_expr (tighter than predicates).
            let pattern = self.parse_expr(prec::BIT_OR)?;
            let escape = self.parse_opt_escape_clause()?;
            Ok(Expr::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                not,
                escape,
            })
        } else if self.is_kw("REGEXP") || self.is_kw("RLIKE") {
            self.bump();
            // Same `bit_expr` pattern precedence as `LIKE` above,
            // confirmed via `godump restore`: `a REGEXP 'x' | 'y'` binds
            // `|` into the pattern operand, not the whole predicate.
            let pattern = self.parse_expr(prec::BIT_OR)?;
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
        let decoded = decode_string(&self.bump().text);
        match decoded.as_bytes() {
            [] => Ok(Some(0)),
            [b'\\'] => Ok(None),
            [b] => Ok(Some(*b)),
            _ => Err(self.err_here("ESCAPE must be a single character")),
        }
    }

    fn parse_is(&mut self, left: Expr) -> PResult<Expr> {
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

/// Whether `name` is a scalar function that the lexer classifies as a keyword
/// (so `name(...)` should parse as an ordinary function call). Special-syntax
/// keyword functions (`CAST`, `EXTRACT`, ...) are deliberately excluded.
fn is_scalar_kw_func(name: &str) -> bool {
    // `LEFT`/`RIGHT` also start joins, but only in `FROM` context; in an
    // expression they are only functions when directly followed by `(`.
    // `VALUES` is also the `INSERT ... VALUES` clause keyword, but that's
    // parsed at the statement level, never through expression parsing, so
    // there's no ambiguity with `VALUES(col)` inside an `ON DUPLICATE KEY
    // UPDATE` assignment.
    matches!(
        name.to_ascii_uppercase().as_str(),
        "IF" | "COALESCE"
            | "LEFT"
            | "RIGHT"
            | "MID"
            | "VALUES"
            | "YEAR"
            | "MONTH"
            | "DAY"
            | "QUARTER"
            | "HOUR"
            | "MINUTE"
            | "SECOND"
            | "DATE_ADD"
            | "DATE_SUB"
            | "LOG"
            | "NOW"
            // `CURRENT_TIMESTAMP` also has a special no-parens grammar form
            // (see the `TokenKind::Keyword` literal match below), but is
            // just as often written `CURRENT_TIMESTAMP(...)` — both need to
            // reach the SAME `Expr::Func` shape, confirmed via
            // `godump restore`: `current_timestamp` (no parens) restores as
            // `CURRENT_TIMESTAMP()`, identical to the parenthesized form.
            | "CURRENT_TIMESTAMP"
            // `TRUNCATE` is also the `TRUNCATE TABLE` statement keyword, but
            // that's parsed at the statement level, never through expression
            // parsing — confirmed real MySQL/TiDB also allows it as an
            // ordinary function here (`pkg/parser/digester_test.go`:
            // `select truncate(1, 2)`), unlike `ROUND`/`CEIL`/`FLOOR`, which
            // aren't lexer keywords at all and need no entry here.
            | "TRUNCATE"
            // `CURDATE`/`CURTIME` are lexer keywords but, UNLIKE
            // `CURRENT_DATE`/`CURRENT_TIME`/`UTC_DATE`/`UTC_TIME`/
            // `UTC_TIMESTAMP` below, have NO bare no-parens grammar form —
            // confirmed via `godump restore`: bare `curdate` parses as an
            // ordinary (unqualified) COLUMN reference, not a function call
            // (the same pre-existing "reserved keyword as identifier" gap
            // bare `now` hits, left alone — not this crate's concern here).
            | "CURTIME"
            // `CURRENT_DATE`/`CURRENT_TIME`/`UTC_DATE`/`UTC_TIME`/
            // `UTC_TIMESTAMP`/`CURRENT_ROLE` all have a bare no-parens
            // form too (each gets its own arm in the `TokenKind::Keyword`
            // literal match below, alongside `CURRENT_TIMESTAMP`) but are
            // just as often written with `()` — both forms need to reach
            // here.
            | "CURRENT_DATE"
            | "CURRENT_TIME"
            | "UTC_DATE"
            | "UTC_TIME"
            | "UTC_TIMESTAMP"
            | "CURRENT_ROLE"
            | "LOCALTIME"
            | "LOCALTIMESTAMP"
            | "CURRENT_USER"
            // `COLLATION` is a lexer keyword (matching real TiDB) that
            // otherwise falls through `parse_prefix`'s keyword-in-
            // expression catch-all with no function-call treatment at
            // all. Restore/evaluation are both `Unsupported` beyond this
            // parse-time recognition (`tidb_expr::func` has no dispatch
            // entry for it), matching the established "parse and restore
            // only" pattern many other functions here already follow —
            // but PARSING at least, which this generic `parse_named_func`
            // call already gets for free, is real restore-fidelity
            // coverage on its own. (`WEIGHT_STRING` used to be listed
            // here too, but has its OWN dedicated dispatch now — see
            // `Parser::parse_weight_string`'s own doc — since its `AS
            // {CHAR|BINARY}(N)` clause needs a genuinely different parse
            // shape this generic comma-arg path can't produce.)
            | "COLLATION"
    )
}

/// Whether `name(...)` uses Go's optional integer precision grammar instead
/// of the generic expression-list grammar. Keep this separate from
/// [`is_scalar_kw_func`] so ordinary scalar functions still accept arbitrary
/// expression arguments.
fn is_datetime_precision_func(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "CURRENT_TIMESTAMP"
            | "CURRENT_DATE"
            | "CURRENT_TIME"
            | "UTC_DATE"
            | "UTC_TIME"
            | "UTC_TIMESTAMP"
            | "CURRENT_ROLE"
            | "CURRENT_USER"
            | "LOCALTIME"
            | "LOCALTIMESTAMP"
            | "CURTIME"
    )
}

/// Returns the canonical restore name for a (supported) window function, or
/// `None`. Scope: only the zero-argument ranking functions — a frame-based
/// window function (`SUM(x) OVER (...)`, `LEAD`/`LAG`/...) is not modelled.
fn window_func_canonical(name: &str) -> Option<&'static str> {
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
fn agg_canonical(name: &str) -> Option<&'static str> {
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
        _ => None,
    }
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

/// Parses a variable token's text (`@name`, `@@name`, `@@scope.name`) into the
/// AST. Returns `None` for shapes this phase does not model (e.g. quoted names).
fn parse_variable(text: &str) -> Option<Expr> {
    if let Some(rest) = text.strip_prefix("@@") {
        // System variable, with an optional GLOBAL/SESSION/LOCAL scope.
        if let Some((prefix, name)) = rest.split_once('.') {
            let scope = match prefix.to_ascii_uppercase().as_str() {
                "GLOBAL" => Some(SysVarScope::Global),
                "SESSION" | "LOCAL" => Some(SysVarScope::Session),
                _ => None,
            };
            if scope.is_some() {
                return Some(Expr::SysVar {
                    scope,
                    name: decode_variable_name(name).to_ascii_lowercase(),
                });
            }
            // A dotted name with an unknown prefix is not a scope; keep it whole.
            return Some(Expr::SysVar {
                scope: None,
                name: decode_variable_name(rest).to_ascii_lowercase(),
            });
        }
        return Some(Expr::SysVar {
            scope: None,
            name: decode_variable_name(rest).to_ascii_lowercase(),
        });
    }
    let name = text.strip_prefix('@')?;
    Some(Expr::UserVar(decode_variable_name(name)))
}

fn decode_variable_name(raw: &str) -> String {
    if matches!(raw.as_bytes().first(), Some(b'\'') | Some(b'"')) {
        decode_string(raw)
    } else if raw.starts_with('`') && raw.ends_with('`') && raw.len() >= 2 {
        raw[1..raw.len() - 1].replace("``", "`")
    } else {
        raw.to_string()
    }
}
