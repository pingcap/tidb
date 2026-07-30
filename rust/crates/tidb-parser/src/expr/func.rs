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

//! Built-in function calls whose argument shape the grammar spells out
//! rather than treating as a plain expression list: `INTERVAL`, the
//! date/time extractors, `CASE`, `ROW`, `DEFAULT`, `CHAR`, `WEIGHT_STRING`,
//! `POSITION`, `SUBSTRING`, `TRIM`, typed literals, ODBC escapes,
//! `MATCH ... AGAINST`, and the generic named-function fallback.
//!
//! This mirrors Go's `FunctionCallKeyword` / `FunctionCallNonKeyword` /
//! `FunctionCallGeneric` production families in `pkg/parser/parser.y`.

use super::*;

impl Parser {
    /// Parses `INTERVAL value unit`. The unit keyword naturally stops the
    /// value's own expression parse (it isn't a recognized infix operator),
    /// so no special precedence handling is needed to separate the two.
    pub(crate) fn parse_interval(&mut self) -> PResult<Expr> {
        self.bump(); // INTERVAL
        let value = self.parse_expr(prec::NONE)?;
        if self.peek().kind != TokenKind::Keyword {
            return Err(self.err_here("expected an INTERVAL unit"));
        }
        let unit = self.bump().text.to_ascii_uppercase();
        let unit = unit.strip_prefix("SQL_TSI_").unwrap_or(&unit).to_string();
        Ok(Expr::Interval {
            value: Box::new(value),
            unit,
        })
    }

    /// Parses `EXTRACT(unit FROM expr)` — the unit keyword comes FIRST
    /// (unlike `INTERVAL value unit`'s own order), so it's read directly
    /// off the token stream rather than via `parse_expr`.
    pub(crate) fn parse_extract(&mut self) -> PResult<Expr> {
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

    /// Parses Go's closed `TimeUnit` production.
    pub(crate) fn parse_bare_time_unit(&mut self) -> PResult<String> {
        if !matches!(self.peek().kind, TokenKind::Ident | TokenKind::Keyword) {
            return Err(self.err_here("expected a time unit"));
        }
        let written = self.peek().text.to_ascii_uppercase();
        let unit = match written.as_str() {
            "SQL_TSI_SECOND" => "SECOND",
            "SQL_TSI_MINUTE" => "MINUTE",
            "SQL_TSI_HOUR" => "HOUR",
            "SQL_TSI_DAY" => "DAY",
            "SQL_TSI_WEEK" => "WEEK",
            "SQL_TSI_MONTH" => "MONTH",
            "SQL_TSI_QUARTER" => "QUARTER",
            "SQL_TSI_YEAR" => "YEAR",
            "MICROSECOND" | "SECOND" | "MINUTE" | "HOUR" | "DAY" | "WEEK" | "MONTH" | "QUARTER"
            | "YEAR" | "SECOND_MICROSECOND" | "MINUTE_MICROSECOND" | "MINUTE_SECOND"
            | "HOUR_MICROSECOND" | "HOUR_SECOND" | "HOUR_MINUTE" | "DAY_MICROSECOND"
            | "DAY_SECOND" | "DAY_MINUTE" | "DAY_HOUR" | "YEAR_MONTH" => written.as_str(),
            _ => return Err(self.err_here("expected a time unit")),
        }
        .to_owned();
        self.bump();
        Ok(unit)
    }

    /// `TIMESTAMPADD(unit, interval, datetime_expr)` — see
    /// `tidb_ast::Expr::TimestampAdd`'s own doc for why `unit` is a
    /// dedicated field, not an ordinary parsed argument expression.
    pub(crate) fn parse_timestamp_add(&mut self) -> PResult<Expr> {
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
    pub(crate) fn parse_timestamp_diff(&mut self) -> PResult<Expr> {
        self.bump(); // TIMESTAMPDIFF
        self.expect_op("(")?;
        let unit = self.parse_bare_time_unit()?;
        if !matches!(
            unit.as_str(),
            "MICROSECOND"
                | "SECOND"
                | "MINUTE"
                | "HOUR"
                | "DAY"
                | "WEEK"
                | "MONTH"
                | "QUARTER"
                | "YEAR"
        ) {
            return Err(self.err_here("TIMESTAMPDIFF requires a single time unit"));
        }
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
    pub(crate) fn parse_adddate_or_subdate(&mut self, name: &str) -> PResult<Expr> {
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
            origin_position: 0,
        })
    }

    /// `GET_FORMAT(DATE|TIME|DATETIME|TIMESTAMP, format_expr)` — see
    /// `tidb_ast::Expr::GetFormat`'s own doc for why `TIMESTAMP` collapses
    /// into the SAME selector as `DATETIME`.
    pub(crate) fn parse_get_format(&mut self) -> PResult<Expr> {
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
    pub(crate) fn parse_case(&mut self) -> PResult<Expr> {
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
    pub(crate) fn parse_row_constructor(&mut self) -> PResult<Expr> {
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
    pub(crate) fn parse_default_expr(&mut self) -> PResult<Expr> {
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
    pub(crate) fn parse_char_func(&mut self) -> PResult<Expr> {
        self.bump(); // CHAR
        self.expect_op("(")?;
        if self.is_op(")") {
            self.bump();
            return Ok(Expr::Func {
                name: "CHAR".to_string(),
                args: vec![],
                origin_position: 0,
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
            origin_position: 0,
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
    pub(crate) fn parse_weight_string(&mut self) -> PResult<Expr> {
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
    pub(crate) fn parse_position_func(&mut self) -> PResult<Expr> {
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
    pub(crate) fn parse_substring_func(&mut self, name: &str) -> PResult<Expr> {
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
            origin_position: 0,
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
    pub(crate) fn parse_trim(&mut self) -> PResult<Expr> {
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
    pub(crate) fn parse_typed_literal(
        &mut self,
        style: CastStyle,
        cast_type: CastType,
    ) -> PResult<Expr> {
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
    /// byte-identically). The inner value is the full expression owned by
    /// Go's `Expression` production, not only a string literal. Any OTHER type identifier
    /// (`fn`, `date`, `time`, `timestamp`, ...) is a pass-through: the
    /// braces are discarded and the inner expression alone survives —
    /// real TiDB's own `default:` arm.
    pub(crate) fn parse_odbc_escape(&mut self) -> PResult<Expr> {
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
            let value = self.parse_expr(prec::NONE)?;
            self.expect_op("}")?;
            return Ok(Expr::Cast(CastExpr {
                expr: Box::new(value),
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
    pub(crate) fn parse_match_against(&mut self) -> PResult<Expr> {
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
    pub(crate) fn parse_ident_or_func(&mut self) -> PResult<Expr> {
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
            let origin_position = self.peek().offset;
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
            return Ok(Expr::GenericFuncCall {
                schema,
                name,
                args,
                origin_position,
            });
        }
        if self.peek_n(1).kind == TokenKind::Op && self.peek_n(1).text == "(" {
            return self.parse_named_func();
        }
        Ok(Expr::Column(self.parse_name_path()?))
    }

    /// Parses `name ( arg, ... )` where the current token is the function name.
    pub(crate) fn parse_named_func(&mut self) -> PResult<Expr> {
        let origin_position = self.peek().offset;
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
        if matches!(name.to_ascii_uppercase().as_str(), "DATE_ADD" | "DATE_SUB")
            && (args.len() != 2 || !matches!(args[1], Expr::Interval { .. }))
        {
            return Err(self.err_here("DATE_ADD/DATE_SUB requires an INTERVAL argument"));
        }
        Ok(Expr::Func {
            name,
            args,
            origin_position,
        })
    }

    /// Parses the datetime functions whose optional argument is an integer
    /// literal precision. Go's `parseCurrentFunc`/`parseCurDateFunc` and the
    /// `FuncDatetimePrecListOpt` grammar reject expressions such as `-1` or
    /// `1+1` here; routing these names through the generic expression parser
    /// would incorrectly accept them as unary/binary expressions.
    pub(crate) fn parse_datetime_precision_func(&mut self) -> PResult<Expr> {
        let origin_position = self.peek().offset;
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
        Ok(Expr::Func {
            name,
            args,
            origin_position,
        })
    }
}

/// Whether `name` is a scalar function that the lexer classifies as a keyword
/// (so `name(...)` should parse as an ordinary function call). Special-syntax
/// keyword functions (`CAST`, `EXTRACT`, ...) are deliberately excluded.
pub(super) fn is_scalar_kw_func(name: &str) -> bool {
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
pub(super) fn is_datetime_precision_func(name: &str) -> bool {
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
