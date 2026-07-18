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

//! `CAST(expr AS type)` / `CONVERT(expr, type)` / `CONVERT(expr USING
//! charset)` — see [`tidb_ast::Expr::Cast`]'s own doc for why the first two
//! share one AST node. The target-type grammar (`parse_cast_type`) is a
//! narrower, MySQL-specific subset of [`crate::ddl`]'s own column-type
//! grammar (confirmed via `godump restore`, not assumed identical): plain
//! `INT`/`INTEGER`/`REAL`/`BOOL`/`BOOLEAN`/`NCHAR` are all genuine
//! `ParseError`s here even though they're valid column types, while `REAL`
//! (accepted, a bare synonym for `DOUBLE`) and the `SIGNED`/`UNSIGNED`
//! integer-reinterpretation targets exist ONLY here, never as a column
//! type.

use tidb_ast::{CastExpr, CastStyle, CastType, Expr};
use tidb_lexer::{canonical_charset, TokenKind};

use crate::{prec, PResult, Parser};

impl Parser {
    /// `CAST(expr AS type)`.
    pub(crate) fn parse_cast(&mut self) -> PResult<Expr> {
        self.bump(); // CAST
        self.expect_op("(")?;
        let expr = self.parse_expr(prec::NONE)?;
        self.expect_kw("AS")?;
        let cast_type = self.parse_cast_type()?;
        let array = self.parse_array_suffix();
        self.expect_op(")")?;
        Ok(Expr::Cast(CastExpr {
            expr: Box::new(expr),
            cast_type,
            style: CastStyle::Cast,
            array,
        }))
    }

    /// `CONVERT(expr, type)` (identical semantics to `CAST(expr AS type)`,
    /// just a different concrete syntax — see [`tidb_ast::Expr::Cast`]'s
    /// own doc) or `CONVERT(expr USING charset)` (a genuinely different
    /// operation, a charset conversion — see [`tidb_ast::Expr::ConvertUsing`]).
    pub(crate) fn parse_convert(&mut self) -> PResult<Expr> {
        self.bump(); // CONVERT
        self.expect_op("(")?;
        let expr = self.parse_expr(prec::NONE)?;
        if self.is_kw("USING") {
            self.bump();
            let raw = self.parse_using_charset_name()?;
            let charset = canonical_charset(&raw)
                .ok_or_else(|| self.err_here("unknown character set"))?
                .to_string();
            self.expect_op(")")?;
            return Ok(Expr::ConvertUsing {
                expr: Box::new(expr),
                charset,
            });
        }
        self.expect_op(",")?;
        let cast_type = self.parse_cast_type()?;
        let array = self.parse_array_suffix();
        self.expect_op(")")?;
        Ok(Expr::Cast(CastExpr {
            expr: Box::new(expr),
            cast_type,
            style: CastStyle::Convert,
            array,
        }))
    }

    /// `BINARY expr` — a bare prefix operator, THE SAME operation as
    /// `CAST(expr AS BINARY)` under a third concrete syntax (see
    /// [`CastStyle::BinaryOperator`]'s own doc). Binds at `precUnary`, the
    /// SAME tight precedence `Parser::unary`'s own `+`/`-`/`~`/`!` already
    /// use (confirmed via `godump restore`: `BINARY -a` restores as
    /// `BINARY -\`a\``, wrapping the whole unary-minus expression).
    pub(crate) fn parse_binary_operator(&mut self) -> PResult<Expr> {
        self.bump(); // BINARY
        let expr = self.parse_expr(prec::UNARY)?;
        Ok(Expr::Cast(CastExpr {
            expr: Box::new(expr),
            cast_type: CastType::Binary { len: None },
            style: CastStyle::BinaryOperator,
            array: false,
        }))
    }

    /// `JSON_SUM_CRC32(expr AS type ARRAY)` — see
    /// [`tidb_ast::CastStyle::JsonSumCrc32`]'s own doc for why this reuses
    /// [`CastExpr`] directly rather than a dedicated node. The `ARRAY`
    /// suffix is MANDATORY here (confirmed via `godump restore`:
    /// `JSON_SUM_CRC32(j AS SIGNED)`, with no `ARRAY`, is a genuine
    /// `ParseError` — real TiDB's own `parseJsonSumCrc32Func` rejects a
    /// non-array target type explicitly, `"JSON_SUM_CRC32 requires ARRAY
    /// type"`), unlike `CAST`/`CONVERT`'s own OPTIONAL suffix.
    pub(crate) fn parse_json_sum_crc32(&mut self) -> PResult<Expr> {
        self.bump(); // JSON_SUM_CRC32
        self.expect_op("(")?;
        let expr = self.parse_expr(prec::NONE)?;
        self.expect_kw("AS")?;
        let cast_type = self.parse_cast_type()?;
        if !self.parse_array_suffix() {
            return Err(self.err_here("JSON_SUM_CRC32 requires ARRAY type"));
        }
        self.expect_op(")")?;
        Ok(Expr::Cast(CastExpr {
            expr: Box::new(expr),
            cast_type,
            style: CastStyle::JsonSumCrc32,
            array: true,
        }))
    }

    /// Parses an optional trailing `ARRAY` suffix after a `CAST`/
    /// `CONVERT`/`JSON_SUM_CRC32` target type — see
    /// [`tidb_ast::CastExpr::array`]'s own doc.
    fn parse_array_suffix(&mut self) -> bool {
        if self.is_kw("ARRAY") {
            self.bump();
            true
        } else {
            false
        }
    }

    /// Parses a `CAST`/`CONVERT` target type. See this module's own doc for
    /// how this differs from `crate::ddl`'s column-type grammar.
    fn parse_cast_type(&mut self) -> PResult<CastType> {
        if self.is_kw("SIGNED") {
            self.bump();
            self.eat_int_suffix();
            Ok(CastType::Signed)
        } else if self.is_kw("UNSIGNED") {
            self.bump();
            self.eat_int_suffix();
            Ok(CastType::Unsigned)
        } else if self.is_kw("CHAR") {
            self.bump();
            let len = self.parse_optional_paren_uint()?;
            let charset = self.parse_optional_charset_clause()?;
            // `len` and `charset` are independent — both may be given
            // together (see `tidb_ast::CastType::Char`'s own doc for why
            // an EARLIER "charset dropped once a length is given"
            // hypothesis here was wrong; the real rule lives in restore,
            // keyed on the charset NAME, not on whether `len` is present).
            Ok(CastType::Char { len, charset })
        } else if self.is_kw("BINARY") {
            self.bump();
            let len = self.parse_optional_paren_uint()?;
            Ok(CastType::Binary { len })
        } else if self.is_kw("DECIMAL") {
            self.bump();
            if self.is_op("(") {
                self.bump();
                let flen = self.parse_uint_arg()?;
                let scale = if self.is_op(",") {
                    self.bump();
                    self.parse_uint_arg()?
                } else {
                    0
                };
                self.expect_op(")")?;
                Ok(CastType::Decimal { flen, scale })
            } else {
                // A bare `DECIMAL` with NO parens is a real (10, 0)
                // default, NOT "unspecified" — see `CastType::Decimal`'s
                // own doc.
                Ok(CastType::Decimal { flen: 10, scale: 0 })
            }
        } else if self.is_kw("DATE") {
            self.bump();
            Ok(CastType::Date)
        } else if self.is_kw("DATETIME") {
            self.bump();
            let fsp = self.parse_optional_paren_uint()?;
            Ok(CastType::DateTime { fsp })
        } else if self.is_kw("TIME") {
            self.bump();
            let fsp = self.parse_optional_paren_uint()?;
            Ok(CastType::Time { fsp })
        } else if self.is_kw("YEAR") {
            self.bump();
            Ok(CastType::Year)
        } else if self.is_kw("DOUBLE") {
            self.bump();
            Ok(CastType::Double)
        } else if self.is_kw("REAL") {
            // A bare synonym for `DOUBLE` — like `DOUBLE` itself, `REAL`
            // takes no parenthesized argument at all as a CAST target
            // (confirmed via `godump restore`: `REAL(5)` is a genuine
            // `ParseError`, unlike `FLOAT`'s own precision argument).
            self.bump();
            Ok(CastType::Double)
        } else if self.is_kw("FLOAT") {
            self.bump();
            self.parse_float_cast_type()
        } else if self.is_kw("JSON") {
            self.bump();
            Ok(CastType::Json)
        } else {
            Err(self.err_here("expected a CAST/CONVERT target type"))
        }
    }

    /// `FLOAT`'s own precision argument, one (`FLOAT(p)`, a true IEEE
    /// bit-precision selector) or two (`FLOAT(M, D)`, the old-style
    /// precision/scale form — `D` is parsed but has no bearing on the
    /// resolved type). Either shape resolves the SAME way, confirmed via
    /// `godump restore`: `p <= 24` stays `FLOAT`, `25 <= p <= 53` resolves
    /// to `DOUBLE` AT PARSE TIME (this crate's `Value::Float` already
    /// covers both uniformly, so the distinction only matters for
    /// restore), `p > 53` is a genuine `ParseError`.
    fn parse_float_cast_type(&mut self) -> PResult<CastType> {
        if !self.is_op("(") {
            return Ok(CastType::Float);
        }
        self.bump();
        let m = self.parse_uint_arg()?;
        if self.is_op(",") {
            self.bump();
            self.parse_uint_arg()?;
        }
        self.expect_op(")")?;
        if m > 53 {
            return Err(self.err_here("FLOAT precision out of range"));
        }
        Ok(if m > 24 {
            CastType::Double
        } else {
            CastType::Float
        })
    }

    /// Consumes an optional `(n)`, `n` a plain non-negative integer.
    fn parse_optional_paren_uint(&mut self) -> PResult<Option<u32>> {
        if self.is_op("(") {
            self.bump();
            let n = self.parse_uint_arg()?;
            self.expect_op(")")?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    /// Consumes an optional `CHARACTER SET name` / `CHARSET name` clause.
    fn parse_optional_charset_clause(&mut self) -> PResult<Option<String>> {
        if self.is_kw("CHARACTER") {
            self.bump();
            self.expect_kw("SET")?;
        } else if self.is_kw("CHARSET") {
            self.bump();
        } else {
            return Ok(None);
        }
        Ok(Some(self.parse_charset_name()?.to_ascii_uppercase()))
    }

    /// Parses one non-negative integer inside a type's `(...)` argument
    /// list.
    fn parse_uint_arg(&mut self) -> PResult<u32> {
        if self.peek().kind == TokenKind::IntLit {
            self.bump()
                .text
                .parse()
                .map_err(|_| self.err_here("invalid integer type argument"))
        } else {
            Err(self.err_here("expected integer type argument"))
        }
    }

    /// Consumes an optional `INT`/`INTEGER` suffix on `SIGNED`/`UNSIGNED` —
    /// BOTH spellings are real, valid synonyms here (confirmed via `godump
    /// restore`: `CAST(a AS SIGNED INT)` restores identically to `CAST(a
    /// AS SIGNED INTEGER)`/bare `SIGNED`), unlike the original code, which
    /// only recognized `INTEGER`.
    fn eat_int_suffix(&mut self) {
        if self.is_kw("INT") || self.is_kw("INTEGER") {
            self.bump();
        }
    }
}
