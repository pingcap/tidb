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

//! `CAST` / `CONVERT` / `BINARY` and their target types, mirroring Go's
//! `FuncCastExpr` in `pkg/parser/ast/functions.go`.

use super::*;

/// Which concrete syntax a [`CastExpr`] was written with — restores
/// differently even though both mean the same thing (see [`Expr::Cast`]'s
/// own doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastStyle {
    /// `CAST(expr AS type)`.
    Cast,
    /// `CONVERT(expr, type)`.
    Convert,
    /// `BINARY expr` — a bare PREFIX operator (no parens, no `AS`/`,`),
    /// binding at the same tight precedence as unary `-`/`~`/`!`
    /// (confirmed via `godump restore`: `BINARY -a` restores as `BINARY
    /// -\`a\``, wrapping the whole unary-minus expression). Read directly
    /// from real TiDB's own hand-written parser
    /// (`pkg/parser/expr_prefix_parser.go`'s `parsePrefixKeywordExpr`,
    /// `case binaryType:`), which builds the EXACT SAME `FuncCastExpr`
    /// shape as `CAST(expr AS BINARY)` (`cast_type` is always
    /// [`CastType::Binary`] with `len: None` here — a length specifier
    /// isn't part of this syntax), just with a different `FunctionType`
    /// discriminator purely for restore, matching [`Expr::Cast`]'s own
    /// "one AST node, several concrete syntaxes" precedent.
    BinaryOperator,
    /// `DATE 'literal'` — an ODBC-style typed date literal, a bare
    /// keyword-prefixed string literal (no parens, no `AS`), the SAME
    /// `FuncCallExpr` shape real TiDB's own `ast.DateLiteral` function
    /// name produces (`pkg/parser/expr_prefix_parser.go`'s
    /// `parsePrefixTimeLiteral`) — reused here as [`Expr::Cast`] with
    /// `cast_type` always [`CastType::Date`]. Bare `DATE 'literal'` supplies
    /// an [`Expr::String`]; ODBC `{d expression}` may supply any expression.
    /// Restores as `DATE 'literal'`, the literal's
    /// quotes escaped but with NO `_UTF8MB4` charset-introducer prefix
    /// (confirmed via `godump restore` — a genuinely different restore
    /// rule from an ordinary standalone [`Expr::String`]). Only
    /// recognized when the token immediately after the `DATE` keyword is
    /// a string literal — otherwise `DATE` is either a bare column
    /// reference or the `DATE(expr)` scalar function, both already
    /// handled by the ordinary non-reserved-keyword path (see
    /// `tidb_parser::parse_prefix`'s own `"DATE"` dispatch doc).
    /// Evaluation is deliberately `Unsupported`, unconditionally — see
    /// this variant's own doc in `tidb_parser` for why reusing
    /// `CAST(... AS DATE)`'s existing (lenient, `NULL`-on-invalid)
    /// evaluation logic would be a genuine correctness regression, not
    /// just an incomplete one.
    DateLiteral,
    /// `TIME 'literal'` — see [`CastStyle::DateLiteral`]'s own doc; the
    /// SAME shape, `cast_type` always [`CastType::Time`] with `fsp:
    /// None`. Evaluation was ALREADY `Unsupported` for every
    /// `CastType::Time` target before this variant existed (real MySQL
    /// `TIME` is an elapsed-time domain this crate doesn't model at all —
    /// see [`CastType::Time`]'s own doc), so this style needs no new
    /// evaluation-side reasoning at all, just restore.
    TimeLiteral,
    /// `TIMESTAMP 'literal'` — see [`CastStyle::DateLiteral`]'s own doc;
    /// the SAME shape, `cast_type` always [`CastType::DateTime`] with
    /// `fsp: None` (real TiDB's own hand-written parser folds `TIMESTAMP`
    /// and `DATETIME` into the SAME target type at this position — this
    /// crate has no separate `TIMESTAMP` value domain from `DATETIME`
    /// either, matching [`GetFormatSelector`]'s own established
    /// `TIMESTAMP`→`DATETIME` collapse precedent).
    TimestampLiteral,
    /// `JSON_SUM_CRC32(expr AS type ARRAY)` — a JSON-array checksum
    /// function whose restore turned out byte-identical to
    /// [`CastStyle::Cast`]'s own `NAME(expr AS type)` shape (confirmed
    /// via reading `pkg/parser/ast/functions.go`'s own
    /// `JSONSumCrc32Expr.Restore` directly: `WriteKeyWord("JSON_SUM_CRC32")`
    /// then the EXACT same `(expr AS type)` body `CastExpr.Restore`
    /// already produces) — so, like `CHAR`/`DEFAULT`/`->` before it, this
    /// needed no dedicated AST node, just a new [`CastStyle`] variant
    /// reusing the SAME [`CastExpr`] payload. `array` is ALWAYS `true`
    /// here — real TiDB's own `parseJsonSumCrc32Func` rejects a non-
    /// `ARRAY` target type as a genuine `ParseError` (`"JSON_SUM_CRC32
    /// requires ARRAY type"`, confirmed via `godump restore`), replicated
    /// at parse time in `tidb-parser` rather than left to a stray runtime
    /// invariant.
    JsonSumCrc32,
}

/// `CAST(expr AS type)` / `CONVERT(expr, type)`'s shared payload.
#[derive(Debug, Clone, PartialEq)]
pub struct CastExpr {
    /// The value being cast.
    pub expr: Box<Expr>,
    /// The target type.
    pub cast_type: CastType,
    /// Which concrete syntax produced this node (affects restore only).
    pub style: CastStyle,
    /// A trailing `ARRAY` suffix (`CAST(x AS SIGNED ARRAY)`), a JSON
    /// multi-valued-index type modifier — confirmed via `godump restore`
    /// to be an independent flag on the type itself, uniformly appended
    /// AFTER any base type (`pkg/parser/types/field_type.go`'s own
    /// `RestoreAsCastType`: `if ft.array { ctx.WritePlain(" "); ctx.
    /// WriteKeyWord("ARRAY") }`, unconditional on which base type
    /// precedes it), shared by both [`CastStyle::Cast`] and
    /// [`CastStyle::Convert`] (confirmed via `godump restore`:
    /// `CONVERT(a, SIGNED ARRAY)` parses too). Always `true` for
    /// [`CastStyle::JsonSumCrc32`] — see that variant's own doc.
    ///
    /// Parses and restores fully (real TiDB's own PARSER accepts it
    /// anywhere a type can appear); evaluation is deliberately
    /// `Unsupported` whenever `true` — NOT merely because this crate has
    /// no JSON value domain (a prior, unverified assumption this doc
    /// comment used to make), but because real TiDB ITSELF rejects a
    /// bare `CAST(x AS type ARRAY)` in any ordinary SELECT/general
    /// expression at PLAN-BUILD time: confirmed directly in
    /// `pkg/planner/core/expression_rewriter.go`'s own
    /// `*ast.FuncCastExpr` case, `v.Tp.IsArray() && !er.allowBuildCastArray`
    /// produces `ErrNotSupportedYet` with the literal message "Use of
    /// CAST( .. AS .. ARRAY) outside of functional index in
    /// CREATE(non-SELECT)/ALTER TABLE or in general expressions" — and
    /// confirmed via `gorun`: even `CAST(CAST('[1,2,3]' AS JSON) AS
    /// SIGNED ARRAY)`, a genuine JSON array value, still errors as a
    /// bare `SELECT` expression. `allowBuildCastArray` only ever flips to
    /// `true` inside functional/multi-valued INDEX definition rewriting
    /// (`CREATE TABLE`'s own index clause, `ALTER TABLE ADD INDEX`) —
    /// DDL machinery this crate does not model at all (no functional/
    /// multi-valued index support whatsoever) — so this is a genuine,
    /// PERMANENT restriction shared with real TiDB, not a capability gap
    /// unique to this crate, matching [`CastType::Json`]'s own "no JSON
    /// value domain" boundary only for the ONE narrow context
    /// (`JSON_SUM_CRC32`) where real TiDB DOES execute an array cast
    /// successfully.
    pub array: bool,
}

/// A `CAST`/`CONVERT` target type. Deliberately narrower than
/// [`crate::ColumnType`]'s own type-name set — `CAST` only accepts a specific
/// MySQL-defined subset (confirmed via `godump restore`: `INT`/`INTEGER`/
/// `REAL`/`BOOL`/`BOOLEAN`/`NCHAR` are all genuine `ParseError`s as a CAST
/// target, unlike as a column type).
#[derive(Debug, Clone, PartialEq)]
pub enum CastType {
    /// `SIGNED [INTEGER]`.
    Signed,
    /// `UNSIGNED [INTEGER]`.
    Unsigned,
    /// `CHAR[(len)] [CHARSET name]`. `len` and `charset` are independent —
    /// both may be given together (confirmed by reading real TiDB's own
    /// `FieldType.RestoreAsCastType`, `pkg/parser/types/field_type.go`: an
    /// EARLIER, wrong hypothesis here was "charset dropped once a length is
    /// given," which happened to match several probes by coincidence but
    /// was never the real rule). Restore omits the `CHARSET` clause
    /// specifically when the name equals TiDB's own default charset
    /// (`UTF8MB4`, case-insensitively — the stored value here is always
    /// uppercased at parse time, matching the Go source's own lowercase-
    /// canonicalized comparison against `mysql.DefaultCharset`), regardless
    /// of `len` — confirmed via `godump restore` across many charset names.
    /// `CHARSET BINARY` restores with the type keyword itself printed as
    /// `BINARY` instead of `CHAR` (real TiDB's own restore logic special-
    /// cases exactly this one charset value) — but this is a RESTORE-TEXT
    /// substitution only, not a semantic equivalence to [`CastType::Binary`]:
    /// confirmed via `goeval` that `CAST('hi' AS CHAR(5) CHARSET binary)`
    /// does NOT right-pad like `CAST('hi' AS BINARY(5))` does (`LENGTH`
    /// gives `2`, not `5`) despite the two casts restoring to
    /// byte-identical text — a case where real TiDB's own `restore()`
    /// doesn't round-trip to identical execution semantics, so this
    /// variant must stay structurally separate from `Binary` even though
    /// restore sometimes prints them the same way.
    Char {
        /// The length, if given; `None` means unspecified (no truncation
        /// at evaluation time), NOT zero-length — `CHAR(0)` is a real,
        /// distinct, sane case (truncates to the empty string).
        len: Option<u32>,
        /// The charset name, if given (see this variant's own doc for the
        /// restore rule — not modelled computationally at evaluation time,
        /// same boundary as [`Expr::ConvertUsing`]'s own `charset` field).
        charset: Option<String>,
    },
    /// `BINARY[(len)]`. Unlike `CHAR`, a given length is a true FIXED
    /// width: shorter values are right-padded with `\0` bytes, not left
    /// as-is (confirmed via `goeval`: `CAST('hi' AS BINARY(5))` is
    /// `"hi\0\0\0"`, 5 bytes exactly) — `CHAR(N)` only ever truncates,
    /// never pads.
    Binary {
        /// The length, if given; `None` means unspecified (no
        /// truncation/padding).
        len: Option<u32>,
    },
    /// `DECIMAL[(flen[, scale])]`. `flen == 0` (written literally as
    /// `DECIMAL(0)`, or defaulted from a bare `DECIMAL(0, scale)`) is
    /// real MySQL/TiDB's own sentinel for "unspecified precision" —
    /// confirmed via `godump restore` (`DECIMAL(0)` restores with no
    /// parens at all, identically to some other unspecified-precision
    /// cases) — so it's treated as unclamped here too, rather than the
    /// pathological near-zero result real TiDB's own evaluator actually
    /// produces for it (an internal error silently recovered to `0`,
    /// confirmed via `goeval`'s own logged error output; not replicated —
    /// a genuinely degenerate edge case, not a realistic query). A bare
    /// `DECIMAL` with NO parens at all is a real, different default:
    /// `flen = 10, scale = 0` (confirmed via `godump restore`:
    /// `CAST(a AS DECIMAL)` restores as `CAST(a AS DECIMAL(10))`, a real
    /// clamp, not unspecified).
    Decimal {
        /// The total digit count, or `0` for "unspecified" (see above).
        flen: u32,
        /// The fractional digit count (`0` if not given).
        scale: u32,
    },
    /// `DATE`.
    Date,
    /// `DATETIME[(fsp)]`. `fsp` (fractional seconds precision) is parsed
    /// and restored for fidelity but not modelled at evaluation time —
    /// this crate's date values have no fractional-second component at
    /// all (see `tidb_expr::date_fn`'s own doc).
    DateTime {
        /// The fractional-seconds-precision digit, if given.
        fsp: Option<u32>,
    },
    /// `TIME[(fsp)]`. Parses and restores fully, but evaluation is
    /// deliberately `Unsupported` — MySQL `TIME` is an elapsed-time
    /// domain (can exceed 24 hours, can be negative), genuinely different
    /// from this crate's `DATE`/`DATETIME` (plain formatted strings) and
    /// not yet modelled at all (confirmed via `goeval`: a `TIME`-typed
    /// result is a `KindMysqlDuration` Datum, a value kind this project's
    /// oracle tooling doesn't even have a comparison label for yet).
    Time {
        /// The fractional-seconds-precision digit, if given.
        fsp: Option<u32>,
    },
    /// `YEAR`.
    Year,
    /// `DOUBLE`. Unlike `FLOAT`, no length/precision form was found to be
    /// accepted as a CAST target (`DOUBLE(10)` is a genuine `ParseError`,
    /// confirmed via `godump restore` — `DOUBLE(10, 2)` DOES parse but
    /// silently drops both numbers on restore, so it isn't modelled as
    /// carrying any payload here either).
    Double,
    /// `FLOAT`. A `FLOAT(p)` bit-precision argument is accepted
    /// syntactically (confirmed via `godump restore`: `p <= 24` restores
    /// as plain `FLOAT`, `25 <= p <= 53` restores as `DOUBLE` — TiDB
    /// resolves it to one of the two AT PARSE TIME) but is not modelled
    /// as a distinct payload here: this crate's `Value::Float` already
    /// covers `FLOAT`/`DOUBLE` uniformly (see that type's own doc), so
    /// the parser folds `FLOAT(p)` with `p > 24` directly into
    /// [`CastType::Double`] rather than carrying the original precision
    /// forward. A two-argument `FLOAT(M, D)` form is a genuine
    /// `ParseError` (confirmed via `godump restore`), matching `DOUBLE`'s
    /// own two-argument-only-past-that restriction being the OPPOSITE
    /// asymmetry — checked, not assumed uniform.
    Float,
    /// `JSON`. Parses and restores fully, but evaluation is deliberately
    /// `Unsupported` — this crate has no JSON value domain at all yet.
    Json,
}

/// Restores a typed date/time/timestamp expression. Bare SQL typed literals
/// contain a string and omit its ordinary `_UTF8MB4` introducer; ODBC
/// `{d|t|ts expression}` escapes may contain any expression and restore it
/// normally after the type keyword.
pub(crate) fn restore_typed_literal(keyword: &str, expr: &Expr, out: &mut String) {
    out.push_str(keyword);
    out.push(' ');
    if let Expr::String(text) = expr {
        out.push('\'');
        out.push_str(&escape_string_literal(text));
        out.push('\'');
    } else {
        expr.restore_into(out);
    }
}

/// Restores a [`CastType`] plus its own optional `ARRAY` suffix (see
/// [`CastExpr::array`]'s own doc), shared by [`CastStyle::Cast`]'s `AS
/// type` form, [`CastStyle::Convert`]'s `, type` form, and
/// [`CastStyle::JsonSumCrc32`]'s own `AS type` form.
pub(crate) fn restore_cast_type(ty: &CastType, array: bool, out: &mut String) {
    match ty {
        CastType::Signed => out.push_str("SIGNED"),
        CastType::Unsigned => out.push_str("UNSIGNED"),
        CastType::Char { len, charset } => {
            // See this variant's own doc: `CHARSET BINARY` prints the type
            // keyword itself as `BINARY` (real TiDB's own restore rule, a
            // text substitution only — NOT the same as `CastType::Binary`
            // at evaluation time). `len` restores independently of
            // `charset` either way.
            let charset_is_binary = charset.as_deref() == Some("BINARY");
            out.push_str(if charset_is_binary { "BINARY" } else { "CHAR" });
            if let Some(n) = len {
                out.push('(');
                out.push_str(&n.to_string());
                out.push(')');
            }
            if !charset_is_binary {
                if let Some(cs) = charset {
                    // The default charset is never printed — real TiDB's
                    // own restore omits `CHARSET` entirely when it equals
                    // `mysql.DefaultCharset` (`UTF8MB4`).
                    if cs != "UTF8MB4" {
                        out.push_str(" CHARSET ");
                        out.push_str(cs);
                    }
                }
            }
        }
        CastType::Binary { len } => {
            out.push_str("BINARY");
            if let Some(n) = len {
                out.push('(');
                out.push_str(&n.to_string());
                out.push(')');
            }
        }
        CastType::Decimal { flen, scale } => {
            out.push_str("DECIMAL");
            if *flen > 0 {
                out.push('(');
                out.push_str(&flen.to_string());
                if *scale > 0 {
                    out.push_str(", ");
                    out.push_str(&scale.to_string());
                }
                out.push(')');
            }
        }
        CastType::Date => out.push_str("DATE"),
        CastType::DateTime { fsp } => {
            out.push_str("DATETIME");
            if let Some(n) = fsp {
                out.push('(');
                out.push_str(&n.to_string());
                out.push(')');
            }
        }
        CastType::Time { fsp } => {
            out.push_str("TIME");
            if let Some(n) = fsp {
                out.push('(');
                out.push_str(&n.to_string());
                out.push(')');
            }
        }
        CastType::Year => out.push_str("YEAR"),
        CastType::Double => out.push_str("DOUBLE"),
        CastType::Float => out.push_str("FLOAT"),
        CastType::Json => out.push_str("JSON"),
    }
    if array {
        out.push_str(" ARRAY");
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for CastStyle {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Cast => {}
            Self::Convert => {}
            Self::BinaryOperator => {}
            Self::DateLiteral => {}
            Self::TimeLiteral => {}
            Self::TimestampLiteral => {}
            Self::JsonSumCrc32 => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CastExpr {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            expr,
            cast_type,
            style,
            array,
        } = self;
        if !crate::Visitable::accept(expr.as_mut(), visitor) {
            return false;
        }
        if !crate::Visitable::accept(cast_type, visitor) {
            return false;
        }
        if !crate::Visitable::accept(style, visitor) {
            return false;
        }
        let _ = expr;
        let _ = cast_type;
        let _ = style;
        let _ = array;
        visitor.leave(self)
    }
}

impl crate::Visitable for CastType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Signed => {}
            Self::Unsigned => {}
            Self::Char { len, charset } => {
                let _ = len;
                let _ = charset;
            }
            Self::Binary { len } => {
                let _ = len;
            }
            Self::Decimal { flen, scale } => {
                let _ = flen;
                let _ = scale;
            }
            Self::Date => {}
            Self::DateTime { fsp } => {
                let _ = fsp;
            }
            Self::Time { fsp } => {
                let _ = fsp;
            }
            Self::Year => {}
            Self::Double => {}
            Self::Float => {}
            Self::Json => {}
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
