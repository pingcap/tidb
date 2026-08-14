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

//! The expression AST: the [`Expr`] tree, its derived flags, and the restore
//! helpers its variants share. Restore/format, casts, and operators live in
//! the sibling modules below.

mod cast;
use cast::restore_cast_type;
use tidb_mysql::to_lowercase as identifier_to_lower;
mod op;
mod restore;

pub use cast::*;
pub use op::*;

use crate::select::restore_window_def;
use crate::util::{
    back_quote, escape_string_literal, format_go_float, normalize_decimal, normalize_int,
};
use crate::{Op, OrderItem, QueryStmt, RestoreContext, RestoreFlags, WindowOver};

/// Expression flag bits from `pkg/parser/ast/ast.go`.
pub const FLAG_CONSTANT: u64 = 0;
/// Contains a prepared-statement parameter marker.
pub const FLAG_HAS_PARAM_MARKER: u64 = 1 << 1;
/// Contains an ordinary scalar function.
pub const FLAG_HAS_FUNC: u64 = 1 << 2;
/// Contains a column or positional reference.
pub const FLAG_HAS_REFERENCE: u64 = 1 << 3;
/// Contains an aggregate function.
pub const FLAG_HAS_AGGREGATE_FUNC: u64 = 1 << 4;
/// Contains a subquery.
pub const FLAG_HAS_SUBQUERY: u64 = 1 << 5;
/// Contains a user or system variable.
pub const FLAG_HAS_VARIABLE: u64 = 1 << 6;
/// Contains a `DEFAULT` expression.
pub const FLAG_HAS_DEFAULT: u64 = 1 << 7;
/// Was pre-evaluated by an earlier phase.
pub const FLAG_PRE_EVALUATED: u64 = 1 << 8;
/// Contains a window function.
pub const FLAG_HAS_WINDOW_FUNC: u64 = 1 << 9;

/// A decoded string value together with the connection character metadata
/// attached by Go's parser driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedString {
    /// Decoded string contents.
    pub value: String,
    /// Connection character set attached to the value.
    pub charset: String,
    /// Connection collation attached to the value.
    pub collation: String,
}

impl TypedString {
    /// Creates a typed string from its decoded value and connection metadata.
    pub fn new(
        value: impl Into<String>,
        charset: impl Into<String>,
        collation: impl Into<String>,
    ) -> Self {
        Self {
            value: value.into(),
            charset: charset.into(),
            collation: collation.into(),
        }
    }
}

impl From<String> for TypedString {
    fn from(value: String) -> Self {
        Self::new(
            value,
            tidb_mysql::DefaultCharset,
            tidb_mysql::DefaultCollationName,
        )
    }
}

impl std::ops::Deref for TypedString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

/// The scope of a system variable (`@@GLOBAL.x`, `@@SESSION.x`, or
/// `@@INSTANCE.x`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SysVarScope {
    /// `@@GLOBAL.`.
    Global,
    /// `@@SESSION.` (also `@@LOCAL.`).
    Session,
    /// `@@INSTANCE.`.
    Instance,
}

/// Go's decoded `types.BitLiteral` payload as stored in a value-expression AST.
///
/// The AST owns bytes rather than source digits: `b'1'` and `b'00000001'`
/// are the same one-byte value, while `b'000000001'` is the distinct two-byte
/// value `[0, 1]`. This makes derived expression equality match Go and keeps
/// byte width available to type inference without retaining irrelevant source
/// spelling.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct BitLiteralValue(Vec<u8>);

impl BitLiteralValue {
    /// Decodes the digit span inside `0b…` / `b'…'` syntax.
    #[must_use]
    pub fn from_digits(digits: &str) -> Self {
        if digits.is_empty() {
            return Self::default();
        }
        let mut bytes = vec![0_u8; digits.len().div_ceil(8)];
        let padding = bytes.len() * 8 - digits.len();
        for (index, digit) in digits.bytes().enumerate() {
            let bit = match digit {
                b'0' => 0,
                b'1' => 1,
                _ => unreachable!("the lexer admits only binary digits"),
            };
            let aligned = padding + index;
            bytes[aligned / 8] |= bit << (7 - aligned % 8);
        }
        Self(bytes)
    }

    /// The unchanged byte-aligned payload.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Go `BinaryLiteral.ToBitLiteralString(true)`'s digits without wrapper.
    #[must_use]
    pub fn restored_digits(&self) -> String {
        if self.0.is_empty() {
            return String::new();
        }
        let mut digits = String::with_capacity(self.0.len() * 8);
        for byte in &self.0 {
            use std::fmt::Write;
            write!(digits, "{byte:08b}").expect("writing to String cannot fail");
        }
        let trimmed = digits.trim_start_matches('0');
        if trimmed.is_empty() {
            "0".to_owned()
        } else {
            trimmed.to_owned()
        }
    }
}

/// A scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A qualified column reference, e.g. `["t", "a"]`.
    Column(Vec<String>),
    /// A prepared-statement parameter marker (`?`).
    ///
    /// The parser assigns positions from zero in left-to-right source order
    /// and restarts numbering for every statement. A marker has no SQL-text
    /// value: a prepared-statement owner must bind its typed execute value
    /// before lowering the expression into an executable plan.
    ParamMarker {
        /// Byte offset in the original SQL text.
        offset: usize,
        /// Zero-based, statement-local marker order.
        order: usize,
        /// Whether an execute-time value has been installed.
        in_execute: bool,
        /// Projection offset used by positional-expression lowering.
        /// The Go driver's embedded zero-value `ValueExpr` initializes this to 0.
        projection_offset: isize,
    },
    /// An integer literal (original digits).
    Int(String),
    /// A fixed-point decimal literal (original text).
    Decimal(String),
    /// A floating-point literal's parsed value.
    Float(f64),
    /// A hexadecimal literal's normalized lowercase, even-length hex digits.
    Hex(String),
    /// A decoded bit-literal value.
    Bit(BitLiteralValue),
    /// A string literal's decoded value (delimiters and doubling removed).
    String(String),
    /// A bare quoted string, restoring WITHOUT [`Expr::String`]'s own
    /// `_UTF8MB4` charset-introducer prefix — used for a synthetic
    /// string value real TiDB's own AST attaches with no charset info at
    /// all (unlike an ordinary string literal, which always inherits the
    /// connection's default charset). Confirmed via `godump restore`:
    /// `CHAR(1 USING gbk)` restores its trailing charset-name argument as
    /// plain `'gbk'`, not `_UTF8MB4'gbk'` — see `tidb-parser`'s own
    /// `parse_char_func` (the only call site so far). A leaf literal like
    /// [`Expr::String`], so it needs no arm in any of `tidb-exec`'s
    /// aggregate/subquery/window traversal functions.
    RawString(String),
    /// A character-set-introduced string literal (`_latin1'x'`,
    /// `_ascii'x'`, `N'x'`/`n'x'` — see `tidb_lexer::TokenKind::
    /// CharsetIntroducer`'s own doc for the token-level distinction this
    /// relies on), for any charset OTHER than the connection's own
    /// default `utf8mb4` — an explicit `_utf8mb4'x'` introducer restores
    /// BYTE-IDENTICALLY to a plain [`Expr::String`] (confirmed via
    /// `godump restore`), so that ONE case reuses the existing variant
    /// instead, no dedicated representation needed. `N'x'`/`n'x'` map to
    /// charset `"UTF8"` specifically, NOT `"UTF8MB4"` (a real, easy-to-
    /// miss distinction confirmed via `godump restore` and read directly
    /// from `pkg/parser/lexer.go`'s own `startWithNn`: `lit = "utf8"`).
    /// Restores as `_<CHARSET>'<value>'`, charset canonically uppercased.
    /// No value domain models an explicit non-default charset (this
    /// crate's own string handling is charset-agnostic beyond the default),
    /// so evaluation is `Unsupported`, relying on `tidb_expr`'s existing
    /// generic wildcard — the SAME scope-cut as `Expr::MemberOf`/
    /// `Expr::Trim`/... A leaf literal, so it needs no arm in any of
    /// `tidb-exec`'s aggregate/subquery/window traversal functions,
    /// matching [`Expr::RawString`]'s own precedent exactly.
    CharsetString {
        /// The canonical charset name, uppercased (`LATIN1`, `ASCII`,
        /// `UTF8`, `BINARY`, ...) — never `UTF8MB4`, see this variant's
        /// own doc for why.
        charset: String,
        /// The string literal's decoded value.
        value: String,
    },
    /// A charset introducer applied to a hexadecimal or bit literal.
    CharsetBinary {
        /// Canonical uppercase charset name.
        charset: String,
        /// The introduced [`Expr::Hex`] or [`Expr::Bit`] literal.
        value: Box<Expr>,
    },
    /// The `NULL` literal.
    Null,
    /// A boolean literal (`TRUE` / `FALSE`).
    Bool(bool),
    /// `DEFAULT` or `DEFAULT(column)`. `None` is the bare value placeholder
    /// accepted only by INSERT/UPDATE assignment grammar.
    Default(Option<Vec<String>>),
    /// A user variable reference `@name`.
    UserVar(String),
    /// A system variable reference `@@[scope.]name`.
    SysVar {
        /// The optional explicit scope.
        scope: Option<SysVarScope>,
        /// The variable name.
        name: String,
    },
    /// An inline user-variable assignment expression, `@name := value`
    /// (usable anywhere an ordinary expression can appear, not just in a
    /// standalone `SET` statement). Restores as `` @`name`:=value `` with
    /// NO spaces around `:=` (confirmed via `godump restore`). `value` is
    /// parsed at the LOWEST precedence (matching real TiDB's own
    /// `p.parseExpression(precNone)`), so it can itself absorb a FURTHER
    /// nested `:=` (`@i := @j := 3` chains right-associatively, each
    /// assignment's own `value` recursing into the next).
    ///
    /// A genuine, if obscure, MySQL/TiDB quirk this variant's own
    /// construction site (`tidb-parser`'s own variable-atom parsing)
    /// must replicate: `:=` following ANY variable atom — even one
    /// written with a `@@` system-variable marker, WITH OR WITHOUT an
    /// explicit `GLOBAL`/`SESSION`/`LOCAL` scope — always targets a
    /// PLAIN user variable using the atom's own bare name, discarding
    /// the `@@` marker and any scope entirely (confirmed via `godump
    /// restore`: `@@session.autocommit := 1` and `@@global.autocommit :=
    /// 1` both restore as `` @`autocommit`:=1 ``, byte-identical to
    /// plain `@autocommit := 1` — real TiDB's own hand-written parser,
    /// `pkg/parser/expr_parser.go`, explicitly sets `IsSystem = false`
    /// the moment it sees `:=` follow a variable atom, regardless of how
    /// that atom was originally written). This is NOT a real system-
    /// variable write.
    Assign {
        /// The assigned-to user variable's bare name (never `@@`-
        /// prefixed or scope-qualified, even if the source wrote it
        /// that way — see this variant's own doc).
        name: String,
        /// The assigned value.
        value: Box<Expr>,
    },
    /// A prefix unary operation.
    Unary(UnaryOp, Box<Expr>),
    /// An infix binary operation.
    Binary(BinaryOp, Box<Expr>, Box<Expr>),
    /// A parenthesized expression (parentheses are preserved, as in the Go AST).
    Paren(Box<Expr>),
    /// A row/tuple constructor, 2+ elements, used for row-wise comparison
    /// (`ROW(1,2) > ROW(3,4)`, lexicographic) and tuple membership
    /// (`ROW(a,b) IN (ROW(1,2), ROW(3,4))`). Two source syntaxes build the
    /// SAME node and restore IDENTICALLY (confirmed via `godump restore`):
    /// the explicit `ROW(expr, expr, ...)` keyword form, and a bare
    /// `(expr, expr, ...)` — a plain parenthesized list with 2+
    /// comma-separated elements, distinct from [`Expr::Paren`]'s own
    /// single-element form (`(1)` stays `Paren`, `(1,2)` becomes `Row`).
    /// Read directly from `pkg/parser/expr_prefix_parser.go`'s
    /// `parsePrefixKeywordExpr` (`case row:`, which rejects fewer than 2
    /// elements — `ROW(1)` is a genuine `ParseError`) and
    /// `pkg/parser/expr_subquery_parser.go`'s `parseParenOrSubquery`
    /// (which builds the identical `ast.RowExpr` from the bare-paren
    /// form). Restores as `ROW(...)`, commas with NO trailing space
    /// (`ROW(1,2,3)`, unlike every other comma-separated list in this
    /// crate) — a real, deliberate MySQL restore quirk, not an
    /// inconsistency to "fix". Row-wise comparison/membership needs a
    /// genuine new tuple-shaped `Value` domain this crate doesn't have yet
    /// (the SAME kind of gap as JSON, deliberately deferred) — evaluation
    /// is a clean `Unsupported`, parse/restore fidelity only.
    Row(Vec<Expr>),
    /// A function call `name(args...)`.
    Func {
        /// The function name (restored uppercase).
        name: String,
        /// The call arguments.
        args: Vec<Expr>,
        /// Byte offset of the function name in the original SQL.
        origin_position: usize,
    },
    /// A schema-qualified GENERIC function call: `schema.func(args...)`
    /// — a real, separate grammar shape from a plain builtin call
    /// ([`Expr::Func`] above), confirmed via `godump restore`: BOTH the
    /// schema AND the function name restore BACK-QUOTED and
    /// CASE-PRESERVED (`` `T`.`upper`(1) ``, `` `t`.`upper`(1) ``) —
    /// unlike a builtin call's own canonical-uppercase, unquoted
    /// restore. Read directly from `pkg/parser/expr_parser.go`'s
    /// `parseIdentOrFuncCall`: it builds a `FuncCallExprTypeGeneric`
    /// node whenever a bare identifier is followed by `.` then ANOTHER
    /// identifier then `(` — the schema name is NOT checked against any
    /// real schema catalog at parse time, so `anything.anything(...)`
    /// parses this way regardless of whether `anything` names a real
    /// database.
    GenericFuncCall {
        /// The schema/qualifier name, restored back-quoted, case-preserved.
        schema: String,
        /// The function name, restored back-quoted, case-preserved.
        name: String,
        /// The call arguments.
        args: Vec<Expr>,
        /// Byte offset of the schema name in the original SQL.
        origin_position: usize,
    },
    /// An aggregate function call `NAME([DISTINCT] arg [, arg ...])` (e.g.
    /// `COUNT`, `SUM`). Every aggregate this crate models takes exactly one
    /// argument EXCEPT `COUNT(DISTINCT a, b, ...)` — real MySQL/TiDB grammar
    /// generically allows a comma-separated arg list here (confirmed by
    /// reading `pkg/parser/expr_func_parser.go`'s `parseAggregateFuncCall`
    /// directly: the arg-list parse itself has no arity limit), then rejects
    /// `len(args) > 1` for every function name EXCEPT `count` (which
    /// additionally requires `DISTINCT`) and `approx_count_distinct`/
    /// `approx_percentile` (unconditional) — `tidb_parser::parse_aggregate`
    /// applies the same name-keyed post-check. `COUNT(*)` is modelled as the
    /// single-element literal `1` (matching the Go AST's `COUNT(1)` restore).
    Aggregate {
        /// The canonical aggregate name (restored uppercase).
        name: String,
        /// Whether `DISTINCT` was specified.
        distinct: bool,
        /// The argument list — always exactly one element except
        /// `COUNT(DISTINCT a, b, ...)`, see this variant's own doc.
        args: Vec<Expr>,
    },
    /// `GROUP_CONCAT([DISTINCT] arg [, arg ...] [ORDER BY item [,item ...]]
    /// [SEPARATOR sep])` — a separate shape from [`Expr::Aggregate`] since
    /// it takes multiple arguments (concatenated per row, like `CONCAT`)
    /// and a separator between rows, rather than a single argument.
    /// `order_by` controls the ROW ORDER within the concatenated result
    /// (confirmed via `godump restore`: a positional item like `ORDER BY
    /// 1` refers to `args`' OWN position, a separate scope from the outer
    /// `SELECT`'s own `ORDER BY`) — see `tidb-exec`'s own
    /// `aggregate::Database::group_concat_order` for the exact
    /// sort/dedup interaction with `DISTINCT`.
    GroupConcat {
        /// Whether `DISTINCT` was specified.
        distinct: bool,
        /// The per-row arguments, concatenated like `CONCAT(args...)`.
        args: Vec<Expr>,
        /// The row ordering, empty if not written.
        order_by: Vec<OrderItem>,
        /// The separator between rows; `,` if not written (always restored
        /// explicitly, matching the Go AST's normalization).
        separator: TypedString,
    },
    /// A window function call: `NAME(args...) OVER (window_spec)`. Five
    /// shapes share this one node, distinguished by `name` and `args.len()`:
    /// the zero-argument ranking functions (`ROW_NUMBER`/`RANK`/
    /// `DENSE_RANK`, confirmed via `godump restore` that real MySQL
    /// grammar takes no arguments for these three); a frame-based window
    /// AGGREGATE (`COUNT`/`SUM`/`AVG`/`MAX`/`MIN`, one argument — the SAME
    /// single-argument shape [`Expr::Aggregate`] uses, `COUNT(*)` modelled
    /// as the literal `1` the same way); `FIRST_VALUE`/`LAST_VALUE` (one
    /// argument) and `NTH_VALUE` (two: the value expression, then a
    /// 1-based position); `LAG`/`LEAD` (one to three: the value
    /// expression, an optional offset — `1` if unwritten — and an
    /// optional default value used when the offset falls outside the
    /// partition — `NULL` if unwritten); and `NTILE` (one argument, a
    /// positive bucket count) alongside the zero-argument `PERCENT_RANK`/
    /// `CUME_DIST`. Aggregate `DISTINCT`, `IGNORE NULLS`, and `FROM LAST`
    /// remain explicit fields because Go's AST preserves them even when a
    /// later semantic phase rejects a particular function/modifier pair. The
    /// `OVER` clause itself may be
    /// a bare or parenthesized named-window reference in addition to a
    /// fully inline spec (see [`WindowOver`]) — resolving a name against
    /// the enclosing [`SelectStmt::windows`] clause, and validating what
    /// an extension may add, both happen in `tidb_exec`, not here.
    Window {
        /// The canonical (uppercase) function name.
        name: String,
        /// The call arguments — empty for a ranking function; see this
        /// variant's own doc for each function's exact arity.
        args: Vec<Expr>,
        /// Whether the aggregate arguments were prefixed with `DISTINCT`.
        distinct: bool,
        /// Whether `IGNORE NULLS` was specified.
        ignore_nulls: bool,
        /// Whether `FROM LAST` was specified.
        from_last: bool,
        /// The `OVER` clause.
        over: WindowOver,
    },
    /// `INTERVAL value unit` — a general prefix expression (usable anywhere
    /// an expression is expected, matching real MySQL grammar, though this
    /// codebase's evaluator currently only gives it meaning as `DATE_ADD`/
    /// `DATE_SUB`'s second argument). `unit` is the keyword text
    /// (canonically uppercased, e.g. `"DAY"`), not a closed enum — the
    /// parser accepts any unit keyword syntactically; only some are
    /// semantically supported by the evaluator.
    Interval {
        /// The interval's magnitude.
        value: Box<Expr>,
        /// The unit keyword, canonically uppercased (`DAY`, `MONTH`, ...).
        unit: String,
    },
    /// `EXTRACT(unit FROM expr)` — a genuinely separate general-purpose
    /// extraction syntax from `Interval` above, with its OWN grammar
    /// (`unit FROM value`, not `value unit`) and restore form (confirmed
    /// via `godump restore`: `EXTRACT(YEAR FROM \`a\`)`, unit first). Same
    /// scope note as `Interval`'s own `unit` field: the parser accepts any
    /// unit keyword syntactically (including MySQL's real compound units
    /// like `DAY_HOUR`/`YEAR_MONTH`, confirmed via `goeval`), but this
    /// codebase's evaluator only gives meaning to the simple units it
    /// already supports as standalone functions (`YEAR`/`MONTH`/`DAY`/
    /// `QUARTER`/`HOUR`/`MINUTE`/`SECOND`).
    Extract {
        /// The unit keyword, canonically uppercased (`YEAR`, `DAY_HOUR`, ...).
        unit: String,
        /// The value to extract from.
        value: Box<Expr>,
    },
    /// `POSITION(substr IN str)` — a dedicated syntax for `LOCATE(substr,
    /// str)`, restoring with the `IN` keyword between its two arguments
    /// rather than the plain `NAME(args...)` shape [`Expr::Func`] produces
    /// (confirmed via reading `pkg/parser/ast/functions.go`'s own
    /// `FuncCallExpr.Restore`, `case "position":` directly — real TiDB
    /// itself models this as an ordinary `FuncCallExpr` with `FnName`
    /// `"position"`, only the RESTORE is custom), so this needs its own
    /// AST variant rather than `Expr::Func`, the SAME "dedicated variant
    /// over restore-hack" choice already made for `Expr::Trim`/
    /// `Expr::MemberOf`/`Expr::Row`/... `substr` parses at
    /// `prec::PREDICATE + 1` (matching real TiDB's own
    /// `parsePositionFunc`: `p.parseExpression(precPredicate + 1)`) so
    /// the `IN` keyword is never swallowed as the SQL `IN` predicate
    /// operator while parsing `substr` itself.
    Position {
        /// The substring being searched for.
        substr: Box<Expr>,
        /// The string being searched.
        str: Box<Expr>,
    },
    /// `WEIGHT_STRING(expr [AS {CHAR|CHARACTER|BINARY}(len)])` — read
    /// directly from `pkg/parser/expr_func_parser.go`'s
    /// `parseWeightStringFuncCall` and `pkg/parser/ast/functions.go`'s
    /// own `case WeightString:` custom restore: a genuinely SEPARATE
    /// grammar from `Expr::Func`'s plain `NAME(args...)` shape (real
    /// TiDB's `AS` clause restores as `AS {CHAR|BINARY}(len)`, not a
    /// third comma-separated argument), so this needs its own AST
    /// variant — the SAME "dedicated variant over restore-hack" choice
    /// already made for `Expr::Trim`/`Expr::Position`/... `CHARACTER` is
    /// a real synonym for `CHAR` here (confirmed via the Go source's own
    /// `case charType, character:` — both produce the SAME `"CHAR"` type
    /// name), so this collapses both spellings to
    /// `WeightStringType::Char` at parse time; the two restore
    /// identically. `len` is a plain non-negative integer with no upper
    /// bound enforced at parse time (confirmed via `godump restore`:
    /// `WEIGHT_STRING('ab' AS BINARY(1000000000000000000))` restores
    /// its huge length back VERBATIM, comfortably within `u64` range).
    /// Evaluation is `Unsupported`, relying on `tidb_expr`'s existing
    /// generic wildcard — this crate models no byte-level collation
    /// comparison-key domain at all, the SAME scope-cut as
    /// `Expr::Trim`/`Expr::Position`/`Expr::MemberOf`.
    WeightString {
        /// The value to compute a weight string for.
        expr: Box<Expr>,
        /// The optional `AS {CHAR|BINARY}(len)` truncation clause.
        as_type: Option<(WeightStringType, u64)>,
    },
    /// `TRIM(expr)` / `TRIM([remstr] FROM expr)` / `TRIM(direction
    /// [remstr] FROM expr)` — a genuinely dedicated grammar with a
    /// CUSTOM restore, not the plain `NAME(args...)` shape
    /// [`Expr::Func`] produces (confirmed via reading `pkg/parser/
    /// ast/functions.go`'s own `FuncCallExpr.Restore`, `case "trim":`
    /// directly), so this needs its own AST variant rather than
    /// `Expr::Func` — the SAME "dedicated variant over restore-hack"
    /// choice already made for `Expr::MemberOf`/`Expr::Row`/... This
    /// also fixes a genuine pre-existing bug: before this variant
    /// existed, `TRIM` had no dedicated dispatch at all and fell
    /// through the ordinary non-reserved-keyword function-call path,
    /// wrongly ACCEPTING `TRIM(a, b)` (comma-separated multi-args) —
    /// confirmed via `godump restore` that real TiDB rejects this as a
    /// genuine `ParseError` (its own hand-written `parseTrimFunc`,
    /// `pkg/parser/expr_cast_parser.go`, never has a comma-list path at
    /// all).
    Trim {
        /// The value being trimmed.
        expr: Box<Expr>,
        /// The substring to remove. `None` ONLY for the bare `TRIM(expr)`
        /// form — `direction.is_some()` always implies `Some` here too
        /// (real TiDB's own grammar defaults an omitted `remstr` to a
        /// single-space string literal when a `direction` is given, and
        /// that DEFAULTED space value restores explicitly, e.g. `TRIM(BOTH
        /// FROM x)` restores as `` TRIM(BOTH `` `' '` `` FROM x) ``,
        /// confirmed via `godump restore`). A genuine restore quirk this
        /// field must preserve exactly rather than collapsing to `None`:
        /// an EXPLICIT `Expr::Null` remstr (`TRIM(NULL FROM x)`,
        /// `TRIM(LEADING NULL FROM x)`) restores with the `NULL` OMITTED
        /// entirely (`TRIM(FROM x)` / `TRIM(LEADING FROM x)`) — a real,
        /// narrow special case in real TiDB's own restore (checked by
        /// VALUE, not by whether the source wrote anything), NOT the same
        /// as this field being `None`.
        remstr: Option<Box<Expr>>,
        /// The trim direction, if given (`None` for both the bare
        /// `TRIM(expr)` form AND the `TRIM(remstr FROM expr)` form with
        /// no direction keyword — real TiDB's own restore genuinely can't
        /// tell those two `None` cases apart from `direction` alone,
        /// distinguishing them via `remstr` instead).
        direction: Option<TrimDirection>,
    },
    /// `TIMESTAMPADD(unit, interval, datetime_expr)` — a THIRD, again
    /// genuinely separate unit-taking syntax alongside [`Expr::Interval`]/
    /// [`Expr::Extract`], with its OWN grammar and restore form: unlike
    /// either of those, `unit` here is a plain, ordinary, COMMA-separated
    /// function argument (confirmed via `godump restore`:
    /// `TIMESTAMPADD(HOUR, 1, x)`, no `FROM`, no bare juxtaposition). Read
    /// directly from real TiDB's own hand-written parser
    /// (`pkg/parser/expr_func_parser.go`'s `parseTimestampAddFuncCall`)
    /// rather than guessed, since a bare unit keyword used as an ordinary
    /// function argument would otherwise be indistinguishable from a
    /// column reference once non-reserved keywords are accepted as
    /// identifiers (see [`Expr::Column`]'s own doc) — TIMESTAMPADD's own
    /// first argument position is a genuinely different grammar
    /// production, not a real expression at all.
    TimestampAdd {
        /// The unit keyword, canonically uppercased.
        unit: String,
        /// The signed magnitude to add.
        interval: Box<Expr>,
        /// The datetime expression added to.
        expr: Box<Expr>,
    },
    /// `TIMESTAMPDIFF(unit, expr1, expr2)` — see [`Expr::TimestampAdd`]'s
    /// own doc for why `unit` needs its own dedicated field rather than
    /// being an ordinary argument `Expr`.
    TimestampDiff {
        /// The unit keyword, canonically uppercased.
        unit: String,
        /// The earlier datetime expression.
        expr1: Box<Expr>,
        /// The later datetime expression.
        expr2: Box<Expr>,
    },
    /// `GET_FORMAT(DATE|TIME|DATETIME|TIMESTAMP, format_expr)` — MySQL's
    /// own date-format-string lookup function. `TIMESTAMP` and `DATETIME`
    /// are genuine SYNONYMS at this position, both restoring as `DATETIME`
    /// (confirmed via `godump restore`: `GET_FORMAT(TIMESTAMP, ...)`
    /// restores as `GET_FORMAT(DATETIME, ...)`) — real TiDB's own AST
    /// collapses both into the SAME selector value
    /// (`pkg/parser/expr_func_parser.go`'s `parseGetFormatFuncCall`), so
    /// [`GetFormatSelector`] only has three variants, not four.
    GetFormat {
        /// The format-type selector.
        selector: GetFormatSelector,
        /// The format-name expression (a string, e.g. `'jis'`).
        expr: Box<Expr>,
    },
    /// `expr [NOT] IN (list...)`.
    In {
        /// The tested expression.
        expr: Box<Expr>,
        /// The value list.
        list: Vec<Expr>,
        /// Whether negated (`NOT IN`).
        not: bool,
    },
    /// `expr [NOT] BETWEEN low AND high`.
    Between {
        /// The tested expression.
        expr: Box<Expr>,
        /// The lower bound.
        low: Box<Expr>,
        /// The upper bound.
        high: Box<Expr>,
        /// Whether negated (`NOT BETWEEN`).
        not: bool,
    },
    /// `expr [NOT] LIKE pattern [ESCAPE 'char']`. Real MySQL/TiDB's own
    /// default escape character (when `ESCAPE` is omitted entirely) is
    /// `\` — read directly from `pkg/parser/expr_parser.go`'s
    /// `parseLikeExpr`, which sets `Escape = '\\'` unconditionally in
    /// that case. `escape` here is `None` for BOTH that default case
    /// AND — a real, deliberate restore-elision quirk, NOT modelled as
    /// two separate booleans (`EscapeExplicit`/`Escape` in the Go AST,
    /// collapsed here since they always move together in every case
    /// that matters for restore) — an EXPLICIT `ESCAPE '\'` matching
    /// that same default: confirmed via `godump restore`, `LIKE 'x'
    /// ESCAPE '\\'` restores with NO visible `ESCAPE` clause at all,
    /// identical to omitting it entirely (real TiDB's own
    /// `PatternLikeOrIlikeExpr.Restore`: `if n.EscapeExplicit &&
    /// n.Escape != '\\'`). `Some(0)` represents the OTHER real, distinct
    /// shape — `ESCAPE ''` (an explicit, deliberately EMPTY escape
    /// string, meaning "no escape character at all", confirmed via
    /// `godump restore` to restore as `ESCAPE ''`, not omitted) — a MySQL
    /// grammar quirk allowing a 0-length string literal there
    /// specifically (any length other than 0 or 1 is a genuine
    /// `ParseError`, `ErrWrongArguments`, confirmed via the same Go
    /// source).
    Like {
        /// The tested expression.
        expr: Box<Expr>,
        /// The pattern expression.
        pattern: Box<Expr>,
        /// Whether negated (`NOT LIKE`).
        not: bool,
        /// Whether the source operator was `ILIKE` rather than `LIKE`.
        ilike: bool,
        /// The explicit escape byte, if written and worth restoring —
        /// see this variant's own doc for the exact `None`/`Some(0)`/
        /// `Some(byte)` distinction.
        escape: Option<u8>,
    },
    /// `expr [NOT] REGEXP pattern` — a regular-expression match predicate,
    /// at the SAME precedence level as [`Expr::Like`]/[`Expr::In`]/
    /// [`Expr::Between`] (confirmed via `godump restore`). `RLIKE` is a
    /// real MySQL synonym that normalizes to `REGEXP` on restore — both
    /// spellings share this one representation, the same "synonym
    /// spellings restore identically" precedent `LOCK IN SHARE MODE`/
    /// `FOR SHARE` already established (see [`crate::LockKind`]'s own
    /// doc). Evaluation is deliberately `Unsupported`: this crate has no
    /// regex engine and zero external dependencies at all today; adding
    /// one (e.g. the `regex` crate) would be a genuinely separate,
    /// bigger decision — this project's FIRST external dependency — left
    /// for a dedicated future turn, not bundled into parse/restore
    /// support. A bare `BINARY` prefix on the pattern (`a REGEXP BINARY
    /// 'x'`, real MySQL grammar making the match case-sensitive) is a
    /// separate, general prefix construct this parser doesn't model at
    /// all yet (confirmed via `godump restore` to restore differently
    /// from `CAST(expr AS BINARY)`, so it isn't simply reducible to the
    /// existing [`CastType::Binary`]) — a known, narrower, deliberately
    /// deferred gap, not silently mishandled (still a genuine
    /// `ParseError` here).
    Regexp {
        /// The tested expression.
        expr: Box<Expr>,
        /// The pattern expression.
        pattern: Box<Expr>,
        /// Whether negated (`NOT REGEXP`).
        not: bool,
    },
    /// `expr IS [NOT] <target>`.
    Is {
        /// The tested expression.
        expr: Box<Expr>,
        /// The `IS` target.
        target: IsTarget,
        /// Whether negated (`IS NOT`).
        not: bool,
    },
    /// A parenthesized scalar subquery `(SELECT ...)` or a set-operation
    /// query `(SELECT ... UNION SELECT ...)`.
    ///
    /// TiDB's Go `parseSubquery` keeps the complete query shape in this
    /// position, including a top-level set operation. Keeping the typed
    /// [`QueryStmt`] envelope here preserves that source grammar instead of
    /// rejecting a scalar subquery solely because its body is a `UNION`.
    Subquery(crate::NodeBox<QueryStmt>),
    /// `[NOT] EXISTS (SELECT ...)` or a set-operation query.
    ///
    /// TiDB's `parseExistsSubquery` delegates to the general subquery
    /// parser, so `EXISTS (SELECT ... UNION [ALL] SELECT ...)` is a real
    /// source shape. Keep the query behind the same typed envelope used by
    /// `IN` subqueries instead of flattening a set operation into one
    /// `SelectStmt` and losing its semantics.
    Exists {
        /// The subquery.
        subquery: crate::NodeBox<QueryStmt>,
        /// Whether negated (`NOT EXISTS`).
        not: bool,
    },
    /// `expr [NOT] IN (SELECT ...)`, whose subquery may be
    /// `UNION`/`EXCEPT`/`INTERSECT`-bodied. Confirmed via `godump restore`:
    /// `x NOT IN
    /// (SELECT 1 UNION SELECT 2)` restores unchanged. Always either
    /// `QueryStmt::Select` or `QueryStmt::SetOpr` (real TiDB's own
    /// `parseSubquery`/this crate's own `Parser::parse_select_or_setopr`,
    /// which this variant's own parsing calls directly — never any other
    /// `Stmt` variant).
    InSubquery {
        /// The tested expression.
        expr: Box<Expr>,
        /// The subquery.
        subquery: crate::NodeBox<QueryStmt>,
        /// Whether negated (`NOT IN`).
        not: bool,
    },
    /// `expr <op> ANY|ALL (query)`, including a set-operation query.
    CompareSubquery {
        /// The comparison operator.
        op: BinaryOp,
        /// The left operand.
        left: Box<Expr>,
        /// `true` for `ALL`, `false` for `ANY`/`SOME`.
        all: bool,
        /// The subquery.
        subquery: crate::NodeBox<QueryStmt>,
    },
    /// `CASE [value] (WHEN cond THEN result)+ [ELSE result] END` — `value`
    /// present is the "simple" form (`WHEN` clauses compare `value = cond`
    /// via ordinary `=`, so a `NULL` `value` or a `NULL` `cond` never
    /// matches, matching `=`'s own propagation — confirmed via `goeval`:
    /// `CASE NULL WHEN NULL THEN 1 ELSE 2 END` is `2`, not `1`); absent is
    /// the "searched" form (each `cond` is truthiness-tested directly, the
    /// same three-valued logic `IF`/`WHERE` already use). At least one
    /// `WHEN` clause is required (`CASE END`/`CASE 1 END` are genuine
    /// parse errors in real MySQL, confirmed via `godump restore`, not
    /// assumed).
    Case {
        /// The simple form's compare value; `None` for the searched form.
        value: Option<Box<Expr>>,
        /// Each `(condition, result)` pair, tried in written order; the
        /// FIRST match wins (confirmed via `goeval`: a later `WHEN` that
        /// would also match is never reached).
        when_clauses: Vec<(Expr, Expr)>,
        /// The `ELSE` result; `None` (yielding `NULL`) if absent.
        else_clause: Option<Box<Expr>>,
    },
    /// `CAST(expr AS type)` / `CONVERT(expr, type)` / `BINARY expr` — THREE
    /// concrete syntaxes for the same operation (confirmed via `godump
    /// restore`: real TiDB's AST keeps a function-kind discriminator
    /// purely to pick which of the three forms to print back,
    /// [`CastExpr::style`] here — see [`CastStyle::BinaryOperator`]'s own
    /// doc for the third), so all three parse into this one node rather
    /// than three.
    Cast(CastExpr),
    /// `CONVERT(expr USING charset)` — a genuinely different operation from
    /// [`Expr::Cast`] (a character-set conversion, not a value-type cast),
    /// confirmed via `godump restore` to have its own distinct restore
    /// form. `charset` is not modelled computationally (this crate has no
    /// charset/collation domain at all) — evaluation is a passthrough.
    ConvertUsing {
        /// The value to convert.
        expr: Box<Expr>,
        /// The target charset name, as written (case as typed; real MySQL
        /// charset names are case-insensitive, but no canonicalization is
        /// applied here beyond what `godump restore` itself showed).
        charset: String,
    },
    /// `expr COLLATE collation_name` — a general postfix suffix usable
    /// anywhere an expression is expected (matching real MySQL grammar),
    /// binding at a VERY HIGH precedence: TIGHTER than unary `-`/`~`/`!`
    /// (confirmed by reading real TiDB's own hand-written parser,
    /// `pkg/parser/expr_parser.go`/`prec.go`: `precCollate` sits directly
    /// above `precUnary`, and MySQL's own documentation gives the
    /// canonical example `-1 COLLATE x` == `-(1 COLLATE x)`) but chains
    /// left-to-right with itself (`a COLLATE x COLLATE y` nests as
    /// `Collate(Collate(a, x), y)`, confirmed via `godump restore`).
    /// `collation` is not modelled computationally (this crate has no
    /// charset/collation domain at all, same boundary as
    /// [`Expr::ConvertUsing`]'s own `charset` field) — evaluation is a
    /// passthrough that ignores it entirely. Real TiDB validates the name
    /// against its own closed collation registry (a genuine `ParseError`
    /// for an unknown name); this parser does not model that registry, so
    /// any identifier-shaped name is accepted here, broader than real
    /// scope — matching the same "not modelled" precedent charset names
    /// already follow throughout this crate.
    Collate {
        /// The collated expression.
        expr: Box<Expr>,
        /// The collation name, canonically lowercased (confirmed via
        /// `godump restore`: `COLLATE UTF8MB4_BIN` restores as `COLLATE
        /// utf8mb4_bin` — the OPPOSITE case convention from a charset
        /// name's own uppercasing, matching [`ColumnOption::Collate`]'s
        /// existing convention for the same reason).
        collation: String,
    },
    /// `MATCH(col, ...) AGAINST(expr [search_modifier])` — MySQL/TiDB
    /// full-text search, usable both as a boolean predicate (`WHERE`/
    /// `HAVING`) and as a scalar relevance-score expression (`SELECT`/
    /// `ORDER BY`). Read directly from real TiDB's own hand-written parser
    /// (`pkg/parser/expr_parser.go`'s `parseMatchAgainstExpr`) and its AST
    /// restore (`pkg/parser/ast/expressions.go`'s `MatchAgainst.Restore`)
    /// rather than guessed from restore text alone, since the modifier
    /// grammar has FOUR written spellings that collapse to only THREE
    /// distinct restore outputs (see [`MatchModifier`]'s own doc). `columns`
    /// mirrors [`Expr::Column`]'s own path shape (each entry may be
    /// qualified, `table.col`). No fulltext index or scoring is modelled at
    /// all — evaluation is `Unsupported`, matching this crate's existing
    /// "parse/restore fidelity only" boundary for features needing a
    /// genuinely new domain (see [`Expr::Regexp`]'s own doc for the same
    /// boundary).
    MatchAgainst {
        /// The matched columns, in written order.
        columns: Vec<Vec<String>>,
        /// The search string/expression.
        against: Box<Expr>,
        /// The search modifier, if any.
        modifier: MatchModifier,
    },
    /// `expr MEMBER OF (array)` — MySQL/TiDB's JSON tuple-membership
    /// predicate: whether `expr` is an element of the JSON array `array`
    /// produces. A genuinely different shape from an ordinary
    /// [`Expr::Binary`] (mandatory parens around ONLY the right operand,
    /// no bare-operator form) — read directly from real TiDB's own
    /// hand-written parser (`pkg/parser/expr_parser.go`'s `case
    /// memberof:`, which builds a generic `FuncCallExpr` under the hood,
    /// `pkg/parser/ast/functions.go`'s `JSONMemberOf` constant) and its
    /// restore (`FuncCallExpr.customRestore`'s own `JSONMemberOf`
    /// special case: `left.Restore() + " MEMBER OF (" +
    /// right.Restore() + ")"`) — modelled here as its OWN dedicated
    /// variant rather than reusing [`Expr::Func`] directly, since the
    /// infix-with-parens-around-only-the-right-operand shape doesn't fit
    /// `Expr::Func`'s own uniform `NAME(args...)` restore. `array` parses
    /// at `prec::UNARY` (matching real TiDB's own `SimpleExpr`
    /// restriction there — confirmed via `godump restore`: `MEMBER OF(1
    /// OR 2)` is a genuine `ParseError`), while `expr` (the left operand)
    /// has NO type restriction (unlike [`Expr::Column`]-only `->`/`->>`
    /// just below). Evaluation lowers this node to the same
    /// `JSON_MEMBER_OF` scalar function Go builds.
    MemberOf {
        /// The candidate element.
        expr: Box<Expr>,
        /// The JSON array expression, restored with mandatory
        /// parentheses.
        array: Box<Expr>,
    },
}

/// A `MATCH(...) AGAINST(... <modifier>)` search modifier. Real MySQL/TiDB
/// grammar has FOUR written spellings (`IN BOOLEAN MODE`, `IN NATURAL
/// LANGUAGE MODE`, `IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION`, `WITH
/// QUERY EXPANSION`), but restore only distinguishes THREE outcomes —
/// confirmed via `godump restore`: an explicit `IN NATURAL LANGUAGE MODE`
/// (the implicit default) restores identically to no modifier at all, so
/// [`MatchModifier::None`] covers both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchModifier {
    /// No modifier written, or `IN NATURAL LANGUAGE MODE` (the implicit
    /// default) — both restore with no suffix.
    None,
    /// `IN BOOLEAN MODE`.
    BooleanMode,
    /// `WITH QUERY EXPANSION`, with or without a preceding `IN NATURAL
    /// LANGUAGE MODE` — both restore identically (` WITH QUERY
    /// EXPANSION`). Combining with [`MatchModifier::BooleanMode`] is a
    /// genuine `ParseError` in real TiDB (confirmed by reading
    /// `parseMatchAgainstExpr`), so this crate's own parser rejects it the
    /// same way rather than needing a combined variant.
    QueryExpansion,
}

impl MatchModifier {
    /// Reports Go's `FulltextSearchModifierBooleanMode` bit.
    pub const fn is_boolean_mode(self) -> bool {
        matches!(self, Self::BooleanMode)
    }

    /// Reports Go's natural-language mode, including its implicit default.
    pub const fn is_natural_language_mode(self) -> bool {
        !self.is_boolean_mode()
    }

    /// Reports Go's `FulltextSearchModifierWithQueryExpansion` bit.
    pub const fn with_query_expansion(self) -> bool {
        matches!(self, Self::QueryExpansion)
    }
}

/// A [`Expr::GetFormat`] format-type selector — see that variant's own doc
/// for why `TIMESTAMP` collapses into [`GetFormatSelector::Datetime`]
/// rather than having its own variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GetFormatSelector {
    /// `DATE`.
    Date,
    /// `TIME`.
    Time,
    /// Written as `DATETIME` or `TIMESTAMP` — both restore as `DATETIME`.
    Datetime,
}

impl Expr {
    /// Returns the source byte offset retained for ordinary function calls.
    pub const fn origin_text_position(&self) -> usize {
        match self {
            Self::Func {
                origin_position, ..
            }
            | Self::GenericFuncCall {
                origin_position, ..
            } => *origin_position,
            _ => 0,
        }
    }

    /// Replaces the source byte offset retained for ordinary function calls.
    pub fn set_origin_text_position(&mut self, position: usize) {
        match self {
            Self::Func {
                origin_position, ..
            }
            | Self::GenericFuncCall {
                origin_position, ..
            } => *origin_position = position,
            _ => {}
        }
    }

    /// Derives the source `SetFlag` result from this immutable Rust tree.
    ///
    /// Go stores the result in every expression node after a visitor pass.
    /// Rust does not need mutable parser metadata: deriving it here removes
    /// the setter state while preserving the observable bit mask.
    pub fn flags(&self) -> u64 {
        let combine = |items: &[Expr]| {
            items
                .iter()
                .fold(FLAG_CONSTANT, |bits, item| bits | item.flags())
        };
        match self {
            Self::Column(_) => FLAG_HAS_REFERENCE,
            Self::ParamMarker { .. } => FLAG_HAS_PARAM_MARKER,
            Self::Default(_) => FLAG_HAS_DEFAULT,
            Self::UserVar(_) | Self::SysVar { .. } => FLAG_HAS_VARIABLE,
            Self::Assign { value, .. } => FLAG_HAS_VARIABLE | value.flags(),
            Self::Unary(_, expr)
            | Self::Paren(expr)
            | Self::Extract { value: expr, .. }
            | Self::GetFormat { expr, .. }
            | Self::Is { expr, .. }
            | Self::ConvertUsing { expr, .. }
            | Self::Collate { expr, .. } => expr.flags(),
            Self::Binary(_, left, right)
            | Self::MemberOf {
                expr: left,
                array: right,
            } => left.flags() | right.flags(),
            Self::Row(items) => combine(items),
            Self::Func { args, .. } | Self::GenericFuncCall { args, .. } => {
                FLAG_HAS_FUNC | combine(args)
            }
            Self::Aggregate { args, .. } | Self::GroupConcat { args, .. } => {
                FLAG_HAS_AGGREGATE_FUNC | combine(args)
            }
            Self::Window { args, .. } => FLAG_HAS_WINDOW_FUNC | combine(args),
            Self::Interval { value, .. } => value.flags(),
            Self::Position { substr, str } => FLAG_HAS_FUNC | substr.flags() | str.flags(),
            Self::WeightString { expr, .. } => FLAG_HAS_FUNC | expr.flags(),
            Self::Trim { expr, remstr, .. } => {
                FLAG_HAS_FUNC | expr.flags() | remstr.as_deref().map_or(0, Self::flags)
            }
            Self::TimestampAdd { interval, expr, .. } => {
                FLAG_HAS_FUNC | interval.flags() | expr.flags()
            }
            Self::TimestampDiff { expr1, expr2, .. } => {
                FLAG_HAS_FUNC | expr1.flags() | expr2.flags()
            }
            Self::In { expr, list, .. } => expr.flags() | combine(list),
            Self::Between {
                expr, low, high, ..
            } => expr.flags() | low.flags() | high.flags(),
            Self::Like { expr, pattern, .. } | Self::Regexp { expr, pattern, .. } => {
                expr.flags() | pattern.flags()
            }
            Self::Subquery(_) | Self::Exists { .. } => FLAG_HAS_SUBQUERY,
            Self::InSubquery { expr, .. } => expr.flags() | FLAG_HAS_SUBQUERY,
            Self::CompareSubquery { left, .. } => left.flags() | FLAG_HAS_SUBQUERY,
            Self::Case {
                value,
                when_clauses,
                else_clause,
            } => {
                let value = value.as_deref().map_or(0, Self::flags);
                let clauses = when_clauses
                    .iter()
                    .fold(0, |bits, (when, then)| bits | when.flags() | then.flags());
                value | clauses | else_clause.as_deref().map_or(0, Self::flags)
            }
            Self::Cast(cast) => FLAG_HAS_FUNC | cast.expr.flags(),
            Self::MatchAgainst { against, .. } => against.flags(),
            Self::Int(_)
            | Self::Decimal(_)
            | Self::Float(_)
            | Self::Hex(_)
            | Self::Bit(_)
            | Self::String(_)
            | Self::RawString(_)
            | Self::CharsetString { .. }
            | Self::CharsetBinary { .. }
            | Self::Null
            | Self::Bool(_) => FLAG_CONSTANT,
        }
    }

    /// Checks the aggregate-function bit.
    pub fn has_aggregate_flag(&self) -> bool {
        self.flags() & FLAG_HAS_AGGREGATE_FUNC != 0
    }

    /// Checks the window-function bit.
    pub fn has_window_flag(&self) -> bool {
        self.flags() & FLAG_HAS_WINDOW_FUNC != 0
    }
}

fn restore_charset_name(out: &mut String, charset: &str, context: &RestoreContext) {
    if context.flags().has_keyword_uppercase() {
        out.push_str(&charset.to_ascii_uppercase());
    } else if context.flags().has_keyword_lowercase() {
        out.push_str(&identifier_to_lower(charset));
    } else {
        out.push_str(charset);
    }
}

fn restore_binary_operand(
    out: &mut String,
    expr: &Expr,
    context: &RestoreContext,
    bracket_binary: bool,
) {
    let bracket = bracket_binary && matches!(expr, Expr::Between { .. });
    if bracket {
        out.push('(');
    }
    expr.restore_into_with_context(out, context);
    if bracket {
        out.push(')');
    }
}

fn format_expr_list(exprs: &[Expr], out: &mut String, separator: &str) {
    for (index, expr) in exprs.iter().enumerate() {
        if index > 0 {
            out.push_str(separator);
        }
        expr.format_into(out);
    }
}

fn format_double_quoted_string(value: &str, out: &mut String) {
    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
}

fn format_cast(cast: &CastExpr, out: &mut String) {
    match cast.style {
        CastStyle::Cast | CastStyle::JsonSumCrc32 => {
            out.push_str(if cast.style == CastStyle::JsonSumCrc32 {
                "JSON_SUM_CRC32("
            } else {
                "CAST("
            });
            cast.expr.format_into(out);
            out.push_str(" AS ");
            restore_cast_type(&cast.cast_type, cast.array, out);
            out.push(')');
        }
        CastStyle::Convert => {
            out.push_str("CONVERT(");
            cast.expr.format_into(out);
            out.push_str(", ");
            restore_cast_type(&cast.cast_type, cast.array, out);
            out.push(')');
        }
        CastStyle::BinaryOperator => {
            out.push_str("BINARY ");
            cast.expr.format_into(out);
        }
        CastStyle::DateLiteral | CastStyle::TimeLiteral | CastStyle::TimestampLiteral => {
            out.push_str(match cast.style {
                CastStyle::DateLiteral => "'tidb`.(dateliteral(",
                CastStyle::TimeLiteral => "'tidb`.(timeliteral(",
                CastStyle::TimestampLiteral => "'tidb`.(timestampliteral(",
                _ => unreachable!(),
            });
            cast.expr.format_into(out);
            out.push(')');
        }
    }
}

/// Restores a qualified name path (`a`, `t.a`, ...), back-quoting each
/// part. Shared by [`Expr::Column`] and [`Expr::MatchAgainst`]'s own column
/// list, which has the exact same shape.
fn restore_path(path: &[String], out: &mut String) {
    for (i, part) in path.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(&back_quote(part));
    }
}

/// Restores a Go `ColumnName` path under `RestoreWithoutSchemaName` and
/// `RestoreWithoutTableName`. The parser's paths have one (`col`), two
/// (`table.col`), or three (`schema.table.col`) components; preserving that
/// distinction avoids incorrectly treating a two-part path as if it had a
/// schema component.
fn restore_path_with_context(path: &[String], out: &mut String, context: &RestoreContext) {
    let flags = context.flags();
    if !flags.contains(RestoreFlags::WITHOUT_SCHEMA_NAME)
        && !flags.contains(RestoreFlags::WITHOUT_TABLE_NAME)
    {
        restore_path(path, out);
        return;
    }

    let visible: Vec<&String> = match path {
        [] | [_] => path.iter().collect(),
        [table, column] if flags.contains(RestoreFlags::WITHOUT_TABLE_NAME) => {
            let _ = table;
            vec![column]
        }
        [schema, table, column] => match (
            flags.contains(RestoreFlags::WITHOUT_SCHEMA_NAME),
            flags.contains(RestoreFlags::WITHOUT_TABLE_NAME),
        ) {
            (true, true) => vec![column],
            (true, false) => vec![table, column],
            (false, true) => vec![schema, column],
            (false, false) => vec![schema, table, column],
        },
        _ => path.iter().collect(),
    };
    for (index, component) in visible.iter().enumerate() {
        if index > 0 {
            out.push('.');
        }
        out.push_str(&back_quote(component));
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for SysVarScope {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Global => {}
            Self::Session => {}
            Self::Instance => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for Expr {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Column(field_0) => {
                let _ = field_0;
            }
            Self::ParamMarker {
                offset,
                order,
                in_execute,
                projection_offset,
            } => {
                let _ = (offset, order, in_execute, projection_offset);
            }
            Self::Int(field_0) => {
                let _ = field_0;
            }
            Self::Decimal(field_0) => {
                let _ = field_0;
            }
            Self::Float(field_0) => {
                let _ = field_0;
            }
            Self::Hex(field_0) => {
                let _ = field_0;
            }
            Self::Bit(field_0) => {
                let _ = field_0;
            }
            Self::String(field_0) => {
                let _ = field_0;
            }
            Self::RawString(field_0) => {
                let _ = field_0;
            }
            Self::CharsetString { charset, value } => {
                let _ = charset;
                let _ = value;
            }
            Self::CharsetBinary { charset, value } => {
                let _ = charset;
                if !value.accept(visitor) {
                    return false;
                }
            }
            Self::Null => {}
            Self::Bool(field_0) => {
                let _ = field_0;
            }
            Self::Default(field_0) => {
                let _ = field_0;
            }
            Self::UserVar(field_0) => {
                let _ = field_0;
            }
            Self::SysVar { scope, name } => {
                if let Some(value) = scope.as_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = scope;
                let _ = name;
            }
            Self::Assign { name, value } => {
                if !crate::Visitable::accept(value.as_mut(), visitor) {
                    return false;
                }
                let _ = name;
                let _ = value;
            }
            Self::Unary(field_0, field_1) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(field_1.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
                let _ = field_1;
            }
            Self::Binary(field_0, field_1, field_2) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(field_1.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(field_2.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
                let _ = field_1;
                let _ = field_2;
            }
            Self::Paren(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Row(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
            Self::Func { name, args, .. } => {
                for value in args.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = name;
                let _ = args;
            }
            Self::GenericFuncCall {
                schema, name, args, ..
            } => {
                for value in args.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = schema;
                let _ = name;
                let _ = args;
            }
            Self::Aggregate {
                name,
                distinct,
                args,
            } => {
                for value in args.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = name;
                let _ = distinct;
                let _ = args;
            }
            Self::GroupConcat {
                distinct,
                args,
                order_by,
                separator,
            } => {
                for value in args.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                for value in order_by.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = distinct;
                let _ = args;
                let _ = order_by;
                let _ = separator;
            }
            Self::Window {
                name,
                args,
                distinct,
                ignore_nulls,
                from_last,
                over,
            } => {
                for value in args.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                if !crate::Visitable::accept(over, visitor) {
                    return false;
                }
                let _ = name;
                let _ = args;
                let _ = distinct;
                let _ = ignore_nulls;
                let _ = from_last;
                let _ = over;
            }
            Self::Interval { value, unit } => {
                if !crate::Visitable::accept(value.as_mut(), visitor) {
                    return false;
                }
                let _ = value;
                let _ = unit;
            }
            Self::Extract { unit, value } => {
                if !crate::Visitable::accept(value.as_mut(), visitor) {
                    return false;
                }
                let _ = unit;
                let _ = value;
            }
            Self::Position { substr, str } => {
                if !crate::Visitable::accept(substr.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(str.as_mut(), visitor) {
                    return false;
                }
                let _ = substr;
                let _ = str;
            }
            Self::WeightString { expr, as_type } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                if let Some(value) = as_type.as_mut() {
                    if !crate::Visitable::accept(&mut value.0, visitor) {
                        return false;
                    }
                }
                let _ = expr;
                let _ = as_type;
            }
            Self::Trim {
                expr,
                remstr,
                direction,
            } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                if let Some(value) = remstr.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                if let Some(value) = direction.as_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = expr;
                let _ = remstr;
                let _ = direction;
            }
            Self::TimestampAdd {
                unit,
                interval,
                expr,
            } => {
                if !crate::Visitable::accept(interval.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                let _ = unit;
                let _ = interval;
                let _ = expr;
            }
            Self::TimestampDiff { unit, expr1, expr2 } => {
                if !crate::Visitable::accept(expr1.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(expr2.as_mut(), visitor) {
                    return false;
                }
                let _ = unit;
                let _ = expr1;
                let _ = expr2;
            }
            Self::GetFormat { selector, expr } => {
                if !crate::Visitable::accept(selector, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                let _ = selector;
                let _ = expr;
            }
            Self::In { expr, list, not } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                for value in list.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = expr;
                let _ = list;
                let _ = not;
            }
            Self::Between {
                expr,
                low,
                high,
                not,
            } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(low.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(high.as_mut(), visitor) {
                    return false;
                }
                let _ = expr;
                let _ = low;
                let _ = high;
                let _ = not;
            }
            Self::Like {
                expr,
                pattern,
                not,
                ilike,
                escape,
            } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(pattern.as_mut(), visitor) {
                    return false;
                }
                let _ = expr;
                let _ = pattern;
                let _ = not;
                let _ = ilike;
                let _ = escape;
            }
            Self::Regexp { expr, pattern, not } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(pattern.as_mut(), visitor) {
                    return false;
                }
                let _ = expr;
                let _ = pattern;
                let _ = not;
            }
            Self::Is { expr, target, not } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(target, visitor) {
                    return false;
                }
                let _ = expr;
                let _ = target;
                let _ = not;
            }
            Self::Subquery(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Exists { subquery, not } => {
                if !crate::Visitable::accept(subquery.as_mut(), visitor) {
                    return false;
                }
                let _ = subquery;
                let _ = not;
            }
            Self::InSubquery {
                expr,
                subquery,
                not,
            } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(subquery.as_mut(), visitor) {
                    return false;
                }
                let _ = expr;
                let _ = subquery;
                let _ = not;
            }
            Self::CompareSubquery {
                op,
                left,
                all,
                subquery,
            } => {
                if !crate::Visitable::accept(op, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(left.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(subquery.as_mut(), visitor) {
                    return false;
                }
                let _ = op;
                let _ = left;
                let _ = all;
                let _ = subquery;
            }
            Self::Case {
                value,
                when_clauses,
                else_clause,
            } => {
                if let Some(value) = value.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                for value in when_clauses.iter_mut() {
                    if !crate::Visitable::accept(&mut value.0, visitor) {
                        return false;
                    }
                    if !crate::Visitable::accept(&mut value.1, visitor) {
                        return false;
                    }
                }
                if let Some(value) = else_clause.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                let _ = value;
                let _ = when_clauses;
                let _ = else_clause;
            }
            Self::Cast(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ConvertUsing { expr, charset } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                let _ = expr;
                let _ = charset;
            }
            Self::Collate { expr, collation } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                let _ = expr;
                let _ = collation;
            }
            Self::MatchAgainst {
                columns,
                against,
                modifier,
            } => {
                if !crate::Visitable::accept(against.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(modifier, visitor) {
                    return false;
                }
                let _ = columns;
                let _ = against;
                let _ = modifier;
            }
            Self::MemberOf { expr, array } => {
                if !crate::Visitable::accept(expr.as_mut(), visitor) {
                    return false;
                }
                if !crate::Visitable::accept(array.as_mut(), visitor) {
                    return false;
                }
                let _ = expr;
                let _ = array;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for MatchModifier {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::BooleanMode => {}
            Self::QueryExpansion => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for GetFormatSelector {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Date => {}
            Self::Time => {}
            Self::Datetime => {}
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
