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

//! Scalar expressions (`Expr`), `CAST`/`CONVERT`, and operators, and their
//! restore.

use crate::select::restore_window_def;
use crate::util::{
    back_quote, escape_string_literal, format_go_float, normalize_decimal, normalize_int,
    restore_string_literal,
};
use crate::{OrderItem, QueryStmt, RestoreContext, RestoreFlags, SelectStmt, WindowOver};

/// The scope of a system variable (`@@GLOBAL.x` / `@@SESSION.x`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SysVarScope {
    /// `@@GLOBAL.`.
    Global,
    /// `@@SESSION.` (also `@@LOCAL.`).
    Session,
}

/// A scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A qualified column reference, e.g. `["t", "a"]`.
    Column(Vec<String>),
    /// An integer literal (original digits).
    Int(String),
    /// A fixed-point decimal literal (original text).
    Decimal(String),
    /// A floating-point literal's parsed value.
    Float(f64),
    /// A hexadecimal literal's normalized lowercase, even-length hex digits.
    Hex(String),
    /// A bit literal's normalized (leading-zero-stripped) bit digits.
    Bit(String),
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
    /// The `NULL` literal.
    Null,
    /// A boolean literal (`TRUE` / `FALSE`).
    Bool(bool),
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
        separator: String,
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
    /// `CUME_DIST`. `DISTINCT` in the aggregate position (`MAX(DISTINCT
    /// x) OVER (...)`, real but rare MySQL grammar) is not modelled — a
    /// genuine `ParseError`, not silently dropped or ignored. `IGNORE
    /// NULLS`/`FROM LAST` (ANSI SQL grammar `LAG`/`LEAD`/etc. can carry)
    /// are not modelled — confirmed via `gorun` that real TiDB itself
    /// rejects both unconditionally, so this is not a real divergence,
    /// just matching real scope exactly. The `OVER` clause itself may be
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
    Subquery(Box<QueryStmt>),
    /// `[NOT] EXISTS (SELECT ...)` or a set-operation query.
    ///
    /// TiDB's `parseExistsSubquery` delegates to the general subquery
    /// parser, so `EXISTS (SELECT ... UNION [ALL] SELECT ...)` is a real
    /// source shape. Keep the query behind the same typed envelope used by
    /// `IN` subqueries instead of flattening a set operation into one
    /// `SelectStmt` and losing its semantics.
    Exists {
        /// The subquery.
        subquery: Box<QueryStmt>,
        /// Whether negated (`NOT EXISTS`).
        not: bool,
    },
    /// `expr [NOT] IN (SELECT ...)` — one of the two parenthesized-subquery
    /// positions (the other is [`Expr::Exists`]) whose subquery may ALSO be
    /// `UNION`/`EXCEPT`/`INTERSECT`-bodied, hence `Box<QueryStmt>` here rather
    /// than `Box<SelectStmt>` — confirmed via `godump restore`: `x NOT IN
    /// (SELECT 1 UNION SELECT 2)` restores unchanged. Always either
    /// `QueryStmt::Select` or `QueryStmt::SetOpr` (real TiDB's own
    /// `parseSubquery`/this crate's own `Parser::parse_select_or_setopr`,
    /// which this variant's own parsing calls directly — never any other
    /// `Stmt` variant). Scalar, `ANY`/`ALL`, and the parenthesized scalar
    /// expression positions retain their narrower `SelectStmt` AST slots.
    InSubquery {
        /// The tested expression.
        expr: Box<Expr>,
        /// The subquery.
        subquery: Box<QueryStmt>,
        /// Whether negated (`NOT IN`).
        not: bool,
    },
    /// `expr <op> ANY|ALL (SELECT ...)`.
    CompareSubquery {
        /// The comparison operator.
        op: BinaryOp,
        /// The left operand.
        left: Box<Expr>,
        /// `true` for `ALL`, `false` for `ANY`/`SOME`.
        all: bool,
        /// The subquery.
        subquery: Box<SelectStmt>,
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
    /// just below). No JSON array/membership semantics are modelled at
    /// all — evaluation is `Unsupported`, the SAME "parse/restore
    /// fidelity only" boundary [`Expr::MatchAgainst`]'s own doc already
    /// established.
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
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_into_with_context(out, RestoreContext::default());
    }

    /// Restores an expression under the statement's source formatting
    /// context. DDL owns the caller today, but the context lives here because
    /// Go's column-name qualifier flags apply recursively inside expressions,
    /// not only to a statement's outer identifier slots.
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        match self {
            Expr::Column(path) => restore_path_with_context(path, out, context),
            // Integer literals restore as their decimal value, so leading
            // zeros are dropped (`0000` -> `0`, `01` -> `1`).
            Expr::Int(s) => out.push_str(&normalize_int(s)),
            Expr::Decimal(s) => out.push_str(&normalize_decimal(s)),
            Expr::Float(f) => out.push_str(&format_go_float(*f)),
            Expr::Hex(h) => {
                out.push_str("x'");
                out.push_str(h);
                out.push('\'');
            }
            Expr::Bit(b) => {
                out.push_str("b'");
                out.push_str(b);
                out.push('\'');
            }
            Expr::String(v) => out.push_str(&restore_string_literal(v)),
            Expr::RawString(v) => {
                out.push('\'');
                out.push_str(&escape_string_literal(v));
                out.push('\'');
            }
            Expr::CharsetString { charset, value } => {
                out.push('_');
                out.push_str(charset);
                out.push('\'');
                out.push_str(&escape_string_literal(value));
                out.push('\'');
            }
            Expr::Null => out.push_str("NULL"),
            Expr::Bool(true) => out.push_str("TRUE"),
            Expr::Bool(false) => out.push_str("FALSE"),
            Expr::UserVar(name) => {
                out.push('@');
                out.push_str(&back_quote(name));
            }
            Expr::SysVar { scope, name } => {
                out.push_str("@@");
                match scope {
                    Some(SysVarScope::Global) => out.push_str("GLOBAL."),
                    Some(SysVarScope::Session) => out.push_str("SESSION."),
                    None => {}
                }
                out.push_str(&back_quote(name));
            }
            Expr::Assign { name, value } => {
                out.push('@');
                out.push_str(&back_quote(name));
                out.push_str(":=");
                value.restore_into_with_context(out, context);
            }
            Expr::Unary(op, e) => {
                out.push_str(op.restore());
                e.restore_into_with_context(out, context);
            }
            Expr::Binary(op, l, r) => {
                l.restore_into_with_context(out, context);
                out.push_str(op.restore());
                r.restore_into_with_context(out, context);
            }
            Expr::Paren(e) => {
                out.push('(');
                e.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::Row(values) => {
                out.push_str("ROW(");
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    v.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            // A bare `DEFAULT` (no parens) — legal in `INSERT`
            // `VALUES`/`SET` items and single-table `UPDATE` assignments,
            // where it means "this column's declared DEFAULT value".
            // Modelled as a zero-arg
            // `DEFAULT` func (real TiDB's `DEFAULT()` with zero args is
            // itself a `ParseError`, so an empty arg list is unambiguous)
            // and restored as the bare keyword. `DEFAULT(col)` (one arg)
            // still restores via the normal `NAME(args)` path below.
            Expr::Func { name, args }
                if args.is_empty() && name.eq_ignore_ascii_case("DEFAULT") =>
            {
                out.push_str("DEFAULT");
            }
            Expr::Func { name, args } => {
                out.push_str(&name.to_ascii_uppercase());
                out.push('(');
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            Expr::GenericFuncCall { schema, name, args } => {
                out.push_str(&back_quote(schema));
                out.push('.');
                out.push_str(&back_quote(name));
                out.push('(');
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            Expr::Aggregate {
                name,
                distinct,
                args,
            } => {
                out.push_str(name);
                out.push('(');
                if *distinct {
                    out.push_str("DISTINCT ");
                }
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            Expr::GroupConcat {
                distinct,
                args,
                order_by,
                separator,
            } => {
                out.push_str("GROUP_CONCAT(");
                if *distinct {
                    out.push_str("DISTINCT ");
                }
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.restore_into_with_context(out, context);
                }
                if !order_by.is_empty() {
                    out.push_str(" ORDER BY ");
                    for (i, item) in order_by.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        item.restore_into(out);
                    }
                }
                out.push_str(" SEPARATOR '");
                out.push_str(&escape_string_literal(separator));
                out.push_str("')");
            }
            Expr::Window { name, args, over } => {
                out.push_str(name);
                out.push('(');
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    a.restore_into_with_context(out, context);
                }
                out.push_str(") OVER ");
                match over {
                    // A bare name has NO enclosing parentheses at all —
                    // confirmed via `godump restore` this restores
                    // DIFFERENTLY from `OVER (name)`, even though both are
                    // semantically identical.
                    WindowOver::Name(name) => out.push_str(&back_quote(name)),
                    WindowOver::Def(def) => {
                        out.push('(');
                        restore_window_def(def, out);
                        out.push(')');
                    }
                }
            }
            Expr::Interval { value, unit } => {
                out.push_str("INTERVAL ");
                value.restore_into_with_context(out, context);
                out.push(' ');
                out.push_str(unit);
            }
            Expr::Extract { unit, value } => {
                out.push_str("EXTRACT(");
                out.push_str(unit);
                out.push_str(" FROM ");
                value.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::Position { substr, str } => {
                out.push_str("POSITION(");
                substr.restore_into_with_context(out, context);
                out.push_str(" IN ");
                str.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::WeightString { expr, as_type } => {
                out.push_str("WEIGHT_STRING(");
                expr.restore_into_with_context(out, context);
                if let Some((ty, len)) = as_type {
                    out.push_str(" AS ");
                    out.push_str(match ty {
                        WeightStringType::Char => "CHAR",
                        WeightStringType::Binary => "BINARY",
                    });
                    out.push('(');
                    out.push_str(&len.to_string());
                    out.push(')');
                }
                out.push(')');
            }
            Expr::Trim {
                expr,
                remstr,
                direction,
            } => {
                out.push_str("TRIM(");
                if let Some(d) = direction {
                    out.push_str(match d {
                        TrimDirection::Both => "BOTH ",
                        TrimDirection::Leading => "LEADING ",
                        TrimDirection::Trailing => "TRAILING ",
                    });
                }
                if let Some(r) = remstr {
                    // An explicit `NULL` remstr is OMITTED from restore —
                    // a real, narrow quirk (checked by VALUE, not by
                    // whether the source wrote anything at all) — see
                    // this variant's own doc.
                    if !matches!(r.as_ref(), Expr::Null) {
                        r.restore_into_with_context(out, context);
                        out.push(' ');
                    }
                    out.push_str("FROM ");
                }
                expr.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::TimestampAdd {
                unit,
                interval,
                expr,
            } => {
                out.push_str("TIMESTAMPADD(");
                out.push_str(unit);
                out.push_str(", ");
                interval.restore_into_with_context(out, context);
                out.push_str(", ");
                expr.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::TimestampDiff { unit, expr1, expr2 } => {
                out.push_str("TIMESTAMPDIFF(");
                out.push_str(unit);
                out.push_str(", ");
                expr1.restore_into_with_context(out, context);
                out.push_str(", ");
                expr2.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::GetFormat { selector, expr } => {
                out.push_str("GET_FORMAT(");
                out.push_str(match selector {
                    GetFormatSelector::Date => "DATE",
                    GetFormatSelector::Time => "TIME",
                    GetFormatSelector::Datetime => "DATETIME",
                });
                out.push_str(", ");
                expr.restore_into_with_context(out, context);
                out.push(')');
            }
            Expr::In { expr, list, not } => {
                expr.restore_into_with_context(out, context);
                out.push_str(if *not { " NOT IN (" } else { " IN (" });
                for (i, e) in list.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    e.restore_into_with_context(out, context);
                }
                out.push(')');
            }
            Expr::Between {
                expr,
                low,
                high,
                not,
            } => {
                expr.restore_into_with_context(out, context);
                out.push_str(if *not { " NOT BETWEEN " } else { " BETWEEN " });
                low.restore_into_with_context(out, context);
                out.push_str(" AND ");
                high.restore_into_with_context(out, context);
            }
            Expr::Like {
                expr,
                pattern,
                not,
                escape,
            } => {
                expr.restore_into_with_context(out, context);
                out.push_str(if *not { " NOT LIKE " } else { " LIKE " });
                pattern.restore_into_with_context(out, context);
                // `None` also covers an explicit `ESCAPE '\'` matching
                // the default — see this variant's own doc.
                if let Some(esc) = escape {
                    out.push_str(" ESCAPE '");
                    if *esc != 0 {
                        out.push_str(&escape_string_literal(&(*esc as char).to_string()));
                    }
                    out.push('\'');
                }
            }
            Expr::Regexp { expr, pattern, not } => {
                expr.restore_into_with_context(out, context);
                out.push_str(if *not { " NOT REGEXP " } else { " REGEXP " });
                pattern.restore_into_with_context(out, context);
            }
            Expr::Is { expr, target, not } => {
                expr.restore_into_with_context(out, context);
                out.push_str(" IS ");
                if *not {
                    out.push_str("NOT ");
                }
                out.push_str(match target {
                    IsTarget::Null => "NULL",
                    IsTarget::True => "TRUE",
                    IsTarget::False => "FALSE",
                    IsTarget::Unknown => "UNKNOWN",
                });
            }
            Expr::Subquery(s) => {
                out.push('(');
                s.restore_into(out);
                out.push(')');
            }
            Expr::Exists { subquery, not } => {
                if *not {
                    out.push_str("NOT ");
                }
                out.push_str("EXISTS (");
                subquery.restore_into(out);
                out.push(')');
            }
            Expr::InSubquery {
                expr,
                subquery,
                not,
            } => {
                expr.restore_into_with_context(out, context);
                out.push_str(if *not { " NOT IN (" } else { " IN (" });
                subquery.restore_into(out);
                out.push(')');
            }
            Expr::CompareSubquery {
                op,
                left,
                all,
                subquery,
            } => {
                left.restore_into(out);
                out.push_str(op.restore());
                out.push_str(if *all { "ALL (" } else { "ANY (" });
                subquery.restore_into(out);
                out.push(')');
            }
            Expr::Case {
                value,
                when_clauses,
                else_clause,
            } => {
                out.push_str("CASE");
                if let Some(v) = value {
                    out.push(' ');
                    v.restore_into_with_context(out, context);
                }
                for (cond, result) in when_clauses {
                    out.push_str(" WHEN ");
                    cond.restore_into_with_context(out, context);
                    out.push_str(" THEN ");
                    result.restore_into_with_context(out, context);
                }
                if let Some(e) = else_clause {
                    out.push_str(" ELSE ");
                    e.restore_into_with_context(out, context);
                }
                out.push_str(" END");
            }
            Expr::Cast(cast) => match cast.style {
                CastStyle::Cast => {
                    out.push_str("CAST(");
                    cast.expr.restore_into_with_context(out, context);
                    out.push_str(" AS ");
                    restore_cast_type(&cast.cast_type, cast.array, out);
                    out.push(')');
                }
                CastStyle::Convert => {
                    out.push_str("CONVERT(");
                    cast.expr.restore_into_with_context(out, context);
                    out.push_str(", ");
                    restore_cast_type(&cast.cast_type, cast.array, out);
                    out.push(')');
                }
                CastStyle::BinaryOperator => {
                    out.push_str("BINARY ");
                    cast.expr.restore_into_with_context(out, context);
                }
                CastStyle::DateLiteral => restore_typed_literal("DATE", &cast.expr, out),
                CastStyle::TimeLiteral => restore_typed_literal("TIME", &cast.expr, out),
                CastStyle::TimestampLiteral => restore_typed_literal("TIMESTAMP", &cast.expr, out),
                CastStyle::JsonSumCrc32 => {
                    out.push_str("JSON_SUM_CRC32(");
                    cast.expr.restore_into_with_context(out, context);
                    out.push_str(" AS ");
                    restore_cast_type(&cast.cast_type, cast.array, out);
                    out.push(')');
                }
            },
            Expr::ConvertUsing { expr, charset } => {
                out.push_str("CONVERT(");
                expr.restore_into_with_context(out, context);
                out.push_str(" USING '");
                out.push_str(&escape_string_literal(charset));
                out.push_str("')");
            }
            Expr::Collate { expr, collation } => {
                expr.restore_into_with_context(out, context);
                out.push_str(" COLLATE ");
                out.push_str(collation);
            }
            Expr::MatchAgainst {
                columns,
                against,
                modifier,
            } => {
                out.push_str("MATCH (");
                for (i, path) in columns.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    restore_path_with_context(path, out, context);
                }
                out.push_str(") AGAINST (");
                against.restore_into_with_context(out, context);
                match modifier {
                    MatchModifier::None => {}
                    MatchModifier::BooleanMode => out.push_str(" IN BOOLEAN MODE"),
                    MatchModifier::QueryExpansion => out.push_str(" WITH QUERY EXPANSION"),
                }
                out.push(')');
            }
            Expr::MemberOf { expr, array } => {
                expr.restore_into_with_context(out, context);
                out.push_str(" MEMBER OF (");
                array.restore_into_with_context(out, context);
                out.push(')');
            }
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
fn restore_path_with_context(path: &[String], out: &mut String, context: RestoreContext) {
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
    /// `cast_type` always [`CastType::Date`], `expr` always an
    /// [`Expr::String`] (never any other expression shape — real TiDB's
    /// own grammar only accepts a raw string-literal token here, not an
    /// arbitrary expression). Restores as `DATE 'literal'`, the literal's
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

/// Restores an ODBC-style typed literal (`DATE`/`TIME`/`TIMESTAMP
/// 'literal'`, see [`CastStyle::DateLiteral`]'s own doc): `keyword`, a
/// space, then the literal's escaped body between quotes — deliberately
/// NOT [`Expr::String`]'s own `restore_into` (which would add an
/// `_UTF8MB4` charset-introducer prefix real TiDB does not print here,
/// confirmed via `godump restore`). `expr` is always `Expr::String` by
/// construction (`tidb_parser`'s own grammar only ever builds one of
/// these three `CastStyle`s from a raw string-literal token).
fn restore_typed_literal(keyword: &str, expr: &Expr, out: &mut String) {
    let Expr::String(text) = expr else {
        unreachable!("typed date/time/timestamp literal always wraps a string literal")
    };
    out.push_str(keyword);
    out.push_str(" '");
    out.push_str(&escape_string_literal(text));
    out.push('\'');
}

/// Restores a [`CastType`] plus its own optional `ARRAY` suffix (see
/// [`CastExpr::array`]'s own doc), shared by [`CastStyle::Cast`]'s `AS
/// type` form, [`CastStyle::Convert`]'s `, type` form, and
/// [`CastStyle::JsonSumCrc32`]'s own `AS type` form.
fn restore_cast_type(ty: &CastType, array: bool, out: &mut String) {
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

/// [`Expr::WeightString`]'s own `AS` clause type — `CHARACTER` is a real
/// synonym for `CHAR`, collapsed to [`WeightStringType::Char`] at parse
/// time (see [`Expr::WeightString`]'s own doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightStringType {
    /// `CHAR` (or its `CHARACTER` synonym).
    Char,
    /// `BINARY`.
    Binary,
}

/// [`Expr::Trim`]'s own direction keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimDirection {
    /// `BOTH`.
    Both,
    /// `LEADING`.
    Leading,
    /// `TRAILING`.
    Trailing,
}

/// The right-hand side of an `IS` predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsTarget {
    /// `IS NULL`.
    Null,
    /// `IS TRUE`.
    True,
    /// `IS FALSE`.
    False,
    /// `IS UNKNOWN`.
    Unknown,
}

/// A prefix unary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// Unary plus `+`.
    Plus,
    /// Unary minus `-`.
    Minus,
    /// Bitwise NOT `~`.
    BitNeg,
    /// Logical NOT via `!`.
    Not,
    /// Logical NOT via the `NOT` keyword.
    NotKeyword,
}

impl UnaryOp {
    fn restore(self) -> &'static str {
        match self {
            UnaryOp::Plus => "+",
            UnaryOp::Minus => "-",
            UnaryOp::BitNeg => "~",
            UnaryOp::Not => "!",
            UnaryOp::NotKeyword => "NOT ",
        }
    }
}

/// An infix binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    /// `+`.
    Plus,
    /// `-`.
    Minus,
    /// `*`.
    Mul,
    /// `/`.
    Div,
    /// `%` / `MOD`.
    Mod,
    /// `DIV` (integer division).
    IntDiv,
    /// `|`.
    BitOr,
    /// `&`.
    BitAnd,
    /// `^`.
    BitXor,
    /// `<<`.
    LeftShift,
    /// `>>`.
    RightShift,
    /// `=`.
    Eq,
    /// `<=>`.
    NullEq,
    /// `>=`.
    Ge,
    /// `>`.
    Gt,
    /// `<=`.
    Le,
    /// `<`.
    Lt,
    /// `!=` / `<>`.
    Ne,
    /// `AND` / `&&`.
    LogicAnd,
    /// `OR` / `||`.
    LogicOr,
    /// `XOR`.
    LogicXor,
}

impl BinaryOp {
    /// The restore text, including surrounding spaces for keyword operators.
    fn restore(self) -> &'static str {
        match self {
            BinaryOp::Plus => "+",
            BinaryOp::Minus => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
            BinaryOp::Mod => "%",
            BinaryOp::IntDiv => " DIV ",
            BinaryOp::BitOr => "|",
            BinaryOp::BitAnd => "&",
            BinaryOp::BitXor => "^",
            BinaryOp::LeftShift => "<<",
            BinaryOp::RightShift => ">>",
            BinaryOp::Eq => "=",
            BinaryOp::NullEq => "<=>",
            BinaryOp::Ge => ">=",
            BinaryOp::Gt => ">",
            BinaryOp::Le => "<=",
            BinaryOp::Lt => "<",
            BinaryOp::Ne => "!=",
            BinaryOp::LogicAnd => " AND ",
            BinaryOp::LogicOr => " OR ",
            BinaryOp::LogicXor => " XOR ",
        }
    }
}
