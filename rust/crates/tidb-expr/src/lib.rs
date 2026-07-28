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

//! A constant-expression evaluator over [`tidb_ast::Expr`] — the seed of the
//! design's `tidb-expr` crate and the first step from syntax into semantics.
//!
//! Scope: the integer/string/decimal/float/`NULL` domain of MySQL scalar
//! expressions — integer/boolean/string/decimal/float literals, `NULL`,
//! unary `+`/`-`/`~`/`NOT`/`!`, binary arithmetic (`+ - * / DIV MOD`),
//! bitwise (`& | ^ << >>`), comparison (`= <=> >= > <= < != <>`, with string
//! operands compared under `utf8mb4_bin` PAD SPACE and int/decimal/float
//! operands freely mixed), and logical (`AND OR XOR`) operators — with
//! MySQL's three-valued
//! logic — the `[NOT] IN (list)`, `[NOT] BETWEEN`, `IS [NOT] NULL/TRUE/FALSE`,
//! and `[NOT] LIKE` (case-sensitive `utf8mb4_bin`, `%`/`_` wildcards; a
//! non-string operand on EITHER side is implicitly stringified via
//! [`Datum::sql_string`], matching real MySQL's coercion — confirmed via
//! `gorun`, including that a `DECIMAL`'s declared scale is preserved, not
//! simplified) predicates, `CASE` (both the simple `CASE value WHEN cond THEN result
//! ... [ELSE result] END` form — `cond` compared via ordinary `=`, so a
//! `NULL` `value` never matches any `WHEN`, matching `=`'s own
//! propagation — and the searched `CASE WHEN cond THEN result ... [ELSE
//! result] END` form, `cond` truthiness-tested directly; the first
//! matching `WHEN` wins, evaluated LAZILY — only the taken branch's
//! expression ever runs, matching real MySQL's short-circuit CASE, a
//! load-bearing idiom for guarding against errors like division by zero;
//! real MySQL additionally infers CASE's overall result type from EVERY
//! branch statically, even ones never evaluated — confirmed via `goeval`:
//! `CASE WHEN 1=0 THEN 1/0 ELSE 5 END` is `DEC:5.0000`, not `INT:5`, even
//! though `1/0` is never evaluated — which cannot be replicated without a
//! genuine type-inference pass and is deliberately NOT attempted here;
//! the result is simply whichever branch was taken, in its own natural
//! type), plus builtin functions: numeric (`ABS`, `SIGN`, `LEAST`,
//! `GREATEST`, `COALESCE`, `IF`, `IFNULL`, `NULLIF`, `CEIL`/`CEILING`,
//! `FLOOR` — the last two return `Int` for an `Int`/`Decimal` argument
//! (`Decimal` computed EXACTLY, via [`Decimal::ceil_floor`], not
//! through `f64`) but `Float` for a `Float` one, confirmed via `goeval`,
//! not assumed; `ROUND`/`TRUNCATE` — a DIFFERENT type rule from
//! `CEIL`/`FLOOR`: `Decimal` NEVER collapses to `Int`, and rounds ties
//! away from zero via [`Decimal::round_to_scale`]/
//! [`Decimal::truncate_to_scale`], clamped to `DECIMAL`'s max
//! scale (30) for a positive scale argument, while `Float` rounds ties TO
//! EVEN via [`math_fn`]'s bit-for-bit port of Go's `types.Round`/
//! `types.Truncate` — including Go's own `math.Pow10` lookup table, which
//! is NOT the same as `f64::powi` for most exponents, confirmed by
//! diffing bit patterns, not assumed), transcendental ([`math_fn`]: `SQRT`,
//! `POW`/`POWER`, `EXP`, `LN`, `LOG`, `LOG2`, `LOG10`, `PI`, and the trigonometric
//! family — `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`/`ATAN2`, `COT`,
//! `RADIANS`, `DEGREES` — every one of these always returns `Float`), and
//! string (`CONCAT`, `LENGTH`, `CHAR_LENGTH`, `UPPER`, `LOWER`, `LEFT`,
//! `RIGHT`, `SUBSTRING`), all of which nest.
//!
//! Date-part extraction (`YEAR`, `MONTH`, `DAY`/`DAYOFMONTH`, `QUARTER`,
//! `DAYOFYEAR`, `DAYOFWEEK`, `WEEKDAY`, `TO_DAYS`, `TO_SECONDS`) and
//! `DATEDIFF` are also
//! covered: a `DATE`/`DATETIME` value has no dedicated value domain here, so
//! these parse a string argument's calendar components directly
//! (calendar-validated: month 1-12, day valid for that specific month/year
//! including leap years; lenient about separator characters and
//! zero-padding, matching MySQL's own leniency, confirmed via `goeval`).
//! `DATEDIFF` converts both dates to an absolute day number
//! ([`time_fn::calendar::days_from_civil`], a well-known algorithm) and subtracts,
//! ignoring any time-of-day component on either side; `DAYOFYEAR` is a
//! `days_from_civil` difference from that year's January 1st; `DAYOFWEEK`
//! (`1`=Sunday..`7`=Saturday) and `WEEKDAY` (`0`=Monday..`6`=Sunday) are
//! both `days_from_civil` read modulo 7 with a fixed offset; `TO_DAYS` and
//! `TO_SECONDS` use the source-compatible zero-date `calcDaynr` arithmetic
//! (including `TO_DAYS('0000-01-01') = 1`) and expose absolute day/second
//! numbers rather than differences. They reject malformed time suffixes and
//! zero-date components at the value boundary.
//! `FROM_DAYS` is `TO_DAYS`'s inverse ([`time_fn::calendar::civil_from_days`], the
//! complementary half of the same well-known algorithm as
//! `days_from_civil`), producing a `YYYY-MM-DD` string (still no dedicated
//! `DATE` value domain — a `goeval` `STR:` label was reused rather than
//! adding a new `DATE:` one, for direct comparability with how every other
//! date-shaped value in this crate is already represented); it returns
//! MySQL's "zero date" string outside the valid year `0001`-`9999` range,
//! except for a narrow, clearly-anomalous real-TiDB `NULL` sub-band just
//! above that range, deliberately not reproduced (documented on
//! [`time_fn::calendar::from_days`] itself).
//!
//! `DATE_ADD`/`DATE_SUB(date, INTERVAL amount unit)` are also covered, for
//! `DAY`, `WEEK`, `MONTH`, `YEAR`, `HOUR`, `MINUTE`, `SECOND`, and every
//! COMPOSITE unit (`YEAR_MONTH`, `DAY_HOUR`, `DAY_MINUTE`, `DAY_SECOND`,
//! `HOUR_MINUTE`, `HOUR_SECOND`, `MINUTE_SECOND`, and their
//! `*_MICROSECOND` variants — see [`time_fn::calendar::date_add`]'s own doc
//! for the composite split rules, ported from `parseTimeValue`
//! (`pkg/types/time.go`)); `QUARTER` is the one remaining unsupported unit.
//! `DAY` is exact day arithmetic via the same
//! `days_from_civil`/`civil_from_days` round-trip `TO_DAYS`/`FROM_DAYS`
//! use, so month/year rollover and leap days are handled correctly for
//! free (`2021-01-31 + 1 DAY` = `2021-02-01`, `2020-02-28 + 1 DAY` =
//! `2020-02-29`); `WEEK` is `DAY` with the (already-rounded) amount
//! pre-multiplied by 7. `MONTH`/`YEAR` are a genuinely DIFFERENT
//! algorithm — calendar-FIELD arithmetic ([`time_fn::calendar::add_months`]): the
//! year/month roll over via total-months arithmetic, and the day CLAMPS to
//! the target month's own length rather than overflowing into the next
//! month (`2021-01-31 + 1 MONTH` = `2021-02-28`, not `2021-03-03`), with
//! the clamp computed once against the FINAL target month, not iteratively
//! re-clamped one month at a time (`2021-01-31 + 2 MONTH` = `2021-03-31`,
//! the full 31 days, not `2021-03-28` from clamping through February
//! first — confirmed via `goeval`, not assumed); `YEAR` reuses the same
//! function with the amount pre-multiplied by 12. `DAY`/`WEEK`/`MONTH`/
//! `YEAR` all preserve an existing time-of-day suffix on the input
//! verbatim (or omit it if absent) — none of them touch it.
//!
//! `HOUR`/`MINUTE`/`SECOND` are a THIRD algorithm ([`time_fn::calendar::date_add_time`]):
//! unlike the units above, they always compute AND render a time-of-day
//! component — even for a `DATE`-only input, treated as midnight
//! (`2021-01-01 + 5 HOUR` = `2021-01-01 05:00:00`) — via absolute
//! seconds-since-epoch arithmetic, so overflow correctly carries into the
//! day and, through `civil_from_days`, into month/year
//! (`22:00:00 + 5 HOUR` = the next day's `03:00:00`). This is a
//! DIFFERENT, much simpler problem than the standalone `HOUR()`/
//! `MINUTE()`/`SECOND()` EXTRACTION functions below (see their own
//! paragraph): `DATE_ADD`'s interval unit is always explicit, so there is
//! no ambiguous string to reinterpret the way bare `MINUTE(...)` needs.
//!
//! `INTERVAL` itself ([`tidb_ast::Expr::Interval`]) is a general prefix
//! expression in the parser (matching real MySQL grammar, not
//! special-cased to `DATE_ADD`/`DATE_SUB`), but this evaluator only gives
//! it meaning as their second argument — an `Expr::Interval` there is
//! intercepted in [`func::eval_func`] BEFORE the uniform
//! eager-argument-evaluation every other function goes through (since its
//! `unit` is metadata, not a value `eval_in` can produce on its own).
//! `QUARTER` still parses but is `Unsupported` to evaluate. The interval
//! amount for a SINGLE unit accepts `Int` directly or `Decimal` (rounded to
//! the nearest whole unit via `Decimal::round_to_i64`, ties away from zero
//! — confirmed via `goeval` for both a positive and a negative half-unit,
//! and BEFORE any per-unit multiplication like `WEEK`'s `×7` or `YEAR`'s
//! `×12`); a `Str` amount is `Unsupported` there, needing MySQL's general
//! string-to-number coercion like `FROM_DAYS`'s argument. A COMPOSITE
//! unit's amount, by contrast, is always read as a string (an `Int`/
//! `Decimal` amount is formatted to its plain decimal string first,
//! matching Go's own `getIntervalFromInt`/`getIntervalFromReal`) and split
//! per [`time_fn::calendar::parse_composite_value`]'s doc. The result's
//! computed year is validated against
//! `DATE`'s real `0001`-`9999` range ([`time_fn::calendar::format_ymd_result`] /
//! [`time_fn::calendar::format_ymdhms_result`]): exactly `0` is MySQL's "zero date"
//! string (matching `FROM_DAYS`'s own convention — for `HOUR`/`MINUTE`/
//! `SECOND`, ONLY the date portion becomes the placeholder, the computed
//! time still shows through, e.g. `'0001-01-01 00:00:00' - 1 HOUR` =
//! `'0000-00-00 23:00:00'`), while any OTHER out-of-range year — negative,
//! or past `9999` — is `NULL` (a genuine asymmetry from `FROM_DAYS`'s
//! all-zero-date convention, confirmed via `goeval` for every unit alike).
//! This range check was MISSING entirely from an earlier increment's
//! `DAY`-only implementation — a real bug (`DATE_ADD('9999-12-31',
//! INTERVAL 1 DAY)` silently produced a malformed `10000-01-01` instead of
//! `NULL`), caught and fixed while probing `MONTH`/`YEAR`'s own boundary
//! behavior and confirming `DAY` obeys the identical rule.
//!
//! `HOUR`/`MINUTE`/`SECOND` EXTRACTION (the standalone functions, as
//! opposed to `DATE_ADD`'s interval arithmetic above) implements real
//! TiDB's own two-path algorithm ([`time_fn::calendar::parse_hms_extended`],
//! confirmed via `goeval`, not assumed), selected by whether the argument
//! contains a `:`: a colon-containing string parses as a structured
//! `[DATE ]H:M:S` (`S` defaults to `0`; `H` may be MULTI-DIGIT and exceed
//! 23, since `TIME` is an ELAPSED-time domain, not a wall-clock hour,
//! clamped to real TiDB's documented maximum `838:59:59` — an overflowing
//! `H` clamps the WHOLE value there, not just `H` alone, even when `M`/`S`
//! were individually valid; an out-of-range `M`/`S` invalidates the WHOLE
//! value regardless of `H`); a colon-LESS string (including a plain
//! `DATE`-only value, the common case for a `DATE` column) instead takes
//! ONLY its first digit run and reinterprets it as a right-aligned
//! `HHMMSS`-style number (so `MINUTE('2021-01-01')` is `20`, not `0`) —
//! the SAME rule an integer-literal argument like `HOUR(103045)` already
//! needs. This is unrelated to `DATE_ADD`'s `HOUR`/`MINUTE`/`SECOND`
//! interval handling above — that unit is always explicit, so there is no
//! ambiguous string to disambiguate the way bare `HOUR(...)` needs.
//!
//! `EXTRACT(unit FROM expr)` ([`tidb_ast::Expr::Extract`]) is a
//! genuinely separate general-purpose extraction syntax from `INTERVAL`
//! above — its OWN grammar (`unit FROM value`, not `value unit`), and
//! evaluated with NO new date/time logic at all: `eval_in`'s own arm is
//! sugar for calling `func::eval_func(unit, &[value], cols)` directly,
//! the SAME dispatch an ordinary `YEAR(x)`/`HOUR(x)`/... call already
//! goes through — every simple unit this evaluator already supports as
//! a standalone function works identically through `EXTRACT`. A COMPOSITE
//! unit like `DAY_HOUR` (real MySQL/TiDB grammar) resolves the SAME way,
//! into `time_fn::dispatch`'s own entry for it
//! ([`time_fn::calendar::extract_composite`]); any other unrecognized unit
//! still falls straight into `eval_func`'s own existing "unsupported
//! function" catch-all, with no separate rejection code needed.
//!
//! A genuinely unrelated gap surfaced while probing `EXTRACT`'s own
//! edge cases (deliberately deferred at the time to a dedicated later
//! increment, now closed): `time_fn::calendar::parse_date_ymd` did not handle a
//! bare, separator-less digit run at all (`YEAR(20240315)` gave `NULL`
//! instead of real TiDB's `2024`), the SAME class of gap `HOUR`/
//! `MINUTE`/`SECOND`'s own colon-less path already solved for `TIME`
//! values — `parse_date_ymd` simply never got the equivalent DATE-side
//! fix. Now fixed: a digit run of EXACTLY 6 or 8 digits is a separate
//! positional `YYMMDD`/`YYYYMMDD` reading (confirmed via `goeval`, not
//! limited to `EXTRACT`, since plain `YEAR(20240315)` diverged too).
//! Probing this surfaced a SECOND, related bug: the 2-digit year inside
//! that 6-digit form — and a separator-based date's own 1- or 2-digit
//! year — needs MySQL's real century-pivot rule (`00..=69` →
//! `2000..=2069`, `70..=99` → `1970..=1999`), which depends on the
//! year's ORIGINAL WRITTEN digit count, not its numeric value: a
//! 3-or-more-digit year is taken LITERALLY even when under 100
//! (`'099-03-15'` is year `99`, confirmed via `goeval`, not pivoted to
//! `1999`). Both fixes share one `expand_year` helper, applied uniformly
//! to the bare-digit-run path and the existing separator-based path
//! alike — `split_numeric_components` now returns each component's
//! digit count alongside its value specifically so the year component's
//! pivot decision has what it needs.
//!
//! `NOW()`/`CURRENT_TIMESTAMP()`/`CURDATE()`/`CURRENT_DATE()`/`CURTIME()`/
//! `CURRENT_TIME()`/`UTC_TIMESTAMP()`/`UTC_DATE()`/`UTC_TIME()` (each
//! `CURRENT_*`/`UTC_*` pair a true synonym of its non-`CURRENT_`/`UTC_`
//! sibling except `CURDATE`/`CURTIME`, which have no `UTC_` counterpart of
//! their own name; `CURRENT_TIMESTAMP`/`CURRENT_DATE`/`CURRENT_TIME`/
//! `UTC_DATE`/`UTC_TIME`/`UTC_TIMESTAMP` all also parse bare, with no `()`
//! at all — a genuine MySQL grammar rule `NOW`/`CURDATE`/`CURTIME` don't
//! share) all read [`Columns::now`] — the current statement's FIXED clock,
//! as `(utc_secs, nanos, tz_offset_seconds)`: the RAW Unix time, never
//! pre-adjusted, plus the session's `time_zone` offset to apply for
//! LOCAL rendering. `NOW`/`CURRENT_TIMESTAMP`/`CURDATE`/`CURTIME` apply the
//! offset; `UTC_TIMESTAMP`/`UTC_DATE`/`UTC_TIME` ignore it and render the
//! raw UTC value directly (confirmed via `gorun`: with a nonzero
//! `time_zone`, `UTC_TIMESTAMP()` only matches `NOW()` when the offset is
//! `+00:00`). `CURDATE`/`CURRENT_DATE`/`UTC_DATE` render `YYYY-MM-DD`
//! only; `CURTIME`/`CURRENT_TIME`/`UTC_TIME` render `HH:MM:SS[.ffffff]`
//! only (no argument at all for the `DATE` trio — confirmed via `godump
//! restore`: `CURDATE(1)` is a genuine parse error); the rest render the
//! full `YYYY-MM-DD HH:MM:SS[.ffffff]`. Rounding is genuinely
//! INCONSISTENT across this family — confirmed via `gorun` and by reading
//! `pkg/expression/builtin_time.go`, not assumed uniform: `NOW`/
//! `CURRENT_TIMESTAMP` always TRUNCATE the fraction; `UTC_TIMESTAMP`
//! always ROUNDS it (ties away from zero), for both its 0-arg and
//! explicit-arg forms alike; `CURTIME`/`CURRENT_TIME`/`UTC_TIME` instead
//! SPLIT — the 0-arg form truncates, but an EXPLICIT argument (even
//! literally `0`) rounds, matching Go's own two separate signatures for
//! each (`format` to no fractional digits at all vs. `format` to full
//! precision then reparse at the target scale). [`NoColumns`]
//! (constant-expression `eval`) has no session, so every function in this
//! family is always `Unsupported` there — this evaluator never falls back
//! to the live wall clock, which would be non-deterministic and
//! unverifiable against a static golden file; a caller establishes the
//! clock (via a `SET timestamp = ...`/`SET time_zone = ...` session, in
//! `tidb-exec`'s case) and threads the SAME value to every resolver used
//! while executing one top-level statement, so every clock-reading call
//! within it reads the identical value — matching real MySQL's "the clock
//! is fixed once per statement" semantics for free, with no dedicated
//! cache. `SYSDATE()` is a genuinely different, harder semantic — it
//! reads the TRUE live clock even mid-statement, ignoring the "fixed per
//! statement" rule every other function here follows (confirmed via
//! reading `builtinSysDateWithoutFspSig`'s own `time.Now()` call) — and
//! remains deliberately out of scope.
//!
//! [`Decimal`] arithmetic (`+`/`-`/`*`) and comparison are exact — computed
//! digit-by-digit on the literal's own digit string, not through a binary
//! float — so they need no rounding and match MySQL's `DECIMAL` bit for bit.
//! `DIV`/`MOD` are exact too (unsigned long division on the same digit
//! strings, truncating toward zero — `DIV`'s quotient is an `Int`; `MOD`'s
//! remainder is a `Decimal` at `max(scale_a, scale_b)`, matching MySQL) and
//! decimal bitwise/shift ops round to the nearest `i64` first (ties away
//! from zero, MySQL's own decimal-to-integer conversion rule) before
//! applying the same integer operator. Bare `/` always promotes both
//! operands to `Decimal` (even two `Int` operands) and rounds to a result
//! scale of the DIVIDEND's own scale plus 4 (MySQL's `div_precision_increment`
//! — the same constant [`avg_of`] already uses, and the divisor's own scale
//! never affects it); `NULL` for division by zero.
//!
//! `FLOAT`/`DOUBLE` (`Datum::Real(f64)`) — the value domain for a
//! scientific-notation literal (`Expr::Float`, e.g. `1.5e2`) — uses
//! NATIVE `f64` arithmetic throughout: unlike `Decimal`, no custom
//! digit-string math is needed, since Rust's own `f64` Display was
//! confirmed (by direct comparison across a wide value range, including
//! subnormals and `f64::MAX`, not assumed) to produce byte-identical
//! output to Go's `strconv.FormatFloat(f, 'f', -1, 64)` — the parity risk
//! this domain was originally deferred over turned out not to exist. An
//! `Int` or `Decimal` operand promotes to `f64` — `Float` DOMINATES
//! `Decimal` in MySQL's promotion hierarchy, the OPPOSITE direction from
//! how `Decimal` dominates `Int` (confirmed via `goeval`: `1.5e2 + 3.14`
//! is `FLOAT:153.14`, not a `Decimal`) — so a `Float` operand is
//! intercepted before the `Decimal`/`Div` dispatch, not after. `DIV`
//! truncates its quotient toward zero to an `Int`, same as `Int`/
//! `Decimal`; `MOD` and `/` use native `f64` remainder/division, so a
//! fractional `MOD` result can carry the same floating-point rounding
//! noise real MySQL's own `f64` does; bitwise/shift operators round to
//! the nearest `i64` first, but TIES TO EVEN — the OPPOSITE tie-breaking
//! rule from `Decimal`'s own bitwise conversion (ties away from zero), a
//! real asymmetry confirmed via `goeval`, not assumed. A literal that
//! would overflow to infinity is rejected at PARSE time by the parser
//! itself (matching real TiDB, confirmed via `godump restore` — the
//! boundary is exactly `f64::MAX`), so every in-domain `Float` value here
//! is finite by construction; an ARITHMETIC result that overflows to
//! infinity is instead a genuine [`EvalError::FloatOverflow`] (confirmed
//! via `goeval`: MySQL raises a real evaluation error there, never
//! silently produces IEEE-754 infinity — underflow to zero, by contrast,
//! is fine and NOT an error). `ABS`/`SIGN`/`LEAST`/`GREATEST`/`NULLIF`
//! all cover `Float`, including MIXED Int/Decimal/Float argument lists
//! for `LEAST`/`GREATEST`/`NULLIF` (their comparison — and, for
//! `LEAST`/`GREATEST`, their RESULT type too — reuses the exact same
//! promotion `+`/`-` already implement, rather than a parallel hand-
//! rolled set of type-pair matches: a real bug where `LEAST`'s result
//! DIDN'T promote was caught by the differential corpus on the very
//! first attempt, not assumed correct); `SIGN(0.0)` is `0`, unlike
//! IEEE-754 `signum` (which is never `0`), confirmed via `goeval`.
//!
//! Anything else outside this domain (columns, other functions, subqueries —
//! resolved by the caller) returns [`EvalError::Unsupported`], so
//! results-ring coverage against the Go engine is measured, not assumed.
//!
//! `CAST(expr AS type)` / `CONVERT(...)` evaluation ([`cast::eval_cast`],
//! `tidb_ast::Expr::Cast`'s own arm here) covers `SIGNED`/`UNSIGNED`/
//! `CHAR`/`BINARY`/`DECIMAL`/`DATE`/`DATETIME`/`YEAR`/`DOUBLE`/`FLOAT`;
//! `TIME`/`JSON` are `Unsupported` (no value domain for either). `UNSIGNED`
//! evaluation is a first-class [`Datum::UInt`] domain: `CAST(-5 AS
//! UNSIGNED)` retains its UInt64 magnitude and comparisons/arithmetic do not
//! fall back to signed display bits.
//!
//! ## Module layout
//!
//! Split by concern so unrelated features can be extended without touching
//! the same file: [`Decimal`] (from the standalone `tidb-datatype` crate),
//! [`value`] (the [`Datum`] domain,
//! [`EvalError`], [`Columns`]), [`ops`] (unary/binary operator evaluation),
//! [`string_fn`] / [`date_fn`] / [`like`] / [`math_fn`] / [`cast`]
//! (builtin-function families and `CAST`/`CONVERT`), and [`func`] (the
//! builtin dispatch table + `IN` predicate) — all wired together by this
//! file's `eval_in`, the single recursive expression evaluator every other
//! module calls back into for its own subexpressions.

mod binary_literal;
mod build;
pub mod builtin_arithmetic;
pub mod builtin_compare;
mod builtin_ext;
pub mod builtin_op;
mod cast;
mod coerce;
pub mod column;
pub mod constant;
mod context;
pub mod expr_collation;
pub mod expression;
mod field_name;
mod func;
mod grouping;
mod like;
mod math_fn;
mod ops;
pub mod pb_comparison;
mod regexp;
pub mod rewriter;
mod rng;
mod row;
pub mod scalar_function;
pub mod schema;
mod string_fn;
mod time_fn;

pub use field_name::{find_field_name, find_field_name_index_by_column, NonUniqueFieldName};

pub use build::{BuildContext, BuiltStringLength, StringLengthFunction, StringLengthSignature};
pub use coerce::truthy_of;
pub use context::{Columns, ErrorLevel, EvalError, JsonError, NoColumns, SessionTimeZone};
pub use grouping::{GroupingFunction, GroupingMetadata, GroupingMetadataError, GroupingMode};
pub use like::{ilike_match, like_match_with_collation};
pub use rng::MysqlRng;
pub use row::compare_datums;
pub(crate) use tidb_datatype::{Datum, Decimal};

use tidb_ast::{CastStyle, Expr, GetFormatSelector, IsTarget};

use binary_literal::{bit_literal_value, hex_literal_value};
use coerce::{bool_int, coerce_str, coerce_str_bytes};
use func::{eval_func, eval_in_list, negate_if};
use like::like_match;
use ops::{
    effective_div_precision_increment, eval_binary, eval_binary_with_div_precision, eval_unary,
    logic_and, truthy_with_mysql_string,
};
use regexp::regexp_match;
use row::row_compare;
use string_fn::{position, trim_value};

/// Evaluates a constant expression, or returns why it is out of scope.
pub fn eval(expr: &Expr) -> Result<Datum, EvalError> {
    eval_in(expr, &NoColumns)
}

/// Applies a binary operator to already-evaluated operands. Exposed so callers
/// that intercept some sub-expressions (e.g. aggregates during grouping) can
/// still reuse the operator semantics.
pub fn apply_binary(op: tidb_ast::BinaryOp, l: Datum, r: Datum) -> Result<Datum, EvalError> {
    eval_binary(op, l, r)
}

/// Applies TiDB's byte-preserving `CONCAT` coercion to already-evaluated
/// values without round-tripping them through literal AST nodes.
pub fn concat_values(values: &[Datum]) -> Result<Datum, EvalError> {
    string_fn::concat(values)
}

/// Applies a binary operator with the current session's explicit
/// `div_precision_increment`. Every table-backed scalar, grouped, and window
/// division path calls this rather than relying on [`apply_binary`]'s
/// context-free default.
pub fn apply_binary_with_div_precision(
    op: tidb_ast::BinaryOp,
    l: Datum,
    r: Datum,
    div_precision_increment: u32,
    ctx: &dyn crate::context::Columns,
) -> Result<Datum, EvalError> {
    eval_binary_with_div_precision(op, l, r, div_precision_increment, ctx)
}

/// Applies a unary operator to an already-evaluated operand.
pub fn apply_unary(op: tidb_ast::UnaryOp, v: Datum) -> Result<Datum, EvalError> {
    eval_unary(op, v)
}

/// `AVG`'s `SUM / COUNT`, exposed so `tidb-exec` can compute it without
/// reimplementing decimal division: an `Int` sum promotes to decimal (scale
/// 0, MySQL's implicit rule, same as every other decimal op); the result
/// scale grows by MySQL's `div_precision_increment` past the sum's own scale,
/// and is ROUNDED to that scale (ties away from zero) via true division — unlike `DIV`/`MOD`,
/// which truncate exactly and need no such growth. A `Float` sum instead
/// divides via plain native `f64` division — MySQL's `div_precision_increment`
/// scale growth is a `DECIMAL`-specific rule that doesn't apply to `AVG`
/// over a real `FLOAT`/`DOUBLE` column (confirmed via `gorun`: `AVG` there
/// is exactly `sum / count`, not assumed to match the `Decimal` rule).
/// `count` must be positive (an empty group is the caller's job to turn
/// into `NULL` before calling this, same as `SUM`).
pub fn avg_of(sum: Datum, count: i64) -> Result<Datum, EvalError> {
    avg_of_with_div_precision(sum, count, 4)
}

/// The session-aware form of [`avg_of`]. `AVG` uses the same
/// `div_precision_increment` as scalar `/`, so callers with a SQL session
/// must pass its current value explicitly.
pub fn avg_of_with_div_precision(
    sum: Datum,
    count: i64,
    div_precision_increment: u32,
) -> Result<Datum, EvalError> {
    let d = match sum {
        Datum::Real(f) => return Ok(Datum::Real(f / count as f64)),
        Datum::Float32(f) => return Ok(Datum::Float32(f / count as f64)),
        Datum::Decimal(d) => d,
        Datum::Int(i) => Decimal::from_int(i),
        Datum::UInt(i) => Decimal::from_uint(i),
        Datum::String(_) | Datum::Bytes(_) | Datum::Null | Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported("AVG of non-numeric"));
        }
        other => {
            other
                .to_decimal()
                .map_err(|_| EvalError::Unsupported("AVG of non-numeric"))?
                .value
        }
    };
    let target_scale = d.scale() + effective_div_precision_increment(div_precision_increment);
    Ok(Datum::Decimal(d.div_round(count, target_scale)))
}

/// Fits a value into a `DECIMAL(precision, scale)` column for storage:
/// rounds a numeric value to `scale` and range-checks its integer part
/// (see [`Decimal::fit_precision_scale`]). Returns the rounded value, or
/// `None` when the integer part overflows (the caller turns that into a
/// column-out-of-range error). `NULL` and any non-numeric value pass
/// through unchanged — coercing those is outside this width-check's scope.
/// Used by `tidb_exec`'s `INSERT`/`UPDATE` column-width validation.
pub fn fit_decimal_column(value: Datum, precision: u32, scale: u32) -> Option<Datum> {
    match value {
        Datum::Decimal(d) => d.fit_precision_scale(precision, scale).map(Datum::Decimal),
        Datum::Int(i) => Decimal::from_int(i)
            .fit_precision_scale(precision, scale)
            .map(Datum::Decimal),
        Datum::UInt(i) => Decimal::from_uint(i)
            .fit_precision_scale(precision, scale)
            .map(Datum::Decimal),
        other => Some(other),
    }
}

/// Evaluates an expression, resolving column references via `cols`.
pub fn eval_in(expr: &Expr, cols: &dyn Columns) -> Result<Datum, EvalError> {
    match expr {
        Expr::Int(s) => s
            .parse::<u64>()
            .map(|i| match i64::try_from(i) {
                Ok(i) => Datum::Int(i),
                Err(_) => Datum::UInt(i),
            })
            .map_err(|_| EvalError::IntOverflow),
        Expr::Bool(b) => Ok(Datum::Int(i64::from(*b))),
        Expr::String(s) => Ok(Datum::new_string(s.clone())),
        Expr::Decimal(s) => Ok(Datum::Decimal(Decimal::from_literal(s))),
        // Always finite: the parser itself rejects a literal that would
        // overflow to infinity (confirmed via `godump restore` — real
        // TiDB rejects `1e400` at PARSE time, not eval time), so no
        // finiteness check is needed here.
        Expr::Float(f) => Ok(Datum::Real(*f)),
        Expr::Hex(digits) => hex_literal_value(digits),
        Expr::Bit(digits) => bit_literal_value(digits),
        Expr::Null => Ok(Datum::Null),
        Expr::Column(path) => cols
            .get(path)
            .ok_or(EvalError::Unsupported("unknown column")),
        // Reading an unset (or session-less) user variable is `NULL`,
        // never an error — the opposite convention from `Expr::SysVar`
        // just below, whose UNRECOGNIZED-name case is a genuine
        // `Unsupported` (see `Columns::get_uservar`'s own doc for why the
        // two differ). `SET @x = ...` (assignment) is a separate
        // top-level `tidb_ast::SessionStmt::SetUserVar` statement, not an
        // expression form reachable from here.
        Expr::UserVar(name) => Ok(cols.get_uservar(name).unwrap_or(Datum::Null)),
        // The inline `@x := expr` ASSIGNMENT EXPRESSION (usable
        // mid-`SELECT`, e.g. the classic MySQL running-total idiom
        // `SELECT @rn := @rn + 1 FROM t`, confirmed via `gorun`):
        // evaluates `value`, writes it through `Columns::set_uservar`
        // (a SIDE EFFECT — see that method's own doc for the interior-
        // mutability architecture this relies on), and evaluates to the
        // SAME assigned value, matching `gorun`'s own observed
        // `SELECT @i := 1` => `1`. This function's own CALLER
        // (`tidb_exec::aggregate::Database::project_row`'s row-wise
        // select-list loop, evaluated left to right per row) is what
        // gives a LATER select-list item visibility into an EARLIER
        // one's assignment within the same row, and one row's
        // assignment visibility into the next — this function itself
        // has no notion of "row" or "order," it just performs the one
        // write it's asked for.
        Expr::Assign { name, value } => {
            let v = eval_in(value, cols)?;
            // Go's scalar `SETVAR` signatures return NULL without touching
            // the existing variable when their RHS is NULL
            // (`builtin_other.go`'s `builtinSet*VarSig`). This is distinct
            // from top-level `SET @x = NULL`, whose executor semantics clear
            // the session value, so keep the boundary here rather than
            // teaching `Columns::set_uservar` a statement-kind flag.
            if v != Datum::Null {
                cols.set_uservar(name, v.clone());
            }
            Ok(v)
        }
        Expr::SysVar { scope, name } => cols
            .sysvar(*scope, name)
            .ok_or(EvalError::Unsupported("unknown system variable")),
        Expr::Paren(e) => eval_in(e, cols),
        Expr::Unary(op, e) => eval_unary(*op, eval_in(e, cols)?),
        // `ROW(...) <op> ROW(...)` — see `crate::row`'s own doc for
        // why this is a special case rather than a new `Datum`
        // variant: real MySQL/TiDB restricts a bare `ROW(...)` to
        // ONLY appear as a comparison/`IN` operand, so `eval_in` never
        // needs to evaluate one standalone.
        Expr::Binary(op, l, r)
            if matches!((l.as_ref(), r.as_ref()), (Expr::Row(_), Expr::Row(_))) =>
        {
            let (Expr::Row(lv), Expr::Row(rv)) = (l.as_ref(), r.as_ref()) else {
                unreachable!("checked above")
            };
            let lv: Vec<Datum> = lv
                .iter()
                .map(|e| eval_in(e, cols))
                .collect::<Result<_, _>>()?;
            let rv: Vec<Datum> = rv
                .iter()
                .map(|e| eval_in(e, cols))
                .collect::<Result<_, _>>()?;
            row_compare(*op, &lv, &rv)
        }
        Expr::Binary(op, l, r) => eval_binary_with_div_precision(
            *op,
            eval_in(l, cols)?,
            eval_in(r, cols)?,
            cols.div_precision_increment(),
            cols,
        ),
        // A constant `RAND(N)` has state per function occurrence for the
        // whole statement. The function node's address is stable while this
        // parsed statement is evaluated; an argument-slice view is not,
        // because temporary views can be reused for siblings.
        Expr::Func { name, args, .. } => {
            eval_func(name, args, cols, Some(expr as *const Expr as usize))
        }
        // `EXTRACT(unit FROM value)` is sugar for calling the SAME
        // single-argument function `unit` already names — a simple unit
        // (`YEAR`/`HOUR`/...) or a composite one (`DAY_HOUR`/...,
        // `time_fn::calendar::extract_composite`'s own entries in
        // `time_fn::dispatch`) alike — no separate extraction logic needed;
        // `eval_func`'s own catch-all rejects only a genuinely unrecognized
        // function name.
        Expr::Extract { unit, value } => {
            eval_func(unit, std::slice::from_ref(value.as_ref()), cols, None)
        }
        // `GET_FORMAT(<type>, location)` — the type is an AST selector (the
        // parser already collapsed `TIMESTAMP` into `Datetime`), so only the
        // location is evaluated; a NULL location yields NULL. Port of
        // `builtinGetFormatSig.evalString` + `getFormat`.
        Expr::GetFormat { selector, expr } => match coerce_str(&eval_in(expr, cols)?)? {
            None => Ok(Datum::Null),
            Some(location) => {
                let format_type = match selector {
                    GetFormatSelector::Date => "DATE",
                    GetFormatSelector::Time => "TIME",
                    GetFormatSelector::Datetime => "DATETIME",
                };
                Ok(Datum::new_string(time_fn::get_format(
                    format_type,
                    &location,
                )))
            }
        },
        // `CAST`/`CONVERT(expr, type)` share one evaluator (see
        // `tidb_ast::Expr::Cast`'s own doc for why they share one AST node);
        // `NULL` maps to `NULL` for every target type, so it's handled once
        // here rather than in each of `cast::eval_cast`'s own arms.
        //
        // The three ODBC-style typed-literal styles (`DATE`/`TIME`/
        // `TIMESTAMP 'literal'`) are checked FIRST and always
        // `Unsupported`, deliberately NOT falling through to
        // `cast::eval_cast` — confirmed via `goeval`/`gorun` that real
        // TiDB's own evaluation for these genuinely diverges from
        // `CAST(... AS DATE)`'s existing (lenient, `NULL`-on-invalid)
        // behavior: an invalid date string is a hard query ERROR for the
        // typed-literal form (`SELECT DATE '2007-10-00'` fails outright),
        // not `NULL` — reusing `cast::eval_cast` here would silently
        // produce the WRONG value for exactly the invalid-date inputs
        // this syntax is most often used to test, not just an incomplete
        // one. See `tidb_ast::CastStyle::DateLiteral`'s own doc.
        Expr::Cast(cast)
            if matches!(
                cast.style,
                CastStyle::DateLiteral | CastStyle::TimeLiteral | CastStyle::TimestampLiteral
            ) =>
        {
            Err(EvalError::Unsupported("date/time/timestamp literal"))
        }
        // `AS type ARRAY` (a JSON multi-valued-index type modifier — see
        // `tidb_ast::CastExpr::array`'s own doc) is ALWAYS `Unsupported`,
        // unconditionally — this crate has no JSON value domain at all,
        // the SAME boundary `CastType::Json` already has. Covers
        // `CastStyle::JsonSumCrc32` too, which always sets `array: true`.
        Expr::Cast(cast) if cast.array => Err(EvalError::Unsupported("ARRAY cast type")),
        Expr::Cast(cast) => match eval_in(&cast.expr, cols)? {
            Datum::Null => Ok(Datum::Null),
            v => cast::eval_cast(&cast.cast_type, v),
        },
        // `CONVERT(expr USING charset)` is a charset conversion, not a
        // value-type cast — this crate has no charset domain at all, so
        // evaluation is a plain stringification passthrough (confirmed via
        // `goeval`: `CONVERT(123 USING utf8)` is the STRING `"123"`, not the
        // integer `123`).
        Expr::ConvertUsing { expr, .. } => match eval_in(expr, cols)? {
            Datum::Null => Ok(Datum::Null),
            v => Ok(Datum::new_string(v.sql_string().map_err(|_| {
                EvalError::Unsupported("invalid UTF-8 string coercion")
            })?)),
        },
        // `COLLATE` doesn't change the value at all (unlike `CONVERT ...
        // USING`, which stringifies) — it only affects comparison/sort
        // behavior, not modelled here (see `tidb_ast::Expr::Collate`'s own
        // doc). A NON-string operand is a genuine error in real TiDB
        // (confirmed via `goeval`: `1 COLLATE utf8mb4_bin` errors), but
        // that type restriction is a KNOWN, deliberately unmodelled
        // boundary here — every real-world use found in the corpus that
        // surfaced this feature collates a string, so this stays a plain
        // passthrough rather than adding a check nothing exercises.
        Expr::Collate { expr, .. } => eval_in(expr, cols),
        Expr::In { expr, list, not } => eval_in_list(expr, list, *not, cols),
        Expr::Between {
            expr,
            low,
            high,
            not,
        } => {
            // `x BETWEEN lo AND hi` is `x >= lo AND x <= hi`, in three-valued
            // logic; `NOT BETWEEN` negates the result (NULL stays NULL).
            let v = eval_in(expr, cols)?;
            let ge = eval_binary(tidb_ast::BinaryOp::Ge, v.clone(), eval_in(low, cols)?)?;
            let le = eval_binary(tidb_ast::BinaryOp::Le, v, eval_in(high, cols)?)?;
            Ok(negate_if(logic_and(ge, le)?, *not))
        }
        Expr::Is { expr, target, not } => {
            // IS is always TRUE/FALSE (never NULL): it tests a definite property.
            let v = eval_in(expr, cols)?;
            let holds = match target {
                IsTarget::Null | IsTarget::Unknown => v == Datum::Null,
                IsTarget::True => truthy_with_mysql_string(&v)? == Some(true),
                IsTarget::False => truthy_with_mysql_string(&v)? == Some(false),
            };
            Ok(bool_int(holds ^ not))
        }
        Expr::Like {
            expr,
            pattern,
            not,
            ilike,
            escape,
        } => {
            // Case-sensitive (utf8mb4_bin) LIKE; either NULL operand yields
            // NULL. A non-string operand (on EITHER side, confirmed via
            // `gorun`: `'2' LIKE 2` is TRUE) is implicitly stringified the
            // SAME way `Datum::sql_string` already renders it — including
            // a `DECIMAL`'s declared scale, confirmed via `gorun`: `12.50
            // LIKE '12.5'` is FALSE but `12.50 LIKE '12.50'` is TRUE,
            // matching how `Decimal`'s own `Display` already keeps
            // trailing zeros rather than simplifying them away. `escape`
            // passes straight through to `like_match` — see
            // `tidb_ast::Expr::Like::escape`'s own doc for its exact
            // `None`/`Some(0)`/`Some(byte)` meaning, confirmed via
            // `gorun` for a custom single-byte escape character.
            match (eval_in(expr, cols)?, eval_in(pattern, cols)?) {
                (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
                (v, p) => {
                    let value = v
                        .sql_string()
                        .map_err(|_| EvalError::Unsupported("invalid UTF-8 LIKE operand"))?;
                    let pattern = p
                        .sql_string()
                        .map_err(|_| EvalError::Unsupported("invalid UTF-8 LIKE pattern"))?;
                    let (value, pattern) = if *ilike {
                        (value.to_lowercase(), pattern.to_lowercase())
                    } else {
                        (value, pattern)
                    };
                    Ok(bool_int(like_match(&value, &pattern, *escape) ^ not))
                }
            }
        }
        // Case-sensitive (utf8mb4_bin) `[NOT] REGEXP`/`RLIKE`, the SAME
        // NULL-propagation and non-string-operand-coercion rules
        // `Expr::Like` just above already established (confirmed via
        // `gorun`: `5 REGEXP '5'` is `TRUE`) — see `crate::regexp::
        // regexp_match`'s own doc for the empty-pattern/malformed-
        // pattern error rules.
        Expr::Regexp { expr, pattern, not } => {
            match (eval_in(expr, cols)?, eval_in(pattern, cols)?) {
                (Datum::Null, _) | (_, Datum::Null) => Ok(Datum::Null),
                (v, p) => {
                    let value = v
                        .sql_string()
                        .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP operand"))?;
                    let pattern = p
                        .sql_string()
                        .map_err(|_| EvalError::Unsupported("invalid UTF-8 REGEXP pattern"))?;
                    Ok(bool_int(regexp_match(&value, &pattern)? ^ not))
                }
            }
        }
        Expr::Position { substr, str } => Ok(position(
            coerce_str(&eval_in(substr, cols)?)?,
            coerce_str(&eval_in(str, cols)?)?,
        )),
        Expr::Trim {
            expr,
            remstr,
            direction,
        } => {
            // A bare `TRIM(expr)` (no `remstr`, no `direction`) defaults
            // to stripping spaces from BOTH ends — see
            // `tidb_ast::Expr::Trim::remstr`'s own doc for why every
            // OTHER combination already has a real `remstr` (a `NULL`
            // remstr's own explicit `NULL` restores un-omitted, so it
            // still reaches here as a real evaluated expression, not a
            // magic `None`).
            let str_value = eval_in(expr, cols)?;
            let binary = matches!(str_value, Datum::Bytes(_));
            let str = coerce_str_bytes(&str_value)?;
            let remstr = match remstr {
                Some(r) => coerce_str_bytes(&eval_in(r, cols)?)?,
                None => Some(b" ".to_vec()),
            };
            Ok(trim_value(
                str,
                remstr,
                direction.unwrap_or(tidb_ast::TrimDirection::Both),
                binary,
            ))
        }
        Expr::Case {
            value,
            when_clauses,
            else_clause,
        } => {
            // LAZY: only the WHEN conditions up to (and including) the
            // first match, plus that one branch's own result, are ever
            // evaluated — matching real MySQL's short-circuit CASE, a
            // load-bearing idiom for guarding against errors (confirmed
            // via `gorun`: `CASE WHEN x != 0 THEN 1/x ELSE NULL END`
            // never raises division-by-zero for `x = 0`). Real MySQL
            // additionally infers CASE's overall result type from EVERY
            // branch statically (even ones never evaluated — confirmed
            // via `gorun`: the type promotes even when the promoting
            // branch is an unreached `1/0`), which cannot be replicated
            // without a genuine type-inference pass; deliberately NOT
            // attempted here — the result is simply whichever branch was
            // taken, in its own natural type, matching the common case
            // where every branch already shares one type.
            let taken = match value {
                // Simple form: `value = cond`, ordinary `=` (not `<=>`) —
                // a NULL `value` or `cond` never matches, matching `=`'s
                // own propagation (confirmed via `goeval`: `CASE NULL
                // WHEN NULL THEN 1 ELSE 2 END` is `2`, not `1`).
                Some(value_expr) => {
                    let v = eval_in(value_expr, cols)?;
                    let mut taken = None;
                    for (cond, result) in when_clauses {
                        let w = eval_in(cond, cols)?;
                        if eval_binary(tidb_ast::BinaryOp::Eq, v.clone(), w)? == Datum::Int(1) {
                            taken = Some(result);
                            break;
                        }
                    }
                    taken
                }
                // Searched form: each `cond` is truthiness-tested
                // directly, the same three-valued logic `IF`/`WHERE`
                // already use.
                None => {
                    let mut taken = None;
                    for (cond, result) in when_clauses {
                        if truthy_with_mysql_string(&eval_in(cond, cols)?)? == Some(true) {
                            taken = Some(result);
                            break;
                        }
                    }
                    taken
                }
            };
            match taken {
                Some(result) => eval_in(result, cols),
                None => match else_clause {
                    Some(e) => eval_in(e, cols),
                    None => Ok(Datum::Null),
                },
            }
        }
        _ => Err(EvalError::Unsupported("unsupported expression")),
    }
}

#[cfg(test)]
mod tests;
