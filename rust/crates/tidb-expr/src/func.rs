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

//! Thin builtin-family routing plus the non-family control functions and the
//! `[NOT] IN (list)` predicate. Called from `crate::eval_in`.

use tidb_ast::{BinaryOp, Expr};

use crate::coerce::{bool_int, truthy_of};
use crate::eval_in;
use crate::row::row_compare;
use crate::string_fn::{
    ascii, bin, bit_count, bit_length, case_convert, char_func, concat, concat_ws, elt, field,
    format_num, from_base64, hex, locate, locate_collation, make_set, oct, ord, quote, replace,
    reverse, str_insert, str_take, strcmp, substring, substring_index, unhex,
};
use crate::string_packet::{pad, repeat, space, to_base64};
use crate::time_fn::calendar::{date_add, date_diff, date_format, date_part, from_days, time_part};
use crate::{BuildContext, Columns, Datum, EvalError, StringLengthFunction};

/// Evaluates a builtin scalar function over its evaluated arguments.
pub(crate) fn eval_func(
    name: &str,
    args: &[Expr],
    cols: &dyn Columns,
    function_key: Option<usize>,
) -> Result<Datum, EvalError> {
    let name = name.to_ascii_uppercase();
    // `DATE_ADD`/`DATE_SUB`'s second argument is an `Expr::Interval` (a
    // value *and* a unit keyword), not a plain expression `eval_in` can
    // evaluate on its own — handled here, before every other function's
    // uniform eager-eval of `args` below, which would otherwise choke on it.
    // `ADDDATE`/`SUBDATE` share this SAME `date, Expr::Interval` shape —
    // `tidb_parser::parse_adddate_or_subdate` already normalizes their own
    // dual `INTERVAL n unit` / bare-number grammar down to it (see that
    // function's own doc) — and evaluate IDENTICALLY to `DATE_ADD`/
    // `DATE_SUB` respectively (confirmed via `gorun`: `ADDDATE(d, 1)` and
    // `DATE_ADD(d, INTERVAL 1 DAY)` produce the same result for every
    // case tried, including a month-end rollover, `NULL` propagation in
    // either argument, and a sub-day `HOUR` unit) — so `ADDDATE` takes
    // the SAME `sign = 1` as `DATE_ADD`, `SUBDATE` the same `sign = -1`
    // as `DATE_SUB`, reusing `date_add` with no new logic at all.
    if name == "DATE_ADD" || name == "DATE_SUB" || name == "ADDDATE" || name == "SUBDATE" {
        let [date_expr, Expr::Interval { value, unit }] = args else {
            return Err(EvalError::Unsupported("DATE_ADD/DATE_SUB arguments"));
        };
        let date_val = eval_in(date_expr, cols)?;
        let amount_val = eval_in(value, cols)?;
        let sign = if name == "DATE_SUB" || name == "SUBDATE" {
            -1
        } else {
            1
        };
        return date_add(unit, &date_val, &amount_val, sign);
    }
    // `NEXTVAL`/`LASTVAL`/`SETVAL`'s first argument is the SEQUENCE NAME
    // (real TiDB parses it as a `TableNameExpr`, this crate as a plain
    // `Expr::Column` — see task #121's restore-equivalence finding), so it
    // must NOT be evaluated as a column reference the way the uniform
    // eager-eval below would — dispatched to the resolver's own sequence
    // catalog instead, the same interior-mutability side-effect
    // architecture `Columns::set_uservar` established.
    if name == "NEXTVAL" || name == "LASTVAL" || name == "SETVAL" {
        let Some(Expr::Column(path)) = args.first() else {
            return Err(EvalError::Unsupported("sequence function argument"));
        };
        return match (name.as_str(), args.len()) {
            ("NEXTVAL", 1) => cols.sequence_nextval(path),
            ("LASTVAL", 1) => cols.sequence_lastval(path),
            ("SETVAL", 2) => match eval_in(&args[1], cols)? {
                // NULL propagates without touching the sequence, matching
                // real TiDB's own `EvalInt` isNull short-circuit (read
                // from `builtinSetValSig.evalInt` directly).
                Datum::Null => Ok(Datum::Null),
                Datum::Int(n) => cols.sequence_setval(path, n),
                Datum::UInt(n) => cols.sequence_setval(path, n as i64),
                _ => Err(EvalError::Unsupported("SETVAL value")),
            },
            _ => Err(EvalError::Unsupported("sequence function arguments")),
        };
    }
    // Go selects these signatures from the argument expression's FieldType
    // while building the function, before EvalString produces a runtime
    // datum. Keep the same ordering here: source AST type facts choose one
    // immutable evaluator first, then only that evaluator sees the value.
    if args.len() == 1 {
        let function = match name.as_str() {
            "LENGTH" | "OCTET_LENGTH" => Some(StringLengthFunction::Length),
            "CHAR_LENGTH" | "CHARACTER_LENGTH" => Some(StringLengthFunction::CharLength),
            _ => None,
        };
        if let Some(function) = function {
            let built = BuildContext::default().build_string_length_for_expr(function, &args[0])?;
            let value = eval_in(&args[0], cols)?;
            // `LENGTH`/`OCTET_LENGTH` are binary-aware and count the ENCODED
            // bytes; `CHAR_LENGTH` is `funcPropNone` and counts characters of
            // the UTF-8 form, so only the former transcodes.
            let value = match function {
                StringLengthFunction::Length => {
                    crate::convert_charset::to_binary_by_collation(&value)?
                }
                StringLengthFunction::CharLength => value,
            };
            return built.eval(&value);
        }
    }
    // `IF` is a lazy control function in Go: `builtinIf*Sig` evaluates the
    // condition through its wrapped `EvalInt`, then evaluates exactly one
    // result branch.  Handle it before the ordinary eager argument material-
    // ization below so an unreachable error (for example `1 / 0`) cannot
    // leak into the selected result.  The value-only evaluator has no
    // FieldType result-promotion pass; the selected branch therefore keeps
    // its natural Datum domain, while the Go function-class type boundary is
    // recorded as explicit partial evidence rather than guessed at runtime.
    if name == "IF" {
        let [condition, when_true, when_false] = args else {
            return Err(EvalError::Unsupported("bad IF arguments"));
        };
        let condition = eval_in(condition, cols)?;
        // Go's `wrapWithIsTrue` gives the condition the same `Datum.ToBool`
        // reading every other boolean context uses; NULL is false because
        // `keepNull` is disabled.
        let take_true = truthy_of(&condition)? == Some(true);
        return eval_in(if take_true { when_true } else { when_false }, cols);
    }
    let vals: Vec<Datum> = args
        .iter()
        .map(|a| eval_in(a, cols))
        .collect::<Result<_, _>>()?;
    // Go `HandleBinaryLiteral`'s `funcPropBinAware` arm. The chunk path reads
    // the argument's static charset; this value-only path reads the datum's
    // own collation, which carries the same charset -- so a `gbk` string
    // reaching `HEX`/`LENGTH`/`ASCII` transcodes here exactly as it does
    // there. See `crate::convert_charset` for why this is the only implicit
    // transcode.
    let vals: Vec<Datum> = if crate::convert_charset::func_prop(&name.to_ascii_lowercase())
        == crate::convert_charset::FuncProp::BinAware
    {
        vals.iter()
            .map(crate::convert_charset::to_binary_by_collation)
            .collect::<Result<_, _>>()?
    } else {
        vals
    };
    // Go's `newBaseBuiltinFuncWithTp` argument-cast layer. This tier has no
    // static argument types (see `crate::arg_eval_type`), so it gets Go's
    // wrap from the values alone -- which is everything except the `YEAR`
    // distinction, a type no value can carry.
    let vals = crate::arg_eval_type::wrap_datetime_args(name.as_str(), vals, &[], cols)?;
    if let Some(result) = crate::math_fn::dispatch(name.as_str(), args, &vals, cols, function_key) {
        return result;
    }
    if let Some(result) = eval_func_values_in(name.as_str(), &vals, cols) {
        return result;
    }
    // Family extension modules (`crate::builtin_ext`), the shared values-only
    // arms, and the session-state functions all live in
    // `eval_func_values_in`, tried above; only the session-clock time family
    // remains.
    crate::time_fn::dispatch(name.as_str(), &vals, cols)
        .unwrap_or(Err(EvalError::Unsupported("unsupported function")))
}

/// The builtins whose result is a function of their argument values AND the
/// session: `ROW_COUNT()` and both forms of `LAST_INSERT_ID`. `None` if
/// `name` is not one of them.
///
/// Ports `builtinRowCountSig.evalInt` and the `builtinLastInsertID*Sig` pair
/// (`pkg/expression/builtin_info.go`).
fn eval_session_state(
    name: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    Some(match (name, vals) {
        ("ROW_COUNT", []) => cols
            .row_count()
            .map(Datum::Int)
            .ok_or(EvalError::Unsupported("ROW_COUNT requires a session")),
        // `LAST_INSERT_ID()` reads the value promoted from the preceding
        // statement. Its one-argument form instead coerces through Go's
        // `EvalInt`, records the raw uint64 bits for NEXT-statement
        // promotion, and returns the same UNSIGNED result immediately.
        // Keeping these forms together makes their same-statement separation
        // explicit: `LAST_INSERT_ID(5), LAST_INSERT_ID()` is `5, old`, not
        // `5, 5` (pkg/executor/select.go's statement-context promotion).
        ("LAST_INSERT_ID", []) => cols
            .last_insert_id()
            .map(Datum::UInt)
            .ok_or(EvalError::Unsupported("LAST_INSERT_ID requires a session")),
        ("LAST_INSERT_ID", [Datum::Null]) => Ok(Datum::Null),
        ("LAST_INSERT_ID", [value]) => match last_insert_id_arg(value) {
            Ok(id) => {
                cols.set_last_insert_id(id);
                Ok(Datum::UInt(id))
            }
            Err(e) => Err(e),
        },
        ("ROW_COUNT" | "LAST_INSERT_ID", _) => Err(EvalError::Unsupported("bad function arity")),
        // The sequence builtins. The first argument is the sequence's name
        // path, substituted for the column reference the parser produced (see
        // the `nextval` arm of `rewriter::rewrite_expr_resolved`).
        //
        // `NEXTVAL` is the one builtin here that MUTATES durable state, and Go
        // does it outside the statement's transaction, so a rollback does not
        // give the value back (captured).
        ("NEXTVAL", [path]) => match sequence_path(path) {
            Ok(path) => cols.sequence_nextval(&path),
            Err(e) => Err(e),
        },
        ("LASTVAL", [path]) => match sequence_path(path) {
            Ok(path) => cols.sequence_lastval(&path),
            Err(e) => Err(e),
        },
        ("SETVAL", [path, value]) => match (sequence_path(path), value) {
            // Go evaluates the second argument as an int; a NULL one makes the
            // whole call NULL before the sequence is touched
            // (`builtinSetValSig.evalInt`'s isNull short-circuit).
            (Ok(_), Datum::Null) => Ok(Datum::Null),
            (Ok(path), value) => match value.as_int() {
                Some(value) => cols.sequence_setval(&path, value),
                None => Err(EvalError::Unsupported("SETVAL needs an integer value")),
            },
            (Err(e), _) => Err(e),
        },
        ("NEXTVAL" | "LASTVAL" | "SETVAL", _) => Err(EvalError::Unsupported("bad function arity")),
        _ => return None,
    })
}

/// The name path a sequence builtin's first argument carries on the CHUNK path,
/// where the rewriter replaced the parser's column reference with one string
/// constant (see `rewriter::rewrite_expr_resolved`).
///
/// The segments are joined by NUL rather than `.` so the split back is exact:
/// a backquoted identifier may contain a dot, but no identifier can contain a
/// NUL byte. That keeps the chunk path handing `Columns::sequence_nextval` the
/// SAME `&[String]` the row path hands it straight from the parser.
pub(crate) const SEQUENCE_PATH_SEPARATOR: char = '\0';

fn sequence_path(value: &Datum) -> Result<Vec<String>, EvalError> {
    match value {
        Datum::Bytes(bytes) => Ok(String::from_utf8(bytes.clone())
            .map_err(|_| EvalError::Unsupported("a sequence name must be text"))?
            .split(SEQUENCE_PATH_SEPARATOR)
            .map(str::to_owned)
            .collect()),
        _ => Err(EvalError::Unsupported(
            "a sequence builtin's first argument must be a name",
        )),
    }
}

/// [`eval_func_values`] plus the statement-context side effects Go attaches
/// to a builtin whose VALUE is still a pure function of its arguments.
///
/// Only `JSON_MERGE` has one today: `builtinJSONMergeSig.evalJSON` appends
/// `errDeprecatedSyntaxNoReplacement` (1681) after computing the merge, so a
/// NULL argument (which returns before that line) and a failed merge both
/// leave the warning unraised. Both evaluators route through here so the
/// warning cannot depend on which one ran.
pub(crate) fn eval_func_values_in(
    name: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    // The session-state builtins: pure functions of their argument VALUES
    // plus the session, which `cols` supplies. They live here rather than in
    // `eval_func_values` (values alone) so the row path and the chunk path
    // run the SAME implementation -- `eval_func`'s own arms used to be the
    // only ones, which is why `ROW_COUNT()` in a chunk-evaluated statement
    // reported "not yet ported" while the identical AST-evaluated statement
    // answered.
    if let Some(result) = eval_session_state(name, vals, cols) {
        return Some(result);
    }
    let result = eval_func_values(name, vals, cols)?;
    if name == "JSON_MERGE" && matches!(result, Ok(ref value) if *value != Datum::Null) {
        cols.append_warning(
            1681,
            "JSON_MERGE is deprecated and will be removed in a future release.",
        );
    }
    Some(result)
}

/// Evaluates a builtin whose result is a pure function of its
/// already-evaluated argument values — the values-only subset of
/// [`eval_func`]'s eager path. This is the bridge entry
/// `crate::scalar_function::ScalarFunction::eval` uses to run builtins over
/// chunk rows; `eval_func` calls it too, so there is exactly ONE
/// implementation of each function.
///
/// Deliberately OUTSIDE this entry (they stay AST/session-bound in
/// `eval_func`):
/// - lazy control forms: `IF` (Go's `builtinIf*Sig` evaluates exactly one
///   branch, so eager-evaluating both would change semantics, e.g. a guarded
///   `1/0`), `CASE`, and the `DATE_ADD`/`DATE_SUB`/`ADDDATE`/`SUBDATE`
///   family whose second argument is an `Expr::Interval`, not a value;
/// - session-state functions: `RAND` (needs
///   the argument AST and per-call `function_key` for generator identity),
///   the sequence functions (`NEXTVAL`/`LASTVAL`/`SETVAL`), and the
///   `time_fn` family (its dispatch takes `Columns` for the statement clock,
///   time zone, and `default_week_format`);
/// - the `LENGTH`/`OCTET_LENGTH`/`CHAR_LENGTH`/`CHARACTER_LENGTH` family: Go
///   selects the signature from the argument expression's FieldType via
///   `BuildContext::build_string_length_for_expr` BEFORE seeing any runtime
///   value, so it genuinely needs the argument AST, not just the value.
///
/// `COALESCE` is eager here exactly as in `eval_func`'s existing eager path
/// (Go's `builtinCoalesceSig` evaluates arguments in order over values, not
/// lazily over unevaluated branches — no guarded-error semantics to protect).
pub(crate) fn eval_func_values(
    name: &str,
    vals: &[Datum],
    ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    if let Some(result) = crate::math_fn::dispatch_values(name, vals, ctx) {
        return Some(result);
    }
    let result = match name {
        // Go's `in` builtin: args[0] is the tested value and the rest are the
        // list. Three-valued: a match is 1; no match with a NULL anywhere
        // (including the tested value) is NULL; otherwise 0.
        "IN" if vals.len() >= 2 => {
            let (value, list) = vals.split_first().expect("at least two arguments");
            let mut found_null = *value == Datum::Null;
            for item in list {
                match crate::ops::eval_binary_in(BinaryOp::Eq, value.clone(), item.clone(), ctx) {
                    Ok(Datum::Int(0)) => {}
                    Ok(Datum::Null) => found_null = true,
                    Ok(_) => return Some(Ok(Datum::Int(1))),
                    Err(e) => return Some(Err(e)),
                }
            }
            Ok(if found_null {
                Datum::Null
            } else {
                Datum::Int(0)
            })
        }
        // Go `builtinIntIsNullSig`: 1 when the argument is NULL, else 0 --
        // never NULL itself. `IS UNKNOWN` is the same function.
        "ISNULL" if vals.len() == 1 => Ok(Datum::Int(i64::from(vals[0] == Datum::Null))),
        // Go `builtinIntIsTrueSig` with keepNull false: NULL and zero are 0.
        "ISTRUE" if vals.len() == 1 => {
            truthy_of(&vals[0]).map(|t| Datum::Int(i64::from(t == Some(true))))
        }
        // Go `builtinIntIsFalseSig`: 1 only for a non-NULL zero.
        "ISFALSE" if vals.len() == 1 => {
            truthy_of(&vals[0]).map(|t| Datum::Int(i64::from(t == Some(false))))
        }
        // COALESCE returns the first non-NULL argument.
        "COALESCE" => Ok(vals
            .iter()
            .find(|v| **v != Datum::Null)
            .cloned()
            .unwrap_or(Datum::Null)),
        "IFNULL" if vals.len() == 2 => {
            let (a, b) = (vals[0].clone(), vals[1].clone());
            Ok(if a != Datum::Null { a } else { b })
        }
        // NULLIF(a, b): NULL when a and b are equal, else a — numeric
        // equality reuses `eval_binary`'s own Int/Decimal/Float promotion
        // (confirmed via goeval: a MIXED pair like `NULLIF(150, 1.5e2)`
        // is also NULL, not just same-type pairs), so no per-type-pair
        // matching is hand-rolled here; strings stay excluded (never
        // NULL), matching this function's pre-existing scope boundary.
        "NULLIF" if vals.len() == 2 => {
            let (a, b) = (vals[0].clone(), vals[1].clone());
            let numeric = |v: &Datum| {
                matches!(
                    v,
                    Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_)
                )
            };
            let equal = if numeric(&a) && numeric(&b) {
                match crate::ops::eval_binary_in(BinaryOp::Eq, a.clone(), b.clone(), ctx) {
                    Ok(v) => v == Datum::Int(1),
                    Err(e) => return Some(Err(e)),
                }
            } else {
                false
            };
            Ok(if equal { Datum::Null } else { a })
        }
        // ---- string functions ----
        "CONCAT" if !vals.is_empty() => concat(vals),
        "UPPER" | "UCASE" => case_convert(vals, true),
        "LOWER" | "LCASE" => case_convert(vals, false),
        "LEFT" if vals.len() == 2 => str_take(vals, true),
        "RIGHT" if vals.len() == 2 => str_take(vals, false),
        "SUBSTRING" | "SUBSTR" | "MID" if vals.len() == 3 => substring(vals),
        "REVERSE" => reverse(vals),
        // `ASCII`: the first BYTE's numeric value (0 for the empty string).
        "ASCII" => ascii(vals),
        "REPEAT" if vals.len() == 2 => repeat(vals, ctx),
        "REPLACE" if vals.len() == 3 => replace(vals),
        "SPACE" if vals.len() == 1 => space(vals, ctx),
        "STRCMP" if vals.len() == 2 => strcmp(vals),
        "LPAD" if vals.len() == 3 => pad(vals, true, ctx),
        "RPAD" if vals.len() == 3 => pad(vals, false, ctx),
        // `LOCATE(substr, str)` / `INSTR(str, substr)` — same 1-indexed
        // char position, arguments in the opposite order (reusing
        // `position`, which already handles the empty-substr and
        // not-found rules).
        "LOCATE" if vals.len() == 2 => {
            locate(&vals[0], &vals[1], locate_collation(&vals[0], &vals[1]))
        }
        "INSTR" if vals.len() == 2 => {
            locate(&vals[1], &vals[0], locate_collation(&vals[0], &vals[1]))
        }
        "HEX" if vals.len() == 1 => hex(vals),
        "UNHEX" if vals.len() == 1 => unhex(vals),
        "BIN" if vals.len() == 1 => bin(vals),
        "OCT" if vals.len() == 1 => oct(vals),
        "BIT_LENGTH" => bit_length(vals),
        "FIELD" if vals.len() >= 2 => field(vals, ctx),
        "ELT" if vals.len() >= 2 => elt(vals),
        "CONCAT_WS" if vals.len() >= 2 => concat_ws(vals),
        "SUBSTRING_INDEX" if vals.len() == 3 => substring_index(vals),
        // The parser renames `INSERT(...)` to `INSERT_FUNC` to avoid the
        // reserved statement keyword (the same desugar `CHAR`→`CHAR_FUNC`
        // uses).
        "INSERT_FUNC" if vals.len() == 4 => str_insert(vals),
        "MAKE_SET" if !vals.is_empty() => make_set(vals),
        "DATE_FORMAT" if vals.len() == 2 => date_format(&vals[0], &vals[1]),
        "ORD" if vals.len() == 1 => ord(vals),
        "QUOTE" if vals.len() == 1 => quote(vals),
        "BIT_COUNT" if vals.len() == 1 => bit_count(vals),
        "FORMAT" if vals.len() == 2 => format_num(vals, ctx),
        "CHAR_FUNC" if !vals.is_empty() => char_func(vals),
        "TO_BASE64" if vals.len() == 1 => to_base64(vals, ctx),
        // Go `builtinLoadFileSig.evalString` reads the argument and then
        // returns `"", true, nil` UNCONDITIONALLY: TiDB has no server-side
        // file access at all, so LOAD_FILE is NULL for every path, readable
        // or not. CAPTURED: `select load_file('/etc/hosts')` is NULL.
        "LOAD_FILE" if vals.len() == 1 => Ok(Datum::Null),
        "FROM_BASE64" if vals.len() == 1 => from_base64(vals),
        // ---- date-part extraction ----
        // A `DATE`/`DATETIME` value is a plain string to this evaluator (no
        // date value domain), so these parse the string's calendar
        // components directly; `NULL` if it doesn't coerce to a string or
        // doesn't parse as a valid date (calendar-validated: month 1-12, day
        // valid for that specific month/year including leap years).
        "YEAR" => date_part(vals, |d| d.0),
        // `HOUR`/`MINUTE`/`SECOND`: a GENUINELY different two-path
        // algorithm from the DATE-part functions above, depending on
        // whether the argument contains a `:` — see
        // `time_fn::calendar::parse_hms_extended`'s own doc for the full rule
        // (confirmed via `goeval`, not assumed): a colon-less value
        // (including a bare `DATE`, non-obviously) decodes its OWN
        // leading digit run as a right-aligned `HHMMSS` number, NOT a
        // calendar date at all.
        "HOUR" => time_part(vals, |t| i64::from(t.0)),
        "MINUTE" => time_part(vals, |t| i64::from(t.1)),
        "SECOND" => time_part(vals, |t| i64::from(t.2)),
        // `DATEDIFF`: the day count between two dates' DATE parts (any
        // time-of-day component is ignored, confirmed via `goeval` — e.g.
        // the same calendar day at 23:59:59 and 00:00:01 diffs to 0), via
        // an absolute day-numbering (`days_from_civil`) whose exact epoch
        // doesn't matter since only the difference is observable.
        // `TO_DAYS`/`TO_SECONDS`: zero-date calendar arithmetic owned by the
        // time-family module, including strict invalid-suffix handling.
        // `FROM_DAYS`: the reverse of `TO_DAYS` (see `time_fn::calendar::from_days`).
        "FROM_DAYS" => from_days(vals),
        "DATEDIFF" if vals.len() == 2 => date_diff(vals),
        // Family extension modules (`crate::builtin_ext`) — each family owns
        // one module with its own `dispatch(name, vals) -> Option<...>`, so
        // parallel agents can add builtins without touching this match.
        // `None` from every family means this entry doesn't know the name.
        _ => return crate::builtin_ext::dispatch(name, vals, ctx),
    };
    Some(result)
}

/// Ports the `EvalInt` coercion used by
/// `builtinLastInsertIDWithIDSig.evalInt` (`pkg/expression/builtin_info.go`).
/// This is intentionally local rather than a broad signed-to-unsigned helper:
/// the builtin publishes the resulting two's-complement bits as a `uint64`,
/// while casts and column assignment have different TiDB warning/range rules.
fn last_insert_id_arg(value: &Datum) -> Result<u64, EvalError> {
    let signed = match value {
        Datum::Int(value) => *value,
        Datum::UInt(value) => return Ok(*value),
        Datum::Decimal(value) => value.round_to_i64_saturating(),
        // Go's `types.Round` conversion used by `EvalInt` rounds fractional
        // numeric values away from zero; Rust's `round` has the same tie rule
        // and its `as i64` conversion saturates at the source's signed range.
        Datum::Real(value) => value.round() as i64,
        // `EvalInt`'s string path takes the leading signed integer run, not
        // the floating/exponent prefix: `'1.9tail'` is 1 and `'1e2tail'` is
        // 1, while an invalid string normalizes to 0 with a Go warning (this
        // seed has no warning result surface).
        Datum::String(value) => value.as_utf8().map(mysql_integer_prefix).unwrap_or(0),
        Datum::Bytes(value) => std::str::from_utf8(value)
            .map(mysql_integer_prefix)
            .unwrap_or(0),
        Datum::Null => unreachable!("NULL is handled before coercion"),
        Datum::MinNotNull | Datum::MaxValue => {
            return Err(EvalError::Unsupported(
                "range sentinel LAST_INSERT_ID argument",
            ));
        }
        other => {
            other
                .to_i64()
                .map_err(|_| EvalError::Unsupported("LAST_INSERT_ID conversion"))?
                .value
        }
    };
    Ok(signed as u64)
}

fn mysql_integer_prefix(value: &str) -> i64 {
    let value = value.trim_start();
    let (negative, digits) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let count = digits
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if count == 0 {
        return 0;
    }
    let magnitude = digits[..count].parse::<u64>().unwrap_or(u64::MAX);
    if negative {
        if magnitude >= (1_u64 << 63) {
            i64::MIN
        } else {
            -(magnitude as i64)
        }
    } else {
        i64::try_from(magnitude).unwrap_or(i64::MAX)
    }
}

/// Evaluates `expr [NOT] IN (list)` in MySQL three-valued logic: a match is
/// TRUE; no match with a NULL in the list (or a NULL left side) is NULL; no
/// match with no NULL is FALSE. `NOT IN` negates TRUE/FALSE but keeps NULL.
/// Element equality reuses `=`, so it honors the string collation.
///
/// A row-value operand (`(a, b) [NOT] IN (...)`, `expr` a bare
/// `Expr::Row`) is handled as its OWN case, checked FIRST: `eval_in`
/// itself has no arm for a standalone `Expr::Row` (see `crate::row`'s
/// own doc for why), so `expr`/each `list` item must be recognized as
/// row-shaped and compared via `crate::row::row_compare`'s own
/// `Eq`-mode element-wise logic BEFORE ever calling plain `eval_in` on
/// them — every `list` item is required to be a `Expr::Row` of the
/// SAME arity too (both a literal row-value list,
/// `(a,b) IN ((1,2),(3,4))`, and a resolved subquery's own captured
/// rows — see `tidb-exec`'s own `Database::in_subquery_rows` — always
/// produce `Expr::Row` list items here, never a mix).
///
/// EVERY list item is evaluated and compared, even after a match is
/// found: the boolean answer is settled by the first match, but the
/// evaluation of the remaining items is OBSERVABLE and so cannot be
/// skipped. Our string-versus-number coercion lives inside the
/// comparison (`crate::ops::eval_binary_in`), so skipping a comparison
/// skips its `1292 Truncated incorrect DOUBLE value` warning and any
/// error it would raise.
///
/// Go settles this the same way, in the vectorized `in` that real
/// execution uses (`pkg/expression/builtin_other_vec_generated.go`,
/// `builtinInRealSig.vecEvalInt`):
///
/// ```text
/// for j := 0; j < len(args); j++ {
///     if err := args[j].VecEvalReal(ctx, input, buf1); err != nil {
///         return err
///     }
///     ...
///     for i := 0; i < n; i++ {
///         if r64s[i] != 0 {
///             continue
///         }
/// ```
///
/// `args[j].VecEvalReal` -- which IS the coercion, because
/// `newBaseBuiltinFuncWithTp` wrapped every arg in `cast(... as double)`
/// at build time (`pkg/expression/builtin.go`, `WrapWithCastAsReal`) --
/// runs for every arg unconditionally; `if r64s[i] != 0 { continue }`
/// skips only the COMPARISON for an already-matched row, never the
/// evaluation. An error from a later arg is returned even for rows that
/// already matched. The scalar `builtinInRealSig.evalInt`
/// (`pkg/expression/builtin_other.go`) does `return 1, false, nil` from
/// inside its loop, but that path is the non-vectorized fallback; the
/// warning count a client observes comes from the vectorized one.
pub(crate) fn eval_in_list(
    expr: &Expr,
    list: &[Expr],
    not: bool,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    if let Expr::Row(left_items) = expr {
        let lv: Vec<Datum> = left_items
            .iter()
            .map(|e| eval_in(e, cols))
            .collect::<Result<_, _>>()?;
        let mut found_null = false;
        let mut found_match = false;
        for item in list {
            let Expr::Row(right_items) = item else {
                return Err(EvalError::Unsupported(
                    "row value IN list item arity mismatch",
                ));
            };
            let rv: Vec<Datum> = right_items
                .iter()
                .map(|e| eval_in(e, cols))
                .collect::<Result<_, _>>()?;
            match row_compare(BinaryOp::Eq, &lv, &rv)? {
                Datum::Int(0) => {}
                Datum::Null => found_null = true,
                _ => found_match = true,
            }
        }
        return Ok(in_result(found_match, found_null, not));
    }
    let v = eval_in(expr, cols)?;
    let mut found_null = false;
    let mut found_match = false;
    for item in list {
        let iv = eval_in(item, cols)?;
        match crate::ops::eval_binary_in(BinaryOp::Eq, v.clone(), iv, cols)? {
            Datum::Int(0) => {}
            Datum::Null => found_null = true,
            _ => found_match = true,
        }
    }
    Ok(in_result(found_match, found_null, not))
}

/// The three-valued answer of an `IN` whose whole list has been compared:
/// a match anywhere is TRUE and outranks a NULL, no match with a NULL is
/// NULL, no match with no NULL is FALSE. `NOT` negates TRUE/FALSE and
/// leaves NULL alone.
///
/// Match-outranks-NULL is what the short-circuiting form produced too --
/// it returned TRUE from inside the loop even when an earlier item had
/// already set `found_null` -- so folding the whole list changes WHICH
/// items get evaluated, never the boolean this returns.
fn in_result(found_match: bool, found_null: bool, not: bool) -> Datum {
    if found_match {
        bool_int(!not)
    } else if found_null {
        Datum::Null
    } else {
        bool_int(not) // no match, no NULL: FALSE for IN, TRUE for NOT IN
    }
}

/// Negates a three-valued boolean when `neg` is set; NULL stays NULL. Called
/// from `crate::eval_in`'s `BETWEEN` handling.
pub(crate) fn negate_if(v: Datum, neg: bool) -> Datum {
    match (neg, v) {
        (true, Datum::Int(i)) => bool_int(i == 0),
        (true, Datum::UInt(i)) => bool_int(i == 0),
        (_, v) => v,
    }
}
