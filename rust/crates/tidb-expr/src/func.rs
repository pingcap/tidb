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

use crate::coerce::{bool_int, coerce_str, truthy_of};
use crate::row::row_compare;
use crate::string_fn::{
    ascii, bin, bit_count, bit_length, case_convert, char_func, concat, concat_ws, elt, field,
    format_num, from_base64, hex, make_set, oct, ord, pad, position, quote, repeat, replace, space,
    str_insert, str_take, str_unary, strcmp, substring, substring_index, to_base64, unhex,
};
use crate::time_fn::calendar::{date_add, date_diff, date_format, date_part, from_days, time_part};
use crate::{eval_binary, eval_in};
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
            return built.eval(&eval_in(&args[0], cols)?);
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
        // Go's `wrapWithIsTrue` maps string/bytes conditions to the real
        // signature, whose `EvalReal` consumes a numeric prefix (`0.1` is
        // true, while `0.0` is false).  Other scalar values use their native
        // truthiness; NULL remains false because `keepNull` is disabled.
        let take_true = match condition {
            Datum::String(_) | Datum::Bytes(_) => {
                crate::ops::to_f64_with_mysql_string(&condition) != 0.0
            }
            _ => truthy_of(&condition)? == Some(true),
        };
        return eval_in(if take_true { when_true } else { when_false }, cols);
    }
    let vals: Vec<Datum> = args
        .iter()
        .map(|a| eval_in(a, cols))
        .collect::<Result<_, _>>()?;
    if let Some(result) = crate::math_fn::dispatch(name.as_str(), args, &vals, cols, function_key) {
        return result;
    }
    match name.as_str() {
        "ROW_COUNT" => match vals.as_slice() {
            [] => cols
                .row_count()
                .map(Datum::Int)
                .ok_or(EvalError::Unsupported("ROW_COUNT requires a session")),
            _ => Err(EvalError::Unsupported("bad function arity")),
        },
        // `LAST_INSERT_ID()` reads the value promoted from the preceding
        // statement. Its one-argument form instead coerces through Go's
        // `EvalInt`, records the raw uint64 bits for NEXT-statement
        // promotion, and returns the same UNSIGNED result immediately.
        // Keeping these forms together makes their same-statement separation
        // explicit: `LAST_INSERT_ID(5), LAST_INSERT_ID()` is `5, old`, not
        // `5, 5` (pkg/executor/select.go's statement-context promotion).
        "LAST_INSERT_ID" => match vals.as_slice() {
            [] => cols
                .last_insert_id()
                .map(Datum::UInt)
                .ok_or(EvalError::Unsupported("LAST_INSERT_ID requires a session")),
            [Datum::Null] => Ok(Datum::Null),
            [value] => {
                let id = last_insert_id_arg(value)?;
                cols.set_last_insert_id(id);
                Ok(Datum::UInt(id))
            }
            _ => Err(EvalError::Unsupported("bad function arity")),
        },
        // COALESCE returns the first non-NULL argument.
        "COALESCE" => Ok(vals
            .into_iter()
            .find(|v| *v != Datum::Null)
            .unwrap_or(Datum::Null)),
        "IFNULL" if vals.len() == 2 => {
            let mut it = vals.into_iter();
            let a = it.next().unwrap();
            let b = it.next().unwrap();
            Ok(if a != Datum::Null { a } else { b })
        }
        // NULLIF(a, b): NULL when a and b are equal, else a — numeric
        // equality reuses `eval_binary`'s own Int/Decimal/Float promotion
        // (confirmed via goeval: a MIXED pair like `NULLIF(150, 1.5e2)`
        // is also NULL, not just same-type pairs), so no per-type-pair
        // matching is hand-rolled here; strings stay excluded (never
        // NULL), matching this function's pre-existing scope boundary.
        "NULLIF" if vals.len() == 2 => {
            let mut it = vals.into_iter();
            let a = it.next().unwrap();
            let b = it.next().unwrap();
            let numeric = |v: &Datum| {
                matches!(
                    v,
                    Datum::Int(_) | Datum::UInt(_) | Datum::Decimal(_) | Datum::Real(_)
                )
            };
            let equal = numeric(&a)
                && numeric(&b)
                && eval_binary(BinaryOp::Eq, a.clone(), b.clone())? == Datum::Int(1);
            Ok(if equal { Datum::Null } else { a })
        }
        // ---- string functions ----
        "CONCAT" if !vals.is_empty() => concat(&vals),
        "UPPER" | "UCASE" => case_convert(&vals, true),
        "LOWER" | "LCASE" => case_convert(&vals, false),
        "LEFT" if vals.len() == 2 => str_take(&vals, true),
        "RIGHT" if vals.len() == 2 => str_take(&vals, false),
        "SUBSTRING" | "SUBSTR" | "MID" if vals.len() == 3 => substring(&vals),
        "REVERSE" => str_unary(&vals, |s| {
            Datum::new_string(s.chars().rev().collect::<String>())
        }),
        // `ASCII`: the first BYTE's numeric value (0 for the empty string).
        "ASCII" => ascii(&vals),
        "REPEAT" if vals.len() == 2 => repeat(&vals),
        "REPLACE" if vals.len() == 3 => replace(&vals),
        "SPACE" if vals.len() == 1 => space(&vals),
        "STRCMP" if vals.len() == 2 => strcmp(&vals),
        "LPAD" if vals.len() == 3 => pad(&vals, true),
        "RPAD" if vals.len() == 3 => pad(&vals, false),
        // `LOCATE(substr, str)` / `INSTR(str, substr)` — same 1-indexed
        // char position, arguments in the opposite order (reusing
        // `position`, which already handles the empty-substr and
        // not-found rules).
        "LOCATE" if vals.len() == 2 => Ok(position(coerce_str(&vals[0])?, coerce_str(&vals[1])?)),
        "INSTR" if vals.len() == 2 => Ok(position(coerce_str(&vals[1])?, coerce_str(&vals[0])?)),
        "HEX" if vals.len() == 1 => hex(&vals),
        "UNHEX" if vals.len() == 1 => unhex(&vals),
        "BIN" if vals.len() == 1 => bin(&vals),
        "OCT" if vals.len() == 1 => oct(&vals),
        "BIT_LENGTH" => bit_length(&vals),
        "FIELD" if vals.len() >= 2 => field(&vals),
        "ELT" if vals.len() >= 2 => elt(&vals),
        "CONCAT_WS" if vals.len() >= 2 => concat_ws(&vals),
        "SUBSTRING_INDEX" if vals.len() == 3 => substring_index(&vals),
        // The parser renames `INSERT(...)` to `INSERT_FUNC` to avoid the
        // reserved statement keyword (the same desugar `CHAR`→`CHAR_FUNC`
        // uses).
        "INSERT_FUNC" if vals.len() == 4 => str_insert(&vals),
        "MAKE_SET" if !vals.is_empty() => make_set(&vals),
        "DATE_FORMAT" if vals.len() == 2 => date_format(&vals[0], &vals[1]),
        "ORD" if vals.len() == 1 => ord(&vals),
        "QUOTE" if vals.len() == 1 => quote(&vals),
        "BIT_COUNT" if vals.len() == 1 => bit_count(&vals),
        "FORMAT" if vals.len() == 2 => format_num(&vals),
        "CHAR_FUNC" if !vals.is_empty() => char_func(&vals),
        "TO_BASE64" if vals.len() == 1 => to_base64(&vals),
        "FROM_BASE64" if vals.len() == 1 => from_base64(&vals),
        // ---- date-part extraction ----
        // A `DATE`/`DATETIME` value is a plain string to this evaluator (no
        // date value domain), so these parse the string's calendar
        // components directly; `NULL` if it doesn't coerce to a string or
        // doesn't parse as a valid date (calendar-validated: month 1-12, day
        // valid for that specific month/year including leap years).
        "YEAR" => date_part(&vals, |d| d.0),
        // `HOUR`/`MINUTE`/`SECOND`: a GENUINELY different two-path
        // algorithm from the DATE-part functions above, depending on
        // whether the argument contains a `:` — see
        // `time_fn::calendar::parse_hms_extended`'s own doc for the full rule
        // (confirmed via `goeval`, not assumed): a colon-less value
        // (including a bare `DATE`, non-obviously) decodes its OWN
        // leading digit run as a right-aligned `HHMMSS` number, NOT a
        // calendar date at all.
        "HOUR" => time_part(&vals, |t| i64::from(t.0)),
        "MINUTE" => time_part(&vals, |t| i64::from(t.1)),
        "SECOND" => time_part(&vals, |t| i64::from(t.2)),
        // `DATEDIFF`: the day count between two dates' DATE parts (any
        // time-of-day component is ignored, confirmed via `goeval` — e.g.
        // the same calendar day at 23:59:59 and 00:00:01 diffs to 0), via
        // an absolute day-numbering (`days_from_civil`) whose exact epoch
        // doesn't matter since only the difference is observable.
        // `TO_DAYS`/`TO_SECONDS`: zero-date calendar arithmetic owned by the
        // time-family module, including strict invalid-suffix handling.
        // `FROM_DAYS`: the reverse of `TO_DAYS` (see `time_fn::calendar::from_days`).
        "FROM_DAYS" => from_days(&vals),
        "DATEDIFF" if vals.len() == 2 => date_diff(&vals),
        // Family extension modules (`crate::builtin_ext`) — each family owns
        // one module with its own `dispatch(name, vals) -> Option<...>`, so
        // parallel agents can add builtins without touching this match.
        // `None` from every family falls through to the honest error.
        _ => crate::time_fn::dispatch(name.as_str(), &vals, cols)
            .or_else(|| crate::builtin_ext::dispatch(name.as_str(), &vals))
            .unwrap_or(Err(EvalError::Unsupported("unsupported function"))),
    }
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
                _ => return Ok(bool_int(!not)), // a match
            }
        }
        return Ok(if found_null {
            Datum::Null
        } else {
            bool_int(not)
        });
    }
    let v = eval_in(expr, cols)?;
    let mut found_null = false;
    for item in list {
        let iv = eval_in(item, cols)?;
        match eval_binary(BinaryOp::Eq, v.clone(), iv)? {
            Datum::Int(0) => {}
            Datum::Null => found_null = true,
            _ => return Ok(bool_int(!not)), // a match
        }
    }
    Ok(if found_null {
        Datum::Null
    } else {
        bool_int(not) // no match, no NULL: FALSE for IN, TRUE for NOT IN
    })
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
