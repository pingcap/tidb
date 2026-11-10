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

use tidb_ast::BinaryOp;

use crate::coerce::truthy_of;
use crate::string_fn::{
    ascii, bin, bit_count, bit_length, case_convert, char_func_with_context, concat_with_context,
    concat_ws_with_context, elt, export_set, field, format_num, from_base64,
    from_base64_with_packet_limit, hex, locate, locate_collation, locate_with_position, make_set,
    oct, ord, quote, replace, reverse, str_take, strcmp, substring, substring_index,
    unhex,
};
use crate::string_packet::{pad, repeat, space, to_base64};
use crate::time_fn::calendar::{date_diff, date_part, from_days, time_part};
use crate::{Columns, Datum, EvalError};

/// Go `builtinBenchmarkSig.evalInt`'s first-argument `EvalInt` boundary.
pub(crate) fn benchmark_loop_count(
    value: Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn Columns,
) -> Result<Option<i64>, EvalError> {
    match crate::cast::cast_arg_as_int(&value, source, ctx)? {
        Datum::Null => Ok(None),
        Datum::Int(value) => Ok(Some(value)),
        Datum::UInt(value) => Ok(Some(value as i64)),
        _ => Err(EvalError::Unsupported("BENCHMARK loop count")),
    }
}

/// Go's `builtinBenchmarkSig` switch intentionally has no vector arm.
pub(crate) fn ensure_benchmark_eval_type(
    source: Option<&tidb_datatype::FieldType>,
) -> Result<(), EvalError> {
    if source
        .is_some_and(|field_type| field_type.eval_type() == tidb_datatype::EvalType::VectorFloat32)
    {
        return Err(EvalError::Unsupported(
            "VectorFloat32 is not supported for BENCHMARK()",
        ));
    }
    Ok(())
}

/// The builtins whose result is a function of their argument values AND the
/// session: `FOUND_ROWS()`, `ROW_COUNT()` and both forms of
/// `LAST_INSERT_ID`. `None` if
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
        ("FOUND_ROWS", []) => cols
            .found_rows()
            .map(Datum::UInt)
            .ok_or(EvalError::Unsupported("FOUND_ROWS requires a session")),
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
        // `CURRENT_RESOURCE_GROUP()` is a zero-argument session builtin just
        // like the other information functions.  Keep it in the shared
        // session-state table.
        ("CURRENT_RESOURCE_GROUP", []) => Ok(match cols.current_resource_group() {
            Some(group) => Datum::new_string_datum(group.into_bytes()),
            None => Datum::Null,
        }),
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

pub(crate) fn sequence_path(value: &Datum) -> Result<Vec<String>, EvalError> {
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
    // Go's `builtinFromBase64Sig` checks the estimated decoded length against
    // `max_allowed_packet` before decoding and routes an over-limit result
    // through the statement warning policy. Keep this context-sensitive arm
    // ahead of the values-only table so AST and chunk evaluation agree.
    if name == "FROM_BASE64" {
        return Some(from_base64_with_packet_limit(vals, Some(cols)));
    }

    // The session-state builtins: pure functions of their argument VALUES
    // plus the session, which `cols` supplies.
    if let Some(result) = eval_session_state(name, vals, cols) {
        return Some(result);
    }
    let result = eval_func_values(name, vals, cols)?;
    if name == "JSON_MERGE" && matches!(result, Ok(ref value) if *value != Datum::Null) {
        cols.append_go_warning(
            &crate::expression_errors::ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT,
            "JSON_MERGE is deprecated and will be removed in a future release.",
        );
    }
    Some(result)
}

/// Evaluates a builtin whose result is a pure function of its
/// already-evaluated argument values. This is the bridge entry
/// `crate::scalar_function::ScalarFunction::eval` uses to run the builtins
/// that have no signature object of their own yet.
pub(crate) fn eval_func_values(
    name: &str,
    vals: &[Datum],
    ctx: &dyn Columns,
) -> Option<Result<Datum, EvalError>> {
    // `json_memberof` is a registered builtin, so the registered-but-
    // unimplemented fallback below would swallow it before the JSON family's
    // own dispatch ever sees the name. Route it through that dispatch here,
    // where the row evaluator lives.
    if name.eq_ignore_ascii_case("json_memberof") {
        return Some(
            crate::builtin_ext::json::dispatch("JSON_MEMBER_OF", vals)
                .unwrap_or_else(|| Err(EvalError::Unsupported("JSON_MEMBER_OF arity"))),
        );
    }
    if let Some(result) = crate::math_fn::dispatch_values(name, vals, ctx) {
        return Some(result);
    }
    let result = match name {
        // Go `builtinGetParamStringSig.evalString` reads the integer selector
        // from the plan-cache parameter list and stringifies the selected
        // datum. An unset/out-of-range selector returns the exact
        // `ErrParamIndexExceedParamCounts` identity; a datum that cannot be
        // rendered by `ToString` becomes NULL without another error.
        "GETPARAM" if vals.len() == 1 => {
            let index = match vals[0] {
                Datum::Null => return Some(Ok(Datum::Null)),
                Datum::Int(index) => {
                    usize::try_from(index).map_err(|_| EvalError::ParamIndexExceedParamCounts)
                }
                Datum::UInt(index) => {
                    usize::try_from(index).map_err(|_| EvalError::ParamIndexExceedParamCounts)
                }
                _ => Err(EvalError::Unsupported("GETPARAM index is not an integer")),
            };
            let index = match index {
                Ok(index) => index,
                Err(error) => return Some(Err(error)),
            };
            let value = match ctx.get_param_value(index) {
                Ok(value) => value,
                Err(error) => return Some(Err(error)),
            };
            return Some(Ok(match value.sql_string() {
                Ok(text) => Datum::new_string_datum(text.into_bytes()),
                Err(_) => Datum::Null,
            }));
        }
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
        // Go's filter wrapper uses `builtinIntIsTrueSig{keepNull:true}` for
        // predicates whose NULL result must survive a NOT/OR rewrite.  This
        // is the value-preserving sibling of ISTRUE: NULL stays NULL, while
        // every other datum follows the same Datum.ToBool truthiness rule.
        "ISTRUE_WITH_NULL" if vals.len() == 1 => truthy_of(&vals[0])
            .map(|t| t.map_or(Datum::Null, |truthy| Datum::Int(i64::from(truthy)))),
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
        // NULLIF(a, b): NULL when a and b are equal, else a. Go has no
        // builtin for this at all -- the expression rewriter turns
        // `NULLIF(a, b)` into `IF(a = b, NULL, a)`
        // (`expression_rewriter.go`'s `ast.NullIf` arm) -- so the equality
        // is the ORDINARY comparison with its full type derivation:
        // Int/Decimal/Float promotion (confirmed via goeval: the MIXED pair
        // `NULLIF(150, 1.5e2)` is NULL), string collation, and the JSON
        // domain (`nullif(json_remove(..), cast('{}' as json))` is the
        // collapse `ALTER USER ... DISCARD OLD PASSWORD` writes; comparing
        // only numeric pairs left it always non-NULL). A NULL condition --
        // either operand NULL -- makes IF take the else branch and answer
        // `a`, which `eval_binary_in`'s NULL propagation reproduces.
        "NULLIF" if vals.len() == 2 => {
            let (a, b) = (vals[0].clone(), vals[1].clone());
            let equal = match crate::ops::eval_binary_in(BinaryOp::Eq, a.clone(), b, ctx) {
                Ok(v) => v == Datum::Int(1),
                Err(e) => return Some(Err(e)),
            };
            Ok(if equal { Datum::Null } else { a })
        }
        // ---- string functions ----
        "CONCAT" if !vals.is_empty() => concat_with_context(vals, ctx),
        "UPPER" | "UCASE" => case_convert(vals, true),
        "LOWER" | "LCASE" => case_convert(vals, false),
        "LEFT" if vals.len() == 2 => str_take(vals, true),
        "RIGHT" if vals.len() == 2 => str_take(vals, false),
        "SUBSTRING" | "SUBSTR" | "MID" if vals.len() == 3 => substring(vals, ctx),
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
        // `POSITION(substr IN str)` is Go's two-argument `locateFunctionClass`.
        "LOCATE" | "POSITION" if vals.len() == 2 => {
            locate(&vals[0], &vals[1], locate_collation(&vals[0], &vals[1]))
        }
        "LOCATE" if vals.len() == 3 => {
            locate_with_position(vals, locate_collation(&vals[0], &vals[1]))
        }
        "INSTR" if vals.len() == 2 => {
            locate(&vals[1], &vals[0], locate_collation(&vals[0], &vals[1]))
        }
        "HEX" if vals.len() == 1 => hex(vals),
        "UNHEX" if vals.len() == 1 => unhex(vals),
        "BIN" if vals.len() == 1 => bin(vals, ctx),
        "OCT" if vals.len() == 1 => oct(vals),
        "BIT_LENGTH" => bit_length(vals),
        "FIELD" if vals.len() >= 2 => field(vals, ctx),
        "ELT" if vals.len() >= 2 => elt(vals),
        "EXPORT_SET" => export_set(vals),
        "CONCAT_WS" if vals.len() >= 2 => concat_ws_with_context(vals, ctx),
        "SUBSTRING_INDEX" if vals.len() == 3 => substring_index(vals),
        // The parser renames `INSERT(...)` to `INSERT_FUNC` to avoid the
        // reserved statement keyword (the same desugar `CHAR`→`CHAR_FUNC`
        // uses).
        "INSERT_FUNC" if vals.len() == 4 => crate::string_fn::insert_with_context(vals, ctx),
        "MAKE_SET" if !vals.is_empty() => make_set(vals),
        "DATE_FORMAT" if vals.len() == 2 => {
            crate::time_fn::calendar::date_format_in(&vals[0], &vals[1], Some(ctx))
        }
        "ORD" if vals.len() == 1 => ord(vals),
        "QUOTE" if vals.len() == 1 => quote(vals),
        "BIT_COUNT" if vals.len() == 1 => bit_count(vals, ctx),
        "FORMAT" if vals.len() == 2 => format_num(vals, ctx),
        "CHAR_FUNC" if !vals.is_empty() => char_func_with_context(vals, ctx),
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;

    

    #[derive(Default)]
    struct PacketLimit {
        warnings: RefCell<Vec<(u16, String)>>,
    }

    impl Columns for PacketLimit {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn max_allowed_packet(&self) -> u64 {
            3
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.warnings.borrow_mut().push((code, message.to_owned()));
        }
    }

    /// Go `TestInsertBinarySig`: all seven source rows, including the one
    /// result that crosses the signature's three-byte packet limit.
    #[test]
    fn test_insert_binary_sig() {
        let ctx = PacketLimit::default();
        let binary = |value: &str| Datum::new_bytes_datum(value.as_bytes().to_vec());
        let rows = [
            (
                [binary("abc"), Datum::Int(3), Datum::Int(-1), binary("d")],
                binary("abd"),
            ),
            (
                [binary("abc"), Datum::Int(3), Datum::Int(-1), binary("de")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Int(0), Datum::Int(-1), binary("d")],
                binary("abc"),
            ),
            (
                [Datum::Null, Datum::Int(3), Datum::Int(-1), binary("d")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Null, Datum::Int(-1), binary("d")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Int(3), Datum::Null, binary("d")],
                Datum::Null,
            ),
            (
                [binary("abc"), Datum::Int(3), Datum::Int(-1), Datum::Null],
                Datum::Null,
            ),
        ];

        for (args, expected) in rows {
            assert_eq!(
                eval_func_values("INSERT_FUNC", &args, &ctx)
                    .expect("INSERT must be dispatched")
                    .expect("source rows must evaluate"),
                expected
            );
        }

        assert_eq!(
            *ctx.warnings.borrow(),
            vec![(
                1301,
                "Result of insert() was larger than max_allowed_packet (3) - truncated".to_owned(),
            )]
        );
    }
}

