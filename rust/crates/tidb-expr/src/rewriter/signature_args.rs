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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The implicit casts Go's builtin classes put around their arguments.
//!
//! Each `*FunctionClass.getFunction` in `pkg/expression/builtin_*.go` hands
//! `newBaseBuiltinFuncWithTp` a per-argument eval-type list, and that
//! constructor wraps every argument that is not already of its type with
//! `WrapWithCastAsInt/Real/Decimal/String/Time/Duration/JSON`. The wrapper is
//! visible in a plan: `length(i)` over an INT column explains as
//! `length(cast(t.i, var_string(20)))`, `1 member of (j)` as
//! `json_memberof(cast(1, json BINARY), t.j)`.
//!
//! Go derives the function's collation and result type from the ORIGINAL
//! arguments first, so this wrap runs after both. A JSON function then clears
//! `ParseToJSONFlag` on the cast of every VALUE argument
//! (`DisableParseJSONFlag4Expr`), so a SQL string stays a JSON string
//! instead of being parsed as a document.

use tidb_datatype::{EvalType, FieldType, FieldTypeCode};

use crate::aggregation::wrap_cast::{
    wrap_with_cast_as_decimal, wrap_with_cast_as_duration, wrap_with_cast_as_int,
    wrap_with_cast_as_json, wrap_with_cast_as_real, wrap_with_cast_as_string,
    wrap_with_cast_as_time,
};
use crate::expr_util::predicates::disable_parse_json_flag_4_expr;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use crate::EvalError;
use tidb_ast::CiString;

use super::{derive_tree_collation_with_connection, is_newly_built_cast};

/// One entry of a Go class's `argTps`.
#[derive(Clone, Copy)]
enum ArgKind {
    Int,
    Real,
    Decimal,
    /// `ETString`.
    Text,
    Datetime,
    Timestamp,
    Duration,
    /// `ETJson`; a string source is parsed as a JSON document.
    Document,
    /// `ETJson` followed by `DisableParseJSONFlag4Expr`.
    Value,
    /// The class passes the argument's own type (`argTp` copied from it).
    Skip,
}

fn eval_type(arg: &Expression) -> Option<EvalType> {
    arg.static_type().map(FieldType::eval_type)
}

/// `abs`, `round`: the integer and decimal families keep their type, every
/// other source is read as a double.
fn numeric_family(arg: &Expression) -> ArgKind {
    match eval_type(arg) {
        Some(EvalType::Int) => ArgKind::Int,
        Some(EvalType::Decimal) => ArgKind::Decimal,
        _ => ArgKind::Real,
    }
}

/// The `argTps` list of Go's class for `name` called with `args`; `None` for
/// a function that does not wrap its arguments this way (or that this table
/// does not cover yet).
fn argument_kinds(
    name: &str,
    args: &[Expression],
    ret_type: Option<&FieldType>,
) -> Option<Vec<ArgKind>> {
    use ArgKind::{Datetime, Decimal, Document, Duration, Int, Real, Skip, Text, Timestamp, Value};
    let count = args.len();
    let first = args.first();
    let document_then_text = || {
        let mut kinds = vec![Document];
        kinds.extend(std::iter::repeat_n(Text, count.saturating_sub(1)));
        kinds
    };
    // `json_doc, (path, value)...`: the loop `for i := 1; i < len(args)-1; i += 2`.
    let document_then_pairs = || {
        let mut kinds = vec![Document];
        let mut index = 1;
        while index + 1 < count {
            kinds.push(Text);
            kinds.push(Value);
            index += 2;
        }
        kinds
    };
    Some(match name {
        "ascii"
        | "bit_length"
        | "char_length"
        | "character_length"
        | "compress"
        | "crc32"
        | "from_base64"
        | "inet6_aton"
        | "inet6_ntoa"
        | "inet_aton"
        | "is_free_lock"
        | "is_ipv4"
        | "is_ipv4_compat"
        | "is_ipv4_mapped"
        | "is_ipv6"
        | "is_used_lock"
        | "is_uuid"
        | "lcase"
        | "length"
        | "load_file"
        | "lower"
        | "ltrim"
        | "md5"
        | "octet_length"
        | "ord"
        | "password"
        | "quote"
        | "release_lock"
        | "reverse"
        | "rtrim"
        | "sha"
        | "sha1"
        | "sm3"
        | "time"
        | "to_base64"
        | "ucase"
        | "uncompress"
        | "uncompressed_length"
        | "unhex"
        | "upper"
        | "uuid_timestamp"
        | "uuid_version"
        | "validate_password_strength" => vec![Text],
        "acos" | "asin" | "cos" | "cot" | "degrees" | "exp" | "format_bytes"
        | "format_nano_time" | "log10" | "log2" | "radians" | "sec_to_time" | "sign" | "sin"
        | "sleep" | "sqrt" | "tan" => vec![Real],
        "date" | "day" | "dayname" | "dayofmonth" | "dayofweek" | "dayofyear" | "last_day"
        | "month" | "monthname" | "quarter" | "to_days" | "to_seconds" | "weekday"
        | "weekofyear" | "year" => vec![Datetime],
        "bin"
        | "bit_count"
        | "from_days"
        | "inet_ntoa"
        | "random_bytes"
        | "space"
        | "tidb_parse_tso_logical"
        | "tidb_shard"
        | "vitess_hash" => vec![Int],
        "decode" | "encode" | "find_in_set" | "instr" | "strcmp" => vec![Text, Text],
        "get_lock" | "left" | "repeat" | "right" | "sha2" => vec![Text, Int],
        "hour" | "microsecond" | "minute" | "second" | "time_to_sec" => vec![Duration],
        "makedate" | "period_add" | "period_diff" => vec![Int, Int],
        "replace" | "translate" => vec![Text, Text, Text],
        "conv" => vec![Text, Int, Int],
        "convert_tz" => vec![Datetime, Text, Text],
        "datediff" => vec![Datetime, Datetime],
        "maketime" => vec![Int, Int, Real],
        "substring_index" => vec![Text, Text, Int],
        "pow" | "power" | "atan" | "atan2" | "ln" | "log" => vec![Real; count],
        "date_format" => vec![Datetime, Text],
        "time_format" => vec![Duration, Text],
        "insert_func" => vec![Text, Int, Int, Text],
        "lpad" | "rpad" => vec![Text, Int, Text],
        "abs" => vec![numeric_family(first?)],
        "ceil" | "ceiling" | "floor" => vec![match eval_type(first?) {
            Some(EvalType::Int) => Int,
            Some(EvalType::Decimal) => Decimal,
            _ => Real,
        }],
        "round" => {
            let mut kinds = vec![numeric_family(first?)];
            if count > 1 {
                kinds.push(Int);
            }
            kinds
        }
        "truncate" => {
            let family = match eval_type(first?) {
                Some(
                    EvalType::String
                    | EvalType::Json
                    | EvalType::Datetime
                    | EvalType::Timestamp
                    | EvalType::Duration,
                ) => Real,
                Some(EvalType::Int) => Int,
                Some(EvalType::Real) => Real,
                Some(EvalType::Decimal) => Decimal,
                _ => return None,
            };
            vec![family, Int]
        }
        "rand" => vec![Int; count],
        "concat" | "concat_ws" => vec![Text; count],
        "char_func" => {
            let mut kinds = vec![Int; count.saturating_sub(1)];
            kinds.push(Text);
            kinds
        }
        "elt" | "make_set" => {
            let mut kinds = vec![Int];
            kinds.extend(std::iter::repeat_n(Text, count.saturating_sub(1)));
            kinds
        }
        "export_set" => {
            let mut kinds = vec![Int, Text, Text];
            if count > 3 {
                kinds.push(Text);
            }
            if count > 4 {
                kinds.push(Int);
            }
            kinds
        }
        "field" => {
            let all = |wanted: EvalType| args.iter().all(|arg| eval_type(arg) == Some(wanted));
            let kind = if all(EvalType::String) {
                Text
            } else if all(EvalType::Int) {
                Int
            } else {
                Real
            };
            vec![kind; count]
        }
        "format" => {
            let mut kinds = vec![
                match eval_type(first?) {
                    Some(EvalType::Decimal | EvalType::Int) => Decimal,
                    _ => Real,
                },
                Int,
            ];
            if count == 3 {
                kinds.push(Text);
            }
            kinds
        }
        "hex" => vec![match eval_type(first?) {
            Some(
                EvalType::String
                | EvalType::Datetime
                | EvalType::Timestamp
                | EvalType::Duration
                | EvalType::Json,
            ) => Text,
            Some(EvalType::Int | EvalType::Real | EvalType::Decimal) => Int,
            _ => return None,
        }],
        "locate" | "position" => {
            let mut kinds = vec![Text, Text];
            if count == 3 {
                kinds.push(Int);
            }
            kinds
        }
        "substring" | "substr" | "mid" => {
            let mut kinds = vec![Text, Int];
            if count == 3 {
                kinds.push(Int);
            }
            kinds
        }
        "oct" => vec![if eval_type(first?) == Some(EvalType::Int) {
            Int
        } else {
            Text
        }],
        "trim" => match count {
            1 => vec![Text],
            2 => vec![Text, Text],
            3 => vec![Text, Text, Int],
            _ => return None,
        },
        "week" | "yearweek" => {
            let mut kinds = vec![Datetime];
            if count == 2 {
                kinds.push(Int);
            }
            kinds
        }
        "timestamp" => vec![Text; count],
        "unix_timestamp" if count == 1 => vec![Datetime],
        "from_unixtime" => {
            let mut kinds = vec![Decimal];
            if count == 2 {
                kinds.push(Text);
            }
            kinds
        }
        "tidb_parse_tso" => vec![Int],
        "interval" => {
            let all_int = args.iter().all(|arg| eval_type(arg) == Some(EvalType::Int));
            vec![if all_int { Int } else { Real }; count]
        }
        "timediff" => args
            .iter()
            .map(|arg| match eval_type(arg) {
                Some(EvalType::Duration) => Duration,
                Some(EvalType::Datetime) => Datetime,
                Some(EvalType::Timestamp) => Timestamp,
                _ => Text,
            })
            .collect(),
        "benchmark" => vec![Int, Skip],
        "bitand" | "bitor" | "bitxor" | "leftshift" | "rightshift" => vec![Int, Int],
        "bitneg" => vec![Int],
        // Go `arithmeticIntDivideFunctionClass`: two integers divide as
        // integers, everything else is divided as decimals.
        "intdiv" => {
            let all_int = args.iter().all(|arg| eval_type(arg) == Some(EvalType::Int));
            vec![if all_int { Int } else { Decimal }; count]
        }
        // Go `unaryNotFunctionClass`: time types read as integers, strings
        // as doubles.
        "not" => vec![match eval_type(first?) {
            Some(EvalType::Timestamp | EvalType::Datetime | EvalType::Duration) => Int,
            Some(EvalType::String) => Real,
            Some(EvalType::Decimal) => Decimal,
            Some(EvalType::Real) => Real,
            Some(EvalType::Json) => Document,
            _ => Int,
        }],
        // Go `isTrueOrFalseFunctionClass`: string, time and JSON arguments
        // read as doubles.
        "istrue" | "istrue_with_null" | "isfalse" | "isfalse_with_null" => {
            vec![match eval_type(first?) {
                Some(
                    EvalType::Timestamp
                    | EvalType::Datetime
                    | EvalType::Duration
                    | EvalType::Json
                    | EvalType::String,
                ) => Real,
                Some(EvalType::Decimal) => Decimal,
                Some(EvalType::Real) => Real,
                _ => Int,
            }]
        }
        // Go `unaryMinusFunctionClass`: integers stay integers, decimal and
        // temporal arguments are negated as decimals, the rest as doubles.
        "unaryminus" => {
            let arg = first?;
            let temporal = arg.static_type().is_some_and(|tp| {
                tp.code().is_type_temporal() || tp.code() == FieldTypeCode::Duration
            });
            vec![match eval_type(arg) {
                Some(EvalType::Int) => Int,
                Some(EvalType::Real) => Real,
                Some(EvalType::Decimal) => Decimal,
                _ if temporal => Decimal,
                _ => Real,
            }]
        }
        // Go `greatestFunctionClass`/`leastFunctionClass`: every argument is
        // cast to the merged result type, or to a string when temporal and
        // non-temporal arguments mix and the comparison parses them back as
        // dates (`resolveType4Extremum`'s `cmpStringMode`).
        "greatest" | "least" => {
            let ret = ret_type?;
            let result_is_temporal =
                ret.code().is_type_temporal() || ret.code() == FieldTypeCode::Duration;
            let mut temporal_item: Option<FieldTypeCode> = None;
            for arg in args {
                let Some(code) = arg.static_type().map(FieldType::code) else {
                    continue;
                };
                if (code.is_type_temporal() || code == FieldTypeCode::Duration)
                    && (temporal_item.is_none() || code == FieldTypeCode::Datetime)
                {
                    temporal_item = Some(code);
                }
            }
            let compares_as_time = ret.eval_type().is_string_kind()
                && !result_is_temporal
                && temporal_item.is_some_and(|code| {
                    matches!(
                        code,
                        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
                    )
                });
            // Go casts every argument to a string and compares them as parsed
            // dates; the evaluator here still reads each argument's own
            // temporal type to pick the DATE or DATETIME mode, so those
            // arguments stay uncast until it reads the mode from the node.
            if compares_as_time {
                return None;
            }
            let kind = match ret.eval_type() {
                EvalType::Int => Int,
                EvalType::Real => Real,
                EvalType::Decimal => Decimal,
                EvalType::String | EvalType::Json => Text,
                EvalType::Datetime => Datetime,
                EvalType::Timestamp => Timestamp,
                EvalType::Duration => Duration,
                _ => return None,
            };
            vec![kind; count]
        }
        "uuid_to_bin" | "bin_to_uuid" => {
            let mut kinds = vec![Text];
            if count == 2 {
                kinds.push(Int);
            }
            kinds
        }
        "aes_encrypt" | "aes_decrypt" => vec![Text; count],
        "str_to_date" => vec![Text, Text],
        "regexp_like" => {
            let mut kinds = vec![Text, Text];
            if count == 3 {
                kinds.push(Text);
            }
            kinds
        }
        "regexp_substr" => {
            let mut kinds = vec![Text, Text];
            match count {
                3 => kinds.push(Int),
                4 => kinds.extend([Int, Int]),
                5 => kinds.extend([Int, Int, Text]),
                _ => {}
            }
            kinds
        }
        "regexp_instr" => {
            let mut kinds = vec![Text, Text];
            match count {
                3 => kinds.push(Int),
                4 => kinds.extend([Int, Int]),
                5 => kinds.extend([Int, Int, Int]),
                6 => kinds.extend([Int, Int, Int, Text]),
                _ => {}
            }
            kinds
        }
        "regexp_replace" => {
            let mut kinds = vec![Text, Text];
            match count {
                3 => kinds.push(Text),
                4 => kinds.extend([Text, Int]),
                5 => kinds.extend([Text, Int, Int]),
                6 => kinds.extend([Text, Int, Int, Text]),
                _ => {}
            }
            kinds
        }
        // The JSON family (`builtin_json.go`).
        "json_type" | "json_depth" | "json_storage_free" | "json_storage_size" | "json_pretty" => {
            vec![Document]
        }
        "json_extract" | "json_remove" | "json_keys" | "json_length" | "json_contains_path"
        | "json_search" => document_then_text(),
        "json_set" | "json_insert" | "json_replace" | "json_array_append" | "json_array_insert" => {
            document_then_pairs()
        }
        "json_merge" | "json_merge_preserve" | "json_merge_patch" => vec![Document; count],
        "json_array" => vec![Value; count],
        "json_object" => (0..count)
            .map(|index| if index % 2 == 0 { Text } else { Value })
            .collect(),
        "json_memberof" => vec![Value, Document],
        "json_contains" => {
            let mut kinds = vec![Document, Document];
            kinds.extend(std::iter::repeat_n(Text, count.saturating_sub(2)));
            kinds
        }
        "json_overlaps" | "json_schema_valid" => vec![Document, Document],
        "json_unquote" | "json_quote" => vec![Text],
        // Go `isNullFunctionClass.getFunction`: a JSON argument is tested as
        // its string form (`argTp = types.ETString`).
        "isnull" if eval_type(first?) == Some(EvalType::Json) => vec![Text],
        _ => return None,
    })
}

/// Builds the call node for builtin `name` the way Go's `getFunction` does:
/// the function's collation is derived from the ORIGINAL arguments
/// (`newBaseBuiltinFuncWithTp` calls `deriveCollation` before it casts), then
/// the arguments are wrapped. Deriving afterwards would read the casts'
/// connection collation instead (`reverse(json)` is `utf8mb4_bin`).
pub(super) fn build_call(
    name: &str,
    ret_type: FieldType,
    args: Vec<Expression>,
    connection: (&str, &str),
    fold: &dyn Fn(&mut Expression),
) -> Result<Expression, EvalError> {
    let wraps = argument_kinds(name, &args, Some(&ret_type)).is_some();
    let mut call =
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), ret_type, args));
    if !wraps {
        return Ok(call);
    }
    derive_tree_collation_with_connection(&mut call, connection)?;
    if let Expression::ScalarFunction(function) = &mut call {
        let ret = function.ret_type.clone();
        let args = std::mem::take(&mut function.args);
        // Go `fieldString(arg0Tp.GetType())`, read before the cast.
        let string_field_source = name == "from_unixtime"
            && args
                .first()
                .and_then(Expression::static_type)
                .is_some_and(|tp| {
                    matches!(
                        tp.code(),
                        FieldTypeCode::String
                            | FieldTypeCode::Varchar
                            | FieldTypeCode::TinyBlob
                            | FieldTypeCode::MediumBlob
                            | FieldTypeCode::LongBlob
                            | FieldTypeCode::Blob
                    )
                });
        function.args = wrap_builtin_arguments(name, args, connection, fold, ret.as_ref())?;
        if string_field_source {
            // Go "used to adjust FromUnixTime precision #Fixbug35184": a
            // string's cast to an integral DECIMAL gains six fractional digits.
            if let Some(Expression::ScalarFunction(cast)) = function.args.first_mut() {
                if let Some(tp) = cast.ret_type.as_mut() {
                    if tp.code() == FieldTypeCode::NewDecimal && tp.decimal() == 0 {
                        tp.set_decimal(6);
                        tp.set_flen((tp.flen() + 6).min(tidb_datatype::MAX_DECIMAL_WIDTH));
                    }
                }
            }
        }
    }
    Ok(call)
}

/// Wraps `args` the way Go's `newBaseBuiltinFuncWithTp` does for the builtin
/// `name`. `fold` is `BuildCastFunction`'s closing `FoldConstant`, which Go
/// skips for a cast whose result is JSON.
pub(super) fn wrap_builtin_arguments(
    name: &str,
    args: Vec<Expression>,
    connection: (&str, &str),
    fold: &dyn Fn(&mut Expression),
    ret_type: Option<&FieldType>,
) -> Result<Vec<Expression>, EvalError> {
    let Some(kinds) = argument_kinds(name, &args, ret_type) else {
        return Ok(args);
    };
    if kinds.len() != args.len() {
        // A malformed call (wrong argument count) is rejected by the class's
        // own check; leave its arguments as they are.
        return Ok(args);
    }
    args.into_iter()
        .zip(kinds)
        .map(|(arg, kind)| {
            let was_cast = is_newly_built_cast(&arg);
            let mut wrapped = match kind {
                ArgKind::Skip => return Ok(arg),
                ArgKind::Int => wrap_with_cast_as_int(arg, None)?,
                ArgKind::Real => wrap_with_cast_as_real(arg)?,
                ArgKind::Decimal => wrap_with_cast_as_decimal(arg)?,
                ArgKind::Text => wrap_with_cast_as_string(arg, connection)?,
                ArgKind::Datetime => {
                    wrap_with_cast_as_time(arg, FieldType::new(FieldTypeCode::Datetime))?
                }
                ArgKind::Timestamp => {
                    wrap_with_cast_as_time(arg, FieldType::new(FieldTypeCode::Timestamp))?
                }
                ArgKind::Duration => wrap_with_cast_as_duration(arg)?,
                ArgKind::Document => return wrap_with_cast_as_json(arg),
                ArgKind::Value => {
                    let mut wrapped = wrap_with_cast_as_json(arg)?;
                    disable_parse_json_flag_4_expr(&mut wrapped);
                    return Ok(wrapped);
                }
            };
            if is_newly_built_cast(&wrapped) && !was_cast {
                derive_tree_collation_with_connection(&mut wrapped, connection)?;
                fold(&mut wrapped);
            }
            Ok(wrapped)
        })
        .collect()
}
