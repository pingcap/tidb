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

//! `pkg/expression/chunk_executor.go`'s `evalOneVec`: the result column a
//! vectorized expression leaves in the output chunk.
//!
//! Go hands the output chunk's column to the expression's `VecEval*`, and
//! each vectorized builtin sizes that `result` itself: most `Resize*` it
//! exactly (`make` when it is short), string builtins `Reserve*` it and
//! append, and arithmetic evaluates one argument straight into it -- where a
//! column reference `CopyReconstruct`s its input column and a constant
//! `Resize*`s (`genVecFromConstExpr`). So the output chunk's capacities are
//! the expression tree's, not those of one append per row.
//!
//! The VALUES come from [`crate::evaluator::EvaluatorSuite::run`]'s batch and
//! row evaluators. This module performs the allocation steps Go's
//! `vecEval*` perform on `result` and then empties the column, so the
//! evaluator's appends fill buffers that Go sized: a fixed-width column
//! already holds its `n` cells, a copied column all of its bytes, and a
//! reserved one grows by Go's `append` as Go's own appends do.
//!
//! Narrowings, each named:
//! - The builtins whose `vecEval*` result step is ported are the ones
//!   [`builtin_vec_result`] lists. Any other builtin is treated as Go treats
//!   a non-vectorized one (`evalOneColumn` appends row by row), so its tree
//!   keeps the evaluator's appends.
//! - Go's control functions (`IF`, `IFNULL`, `CASE`, `COALESCE`, `AND`,
//!   `OR`) fall back to row evaluation (`fallbackEval*`) when an argument's
//!   vectorized evaluation errors or warns. For a fixed-width result that
//!   fallback `Resize*`s the column; this port always takes the vectorized
//!   step, which differs only when `IF`'s condition warns before its
//!   branch was evaluated into the column.
//! - `GREATEST`/`LEAST` over strings (which swap the result with a buffer
//!   per argument) are not ported.
//! - A prepared parameter is shaped by its planned type. Go's
//!   `Constant.GetType` infers it from this execution's value; reading the
//!   value here would be a second parameter read the evaluator does not make.

use tidb_chunk::chunk::Chunk;
use tidb_chunk::column::Column as ChunkColumn;
use tidb_datatype::{EvalType, FieldTypeCode};

use crate::column::Column;
use crate::expression::{Expression, ScalarFunction};

/// What a Go `vecEval*` does to its `result` before writing its rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VecResult {
    /// `result.Resize*(n, _)`, or `Reserve*(n)` and one append per row for
    /// a variable-width result (`string`, `JSON`).
    Allocate,
    /// `b.args[i].VecEval*(ctx, input, result)`: the argument writes the
    /// result column, which the builtin then updates in place.
    Arg(usize),
}

/// The evaluation type Go's `GetType(ctx).EvalType()` reports, from the
/// planned type: a prepared parameter's planned type, not one inferred from
/// this execution's value (see the module's narrowings).
fn eval_type_of(expression: &Expression) -> Option<EvalType> {
    expression.static_type().map(|ty| ty.eval_type())
}

/// Go's `types.ETDatetime` and `types.ETTimestamp` share every time builtin
/// and `Column.VecEvalTime`.
fn same_class(left: EvalType, right: EvalType) -> bool {
    let time = |ty| matches!(ty, EvalType::Datetime | EvalType::Timestamp);
    left == right || (time(left) && time(right))
}

/// The result step of the Go builtin signature `function` selects, for the
/// signatures whose `vectorized()` is true and whose step is ported. `None`
/// is Go's non-vectorized builtin (`baseBuiltinFunc.vectorized`), or one not
/// ported here.
fn builtin_vec_result(function: &ScalarFunction) -> Option<VecResult> {
    use EvalType::{Datetime, Decimal, Duration, Int, Json, Real, String, Timestamp, VectorFloat32};
    use VecResult::{Allocate, Arg};
    let eval_type = function.get_static_type()?.eval_type();
    let arg_type = |index: usize| function.args.get(index).and_then(|arg| eval_type_of(arg));
    // Go's `*VectorFloat32Sig`s keep `baseBuiltinFunc.vectorized`.
    if eval_type == VectorFloat32 || (0..function.args.len()).any(|index| arg_type(index) == Some(VectorFloat32)) {
        return None;
    }
    let fixed = !matches!(eval_type, String | Json);
    Some(match function.func_name.lowercase() {
        // builtin_arithmetic_vec.go: `Plus`/`Minus`/`Mod` over integers
        // evaluate the right argument into `result` (`lh` is a buffer),
        // `Multiply`/`Divide` and the real and decimal signatures the left.
        "plus" | "minus" | "mod" => match eval_type {
            Int => Arg(1),
            Real | Decimal => Arg(0),
            _ => return None,
        },
        "mul" | "div" => match eval_type {
            Int | Real | Decimal => Arg(0),
            _ => return None,
        },
        // `intDivideFunctionClass`: two integers are `IntDivideIntSig`
        // (right argument into `result`); otherwise `IntDivideDecimalSig`
        // resizes it.
        "intdiv" => {
            if arg_type(0) == Some(Int) && arg_type(1) == Some(Int) {
                Arg(1)
            } else {
                Allocate
            }
        }
        // builtin_op_vec.go `UnaryMinus*`, builtin_math_vec.go `Abs*`,
        // `Round*`/`RoundWithFrac*`: the argument into `result`.
        "unaryminus" | "abs" | "round" => match eval_type {
            Int | Real | Decimal => Arg(0),
            _ => return None,
        },
        // builtin_compare_vec(_generated).go: every comparison and `In*`
        // resizes an integer result.
        "eq" | "ne" | "lt" | "le" | "gt" | "ge" | "nulleq" | "in" => Allocate,
        "greatest" | "least" => match eval_type {
            Int | Real | Decimal => Arg(0),
            Datetime | Timestamp | Duration => Allocate,
            _ => return None,
        },
        // `Coalesce*` and `CaseWhen*` resize (or reserve and append).
        "coalesce" | "case" => Allocate,
        // builtin_control_vec_generated.go: `If*` evaluates its true branch
        // into `result`, `IfNull*` its first argument; the string and JSON
        // signatures reserve and append.
        "if" => {
            if fixed {
                Arg(1)
            } else {
                Allocate
            }
        }
        "ifnull" => {
            if fixed {
                Arg(0)
            } else {
                Allocate
            }
        }
        // builtin_op_vec.go: `LogicAnd`/`LogicOr`/`LogicXor` evaluate their
        // left argument into `result` (a non-integer one is wrapped by
        // `wrapWithIsTrue`, whose `*IsTrue` resizes).
        "and" | "or" | "xor" => Arg(0),
        // `UnaryNot*`, `*IsNull`, `*IsTrue`/`*IsFalse`: the integer
        // signatures evaluate the argument into `result`, the others resize
        // it; `UnaryNotJSON` keeps `baseBuiltinFunc.vectorized`.
        "not" => match arg_type(0)? {
            Int => Arg(0),
            Json => return None,
            _ => Allocate,
        },
        "isnull" | "istrue" | "isfalse" | "istrue_with_null" | "isfalse_with_null" => {
            match arg_type(0)? {
                Int => Arg(0),
                _ => Allocate,
            }
        }
        // builtin_string_vec.go: lengths resize; the string results reserve
        // and append, except `Lower`/`Upper` over a binary string, which
        // evaluate the argument into `result` and case-map it in place.
        "length" | "octet_length" | "char_length" | "character_length" | "bit_length" => Allocate,
        "concat" | "concat_ws" | "substring" | "substr" | "mid" | "left" | "right" | "trim"
        | "ltrim" | "rtrim" | "replace" => Allocate,
        "upper" | "ucase" | "lower" | "lcase" => {
            if function.args.first()?.static_type()?.is_binary_string() {
                Arg(0)
            } else {
                Allocate
            }
        }
        // The string builtins that reserve and append (`Repeat`, `Lpad`,
        // `Hex`, `MD5`, ...) or resize an integer (`ASCII`, `Strcmp`,
        // `FindInSet`, `Field*`, `Instr`, ...); `Reverse` evaluates its
        // argument into `result` and reverses each cell in place.
        "repeat" | "space" | "hex" | "unhex" | "quote" | "insert" | "substring_index"
        | "export_set" | "lpad" | "rpad" | "format" | "oct" | "bin" | "elt" | "char"
        | "make_set" | "to_base64" | "from_base64" | "translate" | "md5" | "sha1" | "sha"
        | "sha2" | "sm3" | "ascii" | "ord" | "strcmp" | "find_in_set" | "field" | "instr"
        | "conv" => Allocate,
        "reverse" | "bit_count" => Arg(0),
        // `Locate2Args*` resize the result; `Locate3Args*` evaluate the
        // position argument into it.
        "locate" | "position" => {
            if function.args.len() == 3 {
                Arg(2)
            } else {
                Allocate
            }
        }
        // builtin_math_vec.go: the real functions evaluate their (first)
        // argument into `result` and `Pow` its exponent; `Floor`, `Ceil` and
        // `Truncate` evaluate the argument into `result` when it has the
        // result's type (`FloorIntToInt`, `FloorDecToDec`, `FloorReal`, ...)
        // and resize it otherwise (`FloorDecToInt`, `FloorIntToDec`, ...);
        // `Sign` and `CRC32` resize.
        "sqrt" | "exp" | "ln" | "log" | "log2" | "log10" | "sin" | "cos" | "tan" | "cot"
        | "asin" | "acos" | "atan" | "atan2" | "degrees" | "radians" => match eval_type {
            Real => Arg(0),
            _ => return None,
        },
        "pow" | "power" => match eval_type {
            Real => Arg(1),
            _ => return None,
        },
        "floor" | "ceil" | "ceiling" | "truncate" => {
            if arg_type(0).is_some_and(|ty| same_class(ty, eval_type)) {
                Arg(0)
            } else {
                Allocate
            }
        }
        "sign" | "crc32" => Allocate,
        // builtin_time_vec.go: the field extractors, differences,
        // conversions and `DATE_ADD`/`DATE_SUB` resize (`Hour`, `Minute`,
        // `Second`, `MicroSecond` through `vecEvalIntByRows`), the
        // formatters reserve and append; `Date` and `LastDay` evaluate their
        // argument into `result`.
        "year" | "month" | "quarter" | "day" | "dayofmonth" | "dayofweek" | "dayofyear"
        | "weekday" | "week" | "weekofyear" | "yearweek" | "to_days" | "to_seconds"
        | "datediff" | "timestampdiff" | "extract" | "time_to_sec" | "hour" | "minute"
        | "second" | "microsecond" | "unix_timestamp" | "from_days" | "makedate" | "maketime"
        | "sec_to_time" | "monthname" | "dayname" | "date_format" | "time_format"
        | "from_unixtime" | "date_add" | "date_sub" | "adddate" | "subdate" | "timestamp"
        | "str_to_date" | "timestampadd" => Allocate,
        "date" | "last_day" => Arg(0),
        // builtin_time_vec.go `ConvertTz` evaluates its time into `result`;
        // builtin_time_vec_generated.go's `Add*`/`Sub*` signatures over a
        // DATETIME/TIMESTAMP or TIME first argument evaluate it into
        // `result`, except over a DATETIME/TIMESTAMP second argument (the
        // `*Null` signatures), and the DATE and string ones resize.
        "convert_tz" => Arg(0),
        "addtime" | "subtime" => {
            let code = |index: usize| function.args.get(index)?.static_type().map(|field_type| field_type.code());
            let temporal = |code| matches!(code, Some(FieldTypeCode::Datetime | FieldTypeCode::Timestamp));
            if (temporal(code(0)) || code(0) == Some(FieldTypeCode::Duration)) && !temporal(code(1)) {
                Arg(0)
            } else {
                Allocate
            }
        }
        // builtin_json_vec.go: every vectorized JSON builtin resizes or
        // reserves and appends; `JSONValid*` and `JSON_SCHEMA_VALID` keep
        // `baseBuiltinFunc.vectorized`.
        "json_array" | "json_array_append" | "json_array_insert" | "json_contains"
        | "json_contains_path" | "json_depth" | "json_extract" | "json_insert" | "json_keys"
        | "json_length" | "json_memberof" | "json_merge" | "json_merge_patch"
        | "json_merge_preserve" | "json_object" | "json_overlaps" | "json_pretty"
        | "json_quote" | "json_remove" | "json_replace" | "json_search" | "json_set"
        | "json_storage_free" | "json_storage_size" | "json_type" | "json_unquote" => Allocate,
        // builtin_miscellaneous_vec.go `*AnyValue` evaluate their argument
        // into `result`.
        "any_value" => Arg(0),
        // builtin_info_vec.go: the info builtins resize or reserve;
        // `LAST_INSERT_ID(expr)` evaluates its argument into `result`.
        "database" | "schema" | "user" | "current_user" | "session_user" | "system_user"
        | "current_role" | "connection_id" | "found_rows" | "row_count" | "version"
        | "tidb_version" | "current_resource_group" | "tidb_is_ddl_owner" | "tidb_decode_key" => {
            Allocate
        }
        "last_insert_id" => {
            if function.args.is_empty() {
                Allocate
            } else {
                Arg(0)
            }
        }
        // builtin_other_vec.go `SetVar*` reserve or resize; `SetTimeVar` and
        // `SetDurationVar` keep `baseBuiltinFunc.vectorized`.
        "setvar" => match eval_type {
            Int | Real | Decimal | String => Allocate,
            _ => return None,
        },
        name if name.starts_with("cast_") => cast_vec_result(function.args.first()?, eval_type)?,
        _ => return None,
    })
}

/// builtin_cast_vec.go: a cast to its own evaluation type evaluates the
/// argument into `result` (`CastIntAsInt`, `CastDecimalAsDecimal`,
/// `CastTimeAsTime`, `CastJSONAsJSON`, ...) except `CastStringAsString`,
/// which reserves and appends; `CastStringAsInt` over a hybrid argument
/// evaluates it as an integer into `result`; every other cast resizes (or
/// reserves and appends).
fn cast_vec_result(argument: &Expression, target: EvalType) -> Option<VecResult> {
    let source = eval_type_of(argument)?;
    if same_class(source, target) {
        return Some(if target == EvalType::String {
            VecResult::Allocate
        } else {
            VecResult::Arg(0)
        });
    }
    let hybrid = argument.static_type().is_some_and(|ty| ty.is_hybrid());
    Some(if source == EvalType::String && target == EvalType::Int && hybrid {
        VecResult::Arg(0)
    } else {
        VecResult::Allocate
    })
}

/// Go `Expression.Vectorized`: a column or constant is, a scalar function
/// when its signature is and so are all of its arguments
/// (`isChildrenVectorized`).
pub(crate) fn vectorized(expression: &Expression) -> bool {
    match expression {
        Expression::Column(_) | Expression::CorrelatedColumn(_) => true,
        Expression::Constant(constant) => constant
            .deferred_expr
            .as_deref()
            .is_none_or(|deferred| vectorized(deferred)),
        Expression::ScalarFunction(function) => {
            builtin_vec_result(function).is_some()
                && function.args.iter().all(|arg| vectorized(arg))
        }
        // The evaluator has no vectorized path for an unevaluated subquery.
        Expression::ScalarSubQuery(_) => false,
    }
}

/// `result.Resize*(n, false)` for a fixed-width evaluation type, or
/// `result.Reserve*(n)` for a variable-width one: Go `genVecFromConstExpr`
/// and every builtin whose result step is [`VecResult::Allocate`].
fn allocate(eval_type: EvalType, n: usize, result: &mut ChunkColumn) {
    match eval_type {
        EvalType::Int => result.resize_int64(n, false),
        EvalType::Real => result.resize_float64(n, false),
        EvalType::Decimal => result.resize_decimal(n, false),
        EvalType::Datetime | EvalType::Timestamp => result.resize_time(n, false),
        EvalType::Duration => result.resize_go_duration(n, false),
        EvalType::String => result.reserve_string(n),
        EvalType::Json => result.reserve_json(n),
        EvalType::VectorFloat32 => result.reserve_vector_float32(n),
    }
}

/// Go `Column.VecEval*` (`expression/column.go`): a hybrid column (enum,
/// set, bit) read as an integer is re-appended from `ResizeInt64(0)`, and
/// as a string from `ReserveString(n)`; a FLOAT column read as a real is
/// widened into `ResizeFloat64(n)`; any other column is
/// `CopyReconstruct(input.Sel(), result)`. A request in another evaluation
/// type is the cast Go wraps the column in.
fn column_vec_eval(column: &Column, eval_type: EvalType, input: &Chunk, result: &mut ChunkColumn) {
    let n = input.num_rows();
    let Some(field_type) = column.get_static_type() else {
        return allocate(eval_type, n, result);
    };
    let hybrid = field_type.is_hybrid();
    match eval_type {
        EvalType::Int if hybrid => return result.resize_int64(0, false),
        EvalType::String if hybrid => return result.reserve_string(n),
        EvalType::Real if field_type.code() == FieldTypeCode::Float => {
            return result.resize_float64(n, false);
        }
        _ => {}
    }
    let index = usize::try_from(column.index).ok().filter(|&index| index < input.num_cols());
    match index {
        Some(index) if same_class(field_type.eval_type(), eval_type) => {
            let destination = std::mem::take(result);
            *result = input.column(index).copy_reconstruct(input.sel(), Some(destination));
        }
        _ => allocate(eval_type, n, result),
    }
}

/// Go `expression.VecEval<eval_type>(ctx, input, result)`, as far as the
/// buffers it leaves in `result`.
fn vec_eval(
    expression: &Expression,
    eval_type: EvalType,
    input: &Chunk,
    result: &mut ChunkColumn,
) {
    let n = input.num_rows();
    match expression {
        Expression::Column(column) => column_vec_eval(column, eval_type, input, result),
        Expression::Constant(constant) => match constant.deferred_expr.as_deref() {
            Some(deferred) => vec_eval(deferred, eval_type, input, result),
            None => allocate(eval_type, n, result),
        },
        Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => {
            allocate(eval_type, n, result);
        }
        Expression::ScalarFunction(function) => {
            // A request in another evaluation type is the cast Go wraps the
            // function in, which resizes (or reserves).
            let own = function.get_static_type().map(|ty| ty.eval_type());
            if own.is_none_or(|own| !same_class(own, eval_type)) {
                return allocate(eval_type, n, result);
            }
            match builtin_vec_result(function) {
                Some(VecResult::Arg(index)) if index < function.args.len() => {
                    vec_eval(&function.args[index], eval_type, input, result);
                }
                _ => allocate(eval_type, n, result),
            }
        }
    }
}

/// Go `evalOneVec`'s effect on output column `col_idx`: the expression's
/// `VecEval*` sizes the column, then a BIT integer, a FLOAT real, and an
/// ENUM or SET string replace it with `chunk.NewColumn(ft, n)` sized for
/// their own cells (`ReserveBytes`, `ResizeFloat32`, `ReserveEnum`,
/// `ReserveSet`). The column is left empty for the evaluator's appends.
///
/// Callers check [`vectorized`] first: Go's `evalOneColumn` (a
/// non-vectorized expression) appends into the column as it stands.
pub(crate) fn eval_one_vec(expression: &Expression, input: &Chunk, output: &mut Chunk, col_idx: usize) {
    let Some(field_type) = expression.static_type().cloned() else {
        return;
    };
    let n = input.num_rows();
    let eval_type = field_type.eval_type();
    let mut result = output.column_mut(col_idx);
    vec_eval(expression, eval_type, input, &mut result);
    match (eval_type, field_type.code()) {
        (EvalType::Int, FieldTypeCode::Bit) => {
            *result = ChunkColumn::new_column(&field_type, n);
            result.reserve_bytes(n);
        }
        (EvalType::Real, FieldTypeCode::Float) => {
            *result = ChunkColumn::new_column(&field_type, n);
            result.resize_float32(n, false);
        }
        (EvalType::String, FieldTypeCode::Enum) => {
            *result = ChunkColumn::new_column(&field_type, n);
            result.reserve_enum(n);
        }
        (EvalType::String, FieldTypeCode::Set) => {
            *result = ChunkColumn::new_column(&field_type, n);
            result.reserve_set(n);
        }
        _ => {}
    }
    result.reset();
}
