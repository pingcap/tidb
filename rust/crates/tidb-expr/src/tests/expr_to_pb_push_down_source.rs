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

//! Go `pkg/expression/expr_to_pb_test.go`'s push-down tables: the functions
//! each store evaluates and the ones it refuses, row for row.
//! `TestExprOnlyPushDownToFlash` is skipped by Go and is not here.

use tidb_charset::Collation;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags, StringDatum};
use tidb_proto::tipb::ScalarFuncSig;

use crate::column::Column;
use crate::constant::Constant;
use crate::expression::Expression;
use crate::infer_pushdown::{
    can_exprs_push_down, can_exprs_push_down_with_extra_info, push_down_exprs_with_extra_info,
};
use tidb_kv::store_type::StoreType;
use crate::scalar_function::ScalarFunction;

const NONE: Vec<String> = Vec::new();

/// Go `mock.NewContext()`: a statement context with a clock.
struct MockContext;

impl tidb_exprctx::Columns for MockContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((1_700_000_000, 0, 0))
    }
}

/// Go `genColumn`.
fn gen_column(tp: FieldTypeCode, id: i64) -> Column {
    let mut column = Column::default();
    column.ret_type = Some(FieldType::new(tp));
    column.id = id;
    column.index = id;
    column
}

/// Go `column.RetType`.
fn ret_type(column: &mut Column) -> &mut FieldType {
    column.ret_type.as_mut().expect("genColumn sets the type")
}

fn col(column: &Column) -> Expression {
    Expression::Column(column.clone())
}

/// A Go `Constant` holding `types.NewStringDatum(value)`.
fn string_constant(value: &str, ret_type: FieldType) -> Expression {
    Expression::Constant(Constant::new(
        Datum::String(StringDatum::new(value, Collation::Utf8Mb4Bin)),
        ret_type,
    ))
}

/// Go `NewStrConst`.
fn new_str_const(value: &str) -> Expression {
    let mut ret_type = FieldType::new(FieldTypeCode::VarString);
    ret_type.set_flen(value.len() as i64);
    string_constant(value, ret_type)
}

/// Go `NewFunction(mock.NewContext(), name, retType, args...)`, which the
/// tables require to succeed.
fn func(name: &str, ret_type: FieldType, args: Vec<Expression>) -> Expression {
    crate::new_function::new_function(&MockContext, name, ret_type, args)
        .unwrap_or_else(|error| panic!("NewFunction({name}): {error:?}"))
}

/// Go `function.(*ScalarFunction).Function.PbCode()`.
fn pb_code(function: &Expression) -> ScalarFuncSig {
    match function {
        Expression::ScalarFunction(function) => function.pb_code(),
        _ => ScalarFuncSig::Unspecified,
    }
}

/// The row's place in its Go table, its function and its argument types.
fn describe(index: usize, expr: &Expression) -> String {
    let Expression::ScalarFunction(function) = expr else {
        return format!("#{index} {expr:?}");
    };
    let args: Vec<String> = function
        .args
        .iter()
        .map(|arg| match arg {
            Expression::ScalarFunction(arg) => arg.func_name.lowercase().to_string(),
            arg => arg
                .static_type()
                .map_or_else(String::new, |tp| format!("{:?}", tp.code())),
        })
        .collect();
    format!(
        "#{index} {}({}) {}",
        function.func_name.lowercase(),
        args.join(", "),
        pb_code(expr).as_str_name()
    )
}

fn split(exprs: &[Expression], store: StoreType, want_pushed: bool) -> Vec<String> {
    split_with_extra_info(exprs, store, false, want_pushed)
}

fn split_with_extra_info(exprs: &[Expression], store: StoreType, can_enum_push: bool, want_pushed: bool) -> Vec<String> {
    exprs
        .iter()
        .enumerate()
        .filter(|(_, expr)| {
            let (pushed, _) = push_down_exprs_with_extra_info(
                vec![(*expr).clone()],
                store,
                &Default::default(),
                can_enum_push,
                &|_| {},
            );
            pushed.len() == usize::from(want_pushed)
        })
        .map(|(index, expr)| describe(index, expr))
        .collect()
}

/// Go `pushed, _ := PushDownExprsWithExtraInfo(pushDownCtx, exprs, store, canEnumPush)`, each row named.
fn pushed_with_extra_info(exprs: &[Expression], store: StoreType, can_enum_push: bool) -> Vec<String> {
    split_with_extra_info(exprs, store, can_enum_push, true)
}

/// Go `_, remained := PushDownExprsWithExtraInfo(pushDownCtx, exprs, store, canEnumPush)`, each row named.
fn remained_with_extra_info(exprs: &[Expression], store: StoreType, can_enum_push: bool) -> Vec<String> {
    split_with_extra_info(exprs, store, can_enum_push, false)
}

/// Go `pushed, _ := PushDownExprs(pushDownCtx, exprs, store)`, each row named.
fn pushed(exprs: &[Expression], store: StoreType) -> Vec<String> {
    split(exprs, store, true)
}

/// Go `_, remained := PushDownExprs(pushDownCtx, exprs, store)`, each row named.
fn remained(exprs: &[Expression], store: StoreType) -> Vec<String> {
    split(exprs, store, false)
}

/// Go `TestJsonPushDownToFlash`.
#[test]
#[allow(unused_mut, unused_variables)]
fn json_push_down_to_flash() {
    let mut exprs: Vec<Expression> = Vec::new();
    let mut json_column = gen_column(FieldTypeCode::Json, 1);
    let mut int_column = gen_column(FieldTypeCode::LongLong, 2);
    let mut string_column = gen_column(FieldTypeCode::String, 5);

    // functions that can be pushdown to tiflash
    // json_length
    let function = func("json_length", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column)]);
    exprs.push(function.clone());

    // json_extract
    let function = func("json_extract", FieldType::new(FieldTypeCode::Json), vec![col(&json_column), col(&string_column)]);
    exprs.push(function.clone());

    // json_unquote argument is cast(json as string)
    let sub_func = func("cast", FieldType::new(FieldTypeCode::String), vec![col(&json_column)]);
    let function = func("json_unquote", FieldType::new(FieldTypeCode::String), vec![sub_func.clone()]);
    exprs.push(function.clone());

    // json_unquote's argument is not cast(json as string)
    let function = func("json_unquote", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // json_array
    let function = func("json_array", FieldType::new(FieldTypeCode::Json), vec![col(&json_column), col(&json_column), col(&json_column), col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // json_object
    let function = func("json_object", FieldType::new(FieldTypeCode::Json), vec![col(&string_column), col(&json_column), col(&string_column), col(&json_column)]);
    exprs.push(function.clone());

    // json_depth
    let function = func("json_depth", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column)]);
    exprs.push(function.clone());

    // json_contains_path
    let function = func("json_contains_path", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column), col(&string_column), col(&string_column)]);
    exprs.push(function.clone());

    // json_valid
    // json_valid_others
    let function = func("json_valid", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column)]);
    exprs.push(function.clone());
    // json_valid_json
    let function = func("json_valid", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column)]);
    exprs.push(function.clone());
    // json_valid_string
    let function = func("json_valid", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column)]);
    exprs.push(function.clone());

    // json_keys
    // 1 arg
    let function = func("json_keys", FieldType::new(FieldTypeCode::Json), vec![col(&json_column)]);
    exprs.push(function.clone());
    // 2 args
    let function = func("json_keys", FieldType::new(FieldTypeCode::Json), vec![col(&json_column), col(&string_column)]);
    exprs.push(function.clone());

    // CastJsonAsString
    let function = func("cast", FieldType::new(FieldTypeCode::String), vec![col(&json_column)]);
    exprs.push(function.clone());

    // CastJsonAsJson
    let function = func("cast", FieldType::new(FieldTypeCode::Json), vec![col(&json_column)]);
    exprs.push(function.clone());

    // IfNullJson
    let function = func("ifnull", FieldType::new(FieldTypeCode::Json), vec![col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // IfJson
    let function = func("if", FieldType::new(FieldTypeCode::Json), vec![col(&int_column), col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // JsonIsNull is not implement, for function json_col is null, it will be converted to cast(json as string) is null
    let function = func("isnull", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column)]);
    exprs.push(function.clone());

    // CaseWhenJson
    let function = func("case", FieldType::new(FieldTypeCode::Json), vec![col(&int_column), col(&json_column), col(&int_column), col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // CoalesceJson
    let function = func("coalesce", FieldType::new(FieldTypeCode::Json), vec![col(&json_column), col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    assert_eq!(remained(&exprs, StoreType::TiFlash), NONE);

    // functions that can not be pushed to tiflash
    exprs.clear();
    // LTJson
    let function = func("lt", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // LEJson
    let function = func("le", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // GTJson
    let function = func("gt", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // GEJson
    let function = func("ge", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // EQJson
    let function = func("eq", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // NEJson
    let function = func("ne", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    // InJson
    let function = func("in", FieldType::new(FieldTypeCode::LongLong), vec![col(&json_column), col(&json_column), col(&json_column)]);
    exprs.push(function.clone());

    assert_eq!(pushed(&exprs, StoreType::TiFlash), NONE);
}

/// Go `TestExprPushDownToFlash`.
#[test]
#[allow(unused_mut, unused_variables)]
fn expr_push_down_to_flash() {
    let mut exprs: Vec<Expression> = Vec::new();
    let mut int_column = gen_column(FieldTypeCode::LongLong, 2);
    let mut real_column = gen_column(FieldTypeCode::Double, 3);
    let mut decimal_column = gen_column(FieldTypeCode::NewDecimal, 4);
    ret_type(&mut decimal_column).set_decimal(30);
    ret_type(&mut decimal_column).set_flen(65);
    let mut string_column = gen_column(FieldTypeCode::String, 5);
    let mut datetime_column = gen_column(FieldTypeCode::Datetime, 6);
    let mut binary_string_column = gen_column(FieldTypeCode::String, 7);
    ret_type(&mut binary_string_column).set_collation_name("binary");
    let mut int32_column = gen_column(FieldTypeCode::Long, 8);
    let mut float32_column = gen_column(FieldTypeCode::Float, 9);
    let mut enum_column = gen_column(FieldTypeCode::Enum, 10);
    let mut duration_column = gen_column(FieldTypeCode::Duration, 11);
    // uint64 col
    let mut uint_column = gen_column(FieldTypeCode::LongLong, 12);
    ret_type(&mut uint_column).add_flags(FieldTypeFlags::UNSIGNED);

    let function = func("lpad", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int32_column), col(&string_column)]);
    exprs.push(function.clone());

    // truncate
    let function = func("truncate", FieldType::new(FieldTypeCode::NewDecimal), vec![col(&decimal_column), col(&int_column)]);
    exprs.push(function.clone());

    let function = func("truncate", FieldType::new(FieldTypeCode::Double), vec![col(&float32_column), col(&int_column)]);
    exprs.push(function.clone());

    let function = func("truncate", FieldType::new(FieldTypeCode::Long), vec![col(&int_column), col(&int_column)]);
    exprs.push(function.clone());

    // rpad
    let function = func("rpad", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int32_column), col(&string_column)]);
    exprs.push(function.clone());

    let function = func("if", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column), col(&int_column), col(&int_column)]);
    exprs.push(function.clone());

    let function = func("bitneg", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column)]);
    exprs.push(function.clone());

    let function = func("bitxor", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column), col(&int_column)]);
    exprs.push(function.clone());

    // ExtractDatetime: can be pushed
    let extract_datetime_unit_col = string_constant("day", FieldType::new(FieldTypeCode::String));
    let function = func("extract", FieldType::new(FieldTypeCode::LongLong), vec![extract_datetime_unit_col.clone(), col(&datetime_column)]);
    exprs.push(function.clone());

    // ExtractDuration
    let extract_duration_unit_col = string_constant("microsecond", FieldType::new(FieldTypeCode::String));
    let function = func("extract", FieldType::new(FieldTypeCode::LongLong), vec![extract_duration_unit_col.clone(), col(&duration_column)]);
    exprs.push(function.clone());

    // CastIntAsInt
    let function = func("cast", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column)]);
    exprs.push(function.clone());

    // CastRealAsInt
    let function = func("cast", FieldType::new(FieldTypeCode::LongLong), vec![col(&real_column)]);
    exprs.push(function.clone());

    // CastDecimalAsInt
    let function = func("cast", FieldType::new(FieldTypeCode::LongLong), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // CastDecimalAsDouble
    let function = func("cast", FieldType::new(FieldTypeCode::Double), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // CastStringAsInt
    let function = func("cast", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column)]);
    exprs.push(function.clone());

    // CastTimeAsInt
    let function = func("cast", FieldType::new(FieldTypeCode::LongLong), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    let mut valid_decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
    valid_decimal_type.set_flen(20);
    valid_decimal_type.set_decimal(2);
    // CastIntAsDecimal
    let function = func("cast", valid_decimal_type.clone(), vec![col(&int_column)]);
    exprs.push(function.clone());

    // CastRealAsDecimal
    let function = func("cast", valid_decimal_type.clone(), vec![col(&real_column)]);
    exprs.push(function.clone());

    // CastDecimalAsDecimal
    let function = func("cast", valid_decimal_type.clone(), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // CastStringAsDecimal
    let function = func("cast", valid_decimal_type.clone(), vec![col(&string_column)]);
    exprs.push(function.clone());

    // CastTimeAsDecimal
    let function = func("cast", valid_decimal_type.clone(), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // CastIntAsString
    let function = func("cast", FieldType::new(FieldTypeCode::String), vec![col(&int_column)]);
    exprs.push(function.clone());

    // CastRealAsString
    let function = func("cast", FieldType::new(FieldTypeCode::String), vec![col(&real_column)]);
    exprs.push(function.clone());

    // CastDecimalAsString
    let function = func("cast", FieldType::new(FieldTypeCode::String), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // CastStringAsString
    let function = func("cast", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // CastIntAsTime
    let function = func("cast", FieldType::new(FieldTypeCode::Datetime), vec![col(&int_column)]);
    exprs.push(function.clone());

    // CastRealAsTime
    let function = func("cast", FieldType::new(FieldTypeCode::Datetime), vec![col(&real_column)]);
    exprs.push(function.clone());

    // CastDecimalAsTime
    let function = func("cast", FieldType::new(FieldTypeCode::Datetime), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // CastTimeAsTime
    let function = func("cast", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // CastStringAsReal
    let function = func("cast", FieldType::new(FieldTypeCode::Double), vec![col(&string_column)]);
    exprs.push(function.clone());

    // Substring2ArgsUTF8
    let function = func("substr", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int_column)]);
    exprs.push(function.clone());

    // Substring3ArgsUTF8
    let function = func("substr", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int_column), col(&int_column)]);
    exprs.push(function.clone());

    // sqrt
    let function = func("sqrt", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_CeilReal
    let function = func("ceil", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_CeilIntToInt
    let function = func("ceil", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_CeilDecimalToInt
    let function = func("ceil", FieldType::new(FieldTypeCode::LongLong), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_CeilDecToDec
    let function = func("ceil", FieldType::new(FieldTypeCode::NewDecimal), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_FloorReal
    let function = func("floor", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_FloorIntToInt
    let function = func("floor", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_FloorDecToInt
    let function = func("floor", FieldType::new(FieldTypeCode::LongLong), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_FloorDecToDec
    let function = func("floor", FieldType::new(FieldTypeCode::NewDecimal), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Log1Arg
    let function = func("log", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Log2Args
    let function = func("log", FieldType::new(FieldTypeCode::Double), vec![col(&real_column), col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Log2
    let function = func("log2", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Log10
    let function = func("log10", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Exp
    let function = func("exp", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Pow
    let function = func("pow", FieldType::new(FieldTypeCode::Double), vec![col(&real_column), col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Pow, Power is a synonym for Pow
    let function = func("power", FieldType::new(FieldTypeCode::Double), vec![col(&real_column), col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Radians
    let function = func("radians", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Degrees
    let function = func("degrees", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_CRC32
    let function = func("crc32", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_Conv
    let function = func("conv", FieldType::new(FieldTypeCode::Double), vec![col(&string_column), col(&int_column), col(&int_column)]);
    exprs.push(function.clone());

    // Replace
    let function = func("replace", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&string_column), col(&string_column)]);
    exprs.push(function.clone());

    // InetAton
    let function = func("inet_aton", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // InetNtoa
    let function = func("inet_ntoa", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column)]);
    exprs.push(function.clone());

    // Inet6Aton
    let function = func("inet6_aton", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // Inet6Ntoa
    let function = func("inet6_ntoa", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_RoundReal
    let function = func("round", FieldType::new(FieldTypeCode::Double), vec![col(&real_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_RoundInt
    let function = func("round", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_RoundDecimal
    let function = func("round", FieldType::new(FieldTypeCode::NewDecimal), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_RoundWithFracReal
    let function = func("round", FieldType::new(FieldTypeCode::Double), vec![col(&real_column), col(&int_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_RoundWithFracInt
    let function = func("round", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column), col(&int_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_RoundWithFracDecimal
    let function = func("round", FieldType::new(FieldTypeCode::NewDecimal), vec![col(&decimal_column), col(&int_column)]);
    exprs.push(function.clone());

    // concat
    let function = func("concat", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int_column), col(&real_column)]);
    exprs.push(function.clone());

    // UnixTimestampCurrent
    let function = func("unix_timestamp", FieldType::new(FieldTypeCode::LongLong), vec![]);
    assert!(matches!(function, Expression::Constant(_)));

    // UnixTimestampInt
    ret_type(&mut datetime_column).set_decimal(0);
    let function = func("unix_timestamp", FieldType::new(FieldTypeCode::LongLong), vec![col(&datetime_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::UnixTimestampInt);
    exprs.push(function.clone());

    // UnixTimestampDecimal
    ret_type(&mut datetime_column).set_decimal(-1);
    let function = func("unix_timestamp", FieldType::new(FieldTypeCode::NewDecimal), vec![col(&datetime_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::UnixTimestampDec);
    exprs.push(function.clone());

    // Year
    let function = func("year", FieldType::new(FieldTypeCode::LongLong), vec![col(&datetime_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::Year);
    exprs.push(function.clone());

    // Day
    let function = func("day", FieldType::new(FieldTypeCode::LongLong), vec![col(&datetime_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::DayOfMonth);
    exprs.push(function.clone());

    // Repeat
    let function = func("repeat", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::Repeat);
    exprs.push(function.clone());

    // Datediff
    let function = func("datediff", FieldType::new(FieldTypeCode::LongLong), vec![col(&datetime_column), col(&datetime_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::DateDiff);
    exprs.push(function.clone());

    // Datesub
    let const_string_column = string_constant("day", FieldType::new(FieldTypeCode::String));
    let function = func("date_sub", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column), col(&int_column), const_string_column.clone()]);
    assert_eq!(pb_code(&function), ScalarFuncSig::SubDateDatetimeInt);
    exprs.push(function.clone());
    let function = func("date_sub", FieldType::new(FieldTypeCode::Datetime), vec![col(&string_column), col(&int_column), const_string_column.clone()]);
    assert_eq!(pb_code(&function), ScalarFuncSig::SubDateStringInt);
    exprs.push(function.clone());
    let function = func("subdate", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column), col(&int_column), const_string_column.clone()]);
    assert_eq!(pb_code(&function), ScalarFuncSig::SubDateDatetimeInt);
    exprs.push(function.clone());

    // castTimeAsString:
    let function = func("cast", FieldType::new(FieldTypeCode::String), vec![col(&datetime_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::CastTimeAsString);
    exprs.push(function.clone());

    // concat_ws
    let function = func("concat_ws", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&string_column), col(&string_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::ConcatWs);
    exprs.push(function.clone());

    let function = func("from_days", FieldType::new(FieldTypeCode::Date), vec![col(&int_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::FromDays);
    exprs.push(function.clone());

    let function = func("to_days", FieldType::new(FieldTypeCode::LongLong), vec![col(&datetime_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::ToDays);
    exprs.push(function.clone());

    // StrToDateDateTime
    let function = func("str_to_date", FieldType::new(FieldTypeCode::Datetime), vec![col(&string_column), col(&string_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::StrToDateDatetime);
    exprs.push(function.clone());

    // cast Int32 to Int32
    let function = func("cast", FieldType::new(FieldTypeCode::Long), vec![col(&int32_column)]);
    exprs.push(function.clone());

    // cast float32 to float32
    let function = func("cast", FieldType::new(FieldTypeCode::Float), vec![col(&float32_column)]);
    exprs.push(function.clone());

    // upper string
    let function = func("upper", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // ucase string
    let function = func("ucase", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // lower string
    let function = func("lower", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // lcase string
    let function = func("lcase", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // sysdate
    let function = func("sysdate", FieldType::new(FieldTypeCode::Datetime), vec![col(&string_column)]);
    exprs.push(function.clone());

    assert_eq!(remained(&exprs, StoreType::TiFlash), NONE);

    exprs.clear();

    // Substring2Args: can not be pushed
    let function = func("substr", FieldType::new(FieldTypeCode::String), vec![col(&binary_string_column), col(&int_column)]);
    exprs.push(function.clone());

    // Substring3Args: can not be pushed
    let function = func("substr", FieldType::new(FieldTypeCode::String), vec![col(&binary_string_column), col(&int_column), col(&int_column)]);
    exprs.push(function.clone());

    // ExtractDatetimeFromString: can not be pushed
    let extract_datetime_from_string_unit_col = string_constant("day_microsecond", FieldType::new(FieldTypeCode::String));
    let function = func("extract", FieldType::new(FieldTypeCode::LongLong), vec![extract_datetime_from_string_unit_col.clone(), col(&string_column)]);
    exprs.push(function.clone());

    // Cast to Int32: not supported
    let function = func("cast", FieldType::new(FieldTypeCode::Long), vec![col(&string_column)]);
    exprs.push(function.clone());

    // Cast to Float: not supported
    let function = func("cast", FieldType::new(FieldTypeCode::Float), vec![col(&int_column)]);
    exprs.push(function.clone());

    // Cast to invalid decimal Type: not supported
    let function = func("cast", FieldType::new(FieldTypeCode::NewDecimal), vec![col(&int_column)]);
    exprs.push(function.clone());

    // cast Int32 to UInt32
    let mut unsigned_int32_type = FieldType::new(FieldTypeCode::Long);
    unsigned_int32_type.set_flag(FieldTypeFlags::UNSIGNED);
    let function = func("cast", unsigned_int32_type.clone(), vec![col(&int32_column)]);
    exprs.push(function.clone());

    // cast Enum as String : not supported
    let function = func("cast", FieldType::new(FieldTypeCode::String), vec![col(&enum_column)]);
    exprs.push(function.clone());

    assert_eq!(pushed(&exprs, StoreType::TiFlash), NONE);

    assert_eq!(pushed_with_extra_info(&exprs, StoreType::TiFlash, true), NONE);

    exprs.clear();
    // cast Enum as UInt : supported
    let mut unsigned_int = FieldType::new(FieldTypeCode::LongLong);
    unsigned_int.set_flag(FieldTypeFlags::UNSIGNED);
    let function = func("cast", unsigned_int.clone(), vec![col(&enum_column)]);
    exprs.push(function.clone());

    // cast Enum as Int : supported
    let function = func("cast", FieldType::new(FieldTypeCode::LongLong), vec![col(&enum_column)]);
    exprs.push(function.clone());

    // cast Enum as Double : supported
    let function = func("cast", FieldType::new(FieldTypeCode::Double), vec![col(&enum_column)]);
    exprs.push(function.clone());

    // cast Enum as decimal : supported
    let function = func("cast", valid_decimal_type.clone(), vec![col(&enum_column)]);
    exprs.push(function.clone());

    // quarter: supported
    let function = func("quarter", FieldType::new(FieldTypeCode::LongLong), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // strcmp
    let function = func("strcmp", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column), col(&string_column)]);
    exprs.push(function.clone());

    // DayName: supported
    let function = func("dayname", FieldType::new(FieldTypeCode::String), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // MonthName: supported
    let function = func("monthname", FieldType::new(FieldTypeCode::String), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // ToSeconds: supported
    let function = func("to_seconds", FieldType::new(FieldTypeCode::LongLong), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // regexpUTF8: supported
    let function = func("regexp", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column), col(&string_column)]);
    exprs.push(function.clone());

    // regexp: supported
    let function = func("regexp", FieldType::new(FieldTypeCode::LongLong), vec![col(&binary_string_column), col(&binary_string_column)]);
    exprs.push(function.clone());

    // regexp_like: supported
    let function = func("regexp_like", FieldType::new(FieldTypeCode::LongLong), vec![col(&binary_string_column), col(&binary_string_column), col(&binary_string_column)]);
    exprs.push(function.clone());

    // regexp_instr: supported
    let function = func("regexp_instr", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column), col(&string_column), col(&int_column), col(&int_column), col(&int_column), col(&string_column)]);
    exprs.push(function.clone());

    // regexp_substr: supported
    let function = func("regexp_substr", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&string_column), col(&int_column), col(&int_column), col(&string_column)]);
    exprs.push(function.clone());

    // regexp_replace: supported
    let function = func("regexp_replace", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&string_column), col(&string_column), col(&int_column), col(&int_column), col(&string_column)]);
    exprs.push(function.clone());

    // ilike: supported
    let function = func("ilike", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column), col(&string_column), col(&string_column)]);
    exprs.push(function.clone());

    // greatest
    let function = func("greatest", FieldType::new(FieldTypeCode::LongLong), vec![col(&int32_column), col(&int_column)]);
    exprs.push(function.clone());

    let function = func("greatest", FieldType::new(FieldTypeCode::Double), vec![col(&float32_column), col(&int_column)]);
    exprs.push(function.clone());

    let function = func("greatest", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&string_column)]);
    exprs.push(function.clone());

    // least
    let function = func("least", FieldType::new(FieldTypeCode::LongLong), vec![col(&int32_column), col(&int_column)]);
    exprs.push(function.clone());

    let function = func("least", FieldType::new(FieldTypeCode::Double), vec![col(&float32_column), col(&int_column)]);
    exprs.push(function.clone());

    let function = func("least", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&string_column)]);
    exprs.push(function.clone());

    // is true
    let function = func("istrue", FieldType::new(FieldTypeCode::LongLong), vec![col(&int32_column)]);
    exprs.push(function.clone());
    let function = func("istrue", FieldType::new(FieldTypeCode::LongLong), vec![col(&float32_column)]);
    exprs.push(function.clone());
    let function = func("istrue", FieldType::new(FieldTypeCode::LongLong), vec![col(&decimal_column)]);
    exprs.push(function.clone());
    // is true with null
    let function = func("istrue_with_null", FieldType::new(FieldTypeCode::LongLong), vec![col(&int32_column)]);
    exprs.push(function.clone());
    let function = func("istrue_with_null", FieldType::new(FieldTypeCode::LongLong), vec![col(&float32_column)]);
    exprs.push(function.clone());
    let function = func("istrue_with_null", FieldType::new(FieldTypeCode::LongLong), vec![col(&decimal_column)]);
    exprs.push(function.clone());
    // is false, note seems there is actually no is_false_with_null, so unable to add ut for it
    let function = func("isfalse", FieldType::new(FieldTypeCode::LongLong), vec![col(&int32_column)]);
    exprs.push(function.clone());
    let function = func("isfalse", FieldType::new(FieldTypeCode::LongLong), vec![col(&float32_column)]);
    exprs.push(function.clone());
    let function = func("isfalse", FieldType::new(FieldTypeCode::LongLong), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // DayOfWeek
    let function = func("dayofweek", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // DayOfMonth
    let function = func("dayofmonth", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // DayOfYear
    let function = func("dayofyear", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // LastDay
    let function = func("last_day", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // WeekOfYear
    let function = func("weekofyear", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // TimeToSec
    let function = func("time_to_sec", FieldType::new(FieldTypeCode::Duration), vec![col(&duration_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_RightShift
    let function = func("rightshift", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column), col(&int_column)]);
    exprs.push(function.clone());

    // ScalarFuncSig_LeftShift
    let function = func("leftshift", FieldType::new(FieldTypeCode::LongLong), vec![col(&int_column), col(&int_column)]);
    exprs.push(function.clone());

    // ReverseUTF8
    let function = func("reverse", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // Reverse
    let function = func("reverse", FieldType::new(FieldTypeCode::Blob), vec![col(&string_column)]);
    exprs.push(function.clone());

    // space
    let function = func("space", FieldType::new(FieldTypeCode::LongLong), vec![col(&int32_column)]);
    exprs.push(function.clone());

    // HexStr
    let function = func("hex", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // HexInt
    let function = func("hex", FieldType::new(FieldTypeCode::String), vec![col(&int_column)]);
    exprs.push(function.clone());

    // Bin
    let function = func("bin", FieldType::new(FieldTypeCode::String), vec![col(&int_column)]);
    exprs.push(function.clone());

    // Elt
    let function = func("elt", FieldType::new(FieldTypeCode::String), vec![col(&int_column), col(&string_column)]);
    exprs.push(function.clone());

    // CastTimeAsDuration
    let function = func("cast", FieldType::new(FieldTypeCode::Duration), vec![col(&datetime_column)]);
    assert_eq!(pb_code(&function), ScalarFuncSig::CastTimeAsDuration);
    exprs.push(function.clone());

    // Unhex
    let function = func("unhex", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // IsIPv4
    let function = func("is_ipv4", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // IsIPv6
    let function = func("is_ipv6", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    // Grouping
    let init = |mut grouping_func: ScalarFunction| {
        if grouping_func.func_name.lowercase() == "grouping" {
            grouping_func
                .set_grouping_metadata(crate::grouping::GroupingMode::BitAnd, Vec::new())
                .expect("grouping metadata");
        }
        Ok(grouping_func)
    };
    let function = crate::new_function::new_function_with_init(
        &MockContext,
        "grouping",
        FieldType::new(FieldTypeCode::LongLong),
        &init,
        vec![col(&uint_column)],
    )
    .expect("grouping");
    exprs.push(function.clone());

    // CastIntAsJson
    let function = func("cast", FieldType::new(FieldTypeCode::Json), vec![col(&int_column)]);
    exprs.push(function.clone());

    // CastRealAsJson
    let function = func("cast", FieldType::new(FieldTypeCode::Json), vec![col(&real_column)]);
    exprs.push(function.clone());

    // CastDecimalAsJson
    let function = func("cast", FieldType::new(FieldTypeCode::Json), vec![col(&decimal_column)]);
    exprs.push(function.clone());

    // CastStringAsJson
    let function = func("cast", FieldType::new(FieldTypeCode::Json), vec![col(&string_column)]);
    exprs.push(function.clone());
    let function = func("cast", FieldType::new(FieldTypeCode::Json), vec![col(&binary_string_column)]);
    exprs.push(function.clone());

    // CastTimeAsJson
    let function = func("cast", FieldType::new(FieldTypeCode::Json), vec![col(&datetime_column)]);
    exprs.push(function.clone());

    // CastDurationAsJson
    let function = func("cast", FieldType::new(FieldTypeCode::Json), vec![col(&duration_column)]);
    exprs.push(function.clone());

    assert_eq!(remained(&exprs, StoreType::TiFlash), NONE);

    assert_eq!(remained_with_extra_info(&exprs, StoreType::TiFlash, true), NONE);
}

/// Go `TestExprPushDownToTiKV`.
#[test]
#[allow(unused_mut, unused_variables)]
fn expr_push_down_to_tikv() {
    let mut exprs: Vec<Expression> = Vec::new();
    let mut json_column = gen_column(FieldTypeCode::Json, 1);
    let mut int_column = gen_column(FieldTypeCode::LongLong, 2);
    let mut real_column = gen_column(FieldTypeCode::Double, 3);
    let mut decimal_column = gen_column(FieldTypeCode::NewDecimal, 4);
    let mut string_column = gen_column(FieldTypeCode::String, 5);
    let mut datetime_column = gen_column(FieldTypeCode::Datetime, 6);
    let mut binary_string_column = gen_column(FieldTypeCode::String, 7);
    let mut date_column = gen_column(FieldTypeCode::Date, 8);
    let mut byte_column = gen_column(FieldTypeCode::Bit, 9);
    let mut duration_column = gen_column(FieldTypeCode::Duration, 10);
    ret_type(&mut binary_string_column).set_collation_name("binary");

    // Test exprs that cannot be pushed.
    let function = func("inet_aton", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    let function = func("inet_ntoa", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column)]);
    exprs.push(function.clone());

    let function = func("inet6_aton", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    let function = func("inet6_ntoa", FieldType::new(FieldTypeCode::LongLong), vec![col(&string_column)]);
    exprs.push(function.clone());

    let function = func("is_ipv4", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    let function = func("is_ipv6", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    let function = func("is_ipv4_compat", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    let function = func("is_ipv4_mapped", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]);
    exprs.push(function.clone());

    assert_eq!(pushed(&exprs, StoreType::TiKv), NONE);

    // Test Conv function, `conv` function for a BIT column should not be pushed down for its special behavior which
    // is only handled in TiDB currently.
    // see issue: https://github.com/pingcap/tidb/issues/51877
    exprs.clear();
    let function = func("conv", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int_column), col(&int_column)]);
    exprs.push(function.clone());
    assert_eq!(remained(&exprs, StoreType::TiKv), NONE);
    exprs.clear();
    // when conv a column with type BIT, a cast function will be used to cast bit to a binary string
    let mut cast_tp = FieldType::new(FieldTypeCode::String);
    cast_tp.set_charset_name("binary");
    cast_tp.set_collation_name("binary");
    let cast_byte_as_string_func = func("cast", cast_tp.clone(), vec![col(&byte_column)]);
    let function = func("conv", FieldType::new(FieldTypeCode::String), vec![cast_byte_as_string_func.clone(), col(&int_column), col(&int_column)]);
    exprs.push(function.clone());
    assert_eq!(pushed(&exprs, StoreType::TiKv), NONE);

    // Test exprs that can be pushed.
    exprs.clear();

    for expr_name in ["substr", "substring", "mid"] {
        exprs.push(func(expr_name, FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int_column), col(&int_column)]));
    }

    exprs.push(func("char_length", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]));
    exprs.push(func("sin", FieldType::new(FieldTypeCode::Double), vec![col(&int_column)]));
    exprs.push(func("asin", FieldType::new(FieldTypeCode::Double), vec![col(&int_column)]));
    exprs.push(func("cos", FieldType::new(FieldTypeCode::Double), vec![col(&int_column)]));
    exprs.push(func("acos", FieldType::new(FieldTypeCode::Double), vec![col(&int_column)]));
    exprs.push(func("atan", FieldType::new(FieldTypeCode::Double), vec![col(&int_column)]));
    exprs.push(func("cot", FieldType::new(FieldTypeCode::Double), vec![col(&int_column)]));
    exprs.push(func("atan2", FieldType::new(FieldTypeCode::Double), vec![col(&int_column), col(&int_column)]));
    exprs.push(func("date_format", FieldType::new(FieldTypeCode::Date), vec![col(&date_column), col(&string_column)]));
    exprs.push(func("hour", FieldType::new(FieldTypeCode::Date), vec![col(&date_column)]));
    exprs.push(func("minute", FieldType::new(FieldTypeCode::Date), vec![col(&date_column)]));
    exprs.push(func("second", FieldType::new(FieldTypeCode::Date), vec![col(&date_column)]));
    exprs.push(func("month", FieldType::new(FieldTypeCode::Date), vec![col(&date_column)]));
    exprs.push(func("microsecond", FieldType::new(FieldTypeCode::Date), vec![col(&date_column)]));
    exprs.push(func("pi", FieldType::new(FieldTypeCode::Double), vec![]));
    exprs.push(func("round", FieldType::new(FieldTypeCode::Double), vec![col(&int_column)]));
    //{
    //	functionName: ast.Truncate,
    //	retType:      types.NewFieldType(mysql.TypeDouble),
    //	args:         []Expression{intColumn, intColumn},
    //},
    exprs.push(func("date", FieldType::new(FieldTypeCode::Date), vec![col(&date_column)]));
    exprs.push(func("week", FieldType::new(FieldTypeCode::Date), vec![col(&date_column)]));
    exprs.push(func("datediff", FieldType::new(FieldTypeCode::Date), vec![col(&date_column), col(&date_column)]));
    exprs.push(func("mod", FieldType::new(FieldTypeCode::Int24), vec![col(&int_column), col(&int_column)]));
    exprs.push(func("upper", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]));
    exprs.push(func("lower", FieldType::new(FieldTypeCode::String), vec![col(&string_column)]));
    exprs.push(func("pow", FieldType::new(FieldTypeCode::Double), vec![col(&real_column), col(&real_column)]));
    exprs.push(func("power", FieldType::new(FieldTypeCode::Double), vec![col(&real_column), col(&real_column)]));
    exprs.push(func("json_replace", FieldType::new(FieldTypeCode::Json), vec![col(&json_column), col(&string_column), col(&json_column), col(&string_column), col(&json_column)]));
    exprs.push(func("json_array_append", FieldType::new(FieldTypeCode::Json), vec![col(&json_column), col(&string_column), col(&json_column), col(&string_column), col(&json_column)]));
    exprs.push(func("json_merge_patch", FieldType::new(FieldTypeCode::Json), vec![col(&json_column), col(&json_column), col(&json_column)]));
    exprs.push(func("date_add", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&string_column), new_str_const("second")]));
    exprs.push(func("date_add", FieldType::new(FieldTypeCode::String), vec![col(&decimal_column), col(&real_column), new_str_const("day")]));
    exprs.push(func("date_add", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column), col(&int_column), new_str_const("year")]));
    exprs.push(func("date_add", FieldType::new(FieldTypeCode::Duration), vec![col(&duration_column), col(&string_column), new_str_const("minute")]));
    exprs.push(func("date_add", FieldType::new(FieldTypeCode::Datetime), vec![col(&duration_column), col(&string_column), new_str_const("year_month")]));
    exprs.push(func("date_sub", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int_column), new_str_const("microsecond")]));
    exprs.push(func("date_sub", FieldType::new(FieldTypeCode::String), vec![col(&int_column), col(&real_column), new_str_const("day")]));
    exprs.push(func("date_sub", FieldType::new(FieldTypeCode::Datetime), vec![col(&datetime_column), col(&int_column), new_str_const("quarter")]));
    exprs.push(func("date_sub", FieldType::new(FieldTypeCode::Duration), vec![col(&duration_column), col(&string_column), new_str_const("hour")]));
    exprs.push(func("date_sub", FieldType::new(FieldTypeCode::Datetime), vec![col(&duration_column), col(&string_column), new_str_const("year_month")]));
    exprs.push(func("adddate", FieldType::new(FieldTypeCode::Datetime), vec![col(&duration_column), col(&string_column), new_str_const("WEEK")]));
    exprs.push(func("subdate", FieldType::new(FieldTypeCode::String), vec![col(&string_column), col(&int_column), new_str_const("hour")]));
    exprs.push(func("from_unixtime", FieldType::new(FieldTypeCode::Datetime), vec![col(&decimal_column)]));
    exprs.push(func("from_unixtime", FieldType::new(FieldTypeCode::String), vec![col(&decimal_column), col(&string_column)]));
    //{
    //	functionName: ast.StrToDate,
    //	retType:      types.NewFieldType(mysql.TypeDatetime),
    //	args:         []Expression{stringColumn, stringColumn},
    //},
    //{
    //	functionName: ast.StrToDate,
    //	retType:      types.NewFieldType(mysql.TypeDuration),
    //	args:         []Expression{stringColumn, NewStrConst("%h")},
    //},
    //{
    //	functionName: ast.StrToDate,
    //	retType:      types.NewFieldType(mysql.TypeDate),
    //	args:         []Expression{stringColumn, NewStrConst("%y")},
    //},
    //{
    //	functionName: ast.StrToDate,
    //	retType:      types.NewFieldType(mysql.TypeDatetime),
    //	args:         []Expression{stringColumn, NewStrConst("%h%y")},
    //},
    exprs.push(func("timestampdiff", FieldType::new(FieldTypeCode::Long), vec![new_str_const("Second"), col(&datetime_column), col(&datetime_column)]));
    exprs.push(func("timestampdiff", FieldType::new(FieldTypeCode::Long), vec![new_str_const("DAY"), col(&datetime_column), col(&datetime_column)]));
    exprs.push(func("timestampdiff", FieldType::new(FieldTypeCode::Long), vec![new_str_const("year"), col(&datetime_column), col(&datetime_column)]));
    exprs.push(func("unix_timestamp", FieldType::new(FieldTypeCode::Long), vec![col(&datetime_column)]));
    exprs.push(func("unix_timestamp", FieldType::new(FieldTypeCode::NewDecimal), vec![col(&string_column)]));

    assert_eq!(remained(&exprs, StoreType::TiKv), NONE);
}

/// Go `TestExprOnlyPushDownToTiKV`.
#[test]
#[allow(unused_mut, unused_variables)]
fn expr_only_push_down_to_tikv() {
    let mut exprs: Vec<Expression> = Vec::new();
    let function = func("uuid", FieldType::new(FieldTypeCode::LongLong), vec![]);
    exprs.push(function.clone());

    assert_eq!(remained(&exprs, StoreType::Unspecified), NONE);

    assert!(!can_exprs_push_down(&exprs, StoreType::TiFlash, &Default::default()));
    assert_eq!(remained(&exprs, StoreType::TiKv), NONE);

    assert_eq!(pushed(&exprs, StoreType::TiFlash), NONE);
    assert_eq!(remained(&exprs, StoreType::TiKv), NONE);
}

/// Go `CanExprsPushDownWithExtraInfo` and `PushDownExprsWithExtraInfo` over
/// the columns enum, enum, bigint and set (unique ids 1 to 4), for TiFlash: without `canEnumPush`
/// both enums and the set are refused, each with its warning; with it only
/// the set is. TiKV takes the enum either way.
#[test]
fn can_enum_push_admits_an_enum_to_tiflash() {
    let column = |tp: FieldTypeCode, id: i64| {
        let mut column = gen_column(tp, id);
        column.unique_id = id;
        col(&column)
    };
    let exprs = [
        column(FieldTypeCode::Enum, 1),
        column(FieldTypeCode::Enum, 2),
        column(FieldTypeCode::LongLong, 3),
        column(FieldTypeCode::Set, 4),
    ];
    let refused = |can_enum_push: bool| {
        let warnings = std::cell::RefCell::new(Vec::new());
        let can = can_exprs_push_down_with_extra_info(
            &exprs,
            StoreType::TiFlash,
            &Default::default(),
            can_enum_push,
            &|warning| warnings.borrow_mut().push(warning),
        );
        assert!(!can);
        warnings.into_inner()
    };
    let warning = |column: i64, tp: &str| {
        format!(
            "Expression about 'Column#{column}' can not be pushed to TiFlash because it contains unsupported calculation of type '{tp}'."
        )
    };
    assert_eq!(refused(false), [warning(1, "enum"), warning(2, "enum"), warning(4, "set")]);
    assert_eq!(refused(true), [warning(4, "set")]);

    for (can_enum_push, want) in [(false, (1, 3)), (true, (3, 1))] {
        let (pushed, remained) = push_down_exprs_with_extra_info(
            exprs.to_vec(),
            StoreType::TiFlash,
            &Default::default(),
            can_enum_push,
            &|_| {},
        );
        assert_eq!((pushed.len(), remained.len()), want, "canEnumPush={can_enum_push}");
        assert!(can_exprs_push_down_with_extra_info(
            &exprs[..1],
            StoreType::TiKv,
            &Default::default(),
            can_enum_push,
            &|_| {},
        ));
    }
}
