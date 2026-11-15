// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::*;
use crate::{column::Column as ExprColumn, NoColumns};
use tidb_datatype::FieldTypeFlags;

fn column(index: i64, field: &FieldType) -> Expression {
    let mut column = ExprColumn::new(index + 1, field.clone());
    column.index = index;
    Expression::Column(column)
}

fn numeric(value: i64, field: &FieldType) -> Datum {
    match field.eval_type() {
        EvalType::Int if field.is_unsigned() => Datum::UInt(value.unsigned_abs()),
        EvalType::Int => Datum::Int(value),
        EvalType::Real => Datum::Real(value as f64 / 10.0),
        EvalType::Decimal => Datum::Decimal(Decimal::parse_mysql(&format!("{}.25", value)).0),
        _ => unreachable!(),
    }
}

/// Go `TestVectorizedBuiltinArithmeticFunc`, `vecBuiltinArithmeticCases`:
/// all six operations, their three domains and four integer flag combinations.
#[test]
fn test_vectorized_builtin_arithmetic_func() {
    for code in [
        FieldTypeCode::LongLong,
        FieldTypeCode::Double,
        FieldTypeCode::NewDecimal,
    ] {
        for unsigned in [[false, false], [false, true], [true, false], [true, true]] {
            let fields = unsigned.map(|unsigned| {
                let mut field = FieldType::new(code);
                if unsigned {
                    field.add_flags(FieldTypeFlags::UNSIGNED);
                }
                if code == FieldTypeCode::NewDecimal {
                    field.set_decimal(2);
                    field.set_flen(20);
                }
                field
            });
            for name in ["plus", "minus", "mul", "div", "intdiv", "mod"] {
                let expression = crate::new_function::new_function_base(
                    &NoColumns,
                    name,
                    FieldType::new(code),
                    vec![column(0, &fields[0]), column(1, &fields[1])],
                )
                .unwrap();
                let result_type = expression.static_type().unwrap();
                let mut input = Chunk::new_chunk_with_capacity(&fields, 1024);
                for i in 0..1024 {
                    // Go uses range generators, keeping unsigned subtraction
                    // nonnegative and divisors nonzero in the agreement cases.
                    for j in 0..2 {
                        let number = if j == 0 {
                            500 + i as i64 % 101
                        } else {
                            1 + i as i64 % 97
                        };
                        let value = if (i + j * 3) % 11 == 0 {
                            Datum::Null
                        } else {
                            numeric(number, &fields[j])
                        };
                        input.append_datum(j, &value);
                    }
                }
                for selection in [None, Some(vec![1023, 0, 7, 7, 11, 200]), Some(vec![])] {
                    input.set_sel(selection);
                    let mut result = Column::default();
                    eval_into(
                        &expression,
                        result_type.eval_type(),
                        &NoColumns,
                        &input,
                        &mut result,
                    )
                    .unwrap();
                    for row in 0..input.num_rows() {
                        let actual =
                            value(&result, row, result_type.eval_type(), Some(result_type));
                        let expected = expression.eval(&NoColumns, input.get_row(row)).unwrap();
                        assert_eq!(actual, expected, "{name} {code:?} {unsigned:?} row {row}");
                    }
                }
            }
        }
    }
}

/// Go `TestVectorizedDecimalErrOverflow`: the native 81-digit decimal range
/// is independent of the SQL result type's maximum declared precision.
#[test]
fn test_vectorized_decimal_err_overflow() {
    let mut field = FieldType::new(FieldTypeCode::NewDecimal);
    field.set_flen(65);
    field.set_decimal(0);
    for (name, right) in [
        ("plus", "81e79"),
        ("minus", "-81e79"),
        ("mul", "81e79"),
        ("div", "0.1"),
    ] {
        let expression = crate::new_function::new_function_base(
            &NoColumns,
            name,
            field.clone(),
            vec![column(0, &field), column(1, &field)],
        )
        .unwrap();
        let mut input = Chunk::new_chunk_with_capacity(&[field.clone(), field.clone()], 1);
        input.append_datum(0, &Datum::Decimal(Decimal::parse_mysql("81e79").0));
        input.append_datum(1, &Datum::Decimal(Decimal::parse_mysql(right).0));
        let error = eval_into(
            &expression,
            EvalType::Decimal,
            &NoColumns,
            &input,
            &mut Column::default(),
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                EvalError::DataOutOfRange {
                    value: "DECIMAL",
                    ..
                }
            ),
            "{name}: {error:?}"
        );
    }
}

fn types() -> Vec<FieldType> {
    [
        FieldTypeCode::LongLong,
        FieldTypeCode::Double,
        FieldTypeCode::NewDecimal,
        FieldTypeCode::VarString,
        FieldTypeCode::Datetime,
        FieldTypeCode::Duration,
        FieldTypeCode::Json,
    ]
    .map(|code| {
        let mut field = FieldType::new(code);
        field.set_decimal(0);
        if code == FieldTypeCode::VarString {
            field.set_charset_name("utf8mb4");
            field.set_collation_name("utf8mb4_bin");
        }
        field
    })
    .to_vec()
}

/// Go's vec agreement tests read a duration column's `GoDurations()` and
/// compare `val.Duration` only (a duration cell has no fsp), and a string
/// column's `GetString(i)`.
fn go_vec(value: Datum) -> Datum {
    match value {
        Datum::Duration(duration) => {
            Datum::Duration(tidb_datatype::MySqlDuration::from_raw_parts(duration.nanoseconds(), 0))
        }
        // `GetString(i)` against the row's string: bytes, no collation.
        Datum::String(value) => Datum::Bytes(value.bytes().to_vec()),
        value => value,
    }
}

fn sample(field: &FieldType, i: usize) -> Datum {
    use tidb_datatype::{BinaryJSON, MySqlDuration, Time, TimeType};
    if i % 7 == 0 {
        return Datum::Null;
    }
    match field.eval_type() {
        EvalType::Int => Datum::Int(i as i64 % 3 - 1),
        EvalType::Real => Datum::Real(i as f64 / 7.0),
        EvalType::Decimal => Datum::Decimal(Decimal::from_int(i as i64)),
        EvalType::String => Datum::new_string_datum(format!("item-{}", i % 13)),
        EvalType::Datetime => Datum::Time(
            Time::from_date_checked(
                2020,
                1,
                1 + (i % 28) as i32,
                0,
                0,
                0,
                0,
                // Go's vec test generators stamp the field's own time type.
                match field.code() {
                    FieldTypeCode::Date => TimeType::Date,
                    FieldTypeCode::Timestamp => TimeType::Timestamp,
                    _ => TimeType::DateTime,
                },
                0,
            )
            .unwrap(),
        ),
        EvalType::Duration => {
            Datum::Duration(MySqlDuration::new((i % 24) as i64, 0, 0, 0, 0).unwrap())
        }
        EvalType::Json => Datum::Json(BinaryJSON::parse(&format!("[{}]", i % 9)).unwrap()),
        EvalType::VectorFloat32 => {
            Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![
                i as f32, 2.0, 3.0,
            ]))
        }
        _ => unreachable!(),
    }
}

fn agreement(name: &str, fields: &[FieldType]) {
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        name,
        FieldType::new(FieldTypeCode::LongLong),
        fields
            .iter()
            .enumerate()
            .map(|(i, ft)| column(i as i64, ft))
            .collect(),
    )
    .unwrap();
    let Expression::ScalarFunction(function) = &expression else {
        panic!("unfolded signature");
    };
    assert!(has_kernel(function), "{name}: missing Go vector kernel");
    let field = expression.static_type().unwrap();
    let mut input = Chunk::new_chunk_with_capacity(fields, 128);
    if fields.is_empty() {
        input.set_num_virtual_rows(128);
    }
    for i in 0..128 {
        for (j, field) in fields.iter().enumerate() {
            input.append_datum(j, &sample(field, i + j * 3));
        }
    }
    let mut result = Column::default();
    for selection in [None, Some(vec![127, 3, 0, 7, 7]), Some(vec![])] {
        input.set_sel(selection);
        eval_into(
            &expression,
            field.eval_type(),
            &NoColumns,
            &input,
            &mut result,
        )
        .unwrap();
        for row in 0..input.num_rows() {
            assert_eq!(
                go_vec(value(&result, row, field.eval_type(), Some(field))),
                go_vec(expression.eval(&NoColumns, input.get_row(row)).unwrap()),
                "{name} {fields:?} row {row}"
            );
        }
    }
}

/// Go `TestVectorizedGeneratedBuiltinCompareFunc` and
/// `TestVectorizedBuiltinCompareFunc`: all comparison domains and NULL equality.
#[test]
fn test_vectorized_generated_builtin_compare_func() {
    for field in types() {
        for name in ["eq", "ne", "lt", "le", "gt", "ge", "nulleq"] {
            agreement(name, &[field.clone(), field.clone()]);
        }
    }
}

/// Go `TestVectorizedBuiltinOtherFuncGenerated`: IN over typed columns.
#[test]
fn test_vectorized_builtin_other_func_generated() {
    for field in types() {
        agreement("in", &vec![field; 5]);
    }
}

/// Go `TestVectorizedBuiltinControlFuncGenerated`: IF, IFNULL and both CASE
/// arities over every generated return domain; Coalesce from compare cases.
#[test]
fn test_vectorized_builtin_control_func_generated() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    for field in types() {
        agreement("if", &[int.clone(), field.clone(), field.clone()]);
        agreement("ifnull", &[field.clone(), field.clone()]);
        agreement("coalesce", &vec![field.clone(); 4]);
        for else_branch in [false, true] {
            let mut fields = vec![int.clone(), field.clone(), int.clone(), field.clone()];
            if else_branch {
                fields.push(field.clone());
            }
            agreement("case", &fields);
        }
    }
}

/// Go `TestVectorizedBuiltinOpFunc`: nullable logical truth tables, bitwise
/// operators, and typed IS NULL/IS TRUE/IS FALSE/NOT predicates.
#[test]
fn test_vectorized_builtin_op_func() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    for name in [
        "and",
        "or",
        "xor",
        "bitand",
        "bitor",
        "bitxor",
        "leftshift",
        "rightshift",
    ] {
        agreement(name, &[int.clone(), int.clone()]);
    }
    for field in types() {
        agreement("isnull", &[field.clone()]);
        if matches!(
            field.eval_type(),
            EvalType::Int | EvalType::Real | EvalType::Decimal
        ) {
            for name in ["not", "istrue", "isfalse", "istrue_with_null", "unaryminus"] {
                agreement(name, &[field.clone()]);
            }
        }
    }
}

/// Go `TestVectorizedCastStringAsDecimalWithUnsignedFlagInUnion`.
#[test]
fn test_vectorized_cast_string_as_decimal_with_unsigned_flag_in_union() {
    let source = FieldType::new(FieldTypeCode::VarString);
    let mut target = FieldType::new(FieldTypeCode::NewDecimal);
    target.add_flags(FieldTypeFlags::UNSIGNED);
    target.set_flen(20);
    target.set_decimal(4);
    let expression = crate::builtin_cast::build_cast_function_with_check(
        &crate::builtin::ColumnsBuildContext(&NoColumns),
        column(0, &source),
        target.clone(),
        true,
        false,
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[source], 128);
    for i in 0..128 {
        input.append_bytes(0, format!("{}.125", i as i64 - 64).as_bytes());
    }
    let mut result = Column::default();
    eval_into(
        &expression,
        EvalType::Decimal,
        &NoColumns,
        &input,
        &mut result,
    )
    .unwrap();
    for i in 0..128 {
        assert_eq!(
            value(&result, i, EvalType::Decimal, Some(&target)),
            expression.eval(&NoColumns, input.get_row(i)).unwrap()
        );
    }
}

/// Go `TestVectorizedBuiltinMathFunc`: nullable input columns for each math
/// signature; the existing scalar tests pin the per-cell transcendental math.
#[test]
fn test_vectorized_builtin_math_func() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let real = FieldType::new(FieldTypeCode::Double);
    let dec = FieldType::new(FieldTypeCode::NewDecimal);
    for field in [&int, &real, &dec] {
        for name in ["abs", "ceil", "floor", "round"] {
            agreement(name, &[field.clone()]);
        }
        for name in ["round", "truncate"] {
            agreement(name, &[field.clone(), int.clone()]);
        }
    }
    for name in [
        "sqrt", "sin", "cos", "tan", "atan", "degrees", "radians", "sign", "exp", "acos", "asin",
        "cot", "log", "log2", "log10",
    ] {
        agreement(name, &[real.clone()]);
    }
    for name in ["pow", "atan2"] {
        agreement(name, &[real.clone(), real.clone()]);
    }
    agreement("pi", &[]);
}

/// Go `TestVectorizedBuiltinStringFunc` and generated FIELD cases.
#[test]
fn test_vectorized_builtin_string_func() {
    let text = types()
        .into_iter()
        .find(|f| f.eval_type() == EvalType::String)
        .unwrap();
    let int = FieldType::new(FieldTypeCode::LongLong);
    for name in [
        "length",
        "char_length",
        "bit_length",
        "upper",
        "lower",
        "reverse",
        "hex",
        "unhex",
        "quote",
        "ascii",
        "ord",
        "ltrim",
        "rtrim",
        "trim",
        "to_base64",
        "from_base64",
    ] {
        agreement(name, &[text.clone()]);
    }
    for name in ["left", "right", "repeat", "substring"] {
        agreement(name, &[text.clone(), int.clone()]);
    }
    for name in [
        "strcmp",
        "find_in_set",
        "instr",
        "locate",
        "concat",
        "concat_ws",
    ] {
        agreement(name, &[text.clone(), text.clone()]);
    }
    for name in ["lpad", "rpad"] {
        agreement(name, &[text.clone(), int.clone(), text.clone()]);
    }
    for name in ["replace", "translate", "field"] {
        agreement(name, &vec![text.clone(); 3]);
    }
    agreement("elt", &[int.clone(), text.clone(), text.clone()]);
    agreement(
        "substring_index",
        &[text.clone(), text.clone(), int.clone()],
    );
    agreement("locate", &[text.clone(), text.clone(), int.clone()]);
    for name in ["space", "bin", "oct"] {
        agreement(name, &[int.clone()]);
    }
}

/// Go `TestVectorizedBuiltinCompareFunc`: extrema and INTERVAL typed columns.
#[test]
fn test_vectorized_builtin_compare_func() {
    for field in types()
        .into_iter()
        .filter(|f| f.eval_type() != EvalType::Json)
    {
        for name in ["greatest", "least"] {
            for count in 2..6 {
                agreement(name, &vec![field.clone(); count]);
            }
        }
    }
    for field in [
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::Double),
    ] {
        agreement("interval", &vec![field; 4]);
    }
}

/// Go `TestVectorizedBuiltinJSONFunc`: nullable document and scalar columns.
#[test]
fn test_vectorized_builtin_json_func() {
    let json = FieldType::new(FieldTypeCode::Json);
    let string = types().remove(3);
    for name in [
        "json_type",
        "json_unquote",
        "json_depth",
        "json_keys",
        "json_pretty",
        "json_storage_free",
        "json_storage_size",
        "json_length",
    ] {
        agreement(name, &[json.clone()]);
    }
    for name in [
        "json_merge",
        "json_merge_preserve",
        "json_merge_patch",
        "json_contains",
        "json_overlaps",
    ] {
        agreement(name, &[json.clone(), json.clone()]);
    }
    agreement("json_quote", &[string]);
    agreement("json_array", &[json.clone(), json]);
}

#[derive(Default)]
struct InformationContext {
    database_reads: std::cell::Cell<usize>,
    insert_ids: std::cell::RefCell<Vec<u64>>,
}

impl Columns for InformationContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn current_database(&self) -> Option<String> {
        self.database_reads.set(self.database_reads.get() + 1);
        Some("test".into())
    }
    fn set_last_insert_id(&self, value: u64) {
        self.insert_ids.borrow_mut().push(value);
    }
}

/// Go `TestVectorizedBuiltinInfoFunc`: one snapshot per batch and
/// `builtinLastInsertIDWithIDSig.vecEvalInt`'s last-non-NULL single write.
#[test]
fn test_vectorized_builtin_info_func() {
    let ctx = InformationContext::default();
    let int = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_chunk_with_capacity(&[int.clone()], 4);
    for value in [Datum::Int(3), Datum::Null, Datum::Int(9), Datum::Null] {
        input.append_datum(0, &value);
    }
    let expression = crate::new_function::new_function_base(
        &ctx,
        "last_insert_id",
        int.clone(),
        vec![column(0, &int)],
    )
    .unwrap();
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &ctx, &input, &mut result).unwrap();
    assert_eq!(*ctx.insert_ids.borrow(), [9]);
    assert_eq!(result.get_int64(0), 3);
    assert!(result.is_null(1));
    let database = crate::new_function::new_function_base(
        &ctx,
        "database",
        FieldType::new(FieldTypeCode::VarString),
        vec![],
    )
    .unwrap();
    ctx.database_reads.set(0);
    eval_into(&database, EvalType::String, &ctx, &input, &mut result).unwrap();
    assert_eq!(ctx.database_reads.get(), 1);
    for i in 0..4 {
        assert_eq!(result.get_bytes(i).as_ref(), b"test");
    }
}

/// Go `TestVectorizedBuiltinLikeFunc`.
#[test]
fn test_vectorized_builtin_like_func() {
    let string = types().remove(3);
    let int = FieldType::new(FieldTypeCode::LongLong);
    agreement("like", &[string.clone(), string, int]);
}

/// Go `TestVectorizedBuiltinVecFunc`.
#[test]
fn test_vectorized_builtin_vec_func() {
    let field = FieldType::new(FieldTypeCode::VectorFloat32);
    for name in [
        "vec_dims",
        "vec_l2_norm",
        "vec_as_text",
        "vec_l1_distance",
        "vec_l2_distance",
        "vec_negative_inner_product",
        "vec_cosine_distance",
    ] {
        agreement(
            name,
            &vec![
                field.clone();
                if matches!(name, "vec_dims" | "vec_l2_norm" | "vec_as_text") {
                    1
                } else {
                    2
                }
            ],
        );
    }
}

/// Go `TestVectorizedBuiltinEncryptionFunc`: nullable byte strings and keys.
#[test]
fn test_vectorized_builtin_encryption_func() {
    let string = types().remove(3);
    let int = FieldType::new(FieldTypeCode::LongLong);
    for name in [
        "md5",
        "sha1",
        "sm3",
        "compress",
        "uncompress",
        "uncompressed_length",
    ] {
        agreement(name, &[string.clone()]);
    }
    for name in ["aes_encrypt", "aes_decrypt", "encode", "decode"] {
        agreement(name, &[string.clone(), string.clone()]);
    }
    agreement("sha2", &[string, int]);
}

/// Go `TestVectorizedBuiltinTimeFunc`: date and duration extraction kernels.
#[test]
fn test_vectorized_builtin_time_func() {
    let date = types().remove(4);
    let duration = types().remove(5);
    let int = FieldType::new(FieldTypeCode::LongLong);
    let real = FieldType::new(FieldTypeCode::Double);
    for name in [
        "month",
        "year",
        "dayofmonth",
        "dayofweek",
        "dayofyear",
        "week",
        "weekofyear",
        "yearweek",
        "weekday",
        "quarter",
        "dayname",
        "monthname",
        "last_day",
        "date",
        "to_days",
        "to_seconds",
        "unix_timestamp",
    ] {
        agreement(name, &[date.clone()]);
    }
    for name in ["hour", "minute", "second", "microsecond", "time_to_sec"] {
        agreement(name, &[duration.clone()]);
    }
    for name in ["week", "yearweek"] {
        agreement(name, &[date.clone(), int.clone()]);
    }
    agreement("datediff", &[date.clone(), date]);
    agreement("from_days", &[int.clone()]);
    agreement("makedate", &[int.clone(), int.clone()]);
    agreement("maketime", &[int.clone(), int, real.clone()]);
    agreement("sec_to_time", &[real]);
}

/// Go `TestVectorizedBuiltinMiscellaneousFunc`: typed delegation and addresses.
#[test]
fn test_vectorized_builtin_miscellaneous_func() {
    for field in types() {
        agreement("any_value", &[field]);
    }
    let string = types().remove(3);
    let int = FieldType::new(FieldTypeCode::LongLong);
    for name in [
        "is_ipv4",
        "is_ipv6",
        "is_ipv4_compat",
        "is_ipv4_mapped",
        "is_uuid",
        "inet6_ntoa",
    ] {
        agreement(name, &[string.clone()]);
    }
    for name in ["inet_ntoa", "vitess_hash"] {
        agreement(name, &[int.clone()]);
    }
}

/// Go `TestVectorizedBuiltinOtherFunc`: BIT_COUNT's typed column kernel.
#[test]
fn test_vectorized_builtin_other_func() {
    agreement("bit_count", &[FieldType::new(FieldTypeCode::LongLong)]);
}

/// Go `TestVectorizedBuiltinTimeFunc`: ADD/SUB DATE argument domains and units.
#[test]
fn test_vectorized_builtin_date_arithmetic() {
    use crate::constant::Constant;
    // Go's test context has a statement timestamp: a DURATION date with a
    // non-clock unit converts through it (`Duration.ConvertToTime`).
    struct StmtClock;
    impl Columns for StmtClock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            Some((1_700_000_000, 123_456_000, 0))
        }
    }
    let dates = [
        FieldTypeCode::Date,
        FieldTypeCode::Datetime,
        FieldTypeCode::Duration,
    ];
    let amounts = [
        FieldTypeCode::LongLong,
        FieldTypeCode::Double,
        FieldTypeCode::NewDecimal,
        FieldTypeCode::VarString,
    ];
    for date_code in dates {
        let mut date = FieldType::new(date_code);
        date.set_decimal(0);
        for amount_code in amounts {
            let mut amount = FieldType::new(amount_code);
            amount.set_decimal(0);
            for unit in ["DAY", "HOUR", "SECOND", "MINUTE_SECOND"] {
                for name in ["date_add", "date_sub"] {
                    let expression = crate::new_function::new_function_base(
                        &NoColumns,
                        name,
                        FieldType::new(FieldTypeCode::Datetime),
                        vec![
                            column(0, &date),
                            column(1, &amount),
                            Expression::Constant(Constant::new(
                                Datum::new_string_datum(unit),
                                FieldType::new(FieldTypeCode::VarString),
                            )),
                        ],
                    )
                    .unwrap();
                    let result_field = expression.static_type().unwrap();
                    let mut input =
                        Chunk::new_chunk_with_capacity(&[date.clone(), amount.clone()], 16);
                    for row in 0..16 {
                        input.append_datum(0, &sample(&date, row));
                        let value = match amount.eval_type() {
                            EvalType::Int => Datum::Int(2),
                            EvalType::Real => Datum::Real(2.0),
                            EvalType::Decimal => Datum::Decimal(Decimal::from_int(2)),
                            _ => Datum::new_string_datum("2"),
                        };
                        input.append_datum(1, &value);
                    }
                    let mut result = Column::default();
                    eval_into(
                        &expression,
                        result_field.eval_type(),
                        &StmtClock,
                        &input,
                        &mut result,
                    )
                    .unwrap();
                    for row in 0..input.num_rows() {
                        assert_eq!(
                            go_vec(value(&result, row, result_field.eval_type(), Some(result_field))),
                            go_vec(expression.eval(&StmtClock, input.get_row(row)).unwrap()),
                            "{name} {date_code:?} {amount_code:?} {unit} row {row}"
                        );
                    }
                }
            }
        }
    }
}

/// Go `TestVectorizedTimeFormatEmptyFormatReturnsNull`.
#[test]
fn test_vectorized_time_format_empty_format_returns_null() {
    let fields = [types().remove(5), types().remove(3)];
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "time_format",
        fields[1].clone(),
        vec![column(0, &fields[0]), column(1, &fields[1])],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&fields, 2);
    for (hour, minute, second, mask) in [(12, 34, 56, ""), (1, 2, 3, "%H:%i:%s")] {
        input.append_datum(
            0,
            &Datum::Duration(
                tidb_datatype::MySqlDuration::new(hour, minute, second, 0, 0).unwrap(),
            ),
        );
        input.append_bytes(1, mask.as_bytes());
    }
    let mut result = Column::default();
    eval_into(
        &expression,
        EvalType::String,
        &NoColumns,
        &input,
        &mut result,
    )
    .unwrap();
    assert!(result.is_null(0));
    assert_eq!(result.get_bytes(1).as_ref(), b"01:02:03");
}

/// Go `TestVectorizedBuiltinCastFunc`: the 7 by 7 generated cast domains,
/// with result/error agreement on each nullable source cell.
#[test]
fn test_vectorized_builtin_cast_func() {
    for source in types() {
        for mut target in types() {
            if target.eval_type() == EvalType::Decimal {
                target.set_flen(65);
                target.set_decimal(6);
            }
            let expression = crate::builtin_cast::build_cast_function_with_check(
                &crate::builtin::ColumnsBuildContext(&NoColumns),
                column(0, &source),
                target.clone(),
                false,
                false,
            )
            .unwrap();
            for row in 0..16 {
                let mut input = Chunk::new_chunk_with_capacity(&[source.clone()], 1);
                input.append_datum(0, &sample(&source, row));
                let mut result = Column::default();
                let actual = eval_into(
                    &expression,
                    target.eval_type(),
                    &NoColumns,
                    &input,
                    &mut result,
                )
                .map(|()| value(&result, 0, target.eval_type(), Some(&target)));
                let expected = expression.eval(&NoColumns, input.get_row(0));
                match (actual, expected) {
                    (Ok(actual), Ok(expected)) => assert_eq!(
                        actual,
                        expected,
                        "{:?} to {:?}, row {row}",
                        source.eval_type(),
                        target.eval_type()
                    ),
                    (Err(actual), Err(expected)) => {
                        use crate::eval_error_sql::EvalErrorSql;
                        assert_eq!(actual.to_sql_error().code, expected.to_sql_error().code);
                    }
                    (actual, expected) => panic!(
                        "{:?} to {:?}, row {row}: {actual:?} vs {expected:?}",
                        source.eval_type(),
                        target.eval_type()
                    ),
                }
            }
        }
    }
}

/// Go `TestVectorizedBuiltinInfoFunc`: BENCHMARK repeats its entire argument
/// column, and only a positive signed constant count enables its vector kernel.
#[test]
fn test_vectorized_builtin_info_benchmark() {
    use crate::constant::Constant;
    let ctx = InformationContext::default();
    let int = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_chunk_with_capacity(&[int.clone()], 3);
    for value in [1, 2, 9] {
        input.append_int64(0, value);
    }
    let effect = crate::new_function::new_function_base(
        &ctx,
        "last_insert_id",
        int.clone(),
        vec![column(0, &int)],
    )
    .unwrap();
    let expression = crate::new_function::new_function_base(
        &ctx,
        "benchmark",
        int.clone(),
        vec![
            Expression::Constant(Constant::new(Datum::Int(3), int)),
            effect,
        ],
    )
    .unwrap();
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &ctx, &input, &mut result).unwrap();
    assert_eq!(*ctx.insert_ids.borrow(), [9, 9, 9]);
    for i in 0..3 {
        assert_eq!(result.get_int64(i), 0);
        assert!(!result.is_null(i));
    }
}

/// Go `TestVectorizedBuiltinLikeFunc` includes ILIKE's constant escape path.
#[test]
fn test_vectorized_builtin_ilike_func() {
    use crate::constant::Constant;
    let string = types().remove(3);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "ilike",
        FieldType::new(FieldTypeCode::LongLong),
        vec![
            column(0, &string),
            column(1, &string),
            Expression::Constant(Constant::new(
                Datum::Int(i64::from(b'\\')),
                FieldType::new(FieldTypeCode::LongLong),
            )),
        ],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[string.clone(), string], 4);
    for (text, pattern) in [
        ("ABC", "a%"),
        ("abc_", "ABC\\_"),
        ("你好A", "你好a"),
        ("Abc", "D%"),
    ] {
        input.append_bytes(0, text.as_bytes());
        input.append_bytes(1, pattern.as_bytes());
    }
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
    for row in 0..4 {
        assert_eq!(
            Datum::Int(result.get_int64(row)),
            expression.eval(&NoColumns, input.get_row(row)).unwrap()
        );
    }
}

/// Go `TestVectorizedBuiltinStringFunc`, `builtinConcatWSSig.vecEvalString`:
/// retain the source's row-index condition when accounting for separators.
#[test]
fn test_vectorized_concat_ws_packet_accounting() {
    struct PacketLimit;
    impl Columns for PacketLimit {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn max_allowed_packet(&self) -> u64 {
            3
        }
    }
    let ctx = PacketLimit;
    let string = types().remove(3);
    let fields = vec![string.clone(); 3];
    let expression = crate::new_function::new_function_base(
        &ctx,
        "concat_ws",
        string.clone(),
        (0..3).map(|i| column(i, &string)).collect(),
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&fields, 3);
    for _ in 0..3 {
        input.append_bytes(0, b",,");
        input.append_bytes(1, b"a");
        input.append_bytes(2, b"b");
    }
    let mut result = Column::default();
    eval_into(&expression, EvalType::String, &ctx, &input, &mut result).unwrap();
    assert_eq!(result.get_bytes(0).as_ref(), b"a,,b");
    assert_eq!(result.get_bytes(1).as_ref(), b"a,,b");
    assert!(result.is_null(2));
}

/// Go `TestVectorizedBuiltinMathFuncForRand`: the constant-seed signature
/// advances one captured generator, which its Clone also shares.
#[test]
fn test_vectorized_builtin_math_func_for_rand() {
    use crate::constant::Constant;
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "rand",
        FieldType::new(FieldTypeCode::Double),
        vec![Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ))],
    )
    .unwrap();
    let copy = expression.clone();
    let expected = crate::MysqlRng::new_with_seed(1);
    let mut input = Chunk::new_chunk_with_capacity(&[], 16);
    input.set_num_virtual_rows(16);
    let mut result = Column::default();
    for expression in [&expression, &copy, &expression] {
        eval_into(expression, EvalType::Real, &NoColumns, &input, &mut result).unwrap();
        for row in 0..16 {
            assert_eq!(result.get_float64(row), expected.gen());
        }
    }
    agreement("rand", &[FieldType::new(FieldTypeCode::LongLong)]);
}

/// Go `builtinConvSig.vectorized` deliberately remains false for hybrid arguments.
#[test]
fn conv_keeps_go_non_vectorized_signature() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let string = FieldType::new(FieldTypeCode::VarString);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "conv",
        string.clone(),
        vec![column(0, &string), column(1, &int), column(2, &int)],
    )
    .unwrap();
    assert!(!crate::chunk_executor::vectorized(&expression));
}

/// Go `builtinInIntSig.vecEvalInt`: hash probes merge the input's NULL bitmap.
#[test]
fn in_constant_hash_set_preserves_input_nulls_and_first_sign_flag() {
    let field = FieldType::new(FieldTypeCode::LongLong);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "in",
        field.clone(),
        vec![
            column(0, &field),
            Expression::Constant(crate::constant::Constant::new(Datum::UInt(u64::MAX), {
                let mut ty = field.clone();
                ty.add_flags(FieldTypeFlags::UNSIGNED);
                ty
            })),
            Expression::Constant(crate::constant::Constant::new(
                Datum::Int(-1),
                field.clone(),
            )),
        ],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[field], 3);
    for v in [Datum::Null, Datum::Int(-1), Datum::Int(1)] {
        input.append_datum(0, &v);
    }
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
    assert!(result.is_null(0));
    assert_eq!(result.get_int64(1), 0);
    assert_eq!(result.get_int64(2), 0);
}

/// Go `TestVectorizedBuiltinTimeFunc`: the dedicated literal signatures retain
/// the parsed value and precision, and evaluate a full virtual input chunk.
#[test]
fn test_vectorized_builtin_time_literal_func() {
    for (name, text) in [
        ("'tidb`.(dateliteral", "2024-01-02"),
        ("'tidb`.(timeliteral", "12:34:56.120"),
        ("'tidb`.(timestampliteral", "2024-01-02 12:34:56.120"),
    ] {
        let field = FieldType::new(FieldTypeCode::VarString);
        let expression = crate::new_function::new_function_base(
            &NoColumns,
            name,
            field.clone(),
            vec![Expression::Constant(crate::constant::Constant::new(
                Datum::new_string_datum(text),
                field,
            ))],
        )
        .unwrap();
        assert!(crate::chunk_executor::vectorized(&expression));
        let Expression::ScalarFunction(function) = &expression else {
            panic!("unfolded literal signature")
        };
        assert!(function.args.is_empty());
        let target = expression.static_type().unwrap();
        let expected = expression.eval(&NoColumns, Row::empty()).unwrap();
        let mut input = Chunk::new_chunk_with_capacity(&[], 128);
        input.set_num_virtual_rows(128);
        let mut result = Column::default();
        eval_into(
            &expression,
            target.eval_type(),
            &NoColumns,
            &input,
            &mut result,
        )
        .unwrap();
        for i in 0..128 {
            assert_eq!(
                value(&result, i, target.eval_type(), Some(target)),
                expected
            );
        }
    }
}

/// Go `builtinUTCTimestampWithArgSig.vecEvalTime` merges its FSP NULL bitmap,
/// unlike `evalTime`, and the duration clock kernels read one timestamp per batch.
#[test]
fn vectorized_clock_nulls_and_snapshot_follow_go() {
    struct Clock(std::cell::Cell<usize>);
    impl Columns for Clock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn now(&self) -> Option<(i64, u32, i32)> {
            self.0.set(self.0.get() + 1);
            Some((1_700_000_000, 123_456_789, 0))
        }
    }
    let context = Clock(std::cell::Cell::new(0));
    let int = FieldType::new(FieldTypeCode::LongLong);
    let mut input = Chunk::new_chunk_with_capacity(&[int.clone()], 3);
    for datum in [Datum::Null, Datum::Int(0), Datum::Int(3)] {
        input.append_datum(0, &datum);
    }
    for name in ["utc_timestamp", "utc_time", "current_time"] {
        let expression = crate::new_function::new_function_base(
            &context,
            name,
            int.clone(),
            vec![column(0, &int)],
        )
        .unwrap();
        let field = expression.static_type().unwrap();
        context.0.set(0);
        let mut result = Column::default();
        eval_into(
            &expression,
            field.eval_type(),
            &context,
            &input,
            &mut result,
        )
        .unwrap();
        assert_eq!(result.is_null(0), name != "current_time", "{name}");
        if name != "utc_timestamp" {
            assert_eq!(context.0.get(), 1, "{name}");
        }
        for i in 1..3 {
            assert_eq!(
                go_vec(value(&result, i, field.eval_type(), Some(field))),
                go_vec(expression.eval(&context, input.get_row(i)).unwrap()),
                "{name} {i}"
            );
        }
        if name == "utc_timestamp" {
            assert!(!expression
                .eval(&context, input.get_row(0))
                .unwrap()
                .is_null());
        }
    }
}

/// Go `testVectorizedBuiltinFunc`: deterministic source-case generators,
/// complete columns, duplicate/reordered selection, and empty input.
fn agreement_rows(name: &str, fields: &[FieldType], rows: &[Vec<Datum>]) {
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        name,
        FieldType::new(FieldTypeCode::LongLong),
        fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                if matches!(name, "extract" | "timestampadd") && i == 0 {
                    Expression::Constant(crate::constant::Constant::new(
                        rows[0][i].clone(),
                        field.clone(),
                    ))
                } else {
                    column(i as i64, field)
                }
            })
            .collect(),
    )
    .unwrap();
    assert!(crate::chunk_executor::vectorized(&expression), "{name}");
    let field = expression.static_type().unwrap();
    let mut input = Chunk::new_chunk_with_capacity(fields, 128);
    for i in 0..128 {
        for (j, value) in rows[i % rows.len()].iter().enumerate() {
            input.append_datum(j, value);
        }
    }
    let mut result = Column::default();
    for selection in [None, Some(vec![127, 0, 3, 3]), Some(vec![])] {
        input.set_sel(selection);
        eval_into(
            &expression,
            field.eval_type(),
            &NoColumns,
            &input,
            &mut result,
        )
        .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        for i in 0..input.num_rows() {
            assert_eq!(
                go_vec(value(&result, i, field.eval_type(), Some(field))),
                go_vec(expression.eval(&NoColumns, input.get_row(i)).unwrap()),
                "{name} row {i}"
            );
        }
    }
}

/// Go `vecBuiltinJSONCases` path and document generators.
#[test]
fn test_vectorized_builtin_json_func_paths() {
    let json = FieldType::new(FieldTypeCode::Json);
    let string = types().remove(3);
    let document = Datum::Json(tidb_datatype::BinaryJSON::parse(r#"{"a":{"c":3},"b":2}"#).unwrap());
    let child = Datum::Json(tidb_datatype::BinaryJSON::parse(r#"{"b":2}"#).unwrap());
    let text = |s: &str| Datum::new_string_datum(s);
    for name in ["json_keys", "json_extract", "json_length", "json_remove"] {
        agreement_rows(
            name,
            &[json.clone(), string.clone()],
            &[
                vec![document.clone(), text("$.a")],
                vec![document.clone(), text("$.missing")],
                vec![Datum::Null, text("$.a")],
            ],
        );
    }
    for name in [
        "json_set",
        "json_insert",
        "json_replace",
        "json_array_append",
    ] {
        agreement_rows(
            name,
            &[
                json.clone(),
                string.clone(),
                json.clone(),
                string.clone(),
                json.clone(),
            ],
            &[
                vec![
                    document.clone(),
                    text("$.a"),
                    child.clone(),
                    text("$.x"),
                    child.clone(),
                ],
                vec![
                    Datum::Null,
                    text("$.a"),
                    child.clone(),
                    text("$.x"),
                    Datum::Null,
                ],
            ],
        );
    }
    let array =
        Datum::Json(tidb_datatype::BinaryJSON::parse(r#"["a",{"b":[1,2]},[3,4]]"#).unwrap());
    agreement_rows(
        "json_array_insert",
        &[json.clone(), string.clone(), json.clone()],
        &[vec![array, text("$[1]"), child.clone()]],
    );
    agreement_rows(
        "json_contains",
        &[json.clone(), json.clone(), string.clone()],
        &[vec![document.clone(), child.clone(), text("$.a")]],
    );
    agreement_rows(
        "json_contains_path",
        &[json.clone(), string.clone(), string.clone(), string.clone()],
        &[
            vec![document.clone(), text("one"), text("$.a"), text("$.x")],
            vec![document.clone(), text("all"), text("$.a"), text("$.x")],
        ],
    );
    agreement_rows(
        "json_object",
        &[string.clone(), json.clone(), string.clone(), json.clone()],
        &[vec![text("a"), child.clone(), text("b"), Datum::Null]],
    );
    let search = Datum::Json(
        tidb_datatype::BinaryJSON::parse(r#"["abc",[{"k":"10"},"def"],{"x":"abc"}]"#).unwrap(),
    );
    agreement_rows(
        "json_search",
        &[json, string.clone(), string],
        &[
            vec![search.clone(), text("one"), text("abc")],
            vec![search, text("all"), text("%a%")],
        ],
    );
}

/// Go `TestInDecimal`: equal decimals with different stored fraction widths.
#[test]
fn test_in_decimal() {
    let decimal = FieldType::new(FieldTypeCode::NewDecimal);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "in",
        FieldType::new(FieldTypeCode::LongLong),
        vec![column(0, &decimal), column(1, &decimal)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[decimal.clone(), decimal], 1024);
    for i in 0..1024 {
        let text = format!("{}.{}", i % 1000, i * 1973 + 1);
        input.append_datum(0, &Datum::Decimal(Decimal::parse_mysql(&text).0));
        input.append_datum(
            1,
            &Datum::Decimal(Decimal::parse_mysql(&format!("{text}00")).0),
        );
    }
    let mut result = Column::default();
    eval_into(&expression, EvalType::Int, &NoColumns, &input, &mut result).unwrap();
    for i in 0..1024 {
        assert!(!result.is_null(i));
        assert_eq!(result.get_int64(i), 1);
    }
}

/// Go `TestGetParamVec`: typed indices, string conversion and out-of-range error.
#[test]
fn test_get_param_vec() {
    struct Params;
    impl Columns for Params {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn get_param_value(&self, index: usize) -> Result<Datum, EvalError> {
            [Datum::Int(123), Datum::new_string_datum("abc")]
                .get(index)
                .cloned()
                .ok_or(EvalError::ParamIndexExceedParamCounts)
        }
    }
    let int = FieldType::new(FieldTypeCode::LongLong);
    let expression = crate::new_function::new_function_base(
        &Params,
        "getparam",
        int.clone(),
        vec![column(0, &int)],
    )
    .unwrap();
    let mut input = Chunk::new_chunk_with_capacity(&[int], 3);
    input.append_datum(0, &Datum::Int(0));
    input.append_datum(0, &Datum::Int(1));
    let mut result = Column::default();
    eval_into(&expression, EvalType::String, &Params, &input, &mut result).unwrap();
    assert_eq!(result.get_bytes(0).as_ref(), b"123");
    assert_eq!(result.get_bytes(1).as_ref(), b"abc");
    input.append_datum(0, &Datum::Int(2));
    assert!(eval_into(&expression, EvalType::String, &Params, &input, &mut result).is_err());
}

/// Go `TestVectorizedBuiltinStringFunc2`: multibyte/binary signatures,
/// FORMAT, INSERT, EXPORT_SET, MAKE_SET, and FIELD's three domains.
#[test]
fn test_vectorized_builtin_string_func2() {
    let text = |s: &str| Datum::new_string_datum(s);
    let int = FieldType::new(FieldTypeCode::LongLong);
    let string = types().remove(3);
    let mut binary = string.clone();
    binary.set_charset_name("binary");
    binary.set_collation_name("binary");
    binary.add_flags(FieldTypeFlags::BINARY);
    for field in [string.clone(), binary] {
        for name in ["upper", "lower", "reverse", "char_length"] {
            agreement_rows(
                name,
                &[field.clone()],
                &[vec![text(" Ab中ß ")], vec![Datum::Null]],
            );
        }
        for name in ["left", "right", "substring"] {
            agreement_rows(
                name,
                &[field.clone(), int.clone()],
                &[
                    vec![text("a中😀z"), Datum::Int(2)],
                    vec![text("abc"), Datum::Int(-1)],
                ],
            );
        }
        agreement_rows(
            "substring",
            &[field.clone(), int.clone(), int.clone()],
            &[vec![text("a中😀z"), Datum::Int(2), Datum::Int(2)]],
        );
        // Go `ast.InsertFunc`.
        agreement_rows(
            "insert_func",
            &[field.clone(), int.clone(), int.clone(), field.clone()],
            &[vec![
                text("a中😀z"),
                Datum::Int(2),
                Datum::Int(2),
                text("new"),
            ]],
        );
        for name in ["lpad", "rpad"] {
            agreement_rows(
                name,
                &[field.clone(), int.clone(), field.clone()],
                &[vec![text("中"), Datum::Int(5), text("😀!")]],
            );
        }
        for name in ["instr", "locate"] {
            agreement_rows(
                name,
                &[field.clone(), field.clone()],
                &[vec![text("中"), text("a中😀z")]],
            );
        }
    }
    for count in 3..=5 {
        let fields = [
            int.clone(),
            string.clone(),
            string.clone(),
            string.clone(),
            int.clone(),
        ];
        let row = [
            Datum::Int(10),
            text("Y"),
            text("N"),
            text(","),
            Datum::Int(8),
        ];
        agreement_rows("export_set", &fields[..count], &[row[..count].to_vec()]);
    }
    agreement_rows(
        "make_set",
        &[int.clone(), string.clone(), string.clone()],
        &[
            vec![Datum::Int(3), text("one"), text("two")],
            vec![Datum::Int(3), Datum::Null, text("two")],
        ],
    );
    agreement_rows(
        "format",
        &[string.clone(), int.clone()],
        &[vec![text("12345.6789"), Datum::Int(2)]],
    );
    agreement_rows(
        "format",
        &[string.clone(), int.clone(), string.clone()],
        &[vec![text("12345.6789"), Datum::Int(2), text("en_US")]],
    );
    agreement_rows(
        "trim",
        &[string.clone(), string.clone()],
        &[vec![text("xyzhelloxyz"), text("xyz")]],
    );
    for field in [int, FieldType::new(FieldTypeCode::Double), string] {
        agreement("field", &vec![field; 4]);
    }
}

/// Go `vecBuiltinTimeCases` and generated add/sub/time-diff cases: typed
/// temporal combinations, unit/format columns and nullable argument rows.
#[test]
fn test_vectorized_builtin_time_func_formats_and_arithmetic() {
    let date = types().remove(4);
    let duration = types().remove(5);
    let string = types().remove(3);
    let int = FieldType::new(FieldTypeCode::LongLong);
    let text = |s: &str| Datum::new_string_datum(s);
    let datetime = sample(&date, 1);
    let time = sample(&duration, 1);
    for name in ["period_add", "period_diff"] {
        agreement_rows(
            name,
            &[int.clone(), int.clone()],
            &[
                vec![Datum::Int(200801), Datum::Int(2)],
                vec![Datum::Null, Datum::Int(0)],
            ],
        );
    }
    agreement_rows(
        "date_format",
        &[date.clone(), string.clone()],
        &[
            vec![datetime.clone(), text("%Y-%m-%d %H:%i:%s")],
            vec![Datum::Null, text("%Y")],
        ],
    );
    agreement_rows(
        "time_format",
        &[duration.clone(), string.clone()],
        &[vec![time.clone(), text("%H:%i:%s")]],
    );
    agreement_rows(
        "get_format",
        &[string.clone(), string.clone()],
        &[
            vec![text("DATE"), text("USA")],
            vec![text("DATETIME"), text("ISO")],
        ],
    );
    for (field, datum) in [
        (date.clone(), datetime.clone()),
        (duration.clone(), time.clone()),
    ] {
        agreement_rows(
            "extract",
            &[string.clone(), field],
            &[
                vec![text("HOUR"), datum.clone()],
                vec![text("SECOND"), datum],
            ],
        );
    }
    agreement_rows(
        "timestampdiff",
        &[string.clone(), date.clone(), date.clone()],
        &[vec![text("DAY"), datetime.clone(), sample(&date, 2)]],
    );
    agreement_rows(
        "timestampadd",
        &[string.clone(), int.clone(), date.clone()],
        &[vec![text("DAY"), Datum::Int(3), datetime.clone()]],
    );
    agreement_rows(
        "convert_tz",
        &[date.clone(), string.clone(), string.clone()],
        &[vec![datetime.clone(), text("+00:00"), text("+08:00")]],
    );
    for (first_field, first) in [
        (date.clone(), datetime.clone()),
        (duration.clone(), time.clone()),
        (string.clone(), text("2020-01-02 01:02:03")),
    ] {
        for (second_field, second) in [
            (duration.clone(), time.clone()),
            (string.clone(), text("01:02:03")),
        ] {
            for name in ["addtime", "subtime", "timediff"] {
                agreement_rows(
                    name,
                    &[first_field.clone(), second_field.clone()],
                    &[
                        vec![first.clone(), second.clone()],
                        vec![Datum::Null, second.clone()],
                        vec![first.clone(), Datum::Null],
                    ],
                );
            }
        }
    }
    for formats in [
        vec![text("2020-01-02"), text("%Y-%m-%d")],
        vec![text("2020-01-02 01:02:03"), text("%Y-%m-%d %H:%i:%s")],
        vec![text("01:02:03"), text("%H:%i:%s")],
    ] {
        agreement_rows("str_to_date", &[string.clone(), string.clone()], &[formats]);
    }
    agreement_rows(
        "timestamp",
        &[string.clone()],
        &[vec![text("2020-01-02 01:02:03")]],
    );
    agreement_rows(
        "timestamp",
        &[string.clone(), string.clone()],
        &[vec![text("2020-01-02"), text("01:02:03")]],
    );
    agreement_rows("time", &[string.clone()], &[vec![text("01:02:03.123456")]]);
    agreement_rows(
        "from_unixtime",
        &[int.clone()],
        &[vec![Datum::Int(1_700_000_000)]],
    );
    agreement_rows(
        "from_unixtime",
        &[int, string],
        &[vec![Datum::Int(1_700_000_000), text("%Y-%m-%d")]],
    );
}

/// Go `builtinExtractDatetimeFromStringSig.vectorized` is disabled explicitly.
#[test]
fn extract_from_string_keeps_go_non_vectorized_signature() {
    let string = types().remove(3);
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        "extract",
        FieldType::new(FieldTypeCode::LongLong),
        vec![
            Expression::Constant(crate::constant::Constant::new(
                Datum::new_string_datum("DAY_SECOND"),
                string.clone(),
            )),
            column(1, &string),
        ],
    )
    .unwrap();
    assert!(!crate::chunk_executor::vectorized(&expression));
}

/// Runs `name` over `rows` (one column per field) through its Go vector
/// kernel, answering the result column's cells.
fn vec_cells(name: &str, fields: &[FieldType], rows: &[Vec<Datum>]) -> (Expression, Vec<Datum>) {
    let expression = crate::new_function::new_function_base(
        &NoColumns,
        name,
        FieldType::new(FieldTypeCode::LongLong),
        fields
            .iter()
            .enumerate()
            .map(|(i, ft)| column(i as i64, ft))
            .collect(),
    )
    .unwrap();
    let Expression::ScalarFunction(function) = &expression else {
        panic!("unfolded signature");
    };
    assert!(has_kernel(function), "{name}: missing Go vector kernel");
    let field = expression.static_type().unwrap().clone();
    let mut input = Chunk::new_chunk_with_capacity(fields, rows.len());
    for row in rows {
        for (j, value) in row.iter().enumerate() {
            input.append_datum(j, value);
        }
    }
    let mut result = Column::default();
    eval_into(&expression, field.eval_type(), &NoColumns, &input, &mut result).unwrap();
    let cells = (0..rows.len())
        .map(|i| go_vec(value(&result, i, field.eval_type(), Some(&field))))
        .collect();
    (expression, cells)
}

fn text_field() -> FieldType {
    let mut field = FieldType::new(FieldTypeCode::VarString);
    field.set_charset_name("utf8mb4");
    field.set_collation_name("utf8mb4_bin");
    field
}

fn row_of(values: &[Datum]) -> Chunk {
    let fields: Vec<_> = values
        .iter()
        .map(|value| match value {
            Datum::Int(_) => FieldType::new(FieldTypeCode::LongLong),
            _ => text_field(),
        })
        .collect();
    let mut chunk = Chunk::new_chunk_with_capacity(&fields, 1);
    for (j, value) in values.iter().enumerate() {
        chunk.append_datum(j, value);
    }
    chunk
}

/// Go `builtinInetAtonSig.vecEvalInt` answers NULL for a malformed address
/// where `evalInt` answers `errWrongValueForType` (1411).
#[test]
fn inet_aton_vector_kernel_nulls_what_the_row_form_refuses() {
    let text = text_field();
    let rows: Vec<Vec<Datum>> = ["", "0.0.0.256", "127.0.0.1", "123.2.1."]
        .iter()
        .map(|s| vec![Datum::new_string_datum(*s)])
        .collect();
    let (expression, cells) = vec_cells("inet_aton", &[text], &rows);
    assert_eq!(cells[0], Datum::Null);
    assert_eq!(cells[1], Datum::Null);
    assert_eq!(cells[3], Datum::Null);
    assert!(matches!(cells[2], Datum::UInt(2130706433) | Datum::Int(2130706433)), "{:?}", cells[2]);
    let row = row_of(&[Datum::new_string_datum("0.0.0.256")]);
    assert!(matches!(
        expression.eval(&NoColumns, row.get_row(0)),
        Err(EvalError::WrongValueForType { .. })
    ));
}

/// Go `builtinInet6AtonSig.vecEvalString`: an empty or unparseable address is
/// NULL; `evalString` answers `errWrongValueForType`.
#[test]
fn inet6_aton_vector_kernel_nulls_what_the_row_form_refuses() {
    let rows: Vec<Vec<Datum>> = ["", "zzz", "::1"]
        .iter()
        .map(|s| vec![Datum::new_string_datum(*s)])
        .collect();
    let (expression, cells) = vec_cells("inet6_aton", &[text_field()], &rows);
    assert_eq!(cells[0], Datum::Null);
    assert_eq!(cells[1], Datum::Null);
    let mut loopback = [0u8; 16];
    loopback[15] = 1;
    assert_eq!(cells[2], Datum::Bytes(loopback.to_vec()));
    let row = row_of(&[Datum::new_string_datum("zzz")]);
    assert!(matches!(
        expression.eval(&NoColumns, row.get_row(0)),
        Err(EvalError::WrongValueForType { .. })
    ));
}

/// Go `builtinPasswordSig.vecEvalString` hashes a NULL to "" where
/// `evalString` answers NULL.
#[test]
fn password_vector_kernel_hashes_null_to_the_empty_string() {
    let (expression, cells) = vec_cells("password", &[text_field()], &[vec![Datum::Null]]);
    assert_eq!(cells[0], Datum::Bytes(Vec::new()));
    let row = row_of(&[Datum::Null]);
    assert_eq!(expression.eval(&NoColumns, row.get_row(0)).unwrap(), Datum::Null);
}

/// Go `builtinRepeatSig.vecEvalString`: past the packet check, a cell longer
/// than `flen / num` is NULL -- a check `evalString` does not have.
#[test]
fn repeat_vector_kernel_nulls_a_cell_past_flen_over_count() {
    let int = FieldType::new(FieldTypeCode::LongLong);
    let count = 20_000_000;
    let (expression, cells) = vec_cells(
        "repeat",
        &[text_field(), int],
        &[vec![Datum::new_string_datum("a"), Datum::Int(count)]],
    );
    let flen = expression.static_type().unwrap().flen();
    assert!(flen / count < 1, "the fixture must cross flen/num: flen {flen}");
    assert_eq!(cells[0], Datum::Null);
    let row = row_of(&[Datum::new_string_datum("a"), Datum::Int(count)]);
    let value = expression.eval(&NoColumns, row.get_row(0)).unwrap();
    assert_eq!(
        crate::arg_eval_type::eval_string(&value).unwrap().map(|bytes| bytes.len()),
        Some(count as usize)
    );
}

/// Go `builtinGreatestStringSig.vecEvalString` keeps the running value only
/// when it compares strictly ahead, so of two equal arguments it answers the
/// later; `evalString` keeps the earlier.
#[test]
fn greatest_string_vector_kernel_answers_the_later_of_equal_arguments() {
    let (expression, cells) = vec_cells(
        "greatest",
        &[text_field(), text_field()],
        &[vec![Datum::new_string_datum("a"), Datum::new_string_datum("a ")]],
    );
    assert_eq!(cells[0], Datum::Bytes(b"a ".to_vec()));
    let row = row_of(&[Datum::new_string_datum("a"), Datum::new_string_datum("a ")]);
    assert_eq!(
        expression.eval(&NoColumns, row.get_row(0)).unwrap(),
        Datum::new_string_datum("a")
    );
}
