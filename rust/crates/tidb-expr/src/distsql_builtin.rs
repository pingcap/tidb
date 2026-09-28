//! Go `pkg/expression/distsql_builtin.go`: decodes a pushed-down `tipb.Expr`
//! into the shared [`Expression`] tree, so a coprocessor evaluates a pushed
//! signature with the same builtin the root task would have used.
//!
//! Go's `getSignatureByPB` maps each `ScalarFuncSig` to its builtin. Here the
//! decoder selects a typed implementation independently of the encoder
//! admission catalog. Names label expressions; they never select execution.

use crate::column::Column;
use crate::constant::Constant;
use crate::expression::Expression;
use crate::scalar_function::PbBuiltin;
use crate::scalar_function::ScalarFunction;
#[cfg(test)]
use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_proto::tipb;

/// Go `PbTypeToFieldType`.
#[must_use]
pub fn pb_type_to_field_type(tp: &tipb::FieldType) -> FieldType {
    let code = u8::try_from(tp.tp()).unwrap_or(0);
    let mut field_type = FieldType::new(FieldTypeCode::from_mysql_type(code))
        .with_flen(i64::from(tp.flen()))
        .with_decimal(i64::from(tp.decimal()));
    field_type.add_flags(tp.flag());
    if !tp.charset().is_empty() {
        field_type.set_charset_name(tp.charset());
    }
    if tp.collate() != 0 {
        let name = tidb_datatype::proto_to_collation(tp.collate());
        if !name.is_empty() {
            field_type.set_collation_name(name);
        }
    }
    if !tp.elems.is_empty() {
        field_type = field_type.with_elems(tp.elems.iter().map(String::as_str));
    }
    field_type
}

/// Whether the decoder has the concrete implementation for this wire signature.
#[must_use]
pub fn supports_signature(sig: tipb::ScalarFuncSig) -> bool {
    PbBuiltin::new(sig).is_some()
}

fn constant(value: Datum, ret_type: FieldType) -> Expression {
    Expression::Constant(Constant::new(value, ret_type))
}

fn typed_or(expr: &tipb::Expr, default: FieldTypeCode) -> FieldType {
    expr.field_type
        .as_ref()
        .map_or_else(|| FieldType::new(default), pb_type_to_field_type)
}

/// Go `PBToExpr`'s literal arms (`convertInt`, `convertUint`,
/// `convertString`, `convertFloat`, `convertDecimal`, `convertDuration`,
/// `convertTime`, `convertJSON`). `None`: `expr` is not a literal.
fn pb_literal(
    expr: &tipb::Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> Option<Result<Expression, String>> {
    use tipb::ExprType;
    let val = expr.val();
    let bad = |what: &str, err: &dyn std::fmt::Debug| format!("invalid {what} literal: {err:?}");
    Some(match expr.tp() {
        ExprType::Null => Ok(constant(Datum::Null, FieldType::new(FieldTypeCode::Null))),
        ExprType::Int64 => tidb_codec::decode_int(val)
            .map(|(_, v)| constant(Datum::Int(v), typed_or(expr, FieldTypeCode::LongLong)))
            .map_err(|e| bad("int", &e)),
        ExprType::Uint64 => tidb_codec::decode_uint(val)
            .map(|(_, v)| constant(Datum::UInt(v), typed_or(expr, FieldTypeCode::LongLong)))
            .map_err(|e| bad("uint", &e)),
        ExprType::String => Ok(constant(
            Datum::Bytes(val.to_vec()),
            typed_or(expr, FieldTypeCode::String),
        )),
        ExprType::Bytes => Ok(constant(
            Datum::Bytes(val.to_vec()),
            FieldType::new(FieldTypeCode::String),
        )),
        ExprType::MysqlBit => Ok(constant(
            Datum::Bit(val.into()),
            FieldType::new(FieldTypeCode::String),
        )),
        ExprType::MysqlEnum => (|| {
            let (_, value) = tidb_codec::decode_uint(val).map_err(|e| bad("enum", &e))?;
            let field_type = typed_or(expr, FieldTypeCode::Enum);
            let value = if value == 0 {
                tidb_datatype::MysqlEnum::default()
            } else {
                tidb_datatype::parse_enum_value(&field_type.elems_snapshot(), value)
                    .map_err(|e| bad("enum", &e))?
            };
            Ok(constant(
                Datum::Enum(value, field_type.collation()),
                field_type,
            ))
        })(),
        ExprType::TiDbVectorFloat32 => tidb_datatype::deserialize_vector_float32(val)
            .map(|(value, _)| {
                constant(
                    Datum::VectorFloat32(value),
                    FieldType::new(FieldTypeCode::VectorFloat32),
                )
            })
            .map_err(|e| bad("vector", &e)),
        ExprType::Float32 | ExprType::Float64 => tidb_codec::decode_float(val)
            .map(|(_, v)| {
                constant(
                    if expr.tp() == ExprType::Float32 {
                        Datum::Float32(f64::from(v as f32))
                    } else {
                        Datum::Real(v)
                    },
                    FieldType::new(FieldTypeCode::Double),
                )
            })
            .map_err(|e| bad("float", &e)),
        ExprType::MysqlDecimal => tidb_codec::decode_decimal(val)
            .map(|(_, v, _, _)| {
                constant(Datum::Decimal(v), typed_or(expr, FieldTypeCode::NewDecimal))
            })
            .map_err(|e| bad("decimal", &e)),
        ExprType::MysqlDuration | ExprType::MysqlTime | ExprType::MysqlJson => {
            pb_codec_literal(expr, zone)
        }
        _ => return None,
    })
}

/// Go `convertDuration`, `convertTime`, `convertJSON`.
fn pb_codec_literal(
    expr: &tipb::Expr,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Expression, String> {
    use tipb::ExprType;
    let val = expr.val();
    match expr.tp() {
        ExprType::MysqlDuration => {
            let (_, nanos) = tidb_codec::decode_int(val)
                .map_err(|err| format!("invalid duration literal: {err:?}"))?;
            let field_type = FieldType::new(FieldTypeCode::Duration);
            let fsp = 6;
            let value = tidb_datatype::MySqlDuration::from_nanoseconds(nanos, fsp)
                .map_err(|err| format!("invalid duration literal: {err:?}"))?;
            Ok(constant(Datum::Duration(value), field_type))
        }
        ExprType::MysqlTime => {
            let (_, packed) = tidb_codec::decode_uint(val)
                .map_err(|err| format!("invalid time literal: {err:?}"))?;
            let field_type = typed_or(expr, FieldTypeCode::Datetime);
            let kind = match field_type.code() {
                FieldTypeCode::Date => tidb_datatype::TimeType::Date,
                FieldTypeCode::Timestamp => tidb_datatype::TimeType::Timestamp,
                _ => tidb_datatype::TimeType::DateTime,
            };
            let mut value =
                tidb_datatype::Time::from_packed_uint(packed, kind, field_type.decimal())
                    .map_err(|err| format!("invalid time literal: {err:?}"))?;
            if kind == tidb_datatype::TimeType::Timestamp && !zone.is_utc() {
                value
                    .convert_time_zone(&tidb_datatype::SessionTimeZone::utc(), zone)
                    .map_err(|err| format!("invalid time literal: {err:?}"))?;
            }
            Ok(constant(Datum::Time(value), field_type))
        }
        _ => {
            let (_, datum) = tidb_codec::decode_one(val)
                .map_err(|err| format!("invalid json literal: {err:?}"))?;
            if !matches!(datum, Datum::Json(_)) {
                return Err("invalid json literal: expected a JSON datum".to_owned());
            }
            Ok(constant(datum, FieldType::new(FieldTypeCode::Json)))
        }
    }
}

fn pb_column(expr: &tipb::Expr, column_types: &[FieldType]) -> Result<Expression, String> {
    let (_, offset) = tidb_codec::decode_int(expr.val())
        .map_err(|err| format!("invalid column offset: {err:?}"))?;
    let index = usize::try_from(offset).map_err(|_| format!("negative column offset {offset}"))?;
    // Go reads `tps[offset]`; a caller without the scan schema (the
    // coprocessor's row-at-a-time evaluator) uses the type the column
    // reference carries on the wire, which TiDB always fills.
    let ret_type = match column_types.get(index) {
        Some(field_type) => field_type.clone(),
        None if expr.field_type.is_some() => typed_or(expr, FieldTypeCode::Null),
        None => return Err(format!("column offset {index} out of range")),
    };
    let mut column = Column::new(0, ret_type);
    column.index = offset;
    Ok(Expression::Column(column))
}

/// Go `PBToExpr` (`distsql_builtin.go:1218`) over the scanned row's column
/// types.
///
/// # Errors
///
/// A malformed literal, an out-of-range column, or a signature TiDB does not
/// push (Go `getSignatureByPB`'s `default:` arm).
pub fn pb_to_expr(expr: &tipb::Expr, column_types: &[FieldType]) -> Result<Expression, String> {
    pb_to_expr_in(expr, column_types, &tidb_datatype::SessionTimeZone::utc())
}

/// Decode using the request timezone, including UTC TIMESTAMP wire literals.
pub fn pb_to_expr_in(
    expr: &tipb::Expr,
    column_types: &[FieldType],
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Expression, String> {
    let kind = tipb::ExprType::try_from(expr.tp.unwrap_or_default())
        .map_err(|_| format!("unknown expression type {}", expr.tp.unwrap_or_default()))?;
    if kind == tipb::ExprType::ColumnRef {
        return pb_column(expr, column_types);
    }
    if let Some(literal) = pb_literal(expr, zone) {
        return literal;
    }
    if kind != tipb::ExprType::ScalarFunc {
        return Err(format!("expr type {:?} is not decodable", expr.tp()));
    }
    let mut args = Vec::with_capacity(expr.children.len());
    for child in &expr.children {
        if child.tp == Some(tipb::ExprType::ValueList as i32) {
            let values = if child.val().is_empty() {
                Vec::new()
            } else {
                tidb_codec::decode(child.val(), 1)
                    .map_err(|error| format!("invalid value list: {error:?}"))?
            };
            // Go PBToExpr returns FALSE immediately for an empty packed list.
            if values.is_empty() {
                return Ok(constant(
                    Datum::Int(0),
                    FieldType::new(FieldTypeCode::LongLong),
                ));
            }
            args.extend(values.into_iter().map(|value| {
                let mut constant = Constant::default();
                constant.value = value;
                Expression::Constant(constant)
            }));
        } else {
            args.push(pb_to_expr_in(child, column_types, zone)?);
        }
    }
    let sig = tipb::ScalarFuncSig::try_from(expr.sig.unwrap_or_default())
        .map_err(|_| format!("unknown scalar signature {}", expr.sig.unwrap_or_default()))?;
    let builtin = PbBuiltin::new(sig)
        .ok_or_else(|| format!("scalar signature {sig:?} is not a pushdown builtin"))?;
    if args.iter().any(|arg| arg.static_type().is_none()) {
        // Go's decodeValueList leaves RetType nil, which panics in collation
        // derivation. Reject malformed untyped arguments at the Rust boundary.
        return Err("protobuf builtin argument has no field type".to_owned());
    }
    let ret_type = if builtin.is_date_arithmetic() {
        // Go getSignatureByPB delegates this family to addSubDateFunctionClass,
        // deriving its result type from the arguments rather than the wire type.
        let [_, _, unit] = args.as_slice() else {
            return Err("protobuf date arithmetic arity".to_owned());
        };
        let value = unit
            .eval(&crate::NoColumns, tidb_chunk::row::Row::empty())
            .map_err(|error| format!("invalid date interval unit: {error:?}"))?;
        let bytes = crate::arg_eval_type::eval_string(&value)
            .map_err(|error| format!("invalid date interval unit: {error:?}"))?
            .unwrap_or_default();
        let unit = std::str::from_utf8(&bytes).map_err(|error| error.to_string())?;
        crate::rewriter::result_type::date_arithmetic_return_type(unit, &args[..2])
            .ok_or_else(|| "invalid date arithmetic argument types".to_owned())?
    } else {
        typed_or(expr, FieldTypeCode::Null)
    };
    Ok(Expression::ScalarFunction(ScalarFunction::from_pb(
        builtin, ret_type, args,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> tipb::Expr {
        tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(value.as_bytes().to_vec()),
            field_type: Some(tipb::FieldType {
                tp: Some(253),
                charset: Some("utf8mb4".into()),
                collate: Some(46),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    fn call(sig: tipb::ScalarFuncSig, args: Vec<tipb::Expr>, tp: tipb::FieldType) -> tipb::Expr {
        tipb::Expr {
            tp: Some(tipb::ExprType::ScalarFunc as i32),
            sig: Some(sig as i32),
            children: args,
            field_type: Some(tp),
            ..Default::default()
        }
    }
    fn integer(value: i64) -> tipb::Expr {
        let mut val = Vec::new();
        tidb_codec::encode_int(&mut val, value);
        tipb::Expr {
            tp: Some(tipb::ExprType::Int64 as i32),
            val: Some(val),
            field_type: Some(tidb_proto::tipb::FieldType {
                tp: Some(8),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn legacy_families_compose_through_one_recursive_decoder() {
        use tipb::ScalarFuncSig::*;
        let int_type = tipb::FieldType {
            tp: Some(8),
            decimal: Some(0),
            ..Default::default()
        };
        let row = tidb_chunk::row::Row::empty();
        for (sig, args, expected) in [
            (MinusInt, vec![integer(3), integer(1)], 2),
            (MultiplyInt, vec![integer(3), integer(2)], 6),
            (IntDivideInt, vec![integer(7), integer(2)], 3),
            (CastIntAsInt, vec![integer(7)], 7),
            (InInt, vec![integer(2), integer(1), integer(2)], 1),
            (InString, vec![text("x"), text("y"), text("x")], 1),
            (LikeSig, vec![text("abc"), text("a%"), integer(92)], 1),
        ] {
            let child = call(sig, args, int_type.clone());
            let parent = call(IfInt, vec![integer(1), child, integer(0)], int_type.clone());
            let expr = pb_to_expr(&parent, &[]).unwrap();
            assert_eq!(
                expr.eval(&crate::NoColumns, row).unwrap(),
                Datum::Int(expected),
                "{sig:?}"
            );
        }
        for sig in [AddDateStringInt, SubDateStringInt] {
            let expected = if sig == AddDateStringInt {
                "2024-01-03"
            } else {
                "2024-01-01"
            };
            // Go's date builder derives VarString even when the PB return type is Int.
            let child = call(
                sig,
                vec![text("2024-01-02"), integer(1), text("DAY")],
                int_type.clone(),
            );
            let parent = call(
                IfNullString,
                vec![child, text("fallback")],
                text("").field_type.unwrap(),
            );
            let expr = pb_to_expr(&parent, &[]).unwrap();
            assert_eq!(
                expr.eval(&crate::NoColumns, row)
                    .unwrap()
                    .sql_bytes()
                    .unwrap(),
                expected.as_bytes()
            );
        }
    }

    #[test]
    fn protobuf_multiply_signedness_is_selected_by_signature() {
        for (sig, expected) in [
            (tipb::ScalarFuncSig::MultiplyInt, Datum::Int(1)),
            (
                tipb::ScalarFuncSig::MultiplyIntUnsigned,
                Datum::UInt(u64::MAX),
            ),
        ] {
            let right = if sig == tipb::ScalarFuncSig::MultiplyInt {
                -1
            } else {
                1
            };
            let field = tipb::FieldType {
                tp: Some(8),
                flag: Some(if right == 1 { 32 } else { 0 }),
                ..Default::default()
            };
            let expr =
                pb_to_expr(&call(sig, vec![integer(-1), integer(right)], field), &[]).unwrap();
            assert_eq!(
                expr.eval(&crate::NoColumns, tidb_chunk::row::Row::empty())
                    .unwrap(),
                expected
            );
        }
        let field = tipb::FieldType {
            tp: Some(8),
            flag: Some(32),
            ..Default::default()
        };
        let expr = pb_to_expr(
            &call(
                tipb::ScalarFuncSig::MultiplyIntUnsigned,
                vec![integer(-1), integer(-1)],
                field,
            ),
            &[],
        )
        .unwrap();
        assert!(matches!(
            expr.eval(&crate::NoColumns, tidb_chunk::row::Row::empty()),
            Err(crate::EvalError::DataOutOfRange {
                value: "BIGINT UNSIGNED",
                ..
            })
        ));
    }

    #[test]
    fn packed_lists_preserve_go_empty_shortcut_and_reject_untyped_members() {
        let int_type = tipb::FieldType {
            tp: Some(8),
            ..Default::default()
        };
        for bytes in [
            vec![255],
            tidb_codec::encode_value(&[Datum::Int(1)]).unwrap(),
        ] {
            let list = tipb::Expr {
                tp: Some(tipb::ExprType::ValueList as i32),
                val: Some(bytes),
                ..Default::default()
            };
            let expr = call(
                tipb::ScalarFuncSig::InInt,
                vec![integer(1), list],
                int_type.clone(),
            );
            assert!(pb_to_expr(&expr, &[]).is_err());
        }
        let list = tipb::Expr {
            tp: Some(tipb::ExprType::ValueList as i32),
            ..Default::default()
        };
        let expr = call(
            tipb::ScalarFuncSig::InInt,
            vec![tipb::Expr::default(), list],
            int_type,
        );
        assert_eq!(
            pb_to_expr(&expr, &[])
                .unwrap()
                .eval(&crate::NoColumns, tidb_chunk::row::Row::empty())
                .unwrap(),
            Datum::Int(0)
        );
    }

    #[test]
    fn go_rejected_arithmetic_variants_do_not_gain_fallback_support() {
        use tipb::ScalarFuncSig::*;
        for sig in [
            IntDivideIntSignedSigned,
            IntDivideIntSignedUnsigned,
            IntDivideIntUnsignedSigned,
            IntDivideIntUnsignedUnsigned,
            MinusIntForcedSignedUnsigned,
            MinusIntForcedUnsignedSigned,
            MinusIntForcedUnsignedUnsigned,
            MinusIntSignedSigned,
            MinusIntSignedUnsigned,
            MinusIntUnsignedSigned,
            MinusIntUnsignedUnsigned,
            PlusIntSignedSigned,
            PlusIntSignedUnsigned,
            PlusIntUnsignedSigned,
            PlusIntUnsignedUnsigned,
        ] {
            assert!(
                pb_to_expr(
                    &call(
                        sig,
                        vec![integer(1), integer(1)],
                        tipb::FieldType {
                            tp: Some(8),
                            ..Default::default()
                        }
                    ),
                    &[]
                )
                .is_err(),
                "{sig:?}"
            );
        }
    }

    #[test]
    fn unknown_wire_kind_is_not_sql_null() {
        let pb = tipb::Expr {
            tp: Some(i32::MAX),
            ..Default::default()
        };
        assert!(pb_to_expr(&pb, &[]).is_err());
    }

    #[test]
    fn timestamp_literals_use_the_same_zone_in_both_wire_directions() {
        use crate::pushdown_catalog::{build_call, to_pb_in, PbScalar};
        use tidb_datatype::{SessionTimeZone, Time, TimeType};
        for (zone, hour) in [
            (SessionTimeZone::utc(), 3),
            (SessionTimeZone::Named(chrono_tz::Asia::Shanghai), 11),
            (
                SessionTimeZone::Fixed {
                    name: String::new(),
                    offset_secs: 3600,
                },
                4,
            ),
        ] {
            for (code, kind) in [
                (FieldTypeCode::Timestamp, TimeType::Timestamp),
                (FieldTypeCode::Datetime, TimeType::DateTime),
            ] {
                let local = Time::from_date_checked(2024, 1, 2, hour, 4, 5, 0, kind, 0).unwrap();
                let literal = PbScalar::TimeLiteral {
                    value: local,
                    field_type: FieldType::new(code).with_decimal(0),
                };
                let pb = to_pb_in(&literal, &|_| None, &zone).unwrap();
                let wire = Time::from_date_checked(
                    2024,
                    1,
                    2,
                    if kind == TimeType::Timestamp { 3 } else { hour },
                    4,
                    5,
                    0,
                    kind,
                    0,
                )
                .unwrap();
                assert_eq!(
                    tidb_codec::decode_uint(pb.val()).unwrap().1,
                    wire.to_packed_uint().unwrap()
                );
                let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
                let value = pb_to_expr_in(&pb, &[], &zone)
                    .unwrap()
                    .eval(&crate::ZonedNoColumns(zone.clone()), row.to_row())
                    .unwrap();
                assert_eq!(value, Datum::Time(local));
                let call = build_call("isnull", vec![literal]).unwrap();
                let parent = to_pb_in(&call, &|_| None, &zone).unwrap();
                assert_eq!(parent.children[0].val, pb.val);
            }
        }
    }

    #[test]
    fn float32_and_duration_literals_retain_go_datum_metadata() {
        let mut val = Vec::new();
        tidb_codec::encode_float(&mut val, 1.23456789);
        let pb = tipb::Expr {
            tp: Some(tipb::ExprType::Float32 as i32),
            val: Some(val),
            ..Default::default()
        };
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        assert_eq!(
            pb_to_expr(&pb, &[])
                .unwrap()
                .eval(&crate::NoColumns, row.to_row())
                .unwrap(),
            Datum::Float32(f64::from(1.23456789_f32))
        );
        let mut val = Vec::new();
        tidb_codec::encode_int(&mut val, 1_234_567_000);
        let pb = tipb::Expr {
            tp: Some(tipb::ExprType::MysqlDuration as i32),
            val: Some(val),
            field_type: Some(tipb::FieldType {
                decimal: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            pb_to_expr(&pb, &[])
                .unwrap()
                .eval(&crate::NoColumns, row.to_row())
                .unwrap(),
            Datum::Duration(
                tidb_datatype::MySqlDuration::from_nanoseconds(1_234_567_000, 6).unwrap()
            )
        );
        let constant = constant(Datum::Float32(1.25), FieldType::new(FieldTypeCode::Double));
        let pb = crate::pushdown_catalog::expression_to_pb(&constant, &|_| None).unwrap();
        assert_eq!(pb.tp(), tipb::ExprType::Float32);
    }

    #[test]
    fn bit_enum_and_vector_literals_decode_like_go() {
        use crate::pushdown_catalog::{to_pb, PbScalar};
        let literals = [
            PbScalar::BitLiteral {
                value: vec![3].into(),
                field_type: FieldType::new(FieldTypeCode::Bit),
            },
            PbScalar::EnumLiteral {
                value: 2,
                field_type: FieldType::new(FieldTypeCode::Enum).with_elems(["red", "green"]),
            },
            PbScalar::VectorLiteral {
                value: tidb_datatype::VectorFloat32::must_create([1.0, 2.0]),
                field_type: FieldType::new(FieldTypeCode::VectorFloat32),
            },
        ];
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        for literal in literals {
            let pb = to_pb(&literal, &|_| None).unwrap();
            let decoded = pb_to_expr(&pb, &[]).unwrap();
            let value = decoded.eval(&crate::NoColumns, row.to_row()).unwrap();
            match literal {
                PbScalar::BitLiteral {
                    value: expected, ..
                } => assert_eq!(value, Datum::Bit(expected)),
                PbScalar::EnumLiteral { .. } => assert_eq!(
                    value,
                    Datum::Enum(
                        tidb_datatype::MysqlEnum::new("green", 2),
                        decoded.static_type().unwrap().collation()
                    )
                ),
                PbScalar::VectorLiteral {
                    value: expected, ..
                } => assert_eq!(value, Datum::VectorFloat32(expected)),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn json_literals_follow_go_admission_and_decoder_kind_checks() {
        let value = tidb_datatype::BinaryJSON::parse("{\"a\":1}").unwrap();
        let scalar = crate::pushdown_catalog::PbScalar::JsonLiteral {
            value,
            field_type: FieldType::new(FieldTypeCode::Json),
        };
        assert!(crate::pushdown_catalog::to_pb(&scalar, &|_| None).is_none());
        let pb = tipb::Expr {
            tp: Some(tipb::ExprType::MysqlJson as i32),
            val: Some(tidb_codec::encode_value(&[Datum::Int(1)]).unwrap()),
            ..Default::default()
        };
        assert!(pb_to_expr(&pb, &[]).is_err());
        let value = tidb_datatype::BinaryJSON::parse("{\"a\":1}").unwrap();
        let good = tipb::Expr {
            val: Some(tidb_codec::encode_value(&[Datum::Json(value.clone())]).unwrap()),
            ..pb
        };
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        assert_eq!(
            pb_to_expr(&good, &[])
                .unwrap()
                .eval(&crate::NoColumns, row.to_row())
                .unwrap(),
            Datum::Json(value)
        );
    }

    #[test]
    fn every_encoder_signature_has_a_typed_decoder() {
        for signature in crate::pushdown_catalog::CATALOG
            .iter()
            .map(|row| row.sig)
            .chain(crate::pushdown_catalog::implicit_cast_signatures())
        {
            assert!(supports_signature(signature), "missing {signature:?}");
        }
    }
    #[test]
    fn protobuf_signature_survives_display_name_changes() {
        let pb = call(
            tipb::ScalarFuncSig::UpperUtf8,
            vec![text("abc")],
            text("").field_type.unwrap(),
        );
        let Expression::ScalarFunction(mut function) = pb_to_expr(&pb, &[]).unwrap() else {
            panic!("scalar");
        };
        assert_eq!(
            function.pb_signature(),
            Some(tipb::ScalarFuncSig::UpperUtf8)
        );
        function = function.clone();
        function.func_name = CiString::new("lower");
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        assert_eq!(
            function
                .eval(&crate::NoColumns, row.to_row())
                .unwrap()
                .sql_string()
                .unwrap(),
            "ABC"
        );
    }
    #[test]
    fn protobuf_cast_preserves_wire_type_without_sql_rewriting() {
        let tp = tipb::FieldType {
            tp: Some(246),
            flen: Some(12),
            decimal: Some(3),
            flag: Some(1),
            ..Default::default()
        };
        let pb = call(
            tipb::ScalarFuncSig::CastStringAsDecimal,
            vec![text("12.125")],
            tp.clone(),
        );
        let expr = pb_to_expr(&pb, &[]).unwrap();
        assert_eq!(expr.static_type(), Some(&pb_type_to_field_type(&tp)));
    }
    #[test]
    fn protobuf_mod_signedness_comes_from_the_selected_signature() {
        let integer = |value| {
            let mut bytes = Vec::new();
            tidb_codec::encode_int(&mut bytes, value);
            tipb::Expr {
                tp: Some(tipb::ExprType::Int64 as i32),
                val: Some(bytes),
                ..Default::default()
            }
        };
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        for (signature, expected) in [
            (tipb::ScalarFuncSig::ModIntSignedSigned, -2),
            (tipb::ScalarFuncSig::ModIntUnsignedSigned, 2),
        ] {
            let pb = call(
                signature,
                vec![integer(-5), integer(3)],
                tipb::FieldType {
                    tp: Some(8),
                    ..Default::default()
                },
            );
            assert_eq!(
                pb_to_expr(&pb, &[])
                    .unwrap()
                    .eval(&crate::NoColumns, row.to_row())
                    .unwrap(),
                Datum::Int(expected)
            );
        }
    }
    #[test]
    fn protobuf_json_replace_reuses_the_selected_implementation_across_rows() {
        let column = |index| {
            let mut bytes = Vec::new();
            tidb_codec::encode_int(&mut bytes, index);
            tipb::Expr {
                tp: Some(tipb::ExprType::ColumnRef as i32),
                val: Some(bytes),
                ..Default::default()
            }
        };
        let pb = call(
            tipb::ScalarFuncSig::JsonReplaceSig,
            vec![column(0), text("$.a"), column(1)],
            tipb::FieldType {
                tp: Some(245),
                ..Default::default()
            },
        );
        let expr = pb_to_expr(
            &pb,
            &[
                FieldType::new(FieldTypeCode::Json),
                FieldType::new(FieldTypeCode::LongLong),
            ],
        )
        .unwrap();
        for number in [2, 3] {
            let row = tidb_chunk::mutrow::MutRow::from_datums(&[
                Datum::Json(tidb_datatype::BinaryJSON::parse("{\"a\": 1}").unwrap()),
                Datum::Int(number),
            ]);
            assert_eq!(
                expr.eval(&crate::NoColumns, row.to_row()).unwrap(),
                Datum::Json(
                    tidb_datatype::BinaryJSON::parse(&format!("{{\"a\": {number}}}")).unwrap()
                )
            );
        }
    }
    #[test]
    fn protobuf_control_does_not_evaluate_the_unused_branch() {
        let mut value = Vec::new();
        tidb_codec::encode_int(&mut value, 1);
        let condition = tipb::Expr {
            tp: Some(tipb::ExprType::Int64 as i32),
            val: Some(value),
            ..Default::default()
        };
        let invalid = call(
            tipb::ScalarFuncSig::CastStringAsInt,
            vec![text("not an integer")],
            tipb::FieldType {
                tp: Some(8),
                ..Default::default()
            },
        );
        let pb = call(
            tipb::ScalarFuncSig::IfInt,
            vec![condition.clone(), condition, invalid],
            tipb::FieldType {
                tp: Some(8),
                ..Default::default()
            },
        );
        struct Strict;
        impl crate::Columns for Strict {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> crate::ErrorLevel {
                crate::ErrorLevel::Error
            }
        }
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        assert_eq!(
            pb_to_expr(&pb, &[])
                .unwrap()
                .eval(&Strict, row.to_row())
                .unwrap(),
            Datum::Int(1)
        );
    }
    #[test]
    fn protobuf_binary_and_utf8_signatures_stay_distinct() {
        let row = tidb_chunk::mutrow::MutRow::from_datums(&[]);
        for (sig, expected) in [
            (tipb::ScalarFuncSig::CharLength, 2),
            (tipb::ScalarFuncSig::CharLengthUtf8, 1),
        ] {
            let pb = call(
                sig,
                vec![text("é")],
                tipb::FieldType {
                    tp: Some(8),
                    ..Default::default()
                },
            );
            assert_eq!(
                pb_to_expr(&pb, &[])
                    .unwrap()
                    .eval(&crate::NoColumns, row.to_row())
                    .unwrap(),
                Datum::Int(expected)
            );
        }
    }
}
