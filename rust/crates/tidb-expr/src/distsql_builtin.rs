//! Go `pkg/expression/distsql_builtin.go`: decodes a pushed-down `tipb.Expr`
//! into the shared [`Expression`] tree, so a coprocessor evaluates a pushed
//! signature with the same builtin the root task would have used.
//!
//! Go's `getSignatureByPB` maps each `ScalarFuncSig` to its builtin. Here the
//! pushdown [`CATALOG`] is the inventory of signatures TiDB sends, and each
//! row names the builtin (`BuiltinSignature::name`) that produced it.

use crate::column::Column;
use crate::constant::Constant;
use crate::expression::Expression;
use crate::pushdown_catalog::CATALOG;
use crate::scalar_function::ScalarFunction;
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

/// The builtin name Go's `getSignatureByPB` resolves `sig` to, restricted to
/// the signatures TiDB pushes (the catalog). `None` is Go's `default:` arm.
#[must_use]
pub fn builtin_name_of(sig: tipb::ScalarFuncSig) -> Option<&'static str> {
    CATALOG.iter().find(|row| row.sig == sig).map(|row| row.name)
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
fn pb_literal(expr: &tipb::Expr) -> Option<Result<Expression, String>> {
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
        ExprType::String | ExprType::Bytes => Ok(constant(
            Datum::Bytes(val.to_vec()),
            typed_or(expr, FieldTypeCode::String),
        )),
        ExprType::Float32 | ExprType::Float64 => tidb_codec::decode_float(val)
            .map(|(_, v)| constant(Datum::Real(v), typed_or(expr, FieldTypeCode::Double)))
            .map_err(|e| bad("float", &e)),
        ExprType::MysqlDecimal => tidb_codec::decode_decimal(val)
            .map(|(_, v, _, _)| {
                constant(Datum::Decimal(v), typed_or(expr, FieldTypeCode::NewDecimal))
            })
            .map_err(|e| bad("decimal", &e)),
        ExprType::MysqlDuration | ExprType::MysqlTime | ExprType::MysqlJson => {
            pb_codec_literal(expr)
        }
        _ => return None,
    })
}

/// Go `convertDuration`, `convertTime`, `convertJSON`.
fn pb_codec_literal(expr: &tipb::Expr) -> Result<Expression, String> {
    use tipb::ExprType;
    let val = expr.val();
    match expr.tp() {
        ExprType::MysqlDuration => {
            let (_, nanos) = tidb_codec::decode_int(val)
                .map_err(|err| format!("invalid duration literal: {err:?}"))?;
            let field_type = typed_or(expr, FieldTypeCode::Duration);
            let fsp = field_type.decimal().max(0);
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
            let value = tidb_datatype::Time::from_packed_uint(packed, kind, field_type.decimal())
                .map_err(|err| format!("invalid time literal: {err:?}"))?;
            Ok(constant(Datum::Time(value), field_type))
        }
        _ => {
            let datum = tidb_codec::decode(val, 1)
                .map_err(|err| format!("invalid json literal: {err:?}"))?
                .into_iter()
                .next()
                .ok_or_else(|| "invalid json literal: no datum".to_owned())?;
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
    if expr.tp() == tipb::ExprType::ColumnRef {
        return pb_column(expr, column_types);
    }
    if let Some(literal) = pb_literal(expr) {
        return literal;
    }
    if expr.tp() != tipb::ExprType::ScalarFunc {
        return Err(format!("expr type {:?} is not decodable", expr.tp()));
    }
    let args = expr
        .children
        .iter()
        .map(|child| pb_to_expr(child, column_types))
        .collect::<Result<Vec<_>, _>>()?;
    let sig = expr.sig();
    let name = builtin_name_of(sig)
        .ok_or_else(|| format!("scalar signature {sig:?} is not a pushdown builtin"))?;
    let ret_type = typed_or(expr, FieldTypeCode::Null);
    Ok(Expression::ScalarFunction(ScalarFunction::new(
        CiString::new(name),
        ret_type,
        args,
    )))
}
