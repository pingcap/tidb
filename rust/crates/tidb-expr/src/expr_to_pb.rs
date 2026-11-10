//! Go `pkg/expression/expr_to_pb.go`: `PbConverter.ExprToPB`.
//!
//! A function node is sent as it was built: the signature its class set
//! (`PbCode`), its own result type with its collation forced in, its
//! metadata, and its arguments converted the same way.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_proto::tipb::{Expr, ExprType, ScalarFuncSig};

use tidb_kv::store_type::StoreType;

use crate::scalar_function::ScalarFunction;
use crate::expression::Expression;

/// Go `PbConverter`.
pub struct PbConverter<'a> {
    /// Go `PbConverter.ctx`.
    ctx: &'a dyn crate::Columns,
    /// Whether `ctx` is a statement's context: a deferred constant or a
    /// correlated column is the value it holds now. A caller with no
    /// statement context converts strict constants only.
    bind_values: bool,
    /// Go `typeCtx(pc.ctx).Location()`.
    zone: &'a tidb_datatype::SessionTimeZone,
}

impl<'a> PbConverter<'a> {
    /// Go `NewPBConverter`.
    pub fn new(
        ctx: &'a dyn crate::Columns,
        bind_values: bool,
        zone: &'a tidb_datatype::SessionTimeZone,
    ) -> Self {
        Self {
            ctx,
            bind_values,
            zone,
        }
    }

    /// Go `PbConverter.ExprToPB`.
    pub fn expr_to_pb(&self, expr: &Expression) -> Option<Expr> {
        match expr {
            Expression::Constant(constant) => {
                let value = if self.bind_values {
                    constant.eval_in(self.ctx).ok()?
                } else {
                    constant.literal_value()?.clone()
                };
                // Go `Constant.GetType(ctx)` infers a marker's type from the
                // current datum, independently of its planning-time RetType.
                let parameter_type = (self.bind_values && constant.param_marker.is_some())
                    .then(|| tidb_datatype::infer_param_type_from_datum(&value));
                self.con_or_cor_col_to_pb_expr(
                    parameter_type.as_ref().or(constant.ret_type.as_ref())?,
                    &value,
                )
            }
            Expression::CorrelatedColumn(column) if self.bind_values => {
                self.con_or_cor_col_to_pb_expr(column.column.ret_type.as_ref()?, &column.eval())
            }
            Expression::Column(column) => self.column_to_pb_expr(column, true),
            Expression::ScalarFunction(function) => self.scalar_func_to_pb_expr(expr, function),
            // Go `ExprToPB` has no arm for `ScalarSubQueryExpr`.
            Expression::CorrelatedColumn(_) | Expression::ScalarSubQuery(_) => None,
        }
    }

    /// Go `PbConverter.conOrCorColToPBExpr`.
    fn con_or_cor_col_to_pb_expr(&self, ft: &FieldType, d: &Datum) -> Option<Expr> {
        let (tp, val) = self.encode_datum(ft, d)?;
        Some(Expr {
            tp: Some(tp as i32),
            val,
            children: Vec::new(),
            sig: Some(ScalarFuncSig::Unspecified as i32),
            field_type: Some(to_pb_field_type(ft)),
            has_distinct: Some(false),
            agg_func_mode: None,
            ..Default::default()
        })
    }

    /// Go `PbConverter.encodeDatum`.
    fn encode_datum(&self, ft: &FieldType, d: &Datum) -> Option<(ExprType, Option<Vec<u8>>)> {
        let mut val = Vec::new();
        let tp = match d {
            Datum::Null => return Some((ExprType::Null, None)),
            Datum::Int(value) => {
                tidb_codec::encode_int(&mut val, *value);
                ExprType::Int64
            }
            Datum::UInt(value) => {
                tidb_codec::encode_uint(&mut val, *value);
                ExprType::Uint64
            }
            Datum::String(value) => {
                val = value.bytes().to_vec();
                ExprType::String
            }
            Datum::BinaryLiteral(value) => {
                val = value.as_bytes().to_vec();
                ExprType::String
            }
            Datum::Bit(value) => {
                val = value.as_bytes().to_vec();
                ExprType::MysqlBit
            }
            Datum::Bytes(value) => {
                val = value.clone();
                ExprType::Bytes
            }
            // PB float constants reuse mem-comparable encoding, which
            // normalizes -0 to +0; a negative zero stays at root.
            Datum::Float32(value) | Datum::Real(value) => {
                if *value == 0.0 && value.is_sign_negative() {
                    return None;
                }
                tidb_codec::encode_float(&mut val, *value);
                if matches!(d, Datum::Float32(_)) {
                    ExprType::Float32
                } else {
                    ExprType::Float64
                }
            }
            Datum::Duration(value) => {
                tidb_codec::encode_int(&mut val, value.nanoseconds());
                ExprType::MysqlDuration
            }
            // The precision and fraction of the decimal itself, not the
            // datum's declared ones.
            Datum::Decimal(value) => {
                tidb_codec::encode_decimal_fixed(&mut val, value, 0, 0).ok()?;
                ExprType::MysqlDecimal
            }
            Datum::Time(value) => {
                let kind = match ft.code() {
                    FieldTypeCode::Timestamp => tidb_datatype::TimeType::Timestamp,
                    FieldTypeCode::Date => tidb_datatype::TimeType::Date,
                    _ => tidb_datatype::TimeType::DateTime,
                };
                tidb_codec::encode_mysql_time(self.zone, *value, Some(kind), &mut val).ok()?;
                ExprType::MysqlTime
            }
            Datum::Enum(value, _) => {
                tidb_codec::encode_uint(&mut val, value.value());
                ExprType::MysqlEnum
            }
            Datum::VectorFloat32(value) => {
                val = value.serialize();
                ExprType::TiDbVectorFloat32
            }
            _ => return None,
        };
        Some((tp, Some(val)))
    }

    /// Go `PbConverter.columnToPBExpr`.
    pub fn column_to_pb_expr(&self, column: &crate::column::Column, check_type: bool) -> Option<Expr> {
        let ret_type = column.get_static_type()?;
        if check_type {
            let blacklist = self.ctx.expr_pushdown_blacklist();
            match ret_type.code() {
                FieldTypeCode::Bit => {
                    if !crate::infer_pushdown::is_push_down_enabled(blacklist, "bit", StoreType::TiKv) {
                        return None;
                    }
                }
                FieldTypeCode::Set | FieldTypeCode::Geometry | FieldTypeCode::Unspecified => return None,
                FieldTypeCode::Enum => {
                    if !crate::infer_pushdown::is_push_down_enabled(blacklist, "enum", StoreType::Unspecified) {
                        return None;
                    }
                }
                _ => {}
            }
        }
        let mut val = Vec::with_capacity(8);
        tidb_codec::encode_int(&mut val, column.index);
        Some(Expr {
            tp: Some(ExprType::ColumnRef as i32),
            val: Some(val),
            children: Vec::new(),
            sig: Some(ScalarFuncSig::Unspecified as i32),
            field_type: Some(to_pb_field_type(ret_type)),
            has_distinct: Some(false),
            agg_func_mode: None,
            ..Default::default()
        })
    }

    /// Go `PbConverter.scalarFuncToPBExpr`.
    fn scalar_func_to_pb_expr(&self, expr: &Expression, function: &ScalarFunction) -> Option<Expr> {
        // Check whether this function has ProtoBuf signature.
        let pb_code = function.pb_code();
        if pb_code == ScalarFuncSig::Unspecified {
            return None;
        }
        // Check whether this function can be pushed.
        if !crate::infer_pushdown::scalar_function_can_be_pushed(
            expr,
            function,
            pb_code,
            tidb_kv::store_type::StoreType::Unspecified,
            self.ctx.expr_pushdown_blacklist(),
        ) {
            return None;
        }
        // Check whether all of its parameters can be pushed.
        let children = function
            .args
            .iter()
            .map(|arg| self.expr_to_pb(arg))
            .collect::<Option<Vec<_>>>()?;
        let encoded = function_metadata(function)?;
        // Put collation information into the RetType enforcedly and push it
        // down to TiKV.
        let mut tp = expr.static_type()?.clone();
        if tidb_datatype::new_collation_enabled() {
            let (_, collation) = function.collation.charset_and_collation();
            tp.set_collation_name(collation);
        }
        Some(Expr {
            tp: Some(ExprType::ScalarFunc as i32),
            val: encoded,
            children,
            sig: Some(pb_code as i32),
            field_type: Some(to_pb_field_type(&tp)),
            has_distinct: Some(false),
            agg_func_mode: None,
            ..Default::default()
        })
    }
}

/// Go `ExpressionsToPBList`: every expression, or the first one with no PB
/// form.
///
/// # Errors
/// The expression that cannot be pushed down.
pub fn expressions_to_pb_list<'e>(
    pc: &PbConverter<'_>,
    exprs: impl IntoIterator<Item = &'e Expression>,
) -> Result<Vec<Expr>, &'e Expression> {
    exprs
        .into_iter()
        .map(|expr| pc.expr_to_pb(expr).ok_or(expr))
        .collect()
}

/// Go `ProjectionExpressionsToPBList`: a bare column is sent whatever its
/// type is.
///
/// # Errors
/// The expression that cannot be pushed down.
pub fn projection_expressions_to_pb_list<'e>(
    pc: &PbConverter<'_>,
    exprs: impl IntoIterator<Item = &'e Expression>,
) -> Result<Vec<Expr>, &'e Expression> {
    exprs
        .into_iter()
        .map(|expr| {
            match expr {
                Expression::Column(column) => pc.column_to_pb_expr(column, false),
                expr => pc.expr_to_pb(expr),
            }
            .ok_or(expr)
        })
        .collect()
}

/// Go `GroupByItemToPB`.
#[must_use]
pub fn group_by_item_to_pb(pc: &PbConverter<'_>, expr: &Expression) -> Option<tidb_proto::tipb::ByItem> {
    Some(tidb_proto::tipb::ByItem {
        expr: Some(pc.expr_to_pb(expr)?),
        desc: Some(false),
        ..Default::default()
    })
}

/// Go `SortByItemToPB`.
#[must_use]
pub fn sort_by_item_to_pb(pc: &PbConverter<'_>, expr: &Expression, desc: bool) -> Option<tidb_proto::tipb::ByItem> {
    Some(tidb_proto::tipb::ByItem {
        expr: Some(pc.expr_to_pb(expr)?),
        desc: Some(desc),
        ..Default::default()
    })
}

/// Go `ToPBFieldType`.
#[must_use]
pub fn to_pb_field_type(ft: &FieldType) -> tidb_proto::tipb::FieldType {
    tidb_proto::tipb::FieldType {
        tp: Some(i32::from(ft.code().mysql_type())),
        flag: Some(ft.flags()),
        flen: Some(ft.flen() as i32),
        decimal: Some(ft.decimal() as i32),
        charset: Some(ft.charset_name().to_owned()),
        collate: Some(tidb_datatype::collation_to_proto(ft.collation_name())),
        elems: ft.elems_snapshot().into_iter().map(|elem| elem.to_string()).collect(),
        // Upstream FieldType.array is gogoproto nullable=false.
        array: Some(false),
    }
}

/// Go `ToPBFieldTypeWithCheck`.
///
/// # Errors
/// A TiFlash type whose decimal is invalid.
pub fn to_pb_field_type_with_check(
    ft: &FieldType,
    store_type: StoreType,
) -> Result<tidb_proto::tipb::FieldType, String> {
    if store_type == StoreType::TiFlash && !ft.is_decimal_valid() {
        return Err(format!(
            "{ft} can not be pushed to TiFlash because it contains invalid decimal('{}','{}').",
            ft.flen(),
            ft.decimal()
        ));
    }
    Ok(to_pb_field_type(ft))
}

/// What Go `NewPBConverter(client, ctx)` is handed: the statement's
/// `exprctx.EvalContext`, whose constants, parameters and correlated bindings
/// `ExprToPB` reads ([`crate::Columns`]), plus the expression pushdown
/// blacklist every conversion is gated by. Go reads the blacklist from the
/// global `DefaultExprPushDownBlacklist`; this tier captures it per statement,
/// so the statement's context answers it.
pub trait PbConverterContext: crate::Columns + Send + Sync {}

impl<T: crate::Columns + Send + Sync + ?Sized> PbConverterContext for T {}

/// Go `PbConverter.conOrCorColToPBExpr`: every deferred constant (a parameter
/// marker or a value bound at execution) and every correlated column under
/// `expression` becomes the constant `context` holds for it now. The caller
/// binds a request-owned copy, so the retained plan expression is untouched
/// for the next Open.
pub fn con_or_cor_col_to_pb_expr(
    expression: &mut Expression,
    context: &dyn crate::Columns,
) -> Result<(), crate::EvalError> {
    use crate::constant::Constant;
    match expression {
        Expression::Constant(constant) if constant.literal_value().is_none() => {
            let value = constant.eval_in(context)?;
            let field_type = if constant.param_marker.is_some() {
                tidb_datatype::infer_param_type_from_datum(&value)
            } else {
                constant
                    .ret_type
                    .clone()
                    .ok_or(crate::EvalError::Unsupported(
                        "deferred aggregate constant has no result type",
                    ))?
            };
            *constant = Constant::new(value, field_type);
        }
        Expression::CorrelatedColumn(column) => {
            let field_type =
                column
                    .column
                    .ret_type
                    .clone()
                    .ok_or(crate::EvalError::Unsupported(
                        "correlated aggregate column has no result type",
                    ))?;
            *expression = Expression::Constant(Constant::new(column.eval(), field_type));
        }
        Expression::ScalarFunction(function) => {
            for argument in &mut function.args {
                con_or_cor_col_to_pb_expr(argument, context)?;
            }
        }
        Expression::Column(_) | Expression::Constant(_) | Expression::ScalarSubQuery(_) => {}
    }
    Ok(())
}

/// Go `expr.Function.metadata()` as `scalarFuncToPBExpr` marshals it into the
/// function node's `Val`: `Some(None)` for a function with none, and `None`
/// where Go's `proto.Marshal` fails and the function is not converted -- a
/// `GROUPING` whose metadata was never set marshals no required `mode`.
/// (A cast's `InUnionMetadata` is written where the cast node is encoded.)
pub(crate) fn function_metadata(
    function: &crate::scalar_function::ScalarFunction,
) -> Option<Option<Vec<u8>>> {
    use prost::Message as _;
    // Go `baseBuiltinCastFunc.metadata`: the casts to a number embed it, the
    // other cast signatures inherit `baseBuiltinFunc`'s nil.
    use ScalarFuncSig as Sig;
    if matches!(
        function.pb_code(),
        Sig::CastIntAsInt
            | Sig::CastIntAsReal
            | Sig::CastIntAsDecimal
            | Sig::CastRealAsInt
            | Sig::CastRealAsReal
            | Sig::CastRealAsDecimal
            | Sig::CastDecimalAsInt
            | Sig::CastDecimalAsReal
            | Sig::CastDecimalAsDecimal
            | Sig::CastStringAsInt
            | Sig::CastStringAsReal
            | Sig::CastStringAsDecimal
            | Sig::CastTimeAsInt
            | Sig::CastTimeAsReal
            | Sig::CastTimeAsDecimal
            | Sig::CastDurationAsInt
            | Sig::CastDurationAsReal
            | Sig::CastDurationAsDecimal
            | Sig::CastJsonAsInt
            | Sig::CastJsonAsReal
            | Sig::CastJsonAsDecimal
    ) {
        return Some(Some(
            tidb_proto::tipb::InUnionMetadata {
                in_union: function.cast_in_union(),
            }
            .encode_to_vec(),
        ));
    }
    if function.func_name.lowercase() != "grouping" {
        return Some(None);
    }
    let metadata = function.grouping_metadata().ok()?;
    let mode = match metadata.mode() {
        crate::grouping::GroupingMode::BitAnd => tidb_proto::tipb::GroupingMode::ModeBitAnd,
        crate::grouping::GroupingMode::NumericCmp => tidb_proto::tipb::GroupingMode::ModeNumericCmp,
        crate::grouping::GroupingMode::NumericSet => tidb_proto::tipb::GroupingMode::ModeNumericSet,
    };
    Some(Some(
        tidb_proto::tipb::GroupingFunctionMetadata {
            mode: mode as i32,
            grouping_marks: metadata
                .grouping_marks()
                .iter()
                .map(|mark| tidb_proto::tipb::GroupingMark {
                    grouping_nums: mark.iter().copied().collect(),
                })
                .collect(),
        }
        .encode_to_vec(),
    ))
}

/// Go `expression.PushDownContext` as the aggregate and window lowering
/// read it: the converter, the evaluation context's rendering, and
/// `group_concat_max_len`.
pub trait PushDownContext {
    /// Go `PbConverter().ExprToPB`.
    fn expr_to_pb(&self, expr: &Expression) -> Option<Expr>;
    /// Go `expr.StringWithCtx(ctx.EvalCtx(), errors.RedactLogDisable)`.
    fn expr_string(&self, expr: &Expression) -> String;
    /// Go `GetGroupConcatMaxLen`.
    fn group_concat_max_len(&self) -> u64;
}
