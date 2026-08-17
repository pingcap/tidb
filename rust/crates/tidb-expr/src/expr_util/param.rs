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

//! Constant construction and parameter-marker handling.
//!
//! Go sources: `pkg/expression/util.go:1371` (`DatumToConstant`), `:1378`
//! (`ParamMarkerExpression`), `:1428` (`ConstructPositionExpr`), `:1433`
//! (`PosFromPositionExpr`), `:1449` (`GetStringFromConstant`), `:1463`
//! (`GetIntFromConstant`).

use crate::constant::{Constant, ParamMarker};
use crate::context::{Columns, EvalError};
use crate::expression::Expression;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

/// Go `DatumToConstant` (`util.go:1371`): a `Constant` carrying `d`, typed as
/// `tp` with `flag` added.
///
/// `tp` is Go's raw `byte` MySQL type code and `flag` its `uint` flag set;
/// both are kept in their typed Rust spellings.
#[must_use]
pub fn datum_to_constant(d: Datum, tp: FieldTypeCode, flag: u32) -> Constant {
    let mut field_type = FieldType::new(tp);
    field_type.add_flags(flag);
    Constant::new(d, field_type)
}

/// `// boundary:` Go `driver.ParamMarkerExpr`, which lives in
/// `pkg/types/parser_driver` -- above this crate.
///
/// These are exactly the two fields `ParamMarkerExpression` reads off it, so
/// no driver type is duplicated here.
#[derive(Clone, Debug)]
pub struct ParamMarkerValue {
    /// Go `v.Datum`: the parameter's current value.
    pub datum: Datum,
    /// Go `v.Order`: the parameter's position in the statement.
    pub order: i64,
}

/// Go `ParamMarkerExpression` (`util.go:1378`): builds the constant that
/// stands for a `?` placeholder.
///
/// The `ParamMarker` is attached only when the plan may be CACHED or the
/// caller explicitly asks for it (`need_param`). Without it the constant is an
/// ordinary literal, which is what lets a non-cached statement optimize
/// against the actual value.
///
/// `// boundary:` Go calls `types.InferParamTypeFromDatum(&v.Datum, tp)` to
/// derive the result type. That inference is not in `tidb-datatype` yet, so
/// the inferred type is a parameter here rather than a guess; passing `None`
/// reproduces Go's starting point, `types.NewFieldType(mysql.TypeUnspecified)`.
#[must_use]
pub fn param_marker_expression(
    v: &ParamMarkerValue,
    use_cache: bool,
    need_param: bool,
    inferred_type: Option<FieldType>,
) -> Constant {
    let field_type = inferred_type.unwrap_or_else(|| FieldType::new(FieldTypeCode::Unspecified));
    let mut constant = Constant::new(v.datum.clone(), field_type);
    if use_cache || need_param {
        constant.param_marker = Some(ParamMarker { order: v.order });
    }
    constant
}

/// `// boundary:` Go `ast.PositionExpr`, the `ORDER BY <n>` / `GROUP BY <n>`
/// node. `tidb-ast` does not carry it yet, so this is the two-field view
/// `ConstructPositionExpr` and `PosFromPositionExpr` use.
#[derive(Clone, Debug)]
pub struct PositionExpr {
    /// Go `N`: the literal position, used when `p` is absent.
    pub n: i32,
    /// Go `P`: the parameter marker the position came from, if any.
    pub p: Option<ParamMarkerValue>,
}

/// Go `ConstructPositionExpr` (`util.go:1428`).
#[must_use]
pub fn construct_position_expr(p: ParamMarkerValue) -> PositionExpr {
    PositionExpr { n: 0, p: Some(p) }
}

/// Go `GetStringFromConstant` (`util.go:1449`): the string value of a constant
/// expression.
///
/// `Ok(None)` is Go's `isNull == true`. A non-`Constant` argument is Go's
/// `errors.Errorf("Not a Constant expression %+v")`.
///
/// # Errors
///
/// Returns [`ConstantReadError`] when `value` is not a constant, or when
/// evaluating it to a string fails.
pub fn get_string_from_constant(
    value: &Expression,
    ctx: &impl Columns,
) -> Result<Option<String>, ConstantReadError> {
    let Expression::Constant(_) = value else {
        return Err(ConstantReadError::NotAConstant);
    };
    let datum = super::substitute::eval_once(value, ctx)?;
    if datum.is_null() {
        return Ok(None);
    }
    // Go's `EvalString` converts through the constant's own type; this reads
    // the already-evaluated datum's string form, which agrees for every kind a
    // string-valued constant can hold.
    match datum {
        Datum::Bytes(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
        Datum::Int(v) => Ok(Some(v.to_string())),
        Datum::UInt(v) => Ok(Some(v.to_string())),
        other => Ok(Some(format!("{other:?}"))),
    }
}

/// Go `GetIntFromConstant` (`util.go:1463`): the integer value of a constant,
/// read through its STRING form.
///
/// The detour through a string is Go's, not an accident: `strconv.Atoi` on the
/// string is what makes `'12abc'` a parse failure rather than the truncating
/// numeric conversion an `EvalInt` would perform.
///
/// A string that does not parse yields `Ok(None)` -- Go returns
/// `(0, true, nil)` there, an "is null" with NO error.
///
/// # Errors
///
/// Returns [`ConstantReadError`] when `value` is not a constant, or when
/// evaluating it fails.
pub fn get_int_from_constant(
    value: &Expression,
    ctx: &impl Columns,
) -> Result<Option<i32>, ConstantReadError> {
    let Some(text) = get_string_from_constant(value, ctx)? else {
        return Ok(None);
    };
    Ok(text.parse::<i32>().ok())
}

/// Go `PosFromPositionExpr` (`util.go:1433`): the position a `PositionExpr`
/// denotes.
///
/// Returns `(position, is_null)`. A literal position is never null; a
/// parameter-backed one is null when the parameter does not read as an
/// integer.
///
/// # Errors
///
/// Returns [`ConstantReadError`] when the parameter's constant cannot be read.
pub fn pos_from_position_expr(
    v: &PositionExpr,
    ctx: &impl Columns,
    inferred_type: Option<FieldType>,
) -> Result<(i32, bool), ConstantReadError> {
    let Some(marker) = v.p.as_ref() else {
        return Ok((v.n, false));
    };
    let value = param_marker_expression(marker, false, false, inferred_type);
    match get_int_from_constant(&Expression::Constant(value), ctx)? {
        Some(pos) => Ok((pos, false)),
        None => Ok((0, true)),
    }
}

/// A failure reading a value out of a constant expression.
#[derive(Clone, Debug)]
pub enum ConstantReadError {
    /// Go `errors.Errorf("Not a Constant expression %+v", value)`.
    NotAConstant,
    /// Go's `err` from `EvalString`.
    Eval(EvalError),
}

impl From<EvalError> for ConstantReadError {
    fn from(err: EvalError) -> Self {
        ConstantReadError::Eval(err)
    }
}

impl std::fmt::Display for ConstantReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConstantReadError::NotAConstant => write!(f, "not a Constant expression"),
            ConstantReadError::Eval(err) => write!(f, "{err:?}"),
        }
    }
}

impl std::error::Error for ConstantReadError {}
