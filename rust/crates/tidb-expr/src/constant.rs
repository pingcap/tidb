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

//! `pkg/expression/constant.go`: the `Constant` expression node and its
//! `ParamMarker`.
//!
//! Includes structural methods and contextual datum evaluation with current
//! parameters, deferred expressions, statement conversion flags and warnings.
//! Unported conversion diagnostics remain explicit errors. The typed `Eval*`
//! entrypoints, `GetType(ctx)`'s parameter inference, contextual comparison and
//! display, `CanonicalHashCode`, and `MemoryUsage` remain incomplete.

use std::hash::{Hash, Hasher};

use crate::context::EvalError;
use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, CONSTANT_FLAG, PARAMETER_FLAG};
use tidb_codec::{encode_int, hash_code};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

/// Go `ParamMarker`: a reference to a placeholder parameter in a prepared
/// statement, by its position in `sessionVars.PreparedParams`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ParamMarker {
    /// Go `order`: the parameter's index.
    pub order: i64,
}

/// Go `Constant`: a literal (or deferred/parameter) constant value.
#[derive(Clone, Debug, Default)]
pub struct Constant {
    /// Go `Value`: the constant's datum.
    pub value: Datum,

    /// Go `RetType` (a `*types.FieldType`; `None` mirrors a nil pointer).
    pub ret_type: Option<FieldType>,

    /// Go `DeferredExpr`: a deferred non-deterministic function, evaluated
    /// lazily when a plan-cache entry is reused.
    pub deferred_expr: Option<Box<Expression>>,

    /// Go `ParamMarker`: set when this constant references an `EXECUTE`
    /// parameter / user variable.
    pub param_marker: Option<ParamMarker>,

    /// Lazily-filled `HashCode` cache (Go `hashcode`).
    hashcode: Vec<u8>,

    /// Go `SubqueryRefID`: the id of the original subquery column, for display.
    pub subquery_ref_id: i64,

    /// Go embedded `collationInfo`.
    pub collation: CollationInfo,
}

impl Constant {
    /// Builds a plain literal constant with the given value and type.
    #[must_use]
    pub fn new(value: Datum, ret_type: FieldType) -> Self {
        Constant {
            value,
            ret_type: Some(ret_type),
            ..Default::default()
        }
    }

    /// Go `NewOne`: the unsigned `TINYINT(1)` constant used by boolean
    /// rewrites without triggering integral promotion.
    #[must_use]
    pub fn new_one() -> Self {
        Self::new(Datum::Int(1), specific_tiny_int_type(true))
    }

    /// Go `NewZero`: the unsigned `TINYINT(0)` constant used by boolean
    /// rewrites without triggering integral promotion.
    #[must_use]
    pub fn new_zero() -> Self {
        Self::new(Datum::Int(0), specific_tiny_int_type(true))
    }

    /// Go `NewNull`: a NULL constant declared as signed `TINYINT(1)`.
    #[must_use]
    pub fn new_null() -> Self {
        Self::new(Datum::Null, specific_tiny_int_type(false))
    }

    /// Go `GetStaticType`: the declared result type (`None` for a nil pointer).
    /// The context-dependent `GetType` param-type inference is deferred.
    #[must_use]
    pub fn get_static_type(&self) -> Option<&FieldType> {
        self.ret_type.as_ref()
    }

    /// Go `IsCorrelated`: a constant is never correlated.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        false
    }

    /// Go `ConstLevel`: a deferred or parameter constant is constant only within
    /// one context; a plain literal is strictly constant.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        if self.deferred_expr.is_some() || self.param_marker.is_some() {
            ConstLevel::ONLY_IN_CONTEXT
        } else {
            ConstLevel::STRICT
        }
    }

    /// A context-independent literal value. Parameter snapshots are not
    /// literals: using them for specialization would freeze one execution.
    #[must_use]
    pub fn literal_value(&self) -> Option<&Datum> {
        (self.const_level() == ConstLevel::STRICT).then_some(&self.value)
    }

    /// Evaluates without prepared parameters. Dynamic constants fail rather
    /// than silently returning the saved planning-time value.
    pub fn eval(&self) -> Result<Datum, EvalError> {
        self.eval_in(&crate::NoColumns)
    }

    /// Go `Constant.getLazyDatum/Eval`: lazy values come from this execution,
    /// while the planning value and expression remain immutable.
    pub fn eval_in(&self, ctx: &impl crate::Columns) -> Result<Datum, EvalError> {
        self.eval_lazy_in(ctx, |expression| {
            crate::eval_expression_once(expression, ctx)
        })
    }

    /// Runtime evaluation keeps the caller's row, as Go `getLazyDatum` does.
    /// It must not construct a new empty chunk at every deferred node.
    pub(crate) fn eval_on_row(
        &self,
        ctx: &impl crate::Columns,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<Datum, EvalError> {
        self.eval_lazy_in(ctx, |expression| expression.eval(ctx, row))
    }

    fn eval_lazy_in(
        &self,
        ctx: &impl crate::Columns,
        evaluate_deferred: impl FnOnce(&Expression) -> Result<Datum, EvalError>,
    ) -> Result<Datum, EvalError> {
        let value = if let Some(marker) = self.param_marker {
            let order = usize::try_from(marker.order)
                .map_err(|_| EvalError::Unsupported("unbound prepared parameter"))?;
            ctx.param_value(order)?
        } else if let Some(deferred) = self.deferred_expr.as_deref() {
            evaluate_deferred(deferred)?
        } else {
            return Ok(self.value.clone());
        };
        if self.deferred_expr.is_none() || value.is_null() {
            return Ok(value);
        }
        let target = self.ret_type.as_ref().ok_or(EvalError::Unsupported(
            "deferred constant has no result type",
        ))?;
        if let Datum::Decimal(value) = value {
            // Go adjustDecimal only pads a short fraction. It neither
            // narrows precision nor rounds away existing fractional digits.
            return Ok(Datum::Decimal(
                if i64::from(value.precision_and_frac().1) < target.decimal() {
                    value.round_to_scale(target.decimal() as i32)
                } else {
                    value
                },
            ));
        }
        let warnings = ConversionWarnings(ctx);
        let zone = ctx.time_zone();
        let context = tidb_datatype::ConversionContext::new(
            ctx.type_flags(),
            tidb_datatype::ConversionLocation::from_time_zone(&zone),
            &warnings,
        );
        let converted = value
            .convert_to_in_context(target, &context, &zone)
            .map_err(|_| {
                EvalError::Unsupported("deferred conversion diagnostic routing is not yet ported")
            })?;
        if let Some(error) = converted.error {
            return Err(EvalError::Conversion(error));
        }
        Ok(converted.value)
    }

    /// Go `HashCode` (= `getHashCode(false)`), cached on first call:
    /// - a deferred constant hashes as its deferred expression;
    /// - a parameter hashes as `[parameterFlag, EncodeInt(order)]`;
    /// - a plain literal hashes as `[constantFlag, HashCode(Value)]`.
    pub fn hash_code(&mut self) -> &[u8] {
        if !self.hashcode.is_empty() {
            return &self.hashcode;
        }
        if let Some(deferred) = &mut self.deferred_expr {
            self.hashcode = deferred.hash_code().to_vec();
        } else if let Some(param) = self.param_marker {
            self.hashcode.push(PARAMETER_FLAG);
            encode_int(&mut self.hashcode, param.order);
        } else {
            self.hashcode.push(CONSTANT_FLAG);
            self.hashcode.extend_from_slice(&hash_code(&self.value));
        }
        &self.hashcode
    }

    /// Go `Constant.Hash64`: hashes every field that participates in
    /// [`Self::equals`] while deliberately ignoring the byte-cache and
    /// `SubqueryRefID`, as the source does.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = Fnv64::default();
        hash_constant(self, &mut hasher);
        hasher.finish()
    }

    /// Go `Constant.Equals`: structural equality for plan hash/equality keys.
    /// This is distinct from Go `Constant.Equal(ctx, expr)`, which evaluates
    /// values through an `EvalContext` and remains deferred.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self.ret_type == other.ret_type
            && self.collation == other.collation
            && optional_expression_equals(&self.deferred_expr, &other.deferred_expr)
            && self.param_marker == other.param_marker
            && datum_equals(&self.value, &other.value)
    }
}

/// Borrow the active evaluator's warning sink; no per-conversion warning store.
struct ConversionWarnings<'a, C>(&'a C);

impl<C: crate::Columns> tidb_datatype::ConversionWarningAppender for ConversionWarnings<'_, C> {
    fn append_conversion_warning(&self, warning: tidb_error::terror::TerrorError) {
        let warning = warning.to_sql_error();
        self.0.append_warning(warning.code, &warning.message);
    }
}

const FNV_OFFSET_64: u64 = 14_695_981_039_346_656_037;
const FNV_PRIME_64: u64 = 1_099_511_628_211;

struct Fnv64(u64);

impl Default for Fnv64 {
    fn default() -> Self {
        Self(FNV_OFFSET_64)
    }
}

impl Hasher for Fnv64 {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(FNV_PRIME_64);
        }
    }
}

fn hash_constant(constant: &Constant, hasher: &mut Fnv64) {
    hash_optional_field_type(constant.ret_type.as_ref(), hasher);
    hash_collation(&constant.collation, hasher);
    if let Some(deferred) = &constant.deferred_expr {
        1_u8.hash(hasher);
        hash_expression(deferred, hasher);
    } else if let Some(param) = constant.param_marker {
        PARAMETER_FLAG.hash(hasher);
        param.order.hash(hasher);
    } else {
        CONSTANT_FLAG.hash(hasher);
        hash_code(&constant.value).hash(hasher);
    }
}

fn hash_expression(expression: &Expression, hasher: &mut Fnv64) {
    match expression {
        Expression::Constant(constant) => {
            0_u8.hash(hasher);
            hash_constant(constant, hasher);
        }
        Expression::Column(column) => {
            1_u8.hash(hasher);
            hash_column(column, hasher);
        }
        Expression::CorrelatedColumn(column) => {
            2_u8.hash(hasher);
            hash_column(&column.column, hasher);
        }
        Expression::ScalarFunction(function) => {
            3_u8.hash(hasher);
            function.func_name.lowercase().hash(hasher);
            hash_optional_field_type(function.ret_type.as_ref(), hasher);
            function.args.len().hash(hasher);
            for argument in &function.args {
                hash_expression(argument, hasher);
            }
        }
    }
}

fn hash_column(column: &crate::column::Column, hasher: &mut Fnv64) {
    hash_optional_field_type(column.ret_type.as_ref(), hasher);
    column.id.hash(hasher);
    column.unique_id.hash(hasher);
    column.index.hash(hasher);
    match &column.virtual_expr {
        Some(expression) => {
            1_u8.hash(hasher);
            hash_expression(expression, hasher);
        }
        None => 0_u8.hash(hasher),
    }
    column.orig_name.hash(hasher);
    column.is_hidden.hash(hasher);
    column.is_prefix.hash(hasher);
    column.in_operand.hash(hasher);
    hash_collation(&column.collation, hasher);
    column.correlated_col_unique_id.hash(hasher);
}

fn hash_optional_field_type(field_type: Option<&FieldType>, hasher: &mut Fnv64) {
    match field_type {
        Some(field_type) => {
            1_u8.hash(hasher);
            field_type.hash(hasher);
        }
        None => 0_u8.hash(hasher),
    }
}

fn hash_collation(collation: &CollationInfo, hasher: &mut Fnv64) {
    collation.coercibility().0.hash(hasher);
    collation.has_coercibility().hash(hasher);
    collation.repertoire().0.hash(hasher);
    let (charset, name) = collation.charset_and_collation();
    charset.hash(hasher);
    name.hash(hasher);
    collation.is_explicit_charset().hash(hasher);
}

pub(crate) fn optional_expression_equals(
    left: &Option<Box<Expression>>,
    right: &Option<Box<Expression>>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => expression_equals(left, right),
        (None, None) => true,
        _ => false,
    }
}

pub(crate) fn expression_equals(left: &Expression, right: &Expression) -> bool {
    match (left, right) {
        (Expression::Constant(left), Expression::Constant(right)) => left.equals(right),
        (Expression::Column(left), Expression::Column(right)) => column_equals(left, right),
        (Expression::CorrelatedColumn(left), Expression::CorrelatedColumn(right)) => {
            column_equals(&left.column, &right.column)
        }
        (Expression::ScalarFunction(left), Expression::ScalarFunction(right)) => {
            left.func_name.lowercase() == right.func_name.lowercase()
                && left.ret_type == right.ret_type
                && left.args.len() == right.args.len()
                && left
                    .args
                    .iter()
                    .zip(&right.args)
                    .all(|(left, right)| expression_equals(left, right))
        }
        _ => false,
    }
}

fn column_equals(left: &crate::column::Column, right: &crate::column::Column) -> bool {
    left.ret_type == right.ret_type
        && optional_expression_equals(&left.virtual_expr, &right.virtual_expr)
        && left.id == right.id
        && left.unique_id == right.unique_id
        && left.index == right.index
        && left.orig_name == right.orig_name
        && left.is_hidden == right.is_hidden
        && left.is_prefix == right.is_prefix
        && left.in_operand == right.in_operand
        && left.collation == right.collation
        && left.correlated_col_unique_id == right.correlated_col_unique_id
}

fn datum_equals(left: &Datum, right: &Datum) -> bool {
    match (left, right) {
        (Datum::Real(left), Datum::Real(right)) | (Datum::Float32(left), Datum::Float32(right)) => {
            left.to_bits() == right.to_bits()
        }
        _ => left == right,
    }
}

fn specific_tiny_int_type(unsigned: bool) -> FieldType {
    FieldType::new(FieldTypeCode::Tiny)
        .with_unsigned(unsigned)
        .with_flen(1)
        .with_decimal(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Datum, FieldType, FieldTypeFlags};

    fn ft() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    struct Parameters(Vec<Datum>);

    impl crate::Columns for Parameters {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            self.0
                .get(order)
                .cloned()
                .ok_or(EvalError::Unsupported("unbound prepared parameter"))
        }
    }

    fn parameter(order: i64) -> Expression {
        let mut constant = Constant::new(Datum::Int(99), ft());
        constant.param_marker = Some(ParamMarker { order });
        Expression::Constant(constant)
    }

    fn scalar(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(crate::expression::ScalarFunction::new(
            tidb_ast::CiString::new(name),
            ft(),
            args,
        ))
    }

    fn int(value: i64) -> Expression {
        Expression::Constant(Constant::new(Datum::Int(value), ft()))
    }

    #[test]
    fn prepared_constant_reads_current_typed_value_not_saved_literal() {
        let expression = parameter(0);
        for value in [
            Datum::Null,
            Datum::Int(-7),
            Datum::UInt(u64::MAX),
            Datum::Real(1.25),
            Datum::new_bytes(vec![0, 255, 1]),
            Datum::Decimal(tidb_datatype::Decimal::from_scaled_i128(12340, 3)),
        ] {
            assert_eq!(
                crate::eval_expression_once(&expression, &Parameters(vec![value.clone()])).unwrap(),
                value,
            );
        }
        assert!(crate::eval_expression_once(&expression, &crate::NoColumns).is_err());
        assert!(
            crate::eval_expression_once(&parameter(-1), &Parameters(vec![Datum::Int(3)])).is_err()
        );
        assert!(
            crate::eval_expression_once(&parameter(1), &Parameters(vec![Datum::Int(3)])).is_err()
        );
        let Expression::Constant(saved) = expression else {
            unreachable!()
        };
        assert_eq!(saved.value, Datum::Int(99));
    }

    #[test]
    fn prepared_constants_never_supply_a_context_free_folded_value() {
        assert!(crate::constant_fold::folded_value(&parameter(0)).is_none());
    }

    #[test]
    fn prepared_deferred_constant_converts_current_value_without_mutation() {
        let mut constant = Constant::new(Datum::Real(100.0), FieldType::new(FieldTypeCode::Double));
        constant.deferred_expr = Some(Box::new(scalar("plus", vec![parameter(0), int(1)])));
        for (value, expected) in [
            (Datum::Int(1), Datum::Real(2.0)),
            (Datum::Int(9), Datum::Real(10.0)),
            (Datum::Null, Datum::Null),
        ] {
            assert_eq!(
                constant.eval_in(&Parameters(vec![value])).unwrap(),
                expected
            );
        }
        assert_eq!(constant.value, Datum::Real(100.0));
        assert!(constant.literal_value().is_none());

        // getLazyDatum prioritizes ParamMarker; Eval still applies the
        // declared result conversion whenever DeferredExpr is present.
        constant.param_marker = Some(ParamMarker { order: 0 });
        assert_eq!(
            constant.eval_in(&Parameters(vec![Datum::Int(7)])).unwrap(),
            Datum::Real(7.0),
        );
    }

    #[test]
    fn prepared_deferred_decimal_only_extends_declared_fraction() {
        let mut constant = Constant::new(
            Datum::Null,
            FieldType::new(FieldTypeCode::NewDecimal).with_decimal(2),
        );
        constant.deferred_expr = Some(Box::new(parameter(0)));
        let context = Parameters(vec![Datum::Decimal(
            tidb_datatype::Decimal::from_scaled_i128(12345, 3),
        )]);
        assert_eq!(
            constant.eval_in(&context).unwrap().sql_string().unwrap(),
            "12.345",
        );
        constant.ret_type.as_mut().unwrap().set_decimal(5);
        let Datum::Decimal(value) = constant.eval_in(&context).unwrap() else {
            panic!("deferred decimal lost its datum kind");
        };
        assert_eq!(value.to_string(), "12.34500");
        assert_eq!(value.precision_and_frac().1, 5);
    }

    #[test]
    fn prepared_folding_preserves_parameter_dependencies_in_lazy_functions() {
        let mut expression = Expression::ScalarFunction(crate::expression::ScalarFunction::new(
            tidb_ast::CiString::new("if"),
            ft(),
            vec![
                parameter(0),
                Expression::Constant(Constant::new(Datum::Int(7), ft())),
                Expression::Constant(Constant::new(Datum::Int(11), ft())),
            ],
        ));
        crate::fold_constant_in_mode(
            &mut expression,
            &Parameters(vec![Datum::Int(1)]),
            crate::ConstantFoldMode::Normal,
        );
        for (input, expected) in [(Datum::Int(1), 7), (Datum::Int(0), 11), (Datum::Null, 11)] {
            assert_eq!(
                crate::eval_expression_once(&expression, &Parameters(vec![input])).unwrap(),
                Datum::Int(expected),
            );
        }
    }

    #[test]
    fn prepared_public_folding_keeps_nested_lazy_branches_live() {
        let builder = crate::expr_util::PreservingFunctionBuilder;
        let mut options = crate::expr_util::FoldOptions::new(&builder);
        options.use_plan_cache = true;
        // Go EXECUTE results for parameters [0, 1, NULL, -2]. Compare with
        // those answers as well as the unfurled tree, not just two Rust paths.
        for (original, answers) in [
            (
                scalar("if", vec![parameter(0), int(7), int(11)]),
                [Some(11), Some(7), Some(11), Some(7)],
            ),
            (
                scalar("ifnull", vec![parameter(0), int(11)]),
                [Some(0), Some(1), Some(11), Some(-2)],
            ),
            (
                scalar("isnull", vec![parameter(0)]),
                [Some(0), Some(0), Some(1), Some(0)],
            ),
            (
                scalar("case_when", vec![parameter(0), int(7), int(11)]),
                [Some(11), Some(7), Some(11), Some(7)],
            ),
            (
                scalar("plus", vec![parameter(0), int(11)]),
                [Some(11), Some(12), None, Some(9)],
            ),
            (
                scalar(
                    "plus",
                    vec![scalar("if", vec![parameter(0), int(7), int(11)]), int(3)],
                ),
                [Some(14), Some(10), Some(14), Some(10)],
            ),
        ] {
            let folded = crate::expr_util::fold_constant_with(
                &original,
                &Parameters(vec![Datum::Int(1)]),
                &options,
            );
            for (value, expected) in [Datum::Int(0), Datum::Int(1), Datum::Null, Datum::Int(-2)]
                .into_iter()
                .zip(answers)
            {
                let ctx = Parameters(vec![value]);
                let expected = expected.map_or(Datum::Null, Datum::Int);
                assert_eq!(
                    crate::eval_expression_once(&original, &ctx).unwrap(),
                    expected
                );
                assert_eq!(
                    crate::eval_expression_once(&folded, &ctx).unwrap(),
                    expected,
                    "{original:?}",
                );
            }
        }
    }

    #[test]
    fn prepared_overflow_reports_current_operands() {
        let expression = scalar("plus", vec![parameter(0), int(1)]);
        let literal = scalar("plus", vec![int(i64::MAX), int(1)]);
        let expected = crate::eval_expression_once(&literal, &crate::NoColumns);
        assert!(expected.is_err());
        assert_eq!(
            crate::eval_expression_once(&expression, &Parameters(vec![Datum::Int(i64::MAX)])),
            expected,
        );
    }

    #[test]
    fn prepared_decimal_batch_specialization_does_not_use_saved_parameter() {
        use crate::column::Column;
        use tidb_chunk::chunk::Chunk;
        use tidb_datatype::Decimal;

        let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
        decimal.set_decimal(2);
        let column = |index: i64| {
            let mut column = Column::new(index + 1, decimal.clone());
            column.index = index;
            Expression::Column(column)
        };
        let mut marker = Constant::new(Datum::Int(1), ft());
        marker.param_marker = Some(ParamMarker { order: 0 });
        let minus = Expression::ScalarFunction(crate::expression::ScalarFunction::new(
            tidb_ast::CiString::new("minus"),
            decimal.clone(),
            vec![Expression::Constant(marker), column(1)],
        ));
        let expression = Expression::ScalarFunction(crate::expression::ScalarFunction::new(
            tidb_ast::CiString::new("mul"),
            decimal.clone(),
            vec![column(0), minus],
        ));
        let suite = crate::evaluator::EvaluatorSuite::new(vec![expression.clone()], false);
        for (value, coefficient) in [
            (Datum::Int(2), Some(175000)),
            (Datum::Null, None),
            (Datum::Int(1), Some(75000)),
            (Datum::Int(-2), Some(-225000)),
        ] {
            let ctx = Parameters(vec![value]);
            let mut input = Chunk::new_with_capacity(&[decimal.clone(), decimal.clone()], 1);
            input.append_datum(0, &Datum::Decimal(Decimal::from_scaled_i128(1000, 2)));
            input.append_datum(1, &Datum::Decimal(Decimal::from_scaled_i128(25, 2)));
            let expected = coefficient.map_or(Datum::Null, |value| {
                Datum::Decimal(Decimal::from_scaled_i128(value, 4))
            });
            assert_eq!(expression.eval(&ctx, input.get_row(0)).unwrap(), expected);
            let mut output = Chunk::new_with_capacity(std::slice::from_ref(&decimal), 1);
            suite.run(&ctx, &mut input, &mut output).unwrap();
            assert_eq!(output.get_row(0).get_datum(0, &decimal), expected);
        }
    }

    #[test]
    fn const_level_reflects_deferred_and_param() {
        let lit = Constant::new(Datum::Int(1), ft());
        assert_eq!(lit.const_level(), ConstLevel::STRICT);
        assert!(!lit.is_correlated());

        let mut param = Constant::new(Datum::Null, ft());
        param.param_marker = Some(ParamMarker { order: 3 });
        assert_eq!(param.const_level(), ConstLevel::ONLY_IN_CONTEXT);
    }

    #[test]
    fn literal_hash_code_is_flag_plus_value_encoding() {
        let mut c = Constant::new(Datum::Int(1), ft());
        let mut expected = vec![CONSTANT_FLAG];
        expected.extend_from_slice(&hash_code(&Datum::Int(1)));
        assert_eq!(c.hash_code(), expected.as_slice());
        // Cached.
        assert_eq!(c.hash_code(), expected.as_slice());
    }

    #[test]
    fn param_hash_code_is_flag_plus_order() {
        let mut c = Constant::new(Datum::Null, ft());
        c.param_marker = Some(ParamMarker { order: 5 });
        let mut expected = vec![PARAMETER_FLAG];
        encode_int(&mut expected, 5);
        assert_eq!(c.hash_code(), expected.as_slice());
    }

    #[test]
    fn distinct_literals_hash_differently() {
        let mut a = Constant::new(Datum::Int(1), ft());
        let mut b = Constant::new(Datum::Int(2), ft());
        assert_ne!(a.hash_code(), b.hash_code());
    }

    #[test]
    fn clone_is_deep() {
        let mut c = Constant::new(Datum::Int(9), ft());
        c.subquery_ref_id = 7;
        let d = c.clone();
        assert_eq!(d.value, Datum::Int(9));
        assert_eq!(d.subquery_ref_id, 7);
    }

    /// Source: `pkg/expression/constant_test.go::TestSpecificConstant`.
    #[test]
    fn specific_constant_matches_source() {
        let cases = [
            (Constant::new_one(), Datum::Int(1), true),
            (Constant::new_zero(), Datum::Int(0), true),
            (Constant::new_null(), Datum::Null, false),
        ];

        for (constant, expected_value, expected_unsigned) in cases {
            assert_eq!(constant.value, expected_value);
            let ret_type = constant.ret_type.expect("specific constants have a type");
            assert_eq!(ret_type.code(), FieldTypeCode::Tiny);
            assert_eq!(ret_type.flen(), 1);
            assert_eq!(ret_type.decimal(), 0);
            assert_eq!(ret_type.is_unsigned(), expected_unsigned);
        }
    }

    /// Source: `pkg/expression/constant_test.go::TestConstantHashEquals`.
    #[test]
    fn constant_hash_equals_matches_source() {
        let int_type = FieldType::new(FieldTypeCode::LongLong)
            .with_added_flags(FieldTypeFlags::BINARY)
            .with_flen(20);
        let cst1 = Constant::new(Datum::Int(2333), int_type.clone());
        let mut cst2 = Constant::new(Datum::Int(2333), int_type);

        assert_eq!(cst1.hash64(), cst2.hash64());
        assert!(cst1.equals(&cst2));

        cst2.value = Datum::Int(2334);
        assert_ne!(cst1.hash64(), cst2.hash64());
        assert!(!cst1.equals(&cst2));

        cst2.value = Datum::Int(2333);
        cst2.ret_type = Some(FieldType::new(FieldTypeCode::VarString));
        assert_ne!(cst1.hash64(), cst2.hash64());
        assert!(!cst1.equals(&cst2));
    }
}
