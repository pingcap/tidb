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

//! `pkg/expression/evaluator.go`: evaluate a projection's calculated
//! expressions before transferring any direct input-column owners.

use std::collections::HashMap;
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_chunk::chunk_util::ColumnSwapHelper;

use crate::context::{Columns, EvalError};
use crate::expression::Expression;

/// Go `HasGetSetVarFunc`: whether an expression contains a user-variable read
/// or assignment at any depth.
#[must_use]
pub fn has_get_set_var_func(expression: &Expression) -> bool {
    let Expression::ScalarFunction(function) = expression else {
        return false;
    };

    let name = function.func_name.lowercase();
    name == "setvar"
        || name == "getvar"
        || name.starts_with("getvar_")
        || function.get_args().iter().any(has_get_set_var_func)
}

/// Go `Vectorizable`: whether expressions may be evaluated column by column.
///
/// User-variable functions require select-list order for every row. Sequence
/// functions also require row-major order when a top-level `nextval` is mixed
/// with `lastval`/`setval`, or when more than one top-level `nextval` appears.
#[must_use]
pub fn vectorizable(expressions: &[Expression]) -> bool {
    if expressions.iter().any(has_get_set_var_func) {
        return false;
    }

    let mut nextval = 0;
    let mut lastval = 0;
    let mut setval = 0;
    for expression in expressions {
        let Expression::ScalarFunction(function) = expression else {
            continue;
        };
        match function.func_name.lowercase() {
            "nextval" => nextval += 1,
            "lastval" => lastval += 1,
            "setval" => setval += 1,
            _ => {}
        }
    }

    !((nextval > 0 && (lastval > 0 || setval > 0)) || nextval > 1)
}

/// A failure from [`EvaluatorSuite::run`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvaluatorError {
    /// A calculated expression failed.
    Eval(EvalError),
    /// The chunk ownership transfer rejected an invalid chunk state.
    Chunk(&'static str),
}

impl From<EvalError> for EvaluatorError {
    fn from(error: EvalError) -> Self {
        EvaluatorError::Eval(error)
    }
}

/// Immutable projection expressions and logical column mapping. Actual input
/// column ownership is discovered separately by each execution's suite.
pub struct EvaluatorProgram {
    calculated_output_indexes: Vec<usize>,
    calculated: Vec<Expression>,
    vectorizable: bool,
    column_mapping: HashMap<usize, Vec<usize>>,
}

impl EvaluatorProgram {
    /// Compile the context-independent part of Go `NewEvaluatorSuite`.
    ///
    /// When `avoid_column_evaluator` is true, direct columns are calculated
    /// cell by cell like any other expression. Otherwise their resolved input
    /// indexes are grouped into one [`ColumnSwapHelper`].
    #[must_use]
    pub fn new(exprs: Vec<Expression>, avoid_column_evaluator: bool) -> Self {
        let mut calculated = Vec::with_capacity(exprs.len());
        let mut calculated_output_indexes = Vec::with_capacity(exprs.len());
        let mut column_mapping = HashMap::<usize, Vec<usize>>::new();

        for (output_index, expression) in exprs.into_iter().enumerate() {
            if !avoid_column_evaluator {
                if let Expression::Column(column) = &expression {
                    let input_index = usize::try_from(column.index)
                        .expect("projection column index must be resolved");
                    column_mapping
                        .entry(input_index)
                        .or_default()
                        .push(output_index);
                    continue;
                }
            }
            calculated_output_indexes.push(output_index);
            calculated.push(expression);
        }

        let vectorizable = vectorizable(&calculated);
        Self {
            calculated_output_indexes,
            calculated,
            vectorizable,
            column_mapping,
        }
    }
}

/// Go `EvaluatorSuite`: executes a projection program with an execution-local
/// column ownership cache. Calculated expressions finish before owner moves,
/// so an evaluation error cannot leave the input chunk half-consumed.
pub struct EvaluatorSuite {
    program: Arc<EvaluatorProgram>,
    column_swap_helper: Option<ColumnSwapHelper>,
}

impl EvaluatorSuite {
    /// Go `NewEvaluatorSuite`: compile and instantiate a fresh program.
    #[must_use]
    pub fn new(exprs: Vec<Expression>, avoid_column_evaluator: bool) -> Self {
        Self::from_program(Arc::new(EvaluatorProgram::new(
            exprs,
            avoid_column_evaluator,
        )))
    }

    /// Instantiate without cloning or reclassifying expression trees. Go's
    /// merged column mapping depends on the first input chunk of this execution.
    #[must_use]
    pub fn from_program(program: Arc<EvaluatorProgram>) -> Self {
        let column_swap_helper = (!program.column_mapping.is_empty())
            .then(|| ColumnSwapHelper::from_mapping(program.column_mapping.clone()));
        Self {
            program,
            column_swap_helper,
        }
    }

    /// Go `EvaluatorSuite.Vectorizable`.
    #[must_use]
    pub fn vectorizable(&self) -> bool {
        self.program.vectorizable
    }

    /// Go `EvaluatorSuite.Run`.
    ///
    /// Safe expressions are evaluated column by column. Expressions with
    /// order-sensitive side effects are evaluated in select-list order for
    /// each row. Both modes finish before the helper transfers the first
    /// direct-column owner.
    pub fn run<C: Columns>(
        &self,
        ctx: &C,
        input: &mut Chunk,
        output: &mut Chunk,
    ) -> Result<(), EvaluatorError> {
        let rows = input.num_rows();
        // TPC-H q17/q19's revenue expression — `mul(col, minus(const, col))`
        // over DECIMAL input columns — is the single hottest per-row
        // computation in the suite. The generic path rebuilds two `Decimal`s
        // (heap storage) and walks signature dispatch for every row; the
        // specialized form multiplies i128 coefficients in place.
        let program = &self.program;
        if program.calculated.len() == 1
            && decimal_mul_minus_const_column(
                &program.calculated[0],
                program.calculated_output_indexes[0],
                input,
                output,
            )?
        {
            return Ok(());
        }
        if program.vectorizable {
            for (output_index, expression) in program
                .calculated_output_indexes
                .iter()
                .zip(&program.calculated)
            {
                if let Expression::Constant(constant) = expression {
                    // Go Constant.VecEval* broadcasts only non-deferred
                    // constants. Deferred expressions still consume rows;
                    // parameter values are read anew for every chunk.
                    if constant.deferred_expr.is_none() {
                        if rows != 0 {
                            let value = constant.eval_in(ctx)?;
                            for _ in 0..rows {
                                output.append_datum(*output_index, &value);
                            }
                        }
                        continue;
                    }
                }
                for row_index in 0..rows {
                    let value = expression.eval(ctx, input.get_row(row_index))?;
                    output.append_datum(*output_index, &value);
                }
            }
        } else {
            for row_index in 0..rows {
                for (output_index, expression) in program
                    .calculated_output_indexes
                    .iter()
                    .zip(&program.calculated)
                {
                    let value = expression.eval(ctx, input.get_row(row_index))?;
                    output.append_datum(*output_index, &value);
                }
            }
        }

        if let Some(helper) = &self.column_swap_helper {
            helper
                .swap_columns(input, output)
                .map_err(EvaluatorError::Chunk)?;
        }
        Ok(())
    }
}

/// Recognizes `mul(col_a, minus(const_one, col_b))` over two DECIMAL input
/// columns — TPC-H q17/q19's `l_extendedprice * (1 - l_discount)` — and
/// evaluates it for the whole chunk with i128 coefficient arithmetic.
/// Returns `Ok(None)` when the expression does not match the shape; the
/// caller falls back to per-row evaluation.
fn decimal_mul_minus_const_column(
    expression: &Expression,
    output_index: usize,
    input: &mut Chunk,
    output: &mut Chunk,
) -> Result<bool, EvaluatorError> {
    use tidb_datatype::{Decimal, FieldTypeCode};

    let Expression::ScalarFunction(mul) = expression else {
        return Ok(false);
    };
    if !mul.func_name.lowercase().eq_ignore_ascii_case("mul") || mul.args.len() != 2 {
        return Ok(false);
    }
    // The two operands in either order: a DECIMAL column, and
    // `minus(<const 1>, <DECIMAL column>)`.
    let (column_side, minus_side) = (&mul.args[0], &mul.args[1]);
    let resolve_pair = |a: &Expression, b: &Expression| -> Option<(usize, usize, i64)> {
        let Expression::Column(a_col) = a else {
            return None;
        };
        let a_index = usize::try_from(a_col.index).ok()?;
        let a_decimal = a_col.get_static_type()?.code() == FieldTypeCode::NewDecimal;
        if !a_decimal {
            return None;
        }
        let Expression::ScalarFunction(minus) = b else {
            return None;
        };
        if !minus.func_name.lowercase().eq_ignore_ascii_case("minus") || minus.args.len() != 2 {
            return None;
        }
        let one = match &minus.args[0] {
            Expression::Constant(constant) => match constant.literal_value() {
                Some(tidb_datatype::Datum::Int(value)) if *value == 1 => *value,
                _ => return None,
            },
            _ => return None,
        };
        let Expression::Column(b_col) = &minus.args[1] else {
            return None;
        };
        let b_index = usize::try_from(b_col.index).ok()?;
        let b_decimal = b_col.get_static_type()?.code() == FieldTypeCode::NewDecimal;
        if !b_decimal {
            return None;
        }
        Some((a_index, b_index, one))
    };
    let Some((a_index, b_index, _one)) =
        resolve_pair(column_side, minus_side).or_else(|| resolve_pair(minus_side, column_side))
    else {
        return Ok(false);
    };
    if std::env::var("TIDB_DEBUG_FP").is_ok() {
        eprintln!("[fp] shape matched a={a_index} b={b_index}");
    }

    // Read both columns as i128 coefficients with their scales. A NULL or an
    // out-of-i128 value on either side falls back to the generic path.
    let rows = input.num_rows();
    let mut coefficients = Vec::with_capacity(rows);
    let mut scale: Option<u32> = None;
    for row_index in 0..rows {
        let row = input.get_row(row_index);
        // A child chunk whose columns hold fewer rows than the chunk reports
        // (virtual or constant columns materialized lazily) cannot serve the
        // typed read; fall back to the generic evaluator.
        let a_col = input.column(a_index);
        let b_col = input.column(b_index);
        if a_col.rows() <= row_index || b_col.rows() <= row_index {
            return Ok(false);
        }
        if row.is_null(a_index) || row.is_null(b_index) {
            return Ok(false);
        }
        let Some((ca, sa)) = input.column(a_index).get_my_decimal_i128_scaled(row_index) else {
            return Ok(false);
        };
        let Some((cb, sb)) = input.column(b_index).get_my_decimal_i128_scaled(row_index) else {
            return Ok(false);
        };
        // (1 - discount): rescale the constant 1 into b's scale, then subtract.
        let one_scaled = match 10i128.checked_pow(sb) {
            Some(power) => power,
            None => return Ok(false),
        };
        let numerator = one_scaled - cb;
        // Multiply: coefficient product, scale sum.
        let Some(product) = ca.checked_mul(numerator) else {
            return Ok(false);
        };
        coefficients.push(product);
        let combined = sa + sb;
        match scale {
            None => scale = Some(combined),
            Some(existing) if existing != combined => return Ok(false),
            _ => {}
        }
    }

    let Some(result_scale) = scale else {
        return Ok(false);
    };
    if std::env::var("TIDB_DEBUG_FP").is_ok() {
        eprintln!(
            "[fp] collecting done, rows={} scale={}",
            coefficients.len(),
            result_scale
        );
    }
    for coefficient in coefficients {
        // The result type's scale may differ from the natural one; building
        // the Decimal at the natural scale lets the projection's cast (if
        // any) settle the final form exactly as the generic path does.
        let decimal = Decimal::from_scaled_i128(coefficient, result_scale);
        match decimal.to_my_decimal() {
            Ok(my_decimal) => output.append_my_decimal(output_index, &my_decimal),
            // An unrepresentable value falls back to NULL exactly like the
            // generic path's error-to-NULL handling for out-of-range results.
            Err(_) => output.append_datum(output_index, &tidb_datatype::Datum::Null),
        }
    }
    if std::env::var("TIDB_DEBUG_FP").is_ok() {
        eprintln!("[fp] appended all cells");
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    use crate::column::Column;
    use crate::constant::{Constant, ParamMarker};
    use crate::expression::ScalarFunction;
    use crate::NoColumns;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    fn string() -> FieldType {
        FieldType::new(FieldTypeCode::VarString)
    }

    fn input_column(index: i64) -> Expression {
        let mut column = Column::new(index + 1, long());
        column.index = index;
        Expression::Column(column)
    }

    fn int_const(value: i64) -> Expression {
        Expression::Constant(Constant::new(Datum::Int(value), long()))
    }

    fn string_const(value: &str) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Bytes(value.as_bytes().to_vec()),
            string(),
        ))
    }

    fn scalar(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), long(), args))
    }

    struct CountedParameter {
        value: Result<Datum, EvalError>,
        reads: Cell<usize>,
    }

    impl Columns for CountedParameter {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn param_value(&self, order: usize) -> Result<Datum, EvalError> {
            assert_eq!(order, 0);
            self.reads.set(self.reads.get() + 1);
            self.value.clone()
        }
    }

    fn parameter(field_type: FieldType) -> Expression {
        let mut constant = Constant::new(Datum::Null, field_type);
        constant.param_marker = Some(ParamMarker { order: 0 });
        Expression::Constant(constant)
    }

    #[test]
    fn constant_batch_reads_current_parameter_once_per_nonempty_chunk() {
        // Go Constant.VecEval* -> genVecFromConstExpr evaluates once, not
        // once per row. Reusing the suite must not freeze that execution.
        for (field_type, values) in [
            (long(), vec![Datum::Int(7), Datum::Null, Datum::Int(-9)]),
            (
                string(),
                vec![
                    Datum::Bytes(b"abc".to_vec()),
                    Datum::Bytes(vec![]),
                    Datum::Null,
                ],
            ),
        ] {
            let suite = EvaluatorSuite::new(vec![parameter(field_type.clone())], false);
            for value in values {
                for rows in [0, 1, 1024] {
                    let ctx = CountedParameter {
                        value: Ok(value.clone()),
                        reads: Cell::new(0),
                    };
                    let mut input = Chunk::new_with_capacity(&[], rows);
                    input.set_num_virtual_rows(rows);
                    let mut output =
                        Chunk::new_with_capacity(std::slice::from_ref(&field_type), rows);
                    suite.run(&ctx, &mut input, &mut output).unwrap();
                    assert_eq!(ctx.reads.get(), usize::from(rows != 0));
                    assert_eq!(output.num_rows(), rows);
                    for row in 0..rows {
                        let row = output.get_row(row);
                        match &value {
                            Datum::Null => assert!(row.is_null(0)),
                            Datum::Bytes(bytes) => assert_eq!(row.get_bytes(0), bytes.as_slice()),
                            _ => assert_eq!(row.get_datum(0, &field_type), value),
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn constant_batch_preserves_deferred_rows_and_side_effect_ordering() {
        // A deferred constant delegates to its expression in Go; its saved
        // value is not permission to broadcast the first input row.
        let mut deferred = Constant::new(Datum::Int(99), long());
        deferred.deferred_expr = Some(Box::new(input_column(0)));
        let suite = EvaluatorSuite::new(vec![Expression::Constant(deferred)], false);
        let mut input = Chunk::new_with_capacity(&[long()], 3);
        for value in [3, 7, 11] {
            input.append_int64(0, value);
        }
        let mut output = Chunk::new_with_capacity(&[long()], 3);
        suite.run(&NoColumns, &mut input, &mut output).unwrap();
        for (row, expected) in [3, 7, 11].into_iter().enumerate() {
            assert_eq!(output.get_row(row).get_int64(0), expected);
        }

        let suite = EvaluatorSuite::new(
            vec![
                parameter(long()),
                scalar("getvar_int", vec![string_const("v")]),
            ],
            false,
        );
        assert!(!suite.vectorizable());
        let ctx = CountedParameter {
            value: Ok(Datum::Int(8)),
            reads: Cell::new(0),
        };
        let mut output = Chunk::new_with_capacity(&[long(), long()], 3);
        suite.run(&ctx, &mut input, &mut output).unwrap();
        assert_eq!(ctx.reads.get(), 3);
        for row in 0..3 {
            assert_eq!(output.get_row(row).get_int64(0), 8);
            assert!(output.get_row(row).is_null(1));
        }
    }

    #[test]
    fn constant_batch_error_preserves_input_owners_and_skips_empty_input() {
        let suite = EvaluatorSuite::new(vec![input_column(0), parameter(long())], false);
        for rows in [0, 3] {
            let error = EvalError::Unsupported("unbound prepared parameter");
            let ctx = CountedParameter {
                value: Err(error.clone()),
                reads: Cell::new(0),
            };
            let mut input = Chunk::new_with_capacity(&[long()], rows);
            for _ in 0..rows {
                input.append_int64(0, 7);
            }
            let input_owner = input.column_handle(0);
            let mut output = Chunk::new_with_capacity(&[long(), long()], rows);
            let result = suite.run(&ctx, &mut input, &mut output);
            assert_eq!(ctx.reads.get(), usize::from(rows != 0));
            if rows == 0 {
                assert_eq!(result, Ok(()));
            } else {
                assert_eq!(result, Err(EvaluatorError::Eval(error)));
                assert!(input_owner.same_identity(&input.column_handle(0)));
                assert_eq!(input.num_rows(), rows);
            }
            assert_eq!(output.num_rows(), 0);
        }
    }

    #[derive(Default)]
    struct UserVariables(RefCell<HashMap<String, Datum>>);

    impl Columns for UserVariables {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn get_uservar(&self, name: &str) -> Option<Datum> {
            self.0.borrow().get(&name.to_ascii_lowercase()).cloned()
        }

        fn set_uservar(&self, name: &str, value: Datum) {
            self.0.borrow_mut().insert(name.to_ascii_lowercase(), value);
        }
    }

    #[test]
    fn user_variable_side_effects_follow_select_list_order_for_each_row() {
        let mut column = Column::new(1, string());
        column.index = 0;
        let setvar = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("setvar"),
            string(),
            vec![string_const("v"), Expression::Column(column)],
        ));
        let getvar = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("getvar_string"),
            string(),
            vec![string_const("v")],
        ));
        let suite = EvaluatorSuite::new(vec![setvar, getvar], false);
        assert!(!suite.vectorizable());

        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&string()), 3);
        input.append_string(0, "a");
        input.append_string(0, "b");
        input.append_string(0, "c");
        let mut output = Chunk::new_with_capacity(&[string(), string()], 3);

        suite
            .run(&UserVariables::default(), &mut input, &mut output)
            .unwrap();

        assert_eq!(output.num_rows(), 3);
        for (row_index, expected) in [b"a", b"b", b"c"].into_iter().enumerate() {
            let row = output.get_row(row_index);
            assert_eq!(row.get_bytes(0), expected);
            assert_eq!(row.get_bytes(1), expected);
        }
    }

    #[test]
    fn vectorizable_matches_user_variable_and_sequence_ordering_rules() {
        let nested_getvar = scalar(
            "plus",
            vec![scalar("getvar_int", vec![string_const("v")]), int_const(1)],
        );
        assert!(has_get_set_var_func(&nested_getvar));
        assert!(!vectorizable(&[nested_getvar]));
        assert!(!vectorizable(&[scalar("getvar", vec![])]));

        assert!(vectorizable(&[scalar("nextval", vec![])]));
        assert!(vectorizable(&[
            scalar("lastval", vec![]),
            scalar("setval", vec![]),
        ]));
        assert!(!vectorizable(&[
            scalar("nextval", vec![]),
            scalar("lastval", vec![]),
        ]));
        assert!(!vectorizable(&[
            scalar("nextval", vec![]),
            scalar("setval", vec![]),
        ]));
        assert!(!vectorizable(&[
            scalar("nextval", vec![]),
            scalar("nextval", vec![]),
        ]));

        let nested_nextval = scalar("plus", vec![scalar("nextval", vec![]), int_const(1)]);
        assert!(vectorizable(&[nested_nextval]));
    }

    #[test]
    fn calculated_columns_finish_before_direct_owners_move() {
        let mut input = Chunk::new_with_capacity(&[long(), long()], 2);
        input.append_int64(0, 10);
        input.append_int64(1, 20);
        input.append_int64(0, 30);
        input.append_int64(1, 40);
        let original_input_owner = input.column_handle(0);

        let plus_one = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("plus"),
            long(),
            vec![input_column(1), int_const(1)],
        ));
        let suite = EvaluatorSuite::new(vec![input_column(0), plus_one, input_column(0)], false);
        let mut output = Chunk::new_with_capacity(&[long(), long(), long()], 2);

        suite.run(&NoColumns, &mut input, &mut output).unwrap();

        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.get_row(0).get_int64(0), 10);
        assert_eq!(output.get_row(0).get_int64(1), 21);
        assert_eq!(output.get_row(0).get_int64(2), 10);
        assert_eq!(output.get_row(1).get_int64(0), 30);
        assert_eq!(output.get_row(1).get_int64(1), 41);
        assert_eq!(output.get_row(1).get_int64(2), 30);
        assert!(output.columns_share_identity(0, &output, 2));
        assert!(original_input_owner.same_identity(&output.column_handle(0)));
        assert_eq!(input.num_rows(), 0);
    }

    #[test]
    fn expression_error_does_not_move_a_direct_column_owner() {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&long()), 1);
        input.append_int64(0, 7);
        let mut output = Chunk::new_with_capacity(&[long(), long()], 1);
        let input_before = input.column_handle(0);
        let output_before = output.column_handle(0);
        let unsupported = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("not_a_function"),
            long(),
            vec![],
        ));
        let suite = EvaluatorSuite::new(vec![input_column(0), unsupported], false);

        assert_eq!(
            suite.run(&NoColumns, &mut input, &mut output),
            Err(EvaluatorError::Eval(EvalError::Unsupported(
                "this scalar function is not yet ported"
            )))
        );

        assert!(input_before.same_identity(&input.column_handle(0)));
        assert!(output_before.same_identity(&output.column_handle(0)));
        assert!(!input_before.same_identity(&output.column_handle(0)));
        assert_eq!(input.get_row(0).get_int64(0), 7);
        assert_eq!(output.num_rows(), 0);
    }

    #[test]
    fn avoiding_column_evaluator_copies_without_transferring() {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&long()), 1);
        input.append_int64(0, 9);
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&long()), 1);
        let input_before = input.column_handle(0);
        let suite = EvaluatorSuite::new(vec![input_column(0)], true);

        suite.run(&NoColumns, &mut input, &mut output).unwrap();

        assert_eq!(output.get_row(0).get_int64(0), 9);
        assert!(input_before.same_identity(&input.column_handle(0)));
        assert!(!input_before.same_identity(&output.column_handle(0)));
    }
}
