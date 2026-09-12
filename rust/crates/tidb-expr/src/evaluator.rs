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
use tidb_datatype::Datum;

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

/// Go `expression.VecEvalBool`/`VectorizedFilterConsiderNull`.
///
/// The returned mask is indexed by the physical rows of `input`, just like
/// Go's `selected` slice. The caller-supplied `selected` and `nulls` vectors
/// are output buffers (their previous contents are discarded); the input
/// chunk's selection vector is the only pre-existing selection. `nulls`
/// records which surviving filter evaluations were SQL NULL, while NULL
/// itself never remains selected.
///
/// The expression model exposes Go's typed `VecEval*` only for the numeric
/// comparisons (`ScalarFunction::vec_eval_numeric_compare`). The vector evaluator
/// nevertheless preserves the important vectorized contract: filters run
/// filter-major, rejected rows are removed before the next filter, and direct
/// column/constant expressions are materialized column-wise. Any other
/// scalar-function node uses its row evaluator as the explicit fallback,
/// without evaluating rows that an earlier filter already rejected.
pub fn vectorized_filter_consider_null<C: Columns>(
    ctx: &C,
    vec_enabled: bool,
    filters: &[Expression],
    input: &Chunk,
    mut selected: Vec<bool>,
    mut nulls: Vec<bool>,
) -> Result<(Vec<bool>, Vec<bool>), EvalError> {
    // `Chunk::num_rows` is selection-aware in Rust. Go's VecEvalBool instead
    // clears the input selection while evaluating and returns a mask sized to
    // all physical rows, then reapplies the original selection. Derive that
    // physical width without mutating the caller's chunk.
    let original_sel = input.sel().map(ToOwned::to_owned);
    let physical_rows = if original_sel.is_some() {
        let mut unselected = input.clone();
        unselected.set_sel(None);
        unselected.num_rows()
    } else {
        input.num_rows()
    };
    selected.clear();
    selected.resize(physical_rows, true);
    nulls.clear();
    nulls.resize(physical_rows, false);
    if let Some(sel) = &original_sel {
        let mut in_selection = vec![false; physical_rows];
        for &physical in sel {
            if physical < physical_rows {
                in_selection[physical] = true;
            }
        }
        for (physical, selected) in selected.iter_mut().enumerate() {
            *selected = in_selection[physical];
        }
    }
    if filters.is_empty() {
        return Ok((selected, nulls));
    }

    // Go falls back to rowBasedFilter when vectorization is disabled or any
    // filter is not vectorizable. Keep the same filter-major order and
    // three-valued truth handling in that branch.
    if !vec_enabled || !vectorizable(filters) {
        let mut unselected = input.clone();
        unselected.set_sel(None);
        for filter in filters {
            for row_index in 0..physical_rows {
                if !selected[row_index] {
                    continue;
                }
                let value = filter.eval(ctx, unselected.get_row(row_index))?;
                let truth = crate::truthy_of(&value)?;
                if truth.is_none() {
                    nulls[row_index] = true;
                }
                selected[row_index] = truth == Some(true);
            }
        }
        return Ok((selected, nulls));
    }

    // Go `VecEvalBool`: `sel` is the live physical row set. Each filter runs
    // over it and removes the rows it rejects, so a later filter never
    // evaluates a row an earlier one dropped.
    let mut sel: Vec<usize> = (0..physical_rows)
        .filter(|&physical| selected[physical])
        .collect();
    // `Chunk::get_row` maps through the input selection; the row evaluator
    // is handed the logical index of each live physical row.
    let logical_of_physical: Option<Vec<usize>> = original_sel.as_ref().map(|original| {
        let mut logical = vec![usize::MAX; physical_rows];
        for (index, &physical) in original.iter().enumerate() {
            if physical < physical_rows {
                logical[physical] = index;
            }
        }
        logical
    });
    // Go's `isZero`: -1 NULL, 0 false, 1 true, one entry per row of `sel`.
    let mut is_zero: Vec<i8> = Vec::new();
    let truth_code = |value: &Datum| -> Result<i8, EvalError> {
        Ok(match crate::truthy_of(value)? {
            None => -1,
            Some(false) => 0,
            Some(true) => 1,
        })
    };
    for filter in filters {
        if sel.is_empty() {
            break;
        }
        let column_wise = match filter {
            // Go `Constant.VecEval*` reads parameters once per nonempty
            // batch; a deferred expression keeps row evaluation.
            Expression::Constant(constant) if constant.deferred_expr.is_none() => {
                let code = truth_code(&constant.eval_in(ctx)?)?;
                is_zero.clear();
                is_zero.resize(sel.len(), code);
                true
            }
            Expression::CorrelatedColumn(column) => {
                let code = truth_code(&column.eval())?;
                is_zero.clear();
                is_zero.resize(sel.len(), code);
                true
            }
            Expression::ScalarFunction(function) => {
                function.vec_eval_numeric_compare(input, &sel, &mut is_zero)?
            }
            Expression::Column(_) | Expression::Constant(_) => false,
        };
        if !column_wise {
            // The scalar evaluator is the documented fallback for every
            // other shape, over the live rows only.
            is_zero.clear();
            for &physical in &sel {
                let logical = logical_of_physical
                    .as_ref()
                    .map_or(physical, |map| map[physical]);
                is_zero.push(truth_code(&filter.eval(ctx, input.get_row(logical))?)?);
            }
        }
        let mut kept = 0;
        for index in 0..sel.len() {
            let physical = sel[index];
            match is_zero[index] {
                -1 => nulls[physical] = true,
                0 => {}
                _ => {
                    sel[kept] = physical;
                    kept += 1;
                }
            }
        }
        sel.truncate(kept);
    }
    selected.fill(false);
    for &physical in &sel {
        selected[physical] = true;
    }
    Ok((selected, nulls))
}

/// Convenience form matching Go `VectorizedFilter` when the caller does not
/// need the per-row NULL mask.
pub fn vectorized_filter<C: Columns>(
    ctx: &C,
    vec_enabled: bool,
    filters: &[Expression],
    input: &Chunk,
    selected: Vec<bool>,
) -> Result<Vec<bool>, EvalError> {
    vectorized_filter_consider_null(ctx, vec_enabled, filters, input, selected, Vec::new())
        .map(|(selected, _)| selected)
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
        let program = &self.program;
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

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, Decimal, FieldType, FieldTypeCode};

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

    fn decimal_column(index: i64, field_type: &FieldType) -> Expression {
        let mut column = Column::new(index + 1, field_type.clone());
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
    fn vector_filter_reads_current_parameter_once_per_nonempty_chunk() {
        let filters = vec![parameter(long())];
        for value in [Datum::Null, Datum::Int(0), Datum::Int(1), Datum::Int(-1)] {
            for rows in [0, 1, 8] {
                let ctx = CountedParameter {
                    value: Ok(value.clone()),
                    reads: Cell::new(0),
                };
                let mut input = Chunk::new(&[], rows, rows.max(1));
                input.set_num_virtual_rows(rows);
                let (selected, nulls) = vectorized_filter_consider_null(
                    &ctx,
                    true,
                    &filters,
                    &input,
                    Vec::new(),
                    Vec::new(),
                )
                .unwrap();
                assert_eq!(
                    selected,
                    vec![crate::truthy_of(&value).unwrap() == Some(true); rows]
                );
                assert_eq!(nulls, vec![value.is_null(); rows]);
                assert_eq!(ctx.reads.get(), usize::from(rows > 0));
            }
        }
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
    fn vectorized_filter_preserves_selection_and_null_mask() {
        let mut input = Chunk::new_with_capacity(std::slice::from_ref(&long()), 5);
        for value in [0, 1, 2, 3] {
            input.append_int64(0, value);
        }
        input.append_null(0);
        // Go's VectorizedFilter returns a physical-row mask while the input
        // selection points at only the rows to evaluate. The NULL physical
        // row is deliberately in the middle of this selection.
        input.set_sel(Some(vec![3, 2, 1]));
        let filter = scalar("gt", vec![input_column(0), int_const(1)]);
        let (selected, nulls) = vectorized_filter_consider_null(
            &NoColumns,
            true,
            &[filter],
            &input,
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(selected, vec![false, false, true, true, false]);
        assert_eq!(nulls, vec![false, false, false, false, false]);

        input.set_sel(Some(vec![3, 4, 1]));
        let filter = scalar("gt", vec![input_column(0), int_const(1)]);
        let (selected, nulls) = vectorized_filter_consider_null(
            &NoColumns,
            true,
            &[filter],
            &input,
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(selected, vec![false, false, false, true, false]);
        assert_eq!(nulls, vec![false, false, false, false, true]);
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

    #[test]
    fn decimal_revenue_expression_uses_the_general_expression_evaluator() {
        let mut decimal = FieldType::new(FieldTypeCode::NewDecimal);
        decimal.set_flen(15);
        decimal.set_decimal(2);
        let one = Expression::Constant(Constant::new(Datum::Int(1), long()));
        let discount = decimal_column(1, &decimal);
        let discounted_fraction = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("minus"),
            decimal.clone(),
            vec![one, discount],
        ));
        let revenue = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("mul"),
            decimal.clone(),
            vec![decimal_column(0, &decimal), discounted_fraction],
        ));
        let suite = EvaluatorSuite::new(vec![revenue], false);

        let mut input = Chunk::new_with_capacity(&[decimal.clone(), decimal.clone()], 3);
        for (price, discount) in [("100.00", "0.10"), ("12.50", "0.20")] {
            input.append_datum(0, &Datum::Decimal(Decimal::from_literal(price)));
            input.append_datum(1, &Datum::Decimal(Decimal::from_literal(discount)));
        }
        input.append_null(0);
        input.append_datum(1, &Datum::Decimal(Decimal::from_literal("0.15")));
        let mut output = Chunk::new_with_capacity(std::slice::from_ref(&decimal), 3);

        suite.run(&NoColumns, &mut input, &mut output).unwrap();

        assert_eq!(
            output.get_row(0).get_datum(0, &decimal),
            Datum::Decimal(Decimal::from_literal("90.00"))
        );
        assert_eq!(
            output.get_row(1).get_datum(0, &decimal),
            Datum::Decimal(Decimal::from_literal("10.00"))
        );
        assert!(output.get_row(2).is_null(0));
    }

    fn unsigned_long() -> FieldType {
        let mut field_type = long();
        field_type.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
        field_type
    }

    fn typed_input_column(index: i64, field_type: FieldType) -> Expression {
        let mut column = Column::new(index + 1, field_type);
        column.index = index;
        Expression::Column(column)
    }

    /// Go `builtinGTIntSig.vecEvalInt`: the integer comparison runs over the
    /// column cells, NULL when either side is NULL, and reads each side's
    /// signedness from its argument type (`VecCompareUI`: an unsigned value
    /// above `MaxInt64` is greater than any signed one).
    #[test]
    fn vector_filter_compares_integer_columns_column_wise() {
        let mut input = Chunk::new_with_capacity(&[long(), unsigned_long()], 4);
        for value in [1, 5, 7] {
            input.append_int64(0, value);
            input.append_uint64(1, u64::MAX);
        }
        input.append_null(0);
        input.append_null(1);
        let ctx = NoColumns;

        let filters = vec![scalar("gt", vec![input_column(0), int_const(4)])];
        let (selected, nulls) =
            vectorized_filter_consider_null(&ctx, true, &filters, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![false, true, true, false]);
        assert_eq!(nulls, vec![false, false, false, true]);

        // The same bits read through a signed type are -1, through an
        // unsigned type 18446744073709551615.
        let unsigned = vec![scalar("gt", vec![typed_input_column(1, unsigned_long()), int_const(-1)])];
        let (selected, _) =
            vectorized_filter_consider_null(&ctx, true, &unsigned, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![true, true, true, false]);
        let signed = vec![scalar("gt", vec![typed_input_column(1, long()), int_const(-1)])];
        let (selected, _) =
            vectorized_filter_consider_null(&ctx, true, &signed, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![false, false, false, false]);

        // A second filter sees only the rows the first kept, and an input
        // selection is honored.
        let both = vec![
            scalar("gt", vec![input_column(0), int_const(4)]),
            scalar("lt", vec![input_column(0), int_const(7)]),
        ];
        input.set_sel(Some(vec![0, 2, 3]));
        let (selected, _) =
            vectorized_filter_consider_null(&ctx, true, &both, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![false, false, false, false]);
        input.set_sel(Some(vec![1, 3]));
        let (selected, _) =
            vectorized_filter_consider_null(&ctx, true, &both, &input, Vec::new(), Vec::new())
                .unwrap();
        assert_eq!(selected, vec![false, true, false, false]);
    }

    /// Go `builtinGTDecimalSig.vecEvalInt`: a decimal column against an
    /// integer or decimal constant, and an integer column against a decimal
    /// constant, compare exactly in the decimal domain.
    #[test]
    fn vector_filter_compares_decimals_column_wise() {
        let mut decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
        decimal_type.set_flen(15);
        decimal_type.set_decimal(2);
        let mut input = Chunk::new_with_capacity(&[decimal_type.clone(), long()], 4);
        for (value, int_value) in [("313.99", 313), ("314.00", 314), ("314.01", 315)] {
            input.append_my_decimal(0, &Decimal::from_literal(value).to_my_decimal().unwrap());
            input.append_int64(1, int_value);
        }
        input.append_null(0);
        input.append_null(1);
        let ctx = NoColumns;
        let decimal_const = |text: &str| {
            Expression::Constant(Constant::new(
                Datum::Decimal(Decimal::from_literal(text)),
                decimal_type.clone(),
            ))
        };

        let cases: [(&str, Expression, Expression, [bool; 4]); 5] = [
            ("gt", decimal_column(0, &decimal_type), int_const(314), [false, false, true, false]),
            ("ge", decimal_column(0, &decimal_type), int_const(314), [false, true, true, false]),
            ("lt", decimal_column(0, &decimal_type), decimal_const("314.005"), [true, true, false, false]),
            ("eq", input_column(1), decimal_const("314.00"), [false, true, false, false]),
            ("ne", decimal_const("314.00"), decimal_column(0, &decimal_type), [true, false, true, false]),
        ];
        for (name, lhs, rhs, expected) in cases {
            let filters = vec![scalar(name, vec![lhs, rhs])];
            let (selected, nulls) =
                vectorized_filter_consider_null(&ctx, true, &filters, &input, Vec::new(), Vec::new())
                    .unwrap();
            assert_eq!(selected, expected, "{name}");
            assert_eq!(nulls, vec![false, false, false, true], "{name}");
        }
    }
}
