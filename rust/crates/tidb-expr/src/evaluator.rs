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

/// Go `EvaluatorSuite`: one projection expression list partitioned into
/// calculated expressions and direct input-column transfers.
///
/// Calculated expressions always finish first. The column helper moves input
/// owners only after they all succeed, so an evaluation error cannot leave the
/// input chunk half-consumed.
pub struct EvaluatorSuite {
    calculated_output_indexes: Vec<usize>,
    calculated: Vec<Expression>,
    vectorizable: bool,
    column_swap_helper: Option<ColumnSwapHelper>,
}

impl EvaluatorSuite {
    /// Go `NewEvaluatorSuite`.
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
        let column_swap_helper =
            (!column_mapping.is_empty()).then(|| ColumnSwapHelper::from_mapping(column_mapping));
        EvaluatorSuite {
            calculated_output_indexes,
            calculated,
            vectorizable,
            column_swap_helper,
        }
    }

    /// Go `EvaluatorSuite.Vectorizable`.
    #[must_use]
    pub fn vectorizable(&self) -> bool {
        self.vectorizable
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
        if self.vectorizable {
            for (output_index, expression) in
                self.calculated_output_indexes.iter().zip(&self.calculated)
            {
                for row_index in 0..rows {
                    let value = expression.eval(ctx, input.get_row(row_index))?;
                    output.append_datum(*output_index, &value);
                }
            }
        } else {
            for row_index in 0..rows {
                for (output_index, expression) in
                    self.calculated_output_indexes.iter().zip(&self.calculated)
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
    use std::cell::RefCell;

    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    use crate::column::Column;
    use crate::constant::Constant;
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
