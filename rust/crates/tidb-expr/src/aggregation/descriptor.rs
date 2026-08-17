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

//! Go `pkg/expression/aggregation/descriptor.go`: `AggFuncDesc`, the shared
//! planner/executor description of one aggregate call.
//!
//! See [`super`] for the symbol table, the executor-relationship note and
//! every narrowing.

use super::names;
use super::window_func::WindowFuncDesc;
use super::{AggDescError, AggFunctionMode, BaseFuncDesc};
use crate::column::Column;
use crate::constant::Constant;
use crate::context::Columns;
use crate::expr_util::{evaluate_expr_with_null, RealFunctionBuilder, SubstituteOptions};
use crate::expression::Expression;
use crate::schema::Schema;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

/// Go `pkg/planner/util.ByItems`: one aggregate-local `ORDER BY` term.
///
/// Defined here rather than imported: `pkg/planner/util` sits ABOVE this
/// crate, so taking the type from there would be a dependency cycle. See
/// [`super`]'s narrowings.
#[derive(Clone, Debug)]
pub struct ByItems {
    /// Go `Expr`.
    pub expr: Expression,
    /// Go `Desc`: sort descending.
    pub desc: bool,
}

impl ByItems {
    /// Go `ByItems.Clone` is a deep copy; `#[derive(Clone)]` is the same
    /// thing here because [`Expression`] owns its subtree.
    #[must_use]
    pub fn new(expr: Expression, desc: bool) -> Self {
        ByItems { expr, desc }
    }

    /// Go `ByItems.Equal(ctx, other)` / `Equals(other)`, both of which reduce
    /// to the descending bit plus expression equality.
    #[must_use]
    pub fn equal(&self, other: &ByItems) -> bool {
        self.desc == other.desc && self.expr.equal(&other.expr)
    }
}

/// Go `AggFuncDesc` (`descriptor.go:33`): an aggregation function signature.
///
/// This is the CANONICAL descriptor for the workspace; see [`super`] for how
/// `tidb-executor`'s `AggFunc`/`AggKind` and `tidb-planner`'s generic
/// `AggFuncDesc<A, R, O>` relate to it.
#[derive(Clone, Debug)]
pub struct AggFuncDesc {
    /// Go's embedded `baseFuncDesc`.
    pub base: BaseFuncDesc,
    /// Go `Mode`.
    pub mode: AggFunctionMode,
    /// Go `HasDistinct`.
    pub has_distinct: bool,
    /// Go `OrderByItems`: the `ORDER BY` inside `GROUP_CONCAT`.
    pub order_by_items: Vec<ByItems>,
    /// Go `GroupingID`, which distinguishes from the not-set `0` and so
    /// starts at `1`.
    pub grouping_id: i32,
}

impl AggFuncDesc {
    /// Go `NewAggFuncDesc` (`descriptor.go:47`).
    ///
    /// As Go's own comment says, this CANNOT be called twice on the same
    /// arguments: `TypeInfer` rewrites them the first time.
    pub fn new(
        ctx: &impl Columns,
        name: &str,
        args: Vec<Expression>,
        has_distinct: bool,
    ) -> Result<Self, AggDescError> {
        Ok(AggFuncDesc {
            base: BaseFuncDesc::new(ctx, name, args)?,
            mode: AggFunctionMode::Complete,
            has_distinct,
            order_by_items: Vec::new(),
            grouping_id: 0,
        })
    }

    /// Go `NewAggFuncDescForWindowFunc` (`descriptor.go:56`): reuse the
    /// window descriptor's already-inferred signature.
    ///
    /// Go's `desc.RetTp == nil` safety check is UNREACHABLE here: [`super`]'s
    /// narrowing makes `ret_type` a value, and a [`WindowFuncDesc`] can only
    /// be built through inference. The re-inference fallback is therefore not
    /// reproduced.
    #[must_use]
    pub fn new_for_window_func(desc: &WindowFuncDesc, has_distinct: bool) -> Self {
        AggFuncDesc {
            base: desc.base.clone(),
            mode: AggFunctionMode::Complete,
            has_distinct,
            order_by_items: Vec::new(),
            grouping_id: 0,
        }
    }

    /// Go's promoted `baseFuncDesc.Name`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.base.name
    }

    /// Go's promoted `baseFuncDesc.Args`.
    #[must_use]
    pub fn args(&self) -> &[Expression] {
        &self.base.args
    }

    /// Go's promoted `baseFuncDesc.RetTp`.
    #[must_use]
    pub fn ret_type(&self) -> &FieldType {
        &self.base.ret_type
    }

    /// Go `AggFuncDesc.Equals` (`descriptor.go:75`): the structural
    /// `base.Equals` implementation, which is what plan identity uses.
    #[must_use]
    pub fn equals(&self, other: &AggFuncDesc) -> bool {
        if self.mode != other.mode
            || self.has_distinct != other.has_distinct
            || self.order_by_items.len() != other.order_by_items.len()
        {
            return false;
        }
        if !self
            .order_by_items
            .iter()
            .zip(other.order_by_items.iter())
            .all(|(a, b)| a.equal(b))
        {
            return false;
        }
        self.base.equals(&other.base)
    }

    /// Go `AggFuncDesc.Equal` (`descriptor.go:122`).
    ///
    /// Inherits [`Expression::equal`]'s context-free narrowing; see [`super`].
    #[must_use]
    pub fn equal(&self, other: &AggFuncDesc) -> bool {
        if self.has_distinct != other.has_distinct
            || self.order_by_items.len() != other.order_by_items.len()
        {
            return false;
        }
        if !self
            .order_by_items
            .iter()
            .zip(other.order_by_items.iter())
            .all(|(a, b)| a.equal(b))
        {
            return false;
        }
        self.base.equal(&other.base)
    }

    /// Go `AggFuncDesc.Split` (`descriptor.go:152`): split into the PARTIAL
    /// and FINAL descriptors the parallel aggregate executor runs.
    ///
    /// `ordinal` gives the column index (or, for `avg`, the two indices) of
    /// the intermediate result in the partial output row.
    #[must_use]
    pub fn split(&self, ordinal: &[usize]) -> (AggFuncDesc, AggFuncDesc) {
        let mut partial = self.clone();
        match self.mode {
            AggFunctionMode::Complete => partial.mode = AggFunctionMode::Partial1,
            AggFunctionMode::Final => partial.mode = AggFunctionMode::Partial2,
            _ => {}
        }

        let mut final_desc = AggFuncDesc {
            base: BaseFuncDesc::from_parts(
                self.base.name.clone(),
                Vec::new(),
                self.base.ret_type.clone(),
            ),
            // Only FinalMode is supported in the final phase today.
            mode: AggFunctionMode::Final,
            has_distinct: self.has_distinct,
            order_by_items: Vec::new(),
            grouping_id: 0,
        };

        let intermediate = |index: usize, ret_type: FieldType| {
            let mut column = Column::new(0, ret_type);
            column.index = index as i64;
            Expression::Column(column)
        };

        match self.base.name.as_str() {
            names::AVG => {
                final_desc.base.args = vec![
                    intermediate(ordinal[0], FieldType::new(FieldTypeCode::LongLong)),
                    intermediate(ordinal[1], self.base.ret_type.clone()),
                ];
            }
            names::APPROX_COUNT_DISTINCT => {
                final_desc.base.args = vec![intermediate(
                    ordinal[0],
                    FieldType::new(FieldTypeCode::String),
                )];
            }
            names::COUNT => {
                final_desc.base.args = if self.has_distinct {
                    // Go's own comment: a hack. The real input type of the
                    // final agg is the partial agg's return type, but reusing
                    // `a.Args` is what selects the correct final agg func.
                    self.base.args.clone()
                } else {
                    vec![intermediate(ordinal[0], self.base.ret_type.clone())]
                };
            }
            _ => {
                let mut args = vec![intermediate(ordinal[0], self.base.ret_type.clone())];
                if matches!(
                    final_desc.base.name.as_str(),
                    names::GROUP_CONCAT | names::APPROX_PERCENTILE
                ) {
                    if let Some(separator) = self.base.args.last() {
                        args.push(separator.clone());
                    }
                }
                final_desc.base.args = args;
            }
        }
        (partial, final_desc)
    }

    /// Go `AggFuncDesc.EvalNullValueInOuterJoin` (`descriptor.go:240`): the
    /// value this aggregate produces when the outer join found no inner row,
    /// so every inner-side input is `NULL`.
    ///
    /// `false` in the second slot is Go's "this function cannot produce a
    /// null value" answer. `schema` is the schema of the aggregation's CHILD.
    ///
    /// Go PANICS for a name outside the switch (`descriptor.go:255`); that
    /// arm becomes [`AggDescError::UnsupportedAggFunction`].
    pub fn eval_null_value_in_outer_join(
        &self,
        ctx: &impl Columns,
        schema: &Schema,
    ) -> Result<(Datum, bool), AggDescError> {
        match self.base.name.as_str() {
            names::COUNT => {
                for arg in &self.base.args {
                    let (value, ok) = self.const_of_null_input(ctx, schema, arg)?;
                    if !ok || value.is_null() {
                        return Ok((Datum::Null, ok));
                    }
                }
                Ok((Datum::new_int(1), true))
            }
            names::SUM | names::SUM_INT | names::MAX | names::MIN | names::FIRST_ROW => {
                let arg = self.first_arg()?;
                let (value, ok) = self.const_of_null_input(ctx, schema, arg)?;
                if !ok || value.is_null() {
                    return Ok((Datum::Null, ok));
                }
                Ok((value, true))
            }
            names::AVG | names::GROUP_CONCAT => Ok((Datum::Null, false)),
            names::BIT_AND => {
                let arg = self.first_arg()?;
                let (value, ok) = self.const_of_null_input(ctx, schema, arg)?;
                if !ok || value.is_null() {
                    return Ok((Datum::new_uint(u64::MAX), true));
                }
                Ok((value, true))
            }
            names::BIT_OR | names::BIT_XOR => {
                let arg = self.first_arg()?;
                let (value, ok) = self.const_of_null_input(ctx, schema, arg)?;
                if !ok || value.is_null() {
                    return Ok((Datum::new_int(0), true));
                }
                Ok((value, true))
            }
            other => Err(AggDescError::UnsupportedAggFunction(other.to_owned())),
        }
    }

    fn first_arg(&self) -> Result<&Expression, AggDescError> {
        self.base.args.first().ok_or(AggDescError::MissingArgument(
            "eval_null_value_in_outer_join",
        ))
    }

    /// Go's repeated `EvaluateExprWithNull(ctx, schema, arg, true)` plus the
    /// `*Constant` type assertion. The bool is Go's `ok`.
    fn const_of_null_input(
        &self,
        ctx: &impl Columns,
        schema: &Schema,
        arg: &Expression,
    ) -> Result<(Datum, bool), AggDescError> {
        let builder = RealFunctionBuilder::new(ctx);
        let options = SubstituteOptions::new(&builder);
        let result = evaluate_expr_with_null(arg, schema, true, ctx, &options)
            .map_err(|_| AggDescError::MissingArgument("evaluate_expr_with_null"))?;
        match result {
            Expression::Constant(Constant { value, .. }) => Ok((value, true)),
            _ => Ok((Datum::Null, false)),
        }
    }

    /// Go `UpdateNotNullFlag4RetType` (`descriptor.go:337`): drop the
    /// `NOT NULL` flag from the return type when this aggregate CAN produce
    /// `NULL` in the query shape it was built for.
    ///
    /// `has_group_by` is whether the aggregation has a `GROUP BY`;
    /// `all_aggs_first_row` is [`super::is_all_first_row`] over its sibling
    /// aggregates.
    pub fn update_not_null_flag_4_ret_type(
        &mut self,
        has_group_by: bool,
        all_aggs_first_row: bool,
    ) -> Result<(), AggDescError> {
        let remove_not_null = match self.base.name.as_str() {
            names::COUNT
            | names::APPROX_COUNT_DISTINCT
            | names::APPROX_PERCENTILE
            | names::BIT_AND
            | names::BIT_OR
            | names::BIT_XOR
            | names::FIRST_VALUE
            | names::LAST_VALUE
            | names::NTH_VALUE
            | names::ROW_NUMBER
            | names::RANK
            | names::DENSE_RANK
            | names::CUME_DIST
            | names::NTILE
            | names::PERCENT_RANK
            | names::LEAD
            | names::LAG
            | names::JSON_OBJECTAGG
            | names::JSON_ARRAYAGG
            | names::VAR_SAMP
            | names::VAR_POP
            | names::STDDEV_POP
            | names::STDDEV_SAMP => false,
            names::SUM | names::SUM_INT | names::AVG | names::GROUP_CONCAT => !has_group_by,
            // `select max(a) from empty_tbl` is NULL, while
            // `select max(a) from empty_tbl group by b` is empty.
            names::MAX | names::MIN => {
                !has_group_by && self.base.ret_type.code() != FieldTypeCode::Bit
            }
            // `select a, max(a) from empty_tbl` is `(null, null)`, while
            // `select a, max(a) from empty_tbl group by b` is empty.
            names::FIRST_ROW => !all_aggs_first_row && !has_group_by,
            other => return Err(AggDescError::UnsupportedAggFunction(other.to_owned())),
        };
        if remove_not_null {
            // Go clones before mutating so a shared `RetTp` pointer is not
            // touched; the Rust field is owned, so the clone is implicit.
            self.base.ret_type.del_flags(FieldTypeFlags::NOT_NULL);
        }
        Ok(())
    }
}
