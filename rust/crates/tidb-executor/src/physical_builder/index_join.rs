// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go executorBuilder.buildIndexLookUpJoin and dataReaderBuilder's batch ranges.
use super::*;
use crate::index_lookup_join::{IndexLookUpJoin, InnerCtx, LastColComparator, OuterCtx};
use crate::joiner::{new_joiner, JoinType, JoinerChunkSizes};
use crate::kv_table::IndexRange;
use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};
use tidb_expr::{
    column::Column,
    expression::{Expression, ScalarFunction},
};
use tidb_planner::physical::{ColWithCmpFuncManager, PhysicalIndexJoin};

fn offsets(columns: &[Column]) -> Result<Vec<usize>, ExecError> {
    columns
        .iter()
        .map(|column| {
            usize::try_from(column.index)
                .map_err(|_| ExecError::internal("unresolved IndexJoin key"))
        })
        .collect()
}

/// Execution-owned comparator functions: cached plans retain only expressions.
pub(super) struct CompareFilters {
    pub definition: ColWithCmpFuncManager,
    compare: Vec<(usize, tidb_chunk::compare::CompareFunc)>,
}
impl CompareFilters {
    pub fn new(definition: &ColWithCmpFuncManager) -> Result<Self, ExecError> {
        if definition.op_types.len() != definition.op_args.len() {
            return Err(ExecError::internal(
                "IndexJoin comparison argument count mismatch",
            ));
        }
        let compare = definition
            .affected_col_schema
            .columns
            .iter()
            .map(|column| {
                let field_type = column
                    .ret_type
                    .as_ref()
                    .ok_or_else(|| ExecError::internal("untyped comparison column"))?;
                let compare = tidb_chunk::compare::get_compare_func(field_type)
                    .ok_or_else(|| ExecError::unsupported("IndexJoin row comparison type"))?;
                Ok((
                    usize::try_from(column.index)
                        .map_err(|_| ExecError::internal("unresolved comparison column"))?,
                    compare,
                ))
            })
            .collect::<Result<_, ExecError>>()?;
        Ok(Self {
            definition: definition.clone(),
            compare,
        })
    }
    pub fn ranges(
        &self,
        context: &StmtContext,
        row: tidb_chunk::row::Row<'_>,
    ) -> Result<tidb_planner::ranger::types::Ranges, ExecError> {
        let definition = &self.definition;
        let field_type = definition
            .target_col
            .ret_type
            .as_ref()
            .ok_or_else(|| ExecError::internal("untyped range target"))?;
        let expressions = definition
            .op_types
            .iter()
            .zip(&definition.op_args)
            .map(|(op, arg)| {
                let value = arg.eval(context, row).map_err(ExecError::Eval)?;
                Ok(Expression::ScalarFunction(ScalarFunction::new(
                    tidb_ast::CiString::new(op),
                    FieldType::new(FieldTypeCode::Tiny),
                    vec![
                        Expression::Column(definition.target_col.clone()),
                        Expression::Constant(tidb_expr::constant::Constant::new(
                            value,
                            field_type.clone(),
                        )),
                    ],
                )))
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        tidb_planner::ranger::ranger::build_column_range_in(
            &expressions,
            field_type,
            definition.col_length,
            0,
            &|c| c.eval_in(context),
        )
        .map(|built| built.ranges)
        .map_err(|error| ExecError::internal(format!("IndexJoin last-column ranges: {error:?}")))
    }
}
impl LastColComparator for CompareFilters {
    fn compare_row(
        &self,
        left: tidb_chunk::row::Row<'_>,
        right: tidb_chunk::row::Row<'_>,
    ) -> std::cmp::Ordering {
        self.compare
            .iter()
            .map(|(index, compare)| compare(left, *index, right, *index))
            .find(|order| !order.is_eq())
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl<R: PhysicalReaderBuilder> PhysicalExecutorBuilder<'_, R> {
    pub(super) fn build_index_join(
        &mut self,
        plan: &PhysicalIndexJoin,
        meta: ExecutorMeta,
    ) -> Result<Box<dyn Executor>, ExecError> {
        use tidb_planner::find_best_task::LogicalJoinType as J;
        let join = &plan.join;
        let [left, right] = join.base.children() else {
            return Err(ExecError::internal("IndexJoin requires two children"));
        };
        if join.inner_child_idx > 1
            || plan.outer_join_keys.len() != plan.inner_join_keys.len()
            || plan.outer_join_keys.len() != plan.key_off_to_idx_off.len()
            || plan.outer_hash_keys.len() != plan.inner_hash_keys.len()
        {
            return Err(ExecError::internal("IndexJoin key shape mismatch"));
        }
        let (outer_plan, inner_plan, outer_filter, inner_filter) = if join.inner_child_idx == 0 {
            (right, left, &join.right_conditions, &join.left_conditions)
        } else {
            (left, right, &join.left_conditions, &join.right_conditions)
        };
        if !inner_filter.is_empty() {
            return Err(ExecError::internal(
                "join's inner condition should be empty",
            ));
        }
        let ranges = plan.ranges_for_execution(self.context).map_err(|error| {
            ExecError::internal(format!("cannot reuse IndexJoin ranges: {error}"))
        })?;
        let outer_exec = self.build(outer_plan)?;
        let mut outer_types = outer_exec.ret_field_types().to_vec();
        let mut inner_types = inner_plan
            .schema()
            .ok_or_else(|| ExecError::internal("IndexJoin inner has no schema"))?
            .columns
            .iter()
            .map(|column| {
                column
                    .ret_type
                    .clone()
                    .ok_or_else(|| ExecError::internal("untyped inner column"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for field_type in &mut inner_types {
            if field_type.eval_type() == EvalType::String {
                field_type.set_flen(-1);
            }
        }
        let outer_hash_cols = offsets(&plan.outer_hash_keys)?;
        let inner_hash_cols = offsets(&plan.inner_hash_keys)?;
        for ((outer, inner), key) in outer_hash_cols
            .iter()
            .zip(&inner_hash_cols)
            .zip(&plan.outer_hash_keys)
        {
            let inner = inner_types
                .get(*inner)
                .ok_or_else(|| ExecError::internal("IndexJoin inner hash offset"))?;
            let outer = outer_types
                .get_mut(*outer)
                .ok_or_else(|| ExecError::internal("IndexJoin outer hash offset"))?;
            outer.set_collation_name(inner.collation_name());
            outer.set_flags(
                key.ret_type
                    .as_ref()
                    .ok_or_else(|| ExecError::internal("untyped outer hash key"))?
                    .flags(),
            );
        }
        let hash_types =
            |keys: &[Column], types: &[FieldType]| -> Result<Vec<FieldType>, ExecError> {
                keys.iter()
                    .map(|key| {
                        let mut field_type = types
                            .get(
                                usize::try_from(key.index)
                                    .map_err(|_| ExecError::internal("unresolved hash key"))?,
                            )
                            .cloned()
                            .ok_or_else(|| ExecError::internal("IndexJoin hash offset"))?;
                        field_type.set_flags(
                            key.ret_type
                                .as_ref()
                                .ok_or_else(|| ExecError::internal("untyped hash key"))?
                                .flags(),
                        );
                        Ok(field_type)
                    })
                    .collect()
            };
        let collations = |keys: &[Column]| -> Result<Vec<_>, ExecError> {
            keys.iter()
                .map(|key| {
                    key.ret_type
                        .as_ref()
                        .map(FieldType::collation)
                        .ok_or_else(|| ExecError::internal("untyped join key"))
                })
                .collect()
        };
        let outer_ctx = OuterCtx {
            key_cols: offsets(&plan.outer_join_keys)?,
            hash_cols: outer_hash_cols,
            hash_types: hash_types(&plan.outer_hash_keys, &outer_types)?,
            row_types: outer_types.clone(),
            filter: outer_filter.clone(),
        };
        let mut hash_is_null_eq = vec![false; plan.inner_hash_keys.len()];
        for (value, null_eq) in hash_is_null_eq.iter_mut().zip(&join.is_null_eq) {
            *value = *null_eq;
        }
        let inner_ctx = InnerCtx {
            key_cols: offsets(&plan.inner_join_keys)?,
            key_col_ids: plan
                .inner_join_keys
                .iter()
                .map(|column| column.id)
                .collect(),
            hash_cols: inner_hash_cols,
            hash_types: hash_types(&plan.inner_hash_keys, &inner_types)?,
            row_types: inner_types.clone(),
            key_collators: collations(&plan.inner_join_keys)?,
            hash_collators: collations(&plan.inner_hash_keys)?,
            hash_is_null_eq,
            col_lens: plan.idx_col_lens.clone(),
            has_prefix_col: plan.idx_col_lens.iter().any(|length| *length != -1),
        };
        let (left_types, right_types) = if join.inner_child_idx == 0 {
            (&inner_types, &outer_types)
        } else {
            (&outer_types, &inner_types)
        };
        let join_type = match join.join_type {
            J::Inner => JoinType::Inner,
            J::LeftOuter => JoinType::LeftOuter,
            J::RightOuter => JoinType::RightOuter,
            J::Semi => JoinType::SemiJoin,
            J::AntiSemi => JoinType::AntiSemiJoin,
            J::LeftOuterSemi => JoinType::LeftOuterSemiJoin,
            J::AntiLeftOuterSemi => JoinType::AntiLeftOuterSemiJoin,
        };
        let indicator = matches!(join.join_type, J::LeftOuterSemi | J::AntiLeftOuterSemi);
        let output_count = meta
            .schema()
            .len()
            .checked_sub(usize::from(indicator))
            .ok_or_else(|| ExecError::internal("IndexJoin has no marker column"))?;
        let mut used = (Vec::new(), Vec::new());
        for column in &meta.schema().columns[..output_count] {
            let index = usize::try_from(column.index)
                .map_err(|_| ExecError::internal("unresolved IndexJoin output"))?;
            if index < left_types.len() {
                used.0.push(index);
            } else if index < left_types.len() + right_types.len() {
                used.1.push(index - left_types.len());
            } else {
                return Err(ExecError::internal("IndexJoin output is outside children"));
            }
        }
        let defaults = if join.default_values.is_empty() {
            vec![Datum::Null; inner_types.len()]
        } else {
            join.default_values.clone()
        };
        let joiner = new_joiner(
            self.context.clone(),
            join_type,
            join.inner_child_idx == 0,
            &defaults,
            join.other_conditions.clone(),
            left_types,
            right_types,
            Some(used),
            false,
            JoinerChunkSizes {
                init_chunk_size: self.init_cap,
                max_chunk_size: self.max_chunk_size,
            },
        );
        let reader = self.readers.build_index_join_reader(
            plan,
            &ranges,
            self.context,
            self.init_cap,
            self.max_chunk_size,
        )?;
        let ranges = ranges
            .iter()
            .map(|range| IndexRange {
                low: range.low_val.clone(),
                high: range.high_val.clone(),
                low_exclusive: range.low_exclude,
                high_exclusive: range.high_exclude,
            })
            .collect();
        let mut executor = IndexLookUpJoin::new(
            meta,
            outer_exec,
            outer_ctx,
            inner_ctx,
            reader,
            joiner,
            matches!(join.join_type, J::LeftOuter | J::RightOuter),
            ranges,
            plan.key_off_to_idx_off.clone(),
            self.context.clone(),
        )
        .with_max_batch_size(
            self.context
                .optimizer_cost_env()
                .session
                .index_join_batch_size
                .max(1.0) as usize,
        );
        if let Some(filters) = &plan.compare_filters {
            executor = executor.with_last_col_comparator(Box::new(CompareFilters::new(filters)?));
        }
        Ok(Box::new(executor))
    }
}
