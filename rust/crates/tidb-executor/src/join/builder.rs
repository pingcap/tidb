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

//! Go hash/merge executor builders: consume already bound
//! keys and residuals. Do not rediscover join keys from a reconstructed ON tree.

use tidb_expr::Columns;
use tidb_planner::{
    find_best_task::LogicalJoinType,
    physical::{BasePhysicalJoin, PhysicalHashJoin, PhysicalMergeJoin},
};

use super::{JoinExec, JoinKind, JoinOutput};
use crate::{
    hash_join::{EquiKey, KeyClass},
    ExecError, Executor, ExecutorMeta, StatementMemory,
};

impl<C: Columns> JoinExec<C> {
    pub(crate) fn from_physical(
        meta: ExecutorMeta,
        plan: &PhysicalHashJoin,
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Result<Self, ExecError> {
        if plan.inner_child_idx > 1 {
            return Err(ExecError::internal(
                "hash join inner child index is not 0 or 1",
            ));
        }
        let inner_conditions = if plan.inner_child_idx == 1 {
            &plan.right_conditions
        } else {
            &plan.left_conditions
        };
        if !inner_conditions.is_empty() {
            return Err(ExecError::internal(
                "join's inner condition should be empty",
            ));
        }
        if !plan.na_equal_conditions.is_empty()
            || !plan.left_na_join_keys.is_empty()
            || !plan.right_na_join_keys.is_empty()
        {
            return Err(ExecError::unsupported("native null-aware hash join"));
        }
        if plan.join_type == LogicalJoinType::AntiSemi && !plan.other_conditions.is_empty() {
            return Err(ExecError::unsupported("native anti join nullable residual"));
        }
        let count = plan.left_join_keys.len();
        if plan.right_join_keys.len() != count
            || plan.equal_conditions.len() != count
            || plan.is_null_eq.len() != count
        {
            return Err(ExecError::internal(
                "hash join key arrays have different lengths",
            ));
        }
        let mut keys = Vec::with_capacity(count);
        for index in 0..count {
            let l = &plan.left_join_keys[index];
            let r = &plan.right_join_keys[index];
            let left_index = usize::try_from(l.index)
                .ok()
                .filter(|index| *index < left.ret_field_types().len())
                .ok_or_else(|| ExecError::internal("unresolved left hash key"))?;
            let right_index = usize::try_from(r.index)
                .ok()
                .filter(|index| *index < right.ret_field_types().len())
                .ok_or_else(|| ExecError::internal("unresolved right hash key"))?;
            let class = KeyClass::of(
                l.ret_type
                    .as_ref()
                    .ok_or_else(|| ExecError::internal("left hash key has no type"))?,
                r.ret_type
                    .as_ref()
                    .ok_or_else(|| ExecError::internal("right hash key has no type"))?,
                plan.equal_conditions[index].derived_collation(),
            )
            .ok_or_else(|| ExecError::unsupported("native hash join key comparison type"))?;
            keys.push(EquiKey {
                left: left_index,
                right: right_index,
                class,
                null_safe: plan.is_null_eq[index],
            });
        }
        let mut executor = Self::from_join(
            meta,
            &plan.join,
            plan.inner_child_idx,
            left,
            right,
            ctx,
            memory,
        )?;
        executor.keys = keys;
        executor.native_hash = true;
        executor.concurrency = plan.concurrency.max(1);
        executor.set_hash_build_is_left((plan.inner_child_idx == 0) != plan.use_outer_to_build);
        Ok(executor)
    }

    pub(crate) fn from_merge(
        meta: ExecutorMeta,
        plan: &PhysicalMergeJoin,
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Result<Self, ExecError> {
        let inner = usize::from(plan.join_type != LogicalJoinType::RightOuter);
        let inner_conditions = if inner == 0 {
            &plan.left_conditions
        } else {
            &plan.right_conditions
        };
        if !inner_conditions.is_empty() {
            return Err(ExecError::internal(
                "merge join's inner filter should be empty.",
            ));
        }
        if plan.is_null_eq.iter().any(|value| *value) {
            return Err(ExecError::unsupported("null-equal merge join"));
        }
        if plan.join_type == LogicalJoinType::AntiSemi && !plan.other_conditions.is_empty() {
            return Err(ExecError::unsupported("native anti join nullable residual"));
        }
        if plan.left_join_keys.len() != plan.right_join_keys.len() {
            return Err(ExecError::internal(
                "merge join key arrays have different lengths",
            ));
        }
        let keys = plan
            .left_join_keys
            .iter()
            .zip(&plan.right_join_keys)
            .map(|(l, r)| {
                Ok(crate::merge_join_plan::MergeJoinKey {
                    left: bound_key(l, left.ret_field_types().len())?,
                    right: bound_key(r, right.ret_field_types().len())?,
                })
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        let mut executor = Self::from_join(meta, &plan.join, inner, left, right, ctx, memory)?;
        executor.set_merge_plan(crate::merge_join_plan::MergeJoinPlan {
            keys,
            desc: plan.desc,
        });
        Ok(executor)
    }

    fn from_join(
        meta: ExecutorMeta,
        plan: &BasePhysicalJoin,
        inner: usize,
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        ctx: C,
        memory: StatementMemory,
    ) -> Result<Self, ExecError> {
        let kind = match plan.join_type {
            LogicalJoinType::Inner => JoinKind::Inner,
            LogicalJoinType::LeftOuter => JoinKind::Left,
            LogicalJoinType::RightOuter => JoinKind::Right,
            LogicalJoinType::Semi => JoinKind::Semi,
            LogicalJoinType::AntiSemi => JoinKind::AntiSemi,
            LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi => {
                return Err(ExecError::unsupported("native join nullable semi marker"));
            }
        };
        let mut output = JoinOutput::resolved(
            meta.schema(),
            kind,
            left.ret_field_types().len(),
            right.ret_field_types().len(),
        )?;
        output.set_default(inner == 0, &plan.default_values)?;
        let mut executor = Self::new(meta, kind, Vec::new(), left, right, ctx, memory);
        executor.output = output;
        executor.residual_conditions = plan.other_conditions.clone();
        executor.residual_decimal_mul_lt =
            super::residual_decimal_mul_lt(&executor.residual_conditions);
        executor.filter_is_left = inner == 1;
        executor.outer_filter = if inner == 1 {
            plan.left_conditions.clone()
        } else {
            plan.right_conditions.clone()
        };
        Ok(executor)
    }
}

fn bound_key(key: &tidb_expr::column::Column, width: usize) -> Result<usize, ExecError> {
    usize::try_from(key.index)
        .ok()
        .filter(|index| *index < width)
        .ok_or_else(|| ExecError::internal("unresolved merge join key"))
}
