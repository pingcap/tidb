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

//! Go `pkg/executor/builder.go`: construct fresh executors from a resolved
//! physical tree. Storage readers belong to the caller's current transaction;
//! neither the tree nor this builder retains an executor from another run.

use tidb_expr::aggregation::{is_all_first_row, AggFuncDesc, AggFunctionMode, ByItems};
use tidb_planner::physical::PhysicalPlan;

mod readers;
mod index_join;
pub use readers::CatalogReaderBuilder;
pub(crate) use readers::ReaderBindings;

use crate::{
    ExecError, Executor, ExecutorMeta, LimitExec, ProjectionExec, SelectionExec, SortByItem,
    SortExec, StmtContext, TableDualExec,
};

/// Transaction-owned construction of table, index, and coprocessor readers.
pub trait PhysicalReaderBuilder {
    /// Capture the current inner reader definition and transaction owners for
    /// the batch lookup worker. Implementations must not replan an AST.
    /// `ranges` has already been rebound for this execution; use it for range
    /// metadata instead of the physical plan's initial template values.
    fn build_index_join_reader(
        &mut self,
        _plan: &tidb_planner::physical::PhysicalIndexJoin,
        _ranges: &[tidb_planner::ranger::types::Range],
        _context: &StmtContext,
        _init_cap: usize,
        _max_chunk_size: usize,
    ) -> Result<Box<dyn crate::index_lookup_join::IndexJoinExecutorBuilder>, ExecError> {
        Err(ExecError::unsupported("native IndexJoin reader construction"))
    }

    /// Build a fresh reader for this physical access node and current context.
    /// Retain the access-expression bindings, not only their initial ranges:
    /// correlated readers rebuild ranges on Open after Apply binds the row.
    /// Reader-owned pushed filters likewise read those current bindings.
    fn build_reader(
        &mut self,
        plan: &PhysicalPlan,
        context: &StmtContext,
        meta: ExecutorMeta,
    ) -> Result<Box<dyn Executor>, ExecError>;
}

/// One invocation of Go's executor builder. The physical plan remains caller-owned.
pub struct PhysicalExecutorBuilder<'a, R> {
    context: &'a StmtContext,
    readers: &'a mut R,
    init_cap: usize,
    max_chunk_size: usize,
}

impl<'a, R: PhysicalReaderBuilder> PhysicalExecutorBuilder<'a, R> {
    /// Chunk sizes are the current session settings, not cached plan state.
    pub fn new(
        context: &'a StmtContext,
        readers: &'a mut R,
        init_cap: usize,
        max_chunk_size: usize,
    ) -> Self {
        Self {
            context,
            readers,
            init_cap,
            max_chunk_size,
        }
    }

    fn metadata(&self, plan: &PhysicalPlan, init_cap: usize) -> Result<ExecutorMeta, ExecError> {
        let schema = plan.schema().ok_or_else(|| {
            ExecError::internal(format!(
                "{} has no execution schema",
                plan.explain_id(false)
            ))
        })?;
        Ok(ExecutorMeta::new(
            schema.clone(),
            i64::from(plan.id()),
            init_cap,
            self.max_chunk_size,
        ))
    }

    fn child(&mut self, plan: &PhysicalPlan) -> Result<Box<dyn Executor>, ExecError> {
        let [child] = plan.children() else {
            return Err(ExecError::internal(format!(
                "{} requires one child",
                plan.explain_id(false)
            )));
        };
        self.build(child)
    }

    /// Build without opening or consuming the tree. Plans must already have
    /// passed `ResolveIndices`; errors propagate before execution starts.
    pub fn build(&mut self, plan: &PhysicalPlan) -> Result<Box<dyn Executor>, ExecError> {
        let cap = match plan {
            PhysicalPlan::Limit(limit) => limit.count.min(self.max_chunk_size as u64) as usize,
            PhysicalPlan::TableDual(dual) => dual.row_count,
            _ => self.init_cap,
        };
        let meta = self.metadata(plan, cap)?;
        Ok(match plan {
            PhysicalPlan::IndexJoin(join) => self.build_index_join(join, meta)?,
            PhysicalPlan::Apply(apply) => {
                use crate::joiner::{new_joiner, JoinType, JoinerChunkSizes};
                use tidb_expr::expression::Expression;
                use tidb_planner::find_best_task::LogicalJoinType;

                if apply.concurrency > 1 {
                    return Err(ExecError::unsupported(
                        "parallel physical Apply is not implemented",
                    ));
                }
                let [left_plan, right_plan] = plan.children() else {
                    return Err(ExecError::internal("Apply requires two children"));
                };
                let join = &apply.hash_join.join;
                let (inner_plan, outer_plan) = match join.inner_child_idx {
                    0 => (left_plan, right_plan),
                    1 => (right_plan, left_plan),
                    _ => return Err(ExecError::internal("invalid Apply inner child index")),
                };
                // Rebind the execution-local tree, never the retained plan definition.
                let mut inner_plan = inner_plan.clone();
                let outer_schema = inner_plan.bind_correlated_columns(
                    outer_plan
                        .schema()
                        .ok_or_else(|| ExecError::internal("Apply outer child has no schema"))?,
                );
                let (left, right) = if join.inner_child_idx == 0 {
                    (self.build(&inner_plan)?, self.build(right_plan)?)
                } else {
                    (self.build(left_plan)?, self.build(&inner_plan)?)
                };
                let conditions = apply
                    .hash_join
                    .equal_conditions
                    .iter()
                    .chain(&apply.hash_join.na_equal_conditions)
                    .cloned()
                    .map(Expression::ScalarFunction)
                    .chain(join.other_conditions.iter().cloned())
                    .collect();
                let defaults = if join.default_values.is_empty() {
                    vec![
                        tidb_datatype::Datum::Null;
                        if join.inner_child_idx == 0 {
                            left.ret_field_types().len()
                        } else {
                            right.ret_field_types().len()
                        }
                    ]
                } else {
                    join.default_values.clone()
                };
                let join_type = match join.join_type {
                    LogicalJoinType::Inner => JoinType::Inner,
                    LogicalJoinType::LeftOuter => JoinType::LeftOuter,
                    LogicalJoinType::RightOuter => JoinType::RightOuter,
                    LogicalJoinType::Semi => JoinType::SemiJoin,
                    LogicalJoinType::AntiSemi => JoinType::AntiSemiJoin,
                    LogicalJoinType::LeftOuterSemi => JoinType::LeftOuterSemiJoin,
                    LogicalJoinType::AntiLeftOuterSemi => JoinType::AntiLeftOuterSemiJoin,
                };
                let joiner = new_joiner(
                    self.context.clone(),
                    join_type,
                    join.inner_child_idx == 0,
                    &defaults,
                    conditions,
                    left.ret_field_types(),
                    right.ret_field_types(),
                    None,
                    false,
                    JoinerChunkSizes {
                        init_chunk_size: self.init_cap,
                        max_chunk_size: self.max_chunk_size,
                    },
                );
                let (outer, inner, outer_filter, inner_filter) = if join.inner_child_idx == 0 {
                    (right, left, &join.right_conditions, &join.left_conditions)
                } else {
                    (left, right, &join.left_conditions, &join.right_conditions)
                };
                Box::new(crate::apply::native::NestedLoopApplyExec::new(
                    meta,
                    outer,
                    inner,
                    outer_filter.clone(),
                    inner_filter.clone(),
                    outer_schema,
                    joiner,
                    join.join_type != LogicalJoinType::Inner,
                    apply.can_use_cache,
                    self.context.clone(),
                ))
            }
            PhysicalPlan::MergeJoin(join) => {
                let [left, right] = plan.children() else {
                    return Err(ExecError::internal("merge join requires two children"));
                };
                let left = self.build(left)?;
                let right = self.build(right)?;
                Box::new(crate::join::JoinExec::from_merge(
                    meta,
                    join,
                    left,
                    right,
                    self.context.clone(),
                    self.context.statement_memory(),
                )?)
            }
            PhysicalPlan::HashJoin(join) => {
                let [left, right] = plan.children() else {
                    return Err(ExecError::internal("hash join requires two children"));
                };
                let left = self.build(left)?;
                let right = self.build(right)?;
                Box::new(crate::join::JoinExec::from_physical(
                    meta,
                    join,
                    left,
                    right,
                    self.context.clone(),
                    self.context.statement_memory(),
                )?)
            }
            PhysicalPlan::HashAgg(aggregate) => {
                let child = self.child(plan)?;
                let functions = self.aggregate_functions(&aggregate.agg_funcs)?;
                Box::new(
                    crate::hash_agg::HashAggExec::new(
                        meta,
                        aggregate.group_by_items.clone(),
                        functions,
                        child,
                        self.context.clone(),
                        self.context.statement_memory(),
                    )
                    .with_default_row(has_default_row(&aggregate.agg_funcs)),
                )
            }
            PhysicalPlan::StreamAgg(aggregate) => {
                let child = self.child(plan)?;
                let functions = self.aggregate_functions(&aggregate.agg_funcs)?;
                if aggregate.group_by_items.is_empty() {
                    Box::new(
                        crate::hash_agg::StreamAggExec::new(
                            meta,
                            functions,
                            child,
                            self.context.clone(),
                        )
                        .with_default_row(has_default_row(&aggregate.agg_funcs)),
                    )
                } else {
                    let output_positions = (0..functions.len()).collect();
                    Box::new(crate::hash_agg::GroupedStreamAggExec::new(
                        meta,
                        aggregate.group_by_items.clone(),
                        functions,
                        output_positions,
                        child,
                        self.context.clone(),
                    ))
                }
            }
            PhysicalPlan::Selection(selection) => {
                let child = self.child(plan)?;
                Box::new(SelectionExec::new(
                    meta,
                    selection.conditions.clone(),
                    child,
                    self.context.clone(),
                    self.context.statement_memory(),
                ))
            }
            PhysicalPlan::Projection(projection) => {
                let child = self.child(plan)?;
                Box::new(ProjectionExec::with_column_evaluator(
                    meta,
                    projection.exprs.clone(),
                    child,
                    self.context.clone(),
                    projection.avoid_column_evaluator,
                ))
            }
            PhysicalPlan::Limit(limit) => {
                let child = self.child(plan)?;
                Box::new(LimitExec::from_physical(
                    meta,
                    limit.offset,
                    limit.count,
                    child,
                ))
            }
            PhysicalPlan::Sort(sort) => {
                let child = self.child(plan)?;
                Box::new(SortExec::new(
                    meta,
                    by_items(&sort.by_items),
                    child,
                    self.context.clone(),
                    self.context.statement_memory(),
                ))
            }
            PhysicalPlan::TopN(topn) => {
                if topn.prefix_col.is_some() {
                    return Err(ExecError::unsupported(
                        "TopN rank truncation is not implemented",
                    ));
                }
                let child = self.child(plan)?;
                Box::new(crate::topn::TopNExec::new(
                    meta,
                    by_items(&topn.by_items),
                    child,
                    self.context.clone(),
                    topn.offset,
                    topn.count,
                    self.context.statement_memory(),
                ))
            }
            PhysicalPlan::TableDual(dual) => {
                if dual.row_count > 1 {
                    return Err(ExecError::internal(
                        "buildTableDual requires zero or one row",
                    ));
                }
                Box::new(TableDualExec::new(meta, dual.row_count))
            }
            PhysicalPlan::UnionAll(_) => {
                let children = plan
                    .children()
                    .iter()
                    .map(|child| self.build(child))
                    .collect::<Result<_, _>>()?;
                Box::new(crate::union_all::UnionAllExec::new(meta, children))
            }
            PhysicalPlan::MaxOneRow(_) => {
                let child = self.child(plan)?;
                Box::new(crate::max_one_row::MaxOneRowExec::new(meta, child))
            }
            PhysicalPlan::TableScan(_)
            | PhysicalPlan::IndexScan(_)
            | PhysicalPlan::TableReader(_)
            | PhysicalPlan::IndexReader(_)
            | PhysicalPlan::IndexLookUpReader(_) => {
                self.readers.build_reader(plan, self.context, meta)?
            }
            _ => {
                return Err(ExecError::unsupported(format!(
                    "physical executor builder for {} is not implemented",
                    plan.tp()
                )))
            }
        })
    }

    fn aggregate_functions(
        &self,
        descriptors: &[AggFuncDesc],
    ) -> Result<Vec<crate::hash_agg::AggFunc>, ExecError> {
        descriptors
            .iter()
            .map(|desc| crate::hash_agg::AggFunc::from_descriptor(desc, self.context))
            .collect()
    }
}

fn has_default_row(functions: &[AggFuncDesc]) -> bool {
    functions.first().is_some_and(|function| {
        matches!(
            function.mode,
            AggFunctionMode::Complete | AggFunctionMode::Final
        )
    }) && !is_all_first_row(functions)
}

fn by_items(items: &[ByItems]) -> Vec<SortByItem> {
    items
        .iter()
        .map(|item| SortByItem {
            expr: item.expr.clone(),
            desc: item.desc,
        })
        .collect()
}
