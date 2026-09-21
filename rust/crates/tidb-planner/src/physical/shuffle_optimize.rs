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

//! Go `core/plan.go` shuffle selection for window, stream aggregation and merge join.
use super::{BasePhysicalPlan, PhysicalPlan, PhysicalShuffle};
use crate::physical_property::PhysicalProperty;
use crate::plan_base::{PlanError, PlanIdAllocator};
use crate::task::{attach_plan_to_task, Task};
use tidb_expr::expression::Expression;

/// Resolved session inputs for Go's shuffle rewrite.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ShuffleOptions {
    /// WindowConcurrency(), including executor-concurrency fallback.
    pub window_concurrency: usize,
    /// StreamAggConcurrency().
    pub stream_agg_concurrency: usize,
    /// MergeJoinConcurrency().
    pub merge_join_concurrency: usize,
    /// RiskGroupNDVSkewRatio used by EstimateColsNDVWithMatchedLen.
    pub group_ndv_skew_ratio: f64,
}

impl Default for ShuffleOptions {
    fn default() -> Self {
        Self {
            window_concurrency: 5,
            stream_agg_concurrency: 1,
            merge_join_concurrency: 1,
            group_ndv_skew_ratio: 0.0,
        }
    }
}

/// Called after property enforcement, only for unordered non-MPP tasks.
pub fn optimize_by_shuffle(
    task: Task,
    options: ShuffleOptions,
    allocator: &PlanIdAllocator,
) -> Result<Task, PlanError> {
    let Some(plan) = task.plan() else {
        return Ok(task);
    };
    let (mut concurrency, arrays, limit_ndv) = match plan {
        PhysicalPlan::Window(window) => (
            options.window_concurrency,
            vec![window
                .partition_by
                .iter()
                .map(|item| Expression::Column(item.col.clone()))
                .collect::<Vec<_>>()],
            true,
        ),
        PhysicalPlan::StreamAgg(agg) => (
            options.stream_agg_concurrency,
            vec![agg.group_by_items.clone()],
            true,
        ),
        PhysicalPlan::MergeJoin(join) => (
            options.merge_join_concurrency,
            vec![
                join.left_join_keys
                    .iter()
                    .cloned()
                    .map(Expression::Column)
                    .collect(),
                join.right_join_keys
                    .iter()
                    .cloned()
                    .map(Expression::Column)
                    .collect(),
            ],
            false,
        ),
        _ => return Ok(task),
    };
    if concurrency <= 1 {
        return Ok(task);
    }
    let mut tails = Vec::new();
    let mut sources = Vec::new();
    for child in plan.children() {
        let PhysicalPlan::Sort(_) = child else {
            return Ok(task);
        };
        let source = child
            .children()
            .first()
            .ok_or_else(|| PlanError::internal("shuffle sort has no child"))?;
        tails.push(child.id());
        sources.push(source.id());
        if limit_ndv {
            let ids: Vec<_> = arrays[0]
                .iter()
                .filter_map(|expr| match expr {
                    Expression::Column(col) => Some(col.unique_id),
                    _ => None,
                })
                .collect();
            let stats = source
                .stats_info()
                .ok_or_else(|| PlanError::internal("shuffle source has no statistics"))?;
            let ndvs: Vec<_> = stats
                .col_ndvs()
                .iter()
                .map(|(id, value)| (*id, *value))
                .collect();
            let (ndv, _) = crate::cardinality::ndv::estimate_cols_ndv_with_matched_len(
                &ids,
                &ndvs,
                stats.row_count(),
                stats.group_ndvs(),
                options.group_ndv_skew_ratio,
            );
            if ndv <= 1.0 {
                return Ok(task);
            }
            concurrency = concurrency.min(ndv as usize);
        }
    }
    let mut base = BasePhysicalPlan::new(allocator, "Shuffle", plan.query_block_offset());
    base.base.set_stats(plan.stats_info().cloned());
    base.set_children_req_props(vec![Some(PhysicalProperty::default())]);
    let shuffle = PhysicalPlan::Shuffle(PhysicalShuffle {
        base,
        concurrency,
        tails,
        data_sources: sources,
        by_item_arrays: arrays,
        ..Default::default()
    });
    Ok(attach_plan_to_task(
        shuffle,
        task.into_root_task(allocator)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::{
        PhysicalMergeJoin, PhysicalSort, PhysicalStreamAgg, PhysicalTableDual, PhysicalWindow,
    };
    use crate::physical_property::ColumnSortItem;
    use crate::stats_info::StatsInfo;
    use crate::task::RootTask;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::schema::Schema;

    fn column() -> Column {
        Column::new(1, FieldType::new(FieldTypeCode::LongLong))
    }
    fn input(id: i32, ndv: f64, sorted: bool) -> PhysicalPlan {
        let mut base = BasePhysicalPlan::with_id(id, "TableDual", 0);
        base.base.set_schema(Some(Schema::new(vec![column()])));
        base.base.set_stats(Some(StatsInfo::new(100.0, [(1, ndv)])));
        let source = PhysicalPlan::TableDual(PhysicalTableDual { base, row_count: 1 });
        if !sorted {
            return source;
        }
        let mut base = BasePhysicalPlan::with_id(id + 1, "Sort", 0);
        base.set_children(vec![source]);
        PhysicalPlan::Sort(PhysicalSort {
            base,
            ..Default::default()
        })
    }

    #[test]
    fn shuffle_selection_preserves_sort_ndv_and_concurrency_gates() {
        let options = ShuffleOptions {
            window_concurrency: 4,
            stream_agg_concurrency: 4,
            merge_join_concurrency: 4,
            ..Default::default()
        };
        for kind in ["window", "stream", "merge"] {
            for (ndv, sorted, expected) in [
                (8.0, true, 4),
                (2.9, true, 2),
                (1.0, true, 0),
                (8.0, false, 0),
            ] {
                let mut base = BasePhysicalPlan::with_id(50, kind, 0);
                base.set_children(vec![input(10, ndv, sorted)]);
                let plan = match kind {
                    "window" => PhysicalPlan::Window(PhysicalWindow {
                        base,
                        partition_by: vec![ColumnSortItem {
                            col: column(),
                            desc: false,
                        }],
                        ..Default::default()
                    }),
                    "stream" => PhysicalPlan::StreamAgg(PhysicalStreamAgg {
                        base,
                        group_by_items: vec![Expression::Column(column())],
                        ..Default::default()
                    }),
                    _ => {
                        base.set_children(vec![input(10, ndv, sorted), input(20, ndv, sorted)]);
                        PhysicalPlan::MergeJoin(PhysicalMergeJoin {
                            base,
                            left_join_keys: vec![column()],
                            right_join_keys: vec![column()],
                            ..Default::default()
                        })
                    }
                };
                let mut root = RootTask::default();
                root.set_plan(plan);
                let task =
                    optimize_by_shuffle(Task::Root(root), options, &PlanIdAllocator::default())
                        .unwrap();
                // Go does not cap merge-join workers by key NDV.
                let expected = if kind == "merge" && sorted {
                    4
                } else {
                    expected
                };
                match task.plan().unwrap() {
                    PhysicalPlan::Shuffle(shuffle) => {
                        assert_eq!(shuffle.concurrency, expected, "{kind} {ndv} {sorted}");
                        assert_eq!(shuffle.data_sources[0], 10);
                        assert_eq!(shuffle.tails[0], 11);
                    }
                    _ => assert_eq!(expected, 0, "{kind} {ndv} {sorted}"),
                }
            }
        }
    }
}
