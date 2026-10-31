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

//! Go `core/task.go`: `attach2Task` -- how each physical operator composes
//! onto its child task -- with the helpers it uses. Go's operators reach these
//! bodies through `utilfuncp.Attach2Task4X`; the task types are
//! `tidb_planner_core_operator_physicalop::task`'s (Go `physicalop/task_base.go`).

use tidb_planner_core_operator_physicalop::PhysicalPlan;
use tidb_planner_property::MppPartitionType;
use tidb_kv::store_type::StoreType;
use tidb_planner_core_base::PlanError;
use tidb_expr::expression::Expression;
use tidb_planner_cardinality::derive_stats::StatsInfoScale;
use tidb_planner_core_operator_physicalop::task::*;

// ---------------------------------------------------------------------------
// Go `Attach2Task`: how a physical operator composes onto a child task.
// ---------------------------------------------------------------------------

/// Core's `attach2Task`, registered for the operators (Go `core_init.go`'s
/// `utilfuncp.Attach2Task4X`).
#[derive(Clone, Copy, Debug, Default)]
pub struct CoreTaskAttacher;

impl TaskAttacher for CoreTaskAttacher {
    fn attach2_task(
        &self,
        plan: PhysicalPlan,
        tasks: Vec<Task>,
        allocator: &tidb_planner_core_base::PlanIdAllocator,
        evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    ) -> Result<Task, PlanError> {
        attach2_task_in(
            plan,
            tasks,
            None,
            allocator,
            tidb_vardef::defaults::DEF_OPT_RISK_GROUP_NDV_SKEW_RATIO,
            evaluate,
            &Default::default(),
        )
    }
}

/// The body of Go's `attach2Task4PhysicalUnionScan` after the child became a
/// root task: `unionScan -> selection -> projection` becomes `projection ->
/// unionScan -> selection`, `unionScan -> projection` becomes `projection ->
/// unionScan`, and anything else is attached as is.
fn attach_union_scan(mut union_scan: PhysicalPlan, mut task: Task) -> Task {
    let Task::Root(root) = &mut task else {
        return attach_plan_to_task(union_scan, task);
    };
    let Some(mut child) = root.take_plan() else {
        return attach_plan_to_task(union_scan, task);
    };
    let stats = child.base().base.stats_info().cloned();
    let is_projection = |plan: &PhysicalPlan| matches!(plan, PhysicalPlan::Projection(_));
    let selection_over_projection = matches!(&child, PhysicalPlan::Selection(_))
        && child.children().first().is_some_and(is_projection);
    if selection_over_projection {
        // Convert unionScan->selection->projection to
        // projection->unionScan->selection.
        let mut projection = child.base_mut().children_mut().remove(0);
        let projection_children = std::mem::take(projection.base_mut().children_mut());
        child.base_mut().set_children(projection_children);
        union_scan.base_mut().set_children(vec![child]);
        union_scan.base_mut().base.set_stats(stats);
        root.set_plan(union_scan);
        return attach_plan_to_task(projection, task);
    }
    if is_projection(&child) {
        // Convert unionScan->projection to projection->unionScan.
        let projection_children = std::mem::take(child.base_mut().children_mut());
        union_scan.base_mut().set_children(projection_children);
        union_scan.base_mut().base.set_stats(stats);
        root.set_plan(union_scan);
        return attach_plan_to_task(child, task);
    }
    root.set_plan(child);
    union_scan.base_mut().base.set_stats(stats);
    attach_plan_to_task(union_scan, task)
}

/// Go `attachPlan2Task` (`core/task.go`): wrap the task's plan with `plan`.
///
/// * a root task: `p.SetChildren(v.GetPlan()); v.SetPlan(p)`;
/// * an MPP task: the same wrap on its plan;
/// * a cop task: the plan attaches to whichever half is still OPEN — the
///   index half before `FinishIndexPlan`, the table half after.
///
/// Go's `inheritStatsFromBottomTaskForIndexJoinInner` hook runs first. An
/// operator above a dynamic index-join probe must describe one probe result,
/// not retain the cardinality derived for its ordinary full-input subtree.
#[must_use]
pub fn attach_plan_to_task(mut plan: PhysicalPlan, mut task: Task) -> Task {
    let feedback = match &task {
        Task::Root(root) => root.index_join_info.is_some(),
        Task::Cop(cop) => cop.index_join_info.is_some(),
        Task::Mpp(_) => false,
    };
    inherit_index_join_stats(&mut plan, feedback, task.plan());
    match &mut task {
        Task::Root(root) => {
            let child = root.take_plan();
            plan.base_mut().set_children(child.into_iter().collect());
            root.set_plan(plan);
        }
        Task::Mpp(mpp) => {
            let child = mpp.plan.take().map(|boxed| *boxed);
            plan.base_mut().set_children(child.into_iter().collect());
            mpp.plan = Some(Box::new(plan));
        }
        Task::Cop(cop) => {
            if cop.index_plan_finished {
                let child = cop.table_plan.take().map(|boxed| *boxed);
                plan.base_mut().set_children(child.into_iter().collect());
                cop.table_plan = Some(Box::new(plan));
            } else {
                let child = cop.index_plan.take().map(|boxed| *boxed);
                plan.base_mut().set_children(child.into_iter().collect());
                cop.index_plan = Some(Box::new(plan));
            }
        }
    }
    task
}

fn inherit_index_join_stats(
    plan: &mut PhysicalPlan,
    has_feedback: bool,
    child: Option<&PhysicalPlan>,
) {
    if has_feedback && !matches!(plan, PhysicalPlan::IndexJoin(_)) {
        plan.base_mut().base.set_stats(
            child
                .and_then(PhysicalPlan::stats_info)
                .map(|stats| stats.scale(1.0, 1.0)),
        );
    }
}

/// Go `attach2TaskForMpp` for a physical HashAgg.  The MPP candidate carries
/// the requested run mode; this helper performs the partial/final split and
/// places the exchange between the two phases where the Go planner does.
fn attach_hash_agg_to_mpp(
    mut plan: PhysicalPlan,
    mpp: MppTask,
    column_ids: Option<&tidb_planner_core_base::ColumnIdAllocator>,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
    evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    group_ndv_skew_ratio: f64,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> Result<Task, PlanError> {
    let PhysicalPlan::HashAgg(hash_agg) = &plan else {
        unreachable!("MPP HashAgg attachment received a non-HashAgg plan");
    };
    let mode = hash_agg.mpp_run_mode;
    let requested_partition_cols = hash_agg.mpp_partition_cols.clone();
    let enable_3_stage_distinct_agg = hash_agg.enable_3_stage_distinct_agg;
    let enable_3_stage_multi_distinct_agg = hash_agg.enable_3_stage_multi_distinct_agg;
    let tiflash_pre_agg_mode = hash_agg.tiflash_pre_agg_mode.clone();
    let expr_ctx = tidb_exprctx::ZonedNoColumns(tidb_datatype::SessionTimeZone::utc());
    let multi_distinct_grouping_sets = if mode == tidb_planner_core_operator_physicalop::AggMppRunMode::MppScalar {
        let PhysicalPlan::HashAgg(hash_agg) = &mut plan else {
            unreachable!("MPP scalar aggregate is a HashAgg");
        };
        let grouping_sets = tidb_planner_core_operator_physicalop::final_mode_agg::can_use_three_stage_multi_distinct(
            &hash_agg.agg_funcs,
            &hash_agg.group_by_items,
            enable_3_stage_distinct_agg,
            enable_3_stage_multi_distinct_agg,
        );
        if let Some(grouping_sets) = &grouping_sets {
            if !tidb_planner_core_operator_physicalop::final_mode_agg::mark_three_stage_grouping_ids(
                &mut hash_agg.agg_funcs,
                grouping_sets,
            ) {
                return Ok(Task::invalid_task());
            }
        }
        grouping_sets
    } else {
        None
    };
    let mut avg_projection = None;
    if matches!(
        mode,
        tidb_planner_core_operator_physicalop::AggMppRunMode::Mpp1Phase
            | tidb_planner_core_operator_physicalop::AggMppRunMode::Mpp2Phase
            | tidb_planner_core_operator_physicalop::AggMppRunMode::MppScalar
    ) {
        if let Some(column_ids) = column_ids {
            avg_projection =
                tidb_planner_core_operator_physicalop::final_mode_agg::convert_avg_for_mpp(&mut plan, column_ids, allocator)?;
        }
    }

    match mode {
        tidb_planner_core_operator_physicalop::AggMppRunMode::NoMpp | tidb_planner_core_operator_physicalop::AggMppRunMode::Mpp1Phase => {
            let task = attach_plan_to_task(plan, Task::Mpp(mpp));
            if let Some(projection) = avg_projection.take() {
                Ok(attach_plan_to_task(
                    PhysicalPlan::Projection(projection),
                    task,
                ))
            } else {
                Ok(task)
            }
        }
        tidb_planner_core_operator_physicalop::AggMppRunMode::Mpp2Phase => {
            let Some(column_ids) = column_ids else {
                return Err(PlanError::internal(
                    "MPP HashAgg partial construction needs a column allocator",
                ));
            };
            let (Some(partial), final_agg) = tidb_planner_core_operator_physicalop::final_mode_agg::new_partial_aggregate_mpp(
                &expr_ctx,
                column_ids,
                plan,
                allocator,
                blacklist,
            )? else {
                return Ok(Task::invalid_task());
            };
            let mut partial = partial;
            if let PhysicalPlan::HashAgg(partial_hash_agg) = &mut partial {
                partial_hash_agg.tiflash_pre_agg_mode = tiflash_pre_agg_mode.clone();
            }
            let task = attach_plan_to_task(partial, Task::Mpp(mpp));
            let Task::Mpp(mpp) = task else {
                unreachable!("partial HashAgg attachment must retain MPP task");
            };
            let partition_cols = if requested_partition_cols.is_empty() {
                let PhysicalPlan::HashAgg(final_hash_agg) = &final_agg else {
                    return Ok(Task::invalid_task());
                };
                final_hash_agg
                    .group_by_items
                    .iter()
                    .map(|item| match item {
                        Expression::Column(column) => {
                            Ok(tidb_planner_property::MppPartitionColumn {
                                collate_id: column
                                    .get_static_type()
                                    .map(|field_type| {
                                        tidb_planner_property::collate_id_for_partition(
                                            field_type.collation_name(),
                                        )
                                    })
                                    .unwrap_or(-1),
                                col: column.clone(),
                            })
                        }
                        _ => Err(PlanError::internal(
                            "MPP HashAgg partition key must be a column",
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                requested_partition_cols
            };
            if partition_cols.is_empty() {
                return Ok(Task::invalid_task());
            }
            let required = tidb_planner_property::PhysicalProperty {
                task_tp: tidb_planner_property::task_type::TaskType::Mpp,
                expected_cnt: f64::MAX,
                mpp_partition_tp: MppPartitionType::Hash,
                mpp_partition_cols: partition_cols.clone(),
                ..tidb_planner_property::PhysicalProperty::default()
            };
            let mpp = mpp.enforce_exchanger_impl(&required, allocator)?;
            let task = attach_plan_to_task(final_agg, Task::Mpp(mpp));
            if let Some(projection) = avg_projection.take() {
                Ok(attach_plan_to_task(
                    PhysicalPlan::Projection(projection),
                    task,
                ))
            } else {
                Ok(task)
            }
        }
        tidb_planner_core_operator_physicalop::AggMppRunMode::MppTiDB => {
            let Some(column_ids) = column_ids else {
                return Err(PlanError::internal(
                    "MPP HashAgg partial construction needs a column allocator",
                ));
            };
            let (partial, final_agg) = tidb_planner_core_operator_physicalop::final_mode_agg::new_partial_aggregate_mpp_tidb(
                &expr_ctx,
                column_ids,
                plan,
                allocator,
                blacklist,
            )?;
            let task = if let Some(partial) = partial {
                attach_plan_to_task(partial, Task::Mpp(mpp))
            } else {
                Task::Mpp(mpp)
            };
            let task = task.into_root_task_in(allocator, evaluate)?;
            let task = attach_plan_to_task(final_agg, task);
            if let Some(projection) = avg_projection.take() {
                Ok(attach_plan_to_task(
                    PhysicalPlan::Projection(projection),
                    task,
                ))
            } else {
                Ok(task)
            }
        }
        tidb_planner_core_operator_physicalop::AggMppRunMode::MppScalar => {
            let can_use_three_stage_single = match &plan {
                PhysicalPlan::HashAgg(hash_agg) => {
                    tidb_planner_core_operator_physicalop::final_mode_agg::can_use_three_stage_single_distinct(
                        &hash_agg.agg_funcs,
                        &hash_agg.group_by_items,
                    )
                }
                _ => false,
            };
            let can_use_three_stage =
                can_use_three_stage_single || multi_distinct_grouping_sets.is_some();
            let original_schema = plan.schema().cloned();
            let required = tidb_planner_property::PhysicalProperty {
                task_tp: tidb_planner_property::task_type::TaskType::Mpp,
                expected_cnt: f64::MAX,
                mpp_partition_tp: MppPartitionType::SinglePartition,
                ..tidb_planner_property::PhysicalProperty::default()
            };
            if !tidb_planner_property::need_enforce_exchanger(
                mpp.partition_type(),
                mpp.hash_cols(),
                &required,
                None,
            ) {
                let task = attach_plan_to_task(plan, Task::Mpp(mpp));
                if let Some(projection) = avg_projection.take() {
                    return Ok(attach_plan_to_task(
                        PhysicalPlan::Projection(projection),
                        task,
                    ));
                }
                return Ok(task);
            }
            let Some(column_ids) = column_ids else {
                return Err(PlanError::internal(
                    "MPP HashAgg partial construction needs a column allocator",
                ));
            };
            let (partial, final_agg) = tidb_planner_core_operator_physicalop::final_mode_agg::new_partial_aggregate_mpp(
                &expr_ctx,
                column_ids,
                plan,
                allocator,
                blacklist,
            )?;
            if can_use_three_stage {
                let Some(partial) = partial else {
                    return Ok(Task::invalid_task());
                };
                if let Some(grouping_sets) = multi_distinct_grouping_sets {
                    let child = mpp.plan.as_deref().ok_or_else(|| {
                        PlanError::internal("scalar MPP aggregate has no child plan")
                    })?;
                    let Some(split) =
                        tidb_planner_core_operator_physicalop::final_mode_agg::adjust_three_stage_multi_distinct_with_group_ndv_skew_ratio(
                            partial,
                            final_agg,
                            child,
                            &grouping_sets,
                            column_ids,
                            allocator,
                            group_ndv_skew_ratio,
                        )?
                    else {
                        return Ok(Task::invalid_task());
                    };
                    let task = attach_plan_to_task(split.expand, Task::Mpp(mpp));
                    let task = attach_plan_to_task(split.partial_projection, task);
                    let mut partial = split.partial;
                    if let PhysicalPlan::HashAgg(partial_hash_agg) = &mut partial {
                        partial_hash_agg.tiflash_pre_agg_mode = tiflash_pre_agg_mode.clone();
                    }
                    let task = attach_plan_to_task(partial, task);
                    let Task::Mpp(mpp) = task else {
                        unreachable!("multi-distinct partial HashAgg must retain MPP task");
                    };
                    let hash_required = tidb_planner_property::PhysicalProperty {
                        task_tp: tidb_planner_property::task_type::TaskType::Mpp,
                        expected_cnt: f64::MAX,
                        mpp_partition_tp: MppPartitionType::Hash,
                        mpp_partition_cols: split.partition_cols,
                        ..tidb_planner_property::PhysicalProperty::default()
                    };
                    let mpp = mpp.enforce_exchanger(&hash_required, allocator)?;
                    let task = attach_plan_to_task(split.middle, Task::Mpp(mpp));
                    let Task::Mpp(mpp) = task else {
                        unreachable!("multi-distinct middle HashAgg must retain MPP task");
                    };
                    let mpp = mpp.enforce_exchanger(&required, allocator)?;
                    let task = attach_plan_to_task(split.final_agg, Task::Mpp(mpp));
                    return finish_scalar_aggregate_output(
                        task,
                        avg_projection,
                        original_schema.as_ref(),
                        allocator,
                    );
                }
                let Some(split) = tidb_planner_core_operator_physicalop::final_mode_agg::adjust_three_stage_single_distinct(
                    partial, final_agg, column_ids, allocator,
                )?
                else {
                    return Ok(Task::invalid_task());
                };
                let task = attach_plan_to_task(split.partial, Task::Mpp(mpp));
                let Task::Mpp(mpp) = task else {
                    unreachable!("three-stage partial HashAgg must retain MPP task");
                };
                let hash_required = tidb_planner_property::PhysicalProperty {
                    task_tp: tidb_planner_property::task_type::TaskType::Mpp,
                    expected_cnt: f64::MAX,
                    mpp_partition_tp: MppPartitionType::Hash,
                    mpp_partition_cols: split.partition_cols,
                    ..tidb_planner_property::PhysicalProperty::default()
                };
                let mpp = mpp.enforce_exchanger(&hash_required, allocator)?;
                let task = attach_plan_to_task(split.middle, Task::Mpp(mpp));
                let Task::Mpp(mpp) = task else {
                    unreachable!("three-stage middle HashAgg must retain MPP task");
                };
                let mpp = mpp.enforce_exchanger(&required, allocator)?;
                let task = attach_plan_to_task(split.final_agg, Task::Mpp(mpp));
                return finish_scalar_aggregate_output(
                    task,
                    avg_projection,
                    original_schema.as_ref(),
                    allocator,
                );
            }
            let mpp = if let Some(partial) = partial {
                let task = attach_plan_to_task(partial, Task::Mpp(mpp));
                let Task::Mpp(mpp) = task else {
                    unreachable!("scalar partial HashAgg must retain MPP task");
                };
                mpp
            } else {
                mpp
            };
            let mpp = mpp.enforce_exchanger(&required, allocator)?;
            let task = attach_plan_to_task(final_agg, Task::Mpp(mpp));
            finish_scalar_aggregate_output(
                task,
                avg_projection,
                original_schema.as_ref(),
                allocator,
            )
        }
    }
}

/// Go's scalar MPP path always restores the original aggregate schema with a
/// projection after the single-partition final stage. AVG supplies its own
/// CASE/DIV projection; all other descriptors use this identity projection.
fn finish_scalar_aggregate_output(
    task: Task,
    avg_projection: Option<tidb_planner_core_operator_physicalop::PhysicalProjection>,
    original_schema: Option<&tidb_expr::schema::Schema>,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
) -> Result<Task, PlanError> {
    if let Some(projection) = avg_projection {
        return Ok(attach_plan_to_task(
            PhysicalPlan::Projection(projection),
            task,
        ));
    }
    let Some(schema) = original_schema else {
        return Ok(task);
    };
    let child = task
        .plan()
        .ok_or_else(|| PlanError::internal("scalar MPP aggregate has no final plan"))?;
    let mut base =
        tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(allocator, "Projection", child.query_block_offset());
    base.base.set_stats(child.stats_info().cloned());
    base.base.set_schema(Some(schema.clone()));
    base.set_children_req_props(vec![Some(
        tidb_planner_property::PhysicalProperty::default(),
    )]);
    let projection = PhysicalPlan::Projection(tidb_planner_core_operator_physicalop::PhysicalProjection {
        base,
        exprs: schema
            .columns
            .iter()
            .cloned()
            .map(Expression::Column)
            .collect(),
        calculate_no_delay: false,
        avoid_column_evaluator: false,
    });
    Ok(attach_plan_to_task(projection, task))
}

/// Go `Attach2Task` per operator — the ROOT-TASK slice.
///
/// Every ported arm reproduces its Go body exactly for a root child task.
/// Remaining cop and MPP gaps are kept explicit: cop conversion still needs
/// `convertToRootTaskImpl`, while heavy-function TopN and scalar multi-distinct
/// aggregation still need their Go counterparts. A wrong push-down
/// is a silent wrong plan; a refusal is a loud gap.
/// Go `sinkIntoIndexLookUp` (`task.go:733`): after conversion, a root Limit
/// SINKS into the `PhysicalIndexLookUpReader` (directly or under one
/// Projection) as its `PushedLimit`, cutting the double read short — but
/// only when the table side is a bare `PhysicalTableScan`. The table scan
/// and reader adopt the limit's stats when smaller (Go's `StatsVersion`
/// carry-over has no counterpart field here). The schema-mending extra
/// projection (issue 14428's inlined-Projection shape) compares schema
/// LENGTHS; schemas absent on both sides compare equal.
fn sink_into_index_look_up(
    limit: &tidb_planner_core_operator_physicalop::PhysicalLimit,
    task: &mut Task,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
) -> bool {
    let Task::Root(root) = task else {
        return false;
    };
    let Some(mut plan) = root.take_plan() else {
        return false;
    };
    let limit_schema_len = limit.base.base.schema().map_or(0, |schema| schema.len());
    let limit_stats = limit.base.base.stats_info().cloned();
    let sunk = {
        let (reader, via_proj) = match &mut plan {
            PhysicalPlan::IndexLookUpReader(reader) => (Some(reader), false),
            PhysicalPlan::Projection(_) => {
                let is_lookup = matches!(
                    plan.children().first(),
                    Some(PhysicalPlan::IndexLookUpReader(_))
                );
                if is_lookup {
                    let PhysicalPlan::Projection(proj) = &mut plan else {
                        unreachable!("the arm matched Projection");
                    };
                    let Some(PhysicalPlan::IndexLookUpReader(reader)) =
                        proj.base.children_mut().first_mut()
                    else {
                        unreachable!("checked above");
                    };
                    (Some(reader), true)
                } else {
                    (None, false)
                }
            }
            _ => (None, false),
        };
        match reader {
            None => false,
            Some(reader) => {
                // Only a bare table scan admits the sink: a Selection on the
                // table side must see every row.
                let ts_stats = match reader.table_plan.as_deref() {
                    Some(PhysicalPlan::TableScan(scan)) => scan.base.base.stats_info().cloned(),
                    _ => {
                        root.set_plan(plan);
                        return false;
                    }
                };
                let reader_schema_len = reader.base.base.schema().map_or(0, |schema| schema.len());
                reader.pushed_limit = Some(tidb_planner_core_operator_physicalop::PushedDownLimit {
                    offset: limit.offset,
                    count: limit.count,
                });
                let limit_rows = limit_stats.as_ref().map_or(0.0, |stats| stats.row_count());
                if let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref_mut() {
                    if ts_stats
                        .as_ref()
                        .is_some_and(|stats| stats.row_count() >= limit_rows)
                    {
                        // Go `sinkIntoIndexLookUp` stamps the limit's fresh
                        // stats and then restores the scan's own version:
                        // "keep the original stats version"
                        // (`task.go:792-796`) — without this the analyzed
                        // table side renders `stats:pseudo`.
                        let mut stamped = limit_stats.clone();
                        if let Some(origin) = ts_stats.as_ref() {
                            stamped = stamped
                                .map(|stats| stats.with_stats_version(origin.stats_version()));
                        }
                        scan.base.base.set_stats(stamped);
                    }
                }
                reader.base.base.set_stats(limit_stats.clone());
                if via_proj {
                    if let PhysicalPlan::Projection(proj) = &mut plan {
                        proj.base.base.set_stats(limit_stats.clone());
                    }
                }
                // The schema-mending projection above the reader.
                if limit_schema_len != reader_schema_len {
                    let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                        allocator,
                        "Projection",
                        limit.base.base.query_block_offset(),
                    );
                    base.base.set_stats(limit_stats);
                    base.base.set_schema(limit.base.base.schema().cloned());
                    let exprs = limit.base.base.schema().map_or_else(Vec::new, |schema| {
                        schema
                            .columns
                            .iter()
                            .cloned()
                            .map(tidb_expr::expression::Expression::Column)
                            .collect()
                    });
                    base.set_children(vec![plan]);
                    plan = PhysicalPlan::Projection(tidb_planner_core_operator_physicalop::PhysicalProjection {
                        base,
                        exprs,
                        avoid_column_evaluator: false,
                        calculate_no_delay: false,
                    });
                }
                true
            }
        }
    };
    root.set_plan(plan);
    sunk
}

/// Go `sinkIntoIndexMerge`: limit handles only when the table probe is a
/// bare scan. A root Selection or table filter must run before truncation.
fn sink_into_index_merge(
    limit: &tidb_planner_core_operator_physicalop::PhysicalLimit,
    task: &mut Task,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
) -> bool {
    let Task::Root(root) = task else { return false };
    let Some(mut plan) = root.take_plan() else {
        return false;
    };
    let reader = match &mut plan {
        PhysicalPlan::IndexMergeReader(reader) => Some(reader),
        PhysicalPlan::Projection(projection) => match projection.base.children_mut().first_mut() {
            Some(PhysicalPlan::IndexMergeReader(reader)) => Some(reader),
            _ => None,
        },
        _ => None,
    };
    let sunk = if let Some(reader) = reader {
        if matches!(
            reader.table_plan.as_deref(),
            Some(PhysicalPlan::TableScan(_))
        ) {
            reader.pushed_limit = Some(tidb_planner_core_operator_physicalop::PushedDownLimit {
                offset: limit.offset,
                count: limit.count,
            });
            true
        } else {
            false
        }
    } else {
        false
    };
    if sunk {
        let output = limit.base.base.schema();
        let actual = plan.schema();
        let need_projection = output.map_or(0, |schema| schema.len())
            != actual.map_or(0, |schema| schema.len())
            || output
                .into_iter()
                .flat_map(|schema| &schema.columns)
                .zip(actual.into_iter().flat_map(|schema| &schema.columns))
                .any(|(left, right)| left.unique_id != right.unique_id);
        if need_projection {
            let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                allocator,
                "Projection",
                limit.base.base.query_block_offset(),
            );
            base.base.set_stats(limit.base.base.stats_info().cloned());
            base.base.set_schema(output.cloned());
            let exprs = output
                .into_iter()
                .flat_map(|schema| &schema.columns)
                .cloned()
                .map(Expression::Column)
                .collect();
            base.set_children(vec![plan]);
            plan = PhysicalPlan::Projection(tidb_planner_core_operator_physicalop::PhysicalProjection {
                base,
                exprs,
                calculate_no_delay: false,
                avoid_column_evaluator: false,
            });
        }
    }
    root.set_plan(plan);
    sunk
}

/// Go's merge-specific branch of `attach2Task4PhysicalLimit`. Union may cap
/// each partial; intersection must finish membership before limiting handles.
fn attach_limit_to_index_merge(
    plan: PhysicalPlan,
    mut cop: CopTask,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
    evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
) -> Result<Task, PlanError> {
    let PhysicalPlan::Limit(limit) = &plan else {
        unreachable!()
    };
    let push_limit = |child: PhysicalPlan| {
        let count = limit.offset.wrapping_add(limit.count);
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
            allocator,
            "Limit",
            limit.base.base.query_block_offset(),
        );
        base.base.set_stats(
            child
                .stats_info()
                .map(|stats| stats.derive_limit_stats(count as f64)),
        );
        base.base.set_schema(child.schema().cloned());
        base.set_children(vec![child]);
        PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
            base,
            partition_by: limit.partition_by.clone(),
            offset: 0,
            count,
            prefix_col: None,
            prefix_len: 0,
        })
    };
    if cop.root_task_conds.is_empty() {
        if cop.index_plan_finished && (cop.idx_merge_is_intersection || !cop.keep_order) {
            if let Some(table) = cop.table_plan.take() {
                cop.table_plan = Some(Box::new(push_limit(*table)));
            }
        } else if !cop.idx_merge_is_intersection && !cop.index_plan_finished {
            cop.idx_merge_part_plans = std::mem::take(&mut cop.idx_merge_part_plans)
                .into_iter()
                .map(push_limit)
                .collect();
        }
    }
    let mut task = Task::Cop(cop).into_root_task_in(allocator, evaluate)?;
    if sink_into_index_merge(limit, &mut task, allocator) || !limit.partition_by.is_empty() {
        return Ok(task);
    }
    Ok(attach_plan_to_task(plan, task))
}

/// Go `handleAdvisorySortItemsForIndexMerge`: preserve each partial's property
/// feedback until the parent TopN attaches to the unfinished cop task.
fn attach_advisory_topn_to_index_merge(
    topn: &tidb_planner_core_operator_physicalop::PhysicalTopN,
    mut cop: CopTask,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
    evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
) -> Result<Task, PlanError> {
    let order = cop
        .idx_merge_order
        .as_ref()
        .expect("advisory merge order was checked");
    if order.partial_matches.len() != cop.idx_merge_part_plans.len() {
        return Err(PlanError::internal(
            "merge property results do not match partial plans",
        ));
    }
    let by_exprs: Vec<_> = topn.by_items.iter().map(|item| item.expr.clone()).collect();
    let columns = tidb_expr::simple_expr::extract_columns_from_expressions(&by_exprs, None);
    let count = topn.offset.wrapping_add(topn.count);
    let mut partials = Vec::with_capacity(cop.idx_merge_part_plans.len());
    for (child, matched) in std::mem::take(&mut cop.idx_merge_part_plans)
        .into_iter()
        .zip(&order.partial_matches)
    {
        if matched.matched() {
            let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                allocator,
                "Limit",
                topn.base.base.query_block_offset(),
            );
            base.base.set_stats(
                child
                    .stats_info()
                    .map(|stats| stats.derive_limit_stats(count as f64)),
            );
            base.base.set_schema(child.schema().cloned());
            base.set_children(vec![child]);
            partials.push(PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
                base,
                partition_by: topn.partition_by.clone(),
                offset: 0,
                count,
                prefix_col: None,
                prefix_len: 0,
            }));
        } else if can_push_to_index_plan(Some(&child), &columns) {
            partials.push(merge_partial_topn(topn, child, allocator));
        } else {
            partials.push(child);
        }
    }
    cop.idx_merge_part_plans = partials;
    if let Some(table) = cop.table_plan.take() {
        cop.table_plan = Some(Box::new(merge_partial_topn(topn, *table, allocator)));
    }
    let converted = Task::Cop(cop).into_root_task_in(allocator, evaluate)?;
    if !topn.partition_by.is_empty() {
        return Ok(converted);
    }
    Ok(attach_plan_to_task(
        PhysicalPlan::TopN(topn.clone()),
        converted,
    ))
}

// Advisory order currently originates from column-only TopN items, so no
// heavy-function projection split is needed on these partial plans.
fn merge_partial_topn(
    topn: &tidb_planner_core_operator_physicalop::PhysicalTopN,
    child: PhysicalPlan,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
) -> PhysicalPlan {
    let count = topn.offset.wrapping_add(topn.count);
    let mut pushed = topn.clone();
    pushed.base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
        allocator,
        "TopN",
        topn.base.base.query_block_offset(),
    );
    pushed.base.base.set_stats(
        child
            .stats_info()
            .map(|stats| stats.derive_limit_stats(count as f64)),
    );
    pushed.base.base.set_schema(child.schema().cloned());
    pushed.base.set_children(vec![child]);
    pushed.offset = 0;
    pushed.count = count;
    PhysicalPlan::TopN(pushed)
}

/// Go `canPushToIndexPlan` (`core/task.go`): every referenced column must be
/// supplied by the open index half as a full, non-prefix column.
fn can_push_to_index_plan(
    index_plan: Option<&PhysicalPlan>,
    columns: &[tidb_expr::column::Column],
) -> bool {
    let Some(schema) = index_plan.and_then(PhysicalPlan::schema) else {
        return false;
    };
    columns.iter().all(|column| {
        let position = schema.column_index(column);
        position >= 0 && !schema.columns[position as usize].is_prefix
    })
}

/// Go `pushLimitDownToTiDBCop` (`core/task.go`, tidb#51723): for
/// `CLUSTER_SLOW_QUERY` only, `TopN -> TableReader -> TableFullScan[cop]`
/// becomes `TopN -> TableReader -> Limit[cop] -> scan[cop] + keepOrder`, when
/// the TopN sorts by exactly the table's handle column. Returns the finished
/// task, or the untouched cop task back when the shape does not apply.
fn push_limit_down_to_tidb_cop(
    topn: &tidb_planner_core_operator_physicalop::PhysicalTopN,
    mut cop: CopTask,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
    evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
) -> Result<Result<Task, CopTask>, PlanError> {
    if cop.index_plan.is_some() || cop.table_plan.is_none() {
        return Ok(Err(cop));
    }
    // Walk to the leaf, remembering whether a Selection sits right above it.
    let mut selectivity_source: Option<f64> = None;
    let (scan_rows, desc) = {
        let Some(table_plan) = cop.table_plan.as_deref() else {
            return Ok(Err(cop));
        };
        let mut node = table_plan;
        let mut selection_rows = None;
        while let Some(child) = node.children().first() {
            selection_rows = matches!(node, PhysicalPlan::Selection(_))
                .then(|| node.stats_info().map(tidb_planner_property::stats_info::StatsInfo::row_count))
                .flatten();
            node = child;
        }
        let PhysicalPlan::TableScan(scan) = node else {
            return Ok(Err(cop));
        };
        if scan.table_name != "CLUSTER_SLOW_QUERY" {
            return Ok(Err(cop));
        }
        let Some(sort_items) = tidb_planner_core_operator_physicalop::get_prop_by_order_by_items(&topn.by_items) else {
            return Ok(Err(cop));
        };
        // "For cluster tables, HandleCols may be nil."
        let Some(handle) = scan.handle_col_unique_id else {
            return Ok(Err(cop));
        };
        let [item] = sort_items.as_slice() else {
            return Ok(Err(cop));
        };
        if item.col.unique_id != handle {
            return Ok(Err(cop));
        }
        let scan_rows = node.stats_info().map_or(0.0, tidb_planner_property::stats_info::StatsInfo::row_count);
        if let Some(rows) = selection_rows {
            if scan_rows > 0.0 {
                selectivity_source = Some(rows / scan_rows);
            }
        }
        (scan_rows, item.desc)
    };
    let _ = scan_rows;
    let skew_ratio = cop.stats_context.scale_ndv_skew_ratio;
    // The scan keeps the handle order, descending when the TopN is.
    fn mark_scan(plan: &mut PhysicalPlan, desc: bool) {
        match plan {
            PhysicalPlan::TableScan(scan) => {
                scan.desc = desc;
                scan.keep_order = true;
            }
            other => {
                if let Some(child) = other.base_mut().children_mut().first_mut() {
                    mark_scan(child, desc);
                }
            }
        }
    }
    let mut table_plan = *cop.table_plan.take().expect("the table plan was checked above");
    mark_scan(&mut table_plan, desc);
    let new_count = topn.offset.wrapping_add(topn.count);
    let child_profile = table_plan.stats_info().cloned();
    let mut base =
        tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(allocator, "Limit", topn.base.base.query_block_offset());
    base.base.set_stats(
        child_profile
            .as_ref()
            .map(|profile| profile.derive_limit_stats(new_count as f64)),
    );
    base.base.set_schema(table_plan.schema().cloned());
    // The limit's child is the (cloned) table plan: scale its top node's
    // statistics to the pushed row count, then the scan's back up by the
    // selection's selectivity.
    let scaled = table_plan
        .stats_info()
        .map(|stats| stats.scale_by_expect_cnt(new_count as f64, skew_ratio));
    table_plan.base_mut().base.set_stats(scaled);
    if let Some(selectivity) = selectivity_source.filter(|s| *s > 0.0 && *s < 1.0) {
        let scaled_row_count = table_plan
            .stats_info()
            .map_or(0.0, tidb_planner_property::stats_info::StatsInfo::row_count)
            / selectivity;
        fn rescale_scan(plan: &mut PhysicalPlan, expect: f64, skew_ratio: f64) {
            match plan {
                PhysicalPlan::TableScan(_) => {
                    let scaled = plan
                        .stats_info()
                        .map(|stats| stats.scale_by_expect_cnt(expect, skew_ratio));
                    plan.base_mut().base.set_stats(scaled);
                }
                other => {
                    if let Some(child) = other.base_mut().children_mut().first_mut() {
                        rescale_scan(child, expect, skew_ratio);
                    }
                }
            }
        }
        rescale_scan(&mut table_plan, scaled_row_count, skew_ratio);
    }
    base.set_children(vec![table_plan]);
    cop.table_plan = Some(Box::new(PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
        base,
        partition_by: Vec::new(),
        offset: 0,
        count: new_count,
        prefix_col: None,
        prefix_len: 0,
    })));
    let root = Task::Cop(cop).into_root_task_in(allocator, evaluate)?;
    Ok(Ok(attach_plan_to_task(PhysicalPlan::TopN(topn.clone()), root)))
}

/// The shared cop body of Go `attach2Task4PhysicalStreamAgg` and
/// `...HashAgg` after their gates: split via `NewPartialAggregate`, hang the
/// partial half on the cop task's live side, convert, and attach the final
/// half at root, preserving the inner lookup's per-outer-row statistics.
fn attach_agg_over_cop(
    plan: PhysicalPlan,
    mut cop: CopTask,
    column_ids: Option<&tidb_planner_core_base::ColumnIdAllocator>,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
    evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> Result<Task, PlanError> {
    let Some(column_ids) = column_ids else {
        return Err(PlanError::internal(
            "the aggregate cop push needs the caller's column id allocator",
        ));
    };
    // Go's expression context is consulted only for argument TYPES during
    // the split's TypeInfer; a column-free context is exact for that.
    let ctx = tidb_exprctx::ZonedNoColumns(tidb_datatype::SessionTimeZone::utc());
    let query_block_offset = plan.query_block_offset();
    // Go `NewPartialAggregate(cop.GetStoreType(), false)`.
    let (partial, final_plan) = if cop.get_store_type() == StoreType::TiDb {
        tidb_planner_core_operator_physicalop::final_mode_agg::new_partial_aggregate_tidb(&ctx, column_ids, plan, allocator, blacklist)?
    } else {
        tidb_planner_core_operator_physicalop::final_mode_agg::new_partial_aggregate(&ctx, column_ids, plan, allocator, blacklist)?
    };
    let Some(mut partial) = partial else {
        // Go `NewPartialAggregate` answers `(nil, p.Self)` when the split is
        // impossible (an aggregate TiKV cannot run, DISTINCT with
        // `tidb_opt_distinct_agg_push_down` off), and `attach2Task4PhysicalHashAgg`
        // / `...StreamAgg` still convert the cop task to root and attach the
        // whole aggregate: the task is valid, so the first candidate (the
        // cop-single-read one, whose id the plan then shows) wins a cost tie
        // against the root-task candidate.
        let t = Task::Cop(cop).into_root_task_in(allocator, evaluate)?;
        return Ok(attach_plan_to_task(final_plan, t));
    };
    {
        if let Some(table_plan) = cop.table_plan.take() {
            cop.finish_index_plan();
            inherit_index_join_stats(
                &mut partial,
                cop.index_join_info.is_some(),
                Some(&table_plan),
            );
            partial.base_mut().set_children(vec![*table_plan]);
            cop.table_plan = Some(Box::new(partial));
            // Go: the pushed agg's schema replaces the extra projection a
            // double read would otherwise re-add above the reader.
            cop.need_extra_proj = false;
        } else if let Some(index_plan) = cop.index_plan.take() {
            inherit_index_join_stats(
                &mut partial,
                cop.index_join_info.is_some(),
                Some(&index_plan),
            );
            partial.base_mut().set_children(vec![*index_plan]);
            cop.index_plan = Some(Box::new(partial));
        } else {
            return Err(PlanError::internal(
                "an aggregate cop push over a task with neither half",
            ));
        }
    }
    let t = Task::Cop(cop).into_root_task_in(allocator, evaluate)?;
    Ok(attach_plan_to_task(final_plan, t))
}

/// Go `completePhysicalIndexJoin`: consume the inner task's access feedback,
/// retain only lookup-capable equalities as join keys, and move every unused
/// equality into the residual condition list.
fn complete_physical_index_join(
    join: &mut tidb_planner_core_operator_physicalop::PhysicalIndexJoin,
    info: IndexJoinInfo,
    inner_plan: &PhysicalPlan,
    outer_plan: &PhysicalPlan,
) -> Result<(), PlanError> {
    if info.key_off2_idx_off.len() != join.inner_join_keys.len()
        || join.inner_join_keys.len() != join.outer_join_keys.len()
        || join.inner_join_keys.len() != join.is_null_eq.len()
    {
        return Err(PlanError::internal(
            "completePhysicalIndexJoin received misaligned lookup-key feedback",
        ));
    }
    let mut new_inner_keys = Vec::with_capacity(join.inner_join_keys.len());
    let mut new_outer_keys = Vec::with_capacity(join.outer_join_keys.len());
    let mut new_is_null_eq = Vec::with_capacity(join.is_null_eq.len());
    let mut new_key_off = Vec::with_capacity(info.key_off2_idx_off.len());
    let mut other_conditions = std::mem::take(&mut join.other_conditions);
    for (key_off, idx_off) in info.key_off2_idx_off.iter().copied().enumerate() {
        if idx_off < 0 {
            let equality = join.equal_conditions.get(key_off).cloned().ok_or_else(|| {
                PlanError::internal("completePhysicalIndexJoin cannot restore an unused equality")
            })?;
            other_conditions.push(tidb_expr::expression::Expression::ScalarFunction(equality));
            continue;
        }
        new_inner_keys.push(join.inner_join_keys[key_off].clone());
        new_outer_keys.push(join.outer_join_keys[key_off].clone());
        new_is_null_eq.push(join.is_null_eq[key_off]);
        new_key_off.push(idx_off);
    }

    let mut outer_hash_keys = new_outer_keys.clone();
    let mut inner_hash_keys = new_inner_keys.clone();
    // Go's `completePhysicalIndexJoin` runs this extraction with
    // `extractOtherEQ` hardcoded `true` at BOTH of its call sites that reach
    // it -- `indexJoinAttach2Task` and `indexHashJoinAttach2Task`
    // (`task.go:158,178`) -- so a plain `IndexJoin` batches its outer rows
    // into a hash map for lookup dedup exactly as `IndexHashJoin` does, and
    // reads `OuterHashKeys`/`InnerHashKeys` too. `attach2Task4PhysicalIndexMergeJoin`
    // (`task.go:141`) is the one index-join attach path that never calls
    // `completePhysicalIndexJoin` at all -- its inner plan is already fixed
    // before `Attach2Task` runs, and the merge executor has no hash table to
    // key -- so `IndexMergeJoin` alone keeps skipping this extraction.
    if join.kind != tidb_planner_core_operator_physicalop::IndexJoinKind::IndexMergeJoin {
        let outer_schema = outer_plan.schema().ok_or_else(|| {
            PlanError::internal("completePhysicalIndexJoin outer child has no schema")
        })?;
        let inner_schema = inner_plan.schema().ok_or_else(|| {
            PlanError::internal("completePhysicalIndexJoin inner child has no schema")
        })?;
        for index in (0..other_conditions.len()).rev() {
            let tidb_expr::expression::Expression::ScalarFunction(function) =
                &other_conditions[index]
            else {
                continue;
            };
            if function.func_name.lowercase() != "eq" {
                continue;
            }
            let Some((left, right)) = tidb_expr::expr_util::is_col_op_col(function) else {
                continue;
            };
            if left.in_operand || right.in_operand {
                continue;
            }
            if outer_schema.contains(left) && inner_schema.contains(right) {
                outer_hash_keys.push(left.clone());
                inner_hash_keys.push(right.clone());
            } else if inner_schema.contains(left) && outer_schema.contains(right) {
                outer_hash_keys.push(right.clone());
                inner_hash_keys.push(left.clone());
            }
            other_conditions.remove(index);
        }
    }

    join.inner_range_bare = info.range_bare;
    join.inner_access_table_id = Some(info.table_id);
    join.inner_access_index_id = info.index_id;
    join.inner_access_conditions = info.access_conditions;
    join.ranges = info.ranges;
    join.idx_col_lens = info.idx_col_lens;
    join.compare_filters = info.compare_filters;
    join.key_off2_idx_off = new_key_off;
    join.inner_join_keys = new_inner_keys;
    join.outer_join_keys = new_outer_keys;
    join.is_null_eq = new_is_null_eq;
    join.other_conditions = other_conditions;
    join.outer_hash_keys = outer_hash_keys;
    join.inner_hash_keys = inner_hash_keys;
    join.equal_conditions.clear();
    // Go `mutableIndexJoinRange` when the path has a static template;
    // otherwise the join's ranges are immutable for the plan cache.
    join.range_rebuild = info.range_rebuild;
    let (left, right) = if join.inner_child_idx == 0 {
        (&join.inner_join_keys, &join.outer_join_keys)
    } else {
        (&join.outer_join_keys, &join.inner_join_keys)
    };
    join.left_join_keys.clone_from(left);
    join.right_join_keys.clone_from(right);
    Ok(())
}

/// Go `containVirtualColumn` (`core/task.go:1125-1142`): a pushdown
/// expression may refer to a generated column only when the MPP child schema
/// does not identify that column as virtual.
fn contains_virtual_column_in_plan(
    expressions: &[tidb_expr::expression::Expression],
    plan: &PhysicalPlan,
) -> bool {
    let Some(schema) = plan.schema() else {
        return false;
    };
    let virtual_ids: std::collections::HashSet<i64> = schema
        .columns
        .iter()
        .filter(|column| column.unique_id > 0 && column.virtual_expr.is_some())
        .map(|column| column.unique_id)
        .collect();
    tidb_expr::simple_expr::extract_columns_from_expressions(expressions, None)
        .iter()
        .any(|column| virtual_ids.contains(&column.unique_id))
}

/// Go `HeavyFunctionNameMap` (`core/task.go:46-57`). These expressions are
/// expensive enough that evaluating them once in a pushed-down projection is
/// preferable to evaluating them again in the global TopN.
const HEAVY_TOPN_FUNCTIONS: &[&str] = &[
    "vec_cosine_distance",
    "vec_l1_distance",
    "vec_l2_distance",
    "vec_negative_inner_product",
    "vec_dims",
    "vec_l2_norm",
    "fts_match_word",
];

/// Go `ContainHeavyFunction` (`core/task.go:1083-1092`).
fn contains_heavy_topn_function(expression: &Expression) -> bool {
    let Expression::ScalarFunction(function) = expression else {
        return false;
    };
    if HEAVY_TOPN_FUNCTIONS.contains(&function.func_name.lowercase()) {
        return true;
    }
    function.args.iter().any(contains_heavy_topn_function)
}

/// The plans returned by Go `getPushedDownTopN` when at least one by-item is a
/// heavy function: a bottom projection materializes each heavy expression,
/// a pushed TopN orders by those fresh columns, and a global TopN reuses the
/// same columns after the task returns to TiDB.
fn heavy_topn_split(
    topn: &tidb_planner_core_operator_physicalop::PhysicalTopN,
    child_plan: &PhysicalPlan,
    store_type: StoreType,
    column_ids: Option<&tidb_planner_core_base::ColumnIdAllocator>,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
) -> Option<(
    tidb_planner_core_operator_physicalop::PhysicalProjection,
    tidb_planner_core_operator_physicalop::PhysicalTopN,
    tidb_planner_core_operator_physicalop::PhysicalTopN,
)> {
    if !topn.heavy_function_optimize
        || (store_type == StoreType::TiKv && !topn.allow_projection_push_down)
    {
        return None;
    }
    let heavy_indexes: Vec<usize> = topn
        .by_items
        .iter()
        .enumerate()
        .filter_map(|(index, item)| contains_heavy_topn_function(&item.expr).then_some(index))
        .collect();
    if heavy_indexes.is_empty() {
        return None;
    }
    let column_ids = column_ids?;
    let output_schema = topn.base.base.schema()?.clone();
    let mut projection_exprs = output_schema
        .columns
        .iter()
        .cloned()
        .map(Expression::Column)
        .collect::<Vec<_>>();
    let mut projection_columns = output_schema.columns.clone();
    let mut distance_columns = Vec::with_capacity(heavy_indexes.len());
    for index in heavy_indexes.iter().copied() {
        let expression = topn.by_items[index].expr.clone();
        let ret_type = expression.static_type()?.clone();
        projection_exprs.push(expression);
        let mut distance = tidb_expr::column::Column::new(column_ids.alloc(), ret_type);
        distance.index = (projection_exprs.len() - 1) as i64;
        projection_columns.push(distance.clone());
        distance_columns.push((index, distance));
    }

    let child_stats = child_plan.stats_info().cloned();
    let new_count = topn.offset.wrapping_add(topn.count);
    let pushed_stats = child_stats.map(|stats| stats.derive_limit_stats(new_count as f64));
    let projection_schema = tidb_expr::schema::Schema::new(projection_columns);
    let mut projection_base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
        allocator,
        "Projection",
        topn.base.base.query_block_offset(),
    );
    projection_base.base.set_stats(pushed_stats.clone());
    projection_base
        .base
        .set_schema(Some(projection_schema.clone()));
    let projection = tidb_planner_core_operator_physicalop::PhysicalProjection {
        base: projection_base,
        exprs: projection_exprs,
        ..Default::default()
    };

    let mut pushed = topn.clone();
    pushed.base.set_children(Vec::new());
    pushed.base.base.set_stats(pushed_stats);
    pushed.base.base.set_schema(Some(projection_schema));
    pushed.offset = 0;
    pushed.count = new_count;
    for (index, distance) in &distance_columns {
        pushed.by_items[*index].expr = Expression::Column(distance.clone());
    }

    let mut global = topn.clone();
    global.base.set_children(Vec::new());
    for (index, distance) in distance_columns {
        global.by_items[index].expr = Expression::Column(distance);
    }
    Some((projection, pushed, global))
}

/// Attaches a physical plan with Go's default group-NDV skew ratio.
pub fn attach2_task(
    plan: PhysicalPlan,
    tasks: Vec<Task>,
    column_ids: Option<&tidb_planner_core_base::ColumnIdAllocator>,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
) -> Result<Task, PlanError> {
    attach2_task_with_group_ndv_skew_ratio(
        plan,
        tasks,
        column_ids,
        allocator,
        tidb_vardef::defaults::DEF_OPT_RISK_GROUP_NDV_SKEW_RATIO,
    )
}

/// Attaches a physical plan using the statement's group-NDV estimation ratio.
pub fn attach2_task_with_group_ndv_skew_ratio(
    plan: PhysicalPlan,
    tasks: Vec<Task>,
    column_ids: Option<&tidb_planner_core_base::ColumnIdAllocator>,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
    group_ndv_skew_ratio: f64,
) -> Result<Task, PlanError> {
    attach2_task_in(
        plan,
        tasks,
        column_ids,
        allocator,
        group_ndv_skew_ratio,
        &tidb_ranger::points::evaluate_static,
        &Default::default(),
    )
}

/// Attaches a candidate without losing the statement evaluation context at conversion.
pub(crate) fn attach2_task_in(
    plan: PhysicalPlan,
    mut tasks: Vec<Task>,
    column_ids: Option<&tidb_planner_core_base::ColumnIdAllocator>,
    allocator: &tidb_planner_core_base::PlanIdAllocator,
    group_ndv_skew_ratio: f64,
    evaluate: &tidb_ranger::points::ExpressionEvaluator<'_>,
    blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
) -> Result<Task, PlanError> {
    // The caller hands over owned candidate tasks. Go copies task headers
    // while sharing plan pointers; moving these tasks avoids cloning Rust's
    // owned plan trees again. A caller retaining a candidate uses Task::copy.
    let mut first = tasks
        .drain(..1)
        .next()
        .ok_or_else(|| PlanError::internal("attach2_task with no child task"))?;
    match &plan {
        // `attach2Task4PhysicalSort` (`task.go:843`): copy, attach. No
        // conversion — findBestTask only asks a Sort under a root property.
        PhysicalPlan::Sort(_) => Ok(attach_plan_to_task(plan, first)),
        // `attach2Task4PhysicalSelection` (`task.go:1598`): Go has NO cop
        // push at attach — a cop child CONVERTS and the selection lands at
        // root (pushed filters ride the DataSource's PushedDownConds
        // instead). The MPP arm pushes when TiFlash's
        // `CanExprsPushDown` admits every condition, and converts otherwise.
        PhysicalPlan::Selection(_) => match &first {
            Task::Root(_) | Task::Cop(_) => {
                let converted = first.into_root_task_in(allocator, evaluate)?;
                Ok(attach_plan_to_task(plan, converted))
            }
            Task::Mpp(_) => {
                let PhysicalPlan::Selection(selection) = &plan else {
                    unreachable!("the arm matched Selection");
                };
                if tidb_expr::infer_pushdown::can_exprs_push_down(&selection.conditions, tidb_kv::store_type::StoreType::TiFlash, blacklist) {
                    Ok(attach_plan_to_task(plan, first))
                } else {
                    let converted = first.into_root_task_in(allocator, evaluate)?;
                    Ok(attach_plan_to_task(plan, converted))
                }
            }
        },
        // `attach2Task4PhysicalUnionScan` (`task.go:82`): a union scan always
        // wants a root child; a projection the child carries is pulled above
        // it, because the executor cannot build one underneath.
        PhysicalPlan::UnionScan(_) => {
            let converted = first.into_root_task_in(allocator, evaluate)?;
            Ok(attach_union_scan(plan, converted))
        }
        // `attach2Task4PhysicalWindow` (`task.go:2230`): convert the child to
        // a root task and attach. The TiFlash MPP arm is absent with that
        // tier.
        PhysicalPlan::Window(_) | PhysicalPlan::Shuffle(_) | PhysicalPlan::ShuffleReceiver(_) => {
            let converted = first.copy().convert_to_root_task_in(allocator, evaluate)?;
            Ok(attach_plan_to_task(plan, converted))
        }
        // Go attach2Task4PhysicalExpand retains an MPP child on TiFlash;
        // all other children finish their remote reads before root expansion.
        PhysicalPlan::Expand(_) => {
            let task = match first.copy() {
                mpp @ Task::Mpp(_) => mpp,
                other => other.convert_to_root_task_in(allocator, evaluate)?,
            };
            Ok(attach_plan_to_task(plan, task))
        }
        // `attach2Task4PhysicalProjection` (`task.go:1506`): the cop arm
        // pushes the projection onto the cop task — staying a COP task —
        // when there are no root conds, no index-merge parts, and every
        // expr passes the TiKV gate (`tidb_planner_core_operator_physicalop::pushdown`); an unfinished
        // index half finishes first (the conservative arm of
        // `canPushToIndexPlan`'s column check). Otherwise, and for a root
        // child, convert-then-attach. The MPP arm uses the TiFlash admission
        // gate and converts when a projection cannot be pushed.
        PhysicalPlan::Projection(_) => match &first {
            Task::Root(_) => {
                let converted = first.into_root_task_in(allocator, evaluate)?;
                Ok(attach_plan_to_task(plan, converted))
            }
            Task::Cop(cop_ref) => {
                let PhysicalPlan::Projection(projection) = &plan else {
                    unreachable!("the arm matched Projection");
                };
                let pushable = cop_ref.root_task_conds.is_empty()
                    && cop_ref.idx_merge_part_plans.is_empty()
                    && tidb_expr::infer_pushdown::can_exprs_push_down(&projection.exprs, tidb_kv::store_type::StoreType::TiKv, blacklist);
                if pushable {
                    let Task::Cop(mut cop) = first else {
                        unreachable!("the arm matched Cop");
                    };
                    let columns = tidb_expr::simple_expr::extract_columns_from_expressions(
                        &projection.exprs,
                        None,
                    );
                    if !cop.index_plan_finished
                        && !can_push_to_index_plan(cop.index_plan.as_deref(), &columns)
                    {
                        cop.finish_index_plan();
                    }
                    Ok(attach_plan_to_task(plan, Task::Cop(cop)))
                } else {
                    let converted = first.into_root_task_in(allocator, evaluate)?;
                    Ok(attach_plan_to_task(plan, converted))
                }
            }
            Task::Mpp(_) => {
                let PhysicalPlan::Projection(projection) = &plan else {
                    unreachable!("the arm matched Projection");
                };
                if tidb_expr::infer_pushdown::can_exprs_push_down(&projection.exprs, tidb_kv::store_type::StoreType::TiFlash, blacklist) {
                    Ok(attach_plan_to_task(plan, first))
                } else {
                    let converted = first.into_root_task_in(allocator, evaluate)?;
                    Ok(attach_plan_to_task(plan, converted))
                }
            }
        },
        // `attach2Task4PhysicalLimit` (`task.go:619`): the SINGLE-READ cop
        // branch pushes a partial limit — `Count = Offset + Count`, offset
        // removed, `DeriveLimitStats` over the open half's profile, the
        // child's schema shared — onto the cop task, converts, and attaches
        // the ROOT limit above (unless a partition-by skips it: "a derived
        // topN and window function will take care of the filter").
        // `sinkIntoIndexLookUp` and the index-merge/MPP arms narrow with
        // their readers, named here.
        PhysicalPlan::Limit(_) => {
            let PhysicalPlan::Limit(limit) = &plan else {
                unreachable!("the arm matched Limit");
            };
            let t = match first {
                Task::Root(_) => first.into_root_task_in(allocator, evaluate)?,
                Task::Cop(cop) if !cop.idx_merge_part_plans.is_empty() => {
                    return attach_limit_to_index_merge(plan, cop, allocator, evaluate);
                }
                Task::Cop(_) => {
                    let Task::Cop(mut cop) = first else {
                        unreachable!("the arm matched Cop");
                    };
                    let pushable =
                        (!cop.keep_order || !cop.index_plan_finished || cop.index_plan.is_none())
                            && cop.root_task_conds.is_empty();
                    if pushable {
                        let new_count = limit.offset.wrapping_add(limit.count);
                        let stats = cop
                            .plan()
                            .and_then(PhysicalPlan::stats_info)
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                            allocator,
                            "Limit",
                            plan.query_block_offset(),
                        );
                        base.base.set_stats(stats);
                        // "Don't use clone() so that Limit and its children
                        // share the same schema": the pushed limit reports
                        // the open half's schema.
                        base.base
                            .set_schema(cop.plan().and_then(PhysicalPlan::schema).cloned());
                        let pushed = PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
                            base,
                            partition_by: limit.partition_by.clone(),
                            offset: 0,
                            count: new_count,
                            prefix_col: None,
                            prefix_len: 0,
                        });
                        let Task::Cop(pushed_cop) = attach_plan_to_task(pushed, Task::Cop(cop))
                        else {
                            unreachable!("attaching onto a cop task answers a cop task");
                        };
                        cop = pushed_cop;
                    }
                    let mut t = Task::Cop(cop).into_root_task_in(allocator, evaluate)?;
                    // `sunk = sinkIntoIndexLookUp(p, t)`: a converted double
                    // read absorbs the limit itself.
                    if sink_into_index_look_up(limit, &mut t, allocator) {
                        return Ok(t);
                    }
                    t
                }
                Task::Mpp(mpp) => {
                    let new_count = limit.offset.wrapping_add(limit.count);
                    let stats = mpp
                        .plan
                        .as_deref()
                        .and_then(PhysicalPlan::stats_info)
                        .map(|profile| profile.derive_limit_stats(new_count as f64));
                    let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                        allocator,
                        "Limit",
                        plan.query_block_offset(),
                    );
                    base.base.set_stats(stats);
                    base.base
                        .set_schema(mpp.plan.as_deref().and_then(PhysicalPlan::schema).cloned());
                    let pushed = PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
                        base,
                        partition_by: limit.partition_by.clone(),
                        offset: 0,
                        count: new_count,
                        prefix_col: None,
                        prefix_len: 0,
                    });
                    let mut pushed_task = attach_plan_to_task(pushed, Task::Mpp(mpp));
                    // Go sets the pushed Limit schema from its child after
                    // attachment so generated columns retain the exact child
                    // schema object.
                    if let Some(PhysicalPlan::Limit(pushed_limit)) = pushed_task.plan_mut() {
                        let child_schema = pushed_limit
                            .base
                            .children()
                            .first()
                            .and_then(PhysicalPlan::schema)
                            .cloned();
                        pushed_limit.base.base.set_schema(child_schema);
                    }
                    pushed_task.into_root_task_in(allocator, evaluate)?
                }
            };
            // "Skip limit with partition on the root."
            if !limit.partition_by.is_empty() {
                return Ok(t);
            }
            Ok(attach_plan_to_task(plan, t))
        }
        // `PhysicalMaxOneRow` has no override: `BasePhysicalPlan.Attach2Task`
        // (`base_physical_plan.go:202`) is convert-to-root then attach, on
        // ANY task kind — a cop/MPP child propagates
        // `convert_to_root_task`'s reader-building refusal.
        PhysicalPlan::MaxOneRow(_) => {
            let converted = first.into_root_task_in(allocator, evaluate)?;
            Ok(attach_plan_to_task(plan, converted))
        }
        // An ExchangeSender is born at root conversion (and at MPP fragment
        // generation), never as an attach parent.
        PhysicalPlan::ExchangeSender(_) => Err(PlanError::internal(
            "attach2_task: an ExchangeSender is never an attach parent",
        )),
        PhysicalPlan::ExchangeReceiver(_) => Ok(attach_plan_to_task(plan, first)),
        // `PhysicalLock` has no override: the default convert-then-attach
        // body, exactly as `PhysicalMaxOneRow`'s arm above.
        PhysicalPlan::Lock(_) => {
            let converted = first.into_root_task_in(allocator, evaluate)?;
            Ok(attach_plan_to_task(plan, converted))
        }
        // `attach2Task4PhysicalUnionAll` (`task.go:1573`): convert EVERY
        // child task to root and wire the multi-child plan into a fresh
        // RootTask. Go's MPP arm: a PartitionUnion over any MPP child is the
        // invalid task outright ("PartitionUnion cannot pushdown to
        // tiflash"); a plain union over MPP children needs
        // `attach2MppTasks4PhysicalUnionAll`, which creates one MPP task over
        // the child fragment plans.
        PhysicalPlan::UnionAll(_) => {
            let mut tasks = {
                let mut all = vec![first];
                all.append(&mut tasks);
                all
            };
            if tasks.iter().any(|task| matches!(task, Task::Mpp(_))) {
                if plan.base().base.tp() == "PartitionUnion" {
                    return Ok(Task::invalid_task());
                }
                let stats_context = tasks
                    .iter()
                    .find_map(|task| match task {
                        Task::Mpp(mpp) => Some(mpp.stats_context.clone()),
                        _ => None,
                    })
                    .expect("the MPP branch contains an MPP task");
                let mut child_plans = Vec::with_capacity(tasks.len());
                for task in tasks {
                    let Task::Mpp(mut mpp) = task else {
                        return Ok(Task::invalid_task());
                    };
                    let Some(child) = mpp.plan.take() else {
                        return Ok(Task::invalid_task());
                    };
                    child_plans.push(*child);
                }
                if child_plans.is_empty() {
                    return Ok(Task::invalid_task());
                }
                let mut mpp_plan = plan;
                mpp_plan.base_mut().set_children(child_plans);
                return Ok(Task::Mpp(
                    MppTask::new(
                        mpp_plan,
                        tidb_planner_property::MppPartitionType::Any,
                        [],
                    )
                    .with_stats_context(stats_context),
                ));
            }
            let mut plan = plan;
            let mut children = Vec::with_capacity(tasks.len());
            for task in tasks.drain(..) {
                let Task::Root(mut converted) = task.into_root_task_in(allocator, evaluate)? else {
                    return Err(PlanError::internal(
                        "convert_to_root_task answered a non-root task",
                    ));
                };
                let Some(child) = converted.take_plan() else {
                    return Err(PlanError::internal(
                        "attach2Task4PhysicalUnionAll: a child task has no plan",
                    ));
                };
                children.push(child);
            }
            plan.base_mut().set_children(children);
            let mut root = RootTask::default();
            root.set_plan(plan);
            Ok(Task::Root(root))
        }
        // `attach2Task4PhysicalApply` (`task.go:127`): convert both children
        // to root, wire them in, build the join schema
        // (`BuildPhysicalJoinSchema`), and inherit BOTH children's warnings.
        PhysicalPlan::Apply(_) => {
            let second = tasks
                .drain(..1)
                .next()
                .ok_or_else(|| PlanError::internal("attach2Task4PhysicalApply needs two tasks"))?;
            let Task::Root(mut left) = first.into_root_task_in(allocator, evaluate)? else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let Task::Root(mut right) = second.into_root_task_in(allocator, evaluate)? else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let (Some(left_plan), Some(right_plan)) = (left.take_plan(), right.take_plan()) else {
                return Err(PlanError::internal(
                    "attach2Task4PhysicalApply: a child task has no plan",
                ));
            };
            let mut plan = plan;
            plan.base_mut().set_children(vec![left_plan, right_plan]);
            let join_type = match &plan {
                PhysicalPlan::Apply(apply) => apply.hash_join.join_type,
                _ => unreachable!("the arm matched Apply"),
            };
            let schema = tidb_planner_core_operator_physicalop::build_physical_join_schema(join_type, &plan);
            plan.base_mut().base.set_schema(schema);
            let mut root = RootTask::default();
            root.set_plan(plan);
            root.warnings.copy_from([&left.warnings, &right.warnings]);
            Ok(Task::Root(root))
        }

        // `attach2Task4NominalSort` (`task.go:851`): an only-column nominal
        // sort returns the child task ITSELF — not even a copy — and
        // otherwise it is copy-then-attach with no conversion, like Sort.
        PhysicalPlan::NominalSort(op) => {
            if op.only_column {
                return Ok(first);
            }
            Ok(attach_plan_to_task(plan, first))
        }
        // `attach2Task4PhysicalSequence` (`task.go:2259`): when ANY child
        // task is not MPP, the sequence VANISHES — the last child's task is
        // returned unchanged and every producer's task is discarded. That is
        // Go's own body ("if !isMpp { return tasks[len(tasks)-1] }"), a
        // quirk reproduced rather than fixed: on a root-tier plan the CTE
        // producers are wired elsewhere, not through this attach. The
        // all-MPP arm builds an MppTask over the last child's partition type
        // and carries its hash-column contract into the composed fragment.
        PhysicalPlan::Sequence(_) => {
            let mut all = vec![first];
            all.append(&mut tasks);
            if all.iter().any(|task| !matches!(task, Task::Mpp(_))) {
                return Ok(all.pop().expect("the vec was built with at least one task"));
            }
            let Some(Task::Mpp(last)) = all.last() else {
                unreachable!("the all-MPP branch proved every task is MPP");
            };
            let partition_type = last.partition_type();
            let hash_cols = last.hash_cols.clone();
            let stats_context = last.stats_context.clone();
            let mut child_plans = Vec::with_capacity(all.len());
            let mut warnings = Vec::with_capacity(all.len());
            for task in all {
                let Task::Mpp(mut mpp) = task else {
                    unreachable!("the all-MPP branch proved every task is MPP");
                };
                let Some(child) = mpp.plan.take() else {
                    return Ok(Task::invalid_task());
                };
                child_plans.push(*child);
                warnings.push(mpp.warnings);
            }
            let mut sequence = plan;
            sequence.base_mut().set_children(child_plans);
            Ok(Task::Mpp(
                MppTask::new_with_hash_cols(sequence, partition_type, hash_cols, warnings)
                    .with_stats_context(stats_context),
            ))
        }
        // `attach2Task4PhysicalTopN` (`task.go:1249`), the SIMPLE path:
        // when the by-items carry columns, pass the TiKV gate, and the cop
        // task has no root conds, push a partial TopN — ByItems cloned,
        // `Count = Offset + Count`, offset removed, `DeriveLimitStats` over
        // the open half (`getPushedDownTopN`'s non-heavy half) — then
        // convert and attach the ROOT TopN above (partition-by root skip).
        // The partial-order path below retains the root even for partitioned
        // TopN, as Go handlePartialOrderTopN requires.
        PhysicalPlan::TopN(_) => {
            let PhysicalPlan::TopN(topn) = &plan else {
                unreachable!("the arm matched TopN");
            };
            // Go `handlePartialOrderTopN`: a prefix-index match is carried
            // by the CopTask so the root TopN can retain the partial-order
            // metadata while a special Limit short-circuits the index scan.
            if let Task::Cop(mut cop) = first {
                if let Some(match_result) = cop
                    .partial_order_match_result
                    .take()
                    .filter(|result| result.matched)
                {
                    let Some(prefix_col) = match_result.prefix_col.as_ref() else {
                        return Ok(Task::invalid_task());
                    };
                    let Some(topn_schema) = topn.base.base.schema() else {
                        return Ok(Task::invalid_task());
                    };
                    let Some(prefix_col) = topn_schema
                        .columns
                        .iter()
                        .find(|column| column.unique_id == prefix_col.unique_id)
                        .cloned()
                    else {
                        return Ok(Task::invalid_task());
                    };
                    let mut partial_topn = topn.clone();
                    partial_topn.prefix_col = Some(prefix_col.unique_id);
                    partial_topn.prefix_len = match_result.prefix_len;

                    let can_push_limit = cop.idx_merge_part_plans.is_empty()
                        && !cop.index_plan_finished
                        && cop.root_task_conds.is_empty()
                        && cop.index_plan.is_some();
                    if can_push_limit {
                        let new_count = partial_topn.offset.wrapping_add(partial_topn.count);
                        let child = cop
                            .index_plan
                            .take()
                            .expect("partial-order CopTask has an index plan");
                        let stats = child
                            .stats_info()
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                            allocator,
                            "Limit",
                            plan.query_block_offset(),
                        );
                        base.base.set_stats(stats);
                        base.base.set_schema(child.schema().cloned());
                        base.set_children(vec![*child]);
                        cop.index_plan = Some(Box::new(PhysicalPlan::Limit(
                            tidb_planner_core_operator_physicalop::PhysicalLimit {
                                base,
                                partition_by: Vec::new(),
                                offset: 0,
                                count: new_count,
                                prefix_col: Some(prefix_col.unique_id),
                                prefix_len: match_result.prefix_len,
                            },
                        )));
                    }

                    let converted = Task::Cop(cop).into_root_task_in(allocator, evaluate)?;
                    return Ok(attach_plan_to_task(
                        PhysicalPlan::TopN(partial_topn),
                        converted,
                    ));
                }
                first = Task::Cop(cop);
            }
            let t = match first {
                Task::Root(_) => first.into_root_task_in(allocator, evaluate)?,
                Task::Cop(_) => {
                    let Task::Cop(mut cop) = first else {
                        unreachable!("the arm matched Cop");
                    };
                    let by_exprs: Vec<tidb_expr::expression::Expression> =
                        topn.by_items.iter().map(|item| item.expr.clone()).collect();
                    let columns =
                        tidb_expr::simple_expr::extract_columns_from_expressions(&by_exprs, None);
                    let need_push_down = !columns.is_empty();
                    if need_push_down
                        && cop.get_store_type() == StoreType::TiDb
                        && cop.root_task_conds.is_empty()
                    {
                        match push_limit_down_to_tidb_cop(topn, cop, allocator, evaluate)? {
                            Ok(task) => return Ok(task),
                            Err(untouched) => cop = untouched,
                        }
                    }
                    let pushable = need_push_down
                        && tidb_expr::infer_pushdown::can_exprs_push_down(&by_exprs, tidb_kv::store_type::StoreType::TiKv, blacklist)
                        && cop.root_task_conds.is_empty();
                    if pushable {
                        if !cop.index_plan_finished && !cop.idx_merge_is_intersection {
                            if cop.idx_merge_order.as_ref().is_some_and(|order| {
                                order.advisory
                                    && !order.partial_matches.iter().all(|result| result.matched())
                            }) {
                                return attach_advisory_topn_to_index_merge(topn, cop, allocator, evaluate);
                            }
                        }
                        if !cop.index_plan_finished
                            && !can_push_to_index_plan(cop.index_plan.as_deref(), &columns)
                        {
                            cop.finish_index_plan();
                        }
                        if cop.plan().is_none() {
                            let converted = Task::Cop(cop).convert_to_root_task_in(allocator, evaluate)?;
                            if !topn.partition_by.is_empty() {
                                return Ok(converted);
                            }
                            return Ok(attach_plan_to_task(plan, converted));
                        }
                        if let Some((projection, pushed, global)) = heavy_topn_split(
                            topn,
                            cop.plan().expect("cop plan was checked above"),
                            cop.get_store_type(),
                            column_ids,
                            allocator,
                        ) {
                            let pushed_task = attach_plan_to_task(
                                PhysicalPlan::TopN(pushed),
                                attach_plan_to_task(
                                    PhysicalPlan::Projection(projection),
                                    Task::Cop(cop),
                                ),
                            );
                            if !topn.partition_by.is_empty() {
                                return Ok(pushed_task);
                            }
                            let converted = pushed_task.into_root_task_in(allocator, evaluate)?;
                            return Ok(attach_plan_to_task(
                                PhysicalPlan::TopN(global),
                                converted,
                            ));
                        }
                        let new_count = topn.offset.wrapping_add(topn.count);
                        let stats = cop
                            .plan()
                            .and_then(PhysicalPlan::stats_info)
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                            allocator,
                            "TopN",
                            plan.query_block_offset(),
                        );
                        base.base.set_stats(stats);
                        base.base
                            .set_schema(cop.plan().and_then(PhysicalPlan::schema).cloned());
                        let pushed = PhysicalPlan::TopN(tidb_planner_core_operator_physicalop::PhysicalTopN {
                            base,
                            by_items: topn.by_items.clone(),
                            partition_by: topn.partition_by.clone(),
                            offset: 0,
                            count: new_count,
                            prefix_col: topn.prefix_col,
                            prefix_len: topn.prefix_len,
                            allow_projection_push_down: topn.allow_projection_push_down,
                            heavy_function_optimize: topn.heavy_function_optimize,
                        });
                        let Task::Cop(pushed_cop) = attach_plan_to_task(pushed, Task::Cop(cop))
                        else {
                            unreachable!("attaching onto a cop task answers a cop task");
                        };
                        cop = pushed_cop;
                    }
                    Task::Cop(cop).into_root_task_in(allocator, evaluate)?
                }
                Task::Mpp(mpp) => {
                    let by_exprs: Vec<tidb_expr::expression::Expression> =
                        topn.by_items.iter().map(|item| item.expr.clone()).collect();
                    let columns =
                        tidb_expr::simple_expr::extract_columns_from_expressions(&by_exprs, None);
                    let need_push_down = !columns.is_empty();
                    let pushable = need_push_down
                        && tidb_expr::infer_pushdown::can_exprs_push_down(&by_exprs, tidb_kv::store_type::StoreType::TiFlash, blacklist)
                        && !contains_virtual_column_in_plan(
                            &by_exprs,
                            mpp.plan.as_deref().ok_or_else(|| {
                                PlanError::internal("MPP TopN attachment received an empty task")
                            })?,
                        );
                    if pushable {
                        if let Some((projection, pushed, global)) = heavy_topn_split(
                            topn,
                            mpp.plan.as_deref().ok_or_else(|| {
                                PlanError::internal("MPP TopN attachment received an empty task")
                            })?,
                            StoreType::TiFlash,
                            column_ids,
                            allocator,
                        ) {
                            let pushed_task = attach_plan_to_task(
                                PhysicalPlan::TopN(pushed),
                                attach_plan_to_task(
                                    PhysicalPlan::Projection(projection),
                                    Task::Mpp(mpp),
                                ),
                            );
                            if !topn.partition_by.is_empty() {
                                return Ok(pushed_task);
                            }
                            let converted = pushed_task.into_root_task_in(allocator, evaluate)?;
                            return Ok(attach_plan_to_task(
                                PhysicalPlan::TopN(global),
                                converted,
                            ));
                        }
                        let new_count = topn.offset.wrapping_add(topn.count);
                        let stats = mpp
                            .plan
                            .as_deref()
                            .and_then(PhysicalPlan::stats_info)
                            .map(|profile| profile.derive_limit_stats(new_count as f64));
                        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(
                            allocator,
                            "TopN",
                            plan.query_block_offset(),
                        );
                        base.base.set_stats(stats);
                        base.base.set_schema(
                            mpp.plan.as_deref().and_then(PhysicalPlan::schema).cloned(),
                        );
                        let pushed = PhysicalPlan::TopN(tidb_planner_core_operator_physicalop::PhysicalTopN {
                            base,
                            by_items: topn.by_items.clone(),
                            partition_by: topn.partition_by.clone(),
                            offset: 0,
                            count: new_count,
                            prefix_col: topn.prefix_col,
                            prefix_len: topn.prefix_len,
                            allow_projection_push_down: topn.allow_projection_push_down,
                            heavy_function_optimize: topn.heavy_function_optimize,
                        });
                        let mut pushed_task = attach_plan_to_task(pushed, Task::Mpp(mpp));
                        if let Some(PhysicalPlan::TopN(pushed_topn)) = pushed_task.plan_mut() {
                            let child_schema = pushed_topn
                                .base
                                .children()
                                .first()
                                .and_then(PhysicalPlan::schema)
                                .cloned();
                            pushed_topn.base.base.set_schema(child_schema);
                        }
                        pushed_task.into_root_task_in(allocator, evaluate)?
                    } else {
                        Task::Mpp(mpp).into_root_task_in(allocator, evaluate)?
                    }
                }
            };
            if !topn.partition_by.is_empty() {
                return Ok(t);
            }
            Ok(attach_plan_to_task(plan, t))
        }
        // `attach2Task4PhysicalStreamAgg` (`task.go:1653`): a cop child
        // takes the partial/final split UNLESS the aggregate must not cross
        // the boundary — an order-keeping double read, root-side filters, or
        // an index merge. The TiFlash stream-agg refusal is unreachable on
        // this port's TiKV-only cop tasks.
        PhysicalPlan::StreamAgg(_) => match first {
            Task::Cop(cop) => {
                if (cop.index_plan.is_some() && cop.table_plan.is_some() && cop.keep_order)
                    || !cop.root_task_conds.is_empty()
                    || !cop.idx_merge_part_plans.is_empty()
                {
                    let t = Task::Cop(cop).into_root_task_in(allocator, evaluate)?;
                    Ok(attach_plan_to_task(plan, t))
                } else {
                    attach_agg_over_cop(plan, cop, column_ids, allocator, evaluate, blacklist)
                }
            }
            Task::Mpp(mpp) => {
                let converted = Task::Mpp(mpp).into_root_task_in(allocator, evaluate)?;
                Ok(attach_plan_to_task(plan, converted))
            }
            root @ Task::Root(_) => Ok(attach_plan_to_task(plan, root.into_root_task_in(allocator, evaluate)?)),
        },
        // `attach2Task4PhysicalHashAgg` (`task.go:2162`): same split, gated
        // only on root-side filters and index merge.
        PhysicalPlan::HashAgg(_) => match first {
            Task::Cop(cop) => {
                if cop.root_task_conds.is_empty() && cop.idx_merge_part_plans.is_empty() {
                    attach_agg_over_cop(plan, cop, column_ids, allocator, evaluate, blacklist)
                } else {
                    let t = Task::Cop(cop).into_root_task_in(allocator, evaluate)?;
                    Ok(attach_plan_to_task(plan, t))
                }
            }
            Task::Mpp(mpp) => attach_hash_agg_to_mpp(
                plan,
                mpp,
                column_ids,
                allocator,
                evaluate,
                group_ndv_skew_ratio,
                blacklist,
            ),
            root @ Task::Root(_) => Ok(attach_plan_to_task(plan, root.into_root_task_in(allocator, evaluate)?)),
        },
        // `attach2Task4PhysicalHashJoin` (`task.go:211`): convert BOTH
        // children — Go converts the RIGHT one first — wire them in, and
        // concatenate warnings right-before-left, which is the order SHOW
        // WARNINGS replays them in. `StoreTp == kv.TiFlash` routes to the
        // TiFlash attach in Go; the enum's hash join carries no store type,
        // with no TiFlash tier to route to. HashJoin carries its lookup
        // child's feedback upward; MergeJoin does not.
        PhysicalPlan::HashJoin(_) | PhysicalPlan::MergeJoin(_) => {
            let second = tasks
                .drain(..1)
                .next()
                .ok_or_else(|| PlanError::internal("physical join needs two tasks"))?;
            // Go merge converts left first; hash converts right first.
            let (left, right) = if matches!(plan, PhysicalPlan::MergeJoin(_)) {
                let left = first.into_root_task_in(allocator, evaluate)?;
                (left, second.into_root_task_in(allocator, evaluate)?)
            } else {
                let right = second.into_root_task_in(allocator, evaluate)?;
                (first.into_root_task_in(allocator, evaluate)?, right)
            };
            let Task::Root(mut right) = right else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let Task::Root(mut left) = left else {
                return Err(PlanError::internal(
                    "convert_to_root_task answered a non-root task",
                ));
            };
            let (Some(left_plan), Some(right_plan)) = (left.take_plan(), right.take_plan()) else {
                return Err(PlanError::internal(
                    "physical join: a child task has no plan",
                ));
            };
            let mut plan = plan;
            let propagated_index_join_info = if matches!(plan, PhysicalPlan::HashJoin(_)) {
                right.index_join_info.take().or(left.index_join_info.take())
            } else {
                None
            };
            plan.base_mut().set_children(vec![left_plan, right_plan]);
            let mut root = RootTask::default();
            root.set_plan(plan);
            root.index_join_info = propagated_index_join_info;
            root.warnings.copy_from([&right.warnings, &left.warnings]);
            Ok(Task::Root(root))
        }
        PhysicalPlan::IndexJoin(_) => {
            let second = tasks
                .into_iter()
                .next()
                .ok_or_else(|| PlanError::internal("IndexJoin requires two tasks"))?;
            let PhysicalPlan::IndexJoin(mut join) = plan else {
                unreachable!()
            };
            let (outer, inner) = match join.inner_child_idx {
                0 => (second, first),
                1 => (first, second),
                _ => return Err(PlanError::internal("invalid IndexJoin inner child index")),
            };
            // Go converts outer first and replays its warnings first.
            let Task::Root(mut outer) = outer.into_root_task_in(allocator, evaluate)? else {
                unreachable!()
            };
            let Task::Root(mut inner) = inner.into_root_task_in(allocator, evaluate)? else {
                unreachable!()
            };
            let info = inner.index_join_info.take().ok_or_else(|| {
                PlanError::internal("IndexJoin inner task has no access-path feedback")
            })?;
            complete_physical_index_join(&mut join, info, inner.get_plan(), outer.get_plan())?;
            let outer_plan = outer.take_plan().expect("read above");
            let inner_plan = inner.take_plan().expect("read above");
            join.base.set_children(if join.inner_child_idx == 1 {
                vec![outer_plan, inner_plan]
            } else {
                vec![inner_plan, outer_plan]
            });
            let mut root = RootTask::default();
            root.set_plan(PhysicalPlan::IndexJoin(join));
            root.warnings.copy_from([&outer.warnings, &inner.warnings]);
            Ok(Task::Root(root))
        }
        PhysicalPlan::TableScan(_) => Err(PlanError::internal(
            "a PhysicalTableScan is born inside a cop task by findBestTask, \
             never attached (convertToTableScan, find_best_task.go)",
        )),
        PhysicalPlan::TableSample(_) => Err(PlanError::internal(
            "a PhysicalTableSample is born inside its own root task by \
             convertToSampleTable (find_best_task.go), never attached",
        )),
        PhysicalPlan::IndexLookUpReader(_) => Err(PlanError::internal(
            "a PhysicalIndexLookUpReader is born by BuildIndexLookUpTask at \
             cop-to-root conversion, never attached",
        )),
        PhysicalPlan::TableDual(_) => Err(PlanError::internal(
            "a PhysicalTableDual is born inside its own root task by \
             findBestTask (logical_table_dual.go), never attached",
        )),
        PhysicalPlan::TableReader(_) => Err(PlanError::internal(
            "a PhysicalTableReader is born by convertToRootTaskImpl \
             (task_base.go:571), never attached",
        )),
        PhysicalPlan::IndexScan(_) => Err(PlanError::internal(
            "a PhysicalIndexScan is born inside a cop task by findBestTask \
             (convertToIndexScan), never attached",
        )),
        PhysicalPlan::IndexReader(_) => Err(PlanError::internal(
            "a PhysicalIndexReader is born by convertToRootTaskImpl \
             (task_base.go:563), never attached",
        )),
        PhysicalPlan::PointGet(_) | PhysicalPlan::BatchPointGet(_) => Err(PlanError::internal(
            "PointGet and BatchPointGet plans are complete root plans, never attached",
        )),
        PhysicalPlan::IndexMergeReader(_) => Err(PlanError::internal(
            "a PhysicalIndexMergeReader is born by index-merge task conversion, never attached",
        )),
        PhysicalPlan::LocalIndexLookUp(_) => Err(PlanError::internal(
            "a PhysicalLocalIndexLookUp is born inside an IndexLookUpReader, never attached",
        )),
        PhysicalPlan::Dml(_) => Err(PlanError::internal(
            "a physical DML root owns its select plan and is never attached as a relational child",
        )),
        PhysicalPlan::CTE(_) => Err(PlanError::internal(
            "a PhysicalCTE is born inside its own root task by \
             findBestTask4LogicalCTE (physical_cte.go), never attached",
        )),
        PhysicalPlan::CTETable(_) => Err(PlanError::internal(
            "a PhysicalCTETable is born inside its own root task by \
             findBestTask4LogicalCTETable (physical_cte_table.go), never \
             attached",
        )),
        PhysicalPlan::MemTable(_) => Err(PlanError::internal(
            "a PhysicalMemTable is born inside its own root task by \
             findBestTask4LogicalMemTable (physical_mem_table.go), never attached",
        )),
        PhysicalPlan::Show(_) | PhysicalPlan::ShowDDLJobs(_) => Err(PlanError::internal(
            "a PhysicalShow/PhysicalShowDDLJobs is born inside its own root \
             task by findBestTask4LogicalShow{,DDLJobs} (physical_show.go), \
             never attached",
        )),
    }
}

#[cfg(test)]
mod attach_tests {
    use super::*;
    use tidb_planner_core_operator_physicalop::{BasePhysicalPlan, PhysicalPlan, PhysicalSelection, PhysicalSort};
    use tidb_planner_core_base::PlanIdAllocator;
    use tidb_planner_property::stats_info::StatsInfo;
    use tidb_expr::schema::Schema;

    // All WRITTEN: Go's Attach2Task coverage is planner-integration bound.

    fn op_with_stats(tp: &str, rows: f64) -> BasePhysicalPlan {
        let allocator = PlanIdAllocator::new();
        let mut base = BasePhysicalPlan::default();
        base.base = tidb_planner_core_base::BasePlan::new(&allocator, tp, 0);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        base
    }

    fn root_task_over(rows: f64) -> Task {
        let mut root = RootTask::default();
        root.set_plan(PhysicalPlan::TableDual(
            tidb_planner_core_operator_physicalop::PhysicalTableDual {
                base: op_with_stats("Dual", rows),
                row_count: 0,
            },
        ));
        Task::Root(root)
    }

    fn mpp_task_over(rows: f64) -> Task {
        Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual {
                base: op_with_stats("Dual", rows),
                row_count: 0,
            }),
            MppPartitionType::Any,
            [],
        ))
    }

    #[test]
    fn mpp_unary_attachment_follows_tiflash_pushdown_and_root_fallback() {
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            ..PhysicalSelection::default()
        });
        let pushed = attach2_task(
            selection,
            vec![mpp_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("an empty Selection is TiFlash-pushable");
        assert!(matches!(pushed, Task::Mpp(_)));
        assert!(matches!(pushed.plan(), Some(PhysicalPlan::Selection(_))));

        let mut unsupported = tidb_expr::scalar_function::ScalarFunction::default();
        unsupported.func_name = tidb_ast::CiString::new("tan");
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            conditions: vec![tidb_expr::expression::Expression::ScalarFunction(
                unsupported,
            )],
            ..PhysicalSelection::default()
        });
        let converted = attach2_task(
            selection,
            vec![mpp_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("an unsupported Selection converts to root");
        assert!(matches!(converted, Task::Root(_)));
        assert!(matches!(converted.plan(), Some(PhysicalPlan::Selection(_))));

        let projection = PhysicalPlan::Projection(tidb_planner_core_operator_physicalop::PhysicalProjection {
            base: op_with_stats("Projection", 8.0),
            ..tidb_planner_core_operator_physicalop::PhysicalProjection::default()
        });
        let projected = attach2_task(
            projection,
            vec![mpp_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("an empty Projection is TiFlash-pushable");
        assert!(matches!(projected, Task::Mpp(_)));
        assert!(matches!(
            projected.plan(),
            Some(PhysicalPlan::Projection(_))
        ));
    }

    #[test]
    fn mpp_limit_and_topn_keep_the_partial_operator_inside_the_reader() {
        let limit = PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
            base: op_with_stats("Limit", 5.0),
            offset: 2,
            count: 3,
            ..tidb_planner_core_operator_physicalop::PhysicalLimit::default()
        });
        let limited = attach2_task(
            limit,
            vec![mpp_task_over(20.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP Limit attaches");
        let Some(PhysicalPlan::Limit(root_limit)) = limited.plan() else {
            panic!("the original Limit remains at root");
        };
        let Some(PhysicalPlan::TableReader(reader)) = root_limit.base.children().first() else {
            panic!("root Limit reads through a TableReader");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
            panic!("the MPP reader owns the pass-through sender");
        };
        let Some(PhysicalPlan::Limit(pushed_limit)) = sender.base.children().first() else {
            panic!("the partial Limit is below the sender");
        };
        assert_eq!(pushed_limit.offset, 0);
        assert_eq!(pushed_limit.count, 5);

        let by_item = tidb_expression_aggregation::ByItems::new(
            tidb_expr::expression::Expression::Column(tidb_expr::column::Column::new(
                1,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            )),
            false,
        );
        let topn = PhysicalPlan::TopN(tidb_planner_core_operator_physicalop::PhysicalTopN {
            base: op_with_stats("TopN", 5.0),
            by_items: vec![by_item],
            offset: 4,
            count: 6,
            ..tidb_planner_core_operator_physicalop::PhysicalTopN::default()
        });
        let topned = attach2_task(
            topn,
            vec![mpp_task_over(20.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP TopN attaches");
        let Some(PhysicalPlan::TopN(root_topn)) = topned.plan() else {
            panic!("the original TopN remains at root");
        };
        let Some(PhysicalPlan::TableReader(reader)) = root_topn.base.children().first() else {
            panic!("root TopN reads through a TableReader");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
            panic!("the MPP reader owns the pass-through sender");
        };
        let Some(PhysicalPlan::TopN(pushed_topn)) = sender.base.children().first() else {
            panic!("the partial TopN is below the sender");
        };
        assert_eq!(pushed_topn.offset, 0);
        assert_eq!(pushed_topn.count, 10);
    }

    #[test]
    fn mpp_topn_materializes_heavy_by_items_once() {
        use tidb_ast::CiString;
        use tidb_datatype::{Datum, FieldType, FieldTypeCode, VectorFloat32};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::scalar_function::ScalarFunction;

        let id = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
        let score = Column::new(2, FieldType::new(FieldTypeCode::Double));
        let vector = Column::new(3, FieldType::new(FieldTypeCode::VectorFloat32));
        let child_schema = Schema::new(vec![id.clone(), score.clone(), vector.clone()]);
        let mut child_base = op_with_stats("Dual", 20.0);
        child_base.base.set_schema(Some(child_schema));
        let child = PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual {
            base: child_base,
            row_count: 0,
        });
        let child_task = Task::Mpp(MppTask::new(child, MppPartitionType::Any, []));

        let query_vector = Constant::new(
            Datum::new_vector_float32(VectorFloat32::must_create(vec![1.0, 2.0])),
            FieldType::new(FieldTypeCode::VectorFloat32),
        );
        let heavy = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("vec_l2_distance"),
            FieldType::new(FieldTypeCode::Double),
            vec![
                Expression::Column(vector),
                Expression::Constant(query_vector),
            ],
        ));
        let topn_schema = Schema::new(vec![id.clone(), score.clone()]);
        let mut topn_base = op_with_stats("TopN", 10.0);
        topn_base.base.set_schema(Some(topn_schema));
        let topn = PhysicalPlan::TopN(tidb_planner_core_operator_physicalop::PhysicalTopN {
            base: topn_base,
            by_items: vec![
                tidb_expression_aggregation::ByItems::new(Expression::Column(score), false),
                tidb_expression_aggregation::ByItems::new(heavy, false),
            ],
            offset: 4,
            count: 6,
            ..tidb_planner_core_operator_physicalop::PhysicalTopN::default()
        });
        let PhysicalPlan::TopN(topn_ref) = &topn else {
            unreachable!();
        };
        assert!(tidb_expr::infer_pushdown::can_exprs_push_down_tiflash(
            &topn_ref
                .by_items
                .iter()
                .map(|item| item.expr.clone())
                .collect::<Vec<_>>()
        ));
        assert!(contains_heavy_topn_function(&topn_ref.by_items[1].expr));
        let column_ids = tidb_planner_core_base::ColumnIdAllocator::new();
        let attached = attach2_task(
            topn,
            vec![child_task],
            Some(&column_ids),
            &PlanIdAllocator::new(),
        )
        .expect("heavy MPP TopN attaches");
        let Some(PhysicalPlan::TopN(global)) = attached.plan() else {
            panic!("the global TopN remains at root");
        };
        let Expression::Column(global_distance) = &global.by_items[1].expr else {
            panic!("global heavy by-item was not replaced by a distance column");
        };
        assert_eq!(global_distance.index, 2);
        let Some(PhysicalPlan::TableReader(reader)) = global.base.children().first() else {
            panic!("global TopN reads through a TableReader");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
            panic!("the MPP reader owns the pass-through sender");
        };
        let Some(PhysicalPlan::TopN(pushed)) = sender.base.children().first() else {
            panic!("the partial TopN is below the sender");
        };
        assert_eq!(pushed.count, 10);
        let Expression::Column(pushed_distance) = &pushed.by_items[1].expr else {
            panic!("pushed heavy by-item was not replaced by a distance column");
        };
        assert_eq!(pushed_distance.unique_id, global_distance.unique_id);
        let Some(PhysicalPlan::Projection(projection)) = pushed.base.children().first() else {
            panic!("the distance projection is below the pushed TopN");
        };
        assert_eq!(projection.exprs.len(), 3);
    }

    #[test]
    fn mpp_union_sequence_and_stream_agg_preserve_go_task_shapes() {
        let union = PhysicalPlan::UnionAll(tidb_planner_core_operator_physicalop::PhysicalUnionAll {
            base: op_with_stats("Union", 20.0),
            mpp: true,
        });
        let union_task = attach2_task(
            union,
            vec![mpp_task_over(10.0), mpp_task_over(11.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP UnionAll attaches");
        assert!(matches!(union_task, Task::Mpp(_)));
        assert_eq!(union_task.plan().expect("union plan").children().len(), 2);

        let sequence = PhysicalPlan::Sequence(tidb_planner_core_operator_physicalop::PhysicalSequence {
            base: op_with_stats("Sequence", 20.0),
        });
        let sequence_task = attach2_task(
            sequence,
            vec![mpp_task_over(10.0), mpp_task_over(11.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP Sequence attaches");
        assert!(matches!(sequence_task, Task::Mpp(_)));
        assert_eq!(
            sequence_task
                .plan()
                .expect("sequence plan")
                .children()
                .len(),
            2
        );

        let stream_agg = PhysicalPlan::StreamAgg(tidb_planner_core_operator_physicalop::PhysicalStreamAgg {
            base: op_with_stats("StreamAgg", 2.0),
            ..tidb_planner_core_operator_physicalop::PhysicalStreamAgg::default()
        });
        let stream_task = attach2_task(
            stream_agg,
            vec![mpp_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("MPP StreamAgg converts to root");
        assert!(matches!(stream_task, Task::Root(_)));
        assert!(matches!(
            stream_task.plan(),
            Some(PhysicalPlan::StreamAgg(_))
        ));
    }

    #[test]
    fn mpp_two_phase_hash_agg_attaches_partial_exchange_and_final() {
        use tidb_planner_core_base::ColumnIdAllocator;
        use tidb_planner_core_operator_physicalop::{AggMppRunMode, PhysicalHashAgg};
        use tidb_planner_property::MppPartitionColumn;
        use tidb_expression_aggregation::{names, AggFuncDesc};
        use tidb_exprctx::ZonedNoColumns;
        use tidb_datatype::SessionTimeZone;

        let plan_allocator = PlanIdAllocator::new();
        let column_allocator = ColumnIdAllocator::new();
        let group = tidb_expr::column::Column::new(
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let value = tidb_expr::column::Column::new(
            2,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let count = AggFuncDesc::new(
            &ZonedNoColumns(SessionTimeZone::utc()),
            names::COUNT,
            vec![Expression::Column(value)],
            false,
        )
        .expect("count descriptor");
        let mut base = op_with_stats("HashAgg", 4.0);
        base.base.set_schema(Some(Schema::new(vec![
            tidb_expr::column::Column::new(
                3,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
            group.clone(),
        ])));
        let hash = PhysicalPlan::HashAgg(PhysicalHashAgg {
            base,
            agg_funcs: vec![count],
            group_by_items: vec![Expression::Column(group.clone())],
            mpp_run_mode: AggMppRunMode::Mpp2Phase,
            mpp_partition_cols: vec![MppPartitionColumn {
                col: group.clone(),
                collate_id: -1,
            }],
            ..PhysicalHashAgg::default()
        });
        let mut child_base = op_with_stats("Dual", 12.0);
        child_base.base.set_schema(Some(Schema::new(vec![
            group.clone(),
            tidb_expr::column::Column::new(
                2,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
        ])));
        let child = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual {
                base: child_base,
                row_count: 12,
            }),
            MppPartitionType::Any,
            [],
        ));
        let attached = attach2_task(hash, vec![child], Some(&column_allocator), &plan_allocator)
            .expect("MPP HashAgg attaches");
        let Task::Mpp(mpp) = attached else {
            panic!("two-phase MPP HashAgg remains an MPP task");
        };
        let Some(PhysicalPlan::HashAgg(final_agg)) = mpp.plan() else {
            panic!("final HashAgg is the MPP task root");
        };
        let Some(PhysicalPlan::ExchangeReceiver(receiver)) = final_agg.base.children().first()
        else {
            panic!("hash exchange sits between partial and final aggregation");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = receiver.base.children().first() else {
            panic!("receiver owns the exchange sender");
        };
        assert_eq!(sender.hash_cols[0].col.unique_id, group.unique_id);
        assert!(matches!(
            sender.base.children().first(),
            Some(PhysicalPlan::HashAgg(partial))
                if partial.agg_funcs[0].mode == tidb_expression_aggregation::AggFunctionMode::Partial1
        ));
    }

    #[test]
    fn mpp_scalar_single_distinct_builds_three_aggregation_stages() {
        use tidb_planner_core_base::ColumnIdAllocator;
        use tidb_planner_core_operator_physicalop::{AggMppRunMode, PhysicalHashAgg};
        use tidb_expression_aggregation::{names, AggFuncDesc};
        use tidb_exprctx::ZonedNoColumns;
        use tidb_datatype::SessionTimeZone;

        let plan_allocator = PlanIdAllocator::new();
        let column_allocator = ColumnIdAllocator::new();
        let ctx = ZonedNoColumns(SessionTimeZone::utc());
        let distinct_col = tidb_expr::column::Column::new(
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let ordinary_col = tidb_expr::column::Column::new(
            2,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let distinct = AggFuncDesc::new(
            &ctx,
            names::COUNT,
            vec![Expression::Column(distinct_col.clone())],
            true,
        )
        .expect("distinct count descriptor");
        let ordinary = AggFuncDesc::new(
            &ctx,
            names::COUNT,
            vec![Expression::Column(ordinary_col.clone())],
            false,
        )
        .expect("ordinary count descriptor");
        let mut base = op_with_stats("HashAgg", 4.0);
        base.base.set_schema(Some(Schema::new(vec![
            tidb_expr::column::Column::new(
                3,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
            tidb_expr::column::Column::new(
                4,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
        ])));
        let hash = PhysicalPlan::HashAgg(PhysicalHashAgg {
            base,
            agg_funcs: vec![distinct, ordinary],
            group_by_items: Vec::new(),
            mpp_run_mode: AggMppRunMode::MppScalar,
            ..PhysicalHashAgg::default()
        });
        let mut child_base = op_with_stats("Dual", 12.0);
        child_base
            .base
            .set_schema(Some(Schema::new(vec![distinct_col, ordinary_col])));
        let child = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual {
                base: child_base,
                row_count: 12,
            }),
            MppPartitionType::Any,
            [],
        ));
        let attached = attach2_task(hash, vec![child], Some(&column_allocator), &plan_allocator)
            .expect("three-stage scalar MPP HashAgg attaches");
        let Task::Mpp(mpp) = attached else {
            panic!("scalar MPP HashAgg remains an MPP task");
        };
        let Some(PhysicalPlan::Projection(projection)) = mpp.plan() else {
            panic!("scalar MPP output restores the original schema");
        };
        let Some(PhysicalPlan::HashAgg(final_agg)) = projection.base.children().first() else {
            panic!("projection is above the final HashAgg");
        };
        assert_eq!(final_agg.agg_funcs[0].name(), names::SUM);
        let Some(PhysicalPlan::ExchangeReceiver(single_receiver)) =
            final_agg.base.children().first()
        else {
            panic!("final aggregation is fed by a single-partition exchange");
        };
        let Some(PhysicalPlan::ExchangeSender(single_sender)) =
            single_receiver.base.children().first()
        else {
            panic!("the single-partition receiver owns its sender");
        };
        let Some(PhysicalPlan::HashAgg(middle)) = single_sender.base.children().first() else {
            panic!("the single-partition exchange feeds the middle HashAgg");
        };
        assert_eq!(
            middle.agg_funcs[0].mode,
            tidb_expression_aggregation::AggFunctionMode::Partial1
        );
        let Some(PhysicalPlan::ExchangeReceiver(hash_receiver)) = middle.base.children().first()
        else {
            panic!("the middle aggregation is fed by a hash exchange");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = hash_receiver.base.children().first()
        else {
            panic!("the hash receiver owns its sender");
        };
        assert_eq!(sender.hash_cols.len(), 1);
        assert!(matches!(
            sender.base.children().first(),
            Some(PhysicalPlan::HashAgg(partial))
                if partial.group_by_items.len() == 1
                    && partial.agg_funcs.len() == 1
                    && partial.agg_funcs[0].name() == names::COUNT
        ));
    }

    #[test]
    fn mpp_scalar_multi_distinct_builds_expand_and_three_aggregation_stages() {
        use tidb_planner_core_base::ColumnIdAllocator;
        use tidb_planner_core_operator_physicalop::{AggMppRunMode, PhysicalHashAgg};
        use tidb_expression_aggregation::{names, AggFuncDesc};
        use tidb_exprctx::ZonedNoColumns;
        use tidb_datatype::SessionTimeZone;

        let plan_allocator = PlanIdAllocator::new();
        let column_allocator = ColumnIdAllocator::new();
        let ctx = ZonedNoColumns(SessionTimeZone::utc());
        let distinct_a = tidb_expr::column::Column::new(
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let distinct_b = tidb_expr::column::Column::new(
            2,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let ordinary_c = tidb_expr::column::Column::new(
            3,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        let count_distinct = |column: tidb_expr::column::Column| {
            AggFuncDesc::new(&ctx, names::COUNT, vec![Expression::Column(column)], true)
                .expect("distinct count descriptor")
        };
        let ordinary = AggFuncDesc::new(
            &ctx,
            names::COUNT,
            vec![Expression::Column(ordinary_c.clone())],
            false,
        )
        .expect("ordinary count descriptor");
        let mut base = op_with_stats("HashAgg", 4.0);
        base.base.set_schema(Some(Schema::new(vec![
            tidb_expr::column::Column::new(
                4,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
            tidb_expr::column::Column::new(
                5,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
            tidb_expr::column::Column::new(
                6,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            ),
        ])));
        let hash = PhysicalPlan::HashAgg(PhysicalHashAgg {
            base,
            agg_funcs: vec![
                count_distinct(distinct_a.clone()),
                count_distinct(distinct_b.clone()),
                ordinary,
            ],
            group_by_items: Vec::new(),
            mpp_run_mode: AggMppRunMode::MppScalar,
            enable_3_stage_distinct_agg: true,
            enable_3_stage_multi_distinct_agg: true,
            ..PhysicalHashAgg::default()
        });
        let source_stats = StatsInfo::new(12.0, [(1, 3.0), (2, 5.0), (3, 7.0)])
            .with_stats_version(2)
            .with_hist_coll(tidb_planner_property::stats_info::HistColl::new(false, 12, []))
            .with_group_ndvs(vec![tidb_planner_property::stats_info::GroupNdv {
                columns: vec![1, 2],
                ndv: 8.0,
            }]);
        let mut child_base = op_with_stats("Dual", 12.0);
        child_base.base.set_stats(Some(source_stats.clone()));
        child_base.base.set_schema(Some(Schema::new(vec![
            distinct_a.clone(),
            distinct_b.clone(),
            ordinary_c,
        ])));
        let child = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual {
                base: child_base,
                row_count: 12,
            }),
            MppPartitionType::Any,
            [],
        ));
        let attached = attach2_task(hash, vec![child], Some(&column_allocator), &plan_allocator)
            .expect("multi-distinct scalar MPP HashAgg attaches");
        let Task::Mpp(mpp) = attached else {
            panic!("multi-distinct scalar MPP HashAgg remains an MPP task");
        };
        let Some(PhysicalPlan::Projection(projection)) = mpp.plan() else {
            panic!("scalar MPP output restores the original schema");
        };
        let Some(PhysicalPlan::HashAgg(final_agg)) = projection.base.children().first() else {
            panic!("projection is above the final HashAgg");
        };
        let Some(PhysicalPlan::ExchangeReceiver(single_receiver)) =
            final_agg.base.children().first()
        else {
            panic!("final aggregation is fed by a single-partition exchange");
        };
        let Some(PhysicalPlan::ExchangeSender(single_sender)) =
            single_receiver.base.children().first()
        else {
            panic!("the single-partition receiver owns its sender");
        };
        let Some(PhysicalPlan::HashAgg(middle)) = single_sender.base.children().first() else {
            panic!("the single-partition exchange feeds the middle HashAgg");
        };
        assert!(middle.group_by_items.is_empty());
        let Some(PhysicalPlan::ExchangeReceiver(hash_receiver)) = middle.base.children().first()
        else {
            panic!("the middle aggregation is fed by a hash exchange");
        };
        let Some(PhysicalPlan::ExchangeSender(sender)) = hash_receiver.base.children().first()
        else {
            panic!("the hash receiver owns its sender");
        };
        assert_eq!(
            sender.hash_cols.len(),
            3,
            "a, b, and grouping ID partition the middle stage"
        );
        let Some(PhysicalPlan::HashAgg(partial)) = sender.base.children().first() else {
            panic!("the hash exchange feeds the partial HashAgg");
        };
        assert_eq!(partial.group_by_items.len(), 3);
        let Some(PhysicalPlan::Projection(projection)) = partial.base.children().first() else {
            panic!("ordinary aggregates are guarded below the partial HashAgg");
        };
        assert_eq!(projection.exprs.len(), 5);
        let Some(PhysicalPlan::Expand(expand)) = projection.base.children().first() else {
            panic!("the partial projection is above the grouping-set Expand");
        };
        assert_eq!(expand.level_exprs.len(), 2);
        assert_eq!(expand.base.base.schema().map(Schema::len), Some(4));
        // Go adjust3StagePhaseAgg replicates rows, then the projection and
        // partial aggregation derive from that same expanded profile.
        let expanded_stats = expand.base.base.stats_info().expect("Expand statistics");
        assert_eq!(expanded_stats.row_count(), 24.0);
        assert_eq!(expanded_stats.col_ndvs(), source_stats.col_ndvs());
        assert_eq!(expanded_stats.group_ndvs(), source_stats.group_ndvs());
        assert_eq!(expanded_stats.hist_coll(), source_stats.hist_coll());
        assert_eq!(expanded_stats.stats_version(), source_stats.stats_version());
        assert_eq!(projection.base.base.stats_info(), Some(expanded_stats));
        assert_eq!(partial.base.base.stats_info().unwrap().row_count(), 8.0);
        assert_eq!(
            expand.base.children()[0].stats_info(),
            Some(&source_stats),
            "expansion must not mutate its child's profile",
        );
    }

    #[test]
    fn a_selection_wraps_the_root_plan_and_keeps_its_own_stats() {
        // `attachPlan2Task`'s root arm: the old plan becomes the child, the
        // new plan becomes the task's, and Count reads the NEW top's stats.
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            ..PhysicalSelection::default()
        });
        let task = attach2_task(
            selection,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("root attaches");
        assert!((task.count() - 8.0).abs() < f64::EPSILON);
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        assert_eq!(plan.children().len(), 1, "the old plan became the child");
    }

    #[test]
    fn an_index_join_inner_operator_inherits_probe_stats_and_receipt() {
        let mut child = root_task_over(3.0);
        let Task::Root(root) = &mut child else {
            unreachable!("the helper builds a root task");
        };
        root.index_join_info = Some(IndexJoinInfo {
            table_id: 42,
            ..IndexJoinInfo::default()
        });
        let projection = PhysicalPlan::Projection(tidb_planner_core_operator_physicalop::PhysicalProjection {
            base: op_with_stats("Projection", 30_000.0),
            ..tidb_planner_core_operator_physicalop::PhysicalProjection::default()
        });

        let task =
            attach2_task(projection, vec![child], None, &PlanIdAllocator::new()).expect("attaches");

        assert_eq!(task.count(), 3.0, "one dynamic probe, not the full input");
        let Task::Root(root) = task else {
            panic!("the projection remains a root task");
        };
        assert_eq!(
            root.index_join_info.as_ref().map(|info| info.table_id),
            Some(42),
            "the owning index join still receives the chosen access"
        );
    }

    #[test]
    fn index_join_completion_moves_unusable_equalities_to_residual_conditions() {
        use tidb_datatype::{FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let column = |id| Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let outer_keys = vec![column(1), column(2)];
        let inner_keys = vec![column(11), column(12)];
        let equality = |left: &Column, right: &Column| {
            ScalarFunction::new(
                tidb_ast::CiString::new("eq"),
                FieldType::new(FieldTypeCode::Tiny),
                vec![
                    Expression::Column(left.clone()),
                    Expression::Column(right.clone()),
                ],
            )
        };
        let child = |columns: Vec<Column>| {
            let mut base = op_with_stats("TableDual", 1.0);
            base.base.set_schema(Some(Schema::new(columns)));
            PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual { base, row_count: 1 })
        };
        let outer = child(outer_keys.clone());
        let inner = child(inner_keys.clone());
        let mut join = tidb_planner_core_operator_physicalop::PhysicalIndexJoin {
            inner_child_idx: 1,
            left_join_keys: outer_keys.clone(),
            right_join_keys: inner_keys.clone(),
            outer_join_keys: outer_keys.clone(),
            inner_join_keys: inner_keys.clone(),
            is_null_eq: vec![false, false],
            equal_conditions: vec![
                equality(&outer_keys[0], &inner_keys[0]),
                equality(&outer_keys[1], &inner_keys[1]),
            ],
            // `PhysicalIndexJoin::default()`'s `kind` is the PLAIN
            // `IndexJoin` variant, not `IndexHashJoin` -- proving Go's
            // `extractOtherEQ=true` (`task.go:158,178`) applies to both.
            ..tidb_planner_core_operator_physicalop::PhysicalIndexJoin::default()
        };
        assert_eq!(join.kind, tidb_planner_core_operator_physicalop::IndexJoinKind::IndexJoin);

        complete_physical_index_join(
            &mut join,
            IndexJoinInfo {
                table_id: 7,
                index_id: Some(8),
                range_bare: false,
                ranges: tidb_ranger::types::Ranges::new(),
                idx_col_lens: vec![tidb_datatype::UNSPECIFIED_LENGTH],
                key_off2_idx_off: vec![0, -1],
                access_conditions: vec![],
                range_rebuild: None,
                compare_filters: None,
            },
            &inner,
            &outer,
        )
        .expect("completes");

        assert_eq!(
            join.outer_join_keys
                .iter()
                .map(|column| column.unique_id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            join.inner_join_keys
                .iter()
                .map(|column| column.unique_id)
                .collect::<Vec<_>>(),
            vec![11]
        );
        assert_eq!(join.key_off2_idx_off, vec![0]);
        assert!(join.equal_conditions.is_empty());
        // The equality unusable as a lookup key (`outer_keys[1]` /
        // `inner_keys[1]`) is restored to `other_conditions` first, but it
        // is a bare `col <eq> col` with each side in its own child's
        // schema, so Go's unconditional `extractOtherEQ` immediately
        // promotes it back out into the hash keys instead of leaving it
        // stuck as a residual filter -- for THIS plain `IndexJoin`, not
        // only for `IndexHashJoin`.
        assert!(
            join.other_conditions.is_empty(),
            "the residual equality is a plain IndexJoin's hash key too"
        );
        assert_eq!(
            join.outer_hash_keys
                .iter()
                .map(|column| column.unique_id)
                .collect::<Vec<_>>(),
            vec![outer_keys[0].unique_id, outer_keys[1].unique_id]
        );
        assert_eq!(
            join.inner_hash_keys
                .iter()
                .map(|column| column.unique_id)
                .collect::<Vec<_>>(),
            vec![inner_keys[0].unique_id, inner_keys[1].unique_id]
        );
    }

    #[test]
    fn index_join_completion_leaves_a_non_column_residual_condition_alone() {
        // The extraction only promotes a BARE `col <eq> col`
        // (`exhaust_physical_plans.go:410-437`, `expression.IsColOpCol`): a
        // residual equality against a non-column operand (a cast, a
        // constant, ...) stays a filter for every index-join kind,
        // including the plain `IndexJoin` this fix now also extracts for.
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;
        use tidb_expr::scalar_function::ScalarFunction;
        use tidb_expr::schema::Schema;

        let column = |id| Column::new(id, FieldType::new(FieldTypeCode::LongLong));
        let outer_keys = vec![column(1)];
        let inner_keys = vec![column(11)];
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let equality_against_constant = ScalarFunction::new(
            tidb_ast::CiString::new("eq"),
            FieldType::new(FieldTypeCode::Tiny),
            vec![
                Expression::Column(outer_keys[0].clone()),
                Expression::Constant(Constant::new(Datum::Int(5), ty)),
            ],
        );
        let child = |columns: Vec<Column>| {
            let mut base = op_with_stats("TableDual", 1.0);
            base.base.set_schema(Some(Schema::new(columns)));
            PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual { base, row_count: 1 })
        };
        let outer = child(outer_keys.clone());
        let inner = child(inner_keys.clone());
        let mut join = tidb_planner_core_operator_physicalop::PhysicalIndexJoin {
            inner_child_idx: 1,
            left_join_keys: outer_keys.clone(),
            right_join_keys: inner_keys.clone(),
            outer_join_keys: outer_keys.clone(),
            inner_join_keys: inner_keys.clone(),
            is_null_eq: vec![false],
            equal_conditions: vec![equality_against_constant],
            ..tidb_planner_core_operator_physicalop::PhysicalIndexJoin::default()
        };

        complete_physical_index_join(
            &mut join,
            IndexJoinInfo {
                table_id: 7,
                index_id: Some(8),
                range_bare: false,
                ranges: tidb_ranger::types::Ranges::new(),
                idx_col_lens: vec![tidb_datatype::UNSPECIFIED_LENGTH],
                key_off2_idx_off: vec![-1],
                access_conditions: vec![],
                range_rebuild: None,
                compare_filters: None,
            },
            &inner,
            &outer,
        )
        .expect("completes");

        assert_eq!(
            join.other_conditions.len(),
            1,
            "a non-column residual equality is never a hash key"
        );
        assert!(join.outer_hash_keys.is_empty());
        assert!(join.inner_hash_keys.is_empty());
    }

    #[test]
    fn a_sort_attaches_without_conversion() {
        // `attach2Task4PhysicalSort` is copy-then-attach, nothing else.
        let sort = PhysicalPlan::Sort(PhysicalSort {
            base: op_with_stats("Sort", 10.0),
            ..PhysicalSort::default()
        });
        let task = attach2_task(
            sort,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        assert!(matches!(task.plan(), Some(PhysicalPlan::Sort(_))));
    }

    #[test]
    fn an_only_column_nominal_sort_returns_the_child_task_itself() {
        // `attach2Task4NominalSort` (`task.go:853`): `if p.OnlyColumn {
        // return tasks[0] }` — the fake operator vanishes.
        let nominal = PhysicalPlan::NominalSort(tidb_planner_core_operator_physicalop::NominalSort {
            base: op_with_stats("Sort", 10.0),
            only_column: true,
            ..tidb_planner_core_operator_physicalop::NominalSort::default()
        });
        let task = attach2_task(
            nominal,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("passes through");
        assert!(
            matches!(task.plan(), Some(PhysicalPlan::TableDual(_))),
            "the child plan is still the task's plan"
        );
    }

    #[test]
    fn an_expression_nominal_sort_attaches_without_conversion() {
        // The non-only-column arm is copy-then-attach, exactly Sort's.
        let nominal = PhysicalPlan::NominalSort(tidb_planner_core_operator_physicalop::NominalSort {
            base: op_with_stats("Sort", 10.0),
            only_column: false,
            ..tidb_planner_core_operator_physicalop::NominalSort::default()
        });
        let task = attach2_task(
            nominal,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::NominalSort(_)));
        assert_eq!(plan.children().len(), 1, "the old plan became the child");
    }

    #[test]
    fn a_union_all_converts_every_child_and_owns_them() {
        // `attach2Task4PhysicalUnionAll` (`task.go:1573`): each child task
        // converts to root and its plan becomes a child of the union, which
        // sits in a fresh RootTask.
        let union = PhysicalPlan::UnionAll(tidb_planner_core_operator_physicalop::PhysicalUnionAll {
            base: op_with_stats("Union", 20.0),
            mpp: false,
        });
        let task = attach2_task(
            union,
            vec![root_task_over(10.0), root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::UnionAll(_)));
        assert_eq!(plan.children().len(), 2, "both children wired in");
    }

    #[test]
    fn a_partition_union_over_an_mpp_child_is_invalid() {
        // Go: "PartitionUnion cannot pushdown to tiflash ... return
        // base.InvalidTask immediately".
        let mut base = op_with_stats("Union", 20.0);
        base.base.set_tp("PartitionUnion");
        let union = PhysicalPlan::UnionAll(tidb_planner_core_operator_physicalop::PhysicalUnionAll { base, mpp: false });
        let mpp_child = Task::Mpp(MppTask::new(
            PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual {
                base: op_with_stats("Dual", 1.0),
                ..tidb_planner_core_operator_physicalop::PhysicalTableDual::default()
            }),
            tidb_planner_property::MppPartitionType::Any,
            [],
        ));
        let task = attach2_task(
            union,
            vec![root_task_over(10.0), mpp_child],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("invalid, not error");
        assert!(task.invalid());
    }

    #[test]
    fn an_apply_builds_the_join_schema_and_inherits_both_warning_sets() {
        // `attach2Task4PhysicalApply` (`task.go:127`): both children convert,
        // the schema is BuildPhysicalJoinSchema's, and the fresh root task's
        // warnings are the concatenation of both children's.
        use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
        use tidb_expr::column::Column;
        use tidb_expr::schema::Schema;

        let child = |rows: f64, col_id: i64, not_null: bool| {
            let mut base = op_with_stats("Dual", rows);
            let mut ft = FieldType::new(FieldTypeCode::LongLong);
            if not_null {
                ft.add_flags(FieldTypeFlags::NOT_NULL);
            }
            let mut schema = Schema::default();
            schema.columns = vec![Column::new(col_id, ft)];
            base.base.set_schema(Some(schema));
            let mut root = RootTask::default();
            root.warnings.append_warning(format!("w{col_id}"));
            root.set_plan(PhysicalPlan::TableDual(
                tidb_planner_core_operator_physicalop::PhysicalTableDual {
                    base,
                    ..tidb_planner_core_operator_physicalop::PhysicalTableDual::default()
                },
            ));
            Task::Root(root)
        };

        let apply = PhysicalPlan::Apply(tidb_planner_core_operator_physicalop::PhysicalApply {
            hash_join: tidb_planner_core_operator_physicalop::PhysicalHashJoin {
                base: op_with_stats("Apply", 5.0),
                join_type: tidb_planner_core_base::LogicalJoinType::LeftOuter,
                inner_child_idx: 1,
                ..tidb_planner_core_operator_physicalop::PhysicalHashJoin::default()
            },
            ..tidb_planner_core_operator_physicalop::PhysicalApply::default()
        });
        let task = attach2_task(
            apply,
            vec![child(10.0, 1, true), child(3.0, 2, true)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        let Task::Root(root) = &task else {
            panic!("a root task");
        };
        assert_eq!(root.warnings.warning_count(), 2, "both children's warnings");
        let plan = task.plan().expect("a plan");
        let schema = plan.base().base.schema().expect("the built join schema");
        assert_eq!(schema.len(), 2, "left + right merged");
        assert!(
            !schema.columns[1]
                .ret_type
                .as_ref()
                .expect("a type")
                .has_flag(FieldTypeFlags::NOT_NULL),
            "LeftOuter resets NOT NULL on the right half"
        );
        assert!(
            schema.columns[0]
                .ret_type
                .as_ref()
                .expect("a type")
                .has_flag(FieldTypeFlags::NOT_NULL),
            "the left half keeps its flag"
        );
    }

    #[test]
    fn a_root_sequence_vanishes_into_its_last_childs_task() {
        // `attach2Task4PhysicalSequence` (`task.go:2259`): any non-MPP child
        // returns tasks[len-1] UNCHANGED — the sequence plan and every
        // producer's task are discarded. Go's own body, reproduced.
        let sequence = PhysicalPlan::Sequence(tidb_planner_core_operator_physicalop::PhysicalSequence {
            base: op_with_stats("Sequence", 5.0),
        });
        let task = attach2_task(
            sequence,
            vec![
                root_task_over(1.0),
                root_task_over(2.0),
                root_task_over(3.0),
            ],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("passes through");
        let plan = task.plan().expect("a plan");
        assert!(
            matches!(plan, PhysicalPlan::TableDual(_)),
            "the LAST child's own plan, no Sequence above it"
        );
        assert!(
            (plan.stats_info().expect("stats").row_count() - 3.0).abs() < f64::EPSILON,
            "specifically the last child"
        );
    }

    #[test]
    fn a_hash_join_concatenates_warnings_right_before_left() {
        // `attach2Task4PhysicalHashJoin` (`task.go:227`):
        // `CopyFrom(&rTask.Warnings, &lTask.Warnings)` — the RIGHT child's
        // warnings come first in the replay order.
        let child = |rows: f64, warning: &str| {
            let mut root = RootTask::default();
            root.warnings.append_warning(warning);
            root.set_plan(PhysicalPlan::TableDual(
                tidb_planner_core_operator_physicalop::PhysicalTableDual {
                    base: op_with_stats("Dual", rows),
                    ..tidb_planner_core_operator_physicalop::PhysicalTableDual::default()
                },
            ));
            Task::Root(root)
        };
        let join = PhysicalPlan::HashJoin(tidb_planner_core_operator_physicalop::PhysicalHashJoin {
            base: op_with_stats("HashJoin", 5.0),
            ..tidb_planner_core_operator_physicalop::PhysicalHashJoin::default()
        });
        let task = attach2_task(
            join,
            vec![child(1.0, "left"), child(2.0, "right")],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        let Task::Root(root) = &task else {
            panic!("a root task");
        };
        let warnings = root.warnings.get_warnings();
        assert_eq!(
            warnings
                .iter()
                .map(|w| w.message.as_str())
                .collect::<Vec<_>>(),
            vec!["right", "left"],
            "Go copies the right child's warnings first"
        );
        assert_eq!(task.plan().expect("plan").children().len(), 2);
    }

    #[test]
    fn a_max_one_row_converts_then_attaches() {
        // No Attach2Task override: `BasePhysicalPlan.Attach2Task`
        // (`base_physical_plan.go:202`) converts to root, then attaches.
        let mor = PhysicalPlan::MaxOneRow(tidb_planner_core_operator_physicalop::PhysicalMaxOneRow {
            base: op_with_stats("MaxOneRow", 1.0),
        });
        let task = attach2_task(
            mor,
            vec![root_task_over(10.0)],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        assert!(matches!(task.plan(), Some(PhysicalPlan::MaxOneRow(_))));
    }

    #[test]
    fn a_max_one_row_on_a_cop_task_propagates_the_conversion_refusal() {
        // The cop child's ConvertToRootTask builds readers
        // (`convertToRootTaskImpl`), which is refused — the default attach
        // body surfaces that refusal rather than skipping the conversion.
        let mor = PhysicalPlan::MaxOneRow(tidb_planner_core_operator_physicalop::PhysicalMaxOneRow {
            base: op_with_stats("MaxOneRow", 1.0),
        });
        let error = attach2_task(
            mor,
            vec![Task::Cop(CopTask::default())],
            None,
            &PlanIdAllocator::new(),
        )
        .expect_err("refuses");
        assert!(
            format!("{error}").contains("convertToRootTaskImpl"),
            "the refusal names its Go symbol: {error}"
        );
    }

    #[test]
    fn a_selection_over_a_cop_child_converts_and_lands_at_root() {
        // `attach2Task4PhysicalSelection` (`task.go:1598`): Go pushes NO
        // selection at attach time — the cop child converts through its
        // reader and the selection sits above it at root.
        let scan = {
            let allocator = PlanIdAllocator::new();
            let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::default();
            base.base = tidb_planner_core_base::BasePlan::new(&allocator, "TableScan", 0);
            base.base.set_stats(Some(StatsInfo::new(9.0, [])));
            PhysicalPlan::TableScan(tidb_planner_core_operator_physicalop::PhysicalTableScan {
                base,
                table_id: 1,
                table_as_name: None,
                dynamic_partition_access: None,
                cost_columns: Vec::new(),
                store_type: tidb_kv::store_type::StoreType::TiKv,
                keep_order: false,
                desc: false,
                ranges: tidb_ranger::types::Ranges::new(),
                ..Default::default()
            })
        };
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            ..PhysicalSelection::default()
        });
        let cop = Task::Cop(CopTask {
            table_plan: Some(Box::new(scan)),
            index_plan_finished: true,
            ..CopTask::default()
        });
        let task = attach2_task(selection, vec![cop], None, &PlanIdAllocator::new())
            .expect("converts and attaches");
        let plan = task.plan().expect("a plan");
        assert!(matches!(plan, PhysicalPlan::Selection(_)));
        assert!(
            matches!(plan.children().first(), Some(PhysicalPlan::TableReader(_))),
            "the selection sits ABOVE the reader"
        );
    }

    #[test]
    fn merge_limit_attachment_respects_union_intersection_and_table_filters() {
        let allocator = PlanIdAllocator::new();
        let scan = || {
            PhysicalPlan::TableScan(tidb_planner_core_operator_physicalop::PhysicalTableScan {
                base: op_with_stats("TableScan", 20.0),
                table_id: 42,
                ..Default::default()
            })
        };
        for intersection in [false, true] {
            for filtered in [false, true] {
                for keep_order in [false, true] {
                    let mut table = scan();
                    if filtered {
                        let mut base = op_with_stats("Selection", 10.0);
                        base.set_children(vec![table]);
                        table = PhysicalPlan::Selection(PhysicalSelection {
                            base,
                            ..Default::default()
                        });
                    }
                    let cop = CopTask {
                        table_plan: Some(Box::new(table)),
                        index_plan_finished: filtered,
                        keep_order,
                        idx_merge_part_plans: vec![scan(), scan()],
                        idx_merge_is_intersection: intersection,
                        ..Default::default()
                    };
                    let limit = PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
                        base: op_with_stats("Limit", 3.0),
                        offset: 2,
                        count: 3,
                        ..Default::default()
                    });
                    let task = attach2_task(limit, vec![Task::Cop(cop)], None, &allocator).unwrap();
                    let mut plan = task.plan().unwrap();
                    if filtered {
                        assert!(
                            matches!(plan, PhysicalPlan::Limit(_)),
                            "residual filters require a root limit"
                        );
                        plan = &plan.children()[0];
                    }
                    let PhysicalPlan::IndexMergeReader(reader) = plan else {
                        panic!("pure merge table scans must embed the root limit: {plan:?}");
                    };
                    assert_eq!(reader.pushed_limit.is_some(), !filtered);
                    if let Some(limit) = reader.pushed_limit {
                        assert_eq!((limit.offset, limit.count), (2, 3));
                    }
                    for partial in &reader.partial_plans_raw {
                        assert_eq!(
                            matches!(partial, PhysicalPlan::Limit(_)),
                            !intersection && !filtered
                        );
                        if let PhysicalPlan::Limit(limit) = partial {
                            assert_eq!((limit.offset, limit.count), (0, 5));
                        }
                    }
                    assert_eq!(
                        matches!(reader.table_plan.as_deref(), Some(PhysicalPlan::Limit(_))),
                        filtered && (intersection || !keep_order)
                    );
                }
            }
        }
    }

    #[test]
    fn merge_limit_keeps_root_filters_and_restores_column_order() {
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::column::Column;
        use tidb_expr::constant::Constant;
        let allocator = PlanIdAllocator::new();
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let columns = vec![Column::new(1, ty.clone()), Column::new(2, ty.clone())];
        let scan = || {
            let mut base = op_with_stats("TableScan", 20.0);
            base.base.set_schema(Some(Schema::new(columns.clone())));
            PhysicalPlan::TableScan(tidb_planner_core_operator_physicalop::PhysicalTableScan {
                base,
                table_id: 42,
                ..Default::default()
            })
        };
        for root_filter in [false, true] {
            let mut cop = CopTask {
                table_plan: Some(Box::new(scan())),
                idx_merge_part_plans: vec![scan(), scan()],
                ..Default::default()
            };
            if root_filter {
                cop.root_task_conds.push(Expression::Constant(Constant::new(
                    Datum::Int(1),
                    ty.clone(),
                )));
            }
            let mut base = op_with_stats("Limit", 3.0);
            base.base
                .set_schema(Some(Schema::new(columns.iter().rev().cloned().collect())));
            let limit = PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
                base,
                offset: 1,
                count: 3,
                ..Default::default()
            });
            let task = attach2_task(limit, vec![Task::Cop(cop)], None, &allocator).unwrap();
            let plan = task.plan().unwrap();
            if root_filter {
                assert!(matches!(plan, PhysicalPlan::Limit(_)));
                assert!(matches!(&plan.children()[0], PhysicalPlan::Selection(_)));
                let PhysicalPlan::IndexMergeReader(reader) = &plan.children()[0].children()[0] else {
                    panic!("merge below root filter")
                };
                assert!(reader.pushed_limit.is_none());
                assert!(reader
                    .partial_plans_raw
                    .iter()
                    .all(|partial| !matches!(partial, PhysicalPlan::Limit(_))));
            } else {
                let PhysicalPlan::Projection(projection) = plan else {
                    panic!("reordered output needs a projection")
                };
                assert_eq!(projection.exprs.len(), 2);
                for (expr, expected) in projection.exprs.iter().zip(columns.iter().rev()) {
                    let Expression::Column(column) = expr else { panic!("column projection") };
                    assert_eq!(column.unique_id, expected.unique_id);
                }
                let PhysicalPlan::IndexMergeReader(reader) = &plan.children()[0] else {
                    panic!("merge below projection")
                };
                assert!(reader.pushed_limit.is_some());
            }
        }
    }

    #[test]
    fn an_index_merge_cop_task_converts_to_the_retained_reader_tree() {
        let scan = |id: i32, rows: f64| {
            let mut base = op_with_stats("TableScan", rows);
            base.base.set_id(id);
            PhysicalPlan::TableScan(tidb_planner_core_operator_physicalop::PhysicalTableScan {
                base,
                table_id: 42,
                ..tidb_planner_core_operator_physicalop::PhysicalTableScan::default()
            })
        };
        let task = CopTask {
            table_plan: Some(Box::new(scan(3, 5.0))),
            index_plan_finished: true,
            keep_order: true,
            idx_merge_part_plans: vec![scan(1, 3.0), scan(2, 2.0)],
            idx_merge_is_intersection: true,
            ..CopTask::default()
        }
        .convert_to_root_task_impl(&PlanIdAllocator::new())
        .expect("index merge converts to a root reader");

        let PhysicalPlan::IndexMergeReader(reader) = task.plan().expect("a retained reader") else {
            panic!("index merge reader");
        };
        assert_eq!(reader.partial_plans_raw.len(), 2);
        assert!(reader.table_plan.is_some());
        assert!(reader.is_intersection_type);
        assert!(reader.keep_order);
    }

    #[test]
    fn the_attach_copies_so_the_child_task_survives() {
        // Go's bodies `Copy()` the incoming task; the caller's task must not
        // observe the wrap.
        let original = root_task_over(10.0);
        let selection = PhysicalPlan::Selection(PhysicalSelection {
            base: op_with_stats("Selection", 8.0),
            ..PhysicalSelection::default()
        });
        let _ = attach2_task(
            selection,
            vec![original.copy()],
            None,
            &PlanIdAllocator::new(),
        )
        .expect("attaches");
        assert!(
            matches!(original.plan(), Some(PhysicalPlan::TableDual(_))),
            "the original task keeps its own plan"
        );
    }
}

#[cfg(test)]
mod task_tests {
    use super::*;
    use tidb_planner_core_operator_physicalop::physical_table_reader::ReadReqType;
    use tidb_planner_core_operator_physicalop::PhysicalPlan;
    use tidb_planner_core_base::PlanIdAllocator;
    use tidb_planner_property::stats_info::StatsInfo;
    // All WRITTEN: Go's coverage of the task layer is exercised through
    // planner integration suites; `task_base.go` has no unit tests of its
    // own beside them.

    fn dual_with_rows(rows: f64) -> PhysicalPlan {
        let allocator = PlanIdAllocator::new();
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::default();
        base.base =
            tidb_planner_core_base::BasePlan::new(&allocator, tidb_planner_core_operator_logicalop::LogicalTableDual::TYPE, 0);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        base.base
            .set_schema(Some(tidb_expr::schema::Schema::new(vec![
                tidb_expr::column::Column::new(
                    1,
                    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                ),
            ])));
        PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual { base, row_count: 0 })
    }

    #[test]
    fn root_conversion_uses_statement_values_for_root_filters() {
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::constant::{Constant, ParamMarker};
        let allocator = PlanIdAllocator::new();
        let ty = FieldType::new(FieldTypeCode::LongLong);
        let mut parameter = Constant::new(Datum::Null, ty.clone());
        parameter.param_marker = Some(ParamMarker { order: 0 });
        let condition =
            Expression::ScalarFunction(tidb_expr::scalar_function::ScalarFunction::new(
                tidb_ast::CiString::new("eq"),
                ty.clone(),
                vec![
                    Expression::Column(tidb_expr::column::Column::new(1, ty)),
                    Expression::Constant(parameter),
                ],
            ));
        let histogram = std::sync::Arc::new(tidb_stats::column::Column {
            histogram: tidb_stats::Histogram {
                ndv: 10,
                buckets: vec![tidb_stats::Bucket {
                    count: 100,
                    repeat: 10,
                    ndv: 10,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(10),
                }],
                ..Default::default()
            },
            top_n: None,
            cmsketch: None,
            stats_version: 2,
            ..Default::default()
        });
        let mut plan = table_scan_with_rows(100.0);
        plan.base_mut()
            .base
            .set_schema(Some(tidb_expr::schema::Schema::new(vec![
                tidb_expr::column::Column::new(1, FieldType::new(FieldTypeCode::LongLong)),
            ])));
        plan.set_stats(Some(StatsInfo::new(100.0, [(1, 10.0)]).with_hist_coll(
            tidb_planner_property::stats_info::HistColl::new(false, 100, []).with_histograms([(1, histogram)]),
        )));
        let mpp = Task::Mpp(MppTask {
            plan: Some(Box::new(plan.clone())),
            root_task_conds: vec![condition.clone()],
            ..Default::default()
        });
        let task = Task::Cop(CopTask {
            table_plan: Some(Box::new(plan)),
            root_task_conds: vec![condition],
            ..Default::default()
        });
        let evaluate = |expression: &Expression| match expression {
            Expression::Constant(value) if value.param_marker.is_some() => Ok(Datum::Int(3)),
            _ => tidb_ranger::points::evaluate_static(expression),
        };
        fn filter_rows(plan: &PhysicalPlan) -> Option<f64> {
            if let PhysicalPlan::Selection(selection) = plan {
                if selection.from_data_source {
                    return plan.stats_info().map(|stats| stats.row_count());
                }
            }
            plan.children().iter().find_map(filter_rows)
        }
        for task in [task, mpp] {
            let converted = task
                .copy()
                .into_root_task_in(&allocator, &evaluate)
                .unwrap();
            assert_eq!(
                converted.plan().unwrap().stats_info().unwrap().row_count(),
                10.0
            );
            assert!(matches!(converted.plan(), Some(PhysicalPlan::Selection(_))));

            let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(&allocator, "Selection", 0);
            base.base
                .set_stats(task.plan().and_then(PhysicalPlan::stats_info).cloned());
            let parent = PhysicalPlan::Selection(tidb_planner_core_operator_physicalop::PhysicalSelection {
                base,
                conditions: vec![],
                from_data_source: false,
            });
            let attached =
                attach2_task_in(parent, vec![task.copy()], None, &allocator, 1.0, &evaluate, &Default::default())
                    .unwrap();
            let attached = attached.into_root_task_in(&allocator, &evaluate).unwrap();
            assert_eq!(filter_rows(attached.plan().unwrap()), Some(10.0));

            let property = tidb_planner_property::PhysicalProperty {
                sort_items: vec![tidb_planner_property::SortItem::new(1, false)],
                ..Default::default()
            };
            let enforced =
                tidb_planner_core_operator_physicalop::enforce::enforce_property_in(
                    &property,
                    task,
                    &allocator,
                    &evaluate,
                    None,
                    &CoreTaskAttacher,
                )
                    .unwrap();
            let filter = &enforced.plan().unwrap().children()[0];
            assert!(matches!(filter, PhysicalPlan::Selection(_)));
            assert_eq!(filter.stats_info().unwrap().row_count(), 10.0);
        }
    }

    fn table_scan_with_rows(rows: f64) -> PhysicalPlan {
        let allocator = PlanIdAllocator::new();
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::default();
        base.base = tidb_planner_core_base::BasePlan::new(&allocator, "TableScan", 0);
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        PhysicalPlan::TableScan(tidb_planner_core_operator_physicalop::PhysicalTableScan {
            base,
            table_id: 1,
            table_as_name: None,
            dynamic_partition_access: None,
            cost_columns: Vec::new(),
            store_type: tidb_kv::store_type::StoreType::TiKv,
            keep_order: false,
            desc: false,
            ranges: tidb_ranger::types::Ranges::new(),
            ..Default::default()
        })
    }

    #[test]
    fn mpp_task_converts_to_tiflash_reader_with_passthrough_sender() {
        let task = Task::Mpp(MppTask::new(
            table_scan_with_rows(7.0),
            MppPartitionType::Any,
            [],
        ));
        let converted = task
            .into_root_task(&PlanIdAllocator::new())
            .expect("the MPP fragment converts at the root boundary");
        let Task::Root(root) = converted else {
            panic!("MPP conversion must produce a root task");
        };
        let PhysicalPlan::TableReader(reader) = root.get_plan() else {
            panic!("MPP conversion must build a table reader");
        };
        assert_eq!(reader.store_type, StoreType::TiFlash);
        assert_eq!(reader.read_req_type, ReadReqType::Mpp);
        let Some(PhysicalPlan::ExchangeSender(sender)) = reader.table_plan.as_deref() else {
            panic!("the table reader must own the pass-through sender");
        };
        assert_eq!(
            sender.exchange_type,
            tidb_planner_core_operator_physicalop::ExchangeType::PassThrough
        );
        assert!(sender.hash_cols.is_empty());
        assert!(matches!(
            sender.base.children(),
            [PhysicalPlan::TableScan(_)]
        ));

        let mut filtered = MppTask::new(table_scan_with_rows(7.0), MppPartitionType::Any, []);
        filtered.root_task_conds = vec![Expression::Column(column_with_id(1))];
        let Task::Root(filtered_root) = Task::Mpp(filtered)
            .into_root_task(&PlanIdAllocator::new())
            .expect("a scan-root condition becomes a root selection")
        else {
            panic!("filtered MPP conversion must produce a root task");
        };
        assert!(matches!(
            filtered_root.get_plan(),
            PhysicalPlan::Selection(_)
        ));

        let mut misplaced = MppTask::new(
            PhysicalPlan::TableDual(tidb_planner_core_operator_physicalop::PhysicalTableDual::default()),
            MppPartitionType::Any,
            [],
        );
        misplaced.root_task_conds = vec![Expression::Column(column_with_id(1))];
        let converted = Task::Mpp(misplaced)
            .into_root_task(&PlanIdAllocator::new())
            .expect("Go returns an invalid task for misplaced root conditions");
        assert!(converted.invalid());
    }

    fn column_with_id(id: i64) -> tidb_expr::column::Column {
        let mut column = tidb_expr::column::Column::new(
            id,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        );
        column.id = id;
        column
    }

    fn scan_with_schema(
        allocator: &PlanIdAllocator,
        tp: &str,
        schema: tidb_expr::schema::Schema,
        rows: f64,
    ) -> PhysicalPlan {
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(allocator, tp, 10);
        base.base.set_schema(Some(schema));
        base.base.set_stats(Some(StatsInfo::new(rows, [])));
        match tp {
            "TableScan" => PhysicalPlan::TableScan(tidb_planner_core_operator_physicalop::PhysicalTableScan {
                base,
                table_id: 1,
                store_type: tidb_kv::store_type::StoreType::TiKv,
                ..tidb_planner_core_operator_physicalop::PhysicalTableScan::default()
            }),
            "IndexScan" => PhysicalPlan::IndexScan(tidb_planner_core_operator_physicalop::PhysicalIndexScan {
                base,
                table_id: 1,
                index_id: 1,
                ..tidb_planner_core_operator_physicalop::PhysicalIndexScan::default()
            }),
            _ => unreachable!("only scan operators are built by this helper"),
        }
    }

    #[test]
    fn index_lookup_pushdown_builds_and_flattens_the_go_plan_shape() {
        fn check(reader: &tidb_planner_core_operator_physicalop::PhysicalIndexLookUpReader, complex: bool) {
            assert!(reader.index_lookup_push_down);
            assert!(reader.table_plans.iter().all(|plan| {
                plan.stats_info()
                    .is_some_and(|stats| stats.row_count() == 0.0)
            }));

            let mut ids = std::collections::BTreeSet::new();
            for plan in reader.table_plans.iter().chain(&reader.index_plans) {
                assert!(ids.insert(plan.id()), "duplicated plan id {}", plan.id());
            }

            if complex {
                assert_eq!(
                    reader
                        .table_plans
                        .iter()
                        .map(PhysicalPlan::tp)
                        .collect::<Vec<_>>(),
                    ["TableScan", "Selection", "Projection"]
                );
                assert_eq!(
                    reader
                        .index_plans
                        .iter()
                        .map(PhysicalPlan::tp)
                        .collect::<Vec<_>>(),
                    [
                        "IndexScan",
                        "Limit",
                        "TableScan",
                        "LocalIndexLookUp",
                        "Selection",
                        "Projection"
                    ]
                );
                assert_eq!(
                    reader.index_plans_un_natural_orders,
                    std::collections::BTreeMap::from([(1, 3)])
                );
            } else {
                assert_eq!(reader.table_plans[0].tp(), "TableScan");
                assert_eq!(
                    reader
                        .index_plans
                        .iter()
                        .map(PhysicalPlan::tp)
                        .collect::<Vec<_>>(),
                    ["IndexScan", "TableScan", "LocalIndexLookUp"]
                );
                assert_eq!(
                    reader.index_plans_un_natural_orders,
                    std::collections::BTreeMap::from([(0, 2)])
                );
            }

            let local = reader
                .index_plans
                .iter()
                .find_map(|plan| match plan {
                    PhysicalPlan::LocalIndexLookUp(local) => Some(local),
                    _ => None,
                })
                .expect("the flattened local lookup");
            assert_eq!(local.index_handle_offsets, [1]);
            assert_eq!(local.base.base.query_block_offset(), 10);
            assert_eq!(local.base.base.schema().expect("lookup schema").len(), 3);
        }

        let allocator = PlanIdAllocator::new();
        let table_schema = tidb_expr::schema::Schema::new(vec![
            column_with_id(1),
            column_with_id(2),
            column_with_id(3),
        ]);
        let index_schema =
            tidb_expr::schema::Schema::new(vec![column_with_id(2), column_with_id(1)]);

        let simple = Task::Cop(CopTask {
            index_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "IndexScan",
                index_schema.clone(),
                1_000.0,
            ))),
            table_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "TableScan",
                table_schema.clone(),
                1_000.0,
            ))),
            index_lookup_push_down_by: tidb_planner_util::path::IndexLookupPushDownBy::Hint,
            ..CopTask::default()
        })
        .convert_to_root_task(&allocator)
        .expect("the simple lookup converts");
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = simple.plan() else {
            panic!("an index lookup reader");
        };
        check(reader, false);
        let cloned = simple.plan().expect("reader").deep_clone();
        let PhysicalPlan::IndexLookUpReader(cloned) = cloned else {
            panic!("the clone remains a reader");
        };
        check(&cloned, false);

        let common_handle = Task::Cop(CopTask {
            index_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "IndexScan",
                index_schema.clone(),
                1_000.0,
            ))),
            table_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "TableScan",
                table_schema.clone(),
                1_000.0,
            ))),
            common_handle_cols: vec![column_with_id(1), column_with_id(2)],
            index_lookup_push_down_by: tidb_planner_util::path::IndexLookupPushDownBy::Hint,
            ..CopTask::default()
        })
        .convert_to_root_task(&allocator)
        .expect("the common-handle lookup converts");
        let Some(PhysicalPlan::IndexLookUpReader(common_handle)) = common_handle.plan() else {
            panic!("an index lookup reader");
        };
        let PhysicalPlan::LocalIndexLookUp(local) = &common_handle.index_plans[2] else {
            panic!("the local lookup");
        };
        assert!(local.index_handle_offsets.is_empty());

        let mut table_scan = scan_with_schema(&allocator, "TableScan", table_schema, 500.0);
        let mut selection_base =
            tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(&allocator, "Selection", 10);
        selection_base.base.set_schema(table_scan.schema().cloned());
        selection_base
            .base
            .set_stats(Some(StatsInfo::new(200.0, [])));
        selection_base.set_children(vec![table_scan]);
        let selection = PhysicalPlan::Selection(tidb_planner_core_operator_physicalop::PhysicalSelection {
            base: selection_base,
            conditions: Vec::new(),
            from_data_source: true,
        });
        let mut projection_base =
            tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(&allocator, "Projection", 10);
        projection_base
            .base
            .set_schema(Some(tidb_expr::schema::Schema::new(vec![column_with_id(
                2,
            )])));
        projection_base
            .base
            .set_stats(Some(StatsInfo::new(200.0, [])));
        projection_base.set_children(vec![selection]);
        table_scan = PhysicalPlan::Projection(tidb_planner_core_operator_physicalop::PhysicalProjection {
            base: projection_base,
            exprs: Vec::new(),
            calculate_no_delay: false,
            avoid_column_evaluator: false,
        });

        let index_scan = scan_with_schema(&allocator, "IndexScan", index_schema, 1_000.0);
        let mut limit_base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(&allocator, "Limit", 10);
        limit_base.base.set_schema(index_scan.schema().cloned());
        limit_base.base.set_stats(Some(StatsInfo::new(1_000.0, [])));
        limit_base.set_children(vec![index_scan]);
        let limit = PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
            base: limit_base,
            ..tidb_planner_core_operator_physicalop::PhysicalLimit::default()
        });
        let complex = Task::Cop(CopTask {
            index_plan: Some(Box::new(limit)),
            table_plan: Some(Box::new(table_scan)),
            index_lookup_push_down_by: tidb_planner_util::path::IndexLookupPushDownBy::Hint,
            ..CopTask::default()
        })
        .convert_to_root_task(&allocator)
        .expect("the complex lookup converts");
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = complex.plan() else {
            panic!("an index lookup reader");
        };
        check(reader, true);
    }

    #[test]
    fn a_table_only_cop_task_converts_into_a_table_reader() {
        // `convertToRootTaskImpl`'s table branch (`task_base.go:571`): the
        // pushed-down plan hangs off the reader's TablePlan field, the
        // store type is read off the bottom scan, and the task's warnings
        // ride the deferred copy onto the fresh root task.
        let allocator = PlanIdAllocator::new();
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::default();
        base.base = tidb_planner_core_base::BasePlan::new(&allocator, "TableScan", 0);
        base.base.set_stats(Some(StatsInfo::new(42.0, [])));
        let scan = PhysicalPlan::TableScan(tidb_planner_core_operator_physicalop::PhysicalTableScan {
            base,
            table_id: 9,
            table_as_name: None,
            dynamic_partition_access: None,
            cost_columns: Vec::new(),
            store_type: tidb_kv::store_type::StoreType::TiKv,
            keep_order: false,
            desc: false,
            ranges: tidb_ranger::types::Ranges::new(),
            ..Default::default()
        });
        let mut cop = CopTask {
            table_plan: Some(Box::new(scan)),
            index_plan_finished: true,
            ..CopTask::default()
        };
        cop.warnings.append_warning("carried");
        let task = Task::Cop(cop)
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("converts");
        let Task::Root(root) = &task else {
            panic!("a root task");
        };
        assert_eq!(root.warnings.warning_count(), 1, "the deferred copy");
        let Some(PhysicalPlan::TableReader(reader)) = task.plan() else {
            panic!("a TableReader, got {:?}", task.plan());
        };
        assert_eq!(
            reader.store_type,
            tidb_kv::store_type::StoreType::TiKv
        );
        assert!(
            matches!(
                reader.table_plan.as_deref(),
                Some(PhysicalPlan::TableScan(_))
            ),
            "the pushed-down side hangs off TablePlan, not the child list"
        );
        assert!(reader.base.children().is_empty());
        assert!(
            (task
                .plan()
                .expect("plan")
                .stats_info()
                .expect("stats")
                .row_count()
                - 42.0)
                .abs()
                < f64::EPSILON,
            "the reader carries the table plan's stats"
        );
    }

    #[test]
    fn cop_pushdown_wraps_uint64_row_bounds_like_go() {
        for is_topn in [false, true] {
            let allocator = PlanIdAllocator::new();
            let child = Task::Cop(CopTask {
                table_plan: Some(Box::new(table_scan_with_rows(100.0))),
                index_plan_finished: true,
                ..CopTask::default()
            });
            let base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::with_id(9, "Limit", 0);
            let plan = if is_topn {
                PhysicalPlan::TopN(tidb_planner_core_operator_physicalop::PhysicalTopN {
                    by_items: vec![tidb_expression_aggregation::ByItems::new(
                        Expression::Column(column_with_id(1)),
                        false,
                    )],
                    base,
                    offset: u64::MAX,
                    count: 8,
                    ..Default::default()
                })
            } else {
                PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
                    base,
                    offset: u64::MAX,
                    count: 8,
                    ..Default::default()
                })
            };
            let task = attach2_task(plan, vec![child], None, &allocator).unwrap();
            let root = task.plan().unwrap();
            let bound = |plan: &PhysicalPlan| match plan {
                PhysicalPlan::Limit(p) => (p.offset, p.count),
                PhysicalPlan::TopN(p) => (p.offset, p.count),
                _ => panic!("expected row bound: {plan:?}"),
            };
            assert_eq!(bound(root), (u64::MAX, 8));
            let PhysicalPlan::TableReader(reader) = &root.children()[0] else {
                panic!("expected table reader");
            };
            assert_eq!(bound(reader.table_plan.as_deref().unwrap()), (0, 7));
        }
    }

    #[test]
    fn partial_order_topn_pushes_prefix_limit_and_keeps_root_topn() {
        for partitioned in [false, true] {
            let allocator = PlanIdAllocator::new();
            let schema = tidb_expr::schema::Schema::new(vec![column_with_id(1)]);
            let cop = Task::Cop(CopTask {
                index_plan: Some(Box::new(scan_with_schema(
                    &allocator,
                    "IndexScan",
                    schema.clone(),
                    100.0,
                ))),
                table_plan: Some(Box::new(scan_with_schema(
                    &allocator,
                    "TableScan",
                    schema.clone(),
                    100.0,
                ))),
                partial_order_match_result: Some(
                    tidb_planner_property::PartialOrderMatchResult {
                        matched: true,
                        prefix_col: Some(column_with_id(1)),
                        prefix_len: 4,
                    },
                ),
                ..CopTask::default()
            });
            let mut topn_base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(&allocator, "TopN", 0);
            topn_base.base.set_schema(Some(schema));
            topn_base.base.set_stats(Some(StatsInfo::new(5.0, [])));
            let topn = PhysicalPlan::TopN(tidb_planner_core_operator_physicalop::PhysicalTopN {
                base: topn_base,
                by_items: vec![tidb_expression_aggregation::ByItems::new(
                    Expression::Column(column_with_id(1)),
                    false,
                )],
                partition_by: if partitioned {
                    vec![tidb_planner_property::SortItem::from_column(
                        column_with_id(1),
                        false,
                    )]
                } else {
                    vec![]
                },
                offset: 2,
                count: 3,
                ..Default::default()
            });

            let task = attach2_task(topn, vec![cop], None, &allocator).expect("attaches");
            let Some(PhysicalPlan::TopN(root_topn)) = task.plan() else {
                panic!("partial-order TopN stays at the root: {:?}", task.plan());
            };
            assert_eq!((root_topn.offset, root_topn.count), (2, 3));
            assert_eq!(root_topn.prefix_col, Some(1));
            assert_eq!(root_topn.prefix_len, 4);
            let Some(PhysicalPlan::IndexLookUpReader(reader)) = root_topn.base.children().first()
            else {
                panic!("the cop task converts to an index lookup reader");
            };
            let Some(PhysicalPlan::Limit(limit)) = reader.index_plan.as_deref() else {
                panic!("the prefix limit is pushed to the index plan");
            };
            assert_eq!((limit.offset, limit.count), (0, 5));
            assert_eq!(limit.prefix_col, Some(1));
            assert_eq!(limit.prefix_len, 4);
            assert!(limit.partition_by.is_empty());
            assert_eq!(!root_topn.partition_by.is_empty(), partitioned);
            assert!(matches!(
                limit.base.children().first(),
                Some(PhysicalPlan::IndexScan(_))
            ));
        }
    }

    #[test]
    fn a_limit_sinks_into_the_index_lookup_reader() {
        // `sinkIntoIndexLookUp` (`task.go:733`): a root Limit over a
        // converted double read becomes the reader's PushedLimit — no root
        // Limit operator survives — and the reader plus its bare table scan
        // adopt the limit's stats.
        let double = Task::Cop(CopTask {
            index_plan: Some(Box::new(dual_with_rows(100.0))),
            table_plan: Some(Box::new(table_scan_with_rows(100.0))),
            keep_order: true,
            ..CopTask::default()
        });
        let mut base = tidb_planner_core_operator_physicalop::BasePhysicalPlan::with_id(9, "Limit", 0);
        base.base
            .set_stats(Some(tidb_planner_property::stats_info::StatsInfo::new(5.0, [])));
        let limit = PhysicalPlan::Limit(tidb_planner_core_operator_physicalop::PhysicalLimit {
            base,
            partition_by: Vec::new(),
            offset: 2,
            count: 3,
            prefix_col: None,
            prefix_len: 0,
        });
        let task =
            attach2_task(limit, vec![double], None, &PlanIdAllocator::new()).expect("attaches");
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = task.plan() else {
            panic!(
                "the limit sinks, leaving the reader on top: {:?}",
                task.plan()
            );
        };
        let pushed = reader.pushed_limit.expect("the sunk limit");
        assert_eq!((pushed.offset, pushed.count), (2, 3));
        assert!(
            (reader.base.base.stats_info().expect("stats").row_count() - 5.0).abs() < f64::EPSILON
        );
        let Some(PhysicalPlan::TableScan(scan)) = reader.table_plan.as_deref() else {
            panic!("the bare table side");
        };
        assert!(
            (scan.base.base.stats_info().expect("stats").row_count() - 5.0).abs() < f64::EPSILON,
            "the table side adopts the smaller stats"
        );
    }

    #[test]
    fn need_extra_proj_restores_origin_before_root_conditions_for_all_readers() {
        use tidb_expr::schema::Schema;

        for kind in ["table", "lookup", "merge"] {
            let allocator = PlanIdAllocator::new();
            let origin = Schema::new(vec![column_with_id(11)]);
            let expanded = Schema::new(vec![column_with_id(11), column_with_id(12)]);
            let mut cop = CopTask {
                table_plan: Some(Box::new(scan_with_schema(
                    &allocator,
                    "TableScan",
                    expanded.clone(),
                    20.0,
                ))),
                need_extra_proj: true,
                origin_schema: Some(origin),
                root_task_conds: vec![Expression::Column(column_with_id(11))],
                ..Default::default()
            };
            if kind == "lookup" {
                cop.index_plan = Some(Box::new(scan_with_schema(
                    &allocator,
                    "IndexScan",
                    expanded.clone(),
                    20.0,
                )));
            } else if kind == "merge" {
                cop.idx_merge_part_plans.push(scan_with_schema(
                    &allocator,
                    "IndexScan",
                    expanded,
                    20.0,
                ));
            }
            let task = cop.convert_to_root_task_impl(&allocator).expect(kind);
            let PhysicalPlan::Selection(selection) = task.plan().unwrap() else {
                panic!("root conditions must remain above the projection");
            };
            let PhysicalPlan::Projection(projection) = &selection.base.children()[0] else {
                panic!("{kind}: extra columns must be projected away");
            };
            assert_eq!(projection.base.base.schema().unwrap().len(), 1);
            let [Expression::Column(column)] = projection.exprs.as_slice() else {
                panic!("origin columns must be passed through");
            };
            assert_eq!(column.unique_id, 11);
            let reader = &projection.base.children()[0];
            assert_eq!(reader.schema().unwrap().len(), 2);
            assert_eq!(projection.base.base.stats_info().unwrap().row_count(), 20.0);
            assert_eq!(
                projection.base.base.query_block_offset(),
                reader.query_block_offset()
            );
            assert!(matches!(
                (kind, reader),
                ("table", PhysicalPlan::TableReader(_))
                    | ("lookup", PhysicalPlan::IndexLookUpReader(_))
                    | ("merge", PhysicalPlan::IndexMergeReader(_))
            ));
        }
    }

    #[test]
    fn need_extra_proj_does_not_replace_pushed_aggregate_schema() {
        use tidb_planner_core_operator_physicalop::{BasePhysicalPlan, PhysicalHashAgg, PhysicalStreamAgg};
        use tidb_expr::schema::Schema;
        for double_read in [false, true] {
            for hash in [false, true] {
                let allocator = PlanIdAllocator::new();
                let input = Schema::new(vec![column_with_id(11), column_with_id(12)]);
                let mut base = BasePhysicalPlan::new(&allocator, "Aggregation", 10);
                base.base
                    .set_schema(Some(Schema::new(vec![column_with_id(21)])));
                base.base.set_stats(Some(StatsInfo::new(5.0, [])));
                base.set_children(vec![scan_with_schema(
                    &allocator,
                    "TableScan",
                    input.clone(),
                    20.0,
                )]);
                let aggregate = if hash {
                    PhysicalPlan::HashAgg(PhysicalHashAgg {
                        base,
                        ..Default::default()
                    })
                } else {
                    PhysicalPlan::StreamAgg(PhysicalStreamAgg {
                        base,
                        ..Default::default()
                    })
                };
                let cop = CopTask {
                    table_plan: Some(Box::new(aggregate)),
                    index_plan: double_read
                        .then(|| Box::new(scan_with_schema(&allocator, "IndexScan", input, 20.0))),
                    need_extra_proj: true,
                    origin_schema: Some(Schema::new(vec![column_with_id(11)])),
                    ..Default::default()
                };
                let task = cop.convert_to_root_task_impl(&allocator).unwrap();
                let plan = task.plan().unwrap();
                assert!(matches!(
                    plan,
                    PhysicalPlan::TableReader(_) | PhysicalPlan::IndexLookUpReader(_)
                ));
                assert_eq!(plan.schema().unwrap().columns[0].unique_id, 21);
            }
        }
    }

    #[test]
    fn virtual_expansion_keeps_root_conditions_above_origin_projection() {
        use tidb_expr::schema::Schema;
        let allocator = PlanIdAllocator::new();
        let mut generated = column_with_id(12);
        generated.virtual_expr = Some(Box::new(Expression::Column(column_with_id(11))));
        let cop = CopTask {
            table_plan: Some(Box::new(scan_with_schema(
                &allocator,
                "TableScan",
                Schema::new(vec![generated.clone()]),
                20.0,
            ))),
            root_task_conds: vec![Expression::Column(generated)],
            ..Default::default()
        };
        let task = cop.convert_to_root_task_impl(&allocator).unwrap();
        let PhysicalPlan::Selection(selection) = task.plan().unwrap() else {
            panic!("Go applies root conditions after constructing the origin projection");
        };
        let PhysicalPlan::Projection(projection) = &selection.base.children()[0] else {
            panic!("virtual dependency expansion requires a projection");
        };
        assert_eq!(projection.base.base.schema().unwrap().len(), 1);
        assert_eq!(projection.base.children()[0].schema().unwrap().len(), 2);
    }

    #[test]
    fn root_task_conds_land_as_a_selection_above_the_reader() {
        // `handleRootTaskConds` (`physicalop/task.go:47`): the unpushable
        // conditions become a FromDataSource Selection above the converted
        // reader, stats scaled by Go's own histogram-miss fallback
        // (`cost.SelectionFactor` = 0.8).
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::Expression;

        let cond = Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        let task = Task::Cop(CopTask {
            index_plan: Some(Box::new(dual_with_rows(10.0))),
            root_task_conds: vec![cond],
            ..CopTask::default()
        });
        let converted = task
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("converts");
        let Some(PhysicalPlan::Selection(selection)) = converted.plan() else {
            panic!("a Selection above the reader, got {:?}", converted.plan());
        };
        assert!(selection.from_data_source);
        assert_eq!(selection.conditions.len(), 1);
        assert!(
            (selection.base.base.stats_info().expect("stats").row_count() - 8.0).abs()
                < f64::EPSILON,
            "10 rows * SelectionFactor 0.8"
        );
        assert!(matches!(
            selection.base.children().first(),
            Some(PhysicalPlan::IndexReader(_))
        ));
    }

    #[test]
    fn root_task_filter_preserves_go_one_row_floor() {
        use tidb_planner_property::stats_info::HistColl;
        use std::sync::Arc;
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::{Expression, ScalarFunction};
        use tidb_stats::histogram::{Bucket, Histogram};

        let unique_id = 1;
        let histogram = Histogram {
            id: unique_id,
            ndv: 10,
            last_update_version: 1,
            buckets: vec![Bucket {
                count: 100,
                repeat: 10,
                ndv: 10,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(10),
            }],
            ..Histogram::default()
        };
        let hist_coll = HistColl::new(false, 100, [])
            .with_histograms([(
                unique_id,
                Arc::new(tidb_stats::column::Column {
                    histogram,
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                }),
            )])
            .with_modify_count(0);
        let stats = StatsInfo::new(100.0, [(unique_id, 10.0)]).with_hist_coll(hist_coll);
        let mut scan = table_scan_with_rows(100.0);
        scan.base_mut().base.set_stats(Some(stats));

        let compare = |name: &str, value| {
            let long = FieldType::new(FieldTypeCode::LongLong);
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new(name),
                long.clone(),
                vec![
                    Expression::Column(column_with_id(unique_id)),
                    Expression::Constant(Constant::new(Datum::Int(value), long)),
                ],
            ))
        };
        let task = Task::Cop(CopTask {
            table_plan: Some(Box::new(scan)),
            root_task_conds: vec![compare("gt", 10), compare("lt", 1)],
            ..CopTask::default()
        });
        let converted = task
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("root conditions convert to a selection");
        let rows = converted
            .plan()
            .and_then(PhysicalPlan::stats_info)
            .expect("selection stats")
            .row_count();
        assert_eq!(
            rows, 1.0,
            "Go Selectivity floors a recognized estimate at 1 / RealtimeCount"
        );
    }

    #[test]
    fn root_task_conversion_retains_statement_ndv_scaling() {
        for ratio in [0.0, 0.5, 1.0] {
            let stats_context = TaskStatsContext {
                scale_ndv_skew_ratio: ratio,
                ..Default::default()
            };
            let stats = StatsInfo::new(1_000.0, [(1, 10.0)]);
            let mut scan = table_scan_with_rows(1_000.0);
            scan.set_stats(Some(stats.clone()));
            let conditions = vec![Expression::Column(column_with_id(1))];
            let tasks = [
                Task::Cop(CopTask {
                    table_plan: Some(Box::new(scan.clone())),
                    root_task_conds: conditions.clone(),
                    stats_context: stats_context.clone(),
                    ..Default::default()
                }),
                Task::Cop(CopTask {
                    index_plan: Some(Box::new(scan.clone())),
                    root_task_conds: conditions.clone(),
                    stats_context: stats_context.clone(),
                    ..Default::default()
                }),
                Task::Cop(CopTask {
                    table_plan: Some(Box::new(scan.clone())),
                    idx_merge_part_plans: vec![scan.clone()],
                    root_task_conds: conditions.clone(),
                    stats_context: stats_context.clone(),
                    ..Default::default()
                }),
                Task::Cop(CopTask {
                    table_plan: Some(Box::new(scan.clone())),
                    idx_merge_part_plans: vec![scan.clone(), scan.clone()],
                    idx_merge_is_intersection: true,
                    root_task_conds: conditions.clone(),
                    stats_context: stats_context.clone(),
                    ..Default::default()
                }),
                Task::Mpp(MppTask {
                    plan: Some(Box::new(scan)),
                    root_task_conds: conditions,
                    stats_context: stats_context.clone(),
                    ..Default::default()
                }),
            ];
            for task in tasks {
                let root = task.into_root_task(&PlanIdAllocator::new()).unwrap();
                let actual = root.plan().unwrap().stats_info().unwrap();
                let expected = stats.scale(tidb_planner_core_cost::SELECTION_FACTOR, ratio);
                assert_eq!(actual.row_count(), expected.row_count());
                assert_eq!(actual.col_ndv(1), expected.col_ndv(1), "ratio {ratio}");
            }
        }
    }

    #[test]
    fn root_task_filter_uses_the_task_session_estimator_options() {
        use tidb_planner_cardinality::row_count_estimator::EstimatorOptions;
        use tidb_planner_property::stats_info::HistColl;
        use std::sync::Arc;
        use tidb_datatype::{Datum, FieldType, FieldTypeCode};
        use tidb_expr::constant::Constant;
        use tidb_expr::expression::{Expression, ScalarFunction};
        use tidb_stats::histogram::{Bucket, Histogram};

        let unique_id = 1;
        let histogram = Histogram {
            id: unique_id,
            ndv: 10,
            last_update_version: 1,
            buckets: vec![Bucket {
                count: 100,
                repeat: 10,
                ndv: 10,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(10),
            }],
            ..Histogram::default()
        };
        let hist_coll = HistColl::new(false, 1_000, [])
            .with_histograms([(
                unique_id,
                Arc::new(tidb_stats::column::Column {
                    histogram,
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                }),
            )])
            .with_modify_count(2_000);
        let table_stats = StatsInfo::new(1_000.0, [(unique_id, 10.0)]).with_hist_coll(hist_coll);
        let compare = |name: &str, value| {
            let long = FieldType::new(FieldTypeCode::LongLong);
            Expression::ScalarFunction(ScalarFunction::new(
                tidb_ast::CiString::new(name),
                long.clone(),
                vec![
                    Expression::Column(column_with_id(unique_id)),
                    Expression::Constant(Constant::new(Datum::Int(value), long)),
                ],
            ))
        };
        let conditions = vec![compare("gt", 12), compare("lt", 15)];
        let estimate = |estimator_options, input_rows: f64| {
            let mut scan = table_scan_with_rows(1_000.0);
            scan.base_mut().base.set_stats(Some(
                table_stats.scale(input_rows / table_stats.row_count(), 1.0),
            ));
            let task = CopTask {
                table_plan: Some(Box::new(scan)),
                root_task_conds: conditions.clone(),
                stats_context: TaskStatsContext {
                    root_filter_stats: Some(Arc::new(table_stats.clone())),
                    estimator_options,
                    ..Default::default()
                },
                ..CopTask::default()
            };
            let Task::Root(root) = Task::Cop(task)
                .into_root_task(&PlanIdAllocator::new())
                .expect("cop task converts")
            else {
                panic!("cop task must convert to a root task");
            };
            root.get_plan()
                .stats_info()
                .expect("selection stats")
                .row_count()
        };

        let baseline = estimate(EstimatorOptions::default(), 1_000.0);
        let risk_adjusted = estimate(
            EstimatorOptions {
                risk_range_skew_ratio: 0.5,
                ..EstimatorOptions::default()
            },
            1_000.0,
        );
        let determinate = estimate(
            EstimatorOptions {
                risk_range_skew_ratio: 0.5,
                allow_use_modify_count: false,
                ..EstimatorOptions::default()
            },
            1_000.0,
        );

        assert!(
            (estimate(EstimatorOptions::default(), 100.0) - baseline * 0.1).abs() < 1e-9,
            "the table histogram population must stay unchanged when the reader is scaled"
        );
        assert!(risk_adjusted > baseline);
        assert_ne!(risk_adjusted, determinate);
    }

    #[test]
    fn an_index_only_cop_task_converts_into_an_index_reader() {
        // `convertToRootTaskImpl`'s index branch (`task_base.go:563`): the
        // pushed-down index plan wraps in a PhysicalIndexReader carrying its
        // stats; the DOUBLE-READ shape (both halves) now builds
        // `BuildIndexLookUpTask`'s reader, schema and stats from the TABLE
        // side (`Init`, `physical_indexlookup_reader.go:205`).
        let task = Task::Cop(CopTask {
            index_plan: Some(Box::new(dual_with_rows(4.0))),
            ..CopTask::default()
        });
        let converted = task
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("converts");
        let Some(PhysicalPlan::IndexReader(reader)) = converted.plan() else {
            panic!("an IndexReader, got {:?}", converted.plan());
        };
        assert!(reader.index_plan.is_some());
        assert_eq!(reader.base.base.schema().expect("reader schema").len(), 1);
        assert!(
            (converted
                .plan()
                .expect("plan")
                .stats_info()
                .expect("stats")
                .row_count()
                - 4.0)
                .abs()
                < f64::EPSILON
        );

        let double = Task::Cop(CopTask {
            index_plan: Some(Box::new(dual_with_rows(1.0))),
            table_plan: Some(Box::new(dual_with_rows(3.0))),
            keep_order: true,
            ..CopTask::default()
        });
        let converted = double
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("builds");
        let Some(PhysicalPlan::IndexLookUpReader(lookup)) = converted.plan() else {
            panic!("an IndexLookUpReader, got {:?}", converted.plan());
        };
        assert!(lookup.keep_order);
        // The reader's stats are the TABLE side's, which `FinishIndexPlan`
        // (the first thing `convertToRootTaskImpl` does) has just set to the
        // index plan's.
        assert!(
            (converted
                .plan()
                .expect("plan")
                .stats_info()
                .expect("stats")
                .row_count()
                - 1.0)
                .abs()
                < f64::EPSILON
        );
    }

    /// Go `RootTask.Copy` keeps the plan POINTER (`p: t.p`) and copies only
    /// the warnings. A copy must still behave as its own value: a write
    /// through one task cannot reach the other's plan, and a task that owns
    /// its plan alone hands it over without copying.
    #[test]
    fn a_task_copy_shares_the_plan_and_writes_do_not_cross() {
        let mut original = RootTask::default();
        original.set_plan(dual_with_rows(3.0));
        let rows = |task: &RootTask| task.get_plan().stats_info().expect("stats").row_count();

        let copied = original.copy();
        assert!((rows(&copied) - 3.0).abs() < f64::EPSILON);

        let mut written = Task::Root(copied);
        written
            .plan_mut()
            .expect("plan")
            .set_stats(Some(StatsInfo::new(9.0, [])));
        assert!(
            (rows(&original) - 3.0).abs() < f64::EPSILON,
            "the write must not reach the task it was copied from"
        );
        let Task::Root(mut written) = written else {
            panic!("root task");
        };
        assert!((rows(&written) - 9.0).abs() < f64::EPSILON);

        // The warnings are copied, not shared, exactly as Go's Copy does.
        original.warnings.append_warning("one");
        assert_eq!(written.warnings.warning_count(), 0);

        // Taking the plan out of its only owner yields the same value.
        let taken = written.take_plan().expect("plan");
        assert!((taken.stats_info().expect("stats").row_count() - 9.0).abs() < f64::EPSILON);
        assert!(written.take_plan().is_none());
    }

    #[test]
    fn an_empty_root_task_is_gos_invalid_task() {
        // `base.InvalidTask` is "core's empty RootTask".
        let task = Task::invalid_task();
        assert!(task.invalid());
        assert!(task.plan().is_none());
    }

    #[test]
    fn lookup_pushdown_is_applied_only_without_keep_order() {
        let build = |keep_order| {
            Task::Cop(CopTask {
                index_plan: Some(Box::new(dual_with_rows(1.0))),
                table_plan: Some(Box::new(dual_with_rows(1.0))),
                index_lookup_push_down_by: tidb_planner_util::path::IndexLookupPushDownBy::Hint,
                keep_order,
                ..CopTask::default()
            })
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("lookup task builds")
        };

        let unordered = build(false);
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = unordered.plan() else {
            panic!("an IndexLookUpReader");
        };
        // Go's `detachRootTableScanPlan` asserts the leaf IS a TableScan
        // (`physical_indexlookup.go:155`): a Dual table plan can never be
        // pushed down, so the reader keeps the ordinary two-phase lookup and
        // the flag stays off.
        assert!(!reader.index_lookup_push_down);

        let ordered = build(true);
        let Task::Root(root) = &ordered else {
            panic!("a root task");
        };
        let Some(PhysicalPlan::IndexLookUpReader(reader)) = ordered.plan() else {
            panic!("an IndexLookUpReader");
        };
        assert!(!reader.index_lookup_push_down);
        assert_eq!(root.warnings.warning_count(), 1);
        assert_eq!(
            root.warnings.warnings[0].message,
            "hint INDEX_LOOKUP_PUSHDOWN is inapplicable, keep order is not supported."
        );
    }

    #[test]
    fn count_reads_whichever_half_the_cop_task_has_open() {
        // `CopTask.Count` (`task_base.go:455-461`): the index half before
        // FinishIndexPlan, the table half after.
        let mut task = CopTask {
            index_plan: Some(Box::new(dual_with_rows(7.0))),
            table_plan: Some(Box::new(dual_with_rows(99.0))),
            ..CopTask::default()
        };
        assert!((task.count() - 7.0).abs() < f64::EPSILON);
        task.finish_index_plan();
        // FinishIndexPlan moved the INDEX stats onto the table half: the
        // table read sees exactly the rows the index found.
        assert!((task.count() - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn copies_never_share_a_warnings_slice() {
        // The whole point of `SimpleWarnings.Copy` per Go's comments.
        let mut original = Task::Root(RootTask::default());
        original.append_warning("first");
        let mut copied = original.copy();
        copied.append_warning("second");
        let Task::Root(original) = &original else {
            unreachable!()
        };
        let Task::Root(copied) = &copied else {
            unreachable!()
        };
        assert_eq!(original.warnings.warning_count(), 1);
        assert_eq!(copied.warnings.warning_count(), 2);
    }

    #[test]
    fn the_warning_cap_is_gos_max_uint16() {
        let mut warnings = SimpleWarnings::default();
        for i in 0..u32::from(u16::MAX) + 10 {
            warnings.append_warning(format!("w{i}"));
        }
        assert_eq!(warnings.warning_count(), usize::from(u16::MAX));
    }

    #[test]
    fn converting_a_non_root_task_refuses_by_name() {
        let cop = Task::Cop(CopTask::default());
        let error = cop
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect_err("refuses");
        assert!(format!("{error:?}").contains("convertToRootTaskImpl"));
        let mpp = Task::Mpp(MppTask::default());
        let error = mpp
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect_err("refuses");
        assert!(format!("{error:?}").contains("ConvertToRootTaskImpl"));
    }

    #[test]
    fn a_root_task_converts_by_copying() {
        let mut root = RootTask::default();
        root.set_plan(dual_with_rows(3.0));
        let task = Task::Root(root);
        let converted = task
            .convert_to_root_task(&PlanIdAllocator::new())
            .expect("root -> root");
        assert!(!converted.invalid());
        assert!((converted.count() - 3.0).abs() < f64::EPSILON);
    }
}
