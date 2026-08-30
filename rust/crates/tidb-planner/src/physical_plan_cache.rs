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

//! Execute-time rebuilding for cached physical plans.
//!
//! This is the Rust counterpart of Go
//! `pkg/planner/core/plan_cache_rebuild.go`.  A cache entry retains one
//! [`PhysicalPlan`] tree. A hit resolves every `ParamMarker`/deferred constant
//! on that retained tree and recursively rebuilds scan ranges in both ordinary
//! child lists and the pushed-down plans owned by reader operators. The cache
//! owner serializes this mutation, matching Go's in-place rebuild of a
//! session-local cached plan.
//!
//! Range construction deliberately uses a zero memory limit.  Go does the
//! same: the original plan already proved that the complete access range can
//! be built, and an execute-time range fallback could widen the storage read
//! while the cached plan no longer carries the residual filter needed to make
//! that widening safe.

use std::fmt;

use tidb_datatype::{Datum, FieldType};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;

use crate::physical::{PhysicalPlan, PhysicalTableScan};
use crate::ranger::types::{has_full_range, Ranges};

/// Session inputs read by Go's `isPlanCacheable` /
/// `isPhysicalPlanCacheable` after physical optimization.
#[derive(Clone, Copy, Debug)]
pub struct PlanCacheabilityContext {
    /// Number of prepared-statement parameters in the statement.
    pub parameter_count: usize,
    /// Fix 45798: whether plans accessing multi-valued indexes may be cached.
    pub enable_generated_columns: bool,
    /// `@@tidb_plan_cache_max_plan_size`; zero disables the size limit.
    pub max_plan_size: u64,
}

fn cached_plan_memory_usage(plan: &PhysicalPlan) -> u64 {
    let mut total = u64::try_from(plan.memory_usage()).unwrap_or(u64::MAX);
    let hidden = match plan {
        PhysicalPlan::CTE(cte) => std::iter::once(cte.seed_plan.as_ref())
            .chain(cte.recursive_plan.as_deref())
            .collect(),
        PhysicalPlan::TableReader(reader) => reader.table_plan.as_deref().into_iter().collect(),
        PhysicalPlan::IndexReader(reader) => reader.index_plan.as_deref().into_iter().collect(),
        PhysicalPlan::IndexLookUpReader(reader) => reader
            .index_plan
            .as_deref()
            .into_iter()
            .chain(reader.table_plan.as_deref())
            .collect(),
        PhysicalPlan::IndexMergeReader(reader) => reader
            .partial_plans_raw
            .iter()
            .chain(reader.table_plan.as_deref())
            .collect(),
        PhysicalPlan::Dml(dml) => dml.select_plan.as_deref().into_iter().collect(),
        _ => Vec::new(),
    };
    for child in hidden {
        total = total.saturating_add(cached_plan_memory_usage(child));
    }
    total
}

fn table_scan_is_full(scan: &PhysicalTableScan) -> bool {
    let unsigned_int_handle = matches!(
        scan.range_rebuild.as_ref(),
        Some(TableRangeRebuild {
            unsigned_int_handle: true,
            ..
        })
    );
    scan.ranges
        .iter()
        .all(|range| range.is_full_range(unsigned_int_handle))
}

fn index_scan_is_full(scan: &crate::physical::PhysicalIndexScan) -> bool {
    scan.ranges.iter().all(|range| range.is_full_range(false))
}

fn physical_plan_cacheable(
    plan: &PhysicalPlan,
    context: PlanCacheabilityContext,
    under_index_merge: bool,
) -> Result<(), String> {
    if !plan.base().base.noncacheable_reason().is_empty() {
        return Err(plan.base().base.noncacheable_reason().to_owned());
    }

    let mut index_merge = under_index_merge;
    let hidden: Vec<&PhysicalPlan> = match plan {
        PhysicalPlan::CTE(cte) => std::iter::once(cte.seed_plan.as_ref())
            .chain(cte.recursive_plan.as_deref())
            .collect(),
        PhysicalPlan::TableDual(_) if context.parameter_count > 0 => {
            return Err("get a TableDual plan".to_owned());
        }
        PhysicalPlan::MemTable(_) => {
            return Err("PhysicalMemTable plan is un-cacheable".to_owned());
        }
        PhysicalPlan::TableReader(reader)
            if reader.store_type == crate::physical_table_reader::StoreType::TiFlash =>
        {
            return Err("TiFlash plan is un-cacheable".to_owned());
        }
        PhysicalPlan::Apply(_) => {
            return Err("PhysicalApply plan is un-cacheable".to_owned());
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            if reader.access_mv_index && !context.enable_generated_columns {
                return Err(
                    "the plan with IndexMerge accessing Multi-Valued Index is un-cacheable"
                        .to_owned(),
                );
            }
            index_merge = true;
            reader.partial_plans_raw.iter().collect()
        }
        PhysicalPlan::IndexScan(scan) if under_index_merge && index_scan_is_full(scan) => {
            return Err("IndexMerge plan with full-scan is un-cacheable".to_owned());
        }
        PhysicalPlan::TableScan(scan) if under_index_merge && table_scan_is_full(scan) => {
            return Err("IndexMerge plan with full-scan is un-cacheable".to_owned());
        }
        PhysicalPlan::TableReader(reader) => reader.table_plan.as_deref().into_iter().collect(),
        PhysicalPlan::IndexReader(reader) => reader.index_plan.as_deref().into_iter().collect(),
        // Go deliberately checks only the index side of IndexLookUpReader.
        PhysicalPlan::IndexLookUpReader(reader) => {
            reader.index_plan.as_deref().into_iter().collect()
        }
        PhysicalPlan::Dml(dml) => dml.select_plan.as_deref().into_iter().collect(),
        _ => Vec::new(),
    };

    for child in hidden.into_iter().chain(plan.children()) {
        physical_plan_cacheable(child, context, index_merge)?;
    }
    Ok(())
}

/// Go `isPlanCacheable` over the physical operators represented by this
/// planner. Operators that do not exist in [`PhysicalPlan`] (such as
/// Shuffle) cannot enter this tree; every represented refusal is checked
/// recursively, including reader-owned subplans.
pub fn plan_cacheable(plan: &PhysicalPlan, context: PlanCacheabilityContext) -> Result<(), String> {
    let physical = match plan {
        PhysicalPlan::Dml(dml) => match dml.select_plan.as_deref() {
            Some(select) => select,
            None => return Ok(()),
        },
        physical => physical,
    };
    if context.max_plan_size > 0 && cached_plan_memory_usage(physical) > context.max_plan_size {
        return Err(
            "plan is too large(decided by the variable @@tidb_plan_cache_max_plan_size)".to_owned(),
        );
    }
    physical_plan_cacheable(physical, context, false)
}

/// The table-scan facts retained specifically so a cache hit can rebuild its
/// parameter-dependent ranges without re-running logical optimization.
#[derive(Clone, Debug)]
pub struct TableRangeRebuild {
    /// Go `PhysicalTableScan.AccessCondition`.
    pub access_conditions: Vec<Expression>,
    /// Integer-handle type.  `None` selects the common-handle index columns.
    pub handle_type: Option<FieldType>,
    /// Go primary-index columns for a common handle.
    pub common_handle_columns: Vec<Column>,
    /// Prefix lengths aligned with [`Self::common_handle_columns`].
    pub common_handle_lengths: Vec<i64>,
    /// Whether the integer handle is unsigned.
    pub unsigned_int_handle: bool,
}

impl TableRangeRebuild {
    /// Retains an integer primary-key handle range source.
    #[must_use]
    pub fn int_handle(
        access_conditions: Vec<Expression>,
        handle_type: FieldType,
        unsigned_int_handle: bool,
    ) -> Self {
        Self {
            access_conditions,
            handle_type: Some(handle_type),
            common_handle_columns: Vec::new(),
            common_handle_lengths: Vec::new(),
            unsigned_int_handle,
        }
    }

    /// Retains a clustered common-handle range source.
    #[must_use]
    pub fn common_handle(
        access_conditions: Vec<Expression>,
        columns: Vec<Column>,
        lengths: Vec<i64>,
    ) -> Self {
        Self {
            access_conditions,
            handle_type: None,
            common_handle_columns: columns,
            common_handle_lengths: lengths,
            unsigned_int_handle: false,
        }
    }
}

/// The index-scan facts retained specifically so a cache hit can rebuild its
/// parameter-dependent ranges.
#[derive(Clone, Debug)]
pub struct IndexRangeRebuild {
    /// Go `PhysicalIndexScan.AccessCondition`.
    pub access_conditions: Vec<Expression>,
    /// Go `PhysicalIndexScan.IdxCols`.
    pub index_columns: Vec<Column>,
    /// Go `PhysicalIndexScan.IdxColLens`.
    pub index_column_lengths: Vec<i64>,
}

impl IndexRangeRebuild {
    /// Retains a complete index range source.
    #[must_use]
    pub fn new(
        access_conditions: Vec<Expression>,
        index_columns: Vec<Column>,
        index_column_lengths: Vec<i64>,
    ) -> Self {
        Self {
            access_conditions,
            index_columns,
            index_column_lengths,
        }
    }
}

/// Range metadata retained by point and batch-point plans. Go rebuilds CBO
/// point plans from access conditions exactly like their originating table or
/// index scan, then additionally verifies that the number of point ranges did
/// not change.
#[derive(Clone, Debug)]
pub enum PointRangeRebuild {
    /// A primary-key/table-handle point range.
    Table(TableRangeRebuild),
    /// A unique-index point range.
    Index(IndexRangeRebuild),
}

/// An evaluator for a deferred constant such as a non-deterministic function.
pub type DeferredExpressionEvaluator<'a> =
    dyn Fn(&Expression) -> Result<Datum, String> + Send + Sync + 'a;

/// Per-execution inputs to cached-plan rebuilding.
pub struct CachedPlanRebuildContext<'a> {
    parameters: &'a [Datum],
    deferred_evaluator: Option<&'a DeferredExpressionEvaluator<'a>>,
}

impl<'a> CachedPlanRebuildContext<'a> {
    /// A context with bound prepared-statement parameters and no deferred
    /// expressions.  Encountering a deferred expression fails closed.
    #[must_use]
    pub const fn new(parameters: &'a [Datum]) -> Self {
        Self {
            parameters,
            deferred_evaluator: None,
        }
    }

    /// Installs the statement evaluator used by deferred constants.
    #[must_use]
    pub const fn with_deferred_evaluator(
        mut self,
        evaluator: &'a DeferredExpressionEvaluator<'a>,
    ) -> Self {
        self.deferred_evaluator = Some(evaluator);
        self
    }
}

/// Why a cached plan was unsafe to reuse for the current execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanCacheRebuildError {
    /// A marker referenced a parameter absent from this `EXECUTE`.
    MissingParameter(i64),
    /// A cached deferred expression had no statement evaluator.
    MissingDeferredEvaluator,
    /// The statement evaluator rejected a deferred expression.
    DeferredEvaluation(String),
    /// Ranger could not build the retained access conditions.
    RangeBuild(String),
    /// Rebuilding consumed fewer predicates, produced an empty range, or
    /// widened a previously restricted range to a full scan.
    UnsafeRange {
        /// The physical scan whose access range became unsafe.
        plan_id: i32,
    },
    /// Index/common-handle metadata was internally inconsistent.
    InvalidMetadata {
        /// The physical scan carrying the inconsistent metadata.
        plan_id: i32,
        /// The invariant the retained metadata violated.
        detail: &'static str,
    },
    /// A point or batch-point rebuild changed the number of point keys.
    RangeCountChanged {
        /// The point plan whose key count changed.
        plan_id: i32,
        /// The number of keys retained by the cached plan.
        expected: usize,
        /// The number produced by this execution's parameters.
        actual: usize,
    },
}

impl fmt::Display for PlanCacheRebuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingParameter(order) => write!(formatter, "missing parameter {order}"),
            Self::MissingDeferredEvaluator => formatter.write_str("missing deferred evaluator"),
            Self::DeferredEvaluation(error) => {
                write!(formatter, "deferred expression evaluation failed: {error}")
            }
            Self::RangeBuild(error) => write!(formatter, "range build failed: {error}"),
            Self::UnsafeRange { plan_id } => {
                write!(formatter, "plan {plan_id} rebuilt an unsafe range")
            }
            Self::InvalidMetadata { plan_id, detail } => {
                write!(
                    formatter,
                    "plan {plan_id} has invalid range metadata: {detail}"
                )
            }
            Self::RangeCountChanged {
                plan_id,
                expected,
                actual,
            } => write!(
                formatter,
                "plan {plan_id} rebuilt {actual} point ranges, expected {expected}"
            ),
        }
    }
}

impl std::error::Error for PlanCacheRebuildError {}

fn bind_expression(
    expression: &mut Expression,
    context: &CachedPlanRebuildContext<'_>,
) -> Result<(), PlanCacheRebuildError> {
    match expression {
        Expression::Column(_) | Expression::CorrelatedColumn(_) => Ok(()),
        Expression::Constant(constant) => {
            if let Some(marker) = constant.param_marker {
                let value = usize::try_from(marker.order)
                    .ok()
                    .and_then(|order| context.parameters.get(order))
                    .ok_or(PlanCacheRebuildError::MissingParameter(marker.order))?;
                constant.replace_cached_value(value.clone());
                // Keep the marker on the retained cached tree so the next
                // execution can replace the value again. Go's Constant reads
                // the current session parameter through the same persistent
                // marker during every RebuildPlan4CachedPlan call.
                return Ok(());
            }
            if let Some(deferred) = constant.deferred_expr.as_mut() {
                bind_expression(deferred, context)?;
                let evaluator = context
                    .deferred_evaluator
                    .ok_or(PlanCacheRebuildError::MissingDeferredEvaluator)?;
                let value =
                    evaluator(deferred).map_err(PlanCacheRebuildError::DeferredEvaluation)?;
                constant.replace_cached_value(value);
            }
            Ok(())
        }
        Expression::ScalarFunction(function) => {
            for argument in &mut function.args {
                bind_expression(argument, context)?;
            }
            function.invalidate_cached_arguments();
            Ok(())
        }
    }
}

fn bind_conditions(
    conditions: &mut [Expression],
    context: &CachedPlanRebuildContext<'_>,
) -> Result<(), PlanCacheRebuildError> {
    for condition in conditions {
        bind_expression(condition, context)?;
    }
    Ok(())
}

fn bind_aggregate_expressions(
    functions: &mut [tidb_expr::aggregation::AggFuncDesc],
    group_by: &mut [Expression],
    context: &CachedPlanRebuildContext<'_>,
) -> Result<(), PlanCacheRebuildError> {
    for function in functions {
        bind_conditions(&mut function.base.args, context)?;
        for item in &mut function.order_by_items {
            bind_expression(&mut item.expr, context)?;
        }
    }
    bind_conditions(group_by, context)
}

/// Go parameter constants read the current execute values through the
/// session expression context. Rust constants own their cached datum, so the
/// private physical clone must refresh every expression-bearing operator in
/// addition to rebuilding scan ranges.
fn bind_plan_expressions(
    plan: &mut PhysicalPlan,
    context: &CachedPlanRebuildContext<'_>,
) -> Result<(), PlanCacheRebuildError> {
    match plan {
        PhysicalPlan::Selection(selection) => {
            bind_conditions(&mut selection.conditions, context)?;
        }
        PhysicalPlan::Projection(projection) => {
            bind_conditions(&mut projection.exprs, context)?;
        }
        PhysicalPlan::HashJoin(join) => {
            bind_conditions(&mut join.left_conditions, context)?;
            bind_conditions(&mut join.right_conditions, context)?;
            bind_conditions(&mut join.other_conditions, context)?;
        }
        PhysicalPlan::MergeJoin(join) => {
            bind_conditions(&mut join.left_conditions, context)?;
            bind_conditions(&mut join.right_conditions, context)?;
            bind_conditions(&mut join.other_conditions, context)?;
        }
        PhysicalPlan::IndexJoin(join) => {
            bind_conditions(&mut join.left_conditions, context)?;
            bind_conditions(&mut join.right_conditions, context)?;
            bind_conditions(&mut join.other_conditions, context)?;
            if let Some(compare_filters) = &mut join.compare_filters {
                bind_conditions(&mut compare_filters.args, context)?;
            }
        }
        PhysicalPlan::Apply(apply) => {
            bind_conditions(&mut apply.hash_join.left_conditions, context)?;
            bind_conditions(&mut apply.hash_join.right_conditions, context)?;
            bind_conditions(&mut apply.hash_join.other_conditions, context)?;
        }
        PhysicalPlan::Sort(sort) => {
            for item in &mut sort.by_items {
                bind_expression(&mut item.expr, context)?;
            }
        }
        PhysicalPlan::TopN(topn) => {
            for item in &mut topn.by_items {
                bind_expression(&mut item.expr, context)?;
            }
        }
        PhysicalPlan::HashAgg(aggregation) => bind_aggregate_expressions(
            &mut aggregation.agg_funcs,
            &mut aggregation.group_by_items,
            context,
        )?,
        PhysicalPlan::StreamAgg(aggregation) => bind_aggregate_expressions(
            &mut aggregation.agg_funcs,
            &mut aggregation.group_by_items,
            context,
        )?,
        PhysicalPlan::Dml(dml) => {
            for expression in dml.update_expressions.iter_mut().flatten() {
                bind_expression(expression, context)?;
            }
        }
        PhysicalPlan::Limit(_)
        | PhysicalPlan::TableScan(_)
        | PhysicalPlan::TableSample(_)
        | PhysicalPlan::MemTable(_)
        | PhysicalPlan::TableDual(_)
        | PhysicalPlan::MaxOneRow(_)
        | PhysicalPlan::NominalSort(_)
        | PhysicalPlan::CTE(_)
        | PhysicalPlan::CTETable(_)
        | PhysicalPlan::Show(_)
        | PhysicalPlan::ShowDDLJobs(_)
        | PhysicalPlan::Lock(_)
        | PhysicalPlan::UnionAll(_)
        | PhysicalPlan::Sequence(_)
        | PhysicalPlan::TableReader(_)
        | PhysicalPlan::IndexScan(_)
        | PhysicalPlan::IndexReader(_)
        | PhysicalPlan::IndexLookUpReader(_)
        | PhysicalPlan::LocalIndexLookUp(_)
        | PhysicalPlan::PointGet(_)
        | PhysicalPlan::BatchPointGet(_)
        | PhysicalPlan::IndexMergeReader(_) => {}
    }
    Ok(())
}

fn range_is_safe(
    original: &Ranges,
    rebuilt: &Ranges,
    access_condition_count: usize,
    used_condition_count: usize,
    remained_condition_count: usize,
    unsigned_int_handle: bool,
) -> bool {
    if remained_condition_count != 0
        || used_condition_count != access_condition_count
        || rebuilt.is_empty()
    {
        return false;
    }
    !(access_condition_count != 0
        && has_full_range(rebuilt, unsigned_int_handle)
        && !original.is_empty()
        && !has_full_range(original, unsigned_int_handle))
}

fn rebuild_table_scan(
    scan: &mut PhysicalTableScan,
    context: &CachedPlanRebuildContext<'_>,
) -> Result<(), PlanCacheRebuildError> {
    let Some(rebuild) = scan.range_rebuild.as_mut() else {
        return Ok(());
    };
    bind_conditions(&mut rebuild.access_conditions, context)?;
    let original = scan.ranges.clone();
    if let Some(handle_type) = &rebuild.handle_type {
        let result =
            crate::ranger::ranger::build_table_range(&rebuild.access_conditions, handle_type, 0)
                .map_err(|error| PlanCacheRebuildError::RangeBuild(format!("{error:?}")))?;
        if !range_is_safe(
            &original,
            &result.ranges,
            rebuild.access_conditions.len(),
            result.access_conds.len(),
            result.remained_conds.len(),
            rebuild.unsigned_int_handle,
        ) {
            return Err(PlanCacheRebuildError::UnsafeRange {
                plan_id: scan.base.base.id(),
            });
        }
        scan.ranges = result.ranges;
        return Ok(());
    }
    if rebuild.common_handle_columns.is_empty()
        || rebuild.common_handle_columns.len() != rebuild.common_handle_lengths.len()
    {
        return Err(PlanCacheRebuildError::InvalidMetadata {
            plan_id: scan.base.base.id(),
            detail: "common-handle columns and lengths must be non-empty and aligned",
        });
    }
    let result = crate::ranger::detacher::detach_cond_and_build_range_for_index(
        &rebuild.access_conditions,
        &rebuild.common_handle_columns,
        &rebuild.common_handle_lengths,
        0,
    )
    .map_err(|error| PlanCacheRebuildError::RangeBuild(format!("{error:?}")))?;
    if !range_is_safe(
        &original,
        &result.ranges,
        rebuild.access_conditions.len(),
        result.access_conds.len(),
        result.remained_conds.len(),
        false,
    ) {
        return Err(PlanCacheRebuildError::UnsafeRange {
            plan_id: scan.base.base.id(),
        });
    }
    scan.ranges = result.ranges;
    Ok(())
}

fn rebuild_index_scan(
    scan: &mut crate::physical::PhysicalIndexScan,
    context: &CachedPlanRebuildContext<'_>,
) -> Result<(), PlanCacheRebuildError> {
    let Some(rebuild) = scan.range_rebuild.as_mut() else {
        return Ok(());
    };
    if rebuild.index_columns.is_empty()
        || rebuild.index_columns.len() != rebuild.index_column_lengths.len()
    {
        return Err(PlanCacheRebuildError::InvalidMetadata {
            plan_id: scan.base.base.id(),
            detail: "index columns and lengths must be non-empty and aligned",
        });
    }
    bind_conditions(&mut rebuild.access_conditions, context)?;
    let result = crate::ranger::detacher::detach_cond_and_build_range_for_index(
        &rebuild.access_conditions,
        &rebuild.index_columns,
        &rebuild.index_column_lengths,
        0,
    )
    .map_err(|error| PlanCacheRebuildError::RangeBuild(format!("{error:?}")))?;
    if !range_is_safe(
        &scan.ranges,
        &result.ranges,
        rebuild.access_conditions.len(),
        result.access_conds.len(),
        result.remained_conds.len(),
        false,
    ) {
        return Err(PlanCacheRebuildError::UnsafeRange {
            plan_id: scan.base.base.id(),
        });
    }
    scan.ranges = result.ranges;
    Ok(())
}

fn rebuild_point_ranges(
    plan_id: i32,
    ranges: &mut Ranges,
    rebuild: &mut PointRangeRebuild,
    expected_count: Option<usize>,
    context: &CachedPlanRebuildContext<'_>,
) -> Result<(), PlanCacheRebuildError> {
    let original = ranges.clone();
    let rebuilt = match rebuild {
        PointRangeRebuild::Table(rebuild) => {
            bind_conditions(&mut rebuild.access_conditions, context)?;
            if let Some(handle_type) = &rebuild.handle_type {
                let result = crate::ranger::ranger::build_table_range(
                    &rebuild.access_conditions,
                    handle_type,
                    0,
                )
                .map_err(|error| PlanCacheRebuildError::RangeBuild(format!("{error:?}")))?;
                if !range_is_safe(
                    &original,
                    &result.ranges,
                    rebuild.access_conditions.len(),
                    result.access_conds.len(),
                    result.remained_conds.len(),
                    rebuild.unsigned_int_handle,
                ) {
                    return Err(PlanCacheRebuildError::UnsafeRange { plan_id });
                }
                result.ranges
            } else {
                if rebuild.common_handle_columns.is_empty()
                    || rebuild.common_handle_columns.len() != rebuild.common_handle_lengths.len()
                {
                    return Err(PlanCacheRebuildError::InvalidMetadata {
                        plan_id,
                        detail: "common-handle columns and lengths must be non-empty and aligned",
                    });
                }
                let result = crate::ranger::detacher::detach_cond_and_build_range_for_index(
                    &rebuild.access_conditions,
                    &rebuild.common_handle_columns,
                    &rebuild.common_handle_lengths,
                    0,
                )
                .map_err(|error| PlanCacheRebuildError::RangeBuild(format!("{error:?}")))?;
                if !range_is_safe(
                    &original,
                    &result.ranges,
                    rebuild.access_conditions.len(),
                    result.access_conds.len(),
                    result.remained_conds.len(),
                    false,
                ) {
                    return Err(PlanCacheRebuildError::UnsafeRange { plan_id });
                }
                result.ranges
            }
        }
        PointRangeRebuild::Index(rebuild) => {
            if rebuild.index_columns.is_empty()
                || rebuild.index_columns.len() != rebuild.index_column_lengths.len()
            {
                return Err(PlanCacheRebuildError::InvalidMetadata {
                    plan_id,
                    detail: "index columns and lengths must be non-empty and aligned",
                });
            }
            bind_conditions(&mut rebuild.access_conditions, context)?;
            let result = crate::ranger::detacher::detach_cond_and_build_range_for_index(
                &rebuild.access_conditions,
                &rebuild.index_columns,
                &rebuild.index_column_lengths,
                0,
            )
            .map_err(|error| PlanCacheRebuildError::RangeBuild(format!("{error:?}")))?;
            if !range_is_safe(
                &original,
                &result.ranges,
                rebuild.access_conditions.len(),
                result.access_conds.len(),
                result.remained_conds.len(),
                false,
            ) {
                return Err(PlanCacheRebuildError::UnsafeRange { plan_id });
            }
            result.ranges
        }
    };
    if expected_count.is_some_and(|expected| rebuilt.len() != expected) {
        return Err(PlanCacheRebuildError::RangeCountChanged {
            plan_id,
            expected: expected_count.expect("checked as present"),
            actual: rebuilt.len(),
        });
    }
    *ranges = rebuilt;
    Ok(())
}

fn update_inner_scan_ranges(plan: &mut PhysicalPlan, ranges: &Ranges) -> bool {
    match plan {
        PhysicalPlan::TableScan(scan) => {
            scan.ranges = ranges.clone();
            true
        }
        PhysicalPlan::IndexScan(scan) => {
            scan.ranges = ranges.clone();
            true
        }
        PhysicalPlan::TableReader(reader) => reader
            .table_plan
            .as_deref_mut()
            .is_some_and(|plan| update_inner_scan_ranges(plan, ranges)),
        PhysicalPlan::IndexReader(reader) => reader
            .index_plan
            .as_deref_mut()
            .is_some_and(|plan| update_inner_scan_ranges(plan, ranges)),
        PhysicalPlan::IndexLookUpReader(reader) => reader
            .index_plan
            .as_deref_mut()
            .is_some_and(|plan| update_inner_scan_ranges(plan, ranges)),
        _ => false,
    }
}

/// Recursively rebuilds every parameter-dependent range in one retained plan.
/// Reader-owned pushed-down trees are walked explicitly because they are not
/// ordinary `PhysicalPlan::children()` entries.
pub fn rebuild_ranges_for_cached_plan(
    plan: &mut PhysicalPlan,
    context: &CachedPlanRebuildContext<'_>,
) -> Result<(), PlanCacheRebuildError> {
    bind_plan_expressions(plan, context)?;
    match plan {
        PhysicalPlan::CTE(cte) => {
            rebuild_ranges_for_cached_plan(cte.seed_plan.as_mut(), context)?;
            if let Some(recursive) = cte.recursive_plan.as_deref_mut() {
                rebuild_ranges_for_cached_plan(recursive, context)?;
            }
        }
        PhysicalPlan::TableScan(scan) => rebuild_table_scan(scan, context)?,
        PhysicalPlan::IndexScan(scan) => rebuild_index_scan(scan, context)?,
        PhysicalPlan::TableReader(reader) => {
            if let Some(table_plan) = reader.table_plan.as_deref_mut() {
                rebuild_ranges_for_cached_plan(table_plan, context)?;
            }
        }
        PhysicalPlan::IndexReader(reader) => {
            if let Some(index_plan) = reader.index_plan.as_deref_mut() {
                rebuild_ranges_for_cached_plan(index_plan, context)?;
            }
        }
        PhysicalPlan::IndexLookUpReader(reader) => {
            if let Some(index_plan) = reader.index_plans.first_mut() {
                rebuild_ranges_for_cached_plan(index_plan, context)?;
            }
            if let Some(index_plan) = reader.index_plan.as_deref_mut() {
                rebuild_ranges_for_cached_plan(index_plan, context)?;
            }
        }
        PhysicalPlan::IndexJoin(join) => {
            let plan_id = join.base.base.id();
            if let Some(rebuild) = join.range_rebuild.as_mut() {
                rebuild_point_ranges(plan_id, &mut join.ranges, rebuild, None, context)?;
                let Some(inner) = join.base.children_mut().get_mut(join.inner_child_idx) else {
                    return Err(PlanCacheRebuildError::InvalidMetadata {
                        plan_id,
                        detail: "index join inner child is missing",
                    });
                };
                if !update_inner_scan_ranges(inner, &join.ranges) {
                    return Err(PlanCacheRebuildError::InvalidMetadata {
                        plan_id,
                        detail: "index join inner child has no range-bearing scan",
                    });
                }
            }
        }
        PhysicalPlan::PointGet(point) => {
            let plan_id = point.base.base.id();
            let Some(rebuild) = point.range_rebuild.as_mut() else {
                return Ok(());
            };
            rebuild_point_ranges(plan_id, &mut point.ranges, rebuild, Some(1), context)?;
        }
        PhysicalPlan::BatchPointGet(batch) => {
            let plan_id = batch.base.base.id();
            let expected_count = batch.ranges.len();
            let Some(rebuild) = batch.range_rebuild.as_mut() else {
                return Ok(());
            };
            rebuild_point_ranges(
                plan_id,
                &mut batch.ranges,
                rebuild,
                Some(expected_count),
                context,
            )?;
        }
        PhysicalPlan::IndexMergeReader(reader) => {
            for partial_plan in &mut reader.partial_plans_raw {
                rebuild_ranges_for_cached_plan(partial_plan, context)?;
            }
        }
        PhysicalPlan::Dml(dml) => {
            if let Some(select_plan) = dml.select_plan.as_deref_mut() {
                rebuild_ranges_for_cached_plan(select_plan, context)?;
            }
        }
        _ => {}
    }
    for child in plan.base_mut().children_mut() {
        rebuild_ranges_for_cached_plan(child, context)?;
    }
    Ok(())
}

impl PhysicalPlan {
    /// Rebuilds this retained cached tree for the current execution.
    ///
    /// The caller must serialize access to the tree. This is Go's
    /// `RebuildPlan4CachedPlan` ownership model: a session cache entry is
    /// reused and its parameter-derived state is refreshed in place.
    pub fn rebuild_plan_for_cache_in_place(
        &mut self,
        context: &CachedPlanRebuildContext<'_>,
    ) -> Result<(), PlanCacheRebuildError> {
        rebuild_ranges_for_cached_plan(self, context)
    }

    /// Produces a privately rebuilt copy without changing this template.
    ///
    /// This remains useful to callers that do not own a serialized cache
    /// entry. The prepared-plan cache uses the in-place method above.
    pub fn rebuild_plan_for_cache(
        &self,
        context: &CachedPlanRebuildContext<'_>,
    ) -> Result<Self, PlanCacheRebuildError> {
        let mut rebuilt = self.deep_clone();
        rebuilt.rebuild_plan_for_cache_in_place(context)?;
        Ok(rebuilt)
    }
}
