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

//! Range/filter derivation shared by ordinary and index-merge alternatives.

use super::{AccessPathDerivationContext, PossiblePath};
use crate::{logical::DataSource, plan_base::PlanError};
use tidb_expr::{column::Column, expression::Expression};

/// Go's detached ranges and the subsequent index/table filter partition.
#[derive(Clone, Debug)]
pub struct FilledIndexPath {
    /// Usable key layout at logical derivation, including eligible handles.
    pub columns: Vec<(Column, i64)>,
    /// Full index key layout, retaining unresolved positions and handle suffixes.
    pub full_columns: Vec<Option<(Column, i64)>>,
    /// Range expressions and access-coverage metadata from the same detach run.
    pub detached: crate::ranger::detacher::DetachRangeResult,
    /// Filters evaluated before the table lookup.
    pub index_filters: Vec<Expression>,
    /// Filters requiring the table row or a full prefix-index value.
    pub table_filters: Vec<Expression>,
    /// Go CountAfterIndex; absent when the access estimate is unavailable.
    pub count_after_index: Option<f64>,
}

pub(crate) fn filter_selectivity(
    source: &DataSource,
    conditions: &[Expression],
    context: &AccessPathDerivationContext<'_>,
) -> f64 {
    if conditions.is_empty() {
        return 1.0;
    }
    let Some(stats) = &source.table_stats else {
        return crate::cost_factors::SELECTION_FACTOR;
    };
    let ratio = if stats
        .hist_coll()
        .is_none_or(crate::stats_info::HistColl::pseudo)
    {
        source.base.base.schema().and_then(|schema| {
            crate::logical::rewrite::pseudo_range_filter_selectivity(
                source,
                stats,
                conditions,
                schema,
                context,
                context.selectivity_factor,
            )
        })
    } else {
        crate::logical::rewrite::analyzed_filter_selectivity_in(stats, conditions, context)
    };
    ratio.unwrap_or(crate::cost_factors::SELECTION_FACTOR)
}

/// Derive the range, physical key layout and filter partition together.
/// Ordinary paths and OR branch alternatives must use the same detacher.
pub(crate) fn fill_index_path(
    source: &DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    conditions: &[Expression],
    context: &AccessPathDerivationContext<'_>,
    prefix_single_scan: bool,
) -> Result<FilledIndexPath, PlanError> {
    let mut full_columns = source.declared_index_columns(index);
    let mut columns = full_columns
        .iter()
        .map_while(Clone::clone)
        .collect::<Vec<_>>();
    let suffix = source.handle_cols_to_append(index, &columns);
    full_columns.extend(suffix.iter().cloned().map(Some));
    columns.extend(suffix);
    let detached = if conditions.is_empty() || columns.is_empty() {
        crate::ranger::detacher::DetachRangeResult {
            ranges: crate::ranger::points::full_range(),
            remained_conds: conditions.to_vec(),
            ..Default::default()
        }
    } else {
        let (cols, lengths): (Vec<_>, Vec<_>) = columns.iter().cloned().unzip();
        context
            .detach_index_range(conditions, &cols, &lengths)
            .map_err(|error| match error {
                crate::ranger::points::PointBuilderError::Eval(error) => error.into(),
                crate::ranger::points::PointBuilderError::Value(error) => {
                    PlanError::internal_coded(error.to_string())
                }
                crate::ranger::points::PointBuilderError::Unsupported(message) => {
                    PlanError::unsupported_type(message)
                }
            })?
    };
    let (index_filters, table_filters): (Vec<_>, Vec<_>) = detached
        .remained_conds
        .iter()
        .cloned()
        .partition(|condition| {
            crate::logical::data_source::index_covers_condition(
                source,
                index,
                condition,
                prefix_single_scan,
            )
        });
    Ok(FilledIndexPath {
        columns,
        full_columns,
        detached,
        index_filters,
        table_filters,
        count_after_index: None,
    })
}

pub(crate) fn fill_ordinary_index_paths(
    source: &mut DataSource,
    context: &AccessPathDerivationContext<'_>,
    prefix_single_scan: bool,
) -> Result<(), PlanError> {
    for candidate in &source.enumerated_paths {
        let PossiblePath::Index { index } = candidate else {
            continue;
        };
        let Some(index) = source.indexes.get(*index) else {
            continue;
        };
        if index.is_multi_valued || index.is_columnar {
            continue;
        }
        let mut filled = fill_index_path(
            source, index, &source.pushed_down_conds, context, prefix_single_scan,
        )?;
        let columns = &filled.columns;
        let detached = &filled.detached;
        let index_filters = &filled.index_filters;
        // Go adjusts access rows before computing CountAfterIndex, preserving
        // the old count as the lower risk bound. Both ordinary and cloned
        // intersection paths must see the same adjusted estimate.
        let estimate = source
            .derived_index_paths
            .get(&index.id)
            .and_then(|path| path.row_estimate)
            .map(|mut estimate| {
                if let (Some(filtered), Some(table)) =
                    (source.base.base.stats_info(), source.table_stats.as_ref())
                {
                    if estimate.est + crate::cost_factors::TOLERANCE_FACTOR < filtered.row_count() {
                        estimate.min_est = if estimate.min_est > 0.0 {
                            estimate.min_est.min(estimate.est)
                        } else {
                            estimate.est
                        };
                        let appended = columns.len() > index.columns.len()
                            && detached.ranges.iter().any(|range| {
                                range.low_val.len() > index.columns.len()
                                    || range.high_val.len() > index.columns.len()
                            });
                        estimate.est = if appended {
                            filtered.row_count()
                        } else {
                            (filtered.row_count() / crate::cost_factors::SELECTION_FACTOR)
                                .min(table.row_count())
                        };
                        estimate.max_est = estimate.max_est.max(estimate.est);
                    }
                }
                estimate
            });
        let count_after_index = estimate.map(|estimate| {
            if index_filters.is_empty() {
                estimate.est
            } else {
                (estimate.est * filter_selectivity(source, &index_filters, context)).max(
                    source
                        .base
                        .base
                        .stats_info()
                        .map_or(0.0, |stats| stats.row_count()),
                )
            }
        });
        let state = source.derived_index_paths.entry(index.id).or_default();
        state.row_estimate = estimate;
        filled.count_after_index = count_after_index;
        state.filled = Some(filled);
    }
    Ok(())
}

/// Logical integer/common-handle ranges and residuals shared by table candidates.
#[derive(Clone, Debug)]
pub struct FilledTablePath {
    /// Integer handle present in the source schema, including an explicit rowid.
    pub handle_column: Option<Column>,
    /// The integer ranger's conversion type.
    pub handle_type: tidb_datatype::FieldType,
    /// Usable leading clustered-primary-key columns and their prefix lengths.
    pub common_columns: Vec<(Column, i64)>,
    /// Access ranges, absorbed conditions and residual table filters.
    pub detached: crate::ranger::detacher::DetachRangeResult,
    /// Access count before a physical property's LIMIT/probe adjustment.
    pub count_after_access: Option<f64>,
    /// Go's retained lower access-count bound.
    pub min_count_after_access: f64,
    /// Go's retained upper access-count bound.
    pub max_count_after_access: f64,
}

/// Derive a table alternative from its own conditions without applying the
/// ordinary datasource output-count lower bound to merge partials.
pub(crate) fn detach_table_path(
    source: &DataSource,
    primary_index: Option<usize>,
    conditions: &[Expression],
    context: &AccessPathDerivationContext<'_>,
) -> Result<FilledTablePath, PlanError> {
    let handle_column = source
        .base
        .base
        .schema()
        .and_then(|schema| source.get_pk_is_handle_col(schema))
        .or_else(|| {
            source
                .handle_cols
                .first()
                .filter(|column| column.id == tidb_model::column::EXTRA_HANDLE_ID)
        })
        .cloned();
    let handle_type = handle_column
        .as_ref()
        .and_then(|column| column.ret_type.clone())
        .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
    let common_index = primary_index.and_then(|index| source.indexes.get(index));
    let common_columns =
        common_index.map_or_else(Vec::new, |index| source.index_range_columns(index));
    let map_error = |error| match error {
        crate::ranger::points::PointBuilderError::Eval(error) => PlanError::from(error),
        crate::ranger::points::PointBuilderError::Value(error) => {
            PlanError::internal_coded(error.to_string())
        }
        crate::ranger::points::PointBuilderError::Unsupported(message) => {
            PlanError::unsupported_type(message)
        }
    };
    let detached = if common_index.is_some() {
        if conditions.is_empty() || common_columns.is_empty() {
            crate::ranger::detacher::DetachRangeResult {
                ranges: crate::ranger::points::full_not_null_range(),
                remained_conds: conditions.to_vec(),
                ..Default::default()
            }
        } else {
            let (columns, lengths): (Vec<_>, Vec<_>) = common_columns.iter().cloned().unzip();
            context
                .detach_index_range(conditions, &columns, &lengths)
                .map_err(map_error)?
        }
    } else {
        let access = handle_column.as_ref().map_or_else(Vec::new, |handle| {
            crate::ranger::detacher::extract_access_conditions_for_column(conditions, handle, true)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
        });
        let built = crate::ranger::ranger::build_table_range_in(
            &access,
            &handle_type,
            context.range_max_size,
            context.expression_evaluator,
        )
        .map_err(map_error)?;
        if !built.remained_conds.is_empty() {
            if let Some(handler) = context.range_fallback_handler {
                handler.record_range_fallback(context.range_max_size);
            }
        }
        crate::ranger::detacher::DetachRangeResult {
            ranges: built.ranges,
            access_conds: built.access_conds.to_vec(),
            remained_conds: crate::ranger::detacher::remove_conditions(
                conditions,
                built.access_conds,
            ),
            ..Default::default()
        }
    };
    Ok(FilledTablePath {
        handle_column,
        handle_type,
        common_columns,
        detached,
        count_after_access: None,
        min_count_after_access: 0.0,
        max_count_after_access: 0.0,
    })
}

pub(crate) fn fill_table_path(
    source: &DataSource,
    primary_index: Option<usize>,
    context: &AccessPathDerivationContext<'_>,
) -> Result<FilledTablePath, PlanError> {
    let mut filled = detach_table_path(source, primary_index, &source.pushed_down_conds, context)?;
    let detached = &filled.detached;
    let common_index = primary_index.and_then(|index| source.indexes.get(index));
    let stats = source
        .table_stats
        .as_ref()
        .or_else(|| source.base.base.stats_info());
    let mut count_after_access = stats.map(|stats| stats.row_count());
    if !detached.access_conds.is_empty() {
        count_after_access = match source.table_path_count_after_access {
            Some(count) => Some(count),
            None if common_index.is_some() => stats.map(|stats| {
                crate::ranger::stats_bridge::pseudo_count_by_ranges(
                    &detached.ranges,
                    stats.row_count(),
                )
            }),
            None => Some(estimate_int_table_path(source, &filled, context)?),
        };
    }
    let estimate = common_index
        .and_then(|index| source.derived_index_paths.get(&index.id))
        .and_then(|path| path.row_estimate);
    let mut min_count_after_access = estimate.map_or(0.0, |estimate| estimate.min_est);
    let mut max_count_after_access = estimate.map_or(0.0, |estimate| estimate.max_est);
    if let (Some(count), Some(filtered), Some(table)) = (
        count_after_access.as_mut(),
        source.base.base.stats_info(),
        stats,
    ) {
        if *count + crate::cost_factors::TOLERANCE_FACTOR < filtered.row_count() {
            min_count_after_access = if min_count_after_access > 0.0 {
                min_count_after_access.min(*count)
            } else {
                *count
            };
            *count = (filtered.row_count() / crate::cost_factors::SELECTION_FACTOR)
                .min(table.row_count());
            max_count_after_access = max_count_after_access.max(*count);
        }
    }
    filled.count_after_access = count_after_access;
    filled.min_count_after_access = min_count_after_access;
    filled.max_count_after_access = max_count_after_access;
    Ok(filled)
}

/// Estimate an integer table path from its own typed ranges and the source
/// histogram population. Merge partials must not reuse another path's count.
pub(crate) fn estimate_int_table_path(
    source: &DataSource,
    path: &FilledTablePath,
    context: &AccessPathDerivationContext<'_>,
) -> Result<f64, PlanError> {
    let Some(stats) = source
        .table_stats
        .as_ref()
        .or_else(|| source.base.base.stats_info())
    else {
        return Ok(path.detached.ranges.len() as f64);
    };
    if path.detached.access_conds.is_empty() && !path.detached.ranges.is_empty() {
        return Ok(stats.row_count());
    }
    let hist = stats.hist_coll();
    let column = path
        .handle_column
        .as_ref()
        .and_then(|column| hist?.histogram_for_estimation(column.unique_id));
    let ranges = path
        .detached
        .ranges
        .iter()
        .map(|range| {
            crate::cardinality::row_count_estimator::ColumnRange::new(
                range.low_val[0].clone(),
                range.high_val[0].clone(),
                range.low_exclude,
                range.high_exclude,
            )
        })
        .collect::<Vec<_>>();
    crate::cardinality::row_count_estimator::get_row_count_by_column_ranges(
        column.map(|column| column.as_ref()),
        &ranges,
        path.handle_type.collation(),
        hist.map_or(stats.row_count() as i64, |hist| hist.realtime_count()),
        hist.map_or(0, |hist| hist.modify_count()),
        true,
        context.estimator_options,
    )
    .map(|estimate| estimate.est)
    .map_err(|error| PlanError::internal_coded(error.to_string()))
}
