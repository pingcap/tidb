// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Ordered-scan estimates using the distribution of a correlated filter
//! column, from `pkg/planner/cardinality/cross_estimation.go`.

use crate::cardinality::row_count_estimator::{
    ColumnRange, EstimatorOptions, get_index_row_count,
    get_row_count_by_column_ranges,
};
use crate::ranger::types::Range;
use crate::stats_info::HistColl;
use tidb_datatype::{Collation, Datum};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;

/// Session controls for Go's correlation-adjusted scan estimates.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CorrelationOptions {
    /// `tidb_opt_enable_correlation_adjustment`.
    pub enabled: bool,
    /// `tidb_opt_correlation_threshold`.
    pub threshold: f64,
    /// `tidb_opt_correlation_exp_factor`.
    pub exponent: i64,
}

impl Default for CorrelationOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: 0.9,
            exponent: 1,
        }
    }
}

/// A ranger interval paired with the estimated rows it contains.
#[derive(Clone, Debug)]
pub struct CountedRange {
    range: Range,
    estimated_rows: f64,
}

impl CountedRange {
    /// Pairs a ranger interval with its source row-count estimate.
    #[must_use]
    pub const fn new(range: Range, estimated_rows: f64) -> Self {
        Self {
            range,
            estimated_rows,
        }
    }

    /// Returns the ranger interval.
    #[must_use]
    pub const fn range(&self) -> &Range {
        &self.range
    }

    /// Returns the row estimate used for cumulative selection.
    #[must_use]
    pub const fn estimated_rows(&self) -> f64 {
        self.estimated_rows
    }
}

/// Result of Go's expected-count range conversion.
#[derive(Clone, Debug)]
pub struct ExpectedCountConversion {
    converted_range: Option<Range>,
    skipped_rows: f64,
    full_scan: bool,
}

impl ExpectedCountConversion {
    /// Returns the ranger interval to estimate, or `None` for a full scan.
    #[must_use]
    pub const fn converted_range(&self) -> Option<&Range> {
        self.converted_range.as_ref()
    }

    /// Returns the cumulative estimate skipped before the selected interval.
    #[must_use]
    pub const fn skipped_rows(&self) -> f64 {
        self.skipped_rows
    }

    /// Reports whether all input ranges are needed to meet the expected count.
    #[must_use]
    pub const fn is_full_scan(&self) -> bool {
        self.full_scan
    }
}

/// Converts ordered ranges into the prefix needed to find `expected_count`.
///
/// Ascending scans retain `[zero-datum, selected.low]`; descending scans
/// retain `[selected.high, MaxValueDatum]`. The selected endpoint's exclusion
/// is inverted exactly as in Go, and the original ranger collators are kept.
#[must_use]
pub fn convert_range_from_expected_cnt(
    ranges: &[CountedRange],
    expected_count: f64,
    descending: bool,
) -> ExpectedCountConversion {
    let mut skipped_rows = 0.0;
    let selected_index = if descending {
        ranges.iter().rposition(|range| {
            if skipped_rows + range.estimated_rows >= expected_count {
                true
            } else {
                skipped_rows += range.estimated_rows;
                false
            }
        })
    } else {
        ranges.iter().position(|range| {
            if skipped_rows + range.estimated_rows >= expected_count {
                true
            } else {
                skipped_rows += range.estimated_rows;
                false
            }
        })
    };

    let Some(index) = selected_index else {
        return ExpectedCountConversion {
            converted_range: None,
            skipped_rows: 0.0,
            full_scan: true,
        };
    };

    let source = &ranges[index].range;
    let converted_range = if descending {
        Range {
            low_val: source.high_val.clone(),
            high_val: vec![Datum::MaxValue],
            collators: source.collators.clone(),
            low_exclude: !source.high_exclude,
            high_exclude: false,
        }
    } else {
        Range {
            low_val: vec![Datum::Null],
            high_val: source.low_val.clone(),
            collators: source.collators.clone(),
            low_exclude: false,
            high_exclude: !source.low_exclude,
        }
    };

    ExpectedCountConversion {
        converted_range: Some(converted_range),
        skipped_rows,
        full_scan: false,
    }
}

/// Applies Go `crossEstimateRowCount` for a table scan's single correlated
/// filter column. Callers must reject non-full access ranges and pseudo stats,
/// as the Go table-scan wrapper does before selecting the column.
#[allow(clippy::too_many_arguments)]
pub fn estimate_table_cross_row_count(
    source_stats: &HistColl,
    table_stats: &HistColl,
    filters: &[Expression],
    column: &Column,
    correlation: f64,
    expected_count: f64,
    path_count_after_access: f64,
    descending: bool,
    range_max_size: i64,
    expression_evaluator: &crate::ranger::points::ExpressionEvaluator<'_>,
    estimator_options: EstimatorOptions,
) -> (f64, bool, f64) {
    let Some(field_type) = column.ret_type.as_ref() else {
        return (0.0, false, correlation);
    };
    let (access_conditions, remained_conditions) =
        crate::ranger::detacher::detach_conds_for_column(filters, column, true);
    if access_conditions.is_empty() {
        return (0.0, false, correlation);
    }
    let Ok(built) = crate::ranger::ranger::build_column_range_in(
        &access_conditions,
        field_type,
        crate::ranger::checker::UNSPECIFIED_LENGTH,
        range_max_size,
        expression_evaluator,
    ) else {
        return (0.0, false, correlation);
    };
    if built.ranges.is_empty() || built.access_conds.is_empty() {
        return (0.0, true, correlation);
    }

    // Go selects the first index whose leading column is this filter column;
    // it does not skip an unloaded or unusable first index for a later one.
    let index_id = source_stats
        .index_ids_for_column(column.unique_id)
        .first()
        .copied();
    let Some(counted_ranges) = built
        .ranges
        .iter()
        .map(|range| {
            let estimated_rows = estimate_correlation_range(
                table_stats,
                column.unique_id,
                index_id,
                range,
                estimator_options,
            )?;
            Some(CountedRange::new(range.clone(), estimated_rows))
        })
        .collect::<Option<Vec<_>>>()
    else {
        return (0.0, false, correlation);
    };
    let scan_descending = if correlation < 0.0 {
        !descending
    } else {
        descending
    };
    let converted =
        convert_range_from_expected_cnt(&counted_ranges, expected_count, scan_descending);
    if converted.is_full_scan() {
        return (path_count_after_access, true, 0.0);
    }
    let Some(range) = converted.converted_range() else {
        return (0.0, false, correlation);
    };
    let Some(range_count) = estimate_correlation_range(
        table_stats,
        column.unique_id,
        index_id,
        range,
        estimator_options,
    ) else {
        return (0.0, false, correlation);
    };
    let mut scan_count = range_count + expected_count - converted.skipped_rows();
    if !remained_conditions.is_empty() {
        scan_count /= 0.8; // Go cardinality.SelectionFactor.
    }
    (scan_count.min(path_count_after_access), true, 0.0)
}

fn estimate_correlation_range(
    stats: &HistColl,
    column_id: i64,
    index_id: Option<i64>,
    range: &Range,
    options: EstimatorOptions,
) -> Option<f64> {
    if let Some(index_id) = index_id {
        let index = stats.index_histogram(index_id)?;
        if stats.pseudo() || index.total_row_count() == 0.0 {
            return None;
        }
        let context = stats.index_estimation_stats(index_id);
        let estimate =
            get_index_row_count(&context, &[], &[], std::slice::from_ref(range), options).ok()?;
        return Some(estimate.est);
    }
    let low = range.low_val.first()?.clone();
    let high = range.high_val.first()?.clone();
    let column = stats.histogram_for_estimation(column_id)?;
    if stats.pseudo() || column.total_row_count() == 0.0 {
        return None;
    }
    let collation = range
        .collators
        .first()
        .copied()
        .unwrap_or(Collation::Binary);
    get_row_count_by_column_ranges(
        Some(column),
        &[ColumnRange::new(
            low,
            high,
            range.low_exclude,
            range.high_exclude,
        )],
        collation,
        stats.realtime_count(),
        stats.modify_count(),
        stats.pk_is_handle(),
        options,
    )
    .ok()
    .map(|estimate| estimate.est)
}

#[cfg(test)]
mod tests {
    use super::{
        CountedRange, EstimatorOptions, convert_range_from_expected_cnt,
        estimate_table_cross_row_count,
    };
    use crate::cardinality::row_count_estimator::ColumnStats;
    use crate::ranger::types::Range;
    use crate::stats_info::HistColl;
    use std::sync::Arc;
    use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::expression::Expression;
    use tidb_expr::scalar_function::ScalarFunction;
    use tidb_stats::histogram::Histogram;

    fn counted(
        low: i64,
        high: i64,
        rows: f64,
        low_exclude: bool,
        high_exclude: bool,
    ) -> CountedRange {
        CountedRange::new(
            Range {
                low_val: vec![Datum::Int(low)],
                high_val: vec![Datum::Int(high)],
                collators: vec![Collation::Binary],
                low_exclude,
                high_exclude,
            },
            rows,
        )
    }

    #[test]
    fn ascending_scan_uses_source_range_before_expected_count_is_reached() {
        let ranges = [
            counted(10, 20, 3.0, false, true),
            counted(30, 40, 4.0, true, false),
            counted(50, 60, 9.0, false, false),
        ];

        let converted = convert_range_from_expected_cnt(&ranges, 5.0, false);

        assert_eq!(converted.skipped_rows(), 3.0);
        assert!(!converted.is_full_scan());
        let range = converted.converted_range().expect("a prefix is required");
        assert_eq!(range.low_val, [Datum::Null]);
        assert_eq!(range.high_val, [Datum::Int(30)]);
        assert!(!range.low_exclude);
        assert!(!range.high_exclude);
        assert_eq!(range.collators, [Collation::Binary]);
    }

    #[test]
    fn descending_scan_uses_source_range_before_expected_count_is_reached() {
        let ranges = [
            counted(10, 20, 3.0, false, true),
            counted(30, 40, 4.0, true, false),
            counted(50, 60, 9.0, false, true),
        ];

        let converted = convert_range_from_expected_cnt(&ranges, 12.0, true);

        assert_eq!(converted.skipped_rows(), 9.0);
        assert!(!converted.is_full_scan());
        let range = converted.converted_range().expect("a prefix is required");
        assert_eq!(range.low_val, [Datum::Int(40)]);
        assert_eq!(range.high_val, [Datum::MaxValue]);
        assert!(range.low_exclude);
        assert!(!range.high_exclude);
        assert_eq!(range.collators, [Collation::Binary]);
    }

    #[test]
    fn insufficient_range_rows_select_the_full_scan_and_clear_skipped_count() {
        let ranges = [
            counted(10, 20, 3.0, false, false),
            counted(30, 40, 4.0, false, false),
        ];

        let converted = convert_range_from_expected_cnt(&ranges, 8.0, false);

        assert!(converted.is_full_scan());
        assert!(converted.converted_range().is_none());
        assert_eq!(converted.skipped_rows(), 0.0);
    }

    fn correlated_fixture() -> (HistColl, Column, Vec<Expression>) {
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let column = Column::new(7, field_type.clone());
        let filter = Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("eq"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![
                Expression::Column(column.clone()),
                Expression::Constant(Constant::new(Datum::Int(50), field_type)),
            ],
        ));
        let stats = HistColl::new(false, 100, []).with_histograms([(
            7,
            Arc::new(ColumnStats {
                histogram: Histogram {
                    id: 7,
                    ndv: 100,
                    correlation: 0.95,
                    buckets: vec![tidb_stats::Bucket {
                        count: 100,
                        repeat: 1,
                        ndv: 100,
                        lower_bound: Datum::Int(1),
                        upper_bound: Datum::Int(100),
                    }],
                    ..Histogram::default()
                },
                topn: None,
                cms: None,
                stats_ver: 2,
                unsigned: false,
            }),
        )]);
        (stats, column, vec![filter])
    }

    #[test]
    fn table_cross_estimate_uses_histogram_and_flips_negative_correlation_direction() {
        let (stats, column, filters) = correlated_fixture();
        let ascending = estimate_table_cross_row_count(
            &stats,
            &stats,
            &filters,
            &column,
            0.95,
            1.0,
            100.0,
            false,
            0,
            &crate::ranger::points::evaluate_static,
            EstimatorOptions::default(),
        );
        let negative_correlation = estimate_table_cross_row_count(
            &stats,
            &stats,
            &filters,
            &column,
            -0.95,
            1.0,
            100.0,
            true,
            0,
            &crate::ranger::points::evaluate_static,
            EstimatorOptions::default(),
        );

        assert!(ascending.1);
        assert_eq!(ascending.2, 0.0);
        assert!(ascending.0 > 1.0 && ascending.0 < 100.0);
        assert_eq!(negative_correlation, ascending);
    }

    #[test]
    fn table_cross_estimate_scales_for_filters_left_after_column_detachment() {
        let (stats, column, mut filters) = correlated_fixture();
        let other = Column::new(8, tidb_datatype::FieldType::new(FieldTypeCode::LongLong));
        filters.push(Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new("eq"),
            FieldType::new(FieldTypeCode::LongLong),
            vec![
                Expression::Column(other),
                Expression::Constant(Constant::new(
                    Datum::Int(1),
                    FieldType::new(FieldTypeCode::LongLong),
                )),
            ],
        )));
        let without_residual = estimate_table_cross_row_count(
            &stats,
            &stats,
            &filters[..1],
            &column,
            0.95,
            1.0,
            100.0,
            false,
            0,
            &crate::ranger::points::evaluate_static,
            EstimatorOptions::default(),
        );
        let with_residual = estimate_table_cross_row_count(
            &stats,
            &stats,
            &filters,
            &column,
            0.95,
            1.0,
            100.0,
            false,
            0,
            &crate::ranger::points::evaluate_static,
            EstimatorOptions::default(),
        );

        assert!(without_residual.1 && with_residual.1);
        assert!((with_residual.0 - without_residual.0 / 0.8).abs() < 1e-9);
    }

    #[test]
    fn table_cross_estimate_keeps_correlation_when_statistics_are_unusable() {
        let (_, column, filters) = correlated_fixture();
        let pseudo = HistColl::new(true, 100, []);
        let result = estimate_table_cross_row_count(
            &pseudo,
            &pseudo,
            &filters,
            &column,
            0.95,
            1.0,
            100.0,
            false,
            0,
            &crate::ranger::points::evaluate_static,
            EstimatorOptions::default(),
        );
        assert_eq!(result, (0.0, false, 0.95));
    }

    #[test]
    fn table_cross_estimate_does_not_skip_the_first_leading_index() {
        use crate::cardinality::row_count_estimator::{IndexRowCounts, IndexStats};

        let (column_stats, column, filters) = correlated_fixture();
        let source_stats = HistColl::new(false, 100, []).with_column_index_ids([(7, 11), (7, 4)]);
        let later_index = IndexStats {
            histogram: Histogram {
                id: 11,
                ndv: 100,
                buckets: vec![tidb_stats::Bucket {
                    count: 100,
                    repeat: 1,
                    ndv: 100,
                    lower_bound: Datum::Bytes(b"a".to_vec()),
                    upper_bound: Datum::Bytes(b"z".to_vec()),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            num_columns: 1,
            unique: false,
        };
        // Go uses the first ID in ColUniqueID2IdxIDs. If it is absent or
        // unusable, it falls back to the correlation heuristic; it does not
        // try a later index or switch to the column histogram.
        let table_stats = HistColl::new(false, 100, [])
            .with_histograms([(7, column_stats.histogram(7).unwrap().clone())])
            .with_index_histograms([(11, Arc::new(later_index))])
            .with_index_columns([(11, vec![7])])
            .with_index_row_counts([(11, IndexRowCounts::unscaled(100, 0))]);

        let estimate = estimate_table_cross_row_count(
            &source_stats,
            &table_stats,
            &filters,
            &column,
            0.95,
            1.0,
            100.0,
            false,
            0,
            &crate::ranger::points::evaluate_static,
            EstimatorOptions::default(),
        );

        assert_eq!(estimate, (0.0, false, 0.95));
        let later_index_is_first = HistColl::new(false, 100, []).with_column_index_ids([(7, 11)]);
        let estimate = estimate_table_cross_row_count(
            &later_index_is_first,
            &table_stats,
            &filters,
            &column,
            0.95,
            1.0,
            100.0,
            false,
            0,
            &crate::ranger::points::evaluate_static,
            EstimatorOptions::default(),
        );
        assert!(
            estimate.1,
            "the later index fixture must be usable on its own"
        );
    }
}
