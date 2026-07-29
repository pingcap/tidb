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

//! The statistics-backed row-count estimator, from
//! `pkg/planner/cardinality/row_count_column.go` and `row_count_index.go`.
//!
//! The leaves next to this module (`row_count_column`, `uniform`, `pseudo`,
//! `out_of_range`) port the *arithmetic* of these two Go files against
//! caller-supplied numbers. This module is the layer that owns the actual
//! statistics: it walks real [`Histogram`]/[`TopN`]/[`CmsSketch`] values, so
//! it is what decides which of those numbers each estimate is built from --
//! and that decision, in particular the `equalRowCount` ordering, is where
//! divergence would be silent:
//!
//! 1. **TopN first** (stats version 2). A TopN hit is an exact count.
//! 2. **CMSketch** (stats version 1 only), queried through the value
//!    encoding, never the key encoding.
//! 3. **The histogram**: a bucket's `repeat` for its upper bound, its
//!    per-bucket NDV otherwise.
//! 4. **Uniform distribution** for everything left, which is also where an
//!    out-of-range value lands under version 2 -- never zero.
//!
//! What is deliberately *not* here: `HistColl` itself (the caller passes the
//! column/index statistics it resolved), expression-to-range extraction, the
//! MV-index paths, and `expBackoffEstimation`'s fallback of recursing into a
//! *different* index when a column has no statistics (that needs the
//! collection-level column-to-index map). Each is called out at its use site.

use tidb_codec::encode_key;
use tidb_datatype::{Collation, Datum};
use tidb_stats::cmsketch::{CmsSketch, TopN};
use tidb_stats::histogram::{Histogram, OutOfRangeContext};

use super::pseudo::{
    pseudo_row_count_by_scalar_ranges, pseudo_row_count_by_signed_int_ranges, PseudoBoundKind,
    ScalarRange, SignedIntRange,
};
use super::row_count_column::RowEstimate;
use super::uniform::{estimate_uniform_equality, UniformEqualityStats};
use super::{apply_exponential_backoff, go_max, go_min, MAX_EXPONENTIAL_BACKOFF_COLS};

/// Go `statistics.Version1`.
pub const VERSION1: i64 = 1;
/// Go `statistics.Version2`.
pub const VERSION2: i64 = 2;

/// Go `cost.ToleranceFactor`.
pub const TOLERANCE_FACTOR: f64 = 0.00001;

/// Go `staleLastBucketThreshold`.
const STALE_LAST_BUCKET_THRESHOLD: f64 = 0.3;
/// Go `valueAwareRowAddedThreshold`.
const VALUE_AWARE_ROW_ADDED_THRESHOLD: f64 = 0.5;

/// Go `maxNumStep`, the largest range width converted to point estimates.
const MAX_NUM_STEP: i64 = 10;

/// The session inputs the source estimator reads out of `PlanContext`.
///
/// Defaults match a session that has not touched the risk variables:
/// no skew adjustment, and real-time statistics allowed (that is, an
/// optimizer objective other than `Determinate`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EstimatorOptions {
    /// `tidb_opt_risk_eq_skew_ratio`.
    pub risk_eq_skew_ratio: f64,
    /// `tidb_opt_risk_range_skew_ratio`.
    pub risk_range_skew_ratio: f64,
    /// False under `OptObjectiveDeterminate`, which bans modify-count use.
    pub allow_use_modify_count: bool,
}

impl Default for EstimatorOptions {
    fn default() -> Self {
        Self {
            risk_eq_skew_ratio: 0.0,
            risk_range_skew_ratio: 0.0,
            allow_use_modify_count: true,
        }
    }
}

/// One column's statistics, Go `statistics.Column` reduced to the fields the
/// estimator reads.
#[derive(Clone, Debug)]
pub struct ColumnStats {
    /// The column histogram.
    pub histogram: Histogram,
    /// TopN, present for stats version 2.
    pub topn: Option<TopN>,
    /// CMSketch, present for stats version 1.
    pub cms: Option<CmsSketch>,
    /// `stats_ver` this column was analyzed at.
    pub stats_ver: i64,
    /// Whether the column values are unsigned, for out-of-range scaling.
    pub unsigned: bool,
}

/// One index's statistics, Go `statistics.Index` reduced the same way.
#[derive(Clone, Debug)]
pub struct IndexStats {
    /// The index histogram, whose bounds are index-key bytes.
    pub histogram: Histogram,
    /// TopN over index-key bytes.
    pub topn: Option<TopN>,
    /// CMSketch, present for stats version 1.
    pub cms: Option<CmsSketch>,
    /// `stats_ver` this index was analyzed at.
    pub stats_ver: i64,
    /// Number of columns the index is declared over.
    pub num_columns: usize,
    /// Whether the index is unique.
    pub unique: bool,
}

/// One single-column range, Go `ranger.Range` at one column.
#[derive(Clone, Debug)]
pub struct ColumnRange {
    /// Inclusive-or-exclusive low bound.
    pub low: Datum,
    /// Inclusive-or-exclusive high bound.
    pub high: Datum,
    /// Whether the low bound is excluded.
    pub low_exclude: bool,
    /// Whether the high bound is excluded.
    pub high_exclude: bool,
}

impl ColumnRange {
    /// A closed point range on one value.
    #[must_use]
    pub fn point(value: Datum) -> Self {
        Self {
            low: value.clone(),
            high: value,
            low_exclude: false,
            high_exclude: false,
        }
    }

    /// A range with explicit bounds and exclusivity.
    #[must_use]
    pub const fn new(low: Datum, high: Datum, low_exclude: bool, high_exclude: bool) -> Self {
        Self {
            low,
            high,
            low_exclude,
            high_exclude,
        }
    }
}

/// One multi-column index range.
#[derive(Clone, Debug)]
pub struct IndexRangeDatums {
    /// Low bound, one datum per accessed index column.
    pub low: Vec<Datum>,
    /// High bound, same length as [`Self::low`].
    pub high: Vec<Datum>,
    /// Whether the low bound is excluded.
    pub low_exclude: bool,
    /// Whether the high bound is excluded.
    pub high_exclude: bool,
}

fn topn_num(topn: Option<&TopN>) -> usize {
    topn.map_or(0, TopN::num)
}

fn topn_total_count(topn: Option<&TopN>) -> u64 {
    topn.map_or(0, TopN::total_count)
}

impl ColumnStats {
    /// Go `Column.TotalRowCount`: version 2 folds TopN back in.
    #[must_use]
    pub fn total_row_count(&self) -> f64 {
        if self.stats_ver >= VERSION2 {
            self.histogram.total_row_count() + topn_total_count(self.topn.as_ref()) as f64
        } else {
            self.histogram.total_row_count()
        }
    }

    /// Go `Column.NotNullCount`.
    #[must_use]
    pub fn not_null_count(&self) -> f64 {
        if self.stats_ver >= VERSION2 {
            self.histogram.not_null_count() + topn_total_count(self.topn.as_ref()) as f64
        } else {
            self.histogram.not_null_count()
        }
    }

    /// Go `Column.GetIncreaseFactor`.
    #[must_use]
    pub fn increase_factor(&self, realtime_row_count: i64) -> f64 {
        let column_count = self.total_row_count();
        if column_count == 0.0 {
            return 1.0;
        }
        realtime_row_count as f64 / column_count
    }
}

impl IndexStats {
    /// Go `Index.TotalRowCount`.
    #[must_use]
    pub fn total_row_count(&self) -> f64 {
        if self.stats_ver >= VERSION2 {
            self.histogram.total_row_count() + topn_total_count(self.topn.as_ref()) as f64
        } else {
            self.histogram.total_row_count()
        }
    }

    /// Go `Index.GetIncreaseFactor`.
    #[must_use]
    pub fn increase_factor(&self, realtime_row_count: i64) -> f64 {
        let column_count = self.total_row_count();
        if column_count == 0.0 {
            return 1.0;
        }
        realtime_row_count as f64 / column_count
    }
}

fn to_planner_est(source: tidb_stats::RowEstimate) -> RowEstimate {
    RowEstimate::new(source.est, source.min_est, source.max_est)
}

/// Go `outOfRangeEQSelectivity`, re-exported at this module's boundary.
fn out_of_range_eq_selectivity(ndv: i64, realtime: i64, analyzed: i64) -> f64 {
    super::out_of_range::out_of_range_eq_selectivity(ndv, realtime, analyzed)
}

/// Go `IsLastBucketEndValueUnderrepresented`.
///
/// Concentrated writes after `ANALYZE` leave the final bucket's upper bound
/// with a repeat count that no longer describes the table; when that is
/// likely, the caller must not trust the histogram's exact count.
#[must_use]
pub fn is_last_bucket_end_value_underrepresented(
    histogram: &Histogram,
    value: &Datum,
    collation: Collation,
    hist_count: f64,
    hist_ndv: f64,
    realtime_row_count: i64,
    modify_count: i64,
) -> bool {
    if modify_count <= 0 || histogram.is_empty() || hist_ndv <= 0.0 {
        return false;
    }
    let new_rows_added = histogram.abs_row_count_difference(realtime_row_count);
    let avg_value_count = histogram.not_null_count() / hist_ndv;
    if new_rows_added < avg_value_count * VALUE_AWARE_ROW_ADDED_THRESHOLD {
        return false;
    }
    let location = histogram.locate_bucket(value, collation);
    let is_last_bucket_end_value = location.bucket_idx == histogram.len() - 1
        && location.in_bucket
        && location.match_last_value;
    if !is_last_bucket_end_value {
        return false;
    }
    hist_count < avg_value_count * STALE_LAST_BUCKET_THRESHOLD
}

fn uniform_estimate(
    histogram: &Histogram,
    topn: Option<&TopN>,
    total_row_count: f64,
    increase_factor: f64,
    realtime_row_count: i64,
    modify_count: i64,
    options: EstimatorOptions,
) -> RowEstimate {
    estimate_uniform_equality(UniformEqualityStats {
        histogram_ndv: histogram.ndv,
        topn_len: topn_num(topn),
        total_row_count,
        not_null_count: histogram.not_null_count(),
        null_count: histogram.null_count as f64,
        realtime_row_count: realtime_row_count as f64,
        increase_factor,
        modify_count,
        risk_eq_skew_ratio: options.risk_eq_skew_ratio,
        topn_min_count: topn.map(|topn| topn.min_count() as f64),
    })
}

/// Estimates the rows where a column equals `value`, Go
/// `equalRowCountOnColumn`.
///
/// `encoded_value` is the `codec.EncodeKey` form of `value`, which is the
/// domain TopN entries live in. The CMSketch branch re-encodes internally
/// because Go queries a sketch through `tablecodec.EncodeValue` instead.
#[must_use]
pub fn equal_row_count_on_column(
    column: &ColumnStats,
    value: &Datum,
    encoded_value: &[u8],
    collation: Collation,
    realtime_row_count: i64,
    modify_count: i64,
    options: EstimatorOptions,
) -> RowEstimate {
    if value.is_null() {
        return RowEstimate::default_est(column.histogram.null_count as f64);
    }
    let histogram = &column.histogram;

    if column.stats_ver < VERSION2 {
        if histogram.is_empty() {
            return RowEstimate::default_est(0.0);
        }
        if histogram.ndv > 0 && histogram.out_of_range(value, collation) {
            let total = column.total_row_count();
            let selectivity =
                out_of_range_eq_selectivity(histogram.ndv, realtime_row_count, total as i64);
            return RowEstimate::default_est(selectivity * total);
        }
        if let Some(cms) = column.cms.as_ref() {
            let count = cms
                .query_integer_datum(column.topn.as_ref(), value)
                .unwrap_or(0);
            return RowEstimate::default_est(count as f64);
        }
        let (hist_count, _) = histogram.equal_row_count(value, false, collation);
        return RowEstimate::default_est(hist_count);
    }

    // Stats version 2.
    if histogram.is_empty() && topn_num(column.topn.as_ref()) == 0 {
        return RowEstimate::default_est(0.0);
    }
    // 1. TopN is exact.
    if let Some(topn) = column.topn.as_ref() {
        if let Some(count) = topn.query_bytes(encoded_value) {
            return RowEstimate::default_est(count as f64);
        }
    }
    // 2. Bucket repeat / bucket NDV.
    let (hist_count, matched) = histogram.equal_row_count(value, true, collation);
    let hist_ndv = (histogram.ndv - topn_num(column.topn.as_ref()) as i64) as f64;
    if matched
        && !is_last_bucket_end_value_underrepresented(
            histogram,
            value,
            collation,
            hist_count,
            hist_ndv,
            realtime_row_count,
            modify_count,
        )
    {
        return RowEstimate::default_est(hist_count);
    }
    // 3. Uniform distribution, which is also where out-of-range values land.
    uniform_estimate(
        histogram,
        column.topn.as_ref(),
        column.total_row_count(),
        column.increase_factor(realtime_row_count),
        realtime_row_count,
        modify_count,
        options,
    )
}

/// Estimates the rows in `[low, high)`, Go `betweenRowCountOnColumn`.
#[must_use]
pub fn between_row_count_on_column(
    column: &ColumnStats,
    low: &Datum,
    high: &Datum,
    low_encoded: &[u8],
    high_encoded: &[u8],
    collation: Collation,
    options: EstimatorOptions,
) -> RowEstimate {
    // The source always has a session here (only the version-1 *index* helper
    // documents a nil one), so the same-bucket skew branch runs at both stats
    // versions -- it widens `MaxEst` even with a zero skew ratio.
    let mut result = to_planner_est(column.histogram.between_row_count(
        low,
        high,
        collation,
        Some(options.risk_range_skew_ratio),
    ));
    if column.stats_ver <= VERSION1 {
        return result;
    }
    let topn_count = column
        .topn
        .as_ref()
        .map_or(0, |topn| topn.between_count(low_encoded, high_encoded));
    // Only the default estimate takes the TopN rows; the min/max stay the
    // histogram's, matching the source.
    result.est += topn_count as f64;
    result
}

/// Go `statistics.EnumRangeValues`, restricted to the integer kinds.
///
/// Non-integer kinds (durations, times) return `None` here rather than the
/// source's enumerated steps: the estimator's callers in this crate only
/// build integer ranges so far, and a partial time-step port would be a
/// silent wrong answer rather than an obvious missing one.
#[must_use]
pub fn enum_range_values(
    low: &Datum,
    high: &Datum,
    low_exclude: bool,
    high_exclude: bool,
) -> Option<Vec<Datum>> {
    let exclude = i64::from(low_exclude) + i64::from(high_exclude);
    match (low, high) {
        (Datum::Int(low_val), Datum::Int(high_val)) => {
            let (low_val, high_val) = (*low_val, *high_val);
            if low_val <= 0 && high_val >= 0 && (low_val < -MAX_NUM_STEP || high_val > MAX_NUM_STEP)
            {
                return None;
            }
            let remaining = high_val.checked_sub(low_val)?;
            if remaining > MAX_NUM_STEP {
                return None;
            }
            let remaining = remaining + 1 - exclude;
            if !(0..MAX_NUM_STEP).contains(&remaining) {
                return None;
            }
            let start = if low_exclude { low_val + 1 } else { low_val };
            Some((0..remaining).map(|i| Datum::Int(start + i)).collect())
        }
        (Datum::UInt(low_val), Datum::UInt(high_val)) => {
            let (low_val, high_val) = (*low_val, *high_val);
            let remaining = high_val.wrapping_sub(low_val);
            if remaining > MAX_NUM_STEP as u64 {
                return None;
            }
            let remaining = (remaining + 1).wrapping_sub(exclude as u64);
            if remaining >= MAX_NUM_STEP as u64 {
                return None;
            }
            let start = if low_exclude { low_val + 1 } else { low_val };
            Some((0..remaining).map(|i| Datum::UInt(start + i)).collect())
        }
        _ => None,
    }
}

fn encode_datum(value: &Datum) -> Vec<u8> {
    encode_key(std::slice::from_ref(value)).unwrap_or_default()
}

fn encode_datums(values: &[Datum]) -> Vec<u8> {
    encode_key(values).unwrap_or_default()
}

/// Go `kv.Key.PrefixNext`.
fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut buf = key.to_vec();
    for index in (0..buf.len()).rev() {
        buf[index] = buf[index].wrapping_add(1);
        if buf[index] != 0 {
            return buf;
        }
    }
    let mut buf = key.to_vec();
    buf.push(0);
    buf
}

/// Estimates a column's row count over `ranges`, Go `getColumnRowCount`.
#[must_use]
pub fn get_column_row_count(
    column: &ColumnStats,
    ranges: &[ColumnRange],
    collation: Collation,
    realtime_row_count: i64,
    modify_count: i64,
    pk_is_handle: bool,
    options: EstimatorOptions,
) -> RowEstimate {
    let mut total = RowEstimate::default_est(0.0);
    let increase_factor = column.increase_factor(realtime_row_count);

    for range in ranges {
        let low_encoded = encode_datum(&range.low);
        let high_encoded = encode_datum(&range.high);
        let equal_bounds = range
            .low
            .compare(&range.high, Collation::Binary)
            .is_ok_and(|ordering| ordering == std::cmp::Ordering::Equal);

        if equal_bounds {
            // Case 1: a point.
            if !range.low_exclude && !range.high_exclude {
                if pk_is_handle {
                    total.add_all(1.0);
                    continue;
                }
                let mut count = equal_row_count_on_column(
                    column,
                    &range.low,
                    &low_encoded,
                    collation,
                    realtime_row_count,
                    modify_count,
                    options,
                );
                count.multiply_all(increase_factor);
                total.add(count);
            }
            continue;
        }

        // Case 2: a small range under version 1, where the CMSketch gives a
        // better answer per point than the histogram does for the interval.
        if column.stats_ver < VERSION2 {
            if let Some(values) = enum_range_values(
                &range.low,
                &range.high,
                range.low_exclude,
                range.high_exclude,
            ) {
                for value in &values {
                    // The source passes the *low* bound's encoding for every
                    // enumerated value; only the version-2 TopN branch reads
                    // it, so the version-1 path here is unaffected.
                    let mut count = equal_row_count_on_column(
                        column,
                        value,
                        &low_encoded,
                        collation,
                        realtime_row_count,
                        modify_count,
                        options,
                    );
                    count.multiply_all(increase_factor);
                    total.add(count);
                }
                continue;
            }
        }

        // Case 3: an interval.
        let mut count = between_row_count_on_column(
            column,
            &range.low,
            &range.high,
            &low_encoded,
            &high_encoded,
            collation,
            options,
        );
        if range.low_exclude
            && !range.low.is_null()
            && !matches!(range.low, Datum::MaxValue | Datum::MinNotNull)
        {
            let low_count = equal_row_count_on_column(
                column,
                &range.low,
                &low_encoded,
                collation,
                realtime_row_count,
                modify_count,
                options,
            );
            count.subtract(low_count);
            count.clamp(0.0, column.not_null_count());
        }
        if !range.low_exclude && range.low.is_null() {
            count.add_all(column.histogram.null_count as f64);
        }
        if !range.high_exclude && !matches!(range.high, Datum::MaxValue | Datum::MinNotNull) {
            let high_count = equal_row_count_on_column(
                column,
                &range.high,
                &high_encoded,
                collation,
                realtime_row_count,
                modify_count,
                options,
            );
            count.add(high_count);
        }
        count.clamp(0.0, realtime_row_count as f64);
        count.multiply_all(increase_factor);

        let at_full_range = count.est >= realtime_row_count as f64 * (1.0 - TOLERANCE_FACTOR);
        let out_of_range = (column.histogram.out_of_range(&range.low, collation)
            && !range.low.is_null())
            || column.histogram.out_of_range(&range.high, collation);
        if !at_full_range && out_of_range {
            let mut hist_ndv = column.histogram.ndv;
            if column.stats_ver == VERSION2 {
                hist_ndv -= topn_num(column.topn.as_ref()) as i64;
            }
            count.add(to_planner_est(column.histogram.out_of_range_row_count(
                &range.low,
                &range.high,
                OutOfRangeContext {
                    realtime_row_count,
                    modify_count,
                    hist_ndv,
                    unsigned: column.unsigned,
                    allow_use_modify_count: options.allow_use_modify_count,
                    skew_ratio: options.risk_range_skew_ratio,
                },
            )));
        }
        total.add(count);
    }

    total.clamp(1.0, realtime_row_count as f64);
    total
}

/// Estimates a column's row count, falling back to pseudo statistics when the
/// column has none. Go `GetRowCountByColumnRanges`.
#[must_use]
pub fn get_row_count_by_column_ranges(
    column: Option<&ColumnStats>,
    ranges: &[ColumnRange],
    collation: Collation,
    realtime_row_count: i64,
    modify_count: i64,
    pk_is_handle: bool,
    options: EstimatorOptions,
) -> RowEstimate {
    let Some(column) = column else {
        return RowEstimate::default_est(pseudo_row_count(
            ranges,
            realtime_row_count,
            pk_is_handle,
        ));
    };
    get_column_row_count(
        column,
        ranges,
        collation,
        realtime_row_count,
        modify_count,
        pk_is_handle,
        options,
    )
}

fn bound_kind(value: &Datum) -> PseudoBoundKind {
    match value {
        Datum::Null => PseudoBoundKind::Null,
        Datum::MinNotNull => PseudoBoundKind::MinNotNull,
        Datum::MaxValue => PseudoBoundKind::MaxValue,
        _ => PseudoBoundKind::Value,
    }
}

fn datum_scalar(value: &Datum) -> f64 {
    match value {
        Datum::Int(v) => *v as f64,
        Datum::UInt(v) => *v as f64,
        Datum::Real(v) | Datum::Float32(v) => *v,
        _ => 0.0,
    }
}

fn datum_int(value: &Datum) -> i64 {
    match value {
        Datum::Int(v) => *v,
        Datum::UInt(v) => *v as i64,
        _ => 0,
    }
}

fn pseudo_row_count(ranges: &[ColumnRange], realtime_row_count: i64, pk_is_handle: bool) -> f64 {
    let table_row_count = realtime_row_count as f64;
    if pk_is_handle {
        if ranges.is_empty() {
            return 0.0;
        }
        // Only the signed branch is reachable here: the source picks it from
        // the first range's `KindInt64` low bound.
        if matches!(ranges[0].low, Datum::Int(_)) {
            let signed: Vec<SignedIntRange> = ranges
                .iter()
                .map(|range| {
                    SignedIntRange::new(
                        datum_int(&range.low),
                        datum_int(&range.high),
                        bound_kind(&range.low),
                        bound_kind(&range.high),
                    )
                })
                .collect();
            return pseudo_row_count_by_signed_int_ranges(&signed, table_row_count);
        }
    }
    let scalar: Vec<ScalarRange> = ranges
        .iter()
        .map(|range| {
            ScalarRange::new(
                datum_scalar(&range.low),
                datum_scalar(&range.high),
                bound_kind(&range.low),
                bound_kind(&range.high),
            )
        })
        .collect();
    pseudo_row_count_by_scalar_ranges(&scalar, table_row_count)
}

/// Estimates the rows where an index key equals `encoded`, Go
/// `equalRowCountOnIndex`. `encoded` is `codec.EncodeKey` output.
#[must_use]
pub fn equal_row_count_on_index(
    index: &IndexStats,
    encoded: &[u8],
    realtime_row_count: i64,
    modify_count: i64,
    options: EstimatorOptions,
) -> RowEstimate {
    if index.num_columns == 1 && encoded == null_key_bytes().as_slice() {
        return RowEstimate::default_est(index.histogram.null_count as f64);
    }
    let value = Datum::Bytes(encoded.to_vec());
    let histogram = &index.histogram;

    if index.stats_ver < VERSION2 {
        if histogram.ndv > 0 && out_of_range_on_index(index, &value) {
            let total = index.total_row_count();
            let selectivity =
                out_of_range_eq_selectivity(histogram.ndv, realtime_row_count, total as i64);
            return RowEstimate::default_est(selectivity * total);
        }
        if let Some(cms) = index.cms.as_ref() {
            return RowEstimate::default_est(
                cms.query_with_topn(index.topn.as_ref(), encoded) as f64
            );
        }
        let (hist_count, _) = histogram.equal_row_count(&value, false, Collation::Binary);
        return RowEstimate::default_est(hist_count);
    }

    if let Some(topn) = index.topn.as_ref() {
        if let Some(count) = topn.query_bytes(encoded) {
            return RowEstimate::default_est(count as f64);
        }
    }
    let (hist_count, matched) = histogram.equal_row_count(&value, true, Collation::Binary);
    let hist_ndv = (histogram.ndv - topn_num(index.topn.as_ref()) as i64) as f64;
    if matched
        && !is_last_bucket_end_value_underrepresented(
            histogram,
            &value,
            Collation::Binary,
            hist_count,
            hist_ndv,
            realtime_row_count,
            modify_count,
        )
    {
        return RowEstimate::default_est(hist_count);
    }
    uniform_estimate(
        histogram,
        index.topn.as_ref(),
        index.total_row_count(),
        index.increase_factor(realtime_row_count),
        realtime_row_count,
        modify_count,
        options,
    )
}

/// Go `betweenRowCountOnIndex`, for the half-open key interval `[l, r)`.
#[must_use]
pub fn between_row_count_on_index(
    index: &IndexStats,
    left: &[u8],
    right: &[u8],
    options: EstimatorOptions,
) -> RowEstimate {
    let l = Datum::Bytes(left.to_vec());
    let r = Datum::Bytes(right.to_vec());
    let mut result = to_planner_est(index.histogram.between_row_count(
        &l,
        &r,
        Collation::Binary,
        Some(options.risk_range_skew_ratio),
    ));
    if index.stats_ver == VERSION1 {
        return result;
    }
    let topn_count = index
        .topn
        .as_ref()
        .map_or(0, |topn| topn.between_count(left, right));
    result.add_all(topn_count as f64);
    result
}

/// Go's package-level `nullKeyBytes`.
fn null_key_bytes() -> Vec<u8> {
    encode_datum(&Datum::Null)
}

/// Go `outOfRangeOnIndex`. The prefix check only fires for string bounds,
/// which index-key bounds never are once encoded, so it reduces to the
/// histogram's own range test here.
fn out_of_range_on_index(index: &IndexStats, value: &Datum) -> bool {
    index.histogram.out_of_range(value, Collation::Binary)
}

/// Go `getOrdinalOfRangeCond`: the first index column whose bounds differ.
#[must_use]
pub fn ordinal_of_range_cond(range: &IndexRangeDatums) -> usize {
    for (index, low) in range.low.iter().enumerate() {
        let Some(high) = range.high.get(index) else {
            return index;
        };
        match low.compare(high, Collation::Binary) {
            Ok(std::cmp::Ordering::Equal) => {}
            Ok(_) => return index,
            Err(_) => return 0,
        }
    }
    range.low.len()
}

/// The per-column statistics an index range walk needs, in index order.
///
/// A `None` entry is a column without usable statistics. The source then
/// tries the *other* indexes covering that column before giving up; that
/// fallback needs `HistColl.ColUniqueID2IdxIDs`, which no caller in this
/// crate has yet, so a `None` column is simply skipped here the way the
/// source skips a column it found nothing for.
pub type IndexColumnStats<'a> = Vec<Option<&'a ColumnStats>>;

/// Go `expBackoffEstimation`.
///
/// Returns `None` when the source reports `success = false` -- no column
/// contributed an estimate -- in which case the caller falls back to the
/// index histogram's own interval estimate.
#[must_use]
pub fn exp_backoff_estimation(
    index: &IndexStats,
    columns: &IndexColumnStats<'_>,
    range: &IndexRangeDatums,
    realtime_row_count: i64,
    modify_count: i64,
    options: EstimatorOptions,
) -> Option<(f64, f64, f64)> {
    let mut single_column_results = Vec::with_capacity(range.low.len());
    let mut min_sel = 1.0_f64;
    let mut max_sel = 1.0_f64;

    for position in 0..range.low.len() {
        let last = position == range.low.len() - 1;
        let column_range = ColumnRange {
            low: range.low[position].clone(),
            high: range.high[position].clone(),
            low_exclude: last && range.low_exclude,
            high_exclude: last && range.high_exclude,
        };
        let Some(Some(column)) = columns.get(position) else {
            continue;
        };
        let count = get_column_row_count(
            column,
            std::slice::from_ref(&column_range),
            Collation::Binary,
            realtime_row_count,
            modify_count,
            false,
            options,
        );
        let selectivity = count.est / realtime_row_count as f64;
        max_sel = go_min(max_sel, count.max_est / realtime_row_count as f64);
        single_column_results.push(selectivity);
        min_sel *= selectivity;
    }

    single_column_results.sort_by(f64::total_cmp);
    let len = single_column_results.len();
    if len == 1 {
        let only = single_column_results[0];
        return Some((only, only, only));
    }
    if len == 0 {
        return None;
    }

    let hist_ndv = if index.histogram.ndv > 0 {
        index.histogram.ndv
    } else {
        realtime_row_count
    };
    let mut idx_low_bound = 1.0 / hist_ndv.min(realtime_row_count) as f64;
    let mut min_bound = idx_low_bound;
    if len < index.num_columns {
        idx_low_bound /= 0.9;
    }
    max_sel = go_max(idx_low_bound, max_sel);
    min_sel = go_max(min_bound, min_sel);

    let max_cols = MAX_EXPONENTIAL_BACKOFF_COLS.min(len);
    for value in single_column_results.iter().take(max_cols) {
        min_bound = go_min(min_bound, *value);
    }
    let result = apply_exponential_backoff(&single_column_results, min_bound, 1.0);
    Some((result, min_sel, max_sel))
}

/// Estimates an index's row count over `ranges` under stats version 2, Go
/// `getIndexRowCountForStatsV2`.
#[must_use]
pub fn get_index_row_count_for_stats_v2(
    index: &IndexStats,
    columns: &IndexColumnStats<'_>,
    ranges: &[IndexRangeDatums],
    realtime_row_count: i64,
    modify_count: i64,
    options: EstimatorOptions,
) -> RowEstimate {
    let mut total = RowEstimate::default_est(0.0);
    let is_single_col_idx = index.num_columns == 1;
    let null_key = null_key_bytes();

    for range in ranges {
        let mut count = RowEstimate::default_est(0.0);
        let mut lb = encode_datums(&range.low);
        let mut rb = encode_datums(&range.high);
        let full_len = range.low.len() == range.high.len() && range.low.len() == index.num_columns;

        if lb == rb {
            // Case 1: a point.
            if range.low_exclude || range.high_exclude {
                continue;
            }
            if full_len {
                if index.unique {
                    let only_null = range.low.iter().all(Datum::is_null);
                    if !only_null {
                        total.add_all(1.0);
                    } else {
                        total = RowEstimate::default_est(index.histogram.null_count as f64);
                    }
                    continue;
                }
                let mut point =
                    equal_row_count_on_index(index, &lb, realtime_row_count, modify_count, options);
                point.multiply_all(index.increase_factor(realtime_row_count));
                total.add(point);
                continue;
            }
        }

        // Case 2: an interval, normalized to [low, high).
        if range.low_exclude {
            lb = prefix_next(&lb);
        }
        if !range.high_exclude {
            rb = prefix_next(&rb);
        }
        let low_is_null = lb == null_key;
        if is_single_col_idx && low_is_null {
            count.add_all(index.histogram.null_count as f64);
        }

        let mut exp_backoff_success = false;
        if ordinal_of_range_cond(range) > 0 && index.stats_ver >= VERSION2 {
            if let Some((sel, min_sel, max_sel)) = exp_backoff_estimation(
                index,
                columns,
                range,
                realtime_row_count,
                modify_count,
                options,
            ) {
                exp_backoff_success = true;
                let mut backoff = RowEstimate::new(sel, min_sel, max_sel);
                backoff.multiply_all(index.total_row_count());

                let mut upper_limit = backoff.est;
                if !index.histogram.is_empty() {
                    let lower_bkt = index
                        .histogram
                        .locate_bucket(&Datum::Bytes(lb.clone()), Collation::Binary)
                        .bucket_idx;
                    let upper_bkt = index
                        .histogram
                        .locate_bucket(&Datum::Bytes(rb.clone()), Collation::Binary)
                        .bucket_idx;
                    let pre_count = if lower_bkt > 0 {
                        index.histogram.buckets[lower_bkt - 1].count as f64
                    } else {
                        0.0
                    };
                    upper_limit = index.histogram.buckets[upper_bkt].count as f64 - pre_count;
                    upper_limit += index
                        .topn
                        .as_ref()
                        .map_or(0, |topn| topn.between_count(&lb, &rb))
                        as f64;
                }
                if backoff.est > upper_limit {
                    backoff.est = upper_limit;
                }
                count.add(backoff);
            }
        }
        if !exp_backoff_success {
            count.add(between_row_count_on_index(index, &lb, &rb, options));
        }

        count.multiply_all(index.increase_factor(realtime_row_count));

        let l = Datum::Bytes(lb.clone());
        let r = Datum::Bytes(rb.clone());
        let at_full_range = count.est >= realtime_row_count as f64 * (1.0 - TOLERANCE_FACTOR);
        let out_of_range = (out_of_range_on_index(index, &l)
            && !(is_single_col_idx && low_is_null))
            || out_of_range_on_index(index, &r);
        if !at_full_range && out_of_range {
            let mut hist_ndv = index.histogram.ndv;
            let single_col_range = range.low.len() == range.high.len() && range.low.len() == 1;
            let first_column = columns.first().copied().flatten();
            if index.stats_ver == VERSION2 {
                match first_column {
                    Some(column)
                        if single_col_range
                            && column.histogram.ndv > 0
                            && !column.histogram.is_empty() =>
                    {
                        // A single-column predicate estimates better against
                        // the column's own histogram: index bounds have been
                        // flattened to key bytes, the column's have not.
                        let column_ndv =
                            column.histogram.ndv - topn_num(column.topn.as_ref()) as i64;
                        count.add(to_planner_est(column.histogram.out_of_range_row_count(
                            &range.low[0],
                            &range.high[0],
                            OutOfRangeContext {
                                realtime_row_count,
                                modify_count,
                                hist_ndv: column_ndv,
                                unsigned: column.unsigned,
                                allow_use_modify_count: options.allow_use_modify_count,
                                skew_ratio: options.risk_range_skew_ratio,
                            },
                        )));
                    }
                    _ => {
                        hist_ndv -= topn_num(index.topn.as_ref()) as i64;
                        count.add(to_planner_est(index.histogram.out_of_range_row_count(
                            &l,
                            &r,
                            OutOfRangeContext {
                                realtime_row_count,
                                modify_count,
                                hist_ndv,
                                unsigned: false,
                                allow_use_modify_count: options.allow_use_modify_count,
                                skew_ratio: options.risk_range_skew_ratio,
                            },
                        )));
                    }
                }
            } else {
                count.add(to_planner_est(index.histogram.out_of_range_row_count(
                    &l,
                    &r,
                    OutOfRangeContext {
                        realtime_row_count,
                        modify_count,
                        hist_ndv,
                        unsigned: false,
                        allow_use_modify_count: options.allow_use_modify_count,
                        skew_ratio: options.risk_range_skew_ratio,
                    },
                )));
            }
        }

        total.add(count);
    }

    total.clamp(1.0, realtime_row_count as f64);
    total
}
