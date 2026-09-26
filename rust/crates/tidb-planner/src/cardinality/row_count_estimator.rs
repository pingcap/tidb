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
//! column/index statistics and alternate index histograms it resolved),
//! expression-to-range extraction, and the MV-index paths. Each is called
//! out at its use site.

use tidb_codec::encode_key;
use tidb_datatype::{Collation, Datum};
use tidb_stats::cmsketch::{CmsSketch, TopN};
use tidb_stats::histogram::{Histogram, OutOfRangeContext};
use tidb_stats::memory_usage::{ColumnMemUsage, IndexMemUsage};

use super::pseudo::{
    pseudo_equal_count, pseudo_row_count_by_scalar_ranges, pseudo_row_count_by_signed_int_ranges,
    pseudo_row_count_by_unsigned_int_ranges, PseudoBoundKind, ScalarRange, SignedIntRange,
    UnsignedIntRange,
};
use super::row_count_column::RowEstimate;
use super::uniform::{estimate_uniform_equality, UniformEqualityStats};
use super::{apply_exponential_backoff, go_max, go_min, MAX_EXPONENTIAL_BACKOFF_COLS};
use crate::cost_factors::TOLERANCE_FACTOR;

/// Go `statistics.Version1`.
pub const VERSION1: i64 = 1;
/// Go `statistics.Version2`.
pub const VERSION2: i64 = 2;

/// Go `staleLastBucketThreshold`.
const STALE_LAST_BUCKET_THRESHOLD: f64 = 0.3;
/// Go `valueAwareRowAddedThreshold`.
const VALUE_AWARE_ROW_ADDED_THRESHOLD: f64 = 0.5;

/// A failed range comparison or key encoding during cardinality estimation.
#[derive(Debug)]
pub enum EstimationError {
    /// A column value could not be flattened into its statistics encoding.
    TableValue(tidb_tablecodec::TableRowError),
    /// The range endpoints cannot be compared in their datum domain.
    Comparison(tidb_datatype::DatumValueError),
    /// A range endpoint cannot be encoded as a statistics key.
    Codec(tidb_codec::CodecError),
    /// Range construction failed before an estimate could be formed.
    Range(crate::ranger::points::PointBuilderError),
}

impl std::fmt::Display for EstimationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TableValue(error) => error.fmt(f),
            Self::Comparison(error) => error.fmt(f),
            Self::Codec(error) => error.fmt(f),
            Self::Range(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for EstimationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TableValue(error) => Some(error),
            Self::Comparison(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::Range(error) => Some(error),
        }
    }
}

impl From<crate::ranger::points::PointBuilderError> for EstimationError {
    fn from(error: crate::ranger::points::PointBuilderError) -> Self {
        Self::Range(error)
    }
}

impl From<tidb_tablecodec::TableRowError> for EstimationError {
    fn from(error: tidb_tablecodec::TableRowError) -> Self {
        Self::TableValue(error)
    }
}

impl From<tidb_codec::CodecError> for EstimationError {
    fn from(error: tidb_codec::CodecError) -> Self {
        Self::Codec(error)
    }
}

impl From<tidb_datatype::DatumValueError> for EstimationError {
    fn from(error: tidb_datatype::DatumValueError) -> Self {
        Self::Comparison(error)
    }
}

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

/// The ranger's range, including per-column collators. Cardinality must not
/// project this into a second representation that loses range metadata.
pub use crate::ranger::Range as IndexRangeDatums;

fn topn_num(topn: Option<&TopN>) -> usize {
    topn.map_or(0, TopN::num)
}

fn topn_total_count(topn: Option<&TopN>) -> u64 {
    topn.map_or(0, TopN::total_count)
}

impl ColumnStats {
    /// The payload gate from Go ColumnStatsIsInvalid, independent of the
    /// collection owner's load requests and restricted-SQL policy.
    #[must_use]
    pub fn is_valid_for_estimation(&self, pseudo: bool, essential_loaded: bool) -> bool {
        !pseudo && self.total_row_count() != 0.0 && (essential_loaded || self.histogram.ndv <= 0)
    }

    /// Go `(*statistics.Column).MemoryUsage` for the payload retained by this
    /// statistics object.
    #[must_use]
    pub fn memory_usage(&self) -> ColumnMemUsage {
        let histogram_mem_usage = self.histogram.memory_usage();
        let cmsketch_mem_usage = self.cms.as_ref().map_or(0, |cms| cms.memory_usage() as i64);
        let topn_mem_usage = self
            .topn
            .as_ref()
            .map_or(0, |topn| topn.memory_usage() as i64);
        ColumnMemUsage {
            column_id: self.histogram.id,
            histogram_mem_usage,
            cmsketch_mem_usage,
            fmsketch_mem_usage: 0,
            topn_mem_usage,
            total_mem_usage: histogram_mem_usage
                .wrapping_add(cmsketch_mem_usage)
                .wrapping_add(topn_mem_usage),
        }
    }

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
    /// Go `(*statistics.Index).MemoryUsage` for the payload retained by this
    /// statistics object.
    #[must_use]
    pub fn memory_usage(&self) -> IndexMemUsage {
        let histogram_mem_usage = self.histogram.memory_usage();
        let cmsketch_mem_usage = self.cms.as_ref().map_or(0, |cms| cms.memory_usage() as i64);
        let topn_mem_usage = self
            .topn
            .as_ref()
            .map_or(0, |topn| topn.memory_usage() as i64);
        IndexMemUsage {
            index_id: self.histogram.id,
            histogram_mem_usage,
            cmsketch_mem_usage,
            topn_mem_usage,
            total_mem_usage: histogram_mem_usage
                .wrapping_add(cmsketch_mem_usage)
                .wrapping_add(topn_mem_usage),
        }
    }

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
/// Encoding failures propagate to the range estimator; they are not zero rows.
pub fn equal_row_count_on_column(
    column: &ColumnStats,
    value: &Datum,
    encoded_value: &[u8],
    collation: Collation,
    realtime_row_count: i64,
    modify_count: i64,
    options: EstimatorOptions,
) -> Result<RowEstimate, EstimationError> {
    if value.is_null() {
        return Ok(RowEstimate::default_est(column.histogram.null_count as f64));
    }
    let histogram = &column.histogram;

    if column.stats_ver < VERSION2 {
        if histogram.is_empty() {
            return Ok(RowEstimate::default_est(0.0));
        }
        if histogram.ndv > 0 && histogram.out_of_range(value, collation) {
            let total = column.total_row_count();
            let selectivity =
                out_of_range_eq_selectivity(histogram.ndv, realtime_row_count, total as i64);
            return Ok(RowEstimate::default_est(selectivity * total));
        }
        if let Some(cms) = column.cms.as_ref() {
            let count =
                tidb_stats::cmsketch::query_value(Some(cms), column.topn.as_ref(), value, None)?;
            return Ok(RowEstimate::default_est(count as f64));
        }
        let (hist_count, _) = histogram.equal_row_count(value, false, collation);
        return Ok(RowEstimate::default_est(hist_count));
    }

    // Stats version 2.
    if histogram.is_empty() && topn_num(column.topn.as_ref()) == 0 {
        return Ok(RowEstimate::default_est(0.0));
    }
    // 1. TopN is exact.
    if let Some(topn) = column.topn.as_ref() {
        if let Some(count) = topn.query_bytes(encoded_value) {
            return Ok(RowEstimate::default_est(count as f64));
        }
    }
    // 2. Bucket repeat / bucket NDV.
    let (hist_count, matched) = histogram.equal_row_count(value, true, collation);
    let hist_ndv = (histogram.ndv - topn_num(column.topn.as_ref()) as i64) as f64;
    if matched
        && hist_count > 0.0
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
        return Ok(RowEstimate::default_est(hist_count));
    }
    // 3. Uniform distribution, which is also where out-of-range values land.
    Ok(uniform_estimate(
        histogram,
        column.topn.as_ref(),
        column.total_row_count(),
        column.increase_factor(realtime_row_count),
        realtime_row_count,
        modify_count,
        options,
    ))
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

// Cardinality and statistics share Go's full typed enumeration contract,
// including the distinction between nil and an empty enumerated integer range.
pub use tidb_stats::enum_range_values;

fn encode_datum(value: &Datum) -> Result<Vec<u8>, tidb_codec::CodecError> {
    encode_key(std::slice::from_ref(value))
}

#[cfg(test)]
fn encode_datums(values: &[Datum]) -> Vec<u8> {
    encode_key(values).unwrap()
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

/// A range endpoint as the histogram stores its own bounds: Go
/// `getColumnRowCount`'s first three lines
/// (`row_count_column.go:126-132`).
///
/// ```text
/// if highVal.Kind() == types.KindString {
///     highVal.SetBytes(collate.GetCollator(highVal.Collation()).Key(highVal.GetString()))
/// }
/// ```
///
/// Under a new collation `ANALYZE` writes the collation SORT KEY as the
/// bucket bound, not the value (`tidb_executor::analyze`, and the cluster
/// loader reads back the same encoding), so a raw `'mm3'` compared against
/// those bounds lands in the wrong bucket. Every comparison AFTER this
/// conversion is binary -- Go's `LocateBucket` reads the bounds chunk
/// directly and its callers pass `collate.GetBinaryCollator()` -- because
/// both sides are now sort keys.
///
/// A non-string endpoint, and a `Datum::Bytes` (Go's `KindBytes`, which the
/// source's `KindString` test excludes), pass through untouched.
fn to_sort_key(value: &Datum) -> Datum {
    match value {
        Datum::String(string) => Datum::Bytes(string.collation().key(string.bytes())),
        other => other.clone(),
    }
}

/// Estimates a column's row count over `ranges`, Go `getColumnRowCount`.
///
/// `collation` is read only to reach [`to_sort_key`]'s domain; the estimates
/// themselves compare sort key against sort key, which is binary.
pub fn get_column_row_count(
    column: &ColumnStats,
    ranges: &[ColumnRange],
    _collation: Collation,
    realtime_row_count: i64,
    modify_count: i64,
    pk_is_handle: bool,
    options: EstimatorOptions,
) -> Result<RowEstimate, EstimationError> {
    let mut total = RowEstimate::default_est(0.0);
    let increase_factor = column.increase_factor(realtime_row_count);

    for range in ranges {
        // Go `getColumnRowCount` enters its point branch after preparing both
        // bounds. For the closed point ranges produced by `buildFromIn`, the
        // two written datums are identical, so preparing and encoding them
        // twice only repeats work; the equality and all estimate branches are
        // unchanged. Keep every non-point (including collation-distinct or
        // exclusive) range on the source-shaped path below.
        if !range.low_exclude && !range.high_exclude && range.low == range.high {
            let value = to_sort_key(&range.low);
            value.compare(&value, Collation::Binary)?;
            let encoded = encode_datum(&value)?;
            if pk_is_handle {
                total.add_all(1.0);
                continue;
            }
            let mut count = equal_row_count_on_column(
                column,
                &value,
                &encoded,
                Collation::Binary,
                realtime_row_count,
                modify_count,
                options,
            )?;
            count.multiply_all(increase_factor);
            total.add(count);
            continue;
        }
        // Go clones both endpoints and replaces a string with its sort key
        // BEFORE encoding, comparing, or estimating anything from them.
        let range = &ColumnRange {
            low: to_sort_key(&range.low),
            high: to_sort_key(&range.high),
            low_exclude: range.low_exclude,
            high_exclude: range.high_exclude,
        };
        let collation = Collation::Binary;
        let equal_bounds =
            range.low.compare(&range.high, Collation::Binary)? == std::cmp::Ordering::Equal;
        let low_encoded = encode_datum(&range.low)?;
        let high_encoded = encode_datum(&range.high)?;

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
                )?;
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
                    )?;
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
            )?;
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
            )?;
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
    Ok(total)
}

/// Go getPseudoRowCountWithPartialStats after the collection owner has checked
/// column availability and rejected full ranges. Shared by ordinary and merge
/// index alternatives; ranges belong to the candidate being estimated.
pub fn get_index_row_count_with_partial_stats(
    columns: &[(Option<&ColumnStats>, Collation)],
    ranges: &[IndexRangeDatums],
    realtime: i64,
    modify_count: i64,
    options: EstimatorOptions,
) -> Result<Option<RowEstimate>, EstimationError> {
    if realtime <= 0 {
        return Ok(Some(RowEstimate::default_est(0.0)));
    }
    if columns.len() == 1 {
        let (column, collation) = columns[0];
        let column_ranges = ranges
            .iter()
            .map(|range| {
                ColumnRange::new(
                    range.low_val.first().cloned().unwrap_or(Datum::MinNotNull),
                    range.high_val.first().cloned().unwrap_or(Datum::MaxValue),
                    range.low_exclude,
                    range.high_exclude,
                )
            })
            .collect::<Vec<_>>();
        return get_row_count_by_column_ranges(
            column,
            &column_ranges,
            collation,
            realtime,
            modify_count,
            false,
            options,
        )
        .map(|estimate| Some(RowEstimate::default_est(estimate.est)));
    }
    let mut partial_ranges = Vec::with_capacity(ranges.len());
    for range in ranges {
        let mut counts = Vec::with_capacity(range.low_val.len());
        for (position, low) in range.low_val.iter().enumerate() {
            let Some((column, collation)) = columns.get(position) else {
                return Ok(None);
            };
            let last = position + 1 == range.low_val.len();
            let column_range = ColumnRange::new(
                low.clone(),
                range.high_val.get(position).cloned().unwrap_or(Datum::MaxValue),
                last && range.low_exclude,
                last && range.high_exclude,
            );
            counts.push(
                get_row_count_by_column_ranges(
                    *column,
                    &[column_range],
                    *collation,
                    realtime,
                    modify_count,
                    false,
                    options,
                )?
                .est,
            );
        }
        partial_ranges.push(super::row_count_column::PartialStatsRange {
            column_row_counts: counts,
        });
    }
    let estimate = super::row_count_column::pseudo_row_count_with_partial_stats(
        &partial_ranges,
        realtime as f64,
        false,
    );
    Ok(Some(RowEstimate::new(
        estimate.total_count,
        estimate.total_count,
        estimate.max_count,
    )))
}

/// Estimates a column's row count, falling back to pseudo statistics when the
/// column has none or its retained payload has no rows. The collection owner
/// also filters pseudo/evicted entries using Go `ColumnStatsIsInvalid`.
pub fn get_row_count_by_column_ranges(
    column: Option<&ColumnStats>,
    ranges: &[ColumnRange],
    collation: Collation,
    realtime_row_count: i64,
    modify_count: i64,
    pk_is_handle: bool,
    options: EstimatorOptions,
) -> Result<RowEstimate, EstimationError> {
    let Some(column) = column.filter(|column| column.total_row_count() != 0.0) else {
        return Ok(RowEstimate::default_est(pseudo_row_count(
            ranges,
            collation,
            realtime_row_count,
            pk_is_handle,
        )));
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

#[cfg(test)]
mod tests {
    use super::{get_row_count_by_column_ranges, pseudo_equal_count, ColumnRange};
    use tidb_datatype::{Collation, Datum, StringDatum};

    #[test]
    fn loaded_column_estimation_preserves_invalid_bound_errors() {
        let column = super::ColumnStats {
            histogram: super::Histogram {
                ndv: 10,
                null_count: 100,
                ..Default::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        for range in [
            ColumnRange::point(Datum::Raw(vec![1])),
            ColumnRange::new(
                Datum::Int(1),
                Datum::VectorFloat32(tidb_datatype::VectorFloat32::must_create(vec![1.0])),
                false,
                false,
            ),
            ColumnRange::new(Datum::MinNotNull, Datum::Raw(vec![1]), false, false),
        ] {
            for is_handle in [false, true] {
                let result = get_row_count_by_column_ranges(
                    Some(&column),
                    &[range.clone()],
                    Collation::Binary,
                    100,
                    0,
                    is_handle,
                    Default::default(),
                );
                match range.high {
                    Datum::VectorFloat32(_) => {
                        assert!(matches!(result, Err(super::EstimationError::Comparison(_))))
                    }
                    _ => assert!(matches!(
                        result,
                        Err(super::EstimationError::Codec(
                            tidb_codec::CodecError::InvalidEncoding("unsupported raw datum")
                        ))
                    )),
                }
            }
        }
    }

    #[test]
    fn pseudo_long_point_ranges_keep_go_equality_accumulation() {
        let ranges: Vec<ColumnRange> = (0..1000)
            .map(|value| {
                let datum = Datum::String(StringDatum::new(
                    format!("maker-{value}").into_bytes(),
                    Collation::Utf8Mb4Bin,
                ));
                ColumnRange::point(datum)
            })
            .collect();
        let table_rows = 1_048_576;
        let expected = (0..ranges.len()).fold(0.0_f64, |total, _| {
            total + pseudo_equal_count(table_rows as f64)
        });
        let actual = get_row_count_by_column_ranges(
            None,
            &ranges,
            Collation::Utf8Mb4Bin,
            table_rows,
            0,
            false,
            Default::default(),
        )
        .unwrap()
        .est;
        assert_eq!(actual.to_bits(), expected.min(table_rows as f64).to_bits());
    }
}

fn bound_kind(value: &Datum) -> PseudoBoundKind {
    match value {
        Datum::Null => PseudoBoundKind::Null,
        Datum::MinNotNull => PseudoBoundKind::MinNotNull,
        Datum::MaxValue => PseudoBoundKind::MaxValue,
        _ => PseudoBoundKind::Value,
    }
}

fn datum_int(value: &Datum) -> i64 {
    match value {
        Datum::Int(v) => *v,
        Datum::UInt(v) => *v as i64,
        _ => 0,
    }
}

/// Go `types.Datum.GetUint64`, which REINTERPRETS the stored 64-bit value
/// rather than converting it: a datum holding `-1` reads back as `u64::MAX`.
/// The unsigned pseudo estimator is fed by that accessor even when the range
/// came off a signed column, so the reinterpretation is the behavior, not a
/// rounding of it.
fn datum_uint(value: &Datum) -> u64 {
    match value {
        Datum::Int(v) => *v as u64,
        Datum::UInt(v) => *v,
        _ => 0,
    }
}

fn pseudo_row_count(
    ranges: &[ColumnRange],
    collation: Collation,
    realtime_row_count: i64,
    pk_is_handle: bool,
) -> f64 {
    let table_row_count = realtime_row_count as f64;
    if pk_is_handle {
        if ranges.is_empty() {
            return 0.0;
        }
        // Go `GetRowCountByColumnRanges` dispatches on the FIRST range's low
        // bound alone, and its `else` is the UNSIGNED estimator -- not the
        // scalar one. A handle range whose low bound is an infinity (`id < 5`
        // builds `[-inf,5)`) therefore takes the unsigned branch even on a
        // signed column, and the reinterpretation of the high bound's bits as
        // `uint64` is what Go's own numbers show. Captured on
        // `sbtest1(id bigint primary key, ...)` with no statistics:
        //
        //   id < -1   ->  [-inf,-1)   10000.00   low 0, high u64::MAX
        //   id < 0    ->  [-inf,0)     3333.33   low 0, high 0
        //   id < 5    ->  [-inf,5)     3333.33   low 0, high 5
        //
        // Only the first of those is what a signed reading would produce, and
        // no scalar reading produces any of them. This branch had been marked
        // unreachable, which it is only while nothing builds a clustered
        // handle range at all; it becomes reachable with the first one.
        let signed_low = matches!(ranges[0].low, Datum::Int(_));
        if signed_low {
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
        let unsigned: Vec<UnsignedIntRange> = ranges
            .iter()
            .map(|range| {
                UnsignedIntRange::new(
                    datum_uint(&range.low),
                    datum_uint(&range.high),
                    bound_kind(&range.low),
                    bound_kind(&range.high),
                )
            })
            .collect();
        return pseudo_row_count_by_unsigned_int_ranges(&unsigned, table_row_count);
    }

    // Go's pseudo column estimator charges the equality rate once for each
    // closed point range.  Long literal IN lists are already normalized into
    // exactly that shape by the ranger.  Avoid materializing a second
    // `ScalarRange` for every point and the collation comparison that the
    // generic path repeats for each one; keep the source's left-to-right
    // accumulation so the floating-point result remains the same.
    if ranges.len() >= 32
        && ranges.iter().all(|range| {
            range.low == range.high
                && !matches!(range.low, Datum::Null | Datum::MinNotNull | Datum::MaxValue)
        })
    {
        let equality_count = pseudo_equal_count(table_row_count);
        let row_count = (0..ranges.len()).fold(0.0_f64, |total, _| total + equality_count);
        return row_count.min(table_row_count);
    }
    let scalar: Vec<ScalarRange> = ranges
        .iter()
        .map(|range| {
            // Go compares the original Datum bounds under the range's
            // collator. A numeric surrogate cannot preserve that contract:
            // in particular, mapping every string to 0 turns every string
            // interval into an equality range.
            let equal = range
                .low
                .compare(&range.high, collation)
                .is_ok_and(|ordering| ordering.is_eq());
            ScalarRange::new(
                0.0,
                if equal { 0.0 } else { 1.0 },
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
    vec![tidb_codec::NIL_FLAG]
}

/// Go `outOfRangeOnIndex` and its `matchPrefix` guard.
///
/// A shortened composite-index key sorts before the first full key that has
/// the same prefix. Go does not call that prefix out of range: doing so would
/// add one out-of-range value to an otherwise exact leading-column estimate.
fn out_of_range_on_index(index: &IndexStats, value: &Datum) -> bool {
    if !index.histogram.out_of_range(value, Collation::Binary) {
        return false;
    }
    let Some(first) = index.histogram.buckets.first() else {
        return false;
    };
    if matches!(
        value,
        Datum::String(_) | Datum::Bytes(_) | Datum::BinaryLiteral(_) | Datum::Bit(_)
    ) && first.lower_bound.go_bytes().starts_with(value.go_bytes())
    {
        return false;
    }
    true
}

/// Go `getOrdinalOfRangeCond`: the first index column whose bounds differ.
/// Unlike the point predicate, Go compares every position using Collators[0].
#[must_use]
pub fn ordinal_of_range_cond(range: &IndexRangeDatums) -> usize {
    for (index, low) in range.low_val.iter().enumerate() {
        let Some(high) = range.high_val.get(index) else {
            return index;
        };
        match low.compare(high, range.collators[0]) {
            Ok(std::cmp::Ordering::Equal) => {}
            Ok(_) => return index,
            Err(_) => return 0,
        }
    }
    range.low_val.len()
}

/// The per-column statistics an index range walk needs, in index order.
///
/// A `None` entry is a column without usable statistics. For multi-column
/// ranges the caller supplies other indexes led by that column, matching
/// `HistColl.ColUniqueID2IdxIDs`. If none can estimate a virtual column,
/// backoff gives way to the full index histogram instead of dropping it.
pub type IndexColumnStats<'a> = Vec<Option<&'a ColumnStats>>;

/// Table counts retained for column backoff alongside the possibly scaled
/// index-entry counts from Go `HistColl.GetScaledRealtimeAndModifyCnt`.
#[derive(Clone, Copy, Debug)]
pub struct IndexRowCounts {
    /// Rows currently in the table.
    pub table_realtime: i64,
    /// Modified table rows since analysis.
    pub table_modify: i64,
    /// Current index entries (multiple entries per row for MV indexes).
    pub index_realtime: i64,
    /// Modified index entries since analysis.
    pub index_modify: i64,
}

impl IndexRowCounts {
    /// Counts for an ordinary index or an index ineligible for scaling.
    #[must_use]
    pub const fn unscaled(realtime: i64, modify: i64) -> Self {
        Self {
            table_realtime: realtime,
            table_modify: modify,
            index_realtime: realtime,
            index_modify: modify,
        }
    }
}

/// Statistics for one index together with its collection-owned column inputs.
/// Raw NDVs and valid histograms are distinct in Go's V1 cross validation.
pub struct IndexEstimationStats<'a> {
    /// Retained index payload, including absent or zero-count entries.
    pub index: Option<&'a IndexStats>,
    /// Valid column histograms in physical key order.
    pub columns: IndexColumnStats<'a>,
    /// Declared-column NDVs, retained even when their histograms are invalid.
    pub column_ndvs: Vec<Option<i64>>,
    /// Go Column.IsHandle in declared key order.
    pub column_is_handle: Vec<bool>,
    /// Unscaled table and independently scaled index-entry counts.
    pub row_counts: IndexRowCounts,
    /// IndexInfo conditions controlling full-range short-circuiting.
    pub policy: super::index_range_policy::IndexRangePolicy,
}

/// An alternate leading-column index uses the same estimation context.
pub type RecursiveIndexStats<'a> = IndexEstimationStats<'a>;

impl<'a> IndexEstimationStats<'a> {
    /// Creates a context for fully loaded ordinary column inputs. Collection
    /// adapters replace raw NDVs and handle flags with the original metadata.
    pub fn new(
        index: Option<&'a IndexStats>,
        columns: IndexColumnStats<'a>,
        row_counts: IndexRowCounts,
    ) -> Self {
        let uses_v1 = index.is_some_and(|index| index.stats_ver == VERSION1 && index.cms.is_some());
        let column_ndvs = if uses_v1 {
            columns
                .iter()
                .map(|column| column.map(|column| column.histogram.ndv))
                .collect()
        } else {
            Vec::new()
        };
        let column_is_handle = if uses_v1 {
            vec![false; columns.len()]
        } else {
            Vec::new()
        };
        Self {
            index,
            columns,
            column_ndvs,
            column_is_handle,
            row_counts,
            policy: Default::default(),
        }
    }
}

/// Go GetRowCountByIndexRanges's common full-range, validity and version
/// dispatch. Empty virtual_columns corresponds to nil idxCols on recursion.
pub fn get_index_row_count(
    stats: &IndexEstimationStats<'_>,
    virtual_columns: &[bool],
    recursive_indexes: &[Vec<RecursiveIndexStats<'_>>],
    ranges: &[IndexRangeDatums],
    options: EstimatorOptions,
) -> Result<RowEstimate, EstimationError> {
    if stats.index.is_some()
        && super::index_range_policy::can_skip_datum_index_estimation(
            stats.policy,
            ranges.iter().map(|range| {
                (
                    range.low_val.as_slice(),
                    range.high_val.as_slice(),
                    range.low_exclude,
                    range.high_exclude,
                )
            }),
        )
    {
        return Ok(RowEstimate::default_est(
            stats.row_counts.index_realtime as f64,
        ));
    }
    let Some(index) = stats.index.filter(|index| index.total_row_count() != 0.0) else {
        if !virtual_columns.is_empty()
            && stats.columns.iter().any(Option::is_some)
            && !crate::ranger::types::has_full_range(ranges, false)
        {
            let columns = stats
                .columns
                .iter()
                .enumerate()
                .map(|(position, column)| {
                    (
                        *column,
                        ranges
                            .first()
                            .and_then(|range| range.collators.get(position))
                            .copied()
                            .unwrap_or(Collation::Binary),
                    )
                })
                .collect::<Vec<_>>();
            if let Some(estimate) = get_index_row_count_with_partial_stats(
                &columns,
                ranges,
                stats.row_counts.table_realtime,
                stats.row_counts.table_modify,
                options,
            )? {
                return Ok(estimate);
            }
        }
        if stats.row_counts.table_realtime != 0 {
            for range in ranges {
                range.prefix_equal_len()?;
            }
        }
        return Ok(RowEstimate::default_est(
            crate::ranger::stats_bridge::pseudo_count_by_index_ranges(
                ranges,
                stats.row_counts.table_realtime as f64,
                stats
                    .index
                    .filter(|index| index.unique)
                    .map(|index| index.num_columns),
            ),
        ));
    };
    if index.stats_ver != VERSION1 || index.cms.is_none() {
        return get_index_row_count_for_stats_v2(
            index,
            &stats.columns,
            virtual_columns,
            recursive_indexes,
            ranges,
            stats.row_counts,
            options,
        );
    }
    let mut total = 0.0;
    for range in ranges {
        let mut position = ordinal_of_range_cond(range);
        let values = if position < range.low_val.len() {
            enum_range_values(
                &range.low_val[position],
                &range.high_val[position],
                range.low_exclude,
                range.high_exclude,
            )
        } else {
            None
        };
        if values.is_some() {
            position += 1;
        }
        let single_null = index.num_columns <= 1
            && range.low_val.first().is_some_and(Datum::is_null)
            && range.high_val.first().is_some_and(Datum::is_null);
        if position == 0 || single_null {
            total += get_index_row_count_for_stats_v2(
                index,
                &Vec::new(),
                &[],
                &[],
                std::slice::from_ref(range),
                stats.row_counts,
                options,
            )?
            .est;
            continue;
        }
        let equal_selectivity = |encoded: &[u8]| -> Result<f64, EstimationError> {
            let mut column_counts = Vec::new();
            // Go skips cross validation entirely for a full unique key or
            // an out-of-range prefix; errors in unused columns must not leak.
            if !(index.unique && position == index.num_columns)
                && !out_of_range_on_index(index, &Datum::Bytes(encoded.to_vec()))
            {
                for (column_position, column) in stats
                    .columns
                    .iter()
                    .take(position.min(stats.column_ndvs.len()))
                    .enumerate()
                {
                    let count = if let Some(column) = column {
                        Some(
                            get_column_row_count(
                                column,
                                &[ColumnRange::new(
                                    range.low_val[column_position].clone(),
                                    range.high_val[column_position].clone(),
                                    false,
                                    false,
                                )],
                                range.collators[column_position],
                                stats.row_counts.table_realtime,
                                stats.row_counts.table_modify,
                                stats
                                    .column_is_handle
                                    .get(column_position)
                                    .copied()
                                    .unwrap_or(false),
                                options,
                            )?
                            .est,
                        )
                    } else {
                        None
                    };
                    column_counts.push(count);
                }
            }
            Ok(get_equal_cond_selectivity(EqualCondSelectivityInputs {
                index,
                encoded_value: encoded,
                used_cols_len: position,
                prefix_column_ndvs: &stats.column_ndvs,
                prefix_column_row_counts: &column_counts,
                realtime_row_count: stats.row_counts.index_realtime,
                modify_count: stats.row_counts.index_modify,
                options,
            }))
        };
        let mut selectivity = if let Some(values) = values {
            let mut encoded = encode_key(&range.low_val[..position - 1])?;
            let prefix_len = encoded.len();
            let mut result = 0.0;
            for value in values {
                encoded.truncate(prefix_len);
                encoded.extend(encode_key(&[value])?);
                result += equal_selectivity(&encoded)?;
            }
            result
        } else {
            equal_selectivity(&encode_key(&range.low_val[..position])?)?
        };
        if position < range.low_val.len() {
            let suffix = IndexRangeDatums {
                low_val: vec![range.low_val[position].clone()],
                high_val: vec![range.high_val[position].clone()],
                collators: vec![range.collators[position]],
                low_exclude: range.low_exclude,
                high_exclude: range.high_exclude,
            };
            let count = if let Some(candidate) = recursive_indexes
                .get(position)
                .filter(|_| position < stats.column_ndvs.len())
                .and_then(|candidates| candidates.first())
            {
                get_index_row_count(candidate, &[], &[], std::slice::from_ref(&suffix), options)?
                    .est
            } else {
                get_row_count_by_column_ranges(
                    if position < stats.column_ndvs.len() {
                        stats.columns.get(position).copied().flatten()
                    } else {
                        None
                    },
                    &[ColumnRange::new(
                        suffix.low_val[0].clone(),
                        suffix.high_val[0].clone(),
                        suffix.low_exclude,
                        suffix.high_exclude,
                    )],
                    suffix.collators[0],
                    stats.row_counts.table_realtime,
                    stats.row_counts.table_modify,
                    false,
                    options,
                )?
                .est
            };
            selectivity *= count / index.total_row_count();
        }
        total += selectivity * index.total_row_count();
    }
    if total > index.total_row_count() {
        total = index.total_row_count();
    }
    Ok(RowEstimate::default_est(total))
}

/// Go `expBackoffEstimation`.
///
/// Returns `None` when no column contributes or an unestimated virtual column
/// requires the index histogram's own interval estimate. `virtual_columns`
/// corresponds to Go's supplied `idxCols`; an empty slice matches nil metadata
/// on recursive calls.
pub fn exp_backoff_estimation(
    index: &IndexStats,
    columns: &[Option<&ColumnStats>],
    virtual_columns: &[bool],
    recursive_indexes: &[Vec<RecursiveIndexStats<'_>>],
    range: &IndexRangeDatums,
    realtime_row_count: i64,
    modify_count: i64,
    options: EstimatorOptions,
) -> Result<Option<(f64, f64, f64)>, EstimationError> {
    let mut single_column_results = Vec::with_capacity(range.low_val.len());
    let mut min_sel = 1.0_f64;
    let mut max_sel = 1.0_f64;

    for position in 0..range.low_val.len() {
        // Go skips dimensions absent from Idx2ColUniqueIDs before looking
        // for either a column histogram or an alternate index.
        if position >= columns.len() {
            continue;
        }
        let last = position == range.low_val.len() - 1;
        let column_range = ColumnRange {
            low: range.low_val[position].clone(),
            high: range.high_val[position].clone(),
            low_exclude: last && range.low_exclude,
            high_exclude: last && range.high_exclude,
        };
        let Some(Some(column)) = columns.get(position) else {
            let mut found_stats = false;
            let mut selectivity = 0.0;
            // Go recursively tries other loaded indexes for this column only
            // when the source range has more than one dimension. The recursive
            // call receives a one-column range, so this branch cannot recurse
            // again and remains finite.
            if range.low_val.len() > 1 {
                if let Some(candidates) = recursive_indexes.get(position) {
                    let one_column_range = IndexRangeDatums {
                        collators: vec![range.collators[0]],
                        low_val: vec![range.low_val[position].clone()],
                        high_val: vec![range.high_val[position].clone()],
                        low_exclude: last && range.low_exclude,
                        high_exclude: last && range.high_exclude,
                    };
                    for candidate in candidates {
                        if candidate.index.is_none_or(|index| index.total_row_count() == 0.0) {
                            continue;
                        }
                        let Ok(count) = get_index_row_count(
                            candidate,
                            &[],
                            &[],
                            std::slice::from_ref(&one_column_range),
                            options,
                        ) else {
                            // Go marks the dimension estimated only after a
                            // recursive candidate succeeds. Failed candidates
                            // neither lower the maximum nor suppress the
                            // virtual-column fallback; later candidates remain
                            // eligible.
                            continue;
                        };
                        let candidate_realtime = candidate.row_counts.index_realtime as f64;
                        selectivity = count.est / candidate_realtime;
                        max_sel = go_min(max_sel, count.max_est / candidate_realtime);
                        found_stats = true;
                        break;
                    }
                }
            }
            // Virtual columns have no own histogram. Omitting their range
            // can discard the index's most selective dimension; Go uses the
            // full index histogram/TopN instead unless recursion supplied it.
            if !found_stats
                && virtual_columns.get(position).copied().unwrap_or(false)
                && (!index.histogram.is_empty() || topn_num(index.topn.as_ref()) > 0)
            {
                return Ok(None);
            }
            if found_stats {
                single_column_results.push(selectivity);
                min_sel *= selectivity;
            }
            continue;
        };
        let count = get_column_row_count(
            column,
            std::slice::from_ref(&column_range),
            range.collators[0],
            realtime_row_count,
            modify_count,
            false,
            options,
        )?;
        let selectivity = count.est / realtime_row_count as f64;
        max_sel = go_min(max_sel, count.max_est / realtime_row_count as f64);
        single_column_results.push(selectivity);
        min_sel *= selectivity;
    }

    single_column_results.sort_by(f64::total_cmp);
    let len = single_column_results.len();
    if len == 1 {
        let only = single_column_results[0];
        return Ok(Some((only, only, only)));
    }
    if len == 0 {
        return Ok(None);
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
    Ok(Some((result, min_sel, max_sel)))
}

/// Estimates an index's row count over `ranges` under stats version 2, Go
/// `getIndexRowCountForStatsV2`. Index-bound encoding errors reach the caller.
pub fn get_index_row_count_for_stats_v2(
    index: &IndexStats,
    columns: &[Option<&ColumnStats>],
    virtual_columns: &[bool],
    recursive_indexes: &[Vec<RecursiveIndexStats<'_>>],
    ranges: &[IndexRangeDatums],
    row_counts: IndexRowCounts,
    options: EstimatorOptions,
) -> Result<RowEstimate, EstimationError> {
    let realtime_row_count = row_counts.index_realtime;
    let modify_count = row_counts.index_modify;
    let mut total = RowEstimate::default_est(0.0);
    let is_single_col_idx = index.num_columns == 1;
    let null_key = null_key_bytes();

    for range in ranges {
        let mut count = RowEstimate::default_est(0.0);
        let mut lb = encode_key(&range.low_val)?;
        let mut rb = encode_key(&range.high_val)?;
        let full_len = range.low_val.len() == range.high_val.len() && range.low_val.len() == index.num_columns;

        if lb == rb {
            // Case 1: a point.
            if range.low_exclude || range.high_exclude {
                continue;
            }
            if full_len {
                if index.unique {
                    let only_null = range.low_val.iter().all(Datum::is_null);
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
                virtual_columns,
                recursive_indexes,
                range,
                row_counts.table_realtime,
                row_counts.table_modify,
                options,
            )? {
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
            let single_col_range = range.low_val.len() == range.high_val.len() && range.low_val.len() == 1;
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
                            &range.low_val[0],
                            &range.high_val[0],
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
    Ok(total)
}

/// The inputs Go's `getEqualCondSelectivity`
/// (`pkg/planner/cardinality/selectivity.go`) assembles for one equality
/// condition over an index: the encoded value, how many equality columns
/// cover the index prefix, and the per-prefix-column NDV/row-count
/// estimates the cross validation consults.
pub struct EqualCondSelectivityInputs<'a> {
    /// The index the equality condition targets.
    pub index: &'a IndexStats,
    /// The encoded equality value (index-key bytes).
    pub encoded_value: &'a [u8],
    /// How many of the index's leading columns the equality covers.
    pub used_cols_len: usize,
    /// Per used prefix column: `Histogram.NDV`, `None` when that column has
    /// no loaded histogram.
    pub prefix_column_ndvs: &'a [Option<i64>],
    /// Per used prefix column: the column's row-count estimate over the
    /// point range, `None` when the column has no usable statistics.
    pub prefix_column_row_counts: &'a [Option<f64>],
    /// `coll.GetScaledRealtimeAndModifyCnt(idx)`'s realtime half.
    pub realtime_row_count: i64,
    /// The modify-count half.
    pub modify_count: i64,
    /// Estimator knobs.
    pub options: EstimatorOptions,
}

/// Go `getEqualCondSelectivity`
/// (`pkg/planner/cardinality/selectivity.go`): the selectivity of one
/// equality condition over an index.
///
/// Ordering is the source's:
/// 1. a UNIQUE index whose equality columns cover the whole index holds at
///    most one row — `1 / TotalRowCount`;
/// 2. an OUT-OF-RANGE value cannot be in the CM Sketch, so heuristics apply
///    — the index NDV when the equality covers everything, else the max NDV
///    over the used prefix columns;
/// 3. otherwise the CMSketch/TopN/histogram count competes with the
///    per-column cross validation: the cross validation wins while its
///    minimum row count is below the sketch count.
pub fn get_equal_cond_selectivity(inputs: EqualCondSelectivityInputs<'_>) -> f64 {
    let index = inputs.index;
    let cover_all = inputs.used_cols_len == index.num_columns;
    if index.unique && cover_all {
        return 1.0 / index.total_row_count();
    }
    let value = Datum::Bytes(inputs.encoded_value.to_vec());
    if out_of_range_on_index(index, &value) {
        if index.histogram.ndv > 0 && cover_all {
            return out_of_range_eq_selectivity(
                index.histogram.ndv,
                inputs.realtime_row_count,
                index.total_row_count() as i64,
            );
        }
        let mut ndv = 0_i64;
        for position in 0..inputs.used_cols_len.min(inputs.prefix_column_ndvs.len()) {
            if let Some(Some(column_ndv)) = inputs.prefix_column_ndvs.get(position) {
                ndv = (*column_ndv).max(ndv);
            }
        }
        return out_of_range_eq_selectivity(
            ndv,
            inputs.realtime_row_count,
            index.total_row_count() as i64,
        );
    }
    let (min_row_count, cross_valid) = crate::selectivity_greedy::cross_validation_selectivity(
        inputs.prefix_column_row_counts,
        inputs.used_cols_len,
        index.total_row_count(),
    );
    let idx_count = index.topn.as_ref().and_then(|topn| topn.query_bytes(inputs.encoded_value))
        .or_else(|| index.cms.as_ref().map(|cms| cms.query_bytes(inputs.encoded_value)))
        .unwrap_or_else(|| index.histogram.equal_row_count(
            &value, index.stats_ver >= VERSION2, Collation::Binary,
        ).0 as u64) as f64;
    if min_row_count < idx_count {
        return cross_valid;
    }
    idx_count / index.total_row_count()
}

#[cfg(test)]
mod equal_cond_selectivity_tests {
    use super::{
        EqualCondSelectivityInputs, EstimatorOptions, IndexStats, equal_row_count_on_index,
        get_equal_cond_selectivity, out_of_range_on_index,
    };
    use tidb_datatype::{Collation, Datum};

    fn index(ndv: i64, num_columns: usize, unique: bool) -> IndexStats {
        IndexStats {
            histogram: super::Histogram {
                id: 7,
                ndv,
                buckets: vec![tidb_stats::Bucket {
                    count: 10,
                    repeat: 2,
                    ndv,
                    lower_bound: Datum::Bytes(b"a".to_vec()),
                    upper_bound: Datum::Bytes(b"z".to_vec()),
                }],
                null_count: 0,
                ..super::Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            num_columns,
            unique,
        }
    }

    fn inputs<'a>(
        index: &'a IndexStats,
        encoded: &'a [u8],
        used: usize,
        ndvs: &'a [Option<i64>],
        row_counts: &'a [Option<f64>],
    ) -> EqualCondSelectivityInputs<'a> {
        EqualCondSelectivityInputs {
            index,
            encoded_value: encoded,
            used_cols_len: used,
            prefix_column_ndvs: ndvs,
            prefix_column_row_counts: row_counts,
            realtime_row_count: 20,
            modify_count: 5,
            options: EstimatorOptions::default(),
        }
    }

    /// Go: a UNIQUE index whose equality columns cover the whole index holds
    /// at most one row — selectivity is one over the total row count.
    #[test]
    fn unique_cover_all_answers_one_over_total() {
        let index = index(5, 1, true);
        let value = b"m";
        let result = get_equal_cond_selectivity(inputs(&index, value, 1, &[], &[]));
        assert!(
            (result - 0.1).abs() < 1e-9,
            "1 / total 10 = 0.1, got {result}"
        );
    }

    /// Go: an OUT-OF-RANGE value with the equality covering the whole index
    /// uses the index NDV heuristic.
    #[test]
    fn out_of_range_cover_all_uses_the_index_ndv() {
        let index = index(5, 1, false);
        let value = b"zz";
        assert!(out_of_range_on_index(&index, &Datum::Bytes(value.to_vec())));
        let result =
            get_equal_cond_selectivity(inputs(&index, value, 1, &[Some(100)], &[Some(5.0)]));
        let expected = super::out_of_range_eq_selectivity(5, 20, 10);
        assert!(
            (result - expected).abs() < 1e-9,
            "cover-all out-of-range answers the index-NDV heuristic: {result} vs {expected}"
        );
    }

    /// Go: an OUT-OF-RANGE value on a PREFIX of the index uses the max NDV
    /// over the used prefix columns instead of the index NDV.
    #[test]
    fn out_of_range_prefix_uses_the_prefix_ndv() {
        let index = index(5, 2, false);
        let value = b"zz";
        let result = get_equal_cond_selectivity(inputs(&index, value, 1, &[Some(100)], &[]));
        let expected = super::out_of_range_eq_selectivity(100, 20, 10);
        assert!(
            (result - expected).abs() < 1e-9,
            "prefix out-of-range answers the prefix-NDV heuristic: {result} vs {expected}"
        );
    }

    /// Go: when the per-column cross validation's minimum row count is BELOW
    /// the sketch/histogram count, the cross validation wins.
    #[test]
    fn cross_validation_wins_over_a_maximally_noisy_cms() {
        let mut index = index(5, 1, false);
        index.stats_ver = super::VERSION1;
        let mut cms = tidb_stats::CmsSketch::new(5, 2_048);
        cms.insert_bytes_by_count(b"m", 100_000);
        assert!(cms.query_with_topn(None, b"m") >= 100_000);
        index.cms = Some(cms);
        let result = get_equal_cond_selectivity(inputs(&index, b"m", 1, &[], &[Some(2.0)]));
        assert!(
            (result - 0.2).abs() < 1e-9,
            "cross_valid 2/10 = 0.2 must beat a maximally noisy CMS count, got {result}"
        );
    }

    /// Go: when the cross validation's minimum row count is at or above the
    /// index count, the index count over the total row count wins.
    #[test]
    fn index_count_wins_when_cross_validation_is_above_it() {
        let mut index = index(5, 1, false);
        // The value IS in the TopN with count 3 — the sketch-side count the
        // cross validation competes against.
        let mut topn = tidb_stats::TopN::new(1);
        topn.append(b"m", 3);
        index.topn = Some(topn);

        let result = get_equal_cond_selectivity(inputs(&index, b"m", 1, &[], &[Some(5.0)]));
        let expected = 3.0 / index.total_row_count();
        assert!(
            (result - expected).abs() < 1e-9,
            "index count 3 over the histogram+TopN total wins, got {result} vs {expected}"
        );
    }
}

#[cfg(test)]
mod recursive_index_estimation_tests {
    use super::{
        encode_datums, exp_backoff_estimation, EstimatorOptions, Histogram, IndexColumnStats,
        IndexRangeDatums, IndexRowCounts, IndexStats, RecursiveIndexStats,
    };
    use tidb_datatype::Datum;

    fn index_stats(id: i64, count: i64) -> IndexStats {
        let low = encode_datums(std::slice::from_ref(&Datum::Int(1)));
        let high = encode_datums(std::slice::from_ref(&Datum::Int(10)));
        IndexStats {
            histogram: Histogram {
                id,
                ndv: 10,
                buckets: (count > 0)
                    .then(|| tidb_stats::Bucket {
                        count,
                        repeat: 0,
                        ndv: 10,
                        lower_bound: Datum::Bytes(low),
                        upper_bound: Datum::Bytes(high),
                    })
                    .into_iter()
                    .collect(),
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            num_columns: 1,
            unique: false,
        }
    }

    #[test]
    fn recursive_index_estimation_skips_empty_index_and_uses_next_candidate() {
        let main_index = index_stats(1, 100);
        let empty_candidate = index_stats(2, 0);
        let usable_candidate = index_stats(3, 100);
        let main_columns: IndexColumnStats<'_> = vec![None, None];
        let recursive_indexes = vec![
            vec![
                RecursiveIndexStats::new(Some(&empty_candidate), vec![None], IndexRowCounts::unscaled(100, 0)),
                RecursiveIndexStats::new(Some(&usable_candidate), vec![None], IndexRowCounts::unscaled(100, 0)),
            ],
            Vec::new(),
        ];
        let range = IndexRangeDatums {
            collators: vec![tidb_datatype::Collation::Binary; 2],
            low_val: vec![Datum::Int(1), Datum::Int(5)],
            high_val: vec![Datum::Int(1), Datum::Int(9)],
            low_exclude: false,
            high_exclude: false,
        };

        let estimate = exp_backoff_estimation(
            &main_index,
            &main_columns,
            &[],
            &recursive_indexes,
            &range,
            100,
            0,
            EstimatorOptions::default(),
        )
        .unwrap()
        .expect("the second loaded index estimates the missing first column");

        assert!(estimate.0 > 0.0 && estimate.0 < 1.0, "{estimate:?}");
        assert_eq!(estimate.1, estimate.0);
        assert_eq!(estimate.2, estimate.0);
    }

    #[test]
    fn missing_virtual_column_requires_index_histogram_fallback() {
        let column = super::ColumnStats {
            histogram: Histogram {
                ndv: 10,
                buckets: vec![tidb_stats::Bucket {
                    count: 100,
                    repeat: 10,
                    ndv: 10,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(10),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        let columns = vec![Some(&column), None];
        let range = IndexRangeDatums {
            collators: vec![tidb_datatype::Collation::Binary; 2],
            low_val: vec![Datum::Int(1), Datum::Int(5)],
            high_val: vec![Datum::Int(1), Datum::Int(9)],
            low_exclude: false,
            high_exclude: false,
        };
        let mut index = index_stats(1, 100);
        index.num_columns = 2;
        let estimate = |index: &IndexStats,
                        virtual_columns: &[bool],
                        candidates: &[Vec<RecursiveIndexStats<'_>>]| {
            exp_backoff_estimation(
                index,
                &columns,
                virtual_columns,
                candidates,
                &range,
                100,
                0,
                EstimatorOptions::default(),
            )
            .unwrap()
        };
        // Go retains backoff for an ordinary column without statistics.
        assert!(estimate(&index, &[], &[]).is_some());
        assert!(estimate(&index, &[false, true], &[]).is_none());

        // A usable recursive estimate also satisfies a virtual dimension.
        let candidate = index_stats(2, 100);
        let candidates = vec![
            Vec::new(),
            vec![RecursiveIndexStats::new(Some(&candidate), vec![None], IndexRowCounts::unscaled(100, 0))],
        ];
        assert!(estimate(&index, &[false, true], &candidates).is_some());

        index.histogram.buckets.clear();
        assert!(estimate(&index, &[false, true], &[]).is_some());
        let mut topn = super::TopN::new(1);
        topn.append(&encode_datums(&[Datum::Int(1), Datum::Int(6)]), 100);
        index.topn = Some(topn);
        assert!(estimate(&index, &[false, true], &[]).is_none());
    }

    #[test]
    fn failed_recursive_estimates_do_not_suppress_virtual_index_fallback() {
        let column = super::ColumnStats {
            histogram: Histogram {
                ndv: 10,
                buckets: vec![tidb_stats::Bucket {
                    count: 100,
                    repeat: 10,
                    ndv: 10,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(10),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        let columns = vec![Some(&column), None];
        let mut index = index_stats(1, 100);
        index.num_columns = 2;
        let candidate = index_stats(2, 100);
        let recursive_indexes = vec![
            Vec::new(),
            vec![RecursiveIndexStats::new(Some(&candidate), vec![None], IndexRowCounts::unscaled(100, 0))],
        ];
        let range = IndexRangeDatums {
            collators: vec![tidb_datatype::Collation::Binary; 2],
            low_val: vec![Datum::Int(1), Datum::Raw(vec![1])],
            high_val: vec![Datum::Int(1), Datum::Raw(vec![1])],
            low_exclude: false,
            high_exclude: false,
        };

        let estimate = exp_backoff_estimation(
            &index,
            &columns,
            &[false, true],
            &recursive_indexes,
            &range,
            100,
            0,
            EstimatorOptions::default(),
        )
        .unwrap();

        assert!(
            estimate.is_none(),
            "a failed recursive estimate must leave the virtual dimension eligible for the source index histogram fallback"
        );
    }

    #[test]
    fn recursive_index_uses_scaled_index_count_for_selectivity_and_max() {
        let mut index = index_stats(1, 1_000);
        // Keep the source index's lower bound below the candidate maximum so
        // its normalization denominator remains observable in max_sel.
        index.histogram.ndv = 1_000;
        index.num_columns = 2;
        let second_column = super::ColumnStats {
            histogram: Histogram {
                id: 4,
                ndv: 100,
                buckets: vec![tidb_stats::Bucket {
                    count: 1_000,
                    repeat: 1,
                    ndv: 100,
                    lower_bound: Datum::Int(1),
                    upper_bound: Datum::Int(10),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        let mut candidate = index_stats(2, 4_950);
        candidate.histogram.buckets[0].lower_bound = Datum::Bytes(encode_datums(&[Datum::Int(2)]));
        let mut topn = super::TopN::new(1);
        topn.append(&encode_datums(&[Datum::Int(1)]), 50);
        candidate.topn = Some(topn);
        let candidates = vec![
            vec![RecursiveIndexStats::new(Some(&candidate), vec![None], IndexRowCounts {
                    table_realtime: 1_500,
                    table_modify: 100,
                    index_realtime: 7_500,
                    index_modify: 500,
                })],
            Vec::new(),
        ];
        let range = IndexRangeDatums {
            collators: vec![tidb_datatype::Collation::Binary; 2],
            low_val: vec![Datum::Int(1), Datum::Int(5)],
            high_val: vec![Datum::Int(1), Datum::Int(9)],
            low_exclude: false,
            high_exclude: false,
        };
        // The MV point grows from 50 to 75 entries. Both selectivity and maxSel
        // divide by 7500 current index entries, matching Go's scaled count.
        let estimate = exp_backoff_estimation(
            &index,
            &vec![None, Some(&second_column)],
            &[],
            &candidates,
            &range,
            1_500,
            100,
            EstimatorOptions::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(estimate.2, 0.01);
    }

    #[test]
    fn index_encoding_failure_is_not_an_empty_key_estimate() {
        let index = index_stats(1, 100);
        for (low, high) in [
            (Datum::Raw(vec![1]), Datum::Int(1)),
            (Datum::Int(1), Datum::Raw(vec![1])),
            (Datum::Raw(vec![1]), Datum::Raw(vec![1])),
        ] {
            let range = IndexRangeDatums {
                collators: vec![tidb_datatype::Collation::Binary; 1],
                low_val: vec![low],
                high_val: vec![high],
                low_exclude: false,
                high_exclude: false,
            };
            let result = super::get_index_row_count_for_stats_v2(
                &index,
                &vec![None],
                &[],
                &[],
                &[range],
                IndexRowCounts::unscaled(100, 0),
                EstimatorOptions::default(),
            );
            assert!(matches!(
                result,
                Err(super::EstimationError::Codec(
                    tidb_codec::CodecError::InvalidEncoding("unsupported raw datum")
                ))
            ));
        }
    }

    #[test]
    fn backoff_propagates_a_loaded_column_comparison_error() {
        let index = index_stats(1, 100);
        let column = super::ColumnStats {
            histogram: Histogram {
                null_count: 100,
                ..Default::default()
            },
            topn: None,
            cms: None,
            stats_ver: 2,
            unsigned: false,
        };
        let range = IndexRangeDatums {
            collators: vec![tidb_datatype::Collation::Binary; 1],
            low_val: vec![Datum::Int(1)],
            high_val: vec![Datum::VectorFloat32(
                tidb_datatype::VectorFloat32::must_create(vec![1.0]),
            )],
            low_exclude: false,
            high_exclude: false,
        };
        assert!(matches!(
            exp_backoff_estimation(
                &index,
                &vec![Some(&column)],
                &[],
                &[],
                &range,
                100,
                0,
                EstimatorOptions::default()
            ),
            Err(super::EstimationError::Comparison(_))
        ));
    }

    #[test]
    fn recursive_index_encoding_failure_skips_only_the_failed_estimate() {
        let index = index_stats(1, 100);
        let mut candidate = index_stats(2, 100);
        candidate.histogram.buckets[0].repeat = 10;
        let columns = vec![None, None];
        let candidates = (0..2)
            .map(|_| {
                vec![RecursiveIndexStats::new(Some(&candidate), vec![None], IndexRowCounts::unscaled(100, 0))]
            })
            .collect::<Vec<_>>();
        let range = IndexRangeDatums {
            collators: vec![tidb_datatype::Collation::Binary; 2],
            low_val: vec![Datum::Raw(vec![1]), Datum::Int(1)],
            high_val: vec![Datum::Raw(vec![1]), Datum::Int(1)],
            low_exclude: false,
            high_exclude: false,
        };
        // The first recursive estimate errors; it does not contribute a zero
        // selectivity. The second column's successful recursive estimate still
        // contributes.
        assert_eq!(
            exp_backoff_estimation(
                &index,
                &columns,
                &[],
                &candidates,
                &range,
                100,
                0,
                EstimatorOptions::default()
            )
            .unwrap(),
            Some((0.1, 0.1, 0.1))
        );
        // With virtual metadata, the failed candidate leaves the virtual
        // dimension unestimated, so Go falls back to the source index stats.
        assert_eq!(
            exp_backoff_estimation(
                &index,
                &columns,
                &[true, false],
                &candidates,
                &range,
                100,
                0,
                EstimatorOptions::default()
            )
            .unwrap(),
            None
        );
    }
}

#[cfg(test)]
mod version_one_value_tests {
    use super::*;
    use tidb_datatype::{MySqlDuration, Time, TimeType};

    fn temporal_values(kind: Option<TimeType>) -> Vec<Datum> {
        (0..5)
            .map(|step| match kind {
                None => Datum::Duration(MySqlDuration::from_raw_parts(step * 1_000_000_000, 0)),
                Some(kind) => Datum::Time(
                    Time::from_date_checked(
                        2020,
                        1,
                        if kind == TimeType::Date {
                            1 + step as i32
                        } else {
                            1
                        },
                        0,
                        0,
                        if kind == TimeType::Date {
                            0
                        } else {
                            step as i32
                        },
                        0,
                        kind,
                        0,
                    )
                    .unwrap(),
                ),
            })
            .collect()
    }

    fn column(values: &[Datum]) -> ColumnStats {
        let mut cms = CmsSketch::new(5, 2048);
        for (value, count) in values.iter().zip([1, 2, 7, 13, 77]) {
            cms.insert_bytes_by_count(
                &tidb_tablecodec::encode_table_value(None, value).unwrap(),
                count,
            );
        }
        ColumnStats {
            histogram: Histogram {
                ndv: 5,
                buckets: vec![tidb_stats::Bucket {
                    count: 100,
                    repeat: 77,
                    ndv: 5,
                    lower_bound: values[0].clone(),
                    upper_bound: values[4].clone(),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: Some(cms),
            stats_ver: VERSION1,
            unsigned: false,
        }
    }

    fn estimate(
        column: &ColumnStats,
        low: Datum,
        high: Datum,
        low_exclude: bool,
        high_exclude: bool,
    ) -> f64 {
        get_column_row_count(
            column,
            &[ColumnRange {
                low,
                high,
                low_exclude,
                high_exclude,
            }],
            Collation::Binary,
            200,
            100,
            false,
            EstimatorOptions::default(),
        )
        .unwrap()
        .est
    }

    #[test]
    fn version_one_cms_queries_use_the_typed_value_codec() {
        let mut cases = vec![
            (0..5).map(|i| Datum::Real(i as f64)).collect::<Vec<_>>(),
            (0..5).map(|i| Datum::Bytes(vec![b'a' + i])).collect(),
        ];
        for kind in [
            None,
            Some(TimeType::Date),
            Some(TimeType::DateTime),
            Some(TimeType::Timestamp),
        ] {
            cases.push(temporal_values(kind));
        }
        for values in cases {
            let mut column = column(&values);
            assert_eq!(
                estimate(&column, values[2].clone(), values[2].clone(), false, false),
                14.0,
                "{:?}",
                values[2]
            );
            let mut topn = TopN::new(1);
            topn.append(
                &tidb_tablecodec::encode_table_value(None, &values[2]).unwrap(),
                9,
            );
            column.topn = Some(topn);
            assert_eq!(
                estimate(&column, values[2].clone(), values[2].clone(), false, false),
                18.0,
                "TopN: {:?}",
                values[2]
            );
        }
    }

    #[test]
    fn version_one_temporal_ranges_use_statistics_enumeration() {
        for kind in [
            None,
            Some(TimeType::Date),
            Some(TimeType::DateTime),
            Some(TimeType::Timestamp),
        ] {
            let values = temporal_values(kind);
            let column = column(&values);
            for (low_exclude, high_exclude, expected) in [
                (false, false, 44.0),
                (true, false, 40.0),
                (false, true, 18.0),
                (true, true, 14.0),
            ] {
                assert_eq!(
                    estimate(
                        &column,
                        values[1].clone(),
                        values[3].clone(),
                        low_exclude,
                        high_exclude
                    ),
                    expected,
                    "{kind:?}: {low_exclude}/{high_exclude}"
                );
            }
        }
    }
}

#[cfg(test)]
mod value_encoding_error_tests {
    use super::*;

    #[test]
    fn version_one_value_encoding_errors_are_not_zero_estimates() {
        let value = Datum::Raw(vec![1]);
        let column = ColumnStats {
            histogram: Histogram {
                ndv: 0,
                buckets: vec![tidb_stats::Bucket {
                    count: 1,
                    repeat: 1,
                    ndv: 0,
                    lower_bound: value.clone(),
                    upper_bound: value.clone(),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: Some(CmsSketch::new(5, 2048)),
            stats_ver: VERSION1,
            unsigned: false,
        };
        assert!(matches!(
            equal_row_count_on_column(
                &column,
                &value,
                &[],
                Collation::Binary,
                1,
                0,
                EstimatorOptions::default(),
            ),
            Err(EstimationError::TableValue(
                tidb_tablecodec::TableRowError::Codec(tidb_codec::CodecError::UnsupportedDatum(
                    "raw"
                ))
            ))
        ));
    }
}

#[cfg(test)]
mod index_range_collation_tests {
    use super::*;

    #[test]
    fn index_range_prefix_comparison_retains_ranger_collation() {
        let collation = Collation::Utf8Mb4GeneralCi;
        let range = IndexRangeDatums {
            collators: vec![collation],
            low_val: vec![Datum::new_collation_string("A", collation)],
            high_val: vec![Datum::new_collation_string("a", collation)],
            low_exclude: false,
            high_exclude: false,
        };
        assert_eq!(ordinal_of_range_cond(&range), 1);
        assert!(crate::access_path::only_point_ranges(
            crate::access_path::PointRangePath::Index { column_count: 1 },
            std::slice::from_ref(&range),
        ));
        let mut binary_range = range.clone();
        binary_range.collators[0] = Collation::Binary;
        assert_eq!(ordinal_of_range_cond(&binary_range), 0);
        assert!(!crate::access_path::only_point_ranges(
            crate::access_path::PointRangePath::Index { column_count: 1 },
            &[binary_range],
        ));
    }

    #[test]
    fn range_prefix_and_point_checks_use_their_source_collator_positions() {
        let range = IndexRangeDatums {
            low_val: vec![Datum::Int(1), Datum::new_string("A")],
            high_val: vec![Datum::Int(1), Datum::new_string("a")],
            collators: vec![Collation::Binary, Collation::Utf8Mb4GeneralCi],
            low_exclude: false,
            high_exclude: false,
        };
        assert_eq!(ordinal_of_range_cond(&range), 1);
        assert!(crate::access_path::only_point_ranges(
            crate::access_path::PointRangePath::Index { column_count: 2 },
            &[range],
        ));
    }
}

#[cfg(test)]
mod version_one_index_dispatch_tests {
    use super::*;

    fn index() -> IndexStats {
        let mut cms = CmsSketch::new(5, 2048);
        for (value, count) in [1, 2, 7, 13, 77].into_iter().enumerate() {
            cms.insert_bytes_by_count(&encode_key(&[Datum::Int(value as i64)]).unwrap(), count);
        }
        IndexStats {
            histogram: Histogram {
                id: 1,
                ndv: 5,
                buckets: vec![tidb_stats::Bucket {
                    count: 100,
                    repeat: 77,
                    ndv: 5,
                    lower_bound: Datum::Bytes(encode_key(&[Datum::Int(0)]).unwrap()),
                    upper_bound: Datum::Bytes(encode_key(&[Datum::Int(4)]).unwrap()),
                }],
                ..Histogram::default()
            },
            topn: None,
            cms: Some(cms),
            stats_ver: VERSION1,
            num_columns: 1,
            unique: false,
        }
    }

    #[test]
    fn version_one_index_enumeration_uses_cms_without_v2_growth_scaling() {
        let index = index();
        let range = IndexRangeDatums {
            low_val: vec![Datum::Int(1)],
            high_val: vec![Datum::Int(3)],
            collators: vec![Collation::Binary],
            low_exclude: false,
            high_exclude: false,
        };
        let context =
            IndexEstimationStats::new(Some(&index), vec![None], IndexRowCounts::unscaled(200, 100));
        let estimate =
            get_index_row_count(&context, &[], &[], &[range], EstimatorOptions::default()).unwrap();
        assert!((estimate.est - 22.0).abs() < 1e-10, "{estimate:?}");
    }

    fn range(low: Vec<Datum>, high: Vec<Datum>) -> IndexRangeDatums {
        IndexRangeDatums {
            collators: vec![Collation::Binary; low.len()],
            low_val: low,
            high_val: high,
            low_exclude: false,
            high_exclude: false,
        }
    }

    fn topn_column(value: i64, count: u64) -> ColumnStats {
        let mut topn = TopN::new(1);
        topn.append(&encode_key(&[Datum::Int(value)]).unwrap(), count);
        ColumnStats {
            histogram: Histogram {
                ndv: 1,
                ..Histogram::default()
            },
            topn: Some(topn),
            cms: None,
            stats_ver: VERSION2,
            unsigned: false,
        }
    }

    #[test]
    fn version_one_suffix_prefers_the_first_index_even_when_invalid() {
        let mut index = index();
        index.num_columns = 2;
        let column = topn_column(15, 7);
        let context = IndexEstimationStats::new(
            Some(&index),
            vec![None, Some(&column)],
            IndexRowCounts::unscaled(100, 0),
        );
        let range = range(
            vec![Datum::Int(1), Datum::Int(10)],
            vec![Datum::Int(1), Datum::Int(20)],
        );
        let missing =
            IndexEstimationStats::new(None, vec![Some(&column)], IndexRowCounts::unscaled(100, 0));
        let empty = IndexStats {
            histogram: Histogram::default(),
            cms: None,
            topn: None,
            stats_ver: VERSION2,
            num_columns: 1,
            unique: false,
        };
        let invalid = IndexEstimationStats::new(
            Some(&empty),
            vec![Some(&column)],
            IndexRowCounts::unscaled(100, 0),
        );
        for first in [missing, invalid] {
            let actual = get_index_row_count(
                &context,
                &[],
                &[Vec::new(), vec![first]],
                std::slice::from_ref(&range),
                EstimatorOptions::default(),
            )
            .unwrap();
            assert!((actual.est - 0.05).abs() < 1e-10, "{actual:?}");
        }
        let column_fallback =
            get_index_row_count(&context, &[], &[], &[range], EstimatorOptions::default()).unwrap();
        assert!(
            (column_fallback.est - 2.0).abs() < 1e-10,
            "{column_fallback:?}"
        );
    }

    #[test]
    fn version_one_cross_validation_retains_handle_and_raw_ndv_metadata() {
        let mut index = index();
        index.num_columns = 2;
        let column = topn_column(1, 10);
        let mut context = IndexEstimationStats::new(
            Some(&index),
            vec![Some(&column)],
            IndexRowCounts::unscaled(100, 0),
        );
        let point = range(vec![Datum::Int(1)], vec![Datum::Int(1)]);
        let estimate = get_index_row_count(
            &context,
            &[],
            &[],
            std::slice::from_ref(&point),
            EstimatorOptions::default(),
        )
        .unwrap();
        assert_eq!(estimate.est, 2.0);
        context.column_is_handle[0] = true;
        assert_eq!(
            get_index_row_count(&context, &[], &[], &[point], EstimatorOptions::default())
                .unwrap()
                .est,
            1.0
        );
        context.columns[0] = None;
        context.column_ndvs[0] = Some(1000);
        context.row_counts = IndexRowCounts::unscaled(200, 100);
        let outside = range(vec![Datum::Int(9)], vec![Datum::Int(9)]);
        let estimate =
            get_index_row_count(&context, &[], &[], &[outside], EstimatorOptions::default())
                .unwrap();
        assert!((estimate.est - 0.1).abs() < 1e-10, "{estimate:?}");
    }

    #[test]
    fn version_one_empty_enumeration_and_exact_unique_width_follow_go() {
        let mut index = index();
        let context =
            IndexEstimationStats::new(Some(&index), vec![None], IndexRowCounts::unscaled(200, 100));
        let mut empty = range(vec![Datum::Int(1)], vec![Datum::Int(2)]);
        empty.low_exclude = true;
        empty.high_exclude = true;
        assert_eq!(
            get_index_row_count(&context, &[], &[], &[empty], EstimatorOptions::default())
                .unwrap()
                .est,
            0.0
        );
        index.unique = true;
        index
            .cms
            .as_mut()
            .unwrap()
            .insert_bytes_by_count(&encode_key(&[Datum::Int(1), Datum::Int(2)]).unwrap(), 7);
        let context =
            IndexEstimationStats::new(Some(&index), vec![None], IndexRowCounts::unscaled(100, 0));
        let point = range(
            vec![Datum::Int(1), Datum::Int(2)],
            vec![Datum::Int(1), Datum::Int(2)],
        );
        let estimate =
            get_index_row_count(&context, &[], &[], &[point], EstimatorOptions::default()).unwrap();
        assert!((estimate.est - 7.0).abs() < 1e-10, "{estimate:?}");
    }

    #[test]
    fn version_two_backoff_recurses_through_v1_with_the_first_collator() {
        let mut candidate = index();
        candidate.histogram.ndv = 2;
        candidate.histogram.buckets[0].lower_bound =
            Datum::Bytes(encode_key(&[Datum::new_string("A")]).unwrap());
        candidate.histogram.buckets[0].upper_bound =
            Datum::Bytes(encode_key(&[Datum::new_string("a")]).unwrap());
        candidate.histogram.buckets[0].repeat = 80;
        let mut cms = CmsSketch::new(5, 2048);
        cms.insert_bytes_by_count(&encode_key(&[Datum::new_string("A")]).unwrap(), 20);
        cms.insert_bytes_by_count(&encode_key(&[Datum::new_string("a")]).unwrap(), 80);
        candidate.cms = Some(cms);
        let context = IndexEstimationStats::new(
            Some(&candidate),
            vec![None],
            IndexRowCounts::unscaled(100, 0),
        );
        let mut range = range(
            vec![Datum::Int(1), Datum::new_string("A")],
            vec![Datum::Int(1), Datum::new_string("a")],
        );
        range.collators = vec![Collation::Utf8Mb4GeneralCi, Collation::Binary];
        let recursive = [Vec::new(), vec![context]];
        let mut main = index();
        main.stats_ver = VERSION2;
        main.cms = None;
        main.num_columns = 2;
        main.histogram.ndv = 2;
        main.histogram.buckets[0].repeat = 80;
        main.histogram.buckets[0].lower_bound =
            Datum::Bytes(encode_key(&[Datum::Int(1), Datum::new_string("A")]).unwrap());
        main.histogram.buckets[0].upper_bound =
            Datum::Bytes(encode_key(&[Datum::Int(1), Datum::new_string("a")]).unwrap());
        let columns = vec![None, None];
        let estimate = exp_backoff_estimation(
            &main,
            &columns,
            &[],
            &recursive,
            &range,
            100,
            0,
            EstimatorOptions::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(estimate, (0.2, 0.2, 0.2));
        let context =
            IndexEstimationStats::new(Some(&main), columns, IndexRowCounts::unscaled(100, 0));
        // The full V2 estimator adds one out-of-range row for the inclusive
        // upper endpoint's encoded PrefixNext, as the Go oracle does.
        let estimate = get_index_row_count(
            &context,
            &[],
            &recursive,
            &[range],
            EstimatorOptions::default(),
        )
        .unwrap();
        assert!((estimate.est - 21.0).abs() < 1e-10, "{estimate:?}");
    }
}
