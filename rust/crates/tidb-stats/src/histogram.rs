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

//! Column/index selectivity histogram and bucket row-count estimation from
//! `pkg/statistics/histogram.go` and `pkg/statistics/scalar.go`.
//!
//! Go stores bucket bounds as two rows (lower at `2*i`, upper at `2*i+1`) in
//! a shared `chunk.Chunk`. This port replaces that chunk machinery with a
//! per-bucket `(lower, upper)` [`Datum`] pair held directly on [`Bucket`] --
//! functionally equivalent for every estimation method ported here, without
//! needing the chunk/column-codec dependency. Loading histograms from KV
//! storage, merging, sampling, and protobuf wire conversion are explicit
//! future-unit owners (see the crate module docs).

use tidb_datatype::{Collation, Datum, Time};

use crate::row_estimate::{default_row_est, RowEstimate};

/// A single histogram bucket.
///
/// `count` is the *cumulative* row count through this bucket (matches Go's
/// `Bucket.Count`, which counts every prior bucket plus this one).
#[derive(Debug, Clone)]
pub struct Bucket {
    /// Cumulative row count through this bucket (Go `Bucket.Count`).
    pub count: i64,
    /// Number of times the upper bound value repeats (Go `Bucket.Repeat`).
    pub repeat: i64,
    /// Number of distinct values in the bucket (Go `Bucket.NDV`).
    pub ndv: i64,
    /// Bucket lower bound.
    pub lower_bound: Datum,
    /// Bucket upper bound.
    pub upper_bound: Datum,
}

/// Column/index histogram statistics, ported from Go `Histogram`.
///
/// Field naming follows the Go struct: `id` is the column/index ID, `ndv` is
/// the histogram-level number-of-distinct-values estimate (including values
/// folded into an accompanying TopN), `null_count` counts NULLs,
/// `tot_col_size` is the total encoded column size, and `correlation` is the
/// Pearson-style physical/logical ordering correlation (only meaningful for
/// column histograms).
#[derive(Debug, Clone, Default)]
pub struct Histogram {
    /// Column or index ID.
    pub id: i64,
    /// Number of distinct values, including any excluded into TopN.
    pub ndv: i64,
    /// Number of NULL values.
    pub null_count: i64,
    /// Version this histogram was last updated at.
    pub last_update_version: u64,
    /// Total column size (LEN + BYTE for unfixed-length types).
    pub tot_col_size: i64,
    /// Physical/logical ordering correlation in `[-1, 1]`. Column-only.
    pub correlation: f64,
    /// Buckets in ascending order.
    pub buckets: Vec<Bucket>,
}

/// `pkg/statistics/scalar.go`'s `calcFraction`: fraction of `[lower, upper]`
/// covered by `[lower, value]` under the continuous-value assumption.
#[must_use]
pub fn calc_fraction(lower: f64, upper: f64, value: f64) -> f64 {
    if upper <= lower {
        return 0.5;
    }
    if value <= lower {
        return 0.0;
    }
    if value >= upper {
        return 1.0;
    }
    let frac = (value - lower) / (upper - lower);
    if frac.is_nan() || frac.is_infinite() || !(0.0..=1.0).contains(&frac) {
        return 0.5;
    }
    frac
}

/// `pkg/statistics/scalar.go`'s `commonPrefixLength`.
#[must_use]
pub fn common_prefix_length(a: &[u8], b: &[u8]) -> usize {
    let min_len = a.len().min(b.len());
    for i in 0..min_len {
        if a[i] != b[i] {
            return i;
        }
    }
    min_len
}

/// `pkg/statistics/scalar.go`'s `convertBytesToScalar`: treats up to the
/// first 8 bytes as a big-endian, left-padded base-256 value.
#[must_use]
pub fn convert_bytes_to_scalar(value: &[u8]) -> f64 {
    let mut buf = [0_u8; 8];
    let n = value.len().min(8);
    buf[..n].copy_from_slice(&value[..n]);
    u64::from_be_bytes(buf) as f64
}

fn min_datetime_core() -> tidb_datatype::CoreTime {
    tidb_datatype::CoreTime::from_date(1, 1, 1, 0, 0, 0, 0)
}

fn min_timestamp_core() -> tidb_datatype::CoreTime {
    tidb_datatype::CoreTime::from_date(1970, 1, 1, 0, 0, 1, 0)
}

fn time_to_scalar(value: Time) -> f64 {
    // Go subtracts a per-kind minimum time to get a `time.Duration` and
    // takes its nanosecond count. For DATE/DATETIME, Go's `Time.Sub` builds
    // that duration as `seconds*1e9 + microseconds*1e3` using plain `int64`
    // arithmetic, which silently *wraps* on overflow (`1e9` seconds worth of
    // 2000+ year gaps against `MinDatetime` routinely overflow `int64`
    // nanoseconds). `tidb_datatype::Time::sub` instead saturates to avoid UB,
    // which would diverge from Go's wrapped value here, so this port
    // reimplements the DATE/DATETIME branch's wrapping arithmetic directly.
    // The TIMESTAMP branch uses actual (non-overflowing, post-1970) instants
    // and can use `Time::sub` as-is.
    let min_kind = value.kind();
    if min_kind == tidb_datatype::TimeType::Timestamp {
        let min_time = Time::new(min_timestamp_core(), min_kind, tidb_datatype::DEFAULT_FSP)
            .expect("min-time construction cannot fail for a fixed calendar date");
        return match value.sub(min_time, &chrono_tz::UTC) {
            Ok(duration) => duration.nanoseconds() as f64,
            Err(_) => 0.0,
        };
    }
    let diff = value.core_time().time_diff(min_datetime_core(), 1);
    let magnitude = diff
        .seconds
        .wrapping_mul(1_000_000_000)
        .wrapping_add(i64::from(diff.microseconds).wrapping_mul(1_000));
    (if diff.negative { -magnitude } else { magnitude }) as f64
}

/// `pkg/statistics/scalar.go`'s `convertDatumToScalar`.
#[must_use]
pub fn convert_datum_to_scalar(value: &Datum, common_pfx_len: usize) -> f64 {
    match value {
        Datum::Float32(v) | Datum::Real(v) => *v,
        Datum::Int(v) => *v as f64,
        Datum::UInt(v) => *v as f64,
        Datum::Duration(v) => v.nanoseconds() as f64,
        Datum::Decimal(v) => v.to_f64(),
        Datum::Time(v) => time_to_scalar(*v),
        Datum::String(v) => bytes_to_scalar(v.bytes(), common_pfx_len),
        Datum::Bytes(v) => bytes_to_scalar(v, common_pfx_len),
        Datum::MinNotNull => -f64::MAX,
        Datum::MaxValue => f64::MAX,
        _ => 0.0,
    }
}

fn bytes_to_scalar(bytes: &[u8], common_pfx_len: usize) -> f64 {
    if bytes.len() <= common_pfx_len {
        0.0
    } else {
        convert_bytes_to_scalar(&bytes[common_pfx_len..])
    }
}

/// `pkg/statistics/scalar.go`'s `calcFraction4Datums`: fraction computed
/// directly from a `(lower, upper, value)` datum triple, without a
/// precomputed scalar cache. This crate always takes this path (no
/// `Histogram.Scalars` cache is ported) since bucket bounds are already
/// plain [`Datum`]s here.
#[must_use]
pub fn calc_fraction_from_datums(lower: &Datum, upper: &Datum, value: &Datum) -> f64 {
    match value {
        Datum::Float32(v) | Datum::Real(v) => calc_fraction(
            convert_datum_to_scalar(lower, 0),
            convert_datum_to_scalar(upper, 0),
            *v,
        ),
        Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Duration(_)
        | Datum::Decimal(_)
        | Datum::Time(_) => calc_fraction(
            convert_datum_to_scalar(lower, 0),
            convert_datum_to_scalar(upper, 0),
            convert_datum_to_scalar(value, 0),
        ),
        Datum::String(_) | Datum::Bytes(_) => {
            let lower_bytes = datum_bytes(lower);
            let upper_bytes = datum_bytes(upper);
            let common_pfx_len = common_prefix_length(lower_bytes, upper_bytes);
            calc_fraction(
                convert_datum_to_scalar(lower, common_pfx_len),
                convert_datum_to_scalar(upper, common_pfx_len),
                convert_datum_to_scalar(value, common_pfx_len),
            )
        }
        _ => 0.5,
    }
}

fn datum_bytes(value: &Datum) -> &[u8] {
    match value {
        Datum::String(v) => v.bytes(),
        Datum::Bytes(v) => v,
        _ => &[],
    }
}

/// Outcome of [`Histogram::locate_bucket`], ported from Go `LocateBucket`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketLocation {
    /// The value exceeds the upper bound of the last bucket.
    pub exceed: bool,
    /// Assuming `!exceed`, which bucket the value falls in or before.
    pub bucket_idx: usize,
    /// Assuming `!exceed`, whether the value falls inside this bucket
    /// (rather than in the gap before it).
    pub in_bucket: bool,
    /// Assuming `in_bucket`, whether the value equals the bucket's upper
    /// bound (its `repeat`-counted value).
    pub match_last_value: bool,
}

impl Histogram {
    /// Number of buckets, Go `Histogram.Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// True when there are no buckets.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Row count contributed by bucket `idx` alone, Go `BucketCount`.
    #[must_use]
    pub fn bucket_count(&self, idx: usize) -> i64 {
        if idx == 0 {
            self.buckets[0].count
        } else {
            self.buckets[idx].count - self.buckets[idx - 1].count
        }
    }

    /// Count of non-NULL values, Go `NotNullCount`.
    #[must_use]
    pub fn not_null_count(&self) -> f64 {
        match self.buckets.last() {
            Some(bucket) => bucket.count as f64,
            None => 0.0,
        }
    }

    /// Total row count including NULLs, Go `TotalRowCount`.
    #[must_use]
    pub fn total_row_count(&self) -> f64 {
        self.not_null_count() + self.null_count as f64
    }

    /// Locates where `value` falls relative to the buckets, Go `LocateBucket`.
    ///
    /// Comparisons use `collation` for string/bytes bounds; pass
    /// [`Collation::Binary`] for non-string types or binary collations.
    #[must_use]
    pub fn locate_bucket(&self, value: &Datum, collation: Collation) -> BucketLocation {
        if self.buckets.is_empty() {
            return BucketLocation {
                exceed: true,
                bucket_idx: 0,
                in_bucket: false,
                match_last_value: false,
            };
        }
        // Binary search over the flattened (lower, upper) sequence for the
        // first bound >= value, mirroring Go's `chunk.LowerBound` over the
        // two-row-per-bucket `Bounds` chunk.
        let n = self.buckets.len();
        let bound_at = |i: usize| -> &Datum {
            if i.is_multiple_of(2) {
                &self.buckets[i / 2].lower_bound
            } else {
                &self.buckets[i / 2].upper_bound
            }
        };
        let total = n * 2;
        let mut lo = 0_usize;
        let mut hi = total;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cmp = bound_at(mid)
                .compare(value, collation)
                .unwrap_or(std::cmp::Ordering::Less);
            if cmp == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let index = lo;
        let matched = index < total
            && bound_at(index)
                .compare(value, collation)
                .map(|ordering| ordering == std::cmp::Ordering::Equal)
                .unwrap_or(false);

        if index >= total {
            return BucketLocation {
                exceed: true,
                bucket_idx: n - 1,
                in_bucket: false,
                match_last_value: false,
            };
        }
        let bucket_idx = index / 2;
        if index.is_multiple_of(2) && !matched {
            return BucketLocation {
                exceed: false,
                bucket_idx,
                in_bucket: false,
                match_last_value: false,
            };
        }
        let upper_eq = self.buckets[bucket_idx]
            .upper_bound
            .compare(value, collation)
            .map(|ordering| ordering == std::cmp::Ordering::Equal)
            .unwrap_or(false);
        if (index % 2 == 1 && matched) || upper_eq {
            return BucketLocation {
                exceed: false,
                bucket_idx,
                in_bucket: true,
                match_last_value: true,
            };
        }
        BucketLocation {
            exceed: false,
            bucket_idx,
            in_bucket: true,
            match_last_value: false,
        }
    }

    /// Fraction of bucket `index`'s interval covered by `[lower, value]`,
    /// Go `Histogram.calcFraction`.
    #[must_use]
    pub fn calc_fraction(&self, index: usize, value: &Datum) -> f64 {
        let bucket = &self.buckets[index];
        calc_fraction_from_datums(&bucket.lower_bound, &bucket.upper_bound, value)
    }

    /// Estimates the row count where the column equals `value`, Go
    /// `EqualRowCount`. `matched` is true when the estimate comes from a
    /// bucket's repeat count or bucket-level NDV (more accurate).
    #[must_use]
    pub fn equal_row_count(
        &self,
        value: &Datum,
        has_bucket_ndv: bool,
        collation: Collation,
    ) -> (f64, bool) {
        let location = self.locate_bucket(value, collation);
        if !location.in_bucket {
            return (0.0, false);
        }
        let bucket = &self.buckets[location.bucket_idx];
        if location.match_last_value {
            return (bucket.repeat as f64, true);
        }
        if has_bucket_ndv && bucket.ndv > 1 {
            let bucket_count = self.bucket_count(location.bucket_idx);
            return (
                (bucket_count - bucket.repeat) as f64 / (bucket.ndv - 1) as f64,
                true,
            );
        }
        (self.not_null_count() / self.ndv as f64, false)
    }

    /// Estimates the row count where the column is less than `value`,
    /// returning the bucket index too, Go `LessRowCountWithBktIdx`.
    #[must_use]
    pub fn less_row_count_with_bkt_idx(&self, value: &Datum, collation: Collation) -> (f64, usize) {
        if self.buckets.is_empty() {
            return (0.0, 0);
        }
        let location = self.locate_bucket(value, collation);
        if location.exceed {
            return (self.not_null_count(), self.len() - 1);
        }
        let pre_count = if location.bucket_idx > 0 {
            self.buckets[location.bucket_idx - 1].count as f64
        } else {
            0.0
        };
        if !location.in_bucket {
            return (pre_count, location.bucket_idx);
        }
        let bucket = &self.buckets[location.bucket_idx];
        let cur_count = bucket.count as f64;
        let cur_repeat = bucket.repeat as f64;
        if location.match_last_value {
            return (cur_count - cur_repeat, location.bucket_idx);
        }
        let fraction = self.calc_fraction(location.bucket_idx, value);
        (
            pre_count + fraction * (cur_count - cur_repeat - pre_count),
            location.bucket_idx,
        )
    }

    /// Estimates the row count where the column is less than `value`, Go
    /// `LessRowCount`.
    #[must_use]
    pub fn less_row_count(&self, value: &Datum, collation: Collation) -> f64 {
        self.less_row_count_with_bkt_idx(value, collation).0
    }

    /// Estimates the row count where the column is greater than `value`, Go
    /// `GreaterRowCount`. Deprecated upstream; kept for test parity.
    #[must_use]
    pub fn greater_row_count(&self, value: &Datum, collation: Collation) -> f64 {
        let (hist_row_count, _) = self.equal_row_count(value, false, collation);
        let gt_count =
            self.not_null_count() - self.less_row_count(value, collation) - hist_row_count;
        gt_count.max(0.0)
    }

    /// Estimates the row count in `[a, b)`, Go `BetweenRowCount`.
    ///
    /// `skew` carries the session's `RiskRangeSkewRatio`; `None` stands for
    /// Go's nil `sctx` (stats version 1 callers), which skips the whole
    /// same-bucket skew branch including its `MaxEst` widening.
    #[must_use]
    pub fn between_row_count(
        &self,
        a: &Datum,
        b: &Datum,
        collation: Collation,
        skew: Option<f64>,
    ) -> RowEstimate {
        let (less_count_a, bkt_index_a) = self.less_row_count_with_bkt_idx(a, collation);
        let (less_count_b, bkt_index_b) = self.less_row_count_with_bkt_idx(b, collation);
        let mut range_est = default_row_est(less_count_b - less_count_a);
        let (low_equal, _) = self.equal_row_count(a, false, collation);
        let ndv_avg = self.not_null_count() / self.ndv as f64;
        if range_est.est < low_equal.max(ndv_avg) && self.ndv > 0 {
            let result = less_count_b.min(self.not_null_count() - less_count_a);
            range_est = default_row_est(result.min(low_equal + ndv_avg));
        }
        // Equal less-counts mean no valid bucket was crossed (or both bounds
        // are out of range), so there is no in-bucket skew to account for.
        let in_valid_bucket = less_count_a != less_count_b;
        if let Some(skew_ratio) = skew {
            if in_valid_bucket && bkt_index_a == bkt_index_b {
                let bucket = &self.buckets[bkt_index_a];
                let mut skew_estimate = self.bucket_count(bkt_index_a);
                if less_count_b <= (bucket.count - bucket.repeat) as f64 {
                    skew_estimate -= bucket.repeat;
                }
                let skew_estimate = skew_estimate as f64;
                if skew_ratio > 0.0 {
                    range_est = crate::row_estimate::calculate_skew_ratio_counts(
                        range_est.est,
                        (range_est.est * 2.0).min(skew_estimate),
                        skew_ratio,
                    );
                }
                range_est.max_est = range_est.max_est.max(skew_estimate);
            }
        }
        range_est
    }

    /// Go `Histogram.OutOfRange`: whether `value` falls outside every bucket.
    #[must_use]
    pub fn out_of_range(&self, value: &Datum, collation: Collation) -> bool {
        let (Some(first), Some(last)) = (self.buckets.first(), self.buckets.last()) else {
            return false;
        };
        let greater = first
            .lower_bound
            .compare(value, collation)
            .is_ok_and(|ordering| ordering == std::cmp::Ordering::Greater);
        let less = last
            .upper_bound
            .compare(value, collation)
            .is_ok_and(|ordering| ordering == std::cmp::Ordering::Less);
        greater || less
    }

    /// Go `Histogram.AbsRowCountDifference`.
    #[must_use]
    pub fn abs_row_count_difference(&self, realtime_row_count: i64) -> f64 {
        (realtime_row_count as f64 - self.total_row_count()).abs()
    }

    /// Go `Histogram.GetIncreaseFactor`.
    #[must_use]
    pub fn get_increase_factor(&self, total_count: i64) -> f64 {
        let column_count = self.total_row_count();
        if column_count == 0.0 {
            return 1.0;
        }
        total_count as f64 / column_count
    }

    /// Estimates rows for the part of `[l, r]` that lies outside the analyzed
    /// histogram, Go `Histogram.OutOfRangeRowCount`.
    ///
    /// The table and session inputs come from [`OutOfRangeContext`].
    #[must_use]
    pub fn out_of_range_row_count(
        &self,
        l_datum: &Datum,
        r_datum: &Datum,
        context: OutOfRangeContext,
    ) -> RowEstimate {
        let OutOfRangeContext {
            realtime_row_count,
            modify_count,
            hist_ndv,
            unsigned,
            allow_use_modify_count,
            skew_ratio,
        } = context;
        if self.is_empty() {
            return default_row_est(0.0);
        }
        let mut realtime_row_count = realtime_row_count;
        let hist_ndv = hist_ndv.max(1);
        let mut one_value = self.not_null_count() / hist_ndv as f64;
        if !allow_use_modify_count {
            return default_row_est(one_value);
        }
        if (hist_ndv as f64) < OUT_OF_RANGE_BETWEEN_RATE {
            one_value = one_value
                .min(realtime_row_count as f64 / OUT_OF_RANGE_BETWEEN_RATE)
                .max(1.0);
        }

        let first_lower = &self.buckets[0].lower_bound;
        let last_upper = &self.buckets[self.len() - 1].upper_bound;
        let common_prefix = if matches!(first_lower, Datum::Bytes(_) | Datum::String(_)) {
            crate::scalar_geometry::common_prefix_length(&[
                datum_bytes(first_lower),
                datum_bytes(last_upper),
                datum_bytes(l_datum),
                datum_bytes(r_datum),
            ])
        } else {
            0
        };

        let mut l = convert_datum_to_scalar(l_datum, common_prefix);
        let mut r = convert_datum_to_scalar(r_datum, common_prefix);
        if unsigned {
            let mut left_clamped = false;
            let mut right_clamped = false;
            if l < 0.0 {
                l = 0.0;
                left_clamped = true;
            }
            if r < 0.0 {
                r = 0.0;
                right_clamped = true;
            }
            if l == 0.0 && r == 0.0 && (left_clamped || right_clamped) {
                return default_row_est(0.0);
            }
        }

        let hist_l = convert_datum_to_scalar(first_lower, common_prefix);
        let hist_r = convert_datum_to_scalar(last_upper, common_prefix);
        let mut hist_width = hist_r - hist_l;
        if hist_width < 0.0 || hist_width.is_infinite() {
            hist_width = 0.0;
        }
        let bound_l = hist_l - hist_width;
        let bound_r = hist_r + hist_width;

        let pred_width = r - l;
        if pred_width < 0.0 {
            return default_row_est(0.0);
        }
        if pred_width == 0.0 {
            hist_width = 0.0;
        }

        let left_percent =
            crate::overlap_geometry::left_overlap_percent(l, r, bound_l, hist_l, hist_width);
        let right_percent =
            crate::overlap_geometry::right_overlap_percent(l, r, hist_r, bound_r, hist_width);
        let total_percent = (left_percent * 0.5 + right_percent * 0.5).min(1.0);
        let max_total_percent = (left_percent + right_percent).min(1.0);

        let added_rows = self.abs_row_count_difference(realtime_row_count);
        let mut max_added_rows = added_rows;

        let mut est_rows = one_value;
        if total_percent > 0.0 {
            // 50% of the changed rows are assumed to land outside the analyzed
            // range unless the session overrides that share.
            let added_row_multiplier = if skew_ratio > 0.0 { skew_ratio } else { 0.5 };
            est_rows = (added_rows * added_row_multiplier) * total_percent;
        }

        if modify_count == 0 || added_rows == 0.0 {
            if realtime_row_count <= 0 {
                realtime_row_count = self.total_row_count() as i64;
            }
            max_added_rows =
                max_added_rows.max(realtime_row_count as f64 / OUT_OF_RANGE_BETWEEN_RATE);
        }
        if max_total_percent > 0.0 {
            max_added_rows *= max_total_percent;
        }

        // The source assigns MinEst first and lets the skew branch overwrite
        // the whole estimate, so a positive skew ratio drops that assignment.
        let mut result = RowEstimate {
            est: 0.0,
            min_est: est_rows.min(one_value),
            max_est: 0.0,
        };
        if skew_ratio > 0.0 {
            result = crate::row_estimate::calculate_skew_ratio_counts(
                est_rows,
                max_added_rows,
                skew_ratio,
            );
        } else {
            result.est = est_rows;
        }
        result.est = result.est.max(one_value);
        result.max_est = result.est.max(max_added_rows);
        result
    }
}

/// Go `outOfRangeBetweenRate`, the smoothing divisor for out-of-range work.
pub const OUT_OF_RANGE_BETWEEN_RATE: f64 = 100.0;

/// The table and session inputs of [`Histogram::out_of_range_row_count`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OutOfRangeContext {
    /// The table's current row count.
    pub realtime_row_count: i64,
    /// Rows changed since the histogram was analyzed.
    pub modify_count: i64,
    /// Histogram NDV with any TopN entries already removed.
    pub hist_ndv: i64,
    /// The source's `mysql.HasUnsignedFlag(hg.Tp.GetFlag())`.
    pub unsigned: bool,
    /// False only under `OptObjectiveDeterminate`, which bans modify counts.
    pub allow_use_modify_count: bool,
    /// The session's `RiskRangeSkewRatio`.
    pub skew_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calc_fraction_matches_edge_cases() {
        assert_eq!(calc_fraction(0.0, 0.0, 5.0), 0.5);
        assert_eq!(calc_fraction(0.0, 10.0, -1.0), 0.0);
        assert_eq!(calc_fraction(0.0, 10.0, 11.0), 1.0);
        assert_eq!(calc_fraction(0.0, 10.0, 5.0), 0.5);
    }

    #[test]
    fn common_prefix_length_matches_go() {
        assert_eq!(common_prefix_length(b"apple", b"apply"), 4);
        assert_eq!(common_prefix_length(b"abc", b"xyz"), 0);
        assert_eq!(common_prefix_length(b"abc", b"abc"), 3);
    }

    #[test]
    fn convert_bytes_to_scalar_matches_go_byte_widths() {
        assert_eq!(convert_bytes_to_scalar(&[]), 0.0);
        assert_eq!(convert_bytes_to_scalar(&[0x80]), (0x80_u64 << 56) as f64);
    }
}
