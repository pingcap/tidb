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
//! functionally equivalent for every estimation and merge method ported here,
//! without needing the chunk/column-codec dependency. Loading histograms from
//! KV storage, sampling, and protobuf wire conversion remain explicit future
//! owners (see the crate module docs).

use std::{cmp::Ordering, fmt};

use tidb_datatype::{Collation, Datum, DatumValueError, Time};

use crate::row_estimate::{default_row_est, RowEstimate};

/// A single histogram bucket.
///
/// `count` is the *cumulative* row count through this bucket (matches Go's
/// `Bucket.Count`, which counts every prior bucket plus this one).
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, Default, PartialEq)]
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

/// A TopN value reintroduced while partition histograms are merged.
///
/// This is the output boundary of Go `topNMetaToDatum`: column values are
/// decoded, while index values remain encoded key bytes inside a `Datum`.
/// Tablecodec framing itself remains outside this crate.
#[derive(Debug, Clone, PartialEq)]
pub struct TopNMergeEntry {
    /// Value compared with histogram bounds.
    pub value: Datum,
    /// Number of rows represented by this value.
    pub count: u64,
}

/// Source options for [`merge_partition_histograms`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionMergeOptions {
    /// Requested maximum number of output buckets.
    pub expected_buckets: usize,
    /// Whether bucket-level NDV is retained in the output.
    pub is_index: bool,
    /// Go analyze version, retained for the TopN construction contract.
    pub analyze_version: i64,
}

/// Failure from source-shaped histogram merging.
#[derive(Debug, Clone, PartialEq)]
pub enum HistogramMergeError {
    /// Go: `expBucketNumber can not be zero`.
    ZeroExpectedBuckets,
    /// Go: `not enough buckets to merge`.
    NotEnoughBuckets,
    /// Go: `illegal bucket order`.
    IllegalBucketOrder,
    /// A datum comparison failed.
    Datum(DatumValueError),
}

impl fmt::Display for HistogramMergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroExpectedBuckets => formatter.write_str("expBucketNumber can not be zero"),
            Self::NotEnoughBuckets => formatter.write_str("not enough buckets to merge"),
            Self::IllegalBucketOrder => formatter.write_str("illegal bucket order"),
            Self::Datum(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for HistogramMergeError {}

impl From<DatumValueError> for HistogramMergeError {
    fn from(error: DatumValueError) -> Self {
        Self::Datum(error)
    }
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
    /// Creates an empty histogram with the source metadata and allocation
    /// hint from Go `NewHistogram`.
    #[must_use]
    pub fn new(
        id: i64,
        ndv: i64,
        null_count: i64,
        last_update_version: u64,
        bucket_capacity: usize,
        tot_col_size: i64,
    ) -> Self {
        Self {
            id,
            ndv,
            null_count,
            last_update_version,
            tot_col_size,
            correlation: 0.0,
            buckets: Vec::with_capacity(bucket_capacity),
        }
    }

    /// Returns the lower bound of `bucket_index`, Go `GetLower`.
    #[must_use]
    pub fn get_lower(&self, bucket_index: usize) -> &Datum {
        &self.buckets[bucket_index].lower_bound
    }

    /// Copies the lower bound of `bucket_index`, Go `LowerToDatum`.
    #[must_use]
    pub fn lower_to_datum(&self, bucket_index: usize) -> Datum {
        self.get_lower(bucket_index).clone()
    }

    /// Returns the upper bound of `bucket_index`, Go `GetUpper`.
    #[must_use]
    pub fn get_upper(&self, bucket_index: usize) -> &Datum {
        &self.buckets[bucket_index].upper_bound
    }

    /// Copies the upper bound of `bucket_index`, Go `UpperToDatum`.
    #[must_use]
    pub fn upper_to_datum(&self, bucket_index: usize) -> Datum {
        self.get_upper(bucket_index).clone()
    }

    /// Appends a bucket without a bucket-level NDV, Go `AppendBucket`.
    pub fn append_bucket(
        &mut self,
        lower_bound: Datum,
        upper_bound: Datum,
        count: i64,
        repeat: i64,
    ) {
        self.append_bucket_with_ndv(lower_bound, upper_bound, count, repeat, 0);
    }

    /// Appends a bucket with its bucket-level NDV, Go `AppendBucketWithNDV`.
    pub fn append_bucket_with_ndv(
        &mut self,
        lower_bound: Datum,
        upper_bound: Datum,
        count: i64,
        repeat: i64,
        ndv: i64,
    ) {
        self.buckets.push(Bucket {
            count,
            repeat,
            ndv,
            lower_bound,
            upper_bound,
        });
    }

    fn update_last_bucket(
        &mut self,
        upper_bound: Datum,
        count: i64,
        repeat: i64,
        need_bucket_ndv: bool,
    ) {
        let bucket = self
            .buckets
            .last_mut()
            .expect("Go updateLastBucket requires a non-empty histogram");
        bucket.upper_bound = upper_bound;
        if need_bucket_ndv && bucket.ndv > 0 {
            bucket.ndv = bucket.ndv.wrapping_add(1);
        }
        bucket.count = count;
        bucket.repeat = repeat;
    }

    /// Removes one decoded TopN value from its containing bucket, Go
    /// `BinarySearchRemoveVal`.
    pub fn binary_search_remove_value(
        &mut self,
        value: &Datum,
        count: i64,
        collation: Collation,
    ) -> Result<(), DatumValueError> {
        let mut low = 0_usize;
        let Some(mut high) = self.len().checked_sub(1) else {
            return Ok(());
        };
        if self.len() > 4 {
            if self.buckets[high].upper_bound.compare(value, collation)? == Ordering::Less {
                return Ok(());
            }
            if self.buckets[low].lower_bound.compare(value, collation)? == Ordering::Greater {
                return Ok(());
            }
        }

        let mut found_at = None;
        while low <= high {
            let mid = low + (high - low) / 2;
            if self.buckets[mid].lower_bound.compare(value, collation)? == Ordering::Greater {
                let Some(next_high) = mid.checked_sub(1) else {
                    break;
                };
                high = next_high;
                continue;
            }
            let upper_order = self.buckets[mid].upper_bound.compare(value, collation)?;
            if upper_order == Ordering::Less {
                low = mid + 1;
                continue;
            }

            let bucket = &mut self.buckets[mid];
            if bucket.ndv > 0 {
                bucket.ndv -= 1;
            }
            if upper_order == Ordering::Equal {
                bucket.repeat = 0;
            }
            bucket.count = bucket.count.wrapping_sub(count).max(0);
            found_at = Some(mid);
            break;
        }
        if let Some(mid) = found_at {
            for bucket in &mut self.buckets[mid + 1..] {
                bucket.count = bucket.count.wrapping_sub(count).max(0);
            }
        }
        Ok(())
    }

    /// Removes sorted decoded TopN values from the histogram, Go
    /// `RemoveVals` at the post-tablecodec boundary.
    pub fn remove_values(
        &mut self,
        values: &[TopNMergeEntry],
        collation: Collation,
    ) -> Result<(), DatumValueError> {
        let mut total_sub_count = 0_i64;
        let mut value_index = 0_usize;
        for bucket in &mut self.buckets {
            while value_index < values.len() {
                if bucket
                    .lower_bound
                    .compare(&values[value_index].value, collation)?
                    == Ordering::Greater
                {
                    value_index += 1;
                    continue;
                }
                let upper_order = bucket
                    .upper_bound
                    .compare(&values[value_index].value, collation)?;
                if upper_order == Ordering::Less {
                    break;
                }
                total_sub_count = total_sub_count.wrapping_add(values[value_index].count as i64);
                if bucket.ndv > 0 {
                    bucket.ndv -= 1;
                }
                value_index += 1;
                if upper_order == Ordering::Equal {
                    bucket.repeat = 0;
                    break;
                }
            }
            bucket.count = bucket.count.wrapping_sub(total_sub_count).max(0);
        }
        Ok(())
    }

    /// Removes empty analyze-v2 index buckets and clears bucket NDV, Go
    /// `StandardizeForV2AnalyzeIndex`.
    pub fn standardize_for_v2_analyze_index(&mut self) {
        let mut previous_count = 0_i64;
        self.buckets.retain_mut(|bucket| {
            let count = bucket.count.wrapping_sub(previous_count);
            previous_count = bucket.count;
            let keep = count > 0 || bucket.repeat > 0;
            if keep {
                bucket.ndv = 0;
            }
            keep
        });
    }

    /// Returns a deep copy truncated to `bucket_count` buckets, Go
    /// `TruncateHistogram`.
    #[must_use]
    pub fn truncate(&self, bucket_count: usize) -> Self {
        let mut histogram = self.copy();
        histogram.buckets.truncate(bucket_count);
        histogram
    }

    /// Deep-copies this histogram, Go `Copy`.
    #[must_use]
    pub fn copy(&self) -> Self {
        self.clone()
    }

    fn merge_neighbor_buckets(&mut self, bucket_index: usize) {
        let mut merged = Vec::with_capacity(bucket_index / 2 + 1);
        let mut index = 0_usize;
        while index < bucket_index {
            let left = &self.buckets[index];
            let right = &self.buckets[index + 1];
            merged.push(Bucket {
                count: right.count,
                repeat: right.repeat,
                ndv: right.ndv.wrapping_add(left.ndv),
                lower_bound: left.lower_bound.clone(),
                upper_bound: right.upper_bound.clone(),
            });
            index += 2;
        }
        if bucket_index.is_multiple_of(2) {
            merged.push(self.buckets[bucket_index].clone());
        }
        self.buckets = merged;
    }

    fn pop_first_bucket(&mut self) {
        self.buckets.remove(0);
    }

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

/// Clones a slice's elements, Go `DeepSlice`.
#[must_use]
pub fn deep_slice<T: Clone>(slice: &[T]) -> Vec<T> {
    slice.to_vec()
}

/// Merges adjacent histogram fragments, Go `MergeHistograms`.
///
/// The source mutates both input histograms. Rust takes ownership and returns
/// the mutated left-hand value, making the same data changes observable
/// without caller aliasing.
pub fn merge_histograms(
    mut left: Histogram,
    mut right: Histogram,
    bucket_size: usize,
    stats_version: i64,
    collation: Collation,
) -> Result<Histogram, DatumValueError> {
    assert!(
        bucket_size > 0,
        "Go MergeHistograms requires a positive bucket size"
    );
    if left.is_empty() {
        return Ok(right);
    }
    if right.is_empty() {
        return Ok(left);
    }
    left.ndv = left.ndv.wrapping_add(right.ndv);
    let left_len = left.len();
    let comparison = left.buckets[left_len - 1]
        .upper_bound
        .compare(&right.buckets[0].lower_bound, collation)?;
    let mut offset = 0_i64;
    if comparison == Ordering::Equal {
        left.ndv = left.ndv.wrapping_sub(1);
        left.buckets[left_len - 1].ndv = left.buckets[left_len - 1]
            .ndv
            .wrapping_add(right.buckets[0].ndv);
        if right.buckets[0].ndv > 0 && left.buckets[left_len - 1].repeat > 0 {
            left.buckets[left_len - 1].ndv -= 1;
        }
        let count = left.buckets[left_len - 1]
            .count
            .wrapping_add(right.buckets[0].count);
        let repeat = right.buckets[0].repeat;
        let upper = right.buckets[0].upper_bound.clone();
        left.update_last_bucket(upper, count, repeat, false);
        offset = right.buckets[0].count;
        right.pop_first_bucket();
    }

    while left.len() > bucket_size {
        let last = left.len() - 1;
        left.merge_neighbor_buckets(last);
    }
    if right.is_empty() {
        return Ok(left);
    }
    while right.len() > bucket_size {
        let last = right.len() - 1;
        right.merge_neighbor_buckets(last);
    }

    let left_count = left.buckets[left.len() - 1].count;
    let right_count = right.buckets[right.len() - 1].count.wrapping_sub(offset);
    let mut left_average = left_count as f64 / left.len() as f64;
    let mut right_average = right_count as f64 / right.len() as f64;
    while left.len() > 1 && left_average * 2.0 <= right_average {
        let last = left.len() - 1;
        left.merge_neighbor_buckets(last);
        left_average *= 2.0;
    }
    while right.len() > 1 && right_average * 2.0 <= left_average {
        let last = right.len() - 1;
        right.merge_neighbor_buckets(last);
        right_average *= 2.0;
    }
    for bucket in right.buckets {
        let count = bucket.count.wrapping_add(left_count).wrapping_sub(offset);
        if stats_version >= crate::stats_version::VERSION_2 {
            left.append_bucket_with_ndv(
                bucket.lower_bound,
                bucket.upper_bound,
                count,
                bucket.repeat,
                bucket.ndv,
            );
        } else {
            left.append_bucket(bucket.lower_bound, bucket.upper_bound, count, bucket.repeat);
        }
    }
    while left.len() > bucket_size {
        let last = left.len() - 1;
        left.merge_neighbor_buckets(last);
    }
    Ok(left)
}

#[derive(Debug, Clone, PartialEq)]
struct BucketForMerging {
    lower_bound: Datum,
    upper_bound: Datum,
    count: i64,
    repeat: i64,
    ndv: i64,
    disjoint_ndv: i64,
}

impl BucketForMerging {
    fn from_histogram(histogram: &Histogram) -> Vec<Self> {
        histogram
            .buckets
            .iter()
            .enumerate()
            .map(|(index, bucket)| Self {
                lower_bound: bucket.lower_bound.clone(),
                upper_bound: bucket.upper_bound.clone(),
                count: if index == 0 {
                    bucket.count
                } else {
                    bucket
                        .count
                        .wrapping_sub(histogram.buckets[index - 1].count)
                },
                repeat: bucket.repeat,
                ndv: bucket.ndv,
                disjoint_ndv: 0,
            })
            .collect()
    }

    fn from_topn(entry: &TopNMergeEntry, options: PartitionMergeOptions) -> Self {
        debug_assert!(options.analyze_version <= crate::stats_version::VERSION_2);
        Self {
            lower_bound: entry.value.clone(),
            upper_bound: entry.value.clone(),
            count: entry.count as i64,
            repeat: entry.count as i64,
            ndv: 0,
            disjoint_ndv: 0,
        }
    }
}

fn source_max_float(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left == 0.0 && right == 0.0 {
        if left.is_sign_positive() || right.is_sign_positive() {
            0.0
        } else {
            -0.0
        }
    } else if left > right {
        left
    } else {
        right
    }
}

fn merge_bucket_ndv(
    left: &BucketForMerging,
    right: &BucketForMerging,
    collation: Collation,
) -> Result<BucketForMerging, HistogramMergeError> {
    let mut result = right.clone();
    if left.count == 0 {
        return Ok(result);
    }
    if right.count == 0 {
        result.lower_bound = left.lower_bound.clone();
        result.upper_bound = left.upper_bound.clone();
        result.ndv = left.ndv;
        return Ok(result);
    }

    let upper_order = right.upper_bound.compare(&left.upper_bound, collation)?;
    if upper_order == Ordering::Less {
        return Err(HistogramMergeError::IllegalBucketOrder);
    }
    if upper_order == Ordering::Equal {
        let lower_order = right.lower_bound.compare(&left.lower_bound, collation)?;
        if lower_order == Ordering::Less {
            return Err(HistogramMergeError::IllegalBucketOrder);
        }
        if lower_order == Ordering::Equal {
            if left.ndv > right.ndv {
                result.ndv = left.ndv;
            }
            return Ok(result);
        }
        let ratio =
            calc_fraction_from_datums(&left.lower_bound, &left.upper_bound, &right.lower_bound);
        result.ndv = (ratio * left.ndv as f64
            + source_max_float((1.0 - ratio) * left.ndv as f64, right.ndv as f64))
            as i64;
        result.lower_bound = left.lower_bound.clone();
        return Ok(result);
    }

    let right_lower_to_left_upper = right.lower_bound.compare(&left.upper_bound, collation)?;
    if right_lower_to_left_upper != Ordering::Less {
        result.upper_bound = left.upper_bound.clone();
        result.lower_bound = left.lower_bound.clone();
        result.disjoint_ndv = result.disjoint_ndv.wrapping_add(right.ndv);
        result.ndv = left.ndv;
        return Ok(result);
    }

    let upper_ratio =
        calc_fraction_from_datums(&right.lower_bound, &right.upper_bound, &left.upper_bound);
    let lower_order = right.lower_bound.compare(&left.lower_bound, collation)?;
    if lower_order != Ordering::Less {
        let lower_ratio =
            calc_fraction_from_datums(&left.lower_bound, &left.upper_bound, &right.lower_bound);
        result.ndv = (lower_ratio * left.ndv as f64
            + source_max_float(
                (1.0 - lower_ratio) * left.ndv as f64,
                upper_ratio * right.ndv as f64,
            )
            + (1.0 - upper_ratio) * right.ndv as f64) as i64;
        result.lower_bound = left.lower_bound.clone();
        return Ok(result);
    }

    let lower_ratio =
        calc_fraction_from_datums(&right.lower_bound, &right.upper_bound, &left.lower_bound);
    result.ndv = (lower_ratio * right.ndv as f64
        + source_max_float(
            left.ndv as f64,
            (upper_ratio - lower_ratio) * right.ndv as f64,
        )
        + (1.0 - upper_ratio) * right.ndv as f64) as i64;
    Ok(result)
}

fn merge_partition_buckets(
    buckets: &[BucketForMerging],
    collation: Collation,
) -> Result<BucketForMerging, HistogramMergeError> {
    let Some(last) = buckets.last() else {
        return Err(HistogramMergeError::NotEnoughBuckets);
    };
    let mut result = BucketForMerging {
        lower_bound: Datum::Null,
        upper_bound: last.upper_bound.clone(),
        count: 0,
        repeat: 0,
        ndv: 0,
        disjoint_ndv: 0,
    };
    let mut right = last.clone();
    let mut total_ndv = 0_i64;
    for (index, bucket) in buckets.iter().enumerate().rev() {
        total_ndv = total_ndv.wrapping_add(bucket.ndv);
        result.count = result.count.wrapping_add(bucket.count);
        if bucket.upper_bound.compare(&result.upper_bound, collation)? == Ordering::Equal {
            result.repeat = result.repeat.wrapping_add(bucket.repeat);
        }
        if index != buckets.len() - 1 {
            right = merge_bucket_ndv(bucket, &right, collation)?;
        }
    }
    result.ndv = right.ndv.wrapping_add(right.disjoint_ndv);
    let damped = (result.ndv as f64 * 1.15_f64.powf((buckets.len() - 1) as f64)) as i64;
    result.ndv = damped.min(total_ndv);
    Ok(result)
}

fn sort_buckets_by_upper_bound(
    buckets: &mut [BucketForMerging],
    collation: Collation,
) -> Result<(), HistogramMergeError> {
    let mut comparison_error = None;
    buckets.sort_unstable_by(|left, right| {
        let upper = match left.upper_bound.compare(&right.upper_bound, collation) {
            Ok(ordering) => ordering,
            Err(error) => {
                comparison_error = Some(error);
                Ordering::Equal
            }
        };
        if upper != Ordering::Equal {
            return upper;
        }
        match left.lower_bound.compare(&right.lower_bound, collation) {
            Ok(ordering) => ordering,
            Err(error) => {
                comparison_error = Some(error);
                Ordering::Equal
            }
        }
    });
    comparison_error.map_or(Ok(()), |error| Err(HistogramMergeError::Datum(error)))
}

fn buckets_are_sorted(
    buckets: &[BucketForMerging],
    collation: Collation,
) -> Result<bool, DatumValueError> {
    for pair in buckets.windows(2) {
        let upper = pair[0]
            .upper_bound
            .compare(&pair[1].upper_bound, collation)?;
        if upper == Ordering::Greater {
            return Ok(false);
        }
        if upper == Ordering::Equal
            && pair[0]
                .lower_bound
                .compare(&pair[1].lower_bound, collation)?
                == Ordering::Greater
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Merges partition-level histograms into one global histogram, Go
/// `MergePartitionHist2GlobalHist` at the decoded-Datum boundary.
pub fn merge_partition_histograms(
    histograms: &[Histogram],
    popped_topn: &[TopNMergeEntry],
    options: PartitionMergeOptions,
    collation: Collation,
) -> Result<Option<Histogram>, HistogramMergeError> {
    if options.expected_buckets == 0 {
        return Err(HistogramMergeError::ZeroExpectedBuckets);
    }
    let Some(first_histogram) = histograms.first() else {
        return Ok(None);
    };

    let mut total_count = 0_i64;
    let mut total_null = 0_i64;
    let mut total_column_size = 0_i64;
    let mut bucket_number = 0_usize;
    for histogram in histograms {
        total_column_size = total_column_size.wrapping_add(histogram.tot_col_size);
        total_null = total_null.wrapping_add(histogram.null_count);
        if let Some(last) = histogram.buckets.last() {
            bucket_number = bucket_number.wrapping_add(histogram.len());
            total_count = total_count.wrapping_add(last.count);
        }
    }
    if bucket_number.wrapping_add(popped_topn.len()) == 0 {
        return Ok(Some(Histogram::new(
            first_histogram.id,
            0,
            total_null,
            first_histogram.last_update_version,
            0,
            total_column_size,
        )));
    }

    let mut buckets = Vec::with_capacity(bucket_number.wrapping_add(popped_topn.len()));
    for histogram in histograms {
        buckets.extend(BucketForMerging::from_histogram(histogram));
    }
    for entry in popped_topn {
        total_count = total_count.wrapping_add(entry.count as i64);
        buckets.push(BucketForMerging::from_topn(entry, options));
    }
    buckets.retain(|bucket| bucket.count != 0);
    sort_buckets_by_upper_bound(&mut buckets, collation)?;

    let expected_buckets = options.expected_buckets as i64;
    let mut global_buckets = Vec::with_capacity(options.expected_buckets);
    let mut sum = 0_i64;
    let mut previous_sum = 0_i64;
    let mut right_end = buckets.len();
    let mut output_bucket_count = 1_i64;
    let bucket_count_threshold = total_count
        .wrapping_div(expected_buckets)
        .wrapping_mul(80)
        .wrapping_div(100);
    let mut current_leftmost: Option<Datum> = None;
    let mut index = buckets.len() as isize - 1;

    while index >= 0 {
        let current_index = index as usize;
        match current_leftmost.as_mut() {
            None => current_leftmost = Some(buckets[current_index].lower_bound.clone()),
            Some(leftmost) => {
                if leftmost.compare(&buckets[current_index].lower_bound, collation)?
                    == Ordering::Greater
                {
                    *leftmost = buckets[current_index].lower_bound.clone();
                }
            }
        }
        sum = sum.wrapping_add(buckets[current_index].count);
        let expected_sum = total_count
            .wrapping_mul(output_bucket_count)
            .wrapping_div(expected_buckets);
        if sum >= expected_sum && sum.wrapping_sub(previous_sum) >= bucket_count_threshold {
            while index > 0 {
                let previous = index as usize - 1;
                if buckets[previous]
                    .upper_bound
                    .compare(&buckets[index as usize].upper_bound, collation)?
                    != Ordering::Equal
                {
                    break;
                }
                sum = sum.wrapping_add(buckets[previous].count);
                index -= 1;
            }

            let mut leftmost = current_leftmost
                .take()
                .expect("the reverse scan always establishes a left bound");
            if leftmost.compare(&buckets[index as usize].lower_bound, collation)?
                == Ordering::Greater
            {
                leftmost = buckets[index as usize].lower_bound.clone();
            }

            let mut merge_buffer = Vec::new();
            let mut cut_buffer = Vec::new();
            let leftmost_valid_nonoverlap = index as usize;
            while index > 0 {
                let previous = index as usize - 1;
                if buckets[previous]
                    .upper_bound
                    .compare(&leftmost, collation)?
                    == Ordering::Less
                {
                    break;
                }
                if buckets[previous]
                    .lower_bound
                    .compare(&leftmost, collation)?
                    != Ordering::Less
                {
                    sum = sum.wrapping_add(buckets[previous].count);
                    merge_buffer.push(buckets[previous].clone());
                    index -= 1;
                    continue;
                }

                let overlap = 1.0
                    - calc_fraction_from_datums(
                        &buckets[previous].lower_bound,
                        &buckets[previous].upper_bound,
                        &leftmost,
                    );
                let overlapped_count = (buckets[previous].count as f64 * overlap) as i64;
                let overlapped_ndv = (buckets[previous].ndv as f64 * overlap) as i64;
                sum = sum.wrapping_add(overlapped_count);
                buckets[previous].count = buckets[previous]
                    .count
                    .wrapping_sub(overlapped_count)
                    .max(0);
                buckets[previous].ndv = buckets[previous].ndv.wrapping_sub(overlapped_ndv).max(0);
                buckets[previous].repeat = 0;

                let cut = BucketForMerging {
                    lower_bound: leftmost.clone(),
                    upper_bound: buckets[previous].upper_bound.clone(),
                    count: overlapped_count,
                    repeat: 0,
                    ndv: overlapped_ndv,
                    disjoint_ndv: 0,
                };
                buckets[previous].upper_bound = leftmost.clone();
                merge_buffer.push(cut.clone());
                cut_buffer.push(cut);
                index -= 1;
            }

            let start = index as usize;
            let mut merged = if cut_buffer.is_empty() {
                merge_partition_buckets(&buckets[start..right_end], collation)?
            } else {
                merge_buffer.reverse();
                merge_buffer.extend_from_slice(&buckets[leftmost_valid_nonoverlap..right_end]);
                debug_assert_eq!(buckets_are_sorted(&merge_buffer, collation), Ok(true));
                let merged = merge_partition_buckets(&merge_buffer, collation)?;

                sort_buckets_by_upper_bound(
                    &mut buckets[start..leftmost_valid_nonoverlap],
                    collation,
                )?;
                let mut leftmost_invalid = leftmost_valid_nonoverlap;
                while leftmost_invalid > start {
                    if buckets[leftmost_invalid - 1]
                        .lower_bound
                        .compare(&leftmost, collation)?
                        == Ordering::Less
                    {
                        break;
                    }
                    leftmost_invalid -= 1;
                }
                debug_assert_eq!(
                    buckets_are_sorted(&buckets[start..leftmost_invalid], collation),
                    Ok(true)
                );
                index = leftmost_invalid as isize;
                merged
            };
            merged.lower_bound = leftmost;
            global_buckets.push(merged);
            right_end = index as usize;
            output_bucket_count = output_bucket_count.wrapping_add(1);
            previous_sum = sum;
        }
        index -= 1;
    }

    if right_end > 0 {
        let mut leftmost = buckets[0].lower_bound.clone();
        for bucket in &buckets[1..right_end] {
            if leftmost.compare(&bucket.lower_bound, collation)? == Ordering::Greater {
                leftmost = bucket.lower_bound.clone();
            }
        }
        let mut merged = merge_partition_buckets(&buckets[..right_end], collation)?;
        merged.lower_bound = leftmost;
        global_buckets.push(merged);
    }
    global_buckets.reverse();
    for index in 1..global_buckets.len() {
        global_buckets[index].count = global_buckets[index]
            .count
            .wrapping_add(global_buckets[index - 1].count);
    }

    for bucket in &mut global_buckets {
        let mut repeat = 0.0;
        for histogram in histograms {
            repeat += histogram
                .equal_row_count(&bucket.upper_bound, options.is_index, collation)
                .0;
        }
        if (repeat as i64) > bucket.repeat {
            bucket.repeat = repeat as i64;
        }
    }

    let mut global = Histogram::new(
        first_histogram.id,
        0,
        total_null,
        first_histogram.last_update_version,
        global_buckets.len(),
        total_column_size,
    );
    for bucket in global_buckets {
        global.append_bucket_with_ndv(
            bucket.lower_bound,
            bucket.upper_bound,
            bucket.count,
            bucket.repeat,
            if options.is_index { bucket.ndv } else { 0 },
        );
    }
    Ok(Some(global))
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

    fn merge_bucket(lower: i64, upper: i64, ndv: i64, disjoint_ndv: i64) -> BucketForMerging {
        BucketForMerging {
            lower_bound: Datum::new_int(lower),
            upper_bound: Datum::new_int(upper),
            count: ndv,
            repeat: 0,
            ndv,
            disjoint_ndv,
        }
    }

    #[test]
    fn lockdown_private_histogram_symbols_compile() {
        let _ = std::mem::size_of::<BucketForMerging>();
        let _ = <BucketForMerging as Clone>::clone;
        let _ = BucketForMerging::from_histogram;
        let _ = BucketForMerging::from_topn;
        let _ = Histogram::merge_neighbor_buckets;
        let _ = Histogram::pop_first_bucket;
        let _ = Histogram::update_last_bucket;
        let _ = buckets_are_sorted;
        let _ = merge_bucket_ndv;
        let _ = merge_partition_buckets;
        let _ = sort_buckets_by_upper_bound;
        let _ = merge_bucket;
        let _ = source_merge_bucket_ndv_matches_all_go_cases;
    }

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

    #[test]
    fn source_merge_bucket_ndv_matches_all_go_cases() {
        let cases = [
            (
                merge_bucket(1, 2, 2, 0),
                merge_bucket(1, 2, 3, 0),
                merge_bucket(1, 2, 3, 0),
            ),
            (
                merge_bucket(1, 3, 2, 0),
                merge_bucket(2, 3, 2, 0),
                merge_bucket(1, 3, 3, 0),
            ),
            (
                merge_bucket(1, 3, 2, 0),
                merge_bucket(4, 6, 2, 2),
                merge_bucket(1, 3, 2, 4),
            ),
            (
                merge_bucket(1, 5, 5, 0),
                merge_bucket(2, 6, 5, 0),
                merge_bucket(1, 6, 6, 0),
            ),
            (
                merge_bucket(3, 5, 3, 0),
                merge_bucket(2, 6, 4, 0),
                merge_bucket(2, 6, 5, 0),
            ),
        ];
        for (left, right, expected) in cases {
            let actual = merge_bucket_ndv(&left, &right, Collation::Binary).unwrap();
            assert_eq!(actual.lower_bound, expected.lower_bound);
            assert_eq!(actual.upper_bound, expected.upper_bound);
            assert_eq!(actual.ndv, expected.ndv);
            assert_eq!(actual.disjoint_ndv, expected.disjoint_ndv);
        }
    }

    #[test]
    fn source_merge_bucket_ndv_empty_and_illegal_order_oracle() {
        let mut left_empty = merge_bucket(1, 2, 2, 0);
        left_empty.count = 0;
        let right = merge_bucket(1, 2, 3, 0);
        assert_eq!(
            merge_bucket_ndv(&left_empty, &right, Collation::Binary).unwrap(),
            right
        );

        let left = merge_bucket(1, 2, 2, 0);
        let mut right_empty = merge_bucket(1, 2, 3, 0);
        right_empty.count = 0;
        let merged = merge_bucket_ndv(&left, &right_empty, Collation::Binary).unwrap();
        assert_eq!(merged.lower_bound, Datum::new_int(1));
        assert_eq!(merged.upper_bound, Datum::new_int(2));
        assert_eq!(merged.count, 0);
        assert_eq!(merged.ndv, 2);

        assert_eq!(
            merge_bucket_ndv(
                &merge_bucket(1, 5, 2, 0),
                &merge_bucket(1, 4, 2, 0),
                Collation::Binary,
            ),
            Err(HistogramMergeError::IllegalBucketOrder)
        );
        assert_eq!(
            merge_bucket_ndv(
                &merge_bucket(2, 5, 2, 0),
                &merge_bucket(1, 5, 2, 0),
                Collation::Binary,
            ),
            Err(HistogramMergeError::IllegalBucketOrder)
        );
        assert_eq!(
            merge_partition_buckets(&[], Collation::Binary),
            Err(HistogramMergeError::NotEnoughBuckets)
        );
    }
}
