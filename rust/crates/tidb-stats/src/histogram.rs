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
//! functionally equivalent for every estimation and merge method ported here.
//! What the chunk allocated, which Go's `MemoryUsage` reports, is recorded
//! beside the buckets in [`HistogramAllocation`].

use std::{
    borrow::Cow,
    cmp::Ordering,
    fmt,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use tidb_chunk::column::VAR_ELEM_LEN;
use tidb_charset::Collation;
use tidb_datatype::{
    datums_to_bytes, CollationCollator, Datum, DatumStringError, DatumValueError, FieldType,
    FieldTypeCode, SessionTimeZone,
};

pub use crate::histogram_allocation::HistogramAllocation;
pub use crate::scalar_geometry::{
    calc_fraction, calc_fraction_from_datums, convert_bytes_to_scalar, convert_datum_to_scalar,
};
use crate::{
    cmsketch::TopNEntry,
    histogram_allocation::{
        bound_type_size, histogram_bound_type_size, pre_calculates_scalars, var_len_cell_len,
    },
    row_estimate::{default_row_est, RowEstimate},
    scalar_geometry::{
        bytes_to_scalar, common_prefix_length as common_prefix_length_all, datum_common_prefix_length,
    },
};

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

struct HistogramScalar {
    lower: f64,
    upper: f64,
    common_prefix: usize,
}

impl HistogramScalar {
    fn fraction_for_bytes(&self, value: &[u8]) -> f64 {
        calc_fraction(
            self.lower,
            self.upper,
            bytes_to_scalar(value, self.common_prefix),
        )
    }
}

#[derive(Clone)]
enum HistogramScalars {
    Bytes(Arc<[HistogramScalar]>),
    DecimalOrTime(Arc<[HistogramScalar]>),
}

/// Histogram buckets and their prepared bound scalars. Clones share the
/// entries until a write. Mutable access detaches the entries and clears the
/// scalars so direct changes to the buckets or their bounds remain visible.
#[derive(Clone, Default)]
pub struct HistogramBuckets {
    entries: Option<Arc<Vec<Bucket>>>,
    scalars: Option<HistogramScalars>,
    prefix: Option<BucketPrefix>,
}

#[derive(Clone, Copy)]
struct BucketPrefix {
    len: std::num::NonZeroUsize,
    capacity: usize,
}

impl HistogramBuckets {
    /// Capacity available to this histogram's bucket vector.
    pub fn capacity(&self) -> usize {
        self.prefix.map_or_else(
            || self.entries.as_ref().map_or(0, |entries| entries.capacity()),
            |prefix| prefix.capacity,
        )
    }

    /// Appends a bucket and invalidates the prepared scalars.
    pub fn push(&mut self, bucket: Bucket) {
        self.make_mut().push(bucket);
    }

    /// Removes all buckets while retaining their capacity.
    pub fn clear(&mut self) {
        self.make_mut().clear();
    }

    /// Removes a bucket and shifts subsequent buckets down.
    pub fn remove(&mut self, index: usize) -> Bucket {
        self.make_mut().remove(index)
    }

    /// Retains buckets that pass the predicate, in order.
    pub fn retain_mut(&mut self, keep: impl FnMut(&mut Bucket) -> bool) {
        self.make_mut().retain_mut(keep);
    }

    fn materialize_prefix(&mut self) {
        if let Some(prefix) = self.prefix.take() {
            let entries = self.entries.as_mut().expect("prefix has backing buckets");
            if let Some(entries) = Arc::get_mut(entries) {
                entries.truncate(prefix.len.get());
                entries.shrink_to(prefix.capacity);
            } else {
                let mut retained = Vec::with_capacity(prefix.capacity);
                retained.extend_from_slice(&entries[..prefix.len.get()]);
                *entries = Arc::new(retained);
            }
        }
    }

    #[inline]
    fn make_mut(&mut self) -> &mut Vec<Bucket> {
        self.scalars = None;
        self.materialize_prefix();
        Arc::make_mut(self.entries.get_or_insert_with(Arc::default))
    }
}

impl From<Vec<Bucket>> for HistogramBuckets {
    fn from(entries: Vec<Bucket>) -> Self {
        Self {
            entries: (entries.capacity() != 0).then(|| Arc::new(entries)),
            scalars: None,
            prefix: None,
        }
    }
}

impl From<HistogramBuckets> for Vec<Bucket> {
    fn from(mut buckets: HistogramBuckets) -> Self {
        buckets.materialize_prefix();
        buckets.entries.map(Arc::unwrap_or_clone).unwrap_or_default()
    }
}

impl FromIterator<Bucket> for HistogramBuckets {
    fn from_iter<T: IntoIterator<Item = Bucket>>(iter: T) -> Self {
        Vec::from_iter(iter).into()
    }
}

impl Deref for HistogramBuckets {
    type Target = [Bucket];

    #[inline]
    fn deref(&self) -> &Self::Target {
        let Some(entries) = self.entries.as_deref() else {
            return &[];
        };
        match self.prefix {
            Some(prefix) => &entries[..prefix.len.get()],
            None => entries,
        }
    }
}

impl DerefMut for HistogramBuckets {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.make_mut()
    }
}

impl<'a> IntoIterator for &'a HistogramBuckets {
    type Item = &'a Bucket;
    type IntoIter = std::slice::Iter<'a, Bucket>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for &'a mut HistogramBuckets {
    type Item = &'a mut Bucket;
    type IntoIter = std::slice::IterMut<'a, Bucket>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl IntoIterator for HistogramBuckets {
    type Item = Bucket;
    type IntoIter = std::vec::IntoIter<Bucket>;

    fn into_iter(self) -> Self::IntoIter {
        Vec::from(self).into_iter()
    }
}

impl PartialEq for HistogramBuckets {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl fmt::Debug for HistogramBuckets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
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
    pub buckets: HistogramBuckets,
    /// Go's `Bounds`/`Buckets`/`Scalars` allocation, which
    /// [`Histogram::memory_usage`] reports.
    pub allocation: HistogramAllocation,
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

/// Two-bound compatibility view of `pkg/statistics/scalar.go`'s variadic
/// `commonPrefixLength` helper.
pub fn common_prefix_length(a: &[u8], b: &[u8]) -> usize {
    common_prefix_length_all(&[a, b])
}

/// Go `ValueToString`: a possible encoded index value as Go's diagnostic
/// string, whose bytes (a collation sort key, say) are kept as they are.
///
/// When decoding stops at malformed input, Go deliberately ignores that
/// error, preserves the decoded prefix, and renders the undecoded suffix as
/// one byte datum. `idx_cols == 0` is the ordinary `Datum.ToString` path.
pub fn value_to_bytes(
    value: &Datum,
    idx_cols: usize,
    idx_column_types: Option<&[FieldTypeCode]>,
    timezone: Option<&SessionTimeZone>,
) -> Result<Vec<u8>, DatumStringError> {
    if idx_cols == 0 {
        return value.sql_bytes();
    }
    let (mut decoded_values, remainder) =
        match tidb_codec::decode_range(value.go_bytes(), idx_cols, idx_column_types, timezone) {
            Ok(decoded) => decoded,
            Err(error) => (error.values, error.remainder),
        };
    if !remainder.is_empty() {
        decoded_values.push(Datum::new_bytes_datum(remainder));
    }
    datums_to_bytes(&decoded_values, true, false)
}

fn datum_bytes(value: &Datum) -> &[u8] {
    match value {
        Datum::String(value) => value.bytes(),
        Datum::Bytes(value) => value,
        _ => &[],
    }
}

#[inline]
fn compare_histogram_bound(
    bound: &Datum,
    value: &Datum,
    collation: Collation,
) -> Result<Ordering, DatumValueError> {
    Ok(match (bound, value) {
        (Datum::Int(bound), Datum::Int(value)) => bound.cmp(value),
        (Datum::UInt(bound), Datum::UInt(value)) => bound.cmp(value),
        (Datum::Decimal(bound), Datum::Decimal(value)) => bound.cmp(value),
        (Datum::Time(bound), Datum::Time(value)) => bound.compare(*value),
        (Datum::Bytes(bound), Datum::Bytes(value)) => match collation {
            Collation::Binary | Collation::Utf8Mb40900Bin => {
                // A big-endian word preserves byte order. Only compare the
                // remaining bytes when the first word matches.
                if let (Some((left, left_tail)), Some((right, right_tail))) =
                    (bound.split_first_chunk::<8>(), value.split_first_chunk::<8>())
                {
                    u64::from_be_bytes(*left)
                        .cmp(&u64::from_be_bytes(*right))
                        .then_with(|| left_tail.cmp(right_tail))
                } else {
                    bound.cmp(value)
                }
            }
            _ => collation.compare(bound, value),
        },
        (Datum::String(bound), Datum::String(value)) => {
            collation.compare(bound.bytes(), value.bytes())
        }
        _ => return bound.compare(value, collation),
    })
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
    /// Go `(*Histogram).MemoryUsage`: `EmptyHistogramSize +
    /// Bounds.MemoryUsage() + cap(Buckets)*EmptyBucketSize +
    /// cap(Scalars)*EmptyScalarSize`, or zero when there are no buckets, no
    /// scalars, and a zero-capacity bounds chunk.
    ///
    /// A histogram whose allocation no Go-shaped constructor recorded (a
    /// struct literal, or buckets edited around these methods) is accounted as
    /// Go's storage loader builds it; see [`HistogramAllocation`].
    pub fn memory_usage(&self) -> i64 {
        self.go_allocation().memory_usage()
    }

    /// The allocation Go's `MemoryUsage` reads: the recorded one, or, for a
    /// histogram no Go-shaped constructor built, the one Go's storage loader
    /// makes for these buckets.
    fn go_allocation(&self) -> Cow<'_, HistogramAllocation> {
        if self.allocation.is_tracking(self.buckets.len()) {
            return Cow::Borrowed(&self.allocation);
        }
        let mut loaded = HistogramAllocation::new(self.bound_type_size(), self.buckets.len());
        for bucket in &self.buckets {
            loaded.append_bucket(&bucket.lower_bound, &bucket.upper_bound);
        }
        if let Some(first) = self.buckets.first() {
            if pre_calculates_scalars(&first.lower_bound) {
                loaded.set_scalar_len(self.buckets.len());
            }
        }
        Cow::Owned(loaded)
    }

    /// Creates an empty histogram, Go `NewHistogram`: `tp` decides the
    /// bounds column's layout, and `bucket_size` the capacity of both the
    /// bounds chunk (`2*bucket_size` rows) and `Buckets`.
    pub fn new(
        id: i64,
        ndv: i64,
        null_count: i64,
        last_update_version: u64,
        tp: &FieldType,
        bucket_size: usize,
        tot_col_size: i64,
    ) -> Self {
        Self::with_bound_type_size(
            id,
            ndv,
            null_count,
            last_update_version,
            histogram_bound_type_size(tp),
            bucket_size,
            tot_col_size,
        )
    }

    /// [`Histogram::new`] for a caller that knows the bounds column's Go
    /// `getFixedLen` rather than its field type.
    pub(crate) fn with_bound_type_size(
        id: i64,
        ndv: i64,
        null_count: i64,
        last_update_version: u64,
        bound_type_size: i64,
        bucket_size: usize,
        tot_col_size: i64,
    ) -> Self {
        Self {
            id,
            ndv,
            null_count,
            last_update_version,
            tot_col_size,
            correlation: 0.0,
            buckets: Vec::with_capacity(bucket_size).into(),
            allocation: HistogramAllocation::new(bound_type_size, bucket_size),
        }
    }

    /// Go `getFixedLen(hg.Tp)`: as the constructor recorded it, else the
    /// column type the first non-NULL bound is stored under (an index's
    /// `TypeBlob` when there is none).
    pub(crate) fn bound_type_size(&self) -> i64 {
        self.allocation.type_size().unwrap_or_else(|| {
            self.bounds()
                .find_map(bound_type_size)
                .unwrap_or(VAR_ELEM_LEN)
        })
    }

    /// Go `histogramFromStorageWithParams`'s construction around the metadata
    /// this histogram already holds: `NewHistogram(.., tp, len(rows), ..)`,
    /// one `AppendBucketWithNDV` per stored bucket (whose counts are already
    /// cumulative), then `PreCalculateScalar`. An index histogram's `tp` is
    /// `TypeBlob`.
    pub fn load_buckets_from_storage(&mut self, tp: &FieldType, mut buckets: Vec<Bucket>) {
        let mut allocation = HistogramAllocation::new(histogram_bound_type_size(tp), buckets.len());
        for bucket in &buckets {
            allocation.append_bucket(&bucket.lower_bound, &bucket.upper_bound);
        }
        buckets.shrink_to_fit();
        let mut loaded = Self {
            buckets: buckets.into(),
            allocation,
            ..*self
        };
        loaded.pre_calculate_scalar();
        *self = loaded;
    }

    /// The histogram Go's statistics consumers read after a fresh build is
    /// saved: Go never hands a built histogram to the stats cache, it reloads
    /// it from `mysql.stats_buckets`, so a tier that installs built statistics
    /// directly performs that load's construction itself
    /// ([`Histogram::load_buckets_from_storage`] over these buckets).
    pub fn round_trip_through_storage(&mut self, tp: &FieldType) {
        let buckets = std::mem::take(&mut self.buckets);
        self.load_buckets_from_storage(tp, buckets.into());
    }

    /// Go `ConvertTo`'s allocation, for bounds the caller already converted
    /// in place: `NewHistogram(.., tp, Len(), ..)` receives every converted
    /// bound, and the new histogram shares `Buckets` and drops `Scalars`.
    pub fn record_convert_to(&mut self, tp: &FieldType) {
        let mut allocation =
            HistogramAllocation::new(histogram_bound_type_size(tp), self.buckets.len());
        for bucket in &self.buckets {
            allocation.append_bucket(&bucket.lower_bound, &bucket.upper_bound);
        }
        self.allocation = allocation;
    }

    /// Go `PreCalculateScalar`. Prepare bound scalars for repeated estimates.
    pub fn pre_calculate_scalar(&mut self) {
        let Some(first) = self.buckets.first() else {
            return;
        };
        if !pre_calculates_scalars(&first.lower_bound) {
            return;
        }
        let is_bytes = matches!(first.lower_bound, Datum::Bytes(_) | Datum::String(_));
        let scalars = self
            .buckets
            .iter()
            .map(|bucket| {
                let common_prefix = if is_bytes {
                    datum_common_prefix_length(&bucket.lower_bound, &bucket.upper_bound)
                } else {
                    0
                };
                HistogramScalar {
                    lower: convert_datum_to_scalar(&bucket.lower_bound, common_prefix),
                    upper: convert_datum_to_scalar(&bucket.upper_bound, common_prefix),
                    common_prefix,
                }
            })
            .collect();
        self.buckets.scalars = Some(if is_bytes {
            HistogramScalars::Bytes(scalars)
        } else {
            HistogramScalars::DecimalOrTime(scalars)
        });
        self.allocation.set_scalar_len(self.buckets.len());
    }

    /// Go `DropUnnecessaryData`'s histogram reset: `Bounds =
    /// NewChunkWithCapacity(TypeBlob, 0)` and empty `Buckets` and `Scalars`.
    pub fn drop_bucket_data(&mut self) {
        self.buckets = HistogramBuckets::default();
        self.allocation = HistogramAllocation::new(VAR_ELEM_LEN, 0);
    }

    /// Go `Index.EvictAllStats`'s `Buckets = nil`; `Bounds` and `Scalars`
    /// stay allocated.
    pub fn drop_buckets(&mut self) {
        self.buckets = HistogramBuckets::default();
        self.allocation.drop_buckets();
    }

    /// Returns the lower bound of `bucket_index`, Go `GetLower`.
    pub fn get_lower(&self, bucket_index: usize) -> &Datum {
        &self.buckets[bucket_index].lower_bound
    }

    /// Copies the lower bound of `bucket_index`, Go `LowerToDatum`.
    pub fn lower_to_datum(&self, bucket_index: usize) -> Datum {
        self.get_lower(bucket_index).clone()
    }

    /// Returns the upper bound of `bucket_index`, Go `GetUpper`.
    pub fn get_upper(&self, bucket_index: usize) -> &Datum {
        &self.buckets[bucket_index].upper_bound
    }

    /// Copies the upper bound of `bucket_index`, Go `UpperToDatum`.
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
        if self.allocation.is_tracking(self.buckets.len()) {
            self.allocation.append_bucket(&lower_bound, &upper_bound);
        }
        self.buckets.push(Bucket {
            count,
            repeat,
            ndv,
            lower_bound,
            upper_bound,
        });
    }

    /// Go `updateLastBucket`.
    pub(crate) fn update_last_bucket(
        &mut self,
        upper_bound: Cow<'_, Datum>,
        count: i64,
        repeat: i64,
        need_bucket_ndv: bool,
    ) {
        let last = self
            .buckets
            .last()
            .expect("Go updateLastBucket requires a non-empty histogram");
        let retained = self
            .allocation
            .last_upper_offset(self.buckets.len(), &last.upper_bound);
        self.update_last_bucket_with_offset(upper_bound, count, repeat, need_bucket_ndv, retained);
    }

    fn update_last_bucket_with_offset(
        &mut self,
        upper_bound: Cow<'_, Datum>,
        count: i64,
        repeat: i64,
        need_bucket_ndv: bool,
        retained_data_len: Option<usize>,
    ) {
        if self.allocation.is_tracking(self.buckets.len()) {
            let retained =
                retained_data_len.unwrap_or_else(|| self.bound_data_len(2 * self.buckets.len() - 1));
            self.allocation.replace_last_upper(retained, &upper_bound);
        }
        let bucket = self
            .buckets
            .last_mut()
            .expect("Go updateLastBucket requires a non-empty histogram");
        match (&mut bucket.upper_bound, upper_bound) {
            (Datum::Bytes(current), Cow::Borrowed(Datum::Bytes(next)))
                if current.capacity() == next.len() =>
            {
                current.clone_from(next);
            }
            (current, next) => *current = next.into_owned(),
        }
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
        let compare = |bound: &Datum| compare_histogram_bound(bound, value, collation);
        if self.len() > 4 {
            if compare(&self.buckets[high].upper_bound)? == Ordering::Less {
                return Ok(());
            }
            if compare(&self.buckets[low].lower_bound)? == Ordering::Greater {
                return Ok(());
            }
        }

        while low <= high {
            let mid = low + (high - low) / 2;
            if compare(&self.buckets[mid].lower_bound)? == Ordering::Greater {
                let Some(next_high) = mid.checked_sub(1) else {
                    break;
                };
                high = next_high;
                continue;
            }
            let upper_order = compare(&self.buckets[mid].upper_bound)?;
            if upper_order == Ordering::Less {
                low = mid + 1;
                continue;
            }

            let buckets = &mut self.buckets[mid..];
            let bucket = &mut buckets[0];
            if bucket.ndv > 0 {
                bucket.ndv -= 1;
            }
            if upper_order == Ordering::Equal {
                bucket.repeat = 0;
            }
            for bucket in buckets {
                bucket.count = bucket.count.wrapping_sub(count).max(0);
            }
            break;
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
        self.remove_sorted_values(
            values,
            |bound, entry| bound.compare(&entry.value, collation),
            |entry| entry.count,
        )
    }

    /// Removes the already sorted, encoded index TopN keys from the buckets.
    pub(crate) fn remove_index_topn(
        &mut self,
        values: &[TopNEntry],
    ) -> Result<(), DatumValueError> {
        self.remove_sorted_values(
            values,
            |bound, entry| match bound {
                Datum::Bytes(bytes) => Ok(bytes.cmp(&entry.encoded)),
                _ => bound.compare(&Datum::Bytes(entry.encoded.clone()), Collation::Binary),
            },
            |entry| entry.count,
        )
    }

    fn remove_sorted_values<T>(
        &mut self,
        values: &[T],
        compare: impl Fn(&Datum, &T) -> Result<Ordering, DatumValueError>,
        count: impl Fn(&T) -> u64,
    ) -> Result<(), DatumValueError> {
        let mut total_sub_count = 0_i64;
        let mut value_index = 0_usize;
        for bucket in &mut self.buckets {
            while let Some(value) = values.get(value_index) {
                if compare(&bucket.lower_bound, value)? == Ordering::Greater {
                    value_index += 1;
                    continue;
                }
                let upper_order = compare(&bucket.upper_bound, value)?;
                if upper_order == Ordering::Less {
                    break;
                }
                total_sub_count = total_sub_count.wrapping_add(count(value) as i64);
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
    #[inline]
    pub fn standardize_for_v2_analyze_index(&mut self) {
        if self.buckets.is_empty() {
            return;
        }
        let tracking = self.allocation.is_tracking(self.buckets.len());
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
        if tracking {
            let kept = self.buckets.len();
            let bounds = self.buckets.iter().flat_map(|bucket| [&bucket.lower_bound, &bucket.upper_bound]);
            self.allocation.rebuild_bounds(kept, bounds, kept);
        }
    }

    /// Returns an independently editable copy truncated to `bucket_count` buckets, Go
    /// `TruncateHistogram`.
    pub fn truncate(&self, bucket_count: usize) -> Self {
        let mut histogram = self.copy();
        let original_len = self.buckets.len();
        let bucket_count = bucket_count.min(original_len);
        let tracking = histogram.allocation.is_tracking(original_len);
        histogram.buckets.scalars = None;
        if bucket_count != original_len {
            let backing = self.buckets.entries.as_ref().expect("histogram has buckets");
            if bucket_count >= backing.len().div_ceil(2) {
                histogram.buckets.prefix = Some(BucketPrefix {
                    len: std::num::NonZeroUsize::new(bucket_count).expect("nonempty prefix"),
                    capacity: original_len,
                });
            } else {
                // Avoid retaining the original allocation for a short prefix.
                let mut buckets = Vec::with_capacity(original_len);
                buckets.extend_from_slice(&self.buckets[..bucket_count]);
                histogram.buckets = buckets.into();
            }
        }
        if tracking {
            let retained = if histogram.bound_type_size() == VAR_ELEM_LEN {
                histogram.bound_data_len(2 * bucket_count)
            } else {
                0
            };
            histogram
                .allocation
                .truncate_buckets(bucket_count, retained);
        }
        histogram
    }

    /// Copies this histogram with independent mutations, Go `Copy`.
    pub fn copy(&self) -> Self {
        Self {
            buckets: self.buckets.clone(),
            allocation: self.go_allocation().copy(),
            ..*self
        }
    }

    /// Each bound in Go's `Bounds` row order.
    fn bounds(&self) -> impl Iterator<Item = &Datum> {
        self.buckets
            .iter()
            .flat_map(|bucket| [&bucket.lower_bound, &bucket.upper_bound])
    }

    /// The bytes the first `rows` bounds occupy in a variable-length bounds
    /// column (Go `offsets[rows]`).
    fn bound_data_len(&self, rows: usize) -> usize {
        self.bounds().take(rows).map(var_len_cell_len).sum()
    }

    /// Go `mergeBuckets`.
    pub(crate) fn merge_buckets(&mut self, bucket_index: usize) {
        let tracking = self.allocation.is_tracking(self.buckets.len());
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
        self.buckets = merged.into();
        if tracking {
            let merged_len = self.buckets.len();
            let mut allocation = std::mem::take(&mut self.allocation);
            allocation.rebuild_bounds(bucket_index, self.bounds(), merged_len);
            self.allocation = allocation;
        }
    }

    /// Go `popFirstBucket`.
    pub(crate) fn pop_first_bucket(&mut self) {
        if self.allocation.is_tracking(self.buckets.len()) {
            let mut allocation = std::mem::take(&mut self.allocation);
            allocation.pop_first_bucket(self.bounds().skip(2));
            self.allocation = allocation;
        }
        self.buckets.remove(0);
    }

    /// Number of buckets, Go `Histogram.Len`.
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// True when there are no buckets.
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Row count contributed by bucket `idx` alone, Go `BucketCount`.
    pub fn bucket_count(&self, idx: usize) -> i64 {
        if idx == 0 {
            self.buckets[0].count
        } else {
            self.buckets[idx].count - self.buckets[idx - 1].count
        }
    }

    /// Count of non-NULL values, Go `NotNullCount`.
    pub fn not_null_count(&self) -> f64 {
        match self.buckets.last() {
            Some(bucket) => bucket.count as f64,
            None => 0.0,
        }
    }

    /// Total row count including NULLs, Go `TotalRowCount`.
    pub fn total_row_count(&self) -> f64 {
        self.not_null_count() + self.null_count as f64
    }

    /// Locates where `value` falls relative to the buckets, Go `LocateBucket`.
    ///
    /// Bucket bounds must be in nondecreasing order under `collation`.
    /// Comparisons use `collation` for string/bytes bounds; pass
    /// [`Collation::Binary`] for non-string types or binary collations.
    pub fn locate_bucket(&self, value: &Datum, collation: Collation) -> BucketLocation {
        let buckets: &[Bucket] = &self.buckets;
        if buckets.is_empty() {
            return BucketLocation {
                exceed: true,
                bucket_idx: 0,
                in_bucket: false,
                match_last_value: false,
            };
        }
        let compare = |bound: &Datum| {
            compare_histogram_bound(bound, value, collation).unwrap_or(Ordering::Less)
        };
        // Like Go's chunk.LowerBound, check the last upper bound first. The
        // first qualifying upper bound identifies the bucket, and its lower
        // bound tells us whether the value falls in the gap before it.
        let mut hi = buckets.len() - 1;
        let mut upper_order = compare(&buckets[hi].upper_bound);
        if upper_order == Ordering::Less {
            return BucketLocation {
                exceed: true,
                bucket_idx: hi,
                in_bucket: false,
                match_last_value: false,
            };
        }
        let mut lo = 0;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let ordering = compare(&buckets[mid].upper_bound);
            if ordering == Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
                upper_order = ordering;
            }
        }
        let match_last_value = upper_order == Ordering::Equal;
        BucketLocation {
            exceed: false,
            bucket_idx: lo,
            in_bucket: match_last_value || compare(&buckets[lo].lower_bound) != Ordering::Greater,
            match_last_value,
        }
    }

    /// Fraction of bucket `index`'s interval covered by `[lower, value]`,
    /// Go `Histogram.calcFraction`.
    pub fn calc_fraction(&self, index: usize, value: &Datum) -> f64 {
        match (&self.buckets.scalars, value) {
            (Some(HistogramScalars::Bytes(scalars)), Datum::Bytes(bytes)) => {
                return scalars[index].fraction_for_bytes(bytes);
            }
            (Some(HistogramScalars::Bytes(scalars)), Datum::String(string)) => {
                return scalars[index].fraction_for_bytes(string.bytes());
            }
            (Some(HistogramScalars::DecimalOrTime(scalars)), Datum::Decimal(_) | Datum::Time(_)) => {
                let scalar = &scalars[index];
                return calc_fraction(scalar.lower, scalar.upper, convert_datum_to_scalar(value, 0));
            }
            _ => {}
        }
        let bucket = &self.buckets[index];
        calc_fraction_from_datums(&bucket.lower_bound, &bucket.upper_bound, value)
    }

    /// Estimates the row count where the column equals `value`, Go
    /// `EqualRowCount`. `matched` is true when the estimate comes from a
    /// bucket's repeat count or bucket-level NDV (more accurate).
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
    pub fn less_row_count_with_bkt_idx(&self, value: &Datum, collation: Collation) -> (f64, usize) {
        let buckets: &[Bucket] = &self.buckets;
        if buckets.is_empty() {
            return (0.0, 0);
        }
        let location = self.locate_bucket(value, collation);
        if location.exceed {
            return (self.not_null_count(), self.len() - 1);
        }
        let pre_count = if location.bucket_idx > 0 {
            buckets[location.bucket_idx - 1].count as f64
        } else {
            0.0
        };
        if !location.in_bucket {
            return (pre_count, location.bucket_idx);
        }
        let bucket = &buckets[location.bucket_idx];
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
    pub fn less_row_count(&self, value: &Datum, collation: Collation) -> f64 {
        self.less_row_count_with_bkt_idx(value, collation).0
    }

    /// Estimates the row count where the column is greater than `value`, Go
    /// `GreaterRowCount`. Deprecated upstream; kept for test parity.
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
    pub fn abs_row_count_difference(&self, realtime_row_count: i64) -> f64 {
        (realtime_row_count as f64 - self.total_row_count()).abs()
    }

    /// Go `Histogram.GetIncreaseFactor`.
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
        // The caller may have edited the supplied bounds directly.
        left.update_last_bucket_with_offset(Cow::Owned(upper), count, repeat, false, None);
        offset = right.buckets[0].count;
        right.pop_first_bucket();
    }

    while left.len() > bucket_size {
        let last = left.len() - 1;
        left.merge_buckets(last);
    }
    if right.is_empty() {
        return Ok(left);
    }
    while right.len() > bucket_size {
        let last = right.len() - 1;
        right.merge_buckets(last);
    }

    let left_count = left.buckets[left.len() - 1].count;
    let right_count = right.buckets[right.len() - 1].count.wrapping_sub(offset);
    let mut left_average = left_count as f64 / left.len() as f64;
    let mut right_average = right_count as f64 / right.len() as f64;
    while left.len() > 1 && left_average * 2.0 <= right_average {
        let last = left.len() - 1;
        left.merge_buckets(last);
        left_average *= 2.0;
    }
    while right.len() > 1 && right_average * 2.0 <= left_average {
        let last = right.len() - 1;
        right.merge_buckets(last);
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
        left.merge_buckets(last);
    }
    Ok(left)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct BucketForMerging<'a> {
    // Working buckets reuse input bounds, including the endpoints of cuts.
    lower_bound: &'a Datum,
    upper_bound: &'a Datum,
    count: i64,
    repeat: i64,
    ndv: i64,
    disjoint_ndv: i64,
}

impl<'a> BucketForMerging<'a> {
    fn from_histogram(histogram: &'a Histogram) -> impl Iterator<Item = Self> {
        let mut previous_count = 0;
        histogram.buckets.iter().map(move |bucket| {
            let count = bucket.count.wrapping_sub(previous_count);
            previous_count = bucket.count;
            Self {
                lower_bound: &bucket.lower_bound,
                upper_bound: &bucket.upper_bound,
                count,
                repeat: bucket.repeat,
                ndv: bucket.ndv,
                disjoint_ndv: 0,
            }
        })
    }

    fn from_topn(entry: &'a TopNMergeEntry, options: PartitionMergeOptions) -> Self {
        debug_assert!(options.analyze_version <= crate::stats_version::VERSION_2);
        Self {
            lower_bound: &entry.value,
            upper_bound: &entry.value,
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

fn merge_bucket_ndv<'a>(
    left: &BucketForMerging<'a>,
    right: &BucketForMerging<'a>,
    collation: Collation,
) -> Result<BucketForMerging<'a>, HistogramMergeError> {
    let mut result = *right;
    if left.count == 0 {
        return Ok(result);
    }
    if right.count == 0 {
        result.lower_bound = left.lower_bound;
        result.upper_bound = left.upper_bound;
        result.ndv = left.ndv;
        return Ok(result);
    }

    let upper_order = compare_histogram_bound(right.upper_bound, left.upper_bound, collation)?;
    if upper_order == Ordering::Less {
        return Err(HistogramMergeError::IllegalBucketOrder);
    }
    if upper_order == Ordering::Equal {
        let lower_order = compare_histogram_bound(right.lower_bound, left.lower_bound, collation)?;
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
            calc_fraction_from_datums(left.lower_bound, left.upper_bound, right.lower_bound);
        result.ndv = (ratio * left.ndv as f64
            + source_max_float((1.0 - ratio) * left.ndv as f64, right.ndv as f64))
            as i64;
        result.lower_bound = left.lower_bound;
        return Ok(result);
    }

    let right_lower_to_left_upper =
        compare_histogram_bound(right.lower_bound, left.upper_bound, collation)?;
    if right_lower_to_left_upper != Ordering::Less {
        result.upper_bound = left.upper_bound;
        result.lower_bound = left.lower_bound;
        result.disjoint_ndv = result.disjoint_ndv.wrapping_add(right.ndv);
        result.ndv = left.ndv;
        return Ok(result);
    }

    let upper_ratio =
        calc_fraction_from_datums(right.lower_bound, right.upper_bound, left.upper_bound);
    let lower_order = compare_histogram_bound(right.lower_bound, left.lower_bound, collation)?;
    if lower_order != Ordering::Less {
        let lower_ratio =
            calc_fraction_from_datums(left.lower_bound, left.upper_bound, right.lower_bound);
        result.ndv = (lower_ratio * left.ndv as f64
            + source_max_float(
                (1.0 - lower_ratio) * left.ndv as f64,
                upper_ratio * right.ndv as f64,
            )
            + (1.0 - upper_ratio) * right.ndv as f64) as i64;
        result.lower_bound = left.lower_bound;
        return Ok(result);
    }

    let lower_ratio =
        calc_fraction_from_datums(right.lower_bound, right.upper_bound, left.lower_bound);
    result.ndv = (lower_ratio * right.ndv as f64
        + source_max_float(
            left.ndv as f64,
            (upper_ratio - lower_ratio) * right.ndv as f64,
        )
        + (1.0 - upper_ratio) * right.ndv as f64) as i64;
    Ok(result)
}

fn merge_partition_buckets<'a>(
    buckets: &[BucketForMerging<'a>],
    collation: Collation,
) -> Result<BucketForMerging<'a>, HistogramMergeError> {
    let Some(last) = buckets.last() else {
        return Err(HistogramMergeError::NotEnoughBuckets);
    };
    let mut result = BucketForMerging {
        lower_bound: last.lower_bound,
        upper_bound: last.upper_bound,
        count: 0,
        repeat: 0,
        ndv: 0,
        disjoint_ndv: 0,
    };
    let mut right = *last;
    let mut total_ndv = 0_i64;
    for (index, bucket) in buckets.iter().enumerate().rev() {
        total_ndv = total_ndv.wrapping_add(bucket.ndv);
        result.count = result.count.wrapping_add(bucket.count);
        if compare_histogram_bound(bucket.upper_bound, result.upper_bound, collation)?
            == Ordering::Equal
        {
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
    buckets: &mut [BucketForMerging<'_>],
    collation: Collation,
) -> Result<(), HistogramMergeError> {
    let mut comparison_error = None;
    buckets.sort_unstable_by(|left, right| {
        let upper = match compare_histogram_bound(left.upper_bound, right.upper_bound, collation) {
            Ok(ordering) => ordering,
            Err(error) => {
                comparison_error = Some(error);
                Ordering::Equal
            }
        };
        if upper != Ordering::Equal {
            return upper;
        }
        match compare_histogram_bound(left.lower_bound, right.lower_bound, collation) {
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
    buckets: &[BucketForMerging<'_>],
    collation: Collation,
) -> Result<bool, DatumValueError> {
    for pair in buckets.windows(2) {
        let upper = compare_histogram_bound(pair[0].upper_bound, pair[1].upper_bound, collation)?;
        if upper == Ordering::Greater {
            return Ok(false);
        }
        if upper == Ordering::Equal
            && compare_histogram_bound(pair[0].lower_bound, pair[1].lower_bound, collation)?
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
        return Ok(Some(Histogram::with_bound_type_size(
            first_histogram.id,
            0,
            total_null,
            first_histogram.last_update_version,
            first_histogram.bound_type_size(),
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
    let compare = |left, right| compare_histogram_bound(left, right, collation);
    let mut current_leftmost = None;
    let mut merge_buffer = Vec::new();
    let mut index = buckets.len() as isize - 1;

    while index >= 0 {
        let current_index = index as usize;
        let current_lower = buckets[current_index].lower_bound;
        match current_leftmost {
            None => current_leftmost = Some(current_lower),
            Some(leftmost) => {
                if compare(leftmost, current_lower)? == Ordering::Greater {
                    current_leftmost = Some(current_lower);
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
                if compare(
                    buckets[previous].upper_bound,
                    buckets[index as usize].upper_bound,
                )? != Ordering::Equal
                {
                    break;
                }
                sum = sum.wrapping_add(buckets[previous].count);
                index -= 1;
            }

            let mut leftmost = current_leftmost
                .take()
                .expect("the reverse scan always establishes a left bound");
            if compare(leftmost, buckets[index as usize].lower_bound)? == Ordering::Greater {
                leftmost = buckets[index as usize].lower_bound;
            }

            merge_buffer.clear();
            let mut cut_any = false;
            let leftmost_valid_nonoverlap = index as usize;
            while index > 0 {
                let previous = index as usize - 1;
                if compare(buckets[previous].upper_bound, leftmost)? == Ordering::Less {
                    break;
                }
                if compare(buckets[previous].lower_bound, leftmost)? != Ordering::Less {
                    sum = sum.wrapping_add(buckets[previous].count);
                    merge_buffer.push(buckets[previous]);
                    index -= 1;
                    continue;
                }

                let overlap = 1.0
                    - calc_fraction_from_datums(
                        buckets[previous].lower_bound,
                        buckets[previous].upper_bound,
                        leftmost,
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
                    lower_bound: leftmost,
                    upper_bound: buckets[previous].upper_bound,
                    count: overlapped_count,
                    repeat: 0,
                    ndv: overlapped_ndv,
                    disjoint_ndv: 0,
                };
                buckets[previous].upper_bound = leftmost;
                merge_buffer.push(cut);
                cut_any = true;
                index -= 1;
            }

            let start = index as usize;
            let mut merged = if !cut_any {
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
                    if compare(buckets[leftmost_invalid - 1].lower_bound, leftmost)?
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
        let mut leftmost = buckets[0].lower_bound;
        for bucket in &buckets[1..right_end] {
            if compare(leftmost, bucket.lower_bound)? == Ordering::Greater {
                leftmost = bucket.lower_bound;
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
                .equal_row_count(bucket.upper_bound, options.is_index, collation)
                .0;
        }
        if (repeat as i64) > bucket.repeat {
            bucket.repeat = repeat as i64;
        }
    }

    let mut global = Histogram::with_bound_type_size(
        first_histogram.id,
        0,
        total_null,
        first_histogram.last_update_version,
        first_histogram.bound_type_size(),
        global_buckets.len(),
        total_column_size,
    );
    for bucket in global_buckets {
        global.append_bucket_with_ndv(
            bucket.lower_bound.clone(),
            bucket.upper_bound.clone(),
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

    fn merge_bucket(bounds: &[Datum; 2], ndv: i64, disjoint_ndv: i64) -> BucketForMerging<'_> {
        BucketForMerging {
            lower_bound: &bounds[0],
            upper_bound: &bounds[1],
            count: ndv,
            repeat: 0,
            ndv,
            disjoint_ndv,
        }
    }

    /// Go `TestOutOfRangeRowCountZeroTimestampLowerBound` (8c6cb106410): a
    /// bucket whose lower bound is a zero TIMESTAMP (scalar -1) still yields
    /// a finite out-of-range estimate.
    #[test]
    fn test_out_of_range_row_count_zero_timestamp_lower_bound() {
        let timestamp = |core: tidb_datatype::CoreTime| {
            Datum::Time(tidb_datatype::Time::new(core, tidb_datatype::TimeType::Timestamp, 0).unwrap())
        };
        let zero = timestamp(tidb_datatype::CoreTime::from_date(0, 0, 0, 0, 0, 0, 0));
        let upper = timestamp(tidb_datatype::CoreTime::from_date(1970, 1, 1, 0, 0, 3, 0));
        let right = timestamp(tidb_datatype::CoreTime::from_date(1970, 1, 1, 0, 0, 4, 0));
        let mut hg = Histogram::new(0, 0, 0, 0, &FieldType::new(tidb_datatype::FieldTypeCode::Timestamp), 1, 0);
        hg.append_bucket(zero, upper.clone(), 2, 1);
        let estimate = hg.out_of_range_row_count(
            &upper,
            &right,
            OutOfRangeContext {
                realtime_row_count: 100,
                modify_count: 100,
                hist_ndv: 2,
                unsigned: false,
                allow_use_modify_count: true,
                skew_ratio: 0.5,
            },
        );
        const EPS: f64 = 1e-9;
        assert!((estimate.est - 45.937499984687506).abs() < EPS, "{estimate:?}");
        assert!((estimate.min_est - 18.374999993875).abs() < EPS, "{estimate:?}");
        assert!((estimate.max_est - 73.4999999755).abs() < EPS, "{estimate:?}");
    }

    #[test]
    fn calc_fraction_matches_edge_cases() {
        assert_eq!(calc_fraction(0.0, 0.0, 5.0), 0.5);
        assert_eq!(calc_fraction(0.0, 10.0, -1.0), 0.0);
        assert_eq!(calc_fraction(0.0, 10.0, 11.0), 1.0);
        assert_eq!(calc_fraction(0.0, 10.0, 5.0), 0.5);
    }

    #[test]
    fn prepared_scalars_are_invalidated_when_bounds_change() {
        fn bound(last: u8) -> Datum {
            let mut bytes = vec![b'a'; 128];
            bytes[127] = last;
            Datum::Bytes(bytes)
        }

        let mut original = Histogram::default();
        original.load_buckets_from_storage(
            &FieldType::new(FieldTypeCode::Blob),
            vec![Bucket {
                count: 100,
                repeat: 0,
                ndv: 100,
                lower_bound: bound(10),
                upper_bound: bound(100),
            }],
        );
        let query = bound(40);
        assert!(original.buckets.scalars.is_some());
        assert_eq!(original.calc_fraction(0, &query), 1.0 / 3.0);

        let mut copy = original.copy();
        let Datum::Bytes(upper) = &mut copy.buckets[0].upper_bound else {
            unreachable!();
        };
        upper[127] = 70;
        assert!(copy.buckets.scalars.is_none());
        assert_eq!(copy.calc_fraction(0, &query), 0.5);
        assert_eq!(original.calc_fraction(0, &query), 1.0 / 3.0);

        copy.pre_calculate_scalar();
        assert!(copy.buckets.scalars.is_some());
        assert_eq!(copy.calc_fraction(0, &query), 0.5);
        let numeric_query = Datum::Int(5);
        let bucket = &copy.buckets[0];
        assert_eq!(
            copy.calc_fraction(0, &numeric_query),
            calc_fraction_from_datums(&bucket.lower_bound, &bucket.upper_bound, &numeric_query),
        );
    }

    #[test]
    fn consuming_shared_buckets_preserves_the_source_histogram() {
        let field_type = FieldType::new(FieldTypeCode::Blob);
        let mut original = Histogram::new(1, 20, 0, 0, &field_type, 2, 64);
        original.append_bucket(Datum::Bytes(vec![1]), Datum::Bytes(vec![3]), 20, 1);
        original.pre_calculate_scalar();

        let mut copied = original.copy();
        let mut buckets: Vec<_> = std::mem::take(&mut copied.buckets).into();
        buckets[0].upper_bound = Datum::Bytes(vec![5]);
        copied.load_buckets_from_storage(&field_type, buckets);

        let query = Datum::Bytes(vec![2]);
        assert_eq!(original.calc_fraction(0, &query), 0.5);
        assert_eq!(copied.calc_fraction(0, &query), 0.25);
    }

    #[test]
    fn incremental_upper_offset_matches_recomputed_prefix() {
        let field = FieldType::new(FieldTypeCode::Blob);
        let mut tracked = Histogram::new(1, 5, 0, 0, &field, 2, 0);
        tracked.append_bucket(Datum::Bytes(vec![1; 24]), Datum::Bytes(vec![2; 64]), 2, 1);
        tracked.append_bucket(Datum::Bytes(vec![3; 48]), Datum::Bytes(vec![4; 400]), 4, 1);
        let mut recomputed = tracked.clone();

        for upper in [
            Datum::Bytes(vec![5; 32]),
            Datum::Bytes(vec![6; 512]),
            Datum::MaxValue,
            Datum::Bytes(vec![7; 640]),
        ] {
            tracked.update_last_bucket(Cow::Borrowed(&upper), 5, 1, false);
            recomputed.update_last_bucket_with_offset(Cow::Owned(upper), 5, 1, false, None);
            assert_eq!(tracked, recomputed);
            assert_eq!(tracked.memory_usage(), recomputed.memory_usage());
        }
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
            (([1, 2], 2, 0), ([1, 2], 3, 0), ([1, 2], 3, 0)),
            (([1, 3], 2, 0), ([2, 3], 2, 0), ([1, 3], 3, 0)),
            (([1, 3], 2, 0), ([4, 6], 2, 2), ([1, 3], 2, 4)),
            (([1, 5], 5, 0), ([2, 6], 5, 0), ([1, 6], 6, 0)),
            (([3, 5], 3, 0), ([2, 6], 4, 0), ([2, 6], 5, 0)),
        ];
        for (left, right, expected) in cases {
            let (left_bounds, left_ndv, left_disjoint) = left;
            let (right_bounds, right_ndv, right_disjoint) = right;
            let left_bounds = left_bounds.map(Datum::new_int);
            let right_bounds = right_bounds.map(Datum::new_int);
            let left = merge_bucket(&left_bounds, left_ndv, left_disjoint);
            let right = merge_bucket(&right_bounds, right_ndv, right_disjoint);
            let actual = merge_bucket_ndv(&left, &right, Collation::Binary).unwrap();
            let (bounds, ndv, disjoint_ndv) = expected;
            assert_eq!(actual.lower_bound, &Datum::new_int(bounds[0]));
            assert_eq!(actual.upper_bound, &Datum::new_int(bounds[1]));
            assert_eq!(actual.ndv, ndv);
            assert_eq!(actual.disjoint_ndv, disjoint_ndv);
        }
    }

    #[test]
    fn source_merge_bucket_ndv_empty_and_illegal_order_oracle() {
        let bounds = [Datum::new_int(1), Datum::new_int(2)];
        let mut left_empty = merge_bucket(&bounds, 2, 0);
        left_empty.count = 0;
        let right = merge_bucket(&bounds, 3, 0);
        assert_eq!(
            merge_bucket_ndv(&left_empty, &right, Collation::Binary).unwrap(),
            right
        );

        let left = merge_bucket(&bounds, 2, 0);
        let mut right_empty = merge_bucket(&bounds, 3, 0);
        right_empty.count = 0;
        let merged = merge_bucket_ndv(&left, &right_empty, Collation::Binary).unwrap();
        assert_eq!(merged.lower_bound, &Datum::new_int(1));
        assert_eq!(merged.upper_bound, &Datum::new_int(2));
        assert_eq!(merged.count, 0);
        assert_eq!(merged.ndv, 2);

        assert_eq!(
            merge_bucket_ndv(
                &merge_bucket(&[Datum::new_int(1), Datum::new_int(5)], 2, 0),
                &merge_bucket(&[Datum::new_int(1), Datum::new_int(4)], 2, 0),
                Collation::Binary,
            ),
            Err(HistogramMergeError::IllegalBucketOrder)
        );
        assert_eq!(
            merge_bucket_ndv(
                &merge_bucket(&[Datum::new_int(2), Datum::new_int(5)], 2, 0),
                &merge_bucket(&[Datum::new_int(1), Datum::new_int(5)], 2, 0),
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
