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

//! The allocation Go's `(*Histogram).MemoryUsage` measures
//! (`pkg/statistics/histogram.go`).
//!
//! Go keeps a histogram's bounds in `Bounds`, a one-column `chunk.Chunk`
//! (lower bound of bucket `i` at row `2*i`, upper at `2*i+1`), beside the
//! `Buckets` and `Scalars` slices. Its `MemoryUsage` sums `EmptyHistogramSize`,
//! `Bounds.MemoryUsage()`, `cap(Buckets)*EmptyBucketSize` and
//! `cap(Scalars)*EmptyScalarSize`. Every term is a Go capacity, so it depends
//! on the capacity the histogram was built with and on every `append` that
//! grew a slice since -- not on the values alone. The stats cache's quota and
//! eviction run on this number (`Table.MemoryUsage`, `CacheItemMemoryUsage`).
//!
//! [`Histogram`](crate::Histogram) keeps each bound as a [`Datum`] on its
//! bucket, so estimation never reads a chunk. Moving the bounds into a
//! `tidb_chunk` column would not give Go's number either: that column grows
//! by Rust `Vec` rules, not Go's `growslice`. This module therefore records
//! Go's allocation shape directly -- the lengths and capacities of the bounds
//! column's `nullBitmap`, `offsets`, `data` and `elemBuf`, the chunk
//! capacity, and `cap(Buckets)`/`cap(Scalars)` -- and replays each Go
//! operation's slice growth with Go's 64-bit `growslice` rule, so the
//! accounting is Go's to the byte along every construction path the histogram
//! methods mirror (`NewHistogram` + `AppendBucketWithNDV`, the storage loader,
//! `ConvertTo`, `updateLastBucket`, `mergeBuckets`, `popFirstBucket`,
//! `StandardizeForV2AnalyzeIndex`, `Copy`, `TruncateHistogram`,
//! `PreCalculateScalar`).

use tidb_chunk::column::{
    get_fixed_len, ESTIMATED_ELEM_LEN, MY_DECIMAL_STRUCT_SIZE, SIZE_TIME, VAR_ELEM_LEN,
};
use tidb_datatype::{Datum, FieldType};
use tidb_goruntime::go_runtime::{go_64_next_slice_capacity_for_element, GoSliceElementLayout};

/// Go `EmptyHistogramSize = unsafe.Sizeof(Histogram{})` on 64-bit: the `Tp`
/// and `Bounds` pointers (16), the `Buckets` and `Scalars` slice headers
/// (48), and six 8-byte fields (48).
pub const EMPTY_HISTOGRAM_SIZE: i64 = 112;

/// Go `EmptyBucketSize = unsafe.Sizeof(Bucket{})`: three `int64`s.
pub const EMPTY_BUCKET_SIZE: i64 = 24;

/// Go `EmptyScalarSize = unsafe.Sizeof(scalar{})`: two `float64`s and an
/// `int`.
pub const EMPTY_SCALAR_SIZE: i64 = 24;

/// Go `unsafe.Sizeof(chunk.Column{})` on 64-bit, the per-column term of
/// `Chunk.MemoryUsage`: `length` (8), four slice headers (96), and the padded
/// `avoidReusing` flag (8).
const GO_COLUMN_SIZE: i64 = 112;

/// Go's `append` capacity for a slice of `element_size`-byte, pointer-free
/// elements growing from `capacity` to hold `new_len` (Go `growslice`).
fn grown_capacity(new_len: usize, capacity: usize, element_size: usize) -> usize {
    go_64_next_slice_capacity_for_element(
        new_len,
        capacity,
        element_size,
        GoSliceElementLayout::NoPointers,
    )
}

/// One Go slice's `(len, cap)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SliceShape {
    len: usize,
    cap: usize,
}

impl SliceShape {
    const fn with_capacity(len: usize, cap: usize) -> Self {
        Self { len, cap }
    }

    /// Go `s = append(s, n elements...)`. Appending nothing never allocates,
    /// even to a full or nil slice.
    fn append(&mut self, count: usize, element_size: usize) {
        if count == 0 {
            return;
        }
        let new_len = self.len + count;
        if new_len > self.cap {
            self.cap = grown_capacity(new_len, self.cap, element_size);
        }
        self.len = new_len;
    }

    /// Go `append(nil, s...)`, the copy `Column.CopyConstruct` makes.
    fn copied(self, element_size: usize) -> Self {
        let mut copy = Self::default();
        copy.append(self.len, element_size);
        copy
    }
}

/// The length/capacity state of one Go `chunk.Column`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BoundsColumn {
    /// `len(elemBuf)` of a fixed-length column; `None` for a variable-length
    /// one (`elemBuf == nil`).
    elem_len: Option<usize>,
    elem_buf_cap: usize,
    length: usize,
    null_bitmap: SliceShape,
    offsets: SliceShape,
    data: SliceShape,
}

impl BoundsColumn {
    /// Go `newColumn(typeSize, capacity)`: `newFixedLenColumn` or
    /// `newVarLenColumn`, both of which `make` exact capacities.
    fn new(type_size: i64, capacity: usize) -> Self {
        let null_bitmap = SliceShape::with_capacity(0, (capacity + 7) >> 3);
        if type_size == VAR_ELEM_LEN {
            Self {
                elem_len: None,
                elem_buf_cap: 0,
                length: 0,
                null_bitmap,
                offsets: SliceShape::with_capacity(1, capacity + 1),
                data: SliceShape::with_capacity(0, ESTIMATED_ELEM_LEN * capacity),
            }
        } else {
            let elem_len = usize::try_from(type_size).expect("fixed element length");
            Self {
                elem_len: Some(elem_len),
                elem_buf_cap: elem_len,
                length: 0,
                null_bitmap,
                offsets: SliceShape::default(),
                data: SliceShape::with_capacity(0, elem_len * capacity),
            }
        }
    }

    /// Go `appendNullBitmap`: one more bitmap byte every eighth row.
    fn append_null_bitmap(&mut self) {
        if self.length >> 3 >= self.null_bitmap.len {
            self.null_bitmap.append(1, 1);
        }
    }

    /// Go `Chunk.AppendDatum` into this column: `finishAppendFixed` for a
    /// fixed-length column, `AppendNull`, or the variable-length appends
    /// followed by `finishAppendVar`.
    fn append_datum(&mut self, datum: &Datum) {
        match (datum, self.elem_len) {
            // Go `AppendDatum` has no arm for the range sentinels.
            (Datum::MinNotNull | Datum::MaxValue, _) => return,
            (Datum::Null, Some(elem_len)) => {
                self.append_null_bitmap();
                self.data.append(elem_len, 1);
            }
            (Datum::Null, None) => {
                self.append_null_bitmap();
                self.offsets.append(1, 8);
            }
            (_, Some(elem_len)) => {
                self.data.append(elem_len, 1);
                self.append_null_bitmap();
            }
            (_, None) => {
                for segment in var_len_cell_appends(datum) {
                    self.data.append(segment, 1);
                }
                self.append_null_bitmap();
                self.offsets.append(1, 8);
            }
        }
        self.length += 1;
    }

    /// Go `Chunk.TruncateTo` on this column; `data_len` is the byte length of
    /// the retained rows, which only the bounds themselves know.
    fn truncate_to(&mut self, rows: usize, data_len: usize) {
        match self.elem_len {
            Some(elem_len) => self.data.len = rows * elem_len,
            None => {
                self.data.len = data_len;
                self.offsets.len = rows + 1;
            }
        }
        self.length = rows;
        self.null_bitmap.len = (rows + 7) / 8;
    }

    /// Go `Column.CopyConstruct(nil)`: every slice is `append(nil, s...)`.
    fn copy_construct(&self) -> Self {
        Self {
            elem_len: self.elem_len,
            elem_buf_cap: SliceShape::with_capacity(self.elem_len.unwrap_or(0), 0)
                .copied(1)
                .cap,
            length: self.length,
            null_bitmap: self.null_bitmap.copied(1),
            offsets: self.offsets.copied(8),
            data: self.data.copied(1),
        }
    }

    /// The column's term of Go `Chunk.MemoryUsage`.
    fn memory_usage(&self) -> i64 {
        let bytes = self.null_bitmap.cap + self.offsets.cap * 8 + self.data.cap + self.elem_buf_cap;
        GO_COLUMN_SIZE.wrapping_add(i64::try_from(bytes).unwrap_or(i64::MAX))
    }
}

/// The byte segments Go appends to a variable-length column's `data` for one
/// datum, one `append` call each (`AppendBytes`, `appendNameValue`'s value
/// then name, `AppendJSON`'s type code then value, `AppendVectorFloat32`).
/// Go never appends a fixed-width kind to a variable-length column (its
/// typed chunk would index an empty `elemBuf`); such a datum is accounted at
/// the width its own column type stores.
fn var_len_cell_appends(datum: &Datum) -> Vec<usize> {
    match datum {
        Datum::String(value) => vec![value.bytes().len()],
        Datum::Bytes(value) | Datum::Raw(value) => vec![value.len()],
        Datum::BinaryLiteral(value) | Datum::Bit(value) => vec![value.as_bytes().len()],
        Datum::Enum(value, _) => vec![8, value.name_bytes().len()],
        Datum::Set(value, _) => vec![8, value.name_bytes().len()],
        Datum::Json(value) => vec![1, value.value().len()],
        Datum::VectorFloat32(value) => vec![value.serialized_size()],
        other => bound_type_size(other)
            .and_then(|size| usize::try_from(size).ok())
            .map_or_else(Vec::new, |size| vec![size]),
    }
}

/// Go `getFixedLen` of the column type a bound of this kind is stored under
/// (`VAR_ELEM_LEN` for the variable-length kinds); `None` for NULL and the
/// range sentinels, which every column type can hold.
#[must_use]
pub fn bound_type_size(datum: &Datum) -> Option<i64> {
    match datum {
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => None,
        Datum::Float32(_) => Some(4),
        Datum::Int(_) | Datum::UInt(_) | Datum::Real(_) | Datum::Duration(_) => Some(8),
        Datum::Time(_) => Some(SIZE_TIME),
        Datum::Decimal(_) => Some(MY_DECIMAL_STRUCT_SIZE),
        Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Raw(_)
        | Datum::BinaryLiteral(_)
        | Datum::Bit(_)
        | Datum::Enum(..)
        | Datum::Set(..)
        | Datum::Json(_)
        | Datum::VectorFloat32(_) => Some(VAR_ELEM_LEN),
    }
}

/// Go `getFixedLen(tp)` for a histogram's bound type. Go's
/// `prepareFieldTypeForHistogram` only changes a string type's collation,
/// which never changes the element length.
#[must_use]
pub fn histogram_bound_type_size(field_type: &FieldType) -> i64 {
    get_fixed_len(field_type)
}

/// Go `Histogram.Bounds`: the chunk capacity and its columns.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BoundsChunk {
    capacity: usize,
    /// Always one column, except after Go `popFirstBucket`, which rebuilds
    /// the chunk with two columns of the histogram type and fills only the
    /// first.
    columns: Vec<BoundsColumn>,
}

impl BoundsChunk {
    /// Go `chunk.NewChunkWithCapacity`/`NewChunkFromPoolWithCapacity` over
    /// `column_count` columns of one type. A pooled column comes from the
    /// pool's `newColumn` (or is a reset column of the same shape).
    fn new(type_size: i64, capacity: usize, column_count: usize) -> Self {
        Self {
            capacity,
            columns: (0..column_count)
                .map(|_| BoundsColumn::new(type_size, capacity))
                .collect(),
        }
    }

    fn rows(&self) -> usize {
        self.columns.first().map_or(0, |column| column.length)
    }

    fn type_size(&self) -> i64 {
        self.columns
            .first()
            .and_then(|column| column.elem_len)
            .map_or(VAR_ELEM_LEN, |elem_len| elem_len as i64)
    }

    fn memory_usage(&self) -> i64 {
        self.columns
            .iter()
            .fold(0_i64, |sum, column| sum.wrapping_add(column.memory_usage()))
    }
}

/// Go's allocation shape of one [`Histogram`](crate::Histogram): `Bounds`,
/// `cap(Buckets)` and `cap(Scalars)`, maintained by the histogram methods
/// that mirror Go's.
///
/// The default value describes a histogram no Go constructor shaped -- a
/// struct literal, or [`Histogram::default`](crate::Histogram::default).
/// [`Histogram::memory_usage`](crate::Histogram::memory_usage) accounts such a
/// histogram, and one whose buckets were edited around these methods, as Go's
/// storage loader builds it (`histogramFromStorageWithParams`), since that is
/// the shape every stats-cache histogram arrives in.
///
/// Allocation is not part of a histogram's value: two histograms with the
/// same buckets are equal however they were built (Go `HistogramEqual`
/// compares `ToString`), so equality ignores this field.
#[derive(Clone, Debug, Default)]
pub struct HistogramAllocation {
    /// `None` until a Go-shaped constructor runs.
    bounds: Option<BoundsChunk>,
    /// `len(Buckets)` as these methods last left it, which detects bucket
    /// edits made around them.
    bucket_len: usize,
    bucket_cap: usize,
    scalar_cap: usize,
}

impl PartialEq for HistogramAllocation {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl HistogramAllocation {
    /// Go `NewHistogram(.., tp, bucketSize, ..)`: a pooled bounds chunk with
    /// capacity `2*bucketSize` and `make([]Bucket, 0, bucketSize)`.
    pub(crate) fn new(type_size: i64, bucket_size: usize) -> Self {
        Self {
            bounds: Some(BoundsChunk::new(type_size, 2 * bucket_size, 1)),
            bucket_len: 0,
            bucket_cap: bucket_size,
            scalar_cap: 0,
        }
    }

    /// Whether this records the allocation of a histogram that has
    /// `bucket_len` buckets.
    pub(crate) fn is_tracking(&self, bucket_len: usize) -> bool {
        self.bounds.is_some() && self.bucket_len == bucket_len
    }

    /// The bound column's Go `getFixedLen`, when a constructor recorded it.
    pub(crate) fn type_size(&self) -> Option<i64> {
        self.bounds.as_ref().map(BoundsChunk::type_size)
    }

    /// Go `AppendBucketWithNDV`: `Buckets = append(Buckets, b)` then both
    /// bounds appended to `Bounds`.
    pub(crate) fn append_bucket(&mut self, lower: &Datum, upper: &Datum) {
        let Some(bounds) = &mut self.bounds else {
            return;
        };
        let mut buckets = SliceShape::with_capacity(self.bucket_len, self.bucket_cap);
        buckets.append(1, EMPTY_BUCKET_SIZE as usize);
        self.bucket_len = buckets.len;
        self.bucket_cap = buckets.cap;
        bounds.columns[0].append_datum(lower);
        bounds.columns[0].append_datum(upper);
    }

    /// Go `updateLastBucket`'s bounds edit: `TruncateTo(2*l-1)` then the new
    /// upper bound appended. `retained_data_len` is the byte length of the
    /// bounds kept by the truncation.
    pub(crate) fn replace_last_upper(&mut self, retained_data_len: usize, upper: &Datum) {
        let Some(bounds) = &mut self.bounds else {
            return;
        };
        let column = &mut bounds.columns[0];
        column.truncate_to(column.length.saturating_sub(1), retained_data_len);
        column.append_datum(upper);
    }

    /// Go's rebuilds of `Bounds` from retained buckets: `mergeBuckets`
    /// (`NewChunkWithCapacity(.., bucketIdx)`) and
    /// `StandardizeForV2AnalyzeIndex` (`NewChunkWithCapacity(.., len(kept))`),
    /// both followed by one `AppendDatum` per retained bound. `Buckets` is
    /// resliced, so its capacity stays.
    pub(crate) fn rebuild_bounds<'a>(
        &mut self,
        chunk_capacity: usize,
        bounds: impl IntoIterator<Item = &'a Datum>,
        bucket_len: usize,
    ) {
        let Some(old) = &self.bounds else {
            return;
        };
        let mut rebuilt = BoundsChunk::new(old.type_size(), chunk_capacity, 1);
        for bound in bounds {
            rebuilt.columns[0].append_datum(bound);
        }
        self.bounds = Some(rebuilt);
        self.bucket_len = bucket_len;
    }

    /// Go `popFirstBucket`: `Buckets = Buckets[1:]` (one less capacity), and
    /// a two-column chunk with capacity `NumRows()-2` into which
    /// `Chunk.Append` copies the remaining rows of the single source column
    /// in one `data` append.
    pub(crate) fn pop_first_bucket<'a>(&mut self, remaining: impl IntoIterator<Item = &'a Datum>) {
        let Some(old) = &self.bounds else {
            return;
        };
        let rows = old.rows().saturating_sub(2);
        let mut rebuilt = BoundsChunk::new(old.type_size(), rows, 2);
        let column = &mut rebuilt.columns[0];
        let mut data_len = 0_usize;
        let mut cells = 0_usize;
        for bound in remaining {
            data_len += column.elem_len.unwrap_or_else(|| var_len_cell_len(bound));
            cells += 1;
        }
        column.data.append(data_len, 1);
        if column.elem_len.is_none() {
            for _ in 0..cells {
                column.offsets.append(1, 8);
            }
        }
        for _ in 0..cells {
            column.append_null_bitmap();
            column.length += 1;
        }
        self.bounds = Some(rebuilt);
        self.bucket_len = self.bucket_len.saturating_sub(1);
        self.bucket_cap = self.bucket_cap.saturating_sub(1);
    }

    /// Go `Histogram.Copy`: `Bounds.CopyConstruct()` (a chunk keeping the
    /// capacity, with `append(nil, s...)` copies of every column slice) and
    /// `make([]Bucket, 0, len(Buckets))`; `Scalars` is shared.
    pub(crate) fn copy(&self) -> Self {
        Self {
            bounds: self.bounds.as_ref().map(|bounds| BoundsChunk {
                capacity: bounds.capacity,
                columns: bounds
                    .columns
                    .iter()
                    .map(BoundsColumn::copy_construct)
                    .collect(),
            }),
            bucket_len: self.bucket_len,
            bucket_cap: self.bucket_len,
            scalar_cap: self.scalar_cap,
        }
    }

    /// Go `TruncateHistogram`'s `Buckets[:numBkt]` and
    /// `Bounds.TruncateTo(numBkt*2)` on a copy.
    pub(crate) fn truncate_buckets(&mut self, bucket_len: usize, retained_data_len: usize) {
        let Some(bounds) = &mut self.bounds else {
            return;
        };
        for column in &mut bounds.columns {
            column.truncate_to(2 * bucket_len, retained_data_len);
        }
        self.bucket_len = bucket_len;
    }

    /// Go `PreCalculateScalar`'s `Scalars = make([]scalar, len(Buckets))`.
    pub(crate) fn set_scalar_len(&mut self, len: usize) {
        self.scalar_cap = len;
    }

    /// Go `Index.EvictAllStats`'s `Buckets = nil`, which keeps `Bounds` and
    /// `Scalars`.
    pub(crate) fn drop_buckets(&mut self) {
        if self.bounds.is_some() {
            self.bucket_len = 0;
            self.bucket_cap = 0;
        }
    }

    /// Go `(*Histogram).MemoryUsage` over this allocation.
    pub(crate) fn memory_usage(&self) -> i64 {
        let capacity = self.bounds.as_ref().map_or(0, |bounds| bounds.capacity);
        if self.bucket_len == 0 && self.scalar_cap == 0 && capacity == 0 {
            return 0;
        }
        let bounds = self.bounds.as_ref().map_or(0, BoundsChunk::memory_usage);
        let as_i64 = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
        EMPTY_HISTOGRAM_SIZE
            .wrapping_add(bounds)
            .wrapping_add(as_i64(self.bucket_cap).wrapping_mul(EMPTY_BUCKET_SIZE))
            .wrapping_add(as_i64(self.scalar_cap).wrapping_mul(EMPTY_SCALAR_SIZE))
    }
}

/// Go `PreCalculateScalar`'s gate: scalars are built when the first lower
/// bound, read back through the histogram type, is a decimal, a time, or a
/// string/bytes value.
#[must_use]
pub fn pre_calculates_scalars(first_lower: &Datum) -> bool {
    matches!(
        first_lower,
        Datum::Decimal(_) | Datum::Time(_) | Datum::Bytes(_) | Datum::String(_)
    )
}

/// The byte length a bound occupies in a variable-length bounds column.
pub(crate) fn var_len_cell_len(datum: &Datum) -> usize {
    if matches!(datum, Datum::Null) {
        return 0;
    }
    var_len_cell_appends(datum).iter().sum()
}

#[cfg(test)]
mod tests {
    //! Every expected number below is Go's: a probe in `pkg/statistics`
    //! built the same histograms with the real `NewHistogram`,
    //! `AppendBucketWithNDV`, `updateLastBucket`, `mergeBuckets`,
    //! `popFirstBucket`, `StandardizeForV2AnalyzeIndex`, `Copy`,
    //! `TruncateHistogram`, `PreCalculateScalar`, `HistogramFromProto` and
    //! `MergeHistograms`, and printed `(*Histogram).MemoryUsage()` (go1.27,
    //! darwin/arm64; `EmptyHistogramSize 112 EmptyBucketSize 24
    //! EmptyScalarSize 24`).

    use tidb_datatype::{BinaryJSON, BinaryLiteral, BinaryLiteralWidth, CoreTime, Datum, Decimal, FieldType, FieldTypeCode, MySqlDuration, MysqlEnum, Time, TimeType};
use tidb_charset::Collation;

    use crate::histogram::merge_histograms;
    use crate::{Bucket, CmsSketch, FmSketch, Histogram, TopN};

    fn tp(code: FieldTypeCode) -> FieldType {
        FieldType::new(code)
    }

    fn padded(value: usize, width: usize) -> Datum {
        Datum::Bytes(format!("{value:0width$}").into_bytes())
    }

    /// The probe's `zzVarchar`: `n` buckets of `width`-byte bounds in a
    /// VARCHAR histogram built with capacity `bucket_size`.
    fn varchar(n: usize, width: usize, bucket_size: usize) -> Histogram {
        let mut histogram = Histogram::new(
            1,
            2 * n as i64,
            0,
            0,
            &tp(FieldTypeCode::Varchar),
            bucket_size,
            0,
        );
        for i in 0..n {
            histogram.append_bucket_with_ndv(
                padded(2 * i, width),
                padded(2 * i + 1, width),
                (i as i64 + 1) * 2,
                1,
                2,
            );
        }
        histogram
    }

    fn ints(n: usize, bucket_size: usize) -> Histogram {
        let mut histogram =
            Histogram::new(1, 3000, 0, 0, &tp(FieldTypeCode::LongLong), bucket_size, 0);
        for i in 0..n as i64 {
            histogram.append_bucket(Datum::Int(i * 12), Datum::Int(i * 12 + 11), (i + 1) * 12, 1);
        }
        histogram
    }

    #[test]
    fn storage_shaped_histograms_match_go() {
        // The live `acc.t` columns: `id` (250 int buckets) and `v` (250
        // VARCHAR buckets of at most 8-byte bounds) report 10295 and 20295.
        let mut id = ints(250, 250);
        id.pre_calculate_scalar();
        assert_eq!(id.memory_usage(), 10295);
        let mut v = varchar(250, 5, 250);
        v.pre_calculate_scalar();
        assert_eq!(v.memory_usage(), 20295);

        for (n, width, expected) in [(3, 20, 617), (256, 100, 74024), (40, 13, 4338)] {
            let mut histogram = varchar(n, width, n);
            histogram.pre_calculate_scalar();
            assert_eq!(histogram.memory_usage(), expected, "varchar {n}x{width}");
        }

        let mut index = Histogram::new(2, 6, 0, 0, &tp(FieldTypeCode::Blob), 3, 0);
        for i in 0..3 {
            index.append_bucket_with_ndv(
                padded(2 * i, 9),
                padded(2 * i + 1, 9),
                (i as i64 + 1) * 2,
                1,
                0,
            );
        }
        index.pre_calculate_scalar();
        assert_eq!(index.memory_usage(), 521);
    }

    #[test]
    fn storage_loader_and_untracked_literals_match_go() {
        let mut loaded = Histogram {
            id: 1,
            ndv: 3000,
            ..Histogram::default()
        };
        loaded.load_buckets_from_storage(&tp(FieldTypeCode::LongLong), ints(250, 250).buckets);
        assert_eq!(loaded.memory_usage(), 10295);
        loaded.load_buckets_from_storage(&tp(FieldTypeCode::Varchar), varchar(250, 5, 250).buckets);
        assert_eq!(loaded.memory_usage(), 20295);

        // A struct literal carries no Go allocation and is accounted as the
        // storage loader builds it.
        let literal = Histogram {
            id: 1,
            buckets: varchar(3, 20, 3).buckets,
            ..Histogram::default()
        };
        assert_eq!(literal.memory_usage(), 617);
        // Its `Copy` is Go's copy of that loaded histogram.
        assert_eq!(literal.copy().memory_usage(), 568);
        assert_eq!(Histogram::default().memory_usage(), 0);
    }

    #[test]
    fn empty_capacity_histograms_match_go() {
        // Go bootstrap's index histogram: `NewHistogram(.., TypeBlob, 32, ..)`.
        let blob = Histogram::new(2, 6, 0, 0, &tp(FieldTypeCode::Blob), 32, 0);
        assert_eq!(blob.memory_usage(), 2032);
        let int = Histogram::new(2, 6, 0, 0, &tp(FieldTypeCode::LongLong), 32, 0);
        assert_eq!(int.memory_usage(), 1520);
        let none = Histogram::new(2, 6, 0, 0, &tp(FieldTypeCode::LongLong), 0, 0);
        assert_eq!(none.memory_usage(), 0);
    }

    #[test]
    fn copy_and_truncate_match_go() {
        let mut b2 = varchar(3, 20, 3);
        b2.pre_calculate_scalar();
        assert_eq!(b2.copy().memory_usage(), 568);
        let int5 = ints(5, 5);
        assert_eq!(int5.memory_usage(), 434);
        assert_eq!(int5.copy().memory_usage(), 440);
        assert_eq!(int5.truncate(2).memory_usage(), 440);
        // A Rust clone stands for Go's shared pointer, not `Copy`.
        assert_eq!(int5.clone().memory_usage(), 434);
    }

    #[test]
    fn rebuilding_operations_match_go() {
        let mut updated = Histogram::new(1, 0, 0, 0, &tp(FieldTypeCode::Varchar), 2, 0);
        updated.append_bucket(
            Datum::Bytes(b"a".to_vec()),
            Datum::Bytes(vec![b'b'; 20]),
            1,
            1,
        );
        updated.update_last_bucket(Datum::Bytes(b"c".to_vec()), 2, 1, false);
        updated.append_bucket(
            Datum::Bytes(b"d".to_vec()),
            Datum::Bytes(b"e".to_vec()),
            3,
            1,
        );
        assert_eq!(updated.memory_usage(), 345);

        let mut standardized = Histogram::new(2, 6, 0, 0, &tp(FieldTypeCode::Blob), 4, 0);
        for (i, count) in [2, 2, 5, 7].into_iter().enumerate() {
            standardized.append_bucket_with_ndv(
                padded(2 * i, 9),
                padded(2 * i + 1, 9),
                count,
                0,
                3,
            );
        }
        standardized.standardize_for_v2_analyze_index();
        assert_eq!(standardized.len(), 3);
        assert_eq!(standardized.memory_usage(), 481);

        let mut merged = varchar(5, 6, 5);
        merged.merge_neighbor_buckets(4);
        assert_eq!(merged.memory_usage(), 489);

        let mut popped = varchar(3, 6, 3);
        popped.pop_first_bucket();
        assert_eq!(popped.memory_usage(), 530);

        let left = varchar(3, 6, 3);
        let mut right = Histogram::new(1, 6, 0, 0, &tp(FieldTypeCode::Varchar), 3, 0);
        for i in 3..6 {
            right.append_bucket_with_ndv(
                padded(2 * i, 6),
                padded(2 * i + 1, 6),
                (i as i64 - 2) * 2,
                1,
                2,
            );
        }
        let global = merge_histograms(left, right, 4, 2, Collation::Binary).unwrap();
        assert_eq!(global.memory_usage(), 505);
    }

    #[test]
    fn bound_layouts_match_go() {
        let typed = |code: FieldTypeCode, make: &dyn Fn(usize) -> Datum| {
            let mut histogram = Histogram::new(3, 6, 0, 0, &tp(code), 3, 0);
            for i in 0..3 {
                histogram.append_bucket(make(2 * i), make(2 * i + 1), (i as i64 + 1) * 2, 1);
            }
            histogram.pre_calculate_scalar();
            histogram.memory_usage()
        };
        let decimal = |i: usize| Datum::Decimal(Decimal::from_int(i as i64));
        assert_eq!(typed(FieldTypeCode::NewDecimal, &decimal), 649);
        let datetime = |i: usize| {
            let core = CoreTime::from_date(2020, 1, i as u8 + 1, 0, 0, 0, 0);
            Datum::Time(Time::new(core, TimeType::DateTime, 0).unwrap())
        };
        assert_eq!(typed(FieldTypeCode::Datetime, &datetime), 425);
        assert_eq!(
            typed(FieldTypeCode::Float, &|i| Datum::Float32(i as f64)),
            325
        );
        assert_eq!(
            typed(FieldTypeCode::Double, &|i| Datum::Real(i as f64)),
            353
        );
        let hour =
            |_| Datum::Duration(MySqlDuration::from_nanoseconds(3_600_000_000_000, 0).unwrap());
        assert_eq!(typed(FieldTypeCode::Duration, &hour), 353);
        let enumeration = |i: usize| {
            Datum::Enum(
                MysqlEnum::new(format!("{i:03}"), i as u64 + 1),
                Collation::Binary,
            )
        };
        assert_eq!(typed(FieldTypeCode::Enum, &enumeration), 449);
        let json = |i: usize| Datum::Json(BinaryJSON::parse(&i.to_string()).unwrap());
        assert_eq!(typed(FieldTypeCode::Json, &json), 449);
        let width = BinaryLiteralWidth::try_from(2_u8).unwrap();
        let bit = |i: usize| Datum::Bit(BinaryLiteral::from_uint(i as u64, Some(width)));
        assert_eq!(typed(FieldTypeCode::Bit, &bit), 401);

        // `HistogramFromProto`: TypeBlob bytes, no `PreCalculateScalar`.
        let mut proto = Histogram::new(0, 6, 0, 0, &tp(FieldTypeCode::Blob), 3, 0);
        for i in 0..3 {
            proto.append_bucket(padded(2 * i, 9), padded(2 * i + 1, 9), i as i64 + 1, 1);
        }
        assert_eq!(proto.memory_usage(), 449);

        let mut null_lower = Histogram::new(1, 0, 0, 0, &tp(FieldTypeCode::Varchar), 1, 0);
        null_lower.append_bucket(Datum::Null, Datum::Bytes(b"xyz".to_vec()), 1, 1);
        assert_eq!(null_lower.memory_usage(), 289);
    }

    #[test]
    fn a_storage_round_trip_replaces_the_build_allocation() {
        // `NewHistogram(.., TypeLonglong, 256, ..)` before any bucket, as the
        // analyze builder allocates it.
        let mut built = Histogram::new(1, 0, 0, 0, &tp(FieldTypeCode::LongLong), 256, 0);
        assert_eq!(built.memory_usage(), 10536);
        // Go reloads an all-TopN column with no bucket rows: capacity 0.
        built.round_trip_through_storage(&tp(FieldTypeCode::LongLong));
        assert_eq!(built.memory_usage(), 0);

        let mut built = ints(250, 256);
        built.round_trip_through_storage(&tp(FieldTypeCode::LongLong));
        assert_eq!(built.memory_usage(), 10295);
    }

    #[test]
    fn appends_past_capacity_grow_like_go() {
        assert_eq!(ints(5, 1).memory_usage(), 560);
        assert_eq!(ints(3, 0).memory_usage(), 400);
        assert_eq!(varchar(30, 30, 0).memory_usage(), 3560);
    }

    #[test]
    fn sketch_memory_matches_go() {
        let mut topn = TopN::new(3);
        topn.append(b"abcdefghi", 3);
        topn.append(b"ab", 3);
        assert_eq!(topn.memory_usage(), 107);
        assert_eq!(CmsSketch::new(5, 2048).memory_usage(), 40960);
        let mut fm = FmSketch::new(1000);
        fm.insert_hash(0);
        assert_eq!(fm.memory_usage(), 24);
    }

    #[test]
    fn bucket_edits_around_the_methods_fall_back_to_the_storage_shape() {
        let mut histogram = varchar(3, 20, 8);
        histogram.buckets.push(Bucket {
            count: 8,
            repeat: 1,
            ndv: 0,
            lower_bound: padded(6, 20),
            upper_bound: padded(7, 20),
        });
        let literal = Histogram {
            buckets: histogram.buckets.clone(),
            ..Histogram::default()
        };
        assert_eq!(histogram.memory_usage(), literal.memory_usage());
    }
}
