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

//! Complete Count-Min Sketch, sampled TopN, and snapshot primitives from
//! `pkg/statistics/cmsketch.go`.
//!
//! Datum/tablecodec encoding, histogram/statistics-handle integration, and
//! session tracing remain explicit caller seams; this module owns all of the
//! arithmetic, ranking, merge, and protobuf-wire behavior below those seams.

use std::{cmp::Ordering, collections::HashMap, fmt};

use crate::estimate::calculate_estimate_ndv;
use tidb_datatype::Datum;

const MURMUR_C1: u64 = 0x87c3_7b91_1142_53d5;
const MURMUR_C2: u64 = 0x4cf5_ad43_2745_937f;
const VALUE_VARINT_FLAG: u8 = 8;
const VALUE_UVARINT_FLAG: u8 = 9;

/// The 128-bit hash pair returned by Go's `murmur3.Sum128`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Hash128 {
    /// First hash lane used by the source bucket formula.
    pub h1: u64,
    /// Second hash lane used as the row stride.
    pub h2: u64,
}

impl Hash128 {
    /// Hashes bytes with the same seed and little-endian Murmur3 x64-128
    /// algorithm used by `github.com/twmb/murmur3.Sum128`.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut h1 = 0_u64;
        let mut h2 = 0_u64;

        for block in bytes.chunks_exact(16) {
            let k1 = u64::from_le_bytes(block[..8].try_into().expect("eight-byte block"));
            let k2 = u64::from_le_bytes(block[8..].try_into().expect("eight-byte block"));
            let k1 = mix_k1(k1);
            h1 ^= k1;
            h1 = h1.rotate_left(27).wrapping_add(h2);
            h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

            let k2 = mix_k2(k2);
            h2 ^= k2;
            h2 = h2.rotate_left(31).wrapping_add(h1);
            h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
        }

        let tail = &bytes[bytes.len() / 16 * 16..];
        let mut k1 = 0_u64;
        let mut k2 = 0_u64;
        for (index, byte) in tail.iter().copied().enumerate() {
            if index < 8 {
                k1 ^= u64::from(byte) << (index * 8);
            } else {
                k2 ^= u64::from(byte) << ((index - 8) * 8);
            }
        }
        if tail.len() > 8 {
            h2 ^= mix_k2(k2);
        }
        if !tail.is_empty() {
            h1 ^= mix_k1(k1);
        }

        h1 ^= bytes.len() as u64;
        h2 ^= bytes.len() as u64;
        h1 = h1.wrapping_add(h2);
        h2 = h2.wrapping_add(h1);
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 = h1.wrapping_add(h2);
        h2 = h2.wrapping_add(h1);
        Self { h1, h2 }
    }
}

/// Hashes bytes using the source's zero-seed Murmur3 x64-128 function.
#[must_use]
pub fn hash_bytes(bytes: &[u8]) -> Hash128 {
    Hash128::from_bytes(bytes)
}

/// An unsupported Datum at the dependency-closed statistics value boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DatumEncodingError;

impl fmt::Display for DatumEncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CMSketch integer query requires Int or UInt Datum")
    }
}

impl std::error::Error for DatumEncodingError {}

/// Encodes an integer Datum exactly like Go `codec.EncodeValue`.
///
/// This is deliberately the non-comparable value encoding used by
/// `statistics.QueryValue`: a signed integer has the varint tag and Go's
/// zig-zag LEB128 payload; an unsigned integer has the uvarint tag. Other
/// Datum domains still require their timezone/type-aware codec owners.
pub fn encode_integer_datum_value(value: &Datum) -> Result<Vec<u8>, DatumEncodingError> {
    let mut encoded = Vec::with_capacity(11);
    match value {
        Datum::Int(value) => {
            encoded.push(VALUE_VARINT_FLAG);
            let mut zigzag = (*value as u64) << 1;
            if *value < 0 {
                zigzag = !zigzag;
            }
            encode_varint(zigzag, &mut encoded);
        }
        Datum::UInt(value) => {
            encoded.push(VALUE_UVARINT_FLAG);
            encode_varint(*value, &mut encoded);
        }
        _ => return Err(DatumEncodingError),
    }
    Ok(encoded)
}

fn mix_k1(mut value: u64) -> u64 {
    value = value.wrapping_mul(MURMUR_C1);
    value = value.rotate_left(31);
    value.wrapping_mul(MURMUR_C2)
}

fn mix_k2(mut value: u64) -> u64 {
    value = value.wrapping_mul(MURMUR_C2);
    value = value.rotate_left(33);
    value.wrapping_mul(MURMUR_C1)
}

fn fmix64(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    value ^ (value >> 33)
}

/// Errors returned when a sketch's dimensions cannot represent the source
/// query geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SketchShapeError {
    /// The row-by-column allocation overflows the platform's `usize`.
    TooLarge,
}

impl fmt::Display for SketchShapeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge => formatter.write_str("CMSketch dimensions exceed addressable memory"),
        }
    }
}

impl std::error::Error for SketchShapeError {}

/// An error returned by a dimension-incompatible sketch merge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MergeError {
    /// Destination depth.
    pub destination_depth: u32,
    /// Destination width.
    pub destination_width: u32,
    /// Source depth.
    pub source_depth: u32,
    /// Source width.
    pub source_width: u32,
}

impl fmt::Display for MergeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "CMSketch dimensions differ: destination {}x{}, source {}x{}",
            self.destination_depth, self.destination_width, self.source_depth, self.source_width
        )
    }
}

impl std::error::Error for MergeError {}

/// A dependency-closed Count-Min Sketch.
///
/// Counters use the source's `uint32` wrapping arithmetic.  `count` remains a
/// `uint64`, as in Go, and therefore a caller that inserts a count larger than
/// `u32::MAX` observes the same truncation in each bucket while retaining the
/// full total count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CmsSketch {
    depth: u32,
    width: u32,
    counters: Vec<u32>,
    count: u64,
    default_value: u64,
}

impl CmsSketch {
    /// Creates a sketch with `depth` rows and `width` counters per row.
    ///
    /// `new` follows Go's constructor contract: zero-sized geometries and a
    /// width of one can be allocated, although operations that divide or take
    /// a remainder by the width retain the source's caller preconditions.
    #[must_use]
    pub fn new(depth: u32, width: u32) -> Self {
        Self::try_new(depth, width).expect("invalid CMSketch dimensions")
    }

    /// Creates a sketch while reporting address-space overflow instead of
    /// panicking.
    pub fn try_new(depth: u32, width: u32) -> Result<Self, SketchShapeError> {
        let depth_usize = usize::try_from(depth).map_err(|_| SketchShapeError::TooLarge)?;
        let width_usize = usize::try_from(width).map_err(|_| SketchShapeError::TooLarge)?;
        let cells = depth_usize
            .checked_mul(width_usize)
            .ok_or(SketchShapeError::TooLarge)?;
        Ok(Self {
            depth,
            width,
            counters: vec![0; cells],
            count: 0,
            default_value: 0,
        })
    }

    /// Returns the number of hash rows.
    pub const fn depth(&self) -> u32 {
        self.depth
    }

    /// Returns the number of counters in each row.
    pub const fn width(&self) -> u32 {
        self.width
    }

    /// Returns the total inserted count, excluding any TopN owned by a caller.
    pub const fn total_count(&self) -> u64 {
        self.count
    }

    /// Returns the fallback value used by the source noise boundary.
    pub const fn default_value(&self) -> u64 {
        self.default_value
    }

    /// Replaces the fallback value without introducing sample/statistics
    /// policy.  The full analyze-time calculation belongs above this leaf.
    pub const fn set_default_value(&mut self, value: u64) {
        self.default_value = value;
    }

    /// Calculates the source analyze fallback as `count / max(1, ndv)`.
    pub fn calc_default_value_for_analyze(&mut self, ndv: u64) {
        self.default_value = self.count / ndv.max(1);
    }

    /// Returns the source's approximate memory size for the counter arena.
    #[must_use]
    pub fn memory_usage(&self) -> u64 {
        (self.depth as u64)
            .saturating_mul(self.width as u64)
            .saturating_mul(std::mem::size_of::<u32>() as u64)
    }

    /// Returns the counter at a row/column pair for diagnostics and tests.
    #[must_use]
    pub fn counter_at(&self, row: u32, column: u32) -> Option<u32> {
        if row >= self.depth || column >= self.width {
            return None;
        }
        Some(self.counters[row as usize * self.width as usize + column as usize])
    }

    /// Sets one counter for snapshot fixtures and storage-boundary adapters.
    pub fn set_counter_at(&mut self, row: u32, column: u32, value: u32) -> bool {
        if row >= self.depth || column >= self.width {
            return false;
        }
        self.counters[row as usize * self.width as usize + column as usize] = value;
        true
    }

    /// Sets the aggregate count reconstructed from a storage snapshot.
    pub const fn set_total_count(&mut self, count: u64) {
        self.count = count;
    }

    /// Computes the source bucket index for one row and hash pair.
    #[must_use]
    pub fn bucket_index(&self, row: u32, hash: Hash128) -> u32 {
        assert!(row < self.depth, "CMSketch row is out of range");
        hash.h1
            .wrapping_add(hash.h2.wrapping_mul(row as u64))
            .wrapping_rem(self.width as u64) as u32
    }

    /// Adds one encoded value to the sketch.
    pub fn insert_bytes(&mut self, bytes: &[u8]) {
        self.insert_bytes_by_count(bytes, 1);
    }

    /// Adds an encoded value by count, preserving Go's counter truncation and
    /// wrapping semantics.
    pub fn insert_bytes_by_count(&mut self, bytes: &[u8], count: u64) {
        self.insert_hashed(hash_bytes(bytes), count);
    }

    /// Adds a value when the caller already owns its Murmur3 hash pair.
    pub fn insert_hashed(&mut self, hash: Hash128, count: u64) {
        self.count = self.count.wrapping_add(count);
        for row in 0..self.depth {
            let index = self.bucket_index(row, hash);
            let offset = row as usize * self.width as usize + index as usize;
            self.counters[offset] = self.counters[offset].wrapping_add(count as u32);
        }
    }

    /// Removes a hashed value by count using Go's unsigned wrapping
    /// subtraction.  The caller must preserve the source's no-underflow
    /// invariant when using this during real statistics maintenance.
    pub fn sub_hashed(&mut self, hash: Hash128, count: u64) {
        self.count = self.count.wrapping_sub(count);
        for row in 0..self.depth {
            let index = self.bucket_index(row, hash);
            let offset = row as usize * self.width as usize + index as usize;
            self.counters[offset] = self.counters[offset].wrapping_sub(count as u32);
        }
    }

    /// Queries an encoded value through the source count/noise boundary.
    #[must_use]
    pub fn query_bytes(&self, bytes: &[u8]) -> u64 {
        self.query_hashed(hash_bytes(bytes))
    }

    /// Queries an already-hashed value through the source count/noise
    /// boundary.  Datum encoding and TopN lookup are deliberately separate.
    #[must_use]
    pub fn query_hashed(&self, hash: Hash128) -> u64 {
        let mut values = Vec::with_capacity(self.depth as usize);
        let mut minimum = u32::MAX;
        for row in 0..self.depth {
            let value = self
                .counter_at(row, self.bucket_index(row, hash))
                .expect("valid row");
            minimum = minimum.min(value);
            let noise = self.count.wrapping_sub(u64::from(value)) / (u64::from(self.width) - 1);
            let normalized = if value == 0 {
                0
            } else if u64::from(value) < noise {
                1
            } else {
                value.wrapping_sub(noise as u32).wrapping_add(1)
            };
            values.push(normalized);
        }
        values.sort_unstable();
        let lower_index = (self.depth as usize - 1) / 2;
        let upper_index = self.depth as usize / 2;
        let median = values[lower_index]
            .wrapping_add(values[upper_index].wrapping_sub(values[lower_index]) / 2);
        let result = median.min(minimum.wrapping_add(1));
        if result == 0 {
            return 0;
        }
        let result = u64::from(result - 1);
        if self.consider_default(result) {
            self.default_value
        } else {
            result
        }
    }

    /// Queries TopN first and falls back to the sketch when no TopN entry
    /// matches.  This is the byte-level replacement for Go's `QueryValue`
    /// after the caller has performed Datum encoding.
    #[must_use]
    pub fn query_with_topn(&self, topn: Option<&TopN>, bytes: &[u8]) -> u64 {
        topn.and_then(|topn| topn.query_bytes(bytes))
            .unwrap_or_else(|| self.query_bytes(bytes))
    }

    /// Runs Go `QueryValue` for the currently dependency-closed integer Datum
    /// domain: EncodeValue, TopN lookup, then CMSketch fallback.
    pub fn query_integer_datum(
        &self,
        topn: Option<&TopN>,
        value: &Datum,
    ) -> Result<u64, DatumEncodingError> {
        let encoded = encode_integer_datum_value(value)?;
        Ok(self.query_with_topn(topn, &encoded))
    }

    fn consider_default(&self, count: u64) -> bool {
        self.default_value > 0
            && (count == 0
                || (count > self.default_value
                    && count < self.count.wrapping_div(self.width as u64).wrapping_mul(2)))
    }

    /// Merges an equally-shaped sketch into this sketch.
    pub fn merge(&mut self, other: &Self) -> Result<(), MergeError> {
        if self.depth != other.depth || self.width != other.width {
            return Err(MergeError {
                destination_depth: self.depth,
                destination_width: self.width,
                source_depth: other.depth,
                source_width: other.width,
            });
        }
        self.count = self.count.wrapping_add(other.count);
        for (destination, source) in self.counters.iter_mut().zip(&other.counters) {
            *destination = destination.wrapping_add(*source);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct TopNBuildHelper {
    sorted: Vec<TopNEntry>,
    sample_size: u64,
    singleton_items: u64,
    sum_top_n: u64,
    actual_num_top: u32,
}

fn new_topn_helper(
    sample: &[Vec<u8>],
    num_top: u32,
    stabilize_equal_counts: bool,
) -> TopNBuildHelper {
    let mut counts: HashMap<Vec<u8>, u64> = HashMap::with_capacity(sample.len());
    for value in sample {
        let count = counts.entry(value.clone()).or_default();
        *count = count.wrapping_add(1);
    }
    let singleton_items = counts.values().filter(|&&count| count == 1).count() as u64;
    let mut sorted: Vec<_> = counts
        .into_iter()
        .map(|(encoded, count)| TopNEntry { encoded, count })
        .collect();
    // The source default is a stable count-only sort. Its optional
    // StabilizeV1AnalyzeTopN failpoint adds encoded-byte ordering for ties.
    sorted.sort_by_key(|entry| std::cmp::Reverse(entry.count));
    if stabilize_equal_counts {
        sorted.sort_by(topn_meta_compare);
    }
    let sample_ndv = sorted.len() as u32;
    let num_top = num_top.min(sample_ndv);
    let mut actual_num_top = 0_u32;
    let mut sum_top_n = 0_u64;
    while actual_num_top < sample_ndv && actual_num_top < num_top.wrapping_mul(2) {
        let index = actual_num_top as usize;
        if num_top > 0
            && actual_num_top >= num_top
            && sorted[index].count.wrapping_mul(3)
                < sorted[(num_top - 1) as usize].count.wrapping_mul(2)
        {
            break;
        }
        if sorted[index].count == 1 {
            break;
        }
        sum_top_n = sum_top_n.wrapping_add(sorted[index].count);
        actual_num_top += 1;
    }
    TopNBuildHelper {
        sorted,
        sample_size: sample.len() as u64,
        singleton_items,
        sum_top_n,
        actual_num_top,
    }
}

fn calculate_default_value(
    helper: &TopNBuildHelper,
    estimate_ndv: u64,
    scale_ratio: u64,
    row_count: u64,
) -> u64 {
    let sample_ndv = helper.sorted.len() as u64;
    let sampled_non_singletons = helper
        .sample_size
        .wrapping_sub(helper.singleton_items)
        .wrapping_mul(scale_ratio);
    if row_count <= sampled_non_singletons {
        return 1;
    }
    row_count.wrapping_sub(sampled_non_singletons)
        / estimate_ndv
            .wrapping_sub(sample_ndv)
            .wrapping_add(helper.singleton_items)
            .max(1)
}

fn build_cms_and_topn(
    helper: &TopNBuildHelper,
    depth: u32,
    width: u32,
    scale_ratio: u64,
    default_value: u64,
) -> (CmsSketch, Option<TopN>) {
    let mut sketch = CmsSketch::new(depth, width);
    let enable_topn = helper.sample_size / 10 <= helper.sum_top_n;
    let mut topn = None;
    let first_non_top = if enable_topn {
        let mut built = TopN::new(helper.actual_num_top as usize);
        for entry in helper.sorted.iter().take(helper.actual_num_top as usize) {
            built.append(&entry.encoded, entry.count.wrapping_mul(scale_ratio));
        }
        built.sort();
        topn = Some(built);
        helper.actual_num_top as usize
    } else {
        0
    };
    sketch.default_value = default_value;
    for entry in helper.sorted.iter().skip(first_non_top) {
        let row_count = if entry.count > 1 {
            entry.count.wrapping_mul(scale_ratio)
        } else {
            default_value
        };
        sketch.insert_bytes_by_count(&entry.encoded, row_count);
    }
    (sketch, topn)
}

/// Builds the sampled sketch/TopN pair and returns NDV and scale ratio.
#[must_use]
pub fn new_cmsketch_and_topn(
    depth: u32,
    width: u32,
    sample: &[Vec<u8>],
    num_top: u32,
    row_count: u64,
) -> Option<(CmsSketch, Option<TopN>, u64, u64)> {
    new_cmsketch_and_topn_with_tie_stabilization(depth, width, sample, num_top, row_count, false)
}

/// Builds a sampled sketch while explicitly controlling the source's
/// `StabilizeV1AnalyzeTopN` behavior.
///
/// Production callers should normally use [`new_cmsketch_and_topn`]. This
/// entry point exists for analyze-v1 stabilization and exact source tests.
#[must_use]
pub fn new_cmsketch_and_topn_with_tie_stabilization(
    depth: u32,
    width: u32,
    sample: &[Vec<u8>],
    num_top: u32,
    row_count: u64,
    stabilize_equal_counts: bool,
) -> Option<(CmsSketch, Option<TopN>, u64, u64)> {
    if row_count == 0 || sample.is_empty() {
        return None;
    }
    let helper = new_topn_helper(sample, num_top, stabilize_equal_counts);
    let row_count = row_count.max(sample.len() as u64);
    let (estimate_ndv, scale_ratio) = calculate_estimate_ndv(
        helper.sample_size,
        helper.sorted.len() as u64,
        helper.singleton_items,
        row_count,
    );
    let default_value = calculate_default_value(&helper, estimate_ndv, scale_ratio, row_count);
    let (sketch, topn) = build_cms_and_topn(&helper, depth, width, scale_ratio, default_value);
    Some((sketch, topn, estimate_ndv, scale_ratio))
}

/// One encoded TopN value and its estimated row count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopNEntry {
    /// Source-encoded value bytes.
    pub encoded: Vec<u8>,
    /// Estimated count for the value.
    pub count: u64,
}

/// Sorted TopN lookup metadata at the encoded-byte boundary.
///
/// Call [`TopN::sort`] after appending entries before using binary-search
/// lookup.  Sample extraction, TopN selection thresholds, persistence, and
/// histogram ownership remain outside this type.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TopN {
    entries: Vec<TopNEntry>,
}

impl TopN {
    /// Creates an empty TopN with the requested allocation hint.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Adds an entry, copying its encoded bytes.
    pub fn append(&mut self, encoded: &[u8], count: u64) {
        self.entries.push(TopNEntry {
            encoded: encoded.to_vec(),
            count,
        });
    }

    /// Number of entries (the source calls this `Num` because Histogram owns
    /// the conventional `Len` name).
    #[must_use]
    pub fn num(&self) -> usize {
        self.entries.len()
    }

    /// Returns a source-shaped debug representation without requiring Datum
    /// decoding at this boundary.
    #[must_use]
    pub fn display_string(&self) -> String {
        let entries = self
            .entries
            .iter()
            .map(|entry| format!("({:?}, {})", entry.encoded, entry.count))
            .collect::<Vec<_>>()
            .join(", ");
        format!("TopN{{length: {}, [{}]}}", self.entries.len(), entries)
    }

    /// Sorts entries using the source bytewise ordering.
    pub fn sort(&mut self) {
        self.entries
            .sort_unstable_by(|left, right| left.encoded.cmp(&right.encoded));
    }

    /// Returns the sorted entries for metadata inspection.
    #[must_use]
    pub fn entries(&self) -> &[TopNEntry] {
        &self.entries
    }

    /// Returns the index of an exact entry, or `None` if absent.
    #[must_use]
    pub fn find(&self, encoded: &[u8]) -> Option<usize> {
        self.entries
            .binary_search_by(|entry| entry.encoded.as_slice().cmp(encoded))
            .ok()
    }

    /// Returns the exact TopN count, or `None` when the encoded value is not
    /// represented by TopN.
    #[must_use]
    pub fn query_bytes(&self, encoded: &[u8]) -> Option<u64> {
        self.find(encoded).map(|index| self.entries[index].count)
    }

    /// Returns the half-open interval count for encoded bytes `[left, right)`.
    #[must_use]
    pub fn between_count(&self, left: &[u8], right: &[u8]) -> u64 {
        let left_index = self.lower_bound(left);
        let right_index = self.lower_bound(right);
        if left_index >= right_index {
            return 0;
        }
        self.entries[left_index..right_index]
            .iter()
            .map(|entry| entry.count)
            .sum()
    }

    /// Returns the first index whose encoded value is not less than `encoded`.
    #[must_use]
    pub fn lower_bound(&self, encoded: &[u8]) -> usize {
        self.entries
            .binary_search_by(|entry| entry.encoded.as_slice().cmp(encoded))
            .unwrap_or_else(|index| index)
    }

    /// Returns the sum of all TopN counts.
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.entries
            .iter()
            .fold(0_u64, |total, entry| total.wrapping_add(entry.count))
    }

    /// Returns the smallest TopN count, or zero for an empty list.
    #[must_use]
    pub fn min_count(&self) -> u64 {
        self.entries
            .iter()
            .map(|entry| entry.count)
            .min()
            .unwrap_or(0)
    }

    /// Returns the source's approximate TopN memory footprint.
    #[must_use]
    pub fn memory_usage(&self) -> u64 {
        32_u64.saturating_add(self.entries.iter().fold(0_u64, |sum, entry| {
            sum.saturating_add(32)
                .saturating_add(entry.encoded.capacity() as u64)
        }))
    }

    /// Equality follows Go's TopN contract: two empty lists compare equal,
    /// otherwise both encoded order and counts must match.
    #[must_use]
    pub fn equal(&self, other: Option<&TopN>) -> bool {
        let Some(other) = other else {
            return self.entries.is_empty();
        };
        self.total_count() == other.total_count() && self.entries == other.entries
    }
}

/// Orders TopN metadata by count descending and encoded bytes ascending.
#[must_use]
pub fn topn_meta_compare(left: &TopNEntry, right: &TopNEntry) -> Ordering {
    right
        .count
        .cmp(&left.count)
        .then_with(|| left.encoded.cmp(&right.encoded))
}

/// Sorts ranking metadata in the source order.
pub fn sort_topn_meta(entries: &mut [TopNEntry]) {
    entries.sort_unstable_by(topn_meta_compare);
}

/// Splits ranked metadata into a byte-sorted TopN and ranked remainder.
#[must_use]
pub fn get_merged_topn_from_sorted_slice(
    mut sorted: Vec<TopNEntry>,
    n: u32,
) -> (Option<TopN>, Vec<TopNEntry>) {
    sort_topn_meta(&mut sorted);
    let split = (n as usize).min(sorted.len());
    let remainder = sorted.split_off(split);
    let selected = sorted;
    let mut top_n = TopN::new(selected.len());
    for entry in selected {
        top_n.append(&entry.encoded, entry.count);
    }
    top_n.sort();
    // Go returns a non-nil empty TopN when `n == 0`; callers rely on that to
    // clear the destination before spilling all entries into CMSketch.
    (Some(top_n), remainder)
}

/// Returns true when every input TopN is empty (including a missing TopN).
#[must_use]
pub fn check_empty_topns(topns: &[Option<&TopN>]) -> bool {
    topns
        .iter()
        .all(|topn| topn.is_none_or(|topn| topn.total_count() == 0))
}

/// Merges equal encoded values from multiple TopN lists and keeps the top `n`.
#[must_use]
pub fn merge_topn(topns: &[Option<&TopN>], n: u32) -> (Option<TopN>, Vec<TopNEntry>) {
    if check_empty_topns(topns) {
        return (None, Vec::new());
    }
    let mut counts: HashMap<Vec<u8>, u64> = HashMap::new();
    for topn in topns.iter().flatten() {
        if topn.total_count() == 0 {
            continue;
        }
        for entry in topn.entries() {
            let count = counts.entry(entry.encoded.clone()).or_default();
            *count = count.wrapping_add(entry.count);
        }
    }
    let ranked = counts
        .into_iter()
        .map(|(encoded, count)| TopNEntry { encoded, count })
        .collect();
    get_merged_topn_from_sorted_slice(ranked, n)
}

/// Merges TopN values and spills entries beyond `n` into the sketch.
pub fn merge_topn_and_update_cmsketch(
    destination: &mut TopN,
    source: &TopN,
    sketch: &mut CmsSketch,
    n: u32,
) -> Vec<TopNEntry> {
    let (merged, remainder) = merge_topn(&[Some(source), Some(destination)], n);
    if let Some(merged) = merged {
        *destination = merged;
    }
    for entry in &remainder {
        sketch.insert_bytes_by_count(&entry.encoded, entry.count);
    }
    remainder
}

/// Errors returned by the source-compatible CMSketch protobuf wire codec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    /// A field or nested message ended before its declared length.
    UnexpectedEof,
    /// A varint exceeded the protobuf u64 limit.
    VarintOverflow,
    /// A wire tag used an unsupported protobuf wire type.
    InvalidWireType(u8),
    /// A row had a different shape from the first row.
    InconsistentRowShape,
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => formatter.write_str("truncated CMSketch protobuf"),
            Self::VarintOverflow => formatter.write_str("CMSketch protobuf varint overflow"),
            Self::InvalidWireType(wire) => write!(formatter, "invalid CMSketch wire type {wire}"),
            Self::InconsistentRowShape => formatter.write_str("inconsistent CMSketch row shape"),
        }
    }
}

impl std::error::Error for CodecError {}

fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn encode_message_field(field: u8, message: &[u8], output: &mut Vec<u8>) {
    output.push((field << 3) | 2);
    encode_varint(message.len() as u64, output);
    output.extend_from_slice(message);
}

/// Encodes a CMSketch without TopN using the tipb `CMSketch` wire layout.
///
/// `default_value` is emitted even when zero because the generated Go
/// protobuf marshaler treats this gogoproto non-nullable scalar as present.
pub fn encode_cmsketch_without_topn(
    sketch: Option<&CmsSketch>,
) -> Result<Option<Vec<u8>>, CodecError> {
    let Some(sketch) = sketch else {
        return Ok(None);
    };
    Ok(Some(encode_cmsketch_proto(sketch, None)))
}

/// Encodes a CMSketch and its embedded TopN entries using the full tipb
/// message layout.  This is the in-memory counterpart of Go's
/// `CMSketchToProto`; callers that persist TopN in a separate column should
/// continue using [`encode_cmsketch_without_topn`].
pub fn encode_cmsketch_and_topn(
    sketch: Option<&CmsSketch>,
    topn: Option<&TopN>,
) -> Option<Vec<u8>> {
    match sketch {
        Some(sketch) => Some(encode_cmsketch_proto(sketch, topn)),
        // CMSketchToProto always returns a non-nil message, even when both
        // source pointers are nil. The generated gogoprotobuf marshaler still
        // emits its non-nullable default_value field as `[0x18, 0x00]`.
        None => Some(encode_cmsketch_proto_without_sketch(topn)),
    }
}

fn encode_cmsketch_proto(sketch: &CmsSketch, topn: Option<&TopN>) -> Vec<u8> {
    let mut output = Vec::new();
    for row in 0..sketch.depth {
        let mut encoded_row = Vec::with_capacity(sketch.width as usize * 6);
        for column in 0..sketch.width {
            encoded_row.push(0x08); // CMSketchRow.counters = 1, varint.
            encode_varint(
                u64::from(sketch.counters[row as usize * sketch.width as usize + column as usize]),
                &mut encoded_row,
            );
        }
        encode_message_field(1, &encoded_row, &mut output);
    }
    if let Some(topn) = topn {
        for entry in topn.entries() {
            let mut encoded_entry = Vec::new();
            encoded_entry.push(0x0a); // CMSketchTopN.data = 1, bytes.
            encode_varint(entry.encoded.len() as u64, &mut encoded_entry);
            encoded_entry.extend_from_slice(&entry.encoded);
            encoded_entry.push(0x10); // CMSketchTopN.count = 2, varint.
            encode_varint(entry.count, &mut encoded_entry);
            encode_message_field(2, &encoded_entry, &mut output);
        }
    }
    output.push(0x18); // CMSketch.default_value = 3, varint.
    encode_varint(sketch.default_value, &mut output);
    output
}

fn encode_cmsketch_proto_without_sketch(topn: Option<&TopN>) -> Vec<u8> {
    let mut output = Vec::new();
    if let Some(topn) = topn {
        for entry in topn.entries() {
            let mut encoded_entry = Vec::new();
            encoded_entry.push(0x0a);
            encode_varint(entry.encoded.len() as u64, &mut encoded_entry);
            encoded_entry.extend_from_slice(&entry.encoded);
            encoded_entry.push(0x10);
            encode_varint(entry.count, &mut encoded_entry);
            encode_message_field(2, &encoded_entry, &mut output);
        }
    }
    output.push(0x18);
    output.push(0);
    output
}

fn read_varint(input: &[u8], cursor: &mut usize) -> Result<u64, CodecError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *input.get(*cursor).ok_or(CodecError::UnexpectedEof)?;
        *cursor += 1;
        if shift >= 64 && byte & 0x7f != 0 {
            return Err(CodecError::VarintOverflow);
        }
        value |= u64::from(byte & 0x7f) << shift.min(63);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(CodecError::VarintOverflow)
}

fn read_bytes<'a>(input: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], CodecError> {
    let length =
        usize::try_from(read_varint(input, cursor)?).map_err(|_| CodecError::VarintOverflow)?;
    let end = cursor
        .checked_add(length)
        .ok_or(CodecError::UnexpectedEof)?;
    let bytes = input.get(*cursor..end).ok_or(CodecError::UnexpectedEof)?;
    *cursor = end;
    Ok(bytes)
}

fn skip_field(input: &[u8], cursor: &mut usize, wire: u8) -> Result<(), CodecError> {
    match wire {
        0 => {
            read_varint(input, cursor)?;
            Ok(())
        }
        1 => {
            *cursor = cursor.checked_add(8).ok_or(CodecError::UnexpectedEof)?;
            if *cursor > input.len() {
                Err(CodecError::UnexpectedEof)
            } else {
                Ok(())
            }
        }
        2 => {
            read_bytes(input, cursor)?;
            Ok(())
        }
        5 => {
            *cursor = cursor.checked_add(4).ok_or(CodecError::UnexpectedEof)?;
            if *cursor > input.len() {
                Err(CodecError::UnexpectedEof)
            } else {
                Ok(())
            }
        }
        wire => Err(CodecError::InvalidWireType(wire)),
    }
}

fn decode_row(input: &[u8]) -> Result<Vec<u32>, CodecError> {
    let mut counters = Vec::new();
    let mut cursor = 0;
    while cursor < input.len() {
        let tag = read_varint(input, &mut cursor)?;
        let field = (tag >> 3) as u32;
        let wire = (tag & 7) as u8;
        match (field, wire) {
            (1, 0) => counters.push(read_varint(input, &mut cursor)? as u32),
            (1, 2) => {
                // Although the generated tipb marshaler currently emits one
                // unpacked varint per counter, protobuf permits packed
                // repeated scalars and the authoritative Go unmarshaler
                // accepts both forms.
                let packed = read_bytes(input, &mut cursor)?;
                let mut packed_cursor = 0;
                while packed_cursor < packed.len() {
                    counters.push(read_varint(packed, &mut packed_cursor)? as u32);
                }
            }
            _ => skip_field(input, &mut cursor, wire)?,
        }
    }
    Ok(counters)
}

fn decode_topn_entry(input: &[u8]) -> Result<TopNEntry, CodecError> {
    let mut encoded = Vec::new();
    let mut count = 0_u64;
    let mut cursor = 0;
    while cursor < input.len() {
        let tag = read_varint(input, &mut cursor)?;
        let field = (tag >> 3) as u32;
        let wire = (tag & 7) as u8;
        match (field, wire) {
            (1, 2) => encoded = read_bytes(input, &mut cursor)?.to_vec(),
            (2, 0) => count = read_varint(input, &mut cursor)?,
            _ => skip_field(input, &mut cursor, wire)?,
        }
    }
    Ok(TopNEntry { encoded, count })
}

type DecodedSketch = (Vec<Vec<u32>>, Vec<TopNEntry>, u64);

fn decode_proto(input: &[u8]) -> Result<DecodedSketch, CodecError> {
    let mut rows = Vec::new();
    let mut topn = Vec::new();
    let mut default_value = 0_u64;
    let mut cursor = 0;
    while cursor < input.len() {
        let tag = read_varint(input, &mut cursor)?;
        let field = (tag >> 3) as u32;
        let wire = (tag & 7) as u8;
        match (field, wire) {
            (1, 2) => rows.push(decode_row(read_bytes(input, &mut cursor)?)?),
            (2, 2) => topn.push(decode_topn_entry(read_bytes(input, &mut cursor)?)?),
            (3, 0) => default_value = read_varint(input, &mut cursor)?,
            _ => skip_field(input, &mut cursor, wire)?,
        }
    }
    Ok((rows, topn, default_value))
}

fn sketch_from_rows(
    rows: Vec<Vec<u32>>,
    default_value: u64,
) -> Result<Option<CmsSketch>, CodecError> {
    let Some(first) = rows.first() else {
        return Ok(None);
    };
    let depth = rows.len() as u32;
    let width = first.len() as u32;
    let mut sketch =
        CmsSketch::try_new(depth, width).map_err(|_| CodecError::InconsistentRowShape)?;
    for row in &rows {
        if row.len() > first.len() {
            return Err(CodecError::InconsistentRowShape);
        }
    }
    for (row_index, row) in rows.iter().enumerate() {
        let start = row_index * first.len();
        sketch.counters[start..start + row.len()].copy_from_slice(row);
        // Go resets count once per row and leaves the sum of the final row.
        sketch.count = row
            .iter()
            .fold(0_u64, |sum, &counter| sum.wrapping_add(u64::from(counter)));
    }
    sketch.default_value = default_value;
    Ok(Some(sketch))
}

/// Decodes a CMSketch snapshot. Empty input is the Go `nil` result.
pub fn decode_cmsketch(data: &[u8]) -> Result<Option<CmsSketch>, CodecError> {
    if data.is_empty() {
        return Ok(None);
    }
    let (rows, _, default_value) = decode_proto(data)?;
    sketch_from_rows(rows, default_value)
}

/// Decodes the complete tipb CMSketch message, including embedded TopN.
///
/// This mirrors Go's `CMSketchAndTopNFromProto`; the persistence-oriented
/// [`decode_cmsketch_and_topn`] below continues loading TopN from separate
/// storage rows.
pub fn decode_cmsketch_and_embedded_topn(
    data: &[u8],
) -> Result<(Option<CmsSketch>, Option<TopN>), CodecError> {
    if data.is_empty() {
        return Ok((None, None));
    }
    let (rows, entries, default_value) = decode_proto(data)?;
    let sketch = sketch_from_rows(rows, default_value)?;
    let topn = if entries.is_empty() {
        None
    } else {
        let mut topn = TopN::new(entries.len());
        for entry in entries {
            topn.append(&entry.encoded, entry.count);
        }
        topn.sort();
        Some(topn)
    };
    Ok((sketch, topn))
}

/// Decodes TopN rows supplied by the caller's storage layer.
#[must_use]
pub fn decode_topn_rows(rows: &[(Vec<u8>, u64)]) -> Option<TopN> {
    if rows.is_empty() {
        return None;
    }
    let mut topn = TopN::new(rows.len());
    for (encoded, count) in rows {
        topn.append(encoded, *count);
    }
    topn.sort();
    Some(topn)
}

/// Decodes a CMSketch plus separately stored TopN rows.
pub fn decode_cmsketch_and_topn(
    data: Option<&[u8]>,
    topn_rows: &[(Vec<u8>, u64)],
) -> Result<(Option<CmsSketch>, Option<TopN>), CodecError> {
    if data.is_none() && topn_rows.is_empty() {
        return Ok((None, None));
    }
    let sketch = match data {
        None | Some([]) => None,
        Some(data) => decode_cmsketch(data)?,
    };
    Ok((sketch, decode_topn_rows(topn_rows)))
}
