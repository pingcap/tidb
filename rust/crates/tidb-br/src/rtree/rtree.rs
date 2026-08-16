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

//! Go `br/pkg/rtree/rtree.go`: the range trees themselves.

use std::collections::BTreeMap;

use tidb_codec::table_key::{decode_key_head, KeyHead};

/// Go `rtree.KeyRange`: an origin key range.
///
/// This is its own Go type, distinct from `kv.KeyRange`, so it stays its own
/// type here too.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyRange {
    /// Inclusive lower bound.
    pub start_key: Vec<u8>,
    /// Exclusive upper bound; empty means "no upper bound".
    pub end_key: Vec<u8>,
}

impl KeyRange {
    /// Builds a range from two byte sequences.
    #[must_use]
    pub fn new(start_key: impl Into<Vec<u8>>, end_key: impl Into<Vec<u8>>) -> Self {
        Self {
            start_key: start_key.into(),
            end_key: end_key.into(),
        }
    }

    /// Go `(*KeyRange).Contains`: whether `[start, end)` contains `key`.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start_key.as_slice()
            && (self.end_key.is_empty() || key < self.end_key.as_slice())
    }

    /// Go `(*KeyRange).ContainsRange`: whether this range contains a region's
    /// key range.
    #[must_use]
    pub fn contains_range(&self, start_key: &[u8], end_key: &[u8]) -> bool {
        start_key >= self.start_key.as_slice()
            && (self.end_key.is_empty() || end_key <= self.end_key.as_slice())
    }

    /// Go `(*KeyRange).Intersect`: the intersection with `[start, end)`.
    ///
    /// Returns `None` when the two do not intersect, mirroring Go's
    /// `isIntersect == false` (in which case Go also leaves both returned
    /// slices `nil`).
    #[must_use]
    pub fn intersect(&self, start: &[u8], end: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        // An empty end key means the maximum end key.
        if !self.end_key.is_empty() && start >= self.end_key.as_slice() {
            return None;
        }
        if !end.is_empty() && end <= self.start_key.as_slice() {
            return None;
        }
        let sub_start = if start >= self.start_key.as_slice() {
            start.to_vec()
        } else {
            self.start_key.clone()
        };
        let sub_end = if end.is_empty() {
            self.end_key.clone()
        } else if self.end_key.is_empty() || end < self.end_key.as_slice() {
            end.to_vec()
        } else {
            self.end_key.clone()
        };
        Some((sub_start, sub_end))
    }
}

/// The payload contract of a [`Range`].
///
/// KEY NARROWING: Go carries `[]*backuppb.File` here. Everything this package
/// does with a file is read these three scalars, so the payload stays an
/// opaque generic and no protobuf dependency is needed. See
/// [`crate::restore_utils::File`] for the concrete declaration.
pub trait RangeFile: Clone {
    /// Go `File.GetTotalKvs`.
    fn total_kvs(&self) -> u64;
    /// Go `File.GetTotalBytes`.
    fn total_bytes(&self) -> u64;
    /// Go `File.GetCrc64Xor`.
    fn crc64_xor(&self) -> u64;
}

/// Go `rtree.Range`: a backup response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Range<F> {
    /// Go's embedded `KeyRange`.
    pub key_range: KeyRange,
    /// Go `Range.Files`.
    pub files: Vec<F>,
}

impl<F> Range<F> {
    /// Builds a range with no payload.
    #[must_use]
    pub fn new(start_key: impl Into<Vec<u8>>, end_key: impl Into<Vec<u8>>) -> Self {
        Self {
            key_range: KeyRange::new(start_key, end_key),
            files: Vec::new(),
        }
    }

    /// Builds a range with a payload.
    #[must_use]
    pub fn with_files(
        start_key: impl Into<Vec<u8>>,
        end_key: impl Into<Vec<u8>>,
        files: Vec<F>,
    ) -> Self {
        Self {
            key_range: KeyRange::new(start_key, end_key),
            files,
        }
    }

    /// Go's promoted `Range.StartKey`.
    #[must_use]
    pub fn start_key(&self) -> &[u8] {
        &self.key_range.start_key
    }

    /// Go's promoted `Range.EndKey`.
    #[must_use]
    pub fn end_key(&self) -> &[u8] {
        &self.key_range.end_key
    }
}

impl<F: RangeFile> Range<F> {
    /// Go `(*Range).BytesAndKeys`: total bytes and keys in a range.
    #[must_use]
    pub fn bytes_and_keys(&self) -> (u64, u64) {
        let mut bytes = 0u64;
        let mut keys = 0u64;
        for file in &self.files {
            bytes += file.total_bytes();
            keys += file.total_kvs();
        }
        (bytes, keys)
    }
}

/// Go `rtree.RangeStats`: a restore merge result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeStats<F> {
    /// Go's embedded `Range`.
    pub range: Range<F>,
    /// Go `RangeStats.Size`.
    pub size: u64,
    /// Go `RangeStats.Count`.
    pub count: u64,
}

impl<F> RangeStats<F> {
    /// Go's promoted `RangeStats.StartKey`.
    #[must_use]
    pub fn start_key(&self) -> &[u8] {
        self.range.start_key()
    }

    /// Go's promoted `RangeStats.EndKey`.
    #[must_use]
    pub fn end_key(&self) -> &[u8] {
        self.range.end_key()
    }

    /// Go's promoted `RangeStats.Files`.
    #[must_use]
    pub fn files(&self) -> &[F] {
        &self.range.files
    }
}

/// Go `rtree.RangeStatsTree`.
#[derive(Clone, Debug)]
pub struct RangeStatsTree<F> {
    tree: BTreeMap<Vec<u8>, RangeStats<F>>,
}

impl<F> Default for RangeStatsTree<F> {
    fn default() -> Self {
        Self {
            tree: BTreeMap::new(),
        }
    }
}

impl<F: RangeFile> RangeStatsTree<F> {
    /// Go `NewRangeStatsTree`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
        }
    }

    /// Go's promoted `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Whether the tree holds no ranges.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Go `(*RangeStatsTree).InsertRange`: returns the displaced range when the
    /// insert overlapped an existing one.
    pub fn insert_range(
        &mut self,
        rg: &Range<F>,
        range_size: u64,
        range_count: u64,
    ) -> Option<RangeStats<F>> {
        self.tree.insert(
            rg.key_range.start_key.clone(),
            RangeStats {
                range: rg.clone(),
                size: range_size,
                count: range_count,
            },
        )
    }

    /// Go `(*RangeStatsTree).MergedRanges`: the sorted ranges after merging
    /// according to `split_size_bytes` and `split_key_count`.
    #[must_use]
    pub fn merged_ranges(&self, split_size_bytes: u64, split_key_count: u64) -> Vec<RangeStats<F>> {
        let mut merge_target_index: Option<usize> = None;
        let mut sorted_ranges: Vec<RangeStats<F>> = Vec::with_capacity(self.tree.len());
        for rg in self.tree.values() {
            let merge = merge_target_index.is_some_and(|index| {
                needs_merge(&sorted_ranges[index], rg, split_size_bytes, split_key_count)
            });
            if merge {
                // Merge from `rg` into `sorted_ranges[merge_target_index]`.
                let index = merge_target_index.expect("merge implies an initialized target");
                sorted_ranges[index].range.key_range.end_key = rg.range.key_range.end_key.clone();
                sorted_ranges[index].size += rg.size;
                sorted_ranges[index].count += rg.count;
                sorted_ranges[index]
                    .range
                    .files
                    .extend(rg.range.files.iter().cloned());
            } else {
                // Uninitialized, or the target does not need to be merged.
                merge_target_index = Some(merge_target_index.map_or(0, |index| index + 1));
                sorted_ranges.push(rg.clone());
            }
        }
        sorted_ranges
    }
}

/// Go `tikv.DecodeKey(key, kvrpcpb.APIVersion_V2)`, narrowed to the split it
/// performs: an API-V2 key is a four-byte `mode + 3-byte keyspace id` prefix
/// followed by the inner key. `None` is Go's error return, which
/// [`needs_merge`] treats as "not a V2 (keyspaced) key".
fn decode_keyspace_key(key: &[u8]) -> Option<&[u8]> {
    const KEYSPACE_PREFIX_LEN: usize = 4;
    const TXN_MODE_PREFIX: u8 = b'x';
    const RAW_MODE_PREFIX: u8 = b'r';
    if key.len() < KEYSPACE_PREFIX_LEN {
        return None;
    }
    if key[0] != TXN_MODE_PREFIX && key[0] != RAW_MODE_PREFIX {
        return None;
    }
    Some(&key[KEYSPACE_PREFIX_LEN..])
}

/// Go `NeedsMerge`: whether two adjacent ranges may be fused.
#[must_use]
pub fn needs_merge<F: RangeFile>(
    left: &RangeStats<F>,
    right: &RangeStats<F>,
    split_size_bytes: u64,
    split_key_count: u64,
) -> bool {
    let (left_bytes, left_keys) = left.range.bytes_and_keys();
    let (right_bytes, right_keys) = right.range.bytes_and_keys();
    if right_bytes == 0 {
        return true;
    }
    if left_bytes + right_bytes > split_size_bytes {
        return false;
    }
    if left_keys + right_keys > split_key_count {
        return false;
    }

    // Trim the keyspace prefix when the key carries one.
    let parse_inner_key = |key: &[u8]| {
        let inner = decode_keyspace_key(key).unwrap_or(key);
        decode_key_head(inner)
    };

    let (Ok(head1), Ok(head2)) = (
        parse_inner_key(left.start_key()),
        parse_inner_key(right.start_key()),
    ) else {
        // Failed to decode the file key head... can this happen? Go logs a
        // warning here and skips the merge.
        return false;
    };

    match (head1, head2) {
        // Merge if they are both record keys, but not across tables.
        (KeyHead::Record { table_id: left_id }, KeyHead::Record { table_id: right_id }) => {
            left_id == right_id
        }
        // If they are all index keys, do not merge ranges in different indexes
        // even in the same table: a rewrite rule only supports rewriting one
        // pattern. Merge left and right if they are in the same index.
        (
            KeyHead::Index {
                table_id: left_id,
                index_id: left_index,
            },
            KeyHead::Index {
                table_id: right_id,
                index_id: right_index,
            },
        ) => left_id == right_id && left_index == right_index,
        _ => false,
    }
}

/// Go `rtree.RangeTree`: a sorted tree of non-overlapping [`Range`]s.
#[derive(Clone, Debug)]
pub struct RangeTree<F> {
    tree: BTreeMap<Vec<u8>, Range<F>>,
    /// Go `RangeTree.PhysicalID`.
    pub physical_id: i64,
}

impl<F> Default for RangeTree<F> {
    fn default() -> Self {
        Self {
            tree: BTreeMap::new(),
            physical_id: 0,
        }
    }
}

impl<F> Default for ProgressRange<F> {
    fn default() -> Self {
        Self {
            res: RangeTree::default(),
            origin: KeyRange::default(),
        }
    }
}

impl<F: RangeFile> RangeTree<F> {
    /// Go `NewRangeTree`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
            physical_id: 0,
        }
    }

    /// Go `NewRangeTreeWithFreeListG`.
    ///
    /// `FreeListG` is a Go allocation-reuse knob with no observable behavior,
    /// so only the physical ID survives the crossing.
    #[must_use]
    pub fn new_with_physical_id(physical_id: i64) -> Self {
        Self {
            tree: BTreeMap::new(),
            physical_id,
        }
    }

    /// Go's promoted `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Whether the tree holds no ranges.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Go's promoted `Get`: the range whose start key equals `start_key`.
    #[must_use]
    pub fn get(&self, start_key: &[u8]) -> Option<&Range<F>> {
        self.tree.get(start_key)
    }

    /// Go's promoted `Ascend`.
    pub fn ascend(&self, mut visit: impl FnMut(&Range<F>) -> bool) {
        for item in self.tree.values() {
            if !visit(item) {
                return;
            }
        }
    }

    /// Go `(*RangeTree).Find`: the item containing the range's start key.
    #[must_use]
    pub fn find(&self, rg: &Range<F>) -> Option<&Range<F>> {
        let ret = self
            .tree
            .range(..=rg.key_range.start_key.clone())
            .next_back()
            .map(|(_, item)| item)?;
        if ret.key_range.contains(&rg.key_range.start_key) {
            Some(ret)
        } else {
            None
        }
    }

    /// Go `(*RangeTree).getOverlaps`.
    ///
    /// Note that `find` gets the last item that is less than or equal to the
    /// range:
    ///
    /// ```text
    /// in the case: |_______a_______|_____b_____|___c___|
    /// new range is     |______d______|
    /// ```
    ///
    /// `find` returns range `a`, and both the start key of `a` and of `b` are
    /// less than the end key of `d`, so both count as overlapping.
    fn get_overlaps(&self, rg: &Range<F>) -> Vec<Vec<u8>> {
        let found = self.find(rg).map_or_else(
            || rg.key_range.start_key.clone(),
            |f| f.start_key().to_vec(),
        );

        let mut overlaps = Vec::new();
        for over in self.tree.range(found..).map(|(_, item)| item) {
            if !rg.key_range.end_key.is_empty() && rg.key_range.end_key <= over.key_range.start_key
            {
                break;
            }
            overlaps.push(over.key_range.start_key.clone());
        }
        overlaps
    }

    /// Go `(*RangeTree).Update`: inserts a range and deletes overlapping ones.
    pub fn update(&mut self, rg: Range<F>) -> bool {
        self.update_force(rg, true)
    }

    /// Go `(*RangeTree).updateForce`.
    fn update_force(&mut self, rg: Range<F>, force: bool) -> bool {
        let overlaps = self.get_overlaps(&rg);
        if !force && !overlaps.is_empty() {
            return false;
        }
        // Range has been backed up; overwrite the overlapping ranges.
        for key in overlaps {
            self.tree.remove(&key);
        }
        self.tree.insert(rg.key_range.start_key.clone(), rg);
        true
    }

    /// Go `(*RangeTree).Put`: forms a range and inserts it into the tree.
    pub fn put(&mut self, start_key: &[u8], end_key: &[u8], files: Vec<F>) {
        self.update_force(Range::with_files(start_key, end_key, files), true);
    }

    /// Go `(*RangeTree).PutForce`.
    pub fn put_force(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
        files: Vec<F>,
        force: bool,
    ) -> bool {
        self.update_force(Range::with_files(start_key, end_key, files), force)
    }

    /// Go `(*RangeTree).InsertRange`: returns the displaced range, if any.
    pub fn insert_range(&mut self, rg: Range<F>) -> Option<Range<F>> {
        self.tree.insert(rg.key_range.start_key.clone(), rg)
    }

    /// Go `(*RangeTree).GetIncompleteRange`: the ranges within
    /// `[start_key, end_key)` that this tree does not yet cover.
    #[must_use]
    pub fn get_incomplete_range(&self, start_key: &[u8], end_key: &[u8]) -> Vec<KeyRange> {
        if !start_key.is_empty() && start_key == end_key {
            return Vec::new();
        }
        // Don't use a large buffer, because it will cause memory issues, and
        // the number of missing ranges is usually small.
        let mut incomplete: Vec<KeyRange> = Vec::with_capacity(1);
        let request_range = KeyRange::new(start_key, end_key);
        let mut last_end_key = start_key.to_vec();
        let mut pivot: Range<F> = Range::new(start_key, Vec::<u8>::new());
        if let Some(first) = self.find(&pivot) {
            pivot.key_range.start_key = first.start_key().to_vec();
        }
        let mut pivot_not_found = true;
        for rg in self.tree.range(pivot.key_range.start_key.clone()..) {
            let rg = rg.1;
            pivot_not_found = false;
            if last_end_key.as_slice() < rg.start_key() {
                if let Some((start, end)) = request_range.intersect(&last_end_key, rg.start_key()) {
                    // There is a gap between the last item and the current one.
                    incomplete.push(KeyRange::new(start, end));
                }
            }
            last_end_key = rg.end_key().to_vec();
            if !(end_key.is_empty() || rg.end_key() < end_key) {
                break;
            }
        }

        // Check whether we need to append the last range.
        if pivot_not_found
            || (last_end_key.as_slice() != end_key
                && !last_end_key.is_empty()
                && (end_key.is_empty() || last_end_key.as_slice() < end_key))
        {
            if let Some((start, end)) = request_range.intersect(&last_end_key, end_key) {
                incomplete.push(KeyRange::new(start, end));
            }
        }
        incomplete
    }
}

/// Go `metautil.ChecksumStats`.
///
/// boundary: a flat three-`uint64` struct in a package whose real subject is
/// object-storage metafile serialization.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChecksumStats {
    /// Go `ChecksumStats.Crc64Xor`.
    pub crc64_xor: u64,
    /// Go `ChecksumStats.TotalKvs`.
    pub total_kvs: u64,
    /// Go `ChecksumStats.TotalBytes`.
    pub total_bytes: u64,
}

/// Go's `metautil.MetaWriter`, narrowed to the one call this package makes:
/// `metaWriter.Send(files, metautil.AppendDataFile)`.
pub trait MetaSink<F> {
    /// Go `(*MetaWriter).Send(files, AppendDataFile)`.
    ///
    /// # Errors
    ///
    /// Returns the error that aborts the enclosing tree walk, exactly as Go's
    /// `rangeAscendErr` does.
    fn send(&mut self, files: &[F]) -> Result<(), RtreeError>;
}

/// The errors `br/pkg/rtree` raises through `errors.Errorf`/`errors.Trace`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RtreeError {
    /// Go `(*ProgressRangeTree).Insert`'s overlapping-range error.
    OverlappingRange(String),
    /// Go `(*ProgressRangeTree).FindContained`'s not-contained error.
    NotContained(String),
    /// An error surfaced by the [`MetaSink`].
    Meta(String),
}

impl std::fmt::Display for RtreeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OverlappingRange(message) | Self::NotContained(message) | Self::Meta(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl std::error::Error for RtreeError {}

/// Go `rtree.ProgressRange`.
#[derive(Clone, Debug)]
pub struct ProgressRange<F> {
    /// Go `ProgressRange.Res`.
    pub res: RangeTree<F>,
    /// Go `ProgressRange.Origin`.
    pub origin: KeyRange,
}

/// Go `rtree.ProgressRangeTree`: a sorted tree of non-overlapping
/// [`ProgressRange`]s.
pub struct ProgressRangeTree<F> {
    tree: BTreeMap<Vec<u8>, ProgressRange<F>>,
    checksum_map: BTreeMap<i64, ChecksumStats>,
    skip_checksum: bool,
    meta_writer: Option<Box<dyn MetaSink<F>>>,
    complete_call_back: Box<dyn FnMut()>,
}

impl<F: RangeFile> ProgressRangeTree<F> {
    /// Go `NewProgressRangeTree`.
    #[must_use]
    pub fn new(meta_writer: Option<Box<dyn MetaSink<F>>>, skip_checksum: bool) -> Self {
        Self {
            tree: BTreeMap::new(),
            checksum_map: BTreeMap::new(),
            skip_checksum,
            meta_writer,
            complete_call_back: Box::new(|| {}),
        }
    }

    /// Go's promoted `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tree.len()
    }

    /// Whether the tree holds no progress ranges.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    /// Go `(*ProgressRangeTree).SetCallBack`.
    pub fn set_call_back(&mut self, callback: impl FnMut() + 'static) {
        self.complete_call_back = Box::new(callback);
    }

    /// Go `(*ProgressRangeTree).GetChecksumMap`.
    #[must_use]
    pub fn get_checksum_map(&self) -> &BTreeMap<i64, ChecksumStats> {
        &self.checksum_map
    }

    /// Go `(*ProgressRangeTree).find`: the item containing the range.
    fn find_key(&self, start_key: &[u8]) -> Option<Vec<u8>> {
        let ret = self
            .tree
            .range(..=start_key.to_vec())
            .next_back()
            .map(|(_, item)| item)?;
        if ret.origin.contains(start_key) {
            Some(ret.origin.start_key.clone())
        } else {
            None
        }
    }

    /// Go `(*ProgressRangeTree).Insert`.
    ///
    /// # Errors
    ///
    /// Returns [`RtreeError::OverlappingRange`] when an existing range already
    /// contains the new range's start key.
    pub fn insert(&mut self, pr: ProgressRange<F>) -> Result<(), RtreeError> {
        if let Some(overlap_key) = self.find_key(&pr.origin.start_key) {
            let overlap = &self.tree[&overlap_key];
            return Err(RtreeError::OverlappingRange(format!(
                "failed to insert the progress range into range tree, because there is a \
                 overlapping range. The insert item start key: {}; The overlapped item start \
                 key: {}, end key: {}.",
                tidb_util::redact::key(&pr.origin.start_key),
                tidb_util::redact::key(&overlap.origin.start_key),
                tidb_util::redact::key(&overlap.origin.end_key),
            )));
        }
        self.tree.insert(pr.origin.start_key.clone(), pr);
        Ok(())
    }

    /// Go `(*ProgressRangeTree).FindContained`: the progress range containing
    /// `[start_key, end_key)`.
    ///
    /// `Ok(None)` is Go's `(nil, nil)` return for "maybe a duplicated
    /// response".
    ///
    /// # Errors
    ///
    /// Returns [`RtreeError::NotContained`] when a progress range was found by
    /// start key but does not contain the whole region.
    pub fn find_contained(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Option<&mut ProgressRange<F>>, RtreeError> {
        let Some(found_key) = self.find_key(start_key) else {
            // Go logs "Cannot find progress range that contains the start key,
            // maybe the duplicated response" and returns no error.
            return Ok(None);
        };

        let ret = &self.tree[&found_key];
        if !ret.origin.contains_range(start_key, end_key) {
            return Err(RtreeError::NotContained(format!(
                "The given region is not contained in the found progress range. The region start \
                 key is {}; The progress range start key is {}, end key is {}.",
                String::from_utf8_lossy(start_key),
                tidb_util::redact::key(&ret.origin.start_key),
                tidb_util::redact::key(&ret.origin.end_key),
            )));
        }

        Ok(self.tree.get_mut(&found_key))
    }

    /// Go `(*ProgressRangeTree).GetIncompleteRanges`.
    ///
    /// # Errors
    ///
    /// Propagates the first [`MetaSink`] failure, as Go propagates
    /// `rangeAscendErr`.
    pub fn get_incomplete_ranges(&mut self) -> Result<Vec<KeyRange>, RtreeError> {
        // About 64 MB of memory if there are one million ranges.
        let mut incomplete_ranges = Vec::with_capacity(self.tree.len());
        // Go's `DeletedRange`: the progress range to drop plus its checksum.
        let mut deleted_ranges: Vec<(Vec<u8>, i64, ChecksumStats)> = Vec::new();

        // Go mutates `rangeTree` fields from inside `rangeTree.Ascend`; Rust
        // lifts the two mutable fields out for the duration of the walk.
        let mut writer = self.meta_writer.take();
        let mut callback = std::mem::replace(&mut self.complete_call_back, Box::new(|| {}));
        let mut ascend_err = None;

        for item in self.tree.values() {
            // NOTE: maybe there is a late response whose range overlaps with an
            // existing item, which may cause the complete range tree to become
            // incomplete. Therefore `item.Complete` is only for statistics.
            let incomplete = item
                .res
                .get_incomplete_range(&item.origin.start_key, &item.origin.end_key);
            if incomplete.is_empty() {
                match collect_range_files(writer.as_deref_mut(), item) {
                    Ok(checksum) => {
                        deleted_ranges.push((
                            item.origin.start_key.clone(),
                            item.res.physical_id,
                            checksum,
                        ));
                        callback();
                    }
                    Err(err) => {
                        ascend_err = Some(err);
                        break;
                    }
                }
            } else {
                incomplete_ranges.extend(incomplete);
            }
        }

        self.meta_writer = writer;
        self.complete_call_back = callback;
        if let Some(err) = ascend_err {
            return Err(err);
        }

        for (start_key, physical_id, checksum) in deleted_ranges {
            self.tree.remove(&start_key);
            if !self.skip_checksum {
                self.update_checksum(
                    physical_id,
                    checksum.crc64_xor,
                    checksum.total_kvs,
                    checksum.total_bytes,
                );
            }
        }
        Ok(incomplete_ranges)
    }

    /// Go `(*ProgressRangeTree).UpdateChecksum`.
    pub fn update_checksum(&mut self, physical_id: i64, crc: u64, kvs: u64, bytes: u64) {
        let ckm = self.checksum_map.entry(physical_id).or_default();
        ckm.crc64_xor ^= crc;
        ckm.total_kvs += kvs;
        ckm.total_bytes += bytes;
    }
}

/// Go `utils.SummaryFiles`: the XOR'd CRC and the summed KV and byte counts.
///
/// Go additionally feeds `br/pkg/summary`'s global collectors here; that is
/// process-wide CLI reporting, not part of this package's contract.
fn summary_files<F: RangeFile>(files: &[F]) -> (u64, u64, u64) {
    let mut crc = 0u64;
    let mut kvs = 0u64;
    let mut bytes = 0u64;
    for f in files {
        crc ^= f.crc64_xor();
        kvs += f.total_kvs();
        bytes += f.total_bytes();
    }
    (crc, kvs, bytes)
}

/// Go `(*ProgressRangeTree).collectRangeFiles`.
fn collect_range_files<F: RangeFile>(
    meta_writer: Option<&mut (dyn MetaSink<F> + 'static)>,
    item: &ProgressRange<F>,
) -> Result<ChecksumStats, RtreeError> {
    let mut checksum = ChecksumStats::default();
    let Some(writer) = meta_writer else {
        return Ok(checksum);
    };
    let mut range_ascend_err = None;
    item.res.ascend(|r| {
        let (crc, kvs, bytes) = summary_files(&r.files);
        if let Err(err) = writer.send(&r.files) {
            range_ascend_err = Some(err);
            return false;
        }
        checksum.crc64_xor ^= crc;
        checksum.total_kvs += kvs;
        checksum.total_bytes += bytes;
        true
    });
    range_ascend_err.map_or(Ok(checksum), Err)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use tidb_codec::table_key::{encode_index_seek_key, encode_record_key, RecordHandle};
    use tidb_codec::{encode_int, gen_table_record_prefix};

    use super::*;

    /// The `backuppb.File` fields the `rtree` tests actually set. The full
    /// declaration lives in [`crate::restore_utils::File`]; these tests only
    /// need a payload that satisfies [`RangeFile`] plus a name to assert on.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct TestFile {
        name: String,
        crc64_xor: u64,
        total_kvs: u64,
        total_bytes: u64,
    }

    impl RangeFile for TestFile {
        fn total_kvs(&self) -> u64 {
            self.total_kvs
        }
        fn total_bytes(&self) -> u64 {
            self.total_bytes
        }
        fn crc64_xor(&self) -> u64 {
            self.crc64_xor
        }
    }

    fn named(name: &str) -> Vec<TestFile> {
        vec![TestFile {
            name: name.to_owned(),
            ..TestFile::default()
        }]
    }

    fn new_range(start: &[u8], end: &[u8]) -> Range<TestFile> {
        Range::new(start, end)
    }

    /// Go `TestRangeTree` (`rtree_test.go`).
    #[test]
    fn range_tree() {
        let mut range_tree: RangeTree<TestFile> = RangeTree::new();
        assert!(range_tree.get(b"").is_none());

        fn assert_incomplete(
            tree: &RangeTree<TestFile>,
            start_key: &[u8],
            end_key: &[u8],
            ranges: &[KeyRange],
        ) {
            let incomplete = tree.get_incomplete_range(start_key, end_key);
            assert_eq!(
                ranges.len(),
                incomplete.len(),
                "{incomplete:?} vs {ranges:?}"
            );
            for (idx, rg) in incomplete.iter().enumerate() {
                assert_eq!(ranges[idx].start_key, rg.start_key, "idx={idx}");
                assert_eq!(ranges[idx].end_key, rg.end_key, "idx={idx}");
            }
        }

        fn assert_all_complete(tree: &RangeTree<TestFile>) {
            for s in 0u16..0xfe {
                for e in (s + 1)..0xff {
                    let start = [u8::try_from(s).expect("in range")];
                    let end = [u8::try_from(e).expect("in range")];
                    assert_incomplete(tree, &start, &end, &[]);
                }
            }
        }

        assert_incomplete(
            &range_tree,
            b"",
            b"b",
            &[KeyRange::new(b"".to_vec(), b"b".to_vec())],
        );
        assert_incomplete(
            &range_tree,
            b"",
            b"",
            &[KeyRange::new(b"".to_vec(), b"".to_vec())],
        );
        assert_incomplete(
            &range_tree,
            b"b",
            b"",
            &[KeyRange::new(b"b".to_vec(), b"".to_vec())],
        );

        let range0 = new_range(b"", b"a");
        let range_a = new_range(b"a", b"b");
        let range_b = new_range(b"b", b"c");
        let range_c = new_range(b"c", b"d");
        let range_d = new_range(b"d", b"");

        range_tree.update(range_a.clone());
        assert_eq!(1, range_tree.len());
        assert_incomplete(&range_tree, b"a", b"b", &[]);
        assert_incomplete(
            &range_tree,
            b"",
            b"",
            &[
                KeyRange::new(b"".to_vec(), b"a".to_vec()),
                KeyRange::new(b"b".to_vec(), b"".to_vec()),
            ],
        );
        assert_incomplete(
            &range_tree,
            b"b",
            b"",
            &[KeyRange::new(b"b".to_vec(), b"".to_vec())],
        );

        range_tree.update(range_c.clone());
        assert_eq!(2, range_tree.len());
        assert_incomplete(
            &range_tree,
            b"a",
            b"c",
            &[KeyRange::new(b"b".to_vec(), b"c".to_vec())],
        );
        assert_incomplete(
            &range_tree,
            b"b",
            b"c",
            &[KeyRange::new(b"b".to_vec(), b"c".to_vec())],
        );
        assert_incomplete(
            &range_tree,
            b"",
            b"",
            &[
                KeyRange::new(b"".to_vec(), b"a".to_vec()),
                KeyRange::new(b"b".to_vec(), b"c".to_vec()),
                KeyRange::new(b"d".to_vec(), b"".to_vec()),
            ],
        );

        assert!(range_tree.get(b"").is_none());
        assert_eq!(Some(&range_a), range_tree.get(b"a"));
        assert!(range_tree.get(b"b").is_none());
        assert_eq!(Some(&range_c), range_tree.get(b"c"));
        assert!(range_tree.get(b"d").is_none());

        range_tree.update(range_b.clone());
        assert_eq!(3, range_tree.len());
        assert_eq!(Some(&range_b), range_tree.get(b"b"));
        assert_incomplete(
            &range_tree,
            b"",
            b"",
            &[
                KeyRange::new(b"".to_vec(), b"a".to_vec()),
                KeyRange::new(b"d".to_vec(), b"".to_vec()),
            ],
        );

        range_tree.update(range_d.clone());
        assert_eq!(4, range_tree.len());
        assert_eq!(Some(&range_d), range_tree.get(b"d"));
        assert_incomplete(
            &range_tree,
            b"",
            b"",
            &[KeyRange::new(b"".to_vec(), b"a".to_vec())],
        );

        // No incomplete range at all after inserting range 0.
        range_tree.update(range0);
        assert_eq!(5, range_tree.len());

        // Overwrite range B and C.
        let range_bd = new_range(b"b", b"d");
        range_tree.update(range_bd);
        assert_eq!(4, range_tree.len());
        assert_all_complete(&range_tree);

        // Overwrite range BD; c-d should be empty.
        range_tree.update(range_b);
        assert_eq!(4, range_tree.len());
        assert_incomplete(
            &range_tree,
            b"",
            b"",
            &[KeyRange::new(b"c".to_vec(), b"d".to_vec())],
        );

        range_tree.update(range_c);
        assert_eq!(5, range_tree.len());
        assert_all_complete(&range_tree);
    }

    /// Go `TestRangeTreePutForce` (`rtree_test.go`).
    #[test]
    fn range_tree_put_force() {
        fn check(tree: &RangeTree<TestFile>, expected: &[(&[u8], &[u8], &str)]) {
            assert_eq!(tree.len(), expected.len());
            let mut i = 0;
            tree.ascend(|item| {
                assert_eq!(expected[i].0, item.start_key());
                assert_eq!(expected[i].1, item.end_key());
                assert_eq!(expected[i].2, item.files[0].name);
                i += 1;
                true
            });
        }

        let mut tree: RangeTree<TestFile> = RangeTree::new();
        assert!(tree.put_force(b"aa", b"bb", named("1.sst"), true));
        assert!(tree.put_force(b"ff", b"hh", named("2.sst"), false));
        check(&tree, &[(b"aa", b"bb", "1.sst"), (b"ff", b"hh", "2.sst")]);

        // Put with force.
        assert!(tree.put_force(b"a", b"ab", named("3.sst"), true));
        check(&tree, &[(b"a", b"ab", "3.sst"), (b"ff", b"hh", "2.sst")]);
        assert!(tree.put_force(b"aaa", b"abc", named("4.sst"), true));
        check(&tree, &[(b"aaa", b"abc", "4.sst"), (b"ff", b"hh", "2.sst")]);
        assert!(tree.put_force(b"aaaa", b"aaab", named("5.sst"), true));
        check(
            &tree,
            &[(b"aaaa", b"aaab", "5.sst"), (b"ff", b"hh", "2.sst")],
        );
        assert!(tree.put_force(b"aa", b"bb", named("6.sst"), true));
        check(&tree, &[(b"aa", b"bb", "6.sst"), (b"ff", b"hh", "2.sst")]);

        // Put without force.
        assert!(!tree.put_force(b"f", b"fh", named("7.sst"), false));
        check(&tree, &[(b"aa", b"bb", "6.sst"), (b"ff", b"hh", "2.sst")]);
        assert!(!tree.put_force(b"fff", b"fhi", named("8.sst"), false));
        check(&tree, &[(b"aa", b"bb", "6.sst"), (b"ff", b"hh", "2.sst")]);
        assert!(!tree.put_force(b"ffff", b"fffh", named("9.sst"), false));
        check(&tree, &[(b"aa", b"bb", "6.sst"), (b"ff", b"hh", "2.sst")]);
        assert!(!tree.put_force(b"ff", b"hh", named("10.sst"), false));
        check(&tree, &[(b"aa", b"bb", "6.sst"), (b"ff", b"hh", "2.sst")]);

        // Put with force bound.
        assert!(tree.put_force(b"aa", b"ab", named("11.sst"), true));
        check(&tree, &[(b"aa", b"ab", "11.sst"), (b"ff", b"hh", "2.sst")]);
        assert!(tree.put_force(b"aaa", b"ab", named("12.sst"), true));
        check(&tree, &[(b"aaa", b"ab", "12.sst"), (b"ff", b"hh", "2.sst")]);

        // Put without force bound.
        assert!(!tree.put_force(b"ff", b"fh", named("13.sst"), false));
        check(&tree, &[(b"aaa", b"ab", "12.sst"), (b"ff", b"hh", "2.sst")]);
        assert!(!tree.put_force(b"fh", b"hh", named("14.sst"), false));
        check(&tree, &[(b"aaa", b"ab", "12.sst"), (b"ff", b"hh", "2.sst")]);

        // Put outside the bound.
        assert!(tree.put_force(b"ab", b"abc", named("15.sst"), true));
        check(
            &tree,
            &[
                (b"aaa", b"ab", "12.sst"),
                (b"ab", b"abc", "15.sst"),
                (b"ff", b"hh", "2.sst"),
            ],
        );
        assert!(tree.put_force(b"aa", b"aaa", named("16.sst"), true));
        check(
            &tree,
            &[
                (b"aa", b"aaa", "16.sst"),
                (b"aaa", b"ab", "12.sst"),
                (b"ab", b"abc", "15.sst"),
                (b"ff", b"hh", "2.sst"),
            ],
        );
        assert!(tree.put_force(b"hh", b"hi", named("17.sst"), false));
        check(
            &tree,
            &[
                (b"aa", b"aaa", "16.sst"),
                (b"aaa", b"ab", "12.sst"),
                (b"ab", b"abc", "15.sst"),
                (b"ff", b"hh", "2.sst"),
                (b"hh", b"hi", "17.sst"),
            ],
        );
        assert!(tree.put_force(b"ef", b"ff", named("18.sst"), false));
        check(
            &tree,
            &[
                (b"aa", b"aaa", "16.sst"),
                (b"aaa", b"ab", "12.sst"),
                (b"ab", b"abc", "15.sst"),
                (b"ef", b"ff", "18.sst"),
                (b"ff", b"hh", "2.sst"),
                (b"hh", b"hi", "17.sst"),
            ],
        );
    }

    /// Go `TestRangeIntersect` (`rtree_test.go`).
    #[test]
    fn range_intersect() {
        let rg = new_range(b"a", b"c");

        assert_eq!(
            Some((b"a".to_vec(), b"c".to_vec())),
            rg.key_range.intersect(b"", b"")
        );
        assert_eq!(None, rg.key_range.intersect(b"", b"a"));
        assert_eq!(
            Some((b"a".to_vec(), b"b".to_vec())),
            rg.key_range.intersect(b"", b"b")
        );
        assert_eq!(
            Some((b"a".to_vec(), b"b".to_vec())),
            rg.key_range.intersect(b"a", b"b")
        );
        assert_eq!(
            Some((b"aa".to_vec(), b"b".to_vec())),
            rg.key_range.intersect(b"aa", b"b")
        );
        assert_eq!(
            Some((b"b".to_vec(), b"c".to_vec())),
            rg.key_range.intersect(b"b", b"c")
        );
        assert_eq!(None, rg.key_range.intersect(b"", &[1]));
        assert_eq!(None, rg.key_range.intersect(b"c", b""));
    }

    /// Go `BenchmarkRangeTreeUpdate`: skipped. Benchmarks are a Go `testing`
    /// harness feature with no assertion to preserve.
    #[test]
    #[ignore = "Go BenchmarkRangeTreeUpdate: benchmark, no assertions"]
    fn benchmark_range_tree_update() {}

    fn encode_table_record(prefix: &[u8], row_id: u64) -> Vec<u8> {
        encode_record_key(
            prefix,
            &RecordHandle::Int(i64::try_from(row_id).expect("row id fits")),
        )
    }

    /// Go `makeEncodeKeyspacedTableRecord(1)`: `tikv.NewCodecV2(ModeTxn,
    /// KeyspaceMeta{Id: 1}).EncodeKey(...)` prefixes the key with the txn mode
    /// byte and the three-byte big-endian keyspace id.
    fn encode_keyspaced_table_record(prefix: &[u8], row_id: u64) -> Vec<u8> {
        let mut key = vec![b'x', 0, 0, 1];
        key.extend_from_slice(&encode_table_record(prefix, row_id));
        key
    }

    /// Go `TestRangeTreeMerge` (`rtree_test.go`), both sub-tests.
    #[test]
    fn range_tree_merge() {
        for encode in [
            encode_table_record as fn(&[u8], u64) -> Vec<u8>,
            encode_keyspaced_table_record,
        ] {
            let mut range_tree: RangeStatsTree<TestFile> = RangeStatsTree::new();
            let table_prefix = gen_table_record_prefix(1);
            for i in 0u64..10000 {
                range_tree.insert_range(
                    &Range::with_files(
                        encode(&table_prefix, i),
                        encode(&table_prefix, i + 1),
                        vec![TestFile {
                            name: format!("{i:20}"),
                            crc64_xor: 0,
                            total_kvs: 1,
                            total_bytes: 1,
                        }],
                    ),
                    i,
                    0,
                );
            }
            let sorted_ranges = range_tree.merged_ranges(10, 10);
            assert_eq!(1000, sorted_ranges.len());
            for (i, rg) in sorted_ranges.iter().enumerate() {
                let i = u64::try_from(i).expect("index fits");
                assert_eq!(encode(&table_prefix, i * 10), rg.start_key());
                assert_eq!(encode(&table_prefix, (i + 1) * 10), rg.end_key());
                assert_eq!(i * 10 * 10 + 45, rg.size);
                assert_eq!(10, rg.files().len());
                for (j, file) in rg.files().iter().enumerate() {
                    let j = u64::try_from(j).expect("index fits");
                    assert_eq!(format!("{:20}", i * 10 + j), file.name);
                    assert_eq!(1, file.total_kvs);
                    assert_eq!(1, file.total_bytes);
                }
            }
        }
    }

    /// Go `FuzzMerge` (`merge_fuzz_test.go`), driven by its seed corpus.
    ///
    /// The Go fuzz target has no assertion beyond "does not panic"; the seed
    /// corpus is the single `f.Add` pair in the source (there is no checked-in
    /// `testdata/fuzz` directory for this target).
    #[test]
    fn fuzz_merge_seed_corpus() {
        let base_key_a = encode_index_seek_key(42, 1, &[]);
        let base_key_b = encode_index_seek_key(42, 1, &[]);
        let seed_corpus = vec![(base_key_a, base_key_b)];
        for (a, b) in seed_corpus {
            let one = vec![TestFile {
                total_kvs: 1,
                total_bytes: 1,
                ..TestFile::default()
            }];
            let left = RangeStats {
                range: Range::with_files(a, Vec::<u8>::new(), one.clone()),
                size: 0,
                count: 0,
            };
            let right = RangeStats {
                range: Range::with_files(b, Vec::<u8>::new(), one),
                size: 0,
                count: 0,
            };
            // Both keys address index 1 of table 42, so the merge is allowed.
            assert!(needs_merge(&left, &right, 42, 42));
        }
    }

    fn build_progress_range(start_key: &str, end_key: &str) -> ProgressRange<TestFile> {
        ProgressRange {
            res: RangeTree::new(),
            origin: KeyRange::new(start_key.as_bytes(), end_key.as_bytes()),
        }
    }

    fn build_progress_range_with_physical_id(
        start_key: &str,
        end_key: &str,
        physical_id: i64,
    ) -> ProgressRange<TestFile> {
        let mut pr = build_progress_range(start_key, end_key);
        pr.res.physical_id = physical_id;
        pr
    }

    /// Go `TestProgressRangeTree` (`rtree_test.go`).
    #[test]
    fn progress_range_tree() {
        let mut pr_tree: ProgressRangeTree<TestFile> = ProgressRangeTree::new(None, false);

        assert!(pr_tree.insert(build_progress_range("aa", "cc")).is_ok());
        assert!(pr_tree.insert(build_progress_range("bb", "cc")).is_err());
        assert!(pr_tree.insert(build_progress_range("bb", "dd")).is_err());
        assert!(pr_tree.insert(build_progress_range("cc", "dd")).is_ok());
        assert!(pr_tree.insert(build_progress_range("ee", "ff")).is_ok());

        let ranges = pr_tree.get_incomplete_ranges().expect("no meta writer");
        assert_eq!(KeyRange::new(b"aa".to_vec(), b"cc".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"cc".to_vec(), b"dd".to_vec()), ranges[1]);
        assert_eq!(KeyRange::new(b"ee".to_vec(), b"ff".to_vec()), ranges[2]);

        let put = |tree: &mut ProgressRangeTree<TestFile>, s: &[u8], e: &[u8]| {
            let pr = tree
                .find_contained(s, e)
                .expect("contained")
                .expect("found");
            pr.res.put(s, e, Vec::new());
        };

        put(&mut pr_tree, b"aaa", b"b");
        put(&mut pr_tree, b"cc", b"dd");

        let ranges = pr_tree.get_incomplete_ranges().expect("no meta writer");
        assert_eq!(KeyRange::new(b"aa".to_vec(), b"aaa".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"b".to_vec(), b"cc".to_vec()), ranges[1]);
        assert_eq!(KeyRange::new(b"ee".to_vec(), b"ff".to_vec()), ranges[2]);

        put(&mut pr_tree, b"aa", b"aaa");
        put(&mut pr_tree, b"b", b"cc");
        put(&mut pr_tree, b"ee", b"ff");

        let ranges = pr_tree.get_incomplete_ranges().expect("no meta writer");
        assert_eq!(0, ranges.len());
    }

    /// Go `TestProgreeRangeTreeCallBack` (`rtree_test.go`).
    ///
    /// Go keeps `pr` as a pointer across the tree deletion that completes it,
    /// so the last two `Put`s land on an object the tree no longer holds.
    /// Rust makes that explicit with `detached`.
    #[test]
    fn progress_range_tree_call_back() {
        let mut pr_tree: ProgressRangeTree<TestFile> = ProgressRangeTree::new(None, false);

        assert!(pr_tree.insert(build_progress_range("a", "b")).is_ok());
        assert!(pr_tree.insert(build_progress_range("c", "d")).is_ok());
        assert!(pr_tree.insert(build_progress_range("e", "f")).is_ok());

        let complete_count = Rc::new(RefCell::new(0usize));
        let counter = Rc::clone(&complete_count);
        pr_tree.set_call_back(move || *counter.borrow_mut() += 1);

        pr_tree
            .find_contained(b"a", b"b")
            .expect("contained")
            .expect("found")
            .res
            .put(b"a", b"aa", Vec::new());
        let ranges = pr_tree.get_incomplete_ranges().expect("no meta writer");
        assert_eq!(0, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"aa".to_vec(), b"b".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[1]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[2]);

        pr_tree
            .find_contained(b"a", b"b")
            .expect("contained")
            .expect("found")
            .res
            .put(b"a", b"ab", Vec::new());
        let ranges = pr_tree.get_incomplete_ranges().expect("no meta writer");
        assert_eq!(0, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"ab".to_vec(), b"b".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[1]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[2]);

        // This completes "a".."b", so the tree drops it; `detached` is the
        // object Go's `pr` pointer keeps referring to afterwards.
        let mut detached = {
            let pr = pr_tree
                .find_contained(b"a", b"b")
                .expect("contained")
                .expect("found");
            pr.res.put(b"ab", b"b", Vec::new());
            pr.clone()
        };
        let ranges = pr_tree.get_incomplete_ranges().expect("no meta writer");
        assert_eq!(1, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[1]);

        detached.res.put(b"a", b"abc", Vec::new());
        let ranges = pr_tree.get_incomplete_ranges().expect("no meta writer");
        assert_eq!(1, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[1]);

        detached.res.put(b"cc", b"cd", Vec::new());
        let ranges = pr_tree.get_incomplete_ranges().expect("no meta writer");
        assert_eq!(1, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[1]);
    }

    /// A [`MetaSink`] that records what a real `metautil.MetaWriter` would have
    /// serialized into object storage.
    #[derive(Default)]
    struct RecordingSink {
        sent: Rc<RefCell<Vec<Vec<TestFile>>>>,
    }

    impl MetaSink<TestFile> for RecordingSink {
        fn send(&mut self, files: &[TestFile]) -> Result<(), RtreeError> {
            self.sent.borrow_mut().push(files.to_vec());
            Ok(())
        }
    }

    fn get_files(checksums: &[[u64; 3]]) -> Vec<TestFile> {
        checksums
            .iter()
            .map(|checksum| TestFile {
                name: String::new(),
                crc64_xor: checksum[0],
                total_kvs: checksum[1],
                total_bytes: checksum[2],
            })
            .collect()
    }

    /// Go `TestProgreeRangeTreeCallBack2` (`rtree_test.go`).
    ///
    /// Go drives a real `metautil.MetaWriter` over a local-storage backend; the
    /// [`MetaSink`] narrowing replaces it with a recorder. Every assertion the
    /// Go test makes is about the checksum map, which is unaffected.
    #[test]
    fn progress_range_tree_call_back2() {
        let sent = Rc::new(RefCell::new(Vec::new()));
        let sink = RecordingSink {
            sent: Rc::clone(&sent),
        };
        let mut pr_tree: ProgressRangeTree<TestFile> =
            ProgressRangeTree::new(Some(Box::new(sink)), false);

        assert!(pr_tree
            .insert(build_progress_range_with_physical_id("a", "b", 1))
            .is_ok());
        assert!(pr_tree
            .insert(build_progress_range_with_physical_id("c", "d", 2))
            .is_ok());
        assert!(pr_tree
            .insert(build_progress_range_with_physical_id("e", "f", 3))
            .is_ok());

        let complete_count = Rc::new(RefCell::new(0usize));
        let counter = Rc::clone(&complete_count);
        pr_tree.set_call_back(move || *counter.borrow_mut() += 1);

        pr_tree
            .find_contained(b"a", b"b")
            .expect("contained")
            .expect("found")
            .res
            .put(b"a", b"aa", get_files(&[[1, 1, 1], [2, 2, 2]]));
        let ranges = pr_tree.get_incomplete_ranges().expect("recording sink");
        assert_eq!(0, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"aa".to_vec(), b"b".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[1]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[2]);

        pr_tree
            .find_contained(b"a", b"b")
            .expect("contained")
            .expect("found")
            .res
            .put(b"a", b"ab", get_files(&[[3, 3, 3], [4, 4, 4]]));
        let ranges = pr_tree.get_incomplete_ranges().expect("recording sink");
        assert_eq!(0, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"ab".to_vec(), b"b".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[1]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[2]);

        let mut detached = {
            let pr = pr_tree
                .find_contained(b"a", b"b")
                .expect("contained")
                .expect("found");
            pr.res.put(b"ab", b"b", get_files(&[[5, 5, 5], [6, 6, 6]]));
            pr.clone()
        };
        let ranges = pr_tree.get_incomplete_ranges().expect("recording sink");
        assert_eq!(1, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[1]);
        let cksm = pr_tree.get_checksum_map();
        assert_eq!(1, cksm.len());
        let checksum = cksm[&1];
        assert_eq!(3 ^ 4 ^ 5 ^ 6, checksum.crc64_xor);
        assert_eq!(3 + 4 + 5 + 6, checksum.total_kvs);
        assert_eq!(3 + 4 + 5 + 6, checksum.total_bytes);
        assert_eq!(2, sent.borrow().len());

        detached.res.put(b"a", b"abc", Vec::new());
        let ranges = pr_tree.get_incomplete_ranges().expect("recording sink");
        assert_eq!(1, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[1]);

        detached.res.put(b"cc", b"cd", Vec::new());
        let ranges = pr_tree.get_incomplete_ranges().expect("recording sink");
        assert_eq!(1, *complete_count.borrow());
        assert_eq!(KeyRange::new(b"c".to_vec(), b"d".to_vec()), ranges[0]);
        assert_eq!(KeyRange::new(b"e".to_vec(), b"f".to_vec()), ranges[1]);
    }

    /// Go's `encodeTableRecord` helper leans on `codec.EncodeInt`; this keeps
    /// the import honest when the record encoder changes shape.
    #[test]
    fn table_record_prefix_is_encode_int_framed() {
        let mut expect = vec![b't'];
        encode_int(&mut expect, 1);
        expect.extend_from_slice(b"_r");
        assert_eq!(expect, gen_table_record_prefix(1));
    }
}
