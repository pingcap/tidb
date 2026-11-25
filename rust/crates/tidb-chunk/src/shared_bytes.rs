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

//! Packed bytes that become shared only when `MutRow` creates a shallow cell.
//!
//! Ordinary columns retain a plain `Vec<u8>` fast path. Creating a shallow
//! alias promotes that vector to synchronized shared storage; subsequent
//! growth of either view detaches it using Rust's native allocation policy.
//! This is an aliasing abstraction, not an emulation of Go slice headers.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::ops::{Deref, Range};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, RwLockReadGuard, TryLockError};
use tidb_charset::Collation;
use tidb_datatype::{
    BinaryJSON, BinaryJSONError, Datum, EncodedJSONLayout, SharedVectorBytes, StringDatum,
    VectorError, VectorFloat32,
};
use tidb_goruntime::GoString;

const STRING_COPY_CAPACITY: usize = 64;
const STRING_COPY_SLOTS: usize = 8;

// These cells have the same length and passed the short prefix and suffix checks.
fn equal_json_cells(left: &[u8], right: &[u8]) -> bool {
    debug_assert_eq!(left.len(), right.len());
    let (left, left_tail) = left.as_chunks::<8>();
    let (right, right_tail) = right.as_chunks::<8>();
    let (left, left_remainder) = left.as_chunks::<8>();
    let (right, right_remainder) = right.as_chunks::<8>();
    let mut difference = 0;
    for (a, b) in left.iter().zip(right) {
        for (left, right) in a.iter().zip(b) {
            difference |= u64::from_ne_bytes(*left) ^ u64::from_ne_bytes(*right);
        }
    }
    difference == 0 && left_remainder == right_remainder && left_tail == right_tail
}

struct StringCopyBuffers {
    buffers: [Option<Arc<Vec<u8>>>; STRING_COPY_SLOTS],
    next: usize,
}

thread_local! {
    static STRING_COPIES: RefCell<StringCopyBuffers> = const {
        RefCell::new(StringCopyBuffers { buffers: [const { None }; STRING_COPY_SLOTS], next: 0 })
    };
}

#[inline]
fn copy_string(bytes: &[u8]) -> GoString {
    if let Some(snapshot) = GoString::try_inline_snapshot(bytes) {
        return snapshot;
    }
    copy_string_buffered(bytes)
}

fn copy_string_buffered(bytes: &[u8]) -> GoString {
    if bytes.len() >= STRING_COPY_CAPACITY {
        return GoString::from(bytes);
    }
    let copied = STRING_COPIES
        .try_with(|buffers| {
            let mut buffers = buffers.try_borrow_mut().ok()?;
            let index = buffers.next;
            buffers.next = (index + 1) % STRING_COPY_SLOTS;
            let buffer = buffers.buffers[index]
                .get_or_insert_with(|| Arc::new(vec![0; STRING_COPY_CAPACITY]));
            // Probe one slot per copy. If a string still owns it, use a new
            // allocation so that string keeps its original bytes.
            if Arc::strong_count(buffer) != 1 {
                return None;
            }
            let destination = Arc::get_mut(buffer)?;
            destination[..bytes.len()].copy_from_slice(bytes);
            Some(Arc::clone(buffer))
        })
        .ok()
        .flatten();
    copied.map_or_else(
        || GoString::from(bytes),
        |buffer| GoString::from_shared_vec(buffer, 0..bytes.len()),
    )
}

pub(crate) fn finish_string(bytes: Vec<u8>) -> GoString {
    if (1..STRING_COPY_CAPACITY).contains(&bytes.len()) {
        copy_string(&bytes)
    } else {
        bytes.into()
    }
}

enum Backing {
    Owned(Vec<u8>),
    /// Wide cells can retain immutable slices. Writes reuse the Vec while
    /// its owner is unique and detach while a result still references it.
    Retained {
        bytes: Arc<Vec<u8>>,
        // A refill may overwrite this initialized tail before a re-slice
        // needs it. The visible bytes always live in `bytes`.
        tail: Option<Arc<Vec<u8>>>,
    },
    Shared(Arc<RwLock<Vec<u8>>>),
    /// An immutable slice of a buffer owned elsewhere -- a coprocessor
    /// response payload the chunk decoder points a column at, as Go's
    /// `decodeColumn` does (`col.data = buffer[:n]`, `avoidReusing`). Any
    /// mutation or growth detaches into an owned copy first.
    /// Unaligned vector data can cache an aligned copy for typed reads.
    Frozen(FrozenBytes),
}

#[derive(Clone)]
enum FrozenBytes {
    Raw {
        bytes: bytes::Bytes,
        // A selected prefix keeps the rest initialized for a later resize.
        compacted: bool,
    },
    // The visible span was moved to the front logically. `SharedBytes.start`
    // locates its source, and `len` is also the length of the overwritten prefix.
    RelocatedRaw(bytes::Bytes),
    String {
        bytes: Arc<bytes::Bytes>,
        capacity_end: usize,
    },
    Gathered(Box<GatheredBytes>),
    Vector(Arc<SharedVectorBytes>),
}

#[derive(Clone)]
struct GatheredBytes {
    selected: bytes::Bytes,
    tail: bytes::Bytes,
}

impl FrozenBytes {
    #[inline]
    fn bytes(&self) -> &bytes::Bytes {
        match self {
            Self::Raw { bytes, .. } | Self::RelocatedRaw(bytes) => bytes,
            Self::String { bytes, .. } => bytes,
            Self::Gathered(bytes) => &bytes.selected,
            Self::Vector(vectors) => vectors.bytes(),
        }
    }

    fn has_deferred_tail(&self) -> bool {
        match self {
            Self::Raw { compacted, .. } => *compacted,
            Self::RelocatedRaw(_) | Self::Gathered(_) => true,
            Self::String { capacity_end, .. } => *capacity_end != 0,
            _ => false,
        }
    }
}

impl Backing {
    #[inline]
    fn writable_vec(
        &mut self,
        json_layouts: &mut OnceLock<Box<JsonLayoutCache>>,
    ) -> Option<&mut Vec<u8>> {
        match self {
            Self::Owned(bytes) => Some(bytes),
            Self::Retained { bytes: owner, tail } => {
                // Only live readers should force a copy before a write.
                clear_retained_json_bytes(json_layouts);
                if Arc::strong_count(owner) != 1 {
                    *owner = Arc::new(copy_backing(owner));
                }
                let bytes = Arc::get_mut(owner)?;
                if let Some(old) = tail.take() {
                    if let Some(unwritten) = old.get(bytes.len()..) {
                        bytes.extend_from_slice(unwritten);
                    }
                }
                Some(bytes)
            }
            Self::Shared(_) | Self::Frozen(..) => None,
        }
    }
}

fn copy_backing(bytes: &Vec<u8>) -> Vec<u8> {
    let mut copy = Vec::with_capacity(bytes.capacity());
    copy.extend_from_slice(bytes);
    copy
}

enum JsonLayouts<T = EncodedJSONLayout> {
    // The first two cells fit in the cache allocation without a hash table.
    Few([Option<(Range<usize>, T)>; 2]),
    Many(HashMap<Range<usize>, T>),
}

impl<T> Default for JsonLayouts<T> {
    fn default() -> Self {
        Self::Few([None, None])
    }
}

impl<T: Copy> JsonLayouts<T> {
    fn get(&self, range: &Range<usize>) -> Option<T> {
        match self {
            Self::Few(entries) => entries
                .iter()
                .flatten()
                .find_map(|(key, value)| (key == range).then_some(*value)),
            Self::Many(entries) => entries.get(range).copied(),
        }
    }

    fn insert(&mut self, range: Range<usize>, layout: T) {
        if let Self::Few(entries) = self {
            if let Some(entry) = entries
                .iter_mut()
                .find(|entry| entry.as_ref().is_none_or(|(key, _)| key == &range))
            {
                *entry = Some((range, layout));
                return;
            }
            *self = Self::Many(entries.iter_mut().filter_map(Option::take).collect());
        }
        if let Self::Many(entries) = self {
            entries.insert(range, layout);
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Few(entries) => *entries = [None, None],
            Self::Many(entries) => entries.clear(),
        }
    }
}

#[derive(Default)]
struct JsonLayoutCache {
    // Appends already have exclusive access to storage. Look up these
    // layouts without locking, including when the column is shared for reads.
    appended: JsonLayouts,
    checked: OnceLock<Mutex<JsonLayouts>>,
    depths: OnceLock<Mutex<JsonLayouts<usize>>>,
    distinct_json: AtomicBool,
    retained_bytes: OnceLock<bytes::Bytes>,
}

impl JsonLayoutCache {
    fn get(&self, range: &Range<usize>) -> Option<EncodedJSONLayout> {
        self.appended.get(range).or_else(|| {
            self.checked
                .get()?
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(range)
        })
    }

    fn get_depth(&self, range: &Range<usize>) -> Option<usize> {
        self.depths.get()?.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(range)
    }

    fn previous<T>(
        &self,
        data: &[u8],
        range: &Range<usize>,
        get: impl FnOnce(&Range<usize>) -> Option<T>,
    ) -> Option<T> {
        let width = range.len();
        if width > 32 * 1024 || self.distinct_json.load(Ordering::Relaxed) {
            return None;
        }
        let start = range.start.checked_sub(width)?;
        let previous_range = start..range.start;
        let previous = &data[previous_range.clone()];
        let cell = &data[range.clone()];
        // Reject the common mismatch cases before comparing the whole cell.
        if cell[..16] != previous[..16] || cell[width - 16..] != previous[width - 16..] {
            return None;
        }
        let result = get(&previous_range)?;
        if equal_json_cells(cell, previous) {
            Some(result)
        } else {
            self.distinct_json.store(true, Ordering::Relaxed);
            None
        }
    }

    fn clear(&mut self) {
        self.retained_bytes.take();
        self.appended.clear();
        *self.distinct_json.get_mut() = false;
        if let Some(checked) = self.checked.get_mut() {
            checked
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
        if let Some(depths) = self.depths.get_mut() {
            depths.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
        }
    }
}

fn clear_retained_json_bytes(cache: &mut OnceLock<Box<JsonLayoutCache>>) {
    if let Some(cache) = cache.get_mut() {
        cache.retained_bytes.take();
    }
}

pub(crate) struct RetainedSlice {
    owner: Arc<Vec<u8>>,
    range: Range<usize>,
}

pub(crate) enum RetainedBytes {
    Vec(RetainedSlice),
    Frozen(bytes::Bytes),
}

impl RetainedBytes {
    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        match self {
            Self::Vec(slice) => bytes::Bytes::from_owner(slice),
            Self::Frozen(bytes) => bytes,
        }
    }

    pub(crate) fn into_string(self, range: Range<usize>, collation: Collation) -> StringDatum {
        match self {
            Self::Vec(slice) => {
                assert!(range.start <= range.end && range.end <= slice.range.len());
                let start = slice.range.start;
                let value = GoString::from_shared_vec(slice.owner, start + range.start..start + range.end);
                StringDatum::from_go_string(value, collation)
            }
            Self::Frozen(bytes) => StringDatum::from_shared_bytes(bytes.slice(range), collation),
        }
    }
}

impl AsRef<[u8]> for RetainedBytes {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Vec(slice) => slice.as_ref(),
            Self::Frozen(bytes) => bytes,
        }
    }
}

impl AsRef<[u8]> for RetainedSlice {
    fn as_ref(&self) -> &[u8] {
        &self.owner[self.range.clone()]
    }
}

pub(crate) struct SharedBytes {
    backing: Backing,
    start: usize,
    len: usize,
    // Ranges are relative to this view. Writes that can replace their bytes
    // or change their meaning discard the cache before modifying the backing.
    json_layouts: OnceLock<Box<JsonLayoutCache>>,
}

pub(crate) enum SharedBytesRead<'a> {
    Owned(&'a [u8]),
    Shared {
        backing: RwLockReadGuard<'a, Vec<u8>>,
        start: usize,
        len: usize,
    },
}

impl Deref for SharedBytesRead<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared {
                backing,
                start,
                len,
            } => &backing[*start..*start + *len],
        }
    }
}

impl AsRef<[u8]> for SharedBytesRead<'_> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Default for SharedBytes {
    fn default() -> Self {
        Self::from_vec(Vec::new())
    }
}

impl SharedBytes {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::from_vec(Vec::with_capacity(capacity))
    }

    pub(crate) fn from_vec(bytes: Vec<u8>) -> Self {
        let len = bytes.len();
        Self {
            backing: Backing::Owned(bytes),
            start: 0,
            len,
            json_layouts: OnceLock::new(),
        }
    }

    pub(crate) fn zeros(len: usize) -> Self {
        Self::from_vec(vec![0; len])
    }

    /// Shares `bytes` without copying; see [`Backing::Frozen`].
    pub(crate) fn from_bytes(bytes: bytes::Bytes) -> Self {
        Self::frozen(FrozenBytes::Raw { bytes, compacted: false })
    }

    pub(crate) fn from_string_bytes(bytes: Arc<bytes::Bytes>, range: Range<usize>) -> Self {
        assert!(range.start <= range.end && range.end <= bytes.len());
        Self {
            backing: Backing::Frozen(FrozenBytes::String {
                bytes,
                capacity_end: 0,
            }),
            start: range.start,
            len: range.len(),
            json_layouts: OnceLock::new(),
        }
    }

    /// Shares encoded vector data. Reuse the column owner when no vector
    /// retained the preceding frame.
    pub(crate) fn replace_vector_bytes(&mut self, bytes: bytes::Bytes) {
        if cfg!(target_endian = "little") {
            if let Backing::Frozen(FrozenBytes::Vector(owner)) = &mut self.backing {
                // The live owner keeps this byte range immutable. Keep its
                // aligned copy when the caller decodes that same range again.
                if owner.bytes().as_ptr() == bytes.as_ptr() && owner.bytes().len() == bytes.len() {
                    self.start = 0;
                    self.len = bytes.len();
                    self.json_layouts.take();
                    return;
                }
                if let Some(owner) = Arc::get_mut(owner) {
                    self.start = 0;
                    self.len = bytes.len();
                    self.json_layouts = OnceLock::new();
                    owner.replace_bytes(bytes);
                    return;
                }
            }
            *self = Self::frozen(FrozenBytes::Vector(Arc::new(SharedVectorBytes::new(bytes))));
        } else {
            *self = Self::from_bytes(bytes);
        }
    }

    fn frozen(bytes: FrozenBytes) -> Self {
        let len = bytes.bytes().len();
        Self {
            backing: Backing::Frozen(bytes),
            start: 0,
            len,
            json_layouts: OnceLock::new(),
        }
    }

    #[inline]
    pub(crate) fn retain_for_slices(&mut self) {
        if let Backing::Owned(bytes) = &mut self.backing {
            self.backing = Backing::Retained {
                bytes: Arc::new(mem::take(bytes)),
                tail: None,
            };
        }
    }

    /// Interprets a cell as arbitrary Go string bytes.
    #[inline]
    pub(crate) fn get_string(&self, range: Range<usize>) -> GoString {
        assert!(range.start <= range.end && range.end <= self.len);
        if range.is_empty() {
            return GoString::default();
        }
        let absolute = self.start + range.start..self.start + range.end;
        match &self.backing {
            Backing::Owned(bytes) => copy_string(&bytes[absolute]),
            Backing::Retained { bytes, .. } => {
                GoString::from_shared_vec(Arc::clone(bytes), absolute)
            }
            Backing::Frozen(FrozenBytes::String { bytes, .. }) => {
                GoString::from_shared_bytes_range(Arc::clone(bytes), absolute)
            }
            Backing::Frozen(bytes) => GoString::from_shared_bytes(bytes.bytes().slice(absolute)),
            Backing::Shared(_) => copy_string(&self.read_locked()[range]),
        }
    }

    /// Takes a string datum snapshot without wrapping an already retained
    /// vector in another reference-counted allocation.
    #[inline]
    pub(crate) fn write_string_datum(
        &self,
        range: Range<usize>,
        collation: Collation,
        min_retained_cell_bytes: usize,
        datum: &mut Datum,
    ) {
        // Copy smaller cells when retaining one would pin the rest of the
        // column. Allow wide packed cells to share a usual vector batch,
        // with room for capacity growth beyond the initialized rows.
        const MAX_UNRELATED_RETAINED_BYTES: usize = 64;
        const MIN_PACKED_SNAPSHOT_BYTES: usize = 1024;
        const MAX_PACKED_BACKING_RATIO: usize = 2_048;
        assert!(range.start <= range.end && range.end <= self.len);
        let len = range.len();
        if len > StringDatum::INLINE_CAPACITY {
            let absolute = self.start + range.start..self.start + range.end;
            match &self.backing {
                Backing::Retained { bytes: owner, .. }
                    if len >= min_retained_cell_bytes
                        || len >= MIN_PACKED_SNAPSHOT_BYTES
                            && owner.capacity() <= len.saturating_mul(MAX_PACKED_BACKING_RATIO)
                        || owner.len() - len <= MAX_UNRELATED_RETAINED_BYTES =>
                {
                    datum.set_retained_string(owner, absolute, collation);
                    return;
                }
                Backing::Frozen(bytes) => {
                    let bytes = bytes.bytes().slice(absolute);
                    *datum = Datum::String(StringDatum::from_shared_bytes(bytes, collation));
                    return;
                }
                Backing::Owned(_) | Backing::Retained { .. } | Backing::Shared(_) => {}
            }
            if len < STRING_COPY_CAPACITY {
                let bytes = copy_string(&self.read()[range]);
                *datum = Datum::String(StringDatum::from_go_string(bytes, collation));
                return;
            }
        }
        datum.set_copied_string(&self.read()[range], collation);
    }

    /// Retains an immutable view of published storage. Mutable shallow
    /// aliases need the ordinary snapshot path.
    pub(crate) fn retained_slice(&self, range: Range<usize>) -> Option<RetainedBytes> {
        if range.start > range.end || range.end > self.len {
            return None;
        }
        let range = self.start + range.start..self.start + range.end;
        match &self.backing {
            Backing::Retained { bytes, .. } => Some(RetainedBytes::Vec(RetainedSlice {
                owner: Arc::clone(bytes),
                range,
            })),
            Backing::Frozen(bytes) => Some(RetainedBytes::Frozen(bytes.bytes().slice(range))),
            Backing::Owned(_) | Backing::Shared(_) => None,
        }
    }

    /// Shares one immutable byte owner across retained JSON reads.
    pub(crate) fn retained_json_slice(&self, range: Range<usize>) -> Option<bytes::Bytes> {
        if let Backing::Retained { bytes: owner, .. } = &self.backing {
            if range.start > range.end || range.end > self.len {
                return None;
            }
            let cache = self.json_layouts.get_or_init(Default::default);
            let bytes = cache.retained_bytes.get_or_init(|| {
                bytes::Bytes::from_owner(RetainedSlice {
                    owner: Arc::clone(owner),
                    range: 0..owner.len(),
                })
            });
            return Some(bytes.slice(self.start + range.start..self.start + range.end));
        }
        self.retained_slice(range).map(RetainedBytes::into_bytes)
    }

    #[inline]
    pub(crate) fn retained_vector(
        &self,
        range: Range<usize>,
    ) -> Option<Result<VectorFloat32, VectorError>> {
        if let Backing::Frozen(FrozenBytes::Vector(vectors)) = &self.backing {
            if range.start > range.end || range.end > self.len {
                return None;
            }
            return Some(vectors.read(self.start + range.start..self.start + range.end));
        }
        self.retained_slice(range)
            .map(|bytes| VectorFloat32::from_shared_encoded(bytes.into_bytes()))
    }

    /// Retains an immutable wire cell using its native alignment or the
    /// existing vector column owner.
    #[cfg(target_endian = "little")]
    #[inline]
    pub(crate) fn retained_small_vector(
        &self,
        range: Range<usize>,
    ) -> Option<Result<VectorFloat32, VectorError>> {
        let Backing::Frozen(bytes) = &self.backing else {
            return None;
        };
        if range.start > range.end || range.end > self.len {
            return None;
        }
        let range = self.start + range.start..self.start + range.end;
        if let FrozenBytes::Vector(vectors) = bytes {
            return Some(vectors.read(range));
        }
        let bytes = bytes.bytes();
        if bytes[range.clone()].as_ptr().align_offset(align_of::<f32>()) == 0 {
            return Some(VectorFloat32::from_shared_encoded(bytes.slice(range)));
        }
        None
    }

    pub(crate) fn known_json_layout(&self, range: &Range<usize>) -> Option<EncodedJSONLayout> {
        if self.is_shared() {
            return None;
        }
        self.json_layouts.get()?.get(range)
    }

    pub(crate) fn cached_json_layout(&self, range: Range<usize>) -> Option<EncodedJSONLayout> {
        if self.is_shared() || range.len() < crate::column::Column::MIN_RETAINED_CELL_BYTES {
            return None;
        }
        let cache = self.json_layouts.get_or_init(Default::default);
        if let Some(layout) = cache.get(&range) {
            return Some(layout);
        }
        let data = self.read();
        let cell = &data[range.clone()];
        let previous = cache.previous(&data, &range, |previous| cache.get(previous));
        let layout = previous.unwrap_or_else(|| BinaryJSON::check_encoded_layout(cell));
        cache
            .checked
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(range, layout);
        Some(layout)
    }

    pub(crate) fn cached_json_depth(&self, data: &[u8], range: Range<usize>) -> Result<usize, BinaryJSONError> {
        let cell = &data[range.clone()];
        if self.is_shared() {
            return BinaryJSON::get_encoded_elem_depth(cell, || self.known_json_layout(&range));
        }
        if let Some(cache) = self.json_layouts.get() {
            if let Some(depth) = cache.get_depth(&range) {
                return Ok(depth);
            }
            if let Some(layout) = cache.get(&range) {
                return BinaryJSON::get_encoded_elem_depth(cell, || Some(layout));
            }
        }
        if !BinaryJSON::should_cache_encoded_depth(cell) {
            return BinaryJSON::get_encoded_elem_depth(cell, || None);
        }
        let cache = self.json_layouts.get_or_init(Default::default);
        let depth = match cache.previous(data, &range, |previous| cache.get_depth(previous)) {
            Some(depth) => depth,
            None => BinaryJSON::get_encoded_elem_depth(cell, || None)?,
        };
        // A depth read only validates framing. Other operations must still
        // obtain a fully checked layout from the other cache.
        cache.depths.get_or_init(Default::default).lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner).insert(range, depth);
        Ok(depth)
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn is_frozen(&self) -> bool {
        matches!(self.backing, Backing::Frozen(_))
    }

    pub(crate) fn can_gather_wire_bytes(&self) -> bool {
        matches!(&self.backing, Backing::Frozen(bytes)
            if !matches!(bytes, FrozenBytes::Gathered(_) | FrozenBytes::RelocatedRaw(_)))
    }

    pub(crate) fn capacity(&self) -> usize {
        match &self.backing {
            Backing::Owned(bytes) => bytes.capacity().saturating_sub(self.start),
            Backing::Retained { bytes, .. } => bytes.capacity().saturating_sub(self.start),
            Backing::Shared(backing) => Self::read_shared(backing)
                .capacity()
                .saturating_sub(self.start),
            Backing::Frozen(FrozenBytes::Raw { bytes, compacted: true }) => bytes.len() - self.start,
            Backing::Frozen(FrozenBytes::RelocatedRaw(bytes)) => bytes.len(),
            Backing::Frozen(FrozenBytes::String { capacity_end, .. }) if *capacity_end != 0 => {
                capacity_end - self.start
            }
            Backing::Frozen(FrozenBytes::Gathered(bytes)) => {
                bytes.selected.len() + bytes.tail.len() - self.start
            }
            Backing::Frozen(_) => self.len,
        }
    }

    /// The visible bytes. An owned or frozen backing answers with a plain
    /// slice, inlined into the cell accessors that call this per row; only a
    /// shared backing takes its read guard, out of line.
    #[inline]
    pub(crate) fn read(&self) -> SharedBytesRead<'_> {
        match self.try_borrow() {
            Some(bytes) => SharedBytesRead::Owned(bytes),
            None => self.read_locked(),
        }
    }

    #[inline]
    pub(crate) fn try_borrow(&self) -> Option<&[u8]> {
        let bytes = match &self.backing {
            Backing::Owned(bytes) => bytes.as_slice(),
            Backing::Retained { bytes, .. } => bytes.as_slice(),
            Backing::Frozen(bytes) => bytes.bytes().as_ref(),
            Backing::Shared(_) => return None,
        };
        Some(&bytes[self.start..self.start + self.len])
    }

    /// Borrows the visible bytes without waiting for a mutable shared backing.
    pub(crate) fn try_read(&self) -> Option<SharedBytesRead<'_>> {
        let Backing::Shared(backing) = &self.backing else {
            return self.try_borrow().map(SharedBytesRead::Owned);
        };
        let backing = match backing.try_read() {
            Ok(backing) => backing,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return None,
        };
        Some(SharedBytesRead::Shared {
            backing,
            start: self.start,
            len: self.len,
        })
    }

    #[inline(never)]
    fn read_locked(&self) -> SharedBytesRead<'_> {
        match &self.backing {
            Backing::Shared(backing) => SharedBytesRead::Shared {
                backing: Self::read_shared(backing),
                start: self.start,
                len: self.len,
            },
            Backing::Owned(_) | Backing::Retained { .. } | Backing::Frozen(..) => self.read(),
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        self.read().to_vec()
    }

    pub(crate) fn overwrite_from(&mut self, source: &Self) {
        let bytes = source.read();
        self.reset();
        if matches!(&self.backing, Backing::Shared(_))
            && matches!(&bytes, SharedBytesRead::Shared { .. })
        {
            // Release the source read guard before writing to a shared buffer.
            // The views can overlap or be copied concurrently in reverse order.
            let snapshot = bytes.to_vec();
            drop(bytes);
            self.extend_from_slice(&snapshot);
        } else {
            self.extend_from_slice(&bytes);
        }
    }

    #[inline]
    pub(crate) fn reset(&mut self) {
        self.preserve_relocated_tail(0);
        if let Some(layouts) = self.json_layouts.get_mut() {
            layouts.clear();
        }
        self.len = 0;
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        assert!(len <= self.len, "truncate cannot grow SharedBytes");
        self.preserve_relocated_tail(len);
        self.json_layouts.take();
        self.len = len;
    }

    /// Keeps a prefix of immutable wire bytes while retaining the original
    /// column capacity. A subsequent write still detaches the visible bytes.
    pub(crate) fn truncate_frozen_prefix(&mut self, len: usize) -> bool {
        let capacity_end = self.start + self.capacity();
        match &mut self.backing {
            Backing::Frozen(FrozenBytes::Raw { bytes, compacted }) => {
                if !*compacted {
                    bytes.truncate(capacity_end);
                    *compacted = true;
                }
            }
            Backing::Frozen(FrozenBytes::String { capacity_end: end, .. }) => {
                *end = capacity_end;
            }
            Backing::Frozen(FrozenBytes::Vector(owner))
                if owner.bytes().as_ptr().align_offset(align_of::<f32>()) == 0 =>
            {
                // A compacted prefix still owns the initialized wire tail.
                self.backing = Backing::Frozen(FrozenBytes::Raw {
                    bytes: owner.bytes().slice(..capacity_end),
                    compacted: true,
                });
            }
            _ => return false,
        }
        self.truncate(len);
        true
    }

    /// Installs the selected bytes and keeps the initialized tail available
    /// for a later resize, which in Go can expose those bytes again.
    pub(crate) fn replace_with_gathered(&mut self, mut selected: Vec<u8>) {
        let Backing::Frozen(bytes) = &self.backing else {
            unreachable!("gathering requires wire bytes");
        };
        let len = selected.len();
        let capacity = self.capacity();
        let tail_start = self.start + len;
        let tail_end = self.start + capacity;
        if len >= capacity / 2 {
            // Dense selections release the old response instead of retaining
            // a second copy of most of its bytes.
            selected.reserve_exact(capacity - len);
            selected.extend_from_slice(&bytes.bytes()[tail_start..tail_end]);
            *self = Self::from_vec(selected);
            self.len = len;
            return;
        }
        let tail = bytes.bytes().slice(tail_start..tail_end);
        *self = Self::frozen(FrozenBytes::Gathered(Box::new(GatheredBytes {
            selected: bytes::Bytes::from(selected),
            tail,
        })));
    }

    /// Moves one immutable span to the logical start, retaining the
    /// unchanged Go backing tail after the resulting visible bytes.
    pub(crate) fn select_frozen_range(&mut self, range: Range<usize>) {
        assert!(range.end <= self.len);
        let len = range.len();
        if self.start == 0 {
            if let Backing::Frozen(FrozenBytes::Raw { bytes, compacted }) = &mut self.backing {
                // Keep the original bytes after the visible span for a later resize.
                if !*compacted {
                    bytes.truncate(self.len);
                    if range.start != 0 && len != 0 {
                        self.start = range.start;
                        self.backing = Backing::Frozen(FrozenBytes::RelocatedRaw(mem::take(bytes)));
                    } else {
                        *compacted = true;
                    }
                    self.len = len;
                    self.json_layouts.take();
                    return;
                }
            }
        }
        if (range.start == 0 || len == 0) && self.truncate_frozen_prefix(len) {
            return;
        }
        let end = self.start + self.capacity();
        let Backing::Frozen(bytes) = &mut self.backing else {
            unreachable!("range selection requires wire bytes");
        };
        let bytes = match bytes {
            FrozenBytes::Raw { bytes, .. } if self.start == 0 => {
                let mut result = mem::take(bytes);
                result.truncate(end);
                result
            }
            bytes => bytes.bytes().slice(self.start..end),
        };
        self.backing = Backing::Frozen(FrozenBytes::RelocatedRaw(bytes));
        self.start = range.start;
        self.len = len;
        self.json_layouts.take();
    }

    #[inline]
    pub(crate) fn reconstruct_one(&mut self, range: Range<usize>) {
        assert!(range.end <= self.len);
        let len = range.len();
        if let Backing::Owned(bytes) = &mut self.backing {
            if range.start != 0 && len != 0 {
                let start = self.start + range.start;
                let end = start + len;
                if len == 8 {
                    let cell: [u8; 8] = bytes[start..end].try_into().unwrap();
                    bytes[self.start..self.start + 8].copy_from_slice(&cell);
                } else {
                    bytes.copy_within(start..end, self.start);
                }
            }
            self.len = len;
            self.json_layouts.take();
        } else if self.can_gather_wire_bytes() {
            self.select_frozen_range(range);
        } else {
            if range.start != 0 && len != 0 {
                self.with_write(|data| data.copy_within(range, 0));
            }
            self.truncate(len);
        }
    }

    fn preserve_relocated_tail(&mut self, len: usize) {
        // A later resize can re-expose part of the selected cell after its
        // visible length is shortened, so keep both spans before it changes.
        if len < self.len {
            if let Backing::Frozen(FrozenBytes::RelocatedRaw(bytes)) = &self.backing {
                let selected = bytes.slice(self.start..self.start + self.len);
                let tail = bytes.slice(self.len..);
                *self = Self::frozen(FrozenBytes::Gathered(Box::new(GatheredBytes {
                    selected,
                    tail,
                })));
            }
        }
    }

    /// Consume a prefix for the decoder. Immutable response bytes can travel
    /// across the column-lock boundary by reference. Snapshot mutable storage
    /// while it is locked so another column cannot change the consumed data.
    pub(crate) fn take_prefix(&mut self, count: usize) -> bytes::Bytes {
        assert!(count <= self.len, "prefix exceeds SharedBytes length");
        let prefix = match &self.backing {
            Backing::Frozen(bytes) => bytes.bytes().slice(self.start..self.start + count),
            _ => bytes::Bytes::copy_from_slice(&self.read()[..count]),
        };
        self.advance(count);
        prefix
    }

    pub(crate) fn advance(&mut self, count: usize) {
        assert!(count <= self.len, "advance exceeds SharedBytes length");
        if count != 0 {
            self.materialize_deferred_tail();
        }
        self.json_layouts.take();
        self.start += count;
        self.len -= count;
    }

    /// Go `slices.Grow(data, additional)`.
    pub(crate) fn reserve(&mut self, additional: usize) {
        let capacity = self.capacity();
        if capacity - self.len >= additional {
            return;
        }
        self.discard_deferred_tail();
        if self.start == 0 {
            if let Some(bytes) = self.backing.writable_vec(&mut self.json_layouts) {
                bytes.truncate(self.len);
                crate::go_append::go_slices_grow(bytes, additional);
                return;
            }
        }
        // The same growth over the shared slice: a length of `len + n` grown
        // from its capacity.
        self.detach_with_capacity(self.appended_capacity(additional));
    }

    /// Go `data = append(data, source...)` ([`Self::extend_from_slice`]).
    #[inline]
    pub(crate) fn go_extend_from_slice(&mut self, source: &[u8]) {
        self.extend_from_slice(source);
    }

    /// The capacity Go's `append` of `additional` bytes to these leaves.
    fn appended_capacity(&self, additional: usize) -> usize {
        let capacity = self.capacity();
        let needed = self.len.checked_add(additional).expect("SharedBytes length overflow");
        if needed <= capacity {
            return capacity;
        }
        tidb_goruntime::go_runtime::go_64_next_slice_capacity_for_element(
            needed,
            capacity,
            1,
            tidb_goruntime::go_runtime::GoSliceElementLayout::NoPointers,
        )
    }

    pub(crate) fn resize_preserving(&mut self, len: usize) {
        if len < self.len {
            self.json_layouts.take();
        }
        if self.start == 0 {
            if let Some(bytes) = self.backing.writable_vec(&mut self.json_layouts) {
                if len > bytes.len() {
                    // Go `make([]byte, size)` when the capacity is short.
                    if len > bytes.capacity() {
                        bytes.reserve_exact(len - bytes.len());
                    }
                    bytes.resize(len, 0);
                }
                self.len = len;
                return;
            }
        }
        if len > self.capacity() {
            self.detach_with_capacity(len);
        }
        self.ensure_initialized(len);
        self.len = len;
    }

    #[inline]
    pub(crate) fn extend_from_slice(&mut self, source: &[u8]) {
        if source.is_empty() {
            return;
        }
        let old_len = self.len;
        let new_len = old_len
            .checked_add(source.len())
            .expect("SharedBytes length overflow");
        if self.start == 0 {
            if let Backing::Owned(bytes) = &mut self.backing {
                Self::append_to_vec(bytes, old_len, new_len, source);
                self.len = new_len;
                return;
            }
            // A retained buffer without any active snapshots can append
            // directly to its vector, including after a reset.
            if let Backing::Retained { bytes: owner, tail } = &mut self.backing {
                clear_retained_json_bytes(&mut self.json_layouts);
                if let Some(bytes) = Arc::get_mut(owner) {
                    Self::append_to_vec(bytes, old_len, new_len, source);
                    if tail.as_ref().is_some_and(|old| new_len >= old.len()) {
                        *tail = None;
                    }
                    self.len = new_len;
                    return;
                }
            }
        }
        self.extend_from_slice_slow(source, old_len, new_len);
    }

    #[inline]
    fn append_to_vec(bytes: &mut Vec<u8>, old_len: usize, new_len: usize, source: &[u8]) {
        if let Some(destination) = bytes.get_mut(old_len..new_len) {
            let width = source.len();
            if width >= 64 {
                match width {
                    64 => destination[..64].copy_from_slice(&source[..64]),
                    128 => destination[..128].copy_from_slice(&source[..128]),
                    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
                    1024 | 2048 | 3072 | 4095..=8193 => {
                        Self::copy_wide(destination, source);
                    }
                    _ => destination.copy_from_slice(source),
                }
                return;
            }
            match width {
                1 => destination[0] = source[0],
                2..=3 => Self::copy_ends::<2>(destination, source),
                4..=7 => Self::copy_ends::<4>(destination, source),
                8..=15 => Self::copy_ends::<8>(destination, source),
                16..=31 => Self::copy_ends::<16>(destination, source),
                32..=63 => Self::copy_ends::<32>(destination, source),
                _ => destination.copy_from_slice(source),
            }
        } else {
            Self::grow_and_append(bytes, old_len, source);
        }
    }

    /// The host copy of several KiB is sensitive to size and alignment on
    /// Apple ARM64. Use smaller copies while refilling initialized storage.
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[inline(never)]
    fn copy_wide(destination: &mut [u8], source: &[u8]) {
        debug_assert_eq!(destination.len(), source.len());
        match source.len() {
            ..=3072 if (destination.as_ptr().addr() | source.as_ptr().addr()) % 64 != 0 => {
                destination.copy_from_slice(source);
            }
            1024 => {
                for (destination, source) in destination
                    .as_chunks_mut::<64>()
                    .0
                    .iter_mut()
                    .zip(source.as_chunks::<64>().0)
                {
                    *destination = *source;
                }
            }
            2048 | 3072 => {
                let (head, tail) = destination.split_first_chunk_mut::<64>().unwrap();
                let (source_head, source_tail) = source.split_first_chunk::<64>().unwrap();
                *head = *source_head;
                tail.copy_from_slice(source_tail);
            }
            width @ ..6144 => {
                // Avoid a split exactly on a KiB boundary.
                let split = width / 2 - 1;
                let (head, tail) = destination.split_at_mut(split);
                let (source_head, source_tail) = source.split_at(split);
                head.copy_from_slice(source_head);
                tail.copy_from_slice(source_tail);
            }
            width => {
                let unit = width / 4 - 1;
                for (out, input) in destination.chunks_mut(unit).zip(source.chunks(unit)) {
                    out.copy_from_slice(input);
                }
            }
        }
    }

    #[inline]
    fn copy_ends<const WIDTH: usize>(destination: &mut [u8], source: &[u8]) {
        // Two fixed-size copies cover widths from WIDTH to twice WIDTH.
        let tail = source.len() - WIDTH;
        destination[..WIDTH].copy_from_slice(&source[..WIDTH]);
        destination[tail..][..WIDTH].copy_from_slice(&source[tail..][..WIDTH]);
    }

    #[cold]
    fn grow_and_append(bytes: &mut Vec<u8>, old_len: usize, source: &[u8]) {
        bytes.truncate(old_len);
        crate::go_append::go_grow(bytes, source.len());
        bytes.extend_from_slice(source);
    }

    #[cold]
    fn extend_from_slice_slow(&mut self, source: &[u8], old_len: usize, new_len: usize) {
        let capacity = self.appended_capacity(source.len());
        if self.start == 0 {
            if let Backing::Retained { bytes: owner, tail } = &mut self.backing {
                if old_len == 0 && tail.is_none() && Arc::strong_count(owner) > 1 {
                    let mut bytes = Vec::with_capacity(capacity);
                    bytes.extend_from_slice(source);
                    let old = mem::replace(owner, Arc::new(bytes));
                    if new_len < old.len() {
                        *tail = Some(old);
                    }
                    self.len = new_len;
                    return;
                }
            }
            if let Some(bytes) = self.backing.writable_vec(&mut self.json_layouts) {
                if capacity > bytes.capacity() {
                    bytes.truncate(old_len);
                    bytes.reserve_exact(capacity - old_len);
                }
                if bytes.len() == old_len {
                    // The usual growth: nothing beyond the visible bytes, so
                    // the vector's own append writes the source once.
                    bytes.extend_from_slice(source);
                    self.len = new_len;
                    return;
                }
                if new_len > bytes.len() {
                    bytes.resize(new_len, 0);
                }
                bytes[old_len..new_len].copy_from_slice(source);
                self.len = new_len;
                return;
            }
        }
        if new_len > self.capacity() {
            self.detach_with_capacity(capacity);
        }
        self.ensure_initialized(new_len);
        self.len = new_len;
        self.with_write(|bytes| bytes[old_len..new_len].copy_from_slice(source));
    }

    /// Runs an append-only batch directly on an unaliased vector. Reset keeps
    /// initialized storage beyond the visible length; truncate that stale tail
    /// before appending, retaining capacity. Shared/frozen storage is untouched.
    pub(crate) fn append_owned(&mut self, append: impl FnOnce(&mut Vec<u8>)) -> bool {
        self.discard_deferred_tail();
        if self.start != 0 {
            return false;
        }
        let bytes = match &mut self.backing {
            Backing::Owned(bytes) => bytes,
            Backing::Retained { bytes: owner, tail } => {
                clear_retained_json_bytes(&mut self.json_layouts);
                if Arc::strong_count(owner) != 1 {
                    // The append keeps the visible prefix. The initialized
                    // tail will be discarded, so leave it with the snapshot.
                    let mut prefix = Vec::with_capacity(owner.capacity());
                    prefix.extend_from_slice(&owner[..self.len]);
                    *owner = Arc::new(prefix);
                }
                let Some(bytes) = Arc::get_mut(owner) else {
                    return false;
                };
                *tail = None;
                bytes
            }
            Backing::Shared(_) | Backing::Frozen(_) => return false,
        };
        self.json_layouts.take();
        bytes.truncate(self.len);
        append(bytes);
        self.len = bytes.len();
        true
    }

    pub(crate) fn append_json(&mut self, value: &BinaryJSON, width: usize) {
        self.discard_deferred_tail();
        let start = self.len;
        // The append leaves bytes covered by existing layout checks intact.
        if let Some(bytes) = (self.start == 0)
            .then(|| self.backing.writable_vec(&mut self.json_layouts))
            .flatten()
        {
            bytes.truncate(start);
            crate::go_append::go_grow(bytes, width);
            value.append_encoded_to(bytes);
            self.len = bytes.len();
        } else {
            self.go_extend_from_slice(&value.encoded());
        }

        if width < crate::column::Column::MIN_RETAINED_CELL_BYTES {
            return;
        }
        if let Some(layout) = value.known_layout() {
            self.remember_json_layout(start..self.len, layout);
        }
    }

    pub(crate) fn remember_json_layout(&mut self, range: Range<usize>, layout: EncodedJSONLayout) {
        if range.len() < crate::column::Column::MIN_RETAINED_CELL_BYTES || self.is_shared() {
            return;
        }
        if self.json_layouts.get_mut().is_none() {
            self.json_layouts = OnceLock::from(Box::default());
        }
        self.json_layouts
            .get_mut()
            .expect("JSON layout cache was initialized")
            .appended
            .insert(range, layout);
    }

    /// Go's `append(dst, src[begin:end]...)`, retaining the plain `Vec` fast
    /// path while still detaching a shallow alias before it writes itself.
    #[inline]
    pub(crate) fn extend_from_range(&mut self, source: &Self, range: Range<usize>) {
        assert!(range.start <= range.end, "invalid SharedBytes source range");
        assert!(
            range.end <= source.len,
            "SharedBytes source range exceeds length"
        );
        if range.is_empty() {
            return;
        }

        let same_shared_backing = match (&self.backing, &source.backing) {
            (Backing::Shared(destination), Backing::Shared(source)) => {
                Arc::ptr_eq(destination, source)
            }
            _ => false,
        };
        if same_shared_backing {
            let snapshot = source.read()[range].to_vec();
            self.extend_from_slice(&snapshot);
            return;
        }

        if self.start == 0 {
            if let Some(destination) = self.backing.writable_vec(&mut self.json_layouts) {
                if destination.len() == self.len {
                    crate::go_append::go_grow(destination, range.len());
                    let start = source.start + range.start;
                    let end = source.start + range.end;
                    match &source.backing {
                        Backing::Owned(bytes) => destination.extend_from_slice(&bytes[start..end]),
                        Backing::Retained { bytes, .. } => {
                            destination.extend_from_slice(&bytes[start..end])
                        }
                        Backing::Frozen(bytes) => destination.extend_from_slice(&bytes.bytes()[start..end]),
                        Backing::Shared(backing) => {
                            let bytes = Self::read_shared(backing);
                            destination.extend_from_slice(&bytes[start..end]);
                        }
                    }
                    self.len = destination.len();
                    return;
                }
            }
        }

        let bytes = source.read();
        self.extend_from_slice(&bytes[range]);
    }

    /// Appends bytes already in this buffer, preserving any retained views.
    pub(crate) fn extend_from_within(&mut self, range: Range<usize>) {
        assert!(range.start <= range.end && range.end <= self.len);
        if range.is_empty() {
            return;
        }
        self.discard_deferred_tail();
        if self.start == 0 {
            if let Some(bytes) = self.backing.writable_vec(&mut self.json_layouts) {
                bytes.truncate(self.len);
                crate::go_append::go_grow(bytes, range.len());
                bytes.extend_from_within(range);
                self.len = bytes.len();
                return;
            }
        }
        let source = self.read()[range].to_vec();
        self.extend_from_slice(&source);
    }

    /// [`Self::extend_from_slice`] for a cell whose width is known at compile
    /// time (a fixed-length column element). The owned paths -- a reused
    /// buffer with room after a reset, or plain growth -- copy `N` bytes as
    /// one store instead of a runtime-length copy call per cell; every other
    /// shape takes the general path.
    #[inline]
    pub(crate) fn extend_from_array<const N: usize>(&mut self, source: &[u8; N]) {
        if self.start == 0 {
            if let Backing::Owned(bytes) = &mut self.backing {
                let old_len = self.len;
                let new_len = old_len + N;
                Self::append_to_vec(bytes, old_len, new_len, source);
                self.len = new_len;
                return;
            }
        }
        self.extend_from_slice(source);
    }

    #[inline]
    pub(crate) fn try_append_prefixed<const N: usize>(
        &mut self,
        prefix: &[u8; N],
        payload: &[u8],
    ) -> bool {
        if self.start != 0 {
            return false;
        }
        let (bytes, tail) = match &mut self.backing {
            Backing::Owned(bytes) => (bytes, None),
            Backing::Retained { bytes, tail } => {
                let Some(bytes) = Arc::get_mut(bytes) else {
                    return false;
                };
                (bytes, Some(tail))
            }
            _ => return false,
        };
        let Some(end) = self
            .len
            .checked_add(N)
            .and_then(|start| start.checked_add(payload.len()))
        else {
            return false;
        };
        let Some(destination) = bytes.get_mut(self.len..end) else {
            return false;
        };
        let (header, body) = destination.split_at_mut(N);
        header.copy_from_slice(prefix);
        match payload.len() {
            8..=15 => Self::copy_ends::<8>(body, payload),
            16..=31 => Self::copy_ends::<16>(body, payload),
            _ => body.copy_from_slice(payload),
        }
        if let Some(tail) = tail {
            if tail.as_ref().is_some_and(|old| end >= old.len()) {
                *tail = None;
            }
        }
        self.len = end;
        true
    }

    pub(crate) fn fill(&mut self, value: u8) {
        self.with_write(|bytes| bytes.fill(value));
    }

    pub(crate) fn set(&mut self, index: usize, value: u8) {
        self.with_write(|bytes| bytes[index] = value);
    }

    pub(crate) fn copy_from_slice(&mut self, range: Range<usize>, source: &[u8]) -> usize {
        assert!(range.start <= range.end, "invalid SharedBytes copy range");
        assert!(range.end <= self.len, "SharedBytes copy exceeds length");
        let copied = (range.end - range.start).min(source.len());
        self.with_write(|bytes| {
            bytes[range.start..range.start + copied].copy_from_slice(&source[..copied]);
        });
        copied
    }

    pub(crate) fn share_range(&mut self, start: usize, end: usize) -> Self {
        assert!(start <= end, "invalid SharedBytes view range");
        assert!(end <= self.len, "SharedBytes view exceeds length");
        self.json_layouts.take();
        self.materialize_deferred_tail();
        let previous = mem::replace(&mut self.backing, Backing::Owned(Vec::new()));
        let backing = match previous {
            Backing::Owned(bytes) => Arc::new(RwLock::new(bytes)),
            Backing::Retained { bytes, tail: _ } => Arc::new(RwLock::new(
                Arc::try_unwrap(bytes).unwrap_or_else(|owner| copy_backing(&owner)),
            )),
            Backing::Shared(backing) => backing,
            Backing::Frozen(bytes) => {
                // Immutable on both sides: the slice header is enough.
                self.backing = Backing::Frozen(bytes.clone());
                return Self {
                    backing: Backing::Frozen(bytes),
                    start: self.start + start,
                    len: end - start,
                    json_layouts: OnceLock::new(),
                };
            }
        };
        self.backing = Backing::Shared(Arc::clone(&backing));
        Self {
            backing: Backing::Shared(backing),
            start: self.start + start,
            len: end - start,
            json_layouts: OnceLock::new(),
        }
    }

    pub(crate) fn deep_copy(&self) -> Self {
        Self::from_vec(self.snapshot())
    }

    #[cfg(test)]
    pub(crate) fn backing_ptr_eq(&self, other: &Self) -> bool {
        match (&self.backing, &other.backing) {
            (Backing::Shared(left), Backing::Shared(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    pub(crate) fn is_shared(&self) -> bool {
        matches!(&self.backing, Backing::Shared(_))
    }

    fn ensure_initialized(&mut self, len: usize) {
        self.with_mutable_backing(len, |bytes, start| {
            let required = start
                .checked_add(len)
                .expect("SharedBytes backing length overflow");
            if required > bytes.len() {
                bytes.resize(required, 0);
            }
        });
    }

    pub(crate) fn with_write<R>(&mut self, update: impl FnOnce(&mut [u8]) -> R) -> R {
        self.json_layouts.take();
        let len = self.len;
        self.with_mutable_backing(len, |bytes, start| update(&mut bytes[start..start + len]))
    }

    fn with_mutable_backing<R>(
        &mut self,
        mut minimum_capacity: usize,
        update: impl FnOnce(&mut Vec<u8>, usize) -> R,
    ) -> R {
        let start = self.start;
        if let Some(bytes) = self.backing.writable_vec(&mut self.json_layouts) {
            return update(bytes, start);
        }

        match &self.backing {
            Backing::Owned(_) | Backing::Retained { .. } => unreachable!("owned backing returned above"),
            Backing::Shared(backing) => match backing.try_write() {
                Ok(mut bytes) => return update(&mut bytes, start),
                Err(TryLockError::Poisoned(poisoned)) => {
                    let mut bytes = poisoned.into_inner();
                    return update(&mut bytes, start);
                }
                Err(TryLockError::WouldBlock) => {}
            },
            Backing::Frozen(..) => {
                minimum_capacity = minimum_capacity.max(self.capacity());
            }
        }

        self.detach_with_capacity(minimum_capacity.max(self.len));
        match &mut self.backing {
            Backing::Owned(bytes) => update(bytes, 0),
            Backing::Shared(_) | Backing::Retained { .. } | Backing::Frozen(..) => {
                unreachable!("contention detaches only the mutating header")
            }
        }
    }

    fn detach_with_capacity(&mut self, capacity: usize) {
        clear_retained_json_bytes(&mut self.json_layouts);
        let mut replacement = Vec::with_capacity(capacity);
        match &self.backing {
            Backing::Frozen(FrozenBytes::Raw { bytes, compacted: true }) => {
                replacement.extend_from_slice(&bytes[self.start..]);
            }
            Backing::Frozen(FrozenBytes::RelocatedRaw(bytes)) => {
                replacement.extend_from_slice(&bytes[self.start..self.start + self.len]);
                replacement.extend_from_slice(&bytes[self.len..]);
            }
            Backing::Frozen(FrozenBytes::String { bytes, capacity_end }) if *capacity_end != 0 => {
                replacement.extend_from_slice(&bytes[self.start..*capacity_end]);
            }
            Backing::Frozen(FrozenBytes::Gathered(bytes)) => {
                replacement.extend_from_slice(&bytes.selected[self.start..]);
                replacement.extend_from_slice(&bytes.tail);
            }
            _ => replacement.extend_from_slice(&self.read()),
        }
        let len = self.len;
        *self = Self::from_vec(replacement);
        self.len = len;
    }

    #[inline]
    fn materialize_deferred_tail(&mut self) {
        if matches!(&self.backing, Backing::Retained { tail: Some(_), .. }) {
            self.backing.writable_vec(&mut self.json_layouts);
        } else if self.has_deferred_tail() {
            self.detach_with_capacity(self.capacity());
        }
    }

    #[inline]
    fn discard_deferred_tail(&mut self) {
        if self.has_deferred_tail() {
            self.copy_visible_bytes();
        }
    }

    #[cold]
    fn copy_visible_bytes(&mut self) {
        let mut bytes = Vec::with_capacity(self.capacity());
        bytes.extend_from_slice(&self.read());
        *self = Self::from_vec(bytes);
    }

    #[inline]
    fn has_deferred_tail(&self) -> bool {
        matches!(&self.backing, Backing::Frozen(bytes) if bytes.has_deferred_tail())
    }

    fn read_shared(backing: &RwLock<Vec<u8>>) -> RwLockReadGuard<'_, Vec<u8>> {
        backing
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Clone for SharedBytes {
    fn clone(&self) -> Self {
        self.deep_copy()
    }
}

impl fmt::Debug for SharedBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.read().as_ref(), formatter)
    }
}

impl PartialEq for SharedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedBytes {}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_endian = "little")]
    #[test]
    fn replacing_vector_data_reuses_unique_owner_and_preserves_readers() {
        for misalignment in [0, 1] {
            check_vector_owner_reuse(misalignment);
        }
    }

    #[cfg(target_endian = "little")]
    fn check_vector_owner_reuse(misalignment: usize) {
        let encode = |value| {
            let mut bytes = Vec::<u8>::with_capacity(12);
            let shift = bytes.as_ptr().align_offset(align_of::<f32>()) + misalignment;
            bytes.resize(shift, 0);
            VectorFloat32::must_create(vec![value]).serialize_to(&mut bytes);
            bytes::Bytes::from(bytes).slice(shift..)
        };
        let pointer = |data: &SharedBytes| match &data.backing {
            Backing::Frozen(FrozenBytes::Vector(owner)) => Arc::as_ptr(owner),
            _ => panic!("expected shared vector storage"),
        };
        let mut data = SharedBytes::default();
        let first_frame = encode(1.0);
        data.replace_vector_bytes(first_frame.clone());
        let first_owner = pointer(&data);
        {
            let first = data.retained_vector(0..8).unwrap().unwrap();
            assert_eq!(first.elements(), &[1.0]);
            if misalignment == 0 {
                assert_eq!(
                    first.elements().as_ptr().cast::<u8>(),
                    first_frame[4..].as_ptr(),
                );
            }
        }
        let second_frame = encode(2.0);
        data.replace_vector_bytes(second_frame.clone());
        assert_eq!(pointer(&data), first_owner);
        let retained = data.retained_vector(0..8).unwrap().unwrap();
        assert_eq!(retained.elements(), &[2.0]);

        data.replace_vector_bytes(second_frame);
        assert_eq!(pointer(&data), first_owner);
        let repeated = data.retained_vector(0..8).unwrap().unwrap();
        assert_eq!(repeated.elements().as_ptr(), retained.elements().as_ptr());

        let third_frame = encode(3.0);
        data.replace_vector_bytes(third_frame.clone());
        assert_ne!(pointer(&data), first_owner);
        assert_eq!(
            data.retained_vector(0..8).unwrap().unwrap().elements(),
            &[3.0]
        );
        assert_eq!(retained.elements(), &[2.0]);
        assert_eq!(repeated.elements(), &[2.0]);
        if misalignment == 0 {
            assert!(data.truncate_frozen_prefix(0));
            assert_eq!(data.capacity(), first_frame.len());
            data.resize_preserving(first_frame.len());
            assert_eq!(&*data.read(), third_frame.as_ref());
        }
    }

    #[test]
    fn append_json_preserves_only_known_layouts_at_their_cell_ranges() {
        let known = BinaryJSON::parse(&format!("\"{}\"", "界".repeat(1500))).unwrap();
        let raw = BinaryJSON::from_encoded_parts(known.type_code(), known.value());
        let mut bad_bytes = known.value().to_vec();
        *bad_bytes.last_mut().unwrap() = 0xff;
        let invalid = BinaryJSON::from_encoded_parts(known.type_code(), bad_bytes);
        assert!(invalid.element_depth().is_err());
        let width = known.value().len() + 1;
        let mut bytes = SharedBytes::default();
        for value in [&known, &raw, &known, &invalid] {
            bytes.append_json(value, width);
        }

        let recorded = |bytes: &SharedBytes, row| {
            bytes
                .json_layouts
                .get()?
                .get(&(row * width..(row + 1) * width))
                .map(EncodedJSONLayout::is_canonical)
        };
        assert_eq!(recorded(&bytes, 0), Some(true));
        assert_eq!(recorded(&bytes, 1), None);
        assert_eq!(recorded(&bytes, 2), Some(true));
        assert_eq!(recorded(&bytes, 3), Some(false));

        bytes.reset();
        bytes.append_json(&raw, width);
        assert_eq!(recorded(&bytes, 0), None);
        assert_eq!(recorded(&bytes, 3), None);
    }

    #[test]
    fn short_string_copies_keep_live_bytes_when_buffer_slots_wrap() {
        let held = std::thread::spawn(|| {
            const WIDTH: usize = 33;
            let mut source = SharedBytes::from_vec(vec![0xff; WIDTH]);
            source.set(0, 0);
            let mut held = vec![source.get_string(0..WIDTH).slice(..1)];
            for value in 1..=STRING_COPY_SLOTS {
                source.set(0, value as u8);
                held.push(source.get_string(0..WIDTH));
            }

            // All slots were occupied on the last copy. A sliced result
            // still owns slot zero, but slot one is now available to reuse.
            let reused_address = held[1].as_bytes().as_ptr();
            drop(held.remove(1));
            source.set(0, 0xaa);
            let reused = source.get_string(0..WIDTH);
            assert_eq!(reused.as_bytes().as_ptr(), reused_address);
            assert_eq!(reused.as_bytes()[..2], [0xaa, 0xff]);
            held
        })
        .join()
        .unwrap();

        assert_eq!(held[0].as_bytes(), [0]);
        for (index, value) in held.iter().enumerate().skip(1) {
            assert_eq!(value.as_bytes()[..2], [index as u8 + 1, 0xff]);
        }
    }

    #[test]
    fn shared_try_read_reports_a_busy_writer() {
        let mut bytes = SharedBytes::from_vec(b"abcd".to_vec());
        let alias = bytes.share_range(1, 3);
        let Backing::Shared(backing) = &bytes.backing else {
            panic!("the original and alias must share their backing");
        };
        let writer = backing.write().unwrap();
        assert!(alias.try_read().is_none());
        drop(writer);
        assert_eq!(alias.try_read().unwrap().as_ref(), b"bc");
    }

    #[test]
    fn owned_refill_preserves_initialized_tail_until_overwritten() {
        let mut bytes = SharedBytes::from_vec(b"abcd".to_vec());
        bytes.reset();
        bytes.extend_from_slice(b"xy");
        bytes.resize_preserving(4);
        assert_eq!(bytes.read().as_ref(), b"xycd");

        bytes.truncate(2);
        bytes.extend_from_slice(b"12345");
        assert_eq!(bytes.read().as_ref(), b"xy12345");
    }

    #[test]
    fn short_packed_appends_keep_payload_and_initialized_tail() {
        let old: Vec<_> = (0..300).map(|index| (index * 17) as u8).collect();
        let new: Vec<_> = (0..300).map(|index| (index * 31 + 97) as u8).collect();
        for width in (0..=129).chain([257]) {
            for retained in [false, true] {
                let mut bytes = SharedBytes::from_vec(old.clone());
                if retained {
                    bytes.retain_for_slices();
                }
                bytes.reset();
                bytes.extend_from_slice(&new[..width]);
                assert_eq!(bytes.read().as_ref(), &new[..width]);
                bytes.resize_preserving(old.len());
                assert_eq!(&bytes.read()[width..], &old[width..]);
            }
        }
    }

    #[test]
    fn wide_packed_appends_preserve_the_prefix_and_initialized_tail() {
        let old: Vec<_> = (0..8400)
            .map(|index| (index * 17 + index / 32 + 3) as u8)
            .collect();
        let new: Vec<_> = (0..8400)
            .map(|index| (index * 31 + index / 16 + 97) as u8)
            .collect();
        let source_aligned = new.as_ptr().addr().wrapping_neg() % 64;
        for width in [
            1024, 2048, 3072, 4094, 4095, 4096, 4097, 5120, 6143, 6144, 7168, 8191, 8192,
            8193, 8194,
        ] {
            for (retained, source_shift, destination_shift) in
                [(false, 0, 0), (true, 0, 0), (true, 1, 17), (true, 16, 0)]
            {
                let mut bytes = SharedBytes::from_vec(old.clone());
                let prefix = bytes.read().as_ptr().addr().wrapping_neg() % 64 + destination_shift;
                let start = source_aligned + source_shift;
                let source = &new[start..start + width];
                if retained {
                    bytes.retain_for_slices();
                }
                bytes.reset();
                bytes.extend_from_slice(&new[..prefix]);
                bytes.extend_from_slice(source);
                let end = prefix + width;
                assert_eq!(&bytes.read()[..prefix], &new[..prefix]);
                assert_eq!(&bytes.read()[prefix..end], source);
                bytes.resize_preserving(old.len());
                assert_eq!(&bytes.read()[end..], &old[end..]);
            }
        }
    }

    #[test]
    fn retained_refill_preserves_the_unwritten_tail() {
        let mut bytes = SharedBytes::from_vec(b"abcdefgh".to_vec());
        bytes.retain_for_slices();
        let original = bytes.get_string(0..8);
        bytes.reset();
        bytes.extend_from_slice(b"xy");
        assert!(matches!(&bytes.backing, Backing::Retained { tail: Some(_), .. }));

        let partial = bytes.get_string(0..2);
        bytes.extend_from_slice(b"z");
        bytes.resize_preserving(8);
        assert_eq!(bytes.read().as_ref(), b"xyzdefgh");
        assert_eq!(original.as_bytes(), b"abcdefgh");
        assert_eq!(partial.as_bytes(), b"xy");
    }

    #[test]
    fn retained_refill_releases_tail_after_full_overwrite() {
        let mut bytes = SharedBytes::from_vec(b"abcdefgh".to_vec());
        bytes.retain_for_slices();
        let original = bytes.get_string(0..8);
        bytes.reset();
        bytes.extend_from_slice(b"xy");
        bytes.extend_from_slice(b"123456");
        assert!(matches!(&bytes.backing, Backing::Retained { tail: None, .. }));
        assert_eq!(bytes.read().as_ref(), b"xy123456");
        assert_eq!(original.as_bytes(), b"abcdefgh");
    }

    #[test]
    fn aliases_promote_lazily_and_detach_on_growth() {
        let mut source = SharedBytes::with_capacity(8);
        source.extend_from_slice(b"abc");
        assert!(!source.is_shared());

        let mut alias = source.share_range(1, 3);
        assert!(source.is_shared());
        assert!(source.backing_ptr_eq(&alias));
        alias.set(0, b'X');
        assert_eq!(source.read().as_ref(), b"aXc");

        alias.extend_from_slice(b"defghijk");
        assert!(!alias.backing_ptr_eq(&source));
        alias.set(0, b'Y');
        assert_eq!(source.read().as_ref(), b"aXc");
    }

    #[test]
    fn clone_is_an_owned_deep_copy() {
        let source = SharedBytes::from_vec(b"abc".to_vec());
        let mut copy = source.clone();
        copy.set(0, b'X');
        assert_eq!(source.read().as_ref(), b"abc");
        assert_eq!(copy.read().as_ref(), b"Xbc");
        assert!(!source.is_shared());
    }

    #[test]
    fn frozen_bytes_are_shared_until_mutated() {
        let payload = bytes::Bytes::from(vec![1u8, 2, 3, 4, 5, 6]);
        let mut column = SharedBytes::from_bytes(payload.slice(1..5));
        assert_eq!(&*column.read(), &[2, 3, 4, 5]);
        assert_eq!(column.capacity(), 4);
        let view = column.share_range(1, 3);
        assert_eq!(&*view.read(), &[3, 4]);
        assert!(matches!(view.backing, Backing::Frozen(..)));
        // A write detaches the writer into an owned copy; the view and the
        // payload keep their bytes.
        column.set(0, 9);
        assert_eq!(&*column.read(), &[9, 3, 4, 5]);
        assert!(matches!(column.backing, Backing::Owned(_)));
        assert_eq!(&*view.read(), &[3, 4]);
        assert_eq!(&payload[..], &[1, 2, 3, 4, 5, 6]);
        let mut grown = SharedBytes::from_bytes(payload.clone());
        grown.extend_from_slice(&[7]);
        assert_eq!(&*grown.read(), &[1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn ordinary_growth_stays_on_the_owned_vec_fast_path() {
        let mut bytes = SharedBytes::with_capacity(1);
        bytes.extend_from_slice(b"an ordinary append that must grow");
        bytes.reserve(128);
        bytes.resize_preserving(96);
        assert!(!bytes.is_shared());
    }

    #[test]
    fn const_width_appends_reuse_room_grow_and_detach_like_slice_appends() {
        let mut bytes = SharedBytes::with_capacity(4);
        bytes.extend_from_array(&[1u8; 8]);
        bytes.extend_from_array(&[2u8; 8]);
        assert_eq!(
            &*bytes.read(),
            &[1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2]
        );
        // A reset keeps the vector's bytes behind a zero visible length; the
        // next appends overwrite in place, then run past the old end.
        bytes.reset();
        bytes.extend_from_array(&[3u8; 8]);
        assert_eq!(&*bytes.read(), &[3u8; 8]);
        bytes.truncate(4);
        bytes.extend_from_array(&[4u8; 8]);
        assert_eq!(&*bytes.read(), &[3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4]);
        bytes.extend_from_array(&[5u8; 8]);
        assert_eq!(bytes.len(), 20);
        assert_eq!(&bytes.read()[12..], &[5u8; 8]);
        assert!(!bytes.is_shared());
        // A shared backing takes the general path: the alias keeps its view.
        let alias = bytes.share_range(0, 4);
        bytes.extend_from_array(&[6u8; 4]);
        assert_eq!(&*alias.read(), &[3u8; 4]);
        assert_eq!(&bytes.read()[20..], &[6u8; 4]);
        bytes.extend_from_array(&[7u8; 64]);
        assert_eq!(&*alias.read(), &[3u8; 4]);
        assert_eq!(&bytes.read()[24..], &[7u8; 64]);
        assert!(!bytes.backing_ptr_eq(&alias));
    }

    #[test]
    fn live_sibling_reader_detaches_mutating_header() {
        let mut source = SharedBytes::with_capacity(8);
        source.extend_from_slice(b"abc");
        let alias = source.share_range(0, 3);
        let reader = alias.read();

        source.set(0, b'X');

        assert_eq!(reader.as_ref(), b"abc");
        assert_eq!(source.read().as_ref(), b"Xbc");
        assert!(!source.backing_ptr_eq(&alias));
    }

    #[test]
    fn retained_slices_survive_writes_and_mutable_aliasing() {
        let mut source = SharedBytes::with_capacity(16);
        source.extend_from_slice(b"abcd");
        source.retain_for_slices();
        let retained = source.retained_slice(1..3).unwrap();
        assert_eq!(retained.as_ref().as_ptr(), source.read()[1..].as_ptr());

        source.set(1, b'X');
        assert_eq!(source.capacity(), 16);
        let current = source.retained_slice(0..4).unwrap();
        let mut alias = source.share_range(0, 4);
        alias.set(2, b'Y');

        assert_eq!(retained.as_ref(), b"bc");
        assert_eq!(current.as_ref(), b"aXcd");
        assert_eq!(&*source.read(), b"aXYd");
        assert!(source.retained_slice(0..4).is_none());
    }
}
