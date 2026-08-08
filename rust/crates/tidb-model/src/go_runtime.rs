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

//! Native representations of Go runtime states used by `pkg/meta/model`.
//!
//! Persisted codecs live in `serde_helpers`; this module owns non-wire states
//! such as a typed-nil pointer held inside an `any` interface. Shared pointer
//! and slice-backing representations are added here as model clone surfaces
//! migrate away from Rust-only deep ownership.

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

// Go 1.25's 64-bit allocator size classes. TiDB's supported server targets
// are 64-bit; `growslice` rounding is defined by these byte classes and, for
// scanned allocations only, the runtime's 8-byte malloc-header threshold.
const GO_64_SIZE_CLASSES: &[usize] = &[
    8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352,
    384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048,
    2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728,
    10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672,
    32768,
];

/// Whether a Go slice's element type contains pointers and therefore uses a
/// scanned allocation. Above the malloc-header threshold, scanned and noscan
/// slices with the same element width can have different observable caps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoSliceElementLayout {
    /// The element type contains no pointers.
    NoPointers,
    /// The element type contains at least one pointer.
    PointerBearing,
}

fn go_64_round_allocation(bytes: usize, layout: GoSliceElementLayout) -> usize {
    const MALLOC_HEADER: usize = 8;
    const MIN_HEADER_SIZE: usize = 8 * 64;
    const MAX_SMALL_SIZE: usize = 32_768;
    const PAGE_SIZE: usize = 8_192;

    if bytes <= MAX_SMALL_SIZE - MALLOC_HEADER {
        let header =
            usize::from(layout == GoSliceElementLayout::PointerBearing && bytes > MIN_HEADER_SIZE)
                * MALLOC_HEADER;
        let requested = bytes + header;
        return GO_64_SIZE_CLASSES
            .iter()
            .copied()
            .find(|class| *class >= requested)
            .expect("small Go allocation has a size class")
            - header;
    }
    bytes
        .checked_add(PAGE_SIZE - 1)
        .expect("Go slice allocation overflow")
        & !(PAGE_SIZE - 1)
}

pub(crate) fn go_64_next_slice_capacity_for_element(
    new_len: usize,
    old_capacity: usize,
    element_size: usize,
    layout: GoSliceElementLayout,
) -> usize {
    let double_capacity = old_capacity
        .checked_mul(2)
        .expect("Go slice capacity overflow");
    let mut candidate = if new_len > double_capacity {
        new_len
    } else if old_capacity < 256 {
        double_capacity
    } else {
        let mut grown = old_capacity;
        loop {
            grown = grown
                .checked_add((grown + 3 * 256) >> 2)
                .expect("Go slice capacity overflow");
            if grown >= new_len {
                break grown;
            }
        }
    };
    if candidate < new_len {
        candidate = new_len;
    }
    let bytes = candidate
        .checked_mul(element_size)
        .expect("Go slice allocation overflow");
    go_64_round_allocation(bytes, layout) / element_size
}

fn go_64_next_slice_capacity(new_len: usize, old_capacity: usize) -> usize {
    go_64_next_slice_capacity_for_element(
        new_len,
        old_capacity,
        8,
        GoSliceElementLayout::PointerBearing,
    )
}

fn go_64_pointer_slice_decode_capacity(mut capacity: usize, decoded_len: usize) -> usize {
    while capacity < decoded_len {
        capacity = go_64_next_slice_capacity(capacity + 1, capacity);
    }
    capacity
}

pub(crate) fn go_64_slice_decode_capacity(
    mut capacity: usize,
    decoded_len: usize,
    element_size: usize,
    layout: GoSliceElementLayout,
) -> usize {
    while capacity < decoded_len {
        capacity =
            go_64_next_slice_capacity_for_element(capacity + 1, capacity, element_size, layout);
    }
    capacity
}

/// A Go `any` value at a `*T` type-assertion boundary.
///
/// `Typed(None)` is an interface containing a typed nil `*T`; `Other` covers
/// an untyped nil or any other dynamic type. Go type assertions distinguish
/// these states before comparing pointer values.
#[derive(Clone, Copy, Debug)]
pub enum GoPointerAny<'a, T> {
    /// The assertion to `*T` succeeds, with either a nil or non-nil pointer.
    Typed(Option<&'a T>),
    /// The assertion to `*T` fails.
    Other,
}

/// A Go `time.Time` produced from Unix milliseconds, retaining the full
/// `int64` millisecond domain even when Chrono cannot represent the year.
///
/// Model rules that only compare or carry a timestamp must not silently turn
/// an out-of-range Go time into the Unix epoch. Callers that specifically need
/// Chrono opt into the fallible conversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GoTime {
    unix_millis: i64,
    location: GoTimeLocation,
}

/// Location identity carried by model times created by Go source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GoTimeLocation {
    /// Go's zero `time.Time` uses UTC.
    Utc,
    /// `time.UnixMilli` uses the mutable process-local location.
    Local,
}

impl Default for GoTime {
    fn default() -> Self {
        Self {
            // 0001-01-01T00:00:00Z relative to Unix epoch.
            unix_millis: -62_135_596_800_000,
            location: GoTimeLocation::Utc,
        }
    }
}

/// A concurrency-safe Go pointer whose clones retain pointee identity.
///
/// Go metadata clone methods deliberately mix deep-cloned and shallow-shared
/// pointers. Rust owned values cannot express the latter. This handle makes
/// the sharing explicit and exposes guards instead of an unsafe `Deref` that
/// could outlive synchronization.
///
/// Snapshot-based formatting/serialization requires the model value's `Clone`
/// implementation to copy fields without re-entering this same handle. Model
/// clone implementations that need other shared values snapshot them in a
/// fixed source-field order.
pub struct GoShared<T>(Arc<RwLock<T>>);

impl<T> GoShared<T> {
    /// Allocates a new Go pointer.
    #[must_use]
    pub fn new(value: T) -> Self {
        Self(Arc::new(RwLock::new(value)))
    }

    /// Reads the pointee. Go mutexes do not poison after a panic, so recover
    /// the inner guard when Rust's lock reports poisoning.
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Mutates the pointee through every shallow alias.
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Reports Go pointer identity.
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Allocates an independent pointer using Rust value `Clone`.
    ///
    /// This is a representation primitive, not a model source-clone claim;
    /// selective Go clone methods use explicit field policies instead.
    #[must_use]
    pub fn clone_rust_value(&self) -> Self
    where
        T: Clone,
    {
        Self::new(self.read().clone())
    }
}

impl<T> Clone for GoShared<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: Clone + std::fmt::Debug> std::fmt::Debug for GoShared<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.read().clone();
        formatter.debug_tuple("GoShared").field(&snapshot).finish()
    }
}

impl<T: Clone + PartialEq> GoShared<T> {
    /// Explicit deep-value comparison. Source pointer equality remains
    /// [`Self::ptr_eq`]; named operations prevent Rust derives from silently
    /// selecting one of two different Go rules.
    #[must_use]
    pub fn deep_value_eq(&self, other: &Self) -> bool {
        if self.ptr_eq(other) {
            return true;
        }
        // Snapshot in separate scopes: acquiring both locks in caller-defined
        // order would let concurrent reversed comparisons deadlock.
        let left = self.read().clone();
        let right = other.read().clone();
        left == right
    }
}

impl<T: Clone + serde::Serialize> serde::Serialize for GoShared<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let snapshot = self.read().clone();
        serde::Serialize::serialize(&snapshot, serializer)
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for GoShared<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <T as serde::Deserialize>::deserialize(deserializer).map(Self::new)
    }
}

/// A Go slice header with a shared mutable backing array.
///
/// Cloning copies only `(data,len,cap)`, so element replacement is visible
/// through sibling headers while changing one header's length is not. The
/// wrapper intentionally exposes only operations whose backing behavior is
/// independent of Go's element-size-specific `growslice` capacity policy.
pub struct GoSharedSlice<T> {
    backing: Option<Arc<RwLock<Vec<T>>>>,
    start: usize,
    len: usize,
    capacity: usize,
}

impl<T> GoSharedSlice<T> {
    /// Constructs an allocated slice, retaining the source vector capacity.
    #[must_use]
    pub fn from_vec(values: Vec<T>) -> Self {
        let len = values.len();
        Self {
            backing: Some(Arc::new(RwLock::new(values))),
            start: 0,
            len,
            // A Rust Vec's spare capacity is uninitialized, whereas every Go
            // slice slot through cap is zero-initialized and resliceable. The
            // ordinary conversion therefore exposes only the initialized
            // range. Source call sites that own a larger cap use the explicit
            // initialized-capacity constructor below.
            capacity: len,
        }
    }

    /// Constructs a source slice with a larger, fully initialized capacity.
    #[must_use]
    pub fn from_vec_with_capacity(mut values: Vec<T>, capacity: usize) -> Self
    where
        T: Default,
    {
        assert!(capacity >= values.len(), "Go slice cap is smaller than len");
        let len = values.len();
        values.resize_with(capacity, T::default);
        Self {
            backing: Some(Arc::new(RwLock::new(values))),
            start: 0,
            len,
            capacity,
        }
    }

    /// Returns whether the Go slice is non-nil.
    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.backing.is_some()
    }

    /// Returns the source slice length (nil and allocated-empty are both 0).
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the source length is zero.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the source slice capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Reports whether two headers point into the same backing array.
    #[must_use]
    pub fn backing_ptr_eq(&self, other: &Self) -> bool {
        match (&self.backing, &other.backing) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    /// Copies the visible elements into a fresh outer backing array.
    #[must_use]
    pub fn copy_outer(&self) -> Self
    where
        T: Clone,
    {
        Self::from_vec(self.snapshot())
    }

    /// Go `slices.Clone` for a source element size on the supported 64-bit
    /// runtime. Nil remains nil; allocated-empty gets a fresh allocated cap-0
    /// backing; non-empty inputs allocate through `append`/`growslice` rather
    /// than forcing `cap == len`.
    #[must_use]
    pub fn slices_clone(&self, element_size: usize, layout: GoSliceElementLayout) -> Self
    where
        T: Clone + Default,
    {
        if !self.is_allocated() {
            return Self::default();
        }
        let values = self.snapshot();
        if values.is_empty() {
            // Go 1.25 uses append(S{}, s...), not s[:0:0], specifically so a
            // zero-length Clone does not keep the source backing alive.
            return Self::from_vec(Vec::new());
        }
        let capacity = go_64_next_slice_capacity_for_element(values.len(), 0, element_size, layout);
        Self::from_vec_with_capacity(values, capacity)
    }

    /// Appends one element using Go 1.25's 64-bit growslice capacity rule.
    /// Writes within capacity stay visible through sibling headers; growth
    /// detaches only this header.
    pub fn push_go(&mut self, value: T, element_size: usize, layout: GoSliceElementLayout)
    where
        T: Clone + Default,
    {
        let index = self.len;
        let grown_capacity = if index < self.capacity {
            self.capacity
        } else {
            go_64_next_slice_capacity_for_element(index + 1, self.capacity, element_size, layout)
        };
        self.prepare_decode_slot(index, grown_capacity);
        self.set(index, value);
    }

    /// Clones the visible element values. Pointer handles contained in `T`
    /// retain their own pointee identity.
    #[must_use]
    pub fn snapshot(&self) -> Vec<T>
    where
        T: Clone,
    {
        let Some(backing) = &self.backing else {
            return Vec::new();
        };
        let values = backing
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start..self.start + self.len].to_vec()
    }

    /// Maps the visible elements while holding one read guard. The callback
    /// must not re-enter this same backing; model clone implementations use it
    /// to distinguish an ordinary field read from the type's source-named
    /// `Clone` policy.
    #[must_use]
    pub fn map_visible<U>(&self, mut map: impl FnMut(&T) -> U) -> Vec<U> {
        let Some(backing) = &self.backing else {
            return Vec::new();
        };
        let values = backing
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start..self.start + self.len]
            .iter()
            .map(&mut map)
            .collect()
    }

    /// Mutates one visible element in place through every shared header. The
    /// callback must not re-enter this same backing.
    pub fn update(&self, index: usize, update: impl FnOnce(&mut T)) {
        assert!(index < self.len, "index out of Go slice bounds");
        let mut values = self
            .backing
            .as_ref()
            .expect("index of nil Go slice")
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        update(&mut values[self.start + index]);
    }

    /// Clones one visible element.
    #[must_use]
    pub fn get(&self, index: usize) -> T
    where
        T: Clone,
    {
        assert!(index < self.len, "index out of Go slice bounds");
        let values = self
            .backing
            .as_ref()
            .expect("index of nil Go slice")
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start + index].clone()
    }

    /// Replaces one visible element in the shared backing array.
    pub fn set(&self, index: usize, value: T) {
        assert!(index < self.len, "index out of Go slice bounds");
        let mut values = self
            .backing
            .as_ref()
            .expect("index of nil Go slice")
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start + index] = value;
    }

    /// Reslices this header to zero length without changing sibling headers
    /// or destroying the backing array.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Replaces this header as `encoding/json` replaces a decoded Go slice.
    /// A non-empty array reuses the backing array when capacity permits; the
    /// documented empty-array special case installs a new allocated-empty
    /// slice. Sibling headers observe reused element writes but retain their
    /// own lengths.
    pub(crate) fn replace_decoded(&mut self, values: Vec<T>, grown_capacity: usize)
    where
        T: Default,
    {
        if values.is_empty() {
            *self = Self::from_vec(Vec::new());
            return;
        }
        let new_len = values.len();
        if let Some(backing) = &self.backing {
            if new_len <= self.capacity {
                let mut destination = backing
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                for (offset, value) in values.into_iter().enumerate() {
                    let index = self.start + offset;
                    destination[index] = value;
                }
                self.len = new_len;
                return;
            }
        }
        *self = Self::from_vec_with_capacity(values, grown_capacity);
    }

    /// Makes the sequential decoder slot at `index` visible, growing this
    /// header exactly when Go's `encoding/json` would detach it from the old
    /// backing array. Earlier slot writes therefore remain observable through
    /// sibling headers when a later element triggers growth.
    pub(crate) fn prepare_decode_slot(&mut self, index: usize, grown_capacity: usize)
    where
        T: Clone + Default,
    {
        if index >= self.capacity {
            assert!(
                grown_capacity > self.capacity && grown_capacity > index,
                "Go slice decoder did not grow enough for its next slot"
            );
            let mut values = if let Some(backing) = &self.backing {
                let values = backing
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                values[self.start..self.start + self.len].to_vec()
            } else {
                Vec::new()
            };
            values.resize_with(grown_capacity, T::default);
            self.backing = Some(Arc::new(RwLock::new(values)));
            self.start = 0;
            self.capacity = grown_capacity;
        }
        if index >= self.len {
            self.len = index + 1;
        }
    }

    /// Clones an initialized slot anywhere through the header's capacity.
    /// Go's array decoder can reuse stale pointers beyond the old visible len.
    pub(crate) fn decode_slot(&self, index: usize) -> T
    where
        T: Clone,
    {
        assert!(index < self.capacity, "decode slot exceeds Go slice cap");
        let values = self
            .backing
            .as_ref()
            .expect("decode slot of nil Go slice")
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start + index].clone()
    }

    /// Replaces a decoder-visible slot without imposing the old header len as
    /// a bound. `prepare_decode_slot` has already made the slot visible.
    pub(crate) fn set_decode_slot(&self, index: usize, value: T) {
        assert!(index < self.len, "decode slot is not visible");
        let mut values = self
            .backing
            .as_ref()
            .expect("decode slot of nil Go slice")
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        values[self.start + index] = value;
    }

    /// Applies the successful end-of-array truncation. The empty-array case
    /// is special in Go and replaces even a reusable backing with `[]` cap 0.
    pub(crate) fn finish_decode(&mut self, decoded_len: usize) {
        if decoded_len == 0 {
            *self = Self::from_vec(Vec::new());
        } else {
            assert!(decoded_len <= self.len, "decoder length was not exposed");
            self.len = decoded_len;
        }
    }
}

impl<T> Clone for GoSharedSlice<T> {
    fn clone(&self) -> Self {
        Self {
            backing: self.backing.as_ref().map(Arc::clone),
            start: self.start,
            len: self.len,
            capacity: self.capacity,
        }
    }
}

impl<T> From<Vec<T>> for GoSharedSlice<T> {
    fn from(values: Vec<T>) -> Self {
        Self::from_vec(values)
    }
}

impl<T> Default for GoSharedSlice<T> {
    fn default() -> Self {
        Self {
            backing: None,
            start: 0,
            len: 0,
            capacity: 0,
        }
    }
}

impl<T: Clone + std::fmt::Debug> std::fmt::Debug for GoSharedSlice<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_allocated() {
            formatter
                .debug_struct("GoSharedSlice")
                .field("values", &self.snapshot())
                .field("capacity", &self.capacity)
                .finish()
        } else {
            formatter.write_str("GoSharedSlice(nil)")
        }
    }
}

impl<T: Clone + serde::Serialize> serde::Serialize for GoSharedSlice<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(backing) = &self.backing else {
            return serializer.serialize_none();
        };
        let snapshot = {
            let values = backing
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            values[self.start..self.start + self.len].to_vec()
        };
        serde::Serialize::serialize(&snapshot, serializer)
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for GoSharedSlice<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <Option<Vec<T>> as serde::Deserialize>::deserialize(deserializer)
            .map(|values| values.map_or_else(Self::default, Self::from_vec))
    }
}

/// A Go slice of nullable shared pointers.
pub struct GoSharedPointerSlice<T>(GoSharedSlice<Option<GoShared<T>>>);

impl<T> Clone for GoSharedPointerSlice<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Clone + std::fmt::Debug> std::fmt::Debug for GoSharedPointerSlice<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<T: Clone + serde::Serialize> serde::Serialize for GoSharedPointerSlice<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.0, serializer)
    }
}

impl<T> Default for GoSharedPointerSlice<T> {
    fn default() -> Self {
        Self(GoSharedSlice::default())
    }
}

impl<T> From<Vec<T>> for GoSharedPointerSlice<T> {
    fn from(values: Vec<T>) -> Self {
        Self::from_nullable(values.into_iter().map(Some).collect())
    }
}

/// Source policy when a clone loop encounters a nil pointer element.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoNullClonePolicy {
    /// Retain the nil element.
    Preserve,
    /// Panic at the source dereference boundary.
    Panic,
}

impl<T> GoSharedPointerSlice<T> {
    /// Constructs an allocated slice from nullable owned pointees.
    #[must_use]
    pub fn from_nullable(values: Vec<Option<T>>) -> Self {
        Self(GoSharedSlice::from_vec(
            values
                .into_iter()
                .map(|value| value.map(GoShared::new))
                .collect(),
        ))
    }

    /// Constructs an allocated pointer slice with a larger initialized cap;
    /// the non-visible capacity slots are nil Go pointers.
    #[must_use]
    pub fn from_nullable_with_capacity(values: Vec<Option<T>>, capacity: usize) -> Self {
        Self(GoSharedSlice::from_vec_with_capacity(
            values
                .into_iter()
                .map(|value| value.map(GoShared::new))
                .collect(),
            capacity,
        ))
    }

    /// Constructs an allocated slice from nullable pointer handles.
    #[must_use]
    pub fn from_handles(values: Vec<Option<GoShared<T>>>) -> Self {
        Self(GoSharedSlice::from_vec(values))
    }

    /// Constructs an allocated source header with explicit visible handles and
    /// fully initialized zero pointer slots through `capacity`.
    #[must_use]
    pub fn from_handles_with_capacity(values: Vec<Option<GoShared<T>>>, capacity: usize) -> Self {
        Self(GoSharedSlice::from_vec_with_capacity(values, capacity))
    }

    /// Returns whether the Go slice is non-nil.
    #[must_use]
    pub fn is_allocated(&self) -> bool {
        self.0.is_allocated()
    }

    /// Returns the source length.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the source length is zero.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the exact Go 1.25 64-bit pointer-slice capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Returns one nullable pointer handle. A non-null handle retains pointee
    /// identity when cloned.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<GoShared<T>> {
        self.0.get(index)
    }

    /// Replaces one pointer slot through every shared outer header.
    pub fn set(&self, index: usize, value: Option<GoShared<T>>) {
        self.0.set(index, value);
    }

    /// Copies the outer slice while sharing every non-null pointee.
    #[must_use]
    pub fn copy_outer(&self) -> Self {
        Self(self.0.copy_outer())
    }

    /// Reports outer backing-array identity.
    #[must_use]
    pub fn backing_ptr_eq(&self, other: &Self) -> bool {
        self.0.backing_ptr_eq(&other.0)
    }

    /// Returns nullable pointer handles for the visible range.
    #[must_use]
    pub fn handles(&self) -> Vec<Option<GoShared<T>>> {
        self.0.snapshot()
    }

    /// Iterates nullable pointer slots through a copied Go slice header. Each
    /// slot is loaded when the iterator advances, so writes through a sibling
    /// header between iterations remain observable just as in Go `range`.
    pub fn iter_handles(&self) -> impl Iterator<Item = Option<GoShared<T>>> {
        let header = self.clone();
        let len = header.len();
        (0..len).map(move |index| header.get(index))
    }

    /// Iterates non-null source pointers as cloneable shared handles. The
    /// panic occurs at the same dereference boundary as `for _, v := range`
    /// followed by a field or method access on a nil `*T`.
    pub fn iter_deref(&self) -> impl Iterator<Item = GoShared<T>> {
        self.iter_handles()
            .map(|pointer| pointer.expect("nil pointer in Go slice"))
    }

    /// Allocates a fresh outer slice and maps non-null pointees through the
    /// source type's clone operation. Go clone methods choose different nil
    /// policies, so callers must name it rather than inheriting Rust `Clone`.
    #[must_use]
    pub fn map_clone_with<U>(
        &self,
        null_policy: GoNullClonePolicy,
        mut clone_pointee: impl FnMut(&T) -> U,
    ) -> GoSharedPointerSlice<U> {
        GoSharedPointerSlice::from_handles(
            self.iter_handles()
                .map(|pointer| match pointer {
                    Some(pointer) => {
                        let cloned = {
                            let value = pointer.read();
                            clone_pointee(&value)
                        };
                        Some(GoShared::new(cloned))
                    }
                    None if null_policy == GoNullClonePolicy::Preserve => None,
                    None => panic!("nil pointer in Go clone slice"),
                })
                .collect(),
        )
    }

    /// Reslices this header to allocated-empty or leaves nil as nil.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub(crate) fn replace_decoded(&mut self, values: Vec<Option<GoShared<T>>>) {
        let grown_capacity = go_64_pointer_slice_decode_capacity(self.0.capacity(), values.len());
        self.0.replace_decoded(values, grown_capacity);
    }

    pub(crate) fn prepare_decode_slot(&mut self, index: usize) {
        let grown_capacity = go_64_pointer_slice_decode_capacity(self.0.capacity(), index + 1);
        self.0.prepare_decode_slot(index, grown_capacity);
    }

    pub(crate) fn decode_slot(&self, index: usize) -> Option<GoShared<T>> {
        self.0.decode_slot(index)
    }

    pub(crate) fn set_decode_slot(&self, index: usize, value: Option<GoShared<T>>) {
        self.0.set_decode_slot(index, value);
    }

    pub(crate) fn finish_decode(&mut self, decoded_len: usize) {
        self.0.finish_decode(decoded_len);
    }
}

impl<'de, T> serde::Deserialize<'de> for GoSharedPointerSlice<T>
where
    T: serde::Deserialize<'de>,
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let Some(values) =
            <Option<Vec<Option<GoShared<T>>>> as serde::Deserialize>::deserialize(deserializer)?
        else {
            return Ok(Self::default());
        };
        let capacity = go_64_pointer_slice_decode_capacity(0, values.len());
        Ok(Self::from_handles_with_capacity(values, capacity))
    }
}

impl GoTime {
    /// Go `time.UnixMilli`'s numeric instant constructor.
    #[must_use]
    pub const fn from_unix_millis(unix_millis: i64) -> Self {
        Self {
            unix_millis,
            location: GoTimeLocation::Local,
        }
    }

    /// Go model `TSConvert2Time`: discard the low 18 logical TSO bits, then
    /// interpret the remaining physical component as Unix milliseconds.
    #[must_use]
    pub const fn from_tso(timestamp: u64) -> Self {
        Self::from_unix_millis((timestamp >> 18) as i64)
    }

    /// The exact Unix millisecond value accepted by Go `time.UnixMilli`.
    #[must_use]
    pub const fn unix_millis(self) -> i64 {
        self.unix_millis
    }

    /// Returns the Go location identity used by formatting and DST lookup.
    #[must_use]
    pub const fn location(self) -> GoTimeLocation {
        self.location
    }

    /// Converts to Chrono UTC only when Chrono supports the source year.
    #[must_use]
    pub fn to_chrono_utc(self) -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(self.unix_millis)
    }
}

impl<'a, T> GoPointerAny<'a, T> {
    /// Constructs a source-typed pointer interface.
    #[must_use]
    pub fn typed(value: Option<&'a T>) -> Self {
        Self::Typed(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct Item {
        id: i64,
    }

    #[test]
    fn go_time_retains_full_tso_physical_domain() {
        assert_eq!(GoTime::default().unix_millis(), -62_135_596_800_000);
        assert_eq!(GoTime::default().location(), GoTimeLocation::Utc);
        assert_eq!(GoTime::from_tso(0).unix_millis(), 0);
        assert_eq!(GoTime::from_tso(0).location(), GoTimeLocation::Local);
        assert_eq!(GoTime::from_tso((1_u64 << 18) - 1).unix_millis(), 0);
        assert_eq!(GoTime::from_tso(1_u64 << 18).unix_millis(), 1);
        assert_eq!(GoTime::from_tso(u64::MAX).unix_millis(), (1_i64 << 46) - 1);
        assert!(GoTime::from_tso(u64::MAX).to_chrono_utc().is_some());
        assert!(GoTime::from_unix_millis(i64::MAX).to_chrono_utc().is_none());
    }

    #[test]
    fn go_shared_distinguishes_shallow_and_deep_pointer_clones() {
        let pointer = GoShared::new(Item { id: 1 });
        let shallow = pointer.clone();
        let deep = pointer.clone_rust_value();
        assert!(pointer.ptr_eq(&shallow));
        assert!(!pointer.ptr_eq(&deep));

        shallow.write().id = 2;
        assert_eq!(pointer.read().id, 2);
        assert_eq!(deep.read().id, 1);
    }

    #[test]
    fn go_shared_recovers_partial_state_after_a_panicking_writer() {
        let pointer = GoShared::new(Item { id: 1 });
        let alias = pointer.clone();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut value = alias.write();
            value.id = 7;
            panic!("source callback panic after mutation");
        }))
        .is_err());
        assert_eq!(pointer.read().id, 7);
        pointer.write().id = 8;
        assert_eq!(alias.read().id, 8);
    }

    #[test]
    fn go_shared_slice_copies_headers_and_can_copy_only_the_outer_array() {
        let first = GoShared::new(Item { id: 1 });
        let replacement = GoShared::new(Item { id: 2 });
        let mut caller = GoSharedPointerSlice::from_handles(vec![Some(first.clone()), None]);
        let assigned = caller.clone();
        assert!(caller.backing_ptr_eq(&assigned));
        assert!(assigned.get(0).unwrap().ptr_eq(&first));
        assert!(assigned.get(1).is_none());

        caller.set(0, Some(replacement.clone()));
        assert!(assigned.get(0).unwrap().ptr_eq(&replacement));
        caller.clear();
        assert_eq!(caller.len(), 0);
        assert_eq!(assigned.len(), 2);

        let copied_outer = assigned.copy_outer();
        assert!(!assigned.backing_ptr_eq(&copied_outer));
        assert!(assigned
            .get(0)
            .unwrap()
            .ptr_eq(&copied_outer.get(0).unwrap()));
        copied_outer.set(0, Some(first.clone()));
        assert!(assigned.get(0).unwrap().ptr_eq(&replacement));
        assert!(copied_outer.get(0).unwrap().ptr_eq(&first));

        let mut range = assigned.iter_handles();
        assert!(range.next().unwrap().unwrap().ptr_eq(&replacement));
        assigned.set(1, Some(first.clone()));
        assert!(range.next().unwrap().unwrap().ptr_eq(&first));
        assigned.set(1, None);

        let copied_deep = assigned.map_clone_with(GoNullClonePolicy::Preserve, Clone::clone);
        assert!(!assigned
            .get(0)
            .unwrap()
            .ptr_eq(&copied_deep.get(0).unwrap()));
        assigned.get(0).unwrap().write().id = 3;
        assert_eq!(copied_deep.get(0).unwrap().read().id, 2);

        assert!(std::panic::catch_unwind(|| {
            assigned.map_clone_with(GoNullClonePolicy::Panic, Clone::clone)
        })
        .is_err());
    }

    #[test]
    fn go_shared_pointer_slice_wire_retains_nil_empty_and_null_slots() {
        let nil = GoSharedPointerSlice::<Item>::default();
        let empty = GoSharedPointerSlice::<Item>::from_nullable(Vec::new());
        let nullable = GoSharedPointerSlice::from_nullable(vec![Some(Item { id: 7 }), None]);
        assert_eq!(serde_json::to_string(&nil).unwrap(), "null");
        assert_eq!(serde_json::to_string(&empty).unwrap(), "[]");
        assert_eq!(
            serde_json::to_string(&nullable).unwrap(),
            r#"[{"id":7},null]"#
        );

        let decoded: GoSharedPointerSlice<Item> =
            serde_json::from_str(r#"[{"id":8},null]"#).unwrap();
        assert!(decoded.is_allocated());
        assert_eq!(decoded.get(0).unwrap().read().id, 8);
        assert!(decoded.get(1).is_none());
    }

    #[test]
    fn go_pointer_slice_decode_uses_go_125_64_bit_growth() {
        assert_eq!(go_64_pointer_slice_decode_capacity(0, 0), 0);
        assert_eq!(go_64_pointer_slice_decode_capacity(0, 1), 1);
        assert_eq!(go_64_pointer_slice_decode_capacity(0, 3), 4);
        assert_eq!(go_64_pointer_slice_decode_capacity(4, 5), 8);
        // Pointer-bearing allocations above 512 bytes include Go's malloc
        // header before size-class rounding; 256 -> 257 therefore yields 607.
        assert_eq!(go_64_pointer_slice_decode_capacity(256, 257), 607);

        // The malloc header applies only to scanned objects. At the first
        // boundary above 512 bytes, a []PartitionState-style noscan slice
        // doubles from cap 32 to 64, while a scanned 16-byte element rounds
        // through the header-bearing class to cap 71.
        assert_eq!(
            go_64_next_slice_capacity_for_element(33, 32, 16, GoSliceElementLayout::NoPointers,),
            64
        );
        assert_eq!(
            go_64_next_slice_capacity_for_element(33, 32, 16, GoSliceElementLayout::PointerBearing,),
            71
        );

        let mut decoded = GoSharedPointerSlice::<Item>::default();
        decoded.replace_decoded(vec![
            Some(GoShared::new(Item { id: 1 })),
            None,
            Some(GoShared::new(Item { id: 3 })),
        ]);
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.capacity(), 4);
        let alias = decoded.clone();
        decoded.replace_decoded(vec![
            Some(GoShared::new(Item { id: 4 })),
            None,
            None,
            Some(GoShared::new(Item { id: 7 })),
        ]);
        assert!(decoded.backing_ptr_eq(&alias));
        assert_eq!(decoded.capacity(), 4);
        assert_eq!(alias.len(), 3);
        assert_eq!(alias.get(0).unwrap().read().id, 4);
    }
}
