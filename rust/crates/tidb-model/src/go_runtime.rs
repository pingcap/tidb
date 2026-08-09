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

pub(crate) use tidb_datatype::go_runtime::{
    go_64_next_slice_capacity_for_element, go_64_slice_decode_capacity,
};
pub use tidb_datatype::go_runtime::{GoSharedSlice, GoSliceElementLayout};

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

    /// Stable process-local address used by Go `%p`/default pointer
    /// formatting. Semantic equality should continue to use [`Self::ptr_eq`].
    #[must_use]
    pub(crate) fn identity_address(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
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

    /// Appends one non-null pointer with Go 1.25's 64-bit pointer-slice growth
    /// rule. Appending within capacity writes the shared backing while changing
    /// only this header's length; growth detaches only this header.
    pub fn push_go(&mut self, value: T) {
        self.push_handle_go(Some(GoShared::new(value)));
    }

    /// Appends one nullable pointer handle while preserving pointee identity.
    pub fn push_handle_go(&mut self, value: Option<GoShared<T>>) {
        self.0
            .push_go(value, 8, GoSliceElementLayout::PointerBearing);
    }

    /// Deletes a half-open pointer range with Go 1.25 `slices.Delete`
    /// semantics, including same-backing tail movement and nil-clearing.
    pub fn delete_go(&mut self, start: usize, end: usize) {
        self.0.delete_go(start, end);
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
    fn go_shared_pointer_slice_append_shares_spare_capacity_and_detaches_on_growth() {
        let first = GoShared::new(Item { id: 1 });
        let mut within_capacity =
            GoSharedPointerSlice::from_handles_with_capacity(vec![Some(first.clone())], 2);
        let short_sibling = within_capacity.clone();
        let appended = GoShared::new(Item { id: 2 });
        within_capacity.push_handle_go(Some(appended.clone()));
        assert_eq!(within_capacity.len(), 2);
        assert_eq!(within_capacity.capacity(), 2);
        assert_eq!(short_sibling.len(), 1);
        assert!(within_capacity.backing_ptr_eq(&short_sibling));
        assert!(within_capacity.get(1).unwrap().ptr_eq(&appended));

        let mut must_grow = GoSharedPointerSlice::from_handles(vec![Some(first.clone())]);
        let old_backing = must_grow.clone();
        must_grow.push_handle_go(None);
        assert_eq!(must_grow.len(), 2);
        assert_eq!(must_grow.capacity(), 2);
        assert!(must_grow.get(1).is_none());
        assert!(!must_grow.backing_ptr_eq(&old_backing));
        assert_eq!(old_backing.len(), 1);
        assert!(old_backing.get(0).unwrap().ptr_eq(&first));

        let nil_sibling = GoSharedPointerSlice::<Item>::default();
        let mut appended_nil = nil_sibling.clone();
        appended_nil.push_go(Item { id: 3 });
        assert!(!nil_sibling.is_allocated());
        assert!(appended_nil.is_allocated());
        assert_eq!(appended_nil.capacity(), 1);

        let mut allocator_boundary =
            GoSharedPointerSlice::<Item>::from_handles_with_capacity(vec![None; 256], 256);
        allocator_boundary.push_handle_go(None);
        assert_eq!(allocator_boundary.len(), 257);
        assert_eq!(allocator_boundary.capacity(), 607);
    }

    #[test]
    fn go_shared_pointer_slice_delete_shifts_and_clears_shared_backing() {
        let first = GoShared::new(Item { id: 1 });
        let removed = GoShared::new(Item { id: 2 });
        let last = GoShared::new(Item { id: 3 });
        let fourth = GoShared::new(Item { id: 4 });
        let fifth = GoShared::new(Item { id: 5 });
        let hidden = GoShared::new(Item { id: 6 });
        let mut shortened = GoSharedPointerSlice::from_handles_with_capacity(
            vec![
                Some(first.clone()),
                Some(removed),
                Some(last),
                Some(fourth.clone()),
                Some(fifth.clone()),
            ],
            6,
        );
        shortened.prepare_decode_slot(5);
        shortened.set_decode_slot(5, Some(hidden.clone()));
        shortened.finish_decode(5);
        let old_header = shortened.clone();

        shortened.delete_go(1, 3);
        assert_eq!(shortened.len(), 3);
        assert_eq!(shortened.capacity(), 6);
        assert!(shortened.backing_ptr_eq(&old_header));
        assert!(shortened.get(0).unwrap().ptr_eq(&first));
        assert!(shortened.get(1).unwrap().ptr_eq(&fourth));
        assert!(shortened.get(2).unwrap().ptr_eq(&fifth));
        assert_eq!(old_header.len(), 5);
        assert!(old_header.get(1).unwrap().ptr_eq(&fourth));
        assert!(old_header.get(2).unwrap().ptr_eq(&fifth));
        assert!(old_header.get(3).is_none());
        assert!(old_header.get(4).is_none());
        let mut expose_hidden = old_header.clone();
        expose_hidden.prepare_decode_slot(5);
        assert!(expose_hidden.decode_slot(5).unwrap().ptr_eq(&hidden));

        let backing_before_noop = shortened.clone();
        shortened.delete_go(1, 1);
        assert_eq!(shortened.len(), 3);
        assert!(shortened.backing_ptr_eq(&backing_before_noop));

        let mut nil_noop = GoSharedPointerSlice::<Item>::default();
        nil_noop.delete_go(0, 0);
        assert!(!nil_noop.is_allocated());
        let mut allocated_empty = GoSharedPointerSlice::<Item>::from_handles(Vec::new());
        let empty_backing = allocated_empty.clone();
        allocated_empty.delete_go(0, 0);
        assert!(allocated_empty.is_allocated());
        assert!(allocated_empty.backing_ptr_eq(&empty_backing));

        let visible_ids = |slice: &GoSharedPointerSlice<Item>| {
            slice
                .iter_handles()
                .map(|pointer| pointer.map(|pointer| pointer.read().id))
                .collect::<Vec<_>>()
        };
        let before_invalid = visible_ids(&old_header);
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut invalid = old_header.clone();
            invalid.delete_go(2, 1);
        }))
        .is_err());
        assert_eq!(visible_ids(&old_header), before_invalid);

        let old_length_sibling = old_header.clone();
        let mut full_delete = old_header.clone();
        full_delete.delete_go(0, 5);
        assert!(full_delete.is_allocated());
        assert_eq!(full_delete.len(), 0);
        assert_eq!(full_delete.capacity(), 6);
        assert!(full_delete.backing_ptr_eq(&old_length_sibling));
        assert_eq!(visible_ids(&old_length_sibling), vec![None; 5]);
        let mut expose_hidden_after_full_delete = old_length_sibling.clone();
        expose_hidden_after_full_delete.prepare_decode_slot(5);
        assert!(expose_hidden_after_full_delete
            .decode_slot(5)
            .unwrap()
            .ptr_eq(&hidden));
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
    }
}
