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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GoTime {
    unix_millis: i64,
}

/// A concurrency-safe Go pointer whose clones retain pointee identity.
///
/// Go metadata clone methods deliberately mix deep-cloned and shallow-shared
/// pointers. Rust owned values cannot express the latter. This handle makes
/// the sharing explicit and exposes guards instead of an unsafe `Deref` that
/// could outlive synchronization.
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

    /// Allocates an independent pointer holding a deep value clone.
    #[must_use]
    pub fn deep_clone(&self) -> Self
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

impl<T: Default> Default for GoShared<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for GoShared<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("GoShared")
            .field(&*self.read())
            .finish()
    }
}

impl<T: Clone + PartialEq> PartialEq for GoShared<T> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr_eq(other) || self.read().clone() == other.read().clone()
    }
}

impl<T: Clone + Eq> Eq for GoShared<T> {}

impl<T: serde::Serialize> serde::Serialize for GoShared<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&*self.read(), serializer)
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
        let capacity = values.capacity();
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

impl<T: Clone + PartialEq> PartialEq for GoSharedSlice<T> {
    fn eq(&self, other: &Self) -> bool {
        self.is_allocated() == other.is_allocated() && self.snapshot() == other.snapshot()
    }
}

impl<T: Clone + Eq> Eq for GoSharedSlice<T> {}

impl<T: serde::Serialize> serde::Serialize for GoSharedSlice<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Some(backing) = &self.backing else {
            return serializer.serialize_none();
        };
        let values = backing
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        serde::Serialize::serialize(&values[self.start..self.start + self.len], serializer)
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for GoSharedSlice<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <Option<Vec<T>> as serde::Deserialize>::deserialize(deserializer)
            .map(|values| values.map_or_else(Self::default, Self::from_vec))
    }
}

/// A Go slice of nullable shared pointers.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct GoSharedPointerSlice<T>(GoSharedSlice<Option<GoShared<T>>>);

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

    /// Constructs an allocated slice from nullable pointer handles.
    #[must_use]
    pub fn from_handles(values: Vec<Option<GoShared<T>>>) -> Self {
        Self(GoSharedSlice::from_vec(values))
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

    /// Allocates a fresh outer slice and fresh pointees, retaining null slots.
    #[must_use]
    pub fn deep_clone(&self) -> Self
    where
        T: Clone,
    {
        Self::from_handles(
            self.handles()
                .into_iter()
                .map(|pointer| pointer.map(|pointer| pointer.deep_clone()))
                .collect(),
        )
    }

    /// Reslices this header to allocated-empty or leaves nil as nil.
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

impl GoTime {
    /// Go `time.UnixMilli`'s numeric instant constructor.
    #[must_use]
    pub const fn from_unix_millis(unix_millis: i64) -> Self {
        Self { unix_millis }
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
        assert_eq!(GoTime::from_tso(0).unix_millis(), 0);
        assert_eq!(GoTime::from_tso((1_u64 << 18) - 1).unix_millis(), 0);
        assert_eq!(GoTime::from_tso(1_u64 << 18).unix_millis(), 1);
        assert_eq!(GoTime::from_tso(u64::MAX).unix_millis(), (1_i64 << 46) - 1);
        assert!(GoTime::from_tso(u64::MAX).to_chrono_utc().is_none());
    }

    #[test]
    fn go_shared_distinguishes_shallow_and_deep_pointer_clones() {
        let pointer = GoShared::new(Item { id: 1 });
        let shallow = pointer.clone();
        let deep = pointer.deep_clone();
        assert!(pointer.ptr_eq(&shallow));
        assert!(!pointer.ptr_eq(&deep));

        shallow.write().id = 2;
        assert_eq!(pointer.read().id, 2);
        assert_eq!(deep.read().id, 1);
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

        let copied_deep = assigned.deep_clone();
        assert!(!assigned
            .get(0)
            .unwrap()
            .ptr_eq(&copied_deep.get(0).unwrap()));
        assigned.get(0).unwrap().write().id = 3;
        assert_eq!(copied_deep.get(0).unwrap().read().id, 2);
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
}
