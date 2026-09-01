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

//! Keyed, primitive, and memory-aware sets from `pkg/util/set`.
//!
//! [`KeyedSet`] retains the source-specific stable-key rule. The primitive
//! sets preserve Go map equality and unspecified iteration. The five concrete
//! memory-aware types report the same checkpoint deltas and expose trackers
//! only on the three source types that support one.

use crate::memory::Tracker;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use tidb_datatype::{GoString, MyDecimal};
use tidb_hack::{MapValueLayout, MemAwareMap};
use tidb_mysql::{to_lowercase, to_uppercase};

/// A value whose set identity is an arbitrary-byte Go string.
pub trait SetKey {
    /// Returns the value's stable identity.
    fn set_key(&self) -> GoString;
}

/// A set that keeps the latest value for each stable string key.
pub struct KeyedSet<T> {
    values: HashMap<GoString, T>,
}

impl<T> Clone for KeyedSet<T>
where
    T: Clone + SetKey,
{
    fn clone(&self) -> Self {
        list_to_set(self.to_list())
    }
}

impl<T> Default for KeyedSet<T>
where
    T: Clone + SetKey,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> KeyedSet<T>
where
    T: Clone + SetKey,
{
    /// Creates an empty set.
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Adds values, replacing an older value with the same key.
    pub fn add(&mut self, values: impl IntoIterator<Item = T>) {
        for value in values {
            self.values.insert(value.set_key(), value);
        }
    }

    /// Returns whether a value with the same key exists.
    pub fn contains(&self, value: &T) -> bool {
        self.values.contains_key(&value.set_key())
    }

    /// Removes the value with the same key.
    pub fn remove(&mut self, value: &T) {
        self.values.remove(&value.set_key());
    }

    /// Returns cloned values in stable key order.
    pub fn to_list(&self) -> Vec<T> {
        let mut values: Vec<_> = self.values.values().cloned().collect();
        values.sort_by_key(SetKey::set_key);
        values
    }

    /// Returns the number of distinct keys.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether the set has no values.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns the source `String` representation without assuming UTF-8.
    pub fn string(&self) -> GoString {
        let mut keys: Vec<_> = self.values.values().map(SetKey::set_key).collect();
        keys.sort();
        let mut out = vec![b'{'];
        for (index, key) in keys.iter().enumerate() {
            if index != 0 {
                out.extend_from_slice(b", ");
            }
            out.extend_from_slice(key.as_bytes());
        }
        out.push(b'}');
        GoString::from_bytes(out)
    }
}

fn combinations_from<T>(
    items: &[T],
    current: &mut KeyedSet<T>,
    depth: usize,
    wanted: usize,
    result: &mut Vec<KeyedSet<T>>,
) where
    T: Clone + SetKey,
{
    if current.len() == wanted {
        result.push(current.clone());
        return;
    }
    if depth == items.len() || current.len() > wanted {
        return;
    }
    current.add([items[depth].clone()]);
    combinations_from(items, current, depth + 1, wanted, result);
    current.remove(&items[depth]);
    combinations_from(items, current, depth + 1, wanted, result);
}

/// Converts a list to a set.
pub fn list_to_set<T>(values: impl IntoIterator<Item = T>) -> KeyedSet<T>
where
    T: Clone + SetKey,
{
    let mut result = KeyedSet::new();
    result.add(values);
    result
}

/// Returns the union of all input sets.
pub fn union<T>(sets: &[&KeyedSet<T>]) -> KeyedSet<T>
where
    T: Clone + SetKey,
{
    let mut result = KeyedSet::new();
    for set in sets {
        result.add(set.to_list());
    }
    result
}

/// Returns the intersection of all input sets.
pub fn intersection<T>(sets: &[&KeyedSet<T>]) -> KeyedSet<T>
where
    T: Clone + SetKey,
{
    let Some(first) = sets.first() else {
        return KeyedSet::new();
    };
    let mut result = KeyedSet::new();
    for value in first.to_list() {
        if sets[1..].iter().all(|set| set.contains(&value)) {
            result.add([value]);
        }
    }
    result
}

/// Returns the values present in `left` but absent from `right`.
pub fn difference<T>(left: &KeyedSet<T>, right: &KeyedSet<T>) -> KeyedSet<T>
where
    T: Clone + SetKey,
{
    let mut result = KeyedSet::new();
    for value in left.to_list() {
        if !right.contains(&value) {
            result.add([value]);
        }
    }
    result
}

/// Returns every size-`count` combination in stable source order.
pub fn combinations<T>(set: &KeyedSet<T>, count: isize) -> Vec<KeyedSet<T>>
where
    T: Clone + SetKey,
{
    if count < 0 {
        return Vec::new();
    }
    let wanted = count as usize;
    let items = set.to_list();
    let mut current = KeyedSet::new();
    let mut result = Vec::new();
    combinations_from(&items, &mut current, 0, wanted, &mut result);
    result
}

macro_rules! numeric_set {
    ($name:ident, $value:ty, $doc:literal) => {
        #[doc = $doc]
        pub struct $name {
            values: HashSet<$value>,
        }

        impl $name {
            /// Builds a set from initial values.
            pub fn new(values: impl IntoIterator<Item = $value>) -> Self {
                Self {
                    values: values.into_iter().collect(),
                }
            }

            /// Inserts a value.
            pub fn insert(&mut self, value: $value) {
                self.values.insert(value);
            }

            /// Returns whether `value` exists.
            pub fn contains(&self, value: &$value) -> bool {
                self.values.contains(value)
            }

            /// Returns the number of values.
            pub fn len(&self) -> usize {
                self.values.len()
            }

            /// Returns whether the set has no values.
            pub fn is_empty(&self) -> bool {
                self.values.is_empty()
            }

            /// Iterates over values in unspecified map order.
            pub fn iter(&self) -> impl Iterator<Item = &$value> {
                self.values.iter()
            }
        }
    };
}

numeric_set!(IntSet, isize, "Source `IntSet`.");
numeric_set!(Int64Set, i64, "Source `Int64Set`.");

/// An arbitrary-byte string set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StringSet {
    values: HashSet<GoString>,
}

impl StringSet {
    /// Builds a string set from initial values.
    pub fn new<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<GoString>,
    {
        Self {
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    /// Inserts a string.
    pub fn insert(&mut self, value: impl Into<GoString>) {
        self.values.insert(value.into());
    }

    /// Returns whether a byte string exists.
    pub fn contains(&self, value: &GoString) -> bool {
        self.values.contains(value)
    }

    /// Returns the intersection with `right`.
    pub fn intersection(&self, right: &Self) -> Self {
        Self {
            values: self.values.intersection(&right.values).cloned().collect(),
        }
    }

    /// Returns original values from `right` whose lower/upper-case form is in
    /// this set.
    pub fn intersection_with_case(&self, right: &Self, to_lower: bool) -> Self {
        let mut result = Self::new([] as [GoString; 0]);
        for original in &right.values {
            let text = original.to_utf8_lossy_go();
            let folded = if to_lower {
                to_lowercase(&text)
            } else {
                to_uppercase(&text)
            };
            if self.values.contains(&GoString::from(folded)) {
                result.values.insert(original.clone());
            }
        }
        result
    }

    /// Returns the number of values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Removes every value.
    pub fn clear(&mut self) {
        self.values.clear();
    }

    /// Calls `function` for every value in unspecified map order.
    pub fn iterate_with(&self, mut function: impl FnMut(GoString)) {
        for value in &self.values {
            function(value.clone());
        }
    }
}

/// A float set with Go map equality: signed zero aliases, while every NaN
/// insertion is distinct and a NaN lookup never succeeds.
#[derive(Clone, Debug)]
pub struct Float64Set {
    numbers: HashSet<u64>,
    nan_count: usize,
}

impl Float64Set {
    /// Builds a float set from initial values.
    pub fn new(values: impl IntoIterator<Item = f64>) -> Self {
        let mut result = Self {
            numbers: HashSet::new(),
            nan_count: 0,
        };
        for value in values {
            result.insert(value);
        }
        result
    }

    /// Inserts a value.
    pub fn insert(&mut self, value: f64) {
        match canonical_float_bits(value) {
            Some(bits) => {
                self.numbers.insert(bits);
            }
            None => {
                self.nan_count += 1;
            }
        }
    }

    /// Returns whether `value` exists. NaN never equals a map key in Go.
    pub fn contains(&self, value: f64) -> bool {
        canonical_float_bits(value).is_some_and(|bits| self.numbers.contains(&bits))
    }

    /// Returns the number of map entries.
    pub fn len(&self) -> usize {
        self.numbers.len() + self.nan_count
    }

    /// Returns whether the set has no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn canonical_float_bits(value: f64) -> Option<u64> {
    if value.is_nan() {
        None
    } else if value == 0.0 {
        Some(0)
    } else {
        Some(value.to_bits())
    }
}

/// A hash map that reports native allocation deltas and can consume them
/// immediately on a TiDB memory tracker.
struct MemoryMap<K, V> {
    map: MemAwareMap<K, V>,
    tracker: Option<Arc<Tracker>>,
}

impl<K, V> MemoryMap<K, V>
where
    K: Eq + Hash + MapValueLayout,
    V: MapValueLayout,
{
    /// Creates an empty map.
    fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates an empty map with capacity for at least `capacity` values.
    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: MemAwareMap::new(capacity),
            tracker: None,
        }
    }

    /// Inserts or replaces a value and returns its allocation delta.
    fn insert(&mut self, key: K, value: V) -> i64 {
        self.insert_ext(key, value).0
    }

    /// Inserts or replaces a value, returning allocation delta and whether
    /// the key was new.
    fn insert_ext(&mut self, key: K, value: V) -> (i64, bool) {
        let (delta, inserted) = self.map.set_ext(key, value);
        if delta != 0 {
            if let Some(tracker) = &self.tracker {
                tracker.consume(delta);
                return (0, inserted);
            }
        }
        (delta, inserted)
    }

    /// Installs the tracker that consumes future allocation changes.
    fn set_tracker(&mut self, tracker: Option<Arc<Tracker>>) {
        self.tracker = tracker;
    }

    /// Returns whether a key exists.
    fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    /// Gets a value.
    fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    /// Returns the number of entries.
    fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns whether the map is empty.
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the current accounted map bytes.
    const fn accounted_bytes(&self) -> u64 {
        self.map.bytes()
    }

    /// Iterates over entries.
    fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter()
    }
}

impl<K, V> Default for MemoryMap<K, V>
where
    K: Eq + Hash + MapValueLayout,
    V: MapValueLayout,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A hash set that reports native table-allocation changes.
struct MemorySet<K> {
    map: MemoryMap<K, ()>,
}

impl<K> MemorySet<K>
where
    K: Eq + Hash + MapValueLayout,
{
    /// Creates an empty set.
    fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates an empty set with capacity for at least `capacity` values.
    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: MemoryMap::with_capacity(capacity),
        }
    }

    /// Inserts a value, returning allocation delta and whether it was new.
    fn insert(&mut self, value: K) -> (i64, bool) {
        self.map.insert_ext(value, ())
    }

    /// Installs the tracker that consumes future allocation changes.
    fn set_tracker(&mut self, tracker: Option<Arc<Tracker>>) {
        self.map.set_tracker(tracker);
    }

    /// Returns whether a value exists.
    fn contains(&self, value: &K) -> bool {
        self.map.contains_key(value)
    }

    /// Returns the number of values.
    fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns whether the set is empty.
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns current accounted table bytes.
    const fn accounted_bytes(&self) -> u64 {
        self.map.accounted_bytes()
    }

    /// Iterates over values.
    fn iter(&self) -> impl Iterator<Item = &K> {
        self.map.iter().map(|(key, ())| key)
    }
}

impl<K> Default for MemorySet<K>
where
    K: Eq + Hash + MapValueLayout,
{
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! tracked_memory_map {
    ($name:ident, $value:ty) => {
        /// A source memory-aware string map.
        pub struct $name {
            values: MemoryMap<GoString, $value>,
        }

        impl $name {
            /// Builds an empty map and returns its initial accounted bytes.
            pub fn new() -> (Self, i64) {
                let values = MemoryMap::new();
                let bytes = i64::try_from(values.accounted_bytes()).unwrap_or(i64::MAX);
                (Self { values }, bytes)
            }

            /// Inserts or replaces one value and returns the allocation delta.
            pub fn insert(&mut self, key: GoString, value: $value) -> i64 {
                self.values.insert(key, value)
            }

            /// Sets or clears the tracker that consumes future deltas.
            pub fn set_tracker(&mut self, tracker: Option<Arc<Tracker>>) {
                self.values.set_tracker(tracker);
            }

            /// Returns the value for `key`.
            pub fn get(&self, key: &GoString) -> Option<&$value> {
                self.values.get(key)
            }

            /// Returns the number of entries.
            pub fn len(&self) -> usize {
                self.values.len()
            }

            /// Returns whether the map is empty.
            pub fn is_empty(&self) -> bool {
                self.values.is_empty()
            }

            /// Iterates over entries in unspecified map order.
            pub fn iter(&self) -> impl Iterator<Item = (&GoString, &$value)> {
                self.values.iter()
            }
        }
    };
}

tracked_memory_map!(StringToStringMapWithMemoryUsage, GoString);
tracked_memory_map!(StringToDecimalMapWithMemoryUsage, Box<MyDecimal>);

/// A source memory-aware string set.
pub struct StringSetWithMemoryUsage {
    values: MemorySet<GoString>,
}

impl StringSetWithMemoryUsage {
    /// Builds a set and returns its initial accounted bytes.
    pub fn new<I>(values: I) -> (Self, i64)
    where
        I: IntoIterator<Item = GoString>,
        I::IntoIter: ExactSizeIterator,
    {
        let values = values.into_iter();
        let mut result = Self {
            values: MemorySet::with_capacity(values.len()),
        };
        for value in values {
            result.insert(value);
        }
        let bytes = i64::try_from(result.values.accounted_bytes()).unwrap_or(i64::MAX);
        (result, bytes)
    }

    /// Inserts one value and returns its allocation delta.
    pub fn insert(&mut self, value: GoString) -> i64 {
        self.values.insert(value).0
    }

    /// Sets or clears the tracker that consumes future deltas.
    pub fn set_tracker(&mut self, tracker: Option<Arc<Tracker>>) {
        self.values.set_tracker(tracker);
    }

    /// Returns whether `value` exists.
    pub fn contains(&self, value: &GoString) -> bool {
        self.values.contains(value)
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterates over values in unspecified map order.
    pub fn iter(&self) -> impl Iterator<Item = &GoString> {
        self.values.iter()
    }
}

/// A source memory-aware signed-64-bit set.
pub struct Int64SetWithMemoryUsage {
    values: MemorySet<i64>,
}

impl Int64SetWithMemoryUsage {
    /// Builds a set and returns its initial accounted bytes.
    pub fn new<I>(values: I) -> (Self, i64)
    where
        I: IntoIterator<Item = i64>,
        I::IntoIter: ExactSizeIterator,
    {
        let values = values.into_iter();
        let mut result = Self {
            values: MemorySet::with_capacity(values.len()),
        };
        for value in values {
            result.insert(value);
        }
        let bytes = i64::try_from(result.values.accounted_bytes()).unwrap_or(i64::MAX);
        (result, bytes)
    }

    /// Inserts one value and returns its allocation delta.
    pub fn insert(&mut self, value: i64) -> i64 {
        self.values.insert(value).0
    }

    /// Returns whether `value` exists.
    pub fn contains(&self, value: i64) -> bool {
        self.values.contains(&value)
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterates over values in unspecified map order.
    pub fn iter(&self) -> impl Iterator<Item = &i64> {
        self.values.iter()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum FloatKey {
    Number(u64),
    Nan { bits: u64, identity: u64 },
}

impl MapValueLayout for FloatKey {
    const SOURCE_SIZE: usize = 8;
    const SOURCE_ALIGN: usize = 8;
}

impl FloatKey {
    fn value(self) -> f64 {
        match self {
            Self::Number(bits) | Self::Nan { bits, .. } => f64::from_bits(bits),
        }
    }
}

/// Source float memory-aware set with Go map equality.
pub struct Float64SetWithMemoryUsage {
    values: MemorySet<FloatKey>,
    next_nan_identity: u64,
}

impl Float64SetWithMemoryUsage {
    /// Builds a set from initial values and returns its accounted bytes.
    pub fn new<I>(values: I) -> (Self, i64)
    where
        I: IntoIterator<Item = f64>,
        I::IntoIter: ExactSizeIterator,
    {
        let values = values.into_iter();
        let mut result = Self {
            values: MemorySet::with_capacity(values.len()),
            next_nan_identity: 0,
        };
        for value in values {
            result.insert(value);
        }
        let bytes = i64::try_from(result.accounted_bytes()).unwrap_or(i64::MAX);
        (result, bytes)
    }

    /// Inserts a float and returns the allocation delta.
    pub fn insert(&mut self, value: f64) -> i64 {
        let key = match canonical_float_bits(value) {
            Some(bits) => FloatKey::Number(bits),
            None => {
                let identity = self.next_nan_identity;
                self.next_nan_identity = self.next_nan_identity.wrapping_add(1);
                FloatKey::Nan {
                    bits: value.to_bits(),
                    identity,
                }
            }
        };
        self.values.insert(key).0
    }

    /// Returns whether a float exists. NaN lookups always fail.
    pub fn contains(&self, value: f64) -> bool {
        canonical_float_bits(value)
            .is_some_and(|bits| self.values.contains(&FloatKey::Number(bits)))
    }

    /// Returns the number of entries.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether the set contains no values.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns current accounted table bytes.
    pub const fn accounted_bytes(&self) -> u64 {
        self.values.accounted_bytes()
    }

    /// Iterates over stored values in unspecified map order.
    pub fn iter(&self) -> impl Iterator<Item = f64> + '_ {
        self.values.iter().map(|value| value.value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Item(&'static str);

    impl SetKey for Item {
        fn set_key(&self) -> GoString {
            self.0.into()
        }
    }

    fn keyed(values: &[&'static str]) -> KeyedSet<Item> {
        let mut result = KeyedSet::new();
        result.add(values.iter().copied().map(Item));
        result
    }

    #[test]
    fn test_set_basic() {
        let mut set = keyed(&[]);
        set.add([Item("q1"), Item("q2"), Item("q3")]);
        assert!(set.contains(&Item("q1")));
        assert!(set.contains(&Item("q2")));
        assert!(set.contains(&Item("q3")));
        assert!(!set.contains(&Item("q4")));
        assert_eq!(set.len(), 3);
        assert_eq!(set.to_list(), [Item("q1"), Item("q2"), Item("q3")]);
        set.remove(&Item("q2"));
        assert!(!set.contains(&Item("q2")));
        assert_eq!(set.len(), 2);
        let cloned = set.clone();
        set.remove(&Item("q1"));
        assert!(!set.contains(&Item("q1")));
        assert!(cloned.contains(&Item("q1")));
        assert_eq!(cloned.len(), 2);
    }

    #[test]
    fn test_set_operation() {
        let left = keyed(&["q1", "q2", "q3"]);
        let right = keyed(&["q2", "q3", "q4"]);
        assert_eq!(union(&[&left, &right]).string(), "{q1, q2, q3, q4}");
        assert_eq!(intersection(&[&left, &right]).string(), "{q2, q3}");
        assert_eq!(difference(&left, &right).string(), "{q1}");
        assert_eq!(difference(&right, &left).string(), "{q4}");
    }

    #[test]
    fn test_set_combination() {
        let set = keyed(&["q1", "q2", "q3", "q4"]);
        let render = |count| {
            combinations(&set, count)
                .into_iter()
                .map(|set| set.string().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        assert_eq!(render(1), "{q1}, {q2}, {q3}, {q4}");
        assert_eq!(
            render(2),
            "{q1, q2}, {q1, q3}, {q1, q4}, {q2, q3}, {q2, q4}, {q3, q4}"
        );
        assert_eq!(
            render(3),
            "{q1, q2, q3}, {q1, q2, q4}, {q1, q3, q4}, {q2, q3, q4}"
        );
        assert_eq!(render(4), "{q1, q2, q3, q4}");
        assert_eq!(render(5), "");
    }

    #[test]
    fn test_float64_set() {
        let mut set = Float64Set::new([]);
        let values = [1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0];
        for value in values {
            for _ in 0..5 {
                set.insert(value);
            }
        }
        assert_eq!(set.len(), values.len());
        assert!(values.into_iter().all(|value| set.contains(value)));
        assert!(!set.contains(3.0));
    }

    #[test]
    fn test_int_set() {
        let mut set = IntSet::new([]);
        let values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        for value in values {
            for _ in 0..5 {
                set.insert(value);
            }
        }
        assert_eq!(set.len(), values.len());
        assert!(values.iter().all(|value| set.contains(value)));
        assert!(!set.contains(&11));
    }

    #[test]
    fn test_int64_set() {
        let set = Int64Set::new([1, 2, 3, 4, 5, 6]);
        assert!((1..7).all(|value| set.contains(&value)));
        assert!(!set.contains(&7));
    }

    #[test]
    fn test_string_set() {
        let mut set = StringSet::new([] as [&str; 0]);
        let values = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
        for value in values {
            for _ in 0..5 {
                set.insert(value);
            }
        }
        assert_eq!(set.len(), values.len());
        assert!(values
            .iter()
            .all(|value| set.contains(&GoString::from(*value))));
        assert!(!set.contains(&GoString::from("11")));
        let intersection =
            StringSet::new(["1", "2", "3"]).intersection(&StringSet::new(["4", "2", "3"]));
        assert_eq!(intersection, StringSet::new(["2", "3"]));
        assert_eq!(
            intersection.intersection(&StringSet::new(["4", "5", "3"])),
            StringSet::new(["3"])
        );
        assert!(intersection
            .intersection(&StringSet::new(["4", "5"]))
            .is_empty());
    }

    // Go permits callers to discard constructor, query, and set-operation
    // return values; Rust must not add a `must_use` diagnostic here.
    #[test]
    #[deny(unused_must_use)]
    fn return_values_may_be_ignored_like_go() {
        KeyedSet::<Item>::new();
        let keyed_set = keyed(&["q1"]);
        keyed_set.contains(&Item("q1"));
        keyed_set.to_list();
        keyed_set.len();
        keyed_set.is_empty();
        keyed_set.string();
        list_to_set([Item("q1")]);
        union::<Item>(&[]);
        intersection::<Item>(&[]);
        difference(&keyed_set, &keyed_set);
        combinations(&keyed_set, 1);

        IntSet::new([]);
        let ints = IntSet::new([1]);
        ints.contains(&1);
        ints.len();
        ints.is_empty();

        Int64Set::new([]);
        let int64s = Int64Set::new([1]);
        int64s.contains(&1);
        int64s.len();
        int64s.is_empty();

        StringSet::new([] as [&str; 0]);
        let strings = StringSet::new(["q1"]);
        let key = GoString::from("q1");
        strings.contains(&key);
        strings.intersection(&strings);
        strings.intersection_with_case(&strings, true);
        strings.len();
        strings.is_empty();

        Float64Set::new([]);
        let floats = Float64Set::new([1.0]);
        floats.contains(1.0);
        floats.len();
        floats.is_empty();

        StringToStringMapWithMemoryUsage::new();
        let string_map = StringToStringMapWithMemoryUsage::new().0;
        string_map.get(&key);
        string_map.len();
        string_map.is_empty();

        StringToDecimalMapWithMemoryUsage::new();

        StringSetWithMemoryUsage::new(std::iter::empty::<GoString>());
        let string_memory_set = StringSetWithMemoryUsage::new([key.clone()]).0;
        string_memory_set.contains(&key);
        string_memory_set.len();
        string_memory_set.is_empty();

        Int64SetWithMemoryUsage::new(std::iter::empty::<i64>());
        let int64_memory_set = Int64SetWithMemoryUsage::new([1_i64]).0;
        int64_memory_set.contains(1);
        int64_memory_set.len();
        int64_memory_set.is_empty();

        Float64SetWithMemoryUsage::new(std::iter::empty::<f64>());
        let float_memory_set = Float64SetWithMemoryUsage::new([1.0_f64]).0;
        float_memory_set.contains(1.0);
        float_memory_set.len();
        float_memory_set.is_empty();
        float_memory_set.accounted_bytes();
    }
}
