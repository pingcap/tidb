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

use foldhash::fast::SeedableRandomState;
use foldhash::SharedSeed;
use hashbrown::HashMap;
use std::alloc::Layout;
use std::hash::Hash;
use std::mem::size_of;

/// Maximum Go Swiss-map table capacity before a directory split.
///
/// Hashbrown grows one table instead of a Go extendible-hashing directory, but
/// this source constant remains public for translated memory contracts.
pub const MAX_TABLE_CAPACITY: usize = 1024;

/// Deterministic seed used by the original ABI test.
pub const MOCK_SEED_FOR_TEST: u64 = 4_992_862_800_126_241_206;

/// Go Swiss-map bucket estimate retained for aggregate compatibility.
pub const DEF_BUCKET_MEMORY_USAGE_FOR_MAP_STRING_TO_ANY: u64 = 312;
/// Go Swiss-map bucket estimate retained for aggregate compatibility.
pub const DEF_BUCKET_MEMORY_USAGE_FOR_SET_STRING: u64 = 248;
/// Go Swiss-map bucket estimate retained for aggregate compatibility.
pub const DEF_BUCKET_MEMORY_USAGE_FOR_SET_FLOAT64: u64 = 184;
/// Go Swiss-map bucket estimate retained for aggregate compatibility.
pub const DEF_BUCKET_MEMORY_USAGE_FOR_SET_INT64: u64 = 184;
/// Go Swiss-map bucket estimate retained for aggregate compatibility.
pub const DEF_BUCKET_MEMORY_USAGE_FOR_MAP_STRING_TO_DECIMAL: u64 = 248;
/// Go Swiss-map bucket estimate retained for aggregate compatibility.
pub const DEF_BUCKET_MEMORY_USAGE_FOR_MAP_STRING_TO_STRING: u64 = 312;

#[cfg(any(
    all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64"),
        not(miri)
    ),
    all(
        target_arch = "aarch64",
        target_feature = "neon",
        target_endian = "little",
        not(miri)
    )
))]
const GROUP_WIDTH: usize = 16;

#[cfg(not(any(
    all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64"),
        not(miri)
    ),
    all(
        target_arch = "aarch64",
        target_feature = "neon",
        target_endian = "little",
        not(miri)
    )
)))]
const GROUP_WIDTH: usize = size_of::<usize>();

/// Source-shaped key/value slot and control-group geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MapType {
    /// Bytes in one control group plus its key/value slots.
    pub group_size: usize,
    /// Padded bytes in one key/value slot.
    pub slot_size: usize,
    /// Offset of the value inside the logical slot.
    pub elem_offset: usize,
    /// Number of slots scanned by one hashbrown control group.
    pub group_slots: usize,
}

/// Returns the source-shaped map geometry for `K` and `V`.
#[must_use]
pub fn map_type<K, V>() -> MapType {
    let (slot, elem_offset) = Layout::new::<K>()
        .extend(Layout::new::<V>())
        .expect("key/value layout must fit address space");
    let slot = slot.pad_to_align();
    let controls =
        Layout::from_size_align(GROUP_WIDTH, GROUP_WIDTH).expect("hashbrown group layout");
    let slots = Layout::from_size_align(
        slot.size()
            .checked_mul(GROUP_WIDTH)
            .expect("map group layout must fit address space"),
        slot.align(),
    )
    .expect("map slot layout");
    let (group, _) = controls
        .extend(slots)
        .expect("map group layout must fit address space");

    MapType {
        group_size: group.pad_to_align().size(),
        slot_size: slot.size(),
        elem_offset,
        group_slots: GROUP_WIDTH,
    }
}

fn buckets_from_capacity(capacity: usize) -> usize {
    match capacity {
        0 => 0,
        1..=7 => capacity + 1,
        _ => {
            capacity
                .checked_mul(8)
                .expect("map bucket count must fit address space")
                / 7
        }
    }
}

fn table_allocation_bytes<K, V>(capacity: usize) -> usize {
    let buckets = buckets_from_capacity(capacity);
    if buckets == 0 {
        return 0;
    }
    debug_assert!(buckets.is_power_of_two());

    let entry = Layout::new::<(K, V)>();
    let control_alignment = entry.align().max(GROUP_WIDTH);
    let entries = entry
        .size()
        .checked_mul(buckets)
        .expect("map allocation must fit address space");
    let control_offset = entries
        .checked_add(control_alignment - 1)
        .expect("map allocation must fit address space")
        & !(control_alignment - 1);
    control_offset
        .checked_add(buckets + GROUP_WIDTH)
        .expect("map allocation must fit address space")
}

/// A read-only wrapper exposing the Rust Swiss-table geometry and allocation.
pub struct SwissMapWrap<'a, K, V, S = SeedableRandomState> {
    map: &'a HashMap<K, V, S>,
}

/// Wraps a hashbrown map for allocation and geometry inspection.
#[must_use]
pub const fn to_swiss_map<K, V, S>(map: &HashMap<K, V, S>) -> SwissMapWrap<'_, K, V, S> {
    SwissMapWrap { map }
}

impl<K, V, S> SwissMapWrap<'_, K, V, S> {
    /// Returns the number of elements.
    #[must_use]
    pub fn used(&self) -> usize {
        self.map.len()
    }

    /// Returns the number of elements accepted without reallocating.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.map.capacity()
    }

    /// Returns the map header plus its current table allocation.
    #[must_use]
    pub fn size(&self) -> u64 {
        u64::try_from(
            size_of::<HashMap<K, V, S>>() + table_allocation_bytes::<K, V>(self.map.capacity()),
        )
        .expect("map size must fit u64")
    }

    /// Returns the source-shaped key/value geometry.
    #[must_use]
    pub fn map_type(&self) -> MapType {
        map_type::<K, V>()
    }

    /// Iterates over all entries without exposing raw table slots.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter()
    }
}

/// A hash map with exact allocation-delta tracking for its pinned table.
///
/// Go periodically walks private runtime tables and estimates growth between
/// checkpoints. Hashbrown exposes capacity in constant time, so the Rust owner
/// records the actual allocation transition on every operation. This removes
/// the approximation/checkpoint edge cases while preserving the downstream
/// memory-tracker contract.
pub struct MemAwareMap<K, V> {
    map: HashMap<K, V, SeedableRandomState>,
    bytes: u64,
    seed_for_test: u64,
    clear_sequence: u64,
}

impl<K, V> MemAwareMap<K, V>
where
    K: Eq + Hash,
{
    /// Creates an empty map with the requested initial capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let map = HashMap::with_capacity_and_hasher(capacity, SeedableRandomState::default());
        Self::from_map(map)
    }

    /// Takes ownership of an existing map.
    #[must_use]
    pub fn from_map(map: HashMap<K, V, SeedableRandomState>) -> Self {
        let bytes = to_swiss_map(&map).size();
        Self {
            map,
            bytes,
            seed_for_test: 0,
            clear_sequence: 0,
        }
    }
    /// Returns the number of elements.
    #[must_use]
    pub fn count(&self) -> usize {
        self.map.len()
    }

    /// Returns whether the map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns whether `key` exists.
    #[must_use]
    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    /// Gets a value.
    #[must_use]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    /// Returns the number of elements.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns the current accounted map bytes.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Returns the current map header plus table allocation.
    #[must_use]
    pub fn real_bytes(&self) -> u64 {
        to_swiss_map(&self.map).size()
    }

    /// Inserts or replaces a value and returns the allocation delta.
    pub fn set(&mut self, key: K, value: V) -> i64 {
        let before = self.real_bytes();
        self.map.insert(key, value);
        let after = self.real_bytes();
        self.bytes = after;
        i64::try_from(after).expect("map size must fit i64")
            - i64::try_from(before).expect("map size must fit i64")
    }

    /// Inserts or replaces a value and reports allocation delta and insertion.
    pub fn set_ext(&mut self, key: K, value: V) -> (i64, bool) {
        let inserted = !self.map.contains_key(&key);
        (self.set(key, value), inserted)
    }

    /// Clears entries while retaining capacity, then refreshes test seed state.
    pub fn clear(&mut self) {
        let capacity = self.map.capacity();
        self.map = HashMap::with_capacity_and_hasher(capacity, SeedableRandomState::default());
        self.clear_sequence = self.clear_sequence.wrapping_add(1);
        self.seed_for_test = self.seed_for_test.wrapping_add(1);
        self.bytes = self.real_bytes();
    }

    /// Iterates over entries.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter()
    }

    /// Sets the deterministic source-test seed marker.
    ///
    /// # Panics
    ///
    /// Panics unless the map is empty, matching the Go package.
    pub fn mock_seed_for_test(&mut self) {
        assert!(
            self.map.is_empty(),
            "MockSeedForTest can only be called on empty map"
        );
        let capacity = self.map.capacity();
        self.map = HashMap::with_capacity_and_hasher(
            capacity,
            SeedableRandomState::with_seed(MOCK_SEED_FOR_TEST, SharedSeed::global_fixed()),
        );
        self.seed_for_test = MOCK_SEED_FOR_TEST;
        self.bytes = self.real_bytes();
    }

    #[cfg(test)]
    pub(super) const fn seed_for_test(&self) -> u64 {
        self.seed_for_test
    }

    #[cfg(test)]
    pub(super) const fn clear_sequence(&self) -> u64 {
        self.clear_sequence
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct ComplexBits {
        real: u64,
        imaginary: u64,
    }

    #[test]
    fn TestSwissTable() {
        assert_eq!(MAX_TABLE_CAPACITY, 1024);

        let integer = map_type::<i64, i64>();
        assert_eq!(integer.slot_size, 16);
        assert_eq!(integer.elem_offset, 8);
        assert_eq!(integer.group_slots, GROUP_WIDTH);

        let int32 = map_type::<i32, i32>();
        assert_eq!(int32.slot_size, 8);
        assert_eq!(int32.elem_offset, 4);

        let int8 = map_type::<i8, i8>();
        assert_eq!(int8.slot_size, 2);
        assert_eq!(int8.elem_offset, 1);

        let mixed = map_type::<i64, f64>();
        assert_eq!(mixed.slot_size, 16);
        assert_eq!(mixed.elem_offset, 8);

        let complex = map_type::<ComplexBits, ComplexBits>();
        assert_eq!(complex.slot_size, 32);
        assert_eq!(complex.elem_offset, 16);

        let mut inspected = MemAwareMap::<u64, u64>::new(0);
        inspected.mock_seed_for_test();
        inspected.set(1234, 5678);
        for index in 0..1024_u64 {
            inspected.set(index, index * 2);
        }
        assert_eq!(inspected.len(), 1025);
        let wrapper = to_swiss_map(&inspected.map);
        assert_eq!(wrapper.used(), 1025);
        assert!(wrapper
            .iter()
            .any(|(key, value)| *key == 1234 && *value == 5678));

        let mut strings = MemAwareMap::<String, i32>::new(0);
        strings.mock_seed_for_test();
        for index in 0..2000 {
            strings.set(format!("key-{index}"), index);
        }
        assert_eq!(strings.len(), 2000);
        assert_eq!(strings.bytes(), strings.real_bytes());
        assert!(
            strings.real_bytes()
                > u64::try_from(size_of_val(&strings)).expect("map header must fit u64")
        );

        let mut small = MemAwareMap::<i64, i64>::new(0);
        small.mock_seed_for_test();
        let empty_bytes = small.real_bytes();
        for index in 0..8 {
            small.set(index, index);
        }
        assert!(small.real_bytes() > empty_bytes);
        let eight_bytes = small.real_bytes();
        small.set(9, 9);
        assert!(small.real_bytes() >= eight_bytes);

        let mut aware = MemAwareMap::<ComplexBits, ComplexBits>::new(0);
        aware.mock_seed_for_test();
        let mut delta = i64::try_from(aware.bytes()).expect("initial size");
        for index in 0..(1024 * 50 - 1) {
            let key = ComplexBits {
                real: index,
                imaginary: index,
            };
            delta += aware.set(key, key);
        }
        let size = aware.real_bytes();
        assert_eq!(delta, i64::try_from(size).expect("map size"));
        assert_eq!(aware.bytes(), size);
        assert_eq!(aware.seed_for_test(), MOCK_SEED_FOR_TEST);

        let clear_sequence = aware.clear_sequence();
        aware.clear();
        assert!(aware.is_empty());
        assert_eq!(aware.clear_sequence(), clear_sequence + 1);
        assert_ne!(aware.seed_for_test(), MOCK_SEED_FOR_TEST);
        assert_eq!(aware.real_bytes(), size);
        assert_eq!(aware.bytes(), size);

        aware.mock_seed_for_test();
        for index in 0..1024 {
            let key = ComplexBits {
                real: index,
                imaginary: index,
            };
            let (growth, inserted) = aware.set_ext(key, key);
            assert_eq!(growth, 0);
            assert!(inserted);
        }
        assert_eq!(aware.len(), 1024);
    }

    #[test]
    #[should_panic(expected = "MockSeedForTest can only be called on empty map")]
    fn mock_seed_rejects_a_used_map() {
        let mut map = MemAwareMap::<u64, u64>::new(0);
        map.set(1, 1);
        map.mock_seed_for_test();
    }
}
