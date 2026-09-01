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
use std::cell::Cell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Maximum Go Swiss-map table capacity before a directory split.
const MAX_TABLE_CAPACITY: usize = 1024;

/// Deterministic seed used by the original ABI test.
const MOCK_SEED_FOR_TEST: u64 = 4_992_862_800_126_241_206;

const SOURCE_GROUP_SLOTS: usize = 8;
const SOURCE_MAP_SIZE: u64 = 48;
const SOURCE_TABLE_SIZE: u64 = 32;
const SOURCE_POINTER_SIZE: u64 = 8;
const SOURCE_MAX_GROUPS_PER_TABLE: usize = MAX_TABLE_CAPACITY / SOURCE_GROUP_SLOTS;

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

/// Go runtime size and alignment of a Rust type's source counterpart.
///
/// Go obtains this information from the map's runtime type descriptor. Rust
/// has no equivalent runtime descriptor, so native counterpart types provide
/// the two values explicitly.
pub trait MapValueLayout {
    /// Source value size in bytes.
    const SOURCE_SIZE: usize;
    /// Source value alignment in bytes.
    const SOURCE_ALIGN: usize;
}

macro_rules! primitive_layout {
    ($($type:ty),+ $(,)?) => {
        $(
            impl MapValueLayout for $type {
                const SOURCE_SIZE: usize = std::mem::size_of::<Self>();
                const SOURCE_ALIGN: usize = std::mem::align_of::<Self>();
            }
        )+
    };
}

primitive_layout!(
    (),
    bool,
    i8,
    i16,
    i32,
    i64,
    isize,
    u8,
    u16,
    u32,
    u64,
    usize,
    f32,
    f64,
);

impl MapValueLayout for String {
    const SOURCE_SIZE: usize = 16;
    const SOURCE_ALIGN: usize = 8;
}

impl<T> MapValueLayout for Vec<T> {
    const SOURCE_SIZE: usize = 24;
    const SOURCE_ALIGN: usize = 8;
}

impl<T> MapValueLayout for Box<T> {
    const SOURCE_SIZE: usize = SOURCE_POINTER_SIZE as usize;
    const SOURCE_ALIGN: usize = SOURCE_POINTER_SIZE as usize;
}

/// Source-shaped key/value slot and control-group geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MapType {
    /// Bytes in one control group plus its key/value slots.
    pub group_size: usize,
    /// Padded bytes in one key/value slot.
    pub slot_size: usize,
    /// Offset of the value inside the logical slot.
    pub elem_offset: usize,
}

fn map_type<K, V>() -> MapType
where
    K: MapValueLayout,
    V: MapValueLayout,
{
    let key = Layout::from_size_align(K::SOURCE_SIZE, K::SOURCE_ALIGN)
        .expect("source key layout must be valid");
    let value = Layout::from_size_align(V::SOURCE_SIZE, V::SOURCE_ALIGN)
        .expect("source value layout must be valid");
    let (slot, elem_offset) = key
        .extend(value)
        .expect("key/value layout must fit address space");
    let slot = slot.pad_to_align();
    let controls = Layout::new::<u64>();
    let slots = Layout::from_size_align(
        slot.size()
            .checked_mul(SOURCE_GROUP_SLOTS)
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
    }
}

#[derive(Default)]
struct SourceTable {
    local_depth: u8,
    prefix: u64,
    groups: usize,
    hashes: Vec<u64>,
}

struct SourceMapModel {
    inline_hashes: Vec<u64>,
    global_depth: u8,
    tables: Vec<SourceTable>,
}

impl SourceMapModel {
    fn with_capacity(capacity: usize) -> Self {
        if capacity <= SOURCE_GROUP_SLOTS {
            return Self {
                inline_hashes: Vec::new(),
                global_depth: 0,
                tables: Vec::new(),
            };
        }

        let groups = capacity
            .div_ceil(SOURCE_GROUP_SLOTS - 1)
            .next_power_of_two();
        if groups <= SOURCE_MAX_GROUPS_PER_TABLE {
            return Self {
                inline_hashes: Vec::new(),
                global_depth: 0,
                tables: vec![SourceTable {
                    groups,
                    ..SourceTable::default()
                }],
            };
        }

        let table_count = groups
            .div_ceil(SOURCE_MAX_GROUPS_PER_TABLE)
            .next_power_of_two();
        let global_depth = u8::try_from(table_count.ilog2()).expect("map depth must fit u8");
        let tables = (0..table_count)
            .map(|prefix| SourceTable {
                local_depth: global_depth,
                prefix: u64::try_from(prefix).expect("table prefix must fit u64"),
                groups: SOURCE_MAX_GROUPS_PER_TABLE,
                hashes: Vec::new(),
            })
            .collect();
        Self {
            inline_hashes: Vec::new(),
            global_depth,
            tables,
        }
    }

    fn top_bits(hash: u64, depth: u8) -> u64 {
        if depth == 0 {
            0
        } else {
            hash >> (u64::BITS - u32::from(depth))
        }
    }

    fn table_index(&self, hash: u64) -> usize {
        self.tables
            .iter()
            .position(|table| Self::top_bits(hash, table.local_depth) == table.prefix)
            .expect("source map directory must cover every hash")
    }

    fn insert_hash(&mut self, hash: u64) {
        if self.tables.is_empty() {
            self.inline_hashes.push(hash);
            if self.inline_hashes.len() <= SOURCE_GROUP_SLOTS {
                return;
            }
            self.tables.push(SourceTable {
                groups: 2,
                hashes: std::mem::take(&mut self.inline_hashes),
                ..SourceTable::default()
            });
        } else {
            let table = self.table_index(hash);
            self.tables[table].hashes.push(hash);
        }

        while let Some(table) = self
            .tables
            .iter()
            .position(|table| table.hashes.len() > table.groups * (SOURCE_GROUP_SLOTS - 1))
        {
            if self.tables[table].groups < SOURCE_MAX_GROUPS_PER_TABLE {
                self.tables[table].groups *= 2;
            } else {
                self.split(table);
            }
        }
    }

    fn split(&mut self, table: usize) {
        let old = self.tables.swap_remove(table);
        let depth = old.local_depth + 1;
        let left_prefix = old.prefix << 1;
        let mut left = SourceTable {
            local_depth: depth,
            prefix: left_prefix,
            groups: SOURCE_MAX_GROUPS_PER_TABLE,
            hashes: Vec::new(),
        };
        let mut right = SourceTable {
            local_depth: depth,
            prefix: left_prefix | 1,
            groups: SOURCE_MAX_GROUPS_PER_TABLE,
            hashes: Vec::new(),
        };
        for hash in old.hashes {
            if Self::top_bits(hash, depth) == left.prefix {
                left.hashes.push(hash);
            } else {
                right.hashes.push(hash);
            }
        }
        self.global_depth = self.global_depth.max(depth);
        self.tables.push(left);
        self.tables.push(right);
    }

    fn directory_len(&self) -> usize {
        if self.tables.is_empty() {
            0
        } else {
            1_usize << self.global_depth
        }
    }

    fn cap(&self) -> usize {
        if self.tables.is_empty() {
            SOURCE_GROUP_SLOTS
        } else {
            self.tables
                .iter()
                .map(|table| table.groups * SOURCE_GROUP_SLOTS)
                .sum()
        }
    }

    fn size(&self, group_size: u64) -> u64 {
        if self.tables.is_empty() {
            return SOURCE_MAP_SIZE + group_size;
        }
        SOURCE_MAP_SIZE
            + u64::try_from(self.directory_len()).expect("directory length must fit u64")
                * SOURCE_POINTER_SIZE
            + u64::try_from(self.tables.len()).expect("table count must fit u64")
                * SOURCE_TABLE_SIZE
            + self
                .tables
                .iter()
                .map(|table| {
                    u64::try_from(table.groups).expect("group count must fit u64") * group_size
                })
                .sum::<u64>()
    }
}

fn source_hash<T>(seed: u64, value: &T) -> u64
where
    T: Hash + ?Sized,
{
    let mut hasher = DefaultHasher::new();
    seed.hash(&mut hasher);
    value.hash(&mut hasher);
    hasher.finish()
}

/// A read-only wrapper exposing source Swiss-map geometry and allocation.
pub struct SwissMapWrap<'a, K, V, S = SeedableRandomState>
where
    K: MapValueLayout,
    V: MapValueLayout,
{
    map: &'a HashMap<K, V, S>,
}

/// Wraps a native map for source allocation and geometry inspection.
#[must_use]
pub const fn to_swiss_map<K, V, S>(map: &HashMap<K, V, S>) -> SwissMapWrap<'_, K, V, S>
where
    K: MapValueLayout,
    V: MapValueLayout,
{
    SwissMapWrap { map }
}

impl<K, V, S> SwissMapWrap<'_, K, V, S>
where
    K: Hash + MapValueLayout,
    V: MapValueLayout,
{
    fn source_model(&self) -> SourceMapModel {
        let mut model = SourceMapModel::with_capacity(if self.map.is_empty() {
            self.map.capacity()
        } else {
            0
        });
        for key in self.map.keys() {
            model.insert_hash(source_hash(0, key));
        }
        model
    }

    /// Returns the number of elements.
    #[must_use]
    pub fn used(&self) -> usize {
        self.map.len()
    }

    /// Returns the source map capacity represented by the current allocation.
    #[must_use]
    pub fn cap(&self) -> u64 {
        u64::try_from(self.source_model().cap()).expect("source map capacity must fit u64")
    }

    /// Returns the source map header plus its current table allocation.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.source_model()
            .size(u64::try_from(map_type::<K, V>().group_size).expect("group size must fit u64"))
    }

    /// Returns the source-shaped key/value geometry.
    #[must_use]
    pub fn map_type(&self) -> MapType {
        map_type::<K, V>()
    }

    /// Iterates over all entries without exposing raw table slots.
    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter()
    }

    #[cfg(test)]
    fn directory_len(&self) -> usize {
        self.source_model().directory_len()
    }
}

/// A hash map with Go-compatible checkpointed memory accounting.
pub struct MemAwareMap<K, V> {
    map: HashMap<K, V, SeedableRandomState>,
    group_size: u64,
    next_checkpoint: usize,
    bytes: u64,
    source_capacity: usize,
    real_bytes_floor: Cell<u64>,
    seed_for_test: u64,
    clear_sequence: u64,
}

impl<K, V> MemAwareMap<K, V>
where
    K: Eq + Hash + MapValueLayout,
    V: MapValueLayout,
{
    /// Creates an empty map with the requested initial capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let map = HashMap::with_capacity_and_hasher(capacity, SeedableRandomState::default());
        Self::from_map(map, capacity)
    }

    fn from_map(map: HashMap<K, V, SeedableRandomState>, source_capacity: usize) -> Self {
        let used = map.len();
        let mut source_map = SourceMapModel::with_capacity(source_capacity);
        for key in map.keys() {
            source_map.insert_hash(source_hash(0, key));
        }
        let group_size =
            u64::try_from(map_type::<K, V>().group_size).expect("map group size must fit u64");
        let bytes = source_map.size(group_size);
        Self {
            map,
            group_size,
            next_checkpoint: if used <= SOURCE_GROUP_SLOTS {
                SOURCE_GROUP_SLOTS * 2
            } else {
                used + used.min(MAX_TABLE_CAPACITY)
            },
            bytes,
            source_capacity,
            real_bytes_floor: Cell::new(bytes),
            seed_for_test: 0,
            clear_sequence: 0,
        }
    }

    /// Replaces the map and returns its initial source memory size.
    pub fn init(&mut self, map: HashMap<K, V, SeedableRandomState>) -> i64 {
        *self = Self::from_map(map, 0);
        i64::try_from(self.bytes).expect("map size must fit i64")
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

    /// Removes a value through the source map field.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let removed = self.map.remove(key);
        if removed.is_some() && self.map.is_empty() {
            self.seed_for_test = self.seed_for_test.wrapping_add(1);
        }
        removed
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
        let mut source_map = SourceMapModel::with_capacity(self.source_capacity);
        for key in self.map.keys() {
            source_map.insert_hash(source_hash(self.seed_for_test, key));
        }
        let bytes = self
            .real_bytes_floor
            .get()
            .max(source_map.size(self.group_size));
        self.real_bytes_floor.set(bytes);
        bytes
    }

    /// Inserts or replaces a value and returns Go's checkpointed memory delta.
    pub fn set(&mut self, key: K, value: V) -> i64 {
        self.map.insert(key, value);
        let used = self.map.len();
        if used < self.next_checkpoint {
            return 0;
        }

        let old_bytes = self.bytes;
        self.bytes = self.bytes.max(approx_size(
            self.group_size,
            u64::try_from(used).expect("map length must fit u64"),
        ));
        self.next_checkpoint = used + used.min(MAX_TABLE_CAPACITY);
        i64::try_from(self.bytes).expect("map size must fit i64")
            - i64::try_from(old_bytes).expect("map size must fit i64")
    }

    /// Inserts or replaces a value and reports allocation delta and insertion.
    pub fn set_ext(&mut self, key: K, value: V) -> (i64, bool) {
        let used = self.map.len();
        let delta = self.set(key, value);
        (delta, used != self.map.len())
    }

    /// Clears entries while retaining capacity, then refreshes test seed state.
    pub fn clear(&mut self) {
        let _ = self.real_bytes();
        let capacity = self.map.capacity();
        self.map = HashMap::with_capacity_and_hasher(capacity, SeedableRandomState::default());
        self.clear_sequence = self.clear_sequence.wrapping_add(1);
        self.seed_for_test = self.seed_for_test.wrapping_add(1);
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

fn approx_size(group_size: u64, max_len: u64) -> u64 {
    const RATIO: u64 = 204;
    group_size
        .checked_mul(max_len)
        .and_then(|size| size.checked_mul(RATIO))
        .expect("map approximation must fit u64")
        / 1000
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

    impl MapValueLayout for ComplexBits {
        const SOURCE_SIZE: usize = 16;
        const SOURCE_ALIGN: usize = 8;
    }

    #[test]
    fn TestSwissTable() {
        assert_eq!(MAX_TABLE_CAPACITY, 1024);

        let integer = map_type::<i64, i64>();
        assert_eq!(integer.group_size, 136);
        assert_eq!(integer.slot_size, 16);
        assert_eq!(integer.elem_offset, 8);

        let int32 = map_type::<i32, i32>();
        assert_eq!(int32.group_size, 72);
        assert_eq!(int32.slot_size, 8);
        assert_eq!(int32.elem_offset, 4);

        let int8 = map_type::<i8, i8>();
        assert_eq!(int8.group_size, 24);
        assert_eq!(int8.slot_size, 2);
        assert_eq!(int8.elem_offset, 1);

        let mixed = map_type::<i64, f64>();
        assert_eq!(mixed.group_size, 136);
        assert_eq!(mixed.slot_size, 16);
        assert_eq!(mixed.elem_offset, 8);

        let complex = map_type::<ComplexBits, ComplexBits>();
        assert_eq!(complex.group_size, 264);
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
        assert_eq!(
            (wrapper.map_type().group_size - 8) / wrapper.map_type().slot_size,
            SOURCE_GROUP_SLOTS
        );
        assert!(wrapper
            .iter()
            .any(|(key, value)| *key == 1234 && *value == 5678));
        let original_seed = inspected.seed_for_test();
        let keys = inspected.map.keys().copied().collect::<Vec<_>>();
        for key in keys {
            inspected.remove(&key);
        }
        assert_ne!(inspected.seed_for_test(), original_seed);

        let mut strings = MemAwareMap::<String, i32>::new(0);
        strings.mock_seed_for_test();
        for index in 0..2000 {
            strings.set(format!("key-{index}"), index);
        }
        assert_eq!(strings.len(), 2000);
        let strings = to_swiss_map(&strings.map);
        assert_eq!(strings.used(), 2000);
        assert_eq!(strings.map_type().group_size, 200);
        assert_eq!(strings.directory_len(), 4);
        assert_eq!(strings.size(), 102_608);

        let mut small = MemAwareMap::<i64, i64>::new(0);
        small.mock_seed_for_test();
        assert_eq!(small.len(), 0);
        assert_eq!(small.real_bytes(), 184);
        for index in 0..8 {
            assert_eq!(small.set(index, index), 0);
        }
        assert_eq!(small.len(), 8);
        assert_eq!(small.real_bytes(), 184);
        assert_eq!(small.set(9, 9), 0);
        assert_eq!(small.len(), 9);
        assert_eq!(small.real_bytes(), 360);

        let mut aware = MemAwareMap::<ComplexBits, ComplexBits>::new(0);
        aware.mock_seed_for_test();
        let mut delta = i64::try_from(aware.bytes()).expect("initial size");
        for index in 0..(1024 * 50 - 1) {
            let key = ComplexBits {
                real: index,
                imaginary: index,
            };
            let growth = aware.set(key, key);
            delta += growth;
            if growth > 0 {
                let real = aware.real_bytes();
                let expected_minimum = real * 75 / 100;
                assert!(
                    aware.bytes() >= expected_minimum,
                    "ApproxSize {}, RealSize {real}, index {index}, expMin {expected_minimum}",
                    aware.bytes()
                );
                assert!(
                    approx_size(
                        aware.group_size,
                        u64::try_from(aware.len()).expect("map length must fit u64")
                    ) >= expected_minimum
                );
            }
        }
        let size = aware.real_bytes();
        assert_eq!(size, 2_165_296);
        assert_eq!(delta, 2_702_278);
        assert_eq!(delta, i64::try_from(aware.bytes()).expect("map size"));
        assert_eq!(aware.seed_for_test(), MOCK_SEED_FOR_TEST);

        let clear_sequence = aware.clear_sequence();
        aware.clear();
        assert!(aware.is_empty());
        assert_eq!(aware.clear_sequence(), clear_sequence + 1);
        assert_ne!(aware.seed_for_test(), MOCK_SEED_FOR_TEST);
        assert_eq!(aware.real_bytes(), size);
        assert_eq!(delta, i64::try_from(aware.bytes()).expect("map size"));

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
}
