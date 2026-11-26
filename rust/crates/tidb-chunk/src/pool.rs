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

//! `pkg/util/chunk/pool.go`: capacity-bucketed, synchronized column reuse.
//!
//! Go's global map is keyed by the histogram bucket's initial capacity and
//! each [`Pool`] has one `sync.Pool` for every physical column width used by a
//! chunk. Rust transfers ownership of a [`Column`] into the matching sharded
//! pool and transfers it back on `get_chunk`.

use crate::chunk::Chunk;
use crate::column::{get_fixed_len, Column, VAR_ELEM_LEN};
use crate::column_slot::ColumnSlot;
use parking_lot::Mutex;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{OnceLock, PoisonError, RwLock};
use tidb_datatype::FieldType;
use tidb_zeropool::ThreadShards;

const WIDTH_COUNT: usize = 5;
const MIN_MIXED_BATCH_SLOTS: usize = 2;
const MAX_SPARE_SLOTS: usize = 2;
// Only uniquely owned slots enter these lists.
type PooledColumns = [Vec<ColumnSlot>; WIDTH_COUNT];
type ColumnShard = Mutex<PooledShard>;

#[derive(Default)]
struct PooledShard {
    columns: PooledColumns,
    // A mixed-width batch is the only cached column storage in this shard
    // until it is reused intact or distributed into the width lists.
    batch: Vec<ColumnSlot>,
    order: Vec<u8>,
    positions: [Vec<usize>; WIDTH_COUNT],
    destinations: Vec<usize>,
    // Empty allocation from a partial return, bounded by MAX_SPARE_SLOTS.
    spare_slots: Vec<ColumnSlot>,
}

impl PooledShard {
    fn take_batch(&mut self, fields: &[FieldType]) -> Option<Vec<ColumnSlot>> {
        let prefix = self.batch.len().checked_sub(fields.len())?;
        if prefix != 0 {
            if let [field] = fields {
                return self.take_one(width_index(get_fixed_len(field)));
            }
            return self
                .take_reordered_batch(fields, prefix)
                .or_else(|| self.take_partial_batch(fields));
        }
        if !self.order.iter().zip(fields).all(|(&bucket, field)| {
            usize::from(bucket) == width_index(get_fixed_len(field))
        }) {
            return self.take_reordered_batch(fields, 0);
        }
        let mut slots = std::mem::take(&mut self.batch);
        // Two distinct widths each have a single slot to pop in place.
        if matches!(self.order.as_slice(), [first, second] if first != second) {
            return Some(slots);
        }
        // Pop order is the reverse of put order for each physical width.
        for positions in &self.positions {
            for (&left, &right) in positions
                .iter()
                .zip(positions.iter().rev())
                .take(positions.len() / 2)
            {
                slots.swap(left, right);
            }
        }
        Some(slots)
    }

    fn take_reordered_batch(
        &mut self,
        fields: &[FieldType],
        prefix: usize,
    ) -> Option<Vec<ColumnSlot>> {
        let mut next = self.positions.each_ref().map(Vec::len);
        self.destinations.resize(fields.len(), 0);
        for (destination, field) in fields.iter().enumerate() {
            let bucket = width_index(get_fixed_len(field));
            next[bucket] = next[bucket].checked_sub(1)?;
            let source = self.positions[bucket][next[bucket]].checked_sub(prefix)?;
            self.destinations[source] = destination;
        }

        // Each requested slot lies in the suffix and is selected once.
        let mut slots = if prefix == 0 {
            std::mem::take(&mut self.batch)
        } else {
            self.order.truncate(prefix);
            for (positions, kept) in self.positions.iter_mut().zip(next) {
                positions.truncate(kept);
            }
            split_slots(&mut self.batch, prefix, &mut self.spare_slots)
        };
        // Move each destination together with its slot.
        for index in 0..slots.len() {
            while self.destinations[index] != index {
                let destination = self.destinations[index];
                slots.swap(index, destination);
                self.destinations.swap(index, destination);
            }
        }
        Some(slots)
    }

    fn take_one(&mut self, bucket: usize) -> Option<Vec<ColumnSlot>> {
        let source = self.positions[bucket].pop()?;
        let slot = self.batch.remove(source);
        self.order.remove(source);
        if source != self.batch.len() {
            // Removing one slot shifts only the positions after it.
            for positions in &mut self.positions {
                let start = positions.partition_point(|&index| index < source);
                for position in &mut positions[start..] {
                    *position -= 1;
                }
            }
        }
        let mut slots =
            take_spare(&mut self.spare_slots, 1).unwrap_or_else(|| Vec::with_capacity(1));
        slots.push(slot);
        Some(slots)
    }

    fn take_partial_batch(&mut self, fields: &[FieldType]) -> Option<Vec<ColumnSlot>> {
        let mut remaining = self.positions.each_ref().map(Vec::len);
        self.destinations.resize(fields.len(), 0);
        for (destination, field) in fields.iter().enumerate() {
            let bucket = width_index(get_fixed_len(field));
            remaining[bucket] = remaining[bucket].checked_sub(1)?;
            self.destinations[destination] = self.positions[bucket][remaining[bucket]];
        }

        // Within each width, every slot at or after the first selected
        // slot is taken. Keep the older entries in their original order.
        let cutoff: [usize; WIDTH_COUNT] = std::array::from_fn(|bucket| {
            self.positions[bucket]
                .get(remaining[bucket])
                .copied()
                .unwrap_or(self.batch.len())
        });
        let mut selected = take_spare(&mut self.spare_slots, fields.len())
            .unwrap_or_else(|| Vec::with_capacity(fields.len()));
        selected.extend(
            self.destinations
                .iter()
                .map(|&source| std::mem::take(&mut self.batch[source])),
        );
        for positions in &mut self.positions {
            positions.clear();
        }
        let mut old_index = 0;
        let mut kept = 0;
        // Compact the width tags alongside the slots. Each write is at or
        // before the entry being read.
        self.batch.retain(|_| {
            let bucket = usize::from(self.order[old_index]);
            let keep = old_index < cutoff[bucket];
            old_index += 1;
            if keep {
                self.order[kept] = bucket as u8;
                self.positions[bucket].push(kept);
                kept += 1;
            }
            keep
        });
        self.order.truncate(kept);
        Some(selected)
    }

    fn can_save_batch(&self, slots: &[ColumnSlot]) -> bool {
        self.columns.iter().all(Vec::is_empty)
            && slots.iter().all(|slot| matches!(slot, ColumnSlot::Owned(_)))
    }

    fn save_batch(&mut self, mut slots: Vec<ColumnSlot>) {
        self.order.clear();
        self.order.reserve(slots.len());
        for positions in &mut self.positions {
            positions.clear();
        }
        for (index, slot) in slots.iter_mut().enumerate() {
            let column = slot.owned_mut().unwrap();
            let bucket = width_index(column.type_size());
            self.order.push(bucket as u8);
            self.positions[bucket].push(index);
            column.reset();
        }
        self.batch = slots;
    }

    fn append_batch(&mut self, mut slots: Vec<ColumnSlot>) {
        // Check the width and reset the incoming columns before touching
        // the existing batch. Its index lists remain valid if reset unwinds.
        for slot in &mut slots {
            let column = slot.owned_mut().unwrap();
            width_index(column.type_size());
            column.reset();
        }
        let start = self.batch.len();
        self.order.reserve(slots.len());
        for (index, slot) in slots.iter().enumerate() {
            let bucket = width_index(slot.owned_column().unwrap().type_size());
            self.order.push(bucket as u8);
            self.positions[bucket].push(start + index);
        }
        self.batch.append(&mut slots);
        self.save_spare(slots);
    }

    fn save_spare(&mut self, slots: Vec<ColumnSlot>) {
        debug_assert!(slots.is_empty());
        if slots.capacity() <= MAX_SPARE_SLOTS && slots.capacity() > self.spare_slots.capacity() {
            self.spare_slots = slots;
        }
    }

    #[cold]
    fn materialize_batch(&mut self) {
        for (slot, &bucket) in std::mem::take(&mut self.batch).into_iter().zip(&self.order) {
            self.columns[usize::from(bucket)].push(slot);
        }
    }

    fn pop(&mut self, bucket: usize) -> Option<ColumnSlot> {
        if !self.batch.is_empty() && !self.positions[bucket].is_empty() {
            self.materialize_batch();
        }
        self.columns[bucket].pop()
    }

    #[cfg(test)]
    fn cached(&self, index: usize) -> usize {
        self.columns[index].len()
            + if self.batch.is_empty() {
                0
            } else {
                self.positions[index].len()
            }
    }
}

fn take_spare(spare: &mut Vec<ColumnSlot>, count: usize) -> Option<Vec<ColumnSlot>> {
    let capacity = spare.capacity();
    (capacity >= count).then(|| std::mem::take(spare))
}

fn split_slots(
    slots: &mut Vec<ColumnSlot>,
    start: usize,
    spare: &mut Vec<ColumnSlot>,
) -> Vec<ColumnSlot> {
    if let Some(mut tail) = take_spare(spare, slots.len() - start) {
        tail.extend(slots.drain(start..));
        tail
    } else {
        slots.split_off(start)
    }
}

fn width_index(type_size: i64) -> usize {
    match type_size {
        VAR_ELEM_LEN => 0,
        4 => 1,
        8 => 2,
        16 => 3,
        40 => 4,
        other => panic!("unsupported chunk pool physical width {other}"),
    }
}

/// Go `chunk.Pool`: a synchronized column pool for one initial-capacity
/// bucket. Every shard keeps independent lists for the five physical widths.
pub struct Pool {
    init_capacity: usize,
    columns: OnceLock<ThreadShards<ColumnShard>>,
}

impl fmt::Debug for Pool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Pool")
            .field("init_capacity", &self.init_capacity)
            .finish_non_exhaustive()
    }
}

impl Pool {
    /// Go `NewPool`.
    #[must_use]
    pub fn new(init_capacity: usize) -> Self {
        Self {
            init_capacity,
            columns: OnceLock::new(),
        }
    }

    /// The initial capacity identifying this global pool bucket.
    #[must_use]
    pub fn init_capacity(&self) -> usize {
        self.init_capacity
    }

    fn columns(&self) -> &ThreadShards<ColumnShard> {
        self.columns.get_or_init(|| ThreadShards::new(ColumnShard::default))
    }

    #[cfg(test)]
    fn cached(&self, width: i64) -> usize {
        let index = width_index(width);
        self.columns()
            .iter()
            .map(|shard| shard.lock().cached(index))
            .sum()
    }

    /// Go `Pool.GetChunk`.
    #[must_use]
    pub fn get_chunk(&self, fields: &[FieldType]) -> Chunk {
        if fields.is_empty() {
            return Chunk::from_reusable_columns([], self.init_capacity, self.init_capacity);
        }
        let shards = self.columns();
        let home = shards.local();
        let mut columns = home.lock();
        let width = get_fixed_len(&fields[0]);
        let PooledShard {
            columns: lists,
            spare_slots,
            ..
        } = &mut *columns;
        let list = &mut lists[width_index(width)];
        if list.len() >= fields.len() && fields[1..].iter().all(|field| get_fixed_len(field) == width)
        {
            let mut slots = if list.len() == fields.len() {
                std::mem::take(list)
            } else {
                split_slots(list, list.len() - fields.len(), spare_slots)
            };
            drop(columns);
            // Match the order in which individual slots would have been popped.
            slots.reverse();
            return Chunk::from_reusable_slots(slots, self.init_capacity, self.init_capacity);
        }
        if !columns.batch.is_empty() {
            if let Some(slots) = columns.take_batch(fields) {
                return Chunk::from_reusable_slots(slots, self.init_capacity, self.init_capacity);
            }
            if fields.iter().any(|field| {
                !columns.positions[width_index(get_fixed_len(field))].is_empty()
            }) {
                columns.materialize_batch();
            }
        }
        let mut exhausted = [false; WIDTH_COUNT];
        let values = fields.iter().map(|field| {
            let type_size = get_fixed_len(field);
            let bucket = width_index(type_size);
            if let Some(column) = columns.columns[bucket].pop() {
                return column;
            }
            if !exhausted[bucket] {
                // Never wait on a foreign shard while holding the local lock.
                let remote = shards
                    .iter()
                    .filter(|&shard| !std::ptr::eq(shard, home))
                    .find_map(|shard| shard.try_lock()?.pop(bucket));
                if let Some(column) = remote {
                    return column;
                }
                exhausted[bucket] = true;
            }
            ColumnSlot::new(Column::new_column_with_type_size(type_size, self.init_capacity))
        });
        Chunk::from_reusable_slots(values.collect(), self.init_capacity, self.init_capacity)
    }

    /// Go `Pool.PutChunk`. Reclaims each unique owner by its physical width.
    pub fn put_chunk(&self, fields: &[FieldType], chunk: &mut Chunk) {
        assert_eq!(
            fields.len(),
            chunk.num_cols(),
            "Pool.PutChunk field/column width mismatch"
        );
        let mut slots = chunk.take_column_slots();
        if slots.is_empty() {
            return;
        }
        let mut columns = self.columns().local().lock();
        // Keep moderately sized backing allocations. A pruned chunk can
        // retain a large capacity even when only a few slots remain.
        if slots.capacity() <= slots.len().saturating_mul(2) && columns.batch.is_empty() {
            if let ColumnSlot::Owned(first) = &slots[0] {
                let width = first.type_size();
                let list = &mut columns.columns[width_index(width)];
                if slots[1..].iter().all(|slot| {
                    matches!(slot, ColumnSlot::Owned(column) if column.type_size() == width)
                }) {
                    for slot in &mut slots {
                        slot.owned_mut().unwrap().reset();
                    }
                    if list.is_empty() {
                        *list = slots;
                    } else {
                        list.append(&mut slots);
                        columns.save_spare(slots);
                    }
                    return;
                }
            }
            if slots.len() >= MIN_MIXED_BATCH_SLOTS && columns.can_save_batch(&slots) {
                columns.save_batch(slots);
                return;
            }
        }
        if !columns.batch.is_empty() {
            if slots.iter().all(|slot| matches!(slot, ColumnSlot::Owned(_))) {
                columns.append_batch(slots);
                return;
            }
            columns.materialize_batch();
        }
        if slots.len() > 1 {
            let mut shared_owners = HashSet::new();
            slots.retain(|slot| match slot {
                ColumnSlot::Owned(_) | ColumnSlot::Registered(_) => true,
                ColumnSlot::Shared(owner) => shared_owners.insert(std::sync::Arc::as_ptr(owner)),
            });
        }
        for slot in slots {
            let mut reusable = match slot {
                ColumnSlot::Owned(_) => slot,
                other => match other.into_unique_column() {
                    Ok(column) => ColumnSlot::new(column),
                    Err(_) => continue,
                },
            };
            let owned = reusable.owned_mut().expect("pooled slot owns a column");
            let bucket = width_index(owned.type_size());
            owned.reset();
            columns.columns[bucket].push(reusable);
        }
    }
}

type GlobalPools = RwLock<HashMap<usize, &'static Pool>>;

static GLOBAL_CHUNK_POOL: OnceLock<GlobalPools> = OnceLock::new();

thread_local! {
    static LAST_GLOBAL_POOL: Cell<Option<(usize, &'static Pool)>> = const { Cell::new(None) };
}

fn global_pools() -> &'static GlobalPools {
    GLOBAL_CHUNK_POOL.get_or_init(|| RwLock::new(HashMap::new()))
}

fn pool_for(init_capacity: usize) -> &'static Pool {
    LAST_GLOBAL_POOL
        .try_with(|last| {
            if let Some((capacity, pool)) = last.get() {
                if capacity == init_capacity {
                    return pool;
                }
            }
            let pool = lookup_pool(init_capacity);
            last.set(Some((init_capacity, pool)));
            pool
        })
        .unwrap_or_else(|_| lookup_pool(init_capacity))
}

fn lookup_pool(init_capacity: usize) -> &'static Pool {
    if let Some(pool) = global_pools()
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&init_capacity)
        .copied()
    {
        return pool;
    }

    let mut pools = global_pools()
        .write()
        .unwrap_or_else(PoisonError::into_inner);
    // Global capacity buckets already live for the process's lifetime.
    *pools
        .entry(init_capacity)
        .or_insert_with(|| Box::leak(Box::new(Pool::new(init_capacity))))
}

/// Go `NewChunkFromPoolWithCapacity` / `getChunkFromPool`.
#[must_use]
pub fn new_chunk_from_pool_with_capacity(fields: &[FieldType], init_capacity: usize) -> Chunk {
    pool_for(init_capacity).get_chunk(fields)
}

/// Go `putChunkFromPool`, used by [`Chunk::destroy`](crate::chunk::Chunk::destroy).
pub(crate) fn put_chunk_from_pool(init_capacity: usize, fields: &[FieldType], chunk: &mut Chunk) {
    pool_for(init_capacity).put_chunk(fields, chunk);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tidb_datatype::FieldTypeCode;

    fn fields() -> Vec<FieldType> {
        vec![
            FieldType::new(FieldTypeCode::Varchar),
            FieldType::new(FieldTypeCode::Json),
            FieldType::new(FieldTypeCode::Float),
            FieldType::new(FieldTypeCode::NewDecimal),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::Timestamp),
            FieldType::new(FieldTypeCode::Datetime),
        ]
    }

    /// Go `TestNewPool` and `TestPoolGetChunk`.
    #[test]
    fn pool_builds_every_source_bucket_and_chunk_shape() {
        let capacity = 1024;
        let pool = Pool::new(capacity);
        assert_eq!(pool.init_capacity(), capacity);
        let chunk = pool.get_chunk(&fields());
        assert_eq!(chunk.num_cols(), 8);
        assert_eq!(chunk.capacity(), capacity);
        assert_eq!(chunk.required_rows(), capacity);
        assert_eq!(chunk.column(0).type_size(), VAR_ELEM_LEN);
        assert_eq!(chunk.column(1).type_size(), VAR_ELEM_LEN);
        assert_eq!(chunk.column(2).type_size(), 4);
        assert_eq!(chunk.column(3).type_size(), 40);
        for index in 4..8 {
            assert_eq!(chunk.column(index).type_size(), 8);
        }
        assert_eq!(chunk.column(2).get_data_cap(), capacity * 4);
        assert_eq!(chunk.column(3).get_data_cap(), capacity * 40);
        assert_eq!(chunk.column(4).get_data_cap(), capacity * 8);
    }

    /// Go `TestPoolPutChunk`: putting removes the chunk's column references;
    /// the next get receives the reset owned columns.
    #[test]
    fn put_clears_chunk_and_reuses_reset_columns() {
        let fields = fields();
        let pool = Pool::new(8);
        let mut chunk = pool.get_chunk(&fields);
        chunk.append_string(0, "keep capacity");
        chunk.append_int64(5, 7);
        assert_eq!(chunk.get_row(0).get_int64(5), 7);
        let get_data_cap = chunk.column(0).get_data_cap();
        pool.put_chunk(&fields, &mut chunk);
        assert_eq!(chunk.num_cols(), 0);
        assert_eq!(pool.cached(VAR_ELEM_LEN), 2);
        assert_eq!(pool.cached(4), 1);
        assert_eq!(pool.cached(8), 4);
        assert_eq!(pool.cached(40), 1);

        let reused = pool.get_chunk(&fields);
        assert_eq!(reused.num_rows(), 0);
        assert_eq!(reused.column(0).get_data_cap(), get_data_cap);
    }

    #[test]
    fn put_chunk_buckets_aliased_owner_by_physical_width() {
        let fields = vec![
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::NewDecimal),
        ];
        let pool = Pool::new(8);
        let mut chunk = pool.get_chunk(&fields);
        chunk.make_ref(1, 0);
        assert!(chunk.columns_share_identity(0, &chunk, 1));
        assert_eq!(chunk.column(0).type_size(), 40);
        assert_eq!(chunk.column(1).type_size(), 40);

        pool.put_chunk(&fields, &mut chunk);
        assert_eq!(chunk.num_cols(), 0);
        assert_eq!(pool.cached(8), 0);
        assert_eq!(pool.cached(40), 1);

        let reused = pool.get_chunk(&fields);
        assert_eq!(reused.column(0).type_size(), 8);
        assert_eq!(reused.column(1).type_size(), 40);
        assert!(!reused.columns_share_identity(0, &reused, 1));
        assert_eq!(pool.cached(8), 0);
        assert_eq!(pool.cached(40), 0);
    }

    #[test]
    fn concurrent_pool_reuse_keeps_a_cross_chunk_alias_live() {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong); 3];
        let pool = Pool::new(8);
        let mut chunk = pool.get_chunk(&fields);
        for (column, value) in [17, 19, 23].into_iter().enumerate() {
            chunk.append_int64(column, value);
        }
        let mut alias = Chunk::new_chunk_with_capacity(&fields[..1], 8);
        alias.make_ref_to(0, &mut chunk, 0).unwrap();
        chunk.make_ref(0, 1);
        pool.put_chunk(&fields, &mut chunk);
        assert_eq!(pool.cached(8), 1);

        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let mut reused = pool.get_chunk(&fields);
                    assert_eq!(pool.cached(8), 0);
                    for column in 0..fields.len() {
                        reused.append_int64(column, 29);
                    }
                    pool.put_chunk(&fields, &mut reused);
                })
                .join()
                .unwrap();
        });
        assert_eq!(alias.get_row(0).get_int64(0), 17);
        assert_eq!(pool.cached(8), 3);
    }

    #[test]
    fn wide_chunks_preserve_the_pop_order_of_each_width() {
        let cases = [
            FieldTypeCode::LongLong,
            FieldTypeCode::Float,
            FieldTypeCode::Varchar,
        ];
        for mixed in [false, true] {
            let fields: Vec<_> = (0..48)
                .map(|index| FieldType::new(cases[if mixed { index % cases.len() } else { 0 }]))
                .collect();
            for (split, peer_first) in [(false, false), (true, false), (true, true)] {
                let pool = Pool::new(1);
                let mut source = Chunk::from_reusable_columns(
                    fields.iter().enumerate().map(|(index, field)| {
                        Column::new_column_with_type_size(get_fixed_len(field), index + 1)
                    }),
                    1,
                    1,
                );
                let mut expected = HashMap::<i64, Vec<usize>>::new();
                for (index, field) in fields.iter().enumerate() {
                    let column = source.column(index);
                    expected
                        .entry(get_fixed_len(field))
                        .or_default()
                        .push(column.get_data_cap());
                }
                pool.put_chunk(&fields, &mut source);
                assert_eq!(source.num_cols(), 0);

                let mid = if split { fields.len() / 2 } else { fields.len() };
                let verify = |part: &[FieldType], expected: &mut HashMap<i64, Vec<usize>>| {
                    let chunk = pool.get_chunk(part);
                    for (index, field) in part.iter().enumerate() {
                        let width = get_fixed_len(field);
                        let column = chunk.column(index);
                        assert_eq!(column.type_size(), width);
                        assert_eq!(
                            column.get_data_cap(),
                            expected.get_mut(&width).unwrap().pop().unwrap(),
                        );
                    }
                };
                if peer_first {
                    std::thread::scope(|scope| {
                        scope
                            .spawn(|| verify(&fields[..mid], &mut expected))
                            .join()
                            .unwrap();
                    });
                    verify(&fields[mid..], &mut expected);
                } else {
                    verify(&fields[..mid], &mut expected);
                }
                if split && !peer_first {
                    std::thread::scope(|scope| {
                        scope
                            .spawn(|| verify(&fields[mid..], &mut expected))
                            .join()
                            .unwrap();
                    });
                }
            }
        }
    }

    #[test]
    fn returning_a_second_width_mix_keeps_existing_buffers_in_pop_order() {
        let small = FieldType::new(FieldTypeCode::Float);
        let wide = FieldType::new(FieldTypeCode::LongLong);
        let fields: Vec<_> = (0..16).flat_map(|_| [small.clone(), wide.clone()]).collect();
        let pool = Pool::new(1);
        let mut first = Chunk::from_reusable_columns(
            fields.iter().map(|field| Column::new_column_with_type_size(get_fixed_len(field), 3)),
            3,
            3,
        );
        let mut second = Chunk::from_reusable_columns(
            fields.iter().map(|field| Column::new_column_with_type_size(get_fixed_len(field), 5)),
            5,
            5,
        );
        pool.put_chunk(&fields, &mut first);
        pool.put_chunk(&fields, &mut second);
        for capacity in [5, 3] {
            let chunk = pool.get_chunk(&fields);
            for (index, field) in fields.iter().enumerate() {
                assert_eq!(
                    chunk.column(index).get_data_cap(),
                    capacity * get_fixed_len(field) as usize,
                );
            }
        }
    }

    #[test]
    fn reordered_batch_preserves_per_width_pop_order() {
        let fields: Vec<_> = (0..4)
            .flat_map(|_| {
                [
                    FieldTypeCode::LongLong,
                    FieldTypeCode::Float,
                    FieldTypeCode::Varchar,
                ]
                .map(FieldType::new)
            })
            .collect();
        for different_counts in [false, true] {
            let pool = Pool::new(1);
            let mut source = Chunk::from_reusable_columns(
                fields.iter().enumerate().map(|(index, field)| {
                    Column::new_column_with_type_size(get_fixed_len(field), index + 2)
                }),
                1,
                1,
            );
            let mut capacities = HashMap::<i64, Vec<usize>>::new();
            for (index, field) in fields.iter().enumerate() {
                capacities
                    .entry(get_fixed_len(field))
                    .or_default()
                    .push(source.column(index).get_data_cap());
            }
            pool.put_chunk(&fields, &mut source);

            let mut reordered = fields.clone();
            reordered.rotate_left(1);
            if different_counts {
                reordered[0] = FieldType::new(FieldTypeCode::LongLong);
            }
            let reused = pool.get_chunk(&reordered);
            for (index, field) in reordered.iter().enumerate() {
                let width = get_fixed_len(field);
                let expected = capacities
                    .get_mut(&width)
                    .unwrap()
                    .pop()
                    .unwrap_or_else(|| Column::new_column_with_type_size(width, 1).get_data_cap());
                assert_eq!(reused.column(index).type_size(), width);
                assert_eq!(reused.column(index).get_data_cap(), expected);
            }
            assert_eq!(pool.cached(4), usize::from(different_counts));
        }
    }

    #[test]
    fn partial_take_and_return_preserve_older_and_newer_slots() {
        let fixed = FieldType::new(FieldTypeCode::LongLong);
        let other = FieldType::new(FieldTypeCode::Varchar);
        let float = FieldType::new(FieldTypeCode::Float);
        let decimal = FieldType::new(FieldTypeCode::NewDecimal);
        let mixed: Vec<_> = (0..2)
            .flat_map(|_| [fixed.clone(), other.clone(), float.clone(), decimal.clone()])
            .collect();
        for (fields, requested) in [
            (vec![fixed.clone(); 8], vec![fixed.clone(); 4]),
            (mixed.clone(), mixed[..4].to_vec()),
            (mixed, vec![fixed.clone(), fixed.clone(), other]),
        ] {
            let pool = Pool::new(1);
            let mut source = Chunk::from_reusable_columns(
                fields.iter().enumerate().map(|(index, field)| {
                    Column::new_column_with_type_size(get_fixed_len(field), index + 2)
                }),
                1,
                1,
            );
            let mut expected = HashMap::<i64, Vec<usize>>::new();
            for (index, field) in fields.iter().enumerate() {
                expected
                    .entry(get_fixed_len(field))
                    .or_default()
                    .push(source.column(index).get_data_cap());
            }
            pool.put_chunk(&fields, &mut source);

            let mut smaller = pool.get_chunk(&requested);
            for (index, field) in requested.iter().enumerate() {
                let stack = expected.get_mut(&get_fixed_len(field)).unwrap();
                assert_eq!(smaller.column(index).get_data_cap(), stack.pop().unwrap());
            }
            for (index, field) in requested.iter().enumerate() {
                expected
                    .get_mut(&get_fixed_len(field))
                    .unwrap()
                    .push(smaller.column(index).get_data_cap());
            }
            pool.put_chunk(&requested, &mut smaller);

            let full = pool.get_chunk(&fields);
            for (index, field) in fields.iter().enumerate() {
                let width = get_fixed_len(field);
                let stack = expected.get_mut(&width).unwrap();
                assert_eq!(full.column(index).type_size(), width);
                assert_eq!(full.column(index).get_data_cap(), stack.pop().unwrap());
            }
        }
    }

    #[test]
    fn partial_takes_reuse_an_empty_slot_allocation() {
        let fixed = FieldType::new(FieldTypeCode::LongLong);
        let variable = FieldType::new(FieldTypeCode::Varchar);
        let mixed = vec![fixed.clone(), variable.clone(), fixed.clone(), variable.clone()];
        for (fields, subset) in [
            (vec![fixed.clone(); 2], vec![fixed.clone()]),
            (vec![fixed.clone(), variable.clone()], vec![fixed.clone()]),
            (mixed.clone(), vec![variable, fixed.clone()]),
            (mixed, vec![fixed.clone(); 2]),
        ] {
            let pool = Pool::new(1);
            let mut full = pool.get_chunk(&fields);
            pool.put_chunk(&fields, &mut full);

            let mut partial = pool.get_chunk(&subset);
            let address = partial.columns.as_ptr();
            pool.put_chunk(&subset, &mut partial);
            {
                let home = pool.columns().local().lock();
                assert!(home.spare_slots.is_empty());
                assert_eq!(home.spare_slots.as_ptr(), address);
            }

            let mut full = pool.get_chunk(&fields);
            pool.put_chunk(&fields, &mut full);
            let next = pool.get_chunk(&subset);
            assert_eq!(next.columns.as_ptr(), address);
        }
    }

    #[test]
    fn partial_returns_do_not_retain_large_spare_allocations() {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong); 2 * (MAX_SPARE_SLOTS + 1)];
        let subset = &fields[..MAX_SPARE_SLOTS + 1];
        let pool = Pool::new(1);
        let mut full = pool.get_chunk(&fields);
        pool.put_chunk(&fields, &mut full);
        let mut partial = pool.get_chunk(subset);
        pool.put_chunk(subset, &mut partial);
        assert_eq!(pool.columns().local().lock().spare_slots.capacity(), 0);
    }

    #[test]
    fn two_remaining_slots_of_one_width_still_pop_in_order() {
        let fixed = FieldType::new(FieldTypeCode::LongLong);
        let variable = FieldType::new(FieldTypeCode::Varchar);
        let fields = [fixed.clone(), variable.clone(), fixed.clone()];
        let pool = Pool::new(1);
        let mut source = Chunk::from_reusable_columns(
            [
                Column::new_column_with_type_size(8, 2),
                Column::new_column_with_type_size(VAR_ELEM_LEN, 1),
                Column::new_column_with_type_size(8, 3),
            ],
            1,
            1,
        );
        pool.put_chunk(&fields, &mut source);
        let variable_chunk = pool.get_chunk(&[variable]);
        let fixed_chunk = pool.get_chunk(&[fixed.clone(), fixed]);
        assert_eq!(variable_chunk.column(0).type_size(), VAR_ELEM_LEN);
        assert_eq!(fixed_chunk.column(0).get_data_cap(), 24);
        assert_eq!(fixed_chunk.column(1).get_data_cap(), 16);
    }

    #[test]
    fn returning_a_partial_chunk_keeps_its_live_alias() {
        let fixed = FieldType::new(FieldTypeCode::LongLong);
        let other = FieldType::new(FieldTypeCode::Varchar);
        let fields = [fixed.clone(), other.clone(), fixed.clone(), other];
        let pool = Pool::new(1);
        let mut source = pool.get_chunk(&fields);
        pool.put_chunk(&fields, &mut source);

        let one = &fields[..1];
        let mut partial = pool.get_chunk(one);
        partial.append_int64(0, 17);
        let mut alias = Chunk::new_chunk_with_capacity(one, 1);
        alias.make_ref_to(0, &mut partial, 0).unwrap();
        pool.put_chunk(one, &mut partial);

        assert_eq!(pool.cached(8), 1);
        assert_eq!(pool.cached(VAR_ELEM_LEN), 2);
        assert_eq!(alias.get_row(0).get_int64(0), 17);
    }

    #[test]
    fn global_capacity_buckets_do_not_cross_reuse() {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let mut small = new_chunk_from_pool_with_capacity(&fields, 3);
        let mut large = new_chunk_from_pool_with_capacity(&fields, 11);
        assert_eq!(small.column(0).get_data_cap(), 24);
        assert_eq!(large.column(0).get_data_cap(), 88);
        small.destroy(3, &fields);
        large.destroy(11, &fields);
        assert_eq!(small.num_cols(), 0);
        assert_eq!(large.num_cols(), 0);

        let small_again = new_chunk_from_pool_with_capacity(&fields, 3);
        let large_again = new_chunk_from_pool_with_capacity(&fields, 11);
        assert_eq!(small_again.column(0).get_data_cap(), 24);
        assert_eq!(large_again.column(0).get_data_cap(), 88);
    }

    /// Go's benchmark is parallel; this correctness anchor exercises the same
    /// synchronized get/put surface from multiple Rust threads.
    #[test]
    fn pool_is_synchronized() {
        let pool = Arc::new(Pool::new(16));
        let fields = Arc::new(fields());
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let pool = Arc::clone(&pool);
                let fields = Arc::clone(&fields);
                std::thread::spawn(move || {
                    for _ in 0..128 {
                        let mut chunk = pool.get_chunk(&fields);
                        chunk.append_int64(5, 1);
                        pool.put_chunk(&fields, &mut chunk);
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("pool worker");
        }
    }
    /// Go `TestPoolGetChunk` (`pkg/util/chunk/pool_test.go`): pooled chunks
    /// carry the exact element widths and data capacities of a fresh
    /// `NewChunkWithCapacity`.
    #[test]
    fn go_test_pool_get_chunk() {
        let init_cap = 1024usize;
        let pool = Pool::new(init_cap);
        let field_types = fields();

        let chk = pool.get_chunk(&field_types);
        assert_eq!(chk.num_cols(), field_types.len());
        assert!(chk.column(0).elem_buf.is_none());
        assert!(chk.column(1).elem_buf.is_none());
        for index in 2..6 {
            assert_eq!(
                chk.column(index).elem_buffer_len(),
                get_fixed_len(&field_types[index]) as usize
            );
        }
        for index in 2..6 {
            assert_eq!(
                chk.column(index).get_data_cap(),
                init_cap * get_fixed_len(&field_types[index]) as usize
            );
        }
    }

    /// Go `TestNewPool`: every physical width uses the pool's initial capacity.
    #[test]
    fn go_test_new_pool() {
        let pool = Pool::new(1024);
        assert_eq!(pool.init_capacity(), 1024);
        let fields = [
            FieldTypeCode::Float,
            FieldTypeCode::LongLong,
            FieldTypeCode::Datetime,
            FieldTypeCode::NewDecimal,
            FieldTypeCode::Varchar,
        ]
        .map(FieldType::new);
        let chunk = pool.get_chunk(&fields);
        for (index, width) in [4, 8, 8, 40].into_iter().enumerate() {
            assert_eq!(chunk.column(index).get_data_cap(), 1024 * width);
        }
        assert!(chunk.column(4).get_data_cap() >= 8 * 1024);

        // Field types use 8-byte time columns. The pool still accepts a
        // column whose physical width is 16 bytes when it is returned.
        let mut wide = Chunk::from_reusable_columns(
            [Column::new_column_with_type_size(16, 1024)],
            1024,
            1024,
        );
        pool.put_chunk(&fields[..1], &mut wide);
        assert_eq!(pool.cached(16), 1);
    }
}
