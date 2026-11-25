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
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;
use std::sync::{OnceLock, PoisonError, RwLock};
use tidb_datatype::FieldType;
use tidb_zeropool::Pool as ReusePool;

#[derive(Default)]
struct ColumnBucket {
    columns: OnceLock<ReusePool<Option<Column>>>,
    #[cfg(test)]
    len: std::sync::atomic::AtomicUsize,
}

impl ColumnBucket {
    fn columns(&self) -> &ReusePool<Option<Column>> {
        self.columns.get_or_init(ReusePool::default)
    }

    fn get_or_create(&self, type_size: i64, capacity: usize) -> Column {
        if let Some(column) = self.columns().get() {
            #[cfg(test)]
            self.len.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            column
        } else {
            Column::new_column_with_type_size(type_size, capacity)
        }
    }

    fn put(&self, mut column: Column) {
        column.reset();
        #[cfg(test)]
        self.len.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.columns().put(Some(column));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.len.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl fmt::Debug for ColumnBucket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ColumnBucket").finish_non_exhaustive()
    }
}

/// Go `chunk.Pool`: a synchronized column pool for one initial-capacity
/// bucket.
///
/// The source has five independent `sync.Pool`s: variable-length columns and
/// fixed widths 4, 8, 16, and 40. Keeping those buckets distinct prevents a
/// reused column from silently changing its physical cell layout.
#[derive(Debug)]
pub struct Pool {
    init_capacity: usize,
    variable: ColumnBucket,
    fixed4: ColumnBucket,
    fixed8: ColumnBucket,
    fixed16: ColumnBucket,
    fixed40: ColumnBucket,
}

impl Pool {
    /// Go `NewPool`.
    #[must_use]
    pub fn new(init_capacity: usize) -> Self {
        Self {
            init_capacity,
            variable: ColumnBucket::default(),
            fixed4: ColumnBucket::default(),
            fixed8: ColumnBucket::default(),
            fixed16: ColumnBucket::default(),
            fixed40: ColumnBucket::default(),
        }
    }

    /// The initial capacity identifying this global pool bucket.
    #[must_use]
    pub fn init_capacity(&self) -> usize {
        self.init_capacity
    }

    fn bucket(&self, type_size: i64) -> &ColumnBucket {
        match type_size {
            VAR_ELEM_LEN => &self.variable,
            4 => &self.fixed4,
            8 => &self.fixed8,
            16 => &self.fixed16,
            40 => &self.fixed40,
            other => panic!("unsupported chunk pool physical width {other}"),
        }
    }

    /// Go `Pool.GetChunk`.
    #[must_use]
    pub fn get_chunk(&self, fields: &[FieldType]) -> Chunk {
        let columns = fields.iter().map(|field| {
            let type_size = get_fixed_len(field);
            self.bucket(type_size)
                .get_or_create(type_size, self.init_capacity)
        });
        Chunk::from_reusable_columns(columns, self.init_capacity, self.init_capacity)
    }

    /// Go `Pool.PutChunk`.
    ///
    /// The columns are removed from `chunk`, leaving it with Go's nil-column
    /// post-destroy state, and each column is reset before entering its
    /// physical-width bucket.
    pub fn put_chunk(&self, fields: &[FieldType], chunk: &mut Chunk) {
        assert_eq!(
            fields.len(),
            chunk.num_cols(),
            "Pool.PutChunk field/column width mismatch"
        );
        chunk.reset_for_reuse(|column| {
            let type_size = column.type_size();
            self.bucket(type_size).put(column);
        });
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
        assert_eq!(pool.variable.len(), 2);
        assert_eq!(pool.fixed4.len(), 1);
        assert_eq!(pool.fixed8.len(), 4);
        assert_eq!(pool.fixed40.len(), 1);

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
        assert_eq!(pool.fixed8.len(), 0);
        assert_eq!(pool.fixed40.len(), 1);

        let reused = pool.get_chunk(&fields);
        assert_eq!(reused.column(0).type_size(), 8);
        assert_eq!(reused.column(1).type_size(), 40);
        assert!(!reused.columns_share_identity(0, &reused, 1));
        assert_eq!(pool.fixed8.len(), 0);
        assert_eq!(pool.fixed40.len(), 0);
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
        assert_eq!(pool.fixed8.len(), 1);

        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let mut reused = pool.get_chunk(&fields);
                    assert_eq!(pool.fixed8.len(), 0);
                    for column in 0..fields.len() {
                        reused.append_int64(column, 29);
                    }
                    pool.put_chunk(&fields, &mut reused);
                })
                .join()
                .unwrap();
        });
        assert_eq!(alias.get_row(0).get_int64(0), 17);
        assert_eq!(pool.fixed8.len(), 3);
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

    /// Go `TestNewPool` (`pool_test.go:25`): the pool carries its initial
    /// capacity and all five physical column buckets. Rust constructs every
    /// bucket eagerly (Go's `sync.Pool` values are non-nil at construction),
    /// so the structural assertion is the capacity plus bucket reachability.
    #[test]
    fn go_test_new_pool() {
        let pool = Pool::new(1024);
        assert_eq!(pool.init_capacity(), 1024);
        // Every fixed-width bucket answers `get_column` without panic; a
        // missing bucket would take the `unsupported width` branch.
        // Fixed-width buckets size their packed buffer at
        // `initCap * width`; the variable bucket sizes its OFFSETS buffer
        // (`8 * initCap`), so only the fixed widths have an exact answer.
        for type_size in [4i64, 8, 16, 40] {
            let column = pool.bucket(type_size).get_or_create(type_size, 1024);
            assert_eq!(column.get_data_cap(), 1024 * type_size as usize);
        }
        let variable = pool.bucket(VAR_ELEM_LEN).get_or_create(VAR_ELEM_LEN, 1024);
        assert!(variable.get_data_cap() >= 8 * 1024);
    }
}
