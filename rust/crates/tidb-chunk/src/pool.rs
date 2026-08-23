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
//! chunk. Rust transfers ownership of a [`Column`] into the matching mutex
//! protected bucket and transfers it back on `get_chunk`; no unsafe lifetime
//! extension or duplicate owner is needed.

use crate::chunk::Chunk;
use crate::column::{get_fixed_len, Column, VAR_ELEM_LEN};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, RwLock};
use tidb_datatype::FieldType;

#[derive(Debug, Default)]
struct ColumnBucket {
    columns: Mutex<Vec<Column>>,
}

impl ColumnBucket {
    fn lock(&self) -> MutexGuard<'_, Vec<Column>> {
        self.columns.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn get_or_create(&self, type_size: i64, capacity: usize) -> Column {
        self.lock()
            .pop()
            .unwrap_or_else(|| Column::new_column_with_type_size(type_size, capacity))
    }

    fn put(&self, mut column: Column) {
        column.reset();
        self.lock().push(column);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().len()
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
        let columns = fields
            .iter()
            .map(|field| {
                let type_size = get_fixed_len(field);
                self.bucket(type_size)
                    .get_or_create(type_size, self.init_capacity)
            })
            .collect();
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
        let columns = chunk.take_columns_for_reuse();
        for column in columns {
            let type_size = column.type_size();
            self.bucket(type_size).put(column);
        }
    }
}

type GlobalPools = RwLock<HashMap<usize, Arc<Pool>>>;

static GLOBAL_CHUNK_POOL: OnceLock<GlobalPools> = OnceLock::new();

fn global_pools() -> &'static GlobalPools {
    GLOBAL_CHUNK_POOL.get_or_init(|| RwLock::new(HashMap::new()))
}

fn pool_for(init_capacity: usize) -> Arc<Pool> {
    if let Some(pool) = global_pools()
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&init_capacity)
        .cloned()
    {
        return pool;
    }

    let mut pools = global_pools()
        .write()
        .unwrap_or_else(PoisonError::into_inner);
    Arc::clone(
        pools
            .entry(init_capacity)
            .or_insert_with(|| Arc::new(Pool::new(init_capacity))),
    )
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
        assert_eq!(chunk.column(2).data_capacity(), capacity * 4);
        assert_eq!(chunk.column(3).data_capacity(), capacity * 40);
        assert_eq!(chunk.column(4).data_capacity(), capacity * 8);
    }

    /// Go `TestPoolPutChunk`: putting removes the chunk's column references;
    /// the next get receives the reset owned columns.
    #[test]
    fn put_clears_chunk_and_reuses_reset_columns() {
        let fields = fields();
        let pool = Pool::new(8);
        let mut chunk = pool.get_chunk(&fields);
        chunk.append_string(0, "keep capacity");
        let data_capacity = chunk.column(0).data_capacity();
        pool.put_chunk(&fields, &mut chunk);
        assert_eq!(chunk.num_cols(), 0);
        assert_eq!(pool.variable.len(), 2);
        assert_eq!(pool.fixed4.len(), 1);
        assert_eq!(pool.fixed8.len(), 4);
        assert_eq!(pool.fixed40.len(), 1);

        let reused = pool.get_chunk(&fields);
        assert_eq!(reused.num_rows(), 0);
        assert_eq!(reused.column(0).data_capacity(), data_capacity);
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
    fn global_capacity_buckets_do_not_cross_reuse() {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let mut small = new_chunk_from_pool_with_capacity(&fields, 3);
        let mut large = new_chunk_from_pool_with_capacity(&fields, 11);
        assert_eq!(small.column(0).data_capacity(), 24);
        assert_eq!(large.column(0).data_capacity(), 88);
        small.destroy(3, &fields);
        large.destroy(11, &fields);
        assert_eq!(small.num_cols(), 0);
        assert_eq!(large.num_cols(), 0);

        let small_again = new_chunk_from_pool_with_capacity(&fields, 3);
        let large_again = new_chunk_from_pool_with_capacity(&fields, 11);
        assert_eq!(small_again.column(0).data_capacity(), 24);
        assert_eq!(large_again.column(0).data_capacity(), 88);
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
                chk.column(index).data_capacity(),
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
            assert_eq!(column.data_capacity(), 1024 * type_size as usize);
        }
        let variable = pool.bucket(VAR_ELEM_LEN).get_or_create(VAR_ELEM_LEN, 1024);
        assert!(variable.data_capacity() >= 8 * 1024);
    }
}
