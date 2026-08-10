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

//! Public contract for `pkg/util/chunk/alloc.go` and `pool.go`.

mod pkg_util_chunk_fixture_observation;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use tidb_chunk::alloc::{
    init_chunk_alloc_size, new_allocator, new_empty_allocator, new_reuse_hook_allocator,
    new_sync_allocator, Allocator, ColumnAllocator, DefaultColumnAllocator, MAX_CACHED_LEN,
};
use tidb_chunk::column::{get_fixed_len, Column, VAR_ELEM_LEN};
use tidb_chunk::pool::{new_chunk_from_pool_with_capacity, Pool};
use tidb_datatype::{FieldType, FieldTypeCode};

struct AllocatorConfigReset;

static ALLOCATION_CONTRACT_LOCK: Mutex<()> = Mutex::new(());

impl Drop for AllocatorConfigReset {
    fn drop(&mut self) {
        init_chunk_alloc_size(64, 256);
        MAX_CACHED_LEN.store(16 * 1024, Ordering::Relaxed);
    }
}

fn all_pool_widths() -> Vec<FieldType> {
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

#[test]
fn allocation_pool_public_contract() {
    let _guard = ALLOCATION_CONTRACT_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let _reset = AllocatorConfigReset;
    let fields = all_pool_widths();

    init_chunk_alloc_size(4096, 4096);
    let allocator = tidb_chunk::alloc::new_allocator();
    assert!(allocator.check_reuse_alloc_size());
    let mut first = allocator.alloc(&fields, 5, 100);
    assert_eq!(first.capacity(), 5);
    assert_eq!(first.required_rows(), 100);
    assert_eq!(first.num_cols(), fields.len());
    assert_eq!(first.column(0).type_size(), VAR_ELEM_LEN);
    assert_eq!(first.column(2).type_size(), 4);
    assert_eq!(first.column(3).type_size(), 40);
    assert_eq!(first.column(4).type_size(), 8);
    let empty_shape = allocator.alloc(&[], 5, 100);
    assert_eq!(empty_shape.num_cols(), 0);
    drop(empty_shape);
    first.append_string(0, "retained allocation");
    let retained_capacity = first.column(0).data_capacity();
    drop(first);
    allocator.reset();
    let reused = allocator.alloc(&fields, 5, 100);
    assert_eq!(reused.num_rows(), 0);
    assert!(reused.column(0).data_capacity() >= retained_capacity);
    drop(reused);

    init_chunk_alloc_size(0, 0);
    let disabled = new_allocator();
    assert!(!disabled.check_reuse_alloc_size());
    let empty = new_empty_allocator();
    assert!(!empty.check_reuse_alloc_size());
    let fresh = empty.alloc(&fields, 9, 3);
    assert_eq!(fresh.capacity(), 3);
    assert_eq!(fresh.required_rows(), 3);
    empty.reset();

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let disabled_hook_calls = Arc::clone(&hook_calls);
    let disabled_hook = new_reuse_hook_allocator(new_allocator(), move || {
        disabled_hook_calls.fetch_add(1, Ordering::SeqCst);
    });
    drop(disabled_hook.alloc(&fields, 1, 1));
    assert_eq!(hook_calls.load(Ordering::SeqCst), 0);

    init_chunk_alloc_size(8, 8);
    let enabled_hook_calls = Arc::clone(&hook_calls);
    let enabled_hook = new_reuse_hook_allocator(new_allocator(), move || {
        enabled_hook_calls.fetch_add(1, Ordering::SeqCst);
    });
    drop(enabled_hook.alloc(&fields, 1, 1));
    drop(enabled_hook.alloc(&fields, 1, 1));
    assert_eq!(hook_calls.load(Ordering::SeqCst), 1);

    let synchronized = Arc::new(new_sync_allocator(new_allocator()));
    let shared_fields = Arc::new(fields.clone());
    let workers = (0..8)
        .map(|_| {
            let synchronized = Arc::clone(&synchronized);
            let fields = Arc::clone(&shared_fields);
            std::thread::spawn(move || {
                for _ in 0..32 {
                    let mut chunk = synchronized.alloc(&fields, 2, 8);
                    chunk.append_int64(5, 7);
                    assert_eq!(chunk.get_row(0).get_int64(5), 7);
                    drop(chunk);
                    synchronized.reset();
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().expect("synchronized allocator worker");
    }

    let mut default_columns = DefaultColumnAllocator;
    for field in &fields {
        let allocated = default_columns.new_column(field, 3);
        let direct = Column::new_column(field, 3);
        assert_eq!(allocated, direct);
        assert_eq!(allocated.type_size(), get_fixed_len(field));
    }

    let pool = Pool::new(16);
    assert_eq!(pool.init_capacity(), 16);
    let mut empty_pooled = pool.get_chunk(&[]);
    assert_eq!(empty_pooled.num_cols(), 0);
    pool.put_chunk(&[], &mut empty_pooled);
    let mut pooled = pool.get_chunk(&fields);
    assert_eq!(pooled.capacity(), 16);
    assert_eq!(pooled.required_rows(), 16);
    assert_eq!(
        (0..pooled.num_cols())
            .map(|index| pooled.column(index).type_size())
            .collect::<Vec<_>>(),
        [VAR_ELEM_LEN, VAR_ELEM_LEN, 4, 40, 8, 8, 8, 8]
    );
    pooled.append_string(0, "reused pool bytes");
    let pooled_data_capacity = pooled.column(0).data_capacity();
    pool.put_chunk(&fields, &mut pooled);
    assert_eq!(pooled.num_cols(), 0);
    let pooled_again = pool.get_chunk(&fields);
    assert_eq!(pooled_again.num_rows(), 0);
    assert!(pooled_again.column(0).data_capacity() >= pooled_data_capacity);

    let alias_fields = vec![
        FieldType::new(FieldTypeCode::LongLong),
        FieldType::new(FieldTypeCode::NewDecimal),
    ];
    let alias_pool = Pool::new(8);
    let mut aliased = alias_pool.get_chunk(&alias_fields);
    aliased.make_ref(1, 0);
    assert!(aliased.columns_share_identity(0, &aliased, 1));
    alias_pool.put_chunk(&alias_fields, &mut aliased);
    let restored = alias_pool.get_chunk(&alias_fields);
    assert_eq!(restored.column(0).type_size(), 8);
    assert_eq!(restored.column(1).type_size(), 40);
    assert!(!restored.columns_share_identity(0, &restored, 1));

    let one_longlong = vec![FieldType::new(FieldTypeCode::LongLong)];
    let mut small = new_chunk_from_pool_with_capacity(&one_longlong, 3);
    let mut large = new_chunk_from_pool_with_capacity(&one_longlong, 11);
    assert_eq!(small.column(0).data_capacity(), 24);
    assert_eq!(large.column(0).data_capacity(), 88);
    small.destroy(3, &one_longlong);
    large.destroy(11, &one_longlong);
    assert_eq!(small.num_cols(), 0);
    assert_eq!(large.num_cols(), 0);
    assert_eq!(
        new_chunk_from_pool_with_capacity(&one_longlong, 3)
            .column(0)
            .data_capacity(),
        24
    );
    assert_eq!(
        new_chunk_from_pool_with_capacity(&one_longlong, 11)
            .column(0)
            .data_capacity(),
        88
    );

    let mut put_before_get_chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&one_longlong, 29);
    put_before_get_chunk.append_int64(0, 19);
    put_before_get_chunk.destroy(29, &one_longlong);
    let created_by_put = new_chunk_from_pool_with_capacity(&one_longlong, 29);
    assert_eq!(created_by_put.capacity(), 29);
    assert_eq!(created_by_put.num_rows(), 0);

    pkg_util_chunk_fixture_observation::emit(
        "CHUNK-ALLOCATION-POOL-RUNTIME",
        "Rust preserves TiDB's bounded allocator and physical-width pool behavior through ownership-safe leases; Go pointer invalidation, sync.Pool GC eviction, and benchmark timing are intentionally not reproduced.",
        &[
            (
                "allocator-and-wrapper-semantics",
                "configuration;allocate;drop;reset;hook;sync;empty",
                "capacity, required rows, reuse admission, hook-once, and synchronized calls match",
            ),
            (
                "pool-width-and-alias-semantics",
                "reachable var;4;8;40 widths;capacity buckets;aliased owners",
                "reachable physical widths stay isolated, returned columns reset, and duplicate aliases publish one owner",
            ),
            (
                "runtime-mechanisms-excluded",
                "Go raw pointers; sync.Pool GC; benchmark iteration and timing",
                "Rust ownership and synchronization preserve observable behavior without reproducing runtime machinery",
            ),
        ],
    );
}

#[test]
fn allocator_configuration_compile_anchor() {
    let _guard = ALLOCATION_CONTRACT_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let _reset = AllocatorConfigReset;
    tidb_chunk::alloc::init_chunk_alloc_size(1, 1);
    assert!(new_allocator().check_reuse_alloc_size());
}

#[test]
fn allocator_wrappers_compile_anchor() {
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_calls = Arc::clone(&calls);
    let wrapped = tidb_chunk::alloc::new_reuse_hook_allocator(new_empty_allocator(), move || {
        hook_calls.fetch_add(1, Ordering::SeqCst);
    });
    drop(wrapped.alloc(&[], 1, 1));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn pool_compile_anchor() {
    let fields = vec![FieldType::new(FieldTypeCode::Varchar)];
    let pool = tidb_chunk::pool::Pool::new(2);
    let chunk = pool.get_chunk(&fields);
    assert_eq!(chunk.capacity(), 2);
    assert_eq!(chunk.column(0).type_size(), VAR_ELEM_LEN);
}

#[test]
fn global_pool_compile_anchor() {
    let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
    let chunk = tidb_chunk::pool::new_chunk_from_pool_with_capacity(&fields, 37);
    assert_eq!(chunk.capacity(), 37);
}
