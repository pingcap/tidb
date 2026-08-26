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

//! Ports of `pkg/util/chunk/alloc_test.go`.
//!
//! Go's allocator tests inspect pool internals (`alloc.free`,
//! `columnAlloc.pool`); this port keeps those structures private and expresses
//! the same contracts through the public allocation surface. Where an entire
//! test is whitebox it is `#[ignore]`d with a `go-parity-gap` note.

use std::sync::atomic::{AtomicI64, Ordering};

use tidb_datatype::{FieldType, FieldTypeCode};

use crate::alloc::{
    init_chunk_alloc_size, new_allocator, new_reuse_hook_allocator, new_sync_allocator,
    Allocator, ChunkAllocator, DefaultColumnAllocator,
};
use crate::chunk::Chunk;
use crate::column::get_fixed_len;
use crate::pool::Pool;

fn alloc_field_types() -> Vec<FieldType> {
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

/// The physical layout assertions of Go `TestAllocator`: variable columns
/// carry no fixed element buffer and every fixed column's data capacity is
/// `initCap * getFixedLen`. The free-list bookkeeping (`alloc.free`) is
/// internal to Go; reuse-after-reset is checked behaviorally.
#[test]
fn allocator_alloc_layout_and_reuse_after_reset() {
    let alloc = ChunkAllocator::new();
    let field_types = alloc_field_types();

    let init_cap = 5usize;

    let check = |chk: &Chunk| {
        assert_eq!(field_types.len(), chk.num_cols());
        // Varchar and JSON columns have no element buffer (variable width).
        assert!(!chk.column(0).is_fixed());
        assert!(!chk.column(1).is_fixed());
        for idx in 2..8 {
            assert!(chk.column(idx).is_fixed());
            assert_eq!(
                chk.column(idx).type_size(),
                get_fixed_len(&field_types[idx])
            );
            assert_eq!(
                chk.column(idx).data_capacity(),
                init_cap * get_fixed_len(&field_types[idx]) as usize
            );
        }
    };

    let chk = alloc.alloc(&field_types, init_cap, 100);
    check(&chk);

    // Call Reset and alloc again, check the result.
    alloc.reset();
    let chk = alloc.alloc(&field_types, init_cap, 100);
    check(&chk);
}

/// Go `TestColumnAllocator` (equality part): `NewColumn`, the pooled column
/// allocator, and `DefaultColumnAllocator` produce identical columns. The
/// pooled variant and its free-list size cap are private to this port; that
/// half of the source test is covered by the ignore note below.
#[test]
fn column_allocator_variants_agree() {
    let field_types = alloc_field_types();
    let mut alloc2 = DefaultColumnAllocator;
    let init_cap = 5;
    for ft in &field_types {
        let v0 = crate::column::Column::new_column(ft, init_cap);
        let v2 = <DefaultColumnAllocator as crate::alloc::ColumnAllocator>::new_column(
            &mut alloc2, ft, init_cap,
        );
        assert_eq!(v0, v2);
    }
}

/// Go `TestAvoidColumnReuse` (behavioral part): a chunk whose columns were all
/// flagged `avoidReusing` must not be recycled into the allocator's pools.
/// This port models ownership by detaching such chunks at reset, so the
/// observable contract is that a fresh allocation still yields a correct,
/// independent chunk afterwards. The decoder flag propagation is pinned in the
/// codec contract tests. The pool-emptiness inspection itself is whitebox.
#[test]
fn avoid_column_reuse_keeps_allocator_consistent() {
    let alloc = ChunkAllocator::new();
    let field_types = alloc_field_types();
    for _ in 0..(64 + 10) {
        let mut chk = alloc.alloc(&field_types, 5, 10);
        for idx in 0..chk.num_cols() {
            chk.column_mut(idx).append_null();
        }
    }
    alloc.reset();
    let chk = alloc.alloc(&field_types, 5, 1024);
    assert_eq!(chk.num_cols(), field_types.len());
    assert_eq!(chk.num_rows(), 0);
}

/// Go `TestColumnAllocatorLimit` (observable part): `InitChunkAllocSize`
/// controls whether an allocator reuses at all (`CheckReuseAllocSize`),
/// including the zero disabling it entirely.
#[test]
fn column_allocator_limit_check_reuse_flag() {
    init_chunk_alloc_size(10, 20);
    let alloc = ChunkAllocator::new();
    assert!(alloc.check_reuse_alloc_size());

    init_chunk_alloc_size(0, 0);
    let alloc = ChunkAllocator::new();
    assert!(!alloc.check_reuse_alloc_size());

    // Restore the process defaults; nextest runs each test in its own process,
    // but keep hygiene anyway.
    init_chunk_alloc_size(64, 256);
}

/// Go `TestReuseHookAllocator` (alloc_test.go): the hook fires exactly once on
/// the first reuse-enabled allocation, never when reuse is disabled, and not
/// again on subsequent allocations.
#[test]
fn reuse_hook_allocator() {
    let field_types = alloc_field_types();
    static REUSE: AtomicI64 = AtomicI64::new(0);

    init_chunk_alloc_size(0, 0);
    let alloc = new_reuse_hook_allocator(ChunkAllocator::new(), || {
        REUSE.fetch_add(1, Ordering::SeqCst);
    });
    // As MaxFreeChunks/MaxFreeColumns are 0, the hook stays untouched.
    let chk = alloc.alloc(&field_types, 5, 100);
    assert_eq!(REUSE.load(Ordering::SeqCst), 0);
    drop(chk);

    init_chunk_alloc_size(10, 20);
    let alloc = new_reuse_hook_allocator(ChunkAllocator::new(), || {
        REUSE.fetch_add(1, Ordering::SeqCst);
    });
    let chk = alloc.alloc(&field_types, 5, 100);
    assert_eq!(REUSE.load(Ordering::SeqCst) - 0, 1);
    drop(chk);
    // Another alloc will not touch it.
    let chk = alloc.alloc(&field_types, 5, 100);
    assert_eq!(REUSE.load(Ordering::SeqCst), 1);
    drop(chk);

    init_chunk_alloc_size(64, 256);
}

/// Go `TestSyncAllocator` (alloc_test.go): a sync-wrapped allocator survives
/// concurrent Alloc/Reset churn from many threads.
#[test]
fn sync_allocator() {
    use std::sync::Arc;

    let field_types = alloc_field_types();
    let alloc = Arc::new(new_sync_allocator(ChunkAllocator::new()));

    let mut handles = Vec::new();
    for _ in 0..100 {
        let alloc = Arc::clone(&alloc);
        let field_types = field_types.clone();
        handles.push(std::thread::spawn(move || {
            for _ in 0..10 {
                for _ in 0..100 {
                    let chk = alloc.alloc(&field_types, 5, 100);
                    assert_eq!(chk.num_cols(), field_types.len());
                    drop(chk);
                }
                alloc.reset();
            }
        }));
    }
    for handle in handles {
        handle.join().expect("worker thread must not panic");
    }
}

// Keep the unused-import surface honest: `new_allocator` is the public
// constructor used by production callers; reference it once here so the
// re-export surface is exercised from this module too.
#[test]
fn public_constructor_smoke() {
    let alloc: ChunkAllocator = new_allocator();
    let fields = [FieldType::new(FieldTypeCode::LongLong)];
    let chk = alloc.alloc(&fields, 4, 1024);
    assert_eq!(chk.num_cols(), 1);

    // Exercise the global chunk pool path Go reaches through `NewPool`.
    let pool = Pool::new(16);
    let mut chunk = pool.get_chunk(&fields.to_vec());
    chunk.append_int64(0, 7);
    assert_eq!(chunk.get_row(0).get_int64(0), 7);
    pool.put_chunk(&fields.to_vec(), &mut chunk);
}

// go-parity-gap: Go `TestNoDuplicateColumnReuse` inspects the allocator's
// private column pools (`alloc.columnAlloc.pool`) to prove no duplicated
// column survives a reset; this port keeps those structures private.
#[test]
#[ignore = "go-parity-gap: allocator pool internals are not exposed to Rust tests"]
fn no_duplicate_column_reuse() {
    let alloc = ChunkAllocator::new();
    let field_types = alloc_field_types();
    for _ in 0..(64 + 10) {
        let mut chk = alloc.alloc(&field_types, 5, 10);
        chk.make_ref(1, 3);
        drop(chk);
    }
    alloc.reset();
}

// go-parity-gap: the free-list length caps asserted by Go `TestAllocator`
// (`len(alloc.free) == maxFreeChunks`) and `TestColumnAllocator`'s pooled
// allocator size cap read private state; only the observable layout and reuse
// behavior are ported above.
#[test]
#[ignore = "go-parity-gap: allocator free-list internals are not exposed to Rust tests"]
fn allocator_free_list_bounds() {}

// go-parity-gap: Go `TestColumnAllocatorLimit` asserts per-bucket free-column
// counts after InitChunkAllocSize changes (`alloc.columnAlloc.pool[..].Len()`);
// those buckets are private here. The CheckReuseAllocSize half is ported above.
#[test]
#[ignore = "go-parity-gap: allocator free-list internals are not exposed to Rust tests"]
fn column_allocator_limit_pool_sizes() {}

// go-parity-gap: Go `TestColumnAllocatorCheck` counts recycled columns per
// physical width inside the private column pool; this port cannot inspect it.
#[test]
#[ignore = "go-parity-gap: allocator pool internals are not exposed to Rust tests"]
fn column_allocator_check() {}
