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

//! Public package contract for Go `pkg/util/globalconn`.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tidb_util::globalconn::{
    parse_conn_id, Allocator, AutoIncPool, GlobalAllocator, IdPool, LockFreeCircularPool,
    SimpleAllocator, ID_POOL_INVALID_VALUE, MAX_LOCAL_CONN_ID32, MAX_SERVER_ID32, RESERVED_COUNT,
};

#[test]
fn zero_sized_ring_preserves_source_wrapping_state() {
    let mut pool = LockFreeCircularPool::default();
    pool.init_ext(0, 0);

    assert_eq!(pool.cap(), i64::from(u32::MAX));
    assert_eq!(pool.len(), 0);
    assert_eq!(pool.get(), (ID_POOL_INVALID_VALUE, false));

    assert!(catch_unwind(AssertUnwindSafe(|| pool.put(1))).is_err());
    assert_eq!(pool.len(), 1);
}

#[test]
fn public_pool_boundaries_match_go() {
    let mut auto = AutoIncPool::default();
    assert_eq!(auto.cap(), 0);
    assert_eq!(auto.len(), -1);
    assert_eq!(auto.get(), (0, false));
    assert_eq!(auto.to_string(), "lastID: 0");

    auto.init_ext(4, true, 1);
    assert_eq!(auto.get(), (1, true));
    auto.init_ext(4, false, 1);
    assert_eq!(auto.len(), 1);
    assert!(auto.put(1));

    let mut zero_auto = AutoIncPool::default();
    zero_auto.init(0);
    assert!(catch_unwind(AssertUnwindSafe(|| zero_auto.get())).is_err());

    let mut values = LockFreeCircularPool::default();
    values.init_ext(2, 0);
    assert!(values.put((1_u64 << 32) + 7));
    assert_eq!(values.get(), (7, true));

    let mut size = LockFreeCircularPool::default();
    size.init((1_u64 << 32) + 1);
    assert_eq!(size.cap(), 0);
    assert!(!size.put(1));

    assert!(parse_conn_id(1).unwrap().1);
    assert_eq!(
        parse_conn_id(1_u64 << 32).unwrap_err().to_string(),
        "unexpected connectionID exceeds uint32"
    );
    assert_eq!(
        parse_conn_id(1_u64 << 63).unwrap_err().to_string(),
        "unexpected connectionID exceeds int64"
    );

    let simple = SimpleAllocator::new();
    assert!(catch_unwind(AssertUnwindSafe(|| {
        simple.get_reserved_conn_id(RESERVED_COUNT)
    }))
    .is_err());
}

#[test]
fn server_width_tracks_the_current_source_id() {
    let server_id = Arc::new(AtomicU64::new(MAX_SERVER_ID32 + 1));
    let current = Arc::clone(&server_id);
    let allocator = GlobalAllocator::new(move || current.load(Ordering::SeqCst), true);

    let wide = allocator.allocate();
    assert!(wide.is_64bits);
    assert_eq!(wide.server_id, MAX_SERVER_ID32 + 1);

    server_id.store(7, Ordering::SeqCst);
    let narrow = allocator.allocate();
    assert!(!narrow.is_64bits);
    assert_eq!(narrow.server_id, 7);
    assert_eq!(narrow.local_conn_id, 1);
}

#[test]
fn full_width_allocator_upgrades_and_release_downgrades() {
    let getter_calls = Arc::new(AtomicU64::new(0));
    let calls = Arc::clone(&getter_calls);
    let allocator = GlobalAllocator::new(
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            7
        },
        true,
    );

    let first32 = allocator.next_id();
    let (first, truncated) = parse_conn_id(first32).unwrap();
    assert!(!truncated);
    assert!(!first.is_64bits);
    assert_eq!(first.local_conn_id, 1);

    let mut last = first;
    for _ in 1..MAX_LOCAL_CONN_ID32 {
        last = allocator.allocate();
    }
    assert!(!last.is_64bits);
    assert_eq!(last.local_conn_id, MAX_LOCAL_CONN_ID32);

    let first64 = allocator.allocate();
    assert!(first64.is_64bits);
    assert_eq!(first64.local_conn_id, 1);

    allocator.release(first32);
    let after_release = allocator.allocate();
    assert!(!after_release.is_64bits);
    assert_eq!(after_release.local_conn_id, 1);
    assert_eq!(getter_calls.load(Ordering::SeqCst), MAX_LOCAL_CONN_ID32 + 2);
}
