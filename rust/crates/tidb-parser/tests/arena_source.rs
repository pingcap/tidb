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

//! Direct source tests for `pkg/parser/arena.go`.

use std::collections::HashSet;

use tidb_parser::arena::{alloc, alloc_slice, Arena, Slab, DEFAULT_BLOCK_SIZE, SLAB_SIZE};

#[derive(Default)]
struct Point {
    x: i64,
    y: i64,
}

struct Bytes<const N: usize>([u8; N]);

impl<const N: usize> Default for Bytes<N> {
    fn default() -> Self {
        Self([0; N])
    }
}

#[test]
fn generic_alloc_is_default_initialized_distinct_and_stable() {
    let arena = Arena::new();
    let mut first = alloc::<Point>(&arena);
    assert_eq!((first.x, first.y), (0, 0));
    first.x = 42;
    first.y = 99;
    let second = alloc::<Point>(&arena);
    assert_ne!(std::ptr::from_ref(&*first), std::ptr::from_ref(&*second));
    assert_eq!((second.x, second.y), (0, 0));
    assert_eq!((first.x, first.y), (42, 99));
}

#[test]
fn generic_slice_has_exact_length_defaults_and_zero_length_shape() {
    let arena = Arena::new();
    let mut values = alloc_slice::<i64>(&arena, 10).expect("positive length is non-null");
    assert_eq!(values, vec![0; 10]);
    for (index, value) in values.iter_mut().enumerate() {
        *value = (index * index) as i64;
    }
    assert_eq!(values, vec![0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
    assert!(alloc_slice::<i64>(&arena, 0).is_none());
}

#[test]
fn reset_preserves_live_generic_allocations_and_new_defaults() {
    let mut arena = Arena::new();
    let mut old = alloc::<Bytes<1024>>(&arena);
    old.0[0] = 7;
    arena.reset();
    let fresh = alloc::<Bytes<1024>>(&arena);
    assert!(fresh.0.iter().all(|value| *value == 0));
    assert_eq!(old.0[0], 7);
}

#[test]
fn generic_growth_and_oversized_values_keep_distinct_stable_storage() {
    let arena = Arena::new();
    let values = (0..20)
        .map(|_| alloc::<Bytes<4096>>(&arena))
        .collect::<Vec<_>>();
    let identities = values
        .iter()
        .map(|value| std::ptr::from_ref::<Bytes<4096>>(&**value).cast::<u8>() as usize)
        .collect::<HashSet<_>>();
    assert_eq!(identities.len(), values.len());

    let mut huge = alloc::<Bytes<{ DEFAULT_BLOCK_SIZE * 2 }>>(&arena);
    huge.0[0] = 0xff;
    huge.0[DEFAULT_BLOCK_SIZE * 2 - 1] = 0xfe;
    assert_eq!(
        (huge.0[0], huge.0[DEFAULT_BLOCK_SIZE * 2 - 1]),
        (0xff, 0xfe)
    );
    assert_eq!(*alloc::<i64>(&arena), 0);
}

#[derive(Default)]
struct ManagedNode {
    name: String,
    child: Option<Box<ManagedNode>>,
}

#[test]
fn typed_slab_handles_keep_managed_fields_alive_across_reset() {
    let mut slab = Slab::<ManagedNode>::default();
    let nodes = (0..100)
        .map(|index| {
            let node = slab.alloc();
            node.borrow_mut().name = format!("col_{index}");
            node
        })
        .collect::<Vec<_>>();
    slab.reset();
    for (index, node) in nodes.iter().enumerate() {
        assert_eq!(node.borrow().name, format!("col_{index}"));
        assert!(node.borrow().child.is_none());
    }
}

#[test]
fn typed_slab_allocates_distinct_slots_across_batch_boundary() {
    let mut slab = Slab::<ManagedNode>::default();
    let nodes = (0..SLAB_SIZE + 10)
        .map(|_| slab.alloc())
        .collect::<Vec<_>>();
    let identities = nodes
        .iter()
        .map(|node| node.as_ptr() as usize)
        .collect::<HashSet<_>>();
    assert_eq!(identities.len(), SLAB_SIZE + 10);
}

#[test]
fn cloned_handle_reaches_the_same_typed_slot() {
    let mut slab = Slab::<ManagedNode>::default();
    let first = slab.alloc();
    let alias = first.clone();
    first.borrow_mut().name = "x".to_owned();
    assert_eq!(alias.borrow().name, "x");
    assert_eq!(first.as_ptr(), alias.as_ptr());
}
