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

//! Source-backed tests for the auto-analyze priority queue leaf.

use std::collections::HashMap;

use tidb_stats::{PriorityHeap, PriorityHeapError, PriorityHeapItem};

fn item(table_id: i64, weight: f64) -> PriorityHeapItem {
    PriorityHeapItem::new(table_id, weight)
}

#[test]
fn source_add_or_update_preserves_max_heap_and_replaces_by_key() {
    let mut heap = PriorityHeap::new();
    for value in [(1, 10.0), (2, 1.0), (3, 11.0), (4, 30.0)] {
        heap.add_or_update(item(value.0, value.1)).unwrap();
    }
    heap.add_or_update(item(1, 13.0)).unwrap();

    assert_eq!(heap.pop().unwrap().table_id, 4);
    assert_eq!(heap.pop().unwrap().table_id, 1);

    heap.delete(3).unwrap();
    heap.add_or_update(item(1, 14.0)).unwrap();
    assert_eq!(heap.pop().unwrap().table_id, 1);
    assert_eq!(heap.pop().unwrap().table_id, 2);
}

#[test]
fn source_empty_pop_and_peek_return_source_error() {
    let mut heap = PriorityHeap::new();
    assert_eq!(heap.pop(), Err(PriorityHeapError::Empty));
    assert_eq!(heap.peek(), Err(PriorityHeapError::Empty));
    assert_eq!(heap.pop().unwrap_err().to_string(), "heap is empty");
}

#[test]
fn source_delete_removes_arbitrary_item_and_repairs_heap() {
    let mut heap = PriorityHeap::new();
    for value in [(1, 10.0), (2, 1.0), (3, 31.0), (4, 11.0)] {
        heap.add_or_update(item(value.0, value.1)).unwrap();
    }
    heap.delete(3).unwrap();
    assert_eq!(heap.pop().unwrap().table_id, 4);

    heap.add_or_update(item(5, 30.0)).unwrap();
    heap.delete(2).unwrap();
    assert_eq!(heap.pop().unwrap().table_id, 5);
    assert_eq!(heap.pop().unwrap().table_id, 1);
    assert_eq!(heap.len(), 0);
    assert_eq!(heap.delete(99), Err(PriorityHeapError::ObjectNotFound));
}

#[test]
fn source_update_repairs_both_directions() {
    let mut heap = PriorityHeap::new();
    for value in [(1, 10.0), (2, 1.0), (3, 31.0), (4, 11.0)] {
        heap.add_or_update(item(value.0, value.1)).unwrap();
    }
    heap.update(item(4, 50.0)).unwrap();
    assert_eq!(heap.peek().unwrap().table_id, 4);
    assert_eq!(heap.pop().unwrap().table_id, 4);

    heap.update(item(2, 100.0)).unwrap();
    assert_eq!(heap.peek().unwrap().table_id, 2);
    heap.update(item(2, -100.0)).unwrap();
    assert_eq!(heap.peek().unwrap().table_id, 3);
}

#[test]
fn source_get_and_get_by_key_are_keyed_lookups() {
    let mut heap = PriorityHeap::new();
    for value in [(1, 10.0), (2, 1.0), (3, 31.0), (4, 11.0)] {
        heap.add_or_update(item(value.0, value.1)).unwrap();
    }
    assert_eq!(heap.get(4), Some(item(4, 11.0)));
    assert_eq!(heap.get_by_key(4), Some(item(4, 11.0)));
    assert_eq!(heap.get(5), None);
}

#[test]
fn source_list_and_list_keys_cover_each_live_item() {
    let mut heap = PriorityHeap::new();
    let expected = HashMap::from([(1_i64, 10.0), (2, 1.0), (3, 30.0), (4, 11.0), (5, 30.0)]);
    for (&table_id, &weight) in &expected {
        heap.add_or_update(item(table_id, weight)).unwrap();
    }

    let list = heap.list();
    assert_eq!(list.len(), expected.len());
    for value in list {
        assert_eq!(expected.get(&value.table_id), Some(&value.weight));
    }

    let keys = heap.list_keys();
    assert_eq!(keys.len(), expected.len());
    for table_id in keys {
        assert!(expected.contains_key(&table_id));
    }
}

#[test]
fn source_peek_does_not_remove_the_maximum() {
    let mut heap = PriorityHeap::new();
    for value in [(1, 10.0), (2, 1.0), (3, 31.0), (4, 11.0)] {
        heap.add_or_update(item(value.0, value.1)).unwrap();
    }
    assert_eq!(heap.peek().unwrap().table_id, 3);
    assert_eq!(heap.len(), 4);
    assert_eq!(heap.pop().unwrap().table_id, 3);
}

#[test]
fn source_empty_and_len_track_push_pop_lifecycle() {
    let mut heap = PriorityHeap::new();
    assert!(heap.is_empty());
    assert_eq!(heap.len(), 0);
    heap.add_or_update(item(1, 10.0)).unwrap();
    assert!(!heap.is_empty());
    assert_eq!(heap.len(), 1);
    heap.pop().unwrap();
    assert!(heap.is_empty());
    assert_eq!(heap.len(), 0);
}

#[test]
fn source_nan_weight_keeps_go_greater_than_behavior() {
    let mut heap = PriorityHeap::new();
    heap.add_or_update(item(1, f64::NAN)).unwrap();
    heap.add_or_update(item(2, 1.0)).unwrap();
    // Go's Less uses `>` directly, which is false for either NaN operand.
    assert_eq!(heap.peek().unwrap().table_id, 1);
}
