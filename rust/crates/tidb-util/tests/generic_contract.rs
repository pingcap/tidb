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

//! Public package contract for Go `pkg/util/generic`.

use std::cell::Cell;
use std::cmp::Ordering;

use tidb_util::generic::{BoundedMinHeap, SyncMap};

#[derive(Clone)]
struct Ranked {
    score: i32,
    id: &'static str,
}

#[test]
fn bounded_heap_public_edges_match_go() {
    let zero_calls = Cell::new(0);
    let mut zero = BoundedMinHeap::new(0, |left: &i32, right: &i32| {
        zero_calls.set(zero_calls.get() + 1);
        left.cmp(right)
    });
    zero.add(1);
    assert!(zero.is_empty());
    assert!(zero.to_sorted_slice().is_empty());
    assert_eq!(zero_calls.get(), 0);

    let calls = Cell::new(0);
    let mut tied = BoundedMinHeap::new(3, |left: &Ranked, right: &Ranked| {
        calls.set(calls.get() + 1);
        left.score.cmp(&right.score)
    });
    for id in ["a", "b", "c", "d"] {
        tied.add(Ranked { score: 1, id });
    }
    let mut retained_ids = tied
        .to_sorted_slice()
        .into_iter()
        .map(|item| item.id)
        .collect::<Vec<_>>();
    retained_ids.sort_unstable();
    assert_eq!(retained_ids, ["a", "b", "c"]);
    assert!(calls.get() > 0);

    let mut heap = BoundedMinHeap::new(3, i32::cmp as fn(&i32, &i32) -> Ordering);
    for item in [5, 3, 8] {
        heap.add(item);
    }
    let mut snapshot = heap.to_sorted_slice();
    snapshot[0] = 100;
    heap.add(9);
    assert_eq!(snapshot, [100, 5, 3]);
    assert_eq!(heap.to_sorted_slice(), [9, 8, 5]);
}

#[test]
fn sync_map_delete_and_key_snapshots_match_go() {
    let values = SyncMap::new(0);
    values.store("a", 1);
    values.store("a", 2);
    assert_eq!(values.delete(&"a"), Some(2));
    assert_eq!(values.delete(&"missing"), None);

    let old_keys = values.keys();
    values.store("later", 3);
    assert!(old_keys.is_empty());
    assert_eq!(values.keys(), ["later"]);
}
