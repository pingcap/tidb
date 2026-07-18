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

//! Source-backed tests for the generic bounded min-heap.
//!
//! These mirror all seven exact Go tests in
//! `pkg/util/generic/bounded_min_heap_test.go` (lines 44, 76, 94, 116, 135,
//! 164, and 186). `pkg/statistics/builder.go` uses the same heap to retain
//! TopN candidates; TopN encoding and histogram construction remain external.

use tidb_stats::BoundedMinHeap;

fn int_comparator(a: &i32, b: &i32) -> i32 {
    match a.cmp(b) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

fn non_unit_comparator(a: &i32, b: &i32) -> i32 {
    if a < b {
        -2
    } else if a > b {
        1
    } else {
        0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestItem {
    value: i32,
    name: &'static str,
}

fn test_item_comparator(a: &TestItem, b: &TestItem) -> i32 {
    int_comparator(&a.value, &b.value)
}

#[test]
fn source_bounded_min_heap_basic() {
    let mut heap = BoundedMinHeap::new(3, Some(int_comparator));
    assert_eq!(heap.len(), 0);
    assert!(heap.is_empty());
    assert_eq!(heap.to_sorted_slice(), None);

    for value in [5, 3, 8] {
        heap.add(value);
    }
    assert!(!heap.is_empty());
    assert_eq!(heap.to_sorted_slice(), Some(vec![8, 5, 3]));

    for value in [1, 9, 2, 7, 4] {
        heap.add(value);
    }
    assert_eq!(heap.len(), 3);
    assert_eq!(heap.to_sorted_slice(), Some(vec![9, 8, 7]));

    let mut duplicates = BoundedMinHeap::new(3, Some(int_comparator));
    for value in [5, 5, 3, 8, 5] {
        duplicates.add(value);
    }
    assert_eq!(duplicates.to_sorted_slice(), Some(vec![8, 5, 5]));
}

#[test]
fn source_bounded_min_heap_edge_cases() {
    let mut one = BoundedMinHeap::new(1, Some(int_comparator));
    for value in [3, 1, 7, 2] {
        one.add(value);
    }
    assert_eq!(one.len(), 1);
    assert_eq!(one.to_sorted_slice(), Some(vec![7]));

    let mut zero = BoundedMinHeap::new(0, Some(int_comparator));
    zero.add(5);
    zero.add(10);
    assert_eq!(zero.len(), 0);
    assert_eq!(zero.to_sorted_slice(), None);
}

#[test]
fn source_bounded_min_heap_supports_custom_items() {
    let mut heap = BoundedMinHeap::new(3, Some(test_item_comparator));
    for item in [
        TestItem {
            value: 10,
            name: "ten",
        },
        TestItem {
            value: 5,
            name: "five",
        },
        TestItem {
            value: 15,
            name: "fifteen",
        },
        TestItem {
            value: 8,
            name: "eight",
        },
        TestItem {
            value: 12,
            name: "twelve",
        },
    ] {
        heap.add(item);
    }
    assert_eq!(
        heap.to_sorted_slice(),
        Some(vec![
            TestItem {
                value: 15,
                name: "fifteen",
            },
            TestItem {
                value: 12,
                name: "twelve",
            },
            TestItem {
                value: 10,
                name: "ten",
            },
        ])
    );
}

#[test]
fn source_bounded_min_heap_honors_reverse_comparator() {
    let mut heap = BoundedMinHeap::new(3, Some(|a: &i32, b: &i32| int_comparator(b, a)));
    for value in [9, 2, 7, 1, 8, 3] {
        heap.add(value);
    }
    assert_eq!(heap.to_sorted_slice(), Some(vec![1, 2, 3]));
}

#[test]
fn source_bounded_min_heap_negates_original_comparator() {
    let mut heap = BoundedMinHeap::new(3, Some(non_unit_comparator));
    for value in [1, 2, 3] {
        heap.add(value);
    }
    // Go's `-cmp(a, b)` is intentionally used directly; calling cmp(b, a)
    // would produce the opposite order when magnitudes differ.
    assert_eq!(heap.to_sorted_slice(), Some(vec![3, 2, 1]));
}

#[test]
fn source_bounded_min_heap_wraps_min_comparator_negation() {
    let min_comparator = |a: &i32, b: &i32| {
        if a < b {
            i32::MIN
        } else if a > b {
            i32::MAX
        } else {
            0
        }
    };
    let mut heap = BoundedMinHeap::new(2, Some(min_comparator));
    heap.add(1);
    heap.add(2);
    let result = heap.to_sorted_slice().expect("non-empty heap");
    assert_eq!(result.len(), 2);
    assert!(result.contains(&1));
    assert!(result.contains(&2));
}

#[test]
fn source_bounded_min_heap_replaces_only_worse_root() {
    let mut heap = BoundedMinHeap::new(2, Some(int_comparator));
    for value in [5, 3, 10, 8, 2, 1, 4] {
        heap.add(value);
    }
    assert_eq!(heap.to_sorted_slice(), Some(vec![10, 8]));

    let mut ties = BoundedMinHeap::new(3, Some(int_comparator));
    for value in [5, 5, 5, 5] {
        ties.add(value);
    }
    assert_eq!(ties.to_sorted_slice(), Some(vec![5, 5, 5]));
}

#[test]
fn source_bounded_min_heap_handles_large_dataset() {
    let mut heap = BoundedMinHeap::new(10, Some(int_comparator));
    for value in 0..1000 {
        heap.add(value);
    }
    let result = heap.to_sorted_slice().expect("non-empty heap");
    assert_eq!(result, (990..1000).rev().collect::<Vec<_>>());
}

#[test]
fn source_bounded_min_heap_checks_constructor_safety() {
    assert!(std::panic::catch_unwind(|| {
        let _ = BoundedMinHeap::<i32>::new(10, None::<fn(&i32, &i32) -> i32>);
    })
    .is_err());
    assert!(std::panic::catch_unwind(|| {
        let _ = BoundedMinHeap::new(-1, Some(int_comparator));
    })
    .is_err());
    assert!(std::panic::catch_unwind(|| {
        let _ = BoundedMinHeap::new(0, Some(int_comparator));
    })
    .is_ok());
}
