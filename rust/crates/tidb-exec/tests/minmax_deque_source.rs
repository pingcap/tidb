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

//! Source-backed tests for the max/min deque helper.

use std::cmp::Ordering;

use tidb_exec::minmax_deque::{DequeError, MinMaxDeque};

fn compare_i64(left: &i64, right: &i64) -> Ordering {
    left.cmp(right)
}

#[test]
fn deque_reset_matches_source() {
    // Source: pkg/executor/aggfuncs/func_max_min.go:29-39, 94-96.
    // Direct Go coverage: pkg/executor/aggfuncs/func_max_min_test.go:335
    // (TestDequeReset).
    let mut deque = MinMaxDeque::new(true, compare_i64);
    deque.push_back(0, 12);
    deque.reset();
    assert!(deque.is_empty());
    assert!(deque.is_max());
}

#[test]
fn deque_push_pop_front_back_match_source() {
    // Source: pkg/executor/aggfuncs/func_max_min.go:41-80.
    // Direct Go coverage: pkg/executor/aggfuncs/func_max_min_test.go:345
    // (TestDequePushPop).
    let mut deque = MinMaxDeque::new(true, compare_i64);
    for idx in 0..15 {
        if idx != 0 {
            let front = deque.front().expect("front after first push");
            assert_eq!(front.item, 0);
            assert_eq!(front.idx, 0);
        }
        deque.push_back(idx, idx as i64);
        let back = deque.back().expect("back after push");
        assert_eq!(back.item, idx as i64);
        assert_eq!(back.idx, idx);
    }
    for idx in (0..15).rev() {
        let back = deque.back().expect("back before pop");
        assert_eq!(back.item, idx as i64);
        assert_eq!(back.idx, idx);
        let front = deque.front().expect("front before pop");
        assert_eq!(front.item, 0);
        assert_eq!(front.idx, 0);
        deque.pop_back().unwrap();
    }
    assert!(deque.is_empty());
    assert_eq!(deque.pop_back(), Err(DequeError::EmptyBack));
}

#[test]
fn deque_enqueue_and_dequeue_preserve_max_min_window_invariant() {
    // Source: pkg/executor/aggfuncs/func_max_min.go:98-145.
    let mut max = MinMaxDeque::new(true, compare_i64);
    max.enqueue(0, 1).unwrap();
    max.enqueue(1, 3).unwrap();
    max.enqueue(2, 2).unwrap();
    assert_eq!(
        max.items().iter().map(|pair| pair.item).collect::<Vec<_>>(),
        vec![3, 2]
    );
    max.dequeue(0).unwrap();
    assert_eq!(max.front().map(|pair| pair.item), Some(3));
    max.dequeue(1).unwrap();
    assert_eq!(max.front().map(|pair| pair.item), Some(2));

    let mut min = MinMaxDeque::new(false, compare_i64);
    min.enqueue(0, 3).unwrap();
    min.enqueue(1, 1).unwrap();
    min.enqueue(2, 2).unwrap();
    assert_eq!(
        min.items().iter().map(|pair| pair.item).collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn deque_partial_state_is_resettable() {
    // Source: pkg/executor/aggfuncs/func_max_min.go:34-39, 94-96.
    let mut deque = MinMaxDeque::new(false, compare_i64);
    deque.enqueue(4, 7).unwrap();
    deque.enqueue(5, 8).unwrap();
    assert_eq!(deque.len(), 2);
    deque.reset();
    assert_eq!(deque.len(), 0);
    assert_eq!(deque.pop_front(), Err(DequeError::EmptyFront));
}
