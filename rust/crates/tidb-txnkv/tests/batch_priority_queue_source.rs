// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Direct transit of client-go `internal/client/priority_queue_test.go`.

use std::sync::Arc;

use tidb_txnkv::rpc::batch::{PriorityItem, PriorityQueue};

#[derive(Debug)]
struct FakeItem {
    priority: u64,
    value: usize,
    canceled: bool,
    retained: Option<Arc<()>>,
}

impl PriorityItem for FakeItem {
    fn priority(&self) -> u64 {
        self.priority
    }

    fn is_canceled(&self) -> bool {
        self.canceled
    }
}

#[test]
fn test_priority() {
    let mut queue = PriorityQueue::new();
    for value in 1..=5 {
        queue.push(FakeItem {
            priority: value as u64,
            value,
            canceled: false,
            retained: None,
        });
    }
    assert_eq!(queue.len(), 5);
    assert_eq!(queue.highest_priority(), 5);
    queue.clean_canceled();
    assert_eq!(queue.len(), 5);

    assert_eq!(queue.take(1)[0].value, 5);
    assert_eq!(queue.highest_priority(), 4);
    assert_eq!(
        queue
            .take(2)
            .into_iter()
            .map(|item| item.value)
            .collect::<Vec<_>>(),
        vec![4, 3]
    );
    assert_eq!(queue.highest_priority(), 2);
    assert_eq!(
        queue
            .take(5)
            .into_iter()
            .map(|item| item.value)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(queue.highest_priority(), 0);
    assert!(queue.is_empty());

    queue.push(FakeItem {
        priority: 1,
        value: 1,
        canceled: true,
        retained: None,
    });
    queue.clean_canceled();
    assert!(queue.is_empty());

    // A full Take exposes the raw heap layout, as in client-go.
    for value in 6..=8 {
        queue.push(FakeItem {
            priority: 1,
            value,
            canceled: false,
            retained: None,
        });
    }
    assert_eq!(
        queue
            .drain()
            .into_iter()
            .map(|item| item.value)
            .collect::<Vec<_>>(),
        vec![6, 7, 8]
    );
}

#[test]
fn test_priority_queue_take_all_leaves_references_in_backing_array() {
    let retained = Arc::new(());
    let mut queue = PriorityQueue::new();
    for value in 1..=3 {
        queue.push(FakeItem {
            priority: value as u64,
            value,
            canceled: false,
            retained: Some(Arc::clone(&retained)),
        });
    }
    assert_eq!(Arc::strong_count(&retained), 4);

    let one = queue.take(1);
    assert_eq!(queue.len(), 2);
    assert_eq!(Arc::strong_count(&retained), 4);
    drop(one);
    assert_eq!(Arc::strong_count(&retained), 3);

    let rest = queue.take(queue.len());
    assert!(queue.is_empty());
    assert_eq!(Arc::strong_count(&retained), 3);
    assert!(rest.iter().all(|item| item.retained.is_some()));
    drop(rest);
    assert_eq!(Arc::strong_count(&retained), 1);

    for value in 1..=3 {
        queue.push(FakeItem {
            priority: value as u64,
            value,
            canceled: true,
            retained: Some(Arc::clone(&retained)),
        });
    }
    queue.clean_canceled();
    assert!(queue.is_empty());
    assert_eq!(Arc::strong_count(&retained), 1);
}

#[test]
fn full_take_preserves_source_heap_layout_while_partial_take_pops() {
    let mut queue = PriorityQueue::new();
    for priority in [1, 5, 4, 3, 2] {
        queue.push(FakeItem {
            priority,
            value: priority as usize,
            canceled: false,
            retained: None,
        });
    }
    assert_eq!(
        queue
            .take(queue.len())
            .into_iter()
            .map(|item| item.value)
            .collect::<Vec<_>>(),
        vec![5, 3, 4, 1, 2]
    );

    for priority in [1, 5, 4, 3, 2] {
        queue.push(FakeItem {
            priority,
            value: priority as usize,
            canceled: false,
            retained: None,
        });
    }
    assert_eq!(
        queue
            .take(3)
            .into_iter()
            .map(|item| item.value)
            .collect::<Vec<_>>(),
        vec![5, 4, 3]
    );
}
