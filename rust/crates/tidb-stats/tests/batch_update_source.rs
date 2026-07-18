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

//! Source-backed tests for statistics-cache batch update state.

use std::{cell::RefCell, rc::Rc};

use tidb_stats::BatchUpdate;

type Flushes = Rc<RefCell<Vec<(Vec<i32>, Vec<i64>)>>>;
type Callback = Box<dyn FnMut(&[i32], &[i64])>;
type TestBatch = BatchUpdate<i32, Callback>;

fn new_batch(batch_size: usize) -> (TestBatch, Flushes) {
    let flushes = Rc::new(RefCell::new(Vec::new()));
    let captured = Rc::clone(&flushes);
    let callback: Callback = Box::new(move |updates, deletes| {
        captured
            .borrow_mut()
            .push((updates.to_vec(), deletes.to_vec()));
    });
    let batch = BatchUpdate::new(batch_size, callback);
    (batch, flushes)
}

#[test]
fn source_batch_flushes_at_capacity_and_preserves_order() {
    let (mut batch, flushes) = new_batch(2);
    batch.add_update(1);
    batch.add_update(2);
    assert_eq!(batch.pending_updates(), &[1, 2]);
    assert!(flushes.borrow().is_empty());

    batch.add_update(3);
    assert_eq!(flushes.borrow().as_slice(), &[(vec![1, 2], vec![])]);
    assert_eq!(batch.pending_updates(), &[3]);

    batch.flush();
    assert_eq!(
        flushes.borrow().as_slice(),
        &[(vec![1, 2], vec![]), (vec![3], vec![])]
    );
    assert!(batch.pending_updates().is_empty());
}

#[test]
fn source_update_or_delete_capacity_flushes_both_lists() {
    let (mut batch, flushes) = new_batch(2);
    batch.add_update(10);
    batch.add_delete(7);
    batch.add_delete(8);
    assert!(flushes.borrow().is_empty());
    assert_eq!(batch.pending_updates(), &[10]);
    assert_eq!(batch.pending_deletes(), &[7, 8]);

    batch.add_delete(9);
    assert_eq!(flushes.borrow().as_slice(), &[(vec![10], vec![7, 8])]);
    assert!(batch.pending_updates().is_empty());
    assert_eq!(batch.pending_deletes(), &[9]);

    batch.flush();
    assert_eq!(
        flushes.borrow().as_slice(),
        &[(vec![10], vec![7, 8]), (vec![], vec![9])]
    );
}

#[test]
fn source_empty_flush_is_a_noop_and_zero_size_flushes_before_append() {
    let (mut batch, flushes) = new_batch(0);
    batch.flush();
    assert!(flushes.borrow().is_empty());

    batch.add_update(4);
    assert_eq!(flushes.borrow().as_slice(), &[(vec![], vec![])]);
    assert_eq!(batch.pending_updates(), &[4]);
}
