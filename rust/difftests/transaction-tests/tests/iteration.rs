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

//! Direct source-behavior tests for `pkg/kv/{iter,utils}.go`.

use std::cell::Cell;
use std::rc::Rc;

use tidb_txnkv::{next_until, walk_mem_buffer, Key, KvIterator, KvRetriever};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestError {
    Create,
    Next,
    Callback,
}

struct MockIterator {
    entries: Vec<(Key, Vec<u8>)>,
    index: usize,
    next_error_at: Option<usize>,
    next_calls: Rc<Cell<usize>>,
    close_calls: Rc<Cell<usize>>,
}

impl KvIterator for MockIterator {
    type Error = TestError;

    fn valid(&self) -> bool {
        self.index < self.entries.len()
    }

    fn key(&self) -> &Key {
        &self.entries[self.index].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.index].1
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        self.next_calls.set(self.next_calls.get() + 1);
        if self.next_error_at == Some(self.index) {
            return Err(TestError::Next);
        }
        self.index += 1;
        Ok(())
    }

    fn close(&mut self) {
        self.close_calls.set(self.close_calls.get() + 1);
    }
}

struct MockRetriever {
    entries: Vec<(Key, Vec<u8>)>,
    next_error_at: Option<usize>,
    create_error: Option<TestError>,
    next_calls: Rc<Cell<usize>>,
    close_calls: Rc<Cell<usize>>,
}

impl KvRetriever for MockRetriever {
    type Error = TestError;
    type Iter = MockIterator;

    fn iter(
        &self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iter, Self::Error> {
        assert!(start.is_none());
        assert!(upper_bound.is_none());
        if let Some(error) = self.create_error {
            return Err(error);
        }
        Ok(MockIterator {
            entries: self.entries.clone(),
            index: 0,
            next_error_at: self.next_error_at,
            next_calls: Rc::clone(&self.next_calls),
            close_calls: Rc::clone(&self.close_calls),
        })
    }
}

fn retriever(entries: &[(&[u8], &[u8])]) -> MockRetriever {
    MockRetriever {
        entries: entries
            .iter()
            .map(|(key, value)| (Key::from_bytes(*key), value.to_vec()))
            .collect(),
        next_error_at: None,
        create_error: None,
        next_calls: Rc::new(Cell::new(0)),
        close_calls: Rc::new(Cell::new(0)),
    }
}

/// `NextUntil` tests the current key before advancing and leaves a match in place.
#[test]
fn test_next_until_stops_on_match() {
    let retriever = retriever(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]);
    let mut iterator = retriever.iter(None, None).expect("iterator creates");
    next_until(&mut iterator, |key| key.as_bytes() == b"b").expect("match succeeds");
    assert_eq!(iterator.key().as_bytes(), b"b");
    assert_eq!(retriever.next_calls.get(), 1);
    assert_eq!(retriever.close_calls.get(), 0);
    iterator.close();
}

/// The source returns `Iterator.Next` errors unchanged and does not close here.
#[test]
fn test_next_until_propagates_iterator_error() {
    let mut retriever = retriever(&[(b"a", b"1"), (b"b", b"2")]);
    retriever.next_error_at = Some(0);
    let mut iterator = retriever.iter(None, None).expect("iterator creates");
    assert_eq!(next_until(&mut iterator, |_| false), Err(TestError::Next));
    assert_eq!(retriever.close_calls.get(), 0);
    iterator.close();
}

#[test]
fn test_walk_mem_buffer_visits_all_entries_and_closes() {
    let retriever = retriever(&[(b"a", b"1"), (b"b", b"2")]);
    let mut seen = Vec::new();
    walk_mem_buffer(&retriever, |key, value| {
        seen.push((key.as_bytes().to_vec(), value.to_vec()));
        Ok(())
    })
    .expect("walk succeeds");
    assert_eq!(
        seen,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec())
        ]
    );
    assert_eq!(retriever.next_calls.get(), 2);
    assert_eq!(retriever.close_calls.get(), 1);
}

#[test]
fn test_walk_mem_buffer_propagates_callback_error_and_closes() {
    let retriever = retriever(&[(b"a", b"1")]);
    let mut callbacks = 0;
    let error = walk_mem_buffer(&retriever, |_key, _value| {
        callbacks += 1;
        Err(TestError::Callback)
    })
    .expect_err("callback fails");
    assert_eq!(error, TestError::Callback);
    assert_eq!(callbacks, 1);
    assert_eq!(retriever.next_calls.get(), 0);
    assert_eq!(retriever.close_calls.get(), 1);
}

#[test]
fn test_walk_mem_buffer_empty_iteration_closes_without_callback() {
    let retriever = retriever(&[]);
    let mut callbacks = 0;
    walk_mem_buffer(&retriever, |_key, _value| {
        callbacks += 1;
        Ok(())
    })
    .expect("empty walk succeeds");
    assert_eq!(callbacks, 0);
    assert_eq!(retriever.next_calls.get(), 0);
    assert_eq!(retriever.close_calls.get(), 1);
}

#[test]
fn test_walk_mem_buffer_propagates_iterator_error_and_closes() {
    let mut retriever = retriever(&[(b"a", b"1"), (b"b", b"2")]);
    retriever.next_error_at = Some(0);
    let error = walk_mem_buffer(&retriever, |_key, _value| Ok(())).expect_err("next fails");
    assert_eq!(error, TestError::Next);
    assert_eq!(retriever.next_calls.get(), 1);
    assert_eq!(retriever.close_calls.get(), 1);
}

#[test]
fn test_walk_mem_buffer_propagates_iterator_creation_error() {
    let mut retriever = retriever(&[]);
    retriever.create_error = Some(TestError::Create);
    let error = walk_mem_buffer(&retriever, |_key, _value| Ok(())).expect_err("create fails");
    assert_eq!(error, TestError::Create);
    assert_eq!(retriever.close_calls.get(), 0);
}
