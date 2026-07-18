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

//! Source-shaped iterator helpers translated from `pkg/kv/{iter,utils}.go`
//! and `pkg/util/prefix_helper.go`.
//!
//! The Go helpers consume the wide `kv.Iterator` and `kv.Retriever` interfaces,
//! but their behavior only needs a small set of operations.  These traits make
//! that boundary explicit without inventing an in-memory buffer or a TiKV
//! client.  A real storage owner can implement the traits when its iterator
//! contract is ready.

use crate::Key;

/// The iterator operations required by [`next_until`] and [`walk_mem_buffer`].
///
/// `key` and `value` borrow the current entry, as Go's iterator methods do.
/// `close` has no error result because the source `kv.Iterator.Close` contract
/// is likewise a best-effort cleanup operation.
pub trait KvIterator {
    /// The error returned by advancing this iterator.
    type Error;

    /// Returns whether the current position contains an entry.
    fn valid(&self) -> bool;

    /// Returns the key at the current position.
    fn key(&self) -> &Key;

    /// Returns the value at the current position.
    fn value(&self) -> &[u8];

    /// Advances to the next position, propagating the source iterator error.
    fn next(&mut self) -> Result<(), Self::Error>;

    /// Closes the iterator.
    fn close(&mut self);
}

/// The retriever operation required by [`walk_mem_buffer`].
pub trait KvRetriever {
    /// The error returned when creating the iterator or advancing it.
    type Error;

    /// The concrete iterator returned by this retriever.
    type Iter: KvIterator<Error = Self::Error>;

    /// Creates an iterator over the half-open range `[start, upper_bound)`.
    /// `None` is the source Go `nil` unbounded value.
    fn iter(
        &self,
        start: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iter, Self::Error>;
}

/// Advances an iterator until `fn_key_cmp` matches or the iterator is invalid.
///
/// This is a direct translation of `pkg/kv/iter.go:19 NextUntil`: the current
/// key is tested before each call to `next`, a matching key is not consumed,
/// and no cleanup is performed because the Go caller owns the iterator.
pub fn next_until<I, F>(iterator: &mut I, mut fn_key_cmp: F) -> Result<(), I::Error>
where
    I: KvIterator,
    F: FnMut(&Key) -> bool,
{
    while iterator.valid() && !fn_key_cmp(iterator.key()) {
        iterator.next()?;
    }
    Ok(())
}

/// Returns the source key comparator that stops after a row-key prefix.
///
/// This directly translates `pkg/util/prefix_helper.go::RowKeyPrefixFilter`:
/// keys inside the captured prefix return `false`, while the first key outside
/// it returns `true`. The result composes directly with [`next_until`].
pub fn row_key_prefix_filter(row_key_prefix: Key) -> impl Fn(&Key) -> bool {
    move |current_key| !current_key.has_prefix(&row_key_prefix)
}

/// Walks every buffered key/value pair in a retriever.
///
/// This is a direct translation of `pkg/kv/utils.go:70 WalkMemBuffer` using a
/// narrow trait boundary.  The iterator is always closed, including when
/// iterator creation, callback execution, or advancement returns an error.
/// Callback, creation, and advancement errors share the retriever's error type
/// so each source error is returned unchanged rather than wrapped or erased.
pub fn walk_mem_buffer<R, F>(mem_buf: &R, mut callback: F) -> Result<(), R::Error>
where
    R: KvRetriever,
    F: FnMut(&Key, &[u8]) -> Result<(), R::Error>,
{
    let iterator = mem_buf.iter(None, None)?;
    let mut guard = CloseOnDrop::new(iterator);

    while guard.valid() {
        callback(guard.key(), guard.value())?;
        guard.next()?;
    }

    Ok(())
}

/// Owns an iterator until the source helper returns, ensuring `Close` runs for
/// both successful and error exits (the behavior supplied by Go's `defer`).
struct CloseOnDrop<I: KvIterator> {
    iterator: I,
}

impl<I: KvIterator> CloseOnDrop<I> {
    fn new(iterator: I) -> Self {
        Self { iterator }
    }

    fn valid(&self) -> bool {
        self.iterator.valid()
    }

    fn key(&self) -> &Key {
        self.iterator.key()
    }

    fn value(&self) -> &[u8] {
        self.iterator.value()
    }

    fn next(&mut self) -> Result<(), I::Error> {
        self.iterator.next()
    }
}

impl<I: KvIterator> Drop for CloseOnDrop<I> {
    fn drop(&mut self) {
        self.iterator.close();
    }
}

#[cfg(test)]
mod tests {
    use super::{next_until, walk_mem_buffer, KvIterator, KvRetriever};
    use crate::Key;
    use std::cell::Cell;
    use std::rc::Rc;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TestError {
        Create,
        Next,
        Callback,
    }

    struct TestIterator {
        entries: Vec<(Key, Vec<u8>)>,
        index: usize,
        next_error_at: Option<usize>,
        next_calls: Rc<Cell<usize>>,
        close_calls: Rc<Cell<usize>>,
    }

    impl KvIterator for TestIterator {
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

    struct TestRetriever {
        entries: Vec<(Key, Vec<u8>)>,
        next_error_at: Option<usize>,
        create_error: Option<TestError>,
        next_calls: Rc<Cell<usize>>,
        close_calls: Rc<Cell<usize>>,
    }

    impl KvRetriever for TestRetriever {
        type Error = TestError;
        type Iter = TestIterator;

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
            Ok(TestIterator {
                entries: self.entries.clone(),
                index: 0,
                next_error_at: self.next_error_at,
                next_calls: Rc::clone(&self.next_calls),
                close_calls: Rc::clone(&self.close_calls),
            })
        }
    }

    fn entries() -> Vec<(Key, Vec<u8>)> {
        vec![
            (Key::from_bytes(b"a"), b"1".to_vec()),
            (Key::from_bytes(b"b"), b"2".to_vec()),
            (Key::from_bytes(b"c"), b"3".to_vec()),
        ]
    }

    fn retriever(entries: Vec<(Key, Vec<u8>)>) -> TestRetriever {
        TestRetriever {
            entries,
            next_error_at: None,
            create_error: None,
            next_calls: Rc::new(Cell::new(0)),
            close_calls: Rc::new(Cell::new(0)),
        }
    }

    #[test]
    fn next_until_stops_on_match_without_consuming_matching_key() {
        let retriever = retriever(entries());
        let mut iterator = retriever.iter(None, None).expect("iterator creates");
        next_until(&mut iterator, |key| key.as_bytes() == b"b").expect("match succeeds");
        assert_eq!(iterator.key().as_bytes(), b"b");
        assert_eq!(retriever.next_calls.get(), 1);
        iterator.close();
    }

    #[test]
    fn next_until_propagates_iterator_error_without_closing() {
        let retriever = retriever(entries());
        let mut iterator = retriever.iter(None, None).expect("iterator creates");
        iterator.next_error_at = Some(0);
        let error = next_until(&mut iterator, |_| false).expect_err("next fails");
        assert_eq!(error, TestError::Next);
        assert_eq!(retriever.close_calls.get(), 0);
        iterator.close();
    }

    #[test]
    fn walk_mem_buffer_visits_every_entry_and_closes() {
        let retriever = retriever(entries());
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
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );
        assert_eq!(retriever.next_calls.get(), 3);
        assert_eq!(retriever.close_calls.get(), 1);
    }

    #[test]
    fn walk_mem_buffer_closes_on_callback_error() {
        let retriever = retriever(entries());
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
    fn walk_mem_buffer_closes_on_iterator_error() {
        let mut retriever = retriever(entries());
        retriever.next_error_at = Some(1);
        let mut callbacks = 0;
        let error = walk_mem_buffer(&retriever, |_key, _value| {
            callbacks += 1;
            Ok(())
        })
        .expect_err("next fails");
        assert_eq!(error, TestError::Next);
        assert_eq!(callbacks, 2);
        assert_eq!(retriever.next_calls.get(), 2);
        assert_eq!(retriever.close_calls.get(), 1);
    }

    #[test]
    fn walk_mem_buffer_empty_iteration_still_closes() {
        let retriever = retriever(Vec::new());
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
    fn walk_mem_buffer_propagates_iterator_creation_error_without_close() {
        let mut retriever = retriever(Vec::new());
        retriever.create_error = Some(TestError::Create);
        let error = walk_mem_buffer(&retriever, |_key, _value| Ok(())).expect_err("create fails");
        assert_eq!(error, TestError::Create);
        assert_eq!(retriever.close_calls.get(), 0);
    }
}
