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

//! Merged dirty-buffer and snapshot iteration translated from
//! `pkg/store/driver/txn/union_iter.go` and consumed directly by the
//! transaction read driver.

use std::cmp::Ordering;
use std::fmt;

use crate::{Key, KvIterator};

/// Merges a transaction's dirty iterator with its snapshot iterator.
///
/// Both inputs must already be ordered in the direction selected by `reverse`.
/// Dirty values override equal snapshot keys, and an empty dirty value is the
/// source tombstone representation and is never returned to the caller.
pub struct UnionIter<D, S> {
    dirty: Option<D>,
    snapshot: Option<S>,
    dirty_valid: bool,
    snapshot_valid: bool,
    current_is_dirty: bool,
    valid: bool,
    reverse: bool,
}

/// Constructor failure that preserves caller ownership of both inputs.
///
/// Go callers retain their iterator interface values when `NewUnionIter`
/// fails and explicitly close both. Rust moves inputs into constructors, so
/// the error returns them without closing or replacing the originating error.
pub struct UnionIterInitError<D, S, E> {
    error: E,
    dirty: D,
    snapshot: S,
}

impl<D, S, E> UnionIterInitError<D, S, E> {
    /// Borrows the unchanged originating iterator error.
    pub fn error(&self) -> &E {
        &self.error
    }

    /// Returns the error and both still-open input iterators to the caller.
    pub fn into_parts(self) -> (E, D, S) {
        (self.error, self.dirty, self.snapshot)
    }
}

impl<D, S, E: fmt::Debug> fmt::Debug for UnionIterInitError<D, S, E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnionIterInitError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<D, S> UnionIter<D, S>
where
    D: KvIterator,
    S: KvIterator<Error = D::Error>,
{
    /// Creates a union iterator at the first visible key.
    ///
    /// As in Go `NewUnionIter`, an error encountered while skipping an initial
    /// tombstone or an equal-key input is returned unchanged. The input
    /// iterators are not closed on this constructor error.
    pub fn new(
        dirty: D,
        snapshot: S,
        reverse: bool,
    ) -> Result<Self, UnionIterInitError<D, S, D::Error>> {
        let dirty_valid = dirty.valid();
        let snapshot_valid = snapshot.valid();
        let mut iterator = Self {
            dirty: Some(dirty),
            snapshot: Some(snapshot),
            dirty_valid,
            snapshot_valid,
            current_is_dirty: false,
            valid: false,
            reverse,
        };
        match iterator.update_current() {
            Ok(()) => Ok(iterator),
            Err(error) => Err(UnionIterInitError {
                error,
                dirty: iterator
                    .dirty
                    .take()
                    .expect("constructor retains the dirty iterator"),
                snapshot: iterator
                    .snapshot
                    .take()
                    .expect("constructor retains the snapshot iterator"),
            }),
        }
    }

    fn dirty(&self) -> &D {
        self.dirty
            .as_ref()
            .expect("dirty iterator is unavailable after close")
    }

    fn dirty_mut(&mut self) -> &mut D {
        self.dirty
            .as_mut()
            .expect("dirty iterator is unavailable after close")
    }

    fn snapshot(&self) -> &S {
        self.snapshot
            .as_ref()
            .expect("snapshot iterator is unavailable after close")
    }

    fn snapshot_mut(&mut self) -> &mut S {
        self.snapshot
            .as_mut()
            .expect("snapshot iterator is unavailable after close")
    }

    fn advance_dirty(&mut self) -> Result<(), D::Error> {
        let result = self.dirty_mut().next();
        self.dirty_valid = self.dirty().valid();
        result
    }

    fn advance_snapshot(&mut self) -> Result<(), D::Error> {
        let result = self.snapshot_mut().next();
        self.snapshot_valid = self.snapshot().valid();
        result
    }

    fn update_current(&mut self) -> Result<(), D::Error> {
        self.valid = true;
        loop {
            if !self.dirty_valid && !self.snapshot_valid {
                self.valid = false;
                break;
            }

            if !self.dirty_valid {
                self.current_is_dirty = false;
                break;
            }

            if !self.snapshot_valid {
                self.current_is_dirty = true;
                if self.dirty().value().is_empty() {
                    self.advance_dirty()?;
                    continue;
                }
                break;
            }

            let mut ordering = self.dirty().key().compare(self.snapshot().key());
            if self.reverse {
                ordering = ordering.reverse();
            }

            match ordering {
                Ordering::Equal => {
                    if self.dirty().value().is_empty() {
                        // Preserve the source error order: dirty advances first,
                        // and snapshot is untouched when that advance fails.
                        self.advance_dirty()?;
                        self.advance_snapshot()?;
                        continue;
                    }
                    // The snapshot advances before the dirty value is exposed,
                    // so the next call advances dirty and cannot repeat the key.
                    self.advance_snapshot()?;
                    self.current_is_dirty = true;
                    break;
                }
                Ordering::Greater => {
                    self.current_is_dirty = false;
                    break;
                }
                Ordering::Less => {
                    if self.dirty().value().is_empty() {
                        self.advance_dirty()?;
                        continue;
                    }
                    self.current_is_dirty = true;
                    break;
                }
            }
        }
        Ok(())
    }

    /// Returns whether the current position contains a visible entry.
    pub fn valid(&self) -> bool {
        self.valid
    }

    /// Returns the current key.
    pub fn key(&self) -> &Key {
        if self.current_is_dirty {
            self.dirty().key()
        } else {
            self.snapshot().key()
        }
    }

    /// Returns the current value.
    pub fn value(&self) -> &[u8] {
        if self.current_is_dirty {
            self.dirty().value()
        } else {
            self.snapshot().value()
        }
    }

    fn advance_union(&mut self) -> Result<(), D::Error> {
        if self.current_is_dirty {
            self.advance_dirty()?;
        } else {
            self.advance_snapshot()?;
        }
        self.update_current()
    }

    /// Closes both inputs. Repeated calls are safe.
    pub fn close(&mut self) {
        if let Some(mut snapshot) = self.snapshot.take() {
            snapshot.close();
        }
        if let Some(mut dirty) = self.dirty.take() {
            dirty.close();
        }
    }
}

impl<D, S> KvIterator for UnionIter<D, S>
where
    D: KvIterator,
    S: KvIterator<Error = D::Error>,
{
    type Error = D::Error;

    fn valid(&self) -> bool {
        UnionIter::valid(self)
    }

    fn key(&self) -> &Key {
        UnionIter::key(self)
    }

    fn value(&self) -> &[u8] {
        UnionIter::value(self)
    }

    fn next(&mut self) -> Result<(), Self::Error> {
        self.advance_union()
    }

    fn close(&mut self) {
        UnionIter::close(self);
    }
}
