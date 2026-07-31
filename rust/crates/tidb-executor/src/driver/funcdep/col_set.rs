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

//! Go `pkg/util/intset.FastIntSet` in the one role the functional-dependency
//! graph uses it for: an ordered set of column ids.
//!
//! Go's version is a 64-bit word plus an overflow map, a representation chosen
//! for the small dense sets a single query produces. The set OPERATIONS are
//! what `fd_graph.go` is written against, and they are the same operations on
//! an ordered set, so this is a `BTreeSet` -- eliminating the dense/sparse
//! split rather than reproducing it. The [`std::fmt::Display`] impl reproduces
//! Go's range-collapsing format exactly, because Go's own FD test table
//! asserts on that text.

use std::collections::BTreeSet;

/// An ordered set of column ids.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub(crate) struct ColSet(BTreeSet<i32>);

impl ColSet {
    /// The empty set.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Go `intset.NewFastIntSet(vals...)`.
    pub(crate) fn of(values: impl IntoIterator<Item = i32>) -> Self {
        Self(values.into_iter().collect())
    }

    pub(crate) fn insert(&mut self, value: i32) {
        self.0.insert(value);
    }

    pub(crate) fn remove(&mut self, value: i32) {
        self.0.remove(&value);
    }

    pub(crate) fn contains(&self, value: i32) -> bool {
        self.0.contains(&value)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = i32> + '_ {
        self.0.iter().copied()
    }

    pub(crate) fn subset_of(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }

    pub(crate) fn intersects(&self, other: &Self) -> bool {
        self.0.intersection(&other.0).next().is_some()
    }

    pub(crate) fn union(&self, other: &Self) -> Self {
        Self(self.0.union(&other.0).copied().collect())
    }

    pub(crate) fn union_with(&mut self, other: &Self) {
        self.0.extend(other.0.iter().copied());
    }

    pub(crate) fn difference(&self, other: &Self) -> Self {
        Self(self.0.difference(&other.0).copied().collect())
    }

    pub(crate) fn difference_with(&mut self, other: &Self) {
        for value in other.0.iter() {
            self.0.remove(value);
        }
    }
}

/// Go `FastIntSet.String`: members in order, with runs of three or more
/// collapsed to `start-end`. Go's FD test table asserts on this text, so the
/// collapsing boundary -- a run of exactly two stays `a,b` -- is contract.
impl std::fmt::Display for ColSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn flush(
            f: &mut std::fmt::Formatter<'_>,
            first: &mut bool,
            (start, end): (i32, i32),
        ) -> std::fmt::Result {
            if !*first {
                f.write_str(",")?;
            }
            *first = false;
            if start == end {
                write!(f, "{start}")
            } else if start + 1 == end {
                write!(f, "{start},{end}")
            } else {
                write!(f, "{start}-{end}")
            }
        }
        f.write_str("(")?;
        let mut first = true;
        let mut run: Option<(i32, i32)> = None;
        for value in self.iter() {
            run = match run {
                Some((start, end)) if end + 1 == value => Some((start, value)),
                Some(previous) => {
                    flush(f, &mut first, previous)?;
                    Some((value, value))
                }
                None => Some((value, value)),
            };
        }
        if let Some(previous) = run {
            flush(f, &mut first, previous)?;
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `FastIntSet.String`'s shapes: a run of two is listed, a run of three
    /// collapses.
    #[test]
    fn formats_like_go() {
        assert_eq!(ColSet::new().to_string(), "()");
        assert_eq!(ColSet::of([1, 2]).to_string(), "(1,2)");
        assert_eq!(ColSet::of([1, 2, 3]).to_string(), "(1-3)");
        assert_eq!(ColSet::of([1, 2, 3, 5]).to_string(), "(1-3,5)");
        assert_eq!(ColSet::of([3, 4, 5]).to_string(), "(3-5)");
    }
}
