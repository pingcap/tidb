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

//! Bounded configured `ORDER BY ... LIMIT` and streaming `LIMIT` operators.
//!
//! The TopN shape admits each source row exactly once, retaining at most the
//! planner-checked `offset + count` best rows in a max-heap. It deliberately
//! does not materialize and sort the complete input. A separate LIMIT-only
//! state requests just enough rows to skip the offset and emit the count, then
//! closes its source. Both shapes retain only Campaign 26's non-null signed
//! `BIGINT` configured-order contract; spill, vectorized chunks, rank TopN,
//! arbitrary datum ordering, and memory trackers remain outside this owner.

use std::{cmp::Ordering, error::Error, fmt};

use tidb_planner::configured_order_limit_contract::{
    ConfiguredLimitWindow, ConfiguredOrderLimitSpec,
};

use crate::{
    order::{compare_configured_rows, validate_configured_order_rows, ConfiguredOrderError},
    Row,
};

/// A checked failure in the bounded configured TopN state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredTopNError {
    /// The configured checked end exceeds the caller's process-wide capacity.
    CapacityExceeded {
        /// The planner-checked exclusive end (`offset + count`).
        end_exclusive: usize,
        /// The maximum number of candidates this executor is allowed to retain.
        capacity: usize,
    },
    /// The row count cannot assign another stable source ordinal.
    SourceOrdinalOverflow,
    /// A physical configured row violates the canonical order contract.
    Order(ConfiguredOrderError),
}

impl fmt::Display for ConfiguredTopNError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityExceeded {
                end_exclusive,
                capacity,
            } => write!(
                formatter,
                "configured TopN end {end_exclusive} exceeds capacity {capacity}"
            ),
            Self::SourceOrdinalOverflow => {
                formatter.write_str("configured TopN source ordinal overflows")
            }
            Self::Order(error) => error.fmt(formatter),
        }
    }
}

impl Error for ConfiguredTopNError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Order(error) => Some(error),
            Self::CapacityExceeded { .. } | Self::SourceOrdinalOverflow => None,
        }
    }
}

impl From<ConfiguredOrderError> for ConfiguredTopNError {
    fn from(error: ConfiguredOrderError) -> Self {
        Self::Order(error)
    }
}

#[derive(Debug)]
struct Candidate {
    row: Row,
    source_ordinal: usize,
}

/// One bounded TopN execution state.
///
/// The backing vector is a max-heap: its root is the worst retained candidate
/// according to the configured key tuple and source ordinal. Equal key tuples
/// therefore keep their original source order without relying on the raw heap
/// layout.
#[derive(Debug)]
pub struct ConfiguredTopN {
    spec: ConfiguredOrderLimitSpec,
    full_schema_width: usize,
    candidates: Vec<Candidate>,
    rows_consumed: usize,
}

impl ConfiguredTopN {
    /// Constructs a bounded TopN without consuming a source row.
    ///
    /// A zero-count window is structurally empty, so callers can observe it
    /// before opening PD/TiKV and it neither validates order keys nor reserves
    /// candidate capacity. Every nonempty window checks capacity and physical
    /// key offsets here, before any source row can be read.
    pub fn new(
        spec: ConfiguredOrderLimitSpec,
        full_schema_width: usize,
        capacity: usize,
    ) -> Result<Self, ConfiguredTopNError> {
        if spec.limit().is_empty() {
            return Ok(Self {
                spec,
                full_schema_width,
                candidates: Vec::new(),
                rows_consumed: 0,
            });
        }
        let end_exclusive = spec.limit().end_exclusive();
        if end_exclusive > capacity {
            return Err(ConfiguredTopNError::CapacityExceeded {
                end_exclusive,
                capacity,
            });
        }
        validate_configured_order_rows(&[], full_schema_width, spec.order_keys())?;
        Ok(Self {
            spec,
            full_schema_width,
            candidates: Vec::with_capacity(end_exclusive),
            rows_consumed: 0,
        })
    }

    /// Admits one source row, replacing the current worst candidate only when
    /// this row is better under the canonical configured ordering.
    pub fn push(&mut self, row: Row) -> Result<(), ConfiguredTopNError> {
        if self.is_empty() {
            return Ok(());
        }
        validate_configured_order_rows(
            std::slice::from_ref(&row),
            self.full_schema_width,
            self.spec.order_keys(),
        )?;
        let source_ordinal = self.rows_consumed;
        self.rows_consumed = self
            .rows_consumed
            .checked_add(1)
            .ok_or(ConfiguredTopNError::SourceOrdinalOverflow)?;

        let candidate = Candidate {
            row,
            source_ordinal,
        };
        if self.candidates.len() < self.spec.limit().end_exclusive() {
            self.candidates.push(candidate);
            let index = self.candidates.len() - 1;
            self.sift_up(index);
            return Ok(());
        }

        if self.compare_candidates(&candidate, &self.candidates[0]) == Ordering::Less {
            self.candidates[0] = candidate;
            self.sift_down(0);
        }
        Ok(())
    }

    /// Returns the number of rows currently retained by the bounded heap.
    #[must_use]
    pub fn retained_len(&self) -> usize {
        self.candidates.len()
    }

    /// Returns whether this is a typed `LIMIT 0` fast path that needs no
    /// upstream source, ordering validation, or candidate allocation.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.spec.limit().is_empty()
    }

    /// Finalizes the heap in canonical output order and applies the offset.
    #[must_use]
    pub fn finish(mut self) -> Vec<Row> {
        let keys = self.spec.order_keys();
        self.candidates.sort_by(|left, right| {
            compare_configured_rows(&left.row, &right.row, keys)
                .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
        });
        let limit = self.spec.limit();
        self.candidates
            .into_iter()
            .skip(limit.offset())
            .take(limit.count())
            .map(|candidate| candidate.row)
            .collect()
    }

    fn compare_candidates(&self, left: &Candidate, right: &Candidate) -> Ordering {
        compare_configured_rows(&left.row, &right.row, self.spec.order_keys())
            .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
    }

    fn sift_up(&mut self, mut child: usize) {
        while child > 0 {
            let parent = (child - 1) / 2;
            if self.compare_candidates(&self.candidates[child], &self.candidates[parent])
                != Ordering::Greater
            {
                return;
            }
            self.candidates.swap(child, parent);
            child = parent;
        }
    }

    fn sift_down(&mut self, mut parent: usize) {
        loop {
            let left = parent * 2 + 1;
            if left >= self.candidates.len() {
                return;
            }
            let right = left + 1;
            let largest = if right < self.candidates.len()
                && self.compare_candidates(&self.candidates[right], &self.candidates[left])
                    == Ordering::Greater
            {
                right
            } else {
                left
            };
            if self.compare_candidates(&self.candidates[largest], &self.candidates[parent])
                != Ordering::Greater
            {
                return;
            }
            self.candidates.swap(parent, largest);
            parent = largest;
        }
    }
}

/// A row source that can be closed as soon as a LIMIT-only window completes.
pub trait ConfiguredRowSource {
    /// The source's own failure type.
    type Error;

    /// Returns one source row, or `None` after the source is exhausted.
    fn next_row(&mut self) -> Result<Option<Row>, Self::Error>;

    /// Releases/cancels the source. It must be safe to call after exhaustion.
    fn close(&mut self);
}

/// Lazy LIMIT-only state that never fetches beyond its requested window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfiguredLimitStream {
    limit: ConfiguredLimitWindow,
    rows_skipped: usize,
    rows_emitted: usize,
    source_closed: bool,
}

impl ConfiguredLimitStream {
    /// Creates a lazy LIMIT-only window state.
    #[must_use]
    pub const fn new(limit: ConfiguredLimitWindow) -> Self {
        Self {
            limit,
            rows_skipped: 0,
            rows_emitted: 0,
            source_closed: false,
        }
    }

    /// Requests at most one next emitted row, closing the source at the exact
    /// end of the LIMIT window or on source exhaustion/error.
    pub fn next<S: ConfiguredRowSource>(
        &mut self,
        source: &mut S,
    ) -> Result<Option<Row>, S::Error> {
        if self.source_closed {
            return Ok(None);
        }
        if self.limit.is_empty() {
            self.close_upstream(source);
            return Ok(None);
        }

        while self.rows_skipped < self.limit.offset() {
            match source.next_row() {
                Ok(Some(_)) => self.rows_skipped += 1,
                Ok(None) => {
                    self.close_upstream(source);
                    return Ok(None);
                }
                Err(error) => {
                    self.close_upstream(source);
                    return Err(error);
                }
            }
        }

        if self.rows_emitted == self.limit.count() {
            self.close_upstream(source);
            return Ok(None);
        }
        match source.next_row() {
            Ok(Some(row)) => {
                self.rows_emitted += 1;
                if self.rows_emitted == self.limit.count() {
                    self.close_upstream(source);
                }
                Ok(Some(row))
            }
            Ok(None) => {
                self.close_upstream(source);
                Ok(None)
            }
            Err(error) => {
                self.close_upstream(source);
                Err(error)
            }
        }
    }

    /// Closes the upstream without requesting another row.
    ///
    /// Connection adapters use this when their result-set lifecycle ends
    /// before the LIMIT window naturally completes. It is deliberately
    /// idempotent so `Close`, `Drop`, and a natural terminal pull share one
    /// upstream release.
    pub fn close<S: ConfiguredRowSource>(&mut self, source: &mut S) {
        self.close_upstream(source);
    }

    fn close_upstream<S: ConfiguredRowSource>(&mut self, source: &mut S) {
        if !self.source_closed {
            source.close();
            self.source_closed = true;
        }
    }
}
