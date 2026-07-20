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

/// Immutable accounting from one completed bounded TopN execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfiguredTopNEvidence {
    /// The caller-owned maximum number of retained candidates.
    capacity: usize,
    /// The largest number of candidates retained at any time.
    high_water_candidates: usize,
    /// Number of validated source rows consumed by the state.
    rows_consumed: usize,
    /// Number of rows emitted after the offset window.
    rows_emitted: usize,
}

impl ConfiguredTopNEvidence {
    /// Returns the caller-owned maximum candidate capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the greatest number of candidates retained at one time.
    #[must_use]
    pub const fn high_water_candidates(&self) -> usize {
        self.high_water_candidates
    }

    /// Returns the number of valid rows consumed from the source.
    #[must_use]
    pub const fn rows_consumed(&self) -> usize {
        self.rows_consumed
    }

    /// Returns the number of rows emitted by the final LIMIT window.
    #[must_use]
    pub const fn rows_emitted(&self) -> usize {
        self.rows_emitted
    }
}

/// Rows and immutable accounting returned by a finalized bounded TopN.
#[derive(Debug, PartialEq)]
pub struct ConfiguredTopNResult {
    /// Canonically ordered rows after applying the configured offset/count.
    pub rows: Vec<Row>,
    /// Bounded-memory execution accounting.
    pub evidence: ConfiguredTopNEvidence,
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
    capacity: usize,
    candidates: Vec<Candidate>,
    high_water_candidates: usize,
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
                capacity,
                candidates: Vec::new(),
                high_water_candidates: 0,
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
            capacity,
            candidates: Vec::with_capacity(end_exclusive),
            high_water_candidates: 0,
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
            self.high_water_candidates = self.high_water_candidates.max(self.candidates.len());
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
    pub fn finish(mut self) -> ConfiguredTopNResult {
        let keys = self.spec.order_keys();
        self.candidates.sort_by(|left, right| {
            compare_configured_rows(&left.row, &right.row, keys)
                .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
        });
        let limit = self.spec.limit();
        let rows = self
            .candidates
            .into_iter()
            .skip(limit.offset())
            .take(limit.count())
            .map(|candidate| candidate.row)
            .collect::<Vec<_>>();
        let evidence = ConfiguredTopNEvidence {
            capacity: self.capacity,
            high_water_candidates: self.high_water_candidates,
            rows_consumed: self.rows_consumed,
            rows_emitted: rows.len(),
        };
        ConfiguredTopNResult { rows, evidence }
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

/// Immutable accounting from a streaming LIMIT-only execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfiguredLimitEvidence {
    /// Number of rows requested from the upstream source.
    rows_requested: usize,
    /// Number of rows skipped for the configured offset.
    rows_skipped: usize,
    /// Number of rows emitted to the caller.
    rows_emitted: usize,
    /// Whether this state closed its source.
    source_closed: bool,
}

impl ConfiguredLimitEvidence {
    /// Returns the number of rows requested from upstream.
    #[must_use]
    pub const fn rows_requested(&self) -> usize {
        self.rows_requested
    }

    /// Returns the number of rows skipped for the offset.
    #[must_use]
    pub const fn rows_skipped(&self) -> usize {
        self.rows_skipped
    }

    /// Returns the number of rows emitted to the caller.
    #[must_use]
    pub const fn rows_emitted(&self) -> usize {
        self.rows_emitted
    }

    /// Returns whether the source has been closed.
    #[must_use]
    pub const fn source_closed(&self) -> bool {
        self.source_closed
    }
}

/// Lazy LIMIT-only state that never fetches beyond its requested window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfiguredLimitStream {
    limit: ConfiguredLimitWindow,
    evidence: ConfiguredLimitEvidence,
}

impl ConfiguredLimitStream {
    /// Creates a lazy LIMIT-only window state.
    #[must_use]
    pub const fn new(limit: ConfiguredLimitWindow) -> Self {
        Self {
            limit,
            evidence: ConfiguredLimitEvidence {
                rows_requested: 0,
                rows_skipped: 0,
                rows_emitted: 0,
                source_closed: false,
            },
        }
    }

    /// Requests at most one next emitted row, closing the source at the exact
    /// end of the LIMIT window or on source exhaustion/error.
    pub fn next<S: ConfiguredRowSource>(
        &mut self,
        source: &mut S,
    ) -> Result<Option<Row>, S::Error> {
        if self.evidence.source_closed {
            return Ok(None);
        }
        if self.limit.is_empty() {
            self.close_upstream(source);
            return Ok(None);
        }

        while self.evidence.rows_skipped < self.limit.offset() {
            match self.request(source) {
                Ok(Some(_)) => self.evidence.rows_skipped += 1,
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

        if self.evidence.rows_emitted == self.limit.count() {
            self.close_upstream(source);
            return Ok(None);
        }
        match self.request(source) {
            Ok(Some(row)) => {
                self.evidence.rows_emitted += 1;
                if self.evidence.rows_emitted == self.limit.count() {
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

    /// Returns immutable evidence at the current lazy-stream boundary.
    #[must_use]
    pub const fn evidence(&self) -> ConfiguredLimitEvidence {
        self.evidence
    }

    /// Closes the upstream without requesting another row.
    ///
    /// Connection adapters use this when their result-set lifecycle ends
    /// before the LIMIT window naturally completes. It is deliberately
    /// idempotent so `Close`, `Drop`, and a natural terminal pull share one
    /// upstream release and one truthful `source_closed` evidence bit.
    pub fn close<S: ConfiguredRowSource>(&mut self, source: &mut S) {
        self.close_upstream(source);
    }

    fn request<S: ConfiguredRowSource>(&mut self, source: &mut S) -> Result<Option<Row>, S::Error> {
        self.evidence.rows_requested += 1;
        source.next_row()
    }

    fn close_upstream<S: ConfiguredRowSource>(&mut self, source: &mut S) {
        if !self.evidence.source_closed {
            source.close();
            self.evidence.source_closed = true;
        }
    }
}
