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

//! Canonical physical-partition runtime for ranking, distribution, and
//! row-offset window functions.
//!
//! TiDB's Go implementations keep separate cursors for `RANK`,
//! `DENSE_RANK`, `PERCENT_RANK`, and `CUME_DIST`, but all four obtain their
//! peer boundaries from the same `rowComparer`.  The live Rust executor owns
//! the sort, so it materializes those boundaries once and derives every
//! peer-aware result from this immutable geometry. The same runtime owns the
//! stable-sorted physical row handles and resettable cursor used by
//! `ROW_NUMBER`, `NTILE`, `LAG`, and `LEAD`, so the live executor has one
//! partition authority rather than coordinating independent leaf cursors.

use crate::lead_lag::{LeadLagDefault, LeadLagDirection, LeadLagSelection};

/// Ranking values for one physical row in an already sorted partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PeerPosition {
    peer_start: usize,
    peer_end: usize,
    dense_rank: usize,
}

impl PeerPosition {
    #[must_use]
    pub(crate) const fn rank(self) -> usize {
        self.peer_start + 1
    }

    #[must_use]
    pub(crate) const fn dense_rank(self) -> usize {
        self.dense_rank
    }

    /// `(rank - 1) / (partition_len - 1)`, with the source single-row result.
    #[must_use]
    pub(crate) fn percent_rank(self, partition_len: usize) -> f64 {
        if partition_len <= 1 {
            0.0
        } else {
            self.peer_start as f64 / (partition_len - 1) as f64
        }
    }

    /// The number of rows at or before this row's peer group divided by the
    /// partition length.
    #[must_use]
    pub(crate) fn cume_dist(self, partition_len: usize) -> f64 {
        self.peer_end as f64 / partition_len as f64
    }
}

/// Peer boundaries and derived ranks for one already sorted partition.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PeerGeometry {
    positions: Vec<PeerPosition>,
}

impl PeerGeometry {
    /// Builds geometry using the exact equality relation used by the caller's
    /// stable sort. `are_peers` is asked only about adjacent positions.
    #[must_use]
    pub(crate) fn from_sorted_by(
        partition_len: usize,
        mut are_peers: impl FnMut(usize, usize) -> bool,
    ) -> Self {
        let mut positions = Vec::with_capacity(partition_len);
        let mut peer_start = 0;
        let mut dense_rank = 1;
        while peer_start < partition_len {
            let mut peer_end = peer_start + 1;
            while peer_end < partition_len && are_peers(peer_end - 1, peer_end) {
                peer_end += 1;
            }
            for _ in peer_start..peer_end {
                positions.push(PeerPosition {
                    peer_start,
                    peer_end,
                    dense_rank,
                });
            }
            peer_start = peer_end;
            dense_rank += 1;
        }
        Self { positions }
    }

    #[must_use]
    pub(crate) fn position(&self, physical_index: usize) -> PeerPosition {
        self.positions[physical_index]
    }

    #[must_use]
    pub(crate) const fn row_number(physical_index: usize) -> usize {
        physical_index + 1
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.positions.len()
    }
}

/// One already stable-sorted window partition.
///
/// `rows`, `geometry`, and `cursor` are deliberately co-owned: rebuilding or
/// advancing any one without the others would create a second source of truth
/// for physical positions. A fresh value is constructed at each partition
/// boundary; consumers may reset only the cursor when changing output shape.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WindowPartitionRuntime {
    rows: Vec<usize>,
    geometry: PeerGeometry,
    cursor: u64,
}

impl WindowPartitionRuntime {
    /// Buffers stable-sorted opaque row handles and materializes peer geometry
    /// using the exact adjacent equality relation supplied by the sort owner.
    #[must_use]
    pub(crate) fn from_sorted_rows(
        rows: impl IntoIterator<Item = usize>,
        mut are_peers: impl FnMut(usize, usize) -> bool,
    ) -> Self {
        let rows: Vec<usize> = rows.into_iter().collect();
        let geometry = PeerGeometry::from_sorted_by(rows.len(), &mut are_peers);
        Self {
            rows,
            geometry,
            cursor: 0,
        }
    }

    /// Resets physical output traversal without rebuilding the partition.
    pub(crate) fn reset_cursor(&mut self) {
        self.cursor = 0;
    }

    /// Advances the canonical physical cursor and returns `(position, row)`.
    #[must_use]
    pub(crate) fn next_physical(&mut self) -> Option<(usize, usize)> {
        let position = usize::try_from(self.cursor).ok()?;
        let row = self.rows.get(position).copied()?;
        self.cursor = self.cursor.wrapping_add(1);
        Some((position, row))
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub(crate) fn peer_position(&self, physical_index: usize) -> PeerPosition {
        self.geometry.position(physical_index)
    }

    #[must_use]
    pub(crate) const fn row_number(physical_index: usize) -> usize {
        PeerGeometry::row_number(physical_index)
    }

    /// Returns the source NTILE bucket at one physical position. This is the
    /// quotient/remainder transition written as a direct partition formula;
    /// it preserves NULL for zero and gives the first remainder buckets one
    /// extra row.
    #[must_use]
    pub(crate) fn ntile_bucket(&self, physical_index: usize, divisor: u64) -> Option<u64> {
        if divisor == 0 {
            return None;
        }
        let row_count = u64::try_from(self.len()).unwrap_or(u64::MAX);
        let index = u64::try_from(physical_index).unwrap_or(u64::MAX);
        let quotient = row_count / divisor;
        let remainder = row_count % divisor;
        let large_bucket_span = quotient.saturating_add(1).saturating_mul(remainder);
        if index < large_bucket_span {
            Some(index / quotient.saturating_add(1) + 1)
        } else {
            // `quotient == 0` implies every materialized row is in the first
            // branch because then `remainder == row_count`.
            Some(remainder + (index - large_bucket_span) / quotient + 1)
        }
    }

    /// Selects the source/default row for the next physical partition row.
    /// LEAD intentionally performs wrapping u64 addition before bounds
    /// checking, matching the Go cursor for MAX and MAX-1 offsets.
    #[must_use]
    pub(crate) fn next_lead_lag_selection(
        &mut self,
        direction: LeadLagDirection,
        offset: u64,
        default: LeadLagDefault,
    ) -> Option<(usize, LeadLagSelection)> {
        let current = usize::try_from(self.cursor).ok()?;
        if current >= self.rows.len() {
            return None;
        }
        let target = match direction {
            LeadLagDirection::Lag => usize::try_from(offset)
                .ok()
                .and_then(|offset| current.checked_sub(offset)),
            LeadLagDirection::Lead => self
                .cursor
                .wrapping_add(offset)
                .try_into()
                .ok()
                .filter(|&index| index < self.rows.len()),
        };
        let selection = match target {
            Some(index) => LeadLagSelection::Source(self.rows[index]),
            None => match default {
                LeadLagDefault::Null => LeadLagSelection::Null,
                LeadLagDefault::CurrentRow => LeadLagSelection::Default(self.rows[current]),
            },
        };
        let output_row = self.rows[current];
        self.cursor = self.cursor.wrapping_add(1);
        Some((output_row, selection))
    }
}
