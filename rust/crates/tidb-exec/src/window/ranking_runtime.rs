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

/// Ranking values for one physical row in an already sorted partition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PeerPosition {
    peer_start: usize,
    peer_end: usize,
    dense_rank: usize,
}

impl PeerPosition {
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
    pub(crate) fn len(&self) -> usize {
        self.positions.len()
    }
}
