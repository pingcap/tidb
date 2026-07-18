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

//! `CUME_DIST` peer-rank state from `pkg/executor/aggfuncs/func_cume_dist.go`.
//!
//! This compatibility facade keeps the isolated integer-key API while routing
//! peer boundaries through the same canonical geometry consumed by the live
//! executor. Typed comparison and scheduling are owned by `window`.

use std::mem::size_of;

use crate::window::ranking_runtime::PeerGeometry;

/// The source-shaped partial state for cumulative-distribution ranking.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CumeDistState {
    current_index: usize,
    geometry: PeerGeometry,
}

impl CumeDistState {
    /// Creates state over source-sorted peer keys.
    #[must_use]
    pub fn new(sorted_keys: &[i64]) -> Self {
        Self {
            current_index: 0,
            geometry: PeerGeometry::from_sorted_by(sorted_keys.len(), |prev, curr| {
                sorted_keys[prev] == sorted_keys[curr]
            }),
        }
    }
}

impl Iterator for CumeDistState {
    type Item = f64;

    /// Emits the next `CUME_DIST` value in source row order.
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.geometry.len() {
            return None;
        }
        let value = self
            .geometry
            .position(self.current_index)
            .cume_dist(self.geometry.len());
        self.current_index += 1;
        Some(value)
    }
}

impl CumeDistState {
    /// Returns the source partial-state allocation size for this leaf.
    #[must_use]
    pub const fn partial_state_size() -> usize {
        size_of::<Self>()
    }
}

/// Computes all cumulative-distribution values for sorted peer keys.
#[must_use]
pub fn cumulative_distribution(sorted_keys: &[i64]) -> Vec<f64> {
    let mut values = Vec::with_capacity(sorted_keys.len());
    for value in CumeDistState::new(sorted_keys) {
        values.push(value);
    }
    values
}
