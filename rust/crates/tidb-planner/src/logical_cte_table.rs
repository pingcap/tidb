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

//! LogicalCTETable's dependency-closed DeriveStats state transition from
//! `pkg/planner/core/operator/logicalop/logical_cte_table.go`.
//!
//! The Go operator stores concrete statistics, schemas, expressions, and
//! planner context. This leaf keeps the source reload-list interpretation,
//! existing-versus-seed selection, and changed flag over opaque caller-owned
//! statistics identities; real statistics derivation and plan propagation
//! remain outside this boundary.

/// Opaque caller-owned statistics identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StatsIdentity(u64);

impl StatsIdentity {
    /// Creates a statistics identity token.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the token value for assertions and adapters.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Minimal LogicalCTETable statistics state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LogicalCteTableStats {
    stats: Option<StatsIdentity>,
    seed_stat: Option<StatsIdentity>,
}

impl LogicalCteTableStats {
    /// Creates a CTE-table stats state from current and seed identities.
    #[must_use]
    pub const fn new(stats: Option<StatsIdentity>, seed_stat: Option<StatsIdentity>) -> Self {
        Self { stats, seed_stat }
    }

    /// Returns the currently attached statistics identity, if any.
    #[must_use]
    pub const fn stats(self) -> Option<StatsIdentity> {
        self.stats
    }

    /// Returns the seed statistics identity, if any.
    #[must_use]
    pub const fn seed_stat(self) -> Option<StatsIdentity> {
        self.seed_stat
    }

    /// Applies the source `LogicalCTETable.DeriveStats` transition.
    ///
    /// The Go implementation treats a reload vector as active only when it
    /// contains exactly one element. A false/absent reload retains existing
    /// stats without reporting a change; every other path installs SeedStat
    /// and reports `reload=true`, including a nil seed.
    pub fn derive_stats(&mut self, reloads: &[bool]) -> (Option<StatsIdentity>, bool) {
        let reload = match reloads {
            [reload] => *reload,
            _ => false,
        };
        if !reload {
            if let Some(stats) = self.stats {
                return (Some(stats), false);
            }
        }
        self.stats = self.seed_stat;
        (self.stats, true)
    }
}

#[cfg(test)]
mod tests {
    use super::{LogicalCteTableStats, StatsIdentity};

    #[test]
    fn false_single_reload_retains_existing_stats() {
        let existing = StatsIdentity::new(1);
        let seed = StatsIdentity::new(2);
        let mut state = LogicalCteTableStats::new(Some(existing), Some(seed));

        let (stats, changed) = state.derive_stats(&[false]);
        assert_eq!(stats, Some(existing));
        assert!(!changed);
        assert_eq!(state.stats(), Some(existing));
    }

    #[test]
    fn true_single_reload_replaces_with_seed_stats() {
        let mut state =
            LogicalCteTableStats::new(Some(StatsIdentity::new(1)), Some(StatsIdentity::new(2)));

        let (stats, changed) = state.derive_stats(&[true]);
        assert_eq!(stats.map(StatsIdentity::value), Some(2));
        assert!(changed);
    }

    #[test]
    fn non_single_reload_vectors_are_false_reload() {
        let existing = StatsIdentity::new(3);
        let mut state = LogicalCteTableStats::new(Some(existing), Some(StatsIdentity::new(4)));

        let (empty_stats, empty_changed) = state.derive_stats(&[]);
        assert_eq!(empty_stats, Some(existing));
        assert!(!empty_changed);

        let (many_stats, many_changed) = state.derive_stats(&[true, false]);
        assert_eq!(many_stats, Some(existing));
        assert!(!many_changed);
    }

    #[test]
    fn missing_existing_stats_installs_even_a_nil_seed() {
        let mut state = LogicalCteTableStats::new(None, None);
        let (stats, changed) = state.derive_stats(&[false]);
        assert_eq!(stats, None);
        assert!(changed);
    }
}
