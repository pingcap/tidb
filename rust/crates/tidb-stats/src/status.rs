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

//! Statistics loading/eviction metadata from `pkg/statistics/histogram.go`.
//!
//! This value object keeps the source's distinction between an uninitialized
//! statistics entry, a fully loaded entry, and one whose expensive data has
//! been evicted.  Loading and eviction themselves remain owned by the future
//! statistics-handle/storage integration.

/// Source status meaning that all statistics are resident.
pub const ALL_LOADED: i32 = 0;

/// Source status meaning that all non-essential statistics were evicted.
pub const ALL_EVICTED: i32 = 1;

/// The source `StatsLoadedStatus` metadata value.
///
/// TiDB intentionally stores the eviction status as an integer.  Keeping the
/// integer rather than an exhaustive Rust enum preserves the source behavior
/// for future status values: every value greater than `ALL_LOADED` needs a
/// reload, and every value at least `ALL_EVICTED` is treated as fully evicted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatsLoadedStatus {
    stats_initialized: bool,
    evicted_status: i32,
}

impl StatsLoadedStatus {
    /// Creates a status from the source's two metadata fields.
    #[must_use]
    pub const fn new(stats_initialized: bool, evicted_status: i32) -> Self {
        Self {
            stats_initialized,
            evicted_status,
        }
    }

    /// Returns whether statistics were loaded from storage before.
    #[must_use]
    pub const fn stats_initialized(self) -> bool {
        self.stats_initialized
    }

    /// Returns the source eviction-status integer.
    #[must_use]
    pub const fn evicted_status(self) -> i32 {
        self.evicted_status
    }

    /// Returns a status for a fully loaded column or index.
    #[must_use]
    pub const fn full_load() -> Self {
        Self::new(true, ALL_LOADED)
    }

    /// Returns a status for an initialized entry whose expensive data is
    /// evicted.
    #[must_use]
    pub const fn all_evicted() -> Self {
        Self::new(true, ALL_EVICTED)
    }

    /// Copies the source metadata value.
    #[must_use]
    pub const fn copy(self) -> Self {
        self
    }

    /// Returns whether a storage reload is needed.
    #[must_use]
    pub const fn is_load_needed(self) -> bool {
        self.stats_initialized && self.evicted_status > ALL_LOADED
    }

    /// Returns whether histogram and TopN-level essential statistics remain.
    #[must_use]
    pub const fn is_essential_stats_loaded(self) -> bool {
        self.stats_initialized && self.evicted_status < ALL_EVICTED
    }

    /// Returns whether all statistics are evicted.
    #[must_use]
    pub const fn is_all_evicted(self) -> bool {
        self.stats_initialized && self.evicted_status >= ALL_EVICTED
    }

    /// Returns whether all statistics are fully loaded.
    #[must_use]
    pub const fn is_full_load(self) -> bool {
        self.stats_initialized && self.evicted_status == ALL_LOADED
    }

    /// Returns the source diagnostic label for this status.
    ///
    /// The source intentionally distinguishes an uninitialized value from an
    /// initialized value with an unknown eviction level.
    #[must_use]
    pub const fn status_to_string(self) -> &'static str {
        if !self.stats_initialized {
            return "unInitialized";
        }
        match self.evicted_status {
            ALL_LOADED => "allLoaded",
            ALL_EVICTED => "allEvicted",
            _ => "unknown",
        }
    }
}
