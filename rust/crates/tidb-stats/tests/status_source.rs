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

//! Source-backed tests for statistics loading/eviction metadata.

use tidb_stats::{StatsLoadedStatus, ALL_EVICTED, ALL_LOADED};

#[test]
fn source_zero_value_is_uninitialized_and_does_not_reload() {
    let status = StatsLoadedStatus::default();
    assert!(!status.stats_initialized());
    assert_eq!(status.evicted_status(), ALL_LOADED);
    assert!(!status.is_load_needed());
    assert!(!status.is_essential_stats_loaded());
    assert!(!status.is_all_evicted());
    assert!(!status.is_full_load());
}

#[test]
fn source_full_load_status_is_initialized_without_reload() {
    let status = StatsLoadedStatus::full_load();
    assert!(status.stats_initialized());
    assert_eq!(status.evicted_status(), ALL_LOADED);
    assert!(!status.is_load_needed());
    assert!(status.is_essential_stats_loaded());
    assert!(!status.is_all_evicted());
    assert!(status.is_full_load());
}

#[test]
fn source_all_evicted_status_requires_reload_and_loses_essential_stats() {
    let status = StatsLoadedStatus::all_evicted();
    assert!(status.stats_initialized());
    assert_eq!(status.evicted_status(), ALL_EVICTED);
    assert!(status.is_load_needed());
    assert!(!status.is_essential_stats_loaded());
    assert!(status.is_all_evicted());
    assert!(!status.is_full_load());
}

#[test]
fn source_future_eviction_levels_keep_integer_ordering_semantics() {
    let status = StatsLoadedStatus::new(true, ALL_EVICTED + 1);
    assert!(status.is_load_needed());
    assert!(!status.is_essential_stats_loaded());
    assert!(status.is_all_evicted());
    assert!(!status.is_full_load());
}

#[test]
fn source_integer_status_preserves_lower_than_loaded_ordering() {
    let status = StatsLoadedStatus::new(true, ALL_LOADED - 1);
    assert!(!status.is_load_needed());
    assert!(status.is_essential_stats_loaded());
    assert!(!status.is_all_evicted());
    assert!(!status.is_full_load());
}

#[test]
fn source_copy_is_value_independent() {
    let status = StatsLoadedStatus::all_evicted();
    let copy = status.copy();
    assert_eq!(copy, status);
    assert_eq!(copy.evicted_status(), ALL_EVICTED);
}

#[test]
fn source_status_to_string_preserves_diagnostic_labels() {
    assert_eq!(
        StatsLoadedStatus::default().status_to_string(),
        "unInitialized"
    );
    assert_eq!(
        StatsLoadedStatus::full_load().status_to_string(),
        "allLoaded"
    );
    assert_eq!(
        StatsLoadedStatus::all_evicted().status_to_string(),
        "allEvicted"
    );
    assert_eq!(
        StatsLoadedStatus::new(true, ALL_EVICTED + 1).status_to_string(),
        "unknown"
    );
    assert_eq!(
        StatsLoadedStatus::new(true, ALL_LOADED - 1).status_to_string(),
        "unknown"
    );
    assert_eq!(
        StatsLoadedStatus::new(false, ALL_EVICTED + 1).status_to_string(),
        "unInitialized"
    );
}
