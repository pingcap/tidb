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

//! Source-backed tests for refresher rebuild state.

use tidb_stats::should_rebuild_queue;

#[test]
fn source_prune_mode_change_rebuilds_initialized_queue() {
    assert!(should_rebuild_queue(true, 0.5, 0.5, 1, 2));
}

#[test]
fn source_ratio_change_rebuilds_initialized_queue() {
    assert!(should_rebuild_queue(true, 0.6, 0.5, 1, 1));
}

#[test]
fn source_unchanged_or_uninitialized_queue_does_not_rebuild() {
    assert!(!should_rebuild_queue(true, 0.5, 0.5, 1, 1));
    assert!(!should_rebuild_queue(false, 0.6, 0.5, 1, 2));
    // Go's != also treats NaN values as changed.
    assert!(should_rebuild_queue(true, f64::NAN, f64::NAN, 1, 1));
}
