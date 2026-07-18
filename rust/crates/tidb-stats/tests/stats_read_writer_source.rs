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

//! Source-backed tests for stats-read/writer scalar decisions.

use tidb_stats::{
    historical_stats_meta_record_required, slow_stats_saving_requires_meta_update, LEASE_OFFSET,
    SLOW_STATS_SAVE_ERROR_MESSAGE,
};

#[test]
fn stats_meta_history_requires_success_and_nonzero_version() {
    assert!(historical_stats_meta_record_required(true, 1));
    assert!(!historical_stats_meta_record_required(true, 0));
    assert!(!historical_stats_meta_record_required(false, 1));
    assert!(!historical_stats_meta_record_required(false, 0));
}

#[test]
fn slow_stats_saving_uses_positive_five_lease_threshold() {
    assert_eq!(LEASE_OFFSET, 5);
    assert!(!slow_stats_saving_requires_meta_update(10, 49, false));
    assert!(slow_stats_saving_requires_meta_update(10, 50, false));
    assert!(slow_stats_saving_requires_meta_update(10, 51, false));
}

#[test]
fn slow_stats_saving_disables_nonpositive_leases_but_force_overrides() {
    assert!(!slow_stats_saving_requires_meta_update(0, i64::MAX, false));
    assert!(!slow_stats_saving_requires_meta_update(-1, i64::MAX, false));
    assert!(slow_stats_saving_requires_meta_update(0, 0, true));
    assert!(slow_stats_saving_requires_meta_update(-1, 0, true));
}

#[test]
fn slow_stats_saving_preserves_source_error_text() {
    assert_eq!(
        SLOW_STATS_SAVE_ERROR_MESSAGE,
        "failed to update stats meta version during analyze result save. The system may be too busy. Please retry the operation later"
    );
}
