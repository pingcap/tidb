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

//! Source-backed tests for auto-analyze priority arithmetic.

use tidb_stats::{calculate_priority_weight, special_event_weight, EVENT_NEW_INDEX, EVENT_NONE};

#[test]
fn source_priority_weight_increases_with_change_percentage() {
    let values = [0.6, 1.0, 10.0];
    let mut previous = f64::NEG_INFINITY;
    for change_percentage in values {
        let weight = calculate_priority_weight(change_percentage, 1_000.0, 3_600.0, false);
        assert!(weight > 0.0);
        assert!(weight > previous);
        previous = weight;
    }
}

#[test]
fn source_priority_weight_decreases_with_table_size() {
    let values = [100_000.0, 10_000.0, 1_000.0];
    let mut previous = f64::NEG_INFINITY;
    for table_size in values {
        let weight = calculate_priority_weight(0.6, table_size, 3_600.0, false);
        assert!(weight > previous);
        previous = weight;
    }
}

#[test]
fn source_priority_weight_increases_with_analysis_interval() {
    let values = [3_600.0, 43_200.0, 86_400.0];
    let mut previous = f64::NEG_INFINITY;
    for duration_seconds in values {
        let weight = calculate_priority_weight(0.6, 1_000.0, duration_seconds, false);
        assert!(weight > previous);
        previous = weight;
    }
}

#[test]
fn source_recent_analysis_does_not_outrank_large_change() {
    let older = calculate_priority_weight(0.5, 1_000.0, 7_200.0, false);
    let recent = calculate_priority_weight(1.0, 1_000.0, 600.0, false);
    assert!(recent > older);
}

#[test]
fn source_special_event_weights_match_go() {
    assert_eq!(special_event_weight(true), EVENT_NEW_INDEX);
    assert_eq!(special_event_weight(false), EVENT_NONE);
}

#[test]
fn source_invalid_domains_remain_ieee_values() {
    assert!(calculate_priority_weight(-0.02, 1_000.0, 3_600.0, false).is_nan());
    assert!(calculate_priority_weight(0.6, -2.0, 3_600.0, false).is_nan());
    assert!(calculate_priority_weight(0.6, 1_000.0, -1.0, false).is_nan());
}
