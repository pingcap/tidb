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

//! Source-backed tests for auto-analyze job metadata.

use tidb_stats::{
    as_json_indicators, is_dynamic_partitioned_table_analysis_job, AnalysisIndicators,
    AnalysisJobKind,
};

#[test]
fn source_stringer_indicators_match_json_shape() {
    let indicators = as_json_indicators(AnalysisIndicators::default());
    assert_eq!(indicators.change_percentage, "0.00%");
    assert_eq!(indicators.table_size, "0.00");
    assert_eq!(indicators.last_analysis_duration, "0s");

    let indicators = as_json_indicators(AnalysisIndicators {
        change_percentage: 0.5,
        table_size: 12.345,
        last_analysis_duration_nanos: 3_600_500_000_000,
    });
    assert_eq!(indicators.change_percentage, "50.00%");
    assert_eq!(indicators.table_size, "12.35");
    assert_eq!(indicators.last_analysis_duration, "1h0m0.5s");

    assert_eq!(
        as_json_indicators(AnalysisIndicators {
            last_analysis_duration_nanos: 60 * 1_000_000_000,
            ..AnalysisIndicators::default()
        })
        .last_analysis_duration,
        "1m0s"
    );
    assert_eq!(
        as_json_indicators(AnalysisIndicators {
            last_analysis_duration_nanos: 1_500_000,
            ..AnalysisIndicators::default()
        })
        .last_analysis_duration,
        "1.5ms"
    );
}

#[test]
fn source_dynamic_job_predicate_matches_concrete_kind() {
    assert!(is_dynamic_partitioned_table_analysis_job(
        AnalysisJobKind::DynamicPartitioned
    ));
    assert!(!is_dynamic_partitioned_table_analysis_job(
        AnalysisJobKind::NonPartitioned
    ));
    assert!(!is_dynamic_partitioned_table_analysis_job(
        AnalysisJobKind::StaticPartitioned
    ));
    assert!(!is_dynamic_partitioned_table_analysis_job(
        AnalysisJobKind::Other
    ));
}
