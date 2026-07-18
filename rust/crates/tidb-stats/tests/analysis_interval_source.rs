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

//! Source-backed tests for auto-analyze interval metadata.

use tidb_stats::{
    average_analysis_duration_from_seconds, average_duration_query,
    last_failed_analysis_duration_from_seconds, last_failed_duration_query,
    AVG_DURATION_QUERY_FOR_PARTITION, AVG_DURATION_QUERY_FOR_TABLE,
    DEFAULT_FAILED_ANALYSIS_WAIT_NANOS, JUST_FAILED, LAST_FAILED_DURATION_QUERY_FOR_PARTITION,
    LAST_FAILED_DURATION_QUERY_FOR_TABLE, NO_RECORD,
};

#[test]
fn source_average_duration_query_and_rows_match_table_and_partition_paths() {
    assert_eq!(average_duration_query(false), AVG_DURATION_QUERY_FOR_TABLE);
    assert_eq!(
        average_duration_query(true),
        AVG_DURATION_QUERY_FOR_PARTITION
    );
    assert!(AVG_DURATION_QUERY_FOR_TABLE.contains("partition_name = ''"));
    assert!(AVG_DURATION_QUERY_FOR_PARTITION.contains("partition_name in (%?)"));
    assert_eq!(average_analysis_duration_from_seconds(None), NO_RECORD);
    assert_eq!(
        average_analysis_duration_from_seconds(Some(-1.0)),
        NO_RECORD
    );
    assert_eq!(
        average_analysis_duration_from_seconds(Some(3_600.75)),
        3_600 * 1_000_000_000
    );
}

#[test]
fn source_average_duration_negative_record_maps_to_no_record() {
    // The SQL row can contain a negative clock-skew duration; interval.go
    // deliberately maps it to the same sentinel as an absent row.
    assert_eq!(
        average_analysis_duration_from_seconds(Some(-0.001)),
        NO_RECORD
    );
}

#[test]
fn source_last_failed_duration_query_and_rows_match_all_states() {
    assert_eq!(
        last_failed_duration_query(false),
        LAST_FAILED_DURATION_QUERY_FOR_TABLE
    );
    assert_eq!(
        last_failed_duration_query(true),
        LAST_FAILED_DURATION_QUERY_FOR_PARTITION
    );
    assert!(LAST_FAILED_DURATION_QUERY_FOR_TABLE.contains("state = 'failed'"));
    assert!(LAST_FAILED_DURATION_QUERY_FOR_PARTITION.contains("GROUP BY"));
    assert_eq!(last_failed_analysis_duration_from_seconds(None), NO_RECORD);
    assert_eq!(
        last_failed_analysis_duration_from_seconds(Some(0)),
        JUST_FAILED
    );
    assert_eq!(
        last_failed_analysis_duration_from_seconds(Some(24 * 60 * 60)),
        24 * 60 * 60 * 1_000_000_000
    );
}

#[test]
fn source_last_failed_negative_record_uses_bounded_retry_wait() {
    assert_eq!(
        last_failed_analysis_duration_from_seconds(Some(-1)),
        DEFAULT_FAILED_ANALYSIS_WAIT_NANOS
    );
    assert_eq!(DEFAULT_FAILED_ANALYSIS_WAIT_NANOS, 30 * 60 * 1_000_000_000);
}
