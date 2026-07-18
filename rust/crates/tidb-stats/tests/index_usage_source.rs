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

//! Source-backed tests for index-usage sample metadata.

use tidb_stats::{index_usage_access_bucket, new_index_usage_sample, INDEX_USAGE_BUCKET_COUNT};

#[test]
fn source_bucket_boundaries_match_go_table() {
    let cases = [
        (0.0, 0),
        (0.005, 1),
        (0.01, 2),
        (0.05, 2),
        (0.1, 3),
        (0.15, 3),
        (0.2, 4),
        (0.4, 4),
        (0.5, 5),
        (0.7, 5),
        (1.0, 6),
    ];
    for (value, expected) in cases {
        assert_eq!(index_usage_access_bucket(value), expected);
    }
}

#[test]
fn source_out_of_range_percentages_keep_zero_bucket() {
    assert_eq!(index_usage_access_bucket(-0.1), 0);
    assert_eq!(index_usage_access_bucket(1.1), 0);
    assert_eq!(index_usage_access_bucket(f64::NAN), 0);
}

#[test]
fn source_new_sample_records_full_and_partial_scans() {
    let full = new_index_usage_sample(1, 1, 1, 1);
    assert_eq!(full.query_total, 1);
    assert_eq!(full.kv_req_total, 1);
    assert_eq!(full.row_access_total, 1);
    assert_eq!(full.percentage_access, [0, 0, 0, 0, 0, 0, 1]);

    let partial = new_index_usage_sample(10, 10, 5, 50);
    assert_eq!(partial.percentage_access, [0, 0, 0, 1, 0, 0, 0]);
}

#[test]
fn source_sample_merge_matches_update_by_key() {
    let mut usage = new_index_usage_sample(1, 1, 1, 1);
    usage.merge(&new_index_usage_sample(10, 10, 5, 50));
    assert_eq!(usage.query_total, 11);
    assert_eq!(usage.kv_req_total, 11);
    assert_eq!(usage.row_access_total, 6);
    assert_eq!(usage.percentage_access, [0, 0, 0, 1, 0, 0, 1]);

    usage.merge(&new_index_usage_sample(10, 10, 5, 0));
    assert_eq!(usage.query_total, 21);
    assert_eq!(usage.kv_req_total, 21);
    assert_eq!(usage.row_access_total, 11);
    assert_eq!(usage.percentage_access, [0, 0, 0, 1, 0, 0, 2]);
}

#[test]
fn source_zero_total_rows_use_last_bucket() {
    let sample = new_index_usage_sample(10, 10, 5, 0);
    let mut expected = [0; INDEX_USAGE_BUCKET_COUNT];
    expected[INDEX_USAGE_BUCKET_COUNT - 1] = 1;
    assert_eq!(sample.percentage_access, expected);
}
