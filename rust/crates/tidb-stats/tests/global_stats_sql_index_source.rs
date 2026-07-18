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

//! Source-backed tests for the global-statistics SQL index conversion.

use tidb_stats::to_sql_index;

#[test]
fn source_to_sql_index_matches_global_stats_rows() {
    // TestGlobalStatsData filters mysql.stats_histograms with both values of
    // is_index, so the scalar conversion must preserve each SQL dimension.
    assert_eq!(to_sql_index(false), 0);
    assert_eq!(to_sql_index(true), 1);
}

#[test]
fn source_to_sql_index_is_stable_for_repeated_queries() {
    assert_eq!(
        [false, true, true, false, true]
            .into_iter()
            .map(to_sql_index)
            .collect::<Vec<_>>(),
        vec![0, 1, 1, 0, 1]
    );
}
