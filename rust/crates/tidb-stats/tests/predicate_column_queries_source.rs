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

//! Source-backed tests for predicate-column usage query shapes.

use tidb_stats::{
    cleanup_column_ids_argument, CLEANUP_DROPPED_COLUMN_STATS_USAGE_QUERY,
    GET_PREDICATE_COLUMNS_QUERY, LOAD_COLUMN_STATS_USAGE_FOR_TABLE_QUERY,
    LOAD_COLUMN_STATS_USAGE_QUERY,
};

#[test]
fn source_predicate_column_queries_match_go() {
    assert_eq!(
        LOAD_COLUMN_STATS_USAGE_QUERY,
        "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage"
    );
    assert_eq!(
        LOAD_COLUMN_STATS_USAGE_FOR_TABLE_QUERY,
        "SELECT table_id, column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00'), CONVERT_TZ(last_analyzed_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %?"
    );
    assert_eq!(
        GET_PREDICATE_COLUMNS_QUERY,
        "SELECT column_id, CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00') FROM mysql.column_stats_usage WHERE table_id = %? AND last_used_at IS NOT NULL"
    );
    assert_eq!(
        CLEANUP_DROPPED_COLUMN_STATS_USAGE_QUERY,
        "DELETE FROM mysql.column_stats_usage WHERE table_id = %? AND column_id NOT IN (%?)"
    );
}

#[test]
fn source_cleanup_column_ids_preserves_schema_order() {
    assert_eq!(cleanup_column_ids_argument(&[]), "");
    assert_eq!(cleanup_column_ids_argument(&[4, 2, 9]), "4,2,9");
    assert_eq!(cleanup_column_ids_argument(&[-1, 0, -1]), "-1,0,-1");
}
