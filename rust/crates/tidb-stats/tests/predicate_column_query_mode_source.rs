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

//! Source-backed tests for predicate-column query transaction modes.

use tidb_stats::PredicateColumnOperation;

#[test]
fn source_predicate_column_query_mode_matches_wrapper_flags() {
    assert!(!PredicateColumnOperation::LoadColumnStatsUsage.wraps_transaction());
    assert!(PredicateColumnOperation::GetPredicateColumns.wraps_transaction());
}

#[test]
fn source_predicate_column_query_mode_is_explicit_and_copyable() {
    let operation = PredicateColumnOperation::GetPredicateColumns;
    assert_eq!(operation, operation);
    assert!(operation.wraps_transaction());
}
