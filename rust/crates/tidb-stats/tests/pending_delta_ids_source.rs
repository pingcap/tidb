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

//! Source-backed tests for pending statistics-delta table-ID selection.

use tidb_stats::collect_pending_delta_ids;

#[test]
fn collect_pending_ids_selects_all_when_targets_are_empty() {
    assert_eq!(
        collect_pending_delta_ids(&[42, 3, 17], &[]),
        vec![3, 17, 42]
    );
}

#[test]
fn collect_pending_ids_filters_missing_targets_and_deduplicates() {
    assert_eq!(
        collect_pending_delta_ids(&[42, 3, 17], &[17, 99, 17, 3]),
        vec![3, 17]
    );
}

#[test]
fn collect_pending_ids_returns_empty_for_no_matches() {
    assert!(collect_pending_delta_ids(&[1, 2], &[3, 4, 3]).is_empty());
}
