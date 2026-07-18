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

//! Source-backed tests for index-usage lookup identities.

use tidb_stats::IndexUsageKey;

#[test]
fn source_index_usage_key_preserves_table_and_index_ids() {
    let key = IndexUsageKey::new(10, 4);
    assert_eq!(key.table_id, 10);
    assert_eq!(key.index_id, 4);
}

#[test]
fn source_index_usage_key_distinguishes_each_lookup_pair() {
    assert_ne!(IndexUsageKey::new(10, 4), IndexUsageKey::new(10, 5));
    assert_ne!(IndexUsageKey::new(10, 4), IndexUsageKey::new(11, 4));
}
