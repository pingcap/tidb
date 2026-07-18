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

//! Source-backed tests for statistics-cache key metadata.

use tidb_stats::StatsKeySet;

#[test]
fn source_key_set_add_get_remove_tracks_caller_cost() {
    let set = StatsKeySet::new();
    assert_eq!(set.len(), 0);
    assert_eq!(set.get(1), None);
    assert_eq!(set.remove(1), 0);

    set.add_key_value(1, 10);
    assert_eq!(set.get(1), Some(10));
    assert_eq!(set.len(), 1);
    assert_eq!(set.remove(1), 10);
    assert_eq!(set.remove(1), 0);
    assert_eq!(set.len(), 0);
}

#[test]
fn source_key_set_replacement_and_clear_match_map_semantics() {
    let set = StatsKeySet::new();
    set.add_key_value(1, 10);
    set.add_key_value(2, 20);
    set.add_key_value(1, 30);
    assert_eq!(set.get(1), Some(30));
    assert_eq!(set.len(), 2);

    let mut keys = set.keys();
    keys.sort_unstable();
    assert_eq!(keys, vec![1, 2]);

    set.clear();
    assert!(set.keys().is_empty());
    assert_eq!(set.len(), 0);
}
