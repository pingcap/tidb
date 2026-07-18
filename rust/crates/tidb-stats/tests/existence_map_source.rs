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

//! Source-backed tests for column/index statistics existence metadata.

use tidb_stats::ColAndIdxExistenceMap;

#[test]
fn source_map_distinguishes_known_and_analyzed_entries() {
    let mut map = ColAndIdxExistenceMap::new(2, 1);
    assert!(map.is_empty());
    assert_eq!(map.column_count(), 0);

    map.insert_column(1, true);
    map.insert_column(2, false);
    map.insert_index(7, true);

    assert!(!map.is_empty());
    assert_eq!(map.column_count(), 2);
    assert!(map.has(1, false));
    assert!(map.has(2, false));
    assert!(!map.has(3, false));
    assert!(map.has_analyzed(1, false));
    assert!(!map.has_analyzed(2, false));
    assert!(!map.has_analyzed(3, false));
    assert!(map.has(7, true));
    assert!(map.has_analyzed(7, true));
    assert!(!map.has(7, false));
}

#[test]
fn source_map_replacement_and_deletion_preserve_presence_semantics() {
    let mut map = ColAndIdxExistenceMap::new_without_size();
    map.insert_column(1, false);
    map.insert_column(1, true);
    assert!(map.has_analyzed(1, false));

    map.delete_column_not_found(1);
    assert!(!map.has(1, false));
    assert!(!map.has_analyzed(1, false));

    map.insert_index(9, false);
    map.delete_index_not_found(9);
    assert!(!map.has(9, true));
    map.delete_index_not_found(100);
    assert!(map.is_empty());
}

#[test]
fn source_map_clone_and_equality_are_deep() {
    let mut original = ColAndIdxExistenceMap::new(1, 1);
    original.insert_column(2, true);
    original.insert_index(8, false);
    let mut clone = original.deep_clone();
    assert!(original.is_equal(&clone));

    clone.insert_column(3, false);
    clone.insert_index(8, true);
    assert!(!original.is_equal(&clone));
    assert!(!original.has(3, false));
    assert!(!original.has_analyzed(8, true));
    assert!(clone.has_analyzed(8, true));
}
