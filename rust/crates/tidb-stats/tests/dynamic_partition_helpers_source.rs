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

//! Source-backed tests for dynamic-partition helper assembly.

use tidb_stats::{flatten_partition_names, get_partition_sql};

#[test]
fn source_partition_sql_preserves_placeholder_and_suffix_shape() {
    assert_eq!(
        get_partition_sql("analyze table %n.%n partition", "", 0),
        "analyze table %n.%n partition"
    );
    assert_eq!(
        get_partition_sql("analyze table %n.%n partition", "", 1),
        "analyze table %n.%n partition %n"
    );
    assert_eq!(
        get_partition_sql("analyze table %n.%n partition", " index %n", 3),
        "analyze table %n.%n partition %n, %n, %n index %n"
    );
}

#[test]
fn source_partition_name_flattening_preserves_group_order() {
    let groups = vec![
        vec![String::from("p0"), String::from("p1")],
        vec![String::from("p2")],
        Vec::new(),
    ];
    assert_eq!(
        flatten_partition_names(&groups),
        vec![String::from("p0"), String::from("p1"), String::from("p2")]
    );
}

#[test]
fn source_empty_partition_groups_flatten_to_empty() {
    assert!(flatten_partition_names(&[]).is_empty());
}
