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

#[test]
fn sort_compares_chunk_cells_without_materializing_per_row_keys() {
    let sort = include_str!("../src/sort.rs");
    let partition = include_str!("../src/sort_partition.rs");

    assert!(
        sort.contains("tidb_chunk::compare::get_column_compare_func"),
        "SortExec must compile Go-style comparators over retained column storage"
    );
    assert!(
        partition.contains("compare_rows"),
        "the partition sort must compare the rows stored in its chunks"
    );
    assert!(
        partition.contains("column_views"),
        "the sort must retain column read views instead of reopening columns for every comparison"
    );
    assert!(
        !partition.contains("keys: Vec<Vec<Datum>>"),
        "Go SortExec stores row handles, not an allocated Datum key per row"
    );
    assert!(
        !partition.contains("let mut indices"),
        "Go sorts its row-handle slice in place instead of permuting a second index vector"
    );
    assert!(
        partition.contains("append_sorted_rows_into"),
        "a single sorted run must stream rows directly without merge-head keys"
    );
}
