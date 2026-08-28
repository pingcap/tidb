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

//! Source guards for Go's common INSERT execution path.

#[test]
fn inserts_do_not_use_a_workload_specific_literal_execution_path() {
    let dml = include_str!("../src/driver/dml.rs");
    assert!(
        !dml.contains("fast_literal_shape") && !dml.contains("literal_value_rows"),
        "integer VALUES rows must use the ordinary INSERT row builder"
    );

    let table = include_str!("../src/kv_table.rs");
    assert!(
        !table.contains("if self.indexes.iter().any(|index| !index.clustered_primary)"),
        "the generic row writer must keep one index-maintenance path"
    );
}
