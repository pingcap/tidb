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

//! Source-backed tests for privilege-set helpers.

use tidb_exec::privilege_set::{add_to_set, delete_from_set, set_from_string, set_to_string};

#[test]
fn privilege_set_helpers_preserve_source_order_and_empty_boundary() {
    // Source: pkg/executor/utils.go:38-66 and
    // pkg/executor/revoke_test.go:97-137 (TestRevokeTableScope).
    assert_eq!(set_from_string(""), None);
    assert_eq!(
        set_from_string("Select,Insert,Update,,Delete"),
        Some(vec![
            "Select".to_owned(),
            "Insert".to_owned(),
            "Update".to_owned(),
            "".to_owned(),
            "Delete".to_owned(),
        ])
    );

    let mut privileges =
        set_from_string("Select,Insert,Update").expect("non-empty privilege set should be present");
    assert_eq!(set_to_string(&privileges), "Select,Insert,Update");
    privileges = add_to_set(privileges, "Update");
    assert_eq!(set_to_string(&privileges), "Select,Insert,Update");
    privileges = add_to_set(privileges, "Delete");
    assert_eq!(set_to_string(&privileges), "Select,Insert,Update,Delete");

    privileges = delete_from_set(privileges, "Insert");
    assert_eq!(set_to_string(&privileges), "Select,Update,Delete");
    privileges = delete_from_set(privileges, "Missing");
    assert_eq!(set_to_string(&privileges), "Select,Update,Delete");
    privileges = delete_from_set(
        vec![
            "Select".to_owned(),
            "Select".to_owned(),
            "Update".to_owned(),
        ],
        "Select",
    );
    assert_eq!(privileges, vec!["Select".to_owned(), "Update".to_owned()]);
}
