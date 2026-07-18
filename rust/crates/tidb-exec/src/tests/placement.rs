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

//! Placement-policy execution boundary tests.

use super::*;

#[test]
fn placement_policy_ddl_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    assert_eq!(step(&mut db, "create table pp_boundary (id int)"), "OK");
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(step(&mut db, "insert into pp_boundary values (1)"), "OK");

    for (sql, expected) in [
        (
            "create placement policy pp followers=3",
            "Unsupported(\"CREATE PLACEMENT POLICY\")",
        ),
        (
            "alter placement policy pp followers=3",
            "Unsupported(\"ALTER PLACEMENT POLICY\")",
        ),
        (
            "drop placement policy if exists pp",
            "Unsupported(\"DROP PLACEMENT POLICY\")",
        ),
    ] {
        assert_eq!(step(&mut db, sql), expected, "source SQL: {sql}");
    }

    // Cluster-wide placement metadata is not present in the seed executor.
    // All three rejections must leave the active transaction untouched.
    assert_eq!(step(&mut db, "rollback"), "OK");
    assert_eq!(step(&mut db, "select id from pp_boundary"), "RS:");
}
