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

//! Execution boundaries for the source-owned SHOW MASTER STATUS/PRIVILEGES
//! leaves.

use super::*;

#[test]
fn show_master_status_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_master_status_boundary (id int)");
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into show_master_status_boundary values (1)",
    );

    assert_eq!(
        step(&mut db, "show master status"),
        "Unsupported(\"SHOW MASTER STATUS\")"
    );

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_master_status_boundary"),
        "RS:"
    );
}

#[test]
fn show_privileges_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table show_privileges_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into show_privileges_boundary values (1)");

    assert_eq!(
        step(&mut db, "show privileges"),
        "Unsupported(\"SHOW PRIVILEGES\")"
    );

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from show_privileges_boundary"),
        "RS:"
    );
}
