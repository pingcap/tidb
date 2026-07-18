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

use super::*;

#[test]
fn drop_database_is_unsupported_before_transaction_mutation() {
    let mut db = Database::new();
    assert_eq!(
        step(&mut db, "create table drop_database_boundary (id int)"),
        "OK"
    );
    assert_eq!(step(&mut db, "begin"), "OK");
    assert_eq!(
        step(&mut db, "insert into drop_database_boundary values (1)"),
        "OK"
    );

    assert_eq!(
        step(&mut db, "drop database if exists plan_cache"),
        "Unsupported(\"DROP DATABASE\")"
    );
    assert!(db.transaction.is_active());

    assert_eq!(step(&mut db, "rollback"), "OK");
    assert_eq!(
        step(&mut db, "select id from drop_database_boundary"),
        "RS:"
    );
}
