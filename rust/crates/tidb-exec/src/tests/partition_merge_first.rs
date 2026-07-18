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

//! Executor boundary for `MERGE FIRST PARTITION LESS THAN`.

use super::*;

#[test]
fn alter_merge_first_partition_is_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table merge_first_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into merge_first_boundary values (1)");

    let sql = "alter table merge_first_boundary merge first partition less than (60)";
    assert_eq!(
        step(&mut db, sql),
        "Unsupported(\"ALTER TABLE MERGE FIRST PARTITION\")",
        "source SQL: {sql}"
    );
    assert!(db.transaction.is_active(), "source SQL: {sql}");
    assert_eq!(db.transaction.savepoint_count(), 0, "source SQL: {sql}");
    assert_eq!(
        db.tables["merge_first_boundary"].cols,
        vec!["id".to_string()]
    );

    step(&mut db, "rollback");
    assert_eq!(step(&mut db, "select id from merge_first_boundary"), "RS:");
}
