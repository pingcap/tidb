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

//! Executor boundary for interval `SPLIT MAXVALUE` partition syntax.

use super::*;

#[test]
fn alter_table_split_maxvalue_partition_is_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table partition_split_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into partition_split_boundary values (1)");

    assert_eq!(
        step(
            &mut db,
            "alter table partition_split_boundary split maxvalue partition less than (140)",
        ),
        "Unsupported(\"ALTER TABLE SPLIT MAXVALUE PARTITION\")"
    );
    assert!(db.transaction.is_active());
    assert_eq!(db.transaction.savepoint_count(), 0);
    assert_eq!(
        db.tables["partition_split_boundary"].cols,
        vec!["id".to_string()]
    );

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from partition_split_boundary"),
        "RS:"
    );
}
