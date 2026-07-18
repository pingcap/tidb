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

//! Executor boundary for the source-accepted empty `ADD PARTITION` action.

use super::*;

#[test]
fn alter_table_add_partition_empty_is_unsupported_before_mutation() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table partition_add_empty_boundary (id int)",
    );
    step(&mut db, "begin");
    step(
        &mut db,
        "insert into partition_add_empty_boundary values (1)",
    );

    for sql in [
        "alter table partition_add_empty_boundary add partition",
        "alter table partition_add_empty_boundary add partition if not exists",
        "alter table partition_add_empty_boundary add partition no_write_to_binlog",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE ADD PARTITION\")",
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active());
        assert!(db.transaction.savepoint_count() == 0);
        assert_eq!(
            db.tables["partition_add_empty_boundary"].cols,
            vec!["id".to_string()]
        );
    }

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from partition_add_empty_boundary"),
        "RS:"
    );
}
