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

//! Executor boundary for interval-partition ALTER bound syntax.

use super::*;

#[test]
fn alter_interval_partition_bounds_are_unsupported_before_mutation() {
    let mut db = Database::new();
    step(&mut db, "create table interval_bound_boundary (id int)");
    step(&mut db, "begin");
    step(&mut db, "insert into interval_bound_boundary values (1)");

    for sql in [
        "alter table interval_bound_boundary last partition less than (100)",
        "alter table interval_bound_boundary first partition less than (30)",
    ] {
        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE INTERVAL PARTITION BOUND\")",
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active(), "source SQL: {sql}");
        assert_eq!(db.transaction.savepoint_count(), 0, "source SQL: {sql}");
        assert_eq!(
            db.tables["interval_bound_boundary"].cols,
            vec!["id".to_string()]
        );
    }

    step(&mut db, "rollback");
    assert_eq!(
        step(&mut db, "select id from interval_bound_boundary"),
        "RS:"
    );
}
