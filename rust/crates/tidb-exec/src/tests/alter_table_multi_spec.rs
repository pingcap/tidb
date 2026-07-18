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

//! Executor boundary for source-accepted ordered `ALTER TABLE` specifications.

use super::*;

#[test]
fn source_multi_specs_reject_before_any_catalog_or_transaction_mutation() {
    for sql in [
        "alter table multi_spec_boundary add column b int default 2, add column if not exists a int",
        "alter table multi_spec_boundary drop column if exists c, drop column a",
        "alter table multi_spec_boundary drop column a, drop column if exists d, drop column c",
        "alter table multi_spec_boundary add column d int default 4, add index i3(c), drop column a, drop column if exists z, add column if not exists e int default 5, drop index i2, add column f int default 6, drop column b, drop index i1, add column if not exists c int",
    ] {
        let mut db = Database::new();
        step(
            &mut db,
            "create table multi_spec_boundary (a int, b int, c int)",
        );
        step(&mut db, "begin");
        step(&mut db, "insert into multi_spec_boundary values (1, 2, 3)");
        step(&mut db, "savepoint before_multi_spec");

        assert_eq!(
            step(&mut db, sql),
            "Unsupported(\"ALTER TABLE multiple actions\")",
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active(), "source SQL: {sql}");
        assert!(db.transaction.savepoint_count() != 0, "source SQL: {sql}");
        assert_eq!(
            db.tables["multi_spec_boundary"].cols,
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            "source SQL: {sql}"
        );
        assert_eq!(
            step(&mut db, "rollback to savepoint before_multi_spec"),
            "OK",
            "source SQL: {sql}"
        );
        assert!(db.transaction.is_active(), "source SQL: {sql}");
        assert_eq!(
            db.tables["multi_spec_boundary"].cols,
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            "source SQL: {sql}"
        );
    }
}
