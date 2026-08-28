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

//! GO PORT of `pkg/ddl/foreign_key_test.go:261
//! TestDropIndexNeededInForeignKey2` (`pkg/ddl.part6` batch b105).
//!
//! The Go test interleaves two sessions through the
//! `beforeRunOneJobStep` failpoint so that a second `DROP INDEX` is queued
//! while the first one runs, and pins the error the queued statement ends
//! with: `[ddl:1553]Cannot drop index 'idx2': needed in a foreign key
//! constraint`. The interleaving itself is the DDL job queue's, and that
//! machinery is not transcreated in this tier -- what the interleaving
//! PRODUCES is the guard decision, and that guard is transcreated:
//! `checkIndexNeededInForeignKey` lives at
//! `tidb_executor::foreign_key::check_index_needed`
//! (crates/tidb-executor/src/foreign_key.rs:862) and runs from the
//! `ALTER TABLE ... DROP INDEX` path (crates/tidb-executor/src/ddl/
//! indexes.rs). The test below replays the two decisions in the same order
//! the two sessions reach them: after `idx1` is gone, `idx2` is the only
//! remaining index whose leading key parts cover the constraint's columns,
//! so dropping it is 1553 -- while dropping `idx1` first succeeds because
//! `idx2` still covers.

use tidb_executor::{
    run_alter_table_in, run_create_table_in, Catalog, CreateTableSettings, StmtContext,
};

/// Go's two tables: `create table t1 (id int key, b int)` and
/// `create table t2 (a int, b int, index idx1 (b), index idx2 (b),
/// foreign key (b) references t1(id))`, both in `test`, with
/// `@@global.tidb_enable_foreign_key=1` and `@@foreign_key_checks=1`
/// (foreign_key_test.go:262-270).
fn catalog_with_two_indexes() -> Catalog {
    let mut catalog = Catalog::default();
    run_create_table_in(
        "create table t1 (id int key, b int)",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    run_create_table_in(
        "create table t2 (a int, b int, index idx1 (b), index idx2 (b), foreign key (b) references t1(id))",
        &mut catalog,
        "test",
        CreateTableSettings::default(),
        &StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

/// GO PORT of the guard outcome foreign_key_test.go:271-295 pins.
///
/// `alter table t2 drop index idx1` succeeds (the constraint stays covered by
/// `idx2`, so `checkIndexNeededInForeignKey`, pkg/ddl/foreign_key.go, lets it
/// through), and then `alter table t2 drop index idx2` -- the statement the
/// Go test runs from the second session while the first drop executes --
/// fails with exactly `[ddl:1553]Cannot drop index 'idx2': needed in a
/// foreign key constraint`, because with `idx1` gone no remaining index
/// covers `(b)` in its leading key parts.
#[test]
fn drop_index_needed_in_foreign_key_is_1553_only_for_the_last_cover() {
    let mut catalog = catalog_with_two_indexes();
    run_alter_table_in(
        "alter table t2 drop index idx1",
        &mut catalog,
        "test",
        &StmtContext::for_query(),
    )
    .expect("idx1 is not the last covering index");

    let error = run_alter_table_in(
        "alter table t2 drop index idx2",
        &mut catalog,
        "test",
        &StmtContext::for_query(),
    )
    .expect_err("idx2 is the constraint's last cover");
    let mysql = error.to_mysql_error();
    assert_eq!(mysql.code, 1553, "Go's ErrDropIndexNeededInForeignKey");
    assert_eq!(
        mysql.message,
        "Cannot drop index 'idx2': needed in a foreign key constraint",
        "the exact text foreign_key_test.go:294 pins"
    );
    // The refusal left the index in place.
    let Some(tidb_executor::TableEntry::Kv(t2)) = catalog.table_in("test", "t2") else {
        panic!("t2 is a TiKV-backed table");
    };
    assert!(
        t2.indexes().iter().any(|index| index.name == "idx2"),
        "the refused drop left idx2 in place"
    );
}
