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

//! The catalog's version counter: what moves it, and -- the half that matters
//! to a concurrent transaction -- what must NOT.
//!
//! `tidb-session`'s `txn.rs` compares this number at commit against the one
//! the transaction started from, so a bump with no change behind it aborts an
//! unrelated transaction. Go advances its schema version per COMPLETED DDL
//! job, not per attempted one.

use super::*;

/// Every mutator used to bump the version on entry and only then discover it
/// had nothing to do, so a refused or no-op DDL moved the catalog's version
/// without moving the catalog.
#[test]
fn a_ddl_that_changes_nothing_does_not_move_the_version() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE t (a BIGINT)", &mut catalog).unwrap();

    let steady = catalog.version();
    // A schema that already exists, one that never did, a table that never
    // did, and a rename with no destination schema: four no-ops.
    assert!(!catalog.create_database("test"));
    assert!(!catalog.drop_database("nosuchdb"));
    assert!(!catalog.drop_table_in("test", "nosuch"));
    assert!(!catalog.drop_table_in("nosuchdb", "t"));
    assert!(!catalog.rename_table("test", "t", "nosuchdb", "t2"));
    assert!(!catalog.rename_table("test", "nosuch", "test", "t2"));
    assert_eq!(catalog.version(), steady);

    // The refused rename left the source where it was.
    assert!(catalog.contains_in("test", "t"));

    // And a real change still moves it.
    assert!(catalog.create_database("other"));
    assert!(catalog.version() > steady);
    let after_create = catalog.version();
    assert!(catalog.rename_table("test", "t", "other", "t2"));
    assert!(catalog.version() > after_create);
    assert!(catalog.contains_in("other", "t2"));
    assert!(!catalog.contains_in("test", "t"));
}
