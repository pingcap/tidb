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


//! `CREATE TABLE ... LIKE` (Go `pkg/ddl` `BuildTableInfoWithLike`): the
//! structure without rows, auto-increment value or foreign keys, fresh
//! partition ids, and its refusals, over the cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

#[test]
fn like_copies_the_structure_and_none_of_the_rows() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 650);
    exec_all(&mut tk, &[
        "CREATE TABLE src (a BIGINT, b VARCHAR(10), KEY k (a), UNIQUE KEY u (b))",
        "INSERT INTO src VALUES (1, 'x'), (2, 'y')",
        "CREATE TABLE dst LIKE src",
    ]);
    assert_eq!(
        q(&mut tk, "SELECT column_name FROM information_schema.columns WHERE table_schema = 'test' AND table_name = 'dst' ORDER BY ordinal_position"),
        [["a"], ["b"]]
    );
    assert_eq!(
        q(&mut tk, "SELECT index_name, non_unique FROM information_schema.statistics WHERE table_schema = 'test' AND table_name = 'dst' ORDER BY index_name"),
        [["k", "1"], ["u", "0"]]
    );
    assert!(q(&mut tk, "SELECT a FROM dst").is_empty());
    rows(&mut tk, "INSERT INTO dst VALUES (9, 'z')");
    assert_eq!(q(&mut tk, "SELECT a FROM src").len(), 2);
}

#[test]
fn like_does_not_copy_the_auto_increment_value() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 651);
    exec_all(&mut tk, &[
        "CREATE TABLE src (id BIGINT PRIMARY KEY AUTO_INCREMENT, v BIGINT)",
        "INSERT INTO src (v) VALUES (1)",
        "INSERT INTO src (v) VALUES (1)",
        "INSERT INTO src (v) VALUES (1)",
        "CREATE TABLE dst LIKE src",
        "INSERT INTO dst (v) VALUES (7)",
    ]);
    assert_eq!(q(&mut tk, "SELECT id FROM dst"), [["1"]]);
}

#[test]
fn like_does_not_copy_foreign_keys() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 652);
    exec_all(&mut tk, &[
        "CREATE TABLE parent (id BIGINT PRIMARY KEY)",
        "CREATE TABLE child (id BIGINT PRIMARY KEY, pid BIGINT, FOREIGN KEY (pid) REFERENCES parent (id))",
        "CREATE TABLE copy_of_child LIKE child",
    ]);
    let fks = |tk: &mut Session, name: &str| {
        q(tk, &format!("SELECT count(*) FROM information_schema.referential_constraints WHERE constraint_schema = 'test' AND table_name = '{name}'"))[0][0].clone()
    };
    assert_eq!(fks(&mut tk, "copy_of_child"), "0");
    assert_eq!(fks(&mut tk, "child"), "1");
}

/// Go `ErrWrongObject` (1347).
#[test]
fn like_a_view_is_wrong_object() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 653);
    exec_all(&mut tk, &["CREATE TABLE t (a BIGINT)", "CREATE VIEW v AS SELECT a FROM t"]);
    let refused = error(&mut tk, "CREATE TABLE dst LIKE v");
    assert_eq!((refused.code, refused.message.as_str()), (1347, "'test.v' is not BASE TABLE"));
}

/// Go `ErrTableNotExists` (1146) / `ErrTableExists` (1050); IF NOT EXISTS
/// over an existing name is a 1050 note.
#[test]
fn like_reports_missing_source_and_existing_target() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 654);
    rows(&mut tk, "CREATE TABLE t (a BIGINT)");
    let missing = error(&mut tk, "CREATE TABLE dst LIKE nosuch");
    assert_eq!((missing.code, missing.message.as_str()), (1146, "Table 'test.nosuch' doesn't exist"));
    rows(&mut tk, "CREATE TABLE dst LIKE t");
    let exists = error(&mut tk, "CREATE TABLE dst LIKE t");
    assert_eq!((exists.code, exists.message.as_str()), (1050, "Table 'test.dst' already exists"));
    rows(&mut tk, "CREATE TABLE IF NOT EXISTS dst LIKE t");
    assert_eq!(warning_codes(&mut tk), ["1050"]);
}

/// The copy's partitions keep their names and get fresh physical ids.
#[test]
fn like_copies_partitioning_with_fresh_physical_ids() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 655);
    exec_all(&mut tk, &[
        "CREATE TABLE src (a BIGINT) PARTITION BY HASH (a) PARTITIONS 4",
        "CREATE TABLE dst LIKE src",
    ]);
    let partitions = |tk: &mut Session, name: &str| {
        q(tk, &format!("SELECT partition_name, tidb_partition_id FROM information_schema.partitions WHERE table_schema = 'test' AND table_name = '{name}' ORDER BY partition_ordinal_position"))
    };
    let source = partitions(&mut tk, "src");
    let copy = partitions(&mut tk, "dst");
    assert_eq!(copy.iter().map(|row| row[0].as_str()).collect::<Vec<_>>(), ["p0", "p1", "p2", "p3"]);
    let table_id = q(&mut tk, "SELECT tidb_table_id FROM information_schema.tables WHERE table_schema = 'test' AND table_name = 'dst'")[0][0].clone();
    for row in &copy {
        assert!(source.iter().all(|other| other[1] != row[1]), "{source:?} {copy:?}");
        assert_ne!(row[1], table_id);
    }
    rows(&mut tk, "INSERT INTO dst VALUES (1), (2), (3)");
    assert!(q(&mut tk, "SELECT a FROM src").is_empty());
}
