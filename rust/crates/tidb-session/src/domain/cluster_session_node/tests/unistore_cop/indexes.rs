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


//! Unique and non-unique secondary indexes: duplicate detection on insert
//! and update, NULLs never colliding, a unique entry resolving to its row,
//! and the INDEX_LOOKUP_PUSHDOWN hint, over the cop-backed TestKit stack (X11).

use super::sql_kit::*;
use super::*;

#[test]
fn unique_indexes() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 660);
    exec_all(&mut tk, &[
        "CREATE TABLE u (id BIGINT PRIMARY KEY, email VARCHAR(32) UNIQUE, v BIGINT)",
        "INSERT INTO u VALUES (1, 'a@x', 10), (2, 'b@x', 20)",
    ]);
    let duplicate = error(&mut tk, "INSERT INTO u VALUES (3, 'a@x', 30)");
    assert_eq!((duplicate.code, duplicate.message.as_str()), (1062, "Duplicate entry 'a@x' for key 'u.email'"));
    assert_eq!(q(&mut tk, "SELECT id FROM u").len(), 2);
    assert_eq!(error(&mut tk, "UPDATE u SET email = 'a@x' WHERE id = 2").code, 1062);
    assert_eq!(q(&mut tk, "SELECT email FROM u WHERE id = 2"), [["b@x"]]);
    exec_all(&mut tk, &[
        "UPDATE u SET email = 'c@x' WHERE id = 1",
        "INSERT INTO u VALUES (4, 'a@x', 40)",
        "DELETE FROM u WHERE id = 4",
        "INSERT INTO u VALUES (5, 'a@x', 50)",
        "INSERT INTO u VALUES (6, NULL, 60)",
        "INSERT INTO u VALUES (7, NULL, 70)",
    ]);
    assert_eq!(q(&mut tk, "SELECT id FROM u").len(), 5);
}

#[test]
fn a_non_unique_index_accepts_repeats() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 661);
    exec_all(&mut tk, &[
        "CREATE TABLE n (id BIGINT PRIMARY KEY, tag VARCHAR(8), KEY tag_idx (tag))",
        "INSERT INTO n VALUES (1, 'x'), (2, 'x'), (3, 'y')",
    ]);
    assert_eq!(q(&mut tk, "SELECT id FROM n").len(), 3);
}

/// A unique entry carries its row's handle: a point read through the index
/// answers the row, a missing value answers nothing.
#[test]
fn a_unique_index_entry_points_at_its_row() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 662);
    exec_all(&mut tk, &["CREATE TABLE k (id BIGINT PRIMARY KEY, code VARCHAR(8) UNIQUE)", "INSERT INTO k VALUES (7, 'abc'), (8, 'def')"]);
    let plan = plan_lines(&mut tk, "SELECT id FROM k WHERE code = 'abc'");
    assert!(plan[0].contains("Point_Get") && plan[0].contains("index:code(code)"), "{plan:?}");
    assert_eq!(q(&mut tk, "SELECT id FROM k WHERE code = 'abc'"), [["7"]]);
    assert!(q(&mut tk, "SELECT id FROM k WHERE code = 'nope'").is_empty());
}

/// Go's INDEX_LOOKUP_PUSHDOWN plans a LocalIndexLookUp under the reader; with
/// NO_INDEX_LOOKUP_PUSHDOWN it does not, and warns 1815.
#[test]
fn index_lookup_pushdown_hint_reaches_the_shared_physical_reader() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 663);
    exec_all(&mut tk, &[
        "CREATE TABLE lookup_hint (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, KEY ia(a))",
        "INSERT INTO lookup_hint VALUES (1, 2, 10), (2, 1, 20)",
    ]);
    let hinted = "SELECT /*+ INDEX_LOOKUP_PUSHDOWN(lookup_hint, ia) */ b FROM lookup_hint WHERE a > 0";
    let plan = plan_lines(&mut tk, hinted);
    assert!(plan.iter().any(|line| line.contains("LocalIndexLookUp")), "{plan:?}");
    let mut answered = q(&mut tk, hinted);
    answered.sort();
    assert_eq!(answered, [["10"], ["20"]]);
    let conflict = "SELECT /*+ INDEX_LOOKUP_PUSHDOWN(lookup_hint, ia) NO_INDEX_LOOKUP_PUSHDOWN(lookup_hint) */ b FROM lookup_hint WHERE a > 0";
    let plan = plan_lines(&mut tk, conflict);
    assert!(plan.iter().all(|line| !line.contains("LocalIndexLookUp")), "{plan:?}");
    assert!(
        q(&mut tk, "SHOW WARNINGS").iter().any(|row| row[2] == "hint INDEX_LOOKUP_PUSHDOWN cannot be inapplicable, NO_INDEX_LOOKUP_PUSHDOWN is specified"),
        "{plan:?}"
    );
}
