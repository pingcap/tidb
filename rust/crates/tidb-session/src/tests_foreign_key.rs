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

//! `FOREIGN KEY`: one test per rule `tidb_executor::foreign_key` transcreates.
//!
//! The corpus topics (`foreign_key*`, `savepoint_foreign_key_source` in
//! `rust/difftests/corpus/table/`) prove the SYMPTOM -- that a script of real
//! TiDB output replays -- and these prove the RULE, one at a time, so a
//! regression names itself instead of showing up as a line number in a
//! golden file.
//!
//! Every expectation here was captured from real TiDB through
//! `rust/difftests/gorun` before it was written down.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// The error code a statement fails with, or `None` when it succeeded.
fn code(session: &mut Session, sql: &str) -> Option<u16> {
    match session.run(sql) {
        Ok(_) => None,
        Err(error) => Some(error.to_mysql_error().code),
    }
}

/// A parent holding `(1)` and a child that references it, with `action`
/// spliced into the child's constraint.
fn pair(action: &str) -> Session {
    let mut session = Session::new();
    session.run("CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    session
        .run(&format!(
            "CREATE TABLE c (id INT, pid INT, FOREIGN KEY (pid) REFERENCES p(id) {action})"
        ))
        .unwrap();
    session.run("INSERT INTO p VALUES (1), (2)").unwrap();
    session.run("INSERT INTO c VALUES (10, 1)").unwrap();
    session
}

/// The child side: a referencing value must name an existing parent row.
#[test]
fn a_child_row_must_reference_an_existing_parent_row() {
    let mut session = pair("");
    assert_eq!(
        code(&mut session, "INSERT INTO c VALUES (20, 99)"),
        Some(1452)
    );
    assert_eq!(code(&mut session, "INSERT INTO c VALUES (20, 2)"), None);
    // An UPDATE of the child re-checks exactly as an INSERT does.
    assert_eq!(
        code(&mut session, "UPDATE c SET pid = 99 WHERE id = 20"),
        Some(1452)
    );
    assert_eq!(rows(&mut session, "SELECT pid FROM c ORDER BY id").len(), 2);
}

/// MATCH SIMPLE, MySQL/TiDB's only implemented mode: ANY null component
/// skips the check entirely, composite keys included.
#[test]
fn a_null_referencing_column_skips_the_check_even_in_a_composite_key() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE cp (x INT, y INT, PRIMARY KEY (x, y))")
        .unwrap();
    session
        .run("CREATE TABLE cc (a INT, b INT, FOREIGN KEY (a, b) REFERENCES cp(x, y))")
        .unwrap();
    session.run("INSERT INTO cp VALUES (1, 1)").unwrap();
    assert_eq!(code(&mut session, "INSERT INTO cc VALUES (1, NULL)"), None);
    assert_eq!(code(&mut session, "INSERT INTO cc VALUES (NULL, 2)"), None);
    assert_eq!(code(&mut session, "INSERT INTO cc VALUES (1, 1)"), None);
    // Both components non-null and no parent row: the one rejected case.
    assert_eq!(
        code(&mut session, "INSERT INTO cc VALUES (1, 2)"),
        Some(1452)
    );
}

/// The parent side, default action: a referenced row cannot be deleted, and
/// an unreferenced one can.
#[test]
fn a_referenced_parent_row_cannot_be_deleted_without_an_action() {
    let mut session = pair("");
    assert_eq!(code(&mut session, "DELETE FROM p WHERE id = 1"), Some(1451));
    assert_eq!(
        rows(&mut session, "SELECT id FROM p ORDER BY id"),
        vec![vec!["1"], vec!["2"]]
    );
    assert_eq!(code(&mut session, "DELETE FROM p WHERE id = 2"), None);
}

/// `NO ACTION` and `SET DEFAULT` are not deferred and not defaulting: InnoDB
/// never implemented `SET DEFAULT`, so both behave exactly as `RESTRICT`.
#[test]
fn no_action_and_set_default_both_restrict() {
    for action in [
        "ON DELETE NO ACTION",
        "ON DELETE SET DEFAULT",
        "ON DELETE RESTRICT",
    ] {
        let mut session = pair(action);
        assert_eq!(
            code(&mut session, "DELETE FROM p WHERE id = 1"),
            Some(1451),
            "{action} should restrict"
        );
        assert_eq!(
            rows(&mut session, "SELECT pid FROM c"),
            vec![vec!["1"]],
            "{action} should leave the child untouched"
        );
    }
}

/// The parent side only triggers on a value that ACTUALLY changes: touching
/// an unreferenced column, or assigning a referenced one its own value,
/// never checks a dependent.
#[test]
fn only_a_changed_referenced_value_triggers_the_parent_side() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE p (id INT PRIMARY KEY, nm VARCHAR(10))")
        .unwrap();
    session
        .run("CREATE TABLE c (id INT, pid INT, FOREIGN KEY (pid) REFERENCES p(id))")
        .unwrap();
    session.run("INSERT INTO p VALUES (1, 'a')").unwrap();
    session.run("INSERT INTO c VALUES (10, 1)").unwrap();
    assert_eq!(
        code(&mut session, "UPDATE p SET nm = 'z' WHERE id = 1"),
        None
    );
    assert_eq!(code(&mut session, "UPDATE p SET id = 1 WHERE id = 1"), None);
    assert_eq!(
        code(&mut session, "UPDATE p SET id = 99 WHERE id = 1"),
        Some(1451)
    );
    assert_eq!(
        rows(&mut session, "SELECT id, nm FROM p"),
        vec![vec!["1", "z"]]
    );
}

/// `ON DELETE CASCADE` recurses through as many foreign-key hops as exist,
/// which is what makes deleting one `p` row empty `g` as well as `c`.
#[test]
fn on_delete_cascade_reaches_through_more_than_one_hop() {
    let mut session = Session::new();
    session.run("CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    session
        .run(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE)",
        )
        .unwrap();
    session
        .run(
            "CREATE TABLE g (id INT, cid INT, \
             FOREIGN KEY (cid) REFERENCES c(id) ON DELETE CASCADE)",
        )
        .unwrap();
    session.run("INSERT INTO p VALUES (1), (2)").unwrap();
    session
        .run("INSERT INTO c VALUES (10, 1), (11, 1), (12, 2)")
        .unwrap();
    session.run("INSERT INTO g VALUES (100, 10)").unwrap();
    session.run("DELETE FROM p WHERE id = 1").unwrap();
    assert_eq!(rows(&mut session, "SELECT id FROM p"), vec![vec!["2"]]);
    assert_eq!(rows(&mut session, "SELECT id FROM c"), vec![vec!["12"]]);
    assert!(rows(&mut session, "SELECT id FROM g").is_empty());
}

/// `ON UPDATE CASCADE` repoints the dependents at the new value, and does so
/// through the same hops.
#[test]
fn on_update_cascade_repoints_dependents_through_more_than_one_hop() {
    let mut session = Session::new();
    session.run("CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    session
        .run(
            "CREATE TABLE c (id INT PRIMARY KEY, pid INT, \
             FOREIGN KEY (pid) REFERENCES p(id) ON UPDATE CASCADE)",
        )
        .unwrap();
    session
        .run(
            "CREATE TABLE g (id INT, cid INT, \
             FOREIGN KEY (cid) REFERENCES c(id) ON UPDATE CASCADE)",
        )
        .unwrap();
    session.run("INSERT INTO p VALUES (1)").unwrap();
    session.run("INSERT INTO c VALUES (10, 1)").unwrap();
    session.run("INSERT INTO g VALUES (100, 10)").unwrap();
    session.run("UPDATE p SET id = 99 WHERE id = 1").unwrap();
    assert_eq!(rows(&mut session, "SELECT id FROM p"), vec![vec!["99"]]);
    assert_eq!(
        rows(&mut session, "SELECT id, pid FROM c"),
        vec![vec!["10", "99"]]
    );
    // The second hop's referenced value did not change, so `g` is untouched
    // -- the cascade repoints, it does not renumber.
    assert_eq!(
        rows(&mut session, "SELECT id, cid FROM g"),
        vec![vec!["100", "10"]]
    );
}

/// `SET NULL` nulls the referencing columns rather than removing the row.
#[test]
fn set_null_nulls_the_referencing_columns() {
    let mut session = pair("ON DELETE SET NULL");
    session.run("INSERT INTO c VALUES (11, 2)").unwrap();
    session.run("DELETE FROM p WHERE id = 1").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, pid FROM c ORDER BY id"),
        vec![vec!["10", "NULL"], vec!["11", "2"]]
    );
}

/// `INSERT IGNORE` downgrades a violation to a per-row skip: the good rows
/// land, the violating ones do not, and the statement succeeds.
#[test]
fn insert_ignore_skips_only_the_violating_rows() {
    let mut session = Session::new();
    session.run("CREATE TABLE p (i INT PRIMARY KEY)").unwrap();
    session
        .run("CREATE TABLE c (i INT, FOREIGN KEY (i) REFERENCES p(i))")
        .unwrap();
    session.run("INSERT INTO p VALUES (1), (3)").unwrap();
    session
        .run("INSERT IGNORE INTO c VALUES (1), (NULL), (1), (2), (3), (4)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT i FROM c ORDER BY i"),
        vec![vec!["NULL"], vec!["1"], vec!["1"], vec!["3"]]
    );
}

/// `DELETE IGNORE` downgrades the parent side the same way: a restricted row
/// is left in place and the rest of the statement still runs.
#[test]
fn delete_ignore_skips_only_the_restricted_rows() {
    let mut session = pair("");
    // `p` holds 1 (referenced) and 2 (not).
    assert_eq!(code(&mut session, "DELETE FROM p"), Some(1451));
    assert_eq!(code(&mut session, "DELETE IGNORE FROM p"), None);
    assert_eq!(rows(&mut session, "SELECT id FROM p"), vec![vec!["1"]]);
}

/// `foreign_key_checks = 0` disables the DML checks, and is NOT retroactive:
/// a row written while it was off survives turning it back on.
#[test]
fn foreign_key_checks_off_bypasses_the_checks_and_is_not_retroactive() {
    let mut session = pair("");
    assert_eq!(
        code(&mut session, "INSERT INTO c VALUES (20, 99)"),
        Some(1452)
    );
    session.run("SET foreign_key_checks = 0").unwrap();
    assert_eq!(code(&mut session, "INSERT INTO c VALUES (20, 99)"), None);
    assert_eq!(code(&mut session, "DELETE FROM p WHERE id = 1"), None);
    session.run("SET foreign_key_checks = 1").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, pid FROM c ORDER BY id"),
        vec![vec!["10", "1"], vec!["20", "99"]]
    );
    // And the checks are back on for the NEXT statement.
    assert_eq!(
        code(&mut session, "INSERT INTO c VALUES (30, 98)"),
        Some(1452)
    );
}

/// `foreign_key_checks` is a strictly typed boolean, so a non-boolean value
/// is rejected and leaves the setting where it was.
#[test]
fn foreign_key_checks_rejects_a_non_boolean_value() {
    let mut session = Session::new();
    session.run("SET foreign_key_checks = 0").unwrap();
    assert!(session.run("SET foreign_key_checks = 3").is_err());
    assert_eq!(
        rows(&mut session, "SELECT @@foreign_key_checks"),
        vec![vec!["0"]]
    );
    session.run("SET foreign_key_checks = DEFAULT").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT @@foreign_key_checks"),
        vec![vec!["1"]]
    );
}

/// `DROP TABLE` is refused while a table OUTSIDE the statement still
/// references the parent, all-or-nothing over the whole statement -- and
/// dropping the pair together succeeds whichever order they are listed in.
#[test]
fn drop_table_is_refused_while_a_foreign_key_still_points_at_it() {
    let mut session = pair("");
    assert_eq!(code(&mut session, "DROP TABLE p"), Some(1451));
    assert!(session.run("SELECT id FROM p").is_ok());
    assert_eq!(code(&mut session, "DROP TABLE p, c"), None);
}

/// With the checks off, `DROP TABLE` of a referenced parent is allowed --
/// the same switch governs the DDL check and the DML one.
#[test]
fn foreign_key_checks_off_allows_dropping_a_referenced_parent() {
    let mut session = pair("");
    session.run("SET foreign_key_checks = 0").unwrap();
    assert_eq!(code(&mut session, "DROP TABLE p"), None);
}

/// `CREATE TABLE` resolves the `REFERENCES` clause while the checks are on,
/// so a child cannot name a parent that does not exist -- and can when they
/// are off, which is how a dump restores tables in any order.
#[test]
fn a_reference_to_a_missing_table_is_a_create_time_error_unless_checks_are_off() {
    let mut session = Session::new();
    assert!(session
        .run("CREATE TABLE c (id INT, pid INT, FOREIGN KEY (pid) REFERENCES nope(id))")
        .is_err());
    session.run("SET foreign_key_checks = 0").unwrap();
    session
        .run("CREATE TABLE c (id INT, pid INT, FOREIGN KEY (pid) REFERENCES nope(id))")
        .unwrap();
}

/// A `REFERENCES` clause whose column count does not match the referencing
/// key is `ErrWrongFkDef` (1239), before any row is looked at.
#[test]
fn a_mismatched_reference_arity_is_rejected() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE p (x INT, y INT, PRIMARY KEY (x, y))")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE c (a INT, FOREIGN KEY (a) REFERENCES p(x, y))"
        ),
        Some(1239)
    );
}

/// A cascade inside a transaction is taken back with the transaction, so a
/// `ROLLBACK TO` restores parent and child together.
#[test]
fn a_cascade_is_taken_back_by_rollback_to_savepoint() {
    let mut session = pair("ON DELETE CASCADE");
    session.run("BEGIN").unwrap();
    session.run("SAVEPOINT sp").unwrap();
    session.run("DELETE FROM p WHERE id = 1").unwrap();
    assert!(rows(&mut session, "SELECT id FROM c").is_empty());
    session.run("ROLLBACK TO sp").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, pid FROM c"),
        vec![vec!["10", "1"]]
    );
    assert_eq!(
        rows(&mut session, "SELECT id FROM p ORDER BY id"),
        vec![vec!["1"], vec!["2"]]
    );
    session.run("ROLLBACK").unwrap();
}

/// `SHOW CREATE TABLE` round-trips the constraint, byte for byte against
/// real TiDB -- including the implicit `KEY` TiDB adds for the referencing
/// columns, and its absence when an existing key already covers them.
#[test]
fn show_create_table_prints_the_constraint_and_its_implicit_index() {
    let mut session = Session::new();
    session.run("CREATE TABLE p (id INT PRIMARY KEY)").unwrap();
    session
        .run(
            "CREATE TABLE c (id INT, pid INT, \
             FOREIGN KEY (pid) REFERENCES p(id) ON DELETE CASCADE)",
        )
        .unwrap();
    assert_eq!(
        rows(&mut session, "SHOW CREATE TABLE c")[0][1],
        "CREATE TABLE `c` (\n  \
         `id` int(11) DEFAULT NULL,\n  \
         `pid` int(11) DEFAULT NULL,\n  \
         KEY `fk_1` (`pid`),\n  \
         CONSTRAINT `fk_1` FOREIGN KEY (`pid`) REFERENCES `p` (`id`) ON DELETE CASCADE\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    // A key whose PREFIX is the referencing columns already covers the
    // constraint, so no implicit index is added; one that merely contains
    // them does not.
    session
        .run("CREATE TABLE e (pid INT, k INT, KEY kk (pid, k), FOREIGN KEY (pid) REFERENCES p(id))")
        .unwrap();
    assert!(!rows(&mut session, "SHOW CREATE TABLE e")[0][1].contains("KEY `fk_1`"));
    session
        .run("CREATE TABLE f (pid INT, k INT, KEY kk (k, pid), FOREIGN KEY (pid) REFERENCES p(id))")
        .unwrap();
    assert!(rows(&mut session, "SHOW CREATE TABLE f")[0][1].contains("KEY `fk_1` (`pid`)"));
    // The clustered primary key covers it too.
    session
        .run("CREATE TABLE g (pid INT PRIMARY KEY, FOREIGN KEY (pid) REFERENCES p(id))")
        .unwrap();
    assert!(!rows(&mut session, "SHOW CREATE TABLE g")[0][1].contains("KEY `fk_1`"));
}
