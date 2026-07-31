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

/// A constraint addresses its own side by column offset and the other side
/// by table name, so a layout change or a rename would silently repoint it.
/// Both are REFUSED on a participating table -- on either side of the
/// constraint -- rather than corrupting it. (Real TiDB rewrites the affected
/// `FKInfo`s instead; this is the honest refusal until that lands.)
#[test]
fn a_layout_change_or_rename_is_refused_on_either_side_of_a_constraint() {
    for statement in [
        "ALTER TABLE c ADD COLUMN extra INT",
        "ALTER TABLE c DROP COLUMN id",
        "ALTER TABLE c MODIFY COLUMN pid INT FIRST",
        "ALTER TABLE c RENAME TO cc",
        "ALTER TABLE p ADD COLUMN extra INT",
        "ALTER TABLE p RENAME TO pp",
        "RENAME TABLE c TO cc",
        "RENAME TABLE p TO pp",
    ] {
        let mut session = pair("");
        assert!(
            session.run(statement).is_err(),
            "{statement} should be refused"
        );
    }
    // An index change touches neither addressing scheme, so it still runs.
    let mut session = pair("");
    session.run("ALTER TABLE c ADD INDEX kid (id)").unwrap();
    // And a table with no foreign key at all is unaffected.
    session.run("CREATE TABLE plain (a INT)").unwrap();
    session.run("ALTER TABLE plain ADD COLUMN b INT").unwrap();
    session.run("RENAME TABLE plain TO plainer").unwrap();
}

/// A parent whose REFERENCED column is a STORED GENERATED column, with a
/// child that cascades both ways -- the fixture of
/// `TestForeignKeyAndGeneratedColumn` in
/// `tests/integrationtest/t/executor/foreign_key.test`.
fn generated_parent() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (a INT, b INT AS (a) STORED, INDEX(b))")
        .unwrap();
    session
        .run(
            "CREATE TABLE t2 (a INT, b INT, CONSTRAINT fk FOREIGN KEY (b) REFERENCES t1(b) \
             ON DELETE CASCADE ON UPDATE CASCADE)",
        )
        .unwrap();
    session.run("INSERT INTO t1 (a) VALUES (1),(2)").unwrap();
    session.run("INSERT INTO t2 (a) VALUES (1),(2)").unwrap();
    session.run("UPDATE t2 SET b=a").unwrap();
    session.run("INSERT INTO t2 VALUES (1,1),(2,2)").unwrap();
    session
}

/// THE STALE-KEY TRAP. An `UPDATE` assigns only the columns its `SET` list
/// names, so a STORED generated column still holds the value computed from
/// the OLD dependency when the referential operators run. If the cascade
/// reads that row, the referenced key looks UNCHANGED and `ON UPDATE CASCADE`
/// never fires -- the children keep pointing at a key the parent no longer
/// has, which is a wrong row set on the very next `SELECT`.
///
/// Recorded TiDB (`tests/integrationtest/r/executor/foreign_key.result`,
/// `TestForeignKeyAndGeneratedColumn`): after `update t1 set a=a+10 where
/// a=1`, `t1` is `2|2` and `11|11`, and `t2` is `1|11`, `1|11`, `2|2`, `2|2`.
#[test]
fn on_update_cascade_fires_when_the_referenced_column_is_generated() {
    let mut session = generated_parent();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t2 ORDER BY a"),
        vec![
            vec!["1".to_owned(), "1".to_owned()],
            vec!["1".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "2".to_owned()],
            vec!["2".to_owned(), "2".to_owned()],
        ]
    );
    session.run("UPDATE t1 SET a=a+10 WHERE a=1").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t1 ORDER BY a"),
        vec![
            vec!["2".to_owned(), "2".to_owned()],
            vec!["11".to_owned(), "11".to_owned()],
        ]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM t2 ORDER BY a"),
        vec![
            vec!["1".to_owned(), "11".to_owned()],
            vec!["1".to_owned(), "11".to_owned()],
            vec!["2".to_owned(), "2".to_owned()],
            vec!["2".to_owned(), "2".to_owned()],
        ]
    );
    // The indexes agree with the rows the cascade rewrote, on both sides.
    session.run("ADMIN CHECK TABLE t1").unwrap();
    session.run("ADMIN CHECK TABLE t2").unwrap();
}

/// The same fixture's `ON DELETE CASCADE` half, which withdraws a key by
/// deleting the row rather than by rewriting it. Recorded TiDB: after
/// `delete from t1 where a=2`, `t1` is `11|11` and `t2` is `1|11`, `1|11`.
#[test]
fn on_delete_cascade_follows_a_generated_referenced_column() {
    let mut session = generated_parent();
    session.run("UPDATE t1 SET a=a+10 WHERE a=1").unwrap();
    session.run("DELETE FROM t1 WHERE a=2").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t1 ORDER BY a"),
        vec![vec!["11".to_owned(), "11".to_owned()]]
    );
    assert_eq!(
        rows(&mut session, "SELECT * FROM t2 ORDER BY a"),
        vec![
            vec!["1".to_owned(), "11".to_owned()],
            vec!["1".to_owned(), "11".to_owned()],
        ]
    );
    session.run("ADMIN CHECK TABLE t1").unwrap();
    session.run("ADMIN CHECK TABLE t2").unwrap();
}

/// THE CONTROL. Recomputing the new row before the referential operators must
/// not make every `UPDATE` look like a change: an assignment that leaves the
/// referenced value where it was still withdraws nothing, so a `RESTRICT`
/// parent stays updatable and a `CASCADE` child stays put.
#[test]
fn an_update_that_does_not_move_the_referenced_key_still_cascades_nothing() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (a INT, b INT AS (a) STORED, name VARCHAR(10), INDEX(b))")
        .unwrap();
    session
        .run(
            "CREATE TABLE t2 (a INT, b INT, CONSTRAINT fk FOREIGN KEY (b) REFERENCES t1(b) \
             ON UPDATE CASCADE)",
        )
        .unwrap();
    session
        .run("INSERT INTO t1 (a,name) VALUES (1,'x'),(2,'y')")
        .unwrap();
    session.run("INSERT INTO t2 VALUES (1,1),(2,2)").unwrap();
    // Touching a column the constraint does not reference.
    session.run("UPDATE t1 SET name='z' WHERE a=1").unwrap();
    // Assigning the dependency its own value, which recomputes `b` to the
    // value it already held.
    session.run("UPDATE t1 SET a=1 WHERE a=1").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t2 ORDER BY a"),
        vec![
            vec!["1".to_owned(), "1".to_owned()],
            vec!["2".to_owned(), "2".to_owned()],
        ]
    );
    session.run("ADMIN CHECK TABLE t2").unwrap();
}

/// Go `ddl.checkIndexNeededInForeignKey` (1553): the index a constraint
/// relies on may not be dropped out from under it, on either side.
///
/// A TRIPWIRE, not a dead end. Each `Some(1553)` below is a statement TiDB
/// refuses and this tier used to ACCEPT, so if the guard is ever removed --
/// or narrowed to one side -- these flip to `None` and say so by name.
#[test]
fn an_index_a_foreign_key_needs_cannot_be_dropped() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (id INT KEY, b INT, INDEX idx1(b))")
        .unwrap();
    session
        .run(
            "CREATE TABLE t2 (id INT KEY, b INT, INDEX fk(b), \
             CONSTRAINT fk FOREIGN KEY (b) REFERENCES t1(b))",
        )
        .unwrap();
    // The PARENT's index over the referenced columns.
    assert_eq!(
        code(&mut session, "ALTER TABLE t1 DROP INDEX idx1"),
        Some(1553)
    );
    // The CHILD's index over the referencing columns.
    assert_eq!(
        code(&mut session, "ALTER TABLE t2 DROP INDEX fk"),
        Some(1553)
    );
    // Captured: `foreign_key_checks = 0` does NOT lift this. Go gates the
    // check on the GLOBAL `vardef.EnableForeignKey`, not on the session
    // switch that governs row-level checking, so the refusal stands.
    session.run("SET @@foreign_key_checks=0").unwrap();
    assert_eq!(
        code(&mut session, "ALTER TABLE t1 DROP INDEX idx1"),
        Some(1553)
    );
    session.run("SET @@foreign_key_checks=1").unwrap();
}

/// THE CONTROLS for [`an_index_a_foreign_key_needs_cannot_be_dropped`]: a
/// refusal this broad would break every ordinary `DROP INDEX`, so each of
/// these is a statement TiDB ACCEPTS and this tier must keep accepting.
#[test]
fn an_index_no_foreign_key_needs_still_drops() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (id INT KEY, b INT, INDEX idx1(b))")
        .unwrap();
    session
        .run(
            "CREATE TABLE t2 (id INT KEY, b INT, INDEX fk(b), \
             CONSTRAINT fk FOREIGN KEY (b) REFERENCES t1(b))",
        )
        .unwrap();
    // An index over other columns entirely, on a participating table.
    session
        .run("ALTER TABLE t1 ADD INDEX idx_spare(id, b)")
        .unwrap();
    assert_eq!(
        code(&mut session, "ALTER TABLE t1 DROP INDEX idx_spare"),
        None
    );
    session
        .run("ALTER TABLE t2 ADD INDEX idx_spare2(id)")
        .unwrap();
    assert_eq!(
        code(&mut session, "ALTER TABLE t2 DROP INDEX idx_spare2"),
        None
    );
    // A REDUNDANT index covering the same columns makes the drop legal: the
    // constraint keeps an index, just not this one.
    session.run("ALTER TABLE t1 ADD INDEX idx2(b)").unwrap();
    assert_eq!(code(&mut session, "ALTER TABLE t1 DROP INDEX idx1"), None);
    // And once the constraint itself is gone, both indexes go freely. (Go
    // reaches the same state through `ALTER TABLE t2 DROP FOREIGN KEY fk`,
    // which this tier does not implement yet; dropping the child table
    // withdraws the same constraint.)
    session.run("DROP TABLE t2").unwrap();
    assert_eq!(code(&mut session, "ALTER TABLE t1 DROP INDEX idx2"), None);
}

/// The clustered-primary-key exemption is PARENT-side only.
///
/// Go returns early when the referenced column IS the row handle
/// (`tbInfo.PKIsHandle && len(cols) == 1`) -- there is no index to keep. The
/// CHILD's own index is a separate object and is still 1553, which is the
/// case a symmetric implementation would get wrong.
#[test]
fn the_clustered_handle_exemption_does_not_reach_the_child_index() {
    let mut session = Session::new();
    session.run("CREATE TABLE t1 (id INT PRIMARY KEY)").unwrap();
    session
        .run(
            "CREATE TABLE t2 (id INT KEY, b INT, INDEX fk(b), \
             CONSTRAINT fk FOREIGN KEY (b) REFERENCES t1(id))",
        )
        .unwrap();
    assert_eq!(
        code(&mut session, "ALTER TABLE t2 DROP INDEX fk"),
        Some(1553)
    );
}
