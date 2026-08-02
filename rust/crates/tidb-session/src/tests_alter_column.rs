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

//! `ALTER TABLE ... MODIFY COLUMN` seen from the SCHEMA it leaves behind --
//! Go `pkg/ddl/db_integration_test.go` `TestAlterColumn`, its index-preserving
//! and `AUTO_INCREMENT` halves, plus the anonymous index naming its
//! `multi_unique` block pins.
//!
//! What these cover that the row-level `modify_column` test in `tests_core`
//! cannot: a `MODIFY COLUMN` states a whole new column definition, so anything
//! the OLD column carried and the new definition does not spell out has to be
//! carried forward deliberately. Go does that explicitly
//! (`pkg/ddl/modify_column.go`: copy the index flags, re-imply NOT NULL under
//! a primary key, then check the three AUTO_INCREMENT transitions). A tier
//! that rebuilds the column from the definition alone answers every ROW query
//! correctly and still writes a wrong catalog -- a nullable primary key, an
//! AUTO_INCREMENT that no statement can remove and no statement did remove.
//! Every assertion below is therefore on `SHOW CREATE TABLE` or on an error.
//!
//! NOT ASSERTED (deliberate): integer DISPLAY WIDTHS. A real server applies
//! `deprecate-integer-display-length` in `cmd/tidb-server/main.go`, which the
//! capture harness never runs, so a capture says `int(11)` where a server says
//! `int`; that difference is not what these cases are about.

#![cfg(test)]
use crate::tests_support::*;
use crate::*;

/// Go: `create table mc(a int key nonclustered, b int, c int)` then
/// `alter table mc modify column a int key` / `... c int unique`, both of
/// which must fail -- MODIFY may not ADD a constraint -- and leave the table
/// as it was.
#[test]
fn modify_column_cannot_add_a_key_constraint() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE mc (a INT KEY, b INT, c INT)")
        .unwrap();

    // Go: `err = tk.ExecToErr(...)`, an error either way. Adding a second
    // primary key, or a unique key, through MODIFY is refused.
    assert!(session
        .run("ALTER TABLE mc MODIFY COLUMN a INT KEY")
        .is_err());
    assert!(session
        .run("ALTER TABLE mc MODIFY COLUMN c INT UNIQUE")
        .is_err());

    // Go asserts the whole SHOW CREATE TABLE body here: the refusals changed
    // nothing, and in particular no UNIQUE KEY appeared.
    let create = show_create(&mut session, "mc");
    assert!(create.contains("`a` int NOT NULL"), "{create}");
    assert!(create.contains("`b` int DEFAULT NULL"), "{create}");
    assert!(create.contains("`c` int DEFAULT NULL"), "{create}");
    assert!(create.contains("PRIMARY KEY (`a`)"), "{create}");
    assert!(!create.contains("UNIQUE KEY"), "{create}");
}

/// Go: "Change / modify column should preserve index options."
/// `create table mc(a int key nonclustered, b int, c int unique)`, widen all
/// three columns, and the primary key's NOT NULL and the unique key survive.
///
/// This is the case that caught the bug: the new definition `bigint` says
/// nothing about NULL, so the column was rebuilt nullable and the body read
/// `` `a` bigint DEFAULT NULL `` under a `PRIMARY KEY (`a`)` -- a primary key
/// that admits NULL. Rows read back fine throughout, which is why only a
/// catalog assertion sees it.
#[test]
fn modify_column_preserves_the_primary_key_and_unique_index() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE mc (a INT KEY, b INT, c INT UNIQUE)")
        .unwrap();
    session
        .run("ALTER TABLE mc MODIFY COLUMN a BIGINT")
        .unwrap();
    session
        .run("ALTER TABLE mc MODIFY COLUMN b BIGINT")
        .unwrap();
    session
        .run("ALTER TABLE mc MODIFY COLUMN c BIGINT")
        .unwrap();

    let create = show_create(&mut session, "mc");
    // Captured: `a` bigint(20) NOT NULL -- NOT the `DEFAULT NULL` a bare
    // `bigint` definition would give an ordinary column.
    assert!(create.contains("`a` bigint NOT NULL"), "{create}");
    assert!(create.contains("`b` bigint DEFAULT NULL"), "{create}");
    assert!(create.contains("`c` bigint DEFAULT NULL"), "{create}");
    assert!(create.contains("PRIMARY KEY (`a`)"), "{create}");
    assert!(create.contains("UNIQUE KEY `c` (`c`)"), "{create}");

    // And the preserved key is a key: the second row on the primary key is a
    // duplicate, and so is the second row on the unique column.
    session.run("INSERT INTO mc VALUES (1, 1, 1)").unwrap();
    assert!(session.run("INSERT INTO mc VALUES (1, 2, 2)").is_err());
    assert!(session.run("INSERT INTO mc VALUES (2, 2, 1)").is_err());
    // A NULL primary key is refused, which is what the NOT NULL is for.
    assert!(session.run("INSERT INTO mc VALUES (NULL, 3, 3)").is_err());
}

/// Go: "Dropping or keeping auto_increment is allowed, however adding is not
/// allowed." -- the three transitions, and `@@tidb_allow_remove_auto_inc`
/// gating the one that loses data-generation behavior.
#[test]
fn modify_column_auto_increment_transitions() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE mc (a INT KEY AUTO_INCREMENT, b INT)")
        .unwrap();

    // Keeping it is fine, and it is still AUTO_INCREMENT afterwards.
    session
        .run("ALTER TABLE mc MODIFY COLUMN a BIGINT AUTO_INCREMENT")
        .unwrap();
    let create = show_create(&mut session, "mc");
    assert!(
        create.contains("`a` bigint NOT NULL AUTO_INCREMENT"),
        "{create}"
    );

    // Dropping it needs @@tidb_allow_remove_auto_inc, which is OFF by
    // default. Captured: 8200. The refusal must leave the column alone.
    assert!(matches!(
        session.run("ALTER TABLE mc MODIFY COLUMN a BIGINT"),
        Err(DriverError::UnsupportedModifyColumn(
            "can't remove auto_increment without @@tidb_allow_remove_auto_inc enabled"
        ))
    ));
    let create = show_create(&mut session, "mc");
    assert!(
        create.contains("`a` bigint NOT NULL AUTO_INCREMENT"),
        "{create}"
    );

    // With it on, the drop goes through -- and really drops it, rather than
    // reporting success and leaving the column generating ids.
    session
        .run("SET @@tidb_allow_remove_auto_inc = on")
        .unwrap();
    session
        .run("ALTER TABLE mc MODIFY COLUMN a BIGINT")
        .unwrap();
    let create = show_create(&mut session, "mc");
    assert!(create.contains("`a` bigint NOT NULL"), "{create}");
    assert!(!create.contains("AUTO_INCREMENT"), "{create}");
    // With no generator left, an omitted primary key is 1364 rather than an
    // id -- the observable half of the drop.
    assert!(session.run("INSERT INTO mc (b) VALUES (1)").is_err());

    // Adding it back is refused whatever the variable says. Captured: 8200
    // "can't set auto_increment".
    assert!(matches!(
        session.run("ALTER TABLE mc MODIFY COLUMN a BIGINT AUTO_INCREMENT"),
        Err(DriverError::UnsupportedModifyColumn(
            "can't set auto_increment"
        ))
    ));
}

/// Go, same block: an AUTO_INCREMENT column cannot also carry a DEFAULT
/// (`ErrInvalidDefaultValue`, 1067). Reached through MODIFY, where both the
/// old flag and the new option are in hand.
#[test]
fn modify_column_refuses_auto_increment_with_a_default() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE mc (a INT KEY AUTO_INCREMENT, b INT)")
        .unwrap();
    assert!(matches!(
        session.run("ALTER TABLE mc MODIFY COLUMN a BIGINT AUTO_INCREMENT DEFAULT 5"),
        Err(DriverError::InvalidDefault(_))
    ));
}

/// Go's `multi_unique` block: several anonymous key constraints on one column
/// are all legal, and each gets its OWN name.
///
/// Go `checkConstraintNames`/`setEmptyConstraintName`: an anonymous key is
/// named after its first column, `_2`, `_3`, ... on a collision, drawing from
/// one pool that also holds every explicitly spelled name and the name an
/// inline PRIMARY KEY consumes before it is renamed to `PRIMARY`.
///
/// Naming an inline `UNIQUE` after its column outright, as this tier did, is
/// not a cosmetic difference: it built a table carrying two indexes called
/// `a`, so `SHOW CREATE TABLE` printed a body that cannot be re-executed and
/// `DROP INDEX a` had two answers.
#[test]
fn anonymous_index_names_are_unique() {
    let mut session = Session::new();

    // Captured: UNIQUE KEY `a`, UNIQUE KEY `a_2`.
    session
        .run("CREATE TABLE mu1 (a INT UNIQUE UNIQUE)")
        .unwrap();
    assert_eq!(
        index_names(&mut session, "mu1"),
        ["UNIQUE KEY `a` (`a`)", "UNIQUE KEY `a_2` (`a`)"]
    );

    // Captured: an inline PRIMARY KEY consumes `a` before it is renamed, so
    // the uniques start at `a_2`.
    session
        .run("CREATE TABLE mu3 (a INT KEY UNIQUE UNIQUE UNIQUE)")
        .unwrap();
    assert_eq!(
        index_names(&mut session, "mu3"),
        [
            "UNIQUE KEY `a_2` (`a`)",
            "UNIQUE KEY `a_3` (`a`)",
            "UNIQUE KEY `a_4` (`a`)"
        ]
    );

    // Captured: SERIAL is `bigint(20) unsigned NOT NULL AUTO_INCREMENT
    // UNIQUE`, and SERIAL DEFAULT VALUE is a second, separately named UNIQUE.
    session
        .run("CREATE TABLE mu4 (a SERIAL SERIAL DEFAULT VALUE)")
        .unwrap();
    let create = show_create(&mut session, "mu4");
    assert!(
        create.contains("`a` bigint unsigned NOT NULL AUTO_INCREMENT"),
        "{create}"
    );
    assert_eq!(
        index_names(&mut session, "mu4"),
        ["UNIQUE KEY `a` (`a`)", "UNIQUE KEY `a_2` (`a`)"]
    );

    // Captured `create table n4 (a int, b int, key a_2(b), unique(a), key(a))`:
    // the EXPLICIT `a_2` is reserved before the anonymous ones are filled, so
    // the second anonymous key skips to `a_3` instead of colliding.
    session
        .run("CREATE TABLE n4 (a INT, b INT, KEY a_2 (b), UNIQUE (a), KEY (a))")
        .unwrap();
    assert_eq!(
        index_names(&mut session, "n4"),
        ["KEY `a_2` (`b`)", "UNIQUE KEY `a` (`a`)", "KEY `a_3` (`a`)"]
    );

    // Captured `create table p1 (a int primary key unique, b int)`.
    session
        .run("CREATE TABLE p1 (a INT PRIMARY KEY UNIQUE, b INT)")
        .unwrap();
    assert_eq!(index_names(&mut session, "p1"), ["UNIQUE KEY `a_2` (`a`)"]);
}

/// The index clause lines of one table's `SHOW CREATE TABLE`, in order, so a
/// naming assertion does not have to spell out the column lines around them.
fn index_names(session: &mut Session, table: &str) -> Vec<String> {
    show_create(session, table)
        .lines()
        .map(|line| line.trim().trim_end_matches(',').to_owned())
        .filter(|line| line.starts_with("KEY ") || line.starts_with("UNIQUE KEY "))
        .collect()
}

/// MEASURED DIVERGENCES, pinned so they flip to support when they close.
///
/// Each case below is a statement whose captured TiDB answer this tier does
/// not give. The assertion is written against what this tier DOES do, with
/// the captured answer stated, so closing the gap fails the test and the next
/// unit is told exactly what to assert instead.
#[test]
fn measured_divergences_from_go_test_alter_column() {
    let mut session = Session::new();

    // 1. CLOSED: NONCLUSTERED is honoured. This case pinned the old
    //    accept-and-discard behaviour with the captured TiDB answer stated,
    //    and the CREATE TABLE handle-kind fix closed the gap, flipping the
    //    pin exactly as intended. It now asserts the capture itself: the
    //    declared NONCLUSTERED primary key survives to SHOW CREATE TABLE as
    //    `/*T![clustered_index] NONCLUSTERED */`, matching Go's
    //    ShouldBuildClusteredIndex honouring an explicit clause outright.
    session
        .run("CREATE TABLE nc (a INT PRIMARY KEY NONCLUSTERED, b INT)")
        .unwrap();
    let create = show_create(&mut session, "nc");
    assert!(
        create.contains("/*T![clustered_index] NONCLUSTERED */"),
        "the captured TiDB answer is NONCLUSTERED: {create}"
    );

    // 2. Go's `ErrTooLongKey` (1071): widening a column an index covers past
    //    the 3072-byte key limit is refused. Captured, on an `ascii` table:
    //    `alter table t1 modify column a varchar(3000)` -> ERR, and so do the
    //    CHANGE COLUMN form and `modify column c bigint`, where the widened
    //    column is the first half of a composite key whose other half is
    //    already 3071 bytes. This tier accepts all three, so an index can be
    //    built over a key wider than the encoder's limit.
    session
        .run(
            "CREATE TABLE t1 (a VARCHAR(10), b VARCHAR(100), c TINYINT, d VARCHAR(3071),\
             INDEX (a), INDEX (a, b), INDEX (c, d)) CHARSET = ascii",
        )
        .unwrap();
    assert!(
        session
            .run("ALTER TABLE t1 MODIFY COLUMN a VARCHAR(3000)")
            .is_ok(),
        "captured TiDB answers 1071 ErrTooLongKey here"
    );
    assert!(
        session.run("ALTER TABLE t1 MODIFY COLUMN c BIGINT").is_ok(),
        "captured TiDB answers 1071 ErrTooLongKey here"
    );

    // 3. Two inline PRIMARY KEY clauses on one column: captured TiDB folds
    //    them into the single primary key the column already has, so
    //    `create table (a int key primary key unique unique)` succeeds. This
    //    tier refuses the second one outright.
    assert!(
        session
            .run("CREATE TABLE mu2 (a INT KEY PRIMARY KEY UNIQUE UNIQUE)")
            .is_err(),
        "captured TiDB accepts this and builds one PRIMARY KEY"
    );
}

/// `checkTypeChangeSupported` (Go `pkg/types/field_type.go:1569-1603`, called
/// from `CheckModifyTypeCompatible` at `:1515-1518`): five type-pair MODIFYs
/// Go refuses OUTRIGHT, at statement-build time, before any row is read. The
/// bug this closes: the only gate this tier had before was the per-row
/// `convert_to` call in `KvTable::modify_column`, which never runs on zero
/// rows -- so on an EMPTY table every one of these five refusals used to be
/// silently ACCEPTED. Each rule below is pinned three ways: refused on an
/// EMPTY table (the proof the check moved earlier, not just "still errors
/// somehow"), a same-rule-shaped LEGAL conversion as a control, and refused
/// on a table that already has an offending ROW (proving the statement was
/// always going to fail -- the table check just makes it fail before the
/// scan rather than during it).
///
/// MUTATION PROBE: comment out the `check_type_change_supported` call in
/// `tidb-executor`'s `modify_column_action` (`ddl/alter_table.rs`) and only
/// the five `*_on_empty_table_is_refused` assertions below flip from `Err`
/// to `Ok`; the five `_control_conversion_is_accepted` and
/// `_on_populated_table_is_also_refused` assertions are unaffected, because
/// the row loop still catches the populated cases and nothing here refuses a
/// legal conversion.
///
/// Captured errno: TiDB 8200 (`ErrUnsupportedDDLOperation`), double-wrapped
/// as `Unsupported modify column: Unsupported modify column: change from
/// original type <from> to <to> is currently unsupported yet` -- Go's
/// `CheckModifyTypeCompatible` (`field_type.go:1515-1518`) builds the INNER
/// `ErrUnsupportedModifyColumn`, then its caller `checkModifyTypes`
/// (`pkg/ddl/modify_column.go:2262-2273`) wraps that error's `.Error()` text
/// in a SECOND `ErrUnsupportedModifyColumn`. This tier's
/// `DriverError::UnsupportedModifyColumnType` renders only the inner wrap;
/// the difftest oracle compares REFUSED-vs-ACCEPTED, not this exact text.
#[test]
fn modify_column_type_pair_gate_rule_1_time_like_to_bit() {
    let mut session = Session::new();

    // Rule 1 origin set: date/datetime/timestamp, TIME, YEAR, any string,
    // JSON. Target: BIT. Using JSON -> BIT here.
    session.run("CREATE TABLE r1e (a JSON)").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE r1e MODIFY COLUMN a BIT(1)"),
        Err(DriverError::UnsupportedModifyColumnType { .. })
    ));

    session.run("CREATE TABLE r1c (a INT)").unwrap();
    assert!(session
        .run("ALTER TABLE r1c MODIFY COLUMN a BIGINT")
        .is_ok());

    session.run("CREATE TABLE r1p (a JSON)").unwrap();
    session
        .run(r#"INSERT INTO r1p VALUES ('{"x":1}')"#)
        .unwrap();
    assert!(matches!(
        session.run("ALTER TABLE r1p MODIFY COLUMN a BIT(1)"),
        Err(DriverError::UnsupportedModifyColumnType { .. })
    ));
}

#[test]
fn modify_column_type_pair_gate_rule_2_numeric_and_time_to_enum_set() {
    let mut session = Session::new();

    // Rule 2 origin set: date/datetime/timestamp, TIME, YEAR, DECIMAL,
    // FLOAT, DOUBLE, JSON, BIT. Target: ENUM/SET. Using FLOAT -> ENUM here.
    session.run("CREATE TABLE r2e (a FLOAT)").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE r2e MODIFY COLUMN a ENUM('x','y')"),
        Err(DriverError::UnsupportedModifyColumnType { .. })
    ));

    session.run("CREATE TABLE r2c (a INT)").unwrap();
    assert!(session
        .run("ALTER TABLE r2c MODIFY COLUMN a BIGINT")
        .is_ok());

    session.run("CREATE TABLE r2p (a FLOAT)").unwrap();
    session.run("INSERT INTO r2p VALUES (3.14)").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE r2p MODIFY COLUMN a ENUM('x','y')"),
        Err(DriverError::UnsupportedModifyColumnType { .. })
    ));
}

#[test]
fn modify_column_type_pair_gate_rule_3_enum_set_bit_decimal_float_double_to_time() {
    let mut session = Session::new();

    // Rule 3 origin set: ENUM, SET, BIT, DECIMAL, FLOAT, DOUBLE. Target:
    // date/datetime/timestamp. Using FLOAT -> DATE here. DURATION is
    // deliberately NOT a target of this rule (that is rule 5's job), and
    // YEAR is deliberately NOT an origin of this rule.
    session.run("CREATE TABLE r3e (a FLOAT)").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE r3e MODIFY COLUMN a DATE"),
        Err(DriverError::UnsupportedModifyColumnType { .. })
    ));

    session.run("CREATE TABLE r3c (a INT)").unwrap();
    assert!(session
        .run("ALTER TABLE r3c MODIFY COLUMN a BIGINT")
        .is_ok());

    session.run("CREATE TABLE r3p (a FLOAT)").unwrap();
    session.run("INSERT INTO r3p VALUES (3.14)").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE r3p MODIFY COLUMN a DATE"),
        Err(DriverError::UnsupportedModifyColumnType { .. })
    ));
}

/// Rule 4 (`TypeTiDBVectorFloat32` on either side) has no SQL-level pin: this
/// tier's `column_type_code` does not accept the `VECTOR` type name yet, so
/// no statement can build a `FieldType` carrying that code in the first
/// place. It is pinned directly against `check_type_change_supported` in
/// `tidb-executor`'s `ddl::alter_table::type_change_gate_tests` module
/// instead (`vector_type_is_refused_on_either_side`).
#[test]
fn modify_column_type_pair_gate_rule_5_enum_set_bit_to_duration() {
    let mut session = Session::new();

    // Rule 5 origin set: ENUM, SET, BIT. Target: TIME (DURATION). Using
    // BIT -> TIME here.
    session.run("CREATE TABLE r5e (a BIT(8))").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE r5e MODIFY COLUMN a TIME"),
        Err(DriverError::UnsupportedModifyColumnType { .. })
    ));

    session.run("CREATE TABLE r5c (a INT)").unwrap();
    assert!(session
        .run("ALTER TABLE r5c MODIFY COLUMN a BIGINT")
        .is_ok());

    session.run("CREATE TABLE r5p (a BIT(8))").unwrap();
    session.run("INSERT INTO r5p VALUES (b'1')").unwrap();
    assert!(matches!(
        session.run("ALTER TABLE r5p MODIFY COLUMN a TIME"),
        Err(DriverError::UnsupportedModifyColumnType { .. })
    ));
}
