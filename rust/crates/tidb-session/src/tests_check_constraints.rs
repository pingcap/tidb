//! `CHECK` constraints under `tidb_enable_check_constraint`, split out of
//! `tests_show` when that file passed the repository's 2200-line ceiling.
//!
//! TiDB with the variable OFF parses a `CHECK`, discards it, and warns --
//! it does not store the constraint unenforced. These pin that whole
//! shape, including the ALTER and grouped-ADD-COLUMN forms.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// A `CHECK` constraint with `tidb_enable_check_constraint` at its OFF
/// default. Every expectation is captured from real TiDB -- the
/// `SHOW CREATE TABLE` text through `rust/difftests/gorun`, the warning
/// through testkit's `SHOW WARNINGS`, the insert outcome through both:
///
/// ```text
/// create table ck (a int, check (a > 0))     -- OK, Warning 1105
/// show create table ck                       -- NO `CONSTRAINT ... CHECK` clause
/// insert into ck values (-1)                 -- OK; the constraint is gone
/// select constraint_name from information_schema.check_constraints  -- empty
/// ```
///
/// TiDB DISCARDS the constraint rather than storing it unenforced, so
/// discarding it here is faithful: storing it would make this very
/// `SHOW CREATE TABLE` grow a clause TiDB does not print.
#[test]
fn a_check_constraint_is_accepted_discarded_and_warned_about() {
    let mut session = Session::new();
    let create_table_text = |session: &mut Session, name: &str| match session
        .run_with_columns(&format!("SHOW CREATE TABLE {name}"))
        .unwrap()
    {
        StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    };
    let warnings = |session: &Session| {
        session
            .warnings()
            .iter()
            .map(|w| (w.code, w.message.clone()))
            .collect::<Vec<_>>()
    };
    let is_off = || (1105u16, "tidb_enable_check_constraint is off".to_owned());

    // A table-level CHECK, named and unnamed: each is accepted, warns once,
    // and NEITHER reaches the restored DDL.
    session
        .run("create table ck (a int, b int, check (a > 0), constraint c2 check (b > 0))")
        .unwrap();
    // One warning per discarded constraint, matching Go's per-constraint
    // `AppendWarning`.
    assert_eq!(warnings(&session), vec![is_off(), is_off()]);
    assert_eq!(
        create_table_text(&mut session, "ck"),
        "CREATE TABLE `ck` (\n  \
             `a` int DEFAULT NULL,\n  \
             `b` int DEFAULT NULL\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // The form written inline on a column takes the same path.
    session
        .run("create table ck3 (a int check (a > 5), b int)")
        .unwrap();
    assert_eq!(warnings(&session), vec![is_off()]);
    assert_eq!(
        create_table_text(&mut session, "ck3"),
        "CREATE TABLE `ck3` (\n  \
             `a` int DEFAULT NULL,\n  \
             `b` int DEFAULT NULL\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );

    // `[NOT] ENFORCED` changes nothing while the variable is off: TiDB
    // discards the constraint before the keyword could matter.
    session
        .run("create table ck4 (a int, check (a > 0) not enforced)")
        .unwrap();
    session
        .run("create table ck5 (a int, check (a > 0) enforced)")
        .unwrap();
    assert_eq!(warnings(&session), vec![is_off()]);

    // The constraint really is gone, so a violating row inserts.
    session.run("insert into ck values (-1, -1)").unwrap();
    assert_eq!(
        query_text(&mut session, "select a, b from ck").1,
        vec![vec!["-1".to_owned(), "-1".to_owned()]]
    );
}

/// Turning `tidb_enable_check_constraint` ON stores, prints, and enforces the
/// constraint through the ordinary table write path, matching Go.
#[test]
fn a_check_constraint_is_stored_and_enforced_when_the_variable_is_on() {
    let mut session = Session::new();
    session
        .run("set @@global.tidb_enable_check_constraint = 1")
        .unwrap();
    session
        .run("create table ck (a int, check (a > 0), constraint loose check (a < 10) not enforced)")
        .unwrap();
    let shown = show_create(&mut session, "ck");
    assert_eq!(
        shown,
        "CREATE TABLE `ck` (\n  \
             `a` int DEFAULT NULL,\n  \
             CONSTRAINT `ck_chk_1` CHECK ((`a` > 0)),\n  \
             CONSTRAINT `loose` CHECK ((`a` < 10)) /*!80016 NOT ENFORCED */\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    );
    session.run("insert into ck values (1)").unwrap();
    let violation = session
        .run("insert into ck values (-1)")
        .expect_err("the enforced CHECK rejects false")
        .to_mysql_error();
    assert_eq!(
        (violation.code, violation.message.as_str()),
        (3819, "Check constraint 'ck_chk_1' is violated.")
    );
    // NULL satisfies a CHECK and NOT ENFORCED metadata never enters the
    // writable-constraint list.
    session.run("insert into ck values (null)").unwrap();
}

/// Go `pkg/ddl/constraint_test.go` covers ADD rollback on existing-row
/// violation, enforcement changes, and DROP. A failed validation must leave
/// the previously public metadata untouched.
#[test]
fn alter_check_constraint_validates_atomically_and_changes_enforcement() {
    let mut session = Session::new();
    session
        .run("set @@global.tidb_enable_check_constraint = 1")
        .unwrap();
    session.run("create table ac (a int)").unwrap();
    session.run("insert into ac values (12), (1)").unwrap();

    let add_error = session
        .run("alter table ac add constraint c1 check (a > 10)")
        .expect_err("the existing value 1 violates c1")
        .to_mysql_error();
    assert_eq!(
        (add_error.code, add_error.message.as_str()),
        (3819, "Check constraint 'c1' is violated.")
    );
    assert!(!show_create(&mut session, "ac").contains("CONSTRAINT"));

    session.run("delete from ac where a = 1").unwrap();
    session
        .run("alter table ac add constraint c1 check (a > 10)")
        .unwrap();
    assert!(show_create(&mut session, "ac").contains("CONSTRAINT `c1` CHECK ((`a` > 10))"));
    let write_error = session
        .run("insert into ac values (1)")
        .expect_err("a public enforced CHECK guards writes")
        .to_mysql_error();
    assert_eq!(write_error.code, 3819);

    session
        .run("alter table ac alter constraint c1 not enforced")
        .unwrap();
    assert!(show_create(&mut session, "ac").contains("/*!80016 NOT ENFORCED */"));
    session.run("insert into ac values (1)").unwrap();
    let enforce_error = session
        .run("alter table ac alter constraint c1 enforced")
        .expect_err("the row inserted while disabled prevents enforcement")
        .to_mysql_error();
    assert_eq!(enforce_error.code, 3819);
    assert!(show_create(&mut session, "ac").contains("/*!80016 NOT ENFORCED */"));

    session.run("delete from ac where a = 1").unwrap();
    session
        .run("alter table ac alter constraint c1 enforced")
        .unwrap();
    assert!(!show_create(&mut session, "ac").contains("NOT ENFORCED"));
    session.run("alter table ac drop constraint c1").unwrap();
    assert!(!show_create(&mut session, "ac").contains("CONSTRAINT"));
    session.run("insert into ac values (1)").unwrap();

    let missing = session
        .run("alter table ac drop constraint c1")
        .expect_err("a dropped CHECK no longer resolves")
        .to_mysql_error();
    assert_eq!(
        (missing.code, missing.message.as_str()),
        (3940, "Constraint 'c1' does not exist.")
    );
}

/// `ALTER TABLE`'s three `CHECK`-constraint actions with
/// `tidb_enable_check_constraint` at its OFF default, all captured from real
/// TiDB through `gorun`:
///
/// ```text
/// create table t3 (a int)
/// alter table t3 add constraint cc check (a > 0)      -- OK
/// show warnings          -- Warning|1105|tidb_enable_check_constraint is off
/// show create table t3   -- UNCHANGED: no CONSTRAINT clause
/// insert into t3 values (-1)                          -- OK, nothing enforces
/// alter table e alter constraint nope not enforced    -- OK
/// show warnings          -- Warning|1105|tidb_enable_check_constraint is off
/// alter table e drop constraint nope                  -- ERR
/// show errors            -- Error|3940|Constraint 'nope' does not exist.
/// ```
///
/// The last two are Go's own asymmetry and are ported as measured: with the
/// variable OFF, `DROP CONSTRAINT` still resolves the name and fails, while
/// `ALTER CONSTRAINT` does not look it up at all.
///
/// `DROP CONSTRAINT` is also the only spelling for dropping a CHECK -- it is
/// NOT MySQL's generic constraint drop. Captured: `alter table c drop
/// constraint fk1` where `fk1` is a FOREIGN KEY answers 3940 and leaves the
/// key in place.
#[test]
fn altering_check_constraints_is_discarded_and_warned_about_while_the_variable_is_off() {
    let mut session = Session::new();
    let warnings = |session: &Session| {
        session
            .warnings()
            .iter()
            .map(|w| (w.code, w.message.clone()))
            .collect::<Vec<_>>()
    };
    let is_off = || (1105u16, "tidb_enable_check_constraint is off".to_owned());
    let create_table_text =
        |session: &mut Session| match session.run_with_columns("SHOW CREATE TABLE t3").unwrap() {
            StmtOutput::Rows { rows, .. } => datum_text(&rows[0][1]).unwrap(),
            other => panic!("expected rows, got {other:?}"),
        };

    session.run("create table t3 (a int)").unwrap();
    let before = create_table_text(&mut session);

    session
        .run("alter table t3 add constraint cc check (a > 0)")
        .unwrap();
    assert_eq!(warnings(&session), vec![is_off()]);
    assert_eq!(
        create_table_text(&mut session),
        before,
        "the constraint is discarded, so the restored DDL cannot change"
    );
    // Nothing enforces it, which is the half a stored-but-unenforced
    // constraint would get wrong.
    session.run("insert into t3 values (-1)").unwrap();

    session
        .run("alter table t3 alter constraint nope not enforced")
        .expect("Go does not resolve the name here");
    assert_eq!(warnings(&session), vec![is_off()]);

    let missing = session
        .run("alter table t3 drop constraint nope")
        .expect_err("Go DOES resolve the name here")
        .to_mysql_error();
    assert_eq!(
        (missing.code, missing.message.as_str()),
        (3940, "Constraint 'nope' does not exist.")
    );
}

/// A grouped `ADD COLUMN (...)` has its own CHECK nodes rather than an
/// `AddCheck` action, but Go still emits one off-model warning per check.
#[test]
fn a_grouped_add_column_check_is_discarded_and_warned_about() {
    let mut session = Session::new();
    session.run("CREATE TABLE grouped_check (a INT)").unwrap();
    session
        .run("ALTER TABLE grouped_check ADD (b INT, CONSTRAINT cb CHECK (b > 0))")
        .unwrap();
    assert_eq!(
        session
            .warnings()
            .iter()
            .map(|warning| (warning.code, warning.message.as_str()))
            .collect::<Vec<_>>(),
        vec![(1105, "tidb_enable_check_constraint is off")]
    );
    session
        .run("INSERT INTO grouped_check VALUES (1, -1)")
        .unwrap();
}

/// Pinned Go `CreateNewColumn` calls `buildColumnAndConstraint` but keeps only
/// the column and discards the returned inline constraints. This differs from
/// both CREATE TABLE column checks and grouped table-level ADD CHECK actions.
#[test]
fn an_inline_add_column_check_is_discarded_even_while_enabled() {
    let mut session = Session::new();
    session
        .run("set @@global.tidb_enable_check_constraint = 1")
        .unwrap();
    session.run("create table add_inline (a int)").unwrap();
    session
        .run("alter table add_inline add column b int check (b > 0)")
        .unwrap();

    assert!(!show_create(&mut session, "add_inline").contains("CONSTRAINT"));
    session
        .run("insert into add_inline values (1, -1)")
        .expect("the discarded inline CHECK cannot guard writes");
}

/// A system variable whose assignment Go CLAMPS rather than refuses reports
/// `1292 Truncated incorrect <name> value: '<original>'`, and the value that
/// lands is the clamped one.
///
/// Every row is transcribed from the recorded `SHOW WARNINGS` blocks in
/// `tests/integrationtest/r/session/variable.result`. Note the pairing each
/// row asserts: the STORED value is the clamp, while the WARNING names the
/// value exactly as typed.
///
/// The multibyte alias is the load-bearing row. `中文测试` plus one digit is 5
/// CHARACTERS but 13 BYTES, so thirteen such groups are 65 characters and 169
/// bytes. Go cuts at 64 RUNES, dropping exactly the final `c`; a byte-wise cut
/// would land inside the fifth group -- and inside a UTF-8 sequence. The
/// stored value alone separates the two rules.
#[test]
fn clamping_a_system_variable_warns_1292_with_the_original_value() {
    let alias_65: String = (1..=13)
        .map(|i| format!("中文测试{}", "1234567890abc".chars().nth(i - 1).unwrap()))
        .collect();
    assert_eq!(alias_65.chars().count(), 65);
    assert_eq!(alias_65.len(), 169);
    let alias_64: String = alias_65.chars().take(64).collect();
    let digits_70 = "0123456789".repeat(7);
    let digits_64: String = digits_70.chars().take(64).collect();
    let spaced = format!("abc{}1", " ".repeat(68));

    let cases: Vec<(String, &str, String, String)> = vec![
        (
            "set @@global.tidb_memory_usage_alarm_ratio=1.1".to_owned(),
            "@@global.tidb_memory_usage_alarm_ratio",
            "1".to_owned(),
            "tidb_memory_usage_alarm_ratio value: '1.1'".to_owned(),
        ),
        (
            "set @@global.tidb_memory_usage_alarm_ratio=-1".to_owned(),
            "@@global.tidb_memory_usage_alarm_ratio",
            "0".to_owned(),
            "tidb_memory_usage_alarm_ratio value: '-1'".to_owned(),
        ),
        (
            "set @@global.tidb_memory_usage_alarm_keep_record_num=0".to_owned(),
            "@@global.tidb_memory_usage_alarm_keep_record_num",
            "1".to_owned(),
            "tidb_memory_usage_alarm_keep_record_num value: '0'".to_owned(),
        ),
        (
            "set @@global.tidb_memory_usage_alarm_keep_record_num=10001".to_owned(),
            "@@global.tidb_memory_usage_alarm_keep_record_num",
            "10000".to_owned(),
            "tidb_memory_usage_alarm_keep_record_num value: '10001'".to_owned(),
        ),
        (
            format!("set @@tidb_session_alias='{digits_70}'"),
            "@@tidb_session_alias",
            digits_64,
            format!("tidb_session_alias value: '{digits_70}'"),
        ),
        (
            format!("set @@tidb_session_alias='{alias_65}'"),
            "@@tidb_session_alias",
            alias_64,
            format!("tidb_session_alias value: '{alias_65}'"),
        ),
        (
            "set @@tidb_session_alias='abc  '".to_owned(),
            "@@tidb_session_alias",
            "abc".to_owned(),
            "tidb_session_alias value: 'abc  '".to_owned(),
        ),
        (
            format!("set @@tidb_session_alias='{spaced}'"),
            "@@tidb_session_alias",
            "abc".to_owned(),
            format!("tidb_session_alias value: '{spaced}'"),
        ),
        (
            "set @@group_concat_max_len=1".to_owned(),
            "@@group_concat_max_len",
            "4".to_owned(),
            "group_concat_max_len value: '1'".to_owned(),
        ),
    ];

    for (set, read, stored, warning) in cases {
        let mut session = Session::new();
        session.run(&set).unwrap();
        assert_eq!(
            row_text(session.run("SHOW WARNINGS")),
            vec![vec![
                "Warning".to_owned(),
                "1292".to_owned(),
                format!("Truncated incorrect {warning}"),
            ]],
            "warning for `{set}`"
        );
        assert_eq!(
            row_text(session.run(&format!("select {read}"))),
            vec![vec![stored]],
            "stored value for `{set}`"
        );
    }
}
