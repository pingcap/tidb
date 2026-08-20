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

/// Turning `tidb_enable_check_constraint` ON changes what a `CHECK`
/// constraint MEANS -- TiDB then stores it (auto-named `<table>_chk_<N>`),
/// prints it in `SHOW CREATE TABLE`, and enforces it with error 3819
/// (captured: "Check constraint 'ck3_chk_1' is violated."). None of that is
/// modelled, so the DDL is refused outright rather than silently discarding a
/// constraint the session just asked to have honoured.
#[test]
fn a_check_constraint_is_refused_when_the_variable_is_on() {
    let mut session = Session::new();
    session
        .run("set @@global.tidb_enable_check_constraint = 1")
        .unwrap();
    assert!(matches!(
        session.run("create table ck (a int, check (a > 0))"),
        Err(DriverError::Unsupported(reason)) if reason == "CHECK constraints are only modelled with tidb_enable_check_constraint off"
    ));
    // A table with no CHECK constraint is unaffected by the variable.
    session.run("create table plain (a int)").unwrap();
    // ALTER TABLE reaches the same gate rather than the generic "this ALTER
    // TABLE action is not supported yet" it used to answer -- what Go would
    // do here is STORE the constraint and validate the existing rows against
    // it, so a silent no-op would be the accept-then-discard shape.
    assert!(matches!(
        session.run("alter table plain add constraint cc check (a > 0)"),
        Err(DriverError::Unsupported(reason)) if reason == "CHECK constraints are only modelled with tidb_enable_check_constraint off"
    ));
    // `ALTER CONSTRAINT` is NOT in that gate. Captured with the variable ON:
    // `alter table d alter constraint nope enforced` -> `Error|3940|Constraint
    // 'nope' does not exist.` -- which this tier can always say, because no
    // table it holds can carry a CHECK constraint.
    let missing = session
        .run("alter table plain alter constraint nope enforced")
        .expect_err("3940")
        .to_mysql_error();
    assert_eq!(
        (missing.code, missing.message.as_str()),
        (3940, "Constraint 'nope' does not exist.")
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
