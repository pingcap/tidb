//! Column-scope grants: the four privileges that accept a column list, how
//! `SHOW GRANTS` merges them into one line, and the refusals -- Go
//! `mysql.columns_priv`.

use crate::tests_support::*;
use crate::*;

/// CAPTURED (`pkg/executor/zz_dump_colgrant_test.go`): only
/// `mysql.AllColumnPrivs` -- `SELECT`, `INSERT`, `UPDATE`, `REFERENCES` --
/// may carry a column list, plus `ALL`, which expands to all four. Every
/// other privilege, `GRANT OPTION` included, is `ErrWrongUsage`/1221.
#[test]
fn only_the_four_column_privileges_accept_a_column_list() {
    let mut session = session_with_privileges();
    session
        .run("CREATE TABLE test.t (a int, b int, c int)")
        .unwrap();
    session.run("CREATE USER 'u'@'%'").unwrap();

    for privilege in ["SELECT", "INSERT", "UPDATE", "REFERENCES", "ALL"] {
        session
            .run(&format!("GRANT {privilege} (a) ON test.t TO 'u'@'%'"))
            .unwrap();
    }
    for privilege in ["DELETE", "DROP", "ALTER", "INDEX", "CREATE", "GRANT OPTION"] {
        assert!(
            matches!(
                session.run(&format!("GRANT {privilege} (a) ON test.t TO 'u'@'%'")),
                Err(DriverError::ColumnGrantNonColumnPriv)
            ),
            "{privilege} (a) should be refused"
        );
    }

    // `ALL (a)` covers exactly the four, in `mysql.AllColumnPrivs` order.
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u'@'%'".to_owned()],
            vec![
                "GRANT SELECT(a), INSERT(a), UPDATE(a), REFERENCES(a) ON `test`.`t` TO 'u'@'%'"
                    .to_owned()
            ],
        ]
    );

    // A TABLE-level REVOKE never touches the column rows (CAPTURED: after
    // `REVOKE ALL ON cg.t`, the column line is unchanged).
    session.run("REVOKE ALL ON test.t FROM 'u'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u'@'%'"))[1],
        vec![
            "GRANT SELECT(a), INSERT(a), UPDATE(a), REFERENCES(a) ON `test`.`t` TO 'u'@'%'"
                .to_owned()
        ]
    );
}

/// CAPTURED: one `SHOW GRANTS` line per table carries every column
/// privilege, each with its own parenthesised list. Repeated grants MERGE
/// into that line, and the columns keep the order their `mysql.columns_priv`
/// rows were inserted in -- `SELECT` on `b`, then `a`, then `c` prints
/// `SELECT(b, a, c)`, NOT the sorted list.
#[test]
fn column_grants_merge_into_one_line_in_row_order() {
    let mut session = session_with_privileges();
    session
        .run("CREATE TABLE test.t (a int, b int, c int)")
        .unwrap();
    session.run("CREATE USER 'u'@'%'").unwrap();

    session
        .run("GRANT SELECT (a), INSERT (a,b) ON test.t TO 'u'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u'@'%'"))[1],
        vec!["GRANT SELECT(a), INSERT(a, b) ON `test`.`t` TO 'u'@'%'".to_owned()]
    );

    session.run("CREATE USER 'u2'@'%'").unwrap();
    session
        .run("GRANT SELECT (b) ON test.t TO 'u2'@'%'")
        .unwrap();
    session
        .run("GRANT SELECT (a) ON test.t TO 'u2'@'%'")
        .unwrap();
    session
        .run("GRANT SELECT (c) ON test.t TO 'u2'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u2'@'%'"))[1],
        vec!["GRANT SELECT(b, a, c) ON `test`.`t` TO 'u2'@'%'".to_owned()]
    );

    // Revoking ONE column drops just that column's row; the rest keep
    // their order.
    session
        .run("REVOKE SELECT (b) ON test.t FROM 'u2'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u2'@'%'"))[1],
        vec!["GRANT SELECT(a, c) ON `test`.`t` TO 'u2'@'%'".to_owned()]
    );

    // The TABLE form of the same REVOKE leaves the column rows alone.
    session
        .run("REVOKE SELECT ON test.t FROM 'u2'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u2'@'%'"))[1],
        vec!["GRANT SELECT(a, c) ON `test`.`t` TO 'u2'@'%'".to_owned()]
    );

    // Column names resolve against the table, so the table's own spelling
    // is what prints back (CAPTURED: `GRANT SELECT (A)` -> `SELECT(a)`).
    session.run("CREATE USER 'u5'@'%'").unwrap();
    session
        .run("GRANT SELECT (A) ON test.t TO 'u5'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u5'@'%'"))[1],
        vec!["GRANT SELECT(a) ON `test`.`t` TO 'u5'@'%'".to_owned()]
    );
}

/// CAPTURED: table-level and column-level privileges on the SAME table are
/// two separate `SHOW GRANTS` lines, the column line following the table
/// one. `WITH GRANT OPTION` on a column grant lands on the TABLE row, which
/// is why it surfaces as a `GRANT USAGE ... WITH GRANT OPTION` table line
/// rather than on the column line.
#[test]
fn table_and_column_grants_on_one_table_print_as_two_lines() {
    let mut session = session_with_privileges();
    session
        .run("CREATE TABLE test.t (a int, b int, c int)")
        .unwrap();
    session.run("CREATE USER 'u4'@'%'").unwrap();

    session
        .run("GRANT SELECT, INSERT (a), UPDATE ON test.t TO 'u4'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u4'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u4'@'%'".to_owned()],
            vec!["GRANT SELECT,UPDATE ON `test`.`t` TO 'u4'@'%'".to_owned()],
            vec!["GRANT INSERT(a) ON `test`.`t` TO 'u4'@'%'".to_owned()],
        ]
    );

    session
        .run("GRANT SELECT (c) ON test.t TO 'u4'@'%'")
        .unwrap();
    session.run("GRANT ALL ON test.t TO 'u4'@'%'").unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u4'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u4'@'%'".to_owned()],
            vec!["GRANT ALL PRIVILEGES ON `test`.`t` TO 'u4'@'%'".to_owned()],
            vec!["GRANT SELECT(c), INSERT(a) ON `test`.`t` TO 'u4'@'%'".to_owned()],
        ]
    );

    session.run("CREATE USER 'u5'@'%'").unwrap();
    session
        .run("GRANT SELECT (a) ON test.t TO 'u5'@'%' WITH GRANT OPTION")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u5'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u5'@'%'".to_owned()],
            vec!["GRANT USAGE ON `test`.`t` TO 'u5'@'%' WITH GRANT OPTION".to_owned()],
            vec!["GRANT SELECT(a) ON `test`.`t` TO 'u5'@'%'".to_owned()],
        ]
    );
}

/// CAPTURED: a `GRANT` naming a column the table does not have is Go's plain
/// `Unknown column: <name>`, raised at GRANT time so nothing is stored. A
/// `REVOKE` checks the account's TABLE row FIRST, so the same bad column
/// reports the table-level "no such grant" instead.
#[test]
fn a_column_that_does_not_exist_is_refused_at_grant_time() {
    let mut session = session_with_privileges();
    session
        .run("CREATE TABLE test.t (a int, b int, c int)")
        .unwrap();
    session.run("CREATE USER 'u3'@'%'").unwrap();

    assert!(matches!(
        session.run("GRANT SELECT (nope) ON test.t TO 'u3'@'%'"),
        Err(DriverError::UnknownGrantColumn(ref column)) if column == "nope"
    ));
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u3'@'%'")),
        [vec!["GRANT USAGE ON *.* TO 'u3'@'%'".to_owned()]]
    );

    assert!(matches!(
        session.run("GRANT SELECT (a) ON test.nosuchtable TO 'u3'@'%'"),
        Err(DriverError::Schema(SchemaErrorKind::UnknownTable(ref name)))
            if name == "test.nosuchtable"
    ));

    for statement in [
        "REVOKE SELECT (nope) ON test.t FROM 'u3'@'%'",
        "REVOKE SELECT (a) ON test.t FROM 'u3'@'%'",
    ] {
        assert!(
            matches!(
                session.run(statement),
                Err(DriverError::RevokeNoTableGrant { ref table, .. }) if table == "t"
            ),
            "{statement} should report the table-level missing grant"
        );
    }

    // Once the account holds a grant on the table, the column IS resolved.
    session
        .run("GRANT SELECT (a) ON test.t TO 'u3'@'%'")
        .unwrap();
    assert!(matches!(
        session.run("REVOKE SELECT (nope) ON test.t FROM 'u3'@'%'"),
        Err(DriverError::UnknownGrantColumn(ref column)) if column == "nope"
    ));
}

/// CAPTURED: a role's column grants merge into the account's line under the
/// ACCOUNT's name once the role is active, exactly like its DB- and
/// TABLE-scope rows. Without the role active, the account's own
/// `SHOW GRANTS` shows only the role edge.
#[test]
fn an_active_role_contributes_its_column_grants() {
    let mut session = session_with_privileges();
    session
        .run("CREATE TABLE test.t (a int, b int, c int)")
        .unwrap();
    session.run("CREATE USER 'u3'@'%'").unwrap();
    session.run("CREATE ROLE 'r1'@'%'").unwrap();
    session
        .run("GRANT SELECT (a,b) ON test.t TO 'r1'@'%'")
        .unwrap();
    session.run("GRANT 'r1'@'%' TO 'u3'@'%'").unwrap();

    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u3'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u3'@'%'".to_owned()],
            vec!["GRANT 'r1'@'%' TO 'u3'@'%'".to_owned()],
        ]
    );
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u3'@'%' USING 'r1'@'%'")),
        [
            vec!["GRANT USAGE ON *.* TO 'u3'@'%'".to_owned()],
            vec!["GRANT SELECT(a, b) ON `test`.`t` TO 'u3'@'%'".to_owned()],
            vec!["GRANT 'r1'@'%' TO 'u3'@'%'".to_owned()],
        ]
    );
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'r1'@'%'"))[1],
        vec!["GRANT SELECT(a, b) ON `test`.`t` TO 'r1'@'%'".to_owned()]
    );
}

/// CAPTURED: `GRANT USAGE (a)` is accepted and grants nothing -- Go writes an
/// empty `mysql.columns_priv` row that `SHOW GRANTS` never prints. It still
/// creates the TABLE row, which is what lets a later `REVOKE` on that table
/// get past its "no such grant" check.
#[test]
fn usage_with_a_column_list_grants_nothing_but_creates_the_table_row() {
    let mut session = session_with_privileges();
    session
        .run("CREATE TABLE test.t (a int, b int, c int)")
        .unwrap();
    session.run("CREATE USER 'u7'@'%'").unwrap();

    session
        .run("GRANT USAGE (a) ON test.t TO 'u7'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u7'@'%'")),
        [vec!["GRANT USAGE ON *.* TO 'u7'@'%'".to_owned()]]
    );
    session
        .run("REVOKE SELECT (a) ON test.t FROM 'u7'@'%'")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW GRANTS FOR 'u7'@'%'")),
        [vec!["GRANT USAGE ON *.* TO 'u7'@'%'".to_owned()]]
    );
}
