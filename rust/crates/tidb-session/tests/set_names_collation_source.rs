//! SET NAMES switches the connection charset/collation as a unit and the
//! collation_connection variable reflects it. Column-level INVISIBLE
//! remains absent from the oracle grammar (refused), and an unknown charset
//! name is refused as well.

use tidb_session::Session;

use crate::support::byte_rows_joined as rows;

#[test]
fn set_names_updates_collation_connection() {
    let mut session = Session::new();

    session.run("set names utf8mb4 collate utf8mb4_general_ci").unwrap();

    let value = rows(&mut session, "show variables like 'collation_connection'");
    // "collation_connection|utf8mb4_general_ci"
    assert!(value.contains("utf8mb4_general_ci"), "{value}");
}

#[test]
fn unknown_charset_name_refuses() {
    let mut session = Session::new();

    let error = session
        .run("set names not_a_charset")
        .expect_err("unknown charset")
        .to_string();
    assert!(!error.is_empty(), "{error}");
}

/// The four connection charset variables, joined with `|`.
fn connection_variables(session: &mut Session) -> String {
    let sql = "select @@character_set_client, @@character_set_connection, \
               @@character_set_results, @@collation_connection";
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows[0]
            .iter()
            .map(|datum| match datum {
                tidb_datatype::Datum::String(value) => {
                    String::from_utf8_lossy(value.bytes()).into_owned()
                }
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
            .join("|"),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Go `SetExecutor.setCharset` resolves a named collation through
/// `collate.GetCollationByName`, and `checkCollation` does the same for the
/// collation variables, so a collation the new framework does not implement
/// is 1273 on every path. Go's `executor/charset.test` records all of these.
#[test]
fn an_unsupported_collation_is_refused_by_set_names_and_every_collation_variable() {
    let mut session = Session::new();
    for sql in [
        "set names utf8 collate utf8_roman_ci",
        "set session collation_server = 'utf8_roman_ci'",
        "set session collation_database = 'utf8_roman_ci'",
        "set session collation_connection = 'utf8_roman_ci'",
    ] {
        let error = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 1273, "{sql}");
        assert_eq!(
            error.message,
            "Unsupported collation when new collation is enabled: 'utf8_roman_ci'",
            "{sql}"
        );
    }
    assert_eq!(
        connection_variables(&mut session),
        "utf8mb4|utf8mb4|utf8mb4|utf8mb4_bin"
    );
}

/// Go `setCharset`: a named collation must belong to the named charset.
#[test]
fn set_names_refuses_a_collation_of_another_charset() {
    let mut session = Session::new();
    let error = session
        .run("set names latin1 collate utf8mb4_bin")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1253);
    assert_eq!(
        error.message,
        "COLLATION 'utf8mb4_bin' is not valid for CHARACTER SET 'latin1'"
    );
    assert_eq!(
        connection_variables(&mut session),
        "utf8mb4|utf8mb4|utf8mb4|utf8mb4_bin"
    );
}

/// Go `setCharset`: `SET NAMES utf8mb4` without a collation takes the
/// session's `default_collation_for_utf8mb4`, not the charset's registry
/// default.
#[test]
fn set_names_utf8mb4_takes_the_sessions_default_utf8mb4_collation() {
    let mut session = Session::new();
    session
        .run("set default_collation_for_utf8mb4 = 'utf8mb4_general_ci'")
        .unwrap();
    session.run("set names utf8mb4").unwrap();
    assert_eq!(
        connection_variables(&mut session),
        "utf8mb4|utf8mb4|utf8mb4|utf8mb4_general_ci"
    );
}

/// Go `setCharset` for `SET CHARACTER SET`: the client and results charsets
/// take the named charset, and the connection pair comes from the GLOBAL
/// database charset and collation. Go's `executor/set.test` records
/// `character_set_connection` as `utf8mb4` after `SET CHARACTER SET latin1`.
#[test]
fn set_character_set_takes_the_connection_pair_from_the_global_database_charset() {
    let mut session = Session::new();
    session.run("set character set latin1").unwrap();
    assert_eq!(
        connection_variables(&mut session),
        "latin1|utf8mb4|latin1|utf8mb4_bin"
    );
    session.run("set charset default").unwrap();
    assert_eq!(
        connection_variables(&mut session),
        "utf8mb4|utf8mb4|utf8mb4|utf8mb4_bin"
    );
}
