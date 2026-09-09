//! BINARY(4) zero-pads on store: `'a'` becomes 0x61000000 (`hex` output),
//! the 1-byte literal does NOT compare equal (`b = 'a'` is false), and the
//! explicit 4-byte form does (`b = 'a\0\0\0'`).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| format!("{d:?}"))
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn binary_pads_with_nul_bytes() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key, b binary(4))")
        .unwrap();
    session.run("insert into t values (1, 'a')").unwrap();

    // hex() returns the string "61000000"; the debug form shows its ASCII bytes.
    let hex_row = rows(&mut session, "select hex(b) from t");
    assert!(
        hex_row.contains("54, 49, 48, 48, 48, 48, 48, 48"),
        "{hex_row}"
    );
    assert_eq!(first_count(&mut session, "select count(*) from t where b = 'a'"), "Int(0)");
    assert_eq!(
        first_count(&mut session, r"select count(*) from t where b = 'a\0\0\0'"),
        "Int(1)"
    );
}

fn first_count(session: &mut Session, sql: &str) -> String {
    rows(session, sql)
}
