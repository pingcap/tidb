//! CI collation semantics versus a BINARY override: on a
//! utf8mb4_general_ci column, `=`, LIKE, and DISTINCT all fold case, and
//! `= BINARY 'apple'` restores byte-exact comparison (only the exact-case
//! row matches).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session
        .run("create table t (s varchar(8)) charset utf8mb4 collate utf8mb4_general_ci")
        .unwrap();
    session
        .run("insert into t values ('Apple'), ('apple'), ('APPLE')")
        .unwrap();
}

#[test]
fn ci_folding_with_binary_override() {
    let mut session = Session::new();
    setup(&mut session);

    // CI: `=`, LIKE, and DISTINCT all fold case.
    assert_eq!(rows(&mut session, "select s from t where s = 'apple'"), "'Apple';'apple';'APPLE'");
    assert_eq!(rows(&mut session, "select s from t where s like 'apple%'"), "'Apple';'apple';'APPLE'");
    assert_eq!(rows(&mut session, "select distinct s from t"), "'Apple'");

    // BINARY restores byte comparison: only the exact case matches.
    assert_eq!(rows(&mut session, "select s from t where s = binary 'apple'"), "'apple'");
}
