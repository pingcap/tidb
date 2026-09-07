//! Identifier resolution: table and column names are CASE-INSENSITIVE
//! across every spelling (created `MixedCase`/`ColA`, accessed as `cola`,
//! `COLA`, backticked `` `ColA` ``), and backquotes admit RESERVED WORDS as
//! identifiers (`select`/`from` as column names).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join(";"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn identifiers_resolve_case_insensitively() {
    let mut session = Session::new();
    session
        .run("create table MixedCase (ColA int primary key)")
        .unwrap();

    // Every case spelling of the DML resolves to the same objects.
    session.run("insert into mixedcase (cola) values (7)").unwrap();
    session.run("INSERT INTO MIXEDCASE (COLA) VALUES (8)").unwrap();

    assert_eq!(
        rows(&mut session, "select cola from mixedcase order by cola"),
        "7;8"
    );
    assert_eq!(
        rows(&mut session, "select `ColA` from `MixedCase` order by `ColA`"),
        "7;8"
    );
    assert_eq!(
        rows(&mut session, "SELECT COLA FROM MIXEDCASE ORDER BY COLA"),
        "7;8"
    );
}

#[test]
fn reserved_words_work_as_backquoted_identifiers() {
    let mut session = Session::new();
    session
        .run("create table res_t (`select` int primary key, `from` int)")
        .unwrap();
    session
        .run("insert into res_t (`select`, `from`) values (7, 8)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "select `select`, `from` from res_t"),
        "7|8"
    );
}
