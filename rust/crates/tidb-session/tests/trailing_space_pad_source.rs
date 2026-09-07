//! utf8mb4_bin stays a PAD-SPACE collation in TiDB (unlike MySQL 8's NO
//! PAD): `'a ' = 'a'` is true and `'a ' > 'a'` false; CHAR strips trailing
//! spaces on retrieval; VARCHAR compares padded.

use tidb_session::Session;

fn first(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => format!("{:?}", rows[0][0]),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn pad_space_semantics() {
    let mut session = Session::new();

    // Literals compare padded: equal, not greater.
    assert_eq!(first(&mut session, "select 'a ' = 'a'"), "Int(1)");
    assert_eq!(first(&mut session, "select 'a ' > 'a'"), "Int(0)");

    // CHAR(4): trailing spaces stripped on retrieval.
    session.run("create table t (v char(4))").unwrap();
    session.run("insert into t values ('a ')").unwrap();
    assert_eq!(first(&mut session, "select count(*) from t where v = 'a'"), "Int(1)");

    // VARCHAR: padded comparison still matches.
    session.run("create table t2 (v varchar(8))").unwrap();
    session.run("insert into t2 values ('a ')").unwrap();
    assert_eq!(first(&mut session, "select count(*) from t2 where v = 'a'"), "Int(1)");
}
