use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::Null => "N".to_owned(),
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

fn seed(session: &mut Session) {
    session.run("create table t (a int)").unwrap();
    session.run("insert into t values (1), (5), (10), (NULL)").unwrap();
}

#[test]
fn probe_between_edges() {
    let mut session = Session::new();
    seed(&mut session);
    println!("normal: {}", rows(&mut session, "select a from t where a between 2 and 9"));
    println!("reversed: {}", rows(&mut session, "select a from t where a between 9 and 2"));
    println!("null bound: {}", rows(&mut session, "select a from t where a between 2 and NULL"));
    println!("not between: {}", rows(&mut session, "select a from t where a not between 2 and 9 order by a"));
}
