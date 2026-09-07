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

#[test]
fn probe_alter_column_default() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, b int default 1)").unwrap();
    session.run("insert into t (a) values (1)").unwrap();
    session.run("alter table t alter column b set default 42").unwrap();
    println!("existing: {}", rows(&mut session, "select a, b from t"));
    session.run("insert into t (a) values (2)").unwrap();
    println!("new: {}", rows(&mut session, "select a, b from t order by a"));
    session.run("alter table t alter column b drop default").unwrap();
    session.run("insert into t (a) values (3)").unwrap();
    println!("after drop: {}", rows(&mut session, "select a, b from t order by a"));
    // NOT NULL without default under strict: still 1364
    session.run("create table n (a int primary key, d int not null)").unwrap();
    match session.run("insert into n (a) values (1)") {
        Ok(_) => println!("notnull => OK"),
        Err(e) => println!("notnull => ERR {e}"),
    }
}
