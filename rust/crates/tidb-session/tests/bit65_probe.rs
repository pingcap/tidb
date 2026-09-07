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

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(_) => "OK".to_owned(),
        Err(e) => format!("ERR {}", &e.to_string()[..60.min(e.to_string().len())]),
    }
}

#[test]
fn probe_coalesce_partition() {
    let mut session = Session::new();
    session
        .run("create table t (id int primary key) partition by hash(id) partitions 4")
        .unwrap();
    session.run("insert into t values (1), (2), (3), (4)").unwrap();
    println!("coalesce: {}", try_sql(&mut session, "alter table t coalesce partition 2"));
    println!("count: {}", rows(&mut session, "select count(*) from t"));
    println!("all: {}", rows(&mut session, "select id from t order by id"));
}
