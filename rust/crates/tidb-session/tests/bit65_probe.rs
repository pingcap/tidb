use tidb_session::Session;

fn strings(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(bytes) => {
                            String::from_utf8_lossy(bytes).into_owned()
                        }
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .collect::<Vec<_>>()
            .join("\n"),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

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
fn probe_invisible_column() {
    let mut session = Session::new();
    match session.run("create table t (a int, b int invisible)") {
        Ok(_) => println!("create => OK"),
        Err(e) => println!("create => ERR {e}"),
    }
    let shown = strings(&mut session, "show create table t");
    println!("has INVISIBLE: {}", shown.contains("INVISIBLE"));
    // invisible column excluded from SELECT *
    match session.run("insert into t (a) values (1)").unwrap() {
        _ => {}
    }
    println!("select *: {}", rows(&mut session, "select * from t"));
    // but directly selectable
    println!("direct: {}", rows(&mut session, "select b from t"));
    // SHOW COLUMNS Extra
    let cols = strings(&mut session, "show columns from t");
    println!("cols: {cols}");
}
