use tidb_session::Session;

#[test]
fn probe_prefix_meta() {
    let mut session = Session::new();
    session
        .run("create table t (a int primary key, s varchar(20), unique index uq (s(4)))")
        .unwrap();
    let shown = session
        .run("show index from t")
        .unwrap();
    if let tidb_session::StmtResult::Rows(rows) = shown {
        for row in rows {
            let cells: Vec<String> = row
                .iter()
                .map(|d| match d {
                    tidb_datatype::Datum::Int(i) => format!("{i}"),
                    tidb_datatype::Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                    tidb_datatype::Datum::String(s) => String::from_utf8_lossy(&s.bytes()).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect();
            println!("IDX: {cells:?}");
        }
    }
}
