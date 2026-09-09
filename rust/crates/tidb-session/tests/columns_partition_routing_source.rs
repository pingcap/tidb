//! RANGE COLUMNS (varchar) and LIST COLUMNS (char) partition routing:
//! rows land by string comparison and list membership respectively, and
//! partition-qualified reads agree.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
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
fn string_typed_partition_columns_route() {
    let mut session = Session::new();

    session
        .run(
            "create table rc (id int, s varchar(8)) partition by range columns (s) \
             (partition pa values less than ('m'), partition pb values less than ('z'))",
        )
        .unwrap();
    session
        .run("insert into rc values (1, 'apple'), (2, 'xray')")
        .unwrap();
    assert_eq!(rows(&mut session, "select id from rc partition (pa)"), "1");
    assert_eq!(rows(&mut session, "select id from rc partition (pb)"), "2");

    session
        .run(
            "create table lc (id int, c char(2)) partition by list columns (c) \
             (partition px values in ('x'), partition py values in ('y'))",
        )
        .unwrap();
    session.run("insert into lc values (1, 'x'), (2, 'y')").unwrap();
    assert_eq!(rows(&mut session, "select id from lc partition (px)"), "1");
    assert_eq!(rows(&mut session, "select id from lc partition (py)"), "2");
}
