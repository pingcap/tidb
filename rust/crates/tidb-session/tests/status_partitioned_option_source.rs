//! SHOW TABLE STATUS for a partitioned table: the Create_options cell
//! reports `partitioned`, distinguishing it from a plain table (whose
//! cell is empty).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("{other:?}"),
    }
}

fn create_options(session: &mut Session, like: &str) -> String {
    let row = &rows(session, &format!("show table status like '{like}'"))[0];
    let fields: Vec<&str> = row.split('|').map(str::trim).collect();
    fields[16].to_owned()
}

#[test]
fn partitioned_table_reports_partitioned_option() {
    let mut session = Session::new();

    session
        .run("create table plain (id int primary key)")
        .unwrap();
    session
        .run("create table pt (id int primary key) partition by hash(id) partitions 2")
        .unwrap();

    assert_eq!(create_options(&mut session, "plain"), "");
    assert_eq!(create_options(&mut session, "pt"), "partitioned");
}
