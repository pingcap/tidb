//! A view over a partitioned table sees through the union of partitions
//! and stays live across later partition re-routing (an UPDATE that moves
//! a row out of the view's filter removes it from the view).

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(v) => format!("i:{v}"),
                        tidb_datatype::Datum::Null => "Null".to_owned(),
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
        .run("create table pt (id int primary key, v int) partition by hash(id) partitions 2")
        .unwrap();
    session.run("create view vp as select id, v from pt where v > 5").unwrap();
    session.run("insert into pt values (1, 10), (2, 3), (3, 7)").unwrap();
}

#[test]
fn view_tracks_partition_rerouting() {
    let mut session = Session::new();
    setup(&mut session);

    // v > 5: rows 1 and 3 (across different partitions).
    assert_eq!(rows(&mut session, "select id, v from vp order by id"), "i:1|i:10;i:3|i:7");

    // Moving row 3's value below the filter removes it from the view.
    session.run("update pt set v = 1 where id = 3").unwrap();
    assert_eq!(rows(&mut session, "select id, v from vp order by id"), "i:1|i:10");
}
