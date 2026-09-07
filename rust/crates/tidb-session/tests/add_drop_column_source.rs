//! `ALTER TABLE` column lifecycle: ADD COLUMN backfills existing rows with
//! the column default, `AFTER` positions the new column, and DROP COLUMN
//! removes it from every read.

use tidb_session::Session;

fn rows(session: &mut Session, sql: &str) -> String {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        tidb_datatype::Datum::String(s) => {
                            format!("'{}'", String::from_utf8_lossy(&s.bytes()))
                        }
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
fn column_add_backfill_position_and_drop() {
    let mut session = Session::new();
    session.run("create table t (a int primary key)").unwrap();
    session.run("insert into t values (1), (2)").unwrap();

    // ADD with a default backfills the existing rows.
    session.run("alter table t add column b int default 7").unwrap();
    assert_eq!(rows(&mut session, "select a, b from t order by a"), "1|7;2|7");

    // AFTER positions the column in the row layout.
    session
        .run("alter table t add column c varchar(3) not null default 'zz' after a")
        .unwrap();
    assert_eq!(rows(&mut session, "select * from t order by a limit 1"), "1|'zz'|7");

    // DROP COLUMN removes it.
    session.run("alter table t drop column b").unwrap();
    assert_eq!(rows(&mut session, "select * from t order by a"), "1|'zz';2|'zz'");
}
