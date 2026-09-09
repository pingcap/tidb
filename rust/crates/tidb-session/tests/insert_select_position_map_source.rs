//! INSERT SELECT maps by COLUMN POSITION: `insert into dst (a, b) select x,
//! y from src` lands src.x into dst.a and src.y into dst.b even when the
//! two tables declare their columns in different orders.

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
        other => panic!("expected rows, got {other:?}"),
    }
}

fn setup(session: &mut Session) {
    session.run("create table src (x int, y varchar(4))").unwrap();
    session.run("insert into src values (1, 'one'), (2, 'two')").unwrap();
    // dst declares its columns in the OPPOSITE order.
    session.run("create table dst (b varchar(4), a int)").unwrap();
}

#[test]
fn reversed_column_orders_map_by_name() {
    let mut session = Session::new();
    setup(&mut session);

    let inserted = match session
        .run("insert into dst (a, b) select x, y from src")
        .unwrap()
    {
        tidb_session::StmtResult::Affected(count) => count,
        other => panic!("expected affected, got {other:?}"),
    };
    assert_eq!(inserted, 2);

    assert_eq!(
        rows(&mut session, "select a, b from dst order by a"),
        "1|'one';2|'two'"
    );
}
