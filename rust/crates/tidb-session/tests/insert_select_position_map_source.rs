//! INSERT SELECT maps by COLUMN POSITION: `insert into dst (a, b) select x,
//! y from src` lands src.x into dst.a and src.y into dst.b even when the
//! two tables declare their columns in different orders.

use tidb_session::Session;

use crate::support::quoted_string_integer_rows as rows;

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
