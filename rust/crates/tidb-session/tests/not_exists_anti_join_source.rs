//! `NOT EXISTS` correlated anti-join: rows of `l` with no match in `r`
//! survive — the counterpart of the WHERE-EXISTS pin.

use tidb_session::Session;

use crate::support::integer_rows as rows;

#[test]
fn not_exists_keeps_unmatched_rows() {
    let mut session = Session::new();
    session.run("create table l (id int)").unwrap();
    session.run("insert into l values (1), (2), (3)").unwrap();
    session.run("create table r (id int)").unwrap();
    session.run("insert into r values (2)").unwrap();

    assert_eq!(
        rows(
            &mut session,
            "select l.id from l where not exists (select 1 from r where r.id = l.id) order by l.id"
        ),
        "1;3"
    );
}
