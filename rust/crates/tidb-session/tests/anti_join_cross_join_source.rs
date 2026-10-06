//! The LEFT JOIN + `IS NULL` anti-join idiom keeps only the left rows with
//! no match, and CROSS JOIN produces the full Cartesian product.

use tidb_session::Session;

use crate::support::integer_rows_with_sql as rows;

#[test]
fn anti_join_and_cartesian_product() {
    let mut session = Session::new();
    session.run("create table l (id int)").unwrap();
    session.run("insert into l values (1), (2), (3)").unwrap();
    session.run("create table r (id int)").unwrap();
    session.run("insert into r values (2)").unwrap();

    // Anti-join: rows of l without a match in r.
    assert_eq!(
        rows(
            &mut session,
            "select l.id from l left join r on l.id = r.id where r.id is null order by l.id"
        ),
        "1;3"
    );

    // CROSS JOIN: every left row pairs with every right row.
    assert_eq!(
        rows(&mut session, "select l.id, r.id from l cross join r order by l.id"),
        "1|2;2|2;3|2"
    );
}
