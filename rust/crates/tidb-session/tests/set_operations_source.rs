//! Set operations: UNION deduplicates while UNION ALL keeps multiset rows,
//! EXCEPT subtracts and deduplicates, INTERSECT keeps shared values, and a
//! trailing ORDER BY/LIMIT sorts and truncates the UNION's combined output.

use tidb_session::Session;

fn rows_sorted(session: &mut Session, sql: &str) -> Vec<String> {
    match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|d| match d {
                        tidb_datatype::Datum::Int(i) => format!("{i}"),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect(),
        other => panic!("expected rows for {sql}, got {other:?}"),
    }
}

#[test]
fn union_except_intersect_semantics() {
    let mut session = Session::new();
    session.run("create table a (v int)").unwrap();
    session.run("insert into a values (1), (2), (3)").unwrap();
    session.run("create table b (v int)").unwrap();
    session.run("insert into b values (2), (3), (4)").unwrap();

    // UNION dedups across both sides.
    let mut union = rows_sorted(&mut session, "select v from a union select v from b");
    union.sort();
    assert_eq!(union, vec!["1", "2", "3", "4"]);

    // UNION ALL keeps every row.
    let mut union_all = rows_sorted(&mut session, "select v from a union all select v from b");
    assert_eq!(union_all.len(), 6);
    union_all.sort();
    assert_eq!(union_all, vec!["1", "2", "2", "3", "3", "4"]);

    // EXCEPT subtracts and dedups: {1,2,3} minus {2,3,4} = {1}.
    let mut except = rows_sorted(&mut session, "select v from a except select v from b");
    except.sort();
    assert_eq!(except, vec!["1"]);

    // INTERSECT keeps the shared values (dedup, order-free).
    let mut intersect = rows_sorted(&mut session, "select v from a intersect select v from b");
    intersect.sort();
    assert_eq!(intersect, vec!["2", "3"]);
}

#[test]
fn union_all_composes_with_global_order_and_limit() {
    let mut session = Session::new();
    session.run("create table a (v int)").unwrap();
    session.run("insert into a values (1), (2), (3)").unwrap();
    session.run("create table b (v int)").unwrap();
    session.run("insert into b values (2), (3), (4)").unwrap();
    let rows = rows_sorted(
        &mut session,
        "select v from a union all select v from b order by v limit 3",
    );
    assert_eq!(rows, vec!["1", "2", "2"], "the sort+limit is global");
}
