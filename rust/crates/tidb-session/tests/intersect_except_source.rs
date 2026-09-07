//! INTERSECT and EXCEPT use set semantics (duplicates collapse, NULL-free
//! compare): INTERSECT keeps only members of both sides, EXCEPT subtracts
//! the right side; `EXCEPT ALL` is refused with Go's own text
//! (logical_plan_builder.go:2311).

use tidb_session::Session;

fn try_sql(session: &mut Session, sql: &str) -> String {
    match session.run(sql) {
        Ok(tidb_session::StmtResult::Rows(rows)) => {
            let mut values: Vec<String> = rows
                .into_iter()
                .map(|row| {
                    row.iter()
                        .map(|d| match d {
                            tidb_datatype::Datum::Int(v) => format!("{v}"),
                            other => format!("{other:?}"),
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect();
            values.sort();
            values.join(";")
        }
        Ok(_) => "done".to_owned(),
        Err(e) => format!("ERR {}", e.to_string()),
    }
}

fn setup(session: &mut Session) {
    session.run("create table l (v int)").unwrap();
    session.run("create table r (v int)").unwrap();
    session.run("insert into l values (1), (2), (3), (3)").unwrap();
    session.run("insert into r values (2), (3), (4)").unwrap();
}

#[test]
fn intersect_except_and_the_all_refusal() {
    let mut session = Session::new();
    setup(&mut session);

    // Set semantics: the duplicate 3 collapses; members of both sides.
    assert_eq!(
        try_sql(&mut session, "select v from l intersect select v from r"),
        "2;3"
    );
    // l minus r: {1} (the dup 3 is deduped away before subtraction).
    assert_eq!(try_sql(&mut session, "select v from l except select v from r"), "1");

    // Go's own refusal text for the multiset variant.
    let error = try_sql(&mut session, "select v from l except all select v from r");
    assert!(error.contains("do not support except all"), "{error}");
}
