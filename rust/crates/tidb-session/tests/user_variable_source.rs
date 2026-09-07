//! User variables through the session: a bare `@name` read answers the
//! value the session holds (NULL when unset, Go's string-typed NULL via the
//! `getvar_string` lowering), `@name := expr` assigns inline, and the
//! running-total idiom accumulates in row order.

use tidb_datatype::Datum;
use tidb_session::Session;

/// A compact display for the handful of Datum kinds these pins produce.
fn text(d: &Datum) -> String {
    match d {
        Datum::Null => "NULL".to_owned(),
        Datum::Int(i) => format!("{i}"),
        Datum::UInt(u) => format!("{u}"),
        Datum::Real(r) => format!("{r}"),
        Datum::String(s) => String::from_utf8_lossy(&s.bytes()).into_owned(),
        other => format!("{other:?}"),
    }
}

fn joined(session: &mut Session, sql: &str) -> String {
    let rows = match session.run(sql).unwrap() {
        tidb_session::StmtResult::Rows(rows) => rows,
        other => panic!("expected rows for {sql}, got {other:?}"),
    };
    rows.into_iter()
        .map(|row| {
            row.iter().map(text).collect::<Vec<_>>().join("|")
        })
        .collect::<Vec<_>>()
        .join(";")
}

#[test]
fn unset_variable_reads_null() {
    let mut session = Session::new();
    assert_eq!(joined(&mut session, "select @missing"), "NULL");
}

#[test]
fn assignment_and_read_back() {
    let mut session = Session::new();
    assert_eq!(joined(&mut session, "select @x := 7"), "7");
    assert_eq!(joined(&mut session, "select @x"), "7");
}

#[test]
fn running_total_idiom() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, v int)").unwrap();
    session.run("insert into t values (1, 10), (2, 20), (3, 30)").unwrap();

    // Uninitialized: `@t + v` is NULL, and Go's SETVAR signature never
    // STORES a NULL -- the variable stays unset, so every row reads NULL.
    assert_eq!(
        joined(&mut session, "select @t := @t + v from t order by a"),
        "NULL;NULL;NULL"
    );

    // Initialized (the classic idiom), the total accumulates in row order.
    session.run("set @t = 0").unwrap();
    assert_eq!(
        joined(&mut session, "select @t := @t + v from t order by a"),
        "10;30;60"
    );
}
