//! `SLEEP` conversion, error policy, waiting, and command cancellation.

use crate::*;

#[test]
fn sleep_uses_statement_conversion_and_waiting_semantics() {
    let mut session = Session::new();

    assert_eq!(
        session.run("SELECT SLEEP('a')").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(0)]])
    );
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1292);
    assert_eq!(
        session.warnings()[0].message,
        "Truncated incorrect DOUBLE value: 'a'"
    );

    let error = session
        .run("SELECT SLEEP(-1)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1210);
    assert_eq!(error.message, "Incorrect arguments to sleep");

    // The ETReal cast is the ordinary statement conversion path, not a
    // SLEEP-local parser: a strict write rejects truncation while a
    // permissive write keeps the numeric prefix and warning.
    session.run("CREATE TABLE sleep_out (v BIGINT)").unwrap();
    let error = session
        .run("INSERT INTO sleep_out SELECT SLEEP('a')")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1292);
    session.run("SET sql_mode = ''").unwrap();
    session
        .run("INSERT INTO sleep_out SELECT SLEEP('a')")
        .unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1292);
    assert_eq!(
        session.run("SELECT v FROM sleep_out").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(0)]])
    );

    let error = session
        .run("INSERT INTO sleep_out VALUES (SLEEP(-1))")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1210);
    session
        .run("SET tidb_enable_strict_not_null_check = 0")
        .unwrap();
    session
        .run("INSERT INTO sleep_out VALUES (SLEEP(-1))")
        .unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1210);

    // Go's explicit duration-overflow guard is a hard incorrect-arguments
    // error, not the bad-NULL alias and therefore not downgraded by IGNORE.
    let error = session
        .run("INSERT IGNORE INTO sleep_out SELECT SLEEP(1e300)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1210);

    let started = std::time::Instant::now();
    assert_eq!(
        session.run("SELECT SLEEP(0.02)").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(0)]])
    );
    assert!(started.elapsed() >= std::time::Duration::from_millis(15));

    // A kill that arrives while SLEEP is blocked returns 1. Because this is a
    // standalone SELECT, Go clears that handled signal and evaluates the next
    // SLEEP normally.
    let cancellation = std::sync::Arc::new(session.begin_query_cancellation());
    let interrupter = {
        let cancellation = std::sync::Arc::clone(&cancellation);
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(20));
            cancellation.cancel();
        })
    };
    let started = std::time::Instant::now();
    assert_eq!(
        session.run("SELECT SLEEP(1), SLEEP(0.02)").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1), Datum::Int(0)]])
    );
    interrupter.join().unwrap();
    assert!(started.elapsed() >= std::time::Duration::from_millis(35));
    assert!(started.elapsed() < std::time::Duration::from_millis(200));
    drop(cancellation);
}

#[test]
fn query_cancellation_reaches_non_accounting_executor_batches() {
    let mut session = Session::new();
    let cancellation = session.begin_query_cancellation();
    cancellation.cancel();

    let error = session.run("SELECT 1").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1317);
    assert_eq!(error.state, *b"70100");
}
