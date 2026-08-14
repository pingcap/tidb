use crate::{Session, StmtResult};
use tidb_datatype::Datum;
use tidb_executor::advisory_lock_state::{AdvisoryLockService, LocalAdvisoryLockService};

fn row(values: impl IntoIterator<Item = Datum>) -> StmtResult {
    StmtResult::Rows(vec![values.into_iter().collect()])
}

#[test]
fn advisory_lock_functions_use_session_owned_state() {
    let mut session = Session::new();
    session.set_connection_id(42);

    assert_eq!(
        session.run("select get_lock('RootLock', 0)").unwrap(),
        row([Datum::Int(1)])
    );
    assert_eq!(
        session.run("select is_used_lock('rootlock')").unwrap(),
        row([Datum::Int(42)])
    );
    assert_eq!(
        session.run("select release_lock('ROOTLOCK')").unwrap(),
        row([Datum::Int(1)])
    );
}

#[test]
fn advisory_lock_functions_match_source_boundaries() {
    let mut session = Session::new();
    session.set_connection_id(42);

    for sql in [
        "select get_lock('missing-timeout')",
        "select release_all_locks(1)",
    ] {
        let error = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 1582, "{sql}");
    }

    for sql in [
        "select get_lock('', 0)",
        "select get_lock(null, 0)",
        "select release_lock('')",
        "select release_lock(null)",
        "select is_free_lock('')",
        "select is_used_lock(null)",
        "select get_lock(repeat('a', 65), 0)",
        "select release_lock(repeat('a', 65))",
    ] {
        let error = session.run(sql).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 3057, "{sql}");
    }

    assert_eq!(
        session
            .run("select get_lock('timeout-null', null)")
            .unwrap(),
        row([Datum::Int(1)])
    );
    assert_eq!(
        session.run("select release_lock('TIMEOUT-NULL')").unwrap(),
        row([Datum::Int(1)])
    );
    assert_eq!(
        session.run("select get_lock(1234, -10)").unwrap(),
        row([Datum::Int(1)])
    );
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1292);
    assert_eq!(
        session.warnings()[0].message,
        "Truncated incorrect get_lock value: '-10'"
    );
    assert_eq!(
        session
            .run("select get_lock(repeat(unhex('C3A4'), 33), 0)")
            .unwrap(),
        row([Datum::Int(1)])
    );
    assert_eq!(
        session
            .run("select get_lock('float-timeout', 1.2)")
            .unwrap(),
        row([Datum::Int(1)])
    );
    assert_eq!(
        session.run("select get_lock(repeat('a', 64), 0)").unwrap(),
        row([Datum::Int(1)])
    );

    assert_eq!(
        session
            .run("select get_lock('a1', 0), get_lock('a2', 0), get_lock('a1', 0)")
            .unwrap(),
        row([Datum::Int(1), Datum::Int(1), Datum::Int(1)])
    );
    assert_eq!(
        session
            .run("select is_free_lock('A1'), is_used_lock('a1')")
            .unwrap(),
        row([Datum::Int(0), Datum::Int(42)])
    );
    // Six unique names are held, but repeated GET_LOCK calls count too.
    assert_eq!(
        session.run("select release_all_locks()").unwrap(),
        row([Datum::Int(7)])
    );
    assert_eq!(
        session
            .run("select release_lock('a1'), is_free_lock('a1'), is_used_lock('a1')")
            .unwrap(),
        row([Datum::Int(0), Datum::Int(1), Datum::Null])
    );

    let error = session
        .run("select get_lock('folded-before-error', 0), no_such_fn()")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1305);
    assert_eq!(
        session
            .run("select is_used_lock('folded-before-error')")
            .unwrap(),
        row([Datum::Int(42)])
    );
}

#[test]
fn sessions_contend_and_drop_releases_the_physical_lock() {
    let service: std::sync::Arc<dyn AdvisoryLockService> =
        std::sync::Arc::new(LocalAdvisoryLockService::default());
    let mut first = Session::new();
    first.set_connection_id(42);
    first.set_advisory_lock_service(std::sync::Arc::clone(&service));
    let mut second = Session::new();
    second.set_connection_id(7);
    second.set_advisory_lock_service(service);

    assert_eq!(
        first.run("select get_lock('shared', 0)").unwrap(),
        row([Datum::Int(1)])
    );
    assert_eq!(
        second
            .run("select is_free_lock('shared'), is_used_lock('shared')")
            .unwrap(),
        row([Datum::Int(0), Datum::Int(1)])
    );
    assert_eq!(
        second.run("select get_lock('shared', 0)").unwrap(),
        row([Datum::Int(0)])
    );

    drop(first);
    assert_eq!(
        second.run("select get_lock('shared', 0)").unwrap(),
        row([Datum::Int(1)])
    );
}
