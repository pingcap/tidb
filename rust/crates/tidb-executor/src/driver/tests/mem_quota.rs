//! `tidb_mem_quota_query` on the WRITE path: which statements a quota
//! cancels, and -- the half that outranks the errno -- what the tables hold
//! afterwards.
//!
//! Every cancellation test here is paired with an ACCEPT-CONTROL: the same
//! statement under the shipped 1GiB default must succeed and leave exactly
//! the rows the cancellation must NOT have written. Accounting that
//! over-counts would cancel statements TiDB completes, and only the pair
//! catches that.
//!
//! The suite statements these stand for are
//! `tests/integrationtest/t/executor/executor.test`'s `TestOOMPanicAction`
//! (`insert`/`replace`/`delete`/`update` under quotas of 200 and 244, with
//! accept-controls at 10000) and `executor/foreign_key.test`'s
//! `update t1 set id=id+100000 where id=1` under 81920, which must be 8175
//! with the cascade UNAPPLIED.

use super::*;
use crate::mem_quota::OomAction;

/// A context whose quota cancels any statement that accounts a single row.
fn cancelling() -> crate::StmtContext {
    crate::StmtContext::for_query().with_mem_quota(1, OomAction::Cancel)
}

/// The shipped default: 1GiB, CANCEL. Nothing in these fixtures approaches it.
fn permitting() -> crate::StmtContext {
    crate::StmtContext::for_query()
}

fn seeded() -> Catalog {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE t (a BIGINT, b BIGINT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &permitting(),
    )
    .unwrap();
    catalog
}

fn rows(catalog: &Catalog) -> Vec<Vec<Datum>> {
    run_select_on("SELECT a, b FROM t ORDER BY a", catalog, &permitting()).unwrap()
}

fn is_memory_exceeded(error: &DriverError) -> bool {
    matches!(
        error,
        DriverError::Exec(crate::executor::ExecError::MemoryExceedForQuery { .. })
    )
}

#[test]
fn an_update_over_the_quota_is_8175_and_changes_nothing() {
    let mut catalog = seeded();
    let before = rows(&catalog);

    let error = run_update_on("UPDATE t SET b = b + 1", &mut catalog, &cancelling()).unwrap_err();
    assert!(is_memory_exceeded(&error), "expected 8175, got {error:?}");
    assert_eq!(
        rows(&catalog),
        before,
        "a cancelled UPDATE must leave every row as it found it"
    );

    // Accept-control: the same statement under the default quota.
    assert_eq!(
        run_update_on("UPDATE t SET b = b + 1", &mut catalog, &permitting()).unwrap(),
        3
    );
    assert_eq!(
        rows(&catalog),
        vec![
            vec![Datum::Int(1), Datum::Int(11)],
            vec![Datum::Int(2), Datum::Int(21)],
            vec![Datum::Int(3), Datum::Int(31)],
        ]
    );
}

#[test]
fn a_delete_over_the_quota_is_8175_and_removes_nothing() {
    let mut catalog = seeded();
    let before = rows(&catalog);

    let error =
        run_delete_on("DELETE FROM t WHERE a > 1", &mut catalog, &cancelling()).unwrap_err();
    assert!(is_memory_exceeded(&error), "expected 8175, got {error:?}");
    assert_eq!(rows(&catalog), before);

    assert_eq!(
        run_delete_on("DELETE FROM t WHERE a > 1", &mut catalog, &permitting()).unwrap(),
        2
    );
    assert_eq!(rows(&catalog), vec![vec![Datum::Int(1), Datum::Int(10)]]);
}

#[test]
fn an_insert_over_the_quota_is_8175_and_writes_nothing() {
    let mut catalog = seeded();
    let before = rows(&catalog);

    let error =
        run_insert_on("INSERT INTO t VALUES (4, 40)", &mut catalog, &cancelling()).unwrap_err();
    assert!(is_memory_exceeded(&error), "expected 8175, got {error:?}");
    assert_eq!(
        rows(&catalog),
        before,
        "the consume sits before the write, so a cancelled INSERT stores no row"
    );

    assert_eq!(
        run_insert_on("INSERT INTO t VALUES (4, 40)", &mut catalog, &permitting()).unwrap(),
        1
    );
    assert_eq!(rows(&catalog).len(), 4);
}

/// The `executor/foreign_key` case in miniature: the cancellation must leave
/// the CHILD rows unrepointed, not only fail the parent's own write. A
/// cascade half-applied would be a silent wrong answer wearing a correct
/// errno.
#[test]
fn a_cancelled_cascade_repoints_no_child_row() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE p (id BIGINT PRIMARY KEY)", &mut catalog).unwrap();
    crate::run_create_table_on(
        "CREATE TABLE c (id BIGINT PRIMARY KEY, pid BIGINT, \
         FOREIGN KEY (pid) REFERENCES p (id) ON UPDATE CASCADE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("INSERT INTO p VALUES (1)", &mut catalog, &permitting()).unwrap();
    run_insert_on(
        "INSERT INTO c VALUES (10, 1), (11, 1)",
        &mut catalog,
        &permitting(),
    )
    .unwrap();

    let child = |catalog: &Catalog| {
        run_select_on("SELECT id, pid FROM c ORDER BY id", catalog, &permitting()).unwrap()
    };
    let before = child(&catalog);

    let error = run_update_on("UPDATE p SET id = 100", &mut catalog, &cancelling()).unwrap_err();
    assert!(is_memory_exceeded(&error), "expected 8175, got {error:?}");
    assert_eq!(
        run_select_on("SELECT id FROM p", &catalog, &permitting()).unwrap(),
        vec![vec![Datum::Int(1)]],
        "the parent row is unchanged"
    );
    assert_eq!(
        child(&catalog),
        before,
        "and no child row was repointed by a cascade the statement never finished"
    );

    // Accept-control: with room, the cascade runs in full.
    run_update_on("UPDATE p SET id = 100", &mut catalog, &permitting()).unwrap();
    assert_eq!(
        child(&catalog),
        vec![
            vec![Datum::Int(10), Datum::Int(100)],
            vec![Datum::Int(11), Datum::Int(100)],
        ]
    );
}

/// `tidb_mem_oom_action = LOG` runs the statement to completion however far
/// it overruns -- the write path must honour that too, not only the sort.
#[test]
fn log_lets_an_over_quota_write_finish() {
    let mut catalog = seeded();
    let logging = crate::StmtContext::for_query().with_mem_quota(1, OomAction::Log);
    assert_eq!(
        run_update_on("UPDATE t SET b = b + 1", &mut catalog, &logging).unwrap(),
        3
    );
}
