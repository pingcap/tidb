//! Transaction boundaries: `BEGIN`/`COMMIT`/`ROLLBACK`, nesting, the
//! conflict a commit can lose, `autocommit`, and how `BEGIN` resolves its
//! mode -- Go `pkg/session/txn.go`.

use crate::*;

/// A transaction stages its writes: the session reads its own, a peer
/// sharing the catalog sees nothing until COMMIT, and ROLLBACK discards.
#[test]
fn transaction_stages_writes_until_commit() {
    let mut writer = Session::new();
    writer.run("CREATE TABLE t (a BIGINT)").unwrap();
    writer.run("INSERT INTO t VALUES (1)").unwrap();
    let mut peer = Session::with_catalog(writer.shared_catalog());

    assert_eq!(writer.control_transaction("BEGIN").unwrap(), Some(true));
    assert!(writer.in_transaction());
    writer.run("INSERT INTO t VALUES (2)").unwrap();

    // The transaction reads its own write; the peer does not see it.
    assert_eq!(
        writer.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
    );
    assert_eq!(
        peer.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    assert_eq!(writer.control_transaction("COMMIT").unwrap(), Some(false));
    assert!(!writer.in_transaction());
    assert_eq!(
        peer.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
    );

    // ROLLBACK discards everything staged since BEGIN.
    writer.control_transaction("BEGIN").unwrap();
    writer.run("INSERT INTO t VALUES (3)").unwrap();
    writer.run("DELETE FROM t WHERE a = 1").unwrap();
    assert_eq!(writer.control_transaction("ROLLBACK").unwrap(), Some(false));
    assert_eq!(
        writer.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
    );
}

/// A commit that would discard a peer's writes is refused, rather than
/// silently overwriting them. The refused transaction is over, so its
/// staged writes are gone -- the statements must be retried, not the
/// COMMIT alone.
#[test]
fn a_conflicting_commit_is_refused() {
    let mut first = Session::new();
    first.run("CREATE TABLE t (a BIGINT)").unwrap();
    let mut second = Session::with_catalog(first.shared_catalog());

    first.control_transaction("BEGIN").unwrap();
    first.run("INSERT INTO t VALUES (1)").unwrap();
    // The peer commits first, moving the shared catalog.
    second.run("INSERT INTO t VALUES (2)").unwrap();

    assert!(matches!(
        first.control_transaction("COMMIT"),
        Err(DriverError::Txn(TxnErrorKind::WriteConflict))
    ));
    assert!(!first.in_transaction(), "a refused commit ends the txn");
    // The peer's write survived; the refused one did not.
    assert_eq!(
        second.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(2)]])
    );
}

/// BEGIN inside an open transaction implicitly commits it, as in Go, and
/// COMMIT/ROLLBACK outside one is a no-op, as in MySQL.
#[test]
fn nested_begin_commits_and_stray_commit_is_a_no_op() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT)").unwrap();
    assert_eq!(session.control_transaction("COMMIT").unwrap(), Some(false));
    assert_eq!(
        session.control_transaction("ROLLBACK").unwrap(),
        Some(false)
    );

    session.control_transaction("BEGIN").unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();
    // The implicit commit publishes the first transaction's write.
    session.control_transaction("START TRANSACTION").unwrap();
    session.run("INSERT INTO t VALUES (2)").unwrap();
    session.control_transaction("ROLLBACK").unwrap();
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // A non-transaction statement is not claimed by the hook.
    assert_eq!(session.control_transaction("SELECT 1").unwrap(), None);
    assert!(session
        .control_transaction("ROLLBACK TO SAVEPOINT s")
        .is_err());
}

/// `BEGIN PESSIMISTIC` / `BEGIN OPTIMISTIC` and `@@tidb_txn_mode` decide the
/// mode a transaction opens in, exactly as Go's `newProviderWithRequest`
/// does. This tier takes no row locks in either mode -- its store is one
/// shared catalog behind a mutex -- so the mode is recorded, not acted on.
///
/// Captured from TiDB's mock store: `@@tidb_txn_mode` defaults to
/// `pessimistic`, `SET tidb_txn_mode = ''` is accepted and reads back empty
/// (the variable is `AllowEmptyAll`), `'bogus'` is rejected with 1231, and
/// `BEGIN PESSIMISTIC` still locks rows with the variable set to `optimistic`.
#[test]
fn a_begin_resolves_its_transaction_mode_from_the_keyword_then_the_variable() {
    let mut session = Session::new();
    assert_eq!(session.txn_mode(), None, "no transaction is open");
    assert_eq!(
        session.run("SELECT @@tidb_txn_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("pessimistic")]])
    );

    session.control_transaction("BEGIN").unwrap();
    assert_eq!(session.txn_mode(), Some(SessionTxnMode::Pessimistic));
    session.control_transaction("BEGIN OPTIMISTIC").unwrap();
    assert_eq!(session.txn_mode(), Some(SessionTxnMode::Optimistic));
    session.control_transaction("ROLLBACK").unwrap();
    assert_eq!(session.txn_mode(), None);

    // The variable decides a bare BEGIN; the keyword outranks it.
    session
        .apply_set("SET tidb_txn_mode = 'optimistic'")
        .unwrap();
    session.control_transaction("START TRANSACTION").unwrap();
    assert_eq!(session.txn_mode(), Some(SessionTxnMode::Optimistic));
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    assert_eq!(session.txn_mode(), Some(SessionTxnMode::Pessimistic));
    session.control_transaction("COMMIT").unwrap();

    // The empty string is a value this variable really can hold, and Go reads
    // anything other than `pessimistic` as optimistic.
    session.apply_set("SET tidb_txn_mode = ''").unwrap();
    assert_eq!(
        session.run("SELECT @@tidb_txn_mode").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("")]])
    );
    session.control_transaction("BEGIN").unwrap();
    assert_eq!(session.txn_mode(), Some(SessionTxnMode::Optimistic));
    session.control_transaction("ROLLBACK").unwrap();

    // A value outside the enum is still rejected: Go's 1231.
    assert!(matches!(
        session.apply_set("SET tidb_txn_mode = 'bogus'"),
        Err(DriverError::Var(
            tidb_executor::VarErrorKind::WrongValueForVar(_, _)
        ))
    ));
}

/// `autocommit = 0`'s captured rules (`corpus/table/transactions` and
/// `corpus/table/autocommit_source`): a statement then runs inside a
/// transaction the session opens for it, so `ROLLBACK` discards it; and only
/// the OFF -> ON TRANSITION of `SET autocommit` commits what is open -- `SET
/// autocommit = 1` while it is already on leaves an explicit `BEGIN`
/// running.
#[test]
fn autocommit_off_puts_a_statement_in_a_transaction() {
    let mut session = Session::new();
    session.run("CREATE TABLE ac (id INT)").unwrap();

    session.run("SET autocommit = 0").unwrap();
    session.run("INSERT INTO ac VALUES (1)").unwrap();
    session.run("INSERT INTO ac VALUES (2)").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        session.run("SELECT id FROM ac ORDER BY id").unwrap(),
        StmtResult::Rows(vec![]),
        "captured: both writes were inside the implicit transaction"
    );

    session.run("INSERT INTO ac VALUES (1)").unwrap();
    session.run("COMMIT").unwrap();
    session.run("INSERT INTO ac VALUES (2)").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        session.run("SELECT id FROM ac ORDER BY id").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // OFF -> ON commits what is open.
    session.run("INSERT INTO ac VALUES (2)").unwrap();
    session.run("SET autocommit = 1").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        session.run("SELECT id FROM ac ORDER BY id").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]])
    );

    // ON -> ON is not a transition, so the explicit transaction survives the
    // SET and the ROLLBACK still discards.
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO ac VALUES (3)").unwrap();
    session.run("SET autocommit = 1").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        session.run("SELECT id FROM ac ORDER BY id").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]),
        "captured: a redundant SET does not end the transaction"
    );

    // ... but ON -> OFF -> ON inside the same explicit transaction does.
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO ac VALUES (3)").unwrap();
    session.run("SET autocommit = 0").unwrap();
    session.run("SET autocommit = 1").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        session.run("SELECT id FROM ac ORDER BY id").unwrap(),
        StmtResult::Rows(vec![
            vec![Datum::Int(1)],
            vec![Datum::Int(2)],
            vec![Datum::Int(3)],
        ])
    );
}
