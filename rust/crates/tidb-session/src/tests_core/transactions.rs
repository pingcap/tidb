//! Transaction boundaries: `BEGIN`/`COMMIT`/`ROLLBACK`, nesting, the
//! conflict a commit can lose, `autocommit`, and how `BEGIN` resolves its
//! mode -- Go `pkg/session/txn.go`.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_executor::compiler::PreparedAstCompile;
use crate::*;

/// Go retains TxnCtx.Isolation across changes to the session default.
#[test]
fn locking_read_isolation_is_retained_until_transaction_end() {
    let mut session = Session::new();
    for (initial, subsequent, read_committed) in [
        ("REPEATABLE-READ", "READ-COMMITTED", false),
        ("READ-COMMITTED", "REPEATABLE-READ", true),
    ] {
        session
            .run(&format!("SET SESSION transaction_isolation='{initial}'"))
            .unwrap();
        assert_eq!(session.read_committed_locking(), read_committed);
        session.control_transaction("BEGIN PESSIMISTIC").unwrap();
        session
            .run(&format!("SET SESSION transaction_isolation='{subsequent}'"))
            .unwrap();
        assert_eq!(session.read_committed_locking(), read_committed);
        session.control_transaction("ROLLBACK").unwrap();
        assert_eq!(session.read_committed_locking(), !read_committed);
    }
}

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

/// Go's optimistic commit fails only on a key a peer committed after the
/// transaction began, and its schema check only on a table it wrote: rows a
/// peer inserted elsewhere, or a peer's unrelated DDL, do not refuse the
/// commit, while a write to the row the peer changed does -- and the refused
/// transaction is over, so its statements must be retried, not the COMMIT
/// alone.
#[test]
fn a_commit_conflicts_only_on_a_key_a_peer_committed() {
    let mut first = Session::new();
    first
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b BIGINT)")
        .unwrap();
    first.run("INSERT INTO t VALUES (1, 0), (2, 0)").unwrap();
    let mut second = Session::with_catalog(first.shared_catalog());

    first.control_transaction("BEGIN OPTIMISTIC").unwrap();
    first.run("INSERT INTO t VALUES (3, 0)").unwrap();
    second.run("INSERT INTO t VALUES (4, 0)").unwrap();
    first.control_transaction("COMMIT").unwrap();
    assert_eq!(
        crate::tests_support::row_text(second.run("SELECT a FROM t ORDER BY a")),
        [["1"], ["2"], ["3"], ["4"]]
    );

    first.control_transaction("BEGIN OPTIMISTIC").unwrap();
    first.run("UPDATE t SET b = 3 WHERE a = 2").unwrap();
    second.run("CREATE TABLE u (a INT)").unwrap();
    first.control_transaction("COMMIT").unwrap();

    first.control_transaction("BEGIN OPTIMISTIC").unwrap();
    first.run("UPDATE t SET b = 1 WHERE a = 1").unwrap();
    second.run("UPDATE t SET b = 2 WHERE a = 1").unwrap();
    // Go's message names the conflicting timestamps and key
    // (`newWriteConflictError`), so only its fixed prefix is pinned.
    let error = first
        .control_transaction("COMMIT")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 9007, "{}", error.message);
    assert!(
        error.message.starts_with("Write conflict, "),
        "{}",
        error.message
    );
    assert!(!first.in_transaction(), "a refused commit ends the txn");
    // The peer's write survived; the refused one did not.
    assert_eq!(
        crate::tests_support::row_text(second.run("SELECT a, b FROM t ORDER BY a")),
        [["1", "2"], ["2", "3"], ["3", "0"], ["4", "0"]]
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

/// Go `TestIssue21447`: a pessimistic statement that writes reads the
/// latest committed rows (`for_update_ts`), as a locking read does, while a
/// plain read stays on the transaction's snapshot; and the transaction
/// commits over the peer's commit, whose row it locked after reading it.
#[test]
fn a_pessimistic_write_reads_the_latest_committed_row() {
    let mut first = Session::new();
    first
        .run("CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(40))")
        .unwrap();
    first.run("INSERT INTO t1 VALUES (1, 'abc')").unwrap();
    let mut second = Session::with_catalog(first.shared_catalog());

    first.control_transaction("BEGIN PESSIMISTIC").unwrap();
    second.control_transaction("BEGIN PESSIMISTIC").unwrap();
    assert_eq!(
        second
            .run("UPDATE t1 SET name = 'xyz' WHERE id = 1")
            .unwrap(),
        StmtResult::Affected(1)
    );
    second.control_transaction("COMMIT").unwrap();

    assert_eq!(
        first
            .run("UPDATE t1 SET name = 'xyz' WHERE id = 1")
            .unwrap(),
        StmtResult::Affected(0),
        "the update reads the row the peer committed"
    );
    let read = |session: &mut Session, sql: &str| crate::tests_support::row_text(session.run(sql));
    assert_eq!(
        read(&mut first, "SELECT * FROM t1 WHERE id = 1"),
        [["1", "abc"]]
    );
    assert_eq!(
        read(&mut first, "SELECT * FROM t1 WHERE id = 1 FOR UPDATE"),
        [["1", "xyz"]]
    );
    assert_eq!(
        read(&mut first, "SELECT * FROM t1 WHERE id IN (1, 2)"),
        [["1", "abc"]]
    );
    first.control_transaction("COMMIT").unwrap();

    // A write over a row a peer changed builds on the peer's value, and the
    // peer's other rows survive the commit.
    first
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT, KEY (b))")
        .unwrap();
    first.run("INSERT INTO t VALUES (1, 0), (2, 0)").unwrap();
    first.control_transaction("BEGIN PESSIMISTIC").unwrap();
    first.run("INSERT INTO t VALUES (3, 0)").unwrap();
    second.run("UPDATE t SET b = 10 WHERE a = 1").unwrap();
    second.run("INSERT INTO t VALUES (4, 0)").unwrap();
    first.run("UPDATE t SET b = b + 1 WHERE a = 1").unwrap();
    first.control_transaction("COMMIT").unwrap();
    assert_eq!(
        read(&mut second, "SELECT a, b FROM t ORDER BY a"),
        [["1", "11"], ["2", "0"], ["3", "0"], ["4", "0"]]
    );
    assert_eq!(
        read(&mut second, "SELECT a FROM t USE INDEX (b) WHERE b = 11"),
        [["1"]],
        "the index entry follows the merged row"
    );
}

/// Go's statement rollback discards the failed statement's staging buffer,
/// so a key only that statement wrote is not the transaction's: a later
/// pessimistic statement reads the row a peer committed under it.
#[test]
fn a_failed_statement_leaves_no_write_for_later_reads() {
    let mut first = Session::new();
    first
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
        .unwrap();
    first.run("INSERT INTO t VALUES (1, 0)").unwrap();
    let mut second = Session::with_catalog(first.shared_catalog());

    first.control_transaction("BEGIN PESSIMISTIC").unwrap();
    assert_eq!(
        first
            .run("INSERT INTO t VALUES (2, 0), (1, 0)")
            .unwrap_err()
            .to_mysql_error()
            .code,
        1062
    );
    second.run("INSERT INTO t VALUES (2, 5)").unwrap();
    assert_eq!(
        first.run("UPDATE t SET b = b + 1 WHERE a = 2").unwrap(),
        StmtResult::Affected(1)
    );
    first.control_transaction("COMMIT").unwrap();
    assert_eq!(
        crate::tests_support::row_text(second.run("SELECT a, b FROM t ORDER BY a")),
        [["1", "0"], ["2", "6"]]
    );
}

/// Go `TestSelectForUpdateOf`: a row another pessimistic transaction locked
/// fails `FOR UPDATE NOWAIT` with 3572 until that transaction ends, and
/// `FOR UPDATE OF t` locks only `t`'s rows.
#[test]
fn nowait_fails_on_a_row_another_transaction_locked() {
    let mut first = Session::new();
    first.run("CREATE TABLE t (i INT)").unwrap();
    first.run("CREATE TABLE t1 (i INT)").unwrap();
    first.run("INSERT INTO t VALUES (1)").unwrap();
    first.run("INSERT INTO t1 VALUES (1)").unwrap();
    let mut second = Session::with_catalog(first.shared_catalog());

    first.control_transaction("BEGIN PESSIMISTIC").unwrap();
    first
        .run("SELECT * FROM t, t1 WHERE t.i = t1.i FOR UPDATE OF t")
        .unwrap();
    second.control_transaction("BEGIN PESSIMISTIC").unwrap();
    second.run("SELECT * FROM t1 FOR UPDATE").unwrap();
    assert_eq!(
        second
            .run("SELECT * FROM t FOR UPDATE NOWAIT")
            .unwrap_err()
            .to_mysql_error()
            .code,
        3572
    );
    assert!(
        second.in_transaction(),
        "a lock failure ends the statement only"
    );
    first.control_transaction("ROLLBACK").unwrap();
    second.run("SELECT * FROM t FOR UPDATE NOWAIT").unwrap();
    second.control_transaction("ROLLBACK").unwrap();

    // A closed connection's transaction releases its locks too.
    let mut third = Session::with_catalog(first.shared_catalog());
    third.control_transaction("BEGIN PESSIMISTIC").unwrap();
    third.run("UPDATE t SET i = 2").unwrap();
    second.control_transaction("BEGIN PESSIMISTIC").unwrap();
    assert_eq!(
        second
            .run("SELECT * FROM t FOR UPDATE NOWAIT")
            .unwrap_err()
            .to_mysql_error()
            .code,
        3572
    );
    drop(third);
    second.run("SELECT * FROM t FOR UPDATE NOWAIT").unwrap();
}

/// Go `TestSelectForUpdate`: an optimistic transaction's locking read
/// conflicts at COMMIT only on a row a peer committed after `BEGIN`.
#[test]
fn an_optimistic_locking_read_conflicts_on_its_own_rows() {
    let mut first = Session::new();
    first
        .run("CREATE TABLE t (c1 INT, c2 INT, c3 INT)")
        .unwrap();
    first
        .run("INSERT INTO t VALUES (11, 2, 3), (12, 2, 3), (13, 2, 3)")
        .unwrap();
    let mut second = Session::with_catalog(first.shared_catalog());

    first.control_transaction("BEGIN OPTIMISTIC").unwrap();
    first
        .run("SELECT * FROM t WHERE c1 = 11 FOR UPDATE")
        .unwrap();
    second.run("UPDATE t SET c2 = 211 WHERE c1 = 12").unwrap();
    first.control_transaction("COMMIT").unwrap();

    first.control_transaction("BEGIN OPTIMISTIC").unwrap();
    first
        .run("SELECT * FROM t WHERE c1 = 11 FOR UPDATE")
        .unwrap();
    second.run("UPDATE t SET c2 = 211 WHERE c1 = 11").unwrap();
    assert_eq!(
        crate::tests_support::err_code(first.control_transaction("COMMIT")),
        9007
    );
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
    crate::tests_support::assert_err(
        session.apply_set("SET tidb_txn_mode = 'bogus'"),
        1231,
        "Variable 'tidb_txn_mode' can't be set to the value of 'bogus'",
    );
}

/// Go `optimizeDupKeyCheckForNormalInsert` (`pkg/executor/insert.go:331-337`)
/// sees the implicit pessimistic transaction created for an autocommit DML,
/// not only an explicit `BEGIN`. The statement context must therefore carry
/// lazy duplicate checking on a normal user connection before the transaction
/// exists; changing the mode or constraint variable keeps the same Go matrix.
#[test]
fn normal_insert_duplicate_mode_matches_go_for_autocommit_and_explicit_modes() {
    let mut session = Session::new();
    session.set_connection_id(42);

    let default = session.statement_context(true);
    assert!(!default.constraint_check_in_place());
    assert!(default.pessimistic_lazy_dup_check());

    session
        .apply_set("SET tidb_txn_mode = 'optimistic'")
        .unwrap();
    let optimistic = session.statement_context(true);
    assert!(!optimistic.pessimistic_lazy_dup_check());
    assert!(!optimistic.constraint_check_in_place());

    session
        .apply_set("SET tidb_constraint_check_in_place = ON")
        .unwrap();
    let eager = session.statement_context(true);
    assert!(eager.constraint_check_in_place());
    assert!(!eager.pessimistic_lazy_dup_check());

    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    let explicit_pessimistic = session.statement_context(true);
    assert!(explicit_pessimistic.constraint_check_in_place());
    assert!(explicit_pessimistic.pessimistic_lazy_dup_check());
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

#[test]
fn process_status_uses_the_typed_autocommit_and_transaction_bits() {
    let mut session = Session::new();
    assert_eq!(session.status_text(), "autocommit");

    session.run("SET autocommit = 0").unwrap();
    assert_eq!(session.status_text(), "");

    session.control_transaction("BEGIN").unwrap();
    assert_eq!(session.status_text(), "in transaction");
    session.control_transaction("ROLLBACK").unwrap();
    assert_eq!(session.status_text(), "");

    session.run("SET autocommit = 1").unwrap();
    assert_eq!(session.status_text(), "autocommit");
}

/// A session that has pinned `tidb_snapshot` must not be answered from the
/// present: Go reads the session's `SnapshotTS`, and this tier refuses it --
/// the same answer `tidb-planner`'s bounded scan gives
/// (`UnsupportedReadOnlyFeature::StaleRead`). Silently returning current rows
/// is the one outcome a client cannot detect.
///
/// `tidb_read_staleness` is no such pin on this store. Go applies it to a
/// SELECT only (preprocess `p.stmtTp == TypeSelect`) and reads at
/// `CalAppropriateTime(now + staleness, now, minSafeTS)`
/// (`staleread.CalculateTsWithReadStaleness`). A store whose stores report no
/// safe ts, as unistore's `GetStoreSafeTS` does, leaves client-go's
/// `getMinSafeTSByStores` at MaxUint64, which clamps the read to now: the
/// recorded `session/nontransactional` reads current rows under -100. A
/// write ignores it. Both had been refused.
#[test]
fn read_staleness_reads_the_present_and_a_snapshot_pin_is_refused() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1, 10)").unwrap();

    session.run("SET @@tidb_read_staleness = -1").unwrap();
    // Go reads the schema a second back as well, where the table just
    // created does not exist yet (asked of the Go server: 1146).
    assert_eq!(
        crate::tests_support::err_code(session.run("SELECT b FROM t")),
        1146
    );
    session.run("SET @@tidb_read_staleness = 0").unwrap();
    assert_eq!(
        crate::tests_support::row_text(session.run("SELECT b FROM t")),
        vec![vec!["10"]]
    );
}

/// `AS OF TIMESTAMP` on a table reference reads the store's history --
/// the corpus recipe (`executor/stale_txn`, `TestAsOfTimestampSupportTSO`):
/// the last commit's TSO comes out of `@@tidb_last_txn_info`, reading AS OF
/// it sees that commit, reading AS OF ts-1 sees the state BEFORE it, and
/// Go's `CalculateAsOfTsExpr` refusals (`staleread/util.go:56-74`) keep
/// their 8135 identity. A timestamp older than the retained ring refuses
/// rather than answering from the present -- the one undetectable wrong
/// answer this feature must never give.
#[test]
fn as_of_timestamp_reads_the_stores_history() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT PRIMARY KEY)").unwrap();
    session.run("INSERT INTO t VALUES (1)").unwrap();
    session.run("INSERT INTO t VALUES (2)").unwrap();
    session
        .run("SET @last_commit_ts = json_extract(@@tidb_last_txn_info, '$.commit_ts')")
        .unwrap();
    session
        .run("SET @prev_commit_ts = cast(cast(@last_commit_ts as unsigned) - 1 as char)")
        .unwrap();

    let sql = "SELECT count(*) FROM t AS OF TIMESTAMP @last_commit_ts";
    let statement = session.parse_statement(sql).unwrap();
    session.bind_committed_snapshot().unwrap();
    let OpenedStatement::Rows(mut result) = session.open_record_set_parsed(statement, sql).unwrap()
    else {
        panic!("historical SELECT must retain its executor");
    };
    assert!(session.in_transaction());
    let mut chunk = result.new_chunk();
    result.next(&mut session, &mut chunk).unwrap();
    assert_eq!(chunk.num_rows(), 1);
    assert_eq!(chunk.get_row(0).get_int64(0), 2);
    assert!(
        session.in_transaction(),
        "Next does not end the stale statement"
    );
    result.finish(&mut session).unwrap();
    assert!(!session.in_transaction());
    result.close(&mut session).unwrap();
    result.close(&mut session).unwrap();

    // Go's record-set Close owns statement teardown on abandonment and errors too.
    for cancel in [false, true] {
        let statement = session.parse_statement(sql).unwrap();
        {
            session.bind_committed_snapshot().unwrap();
            let StatementExecution::Rows(mut result) = session
                .open_record_set_parsed(statement, sql)
                .unwrap()
                .attach(&mut session)
            else {
                panic!("historical SELECT must retain its executor");
            };
            assert!(result.session().in_transaction());
            if cancel {
                result
                    .session()
                    .routed_statement_memory()
                    .sql_killer()
                    .send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
                let mut chunk = result.new_chunk();
                assert_eq!(
                    result.next(&mut chunk).unwrap_err().to_mysql_error().code,
                    1317
                );
            }
        }
        assert!(
            !session.in_transaction(),
            "Drop must end the stale statement"
        );
    }
    assert_eq!(
        session
            .run("SELECT count(*) FROM t AS OF TIMESTAMP @prev_commit_ts")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]]),
        "one tick earlier sees the state before it"
    );
    // Go: an integer TSO is accepted directly (`tsoFromDatum`).
    session
        .run("SET @int_tso = cast(@last_commit_ts as unsigned)")
        .unwrap();
    assert_eq!(
        session
            .run("SELECT count(*) FROM t AS OF TIMESTAMP @int_tso")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(2)]]),
    );
    // Go's 8135 refusals, verbatim causes.
    for (sql, cause) in [
        (
            "SELECT count(*) FROM t AS OF TIMESTAMP 'invalid-date'",
            "cannot parse AS OF TIMESTAMP expression as datetime or TSO",
        ),
        (
            "SELECT count(*) FROM t AS OF TIMESTAMP NULL",
            "as of timestamp cannot be NULL",
        ),
        (
            "SELECT count(*) FROM t AS OF TIMESTAMP 100",
            "invalid TSO timestamp: TSO is before 2013-01-01",
        ),
    ] {
        let error = session.run(sql).unwrap_err();
        assert!(
            format!("{error:?}").contains(cause),
            "{sql} must refuse with Go's cause, got {error:?}"
        );
    }
    // Go resolves names in the historical schema: a table created later
    // does not exist there. Never answer from the present. Only a raw TSO
    // is bounded by 2013: a datetime before it is a read timestamp like any
    // other (util.go:64-80).
    for sql in [
        "SELECT a FROM t AS OF TIMESTAMP '2020-01-01 00:00:00'",
        "SELECT a FROM t AS OF TIMESTAMP '2000-01-01 00:00:00'",
    ] {
        let error = session.run(sql).unwrap_err();
        assert!(
            error.equal(&tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS),
            "{sql}: got {error:?}"
        );
    }
}

/// Go `SimpleExec.executeBegin`: `START TRANSACTION READ ONLY` is a no-op
/// clause -- TiDB does not stop writes for it -- so it takes the
/// `tidb_enable_noop_functions` gate and is refused with 1235 at the OFF
/// default.
#[test]
fn start_transaction_read_only_takes_the_noop_functions_gate() {
    let mut session = Session::new();
    let error = session.run("START TRANSACTION READ ONLY").unwrap_err();
    assert!(
        format!("{error:?}").contains("READ ONLY"),
        "expected the 1235 noop refusal, got {error:?}"
    );
    assert!(
        !session.in_transaction(),
        "the refusal must not have opened a transaction"
    );

    // WARN accepts it and says so; ON says nothing.
    session
        .run("SET @@tidb_enable_noop_functions = 'WARN'")
        .unwrap();
    session.run("START TRANSACTION READ ONLY").unwrap();
    assert!(session.in_transaction());
    session.run("ROLLBACK").unwrap();

    session
        .run("SET @@tidb_enable_noop_functions = 'ON'")
        .unwrap();
    session.run("START TRANSACTION READ ONLY").unwrap();
    assert!(session.in_transaction());
    session.run("ROLLBACK").unwrap();
}

#[test]
fn historical_read_batch_snapshot_numeric_and_exclusive_settings_follow_go() {
    let mut session = Session::new();
    // Go tests seed the GC safe point a historical read validates against
    // (`safePointSQL`); a store with no GC worker has none.
    session
        .run("INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '20060102-15:04:05.000 -0700', '')")
        .unwrap();
    session
        .run("SET tidb_snapshot='469540215700324352'")
        .unwrap();
    assert_eq!(
        session.vars().get_system("tidb_snapshot").unwrap(),
        "469540215700324352"
    );
    assert!(session
        .run("SET tidb_read_staleness=-1")
        .unwrap_err()
        .to_string()
        .contains("tidb_snapshot should be clear"));
    session.run("SET tidb_snapshot='0'").unwrap();
    session.run("SELECT 1").unwrap();
    session.run("SET tidb_read_staleness=-1").unwrap();
    assert!(session.run("SET tidb_snapshot='0'").is_err());
    assert!(session
        .run("SET tidb_snapshot='469540215700324352'")
        .unwrap_err()
        .to_string()
        .contains("tidb_read_staleness should be clear"));
    session.run("SET tidb_read_staleness=0").unwrap();
    session.run("SET time_zone='+00:00'").unwrap();
    session
        .run("SET tidb_snapshot='2020-01-01 00:00:00'")
        .unwrap();
    let expected = 1_577_836_800_000_u64 << 18;
    assert_eq!(session.vars().snapshot_ts(), expected);
    session.run("SET time_zone='+08:00'").unwrap();
    assert_eq!(session.vars().snapshot_ts(), expected);
    assert!(session.run("SET tidb_snapshot='invalid'").is_err());
    assert_eq!(session.vars().snapshot_ts(), expected);
    assert_eq!(
        session.vars().get_system("tidb_snapshot").unwrap(),
        "2020-01-01 00:00:00"
    );
    session.run("SET tidb_snapshot=DEFAULT").unwrap();
    assert_eq!(session.vars().snapshot_ts(), 0);
}

#[test]
fn snapshot_selection_retains_schema_and_rolls_back_effective_ts_on_failure() {
    let mut session = Session::new();
    session.run("CREATE TABLE snapshot_schema (v INT)").unwrap();
    session
        .run("INSERT INTO snapshot_schema VALUES (7)")
        .unwrap();
    let historical = session.shared_catalog().lock().unwrap().clone();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&calls);
    session.set_snapshot_schema_provider(Arc::new(move |ts, group, validate| {
        observed
            .lock()
            .unwrap()
            .push((ts, group.to_owned(), validate));
        if ts == 200 {
            return Err(DriverError::unsupported("schema unavailable"));
        }
        Ok(historical.clone())
    }));
    session.set_historical_read_provider(Arc::new(|_, _, schema| {
        Ok(HistoricalRead {
            catalog: schema.expect("read must use the SET-time schema").clone(),
            timestamp_hold: Arc::new(()),
        })
    }));
    session.run("SET tidb_snapshot=100").unwrap();
    assert!(
        !session.in_transaction(),
        "SET must not open a user transaction"
    );
    assert!(session.run("SET tidb_snapshot=200").is_err());
    assert_eq!(session.vars().snapshot_ts(), 100);
    assert_eq!(
        session.vars().get_system("tidb_snapshot").unwrap(),
        "200",
        "Go rolls back the typed timestamp, not its assigned system string"
    );
    assert_eq!(
        session.run("SELECT * FROM snapshot_schema").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(7)]])
    );
    session.run("SET tidb_snapshot=100").unwrap();
    assert_eq!(
        *calls.lock().unwrap(),
        vec![
            (100, "default".into(), true),
            (200, "default".into(), true),
            (100, "default".into(), false),
        ]
    );
    session.run("SET tidb_snapshot=DEFAULT").unwrap();
    assert!(session.snapshot_schema.is_none());
    assert_eq!(session.vars().snapshot_ts(), 0);
    assert_eq!(
        calls.lock().unwrap().len(),
        3,
        "clearing does not call storage"
    );
    assert!(session.run("SET tidb_snapshot=200").is_err());
    assert_eq!(session.vars().snapshot_ts(), 0);
    assert!(
        session.prepared_plan_cache_environment().is_some(),
        "cache policy uses effective TS after failed SET"
    );
    assert!(
        !session
            .statement_context(false)
            .index_lookup_push_down_session()
            .historical_read
    );
    session.run("SET tidb_read_staleness=-1").unwrap();
    session
        .run("SET tidb_read_staleness=0, tidb_snapshot=0")
        .unwrap();
    assert!(
        !session
            .statement_context(false)
            .index_lookup_push_down_session()
            .historical_read,
        "numeric zero clears planner historical-read policy"
    );
}

/// Go isolation snapshot tests: a statement snapshot never replaces the active txn.
#[test]
fn snapshot_provider_batch_preserves_transaction_writes_and_timestamp() {
    for mode in ["OPTIMISTIC", "PESSIMISTIC"] {
        let mut session = Session::new();
        session
            .run("CREATE TABLE sp_batch (id INT PRIMARY KEY, v INT)")
            .unwrap();
        session.run("INSERT INTO sp_batch VALUES (1,10)").unwrap();
        let historical = session.shared_catalog().lock().unwrap().clone();
        session.set_snapshot_schema_provider(Arc::new(move |_, _, _| Ok(historical.clone())));
        session.set_historical_read_provider(Arc::new(|ts, _, schema| {
            Ok(HistoricalRead {
                catalog: schema.unwrap().clone(),
                timestamp_hold: Arc::new(tidb_txnkv::ACTIVE_START_TS.hold(ts)),
            })
        }));
        session.run(&format!("BEGIN {mode}")).unwrap();
        session.run("UPDATE sp_batch SET v=20 WHERE id=1").unwrap();
        let ordinary_ts = session.current_tso().value();
        session.run("SAVEPOINT before_snapshot").unwrap();
        session.run("SET tidb_snapshot=100").unwrap();
        assert_eq!(
            session.run("SELECT v FROM sp_batch").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(10)]])
        );
        assert!(session.in_transaction());
        assert_eq!(session.current_tso().value(), ordinary_ts);
        assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&100));
        assert!(session.run("SELECT missing FROM sp_batch").is_err());
        assert!(session.in_transaction());
        session.run("SET tidb_snapshot=''").unwrap();
        assert_eq!(
            session.run("SELECT v FROM sp_batch").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(20)]])
        );
        session
            .run("ROLLBACK TO SAVEPOINT before_snapshot")
            .unwrap();
        session.run("COMMIT").unwrap();
        assert_eq!(
            session.run("SELECT v FROM sp_batch").unwrap(),
            StmtResult::Rows(vec![vec![Datum::Int(20)]])
        );
    }
}

#[test]
fn snapshot_provider_batch_result_close_preserves_outer_transaction_and_cursor_pin() {
    let mut session = Session::new();
    session.run("CREATE TABLE sp_close (v INT)").unwrap();
    session
        .run("INSERT INTO sp_close VALUES (10),(11)")
        .unwrap();
    let historical = session.shared_catalog().lock().unwrap().clone();
    session.set_snapshot_schema_provider(Arc::new(move |_, _, _| Ok(historical.clone())));
    session.set_historical_read_provider(Arc::new(|ts, _, schema| {
        Ok(HistoricalRead {
            catalog: schema.unwrap().clone(),
            timestamp_hold: Arc::new(tidb_txnkv::ACTIVE_START_TS.hold(ts)),
        })
    }));
    session.run("BEGIN").unwrap();
    session.run("SET tidb_snapshot=987654321").unwrap();
    let sql = "SELECT * FROM sp_close";
    let statement = session.parse_statement(sql).unwrap();
    session.bind_committed_snapshot().unwrap();
    let OpenedStatement::Rows(mut result) = session.open_record_set_parsed(statement, sql).unwrap()
    else {
        panic!("query")
    };
    assert!(tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&987654321));
    let mut authority = session.result_materialization_authority();
    let cursor_pin = authority.take_start_ts_guard();
    result.close(&mut session).unwrap();
    result.close(&mut session).unwrap();
    assert!(session.in_transaction());
    assert!(
        tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&987654321),
        "cursor outlives the statement"
    );
    drop(cursor_pin);
    assert!(!tidb_txnkv::ACTIVE_START_TS.snapshot().contains(&987654321));
    session.run("SET tidb_snapshot=''").unwrap();
    session.run("ROLLBACK").unwrap();
}

#[test]
fn deferred_uniqueness_batch_context_scopes_the_live_setting() {
    let mut session = Session::new();
    session.set_connection_id(42);
    session
        .apply_set("SET tidb_constraint_check_in_place_pessimistic=OFF")
        .unwrap();
    assert!(!session
        .statement_context(true)
        .pessimistic_check_in_prewrite());
    session.control_transaction("BEGIN OPTIMISTIC").unwrap();
    assert!(!session
        .statement_context(true)
        .pessimistic_check_in_prewrite());
    session.control_transaction("ROLLBACK").unwrap();
    session.control_transaction("BEGIN PESSIMISTIC").unwrap();
    assert!(session
        .statement_context(true)
        .pessimistic_check_in_prewrite());
    session.set_restricted_sql(true);
    assert!(!session
        .statement_context(true)
        .pessimistic_check_in_prewrite());
    session.set_restricted_sql(false);
    session.set_connection_id(0);
    assert!(!session
        .statement_context(true)
        .pessimistic_check_in_prewrite());
    session.set_connection_id(42);
    session
        .apply_set("SET tidb_constraint_check_in_place_pessimistic=ON")
        .unwrap();
    assert!(!session
        .statement_context(true)
        .pessimistic_check_in_prewrite());
}

/// Go `txnIsolationLevelOneShot`: `SET TRANSACTION ISOLATION LEVEL` gives
/// only the NEXT transaction its level. The SET's own autocommit `CommitTxn`
/// arms it, the transaction's end clears it, a statement that commits in
/// between (even `SELECT 1`) uses it up while one that fails to compile does
/// not, and inside a transaction the SET is refused with 1568. Captured on a
/// Go tidb-server over TiKV by reading a peer's commit twice inside `BEGIN
/// PESSIMISTIC` (RC sees it, RR does not).
#[test]
fn set_transaction_isolation_level_applies_to_the_next_transaction_only() {
    let mut session = Session::new();
    let begin_is_rc = |session: &mut Session| {
        session.run("BEGIN PESSIMISTIC").unwrap();
        let read_committed = session.read_committed_locking();
        session.run("COMMIT").unwrap();
        read_committed
    };
    let set = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED";
    session.run(set).unwrap();
    assert!(begin_is_rc(&mut session));
    assert!(!begin_is_rc(&mut session));
    session.run(set).unwrap();
    session.run("SELECT 1").unwrap();
    assert!(!begin_is_rc(&mut session));
    session.run(set).unwrap();
    assert!(session.run("SELECT * FROM no_such_table").is_err());
    assert!(begin_is_rc(&mut session));
    session.run("BEGIN PESSIMISTIC").unwrap();
    let error = session.run(set).unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1568, "{}", error.message);
    session.run("COMMIT").unwrap();
    assert!(!begin_is_rc(&mut session));
}
