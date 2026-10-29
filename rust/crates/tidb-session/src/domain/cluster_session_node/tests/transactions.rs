//! The statement and transaction lifecycle over cluster storage: what each
//! statement publishes, which snapshot it reads at, and what a failure leaves
//! behind.
//!
//! Mirrors Go's `pkg/session/session.go`: outside `BEGIN` one implicit
//! transaction per statement, inside it one `kv.Transaction` for the whole
//! transaction (repeatable read, and a racing writer rejected at prewrite),
//! with `ROLLBACK TO SAVEPOINT` restoring the mutation buffer. The counters
//! these tests read are the proof the lifecycle actually ran rather than
//! happening to produce the same rows.

use super::super::*;
use super::node_fixture::*;
use std::sync::atomic::Ordering;
use tidb_datatype::Datum;
use tidb_store_driver::pessimistic_lock_error::{commit_outcome_to_sql_error, ERR_WRITE_CONFLICT};
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticTransactionReceipt, TransactionCause,
    UndeterminedTransaction,
};

#[test]
fn pessimistic_statement_retries_obey_config_and_preserve_prior_writes() {
    if crate::domain::isolate_process_globals() {
        return;
    }
    use tidb_config::config_tree::config::update_global;

    // Point writes conflict before execution; range writes conflict after
    // staging their changes. Both must consume the same statement budget.
    for sql in [
        "UPDATE t SET v = v + 1 WHERE id = 1",
        "UPDATE t SET v = v + 1",
    ] {
        for limit in [0, 2] {
            let (mut session, cluster) = open_session();
            session
                .node_conn()
                .execute_write("INSERT INTO t VALUES (1, 10)")
                .unwrap();
            session
                .node_conn()
                .control_transaction("BEGIN PESSIMISTIC")
                .unwrap();
            session
                .node_conn()
                .execute_write("INSERT INTO t VALUES (2, 20)")
                .unwrap();
            update_global(|config| config.pessimistic_txn.max_retry_count = limit);
            cluster.lock_waits.lock().unwrap().clear();
            cluster
                .retry_pessimistic_locks
                .store(limit + 1, Ordering::Release);

            let error = session
                .node_conn()
                .execute_write(sql)
                .expect_err("configured budget is exhausted");
            assert_query_error_packet(&error, 1105, "pessimistic lock retry limit reached");
            assert_eq!(cluster.lock_waits.lock().unwrap().len(), limit + 1);
            assert!(session.in_transaction());
            assert_eq!(
                rows(&mut session, "SELECT id, v FROM t ORDER BY id"),
                vec![
                    vec![Datum::Int(1), Datum::Int(10)],
                    vec![Datum::Int(2), Datum::Int(20)]
                ]
            );

            // Exactly the configured number of retries is allowed. Successful
            // replay applies the write once and retains the earlier INSERT.
            cluster.lock_waits.lock().unwrap().clear();
            cluster
                .retry_pessimistic_locks
                .store(limit, Ordering::Release);
            session.node_conn().execute_write(sql).unwrap();
            session.node_conn().control_transaction("COMMIT").unwrap();
            let second_value = if sql.contains("WHERE") { 20 } else { 21 };
            assert_eq!(
                rows(&mut session, "SELECT id, v FROM t ORDER BY id"),
                vec![
                    vec![Datum::Int(1), Datum::Int(11)],
                    vec![Datum::Int(2), Datum::Int(second_value)]
                ]
            );
        }
    }
}

#[test]
fn restricted_pessimistic_statement_retries_obey_live_config() {
    if crate::domain::isolate_process_globals() {
        return;
    }
    use tidb_config::config_tree::config::update_global;
    use tidb_store_driver::cluster_table_storage::lock_pessimistic_statement_with;
    use tidb_txnkv::transaction::BufferMutation;

    let (session, _) = open_session();
    for (limit, conflicts, lower_during_retry) in [
        (0, 1, false),
        (2, 3, false),
        (2, 2, false),
        (300, 257, false),
        (2, 2, true),
    ] {
        update_global(|config| config.pessimistic_txn.max_retry_count = limit);
        let mut calls = 0;
        let mut read_timestamps = Vec::new();
        let result = lock_pessimistic_statement_with(
            1,
            &tidb_kv::cluster_storage::MutationBuffer::new(),
            |read_ts| {
                read_timestamps.push(read_ts);
                session.node().domain.transactions.open_snapshot("")
            },
            |keys, _, _| {
                calls += 1;
                if lower_during_retry && calls == 2 {
                    update_global(|config| config.pessimistic_txn.max_retry_count = 0);
                }
                if calls <= conflicts {
                    Ok(LockKeysOutcome::RetryStatement {
                        for_update_ts: calls as u64 + 10,
                        newly_locked: keys,
                    })
                } else {
                    Ok(LockKeysOutcome::Locked {
                        for_update_ts: calls as u64 + 10,
                        newly_locked: keys,
                    })
                }
            },
            |_, _| {
                Ok((
                    (),
                    vec![BufferMutation::lock_only(b"key".to_vec()).unwrap()],
                ))
            },
        );
        let exhausted = conflicts > limit || lower_during_retry;
        if exhausted {
            assert_eq!(
                result.unwrap_err().to_string(),
                "pessimistic lock retry limit reached"
            );
        } else {
            assert!(result.is_ok(), "{result:?}");
        }
        let expected = if lower_during_retry {
            2
        } else {
            conflicts.min(limit) + 1
        };
        assert_eq!(calls, expected);
        assert_eq!(read_timestamps.len(), expected);
        assert_eq!(read_timestamps[0], None);
        for (attempt, read_ts) in read_timestamps.iter().enumerate().skip(1) {
            assert_eq!(*read_ts, Some(attempt as u64 + 10));
        }
    }
}

#[test]
fn changing_session_lock_wait_reaches_an_open_transaction() {
    let (mut session, cluster) = open_session();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .unwrap();
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    for seconds in [7, 13] {
        session
            .node_conn()
            .execute_write(&format!("SET SESSION innodb_lock_wait_timeout = {seconds}"))
            .unwrap();
        cluster.lock_waits.lock().unwrap().clear();
        session
            .node_conn()
            .execute_write(&format!("UPDATE t SET v = {seconds} WHERE id = 1"))
            .unwrap();
        let waits = cluster.lock_waits.lock().unwrap();
        assert!(!waits.is_empty());
        assert!(waits.iter().all(|wait| *wait
            == tidb_txnkv::transaction::LockWaitTime::Timeout(Duration::from_secs(seconds))));
    }
    session.node_conn().control_transaction("ROLLBACK").unwrap();
}

/// Go `pkg/executor/executor_failpoint_test.go::TestTxnWriteThroughputSLI`.
#[test]
fn txn_write_throughput_sli_matches_source() {
    const FAILPOINT: &str = "CheckTxnWriteThroughput";

    struct FailpointGuard;
    impl Drop for FailpointGuard {
        fn drop(&mut self) {
            fail::remove(FAILPOINT);
        }
    }

    fn finish(session: &mut Session, affected_rows: u64) {
        let in_txn = session.in_transaction();
        session.txn_write_throughput_sli().finish_execute_stmt(
            1_000_000_000,
            affected_rows,
            in_txn,
        );
    }

    fn write(session: &mut Session, sql: &str) {
        let affected_rows = session
            .node_conn()
            .execute_write(sql)
            .expect("statement succeeds")
            .expect("write statement")
            .affected_rows;
        finish(session, affected_rows);
    }

    fn control(session: &mut Session, sql: &str) {
        session
            .node_conn()
            .control_transaction(sql)
            .expect("statement succeeds");
        finish(session, 0);
    }

    fn reset(session: &mut Session) {
        session.txn_write_throughput_sli().reset();
    }

    fn state(session: &mut Session) -> String {
        session.txn_write_throughput_sli().to_string()
    }

    fail::cfg(FAILPOINT, "return").expect("enable failpoint");
    let _guard = FailpointGuard;
    let (mut session, _) = open_session();

    write(&mut session, "INSERT INTO t (id, v) VALUES (1, 3), (2, 4)");
    assert!(!session.txn_write_throughput_sli().is_invalid());
    assert!(session.txn_write_throughput_sli().is_small_txn());
    assert_eq!(
        state(&mut session),
        "invalid: false, affectRow: 2, writeSize: 58, readKeys: 0, writeKeys: 2, writeTime: 1s"
    );
    reset(&mut session);

    write(&mut session, "INSERT INTO t SELECT v, id FROM t");
    assert!(session.txn_write_throughput_sli().is_invalid());
    assert!(session.txn_write_throughput_sli().is_small_txn());
    assert_eq!(
        state(&mut session),
        "invalid: true, affectRow: 2, writeSize: 58, readKeys: 2, writeKeys: 2, writeTime: 1s"
    );
    reset(&mut session);

    write(&mut session, "DELETE FROM t");
    assert_eq!(
        state(&mut session),
        "invalid: false, affectRow: 4, writeSize: 76, readKeys: 4, writeKeys: 4, writeTime: 1s"
    );
    reset(&mut session);

    control(&mut session, "BEGIN");
    for value in 0..20 {
        write(
            &mut session,
            &format!("INSERT INTO t (id, v) VALUES ({value}, {value})"),
        );
        assert!(session.txn_write_throughput_sli().is_small_txn());
    }
    let _ = rows(&mut session, "SELECT COUNT(*) FROM t");
    finish(&mut session, 0);
    let _ = rows(&mut session, "SELECT * FROM t");
    finish(&mut session, 0);
    write(&mut session, "INSERT INTO t (id, v) VALUES (20, 20)");
    assert!(!session.txn_write_throughput_sli().is_small_txn());
    control(&mut session, "COMMIT");
    assert!(!session.txn_write_throughput_sli().is_invalid());
    assert_eq!(
        state(&mut session),
        "invalid: false, affectRow: 21, writeSize: 609, readKeys: 0, writeKeys: 21, writeTime: 22s"
    );
    reset(&mut session);

    write(&mut session, "DELETE FROM t");
    reset(&mut session);
    control(&mut session, "BEGIN");
    write(&mut session, "INSERT INTO t (id, v) VALUES (1, 3), (2, 4)");
    write(&mut session, "REPLACE INTO t SELECT v, id FROM t");
    control(&mut session, "COMMIT");
    assert!(session.txn_write_throughput_sli().is_invalid());
    assert_eq!(
        state(&mut session),
        "invalid: true, affectRow: 4, writeSize: 116, readKeys: 0, writeKeys: 4, writeTime: 3s"
    );
    reset(&mut session);

    fail::remove(FAILPOINT);
    control(&mut session, "BEGIN");
    write(&mut session, "INSERT INTO t (id, v) VALUES (1, 3), (2, 4)");
    assert!(session.node_conn().control_transaction("COMMIT").is_err());
    finish(&mut session, 0);
    assert_eq!(
        state(&mut session),
        "invalid: false, affectRow: 0, writeSize: 0, readKeys: 0, writeKeys: 0, writeTime: 0s"
    );

    fail::cfg(FAILPOINT, "return").expect("enable failpoint");
    control(&mut session, "BEGIN");
    write(&mut session, "INSERT INTO t (id, v) VALUES (5, 6)");
    control(&mut session, "COMMIT");
    assert_eq!(
        state(&mut session),
        "invalid: false, affectRow: 1, writeSize: 29, readKeys: 0, writeKeys: 1, writeTime: 2s"
    );

    reset(&mut session);
    assert_eq!(
        state(&mut session),
        "invalid: false, affectRow: 0, writeSize: 0, readKeys: 0, writeKeys: 0, writeTime: 0s"
    );
}

/// Go `pkg/store/driver/error.ToTiDBErr` preserves the result-undetermined
/// identity all the way to `pkg/server`, where the connection closes without
/// sending an ERR packet. The cluster transaction path used to replace that
/// identity with an ordinary 1105, inviting the client to retry a commit that
/// may already have landed.
#[test]
fn an_undetermined_cluster_commit_reaches_the_connection_fatal_identity() {
    let outcome = OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
        receipt: OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1),
        cause: TransactionCause::Transport {
            detail: "commit response lost".to_owned(),
        },
    });
    let lock_error = commit_outcome_to_sql_error(&outcome)
        .expect_err("an ambiguous commit cannot answer success");
    let query_error = super::super::sql_error(lock_error);
    assert_undetermined_closes_without_packet(&query_error);
}

/// Autocommit: each statement publishes its own writes, and each statement
/// reads at its own snapshot. The snapshot count is the proof that the
/// per-statement lifecycle actually runs.
#[test]
fn autocommit_publishes_each_statement_and_takes_a_fresh_snapshot() {
    let (mut session, cluster) = open_session();
    let outcome = session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert")
        .expect("a write answers with an OK packet");
    assert_eq!(outcome.affected_rows, 3);
    assert_eq!(cluster.rows(), 3);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 1);

    // The next statement reads the published rows through a NEW snapshot.
    let opened_before = cluster.opened.load(Ordering::Acquire);
    let mut result = session
        .node_conn()
        .execute("SELECT id, v FROM t ORDER BY id DESC")
        .unwrap();
    let source = result.source();
    let mut chunk = source.new_chunk().expect("query owns native chunks");
    source.next_chunk(&mut chunk).unwrap();
    assert_eq!(
        cluster.live.load(Ordering::Acquire),
        1,
        "snapshot outlives Next"
    );
    let selected: Vec<_> = (0..chunk.num_rows())
        .map(|i| chunk.get_row(i).get_datum_row(source.field_types()))
        .collect();
    source.next_chunk(&mut chunk).unwrap();
    assert_eq!(chunk.num_rows(), 0);
    assert_eq!(cluster.live.load(Ordering::Acquire), 1, "EOF is not Finish");
    source.finish().unwrap();
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
    // Go adapter_internal_test: metadata/NewChunk survive Finish; Next does not.
    assert_eq!(source.new_chunk().unwrap().num_cols(), 2);
    assert_eq!(source.next_chunk(&mut chunk).unwrap_err().code, 1317);
    source.close().unwrap();
    drop(result);
    // The same result owner closes its snapshot if the client abandons rows.
    let mut result = session.node_conn().execute("SELECT id FROM t").unwrap();
    let mut chunk = result.source().new_chunk().unwrap();
    result.source().next_chunk(&mut chunk).unwrap();
    assert_eq!(cluster.live.load(Ordering::Acquire), 1);
    drop(result);
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
    assert_eq!(selected.len(), 3);
    assert_eq!(selected[0], vec![Datum::Int(3), Datum::Int(30)]);
    assert_eq!(cluster.opened.load(Ordering::Acquire), opened_before + 2);
    // A read publishes nothing.
    assert_eq!(cluster.publications.load(Ordering::Acquire), 1);
    // Every statement's snapshot was finished; none is still bound.
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
}

/// An explicit transaction stages every statement's writes and publishes
/// them exactly once, at COMMIT.
#[test]
fn an_explicit_transaction_publishes_once_at_commit() {
    let (mut session, cluster) = open_session();
    assert_eq!(
        session.node_conn().control_transaction("BEGIN").unwrap(),
        Some(true)
    );
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("first insert");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("second insert");
    // Staged, not published: the cluster still holds nothing.
    assert_eq!(cluster.rows(), 0);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
    // The transaction's own statements still see their staged rows,
    // through the buffer in front of the snapshot.
    assert_eq!(rows(&mut session, "SELECT id FROM t").len(), 2);

    assert_eq!(
        session.node_conn().control_transaction("COMMIT").unwrap(),
        Some(false)
    );
    assert_eq!(cluster.rows(), 2);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 1);
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
}

#[test]
fn table_delta_follows_commit_rollback_and_savepoint_lifecycle() {
    let node = MockNode::start();
    let (mut session, usage) = open_session_on_with_usage(&node);
    let table_id = node
        .catalog
        .load()
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "app")
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|table| table.name.lowercase() == "t")
        })
        .expect("app.t exists")
        .id;

    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("first insert");
    session
        .node_conn()
        .control_transaction("SAVEPOINT before_second")
        .expect("savepoint");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("second insert");
    session
        .node_conn()
        .control_transaction("ROLLBACK TO SAVEPOINT before_second")
        .expect("rollback to savepoint");
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");

    let committed = usage
        .session_stats_list()
        .begin_table_delta_dump()
        .get(table_id)
        .expect("committed delta is collected");
    assert_eq!(committed.delta, 1);
    assert_eq!(committed.count, 1);

    usage.session_stats_list().session_table_delta().reset();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("insert before rollback");
    session
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
    assert!(usage
        .session_stats_list()
        .begin_table_delta_dump()
        .get(table_id)
        .is_none());
}

/// One `BEGIN` takes one timestamp, and every statement until `COMMIT`
/// reads through that same transaction rather than opening its own.
#[test]
fn an_explicit_transaction_holds_one_transaction_for_every_statement() {
    let (mut session, cluster) = open_session();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    let autocommit_snapshots = cluster.opened.load(Ordering::Acquire);

    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    assert_eq!(cluster.begun.load(Ordering::Acquire), 1);
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 1);
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 1);
    session
        .node_conn()
        .execute_write("UPDATE t SET v = 11 WHERE id = 1")
        .expect("update");
    // Not one of those statements opened a transaction of its own.
    assert_eq!(cluster.begun.load(Ordering::Acquire), 1);
    assert_eq!(
        cluster.opened.load(Ordering::Acquire),
        autocommit_snapshots,
        "a statement inside BEGIN must not take a timestamp of its own"
    );
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
}

/// Go `TIDB_CURRENT_TSO()` reads the session transaction context rather than
/// allocating a timestamp of its own. An autocommit statement publishes the
/// timestamp its first table read opened, while an explicit transaction keeps
/// one value until COMMIT and then returns to zero.
#[test]
fn current_tso_follows_the_cluster_transaction_lifecycle() {
    let (mut session, cluster) = open_session();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    let autocommit = rows(
        &mut session,
        "SELECT TIDB_CURRENT_TSO(), v FROM t ORDER BY id",
    );
    let Datum::Int(autocommit_tso) = autocommit[0][0] else {
        panic!("TIDB_CURRENT_TSO must return a signed integer")
    };
    assert!(autocommit_tso > 0, "the table read opens the statement TSO");
    assert_eq!(
        autocommit_tso as u64,
        cluster.clock.load(Ordering::Acquire),
        "the builtin reports the timestamp of the read that produced its row"
    );
    assert_eq!(
        rows(&mut session, "SELECT TIDB_CURRENT_TSO()"),
        vec![vec![Datum::Int(0)]],
        "an autocommit statement does not retain its completed transaction"
    );

    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    let first = rows(&mut session, "SELECT TIDB_CURRENT_TSO()");
    let second = rows(&mut session, "SELECT TIDB_CURRENT_TSO()");
    assert_eq!(first, second, "one explicit transaction keeps one TSO");
    assert!(matches!(first[0][0], Datum::Int(value) if value > 0));

    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("a second begin implicitly commits and starts anew");
    let replacement = rows(&mut session, "SELECT TIDB_CURRENT_TSO()");
    assert_ne!(replacement, first, "a new transaction takes a new TSO");

    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    assert_eq!(
        rows(&mut session, "SELECT TIDB_CURRENT_TSO()"),
        vec![vec![Datum::Int(0)]]
    );
}

/// Go `executorBuilder.buildShow` activates the transaction for `SHOW MASTER
/// STATUS` ("show master status need start ts") and `fetchShowMasterStatus`
/// reports `TxnCtx.StartTS`: an autocommit statement that reads no table
/// still answers the timestamp it activated, and an explicit transaction
/// answers its own.
#[test]
fn show_master_status_reports_the_activated_start_ts() {
    let (mut session, cluster) = open_session();
    let status = rows(&mut session, "SHOW MASTER STATUS");
    let Datum::Int(position) = status[0][1] else {
        panic!("SHOW MASTER STATUS's Position is a signed integer")
    };
    assert!(position > 0, "the statement activates its transaction");
    assert_eq!(position as u64, cluster.clock.load(Ordering::Acquire));

    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    let current = rows(&mut session, "SELECT TIDB_CURRENT_TSO()");
    let status = rows(&mut session, "SHOW MASTER STATUS");
    assert_eq!(status[0][1], current[0][0]);
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
}

/// Repeatable read, which holding one transaction is what buys: a statement
/// inside `BEGIN` cannot see a commit made after `BEGIN`, because there is
/// no newer timestamp for it to see it at. Go's default isolation level.
#[test]
fn a_statement_inside_begin_does_not_see_a_commit_made_after_it() {
    let (mut reader, cluster) = open_session();
    reader
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    reader
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    assert_eq!(
        rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );

    let mut writer = open_session_on(&cluster);
    writer
        .node_conn()
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the outside writer commits");
    assert_eq!(
        rows(&mut writer, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]],
        "the outside writer's own commit is durable"
    );

    assert_eq!(
        rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]],
        "a repeatable read must not see a commit made after BEGIN"
    );
    reader
        .node_conn()
        .control_transaction("ROLLBACK")
        .expect("rollback");
    // And once the transaction is over, the session is back at the newest
    // committed state.
    assert_eq!(
        rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]]
    );
}

/// The conflict detection the single `start_ts` exists for: an optimistic
/// transaction that read a row another transaction then committed cannot
/// publish over it. Go reports 9007 at COMMIT, and the writes are gone.
#[test]
fn an_explicit_transaction_that_lost_the_race_fails_at_commit() {
    let (mut loser, cluster) = open_session();
    // A commit-time write conflict is an optimistic transaction contract.
    loser
        .node_conn()
        .execute_write("SET SESSION tidb_txn_mode = 'optimistic'")
        .expect("select optimistic conflict semantics");
    loser
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    loser
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    assert_eq!(
        rows(&mut loser, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );

    let mut winner = open_session_on(&cluster);
    winner
        .node_conn()
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the racing writer commits first");

    loser
        .node_conn()
        .execute_write("UPDATE t SET v = 50 WHERE id = 1")
        .expect("the statement itself succeeds; nothing is published yet");
    let error = loser
        .node_conn()
        .control_transaction("COMMIT")
        .expect_err("a prewrite at the BEGIN timestamp must lose to a newer commit");
    // The code, not just the text: the client is told 9007, which is the
    // one thing a caller that only looked for `Err` from the coordinator
    // could never report.
    assert_eq!(error.code, ERR_WRITE_CONFLICT, "{}", error.message);
    // Go sends `GetMsg()`, not `Error()`: no `[kv:9007]` prefix. And Go's
    // `tryReplaceWriteConflictError` names the table of both keys from the
    // transaction's infoschema.
    assert!(
        error.message.starts_with("Write conflict, txnStartTS="),
        "a lost race is a write conflict, got: {}",
        error.message
    );
    assert!(
        error
            .message
            .contains("key={tableID=101, tableName=app.t, handle=1}, originalKey="),
        "the conflicting key names its table, got: {}",
        error.message
    );
    assert!(
        error
            .message
            .contains("primary={tableID=101, tableName=app.t, handle=1}, originalPrimaryKey="),
        "the primary key names its table, got: {}",
        error.message
    );
    assert!(
        error
            .message
            .ends_with(", reason=Optimistic [try again later]"),
        "{}",
        error.message
    );

    // The winner's row stands, and the loser staged nothing for the next
    // statement to publish by accident.
    assert_eq!(
        rows(&mut loser, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]]
    );
    assert_eq!(cluster.publications.load(Ordering::Acquire), 2);
}

/// The `Error` row `SHOW WARNINGS` / `SHOW ERRORS` report after a failure,
/// followed by the counts `@@warning_count` / `@@error_count` read back.
fn assert_failure_filed(session: &mut Session, error: &SqlQueryError) {
    // Go `buildShowWarningsSchema`: `Code` is an UNSIGNED column.
    let row = vec![
        Datum::new_string("Error"),
        Datum::UInt(u64::from(error.code)),
        Datum::new_string(error.message.clone()),
    ];
    assert_eq!(rows(session, "SHOW WARNINGS"), vec![row.clone()]);
    assert_eq!(rows(session, "SHOW ERRORS"), vec![row]);
    let counts: Vec<Vec<String>> = rows(session, "SELECT @@warning_count, @@error_count")
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|datum| match datum {
                    Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
                    other => panic!("a system variable reads as text, got {other:?}"),
                })
                .collect()
        })
        .collect();
    assert_eq!(counts, vec![vec!["1".to_owned(), "1".to_owned()]]);
}

/// Go `TiDBContext.ExecuteStmt` (`pkg/server/driver_tidb.go:376`) appends the
/// COMMIT's error to the statement context, so after a 9007 at COMMIT both
/// `SHOW WARNINGS` and `SHOW ERRORS` carry the conflict (Go live, `BEGIN
/// OPTIMISTIC; UPDATE ...; COMMIT` losing to another session's UPDATE:
/// `Error | 9007 | Write conflict, txnStartTS=...`, and `@@warning_count`,
/// `@@error_count` both 1).
#[test]
fn a_failed_commit_files_its_error_in_the_warning_buffer() {
    let (mut loser, cluster) = open_session();
    loser
        .node_conn()
        .execute_write("SET SESSION tidb_txn_mode = 'optimistic'")
        .expect("select optimistic conflict semantics");
    loser
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    loser
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    loser
        .node_conn()
        .execute_write("UPDATE t SET v = 50 WHERE id = 1")
        .expect("staged");
    open_session_on(&cluster)
        .node_conn()
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the racing writer commits first");

    let error = loser
        .node_conn()
        .control_transaction("COMMIT")
        .expect_err("the prewrite loses to the newer commit");
    assert_eq!(error.code, ERR_WRITE_CONFLICT, "{}", error.message);
    assert_failure_filed(&mut loser, &error);
}

/// Go `handlePessimisticSelectForUpdate` locks inside `ExecStmt.Exec`, so the
/// lock failure of a `SELECT ... FOR UPDATE` is an `ExecuteStmt` error that
/// `driver_tidb.go:376` appends (Go live: `Error | 3572 | Statement aborted
/// because lock(s) could not be acquired immediately and NOWAIT is set.` after
/// `FOR UPDATE NOWAIT`, `Error | 1205 | Lock wait timeout exceeded; try
/// restarting transaction` after a timed-out `FOR UPDATE`). Both the point
/// read, which locks before it runs, and the range read, which locks the rows
/// it selected, file the row.
#[test]
fn a_failed_locking_read_files_its_error_in_the_warning_buffer() {
    use tidb_txnkv::transaction::PessimisticLockFailure;
    for (sql, failure, code) in [
        (
            "SELECT v FROM t WHERE id = 1 FOR UPDATE NOWAIT",
            PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key: b"k".to_vec() },
            3572,
        ),
        (
            "SELECT v FROM t WHERE id = 1 FOR UPDATE",
            PessimisticLockFailure::LockWaitTimeout { key: b"k".to_vec() },
            1205,
        ),
        (
            "SELECT v FROM t WHERE id >= 1 FOR UPDATE",
            PessimisticLockFailure::LockWaitTimeout { key: b"k".to_vec() },
            1205,
        ),
    ] {
        let (mut session, cluster) = open_session();
        session
            .node_conn()
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("seed");
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        *cluster.fail_next_lock.lock().unwrap() = Some(failure);
        let error = match session.node_conn().execute(sql) {
            Ok(_) => panic!("{sql}: the lock is refused"),
            Err(error) => error,
        };
        assert_eq!(error.code, code, "{sql}: {}", error.message);
        assert_failure_filed(&mut session, &error);
    }
}

/// A locking read whose failure the session already filed (planning's 1146)
/// carries that one row, not a second copy from the node's door.
#[test]
fn a_locking_read_error_the_session_filed_is_not_filed_twice() {
    let (mut session, _cluster) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN PESSIMISTIC")
        .unwrap();
    let error = match session
        .node_conn()
        .execute("SELECT v FROM nosuch WHERE id = 1 FOR UPDATE")
    {
        Ok(_) => panic!("the table does not exist"),
        Err(error) => error,
    };
    assert_eq!(error.code, 1146, "{}", error.message);
    assert_failure_filed(&mut session, &error);
}

/// The same race under autocommit publishes normally: each statement is its
/// own transaction at its own timestamp, so there is no older `start_ts` to
/// conflict with. Nothing about autocommit changed.
#[test]
fn autocommit_takes_a_fresh_timestamp_and_does_not_conflict() {
    let (mut first, cluster) = open_session();
    first
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    let mut second = open_session_on(&cluster);
    second
        .node_conn()
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("second writer");
    first
        .node_conn()
        .execute_write("UPDATE t SET v = 50 WHERE id = 1")
        .expect("an autocommit write reads and publishes at fresh timestamps");
    assert_eq!(
        rows(&mut first, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(50)]]
    );
    assert_eq!(cluster.begun.load(Ordering::Acquire), 0);
}

#[test]
fn rollback_discards_the_transactions_writes() {
    let (mut session, cluster) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("insert");
    assert_eq!(
        session.node_conn().control_transaction("ROLLBACK").unwrap(),
        Some(false)
    );
    assert_eq!(cluster.rows(), 0);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
    // And the discarded row is gone from the session's own view too.
    assert!(rows(&mut session, "SELECT id FROM t").is_empty());
}

/// A statement that fails inside a transaction takes back only its own
/// writes; the transaction's earlier statements survive to COMMIT. This is
/// the buffer savepoint doing its job.
#[test]
fn a_failed_statement_keeps_the_transactions_earlier_writes() {
    let (mut session, cluster) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("first insert");
    // A write naming a column the table does not have: the statement fails
    // after the session has already opened its snapshot.
    assert!(session
        .node_conn()
        .execute_write("INSERT INTO t (id, nosuch) VALUES (2, 20)")
        .is_err());
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("third insert");
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");

    let published = rows(&mut session, "SELECT id FROM t ORDER BY id");
    assert_eq!(published, vec![vec![Datum::Int(1)], vec![Datum::Int(3)]]);
    assert_eq!(cluster.rows(), 2);
    // The failed statement finished its read transaction like any other.
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
}

/// A failure outside any transaction publishes nothing at all: autocommit
/// only flushes a statement that succeeded.
#[test]
fn a_failed_autocommit_statement_publishes_nothing() {
    let (mut session, cluster) = open_session();
    assert!(session
        .node_conn()
        .execute_write("INSERT INTO t (id, nosuch) VALUES (2, 20)")
        .is_err());
    assert_eq!(cluster.rows(), 0);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
    // The connection is still usable, with an empty buffer.
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (7, 70)")
        .expect("the next statement still runs");
    assert_eq!(cluster.rows(), 1);
}

/// The staged record handles of one table, in key order: the row handles
/// this session would publish if it committed right now.
fn staged_handles(session: &Session, table_id: i64) -> Vec<i64> {
    session
        .node()
        .buffer
        .snapshot()
        .into_iter()
        .filter_map(|(key, _)| {
            match tidb_codec::table_key::decode_record_key(key.as_bytes()) {
                Ok((id, tidb_codec::table_key::RecordHandle::Int(handle)))
                    if id == table_id =>
                {
                    Some(handle)
                }
                _ => None,
            }
        })
        .collect()
}

/// A statement that fails AFTER staging some of its own rows leaves the
/// mutation buffer byte-for-byte where it found it.
///
/// This asserts the property on the cluster seam itself, which no other
/// test here does. It matters because the guard is not the one a reader of
/// [`crate::Session`] would assume: a cluster-backed
/// `TableStorage::clone_box` clones `Arc` HANDLES, so the catalog image
/// the session restores on a failed statement cannot take back a staged
/// row -- the image and the original write into the SAME buffer. What
/// takes the row back is [`Session::with_statement`]'s
/// savepoint, and this is the test that fails when it is removed.
///
/// The failure shape is the load-bearing part: `VALUES (1,10),(2,20),
/// (3,99)` stages two rows and only then hits the duplicate handle, so a
/// missing savepoint leaves REAL bytes behind. The sibling tests all fail
/// their statement during planning, which stages nothing and therefore
/// passes either way.
#[test]
fn a_failed_statement_leaves_no_bytes_of_its_own_in_the_mutation_buffer() {
    let (mut session, cluster) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("first insert");
    let staged_before = session.node().buffer.snapshot();
    assert_eq!(staged_handles(&session, 101), vec![3]);

    assert!(session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 99)")
        .is_err());
    // Not merely "no new rows are visible": the staged bytes ARE the ones
    // the failing statement started from.
    assert_eq!(session.node().buffer.snapshot(), staged_before);

    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    assert_eq!(cluster.rows(), 1);
    assert_eq!(
        rows(&mut session, "SELECT id FROM t"),
        vec![vec![Datum::Int(3)]]
    );
}

/// A failed statement does NOT give back the `_tidb_rowid` handles it
/// consumed, for the same reason it does not give back an AUTO_INCREMENT id:
/// Go allocates both from one counter that lives outside transaction
/// semantics.
///
/// Captured through `gorun` on `CREATE TABLE hnd (v INT UNIQUE KEY)` --
/// the statements below verbatim: after `(20), (10)` fails on the unique
/// index the next row lands on `_tidb_rowid` 4, not 2, because the failed
/// statement burned 2 and 3 before it stopped.
///
/// The catalog image the session restores takes the ROWS back, which is
/// what the surviving `SELECT` asserts; the counter it allocated from is a
/// shared cell the image keeps pointing at, so the burn stays.
#[test]
fn a_failed_statement_burns_the_row_handles_it_consumed() {
    let (mut session, cluster) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    session
        .node_conn()
        .execute_write("INSERT INTO hnd (v) VALUES (10)")
        .expect("first insert");
    assert_eq!(staged_handles(&session, 105), vec![1]);

    // Row `20` stages at handle 2 and row `10` takes handle 3 before it
    // duplicates the unique index and ends the statement.
    assert!(session
        .node_conn()
        .execute_write("INSERT INTO hnd (v) VALUES (20), (10)")
        .is_err());
    assert_eq!(staged_handles(&session, 105), vec![1]);

    session
        .node_conn()
        .execute_write("INSERT INTO hnd (v) VALUES (30)")
        .expect("third insert");
    // Handle 4: the two the failed statement consumed are gone for good.
    assert_eq!(staged_handles(&session, 105), vec![1, 4]);

    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    assert_eq!(
        rows(&mut session, "SELECT v FROM hnd ORDER BY v"),
        vec![vec![Datum::Int(10)], vec![Datum::Int(30)]]
    );
    assert!(cluster.rows() > 0);
}

/// `ROLLBACK TO` takes the mutation buffer back to the savepoint's own
/// bytes, leaves the transaction OPEN, and lets `COMMIT` publish exactly
/// the writes that survived.
///
/// This is the cluster-path counterpart of `tidb_session`'s savepoint
/// tests, and it is a separate test for the same reason the statement
/// savepoint above is: the session's catalog image cannot roll a cluster
/// write back, so only a buffer image proves anything here.
#[test]
fn rollback_to_a_savepoint_restores_the_buffer_and_keeps_the_transaction_open() {
    let (mut session, cluster) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("first insert");
    assert_eq!(
        session
            .node_conn()
            .control_transaction("SAVEPOINT s1")
            .unwrap(),
        Some(true)
    );
    let staged_at_savepoint = session.node().buffer.snapshot();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("second insert");

    // ROLLBACK TO reports the transaction still open, and the buffer holds
    // the savepoint's bytes -- not merely "row 2 is invisible".
    assert_eq!(
        session
            .node_conn()
            .control_transaction("ROLLBACK TO s1")
            .unwrap(),
        Some(true)
    );
    assert_eq!(session.node().buffer.snapshot(), staged_at_savepoint);
    assert_eq!(staged_handles(&session, 101), vec![1]);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);

    // The transaction is still running, so this statement joins it.
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("third insert");
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    assert_eq!(
        rows(&mut session, "SELECT id FROM t ORDER BY id"),
        vec![vec![Datum::Int(1)], vec![Datum::Int(3)]]
    );
    assert_eq!(cluster.rows(), 2);
}

/// The stack rules on the cluster path: a savepoint survives its own
/// rollback, `RELEASE` drops the named one and those above it without
/// touching bytes, and an unknown name is Go's 1305.
#[test]
fn the_savepoint_stack_follows_the_same_rules_on_the_cluster_path() {
    let (mut session, _cluster) = open_session();
    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    session
        .node_conn()
        .control_transaction("SAVEPOINT s1")
        .expect("s1");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("insert");
    session
        .node_conn()
        .control_transaction("SAVEPOINT s2")
        .expect("s2");

    // Releasing s1 drops s2 with it and keeps row 1 staged.
    session
        .node_conn()
        .control_transaction("RELEASE SAVEPOINT s1")
        .expect("release");
    assert_eq!(staged_handles(&session, 101), vec![1]);
    for sql in ["ROLLBACK TO s1", "ROLLBACK TO s2"] {
        let reported = session.node_conn().control_transaction(sql).unwrap_err();
        assert!(
            format!("{reported:?}").contains("1305"),
            "{sql} did not report 1305: {reported:?}"
        );
    }

    // A fresh savepoint, rolled back to twice: it survives its own
    // rollback, matched case-insensitively.
    session
        .node_conn()
        .control_transaction("SAVEPOINT S3")
        .expect("s3");
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("insert");
    session
        .node_conn()
        .control_transaction("ROLLBACK TO s3")
        .expect("first");
    assert_eq!(staged_handles(&session, 101), vec![1]);
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("insert");
    session
        .node_conn()
        .control_transaction("ROLLBACK TO s3")
        .expect("second");
    assert_eq!(staged_handles(&session, 101), vec![1]);
}

/// Ending the transaction takes the savepoint stack with it, so a name
/// cannot outlive the transaction that declared it and reach into the
/// next one's buffer.
#[test]
fn ending_the_transaction_clears_the_cluster_savepoint_stack() {
    for ending in ["ROLLBACK", "COMMIT"] {
        let (mut session, _cluster) = open_session();
        session
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin");
        session
            .node_conn()
            .control_transaction("SAVEPOINT s1")
            .expect("s1");
        session
            .node_conn()
            .control_transaction(ending)
            .expect("ending");
        // The stack is the transaction's `TxnCtx.Savepoints`, which ends with it.
        assert!(
            !session.in_transaction(),
            "{ending} kept the transaction context"
        );
        session
            .node_conn()
            .control_transaction("BEGIN")
            .expect("begin");
        assert!(
            session
                .node_conn()
                .control_transaction("ROLLBACK TO s1")
                .is_err(),
            "{ending} left a savepoint reachable"
        );
    }
}

/// A refused publication does not leave the writes staged for the next
/// statement to publish by accident.
#[test]
fn a_refused_publication_drops_the_staged_writes() {
    let (mut session, cluster) = open_session();
    cluster.fail_commit.store(true, Ordering::Release);
    assert!(session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .is_err());
    assert_eq!(cluster.rows(), 0);

    cluster.fail_commit.store(false, Ordering::Release);
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("the next statement publishes only its own row");
    assert_eq!(cluster.rows(), 1);
    assert_eq!(
        rows(&mut session, "SELECT id FROM t"),
        vec![vec![Datum::Int(2)]]
    );
}

/// Binding starts no transaction. Post-plan warmup may request a timestamp, but
/// a statement that reads no cluster row never waits for or opens its snapshot.
///
/// This is what the pending handle buys: the timestamp policy is settled before
/// warmup, and an unused future never becomes session transaction state.
#[test]
fn a_statement_that_reads_no_cluster_row_opens_no_snapshot() {
    let (mut session, cluster) = open_session();
    let opened_before = cluster.opened.load(Ordering::Acquire);
    assert_eq!(rows(&mut session, "SELECT 1").len(), 1);
    assert_eq!(
        cluster.opened.load(Ordering::Acquire),
        opened_before,
        "a statement that touched no table opened a read transaction"
    );
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);

    // The control, in the other direction: a statement that DOES read a table
    // still opens exactly one.
    let opened_before = cluster.opened.load(Ordering::Acquire);
    let _ = rows(&mut session, "SELECT id FROM t");
    assert_eq!(cluster.opened.load(Ordering::Acquire), opened_before + 1);
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
}

/// Deferring the open must not turn one statement into several snapshots:
/// every read of a statement that reads more than once goes through the one
/// transaction its first read opened.
///
/// A per-read open would be a silent wrong answer -- two halves of one
/// statement reading at two timestamps -- and it would still look like a
/// saving in any counter that watches only single-read statements.
#[test]
fn every_read_of_one_statement_shares_the_transaction_the_first_read_opened() {
    let (mut session, cluster) = open_session();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20)")
        .expect("seed");

    let opened_before = cluster.opened.load(Ordering::Acquire);
    let joined = rows(
        &mut session,
        "SELECT a.id FROM t AS a JOIN t AS b ON a.id = b.id ORDER BY a.id",
    );
    assert_eq!(joined.len(), 2);
    assert_eq!(
        cluster.opened.load(Ordering::Acquire),
        opened_before + 1,
        "a statement reading two relations opened more than one snapshot"
    );
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
}

/// The in-transaction direction, restated against the deferred bind: inside
/// `BEGIN` a statement reads through the transaction's own handle, so the
/// deferral cannot reach it and cannot hand a statement a newer timestamp
/// than the one `BEGIN` took. Repeatable read survives the change.
#[test]
fn deferral_does_not_reach_a_statement_inside_a_transaction() {
    let (mut session, cluster) = open_session();
    let mut writer = open_session_on(&cluster);
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 1);
    writer
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("a racing commit lands after BEGIN");
    // Counted after the writer's own autocommit statement, so what follows
    // measures only the reader's in-transaction statement.
    let autocommit_snapshots = cluster.opened.load(Ordering::Acquire);
    // Still one row: the transaction reads at the timestamp BEGIN took.
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 1);
    assert_eq!(
        cluster.opened.load(Ordering::Acquire),
        autocommit_snapshots,
        "a statement inside BEGIN opened an autocommit snapshot of its own"
    );
    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    // Outside the transaction the same connection sees the racing row.
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 2);
}

/// Go's executor never reads the completion type (captured from Go on this
/// branch: `COMMIT AND CHAIN` commits, `ROLLBACK RELEASE` rolls back). Both
/// halves -- the session's transaction and the node's staged writes -- end
/// together, so nothing is stranded in a transaction no statement belongs to.
#[test]
fn completion_types_end_both_halves_like_a_plain_commit() {
    let (mut session, cluster) = open_session();

    assert_eq!(
        session.node_conn().control_transaction("BEGIN").unwrap(),
        Some(true)
    );
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("insert");
    assert_eq!(cluster.rows(), 0, "the write is staged, not published");
    assert_eq!(
        session
            .node_conn()
            .control_transaction("COMMIT AND CHAIN")
            .unwrap(),
        Some(false)
    );
    assert!(!session.in_transaction());
    assert_eq!(cluster.rows(), 1);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 1);

    assert_eq!(
        session.node_conn().control_transaction("BEGIN").unwrap(),
        Some(true)
    );
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("insert");
    assert_eq!(
        session
            .node_conn()
            .control_transaction("ROLLBACK RELEASE")
            .unwrap(),
        Some(false)
    );
    assert!(!session.in_transaction());
    assert_eq!(cluster.rows(), 1);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 1);
}

/// A `BEGIN` spelling this node cannot honor is refused the same way, and
/// leaves the session outside any transaction rather than inside one this
/// node never opened.
#[test]
fn missing_historical_storage_opens_no_half_transaction() {
    let (mut session, cluster) = open_session();
    let opened_before = cluster.opened.load(Ordering::Acquire);
    let error = session
        .node_conn()
        .control_transaction("START TRANSACTION READ ONLY AS OF TIMESTAMP '2020-01-01 00:00:00'")
        .expect_err("a historical read this node cannot serve is refused");
    assert!(
        format!("{error:?}").contains("historical snapshots are unavailable"),
        "got {error:?}"
    );
    assert!(!session.in_transaction());
    assert_eq!(cluster.opened.load(Ordering::Acquire), opened_before);

    // The session still works as an autocommit one: the write publishes.
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("insert")
        .expect("a write answers with an OK packet");
    assert_eq!(cluster.rows(), 1);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 1);
}

/// Go `sysvar.go:297`: `@@tidb_current_ts`'s GetSession hook returns
/// `s.TxnCtx.StartTS` -- the OPEN transaction's timestamp, and `0` when
/// none is open. The variable table's entry carries only the type and the
/// read-only flag, so answering the read from that table reported 0 for
/// every transaction, which is what this pins against.
///
/// It is the same value `TIDB_CURRENT_TSO()` reports, by construction:
/// both read the session's published timestamp.
#[test]
fn tidb_current_ts_reports_the_open_transactions_timestamp() {
    let (mut session, _cluster) = open_session();
    session
        .node_conn()
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    assert_eq!(
        rows(&mut session, "SELECT @@tidb_current_ts"),
        vec![vec![Datum::new_string("0")]],
        "no transaction is open outside one"
    );

    session
        .node_conn()
        .control_transaction("BEGIN")
        .expect("begin");
    let read = rows(&mut session, "SELECT count(*) FROM t");
    assert_eq!(read, vec![vec![Datum::Int(1)]]);
    let inside = rows(&mut session, "SELECT @@tidb_current_ts");
    // Go's `GetSession` hook returns `FormatUint`: a string.
    let Datum::String(start_ts) = &inside[0][0] else {
        panic!("@@tidb_current_ts must return a string")
    };
    let start_ts: u64 = start_ts.as_utf8().unwrap().parse().unwrap();
    assert!(start_ts > 0, "the open transaction's start ts is reported");
    assert_eq!(
        rows(&mut session, "SELECT TIDB_CURRENT_TSO()"),
        vec![vec![Datum::Int(start_ts as i64)]],
        "the variable and the builtin read the same published timestamp"
    );

    session
        .node_conn()
        .control_transaction("COMMIT")
        .expect("commit");
    assert_eq!(
        rows(&mut session, "SELECT @@tidb_current_ts"),
        vec![vec![Datum::new_string("0")]],
        "a committed transaction is no longer open"
    );
}

#[test]
fn transaction_observation_batch_text_prepared_and_failed_commit_share_history() {
    if crate::domain::isolate_process_globals() {
        return;
    }
    use tidb_txninfo::RECORDER;
    RECORDER.set_min_duration(std::time::Duration::ZERO);
    for binary in [false, true] {
        for fail_commit in [false, true] {
            let (mut session, cluster) = open_session();
            session
                .node_conn()
                .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
                .unwrap();
            RECORDER.resize(0);
            RECORDER.resize(32);
            let sql = "UPDATE t SET v = v + ? WHERE id = ?";
            let prepared = binary.then(|| session.node_conn().prepare_general(sql).unwrap());
            session.node_conn().control_transaction("BEGIN").unwrap();
            let executed = if let Some(prepared) = &prepared {
                session
                    .node_conn().execute_general(
                        prepared,
                        &[
                            tidb_protocol::PreparedValue::SignedLongLong(1),
                            tidb_protocol::PreparedValue::SignedLongLong(1),
                        ],
                    )
                    .unwrap();
                sql
            } else {
                let text = "UPDATE t SET v = v + 1 WHERE id = 1";
                session.node_conn().execute_write(text).unwrap();
                text
            };
            cluster.fail_commit.store(fail_commit, Ordering::Release);
            assert_eq!(
                session.node_conn().control_transaction("COMMIT").is_err(),
                fail_commit
            );
            let expected = ["BEGIN", executed, "COMMIT"]
                .map(|sql| tidb_parser::normalize_digest(sql).1.to_string());
            let expected = serde_json::to_string(&expected).unwrap();
            let found = RECORDER.rows();
            assert!(
                found
                    .iter()
                    .any(|row| row[1].sql_string().unwrap() == expected),
                "binary={binary}, failed={fail_commit}: {found:?}, expected {expected}"
            );
            drop(session);
            assert_eq!(
                RECORDER.rows(),
                found,
                "drop must not record the ended transaction again"
            );
        }
    }
}

/// Go `recordLastQueryInfo` stores `err.Error()`: a lock failure keeps its Go
/// class, `ClassTiKV` for client-go's lock errors. Captured from the oracle
/// (two sessions, the second's locking read against the first's lock):
/// `[tikv:3572]Statement aborted because lock(s) could not be acquired
/// immediately and NOWAIT is set.` and `[tikv:1205]Lock wait timeout exceeded;
/// try restarting transaction`.
#[test]
fn a_failed_locking_read_records_its_go_class_in_last_query_info() {
    use tidb_txnkv::transaction::PessimisticLockFailure;
    for (failure, expected) in [
        (
            PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key: b"k".to_vec() },
            "[tikv:3572]Statement aborted because lock(s) could not be acquired immediately \
             and NOWAIT is set.",
        ),
        (
            PessimisticLockFailure::LockWaitTimeout { key: b"k".to_vec() },
            "[tikv:1205]Lock wait timeout exceeded; try restarting transaction",
        ),
    ] {
        let (mut session, cluster) = open_session();
        session
            .node_conn()
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("seed");
        session
            .node_conn()
            .control_transaction("BEGIN PESSIMISTIC")
            .unwrap();
        *cluster.fail_next_lock.lock().unwrap() = Some(failure);
        assert!(session
            .node_conn()
            .execute("SELECT v FROM t WHERE id = 1 FOR UPDATE")
            .is_err());
        let info = match &rows(&mut session, "SELECT @@tidb_last_query_info")[0][0] {
            Datum::String(text) => String::from_utf8_lossy(text.bytes()).into_owned(),
            other => panic!("a system variable reads as text, got {other:?}"),
        };
        let parsed: serde_json::Value = serde_json::from_str(&info).unwrap();
        assert_eq!(parsed["error"], expected, "{info}");
    }
}
