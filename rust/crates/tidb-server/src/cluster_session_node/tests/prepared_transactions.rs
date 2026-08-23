//! Transaction control issued through the PREPARED statement path.
//!
//! The contract is a capture, taken 2026-08-01 from a real TiDB over
//! `mockstore` driving `PrepareStmt`/`ExecutePreparedStmt` on one session while
//! a second session commits underneath it:
//!
//! ```text
//! == A: prepared BEGIN snapshot consistency ==
//! prepared BEGIN -> OK   s1 count #1 -> 1   s2 insert -> OK
//! s1 count #2 -> 1       prepared COMMIT -> OK   s1 count after -> 2
//! == B: control, autocommit (no BEGIN) ==
//! s1 count #1 -> 2       s2 insert -> OK    s1 count #2 -> 3
//! == C: racing writer through prepared BEGIN ==
//! prepared COMMIT -> ERR: [kv:9007]Write conflict, txnStartTS=..., key=...
//! == D: prepared ROLLBACK discards ==
//! s1 sees 9 -> 90        prepared ROLLBACK -> OK   s1 sees 9 after -> (none)
//! == E: mixed prepared/text control ==
//! prepared BEGIN + text COMMIT -> durable; text BEGIN + prepared COMMIT -> durable
//! ```
//!
//! So a prepared `BEGIN` is indistinguishable from a text one, and B is the
//! control that keeps A honest: without it, putting every statement on one
//! snapshot would look like success.
//!
//! These assert the observable transaction behaviour, not the call graph: a
//! `start_ts` that is wrong reads the wrong snapshot, which is far worse than
//! a slow read.

use super::super::*;
use super::node_fixture::*;
use crate::resultset_source::ResultSetSource;
use std::sync::atomic::Ordering;
use tidb_datatype::Datum;
use tidb_exec::pessimistic_lock_error::ERR_WRITE_CONFLICT;

/// Runs one statement through PREPARE + EXECUTE with no parameters, the way
/// `COM_STMT_PREPARE`/`COM_STMT_EXECUTE` do for a statement neither the point
/// read nor the write planner claims.
fn prepared(session: &mut ClusterServerSession, sql: &str) {
    let statement = session.prepare_general(sql).expect("prepare");
    session.execute_general(&statement, &[]).expect("execute");
}

/// Binary prepared DML must retain its parsed tree.  Dropping it at PREPARE
/// sends every YCSB UPDATE back through SQL-text parse/bind on EXECUTE.
#[test]
fn prepared_dml_retains_the_parse_tree_for_execute() {
    let (mut session, _) = open_session();
    let statement = session
        .prepare_general("UPDATE t SET v = ? WHERE id = ?")
        .expect("prepare");
    assert!(statement.template().is_some());
}

/// Reads through the prepared path, so the snapshot under test is the one an
/// EXECUTE takes.
fn prepared_rows(session: &mut ClusterServerSession, sql: &str) -> Vec<Vec<Datum>> {
    let statement = session.prepare_general(sql).expect("prepare");
    let outcome = session.execute_general(&statement, &[]).expect("execute");
    let GeneralExecuteOutcome::Rows(mut result) = outcome else {
        panic!("a query must answer with rows");
    };
    let source = result.source();
    let mut rows = Vec::new();
    loop {
        let batch = source.next_batch(8).expect("batch");
        if batch.is_empty() {
            break;
        }
        rows.extend(batch);
    }
    source.finish().expect("finish");
    source.close().expect("close");
    rows
}

/// Go capture `== A ==`: the count taken before and after another session's
/// commit is the SAME inside a prepared `BEGIN`.
#[test]
fn a_prepared_begin_holds_one_snapshot_for_every_statement() {
    let (mut reader, cluster) = open_session();
    reader
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    prepared(&mut reader, "BEGIN");
    assert_eq!(
        cluster.begun.load(Ordering::Acquire),
        1,
        "a prepared BEGIN must open the connection's transaction"
    );
    assert_eq!(
        prepared_rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );

    let mut writer = open_session_on(&cluster);
    writer
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the outside writer commits");

    assert_eq!(
        prepared_rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]],
        "a statement inside a prepared BEGIN must not see a commit made after it"
    );
    prepared(&mut reader, "COMMIT");
    assert_eq!(
        prepared_rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]],
        "and once the transaction is over the session is back at the newest state"
    );
}

/// Go capture `== B ==`, the control: without a `BEGIN` the two counts
/// DIFFER. Without this a fix that put every statement on one snapshot would
/// look like success.
#[test]
fn prepared_autocommit_statements_do_not_share_a_snapshot() {
    let (mut reader, cluster) = open_session();
    reader
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    assert_eq!(
        prepared_rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );
    let mut writer = open_session_on(&cluster);
    writer
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the outside writer commits");
    assert_eq!(
        prepared_rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]],
        "an autocommit statement reads at a FRESH timestamp"
    );
    assert_eq!(
        cluster.begun.load(Ordering::Acquire),
        0,
        "no explicit transaction was opened"
    );
}

/// Go capture `== C ==`: `[kv:9007]Write conflict` at the prepared COMMIT.
/// The single `start_ts` a prepared BEGIN takes is what detects the race.
#[test]
fn a_prepared_transaction_that_lost_the_race_fails_at_commit() {
    let (mut loser, cluster) = open_session();
    loser
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    prepared(&mut loser, "BEGIN");
    assert_eq!(
        prepared_rows(&mut loser, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );

    let mut winner = open_session_on(&cluster);
    winner
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the racing writer commits first");

    loser
        .execute_write("UPDATE t SET v = 50 WHERE id = 1")
        .expect("the statement itself succeeds; nothing is published yet");
    let statement = loser.prepare_general("COMMIT").expect("prepare commit");
    let error = loser
        .execute_general(&statement, &[])
        .err()
        .expect("a prewrite at the BEGIN timestamp must lose to a newer commit");
    assert_eq!(error.code, ERR_WRITE_CONFLICT, "{}", error.message);

    // The winner's row stands.
    assert_eq!(
        rows(&mut loser, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]]
    );
}

/// Go capture `== D ==`: after a prepared `ROLLBACK` the row is gone, and
/// nothing was published.
#[test]
fn a_prepared_rollback_discards_the_transactions_writes() {
    let (mut session, cluster) = open_session();
    prepared(&mut session, "BEGIN");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("insert");
    prepared(&mut session, "ROLLBACK");
    assert_eq!(
        cluster.rows(),
        0,
        "a prepared ROLLBACK must publish nothing"
    );
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
    assert!(rows(&mut session, "SELECT id FROM t").is_empty());
}

/// Go capture `== E ==`: prepared and text transaction control describe the
/// same transaction, so the two may be mixed inside one.
#[test]
fn prepared_and_text_transaction_control_describe_the_same_transaction() {
    let (mut session, cluster) = open_session();
    prepared(&mut session, "BEGIN");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (11, 110)")
        .expect("insert");
    session.control_transaction("COMMIT").expect("text commit");
    assert_eq!(cluster.rows(), 1);

    session.control_transaction("BEGIN").expect("text begin");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (12, 120)")
        .expect("insert");
    assert_eq!(cluster.rows(), 1, "staged, not published");
    prepared(&mut session, "COMMIT");
    assert_eq!(cluster.rows(), 2);
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
}

/// The explicit transaction is OPTIMISTIC, which `FOR UPDATE` and locking
/// DML both make visible. Go takes a pessimistic lock in both cases, so a
/// second connection WAITS and the first transaction wins; here the second
/// connection commits immediately and the first loses its own row at
/// `COMMIT`.
///
/// This pins the measured behaviour and names the Go behaviour it must
/// become, so wiring the explicit transaction onto the pessimistic
/// machinery turns this red at the right place -- see this module's parent
/// (`transactions.rs`) for why that wiring, not an executor-level lock
/// step alone, is the unit of work.
#[test]
fn an_explicit_transaction_does_not_lock_what_go_would_lock() {
    for (locking_statement, returns_rows) in [
        ("SELECT v FROM t WHERE id = 1 FOR UPDATE", true),
        ("UPDATE t SET v = 50 WHERE id = 1", false),
    ] {
        let (mut holder, cluster) = open_session();
        holder
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("seed");
        prepared(&mut holder, "BEGIN");
        // Go locks the row here; this tier reads it under the snapshot.
        if returns_rows {
            let _ = prepared_rows(&mut holder, locking_statement);
        } else {
            holder
                .execute_write(locking_statement)
                .expect("the locking DML itself succeeds");
        }

        // Go would BLOCK this contender until the holder commits.
        let mut contender = open_session_on(&cluster);
        contender
            .execute_write("UPDATE t SET v = 99 WHERE id = 1")
            .expect("the contender is not blocked, where Go blocks it");

        holder
            .execute_write("UPDATE t SET v = 51 WHERE id = 1")
            .expect("the statement itself succeeds; nothing is published yet");
        let statement = holder.prepare_general("COMMIT").expect("prepare commit");
        let error = holder
            .execute_general(&statement, &[])
            .err()
            .expect("the holder loses its own row, where Go's holder wins");
        assert_eq!(
            error.code, ERR_WRITE_CONFLICT,
            "`{locking_statement}`: {}",
            error.message
        );
    }
}

/// A prepared UPDATE whose table the fast write planner declines — here a
/// composite clustered primary key, which `run_fast_prepared_update` admits
/// only for a single-column handle — falls back to the ordinary bound
/// execution, and that fallback must apply its parameters.
///
/// The regression: the fallback reparsed the PREPARE-time SQL text on every
/// EXECUTE, resurrecting raw `?` markers into the expression rewriter (1105
/// "expression form is not yet supported by the rewriter"). Go binds the
/// execute values onto the retained `*ast.UpdateStmt` (`ExecStmt.exec` ->
/// `UpdateExec`) and never reparses, so TPC-C's payment-style
/// `SET d_ytd = d_ytd + ?` answers 1 affected row; here it answered an error
/// and every prepared payment failed.
#[test]
fn a_prepared_update_the_fast_planner_declines_binds_its_parameters() {
    use tidb_protocol::PreparedValue;

    let (mut session, _node) = open_session();
    session
        .execute_write(
            "CREATE TABLE dist (w INT NOT NULL, d INT NOT NULL, \
             ytd DECIMAL(10,2) NOT NULL DEFAULT 0, PRIMARY KEY (w, d))",
        )
        .expect("create");
    session
        .execute_write("INSERT INTO dist (w, d, ytd) VALUES (1, 1, 100.00)")
        .expect("seed");

    let statement = session
        .prepare_general("UPDATE dist SET ytd = ytd + ? WHERE w = ? AND d = ?")
        .expect("prepare");
    let affected = {
        let outcome = session
            .execute_general(
                &statement,
                &[
                    PreparedValue::Double(25.5),
                    PreparedValue::SignedLongLong(1),
                    PreparedValue::SignedLongLong(1),
                ],
            )
            .expect("execute");
        let GeneralExecuteOutcome::Write(outcome) = outcome else {
            panic!("a prepared UPDATE answers OK with an affected count");
        };
        outcome.affected_rows
    };
    assert_eq!(affected, 1, "the matched row is updated");

    let rows = prepared_rows(&mut session, "SELECT ytd FROM dist WHERE w = 1 AND d = 1");
    assert_eq!(rows.len(), 1);
    let applied = match &rows[0][0] {
        Datum::Decimal(value) => value.to_string(),
        other => panic!("a DECIMAL column answers a decimal, found {other:?}"),
    };
    assert!(
        applied.contains("125.5"),
        "ytd must hold 100.00 + 25.50, found {applied}"
    );

    // And inside an explicit transaction, where the statement runs through
    // the pessimistic loop rather than autocommit publication.
    prepared(&mut session, "BEGIN");
    let affected = {
        let outcome = session
            .execute_general(
                &statement,
                &[
                    PreparedValue::Double(0.5),
                    PreparedValue::SignedLongLong(1),
                    PreparedValue::SignedLongLong(1),
                ],
            )
            .expect("execute inside a transaction");
        let GeneralExecuteOutcome::Write(outcome) = outcome else {
            panic!("a prepared UPDATE answers OK with an affected count");
        };
        outcome.affected_rows
    };
    assert_eq!(affected, 1);
    prepared(&mut session, "COMMIT");

    let rows = prepared_rows(&mut session, "SELECT ytd FROM dist WHERE w = 1 AND d = 1");
    let applied = match &rows[0][0] {
        Datum::Decimal(value) => value.to_string(),
        other => panic!("a DECIMAL column answers a decimal, found {other:?}"),
    };
    assert!(
        applied.contains("126"),
        "the committed row must hold 126.00, found {applied}"
    );
}
