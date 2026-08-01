//! The transaction `SET autocommit = 0` opens, and the one `SET autocommit = 1`
//! closes.
//!
//! `autocommit = 0` is the third door onto this connection's transaction state,
//! beside a text `BEGIN` and a prepared one. It carries no `BEGIN` keyword at
//! all: the driver session turns a variable OFF, and every later statement
//! joins a transaction the session opens lazily for it. A node that hears only
//! the keyword therefore leaves `explicit` unopened, every statement reads at
//! its own fresh timestamp, and a racing writer is never detected -- the same
//! two-halves-disagreeing shape a prepared `BEGIN` had.
//!
//! The contract is a capture, taken 2026-08-01 from a real TiDB over
//! `mockstore` with two sessions on one store:
//!
//! ```text
//! == A: autocommit = 0 snapshot consistency ==
//! s1 SET autocommit=0 -> OK
//! s1 read #1 -> 10
//! s2 update -> OK
//! s1 read #2 -> 10
//! s1 COMMIT -> OK
//! s1 read after -> 99
//! == B: control, autocommit = 1 ==
//! s1 SET autocommit=1 -> OK
//! s1 read #1 -> 10
//! s2 update -> OK
//! s1 read #2 -> 99
//! == C: racing writer under autocommit = 0 ==
//! s1 read -> 10
//! s2 update -> OK
//! s1 update -> OK
//! s1 COMMIT -> ERR: previous statement: update t set v = 50 where id = 1:
//!   [kv:9007]Write conflict, txnStartTS=468069906347720708,
//!   conflictStartTS=468069906347720709, conflictCommitTS=468069906347720710,
//!   key={tableID=117, tableName=test.t, handle=1},
//!   originalKey=7480000000000000755f728000000000000001, primary=[]byte(nil),
//!   originalPrimaryKey=, reason=Optimistic [try again later]
//! s1 read after -> 99
//! == D: ROLLBACK under autocommit = 0 discards ==
//! s1 insert -> OK
//! s1 sees 9 -> 90
//! s1 ROLLBACK -> OK
//! s1 sees 9 after -> (none)
//! s2 sees 9 -> (none)
//! == E: SET autocommit = 1 mid-transaction ==
//! s1 insert 8 -> OK
//! s1 SET autocommit=1 -> OK
//! s2 sees 8 -> 80
//! s1 ROLLBACK -> OK
//! s2 sees 8 after rollback -> 80
//! == F: SET autocommit = 0 with a transaction already open ==
//! s1 BEGIN -> OK
//! s1 insert 7 -> OK
//! s1 SET autocommit=0 -> OK
//! s2 sees 7 -> (none)
//! s1 ROLLBACK -> OK
//! s2 sees 7 after rollback -> (none)
//! s1 @@autocommit -> 0
//! == G: SET autocommit = 0 twice, second is a no-op ==
//! s1 insert 6 -> OK
//! s1 SET autocommit=0 again -> OK
//! s2 sees 6 -> (none)
//! s1 ROLLBACK -> OK
//! s2 sees 6 after rollback -> (none)
//! ```
//!
//! So `SET autocommit = 0` is a `BEGIN` in every observable respect except the
//! moment it takes its timestamp -- Go starts the transaction lazily, at the
//! first statement that touches data, not at the `SET` itself. `E` is the one
//! that is not guessable from MySQL alone and had to be measured: turning
//! autocommit back ON **commits** the open transaction, so the `ROLLBACK` after
//! it finds nothing to discard and the row stays. `B` is the control that keeps
//! `A` honest: without it, putting every statement on one snapshot would look
//! like success.
//!
//! These assert the observable transaction behaviour, not the call graph: a
//! `start_ts` that is wrong reads the wrong snapshot, which is far worse than a
//! slow read.

use super::super::*;
use super::node_fixture::*;
use std::sync::atomic::Ordering;
use tidb_datatype::Datum;
use tidb_exec::pessimistic_lock_error::ERR_WRITE_CONFLICT;

fn set(session: &mut ClusterServerSession, sql: &str) {
    session.execute_write(sql).expect("SET");
}

/// Go capture `== A ==`: the value read before and after another session's
/// commit is the SAME under `autocommit = 0`, with no `BEGIN` anywhere.
#[test]
fn autocommit_off_holds_one_snapshot_for_every_statement() {
    let (mut reader, cluster) = open_session();
    reader
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    set(&mut reader, "SET autocommit = 0");
    assert_eq!(
        rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );
    assert_eq!(
        cluster.begun.load(Ordering::Acquire),
        1,
        "the first statement under autocommit = 0 must open the connection's \
         transaction, exactly as a BEGIN does"
    );

    let mut writer = open_session_on(&cluster);
    writer
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the outside writer commits");

    assert_eq!(
        rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]],
        "a statement under autocommit = 0 must not see a commit made after the \
         transaction started"
    );
    reader.control_transaction("COMMIT").expect("commit");
    assert_eq!(
        rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]],
        "and once the transaction is over the session is back at the newest state"
    );
}

/// Go capture `== B ==`, the control: under `autocommit = 1` the two reads
/// DIFFER. Without this a fix that put every statement on one snapshot would
/// look like success.
#[test]
fn autocommit_on_statements_do_not_share_a_snapshot() {
    let (mut reader, cluster) = open_session();
    reader
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    assert_eq!(
        rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );
    let mut writer = open_session_on(&cluster);
    writer
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the outside writer commits");
    assert_eq!(
        rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]],
        "an autocommit statement reads at a FRESH timestamp"
    );
    assert_eq!(
        cluster.begun.load(Ordering::Acquire),
        0,
        "no transaction was opened"
    );
}

/// Go capture `== C ==`: `[kv:9007]Write conflict`. The single `start_ts` the
/// transaction took is what detects the race; without it the loser's write
/// would silently overwrite the winner's.
#[test]
fn a_transaction_under_autocommit_off_that_lost_the_race_fails_at_commit() {
    let (mut loser, cluster) = open_session();
    loser
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    set(&mut loser, "SET autocommit = 0");
    assert_eq!(
        rows(&mut loser, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );

    let mut winner = open_session_on(&cluster);
    winner
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the racing writer commits first");

    loser
        .execute_write("UPDATE t SET v = 50 WHERE id = 1")
        .expect("the statement itself succeeds; nothing is published yet");
    let error = loser
        .control_transaction("COMMIT")
        .err()
        .expect("a prewrite at the transaction's timestamp must lose to a newer commit");
    assert_eq!(error.code, ERR_WRITE_CONFLICT, "{}", error.message);

    // The winner's row stands.
    set(&mut loser, "SET autocommit = 1");
    assert_eq!(
        rows(&mut loser, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]]
    );
}

/// Go capture `== D ==`: after a `ROLLBACK` under `autocommit = 0` the row is
/// gone, and nothing was ever published.
#[test]
fn a_rollback_under_autocommit_off_discards_the_transactions_writes() {
    let (mut session, cluster) = open_session();
    set(&mut session, "SET autocommit = 0");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (9, 90)")
        .expect("insert");
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 9"),
        vec![vec![Datum::Int(90)]],
        "the transaction reads its own write"
    );
    session.control_transaction("ROLLBACK").expect("rollback");
    assert_eq!(
        cluster.rows(),
        0,
        "a ROLLBACK under autocommit = 0 must publish nothing"
    );
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
    assert!(rows(&mut session, "SELECT id FROM t").is_empty());
}

/// Go capture `== E ==`: turning autocommit back ON COMMITS the open
/// transaction. The `ROLLBACK` that follows finds nothing left to discard, so
/// the row survives it -- which is why the node cannot leave the driver
/// session's implicit commit unheard.
#[test]
fn setting_autocommit_back_on_commits_the_open_transaction() {
    let (mut session, cluster) = open_session();
    set(&mut session, "SET autocommit = 0");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (8, 80)")
        .expect("insert");
    assert_eq!(cluster.rows(), 0, "staged, not published");

    set(&mut session, "SET autocommit = 1");
    assert_eq!(
        cluster.rows(),
        1,
        "SET autocommit = 1 performs Go's implicit commit"
    );

    session.control_transaction("ROLLBACK").expect("rollback");
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 8"),
        vec![vec![Datum::Int(80)]],
        "the ROLLBACK has nothing left to discard"
    );
}

/// Go capture `== F ==` and `== G ==`: `SET autocommit = 0` while a
/// transaction is already open changes nothing about that transaction -- it
/// keeps running and a `ROLLBACK` still discards it. Only the OFF->ON
/// transition ends a transaction, so the OFF assignment never does.
#[test]
fn setting_autocommit_off_inside_a_transaction_leaves_it_running() {
    let (mut session, cluster) = open_session();
    session.control_transaction("BEGIN").expect("begin");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (7, 70)")
        .expect("insert");
    set(&mut session, "SET autocommit = 0");
    assert_eq!(cluster.rows(), 0, "the BEGIN's transaction is still staging");

    session.control_transaction("ROLLBACK").expect("rollback");
    assert_eq!(cluster.rows(), 0, "and a ROLLBACK still discards it");

    // `== G ==`: the same again with the transaction opened by the SET itself,
    // so the redundant assignment is a no-op rather than a commit.
    session
        .execute_write("INSERT INTO t (id, v) VALUES (6, 60)")
        .expect("insert");
    set(&mut session, "SET autocommit = 0");
    assert_eq!(cluster.rows(), 0, "staged, not published");
    session.control_transaction("ROLLBACK").expect("rollback");
    assert_eq!(cluster.rows(), 0);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
}
