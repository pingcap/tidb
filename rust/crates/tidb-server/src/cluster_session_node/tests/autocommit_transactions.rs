//! The transaction `SET autocommit = 0` opens, and the one `SET autocommit = 1`
//! closes.
//!
//! `autocommit = 0` is the third door onto this connection's transaction state,
//! beside a text `BEGIN` and a prepared one, and `SAVEPOINT` -- which under
//! `autocommit = 0` is itself a transaction opening -- is the fourth. Neither
//! carries a `BEGIN` keyword: the driver session turns a variable OFF, and
//! every later statement joins a transaction the session opens lazily for it.
//! A node that hears only the keyword therefore leaves `explicit` unopened,
//! every statement reads at its own fresh timestamp, and a racing writer is
//! never detected -- the same two-halves-disagreeing shape a prepared `BEGIN`
//! had.
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
//! == H: SAVEPOINT is what opens the transaction under autocommit = 0 ==
//! s1 SET autocommit=0 -> OK
//! s1 SAVEPOINT sp -> OK
//! s1 insert 1 -> OK
//! s1 sees 1 -> 10
//! s1 ROLLBACK TO sp -> OK
//! s1 sees 1 after -> (none)
//! s1 COMMIT -> OK
//! s2 sees 1 -> (none)
//! == I: control, SAVEPOINT under autocommit = 1 records nothing ==
//! s1 SET autocommit=1 -> OK
//! s1 SAVEPOINT sp2 -> OK
//! s1 ROLLBACK TO sp2 -> ERR: [executor:1305]SAVEPOINT sp2 does not exist
//! ```
//!
//! So `SET autocommit = 0` is a `BEGIN` in every observable respect except the
//! moment it takes its timestamp -- Go starts the transaction lazily, at the
//! first statement that touches data, not at the `SET` itself. `E` is the one
//! that is not guessable from MySQL alone and had to be measured: turning
//! autocommit back ON **commits** the open transaction, so the `ROLLBACK` after
//! it finds nothing to discard and the row stays. `H` is the other: the
//! SAVEPOINT itself starts the transaction, so its image is a real one and the
//! `ROLLBACK TO` really discards.
//!
//! `B` and `I` are the controls that keep the rest honest. Without `B`, putting
//! every statement on one snapshot would look like success; without `I`,
//! opening a transaction for every SAVEPOINT would.
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
        .expect_err("a prewrite at the transaction's timestamp must lose to a newer commit");
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
    assert_eq!(
        cluster.rows(),
        0,
        "the BEGIN's transaction is still staging"
    );

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

/// Go capture `== H ==`: under `autocommit = 0` the SAVEPOINT is the statement
/// that opens the transaction, so the image it takes is a real one and the
/// `ROLLBACK TO` really discards. A node that opened its transaction only at
/// the first DATA statement would take no image here, the `ROLLBACK TO` would
/// find none, and the COMMIT would publish the very rows it was asked to drop.
#[test]
fn a_savepoint_under_autocommit_off_opens_the_transaction_it_marks() {
    let (mut session, cluster) = open_session();
    set(&mut session, "SET autocommit = 0");
    session
        .control_transaction("SAVEPOINT sp")
        .expect("savepoint");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("insert");
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );

    session
        .control_transaction("ROLLBACK TO sp")
        .expect("rollback to savepoint");
    assert!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1").is_empty(),
        "the row staged after the savepoint is gone"
    );
    session.control_transaction("COMMIT").expect("commit");
    assert_eq!(
        cluster.rows(),
        0,
        "and the COMMIT publishes nothing, because nothing survived"
    );
}

/// Go capture `== I ==`, the control: in autocommit a `SAVEPOINT` records
/// nothing and opens nothing, so a later `ROLLBACK TO` reports 1305.
#[test]
fn a_savepoint_under_autocommit_on_records_nothing() {
    let (mut session, cluster) = open_session();
    session
        .control_transaction("SAVEPOINT sp2")
        .expect("a savepoint in autocommit succeeds");
    assert_eq!(
        cluster.begun.load(Ordering::Acquire),
        0,
        "and opens no transaction"
    );
    let error = session
        .control_transaction("ROLLBACK TO sp2")
        .expect_err("the name was never recorded");
    assert_eq!(error.code, 1305, "{}", error.message);
}

/// The lost-update regression, and the reason this seam exists at all.
///
/// An autocommit `UPDATE` reads at `T`, computes a value from what it saw, and
/// publishes. If it publishes at a FRESH timestamp instead of `T`, then a
/// commit that landed in between is outside TiKV's conflict check — the check
/// compares a key's latest `commit_ts` against the *prewriting* transaction's
/// `start_ts` — and the stale value overwrites it with no error and no
/// warning. That was measured live: two sessions each running 300 blind
/// `v = v + N` statements against one row ended hundreds of increments short
/// while both reported success.
///
/// Go cannot lose it because it spends one timestamp for the whole statement:
/// `pkg/sessiontxn/isolation/optimistic.go:45-46` points `getStmtReadTSFunc`
/// and `getStmtForUpdateTSFunc` at `getTxnStartTS` (`base.go:268`), and
/// client-go's `2pc.go:474` carries that same `txn.StartTS()` into
/// `prewrite.go:174`'s `StartVersion`.
///
/// So the assertion is not "the update fails". It is that the update **cannot
/// silently succeed**: publishing at the read's timestamp turns the race into
/// the 9007 the client is told about.
#[test]
fn an_autocommit_update_cannot_overwrite_a_commit_made_after_its_read() {
    let (mut session, cluster) = open_session();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    cluster.race_next_read.store(true, Ordering::Release);
    let error = session
        .execute_write("UPDATE t SET v = v + 5 WHERE id = 1")
        .expect_err(
            "a statement that read before another session's commit must not publish over it \
             in silence",
        );
    assert_eq!(
        error.code, ERR_WRITE_CONFLICT,
        "the race must reach the client as a write conflict: {}",
        error.message
    );
}

/// The control that keeps the test above honest: with no racing commit, the
/// same statement publishes at its read's timestamp and succeeds.
///
/// Without this, refusing every autocommit write would look like a fix.
#[test]
fn an_autocommit_update_with_no_race_still_publishes() {
    let (mut session, cluster) = open_session();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    session
        .execute_write("UPDATE t SET v = v + 5 WHERE id = 1")
        .expect("an uncontended update publishes");
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(15)]]
    );
    assert_eq!(
        cluster.publications.load(Ordering::Acquire),
        2,
        "the seed and the update, each published once"
    );
}

/// The fix is not scoped to the point-get shape, and it is not scoped to
/// `UPDATE`.
///
/// Live, the loss was measured at the same size with a non-point predicate
/// (2498 of 3700) as with `WHERE id = 1` (2538), so anything that only covered
/// point gets would not have been a fix. A ranged predicate reads through
/// `scan` instead of `get`, and reaches the same publication.
#[test]
fn a_non_point_predicate_is_covered_by_the_same_timestamp() {
    let (mut session, cluster) = open_session();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    cluster.race_next_read.store(true, Ordering::Release);
    let error = session
        .execute_write("UPDATE t SET v = v + 5 WHERE v > 0")
        .expect_err("the ranged shape must not publish over the race either");
    assert_eq!(
        error.code, ERR_WRITE_CONFLICT,
        "{}",
        error.message
    );
}

/// An `INSERT ... VALUES` DOES read: the duplicate-key check is a read, so the
/// statement has a timestamp and publishes at it.
///
/// Worth pinning because the three cases this seam distinguishes are read/write
/// (publish at the read), never-read (a fresh timestamp is correct), and the
/// max-ts shortcut (nothing may publish) -- and it would be easy to assume an
/// INSERT is the middle one. It is not: two sessions inserting the same key
/// race exactly as two updates do, and the loser must hear about it.
#[test]
fn an_insert_reads_for_its_uniqueness_check_and_publishes_at_that_read() {
    let (mut session, cluster) = open_session();
    let opened_before = cluster.opened.load(Ordering::Acquire);
    session
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("an uncontended insert publishes");
    assert_eq!(
        cluster.opened.load(Ordering::Acquire),
        opened_before + 1,
        "the duplicate-key check is a read, so the statement has a timestamp"
    );
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 2"),
        vec![vec![Datum::Int(20)]]
    );
}
