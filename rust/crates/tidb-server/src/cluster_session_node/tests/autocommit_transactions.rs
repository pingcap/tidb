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
use crate::resultset_source::ResultSetSource;
use std::sync::atomic::Ordering;
use tidb_datatype::Datum;
use tidb_exec::pessimistic_lock_error::{ERR_REGION_UNAVAILABLE, ERR_WRITE_CONFLICT};

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
/// something the loser cannot mistake for success.
///
/// What the loser then does is the retry, and this asserts BOTH halves at
/// once. The statement succeeds -- Go's client never sees a 9007 for a single
/// autocommit statement -- and it can only have succeeded by re-reading: the
/// mock refuses any publication whose `start_ts` predates the racing commit,
/// exactly as TiKV does, so a replay that resubmitted the staged buffer at the
/// original read's timestamp would be refused here rather than pass. The
/// second read transaction is counted to say the re-read happened rather than
/// inferring it.
#[test]
fn an_autocommit_update_that_loses_the_race_is_retried_at_a_new_read() {
    let (mut session, cluster) = open_session();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    let opened_before = cluster.opened.load(Ordering::Acquire);

    cluster.race_next_read.store(true, Ordering::Release);
    session
        .execute_write("UPDATE t SET v = v + 5 WHERE id = 1")
        .expect("a conflicted autocommit statement is retried, not refused");

    assert_eq!(
        cluster.opened.load(Ordering::Acquire),
        opened_before + 2,
        "the losing attempt and the replay each opened their own read"
    );
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(15)]],
        "the replay recomputed from what it re-read"
    );
}

#[test]
fn txn_retryable_error_matrix_matches_the_reachable_go_allowlist() {
    let (mut session, _) = open_session();
    let conflict = SqlQueryError::new(ERR_WRITE_CONFLICT, *b"HY000", "Write conflict");
    let unavailable =
        SqlQueryError::new(ERR_REGION_UNAVAILABLE, *b"HY000", "Region is unavailable");

    assert!(session.may_retry_autocommit_statement(&conflict, 0));
    assert!(!session.may_retry_autocommit_statement(&conflict, AUTOCOMMIT_RETRY_LIMIT));
    assert!(!session.may_retry_autocommit_statement(&unavailable, 0));

    session.control_transaction("BEGIN").expect("begin");
    assert!(!session.may_retry_autocommit_statement(&conflict, 0));
}

/// `pkg/kv.IsTxnRetryableError` deliberately excludes TiKV's explicit 9005.
/// A one-shot failure keeps the assertion honest: replaying it would make the
/// second attempt succeed, publish `v = 15`, and hide the error from the
/// client.
#[test]
fn a_region_unavailable_commit_is_reported_without_replaying_the_statement() {
    let (mut session, cluster) = open_session();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    let opened_before = cluster.opened.load(Ordering::Acquire);
    let publications_before = cluster.publications.load(Ordering::Acquire);

    cluster
        .fail_next_region_commit
        .store(true, Ordering::Release);
    let error = session
        .execute_write("UPDATE t SET v = v + 5 WHERE id = 1")
        .expect_err("9005 must reach the client instead of being replayed");

    assert_eq!(error.code, ERR_REGION_UNAVAILABLE, "{}", error.message);
    assert_eq!(
        cluster.opened.load(Ordering::Acquire),
        opened_before + 1,
        "the failed statement opened exactly one attempt"
    );
    assert_eq!(
        cluster.publications.load(Ordering::Acquire),
        publications_before,
        "the failed statement published nothing"
    );
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]],
        "the rejected statement left the row unchanged"
    );
}

/// The bound on that retry, which is the other half of the contract clients
/// depend on.
///
/// Go's replay budget is `@@tidb_retry_limit` and running out of it returns
/// the last commit error (`pkg/session/session.go:1272-1278`), so a conflict
/// that outlives the budget still reaches the client as 9007. A race that
/// never clears is the only way to reach that line: it pins that the node does
/// not spin forever, and pins the budget itself as behaviour rather than as a
/// constant nothing reads.
#[test]
fn a_race_that_never_clears_exhausts_the_budget_and_reports_the_conflict() {
    let (mut session, cluster) = open_session();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    let opened_before = cluster.opened.load(Ordering::Acquire);

    cluster.race_every_read.store(true, Ordering::Release);
    let error = session
        .execute_write("UPDATE t SET v = v + 5 WHERE id = 1")
        .expect_err("a conflict the budget cannot outlast must reach the client");
    cluster.race_every_read.store(false, Ordering::Release);

    assert_eq!(
        error.code, ERR_WRITE_CONFLICT,
        "the exhausted retry reports the conflict itself: {}",
        error.message
    );
    assert_eq!(
        cluster.opened.load(Ordering::Acquire),
        opened_before + 1 + AUTOCOMMIT_RETRY_LIMIT as usize,
        "one attempt plus the whole replay budget, and not one more"
    );
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]],
        "a refused statement wrote nothing"
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

    // The ranged shape reads through `scan`, and reaches the same publication
    // and therefore the same retry: it too must recompute from a re-read
    // rather than publish over the race.
    cluster.race_next_read.store(true, Ordering::Release);
    session
        .execute_write("UPDATE t SET v = v + 5 WHERE v > 0")
        .expect("the ranged shape is retried like the point one");
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(15)]]
    );

    cluster.race_every_read.store(true, Ordering::Release);
    let error = session
        .execute_write("UPDATE t SET v = v + 5 WHERE v > 0")
        .expect_err("the ranged shape must not publish over a race it cannot outlast either");
    cluster.race_every_read.store(false, Ordering::Release);
    assert_eq!(error.code, ERR_WRITE_CONFLICT, "{}", error.message);
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

/// The auto-increment half of the replay, and the one place where "run the
/// statement again" is not the same as "run the statement once".
///
/// Go REUSES the ids the losing attempt already allocated rather than drawing
/// fresh ones. The gap a fresh draw would leave is not the point -- TiDB
/// leaves auto-increment gaps of its own and they are legal.
/// `LAST_INSERT_ID()` is: a client inserts a row, reads `LAST_INSERT_ID()`,
/// and uses it as the foreign key of a child row; if a retry moved the id
/// underneath it, the value names a row that was never written. That is
/// silent wrong data, it appears only under contention, and a row count
/// cannot see it -- so this asserts BY VALUE, from both the stored rows and
/// the function the client would call.
///
/// The expected values are Go's, hand-derived from the source and then
/// MEASURED against a real TiDB over `mockstore`, because this batch's shape
/// makes them counter-intuitive enough that neither method alone was trusted.
/// `INSERT ... VALUES` always takes Go's LAZY arm (`insertRows` sets
/// `e.lazyFillAutoID = true` unconditionally, `insert_common.go:237`), and
/// that arm handles an explicitly-supplied id and a NULL one asymmetrically:
///
/// * attempt 1 records BOTH -- the explicit `1` at `insert_common.go:902` and
///   the allocated `2` at `:946` -- so `RetryInfo`'s list is `[1, 2]`;
/// * the replay `continue`s past the explicit row at `insert_common.go:894-903`
///   WITHOUT advancing the cursor, so the NULL row's consume loop
///   (`insert_common.go:909-921`) reads offset 0 and gets the EXPLICIT id back;
/// * `lastInsertID` is assigned only in the allocating arm
///   (`insert_common.go:936-938`), which the replay never reaches, so the
///   value attempt 1 published survives.
///
/// So the NULL row is handed `1`, collides with this same statement's explicit
/// row, and is redirected into the `ON DUPLICATE KEY UPDATE`: one row remains,
/// no row is ever given `2`, and `LAST_INSERT_ID()` still says `2`. Measured
/// on TiDB with `mockCommitRetryForAutoIncID` failing the first commit:
///
/// ```text
/// create table t (id int primary key auto_increment, v int)
/// insert into t (v) values (10)                  -> rows=[[1 10]] last_insert_id=1
/// insert into t (id, v) values (1,11),(NULL,20)
///   on duplicate key update v = 11               -> rows=[[1 11]] last_insert_id=2
/// ```
///
/// A port that consumed the cursor for the explicit row too answered `3` here
/// -- one id of drift per explicit id in the batch -- which is the exact
/// `LAST_INSERT_ID()` lie above.
#[test]
fn a_conflicted_insert_replays_with_the_ids_the_losing_attempt_allocated() {
    let (mut session, cluster) = open_session();
    session
        .execute_write("INSERT INTO ai (v) VALUES (10)")
        .expect("seed takes id 1");

    // The upsert is what makes the race reachable at all: an id nobody has
    // handed out yet cannot collide, so a statement that ONLY allocates never
    // conflicts on its own row. A statement that touches an existing row and
    // allocates in the same breath does, and that is the shape -- a mixed
    // batch of known and new keys -- a client actually writes.
    cluster.race_next_read.store(true, Ordering::Release);
    session
        .execute_write(
            "INSERT INTO ai (id, v) VALUES (1, 11), (NULL, 20) \
             ON DUPLICATE KEY UPDATE v = 11",
        )
        .expect("a conflicted autocommit insert is retried, not refused");

    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        vec![vec![Datum::Int(1), Datum::Int(11)]],
        "the replay must take the id back out of the losing attempt's list, \
         not allocate a fresh one -- and at offset 0 that list holds the \
         EXPLICIT id, so the NULL row lands on row 1 and is absorbed by the \
         ON DUPLICATE KEY UPDATE. A fresh allocation would leave a second row \
         here (measured on TiDB: rows=[[1 11]])"
    );
    assert_eq!(
        rows(&mut session, "SELECT LAST_INSERT_ID()"),
        vec![vec![Datum::UInt(2)]],
        "and LAST_INSERT_ID() keeps the LOSING attempt's allocation: Go's \
         replay reuses without ever reaching the assignment at \
         insert_common.go:936-938, so the value a client already read cannot \
         move under it (measured on TiDB: last_insert_id=2)"
    );
}

/// The no-race control for the case above.
///
/// Without it, an id that never advanced at all would look like the fix.
#[test]
fn an_uncontended_insert_still_advances_the_auto_increment() {
    let (mut session, _cluster) = open_session();
    session
        .execute_write("INSERT INTO ai (v) VALUES (10)")
        .expect("first");
    session
        .execute_write("INSERT INTO ai (v) VALUES (20)")
        .expect("second");
    assert_eq!(
        rows(&mut session, "SELECT id FROM ai ORDER BY id"),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]],
        "two uncontended inserts take two consecutive ids"
    );
    assert_eq!(
        rows(&mut session, "SELECT LAST_INSERT_ID()"),
        vec![vec![Datum::UInt(2)]]
    );
}

/// A PREPARE under `autocommit = 0` is NOT the statement that opens the
/// transaction -- the first statement that touches data still is.
///
/// This is the third door onto the same question, after a prepared `BEGIN` and
/// `SET autocommit = 0` itself, and it is the one that goes the OTHER way:
/// those two had to open a transaction and did not, this one must not and did.
/// Go's `PrepareStmt` does call `PrepareTxnCtx` (`pkg/session/session.go:3171`),
/// but with a nil statement and through `EnterNewTxnBeforeStmt`, which makes
/// the session's transaction *pending* -- no timestamp is spent and
/// `SessionVars.InTxn()` stays false -- and the timestamp is taken by the first
/// statement that really reads. Captured 2026-08-03 from a real TiDB over
/// `mockstore`, two sessions on one store:
///
/// ```text
/// == A: binary PrepareStmt under autocommit=0 ==
/// s1 @@autocommit -> 0
/// s1 PrepareStmt err -> <nil>
/// s1 @@tidb_current_ts after PREPARE -> 0
/// s2 update -> OK
/// s1 read after s2 commit -> 99
/// == B: control, a SELECT under autocommit=0 ==
/// s1 read #1 -> 10
/// s1 @@tidb_current_ts after read -> 468115494319161354
/// s1 read #2 -> 10
/// == C: text PREPARE under autocommit=0 ==
/// s1 PREPARE -> OK
/// s1 @@tidb_current_ts after PREPARE -> 0
/// s2 update -> OK
/// s1 EXECUTE p -> 99
/// == D: rollback after PREPARE + write ==
/// s2 sees id=7 after s1 rollback -> (none)
/// == E: in-transaction status flag ==
/// status before PREPARE -> false
/// status after PREPARE -> false
/// status after a real read -> true
/// ```
///
/// `B` is the control that says the capture can tell a started transaction
/// from an unstarted one at all: a real read pins the snapshot, so the second
/// read still says 10 while the PREPARE case moves to 99.
///
/// The cost of getting this wrong is a stale read with no error: this node's
/// PREPARE probes a query's result columns by RUNNING it, so opening the
/// connection's transaction there pins its `start_ts` before the client has
/// executed anything, and every later statement of that transaction -- and the
/// conflict check of its eventual commit -- lives at that earlier timestamp.
#[test]
fn a_prepare_under_autocommit_off_opens_no_transaction() {
    let (mut reader, cluster) = open_session();
    reader
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    set(&mut reader, "SET autocommit = 0");
    let statement = reader
        .prepare_general("SELECT v FROM t WHERE id = 1")
        .expect("prepare");
    assert_eq!(
        cluster.begun.load(Ordering::Acquire),
        0,
        "Go's PREPARE leaves the transaction pending: no timestamp is spent \
         (captured `@@tidb_current_ts after PREPARE -> 0`)"
    );

    let mut writer = open_session_on(&cluster);
    writer
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("the outside writer commits after the PREPARE");

    let outcome = reader.execute_general(&statement, &[]).expect("execute");
    let GeneralExecuteOutcome::Rows(mut result) = outcome else {
        panic!("a query must answer with rows");
    };
    let source = result.source();
    let batch = source.next_batch(8).expect("batch");
    source.finish().expect("finish");
    source.close().expect("close");
    assert_eq!(
        batch,
        vec![vec![Datum::Int(99)]],
        "the EXECUTE is the first statement that touches data, so the \
         transaction opens THERE and sees the commit the PREPARE predates \
         (captured `s1 EXECUTE p -> 99`)"
    );
    assert_eq!(
        cluster.begun.load(Ordering::Acquire),
        1,
        "and it is still one transaction, opened once"
    );
}

/// The retry's backoff schedule is Go's, and the cap is what keeps a budget
/// that never wins from turning into a long stall.
#[test]
fn the_backoff_schedule_doubles_and_then_caps() {
    assert_eq!(
        back_off_upper_ms(1),
        2,
        "the first sleep is drawn from [0,2)"
    );
    assert_eq!(back_off_upper_ms(2), 4);
    assert_eq!(back_off_upper_ms(6), 64);
    for attempts in 7..=AUTOCOMMIT_RETRY_LIMIT {
        assert_eq!(
            back_off_upper_ms(attempts),
            RETRY_BACK_OFF_CAP_MS,
            "attempt {attempts} must be capped, not shifted off the end"
        );
    }
}

/// The jitter must actually vary, and this test exists because a version that
/// did not shipped and was caught on a real cluster.
///
/// Reading `SystemTime`'s nanosecond field as the jitter source looks uniform
/// and is not: this machine's clock advances in 1000ns steps, so `nanos % 2`,
/// `% 4` and `% 8` are identically zero and the first three backoffs never
/// slept at all. Two sessions colliding on one key then spun in lockstep, and
/// two of 300 statements burned the whole budget rather than none. A degenerate
/// draw is invisible in every assertion except this one.
#[test]
fn the_backoff_jitter_is_not_degenerate_at_the_small_bounds() {
    for upper in [2_u32, 4, 8, 16] {
        let mut seen = [false; 16];
        for _ in 0..4096 {
            let draw = jitter_below(upper);
            assert!(draw < upper, "a draw must stay below its bound");
            seen[draw as usize] = true;
        }
        for (value, hit) in seen.iter().take(upper as usize).enumerate() {
            assert!(
                *hit,
                "jitter below {upper} never produced {value}, so the sleep is not uniform"
            );
        }
    }
}
