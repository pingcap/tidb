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
use tidb_exec::pessimistic_lock_error::ERR_WRITE_CONFLICT;

/// Autocommit: each statement publishes its own writes, and each statement
/// reads at its own snapshot. The snapshot count is the proof that the
/// per-statement lifecycle actually runs.
#[test]
fn autocommit_publishes_each_statement_and_takes_a_fresh_snapshot() {
    let (mut session, cluster) = open_session();
    let outcome = session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
        .expect("insert")
        .expect("a write answers with an OK packet");
    assert_eq!(outcome.affected_rows, 3);
    assert_eq!(cluster.rows(), 3);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 1);

    // The next statement reads the published rows through a NEW snapshot.
    let opened_before = cluster.opened.load(Ordering::Acquire);
    let selected = rows(&mut session, "SELECT id, v FROM t ORDER BY id DESC");
    assert_eq!(selected.len(), 3);
    assert_eq!(selected[0], vec![Datum::Int(3), Datum::Int(30)]);
    assert_eq!(cluster.opened.load(Ordering::Acquire), opened_before + 1);
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
    assert_eq!(session.control_transaction("BEGIN").unwrap(), Some(true));
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("first insert");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("second insert");
    // Staged, not published: the cluster still holds nothing.
    assert_eq!(cluster.rows(), 0);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
    // The transaction's own statements still see their staged rows,
    // through the buffer in front of the snapshot.
    assert_eq!(rows(&mut session, "SELECT id FROM t").len(), 2);

    assert_eq!(session.control_transaction("COMMIT").unwrap(), Some(false));
    assert_eq!(cluster.rows(), 2);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 1);
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
}

/// One `BEGIN` takes one timestamp, and every statement until `COMMIT`
/// reads through that same transaction rather than opening its own.
#[test]
fn an_explicit_transaction_holds_one_transaction_for_every_statement() {
    let (mut session, cluster) = open_session();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    let autocommit_snapshots = cluster.opened.load(Ordering::Acquire);

    session.control_transaction("BEGIN").expect("begin");
    assert_eq!(cluster.begun.load(Ordering::Acquire), 1);
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 1);
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 1);
    session
        .execute_write("UPDATE t SET v = 11 WHERE id = 1")
        .expect("update");
    // Not one of those statements opened a transaction of its own.
    assert_eq!(cluster.begun.load(Ordering::Acquire), 1);
    assert_eq!(
        cluster.opened.load(Ordering::Acquire),
        autocommit_snapshots,
        "a statement inside BEGIN must not take a timestamp of its own"
    );
    session.control_transaction("COMMIT").expect("commit");
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
}

/// Repeatable read, which holding one transaction is what buys: a statement
/// inside `BEGIN` cannot see a commit made after `BEGIN`, because there is
/// no newer timestamp for it to see it at. Go's default isolation level.
#[test]
fn a_statement_inside_begin_does_not_see_a_commit_made_after_it() {
    let (mut reader, cluster) = open_session();
    reader
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    reader.control_transaction("BEGIN").expect("begin");
    assert_eq!(
        rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );

    let mut writer = open_session_on(&cluster);
    writer
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
    reader.control_transaction("ROLLBACK").expect("rollback");
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
    loser
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    loser.control_transaction("BEGIN").expect("begin");
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
        .expect_err("a prewrite at the BEGIN timestamp must lose to a newer commit");
    // The code, not just the text: the client is told 9007, which is the
    // one thing a caller that only looked for `Err` from the coordinator
    // could never report.
    assert_eq!(error.code, ERR_WRITE_CONFLICT, "{}", error.message);
    assert!(
        error.message.contains("9007"),
        "a lost race is a write conflict, got: {}",
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

/// The same race under autocommit publishes normally: each statement is its
/// own transaction at its own timestamp, so there is no older `start_ts` to
/// conflict with. Nothing about autocommit changed.
#[test]
fn autocommit_takes_a_fresh_timestamp_and_does_not_conflict() {
    let (mut first, cluster) = open_session();
    first
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");
    let mut second = open_session_on(&cluster);
    second
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("second writer");
    first
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
    session.control_transaction("BEGIN").expect("begin");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("insert");
    assert_eq!(
        session.control_transaction("ROLLBACK").unwrap(),
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
    session.control_transaction("BEGIN").expect("begin");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("first insert");
    // A write naming a column the table does not have: the statement fails
    // after the session has already opened its snapshot.
    assert!(session
        .execute_write("INSERT INTO t (id, nosuch) VALUES (2, 20)")
        .is_err());
    session
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("third insert");
    session.control_transaction("COMMIT").expect("commit");

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
        .execute_write("INSERT INTO t (id, nosuch) VALUES (2, 20)")
        .is_err());
    assert_eq!(cluster.rows(), 0);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
    assert_eq!(cluster.live.load(Ordering::Acquire), 0);
    // The connection is still usable, with an empty buffer.
    session
        .execute_write("INSERT INTO t (id, v) VALUES (7, 70)")
        .expect("the next statement still runs");
    assert_eq!(cluster.rows(), 1);
}

/// The staged record handles of one table, in key order: the row handles
/// this session would publish if it committed right now.
fn staged_handles(session: &ClusterServerSession, table_id: i64) -> Vec<i64> {
    session
        .buffer
        .staged()
        .into_iter()
        .filter_map(|(key, _)| {
            match tidb_tablecodec::table_key::decode_record_key(key.as_bytes()) {
                Ok((id, tidb_tablecodec::table_key::RecordHandle::Int(handle)))
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
/// [`tidb_session::Session`] would assume: a cluster-backed
/// `TableStorage::clone_box` clones `Arc` HANDLES, so the catalog image
/// the session restores on a failed statement cannot take back a staged
/// row -- the image and the original write into the SAME buffer. What
/// takes the row back is [`ClusterServerSession::with_statement`]'s
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
    session.control_transaction("BEGIN").expect("begin");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("first insert");
    let staged_before = session.buffer.staged();
    assert_eq!(staged_handles(&session, 101), vec![3]);

    assert!(session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 99)")
        .is_err());
    // Not merely "no new rows are visible": the staged bytes ARE the ones
    // the failing statement started from.
    assert_eq!(session.buffer.staged(), staged_before);

    session.control_transaction("COMMIT").expect("commit");
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
    session.control_transaction("BEGIN").expect("begin");
    session
        .execute_write("INSERT INTO hnd (v) VALUES (10)")
        .expect("first insert");
    assert_eq!(staged_handles(&session, 105), vec![1]);

    // Row `20` stages at handle 2 and row `10` takes handle 3 before it
    // duplicates the unique index and ends the statement.
    assert!(session
        .execute_write("INSERT INTO hnd (v) VALUES (20), (10)")
        .is_err());
    assert_eq!(staged_handles(&session, 105), vec![1]);

    session
        .execute_write("INSERT INTO hnd (v) VALUES (30)")
        .expect("third insert");
    // Handle 4: the two the failed statement consumed are gone for good.
    assert_eq!(staged_handles(&session, 105), vec![1, 4]);

    session.control_transaction("COMMIT").expect("commit");
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
    session.control_transaction("BEGIN").expect("begin");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("first insert");
    assert_eq!(
        session.control_transaction("SAVEPOINT s1").unwrap(),
        Some(true)
    );
    let staged_at_savepoint = session.buffer.staged();
    session
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("second insert");

    // ROLLBACK TO reports the transaction still open, and the buffer holds
    // the savepoint's bytes -- not merely "row 2 is invisible".
    assert_eq!(
        session.control_transaction("ROLLBACK TO s1").unwrap(),
        Some(true)
    );
    assert_eq!(session.buffer.staged(), staged_at_savepoint);
    assert_eq!(staged_handles(&session, 101), vec![1]);
    assert_eq!(cluster.publications.load(Ordering::Acquire), 0);

    // The transaction is still running, so this statement joins it.
    session
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("third insert");
    session.control_transaction("COMMIT").expect("commit");
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
    session.control_transaction("BEGIN").expect("begin");
    session.control_transaction("SAVEPOINT s1").expect("s1");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("insert");
    session.control_transaction("SAVEPOINT s2").expect("s2");

    // Releasing s1 drops s2 with it and keeps row 1 staged.
    session
        .control_transaction("RELEASE SAVEPOINT s1")
        .expect("release");
    assert_eq!(staged_handles(&session, 101), vec![1]);
    for sql in ["ROLLBACK TO s1", "ROLLBACK TO s2"] {
        let reported = session.control_transaction(sql).unwrap_err();
        assert!(
            format!("{reported:?}").contains("1305"),
            "{sql} did not report 1305: {reported:?}"
        );
    }

    // A fresh savepoint, rolled back to twice: it survives its own
    // rollback, matched case-insensitively.
    session.control_transaction("SAVEPOINT S3").expect("s3");
    session
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("insert");
    session
        .control_transaction("ROLLBACK TO s3")
        .expect("first");
    assert_eq!(staged_handles(&session, 101), vec![1]);
    session
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("insert");
    session
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
        session.control_transaction("BEGIN").expect("begin");
        session.control_transaction("SAVEPOINT s1").expect("s1");
        session.control_transaction(ending).expect("ending");
        assert!(session.savepoints.is_empty(), "{ending} kept a savepoint");
        session.control_transaction("BEGIN").expect("begin");
        assert!(
            session.control_transaction("ROLLBACK TO s1").is_err(),
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
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .is_err());
    assert_eq!(cluster.rows(), 0);

    cluster.fail_commit.store(false, Ordering::Release);
    session
        .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
        .expect("the next statement publishes only its own row");
    assert_eq!(cluster.rows(), 1);
    assert_eq!(
        rows(&mut session, "SELECT id FROM t"),
        vec![vec![Datum::Int(2)]]
    );
}

/// The autocommit timestamp belongs to the statement's first READ, not to the
/// binding that precedes planning: a statement that reads no cluster row
/// spends nothing.
///
/// This is what deferring the bind buys. The eager open charged a timestamp
/// before the statement was planned, which is also why a plan-shape timestamp
/// policy could never be consulted here -- the timestamp was already spent by
/// the time a plan existed.
#[test]
fn a_statement_that_reads_no_cluster_row_spends_no_timestamp() {
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
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
        .expect("seed");

    session.control_transaction("BEGIN").expect("begin");
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 1);
    writer
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
    session.control_transaction("COMMIT").expect("commit");
    // Outside the transaction the same connection sees the racing row.
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 2);
}
