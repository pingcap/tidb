//! The statement that reads at `u64::MAX` and pays no timestamp, and every
//! statement that must not.
//!
//! Go serves an autocommit point get on the primary key at `math.MaxUint64` --
//! the latest committed version -- instead of spending a PD timestamp
//! (`OptimisticTxnContextProvider.AdviseOptimizeWithPlan`, guarded by
//! `IsPointGetWithPKOrUniqueKeyByAutoCommit`). This node now does the same, by
//! DECLARING the statement's shape to the bound snapshot before the statement
//! runs.
//!
//! # Why almost every pin here is a refusal
//!
//! `MaxUint64` ignores snapshot isolation: each read at it sees whatever is
//! committed at the moment of that read. That is harmless for a statement that
//! reads one row once and has nothing to stay consistent with, and it is a
//! silent wrong answer for anything else -- no error, wrong rows.
//!
//! The dangerous part is that at the storage seam the safe case and the unsafe
//! ones are INDISTINGUISHABLE. An autocommit `SELECT ... WHERE id = 1`, an
//! `UPDATE ... WHERE id = 1` reading the row it is about to write, and one row
//! lookup of an index double read all arrive as the same `ClusterSnapshot::get`
//! on the same key. So the refusals are not defensive padding; they are the
//! only thing standing between the shortcut and the wrong rows, and each one is
//! pinned in the direction that would fail if the decision ever moved from the
//! statement to the read.
//!
//! Two counters carry the evidence, and they say different things:
//! `opened` counts read transactions that spent a timestamp, `opened_at_max_ts`
//! counts the ones that spent none. A pin that watched only "did it read"
//! would pass for every wrong answer here.

use super::super::*;
use super::node_fixture::*;
use crate::resultset_source::ResultSetSource;
use std::sync::atomic::Ordering;
use tidb_datatype::Datum;

/// What one statement cost: how many timestamped read transactions it opened,
/// and how many `MaxUint64` ones.
#[derive(Debug, PartialEq, Eq)]
struct Opens {
    timestamped: usize,
    max_ts: usize,
}

fn opens_of(node: &MockNode, run: impl FnOnce()) -> Opens {
    let timestamped = node.opened.load(Ordering::Acquire);
    let max_ts = node.opened_at_max_ts.load(Ordering::Acquire);
    run();
    Opens {
        timestamped: node.opened.load(Ordering::Acquire) - timestamped,
        max_ts: node.opened_at_max_ts.load(Ordering::Acquire) - max_ts,
    }
}

const FREE: Opens = Opens {
    timestamped: 0,
    max_ts: 1,
};
const PAID: Opens = Opens {
    timestamped: 1,
    max_ts: 0,
};

fn seed(session: &mut ClusterServerSession) {
    session
        .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20)")
        .expect("seed");
}

// -- #140's pins, re-run against this path ---------------------------------

/// The shortcut itself: an autocommit point get on the primary key opens a
/// `MaxUint64` read transaction and spends no timestamp -- and still answers
/// the row.
#[test]
fn autocommit_point_get_on_the_primary_key_takes_no_timestamp() {
    let (mut session, node) = open_session();
    seed(&mut session);

    let mut answer = Vec::new();
    let opens = opens_of(&node, || {
        answer = rows(&mut session, "SELECT v FROM t WHERE id = 1");
    });
    assert_eq!(answer, vec![vec![Datum::Int(10)]]);
    assert_eq!(opens, FREE);
    assert_eq!(node.live.load(Ordering::Acquire), 0);
}

/// The same statement through PREPARE + EXECUTE with a `?` for the handle,
/// which is the shape a benchmark driver actually sends.
///
/// A `?` is not a value a point get can be planned from, so the shape is
/// decided from the text the execute-time values were bound INTO -- the same
/// text the statement then runs. Without this the whole saving would apply
/// only to the text protocol, which is the protocol nothing measures.
#[test]
fn a_prepared_point_get_takes_no_timestamp_either() {
    use tidb_protocol::PreparedValue;

    let (mut session, node) = open_session();
    seed(&mut session);
    let statement = session
        .prepare_general("SELECT v FROM t WHERE id = ?")
        .expect("prepare");
    let prepared = node.prepared.load(Ordering::Acquire);

    let mut answer = Vec::new();
    let opens = opens_of(&node, || {
        let outcome = session
            .execute_general(&statement, &[PreparedValue::SignedLongLong(2)])
            .expect("execute");
        let GeneralExecuteOutcome::Rows(mut result) = outcome else {
            panic!("a query must answer with rows");
        };
        let source = result.source();
        while let Ok(batch) = source.next_batch(8) {
            if batch.is_empty() {
                break;
            }
            answer.extend(batch);
        }
    });
    assert_eq!(answer, vec![vec![Datum::Int(20)]]);
    assert_eq!(opens, FREE);
    assert_eq!(node.prepared.load(Ordering::Acquire), prepared);
    assert_eq!(node.live.load(Ordering::Acquire), 0);
}

/// Go builds a prepared point plan on the first EXECUTE and reuses its
/// `PointGetExecutor` on later MaxTS executions. `Recreated` installs the new
/// parameter-derived handle before every reuse, so the cache hit must return
/// row 2 here rather than the row 1 selected while the entry was populated.
#[test]
fn a_prepared_point_get_reuses_the_plan_with_each_executions_handle() {
    use tidb_protocol::PreparedValue;

    let (mut session, _) = open_session();
    seed(&mut session);
    let statement = session
        .prepare_general("SELECT id, v, v FROM t WHERE id = ?")
        .expect("prepare");

    let execute = |session: &mut ClusterServerSession, id| {
        let outcome = session
            .execute_general(&statement, &[PreparedValue::SignedLongLong(id)])
            .expect("execute");
        let GeneralExecuteOutcome::Rows(mut result) = outcome else {
            panic!("a query must answer with rows");
        };
        let source = result.source();
        let mut answer = Vec::new();
        loop {
            let batch = source.next_batch(8).expect("batch");
            if batch.is_empty() {
                break;
            }
            answer.extend(batch);
        }
        source.finish().expect("finish");
        source.close().expect("close");
        answer
    };

    assert_eq!(
        execute(&mut session, 1),
        vec![vec![Datum::Int(1), Datum::Int(10), Datum::Int(10)]]
    );
    assert_eq!(
        rows(&mut session, "SELECT @@last_plan_from_cache"),
        vec![vec![Datum::Int(0)]]
    );

    assert_eq!(
        execute(&mut session, 2),
        vec![vec![Datum::Int(2), Datum::Int(20), Datum::Int(20)]]
    );
    assert_eq!(
        rows(&mut session, "SELECT @@last_plan_from_cache"),
        vec![vec![Datum::Int(1)]]
    );
}

/// The benchmark's aggregate root is not eligible for MaxTS. Go nevertheless
/// starts its ordinary TSO future after planning and waits for that same future
/// at the PointGet below StreamAgg.
#[test]
fn an_aggregate_point_get_prepares_and_waits_for_one_ordinary_snapshot() {
    let (mut session, node) = open_session();
    seed(&mut session);
    let prepared = node.prepared.load(Ordering::Acquire);

    let opens = opens_of(&node, || {
        assert_eq!(
            rows(&mut session, "SELECT COUNT(*) FROM t WHERE id = 1"),
            vec![vec![Datum::Int(1)]]
        );
    });

    assert_eq!(node.prepared.load(Ordering::Acquire), prepared + 1);
    assert_eq!(opens, PAID);
    assert_eq!(node.live.load(Ordering::Acquire), 0);
}

/// Fix 52592 deliberately turns point/batch fast plans back into ordinary
/// ranges. Snapshot declaration runs before the statement overlay is applied,
/// so it must derive the SAME effective direct-AST `SET_VAR` value up front;
/// otherwise the range read is incorrectly opened at `MaxUint64`.
#[test]
fn fix_52592_and_its_statement_overlay_gate_max_ts_before_execution() {
    use tidb_protocol::PreparedValue;

    let (mut session, node) = open_session();
    seed(&mut session);

    session
        .execute_write("SET tidb_opt_fix_control = '52592:OFF'")
        .expect("fix off");
    let opens = opens_of(&node, || {
        assert_eq!(
            rows(
                &mut session,
                "SELECT /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ v \
                 FROM t WHERE id=1",
            ),
            vec![vec![Datum::Int(10)]]
        );
    });
    assert_eq!(
        opens, PAID,
        "a direct ON overlay declared a point MaxTS read"
    );

    session
        .execute_write("SET tidb_opt_fix_control = '52592:ON'")
        .expect("persistent fix on");
    let opens = opens_of(&node, || {
        assert_eq!(
            rows(&mut session, "SELECT v FROM t WHERE id=1"),
            vec![vec![Datum::Int(10)]]
        );
    });
    assert_eq!(opens, PAID, "persistent ON declared a point MaxTS read");

    let opens = opens_of(&node, || {
        assert_eq!(
            rows(
                &mut session,
                "SELECT /*+ SET_VAR(tidb_opt_fix_control='52592:OFF') */ v \
                 FROM t WHERE id=1",
            ),
            vec![vec![Datum::Int(10)]]
        );
    });
    assert_eq!(
        opens, FREE,
        "a direct OFF overlay did not restore point MaxTS"
    );

    session
        .execute_write("SET tidb_opt_fix_control = '52592:OFF'")
        .expect("persistent fix off");
    let opens = opens_of(&node, || {
        assert_eq!(
            rows(
                &mut session,
                "SELECT /*+ SET_VAR(tidb_opt_fix_control='invalid') \
                 SET_VAR(tidb_opt_fix_control='52592:ON') */ v FROM t WHERE id=1",
            ),
            vec![vec![Datum::Int(10)]]
        );
    });
    assert_eq!(
        opens, FREE,
        "an invalid first value did not occupy the classifier's first-wins slot"
    );

    let statement = session
        .prepare_general(
            "SELECT /*+ SET_VAR(tidb_opt_fix_control='52592:ON') */ v FROM t WHERE id=?",
        )
        .expect("prepare hinted point");
    let opens = opens_of(&node, || {
        let outcome = session
            .execute_general(&statement, &[PreparedValue::SignedLongLong(2)])
            .expect("execute hinted point");
        let GeneralExecuteOutcome::Rows(mut result) = outcome else {
            panic!("a query must answer with rows");
        };
        let source = result.source();
        let mut answer = Vec::new();
        while let Ok(batch) = source.next_batch(8) {
            if batch.is_empty() {
                break;
            }
            answer.extend(batch);
        }
        assert_eq!(answer, vec![vec![Datum::Int(20)]]);
    });
    assert_eq!(opens, PAID, "prepared direct ON overlay used MaxTS");
}

/// A second equality beside the handle is NOT this tier's point get: the
/// handle arm requires exactly one name/value pair (Go's `len(pairs) == 1`),
/// so the statement plans as a scan with a filter and must pay.
///
/// Go's `PhysicalTableReader` arm admits a residual `Selection` because it
/// rides inside the reader, leaving one point range at the root. This tier has
/// no such reader -- the residual condition is a real operator above a real
/// scan, which is several reads -- so the same SQL is a refusal here. Refusing
/// costs one timestamp; admitting it would cost correctness.
#[test]
fn a_second_equality_beside_the_handle_still_takes_a_timestamp() {
    let (mut session, node) = open_session();
    seed(&mut session);

    let mut answer = Vec::new();
    let opens = opens_of(&node, || {
        answer = rows(&mut session, "SELECT v FROM t WHERE id = 1 AND v = 10");
    });
    assert_eq!(answer, vec![vec![Datum::Int(10)]]);
    assert_eq!(opens, PAID);
}

/// `IN (...)` is Go's `BatchPointGetPlan`, which its `switch` does not list
/// and so refuses through `default`. Several rows is several reads.
#[test]
fn a_batch_point_get_still_takes_a_timestamp() {
    let (mut session, node) = open_session();
    seed(&mut session);

    let mut answer = Vec::new();
    let opens = opens_of(&node, || {
        answer = rows(
            &mut session,
            "SELECT v FROM t WHERE id IN (1, 2) ORDER BY v",
        );
    });
    assert_eq!(answer, vec![vec![Datum::Int(10)], vec![Datum::Int(20)]]);
    assert_eq!(opens, PAID);
}

/// A full scan reads the whole relation, so it has every consistency
/// obligation the shortcut discards.
#[test]
fn a_full_scan_still_takes_a_timestamp() {
    let (mut session, node) = open_session();
    seed(&mut session);

    let opens = opens_of(&node, || {
        assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 2);
    });
    assert_eq!(opens, PAID);
}

/// A point predicate on a non-handle column pins no handle: Go's handle arm
/// requires the pair to be the primary key.
#[test]
fn a_point_predicate_on_a_non_handle_column_still_takes_a_timestamp() {
    let (mut session, node) = open_session();
    seed(&mut session);

    let mut answer = Vec::new();
    let opens = opens_of(&node, || {
        answer = rows(&mut session, "SELECT id FROM t WHERE v = 10");
    });
    assert_eq!(answer, vec![vec![Datum::Int(1)]]);
    assert_eq!(opens, PAID);
}

/// `SELECT ... FOR UPDATE` locks the row, and a lock needs a real timestamp to
/// be taken at.
#[test]
fn a_locking_read_still_takes_a_timestamp() {
    let (mut session, node) = open_session();
    seed(&mut session);

    let opens = opens_of(&node, || {
        let _ = session.execute("SELECT v FROM t WHERE id = 1 FOR UPDATE");
    });
    assert_eq!(opens.max_ts, 0, "a locking read took the shortcut");
}

/// The same point get inside `BEGIN` reads at the transaction's snapshot.
///
/// This is `IsAutoCommitTxn`'s `!InTxn` half, and here it is structural rather
/// than re-asked: inside a transaction the bound snapshot is the transaction's
/// own read handle, which refuses the declaration by inheriting the trait's
/// fail-closed default. There is nothing to declare to.
#[test]
fn the_same_point_get_inside_a_transaction_reads_at_the_transaction_snapshot() {
    let (mut session, node) = open_session();
    let mut writer = open_session_on(&node);
    seed(&mut session);

    session.control_transaction("BEGIN").expect("begin");
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );
    writer
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("a racing commit lands after BEGIN");

    let mut answer = Vec::new();
    let opens = opens_of(&node, || {
        answer = rows(&mut session, "SELECT v FROM t WHERE id = 1");
    });
    assert_eq!(
        answer,
        vec![vec![Datum::Int(10)]],
        "a point get inside BEGIN saw a commit made after BEGIN"
    );
    assert_eq!(
        opens,
        Opens {
            timestamped: 0,
            max_ts: 0
        },
        "a point get inside BEGIN opened a snapshot of its own"
    );
    session.control_transaction("COMMIT").expect("commit");
}

/// `SET autocommit = 0` is the third door onto transaction state and carries
/// no keyword, so it is the one most likely to be missed: the statement that
/// follows it joins a transaction, and must not take the shortcut.
#[test]
fn a_point_get_under_autocommit_zero_takes_no_shortcut() {
    let (mut session, node) = open_session();
    seed(&mut session);
    session
        .execute_write("SET autocommit = 0")
        .expect("autocommit off");

    let opens = opens_of(&node, || {
        assert_eq!(
            rows(&mut session, "SELECT v FROM t WHERE id = 1"),
            vec![vec![Datum::Int(10)]]
        );
    });
    assert_eq!(
        opens.max_ts, 0,
        "a statement inside a transaction opened at MaxUint64"
    );
    session.control_transaction("COMMIT").expect("commit");
}

/// Both visibility directions, which is the pin that says `MaxUint64` means
/// what the name claims.
///
/// A row committed between two autocommit point gets IS visible to the second,
/// because each reads the latest committed version. The same pair inside
/// `BEGIN` is NOT, because the transaction holds one timestamp. Either half
/// alone would pass for a wrong implementation: without the first, never
/// taking the shortcut looks correct; without the second, always taking it
/// does.
#[test]
fn a_concurrent_commit_is_visible_between_autocommit_point_gets_but_not_inside_a_transaction() {
    let (mut session, node) = open_session();
    let mut writer = open_session_on(&node);
    seed(&mut session);

    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(10)]]
    );
    writer
        .execute_write("UPDATE t SET v = 99 WHERE id = 1")
        .expect("racing commit");
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]],
        "an autocommit point get did not see a commit that preceded it"
    );

    session.control_transaction("BEGIN").expect("begin");
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]]
    );
    writer
        .execute_write("UPDATE t SET v = 42 WHERE id = 1")
        .expect("racing commit inside the transaction");
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(99)]],
        "a point get inside BEGIN saw a commit made after BEGIN"
    );
    session.control_transaction("COMMIT").expect("commit");
}

/// The predicate against the guard it ports, statement by statement, with no
/// storage in the picture at all.
#[test]
fn the_statement_shape_predicate_matches_the_guard_it_ports() {
    use tidb_executor::access_path::{statement_read_shape, StatementReadShape};

    let (session, _node) = open_session();
    let catalog = session.session.shared_catalog();
    let shape = |sql: &str| {
        let catalog = catalog.lock().expect("catalog");
        let stmt = tidb_parser::parse(sql).expect("parse");
        statement_read_shape(
            &stmt,
            &catalog,
            "app",
            &tidb_datatype::SessionTimeZone::utc(),
        )
    };
    let takes = |sql: &str| shape(sql) == StatementReadShape::AutocommitPointGet;

    assert!(takes("SELECT v FROM t WHERE id = 7"));
    assert!(takes("SELECT v FROM t WHERE 7 = id"));
    assert!(takes("SELECT * FROM t WHERE id = 7"));
    assert!(takes("SELECT v FROM app.t WHERE id = 7"));

    // Not a query at all: an UPDATE's read-before-write reads the very same
    // row by the very same key, and is refused on the statement, which is the
    // only place the two differ.
    assert!(!takes("UPDATE t SET v = 1 WHERE id = 7"));
    assert!(!takes("DELETE FROM t WHERE id = 7"));
    assert!(!takes("INSERT INTO t (id, v) VALUES (7, 1)"));
    // Not a point on the handle.
    assert!(!takes("SELECT v FROM t WHERE id != 7"));
    assert!(!takes("SELECT v FROM t WHERE id > 1 AND id < 9"));
    assert!(!takes("SELECT v FROM t"));
    assert!(!takes("SELECT v FROM t WHERE v = 7"));
    assert!(!takes("SELECT v FROM t WHERE id IN (7, 8)"));
    assert!(!takes("SELECT v FROM t WHERE id = 7 AND v = 1"));
    // A second read of any kind above or beside the point get.
    assert!(!takes(
        "SELECT v FROM t WHERE id = 7 UNION SELECT v FROM t WHERE id = 8"
    ));
    assert!(!takes("SELECT COUNT(*) FROM t WHERE id = 7"));
    assert!(!takes(
        "SELECT (SELECT v FROM t WHERE id = 8) FROM t WHERE id = 7"
    ));
    assert!(!takes(
        "SELECT a.v FROM t AS a JOIN t AS b ON a.id = b.id WHERE a.id = 7"
    ));
    assert!(!takes("WITH c AS (SELECT 1) SELECT v FROM t WHERE id = 7"));
    // Operators Go's switch would find above the reader.
    assert!(!takes("SELECT DISTINCT v FROM t WHERE id = 7"));
    assert!(!takes("SELECT v FROM t WHERE id = 7 ORDER BY v"));
    assert!(!takes("SELECT v FROM t WHERE id = 7 LIMIT 1"));
    assert!(!takes("SELECT v FROM t WHERE id = 7 FOR UPDATE"));
    assert!(!takes("SELECT v FROM t WHERE id = 7 GROUP BY v"));
    // A table with no clustered handle column has no handle to pin.
    assert!(!takes("SELECT v FROM hnd WHERE v = 7"));
    // No WHERE, no table, and a statement that is not a read at all.
    assert!(!takes("SELECT 1"));
    assert!(!takes("SET autocommit = 0"));
}

// -- The two new negative pins ---------------------------------------------

/// An `UPDATE` whose `WHERE` pins the handle reads that row before writing it,
/// and that read must NOT take the shortcut.
///
/// This is the trap in its exact form: the read arrives at the seam as the
/// same `ClusterSnapshot::get` on the same key that the safe `SELECT` above
/// issues. Only the statement tells them apart, which is why the declaration
/// is made from the statement. Reading the row at `MaxUint64` and writing at
/// the transaction's own timestamp would publish a row computed from a version
/// the write never conflicts with -- a lost update with no error anywhere.
#[test]
fn an_updates_read_before_write_does_not_take_the_shortcut() {
    let (mut session, node) = open_session();
    seed(&mut session);

    let opens = opens_of(&node, || {
        session
            .execute_write("UPDATE t SET v = v + 5 WHERE id = 1")
            .expect("update");
    });
    assert_eq!(
        opens.max_ts, 0,
        "an UPDATE's read-before-write opened at MaxUint64"
    );
    assert_eq!(
        rows(&mut session, "SELECT v FROM t WHERE id = 1"),
        vec![vec![Datum::Int(15)]]
    );

    // The control, in the other direction: the same `WHERE` in a `SELECT` is
    // the one shape that DOES take it, so the refusal above is about the
    // statement and not about the predicate.
    let opens = opens_of(&node, || {
        assert_eq!(
            rows(&mut session, "SELECT v FROM t WHERE id = 1"),
            vec![vec![Datum::Int(15)]]
        );
    });
    assert_eq!(opens, FREE);

    // And a DELETE, whose read-before-write is the same shape.
    let opens = opens_of(&node, || {
        session
            .execute_write("DELETE FROM t WHERE id = 1")
            .expect("delete");
    });
    assert_eq!(
        opens.max_ts, 0,
        "a DELETE's read-before-write opened at MaxUint64"
    );
}

/// A point get through a unique SECONDARY index is a double read -- one read
/// of the index entry, one of the row -- and must NOT take the shortcut.
///
/// Go refuses it by name: `noSecondRead` is `IndexInfo == nil ||
/// (Primary && IsCommonHandle)`, because at `MaxUint64` the index entry and
/// the row are read at two different moments and can come from two different
/// versions. `app.hnd(v BIGINT UNIQUE)` has no clustered handle column, so its
/// only point get is exactly that double read.
#[test]
fn a_double_reads_row_lookup_does_not_take_the_shortcut() {
    let (mut session, node) = open_session();
    session
        .execute_write("INSERT INTO hnd (v) VALUES (10), (20)")
        .expect("seed");

    let mut answer = Vec::new();
    let opens = opens_of(&node, || {
        answer = rows(&mut session, "SELECT v FROM hnd WHERE v = 10");
    });
    assert_eq!(answer, vec![vec![Datum::Int(10)]]);
    assert_eq!(
        opens, PAID,
        "a unique-index double read opened at MaxUint64"
    );
}

// -- #146's pins, re-run against this path ---------------------------------

/// Go starts the ordinary TSO future after planning, even when execution never
/// needs a cluster row. Such a statement never waits for or exposes a snapshot.
#[test]
fn a_statement_that_reads_no_cluster_row_prepares_but_never_opens_a_snapshot() {
    let (mut session, node) = open_session();
    let prepared = node.prepared.load(Ordering::Acquire);
    let timestamp = node.clock.load(Ordering::Acquire);

    let opens = opens_of(&node, || {
        assert_eq!(rows(&mut session, "SELECT 1").len(), 1);
    });
    assert_eq!(
        node.prepared.load(Ordering::Acquire),
        prepared + 1,
        "the statement did not start Go's post-plan TSO future"
    );
    assert_eq!(
        node.clock.load(Ordering::Acquire),
        timestamp + 1,
        "preparing the future must request exactly one timestamp"
    );
    assert_eq!(
        opens,
        Opens {
            timestamped: 0,
            max_ts: 0
        }
    );
    assert_eq!(node.live.load(Ordering::Acquire), 0);
}

/// Preparing a future is not activating a transaction. Go reports an oracle
/// future's error only when a read asks `Txn()` to wait for it.
#[test]
fn a_prepared_snapshot_error_is_reported_only_if_the_statement_reads() {
    let (mut session, node) = open_session();
    seed(&mut session);

    node.fail_next_prepared_snapshot
        .store(true, Ordering::Release);
    assert_eq!(rows(&mut session, "SELECT 1"), vec![vec![Datum::Int(1)]]);

    node.fail_next_prepared_snapshot
        .store(true, Ordering::Release);
    let error = session
        .execute("SELECT COUNT(*) FROM t WHERE id = 1")
        .err()
        .expect("a read must wait for and report the failed future");
    assert!(
        error.message.contains("table bytes failed to decode"),
        "{}",
        error.message
    );
    assert_eq!(node.live.load(Ordering::Acquire), 0);
}

/// Every read of one statement goes through the one transaction its FIRST read
/// opened -- the pin a shape declaration that fired per read rather than per
/// statement would break.
///
/// A per-read declaration would split one statement across two timestamps and
/// would still look like a saving in any counter that watches only single-read
/// statements, which is why this is asserted for both branches: the join opens
/// exactly one timestamped transaction and no `MaxUint64` one, and the point
/// get opens exactly one `MaxUint64` transaction and no timestamped one.
#[test]
fn every_read_of_one_statement_shares_the_transaction_the_first_read_opened() {
    let (mut session, node) = open_session();
    seed(&mut session);

    let mut joined = Vec::new();
    let opens = opens_of(&node, || {
        joined = rows(
            &mut session,
            "SELECT a.id FROM t AS a JOIN t AS b ON a.id = b.id ORDER BY a.id",
        );
    });
    assert_eq!(joined.len(), 2);
    assert_eq!(
        opens, PAID,
        "a statement reading two relations opened more than one snapshot"
    );
    assert_eq!(node.live.load(Ordering::Acquire), 0);

    let opens = opens_of(&node, || {
        assert_eq!(rows(&mut session, "SELECT v FROM t WHERE id = 1").len(), 1);
    });
    assert_eq!(opens, FREE);
    assert_eq!(node.live.load(Ordering::Acquire), 0);
}

/// Inside `BEGIN` neither the deferral nor the declaration reaches the
/// statement: it reads through the transaction's handle at the timestamp
/// `BEGIN` took, and repeatable read survives both changes.
#[test]
fn neither_the_deferral_nor_the_declaration_reaches_a_statement_inside_a_transaction() {
    let (mut session, node) = open_session();
    let mut writer = open_session_on(&node);
    seed(&mut session);

    session.control_transaction("BEGIN").expect("begin");
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 2);
    writer
        .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
        .expect("a racing commit lands after BEGIN");

    let opens = opens_of(&node, || {
        // A point get and a scan, both inside the transaction.
        assert_eq!(rows(&mut session, "SELECT v FROM t WHERE id = 1").len(), 1);
        assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 2);
    });
    assert_eq!(
        opens,
        Opens {
            timestamped: 0,
            max_ts: 0
        }
    );
    session.control_transaction("COMMIT").expect("commit");
    assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 3);
}
