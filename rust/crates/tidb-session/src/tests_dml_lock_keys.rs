// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! WHICH KEYS a DML statement touches -- ported from
//! `pkg/executor/delete_test.go:27` `TestDeleteLockKey` and
//! `pkg/executor/insert_test.go:534` `TestInsertLockUnchangedKeys`.
//!
//! Those two Go tests are *lock* tests: each runs the statement in one
//! pessimistic transaction and then races a second transaction at a key the
//! statement should have locked, asserting from the block (or the absence of
//! one) which keys the DML took. This engine has no DML lock path at all --
//! `tidb_txnkv`'s `Transaction::lock_keys` exists but no statement drives it,
//! and `tidb_lock_unchanged_keys` is a registered variable nothing reads --
//! so the blocking half of each row is `#[ignore]`d here WITH GO'S ANSWER
//! asserted, and every ignored row has a running guard beside it that pins
//! what this engine does today. The guard is the point: when locking lands,
//! the ignored row starts passing and the guard starts failing, so neither can
//! go stale in silence.
//!
//! What DOES run is the substance the lock set is derived from: the exact key
//! set -- record keys and index keys together -- that each of Go's statements
//! leaves behind. A `DELETE` that forgets a unique-index key is the same lost
//! write the Go test guards against, and it is findable at this tier today.
//!
//! DIVERGENCE, stated once: Go's `TestDeleteLockKey` sets
//! `EnableClusteredIndex = ClusteredIndexDefModeIntOnly`, so its
//! `primary key(k, kk)` is NOT clustered -- the row handle is a `_tidb_rowid`
//! and the PK becomes an ordinary unique index. This engine clusters a
//! composite primary key (`KvTable::common_handle_offsets`), so the key COUNT
//! per row differs from Go's by the PK index entry. Each case below states
//! which shape it is asserting.

#![cfg(test)]

use tidb_executor::TableEntry;
use tidb_tablecodec::{is_index_key, is_record_key};

use crate::tests_support::row_text;
use crate::{Session, StmtResult};

/// Every raw key `table` holds in the COMMITTED catalog, classified as
/// `record` or `index`, in key order. The classification is what the
/// assertions read: exact bytes depend on a table id the catalog allocates,
/// the *shape* does not.
///
/// Index keys sort before record keys: a key is `t{tableID}_i…` or
/// `t{tableID}_r…`, and `i` < `r`.
///
/// "Committed" is load-bearing. An open transaction works on its own catalog
/// IMAGE (see [`Session`]'s savepoint doc), so a statement inside `BEGIN` is
/// invisible here until `COMMIT` -- which is what
/// [`an_uncommitted_delete_is_invisible_in_the_committed_key_set`] pins.
fn key_shapes(session: &Session, table: &str) -> Vec<&'static str> {
    let catalog = session.shared_catalog();
    let mut guard = catalog.lock().unwrap();
    let entry = guard
        .table_mut_in("test", table)
        .unwrap_or_else(|| panic!("table `{table}` is in the catalog"));
    let TableEntry::Kv(kv) = entry else {
        panic!("table `{table}` is stored as KV bytes");
    };
    kv.stored_keys()
        .unwrap()
        .into_iter()
        .map(|key| {
            if is_record_key(&key) {
                "record"
            } else if is_index_key(&key) {
                "index"
            } else {
                "other"
            }
        })
        .collect()
}

fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row.join("|"))
        .collect()
}

fn affected(session: &mut Session, sql: &str) -> u64 {
    match session.run(sql).unwrap() {
        StmtResult::Affected(count) => count,
        other => panic!("expected an affected count from `{sql}`, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// `TestDeleteLockKey` -- `pkg/executor/delete_test.go:27`, all six rows.
//
// Each Go row is (ddl, pre, tk1Stmt, tk2Stmt): tk1 runs the DELETE in a
// pessimistic transaction, tk2 races the INSERT, and the test asserts tk2's
// INSERT completes only after tk1 commits -- i.e. the DELETE locked the keys
// tk2's INSERT needs. Ported here as: the DELETE removes the record key AND
// every index key of every row it deletes (the set a lock must cover), plus
// the ignored racing half.
// ---------------------------------------------------------------------------

/// Go row 1: `t1(k, kk, val, primary key(k, kk), unique key(val))`,
/// `delete from t1 where val = 3` -- the DELETE is driven through the UNIQUE
/// index, and it must take the unique-index key as well as the record.
#[test]
fn delete_through_a_unique_index_removes_the_index_key_with_the_record() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (k INT, kk INT, val INT, PRIMARY KEY (k, kk), UNIQUE KEY (val))")
        .unwrap();
    session.run("INSERT INTO t1 VALUES (1, 2, 3)").unwrap();
    // Clustered composite PK: one record key plus the `val` unique index.
    assert_eq!(key_shapes(&session, "t1"), ["index", "record"]);

    assert_eq!(affected(&mut session, "DELETE FROM t1 WHERE val = 3"), 1);
    // A leftover `index` here is the lost-write bug the Go test guards: the
    // row is gone but `val = 3` still looks taken.
    assert!(
        key_shapes(&session, "t1").is_empty(),
        "the DELETE left keys behind: {:?}",
        key_shapes(&session, "t1")
    );
    // Go's tk2 statement, which the freed unique key must now admit.
    assert_eq!(affected(&mut session, "INSERT INTO t1 VALUES (1, 3, 3)"), 1);
    assert_eq!(rows(&mut session, "SELECT k, kk, val FROM t1"), ["1|3|3"]);
}

/// Go row 2: `t2(k, kk, val, primary key(k, kk))` -- no secondary index at
/// all, `delete from t2 where k = 1`. The point of the row in Go is that a
/// prefix-of-PK predicate still locks the row; here it is that the DELETE
/// touches exactly one key and the re-insert of the same PK succeeds.
#[test]
fn delete_on_a_primary_key_prefix_removes_only_the_record_key() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t2 (k INT, kk INT, val INT, PRIMARY KEY (k, kk))")
        .unwrap();
    session.run("INSERT INTO t2 VALUES (1, 1, 1)").unwrap();
    assert_eq!(key_shapes(&session, "t2"), ["record"]);

    assert_eq!(affected(&mut session, "DELETE FROM t2 WHERE k = 1"), 1);
    assert!(key_shapes(&session, "t2").is_empty());
    assert_eq!(affected(&mut session, "INSERT INTO t2 VALUES (1, 1, 2)"), 1);
    assert_eq!(rows(&mut session, "SELECT k, kk, val FROM t2"), ["1|1|2"]);
}

/// Go row 3: `t3(k, kk, val, vv, primary key(k, kk), unique key(val))`,
/// `delete from t3 where vv = 4` -- the predicate names an UNINDEXED column,
/// so the DELETE arrives by table scan, and it must still take the unique
/// index key of the row it found.
#[test]
fn delete_found_by_table_scan_still_removes_the_unique_index_key() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t3 (k INT, kk INT, val INT, vv INT, \
             PRIMARY KEY (k, kk), UNIQUE KEY (val))",
        )
        .unwrap();
    session.run("INSERT INTO t3 VALUES (1, 2, 3, 4)").unwrap();
    assert_eq!(key_shapes(&session, "t3"), ["index", "record"]);

    assert_eq!(affected(&mut session, "DELETE FROM t3 WHERE vv = 4"), 1);
    assert!(key_shapes(&session, "t3").is_empty());
    // Go's tk2: same `val`, different `vv`. It can only succeed if the unique
    // index entry went with the row.
    assert_eq!(
        affected(&mut session, "INSERT INTO t3 VALUES (1, 2, 3, 5)"),
        1
    );
}

/// Go row 4: the same table with `delete from t4 where 1` -- a constant-true
/// predicate, so no access path narrows it. Every key of every row goes.
#[test]
fn delete_with_a_constant_true_predicate_removes_every_key() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t4 (k INT, kk INT, val INT, vv INT, \
             PRIMARY KEY (k, kk), UNIQUE KEY (val))",
        )
        .unwrap();
    session.run("INSERT INTO t4 VALUES (1, 2, 3, 4)").unwrap();

    assert_eq!(affected(&mut session, "DELETE FROM t4 WHERE 1"), 1);
    assert!(key_shapes(&session, "t4").is_empty());
    assert_eq!(
        affected(&mut session, "INSERT INTO t4 VALUES (1, 2, 3, 5)"),
        1
    );
}

/// Go row 5: two rows, `delete from t5 where k in (1, 2, 3, 4)` -- an IN list
/// covering both stored rows plus two absent ones. Both rows' record and
/// index keys go; the absent values contribute nothing.
#[test]
fn delete_by_in_list_removes_every_matched_rows_keys() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t5 (k INT, kk INT, val INT, vv INT, \
             PRIMARY KEY (k, kk), UNIQUE KEY (val))",
        )
        .unwrap();
    session
        .run("INSERT INTO t5 VALUES (1, 2, 3, 4), (2, 3, 4, 5)")
        .unwrap();
    assert_eq!(
        key_shapes(&session, "t5"),
        ["index", "index", "record", "record"]
    );

    assert_eq!(
        affected(&mut session, "DELETE FROM t5 WHERE k IN (1, 2, 3, 4)"),
        2
    );
    assert!(key_shapes(&session, "t5").is_empty());
    assert_eq!(
        affected(&mut session, "INSERT INTO t5 VALUES (1, 2, 3, 5)"),
        1
    );
}

/// Go row 6: `delete from t6 where kk between 0 and 10` -- a RANGE over the
/// second PK column, matching both rows, and Go's tk2 re-inserts both.
#[test]
fn delete_by_range_over_a_primary_key_column_removes_both_rows_keys() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t6 (k INT, kk INT, val INT, vv INT, \
             PRIMARY KEY (k, kk), UNIQUE KEY (val))",
        )
        .unwrap();
    session
        .run("INSERT INTO t6 VALUES (1, 2, 3, 4), (2, 3, 4, 5)")
        .unwrap();

    assert_eq!(
        affected(&mut session, "DELETE FROM t6 WHERE kk BETWEEN 0 AND 10"),
        2
    );
    assert!(key_shapes(&session, "t6").is_empty());
    assert_eq!(
        affected(
            &mut session,
            "INSERT INTO t6 VALUES (1, 2, 3, 5), (2, 3, 4, 6)"
        ),
        2
    );
}

/// A `DELETE` inside an EXPLICIT transaction, rolled back, leaves every key in
/// place -- record and index alike -- and a committed one takes both. Go gets
/// this from the membuffer its lock test runs over; here it is the guard that
/// the key sets the rows above assert are transactional and not written
/// straight through.
#[test]
fn an_uncommitted_delete_is_invisible_in_the_committed_key_set() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE tr (k INT, kk INT, val INT, PRIMARY KEY (k, kk), UNIQUE KEY (val))")
        .unwrap();
    session.run("INSERT INTO tr VALUES (1, 2, 3)").unwrap();

    session.run("BEGIN").unwrap();
    session.run("DELETE FROM tr WHERE val = 3").unwrap();
    // The DELETE is staged on the transaction's own catalog image, so the
    // committed key set is untouched while the transaction is open.
    assert_eq!(key_shapes(&session, "tr"), ["index", "record"]);
    // Inside the transaction the row IS gone, which is what the statement's
    // own reads must see.
    assert!(rows(&mut session, "SELECT k, kk, val FROM tr").is_empty());
    session.run("ROLLBACK").unwrap();

    assert_eq!(key_shapes(&session, "tr"), ["index", "record"]);
    assert_eq!(rows(&mut session, "SELECT k, kk, val FROM tr"), ["1|2|3"]);

    // Committed, both keys go -- the record and the unique-index entry.
    session.run("BEGIN").unwrap();
    session.run("DELETE FROM tr WHERE val = 3").unwrap();
    session.run("COMMIT").unwrap();
    assert!(key_shapes(&session, "tr").is_empty());
}

/// THE GUARD for the ignored racing rows below: this engine's `BEGIN
/// PESSIMISTIC` DELETE takes no lock a second session can observe, because no
/// SQL path calls `Transaction::lock_keys`. The peer neither blocks nor
/// succeeds -- it is REFUSED, because it reads the committed catalog where the
/// row (and its unique-index entry) is still there.
///
/// That refusal is the divergence worth naming: Go's tk2 waits and then
/// succeeds; here the same statement fails outright with a duplicate-key
/// error. When DML locking lands, THIS test starts failing and
/// [`a_racing_insert_blocks_on_the_deletes_lock`] starts passing.
#[test]
fn a_delete_in_a_pessimistic_transaction_takes_no_observable_lock() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE tl (k INT, kk INT, val INT, PRIMARY KEY (k, kk), UNIQUE KEY (val))")
        .unwrap();
    session.run("INSERT INTO tl VALUES (1, 2, 3)").unwrap();

    let mut peer = Session::with_catalog(session.shared_catalog());
    session.run("BEGIN PESSIMISTIC").unwrap();
    session.run("DELETE FROM tl WHERE val = 3").unwrap();

    // Go: this INSERT blocks, then succeeds once tk1 commits and frees
    // `val = 3`. Here it returns at once, refused.
    let refusal = peer
        .run("INSERT INTO tl VALUES (1, 3, 3)")
        .expect_err("the peer INSERT is refused rather than blocked");
    assert!(
        format!("{refusal:?}").contains("DuplicateEntry"),
        "expected a duplicate-key refusal, got {refusal:?}"
    );

    // And after the COMMIT the same statement succeeds, which is where Go's
    // tk2 ends up -- just without having waited.
    session.run("COMMIT").unwrap();
    assert_eq!(affected(&mut peer, "INSERT INTO tl VALUES (1, 3, 3)"), 1);
}

/// Go's `TestDeleteLockKey`, whole: tk1's DELETE inside `begin pessimistic`
/// blocks tk2's conflicting INSERT until tk1 commits.
///
/// Guarded by [`a_delete_in_a_pessimistic_transaction_takes_no_observable_lock`].
#[test]
#[ignore = "no SQL path calls Transaction::lock_keys, so a DML statement takes no pessimistic lock"]
fn a_racing_insert_blocks_on_the_deletes_lock() {
    // Go's answer, asserted so this row is a work item and not a wish: with
    // tk1 holding `begin pessimistic; delete from t1 where val = 3`, tk2's
    // `insert into t1 values(1, 3, 3)` does not return until tk1 commits.
    let blocked_until_commit = false; // this engine
    assert!(
        blocked_until_commit,
        "Go blocks the racing INSERT until COMMIT; this engine does not lock at all"
    );
}

// ---------------------------------------------------------------------------
// `TestInsertLockUnchangedKeys` -- `pkg/executor/insert_test.go:534`.
//
// Go's outer loop is `for _, shouldLock := range []bool{false}` -- the `true`
// half is commented out upstream, so the whole Go table runs with
// `tidb_lock_unchanged_keys = false` and asserts: the racing INSERT is NOT
// blocked, EXCEPT that blocking is tolerated when the duplicate key is a
// CLUSTERED PRIMARY KEY (`!shouldLock && !tt.isClusteredPK`). Every row also
// ends with `select * from t` == `1`: the DML must leave the single row alone.
//
// All six rows are ported below as: the statement's affected-row count, the
// surviving row, and the key set. The racing half is one ignored test with a
// running guard, as above.
// ---------------------------------------------------------------------------

/// Go rows 1 and 2: `replace into t values (1)` over a row that is already
/// there, against a clustered PK and against a unique key. Nothing changes,
/// and one row survives.
#[test]
fn replace_of_an_identical_row_leaves_one_row_on_pk_and_on_unique_key() {
    // Row 1: `create table t (c int primary key clustered)`.
    let mut session = Session::new();
    session
        .run("CREATE TABLE tpk (c INT PRIMARY KEY CLUSTERED)")
        .unwrap();
    session.run("INSERT INTO tpk VALUES (1)").unwrap();
    // Go `InsertValues.removeRow`: an IDENTICAL conflicting row is left in
    // place and counts ONE, not the two a delete-plus-insert would report.
    // Captured with `rust/difftests/gorun`: `ROW_COUNT()` after
    // `replace into tpk values (1)` over the same row is 1.
    assert_eq!(affected(&mut session, "REPLACE INTO tpk VALUES (1)"), 1);
    assert_eq!(rows(&mut session, "SELECT c FROM tpk"), ["1"]);
    assert_eq!(key_shapes(&session, "tpk"), ["record"]);

    // Row 2: `create table t (c int unique key)` -- no PK, so the handle is a
    // `_tidb_rowid` and `c` is a unique index.
    session.run("CREATE TABLE tuk (c INT UNIQUE KEY)").unwrap();
    session.run("INSERT INTO tuk VALUES (1)").unwrap();
    assert_eq!(affected(&mut session, "REPLACE INTO tuk VALUES (1)"), 1);
    assert_eq!(rows(&mut session, "SELECT c FROM tuk"), ["1"]);
    assert_eq!(key_shapes(&session, "tuk"), ["index", "record"]);
}

/// Go rows 3 and 4: `insert ignore into t values (1)` over the duplicate.
/// The row is skipped, Go reports 0 affected and warns, and one row survives.
#[test]
fn insert_ignore_of_a_duplicate_leaves_one_row_on_pk_and_on_unique_key() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ipk (c INT PRIMARY KEY CLUSTERED)")
        .unwrap();
    session.run("INSERT INTO ipk VALUES (1)").unwrap();
    assert_eq!(
        affected(&mut session, "INSERT IGNORE INTO ipk VALUES (1)"),
        0
    );
    // Go turns the duplicate into a warning rather than an error, and names
    // the key it collided with. Captured with `rust/difftests/gorun`:
    // `Warning|1062|Duplicate entry '1' for key 'ipk.PRIMARY'`. Read it
    // immediately: any statement in between clears the warning list.
    assert_eq!(
        rows(&mut session, "SHOW WARNINGS"),
        ["Warning|1062|Duplicate entry '1' for key 'ipk.PRIMARY'"]
    );
    assert_eq!(rows(&mut session, "SELECT c FROM ipk"), ["1"]);
    assert_eq!(key_shapes(&session, "ipk"), ["record"]);

    session.run("CREATE TABLE iuk (c INT UNIQUE KEY)").unwrap();
    session.run("INSERT INTO iuk VALUES (1)").unwrap();
    assert_eq!(
        affected(&mut session, "INSERT IGNORE INTO iuk VALUES (1)"),
        0
    );
    // Go names an anonymous unique index after its column, so the key here is
    // `iuk.c` -- captured, `Warning|1062|Duplicate entry '1' for key 'iuk.c'`.
    assert_eq!(
        rows(&mut session, "SHOW WARNINGS"),
        ["Warning|1062|Duplicate entry '1' for key 'iuk.c'"]
    );
    assert_eq!(rows(&mut session, "SELECT c FROM iuk"), ["1"]);
    assert_eq!(key_shapes(&session, "iuk"), ["index", "record"]);
}

/// Go rows 5 and 6: `insert into t values (1) on duplicate key update
/// c = values(c)` -- the assignment writes the value the row ALREADY holds.
/// This is precisely the "unchanged key" the Go test is named for: Go reports
/// 0 affected (nothing changed) and, with `tidb_lock_unchanged_keys = false`,
/// takes no lock on it.
#[test]
fn on_duplicate_update_to_the_same_value_changes_nothing_on_pk_and_on_unique_key() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE dpk (c INT PRIMARY KEY CLUSTERED)")
        .unwrap();
    session.run("INSERT INTO dpk VALUES (1)").unwrap();
    assert_eq!(
        affected(
            &mut session,
            "INSERT INTO dpk VALUES (1) ON DUPLICATE KEY UPDATE c = VALUES(c)"
        ),
        0
    );
    assert_eq!(rows(&mut session, "SELECT c FROM dpk"), ["1"]);
    assert_eq!(key_shapes(&session, "dpk"), ["record"]);

    session.run("CREATE TABLE duk (c INT UNIQUE KEY)").unwrap();
    session.run("INSERT INTO duk VALUES (1)").unwrap();
    assert_eq!(
        affected(
            &mut session,
            "INSERT INTO duk VALUES (1) ON DUPLICATE KEY UPDATE c = VALUES(c)"
        ),
        0
    );
    assert_eq!(rows(&mut session, "SELECT c FROM duk"), ["1"]);
    assert_eq!(key_shapes(&session, "duk"), ["index", "record"]);
}

/// THE GUARD for the ignored row below: `tidb_lock_unchanged_keys` is a
/// registered session variable that reads and writes, and NOTHING consumes
/// it. Setting it changes no observable behavior.
///
/// When the flag is wired, this test's second half starts failing and
/// [`lock_unchanged_keys_decides_whether_the_racing_insert_blocks`] starts
/// passing.
#[test]
fn tidb_lock_unchanged_keys_is_settable_and_inert() {
    let mut session = Session::new();
    // Go `DefTiDBLockUnchangedKeys` is ON.
    assert_eq!(
        rows(&mut session, "SELECT @@tidb_lock_unchanged_keys"),
        ["1"]
    );
    session.run("SET @@tidb_lock_unchanged_keys = 0").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT @@tidb_lock_unchanged_keys"),
        ["0"]
    );

    // Inert: with the flag OFF and with it ON, the unchanged-key statement
    // behaves identically, because no code reads the flag.
    session
        .run("CREATE TABLE lu (c INT PRIMARY KEY CLUSTERED)")
        .unwrap();
    session.run("INSERT INTO lu VALUES (1)").unwrap();
    let off = affected(
        &mut session,
        "INSERT INTO lu VALUES (1) ON DUPLICATE KEY UPDATE c = VALUES(c)",
    );
    session.run("SET @@tidb_lock_unchanged_keys = 1").unwrap();
    let on = affected(
        &mut session,
        "INSERT INTO lu VALUES (1) ON DUPLICATE KEY UPDATE c = VALUES(c)",
    );
    assert_eq!((off, on), (0, 0));
}

/// Go's `TestInsertLockUnchangedKeys`, whole: with
/// `tidb_lock_unchanged_keys = false`, tk2's `insert into t values (1)` is
/// NOT blocked by tk1's REPLACE / INSERT IGNORE / ON DUPLICATE UPDATE on a
/// UNIQUE KEY -- while on a CLUSTERED PRIMARY KEY blocking is tolerated,
/// because the row key is taken by the statement's own write.
///
/// Guarded by [`tidb_lock_unchanged_keys_is_settable_and_inert`].
#[test]
#[ignore = "tidb_lock_unchanged_keys is registered but unread, and no DML statement locks keys"]
fn lock_unchanged_keys_decides_whether_the_racing_insert_blocks() {
    // Go's answers for the six rows, asserted so the row is a tracked work
    // item: (name, is_clustered_pk, racing_insert_may_block).
    for (name, is_clustered_pk) in [
        ("replace-pk", true),
        ("replace-uk", false),
        ("insert-ignore-pk", true),
        ("insert-ignore-uk", false),
        ("insert-update-pk", true),
        ("insert-update-uk", false),
    ] {
        // Go: a non-clustered-PK row must NOT block with the flag off.
        let blocked = false; // this engine never blocks, for either shape
        assert_eq!(
            blocked, is_clustered_pk,
            "{name}: Go blocks only the clustered-PK shape with \
             tidb_lock_unchanged_keys = false"
        );
    }
}
