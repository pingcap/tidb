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

//! READING A TABLE THE OPEN TRANSACTION HAS WRITTEN: which rows come back, and
//! in which ORDER.
//!
//! # What Go does, and which half of it this tier already had
//!
//! Go's `UnionScanExec` (`pkg/executor/union_scan.go`) merges two streams --
//! the coprocessor's SNAPSHOT stream and the transaction membuffer's ADDED
//! stream -- and does three things with them:
//!
//!  * a snapshot row whose handle also appears in the membuffer is DROPPED
//!    (`getSnapshotRow`: `if _, err := us.memBufSnap.Get(ctx, checkKey); err
//!    == nil { continue }`), so an updated row is not returned twice and a
//!    deleted one is not returned at all;
//!  * a staged row is returned in its place, from the membuffer;
//!  * the two streams INTERLEAVE by `compareExec::compare` -- the index's own
//!    key columns first (`usedIndex`, filled in for a double read by
//!    `builder.go`'s `*IndexLookUpExecutor` arm), the handle last.
//!
//! The planner puts that operator over the reader only when
//! `tableHasDirtyContent` -> `session.HasDirtyContent(tid)` holds: the
//! transaction's membuffer has a key under THAT table's prefix
//! (`pkg/session/txn.go:730`).
//!
//! The first two rules are already this tier's, for free and by construction:
//! a transaction here stages into a private CATALOG COPY, so an update
//! overwrites the row and a delete removes it in the one stream every read
//! walks. Read-your-own-writes therefore never depended on the operator, and
//! the first test below asserts the row SET beside the order so that stays
//! true.
//!
//! The third rule is the one that was missing, and the only one with an
//! observable cost: a NON-COVERING index read answers in handle order here
//! (Go's `canReorderHandles` batch sort, ported in
//! `tidb_executor::access_path`), which for a dirty table is not what the
//! merge above it produces. Merging one index-ordered stream into another
//! index-ordered stream yields that same index order, so on this tier the
//! whole of `compare()` reduces to: over a dirty table, do not reorder the
//! handle batch.
//!
//! # Why this fixture discriminates
//!
//! Handle order and `ki` order DISAGREE on every row of `us`, in both
//! directions, so "handle order", "index order" and "insertion order" are
//! three different answers and an assertion cannot pass by accident. The reads
//! below are the sides of the gate: a dirty table; a clean table read inside
//! the same dirty transaction; the same table before the transaction, after
//! `COMMIT`, after `ROLLBACK`, and after an autocommit write; and each of the
//! three row writes marking the table on its own.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// `h` is the handle, `k` the indexed column, `v` a column the index does NOT
/// carry -- so `select h, v ... use index(ki)` is a double read (Go's
/// `IndexLookUp`), the shape `UnionScanExec` reorders.
///
/// The committed rows are laid out so the `ki` walk (`k` = 30, 50, 60, 70 ->
/// handles 2, 1, 4, 3) is a DERANGEMENT of the handle walk: no row is in the
/// same position under the two orders.
fn session_with_rows() -> Session {
    let mut session = Session::new();
    for ddl in [
        "CREATE TABLE us (h INT PRIMARY KEY, k INT, v INT, KEY ki (k))",
        "CREATE TABLE clean (h INT PRIMARY KEY, k INT, v INT, KEY ki (k))",
    ] {
        session.run(ddl).unwrap();
    }
    session
        .run("INSERT INTO us VALUES (1, 50, 100), (2, 30, 200), (3, 70, 300), (4, 60, 400)")
        .unwrap();
    session
        .run("INSERT INTO clean VALUES (1, 50, 100), (2, 30, 200), (3, 70, 300)")
        .unwrap();
    session
}

/// The double read this whole module is about.
fn double_read(session: &mut Session, table: &str) -> Vec<Vec<String>> {
    rows(
        session,
        &format!("SELECT h, v FROM {table} USE INDEX(ki) WHERE k > 0"),
    )
}

/// THE MISSING OPERATOR'S ONE OBSERVABLE EFFECT: a double read of a table the
/// open transaction has written answers in INDEX order, and the three staged
/// edits each show up the way `UnionScanExec` shows them.
///
/// The transaction stages one of each kind Go's merge distinguishes:
///
///  * `h=5, k=10` -- a staged row that sorts BEFORE every snapshot row, so it
///    must come FIRST. Go reaches it through `getOneRow`'s `isSnapshotRow =
///    isSnapshotRowInt < 0` arm on the very first comparison;
///  * `h=2, k=30 -> 80` -- a staged row REPLACING a snapshot row under the
///    same handle, which must appear ONCE (Go drops the snapshot copy in
///    `getSnapshotRow`) and at its NEW index position, which is last;
///  * `h=3` -- a DELETED row, which must not appear at all.
///
/// Derived from Go for this exact fixture: the snapshot stream is `h1(k=50)`,
/// `h4(k=60)` (h2 and h3 are dropped by the membuffer probe), the added stream
/// is `h5(k=10)`, `h2(k=80)`, and `compare` on `k` then handle interleaves them
/// as `h5, h1, h4, h2`.
///
/// Both halves are asserted: the ORDERED vector is the merge, and the sorted
/// multiset beside it is read-your-own-writes -- which held before this rule
/// existed and must keep holding.
#[test]
fn a_double_read_of_a_dirty_table_answers_in_index_order_with_the_staged_rows_merged_in() {
    let mut session = session_with_rows();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO us VALUES (5, 10, 500)").unwrap();
    session.run("UPDATE us SET k = 80 WHERE h = 2").unwrap();
    session.run("DELETE FROM us WHERE h = 3").unwrap();

    let answer = double_read(&mut session, "us");
    assert_eq!(
        answer,
        vec![
            vec!["5".to_owned(), "500".to_owned()],
            vec!["1".to_owned(), "100".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
        ],
    );
    // The row SET, stated separately so a future ordering change cannot hide
    // a lost or duplicated row: the deleted handle is gone, the replaced one
    // is present exactly once, the staged insert is visible.
    let mut set = answer;
    set.sort();
    assert_eq!(
        set,
        vec![
            vec!["1".to_owned(), "100".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
            vec!["5".to_owned(), "500".to_owned()],
        ],
    );
}

/// EACH of the three row writes marks the table on its own. The test above
/// stages all three at once, so it would still pass with two of the three
/// marks missing; these three stage exactly one apiece.
///
/// Every expected vector below is the `ki` walk and disagrees with the handle
/// walk, so "unmarked" is a visible answer rather than a coincidence:
/// `1, 2, 3, 4`-shaped orders are what a missing mark produces.
#[test]
fn a_staged_insert_alone_marks_the_table() {
    let mut session = session_with_rows();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO us VALUES (5, 10, 500)").unwrap();
    assert_eq!(
        double_read(&mut session, "us"),
        vec![
            vec!["5".to_owned(), "500".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
            vec!["1".to_owned(), "100".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
        ],
    );
}

/// See [`a_staged_insert_alone_marks_the_table`]. The update touches only a
/// NON-indexed column, so it moves no index entry and the order it produces
/// comes from the mark alone.
#[test]
fn a_staged_update_alone_marks_the_table() {
    let mut session = session_with_rows();
    session.run("BEGIN").unwrap();
    session.run("UPDATE us SET v = 999 WHERE h = 1").unwrap();
    assert_eq!(
        double_read(&mut session, "us"),
        vec![
            vec!["2".to_owned(), "200".to_owned()],
            vec!["1".to_owned(), "999".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
        ],
    );
}

/// See [`a_staged_insert_alone_marks_the_table`]. A delete stages a membuffer
/// entry in Go just as a write does, which is why `HasDirtyContent` -- a plain
/// prefix seek over the buffer -- answers true for a transaction that has only
/// deleted.
#[test]
fn a_staged_delete_alone_marks_the_table() {
    let mut session = session_with_rows();
    session.run("BEGIN").unwrap();
    session.run("DELETE FROM us WHERE h = 1").unwrap();
    assert_eq!(
        double_read(&mut session, "us"),
        vec![
            vec!["2".to_owned(), "200".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
        ],
    );
}

/// The tie-break is the HANDLE, which is `compare`'s last step after every
/// `usedIndex` column compared equal (`pkg/executor/union_scan.go:327`:
/// `cmp, err = ce.handleCols.Compare(a, b, ...)`).
///
/// `h=6` is staged with the `k` an existing row already has, so the two rows
/// tie on the only index column and only the handle separates them: `h=2`
/// before `h=6`. Reversing that step reorders the first two rows without
/// touching the rest.
#[test]
fn rows_that_tie_on_the_index_key_come_back_in_handle_order() {
    let mut session = session_with_rows();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO us VALUES (6, 30, 600)").unwrap();
    assert_eq!(
        double_read(&mut session, "us"),
        vec![
            vec!["2".to_owned(), "200".to_owned()],
            vec!["6".to_owned(), "600".to_owned()],
            vec!["1".to_owned(), "100".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
        ],
    );
}

/// THE GATE IS PER TABLE, not per transaction: Go asks
/// `HasDirtyContent(tableInfo.ID)` about the table being read, so a table the
/// transaction has NOT written gets no `UnionScan` and its double read still
/// answers in handle order.
///
/// `clean` is read from inside the very transaction that dirtied `us`. Handle
/// order is `1, 2, 3`; the `ki` walk would be `2, 1, 3`, so a gate that
/// keyed off "a transaction is open" fails here.
#[test]
fn a_clean_table_read_inside_a_dirty_transaction_still_answers_in_handle_order() {
    let mut session = session_with_rows();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO us VALUES (5, 10, 500)").unwrap();
    assert_eq!(
        double_read(&mut session, "clean"),
        vec![
            vec!["1".to_owned(), "100".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
        ],
    );
}

/// A read with NO transaction behind it keeps Go's unordered double read:
/// `canReorderHandles` is true, the handle batch is sorted, and the answer is
/// handle-ascending -- `1, 2, 3, 4` where the `ki` walk would be `2, 1, 4, 3`.
///
/// This is the control that makes the dirty-read assertion mean something: the
/// rule under test is a DIFFERENCE between two reads of the same statement over
/// the same rows, not a blanket "index reads answer in index order".
#[test]
fn a_double_read_outside_a_transaction_answers_in_handle_order() {
    let mut session = session_with_rows();
    assert_eq!(
        double_read(&mut session, "us"),
        vec![
            vec!["1".to_owned(), "100".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
        ],
    );
}

/// The staged-write mark is CLEARED when the transaction ends, because Go's
/// membuffer is: after `COMMIT` the same statement is an ordinary autocommit
/// read of a clean table and goes back to handle order.
///
/// The rows are the committed result of the transaction above, so the two
/// candidate orders are `1, 2, 4, 5` (handle) and `5, 1, 4, 2` (`ki`, since
/// `h=2` now holds `k=80`) -- they disagree, so a mark left set is visible
/// here rather than silently harmless.
#[test]
fn the_dirty_mark_does_not_outlive_the_transaction_that_set_it() {
    let mut session = session_with_rows();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO us VALUES (5, 10, 500)").unwrap();
    session.run("UPDATE us SET k = 80 WHERE h = 2").unwrap();
    session.run("DELETE FROM us WHERE h = 3").unwrap();
    session.run("COMMIT").unwrap();
    assert_eq!(
        double_read(&mut session, "us"),
        vec![
            vec!["1".to_owned(), "100".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
            vec!["5".to_owned(), "500".to_owned()],
        ],
    );
}

/// The mark does not leak from an AUTOCOMMIT write into the next statement
/// either. Go gives every autocommit statement its own membuffer, so the
/// `SELECT` after an `INSERT` sees a clean table.
///
/// Without the statement-boundary clear this read answers `2, 1, 4, 3, 5`
/// (the `ki` walk) instead of the handle order below -- the failure mode is a
/// plain `INSERT; SELECT` pair, which is most of the corpus.
#[test]
fn an_autocommit_write_does_not_leave_the_table_marked_for_the_next_statement() {
    let mut session = session_with_rows();
    session.run("INSERT INTO us VALUES (5, 10, 500)").unwrap();
    assert_eq!(
        double_read(&mut session, "us"),
        vec![
            vec!["1".to_owned(), "100".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
            vec!["5".to_owned(), "500".to_owned()],
        ],
    );
}

/// ROLLBACK takes the mark back with the rows: the transaction's staged copy
/// is discarded whole, so the shared catalog was never marked at all and the
/// next read is an ordinary clean one over the ORIGINAL four rows.
#[test]
fn rollback_leaves_neither_the_staged_rows_nor_the_mark() {
    let mut session = session_with_rows();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO us VALUES (5, 10, 500)").unwrap();
    session.run("DELETE FROM us WHERE h = 3").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        double_read(&mut session, "us"),
        vec![
            vec!["1".to_owned(), "100".to_owned()],
            vec!["2".to_owned(), "200".to_owned()],
            vec!["3".to_owned(), "300".to_owned()],
            vec!["4".to_owned(), "400".to_owned()],
        ],
    );
}
