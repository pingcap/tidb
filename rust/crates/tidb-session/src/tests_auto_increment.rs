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

//! `AUTO_INCREMENT` allocation, rebasing, publication and burn.
//!
//! Every expectation here was captured from real TiDB through
//! `testkit.CreateMockStore` (`pkg/session/test`, `-tags=intest`) against
//! `CREATE TABLE ai (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, v INT
//! UNIQUE)` and its `AUTO_INCREMENT=100` variant.
//!
//! Two things the capture shows are deliberately NOT reproduced, because both
//! are artifacts of Go's BATCH-CACHING allocator rather than of
//! `AUTO_INCREMENT` semantics, and this tier's allocator has no cache:
//!
//!  * `SHOW CREATE TABLE`'s reported `AUTO_INCREMENT=` is the cached END of
//!    the current batch, not the next id -- Go answers `AUTO_INCREMENT=2000100`
//!    for a table seeded at 100 that has handed out exactly two ids.
//!  * consequently `ALTER TABLE ... AUTO_INCREMENT=n` looks like it does
//!    nothing in the mock store: the counter is already millions past `n`, and
//!    a rebase never moves DOWN. The rule captured -- rebase up only -- is the
//!    one reproduced; the cached numbers are not.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

fn one(session: &mut Session, sql: &str) -> String {
    rows(session, sql).remove(0).remove(0)
}

/// A session with the standard capture table, optionally seeded.
fn table(seed: Option<i64>) -> Session {
    let mut session = Session::new();
    let mut sql =
        "CREATE TABLE ai (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, v INT UNIQUE)".to_owned();
    if let Some(seed) = seed {
        sql.push_str(&format!(" AUTO_INCREMENT={seed}"));
    }
    session.run(&sql).unwrap();
    session
}

/// The `AUTO_INCREMENT=n` table option seeds the allocator, so the FIRST row
/// lands on `n` rather than on 1. Captured: ids 100 and 101.
#[test]
fn the_create_table_auto_increment_option_seeds_the_first_id() {
    let mut session = table(Some(100));
    session.run("INSERT INTO ai (v) VALUES (1),(2)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["100", "1"], ["101", "2"]]
    );
}

/// `ALTER TABLE ... AUTO_INCREMENT=n` is a REBASE: it moves the counter up to
/// `n` and does nothing at all when the counter has already run past it. The
/// mock store hides the second half behind its batch cache; the rule is Go's
/// `Allocator.Rebase`, which only ever raises the base.
#[test]
fn alter_table_auto_increment_only_raises_the_counter() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    session.run("ALTER TABLE ai AUTO_INCREMENT=500").unwrap();
    session.run("INSERT INTO ai (v) VALUES (2)").unwrap();
    // Naming a value the counter is already past changes nothing.
    session.run("ALTER TABLE ai AUTO_INCREMENT=10").unwrap();
    session.run("INSERT INTO ai (v) VALUES (3)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["1", "1"], ["500", "2"], ["501", "3"]]
    );
}

/// An explicit value LARGER than the counter rebases it; a SMALLER one does
/// not; explicit `0` and explicit `NULL` both mean "allocate". Captured:
/// 50, 51, 10, 52, 53, 54 in that statement order.
#[test]
fn an_explicit_value_rebases_upward_only_and_zero_and_null_allocate() {
    let mut session = table(None);
    session.run("INSERT INTO ai (id,v) VALUES (50,1)").unwrap();
    session.run("INSERT INTO ai (v) VALUES (2)").unwrap();
    session.run("INSERT INTO ai (id,v) VALUES (10,3)").unwrap();
    session.run("INSERT INTO ai (v) VALUES (4)").unwrap();
    session.run("INSERT INTO ai (id,v) VALUES (0,5)").unwrap();
    session
        .run("INSERT INTO ai (id,v) VALUES (NULL,6)")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [
            ["10", "3"],
            ["50", "1"],
            ["51", "2"],
            ["52", "4"],
            ["53", "5"],
            ["54", "6"],
        ]
    );
}

/// An explicit value never publishes `LAST_INSERT_ID()`: only an ALLOCATED id
/// does. Captured: after `INSERT INTO ai (id,v) VALUES (77,9)` the reported
/// id is still the one the previous allocating statement published.
#[test]
fn an_explicit_id_does_not_publish_last_insert_id() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "1");
    session.run("INSERT INTO ai (id,v) VALUES (77,9)").unwrap();
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "1");
}

/// One multi-row insert allocates row by row, interleaved with the rebases
/// its explicit values cause, and publishes the FIRST allocated id.
/// Captured over `(NULL,1),(30,2),(NULL,3),(5,4),(0,5)`: ids 1, 30, 31, 5, 32
/// and `LAST_INSERT_ID()` = 1.
#[test]
fn a_multi_row_insert_interleaves_allocation_with_rebasing() {
    let mut session = table(None);
    session
        .run("INSERT INTO ai (id,v) VALUES (NULL,1),(30,2),(NULL,3),(5,4),(0,5)")
        .unwrap();
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "1");
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [
            ["1", "1"],
            ["5", "4"],
            ["30", "2"],
            ["31", "3"],
            ["32", "5"],
        ]
    );
}

/// A row redirected into `ON DUPLICATE KEY UPDATE` still BURNS its id but
/// never publishes it, because it is never handed to storage as an insert.
/// Captured: after two rows (ids 1,2) an ODKU statement that updates row 1
/// leaves `LAST_INSERT_ID()` at 1 and the next insert takes 4.
#[test]
fn on_duplicate_key_update_burns_its_id_without_publishing_it() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1),(2)").unwrap();
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "1");
    session
        .run("INSERT INTO ai (v) VALUES (1) ON DUPLICATE KEY UPDATE v = 9")
        .unwrap();
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "1");
    session.run("INSERT INTO ai (v) VALUES (4)").unwrap();
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "4");
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["1", "9"], ["2", "2"], ["4", "4"]]
    );
}

/// `REPLACE` inserts a real row, so it publishes the id it allocated.
/// Captured: `REPLACE INTO ai (v) VALUES (1)` over stored `(1,1)` answers id
/// 2 and reports `LAST_INSERT_ID()` = 2.
#[test]
fn replace_publishes_the_id_it_allocated() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    session.run("REPLACE INTO ai (v) VALUES (1)").unwrap();
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "2");
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["2", "1"]]
    );
}

/// A statement whose EXPLICIT id duplicates a stored one fails and publishes
/// nothing -- it allocated nothing to publish -- but the failing value still
/// rebased the counter. Captured: explicit 7 twice, then the next allocation
/// is 8 and `LAST_INSERT_ID()` never moves.
#[test]
fn an_explicit_duplicate_rebases_but_publishes_nothing() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ai (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO ai (id,v) VALUES (7,1)").unwrap();
    let before = one(&mut session, "SELECT last_insert_id()");
    assert!(session.run("INSERT INTO ai (id,v) VALUES (7,2)").is_err());
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), before);
    session.run("INSERT INTO ai (v) VALUES (3)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["7", "1"], ["8", "3"]]
    );
}

/// Every id a rolled-back transaction consumed stays consumed: the allocator
/// is not transactional. Captured: two rows inserted and rolled back, then
/// the next insert takes 3.
#[test]
fn a_rolled_back_transaction_burns_the_ids_it_consumed() {
    let mut session = table(None);
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    session.run("INSERT INTO ai (v) VALUES (2)").unwrap();
    session.run("ROLLBACK").unwrap();
    session.run("INSERT INTO ai (v) VALUES (3)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["3", "3"]]
    );
}

/// A COMMITTED transaction keeps its ids too, so the shared counter is not a
/// rollback-only trick.
#[test]
fn a_committed_transaction_keeps_its_ids() {
    let mut session = table(None);
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    session.run("COMMIT").unwrap();
    session.run("INSERT INTO ai (v) VALUES (2)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["1", "1"], ["2", "2"]]
    );
}

/// `TRUNCATE` starts the counter over, which Go reaches by replacing the
/// table with a fresh one -- the one operation that moves the base DOWN.
#[test]
fn truncate_restarts_the_counter() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1),(2)").unwrap();
    session.run("TRUNCATE TABLE ai").unwrap();
    session.run("INSERT INTO ai (v) VALUES (3)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["1", "3"]]
    );
}

/// REFUSED, not approximated: Go's `@@auto_increment_increment` /
/// `@@auto_increment_offset` make the ids an arithmetic progression (captured
/// 2, 5, 8 for increment 3 offset 2). This allocator hands out consecutive
/// ids only, so an insert into a table with an auto column FAILS while either
/// variable is off its default rather than answering the wrong ids. A table
/// with no auto column is unaffected.
#[test]
fn a_non_default_auto_increment_step_is_refused_rather_than_ignored() {
    let mut session = table(None);
    session.run("SET @@auto_increment_increment = 3").unwrap();
    assert!(session.run("INSERT INTO ai (v) VALUES (1)").is_err());
    session.run("SET @@auto_increment_increment = 1").unwrap();
    session.run("SET @@auto_increment_offset = 2").unwrap();
    assert!(session.run("INSERT INTO ai (v) VALUES (1)").is_err());
    session.run("SET @@auto_increment_offset = 1").unwrap();
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    assert_eq!(one(&mut session, "SELECT id FROM ai"), "1");
}

/// REFUSED: `FORCE AUTO_INCREMENT`, the TiDB extension that lets the counter
/// move DOWN, is rejected instead of being read as the plain form.
#[test]
fn force_auto_increment_is_refused() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1),(2)").unwrap();
    assert!(session
        .run("ALTER TABLE ai FORCE AUTO_INCREMENT = 1")
        .is_err());
}
