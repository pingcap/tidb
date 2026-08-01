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

/// The signed domain ENDS in an error, never in a wrapped or repeated id.
///
/// Captured on `BIGINT` (signed): after `INSERT ... VALUES
/// (9223372036854775807)` the next allocation is
/// `[autoid:1467]Failed to read auto-increment value from storage engine`,
/// and the same at `9223372036854775806` -- Go's `alloc4Signed` refuses while
/// `math.MaxInt64 - base <= 1`, so the counter never reaches the type's end.
#[test]
fn the_signed_allocator_reports_1467_instead_of_overflowing() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE big (id BIGINT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .unwrap();
    session
        .run("INSERT INTO big VALUES (9223372036854775807, 1)")
        .unwrap();
    let error = session
        .run("INSERT INTO big (v) VALUES (2)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1467);
    assert_eq!(
        error.message,
        "Failed to read auto-increment value from storage engine"
    );
    // The explicit row is still the only one: a refused allocation writes
    // nothing rather than a duplicate of the id already there.
    assert_eq!(
        one(&mut session, "SELECT id FROM big"),
        "9223372036854775807"
    );
}

/// A `BIGINT UNSIGNED` allocator crosses `i64::MAX` into its OWN domain: the
/// id after an explicit `9223372036854775807` is `9223372036854775808`.
///
/// Captured on `BIGINT UNSIGNED`, and this is the whole of the signed/unsigned
/// split -- reading the explicit id as an `i64` made the rebase see a value the
/// counter was already past, so the next insert took a LOW id on top of rows
/// that already existed. (The allocator's behavior at the unsigned domain's own
/// end is covered in `tidb-executor`'s `kv_table` tests, because a literal
/// above `i64::MAX` is not yet expressible in this tier's SQL.)
#[test]
fn an_unsigned_id_above_the_signed_maximum_rebases_the_allocator() {
    let mut session = table(None);
    session
        .run("INSERT INTO ai VALUES (9223372036854775807, 1)")
        .unwrap();
    session.run("INSERT INTO ai (v) VALUES (2)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["9223372036854775807", "1"], ["9223372036854775808", "2"]]
    );
}

/// An auto id must fit the COLUMN, not just the counter: the id AT a narrow
/// type's maximum is written, and the next one is refused with `1690`.
///
/// Captured through `gorun` on `CREATE TABLE t(id <type> PRIMARY KEY
/// AUTO_INCREMENT, n INT)` seeded one below each type's maximum: every width
/// and both signednesses accept the id that equals the maximum and refuse the
/// one past it, with the refused row absent from the table afterwards. The
/// error is Go's `types.overflow` from `setDatumAutoIDAndCast`, `[types:1690]
/// constant <id> overflows <type>`, and NOT the allocator's `1467` -- ids
/// remain in the 64-bit domain the allocator counts in, this column just
/// cannot hold them.
///
/// BOTH directions are pinned on purpose. A bound that fires one id early
/// turns working inserts into errors, which is worse than the missing check
/// it replaces, and only the at-maximum half of each pair can see that.
#[test]
fn an_auto_id_past_the_column_type_is_refused_while_the_maximum_is_accepted() {
    // (type, one below the maximum, the maximum, the id past it)
    let widths = [
        ("TINYINT", "126", "127", "128", "tinyint"),
        ("TINYINT UNSIGNED", "254", "255", "256", "tinyint"),
        ("SMALLINT", "32766", "32767", "32768", "smallint"),
        ("SMALLINT UNSIGNED", "65534", "65535", "65536", "smallint"),
        ("MEDIUMINT", "8388606", "8388607", "8388608", "mediumint"),
        (
            "MEDIUMINT UNSIGNED",
            "16777214",
            "16777215",
            "16777216",
            "mediumint",
        ),
        ("INT", "2147483646", "2147483647", "2147483648", "int"),
        (
            "INT UNSIGNED",
            "4294967294",
            "4294967295",
            "4294967296",
            "int",
        ),
    ];
    for (column_type, seed, maximum, past, type_name) in widths {
        let mut session = Session::new();
        session
            .run(&format!(
                "CREATE TABLE t (id {column_type} AUTO_INCREMENT PRIMARY KEY, n INT)"
            ))
            .unwrap();
        session
            .run(&format!("INSERT INTO t VALUES ({seed}, 1)"))
            .unwrap();
        // The id AT the maximum is a plain accepted insert.
        session.run("INSERT INTO t (n) VALUES (2)").unwrap();
        assert_eq!(
            rows(&mut session, "SELECT id FROM t ORDER BY id"),
            [[seed], [maximum]],
            "{column_type}: the id at the type maximum must be written"
        );
        // The next one does not fit the column.
        let error = session
            .run("INSERT INTO t (n) VALUES (3)")
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(error.code, 1690, "{column_type}");
        assert_eq!(
            error.message,
            format!("constant {past} overflows {type_name}"),
            "{column_type}"
        );
        // A refused allocation writes nothing.
        assert_eq!(
            rows(&mut session, "SELECT id FROM t ORDER BY id"),
            [[seed], [maximum]],
            "{column_type}: the refused row must be absent"
        );
    }
}

/// The unsigned case whose maximum is above `i64::MAX`: a `BIGINT UNSIGNED`
/// column reaches ids no signed intermediate can express, so the column bound
/// must be read as a 64-bit PATTERN or it truncates and refuses ids the
/// column holds.
///
/// Captured: seeded at `18446744073709551613`, the next insert takes
/// `18446744073709551614` and the one after is refused. At this width the
/// column bound IS the domain end, so the refusal comes from the allocator's
/// own rule (`1467`, one id below `u64::MAX`) and never from the cast --
/// which is exactly what says the cast did not fire early here.
#[test]
fn a_bigint_unsigned_column_keeps_its_ids_above_the_signed_maximum() {
    let mut session = table(None);
    session
        .run("INSERT INTO ai VALUES (18446744073709551613, 1)")
        .unwrap();
    session.run("INSERT INTO ai (v) VALUES (2)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [
            ["18446744073709551613", "1"],
            ["18446744073709551614", "2"]
        ]
    );
    let error = session
        .run("INSERT INTO ai (v) VALUES (3)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(error.code, 1467);
}

/// REFUSED, not silently ignored: under `NO_AUTO_VALUE_ON_ZERO` Go STORES an
/// explicit `0` (captured: the row is `0` and the next insert gets `1`), while
/// this tier allocates over it. Writing a different row than Go writes is
/// worse than failing, so the insert fails while the mode is on.
#[test]
fn the_no_auto_value_on_zero_sql_mode_is_refused_rather_than_ignored() {
    let mut session = table(None);
    session
        .run("SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO'")
        .unwrap();
    assert!(session.run("INSERT INTO ai VALUES (0, 1)").is_err());
    session.run("SET SESSION sql_mode = ''").unwrap();
    // With the mode off, a zero allocates as it always did.
    session.run("INSERT INTO ai VALUES (0, 1)").unwrap();
    assert_eq!(one(&mut session, "SELECT id FROM ai"), "1");
}

/// The OK packet's insert id and `LAST_INSERT_ID()` come off the SAME
/// publication, so they can only differ where Go itself differs: the wire
/// falls back to the statement's explicit auto value (Go `StmtCtx.InsertID`)
/// and the function never follows one.
///
/// Captured from TiDB, `session.LastInsertID()` (the field `writeOkWith`
/// sends) beside `SELECT LAST_INSERT_ID()`: allocating insert 1/1, explicit
/// `VALUES (50,2)` 50/1, `UPDATE` 0/1, an `INSERT IGNORE` whose only row is a
/// duplicate 0/1 -- it BURNS an id but reports none.
#[test]
fn the_ok_packets_insert_id_follows_gos_two_fallbacks() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    assert_eq!(session.statement_insert_id(), 1);
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "1");

    session.run("INSERT INTO ai (id,v) VALUES (50,2)").unwrap();
    assert_eq!(
        session.statement_insert_id(),
        50,
        "the wire reports the explicit value"
    );
    assert_eq!(
        one(&mut session, "SELECT last_insert_id()"),
        "1",
        "the function does not follow it"
    );

    session.run("UPDATE ai SET v = 3 WHERE id = 50").unwrap();
    assert_eq!(session.statement_insert_id(), 0);
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "1");

    // The burned-id case: the row is skipped, so nothing is published and the
    // wire reports 0 even though the allocator moved.
    session.run("INSERT IGNORE INTO ai (v) VALUES (3)").unwrap();
    assert_eq!(
        session.statement_insert_id(),
        0,
        "an id burned by a skipped row never reaches the wire"
    );
    assert_eq!(one(&mut session, "SELECT last_insert_id()"), "1");
}

/// DDL implicitly COMMITS the open transaction before it runs, so a TRUNCATE
/// inside `BEGIN` is durable and the counter it resets is the SURVIVING
/// table's, not a working copy's.
///
/// Captured from TiDB over `INSERT (committed); BEGIN; INSERT; TRUNCATE;
/// ROLLBACK`: the table is EMPTY afterwards -- the ROLLBACK takes nothing
/// back, because the TRUNCATE committed what preceded it -- and the next
/// insert gets id 1.
#[test]
fn truncate_inside_a_transaction_implicitly_commits_it() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO ai (v) VALUES (2)").unwrap();
    session.run("TRUNCATE TABLE ai").unwrap();
    session.run("ROLLBACK").unwrap();
    assert!(
        rows(&mut session, "SELECT id, v FROM ai").is_empty(),
        "the TRUNCATE committed, so the ROLLBACK restores nothing"
    );
    session.run("INSERT INTO ai (v) VALUES (3)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai"),
        [["1", "3"]],
        "the counter the TRUNCATE reset is the one the next insert allocates from"
    );
}

/// The same implicit commit on the `ALTER TABLE ... AUTO_INCREMENT` path,
/// where the discriminator is the row rather than the counter. Captured: the
/// row inserted inside the transaction SURVIVES the ROLLBACK.
#[test]
fn alter_auto_increment_inside_a_transaction_implicitly_commits_it() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    session.run("BEGIN").unwrap();
    session.run("INSERT INTO ai (v) VALUES (2)").unwrap();
    session.run("ALTER TABLE ai AUTO_INCREMENT=100").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY id"),
        [["1", "1"], ["2", "2"]],
        "the ALTER committed the row staged before it"
    );
}

/// Assigning to the `AUTO_INCREMENT` column through UPDATE rebases the
/// allocator, so later rows land PAST the value the UPDATE named -- Go's
/// `updateRecord` calls the same `Rebase` an explicit INSERT value does.
///
/// The rebase only moves the counter UP, which is why `SET id = 0` changes
/// nothing and the run continues from where the 300 left it; and it reads the
/// value in the auto column's own domain, which is why the same
/// `18446744073709551615` on a SIGNED column is a negative base that moves
/// nothing at all.
///
/// Captured from real TiDB via `rust/difftests/gorun`, on
/// `(id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, v INT UNIQUE)`:
///
/// | after | `SELECT id, v ORDER BY v` |
/// | --- | --- |
/// | insert v=1; `SET id = 300`; insert v=2 | `300|1`, `301|2` |
/// | `SET id = 0` where v=2; insert v=3 | `300|1`, `0|2`, `302|3` |
/// | `SET id = 5` where v=3; insert v=4 | `300|1`, `0|2`, `5|3`, `303|4` |
/// | `SET v = 40` where v=4; insert v=5 | ..., `304|5`, `303|40` |
///
/// The signed-column half of that capture -- insert v=1; `SET id =
/// 18446744073709551615`; insert v=2 gives `-1|1`, `2|2` in Go -- is NOT
/// asserted here, because this tier rejects the assignment with
/// `DataOutOfRange` before the rebase is ever reached. That is a separate,
/// unclaimed divergence in UPDATE's range check, not in the rebase; the
/// signed/unsigned domain split is covered by
/// `an_auto_increment_option_above_i64_max_seeds_create_but_rebases_alter`.
#[test]
fn assigning_the_auto_increment_column_through_update_rebases_the_allocator() {
    let mut session = table(None);
    session.run("INSERT INTO ai (v) VALUES (1)").unwrap();
    session.run("UPDATE ai SET id = 300 WHERE v = 1").unwrap();
    session.run("INSERT INTO ai (v) VALUES (2)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY v"),
        [["300", "1"], ["301", "2"]],
        "the UPDATE moved the counter to 300, so the next row is 301"
    );

    // A rebase never moves DOWN, so zero leaves the counter at 301.
    session.run("UPDATE ai SET id = 0 WHERE v = 2").unwrap();
    session.run("INSERT INTO ai (v) VALUES (3)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY v"),
        [["300", "1"], ["0", "2"], ["302", "3"]]
    );

    // Nor to a value the counter is already past.
    session.run("UPDATE ai SET id = 5 WHERE v = 3").unwrap();
    session.run("INSERT INTO ai (v) VALUES (4)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY v"),
        [["300", "1"], ["0", "2"], ["5", "3"], ["303", "4"]]
    );

    // An UPDATE that leaves the auto column alone burns nothing.
    session.run("UPDATE ai SET v = 40 WHERE v = 4").unwrap();
    session.run("INSERT INTO ai (v) VALUES (5)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT id, v FROM ai ORDER BY v"),
        [
            ["300", "1"],
            ["0", "2"],
            ["5", "3"],
            ["304", "5"],
            ["303", "40"],
        ]
    );
}

/// CREATE reads the `AUTO_INCREMENT=` option in the SIGNED domain and ALTER
/// reads it in the auto column's OWN domain -- Go's two paths genuinely
/// disagree, and the split is not a wart to be smoothed over.
///
/// `handleAutoIncID` seeds only when `tbInfo.AutoIncID > 1`, and that field is
/// `int64(opt.UintValue)`, so every value above `i64::MAX` is negative there
/// and seeds NOTHING. `RebaseAutoID` instead sends the same pattern through
/// `adjustNewBaseToNextGlobalID`, which compares in the column's domain, so a
/// `BIGINT UNSIGNED` counter really does move to the top of its range.
///
/// Captured from real TiDB via `rust/difftests/gorun`:
///
/// | statement | Go |
/// | --- | --- |
/// | `CREATE ... UNSIGNED ... AUTO_INCREMENT=18446744073709551615`; insert; select | `1` |
/// | `CREATE ... UNSIGNED ... AUTO_INCREMENT=9223372036854775808`; insert; select | `1` |
/// | `CREATE ... UNSIGNED`; `ALTER ... AUTO_INCREMENT=18446744073709551615`; insert; select | `18446744073709551615` |
/// | `CREATE ... UNSIGNED`; `ALTER ... AUTO_INCREMENT=9223372036854775808`; insert; select | `9223372036854775808` |
/// | `CREATE ... BIGINT` signed; `ALTER ... AUTO_INCREMENT=18446744073709551615`; insert; select | `1` |
///
/// The ALTER-to-`18446744073709551615` row is deliberately NOT asserted: it
/// lands on the top-of-domain id that `AutoIdAllocator::alloc` documents
/// itself as refusing one step early, a divergence that predates this test and
/// belongs to Go's batch cache rather than to the option's domain.
#[test]
fn an_auto_increment_option_above_i64_max_seeds_create_but_rebases_alter() {
    for option in ["18446744073709551615", "9223372036854775808"] {
        let mut session = Session::new();
        session
            .run(&format!(
                "CREATE TABLE big (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY) \
                 AUTO_INCREMENT={option}"
            ))
            .unwrap_or_else(|e| panic!("AUTO_INCREMENT={option} must be accepted, got {e:?}"));
        session.run("INSERT INTO big VALUES ()").unwrap();
        assert_eq!(
            one(&mut session, "SELECT id FROM big"),
            "1",
            "CREATE compares AUTO_INCREMENT={option} as a signed int64, so it seeds nothing"
        );
    }

    // The same pattern through ALTER moves the unsigned counter up instead.
    let mut session = Session::new();
    session
        .run("CREATE TABLE big (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY)")
        .unwrap();
    session
        .run("ALTER TABLE big AUTO_INCREMENT=9223372036854775808")
        .unwrap();
    session.run("INSERT INTO big VALUES ()").unwrap();
    assert_eq!(
        one(&mut session, "SELECT id FROM big"),
        "9223372036854775808",
        "ALTER rebases in the column's unsigned domain, where CREATE would not have"
    );

    // On a SIGNED column the pattern is a negative base, so ALTER moves nothing.
    let mut session = Session::new();
    session
        .run("CREATE TABLE sgn (id BIGINT AUTO_INCREMENT PRIMARY KEY)")
        .unwrap();
    session
        .run("ALTER TABLE sgn AUTO_INCREMENT=18446744073709551615")
        .unwrap();
    session.run("INSERT INTO sgn VALUES ()").unwrap();
    assert_eq!(one(&mut session, "SELECT id FROM sgn"), "1");
}
