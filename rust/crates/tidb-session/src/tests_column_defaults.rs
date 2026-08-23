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

//! A column `DEFAULT` that is COMPUTED rather than settled, end to end over a
//! real session: what it stores, what `SHOW CREATE TABLE` prints back, and
//! what an omitted column takes on `INSERT`.
//!
//! Every expectation here was CAPTURED from real TiDB through
//! `rust/difftests/gorun` before it was written down; the capture script is
//! reproduced statement by statement in the tests below, so a reader can see
//! which Go answer each assertion quotes. The captured `SHOW CREATE TABLE`
//! bodies were, verbatim:
//!
//! ```text
//! create table t1 (a int, b timestamp default current_timestamp)
//!   `b` timestamp DEFAULT CURRENT_TIMESTAMP
//! create table t3 (a int, b timestamp default now())
//!   `b` timestamp DEFAULT CURRENT_TIMESTAMP      -- `now()` stores the marker
//! create table t7 (a int, b timestamp(3) default current_timestamp(3))
//!   `b` timestamp(3) DEFAULT CURRENT_TIMESTAMP(3)
//! create table t4 (a int, b varchar(64) default (uuid()))
//!   `b` varchar(64) DEFAULT (uuid())          -- accepted by Go; see below
//! create table t5 (a int, b double default (rand()))
//!   `b` double DEFAULT (rand())
//! create table t9 (a int, b int default (1+1))
//!   `b` int(11) DEFAULT '2'                      -- folded, and QUOTED
//! create table t8 (a int, b varchar(10) default (upper('ab')))
//!   ERR                                          -- not on Go's whitelist
//! ```
//!
//! Those bodies are `gorun`'s output verbatim, and `int(11)` is the one place
//! they do NOT describe a running node: `deprecate-integer-display-length`
//! defaults to true and only `cmd/tidb-server/main.go` applies it, so a real
//! server prints `int` there. The assertions below quote the DEFAULT, which
//! the switch does not touch.
//!
//! Mirrors Go `pkg/ddl/add_column.go` (`SetDefaultValue`, `getDefaultValue`,
//! `getFuncCallDefaultValue`), `pkg/table/column.go` (`GetColDefaultValue`,
//! `NewColDesc`) and `pkg/executor/show.go`'s default printer.

#![cfg(test)]

use crate::tests_support::row_text;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

#[test]
fn on_update_current_timestamp_tracks_real_row_changes() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    session.run("SET timestamp = 1700000000").unwrap();
    session
        .run(
            "CREATE TABLE on_update_clock (\
             id INT PRIMARY KEY, v INT, \
             changed_at TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP)",
        )
        .unwrap();
    session
        .run("INSERT INTO on_update_clock (id, v) VALUES (1, 1)")
        .unwrap();
    let definition = show_create(&mut session, "on_update_clock");
    assert!(
        definition.contains("`changed_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP"),
        "{definition}"
    );
    let changed_at = rows(&mut session, "SHOW COLUMNS FROM on_update_clock")
        .into_iter()
        .find(|column| column[0] == "changed_at")
        .unwrap();
    assert_eq!(
        changed_at[5],
        "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
    );
    assert_eq!(
        rows(&mut session, "SELECT changed_at FROM on_update_clock"),
        [["NULL"]]
    );

    session.run("SET timestamp = 1700000100").unwrap();
    assert_eq!(
        session.run("UPDATE on_update_clock SET v = 2").unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        rows(&mut session, "SELECT changed_at FROM on_update_clock"),
        [["2023-11-14 22:15:00"]]
    );

    session.run("SET timestamp = 1700000200").unwrap();
    assert_eq!(
        session.run("UPDATE on_update_clock SET v = v").unwrap(),
        StmtResult::Affected(0)
    );
    assert_eq!(
        rows(&mut session, "SELECT changed_at FROM on_update_clock"),
        [["2023-11-14 22:15:00"]],
        "a no-op update advanced the implicit clock"
    );

    session.run("SET timestamp = 1700000300").unwrap();
    assert_eq!(
        session
            .run("UPDATE on_update_clock SET v = 3, changed_at = changed_at")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        rows(&mut session, "SELECT changed_at FROM on_update_clock"),
        [["2023-11-14 22:15:00"]],
        "an explicitly assigned on-update column was overwritten"
    );

    session.run("SET timestamp = 1700000400").unwrap();
    assert_eq!(
        session
            .run(
                "INSERT INTO on_update_clock (id, v) VALUES (1, 4) \
                 ON DUPLICATE KEY UPDATE v = VALUES(v)",
            )
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        rows(&mut session, "SELECT changed_at FROM on_update_clock"),
        [["2023-11-14 22:20:00"]]
    );

    session.run("SET timestamp = 1700000500").unwrap();
    assert_eq!(
        session
            .run(
                "INSERT INTO on_update_clock (id, v) VALUES (1, 4) \
                 ON DUPLICATE KEY UPDATE v = VALUES(v)",
            )
            .unwrap(),
        StmtResult::Affected(0)
    );
    assert_eq!(
        rows(&mut session, "SELECT changed_at FROM on_update_clock"),
        [["2023-11-14 22:20:00"]]
    );

    session
        .run("CREATE TABLE on_update_peer (id INT PRIMARY KEY, delta INT)")
        .unwrap();
    session
        .run("INSERT INTO on_update_peer VALUES (1, 1)")
        .unwrap();
    session.run("SET timestamp = 1700000600").unwrap();
    assert_eq!(
        session
            .run(
                "UPDATE on_update_clock c JOIN on_update_peer p ON c.id = p.id \
                 SET c.v = c.v + p.delta",
            )
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        rows(&mut session, "SELECT v, changed_at FROM on_update_clock"),
        [["5", "2023-11-14 22:23:20"]]
    );
}

#[test]
fn invalid_on_update_clauses_keep_tidbs_error_boundary() {
    let mut session = Session::new();
    for sql in [
        "CREATE TABLE bad_on_update_type (v INT ON UPDATE CURRENT_TIMESTAMP)",
        "CREATE TABLE bad_on_update_fsp (v TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP)",
    ] {
        assert_eq!(code(&mut session, sql), Some(1294), "{sql}");
    }

    session
        .run(
            "CREATE TABLE valid_on_update_fsp (\
             id INT PRIMARY KEY, v INT, \
             changed_at DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) \
             ON UPDATE CURRENT_TIMESTAMP(3))",
        )
        .unwrap();
    let definition = show_create(&mut session, "valid_on_update_fsp");
    assert!(
        definition.contains(
            "`changed_at` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) \
             ON UPDATE CURRENT_TIMESTAMP(3)"
        ),
        "{definition}"
    );
}

#[test]
fn alter_column_options_preserve_computed_default_and_on_update_semantics() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    session
        .run(
            "CREATE TABLE alter_clock (\
             id INT PRIMARY KEY, v INT, changed_at DATETIME(3) NULL, \
             token VARCHAR(64))",
        )
        .unwrap();
    session
        .run("INSERT INTO alter_clock (id, v, changed_at) VALUES (1, 1, NULL)")
        .unwrap();

    session.run("SET timestamp = 1700000000").unwrap();
    session
        .run(
            "ALTER TABLE alter_clock ADD COLUMN added_at DATETIME(3) \
             DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)",
        )
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT changed_at, added_at FROM alter_clock WHERE id = 1"
        ),
        [["NULL", "2023-11-14 22:13:20.000"]]
    );
    let definition = show_create(&mut session, "alter_clock");
    assert!(
        definition.contains(
            "`added_at` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) \
             ON UPDATE CURRENT_TIMESTAMP(3)"
        ),
        "{definition}"
    );

    session.run("SET timestamp = 1700000050").unwrap();
    session
        .run("INSERT INTO alter_clock (id, v) VALUES (2, 1)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT changed_at, added_at FROM alter_clock WHERE id = 2"
        ),
        [["NULL", "2023-11-14 22:14:10.000"]],
        "the ADD default was settled instead of retained as a computation"
    );

    session
        .run(
            "ALTER TABLE alter_clock MODIFY COLUMN changed_at DATETIME(3) NULL \
             DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)",
        )
        .unwrap();
    session.run("SET timestamp = 1700000100").unwrap();
    assert_eq!(
        session
            .run("UPDATE alter_clock SET v = 2 WHERE id = 1")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT changed_at, added_at FROM alter_clock WHERE id = 1"
        ),
        [["2023-11-14 22:15:00.000", "2023-11-14 22:15:00.000"]]
    );

    session.run("SET timestamp = 1700000200").unwrap();
    session
        .run("INSERT INTO alter_clock (id, v) VALUES (3, 1)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT changed_at, added_at FROM alter_clock WHERE id = 3"
        ),
        [["2023-11-14 22:16:40.000", "2023-11-14 22:16:40.000"]],
        "the MODIFY default was settled instead of retained as a computation"
    );

    session
        .run(
            "ALTER TABLE alter_clock \
             ADD COLUMN order_cleared TIMESTAMP ON UPDATE CURRENT_TIMESTAMP \
             DEFAULT CURRENT_TIMESTAMP, \
             ADD COLUMN order_kept TIMESTAMP DEFAULT CURRENT_TIMESTAMP \
             ON UPDATE CURRENT_TIMESTAMP, \
             ADD COLUMN null_cleared TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NULL",
        )
        .unwrap();
    let definition = show_create(&mut session, "alter_clock");
    let order_cleared = definition
        .lines()
        .find(|line| line.contains("`order_cleared`"))
        .unwrap();
    let order_kept = definition
        .lines()
        .find(|line| line.contains("`order_kept`"))
        .unwrap();
    let null_cleared = definition
        .lines()
        .find(|line| line.contains("`null_cleared`"))
        .unwrap();
    assert!(!order_cleared.contains("ON UPDATE"), "{order_cleared}");
    assert!(order_kept.contains("ON UPDATE"), "{order_kept}");
    assert!(!null_cleared.contains("ON UPDATE"), "{null_cleared}");

    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE alter_clock ADD COLUMN unsafe_token VARCHAR(64) DEFAULT (uuid())"
        ),
        Some(1674)
    );
    assert!(!show_create(&mut session, "alter_clock").contains("`unsafe_token`"));
    session
        .run("ALTER TABLE alter_clock MODIFY COLUMN token VARCHAR(64) DEFAULT (uuid())")
        .unwrap();
    session
        .run("INSERT INTO alter_clock (id, v) VALUES (4, 1)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT length(token) FROM alter_clock WHERE id = 4"
        ),
        [["36"]],
        "MODIFY incorrectly inherited ADD COLUMN's unsafe-origin refusal"
    );

    for sql in [
        "ALTER TABLE alter_clock ADD COLUMN bad_type INT ON UPDATE CURRENT_TIMESTAMP",
        "ALTER TABLE alter_clock MODIFY COLUMN changed_at DATETIME(3) \
         ON UPDATE CURRENT_TIMESTAMP",
    ] {
        assert_eq!(code(&mut session, sql), Some(1294), "{sql}");
    }
}

/// The body of `SHOW CREATE TABLE t`, which is its second cell.
fn show_create(session: &mut Session, table: &str) -> String {
    rows(session, &format!("SHOW CREATE TABLE {table}")).remove(0)[1].clone()
}

/// The error code a statement fails with, or `None` when it succeeded.
fn code(session: &mut Session, sql: &str) -> Option<u16> {
    match session.run(sql) {
        Ok(_) => None,
        Err(error) => Some(error.to_mysql_error().code),
    }
}

/// `DEFAULT CURRENT_TIMESTAMP` stores Go's marker word, and every spelling of
/// the clock -- `current_timestamp`, `now()` -- stores the SAME one, so all of
/// them print back identically.
#[test]
fn current_timestamp_default_prints_as_the_marker() {
    let mut session = Session::new();
    for (table, written) in [
        ("t1", "b TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ("t2", "b DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ("t3", "b TIMESTAMP DEFAULT now()"),
    ] {
        session
            .run(&format!("CREATE TABLE {table} (a INT, {written})"))
            .unwrap();
        assert!(
            show_create(&mut session, table).contains("DEFAULT CURRENT_TIMESTAMP"),
            "{table} declared `{written}`: {}",
            show_create(&mut session, table)
        );
    }
}

/// Go `getFuncCallDefaultValue`'s whole rule for the clock marker on a
/// `TIMESTAMP`/`DATETIME` column: the fsp WRITTEN on the default -- 0 when it
/// is written bare -- must EQUAL the column's own fsp, and `ErrInvalidDefault`
/// (1067) is the answer when it does not. So the two spellings are not
/// interchangeable on a column that has an fsp: `DATETIME(3)` demands
/// `CURRENT_TIMESTAMP(3)` and refuses the bare word.
///
/// Captured from real TiDB, statement by statement:
///
/// ```text
/// create table a10 (ts timestamp(3) default current_timestamp(3))  OK
/// create table a3  (ts datetime(3)  default now(3))                OK
/// create table a1  (ts datetime(3)  default current_timestamp)     ERR
/// create table a2  (ts datetime     default current_timestamp(3))  ERR
/// create table a6  (ts datetime(3)  default current_timestamp(2))  ERR
/// ```
#[test]
fn the_clock_defaults_fsp_must_equal_the_columns_own() {
    let mut session = Session::new();
    for (table, written) in [
        ("t7", "b TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)"),
        ("t7b", "b DATETIME(3) DEFAULT now(3)"),
        ("t7c", "b TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
    ] {
        session
            .run(&format!("CREATE TABLE {table} (a INT, {written})"))
            .unwrap_or_else(|error| panic!("{written}: {error:?}"));
    }
    for written in [
        "b DATETIME(3) DEFAULT CURRENT_TIMESTAMP",
        "b DATETIME DEFAULT CURRENT_TIMESTAMP(3)",
        "b DATETIME(3) DEFAULT CURRENT_TIMESTAMP(2)",
        "b TIMESTAMP(6) DEFAULT now()",
    ] {
        assert_eq!(
            code(
                &mut session,
                &format!("CREATE TABLE bad (a INT, {written})")
            ),
            Some(1067),
            "{written}"
        );
    }
}

/// The fsp travels from the declared type through the stored default and into
/// the WRITE: an omitted `DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)` column
/// stores a clock reading with three fractional digits, not a whole second.
///
/// Captured from real TiDB:
///
/// ```text
/// create table t72 (id int primary key, ts datetime(3) default current_timestamp(3),
///                   d datetime(6) default current_timestamp(6),
///                   z datetime default current_timestamp)
/// insert into t72 (id) values (1)
/// select id, ts, d, z from t72
///   1|2026-08-01 00:40:17.093|2026-08-01 00:40:17.093391|2026-08-01 00:40:17
/// select length(ts), length(d), length(z) from t72
///   23|26|19
/// ```
#[test]
fn the_columns_fsp_reaches_the_value_a_clock_default_writes() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t72 (id INT PRIMARY KEY, \
             ts DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3), \
             d DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6), \
             z DATETIME DEFAULT CURRENT_TIMESTAMP)",
        )
        .unwrap();
    session.run("INSERT INTO t72 (id) VALUES (1)").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT LENGTH(ts), LENGTH(d), LENGTH(z) FROM t72"
        ),
        vec![vec!["23".to_owned(), "26".to_owned(), "19".to_owned()]]
    );
}

/// `SHOW CREATE TABLE` prints the marker with the column's fsp appended, and
/// `now(3)` prints back as `CURRENT_TIMESTAMP(3)` because Go stores the marker
/// word rather than the written spelling. Captured:
/// `` `ts` datetime(3) DEFAULT CURRENT_TIMESTAMP(3) ``.
#[test]
fn a_clock_default_with_an_fsp_prints_the_marker_with_it() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE a3 (ts DATETIME(3) DEFAULT now(3))")
        .unwrap();
    assert!(
        show_create(&mut session, "a3").contains("`ts` datetime(3) DEFAULT CURRENT_TIMESTAMP(3)"),
        "{}",
        show_create(&mut session, "a3")
    );
}

/// An omitted `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` column takes a clock
/// reading, not NULL: `insert into t1 (a) values (1)` then
/// `select a, b is not null from t1` captured `1|1`.
#[test]
fn an_omitted_clock_default_reads_the_clock() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (a INT, b TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        .unwrap();
    session.run("INSERT INTO t1 (a) VALUES (1)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a, b IS NOT NULL FROM t1"),
        vec![vec!["1".to_owned(), "1".to_owned()]]
    );
}

/// A `DefaultIsExpr` default prints PARENTHESISED and unquoted, which is the
/// one visible difference from a literal default.
#[test]
fn an_expression_default_prints_parenthesised() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t5 (a INT, b DOUBLE DEFAULT (rand()))")
        .unwrap();
    assert!(
        show_create(&mut session, "t5").contains("DEFAULT (rand())"),
        "{}",
        show_create(&mut session, "t5")
    );
}

/// `DEFAULT (uuid())` is on Go's whitelist, prints as an expression, and is
/// evaluated independently for every omitted row.
#[test]
fn a_uuid_default_is_evaluated_per_omitted_row() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t4 (a INT, b VARCHAR(64) DEFAULT (uuid()))")
        .unwrap();
    assert!(
        show_create(&mut session, "t4").contains("DEFAULT (uuid())"),
        "{}",
        show_create(&mut session, "t4")
    );
    session.run("INSERT INTO t4 (a) VALUES (1), (2)").unwrap();
    let values = rows(&mut session, "SELECT b FROM t4 ORDER BY a");
    assert_eq!(values.len(), 2);
    for value in &values {
        let value = &value[0];
        assert_eq!(value.len(), 36, "{value}");
        for at in [8, 13, 18, 23] {
            assert_eq!(value.as_bytes()[at], b'-', "{value}");
        }
    }
    assert_ne!(values[0][0], values[1][0]);
}

/// An omitted expression-default column is evaluated per row: `insert into t4
/// (a) values (1)` then `select a, length(b) from t4` captured `1|36`, the
/// length of a UUID.
#[test]
fn an_omitted_expression_default_is_evaluated() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t5 (a INT, b DOUBLE DEFAULT (rand()))")
        .unwrap();
    session.run("INSERT INTO t5 (a) VALUES (1)").unwrap();
    // `rand()` is in [0, 1): the assertion is that a value was COMPUTED for
    // the omitted column rather than the NULL a missing default would give.
    assert_eq!(
        rows(&mut session, "SELECT a, b >= 0 AND b < 1 FROM t5"),
        vec![vec!["1".to_owned(), "1".to_owned()]]
    );
}

/// `DEFAULT (1+1)` is NOT a function call, so Go folds it at DDL time and
/// stores `2` -- printed QUOTED, like every other literal default, and read
/// back as the integer 2.
#[test]
fn a_folded_default_stays_a_settled_literal() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t9 (a INT, b INT DEFAULT (1+1))")
        .unwrap();
    assert!(
        show_create(&mut session, "t9").contains("DEFAULT '2'"),
        "{}",
        show_create(&mut session, "t9")
    );
    session.run("INSERT INTO t9 (a) VALUES (1)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT * FROM t9"),
        vec![vec!["1".to_owned(), "2".to_owned()]]
    );
}

/// The whitelist is the whole rule: a function that is not on it is refused
/// even when it FOLDS to a constant. Captured from TiDB, `create table t8 (a
/// int, b varchar(10) default (upper('ab')))` is an error -- `upper` is
/// accepted only as `UPPER(SUBSTRING_INDEX(USER(), '@', 1))`.
#[test]
fn a_function_off_the_whitelist_is_refused_even_when_constant() {
    let mut session = Session::new();
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE t8 (a INT, b VARCHAR(10) DEFAULT (upper('ab')))"
        ),
        Some(3770)
    );
}

/// `SHOW COLUMNS` reports the STORED string, which for an expression default
/// is the text WITHOUT the parentheses `SHOW CREATE TABLE` adds -- Go
/// `NewColDesc` and `pkg/executor/show.go` read the same field and render it
/// differently, so both renderings are asserted here together.
#[test]
fn show_columns_reports_the_stored_default_text() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t5 (a INT, b DOUBLE DEFAULT (rand()))")
        .unwrap();
    session
        .run("CREATE TABLE t1 (a INT, b TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        .unwrap();
    let expression = rows(&mut session, "SHOW COLUMNS FROM t5");
    assert_eq!(expression[1][4], "rand()");
    assert_eq!(
        rows(
            &mut session,
            "SELECT column_default, extra FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 't5' AND column_name = 'b'",
        ),
        [["rand()", "DEFAULT_GENERATED"]]
    );
    let clock = rows(&mut session, "SHOW COLUMNS FROM t1");
    assert_eq!(clock[1][4], "CURRENT_TIMESTAMP");
}

/// A literal TIMESTAMP default is stored as a UTC wall clock in version-1+
/// metadata, but every metadata display reads it through
/// `GetColDefaultValue` and therefore prints it in the consuming session's
/// zone. DATETIME is the control: its stored spelling is zone-free and must
/// not move. Captured from TiDB:
///
/// ```text
/// set time_zone = '+00:00';
/// create table td (ts timestamp default '2020-01-02 00:00:00',
///                  dt datetime default '2020-01-02 00:00:00');
/// set time_zone = '+08:00';
/// show columns from td;
///   ts ... 2020-01-02 08:00:00
///   dt ... 2020-01-02 00:00:00
/// ```
#[test]
fn literal_timestamp_defaults_print_in_the_consuming_session_zone() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    session
        .run(
            "CREATE TABLE td (ts TIMESTAMP DEFAULT '2020-01-02 00:00:00', \
             dt DATETIME DEFAULT '2020-01-02 00:00:00')",
        )
        .unwrap();

    session.run("SET time_zone = '+08:00'").unwrap();
    let columns = rows(&mut session, "SHOW COLUMNS FROM td");
    assert_eq!(columns[0][4], "2020-01-02 08:00:00");
    assert_eq!(columns[1][4], "2020-01-02 00:00:00");
    let create = show_create(&mut session, "td");
    assert!(
        create.contains("`ts` timestamp DEFAULT '2020-01-02 08:00:00'"),
        "{create}"
    );
    assert!(
        create.contains("`dt` datetime DEFAULT '2020-01-02 00:00:00'"),
        "{create}"
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT column_name, column_default FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 'td' \
             ORDER BY ordinal_position",
        ),
        vec![
            vec!["ts".to_owned(), "2020-01-02 08:00:00".to_owned()],
            vec!["dt".to_owned(), "2020-01-02 00:00:00".to_owned()],
        ]
    );

    session.run("SET time_zone = '+00:00'").unwrap();
    assert_eq!(
        rows(&mut session, "SHOW COLUMNS FROM td")[0][4],
        "2020-01-02 00:00:00"
    );
}

/// Metadata corruption is not hidden by the display layer. SHOW propagates
/// the TIMESTAMP conversion failure; INFORMATION_SCHEMA.COLUMNS follows Go's
/// best-effort retriever and falls back to the raw stored string.
#[test]
fn malformed_literal_timestamp_default_has_surface_specific_error_handling() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE bad_ts (ts TIMESTAMP DEFAULT '2020-01-02 00:00:00')")
        .unwrap();
    session
        .with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(table)) = catalog.table_mut_in("test", "bad_ts")
            else {
                panic!("bad_ts is not storage-backed");
            };
            table.columns_mut()[0].default_value =
                Some(tidb_executor::column_default::ColumnDefault::Value(
                    Datum::new_string("not-a-timestamp"),
                ));
            Ok(())
        })
        .unwrap();

    for sql in ["SHOW COLUMNS FROM bad_ts", "SHOW CREATE TABLE bad_ts"] {
        let error = session
            .run(sql)
            .expect_err("SHOW propagates malformed TIMESTAMP metadata")
            .to_mysql_error();
        assert_eq!(error.code, 8038, "{sql}");
        assert_eq!(error.state, *b"HY000", "{sql}");
        assert_eq!(error.message, "Field 'ts' get default value fail", "{sql}");
    }
    assert_eq!(
        rows(
            &mut session,
            "SELECT column_default FROM information_schema.columns \
             WHERE table_schema = 'test' AND table_name = 'bad_ts'",
        ),
        vec![vec!["not-a-timestamp".to_owned()]]
    );
}

/// Transcreates Go `pkg/ddl/db_integration_test.go`'s `TestEnumAndSetDefaultValue`.
///
/// A HEX LITERAL is a legal way to spell an `ENUM`/`SET` element and its
/// `DEFAULT`, and it must be RESOLVED to the member string before the column
/// is stored -- `0x61` names the member `'a'`; it is not the number 97 and not
/// the text `0x61`. Go decides this in `pkg/ddl/add_column.go`
/// `getDefaultValue` -> `types.Datum.ConvertTo` against the enum's field type,
/// so the resolution happens once at CREATE time and everything downstream
/// reads a settled member.
///
/// This is the shape a wrong answer takes here: the hex is ACCEPTED by the
/// parser and then DISCARDED by whoever stores it, leaving a column whose
/// default is not a member of its own type. `0x61` is chosen over an ASCII
/// `'a'` for exactly that reason -- with `'a'` written directly, an engine
/// that never converts anything still passes.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim:
///
/// ```text
/// create table t (a enum(0x61, 'b') not null default 0x61,
///                 b set(0x61, 'b') not null default 0x61) character set latin1
///   `a` enum('a','b') NOT NULL DEFAULT 'a',
///   `b` set('a','b') NOT NULL DEFAULT 'a'
/// ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin
/// insert into t values ()
/// select a, b from t   ->   a|a
/// ```
///
/// The charset is varied because Go's conversion runs against the column's
/// resolved charset: `latin1` and `utf8mb4` must reach the same member.
#[test]
fn a_hex_literal_enum_and_set_default_resolves_to_the_member_string() {
    for (table, charset, collation) in [
        ("t", "latin1", "latin1_bin"),
        ("t2", "utf8mb4", "utf8mb4_bin"),
    ] {
        let mut session = Session::new();
        session
            .run(&format!(
                "CREATE TABLE {table} (a ENUM(0x61, 'b') NOT NULL DEFAULT 0x61, \
                 b SET(0x61, 'b') NOT NULL DEFAULT 0x61) CHARACTER SET {charset}"
            ))
            .unwrap();
        let body = show_create(&mut session, table);
        assert!(
            body.contains("`a` enum('a','b') NOT NULL DEFAULT 'a'"),
            "{charset}: enum column did not resolve its hex element/default: {body}"
        );
        assert!(
            body.contains("`b` set('a','b') NOT NULL DEFAULT 'a'"),
            "{charset}: set column did not resolve its hex element/default: {body}"
        );
        assert!(
            body.contains(&format!("DEFAULT CHARSET={charset} COLLATE={collation}")),
            "{charset}: table charset/collation: {body}"
        );

        session
            .run(&format!("INSERT INTO {table} VALUES ()"))
            .unwrap();
        assert_eq!(
            rows(&mut session, &format!("SELECT a, b FROM {table}")),
            vec![vec!["a".to_owned(), "a".to_owned()]],
            "{charset}: the omitted columns did not take the resolved member"
        );
    }
}

/// Transcreates Go `pkg/ddl/db_integration_test.go`'s `TestEnumDefaultValue`.
///
/// An `ENUM` `DEFAULT` is matched against the member list the way a VALUE of
/// that type is: trailing spaces are not significant, so `DEFAULT 'b '`
/// settles as the member `'b'` and prints back without its space. An engine
/// that stores the literal it was handed keeps a default that no member equals.
///
/// The empty-string member is kept from the Go case on purpose: it makes
/// "matched a member" distinguishable from "fell back to the first member",
/// which for this list is `''` and not `'b'`.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim -- both
/// the exact and the space-padded spelling produce the SAME body:
///
/// ```text
/// CREATE TABLE t3 ( a enum('','a','b') NOT NULL DEFAULT 'b' )
///   ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
///   `a` enum('','a','b') COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'b'
/// CREATE TABLE t4 ( a enum('','a','b') NOT NULL DEFAULT 'b ' )  -- trailing space
///   `a` enum('','a','b') COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'b'
/// ```
#[test]
fn a_space_padded_enum_default_settles_on_the_member_it_names() {
    let expected = "`a` enum('','a','b') COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'b'";
    for (table, written) in [("t3", "'b'"), ("t4", "'b '")] {
        let mut session = Session::new();
        session
            .run(&format!(
                "CREATE TABLE {table} ( a ENUM('','a','b') NOT NULL DEFAULT {written} ) \
                 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"
            ))
            .unwrap();
        let body = show_create(&mut session, table);
        assert!(
            body.contains(expected),
            "declared DEFAULT {written}: {body}"
        );
    }
}

/// Transcreates Go `pkg/ddl/db_integration_test.go`'s `TestBitDefaultValue`,
/// whole, every statement of it.
///
/// A `BIT(n)` column's `DEFAULT` is another accept-then-discard shape, and a
/// wider one than the `ENUM` case above: the value can be written as a plain
/// INTEGER (`DEFAULT 250`) or as a bit literal (`DEFAULT b'1100110111001'`),
/// and Go settles both into the SAME stored form -- the big-endian bytes of
/// the number, padded to the declared width. `pkg/ddl/add_column.go`'s
/// `getDefaultValue` takes the `KindBinaryLiteral`/`KindMysqlBit` branch for
/// the literal spelling and `Datum.ConvertTo` against the `BIT` field type for
/// the integer one; `pkg/executor/show.go` then prints both back as `b'...'`.
///
/// The last case is the one an engine is most likely to get wrong in the other
/// direction: `ALTER TABLE ... MODIFY COLUMN b BIT(1) DEFAULT b'1'` changes
/// what a FUTURE omitted column takes and must NOT rewrite the row already
/// stored, which keeps its `b'0'`.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim (the
/// `SHOW CREATE TABLE` bodies are the hex cells decoded):
///
/// ```text
/// create table t_bit (c1 bit(10) default 250, c2 int)
/// insert into t_bit set c2=1
/// select bin(c1),c2 from t_bit          ->  11111010|1
/// select c1 from t_bit                  ->  BYTES_HEX:00FA
///   `c1` bit(10) DEFAULT b'11111010',
///   `c2` int(11) DEFAULT NULL
///
/// create table t_bit (a int); insert into t_bit value (1)
/// alter table t_bit add column c bit(16) null default b'1100110111001'
/// select c from t_bit                   ->  BYTES_HEX:19B9
/// select bin(c) from t_bit              ->  1100110111001
/// update t_bit set c = b'11100000000111'
/// select bin(c) from t_bit              ->  11100000000111
///   `c` bit(16) DEFAULT b'1100110111001'
///
/// create table t_bit (a int); insert into t_bit value (1)
/// alter table t_bit add column b bit(1) default b'0'
/// alter table t_bit modify column b bit(1) default b'1'
/// select bin(b) from t_bit              ->  0        (the stored row is kept)
///   `b` bit(1) DEFAULT b'1'
/// insert into t_bit (a) values (2)
/// select a, bin(b) from t_bit           ->  1|0;2|1  (the NEW row takes b'1')
///
/// create table t_bit (a bit); insert into t_bit values (null)
/// select count(*) from t_bit where a is null  ->  1
///
/// create table testalltypes1 (field_1 bit default 1, field_2 tinyint null default null)
///   `field_1` bit(1) DEFAULT b'1'
/// ```
///
/// `int(11)`/`tinyint(4)` above are `gorun` display lengths, not what a server
/// prints; nothing here asserts them.
#[test]
fn a_bit_column_default_settles_whether_it_was_written_as_a_number_or_a_literal() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t_bit (c1 BIT(10) DEFAULT 250, c2 INT)")
        .unwrap();
    session.run("INSERT INTO t_bit SET c2=1").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT bin(c1), c2 FROM t_bit"),
        vec![vec!["11111010".to_owned(), "1".to_owned()]],
        "an integer BIT default did not reach the row"
    );
    let body = show_create(&mut session, "t_bit");
    assert!(
        body.contains("`c1` bit(10) DEFAULT b'11111010'"),
        "an integer BIT default did not print back as a bit literal: {body}"
    );
    session.run("DROP TABLE t_bit").unwrap();
}

/// Every OTHER surface that prints a column default carries Go's same
/// `TypeBit` branch, so all three must agree. Captured verbatim:
///
/// ```text
/// create table t_bit (c1 bit(10) default 250, c2 bit(16) default b'1100110111001',
///                     c3 bit(1), c4 bit default 1)
/// show columns from t_bit
///   c1|bit(10)|YES||b'11111010'|
///   c2|bit(16)|YES||b'1100110111001'|
///   c3|bit(1)|YES||<nil>|
///   c4|bit(1)|YES||b'1'|
/// select column_name, column_default from information_schema.columns
///   c1|b'11111010'  c2|b'1100110111001'  c3|<nil>  c4|b'1'
/// ```
#[test]
fn every_surface_prints_a_bit_default_as_the_same_literal() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t_bit (c1 BIT(10) DEFAULT 250, c2 BIT(16) DEFAULT b'1100110111001', \
             c3 BIT(1), c4 BIT DEFAULT 1)",
        )
        .unwrap();
    let expected = [
        ("c1", "b'11111010'"),
        ("c2", "b'1100110111001'"),
        ("c3", "NULL"),
        ("c4", "b'1'"),
    ];

    let columns = rows(&mut session, "SHOW COLUMNS FROM t_bit");
    for (row, (name, default)) in columns.iter().zip(expected) {
        assert_eq!(row[0], name);
        assert_eq!(row[4], default, "SHOW COLUMNS default for {name}");
    }

    let mut catalog = rows(
        &mut session,
        "SELECT column_name, column_default FROM information_schema.columns \
         WHERE table_name = 't_bit'",
    );
    catalog.sort();
    assert_eq!(
        catalog,
        expected
            .iter()
            .map(|(name, default)| vec![(*name).to_owned(), (*default).to_owned()])
            .collect::<Vec<_>>(),
        "information_schema.columns disagreed with SHOW COLUMNS"
    );
}

/// The `ADD COLUMN` half of `TestBitDefaultValue`: a bit literal wider than a
/// byte, backfilled into a row that already exists, then overwritten.
#[test]
fn an_added_bit_column_backfills_its_literal_default_and_still_takes_an_update() {
    let mut session = Session::new();
    session.run("CREATE TABLE t_bit (a INT)").unwrap();
    session.run("INSERT INTO t_bit VALUE (1)").unwrap();
    session
        .run("ALTER TABLE t_bit ADD COLUMN c BIT(16) NULL DEFAULT b'1100110111001'")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT bin(c) FROM t_bit"),
        vec![vec!["1100110111001".to_owned()]],
        "the added column did not backfill its default"
    );
    let body = show_create(&mut session, "t_bit");
    assert!(
        body.contains("`c` bit(16) DEFAULT b'1100110111001'"),
        "the added BIT column's default did not print back: {body}"
    );
    session
        .run("UPDATE t_bit SET c = b'11100000000111'")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT bin(c) FROM t_bit"),
        vec![vec!["11100000000111".to_owned()]],
        "a bit literal did not survive an UPDATE"
    );
}

/// The `MODIFY COLUMN` half of `TestBitDefaultValue`: changing a `DEFAULT`
/// changes what a LATER omitted column takes and leaves stored rows alone.
#[test]
fn modifying_a_bit_default_reaches_the_next_row_and_not_the_stored_one() {
    let mut session = Session::new();
    session.run("CREATE TABLE t_bit (a INT)").unwrap();
    session.run("INSERT INTO t_bit VALUE (1)").unwrap();
    session
        .run("ALTER TABLE t_bit ADD COLUMN b BIT(1) DEFAULT b'0'")
        .unwrap();
    session
        .run("ALTER TABLE t_bit MODIFY COLUMN b BIT(1) DEFAULT b'1'")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT bin(b) FROM t_bit"),
        vec![vec!["0".to_owned()]],
        "MODIFY COLUMN rewrote a stored row it must not touch"
    );
    let body = show_create(&mut session, "t_bit");
    assert!(
        body.contains("`b` bit(1) DEFAULT b'1'"),
        "MODIFY COLUMN did not record the new default: {body}"
    );
    session.run("INSERT INTO t_bit (a) VALUES (2)").unwrap();
    let mut observed = rows(&mut session, "SELECT a, bin(b) FROM t_bit");
    observed.sort();
    assert_eq!(
        observed,
        vec![
            vec!["1".to_owned(), "0".to_owned()],
            vec!["2".to_owned(), "1".to_owned()],
        ],
        "the row inserted after the MODIFY did not take the new default"
    );
}

/// The remaining two statements of `TestBitDefaultValue`: an undeclared width
/// is `BIT(1)`, a NULL is storable in one, and a bare integer default on a
/// bare `BIT` prints as `b'1'`.
#[test]
fn a_bare_bit_column_is_one_bit_wide_and_holds_null() {
    let mut session = Session::new();
    session.run("CREATE TABLE t_bit (a BIT)").unwrap();
    session.run("INSERT INTO t_bit VALUES (null)").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT count(*) FROM t_bit WHERE a IS NULL"),
        vec![vec!["1".to_owned()]],
        "a NULL BIT did not read back as NULL"
    );

    session
        .run(
            "CREATE TABLE testalltypes1 (field_1 BIT DEFAULT 1, field_2 TINYINT NULL DEFAULT NULL)",
        )
        .unwrap();
    let body = show_create(&mut session, "testalltypes1");
    assert!(
        body.contains("`field_1` bit(1) DEFAULT b'1'"),
        "a bare BIT column did not settle to bit(1) DEFAULT b'1': {body}"
    );
}

/// Transcreates the `ALTER TABLE ... ALTER COLUMN` half of Go
/// `pkg/ddl/db_integration_test.go`'s `TestAlterColumn`, statement for
/// statement.
///
/// `ALTER COLUMN ... SET DEFAULT` replaces the column's default and NOTHING
/// else: the rows already written keep what they hold, and only a row written
/// AFTERWARDS that omits the column takes the new value. Go `AlterColumn`
/// touches `ColumnInfo.DefaultValue` alone.
///
/// The accept-then-discard candidate in this statement is the LAST assertion:
/// `SET DEFAULT NULL` on a `NOT NULL` column is `ErrInvalidDefault` (1067),
/// because the column could never hold it. An engine that stores the default
/// it was handed accepts a `NOT NULL` column whose default is NULL.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim:
///
/// ```text
/// create table test_alter_column (a int default 111, b varchar(8),
///                                 c varchar(8) not null,
///                                 d timestamp on update current_timestamp)
/// insert into test_alter_column set b = 'a', c = 'aa'
/// select a from test_alter_column                             ->  111
/// alter table test_alter_column alter column a set default 222
/// insert into test_alter_column set b = 'b', c = 'bb'
/// select a from test_alter_column                             ->  111;222
/// alter table test_alter_column alter column b set default null
/// insert into test_alter_column set c = 'cc'
/// select b from test_alter_column                             ->  <nil>;a;b
/// alter table test_alter_column alter column c set default 'xx'
/// insert into test_alter_column set a = 123
/// select c from test_alter_column                             ->  aa;bb;cc;xx
/// show create table test_alter_column
///   `a` int(11) DEFAULT '222',
///   `b` varchar(8) DEFAULT NULL,
///   `c` varchar(8) NOT NULL DEFAULT 'xx',
///   `d` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP
/// alter table db_not_exist.test_alter_column alter column b set default 'c' ERR 1146
/// alter table test_not_exist alter column b set default 'c'                 ERR 1146
/// alter table test_alter_column alter column col_not_exist set default 'c'  ERR 1054
/// alter table test_alter_column alter column c set default null             ERR 1067
/// ```
///
/// The error NUMBERS come from the Go test's own `MustGetErrCode` calls
/// (`ErrNoSuchTable`, `ErrBadField`, `ErrInvalidDefault`); `gorun` prints a
/// bare `ERR`.
#[test]
fn alter_column_set_default_reaches_the_next_row_only() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE test_alter_column (a INT DEFAULT 111, b VARCHAR(8), \
             c VARCHAR(8) NOT NULL, d TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)",
        )
        .unwrap();
    session
        .run("INSERT INTO test_alter_column SET b = 'a', c = 'aa'")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a FROM test_alter_column"),
        vec![vec!["111".to_owned()]]
    );

    session
        .run("ALTER TABLE test_alter_column ALTER COLUMN a SET DEFAULT 222")
        .unwrap();
    session
        .run("INSERT INTO test_alter_column SET b = 'b', c = 'bb'")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a FROM test_alter_column"),
        vec![vec!["111".to_owned()], vec!["222".to_owned()]],
        "SET DEFAULT rewrote the row written before it"
    );

    session
        .run("ALTER TABLE test_alter_column ALTER COLUMN b SET DEFAULT null")
        .unwrap();
    session
        .run("INSERT INTO test_alter_column SET c = 'cc'")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT b FROM test_alter_column"),
        vec![
            vec!["a".to_owned()],
            vec!["b".to_owned()],
            vec!["NULL".to_owned()]
        ]
    );

    session
        .run("ALTER TABLE test_alter_column ALTER COLUMN c SET DEFAULT 'xx'")
        .unwrap();
    session
        .run("INSERT INTO test_alter_column SET a = 123")
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT c FROM test_alter_column"),
        vec![
            vec!["aa".to_owned()],
            vec!["bb".to_owned()],
            vec!["cc".to_owned()],
            vec!["xx".to_owned()]
        ],
        "the NOT NULL column's new default did not reach the row that omitted it"
    );

    let body = show_create(&mut session, "test_alter_column");
    for clause in [
        "`a` int DEFAULT '222'",
        "`b` varchar(8) DEFAULT NULL",
        "`c` varchar(8) NOT NULL DEFAULT 'xx'",
        "`d` timestamp NULL DEFAULT NULL",
    ] {
        assert!(body.contains(clause), "missing `{clause}`: {body}");
    }

    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE db_not_exist.test_alter_column ALTER COLUMN b SET DEFAULT 'c'"
        ),
        Some(1146)
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE test_not_exist ALTER COLUMN b SET DEFAULT 'c'"
        ),
        Some(1146)
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE test_alter_column ALTER COLUMN col_not_exist SET DEFAULT 'c'"
        ),
        Some(1054)
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE test_alter_column ALTER COLUMN col_not_exist SET DEFAULT (ABS(1))"
        ),
        Some(1054),
        "the missing target must win before the independently unsupported default"
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE test_alter_column ALTER COLUMN c SET DEFAULT null"
        ),
        Some(1067),
        "a NOT NULL column accepted a DEFAULT it can never hold"
    );
}

#[test]
fn alter_column_set_default_retains_computed_expressions() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    session
        .run("CREATE TABLE alter_default_expr (id INT, ts DATETIME(3), token VARCHAR(64))")
        .unwrap();
    session
        .run("INSERT INTO alter_default_expr VALUES (0, NULL, NULL)")
        .unwrap();

    session
        .run(
            "ALTER TABLE alter_default_expr ALTER COLUMN ts \
             SET DEFAULT (CURRENT_TIMESTAMP(3))",
        )
        .unwrap();
    session
        .run(
            "ALTER TABLE alter_default_expr ALTER COLUMN token \
             SET DEFAULT (uuid())",
        )
        .unwrap();
    let definition = show_create(&mut session, "alter_default_expr");
    assert!(
        definition.contains("`ts` datetime(3) DEFAULT CURRENT_TIMESTAMP(3)"),
        "{definition}"
    );
    assert!(
        definition.contains("`token` varchar(64) DEFAULT (uuid())"),
        "{definition}"
    );

    session.run("SET timestamp = 1700000000").unwrap();
    session
        .run("INSERT INTO alter_default_expr (id) VALUES (1)")
        .unwrap();
    session.run("SET timestamp = 1700000100").unwrap();
    session
        .run("INSERT INTO alter_default_expr (id) VALUES (2)")
        .unwrap();
    let values = rows(
        &mut session,
        "SELECT id, ts, token FROM alter_default_expr ORDER BY id",
    );
    assert_eq!(values[0], ["0", "NULL", "NULL"]);
    assert_eq!(values[1][1], "2023-11-14 22:13:20.000");
    assert_eq!(values[2][1], "2023-11-14 22:15:00.000");
    assert_eq!(values[1][2].len(), 36);
    assert_eq!(values[2][2].len(), 36);
    assert_ne!(values[1][2], values[2][2]);

    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE alter_default_expr ALTER COLUMN ts \
             SET DEFAULT CURRENT_TIMESTAMP"
        ),
        Some(1064)
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE alter_default_expr ALTER COLUMN ts \
             SET DEFAULT (CURRENT_TIMESTAMP)"
        ),
        Some(1067)
    );

    session
        .run("CREATE TABLE alter_default_ai (id BIGINT AUTO_INCREMENT PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE alter_default_ai ALTER COLUMN id SET DEFAULT (uuid())"
        ),
        Some(1067)
    );
    session
        .run("CREATE TABLE alter_default_ar (id BIGINT AUTO_RANDOM(5) PRIMARY KEY)")
        .unwrap();
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE alter_default_ar ALTER COLUMN id SET DEFAULT 1"
        ),
        Some(8216)
    );
}

#[test]
fn alter_column_current_date_retains_the_temporal_marker() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    session
        .run("CREATE TABLE alter_current_date (id INT, d DATE)")
        .unwrap();
    session
        .run("INSERT INTO alter_current_date VALUES (0, NULL)")
        .unwrap();
    session
        .run(
            "ALTER TABLE alter_current_date ALTER COLUMN d \
             SET DEFAULT (CURRENT_DATE())",
        )
        .unwrap();

    let definition = show_create(&mut session, "alter_current_date");
    assert!(
        definition.contains("`d` date DEFAULT (CURRENT_DATE)"),
        "{definition}"
    );
    let columns = rows(&mut session, "SHOW COLUMNS FROM alter_current_date");
    assert_eq!(columns[1][4], "CURRENT_DATE");
    assert_eq!(columns[1][5], "");

    session.run("SET timestamp = 1700000000").unwrap();
    session
        .run("INSERT INTO alter_current_date (id) VALUES (1)")
        .unwrap();
    session.run("SET timestamp = 1700086400").unwrap();
    session
        .run("INSERT INTO alter_current_date (id) VALUES (2)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT id,d FROM alter_current_date ORDER BY id"
        ),
        [["0", "NULL"], ["1", "2023-11-14"], ["2", "2023-11-15"],]
    );
}

#[test]
fn str_to_date_default_reaches_every_ddl_entry_point() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE default_str_to_date (\
             id INT, \
             created_value VARCHAR(32) \
             DEFAULT (str_to_date('1980-01-01','%Y-%m-%d')))",
        )
        .unwrap();
    session
        .run("INSERT INTO default_str_to_date (id) VALUES (1)")
        .unwrap();
    session
        .run(
            "ALTER TABLE default_str_to_date ADD COLUMN added_value VARCHAR(32) \
             DEFAULT (str_to_date('1981-02-03','%Y-%m-%d'))",
        )
        .unwrap();
    session
        .run(
            "ALTER TABLE default_str_to_date MODIFY COLUMN created_value VARCHAR(32) \
             DEFAULT (str_to_date('1982-03-04','%Y-%m-%d'))",
        )
        .unwrap();
    session
        .run(
            "ALTER TABLE default_str_to_date ALTER COLUMN added_value \
             SET DEFAULT(str_to_date('1983-04-05','%Y-%m-%d'))",
        )
        .unwrap();
    session
        .run("INSERT INTO default_str_to_date (id) VALUES (2)")
        .unwrap();

    assert_eq!(
        rows(
            &mut session,
            "SELECT id, created_value, added_value \
             FROM default_str_to_date ORDER BY id"
        ),
        [
            ["1", "1980-01-01", "1981-02-03"],
            ["2", "1982-03-04", "1983-04-05"],
        ]
    );
    let definition = show_create(&mut session, "default_str_to_date");
    assert!(
        definition.contains("DEFAULT (str_to_date(_utf8mb4'1982-03-04', _utf8mb4'%Y-%m-%d'))"),
        "{definition}"
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE bad_str_to_date (\
             value VARCHAR(32) \
             DEFAULT (str_to_date(upper('1980-01-01'),'%Y-%m-%d')))"
        ),
        Some(3770)
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE bad_str_to_date (\
             value VARCHAR(32) DEFAULT (str_to_date('1980-01-01')))"
        ),
        Some(1582)
    );
}

#[test]
fn computed_default_whitelist_evaluates_the_allowed_function_shapes() {
    let mut session = Session::new();
    session.set_user("bob@%".to_owned(), "bob@10.0.0.1".to_owned());
    session.run("SET time_zone = '+00:00'").unwrap();
    session.run("SET timestamp = 1700000000").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT UPPER(USER()), UPPER(DATABASE())"),
        [["BOB@10.0.0.1", "TEST"]]
    );
    session
        .run(
            "CREATE TABLE default_function_suite (\
             id INT, \
             formatted VARCHAR(32) DEFAULT (date_format(now(),'%Y-%m-%d')), \
             compact VARCHAR(32) DEFAULT (replace(upper(uuid()),'-','')), \
             login_name VARCHAR(64) \
                 DEFAULT (upper(substring_index(user(),'@',1))), \
             packed VARBINARY(16) DEFAULT (\
                 uuid_to_bin('6ccd780c-baba-1026-9564-5b8c656024db')), \
             document JSON DEFAULT (json_object('k',7)))",
        )
        .unwrap();
    session
        .run("INSERT INTO default_function_suite (id) VALUES (1)")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT formatted, LENGTH(compact), login_name, \
                    compact REGEXP '^[A-F0-9]{32}$', HEX(packed), \
                    JSON_EXTRACT(document,'$.k') \
             FROM default_function_suite"
        ),
        [[
            "2023-11-14",
            "32",
            "BOB",
            "1",
            "6CCD780CBABA102695645B8C656024DB",
            "7",
        ]]
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE default_function_suite ADD COLUMN unsafe_packed VARBINARY(16) \
             DEFAULT (uuid_to_bin('6ccd780c-baba-1026-9564-5b8c656024db'))"
        ),
        Some(1674)
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE bad_date_format (\
             value VARCHAR(32) DEFAULT (date_format(now(),'%b %d %Y')))"
        ),
        Some(3770)
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE bad_uuid (value VARCHAR(64) DEFAULT (uuid(1)))"
        ),
        Some(1582)
    );
}

#[test]
fn alter_add_and_modify_keep_computed_defaults_as_expressions() {
    let mut session = Session::new();
    session.run("SET time_zone = '+00:00'").unwrap();
    session.run("SET timestamp = 1700000000").unwrap();
    session
        .run("CREATE TABLE alter_computed (id INT PRIMARY KEY)")
        .unwrap();
    session
        .run("INSERT INTO alter_computed VALUES (1)")
        .unwrap();
    session
        .run(
            "ALTER TABLE alter_computed ADD COLUMN payload BLOB \
             DEFAULT (date_format(now(),'%Y-%m-%d'))",
        )
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT payload FROM alter_computed"),
        [["2023-11-14"]]
    );
    assert!(show_create(&mut session, "alter_computed")
        .contains("`payload` blob DEFAULT (date_format(now(), _utf8mb4'%Y-%m-%d'))"));

    session
        .run("ALTER TABLE alter_computed ADD COLUMN document JSON")
        .unwrap();
    session
        .run(
            "ALTER TABLE alter_computed MODIFY COLUMN document JSON \
             DEFAULT (json_quote('foobar'))",
        )
        .unwrap();
    assert!(show_create(&mut session, "alter_computed")
        .contains("`document` json DEFAULT (json_quote(_utf8mb4'foobar'))"));

    session
        .run("ALTER TABLE alter_computed ADD COLUMN choice ENUM('y','n') DEFAULT 'y'")
        .unwrap();
    session
        .run(
            "ALTER TABLE alter_computed MODIFY COLUMN choice ENUM('y','n') \
             DEFAULT (date_format(now(),'%Y-%m-%d'))",
        )
        .unwrap();
    assert_eq!(
        rows(&mut session, "SELECT choice FROM alter_computed"),
        [["y"]]
    );
    assert!(show_create(&mut session, "alter_computed")
        .contains("`choice` enum('y','n') DEFAULT (date_format(now(), _utf8mb4'%Y-%m-%d'))"));
}

/// Go `pkg/ddl/add_column.go` runs the inline-key precheck before installing a
/// table-level primary key and before the final `checkDefaultValue`:
///
///  * a `NOT NULL` column whose `DEFAULT` is `NULL` is `ErrInvalidDefault`
///    (1067);
///  * an inline `PRIMARY KEY DEFAULT NULL` is also 1067 because its PRI flag
///    exists at the early `checkPriKeyConstraint` precheck;
///  * a table-level primary key is installed only after that precheck, so its
///    `DEFAULT NULL` reaches final validation as `ErrPrimaryCantHaveNull`
///    (1171);
///  * `NULL` and `NOT NULL` options change the flag in source order, while the
///    occurrence of any explicit `NULL` remains a primary-key refusal.
///
/// This is accept-then-discard in its purest form: the column is declared
/// unable to hold NULL and handed NULL as the value an omitted column takes.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim:
///
/// ```text
/// create table n1 (a int not null default null)                ERR
/// create table n2 (a int primary key default null)             ERR
/// create table n4 (a int default null, primary key(a))          ERR
/// create table n3 (a int not null)                             OK
/// alter table n3 add column b varchar(4) not null default null ERR
/// ```
///
/// The error NUMBERS are `checkDefaultValue`'s own returns; `gorun` prints a
/// bare `ERR`.
#[test]
fn a_column_that_cannot_hold_null_cannot_default_to_it() {
    let mut session = Session::new();
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE n1 (a INT NOT NULL DEFAULT null)"
        ),
        Some(1067)
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE n2 (a INT PRIMARY KEY DEFAULT null)"
        ),
        Some(1067)
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE n4 (a INT DEFAULT null, PRIMARY KEY (a))"
        ),
        Some(1171)
    );
    assert_eq!(
        code(
            &mut session,
            "CREATE TABLE n8 (a INT NOT NULL DEFAULT null, PRIMARY KEY (a))"
        ),
        Some(1171)
    );
    session.run("CREATE TABLE n3 (a INT NOT NULL)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE n3 ADD COLUMN b VARCHAR(4) NOT NULL DEFAULT null"
        ),
        Some(1067)
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE n3 MODIFY COLUMN a INT NOT NULL DEFAULT null"
        ),
        Some(1067)
    );

    session
        .run("CREATE TABLE n5 (a INT NOT NULL NULL)")
        .unwrap();
    session.run("INSERT INTO n5 VALUES (NULL)").unwrap();
    session
        .run("CREATE TABLE n6 (a INT NULL NOT NULL)")
        .unwrap();
    assert_eq!(
        code(&mut session, "INSERT INTO n6 VALUES (NULL)"),
        Some(1048)
    );
    assert_eq!(
        code(&mut session, "CREATE TABLE n7 (a INT NULL PRIMARY KEY)"),
        Some(1171)
    );

    // The same source-order mutation is used by ADD and MODIFY. The last
    // NULL/NOT NULL option controls the final flag that `checkDefaultValue`
    // sees; merely finding any NOT NULL is not equivalent.
    session.run("CREATE TABLE n9 (a INT)").unwrap();
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE n9 ADD COLUMN b INT NOT NULL NULL DEFAULT null"
        ),
        None
    );
    assert!(show_create(&mut session, "n9").contains("`b` int DEFAULT NULL"));
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE n9 ADD COLUMN c INT NULL NOT NULL DEFAULT null"
        ),
        Some(1067)
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE n9 MODIFY COLUMN a INT NOT NULL NULL DEFAULT null"
        ),
        None
    );
    assert!(show_create(&mut session, "n9").contains("`a` int DEFAULT NULL"));
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE n9 MODIFY COLUMN a INT NULL NOT NULL DEFAULT null"
        ),
        Some(1067)
    );

    // MODIFY copies an existing primary key's PRI/NOT-NULL baseline before
    // visiting its options. Any explicit NULL is then 1171 even if NOT NULL
    // follows it, while DEFAULT NULL wins in the preceding key-default check
    // with 1067.
    session.run("CREATE TABLE n10 (a INT PRIMARY KEY)").unwrap();
    assert_eq!(
        code(&mut session, "ALTER TABLE n10 MODIFY COLUMN a INT NULL"),
        Some(1171)
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE n10 MODIFY COLUMN a INT NULL NOT NULL"
        ),
        Some(1171)
    );
    assert_eq!(
        code(
            &mut session,
            "ALTER TABLE n10 MODIFY COLUMN a INT NULL DEFAULT null"
        ),
        Some(1067)
    );
}

/// Go `pkg/ddl/add_column.go` `setDefaultValueWithBinaryPadding`: a fixed-width
/// `BINARY(n)` column pads its `DEFAULT` with NUL bytes to the full width, the
/// way a VALUE written into one is padded. `VARBINARY` and `VARCHAR` are
/// variable width and are not padded.
///
/// Without the padding the stored default is shorter than anything the column
/// can hold, so an omitted column and an explicitly written one disagree on a
/// column whose whole point is a fixed width.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim:
///
/// ```text
/// create table t_bin (a binary(4) default 0x61, b varbinary(4) default 0x61,
///                     c varchar(4) default 0x61, d varchar(4) default 0x615c62)
///   `a` binary(4) DEFAULT 'a\0\0\0',
///   `b` varbinary(4) DEFAULT 'a',
///   `c` varchar(4) DEFAULT 'a'
///   `d` varchar(4) DEFAULT 'a\\b'
/// ```
///
/// The default is written as the hex literal `0x61` rather than `'a'` on
/// purpose: it has to be decoded to the member text before it can be padded,
/// so one fixture covers both steps.
#[test]
fn a_fixed_width_binary_default_is_padded_to_the_columns_width() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t_bin (a BINARY(4) DEFAULT 0x61, b VARBINARY(4) DEFAULT 0x61, \
             c VARCHAR(4) DEFAULT 0x61, d VARCHAR(4) DEFAULT 0x615c62)",
        )
        .unwrap();
    session.run("INSERT INTO t_bin VALUES ()").unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT length(a), length(b), length(c) FROM t_bin"
        ),
        vec![vec!["4".to_owned(), "1".to_owned(), "1".to_owned()]],
        "the fixed-width column's default was not padded to its width"
    );
    let body = show_create(&mut session, "t_bin");
    for clause in [
        // `format.OutputFormat` escapes the padding NULs.
        "`a` binary(4) DEFAULT 'a\\0\\0\\0'",
        "`b` varbinary(4) DEFAULT 'a'",
        "`c` varchar(4) DEFAULT 'a'",
        // `pkg/util/format.OutputFormat`, unlike the parser package's
        // same-named helper, doubles a backslash.
        "`d` varchar(4) DEFAULT 'a\\\\b'",
    ] {
        assert!(body.contains(clause), "missing `{clause}`: {body:?}");
    }

    // The same printer rule, on the character the escaping exists for.
    // Captured: `create table q (a varchar(10) default 'a''b')` prints
    // `` `a` varchar(10) DEFAULT 'a''b' `` and the stored value is `a'b`,
    // length 3.
    session
        .run("CREATE TABLE q (a VARCHAR(10) DEFAULT 'a''b')")
        .unwrap();
    let body = show_create(&mut session, "q");
    assert!(
        body.contains("`a` varchar(10) DEFAULT 'a''b'"),
        "an embedded quote was not doubled, so the body does not re-parse: {body}"
    );
    session.run("INSERT INTO q VALUES ()").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a, length(a) FROM q"),
        vec![vec!["a'b".to_owned(), "3".to_owned()]]
    );
}

#[test]
fn drop_default_preserves_the_no_default_column_state() {
    let mut session = Session::new();
    session.run("CREATE TABLE ti (a INT)").unwrap();
    assert!(
        session
            .run("ALTER TABLE ti ALTER COLUMN a DROP DEFAULT")
            .is_ok(),
        "DROP DEFAULT must install the no-default flag"
    );
    assert_eq!(code(&mut session, "INSERT INTO ti VALUES ()"), Some(1364));
    let create = show_create(&mut session, "ti");
    assert!(create.contains("`a` int\n"), "{create}");
    session
        .run("ALTER TABLE ti ALTER COLUMN a SET DEFAULT 7")
        .unwrap();
    session.run("INSERT INTO ti VALUES ()").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a FROM ti"),
        vec![vec!["7".to_owned()]]
    );

    session.run("CREATE TABLE te (a ENUM('a','b'))").unwrap();
    session
        .run("ALTER TABLE te ALTER COLUMN a DROP DEFAULT")
        .unwrap();
    session.run("INSERT INTO te VALUES ()").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a FROM te"),
        vec![vec!["NULL".to_owned()]]
    );

    session
        .run("CREATE TABLE te2 (a ENUM('a','b') NOT NULL)")
        .unwrap();
    session
        .run("ALTER TABLE te2 ALTER COLUMN a DROP DEFAULT")
        .unwrap();
    session.run("INSERT INTO te2 VALUES ()").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a FROM te2"),
        vec![vec!["a".to_owned()]]
    );
}

/// `checkColumnDefaultValue`'s `TypeBit` arm: a `BIT(n)` default must FIT in
/// the declared width, or it is `ErrInvalidDefault` (1067). Go reads the
/// settled bits back as an integer and compares against `1 << flen`.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim:
///
/// ```text
/// create table n4 (a bit(1) default 250)                     ERR
/// create table n5 (a bit(10) default 1024)                   ERR
/// create table n6 (a bit(10) default 1023)                   OK
///   `a` bit(10) DEFAULT b'1111111111'
/// create table n7 (a bit(64) default 18446744073709551615)   OK
///   `a` bit(64) DEFAULT b'111...1'  (64 ones)
/// ```
#[test]
fn a_bit_default_wider_than_its_column_is_refused() {
    let mut session = Session::new();
    assert_eq!(
        code(&mut session, "CREATE TABLE n4 (a BIT(1) DEFAULT 250)"),
        Some(1067)
    );
    assert_eq!(
        code(&mut session, "CREATE TABLE n5 (a BIT(10) DEFAULT 1024)"),
        Some(1067)
    );
    session
        .run("CREATE TABLE n6 (a BIT(10) DEFAULT 1023)")
        .unwrap();
    assert!(show_create(&mut session, "n6").contains("`a` bit(10) DEFAULT b'1111111111'"));
    session
        .run("CREATE TABLE n7 (a BIT(64) DEFAULT 18446744073709551615)")
        .unwrap();
    assert!(show_create(&mut session, "n7")
        .contains(&format!("`a` bit(64) DEFAULT b'{}'", "1".repeat(64))));
}

#[test]
fn vector_columns_enforce_dimensions_and_computed_defaults() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE vectors (\
             id INT, embedding VECTOR(3) DEFAULT (VEC_FROM_TEXT('[1,2,3]')))",
        )
        .unwrap();
    let create = show_create(&mut session, "vectors");
    assert!(
        create.contains("`embedding` vector(3) DEFAULT (vec_from_text(_utf8mb4'[1,2,3]'))"),
        "{create}"
    );

    session.run("INSERT INTO vectors (id) VALUES (1)").unwrap();
    session
        .run("INSERT INTO vectors VALUES (2, '[4,5,6]')")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT id, VEC_AS_TEXT(embedding), VEC_DIMS(embedding) \
             FROM vectors ORDER BY id"
        ),
        vec![
            vec!["1".to_owned(), "[1,2,3]".to_owned(), "3".to_owned()],
            vec!["2".to_owned(), "[4,5,6]".to_owned(), "3".to_owned()],
        ]
    );

    session
        .run("CREATE TABLE dynamic_vectors (id INT, embedding VECTOR)")
        .unwrap();
    session
        .run("INSERT INTO dynamic_vectors VALUES (1, '[1,2]'), (2, '[3,4,5]')")
        .unwrap();
    assert_eq!(
        rows(
            &mut session,
            "SELECT VEC_DIMS(embedding) FROM dynamic_vectors ORDER BY id"
        ),
        vec![vec!["2".to_owned()], vec!["3".to_owned()]]
    );
    let mixed = session
        .run("ALTER TABLE dynamic_vectors MODIFY COLUMN embedding VECTOR(2)")
        .expect_err("every stored vector must fit the fixed dimension")
        .to_mysql_error();
    assert_eq!(mixed.code, 1105);
    assert_eq!(
        mixed.message,
        "vector has 3 dimensions, does not fit VECTOR(2)"
    );
    session
        .run("DELETE FROM dynamic_vectors WHERE id = 2")
        .unwrap();
    session
        .run("ALTER TABLE dynamic_vectors MODIFY COLUMN embedding VECTOR(2)")
        .unwrap();
    let wrong_row = session
        .run("INSERT INTO dynamic_vectors VALUES (3, '[1,2,3]')")
        .expect_err("fixed vectors reject a mismatched row")
        .to_mysql_error();
    assert_eq!(wrong_row.code, 1105);
    assert_eq!(
        wrong_row.message,
        "vector has 3 dimensions, does not fit VECTOR(2)"
    );
    let wrong_update = session
        .run("UPDATE dynamic_vectors SET embedding = '[1,2,3]' WHERE id = 1")
        .expect_err("updates obey the same fixed dimension")
        .to_mysql_error();
    assert_eq!(wrong_update.code, 1105);
    assert_eq!(
        wrong_update.message,
        "vector has 3 dimensions, does not fit VECTOR(2)"
    );

    let literal = session
        .run("CREATE TABLE literal_vector (embedding VECTOR DEFAULT '[1,2,3]')")
        .expect_err("a VECTOR literal default is forbidden")
        .to_mysql_error();
    assert_eq!(literal.code, 1105);
    assert_eq!(
        literal.message,
        "VECTOR column 'embedding' can't have a literal default. Use expression default instead: ((VEC_FROM_TEXT('...')))"
    );

    let oversized = session
        .run("CREATE TABLE oversized_vector (embedding VECTOR(16384))")
        .expect_err("the maximum dimension is 16383")
        .to_mysql_error();
    assert_eq!(oversized.code, 1105);
    assert_eq!(
        oversized.message,
        "vector cannot have more than 16383 dimensions"
    );

    session
        .run(
            "CREATE TABLE delayed_dimension_check (\
             id INT, embedding VECTOR(2) DEFAULT (VEC_FROM_TEXT('[1,2,3]')))",
        )
        .unwrap();
    let mismatch = session
        .run("INSERT INTO delayed_dimension_check (id) VALUES (1)")
        .expect_err("computed defaults are dimension-checked when written")
        .to_mysql_error();
    assert_eq!(mismatch.code, 1105);
    assert_eq!(
        mismatch.message,
        "vector has 3 dimensions, does not fit VECTOR(2)"
    );
}
