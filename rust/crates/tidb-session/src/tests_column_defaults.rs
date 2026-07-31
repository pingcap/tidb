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

/// `DEFAULT (uuid())` is ON Go's whitelist and captured as
/// `` `b` varchar(64) DEFAULT (uuid()) ``, but `uuid` is not among the
/// builtins this tier evaluates over a chunk, so the default is refused at
/// DDL time rather than stored as one that would fail on every INSERT.
#[test]
fn a_uuid_default_is_refused_because_the_builtin_is_not_evaluable() {
    let mut session = Session::new();
    assert!(session
        .run("CREATE TABLE t4 (a INT, b VARCHAR(64) DEFAULT (uuid()))")
        .is_err());
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
    let clock = rows(&mut session, "SHOW COLUMNS FROM t1");
    assert_eq!(clock[1][4], "CURRENT_TIMESTAMP");
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
            "ALTER TABLE test_alter_column ALTER COLUMN c SET DEFAULT null"
        ),
        Some(1067),
        "a NOT NULL column accepted a DEFAULT it can never hold"
    );
}

/// Go `pkg/ddl/add_column.go` `checkDefaultValue`'s last two arms, which every
/// path that writes a default runs -- `CREATE TABLE`, `ADD COLUMN`,
/// `MODIFY COLUMN` and `ALTER COLUMN ... SET DEFAULT` alike:
///
///  * a `NOT NULL` column whose `DEFAULT` is `NULL` is `ErrInvalidDefault`
///    (1067);
///  * a `PRIMARY KEY` column whose `DEFAULT` is `NULL` is
///    `ErrPrimaryCantHaveNull` (1171), which is checked FIRST and so wins on a
///    column that is both.
///
/// This is accept-then-discard in its purest form: the column is declared
/// unable to hold NULL and handed NULL as the value an omitted column takes.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim:
///
/// ```text
/// create table n1 (a int not null default null)                ERR
/// create table n2 (a int primary key default null)             ERR
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
///                     c varchar(4) default 0x61)
///   `a` binary(4) DEFAULT 'a\0\0\0',
///   `b` varbinary(4) DEFAULT 'a',
///   `c` varchar(4) DEFAULT 'a'
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
             c VARCHAR(4) DEFAULT 0x61)",
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

/// PINS A REFUSAL. `ALTER TABLE ... ALTER COLUMN c DROP DEFAULT` is REFUSED by
/// this tier rather than approximated, and this test exists so that the refusal
/// cannot be forgotten: when the gap closes it FAILS, and the Go answers it
/// carries -- captured, and asserted nowhere yet -- become its assertions.
///
/// Go's `DROP DEFAULT` is not "clear the default". `AlterColumn` sets
/// `mysql.NoDefaultValueFlag`, and that flag is what a later `INSERT` reads:
/// omitting the column is 1364 "Field 'a' doesn't have a default value" even on
/// a NULLABLE column that would otherwise have taken NULL, and
/// `SHOW CREATE TABLE` prints the column with NO `DEFAULT` clause at all rather
/// than `DEFAULT NULL`. This tier models a default as "written or not written"
/// with no such flag, so clearing it would silently answer NULL where TiDB
/// raises 1364 -- a wrong answer in place of an error.
///
/// Captured from real TiDB through `rust/difftests/gorun`, verbatim:
///
/// ```text
/// create table ti (a int)
/// alter table ti alter column a drop default        OK
/// insert into ti values ()                          ERR   (1364)
/// show create table ti
///   `a` int(11)                                     -- no DEFAULT clause
///
/// create table te (a enum('a','b'))
/// alter table te alter column a drop default        OK
/// insert into te values ()                          OK
/// select * from te                                  ->  <nil>
///
/// create table te2 (a enum('a','b') not null)
/// alter table te2 alter column a drop default       OK
/// insert into te2 values ()                         OK
/// select * from te2                                 ->  a
/// ```
///
/// The 1364 is `errno.ErrNoDefaultForField`, named by the Go test itself.
#[test]
fn drop_default_is_refused_rather_than_approximated() {
    let mut session = Session::new();
    session.run("CREATE TABLE ti (a INT)").unwrap();
    let error = session
        .run("ALTER TABLE ti ALTER COLUMN a DROP DEFAULT")
        .expect_err(
            "DROP DEFAULT now succeeds: replace this test with the captured Go answers above -- \
             `INSERT INTO ti VALUES ()` must be 1364 and SHOW CREATE must print `a` with no \
             DEFAULT clause",
        );
    assert!(
        format!("{error:?}").contains("NoDefaultValueFlag"),
        "refused for an unexpected reason: {error:?}"
    );
    // The CONTROL: a default that was never written still behaves, so the
    // refusal above is about DROP DEFAULT and not about the column.
    session.run("INSERT INTO ti VALUES ()").unwrap();
    assert_eq!(
        rows(&mut session, "SELECT a FROM ti"),
        vec![vec!["NULL".to_owned()]]
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
