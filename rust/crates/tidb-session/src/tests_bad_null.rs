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

//! A NULL written into a `NOT NULL` column, end to end over a real session,
//! in BOTH SQL modes.
//!
//! The engine half is `tidb_executor::bad_null`, which mirrors Go
//! `pkg/table/column.go`'s `HandleBadNull` / `GetZeroValue` and the per
//! statement kind level `pkg/executor/select.go` derives. These tests exist
//! because the two modes fail in OPPOSITE directions: under the default
//! strict mode the bug was a silently stored NULL, and a fix that merely
//! errored everywhere would have turned four statements TiDB accepts under
//! `sql_mode = ''` into errors. So every case below is asserted twice.
//!
//! Captured from real TiDB (mock store, `SHOW WARNINGS` read after each
//! statement) before any of it was written down:
//!
//! ```text
//! -- default sql_mode
//! insert into s5 values (NULL)      ERR 1048 Column 'a' cannot be null
//! insert into s5 () values ()       ERR 1364 Field 'a' doesn't have a default value
//! update s6 set a = NULL            ERR 1048 Column 'a' cannot be null
//! insert into m4 values (NULL),(2)  ERR 1048 Column 'a' cannot be null
//!
//! -- sql_mode = ''
//! insert into n5 values (NULL)      ERR 1048 Column 'a' cannot be null
//! insert into n5 () values ()       OK   WARN 1364          stores 0
//! update n6 set a = NULL            OK   WARN 1048          stores 0
//! insert into m1 values (NULL,1),(2,2)
//!                                   OK   WARN 1048          stores 0|1, 2|2
//! update m2 set a=NULL,b=NULL,c=NULL,d=NULL   -- varchar(5), date, decimal(6,2), double
//!                                   OK   WARN 1048 x4       stores ''|0000-00-00|0.00|0
//! ```
//!
//! The single-row INSERT staying an ERROR under `sql_mode = ''` is not an
//! oversight: it is MySQL's own "for single-row inserts, ignore non-strict
//! mode" rule, which Go spells as `strictSQLMode || len(stmt.Lists) == 1`.
//! The multi-row insert two lines below it is the control that proves the
//! rule is the row count and not the mode.

use super::Session;
use crate::tests_support::row_text;

/// The warnings the last statement left, as `(code, message)`.
fn warnings(session: &Session) -> Vec<(u16, String)> {
    session
        .warnings()
        .iter()
        .map(|w| (w.code, w.message.clone()))
        .collect()
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// Under the default (strict) mode every NOT NULL violation fails the
/// statement and stores nothing.
#[test]
fn strict_mode_refuses_every_bad_null() {
    let mut session = Session::new();
    session.run("CREATE TABLE s5 (a INT NOT NULL)").unwrap();

    // insert into s5 values (NULL) -> ERR 1048
    let error = session
        .run("INSERT INTO s5 VALUES (NULL)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1048, "Column 'a' cannot be null")
    );

    // insert into s5 () values () -> ERR 1364
    let error = session
        .run("INSERT INTO s5 () VALUES ()")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1364, "Field 'a' doesn't have a default value")
    );

    // insert into m4 values (NULL),(2) -> ERR 1048 even though it is multi-row
    let error = session
        .run("INSERT INTO s5 VALUES (NULL),(2)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1048, "Column 'a' cannot be null")
    );

    assert!(rows(&mut session, "SELECT a FROM s5").is_empty());
}

/// The UPDATE half of the same rule: this is the case that silently stored a
/// NULL into a NOT NULL column before `handle_bad_null` reached the update
/// path at all.
#[test]
fn strict_mode_refuses_an_update_to_null() {
    let mut session = Session::new();
    session.run("CREATE TABLE s6 (a INT NOT NULL)").unwrap();
    session.run("INSERT INTO s6 VALUES (7)").unwrap();

    let error = session
        .run("UPDATE s6 SET a = NULL")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1048, "Column 'a' cannot be null")
    );
    // The row is untouched: a refused UPDATE writes nothing.
    assert_eq!(rows(&mut session, "SELECT a FROM s6"), [["7"]]);
}

/// THE CONTROL. Under `sql_mode = ''` these statements are ACCEPTED, and a
/// fix that errored regardless of mode would turn every one of them red.
/// The stored VALUE is asserted too -- "no error" is not the contract, the
/// zero value is.
#[test]
fn non_strict_mode_warns_and_stores_the_type_zero() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("CREATE TABLE n5 (a INT NOT NULL, b INT NOT NULL DEFAULT 3)")
        .unwrap();

    // insert into n5 (b) values (9) -> accepted, WARN 1364, stores 0|9
    session.run("INSERT INTO n5 (b) VALUES (9)").unwrap();
    assert_eq!(
        warnings(&session),
        [(1364, "Field 'a' doesn't have a default value".to_owned())]
    );
    assert_eq!(rows(&mut session, "SELECT a,b FROM n5"), [["0", "9"]]);

    // update n5 set a = NULL -> accepted, WARN 1048, stores 0
    session.run("UPDATE n5 SET a = NULL").unwrap();
    assert_eq!(
        warnings(&session),
        [(1048, "Column 'a' cannot be null".to_owned())]
    );
    assert_eq!(rows(&mut session, "SELECT a,b FROM n5"), [["0", "9"]]);
}

/// The row count, not the SQL mode, is what separates these two INSERTs.
#[test]
fn non_strict_mode_separates_a_single_row_insert_from_a_multi_row_one() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session
        .run("CREATE TABLE m1 (a INT NOT NULL, b INT)")
        .unwrap();

    // Single row: still an ERROR under `sql_mode = ''`.
    let error = session
        .run("INSERT INTO m1 VALUES (NULL, 1)")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1048, "Column 'a' cannot be null")
    );

    // Two rows: accepted, one warning, and the NULL becomes 0.
    session.run("INSERT INTO m1 VALUES (NULL,1),(2,2)").unwrap();
    assert_eq!(
        warnings(&session),
        [(1048, "Column 'a' cannot be null".to_owned())]
    );
    assert_eq!(
        rows(&mut session, "SELECT a,b FROM m1 ORDER BY b"),
        [["0", "1"], ["2", "2"]]
    );
}

/// `GetZeroValue` is per TYPE: the substitute is the column's own zero, and
/// a DECIMAL keeps its declared scale.
#[test]
fn the_substituted_zero_has_the_columns_own_type() {
    let mut session = Session::new();
    session.run("SET sql_mode=''").unwrap();
    session
        .run(
            "CREATE TABLE m2 (a VARCHAR(5) NOT NULL, b DATE NOT NULL, \
             c DECIMAL(6,2) NOT NULL, d DOUBLE NOT NULL)",
        )
        .unwrap();
    session
        .run("INSERT INTO m2 VALUES ('x','2020-01-01',1.5,2.5)")
        .unwrap();

    session
        .run("UPDATE m2 SET a=NULL, b=NULL, c=NULL, d=NULL")
        .unwrap();
    assert_eq!(
        warnings(&session),
        [
            (1048, "Column 'a' cannot be null".to_owned()),
            (1048, "Column 'b' cannot be null".to_owned()),
            (1048, "Column 'c' cannot be null".to_owned()),
            (1048, "Column 'd' cannot be null".to_owned()),
        ]
    );
    assert_eq!(
        rows(&mut session, "SELECT a,b,c,d FROM m2"),
        [["", "0000-00-00", "0.00", "0"]]
    );
}
