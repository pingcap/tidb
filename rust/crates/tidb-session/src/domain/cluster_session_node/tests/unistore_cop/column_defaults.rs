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

//! The values a column supplies when a statement does not -- DEFAULT, NOT
//! NULL rejection, AUTO_INCREMENT, generated columns -- as the executor's
//! in-memory tier used to check them, over the cop-backed TestKit stack (X11).

use super::*;

fn session(stack: &UnistoreClusterStack, connection: u64) -> Session {
    let mut session = stack.factory.open_session(session_context(connection)).expect("session opens");
    rows(&mut session, "use test");
    session
}

fn q(tk: &mut Session, sql: &str) -> Vec<Vec<String>> {
    displayed(rows(tk, sql))
}

fn error(tk: &mut Session, sql: &str) -> tidb_store_driver_error::sql_query_error::SqlQueryError {
    tk.node_conn().execute(sql).map(|_| ()).expect_err(sql)
}

/// Go `fillColValue` and `CheckNotNull`: an omitted column takes its DEFAULT,
/// an omitted NOT NULL column without one is 1364, an explicit NULL into it is
/// 1048, DEFAULT NULL is omittable, and AUTO_INCREMENT and generated columns
/// supply their own values.
#[test]
fn column_defaults_and_not_null() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 510);
    rows(&mut tk, "CREATE TABLE d (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, w BIGINT DEFAULT 7, s VARCHAR(4) DEFAULT 'zz', plain BIGINT)");
    rows(&mut tk, "INSERT INTO d (id, n) VALUES (1, 5)");
    assert_eq!(q(&mut tk, "SELECT w, s, plain FROM d WHERE id = 1"), [["7", "zz", "NULL"]]);
    rows(&mut tk, "INSERT INTO d (id, n, w) VALUES (2, 5, 100)");
    assert_eq!(q(&mut tk, "SELECT w FROM d WHERE id = 2"), [["100"]]);
    let missing = error(&mut tk, "INSERT INTO d (id) VALUES (3)");
    assert_eq!((missing.code, missing.message.as_str()), (1364, "Field 'n' doesn't have a default value"));
    let null = error(&mut tk, "INSERT INTO d (id, n) VALUES (3, NULL)");
    assert_eq!((null.code, null.message.as_str()), (1048, "Column 'n' cannot be null"));
    rows(&mut tk, "INSERT INTO d (id, n, plain) VALUES (3, 5, NULL)");
    rows(&mut tk, "CREATE TABLE e (id BIGINT PRIMARY KEY, v BIGINT DEFAULT NULL)");
    rows(&mut tk, "INSERT INTO e (id) VALUES (1)");
    assert_eq!(q(&mut tk, "SELECT v FROM e"), [["NULL"]]);
    assert_eq!(error(&mut tk, "INSERT INTO e (v) VALUES (1)").code, 1364);
    rows(&mut tk, "CREATE TABLE f (a BIGINT AUTO_INCREMENT, KEY (a))");
    rows(&mut tk, "INSERT INTO f () VALUES ()");
    assert_eq!(q(&mut tk, "SELECT a FROM f"), [["1"]]);
    rows(&mut tk, "CREATE TABLE g2 (a BIGINT, b BIGINT GENERATED ALWAYS AS (a+1) VIRTUAL)");
    rows(&mut tk, "INSERT INTO g2 (a) VALUES (5)");
    assert_eq!(q(&mut tk, "SELECT a, b FROM g2"), [["5", "6"]]);
}

/// A bare assignment DEFAULT is the target column's default value; for a
/// generated column it keeps the generation expression as the value source.
#[test]
fn explicit_default_uses_the_column_value_source() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 511);
    rows(
        &mut tk,
        "CREATE TABLE explicit_default (id BIGINT PRIMARY KEY, v BIGINT DEFAULT 7, n BIGINT DEFAULT NULL, g BIGINT GENERATED ALWAYS AS (v + 1) VIRTUAL)",
    );
    rows(&mut tk, "INSERT INTO explicit_default (id, v, n, g) VALUES (1, DEFAULT, DEFAULT, DEFAULT)");
    assert_eq!(q(&mut tk, "SELECT id, v, n, g FROM explicit_default"), [["1", "7", "NULL", "8"]]);
    rows(&mut tk, "UPDATE explicit_default SET v = 20, g = DEFAULT WHERE id = 1");
    assert_eq!(q(&mut tk, "SELECT v, g FROM explicit_default"), [["20", "21"]]);
    rows(&mut tk, "UPDATE explicit_default SET v = DEFAULT, n = DEFAULT, g = DEFAULT WHERE id = 1");
    assert_eq!(q(&mut tk, "SELECT id, v, n, g FROM explicit_default"), [["1", "7", "NULL", "8"]]);
}
