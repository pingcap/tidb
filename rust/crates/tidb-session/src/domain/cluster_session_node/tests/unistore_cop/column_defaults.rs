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

use super::sql_kit::*;
use super::*;

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

/// An explicit TIMESTAMP default is read in the zone the table was created
/// in and converted for the writing and reading sessions.
#[test]
fn explicit_timestamp_default_uses_the_writing_session_zone() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 512);
    exec_all(&mut tk, &[
        "SET time_zone = '+00:00'",
        "CREATE TABLE explicit_ts (id BIGINT PRIMARY KEY, ts TIMESTAMP DEFAULT '2020-01-02 00:00:00')",
        "SET time_zone = '+08:00'",
        "INSERT INTO explicit_ts (id, ts) VALUES (1, DEFAULT), (2, '2020-03-04 12:00:00')",
        "UPDATE explicit_ts SET ts = DEFAULT WHERE id = 2",
    ]);
    assert_eq!(q(&mut tk, "SELECT ts FROM explicit_ts ORDER BY id"), [["2020-01-02 08:00:00"], ["2020-01-02 08:00:00"]]);
    rows(&mut tk, "SET time_zone = '+00:00'");
    assert_eq!(q(&mut tk, "SELECT ts FROM explicit_ts ORDER BY id"), [["2020-01-02 00:00:00"], ["2020-01-02 00:00:00"]]);
}

/// `DEFAULT(col)` reads the named column's default, nested in expressions too.
#[test]
fn named_and_nested_defaults_use_the_referenced_column() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 513);
    exec_all(&mut tk, &[
        "CREATE TABLE named_default (id BIGINT PRIMARY KEY, a BIGINT DEFAULT 1, b BIGINT DEFAULT 2, c BIGINT DEFAULT 3)",
        "INSERT INTO named_default (id, a, b, c) VALUES (1, DEFAULT(b), DEFAULT(a) + DEFAULT(b), DEFAULT)",
        "INSERT INTO named_default (id, b) VALUES (2, DEFAULT(a))",
    ]);
    assert_eq!(q(&mut tk, "SELECT id, a, b, c FROM named_default ORDER BY id"), [["1", "2", "3", "3"], ["2", "1", "1", "3"]]);
    rows(&mut tk, "UPDATE named_default SET a = DEFAULT(b), b = DEFAULT(a), c = DEFAULT(a) + DEFAULT(b) WHERE id = 1");
    assert_eq!(q(&mut tk, "SELECT a, b, c FROM named_default WHERE id = 1"), [["2", "1", "3"]]);
}

/// Go lowers explicit DEFAULTs of a VALUES list before the row's runtime
/// expressions: both `rand()` defaults draw before both `RAND()` values.
#[test]
fn explicit_values_defaults_are_lowered_before_runtime_expressions() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 514);
    rows(&mut tk, "CREATE TABLE default_rng (a DOUBLE DEFAULT (rand()), b DOUBLE)");
    rows(&mut tk, "SET @@rand_seed1 = 1, @@rand_seed2 = 1");
    let draws = q(&mut tk, "SELECT rand() FROM (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) x");
    rows(&mut tk, "SET @@rand_seed1 = 1, @@rand_seed2 = 1");
    rows(&mut tk, "INSERT INTO default_rng VALUES (DEFAULT, RAND()), (DEFAULT, RAND())");
    assert_eq!(
        q(&mut tk, "SELECT a, b FROM default_rng"),
        [[draws[0][0].clone(), draws[2][0].clone()], [draws[1][0].clone(), draws[3][0].clone()]]
    );
}

/// Go `ErrBadGeneratedColumn` (3105) for every assignment of a generated
/// column other than DEFAULT, checked before any row is written; `DEFAULT(g)`
/// assigned to an ordinary column is NULL.
#[test]
fn generated_default_identity_matrix_and_on_duplicate_timing() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 515);
    exec_all(&mut tk, &[
        "CREATE TABLE generated_default (id BIGINT PRIMARY KEY, a BIGINT DEFAULT 5, g BIGINT GENERATED ALWAYS AS (a + 1) VIRTUAL)",
        "INSERT INTO generated_default VALUES (1, DEFAULT, DEFAULT), (2, DEFAULT, DEFAULT(g))",
    ]);
    for sql in [
        "INSERT INTO generated_default (id, g) VALUES (3, DEFAULT(a))",
        "INSERT INTO generated_default (id, a) VALUES (3, DEFAULT(g))",
        "INSERT INTO generated_default (id, a) VALUES (3, 8) ON DUPLICATE KEY UPDATE g = DEFAULT(a)",
        "UPDATE generated_default SET g = DEFAULT(a) WHERE id = 1",
    ] {
        assert_eq!(error(&mut tk, sql).code, 3105, "{sql}");
    }
    assert!(q(&mut tk, "SELECT id FROM generated_default WHERE id = 3").is_empty());
    rows(&mut tk, "UPDATE generated_default SET g = DEFAULT(g) WHERE id = 1");
    rows(&mut tk, "UPDATE generated_default SET a = DEFAULT(g) WHERE id = 2");
    assert_eq!(q(&mut tk, "SELECT a, g FROM generated_default WHERE id = 2"), [["NULL", "NULL"]]);
}

/// `DEFAULT(col)` in ON DUPLICATE KEY UPDATE, and an unknown column there is
/// 1054 even when no row conflicts.
#[test]
fn on_duplicate_defaults_cover_conflict_and_no_conflict() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 516);
    exec_all(&mut tk, &[
        "CREATE TABLE duplicate_default (id BIGINT PRIMARY KEY, a BIGINT DEFAULT 7, b BIGINT DEFAULT 9)",
        "INSERT INTO duplicate_default VALUES (1, 0, 0)",
        "INSERT INTO duplicate_default VALUES (1, 1, 2) ON DUPLICATE KEY UPDATE a = DEFAULT(b), b = DEFAULT(a) + VALUES(b)",
    ]);
    assert_eq!(q(&mut tk, "SELECT a, b FROM duplicate_default WHERE id = 1"), [["9", "9"]]);
    assert_eq!(error(&mut tk, "INSERT INTO duplicate_default VALUES (2, 1, 2) ON DUPLICATE KEY UPDATE a = DEFAULT(no_such_column)").code, 1054);
    assert!(q(&mut tk, "SELECT id FROM duplicate_default WHERE id = 2").is_empty());
}

/// An explicit DEFAULT on a NOT NULL column without one: 1364 in strict
/// mode, a 1364 warning and the zero value (an ENUM's first member) otherwise;
/// UPDATE ... SET n = DEFAULT is 1364 even when it matches no row.
#[test]
fn explicit_no_default_and_not_null_enum_follow_go_order() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 517);
    rows(&mut tk, "CREATE TABLE nil_default (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, e ENUM('first','second') NOT NULL)");
    let strict = error(&mut tk, "INSERT INTO nil_default (id, n, e) VALUES (1, DEFAULT, DEFAULT)");
    assert_eq!((strict.code, strict.message.as_str()), (1364, "Field 'n' doesn't have a default value"));
    rows(&mut tk, "SET sql_mode = ''");
    rows(&mut tk, "INSERT INTO nil_default (id, n, e) VALUES (1, DEFAULT, DEFAULT)");
    assert_eq!(warning_codes(&mut tk).iter().filter(|code| *code == "1364").count(), 1);
    assert_eq!(q(&mut tk, "SELECT n, e FROM nil_default"), [["0", "first"]]);
    rows(&mut tk, "SET sql_mode = DEFAULT");
    for sql in ["UPDATE nil_default SET n = DEFAULT WHERE id = 999", "UPDATE nil_default SET n = DEFAULT WHERE id > 998"] {
        let refused = error(&mut tk, sql);
        assert_eq!((refused.code, refused.message.as_str()), (1364, "Field 'n' doesn't have a default value"), "{sql}");
    }
}

/// A multi-table UPDATE resolves `DEFAULT(t.col)` against the named source.
#[test]
fn multi_update_defaults_resolve_across_sources() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 518);
    exec_all(&mut tk, &[
        "CREATE TABLE default_left (id BIGINT PRIMARY KEY, v BIGINT DEFAULT 11)",
        "CREATE TABLE default_right (id BIGINT PRIMARY KEY, v BIGINT DEFAULT 22)",
        "INSERT INTO default_left VALUES (1, 100)",
        "INSERT INTO default_right VALUES (1, 200)",
    ]);
    assert_eq!(affected(&mut tk, "UPDATE default_left l JOIN default_right r ON l.id = r.id SET l.v = DEFAULT(r.v), r.v = DEFAULT(l.v)"), 2);
    assert_eq!(q(&mut tk, "SELECT l.v, r.v FROM default_left l JOIN default_right r ON l.id = r.id"), [["22", "11"]]);
}

/// INSERT ... SELECT into a generated column is 3105 before the SELECT's
/// assignments run.
#[test]
fn insert_select_generated_target_is_rejected_before_source_effects() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 519);
    exec_all(&mut tk, &["CREATE TABLE select_default (a BIGINT, g BIGINT GENERATED ALWAYS AS (a + 1) VIRTUAL)", "SET @probe = 0"]);
    assert_eq!(error(&mut tk, "INSERT INTO select_default (g) SELECT @probe := 1").code, 3105);
    assert_eq!(q(&mut tk, "SELECT @probe"), [["0"]]);
}

/// AUTO_INCREMENT: allocated ids, an explicit id rebasing the allocator, NULL
/// and 0 allocating, a non-key auto column, two auto columns (1075) and a
/// string one (Go's "Incorrect column specifier").
#[test]
fn auto_increment() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 520);
    exec_all(&mut tk, &[
        "CREATE TABLE a1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)",
        "INSERT INTO a1 (v) VALUES (10), (20)",
        "INSERT INTO a1 VALUES (100, 30)",
        "INSERT INTO a1 (v) VALUES (40)",
        "INSERT INTO a1 VALUES (NULL, 50), (0, 60)",
    ]);
    assert_eq!(
        q(&mut tk, "SELECT id, v FROM a1"),
        [["1", "10"], ["2", "20"], ["100", "30"], ["101", "40"], ["102", "50"], ["103", "60"]]
    );
    exec_all(&mut tk, &["CREATE TABLE bad (a BIGINT AUTO_INCREMENT, b BIGINT)", "INSERT INTO bad (b) VALUES (1), (2)"]);
    assert_eq!(q(&mut tk, "SELECT a FROM bad"), [["1"], ["2"]]);
    assert_eq!(error(&mut tk, "CREATE TABLE two (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT AUTO_INCREMENT)").code, 1075);
    let refused = error(&mut tk, "CREATE TABLE strk (a VARCHAR(4) AUTO_INCREMENT PRIMARY KEY)");
    assert_eq!((refused.code, refused.message.as_str()), (1105, "Incorrect column specifier for column 'a'"));
}
