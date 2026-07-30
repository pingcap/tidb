//! `INSERT` in its several forms, its conflict policies, and the casting
//! and defaulting a write does on the way into a column -- Go
//! `pkg/executor/insert.go`.

use crate::tests_support::*;
use crate::*;

/// `INSERT ... SET col = value`, checked against captured TiDB output.
///
/// Go normalizes the `SET` list into a column list plus one VALUES row,
/// so every rule the VALUES form obeys -- defaults, NOT NULL, the column
/// cast, ON DUPLICATE KEY UPDATE and REPLACE -- applies unchanged.
#[test]
fn insert_set_syntax() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10) DEFAULT 'dd', \
                 c BIGINT NOT NULL DEFAULT 5)",
        )
        .unwrap();

    // Captured: the columns it names are assigned, the rest take their
    // defaults.
    assert_eq!(
        session.run("INSERT INTO t SET a = 1, b = 'x'").unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(row_text(session.run("SELECT * FROM t")), [["1", "x", "5"]]);
    session.run("INSERT INTO t SET a = 2").unwrap();
    // Captured: an assigned value may be an expression.
    session.run("INSERT INTO t SET a = 3, c = 1+1").unwrap();
    assert_eq!(
        row_text(session.run("SELECT * FROM t ORDER BY a")),
        [["1", "x", "5"], ["2", "dd", "5"], ["3", "dd", "2"]]
    );

    // Captured: a column with no default that the SET list omits is
    // 1364, the same as in the VALUES form.
    match session.run("INSERT INTO t SET b = 'nope'") {
        Err(error) => assert_eq!(error.to_mysql_error().code, 1364),
        Ok(other) => panic!("expected 1364, got {other:?}"),
    }
    // Captured: an unknown column names the field list.
    match session.run("INSERT INTO t SET nosuch = 1") {
        Err(error) => assert_eq!(error.to_mysql_error().code, 1054),
        Ok(other) => panic!("expected 1054, got {other:?}"),
    }

    // Captured: the conflict policies compose with it.
    assert_eq!(
        session
            .run("INSERT INTO t SET a = 1, b = 'dup' ON DUPLICATE KEY UPDATE b = 'updated'")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT b FROM t WHERE a = 1")),
        [["updated"]]
    );
    assert_eq!(
        session.run("REPLACE INTO t SET a = 2, b = 'repl'").unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
        [["1", "updated", "5"], ["2", "repl", "5"], ["3", "dd", "2"]]
    );
}

/// The three conflict policies -- `REPLACE`, `INSERT IGNORE` and
/// `ON DUPLICATE KEY UPDATE` -- checked against captured TiDB output,
/// including the affected-row counts, which is how MySQL clients tell
/// an insert from an update.
#[test]
fn insert_conflict_policies() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT, UNIQUE KEY ub (b))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'p',10),(2,'q',20)")
        .unwrap();

    // Captured: an update that changes nothing affects no rows, and
    // raises no warning.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b,c) VALUES (1,'p',10) ON DUPLICATE KEY UPDATE c = c")
            .unwrap(),
        StmtResult::Affected(0)
    );
    assert!(session.warnings().is_empty());

    // Captured: VALUES(c) is the value the insert would have written, and
    // a real update affects two rows.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b,c) VALUES (1,'p',77) ON DUPLICATE KEY UPDATE c = VALUES(c)")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT c FROM t WHERE a = 1")),
        [["77"]]
    );

    // Captured: the conflict is found on a UNIQUE INDEX too, and the
    // assignment updates THAT row -- the candidate's own key is never
    // inserted.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b,c) VALUES (9,'q',5) ON DUPLICATE KEY UPDATE c = 42")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
        [["1", "p", "77"], ["2", "q", "42"]]
    );

    // Captured: the assignments read the EXISTING row.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b,c) VALUES (1,'p',1000) ON DUPLICATE KEY UPDATE c = c + 1")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT c FROM t WHERE a = 1")),
        [["78"]]
    );

    // Captured: INSERT IGNORE skips the conflicting row with a 1062
    // warning and inserts the rest.
    assert_eq!(
        session
            .run("INSERT IGNORE INTO t (a,b,c) VALUES (1,'zzz',1),(5,'five',5)")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1062);
    assert_eq!(
        session.warnings()[0].message,
        "Duplicate entry '1' for key 't.PRIMARY'"
    );

    // Captured: REPLACE deletes EVERY row it collides with -- here one on
    // the primary key and another on the unique key -- and the affected
    // count is one per deleted row plus one for the inserted row.
    assert_eq!(
        session
            .run("REPLACE INTO t (a,b,c) VALUES (2,'five',99)")
            .unwrap(),
        StmtResult::Affected(3)
    );
    assert_eq!(
        row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
        [["1", "p", "78"], ["2", "five", "99"]]
    );
    // Captured: a REPLACE with no conflict is a plain insert.
    assert_eq!(
        session
            .run("REPLACE INTO t (a,b,c) VALUES (77,'new',1)")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        [["1"], ["2"], ["77"]]
    );
}

/// `INSERT ... SELECT` and the `ORDER BY`/`LIMIT` forms of UPDATE and
/// DELETE, checked against captured TiDB output.
///
/// STILL REFUSED, each recorded at its gate: `REPLACE INTO`,
/// `INSERT IGNORE`, `ON DUPLICATE KEY UPDATE` (all three need
/// conflict-time row replacement), the `SET` insert syntax, and
/// partitions. `RETURNING` is parsed and silently ignored, matching Go
/// (testkit probe: the write succeeds with a plain OK, no result set,
/// no warning).
#[test]
fn insert_select_and_ordered_dml() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(10), c BIGINT)")
        .unwrap();
    session
        .run("CREATE TABLE u (x BIGINT, y VARCHAR(10))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'p',10),(2,'q',20),(3,'r',30)")
        .unwrap();
    session
        .run("INSERT INTO u VALUES (7,'seven'),(8,'eight')")
        .unwrap();

    // Captured: INSERT ... SELECT inserts the query's rows, and the
    // columns it does not name stay NULL.
    assert_eq!(
        session
            .run("INSERT INTO t (a,b) SELECT x, y FROM u")
            .unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT a, b, c FROM t ORDER BY a")),
        [
            ["1", "p", "10"],
            ["2", "q", "20"],
            ["3", "r", "30"],
            ["7", "seven", "NULL"],
            ["8", "eight", "NULL"],
        ]
    );

    // Captured: UPDATE ... ORDER BY ... LIMIT updates that many rows, in
    // that order -- here the largest `a`.
    assert_eq!(
        session
            .run("UPDATE t SET c = 99 ORDER BY a DESC LIMIT 1")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a, c FROM t ORDER BY a")),
        [
            ["1", "10"],
            ["2", "20"],
            ["3", "30"],
            ["7", "NULL"],
            ["8", "99"],
        ]
    );

    // Captured: DELETE ... ORDER BY ... LIMIT, and the WHERE + LIMIT form
    // whose cap counts rows DELETED rather than rows examined.
    assert_eq!(
        session
            .run("DELETE FROM t ORDER BY a DESC LIMIT 1")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        [["1"], ["2"], ["3"], ["7"]]
    );
    assert_eq!(
        session.run("DELETE FROM t WHERE c > 0 LIMIT 2").unwrap(),
        StmtResult::Affected(2)
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        [["3"], ["7"]]
    );

    // RETURNING parses but is silently ignored, exactly as in Go: the
    // planner and executor never read the AST's Returning list, so the
    // write lands and answers with a plain OK (affected rows), no result
    // set and no warning. Captured with a Go testkit probe.
    assert_eq!(
        session
            .run("INSERT INTO t (a) VALUES (42) RETURNING a")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        session
            .run("UPDATE t SET c = 0 WHERE a = 42 RETURNING a, c")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        session
            .run("DELETE FROM t WHERE a = 42 RETURNING a")
            .unwrap(),
        StmtResult::Affected(1)
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t ORDER BY a")),
        [["3"], ["7"]]
    );
}

/// Go `buildValuesListOfInsert`: `INSERT t VALUES ()` is a row of nothing
/// but defaults, legal only while BOTH the column list and the first value
/// list are empty.
///
/// Captured from TiDB (`testkit`, `pkg/executor`):
///
/// ```text
/// create table ev1 (a int default 7, b varchar(4) default 'zz', c int)
/// insert into ev1 values ()          OK
/// select * from ev1                  [[7 zz <nil>]]
/// insert into ev1 (a) values ()      ERR errno=1136 "[planner:1136]Column count
///                                        doesn't match value count at row 1"
/// insert into ev1 values (), ()      OK   -- select count(*) -> [[3]]
///
/// create table ev2 (a int not null, b int)
/// insert into ev2 values ()          ERR errno=1364 "[table:1364]Field 'a'
///                                        doesn't have a default value"
/// select * from ev2                  []
///
/// create table ev3 (id int auto_increment primary key, v int)
/// insert into ev3 values () x2       OK
/// select * from ev3                  [[1 <nil>] [2 <nil>]]
/// select last_insert_id()            [[2]]
/// ```
#[test]
fn insert_with_an_empty_values_row_takes_every_default() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ev1 (a INT DEFAULT 7, b VARCHAR(4) DEFAULT 'zz', c INT)")
        .unwrap();
    session.run("INSERT INTO ev1 VALUES ()").unwrap();
    assert_eq!(
        row_text(session.run("SELECT a, b, c FROM ev1")),
        [["7", "zz", "NULL"]]
    );

    // A named column list still demands a matching arity: only the pairing of
    // "no columns" with "no values" is the all-defaults row.
    assert!(session.run("INSERT INTO ev1 (a) VALUES ()").is_err());

    // Every later row must match the first, so `(), ()` is two default rows.
    session.run("INSERT INTO ev1 VALUES (), ()").unwrap();
    assert_eq!(row_text(session.run("SELECT COUNT(*) FROM ev1")), [["3"]]);
    // ... while mixing widths is not a row of defaults at all.
    assert!(session
        .run("INSERT INTO ev1 VALUES (), (1, 'q', 2)")
        .is_err());
    assert!(session
        .run("INSERT INTO ev1 VALUES (1, 'q', 2), ()")
        .is_err());

    // A NOT NULL column with no default has no value to take, which is the
    // ordinary 1364 -- not an arity error.
    session
        .run("CREATE TABLE ev2 (a INT NOT NULL, b INT)")
        .unwrap();
    let error = session.run("INSERT INTO ev2 VALUES ()").unwrap_err();
    assert!(
        matches!(&error, DriverError::NoDefaultForField(name) if name == "a"),
        "expected 1364 for field a, got {error:?}"
    );
    let rendered = error.to_mysql_error();
    assert_eq!(rendered.code, 1364);
    assert_eq!(rendered.message, "Field 'a' doesn't have a default value");
    assert!(row_text(session.run("SELECT a FROM ev2")).is_empty());

    // The auto-increment column is allocated over the empty row, so the two
    // rows get 1 and 2 and LAST_INSERT_ID() reports the second.
    session
        .run("CREATE TABLE ev3 (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        .unwrap();
    session.run("INSERT INTO ev3 VALUES ()").unwrap();
    session.run("INSERT INTO ev3 VALUES ()").unwrap();
    assert_eq!(
        row_text(session.run("SELECT id, v FROM ev3 ORDER BY id")),
        [["1", "NULL"], ["2", "NULL"]]
    );
    assert_eq!(row_text(session.run("SELECT LAST_INSERT_ID()")), [["2"]]);
}

/// Go `table.CastValue`: a written value takes its column's type, checked
/// against captured TiDB output.
///
/// NOT PORTED from Go's own suites: the temporal columns (a DATE/DATETIME
/// column's zero-date handling is its own error path), ENUM/SET, and the
/// `INSERT IGNORE` form, which Go treats like a non-strict mode.
#[test]
fn insert_casts_to_column_type() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (d DECIMAL(10,3), i INT, v VARCHAR(4))")
        .unwrap();

    // Captured: a decimal rounds to the column's scale, a float rounds to
    // the integer column, and a numeric string parses.
    session
        .run("INSERT INTO t VALUES (1.23456, 7.6, 'ab')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT d, i, v FROM t")),
        [["1.235", "8", "ab"]]
    );
    assert!(session.warnings().is_empty());
    session.run("INSERT INTO t (i) VALUES ('12')").unwrap();
    assert_eq!(row_text(session.run("SELECT i FROM t")), [["8"], ["12"]]);

    // Captured: under the default strict mode a value that does not fit
    // fails the statement, and the row is not written.
    assert!(matches!(
        session.run("INSERT INTO t (v) VALUES ('abcdefg')"),
        Err(DriverError::DataTooLong { row: 1, .. })
    ));
    assert!(matches!(
        session.run("INSERT INTO t (i) VALUES ('x')"),
        Err(DriverError::IncorrectValue { row: 1, .. })
    ));
    assert_eq!(row_text(session.run("SELECT i FROM t")).len(), 2);
    // The failure is reported with Go's own message.
    match session.run("INSERT INTO t (i) VALUES ('x')") {
        Err(error) => {
            let reported = error.to_mysql_error();
            assert_eq!(reported.code, 1366);
            assert_eq!(
                reported.message,
                "Incorrect int value: 'x' for column 'i' at row 1"
            );
        }
        Ok(other) => panic!("expected a failure, got {other:?}"),
    }

    // Captured: UPDATE casts an assigned value the same way.
    session.run("UPDATE t SET d = 9.87654 WHERE i = 8").unwrap();
    assert_eq!(
        row_text(session.run("SELECT d FROM t WHERE i = 8")),
        [["9.877"]]
    );
    assert!(matches!(
        session.run("UPDATE t SET v = 'abcdefg' WHERE i = 8"),
        Err(DriverError::DataTooLong { .. })
    ));

    // Captured: without a strict mode the converted value is stored and
    // the same message is a warning -- the string truncates to the
    // column's width and an unparseable number becomes zero.
    session.apply_set("SET sql_mode = ''").unwrap();
    session.run("INSERT INTO t (v) VALUES ('abcdefg')").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1406);
    assert_eq!(
        session.warnings()[0].message,
        "Data too long for column 'v' at row 1"
    );
    session.run("INSERT INTO t (i) VALUES ('x')").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1366);
    assert_eq!(
        row_text(session.run("SELECT v FROM t")),
        [["ab"], ["NULL"], ["abcd"], ["NULL"]]
    );
}
