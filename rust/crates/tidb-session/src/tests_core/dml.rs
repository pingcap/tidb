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

/// Go `ResetContextOfStmt`'s `*ast.InsertStmt` arm plus
/// `util.GetTypeFlagsForInsert`: the `IGNORE` modifier makes every
/// value-level failure a WARNING and stores the coerced value, under the
/// DEFAULT strict mode.
///
/// Captured from TiDB (`pkg/session` over `mockstore`, default `sql_mode`).
/// The stored values and the warning codes are BOTH asserted, because the
/// bug this pins was visible only as "eight later reads return zero rows":
/// a statement that must have been accepted was refused, so a test that
/// only checked "no error" would have been satisfied by an empty table.
///
/// ```text
/// insert ignore into tb values(-1,-1),(0,0),(1,1),(3,3);
///   1406 Data too long for column 'a' at row 1 / at row 4; a+0 = 1,0,1,1
/// insert ignore into tt(a tinyint) values(1000);   1264, stores 127
/// insert ignore into ti(a int) values('12abc');    1366, stores 12
/// insert ignore into td(a date) values('2020-13-45'); 1292, stores 0000-00-00
/// insert ignore into nn(a int not null) values(null); 1048, stores 0
/// ```
#[test]
fn insert_ignore_downgrades_a_value_error_to_a_warning() {
    let mut session = Session::new();
    session.run("CREATE TABLE tb (a BIT(1), b INT)").unwrap();
    session
        .run("INSERT IGNORE INTO tb VALUES (-1,-1),(0,0),(1,1),(3,3)")
        .unwrap();
    // Both out-of-range rows warn; the two that fit stay silent.
    let warnings = session.warnings();
    assert_eq!(warnings.len(), 2);
    assert_eq!(warnings[0].code, 1406);
    assert_eq!(warnings[0].message, "Data too long for column 'a' at row 1");
    assert_eq!(warnings[1].code, 1406);
    assert_eq!(warnings[1].message, "Data too long for column 'a' at row 4");
    // The CLAMPED values are stored -- all four rows, in order.
    assert_eq!(
        row_text(session.run("SELECT a+0, b FROM tb ORDER BY b")),
        [["1", "-1"], ["0", "0"], ["1", "1"], ["1", "3"]]
    );

    // Every other value-level group behaves the same way, and each is
    // paired with the plain INSERT that must KEEP failing: a fix that
    // turned the strict mode permissive would pass the first half alone.
    session.run("CREATE TABLE tt (a TINYINT)").unwrap();
    session.run("INSERT IGNORE INTO tt VALUES (1000)").unwrap();
    assert_eq!(session.warnings()[0].code, 1264);
    assert_eq!(row_text(session.run("SELECT a FROM tt")), [["127"]]);
    assert!(matches!(
        session.run("INSERT INTO tt VALUES (1000)"),
        Err(DriverError::DataOutOfRange { row: 1, .. })
    ));

    session.run("CREATE TABLE ti (a INT)").unwrap();
    session
        .run("INSERT IGNORE INTO ti VALUES ('12abc')")
        .unwrap();
    assert_eq!(session.warnings()[0].code, 1366);
    assert_eq!(row_text(session.run("SELECT a FROM ti")), [["12"]]);
    assert!(matches!(
        session.run("INSERT INTO ti VALUES ('12abc')"),
        Err(DriverError::IncorrectValue { row: 1, .. })
    ));

    session.run("CREATE TABLE td (a DATE)").unwrap();
    session
        .run("INSERT IGNORE INTO td VALUES ('2020-13-45')")
        .unwrap();
    assert_eq!(session.warnings()[0].code, 1292);
    assert_eq!(row_text(session.run("SELECT a FROM td")), [["0000-00-00"]]);
    assert!(matches!(
        session.run("INSERT INTO td VALUES ('2020-13-45')"),
        Err(DriverError::IncorrectTemporalValue { row: 1, .. })
    ));

    // The bad-NULL group is the one rule `IGNORE` does not reach through
    // the strict flag: Go promotes a SINGLE-ROW insert to an error in every
    // SQL mode, and `IGNORE` overrides that promotion separately.
    session.run("CREATE TABLE nn (a INT NOT NULL)").unwrap();
    session.run("INSERT IGNORE INTO nn VALUES (NULL)").unwrap();
    assert_eq!(session.warnings()[0].code, 1048);
    assert_eq!(row_text(session.run("SELECT a FROM nn")), [["0"]]);
    assert!(matches!(
        session.run("INSERT INTO nn VALUES (NULL)"),
        Err(DriverError::ColumnCannotBeNull(_))
    ));
}

/// Go `convertToMysqlBit` returns `ErrDataTooLong` -- NOT the generic
/// "Incorrect bit value" -- when a value does not fit the declared width,
/// after clamping it to `(1<<flen)-1`.
///
/// Captured from TiDB: `INSERT INTO t(a BIT(1)) VALUES (-1)` under the
/// default strict mode is `1406 Data too long for column 'a' at row 1`, and
/// under `sql_mode = ''` the same message is a warning with `a+0 = 1`.
#[test]
fn a_bit_column_reports_an_overflow_as_data_too_long() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIT(1))").unwrap();
    match session.run("INSERT INTO t VALUES (-1)") {
        Err(error) => {
            let reported = error.to_mysql_error();
            assert_eq!(reported.code, 1406);
            assert_eq!(reported.message, "Data too long for column 'a' at row 1");
        }
        Ok(other) => panic!("expected a failure, got {other:?}"),
    }
    assert_eq!(row_text(session.run("SELECT a+0 FROM t")).len(), 0);

    session.apply_set("SET sql_mode = ''").unwrap();
    session.run("INSERT INTO t VALUES (-1)").unwrap();
    assert_eq!(session.warnings()[0].code, 1406);
    assert_eq!(row_text(session.run("SELECT a+0 FROM t")), [["1"]]);
}

/// `VALUES()` nested inside a larger expression. Go handles `*ast.ValuesExpr`
/// in its expression rewriter's `Enter`
/// (`pkg/planner/core/expression_rewriter.go:623`), which is driven by the
/// generic `Node.Accept` walk -- so the nesting depth and the surrounding
/// variant do not matter. A per-variant substitution here reached only
/// `Paren`/`Unary`/`Binary` and left `VALUES()` alive inside every function
/// call, `CASE`, `IN` and `BETWEEN`.
///
/// Captured from a real Go session (mockstore, `gorun`):
///
/// ```text
/// create table od (a int primary key, b int, c varchar(10));
/// insert into od values (1, 10, 'x');
/// insert into od values (1, 20, 'y')
///   on duplicate key update b = ifnull(values(b), 0) + 1;   -- 1|21|x
/// insert into od values (1, 30, 'z')
///   on duplicate key update b = case when values(b) > 5
///                               then values(b) * 2 else b end;   -- 1|60|x
/// insert into od values (1, 7, 'q')
///   on duplicate key update b = values(b) in (7, 8);        -- 1|1|x
/// insert into od values (1, 3, 'w')
///   on duplicate key update c = concat(values(c), '!');     -- 1|1|w!
/// insert into od values (1, 3, 'w')
///   on duplicate key update b = values(b) between 1 and 5;  -- 1|1|w!
/// ```
#[test]
fn on_duplicate_key_update_substitutes_nested_values_references() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE od (a INT PRIMARY KEY, b INT, c VARCHAR(10))")
        .unwrap();
    session.run("INSERT INTO od VALUES (1, 10, 'x')").unwrap();

    let expect = |session: &mut Session, sql: &str, row: [&str; 3]| {
        session.run(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(
            row_text(session.run("SELECT a, b, c FROM od")),
            [row.map(str::to_owned)],
            "{sql}"
        );
    };

    // Inside a function call.
    expect(
        &mut session,
        "INSERT INTO od VALUES (1, 20, 'y') ON DUPLICATE KEY UPDATE b = IFNULL(VALUES(b), 0) + 1",
        ["1", "21", "x"],
    );
    // Inside a CASE, twice, in two different clauses.
    expect(
        &mut session,
        "INSERT INTO od VALUES (1, 30, 'z') ON DUPLICATE KEY UPDATE \
         b = CASE WHEN VALUES(b) > 5 THEN VALUES(b) * 2 ELSE b END",
        ["1", "60", "x"],
    );
    // Inside an IN list's left-hand side.
    expect(
        &mut session,
        "INSERT INTO od VALUES (1, 7, 'q') ON DUPLICATE KEY UPDATE b = VALUES(b) IN (7, 8)",
        ["1", "1", "x"],
    );
    // A string function over a string column.
    expect(
        &mut session,
        "INSERT INTO od VALUES (1, 3, 'w') ON DUPLICATE KEY UPDATE c = CONCAT(VALUES(c), '!')",
        ["1", "1", "w!"],
    );
    // Inside a BETWEEN.
    expect(
        &mut session,
        "INSERT INTO od VALUES (1, 3, 'w') ON DUPLICATE KEY UPDATE \
         b = VALUES(b) BETWEEN 1 AND 5",
        ["1", "1", "w!"],
    );

    // Go scopes an unknown column inside VALUES() to the insert's field list:
    // 1054 "Unknown column 'zz' in 'field list'".
    let error = session
        .run("INSERT INTO od VALUES (1, 3, 'w') ON DUPLICATE KEY UPDATE b = ABS(VALUES(zz))")
        .expect_err("zz is not a column")
        .to_mysql_error();
    assert_eq!(error.code, 1054);
    assert_eq!(error.message, "Unknown column 'zz' in 'field list'");
}

/// Go `doDupRowUpdate`'s assignment cast (`pkg/executor/insert.go:495-521`)
/// calls its error handler AFTER `table.CastValue`, so the warnings the cast
/// produced are re-titled over the ALREADY-CAST value:
///
/// ```text
/// _ = errorHandler(sctx, assign, &val, nil)   // val is the cast result
/// ```
///
/// The VALUES-row path calls `InsertValues.handleErr` BEFORE the cast, which
/// is why the same bad text is named `'abc'` there and `'0'` here.
///
/// Captured with `gorunmsg` under `sql_mode = ''`, on a two-row batch so the
/// per-row index is visible:
///
/// ```text
/// insert into o values (1,9),(2,9) on duplicate key update b='abc';
/// show warnings;
///   Warning 1366 Incorrect int value: '0' for column 'b' at row 1
///   Warning 1366 Incorrect int value: '0' for column 'b' at row 2
/// select a,b from o;  -> 1|0; 2|0
/// ```
///
/// This port named the SOURCE text and hardcoded row 1 for every row of the
/// batch.
///
/// NOT FIXED, and pinned nowhere because pinning a wrong answer records it as
/// expected: the STRICT spelling. Go returns `table.CastValue`'s error
/// UNWRAPPED there -- `[types:1292] Truncated incorrect DOUBLE value: 'abc'`,
/// with no column and no row and a different code from the 1366 the insert
/// spelling raises. See `tidb_executor::driver::dml`'s
/// `cast_value_for_assignment` for why.
#[test]
fn an_on_duplicate_assignment_warns_over_the_cast_value_and_its_own_row() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE o (a INT PRIMARY KEY, b INT, d DATETIME)")
        .unwrap();
    session
        .run("INSERT INTO o VALUES (1,1,'2020-01-01'),(2,2,'2020-01-01')")
        .unwrap();
    session.run("SET sql_mode = ''").unwrap();

    session
        .run(
            "INSERT INTO o VALUES (1,9,'2020-01-01'),(2,9,'2020-01-01') \
             ON DUPLICATE KEY UPDATE b = 'abc'",
        )
        .unwrap();
    let warnings: Vec<(u16, String)> = session
        .warnings()
        .iter()
        .map(|warning| (warning.code, warning.message.clone()))
        .collect();
    assert_eq!(
        warnings,
        vec![
            (
                1366,
                "Incorrect int value: '0' for column 'b' at row 1".to_owned()
            ),
            (
                1366,
                "Incorrect int value: '0' for column 'b' at row 2".to_owned()
            ),
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT a, b FROM o ORDER BY a")),
        [["1", "0"], ["2", "0"]]
    );

    // The temporal arm names the zero datetime the failed cast produced, in
    // its own SQL text -- Go's `types.Datum.ToString`.
    session
        .run("INSERT INTO o VALUES (1,9,'2020-01-01') ON DUPLICATE KEY UPDATE d = 'nope'")
        .unwrap();
    let warning = session.warnings().last().cloned().expect("one warning");
    assert_eq!(warning.code, 1292);
    assert_eq!(
        warning.message,
        "Incorrect datetime value: '0000-00-00 00:00:00' for column 'd' at row 1"
    );

    // An out-of-range assignment names no value at all, in either spelling.
    session
        .run("INSERT INTO o VALUES (2,9,'2020-01-01') ON DUPLICATE KEY UPDATE b = 99999999999999999999")
        .unwrap();
    let warning = session.warnings().last().cloned().expect("one warning");
    assert_eq!(warning.code, 1264);
    assert_eq!(
        warning.message,
        "Out of range value for column 'b' at row 1"
    );
}
