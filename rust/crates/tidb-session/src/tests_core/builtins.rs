//! Scalar builtins over the string, temporal and conditional domains, and
//! the comparison rules that decide their argument types -- Go
//! `pkg/expression/builtin_*.go`.

use crate::tests_support::*;
use crate::*;

/// A handful of everyday string/date builtins that were previously
/// refused by the chunk rewriter's return-type gate (`builtin_return_type`
/// had no arm for them, even though `eval_func_values`/`time_fn::dispatch`
/// already implement them). Expected values captured from upstream Go
/// via `SELECT ...` in a mock-store testkit session.
#[test]
fn everyday_string_and_date_builtins() {
    let mut session = Session::new();
    assert_eq!(
        session
            .run("SELECT SUBSTRING_INDEX('a.b.c', '.', 2)")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("a.b")]])
    );
    assert_eq!(
        session.run("SELECT CHAR(77, 121, 83, 81, 76)").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("MySQL")]])
    );
    assert_eq!(
        session
            .run("SELECT INSERT('Quadratic', 3, 4, 'What')")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("QuWhattic")]])
    );
    assert_eq!(
        session
            .run("SELECT EXPORT_SET(5, 'Y', 'N', ',', 4)")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("Y,N,Y,N")]])
    );
    assert_eq!(
        session
            .run("SELECT DATE_FORMAT('2024-01-01 10:00:00', '%Y-%m-%d %H:%i:%s')")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("2024-01-01 10:00:00")]])
    );
    assert_eq!(
        session
            .run("SELECT STR_TO_DATE('01,5,2024','%d,%m,%Y')")
            .unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("2024-05-01")]])
    );
    assert_eq!(
        session.run("SELECT QUOTE('a''b')").unwrap(),
        StmtResult::Rows(vec![vec![Datum::new_string("'a\\'b'")]])
    );
}

/// A DATETIME/DATE column compared with a string or a number, checked
/// against captured TiDB output.
///
/// This was a SILENT WRONG ANSWER before: the generic string-vs-numeric
/// rule compared '2024-12-31' by its numeric prefix, so the WHERE clause
/// every application writes returned the wrong rows without any error.
#[test]
fn time_compared_with_strings_and_numbers() {
    let mut session = Session::new();
    session.apply_set("SET time_zone = '+00:00'").unwrap();
    session
        .run("CREATE TABLE t (id BIGINT, created DATETIME, d DATE)")
        .unwrap();
    session
        .run(
            "INSERT INTO t VALUES (1,'2024-06-15 10:00:00','2024-06-15'),\
                 (2,'2024-12-30 23:59:59','2024-12-30'),(3,'2025-01-02 00:00:00','2025-01-02')",
        )
        .unwrap();

    // Captured: a bare date string means that date's midnight.
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created <= '2024-12-31'")),
        [["1"], ["2"]]
    );
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created > '2024-12-31'")),
        [["3"]]
    );
    assert_eq!(
        row_text(
            session.run(
                "SELECT id FROM t WHERE created BETWEEN '2024-01-01' AND '2024-12-31 23:59:59'"
            )
        ),
        [["1"], ["2"]]
    );
    // Captured: equality both ways, and against a DATE column.
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created = '2024-06-15 10:00:00'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT '2024-06-15 10:00:00' = created FROM t WHERE id = 1")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE d = '2024-06-15'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE d < '2024-12-31'")),
        [["1"], ["2"]]
    );
    // Captured: a bare NUMBER parses as a date too.
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created <= 20241231")),
        [["1"], ["2"]]
    );
    // Captured: garbage filters every row with warning 1292, not an error.
    assert_eq!(
        row_text(session.run("SELECT id FROM t WHERE created <= 'garbage'")),
        Vec::<Vec<String>>::new()
    );
    // DOCUMENTED DIVERGENCE (the standing coprocessor-merge one): TiDB
    // reported ONE 1292 here because its coprocessor merges a batch's
    // warnings; this tier warns once per row compared.
    assert_eq!(session.warnings().len(), 3, "one warning per row compared");
    assert_eq!(session.warnings()[0].code, 1292);
    assert_eq!(
        session.warnings()[0].message,
        "Incorrect datetime value: 'garbage'"
    );
}

/// `1292 Truncated incorrect DOUBLE value` end to end, through a real
/// session's chunk executor onto `SHOW WARNINGS`.
///
/// The values below always matched TiDB; the WARNINGS did not exist at all,
/// because `crate::expr`'s values-only dispatch had no statement context to
/// raise them on. Since the integration suite records `SHOW WARNINGS`, a
/// statement could agree on every data row and still diverge on the warning
/// line, which is why the count and text are asserted here and not just the
/// presence of a warning.
///
/// Every expectation is a `gorun` capture. The one that constrains the design
/// hardest is the last: Go raises the warning ONCE PER COERCION, so three rows
/// read through two coercing sites record SIX warnings, in row order.
#[test]
fn numeric_prefix_strings_warn_once_per_coercion() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a VARCHAR(20))").unwrap();
    session
        .run("INSERT INTO t VALUES ('12abc'),('3x'),('zz')")
        .unwrap();
    let truncated = |text: &str| (1292, format!("Truncated incorrect DOUBLE value: '{text}'"));
    let seen = |session: &Session| -> Vec<(u16, String)> {
        session
            .warnings()
            .iter()
            .map(|w| (w.code, w.message.clone()))
            .collect()
    };

    // A constant argument: the value was already right, the warning was not
    // raised at all.
    assert_eq!(row_text(session.run("SELECT ABS('12abc')")), [["12"]]);
    assert_eq!(seen(&session), [truncated("12abc")]);

    // A comparison promoted to REAL raises the same warning.
    assert_eq!(row_text(session.run("SELECT '12abc' = 12")), [["1"]]);
    assert_eq!(seen(&session), [truncated("12abc")]);

    // And so does plain ARITHMETIC, now that a string operand takes the
    // `ETReal` cast Go's arithmetic classes wrap it in rather than being
    // refused. Captured: `select '12abc' + 1` -> 13 with
    // `1292 Truncated incorrect DOUBLE value: '12abc'`.
    assert_eq!(row_text(session.run("SELECT '12abc' + 1")), [["13"]]);
    assert_eq!(seen(&session), [truncated("12abc")]);

    // A complete number, a padded one, and the empty string are all silent
    // -- the last only because the engine reaches the coercion through a
    // function cast.
    assert_eq!(row_text(session.run("SELECT ABS(' 12 ')")), [["12"]]);
    assert_eq!(seen(&session), []);
    assert_eq!(row_text(session.run("SELECT ABS('')")), [["0"]]);
    assert_eq!(seen(&session), []);

    // One per row, in row order.
    assert_eq!(
        row_text(session.run("SELECT ABS(a) FROM t")),
        [["12"], ["3"], ["0"]]
    );
    assert_eq!(
        seen(&session),
        [truncated("12abc"), truncated("3x"), truncated("zz")]
    );

    // Two coercing sites over three rows: six warnings, not three.
    assert_eq!(
        row_text(session.run("SELECT ABS(a), SIGN(a) FROM t")),
        [["12", "1"], ["3", "1"], ["0", "0"]]
    );
    assert_eq!(seen(&session).len(), 6, "one warning per coercion per row");
}

/// The math, conditional and TRIM builtins through the chunk executor,
/// checked against captured TiDB output -- including the result TYPES,
/// which are what size a chunk cell.
///
/// The types are the subtle part and were read off TiDB's own result
/// fields: `ABS` and `MOD` keep the argument's domain, `CEIL`/`FLOOR`
/// return an integer for an integer OR decimal argument but stay real
/// for a real one, `ROUND`/`TRUNCATE` keep the decimal domain, and the
/// transcendental functions are always real.
#[test]
fn math_and_conditional_builtins() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'x',10),(2,'y',20)")
        .unwrap();

    // Captured: ABS keeps the argument's domain.
    assert_eq!(
        row_text(session.run("SELECT ABS(-3), ABS(-3.5)")),
        [["3", "3.5"]]
    );
    // Captured: CEIL/FLOOR of a decimal are integers, and of an integer
    // are the integer itself.
    assert_eq!(
        row_text(session.run("SELECT CEIL(1.2), FLOOR(1.8), CEIL(3), FLOOR(3)")),
        [["2", "1", "3", "3"]]
    );
    // Captured: ROUND keeps the decimal domain and rounds half away from
    // zero; TRUNCATE cuts instead.
    assert_eq!(
        row_text(session.run("SELECT ROUND(1.55,1), ROUND(1.55), ROUND(2.5), TRUNCATE(1.999,2)")),
        [["1.6", "2", "3", "1.99"]]
    );
    // Captured: MOD follows its arguments.
    assert_eq!(
        row_text(session.run("SELECT MOD(7,3), MOD(7.5,3)")),
        [["1", "1.5"]]
    );
    // Captured: the always-real family.
    assert_eq!(
        row_text(session.run("SELECT POW(2,3), SQRT(9), LOG10(100)")),
        [["8", "3", "2"]]
    );
    // Captured: SIGN, CONV and CRC32.
    assert_eq!(
        row_text(session.run("SELECT SIGN(-2), CONV(255,10,16), CRC32('a')")),
        [["-1", "FF", "3904355907"]]
    );

    // Captured: GREATEST/LEAST take the merged argument type, and work
    // over strings as well as numbers.
    assert_eq!(
        row_text(session.run("SELECT GREATEST(1,2,3), LEAST(1,2,3), GREATEST('a','b')")),
        [["3", "1", "b"]]
    );
    // Captured: IF picks one branch, and NULLIF is NULL only on equality.
    assert_eq!(
        row_text(session.run("SELECT IF(1,'big','small'), NULLIF(1,1), NULLIF(1,2)")),
        [["big", "NULL", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a, IF(c>15,'big','small') FROM t")),
        [["1", "small"], ["2", "big"]]
    );

    // Captured: TRIM's three directions, and its implicit space.
    assert_eq!(
        row_text(session.run("SELECT TRIM(' x '), TRIM(LEADING 'x' FROM 'xxa')")),
        [["x", "a"]]
    );
    assert_eq!(
        row_text(session.run("SELECT TRIM(TRAILING 'a' FROM 'xaa'), SUBSTRING('abc',1,2)")),
        [["x", "ab"]]
    );

    // IF is lazy, so the branch not taken never runs -- a division by
    // zero there would otherwise warn.
    session.run("SELECT IF(1, 1, 1/0)").unwrap();
    assert!(session.warnings().is_empty());
}

/// The date/time family through the chunk executor, checked against
/// captured TiDB output with `time_zone = '+00:00'`.
///
/// Go fixes the statement clock once, so every `NOW()` in one statement
/// agrees; the context carries that instant and the resolved session
/// zone (Go `timeutil.ParseTimeZone`).
///
/// DOCUMENTED DIVERGENCE, the same one the temporal casts carry: this
/// crate's date/time builtins produce formatted STRINGS, so the reported
/// column type is `VarString` where TiDB says `DATETIME`. The values
/// match.
/// `DATE_ADD`/`DATE_SUB`/`ADDDATE`/`SUBDATE`, `EXTRACT` and
/// `TIMESTAMPDIFF` through the CHUNK path, checked against captured TiDB
/// output with `time_zone = '+00:00'` (`pkg/executor`, a table holding
/// `('2024-01-31 10:20:30', '2024-01-31')` and
/// `('2025-03-15 23:59:59', '2025-03-15')` plus an all-NULL row).
///
/// The INTERVAL unit is a build-time keyword, not a value, so the
/// rewriter records it in the function NAME and the chunk evaluator
/// reuses the same `date_add` implementation the row path calls.
///
/// DOCUMENTED DIVERGENCE, the same one every other date/time builtin
/// here carries: the result is a formatted STRING (`VarString`) where
/// TiDB reports `DATE`/`DATETIME`. The values match.
#[test]
fn date_interval_extract_and_timestampdiff() {
    let mut session = Session::new();
    session.apply_set("SET time_zone = '+00:00'").unwrap();
    session
        .run("CREATE TABLE t (created VARCHAR(30), d VARCHAR(30))")
        .unwrap();
    session
        .run(
            "INSERT INTO t VALUES ('2024-01-31 10:20:30', '2024-01-31'), \
                 ('2025-03-15 23:59:59', '2025-03-15'), (NULL, NULL)",
        )
        .unwrap();

    // Captured: DAY arithmetic keeps the time-of-day, HOUR recomputes it
    // (and rolls the date over), and NULL propagates.
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL 1 DAY) FROM t")),
        [["2024-02-01 10:20:30"], ["2025-03-16 23:59:59"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL 2 HOUR) FROM t")),
        [["2024-01-31 12:20:30"], ["2025-03-16 01:59:59"], ["NULL"]]
    );
    // Captured: the month-end CLAMP -- January 31 plus one month is
    // February 29 in a leap year, not March 3.
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL 1 MONTH) FROM t")),
        [["2024-02-29 10:20:30"], ["2025-04-15 23:59:59"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT DATE_SUB(created, INTERVAL 1 DAY) FROM t")),
        [["2024-01-30 10:20:30"], ["2025-03-14 23:59:59"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT DATE_SUB(created, INTERVAL 1 MONTH) FROM t")),
        [["2023-12-31 10:20:30"], ["2025-02-15 23:59:59"], ["NULL"]]
    );
    // Captured: a date-only column keeps no time component at all.
    assert_eq!(
        row_text(session.run("SELECT DATE_SUB(d, INTERVAL 1 MONTH) FROM t")),
        [["2023-12-31"], ["2025-02-15"], ["NULL"]]
    );

    // Captured: ADDDATE/SUBDATE's bare-number form is exactly the DAY
    // interval, and their explicit INTERVAL form agrees with it.
    assert_eq!(
        row_text(session.run("SELECT ADDDATE(d, 5), SUBDATE(d, 5) FROM t")),
        [
            ["2024-02-05", "2024-01-26"],
            ["2025-03-20", "2025-03-10"],
            ["NULL", "NULL"]
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT ADDDATE(d, INTERVAL 5 DAY) FROM t")),
        [["2024-02-05"], ["2025-03-20"], ["NULL"]]
    );

    // Captured: EXTRACT of a simple unit is the same function that unit
    // already names.
    assert_eq!(
        row_text(session.run(
            "SELECT EXTRACT(YEAR FROM created), EXTRACT(MONTH FROM created), \
                 EXTRACT(DAY FROM d), EXTRACT(HOUR FROM created) FROM t"
        )),
        [
            ["2024", "1", "31", "10"],
            ["2025", "3", "15", "23"],
            ["NULL", "NULL", "NULL", "NULL"]
        ]
    );

    // Captured: TIMESTAMPDIFF counts WHOLE units -- January 31 to March 1
    // is 30 days but only 1 whole month, and a month whose day-of-month
    // is reached but whose clock time is not counts as 0.
    assert_eq!(
        row_text(session.run(
            "SELECT TIMESTAMPDIFF(DAY, '2024-01-31', '2024-03-01'), \
                 TIMESTAMPDIFF(MONTH, '2024-01-31', '2024-03-01')"
        )),
        [["30", "1"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT TIMESTAMPDIFF(MONTH, '2024-01-31 10:00:00', '2024-02-29 09:00:00'), \
                 TIMESTAMPDIFF(HOUR, '2024-01-31 10:00:00', '2024-02-01 09:00:00')"
        )),
        [["0", "23"]]
    );
    assert_eq!(
        row_text(session.run("SELECT TIMESTAMPDIFF(YEAR, d, created) FROM t")),
        [["0"], ["0"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT TIMESTAMPDIFF(DAY, NULL, '2024-01-01')")),
        [["NULL"]]
    );

    // Captured: a filter is the same expression in predicate position.
    assert_eq!(
        row_text(
            session
                .run("SELECT d FROM t WHERE created >= DATE_SUB('2025-01-01', INTERVAL 1 MONTH)")
        ),
        [["2025-03-15"]]
    );

    // Captured: an unparseable calendar date and a NULL amount are both
    // NULL, not an error.
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD('2024-02-30', INTERVAL 1 DAY)")),
        [["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL NULL DAY) FROM t LIMIT 1")),
        [["NULL"]]
    );

    // Composite units -- ported from `parseTimeValue`/
    // `ExtractDatetimeNum` (`pkg/types/time.go`); captured against
    // `pkg/executor`: `'2024-01-31 10:20:30' + INTERVAL '1:30'
    // HOUR_MINUTE` is `2024-01-31 11:50:30`, and `EXTRACT(HOUR_MINUTE
    // FROM '2024-01-31 10:20:30')` is `1020`. Both the row path
    // (`time_fn::calendar::date_add`/`extract_composite`) and the chunk
    // rewriter build these now.
    assert_eq!(
        row_text(session.run("SELECT DATE_ADD(created, INTERVAL '1:30' HOUR_MINUTE) FROM t")),
        [["2024-01-31 11:50:30"], ["2025-03-16 01:29:59"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT EXTRACT(HOUR_MINUTE FROM created) FROM t")),
        [["1020"], ["2359"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT EXTRACT(DAY_SECOND FROM created) FROM t")),
        [["31102030"], ["15235959"], ["NULL"]]
    );
}

#[test]
fn date_time_builtins() {
    let mut session = Session::new();
    session.apply_set("SET time_zone = '+00:00'").unwrap();
    session.run("CREATE TABLE t (d VARCHAR(30))").unwrap();
    session
        .run("INSERT INTO t VALUES ('2020-03-05 06:07:08')")
        .unwrap();

    // Captured: the field extractors.
    assert_eq!(
            row_text(session.run(
                "SELECT MONTH(d), DAY(d), YEAR(d), DAYOFWEEK(d), DAYOFYEAR(d), WEEKDAY(d), QUARTER(d) FROM t"
            )),
            [["3", "5", "2020", "5", "65", "3", "1"]]
        );
    assert_eq!(
        row_text(session.run(
            "SELECT MONTHNAME(d), DAYNAME(d), LAST_DAY(d), TO_DAYS(d), TIME_TO_SEC(d) FROM t"
        )),
        [["March", "Thursday", "2020-03-31", "737854", "22028"]]
    );
    assert_eq!(
        row_text(session.run("SELECT WEEK(d), WEEKOFYEAR(d), YEARWEEK(d) FROM t")),
        [["9", "10", "202009"]]
    );
    assert_eq!(
        row_text(session.run("SELECT SEC_TO_TIME(3661), MAKEDATE(2020,10), MAKETIME(1,2,3)")),
        [["01:01:01", "2020-01-10", "01:02:03"]]
    );
    assert_eq!(
        row_text(session.run("SELECT PERIOD_ADD(202001, 2), PERIOD_DIFF(202003, 202001)")),
        [["202003", "2"]]
    );

    // Captured: the statement clock is fixed, so NOW() agrees with
    // itself and prints a full second-resolution datetime.
    assert_eq!(
        row_text(session.run("SELECT NOW() = NOW(), LENGTH(NOW()) = 19")),
        [["1", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CURDATE() = CURDATE(), LENGTH(CURDATE()) = 10")),
        [["1", "1"]]
    );

    // `pkg/expression/builtin.go:722-725` binds NOW, CURRENT_TIMESTAMP,
    // LOCALTIME and LOCALTIMESTAMP to the SAME `nowFunctionClass`, so the
    // four are one function with four spellings -- including the bare
    // keyword forms the parser already accepted and the eval layer refused.
    // Captured: `select localtime(), localtimestamp(), localtime,
    // localtimestamp, now()` prints one value five times, `localtime() =
    // now()` is 1, and `localtimestamp(3)` carries three fractional digits.
    assert_eq!(
        row_text(session.run(
            "SELECT LOCALTIME() = NOW(), LOCALTIMESTAMP() = NOW(), \
             LOCALTIME = NOW(), LOCALTIMESTAMP = NOW()"
        )),
        [["1", "1", "1", "1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LENGTH(LOCALTIMESTAMP(3)) = 23")),
        [["1"]]
    );

    // The session zone reaches the clock: UTC and a +10 offset differ by
    // ten hours in the hour NOW() reports for the same instant.
    let hour_at = |session: &mut Session, zone: &str| -> i64 {
        session
            .apply_set(&format!("SET time_zone = '{zone}'"))
            .unwrap();
        match session.run("SELECT HOUR(NOW())").unwrap() {
            StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap().parse().unwrap(),
            other => panic!("expected rows, got {other:?}"),
        }
    };
    let utc = hour_at(&mut session, "+00:00");
    let plus_ten = hour_at(&mut session, "+10:00");
    assert_eq!((utc + 10) % 24, plus_ten);
}

/// `CAST(expr AS type)` and its `CONVERT`/`BINARY` spellings through the
/// chunk executor, checked against captured TiDB output.
///
/// The target type IS the operation in Go (it picks a
/// `builtinCast*As*Sig` from it), so the rewriter puts the target in the
/// function's result type and evaluation reads it back from there.
///
/// STILL REFUSED, for the reason `cast::eval_cast` already records:
/// `TIME` and `JSON` targets have no value domain in this crate, and the
/// `ARRAY` modifier is a JSON multi-valued index.
#[test]
fn cast_and_convert() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'12abc',10),(2,'zz',20)")
        .unwrap();

    // Captured: a number to CHAR, and the width truncating it.
    assert_eq!(
        row_text(session.run("SELECT CAST(c AS CHAR) FROM t")),
        [["10"], ["20"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST(c AS CHAR(1)) FROM t")),
        [["1"], ["2"]]
    );

    // Captured: a string to a number takes the leading digits, or zero.
    assert_eq!(
        row_text(session.run("SELECT CAST(b AS SIGNED) FROM t")),
        [["12"], ["0"]]
    );
    // Captured: the rounding asymmetry -- a string keeps only the integer
    // prefix while a decimal or a float rounds.
    assert_eq!(
        row_text(session.run("SELECT CAST('3.7' AS SIGNED), CAST(3.7 AS SIGNED)")),
        [["3", "4"]]
    );
    // Captured: UNSIGNED wraps a negative rather than clamping it.
    assert_eq!(
        row_text(session.run("SELECT CAST(-1 AS UNSIGNED)")),
        [["18446744073709551615"]]
    );

    // Captured: DECIMAL rounds to the written scale, and pads to it.
    assert_eq!(
        row_text(session.run("SELECT CAST('12.345' AS DECIMAL(6,2))")),
        [["12.35"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST(1 AS DECIMAL(6,2))")),
        [["1.00"]]
    );

    // Captured: the temporal targets.
    assert_eq!(
        row_text(session.run("SELECT CAST('2020-01-02' AS DATE)")),
        [["2020-01-02"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST('2020-1-2' AS DATE)")),
        [["2020-01-02"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST('2020-01-02 03:04:05' AS DATETIME)")),
        [["2020-01-02 03:04:05"]]
    );

    // Captured: BINARY(n) pads with NUL rather than truncating short.
    assert_eq!(
        row_text(session.run("SELECT CAST(b AS BINARY(3)) FROM t")),
        [["12a"], ["zz\u{0}"]]
    );

    // Captured: CONVERT and the BINARY operator are the same node.
    assert_eq!(
        row_text(session.run("SELECT CONVERT(c, CHAR), CONVERT('7', SIGNED) FROM t")),
        [["10", "7"], ["20", "7"]]
    );
    assert_eq!(
        row_text(session.run("SELECT BINARY b FROM t")),
        [["12abc"], ["zz"]]
    );

    // Captured: NULL casts to NULL, and a cast result is an ordinary
    // operand afterwards.
    assert_eq!(
        row_text(session.run("SELECT CAST(NULL AS SIGNED) IS NULL")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CAST(c AS DOUBLE)/2 FROM t")),
        [["5"], ["10"]]
    );

    // The JSON target produces this tier's canonical JSON text -- see
    // `json_value_functions` for the whole slice and its divergence note.
    assert_eq!(
        row_text(session.run("SELECT CAST(c AS JSON) FROM t")),
        [["10"], ["20"]]
    );

    // The refusals are refusals, not wrong answers.
    assert!(session.run("SELECT CAST(c AS TIME) FROM t").is_err());
}

/// LIKE, BETWEEN, CASE and the ordinary builtins through the chunk
/// executor, checked against captured TiDB output.
///
/// These forms all existed in `tidb_expr`'s AST evaluator already; what
/// was missing was the rewriter building them for chunk evaluation, so a
/// query using any of them failed outright.
///
/// STILL REFUSED, each for its own reason recorded at
/// `tidb_expr::rewriter::builtin_return_type`: the session-state
/// functions (`DATABASE`, `VERSION`, `CURRENT_USER`, `NOW`) need a
/// resolver carrying session state into the chunk path, `CAST`/`CONVERT`
/// take a target type rather than a value, `GROUP_CONCAT` is an
/// aggregate, and the `DATE_ADD` family takes an `Expr::Interval`.
#[test]
fn like_between_case_and_builtins() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20), c BIGINT, KEY kb (b))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'xy',10),(2,'Yz',20),(3,'z',30)")
        .unwrap();

    // Captured: LIKE's wildcards, its negation and its escape.
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b LIKE 'x%'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b LIKE '%y%'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b LIKE 'x_'")),
        [["1"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b NOT LIKE 'x%'")),
        [["2"], ["3"]]
    );
    assert_eq!(row_text(session.run(r"SELECT 'a%b' LIKE 'a\%b'")), [["1"]]);
    // `b` is covered by `kb(b)`, so this reads the whole INDEX, and the rows
    // therefore leave in index-key order rather than handle order. Captured
    // (v8.5 `gorun`), which is what fixes the order this used to assert:
    //
    // ```text
    // explain SELECT b FROM t WHERE b LIKE '%'
    //   IndexFullScan_8  cop[tikv]  table:t, index:kb(b)  keep order:false, stats:pseudo
    // SELECT group_concat(b) FROM t WHERE b LIKE '%'   ->  Yz,xy,z
    // SELECT group_concat(a) FROM t WHERE b LIKE '%'   ->  2,1,3
    // ```
    assert_eq!(
        row_text(session.run("SELECT b FROM t WHERE b LIKE '%'")),
        [["Yz"], ["xy"], ["z"]]
    );

    // Captured: BETWEEN is inclusive, and its negation is the complement.
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE c BETWEEN 10 AND 20")),
        [["1"], ["2"]]
    );
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE c NOT BETWEEN 10 AND 20")),
        [["3"]]
    );

    // Captured: the searched CASE, the simple CASE, a NULL condition
    // (which is not a match), and a missing ELSE (which is NULL).
    assert_eq!(
        row_text(session.run("SELECT a, CASE WHEN c > 15 THEN 'hi' ELSE 'lo' END FROM t")),
        [["1", "lo"], ["2", "hi"], ["3", "hi"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CASE c WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' END FROM t")),
        [["ten"], ["twenty"], ["NULL"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CASE WHEN NULL THEN 'x' ELSE 'y' END")),
        [["y"]]
    );
    assert_eq!(
        row_text(session.run("SELECT CASE WHEN c > 100 THEN 'x' END FROM t")),
        [["NULL"], ["NULL"], ["NULL"]]
    );

    // Captured: the string builtins, including LENGTH counting bytes
    // while CHAR_LENGTH counts characters. Only `b` is read, so this too is
    // a covering read of `kb(b)` and arrives in index order -- captured
    // (v8.5 `gorun`) as `IndexFullScan_6 table:t, index:kb(b)` with
    // `SELECT group_concat(CONCAT(b,'!')) FROM t  ->  Yz!,xy!,z!`.
    assert_eq!(
        row_text(
            session
                .run("SELECT CONCAT(b,'!'), UPPER(b), LOWER(b), LENGTH(b), CHAR_LENGTH(b) FROM t")
        ),
        [
            ["Yz!", "YZ", "yz", "2", "2"],
            ["xy!", "XY", "xy", "2", "2"],
            ["z!", "Z", "z", "1", "1"],
        ]
    );
    assert_eq!(
        row_text(session.run("SELECT LENGTH('héllo'), CHAR_LENGTH('héllo')")),
        [["6", "5"]]
    );

    // Captured: COALESCE and IFNULL over a column and a literal, whose
    // branch types Go merges to one string type.
    assert_eq!(
        row_text(session.run("SELECT COALESCE(NULL, b), IFNULL(b,'n'), IFNULL(NULL,'n') FROM t")),
        // Index order again: only `b` is read, so `kb(b)` covers.
        [["Yz", "Yz", "n"], ["xy", "xy", "n"], ["z", "z", "n"],]
    );

    // Captured: DATABASE() and its SCHEMA() synonym report the current
    // database, and VERSION() reports the same string as @@version.
    assert_eq!(
        row_text(session.run("SELECT DATABASE(), SCHEMA()")),
        [["test", "test"]]
    );
    let version = match session.run("SELECT VERSION()").unwrap() {
        StmtResult::Rows(rows) => datum_text(&rows[0][0]).unwrap(),
        other => panic!("expected rows, got {other:?}"),
    };
    assert_eq!(version, session.vars().get_system("version").unwrap());
    assert!(version.contains("TiDB"), "{version}");
    // Captured: with no database selected, DATABASE() is NULL.
    let mut fresh = Session::new();
    fresh.run("DROP DATABASE test").unwrap();
    assert_eq!(row_text(fresh.run("SELECT DATABASE()")), [["NULL"]]);

    // A session with no authenticated user answers NULL for the identity
    // builtins, which is what Go does for a session without one; a front
    // end that authenticates sets it (see the server's client test).
    assert_eq!(
        row_text(session.run("SELECT CURRENT_USER(), USER()")),
        [["NULL", "NULL"]]
    );
    session.set_user("bob@%".to_owned(), "bob@10.0.0.1".to_owned());
    // Only CURRENT_USER is bound to `currentUserFunctionClass`
    // (`pkg/expression/builtin.go:823`) and so reports the matched grant
    // identity. USER, SESSION_USER and SYSTEM_USER (`:833`, `:840`, `:841`)
    // all share `userFunctionClass`, whose sig returns `LoginString()`.
    assert_eq!(
        row_text(session.run("SELECT CURRENT_USER(), USER(), SESSION_USER(), SYSTEM_USER()")),
        [["bob@%", "bob@10.0.0.1", "bob@10.0.0.1", "bob@10.0.0.1"]]
    );

    // CONNECTION_ID() is NULL until a front end attaches one (Go itself
    // errors here rather than reporting NULL, but that path is
    // unreachable in practice -- see `Columns::connection_id`'s doc); once
    // set, the same value keeps reporting on later statements.
    assert_eq!(row_text(session.run("SELECT CONNECTION_ID()")), [["NULL"]]);
    session.set_connection_id(42);
    assert_eq!(row_text(session.run("SELECT CONNECTION_ID()")), [["42"]]);
    assert_eq!(row_text(session.run("SELECT CONNECTION_ID()")), [["42"]]);

    // The refusals above are refusals, not wrong answers. (CAST,
    // GROUP_CONCAT, CURRENT_USER, GROUP_CONCAT's inner ORDER BY, and
    // multi-argument GROUP_CONCAT were each this example in turn; all of
    // them work now.) `COUNT(b, a)` without DISTINCT stays refused, but as
    // a parser-level SQL syntax error, not a driver limitation: captured
    // from TiDB, `COUNT(a, b)` is only valid SQL as `COUNT(DISTINCT a,
    // b)` (see `multi_argument_count` below) -- the grammar itself
    // rejects the non-DISTINCT, multi-argument form.
    assert!(session.run("SELECT COUNT(b, a) FROM t").is_err());
}

/// `[NOT] REGEXP` through the chunk (table-scan `WHERE`) path, checked
/// against captured TiDB output. Before this test, the chunk rewriter had
/// no `Expr::Regexp` arm, so `SELECT ... WHERE b REGEXP '...'` failed
/// even though the same expression worked as a bare `SELECT`.
#[test]
fn regexp_through_the_chunk_path() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b VARCHAR(20))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,'abc'),(2,'xyz'),(3,NULL)")
        .unwrap();

    // Captured: `abc` matches `^a`, `xyz` and the NULL row do not.
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b REGEXP '^a'")),
        [["1"]]
    );
    // Captured: NOT REGEXP is the complement, still excluding the NULL
    // row -- a NULL operand is never TRUE for either polarity.
    assert_eq!(
        row_text(session.run("SELECT a FROM t WHERE b NOT REGEXP '^a'")),
        [["2"]]
    );
    // Captured: a bare SELECT REGEXP still works (the row path this
    // reused already handled it), so both paths agree.
    assert_eq!(row_text(session.run("SELECT 'abc' REGEXP '^a'")), [["1"]]);
    assert_eq!(
        row_text(session.run("SELECT 'abc' NOT REGEXP '^a'")),
        [["0"]]
    );
    // Captured: NULL propagates from either operand.
    assert_eq!(row_text(session.run("SELECT NULL REGEXP '^a'")), [["NULL"]]);
    assert_eq!(
        row_text(session.run("SELECT 'abc' REGEXP NULL")),
        [["NULL"]]
    );
    // Captured: an invalid pattern is a query error, not a NULL/false
    // result -- `[expression:1139]Got error 'error parsing regexp:
    // missing closing ): `(`' from regexp`.
    assert!(session.run("SELECT 'abc' REGEXP '('").is_err());
}

/// `MAKE_SET` regression, checked against mock TiDB. `1|4` evaluates to
/// the UNSIGNED domain, which used to fall through the builtin's
/// `Datum::Int`-only match and answer NULL instead of `'a,c'`.
#[test]
fn make_set_accepts_a_bitwise_or_result() {
    let mut session = Session::new();
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(1|4,'a','b','c')")),
        [["a,c"]]
    );
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(0,'a','b','c')")),
        [[""]]
    );
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(NULL,'a','b','c')")),
        [["NULL"]]
    );
    // A NULL string argument is skipped, not propagated.
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(1,'a',NULL,'c')")),
        [["a"]]
    );
    // More set bits than strings simply has nothing left to match.
    assert_eq!(
        row_text(session.run("SELECT MAKE_SET(31,'a','b','c')")),
        [["a,b,c"]]
    );
}

/// Go `resolveType4Extremum` takes GREATEST/LEAST's result type from
/// `aggregateType`, and `AggFieldType` does one thing a pairwise rank cannot:
/// a MIXED-SIGN pair of same-width integers is promoted one rank, and
/// LONGLONG's next rank is DECIMAL (`types/field_type.go:77-97`).
///
/// Without it the pair stayed an ETInt signature, whose comparison is signed
/// (`builtinGreatestIntSig.evalInt` is a plain `v > maxv`), so 2^63 read back
/// as a negative and lost to the literal 1 -- and the printed answer was the
/// clamp `9223372036854775807`.
///
/// Every expectation below is TiDB's own answer, captured with `gorun` over
/// `create table g(a bigint unsigned)` holding `9223372036854775808`.
#[test]
fn greatest_and_least_promote_a_mixed_sign_integer_pair_to_decimal() {
    let mut session = Session::new();
    session.run("CREATE TABLE g (a BIGINT UNSIGNED)").unwrap();
    session
        .run("INSERT INTO g VALUES (9223372036854775808)")
        .unwrap();
    for (sql, expected) in [
        (
            "SELECT GREATEST(CAST(9223372036854775808 AS UNSIGNED), 1)",
            "9223372036854775808",
        ),
        ("SELECT GREATEST(a, 1) FROM g", "9223372036854775808"),
        ("SELECT GREATEST(a, 5) FROM g", "9223372036854775808"),
        // A NEGATIVE literal is the case the signed comparison happened to
        // get right, so it is not on its own evidence -- it is here because
        // the decimal domain has to keep getting it right.
        ("SELECT GREATEST(a, -1) FROM g", "9223372036854775808"),
        (
            "SELECT LEAST(CAST(9223372036854775808 AS UNSIGNED), 1)",
            "1",
        ),
        // NOT mixed sign: both unsigned, so no promotion and the result
        // stays an unsigned integer.
        ("SELECT GREATEST(a, a) FROM g", "9223372036854775808"),
        // NOT mixed sign the other way: two signed literals stay integers.
        ("SELECT GREATEST(1, 2)", "2"),
    ] {
        assert_eq!(
            scalar_text(&mut session, sql).as_deref(),
            Some(expected),
            "{sql}"
        );
    }
}

/// Go's ETDatetime/ETTimestamp arm of `greatestFunctionClass.getFunction`
/// (`builtin_compare.go:546-553`): when every argument is temporal the
/// signature returns a TIME, cast onto the AGGREGATED temporal type, not
/// text. `builtinGreatestTimeSig.evalTime` stamps the winner with that type
/// on the way out (`res.SetType(resTimeTp)`).
///
/// The all-temporal shape used to fall through to the string signature, which
/// answered `2020-01-01` for `LEAST(date, datetime)` where TiDB answers
/// `2020-01-01 00:00:00`, and made arithmetic on the result a string-operand
/// error.
///
/// Captured with `gorun` over
/// `create table td(d date, dt datetime, ts timestamp, tm time)` holding
/// `2020-01-01` / `2020-01-01 10:00:00` / `2020-01-01 10:00:00` / `10:00:00`.
#[test]
fn an_all_temporal_greatest_returns_the_aggregated_temporal_type() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE td (d DATE, dt DATETIME, ts TIMESTAMP, tm TIME)")
        .unwrap();
    session
        .run(
            "INSERT INTO td VALUES \
             ('2020-01-01','2020-01-01 10:00:00','2020-01-01 10:00:00','10:00:00')",
        )
        .unwrap();
    for (sql, expected) in [
        ("SELECT GREATEST(d, dt) FROM td", "2020-01-01 10:00:00"),
        // The one that MOVED: the DATE is widened to midnight of that day and
        // wins as a DATETIME, so it prints with a time part.
        ("SELECT LEAST(d, dt) FROM td", "2020-01-01 00:00:00"),
        ("SELECT GREATEST(dt, ts) FROM td", "2020-01-01 10:00:00"),
        // ALL DATE: the aggregate stays a DATE and no time part appears.
        ("SELECT GREATEST(d, d) FROM td", "2020-01-01"),
        // A DURATION is its own eval type and stays one.
        ("SELECT GREATEST(tm, tm) FROM td", "10:00:00"),
        // The result is a TIME, so arithmetic on it is numeric rather than a
        // string-operand refusal.
        ("SELECT GREATEST(d, dt) + 0 FROM td", "20200101100000"),
    ] {
        assert_eq!(row_text(session.run(sql))[0][0], expected, "{sql}");
    }
}

/// Go's `argTp := resTp` (`builtin_compare.go:504`): GREATEST/LEAST compare in
/// the domain the ARGUMENT TYPES aggregate to, which is fixed before any value
/// exists. Reading the domain off the runtime datum instead answers a
/// different question for every argument whose declared type and datum
/// disagree about a domain -- an ENUM, a SET and a BIT are all such arguments,
/// and each one used to compare as a NUMBER against its ordinal.
///
/// Every expectation is captured with `gorun` over
/// `create table q(e1 enum('{}','[1]','x'), e2 enum('a','b','!'),
/// s1 set('a','b','c'), s2 set('a','b','c'), b64 bit(64), tm time,
/// tm100 time, tm20 time)`
/// holding `'{}'` / `'!'` / `'b'` / `'a'` / `9007199254740993` / `'10:00:00'` /
/// `'100:00:00'` / `'20:00:00'`.
#[test]
fn greatest_and_least_compare_in_the_aggregated_argument_domain() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE q (e1 ENUM('{}','[1]','x'), e2 ENUM('a','b','!'), \
             s1 SET('a','b','c'), s2 SET('a','b','c'), b64 BIT(64), tm TIME, \
             tm100 TIME, tm20 TIME)",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO q VALUES \
             ('{}','!','b','a',9007199254740993,'10:00:00','100:00:00','20:00:00')",
        )
        .unwrap();
    for (sql, expected) in [
        // An ENUM beside an integer aggregates to a string kind, so the `2` is
        // stringified and the NAMES are compared. `'{}'` is ordinal 1 but
        // sorts ABOVE `'2'`, so both answers invert between the two domains.
        ("SELECT GREATEST(e1, 2) FROM q", "{}"),
        ("SELECT LEAST(e1, 2) FROM q", "2"),
        // The MIRROR of that boundary, so the fix cannot be a blanket
        // inversion: `'!'` is ordinal 3 -- ABOVE the literal 2 -- and sorts
        // BELOW `'2'`. The ordinal domain and the name domain disagree here in
        // the opposite direction, and TiDB still answers by name.
        ("SELECT GREATEST(e2, 2) FROM q", "2"),
        ("SELECT LEAST(e2, 2) FROM q", "!"),
        // The EQUAL-ordinal boundary: set member `'b'` has ordinal 2, exactly
        // the literal, so an ordinal comparison finds a tie and keeps the
        // first argument for BOTH -- which is right for GREATEST by accident
        // and wrong for LEAST.
        ("SELECT GREATEST(s1, 2) FROM q", "b"),
        ("SELECT LEAST(s1, 2) FROM q", "2"),
        ("SELECT GREATEST(s2, 2) FROM q", "a"),
        ("SELECT LEAST(s2, 2) FROM q", "2"),
        // A BIT is NOT a string kind: it aggregates into the numeric domain
        // and must keep comparing as a number.
        ("SELECT GREATEST(b64, 2) FROM q", "9007199254740993"),
        ("SELECT LEAST(b64, 2) FROM q", "2"),
        // 2^53 and 2^53+1 are the SAME f64, so a numeric domain that rounds
        // through a double cannot separate them.
        (
            "SELECT GREATEST(b64, 9007199254740992) FROM q",
            "9007199254740993",
        ),
        (
            "SELECT LEAST(b64, 9007199254740992) FROM q",
            "9007199254740992",
        ),
        // OVER-APPLICATION guard. A DURATION aggregate is Go's ETDuration arm,
        // not a string one: sending everything that is neither a number nor a
        // date through the string signature would make this arithmetic a
        // string-operand read of `10:00:00` instead of `100000`.
        ("SELECT GREATEST(tm, tm) + 0 FROM q", "100000"),
        // The same guard where the DECLARED result type cannot cover for it.
        // A duration result column casts a stray string answer straight back
        // into a duration, so it hides a wrong domain unless the two domains
        // ORDER the values differently: `100:00:00` is the larger duration and
        // `20:00:00` is the larger text.
        ("SELECT GREATEST(tm100, tm20) FROM q", "100:00:00"),
        ("SELECT LEAST(tm100, tm20) FROM q", "20:00:00"),
        // The ETString return type is Go's `mysql.TypeVarString`, never an
        // argument's ENUM/SET code -- an ENUM result column declares a
        // name/value cell that `builtinGreatestStringSig`'s plain string
        // cannot fill.
        ("SELECT GREATEST(e1, e1) FROM q", "{}"),
        ("SELECT LEAST(s1, s1) FROM q", "b"),
        // Two DIFFERENT enums aggregate to a VARCHAR, and their ordinals (1
        // and 3) rank the opposite way from their names.
        ("SELECT GREATEST(e1, e2) FROM q", "{}"),
        ("SELECT LEAST(e1, e2) FROM q", "!"),
    ] {
        assert_eq!(row_text(session.run(sql))[0][0], expected, "{sql}");
    }
}

/// Go's `else if resTp == types.ETJson { ...; argTp = types.ETString; resTp =
/// types.ETString }` (`builtin_compare.go:508-512`): a JSON aggregate is the
/// one eval type GREATEST/LEAST refuse to compare in their own domain -- they
/// warn and compare the RENDERED TEXT instead.
///
/// The fixture is chosen so the two domains disagree: as text `'9'` is above
/// `'10'`, as numbers it is below. Captured with `gorun` over
/// `create table jt(a json, b json)` holding `'10'` and `'9'`:
/// `greatest(a,b)` is `9` and `least(a,b)` is `10`.
#[test]
fn a_json_greatest_compares_the_rendered_text() {
    let mut session = Session::new();
    session.run("CREATE TABLE jt (a JSON, b JSON)").unwrap();
    session.run("INSERT INTO jt VALUES ('10','9')").unwrap();
    for (sql, expected) in [
        ("SELECT GREATEST(a, b) FROM jt", "9"),
        ("SELECT LEAST(a, b) FROM jt", "10"),
    ] {
        assert_eq!(row_text(session.run(sql))[0][0], expected, "{sql}");
    }
}

/// `fieldTimeType` is read off the AGGREGATE, not off the arriving times:
/// `builtinGreatestTimeSig`'s `cmpAsDate` is
/// `aggType.GetType() == mysql.TypeDate`, and the winner is converted to that
/// type on the way out.
///
/// An expression can carry a DATETIME type while every value it produces is a
/// pure DATE -- `IFNULL(d, dt)` is exactly that, since `InferType4ControlFuncs`
/// merges the two branches but the branch that runs is the DATE one. The two
/// rules answer differently there and only the aggregate's is TiDB's.
///
/// Captured with `gorun` over `create table td(d date, dt datetime, j json)`
/// holding `2020-01-01` / `2020-01-01 10:00:00` / `[1]`.
#[test]
fn the_temporal_greatest_result_type_follows_the_aggregate_not_the_values() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE td (d DATE, dt DATETIME, j JSON)")
        .unwrap();
    session
        .run("INSERT INTO td VALUES ('2020-01-01','2020-01-01 10:00:00','[1]')")
        .unwrap();
    for (sql, expected) in [
        // Both values are `2020-01-01`, both datum kinds are DATE -- and the
        // aggregate is still a DATETIME, so the answer carries a time part.
        (
            "SELECT GREATEST(d, IFNULL(d, dt)) FROM td",
            "2020-01-01 00:00:00",
        ),
        (
            "SELECT LEAST(d, IFNULL(d, dt)) FROM td",
            "2020-01-01 00:00:00",
        ),
        (
            "SELECT GREATEST(d, COALESCE(d, dt)) FROM td",
            "2020-01-01 00:00:00",
        ),
        // A JSON argument beside a DATE does NOT aggregate to JSON: JSON
        // merges with anything else to a VARCHAR, which is why Go's ETJson
        // arm and its compare-as-time arm can never both apply. Here the DATE
        // selects the compare-as-date signature and `[1]` -- which does not
        // parse -- keeps its own text and wins on `[` sorting above `2`.
        ("SELECT GREATEST(j, d) FROM td", "[1]"),
        ("SELECT LEAST(j, d) FROM td", "2020-01-01"),
        // The temporal scan PREFERS a DATETIME argument over a DATE one
        // wherever they sit in the list, so a string beside both compares as
        // a DATETIME. Taking the first temporal instead would parse all three
        // as DATEs and collapse them onto the same day.
        (
            "SELECT GREATEST(d, dt, '2020-01-01 05:00:00') FROM td",
            "2020-01-01 10:00:00",
        ),
        (
            "SELECT LEAST(d, dt, '2020-01-01 05:00:00') FROM td",
            "2020-01-01 00:00:00",
        ),
    ] {
        assert_eq!(row_text(session.run(sql))[0][0], expected, "{sql}");
    }
}

/// A `DATE 'lit'` / `TIMESTAMP 'lit'` is TEMPORALLY TYPED, exactly like a
/// column of that type -- Go's `dateLiteralFunctionClass.getFunction` calls
/// `setDecimalAndFlenForDate` and its timestamp twin
/// `setDecimalAndFlenForDatetime(tm.Fsp())` -- so `resolveType4Extremum` sees
/// it and every one of these answers changes with it.
///
/// The literal used to fold to a VarString, which made a literal invisible
/// where a COLUMN of the same type was not. The first two rows below are the
/// trap: when the literal happens to WIN, a string compare prints the same
/// text and the bug is invisible; only a row where a NUMBER wins shows it.
///
/// Recorded oracle (`tests/integrationtest/r/expression/issues.result`, the
/// `TestIssue38736`/`greatest` block) for the four SELECTs; the two arithmetic
/// rows are captured with `gorun` and are the pure TYPE proof -- a VarString
/// operand would be read as the leading `2020`, not as the packed number.
///
/// ```text
/// select date '2020-01-01' + 0, timestamp '2020-01-01 10:00:00.5' + 0
///   RS:20200101|20200101100000.5
/// ```
#[test]
fn a_temporal_literal_is_typed_where_a_column_of_that_type_is() {
    let mut session = Session::new();
    for (sql, expected) in [
        // The literal WINS: a string compare prints the same text, so this
        // row alone cannot tell the two rules apart.
        (
            "SELECT GREATEST(date '2005-05-05', 20010101, 20040404, 20030303)",
            "2005-05-05",
        ),
        // A NUMBER wins, and only the typed literal reformats it as a date.
        (
            "SELECT GREATEST(date '1995-05-05', 19910101, 20050505, 19930303)",
            "2005-05-05",
        ),
        (
            "SELECT GREATEST(date'101001', '19990329', 120101)",
            "2012-01-01",
        ),
        // Two literals: `AggFieldType(TypeDate, TypeDatetime)` widens the
        // winning DATE onto midnight rather than printing it bare.
        (
            "SELECT GREATEST(date '21000101', timestamp '2069-12-31 12:00:00')",
            "2100-01-01 00:00:00",
        ),
        ("SELECT date '2020-01-01' + 0", "20200101"),
        (
            "SELECT timestamp '2020-01-01 10:00:00.5' + 0",
            "20200101100000.5",
        ),
    ] {
        assert_eq!(row_text(session.run(sql))[0][0], expected, "{sql}");
    }
}

/// Go's ETDatetime GREATEST/LEAST arm CASTS its arguments before comparing
/// them (`newBaseBuiltinFuncWithTp(ctx, funcName, args, resTp, argTps...)`
/// wraps each in `WrapWithCastAsTime`), and a DURATION's cast is
/// `builtinCastDurationAsTimeSig`: `ConvertToTimeWithTimestamp` mixes the
/// elapsed time into the calendar date of the STATEMENT TIMESTAMP, in the
/// SESSION's zone. `AggFieldType(TypeTime, TypeDatetime)` is `TypeDatetime`,
/// so a `time` column beside a `TIMESTAMP 'lit'` lands here.
///
/// The boundary that matters is which argument WINS. The recorded corpus only
/// has the literal winning (`greatest(c, timestamp '2069-12-31 12:00:00')`),
/// and every wrong conversion still answers 2069 there. These rows put the
/// DURATION on the winning side, where the converted date is the printed
/// answer -- and then move the session zone across the date line, where a
/// conversion done in UTC prints the same day for both.
///
/// Captured with `gorun`, statement clock pinned so the answer is stable:
///
/// ```text
/// SET timestamp=UNIX_TIMESTAMP('2011-11-01 17:48:00');   -- 09:48:00Z
/// create table td(tm time); insert into td values('10:00:00');
/// select greatest(tm, timestamp '2000-01-01 00:00:00')     2011-11-01 10:00:00
/// least   (tm, timestamp '2000-01-01 00:00:00')            2000-01-01 00:00:00
/// greatest(tm, date '2000-01-01')                          2011-11-01 10:00:00
/// greatest(tm, timestamp '2000-01-01 00:00:00.5')          2011-11-01 10:00:00
/// least   (tm, timestamp '2000-01-01 00:00:00.5')          2000-01-01 00:00:00.5
/// time_zone='+13:00'  greatest(tm, timestamp '2000-01-01 00:00:00')
///                                                          2011-11-01 10:00:00
/// time_zone='-11:00'  same statement                       2011-10-31 10:00:00
/// ```
#[test]
fn a_duration_beside_a_temporal_literal_lands_on_the_statement_date() {
    let mut session = Session::new();
    session.run("CREATE TABLE td (tm TIME)").unwrap();
    session.run("INSERT INTO td VALUES ('10:00:00')").unwrap();
    // 2011-11-01 17:48:00 at +08:00, the zone the capture ran under.
    session.run("SET timestamp = 1320140880").unwrap();
    for (sql, expected) in [
        (
            "SELECT GREATEST(tm, timestamp '2000-01-01 00:00:00') FROM td",
            "2011-11-01 10:00:00",
        ),
        (
            "SELECT LEAST(tm, timestamp '2000-01-01 00:00:00') FROM td",
            "2000-01-01 00:00:00",
        ),
        (
            "SELECT GREATEST(tm, date '2000-01-01') FROM td",
            "2011-11-01 10:00:00",
        ),
        (
            "SELECT GREATEST(tm, timestamp '2000-01-01 00:00:00.5') FROM td",
            "2011-11-01 10:00:00",
        ),
        // The LITERAL's own fsp survives the compare and is printed.
        (
            "SELECT LEAST(tm, timestamp '2000-01-01 00:00:00.5') FROM td",
            "2000-01-01 00:00:00.5",
        ),
    ] {
        assert_eq!(row_text(session.run(sql))[0][0], expected, "{sql}");
    }
    // The same instant, two zones on opposite sides of the date line: the
    // calendar day is the SESSION's, not UTC's.
    let sql = "SELECT GREATEST(tm, timestamp '2000-01-01 00:00:00') FROM td";
    session.run("SET time_zone = '+13:00'").unwrap();
    assert_eq!(row_text(session.run(sql))[0][0], "2011-11-01 10:00:00");
    session.run("SET time_zone = '-11:00'").unwrap();
    assert_eq!(row_text(session.run(sql))[0][0], "2011-10-31 10:00:00");
}

/// The miscellaneous and encryption builtins whose bodies were already ported
/// and unit-tested in `tidb_expr::builtin_ext::{misc,crypto}`, but which live
/// SQL could not reach because `builtin_return_type` had no arm for their
/// names -- the rewriter's gate is the ONLY one, and a miss there is a hard
/// client error with no AST fallback.
///
/// Every expected value is a Go capture (`goeval`, one expression per line):
///
/// ```text
/// password('x')                                        STR:*B69027D44F6E5EDC07F1AEAD1477967B16F28227
/// md5('a')                                             STR:0cc175b9c0f1b6a831c399e269772661
/// tidb_shard(1)                                        UINT:214
/// vitess_hash(123)                                     UINT:1155070131015363447
/// hex(uuid_to_bin('6ccd780c-...-5b8c656024db'))        STR:6CCD780CBABA102695645B8C656024DB
/// bin_to_uuid(unhex('6CCD780C...5B8C656024DB'))        STR:6ccd780c-baba-1026-9564-5b8c656024db
/// uuid_timestamp('6ccd780c-...-5b8c656024db')          DEC:-11129156903.290674
/// name_const('a',5)                                    INT:5
/// hex(encode('abc','k'))                               STR:ED1DFA
/// uncompressed_length(unhex('08000000789C4A84...0DAC0309'))  INT:8
/// uncompress(unhex('08000000789C4A84...0DAC0309'))           STR:aaaaaaaa
/// ```
#[test]
fn misc_and_encryption_builtins_reach_live_sql() {
    let mut session = Session::new();
    // The payload below is exactly what Go's `COMPRESS('aaaaaaaa')` emits
    // (captured: `select hex(compress('aaaaaaaa'))`): a four-byte
    // little-endian original length (8) followed by the zlib stream. COMPRESS
    // itself is NOT ported, so the fixture is spelled as a hex literal rather
    // than as a round trip through it.
    const COMPRESSED_AAAAAAAA: &str = "08000000789C4A840240000000FFFF0DAC0309";
    for (sql, expected) in [
        ("SELECT NAME_CONST('a', 5)", "5"),
        (
            "SELECT HEX(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db'))",
            "6CCD780CBABA102695645B8C656024DB",
        ),
        (
            "SELECT BIN_TO_UUID(UNHEX('6CCD780CBABA102695645B8C656024DB'))",
            "6ccd780c-baba-1026-9564-5b8c656024db",
        ),
        (
            "SELECT UUID_TIMESTAMP('6ccd780c-baba-1026-9564-5b8c656024db')",
            "-11129156903.290674",
        ),
        ("SELECT TIDB_SHARD(1)", "214"),
        ("SELECT VITESS_HASH(123)", "1155070131015363447"),
        (
            "SELECT PASSWORD('x')",
            "*B69027D44F6E5EDC07F1AEAD1477967B16F28227",
        ),
        ("SELECT HEX(ENCODE('abc', 'k'))", "ED1DFA"),
        ("SELECT DECODE(ENCODE('abc', 'k'), 'k')", "abc"),
        ("SELECT MD5('a')", "0cc175b9c0f1b6a831c399e269772661"),
    ] {
        assert_eq!(row_text(session.run(sql))[0][0], expected, "{sql}");
    }
    assert_eq!(
        row_text(session.run(&format!(
            "SELECT UNCOMPRESSED_LENGTH(UNHEX('{COMPRESSED_AAAAAAAA}'))"
        )))[0][0],
        "8"
    );
    assert_eq!(
        row_text(session.run(&format!(
            "SELECT UNCOMPRESS(UNHEX('{COMPRESSED_AAAAAAAA}'))"
        )))[0][0],
        "aaaaaaaa"
    );

    // AES_ENCRYPT/AES_DECRYPT stay REFUSED on purpose: the ported body is
    // `aes-128-ecb` only, while Go picks the cipher from
    // `block_encryption_mode`, which this gate cannot see. A refusal beats a
    // silently wrong ciphertext -- see `builtin_return_type`'s own doc.
    assert!(session.run("SELECT AES_ENCRYPT('a', 'k')").is_err());
}

/// A string operand of an arithmetic, bitwise or unary operator, end to end
/// through the chunk tier. `WHERE varchar_col + 0 = ...` is an everyday idiom
/// and used to be a hard statement error.
///
/// Go never lets a string reach an arithmetic signature: each `getFunction`
/// reads `numericContextResultType` (`pkg/expression/builtin_arithmetic.go:80`)
/// for both arguments and `newBaseBuiltinFuncWithTp` wraps every argument in
/// the cast the chosen signature's `argTps` name. A string's numeric context is
/// always `ETReal` (`:94-100`), so the cast is fixed by the OPERATOR: `ETInt`
/// for the bitwise family, `ETDecimal` for `DIV` (unless both sides are
/// already `ETInt`), `ETReal` for everything else.
///
/// Every expectation is a Go capture (`goeval`, one expression per line):
///
/// ```text
/// '3' + 1      FLOAT:4      '3' - 1     FLOAT:2     '3' * 2   FLOAT:6
/// '3' / 2      FLOAT:1.5    '3' div 2   INT:1       '3' % 2   FLOAT:1
/// '3' & 1      UINT:1       '3' | 4     UINT:7      '3' ^ 1   UINT:2
/// '3' << 2     UINT:12      '3.7' + 1   FLOAT:4.7   '3' + 1.5 FLOAT:4.5
/// 1.5 & '3'    UINT:2       'abc' + 1   FLOAT:1     '12abc'+1 FLOAT:13
/// -'3'         FLOAT:-3     +'3'        STR:3       ~'3'      UINT:18446744073709551612
/// '1e3' + 1    FLOAT:1001   '  7 ' + 1  FLOAT:8     '3.7' & 1 UINT:1
/// '3.5' & 1    UINT:1       '-3' & 1    UINT:1      '7.9' div 2 INT:3
/// 'abc' & 1    UINT:0       'abc' div 2 INT:0       '3.7' << 1  UINT:6
/// ```
#[test]
fn string_operands_take_go_s_per_operator_numeric_cast() {
    let mut session = Session::new();
    for (sql, expected) in [
        ("SELECT '3' + 1", "4"),
        ("SELECT '3' - 1", "2"),
        ("SELECT '3' * 2", "6"),
        ("SELECT '3' / 2", "1.5"),
        // DIV takes the DECIMAL cast, so the fraction is truncated, not
        // rounded through a float.
        ("SELECT '3' div 2", "1"),
        ("SELECT '7.9' div 2", "3"),
        // The row that TELLS the DIV cast apart from the ETReal one: a
        // 17-digit string is exact as a DECIMAL and rounds to 1e17 as a
        // double. Captured: `'99999999999999999' div 1` -> 99999999999999999
        // while `'99999999999999999' + 0` -> 100000000000000000.
        ("SELECT '99999999999999999' div 1", "99999999999999999"),
        ("SELECT '99999999999999999' + 0", "100000000000000000"),
        ("SELECT '3' % 2", "1"),
        // The bitwise family takes the INT cast, which is `StrToInt`'s
        // truncating integer prefix -- '3.7' is 3, never a rounded 4.
        ("SELECT '3' & 1", "1"),
        ("SELECT '3' | 4", "7"),
        ("SELECT '3' ^ 1", "2"),
        ("SELECT '3' << 2", "12"),
        ("SELECT '3.7' & 1", "1"),
        ("SELECT '3.5' & 1", "1"),
        ("SELECT '3.7' << 1", "6"),
        ("SELECT '-3' & 1", "1"),
        ("SELECT 'abc' & 1", "0"),
        ("SELECT 'abc' div 2", "0"),
        // A numeric partner promotes through the ordinary hierarchy; only the
        // string is converted here, and the answer still matches Go's
        // cast-both-arguments build.
        ("SELECT '3.7' + 1", "4.7"),
        ("SELECT '3' + 1.5", "4.5"),
        ("SELECT 1.5 & '3'", "2"),
        // The numeric PREFIX rule, including the empty prefix.
        ("SELECT 'abc' + 1", "1"),
        ("SELECT '12abc' + 1", "13"),
        ("SELECT '1e3' + 1", "1001"),
        ("SELECT '  7 ' + 1", "8"),
        // Two strings: an arithmetic operator over them is NOT a collation
        // comparison, so both take the same cast.
        ("SELECT '1231' % '12'", "7"),
        // Unary. UNARY PLUS is not a function class in TiDB at all -- the
        // parser hands the operand back untouched -- so it stays a STRING.
        ("SELECT -'3'", "-3"),
        ("SELECT +'3'", "3"),
        ("SELECT ~'3'", "18446744073709551612"),
    ] {
        assert_eq!(row_text(session.run(sql))[0][0], expected, "{sql}");
    }

    // The truncation warning is worded from the CAST the operator chose, not
    // from the operand: DIV's is DECIMAL where `+`'s is DOUBLE. Captured:
    // `select 'abc' div 2` warns `1292 Truncated incorrect DECIMAL value:
    // 'abc'`.
    let mut warned = Session::new();
    warned.run("SELECT 'abc' div 2").unwrap();
    assert_eq!(
        warned
            .warnings()
            .iter()
            .map(|w| (w.code, w.message.clone()))
            .collect::<Vec<_>>(),
        [(1292, "Truncated incorrect DECIMAL value: 'abc'".to_owned())]
    );
    warned.run("SELECT 'abc' + 2").unwrap();
    assert_eq!(
        warned
            .warnings()
            .iter()
            .map(|w| (w.code, w.message.clone()))
            .collect::<Vec<_>>(),
        [(1292, "Truncated incorrect DOUBLE value: 'abc'".to_owned())]
    );

    // The everyday idiom this unblocks, over a real VARCHAR column.
    session
        .run("CREATE TABLE s (a INT PRIMARY KEY, b VARCHAR(20))")
        .unwrap();
    session
        .run("INSERT INTO s VALUES (1,'10'),(2,'20'),(3,'x')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT a FROM s WHERE b + 0 = 20")),
        [["2"]]
    );
    assert_eq!(
        row_text(session.run("SELECT b + 0 FROM s ORDER BY a")),
        [["10"], ["20"], ["0"]]
    );
}

/// `ADDTIME` over TYPED temporal columns, which is where Go's twelve-way
/// `addTimeFunctionClass.getFunction` switch is actually observable: the
/// same values answer differently depending on whether the column is
/// DATETIME, DATE, TIME or VARCHAR, and a DATETIME second argument makes
/// the whole call NULL.
///
/// Every expected value was captured from a real TiDB session (mock store)
/// running exactly these statements.
#[test]
fn addtime_selects_its_signature_from_the_column_types() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE w(dt DATETIME(3), tm TIME(4), dt6 DATETIME(6), \
             tm6 TIME(6), d0 DATETIME, t0 TIME, dd DATE, s VARCHAR(50))",
        )
        .unwrap();
    session
        .run(
            "INSERT INTO w VALUES('2020-01-01 10:00:00.123','01:02:03.4567',\
             '2020-01-01 10:00:00.123456','01:02:03.456789','2020-01-01 10:00:00',\
             '01:02:03','2020-01-01','2020-01-01 10:00:00')",
        )
        .unwrap();
    for (expr, want) in [
        // DATETIME + TIME: the result's fsp is the DATETIME's own, because
        // the vectorized arm hands `Time.Add` a `Duration{Fsp: -1}`. The
        // microsecond field is exact (`.579789`) and TRUNCATED to print.
        ("addtime(dt, tm)", "2020-01-01 11:02:03.579"),
        ("addtime(dt6, tm6)", "2020-01-01 11:02:03.580245"),
        ("addtime(dt, tm6)", "2020-01-01 11:02:03.579"),
        ("addtime(dt6, tm)", "2020-01-01 11:02:03.580156"),
        ("addtime(d0, tm)", "2020-01-01 11:02:03"),
        ("addtime(dt, t0)", "2020-01-01 11:02:03.123"),
        // DATE + TIME: the DATE reads as midnight and its own fsp is 0, so
        // the DURATION's fsp decides -- the opposite operand from above.
        ("addtime(dd, tm)", "2020-01-01 01:02:03.4567"),
        // TIME + TIME: the larger of the two fsps.
        ("addtime(tm, tm)", "02:04:06.9134"),
        ("addtime(tm, tm6)", "02:04:06.913489"),
        ("addtime(t0, tm)", "02:04:06.4567"),
        // A VARCHAR first argument sniffs its own text at RUNTIME, and the
        // VECTORIZED body has no `<digits>-<rest>` guard, so this is the one
        // that differs from the constant-folded spelling (which is NULL).
        ("addtime(s, s)", "2020-01-01 20:00:00"),
        ("addtime(s, tm)", "2020-01-01 11:02:03.456700"),
    ] {
        assert_eq!(
            session.run(&format!("SELECT {expr} FROM w")).unwrap(),
            StmtResult::Rows(vec![vec![Datum::new_string(want)]]),
            "{expr}"
        );
    }
    // Every `...Null` signature: a DATETIME/TIMESTAMP second argument.
    for expr in [
        "addtime(dt, dt)",
        "addtime(dd, dt)",
        "addtime(tm, dt)",
        "addtime(s, dt)",
    ] {
        assert_eq!(
            session.run(&format!("SELECT {expr} FROM w")).unwrap(),
            StmtResult::Rows(vec![vec![Datum::Null]]),
            "{expr}"
        );
    }
}

/// `SYSDATE` is NOT the statement clock: `builtinSysDateWithoutFspSig`
/// calls `time.Now()` per evaluation, where `NOW` returns the one fixed
/// statement timestamp. Only the SHAPE is asserted -- the value is a real
/// wall-clock reading and cannot be pinned.
#[test]
fn sysdate_reads_the_wall_clock_and_not_the_statement_timestamp() {
    let mut session = Session::new();
    let StmtResult::Rows(rows) = session.run("SELECT SYSDATE(), SYSDATE(3)").unwrap() else {
        panic!("SYSDATE did not produce rows")
    };
    let text = |value: &Datum| match value {
        Datum::String(payload) => payload.as_utf8().expect("SYSDATE is UTF-8").to_owned(),
        other => panic!("SYSDATE did not produce a string: {other:?}"),
    };
    let plain = text(&rows[0][0]);
    let with_fsp = text(&rows[0][1]);
    assert_eq!(plain.len(), 19, "SYSDATE() width: {plain}");
    assert_eq!(with_fsp.len(), 23, "SYSDATE(3) width: {with_fsp}");
    assert_eq!(&with_fsp[19..20], ".", "SYSDATE(3) fraction: {with_fsp}");
}

/// Go's `types.ETDatetime` ARGUMENT declaration over real COLUMNS, which is
/// the only place its one type-dependent rule is observable: a `YEAR` column
/// takes `types.ParseTimeFromYear` and every other integer source takes
/// `types.ParseTimeFromNum` (`builtin_cast.go:1127-1131`). The value tier
/// alone cannot tell those two apart -- both are a `Datum::Int` -- so this
/// end-to-end path is the proof that the static type reaches the cast.
///
/// NO RECORDED ROW EXISTS: `tests/integrationtest/r/**` never calls these
/// builtins over a YEAR or packed-integer column, only inside partition
/// definitions and over already-temporal columns. Every expected value is
/// GO-DERIVED, captured from real TiDB through `gorun`.
#[test]
fn an_etdatetime_argument_is_cast_from_its_column_type() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE yt (y YEAR, n BIGINT, d DATE)")
        .unwrap();
    session
        .run("INSERT INTO yt VALUES (2024, 20240315123045, '2024-03-15')")
        .unwrap();

    // Captured `RS:0|0|0|2024`. `ParseTimeFromYear(2024)` injects the value
    // as the year FIELD, so the date is `2024-00-00` and the month, day and
    // quarter are the STORED zeros -- not NULL, and not March. Routing the
    // same integer through `ParseTimeFromNum` fails outright, so this row is
    // the boundary case for threading the argument's FieldType at all: drop
    // it and all four columns become NULL.
    assert_eq!(
        row_text(session.run("SELECT month(y), day(y), quarter(y), year(y) FROM yt")),
        [["0", "0", "0", "2024"]]
    );

    // Captured `RS:3|15|1|2024`. The SAME integer magnitude under a BIGINT
    // column is a packed `YYYYMMDDHHMMSS`, so it reads as a real date. The
    // pair above and below is the whole point: one value, two column types,
    // two different Go parsers.
    assert_eq!(
        row_text(session.run("SELECT month(n), day(n), quarter(n), year(n) FROM yt")),
        [["3", "15", "1", "2024"]]
    );

    // Captured `RS:3|1|2024`. A DATE column is Go's early return
    // (`builtin_cast.go:2821`: `exprTp == mysql.TypeDate && tp.GetType() ==
    // mysql.TypeDatetime` returns the expression unwrapped), so the cast must
    // be a pass-through and not re-derive anything.
    assert_eq!(
        row_text(session.run("SELECT month(d), quarter(d), year(d) FROM yt")),
        [["3", "1", "2024"]]
    );

    // Captured `RS:739325|2024-03|74|2024-03-16 12:30:45`. Four more
    // ETDatetime-declaring classes over the same packed-integer column, each
    // with a DIFFERENT argument index (`TO_DAYS` 0, `DATE_FORMAT` 0,
    // `TIMESTAMPDIFF` 1 and 2, `TIMESTAMPADD` 2) -- so a mask that is right
    // for one and wrong for another cannot pass this row.
    assert_eq!(
        row_text(session.run(
            "SELECT to_days(n), date_format(n,'%Y-%m'), \
             timestampdiff(day,'2024-01-01',n), timestampadd(day,1,n) FROM yt"
        )),
        [["739325", "2024-03", "74", "2024-03-16 12:30:45"]]
    );
}
