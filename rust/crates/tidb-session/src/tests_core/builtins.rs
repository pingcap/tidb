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
    assert_eq!(
        row_text(session.run("SELECT b FROM t WHERE b LIKE '%'")),
        [["xy"], ["Yz"], ["z"]]
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
    // while CHAR_LENGTH counts characters.
    assert_eq!(
        row_text(
            session
                .run("SELECT CONCAT(b,'!'), UPPER(b), LOWER(b), LENGTH(b), CHAR_LENGTH(b) FROM t")
        ),
        [
            ["xy!", "XY", "xy", "2", "2"],
            ["Yz!", "YZ", "yz", "2", "2"],
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
        [["xy", "xy", "n"], ["Yz", "Yz", "n"], ["z", "z", "n"],]
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
    assert_eq!(
        row_text(session.run("SELECT CURRENT_USER(), USER(), SESSION_USER()")),
        [["bob@%", "bob@10.0.0.1", "bob@10.0.0.1"]]
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
