//! The numeric domain's edges: literal typing, division by zero under each
//! SQL mode, and `RAND` with its seed variables.

use crate::tests_support::*;
use crate::*;

/// Decimal, hex and bit literals through the whole session path, checked
/// against captured TiDB output.
///
/// NOT PORTED: `-2.750` is one literal token in Go's parser, so its type
/// carries the sign in its flen; this AST keeps the sign as a unary minus
/// over the literal, so the sign shapes the value but not the literal's
/// own flen. The printed value is the same.
#[test]
fn numeric_literals() {
    let mut session = Session::new();

    // Captured: a decimal literal keeps its written scale.
    assert_eq!(row_text(session.run("SELECT 1.5")), [["1.5"]]);
    assert_eq!(row_text(session.run("SELECT 0.10")), [["0.10"]]);
    assert_eq!(row_text(session.run("SELECT -2.750")), [["-2.750"]]);

    // Captured: decimal arithmetic keeps the wider scale, and division by
    // zero is still NULL plus a warning.
    assert_eq!(row_text(session.run("SELECT 1.5 + 1")), [["2.5"]]);
    assert_eq!(row_text(session.run("SELECT 1.5 * 2")), [["3.0"]]);
    assert_eq!(
        session.run("SELECT 1.5 / 0").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null]])
    );
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1365);

    // Captured: a decimal comparison against an integer.
    assert_eq!(row_text(session.run("SELECT 1.5 > 1")), [["1"]]);

    // Captured: DIV and MOD truncate toward zero.
    assert_eq!(
        row_text(session.run("SELECT 7 DIV 2, 7 MOD 2, -7 DIV 2")),
        [["3", "1", "-3"]]
    );

    // Captured: a hex or bit literal prints as its bytes.
    assert_eq!(row_text(session.run("SELECT 0x41")), [["A"]]);
    assert_eq!(row_text(session.run("SELECT x'4142'")), [["AB"]]);
    assert_eq!(row_text(session.run("SELECT b'1010'")), [["\n"]]);

    // Captured: and reads as a number in arithmetic.
    assert_eq!(row_text(session.run("SELECT 0x41 + 0")), [["65"]]);
    assert_eq!(row_text(session.run("SELECT b'1010' + 0")), [["10"]]);

    // A decimal literal reaches a stored decimal column and compares
    // against it.
    session.run("CREATE TABLE t (d DECIMAL(10,3))").unwrap();
    session.run("INSERT INTO t VALUES (1.5), (2.25)").unwrap();
    assert_eq!(
        row_text(session.run("SELECT d FROM t WHERE d > 1.4")),
        [["1.500"], ["2.250"]]
    );
}

/// Division by zero, checked against captured TiDB output.
///
/// The value is `NULL` in every case; what the SQL mode decides is whether
/// the statement also warns, fails, or stays silent.
///
/// NOT PORTED from Go's own suites: the coprocessor's own warning
/// merging. TiDB pushes a `WHERE a/0 IS NULL` filter down and reports ONE
/// warning for all the rows a region produced, while three zero divisors
/// in a projection give three warnings; this tier has no coprocessor
/// boundary, so it reports one warning per evaluation everywhere.
#[test]
fn division_by_zero() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();

    // Captured: a query returns NULL and warns 1365.
    assert_eq!(
        session.run("SELECT 1 / 0").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null]])
    );
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1365);
    assert_eq!(session.warnings()[0].message, "Division by 0");
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        [[
            "Warning".to_owned(),
            "1365".to_owned(),
            "Division by 0".to_owned()
        ]]
    );

    // Captured: every zero divisor raises its own warning.
    assert_eq!(
        session.run("SELECT 1 / 0, 2 / 0").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Null, Datum::Null]])
    );
    assert_eq!(session.warnings().len(), 2);
    // DEFERRED (pre-existing rewriter gaps, not this channel's): `DIV`,
    // `MOD` and a decimal literal operand reach the same zero-divisor
    // check in `ops.rs`, but the rewriter does not build those expression
    // forms yet, so they cannot be asserted through the session here.

    // Captured: a zero dividend is ordinary arithmetic, not this case.
    session.run("SELECT 0 / 1").unwrap();
    assert!(session.warnings().is_empty());

    // Captured: under the default SQL mode an INSERT fails with 1365 and
    // writes nothing.
    assert!(matches!(
        session.run("INSERT INTO t VALUES (1 / 0, 1)"),
        Err(DriverError::Exec(tidb_executor::ExecError::Eval(
            tidb_executor::EvalError::DivisionByZero
        )))
    ));
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![])
    );

    // The same holds for UPDATE and DELETE, which Go gives the same level.
    session.run("INSERT INTO t VALUES (1, 1)").unwrap();
    assert!(session.run("UPDATE t SET a = a / 0").is_err());
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );
    assert!(session.run("DELETE FROM t WHERE a = 1 / 0").is_err());
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)]])
    );

    // Captured: without ERROR_FOR_DIVISION_BY_ZERO the condition is
    // ignored entirely -- NULL is written, with no warning at all.
    session.apply_set("SET sql_mode = ''").unwrap();
    session.run("INSERT INTO t VALUES (1 / 0, 2)").unwrap();
    assert!(session.warnings().is_empty());
    assert_eq!(
        session.run("SELECT a FROM t").unwrap(),
        StmtResult::Rows(vec![vec![Datum::Int(1)], vec![Datum::Null]])
    );
    // Captured: a strict mode without that flag ignores it too.
    session
        .apply_set("SET sql_mode = 'STRICT_TRANS_TABLES'")
        .unwrap();
    session.run("INSERT INTO t VALUES (1 / 0, 3)").unwrap();
    assert!(session.warnings().is_empty());

    // Non-strict with the flag warns instead of failing.
    session
        .apply_set("SET sql_mode = 'ERROR_FOR_DIVISION_BY_ZERO'")
        .unwrap();
    session.run("INSERT INTO t VALUES (1 / 0, 4)").unwrap();
    assert_eq!(session.warnings().len(), 1);
    assert_eq!(session.warnings()[0].code, 1365);

    // A query keeps warning whatever the SQL mode says.
    session.apply_set("SET sql_mode = ''").unwrap();
    session.run("SELECT 1 / 0").unwrap();
    assert_eq!(session.warnings().len(), 1);
}

/// `RAND(N)`/`RAND()` through the chunk executor and `ORDER BY RAND()`.
///
/// Captured from Go (`pkg/executor`, a fresh mock session, table `t(a)`
/// holding `(1),(2),(3),(4),(5)`): a constant `RAND(5)` evaluated once
/// per row of a 5-row scan produces the EXACT sequence asserted below --
/// one generator per AST occurrence, seeded once and advanced per row,
/// not reseeded. `ORDER BY RAND()` only needs to permute the rows: Go's
/// own captured order (`[4] [2] [5] [1] [3]`) is one specific shuffle
/// among the seed's many possible ones, so only the SET is checked here,
/// not the exact order.
#[test]
fn rand_constant_sequence_and_order_by_rand() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();

    // A constant RAND(5) evaluated on the SAME row three times in one
    // statement returns the SAME value: MySQL's docs describe RAND(N)
    // as "producing a repeatable sequence", but a single implicit row
    // draws only the sequence's first value from each of these three
    // INDEPENDENT call sites -- they agree because they share both seed
    // and position, not because they are the same generator.
    assert_eq!(
        row_text(session.run("SELECT RAND(5), RAND(5), RAND(5)")),
        [[
            "0.40613597483014313",
            "0.40613597483014313",
            "0.40613597483014313"
        ]]
    );

    // The SAME call site advances across rows, producing Go's exact
    // captured sequence.
    assert_eq!(
        row_text(session.run("SELECT RAND(5) FROM t")),
        [
            ["0.40613597483014313"],
            ["0.8745439358749836"],
            ["0.15431178561813363"],
            ["0.1479271511993624"],
            ["0.276700429876056"],
        ]
    );

    // ORDER BY RAND() must not error and must produce a permutation of
    // every row -- the unseeded sequence itself is not pinned.
    let mut rows: Vec<String> = row_text(session.run("SELECT a FROM t ORDER BY RAND()"))
        .into_iter()
        .flatten()
        .collect();
    rows.sort();
    assert_eq!(rows, ["1", "2", "3", "4", "5"]);
}

/// `SET rand_seed1`/`rand_seed2` are RAW SEEDS for this session's `RAND()`
/// generator, and are never retained as the variables' values.
///
/// Go's two `SysVar`s answer `GetSession` with the constant `"0"`, so every
/// read surface reports 0 no matter what was set or how far the generator has
/// advanced. Only `GetStateValue` -- session-state serialization, which this
/// tier has no surface for -- ever exposes a live seed.
///
/// Captured from real TiDB via `rust/difftests/gorun`
/// (`corpus/table/rand_session`):
///
/// | statements | result |
/// | --- | --- |
/// | `SET rand_seed1=10000000; SET rand_seed2=1000000;` `SELECT RAND(), RAND(), RAND(), @@rand_seed1, @@rand_seed2` | `0.028870999839968048`, `0.11641535266900002`, `0.49546379455874096`, `0`, `0` |
/// | `SET rand_seed1=-1; SET rand_seed2=2147483648;` `SELECT RAND(), @@rand_seed1, @@rand_seed2` | `0.0000000009313225754828403`, `0`, `0` |
/// | `SET rand_seed1=7; SET rand_seed2=11; BEGIN; SET rand_seed1=19; ROLLBACK;` `SELECT RAND(), ...` | `0.00000006332993513283314`, `0`, `0` |
/// | `SET rand_seed1=DEFAULT; SET rand_seed2=DEFAULT;` `SELECT RAND(), ...` | `0`, `0`, `0` |
/// | `SET rand_seed1=12345;` `SELECT @@rand_seed1, @@session.rand_seed1` | `0`, `0` |
/// | `SHOW VARIABLES LIKE 'rand_seed1'` | `rand_seed1`, `0` |
/// | `SET rand_seed1=0; SET rand_seed2=0;` `SELECT RAND(), RAND()` | `0`, `0.00000003073364499093373` |
/// | `SET rand_seed1=-5; SET rand_seed2=100;` `SELECT RAND()` | `0.00000009313225754828403` |
///
/// Four rules the table pins, each load-bearing:
///
///  * out-of-range values arrive NORMALIZED -- `2147483648` clamps to
///    `MaxInt32` (which is why the seed-1 read yields `1/0x3FFFFFFF`, not
///    something else), and a negative clamps to 0 exactly as Go's
///    `tidbOptPositiveInt32` would;
///  * `DEFAULT` really SEEDS, pushing 0 in rather than leaving the last value;
///  * the seeds are session metadata, not transaction state, so a `SET` inside
///    `BEGIN` survives `ROLLBACK`;
///  * `@@`, `@@session.` and `SHOW VARIABLES` agree on 0.
#[test]
fn rand_seed_sysvars_seed_the_generator_and_always_read_back_as_zero() {
    let mut session = Session::new();
    session.run("SET rand_seed1 = 10000000").unwrap();
    session.run("SET rand_seed2 = 1000000").unwrap();
    assert_eq!(
        row_text(session.run("SELECT RAND(), RAND(), RAND(), @@rand_seed1, @@rand_seed2")),
        [[
            "0.028870999839968048",
            "0.11641535266900002",
            "0.49546379455874096",
            "0",
            "0",
        ]]
    );

    // A negative clamps to 0; 2147483648 clamps to MaxInt32.
    session.run("SET rand_seed1 = -1").unwrap();
    session.run("SET rand_seed2 = 2147483648").unwrap();
    assert_eq!(
        row_text(session.run("SELECT RAND(), @@rand_seed1, @@rand_seed2")),
        [["0.0000000009313225754828403", "0", "0"]]
    );

    // Session metadata, not transaction state: the SET survives the ROLLBACK.
    session.run("SET rand_seed1 = 7").unwrap();
    session.run("SET rand_seed2 = 11").unwrap();
    session.run("BEGIN").unwrap();
    session.run("SET rand_seed1 = 19").unwrap();
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        row_text(session.run("SELECT RAND(), @@rand_seed1, @@rand_seed2")),
        [["0.00000006332993513283314", "0", "0"]]
    );

    // DEFAULT seeds 0 rather than leaving the 19 in place.
    session.run("SET rand_seed1 = DEFAULT").unwrap();
    session.run("SET rand_seed2 = DEFAULT").unwrap();
    assert_eq!(
        row_text(session.run("SELECT RAND(), @@rand_seed1, @@rand_seed2")),
        [["0", "0", "0"]]
    );

    // Every read surface reports 0, including SHOW.
    session.run("SET rand_seed1 = 12345").unwrap();
    assert_eq!(
        row_text(session.run("SELECT @@rand_seed1, @@session.rand_seed1")),
        [["0", "0"]]
    );
    assert_eq!(
        row_text(session.run("SHOW VARIABLES LIKE 'rand_seed1'")),
        [["rand_seed1", "0"]]
    );

    // An explicit zero pair is a real seeding, not a no-op.
    session.run("SET rand_seed1 = 0").unwrap();
    session.run("SET rand_seed2 = 0").unwrap();
    assert_eq!(
        row_text(session.run("SELECT RAND(), RAND()")),
        [["0", "0.00000003073364499093373"]]
    );

    session.run("SET rand_seed1 = -5").unwrap();
    session.run("SET rand_seed2 = 100").unwrap();
    assert_eq!(
        row_text(session.run("SELECT RAND()")),
        [["0.00000009313225754828403"]]
    );
}

/// Converting a FLOAT to a DECIMAL, against values captured from a running
/// TiDB.
///
/// Go's `ConvertDatumToDecimal` calls `dec.FromFloat64`, which formats the
/// float with `strconv.FormatFloat(f, 'g', -1, 64)` and feeds that to
/// `FromString`, RETURNING the error. This tier's live path
/// (`tidb-datatype`'s `Datum::to_decimal`) instead formats with Rust's
/// `f64::Display` and discards the error (`event: None`).
///
/// Measured, and this is what splits the finding in two:
///
///  - the VALUES agree with Go on every case tried, including the ones where
///    the two spellings differ (Rust prints `1e308` as 309 digits where Go
///    prints `1e+308`), because `parse_mysql` reaches the same decimal from
///    either spelling. The "rendering diverges" half does not reach an
///    observable answer.
///
///  - the WARNINGS half is now HALF closed. Go raises TWO on the
///    out-of-range cases below, from two different places:
///
///     * 1292 `Truncated incorrect DECIMAL value: '1e+308'` from
///       `builtinCastRealAsDecimalSig`'s own `res.FromFloat64` overflow,
///       whose text is the `'g'` spelling of the ARGUMENT -- still open,
///       and still the discarded-error half above; and
///     * 1690 `DECIMAL value is out of range in '(65, 0)'` from
///       `ProduceDecWithSpecifiedTp`'s clamp, which `tidb-expr`'s
///       `report_decimal_production` now raises.
#[test]
fn float_to_decimal_matches_gos_values_but_not_yet_its_from_float64_warning() {
    let mut session = Session::new();

    // Captured from TiDB.
    assert_eq!(
        row_text(session.run("SELECT cast(1e30 as decimal(65,0))")),
        [["1000000000000000000000000000000"]]
    );
    assert_eq!(
        row_text(session.run("SELECT cast(1.5e-8 as decimal(30,20))")),
        [["0.00000001500000000000"]]
    );
    assert_eq!(
        row_text(session.run("SELECT cast(1e-30 as decimal(35,30))")),
        [["0.000000000000000000000000000001"]]
    );
    // The extremes, where Rust's `Display` and Go's `'g'` spell the input
    // differently and still land on the same decimal.
    assert_eq!(
        row_text(session.run("SELECT cast(5e-324 as decimal(65,30))")),
        [["0.000000000000000000000000000000"]]
    );
    for sql in [
        "SELECT cast(1e308 as decimal(65,0))",
        "SELECT cast(1e300 as decimal(65,0))",
    ] {
        assert_eq!(
            row_text(session.run(sql)),
            [["99999999999999999999999999999999999999999999999999999999999999999"]],
            "{sql}"
        );
        // Go reports 1292 and THEN 1690 here; only the 1690 is modelled,
        // and the missing 1292 is `FromFloat64`'s discarded error.
        assert_eq!(
            row_text(session.run("SHOW WARNINGS")),
            [[
                "Warning".to_owned(),
                "1690".to_owned(),
                "DECIMAL value is out of range in '(65, 0)'".to_owned()
            ]],
            "{sql}"
        );
    }
}
