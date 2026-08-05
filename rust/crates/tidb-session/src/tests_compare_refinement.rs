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

//! `int column <cmp> non-int constant`: the constant is folded against the
//! column's type ONCE, at build time, not re-coerced on every row.
//!
//! # The signature that makes this a semantic gap, not a diagnostics nit
//!
//! `SELECT * FROM <t> WHERE a > '10ab'` over an INT column raised
//! `Truncated incorrect DOUBLE value: '10ab'` ONCE PER SCANNED ROW here --
//! 11 for `t`, 5 for `trange`, 11 for `thash` -- while TiDB raises it TWICE
//! for all three. TiDB's count does not move with the table because the
//! coercion never reaches a row; ours did, because it happened inside the
//! evaluation loop. A warning multiplicity that tracks row count is the
//! observable shadow of a per-row cost TiDB does not pay.
//!
//! # The Go that decides it
//!
//! `compareFunctionClass.refineArgs` (`pkg/expression/builtin_compare.go`
//! :1778) rewrites the arguments before the signature is chosen. Its
//! `int non-constant [cmp] non-int constant` arm (:1811-1813) calls
//! `RefineComparedConstant(ctx, *arg0Type, arg1, c.op)` (:1574); the mirrored
//! arm (:1838-1840) does the same with `symmetricOp[c.op]` when the constant
//! is on the left. `refineArgs` itself is called from
//! `compareFunctionClass.getFunction` (:1984), so EVERY `lt/le/gt/ge/eq/ne/
//! nulleq` built through the function class goes through it.
//!
//! `RefineComparedConstant` first converts the constant to the column's type
//! (:1585-1598). That conversion is warning one. It then compares the
//! converted value against the original (:1600): when they are equal the int
//! is exact and is returned as-is. When they differ -- '10ab' converts to 10
//! but 10 != '10ab' -- the operator decides the rounding direction:
//!
//! ```text
//! case opcode.LT, opcode.GE:   ast.Ceil    builtin_compare.go:1613-1614
//! case opcode.LE, opcode.GT:   ast.Floor   builtin_compare.go:1618-1619
//! ```
//!
//! `a > '10ab'` is GT, so it takes the `Floor` fold, and the fold's own
//! string->double coercion is warning two. `tryToConvertConstantInt`
//! (:1516-1564) then turns the folded constant into the column's int type.
//! Two conversions, both at build time, and the comparison that survives is
//! `gt(a, 10)` -- int to int, so no row ever coerces a string.
//!
//! TiDB's own recording shows exactly that plan
//! (`tests/integrationtest/r/executor/partition/partition_with_expression.result`
//! :1239): `gt(executor__partition__partition_with_expression.trange.a, 10)`.

use super::Session;
use crate::tests_support::row_text;

/// The three tables of `TestDynamicPruneModeWithExpression`
/// (`tests/integrationtest/t/executor/partition/partition_with_expression.test`
/// :137-143), verbatim.
fn partition_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "create table trange(a int, b int) partition by range(a) (partition p0 values less than(3), partition p1 values less than (5), partition p2 values less than(11))",
        "create table thash(a int, b int) partition by hash(a) partitions 4",
        "create table t(a int, b int)",
        "insert into trange values(1, NULL), (1, NULL), (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 7), (7, 7), (7, 7), (10, NULL), (NULL, NULL), (NULL, 1)",
        "insert into thash values(1, NULL), (1, NULL), (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 7), (7, 7), (7, 7), (10, NULL), (NULL, NULL), (NULL, 1)",
        "insert into t values(1, NULL), (1, NULL), (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 7), (7, 7), (7, 7), (10, NULL), (NULL, NULL), (NULL, 1)",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    session
}

fn warning_texts(session: &Session) -> Vec<String> {
    session
        .warnings()
        .iter()
        .map(|w| format!("{} {}", w.code, w.message))
        .collect()
}

/// The reported unit: the warning count is the SAME for all three tables,
/// because the coercion happens once at build time and never in the scan.
///
/// Asserting table-independence is the point. A per-row coercion cannot
/// produce equal counts over a 13-row heap, a range-partitioned table and a
/// hash-partitioned one; only a build-time fold can.
#[test]
fn int_column_gt_string_constant_warns_twice_regardless_of_table() {
    let mut session = partition_session();
    let mut counts = Vec::new();
    for table in ["t", "trange", "thash"] {
        let sql = format!("SELECT * from {table} where a > '10ab'");
        // TiDB's recording: no row is greater than 10 in any of the three.
        assert_eq!(
            row_text(session.run(&sql)),
            Vec::<Vec<String>>::new(),
            "{sql}"
        );
        let texts = warning_texts(&session);
        assert!(
            texts
                .iter()
                .all(|t| t.starts_with("1292 Truncated incorrect DOUBLE value")),
            "{sql} -> {texts:?}"
        );
        // Both channels, which have been proven independent here before.
        assert_eq!(
            session.wire_warning_count(),
            u16::try_from(texts.len()).unwrap(),
            "wire count disagrees with the buffer for {sql}"
        );
        counts.push((table, texts.len()));
    }
    assert_eq!(
        counts,
        vec![("t", 2), ("trange", 2), ("thash", 2)],
        "TiDB raises the truncation twice per statement for all three tables; a count that \
         moves with the table means the string is being coerced inside the scan"
    );
}

/// The same fact measured against ROW COUNT directly: a table an order of
/// magnitude larger still warns twice.
///
/// [`int_column_gt_string_constant_warns_twice_regardless_of_table`] proves
/// the count does not move with the TABLE; this proves it does not move with
/// the number of rows that table holds, which is what a per-row coercion
/// would track and what no amount of deduplication could produce (a
/// deduplicating sink would answer ONE, not two).
#[test]
fn the_warning_count_does_not_grow_with_the_scanned_rows() {
    let mut session = Session::new();
    session.run("create table big(a int, b int)").unwrap();
    for start in (1..=200).step_by(20) {
        let values = (start..start + 20)
            .map(|i| format!("({i}, {i})"))
            .collect::<Vec<_>>()
            .join(", ");
        session
            .run(&format!("insert into big values {values}"))
            .unwrap();
    }
    assert_eq!(
        row_text(session.run("select count(*) from big")),
        [["200"]],
        "the fixture must be big enough for a per-row count to be unmistakable"
    );
    let rows = row_text(session.run("SELECT * from big where a > '10ab'"));
    assert_eq!(rows.len(), 190, "a > 10 over 1..200");
    assert_eq!(warning_texts(&session).len(), 2);
    assert_eq!(session.wire_warning_count(), 2);
}

/// EXPLAIN still prints the constant AS WRITTEN, and that is a REMAINING gap,
/// not a passing assertion: TiDB's own recording prints the refined form
/// (`gt(executor__partition__partition_with_expression.trange.a, 10)`,
/// `tests/integrationtest/r/executor/partition/partition_with_expression.result`
/// :1239).
///
/// The cause is structural and is NOT the refinement: this tier's plan trace
/// renders the WRITTEN AST (`PlanTrace::selection` takes the `tidb_ast::Expr`),
/// while the refinement rewrites the built `Expression` the scan evaluates.
/// So the predicate that RUNS is int-to-int -- which is what
/// `tidb_expr::builtin_compare`'s own tests assert structurally, and what the
/// two warning-count tests above measure -- and only the printed text lags.
/// Closing it means teaching the trace to print built expressions, which is a
/// change to the recorder rather than to this rule.
#[test]
#[ignore = "EXPLAIN prints the written AST, not the refined expression"]
fn explain_still_prints_the_written_constant() {
    let mut session = partition_session();
    let plan = row_text(session.run("explain select * from t where a > '10ab'"))
        .iter()
        .map(|row| row.join(" "))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("gt(") && plan.contains(", 10)") && !plan.contains("10ab"),
        "TiDB prints the refined constant; got:\n{plan}"
    );
}

/// The ACCESS-PATH question, answered: refinement is not what lets an
/// `int column <cmp> string constant` become an index range here, because the
/// ranger already coerces the constant itself.
///
/// This was the expensive half of the hypothesis -- that a string-coerced
/// predicate could not build a range at all, silently costing index access on
/// ordinary integer predicates. It does not: `a > '10ab'` and `a > 10` reach
/// the SAME `range:(10,+inf]`, and so does TiDB (`gorun`, covering index:
/// `IndexRangeScan_5 ... range:(10,+inf]` for both spellings). The remaining
/// value of the refinement is therefore the per-row coercion it removes and
/// the warning multiplicity that measured it -- not an access path.
#[test]
fn a_string_constant_does_not_cost_the_index_range() {
    let mut session = Session::new();
    session
        .run("create table ti(a int, b int, key ia(a))")
        .unwrap();
    session
        .run("insert into ti values(1,1),(2,2),(3,3),(10,10),(20,20)")
        .unwrap();
    let plan_of = |session: &mut Session, sql: &str| {
        row_text(session.run(sql))
            .iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n")
    };
    // A covering read, so the index path is the one the cost model picks.
    let written = plan_of(&mut session, "explain select a from ti where a > '10ab'");
    let refined = plan_of(&mut session, "explain select a from ti where a > 10");
    for plan in [&written, &refined] {
        assert!(
            plan.contains("IndexRangeScan") && plan.contains("range:(10,+inf]"),
            "expected the same index range from both spellings; got:\n{plan}"
        );
    }
}

/// `TestCompareIssue38361` (`tests/integrationtest/t/executor/executor.test`):
/// a DATETIME/TIMESTAMP compared with a numeric value is compared in the REAL
/// domain -- `getBaseCmpType(ETDatetime, ETInt)` is ETReal -- so a datetime
/// column against a bigint column (or a non-convertible int constant) reads
/// the datetime's numeric form (`YYYYMMDDHHMMSS`) rather than parsing the
/// number as a datetime and dropping the row. A numeric CONSTANT that DOES
/// convert to a datetime is refined to a datetime constant first
/// (`refineNumericConstantCmpDatetime`), so it compares in the datetime
/// domain instead. The rows here are TiDB's recorded values verbatim.
///
/// This pins both coercion arms: mutate the value-level Time-vs-numeric route
/// back to a datetime parse and `a < c` / `a > b` change (and `a < 20231310`
/// becomes NULL); drop the rule-3 refinement and `a > 20230809` flips from 0
/// (datetime equal) to 1 (real `20230809000000 > 20230809`).
#[test]
fn datetime_compared_with_numeric_is_real_except_convertible_constant() {
    let mut session = Session::new();
    for sql in [
        "create table t(a datetime, b bigint, c bigint)",
        "insert into t values(cast('2023-08-09 00:00:00' as datetime), 20230809, 20231310)",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    // (query, TiDB's recorded scalar result)
    let cases = [
        // datetime column vs int CONSTANT: rule 3 converts the constant to a
        // datetime when it is valid, so these compare in the datetime domain.
        ("select a > 20230809 from t", "0"),
        ("select a = 20230809 from t", "1"),
        ("select a < 20230810 from t", "1"),
        ("select 20230809 = a from t", "1"),
        ("select 20230810 > a from t", "1"),
        // 20231310 (month 13) does NOT convert, so it compares as real.
        ("select a < 20231310 from t", "0"),
        ("select 20231310 > a from t", "0"),
        // datetime column vs bigint COLUMN: always real.
        ("select a > b from t", "1"),
        ("select a = b from t", "0"),
        ("select a < b + 1 from t", "0"),
        ("select a < c from t", "0"),
        ("select b < a from t", "1"),
        ("select b = a from t", "0"),
        ("select c > a from t", "0"),
    ];
    for (sql, want) in cases {
        assert_eq!(
            row_text(session.run(sql)),
            vec![vec![want.to_string()]],
            "{sql}"
        );
    }
}

/// `year_col <cmp> <int constant>`: the constant moves through MySQL's
/// two-digit YEAR window ONCE, at build time, so `y < 30` means `y < 2030`.
///
/// # The silent row loss this closes
///
/// `select * from ty where y < 30` over `(2018, 0, 1999, 2069, 1970)` returned
/// ONE row here and FOUR in TiDB, and its mirror `30 > y` did the same: a
/// YEAR column reaches the value evaluator as a plain `Datum::Int`, so
/// `2018 < 30` decided every row. No error, no warning -- three rows simply
/// were not there.
///
/// # The Go that decides it
///
/// `compareFunctionClass.refineArgs` (`pkg/expression/builtin_compare.go`
/// :1856-1873) has one arm per side, both reading the OTHER argument's static
/// type:
///
/// ```go
/// // year type [cmp] int constant
/// if arg1IsCon && arg1IsInt && arg0Type.GetType() == mysql.TypeYear && !arg1.Value.IsNull() {
///     adjusted, failed := types.AdjustYear(arg1.Value.GetInt64(), false)
///     if failed == nil {
///         arg1.Value.SetInt64(adjusted)
///         finalArg1 = arg1
///     }
/// }
/// ```
///
/// and `types.AdjustYear` (`pkg/types/time.go:1278`) is the window itself:
/// `0..=69` becomes `2000+y`, `70..=99` becomes `1900+y`, a literal `0` stays
/// `0` because `adjustZero` is false, and anything outside `1901..=2155`
/// reports an error, which the `failed == nil` gate turns into "leave the
/// comparison exactly as written".
///
/// A STRING constant takes a different route to the same place:
/// `RefineComparedConstant`'s `EQ`/`ETString` arm (`:1633-1657`) returns the
/// constant converted to the column's YEAR type, and THAT conversion passes
/// `adjustZero = len(s) != 4` -- which is why `y = '0'` is `2000` and matches
/// nothing while `y = '0000'` is `0` and matches the zero year.
///
/// Every expectation below is quoted from `gorun` over this exact fixture.
#[test]
fn year_column_compared_with_an_int_constant_takes_the_two_digit_window() {
    let mut session = Session::new();
    for sql in [
        "create table ty(k int, y year)",
        "insert into ty values (1,2018),(2,0),(3,1999),(4,2069),(5,1970)",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }

    // `RS:1|2018;2|0;3|1999;5|1970` -- FOUR of the five rows, and which one is
    // missing is the whole proof: 30 became 2030, so 2069 is the only year
    // above it. Unadjusted, `y < 30` selects the zero year ALONE.
    let below_2030 = vec![
        vec!["1".to_string(), "2018".to_string()],
        vec!["2".to_string(), "0".to_string()],
        vec!["3".to_string(), "1999".to_string()],
        vec!["5".to_string(), "1970".to_string()],
    ];
    assert_eq!(
        row_text(session.run("select k,y from ty where y < 30 order by k")),
        below_2030
    );
    // `symmetricOp`'s mirror: the constant on the LEFT is the OTHER of Go's
    // two arms (`arg0IsCon && arg0IsInt && arg1Type.GetType() == mysql.TypeYear`).
    assert_eq!(
        row_text(session.run("select k,y from ty where 30 > y order by k")),
        below_2030
    );
    // `RS:2|0;3|1999;4|2069;5|1970` -- `!=` is refined too, so 2018 is the
    // row that leaves rather than the row that stays.
    assert_eq!(
        row_text(session.run("select k,y from ty where y != 18 order by k")),
        vec![
            vec!["2".to_string(), "0".to_string()],
            vec!["3".to_string(), "1999".to_string()],
            vec!["4".to_string(), "2069".to_string()],
            vec!["5".to_string(), "1970".to_string()],
        ]
    );
    // The window's own boundary, as a PARTITION: 70 is 1970 and 69 is 2069,
    // so these two predicates split the table at a point no unadjusted
    // comparison can produce -- `y >= 70` keeps every row but the zero year.
    // `RS:1|2018;3|1999;4|2069;5|1970` and `RS:2|0`.
    assert_eq!(
        row_text(session.run("select k,y from ty where y >= 70 order by k")),
        vec![
            vec!["1".to_string(), "2018".to_string()],
            vec!["3".to_string(), "1999".to_string()],
            vec!["4".to_string(), "2069".to_string()],
            vec!["5".to_string(), "1970".to_string()],
        ]
    );
    assert_eq!(
        row_text(session.run("select k,y from ty where y < 70 order by k")),
        vec![vec!["2".to_string(), "0".to_string()]]
    );

    // The same rules read as SCALARS, one column per constant, so the
    // `AdjustYear` mapping is pinned value by value rather than only through
    // a row set. Quoted from `gorun`:
    //
    // ```text
    // select k, y=69, y=70, y=18, y="18", y="0", y="0000", y=0 from ty order by k;
    // RS:1|0|0|1|1|0|0|0;2|0|0|0|0|0|1|1;3|0|0|0|0|0|0|0;4|1|0|0|0|0|0|0;5|0|1|0|0|0|0|0
    // ```
    //
    // Column by column: `69` is 2069 (row 4 alone), `70` is 1970 (row 5
    // alone) -- the 69/70 hinge, which no other rule puts THERE. `'18'` is
    // 2018 exactly as the integer 18 is, which is `RefineComparedConstant`'s
    // ETString arm. `'0'` is 2000 and matches NOTHING while `'0000'` and the
    // integer `0` are both the zero year: the `adjustZero = len(s) != 4`
    // split, and the reason Go's own arm passes `false` for an integer.
    let scalars = "select k, y=69, y=70, y=18, y=\"18\", y=\"0\", y=\"0000\", y=0 from ty order by k";
    assert_eq!(
        row_text(session.run(scalars)),
        vec![
            vec!["1", "0", "0", "1", "1", "0", "0", "0"],
            vec!["2", "0", "0", "0", "0", "0", "1", "1"],
            vec!["3", "0", "0", "0", "0", "0", "0", "0"],
            vec!["4", "1", "0", "0", "0", "0", "0", "0"],
            vec!["5", "0", "1", "0", "0", "0", "0", "0"],
        ]
        .into_iter()
        .map(|row| row.into_iter().map(String::from).collect::<Vec<_>>())
        .collect::<Vec<_>>()
    );

    // Out of the YEAR domain: `AdjustYear` reports `ErrWarnDataOutOfRange`
    // alongside a CLAMPED value, and Go keeps the clamp only when there was
    // no error -- so 2156 stays 2156 and selects nothing rather than being
    // silently pulled down to the 2155 boundary, where it would have matched
    // a row. `RS:` (empty) for both halves.
    assert_eq!(
        row_text(session.run("select k,y from ty where y = 2156")),
        Vec::<Vec<String>>::new()
    );
    // `IN` is NOT `compareFunctionClass`, so `refineArgs` never runs on it:
    // Go answers `RS:` here even though `y = 18` finds a row. This is the
    // control that keeps the adjustment from being applied by some broader
    // rule than the one Go wrote.
    assert_eq!(
        row_text(session.run("select k,y from ty where y in (18, 99)")),
        Vec::<Vec<String>>::new()
    );
}

/// `CAST(<year column> AS CHAR)` renders a zero YEAR as `'0000'`, which no
/// other `Datum::Int(0)` does.
///
/// Go `builtinCastIntAsStringSig.evalString` (`builtin_cast.go:1090-1099`)
/// formats the integer first and then overrides ONE rendering from the
/// source's static type:
///
/// ```go
/// tp := b.args[0].GetType(ctx)
/// if !mysql.HasUnsignedFlag(tp.GetFlag()) {
///     res = strconv.FormatInt(val, 10)
/// } else {
///     res = strconv.FormatUint(uint64(val), 10)
/// }
/// if tp.GetType() == mysql.TypeYear && res == "0" {
///     res = "0000"
/// }
/// ```
///
/// The INT column in the same row is the control: the two datums are both
/// `Datum::Int(0)` and only the static type separates them.
#[test]
fn casting_a_zero_year_to_char_renders_four_digits() {
    let mut session = Session::new();
    for sql in [
        "create table ty(k int, y year)",
        "insert into ty values (1,2018),(2,0)",
    ] {
        session
            .run(sql)
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    }
    // ```text
    // select cast(y as char), length(cast(y as char)), hex(cast(y as binary)) from ty order by k;
    // RS:2018|4|32303138;0000|4|30303030
    // ```
    //
    // `length` and `hex` are there because `'0000'` and `'0'` are not
    // distinguishable by eye in a result grid, and the BINARY target proves
    // the rendering happens in the SIGNATURE rather than in the CHAR arm --
    // Go picks `builtinCastIntAsStringSig` for both and only the padding
    // differs.
    assert_eq!(
        row_text(session.run(
            "select cast(y as char), length(cast(y as char)), hex(cast(y as binary)) from ty order by k"
        )),
        vec![
            vec!["2018".to_string(), "4".to_string(), "32303138".to_string()],
            vec!["0000".to_string(), "4".to_string(), "30303030".to_string()],
        ]
    );
    // The control: `k` is an ordinary INT holding the same `Datum::Int`
    // values, and `RS:1;2` shows it is untouched.
    assert_eq!(
        row_text(session.run("select cast(k as char) from ty order by k")),
        vec![vec!["1".to_string()], vec!["2".to_string()]]
    );
}
