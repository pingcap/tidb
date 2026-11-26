//! Aggregate edge semantics: over an EMPTY table the scalar aggregation
//! answers one row (COUNT 0, SUM/MAX NULL) while GROUP BY answers none;
//! HAVING without GROUP BY filters that scalar row.

use tidb_session::Session;

use crate::support::testkit::TestKit;

use crate::support::nullable_integer_rows_with_sql as rows;

#[test]
fn empty_table_and_having_without_group_by() {
    let mut session = Session::new();
    session.run("create table t (a int)").unwrap();

    // COUNT is 0; SUM/MAX are NULL — but the row itself still exists.
    assert_eq!(
        rows(&mut session, "select count(*), count(a), sum(a), max(a) from t"),
        "0|0|NULL|NULL"
    );
    // With GROUP BY there are no groups, so no rows.
    assert_eq!(rows(&mut session, "select a, count(*) from t group by a"), "");

    session.run("insert into t values (5)").unwrap();
    // HAVING keeps the scalar row when it passes...
    assert_eq!(rows(&mut session, "select count(*) from t having count(*) > 0"), "1");
    // ...and drops it when it fails.
    assert_eq!(rows(&mut session, "select count(*) from t having count(*) > 5"), "");
}

/// Go master `TestBinaryLiteralNumericAggregation` (#71234): SUM and AVG read
/// a binary literal as a number; MIN, MAX and COUNT keep its bytes.
#[test]
fn binary_literal_numeric_aggregation() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table bit_agg(a int)");
    tk.must_exec("insert into bit_agg values(1),(1),(2)");
    tk.must_query("select a,sum(b'101010'),avg(b'101010') from bit_agg group by a order by a")
        .check(["1 84 42", "2 42 42"]);
    tk.must_query("select a,sum(x'31'),avg(x'31') from bit_agg group by a order by a").check(["1 98 49", "2 49 49"]);
    tk.must_query("select a,sum('1'),avg('1') from bit_agg group by a order by a").check(["1 2 1", "2 1 1"]);
    tk.must_query("select hex(min(b'101010')),hex(max(b'101010')),count(b'101010') from bit_agg").check(["2A 2A 3"]);
    tk.must_query("select sum(b'101010'),avg(b'101010') from bit_agg where a=3").check(["<nil> <nil>"]);
}

/// Go master `TestBinaryLiteralMinMaxHaving` (#71235): HAVING does not swap a
/// binary-literal MIN/MAX for the raw literal, whose numeric coercion differs.
#[test]
fn binary_literal_min_max_having() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    for key in [" primary key", ""] {
        tk.must_exec(&format!("create table binary_having(a int{key})"));
        tk.must_exec("insert into binary_having values(1),(2)");
        tk.must_query("select abs(min(b'101010')) c2,1 c3 from binary_having group by a having any_value(c2)")
            .check(Vec::<&str>::new());
        tk.must_query("select a from binary_having group by a having max(x'31')=49").check(Vec::<&str>::new());
        tk.must_query("select a from binary_having group by a having max(x'31')=1 order by a").check(["1", "2"]);
        tk.must_query("select a from binary_having group by a having min(49)=49 order by a").check(["1", "2"]);
        tk.must_query("select a from binary_having group by a having min('1')=1 order by a").check(["1", "2"]);
        tk.must_exec("drop table binary_having");
    }
}
