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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Window-function execution tests.

use super::*;

#[test]
fn ranking_peer_identity_uses_order_comparator() {
    // Go's rank rowComparer asks the typed chunk comparator whether adjacent
    // ordered rows are peers.  Keep that contract at the executor seam: the
    // comparator treats signed and unsigned one as the same sort key even
    // though their tagged Rust Datum variants differ.
    use tidb_ast::{Expr, OrderItem, WindowSpec};

    let db = Database::new();
    let rows = [
        vec![Datum::Int(1)],
        vec![Datum::UInt(1)],
        vec![Datum::Int(2)],
    ];
    let groups: Vec<Vec<&Row>> = rows.iter().map(|row| vec![row]).collect();
    let cols = vec![crate::catalog::Column {
        tables: vec![],
        name: "v".to_string(),
    }];
    let spec = WindowSpec {
        partition_by: vec![],
        order_by: vec![OrderItem {
            expr: Expr::Column(vec!["v".to_string()]),
            desc: false,
        }],
        frame: None,
    };

    assert_eq!(
        db.compute_window("RANK", &[], &spec, &groups, &cols, None)
            .unwrap(),
        vec![Datum::Int(1), Datum::Int(1), Datum::Int(3)]
    );
    assert_eq!(
        db.compute_window("DENSE_RANK", &[], &spec, &groups, &cols, None)
            .unwrap(),
        vec![Datum::Int(1), Datum::Int(1), Datum::Int(2)]
    );
    assert_eq!(
        db.compute_window("PERCENT_RANK", &[], &spec, &groups, &cols, None)
            .unwrap(),
        vec![Datum::Real(0.0), Datum::Real(0.0), Datum::Real(1.0)]
    );
}

#[test]
fn lead_lag_live_runtime_matches_source_vectors_and_partition_reset() {
    let mut db = Database::new();
    step(&mut db, "create table ll (id int, p varchar(10), v int)");
    step(
        &mut db,
        "insert into ll values (1,'a',0),(2,'a',1),(3,'a',2),(4,'b',10),(5,'b',11)",
    );

    // Source TestLeadLag's complete physical-offset set, exercised through
    // Database::compute_window rather than only the cursor leaf.
    assert_eq!(
        step(
            &mut db,
            "select v, lag(v,0) over (partition by p order by id), lag(v,1) over (partition by p order by id), lag(v,2) over (partition by p order by id), lag(v,3) over (partition by p order by id), lag(v,1000000) over (partition by p order by id) from ll where p='a'"
        ),
        "RS:0|0|<nil>|<nil>|<nil>|<nil>;1|1|0|<nil>|<nil>|<nil>;2|2|1|0|<nil>|<nil>"
    );
    assert_eq!(
        step(
            &mut db,
            "select v, lead(v,0) over (partition by p order by id), lead(v,1) over (partition by p order by id), lead(v,2) over (partition by p order by id), lead(v,3) over (partition by p order by id), lead(v,1000000) over (partition by p order by id) from ll where p='a'"
        ),
        "RS:0|0|1|2|<nil>|<nil>;1|1|2|<nil>|<nil>|<nil>;2|2|<nil>|<nil>|<nil>|<nil>"
    );

    // The same complete offset matrix with the source test's constant and
    // current-row default expressions.
    assert_eq!(
        step(
            &mut db,
            "select v, lag(v,0,1000000) over (order by id), lag(v,1,1000000) over (order by id), lag(v,2,1000000) over (order by id), lag(v,3,1000000) over (order by id), lag(v,1000000,1000000) over (order by id) from ll where p='a'"
        ),
        "RS:0|0|1000000|1000000|1000000|1000000;1|1|0|1000000|1000000|1000000;2|2|1|0|1000000|1000000"
    );
    assert_eq!(
        step(
            &mut db,
            "select v, lead(v,0,1000000) over (order by id), lead(v,1,1000000) over (order by id), lead(v,2,1000000) over (order by id), lead(v,3,1000000) over (order by id), lead(v,1000000,1000000) over (order by id) from ll where p='a'"
        ),
        "RS:0|0|1|2|1000000|1000000;1|1|2|1000000|1000000|1000000;2|2|1000000|1000000|1000000|1000000"
    );
    // Go evaluates LEAD's `curIdx + offset` in uint64 and checks the
    // partition bound only after overflow wraps.
    assert_eq!(
        step(
            &mut db,
            "select v, lead(v,18446744073709551615,1000000) over (order by id), lead(v,18446744073709551614,1000000) over (order by id) from ll where p='a'"
        ),
        "RS:0|1000000|1000000;1|0|1000000;2|1|0"
    );
    assert_eq!(
        step(
            &mut db,
            "select v, lag(v,0,v) over (order by id), lag(v,1,v) over (order by id), lag(v,2,v) over (order by id), lag(v,3,v) over (order by id), lag(v,1000000,v) over (order by id) from ll where p='a'"
        ),
        "RS:0|0|0|0|0|0;1|1|0|1|1|1;2|2|1|0|2|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select v, lead(v,0,v) over (order by id), lead(v,1,v) over (order by id), lead(v,2,v) over (order by id), lead(v,3,v) over (order by id), lead(v,1000000,v) over (order by id) from ll where p='a'"
        ),
        "RS:0|0|1|2|0|0;1|1|2|1|1|1;2|2|2|2|2|2"
    );

    // The default expression is evaluated against the current row only when
    // the physical target is out of range. The second partition must start
    // from a reset cursor rather than observing partition a's tail.
    assert_eq!(
        step(
            &mut db,
            "select id, lag(v,2,1000000) over (partition by p order by id), lag(v,2,v) over (partition by p order by id), lead(v,2,1000000) over (partition by p order by id), lead(v,2,v) over (partition by p order by id) from ll"
        ),
        "RS:1|1000000|0|2|2;2|1000000|1|1000000|1;3|0|0|1000000|2;4|1000000|10|1000000|10;5|1000000|11|1000000|11"
    );

    // This executor accepts syntactic integer-literal offsets. Reject a
    // row-derived expression even when it evaluates to the same value for
    // every row instead of silently treating observed equality as literal
    // syntax. Go's broader Constant prepared/deferred boundary remains open.
    assert!(
        step(&mut db, "select lag(v,v-v+1) over (order by id) from ll").starts_with("Unsupported(")
    );
}

#[test]
fn lead_offset_zero_does_not_evaluate_unreachable_default() {
    use tidb_ast::{Expr, WindowSpec};

    let db = Database::new();
    let rows = [vec![Datum::Int(7)], vec![Datum::Int(8)]];
    let groups: Vec<Vec<&Row>> = rows.iter().map(|row| vec![row]).collect();
    let cols = vec![crate::catalog::Column {
        tables: vec![],
        name: "v".to_string(),
    }];
    let args = vec![
        Expr::Column(vec!["v".to_string()]),
        Expr::Int("0".to_string()),
        // Evaluating this against either group would return UnknownColumn.
        Expr::Column(vec!["unreachable_default".to_string()]),
    ];

    assert_eq!(
        db.compute_window(
            "LEAD",
            &args,
            &WindowSpec {
                partition_by: vec![],
                order_by: vec![],
                frame: None,
            },
            &groups,
            &cols,
            None,
        )
        .unwrap(),
        vec![Datum::Int(7), Datum::Int(8)]
    );
}

#[test]
fn window_functions() {
    let mut db = Database::new();
    step(
        &mut db,
        "create table t (id int, dept varchar(10), salary int)",
    );
    step(
        &mut db,
        "insert into t values (1,'a',100),(2,'a',200),(3,'a',200),(4,'b',150),(5,'b',300)",
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, row_number() over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|1;2|a|200|2;3|a|200|3;4|b|150|1;5|b|300|2"
    );
    // RANK gives ties the SAME rank (does not distinguish from
    // DENSE_RANK here, since there's no distinct value after the tie
    // to show the skip -- see the next assertion for that).
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, rank() over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|1;2|a|200|2;3|a|200|2;4|b|150|1;5|b|300|2"
    );
    // No PARTITION BY: ordered across the whole table; output row
    // order is still the ORIGINAL scan order (no top-level ORDER BY).
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, row_number() over (order by salary) from t"
        ),
        "RS:1|a|100|1;2|a|200|3;3|a|200|4;4|b|150|2;5|b|300|5"
    );
    // Empty OVER (): sequential scan order.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, row_number() over () from t"
        ),
        "RS:1|a|100|1;2|a|200|2;3|a|200|3;4|b|150|4;5|b|300|5"
    );
    // A window function referenced directly by the query's own
    // top-level ORDER BY, and TWO distinct window specs in one query.
    assert_eq!(
        step(
            &mut db,
            "select id, rank() over (order by salary), row_number() over (partition by dept) from t"
        ),
        "RS:1|1|1;2|3|2;3|3|3;4|2|1;5|5|2"
    );
    // WHERE filters BEFORE window computation (`t` still has all 5
    // rows from the earlier assertions above).
    assert_eq!(
        step(
            &mut db,
            "select id, salary, row_number() over (order by salary) from t where salary > 100"
        ),
        "RS:2|200|2;3|200|3;4|150|1;5|300|4"
    );

    // RANK vs DENSE_RANK: a NULL sorts first (ascending), and RANK
    // SKIPS past tied rows (position-based) while DENSE_RANK does not
    // (distinct-value-based) -- confirmed via `gorun`, not assumed.
    let mut db2 = Database::new();
    step(&mut db2, "create table u (id int, v int)");
    step(
        &mut db2,
        "insert into u values (1,10),(2,20),(3,20),(4,30),(5,NULL)",
    );
    assert_eq!(
        step(
            &mut db2,
            "select id, v, rank() over (order by v), dense_rank() over (order by v) from u"
        ),
        "RS:1|10|2|2;2|20|3|3;3|20|3|3;4|30|5|4;5|<nil>|1|1"
    );
    // DESC reverses the order; NULL now sorts LAST.
    assert_eq!(
        step(
            &mut db2,
            "select id, v, rank() over (order by v desc) from u"
        ),
        "RS:1|10|4;2|20|2;3|20|2;4|30|1;5|<nil>|5"
    );
    // No ORDER BY within a partition: every row ties (RANK/DENSE_RANK
    // are 1 for all), but ROW_NUMBER still assigns sequential
    // scan-order positions.
    step(&mut db2, "create table dep (id int, dept varchar(10))");
    step(
        &mut db2,
        "insert into dep values (1,'a'),(2,'a'),(3,'a'),(4,'b')",
    );
    assert_eq!(
        step(
            &mut db2,
            "select id, dept, rank() over (partition by dept), row_number() over (partition by dept), dense_rank() over (partition by dept) from dep"
        ),
        "RS:1|a|1|1|1;2|a|1|2|1;3|a|1|3|1;4|b|1|1|1"
    );

    // Frame-based window AGGREGATES (`COUNT`/`SUM`/`AVG`/`MAX`/`MIN`
    // `OVER (...)`), the default frame: no `ORDER BY` -> the WHOLE
    // partition for every row; `ORDER BY` present -> cumulative
    // `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, where
    // `RANGE`'s "CURRENT ROW" means every row sharing the SAME
    // `ORDER BY` key (a peer group), not just the single physical
    // row -- both TIED `salary=200` rows below show the SAME
    // cumulative value (500), confirmed via `gorun`, not assumed.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, sum(salary) over (partition by dept) from t"
        ),
        "RS:1|a|100|500;2|a|200|500;3|a|200|500;4|b|150|450;5|b|300|450"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, sum(salary) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|100;2|a|200|500;3|a|200|500;4|b|150|150;5|b|300|450"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, count(*) over (partition by dept) from t"
        ),
        "RS:1|a|100|3;2|a|200|3;3|a|200|3;4|b|150|2;5|b|300|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, avg(salary) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|100.0000;2|a|200|166.6667;3|a|200|166.6667;4|b|150|150.0000;5|b|300|225.0000"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, max(salary) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|100;2|a|200|200;3|a|200|200;4|b|150|150;5|b|300|300"
    );
    // `COUNT(*)` restores/behaves the same as the literal `1`,
    // matching `Expr::Aggregate`'s own established convention.
    assert_eq!(
        step(&mut db, "select count(*) over () from t"),
        "RS:5;5;5;5;5"
    );

    // LAG/LEAD: PHYSICAL (`ROWS`-style) adjacency within the sorted
    // partition, NOT the frame -- unlike the aggregate/`LAST_VALUE`
    // functions above, two rows TIED on `salary=200` still get their
    // own DISTINCT physical predecessor/successor value (100 and 200
    // respectively for `LAG`), not a shared peer-group value.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, lag(salary) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|<nil>;2|a|200|100;3|a|200|200;4|b|150|<nil>;5|b|300|150"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, lead(salary) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|200;2|a|200|200;3|a|200|<nil>;4|b|150|300;5|b|300|<nil>"
    );
    // An explicit offset, and an explicit out-of-range default value.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, lag(salary, 2) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|<nil>;2|a|200|<nil>;3|a|200|100;4|b|150|<nil>;5|b|300|<nil>"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, lag(salary, 1, 0) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|0;2|a|200|100;3|a|200|200;4|b|150|0;5|b|300|150"
    );
    // `LAG`/`LEAD` work fine with no `ORDER BY` too, using the SAME
    // stable partition scan order as the ranking functions.
    assert_eq!(
        step(
            &mut db,
            "select id, lag(salary) over (partition by dept) from t"
        ),
        "RS:1|<nil>;2|100;3|200;4|<nil>;5|150"
    );
    // A negative offset is a real MySQL error, confirmed via `gorun`.
    assert!(step(
        &mut db,
        "select id, lag(salary, -1) over (order by salary) from t"
    )
    .starts_with("Unsupported("));

    // FIRST_VALUE/LAST_VALUE/NTH_VALUE reuse the SAME default-frame
    // machinery as the aggregate functions above -- `LAST_VALUE`
    // gives BOTH tied `salary=200` rows the SAME peer-group value
    // (200), unlike `LEAD`'s own distinct-per-row physical result.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, first_value(salary) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|100;2|a|200|100;3|a|200|100;4|b|150|150;5|b|300|150"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, last_value(salary) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|100;2|a|200|200;3|a|200|200;4|b|150|150;5|b|300|300"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, nth_value(salary, 2) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|<nil>;2|a|200|200;3|a|200|200;4|b|150|<nil>;5|b|300|300"
    );
    // A position beyond every frame's length is NULL for every row.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, nth_value(salary, 5) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|<nil>;2|a|200|<nil>;3|a|200|<nil>;4|b|150|<nil>;5|b|300|<nil>"
    );
    // No `ORDER BY`: the frame is the whole partition, matching the
    // aggregate functions' own no-`ORDER BY` rule exactly.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, first_value(salary) over (partition by dept) from t"
        ),
        "RS:1|a|100|100;2|a|200|100;3|a|200|100;4|b|150|150;5|b|300|150"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, last_value(salary) over (partition by dept) from t"
        ),
        "RS:1|a|100|200;2|a|200|200;3|a|200|200;4|b|150|300;5|b|300|300"
    );

    // NTILE: PHYSICAL-position-based bucket assignment (NOT
    // peer-group aware -- unlike `LAST_VALUE` above, tied
    // `salary=200` rows land in DIFFERENT buckets here).
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, ntile(2) over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|1;2|a|200|1;3|a|200|2;4|b|150|1;5|b|300|2"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, salary, ntile(3) over (order by salary) from t"
        ),
        "RS:1|100|1;2|200|2;3|200|2;4|150|1;5|300|3"
    );
    // A non-positive `NTILE` argument is a real MySQL error.
    assert!(
        step(&mut db, "select id, ntile(0) over (order by salary) from t")
            .starts_with("Unsupported(")
    );

    // PERCENT_RANK reuses the SAME peer-aware `RANK` computation
    // (`(rank-1)/(partition_len-1)`); CUME_DIST reuses the SAME
    // peer-group-inclusive default frame as `LAST_VALUE`
    // (`frame_len/partition_len`) -- both confirmed via `gorun`,
    // including that a no-`ORDER BY` partition gives `0`/`1`
    // respectively for every row.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, percent_rank() over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|0;2|a|200|0.5;3|a|200|0.5;4|b|150|0;5|b|300|1"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, cume_dist() over (partition by dept order by salary) from t"
        ),
        "RS:1|a|100|0.3333333333333333;2|a|200|1;3|a|200|1;4|b|150|0.5;5|b|300|1"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, percent_rank() over (partition by dept) from t"
        ),
        "RS:1|a|100|0;2|a|200|0;3|a|200|0;4|b|150|0;5|b|300|0"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, cume_dist() over (partition by dept) from t"
        ),
        "RS:1|a|100|1;2|a|200|1;3|a|200|1;4|b|150|1;5|b|300|1"
    );
    // A single-row partition is a documented special case for
    // `PERCENT_RANK` (`0`, not a `0/0` division).
    let mut db3 = Database::new();
    step(
        &mut db3,
        "create table single (id int, dept varchar(10), salary int)",
    );
    step(&mut db3, "insert into single values (1,'a',100)");
    assert_eq!(
        step(
            &mut db3,
            "select id, dept, salary, percent_rank() over (partition by dept order by salary) from single"
        ),
        "RS:1|a|100|0"
    );
    assert_eq!(
        step(
            &mut db3,
            "select id, dept, salary, cume_dist() over (partition by dept order by salary) from single"
        ),
        "RS:1|a|100|1"
    );
    assert_eq!(
        step(
            &mut db3,
            "select id, dept, salary, ntile(3) over (partition by dept order by salary) from single"
        ),
        "RS:1|a|100|1"
    );

    // Deliberate scope boundaries: `DISTINCT` in a window aggregate and
    // `IGNORE NULLS` (confirmed via `gorun` that real TiDB itself
    // rejects it too) stay a `ParseError` rather than silently
    // misparsing or computing a wrong value.
    assert!(
        tidb_parser::parse("select max(distinct salary) over (partition by dept) from t").is_err()
    );
    assert!(
        tidb_parser::parse("select lag(salary) ignore nulls over (order by salary) from t")
            .is_err()
    );

    // `RANGE` frames: value-distance against the single `ORDER BY`
    // key, not physical position -- `50 PRECEDING`/`50 FOLLOWING`
    // around each row's own `salary` (sorted ASC: 100, 150, 200, 200,
    // 300).
    assert_eq!(
        step(
            &mut db,
            "select id, salary, sum(salary) over (order by salary range between 50 preceding and 50 following) from t order by id"
        ),
        "RS:1|100|250;2|200|550;3|200|550;4|150|650;5|300|300"
    );
    // `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is
    // byte-identical to the implicit default frame -- both peer-group
    // cumulative through each tied `salary` value.
    assert_eq!(
        step(
            &mut db,
            "select id, salary, sum(salary) over (order by salary range between unbounded preceding and current row) from t order by id"
        ),
        "RS:1|100|100;2|200|650;3|200|650;4|150|250;5|300|950"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, salary, sum(salary) over (order by salary) from t order by id"
        ),
        "RS:1|100|100;2|200|650;3|200|650;4|150|250;5|300|950"
    );
    // `DESC` flips which side of the current row's value `PRECEDING`/
    // `FOLLOWING` extend into, since both are defined relative to
    // SCAN order, not raw value order -- an asymmetric offset pair
    // (100 PRECEDING, 10 FOLLOWING) makes `ASC` and `DESC` diverge
    // observably.
    assert_eq!(
        step(
            &mut db,
            "select id, salary, sum(salary) over (order by salary range between 100 preceding and 10 following) from t order by id"
        ),
        "RS:1|100|100;2|200|650;3|200|650;4|150|250;5|300|700"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, salary, sum(salary) over (order by salary desc range between 100 preceding and 10 following) from t order by id"
        ),
        "RS:1|100|650;2|200|700;3|200|700;4|150|550;5|300|300"
    );
    // A `PRECEDING`/`FOLLOWING` numeric offset requires EXACTLY one
    // `ORDER BY` column -- zero or two both error -- but
    // `UnboundedPreceding`/`CurrentRow`-only bounds need no
    // arithmetic and work under ANY column count, including a
    // STRING key (`dept`), matching `ROWS`'s own unrestricted arity.
    assert!(step(
        &mut db,
        "select id, sum(salary) over (range between 50 preceding and 50 following) from t"
    )
    .starts_with("Unsupported("));
    assert!(step(&mut db, "select id, sum(salary) over (order by dept, salary range between 50 preceding and 50 following) from t").starts_with("Unsupported("));
    assert_eq!(
        step(
            &mut db,
            "select id, sum(salary) over (order by dept range between unbounded preceding and current row) from t order by id"
        ),
        "RS:1|500;2|500;3|500;4|950;5|950"
    );
    assert_eq!(
        step(
            &mut db,
            "select id, sum(salary) over (range between unbounded preceding and current row) from t order by id"
        ),
        "RS:1|950;2|950;3|950;4|950;5|950"
    );
    // A numeric offset against a STRING-ordered column is a genuine
    // execution-time error, same as any other non-numeric arithmetic
    // (`apply_binary`'s own `EvalError::Unsupported`, wrapped as
    // `ExecError::Eval`).
    assert!(step(&mut db, "select id, sum(salary) over (order by dept range between 50 preceding and 50 following) from t").starts_with("Eval("));
    // A negative offset errors, same as `ROWS`'s own non-negative
    // requirement.
    assert!(step(&mut db, "select id, sum(salary) over (order by salary range between -5 preceding and 5 following) from t").starts_with("Unsupported("));
    // `DECIMAL` arithmetic on the `ORDER BY` key works correctly for
    // `RANGE` offset computation.
    let mut db_dec = Database::new();
    step(&mut db_dec, "create table d (id int, amt decimal(10,2))");
    step(
        &mut db_dec,
        "insert into d values (1,10.50),(2,12.00),(3,15.00)",
    );
    assert_eq!(
        step(
            &mut db_dec,
            "select id, amt, sum(amt) over (order by amt range between 2.00 preceding and 2.00 following) from d order by id"
        ),
        "RS:1|10.50|22.50;2|12.00|22.50;3|15.00|15.00"
    );

    // Named windows: `OVER w` (bare) references a `WINDOW w AS (...)`
    // clause's own definition directly.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, row_number() over w from t window w as (partition by dept order by salary) order by id"
        ),
        "RS:1|a|100|1;2|a|200|2;3|a|200|3;4|b|150|1;5|b|300|2"
    );
    // `OVER (w ...)` extends a named window with an ADDITIONAL clause
    // the base doesn't already have (here, `ORDER BY`) -- confirmed
    // via `gorun` this is a real, distinct grammar shape from `OVER
    // w`'s own bare form, not just an alternate spelling.
    assert_eq!(
        step(
            &mut db,
            "select id, dept, salary, sum(salary) over (w order by salary) from t window w as (partition by dept) order by id"
        ),
        "RS:1|a|100|100;2|a|200|500;3|a|200|500;4|b|150|150;5|b|300|450"
    );
    // A `WINDOW` clause that defines a name no `OVER` clause actually
    // references is a harmless no-op, not an error.
    assert_eq!(
        step(
            &mut db,
            "select id from t window w as (partition by dept order by salary) order by id"
        ),
        "RS:1;2;3;4;5"
    );
    // Extending a named window can never re-specify `PARTITION BY`,
    // and can add `ORDER BY`/a frame only when the base doesn't
    // already have one -- all confirmed via `gorun` to be real
    // EXECUTION-time errors (the grammar parses every combination
    // syntactically), not silently accepted or misapplied.
    assert!(step(
        &mut db,
        "select sum(salary) over (w partition by dept) from t window w as (order by salary)"
    )
    .starts_with("Unsupported("));
    assert!(step(
            &mut db,
            "select sum(salary) over (w order by salary) from t window w as (partition by dept order by salary)"
        )
        .starts_with("Unsupported("));
    assert!(step(
            &mut db,
            "select sum(salary) over (w rows between 1 preceding and current row) from t window w as (partition by dept rows between 1 preceding and current row)"
        )
        .starts_with("Unsupported("));
    // A self-referencing (circular) window definition is a genuine
    // error too, not an infinite loop.
    assert!(step(
        &mut db,
        "select sum(salary) over w from t window w as (w order by salary)"
    )
    .starts_with("Unsupported("));
    // A named window may itself extend ANOTHER one, chaining the same
    // rules transitively -- confirmed via `gorun` this works
    // regardless of which order the two are WRITTEN in.
    assert_eq!(
        step(
            &mut db,
            "select id, sum(salary) over w2 from t window w1 as (partition by dept order by salary), w2 as (w1 rows between 1 preceding and current row) order by id"
        ),
        "RS:1|100;2|300;3|400;4|150;5|450"
    );
    // Window functions combined with GROUP BY (confirmed via `gorun`):
    // the window computes over the post-aggregation "virtual rows,"
    // one per group -- dept 'a' (3 rows) and dept 'b' (2 rows), in
    // group-discovery (scan) order since there's no top-level ORDER BY.
    assert_eq!(
        step(
            &mut db,
            "select dept, count(*), row_number() over (order by dept) from t group by dept"
        ),
        "RS:a|3|1;b|2|2"
    );
    // A window function in `HAVING` is rejected (confirmed via `gorun`
    // that real TiDB rejects it too) -- naturally, since `HAVING` is
    // evaluated via `eval_group` before any window resolution runs,
    // so a raw `Expr::Window` node reaches `eval_in`, which has no
    // notion of it.
    assert!(step(&mut db, "select dept, count(*) c, rank() over (order by dept) from t group by dept having rank() over (order by dept) > 1").starts_with("Eval("));
    assert!(step(
        &mut db,
        "select id, dept, salary from t where row_number() over (order by id) > 1"
    )
    .starts_with("Eval("));
    // The top-level `ORDER BY` may reference a window-function's own
    // select-list alias (ordinary alias resolution, not special to
    // windows -- `crate::order::resolve_alias`).
    assert_eq!(
        step(
            &mut db,
            "select dept, sum(salary), rank() over (order by sum(salary) desc) rnk from t group by dept order by rnk"
        ),
        "RS:a|500|1;b|450|2"
    );
    // A window's own argument/PARTITION BY/ORDER BY may NOT reference
    // a select-list alias (confirmed via `gorun`) -- `c` here is only
    // a display alias for `COUNT(*)`, and is rejected as an ungrouped
    // column, the same as any other bare, non-`GROUP BY` column would
    // be.
    assert!(step(
        &mut db,
        "select dept, sum(salary) s, count(*) c, sum(c) over (order by dept) from t group by dept"
    )
    .starts_with("UngroupedColumn"));
    // A window's own PARTITION BY may reference an aggregate
    // expression directly (confirmed via `gorun`): both departments'
    // SUM(salary) exceed 400, so they land in the SAME partition.
    assert_eq!(
        step(
            &mut db,
            "select dept, count(*), rank() over (partition by sum(salary) > 400 order by dept) from t group by dept"
        ),
        "RS:a|3|1;b|2|2"
    );
    // A window AGGREGATE's own argument may itself be an aggregate
    // expression (confirmed via `gorun`): each group contributes ONE
    // resolved `SUM(salary)` value, and those per-group sums are what
    // the outer `SUM(...) OVER (...)` accumulates -- 500, then
    // 500+450=950 -- not a re-aggregation over raw rows.
    assert_eq!(
        step(
            &mut db,
            "select dept, count(*) c, sum(sum(salary)) over (order by dept) from t group by dept"
        ),
        "RS:a|3|500;b|2|950"
    );

    // Explicit ROWS frame clause (confirmed via `gorun`): a PHYSICAL
    // row-offset range around the current row, NOT peer-group aware
    // (unlike the implicit default RANGE-cumulative frame every
    // aggregate/value function above falls back to without one).
    let mut db4 = Database::new();
    step(&mut db4, "create table t (id int, dept varchar(10), v int)");
    step(
        &mut db4,
        "insert into t values (1,'a',10),(2,'a',20),(3,'a',20),(4,'b',30),(5,'b',40)",
    );
    assert_eq!(
        step(
            &mut db4,
            "select id, v, sum(v) over (order by id rows between 1 preceding and 1 following) from t"
        ),
        "RS:1|10|30;2|20|50;3|20|70;4|30|90;5|40|70"
    );
    assert_eq!(
        step(
            &mut db4,
            "select id, v, sum(v) over (order by id rows 2 preceding) from t"
        ),
        "RS:1|10|10;2|20|30;3|20|50;4|30|70;5|40|90"
    );
    // Two rows TIED on `v` (id 2 and 3, both `v=20`) get their OWN
    // distinct `ROWS`-frame value, unlike the default frame's
    // peer-group sharing.
    assert_eq!(
        step(
            &mut db4,
            "select id, v, sum(v) over (order by v rows between 1 preceding and current row) from t"
        ),
        "RS:1|10|10;2|20|30;3|20|40;4|30|50;5|40|70"
    );
    // A same-kind bound pair that happens to invert at runtime
    // (`2 FOLLOWING` before `1 FOLLOWING`) silently yields an empty
    // frame (`NULL`/`0`) for every row, not an error.
    assert_eq!(
        step(
            &mut db4,
            "select id, v, sum(v) over (order by id rows between 2 following and 1 following) from t"
        ),
        "RS:1|10|<nil>;2|20|<nil>;3|20|<nil>;4|30|<nil>;5|40|<nil>"
    );
    assert_eq!(
        step(
            &mut db4,
            "select id, v, count(*) over (order by id rows between 1 following and 2 following) from t"
        ),
        "RS:1|10|2;2|20|2;3|20|2;4|30|1;5|40|0"
    );
    // A frame whose start bound outranks its end bound
    // (`UnboundedPreceding < Preceding < CurrentRow < Following <
    // UnboundedFollowing`) is a genuine execution error regardless of
    // row position or the individual bounds' own offsets.
    assert!(step(
        &mut db4,
        "select id, v, sum(v) over (order by id rows between current row and 1 preceding) from t"
    )
    .starts_with("Unsupported"));
    // `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` over an explicit frame.
    assert_eq!(
        step(
            &mut db4,
            "select id, v, first_value(v) over (order by id rows between 1 preceding and 1 following) from t"
        ),
        "RS:1|10|10;2|20|10;3|20|20;4|30|20;5|40|30"
    );
    assert_eq!(
        step(
            &mut db4,
            "select id, v, nth_value(v, 2) over (order by id rows between 1 preceding and 1 following) from t"
        ),
        "RS:1|10|20;2|20|20;3|20|20;4|30|30;5|40|40"
    );
    // A frame clause is accepted but has NO effect on a
    // non-frame-eligible function (confirmed via `gorun`).
    assert_eq!(
        step(
            &mut db4,
            "select id, row_number() over (order by id rows between 1 preceding and 1 following) from t"
        ),
        "RS:1|1;2|2;3|3;4|4;5|5"
    );
    // Combines with window+`GROUP BY`: each group contributes ONE
    // resolved value to the explicit frame, same as the default-frame
    // case above.
    assert_eq!(
        step(
            &mut db4,
            "select dept, sum(v) s, sum(sum(v)) over (order by dept rows between 1 preceding and current row) from t group by dept"
        ),
        "RS:a|50|50;b|70|120"
    );
    // `RANGE` shares the SAME frame grammar/validity rules as `ROWS`
    // (the start-ranks-after-end static error; a same-kind inverted
    // bound pair silently yielding an empty/`NULL` frame instead) --
    // and, against a column with NO ties (`id`), reduces to the exact
    // SAME per-row result as the equivalent `ROWS` frame above.
    assert_eq!(
        step(
            &mut db4,
            "select id, v, sum(v) over (order by id range between 1 preceding and 1 following) from t"
        ),
        "RS:1|10|30;2|20|50;3|20|70;4|30|90;5|40|70"
    );
    assert_eq!(
        step(
            &mut db4,
            "select id, v, sum(v) over (order by id range between 2 following and 1 following) from t"
        ),
        "RS:1|10|<nil>;2|20|<nil>;3|20|<nil>;4|30|<nil>;5|40|<nil>"
    );
    assert!(step(
        &mut db4,
        "select id, v, sum(v) over (order by id range between current row and 1 preceding) from t"
    )
    .starts_with("Unsupported"));
}
