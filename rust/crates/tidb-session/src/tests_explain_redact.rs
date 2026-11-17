//! Go `tidb_redact_log` in EXPLAIN: each operator renders its constants,
//! ranges, offsets and handles as `EnableRedactLog` says (`?` for `ON`,
//! `‹v›` for `MARKER`).

use super::*;
use crate::tests_support::row_text;

/// The brief EXPLAIN rows as Go prints them (`id|estRows|task|access
/// object|operator info`), sorted.
fn brief_rows(session: &mut Session, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = row_text(session.run(sql)).into_iter().map(|row| row.join("|")).collect();
    rows.sort();
    rows
}

/// Captured from Go's testkit (`tidb-wt/tools/e31`): `SET GLOBAL
/// tidb_redact_log` to `ON`, then to `MARKER`, over
/// `t (a int primary key, b int, c varchar(10), key(b))`, a recursive CTE
/// with a LIMIT, and a ROWS window frame. The testkit prints the CTE's cast
/// as `bigint(1)`; a Go tidb-server deprecates integer display widths and
/// prints `bigint`.
#[test]
fn explain_redacts_values_as_tidb_redact_log_says() {
    let mut session = Session::new();
    session.run("create table t (a int primary key, b int, c varchar(10), key(b))").unwrap();
    session.run("set global tidb_redact_log='ON'").unwrap();
    let on: Vec<(&str, Vec<&str>)> = vec![
        ("explain format='brief' select * from t where a > 1 and b < 3 limit 2", vec!["      └─TableRangeScan|6.02|cop[tikv]|table:t|range:(?,+inf], keep order:false, stats:pseudo", "    └─Selection|2.00|cop[tikv]||lt(test.t.b, ?)", "  └─Limit|2.00|cop[tikv]||offset:?, count:?", "Limit|2.00|root||offset:?, count:?", "└─TableReader|2.00|root||data:Limit"]),
        ("explain format='brief' select * from t where c = 'x' order by b limit 3", vec!["      └─TableFullScan|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo", "    └─Selection|10.00|cop[tikv]||eq(test.t.c, ?)", "  └─TopN|3.00|cop[tikv]||test.t.b, offset:?, count:?", "TopN|3.00|root||test.t.b, offset:?, count:?", "└─TableReader|3.00|root||data:TopN"]),
        ("explain format='brief' select a+1, c from t where b in (1,2)", vec!["  └─TableRowIDScan(Probe)|20.00|cop[tikv]|table:t|keep order:false, stats:pseudo", "  ├─IndexRangeScan(Build)|20.00|cop[tikv]|table:t, index:b(b)|range:[?,?], [?,?], keep order:false, stats:pseudo", "Projection|20.00|root||plus(test.t.a, ?)->Column#5, test.t.c", "└─IndexLookUp|20.00|root||"]),
        ("explain format='brief' select * from t where a = 5", vec!["Point_Get|1.00|root|table:t|handle:?"]),
        ("explain format='brief' select * from t where a in (5,6)", vec!["Batch_Point_Get|2.00|root|table:t|handle:[? ?], keep order:false, desc:false"]),
        ("explain format='brief' with recursive cte(n) as (select 1 union all select n+1 from cte where n < 5 limit 2) select * from cte", vec!["    └─CTETable|1.00|root||Scan on CTE_0", "  └─Selection|0.80|root||lt(Column#3, ?)", "CTEFullScan|1.80|root|CTE:cte|data:CTE_0", "CTE_0|1.80|root||Recursive CTE, limit(offset:?, count:?)", "│ └─TableDual|1.00|root||rows:1", "└─Projection(Recursive Part)|0.80|root||cast(plus(Column#3, ?), bigint BINARY)->Column#5", "├─Projection(Seed Part)|1.00|root||?->Column#2"]),
    ];
    for (sql, want) in on {
        assert_eq!(brief_rows(&mut session, sql), want, "{sql}");
    }
    session.run("set global tidb_redact_log='MARKER'").unwrap();
    let marker: Vec<(&str, Vec<&str>)> = vec![
        ("explain format='brief' select * from t where a > 1 and b < 3 limit 2", vec!["      └─TableRangeScan|6.02|cop[tikv]|table:t|range:(‹1›,+inf], keep order:false, stats:pseudo", "    └─Selection|2.00|cop[tikv]||lt(test.t.b, ‹3›)", "  └─Limit|2.00|cop[tikv]||offset:‹0›, count:‹2›", "Limit|2.00|root||offset:‹0›, count:‹2›", "└─TableReader|2.00|root||data:Limit"]),
        ("explain format='brief' select a+1, c from t where b in (1,2)", vec!["  └─TableRowIDScan(Probe)|20.00|cop[tikv]|table:t|keep order:false, stats:pseudo", "  ├─IndexRangeScan(Build)|20.00|cop[tikv]|table:t, index:b(b)|range:[‹1›,‹1›], [‹2›,‹2›], keep order:false, stats:pseudo", "Projection|20.00|root||plus(test.t.a, ‹1›)->Column#5, test.t.c", "└─IndexLookUp|20.00|root||"]),
        ("explain format='brief' with recursive cte(n) as (select 1 union all select n+1 from cte where n < 5 limit 2) select * from cte", vec!["    └─CTETable|1.00|root||Scan on CTE_0", "  └─Selection|0.80|root||lt(Column#3, ‹5›)", "CTEFullScan|1.80|root|CTE:cte|data:CTE_0", "CTE_0|1.80|root||Recursive CTE, limit(offset:‹0›, count:‹2›)", "│ └─TableDual|1.00|root||rows:1", "└─Projection(Recursive Part)|0.80|root||cast(plus(Column#3, ‹1›), bigint BINARY)->Column#5", "├─Projection(Seed Part)|1.00|root||‹1›->Column#2"]),
        ("explain format='brief' select sum(a) over (order by a rows between 2 preceding and 1 following) from (select 1 a) t", vec!["    └─TableDual|1.00|root||rows:1", "  └─Projection|1.00|root||‹1›->Column#1", "Projection|1.00|root||Column#3->Column#4", "└─Window|1.00|root||sum(cast(Column#1, decimal(20,0) BINARY))->Column#3 over(order by Column#1 rows between ‹2› preceding and ‹1› following)"]),
    ];
    for (sql, want) in marker {
        assert_eq!(brief_rows(&mut session, sql), want, "{sql}");
    }
    session.run("set global tidb_redact_log='OFF'").unwrap();
}
