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

//! `WITH` (common table expression) execution tests,
//! including `WITH RECURSIVE`.

use super::*;

#[test]
fn cte() {
    let mut db = Database::new();
    step(&mut db, "create table t (id int, v int)");
    step(&mut db, "insert into t values (1, 10), (2, 20), (3, 30)");
    assert_eq!(
        step(
            &mut db,
            "with a as (select id, v from t where v > 10) select * from a"
        ),
        "RS:2|20;3|30"
    );
    assert_eq!(
        step(
            &mut db,
            "with a as (select id, v from t where v > 10) select * from a order by id desc"
        ),
        "RS:3|30;2|20"
    );
    // A later CTE may reference an earlier one (non-recursive
    // chaining, not recursion).
    assert_eq!(
        step(
            &mut db,
            "with a as (select id, v from t where v > 10), b as (select id from a where v > 20) select * from b"
        ),
        "RS:3"
    );
    assert_eq!(
        step(
            &mut db,
            "with a as (select id, v from t) select count(*) from a"
        ),
        "RS:3"
    );
    // An explicit column rename list.
    assert_eq!(
        step(
            &mut db,
            "with a (x, y) as (select id, v from t) select x, y from a where x = 2"
        ),
        "RS:2|20"
    );
    // The SAME CTE referenced twice in one query (a self-join) --
    // re-resolved fresh at each reference, not materialized once.
    assert_eq!(
        step(
            &mut db,
            "with a as (select id, v from t where v > 10) select t1.id, t2.id from a t1 join a t2 on t1.id != t2.id"
        ),
        "RS:2|3;3|2"
    );
    // A CTE sharing a real table's name shadows it in the OUTER
    // query, but NOT inside the CTE's own defining query (confirmed
    // via `gorun`, not assumed).
    assert_eq!(
        step(
            &mut db,
            "with t as (select id, v * 10 as v from t) select * from t"
        ),
        "RS:1|100;2|200;3|300"
    );
    assert_eq!(step(&mut db, "select * from t"), "RS:1|10;2|20;3|30");

    // A CTE body can own its own non-recursive WITH scope. The inner name
    // must resolve while building the outer CTE, then disappear outside it.
    assert_eq!(
        step(
            &mut db,
            "with outer_cte as (with inner_cte as (select id, v from t where id = 2) select v from inner_cte) select v + 1 from outer_cte",
        ),
        "RS:21"
    );

    // `RECURSIVE` on a CTE with no actual `UNION`/self-reference
    // degenerates to a single execution, same as a non-recursive CTE
    // (confirmed via `gorun`).
    assert_eq!(
        step(
            &mut db,
            "with recursive r as (select 1 as n) select * from r"
        ),
        "RS:1"
    );
    // The classic recursive-counter pattern: base term seeds `n=1`,
    // the recursive term adds 1 while `n<5` -- confirmed via `gorun`
    // that `UNION ALL` and `UNION` give the SAME result here (no
    // duplicates ever arise in this particular sequence).
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union all select n + 1 from cte where n < 5) select * from cte"
        ),
        "RS:1;2;3;4;5"
    );
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union select n + 1 from cte where n < 5) select n from cte order by n"
        ),
        "RS:1;2;3;4;5"
    );
    // A recursive term may also join a REAL table.
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union all select cte.n + 1 from cte join t on t.id = 1 where cte.n < t.v) select n from cte order by n"
        ),
        "RS:1;2;3;4;5;6;7;8;9;10"
    );
    // A non-recursive CTE may still appear inside a `WITH RECURSIVE`
    // clause (`RECURSIVE` is a CLAUSE-level flag, not per-CTE,
    // confirmed via `gorun`) and reference an earlier, genuinely
    // recursive one.
    assert_eq!(
        step(
            &mut db,
            "with recursive a as (select 1 as n union all select n+1 from a where n<2), b as (select * from a) select * from b"
        ),
        "RS:1;2"
    );
    // The materialized CTE may be referenced MORE than once at the
    // OUTER query level (a self-join of the recursive CTE's own
    // final result, as opposed to inside its own recursive term,
    // which is rejected -- see below).
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union all select n+1 from cte where n<2) select * from cte c1 join cte c2 on c1.n = c2.n"
        ),
        "RS:1|1;2|2"
    );
    // `UNION` deduplicates across the WHOLE accumulated result, not
    // just within one round's own new rows -- a diamond-shaped graph
    // (1->2, 1->3, 2->4, 3->4) reaches node 4 via TWO paths, so
    // `UNION ALL` shows it twice but `UNION` shows it once.
    step(&mut db, "create table edges (src int, dst int)");
    step(&mut db, "insert into edges values (1,2),(1,3),(2,4),(3,4)");
    assert_eq!(
        step(
            &mut db,
            "with recursive reach as (select 1 as n union all select e.dst from edges e join reach r on e.src = r.n) select n from reach order by n"
        ),
        "RS:1;2;3;4;4"
    );
    assert_eq!(
        step(
            &mut db,
            "with recursive reach as (select 1 as n union select e.dst from edges e join reach r on e.src = r.n) select n from reach order by n"
        ),
        "RS:1;2;3;4"
    );
    // Deliberate scope boundaries, all confirmed via `gorun`: a
    // self-join WITHIN a recursive term (two references to the same
    // CTE), an aggregate, `ORDER BY`, and `RANGE`... `DISTINCT`
    // inside a recursive term are all genuine `ERR`s in real TiDB,
    // not silently accepted here either.
    assert!(step(&mut db, "with recursive cte as (select 1 as n union all select c1.n + c2.n from cte c1, cte c2 where c1.n < 3) select * from cte limit 5").starts_with("Unsupported("));
    assert!(step(&mut db, "with recursive cte as (select 1 as n union all select count(*) from cte) select * from cte limit 3").starts_with("Unsupported("));
    assert!(step(&mut db, "with recursive cte as (select 1 as n union all select n+1 from cte where n < 5 order by n) select * from cte").starts_with("Unsupported("));
    assert!(step(&mut db, "with recursive cte as (select 1 as n union all select distinct n+1 from cte where n < 5) select * from cte").starts_with("Unsupported("));

    // Unlike `ORDER BY`, a `LIMIT` on the recursive CTE's own
    // definition IS supported -- a real early-termination
    // optimization (confirmed via `gorun`): it caps the TOTAL
    // accumulated row count across every round, stopping the
    // fixpoint EARLY, well before the `WHERE` clause alone would
    // (`n < 1000000` would otherwise take ~1M rounds).
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union all select n+1 from cte where n<1000000 limit 5) select n from cte order by n"
        ),
        "RS:1;2;3;4;5"
    );
    // `OFFSET` windows the SAME capped total, exactly like an
    // ordinary `LIMIT offset, count` elsewhere.
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union all select n+1 from cte where n<1000000 limit 3 offset 2) select n from cte order by n"
        ),
        "RS:3;4;5"
    );
    // `LIMIT 0` short-circuits before any recursive round runs at
    // all (the seed alone already meets the target).
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union all select n+1 from cte where n<1000000 limit 0) select n from cte"
        ),
        "RS:"
    );
    // A `LIMIT` exceeding the fixpoint's own NATURAL termination
    // point is a no-op -- the `WHERE` clause still ends the
    // recursion first.
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union all select n+1 from cte where n<3 limit 100) select n from cte order by n"
        ),
        "RS:1;2;3"
    );
    // `UNION` (dedup, not `UNION ALL`) combines with `LIMIT` the
    // same way.
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union select n+1 from cte where n<1000000 limit 5) select n from cte order by n"
        ),
        "RS:1;2;3;4;5"
    );

    // A `UNION`-bodied CTE that never references itself is legal even
    // WITHOUT `RECURSIVE` on the clause (confirmed via `gorun`) --
    // evaluated once and folded, exactly like an ordinary top-level
    // `UNION` statement (mixed `UNION`/`UNION ALL` terms included).
    assert_eq!(
        step(
            &mut db,
            "with cte as (select 1 as n union select 2 as n) select * from cte order by n"
        ),
        "RS:1;2"
    );
    assert_eq!(
        step(
            &mut db,
            "with cte as (select 1 as n union all select 2 as n union select 1 as n) select * from cte order by n"
        ),
        "RS:1;2"
    );
    // `RECURSIVE` on the clause but no term self-references still
    // degenerates to the same single-evaluation path (a `QueryStmt::Select`
    // CTE already does this above; a `QueryStmt::SetOpr` one must too).
    assert_eq!(
        step(
            &mut db,
            "with recursive cte as (select 1 as n union select 2 as n) select * from cte order by n"
        ),
        "RS:1;2"
    );
    // A later CTE's own UNION body may reference an earlier one.
    assert_eq!(
        step(
            &mut db,
            "with a as (select 1 as n), b as (select * from a union select 2 as n) select * from b order by n"
        ),
        "RS:1;2"
    );
    // The materialized CTE may be self-joined at the OUTER level.
    assert_eq!(
        step(
            &mut db,
            "with cte as (select 1 as n union select 2 as n) select * from cte t1 join cte t2 on t1.n = t2.n"
        ),
        "RS:1|1;2|2"
    );
    // The CTE's own `ORDER BY`/`LIMIT` apply to its body BEFORE the
    // outer query sees it -- unlike the recursive-fixpoint case, where
    // this is a deliberate `Unsupported` boundary (see above). Ordered
    // by the alias `n` -- see `crate::order::output_index`'s own doc
    // for why this resolves (a separate, previously-pre-existing gap,
    // now fixed).
    assert_eq!(
        step(
            &mut db,
            "with cte as (select 1 as n union select 2 as n union select 3 as n order by n desc limit 2) select count(*) from cte"
        ),
        "RS:2"
    );
    // Self-referencing a UNION-bodied CTE WITHOUT `RECURSIVE` on the
    // clause is a real error, not a silent non-recursive evaluation:
    // the self-reference resolves to no table at all (confirmed via
    // `gorun`).
    assert!(step(
        &mut db,
        "with cte as (select 1 as n union all select n+1 from cte where n<5) select * from cte"
    )
    .starts_with("UnknownTable"));
}

/// The parser accepts the source-backed recursive-CTE/LATERAL shapes, but
/// execution remains an honest boundary: the relation engine has no
/// per-outer-row re-evaluation analogue for a lateral derived table. Reject
/// both seed variants before attempting catalog or transaction mutation.
#[test]
fn recursive_lateral_cte_rejected_before_execution() {
    let mut db = Database::new();
    step(&mut db, "create table t2 (a int, b varchar(10))");
    for seed in ["''", "cast(null as char(10))"] {
        let sql = format!(
            "with recursive tr (level, b_col) as (select 1, {seed} union all select level + 1, lat.result_col from tr, lateral (select b as result_col from t2 where a = level order by b limit 1) as lat where level < 3) select * from tr where level > 0 order by level"
        );
        assert!(
            step(&mut db, &sql).starts_with("Unsupported("),
            "recursive LATERAL must stay an execution boundary: {sql}"
        );
    }
}
