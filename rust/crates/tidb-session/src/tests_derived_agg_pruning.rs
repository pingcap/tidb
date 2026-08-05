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

//! `driver::derived_agg_pruning` over the wire: an ungrouped aggregation in a
//! derived table nobody reads keeps only the row it produces, and the index
//! that can answer a row count answers it.
//!
//! Every row set and every `IndexFullScan index:c2(c2)` below was captured
//! from real TiDB with `rust/difftests/gorun` over this exact fixture. The
//! rewrite changes WHAT THE DERIVED TABLE COMPUTES, so the row assertions are
//! the load-bearing half: an aggregation returns one row over an empty table
//! too, which is the property that makes replacing it with `count(1)` legal
//! rather than merely cheaper.

#![cfg(test)]

use crate::tests_support::cell_text;
use crate::{Session, StmtResult};

/// The rows of `sql`, sorted, `|`-joined per row.
fn rows(session: &mut Session, sql: &str) -> Vec<String> {
    let mut out = match session.run(sql).unwrap() {
        StmtResult::Rows(rows) => rows
            .into_iter()
            .map(|row| row.iter().map(cell_text).collect::<Vec<_>>().join("|"))
            .collect::<Vec<_>>(),
        other => panic!("expected rows from `{sql}`, got {other:?}"),
    };
    out.sort();
    out
}

/// Whether the plan for `sql` reads an index rather than the whole row -- the
/// observable effect of pruning the aggregation's arguments away.
fn reads_the_narrow_index(session: &mut Session, sql: &str) -> bool {
    let plan = match session.run(&format!("EXPLAIN {sql}")).unwrap() {
        StmtResult::Rows(rows) => rows,
        other => panic!("expected rows from EXPLAIN, got {other:?}"),
    };
    plan.iter()
        .any(|row| cell_text(&row[3]).contains("index:c2(c2)"))
}

/// `p1` is `explain_easy`'s own `t1` -- `c1` the handle, a narrow `c2(c2)`
/// index, and `c3` reachable only through the row. `pe` is the same table
/// left EMPTY, which is where "an ungrouped aggregation still returns one
/// row" is observable.
fn pruning_session() -> Session {
    let mut session = Session::new();
    for sql in [
        "create table p1 (c1 int primary key, c2 int, c3 int, index c2 (c2))",
        "create table pe (c1 int primary key, c2 int, c3 int, index c2 (c2))",
        "insert into p1 values (1,1,1),(2,null,2),(3,3,null)",
    ] {
        session.run(sql).unwrap();
    }
    session
}

#[test]
fn an_unread_ungrouped_aggregation_keeps_its_row_and_reads_the_index() {
    let mut session = pruning_session();
    // gorun: RS:1, plan `IndexReader -> HashAgg(count(1)) -> IndexFullScan
    // index:c2(c2)`. Neither `c2` nor `c3` is fetched even though both are
    // written, because nothing above the derived table reads either count.
    let sql = "select 1 from (select count(c2), count(c3) from p1) k";
    assert_eq!(rows(&mut session, sql), ["1"]);
    assert!(reads_the_narrow_index(&mut session, sql));

    // gorun: RS:1, same access path. `max(c2)` is pruned along with the count.
    let sql = "select count(1) from (select max(c2), count(c3) as m from p1) k";
    assert_eq!(rows(&mut session, sql), ["1"]);
    assert!(reads_the_narrow_index(&mut session, sql));

    // gorun: RS:1, same access path. Go's `ExprsHasSideEffects` is over the
    // ARGUMENTS, so a `DISTINCT` aggregate is prunable like any other.
    let sql = "select 1 from (select count(distinct c2) from p1) k";
    assert_eq!(rows(&mut session, sql), ["1"]);
    assert!(reads_the_narrow_index(&mut session, sql));
}

#[test]
fn an_empty_table_still_yields_the_aggregations_one_row() {
    let mut session = pruning_session();
    // gorun: RS:1 for both. This is the whole reason Go appends `count(1)`
    // instead of deleting the aggregation: an UNGROUPED aggregation over an
    // empty table returns one row, and the parent counts it.
    assert_eq!(
        rows(
            &mut session,
            "select 1 from (select count(c2), count(c3) from pe) k"
        ),
        ["1"]
    );
    assert_eq!(
        rows(
            &mut session,
            "select count(*) from (select sum(c2) from pe) k"
        ),
        ["1"]
    );
}

#[test]
fn the_derived_where_and_group_by_are_untouched() {
    let mut session = pruning_session();
    // gorun: RS:1 -- the rewrite replaces the FIELD LIST only, so the derived
    // `WHERE` still runs and the aggregation still sees one row.
    assert_eq!(
        rows(
            &mut session,
            "select 1 from (select count(c2) from p1 where c3 = 1) k"
        ),
        ["1"]
    );
    // gorun: RS:1;1;1 -- a GROUPED aggregation's row count depends on the
    // data, so it is refused and all three groups survive.
    assert_eq!(
        rows(
            &mut session,
            "select 1 from (select count(c2) from p1 group by c3) k"
        ),
        ["1", "1", "1"]
    );
}

#[test]
fn a_read_derived_column_is_refused() {
    let mut session = pruning_session();
    // gorun: RS:2 -- `count(c2)` skips the NULL row, so the value is 2 and
    // not 3. Rewriting it to `count(1)` would answer 3.
    assert_eq!(
        rows(
            &mut session,
            "select k.n from (select count(c2) as n from p1) k"
        ),
        ["2"]
    );
    // gorun: RS:1 -- the outer `WHERE` reads the derived column, so the
    // aggregate must still be computed even though the outer projects a
    // literal.
    assert_eq!(
        rows(
            &mut session,
            "select 1 from (select count(c2) as n from p1) k where k.n > 0"
        ),
        ["1"]
    );
}

#[test]
fn a_shape_whose_row_count_depends_on_what_is_computed_is_refused() {
    let mut session = pruning_session();
    // gorun: RS:1;1;1 -- a plain projection is not an aggregation at all, so
    // its row count is the table's. Rewriting it to `count(1)` would answer
    // ONE row.
    assert_eq!(
        rows(&mut session, "select 1 from (select c2 from p1) k"),
        ["1", "1", "1"]
    );
    // gorun: RS:1;1;1 -- and this is the one the demand check does NOT also
    // catch: the derived column is NAMED `c2 + 0`, which no reference in the
    // outer statement can collide with, so only "every field is an aggregate"
    // stands between this and a three-row answer collapsing to one.
    assert_eq!(
        rows(&mut session, "select 1 from (select c2 + 0 from p1) k"),
        ["1", "1", "1"]
    );
    // gorun: RS:1;1;1 -- a grouped aggregation with a plain grouping key
    // among its fields, refused for the same reason.
    assert_eq!(
        rows(
            &mut session,
            "select 1 from (select c2, count(c3) from p1 group by c2) k"
        ),
        ["1", "1", "1"]
    );
    // gorun: RS: (no rows) and RS:1 -- a `HAVING` decides whether the
    // aggregation's one row survives at all, so the aggregate it tests must
    // still be computed.
    assert_eq!(
        rows(
            &mut session,
            "select 1 from (select count(c2) from p1 having count(c2) > 5) k"
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        rows(
            &mut session,
            "select 1 from (select count(c2) from p1 having count(c2) > 1) k"
        ),
        ["1"]
    );
    // gorun: RS: (no rows) -- a `LIMIT 0` removes the row the parent counts.
    assert_eq!(
        rows(
            &mut session,
            "select 1 from (select count(c2) from p1 limit 0) k"
        ),
        Vec::<String>::new()
    );
}
