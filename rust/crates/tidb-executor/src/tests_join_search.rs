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

//! THE ROW SOURCE AND THE JOIN-STRATEGY SEARCH, observed through the driver.
//!
//! Every test here runs a whole statement and reads
//! [`crate::driver::join_search::ANSWERS`] -- what the chooser was asked and
//! what it answered at each join site. That recorder is the only way to see
//! the answer for a statement that is NOT being explained, which is the
//! comparison [`the_choice_is_the_same_under_explain_and_bare_execution`]
//! exists to make.
//!
//! The oracle for the row counts is TiDB's own, quoted per test.

use tidb_datatype::Datum;

use crate::driver::join_search::{Answer, Chosen, Refusal, ANSWERS};
use crate::Catalog;
use crate::StmtContext;

/// The schema `t/planner/core/join_reorder_through_projection.test` creates
/// for the statements below, with no statistics loaded -- which is the state
/// the recording was made in (`stats:pseudo` in every recorded row).
fn tables() -> Catalog {
    let mut catalog = Catalog::default();
    for name in ["t1", "t2", "t3", "t4"] {
        crate::run_create_table_on(
            &format!("CREATE TABLE {name} (a INT, b INT, c VARCHAR(32), PRIMARY KEY (a), KEY(b))"),
            &mut catalog,
        )
        .unwrap();
    }
    catalog
}

/// The session the topic runs these statements in.
fn ctx() -> StmtContext {
    StmtContext::for_query()
        .with_join_reorder_threshold(10)
        .with_join_reorder_through_proj(true)
}

/// `r/planner/core/join_reorder_through_projection.result:1042`'s statement:
/// the one whose recorded plan is an `IndexHashJoin` under two `MergeJoin`s.
const RESULT_1042: &str = "select outer_t.a, dt2.* from t1 outer_t, \
     (select dt1.key_a, dt1.doubled_b + 10 as adjusted from \
     (select t2.a as key_a, t2.b * 2 as doubled_b from t2 join t3 on t2.a = t3.a) dt1 \
     join t4 on dt1.key_a = t4.a) dt2 \
     where outer_t.b = dt2.adjusted";

/// A three-way group whose recorded plan is a chain of `MergeJoin`s, so a
/// non-empty property DOES reach the join sites below the top one.
const MERGE_CHAIN: &str = "select t1.a, t2.a from t1, t2, t3 \
     where t1.a = t2.a and t2.a = t3.a";

/// Runs `sql` through `EXPLAIN` and returns what the chooser answered.
fn answers_explained(sql: &str, catalog: &Catalog) -> Vec<Answer> {
    let stmt = tidb_parser::parse(sql).unwrap();
    let tidb_ast::Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    ANSWERS.with(|answers| answers.borrow_mut().clear());
    crate::explain::explain_select_stmt(
        select,
        catalog,
        "test",
        &ctx(),
        crate::explain::ExplainFormat::Row,
    )
    .unwrap();
    ANSWERS.with(|answers| answers.borrow().clone())
}

/// Runs `sql` as an ordinary statement -- no recorder, no `EXPLAIN` -- and
/// returns what the chooser answered.
fn answers_bare(sql: &str, catalog: &Catalog) -> Vec<Answer> {
    ANSWERS.with(|answers| answers.borrow_mut().clear());
    crate::run_select_on(sql, catalog, &ctx()).unwrap();
    ANSWERS.with(|answers| answers.borrow().clone())
}

/// THE ROW SOURCE, against TiDB's own numbers.
///
/// `EXPLAIN FORMAT='cost_trace'` from a `tidb-server` built from this tree,
/// in a session carrying mysql-tester's DSN variables, prints for
/// `result:1042`'s statement:
///
/// ```text
/// Projection_23     15625.00
/// IndexHashJoin_51  10000.00
/// TableReader_55     8000.00   (the t2 side of that join)
/// IndexReader_69     9990.00   (the t1 side)
/// ```
///
/// and the join above the index join carries `12500.00`. Those five numbers
/// are what [`crate::driver::join_reorder::RowSource`] derives here, from the
/// statement, the catalog and the (pseudo) statistics alone.
///
/// This is also the mutation probe on the derivation: a row count off by ANY
/// factor -- a missing `not(isnull(...))` on `t1.b` (`9990` -> `10000`), a
/// dropped `SelectionFactor` on `t2` (`8000` -> `10000`), a join estimate
/// that divides by the wrong NDV -- turns one of these equalities red.
#[test]
fn the_row_source_reproduces_every_row_count_tidb_records_for_result_1042() {
    let catalog = tables();
    let answers = answers_explained(RESULT_1042, &catalog);
    let sites: Vec<String> = answers
        .iter()
        .map(|answer| {
            let rows = answer.rows.expect("the estimate owner answered");
            format!(
                "{} x {} -> {:.2} x {:.2} = {:.2}",
                answer.left.join(","),
                answer.right.join(","),
                rows.left,
                rows.right,
                rows.joined,
            )
        })
        .collect();
    assert_eq!(
        sites,
        vec![
            // `IndexReader_69 9990.00` x `TableReader_55 8000.00` ->
            // `IndexHashJoin_51 10000.00`.
            "outer_t x t2 -> 9990.00 x 8000.00 = 10000.00",
            "outer_t,t2 x t4 -> 10000.00 x 10000.00 = 12500.00",
            // `Projection_23 15625.00`.
            "outer_t,t2,t4 x t3 -> 12500.00 x 10000.00 = 15625.00",
        ],
    );
}

/// THE PROBE THE ROW SOURCE WAS BUILT FOR.
///
/// This tier's other per-node row estimate lives in
/// [`crate::plan_trace::PlanTrace`], which the driver constructs only for
/// `EXPLAIN`. A chooser reading it would make the STRATEGY depend on whether
/// the statement is being explained -- `EXPLAIN` printing an index join over a
/// pipeline that hash-joins, which is a lie no test above the driver can see.
///
/// Point [`crate::driver::join_search::choose`] at the trace and this test
/// fails: the bare run records different rows, or no site at all.
#[test]
fn the_choice_is_the_same_under_explain_and_bare_execution() {
    let catalog = tables();
    let explained = answers_explained(RESULT_1042, &catalog);
    let bare = answers_bare(RESULT_1042, &catalog);
    assert!(!bare.is_empty(), "the bare statement reached no join site");
    assert_eq!(
        explained, bare,
        "the chooser answered differently for the explained statement",
    );
}

/// THE CENSUS, at the statement the whole decision exists for.
///
/// CORRECTION. This test used to assert the opposite -- that EVERY site of
/// `result:1042` is refused because the property it is asked for is EMPTY,
/// which is what puts `getHashJoins` back in the enumeration. That was not a
/// property of the index rule but of what sat ABOVE these joins: nothing,
/// because `merge_decision::join_properties` reported only the order a join's
/// OWN chosen plan produces, and with no index join the bottom join produced
/// none. The circle is documented at the top of
/// [`crate::driver::merge_decision`].
///
/// With the PROMISE restored to Go's `PreparePossibleProperties` union and
/// the delivery VERIFIED after the children are built, the bottom site is
/// asked for `{t2.a asc}` by the merge join above it, `getHashJoins` answers
/// nothing under that non-empty property, and the index join is the choice by
/// elimination -- which is the operator TiDB's own recording carries there.
#[test]
fn result_1042_requires_an_order_of_its_bottom_site_and_reaches_the_index_join() {
    let catalog = tables();
    let census: Vec<String> = answers_explained(RESULT_1042, &catalog)
        .iter()
        .map(|answer| {
            format!(
                "{} x {} ordered={} {:?}",
                answer.left.join(","),
                answer.right.join(","),
                answer.ordered,
                answer.chosen,
            )
        })
        .collect();
    assert_eq!(
        census,
        vec![
            // The site TiDB records an `IndexHashJoin` at. Asked for the
            // order the merge join above it needs, and answered by
            // elimination.
            "outer_t x t2 ordered=true Index",
            // The MIDDLE merge join is itself asked for `{t2.a asc}` by the
            // top one, so `getHashJoins` is silent there too -- but a merge
            // join IS still enumerated (it is the plan this tier builds), and
            // choosing between two families is the costing layer this tier
            // refuses. The chooser's refusal costs nothing here: the merge is
            // `merge_decision`'s answer, not this chooser's.
            "outer_t,t2 x t4 ordered=true Refused(MergeAlsoEnumerated)",
            // The TOP join is asked for nothing, so hash is enumerated again.
            "outer_t,t2,t4 x t3 ordered=false Refused(HashAlsoEnumerated)",
        ],
    );
}

/// THE WIRING IS LIVE, not dead.
///
/// The test above would also pass if the property never propagated at all. It
/// does: on a group this tier DOES merge, the join below the top one is asked
/// for its keys' order, `getHashJoins` returns nothing there, and the refusal
/// changes reason -- a merge join is the alternative, not a hash join.
#[test]
fn a_site_under_a_merge_join_is_asked_for_an_order_and_has_no_hash_alternative() {
    let catalog = tables();
    let answers = answers_explained(MERGE_CHAIN, &catalog);
    assert!(
        answers.iter().any(|answer| answer.ordered
            && answer.chosen == Chosen::Refused(Refusal::MergeAlsoEnumerated)),
        "no site was asked for an order: {answers:?}",
    );
}

/// The statement still ANSWERS, whatever the chooser decided: the search is a
/// planning input, and a refusal must never change a row.
#[test]
fn the_refusal_does_not_change_the_result() {
    let mut catalog = tables();
    for (name, values) in [
        ("t1", "(1, 12, 'a'), (2, 22, 'b')"),
        ("t2", "(1, 1, 'x'), (2, 6, 'y')"),
        ("t3", "(1, 1, 'p'), (2, 2, 'q')"),
        ("t4", "(1, 1, 'm'), (2, 2, 'n')"),
    ] {
        crate::run_insert_on(
            &format!("INSERT INTO {name} VALUES {values}"),
            &mut catalog,
            &StmtContext::for_dml(false, true, false),
        )
        .unwrap();
    }
    let rows = crate::run_select_on(RESULT_1042, &catalog, &ctx()).unwrap();
    // `t2.b * 2 + 10` is `12` for `t2.b = 1` and `22` for `t2.b = 6`, which
    // the two `t1` rows match one each.
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().all(|row| matches!(row[0], Datum::Int(_))));
}
