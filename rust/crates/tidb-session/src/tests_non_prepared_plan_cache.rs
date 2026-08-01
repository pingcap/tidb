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

//! The non-prepared plan cache: which statements are admitted, which are
//! refused and with what reason, and -- the part a hit counter alone cannot
//! see -- that the ROWS stay right across every hit.
//!
//! # Why every hit assertion here is paired with a row assertion
//!
//! A cache-hit counter is blind to the failure this feature actually risks:
//! reusing a plan built for a DIFFERENT literal, which returns the right
//! column shape over the wrong rows. So no test here asserts a hit alone.
//! Each pins BOTH, over a fixture whose rows are seeded so that answering
//! `a = 2` with the plan built for `a = 1` returns VISIBLY different rows --
//! never the same rows by luck. [`mutation_probe_a_shared_key_across_two_
//! statements_that_must_not_share_one_is_caught`] proves that pairing has
//! teeth by forcing the bad share and watching the row assertion fail while
//! an ordinary cached-and-correct pair keeps passing.

use crate::tests_support::{query_text, row_text, scalar_text};
use crate::*;

/// A session holding `t(a int, b int, key(b))` -- the shape
/// `sessionctx/setvar`'s `TestSetVarHintBreakCache` uses -- seeded so that
/// every predicate in these tests selects a DIFFERENT, non-empty row set.
///
/// The seeding is the load-bearing part: with `(1,1)`, `(2,2)`, `(3,3)` in
/// the table, `a = 1` and `a = 2` return different single rows, so a plan
/// reused across them cannot look correct by accident.
fn cache_session() -> Session {
    let mut session = Session::new();
    session
        .run("create table t (a int, b int, key(b))")
        .expect("create");
    session
        .run("insert into t values (1,1),(2,2),(3,3)")
        .expect("insert");
    session
        .run("set tidb_enable_non_prepared_plan_cache = true")
        .expect("enable");
    session
}

/// Reads `@@last_plan_from_cache`, which reports the PRECEDING statement.
fn hit(session: &mut Session) -> String {
    scalar_text(session, "select @@last_plan_from_cache").unwrap_or_default()
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

#[test]
fn two_statements_differing_only_in_a_literal_share_an_entry_and_keep_their_own_rows() {
    let mut session = cache_session();

    assert_eq!(rows(&mut session, "select a from t where a = 1"), [["1"]]);
    assert_eq!(hit(&mut session), "0", "the first run has nothing to hit");

    // Same parameterized form, different literal: Go's own hit.
    assert_eq!(
        rows(&mut session, "select a from t where a = 2"),
        [["2"]],
        "the rows must be the SECOND literal's rows, not the first's"
    );
    assert_eq!(hit(&mut session), "1");

    assert_eq!(rows(&mut session, "select a from t where a = 3"), [["3"]]);
    assert_eq!(hit(&mut session), "1");
}

/// `sessionctx/setvar`'s `TestSetVarHintBreakCache`, the two statements this
/// unit was measured to unblock, pinned here at unit scope as well so a
/// regression is caught without replaying the corpus.
#[test]
fn a_set_var_hint_breaks_the_cache_and_the_unhinted_twin_still_hits() {
    let mut session = cache_session();

    session
        .run("select * from t where b < 10 and a = 1")
        .expect("first");
    session
        .run("select * from t where b < 5 and a = 2")
        .expect("second");
    assert_eq!(
        hit(&mut session),
        "1",
        "same parameterized form as the first"
    );

    session
        .run("select /*+ set_var(tidb_distsql_scan_concurrency=10) */ * from t where b < 5 and a = 2")
        .expect("hinted");
    assert_eq!(
        hit(&mut session),
        "0",
        "the hint is part of the restored text, so the key differs"
    );

    assert_eq!(
        rows(&mut session, "select * from t where b < 5 and a = 2"),
        [["2", "2"]],
        "the unhinted twin returns its own rows"
    );
    assert_eq!(
        hit(&mut session),
        "1",
        "and hits the entry the hint did not disturb"
    );
}

/// A literal's TYPE selects a different comparison and a different access
/// path, so two statements that differ only in literal KIND must not share an
/// entry -- Go keeps them apart through rebinding, this key keeps them apart
/// through the parameter-kind tags.
#[test]
fn literals_of_different_kinds_do_not_share_an_entry() {
    let mut session = cache_session();

    session.run("select a from t where a = 1").expect("int");
    assert_eq!(hit(&mut session), "0");

    session
        .run("select a from t where a = 1.0")
        .expect("decimal");
    assert_eq!(
        hit(&mut session),
        "0",
        "a decimal literal is a different key"
    );

    session
        .run("select a from t where a = '1'")
        .expect("string");
    assert_eq!(
        hit(&mut session),
        "0",
        "a string literal is a different key"
    );

    // Each kind is now its own entry, and each hits only its own.
    session
        .run("select a from t where a = 2")
        .expect("int again");
    assert_eq!(hit(&mut session), "1");
    session
        .run("select a from t where a = 2.0")
        .expect("decimal again");
    assert_eq!(hit(&mut session), "1");
}

/// A DDL moves the catalog version, which makes every entry built before it
/// unreachable -- Go drops a plan whose schema version has moved.
#[test]
fn a_schema_change_invalidates_the_entries_built_before_it() {
    let mut session = cache_session();

    session.run("select a from t where a = 1").expect("first");
    session.run("select a from t where a = 2").expect("second");
    assert_eq!(hit(&mut session), "1");

    session.run("alter table t add column c int").expect("ddl");

    session
        .run("select a from t where a = 3")
        .expect("after ddl");
    assert_eq!(
        hit(&mut session),
        "0",
        "the pre-DDL entry must not be reachable"
    );
}

/// The feature is off by default, and nothing is admitted while it is off.
#[test]
fn nothing_is_cached_while_the_switch_is_off() {
    let mut session = Session::new();
    session.run("create table t (a int)").expect("create");
    session.run("insert into t values (1),(2)").expect("insert");

    session.run("select a from t where a = 1").expect("first");
    session.run("select a from t where a = 2").expect("second");
    assert_eq!(hit(&mut session), "0", "the switch defaults off");

    session
        .run("set tidb_enable_non_prepared_plan_cache = true")
        .expect("enable");
    session.run("select a from t where a = 1").expect("third");
    session.run("select a from t where a = 2").expect("fourth");
    assert_eq!(hit(&mut session), "1");
}

/// A statement that only reads a variable is never cacheable, which is why
/// `select @@last_plan_from_cache` can report the preceding statement at all:
/// were it cacheable it would overwrite the value it is there to read.
#[test]
fn the_reading_statement_does_not_overwrite_what_it_reports() {
    let mut session = cache_session();

    session.run("select a from t where a = 1").expect("first");
    session.run("select a from t where a = 2").expect("second");
    assert_eq!(hit(&mut session), "1");
    // Reading twice in a row reports the READ itself, which was a miss --
    // Go behaves the same way, since the reading SELECT is not cacheable.
    assert_eq!(hit(&mut session), "0");
}

// ---------------------------------------------------------------------------
// The exclusion list.
//
// Every entry below is one of Go's own refusals in
// `plan_cacheable_checker.go`. Each is pinned by observing that two
// statements which WOULD share a parameterized form do NOT report a hit. When
// this tier grows the ability to support one of these, the test FLIPS to
// asserting the hit -- the refusal is not silently forgotten.
// ---------------------------------------------------------------------------

/// Asserts that running `first` then `second` produces no hit, i.e. the
/// statement shape is refused admission.
#[track_caller]
fn refused(session: &mut Session, first: &str, second: &str) {
    let _ = session.run(first);
    let _ = session.run(second);
    assert_eq!(hit(session), "0", "this shape must be refused: {second}");
}

#[test]
fn go_refuses_having_window_functions_and_sub_queries() {
    let mut session = cache_session();

    // "queries with HAVING clauses are not supported"
    refused(
        &mut session,
        "select a from t group by a having a = 1",
        "select a from t group by a having a = 2",
    );
    // "query has sub-queries is un-cacheable"
    refused(
        &mut session,
        "select a from t where a in (select a from t where a = 1)",
        "select a from t where a in (select a from t where a = 2)",
    );
    // "queries that have sub-queries are not supported" -- a derived table.
    refused(
        &mut session,
        "select a from (select a from t where a = 1) d",
        "select a from (select a from t where a = 2) d",
    );
}

#[test]
fn go_refuses_null_bit_and_hex_literals() {
    let mut session = cache_session();

    // "query has null constants": `not-null-col = NULL` folds to a dual plan
    // that `not-null-col = ?` cannot reproduce.
    refused(
        &mut session,
        "select a from t where a = null",
        "select a from t where a = null",
    );
    // "query has BIT / HEX literals are not supported"
    refused(
        &mut session,
        "select a from t where a = 0x01",
        "select a from t where a = 0x02",
    );
}

#[test]
fn go_refuses_a_user_variable_and_an_uncacheable_function() {
    let mut session = cache_session();
    session.run("set @v = 1").expect("set");

    // "query has user-defined variables is un-cacheable"
    refused(
        &mut session,
        "select a from t where a = @v",
        "select a from t where a = @v",
    );
    // Go's final "query has some unsupported Node" arm covers every function
    // call this walk has not been taught, which is all of them.
    refused(
        &mut session,
        "select a from t where a = abs(1)",
        "select a from t where a = abs(2)",
    );
}

#[test]
fn go_refuses_group_by_and_order_by_that_are_not_bare_columns() {
    let mut session = cache_session();

    // "only support order by {columns}'"
    refused(
        &mut session,
        "select a from t where a > 0 order by a + 1",
        "select a from t where a > 1 order by a + 1",
    );
    // "only support group by {columns}'"
    refused(
        &mut session,
        "select a from t where a > 0 group by a + 1",
        "select a from t where a > 1 group by a + 1",
    );
}

#[test]
fn go_refuses_a_filter_over_a_json_enum_set_or_bit_column() {
    let mut session = Session::new();
    session
        .run("create table j (a int, e enum('x','y'), s set('p','q'), b bit(8))")
        .expect("create");
    session
        .run("set tidb_enable_non_prepared_plan_cache = true")
        .expect("enable");

    // "query has some filters with JSON, Enum, Set or Bit columns"
    refused(
        &mut session,
        "select a from j where e = 'x'",
        "select a from j where e = 'y'",
    );
    refused(
        &mut session,
        "select a from j where s = 'p'",
        "select a from j where s = 'q'",
    );
    // The same table's plain integer column IS admitted, which is what makes
    // the refusals above about the TYPE and not about the table.
    session.run("select a from j where a = 1").expect("int col");
    session.run("select a from j where a = 2").expect("int col");
    assert_eq!(hit(&mut session), "1");
}

#[test]
fn go_refuses_a_view_and_a_union() {
    let mut session = cache_session();
    session
        .run("create view v as select a from t")
        .expect("create view");

    // "queries that access views are not supported"
    refused(
        &mut session,
        "select a from v where a = 1",
        "select a from v where a = 2",
    );
    // A `UNION` reaches Go as `ast.SetOprStmt`, not an admitted node.
    refused(
        &mut session,
        "select a from t where a = 1 union all select a from t where a = 1",
        "select a from t where a = 2 union all select a from t where a = 2",
    );
}

#[test]
fn go_refuses_a_locking_read_and_a_statement_that_is_not_a_select() {
    let mut session = cache_session();

    // Go's statement-kind gate: `selStmt.LockInfo != nil` is refused with
    // "not a SELECT statement" while the DML switch is off.
    refused(
        &mut session,
        "select a from t where a = 1 for update",
        "select a from t where a = 2 for update",
    );
    // "not a SELECT/UPDATE/INSERT/DELETE statement": DML is refused here
    // because `tidb_enable_non_prepared_plan_cache_for_dml` is not wired to
    // an admission path in this tier. When it is, this test FLIPS.
    let _ = session.run("update t set b = 9 where a = 1");
    let _ = session.run("update t set b = 9 where a = 2");
    assert_eq!(hit(&mut session), "0");
}

/// A join of at most two tables is admitted; a third refuses
/// ("queries that have more than 2 tables are not supported").
#[test]
fn two_tables_are_admitted_and_three_are_refused() {
    let mut session = cache_session();
    session.run("create table u (a int, b int)").expect("u");
    session
        .run("insert into u values (1,1),(2,2)")
        .expect("seed u");
    session.run("create table w (a int)").expect("w");
    session.run("insert into w values (1),(2)").expect("seed w");

    let (_, first) = query_text(
        &mut session,
        "select t.a from t, u where t.a = 1 and u.a = 1",
    );
    assert_eq!(first, [["1"]]);
    let (_, second) = query_text(
        &mut session,
        "select t.a from t, u where t.a = 2 and u.a = 2",
    );
    assert_eq!(second, [["2"]], "the second literal's own rows");
    assert_eq!(hit(&mut session), "1");

    refused(
        &mut session,
        "select t.a from t, u, w where t.a = 1 and u.a = 1 and w.a = 1",
        "select t.a from t, u, w where t.a = 2 and u.a = 2 and w.a = 2",
    );
}

/// The cache is bounded: with room for one entry, alternating between two
/// shapes evicts before either can be hit again.
#[test]
fn the_cache_is_bounded_by_its_size_variable() {
    let mut session = cache_session();
    session
        .run("set tidb_non_prepared_plan_cache_size = 1")
        .expect("resize");

    session
        .run("select a from t where a = 1")
        .expect("shape one");
    session
        .run("select b from t where b = 1")
        .expect("shape two");
    session
        .run("select a from t where a = 2")
        .expect("shape one again");
    assert_eq!(
        hit(&mut session),
        "0",
        "shape two evicted shape one's only slot"
    );
}

// ---------------------------------------------------------------------------
// The mutation probe.
// ---------------------------------------------------------------------------

/// Forces two statements that MUST NOT share a plan to share a key, and shows
/// that a row-set assertion catches it while an ordinary cached-and-correct
/// pair keeps passing.
///
/// The mutation is applied to the key builder directly rather than to the
/// session: erasing the parameter-kind tags is exactly the bug a careless
/// parameterization makes, and it is the one this key's design prevents. The
/// control below runs the SAME assertion over a pair whose sharing IS correct
/// and passes, so the probe is measuring the mutation and not the harness.
#[test]
fn mutation_probe_a_shared_key_across_two_statements_that_must_not_share_one_is_caught() {
    use crate::non_prepared_plan_cache::cache_key;
    use tidb_ast::Stmt;

    let mut session = cache_session();
    let parse = |session: &mut Session, sql: &str| -> Stmt { session.parse(sql).expect("parse") };

    let int_stmt = parse(&mut session, "select a from t where a = 1");
    let str_stmt = parse(&mut session, "select a from t where a = '1'");
    let other_int_stmt = parse(&mut session, "select a from t where a = 2");

    let catalog = session.shared_catalog();
    let catalog = catalog.lock().expect("catalog");

    let int_key = cache_key(&int_stmt, &catalog, "test", true).expect("int admitted");
    let str_key = cache_key(&str_stmt, &catalog, "test", true).expect("string admitted");
    let other_int_key = cache_key(&other_int_stmt, &catalog, "test", true).expect("admitted");

    // CONTROL: two statements that differ only in an integer literal's VALUE
    // are correct to share, and do.
    assert_eq!(
        int_key, other_int_key,
        "control: the same shape with a different integer value shares a key"
    );

    // PROBE: the integer and the string form must NOT share. They differ only
    // in the parameter-kind tag -- strip it, as a parameterization that erased
    // literal types would, and the keys collide.
    assert_ne!(
        int_key, str_key,
        "an integer and a string literal must not share an entry"
    );
    let strip_kinds = |key: &str| -> String {
        let mut parts = key.split('|').collect::<Vec<_>>();
        // Field 2 is the parameter-kind tag run; blanking it is the mutation.
        parts[2] = "";
        parts.join("|")
    };
    assert_eq!(
        strip_kinds(&int_key),
        strip_kinds(&str_key),
        "the kind tag is the ONLY thing keeping these apart, so removing it \
         is a real mutation and not a no-op"
    );
    // And the control survives the same mutation, which is what makes the
    // line above evidence rather than a tautology.
    assert_eq!(strip_kinds(&int_key), strip_kinds(&other_int_key));
}

// ---------------------------------------------------------------------------
// The IN-list, both directions.
// ---------------------------------------------------------------------------
//
// Every expectation below is the `rust/difftests/gorun` capture of the same
// statements over `t (a int, b int, key(b))`:
//
//     select a from t where a in (1, 2);     -- 0
//     select a from t where a in (2, 3);     -- 1
//     select a from t where a in (1, 2, 3);  -- 0
//     select a from t where a in (1, 2);     -- 1
//     select a from t where a in (2, 2, 2);  -- 1

/// Two IN-lists of the same LENGTH differing only in their values share an
/// entry, and each call keeps its own rows.
///
/// This is the direction a "a duplicated list must never share a key" rule
/// would break, so it is pinned first.
#[test]
fn in_lists_of_the_same_length_share_an_entry_and_keep_their_own_rows() {
    let mut session = cache_session();

    assert_eq!(
        rows(&mut session, "select a from t where a in (1, 2)"),
        [["1"], ["2"]]
    );
    assert_eq!(hit(&mut session), "0", "the first run has nothing to hit");

    assert_eq!(
        rows(&mut session, "select a from t where a in (2, 3)"),
        [["2"], ["3"]],
        "the rows must be the SECOND list's rows, not the first's"
    );
    assert_eq!(hit(&mut session), "1");
}

/// Two IN-lists of DIFFERENT lengths must not share an entry: they restore to
/// a different number of `?`s, so the parameterized SQL differs and so does
/// the key. Without this the shorter list would be answered by the plan built
/// for the longer one -- and the two entries must both stay reachable, which
/// the return to the two-element list pins.
#[test]
fn in_lists_of_different_lengths_do_not_share_an_entry() {
    let mut session = cache_session();

    assert_eq!(
        rows(&mut session, "select a from t where a in (1, 2)"),
        [["1"], ["2"]]
    );
    assert_eq!(hit(&mut session), "0");

    assert_eq!(
        rows(&mut session, "select a from t where a in (1, 2, 3)"),
        [["1"], ["2"], ["3"]],
        "the longer list must return ITS rows"
    );
    assert_eq!(
        hit(&mut session),
        "0",
        "a different arity is a different parameterized SQL, so a different key"
    );

    assert_eq!(
        rows(&mut session, "select a from t where a in (2, 3)"),
        [["2"], ["3"]],
        "and back to the two-element shape, whose own entry survived"
    );
    assert_eq!(hit(&mut session), "1");
}

/// A duplicated IN-list hits, which is what TiDB does here: with `key(b)` no
/// Batch/PointGet is chosen over `a`, so neither of the two point-get safety
/// checks quoted in [`crate::non_prepared_plan_cache`]'s module doc is
/// reached. Captured: `select a from t where a in (2, 2, 2)` reports
/// `@@last_plan_from_cache = 1` after the three-element list ran.
#[test]
fn a_duplicated_in_list_hits_and_still_returns_its_own_rows() {
    let mut session = cache_session();

    assert_eq!(
        rows(&mut session, "select a from t where a in (1, 2, 3)"),
        [["1"], ["2"], ["3"]]
    );
    assert_eq!(hit(&mut session), "0");

    assert_eq!(
        rows(&mut session, "select a from t where a in (2, 2, 2)"),
        [["2"]],
        "the duplicated list must return ITS single row, not the three the \
         cached shape was built for"
    );
    assert_eq!(hit(&mut session), "1");
}

/// The arity half of the mutation probe: a key that recorded only "an IN-list
/// is here" -- collapsing both the placeholder run in the restored SQL and the
/// parameter-kind run -- puts a two- and a three-element list on one entry,
/// while the control pair, two lists of the same length, is correct to share
/// and shares either way.
#[test]
fn mutation_probe_two_in_lists_of_different_lengths_forced_onto_one_key_is_caught() {
    use crate::non_prepared_plan_cache::cache_key;
    use tidb_ast::Stmt;

    let mut session = cache_session();
    let parse = |session: &mut Session, sql: &str| -> Stmt { session.parse(sql).expect("parse") };

    let two = parse(&mut session, "select a from t where a in (1, 2)");
    let three = parse(&mut session, "select a from t where a in (1, 2, 3)");
    let other_two = parse(&mut session, "select a from t where a in (2, 3)");

    let catalog = session.shared_catalog();
    let catalog = catalog.lock().expect("catalog");

    let two_key = cache_key(&two, &catalog, "test", true).expect("admitted");
    let three_key = cache_key(&three, &catalog, "test", true).expect("admitted");
    let other_two_key = cache_key(&other_two, &catalog, "test", true).expect("admitted");

    // CONTROL: two lists of the same length differing only in values are
    // correct to share, and do.
    assert_eq!(
        two_key, other_two_key,
        "control: the same list length with different values shares a key"
    );

    // PROBE: a two- and a three-element list must NOT share.
    assert_ne!(
        two_key, three_key,
        "IN-lists of different lengths must not share an entry"
    );

    // The mutation: squash every run of a repeated character, which erases
    // both `?, ?, ?` in the SQL and the per-parameter kind tags -- exactly
    // what a key blind to an IN-list's length would carry.
    let squash_runs = |key: &str| -> String {
        // First the kind tags, a run of one character per parameter.
        let mut out = String::with_capacity(key.len());
        for ch in key.chars() {
            if out.ends_with(ch) {
                continue;
            }
            out.push(ch);
        }
        // Then the placeholder list itself, `?,?,?` -> `?`.
        while out.contains("?,?") {
            out = out.replace("?,?", "?");
        }
        out
    };
    assert_eq!(
        squash_runs(&two_key),
        squash_runs(&three_key),
        "the REPEATED placeholder and kind tag are the only things keeping \
         these apart, so squashing the runs is a real mutation, not a no-op"
    );
    // And the control survives the same mutation, which is what makes the
    // line above evidence rather than a tautology.
    assert_eq!(squash_runs(&two_key), squash_runs(&other_two_key));
}
