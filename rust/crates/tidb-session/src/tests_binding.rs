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

//! SQL bindings, against captures taken from real TiDB through `gorun` on
//! this branch.
//!
//! # Why a match assertion is never alone here
//!
//! `@@last_plan_from_binding = 1` says a binding was FOUND. It says nothing
//! about whether the binding's hints reached the plan, and a binding that
//! stores a hint no planner reads is the accept-then-discard shape this
//! project has been bitten by. So the tests that matter pair the flag with
//! either the ROWS the bound plan returns or the ACCESS PATH it chose, over a
//! fixture where the two differ visibly.

use crate::tests_support::{query_text, scalar_text};
use crate::*;

/// `t(a int, b int, key kb(b))` in database `test`, the shape every capture
/// in this file was taken against.
fn binding_session() -> Session {
    let mut session = Session::new();
    session
        .run("create table t (a int, b int, key kb(b))")
        .expect("create");
    session
        .run("insert into t values (1,10),(2,20),(3,30)")
        .expect("insert");
    session
}

/// Reads `@@last_plan_from_binding`, which reports the PRECEDING statement.
fn matched(session: &mut Session) -> String {
    scalar_text(session, "select @@last_plan_from_binding").unwrap_or_default()
}

/// A fresh session reports 0, which is the floor every other assertion here
/// is measured against.
///
/// Captured from real TiDB: `select @@last_plan_from_binding` before any
/// binding exists answers `0`.
#[test]
fn a_session_with_no_binding_reports_no_match() {
    let mut session = binding_session();
    session.run("select * from t where a = 1").expect("select");
    assert_eq!(matched(&mut session), "0");
}

/// The central capture. Real TiDB, `gorun`, database `bt3`:
///
/// ```text
/// create table t (a int, b int, key kb(b))
/// create session binding for select * from t where a = 1
///        using select * from t use index(kb) where a = 1   -> OK
/// select * from t where a = 5                              -> RS:
/// select @@last_plan_from_binding                          -> RS:1
/// select * from t use index(kb) where a = 5                -> RS:
/// select @@last_plan_from_binding                          -> RS:1
/// select * from t ignore index(kb) where a = 5             -> RS:
/// select @@last_plan_from_binding                          -> RS:1
/// ```
///
/// All THREE spellings match one binding, which is the measured consequence
/// of `reduceOptimizerHint` erasing `use`/`force`/`ignore index (...)` from
/// the normalized text -- not an approximation made here.
#[test]
fn an_index_hint_binding_matches_the_query_whatever_index_hint_the_query_wrote() {
    let mut session = binding_session();
    session
        .run(
            "create session binding for select * from t where a = 1 \
             using select * from t use index(kb) where a = 1",
        )
        .expect("create binding");
    for query in [
        "select * from t where a = 2",
        "select * from t use index(kb) where a = 2",
        "select * from t ignore index(kb) where a = 2",
    ] {
        session
            .run(query)
            .unwrap_or_else(|e| panic!("{query}: {e:?}"));
        assert_eq!(matched(&mut session), "1", "{query}");
    }
}

/// The binding's hints reach the PLAN, not just the match flag.
///
/// `select * from t where b = 20` costs the index and the table scan on its
/// own; a binding that forces the table path must move `EXPLAIN` off the
/// index, and the rows must stay identical either way. Both halves are
/// asserted, because a binding that changed the rows would be a far worse
/// bug than one that changed nothing.
#[test]
fn a_binding_moves_the_access_path_and_leaves_the_rows_alone() {
    let mut session = binding_session();
    let unbound_rows = query_text(&mut session, "select * from t where b = 20").1;
    let unbound_plan = query_text(&mut session, "explain select * from t where b = 20")
        .1
        .concat()
        .concat();
    assert!(
        unbound_plan.contains("kb"),
        "the unbound plan must use the index for this test to be able to move it: {unbound_plan}"
    );
    session
        .run(
            "create session binding for select * from t where b = 20 \
             using select * from t use index() where b = 20",
        )
        .expect("create binding");
    let bound_rows = query_text(&mut session, "select * from t where b = 20").1;
    assert_eq!(matched(&mut session), "1");
    assert_eq!(
        bound_rows, unbound_rows,
        "a binding must not change the rows"
    );
    let bound_plan = query_text(&mut session, "explain select * from t where b = 20")
        .1
        .concat()
        .concat();
    assert!(
        !bound_plan.contains("index:kb"),
        "USE INDEX () in the binding must take the index path away: {bound_plan}"
    );
}

/// Go's `in`-list collapse, which is what makes one binding cover a family of
/// literals. Captured from `tests/integrationtest/t/bindinfo/bind.test`'s own
/// first block: a binding created with `in (1)` is expected to take effect for
/// `in (1, 2, 3)` and `in (1, 2)` alike.
#[test]
fn one_in_list_binding_covers_every_in_list_length() {
    let mut session = binding_session();
    session
        .run(
            "create session binding for select a from t where a in (1) \
             using select a from t where a in (1)",
        )
        .expect("create binding");
    for query in [
        "select a from t where a in (1, 2, 3)",
        "select a from t where a in (1, 2)",
        "select a from t where a in (1)",
    ] {
        session
            .run(query)
            .unwrap_or_else(|e| panic!("{query}: {e:?}"));
        assert_eq!(matched(&mut session), "1", "{query}");
    }
}

/// A binding for a DIFFERENT statement must not match. This is the control
/// for every positive assertion above: without it, a matcher that answered
/// "yes" unconditionally would pass all of them.
#[test]
fn a_binding_for_another_statement_does_not_match() {
    let mut session = binding_session();
    session
        .run(
            "create session binding for select * from t where a = 1 \
             using select * from t use index(kb) where a = 1",
        )
        .expect("create binding");
    session
        .run("select * from t where b = 10")
        .expect("other statement");
    assert_eq!(matched(&mut session), "0");
}

/// Dropping removes the match, and dropping again is not an error.
///
/// Captured from real TiDB: two consecutive `drop session binding for ...`
/// for the same statement both answer OK, and so does a drop for a statement
/// that never had one.
#[test]
fn dropping_a_binding_stops_the_match_and_dropping_twice_is_not_an_error() {
    let mut session = binding_session();
    session
        .run("drop session binding for select * from t where a = 1")
        .expect("drop with nothing bound is OK");
    session
        .run(
            "create session binding for select * from t where a = 1 \
             using select * from t use index(kb) where a = 1",
        )
        .expect("create binding");
    session
        .run("drop session binding for select * from t where a = 1")
        .expect("drop");
    session
        .run("drop session binding for select * from t where a = 1")
        .expect("second drop is OK");
    session.run("select * from t where a = 2").expect("select");
    assert_eq!(matched(&mut session), "0");
}

/// `SHOW SESSION BINDINGS`' eleven columns and the NORMALIZED text they
/// carry. Captured from real TiDB (database `bt2`):
///
/// ```text
/// RS:select * from `bt2` . `t` where `b` = ?|SELECT * FROM `bt2`.`t` USE INDEX (`kb`) WHERE `b` = 1|bt2|enabled|<ts>|<ts>|utf8mb4|utf8mb4_bin|manual|b3a94ec6...|
/// ```
///
/// The first column is the normalizer's output -- spaces around the dot, `?`
/// for the literal -- and the second is the restore, with none of that.
#[test]
fn show_bindings_prints_the_normalized_origin_and_the_restored_hinted_sql() {
    let mut session = binding_session();
    session
        .run(
            "create session binding for select * from t where b = 1 \
             using select * from t use index(kb) where b = 1",
        )
        .expect("create binding");
    let rows = query_text(&mut session, "show session bindings").1;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.len(), 11, "Go prints eleven columns");
    assert_eq!(row[0], "select * from `test` . `t` where `b` = ?");
    assert_eq!(
        row[1],
        "SELECT * FROM `test`.`t` USE INDEX (`kb`) WHERE `b` = 1"
    );
    assert_eq!(row[2], "test");
    assert_eq!(row[3], "enabled");
    assert_eq!(row[6], "utf8mb4");
    assert_eq!(row[7], "utf8mb4_bin");
    assert_eq!(row[8], "manual");
    assert_eq!(row[9].len(), 64, "a SHA-256 digest in hex");
    assert_eq!(row[10], "", "a manual binding has no plan digest");
}

/// Binding the same normalized statement twice REPLACES rather than
/// accumulates. Captured from real TiDB: after two `create session binding`
/// for `a = 1` and `a = 2` -- which normalize identically -- `show session
/// bindings` returned ONE row, carrying the second binding's `bind_sql`.
#[test]
fn creating_a_second_binding_for_the_same_normalized_statement_replaces_the_first() {
    let mut session = binding_session();
    session
        .run(
            "create session binding for select * from t where b = 1 \
             using select * from t use index(kb) where b = 1",
        )
        .expect("first");
    session
        .run(
            "create session binding for select * from t where b = 2 \
             using select * from t use index() where b = 2",
        )
        .expect("second");
    let rows = query_text(&mut session, "show session bindings").1;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][1],
        "SELECT * FROM `test`.`t` USE INDEX () WHERE `b` = 2"
    );
}

/// Go's preprocessor check, message included. Captured from real TiDB:
///
/// ```text
/// create session binding for select * from t where a = 1 using select * from t where b = 1
///   -> Error|1105|hinted sql and origin sql don't match when hinted sql erase the hint
///      info, after erase hint info, originSQL:select * from `bt5` . `t` where `a` = ?,
///      hintedSQL:select * from `bt5` . `t` where `b` = ?
/// ```
#[test]
fn an_origin_and_hinted_pair_that_differ_in_more_than_hints_is_refused_with_gos_message() {
    let mut session = binding_session();
    let error = session
        .run(
            "create session binding for select * from t where a = 1 \
             using select * from t where b = 1",
        )
        .expect_err("must be refused");
    let reported = error.to_mysql_error();
    assert_eq!(reported.code, 1105);
    assert_eq!(
        reported.message,
        "hinted sql and origin sql don't match when hinted sql erase the hint info, \
         after erase hint info, originSQL:select * from `test` . `t` where `a` = ?, \
         hintedSQL:select * from `test` . `t` where `b` = ?"
    );
}

/// Go's `checkBindingValidation` reaches the catalog. Captured from real
/// TiDB: a binding over a missing table is `Error|1146|Table 'bt5.nosuchtbl'
/// doesn't exist`, and one naming a missing index is `Error|1176|Key
/// 'nosuchidx' doesn't exist in table 't'`.
///
/// MEASURED DIVERGENCE, and not a binding one: this tier answers a missing
/// table on the read path with its own `1105 table not found in catalog`
/// rather than 1146 -- the same cascade `tests_system_schemas` documents and
/// `integration_diff` carries as its largest single gap. The assertion here
/// therefore pins the tier's code and names the Go code it should become, so
/// closing that cascade turns this red at the right place.
#[test]
fn a_binding_whose_statement_cannot_plan_is_refused_at_create_time() {
    let mut session = binding_session();
    let missing_table = session
        .run(
            "create session binding for select * from nosuchtbl where a = 1 \
             using select * from nosuchtbl where a = 1",
        )
        .expect_err("must be refused")
        .to_mysql_error();
    assert_eq!(
        (missing_table.code, missing_table.message.as_str()),
        (1105, "table not found in catalog"),
        "Go reports 1146 Table 'test.nosuchtbl' doesn't exist here"
    );

    let missing_index = session
        .run(
            "create session binding for select * from t where a = 1 \
             using select * from t use index(nosuchidx) where a = 1",
        )
        .expect_err("must be refused")
        .to_mysql_error();
    assert_eq!(missing_index.code, 1176, "{}", missing_index.message);
}

/// The pinned refusal for GLOBAL scope, and the MEASURED reason for it in the
/// same test: `mysql.bind_info` is not in this tier's catalog, so there is
/// nowhere for a global binding to live.
///
/// Both halves are here on purpose. The day someone bootstraps
/// `mysql.bind_info`, the second assertion fails and points straight at this
/// refusal -- which is the whole reason to pin a refusal rather than just
/// write one.
#[test]
fn global_scope_is_refused_because_the_storage_table_is_absent() {
    let mut session = binding_session();
    for statement in [
        "create global binding for select * from t where a = 1 \
         using select * from t use index(kb) where a = 1",
        "drop global binding for select * from t where a = 1",
        "show global bindings",
    ] {
        let error = session.run(statement).expect_err("global scope is refused");
        assert!(
            matches!(&error, DriverError::Unsupported(reason) if reason.contains("mysql.bind_info")),
            "{statement}: {error:?}"
        );
    }
    // The measured gap the refusal names.
    let storage = session.run("select * from mysql.bind_info");
    assert!(
        storage.is_err(),
        "mysql.bind_info answered rows -- the GLOBAL refusal above is now stale"
    );
}

/// `tidb_use_plan_baselines = OFF` turns matching off wholesale, which is
/// Go's `SessionVars.UsePlanBaselines` gate in `planner.optimize`.
#[test]
fn turning_plan_baselines_off_stops_the_match() {
    let mut session = binding_session();
    session
        .run(
            "create session binding for select * from t where a = 1 \
             using select * from t use index(kb) where a = 1",
        )
        .expect("create binding");
    session.run("select * from t where a = 2").expect("select");
    assert_eq!(matched(&mut session), "1", "the control: matching is on");
    session
        .run("set @@tidb_use_plan_baselines = 0")
        .expect("set");
    session.run("select * from t where a = 2").expect("select");
    assert_eq!(matched(&mut session), "0");
}
