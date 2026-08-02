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

//! Table-level index hints (`USE`/`FORCE`/`IGNORE INDEX`) on the live
//! `--cluster-session` path, pinned against real TiDB.
//!
//! # The seam these tests pin
//!
//! `tidb_ast::TableRef::hints` is produced by the parser and then read by
//! nobody on the live path. The live access-path decision is
//! `tidb_executor::driver::access::choose_index_range_path` ->
//! `tidb_executor::access_cost::choose_access_path` ->
//! `tidb_executor::skyline::skyline_pruning`, and none of those three ever
//! sees a hint. So today the session ACCEPTS an index hint and the plan
//! DISREGARDS it — the accept-and-discard shape.
//!
//! The only readers of `TableRef::hints` anywhere are fail-closed refusals in
//! narrower tiers that never serve this path:
//! `tidb_planner::read_only_scan::validate_table_ref`,
//! `tidb_planner::prepared_dml`, `tidb_planner::configured_relation_tree`, and
//! `tidb_exec::result_schema_multi`.
//!
//! Closing it is one filter in one place. `choose_index_range_path` already
//! holds both halves it needs -- the `SelectStmt` and the `FromScope` that
//! resolves the `TableRef` carrying the hints -- so the filter goes between
//! its `enumerate_paths` call and its `choose_access_path` call, over the
//! `Vec<Candidate<AccessPath>>`. That is exactly where Go puts it:
//! `getPossibleAccessPaths` returns the already-filtered candidate set, and
//! physical selection never sees the paths a hint excluded. The 1176 ERROR
//! for a table-level hint naming a missing index has to be raised there too,
//! before any path is chosen, because Go raises it whether or not the cost
//! model would have wanted that index.
//!
//! # Go's actual rule (`getPossibleAccessPaths`, `pkg/planner/core/planbuilder.go:1320`)
//!
//! * A table-level `USE`/`FORCE INDEX` naming real indexes sets `hasUseOrForce`
//!   and `path.Forced = true`, and `available` becomes ONLY the named paths —
//!   the cost model may no longer reach for anything else.
//! * `FORCE` and `USE` are deliberately identical. Go says so in place:
//!   "Currently we don't distinguish between `FORCE` and `USE` because our cost
//!   estimation is not reliable." Both merely constrain the candidate set.
//! * `IGNORE INDEX` collects into `ignored` and `removeIgnoredPaths` strips
//!   those index paths; the table path always survives.
//! * `USE INDEX ()` with an empty list has `IndexNames == nil` and is not
//!   `HintIgnore`, so it forces the TABLE path — "use no indexes".
//! * A table-level hint naming an index that does not exist is an ERROR,
//!   `plannererrors.ErrKeyDoesNotExist` = 1176. A comment-style
//!   `/*+ use_index(t, x) */` naming a missing index is only a WARNING 1176.
//! * If the surviving set is empty, Go appends the table path back.
//!
//! # Capture (`rust/difftests/gorun`, verbatim protocol lines)
//!
//! Schema: `create table t (a int primary key, b int, c int, key idx_b(b),
//! key idx_c(c))`, rows `(1,1,1)..(5,5,5)`.
//!
//! ```text
//! explain format='brief' select b from t force index(idx_b) where a = 2
//!   RS:    └─IndexFullScan|10000.00|cop[tikv]|table:t, index:idx_b(b)|keep order:false, stats:pseudo;
//!            └─Selection|1.00|cop[tikv]||eq(test.t.a, 2);
//!          IndexReader|1.00|root||index:Projection;
//!          └─Projection|1.00|cop[tikv]||test.t.b
//!   show warnings -> RS:            (zero warnings)
//!
//! explain format='brief' select b from t use index(idx_b) where a = 2
//!   (byte-identical to the FORCE capture above)
//!
//! explain format='brief' select * from t ignore index(idx_b) where b = 2
//!   RS:  └─TableFullScan|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo;
//!        TableReader|10.00|root||data:Selection;
//!        └─Selection|10.00|cop[tikv]||eq(test.t.b, 2)
//!   show warnings -> RS:            (zero warnings)
//!
//! explain format='brief' select * from t force index(no_such_idx) where b = 2
//!   ERR
//!   show warnings -> RS:Error|1176|Key 'no_such_idx' doesn't exist in table 't'
//!   (identical for use index(no_such_idx) and ignore index(no_such_idx))
//!
//! explain format='brief' select * from t use index() where b = 2
//!   RS:  └─TableFullScan|10000.00|cop[tikv]|table:t|keep order:false, stats:pseudo;
//!        TableReader|10.00|root||data:Selection;
//!        └─Selection|10.00|cop[tikv]||eq(test.t.b, 2)
//!
//! explain format='brief' select /*+ use_index(t, no_such_idx) */ * from t where b = 2
//!   RS:IndexLookUp|10.00|root||;...index:idx_b(b)...        (plan unaffected)
//!   show warnings -> RS:Warning|1176|Key 'no_such_idx' doesn't exist in table 't'
//!
//! explain format='brief' select /*+ use_index(zzz, idx_b) */ * from t where b = 2
//!   RS:IndexLookUp|10.00|root||;...index:idx_b(b)...        (plan unaffected)
//!   show warnings ->
//!     RS:Warning|1815|use_index(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists
//!
//! explain format='brief' select * from t force index(idx_b) ignore index(idx_b) where b = 2
//!   RS:  └─TableFullScan|...;TableReader|10.00|root||data:Selection;└─Selection|...
//!
//! select b from t force index(idx_b) where a = 2            -> RS:2
//! select count(*) from t ignore index(idx_b) where b = 2    -> RS:1
//! ```
//!
//! The 1815 text comes from `hint.collectUnmatchedIndexHintWarning`
//! (`pkg/util/hint/hint.go:1234`), emitted through
//! `PlanBuilder.popTableHints` -> `StmtCtx.SetHintWarning`
//! (`pkg/sessionctx/stmtctx/stmtctx.go:1003`), which is
//! `plannererrors.ErrInternal.FastGen(reason)`. `FastGen` replaces the
//! class message wholesale, which is why the wire text carries no
//! `Internal : ` prefix even though `errno.ErrInternal`'s registered
//! message is `"Internal : %s"`.
//!
//! Every assertion below that names a DIVERGENCE is a tripwire: closing the
//! seam MUST break it, and the Go row it should become is in the comment
//! immediately above it.

#![cfg(test)]

use crate::tests_support::*;
use crate::warnings::WarningLevel;
use crate::*;

/// The capture's schema and rows, so every case below reads the same table.
fn hinted_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a BIGINT PRIMARY KEY, b INT, c INT, INDEX idx_b(b), INDEX idx_c(c))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
        .unwrap();
    session
}

/// The source column of an `EXPLAIN` row set: the `access object` of every
/// row, which is where an index name shows up if one was chosen.
fn access_objects(session: &mut Session, sql: &str) -> Vec<String> {
    row_text(session.run(sql))
        .into_iter()
        .map(|row| row[3].clone())
        .collect()
}

/// Whether any row of the plan reads through the named index.
fn plan_uses_index(session: &mut Session, sql: &str, index: &str) -> bool {
    access_objects(session, sql)
        .iter()
        .any(|object| object.contains(&format!("index:{index}")))
}

/// `FORCE INDEX` naming a real index must constrain the access path even when
/// the cost model would rather use the clustered handle.
///
/// Go plans `IndexReader -> IndexFullScan on idx_b` with a `Selection` for
/// `eq(a, 2)` pushed to the coprocessor: the hint wins over a far cheaper
/// point get. This tier ignores the hint and takes the point get.
#[test]
fn force_index_is_accepted_and_disregarded() {
    let mut session = hinted_session();

    // Control: with no hint at all, the cost model picks the handle. This is
    // the same plan the hinted statement below produces, which is exactly how
    // we know the hint changed nothing.
    let unhinted = access_objects(&mut session, "EXPLAIN SELECT b FROM t WHERE a = 2");
    assert_eq!(unhinted, vec!["", "", "table:t"]);
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT b FROM t WHERE a = 2",
        "idx_b"
    ));

    // DIVERGENCE (#179): Go's plan reads `table:t, index:idx_b(b)`. This tier
    // produces the byte-identical unhinted plan, so `FORCE INDEX` is inert.
    let hinted = access_objects(
        &mut session,
        "EXPLAIN SELECT b FROM t FORCE INDEX(idx_b) WHERE a = 2",
    );
    assert_eq!(hinted, unhinted);
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT b FROM t FORCE INDEX(idx_b) WHERE a = 2",
        "idx_b"
    ));

    // The rows are right either way -- silently ignoring the hint costs the
    // plan, not the answer. Go's capture is `RS:2`.
    assert_eq!(
        row_text(session.run("SELECT b FROM t FORCE INDEX(idx_b) WHERE a = 2")),
        vec![vec!["2".to_owned()]]
    );

    // And it is silent about it: Go emits nothing here either, so the wire
    // count agrees for the wrong reason.
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// Go treats `USE INDEX` and `FORCE INDEX` identically (planbuilder.go:1513,
/// "we don't distinguish between FORCE and USE"). Both are equally inert here,
/// so the equivalence holds vacuously and will keep holding once the seam
/// closes.
#[test]
fn use_index_matches_force_index() {
    let mut session = hinted_session();

    let forced = access_objects(
        &mut session,
        "EXPLAIN SELECT b FROM t FORCE INDEX(idx_b) WHERE a = 2",
    );
    let used = access_objects(
        &mut session,
        "EXPLAIN SELECT b FROM t USE INDEX(idx_b) WHERE a = 2",
    );
    assert_eq!(forced, used);

    // DIVERGENCE (#179): Go's shared plan is the idx_b IndexReader; this is
    // the point-get plan instead.
    assert_eq!(used, vec!["", "", "table:t"]);
    assert_eq!(session.warnings(), &[]);
}

/// `IGNORE INDEX` must remove the named index from the candidate set, leaving
/// the table path. Go's capture is `TableReader -> TableFullScan`.
#[test]
fn ignore_index_is_accepted_and_disregarded() {
    let mut session = hinted_session();

    // Control: unhinted, the cost model reaches for idx_b. That is the path
    // IGNORE INDEX is supposed to take away.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t WHERE b = 2",
        "idx_b"
    ));

    // DIVERGENCE (#179): Go plans a TableFullScan. This tier still scans
    // idx_b -- the one index the statement explicitly forbade.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t IGNORE INDEX(idx_b) WHERE b = 2",
        "idx_b"
    ));

    // Go's capture is `RS:1`.
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM t IGNORE INDEX(idx_b) WHERE b = 2")),
        vec![vec!["1".to_owned()]]
    );
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// `USE INDEX ()` with an empty list means "use no indexes" and forces the
/// table path (planbuilder.go:1477, the `IndexNames == nil` branch).
#[test]
fn empty_use_index_list_is_accepted_and_disregarded() {
    let mut session = hinted_session();

    // DIVERGENCE (#179): Go plans a TableFullScan. This tier scans idx_b.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t USE INDEX() WHERE b = 2",
        "idx_b"
    ));
    assert_eq!(session.warnings(), &[]);
}

/// `FORCE INDEX` plus `IGNORE INDEX` on the same index leaves no usable index
/// path, and Go falls back to the table path rather than failing to plan.
#[test]
fn a_hint_pair_that_leaves_no_index_path_is_accepted_and_disregarded() {
    let mut session = hinted_session();

    // DIVERGENCE (#179): Go plans a TableFullScan. This tier scans idx_b.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t FORCE INDEX(idx_b) IGNORE INDEX(idx_b) WHERE b = 2",
        "idx_b"
    ));
    assert_eq!(session.warnings(), &[]);
}

/// A table-level hint naming an index that does not exist is a hard error in
/// Go -- 1176, `Key 'no_such_idx' doesn't exist in table 't'` -- for all three
/// of FORCE, USE and IGNORE. This tier plans the statement as if the hint were
/// not there and returns rows.
#[test]
fn a_hint_naming_a_missing_index_is_accepted_and_disregarded() {
    let mut session = hinted_session();

    for keyword in ["FORCE", "USE", "IGNORE"] {
        let sql = format!("EXPLAIN SELECT * FROM t {keyword} INDEX(no_such_idx) WHERE b = 2");

        // DIVERGENCE (#179): Go fails the statement with
        //   Error | 1176 | Key 'no_such_idx' doesn't exist in table 't'
        // This tier plans it, and picks idx_b as if unhinted.
        assert!(
            plan_uses_index(&mut session, &sql, "idx_b"),
            "{keyword}: expected the unhinted idx_b plan"
        );

        // ...and reports nothing at all about the index it could not find.
        assert_eq!(session.warnings(), &[], "{keyword}");
        assert_eq!(session.wire_warning_count(), 0, "{keyword}");
    }

    // The statement even succeeds and returns rows, where Go returns 1176.
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM t FORCE INDEX(no_such_idx) WHERE b = 2")),
        vec![vec!["1".to_owned()]]
    );
}

/// A comment-style optimizer hint naming a table the statement never mentions
/// is Go's canonical inapplicable-hint case (#153): the plan is unaffected and
/// warning 1815 says so, verbatim
///   `use_index(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists`
///
/// This tier drops it in silence. Reporting it is deliberately NOT implemented
/// here: the set of hints that are genuinely inapplicable only becomes
/// well-defined once table-level hints actually bind (#179), and announcing a
/// hint as unhonoured while honourable ones are still being ignored would be
/// wrong in both directions.
#[test]
fn an_inapplicable_comment_hint_is_dropped_without_reporting() {
    let mut session = hinted_session();

    // The plan is unaffected in both systems: Go also keeps the idx_b read.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT /*+ use_index(zzz, idx_b) */ * FROM t WHERE b = 2",
        "idx_b"
    ));

    // DIVERGENCE (#153): Go's `SHOW WARNINGS` has exactly one row,
    //   Warning | 1815 | use_index(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists
    // and the wire warning count is 1. Both are zero here.
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);

    // Same shape for a comment hint naming a missing index: Go warns 1176
    // (a warning, not the 1176 ERROR the table-level spelling raises) and
    // leaves the plan alone.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT /*+ use_index(t, no_such_idx) */ * FROM t WHERE b = 2",
        "idx_b"
    ));
    // DIVERGENCE (#153): Go has one `Warning | 1176 | Key 'no_such_idx'
    // doesn't exist in table 't'` here.
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// The warning channel this tier would report through already carries
/// everything both captured Go warnings need -- a level, a code, and a
/// message -- so #153 is blocked by the missing hint binding, not by the
/// channel. Go reaches 1815 through `StmtCtx.SetHintWarning`, which is
/// `AppendWarning`, i.e. level `Warning`, never `Note`.
#[test]
fn the_warning_channel_can_already_carry_both_captured_hint_warnings() {
    let mut session = hinted_session();

    session.append_warning(
        WarningLevel::Warning,
        1815,
        "use_index(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists"
            .to_owned(),
    );
    session.append_warning(
        WarningLevel::Warning,
        1176,
        "Key 'no_such_idx' doesn't exist in table 't'".to_owned(),
    );

    let reported: Vec<(&str, u16, &str)> = session
        .warnings()
        .iter()
        .map(|warning| {
            (
                warning.level.as_str(),
                warning.code,
                warning.message.as_str(),
            )
        })
        .collect();
    assert_eq!(
        reported,
        vec![
            (
                "Warning",
                1815,
                "use_index(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists"
            ),
            ("Warning", 1176, "Key 'no_such_idx' doesn't exist in table 't'"),
        ]
    );
    assert_eq!(session.wire_warning_count(), 2);
}
