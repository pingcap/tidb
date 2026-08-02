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
//! `tidb_ast::TableRef::hints` used to be produced by the parser and read by
//! nobody on the live path: the access-path decision is
//! `tidb_executor::driver::access::choose_index_range_path` ->
//! `tidb_executor::access_cost::enumerate_paths` -> `choose_access_path` ->
//! `tidb_executor::skyline::skyline_pruning`, and none of those saw a hint,
//! so the session ACCEPTED an index hint and the plan DISREGARDED it.
//!
//! `tidb_executor::index_hints` now resolves a table's hints into the set of
//! paths that still exist, and `enumerate_paths` builds only those --
//! Go's own placement, where `getPossibleAccessPaths` hands physical
//! selection an already-restricted `available` and the excluded paths are
//! never costed at all. Two consequences are load-bearing and are pinned
//! below rather than left to the reader:
//!
//! * the point get over the handle IS the table path, so a hint that deleted
//!   the table path deletes the point get with it (Go gates it on the same
//!   `indexIsAvailableByHints`, `point_get_plan.go:571`) -- otherwise
//!   `FORCE INDEX(idx_b) WHERE a = 2` would still answer from the row;
//! * 1176 is raised while resolving, before any path is chosen, over every
//!   table of the `FROM` rather than only the one the fast path narrows,
//!   because Go raises it per `DataSource` and whether or not the cost model
//!   would ever have wanted that index.
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
//! Every assertion below that still names a DIVERGENCE is a tripwire for
//! #153, the COMMENT-hint spelling, which is deliberately still unreported:
//! the set of hints that are genuinely inapplicable only becomes well-defined
//! now that table-level hints bind, and those two tests are what will go red
//! when it is implemented. They also serve as this file's control -- closing
//! #179 must not have moved the comment-hint plans or the warning count.

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

/// The code and message a statement failed with.
fn error_of(session: &mut Session, sql: &str) -> (u16, String) {
    let error = session.run(sql).unwrap_err().to_mysql_error();
    (error.code, error.message)
}

/// `FORCE INDEX` naming a real index constrains the access path even when the
/// cost model would rather use the clustered handle.
///
/// Go plans `IndexReader -> IndexFullScan on idx_b` with a `Selection` for
/// `eq(a, 2)` pushed to the coprocessor: the hint deletes the table path, and
/// with it the point get that path would have become, so a far cheaper plan
/// is simply not available to choose.
#[test]
fn force_index_constrains_the_access_path() {
    let mut session = hinted_session();

    // Control: with no hint at all, the cost model picks the handle. Go's
    // capture for the unhinted statement is `Point_Get ... handle:2`, so the
    // hinted plan below differing from this one is the whole point.
    let unhinted = access_objects(&mut session, "EXPLAIN SELECT b FROM t WHERE a = 2");
    assert_eq!(unhinted, vec!["", "", "table:t"]);
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT b FROM t WHERE a = 2",
        "idx_b"
    ));

    // Go reads `table:t, index:idx_b(b)`.
    let hinted = access_objects(
        &mut session,
        "EXPLAIN SELECT b FROM t FORCE INDEX(idx_b) WHERE a = 2",
    );
    assert_ne!(hinted, unhinted);
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT b FROM t FORCE INDEX(idx_b) WHERE a = 2",
        "idx_b"
    ));

    // Go's capture is `RS:2`: the plan changed, the answer did not.
    assert_eq!(
        row_text(session.run("SELECT b FROM t FORCE INDEX(idx_b) WHERE a = 2")),
        vec![vec!["2".to_owned()]]
    );

    // Go's `SHOW WARNINGS` is empty here: honouring a hint is silent.
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// Go treats `USE INDEX` and `FORCE INDEX` identically (planbuilder.go:1513,
/// "we don't distinguish between FORCE and USE"), and its two captures for
/// these statements are byte-identical.
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

    // Go's shared plan reads `table:t, index:idx_b(b)`, not the handle.
    assert!(used.iter().any(|object| object.contains("index:idx_b")));
    assert_eq!(
        row_text(session.run("SELECT b FROM t USE INDEX(idx_b) WHERE a = 2")),
        vec![vec!["2".to_owned()]]
    );
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// `IGNORE INDEX` removes the named index from the candidate set, leaving the
/// table path. Go's capture is `TableReader -> TableFullScan`.
#[test]
fn ignore_index_removes_the_named_path() {
    let mut session = hinted_session();

    // Control: unhinted, the cost model reaches for idx_b. That is the path
    // IGNORE INDEX takes away.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t WHERE b = 2",
        "idx_b"
    ));

    // Go plans a TableFullScan.
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t IGNORE INDEX(idx_b) WHERE b = 2",
        "idx_b"
    ));
    // The OTHER index is untouched: `IGNORE` removes one path, not indexing.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t IGNORE INDEX(idx_b) WHERE c = 2",
        "idx_c"
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
fn an_empty_use_index_list_forces_the_table_path() {
    let mut session = hinted_session();

    // Go plans a TableFullScan.
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t USE INDEX() WHERE b = 2",
        "idx_b"
    ));
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM t USE INDEX() WHERE b = 2")),
        vec![vec!["1".to_owned()]]
    );

    // The table path is the WHOLE table path, so the point get over the
    // handle is still reachable through it: Go's capture for
    // `use index() where a = 2` is `Point_Get ... handle:2`, the same plan
    // the unhinted statement gets.
    assert_eq!(
        access_objects(
            &mut session,
            "EXPLAIN SELECT * FROM t USE INDEX() WHERE a = 2"
        ),
        access_objects(&mut session, "EXPLAIN SELECT * FROM t WHERE a = 2")
    );
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// `FORCE INDEX` plus `IGNORE INDEX` on the same index leaves no usable index
/// path, and Go falls back to the table path rather than failing to plan --
/// "If we have got FORCE or USE index hint but got no available index, we
/// have to use table scan."
#[test]
fn a_hint_pair_that_leaves_no_index_path_reads_the_table() {
    let mut session = hinted_session();

    // Go plans a TableFullScan.
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t FORCE INDEX(idx_b) IGNORE INDEX(idx_b) WHERE b = 2",
        "idx_b"
    ));
    assert_eq!(
        row_text(
            session
                .run("SELECT count(*) FROM t FORCE INDEX(idx_b) IGNORE INDEX(idx_b) WHERE b = 2")
        ),
        vec![vec!["1".to_owned()]]
    );

    // What comes back is the WHOLE table path, not a bare full scan -- Go
    // appends `tablePath` itself, so everything that path can still do it
    // still does. Captured: under this hint pair `where a = 2` is
    // `Point_Get ... handle:2` and `where a > 3` is
    // `TableRangeScan ... range:(3,+inf]`, both the unhinted plans.
    for predicate in ["a = 2", "a > 3"] {
        let hinted = row_text(session.run(&format!(
            "EXPLAIN SELECT * FROM t FORCE INDEX(idx_b) IGNORE INDEX(idx_b) WHERE {predicate}"
        )));
        let unhinted = row_text(session.run(&format!("EXPLAIN SELECT * FROM t WHERE {predicate}")));
        assert_eq!(hinted, unhinted, "{predicate}");
    }
    // Go's capture is `RS:1`.
    assert_eq!(
        row_text(
            session
                .run("SELECT count(*) FROM t FORCE INDEX(idx_b) IGNORE INDEX(idx_b) WHERE a = 2")
        ),
        vec![vec!["1".to_owned()]]
    );
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// A table-level hint naming an index that does not exist fails the statement
/// -- 1176, `Key 'no_such_idx' doesn't exist in table 't'` -- for all three of
/// FORCE, USE and IGNORE, and before any path is chosen.
#[test]
fn a_hint_naming_a_missing_index_is_1176() {
    let mut session = hinted_session();

    for keyword in ["FORCE", "USE", "IGNORE"] {
        // Go: `ERR`, and `SHOW WARNINGS` reports it as
        //   Error | 1176 | Key 'no_such_idx' doesn't exist in table 't'
        let (code, message) = error_of(
            &mut session,
            &format!("EXPLAIN SELECT * FROM t {keyword} INDEX(no_such_idx) WHERE b = 2"),
        );
        assert_eq!(code, 1176, "{keyword}: {message}");
        assert_eq!(
            message, "Key 'no_such_idx' doesn't exist in table 't'",
            "{keyword}"
        );

        // The statement itself fails too, not just its EXPLAIN.
        let (code, message) = error_of(
            &mut session,
            &format!("SELECT count(*) FROM t {keyword} INDEX(no_such_idx) WHERE b = 2"),
        );
        assert_eq!(code, 1176, "{keyword}: {message}");
    }

    // Control: the SAME statements naming a real index plan and answer, so
    // 1176 is about the missing name and not about hints being present.
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM t FORCE INDEX(idx_b) WHERE b = 2")),
        vec![vec!["1".to_owned()]]
    );

    // A hint on ANY table of a join is validated, not only the one the
    // access-path decision would have narrowed. Go raises 1176 for both
    // spellings; captured.
    session
        .run("CREATE TABLE t2 (a BIGINT PRIMARY KEY, b INT)")
        .unwrap();
    session.run("INSERT INTO t2 VALUES (1,1),(2,2)").unwrap();
    for sql in [
        "SELECT count(*) FROM t FORCE INDEX(no_such_idx) JOIN t2 ON t.a = t2.a",
        "SELECT count(*) FROM t JOIN t2 FORCE INDEX(no_such_idx) ON t.a = t2.a",
    ] {
        let (code, message) = error_of(&mut session, sql);
        assert_eq!(code, 1176, "{sql}: {message}");
    }
}

/// A hinted index that neither narrows a range nor covers the read is still
/// the path, because the same hint deleted the table path it would otherwise
/// have lost to. This is Go's `keepIndex := ... || path.Forced` arm, and it is
/// the case where "the hint restricts the candidate set" and "the hint makes
/// the optimizer prefer the index" stop being distinguishable from the
/// outside -- there is nothing left to prefer over.
///
/// Go's capture for `use index(idx_c) where b = 2` is an `IndexLookUp` whose
/// build side is `IndexFullScan ... index:idx_c(c)` with `eq(test.t.b, 2)` as
/// the probe-side `Selection`, and the rows are `RS:2|2|2`.
#[test]
fn a_forced_index_that_neither_narrows_nor_covers_is_still_the_path() {
    let mut session = hinted_session();

    // Control: unhinted, the cost model reads idx_b and would never reach for
    // idx_c, which has nothing to say about `b`.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t WHERE b = 2",
        "idx_b"
    ));
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t WHERE b = 2",
        "idx_c"
    ));

    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t USE INDEX(idx_c) WHERE b = 2",
        "idx_c"
    ));
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t USE INDEX(idx_c) WHERE b = 2",
        "idx_b"
    ));
    // Reading the wrong index is still reading every matching row.
    assert_eq!(
        row_text(session.run("SELECT * FROM t USE INDEX(idx_c) WHERE b = 2")),
        vec![vec!["2".to_owned(), "2".to_owned(), "2".to_owned()]]
    );
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// A `FOR JOIN`/`FOR ORDER BY`/`FOR GROUP BY` qualifier takes the hint out of
/// scan-path selection entirely -- Go's `hint.HintScope != ast.HintForScan ->
/// continue`, which skips the name lookup too. So such a hint changes no plan
/// AND a missing index name in one is not 1176, which is the surprising half.
///
/// Captured: all three of `use index for join (idx_b)`,
/// `use index for join (no_such_idx)` and `use index for order by (idx_c)`
/// over `where a = 2` plan the same `Point_Get ... handle:2` the unhinted
/// statement does.
#[test]
fn a_scope_qualified_hint_is_inert() {
    let mut session = hinted_session();

    let unhinted = access_objects(&mut session, "EXPLAIN SELECT b FROM t WHERE a = 2");
    for hint in [
        "USE INDEX FOR JOIN (idx_b)",
        "USE INDEX FOR JOIN (no_such_idx)",
        "USE INDEX FOR ORDER BY (idx_c)",
        "IGNORE INDEX FOR GROUP BY (idx_b)",
    ] {
        let sql = format!("EXPLAIN SELECT b FROM t {hint} WHERE a = 2");
        assert_eq!(access_objects(&mut session, &sql), unhinted, "{hint}");
    }
    // Not 1176: the name was never looked up.
    assert_eq!(
        row_text(session.run("SELECT b FROM t USE INDEX FOR JOIN (no_such_idx) WHERE a = 2")),
        vec![vec!["2".to_owned()]]
    );
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// An INVISIBLE index is not an access path at all, so naming one in a hint is
/// the same 1176 as naming an index that was never created -- Go builds
/// `publicPaths` without it and `getPathByIndexName` then finds nothing.
/// Captured: `force index(idx_b)` on a table whose `idx_b` is invisible is
/// `ERR`, where the same statement with the index visible reads it.
#[test]
fn an_invisible_index_is_1176_in_a_hint() {
    let mut session = hinted_session();
    session
        .run("CREATE TABLE u (a BIGINT PRIMARY KEY, b INT, INDEX idx_b(b))")
        .unwrap();
    session.run("INSERT INTO u VALUES (1,1),(2,2)").unwrap();

    // Control: while it is visible, the hint binds to it.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM u FORCE INDEX(idx_b) WHERE b = 2",
        "idx_b"
    ));

    session
        .run("ALTER TABLE u ALTER INDEX idx_b INVISIBLE")
        .unwrap();
    let (code, message) = error_of(
        &mut session,
        "EXPLAIN SELECT * FROM u FORCE INDEX(idx_b) WHERE b = 2",
    );
    assert_eq!(code, 1176, "{message}");
    assert_eq!(message, "Key 'idx_b' doesn't exist in table 'u'");
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
