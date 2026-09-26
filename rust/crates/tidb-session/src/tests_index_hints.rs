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
//! The COMMENT-hint spelling (#153) is reported now too, by
//! `tidb_executor::index_hints::report_comment_index_hints`. It stays a
//! separate rule from the `FROM` spelling in both directions: a comment hint
//! matches the query block's ALIAS rather than the table name, and a bad
//! index name in one is a WARNING where the `FROM` spelling fails the
//! statement.
//!
//! MEASURED NEGATIVE, not implemented here: a matched comment hint does not
//! yet CONSTRAIN the plan the way the `FROM` spelling does. Go's
//! `getPossibleAccessPaths` appends it to the same `indexHints` slice, so
//! `/*+ use_index(t, idx_c) */ ... WHERE b = 2` plans the same `IndexLookUp`
//! over `idx_c` that `USE INDEX(idx_c)` does (captured). Only the WARNING
//! surface is closed.

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

/// The level, code and message of every warning the last statement reported.
fn reported(session: &Session) -> Vec<(&str, u16, String)> {
    session
        .warnings()
        .iter()
        .map(|warning| {
            (
                warning.level.as_str(),
                warning.code,
                warning.message.clone(),
            )
        })
        .collect()
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
    assert_eq!(unhinted, vec!["table:t"]);
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
/// The comment spelling is a genuinely different rule from the `FROM` one two
/// tests up: naming a missing INDEX there is a statement ERROR, here it is a
/// warning with the same 1176 and the same text, and the statement answers.
#[test]
fn an_inapplicable_comment_hint_is_reported_as_1815() {
    let mut session = hinted_session();

    // The plan is unaffected in both systems: Go also keeps the idx_b read.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT /*+ use_index(zzz, idx_b) */ * FROM t WHERE b = 2",
        "idx_b"
    ));
    assert_eq!(
        reported(&session),
        vec![(
            "Warning",
            1815,
            "use_index(test.zzz, idx_b) is inapplicable, \
             check whether the table(test.zzz) exists"
                .to_owned()
        )]
    );
    assert_eq!(session.wire_warning_count(), 1);

    // A comment hint naming a missing index of a table that DOES match warns
    // 1176 and leaves the plan alone -- the `FROM` spelling of the same
    // mistake fails the statement.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT /*+ use_index(t, no_such_idx) */ * FROM t WHERE b = 2",
        "idx_b"
    ));
    assert_eq!(
        reported(&session),
        vec![(
            "Warning",
            1176,
            "Key 'no_such_idx' doesn't exist in table 't'".to_owned()
        )]
    );
    assert_eq!(session.wire_warning_count(), 1);

    // Control: a comment hint that names a real table and a real index of it
    // is applicable, and reports nothing at all.
    session
        .run("SELECT /*+ use_index(t, idx_b) */ * FROM t WHERE b = 2")
        .unwrap();
    assert_eq!(session.warnings(), &[]);
    assert_eq!(session.wire_warning_count(), 0);
}

/// Go renders the 1815 text through `HintedIndex.IndexString`, and every
/// asymmetry of that rendering is measured rather than guessed: the table name
/// keeps the case it was WRITTEN in while each index name is lowercased, an
/// unqualified table is reported under the CURRENT database, a hint with no
/// index list has no comma at all, and `order_index`/`no_order_index` render
/// with an EMPTY hint name because Go's own `HintTypeString` has no arm for
/// them. All four captured verbatim.
#[test]
fn the_1815_text_reproduces_gos_rendering_asymmetries() {
    let mut session = hinted_session();

    for (sql, expected) in [
        (
            "SELECT /*+ USE_INDEX(ZZZ, IdX_B) */ * FROM t WHERE b = 2",
            "use_index(test.ZZZ, idx_b) is inapplicable, check whether the table(test.ZZZ) exists",
        ),
        (
            "SELECT /*+ use_index(mydb.zzz, idx_b) */ * FROM t WHERE b = 2",
            "use_index(mydb.zzz, idx_b) is inapplicable, check whether the table(mydb.zzz) exists",
        ),
        (
            "SELECT /*+ use_index(zzz) */ * FROM t WHERE b = 2",
            "use_index(test.zzz) is inapplicable, check whether the table(test.zzz) exists",
        ),
        (
            "SELECT /*+ order_index(zzz, idx_b) */ * FROM t WHERE b = 2",
            "(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists",
        ),
        (
            "SELECT /*+ ignore_index(zzz, idx_b) */ * FROM t WHERE b = 2",
            "ignore_index(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists",
        ),
        (
            "SELECT /*+ force_index(zzz, idx_b) */ * FROM t WHERE b = 2",
            "force_index(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists",
        ),
        (
            "SELECT /*+ use_index_merge(zzz, idx_b) */ * FROM t WHERE b = 2",
            "use_index_merge(test.zzz, idx_b) is inapplicable, check whether the table(test.zzz) exists",
        ),
    ] {
        session.run(sql).unwrap();
        assert_eq!(
            reported(&session),
            vec![("Warning", 1815, expected.to_owned())],
            "{sql}"
        );
    }
}

/// Go matches the hint against the `DataSource`'s reported name, which is the
/// ALIAS whenever one is written -- so aliasing a table makes its own name
/// stop matching. A DERIVED table is not a `DataSource` at all, so its alias
/// matches nothing either. A `FROM`-less select matches nothing by
/// construction. All captured.
#[test]
fn a_comment_hint_matches_the_alias_not_the_table() {
    let mut session = hinted_session();

    // The alias matches, and reports nothing.
    session
        .run("SELECT /*+ use_index(t2, idx_b) */ * FROM t t2 WHERE b = 2")
        .unwrap();
    assert_eq!(session.warnings(), &[]);

    // The underlying name no longer does.
    session
        .run("SELECT /*+ use_index(t, idx_b) */ * FROM t t2 WHERE b = 2")
        .unwrap();
    assert_eq!(
        reported(&session),
        vec![(
            "Warning",
            1815,
            "use_index(test.t, idx_b) is inapplicable, check whether the table(test.t) exists"
                .to_owned()
        )]
    );

    // A derived table's alias is not a `DataSource` name.
    session
        .run("SELECT /*+ use_index(d, idx_b) */ * FROM (SELECT * FROM t) d WHERE b = 2")
        .unwrap();
    assert_eq!(
        reported(&session),
        vec![(
            "Warning",
            1815,
            "use_index(test.d, idx_b) is inapplicable, check whether the table(test.d) exists"
                .to_owned()
        )]
    );

    // No `FROM` at all: the hint still reports.
    session.run("SELECT /*+ use_index(t, i) */ 1").unwrap();
    assert_eq!(
        reported(&session),
        vec![(
            "Warning",
            1815,
            "use_index(test.t, i) is inapplicable, check whether the table(test.t) exists"
                .to_owned()
        )]
    );

    // An explicit database qualifier matches case-insensitively.
    session
        .run("SELECT /*+ use_index(TeSt.t, idx_b) */ * FROM t WHERE b = 2")
        .unwrap();
    assert_eq!(session.warnings(), &[]);
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

/// A COMMENT-style `use_index` restricts the candidate set exactly as the
/// `FROM`-clause spelling does.
///
/// Go appends the comment hints to the very same `indexHints` slice
/// (`getPossibleAccessPaths`, `planbuilder.go:1445`) and iterates it once, so
/// there is one rule, not two. Before this was wired the comment spelling was
/// only ever a source of WARNINGS: the plan disregarded it and read the table.
///
/// MUTATION: drop the comment-hint arm of `HintAccumulator` and this reads
/// `TableFullScan` instead of `IndexFullScan table:t, index:idx_b(b)`.
#[test]
fn a_comment_index_hint_constrains_the_access_path() {
    let mut session = hinted_session();

    // Control: unhinted, `SELECT *` over a table with no `WHERE` is the
    // cheapest full scan and reaches for no index at all.
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT * FROM t",
        "idx_b"
    ));

    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT /*+ use_index(t, idx_b) */ * FROM t",
        "idx_b"
    ));
    // `index_lookup_pushdown` is Go's `ast.HintUse` with `PushDownLookUp`
    // set, so it restricts identically -- which is the whole reason the
    // recorded plan for it reads a NON-COVERING index.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT /*+ index_lookup_pushdown(t, idx_b) */ * FROM t",
        "idx_b"
    ));
    // And the hint must reach the table it NAMES, not any table: an alias
    // makes `use_index(t, ...)` match nothing (Go matches the `DataSource`'s
    // reported name, which is the alias when one is written).
    assert!(!plan_uses_index(
        &mut session,
        "EXPLAIN SELECT /*+ use_index(t, idx_b) */ * FROM t AS x",
        "idx_b"
    ));
}

/// `INDEX_LOOKUP_PUSHDOWN` naming a GLOBAL index of a partitioned table is
/// Go's 1815 `checkIndexLookUpPushDownSupported` refusal -- and the plan that
/// follows reads the TABLE, because the refusal happens AFTER the hint has
/// already deleted every other path.
///
/// Recorded verbatim in
/// `tests/integrationtest/r/executor/index_lookup_pushdown_partition.result`.
///
/// MUTATION: return `true` unconditionally from
/// `check_index_look_up_push_down_supported` and the plan reads `idx_c` with
/// no warning at all; drop only the warning and the plan stays right while
/// the wire loses the explanation.
#[test]
fn index_lookup_pushdown_refuses_a_global_index_with_1815() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE tp (a INT PRIMARY KEY, b INT, c INT, KEY idx_b(b), \
             KEY idx_c(c) GLOBAL) PARTITION BY HASH(a) PARTITIONS 4",
        )
        .unwrap();

    // Control: the same hint on a LOCAL index of the same table is honoured,
    // so the refusal below is about `GLOBAL` and not about partitioning.
    assert!(plan_uses_index(
        &mut session,
        "EXPLAIN SELECT /*+ index_lookup_pushdown(tp, idx_b) */ * FROM tp",
        "idx_b"
    ));
    assert!(reported(&session).is_empty());

    let objects = access_objects(
        &mut session,
        "EXPLAIN SELECT /*+ index_lookup_pushdown(tp, idx_c) */ * FROM tp",
    );
    assert!(
        objects.iter().all(|object| !object.contains("index:idx_c")),
        "the refused hint must not leave the global index in the plan: {objects:?}"
    );
    assert!(
        objects.iter().any(|object| object.contains("table:tp")),
        "Go's emptied candidate set falls back to the table path: {objects:?}"
    );
    assert_eq!(
        reported(&session),
        vec![(
            "Warning",
            1815,
            "hint INDEX_LOOKUP_PUSHDOWN is inapplicable, \
             the global index in partition table is not supported"
                .to_owned()
        )]
    );
}

#[test]
fn tidb_enable_index_merge_controls_automatic_or_paths() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE im (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, \
             KEY ia(a), KEY ib(b))",
        )
        .unwrap();
    session
        .run("INSERT INTO im VALUES (1,1,2),(2,1,0),(3,0,2),(4,0,0),(5,1,9)")
        .unwrap();
    let sql = "EXPLAIN SELECT id FROM im WHERE a = 1 OR b = 2";
    assert!(
        row_text(session.run(sql))
            .iter()
            .flatten()
            .any(|cell| cell.contains("IndexMerge")),
        "the default ON value must cost the automatic OR reader"
    );

    session.run("SET tidb_enable_index_merge = OFF").unwrap();
    assert!(
        row_text(session.run(sql))
            .iter()
            .flatten()
            .all(|cell| !cell.contains("IndexMerge")),
        "OFF must remove automatic IndexMerge candidates"
    );
}

#[test]
fn hinted_index_merge_intersection_retains_residual_predicates_when_disabled() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE im_and (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, KEY ia(a), KEY ib(b))",
        )
        .unwrap();
    session
        .run("INSERT INTO im_and VALUES (1,1,2),(2,1,0),(3,0,2),(4,1,2)")
        .unwrap();
    session.run("SET tidb_enable_index_merge = OFF").unwrap();
    let query = "SELECT /*+ USE_INDEX_MERGE(im_and, ia, ib) */ id FROM im_and WHERE a = 1 AND b = 2 AND id + 1 > 2";
    let plan = row_text(session.run(&format!("EXPLAIN {query}")));
    assert!(
        plan.iter()
            .flatten()
            .any(|cell| cell.contains("IndexMerge")),
        "{plan:?}"
    );
    let builds = plan
        .iter()
        .filter(|row| row[0].contains("Selection") && row[0].contains("(Build)"))
        .collect::<Vec<_>>();
    assert_eq!(
        builds.len(),
        2,
        "Go filters the handle expression on both partial indexes: {plan:?}"
    );
    assert!(builds.iter().all(|row| row[1] == "8.00"));
    assert!(plan
        .iter()
        .filter(|row| row[0].contains("IndexRangeScan"))
        .all(|row| row[1] == "10.00"));
    assert!(plan
        .iter()
        .filter(|row| row[0].contains("IndexMerge") || row[0].contains("TableRowIDScan"))
        .all(|row| row[1] == "1.00"));
    assert_eq!(row_text(session.run(query)), vec![vec!["4".to_owned()]]);
    assert_eq!(
        row_text(session.run(&format!("{query} LIMIT 1"))),
        vec![vec!["4".to_owned()]]
    );
    session
        .run("INSERT INTO mysql.expr_pushdown_blacklist VALUES ('gt','tikv','test')")
        .unwrap();
    session.run("ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST").unwrap();
    let blacklisted = row_text(session.run(&format!("EXPLAIN {query}")));
    assert!(
        blacklisted
            .iter()
            .all(|row| !(row[0].contains("Selection") && row[0].contains("(Build)"))),
        "blacklisted residual must not be covered by either partial index: {blacklisted:?}"
    );
    assert!(
        blacklisted
            .iter()
            .any(|row| row[0].contains("Selection") && row[1] == "0.80" && row[2] == "root"),
        "Go retains the residual on the root task: {blacklisted:?}"
    );
    assert_eq!(row_text(session.run(query)), vec![vec!["4".to_owned()]]);
}

#[test]
fn hinted_index_merge_intersection_preserves_prefix_rechecks() {
    let mut session = Session::new();
    session.run("CREATE TABLE im_prefix (id BIGINT PRIMARY KEY, a VARCHAR(20), b BIGINT, KEY ia(a(2)), KEY ib(b))").unwrap();
    session
        .run("INSERT INTO im_prefix VALUES (1,'abc',2),(2,'abd',2),(3,'abc',3),(4,'abc',2)")
        .unwrap();
    let query = "SELECT /*+ USE_INDEX_MERGE(im_prefix, ia, ib) */ id FROM im_prefix WHERE a = 'abc' AND b = 2";
    let plan = row_text(session.run(&format!("EXPLAIN {query}")));
    assert!(
        plan.iter()
            .flatten()
            .any(|cell| cell.contains("IndexMerge")),
        "{plan:?}"
    );
    assert!(
        plan.iter()
            .any(|row| row[0].contains("Selection") && row[0].contains("(Probe)")),
        "the prefix predicate needs a full-value recheck: {plan:?}"
    );
    let mut rows = row_text(session.run(query));
    rows.sort();
    assert_eq!(rows, vec![vec!["1".to_owned()], vec!["4".to_owned()]]);
}

#[test]
fn skyline_keeps_indexes_with_incomparable_access_columns() {
    let mut session = Session::new();
    session.run("CREATE TABLE skyline_columns (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, c BIGINT, payload BIGINT, KEY iab(a,b), KEY ic(c))").unwrap();
    let rows = (0..1000)
        .map(|n| format!("({n},1,1,{n},{n})"))
        .collect::<Vec<_>>()
        .join(",");
    session
        .run(&format!("INSERT INTO skyline_columns VALUES {rows}"))
        .unwrap();
    session.run("ANALYZE TABLE skyline_columns").unwrap();
    let query = "SELECT payload FROM skyline_columns WHERE a=1 AND b=1 AND c=7";
    let plan = row_text(session.run(&format!("EXPLAIN {query}")));
    assert!(
        plan.iter()
            .flatten()
            .any(|cell| cell.contains("index:ic(c)")),
        "incomparable access-column sets must reach Go's skyline comparison: {plan:?}"
    );
    assert_eq!(row_text(session.run(query)), vec![vec!["7".to_owned()]]);
}

#[test]
fn single_index_union_requires_an_explicit_merge_hint() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE im_same (id BIGINT PRIMARY KEY, a BIGINT, payload BIGINT, KEY ia(a))")
        .unwrap();
    session
        .run("INSERT INTO im_same VALUES (1,1,10),(2,2,20),(3,3,30),(4,1,40)")
        .unwrap();
    session.run("SET tidb_enable_index_merge=ON").unwrap();
    for (hint, expected_merge) in [("", false), ("/*+ USE_INDEX_MERGE(im_same, ia) */", true)] {
        let query = format!("SELECT {hint} payload FROM im_same WHERE a=1 OR a=2");
        let plan = row_text(session.run(&format!("EXPLAIN {query}")));
        assert_eq!(
            plan.iter()
                .flatten()
                .any(|cell| cell.contains("IndexMerge")),
            expected_merge,
            "{plan:?}"
        );
        let mut rows = row_text(session.run(&query));
        rows.sort();
        assert_eq!(
            rows,
            vec![
                vec!["10".to_owned()],
                vec!["20".to_owned()],
                vec!["40".to_owned()]
            ]
        );
    }
}

#[test]
fn union_merge_limit_caps_partials_and_deduplicates_before_offset() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE im_limit(id INT PRIMARY KEY, a INT, b INT, KEY ia(a), KEY ib(b))")
        .unwrap();
    session
        .run("INSERT INTO im_limit VALUES(1,1,0),(2,0,2),(3,1,2),(4,1,0),(5,0,2),(6,0,0)")
        .unwrap();
    let query =
        "SELECT /*+ USE_INDEX_MERGE(im_limit,ia,ib) */ id FROM im_limit WHERE a=1 OR b=2 LIMIT 1,3";
    let unlimited = row_text(session.run(&format!(
        "EXPLAIN FORMAT='brief' {}",
        query.trim_end_matches(" LIMIT 1,3")
    )));
    assert!(
        unlimited
            .iter()
            .filter(|row| row[0].contains("IndexMerge") || row[0].contains("TableRowIDScan"))
            .all(|row| row[1] == "19.99"),
        "{unlimited:?}"
    );
    let grouped = row_text(session.run("EXPLAIN FORMAT='brief' SELECT /*+ USE_INDEX_MERGE(im_limit,ia,ib) */ id FROM im_limit WHERE a=1 OR a=2 OR b=2"));
    assert!(
        grouped
            .iter()
            .filter(|row| row[0].contains("IndexMerge") || row[0].contains("TableRowIDScan"))
            .all(|row| row[1] == "259.75"),
        "{grouped:?}"
    );
    let plan = row_text(session.run(&format!("EXPLAIN FORMAT='brief' {query}")));
    assert!(
        plan.iter().any(|row| row[0].contains("IndexMerge")
            && row[4].contains("limit embedded(offset:1, count:3)")),
        "{plan:?}"
    );
    assert_eq!(
        plan.iter()
            .filter(|row| row[0].contains("Limit(Build)"))
            .count(),
        2,
        "{plan:?}"
    );
    assert!(
        !plan.iter().any(|row| row[0].contains("Limit(Probe)")),
        "{plan:?}"
    );
    assert!(
        plan.iter()
            .filter(|row| row[0].contains("IndexRangeScan"))
            .all(|row| row[1] == "2.00"),
        "{plan:?}"
    );
    assert!(
        plan.iter()
            .filter(|row| row[0].contains("IndexMerge") || row[0].contains("TableRowIDScan"))
            .all(|row| row[1] == "4.00"),
        "{plan:?}"
    );
    let rows = row_text(session.run(query));
    assert_eq!(rows.len(), 3);
    let ids: std::collections::BTreeSet<_> = rows
        .iter()
        .map(|row| row[0].parse::<i64>().unwrap())
        .collect();
    assert_eq!(
        ids.len(),
        3,
        "union deduplicates before applying offset/count"
    );
    assert!(ids.iter().all(|id| (1..=5).contains(id)));
}

#[test]
fn ordered_union_merge_preserves_direction_dedup_and_limit() {
    let mut session = Session::new();
    session.run("CREATE TABLE ordered_union(id INT PRIMARY KEY,a INT,b INT,c INT,KEY ia(a,c),KEY ib(b,c))").unwrap();
    session.run("INSERT INTO ordered_union VALUES(1,1,0,40),(2,0,2,10),(3,1,2,30),(4,1,0,20),(5,0,2,50),(6,0,0,0)").unwrap();
    for (suffix, expected, partial_rows, probe_rows) in [
        (
            "ORDER BY c",
            vec![
                vec!["2", "10"],
                vec!["4", "20"],
                vec!["3", "30"],
                vec!["1", "40"],
                vec!["5", "50"],
            ],
            "10.00",
            "19.99",
        ),
        (
            "ORDER BY c DESC",
            vec![
                vec!["5", "50"],
                vec!["1", "40"],
                vec!["3", "30"],
                vec!["4", "20"],
                vec!["2", "10"],
            ],
            "10.00",
            "19.99",
        ),
        (
            "ORDER BY c LIMIT 1,3",
            vec![vec!["4", "20"], vec!["3", "30"], vec!["1", "40"]],
            "2.00",
            "4.00",
        ),
        (
            "ORDER BY c DESC LIMIT 3",
            vec![vec!["5", "50"], vec!["1", "40"], vec!["3", "30"]],
            "1.50",
            "3.00",
        ),
    ] {
        let query = format!("SELECT /*+ USE_INDEX_MERGE(ordered_union,ia,ib) */ id,c FROM ordered_union WHERE a=1 OR b=2 {suffix}");
        let plan = row_text(session.run(&format!("EXPLAIN FORMAT='brief' {query}")));
        assert!(
            !plan
                .iter()
                .any(|row| row[0].contains("Sort") || row[0].contains("TopN")),
            "{plan:?}"
        );
        let scans: Vec<_> = plan
            .iter()
            .filter(|row| row[0].contains("IndexRangeScan"))
            .collect();
        assert_eq!(scans.len(), 2, "{plan:?}");
        for scan in scans {
            assert_eq!(scan[1], partial_rows, "{plan:?}");
            assert!(scan[4].contains("keep order:true"), "{plan:?}");
            assert_eq!(
                scan[4].contains("desc"),
                suffix.contains("DESC"),
                "{plan:?}"
            );
        }
        assert!(
            plan.iter()
                .any(|row| row[0].contains("IndexMerge") && row[1] == probe_rows),
            "{plan:?}"
        );
        assert_eq!(row_text(session.run(&query)), expected, "{suffix}");
    }
    let hidden = "SELECT /*+ USE_INDEX_MERGE(ordered_union,ia,ib) */ id FROM ordered_union WHERE a=1 OR b=2 ORDER BY c DESC LIMIT 3";
    let hidden_plan = row_text(session.run(&format!("EXPLAIN FORMAT='brief' {hidden}")));
    assert!(hidden_plan.iter().any(|row| row[0].contains("IndexMerge")), "{hidden_plan:?}");
    assert!(!hidden_plan.iter().any(|row| row[0].contains("Sort") || row[0].contains("TopN")), "{hidden_plan:?}");
    assert_eq!(
        row_text(session.run(hidden)),
        vec![vec!["5"], vec!["1"], vec!["3"]]
    );
    session
        .run("CREATE TABLE ordered_handle(id INT PRIMARY KEY,a INT,b INT,KEY ia(a),KEY ib(b))")
        .unwrap();
    session
        .run("INSERT INTO ordered_handle VALUES(5,0,2),(1,1,0),(3,1,2),(2,0,2),(4,1,0)")
        .unwrap();
    let handles = "SELECT /*+ USE_INDEX_MERGE(ordered_handle,ia,ib) */ id FROM ordered_handle WHERE a=1 OR b=2 ORDER BY id LIMIT 1,3";
    let handle_plan = row_text(session.run(&format!("EXPLAIN FORMAT='brief' {handles}")));
    assert!(
        !handle_plan
            .iter()
            .any(|row| row[0].contains("Sort") || row[0].contains("TopN")),
        "{handle_plan:?}"
    );
    assert_eq!(
        row_text(session.run(handles)),
        vec![vec!["2"], vec!["3"], vec!["4"]]
    );
}

#[test]
fn merge_table_probe_preserves_residual_topn_and_offset() {
    let mut session = Session::new();
    session.run("CREATE TABLE im_probe (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, c BIGINT, KEY ia(a), KEY ib(b))").unwrap();
    session.run("INSERT INTO im_probe VALUES (1,1,2,10),(2,1,2,20),(3,1,2,30),(4,1,2,40),(5,1,2,50),(6,1,2,60)").unwrap();
    for batch in [3, 20_000] {
        session
            .run(&format!("SET tidb_index_lookup_size={batch}"))
            .unwrap();
        let query = "SELECT /*+ USE_INDEX_MERGE(im_probe, ia, ib) */ id FROM im_probe WHERE a=1 AND b=2 AND c>20 ORDER BY c DESC LIMIT 1,2";
        let plan = row_text(session.run(&format!("EXPLAIN {query}")));
        assert!(
            plan.iter().any(|row| row[0].contains("IndexMerge")),
            "{plan:?}"
        );
        assert!(
            plan.iter().any(|row| row[0].contains("TopN")
                && row[0].contains("(Probe)")
                && row[4].contains("offset:0, count:3")),
            "{plan:?}"
        );
        assert_eq!(
            row_text(session.run(query)),
            vec![vec!["5".to_owned()], vec!["4".to_owned()]]
        );
    }
}

/// Go pkg/executor/test/indexmergereadtest.TestIssues70910.
#[test]
fn ordered_index_merge_limit_exceeds_initial_heap_allocation() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a INT, b INT, c INT, INDEX idx1(a,c), INDEX idx2(b,c))")
        .unwrap();
    for start in (0..3000).step_by(500) {
        let values = (start..start + 500)
            .map(|i| format!("(1,1,{i})"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .run(&format!("INSERT INTO t(a,b,c) VALUES {values}"))
            .unwrap();
    }
    for (direction, offset, count) in [("ASC", 0, 2000), ("ASC", 1500, 1100), ("DESC", 1500, 1100)]
    {
        let query = format!("SELECT /*+ USE_INDEX_MERGE(t, idx1, idx2) */ * FROM t WHERE a=1 OR b=1 ORDER BY c {direction} LIMIT {offset},{count}");
        let plan = row_text(session.run(&format!("EXPLAIN {query}")));
        assert!(
            plan.iter().any(|row| row[0].contains("IndexMerge")),
            "{plan:?}"
        );
        assert!(
            !plan
                .iter()
                .any(|row| row[0].contains("TopN") || row[0].contains("Sort")),
            "{plan:?}"
        );
        let rows = row_text(session.run(&query));
        assert_eq!(rows.len(), count);
        for (index, row) in rows.iter().enumerate() {
            let value = if direction == "ASC" {
                offset + index
            } else {
                2999 - offset - index
            };
            assert_eq!(row[2], value.to_string());
        }
    }
}

#[test]
fn union_merge_retains_source_or_for_uncovered_branch_predicates() {
    let mut session = Session::new();
    session.run("CREATE TABLE union_residual(id bigint primary key,a bigint,b bigint,c bigint,key ia(a),key ib(b))").unwrap();
    session
        .run("INSERT INTO union_residual VALUES (1,1,0,1),(2,1,0,3),(3,0,2,1),(4,1,2,3)")
        .unwrap();
    for (indexes, predicate, expected) in [
        (
            "ia,ib",
            "(a=1 AND c>2) OR b=2",
            vec![vec!["2"], vec!["3"], vec!["4"]],
        ),
        (
            "primary,ib",
            "(id=1 AND c>2) OR b=2",
            vec![vec!["3"], vec!["4"]],
        ),
        (
            "primary,ib",
            "(id<3 AND id+1>2) OR b=2",
            vec![vec!["2"], vec!["3"], vec!["4"]],
        ),
        (
            "primary,ib",
            "(id< -3 AND c>2) OR b=2",
            vec![vec!["3"], vec!["4"]],
        ),
        (
            "ia,ib",
            "(a=1 AND id+1>2) OR b=2",
            vec![vec!["2"], vec!["3"], vec!["4"]],
        ),
    ] {
        let query = format!("SELECT /*+ USE_INDEX_MERGE(union_residual,{indexes}) */ id FROM union_residual WHERE {predicate}");
        let plan = row_text(session.run(&format!("EXPLAIN {query}")));
        assert!(
            plan.iter()
                .any(|row| row[0].contains("IndexMerge") && row[4].contains("type: union")),
            "Go retains the range-producing branch and rechecks the source OR: {query}: {plan:?}"
        );
        let probe = plan
            .iter()
            .find(|row| row[0].contains("TableRowIDScan"))
            .unwrap();
        if predicate == "(a=1 AND c>2) OR b=2" {
            assert_eq!(probe[1], "19.99", "{plan:?}");
        } else if predicate == "(id=1 AND c>2) OR b=2" {
            assert_eq!(probe[1], "11.00", "{plan:?}");
        } else if predicate == "(id<3 AND id+1>2) OR b=2" {
            assert_eq!(probe[1], "13.00", "{plan:?}");
        }
        if predicate == "(id< -3 AND c>2) OR b=2" {
            assert_eq!(probe[1], "3340.00", "{plan:?}");
        }
        if predicate.contains("id+1") {
            assert!(
                plan.iter()
                    .any(|row| row[0].contains("Selection") && row[4].contains("plus(")),
                "branch residual must survive physical conversion: {plan:?}"
            );
        }
        let mut rows = row_text(session.run(&query));
        rows.sort();
        assert_eq!(rows, expected, "{query}: {plan:?}");
        let limited = row_text(session.run(&format!("{query} ORDER BY id LIMIT 2 OFFSET 1")));
        assert_eq!(
            limited,
            expected.into_iter().skip(1).take(2).collect::<Vec<_>>(),
            "{query}"
        );
    }
    session
        .run("INSERT INTO mysql.expr_pushdown_blacklist VALUES ('gt','tikv','union residual')")
        .unwrap();
    session.run("ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST").unwrap();
    for (indexes, predicate, expected) in [
        (
            "ia,ib",
            "(a=1 AND c>2) OR b=2",
            vec![vec!["2"], vec!["3"], vec!["4"]],
        ),
        (
            "primary,ib",
            "(id=1 AND c>2) OR b=2",
            vec![vec!["3"], vec!["4"]],
        ),
    ] {
        let query = format!("SELECT /*+ USE_INDEX_MERGE(union_residual,{indexes}) */ id FROM union_residual WHERE {predicate}");
        let plan = row_text(session.run(&format!("EXPLAIN {query}")));
        assert!(
            plan.iter().any(|row| row[0].contains("IndexMerge")),
            "{plan:?}"
        );
        assert!(
            plan.iter().any(|row| row[0].contains("Selection")
                && row[2] == "root"
                && row[4].contains("or(")),
            "{plan:?}"
        );
        let mut rows = row_text(session.run(&query));
        rows.sort();
        assert_eq!(rows, expected);
    }
    session
        .run("DELETE FROM mysql.expr_pushdown_blacklist")
        .unwrap();
    session.run("ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST").unwrap();
    session
        .run("ANALYZE TABLE union_residual ALL COLUMNS")
        .unwrap();
    let query = "SELECT /*+ USE_INDEX_MERGE(union_residual,primary,ib) */ id FROM union_residual WHERE (id=1 AND c>2) OR b=2";
    let plan = row_text(session.run(&format!("EXPLAIN {query}")));
    let partial = plan
        .iter()
        .find(|row| row[0].contains("TableRangeScan"))
        .unwrap();
    assert_eq!(
        partial[1], "1.00",
        "table partial must estimate its own range: {plan:?}"
    );
    let partial = plan
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap();
    assert_eq!(
        partial[1], "2.00",
        "TopN-only index statistics must reach branch estimation: {plan:?}"
    );
    let mut rows = row_text(session.run(query));
    rows.sort();
    assert_eq!(rows, vec![vec!["3"], vec!["4"]]);
    session
        .run("ALTER TABLE union_residual ADD INDEX ibc(b,c)")
        .unwrap();
    let query = "SELECT /*+ USE_INDEX_MERGE(union_residual,primary,ibc) */ id FROM union_residual WHERE (id=1 AND c>2) OR b=2";
    let plan = row_text(session.run(&format!("EXPLAIN {query}")));
    let partial = plan
        .iter()
        .find(|row| row[0].contains("IndexRangeScan"))
        .unwrap();
    assert_eq!(
        partial[1], "2.00",
        "new index branch must use its own column statistics: {plan:?}"
    );
    let mut rows = row_text(session.run(query));
    rows.sort();
    assert_eq!(rows, vec![vec!["3"], vec!["4"]]);
}

#[test]
fn union_merge_completes_composite_ranges_from_top_level_predicates() {
    let mut session = Session::new();
    session.run("CREATE TABLE union_cnf(id bigint primary key,a bigint,b bigint,c bigint,d bigint,key iab(a,b),key iac(a,c))").unwrap();
    session
        .run("INSERT INTO union_cnf VALUES (1,1,2,0,9),(2,1,0,3,8),(3,2,2,3,7),(4,1,4,5,6)")
        .unwrap();
    let query = "SELECT /*+ USE_INDEX_MERGE(union_cnf,iab,iac) */ id FROM union_cnf WHERE a=1 AND (b=2 OR c=3)";
    let plan = row_text(session.run(&format!("EXPLAIN {query}")));
    assert!(
        plan.iter()
            .any(|row| row[0].contains("IndexMerge") && row[4].contains("type: union")),
        "{plan:?}"
    );
    assert!(
        !plan.iter().any(|row| row[0].contains("Selection")),
        "Go removes a=1 after every selected branch absorbs it: {plan:?}"
    );
    let probe = plan.iter().find(|row| row[0].contains("TableRowIDScan")).unwrap();
    assert_eq!(probe[1], "8.00", "{plan:?}");
    for partial in plan.iter().filter(|row| row[0].contains("IndexRangeScan")) {
        assert_eq!(partial[1], "0.10", "{plan:?}");
    }
    let mut rows = row_text(session.run(query));
    rows.sort();
    assert_eq!(rows, vec![vec!["1"], vec!["2"]]);
    for (extra, expected) in [
        (" AND d>8", vec![vec!["1"]]),
        (" AND (b=0 OR c=0)", vec![vec!["1"], vec!["2"]]),
        (" ORDER BY id LIMIT 1 OFFSET 1", vec![vec!["2"]]),
    ] {
        let sql = format!("{query}{extra}");
        let plan = row_text(session.run(&format!("EXPLAIN {sql}")));
        assert!(
            plan.iter()
                .any(|row| row[0].contains("IndexMerge") && row[4].contains("type: union")),
            "{sql}: {plan:?}"
        );
        let mut rows = row_text(session.run(&sql));
        rows.sort();
        assert_eq!(rows, expected, "{sql}: {plan:?}");
    }
}
