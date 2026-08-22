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

//! `mysql.expr_pushdown_blacklist`, `mysql.opt_rule_blacklist` and the
//! `ADMIN RELOAD` that publishes each.
//!
//! Queries are the source corpus's own:
//! `tests/integrationtest/t/black_list.test`.

use crate::tests_support::row_text;
use crate::Session;

/// The plan an `EXPLAIN` prints, one operator per line.
fn plan(session: &mut Session, sql: &str) -> String {
    row_text(session.run(&format!("EXPLAIN {sql}")))
        .into_iter()
        .map(|row| row.join(" | "))
        .collect::<Vec<_>>()
        .join("\n")
}

fn enum_fixture() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t(a enum('a','b','c'), b enum('a','b','c'), c int, index idx(b,a))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    session
}

/// Both tables exist for a fresh store, and both accept ordinary DML.
///
/// Go creates them in `doDDLWorks` and leaves them EMPTY: only an upgrade
/// from a pre-v4 cluster seeds `expr_pushdown_blacklist`
/// (`writeDefaultExprPushDownBlacklist`).
#[test]
fn a_fresh_store_has_both_blacklist_tables_and_they_start_empty() {
    let mut session = Session::new();

    assert!(row_text(session.run("SELECT * FROM mysql.expr_pushdown_blacklist")).is_empty());
    assert!(row_text(session.run("SELECT * FROM mysql.opt_rule_blacklist")).is_empty());

    session
        .run("INSERT INTO mysql.expr_pushdown_blacklist VALUES ('<','tikv,tiflash,tidb','why')")
        .unwrap();
    assert_eq!(
        row_text(session.run("SELECT name, store_type, reason FROM mysql.expr_pushdown_blacklist")),
        vec![vec!["<", "tikv,tiflash,tidb", "why"]]
    );
    // The `store_type` default is Go's own column default.
    session
        .run("INSERT INTO mysql.expr_pushdown_blacklist(name) VALUES ('enum')")
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT store_type FROM mysql.expr_pushdown_blacklist WHERE name = 'enum'"
        )),
        vec![vec!["tikv,tiflash,tidb"]]
    );
}

/// Blacklisting a column TYPE removes the index path, not just the pushed
/// filter.
///
/// Go's `columnToPBExpr` refuses an `ENUM` column outright once `enum` is
/// blacklisted, and `DataSource.PredicatePushDown` filters predicates through
/// `PushDownExprs` BEFORE any access path is derived -- so the ranger never
/// sees the condition and the index it constrained stops being a candidate.
/// The condition still filters, from the `Selection` above the scan, so the
/// ROWS are unchanged.
#[test]
fn blacklisting_enum_drops_the_enum_index_path_and_keeps_the_rows() {
    let mut session = enum_fixture();

    let before = plan(&mut session, "SELECT * FROM t WHERE b = 'a'");
    assert!(
        before.contains("IndexRangeScan") && before.contains("range:[\"a\",\"a\"]"),
        "the enum index range is the starting point:\n{before}"
    );

    session
        .run("INSERT INTO mysql.expr_pushdown_blacklist(name) VALUES ('enum')")
        .unwrap();
    // Nothing changes until the reload: Go plans from the PUBLISHED copy.
    assert_eq!(plan(&mut session, "SELECT * FROM t WHERE b = 'a'"), before);

    session.run("ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST").unwrap();
    let after = plan(&mut session, "SELECT * FROM t WHERE b = 'a'");
    assert!(
        after.contains("TableFullScan") && !after.contains("IndexRangeScan"),
        "a blacklisted enum leaves no index path:\n{after}"
    );
    assert!(
        after.contains("eq(test.t.b, \"a\")"),
        "and the condition still filters:\n{after}"
    );
    assert_eq!(
        row_text(session.run("SELECT * FROM t WHERE b = 'a'")),
        vec![vec!["a", "a", "1"]]
    );

    // And it comes back.
    session.run("DELETE FROM mysql.expr_pushdown_blacklist").unwrap();
    session.run("ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST").unwrap();
    assert_eq!(plan(&mut session, "SELECT * FROM t WHERE b = 'a'"), before);
}

/// An operator may name a function by the spelling they know.
///
/// Go's `funcName2Alias` rewrites `<` to `lt` while loading, so the two rows
/// the corpus writes -- `('<', ...)` and `('lt', ...)` -- blacklist the same
/// function and produce the same plan.
///
/// Both carry the corpus's own `'tikv,tiflash,tidb'`, and that is not
/// decoration: `IsPushDownEnabled(name, kv.UnSpecified)` masks against ALL
/// THREE store bits and refuses only when every one of them is set, so a row
/// naming `tikv` alone leaves the `kv.UnSpecified` question -- the one
/// `DataSource.PredicatePushDown` asks -- answered yes.
#[test]
fn the_operator_spelling_and_the_function_name_blacklist_the_same_function() {
    for name in ["<", "lt"] {
        let mut session = Session::new();
        session.run("CREATE TABLE t(a int, key ia(a))").unwrap();
        session.run("INSERT INTO t VALUES (1),(2),(3)").unwrap();

        let before = plan(&mut session, "SELECT * FROM t WHERE a < 3");
        assert!(
            before.contains("IndexRangeScan"),
            "`a < 3` ranges over `ia` to begin with:\n{before}"
        );

        session
            .run(&format!(
                "INSERT INTO mysql.expr_pushdown_blacklist VALUES \
                 ('{name}','tikv,tiflash,tidb','for test')"
            ))
            .unwrap();
        session.run("ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST").unwrap();

        let after = plan(&mut session, "SELECT * FROM t WHERE a < 3");
        assert!(
            !after.contains("IndexRangeScan"),
            "blacklisting `{name}` must remove the range:\n{after}"
        );
        assert_eq!(
            row_text(session.run("SELECT * FROM t WHERE a < 3")),
            vec![vec!["1"], vec!["2"]],
            "and `{name}` must not change the answer"
        );
    }
}

/// `mysql.opt_rule_blacklist` switches a logical rule off by its own
/// `Name()`.
///
/// Go's `isLogicalRuleDisabled` skips the rule entirely, so the `DataSource`
/// is handed no predicate at all and every path is a full scan --- while the
/// `WHERE` still filters from above.
#[test]
fn blacklisting_predicate_push_down_leaves_every_path_a_full_scan() {
    let mut session = Session::new();
    session.run("CREATE TABLE t(a int, key ia(a))").unwrap();
    session.run("INSERT INTO t VALUES (1),(2),(3)").unwrap();

    assert!(plan(&mut session, "SELECT * FROM t WHERE a < 3").contains("IndexRangeScan"));

    session
        .run("INSERT INTO mysql.opt_rule_blacklist VALUES ('predicate_push_down')")
        .unwrap();
    session.run("ADMIN RELOAD OPT_RULE_BLACKLIST").unwrap();

    let after = plan(&mut session, "SELECT * FROM t WHERE a < 3");
    assert!(
        !after.contains("IndexRangeScan"),
        "the rule is off, so no predicate reaches the ranger:\n{after}"
    );
    assert_eq!(
        row_text(session.run("SELECT * FROM t WHERE a < 3")),
        vec![vec!["1"], vec!["2"]]
    );

    session
        .run("DELETE FROM mysql.opt_rule_blacklist WHERE name='predicate_push_down'")
        .unwrap();
    session.run("ADMIN RELOAD OPT_RULE_BLACKLIST").unwrap();
    assert!(plan(&mut session, "SELECT * FROM t WHERE a < 3").contains("IndexRangeScan"));
}
