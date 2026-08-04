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

//! WHICH JOIN ALGORITHM, and WHETHER ITS SIDES KEEP ORDER -- compared against
//! TiDB's own recordings.
//!
//! # Why this file exists at all
//!
//! `integration_diff` compares an `EXPLAIN` by its ACCESS property
//! (`integration_plan_property::access_property`): for each data-reading LEAF,
//! its operator, its access object, its range and whether the estimate was
//! pseudo. Three things are deliberately outside that property, and all three
//! are exactly what a join decision is made of:
//!
//!  * the JOIN OPERATORS are not compared at all -- `access_property` keeps
//!    only rows whose name contains `Scan`, `Point_Get` or `Point Get`;
//!  * `keep order:` is not compared -- the property text is
//!    `"{operator} {access object}"` plus `range:` plus `stats:pseudo`, and
//!    `keep order` appears in none of them;
//!  * a leaf is credited PER TABLE against whatever the recording read for
//!    that table, so a `TableFullScan` this tier prints for its own reasons
//!    matches a `TableFullScan` TiDB printed because a merge join demanded
//!    its order.
//!
//! That last point was measured, not assumed: 381 recorded `TableFullScan`
//! leaves sit under Merge/Index join trees and are credited by
//! `access_property` today. So renaming a `HashJoin` to `MergeJoin` and
//! flipping every `keep order` in this tier moves the replay's numbers by
//! ZERO. The claim "this tier now merges where TiDB merges" therefore had no
//! instrument behind it -- only hand-written pinning tests naming a handful of
//! queries.
//!
//! This file is that instrument. It replays the SAME enrolled topics
//! (`enrolled_topics::TOPICS`, shared with `integration_diff` so the two
//! reports describe one corpus) and compares, for every `EXPLAIN` both sides
//! produce a plan for:
//!
//!  1. the MULTISET OF JOIN OPERATOR NAMES (`join_names`), and
//!  2. the KEEP-ORDER FLAG of every access leaf, keyed by table
//!     (`keep_order_by_table`), and
//!  3. the ORDERED-MERGE PAIRS (`ordered_merge_pairs`): the unordered pair of
//!     tables under a `MergeJoin` whose two sides each bottom out in exactly
//!     one access leaf and both leaves say `keep order:true`. This is the
//!     precise shape the merge-join increment produces, so it is the number
//!     the increment is answerable to.
//!
//! # What is NOT compared, and why
//!
//! `Apply` is excluded from the join multiset. `tidb_executor::explain`'s
//! module docs state that this tier's recorder prints NO node for the Apply a
//! correlated subquery builds -- the executor exists, the row does not. So
//! every recorded `Apply` would be reported here as a missing join, which
//! would be a statement about the printer and not about a join decision. The
//! same reasoning is why `access_property` drops `TableRowIDScan`, and this
//! file drops it too: it is the second half of Go's double read, and its
//! `keep order` is a consequence of the index scan above it.
//!
//! Operator IDS are dropped (build-order here, plan-construction order in
//! Go), as are the `(Build)`/`(Probe)` labels: which side of a hash join is
//! built is not the decision under test, and the two trees mark it on
//! different rows because the recorded tree has a `TableReader` this tier
//! does not print.
//!
//! # The ratchet is two-sided on every number
//!
//! Compared, agreeing and recorded-shape counts are all pinned with
//! `assert_eq!`. A one-sided ratchet let a fake improvement through once
//! already (the divergence count fell while `compared` fell further), so
//! nothing here is allowed to move silently in EITHER direction. When a
//! number moves, the instruction is the same one `integration_diff` carries:
//! re-read the recorded witness in `tests/integrationtest/r/**` for the
//! statements that moved and state what changed, then update the number.
//! `r/*.result` is TiDB's oracle and is never edited.

#[path = "enrolled_topics.rs"]
mod enrolled_topics;
#[path = "integration_plan_property.rs"]
mod integration_plan_property;
#[path = "mysqltest_connections.rs"]
mod mysqltest_connections;
#[path = "mysqltest_script.rs"]
mod mysqltest_script;

use std::collections::BTreeMap;
use std::fs;

use enrolled_topics::TOPICS;
use integration_plan_property::{plan_statement, PlanStatement};
use mysqltest_connections::Connections;
use mysqltest_script::{align, parse_test, split_warnings, Item, Stmt};
use tidb_datatype::Datum;
use tidb_session::{Session, StmtOutput};

/// The join operators TiDB's `EXPLAIN` prints, minus `Apply`.
///
/// Go builds all of these in `exhaust_physical_plans.go`
/// (`getHashJoins`, `GetMergeJoin`, `getIndexJoinByOuterIdx` and the
/// `IndexHashJoin`/`IndexMergeJoin` variants `constructIndexJoin` wraps), and
/// each prints under its own name. `Apply` is left out for the reason in the
/// module docs: this tier prints no row for it, so counting it would measure
/// the printer.
const JOIN_OPERATORS: &[&str] = &[
    "HashJoin",
    "MergeJoin",
    "IndexJoin",
    "IndexHashJoin",
    "IndexMergeJoin",
];

/// Operator name substrings that mark a row as a data-reading leaf. Same set
/// `integration_plan_property::access_property` uses, for the same reason: a
/// property that reads different rows on the two sides is not a comparison.
const ACCESS_OPERATORS: &[&str] = &["Scan", "Point_Get", "Point Get"];

/// One row of a plan, already split into its tree depth and its parts.
#[derive(Debug, Clone)]
struct PlanRow {
    /// Nesting depth, read off the tree drawing: Go writes two display
    /// characters per level (`├─`, `└─`, `│ `, two spaces), and this tier's
    /// printer draws the same prefix (`tidb_executor::explain`).
    depth: usize,
    /// The operator name with its id, `(Build)`/`(Probe)` label and tree
    /// prefix stripped.
    operator: String,
    /// The access-object column (`table:t, index:i(a)`), or `""`.
    access: String,
    /// The operator-info column, which is where `keep order:` lives.
    info: String,
}

/// Splits one tab-separated plan line into depth, operator name and the two
/// text columns.
///
/// Only the FIRST and last two columns are read, exactly as
/// `access_property` does, because every text tree format shares those three
/// and differs only in the numeric columns between them.
fn plan_row(line: &str) -> Option<PlanRow> {
    let columns: Vec<&str> = line.split('\t').collect();
    if columns.len() < 3 {
        return None;
    }
    let id = columns[0];
    let prefix_chars = id
        .chars()
        .take_while(|c| matches!(c, '│' | '├' | '└' | '─' | ' '))
        .count();
    let name: String = id.chars().skip(prefix_chars).collect();
    Some(PlanRow {
        depth: prefix_chars / 2,
        operator: operator_name(&name).to_owned(),
        access: columns[columns.len() - 2].trim().to_owned(),
        info: columns[columns.len() - 1].to_owned(),
    })
}

/// Strips a plan id of its `_N` suffix and its `(Build)`/`(Probe)` qualifier.
fn operator_name(name: &str) -> &str {
    let name = name.split('(').next().unwrap_or(name);
    match name.rfind('_') {
        Some(at)
            if !name[at + 1..].is_empty() && name[at + 1..].bytes().all(|c| c.is_ascii_digit()) =>
        {
            &name[..at]
        }
        _ => name,
    }
}

/// Reads a whole plan's rows, dropping the header line and anything that is
/// not a tree row.
fn plan_rows(lines: &[String]) -> Vec<PlanRow> {
    lines.iter().filter_map(|line| plan_row(line)).collect()
}

/// Whether this row is a data-reading leaf whose access decision is real.
fn is_access(row: &PlanRow) -> bool {
    ACCESS_OPERATORS
        .iter()
        .any(|kind| row.operator.contains(kind))
        && row.operator != "TableRowIDScan"
}

/// The table an access row reads.
fn access_table(row: &PlanRow) -> String {
    row.access
        .split(", ")
        .find_map(|part| part.strip_prefix("table:"))
        .unwrap_or("")
        .to_owned()
}

/// Whether an access row's operator info claims the scan preserves the
/// index's or the handle's order.
fn keeps_order(row: &PlanRow) -> bool {
    row.info.contains("keep order:true")
}

/// The multiset of join operator names in a plan, sorted so two plans that
/// built the same joins in a different order still compare equal.
///
/// Sorted rather than positional on purpose: this tier's tree has no
/// `TableReader` and prints its children in build order, so a positional
/// comparison would report the SHAPE divergence `explain`'s module docs
/// already name, not a join decision.
fn join_names(rows: &[PlanRow]) -> Vec<String> {
    let mut names: Vec<String> = rows
        .iter()
        .filter(|row| JOIN_OPERATORS.contains(&row.operator.as_str()))
        .map(|row| row.operator.clone())
        .collect();
    names.sort();
    names
}

/// Every access leaf's keep-order flag, keyed by table.
///
/// Keyed by table for `access_property`'s reason: this tier prints no node for
/// several operators it really runs, so a recorded plan routinely carries
/// leaves for tables this side never printed. Per table, the question is the
/// one a merge join asks -- was THIS table read in order.
fn keep_order_by_table(rows: &[PlanRow]) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows.iter().filter(|row| is_access(row)) {
        out.entry(access_table(row))
            .or_default()
            .push(format!("keep order:{}", keeps_order(row)));
    }
    for flags in out.values_mut() {
        flags.sort();
    }
    out
}

/// The index range of `rows[at]`'s whole subtree, `at` included.
fn subtree(rows: &[PlanRow], at: usize) -> std::ops::Range<usize> {
    let depth = rows[at].depth;
    let mut end = at + 1;
    while end < rows.len() && rows[end].depth > depth {
        end += 1;
    }
    at..end
}

/// The direct children of `rows[at]`.
fn children(rows: &[PlanRow], at: usize) -> Vec<usize> {
    let span = subtree(rows, at);
    let depth = rows[at].depth;
    span.filter(|i| rows[*i].depth == depth + 1).collect()
}

/// The access leaves inside a subtree.
fn access_leaves(rows: &[PlanRow], at: usize) -> Vec<usize> {
    subtree(rows, at).filter(|i| is_access(&rows[*i])).collect()
}

/// The pairs of tables joined by a `MergeJoin` whose two sides each bottom out
/// in exactly ONE access leaf, with BOTH leaves keeping order.
///
/// This is the shape the merge-join increment produces and the shape the
/// recordings hold 101 of across the enrolled topics:
///
/// ```text
/// MergeJoin_9      root  inner join, left key:test.t2.a, right key:test.t3.a
/// ├─TableReader(Build)   root  data:TableFullScan
/// │ └─TableFullScan      cop[tikv]  table:t3  keep order:true, stats:pseudo
/// └─TableReader(Probe)   root  data:TableFullScan
///   └─TableFullScan      cop[tikv]  table:t2  keep order:true, stats:pseudo
/// ```
///
/// The `TableReader` wrapper is why the leaves are looked for in the SUBTREE
/// rather than as direct children: this tier prints the scan directly under
/// the join, TiDB prints it two levels down, and that difference is
/// `explain`'s named divergence 1 rather than a different join.
///
/// The pair is UNORDERED (sorted) because which side is the build side is not
/// under test here, and the two trees label it on different rows.
///
/// A multiset, not a set: a statement whose plan merges the same two tables
/// twice really did make the decision twice.
fn ordered_merge_pairs(rows: &[PlanRow]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for at in 0..rows.len() {
        if rows[at].operator != "MergeJoin" {
            continue;
        }
        let sides = children(rows, at);
        if sides.len() != 2 {
            continue;
        }
        let leaves: Vec<Vec<usize>> = sides.iter().map(|s| access_leaves(rows, *s)).collect();
        if leaves.iter().any(|l| l.len() != 1) {
            continue;
        }
        let leaves: Vec<&PlanRow> = leaves.iter().map(|l| &rows[l[0]]).collect();
        if !leaves.iter().all(|leaf| keeps_order(leaf)) {
            continue;
        }
        let mut pair = [access_table(leaves[0]), access_table(leaves[1])];
        pair.sort();
        let [left, right] = pair;
        out.push((left, right));
    }
    out.sort();
    out
}

/// The whole comparable join property of one plan.
#[derive(Debug, PartialEq, Eq)]
struct JoinShape {
    joins: Vec<String>,
    keep_order: BTreeMap<String, Vec<String>>,
    merge_pairs: Vec<(String, String)>,
}

fn join_shape(lines: &[String]) -> JoinShape {
    let rows = plan_rows(lines);
    JoinShape {
        joins: join_names(&rows),
        keep_order: keep_order_by_table(&rows),
        merge_pairs: ordered_merge_pairs(&rows),
    }
}

/// One topic's join-shape outcome.
#[derive(Default)]
struct Report {
    /// Plans where both sides produced a tree and at least one side named a
    /// join operator.
    compared: usize,
    /// Of those, plans whose join multiset agrees.
    joins_agree: usize,
    /// Of those, plans whose per-table keep-order flags agree for every table
    /// THIS side read.
    keep_order_agrees: usize,
    /// Of those, plans where both agree.
    both_agree: usize,
    /// Recorded ordered-merge pairs, over compared plans.
    recorded_merge_pairs: usize,
    /// Recorded ordered-merge pairs this tier also produces.
    agreed_merge_pairs: usize,
    /// Ordered-merge pairs this tier produces that the recording does not.
    extra_merge_pairs: usize,
    /// Plans neither side named a join in.
    joinless: usize,
    /// `EXPLAIN`s that did not reach a comparison: refused here, recorded as
    /// an error, rewritten by the recorder, or a non-text format.
    unreachable: usize,
    disagreements: Vec<String>,
    /// Statements whose ordered-merge pairs differ in EITHER direction: the
    /// missing ones are the shape not reached, the extra ones are this tier
    /// merging where TiDB does not.
    merge_pair_differences: Vec<String>,
}

impl Report {
    fn absorb(&mut self, other: Report) {
        self.compared += other.compared;
        self.joins_agree += other.joins_agree;
        self.keep_order_agrees += other.keep_order_agrees;
        self.both_agree += other.both_agree;
        self.recorded_merge_pairs += other.recorded_merge_pairs;
        self.agreed_merge_pairs += other.agreed_merge_pairs;
        self.extra_merge_pairs += other.extra_merge_pairs;
        self.joinless += other.joinless;
        self.unreachable += other.unreachable;
        self.disagreements.extend(other.disagreements);
        self.merge_pair_differences
            .extend(other.merge_pair_differences);
    }
}

/// Renders a result cell the way the recorder writes it.
fn cell(value: &Datum) -> String {
    if value.is_null() {
        return "NULL".to_owned();
    }
    value.sql_string().unwrap_or_else(|_| value.label())
}

/// How many of `want`'s entries `got` also holds, counting multiplicity.
fn intersection_size<T: Ord + Clone>(want: &[T], got: &[T]) -> usize {
    let mut pool: BTreeMap<T, usize> = BTreeMap::new();
    for item in got {
        *pool.entry(item.clone()).or_default() += 1;
    }
    want.iter()
        .filter(|item| match pool.get_mut(*item) {
            Some(count) if *count > 0 => {
                *count -= 1;
                true
            }
            _ => false,
        })
        .count()
}

/// Replays one topic and compares the join shape of every `EXPLAIN` in it.
///
/// Every statement is RUN, whether or not it is compared: the state a later
/// statement reads is the state the writes before it left, and skipping a run
/// silently rewinds the session (`integration_diff::compare_output` carries
/// the same rule and the bug that taught it).
fn run_topic(topic: &str) -> Result<Report, String> {
    let topic = topic.to_owned();
    difftest::on_deep_stack(move || run_topic_on_this_stack(&topic))
}

fn run_topic_on_this_stack(topic: &str) -> Result<Report, String> {
    let dir = difftest::parser_oracle::repo_root().join("tests/integrationtest");
    let script = fs::read_to_string(dir.join(format!("t/{topic}.test")))
        .map_err(|e| format!("read t/{topic}.test: {e}"))?;
    let recorded = fs::read_to_string(dir.join(format!("r/{topic}.result")))
        .map_err(|e| format!("read r/{topic}.result: {e}"))?;
    let items = parse_test(&script)?;
    let aligned = align(&items, &recorded)?;

    let mut report = Report::default();
    let mut connections = Connections::open(topic)?;
    for (item, block) in aligned {
        let stmt = match item {
            Item::Stmt(stmt) => stmt,
            Item::Connection(cmd) => {
                connections.apply(cmd)?;
                continue;
            }
            Item::Echo(_) => continue,
        };
        compare_statement(connections.current(), topic, stmt, &block, &mut report);
    }
    Ok(report)
}

fn compare_statement(
    session: &mut Session,
    topic: &str,
    stmt: &Stmt,
    block: &[String],
    report: &mut Report,
) {
    let recorded = if stmt.warnings {
        split_warnings(block).0
    } else {
        block
    };
    // A plan statement runs as this tier's own default `EXPLAIN` whatever
    // format the recording asked for, exactly as the replay does.
    let plan = plan_statement(&stmt.sql);
    let sql = match (&plan, stmt.blocker) {
        // Not an `EXPLAIN` at all, or one whose recording was rewritten: run
        // it for its effect and compare nothing.
        (Some(PlanStatement::RunDefaultExplain(sql)), None) => sql.clone(),
        (_, _) => {
            drop(session.run_with_columns(&stmt.sql));
            if matches!(plan, Some(PlanStatement::RunDefaultExplain(_))) {
                report.unreachable += 1;
            }
            return;
        }
    };
    if stmt.expect_error || recorded.first().is_some_and(|l| l.starts_with("Error ")) {
        report.unreachable += 1;
        return;
    }
    let Ok(StmtOutput::Rows { rows, .. }) = session.run_with_columns(&sql) else {
        report.unreachable += 1;
        return;
    };
    let ours: Vec<String> = rows
        .iter()
        .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t"))
        .collect();
    // Drop the header on the recorded side; ours carries none.
    let theirs = &recorded[1.min(recorded.len())..];
    let got = join_shape(&ours);
    let want = join_shape(theirs);

    report.recorded_merge_pairs += want.merge_pairs.len();
    let agreed = intersection_size(&want.merge_pairs, &got.merge_pairs);
    report.agreed_merge_pairs += agreed;
    report.extra_merge_pairs += got.merge_pairs.len() - agreed;
    if want.merge_pairs != got.merge_pairs {
        report.merge_pair_differences.push(format!(
            "\n--- [{topic}] {}\n  tidb ordered merges: {:?}\n  rust ordered merges: {:?}",
            stmt.sql, want.merge_pairs, got.merge_pairs
        ));
    }

    if want.joins.is_empty() && got.joins.is_empty() {
        report.joinless += 1;
        return;
    }
    report.compared += 1;
    let joins_agree = want.joins == got.joins;
    // Only the tables THIS side read are asserted, per `keep_order_by_table`'s
    // own contract.
    let keep_order_agrees = got
        .keep_order
        .iter()
        .all(|(table, flags)| want.keep_order.get(table) == Some(flags));
    report.joins_agree += usize::from(joins_agree);
    report.keep_order_agrees += usize::from(keep_order_agrees);
    if joins_agree && keep_order_agrees {
        report.both_agree += 1;
        return;
    }
    report.disagreements.push(format!(
        "\n--- [{topic}] {}\n  tidb joins: {:?} keep order: {:?}\n  rust joins: {:?} keep order: {:?}",
        stmt.sql, want.joins, want.keep_order, got.joins, got.keep_order
    ));
}

/// The gate.
///
/// Numbers move only for a reason that can be named from
/// `tests/integrationtest/r/**`, so every one of them is pinned in BOTH
/// directions. If a change moves one:
///
///  1. run with `JOIN_SHAPE_SHOW_DISAGREEMENTS=1` and read the statements that
///     moved;
///  2. open the recorded witness for each in `tests/integrationtest/r/**` --
///     TiDB's own output, never edited here -- and state what TiDB records and
///     what this tier now says;
///  3. write that finding into the comment below and set the number.
///
/// A number that rises without step 2 is a claim without a witness, which is
/// the failure mode this file was built to close.
#[test]
fn join_operators_and_their_keep_order_match_recorded_tidb_plans() {
    let mut total = Report::default();
    let mut per_topic = Vec::new();
    for (topic, _why) in TOPICS {
        let report = run_topic(topic).unwrap_or_else(|e| panic!("topic {topic}: {e}"));
        if report.compared > 0 || report.recorded_merge_pairs > 0 {
            per_topic.push(format!(
                "{topic}: {} of {} join plans agree, ordered-merge pairs {}/{} (+{} extra)",
                report.both_agree,
                report.compared,
                report.agreed_merge_pairs,
                report.recorded_merge_pairs,
                report.extra_merge_pairs,
            ));
        }
        total.absorb(report);
    }
    eprintln!(
        "join shape over {} topics: {} of {} join plans agree on BOTH \
         (joins alone {}, keep order alone {}); {} joinless plans, {} \
         unreachable EXPLAINs\nordered-merge pairs: {} of {} recorded \
         reproduced, {} produced here that TiDB does not record\n  {}",
        TOPICS.len(),
        total.both_agree,
        total.compared,
        total.joins_agree,
        total.keep_order_agrees,
        total.joinless,
        total.unreachable,
        total.agreed_merge_pairs,
        total.recorded_merge_pairs,
        total.extra_merge_pairs,
        per_topic.join("\n  "),
    );
    if std::env::var_os("JOIN_SHAPE_SHOW_DISAGREEMENTS").is_some() {
        eprintln!("disagreements:{}", total.disagreements.join(""));
        eprintln!(
            "ordered-merge differences:{}",
            total.merge_pair_differences.join("")
        );
    }

    // THE FIRST MEASUREMENT, and its classification. Every number below was
    // read off the recordings in `tests/integrationtest/r/**`, and the
    // classes are the whole of the 137 disagreeing plans and the 37
    // statements whose ordered-merge pairs differ.
    //
    // THE HEADLINE. The enrolled recordings hold 227 `MergeJoin` nodes; 101 of
    // them are the exact shape the merge-join increment builds -- two sides
    // that each bottom out in ONE access leaf, both `keep order:true`. 84 of
    // those 101 sit in `EXPLAIN`s this replay reaches (the other 17 are inside
    // statements this tier refuses, records an error for, or whose recording
    // the recorder rewrote). Of the 84, THIS TIER REPRODUCES 63. That is the
    // number the stage-1 merge-join claim was missing: it rested on 6 pinning
    // tests, and the replay could not see it at all.
    //
    // THE 30 EXTRAS ARE MOSTLY JOIN ORDER, NOT OVER-MERGING. Of the 37
    // statements whose pairs differ, the largest classes are a merge in a
    // DIFFERENT POSITION: TiDB merges `(t1, t2)` at the bottom where this tier
    // merges `(t2, t3)` (7 statements), or TiDB's bottom join has a deeper
    // subtree on one side while this tier's is two bare scans (11). Only the
    // `explain_easy` pair `(s, t1)` is a merge TiDB records that this tier
    // replaces with no join at all. So the merge DECISION is largely right and
    // the ORDER the joins are built in is not -- a separate gap from the one
    // stage 1 closed.
    //
    // WHY ONLY 45 OF 182 PLANS AGREE OUTRIGHT. Three named causes, in size
    // order, and none of them is the bottom-level merge above:
    //
    //  * 44 plans PRINT NO JOIN NODE HERE AT ALL. `naaj`'s
    //    `(a, b) not in (select ...)` is a null-aware anti semi join in TiDB's
    //    plan (34 plans), and `explain_easy`'s scalar and `IN` subqueries
    //    another 10. This tier evaluates the subquery without recording a join
    //    row -- the same class `tidb_executor::explain` names for `Apply`, and
    //    the reason `Apply` itself is excluded from the multiset. These are
    //    NOT excluded: an `Apply` this tier never prints is a documented
    //    constant, while a `HashJoin` it does not print is a gap that should
    //    become visible when the recorder learns the shape.
    //  * 69 plans DO NOT PROPAGATE ORDER ABOVE THE FIRST JOIN. TiDB's
    //    `MergeJoin` output is sorted on its keys, so a second join on the same
    //    key merges too (`["MergeJoin", "MergeJoin"]` where this tier says
    //    `["HashJoin", "MergeJoin"]`, 31 plans, plus the three-way and
    //    four-way forms). This tier derives order only from a SCAN, so the
    //    order stops at the first join. This is the largest single gap the
    //    instrument found and it is neither half of the covering-index /
    //    index-join increment. 6 of the 69 also have an `IndexHashJoin` at the
    //    top where this tier hashes.
    //  * 12 plans IGNORE A `TIDB_SMJ` HINT. `executor/merge_join` merges under
    //    an explicit hint with a `Sort` on each side; this tier hashes.
    //
    // The remaining 12: 6 where this tier MERGES and TiDB hashes (all under
    // `leading()` hints in `join_reorder2` -- a different join ORDER, so a
    // different pair is the one with two ordered sides), 2 where TiDB reaches
    // an `IndexJoin`/`IndexHashJoin` and this tier hashes with no other
    // difference (the probe-side gap, and it is 2, not the 8 the divergence
    // list suggested), 2 where TiDB ELIMINATED an outer join entirely and this
    // tier still joins, 1 that differs only in `keep order`, and 1 where this
    // tier builds one join where TiDB builds two.
    // SECOND MEASUREMENT, after order propagation above the first join
    // (`tidb_executor::driver::merge_decision`). 45 -> 99 plans agree on
    // BOTH, and the 69-plan class named above is the one that moved.
    //
    // WHAT THE RECORDINGS SAY ABOUT THE 54. Every one is in
    // `planner/core/join_reorder_through_projection`, and every one has a
    // DERIVED TABLE over a join on one side --
    // `r/planner/core/join_reorder_through_projection.result` records e.g.
    // `select t1.a, dt.doubled_b, dt.shifted_b from t1, (select t2.a as a2,
    // t2.b * 2 as doubled_b, t3.b + 100 as shifted_b from t2 join t3 on t2.a
    // = t3.a) dt where t1.a = dt.a2` as `MergeJoin` over `MergeJoin` with
    // `keep order:true` on t1, t2 AND t3. This tier now reaches the same
    // three: the derived table reports the inner merge's key order through
    // its projection, and the `WHERE` equality is a join key exactly as
    // `LogicalJoin.PredicatePushDown` makes it one.
    //
    // THE ORDERED-MERGE PAIRS MOVED 63 -> 66 AND 30 -> 31, with three
    // witnesses, all in the same recording:
    //
    //  * `select t4.a, tt.plus from t4, (select t.a + t.b as plus, t3.a as
    //    t3_a from (select t1.a as a, t2.b as b from t1, t2 where t1.a =
    //    t2.a) t join t3 on t.a = t3.a) tt` -- recorded TWICE (the suite runs
    //    each case with `tidb_opt_join_reorder_through_proj` off and on) and
    //    recorded BOTH times as an ordered merge of `(t1, t2)`. This tier
    //    produced none before and produces `(t1, t2)` now: +2 agreed.
    //  * `select t1.a, v.* from t1, (select t2.a as va, t2.b * 2 as vb, t3.b
    //    as vb2 from t2, t3 where t2.a = t3.a) v where t1.a = v.va` -- also
    //    recorded twice. One recording is an ordered merge of `(t2, t3)`,
    //    which this tier now reproduces: +1 agreed. The other records
    //    `(t1, t2)` -- TiDB REORDERED the three tables and merged a pair this
    //    tier never forms -- while this tier merges `(t2, t3)` in the
    //    position the statement wrote: +1 extra. That extra is the join-ORDER
    //    gap the first measurement already named, now visible on one more
    //    statement rather than a new kind of disagreement.
    //
    // NOTHING REGRESSED: the set of disagreeing statements after this change
    // is a strict SUBSET of the set before it (0 newly disagreeing), and the
    // replay is unchanged at 5639 compared / `PlanProperty` 806.
    // THIRD MEASUREMENT, after the DP join reorder
    // (`tidb_executor::driver::join_reorder`). 99 -> 101 -> 103 plans agree on
    // BOTH, all four in `planner/core/join_reorder_through_projection`
    // (62 -> 66 of 82), and every one of them is a statement that topic runs
    // at `set tidb_opt_join_reorder_threshold = 10`.
    //
    // WHAT THE RECORDINGS SAY. Each of the four is
    // `from t1, t5, (select ... from t2 join t3 on t2.a = t3.a) dt where
    // t1.a = dt.key_a and dt.key_a = t5.a [and dt.doubled_b > 100]`, whose
    // WRITTEN tree joins `t1` to `t5` first -- a pair with no equality between
    // them, so a cartesian product this tier hashed. `r/planner/core/
    // join_reorder_through_projection.result:1249` and `:1399` record TiDB
    // building `(t1 join dt) join t5` instead, three `MergeJoin`s deep with
    // `keep order:true` on t1, t2, t3 and t5. This tier now builds the same
    // tree, so the same four leaves keep order and the same three joins merge.
    //
    // THE MERGE PAIRS DID NOT MOVE (66/84 agreed, 31 extra, unchanged). The
    // reordered plans' merges are all over MULTI-leaf subtrees, which
    // `ordered_merge_pairs` does not count, and their one single-leaf pair
    // `(t2, t3)` was already produced before. The 27 extras this topic owns
    // are the `tidb_opt_join_reorder_through_proj = on` recordings, where TiDB
    // INLINES the derived table's projection into the join group and reaches a
    // `(t1, t2)` pair no un-inlined tree can form; projection inlining is not
    // part of this increment.
    //
    // NOTHING REGRESSED: the disagreeing set is a strict subset of the one
    // before (0 newly disagreeing), the replay is unchanged at 5639 compared,
    // and `executor/merge_join` (246 matched, 0 diverged) and
    // `executor/jointest/join` (801 matched, 3 diverged) did not move -- as
    // they cannot, since neither topic raises the threshold.
    const COMPARED: usize = 182;
    const BOTH_AGREE: usize = 103;
    const RECORDED_MERGE_PAIRS: usize = 84;
    const AGREED_MERGE_PAIRS: usize = 66;
    const EXTRA_MERGE_PAIRS: usize = 31;

    assert_eq!(
        (
            total.compared,
            total.both_agree,
            total.recorded_merge_pairs,
            total.agreed_merge_pairs,
            total.extra_merge_pairs
        ),
        (
            COMPARED,
            BOTH_AGREE,
            RECORDED_MERGE_PAIRS,
            AGREED_MERGE_PAIRS,
            EXTRA_MERGE_PAIRS
        ),
        "the join-shape numbers moved (compared, both agree, recorded merge \
         pairs, agreed merge pairs, extra merge pairs). Run with \
         JOIN_SHAPE_SHOW_DISAGREEMENTS=1, open the recorded witness in \
         tests/integrationtest/r/** for each statement that moved, state what \
         TiDB records there, and only then update these numbers. A FALLING \
         `compared` is not an improvement: it means plans stopped being \
         comparable."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(text: &str) -> Vec<String> {
        text.lines().map(str::to_owned).collect()
    }

    /// The recorded shape, verbatim from
    /// `r/planner/core/join_reorder_through_projection.result`, and this
    /// tier's shape for the same join. They differ by the `TableReader`
    /// wrapper and the ids, and by nothing this file compares.
    #[test]
    fn the_recorded_merge_and_this_tiers_merge_have_the_same_join_shape() {
        let tidb = lines(
            "MergeJoin_9\t12500.00\troot\t\tinner join, left key:test.t2.a, right key:test.t3.a\n\
             ├─TableReader_11(Build)\t10000.00\troot\t\tdata:TableFullScan_10\n\
             │ └─TableFullScan_10\t10000.00\tcop[tikv]\ttable:t3\tkeep order:true, stats:pseudo\n\
             └─TableReader_13(Probe)\t10000.00\troot\t\tdata:TableFullScan_12\n\
             \x20 └─TableFullScan_12\t10000.00\tcop[tikv]\ttable:t2\tkeep order:true, stats:pseudo",
        );
        let ours = lines(
            "MergeJoin_3\tN/A\troot\t\tinner join, left key:test.t2.a, right key:test.t3.a\n\
             ├─TableFullScan_1(Build)\t10000.00\troot\ttable:t3\tkeep order:true, stats:pseudo\n\
             └─TableFullScan_2(Probe)\t10000.00\troot\ttable:t2\tkeep order:true, stats:pseudo",
        );
        assert_eq!(join_shape(&tidb), join_shape(&ours));
        assert_eq!(
            join_shape(&tidb).merge_pairs,
            vec![("t2".to_owned(), "t3".to_owned())]
        );
    }

    /// A hash join where TiDB merges is exactly what this file exists to see,
    /// and `access_property` cannot: the two plans read the same object over
    /// the same range and differ only in the join and the order flags.
    #[test]
    fn a_hash_join_where_tidb_merges_is_a_disagreement() {
        let tidb = lines(
            "MergeJoin_9\t12500.00\troot\t\tinner join, left key:test.t2.a, right key:test.t3.a\n\
             ├─TableReader_11(Build)\t10000.00\troot\t\tdata:TableFullScan_10\n\
             │ └─TableFullScan_10\t10000.00\tcop[tikv]\ttable:t3\tkeep order:true, stats:pseudo\n\
             └─TableReader_13(Probe)\t10000.00\troot\t\tdata:TableFullScan_12\n\
             \x20 └─TableFullScan_12\t10000.00\tcop[tikv]\ttable:t2\tkeep order:true, stats:pseudo",
        );
        let ours = lines(
            "HashJoin_3\tN/A\troot\t\tinner join, equal:[eq(test.t2.a, test.t3.a)]\n\
             ├─TableFullScan_1(Build)\t10000.00\troot\ttable:t3\tkeep order:false, stats:pseudo\n\
             └─TableFullScan_2(Probe)\t10000.00\troot\ttable:t2\tkeep order:false, stats:pseudo",
        );
        assert_ne!(join_shape(&tidb).joins, join_shape(&ours).joins);
        assert_ne!(
            join_shape(&tidb).keep_order,
            join_shape(&ours).keep_order,
            "the keep-order flags are the other half of the same decision"
        );
        assert!(join_shape(&ours).merge_pairs.is_empty());
    }

    /// `access_property` credits the two plans above as EQUAL, which is the
    /// hole this file fills. Pinned so that a later change to that comparator
    /// which closes the hole is visible here rather than silently making this
    /// file redundant.
    #[test]
    fn the_access_property_comparator_cannot_tell_those_two_apart() {
        use integration_plan_property::access_property;
        let tidb = lines(
            "MergeJoin_9\t12500.00\troot\t\tinner join, left key:test.t2.a, right key:test.t3.a\n\
             ├─TableReader_11(Build)\t10000.00\troot\t\tdata:TableFullScan_10\n\
             │ └─TableFullScan_10\t10000.00\tcop[tikv]\ttable:t3\tkeep order:true, stats:pseudo\n\
             └─TableReader_13(Probe)\t10000.00\troot\t\tdata:TableFullScan_12\n\
             \x20 └─TableFullScan_12\t10000.00\tcop[tikv]\ttable:t2\tkeep order:true, stats:pseudo",
        );
        let ours = lines(
            "HashJoin_3\tN/A\troot\t\tinner join, equal:[eq(test.t2.a, test.t3.a)]\n\
             ├─TableFullScan_1(Build)\t10000.00\troot\ttable:t3\tkeep order:false, stats:pseudo\n\
             └─TableFullScan_2(Probe)\t10000.00\troot\ttable:t2\tkeep order:false, stats:pseudo",
        );
        assert_eq!(access_property(&tidb[1..]), access_property(&ours[1..]));
    }

    /// A `MergeJoin` whose side is a `Sort` is NOT the increment's shape: the
    /// order was enforced above the scan, not provided by it.
    #[test]
    fn a_merge_over_sorts_is_not_an_ordered_merge_pair() {
        let tidb = lines(
            "MergeJoin_9\t12500.00\troot\t\tinner join, left key:test.t1.c1, right key:test.t2.c1\n\
             ├─Sort_11(Build)\t10000.00\troot\t\ttest.t2.c1\n\
             │ └─TableReader_10\t10000.00\troot\t\tdata:TableFullScan_9\n\
             │   └─TableFullScan_9\t10000.00\tcop[tikv]\ttable:t2\tkeep order:false, stats:pseudo\n\
             └─Sort_13(Probe)\t10000.00\troot\t\ttest.t1.c1\n\
             \x20 └─TableReader_12\t10000.00\troot\t\tdata:TableFullScan_11\n\
             \x20   └─TableFullScan_11\t10000.00\tcop[tikv]\ttable:t1\tkeep order:false, stats:pseudo",
        );
        let shape = join_shape(&tidb);
        assert_eq!(shape.joins, vec!["MergeJoin".to_owned()]);
        assert!(
            shape.merge_pairs.is_empty(),
            "both scans say keep order:false, so no side PROVIDED the order"
        );
    }

    /// An `Apply` is not counted: this tier prints no row for it
    /// (`tidb_executor::explain`), so counting it would measure the printer.
    #[test]
    fn an_apply_is_not_a_join_for_this_comparison() {
        let tidb = lines(
            "Apply_11\t10000.00\troot\t\tCARTESIAN semi join\n\
             ├─TableReader_13(Build)\t10000.00\troot\t\tdata:TableFullScan_12\n\
             │ └─TableFullScan_12\t10000.00\tcop[tikv]\ttable:t1\tkeep order:false, stats:pseudo\n\
             └─TableReader_15(Probe)\t10.00\troot\t\tdata:TableFullScan_14\n\
             \x20 └─TableFullScan_14\t10.00\tcop[tikv]\ttable:t2\tkeep order:false, stats:pseudo",
        );
        assert!(join_shape(&tidb).joins.is_empty());
    }

    /// A `TableRowIDScan`'s `keep order` is a consequence of the index scan
    /// above it, not a decision, and this tier has no such operator.
    #[test]
    fn a_row_id_scan_contributes_no_keep_order_flag() {
        let tidb = lines(
            "IndexLookUp_10\t10.00\troot\t\t\n\
             ├─IndexRangeScan_8(Build)\t10.00\tcop[tikv]\ttable:t, index:i(a)\trange:[1,1], keep order:true\n\
             └─TableRowIDScan_9(Probe)\t10.00\tcop[tikv]\ttable:t\tkeep order:false",
        );
        assert_eq!(
            join_shape(&tidb).keep_order.get("t").map(Vec::as_slice),
            Some(["keep order:true".to_owned()].as_slice())
        );
    }

    /// Depth is read in CHARACTERS, not bytes: `│` is three bytes, and a byte
    /// count would put every row under a branch line at the wrong level.
    #[test]
    fn tree_depth_counts_characters_not_bytes() {
        let row = plan_row("│ └─TableFullScan_10\t1.00\tcop[tikv]\ttable:t\tkeep order:true")
            .expect("a plan row");
        assert_eq!(row.depth, 2);
        assert_eq!(row.operator, "TableFullScan");
    }

    #[test]
    fn intersection_counts_multiplicity() {
        assert_eq!(intersection_size(&[1, 1, 2], &[1, 2, 3]), 2);
        assert_eq!(intersection_size(&[1, 1], &[1, 1, 1]), 2);
        assert_eq!(intersection_size::<u8>(&[], &[1]), 0);
    }
}
