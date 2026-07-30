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

//! The result ring pointed at TIDB'S OWN record/replay suite.
//!
//! Every other differential ring here runs a corpus WE wrote and captured
//! through `gorun`. That corpus is 1,668 statements someone thought to write,
//! and it is exhausted as a work source. `tests/integrationtest` is a
//! different kind of oracle: 257 topics, 30,296 statements, and 148,368 lines
//! of output recorded from real TiDB by the suite the TiDB project itself
//! gates on. This driver replays a topic's `t/<topic>.test` script through
//! [`tidb_session::Session`] -- the same parse -> plan -> execute path every
//! in-process caller uses -- and compares against `r/<topic>.result`.
//!
//! `r/*.result` is TiDB's oracle, not ours: this test only ever reads it, and
//! a divergence is carried as a ratchet number to be worked off, never
//! written into the recording.
//!
//! # Onboarding is explicit, and the count is honest
//!
//! [`TOPICS`] is the onboarded list. It is short on purpose: a topic is
//! onboarded only once its subject matter is one this engine actually covers,
//! and every statement that does not run is counted in a NAMED skip class
//! (see [`SkipClass`]) so a green run states exactly what it proved. The
//! remaining topics are not silently skipped -- they are simply not on the
//! list yet, and [`survey_unonboarded_topics`] is the tool that ranks them.
//!
//! # The two content classes differ in kind
//!
//! Row results are directly comparable and are the bulk of the value. Plan
//! text is NOT: this tier's `EXPLAIN` printer deliberately describes the
//! executors it has rather than Go's, so an `EXPLAIN` is compared by the
//! access PROPERTY its case guards -- see `integration_plan_property`.

#[path = "integration_plan_property.rs"]
mod integration_plan_property;
#[path = "mysqltest_script.rs"]
mod mysqltest_script;

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use integration_plan_property::{access_property, plan_statement, PlanStatement};
use mysqltest_script::{align, parse_test, Item, Stmt};
use tidb_datatype::Datum;
use tidb_session::{Session, StmtOutput};

/// The onboarded topics, chosen from [`survey_unonboarded_topics`]'s own
/// ranking rather than by name: each replays far enough that its remaining
/// divergences are a countable list with named causes, so a regression
/// anywhere in these areas turns the gate red.
///
/// Nine of the ten had ZERO divergences when they were onboarded. The tenth,
/// `explain_easy`, is deliberately on the list at a cost: it is the only topic
/// here dense enough in plan text to prove the access-property comparison
/// works at all (53 of its plans match), and it contributes the whole of the
/// carried access-path debt below.
///
/// The reason recorded with each is what the topic buys that the topics
/// already on the list do not.
const TOPICS: &[(&str, &str)] = &[
    (
        "planner/core/join_reorder_through_projection",
        "the largest zero-divergence topic in the suite: join reorder through a \
         projection, row results and access properties together",
    ),
    (
        "util/admin",
        "the best-covered topic by ratio -- ADMIN's own row results over real tables",
    ),
    (
        "naaj",
        "null-aware anti join: the NULL semantics of NOT IN / != ALL over a subquery",
    ),
    (
        "planner/funcdep/only_full_group_by",
        "ONLY_FULL_GROUP_BY: which GROUP BY queries are accepted and which are refused",
    ),
    (
        "explain_easy",
        "the suite's plainest EXPLAIN topic -- the access-property comparison's own \
         proving ground",
    ),
    (
        "planner/core/rule_outer2inner",
        "outer-join-to-inner conversion, where a WHERE on the null-extended side \
         changes the answer",
    ),
    (
        "subquery",
        "correlated and uncorrelated subqueries in every clause",
    ),
    (
        "session/user_variables",
        "every statement compares: user variables end to end, nothing skipped",
    ),
    (
        "globalindex/insert",
        "INSERT against a global index on a partitioned table -- also nothing skipped",
    ),
    (
        "executor/admin",
        "ADMIN CHECK/SHOW over real tables, and the topic that PROVES the recursive-CTE \
         fixpoint is not quadratic: its 100,000-row `WITH RECURSIVE` never terminated \
         while the fold re-deduplicated the whole accumulated result each round",
    ),
];

/// Why one statement did not produce a comparable outcome. Every skip lands in
/// exactly one of these, and the totals are printed on every run: a driver
/// that reports what it skipped is worth more than one that reports a big
/// number of cases and quietly drops most of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SkipClass {
    /// A mysqltest directive rewrote or extended the recorded output (warnings
    /// block, info line, `replace_regex`, ...), so the recording is not the
    /// statement's own result.
    RecorderRewroteOutput(&'static str),
    /// TiDB recorded an error and this engine also refused the statement. The
    /// message wording is TiDB's own and is not compared; agreement on
    /// rejection IS the assertion, so this is a match, not a gap.
    BothRejected,
    /// The statement does not parse or plan here at all: a capability this
    /// engine does not model.
    OutOfDomain,
    /// An `EXPLAIN` whose recorded plan carries no extractable access
    /// property (nothing but shape, which this tier prints differently by
    /// design).
    PlanWithoutProperty,
    /// An `EXPLAIN` whose recording is not a text operator tree at all (JSON,
    /// DOT, a binary plan, or `EXPLAIN ANALYZE`'s execution counters).
    PlanFormatNotComparable(&'static str),
}

fn integrationtest_dir() -> PathBuf {
    difftest::parser_oracle::repo_root().join("tests/integrationtest")
}

/// What a matched statement proved. The three are different in kind, and a
/// count that merges them says less than it appears to: a matched row result
/// compared VALUES, while a matched side effect only agreed that a statement
/// recorded no output of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MatchKind {
    /// A result set: header and every row compared cell by cell.
    Rows,
    /// An `EXPLAIN`: the recorded plan's access property compared.
    PlanProperty,
    /// A statement whose recording is empty, and which this engine also
    /// completed without a result set.
    SideEffect,
}

/// One topic's replay outcome.
#[derive(Default)]
struct TopicReport {
    matched: BTreeMap<String, usize>,
    skipped: BTreeMap<String, usize>,
    divergences: Vec<String>,
}

impl TopicReport {
    fn skip(&mut self, class: SkipClass) {
        *self.skipped.entry(format!("{class:?}")).or_default() += 1;
    }

    fn matched(&mut self, kind: MatchKind) {
        *self.matched.entry(format!("{kind:?}")).or_default() += 1;
    }

    fn matched_total(&self) -> usize {
        self.matched.values().sum()
    }

    fn total(&self) -> usize {
        self.matched_total() + self.divergences.len() + self.skipped.values().sum::<usize>()
    }
}

/// Whether to print each out-of-domain statement with the error that refused
/// it (`INTEGRATION_SHOW_OUT_OF_DOMAIN=1`). This is the work list for the next
/// capability increment: a topic's skips ARE its remaining gaps.
fn show_out_of_domain() -> bool {
    static SHOW: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SHOW.get_or_init(|| std::env::var_os("INTEGRATION_SHOW_OUT_OF_DOMAIN").is_some())
}

/// Renders one result cell the way the recorder writes it: `NULL` for a null,
/// the value's SQL string otherwise.
fn cell(value: &Datum) -> String {
    if value.is_null() {
        return "NULL".to_owned();
    }
    value.sql_string().unwrap_or_else(|_| value.label())
}

/// Renders a result set as the recorder does: a tab-separated header of column
/// names, then one tab-separated line per row.
fn render_rows(columns: &[(String, tidb_datatype::FieldType)], rows: &[Vec<Datum>]) -> Vec<String> {
    let mut out = vec![columns
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>()
        .join("\t")];
    out.extend(
        rows.iter()
            .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t")),
    );
    out
}

/// Compares one statement's outcome against its recorded block.
///
/// Returns `Ok(kind)` when the outcome matches, `Err(Some(detail))` for a
/// divergence to report, and `Err(None)` when the statement was skipped (its
/// class already recorded).
fn compare(
    session: &mut Session,
    stmt: &Stmt,
    recorded: &[String],
    report: &mut TopicReport,
) -> Result<MatchKind, Option<String>> {
    if let Some(reason) = stmt.blocker {
        report.skip(SkipClass::RecorderRewroteOutput(reason));
        return Err(None);
    }
    let recorded_error = stmt.expect_error
        || recorded
            .first()
            .is_some_and(|line| line.starts_with("Error "));

    // A plan statement runs as this tier's own default EXPLAIN whatever format
    // the recording asked for -- see `PlanStatement::RunDefaultExplain`.
    let plan = plan_statement(&stmt.sql);
    let sql = match &plan {
        Some(PlanStatement::NotComparable(reason)) if !recorded_error => {
            report.skip(SkipClass::PlanFormatNotComparable(reason));
            return Err(None);
        }
        Some(PlanStatement::RunDefaultExplain(sql)) => sql.as_str(),
        _ => stmt.sql.as_str(),
    };
    let outcome = session.run_with_columns(sql);
    match (outcome, recorded_error) {
        // TiDB rejected it and so did we. The wording is TiDB's; only the
        // rejection is asserted.
        (Err(_), true) => {
            report.skip(SkipClass::BothRejected);
            Err(None)
        }
        (Ok(_), true) => Err(Some(format!(
            "  tidb: {}\n  rust: accepted the statement",
            recorded.first().map_or("<error>", String::as_str)
        ))),
        (Err(error), false) => {
            report.skip(SkipClass::OutOfDomain);
            if show_out_of_domain() {
                eprintln!("OUT OF DOMAIN: {}\n  {error:?}", stmt.sql);
            }
            Err(None)
        }
        (Ok(StmtOutput::Rows { columns, rows }), false) => {
            let mut ours = render_rows(&columns, &rows);
            let mut theirs: Vec<String> = recorded.to_vec();
            if plan.is_some() {
                // Drop the header on both sides: a plan's columns are fixed,
                // and only the access rows carry the guarded property.
                let want = access_property(&theirs[1.min(theirs.len())..]);
                let got = access_property(&ours[1..]);
                // Only the tables THIS side read are asserted, per
                // `access_property`'s own contract.
                let mut differences = Vec::new();
                for (table, ours) in &got {
                    let theirs = want.get(table);
                    if theirs != Some(ours) {
                        differences.push(format!(
                            "\n  table {table}\n    tidb: {}\n    rust: {}",
                            theirs.map_or("<not read>".to_owned(), |p| p.join(" + ")),
                            ours.join(" + ")
                        ));
                    }
                }
                return match (got.is_empty(), differences.is_empty()) {
                    (true, _) => {
                        report.skip(SkipClass::PlanWithoutProperty);
                        Err(None)
                    }
                    (false, true) => Ok(MatchKind::PlanProperty),
                    (false, false) => Err(Some(differences.join(""))),
                };
            }
            // `--sorted_result` means the recorder sorted the row lines under
            // an already-written header, so only the bodies are sorted here.
            if stmt.sorted {
                ours[1..].sort();
                if !theirs.is_empty() {
                    theirs[1..].sort();
                }
            }
            if ours == theirs {
                Ok(MatchKind::Rows)
            } else {
                Err(Some(format!(
                    "  tidb: {}\n  rust: {}",
                    theirs.join(" / "),
                    ours.join(" / ")
                )))
            }
        }
        // A side effect records no output of its own.
        (Ok(_), false) if recorded.is_empty() => Ok(MatchKind::SideEffect),
        (Ok(other), false) => Err(Some(format!(
            "  tidb: {}\n  rust: {other:?}",
            recorded.join(" / ")
        ))),
    }
}

/// Replays one topic against a fresh session.
fn run_topic(topic: &str) -> Result<TopicReport, String> {
    let dir = integrationtest_dir();
    let script = fs::read_to_string(dir.join(format!("t/{topic}.test")))
        .map_err(|e| format!("read t/{topic}.test: {e}"))?;
    let recorded = fs::read_to_string(dir.join(format!("r/{topic}.result")))
        .map_err(|e| format!("read r/{topic}.result: {e}"))?;
    let items = parse_test(&script)?;
    let aligned = align(&items, &recorded)?;

    let mut report = TopicReport::default();
    let mut session = Session::new();
    for (item, block) in aligned {
        let Item::Stmt(stmt) = item else { continue };
        match compare(&mut session, stmt, &block, &mut report) {
            Ok(kind) => report.matched(kind),
            Err(None) => {}
            Err(Some(detail)) => report
                .divergences
                .push(format!("\n--- [{topic}] {}\n{detail}", stmt.sql)),
        }
    }
    Ok(report)
}

#[test]
fn integrationtest_replay_matches_recorded_tidb_output() {
    let mut total = TopicReport::default();
    let mut per_topic = Vec::new();
    for (topic, _why) in TOPICS {
        let report = run_topic(topic).unwrap_or_else(|e| panic!("topic {topic}: {e}"));
        per_topic.push(format!(
            "{topic}: {} matched {:?}, {} diverged, {} skipped of {}",
            report.matched_total(),
            report.matched,
            report.divergences.len(),
            report.skipped.values().sum::<usize>(),
            report.total()
        ));
        total.divergences.extend(report.divergences);
        for (kind, count) in report.matched {
            *total.matched.entry(kind).or_default() += count;
        }
        for (class, count) in report.skipped {
            *total.skipped.entry(class).or_default() += count;
        }
    }

    eprintln!(
        "integrationtest replay over {} topics: {} of {} statements compared\n  {}\nmatches by kind: {:?}\nskips by class: {:?}",
        TOPICS.len(),
        total.matched_total() + total.divergences.len(),
        total.total(),
        per_topic.join("\n  "),
        total.matched,
        total.skipped
    );

    // The carried divergences are printed on demand, not only when the ratchet
    // breaks: they are the work list, and a work list that is only visible on
    // failure does not get worked off.
    if std::env::var_os("INTEGRATION_SHOW_DIVERGENCES").is_some() {
        eprintln!("carried divergences:{}", total.divergences.join(""));
    }

    // The ratchet. Every divergence below is a real gap against real TiDB's
    // own recorded output, printed in full so it can be worked off. The count
    // may only go DOWN: a permanently red suite destroys the signal, and
    // deleting a case destroys the evidence, so the debt is a number that
    // fails the moment it grows.
    //
    // The 35 statements remaining are ALL access-path decisions: every
    // row result in these nine topics already matches TiDB's recording exactly,
    // and so does every `Rows` header. Between them they carry 44 table-level
    // differences, in these causes -- none of them in this driver:
    //
    //  * NO COVERING-INDEX PREFERENCE (17). `select ifnull(null, t1.c1)` reads
    //    only indexed columns, so TiDB scans the narrow index
    //    (`IndexFullScan index:c2(c2)`); this tier scans the table.
    //  * NO STATISTICS (12). TiDB drops `stats:pseudo` once a table has been
    //    ANALYZEd; with no loaded statistics this tier prints it for a table
    //    TiDB had real statistics for. The whole difference is that one token.
    //  * NO EMPTY-RANGE FOLD (5). `where a = 1 and a = 2` is unsatisfiable, so
    //    TiDB reads NOTHING at all; this tier still plans a scan (with an
    //    empty range, or a Selection that filters every row) and answers the
    //    same empty result the slow way.
    //  * WRITES DO NOT CHOOSE AN ACCESS PATH (4). `update`/`delete` always
    //    scan and filter here, which `tidb_executor::explain`'s module docs
    //    already name as divergence 8; TiDB reaches `Point_Get` and
    //    `IndexRangeScan` for the same WHERE.
    //  * NO CLUSTERED-PK RANGE (3 + 1 `Point_Get`). `where t1.c1 > 0` on an
    //    integer primary key is `TableRangeScan range:(0,+inf]` in TiDB and a
    //    `TableFullScan` here: ranges are derived for INDEXES, not for the
    //    handle. The same absence is why `where c1 = 1 and c2 > 1` reaches an
    //    index here where TiDB reaches a point get.
    //  * TWO SINGLE CASES. An index range over a clustered table stops at the
    //    index column where TiDB extends it with the handle
    //    (`range:[1,1]` vs `range:(1 1,1 +inf]`), and an uncorrelated
    //    `c1 in (select ...)` folds to a `Batch_Point_Get` here where TiDB
    //    scans -- that one is a BETTER path reaching the same rows, kept in
    //    the count because the property really does differ.
    //
    // FIXED, and the reason this ratchet moved from 36 to 35: a BINARY-STRING
    // COLUMN COMPARED TO A HEX LITERAL was a WRONG ANSWER and not only a plan
    // -- `where a = x'FA34...'` on a `binary(16)` column matched NO row, with
    // or without an index, because the literal was typed as an INTEGER where
    // TiDB types it as a binary STRING (Go `DefaultTypeForValue`'s
    // `HexLiteral` arm). `tidb_expr::ops::string_cmp_operand` now carries the
    // literal's string domain, and `tidb_executor::go_quote` renders a binary
    // range bound with Go's `%q` instead of a lossy UTF-8 conversion, so the
    // `select` form's `IndexRangeScan range:["\xfa4\xe1\t<..." "xb",...]`
    // matches TiDB's recording exactly. The `update` form of the same WHERE
    // remains, under WRITES DO NOT CHOOSE AN ACCESS PATH above. The row-level
    // contract is pinned by `corpus/table/hex_literal_comparison`.
    const KNOWN_DIVERGENCES: usize = 35;

    assert!(
        total.divergences.len() <= KNOWN_DIVERGENCES,
        "{} of {} compared statements diverge from TiDB's recording, up from {} -- a new \
         divergence appeared:{}",
        total.divergences.len(),
        total.matched_total() + total.divergences.len(),
        KNOWN_DIVERGENCES,
        total.divergences.join("")
    );
    assert!(
        total.divergences.len() >= KNOWN_DIVERGENCES,
        "only {} of {} compared statements diverge now, down from {}. Lower \
         KNOWN_DIVERGENCES to {} so the ratchet holds.",
        total.divergences.len(),
        total.matched_total() + total.divergences.len(),
        KNOWN_DIVERGENCES,
        total.divergences.len()
    );
}

/// Replays the single topic named by `INTEGRATION_TOPIC`, printing one status
/// line. This is the survey's child process (see
/// [`survey_unonboarded_topics`]) and also the way to look at one topic by
/// hand:
///
/// ```sh
/// INTEGRATION_TOPIC=executor/join cargo test -p difftest-result-tests \
///   --test integration_diff -- --ignored --nocapture replay_one_topic
/// ```
#[test]
#[ignore = "onboarding tool: replays the one topic named by INTEGRATION_TOPIC"]
fn replay_one_topic_from_env() {
    let topic = std::env::var("INTEGRATION_TOPIC").expect("INTEGRATION_TOPIC must name a topic");
    let started = std::time::Instant::now();
    match run_topic(&topic) {
        Ok(report) => {
            eprintln!(
                "{:>5} matched {:>5} diverged of {:>5} in {:>6}ms  {topic}  {:?} {:?}",
                report.matched_total(),
                report.divergences.len(),
                report.total(),
                started.elapsed().as_millis(),
                report.matched,
                report.skipped,
            );
            if std::env::var_os("INTEGRATION_SHOW_DIVERGENCES").is_some() {
                eprintln!("{}", report.divergences.join(""));
            }
        }
        Err(reason) => eprintln!("UNALIGNED {topic}: {reason}"),
    }
}

/// Ranks the topics that are NOT yet onboarded by how much of each already
/// replays, so the next onboarding increment is chosen by evidence instead of
/// by name. Ignored by default: it replays all 257 topics and is an onboarding
/// tool, not a gate.
///
/// Each topic runs in its own CHILD PROCESS, because a survey of an engine
/// under construction meets outcomes a `catch_unwind` cannot survive: a stack
/// overflow aborts the process outright, and a statement that does not
/// terminate would hang the whole sweep. Isolation turns both into one
/// reported line for one topic instead of the end of the run.
///
/// ```sh
/// cargo test -p difftest-result-tests --test integration_diff -- --ignored --nocapture survey
/// ```
///
/// # The crashes the survey still reports, named
///
/// 200 topics report a status line; the rest are `UNALIGNED` on this driver's
/// own limits (a multi-connection `connect (...)` script, or a `.result`
/// recording that is not valid UTF-8). Eight topics CRASH, and every one of
/// them is the SAME defect, distinct from the four this driver's arrival
/// already worked off:
///
/// A chunk column's shape comes from a FIELD TYPE while `Chunk::append_datum`
/// dispatches on the DATUM KIND, so the two must agree -- and an expression's
/// INFERRED return type does not always match the datum it evaluates to.
/// `executor/ddl`, `planner/core/casetest/integration` and
/// `planner/core/issuetest/planner_issue` put an 8-byte value in a
/// variable-length column; `explain_complex` and `select` cross a decimal's
/// 40-byte cell with an 8-byte one; `expression/builtin` reaches
/// `append_datum` with a decimal whose text is not a number at all (`I311`).
/// `expression/issues` and `expression/json` are a different, expression-side
/// panic (`tidb-expr/src/ops.rs:349`).
///
/// The fix for the first six is expression return-type inference, which
/// `driver::set_opr` already names as its own DEFERRED item (a set operation's
/// column metadata comes from the first term, with no type unification). It is
/// one root cause, not six, and it is why those topics are not onboarded.
#[test]
#[ignore = "onboarding tool: replays all 257 topics to rank the next candidates"]
fn survey_unonboarded_topics() {
    let dir = integrationtest_dir();
    let mut topics = Vec::new();
    let mut stack = vec![dir.join("t")];
    while let Some(at) = stack.pop() {
        for entry in fs::read_dir(&at).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "test") {
                topics.push(
                    path.strip_prefix(dir.join("t"))
                        .unwrap()
                        .with_extension("")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    topics.sort();

    for topic in &topics {
        match replay_in_child(topic) {
            Ok(()) => {}
            Err(status) => eprintln!("{status}  {topic}"),
        }
    }
}

/// Runs one topic in a child process, returning `Err` with the outcome when the
/// child did not report for itself. The child inherits stderr, so a successful
/// replay's own status line is already on the terminal.
fn replay_in_child(topic: &str) -> Result<(), String> {
    /// A topic that hangs must not hang the sweep. Every onboarded topic
    /// replays in tens of milliseconds; a second is already three orders of
    /// magnitude of headroom.
    const BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "--nocapture",
            "--ignored",
            "replay_one_topic_from_env",
        ])
        .env("INTEGRATION_TOPIC", topic)
        .stdout(std::process::Stdio::null())
        .spawn()
        .map_err(|e| format!("SPAWN FAILED ({e})"))?;

    let deadline = std::time::Instant::now() + BUDGET;
    loop {
        match child.try_wait().map_err(|e| format!("WAIT FAILED ({e})"))? {
            Some(status) if status.success() => return Ok(()),
            Some(status) => return Err(format!("CRASHED ({status})")),
            None if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("DID NOT FINISH IN {BUDGET:?}"));
            }
            None => std::thread::sleep(std::time::Duration::from_millis(20)),
        }
    }
}
