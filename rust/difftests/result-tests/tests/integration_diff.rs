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
//! # A topic may drive several connections
//!
//! A topic that drives SEVERAL connections replays through
//! `mysqltest_connections`, which holds one session per connection over one
//! shared store -- read its docs for which state is shared and which is not,
//! and for why an account it cannot authenticate refuses the topic instead of
//! falling back to root.
//!
//! # The two content classes differ in kind
//!
//! Row results are directly comparable and are the bulk of the value. Plan
//! text is NOT: this tier's `EXPLAIN` printer deliberately describes the
//! executors it has rather than Go's, so an `EXPLAIN` is compared by the
//! access PROPERTY its case guards -- see `integration_plan_property`, which
//! also owns the rule for WHICH statements are plans: `EXPLAIN`, `DESCRIBE`
//! and `DESC` are one statement in TiDB's parser, and the split against
//! `DESC <table>`'s column list is made by the token after the keyword.

#[path = "integration_plan_property.rs"]
mod integration_plan_property;
#[path = "mysqltest_connections.rs"]
mod mysqltest_connections;
#[path = "mysqltest_script.rs"]
mod mysqltest_script;

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use integration_plan_property::{access_property, plan_statement, PlanStatement};
use mysqltest_connections::Connections;
use mysqltest_script::{align, parse_test, split_warnings, Item, Stmt};
use tidb_datatype::Datum;
use tidb_session::{Session, StmtOutput};

/// The onboarded topics, chosen from [`survey_unonboarded_topics`]'s own
/// ranking rather than by name: each replays far enough that its remaining
/// divergences are a countable list with named causes, so a regression
/// anywhere in these areas turns the gate red.
///
/// All but one had ZERO divergences when they were onboarded. The exception,
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
        "INSERT against a global index on a partitioned table. It was onboarded \
         with nothing skipped, which was an ILLUSION: its `CREATE TABLE ... \
         PARTITION BY` silently built an ordinary table, so 12 of its 14 \
         statements were compared against the wrong object. Now that the \
         create is refused those 12 are named OutOfDomain skips and 2 \
         statements are proved -- a smaller claim that is a true one, and the \
         topic's skip count is the size of the partitioning gap here",
    ),
    (
        "session/txn",
        "the first MULTI-CONNECTION topic on the gate: a second connection's \
         `BEGIN`/`COMMIT` against the same store as the first",
    ),
    (
        "executor/rowid",
        "`_tidb_rowid` written and read back across two connections",
    ),
    (
        "ddl/ddl_tiflash",
        "TiFlash replica DDL, refused on a peer connection exactly where TiDB \
         refuses it",
    ),
    (
        "executor/admin",
        "ADMIN CHECK/SHOW over real tables, and the topic that PROVES the recursive-CTE \
         fixpoint is not quadratic: its 100,000-row `WITH RECURSIVE` never terminated \
         while the fold re-deduplicated the whole accumulated result each round",
    ),
    (
        "executor/merge_join",
        "the largest zero-divergence topic left in the suite (246 of 259), and the one \
         that gates DERIVED TABLES: it compares row results and access properties for \
         merge joins whose sides are subqueries in `FROM`, which only became \
         describable once the plan recorder learned to descend into a derived table",
    ),
    (
        "ddl/db_rename",
        "the metadata-only ALTER actions' own gate: `RENAME INDEX`'s three \
         outcomes -- renamed, ignored as the same spelling, and 1061 naming the \
         EXISTING index -- decided by the case-sensitivity rule in Go's \
         `ValidateRenameIndex`, with `ADMIN CHECK INDEX` reading the renamed \
         key back",
    ),
    (
        "planner/core/join_reorder2",
        "join reorder over derived tables specifically -- 12 of its 30 matches are \
         access properties, so a regression in which side of a `FROM (SELECT ...)` is \
         read, or how, turns it red",
    ),
    (
        "session/variable",
        "the sysvar registry's own edge cases: which SET values clamp, which are \
         refused, and which switches name a feature that is now always on",
    ),
    (
        "executor/analyze",
        "ANALYZE's own statement surface: which forms are accepted, which are refused \
         as removed features, and the warnings a `SET` of a removed switch raises",
    ),
    // The four partition topics below reached zero divergences the moment
    // `CREATE TABLE ... PARTITION BY` stopped silently building an ordinary
    // table (they carried 32, 107, 90 and 10 divergences against the flat
    // object). They are onboarded for exactly that reason: their value is not
    // in what they prove about partitioning -- most of each is an honest
    // OutOfDomain skip -- but in being the tripwire that turns red the day a
    // partial partitioning implementation starts answering these statements
    // WRONGLY again rather than refusing them.
    (
        "table/partition",
        "the partition-refusal gate: 41 side effects proved and 35 statements \
         refused exactly where TiDB refuses them, with every partitioned \
         object's own query named as a skip rather than compared against a \
         flat table",
    ),
    (
        "planner/core/partition_pruner",
        "the largest partition topic at zero divergences (156 matched): every \
         query whose answer DEPENDS on pruning is a named skip, so a pruning \
         implementation that prunes wrongly cannot pass this quietly",
    ),
    (
        "executor/partition/partition_with_expression",
        "83 matched over expression-partitioned tables, the topic that carried \
         90 divergences while the partition expression was being discarded",
    ),
    (
        "executor/index_lookup_pushdown_partition",
        "index-lookup pushdown against a partitioned table -- the smallest of \
         the four and the only one that reaches a partitioned read path at all",
    ),
    (
        "table/cache",
        "reached zero divergences (82 matched, from 20) when a NOT NULL column \
         added by `ALTER TABLE` stopped backfilling NULL into the rows written \
         before it: the topic is dense in `ALTER TABLE ... ADD COLUMN` over \
         tables that already hold rows, so it is the read-back of an origin \
         default that it really gates",
    ),
    (
        "explain",
        "the only onboarded topic that compares `DESC <view>`, which is the one \
         place a view's column metadata is read back through the SHOW surface \
         rather than through `information_schema.columns` -- the two disagree \
         on purpose (see `view_column_description` in `tidb_session::show`) \
         and only a recording can hold both halves in place at once",
    ),
    (
        "executor/jointest/join",
        "the suite's join topic, and the largest single block of newly \
         MEASURABLE statements onboarded here: 793 compared where the topic \
         previously could not be replayed at all, because its \
         `tidb_mem_quota_query = 1 << 18` cross join ran forever instead of \
         raising 8175. It is the only onboarded topic that gates the memory \
         quota on the READ path",
    ),
    (
        "sessionctx/setvar",
        "the largest ZERO-divergence topic in the suite: 709 statements over the \
         system-variable surface -- what each variable accepts, what it refuses, \
         what it reads back, and what a SET_VAR hint does to a statement. Its \
         last two divergences were the non-prepared plan cache's own \
         `@@last_plan_from_cache`, which is why onboarding it belongs to that \
         unit: without this entry nothing gates the cache against regressing",
    ),
];

/// A topic listed twice is replayed twice, and every statement it compares is
/// counted twice in the headline totals -- which is exactly what happened when
/// `session/variable` and `table/cache` were each onboarded a second time by a
/// later unit that did not notice the first entry. The ratchet never lied (a
/// duplicated topic contributes its divergences twice, so the count still only
/// moves when real behaviour moves), but the compared/matched figures did, and
/// a number that overstates what was proved is the one number this driver may
/// not get wrong. Checked before the replay so the run stops rather than
/// reporting an inflated total.
fn assert_topics_are_unique() {
    let mut seen = BTreeMap::new();
    for (topic, _why) in TOPICS {
        *seen.entry(*topic).or_insert(0usize) += 1;
    }
    let dupes: Vec<_> = seen
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(t, n)| format!("{t} ({n}x)"))
        .collect();
    assert!(
        dupes.is_empty(),
        "TOPICS lists {} topic(s) more than once: {}. Each is replayed once per \
         entry and counted once per replay, inflating the compared and matched \
         totals. Remove the extra entry.",
        dupes.len(),
        dupes.join(", ")
    );
}

#[test]
fn topics_are_listed_once_each() {
    assert_topics_are_unique();
}

/// How far the warning comparison actually reaches, stated as a number instead
/// of assumed.
///
/// [`compare`] asks `SHOW WARNINGS` for a statement ONLY when `stmt.warnings`
/// is set, and that flag comes from the `--enable_warnings` directive in the
/// `.test` script -- not from anything this engine or TiDB did. Every other
/// statement is compared on its rows alone, so a warning TiDB raises and this
/// tier does not (or the reverse) leaves the rows identical and the replay
/// calls it a match.
///
/// That is not a hypothesis. This test parses the onboarded scripts with the
/// replay's own reader and counts the statements the gate can see: 28 of 4,906
/// -- 0.6%. The blind spot is the other 4,878, and it is the reason a fix that
/// added three real `CAST` truncation warnings moved neither ratchet.
///
/// The number is pinned so that widening or narrowing the warning gate is a
/// visible edit rather than a silent one. Raising the covered count is
/// progress; the total moving means TOPICS changed and both figures should be
/// re-read, not patched.
#[test]
fn warning_comparison_covers_only_enable_warnings_statements() {
    let dir = integrationtest_dir();
    let mut covered = 0;
    let mut total = 0;
    let mut per_topic = Vec::new();
    for (topic, _why) in TOPICS {
        let script = fs::read_to_string(dir.join(format!("t/{topic}.test")))
            .unwrap_or_else(|e| panic!("read t/{topic}.test: {e}"));
        let items = parse_test(&script).unwrap_or_else(|e| panic!("parse t/{topic}.test: {e}"));
        let stmts: Vec<&Stmt> = items
            .iter()
            .filter_map(|item| match item {
                Item::Stmt(stmt) => Some(stmt),
                _ => None,
            })
            .collect();
        let warned = stmts.iter().filter(|stmt| stmt.warnings).count();
        covered += warned;
        total += stmts.len();
        if warned > 0 {
            per_topic.push(format!("{topic}: {warned} of {}", stmts.len()));
        }
    }
    eprintln!(
        "warning gate reaches {covered} of {total} statements across {} topics\n  {}",
        TOPICS.len(),
        per_topic.join("\n  ")
    );
    assert_eq!(
        (covered, total),
        (28, 4906),
        "the warning gate's reach changed; re-read what it now covers rather \
         than updating this number to match"
    );
}

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
    // `--enable_warnings` appended this statement's `SHOW WARNINGS` to its own
    // output rather than rewriting it, so the block is two blocks. Comparing
    // the halves separately is what makes these statements comparable at all,
    // and it puts the warning texts themselves under the gate.
    let (rows, warnings) = if stmt.warnings {
        let (rows, warnings) = split_warnings(recorded);
        (rows, Some(warnings))
    } else {
        (recorded, None)
    };
    match (compare_output(session, stmt, rows, report), warnings) {
        // Only a statement that was actually COMPARED gets its warnings
        // compared: a skip means this tier never produced the outcome the
        // warnings would belong to.
        (Ok(kind), Some(want)) => match warning_difference(session, want) {
            None => Ok(kind),
            Some(detail) => Err(Some(detail)),
        },
        (outcome, _) => outcome,
    }
}

/// Reports how this session's warnings differ from the recorded ones, or
/// `None` when they agree.
///
/// `want` is `None` when the recorder appended no block at all, which says the
/// statement warned about NOTHING -- an assertion in its own right, so it is
/// read as the empty list rather than waived.
///
/// `SHOW WARNINGS` does not consume what it reports and the buffer is reset by
/// the next statement anyway, so asking here is observationally neutral -- it
/// is also exactly what mysqltest itself did to produce the recording.
fn warning_difference(session: &mut Session, want: Option<&[String]>) -> Option<String> {
    let ours = match session.run_with_columns("SHOW WARNINGS") {
        Ok(StmtOutput::Rows { columns: _, rows }) => rows
            .iter()
            .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t"))
            .collect::<Vec<String>>(),
        _ => return Some("  rust: SHOW WARNINGS answered with no result set".to_owned()),
    };
    let want = want.unwrap_or(&[]);
    if ours == want {
        return None;
    }
    Some(format!(
        "  tidb warnings: {}\n  rust warnings: {}",
        if want.is_empty() {
            "<none>".to_owned()
        } else {
            want.join(" / ")
        },
        if ours.is_empty() {
            "<none>".to_owned()
        } else {
            ours.join(" / ")
        }
    ))
}

/// Compares one statement's own output -- rows or rejection -- against the
/// recorded block, with any appended warnings block already removed.
fn compare_output(
    session: &mut Session,
    stmt: &Stmt,
    recorded: &[String],
    report: &mut TopicReport,
) -> Result<MatchKind, Option<String>> {
    if let Some(reason) = stmt.blocker {
        // The recorder rewrote this statement's output, so nothing about it is
        // comparable -- but mysql-tester still RAN it, and what it did is what
        // the statements after it read. Skipping the RUN as well silently
        // rewinds the session: in `session/variable`, `set @@global.x = 1.1`
        // sits under `--enable_warnings`, so the clamped value it stores never
        // happened here and the next four `select @@global.x` diverged on a
        // value this driver had suppressed rather than on anything the engine
        // does. Run it, discard the outcome, and count the skip.
        drop(session.run_with_columns(&stmt.sql));
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
            // A CELL may itself contain newlines (`SHOW CREATE TABLE` is the
            // common one), and the recorder has no escape for that: it writes
            // the row out and an embedded newline simply becomes another
            // PHYSICAL line in the `.result` file. Splitting after the sort --
            // the recorder sorts ROWS, then writes them -- puts both sides in
            // the same units, so a multi-line cell compares by its text
            // instead of always diverging on the line count.
            let ours: Vec<String> = ours
                .iter()
                .flat_map(|line| line.split('\n').map(str::to_owned))
                .collect();
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
///
/// The replay runs on `difftest::on_deep_stack` for the same reason
/// `query_diff` does, and the reason is worth stating precisely because it is
/// NOT a depth limit: Go runs every statement on a goroutine whose stack GROWS
/// on demand, so no recursion bound is part of the recorded behaviour --
/// verified by grep over `pkg/parser`, `pkg/expression` and `pkg/planner`,
/// which contain no nesting-depth guard at all. This tier recurses on a fixed
/// OS thread stack, so the harness has to supply the room Go's runtime supplies
/// itself. TiDB's own suite is far past libtest's default 8MB: `select` writes
/// `select --------------------1`, `executor/window` has a single `INSERT` with
/// 175 value rows, and `executor/jointest/join` joins 21 tables -- the last two
/// are recursion over INPUT SIZE, not over nesting the user wrote. Sizing the
/// replay thread changes nothing about what a statement EVALUATES to; it only
/// stops the process from aborting before the comparison happens.
fn run_topic(topic: &str) -> Result<TopicReport, String> {
    let topic = topic.to_owned();
    difftest::on_deep_stack(move || run_topic_on_this_stack(&topic))
}

fn run_topic_on_this_stack(topic: &str) -> Result<TopicReport, String> {
    let dir = integrationtest_dir();
    let script = fs::read_to_string(dir.join(format!("t/{topic}.test")))
        .map_err(|e| format!("read t/{topic}.test: {e}"))?;
    let recorded = fs::read_to_string(dir.join(format!("r/{topic}.result")))
        .map_err(|e| format!("read r/{topic}.result: {e}"))?;
    let items = parse_test(&script)?;
    let aligned = align(&items, &recorded)?;

    let mut report = TopicReport::default();
    let mut connections = Connections::open(topic)?;
    for (item, block) in aligned {
        let stmt = match item {
            Item::Stmt(stmt) => stmt,
            // A connection command drives the pool, not the server. A command
            // the pool cannot honour faithfully -- an account it cannot
            // authenticate above all -- ends the topic here instead of running
            // the rest on the wrong session.
            Item::Connection(cmd) => {
                connections.apply(cmd)?;
                continue;
            }
            Item::Echo(_) => continue,
        };
        match compare(connections.current(), stmt, &block, &mut report) {
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
    assert_topics_are_unique();

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
    //  * WRITES ARE NOT OFFERED AN INDEX PATH (2). An `update`/`delete` whose
    //    `WHERE` names a non-unique secondary index scans and filters here
    //    where TiDB reaches `IndexRangeScan`; `tidb_executor::explain`'s
    //    module docs name it as what is left of divergence 8. (The `Point_Get`
    //    half of this class closed when the write path started calling
    //    `try_point_get`, which is the 51 -> 49 note below.)
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
    //
    // MOVED 35 -> 61 when the plan recorder learned to DESCEND INTO A DERIVED
    // TABLE. Not one new wrong answer: an `EXPLAIN` over a subquery in `FROM`
    // used to be refused outright, so 168 statements -- 26 of them in the two
    // topics already onboarded here -- were counted as OUT OF DOMAIN and their
    // access property was never compared to TiDB's at all. Describing the plan
    // did not change which rows any statement returns (no topic's matched
    // count fell, and `Rows` matches only rose); it removed a blindfold, and
    // what was behind it was debt that already existed everywhere else in this
    // list. `planner/core/join_reorder_through_projection` carries 26 where it
    // carried 2, and `explain_easy` 34 where it carried 32. Every one of the
    // newly compared divergences falls in a class named above or here:
    //
    //  * COVERING-INDEX ENUMERATION (the large majority, and the whole of the
    //    +2 in `explain_easy`). TiDB reads `IndexFullScan` on a narrow index
    //    that covers the columns the query needs; this tier reads the whole
    //    table. `tidb_executor::access_cost`'s module doc already names this
    //    as its own deferred item -- Go keeps an index path with NO access
    //    conditions (`keepIndex := ... || path.IsSingleScan`) and this tier's
    //    `enumerate_paths` only builds a candidate the detacher ranged. It
    //    costs rows read, not correctness, and it is the same absence whether
    //    the scan sits under a derived table or not: `explain select * from t
    //    where a > 1` and the same select wrapped in `(SELECT ...) x` pick the
    //    identical path here, which is the proof it is not derived-table
    //    specific.
    //  * NO STATISTICS. FIXED -- see the 55 -> 41 note below.
    //
    // Lowering this number again is the job of those two, and the shape of the
    // work is already measured: `INTEGRATION_SHOW_DIVERGENCES=1` on either
    // topic prints the per-table pairs, and 17 of the 26 in
    // `join_reorder_through_projection` are the covering-index class alone.
    //  * DISTINCT WITH AN ORDER BY OUTSIDE THE SELECT LIST. FIXED -- see the
    //    58 -> 55 note below.
    //
    // 64 -> 58 when `tidb_executor::access_cost` learned Go's `keepIndex :=
    // ... || path.IsSingleScan`: an index the ranger narrowed nothing on is
    // now a candidate whenever it COVERS the statement, so `explain_easy`
    // carries 28 where it carried 34 and its `PlanProperty` matches rose 58
    // -> 64. Not one row changed: `Rows` stayed at 347 and `SideEffect` at
    // 1211 across the whole gate, which is the check that separates an
    // access-path fix from a semantics break. `join_reorder_through
    // _projection` did NOT move: its 26 are all reads of a table INSIDE a
    // join, and the single-table seam that commits an access path
    // (`driver::access::commit_fast_path_source`) is never reached for a
    // join input. That is the next increment there, and it is a bigger one.
    //
    // 58 -> 55, and `planner/funcdep/only_full_group_by` is at ZERO: Go's
    // `checkOrderByInDistinct` is ported (`tidb_executor::driver::
    // only_full_group_by::check_order_by_in_distinct`), so `SELECT DISTINCT
    // t1.a FROM t as t1 ORDER BY t1.d LIMIT 1` is 3065 here as it is in TiDB.
    // The rule needed none of the functional-dependency machinery 1055 needs,
    // and the capture is why: `SELECT DISTINCT id, v FROM pk(id INT PRIMARY
    // KEY, v, w) ORDER BY w` is 3065 even though the key IS in the select
    // list, because after DISTINCT the query does not report `w` at all.
    // Nothing else moved -- `PlanProperty` stayed at 177, `Rows` at 347 and
    // `SideEffect` at 1212 -- and the three statements moved from `diverged`
    // to `BothRejected`, which asserts the REJECTION only; the errno and the
    // message text are pinned by `tidb_session`'s
    // `select_distinct_may_only_order_by_a_field_it_reports`.
    //
    // 55 -> 41, and the NO STATISTICS class is at ZERO: the in-process session
    // runs `ANALYZE TABLE` (`tidb_executor::analyze`, driven from
    // `tidb_session::analyze_arm`), so a table these scripts analyze now has a
    // real row count and real histograms instead of `statistics.PseudoTable`.
    // Of the 91 table-level pairs carried at 55, exactly 38 were the pure
    // stats-token difference -- the access OBJECT and range already agreed and
    // only the `stats:pseudo` suffix did not -- and all 38 are gone; the 53
    // that remain are the access-path classes above, unchanged.
    //
    // Nothing this measured moved a ROW. Every topic's `Rows` count is
    // identical either side of the change (59, 30, 24, 4, 2, 13, 5, 44, 1, 43,
    // 2, 82, 50, 4, 56), which is the check that separates an estimate change
    // from a semantics break; `PlanProperty` rose 64 -> 70 in `explain_easy`
    // and 56 -> 64 in `join_reorder_through_projection`, and the compared total
    // rose 1894 -> 1914 because `ANALYZE` itself, and the statements a refused
    // `ANALYZE` used to skip past, are now compared too.
    //
    // 41 of these are the access-path classes above, in two topics:
    // `explain_easy` 22 and `planner/core/join_reorder_through_projection` 18,
    // plus 1 in `subquery`.
    //
    // # The 11 that HASH partitioning added, each classified
    //
    // `CREATE TABLE ... PARTITION BY HASH` now succeeds instead of refusing
    // (see `tidb_executor::partition_routing`), so the statements that used to
    // skip past a table that did not exist are compared. The compared total
    // rose 1914 -> 2147 (+233) and four partition topics moved off zero:
    // `table/partition` 41 -> 53 matched, `planner/core/partition_pruner`
    // 156 -> 195, `executor/partition/partition_with_expression` 89 -> 167,
    // `executor/index_lookup_pushdown_partition` 15 -> 30; `globalindex/insert`
    // 2 -> 8, `executor/analyze` 82 -> 150 and `executor/admin` 107 -> 111
    // followed. The 11 new divergences are all in those topics:
    //
    // * PARTITION PRUNING IS NOT MODELLED (4). Go's access object names the
    //   partitions the plan will read (`Batch_Point_Get table:t,
    //   partition:p1,P2`, and the per-partition `IndexFullScan ... partition:p0
    //   + ... partition:p1 + ...` fan-out); this tier reads the whole relation
    //   as one key range and prints no partition. 2 in `partition_pruner`,
    //   2 in `index_lookup_pushdown_partition`. This is the next rung, and it
    //   is the one that closes them.
    // * AN INDEX PATH IS NOT CHOSEN ON A PARTITIONED TABLE (3). Go plans
    //   `IndexRangeScan`/`IndexFullScan` where this tier plans `TableFullScan`,
    //   in `index_lookup_pushdown_partition`. Same class as the 41 above -- the
    //   access-path chooser -- not a partitioning fault.
    // * AN UNORDERED `LIMIT` RETURNS A DIFFERENT ROW SET (1). `limit 3` with no
    //   `ORDER BY` over a partitioned table: Go's rows come out in its
    //   per-partition scan order and the recording pins that order.
    // * AUTO-ID ALLOCATION ON A PARTITIONED TABLE (1). Go allocates
    //   `_tidb_rowid`/AUTO_INCREMENT per PARTITION, so `table/partition`'s
    //   `t_a` reads back 2,4,6,8,10 where this tier's single monotone counter
    //   gives 1,2,3,4,5. Real and worth its own unit; the values are unique
    //   either way, so nothing is lost or overwritten.
    // * `HEX()` OF A BIT COLUMN KEEPS LEADING ZERO BYTES (1). `hex(b)` reads
    //   `00080A0D091A` here and `80A0D091A` in TiDB. Nothing to do with
    //   partitioning -- it became measurable because the table now exists.
    // # 52 -> 51: the clustered-handle range
    //
    // The table path now builds ranges over an integer primary key
    // (`tidb_executor::handle_range`), so a `WHERE` that bounds the handle
    // reads `TableRangeScan range:[...]` instead of `TableFullScan`. That
    // closes one of `explain_easy`'s access-path divergences (22 -> 21, and
    // its `PlanProperty` matches 70 -> 71); no other topic's count moved.
    //
    // The remaining `explain_easy` 21 are all still the SAME access-path
    // class -- Go reaching an index or a point get where this tier reaches a
    // scan -- and several are now a range scan rather than a full scan, which
    // is closer to Go's row count without yet being its operator. Two named
    // shapes stay out of reach on purpose: a handle bound under a JOIN (the
    // range is offered by the single-table fast path only), and
    // `USE INDEX(idx)`, whose hint this tier does not honour either way.
    //
    // Nothing here moved a ROW: `explain_easy` matches only `PlanProperty`
    // and `SideEffect`, and every topic's `Rows` count is unchanged.
    //
    // # 51 -> 49: the write's point plan
    //
    // A single-table `UPDATE`/`DELETE` whose `WHERE` pins a whole key now
    // plans Go's `Point_Get` instead of a `TableRangeScan` over the
    // degenerate range, because the write path calls `try_point_get` -- the
    // same function a `SELECT` reaches -- exactly as
    // `tryUpdatePointPlan`/`tryDeletePointPlan` hand `tryPointGetPlan` a
    // `SelectStmt` synthesized from the write's own clauses. That closes two
    // more of `explain_easy`'s access-path divergences (21 -> 19, and its
    // `PlanProperty` matches 71 -> 73); no other topic's count moved, and
    // again no `Rows` count moved anywhere -- the point plan changes which
    // request finds the row, never which row is found.
    //
    // # 49 -> 50: prefix indexes became measurable
    //
    // `CREATE TABLE ... INDEX indexIDname (ID(8), name(8))` used to be
    // REFUSED, so `executor/admin`'s whole `t` block was out of domain.
    // Prefix indexes on secondary keys are built now, which moved 7
    // statements into the compared set (2339 -> 2346) and 6 of them into the
    // matched set (2290 -> 2296) -- `admin check table t` over that very
    // prefix index among them.
    //
    // The 7th is the one new divergence, and it is NOT about prefixes:
    //
    //     use mysql;
    //     admin check table t;   -- tidb: 1146 Table 'mysql.t' doesn't exist
    //
    // The in-process catalog has no `mysql` schema, so `USE mysql` fails,
    // the session's current database stays `executor__admin`, and the
    // unqualified `t` resolves to the table that was just created there.
    // Had `USE` taken effect the lookup would have been `mysql.t` and the
    // answer 1146, which is what makes this a `USE`/bootstrap-schema gap
    // rather than an admin-check or index one -- the resolution itself
    // (`Session::split_table_path`) already goes through the current
    // database.
    //
    // # 50 -> 49: the `mysql` schema became a name that exists
    //
    // That prediction paid out exactly. `Catalog::default` now seeds `mysql`
    // as a schema OBJECT with no tables, so `use mysql` succeeds, the
    // unqualified `t` behind it resolves to `mysql.t`, and `admin check
    // table t` answers 1146 as TiDB does. One statement moved into the
    // compared set (2346 -> 2347) and three into the matched set (2296 ->
    // 2298) -- the divergence itself plus the two statements after it in the
    // block, which had been checked against `executor__admin`'s tables.
    //
    // Nothing else moved: no other topic's counts changed, and the survey's
    // 283 refusals that NAME a `mysql.*` table are untouched, because they
    // want the tables' CONTENTS and an empty schema has none. The
    // measurement that decided the scope: `Schema(UnknownDatabase("mysql"))`
    // refused 5 statements in the whole 257-topic survey -- four `use
    // mysql;` and one `connect (conn1,...,mysql)` -- and that connect is the
    // expensive one, since it left `statistics/lock_table_stats` UNALIGNED,
    // i.e. never compared at all.
    // # 50 -> 51: RANGE partitioning became measurable
    //
    // `CREATE TABLE ... PARTITION BY RANGE` used to be REFUSED, so every
    // statement against a range-partitioned table was out of domain. RANGE is
    // built, routed and pruned now, and `SELECT ... PARTITION (p)` is
    // answered for every method, which moved 192 statements into the compared
    // set (2346 -> 2538) and 191 of them into the matched set. Three topics
    // carry almost all of it: `table/partition` 53 -> 99 matched,
    // `executor/partition/partition_with_expression` 167 -> 268, and
    // `planner/core/partition_pruner` 199 -> 221.
    //
    // Net ONE new divergence, and the whole newly-measurable set is two
    // classes, neither about a row a partitioned read returns:
    //
    // 1. EXPLAIN does not name the partitions (4 statements). Go's
    //    `PartitionProcessor` REWRITES the plan into one `DataSource` per
    //    surviving partition under a union, so its access property reads
    //    `TableFullScan table:t2, partition:p1 + ... partition:p2` and its
    //    point plan reads `Batch_Point_Get table:t, partition:p1,P2`. This
    //    node prunes INSIDE one scan (`KvTable::read_partitions`), so the
    //    node count and the `partition:` cell differ while the rows do not.
    //    The two `a = 2 and a = 3` rows are the same class from the other
    //    side: Go plans a `TableDual` and reads no table at all, while this
    //    node keeps a scan whose pruned range set is empty -- it reads zero
    //    records, which `range_pruning_reads_only_the_partitions_that_can_match`
    //    pins by `actRows`.
    //
    // 2. `select hex(b) from tb` (1 statement), which is a BIT-column
    //    rendering bug -- `hex()` keeps two leading zero bytes TiDB drops --
    //    and has nothing to do with partitioning. It became visible only
    //    because `tb`'s partitioned `CREATE TABLE` now succeeds.
    //
    // Two of the divergences the older set carried are GONE for the same
    // reason (`table/partition` 2 -> 1, and the `timezone_test` block below),
    // which is why 5 statements are named as new while the total moves by
    // one.
    //
    // A `VALUES LESS THAN (UNIX_TIMESTAMP('...'))` bound is now FOLDED under
    // the session's own `time_zone`: `run_create_table_in` carries the
    // session's `StmtContext` through the re-parse. That was the last thing
    // keeping the `timezone_test` block refused, and unrefusing it is why
    // this count moves UP by one while the suite gets strictly stronger:
    //
    //   before: 50 divergences of 2539 compared (2489 matched)
    //   after:  51 divergences of 2556 compared (2505 matched)
    //
    // 17 statements that used to be named OutOfDomain skips are now really
    // compared, 16 of them matching. The one that did not was
    // `SELECT * FROM timezone_test PARTITION (p5)` read back from a UTC
    // session after a Shanghai session inserted the row: TiDB reported
    // `2020-01-03 07:16:59` and this node `2020-01-03 15:16:59`.
    //
    // # 51 -> 50: TIMESTAMP is stored in UTC
    //
    // That was the TIMESTAMP STORAGE seam, and it is closed. A `TIMESTAMP`
    // is converted to UTC on the way into the row bytes and back into the
    // READING session's `time_zone` on the way out, which is the whole
    // meaning of the type; `DATETIME` and `DATE` still store the written
    // wall-clock text, which is the whole meaning of THOSE. The seams are
    // `tidb_codec::rowcodec`'s column encode/decode for the stored row and
    // `encode_key_in_timezone` for the index entry -- Go's
    // `rowcodec`/`codec.EncodeKey` -- and what changed is that the session's
    // zone now reaches them instead of a hardcoded `None`. See
    // `tests_timezone_storage` in `tidb-session` for the captured
    // cross-session round trips, DST boundaries included.
    //
    // # A note on the compared/matched figures quoted above
    //
    // Every figure in the narrative above was read off a run whose TOPICS
    // list held `session/variable` and `table/cache` TWICE, so each of those
    // topics was replayed and counted twice: the totals are inflated by 167
    // matched and 216 statements. The list is de-duplicated now and
    // `assert_topics_are_unique` keeps it that way. The DELTAS above are all
    // still exact -- a duplicated topic inflates both sides of a difference
    // equally -- and so is this ratchet, because both duplicated topics sat
    // at zero divergences. Only the absolute totals were wrong. Corrected,
    // the run this constant was last measured on is
    //
    //   50 divergences of 2389 compared (2339 matched) over 3334 statements
    //
    // where it used to read 2556 / 2506 / 3550.
    // # 50 -> 62: `executor/jointest/join` becomes measurable at all
    //
    // The topic could not be replayed before: its
    // `set @@tidb_mem_quota_query = 1 << 18` followed by
    // `desc analyze select * from t t1, .., t t6` is DELIBERATELY a runaway
    // cross join expecting `--error 8175`, and this engine's join accounted
    // none of its materialization, so the replay ran until the OS killed it.
    // Timed before and after the fix in `executor::join`: over 2 minutes with
    // no end in sight, then 140ms.
    //
    //   before: 50 divergences, this topic contributing 0 of 0 compared
    //   after:  62 divergences, this topic contributing 12 of 793 compared
    //
    // So 793 statements moved from unmeasurable to measured and 781 of them
    // MATCH. Every one of the 12 is named below; none is a regression in an
    // already-onboarded topic, and none is in the memory accounting itself.
    //
    // 3 -- the RECORDER, not this engine. `select (select /*+ INL_JOIN(x2) */
    //      ...)` and its INL_HASH/INL_MERGE twins: TiDB's own recording drops
    //      the `*/` from the derived column NAME (`/*+ INL_JOIN(x2)  x2.a`).
    //      The row values match; only the recorded label differs, and this
    //      engine prints the statement text as written.
    // 4 -- `NATURAL`/`USING` star expansion across THREE tables:
    //      `t1 join t2 using (a) right join t3`, `(t1 join t2 using (a)) join
    //      (t3 join t4 using (a))`, and `t1,t2 natural left/right join t3`.
    //      The coalescing is applied per join node here, so a third table
    //      either keeps a column TiDB coalesced away or drops one TiDB kept.
    //      Same class as `driver::from`'s stated per-node `coalesced` seam.
    // 2 -- a correlated scalar subquery over an EMPTY outer row set:
    //      `select count(1), (select count(1) from t2 where t2.a > t1.a) from
    //      t1 where t1.a = 100` -- TiDB reports the correlated column NULL,
    //      this engine reports 0. The aggregate is right; what is wrong is
    //      that a scalar subquery with no outer row must be NULL, not the
    //      aggregate's own empty-set identity.
    // 2 -- prepared-statement column LABELS: `execute stmt1 using @a` names
    //      the column `m1.a / 2` where TiDB names it `a / 2`. Values match.
    // 1 -- an ACCESS PATH: `t1 left outer join t2 on t1.a=t2.a and t1.a!=3`
    //      gives TiDB `TableRangeScan range:[-inf,3), (3,+inf]` and this
    //      engine a `TableFullScan`. A `!=` is not turned into its two ranges
    //      yet -- the ranger seam, not the join.
    //
    // 3 + 4 + 2 + 2 + 1 = 12.
    //
    // The last 2 arrived with the `--enable_warnings` split, which turned 28
    // statements that had been dismissed as "recorded SHOW WARNINGS block"
    // into 23 compared statements and 5 OutOfDomain skips: compared rose
    // 3891 -> 3914 and matched rose 3829 -> 3841, so every one of these was
    // previously UNMEASURED, not previously passing. They are:
    //
    // 1 -- a MISSING WARNING: `Warning 1815 hint INDEX_LOOKUP_PUSHDOWN is
    //      inapplicable, the global index in partition table is not
    //      supported` -- an unusable hint is dropped here without reporting
    //      that it was. This is the planner's hint-applicability report, a
    //      different mechanism from the sysvar warnings below.
    //
    //      The nine sysvar `Warning 1292 Truncated incorrect <var> value:
    //      '<v>'` cases that stood beside it are FIXED: the validator had
    //      always computed the clamp AND a `truncated` flag, and only the
    //      flag was being discarded on the way to the statement. See
    //      `Session::warn_truncated_var`.
    //
    //  1 -- a COLUMN NAME: `SELECT * FROM (select null) v NATURAL LEFT JOIN
    //       (select null) v1` is headed `NULL` by TiDB and `null` here. The
    //       script wrote `null` in lower case, so this is not an echo of the
    //       source text: TiDB names a bare NULL literal's column `NULL`
    //       regardless, and this tier derives the name in lower case.
    // 73 - 9 = 64: the nine sysvar 1292 warnings above are now raised, and
    // `session/variable` diverges nowhere.
    const KNOWN_DIVERGENCES: usize = 64;

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

/// Lists every topic this reader cannot ALIGN, with the cause for each.
///
/// An unaligned topic is worse than a refused one. A refused statement is
/// counted in a named [`SkipClass`] and appears in every total this driver
/// prints; a topic that does not align is not compared, not refused, and not
/// counted -- it is simply ABSENT from every number the survey reports, and
/// nothing in a green run points at it. That is the same shape as the
/// instrument bugs this ring has already found, so the inventory is kept as a
/// standing tool rather than being rediscovered each time.
///
/// This is cheap on purpose: it reads the two files and runs `parse_test` +
/// `align`, which is where alignment is decided, so it does not need to
/// execute a single statement and finishes in under a second over all topics.
///
/// ```sh
/// cargo test -p difftest-result-tests --test integration_diff -- \
///   --ignored --nocapture survey_unaligned
/// ```
///
/// # The inventory, and what is left in it
///
/// The first run of this tool found 15 unaligned topics in three groups. The
/// three whose ECHO did not line up were the reader's own bug and are fixed
/// (see `mysqltest_script::strip_trailing_comment` and the empty-statement
/// arm); they were 2,286 statements that no number here had ever counted:
///
///   `planner/core/plan_cache`  1,354 statements, 1,128 matched, 117 diverged
///   `executor/issues`            474 statements,   346 matched,  25 diverged
///   `executor/autoid`            458 statements,   356 matched,  68 diverged
///
/// (`executor/autoid` has since moved to 380 matched, 20 diverged: the
/// allocator now bounds an id by the COLUMN's type as Go's
/// `setDatumAutoIDAndCast` does, which removed 24 inserts this engine
/// accepted that TiDB refuses and the 24 selects that showed the extra row.)
///
/// They are ALIGNED, not onboarded: onboarding is what puts a topic on the
/// gate, and each would carry a divergence list that has to be classified
/// first. They are now visible to [`survey_unonboarded_topics`] like any other
/// candidate, which is the point -- `plan_cache` at 1,128 matched is among the
/// largest candidates in the suite.
///
/// The remaining 12 are NOT the reader's bug, and each is named here so no
/// later unit has to rediscover that:
///
///   8 topics whose `r/*.result` is not valid UTF-8, because the recording
///     holds deliberately invalid byte sequences -- these are the charset
///     topics (`executor/charset`, `executor/insert`,
///     `expression/charset_and_collation`, `new_character_set`,
///     `new_character_set_builtin`, `planner/core/integration`,
///     `planner/core/integration_partition`,
///     `planner/core/tests/prepare/issue`). Reading them means carrying the
///     recording as BYTES rather than as `String`, which is a real change to
///     every comparison here, not a parser fix.
///   4 topics with no `r/*.result` at all: `collation_agg_func`,
///     `collation_check_use_collation`, `collation_misc`,
///     `collation_pointget`. Each has TWO recordings instead --
///     `r/<topic>_disabled.result` and `r/<topic>_enabled.result`, one per
///     `new_collations_enabled_on_first_bootstrap` setting -- so the topic is
///     not a single `t`/`r` pair at all. Aligning them means choosing which
///     recording the replay's own collation configuration corresponds to,
///     which is a claim about this engine's collation support, not a parser
///     fix.
#[test]
#[ignore = "inventory tool: lists every topic the reader cannot align, with its cause"]
fn survey_unaligned_topics() {
    let dir = integrationtest_dir();
    let mut unaligned = 0usize;
    let mut aligned = 0usize;
    for topic in all_topics() {
        let script = match fs::read_to_string(dir.join(format!("t/{topic}.test"))) {
            Ok(text) => text,
            Err(e) => {
                unaligned += 1;
                eprintln!("UNALIGNED  {topic}: read .test: {e}");
                continue;
            }
        };
        let recorded = match fs::read_to_string(dir.join(format!("r/{topic}.result"))) {
            Ok(text) => text,
            Err(e) => {
                unaligned += 1;
                eprintln!("UNALIGNED  {topic}: read .result: {e}");
                continue;
            }
        };
        match parse_test(&script).and_then(|items| {
            let count = items.len();
            align(&items, &recorded).map(|_| count)
        }) {
            Ok(count) => {
                aligned += 1;
                if std::env::var_os("INTEGRATION_SHOW_ALIGNED").is_some() {
                    eprintln!("aligned    {topic}: {count} items");
                }
            }
            Err(reason) => {
                unaligned += 1;
                eprintln!("UNALIGNED  {topic}: {reason}");
            }
        }
    }
    eprintln!("{aligned} topics align, {unaligned} do not");
}

/// Every topic in the suite, as `t/<topic>.test` relative paths without the
/// extension.
fn all_topics() -> Vec<String> {
    let root = integrationtest_dir().join("t");
    let mut topics = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(at) = stack.pop() {
        for entry in fs::read_dir(&at).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "test") {
                topics.push(
                    path.strip_prefix(&root)
                        .unwrap()
                        .with_extension("")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    topics.sort();
    topics
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
/// Most topics report a status line; the rest are `UNALIGNED` on this driver's
/// own limits (a `.result` recording that is not valid UTF-8, an echo sequence
/// that does not line up, or a `connect` whose account the engine cannot
/// authenticate -- see `mysqltest_connections`). THREE topics still CRASH, down
/// from ten, and what is left is ONE root cause:
///
/// A chunk column's shape comes from a FIELD TYPE while `Chunk::append_datum`
/// dispatches on the DATUM KIND, so the two must agree -- and an expression's
/// INFERRED return type does not always match the datum it evaluates to.
/// `executor/ddl`, `planner/core/casetest/integration` and
/// `planner/core/issuetest/planner_issue` put an 8-byte value in a
/// variable-length column; `expression/issues` reaches `append_datum` with a
/// decimal whose text is not a number at all (`I311`).
///
/// `executor/ddl`'s is the clearest statement of the cause, and it is a
/// WRONG-VALUE bug, not only a crash. Its view
/// `select 'a', 'bbb...' from t union select 'ccc...', count(distinct ...)`
/// unions a string column with a `COUNT`. Go's `unionJoinFieldType` merges the
/// two branches' column types and CASTS each branch to the merged type, so
/// TiDB records the count as the STRING `1`; this tier takes the set
/// operation's column metadata from the FIRST term with no unification (which
/// `driver::set_opr` already names as its own deferred item) and then hands an
/// `Int` datum to a var-length column. Casting later terms to the first term's
/// type would silence the panic and be WRONG in the mirror case -- first term
/// `int`, second `varchar`, where Go merges to `varchar` -- so the fix is the
/// type unification itself, not a cast at the append.
///
/// One topic no longer aborts but does not FINISH the survey's 30s child
/// budget: `expression/issues` runs long and then reaches the `I311` decimal
/// cell above.
///
/// `executor/jointest/join` was the second, and it is FIXED. Its 21-table
/// join did not finish in 400s, which was read here as an unbounded
/// join-order search; timing the statement at every prefix length disproved
/// that (there is no join-order search in this engine, and the cost doubled
/// per table added, which is the cross product and is order-independent).
/// The cause was a comma join whose equalities were all in `WHERE`, so no
/// join node had an `ON` to hash on -- see `driver::predicate_push_down` and
/// the `many_table_join` tests. The topic's next obstacle was its DEBUG
/// stack overflow at 21 tables, which `on_deep_stack` covers here and the
/// survey's child processes do not.
///
/// It had a THIRD non-termination after that one, and it is also fixed: the
/// script sets `tidb_mem_quota_query = 1 << 18` and then runs a six-way self
/// cross join under `--error 8175` -- a statement TiDB is expected to CANCEL.
/// This engine's join accounted none of its materialization, so it ran until
/// the OS killed it (timed here: past 2 minutes with no end, against 140ms
/// after). `executor::join` now consumes against the statement's budget, and
/// the topic is ONBOARDED in `TOPICS` rather than merely survivable.
///
/// # Out-of-domain refusal causes, ranked
///
/// `INTEGRATION_SHOW_OUT_OF_DOMAIN=1` prints every refused statement with the
/// error that refused it (see `compare`'s `SkipClass::OutOfDomain` arm); this
/// is the ranking of that output's causes, over 6,755 out-of-domain
/// statements, as a work list for the next capability increment:
///
/// 2,187 `table not found in catalog` -- a CASCADE from an earlier refusal in
///       the same script leaving a table unregistered, not an independent gap.
///       See the cascade ranking below, which is the number that decides what
///       is worth building.
///   307 `this ALTER TABLE action is not supported yet` -- 184 of them
///       PARTITION actions (reorganize/exchange/add/drop/truncate/coalesce,
///       `PARTITION BY`, `REMOVE PARTITIONING`), then 35 CACHE/NOCACHE, 14
///       `ADD CONSTRAINT`, 14 foreign-key add/drop.
///   230 `this statement kind (ADMIN AnalyzeTable) is not supported yet`.
///   208 `expression form is not yet supported by the rewriter`.
///   186 `this builtin is not yet built for chunk evaluation`.
///   162 `unknown table` -- the cascade's second spelling.
///     0 `generated columns are not supported yet` -- GRADUATED; was 175 at
///       its own re-measure. See "The generated-column increment, measured".
///   148 `this statement kind (ADMIN AdminCheck) is not supported yet`.
///   113 `an expression index is not supported yet`.
///    88 `EXPLAIN of a WITH clause is not supported yet`.
///    79 `CHECK constraints are only modelled with
///       tidb_enable_check_constraint off`.
///    56 `an expression DEFAULT is not supported yet`.
///    46 `this ALTER TABLE table option is not supported yet`.
///    42 `a prefix-length index is not supported yet`.
///    41 `CREATE TABLE LIKE is deferred`.
///    28 `a prefix-length primary key is not supported yet`.
///
/// # The cascade, blamed on the CREATE that was refused
///
/// A flat count of refusals says which gap is COMMON; it does not say which
/// gap is EXPENSIVE. The 2,187 + 162 cascade statements are the expense, and
/// they can be attributed: for each of them, find the table it names and the
/// cause that refused that table's `CREATE` earlier in the same script. 1,891
/// of 2,351 attribute cleanly (the rest name a table created in another
/// topic, or a multi-line `CREATE` this attribution does not reassemble).
/// Ranked by DOWNSTREAM statements unblocked, which is the opposite order
/// from the flat count above:
///
///   714 `generated columns are not supported yet` -- by a factor of four the
///       most expensive absence in the whole suite. 155 refusals of its own,
///       and 714 later statements that never run because of them.
///   159 `an expression DEFAULT is not supported yet`.
///   155 `a prefix-length primary key is not supported yet`.
///   143 `an expression index is not supported yet`.
///    74 `CHECK constraints are only modelled with
///       tidb_enable_check_constraint off`.
///    38 `a prefix-length index is not supported yet`.
///    35 `CREATE TABLE LIKE is deferred`.
///
/// Note what is NOT here: `ALTER TABLE`, in any form. A refused `ALTER` leaves
/// the table registered, so it costs its own statement and nothing more --
/// which is why the metadata-only ALTER unit that added `ddl/db_rename` to
/// [`TOPICS`] moved the ALTER refusals from 342 to 307 and the cascade by
/// EXACTLY ZERO. Partition DDL is the largest ALTER group by far and would be
/// the same shape of win: many statements, no cascade. The cascade is bought
/// by CREATE TABLE capability, and generated columns are where it is
/// concentrated -- and that prediction was then paid out in full; see the
/// measurement below.
///
/// Every `this statement kind (...) is not supported yet` and
/// `this DDL/DML statement kind (...) is not supported yet` message above
/// names the AST variant it refused -- the generic, unnamed
/// `"this statement kind is not supported yet"` message (1,008 statements
/// under the old wording, the single largest opaque group in the whole
/// list) no longer exists as of the diagnostic naming pass in
/// `tidb_session::dispatch`/`explain_arm`. Naming the kind did not change
/// which statements are accepted or refused.
///
/// # The generated-column increment, measured
///
/// Generated columns were the single most EXPENSIVE absence on this list: not
/// for their own 175 refusals but for the cascade behind them, since a refused
/// `CREATE TABLE` puts every later statement touching that table out of
/// domain. Landing them (`tidb_executor::generated_column`) was measured by
/// running this survey immediately before and after, counting the refusal
/// detail lines of a full `INTEGRATION_SHOW_OUT_OF_DOMAIN=1` run:
///
///   `generated columns are not supported yet`  175 -> 0
///   `table not found in catalog`             2,248 -> 1,500  (-748)
///   `unknown table`                            164 ->    65  (-99)
///   out-of-domain statements, all causes     6,977 -> 5,933  (-1,044)
///   statements matching TiDB's recording    21,829 -> 22,600 (+771)
///
/// So the cascade was real and the ratio was as predicted: 155-175 own
/// refusals bought roughly 850 statements. (The absolute counts here come
/// from that one pair of runs and are directly comparable to each other;
/// they need not line up with the ranked list above, which was measured
/// separately.)
///
/// A statement whose `EXPLAIN` is refused is named after its INNER
/// statement kind, not just "EXPLAIN": `explain_stmt` produces
/// `"EXPLAIN [ANALYZE] of <kind> is not supported yet"` for every wrapped
/// kind it does not run, following the shape its own `EXPLAIN of a WITH
/// clause` / `EXPLAIN of a set operation` messages already used. Of the
/// 1,000 EXPLAIN-leading out-of-domain statements in this corpus, none
/// actually land on that message or on the old generic one: `EXPLAIN`'s own
/// dispatch already named every refusal specifically, so the EXPLAIN-leading
/// statements are refused for the SAME reasons their inner statement would
/// be run alone -- mostly the `table not found in catalog` cascade (481),
/// `EXPLAIN of a WITH clause` (85), and `derived tables are not supported
/// yet` (84).
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
        // A topic already on the gate is not a candidate, and printing it as
        // one is how `session/variable` and `table/cache` each got onboarded
        // twice: the second unit read this survey's output, saw a topic that
        // replayed clean, and added it. Naming the onboarded ones here is the
        // guard at the point where the mistake was actually made --
        // `assert_topics_are_unique` is the backstop behind it.
        if TOPICS.iter().any(|(onboarded, _)| onboarded == topic) {
            eprintln!("ONBOARDED  {topic}");
            continue;
        }
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
