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
use std::path::PathBuf;

use enrolled_topics::TOPICS;
use integration_plan_property::{access_property, plan_statement, PlanStatement};
use mysqltest_connections::Connections;
use mysqltest_script::{align_bytes, parse_test, recording_path, split_warnings_bytes, Item, Stmt};
use tidb_datatype::Datum;
use tidb_session::{Session, StmtOutput};

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
/// replay's own reader and prints the current reach. The latest measured line
/// is `warning gate reaches 62 of 11465 statements across 110 topics`; it is
/// the reason a fix that adds a real warning outside those 62 can move neither
/// ratchet.
///
/// The blind spot is in the RECORDING, not in this reader, and that is the
/// part worth knowing before anyone tries to close it. mysqltest writes a
/// warning into `.result` only under `--enable_warnings` or for an explicit
/// `show warnings;` (7 more statements, compared as ordinary rows). Everywhere
/// else TiDB's warnings were never captured: `executor/analyze` records
/// nothing for `set @@session.tidb_enable_fast_analyze=1` and only shows that
/// it warns because the script asks on the next line. So there is no reader
/// change that widens this gate against the recordings we have -- comparing
/// warnings outside the printed gate needs TiDB's warnings from somewhere else, a
/// re-recording or a live server, not a better comparison.
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
    // Re-read, not patched. The enrollment census added 57 topics and 3,865
    // statements (6,882 + 3,865 = 10,747), and the WARNING half of the move is
    // TWO of them: `expression/noop_functions` runs 17 statements under
    // `--enable_warnings` -- it is a topic about which statements raise a
    // warning instead of an error, so a high ratio is what it is FOR -- and
    // `table/index` runs 1. 31 + 17 + 1 = 49. The other 55 new topics add
    // 3,787 statements and NOT ONE warning-gated statement, so the gate's
    // reach per topic did not change; it was extended by exactly the two
    // topics that use the directive.
    //
    // Re-read again after batch57's three enrollments (`window_function`,
    // `executor/expand`, `session/vars`), which add 225 statements
    // (10,747 + 225 = 10,972). The WARNING half of that move is ONE of the
    // three: `session/vars` runs 8 statements under `--enable_warnings`, which
    // is what a topic about variable behavior would: the warning is how a
    // variable reports that it refused or clamped a value. `window_function`
    // and `executor/expand` add 98 statements and NOT ONE warning-gated
    // statement. 49 + 8 = 57.
    //
    // The harness-alignment enrollment adds
    // `planner/core/integration_partition`, whose output line is `5 of 493`.
    // The resulting current oracle line is `warning gate reaches 62 of 11465
    // statements across 110 topics`.
    assert_eq!(
        (covered, total),
        (62, 11465),
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

/// One result cell, as the CLIENT receives it.
///
/// A recording is what mysql-tester read off the wire, so the comparison has
/// to render the same way the wire does --
/// [`tidb_protocol::format_datum_text`], which the server's own row writer
/// uses. Rendering the `Datum` alone agrees for most types and then quietly
/// disagrees for the ones whose text depends on the COLUMN: a `FLOAT` prints
/// `2.77311e38` where the value alone prints every digit, and only the
/// column's declared width says which. A harness that cannot tell those apart
/// reports a divergence for a correct answer -- or, worse, a match for a
/// wrong one.
fn cell_bytes(field_type: &tidb_datatype::FieldType, value: &Datum) -> Vec<u8> {
    if value.is_null() {
        return b"NULL".to_vec();
    }
    // `table_is_empty` is Go's `ColumnInfo.Table == ""`. This tier does not
    // carry the source table on a result column, and the flag only decides
    // whether a FIXED decimal precision is honoured, so the conservative
    // reading -- treat every column as computed, format at full precision --
    // is the one that cannot silently truncate a cell.
    let column = tidb_protocol::TextColumn::from_field_type(field_type, true);
    match tidb_protocol::format_datum_text(column, value) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => b"NULL".to_vec(),
        // A value the protocol cannot render is not something to paper over
        // with a different spelling: fall back to what the datum says, which
        // is what this did for every cell before.
        Err(_) => value
            .to_bytes()
            .unwrap_or_else(|_| value.label().into_bytes()),
    }
}

/// Renders a result set as the recorder does: a tab-separated header of column
/// names, then one tab-separated line per row.
fn render_rows(
    columns: &[(String, tidb_datatype::FieldType)],
    rows: &[Vec<Datum>],
) -> Vec<Vec<u8>> {
    let mut out = vec![columns
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>()
        .join("\t")
        .into_bytes()];
    out.extend(rows.iter().map(|row| {
        let mut line = Vec::new();
        for (index, value) in row.iter().enumerate() {
            if index > 0 {
                line.push(b'\t');
            }
            match columns.get(index) {
                Some((_, field_type)) => line.extend(cell_bytes(field_type, value)),
                None => line.extend(
                    value
                        .to_bytes()
                        .unwrap_or_else(|_| value.label().into_bytes()),
                ),
            }
        }
        line
    }));
    out
}

fn display_line(line: &[u8]) -> String {
    String::from_utf8_lossy(line).into_owned()
}

fn display_block(lines: &[Vec<u8>]) -> String {
    lines
        .iter()
        .map(|line| display_line(line))
        .collect::<Vec<_>>()
        .join(" / ")
}

/// Compares one statement's outcome against its recorded block.
///
/// Returns `Ok(kind)` when the outcome matches, `Err(Some(detail))` for a
/// divergence to report, and `Err(None)` when the statement was skipped (its
/// class already recorded).
fn compare(
    session: &mut Session,
    stmt: &Stmt,
    recorded: &[Vec<u8>],
    report: &mut TopicReport,
) -> Result<MatchKind, Option<String>> {
    // `--enable_warnings` appended this statement's `SHOW WARNINGS` to its own
    // output rather than rewriting it, so the block is two blocks. Comparing
    // the halves separately is what makes these statements comparable at all,
    // and it puts the warning texts themselves under the gate.
    let (rows, warnings) = if stmt.warnings {
        let (rows, warnings) = split_warnings_bytes(recorded);
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
        (Ok(kind), None) => {
            survey_unwatched_warning(session, stmt);
            Ok(kind)
        }
        (outcome, _) => outcome,
    }
}

/// Records that a statement OUTSIDE the warning gate raised a warning here.
///
/// The gate reaches 62 of 11,465 statements
/// ([`warning_comparison_covers_only_enable_warnings_statements`]); for the
/// rest the replay compares rows only, so a warning is invisible either way.
/// This measures one half of that blind spot -- what THIS engine raises where
/// nothing is checked. `INTEGRATION_SURVEY_UNWATCHED_WARNINGS=1` FIRST printed
/// 18 such statements: five deprecated-sysvar notices, eleven `Truncated
/// incorrect DOUBLE value` from string-vs-number comparisons, and a
/// `tidb_max_chunk_size` clamp. None of the 18 was checked against TiDB by
/// anything in this suite.
///
/// THAT 18 IS THE PRE-`Note` NUMBER AND IS KEPT ONLY AS HISTORY. Giving the
/// session Go's third warning level turned the whole `IF EXISTS` family from
/// silence into notes, and the survey went to 155 statements -- 216 of the new
/// lines being `Note 1051`. The rise is the instrument becoming able to see a
/// class it had no way to represent, not the engine getting noisier: with two
/// levels there was nowhere to demote a suppressed error to, so Go's notes were
/// dropped rather than reported. Read a survey count as a measurement OF THE
/// TREE THAT PRODUCED IT; it moves whenever this engine's diagnostics do, which
/// is why it is a survey and not a ratchet.
///
/// Every one of the 18 has since been put to `gorun` -- a real TiDB session --
/// and the answers are banked here, because the suite still cannot ask them:
///
/// * The seven `set` sites AGREE exactly, text and level:
///   `set [global] tidb_enable_table_partition=off` ->
///   `Warning 1105 tidb_enable_table_partition is always turned on. ...`;
///   `set [global] tidb_enable_list_partition=on|1` ->
///   `Warning 1681 tidb_enable_list_partition is deprecated ...`;
///   `set @@session.tidb_enable_fast_analyze=1` ->
///   `Warning 1105 the fast analyze feature has already been removed ...`;
///   `set @@tidb_max_chunk_size=2` ->
///   `Warning 1292 Truncated incorrect tidb_max_chunk_size value: '2'`.
///   They are `Warning`, not `Note`: Go reaches them through
///   `StmtCtx.AppendWarning` (`pkg/sessionctx/variable/sysvar.go`), and
///   `AppendNote` -- the third level this engine cannot represent -- is never
///   on that path.
/// * The two varchar sites AGREE: `select * from t where a > 0` over
///   `('aaa'),('bbbb'),('ccc'),('dfg'),('kkkk'),('10')` raises five
///   `Warning 1292 Truncated incorrect DOUBLE value` in both, one per row that
///   does not parse, and none for `'10'`.
/// * The three `a > '10ab'` sites DIVERGE ON COUNT. TiDB raises the truncation
///   TWICE, the same two for `t`, `trange` and `thash` alike, because the
///   string is folded against the int column ONCE while the comparison is
///   refined -- it never reaches a row. This engine raises it PER ROW SCANNED:
///   11, 5 and 11. Same text, same level, wrong multiplicity.
/// * `select hex(t0.c1) from t0 where 0 in (select t0.c1 from t0)` over a
///   `blob` holding `'gO'` and `'W'` DIVERGES IN THE OTHER DIRECTION: TiDB
///   raises FOUR (`'W'`, `'W'`, `'gO'`, `'gO'`), this engine only the two for
///   `'gO'`. It is the one place in the 18 where TiDB says more than we do.
///   FIXED: `IN` no longer stops its probe at the first match, because the
///   coercion the remaining values would run is observable -- see
///   `tidb_session::tests_in_list_full_evaluation`. The COUNT should now be
///   four here. The ORDER should still differ: TiDB's `'W'`, `'W'`, `'gO'`,
///   `'gO'` is the vectorized loop's args-outer/rows-inner grouping, while
///   this engine evaluates row-at-a-time and interleaves the values. Nothing
///   in this suite compares warning order, so the survey line is where that
///   residual is visible.
///
/// It is OFF by default, and that is not tidiness. Turning it on moved the
/// divergence count from 64 to 66: the two `select @@last_plan_from_cache`
/// statements in `sessionctx/setvar` answered 0 instead of 1, because the
/// extra `SHOW WARNINGS` ran between the prepared statement and the read and
/// became the last plan. So `SHOW WARNINGS` is NOT the observationally neutral
/// probe [`warning_difference`] calls it -- it is neutral only for the 28
/// statements it happens to be asked on today. Widening the gate has to read
/// the warning count off the wire instead, and that is a change to the shared
/// reader, not a line in this survey.
fn survey_unwatched_warning(session: &mut Session, stmt: &Stmt) {
    if std::env::var_os("INTEGRATION_SURVEY_UNWATCHED_WARNINGS").is_none() {
        return;
    }
    let Ok(StmtOutput::Rows { rows, .. }) = session.run_with_columns("SHOW WARNINGS") else {
        return;
    };
    if rows.is_empty() {
        return;
    }
    let texts = rows
        .iter()
        .map(|row| row.iter().map(cell).collect::<Vec<_>>().join("\t"))
        .collect::<Vec<_>>()
        .join(" / ");
    eprintln!("UNWATCHED WARNING: {} -> {texts}", stmt.sql);
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
fn warning_difference(session: &mut Session, want: Option<&[Vec<u8>]>) -> Option<String> {
    let ours = match session.run_with_columns("SHOW WARNINGS") {
        // `SHOW WARNINGS` answers `Level`, `Code`, `Message` -- and its rows
        // go through the same client rendering every other result set does.
        Ok(StmtOutput::Rows { columns, rows }) => rows
            .iter()
            .map(|row| {
                let mut line = Vec::new();
                for (index, value) in row.iter().enumerate() {
                    if index > 0 {
                        line.push(b'\t');
                    }
                    match columns.get(index) {
                        Some((_, field_type)) => line.extend(cell_bytes(field_type, value)),
                        None => line.extend(
                            value
                                .to_bytes()
                                .unwrap_or_else(|_| value.label().into_bytes()),
                        ),
                    }
                }
                line
            })
            .collect::<Vec<Vec<u8>>>(),
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
            display_block(want)
        },
        if ours.is_empty() {
            "<none>".to_owned()
        } else {
            display_block(&ours)
        }
    ))
}

/// Compares one statement's own output -- rows or rejection -- against the
/// recorded block, with any appended warnings block already removed.
/// Resolves a `load stats 'relative/path.json'` statement against
/// `tests/integrationtest/`, the directory the recording's CLIENT ran from.
///
/// This is a HARNESS concern, not an engine one, and the boundary is Go's
/// own: TiDB's executor never opens the file -- the connection layer fetches
/// the bytes from the client over the local-infile protocol
/// (`pkg/executor/plan_replayer.go`'s `FileTransInConnHandlers`), so a
/// relative path in a script resolves against mysql-tester's working
/// directory, which `run-tests.sh` sets to `tests/integrationtest/`. This
/// replay's engine reads the path as given, so the harness substitutes the
/// absolute path the recording's client would have opened. An already
/// absolute path passes through untouched.
///
/// The fixtures themselves ship zipped (`s.zip`; `run-tests.sh` line 129
/// runs `unzip -qq s.zip` before any test), so the first `load stats` also
/// unpacks the archive if `s/` is not there yet.
fn rewrite_load_stats_path(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    if !trimmed
        .get(..10)
        .is_some_and(|head| head.eq_ignore_ascii_case("load stats"))
    {
        return None;
    }
    let first = trimmed.find('\'')?;
    let last = trimmed.rfind('\'')?;
    if last <= first {
        return None;
    }
    let path = &trimmed[first + 1..last];
    if std::path::Path::new(path).is_absolute() {
        return None;
    }
    let dir = integrationtest_dir();
    if path.starts_with("s/") {
        ensure_stats_fixtures_unzipped(&dir);
    }
    Some(format!("load stats '{}'", dir.join(path).display()))
}

/// Unpacks `tests/integrationtest/s.zip` into `s/` once, the way
/// `run-tests.sh` does before starting mysql-tester.
///
/// Concurrency is the reason this is not a plain `unzip -o`: the full replay
/// runs topics in parallel and `replay_in_child` adds separate PROCESSES, so
/// two racers must never read each other's half-written files. Each racer
/// extracts into its own scratch directory and installs with one `rename`;
/// the loser of the rename finds `s/` present and discards its copy.
fn ensure_stats_fixtures_unzipped(dir: &std::path::Path) {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let target = dir.join("s");
        if target.exists() {
            return;
        }
        let scratch = dir.join(format!(".s_unzip_{}", std::process::id()));
        let unzipped = std::process::Command::new("unzip")
            .arg("-qq")
            .arg(dir.join("s.zip"))
            .arg("-d")
            .arg(&scratch)
            .status();
        match unzipped {
            Ok(status) if status.success() => {
                // A losing rename means another process installed `s/` while
                // we extracted; its copy is identical, so ours just goes.
                if fs::rename(scratch.join("s"), &target).is_err() && !target.exists() {
                    eprintln!("could not install {}", target.display());
                }
                let _ = fs::remove_dir_all(&scratch);
            }
            outcome => {
                // Leave the statement to fail on the missing file, which the
                // report then counts and names instead of hiding.
                eprintln!("unzip s.zip failed: {outcome:?}");
                let _ = fs::remove_dir_all(&scratch);
            }
        }
    });
}

fn compare_output(
    session: &mut Session,
    stmt: &Stmt,
    recorded: &[Vec<u8>],
    report: &mut TopicReport,
) -> Result<MatchKind, Option<String>> {
    // The one statement class whose TEXT the harness owns a piece of: a
    // relative `load stats` path belongs to the recording client's working
    // directory, resolved here so the engine below can take it literally.
    let resolved_load_stats = rewrite_load_stats_path(&stmt.sql);
    let stmt_sql: &str = resolved_load_stats.as_deref().unwrap_or(&stmt.sql);
    if let Some(reason) = stmt.blocker {
        // The recorder rewrote this statement's output, so nothing about it is
        // comparable -- but mysql-tester still RAN it, and what it did is what
        // the statements after it read. Skipping the RUN as well silently
        // rewinds the session: in `session/variable`, `set @@global.x = 1.1`
        // sits under `--enable_warnings`, so the clamped value it stores never
        // happened here and the next four `select @@global.x` diverged on a
        // value this driver had suppressed rather than on anything the engine
        // does. Run it, discard the outcome, and count the skip.
        drop(session.run_with_columns(stmt_sql));
        report.skip(SkipClass::RecorderRewroteOutput(reason));
        return Err(None);
    }
    let recorded_error = stmt.expect_error
        || recorded
            .first()
            .is_some_and(|line| line.starts_with(b"Error "));

    // A plan statement runs as this tier's own default EXPLAIN whatever format
    // the recording asked for -- see `PlanStatement::RunDefaultExplain`.
    let plan = plan_statement(stmt_sql);
    let sql = match &plan {
        Some(PlanStatement::NotComparable(reason)) if !recorded_error => {
            report.skip(SkipClass::PlanFormatNotComparable(reason));
            return Err(None);
        }
        // TiDB PLANNED this statement even though its output is not a tree
        // this reader compares, and planning has observable side effects: the
        // hint-deprecation warning a following `show warnings` reads. Run the
        // default-format spelling for the effects, discard the output.
        Some(PlanStatement::RunAndDiscard { sql, reason }) if !recorded_error => {
            drop(session.run_with_columns(sql));
            report.skip(SkipClass::PlanFormatNotComparable(reason));
            return Err(None);
        }
        Some(PlanStatement::RunDefaultExplain(sql)) => sql.as_str(),
        _ => stmt_sql,
    };
    // A statement that PANICS takes the process with it, so its identity is
    // not in any report -- and attributing a crashing topic means knowing
    // which statement it died on. `INTEGRATION_TRACE_SQL=1` names each
    // statement BEFORE it runs, so the last line printed is the one that
    // crashed. Off by default: a replay prints 30,000 lines with it on.
    let traced = std::env::var_os("INTEGRATION_TRACE_SQL").is_some();
    if traced {
        eprintln!("SQL> {sql}");
    }
    let started = std::time::Instant::now();
    let outcome = session.run_with_columns(sql);
    if traced {
        eprintln!("SQL< {}ms", started.elapsed().as_millis());
    }
    match (outcome, recorded_error) {
        // TiDB rejected it and so did we. The wording is TiDB's; only the
        // rejection is asserted.
        (Err(_), true) => {
            report.skip(SkipClass::BothRejected);
            Err(None)
        }
        (Ok(_), true) => Err(Some(format!(
            "  tidb: {}\n  rust: accepted the statement",
            recorded
                .first()
                .map_or_else(|| "<error>".to_owned(), |line| display_line(line))
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
            let mut theirs: Vec<Vec<u8>> = recorded.to_vec();
            if plan.is_some() {
                // Drop the header on both sides: a plan's columns are fixed,
                // and only the access rows carry the guarded property.
                let theirs_text = theirs
                    .iter()
                    .map(|line| display_line(line))
                    .collect::<Vec<_>>();
                let ours_text = ours
                    .iter()
                    .map(|line| display_line(line))
                    .collect::<Vec<_>>();
                let want = access_property(&theirs_text[1.min(theirs_text.len())..]);
                let got = access_property(&ours_text[1..]);
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
            let ours: Vec<Vec<u8>> = ours
                .iter()
                .flat_map(|line| line.split(|byte| *byte == b'\n').map(|part| part.to_vec()))
                .collect();
            if ours == theirs {
                Ok(MatchKind::Rows)
            } else {
                Err(Some(format!(
                    "  tidb: {}\n  rust: {}",
                    display_block(&theirs),
                    display_block(&ours)
                )))
            }
        }
        // A side effect records no output of its own.
        (Ok(_), false) if recorded.is_empty() => Ok(MatchKind::SideEffect),
        (Ok(other), false) => Err(Some(format!(
            "  tidb: {}\n  rust: {other:?}",
            display_block(recorded)
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
    let recorded_path = recording_path(&dir, topic);
    let recorded =
        fs::read(&recorded_path).map_err(|e| format!("read {}: {e}", recorded_path.display()))?;
    let items = parse_test(&script)?;
    let aligned = align_bytes(&items, &recorded)?;

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
        let outcome = compare(connections.current(), stmt, &block, &mut report);
        if matches!(outcome, Err(None)) && !stmt.expect_error {
            connections.recover_account_row_from_unsupported_create_user(&stmt.sql);
        }
        match outcome {
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
    //
    // 64 - 1 = 63: a TYPED TEMPORAL LITERAL was being lowered into an ordinary
    // `CAST`, which never fails and carries no fractional precision, so a
    // `TIMESTAMP 'lit'` this suite compares answered where TiDB refuses and
    // printed a value a digit short. See `tidb_expr::time_literal`; the whole
    // class is measured by `tests/integrationtest/t/types/time.test`, which
    // went from 56 divergences to 6 and is the topic to onboard next.
    //
    // 63 - 1 = 62: a chosen access path with NO ranges was still committed as
    // an `IndexRangeScan`, printed with an empty `range:` cell, where Go's
    // `findBestTask` returns a `PhysicalTableDual`. The rows were already
    // right on both sides, so this was a plan-text divergence only. See
    // `tidb_executor::plan_trace::PlanTrace::empty_range_table_dual`; the
    // class is measured by `tests/integrationtest/t/util/ranger.test`, whose
    // divergences went from 38 to 31 (the other 7 are in that unonboarded
    // topic).
    //
    // `util/ranger` is 28, DOWN FROM 31, and the THREE that closed were the
    // row-ORDER ones. The note that stood here had the cause wrong twice
    // over, so the correction is written down instead of the theory. They
    // were `select * from t`, `... where a < 3`, and `... where a > -1` over
    // `TestIndexRangeForBit`'s table:
    //
    //     CREATE TABLE t (a bit(1), b int) PARTITION BY HASH(a) PARTITIONS 3;
    //     insert ignore into t values(-1,-1),(0,0),(1,1),(3,3);
    //
    // TiDB answers `<0x00>,0 / <0x01>,-1 / <0x01>,1 / <0x01>,3` and this
    // engine answered the same four rows as `-1 / 0 / 1 / 3`. The old note
    // called that "the PARTITION scan order, which falls out when HASH
    // partitioning lands, and nothing in the codec or the ranger will move".
    // HASH partitioning HAD landed, and the table scan already reads one key
    // range per partition in ascending id order -- so the order should
    // already have been TiDB's. It was not, because `locateHashPartition`
    // put every row of a BIT-keyed table in `p0`: it read the datum's KIND
    // and treated anything that was not `Int`/`UInt` as zero, where Go
    // CONVERTS a non-integer kind with `ConvertTo(TypeLonglong)` first.
    // A `bit(1)` value is `KindMysqlBit`, so every row hashed to 0.
    //
    // That was a WRONG ANSWER hiding behind a row-order divergence, not a
    // plan-text gap: `select * from t partition (p1)` returned NOTHING here
    // where TiDB returns three rows, on a table this suite never asked that
    // question of. See `tidb_executor::partition_routing`, whose captures
    // (`bit(8)` over 3 partitions: p0 reads 0 and 3, p1 reads 1, p2 reads 2)
    // pin the rule.
    //
    // The NINE `partition:p0` / `partition:p0 + partition:p1` plan-text
    // divergences beside them do NOT share that cause and remain -- the old
    // note said "eleven", and the measurement is nine. Those are the planner
    // not modelling partitions at all: no static-mode PartitionUnion, no
    // hash pruning off the partition column's ranges, and no partition
    // annotation in any access object this tier prints. Three more in the
    // same block (`where a = -1` / `a = 3` / `a < -1`) are the same absence
    // seen from the other side: TiDB prunes EVERY partition away and returns
    // `TableDual rows:0` where this tier plans a scan. Twelve of
    // `util/ranger`'s 28 are that one unbuilt rung.
    //
    // Two measured negatives from that chase, so neither is repeated: the
    // `decimal unsigned` index cases at the head of the same file
    // (`TestIndexRangeForDecimal`, `a in (-1,0)` / `a > -1` / `a <= -1` over
    // `decimal unsigned` keys) diverge NOWHERE; and a decimal key takes no
    // signed-vs-unsigned fork in Go at all, since
    // `codec.go::Encoder::encode` dispatches on datum KIND and
    // `KindMysqlDecimal` has one arm that never reads `mysql.UnsignedFlag`.
    // Both facts are pinned to Go's bytes in
    // `tidb-codec/tests/unsigned_decimal_key_order.rs`.
    //
    // 62 -> 61: `points2TableRanges`' `skipNull` landed
    // (`handle_range::build_handle_ranges`), so an `IS NULL` over an integer
    // handle now drops its NULL-ended interval and plans the `TableDual rows:0`
    // TiDB records instead of a full table scan.
    //
    // 61 -> 58: Go's `PredicateSimplification` index-independent `TableDual`
    // landed (`index_range::where_is_unsatisfiable`, wired in
    // `driver::access::commit_fast_path_source`). A `WHERE` an equality proves
    // contradictory on some column now plans `TableDual rows:0` before any path
    // is costed, matching TiDB's `<not read>` for three statements:
    // `explain_easy`'s `select * from t where b = 1 and b = 2` (b is the
    // non-leading column of `idx(a, b)`, so no range path ever caught it), and
    // `partition_with_expression`'s `select * from {trange,thash} where a = 2
    // and a = 3`. All three carry no access property on either side now, so
    // they move from compared-and-diverged to the `PlanWithoutProperty` skip
    // class (`5642 -> 5639` compared): TiDB's plan has no scan node to compare
    // against, and neither does this tier's.
    //
    // 58 -> 54: Go's `buildProjectionFieldNameFromExpressions` column-name
    // rules landed (`driver::default_field_display_name`). A bare NULL literal
    // is named `NULL` (Go's `types.KindNull` arm), so `SELECT * FROM (select
    // null) v NATURAL LEFT JOIN (select null) v1` now heads its column `NULL`
    // not `null`; and a non-literal field's label has its MySQL
    // special-result-field comment markers stripped (Go's `SpecFieldPattern` +
    // `TrimComment`), so the three `select (select /*+ INL_*_JOIN(x2) */ x2.a
    // ...) from t1` labels drop their closing `*/` to match TiDB. All four are
    // `Rows`-kind, so `compared` holds at 5639 and matched rises.
    //
    // 54 -> 52: an `UPDATE`/`DELETE` now costs the index paths beside the table
    // path, the same chooser a `SELECT` reaches (Go's write plan falls through
    // to the ordinary `DataSource`). `driver::access::write_read_path` gained a
    // `WriteReadPath::IndexRanges` arm fed by `enumerate_paths` +
    // `choose_access_path`, so `delete from t1 where t1.c2 = 1` and `update t
    // ... where a = x'..' and b = 'xb'` read through their index
    // (`IndexRangeScan`) instead of a full scan. Both are `PlanProperty`-kind,
    // so `compared` holds at 5639 and matched rises.
    //
    // 52 -> 51: Go's `isPointGetPath` converts a table path whose one range is
    // a single non-null point on the integer handle to a `Point_Get`
    // (`find_best_task.go`'s `convertToPointGet`), even when a further conjunct
    // stays a filter. `driver::access::single_point_handle` detects that shape
    // in the `HandleRange` arm, so `select * from t1 where c1 = 1 and c2 > 1`
    // plans `Point_Get` instead of a `TableRangeScan` over `[1,1]`. Still
    // diverging in the same file is the appended-PK secondary-index range
    // (`c1 > 1 and c2 = 1 and c3 < 1` reads `(1 1,1 +inf]`), which needs the
    // handle appended to a non-clustered index's tail -- a documented deferred
    // in `index_range`.
    //
    // 51 -> 49: Go's `getTableScanPenalty` (`plan_cost_ver2.go`) landed in
    // `access_cost::table_scan_penalty_rows`. A full-range table scan whose
    // statistics are pseudo, stale, or outrun by `modify_count` is costed at a
    // SECOND scan's worth of rows, which is what makes real TiDB read a
    // covering index over a table the index covers: `explain_easy`'s `select *
    // from t where b in (1,2) and b in (1,3)` and `select * from t t1 where
    // not exists (select * from t t2 where t1.b = t2.b)` both read
    // `IndexFullScan idx(a, b)` now. NOTHING regressed: the divergence sets
    // before and after are a strict subset relation, which is the property
    // that mattered -- these 48 topics record ~9 full scans for every index
    // read, and a penalty that merely made indexes attractive would have
    // traded correct agreements for divergences.
    //
    // The 34 access-path divergences left are almost all ONE seam: path
    // selection runs in `driver::access::commit_fast_path_source`, which bails
    // on `single_kv_table`, so every leaf of a multi-table `FROM` reads its
    // whole table with no path costed at all. 30 of the 34 are that; of those,
    // 8 additionally need Go's index-JOIN inner side (`range: decided by
    // [eq(...)]`), which is a physical operator this tier does not build. The
    // remaining 4 are `index_lookup_pushdown(t, idx)`, a scan hint
    // `index_hints` does not resolve, which in Go sets `path.Forced` and so
    // keeps a NON-covering index alive through `skylinePruning`'s `keepIndex`.
    // 46 -> 42: a plain join whose CHILD coalesced (`USING`/`NATURAL`) now
    // takes its display order from its two children's output names, which is
    // what Go's `buildJoin` copies into a plain join's `OutputNames`. `select
    // * from t1 join t2 using (a) right join t3 on (t2.a = t3.a)` had been
    // dropping t3's column (3 headers where TiDB prints 4) and `select * from
    // t1, t2 natural left join t3` had been printing the coalesced-away one
    // (3 where TiDB prints 2) -- the two directions of one missing rule in
    // `driver::from::build_join`. All four are `Rows`-kind, so `compared`
    // holds at 5639 and matched rises.
    //
    // 42 -> 40: Go's apply deselects the aggregation's DEFAULT row
    // (`aggExecutorTreeInputEmpty` in `NestedLoopApplyExec
    // .fetchSelectedOuterRow`), so a correlated SCALAR subquery beside an
    // aggregate over an EMPTY outer answers NULL rather than the value its
    // inner would compute from the all-NULL default row: `select count(1),
    // (select count(1) from t2 where t2.a > t1.a) from t1 where t1.a = 100`
    // is `0, NULL`. Both statements are `Rows`-kind, so `compared` holds at
    // 5639 and matched rises.
    // 40 -> 39: the two STATEMENT-WIDE facts Go's access-path costing reads,
    // both of which this tier had been answering per table.
    //
    // * The columns an index must cover are the ones the `DataSource` still
    //   needs after Go's `rule_column_pruning`, which walks a correlated
    //   subquery like any other expression. This tier had been asking its
    //   EXACT column pruner instead, whose job is to narrow the scan's output
    //   and which therefore refuses any statement containing a subquery -- a
    //   refusal that reads as "every column", so no index ever covered such a
    //   statement and the full scan won by construction rather than by cost.
    //   The cost model now reads the same over-approximating leaf walk
    //   (`driver::leaf_demand`) that a leaf of a multi-table `FROM` already
    //   used.
    // * `getTableScanPenalty` reads `StmtCtx.GetIndexForce()`, which
    //   `stats.go` raises the moment ANY path of the statement is
    //   `path.Forced` -- so a `USE INDEX` on one table penalizes every full
    //   table scan of the statement, including one over a table no hint
    //   named. This tier had been passing the hinted table's OWN
    //   `AvailablePaths`, so the flag could never be true for a table without
    //   a hint of its own.
    //
    // MEASURED, one at a time: only the SECOND moves the count. The
    // statement that went from diverging to agreeing is `subquery`'s `select
    // t.c in (select count(*) from t s use index(idx), t t1 where ...) from
    // t`, over `t(a int primary key, b int, c int, d int, index idx(b,c,d))`
    // -- and `idx(b, c, d)` plus the integer handle covers every column that
    // table has, so it was already a candidate over the outer `t` whichever
    // column demand was asked. What it was not was CHEAPER: with five
    // analyzed rows the two paths cost the same to the cent (`explain
    // format='verbose'` prints `123.64` for both readers), the tie-break kept
    // the table path, and the `use index(idx)` on the inner `s` is the whole
    // reason Go reads the index over the outer `t`. Verified through
    // `rust/difftests/gorun` in both directions: delete the hint, or write it
    // as `ignore index(idx)` or `use index for join(idx)` -- neither of which
    // Go turns into `path.Forced` -- and TiDB reads the table.
    //
    // The FIRST is net zero here, and is landed anyway because the pruner's
    // refusal is not Go's rule and the cost model should not inherit it. Its
    // two visible statements are both in `explain_easy`, which held at 8
    // rather than falling, and the swap is named because it is a real blind
    // spot rather than noise. `select c2 = (select c2 from t2 where t1.c1 =
    // t2.c1 order by c1 limit 1) from t1` now reads `IndexFullScan(c2)` and
    // agrees. `select (select count(1) k from t1 s where s.c1 = t1.c1 having
    // k != 0) from t1` now reads it and does NOT: Go DECORRELATES that
    // subquery into a `MergeJoin`, whose child property is `t1.c1`'s order,
    // and a table scan over the integer handle provides that order while an
    // index walk does not -- so Go's `matchProp` dimension prunes the index
    // path before cost is consulted. Confirmed by forcing the join type
    // through `gorun`: `/*+ hash_join(t1) */` removes the order requirement
    // and TiDB then reads `IndexFullScan(c2)` there too. This tier builds an
    // `Apply` and has no decorrelation, so the requirement never exists to be
    // matched; the statement had been agreeing for the wrong reason.
    //
    // Every statement named above is `PlanProperty`-kind, so `compared` holds
    // at 5639.
    // 39 -> 35: the DP join reorder (`tidb_executor::driver::join_reorder`),
    // Go's `joinReorderDPSolver`. All four are in
    // `planner/core/join_reorder_through_projection` and all four are
    // statements the topic runs at `set tidb_opt_join_reorder_threshold = 10`
    // -- the only enrolled setting under which Go reaches the DP at all
    // (`rule_join_reorder.go:374`; the shipped default of `0` sends every
    // group to the greedy solver).
    //
    // Each is `from t1, t5, (select t2.a as key_a, t2.b * 2 as doubled_b from
    // t2 join t3 on t2.a = t3.a) dt where t1.a = dt.key_a and dt.key_a = t5.a
    // [and dt.doubled_b > 100]`, recorded at
    // `r/planner/core/join_reorder_through_projection.result:1249` and
    // `:1399` as
    //
    //   MergeJoin  left key:t2.a, right key:t5.a
    //   |- TableFullScan table:t5  keep order:true
    //   `- MergeJoin  left key:t1.a, right key:t2.a
    //      |- Projection -> MergeJoin(t2.a, t3.a) -> t3, t2  keep order:true
    //      `- TableFullScan table:t1  keep order:true
    //
    // The WRITTEN tree joins `t1` to `t5` first, and nothing connects that
    // pair, so this tier hashed a cartesian product and neither `t1` nor `t5`
    // was read in any order -- which is why both had been reported as
    // `IndexFullScan table:tN, index:b(b)` against TiDB's `TableFullScan`.
    // Once the group is rebuilt as `(t1 join dt) join t5` the merge decision
    // demands each side's own key order and both leaves take the handle scan,
    // matching the recording leaf for leaf.
    //
    // The remaining 13 divergences in that topic are unrelated to join order:
    // eight are TiDB reaching an `IndexRangeScan ... range: decided by [...]`
    // (an index join's probe side) and five are a covering-index choice.
    // 35 -> 28: partition-aware plan TEXT plus the pushdown hint. Seven
    // statements across the two partition-planning topics, in four named
    // pieces; the total moved by exactly seven, so nothing outside them did.
    //
    // 1. COMMENT-style index hints now restrict the access paths (5 of the 7).
    //    Go appends `PlanHints.IndexHintList` to the very same `indexHints`
    //    slice `getPossibleAccessPaths` iterates (`planbuilder.go:1445`), so
    //    `/*+ use_index(t, i) */` and `USE INDEX(i)` are ONE rule and both set
    //    `path.Forced`. This tier had wired only the `FROM` spelling, so the
    //    comment spelling produced warnings and a plan that disregarded it.
    //    `index_lookup_pushdown` is Go's `ast.HintUse` with `PushDownLookUp`
    //    (`hint.go:945`), which is why the recording's plans read a
    //    NON-COVERING index -- `path.Forced` is exactly what carries such a
    //    path through skyline pruning's `keepIndex`.
    //
    // 2. `INDEX_LOOKUP_PUSHDOWN` on a GLOBAL index is Go's 1815 refusal
    //    (`checkIndexLookUpPushDownSupported`), and the plan that follows
    //    reads the TABLE: Go sets `hasUseOrForce`/`path.Forced` BEFORE the
    //    check and skips only the `append`, so the candidate set stays
    //    restricted, empties, and hits the "we have to use table scan"
    //    fallback. `KvIndex` gained Go's `IndexInfo.Global` to see it.
    //
    // 3. A `Batch_Point_Get` names the partitions its handles route into
    //    (`BatchPointGetPlan.AccessObject`), deduplicated, in DEFINITION order
    //    and declared case: `table:t, partition:p1,P2`.
    //
    // 4. Under `@@tidb_partition_prune_mode = 'static'` a partitioned scan
    //    fans out into one scan per SURVIVING partition under a
    //    `PartitionUnion`, each naming its own -- Go's
    //    `rule_partition_processor`. The shipped `dynamic` mode is untouched,
    //    which is why the 1,035 statements of `partition_boundaries` and the
    //    268 of `partition_with_expression` did not move.
    //
    // The three that did NOT close, each for a reason outside this seam:
    //
    // * `planner/core/partition_pruner`'s `select * from t2 where not (a < 5)`
    //   wants `p1 + p2` and gets all three. Go's `expression.PushDownNot`
    //   normalizes `NOT (a < 5)` to `ge(a, 5)` during expression REWRITING --
    //   the recording's own `Selection` reads `ge(test_partition_1.t2.a, 5)`
    //   where this tier prints `NOT (a<5)` -- and the range builder that
    //   drives pruning is handed the un-normalized form. The fan-out is right;
    //   the pruning input is not, and repairing it is a rewriter change that
    //   would move every printed `NOT` in every plan.
    // * two `executor/index_lookup_pushdown_partition` ROW-ORDER divergences,
    //   both of them a direct consequence of piece 1 above and both explained
    //   by one missing rule: Go's `IndexLookUpExecutor` returns each
    //   partition's rows in HANDLE order (it sorts the handles of a task
    //   before the table read), so `select ... from tp3` reads `4 | 1,5 | 2,6
    //   | 3` -- per-partition, handle-ascending. This tier walks the index and
    //   reads each row as it finds it, so it answers in index order. One of
    //   the two had been agreeing only because the hint was ignored and the
    //   TABLE scan, which does read per-partition in handle order, answered
    //   instead. Sorting an index lookup's handles is an executor change whose
    //   blast radius is every non-covering index read in the corpus, so it is
    //   named here rather than attempted alongside the plan text.
    //
    // 24 -> 26, and THE DEBT DID NOT GROW -- the COMPARED SET did. `compared`
    // rises 5639 -> 5641 because two statements that produced no plan at all
    // now produce one, and both land in the already-listed NO COVERING-INDEX
    // PREFERENCE class (`t1`: TiDB `IndexFullScan index:b(b)`, this tier
    // `TableFullScan`). They are the same statement, replayed twice, in
    // `planner/core/join_reorder_through_projection`:
    //
    //   explain format = 'plan_tree' select t1.a, dt.key_a, dt.sum_b from t1
    //     join (select t2.a as key_a, sum(t3.b) as sum_b
    //           from t2 join t3 on t2.a = t3.a group by t2.a with rollup) dt
    //     on t1.a = dt.key_a;
    //
    // `dt.key_a` names a derived-table column that is an ALIAS over a
    // `GROUP BY`. A grouped plain field used to report the AGGREGATION's own
    // column name (`a`) rather than the written alias, so the reference did
    // not resolve; TiDB names it `key_a`. The same fix moves `join_shape` in
    // the improving direction (182/103 -> 184/105 compared/agreeing, extras
    // unchanged), and no previously compared statement moved: every other
    // topic's matched/diverged/skipped triple is identical.
    //
    // THE ENROLLMENT CENSUS (batch46): 24 -> 71, and every one of the 47 new
    // ones is carried in by a topic this batch ONBOARDED, with its cause named
    // in that topic's own entry in `enrolled_topics::TOPICS`. The 24 above did
    // not move and no new divergence appeared inside them: 5,639 of 6,882
    // compared became 7,875 of 10,747, and 24 + 47 = 71 exactly, so the two
    // halves of the corpus do not interact.
    //
    // The 47, grouped by cause rather than by topic, because that is what a
    // work list needs:
    //
    //  * 11 THE INDEX-JOIN INNER SIDE IS NOT A PER-PROBE RANGE SCAN. Every
    //    `INL_JOIN`/`TIDB_INLJ` inner side reads a full scan where TiDB builds
    //    an `IndexRangeScan ... range: decided by [eq(...)]`. This is one
    //    absent capability spread over `topn_push_down` (3), `explain_complex`
    //    (3), `index_join` (2), `planner/core/join_key_type_cast` (1) and the
    //    two access-property halves elsewhere. It is the single largest
    //    remaining PLAN cause in the whole corpus.
    //  * 5 `@@last_plan_from_cache` / `@@last_plan_from_binding` READ 0. The
    //    non-prepared plan cache and the binding hit are not reported through
    //    the session variable.
    //  * 8 A REFUSAL THIS TIER DOES NOT MAKE. 1826 (duplicate FK name), 1060
    //    (a column repeated in an index), 8216 (auto_random range below 32
    //    bits), 8232 (ENGINE = MERGE UNION), 1221 (LIMIT in a recursive term),
    //    3636 (cte_max_recursion_depth), the `tidb_enforce_mpp` interlock, and
    //    the 1815 INDEX MERGE JOIN deprecation warning.
    //  * 6 WRONG ROWS, and they are the honest reason to onboard rather than
    //    to wait: `executor/parallel_apply`'s correlated-subquery UPDATE and
    //    DELETE (3), `executor/cte`'s recursive UNION not deduplicating across
    //    iterations (1), `planner/core/rule_constant_propagation`'s propagated
    //    constant not reaching the UPDATE (1), and `ddl/serial`'s rows
    //    surviving a refused TRUNCATE PARTITION (1).
    //  * 4 ROW ORDER over a join or a window, none of them the double read
    //    this batch fixed.
    //  * 13 the remainder, each a single named surface: the expression
    //    push-down blacklist (3), a padded `_bin` index key answering a column
    //    (2), `mysql_native_password` hashing (2), memory-table ids in
    //    `information_schema.tables` (2), `unix_timestamp` of a same-statement
    //    row (3, one cause), a database's COLLATE not inherited (1), a view's
    //    definer not recorded (1), `AUTO_ID_CACHE` (1), a `Point_Get` not
    //    naming its index (1), 1061 raised as an Error rather than a Note (1),
    //    and the NULL-partition pruning rule (1).
    //
    // (The groups overlap the per-topic counts by design: a topic's entry
    // names what IT carries, this list names what the corpus owes.)
    // 73 -> 72 (batch50). ONE of the census's six WRONG-ROWS statements is
    // closed: `executor/cte`'s
    //
    //   with recursive cte1(c1) as
    //     (select c1 from t1 union select c1 + 1 c1 from cte1 where c1 < 4)
    //   select * from cte1 order by c1
    //
    // over `t1 = (1),(1),(1),(2),(2),(2)` answered `1 1 1 2 2 2 3 4` where
    // TiDB answers `1 2 3 4`. The recursion WAS deduplicating; the SEED was
    // not. Go's `computeSeedPart` (`executor/cte.go:409`) hands every seed
    // chunk to the same `tryDedupAndAdd` the recursive part uses, so a
    // recursive `UNION`'s DISTINCT covers the seed's own duplicates too --
    // this tier only SEEDED its hash set from the seed rows and kept them
    // all. Nothing else moved: `executor/cte` goes 3 divergences -> 2 and
    // every other topic's triple is identical.
    //
    // The remaining wrong-row statement in that original census is
    // `ddl/serial`'s `alter table partition_table truncate
    //    partition all`, which this tier REFUSES: `AlterTableAction::
    //    Partition` reaches `alter_table`'s catch-all. Partition MAINTENANCE
    //    is unwired as a whole (`AlterPartitionAction` is parse-and-restore
    //    only), and one arm of it is not a port of the feature.
    //
    // 73 -> 71: Go's `rule_join_elimination` landed
    // (`driver::outer_join_elimination`, wired into `run_select_traced` between
    // the join-reorder-through-projection inline and the `FROM` build). An
    // outer join whose null-producing side no column of the statement reads,
    // and whose inner join keys contain a unique key of that side, is replaced
    // by its outer side alone -- so the inner table is not read at all, which
    // is what TiDB records as `<not read>`. The two statements are
    // `explain_easy`'s
    //
    //   select t1.a, t1.b from t1 left outer join t2 on t1.a = t2.a;
    //   select distinct t1.a, t1.b from t1 left outer join t2 on t1.a = t2.a;
    //
    // where `t2.a` is `t2`'s PRIMARY KEY, so Go's ground (1) applies to both
    // and its duplicate-agnostic ground (2) is not needed for either. Both now
    // read `t1` through `IndexFullScan index:PRIMARY(a, b)` exactly as TiDB
    // does -- not a separate fix: with `t2` gone the `FROM` is one table, and
    // the covering-index preference that was already there picks the same path
    // TiDB picks. `compared` holds at 7885 (nothing entered or left the
    // comparison; two statements moved from diverged to matched), and
    // `explain_easy` drops 7 -> 5.
    //
    // Both were `PlanProperty`-kind, so no row result moved. That is not an
    // assumption: the rule's own precondition is that the eliminated side
    // contributes no column to the output and cannot multiply an outer row,
    // and `driver::outer_join_elimination`'s tests replay the row sets of the
    // eliminated and non-eliminated shapes against each other, NULL-only inner
    // tables included.
    //
    // 71 -> 69: Go's `LogicalAggregation.PruneColumns` reached a derived table
    // (`driver::derived_agg_pruning`, wired next to the elimination above). An
    // UNGROUPED aggregation in a derived table whose columns nothing above
    // reads keeps only the `count(1)` Go appends when it prunes the last
    // aggregate -- so the `DataSource` under it needs NO column and the
    // narrowest index answers the row count. The two statements are
    // `explain_easy`'s
    //
    //   select 1 from (select count(c2), count(c3) from t1) k;
    //   select count(1) from (select max(c2), count(c3) as m from t1) k;
    //
    // both of which TiDB records as `IndexFullScan table:t1, index:c2(c2)`
    // where this tier scanned the table for a `c3` nobody wanted. `compared`
    // holds at 7885 again and `explain_easy` drops 5 -> 3.
    //
    // The rewrite changes what the derived table COMPUTES, so the rows are
    // proven rather than assumed: `crates/tidb-session/src/
    // tests_derived_agg_pruning.rs` replays the gorun captures, including the
    // empty-table case that is the whole reason Go appends a `count(1)`
    // instead of deleting the aggregation (an ungrouped aggregation returns
    // one row over an empty table, and the parent counts it).
    // 68 -> 64. The join PROMISE is Go's `PreparePossibleProperties` union
    // again (`tidb_executor::driver::merge_decision`'s CORRECTION), so a
    // parent merge join forms above a child that only COULD produce its
    // order, and the child is then VERIFIED after it is built. Four
    // statements of `planner/core/join_reorder_through_projection` reach
    // TiDB's recorded tree as a result -- the shape at
    // `r/planner/core/join_reorder_through_projection.result:1042`, two
    // `MergeJoin`s over an index join, with the two upper leaves reading the
    // TABLE in handle order instead of walking a covering index. `compared`
    // holds at 7885: nothing entered or left the comparable set.
    //
    // 64 -> 77, and THE DEBT DID NOT GROW -- the COMPARED SET did, 7885 ->
    // 8098. Batch57 enrolled three topics that had fallen to at most five
    // divergences, and all thirteen new entries are theirs, each named in that
    // topic's own entry in `enrolled_topics::TOPICS`: `window_function` 4
    // (one cause, the covering-index preference already carried above),
    // `executor/expand` 4 (one cause, `WITH ROLLUP` answering NULL for the
    // super-aggregate rows), `session/vars` 5 (four causes, all of them a
    // variable's own value). 4 + 4 + 5 = 13 exactly, and no divergence
    // appeared inside the 106 topics that were already enrolled: every one of
    // their matched/diverged/skipped triples is identical across the move.
    //
    // The same commit took the LITERAL COLUMN LABEL class off the unenrolled
    // frontier -- `buildProjectionFieldNameFromExpressions`'s literal switch,
    // measured at `executor/executor` 129 -> 116, `expression/builtin`
    // 97 -> 90 and `expression/issues` 154 -> 150 -- which is 24 statements
    // that none of these numbers can show, because none of those three topics
    // is enrolled. That is the frontier this ratchet does not reach, and it is
    // why the per-topic replay is run alongside it.
    //
    // 77 -> 75, and `compared` 8098 -> 8099. Two topics moved and no other
    // topic's matched/diverged/skipped triple changed at all:
    //
    //  * `planner/core/join_reorder_through_projection` 11 -> 9 diverged. Both
    //    entries are the same recorded statement,
    //
    //      select /*+ leading(ab) */ mp.id, ms.note from jt_mp mp
    //        left join jt_ms ms on mp.ms_id = ms.id
    //        left join jt_ch ch on mp.pay_receive_id = ch.channel_id
    //                          and mp.payline_id * 100 = ch.pay_kind
    //        left join jt_ab ab on ab.user_id = mp.user_id
    //                          and ab.site_code = mp.site_code
    //      where ab.regpkgid = 1;
    //
    //    where TiDB records `IndexFullScan table:ch, index:channel_id(
    //    channel_id, pay_kind)` and this tier read `TableFullScan table:ch`.
    //    The leaf `ch` is required to produce `channel_id` order and USED to
    //    have its index candidates deleted outright for exactly that reason;
    //    it now enumerates them under the order (`tidb_executor::driver::
    //    access::leaf_index_path`'s `wanted` filter, Go's `convertToIndexScan`
    //    under a non-empty property) and walks the index TiDB walks.
    //
    //  * `executor/merge_join` 246 -> 247 matched, `OutOfDomain` 13 -> 12.
    //    `TestMergeJoinDifferentTypes`' own statement,
    //
    //      create table t1(a bigint, b bit(1), index idx_a(a));
    //      create table t2(a bit(1) not null, b bit(1), index idx_a(a));
    //      select hex(t1.a), hex(t2.a) from t1 inner join t2 on t1.a=t2.a;
    //
    //    was REFUSED here with `join key value outside its column's comparison
    //    domain` -- this tier's hash join cannot key a `bit(1)` against a
    //    `bigint`. Both tables carry `idx_a(a)`, so the merge join TiDB plans
    //    for it is now available and the statement answers TiDB's own recorded
    //    rows. That is the +1 on `compared`, and it MATCHED.
    //
    // The same landing gates the merge candidate on the statement's join-
    // method hints before any cost is compared (`tidb_executor::driver::
    // join_method_hints`). It moves nothing in this replay -- the four
    // statements it decides are `topn_push_down`'s, whose recorded access
    // property this reader already matched -- and everything in the
    // `join_shape` casetest, where it is what keeps EXTRA merge pairs from
    // rising. Two oracles, two different questions about the same commit.
    //
    // 75 -> 78: the byte-preserving result reader made all eight charset
    // recordings align, and the mode-aware path selector did the same for the
    // four dual-recording collation topics. Of those twelve plus the two
    // account-setup topics, only `planner/core/integration_partition` is at
    // the enrollment bar: 132 matched, 3 diverged, 358 skipped of 493. All
    // three divergences are one cascade, named in `enrolled_topics`: an
    // `INSERT ... SELECT` from a partitioned table silently copies zero rows
    // into the ordinary reference table instead of refusing the read. The
    // other thirteen topics carry 9 to 173 divergences and stay off the gate.
    //
    // `session/vars` moves the ratchet 78 -> 76: explicit
    // `@@session.max_connections` and `@@local.max_connections` now raise
    // TiDB's 1238 instead of returning the node-wide value, moving both of
    // those statements out of `diverged` and into `BothRejected`. The same
    // read-scope repair admits a ScopeNone property at any explicit scope, so
    // `@@global.performance_schema_max_mutex_classes` moves from a skip to a
    // matching row (`200`). The topic therefore moves from 116 matched / 5
    // diverged / 6 skipped to 117 / 3 / 7.
    //
    // The complete generated variable visitor also makes three previously
    // skipped `executor/stale_txn` statements comparable: both SETs whose
    // values contain `CAST(@last_commit_ts AS UNSIGNED)` match, while the
    // following `@@tidb_current_ts` equality returns NULL for TiDB's 1. That
    // is the existing transaction-publication gap -- this tier does not yet
    // put its last commit TSO in `@@tidb_last_txn_info` -- rather than a
    // variable-rewriter difference. `executor/stale_txn` moves from 24
    // matched / 0 diverged / 19 skipped to 26 / 1 / 16, so the honest global
    // net is 78 -> 77.
    //
    // `executor/expand` reached zero: four wrong super-aggregate rows now
    // preserve aggregate inputs while only the copied grouping keys become
    // NULL (77 -> 73).
    // `executor/parallel_apply` reached zero: its three wrong DML row sets now
    // evaluate correlated subqueries before staging the write (73 -> 70).
    // `planner/core/rule_constant_propagation` now evaluates the correlated
    // scalar UPDATE assignment against each source row (70 -> 69).
    // Its remaining UNION DISTINCT plan now carries duplicate-agnostic parent
    // demand into each operand and eliminates the unread outer join (69 -> 68).
    // `ddl/serial` now truncates physical partitions and validates AUTO_RANDOM
    // ranges before creating a table (68 -> 66).
    //
    // 54 -> 53, and `compared` 9275 -> 9278. Read that pair together: this
    // ratchet reported NOTHING between 2026-08-15 and 2026-08-22, because the
    // replay was aborting on a panic in `chunk::Column::get_uint64` -- a
    // `bit(64)` join key read through the fixed 8-byte accessor -- and a test
    // that panics never reaches its own assertion. When it ran again the count
    // was 97, not 54. Every step back down was measured and committed one at a
    // time; the causes, in the order they were found:
    //
    //  * a subquery rewrite's synthesized relation became an output column,
    //    because `*` was left to resolve against the `FROM` the rewrite had
    //    just replaced (9);
    //  * static pruning divided a bare scan but not a whole reader, so an
    //    `IndexLookUp` never fanned out per partition (11);
    //  * a pruned partition was costed from the LOGICAL table's statistics,
    //    which static pruning never stores (6);
    //  * a plain-string `IN` list was pruned under the CONNECTION's collation
    //    instead of the column's (6);
    //  * a pruned reader decoded the table's FIRST columns rather than the
    //    ones its narrowed schema names (4);
    //  * an index join re-seeded a leaf through a rollup `Expand`, which Go's
    //    `admitIndexJoinInnerChildPattern` does not admit (2);
    //  * the `IN`-to-join rewrite deduplicated in the inner column's own
    //    domain while its join compared in another (1);
    //  * `SET_VAR` skipped the variable's `Validation`, and `@@warning_count`
    //    read a buffer only three statements inherit (2);
    //  * a NULL stayed NULL when its column turned `TIMESTAMP NOT NULL`, so
    //    the rewrite failed outright -- which is where the three extra
    //    `compared` come from (3).
    //
    // 53 -> 108, and `compared` 9278 -> 9477. This one RISES, so read the
    // pair: `_tidb_rowid` -- Go's extra handle column -- now resolves, and
    // 199 statements that used to fail with "Unknown column" are comparable
    // for the first time. 144 of them AGREE. No statement that agreed before
    // diverges now: the change fires only where the name is written, and
    // every such statement was previously unreachable.
    //
    // The 55 that diverge are pre-existing gaps in OTHER features, which only
    // these statements reach:
    //
    //  * 33 `planner/core/range_scan_for_like`: TiDB prints an index range in
    //    the collation's SORT KEY bytes -- `["\x00A\x00A","\x00A\x00A"]`
    //    for `'aa'`, `["",""]` for `' '` under PAD SPACE -- and this tier
    //    prints the written string.
    //  * 11 `executor/rowid`: that topic's own rows differ, so the values
    //    being compared are of different data.
    //  * 4 `planner/core/integration_partition`: `where _tidb_rowid = 1`
    //    should build a HANDLE range (`TableRangeScan range:[1,1]`) and still
    //    full-scans. `_tidb_rowid` IS the handle, so this is the extra-handle
    //    increment's own next step, not a foreign gap.
    //  * 3 `table/tables`: `SHARD_ROW_ID_BITS`, which this tier does not
    //    allocate, so `_tidb_rowid >> 48` is constant here.
    //  * 1 `executor/autoid`: row order.
    //
    // One topic LEFT the list in the same window: `ddl/db_change`, from the
    // `TIMESTAMP NOT NULL` substitution.
    //
    // 108 -> 87, `compared` FLAT at 9477. An index stores a string column's
    // COLLATION KEY, and Go's ranger converts its points to that key once, at
    // build time (`convertPointsToSortKeyInPlace`), so the range carries the
    // key from then on and `EXPLAIN` prints it: `["\x00A\x00A",
    // "\x00A\x00A"]` for `a = 'aa'` under `utf8mb4_general_ci`, not `"aa"`.
    // This tier kept the written value in the range and collated at ENCODE
    // time instead -- the same stored key, a different range, and 21
    // statements printing a range TiDB does not.
    //
    // The conversion has exactly one entry that opts out, and Go names it:
    // `DetachCondAndBuildRangeForPartition` passes `convertToSortKey =
    // false`, because a partition bound is a written VALUE compared under the
    // partition column's own collation, not an index's stored form. That
    // entry now exists here too.
    //
    // One of the 21 was a WRONG ANSWER, not a printed range:
    // `a like '测试%'` on `KEY (a(3))` returned nothing. The `LIKE` arm cuts
    // its prefix and converts its two bounds itself -- it has to, because the
    // upper bound is derived FROM the cut value and the two bounds need
    // different trimming -- so the shared tail was cutting an already
    // converted point a second time, reading a SORT KEY as text. It
    // truncated `6D4B8BD5` at a character boundary the key does not have,
    // and converting the remains again gave the weights of `'m'`, `'K'` and
    // one replacement character. `ColumnPoints::finished` is that arm saying
    // it is done, which is what Go's per-arm structure says by construction.
    //
    // 87 -> 85, and `compared` 9477 -> 9480. `_tidb_rowid` now resolves in a
    // WRITE as well as a read: Go gives an `UPDATE`/`DELETE` `DataSource` the
    // same schema a `SELECT` gets, so the name reads there too. The three
    // extra `compared` are `executor/rowid`'s `update ... where _tidb_rowid`
    // and its `delete` sibling, which used to fail outright -- and because
    // they did, every later statement of that topic was comparing different
    // data, which is what made its divergences look like value mismatches
    // rather than a missing feature.
    // 85 -> 75, `compared` FLAT at 9480. `ColumnPoints::finished` was too
    // broad: it reported the WHOLE `LIKE` arm as having converted its own
    // points, when Go's `newBuildFromPatternLike` does that in exactly one of
    // its five return cases. Its "case 3" -- a pattern with no wildcard at
    // all, `a LIKE 'aa'` -- is an equality on the pattern text, and Go cuts
    // and converts it through the same shared pair as every other arm. Marked
    // finished, it skipped both, so those ranges printed their raw text
    // (`["aa","aa"]`, `[" "," "]`) where TiDB prints the weight string.
    // `points_from_like` now reports per-case which of the two it did, which
    // is the shape Go's own five returns have.
    //
    // The same read added Go's "case 4-1", which had no counterpart here: a
    // wildcard range's upper bound is the INCREMENTED SORT KEY of the prefix,
    // so it only bounds a scan whose keys are sort keys. Go declines to build
    // one at all for the entry that reads raw values --
    // `DetachCondAndBuildRangeForPartition` -- unless the collation makes the
    // key and the value the same string.
    //
    // 75 -> 73, `compared` still 9480. `!=` is not an access condition on a
    // PREFIX index column, which Go's `conditionChecker` says in one line:
    // `if scalar.FuncName.L == ast.NE { return isFullLength, !isFullLength }`.
    // Cutting a point to the prefix widens every other comparison into a
    // superset that the reserved filter narrows back; `!=` is the one shape
    // it makes SMALLER, because the cut excludes the prefix rather than the
    // value and takes every row sharing it. Go reads the whole index instead.
    //
    // 73 -> 72, `compared` still 9480, and the one that moved is the smallest
    // part of what the read found. An index stores an `ENUM` as the member's
    // NUMBER, and the ranger's endpoint conversion had a fast path for a
    // string literal that already fits the target -- resting on Go's
    // `ConvertTo` returning such a value unchanged, which is true for
    // `VARCHAR` and false for `ENUM`, where it resolves the member. So the
    // point kept its raw text, the key codec wrote text where the index holds
    // a number, and `b = 'a'` on an `ENUM` index answered ZERO rows. A wrong
    // answer, and one the corpus barely exercises.
    //
    // Beside it, Go's `handleEnumFromBinOp`, which had no counterpart here: a
    // string comparison against an `ENUM` orders by NAME while the key orders
    // by NUMBER, so Go stops building intervals for an `ENUM` entirely and
    // emits one point range per admitted member. `b > 'a'` is
    // `["b","b"], ["c","c"]`, not `("a",+inf]`. It runs only when the
    // comparison is a STRING one: `getBaseCmpType` calls an `ENUM` Hybrid, so
    // `enum <cmp> int` is `ETInt`, and `WrapWithCastAsInt` stamps
    // `EnumSetAsIntFlag` on its own clone of the column -- which is also what
    // makes the `conditionChecker` collation gate skip the condition, and is
    // why `b = 1 AND a > 1` builds the interval `("a" "a","a" +inf]` where
    // `b = 1 AND a > 'a'` builds two points.
    //
    // The 3 that remain in `black_list` all need `mysql.expr_pushdown_blacklist`
    // and `ADMIN RELOAD`, which this tier does not have at all: with `enum`
    // blacklisted TiDB drops the enum index path and full-scans.
    //
    // 72 -> 91, and `compared` 9480 -> 9557. The count ROSE because 77 more
    // statements are now reachable, not because anything regressed: the
    // divergence set gained 24 and lost 5, and every one of the 24 is a
    // statement that used to be an `OutOfDomain` skip.
    //
    // `mysql.expr_pushdown_blacklist` and `mysql.opt_rule_blacklist` now
    // exist, `ADMIN RELOAD` publishes each, and the optimizer reads them.
    // The mechanism is not "the filter runs somewhere else": Go filters a
    // `DataSource`'s predicates through `PushDownExprs` BEFORE any access
    // path is derived, and the ranger sees only `PushedDownConds` -- so a
    // refused condition stops bounding any scan and the index it constrained
    // stops being a candidate. Blacklisting `enum` makes `columnToPBExpr`
    // refuse the column, which is how TiDB's recording turns an
    // `IndexRangeScan` over `idx(b,a)` into a root `Selection` over a
    // `TableFullScan`. `black_list` went from 25 matched / 3 diverged / 26
    // skipped to 54 matched, 0 diverged, 0 skipped.
    //
    // The tables exist because the REPLAY's catalog is now bootstrapped: it
    // is created by `Connections::open`, so it is a fresh store and gets the
    // `mysql.*` tables Go's `bootstrap()` creates for one. That is what made
    // the other 4 fixes and all 24 new divergences reachable at once --
    // `mysql.bind_info` too, so `@@last_plan_from_binding` reports a real
    // GLOBAL binding in `planner/core/physical_plan`.
    //
    // All 24 are `bindinfo/temptable`, and all are the same missing feature:
    // Go refuses `CREATE BINDING` whose origin names a TEMPORARY table with
    // 8006 (`preprocess.go`'s `TempTableType != TempTableNone` check). This
    // tier refuses `CREATE TEMPORARY TABLE` itself, so `tmp1` never exists,
    // and Go's own rule skips a table that does not exist. They close when
    // temporary tables land, not before. (They have; see the 76 -> 52 note
    // below.)
    //
    // 91 -> 86, `compared` flat at 9557. `_tidb_rowid` IS the row handle, and
    // Go says so structurally: `buildDataSource` appends
    // `NewExtraHandleSchemaCol()` to a table with neither an integer primary
    // key nor a common handle, and builds `ds.handleCols` FROM it. So the
    // ranger bounds a scan by it exactly as it does by an integer primary
    // key, and `findPKHandle`'s `!tblInfo.PKIsHandle` branch makes an
    // equality on it a `Point_Get` -- with no `Selection` above, because the
    // handle pins the row completely. This tier read every row and filtered.
    //
    // Both are refused for a PARTITIONED table, which is Go's own carve-out
    // ("Partition table can't use `_tidb_rowid` to generate PointGet Plan"):
    // a row id alone does not say which partition holds the row.
    //
    // The point-get source had to learn the column too. Nothing in a decoded
    // row fills the extra handle -- it reports the HANDLE -- so a schema that
    // names it has a slot only the source can write, the same contract the
    // table scan already had.
    //
    // Also here: `mysql.expr_pushdown_blacklist` now refuses an AGGREGATE by
    // its own name (`CheckAggPushDown`'s last line), which it did not before
    // -- the executor's partial-aggregate offer asked nothing, and the
    // planner crate's own check was handed a hardcoded empty map. And the two
    // blacklists became INSTANCE-wide rather than per-session, which is Go's
    // scope: its atomics are package-level, so `ADMIN RELOAD` on one
    // connection changes what every other connection plans.
    //
    // 86 -> 77, and `compared` 9557 -> 9565. An INSERT may WRITE
    // `_tidb_rowid` now: Go `initInsertColumns` takes the named column as the
    // extra handle and gates it on `tidb_opt_write_row_id`, raising a plain
    // error -- 1105 to the client -- without it. `fillRow` appends the
    // pseudo-column at `len(tCols)` and widens the row buffer by one, so the
    // value travels with the row and `adjustImplicitRowID` takes it off as
    // the record HANDLE rather than storing it, after `rebaseImplicitRowID`
    // lifts the counter above it.
    //
    // A written ZERO is not a handle: it means "allocate", and Go's condition
    // for that is `d.IsNull() || SQLMode&ModeNoAutoValueOnZero == 0` -- so
    // with `NO_AUTO_VALUE_ON_ZERO` set the zero falls PAST the allocation
    // branch and IS stored as handle 0. The corpus asserts the pair, the same
    // statement answering masked row id 0 with the mode and 8 without it, and
    // those three statements were the last of this topic to diverge.
    //
    // `executor/rowid` went from 38 matched / 9 diverged / 8 unreachable to
    // 55 matched, 0 diverged, 0 unreachable.
    //
    // 77 -> 76. `SHOW CREATE TABLE` prints a table's TTL back. Go's
    // `ShowCreateTable` writes `TTL`, `TTL_ENABLE` and `TTL_JOB_INTERVAL`
    // together once the table has a `TTLInfo`, each behind the `ttl` feature
    // comment, and the last two unconditionally -- so a table created with
    // `TTL=` alone prints `TTL_ENABLE='ON'` and the default job interval
    // back. This tier accepted the option and stored nothing, so a definition
    // did not round-trip through its own output. Metadata only: there is no
    // background job here to delete expired rows.
    // 76 -> 52, and `compared` 9557 -> 9595. TEMPORARY TABLES, both scopes.
    //
    // The 24 the note above promised are gone, and they are gone for the
    // reason it named: `CREATE TEMPORARY TABLE` and
    // `CREATE GLOBAL TEMPORARY TABLE` now build a table with Go's
    // `TableInfo.TempTableType` on it (`setTemporaryType`), so `tmp1` and
    // `tmp2` exist, so `checkBindGrammar`'s `TempTableType != TempTableNone`
    // test finds them and every `create global binding` over one is 8006.
    // `bindinfo/temptable` went from 4 matched / 24 diverged to 8 matched /
    // 0 diverged of 38.
    //
    // The other 38 statements are `executor/executor_txn`'s
    // `TestSavepointWithTemporaryTable`, which used to stop at the first
    // `create temporary table` and skip the rest as `OutOfDomain`; the topic
    // went from 81 matched / 0 diverged / 40 skipped to 114 matched /
    // 0 diverged / 1 skipped. It is the reason the compare COUNT rose rather
    // than a divergence being hidden.
    //
    // What those 38 forced, and what they caught: a temporary table's rows
    // are TRANSACTIONAL. Go keeps them in the transaction membuffer, so
    // `ROLLBACK TO SAVEPOINT` truncates them back with everything else
    // (`RollbackMemDBToCheckpoint`), and only at COMMIT does
    // `commitTxnWithTemporaryData` copy the LOCAL ones into the session's own
    // buffer while `temporaryTableKVFilter` throws every temporary key away.
    // The first attempt here rolled back only the local kind and left the
    // six global-temporary savepoint reads diverging inside a net
    // improvement -- the count alone would have shown 58 and looked like a
    // win. Both kinds are snapshotted per savepoint now.
    //
    // Two kinds, two homes, which is the whole feature: a GLOBAL temporary
    // table's `TableInfo` is shared (a real DDL job creates it) while its
    // rows are one session's and die with the transaction; a LOCAL one is in
    // no shared schema at all and lives in `SessionVars.LocalTemporaryTables`
    // (`tidb_session`'s per-statement attach/detach), where it SHADOWS a
    // permanent table of the same name without destroying it.
    //
    // 52 -> 48, at the MERGE of two lines of work whose bases each lacked
    // the other. One line: temporary tables (the 24 of `bindinfo/temptable`,
    // and `executor/executor_txn`'s 39 unreachable statements). The other:
    // result cells rendered the way a CLIENT receives them -- the one
    // `format_datum_text` in `tidb_protocol` that the server's row writer
    // also uses, which closed the two FLOAT e-format rows miscounted against
    // `partition_pruner` -- and `SHARD_ROW_ID_BITS`, stored, printed, and
    // composed into every allocated `_tidb_rowid` with the shard run scoped
    // to the TRANSACTION as Go's `GetRowIDShardGenerator` is (which is what
    // makes `tidb_shard_allocate_step` count rows, not statements;
    // `table/tables` 3 -> 0).
    //
    // 76 -> 69. PARTITION PRUNING, one Go rule
    // (`pkg/planner/core/rule/rule_partition_processor.go`) and the two
    // places that read its output. Every item is one recorded TiDB statement:
    //
    //  * `PartitionProcessor.prune` runs its conditions through
    //    `applyPredicateSimplification` -- whose first act is
    //    `expression.PushDownNot` -- BEFORE pruning, and the rule's own
    //    comment says why: a condition "like 'not (a != 1)' would not be
    //    handled ... when building range". A `NOT` the ranger cannot read
    //    yields NO range, which this tier read as "prune nothing", so
    //    `where not (a < 5)` over `range (a)` kept the `values less than (0)`
    //    partition TiDB drops (1).
    //
    //  * `makeUnionAllChildren` returns `LogicalTableDual{RowCount: 0}` when
    //    pruning left NO partition -- there are no children to union with.
    //    This tier printed the scan over an empty partition set instead, so
    //    `where b > 10` over a `list (b)` table of values `0..5` showed
    //    `TableRangeScan range:(10,+inf]` where TiDB prints `TableDual
    //    rows:0` (1). Neither side has an access row left, so that statement
    //    moves from compared-and-diverged to the `PlanWithoutProperty` skip
    //    class, exactly as the three `PredicateSimplification` duals above
    //    did.
    //
    //  * `rewriteDataSource` recurses through the WHOLE logical plan, so a
    //    partitioned table read as a JOIN LEAF fans out into a
    //    `PartitionUnion` exactly as a single-table source does. Only the
    //    single-table shape did here, so TiDB's `partition:p1` +
    //    `partition:p2` under a `PartitionUnion(Probe)` was one
    //    partition-less `TableFullScan` (2).
    //
    //  * `makeUnionAllChildren` gives a static-mode `Batch_Point_Get` one
    //    `DataSource` per partition too, each naming its own.
    //    `PlanTrace::partition_union` fans out SCANS and a point get is not
    //    one, so it printed a single partition-less node (1).
    //
    //  * `find_best_task.go`'s point-get conversion allows a heap table's
    //    `_tidb_rowid` on a PARTITIONED table when exactly one partition is
    //    NAMED ("unless one partition is explicitly specified":
    //    `len(ds.PartitionNames) != 1` disables it), and
    //    `PointGetPlan.AccessObject` then names that partition (2).
    //
    // Still open in this cluster and none of it this rule: TiDB builds one
    // `lookupTableTask` PER PARTITION (`executor/distsql.go`:
    // `tableLookUpTask.partitionTable = prunedPartitions[curResultIdx]`), so
    // an `IndexLookUp` over a partitioned table answers partition by
    // partition where this tier answers one merged batch (3 rows-kind);
    // a hash join's tie order over an UNPARTITIONED pair (1); and a `HAVING`
    // on the GROUP BY key pushed under the aggregate and intersected with
    // the `WHERE`'s DNF, which leaves one point on a clustered `gbk_bin`
    // primary key and lets TiDB plan a `Point_Get` (1).
    //
    // 48 -> 41 at the merge of the partition-processor line of work, whose
    // measured delta on its own base was exactly -7 -- the same seven close
    // here, so the two lines compose without overlap.
    // 48 -> 46: `LOAD STATS` executes. The two closed divergences are
    // `explain_complex`'s pair over `issue_50080.json` -- identical
    // `IndexRangeScan` object and range on both sides, differing ONLY by
    // `stats:pseudo`, because the statement that would have installed the
    // dump's 859M-row histograms was refused and every estimate under it
    // stayed pseudo. The dump now loads through
    // `tidb_executor::load_stats` (Go `storage.TableStatsFromJSON`:
    // column bounds string-decoded back to their types, index bounds kept
    // as key bytes, TopN re-sorted) into the same catalog slot `ANALYZE`
    // publishes to, so the estimates flow through the unchanged estimator.
    // The compare count RISES with the divergence count falling:
    // `explain_complex` 39 -> 42 matched of 45, `explain_stats` 8 -> 9,
    // and `tpch`'s eight dump loads go from `OutOfDomain` to side-effect
    // matches (11 -> 19 of 40), all at zero divergences.
    //
    // 41 -> 37: two from `LOAD STATS` (merged; its own accounting above) and
    // two from the PREPARED plan cache's observable contract. Go decides
    // cacheability at PREPARE (`IsASTCacheable`) and a second `EXECUTE` under
    // an identical key -- schema version, database, sql_mode, time zone, and
    // the push-down blacklist's reload counter (`plan_cache_utils.go:443`) --
    // reports `@@last_plan_from_cache = 1`. This tier re-plans from text, so
    // no plan is reused; the ADMISSION and the HIT are what is modelled, the
    // same honesty split `non_prepared_plan_cache` documents.
    //
    // The first cut over-reported: `executor/parallel_apply` expects 0 after
    // two executes of a correlated subquery, because Go's SECOND gate --
    // `isPhysicalPlanCacheable`, on the BUILT plan -- refuses any plan
    // containing a `PhysicalApply`. The driver now reports Apply construction
    // through the statement context, and the prepared path neither stores nor
    // hits for such a statement.
    //
    // 37 -> 36. `INL_MERGE_JOIN` is DROPPED, as Go's `hint.go` `HintINLMJ`
    // arm drops it -- `SetHintWarning("The INDEX MERGE JOIN hint is
    // deprecated for usage, try other hints."); continue` -- so the optimizer
    // plans as if it were not written and the statement carries warning 1815.
    // This tier still collected it and built an IndexMergeJoin TiDB no longer
    // builds.
    //
    // Reaching that also took a harness fix: an `EXPLAIN FORMAT='hint'` was
    // skipped COLD as not-comparable, but TiDB PLANNED it, and planning has
    // observable side effects -- the deprecation warning the corpus's next
    // `show warnings` reads. A non-tree-format EXPLAIN over an explainable
    // target now runs as the default-format spelling for its effects, with
    // the output still discarded and the skip still counted.
    // 37 -> 34: the three ROW-ORDER divergences of
    // `executor/index_lookup_pushdown_partition` (43 of 44 matched now), the
    // "per-partition lookupTableTask" debt named twice above. Go's
    // `IndexLookUpExecutor` builds one index request per pruned partition
    // (`buildTableKeyRanges`), drains each partition's `SelectResult` before
    // the next (`indexWorker.fetchHandles`), never lets a task span two
    // partitions (`buildAndDispatchLookupTasks` tags each with
    // `prunedPartitions[curResultIdx]`), and sorts each task's handles
    // before the table read (`buildTableReaderFromHandles`'
    // `slices.SortFunc`) -- so a partitioned unordered lookup answers
    // partition by partition, handle-ascending within each.
    // `IndexRangeSourceExec` now cuts its handle batches at partition
    // boundaries over a partition-major all-ranges cursor, and a pushed
    // LIMIT truncates per Go's TWO flavours: the plain lookup cuts the
    // index-order stream cumulatively BEFORE the sort
    // (`extractTaskHandles`' `leftCnt`, counted against extracted keys),
    // while the hinted `LocalIndexLookUp` carries the limit INSIDE each
    // per-partition cop request and counts handle-sorted ARRIVALS
    // (`extractLookUpPushDownRowsOrHandles`), which is why the recorded
    // `tp2 ... limit 5` keeps `e` and not `f`. Every other topic's
    // matched/diverged/skipped triple is unchanged -- in particular the
    // `casetest/partition/partition_pruner` row-order entry did NOT close:
    // its inner side `t2` is UNPARTITIONED there (`test_partition_2`), and
    // Go's `8,8,8 / 7,7,7` order comes from the hash join emitting a
    // cartesian probe's matches in build-chain order (reverse insertion),
    // a join-executor contract outside this seam.
    //
    // 36 -> 32 at the merge of two lines. One: the per-partition index
    // lookup (its own accounting above; -3, `executor/
    // index_lookup_pushdown_partition` fully closed). The other: `MATCH ...
    // AGAINST`'s LIKE fallback, Go `fts_to_like.go` and
    // `matchAgainstToLike` -- a direct-boolean-context `MATCH` becomes
    // `IFNULL(col ILIKE '%term%' ESCAPE '\\', 0)` predicates (ILIKE because
    // MySQL FTS is case-insensitive regardless of collation; IFNULL so a
    // NULL column is "does not contain", which keeps NOT over an excluded
    // term honest), composed per mode: natural language is one OR; boolean
    // mode ANDs required-term DNFs, NOTs excluded-term DNFs, and anchors on
    // optionals only when nothing required does. Gated exactly as Go gates
    // it: `tidb_opt_enable_alternative_logical_plans` (default OFF), only
    // the strict token subset, only STRING columns -- a non-string column
    // stays unrewritten and refuses BEFORE the NULL fast path, which is why
    // `match(int_col) against(NULL)` errors rather than answering NULL. The
    // corpus flips the gate both ways and reads all of it:
    // `planner/core/fulltext_search` went 33 matched / 1 diverged to 60
    // matched / 0, with 26 more statements answering ROWS.
    // 37 -> 35: `executor/dual_password`'s two `show create user` lines, and
    // they were CASCADES, not a hashing bug: TiDB's recorded hash is of the
    // password a `RETAIN CURRENT PASSWORD` statement set, and this tier had
    // refused those statements, so it printed the hash of the password the
    // account STILL had. `mysql.user` is a real bootstrapped table now
    // (`tidb_session::bootstrap` runs Go `metadef.CreateUserTable` plus
    // `doDMLWorks`' root row; `tidb_session::user_table` keeps the account
    // statements writing it), and `ALTER USER`/`SET PASSWORD` carry MySQL
    // 8.0 dual passwords (`executeAlterUser`/`executeSetPwd`: the
    // `$.additional_password` promotion, the 3894/3895/3878 validation
    // order, and the APPLICATION_PASSWORD_ADMIN self-service gate). The
    // topic itself went 39 matched / 2 diverged / 39 OutOfDomain to
    // 80 / 0 / 0 (12 BothRejected either way), which is also why `compared`
    // rose. Two engine bugs fell out of the same corpus and are fixed at
    // their roots rather than worked around: `NULLIF` compared only NUMERIC
    // pairs where Go's rewriter makes it `IF(a = b, NULL, a)` with the FULL
    // comparison (`expression_rewriter.go`'s `ast.NullIf` arm; the JSON
    // domain is what `DISCARD OLD PASSWORD`'s NULLIF collapse needs), and
    // the ranger converted endpoints into the column's DECLARED length where
    // Go's `newFieldType` (`ranger.go:779`) strips it -- a `CHAR(32)` key
    // part NUL-padded the sort key, the repaired interval emptied, and
    // `WHERE user = '...'` through `KEY i_user (User)` planned
    // `TableDual rows:0`.
    //
    // 32 -> 30 at the merge of the mysql.user line of work (its own
    // accounting above): `executor/dual_password` fully closed, and
    // `compared` 9629 -> 9670 as the account statements became reachable
    // through a real table.
    //
    // 30 -> 27, and the mechanism is the recording CLIENT's, found in its
    // source rather than guessed: mysql-tester's DSN is
    // `...?time_zone='Asia/Shanghai'` (every `OpenDBWithRetry` call site at
    // the pinned commit f2d90ea), which go-sql-driver turns into
    // `SET time_zone = 'Asia/Shanghai'` on each connection it opens --
    // beside the `SET NAMES utf8mb4 COLLATE utf8mb4_general_ci` half this
    // harness already modelled. `run-tests.sh` exports TZ=Asia/Shanghai for
    // the SERVER to agree. The harness's `new_session` now runs both, so
    // `select @@time_zone` reads the recorded literal and every
    // FROM_UNIXTIME/UUID_TIMESTAMP cell lands in the recorded zone.
    // `session/vars` and `expression/uuid` are both fully closed.
    //
    // 27 -> 26: the chooser gap 973dc3ba2d's pin work exposed. Go selects
    // the access path for `update t set j = -j where i = 1 and j = 1` on
    // `t (i int key, j int, unique key (i, j))` by HEURISTIC, not by cost:
    // `derivePathStatsAndTryHeuristics` (`pkg/planner/core/stats.go`) walks
    // the paths table-first and the FIRST only-point-range path that is the
    // table path or a unique index AND a single scan wins outright, pruning
    // every other path before skyline pruning or `findBestTask` ever run --
    // so the int handle's `[1,1]` beats the unique `(i, j)` point, which is
    // never even examined. The whole selection is ported
    // (`access_cost::heuristic_point_path`: the empty-range short-circuit,
    // the immediate single-scan point win, and the
    // `uniqueBest`/`refinedBest` double-scan arbitration with its 2x range
    // bound), gated off for the IndexMerge partial enumerations exactly as
    // Go's `generateIndexMergePath` runs after -- never through -- the
    // heuristic. The write plan reaches it because its read falls through
    // to the same ordinary `DataSource`, and the single-point handle range
    // then prints as Go's `convertToPointGet` bare handle plan
    // (`trace_dml_source`'s Ranges arm, the write-side twin of the read's
    // `single_point_handle` conversion). `explain_easy` drops 2 -> 1; its
    // remaining divergence is the decorrelation cost shape, a different
    // family. Both are `PlanProperty`-kind, so `compared` holds at 9670.
    //
    // 26 -> 24: `information_schema.tables` lists the schema's own tables --
    // Go's `tableIDMap` ported whole, every registered memory table a
    // `SYSTEM VIEW` row with its real id. `infoschema/v2` fully closed.
    // 30 -> 29: `executor/jointest/join` fully closed (828 matched / 0). The
    // one statement was `TIDB_SMJ(t2) ... left outer join t2 on t1.a=t2.a
    // and t1.a!=3`, whose inner side TiDB reads as `TableRangeScan
    // range:[-inf,3), (3,+inf]` while this tier full-scanned it. The
    // mechanism is Go `expression.PropConstForOuterJoin`'s
    // `propagateColumnEQ`/`deriveConds` (constant_propagation.go:846-941):
    // a preserved-side ON condition -- never a filter, it stays at the join
    // as the printed left cond -- has its outer column replaced by the inner
    // one through the `t1.a = t2.a` key, and the derived `ne(t2.a, 3)` is
    // routed to the inner child where the ranger consumes it. Ported as
    // `tidb_executor::driver::predicate_push_down::propagate_over_outer_join`
    // (join keys into a union-find, `tryToReplaceCond`'s nullAware refusals
    // -- IFNULL/IF/CASE/`<=>`/ISNULL and the unfoldables -- verbatim), with
    // the derived family offered to the leaf's access-path chooser so it can
    // range instead of re-filtering. Only the DERIVED family is offered: an
    // earlier draft offered every distributed filter and re-priced
    // inner-join leaves, flipping `explain_complex`'s recorded `HashJoin`
    // into an index join -- see `Plan::derived`'s own doc. Rows moved
    // nowhere (`compared` holds at 9670); the family is pinned by
    // `tidb_session::tests_join_predicate_placement`'s
    // `an_outer_join_on_condition_derives_the_inner_range_through_the_key`
    // and its four siblings.
    // 30 -> 22 at the JOIN-REORDER COST family, `planner/core/join_reorder2`
    // (8 -> 0) and `planner/core/join_reorder_through_projection` (5 -> 5,
    // the five STATEMENTS shifted -- see below). One mechanism closed all
    // eight: a derived table whose `FROM` writes a LEFT OUTER join was
    // declined whole by the row inventory
    // (`driver::join_reorder::DerivedRel`), so the join ABOVE it -- the
    // `(sub, t4)` top join of every diverging statement -- had no row
    // estimate, no priced alternatives, and fell to `build_join_with_choice`'s
    // structural merge fallback; the committed merge's child order then
    // forced an INDEX join by elimination below (`getHashJoins` returns
    // nothing under a non-empty property), which is the recorded
    // `TableRangeScan t3 range: decided by [t2.id]` row. Modelling the
    // outer join (`LogicalJoin.DeriveStats`' `count = math.Max(count,
    // leftProfile.RowCount)` arm, reached exactly as Go reaches it through
    // `optimizeRecursive` into the subquery) lets the site PRICE: hash
    // 3,872,144 beats merge 8,331,569 under ver2, Go's own pick, and the
    // left-outer site below -- now under the EMPTY property -- picks the
    // recorded MergeJoin over the ordered `t3` full scan on cost.
    //
    // Two more Go mechanisms landed in the same unit and keep
    // `join_reorder_through_projection` at five while CHANGING which five:
    // `hash_join_candidate` now reads the session's resolved
    // `tidb_hash_join_concurrency` (Go stamps `p.Concurrency` in
    // `getHashJoins` and `getPlanCostVer24PhysicalHashJoin` divides the
    // probe terms by it; mysql-tester's DSN pins it to 1, and the hardcoded
    // plain-session 5 priced a different session than the recordings), and a
    // COMPUTED simple projection now delivers Go's `PhysicalProjection` task
    // receipt so the joins above a `(select t2.a, t2.b*2 ...) dt` derived
    // table compare priced candidates instead of keeping the structural
    // merge. The five that remain are one family: sites where the recorded
    // choice turns on per-node candidate SHAPE fidelity (Go's cop Selection
    // below the injected projection, the reader's net term, `PruneColumns`
    // over the injected wrapper) that this tier's assembly does not yet
    // reproduce -- publishing the pruned wrapper was measured at 5 -> 6 and
    // reverted; see `driver::through_proj::wrap_node`'s NAMED RESIDUE.
    const KNOWN_DIVERGENCES: usize = 24;
    //
    //
    // 28 -> 24 (written as 35 -> 31 in batch43's own tree, which branched before batch42), in three unrelated causes, none of them an access-path
    // heuristic:
    //
    //  * THE CLUSTERED HANDLE IS AN INDEX KEY PART (1, and the whole of the
    //    first half of TWO SINGLE CASES above). A non-unique secondary index
    //    stores the row's handle in its KEY, so Go's `fillIndexPath` appends
    //    it to `path.IdxCols` and the ranger narrows on it:
    //    `where c1 > 1 and c2 = 1 and c3 < 1` is `range:(1 1,1 +inf]`, not
    //    `range:[1,1]`. The estimate is trimmed back to the DECLARED columns
    //    before the row count is asked for (`pruneEstimateRange`), which is
    //    what keeps the two-dimension range from costing the index path out
    //    of the plan. `tidb_executor::access_cost::handle_key_part` owns both
    //    halves and the row set is pinned by
    //    `a_handle_range_reads_the_rows_a_full_scan_reads` over negative,
    //    zero and NULL-bearing data.
    //
    //  * A PREPARED FIELD THAT IS A COLUMN REFERENCE WAS RENAMED (2). This is
    //    a WIRE HEADER, not a plan: `execute stmt1 using @a` over
    //    `select m1.a ... where m1.a in (select m2.b+? ...)` printed the
    //    column `m1.a` where TiDB prints `a`. Binding a `?` here means
    //    RESTORING the statement, so unaliased fields have their source text
    //    pinned as an alias first -- and a field that is an
    //    `ast.ColumnNameExpr` is named by `colNameField.Name.Name` rather
    //    than by its text, so pinning it OVERRODE the column identifier
    //    instead of preserving it. The same statement prepared without a `?`
    //    always printed `a`.
    //
    //  * `HEX` OVER A `BIT` COLUMN (1). `hexFunctionClass` switches on the
    //    argument's EvalType and `mysql.TypeBit` is `ETInt`, so a `bit(48)`
    //    is hexed as a NUMBER: `80A0D091A`, not the stored `00080A0D091A`.
    //    A bit LITERAL is unaffected -- Go types it `TypeVarString` -- which
    //    is why only a stored column exposed this.
    //
    // Two are `Rows`-kind and two are `PlanProperty`-kind, so `compared`
    // holds at 5639 and matched rises.

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
/// The remaining 12 were all harness gaps and are now covered by the byte
/// reader and dual-recording selector below:
///
///   8 topics whose `r/*.result` is not valid UTF-8, because the recording
///     holds deliberately invalid byte sequences -- these are the charset
///     topics (`executor/charset`, `executor/insert`,
///     `expression/charset_and_collation`, `new_character_set`,
///     `new_character_set_builtin`, `planner/core/integration`,
///     `planner/core/integration_partition`,
///     `planner/core/tests/prepare/issue`). Reading them means carrying the
///     recording as BYTES rather than as `String`; [`align_bytes`] now keeps
///     those octets intact through row comparison.
///   4 topics with no `r/*.result` at all: `collation_agg_func`,
///     `collation_check_use_collation`, `collation_misc`,
///     `collation_pointget`. Each has TWO recordings instead --
///     `r/<topic>_disabled.result` and `r/<topic>_enabled.result`, one per
///     `new_collations_enabled_on_first_bootstrap` setting -- so the topic is
///     not a single `t`/`r` pair at all. Aligning them means choosing which
///     recording the replay's own collation configuration corresponds to,
///     which is a claim about this engine's collation support, not a parser
///     fix. [`recording_path`] chooses the recording for the live collation
///     mode.
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
        let result_path = recording_path(&dir, &topic);
        let recorded = match fs::read(&result_path) {
            Ok(bytes) => bytes,
            Err(e) => {
                unaligned += 1;
                eprintln!("UNALIGNED  {topic}: read {}: {e}", result_path.display());
                continue;
            }
        };
        match parse_test(&script).and_then(|items| {
            let count = items.len();
            align_bytes(&items, &recorded).map(|_| count)
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
/// # THE ENROLLMENT CENSUS (batch46), and what it left off
///
/// This survey was run over EVERY topic and every one classified, rather than
/// used to pick a favourite. 257 topics: 49 were already enrolled, 190 report
/// a status line, 14 do not ALIGN, 3 CRASH and 1 does not finish.
///
/// 57 of the 190 were enrolled (see `enrolled_topics::TOPICS`), taking the
/// compared corpus from 5,639 of 6,882 to 7,875 of 10,747 and the ratchet from
/// 24 to 71. The bar was the one the original list used: replays at zero
/// divergences, OR replays with a countable list of divergences whose causes
/// can be named. 33 of the 57 are at zero; the other 24 carry 47 divergences
/// between them, named per topic there and grouped by cause at
/// `KNOWN_DIVERGENCES`.
///
/// ## What was left off, and why
///
/// 133 aligned topics were NOT enrolled. They hold 21,250 matched and 2,715
/// diverged statements, and the reason is the same for almost all of them:
/// 69 carry TEN OR MORE divergences, which is past the point where a ratchet
/// stops naming a regression and starts hiding one. The largest, with their
/// counts, as the standing work list:
///
///   228 `planner/core/casetest/rule/rule_join_reorder`   (272 matched)
///   167 `planner/core/casetest/physicalplantest/physical_plan` (635)
///   132 `executor/executor`                              (1,487)
///   122 `planner/core/plan_cache`                        (1,158)
///    97 `expression/builtin`                             (1,076)
///    91 `executor/write`                                 (389)
///    74 `executor/aggregate`                             (742)
///    73 `planner/core/casetest/hint/hint`                (102)
///    68 `session/nontransactional`                       (1,230)
///    68 `ddl/db_integration`                             (358)
///
/// Note the SHAPE of that list: `executor/executor`, `planner/core/plan_cache`,
/// `expression/builtin`, `session/nontransactional` and `ddl/column_type_change`
/// each already match over a thousand statements. They are not far from the
/// bar -- they are large. Onboarding them is a per-topic triage job of the same
/// kind this census did for the 24, not a capability increment.
///
/// The remaining 64 left-off topics carry NINE OR FEWER divergences. They were
/// skipped for VALUE, not for behaviour: 5 of them (`executor/perfschema`,
/// `executor/inspection_common`, `infoschema/keywords`,
/// `globalindex/multi_valued_index`, `topn_pushdown`) compare ZERO statements,
/// so enrolling them would gate nothing, and the rest match fewer than 15
/// statements each -- each would add one or two divergences to the ratchet for
/// a handful of compared statements. Every one of their causes was read and is
/// a duplicate of a cause already carried: the index-join inner side, a refusal
/// this tier does not make, or a `SHOW WARNINGS` text.
///
/// ## Class (c): the 14 topics that do not ALIGN, by harness gap
///
/// * A `.result` recording that is not valid UTF-8 (8): `executor/charset`,
///   `executor/insert`, `expression/charset_and_collation`, `new_character_set`,
///   `new_character_set_builtin`, `planner/core/integration`,
///   `planner/core/integration_partition`,
///   `planner/core/tests/prepare/issue`. The reader takes the file as a Rust
///   `String`; these recordings hold raw bytes in non-UTF-8 charsets on
///   purpose, so the gap is the READER's, not the engine's.
/// * No `.result` file at all (4): the four `collation_*` topics. The
///   integration suite records them only under a non-default collation build.
/// * An account the replay cannot authenticate (2): `ddl/sequence`'s
///   `connect (conn1, ...)` is refused with `DbAccessDenied` for
///   `myuser@localhost`, and `executor/simple`'s `testuser1@localhost` is
///   absent from the registry after the script's own account statements.
///
/// ## Class (d): the 4 topics that do not FINISH
///
/// * `ddl/db`, `explain_shard_index` and `planner/core/casetest/integration`
///   CRASH (exit 101). The two named in the section above are the
///   field-type/datum-kind mismatch at `Chunk::append_datum`;
///   `explain_shard_index` is new to this census and is not yet attributed.
/// * `expression/issues` still does not finish inside the 30s child budget.
///
/// None of the four is enrolled, and none can be: a child that aborts reports
/// no counts at all.
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

#[test]
#[ignore = "scratch probe"]
fn zz_scratch_probe() {
    let mut s = Session::new();
    for sql in [
        "set @@time_zone='+00:00'",
        "select timestamp '2024-01-01 14.000011'",
        "select timestamp '2024-01-01 14:00:00.010'",
        "select timestamp '2024-01-01 14.66'",
        "select timestamp '2024-01-01'",
        "select timestamp '2024-01-01 14:00:00+14:01'",
    ] {
        eprintln!("SQL {sql} => {:?}", s.run_with_columns(sql));
    }
}
