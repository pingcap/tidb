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
//! Replay `tests/integrationtest/t/<topic>.test` through the live Session
//! and compare with TiDB's recorded `r/<topic>.result`. Recordings are read-only:
//! every divergence in an enrolled topic fails the gate.
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

use difftest_result_tests::enrolled_topics;
use difftest_result_tests::integration_plan_property;
use difftest_result_tests::mysqltest_connections;
use difftest_result_tests::mysqltest_script;

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use enrolled_topics::TOPICS;
use integration_plan_property::{access_property, plan_statement, PlanStatement};
use mysqltest_connections::Connections;
use mysqltest_script::{align_bytes, parse_test, recording_path, split_warnings_bytes, Item, Stmt};
use tidb_datatype::Datum;
use tidb_executor::DriverError;
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
    for topic in TOPICS {
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
    for topic in TOPICS {
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

/// One recorded statement line as mysql-tester sends it: one COM_QUERY,
/// which the server splits and runs statement by statement (Go
/// `handleQuery`, `conn.go:1861`), stopping at the first error. A line such
/// as `truncate t1;truncate t2;` is several statements, admitted by the
/// connection's `@@tidb_multi_statement_mode` (see `Connections::open`).
/// Only the last statement's output reaches the recorder's result block.
fn run_command(session: &mut Session, sql: &str) -> Result<StmtOutput, DriverError> {
    let statements = session.split_statements(sql, false)?;
    let mut output = StmtOutput::Affected(0);
    for statement in &statements {
        output = session.run_with_columns(statement)?;
    }
    Ok(output)
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
    // mysql-tester sends the statement WITHOUT its `;` delimiter, which the
    // server can observe: a syntax error's `near "..."` text runs to the end
    // of the source (`create invalid` reports near "invalid", not
    // "invalid;").
    let stmt_sql = stmt_sql.trim_end().strip_suffix(';').unwrap_or(stmt_sql);
    if let Some(reason) = stmt.blocker {
        // The recorder rewrote this statement's output, so nothing about it is
        // comparable -- but mysql-tester still RAN it, and what it did is what
        // the statements after it read. Skipping the RUN as well silently
        // rewinds the session: in `session/variable`, `set @@global.x = 1.1`
        // sits under `--enable_warnings`, so the clamped value it stores never
        // happened here and the next four `select @@global.x` diverged on a
        // value this driver had suppressed rather than on anything the engine
        // does. Run it, discard the outcome, and count the skip.
        drop(run_command(session, stmt_sql));
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
            drop(run_command(session, sql));
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
    let outcome = run_command(session, sql);
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
    // `INTEGRATION_SHOW_CONTEXT` lists the statements that ran before each
    // divergence, which a repeated `select @@last_plan_from_cache` needs.
    let show_context = std::env::var_os("INTEGRATION_SHOW_CONTEXT").is_some();
    let mut recent: std::collections::VecDeque<String> = std::collections::VecDeque::new();
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
        let command = connections.begin_command();
        let outcome = compare(connections.current(), stmt, &block, &mut report);
        drop(command);
        if matches!(outcome, Err(None)) && !stmt.expect_error {
            connections.recover_account_row_from_unsupported_create_user(&stmt.sql);
        }
        match outcome {
            Ok(kind) => report.matched(kind),
            Err(None) => {}
            Err(Some(detail)) => {
                let context = if show_context {
                    recent
                        .iter()
                        .map(|sql| format!("  after: {sql}\n"))
                        .collect::<String>()
                } else {
                    String::new()
                };
                report
                    .divergences
                    .push(format!("\n--- [{topic}] {}\n{context}{detail}", stmt.sql));
            }
        }
        if show_context {
            recent.push_back(stmt.sql.clone());
            if recent.len() > 8 {
                recent.pop_front();
            }
        }
    }
    Ok(report)
}

#[test]
fn integrationtest_replay_matches_recorded_tidb_output() {
    assert_topics_are_unique();

    let mut total = TopicReport::default();
    let mut per_topic = Vec::new();
    for topic in TOPICS {
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

    assert!(
        total.divergences.is_empty(),
        "{} of {} compared statements diverge from TiDB's recording:{}",
        total.divergences.len(),
        total.matched_total() + total.divergences.len(),
        total.divergences.join("")
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
    let topics = std::env::var("INTEGRATION_TOPIC").expect("INTEGRATION_TOPIC must name a topic");
    // A comma-separated list replays its topics in order in this one process,
    // as the corpus test does, which is what exposes state one topic leaves
    // behind for the next.
    for topic in topics
        .split(',')
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
    {
        replay_topic_reporting(topic);
    }
}

fn replay_topic_reporting(topic: &str) {
    let topic = topic.to_owned();
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
/// by name. Ignored by default: it surveys the fixture topics and is an onboarding
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
#[test]
#[ignore = "onboarding tool: ranks fixture topics for enrollment"]
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
        if TOPICS.contains(&topic.as_str()) {
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
