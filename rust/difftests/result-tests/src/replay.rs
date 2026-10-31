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

//! The replay engine: one topic's script replayed statement by statement and
//! compared with TiDB's recording.
//!
//! The engine drives whatever runs SQL through two small traits, so the same
//! comparison measures more than one engine: [`ReplayBackend`] holds a
//! topic's connections and honours `connect`/`connection`/`disconnect`, and
//! [`ReplaySession`] runs one statement. The in-process engine
//! (`tidb_session::Session` over [`crate::mysqltest_connections::Connections`])
//! is one backend; the production node over the embedded store is another
//! (`rust/docs/one-engine-corpus-execplan.md`).

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use crate::integration_plan_property::{access_property, plan_statement, PlanStatement};
use crate::mysqltest_script::{
    align_bytes, parse_test, recording_path, split_warnings_bytes, ConnectionCmd, Item, Stmt,
};
use tidb_datatype::Datum;
use tidb_driver_error::DriverError;
use tidb_executor::StmtOutput;

/// One connection of a replay backend: what the engine asks of whatever
/// runs SQL.
pub trait ReplaySession {
    /// Splits one COM_QUERY text into its statements, as the server's query
    /// handler does before running them.
    ///
    /// # Errors
    ///
    /// The refusal the server answers the whole text with.
    fn split_statements(
        &mut self,
        sql: &str,
        client_multi_statements: bool,
    ) -> Result<Vec<String>, DriverError>;

    /// Runs one statement: rows with their column names and types, an
    /// affected count, or the error the client would read.
    ///
    /// # Errors
    ///
    /// The statement's error, carrying the MySQL code and message.
    fn run_with_columns(&mut self, sql: &str) -> Result<StmtOutput, DriverError>;

    /// The last result's column name metadata (Go `ExecStmt.OutputNames`),
    /// empty when the statement returned no result or its plan had none.
    fn last_result_output_names(&self) -> Vec<tidb_datatype::FieldNameMetadata>;
}

/// A topic's connections over one store.
pub trait ReplayBackend: Sized {
    /// Opens a fresh store for `topic` and its first (root) connection.
    ///
    /// # Errors
    ///
    /// Why the topic cannot be replayed on this backend.
    fn open(topic: &str) -> Result<Self, String>;

    /// Honours one `connect`/`connection`/`disconnect` command.
    ///
    /// # Errors
    ///
    /// A command the backend cannot honour faithfully, which ends the topic.
    fn apply(&mut self, cmd: &ConnectionCmd) -> Result<(), String>;

    /// Runs `body` over the current connection, under whatever guard the
    /// backend keeps for one command.
    fn with_current<R>(&mut self, body: &mut dyn FnMut(&mut dyn ReplaySession) -> R) -> R;

    /// Called after a statement this backend refused where TiDB answered.
    fn after_refused_statement(&mut self, _sql: &str) {}
}

impl ReplaySession for tidb_session::Session {
    fn split_statements(
        &mut self,
        sql: &str,
        client_multi_statements: bool,
    ) -> Result<Vec<String>, DriverError> {
        tidb_session::Session::split_statements(self, sql, client_multi_statements)
    }

    fn run_with_columns(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        tidb_session::Session::run_with_columns(self, sql)
    }

    fn last_result_output_names(&self) -> Vec<tidb_datatype::FieldNameMetadata> {
        tidb_session::Session::last_result_output_names(self)
            .0
            .to_vec()
    }
}

impl ReplayBackend for crate::mysqltest_connections::Connections {
    fn open(topic: &str) -> Result<Self, String> {
        crate::mysqltest_connections::Connections::open(topic)
    }

    fn apply(&mut self, cmd: &ConnectionCmd) -> Result<(), String> {
        crate::mysqltest_connections::Connections::apply(self, cmd)
    }

    fn with_current<R>(&mut self, body: &mut dyn FnMut(&mut dyn ReplaySession) -> R) -> R {
        let command = self.begin_command();
        let outcome = body(self.current());
        drop(command);
        outcome
    }

    fn after_refused_statement(&mut self, sql: &str) {
        self.recover_account_row_from_unsupported_create_user(sql);
    }
}

/// Why one statement did not produce a comparable outcome. Every skip lands in
/// exactly one of these, and the totals are printed on every run: a driver
/// that reports what it skipped is worth more than one that reports a big
/// number of cases and quietly drops most of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SkipClass {
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
    /// TiDB's recording succeeded and this engine raised an error the
    /// script's own `--error 0,<code>` list accepts: both outcomes are the
    /// test's contract, not a gap.
    AcceptedAlternativeError,
}

/// `tests/integrationtest` at the repository root: the scripts, the
/// recordings and the files they load.
pub fn integrationtest_dir() -> PathBuf {
    difftest::parser_oracle::repo_root().join("tests/integrationtest")
}

/// What a matched statement proved. The three are different in kind, and a
/// count that merges them says less than it appears to: a matched row result
/// compared VALUES, while a matched side effect only agreed that a statement
/// recorded no output of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MatchKind {
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
pub struct TopicReport {
    /// Matched statements, by [`MatchKind`].
    pub matched: BTreeMap<String, usize>,
    /// Skipped statements, by [`SkipClass`].
    pub skipped: BTreeMap<String, usize>,
    /// One rendered report per divergent statement.
    pub divergences: Vec<String>,
    /// One `statement`, `error` pair per statement TiDB answered and this
    /// engine refused (counted as [`SkipClass::OutOfDomain`]).
    pub out_of_domain: Vec<(String, String)>,
    /// The topic's wall time: store bootstrap and every statement.
    pub elapsed: std::time::Duration,
    /// Statement count and wall time by [`statement_kind`].
    pub statement_time: BTreeMap<String, (usize, std::time::Duration)>,
}

/// The leading keywords that name what a statement does (`create table`,
/// `select`, `explain`), the key [`TopicReport::statement_time`] groups by.
#[must_use]
pub fn statement_kind(sql: &str) -> String {
    let mut words = sql
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_lowercase);
    let first = words.next().unwrap_or_default();
    match first.as_str() {
        "create" | "drop" | "alter" | "admin" | "show" | "set" | "load" => {
            let second = words.next().unwrap_or_default();
            format!("{first} {second}")
        }
        _ => first,
    }
}

impl TopicReport {
    /// Counts one skipped statement.
    pub fn skip(&mut self, class: SkipClass) {
        *self.skipped.entry(format!("{class:?}")).or_default() += 1;
    }

    /// Counts one matched statement.
    pub fn matched(&mut self, kind: MatchKind) {
        *self.matched.entry(format!("{kind:?}")).or_default() += 1;
    }

    /// Matched statements of every kind.
    #[must_use]
    pub fn matched_total(&self) -> usize {
        self.matched.values().sum()
    }

    /// Every statement the topic held.
    #[must_use]
    pub fn total(&self) -> usize {
        self.matched_total() + self.divergences.len() + self.skipped.values().sum::<usize>()
    }
}

/// [`TopicReport`] as the lines a replay child process hands its parent:
/// one record per line, tab-separated, each text escaped so a multi-line
/// divergence stays on its line.
impl TopicReport {
    /// The report as [`Self::decode`] reads it back.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut out = String::new();
        for (kind, count) in &self.matched {
            out.push_str(&format!("M\t{}\t{count}\n", escape(kind)));
        }
        for (class, count) in &self.skipped {
            out.push_str(&format!("S\t{}\t{count}\n", escape(class)));
        }
        for divergence in &self.divergences {
            out.push_str(&format!("D\t{}\n", escape(divergence)));
        }
        for (sql, error) in &self.out_of_domain {
            out.push_str(&format!("O\t{}\t{}\n", escape(sql), escape(error)));
        }
        out.push_str(&format!("E\t{}\n", self.elapsed.as_nanos()));
        for (kind, (count, spent)) in &self.statement_time {
            out.push_str(&format!("T\t{}\t{count}\t{}\n", escape(kind), spent.as_nanos()));
        }
        out
    }

    /// Reads back [`Self::encode`].
    ///
    /// # Errors
    ///
    /// A line that is not one of the records `encode` writes.
    pub fn decode(text: &str) -> Result<Self, String> {
        let mut report = TopicReport::default();
        let nanos = |field: &str| {
            field
                .parse::<u64>()
                .map(std::time::Duration::from_nanos)
                .map_err(|e| format!("bad duration {field:?}: {e}"))
        };
        let count = |field: &str| field.parse::<usize>().map_err(|e| format!("bad count {field:?}: {e}"));
        for line in text.lines() {
            let fields: Vec<&str> = line.split('\t').collect();
            match fields.as_slice() {
                ["M", kind, n] => {
                    report.matched.insert(unescape(kind), count(n)?);
                }
                ["S", class, n] => {
                    report.skipped.insert(unescape(class), count(n)?);
                }
                ["D", divergence] => report.divergences.push(unescape(divergence)),
                ["O", sql, error] => report.out_of_domain.push((unescape(sql), unescape(error))),
                ["E", elapsed] => report.elapsed = nanos(elapsed)?,
                ["T", kind, n, spent] => {
                    report.statement_time.insert(unescape(kind), (count(n)?, nanos(spent)?));
                }
                _ => return Err(format!("unreadable report line {line:?}")),
            }
        }
        Ok(report)
    }
}

fn escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\n', "\\n").replace('\t', "\\t")
}

fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
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
/// [`tidb_format_textrow::format_datum_text`], which the server's own row writer
/// uses. Rendering the `Datum` alone agrees for most types and then quietly
/// disagrees for the ones whose text depends on the COLUMN: a `FLOAT` prints
/// `2.77311e38` where the value alone prints every digit, and only the
/// column's declared width says which. A harness that cannot tell those apart
/// reports a divergence for a correct answer -- or, worse, a match for a
/// wrong one.
fn cell_bytes(
    field_type: &tidb_datatype::FieldType,
    table_is_empty: bool,
    value: &Datum,
) -> Vec<u8> {
    if value.is_null() {
        return b"NULL".to_vec();
    }
    // `table_is_empty` is Go's `ColumnInfo.Table == ""`, which decides
    // whether a DOUBLE/FLOAT's FIXED decimal precision is honoured.
    let column = tidb_format_textrow::TextColumn::from_field_type(field_type, table_is_empty);
    match tidb_format_textrow::format_datum_text(column, value) {
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
    output_names: &[tidb_datatype::FieldNameMetadata],
    rows: &[Vec<Datum>],
) -> Vec<Vec<u8>> {
    // The recorder reads names off the wire, where Go `column.Info.Dump` cuts
    // each one to `maxColumnNameSize` bytes.
    let mut out = vec![columns
        .iter()
        .map(|(name, _)| &name.as_bytes()[..name.len().min(tidb_server_internal_column::MAX_COLUMN_NAME_SIZE)])
        .collect::<Vec<_>>()
        .join(&b'\t')];
    out.extend(rows.iter().map(|row| {
        let mut line = Vec::new();
        for (index, value) in row.iter().enumerate() {
            if index > 0 {
                line.push(b'\t');
            }
            match columns.get(index) {
                Some((_, field_type)) => {
                    // Go `ResultField.TableAsName` is the output name's
                    // `TblName` (colNames2ResultFields); a result without
                    // names formats every column as computed.
                    let table_is_empty = output_names
                        .get(index)
                        .is_none_or(|name| name.table.original.is_empty());
                    line.extend(cell_bytes(field_type, table_is_empty, value));
                }
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
    session: &mut dyn ReplaySession,
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
/// The gate reaches 704 of 21,636 statements
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
fn survey_unwatched_warning(session: &mut dyn ReplaySession, stmt: &Stmt) {
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
fn run_command(session: &mut dyn ReplaySession, sql: &str) -> Result<StmtOutput, DriverError> {
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
fn warning_difference(session: &mut dyn ReplaySession, want: Option<&[Vec<u8>]>) -> Option<String> {
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
                        Some((_, field_type)) => line.extend(cell_bytes(field_type, true, value)),
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
    // mysql-tester sorts the block it appends under `--enable_warnings`
    // (every recorded multi-warning block is in byte order, while an explicit
    // `SHOW WARNINGS` result keeps the server's order), so the server's order
    // is compared the same way.
    let mut ours = ours;
    ours.sort();
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

/// Loads `tests/integrationtest/config.toml` into the process config once:
/// `run-tests.sh` starts the recording tidb-server with it, and recorded
/// answers depend on its settings (`allow-expression-index`).
fn ensure_integration_config(dir: &std::path::Path) {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let path = dir.join("config.toml");
        let text = fs::read_to_string(&path).expect("read the integration config");
        let mut config = tidb_config::config_tree::config::new_config();
        // An undecoded key is Go's non-fatal ErrConfigValidationFailed.
        let _ = config.load_str(&path.display().to_string(), &text);
        tidb_config::config_tree::config::store_global_config(config);
    });
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
    session: &mut dyn ReplaySession,
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
    // An EXPLAIN is compared as the WHOLE recorded text -- ids, estimates,
    // operator info, in the recorded format -- not by each table's access
    // property alone, which hid hundreds of plan divergences. A mismatch is
    // tagged so the suite can budget plan-text divergences apart from the rest
    // (`INTEGRATION_LOOSE_PLANS` restores the access-property reading).
    let strict_plan = matches!(plan, Some(PlanStatement::RunDefaultExplain(_)))
        && std::env::var_os("INTEGRATION_LOOSE_PLANS").is_none();
    let plan = if strict_plan { None } else { plan };
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
    let output_names = session.last_result_output_names();
    if traced {
        eprintln!("SQL< {}ms", started.elapsed().as_millis());
    }
    match (outcome, recorded_error) {
        // TiDB rejected it and so did we. The wording is TiDB's; only the
        // rejection is asserted, and so is the recorded error CODE -- a
        // rejection with the wrong code is a real divergence (it hid ~150 of
        // them while this was an opt-in survey switch).
        // `INTEGRATION_IGNORE_ERROR_CODES` turns the code comparison off.
        (Err(error), true) => {
            if std::env::var_os("INTEGRATION_IGNORE_ERROR_CODES").is_none() {
                let first = recorded.first().map(|line| display_line(line));
                // The recorder writes `Error <code> (<sqlstate>): <message>`;
                // both the code and the SQLSTATE reach the client.
                let want = first.as_deref().and_then(|line| {
                    let rest = line.strip_prefix("Error ")?;
                    let code = rest
                        .split(|c: char| !c.is_ascii_digit())
                        .next()?
                        .parse::<u16>()
                        .ok()?;
                    let state = rest
                        .split_once(" (")
                        .filter(|(_, tail)| tail.get(5..7) == Some("):"))
                        .and_then(|(_, tail)| tail.get(..5))
                        .map(str::to_owned);
                    Some((code, state))
                });
                let ours = error.to_mysql_error();
                let our_state = String::from_utf8_lossy(&ours.state).into_owned();
                if want.as_ref().is_some_and(|(code, state)| {
                    *code != ours.code || state.as_ref().is_some_and(|state| *state != our_state)
                }) {
                    return Err(Some(format!(
                        "  tidb: {}\n  rust: Error {} ({}): {}",
                        first.unwrap_or_default(),
                        ours.code,
                        our_state,
                        ours.message
                    )));
                }
            }
            report.skip(SkipClass::BothRejected);
            Err(None)
        }
        (Ok(_), true) => Err(Some(format!(
            "  tidb: {}\n  rust: accepted the statement",
            recorded
                .first()
                .map_or_else(|| "<error>".to_owned(), |line| display_line(line))
        ))),
        (Err(error), false)
            if stmt
                .accepted_errors
                .contains(&error.clone().to_mysql_error().code) =>
        {
            report.skip(SkipClass::AcceptedAlternativeError);
            Err(None)
        }
        (Err(error), false) => {
            report.skip(SkipClass::OutOfDomain);
            if show_out_of_domain() {
                eprintln!("OUT OF DOMAIN: {}\n  {error:?}", stmt.sql);
            }
            report
                .out_of_domain
                .push((stmt.sql.clone(), format!("{error:?}")));
            Err(None)
        }
        (Ok(StmtOutput::Rows { columns, rows }), false) => {
            let mut ours = render_rows(&columns, &output_names, &rows);
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
                    "{}  tidb: {}\n  rust: {}",
                    if strict_plan { PLAN_TEXT_TAG } else { "" },
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

/// Marks a divergence of an EXPLAIN's full text, so a suite can budget those
/// separately from every other kind.
pub const PLAN_TEXT_TAG: &str = "  [plan text]\n";

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
pub fn run_topic<B: ReplayBackend>(topic: &str) -> Result<TopicReport, String> {
    let topic = topic.to_owned();
    difftest::on_deep_stack(move || run_topic_on_this_stack::<B>(&topic))
}

fn run_topic_on_this_stack<B: ReplayBackend>(topic: &str) -> Result<TopicReport, String> {
    let dir = integrationtest_dir();
    ensure_integration_config(&dir);
    let script = fs::read_to_string(dir.join(format!("t/{topic}.test")))
        .map_err(|e| format!("read t/{topic}.test: {e}"))?;
    let recorded_path = recording_path(&dir, topic);
    let recorded =
        fs::read(&recorded_path).map_err(|e| format!("read {}: {e}", recorded_path.display()))?;
    let items = parse_test(&script)?;
    let aligned = align_bytes(&items, &recorded)?;

    let started = std::time::Instant::now();
    let mut report = TopicReport::default();
    let mut connections = B::open(topic)?;
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
        let statement_started = std::time::Instant::now();
        let outcome =
            connections.with_current(&mut |session| compare(session, stmt, &block, &mut report));
        let spent = report
            .statement_time
            .entry(statement_kind(&stmt.sql))
            .or_default();
        spent.0 += 1;
        spent.1 += statement_started.elapsed();
        if matches!(outcome, Err(None)) && !stmt.expect_error {
            connections.after_refused_statement(&stmt.sql);
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
    report.elapsed = started.elapsed();
    Ok(report)
}
