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

//! Reader for TiDB's own record/replay suite at `tests/integrationtest/`.
//!
//! A topic there is a pair: `t/<topic>.test` holds the statements and
//! `r/<topic>.result` holds the RECORDED output -- each statement echoed
//! verbatim, then its tab-separated rows. That recording is the authoritative
//! oracle the TiDB project gates on, so this module only ever READS it.
//!
//! The alignment problem this solves: the `.result` file has no statement
//! delimiter of its own. It is a flat line stream in which a statement's echo
//! is indistinguishable from a result row that happens to carry the same
//! text. What makes it parseable is that the `.test` file already knows the
//! whole echo sequence, so the echoes are located IN ORDER by matching each
//! next item's complete (multi-line) echo, and everything between two echoes
//! is the first one's output. A topic whose echoes do not line up is rejected
//! outright rather than silently mis-attributed to the wrong statement.
//!
//! Leading indentation is dropped from the echo by the recorder (a
//! `create table t (\n  a int,\n)` records its `a int,` unindented), so
//! matching is per-line trimmed on both sides.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

const DUAL_COLLATION_RECORDINGS: &[&str] = &[
    "collation_agg_func",
    "collation_check_use_collation",
    "collation_misc",
    "collation_pointget",
];

/// Returns the recording that corresponds to this process's collation mode.
///
/// The four collation topics have no unsuffixed result: TiDB records an
/// enabled and a disabled oracle separately. Every other topic keeps the
/// ordinary `r/<topic>.result` path.
pub fn recording_path(integrationtest_dir: &Path, topic: &str) -> PathBuf {
    let suffix = if DUAL_COLLATION_RECORDINGS.contains(&topic) {
        if tidb_datatype::new_collation_enabled() {
            "_enabled"
        } else {
            "_disabled"
        }
    } else {
        ""
    };
    integrationtest_dir.join(format!("r/{topic}{suffix}.result"))
}

/// One `.test` item that produces output in the `.result` stream.
#[derive(Debug)]
pub enum Item {
    /// An `--echo <text>` directive, which records `<text>` and nothing else.
    Echo(String),
    /// A SQL statement.
    Stmt(Stmt),
    /// A connection command, which records NOTHING: it drives the harness,
    /// not the server. See [`ConnectionCmd`].
    Connection(ConnectionCmd),
}

pub type AlignedBytes<'a> = Vec<(&'a Item, Vec<Vec<u8>>)>;

/// One mysqltest connection command.
///
/// These are the three commands that make a script drive SEVERAL sessions.
/// The forms modelled here are exactly the forms the suite uses -- derived
/// from the scripts themselves, not from a general grammar:
///
/// ```text
/// connect (conn1, localhost, root,, executor__write)   -- 70 occurrences
/// connect (conn1, localhost, root,)                    -- password and db omitted
/// connection conn1;   connection default;              -- 200 occurrences
/// disconnect conn1;                                    -- 100 occurrences
/// ```
///
/// A form outside those is refused by [`parse_test`] rather than guessed, for
/// the same reason an unmodelled directive is: a connection command read
/// wrongly attributes one session's statements to another.
#[derive(Debug, PartialEq, Eq)]
pub enum ConnectionCmd {
    /// `connect (<name>, <host>, <user>, <password>, <db>)`: opens a NEW
    /// connection and makes it current. `db` is empty when the script omits
    /// it. The password is parsed but not carried -- every script gives the
    /// account's real password -- while the HOST is: TiDB matches an account
    /// row by `user` AND `host`, and the suite creates accounts for
    /// `localhost` as often as for `%` (see `mysqltest_connections`).
    Open {
        name: String,
        host: String,
        user: String,
        db: String,
    },
    /// `connection <name>`: makes an already-open connection current.
    /// `default` names the connection the script started on.
    Switch(String),
    /// `disconnect <name>`: closes a connection.
    Close(String),
}

/// Parses one `connect`/`connection`/`disconnect` line, or refuses it.
fn parse_connection_cmd(line: &str) -> Result<ConnectionCmd, String> {
    let body = line.trim().trim_end_matches(';').trim();
    let (verb, rest) = body
        .split_once(['(', ' '])
        .map_or((body, ""), |(v, r)| (v.trim(), r.trim()));
    match verb {
        "connection" if !rest.is_empty() => Ok(ConnectionCmd::Switch(rest.to_owned())),
        "disconnect" if !rest.is_empty() => Ok(ConnectionCmd::Close(rest.to_owned())),
        "connect" => {
            // Both spellings occur: `connect (conn1, ...)` and `connect(conn1,
            // ...)`, so the paren is stripped here rather than by the split
            // above.
            let inner = rest
                .trim_start_matches('(')
                .trim()
                .strip_suffix(')')
                .ok_or_else(|| format!("unclosed `connect (` in `{line}`"))?;
            let fields: Vec<&str> = inner.split(',').map(str::trim).collect();
            // name, host, user, password, and an optional db: the shortest
            // form in the suite omits the db and leaves the password empty.
            if fields.len() < 4 || fields.len() > 5 || fields[0].is_empty() || fields[2].is_empty()
            {
                return Err(format!("unmodelled `connect` form `{line}`"));
            }
            Ok(ConnectionCmd::Open {
                name: fields[0].to_owned(),
                host: fields[1].to_owned(),
                user: fields[2].to_owned(),
                db: fields.get(4).copied().unwrap_or_default().to_owned(),
            })
        }
        _ => Err(format!("unmodelled connection command `{line}`")),
    }
}

/// One statement of a `.test` script with the directives that govern how its
/// recorded output must be read.
#[derive(Debug)]
pub struct Stmt {
    /// The statement text as written, newlines preserved, `;` included.
    pub sql: String,
    /// `--error <code>` preceded this statement: the recording is an
    /// `Error ...` line, not rows.
    pub expect_error: bool,
    /// `--sorted_result` preceded this statement: the recorder sorted the row
    /// lines before writing them, so a comparison must sort too.
    pub sorted: bool,
    /// `--enable_warnings` was in effect: the recorder APPENDED this
    /// statement's `SHOW WARNINGS` to its rows. See [`split_warnings`] --
    /// this does not make the output uncomparable, it makes it two blocks.
    pub warnings: bool,
    /// Why this statement's recorded output is not directly comparable, if it
    /// is not: the name of the mysqltest feature that rewrote or extended it.
    /// Empty means directly comparable.
    pub blocker: Option<&'static str>,
}

impl Item {
    /// The lines this item contributes to the `.result` stream before its
    /// output, each trimmed the way the recorder writes them.
    fn echo_lines(&self) -> Vec<&str> {
        match self {
            Item::Echo(text) => vec![text.as_str()],
            Item::Stmt(stmt) => stmt.sql.lines().map(str::trim).collect(),
            Item::Connection(_) => Vec::new(),
        }
    }
}

/// Directives that apply to the next statement only, paired with the reason
/// its recorded output is not directly comparable.
const ONE_SHOT_BLOCKERS: &[(&str, &str)] = &[
    ("replace_regex", "recorder rewrote the output by regex"),
    ("replace_column", "recorder replaced a column of the output"),
];

/// Directives that stay in effect until their counterpart, each paired with
/// the reason the output they cover is not directly comparable. Modes are
/// tracked independently: `--disable_warnings` must not clear `--enable_info`.
const MODE_BLOCKERS: &[(&str, &str, &str)] = &[
    (
        "enable_info",
        "disable_info",
        "recorded affected-rows info line",
    ),
    (
        "disable_result_log",
        "enable_result_log",
        "output suppressed by the recorder",
    ),
];

/// Drops a `#` comment that follows the statement terminator on the same line.
///
/// The `#` must be OUTSIDE a string or identifier quote and the text before it
/// must end with `;`, so `select 'a # b';` and `select 1; # note` are told
/// apart by the same scan rather than by a special case. A `#` that opens a
/// comment before any terminator (a whole-line comment) is handled by the
/// caller and never reaches here.
fn strip_trailing_comment(line: &str) -> &str {
    let mut quote: Option<char> = None;
    for (at, ch) in line.char_indices() {
        match (quote, ch) {
            (Some(open), c) if c == open => quote = None,
            (Some(_), _) => {}
            (None, '\'' | '"' | '`') => quote = Some(ch),
            (None, '#') if line[..at].trim_end().ends_with(';') => {
                return line[..at].trim_end();
            }
            (None, _) => {}
        }
    }
    line
}

/// Parses a `.test` script into the items that produce recorded output.
///
/// Fails on a directive this reader does not model rather than guessing: an
/// unmodelled directive can silently shift every following statement's
/// expected output onto the wrong statement.
pub fn parse_test(text: &str) -> Result<Vec<Item>, String> {
    let mut items = Vec::new();
    let mut pending: Option<&'static str> = None;
    let mut modes = [false; MODE_BLOCKERS.len()];
    let mut expect_error = false;
    let mut sorted = false;
    let mut warnings = false;
    let mut buffer: Vec<&str> = Vec::new();

    for raw in text.lines() {
        let line = raw.trim_end();
        let trimmed = line.trim();
        // A blank line inside a statement is dropped, not kept: the recorder
        // echoes `select ...\n\n(a > 1) ...;` with its two SQL lines adjacent,
        // so keeping the blank would put a line in the echo that the recording
        // does not have -- and every following statement would then be read
        // against the wrong block.
        if trimmed.is_empty() || (buffer.is_empty() && trimmed.starts_with('#')) {
            continue;
        }
        if buffer.is_empty() && trimmed.starts_with("--") {
            let body = trimmed.trim_start_matches('-').trim();
            let (name, rest) = body.split_once(' ').unwrap_or((body, ""));
            // A stray `;` after a bare directive (`--enable_warnings;`) is
            // part of no statement -- the recorder reads the directive and
            // drops it. An `--echo` argument keeps its own text intact.
            let name = name.trim_end_matches(';');
            match name {
                "error" => expect_error = true,
                "sorted_result" => sorted = true,
                "enable_warnings" => warnings = true,
                "disable_warnings" => warnings = false,
                "echo" => items.push(Item::Echo(rest.to_owned())),
                _ => {
                    if let Some((_, reason)) = ONE_SHOT_BLOCKERS.iter().find(|(d, _)| *d == name) {
                        pending = Some(reason);
                    } else if let Some(slot) = MODE_BLOCKERS.iter().position(|(on, ..)| *on == name)
                    {
                        modes[slot] = true;
                    } else if let Some(slot) =
                        MODE_BLOCKERS.iter().position(|(_, off, _)| *off == name)
                    {
                        modes[slot] = false;
                    } else {
                        return Err(format!("unmodelled directive `{name}`"));
                    }
                }
            }
            continue;
        }
        // `connect`/`connection`/`disconnect` are mysqltest commands, not SQL:
        // the script drives SEVERAL sessions. They record nothing of their
        // own, and an unmodelled form is refused rather than dropped -- a
        // dropped connection command silently runs one session's statements
        // on another.
        if buffer.is_empty() {
            let first = trimmed.split(['(', ' ', ';']).next().unwrap_or("");
            if matches!(first, "connect" | "connection" | "disconnect") {
                items.push(Item::Connection(parse_connection_cmd(trimmed)?));
                continue;
            }
        }
        // A `#` comment after the terminator belongs to the SCRIPT, not the
        // statement: the recorder echoes `insert into t values(1);` for
        // `insert into t values(1); # why`. Dropping it here is what makes the
        // line terminate at all -- left in place the line does not end with
        // `;`, so every following line (directives included) is swallowed into
        // the same statement.
        let line = strip_trailing_comment(line);
        let trimmed = line.trim();
        buffer.push(line);
        if !trimmed.ends_with(';') {
            continue;
        }
        // An EMPTY statement -- a line that is nothing but the terminator --
        // is a script artifact the recorder writes nothing for, so it produces
        // no item. Any pending directive stays pending: the recorder likewise
        // carries it to the next statement that actually runs.
        if buffer.iter().all(|l| l.trim().trim_matches(';').is_empty()) {
            buffer.clear();
            continue;
        }
        items.push(Item::Stmt(Stmt {
            sql: buffer.join("\n"),
            expect_error,
            sorted,
            warnings,
            blocker: pending.take().or_else(|| {
                modes
                    .iter()
                    .position(|on| *on)
                    .map(|slot| MODE_BLOCKERS[slot].2)
            }),
        }));
        buffer.clear();
        expect_error = false;
        sorted = false;
    }
    if !buffer.is_empty() {
        return Err(format!("unterminated statement `{}`", buffer.join(" ")));
    }
    Ok(items)
}

/// Attaches each item's recorded output block from the `.result` stream.
///
/// See the module docs for why the echoes are located in order rather than
/// parsed out of the result file on their own.
pub fn align<'a>(items: &'a [Item], result: &str) -> Result<Vec<(&'a Item, Vec<String>)>, String> {
    align_bytes(items, result.as_bytes())?
        .into_iter()
        .map(|(item, block)| {
            block
                .into_iter()
                .map(|line| {
                    String::from_utf8(line)
                        .map_err(|error| format!("recorded output is not valid UTF-8: {error}"))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|block| (item, block))
        })
        .collect()
}

/// Byte-preserving form of [`align`].
///
/// Several charset integration recordings deliberately contain invalid UTF-8.
/// Their raw result cells are part of the oracle, so decoding the entire file
/// before alignment would either reject those topics or corrupt the bytes that
/// need to be compared.
pub fn align_bytes<'a>(items: &'a [Item], result: &[u8]) -> Result<AlignedBytes<'a>, String> {
    let mut lines: Vec<&[u8]> = result
        .split(|byte| *byte == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .collect();
    if lines.last().is_some_and(|line| line.is_empty()) {
        lines.pop();
    }
    let mut out = Vec::with_capacity(items.len());
    let mut cursor = 0usize;

    for (index, item) in items.iter().enumerate() {
        let echo = item.echo_lines();
        if !matches_at_bytes(&lines, cursor, &echo) {
            return Err(format!(
                "recorded output does not echo item {index} (`{}`) at result line {}",
                echo.join(" "),
                cursor + 1
            ));
        }
        cursor += echo.len();

        // The block runs to wherever the NEXT item's whole echo begins; the
        // last item owns the rest of the file. An item that records NOTHING
        // (a connection command) cannot be that terminator: an empty echo
        // matches immediately and would give the item before it an empty
        // block, so the search skips to the next item that records something.
        let end = match items[index + 1..]
            .iter()
            .find(|next| !next.echo_lines().is_empty())
        {
            Some(next) => {
                let next_echo = next.echo_lines();
                let mut probe = cursor;
                loop {
                    if probe > lines.len() {
                        return Err(format!(
                            "item {index} is not closed by the next echo (`{}`)",
                            next_echo.join(" ")
                        ));
                    }
                    if matches_at_bytes(&lines, probe, &next_echo) {
                        break probe;
                    }
                    probe += 1;
                }
            }
            None => lines.len(),
        };
        out.push((
            item,
            lines[cursor..end]
                .iter()
                .map(|line| line.to_vec())
                .collect(),
        ));
        cursor = end;
    }
    Ok(out)
}

/// The header mysqltest writes before an appended `SHOW WARNINGS` block.
pub const WARNINGS_HEADER: &str = "Level\tCode\tMessage";

/// Splits a statement's recorded block into its own output and the
/// `SHOW WARNINGS` block `--enable_warnings` appended to it.
///
/// Under `--enable_warnings` mysqltest does not REWRITE a statement's output:
/// after writing the rows it runs `SHOW WARNINGS` and appends that result set
/// verbatim -- header line included -- and appends nothing at all when the
/// statement warned about nothing. So the recording stays fully comparable as
/// long as the two halves are told apart, which the header line does.
///
/// Returned as `(rows, warnings)` where `warnings` is `None` when the recorder
/// appended no block, i.e. the statement produced NO warnings. That is an
/// assertion in its own right -- an engine that warns where TiDB did not
/// diverges -- so it must not be confused with an empty block, which cannot
/// occur: the header is written whenever anything is.
///
/// Only called for a statement whose `warnings` flag is set. Outside that mode
/// a row reading `Level\tCode\tMessage` is ordinary data, and splitting on it
/// would cut a result set in half.
pub fn split_warnings(recorded: &[String]) -> (&[String], Option<&[String]>) {
    match recorded.iter().position(|l| l.trim() == WARNINGS_HEADER) {
        Some(at) => (&recorded[..at], Some(&recorded[at + 1..])),
        None => (recorded, None),
    }
}

/// Byte-preserving form of [`split_warnings`].
pub fn split_warnings_bytes(recorded: &[Vec<u8>]) -> (&[Vec<u8>], Option<&[Vec<u8>]>) {
    match recorded
        .iter()
        .position(|line| trim_ascii(line) == WARNINGS_HEADER.as_bytes())
    {
        Some(at) => (&recorded[..at], Some(&recorded[at + 1..])),
        None => (recorded, None),
    }
}

fn matches_at(lines: &[&str], at: usize, echo: &[&str]) -> bool {
    at + echo.len() <= lines.len()
        && lines[at..at + echo.len()]
            .iter()
            .zip(echo)
            .all(|(have, want)| have.trim() == *want)
}

fn matches_at_bytes(lines: &[&[u8]], at: usize, echo: &[&str]) -> bool {
    at + echo.len() <= lines.len()
        && lines[at..at + echo.len()]
            .iter()
            .zip(echo)
            .all(|(have, want)| trim_ascii(have) == want.as_bytes())
}

fn trim_ascii(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligns_statements_with_their_recorded_blocks() {
        let script = "# a comment\n--sorted_result\nselect a from t;\ninsert into t values (1);\n\
                      -- error 1054\nselect nope from t;\n";
        let recorded = "select a from t;\na\n1\n2\ninsert into t values (1);\n\
                        select nope from t;\nError 1054 (42S22): unknown column\n";
        let items = parse_test(script).unwrap();
        let aligned = align(&items, recorded).unwrap();
        assert_eq!(aligned.len(), 3);
        assert_eq!(aligned[0].1, vec!["a", "1", "2"]);
        assert!(matches!(aligned[0].0, Item::Stmt(s) if s.sorted && !s.expect_error));
        assert!(aligned[1].1.is_empty());
        assert!(matches!(aligned[2].0, Item::Stmt(s) if s.expect_error));
    }

    #[test]
    fn byte_alignment_preserves_non_utf8_result_cells() {
        let items = parse_test("select a from t;\nselect 2;\n").unwrap();
        let recorded = b"select a from t;\na\n\xff\nselect 2;\n2\n2\n";
        let aligned = align_bytes(&items, recorded).unwrap();
        assert_eq!(aligned[0].1, vec![b"a".to_vec(), b"\xff".to_vec()]);
        assert_eq!(aligned[1].1, vec![b"2".to_vec(), b"2".to_vec()]);
    }

    #[test]
    fn collation_topics_select_the_recording_for_the_live_mode() {
        let root = Path::new("/integrationtest");
        let suffix = if tidb_datatype::new_collation_enabled() {
            "enabled"
        } else {
            "disabled"
        };
        assert_eq!(
            recording_path(root, "collation_misc"),
            root.join(format!("r/collation_misc_{suffix}.result"))
        );
        assert_eq!(
            recording_path(root, "executor/charset"),
            root.join("r/executor/charset.result")
        );
    }

    #[test]
    fn multi_line_statement_echo_is_matched_unindented() {
        let items = parse_test("create table t (\n  a int\n);\n").unwrap();
        let aligned = align(&items, "create table t (\na int\n);\n").unwrap();
        assert_eq!(aligned.len(), 1);
        assert!(aligned[0].1.is_empty());
    }

    #[test]
    fn sticky_directive_marks_every_statement_it_covers() {
        let items = parse_test("--enable_info\nselect 1;\n--disable_info\nselect 2;\n").unwrap();
        let blockers: Vec<_> = items
            .iter()
            .map(|i| match i {
                Item::Stmt(s) => s.blocker,
                Item::Echo(_) | Item::Connection(_) => None,
            })
            .collect();
        assert_eq!(
            blockers,
            vec![Some("recorded affected-rows info line"), None]
        );
    }

    #[test]
    fn enable_warnings_is_a_split_not_a_blocker() {
        let items =
            parse_test("--enable_warnings\nselect 1;\n--disable_warnings\nselect 2;\n").unwrap();
        let flags: Vec<_> = items
            .iter()
            .map(|i| match i {
                Item::Stmt(s) => (s.warnings, s.blocker),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(flags, vec![(true, None), (false, None)]);
    }

    #[test]
    fn a_recorded_warnings_block_is_split_off_from_the_rows() {
        // Shaped after `executor/issues.result`: rows, then the header, then
        // the warning lines.
        let block: Vec<String> = [
            "cast(a as time)",
            "NULL",
            "Level\tCode\tMessage",
            "Warning\t1292\tTruncated incorrect time value",
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        let (rows, warnings) = split_warnings(&block);
        assert_eq!(rows, &block[..2]);
        assert_eq!(warnings, Some(&block[3..]));

        // MUTATION PROBE: a recording whose warning text differs from the
        // engine's must be visible AS A WARNING difference, not swallowed
        // into the rows.
        let mutated: Vec<String> = block[..3]
            .iter()
            .cloned()
            .chain(["Warning\t1292\tsomething else".to_owned()])
            .collect();
        let (mutated_rows, mutated_warnings) = split_warnings(&mutated);
        assert_eq!(mutated_rows, rows, "the rows half is unaffected");
        assert_ne!(mutated_warnings, warnings);

        // CONTROL: a statement that warned about nothing has no header, so
        // the whole block stays the rows and `None` records the ABSENCE --
        // an engine that warns here diverges.
        let quiet: Vec<String> = block[..2].to_vec();
        assert_eq!(split_warnings(&quiet), (&quiet[..], None));
    }

    #[test]
    fn connection_commands_record_nothing_and_do_not_split_a_block() {
        let items = parse_test(
            "connect (conn1, localhost, u1,, db1);\nselect a from t;\ndisconnect conn1;\n\
             connection default;\nselect 2;\n",
        )
        .unwrap();
        assert_eq!(
            items
                .iter()
                .filter_map(|i| match i {
                    Item::Connection(cmd) => Some(cmd),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![
                &ConnectionCmd::Open {
                    name: "conn1".to_owned(),
                    host: "localhost".to_owned(),
                    user: "u1".to_owned(),
                    db: "db1".to_owned()
                },
                &ConnectionCmd::Close("conn1".to_owned()),
                &ConnectionCmd::Switch("default".to_owned()),
            ]
        );
        // The two rows belong to `select a from t`, even though a command that
        // records nothing sits between it and the next statement.
        let aligned = align(&items, "select a from t;\na\n1\nselect 2;\n2\n2\n").unwrap();
        assert_eq!(aligned[1].1, vec!["a", "1"]);
        assert_eq!(aligned[4].1, vec!["2", "2"]);
    }

    #[test]
    fn connect_without_a_database_keeps_the_form_and_a_bad_form_is_refused() {
        assert_eq!(
            parse_connection_cmd("connect (conn1, localhost, root,)").unwrap(),
            ConnectionCmd::Open {
                name: "conn1".to_owned(),
                host: "localhost".to_owned(),
                user: "root".to_owned(),
                db: String::new()
            }
        );
        assert!(parse_connection_cmd("connect (conn1, localhost)").is_err());
        assert!(parse_connection_cmd("connection").is_err());
    }

    #[test]
    fn comment_after_the_terminator_is_dropped_and_does_not_extend_the_statement() {
        let items = parse_test(
            "insert into t values(1); # the maximum\n--error 1467\ninsert into t values();\n",
        )
        .unwrap();
        let sql: Vec<_> = items
            .iter()
            .map(|i| match i {
                Item::Stmt(s) => (s.sql.as_str(), s.expect_error),
                _ => unreachable!(),
            })
            .collect();
        // Without the strip, the `--error` line lands INSIDE the first
        // statement and the second statement never gets its directive.
        assert_eq!(
            sql,
            vec![
                ("insert into t values(1);", false),
                ("insert into t values();", true)
            ]
        );
        // A `#` inside a quoted string is data, not a comment.
        assert_eq!(
            strip_trailing_comment("select 'a; # b';"),
            "select 'a; # b';"
        );
        // A `#` before any terminator does not end a statement either.
        assert_eq!(strip_trailing_comment("select 1 # x"), "select 1 # x");
    }

    #[test]
    fn a_bare_terminator_produces_no_item_and_keeps_a_pending_directive() {
        let items =
            parse_test("insert into t values (1);\n;\n--sorted_result\n;\nselect a;\n").unwrap();
        assert_eq!(items.len(), 2);
        assert!(matches!(&items[1], Item::Stmt(s) if s.sql == "select a;" && s.sorted));
        // And the recording, which writes nothing for either bare `;`, aligns.
        let aligned = align(&items, "insert into t values (1);\nselect a;\na\n1\n").unwrap();
        assert_eq!(aligned[1].1, vec!["a", "1"]);
    }

    #[test]
    fn unmodelled_directive_is_refused_rather_than_guessed() {
        assert!(parse_test("--connect (a,b)\nselect 1;\n").is_err());
    }
}
