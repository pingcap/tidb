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

//! Generate the parser ring's source-backed integration-fixture inventory.
//!
//! The inventory is intentionally not a parser-parity result. It makes every
//! SQL input executed by TiDB's `tests/**/t/*.test` suites visible, with the
//! fixture and source line range that produced it. A changed fixture makes
//! `--check` fail until `--write` regenerates the checked-in inventory.
//!
//! The boundary rules come from the exact `mysql-tester` revision pinned by
//! `tests/integrationtest/run-tests.sh`:
//! `github.com/pingcap/mysql-tester` `f2d90ea`, `tester.loadQueries`. It trims
//! every physical line, skips `#` lines, handles direct and `--` mysqltest
//! directives, supports both `delimiter X` and `--delimiter X`, and emits an
//! input only after its active delimiter. A recognized runner command is
//! retained in the companion directive inventory; only `Q_QUERY` payloads
//! enter the parser inventory. This scanner keeps those fixture-level rules,
//! but recognizes SQL strings and comments while finding the delimiter so a
//! delimiter inside a quoted literal or comment cannot silently masquerade as
//! a normal SQL terminator. Just like `loadQueries`, when a physical line
//! contains multiple completed SQL statements, the input ends at its *last*
//! delimiter; the preceding statements remain in the one multi-statement
//! input.
//!
//! ```text
//! cd rust
//! cargo run -p difftest --bin integration_parser_inventory -- --write
//! cargo run -p difftest --bin integration_parser_inventory -- --check
//! ```

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const SQL_INVENTORY_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/integration_parser_inventory.tsv";
const RUNNER_DIRECTIVE_INVENTORY_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/integration_runner_directive_inventory.tsv";

#[derive(Clone, Debug, Eq, PartialEq)]
struct SqlInput {
    path: String,
    start_line: usize,
    end_line: usize,
    delimiter: String,
    boundary: Boundary,
    sql: String,
}

/// A recognized mysql-tester command that is handled by the fixture runner,
/// rather than sent to the TiDB SQL connection.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RunnerDirective {
    path: String,
    start_line: usize,
    end_line: usize,
    command: String,
    payload: String,
}

impl RunnerDirective {
    fn render(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}\t{}",
            self.path,
            self.start_line,
            self.end_line,
            self.command,
            escape_tsv(&self.payload),
        )
    }
}

#[derive(Debug, Default)]
struct FixtureInputs {
    sql: Vec<SqlInput>,
    directives: Vec<RunnerDirective>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Boundary {
    Lexical,
    RunnerRawFallback,
    DirectiveQuery,
}

impl SqlInput {
    fn render(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            self.path,
            self.start_line,
            self.end_line,
            escape_tsv(&self.delimiter),
            match self.boundary {
                Boundary::Lexical => "lexical",
                Boundary::RunnerRawFallback => "runner_raw_fallback",
                Boundary::DirectiveQuery => "directive_query",
            },
            escape_tsv(&self.sql),
        )
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("difftest must live at rust/difftests")
        .to_path_buf()
}

fn sql_inventory_path(root: &Path) -> PathBuf {
    root.join(SQL_INVENTORY_RELATIVE_PATH)
}

fn runner_directive_inventory_path(root: &Path) -> PathBuf {
    root.join(RUNNER_DIRECTIVE_INVENTORY_RELATIVE_PATH)
}

fn fixture_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .expect("fixture path must be inside the repository")
        .to_string_lossy()
        .replace('\\', "/")
}

fn is_fixture(path: &Path) -> bool {
    path.extension().is_some_and(|extension| extension == "test")
        // Both pinned mysql-tester variants discover test files by walking
        // `./t/` recursively, so `t/executor/foo.test` is as authoritative as
        // a top-level `t/foo.test` fixture.
        && path.components().any(|component| component.as_os_str() == "t")
}

fn walk(current: &Path, fixtures: &mut Vec<PathBuf>) -> io::Result<()> {
    for item in fs::read_dir(current)? {
        let item = item?;
        let path = item.path();
        if item.file_type()?.is_dir() {
            walk(&path, fixtures)?;
        } else if item.file_type()?.is_file() && is_fixture(&path) {
            fixtures.push(path);
        }
    }
    Ok(())
}

fn directive_name<'a>(line: &'a str, delimiter: &str) -> Option<(&'a str, &'a str)> {
    let after_marker = line.strip_prefix("--")?;
    let after_optional_space = after_marker.strip_prefix(' ').unwrap_or(after_marker);
    let end = after_optional_space
        .find(|character: char| {
            character == '('
                || character == ' '
                || character == '\n'
                || delimiter.starts_with(character)
        })
        .unwrap_or(after_optional_space.len());
    Some((&after_optional_space[..end], &after_optional_space[end..]))
}

fn command_name<'a>(input: &'a str, delimiter: &str) -> (&'a str, &'a str) {
    let end = input
        .find(|character: char| {
            character == '('
                || character == ' '
                || character == '\n'
                || delimiter.starts_with(character)
        })
        .unwrap_or(input.len());
    (&input[..end], &input[end..])
}

fn is_known_directive(name: &str) -> bool {
    // Exact union of the `commandMap` tables in the runner revisions pinned by
    // tests/integrationtest/run-tests.sh (PingCAP f2d90ea) and
    // tests/integrationtest2/run-tests.sh (bb7133 2148bd9). Keep this list
    // source-backed: a runner-map change must update this classifier and the
    // generated directive inventory together.
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "query"
            | "connect"
            | "sleep"
            | "real_sleep"
            | "inc"
            | "dec"
            | "source"
            | "disconnect"
            | "let"
            | "echo"
            | "while"
            | "end"
            | "system"
            | "result"
            | "require"
            | "save_master_pos"
            | "sync_with_master"
            | "sync_slave_with_master"
            | "error"
            | "send"
            | "reap"
            | "dirty_close"
            | "replace_result"
            | "replace_column"
            | "ping"
            | "eval"
            | "eval_result"
            | "enable_query_log"
            | "disable_query_log"
            | "enable_result_log"
            | "disable_result_log"
            | "enable_connect_log"
            | "disable_connect_log"
            | "wait_for_slave_to_stop"
            | "enable_warnings"
            | "disable_warnings"
            | "enable_info"
            | "disable_info"
            | "enable_session_track_info"
            | "disable_session_track_info"
            | "enable_metadata"
            | "disable_metadata"
            | "exec"
            | "execw"
            | "delimiter"
            | "disable_abort_on_error"
            | "enable_abort_on_error"
            | "vertical_results"
            | "horizontal_results"
            | "query_vertical"
            | "query_horizontal"
            | "sorted_result"
            | "lowercase_result"
            | "start_timer"
            | "end_timer"
            | "character_set"
            | "disable_ps_protocol"
            | "enable_ps_protocol"
            | "disable_reconnect"
            | "enable_reconnect"
            | "if"
            | "disable_parsing"
            | "enable_parsing"
            | "replace_regex"
            | "replace_numeric_round"
            | "remove_file"
            | "file_exists"
            | "write_file"
            | "copy_file"
            | "perl"
            | "die"
            | "exit"
            | "skip"
            | "chmod"
            | "append_file"
            | "cat_file"
            | "diff_files"
            | "send_quit"
            | "change_user"
            | "mkdir"
            | "rmdir"
            | "list_files"
            | "list_files_write_file"
            | "list_files_append_file"
            | "send_shutdown"
            | "shutdown_server"
            | "result_format"
            | "move_file"
            | "remove_files_wildcard"
            | "send_eval"
            | "output"
            | "reset_connection"
            | "single_query"
            | "begin_concurrent"
            | "end_concurrent"
            | "wait_tiflash_replica_ready"
            // `tests/integrationtest2/run-tests.sh` pins the bb7133 fork of
            // mysql-tester, whose command map adds these three directives.
            | "backup_and_restore"
            | "dump_and_import"
            | "replication_checkpoint"
    )
}

fn delimiter_from_payload(payload: &str, path: &str, line: usize) -> Result<String, String> {
    // This mirrors `strings.Split(strings.TrimSpace(...), " ")` in the
    // upstream runner: it takes the first literal-space-delimited token.
    let trimmed = payload.trim();
    let delimiter = trimmed
        .split(' ')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{path}:{line}: DELIMITER needs a non-empty delimiter"))?;
    Ok(delimiter.to_owned())
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ScanState {
    Normal,
    SingleQuoted,
    DoubleQuoted,
    BacktickQuoted,
    BlockComment,
    LineComment,
}

fn last_delimiter_outside_sql_syntax(
    input: &str,
    delimiter: &str,
) -> Result<(Option<usize>, ScanState), String> {
    if delimiter.is_empty() {
        return Err("empty mysqltest delimiter".to_owned());
    }

    let bytes = input.as_bytes();
    let delimiter_bytes = delimiter.as_bytes();
    let mut state = ScanState::Normal;
    let mut last = None;
    let mut index = 0;

    while index < bytes.len() {
        match state {
            ScanState::Normal => {
                if bytes[index] == b'\'' {
                    state = ScanState::SingleQuoted;
                    index += 1;
                } else if bytes[index] == b'"' {
                    state = ScanState::DoubleQuoted;
                    index += 1;
                } else if bytes[index] == b'`' {
                    state = ScanState::BacktickQuoted;
                    index += 1;
                } else if bytes[index..].starts_with(b"/*") {
                    state = ScanState::BlockComment;
                    index += 2;
                } else if bytes[index] == b'#' {
                    state = ScanState::LineComment;
                    index += 1;
                } else if bytes[index..].starts_with(b"--")
                    && bytes.get(index + 2).is_some_and(u8::is_ascii_whitespace)
                {
                    state = ScanState::LineComment;
                    index += 2;
                } else if bytes[index..].starts_with(delimiter_bytes) {
                    last = Some(index);
                    index += delimiter_bytes.len();
                } else {
                    index += 1;
                }
            }
            ScanState::SingleQuoted => {
                if bytes[index] == b'\\' {
                    index += 2;
                } else if bytes[index] == b'\'' {
                    if bytes.get(index + 1) == Some(&b'\'') {
                        index += 2;
                    } else {
                        state = ScanState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            ScanState::DoubleQuoted => {
                if bytes[index] == b'\\' {
                    index += 2;
                } else if bytes[index] == b'"' {
                    if bytes.get(index + 1) == Some(&b'"') {
                        index += 2;
                    } else {
                        state = ScanState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            ScanState::BacktickQuoted => {
                if bytes[index] == b'`' {
                    if bytes.get(index + 1) == Some(&b'`') {
                        index += 2;
                    } else {
                        state = ScanState::Normal;
                        index += 1;
                    }
                } else {
                    index += 1;
                }
            }
            ScanState::BlockComment => {
                if bytes[index..].starts_with(b"*/") {
                    state = ScanState::Normal;
                    index += 2;
                } else {
                    index += 1;
                }
            }
            ScanState::LineComment => {
                if bytes[index] == b'\n' {
                    state = ScanState::Normal;
                }
                index += 1;
            }
        }
    }

    Ok((last, state))
}

fn append_line(buffer: &mut String, line: &str) {
    if !buffer.is_empty() {
        buffer.push('\n');
    }
    buffer.push_str(line);
}

fn push_directive(
    inputs: &mut FixtureInputs,
    path: &str,
    start_line: usize,
    end_line: usize,
    name: &str,
    payload: &str,
) {
    inputs.directives.push(RunnerDirective {
        path: path.to_owned(),
        start_line,
        end_line,
        command: name.to_ascii_lowercase(),
        payload: payload.to_owned(),
    });
}

fn parse_fixture(path: &str, source: &str) -> Result<FixtureInputs, String> {
    let mut inputs = FixtureInputs::default();
    let mut delimiter = ";".to_owned();
    let mut buffer = String::new();
    let mut start_line = None;

    for (index, raw_line) in source.lines().enumerate() {
        let line_number = index + 1;
        let line = raw_line.trim();

        if line.starts_with('#') {
            if !buffer.is_empty() {
                return Err(format!(
                    "{path}:{line_number}: comment follows unfinished SQL input"
                ));
            }
            continue;
        }

        if let Some((name, payload)) = directive_name(line, &delimiter) {
            if !buffer.is_empty() {
                return Err(format!(
                    "{path}:{line_number}: directive follows unfinished SQL input"
                ));
            }
            if !is_known_directive(name) {
                return Err(format!(
                    "{path}:{line_number}: unknown mysqltest directive --{name}"
                ));
            }
            if name.eq_ignore_ascii_case("query") {
                // `ParseQuery` classifies this command as Q_QUERY, and Run
                // executes its payload just like a normal fixture SQL input.
                // Keep the leading whitespace because it is part of the query
                // string the mysql-tester passes to the server.
                if !payload.trim().is_empty() {
                    inputs.sql.push(SqlInput {
                        path: path.to_owned(),
                        start_line: line_number,
                        end_line: line_number,
                        delimiter: delimiter.clone(),
                        boundary: Boundary::DirectiveQuery,
                        sql: payload.to_owned(),
                    });
                }
            } else if name.eq_ignore_ascii_case("let") && payload.contains('`') {
                // Q_LET executes backtick-wrapped SQL via executeStmtString.
                // Its mini-language is not in the current fixture corpus;
                // fail loudly on a future occurrence rather than omit it.
                return Err(format!(
                    "{path}:{line_number}: --let SQL expression needs explicit inventory support"
                ));
            } else {
                push_directive(&mut inputs, path, line_number, line_number, name, payload);
                if name.eq_ignore_ascii_case("delimiter") {
                    delimiter = delimiter_from_payload(payload, path, line_number)?;
                }
            }
            continue;
        }

        if line.to_ascii_lowercase().starts_with("delimiter ") {
            if !buffer.is_empty() {
                return Err(format!(
                    "{path}:{line_number}: DELIMITER follows unfinished SQL input"
                ));
            }
            let payload = &line["delimiter".len()..];
            push_directive(
                &mut inputs,
                path,
                line_number,
                line_number,
                "delimiter",
                payload,
            );
            delimiter = delimiter_from_payload(payload, path, line_number)?;
            continue;
        }

        if line.is_empty() {
            continue;
        }

        if start_line.is_none() {
            start_line = Some(line_number);
        }
        append_line(&mut buffer, line);

        let (lexical_delimiter, _state) = last_delimiter_outside_sql_syntax(&buffer, &delimiter)
            .map_err(|error| format!("{path}:{line_number}: {error}"))?;
        let Some(raw_delimiter) = buffer.rfind(&delimiter) else {
            continue;
        };
        let (last_delimiter, boundary) = if lexical_delimiter == Some(raw_delimiter) {
            (raw_delimiter, Boundary::Lexical)
        } else {
            // The upstream runner deliberately uses raw `LastIndex`, including
            // for intentionally-invalid SQL and trailing SQL comments. Keep
            // that source input visible, but label the row so a raw boundary
            // cannot be mistaken for lexically complete SQL.
            (raw_delimiter, Boundary::RunnerRawFallback)
        };

        let end = last_delimiter + delimiter.len();
        let input = buffer[..end].trim();
        let input_start_line = start_line.expect("buffer start is recorded");
        let (name, payload) = command_name(input, &delimiter);
        if is_known_directive(name) {
            // `ParseQuery` applies the same command map to unprefixed inputs
            // after `loadQueries` has found the active delimiter. In
            // particular, `connection default;` is a client-side session
            // switch, never SQL sent to TiDB.
            if name.eq_ignore_ascii_case("query") {
                if !payload.trim().is_empty() {
                    inputs.sql.push(SqlInput {
                        path: path.to_owned(),
                        start_line: input_start_line,
                        end_line: line_number,
                        delimiter: delimiter.clone(),
                        boundary: Boundary::DirectiveQuery,
                        sql: payload.to_owned(),
                    });
                }
            } else if name.eq_ignore_ascii_case("let") && payload.contains('`') {
                return Err(format!(
                    "{path}:{line_number}: let SQL expression needs explicit inventory support"
                ));
            } else {
                push_directive(
                    &mut inputs,
                    path,
                    input_start_line,
                    line_number,
                    name,
                    payload,
                );
            }
        } else if !input.is_empty() {
            inputs.sql.push(SqlInput {
                path: path.to_owned(),
                start_line: input_start_line,
                end_line: line_number,
                delimiter: delimiter.clone(),
                boundary,
                sql: input.to_owned(),
            });
        }
        buffer = buffer[end..].trim().to_owned();
        // `loadQueries` drops a trailing `#` comment after it has emitted the
        // SQL before it. Preserve that fixture behavior so the next directive
        // is not incorrectly treated as following unfinished SQL.
        if buffer.starts_with('#') {
            buffer.clear();
        }
        start_line = (!buffer.is_empty()).then_some(line_number);
    }

    if !buffer.is_empty() {
        let (_, state) = last_delimiter_outside_sql_syntax(&buffer, &delimiter)
            .map_err(|error| format!("{path}: {error}"))?;
        let reason = match state {
            ScanState::Normal | ScanState::LineComment => "missing active delimiter",
            ScanState::SingleQuoted => "unterminated single-quoted SQL literal",
            ScanState::DoubleQuoted => "unterminated double-quoted SQL literal",
            ScanState::BacktickQuoted => "unterminated backtick-quoted identifier",
            ScanState::BlockComment => "unterminated SQL block comment",
        };
        return Err(format!("{path}: unfinished SQL input ({reason}): {buffer}"));
    }
    Ok(inputs)
}

fn collect(root: &Path) -> Result<FixtureInputs, String> {
    let tests_root = root.join("tests");
    let mut fixtures = Vec::new();
    walk(&tests_root, &mut fixtures).map_err(|error| error.to_string())?;
    fixtures.sort();

    let mut inputs = FixtureInputs::default();
    for fixture in fixtures {
        let path = fixture_path(root, &fixture);
        let source = fs::read_to_string(&fixture)
            .map_err(|error| format!("failed to read {path}: {error}"))?;
        let fixture_inputs = parse_fixture(&path, &source)?;
        inputs.sql.extend(fixture_inputs.sql);
        inputs.directives.extend(fixture_inputs.directives);
    }
    Ok(inputs)
}

fn escape_tsv(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            _ if character.is_control() => {
                use std::fmt::Write;
                write!(&mut escaped, "\\u{{{:04X}}}", character as u32)
                    .expect("write to String cannot fail");
            }
            _ => escaped.push(character),
        }
    }
    escaped
}

fn render(root: &Path) -> Result<(String, String), String> {
    let inputs = collect(root)?;
    let mut sql_output =
        String::from("source_path\tsource_start_line\tsource_end_line\tdelimiter\tboundary\tsql\n");
    for input in inputs.sql {
        sql_output.push_str(&input.render());
        sql_output.push('\n');
    }
    let mut directive_output =
        String::from("source_path\tsource_start_line\tsource_end_line\tcommand\tpayload\n");
    for directive in inputs.directives {
        directive_output.push_str(&directive.render());
        directive_output.push('\n');
    }
    Ok((sql_output, directive_output))
}

fn check(root: &Path) -> Result<(), String> {
    let (expected_sql, expected_directives) = render(root)?;
    let inventories = [
        (sql_inventory_path(root), expected_sql),
        (runner_directive_inventory_path(root), expected_directives),
    ];
    for (path, expected) in inventories {
        let actual = fs::read_to_string(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        if actual != expected {
            return Err(format!(
                "parser fixture inventory is stale; regenerate it with `cd rust && cargo run -p difftest --bin integration_parser_inventory -- --write` ({})",
                path.display()
            ));
        }
    }
    Ok(())
}

fn write(root: &Path) -> Result<(), String> {
    let (sql_output, directive_output) = render(root)?;
    let sql_path = sql_inventory_path(root);
    let directive_path = runner_directive_inventory_path(root);
    fs::create_dir_all(
        sql_path
            .parent()
            .expect("inventory has a parent coverage directory"),
    )
    .map_err(|error| format!("failed to create inventory directory: {error}"))?;
    fs::write(&sql_path, sql_output)
        .map_err(|error| format!("failed to write {}: {error}", sql_path.display()))?;
    fs::write(&directive_path, directive_output)
        .map_err(|error| format!("failed to write {}: {error}", directive_path.display()))
}

fn main() {
    let arguments: Vec<String> = env::args().skip(1).collect();
    let root = repo_root();
    let result = match arguments.as_slice() {
        [command] if command == "--write" => write(&root),
        [command] if command == "--check" => check(&root),
        [] => match collect(&root) {
            Ok(inputs) => {
                println!(
                    "{} parser-ring SQL inputs; {} runner directives",
                    inputs.sql.len(),
                    inputs.directives.len()
                );
                Ok(())
            }
            Err(error) => Err(error),
        },
        _ => Err("usage: integration_parser_inventory [--write|--check]".to_owned()),
    };
    if let Err(error) = result {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{check, parse_fixture, write};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn preserves_multiline_sql_and_ignores_semicolons_in_sql_syntax() {
        let source = r#"
# Test comments are not SQL.
--sorted_result
SELECT
  'single;quote', "double;quote", `back;tick`, 'doubled '' ; quote', /* block ; comment */ 1;
--error 1064
SELECT 'second;input';
"#;
        let inputs = parse_fixture("tests/integrationtest/t/fixture.test", source).unwrap();
        assert_eq!(inputs.sql.len(), 2);
        assert_eq!(inputs.sql[0].start_line, 4);
        assert_eq!(inputs.sql[0].end_line, 5);
        assert_eq!(inputs.sql[0].boundary, super::Boundary::Lexical);
        assert_eq!(
            inputs.sql[0].sql,
            "SELECT\n'single;quote', \"double;quote\", `back;tick`, 'doubled '' ; quote', /* block ; comment */ 1;"
        );
        assert_eq!(inputs.sql[1].start_line, 7);
        assert_eq!(inputs.sql[1].end_line, 7);
        assert_eq!(inputs.sql[1].sql, "SELECT 'second;input';");
    }

    #[test]
    fn lexical_scanner_ignores_quoted_and_commented_delimiters() {
        let sql = "SELECT 'single;quote', \"double;quote\", `back;tick`, /* block ; comment */ 1;";
        let (delimiter, state) = super::last_delimiter_outside_sql_syntax(sql, ";").unwrap();
        assert_eq!(state, super::ScanState::Normal);
        assert_eq!(delimiter, sql.rfind(';'));
    }

    #[test]
    fn follows_mysql_tester_last_delimiter_and_custom_delimiters() {
        let source = r#"
select 1; select 'still one input';
delimiter $$
CREATE PROCEDURE p()
BEGIN
  SELECT 'semicolon; stays in body';
END$$
--delimiter ;
SELECT 3;
"#;
        let inputs = parse_fixture("tests/integrationtest/t/fixture.test", source).unwrap();
        assert_eq!(inputs.sql.len(), 3);
        assert_eq!(inputs.sql[0].sql, "select 1; select 'still one input';");
        assert_eq!(inputs.sql[0].start_line, 2);
        assert_eq!(inputs.sql[0].end_line, 2);
        assert_eq!(inputs.sql[1].delimiter, "$$");
        assert_eq!(
            inputs.sql[1].sql,
            "CREATE PROCEDURE p()\nBEGIN\nSELECT 'semicolon; stays in body';\nEND$$"
        );
        assert_eq!(inputs.sql[2].sql, "SELECT 3;");
    }

    #[test]
    fn rejects_unfinished_sql_before_directives_and_unknown_directives() {
        let unfinished = "SELECT 1\n--error 1064\nSELECT 2;\n";
        assert!(parse_fixture("fixture.test", unfinished)
            .unwrap_err()
            .contains("directive follows unfinished SQL input"));
        assert!(
            parse_fixture("fixture.test", "--not_a_command\nSELECT 1;\n")
                .unwrap_err()
                .contains("unknown mysqltest directive")
        );
    }

    #[test]
    fn retains_invalid_sql_that_the_upstream_raw_delimiter_runner_executes() {
        let source = "--error 1064\nINSERT INTO t VALUES ('unterminated);\n";
        let inputs = parse_fixture("fixture.test", source).unwrap();
        assert_eq!(inputs.sql.len(), 1);
        assert_eq!(inputs.sql[0].boundary, super::Boundary::RunnerRawFallback);
        assert_eq!(inputs.sql[0].sql, "INSERT INTO t VALUES ('unterminated);");
    }

    #[test]
    fn labels_a_trailing_comment_delimiter_that_mysql_tester_uses_raw() {
        let source = "SELECT 1; -- the runner's last raw semicolon ;\n";
        let inputs = parse_fixture("fixture.test", source).unwrap();
        assert_eq!(inputs.sql.len(), 1);
        assert_eq!(inputs.sql[0].boundary, super::Boundary::RunnerRawFallback);
        assert_eq!(
            inputs.sql[0].sql,
            "SELECT 1; -- the runner's last raw semicolon ;"
        );
    }

    #[test]
    fn retains_the_sql_payload_of_the_query_directive() {
        let inputs = parse_fixture("fixture.test", "--query SELECT 'directive query';\n").unwrap();
        assert_eq!(inputs.sql.len(), 1);
        assert_eq!(inputs.sql[0].boundary, super::Boundary::DirectiveQuery);
        assert_eq!(inputs.sql[0].sql, " SELECT 'directive query';");
    }

    #[test]
    fn accounts_for_direct_and_comment_runner_commands_without_misclassifying_them_as_sql() {
        let source = r#"
connect (conn1,localhost,root,,test);
connection conn1;
--connection default;
disconnect conn1;
query SELECT 'direct query';
--query SELECT 'comment query';
SELECT 'ordinary SQL';
"#;
        let inputs = parse_fixture("fixture.test", source).unwrap();
        assert_eq!(inputs.sql.len(), 3);
        assert_eq!(inputs.sql[0].sql, " SELECT 'direct query';");
        assert_eq!(inputs.sql[1].sql, " SELECT 'comment query';");
        assert_eq!(inputs.sql[2].sql, "SELECT 'ordinary SQL';");
        assert_eq!(
            inputs
                .directives
                .iter()
                .map(|directive| (directive.command.as_str(), directive.payload.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("connect", " (conn1,localhost,root,,test);"),
                ("connection", " conn1;"),
                ("connection", " default;"),
                ("disconnect", " conn1;"),
            ]
        );
        assert_eq!(inputs.sql.len() + inputs.directives.len(), 7);
    }

    #[test]
    fn fixture_change_makes_the_written_inventory_stale() {
        let root = std::env::temp_dir().join(format!(
            "tidb-parser-inventory-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock is after Unix epoch")
                .as_nanos()
        ));
        let fixture = root.join("tests/integrationtest/t/sample.test");
        fs::create_dir_all(fixture.parent().unwrap()).unwrap();
        fs::write(&fixture, "connection default;\nSELECT 1;\n").unwrap();
        write(&root).unwrap();
        check(&root).unwrap();
        fs::write(&fixture, "connection conn1;\nSELECT 1;\n").unwrap();
        assert!(check(&root).unwrap_err().contains("inventory is stale"));
        fs::remove_dir_all(root).unwrap();
    }
}
