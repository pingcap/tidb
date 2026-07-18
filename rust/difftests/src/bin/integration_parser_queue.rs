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

//! Produce a deterministic parser-porting queue from the checked Go oracle.
//!
//! This binary never starts Go. `integration_parser_golden --write` is the
//! deliberate Go-dependent operation; this queue only checks that the static
//! golden still names the current fixture inventory, then replays the Rust
//! parser and groups every non-match into a small task-selection report.
//!
//! ```text
//! cd rust
//! cargo run -p difftest --bin integration_parser_queue -- --check
//! ```

#[path = "integration_parser_golden.rs"]
#[allow(dead_code)]
mod integration_parser_golden;

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;

use integration_parser_golden::{read_golden, repo_root, GoOutcome, GoldenRecord, Input};

const INVENTORY_RELATIVE_PATH: &str =
    "rust/difftests/corpus/coverage/integration_parser_inventory.tsv";
const INVENTORY_HEADER: &str =
    "source_path\tsource_start_line\tsource_end_line\tdelimiter\tboundary\tsql";
const MAX_EXAMPLES_PER_QUEUE: usize = 3;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RustOutcome {
    Matched,
    MultiStatementMatched,
    /// Go and Rust both reject the input. It is evidence of rejection parity,
    /// not a parser-porting obligation.
    RejectedAsExpected,
    ParseFailure,
    RestoreMismatch,
    AcceptedGoRejected,
    AcceptedGoRestoreFailure,
}

impl RustOutcome {
    const ALL: [Self; 7] = [
        Self::Matched,
        Self::RejectedAsExpected,
        Self::ParseFailure,
        Self::RestoreMismatch,
        Self::AcceptedGoRejected,
        Self::MultiStatementMatched,
        Self::AcceptedGoRestoreFailure,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "rust_matched",
            Self::RejectedAsExpected => "rust_rejected_as_expected",
            Self::ParseFailure => "rust_parse_failure",
            Self::RestoreMismatch => "rust_restore_mismatch",
            Self::AcceptedGoRejected => "rust_accepted_go_rejected",
            Self::MultiStatementMatched => "rust_multi_statement_matched",
            Self::AcceptedGoRestoreFailure => "rust_accepted_go_restore_failure",
        }
    }

    fn is_match(self) -> bool {
        matches!(
            self,
            Self::Matched | Self::RejectedAsExpected | Self::MultiStatementMatched
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InventoryInput {
    path: String,
    start_line: usize,
    end_line: usize,
    delimiter: String,
    boundary: String,
    sql: String,
}

impl From<&Input> for InventoryInput {
    fn from(input: &Input) -> Self {
        Self {
            path: input.path.clone(),
            start_line: input.start_line,
            end_line: input.end_line,
            delimiter: input.delimiter.clone(),
            boundary: input.boundary.clone(),
            sql: input.sql.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct Example {
    path: String,
    start_line: usize,
    end_line: usize,
    boundary: String,
    sql: String,
}

#[derive(Default)]
struct QueueGroup {
    count: usize,
    examples: Vec<Example>,
}

fn unescape_tsv(value: &str) -> Result<String, String> {
    let mut output = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            output.push(character);
            continue;
        }
        let escaped = chars
            .next()
            .ok_or_else(|| "truncated TSV escape".to_owned())?;
        match escaped {
            '\\' => output.push('\\'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            'u' => {
                if chars.next() != Some('{') {
                    return Err("invalid Unicode TSV escape".to_owned());
                }
                let mut hex = String::new();
                loop {
                    let digit = chars
                        .next()
                        .ok_or_else(|| "unterminated Unicode TSV escape".to_owned())?;
                    if digit == '}' {
                        break;
                    }
                    hex.push(digit);
                }
                let code_point = u32::from_str_radix(&hex, 16)
                    .map_err(|_| format!("invalid Unicode TSV escape {hex:?}"))?;
                output.push(
                    char::from_u32(code_point)
                        .ok_or_else(|| format!("invalid Unicode code point {code_point:X}"))?,
                );
            }
            _ => return Err(format!("unknown TSV escape \\{escaped}")),
        }
    }
    Ok(output)
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
                use std::fmt::Write as _;
                write!(&mut escaped, "\\u{{{:04X}}}", character as u32)
                    .expect("write to String cannot fail");
            }
            _ => escaped.push(character),
        }
    }
    escaped
}

fn parse_inventory(text: &str, context: &str) -> Result<Vec<InventoryInput>, String> {
    let mut lines = text.lines();
    if lines.next() != Some(INVENTORY_HEADER) {
        return Err(format!("{context}: wrong or missing inventory header"));
    }
    lines
        .enumerate()
        .map(|(index, line)| {
            let row_context = format!("{context}:{}", index + 2);
            let fields: Vec<_> = line.split('\t').collect();
            if fields.len() != 6 {
                return Err(format!(
                    "{row_context}: expected 6 TSV fields, got {}",
                    fields.len()
                ));
            }
            let start_line = fields[1]
                .parse::<usize>()
                .map_err(|_| format!("{row_context}: invalid source start line {:?}", fields[1]))?;
            let end_line = fields[2]
                .parse::<usize>()
                .map_err(|_| format!("{row_context}: invalid source end line {:?}", fields[2]))?;
            if fields[0].is_empty() || start_line == 0 || end_line < start_line {
                return Err(format!("{row_context}: invalid source location"));
            }
            if !matches!(
                fields[4],
                "lexical" | "runner_raw_fallback" | "directive_query"
            ) {
                return Err(format!(
                    "{row_context}: unknown inventory boundary {:?}",
                    fields[4]
                ));
            }
            let delimiter =
                unescape_tsv(fields[3]).map_err(|error| format!("{row_context}: {error}"))?;
            if delimiter.is_empty() {
                return Err(format!("{row_context}: empty delimiter"));
            }
            Ok(InventoryInput {
                path: fields[0].to_owned(),
                start_line,
                end_line,
                delimiter,
                boundary: fields[4].to_owned(),
                sql: unescape_tsv(fields[5]).map_err(|error| format!("{row_context}: {error}"))?,
            })
        })
        .collect()
}

fn read_inventory(root: &Path) -> Result<Vec<InventoryInput>, String> {
    let path = root.join(INVENTORY_RELATIVE_PATH);
    let text =
        fs::read_to_string(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    parse_inventory(&text, &path.display().to_string())
}

fn assert_current_inventory(
    records: &[GoldenRecord],
    inventory: &[InventoryInput],
) -> Result<(), String> {
    if records.len() != inventory.len() {
        return Err(format!(
            "parser queue cannot use stale golden: inventory has {} inputs but golden has {}; regenerate with `cd rust && cargo run -p difftest --bin integration_parser_golden -- --write`",
            inventory.len(),
            records.len()
        ));
    }
    for (index, (record, input)) in records.iter().zip(inventory).enumerate() {
        if InventoryInput::from(&record.input) != *input {
            return Err(format!(
                "parser queue cannot use stale golden at input {index} ({}:{}-{}); regenerate with `cd rust && cargo run -p difftest --bin integration_parser_golden -- --write`",
                input.path, input.start_line, input.end_line
            ));
        }
    }
    Ok(())
}

fn classify_from_restore(record: &GoldenRecord, rust_restore: Option<&[u8]>) -> RustOutcome {
    let Some(rust_restore) = rust_restore else {
        return RustOutcome::ParseFailure;
    };
    match record.outcome {
        GoOutcome::Accepted if record.statement_count == 1 => {
            if rust_restore == record.restores[0].as_slice() {
                RustOutcome::Matched
            } else {
                RustOutcome::RestoreMismatch
            }
        }
        GoOutcome::Accepted => RustOutcome::RestoreMismatch,
        GoOutcome::Rejected => RustOutcome::AcceptedGoRejected,
        GoOutcome::RestoreFailure => RustOutcome::AcceptedGoRestoreFailure,
    }
}

fn classify(record: &GoldenRecord) -> RustOutcome {
    if record.outcome == GoOutcome::Accepted && record.statement_count != 1 {
        return match tidb_parser::parse_multi(&record.input.sql) {
            Ok(statements) => {
                let restores: Vec<_> = statements
                    .iter()
                    .map(|statement| statement.restore_bytes())
                    .collect();
                if restores
                    .iter()
                    .map(Vec::as_slice)
                    .eq(record.restores.iter().map(Vec::as_slice))
                {
                    RustOutcome::MultiStatementMatched
                } else {
                    RustOutcome::RestoreMismatch
                }
            }
            Err(_) => RustOutcome::ParseFailure,
        };
    }
    match tidb_parser::parse(&record.input.sql) {
        Ok(statement) => {
            let restore = statement.restore_bytes();
            classify_from_restore(record, Some(&restore))
        }
        Err(_) if record.outcome == GoOutcome::Rejected => RustOutcome::RejectedAsExpected,
        Err(_) => classify_from_restore(record, None),
    }
}

fn skip_leading_comments(mut sql: &str) -> &str {
    loop {
        sql = sql.trim_start();
        if let Some(rest) = sql.strip_prefix("/*") {
            let Some(end) = rest.find("*/") else {
                return sql;
            };
            sql = &rest[end + 2..];
        } else if sql.starts_with('#') {
            sql = sql.find('\n').map_or("", |index| &sql[index + 1..]);
        } else if sql.starts_with("-- ") || sql.starts_with("--\t") {
            sql = sql.find('\n').map_or("", |index| &sql[index + 1..]);
        } else {
            return sql;
        }
    }
}

fn leading_words(sql: &str) -> Vec<String> {
    let sql = skip_leading_comments(sql);
    let mut words = Vec::with_capacity(2);
    let mut current = String::new();
    for character in sql.chars() {
        if character.is_ascii_alphanumeric() || character == '_' || character == '$' {
            current.push(character.to_ascii_uppercase());
        } else if !current.is_empty() {
            words.push(std::mem::take(&mut current));
            if words.len() == 2 {
                break;
            }
        }
    }
    if words.len() < 2 && !current.is_empty() {
        words.push(current);
    }
    words
}

fn leading_sql_shape(sql: &str) -> String {
    let sql = skip_leading_comments(sql);
    let words = leading_words(sql);
    let Some(first) = words.first() else {
        return "<EMPTY>".to_owned();
    };
    let wants_second = matches!(
        first.as_str(),
        "ADMIN"
            | "ALTER"
            | "ANALYZE"
            | "CREATE"
            | "DROP"
            | "FLASHBACK"
            | "GRANT"
            | "LOCK"
            | "RECOVER"
            | "RENAME"
            | "REVOKE"
            | "SHOW"
            | "START"
            | "TRUNCATE"
            | "UNLOCK"
    );
    if wants_second {
        if let Some(second) = words.get(1) {
            return format!("{first} {second}");
        }
    }
    if first
        .chars()
        .next()
        .is_some_and(|character| character.is_ascii_digit())
    {
        return "<LEADING_NUMBER>".to_owned();
    }
    if sql
        .chars()
        .next()
        .is_some_and(|character| !character.is_ascii_alphabetic() && character != '_')
    {
        return format!("<LEADING_{}>", sql.chars().next().expect("checked above"));
    }
    first.clone()
}

fn build_groups(
    records: &[GoldenRecord],
    outcomes: &[RustOutcome],
) -> BTreeMap<(RustOutcome, String), QueueGroup> {
    assert_eq!(
        records.len(),
        outcomes.len(),
        "every input needs one outcome"
    );
    let mut groups: BTreeMap<_, QueueGroup> = BTreeMap::new();
    for (record, outcome) in records.iter().zip(outcomes) {
        if outcome.is_match() {
            continue;
        }
        let shape = leading_sql_shape(&record.input.sql);
        let group = groups.entry((*outcome, shape)).or_default();
        group.count += 1;
        if group.examples.len() < MAX_EXAMPLES_PER_QUEUE {
            group.examples.push(Example {
                path: record.input.path.clone(),
                start_line: record.input.start_line,
                end_line: record.input.end_line,
                boundary: record.input.boundary.clone(),
                sql: record.input.sql.clone(),
            });
        }
    }
    groups
}

fn render(records: &[GoldenRecord]) -> String {
    let outcomes: Vec<_> = records.iter().map(classify).collect();
    let mut outcome_counts: BTreeMap<_, usize> = BTreeMap::new();
    for outcome in &outcomes {
        *outcome_counts.entry(*outcome).or_default() += 1;
    }
    let groups = build_groups(records, &outcomes);
    let mut ordered_groups: Vec<_> = groups.into_iter().collect();
    ordered_groups.sort_by(
        |((left_outcome, left_shape), left_group), ((right_outcome, right_shape), right_group)| {
            right_group
                .count
                .cmp(&left_group.count)
                .then_with(|| left_outcome.cmp(right_outcome))
                .then_with(|| left_shape.cmp(right_shape))
        },
    );

    let mut output = String::new();
    output.push_str("# integration parser porting queue (static Go oracle; no Go subprocess)\n");
    output.push_str(&format!(
        "# inputs={} non_matches={}\n",
        records.len(),
        records.len() - outcomes.iter().filter(|outcome| outcome.is_match()).count()
    ));
    output.push_str("summary\toutcome\tcount\n");
    for outcome in RustOutcome::ALL {
        output.push_str(&format!(
            "summary\t{}\t{}\n",
            outcome.as_str(),
            outcome_counts.get(&outcome).copied().unwrap_or_default()
        ));
    }
    output.push_str("queue\toutcome\tleading_sql_shape\tcount\texample_rank\tsource_path\tsource_start_line\tsource_end_line\tboundary\tsql\n");
    for ((outcome, shape), group) in ordered_groups {
        for (index, example) in group.examples.iter().enumerate() {
            output.push_str(&format!(
                "queue\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                outcome.as_str(),
                escape_tsv(&shape),
                group.count,
                index + 1,
                escape_tsv(&example.path),
                example.start_line,
                example.end_line,
                escape_tsv(&example.boundary),
                escape_tsv(&example.sql),
            ));
        }
    }
    output
}

fn check() -> Result<String, String> {
    let root = repo_root();
    let records = read_golden(&root)?;
    let inventory = read_inventory(&root)?;
    assert_current_inventory(&records, &inventory)?;
    Ok(render(&records))
}

fn main() {
    let arguments: Vec<_> = env::args().skip(1).collect();
    let result = match arguments.as_slice() {
        [command] if command == "--check" => check(),
        _ => Err("usage: integration_parser_queue --check".to_owned()),
    };
    match result {
        Ok(report) => print!("{report}"),
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        assert_current_inventory, classify_from_restore, escape_tsv, leading_sql_shape,
        parse_inventory, unescape_tsv, InventoryInput, RustOutcome,
    };
    use crate::integration_parser_golden::{GoOutcome, GoldenRecord, Input};

    fn record(outcome: GoOutcome, statement_count: usize, restores: Vec<Vec<u8>>) -> GoldenRecord {
        GoldenRecord {
            input: Input {
                path: "tests/integrationtest/t/example.test".to_owned(),
                start_line: 7,
                end_line: 8,
                delimiter: ";".to_owned(),
                boundary: "lexical".to_owned(),
                sql: "SELECT 1".to_owned(),
            },
            outcome,
            statement_count,
            restores,
        }
    }

    #[test]
    fn classification_is_mutually_exclusive_at_every_boundary() {
        let accepted = record(GoOutcome::Accepted, 1, vec![b"SELECT 1".to_vec()]);
        assert_eq!(
            classify_from_restore(&accepted, Some(b"SELECT 1")),
            RustOutcome::Matched
        );
        assert_eq!(
            classify_from_restore(&accepted, Some(b"SELECT 2")),
            RustOutcome::RestoreMismatch
        );
        assert_eq!(
            classify_from_restore(&accepted, None),
            RustOutcome::ParseFailure
        );
        assert_eq!(
            classify_from_restore(&record(GoOutcome::Accepted, 2, vec![]), Some(b"SELECT 1"),),
            RustOutcome::RestoreMismatch
        );
        assert_eq!(
            classify_from_restore(&record(GoOutcome::Rejected, 0, vec![]), Some(b"SELECT 1"),),
            RustOutcome::AcceptedGoRejected
        );
        assert_eq!(
            classify_from_restore(
                &record(GoOutcome::RestoreFailure, 0, vec![]),
                Some(b"SELECT 1")
            ),
            RustOutcome::AcceptedGoRestoreFailure
        );
    }

    #[test]
    fn tsv_controls_are_lossless_and_inventory_rows_keep_one_line() {
        let sql = "SELECT 'line\nnext\t\u{1}\\slash'";
        let escaped = escape_tsv(sql);
        assert_eq!(unescape_tsv(&escaped).unwrap(), sql);
        let inventory = format!(
            "source_path\tsource_start_line\tsource_end_line\tdelimiter\tboundary\tsql\nfile.test\t4\t5\t;\trunner_raw_fallback\t{escaped}\n"
        );
        let rows = parse_inventory(&inventory, "synthetic").unwrap();
        assert_eq!(rows[0].sql, sql);
        assert_eq!(rows[0].boundary, "runner_raw_fallback");
    }

    #[test]
    fn leading_shapes_normalize_sql_family_without_identifier_noise() {
        assert_eq!(
            leading_sql_shape("create table `a` (id int)"),
            "CREATE TABLE"
        );
        assert_eq!(leading_sql_shape("/*+ hint */ select * from t"), "SELECT");
        assert_eq!(
            leading_sql_shape("start transaction read only"),
            "START TRANSACTION"
        );
        assert_eq!(leading_sql_shape("(select 1)"), "<LEADING_(>");
    }

    #[test]
    fn inventory_comparison_rejects_a_stale_golden_location() {
        let golden = record(GoOutcome::Accepted, 1, vec![b"SELECT 1".to_vec()]);
        let stale = InventoryInput {
            path: golden.input.path.clone(),
            start_line: golden.input.start_line + 1,
            end_line: golden.input.end_line,
            delimiter: golden.input.delimiter.clone(),
            boundary: golden.input.boundary.clone(),
            sql: golden.input.sql.clone(),
        };
        assert!(assert_current_inventory(&[golden], &[stale]).is_err());
    }
}
