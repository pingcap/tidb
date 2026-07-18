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

#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{shared_golden, GoOutcome, GoldenRecord};

fn after_table_name(sql: &str) -> Option<&str> {
    let sql = sql.trim_start();
    let prefix = "alter table";
    if !sql
        .get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
    {
        return None;
    }
    let mut rest = sql.get(prefix.len()..)?.trim_start();
    let mut quoted = false;
    while let Some(character) = rest.chars().next() {
        match character {
            '`' => {
                quoted = !quoted;
                rest = &rest[character.len_utf8()..];
            }
            character if !quoted && character.is_whitespace() => return Some(rest.trim_start()),
            _ => rest = &rest[character.len_utf8()..],
        }
    }
    None
}

fn consume_word_ignore_ascii_case<'a>(input: &'a str, word: &str) -> Option<&'a str> {
    let rest = input.get(word.len()..)?;
    input
        .get(..word.len())
        .filter(|prefix| prefix.eq_ignore_ascii_case(word))?;
    if rest
        .chars()
        .next()
        .is_none_or(|character| character.is_whitespace() || character == '=')
    {
        Some(rest)
    } else {
        None
    }
}

fn consume_partition_name(rest: &str) -> Option<&str> {
    let rest = rest.trim_start();
    if let Some(quoted) = rest.strip_prefix('`') {
        let mut index = 0;
        while index < quoted.len() {
            match quoted.as_bytes()[index] {
                b'`' if quoted.as_bytes().get(index + 1) == Some(&b'`') => index += 2,
                b'`' => return quoted.get(index + 1..),
                _ => index += 1,
            }
        }
        None
    } else {
        let name_end = rest
            .find(|character: char| character.is_whitespace() || matches!(character, ',' | ';'))?;
        rest.get(name_end..)
    }
}

/// Direct `PARTITION name PLACEMENT POLICY` is disjoint from table-level
/// placement and from terminal `PARTITION BY` replacement syntax.
fn is_direct_partition_placement_policy(sql: &str) -> bool {
    let Some(rest) = after_table_name(sql) else {
        return false;
    };
    let Some(rest) = consume_word_ignore_ascii_case(rest, "PARTITION") else {
        return false;
    };
    let Some(rest) = consume_partition_name(rest) else {
        return false;
    };
    consume_word_ignore_ascii_case(rest.trim_start(), "PLACEMENT POLICY").is_some()
}

/// `PartitionDefinition.options` already owns this nested form; make its two
/// checked source rows explicit without selecting CREATE TABLE definitions or
/// root-level table options.
fn is_add_partition_definition_placement_policy(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE ")
        && upper.contains(" ADD PARTITION (")
        && upper.contains(" PLACEMENT POLICY")
}

fn assert_exact_rows(records: Vec<&GoldenRecord>, expected: usize, label: &str) {
    assert_eq!(records.len(), expected, "{label} selector drifted");
    let failures: Vec<_> = records
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}: {}\n  go: {}\n  rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!(
                "{}:{}: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn alter_partition_placement_policy_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    assert_exact_rows(
        records
            .iter()
            .filter(|record| {
                record.outcome == GoOutcome::Accepted
                    && record.statement_count == 1
                    && is_direct_partition_placement_policy(&record.input.sql)
            })
            .collect(),
        6,
        "direct partition placement policy",
    );
    assert_exact_rows(
        records
            .iter()
            .filter(|record| {
                record.outcome == GoOutcome::Accepted
                    && record.statement_count == 1
                    && is_add_partition_definition_placement_policy(&record.input.sql)
            })
            .collect(),
        2,
        "ADD PARTITION definition placement policy",
    );
}
