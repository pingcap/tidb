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

use difftest::parser_oracle::{shared_golden, GoOutcome};

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

fn starts_word(input: &str, word: &str) -> bool {
    input
        .get(..word.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(word))
        && input
            .get(word.len()..)
            .and_then(|rest| rest.chars().next())
            .is_some_and(char::is_whitespace)
}

/// Selects the typed terminal re-partitioning branch. COMMENT and ENABLE KEYS
/// prefixes are separate ALTER action owners, so they remain outside this
/// partition-only differential slice.
fn is_repartition_slice(sql: &str) -> bool {
    let Some(action) = after_table_name(sql) else {
        return false;
    };
    if starts_word(action, "PARTITION") {
        return action
            .get("PARTITION".len()..)
            .is_some_and(|rest| starts_word(rest.trim_start(), "BY"));
    }
    starts_word(action, "ADD")
        && starts_word(action["ADD".len()..].trim_start(), "COLUMN")
        && action.to_ascii_uppercase().contains(" PARTITION BY")
}

#[test]
fn alter_repartition_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_repartition_slice(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 49, "source-backed selector drifted");

    let failures: Vec<_> = selected
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}\n  go: {}\n rust: {}",
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!("{}\n  parse error: {error:?}", record.input.sql)),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
