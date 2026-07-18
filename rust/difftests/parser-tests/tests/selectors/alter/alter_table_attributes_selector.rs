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

/// Finds the first `ALTER TABLE` action while keeping quoted table paths out
/// of the grammar decision. Partition attributes deliberately start with
/// `PARTITION`, so they remain a separate source owner.
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

/// Exact table-level `parseAlterTableOptions` ATTRIBUTES branch. This avoids
/// conflating `PARTITION name ATTRIBUTES` with the separate partition action.
fn is_table_attributes_action(sql: &str) -> bool {
    after_table_name(sql).is_some_and(|action| {
        action
            .get(.."attributes".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("attributes"))
            && action
                .as_bytes()
                .get("attributes".len())
                .is_none_or(|byte| byte.is_ascii_whitespace() || *byte == b'=')
    })
}

#[test]
fn alter_table_attributes_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_table_attributes_action(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 13, "source-backed selector drifted");

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
