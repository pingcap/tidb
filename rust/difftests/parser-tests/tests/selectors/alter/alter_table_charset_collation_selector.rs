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

/// Locates the action boundary without pretending that a table path is one
/// bare identifier. The selected source rows use ordinary names, but keeping
/// this scanner quote-aware makes selector drift explicit when fixtures grow.
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

/// Exact Go grammar slice owned here: generic charset/collation options and
/// the distinct `CONVERT TO` option. Column definitions carrying COLLATE are
/// intentionally excluded; those belong to the column-type workstream.
fn is_charset_collation_action(sql: &str) -> bool {
    let Some(action) = after_table_name(sql) else {
        return false;
    };
    let action = action.to_ascii_uppercase();
    [
        "CHARACTER SET ",
        "CHAR SET ",
        "CHARSET ",
        "COLLATE ",
        "DEFAULT CHARACTER SET ",
        "DEFAULT CHAR SET ",
        "DEFAULT CHARSET ",
        "DEFAULT COLLATE ",
        "CONVERT TO CHARACTER SET ",
        "CONVERT TO CHAR SET ",
        "CONVERT TO CHARSET ",
    ]
    .iter()
    .any(|prefix| action.starts_with(prefix))
}

#[test]
fn alter_table_charset_collation_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_charset_collation_action(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 20, "source-backed selector drifted");

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
