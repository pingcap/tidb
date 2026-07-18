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

/// Selects the accepted single-statement ALTER rows whose shared column type
/// is an ENUM/SET list containing a Go binary/hex member literal. Keep the
/// predicate tied to the source grammar rather than pinning only the current
/// fourteen paths: adding another integration row must fail loudly in the
/// count assertion and receive the same source-backed restore proof.
fn is_enum_set_binary_alter(sql: &str) -> bool {
    let lower = sql.trim().to_ascii_lowercase();
    if !lower.starts_with("alter table ")
        || !(lower.contains(" modify ") || lower.contains(" change "))
    {
        return false;
    }
    let Some(type_start) = lower.find("enum(").or_else(|| lower.find("set(")) else {
        return false;
    };
    let Some(type_end) = lower[type_start..].find(')') else {
        return false;
    };
    let members = &lower[type_start..type_start + type_end];
    ["0x", "0b", "x'", "b'"]
        .iter()
        .any(|literal| contains_binary_token(members, literal))
}

fn contains_binary_token(text: &str, marker: &str) -> bool {
    let mut offset = 0;
    while let Some(found) = text[offset..].find(marker) {
        let index = offset + found;
        let preceding = text[..index].chars().next_back();
        if !preceding
            .is_some_and(|character| character == '\'' || character.is_ascii_alphanumeric())
        {
            return true;
        }
        offset = index + marker.len();
    }
    false
}

#[test]
fn alter_enum_set_binary_members_match_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_enum_set_binary_alter(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 14, "source-backed selector drifted");

    let failures: Vec<_> = selected
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
