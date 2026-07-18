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

fn named_specs(sql: &str) -> Vec<&str> {
    let Some(rest) = sql.trim_start().get("alter user".len()..) else {
        return Vec::new();
    };
    let mut specs = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut quote = None;
    let mut chars = rest.char_indices().peekable();
    while let Some((index, character)) = chars.next() {
        if let Some(delimiter) = quote {
            if character == '\\' && delimiter != '`' {
                chars.next();
            } else if character == delimiter {
                if delimiter == '`' && chars.peek().is_some_and(|(_, next)| *next == '`') {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }
        match character {
            '\'' | '"' | '`' => quote = Some(character),
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                specs.push(&rest[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    specs.push(&rest[start..]);
    specs
}

fn has_actionless_dual_password_sibling(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    if !(upper.contains(" RETAIN CURRENT PASSWORD") || upper.contains(" DISCARD OLD PASSWORD")) {
        return false;
    }
    let specs = named_specs(sql);
    specs.len() > 1
        && specs.iter().any(|spec| {
            let upper = spec.to_ascii_uppercase();
            !upper.contains(" IDENTIFIED ")
                && !upper.contains(" RETAIN CURRENT PASSWORD")
                && !upper.contains(" DISCARD OLD PASSWORD")
        })
}

/// The exact Go-accepted option families that the old narrow ALTER USER route
/// rejected. Password-expire, resource-group-only, and fully actioned
/// dual-password rows have older dedicated selectors and are intentionally
/// excluded so this selector measures the controlled 42-row delta.
fn is_new_full_alter_user_slice(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("ALTER USER ")
        && (upper.contains(" REQUIRE ")
            || upper.contains(" WITH MAX_")
            || upper.contains(" PASSWORD HISTORY")
            || upper.contains(" PASSWORD REUSE")
            || upper.contains(" PASSWORD REQUIRE CURRENT")
            || upper.contains(" FAILED_LOGIN_ATTEMPTS")
            || upper.contains(" PASSWORD_LOCK_TIME")
            || upper.contains(" ACCOUNT LOCK")
            || upper.contains(" ACCOUNT UNLOCK")
            || upper.contains(" COMMENT ")
            || upper.contains(" ATTRIBUTE ")
            || upper.contains("CURRENT_USER()")
            || has_actionless_dual_password_sibling(sql))
}

#[test]
fn alter_user_full_42_row_delta_matches_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_new_full_alter_user_slice(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 42, "source-backed selector drifted");

    let mut failures = Vec::new();
    for record in selected {
        match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
            Ok(statement) => failures.push(format!(
                "{}\n  go: {}\n rust: {}",
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => failures.push(format!("{}\n  parse error: {error:?}", record.input.sql)),
        }
    }
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
