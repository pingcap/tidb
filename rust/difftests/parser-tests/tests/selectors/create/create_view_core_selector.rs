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

fn strip_keyword<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let input = input.trim_start();
    let prefix = input.get(..keyword.len())?;
    if !prefix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    let rest = &input[keyword.len()..];
    (!rest
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_'))
    .then_some(rest)
}

fn take_word(input: &str) -> Option<(&str, &str)> {
    let input = input.trim_start();
    let end = input
        .find(|character: char| character.is_ascii_whitespace())
        .unwrap_or(input.len());
    (!input[..end].is_empty()).then_some((&input[..end], &input[end..]))
}

fn find_top_level_as(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut depth = 0usize;
    let mut quote = None;
    let mut index = 0usize;
    while index < bytes.len() {
        match quote {
            Some(delimiter) if bytes[index] == delimiter => {
                if index + 1 < bytes.len() && bytes[index + 1] == delimiter {
                    index += 2;
                    continue;
                }
                quote = None;
            }
            Some(_) => {}
            None => match bytes[index] {
                b'\'' | b'"' | b'`' => quote = Some(bytes[index]),
                b'(' => depth += 1,
                b')' => depth = depth.saturating_sub(1),
                _ if depth == 0
                    && index + 2 <= bytes.len()
                    && input[index..index + 2].eq_ignore_ascii_case("AS")
                    && (index == 0 || bytes[index - 1].is_ascii_whitespace())
                    && (index + 2 == bytes.len()
                        || bytes[index + 2].is_ascii_whitespace()
                        || bytes[index + 2] == b'(') =>
                {
                    return Some(index);
                }
                _ => {}
            },
        }
        index += 1;
    }
    None
}

/// The source-backed view grammar this wave owns: Go's default
/// definer/security contract plus `OR REPLACE`, optional `ALGORITHM`, named
/// columns, and a `SELECT`/`WITH` query. Explicit `DEFINER`/`SQL SECURITY`
/// forms are excluded because the Rust AST deliberately has no lossy identity
/// payload. `TABLE`/`VALUES` view queries are separate Go grammar forms.
fn is_core_create_view(sql: &str) -> bool {
    let Some(mut rest) = strip_keyword(sql, "CREATE") else {
        return false;
    };
    if let Some(after_or) = strip_keyword(rest, "OR") {
        let Some(after_replace) = strip_keyword(after_or, "REPLACE") else {
            return false;
        };
        rest = after_replace;
    }
    if let Some(after_algorithm) = strip_keyword(rest, "ALGORITHM") {
        let after_equals = after_algorithm.trim_start().strip_prefix('=');
        let Some(after_equals) = after_equals else {
            return false;
        };
        let Some((algorithm, after_value)) = take_word(after_equals) else {
            return false;
        };
        if !matches!(
            algorithm.to_ascii_uppercase().as_str(),
            "UNDEFINED" | "MERGE" | "TEMPTABLE"
        ) {
            return false;
        }
        rest = after_value;
    }
    let Some(after_view) = strip_keyword(rest, "VIEW") else {
        return false;
    };
    let Some(as_index) = find_top_level_as(after_view) else {
        return false;
    };
    let mut query = after_view[as_index + 2..].trim_start();
    if let Some(after_open) = query.strip_prefix('(') {
        query = after_open.trim_start();
    }
    strip_keyword(query, "SELECT").is_some() || strip_keyword(query, "WITH").is_some()
}

#[test]
fn create_view_core_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_core_create_view(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 104, "source-backed selector drifted");

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
