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

use difftest::parser_oracle::{shared_golden, GoOutcome, Input};

fn keyword_at(input: &str, offset: usize, keyword: &str) -> bool {
    let bytes = input.as_bytes();
    let end = offset + keyword.len();
    end <= bytes.len()
        && bytes[offset..end].eq_ignore_ascii_case(keyword.as_bytes())
        && (offset == 0 || !bytes[offset - 1].is_ascii_alphanumeric() && bytes[offset - 1] != b'_')
        && (end == bytes.len() || !bytes[end].is_ascii_alphanumeric() && bytes[end] != b'_')
}

fn find_top_level_keyword(input: &str, keyword: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut quote = None;
    let mut depth = 0usize;
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
                _ if depth == 0 && keyword_at(input, index, keyword) => return Some(index),
                _ => {}
            },
        }
        index += 1;
    }
    None
}

fn starts_core_view_query(input: &str) -> bool {
    let Some(as_offset) = find_top_level_keyword(input, "AS") else {
        return false;
    };
    let mut query = input[as_offset + 2..].trim_start();
    if let Some(after_open) = query.strip_prefix('(') {
        query = after_open.trim_start();
    }
    keyword_at(query, 0, "SELECT") || keyword_at(query, 0, "WITH")
}

fn has_unported_query_shape(input: &str) -> bool {
    let Some(as_offset) = find_top_level_keyword(input, "AS") else {
        return true;
    };
    let query = input[as_offset + 2..].trim_start();
    // Go's view parser separately parses a parenthesized subquery and then
    // attaches a following UNION. Rust's typed view boundary currently only
    // retains parentheses around the whole query, so this remains a query
    // grammar wave rather than an identity/security one.
    query.starts_with('(') && find_top_level_keyword(query, "UNION").is_some()
}

fn is_outside_typed_query_slice(input: &Input) -> bool {
    // This fuzz-derived view query combines a window expression with nested
    // correlated subqueries and set operations. It is an explicit source
    // anchor rather than a broad `OVER` exclusion: simpler window queries
    // that the current typed query AST already restores remain covered.
    input.path == "tests/integrationtest/t/planner/core/casetest/integration.test"
        && input.start_line == 686
        && input.end_line == 686
}

/// Selects exactly the Go-accepted one-statement `CREATE VIEW` rows whose
/// header explicitly writes `DEFINER` and/or `SQL SECURITY`, and whose query
/// starts with the existing typed `SELECT`/`WITH` view grammar. The selector
/// intentionally excludes `TABLE`/`VALUES` view sources, malformed headers,
/// parenthesized set-operation branches, and the separately anchored complex
/// query above; those need their own query AST payloads rather than being
/// folded into this identity/security wave.
fn is_explicit_view_identity_or_security(sql: &str) -> bool {
    let input = sql.trim_start();
    if !keyword_at(input, 0, "CREATE") {
        return false;
    }
    let Some(view_offset) = find_top_level_keyword(input, "VIEW") else {
        return false;
    };
    if find_top_level_keyword(&input[view_offset + 4..], "AS").is_none() {
        return false;
    }
    let header = &input[..view_offset];
    let has_explicit_clause = find_top_level_keyword(header, "DEFINER").is_some()
        || find_top_level_keyword(header, "SQL").is_some_and(|sql_offset| {
            keyword_at(header[sql_offset + 3..].trim_start(), 0, "SECURITY")
        });
    let view_body = &input[view_offset + 4..];
    has_explicit_clause && starts_core_view_query(view_body) && !has_unported_query_shape(view_body)
}

#[test]
fn create_view_explicit_definer_security_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_explicit_view_identity_or_security(&record.input.sql)
                && !is_outside_typed_query_slice(&record.input)
        })
        .collect();
    assert_eq!(selected.len(), 86, "source-backed selector drifted");

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
