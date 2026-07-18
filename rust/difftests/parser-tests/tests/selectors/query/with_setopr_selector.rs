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
use tidb_lexer::{Lexer, Token};

fn is_word(token: &Token, word: &str) -> bool {
    token.text.eq_ignore_ascii_case(word)
}

fn matching_paren(tokens: &[Token], start: usize) -> Option<usize> {
    if tokens.get(start)?.text != "(" {
        return None;
    }
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(start) {
        match token.text.as_str() {
            "(" => depth += 1,
            ")" => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

/// Returns where the actual outer query begins after a valid CTE list.
/// Keeping this lexical selector deliberately small mirrors TiDB's
/// `parseWithStmt`: it only classifies a row after finding each CTE's
/// `AS (subquery)` boundary, and never treats a UNION inside that body as
/// a set operation of the outer query.
fn outer_query_start(tokens: &[Token]) -> Option<usize> {
    if !is_word(tokens.first()?, "WITH") {
        return None;
    }
    let mut index = 1;
    if tokens
        .get(index)
        .is_some_and(|token| is_word(token, "RECURSIVE"))
    {
        index += 1;
    }
    loop {
        index += 1; // CTE name
        if tokens.get(index).is_some_and(|token| token.text == "(") {
            index = matching_paren(tokens, index)? + 1;
        }
        if !tokens.get(index).is_some_and(|token| is_word(token, "AS")) {
            return None;
        }
        index += 1;
        index = matching_paren(tokens, index)? + 1;
        if tokens.get(index).is_some_and(|token| token.text == ",") {
            index += 1;
            continue;
        }
        return Some(index);
    }
}

fn has_outer_set_operation(tokens: &[Token], query_start: usize) -> bool {
    let mut depth = 0usize;
    tokens[query_start..]
        .iter()
        .any(|token| match token.text.as_str() {
            "(" => {
                depth += 1;
                false
            }
            ")" => {
                depth = depth.saturating_sub(1);
                false
            }
            _ => {
                depth == 0
                    && (is_word(token, "UNION")
                        || is_word(token, "EXCEPT")
                        || is_word(token, "INTERSECT"))
            }
        })
}

fn has_with_setopr(tokens: &[Token]) -> bool {
    tokens.iter().enumerate().any(|(index, token)| {
        is_word(token, "WITH")
            && outer_query_start(&tokens[index..])
                .is_some_and(|query_start| has_outer_set_operation(&tokens[index..], query_start))
    })
}

fn selected_with_setopr(sql: &str) -> bool {
    let tokens = Lexer::new(sql).tokenize();
    // `PLAN REPLAYER DUMP EXPLAIN ...` is a separate statement envelope.
    // It can contain this query shape, but belongs to its own parser wave;
    // this selector intentionally measures only the Query/EXPLAIN seam.
    if tokens.first().is_some_and(|token| is_word(token, "PLAN"))
        && tokens
            .get(1)
            .is_some_and(|token| is_word(token, "REPLAYER"))
    {
        return false;
    }
    // `NO_ORDER_INDEX` is a distinct hint-parser production in
    // `pkg/parser/hintparser.go`, unrelated to the CTE/set-operation
    // ownership seam. Keep this selector to rows whose only missing
    // grammar is the source-backed WITH attachment itself.
    if tokens
        .iter()
        .any(|token| token.text.to_ascii_uppercase().contains("NO_ORDER_INDEX"))
    {
        return false;
    }
    has_with_setopr(&tokens)
}

fn is_derived_with_start(tokens: &[Token], with_index: usize) -> bool {
    if with_index == 0 || tokens[with_index - 1].text != "(" {
        return false;
    }
    let mut index = with_index - 1;
    while index > 0 && tokens[index - 1].text == "(" {
        index -= 1;
    }
    index > 0
        && (is_word(&tokens[index - 1], "FROM")
            || is_word(&tokens[index - 1], "JOIN")
            || is_word(&tokens[index - 1], "LATERAL"))
}

fn selected_derived_with(sql: &str) -> bool {
    let tokens = Lexer::new(sql).tokenize();
    tokens
        .iter()
        .enumerate()
        .any(|(index, token)| is_word(token, "WITH") && is_derived_with_start(&tokens, index))
}

fn assert_static_go_rows_match(
    selector_name: &str,
    expected_count: usize,
    selector: fn(&str) -> bool,
) {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && selector(&record.input.sql)
        })
        .collect();
    if selected.len() != expected_count {
        let locations = selected
            .iter()
            .map(|record| {
                format!(
                    "{}:{}-{}",
                    record.input.path, record.input.start_line, record.input.end_line
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        panic!(
            "{selector_name} source-backed selector drifted: expected {expected_count}, \
             got {}\n{locations}",
            selected.len()
        );
    }

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

#[test]
fn with_setopr_static_go_rows_match() {
    assert_static_go_rows_match("WITH/set-operation", 21, selected_with_setopr);
}

#[test]
fn derived_with_static_go_rows_match() {
    assert_static_go_rows_match("derived WITH", 9, selected_derived_with);
}
