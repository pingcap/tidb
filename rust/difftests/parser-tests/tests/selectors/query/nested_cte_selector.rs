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

#[derive(Clone, Copy)]
struct WithShape {
    has_nested_with: bool,
    has_outer_set_operation: bool,
}

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

fn with_shape(tokens: &[Token]) -> Option<WithShape> {
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
    let mut has_nested_with = false;
    loop {
        // CTE name, then its optional column-name list.
        index += 1;
        if tokens.get(index).is_some_and(|token| token.text == "(") {
            index = matching_paren(tokens, index)? + 1;
        }
        if !tokens.get(index).is_some_and(|token| is_word(token, "AS")) {
            return None;
        }
        index += 1;
        let body_end = matching_paren(tokens, index)?;
        has_nested_with |= tokens[index + 1..body_end]
            .iter()
            .any(|token| is_word(token, "WITH"));
        index = body_end + 1;
        if tokens.get(index).is_some_and(|token| token.text == ",") {
            index += 1;
            continue;
        }
        break;
    }

    let mut depth = 0usize;
    let mut has_outer_set_operation = false;
    for token in &tokens[index..] {
        match token.text.as_str() {
            "(" => depth += 1,
            ")" => depth = depth.saturating_sub(1),
            _ if depth == 0
                && (is_word(token, "UNION")
                    || is_word(token, "EXCEPT")
                    || is_word(token, "INTERSECT")) =>
            {
                has_outer_set_operation = true;
            }
            _ => {}
        }
    }
    Some(WithShape {
        has_nested_with,
        has_outer_set_operation,
    })
}

fn selected_nested_cte(sql: &str) -> bool {
    let tokens = Lexer::new(sql).tokenize();
    let with_start = if tokens.first().is_some_and(|token| is_word(token, "WITH")) {
        Some(0)
    } else if tokens
        .first()
        .is_some_and(|token| is_word(token, "EXPLAIN"))
    {
        tokens.iter().position(|token| is_word(token, "WITH"))
    } else {
        None
    };
    let Some(with_start) = with_start else {
        return false;
    };
    let Some(outer) = with_shape(&tokens[with_start..]) else {
        return false;
    };
    if !outer.has_nested_with || outer.has_outer_set_operation {
        return false;
    }

    tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| is_word(token, "WITH"))
        .filter_map(|(index, _)| with_shape(&tokens[index..]))
        .all(|shape| !shape.has_outer_set_operation)
}

#[test]
fn nested_cte_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && selected_nested_cte(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 57, "source-backed selector drifted");

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
