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

/// Returns the unquoted top-level words. `CREATE USER` has no grammar whose
/// policy discrimination depends on nested expressions, but quoted plugin,
/// comment, and JSON payloads must not accidentally select an unported clause.
fn top_level_words(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut words = Vec::new();
    let mut quote = None;
    let mut index = 0usize;
    while index < bytes.len() {
        match quote {
            Some(_) if bytes[index] == b'\\' && index + 1 < bytes.len() => index += 2,
            Some(delimiter) if bytes[index] == delimiter => {
                if index + 1 < bytes.len() && bytes[index + 1] == delimiter {
                    index += 2;
                } else {
                    quote = None;
                    index += 1;
                }
            }
            Some(_) => index += 1,
            None if matches!(bytes[index], b'\'' | b'"' | b'`') => {
                quote = Some(bytes[index]);
                index += 1;
            }
            None if bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                words.push(sql[start..index].to_ascii_uppercase());
            }
            None => index += 1,
        }
    }
    words
}

fn contains_phrase(words: &[String], phrase: &[&str]) -> bool {
    words.windows(phrase.len()).any(|window| {
        window
            .iter()
            .zip(phrase)
            .all(|(word, expected)| word == expected)
    })
}

/// Selects Go's `parseCreateUserStmt` rows covered by the typed Rust shape:
/// account specs, REQUIRE TLS/token clauses, WITH resource limits,
/// password/lock options, one COMMENT/ATTRIBUTE annotation, and the final
/// statement-global RESOURCE GROUP payload.
fn is_create_user_statement_slice(sql: &str) -> bool {
    let words = top_level_words(sql);
    words.first().is_some_and(|word| word == "CREATE")
        && words.get(1).is_some_and(|word| word == "USER")
}

#[test]
fn create_user_statement_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_create_user_statement_slice(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 233, "source-backed selector drifted");

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
