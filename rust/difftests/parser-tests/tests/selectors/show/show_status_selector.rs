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

/// The standalone and scoped Go `SHOW STATUS` forms, distinct from SHOW
/// VARIABLES and `SHOW TABLE STATUS`.
fn is_show_status(sql: &str) -> bool {
    let mut words = sql.trim_start().split_ascii_whitespace();
    if !matches!(words.next(), Some(show) if show.eq_ignore_ascii_case("show")) {
        return false;
    }
    match words.next() {
        Some(status) if status.trim_end_matches(';').eq_ignore_ascii_case("status") => true,
        Some(scope)
            if scope.eq_ignore_ascii_case("global") || scope.eq_ignore_ascii_case("session") =>
        {
            matches!(
                words.next(),
                Some(status) if status.trim_end_matches(';').eq_ignore_ascii_case("status")
            )
        }
        _ => false,
    }
}

#[test]
fn show_status_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_show_status(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 3, "source-backed selector drifted");

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
