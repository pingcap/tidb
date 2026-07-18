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

/// Select the standalone Go table-lock grammar only. `LOCK STATS` has a
/// different AST/execution boundary, and `GRANT LOCK TABLES` is a privilege
/// name rather than a lock statement, so neither may inflate this family.
fn is_lock_tables_statement(sql: &str) -> bool {
    let mut words = sql.trim_start().split_ascii_whitespace();
    let Some(command) = words.next() else {
        return false;
    };
    let Some(table_word) = words.next() else {
        return false;
    };
    let table_word = table_word.trim_end_matches(';');
    (command.eq_ignore_ascii_case("LOCK") || command.eq_ignore_ascii_case("UNLOCK"))
        && (table_word.eq_ignore_ascii_case("TABLE") || table_word.eq_ignore_ascii_case("TABLES"))
}

#[test]
fn lock_tables_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_lock_tables_statement(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 24, "source-backed selector drifted");

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
