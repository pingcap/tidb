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

/// Select the `CREATE DATABASE` tail that starts Go's database-option
/// production after its optional `IF NOT EXISTS` and database-name slots.
/// This deliberately excludes optionless databases and `CREATE TABLE`'s
/// independently-owned option grammar.
fn has_create_database_option_tail(sql: &str) -> bool {
    let mut words = sql.trim_start().split_ascii_whitespace();
    if !matches!(
        (words.next(), words.next()),
        (Some(create), Some(database))
            if create.eq_ignore_ascii_case("CREATE")
                && database.eq_ignore_ascii_case("DATABASE")
    ) {
        return false;
    }
    let first_after_database = words.next();
    if matches!(first_after_database, Some(if_kw) if if_kw.eq_ignore_ascii_case("IF")) {
        if !matches!(
            (words.next(), words.next()),
            (Some(not), Some(exists))
                if not.eq_ignore_ascii_case("NOT") && exists.eq_ignore_ascii_case("EXISTS")
        ) {
            return false;
        }
        // The name follows the optional three-word clause. In the ordinary
        // form it was already consumed as `first_after_database`.
        if words.next().is_none() {
            return false;
        }
    } else if first_after_database.is_none() {
        return false;
    }
    matches!(
        words.next(),
        Some(option)
            if [
                "DEFAULT",
                "CHARACTER",
                "CHAR",
                "CHARSET",
                "COLLATE",
                "ENCRYPTION",
                "PLACEMENT",
                "SET",
            ]
            .iter()
            .any(|candidate| option.trim_end_matches(';').eq_ignore_ascii_case(candidate))
    )
}

#[test]
fn create_database_options_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && has_create_database_option_tail(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 10, "source-backed selector drifted");

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
