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
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(dead_code, missing_docs)]

use difftest::parser_oracle::{read_golden, repo_root, GoOutcome};

/// Go dispatches RENAME USER to its own account-rename production. That
/// command root cannot include table renames or account auth/resource policy
/// changes, which have distinct statement roots.
fn is_clean_rename_user(sql: &str) -> bool {
    let mut words = sql.split_ascii_whitespace();
    matches!(
        (words.next(), words.next()),
        (Some(rename), Some(user))
            if rename.eq_ignore_ascii_case("RENAME") && user.eq_ignore_ascii_case("USER")
    )
}

#[test]
fn rename_user_static_go_rows_match() {
    let records = read_golden(&repo_root()).expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_clean_rename_user(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 14, "source-backed selector drifted");

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
