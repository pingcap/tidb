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

use difftest::parser_oracle::{shared_golden, GoOutcome};

/// The Go parser has a dedicated `DROP PLACEMENT POLICY [IF EXISTS] name`
/// production. Select that command root only: definitions and table/database
/// placement-policy attachments have different roots and cannot enter here.
fn is_clean_drop_placement_policy(sql: &str) -> bool {
    let mut words = sql.split_ascii_whitespace();
    matches!(
        (words.next(), words.next(), words.next()),
        (Some(drop), Some(placement), Some(policy))
            if drop.eq_ignore_ascii_case("DROP")
                && placement.eq_ignore_ascii_case("PLACEMENT")
                && policy.eq_ignore_ascii_case("POLICY")
    )
}

#[test]
fn drop_placement_policy_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_clean_drop_placement_policy(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 20, "source-backed selector drifted");

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
