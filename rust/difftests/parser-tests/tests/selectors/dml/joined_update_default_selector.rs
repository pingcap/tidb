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

/// The two accepted single-statement integration rows that exercise Go's
/// bare-`DEFAULT` RHS in joined UPDATE assignments.  Other UPDATE parse
/// failures in the same leading-shape queue are unrelated expression or
/// multi-statement boundaries and stay outside this source-owned slice.
fn is_joined_update_default(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    matches!(
        (
            record.input.path.as_str(),
            record.input.start_line,
            record.input.end_line,
        ),
        (
            "tests/integrationtest/t/planner/core/integration.test",
            89,
            89
        ) | (
            "tests/integrationtest/t/planner/core/integration.test",
            942,
            942
        )
    )
}

#[test]
fn joined_update_default_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_joined_update_default(record)
        })
        .collect();
    assert_eq!(selected.len(), 2, "source-backed selector drifted");

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
