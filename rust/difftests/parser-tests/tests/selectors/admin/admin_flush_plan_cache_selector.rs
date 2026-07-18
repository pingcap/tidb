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

#![allow(missing_docs)]

//! Checked Go-oracle rows for scoped `ADMIN FLUSH PLAN_CACHE` statements.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const PLAN_CACHE_ROWS: [(&str, usize); 4] = [
    (
        "tests/integrationtest/t/planner/core/tests/prepare/prepare.test",
        61,
    ),
    (
        "tests/integrationtest/t/planner/core/tests/prepare/prepare.test",
        81,
    ),
    (
        "tests/integrationtest/t/planner/core/tests/prepare/prepare.test",
        101,
    ),
    (
        "tests/integrationtest/t/planner/core/tests/prepare/prepare.test",
        122,
    ),
];

#[test]
fn admin_flush_plan_cache_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && PLAN_CACHE_ROWS.contains(&(record.input.path.as_str(), record.input.start_line))
        })
        .collect();
    assert_eq!(rows.len(), PLAN_CACHE_ROWS.len());

    let failures: Vec<_> = rows
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}: {}\n  go: {}\n  rust: {}",
                record.input.path,
                record.input.start_line,
                record.input.sql,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!(
                "{}:{}: {}\n  parse error: {error:?}",
                record.input.path, record.input.start_line, record.input.sql
            )),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
