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

fn is_set_transaction_snapshot(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    (upper.starts_with("SET TRANSACTION READ ONLY AS OF TIMESTAMP")
        || upper.starts_with("SET SESSION TRANSACTION READ ONLY AS OF TIMESTAMP")
        || upper.starts_with("SET GLOBAL TRANSACTION READ ONLY AS OF TIMESTAMP"))
        && upper
            .get(upper.find("TIMESTAMP").unwrap_or(upper.len()) + "TIMESTAMP".len()..)
            .is_some_and(|tail| !tail.trim().is_empty())
}

#[test]
fn set_transaction_snapshot_integration_rows_match_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_set_transaction_snapshot(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 1, "source-backed selector drifted");

    let failures: Vec<_> = selected
        .into_iter()
        .filter_map(|record| match tidb_parser::parse(&record.input.sql) {
            Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {
                None
            }
            Ok(statement) => Some(format!(
                "{}:{}\n  go: {}\n  rust: {}",
                record.input.path,
                record.input.start_line,
                String::from_utf8_lossy(&record.restores[0]),
                statement.restore()
            )),
            Err(error) => Some(format!(
                "{}:{}\n  parse error: {error:?}",
                record.input.path, record.input.start_line
            )),
        })
        .collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
