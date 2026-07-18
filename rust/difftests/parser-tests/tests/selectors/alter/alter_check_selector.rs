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

use difftest::parser_oracle::{shared_golden, GoOutcome};

#[test]
fn alter_check_static_go_rows_match() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            let sql = record.input.sql.trim_start().to_ascii_uppercase();
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && sql.starts_with("ALTER TABLE ")
                && (sql.contains(" ALTER CHECK ") || sql.contains(" ALTER CONSTRAINT "))
        })
        .collect();
    assert_eq!(rows.len(), 16, "source-backed selector drifted");

    let failures: Vec<_> = rows
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
