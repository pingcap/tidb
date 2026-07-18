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

const ROW: (&str, usize) = ("tests/integrationtest/t/privilege/privileges.test", 926);

#[test]
fn admin_alter_ddl_jobs_static_go_row_matches() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && (record.input.path.as_str(), record.input.start_line) == ROW
        })
        .collect();
    assert_eq!(rows.len(), 1, "source-backed selector drifted");

    let record = rows[0];
    match tidb_parser::parse(&record.input.sql) {
        Ok(statement) => assert_eq!(
            statement.restore().as_bytes(),
            record.restores[0].as_slice(),
            "{}:{}: {}",
            record.input.path,
            record.input.start_line,
            record.input.sql
        ),
        Err(error) => panic!(
            "{}:{}: {}\n  parse error: {error:?}",
            record.input.path, record.input.start_line, record.input.sql
        ),
    }
}
