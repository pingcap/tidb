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

//! Checked Go-oracle row for a non-reserved keyword used as a `USE` database.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const SOURCE_PATH: &str = "tests/integrationtest/t/executor/explainfor.test";
const SOURCE_LINE: usize = 240;

#[test]
fn use_reserved_name_static_go_row_matches() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && record.input.path == SOURCE_PATH
                && record.input.start_line == SOURCE_LINE
        })
        .collect();
    assert_eq!(rows.len(), 1, "source-backed selector drifted");

    let record = rows[0];
    match tidb_parser::parse(&record.input.sql) {
        Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
        Ok(statement) => panic!(
            "{}:{} restore mismatch\n  go: {}\n rust: {}",
            SOURCE_PATH,
            SOURCE_LINE,
            String::from_utf8_lossy(&record.restores[0]),
            statement.restore()
        ),
        Err(error) => panic!("{}:{} parse failure: {error:?}", SOURCE_PATH, SOURCE_LINE),
    }
}
