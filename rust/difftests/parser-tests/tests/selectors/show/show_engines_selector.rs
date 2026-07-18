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

//! Checked Go-oracle row for the source-owned `SHOW ENGINES` leaf.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const SHOW_ENGINES_ROW: (&str, usize) = ("tests/integrationtest/t/executor/executor.test", 1660);

#[test]
fn show_engines_static_go_row_matches() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let rows: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && (record.input.path.as_str(), record.input.start_line) == SHOW_ENGINES_ROW
        })
        .collect();
    assert_eq!(rows.len(), 1, "source-backed selector drifted");

    let record = rows[0];
    let statement = tidb_parser::parse(&record.input.sql).expect("parse SHOW ENGINES");
    assert_eq!(
        statement.restore().as_bytes(),
        record.restores[0].as_slice(),
        "{}:{}",
        record.input.path,
        record.input.start_line
    );
}
