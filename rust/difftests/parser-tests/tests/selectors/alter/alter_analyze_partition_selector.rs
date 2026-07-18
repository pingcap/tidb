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

//! Exact checked-oracle coverage for the partition ANALYZE diversion.

use difftest::parser_oracle::{shared_golden, GoOutcome};

const ROW: (&str, usize) = ("tests/integrationtest/t/table/partition.test", 390);

#[test]
fn alter_analyze_partition_static_go_row_matches() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && (record.input.path.as_str(), record.input.start_line) == ROW
        })
        .collect();
    assert_eq!(selected.len(), 1, "source-backed selector drifted");

    let record = selected[0];
    match tidb_parser::parse(&record.input.sql) {
        Ok(statement) if statement.restore().as_bytes() == record.restores[0].as_slice() => {}
        Ok(statement) => panic!(
            "{}:{}\n  go: {}\n  rust: {}",
            record.input.path,
            record.input.start_line,
            String::from_utf8_lossy(&record.restores[0]),
            statement.restore()
        ),
        Err(error) => panic!(
            "{}:{}\n  parse error: {error:?}",
            record.input.path, record.input.start_line
        ),
    }
}
