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

const SOURCE_ROW: (&str, usize) = ("tests/integrationtest/t/executor/executor.test", 704);

#[test]
fn limit_uint64_overflow_source_row_rejects_like_go() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| (record.input.path.as_str(), record.input.start_line) == SOURCE_ROW)
        .collect();
    assert_eq!(selected.len(), 1, "LIMIT overflow source selector drifted");

    let record = selected[0];
    assert_eq!(record.outcome, GoOutcome::Rejected, "{}", record.input.sql);
    assert!(
        tidb_parser::parse(&record.input.sql).is_err(),
        "Rust accepted Go-rejected LIMIT overflow: {}",
        record.input.sql
    );
}
