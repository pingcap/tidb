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

/// Go's integration corpus contains this top-level `VALUES` spelling only as
/// a rejected grammar case: the production requires `ROW` before every row.
fn is_values_statement(sql: &str) -> bool {
    sql.trim_start()
        .split_ascii_whitespace()
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("VALUES"))
}

#[test]
fn values_statement_source_rejections_remain_rejected() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Rejected
                && record.statement_count == 0
                && is_values_statement(&record.input.sql)
        })
        .collect();
    assert_eq!(selected.len(), 3, "source-backed selector drifted");

    for record in selected {
        assert!(
            tidb_parser::parse(&record.input.sql).is_err(),
            "unexpectedly accepted: {}",
            record.input.sql
        );
    }
}
