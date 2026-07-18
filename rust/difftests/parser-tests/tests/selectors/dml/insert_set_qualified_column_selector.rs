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

/// Go's `parseInsert` stores SET assignment `ColumnName`s in
/// `ast.InsertStmt.Columns`, retaining their table qualifier for Restore.
/// This is the sole static fixture using that typed path rather than an
/// unqualified SET target.
fn is_qualified_insert_set_column(record: &difftest::parser_oracle::GoldenRecord) -> bool {
    matches!(
        (
            record.input.path.as_str(),
            record.input.start_line,
            record.input.end_line,
        ),
        ("tests/integrationtest/t/executor/write.test", 229, 229)
    )
}

#[test]
fn qualified_insert_set_column_static_go_row_matches() {
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| {
            record.outcome == GoOutcome::Accepted
                && record.statement_count == 1
                && is_qualified_insert_set_column(record)
        })
        .collect();
    assert_eq!(selected.len(), 1, "source-backed selector drifted");

    let record = selected[0];
    let statement = tidb_parser::parse(&record.input.sql).expect("parse selected Go row");
    assert_eq!(
        statement.restore().as_bytes(),
        record.restores[0].as_slice()
    );
}
