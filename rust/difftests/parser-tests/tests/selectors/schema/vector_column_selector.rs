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

use std::collections::BTreeSet;

use difftest::parser_oracle::{shared_golden, GoOutcome};

// The complete checked-oracle population whose only missing grammar is a
// VECTOR field type. Vector-index rows are deliberately excluded: they need
// typed expression/index metadata and ANN storage semantics, not this direct
// FieldType translation.
const VECTOR_COLUMN_FIXTURES: [(&str, usize); 11] = [
    ("tests/clusterintegrationtest/t/vector.test", 2),
    ("tests/clusterintegrationtest/t/vector.test", 49),
    ("tests/clusterintegrationtest/t/vector.test", 259),
    ("tests/clusterintegrationtest/t/vector.test", 263),
    ("tests/clusterintegrationtest/t/vector.test", 265),
    ("tests/clusterintegrationtest/t/vector.test", 278),
    ("tests/clusterintegrationtest/t/vector.test", 281),
    ("tests/clusterintegrationtest/t/vector_index_ddl.test", 38),
    ("tests/clusterintegrationtest/t/vector_long.test", 2),
    ("tests/clusterintegrationtest/t/vector_long.test", 40),
    (
        "tests/integrationtest/t/planner/core/topn_heavy_function_optimize.test",
        8,
    ),
];

#[test]
fn vector_fieldtype_static_go_rows_match() {
    let expected: BTreeSet<_> = VECTOR_COLUMN_FIXTURES.into_iter().collect();
    let records = shared_golden().expect("read checked Go parser oracle");
    let selected: Vec<_> = records
        .iter()
        .filter(|record| expected.contains(&(record.input.path.as_str(), record.input.start_line)))
        .collect();
    assert_eq!(selected.len(), expected.len(), "source fixture drifted");

    for record in selected {
        assert_eq!(record.outcome, GoOutcome::Accepted, "{}", record.input.sql);
        assert_eq!(record.statement_count, 1, "{}", record.input.sql);
        let statement = tidb_parser::parse(&record.input.sql)
            .unwrap_or_else(|error| panic!("{}: {error:?}", record.input.sql));
        assert_eq!(
            statement.restore().as_bytes(),
            record.restores[0].as_slice(),
            "{}",
            record.input.sql
        );
    }
}
