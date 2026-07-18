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

// The complete reviewed static-oracle population whose only mismatch is Go's
// ENUM/SET FieldType normalization: suppressing a field charset on restore
// and right-trimming decoded string members. Do not broaden this selector to
// binary/hex/bit members or partitioned tables; those need distinct AST
// payloads/DDL grammar respectively.
const ENUM_SET_RESTORE_FIXTURES: [(&str, usize); 9] = [
    (
        "tests/integrationtest/t/collation_check_use_collation.test",
        27,
    ),
    (
        "tests/integrationtest/t/collation_check_use_collation.test",
        55,
    ),
    ("tests/integrationtest/t/executor/show.test", 231),
    ("tests/integrationtest/t/expression/issues.test", 1384),
    ("tests/integrationtest/t/expression/issues.test", 1386),
    ("tests/integrationtest/t/expression/issues.test", 1426),
    ("tests/integrationtest/t/expression/issues.test", 1430),
    ("tests/integrationtest/t/expression/issues.test", 1514),
    ("tests/integrationtest/t/expression/issues.test", 1516),
];

#[test]
fn enum_set_fieldtype_restore_static_go_rows_match() {
    let expected: BTreeSet<_> = ENUM_SET_RESTORE_FIXTURES.into_iter().collect();
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
