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

//! Direct obligations from `pkg/expression/simple_rewriter_test.go`.

use tidb_datatype::{
    contains_column, FieldName, FieldNameMetadata, IdentifierMetadata, QualifiedColumnName,
    EMPTY_NAME,
};
use tidb_expr::{find_field_name, find_field_name_index_by_column};

fn identifier(value: &str) -> IdentifierMetadata {
    IdentifierMetadata::new(value)
}

fn field(database: &str, table: &str, column: &str, redundant: bool) -> FieldName {
    FieldName {
        names: FieldNameMetadata {
            original_table: identifier(table),
            original_column: identifier(column),
            database: identifier(database),
            table: identifier(table),
            column: identifier(column),
        },
        hidden: false,
        not_explicit_usable: false,
        redundant,
    }
}

fn reference_find(
    names: &[FieldName],
    column: &QualifiedColumnName,
) -> Result<Option<usize>, String> {
    let mut index: Option<usize> = None;
    for (candidate_index, name) in names.iter().enumerate() {
        if !name.not_explicit_usable && name.matches_column(column) {
            if let Some(previous_index) = index {
                if names[previous_index].redundant || name.redundant {
                    if !name.redundant {
                        index = Some(candidate_index);
                    }
                    continue;
                }
                return Err(format!(
                    "[expression:1052]Column '{}' in field list is ambiguous",
                    column.display_name()
                ));
            }
            index = Some(candidate_index);
        }
    }
    Ok(index)
}

#[test]
fn test_find_field_name() {
    struct Case {
        label: &'static str,
        names: Vec<FieldName>,
        column: QualifiedColumnName,
        expected: Result<Option<usize>, &'static str>,
    }

    let cases = [
        Case {
            label: "Simple match",
            names: vec![field("db", "tbl", "col", false)],
            column: QualifiedColumnName::new("db", "tbl", "col"),
            expected: Ok(Some(0)),
        },
        Case {
            label: "Match with empty schema and table",
            names: vec![field("db", "tbl", "col", false)],
            column: QualifiedColumnName::new("", "", "col"),
            expected: Ok(Some(0)),
        },
        Case {
            label: "Match with empty schema, non-empty table",
            names: vec![field("db", "tbl", "col", false)],
            column: QualifiedColumnName::new("", "tbl", "col"),
            expected: Ok(Some(0)),
        },
        Case {
            label: "Match with non-empty schema, empty table",
            names: vec![field("db", "tbl", "col", false)],
            column: QualifiedColumnName::new("db", "", "col"),
            expected: Ok(Some(0)),
        },
        Case {
            label: "No match",
            names: vec![field("db", "tbl", "col1", false)],
            column: QualifiedColumnName::new("db", "tbl", "col2"),
            expected: Ok(None),
        },
        Case {
            label: "Match with redundant field",
            names: vec![
                field("db", "tbl", "col", true),
                field("db", "tbl", "col", true),
                field("db", "tbl", "col", false),
            ],
            column: QualifiedColumnName::new("db", "tbl", "col"),
            expected: Ok(Some(2)),
        },
        Case {
            label: "Non-unique match",
            names: vec![
                field("db", "tbl", "col", false),
                field("db", "tbl", "col", false),
            ],
            column: QualifiedColumnName::new("db", "tbl", "col"),
            expected: Err("[expression:1052]Column 'db.tbl.col' in field list is ambiguous"),
        },
        Case {
            label: "Match with empty schema and table and redundant",
            names: vec![
                field("db", "tbl", "col", true),
                field("db", "tbl", "col", false),
            ],
            column: QualifiedColumnName::new("", "", "col"),
            expected: Ok(Some(1)),
        },
        Case {
            label: "Non-unique match with a redundant",
            names: vec![
                field("db", "tbl", "col", true),
                field("db", "tbl", "col", false),
                field("db", "tbl", "col", false),
            ],
            column: QualifiedColumnName::new("db", "tbl", "col"),
            expected: Err("[expression:1052]Column 'db.tbl.col' in field list is ambiguous"),
        },
        Case {
            label: "Match with multiple redundant",
            names: vec![
                field("db", "tbl", "col", true),
                field("db", "tbl", "col", true),
                field("db", "tbl", "col", true),
            ],
            column: QualifiedColumnName::new("db", "tbl", "col"),
            expected: Ok(Some(0)),
        },
    ];

    for case in cases {
        let reference = reference_find(&case.names, &case.column);
        let optimized =
            find_field_name(&case.names, &case.column).map_err(|error| error.to_string());
        let expected = case.expected.map_err(str::to_owned);
        assert_eq!(reference, expected, "reference: {}", case.label);
        assert_eq!(optimized, expected, "optimized: {}", case.label);
    }
}

#[test]
fn benchmark_find_field_name_source_shapes_match_the_reference() {
    for size in [10, 100, 200, 1_000, 10_000] {
        let names = (0..size)
            .map(|index| {
                let suffix = char::from_u32(u32::from('A') + index as u32)
                    .expect("benchmark source sizes remain valid Unicode scalar values");
                field("db", "tbl", &format!("col{suffix}"), false)
            })
            .collect::<Vec<_>>();
        let column = QualifiedColumnName::new("db", "tbl", "colZ");
        assert_eq!(
            find_field_name(&names, &column).map_err(|error| error.to_string()),
            reference_find(&names, &column),
            "source benchmark size {size}"
        );
    }
}

#[test]
fn field_name_source_helpers_and_unusable_filter_are_shared() {
    let mut unusable = field("db", "tbl", "col", false);
    unusable.not_explicit_usable = true;
    let visible = field("db", "tbl", "col", false);
    let names = vec![unusable, visible.clone()];
    let column = QualifiedColumnName::new("db", "tbl", "col");

    assert!(contains_column(&names, &column));
    assert_eq!(find_field_name(&names, &column).unwrap(), Some(1));
    assert_eq!(find_field_name_index_by_column(&names, "col"), Some(0));
    assert_eq!(visible.display_name(), "db.tbl.col");

    let mut hidden = visible;
    hidden.hidden = true;
    assert_eq!(hidden.display_name(), EMPTY_NAME);

    let ambiguous = vec![
        field("db", "tbl", "col", false),
        field("db", "tbl", "col", false),
    ];
    let error = find_field_name(&ambiguous, &column).unwrap_err();
    assert_eq!(error.sql_error.code, tidb_error::mysql::errcode::ErrNonUniq);
    assert_eq!(error.sql_error.state, "23000");
}
