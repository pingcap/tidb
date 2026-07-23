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

// aggregate-test: standalone

#![allow(missing_docs)]

use tidb_ast::{Expr, SelectField};
use tidb_datatype::{Collation, FieldTypeCode};
use tidb_exec::{
    resolve_result_fields, resolve_select_fields, ResultFieldResolveError, ResultFieldSpec,
    ResultFieldTypeMetadata,
};

#[test]
fn tableless_literals_use_source_expression_names_and_types() {
    let specs = [
        ResultFieldSpec::new(Expr::Int("1".to_owned())),
        ResultFieldSpec::new(Expr::Decimal("1.20".to_owned())).with_alias("amount"),
        ResultFieldSpec::new(Expr::String("raw".to_owned())),
    ];
    let fields = resolve_result_fields(&specs, Collation::Utf8Mb4Bin).expect("literal metadata");

    assert_eq!(fields[0].names.column.original, "1");
    assert_eq!(fields[0].field_type.code, FieldTypeCode::LongLong);
    assert_eq!(fields[0].field_type.flen, Some(1));
    assert_eq!(fields[1].names.column.original, "amount");
    assert!(fields[1].names.original_column.original.is_empty());
    assert_eq!(fields[1].field_type.code, FieldTypeCode::NewDecimal);
    assert_eq!(fields[1].field_type.decimal, Some(2));
    assert_eq!(fields[2].names.column.original, "'raw'");
    assert_eq!(fields[2].field_type.collation, Collation::Utf8Mb4Bin);
}

#[test]
fn qualified_column_keeps_source_qualifiers_when_type_is_supplied() {
    let hint = ResultFieldTypeMetadata {
        code: FieldTypeCode::LongLong,
        flags: 0,
        flen: Some(11),
        decimal: Some(0),
        collation: Collation::Binary,
    };
    let fields = resolve_result_fields(
        &[
            ResultFieldSpec::new(Expr::Column(vec!["db".into(), "t".into(), "a".into()]))
                .with_type_hint(hint.clone()),
        ],
        Collation::DEFAULT,
    )
    .expect("qualified column metadata");
    let field = &fields[0];
    assert_eq!(field.names.database.original, "db");
    assert_eq!(field.names.table.original, "t");
    assert_eq!(field.names.original_table.original, "t");
    assert_eq!(field.names.original_column.original, "a");
    assert_eq!(field.names.column.original, "a");
    assert_eq!(field.field_type, hint);
}

#[test]
fn unresolved_row_dependent_type_and_wildcard_are_explicit() {
    let err = resolve_result_fields(
        &[ResultFieldSpec::new(Expr::Column(vec!["a".into()]))],
        Collation::DEFAULT,
    )
    .expect_err("column needs schema type");
    assert!(matches!(err, ResultFieldResolveError::MissingType { .. }));

    let err = resolve_select_fields(&[SelectField::Wildcard(Vec::new())], Collation::DEFAULT)
        .expect_err("wildcard needs schema");
    assert_eq!(err, ResultFieldResolveError::WildcardRequiresSchema);
}

#[test]
fn explicit_alias_is_distinct_from_original_column_name() {
    let fields = resolve_result_fields(
        &[
            ResultFieldSpec::new(Expr::Column(vec!["t".into(), "a".into()]))
                .with_alias("display")
                .with_type_hint(ResultFieldTypeMetadata {
                    code: FieldTypeCode::LongLong,
                    flags: 0,
                    flen: Some(11),
                    decimal: Some(0),
                    collation: Collation::Binary,
                }),
        ],
        Collation::DEFAULT,
    )
    .map(|mut fields| fields.remove(0))
    .expect("aliased column");
    assert_eq!(fields.names.column.original, "display");
    assert_eq!(fields.names.original_column.original, "a");
    assert_eq!(fields.names.table.original, "t");
}
