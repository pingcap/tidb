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

use tidb_ast::{BitLiteralValue, Expr, SelectField};
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
fn binary_literal_metadata_uses_decoded_width_and_source_flags() {
    let fields = resolve_result_fields(
        &[
            ResultFieldSpec::new(Expr::Hex("0001".to_owned())),
            ResultFieldSpec::new(Expr::Bit(BitLiteralValue::from_digits("000000001"))),
        ],
        Collation::DEFAULT,
    )
    .expect("binary literal metadata");

    assert_eq!(fields[0].names.column.original, "x'0001'");
    assert_eq!(fields[0].field_type.flen, Some(6));
    assert_eq!(
        fields[0].field_type.flags,
        tidb_protocol::BINARY_FLAG | tidb_exec::NOT_NULL_FLAG | tidb_exec::UNSIGNED_FLAG
    );

    assert_eq!(fields[1].names.column.original, "b'1'");
    assert_eq!(fields[1].field_type.flen, Some(6));
    assert_eq!(
        fields[1].field_type.flags,
        tidb_protocol::BINARY_FLAG | tidb_exec::NOT_NULL_FLAG
    );
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

/// Every `CAST` target's result metadata mirrors Go's hand-written
/// `parseCastType`/`parseCastTypeInternal` (`pkg/parser/expr_cast_parser.go`)
/// plus its unspecified-flen/decimal wrapper, which applies
/// `mysql.GetDefaultFieldLengthAndDecimalForCast`
/// (`pkg/parser/mysql/util.go` `defaultLengthAndDecimalForCast`): SIGNED and
/// UNSIGNED are `LongLong (22, 0)`; bare `BINARY` keeps `TypeVarString` while
/// `BINARY(n)` flips to `TypeString` (`tp.SetType(mysql.TypeString)` when the
/// length is specified); DATETIME/TIME take their default flen 19/10 plus
/// `1 + fsp` when a positive fsp is given; YEAR leaves flen and decimal
/// unspecified (TypeYear is absent from the default map); DOUBLE defaults to
/// `(22, unspecified)`, FLOAT to `(12, unspecified)` and stays `TypeFloat`;
/// JSON defaults to `(4194304, 0)`. Every binary-charset target carries
/// `mysql.BinaryFlag` (`setBinaryCastType`), which the server's
/// `ConvertColumnInfo` copies into the wire column flags
/// (`pkg/server/internal/column/convert.go:31`).
#[test]
fn cast_target_metadata_follows_go_parse_cast_type() {
    use tidb_ast::{CastExpr, CastStyle, CastType};

    let cast = |cast_type: CastType| {
        Expr::Cast(CastExpr {
            expr: Box::new(Expr::Int("1".to_owned())),
            cast_type,
            style: CastStyle::Cast,
            array: false,
        })
    };
    let binary = tidb_protocol::BINARY_FLAG;
    let cases: Vec<(CastType, FieldTypeCode, u16, Option<u32>, Option<u8>)> = vec![
        (
            CastType::Signed,
            FieldTypeCode::LongLong,
            binary,
            Some(22),
            Some(0),
        ),
        (
            CastType::Unsigned,
            FieldTypeCode::LongLong,
            binary | tidb_exec::UNSIGNED_FLAG,
            Some(22),
            Some(0),
        ),
        (
            CastType::Char {
                len: None,
                charset: None,
            },
            FieldTypeCode::VarString,
            0,
            None,
            None,
        ),
        (
            CastType::Char {
                len: Some(3),
                charset: None,
            },
            FieldTypeCode::VarString,
            0,
            Some(3),
            None,
        ),
        (
            CastType::Binary { len: None },
            FieldTypeCode::VarString,
            binary,
            None,
            None,
        ),
        (
            CastType::Binary { len: Some(5) },
            FieldTypeCode::String,
            binary,
            Some(5),
            None,
        ),
        (
            CastType::Decimal { flen: 10, scale: 0 },
            FieldTypeCode::NewDecimal,
            binary,
            Some(10),
            Some(0),
        ),
        (
            CastType::Decimal { flen: 7, scale: 2 },
            FieldTypeCode::NewDecimal,
            binary,
            Some(7),
            Some(2),
        ),
        (
            CastType::Date,
            FieldTypeCode::Date,
            binary,
            Some(10),
            Some(0),
        ),
        (
            CastType::DateTime { fsp: None },
            FieldTypeCode::Datetime,
            binary,
            Some(19),
            Some(0),
        ),
        (
            CastType::DateTime { fsp: Some(3) },
            FieldTypeCode::Datetime,
            binary,
            Some(23),
            Some(3),
        ),
        (
            CastType::Time { fsp: None },
            FieldTypeCode::Duration,
            binary,
            Some(10),
            Some(0),
        ),
        (
            CastType::Time { fsp: Some(3) },
            FieldTypeCode::Duration,
            binary,
            Some(14),
            Some(3),
        ),
        (CastType::Year, FieldTypeCode::Year, binary, None, None),
        (
            CastType::Double,
            FieldTypeCode::Double,
            binary,
            Some(22),
            None,
        ),
        (
            CastType::Float,
            FieldTypeCode::Float,
            binary,
            Some(12),
            None,
        ),
        (
            CastType::Vector { dimensions: None },
            FieldTypeCode::VectorFloat32,
            binary,
            None,
            Some(0),
        ),
        (
            CastType::Json,
            FieldTypeCode::Json,
            binary,
            Some(4194304),
            Some(0),
        ),
    ];
    for (target, code, flags, flen, decimal) in cases {
        let fields = resolve_select_fields(
            &[SelectField::Expr {
                expr: cast(target.clone()),
                alias: None,
            }],
            Collation::Utf8Mb4Bin,
        )
        .expect("cast target metadata");
        let field_type = &fields[0].field_type;
        assert_eq!(field_type.code, code, "code for {target:?}");
        assert_eq!(field_type.flags, flags, "flags for {target:?}");
        assert_eq!(field_type.flen, flen, "flen for {target:?}");
        assert_eq!(field_type.decimal, decimal, "decimal for {target:?}");
    }
}
