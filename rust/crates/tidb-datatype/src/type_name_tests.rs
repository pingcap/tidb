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

//! Translation of `pkg/types/etc_test.go::TestTypeToStr` against the
//! `field_type::names` helpers (`TypeStr` / `TypeToStr` in Go's
//! `pkg/parser/types/field_type.go`).

use crate::{type_str, type_to_str, FieldTypeCode};

/// Source: `pkg/types/etc_test.go::TestTypeToStr` (`testTypeStr` rows).
#[test]
fn test_type_str() {
    assert_eq!(type_str(FieldTypeCode::Year), "year");
    // Unknown type codes render as an empty label.
    assert_eq!(type_str(FieldTypeCode::Unknown(0xdd)), "");
}

/// Source: `pkg/types/etc_test.go::TestTypeToStr` (`testTypeToStr` rows).
#[test]
fn test_type_to_str() {
    let rows: &[(FieldTypeCode, &str, &str)] = &[
        (FieldTypeCode::Blob, "utf8", "text"),
        (FieldTypeCode::LongBlob, "utf8", "longtext"),
        (FieldTypeCode::TinyBlob, "utf8", "tinytext"),
        (FieldTypeCode::MediumBlob, "utf8", "mediumtext"),
        (FieldTypeCode::Varchar, "binary", "varbinary"),
        (FieldTypeCode::String, "binary", "binary"),
        (FieldTypeCode::Tiny, "binary", "tinyint"),
        (FieldTypeCode::Blob, "binary", "blob"),
        (FieldTypeCode::LongBlob, "binary", "longblob"),
        (FieldTypeCode::TinyBlob, "binary", "tinyblob"),
        (FieldTypeCode::MediumBlob, "binary", "mediumblob"),
        (FieldTypeCode::Varchar, "utf8", "varchar"),
        (FieldTypeCode::String, "utf8", "char"),
        (FieldTypeCode::Short, "binary", "smallint"),
        (FieldTypeCode::Int24, "binary", "mediumint"),
        (FieldTypeCode::Long, "binary", "int"),
        (FieldTypeCode::LongLong, "binary", "bigint"),
        (FieldTypeCode::Float, "binary", "float"),
        (FieldTypeCode::Double, "binary", "double"),
        (FieldTypeCode::Year, "binary", "year"),
        (FieldTypeCode::Duration, "binary", "time"),
        (FieldTypeCode::Datetime, "binary", "datetime"),
        (FieldTypeCode::Date, "binary", "date"),
        (FieldTypeCode::Timestamp, "binary", "timestamp"),
        (FieldTypeCode::NewDecimal, "binary", "decimal"),
        (FieldTypeCode::Unspecified, "binary", "unspecified"),
        (FieldTypeCode::Unknown(0xdd), "binary", ""),
        (FieldTypeCode::Bit, "binary", "bit"),
        (FieldTypeCode::Enum, "binary", "enum"),
        (FieldTypeCode::Set, "binary", "set"),
    ];
    for (code, charset, expected) in rows {
        assert_eq!(type_to_str(*code, charset), *expected, "{code:?}/{charset}");
    }
}
