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

use super::FieldTypeCode;

/// Returns the source type label for one code.
pub fn type_str(code: FieldTypeCode) -> &'static str {
    type_to_str(code, "")
}

/// Returns the source type label, applying binary text/blob aliases.
pub fn type_to_str(code: FieldTypeCode, charset: &str) -> &'static str {
    let binary = charset == "binary";
    match code {
        FieldTypeCode::Bit => "bit",
        FieldTypeCode::Blob => {
            if binary {
                "blob"
            } else {
                "text"
            }
        }
        FieldTypeCode::Date => "date",
        FieldTypeCode::Datetime => "datetime",
        FieldTypeCode::Unspecified => "unspecified",
        FieldTypeCode::NewDecimal => "decimal",
        FieldTypeCode::Double => "double",
        FieldTypeCode::Enum => "enum",
        FieldTypeCode::Float => "float",
        FieldTypeCode::Geometry => "geometry",
        FieldTypeCode::VectorFloat32 => "vector",
        FieldTypeCode::Int24 => "mediumint",
        FieldTypeCode::Json => "json",
        FieldTypeCode::Long => "int",
        FieldTypeCode::LongLong => "bigint",
        FieldTypeCode::LongBlob => {
            if binary {
                "longblob"
            } else {
                "longtext"
            }
        }
        FieldTypeCode::MediumBlob => {
            if binary {
                "mediumblob"
            } else {
                "mediumtext"
            }
        }
        FieldTypeCode::Null => {
            if binary {
                "binary"
            } else {
                "null"
            }
        }
        FieldTypeCode::Set => "set",
        FieldTypeCode::Short => "smallint",
        FieldTypeCode::String => {
            if binary {
                "binary"
            } else {
                "char"
            }
        }
        FieldTypeCode::Duration => "time",
        FieldTypeCode::Timestamp => "timestamp",
        FieldTypeCode::Tiny => "tinyint",
        FieldTypeCode::TinyBlob => {
            if binary {
                "tinyblob"
            } else {
                "tinytext"
            }
        }
        FieldTypeCode::Varchar => {
            if binary {
                "varbinary"
            } else {
                "varchar"
            }
        }
        FieldTypeCode::VarString => "var_string",
        FieldTypeCode::Year => "year",
        FieldTypeCode::NewDate | FieldTypeCode::Unknown(_) => "",
    }
}

/// Converts a source type label to its code, including blob/binary aliases.
pub fn str_to_type(label: &str) -> FieldTypeCode {
    let label = label
        .replacen("blob", "text", 1)
        .replacen("binary", "char", 1);
    match label.as_str() {
        "bit" => FieldTypeCode::Bit,
        "text" => FieldTypeCode::Blob,
        "date" => FieldTypeCode::Date,
        "datetime" => FieldTypeCode::Datetime,
        "unspecified" => FieldTypeCode::Unspecified,
        "decimal" => FieldTypeCode::NewDecimal,
        "double" => FieldTypeCode::Double,
        "enum" => FieldTypeCode::Enum,
        "float" => FieldTypeCode::Float,
        "geometry" => FieldTypeCode::Geometry,
        "vector" => FieldTypeCode::VectorFloat32,
        "mediumint" => FieldTypeCode::Int24,
        "json" => FieldTypeCode::Json,
        "int" => FieldTypeCode::Long,
        "bigint" => FieldTypeCode::LongLong,
        "longtext" => FieldTypeCode::LongBlob,
        "mediumtext" => FieldTypeCode::MediumBlob,
        "null" => FieldTypeCode::Null,
        "set" => FieldTypeCode::Set,
        "smallint" => FieldTypeCode::Short,
        "char" => FieldTypeCode::String,
        "time" => FieldTypeCode::Duration,
        "timestamp" => FieldTypeCode::Timestamp,
        "tinyint" => FieldTypeCode::Tiny,
        "tinytext" => FieldTypeCode::TinyBlob,
        "varchar" => FieldTypeCode::Varchar,
        "var_string" => FieldTypeCode::VarString,
        "year" => FieldTypeCode::Year,
        _ => FieldTypeCode::Unspecified,
    }
}
