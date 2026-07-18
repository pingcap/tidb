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

use crate::{
    EvalType, ET_DATETIME, ET_DECIMAL, ET_DURATION, ET_INT, ET_JSON, ET_REAL, ET_STRING,
    ET_TIMESTAMP, ET_VECTOR_FLOAT32,
};

/// Sources: the `iota` declaration in `pkg/parser/types/eval_type.go` and all
/// aliases in `pkg/types/eval_type.go`.
#[test]
fn every_discriminant_and_alias_matches_the_go_source() {
    let rows = [
        (ET_INT, EvalType::Int, 0),
        (ET_REAL, EvalType::Real, 1),
        (ET_DECIMAL, EvalType::Decimal, 2),
        (ET_STRING, EvalType::String, 3),
        (ET_DATETIME, EvalType::Datetime, 4),
        (ET_TIMESTAMP, EvalType::Timestamp, 5),
        (ET_DURATION, EvalType::Duration, 6),
        (ET_JSON, EvalType::Json, 7),
        (ET_VECTOR_FLOAT32, EvalType::VectorFloat32, 8),
    ];

    assert_eq!(EvalType::ALL.len(), rows.len());
    for (index, (alias, variant, source_byte)) in rows.into_iter().enumerate() {
        assert_eq!(alias, variant);
        assert_eq!(EvalType::ALL[index], variant);
        assert_eq!(u8::from(variant), source_byte);
        assert_eq!(EvalType::try_from(source_byte), Ok(variant));
    }
}

/// Source: `pkg/parser/types/eval_type.go::EvalType.IsStringKind`.
#[test]
fn string_kind_classifies_every_source_discriminant() {
    let rows = [
        (EvalType::Int, false),
        (EvalType::Real, false),
        (EvalType::Decimal, false),
        (EvalType::String, true),
        (EvalType::Datetime, true),
        (EvalType::Timestamp, true),
        (EvalType::Duration, true),
        (EvalType::Json, true),
        (EvalType::VectorFloat32, true),
    ];

    for (eval_type, expected) in rows {
        assert_eq!(eval_type.is_string_kind(), expected, "{eval_type}");
    }
}

/// Source: `pkg/parser/types/eval_type.go::EvalType.IsVectorKind`.
#[test]
fn vector_kind_classifies_every_source_discriminant() {
    for eval_type in EvalType::ALL {
        assert_eq!(
            eval_type.is_vector_kind(),
            eval_type == EvalType::VectorFloat32,
            "{eval_type}"
        );
    }
}

/// Source: `pkg/parser/types/eval_type.go::EvalType.String`.
#[test]
fn display_matches_every_go_string_case() {
    let rows = [
        (EvalType::Int, "Int"),
        (EvalType::Real, "Real"),
        (EvalType::Decimal, "Decimal"),
        (EvalType::String, "String"),
        (EvalType::Datetime, "Datetime"),
        (EvalType::Timestamp, "Timestamp"),
        (EvalType::Duration, "Time"),
        (EvalType::Json, "Json"),
        (EvalType::VectorFloat32, "VectorFloat32"),
    ];

    for (eval_type, text) in rows {
        assert_eq!(eval_type.as_str(), text);
        assert_eq!(eval_type.to_string(), text);
    }
}

/// Source boundary: Go's underlying type is a byte, but formatting an invalid
/// byte panics. Rust rejects the byte before the value exists.
#[test]
fn invalid_source_bytes_are_typed_errors_instead_of_panic_states() {
    for invalid in 9..=u8::MAX {
        let error = EvalType::try_from(invalid).unwrap_err();
        assert_eq!(error.value(), invalid);
        assert_eq!(error.to_string(), format!("invalid EvalType {invalid}"));
    }
}
