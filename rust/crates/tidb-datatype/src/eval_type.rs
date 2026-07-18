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

use std::{error::Error, fmt};

/// The value representation used to evaluate a built-in function.
///
/// This is the single Rust type for both `pkg/parser/types.EvalType` and the
/// alias exported by `pkg/types`. Keeping the alias surface as constants of
/// this type preserves Go's identity relationship without a second enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EvalType {
    /// Go `ETInt`.
    Int = 0,
    /// Go `ETReal`.
    Real = 1,
    /// Go `ETDecimal`.
    Decimal = 2,
    /// Go `ETString`.
    String = 3,
    /// Go `ETDatetime`.
    Datetime = 4,
    /// Go `ETTimestamp`.
    Timestamp = 5,
    /// Go `ETDuration`.
    Duration = 6,
    /// Go `ETJson`.
    Json = 7,
    /// Go `ETVectorFloat32`.
    VectorFloat32 = 8,
}

impl EvalType {
    /// Every valid source discriminant in declaration order.
    pub const ALL: [Self; 9] = [
        Self::Int,
        Self::Real,
        Self::Decimal,
        Self::String,
        Self::Datetime,
        Self::Timestamp,
        Self::Duration,
        Self::Json,
        Self::VectorFloat32,
    ];

    /// Mirrors `EvalType.IsStringKind`.
    ///
    /// Vector values intentionally belong to this source-defined family even
    /// though they also have their own vector classification.
    pub const fn is_string_kind(self) -> bool {
        matches!(
            self,
            Self::String
                | Self::Datetime
                | Self::Timestamp
                | Self::Duration
                | Self::Json
                | Self::VectorFloat32
        )
    }

    /// Mirrors `EvalType.IsVectorKind`.
    pub const fn is_vector_kind(self) -> bool {
        matches!(self, Self::VectorFloat32)
    }

    /// Returns the exact text emitted by Go's `EvalType.String`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Int => "Int",
            Self::Real => "Real",
            Self::Decimal => "Decimal",
            Self::String => "String",
            Self::Datetime => "Datetime",
            Self::Timestamp => "Timestamp",
            Self::Duration => "Time",
            Self::Json => "Json",
            Self::VectorFloat32 => "VectorFloat32",
        }
    }
}

impl fmt::Display for EvalType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<EvalType> for u8 {
    fn from(eval_type: EvalType) -> Self {
        eval_type as Self
    }
}

/// A byte outside the source-defined `EvalType` discriminant range.
///
/// Go can construct such a byte and panics only when formatting it. Rust
/// rejects it at the numeric boundary, so every constructed [`EvalType`] is
/// safe to classify and display.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidEvalType(u8);

impl InvalidEvalType {
    /// Returns the rejected source byte.
    pub const fn value(self) -> u8 {
        self.0
    }
}

impl fmt::Display for InvalidEvalType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid EvalType {}", self.0)
    }
}

impl Error for InvalidEvalType {}

impl TryFrom<u8> for EvalType {
    type Error = InvalidEvalType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Int),
            1 => Ok(Self::Real),
            2 => Ok(Self::Decimal),
            3 => Ok(Self::String),
            4 => Ok(Self::Datetime),
            5 => Ok(Self::Timestamp),
            6 => Ok(Self::Duration),
            7 => Ok(Self::Json),
            8 => Ok(Self::VectorFloat32),
            invalid => Err(InvalidEvalType(invalid)),
        }
    }
}

// `pkg/types/eval_type.go` aliases both the type and every constant from
// `pkg/parser/types`; these constants reproduce that public alias surface while
// retaining exactly one Rust enum.
/// The `pkg/types.ETInt` alias.
pub const ET_INT: EvalType = EvalType::Int;
/// The `pkg/types.ETReal` alias.
pub const ET_REAL: EvalType = EvalType::Real;
/// The `pkg/types.ETDecimal` alias.
pub const ET_DECIMAL: EvalType = EvalType::Decimal;
/// The `pkg/types.ETString` alias.
pub const ET_STRING: EvalType = EvalType::String;
/// The `pkg/types.ETDatetime` alias.
pub const ET_DATETIME: EvalType = EvalType::Datetime;
/// The `pkg/types.ETTimestamp` alias.
pub const ET_TIMESTAMP: EvalType = EvalType::Timestamp;
/// The `pkg/types.ETDuration` alias.
pub const ET_DURATION: EvalType = EvalType::Duration;
/// The `pkg/types.ETJson` alias.
pub const ET_JSON: EvalType = EvalType::Json;
/// The `pkg/types.ETVectorFloat32` alias.
pub const ET_VECTOR_FLOAT32: EvalType = EvalType::VectorFloat32;
