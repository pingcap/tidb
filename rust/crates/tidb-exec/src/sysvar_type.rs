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

//! System-variable type-kind flags from `pkg/sessionctx/vardef/tidb_vars.go`.
//!
//! TiDB's `TypeFlag` is a byte-backed kind tag, with source discriminants for
//! strings, booleans, integers, enums, floats, unsigned integers, times, and
//! durations. This leaf ports that value domain only; SysVar registration,
//! validation, parsing, and value conversion remain external.

/// Byte-backed system-variable type kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SysVarType(u8);

impl SysVarType {
    /// String-valued system variable (source `TypeStr`).
    pub const STR: Self = Self(0);
    /// Boolean system variable (source `TypeBool`).
    pub const BOOL: Self = Self(1);
    /// Signed integer system variable (source `TypeInt`).
    pub const INT: Self = Self(2);
    /// Enum system variable (source `TypeEnum`).
    pub const ENUM: Self = Self(3);
    /// Floating-point system variable (source `TypeFloat`).
    pub const FLOAT: Self = Self(4);
    /// Unsigned integer system variable (source `TypeUnsigned`).
    pub const UNSIGNED: Self = Self(5);
    /// Time-valued system variable (source `TypeTime`).
    pub const TIME: Self = Self(6);
    /// Duration-valued system variable (source `TypeDuration`).
    pub const DURATION: Self = Self(7);

    /// Number of defined source type kinds.
    pub const COUNT: u8 = 8;

    /// Creates a type kind from its source byte representation.
    #[must_use]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Returns the source byte representation.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }
}
