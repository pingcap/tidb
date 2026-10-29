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

//! Error prototypes defined by the Go package(s) named per entry.
//!
//! GENERATED mechanically from the Go sources by scanning every
//! package-level `Class*.NewStd` / `NewStdErr` definition; the (class,
//! code) set of every module is cross-checked against the real Go
//! registry (`terror.ErrClassToMySQLCodes`) by
//! `tests/catalog_source.rs`. Do not hand-edit entries.

use std::sync::LazyLock;

use super::{std_err, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

/// Source `parser/types.ErrInvalidDefault` (`pkg/parser/types/etc.go:163`).
pub static ERR_INVALID_DEFAULT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrInvalidDefault));
/// Source `parser/types.ErrDataOutOfRange` (`pkg/parser/types/etc.go:165`).
pub static ERR_DATA_OUT_OF_RANGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrDataOutOfRange));
/// Source `parser/types.ErrTruncatedWrongValue` (`pkg/parser/types/etc.go:168`).
pub static ERR_TRUNCATED_WRONG_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrTruncatedWrongValue));
/// Source `parser/types.ErrIllegalValueForType` (`pkg/parser/types/etc.go:170`).
pub static ERR_ILLEGAL_VALUE_FOR_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Types, errcode::ErrIllegalValueForType));
