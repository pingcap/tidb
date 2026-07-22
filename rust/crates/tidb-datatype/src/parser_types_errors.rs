// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Exported error prototypes from `pkg/parser/types/etc.go`.

use std::sync::LazyLock;

use tidb_error::mysql::errcode;
use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError};

fn types_error(code: u16) -> TerrorError {
    TerrorError::registered_from_catalog(
        TerrorClass::Types,
        TerrorCode::new(
            isize::try_from(code).expect("MySQL error code fits the source int domain"),
        ),
    )
}

/// Source `types.ErrInvalidDefault`.
pub static ERR_INVALID_DEFAULT: LazyLock<TerrorError> =
    LazyLock::new(|| types_error(errcode::ErrInvalidDefault));
/// Source `types.ErrDataOutOfRange`.
pub static ERR_DATA_OUT_OF_RANGE: LazyLock<TerrorError> =
    LazyLock::new(|| types_error(errcode::ErrDataOutOfRange));
/// Source `types.ErrTruncatedWrongValue`.
pub static ERR_TRUNCATED_WRONG_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| types_error(errcode::ErrTruncatedWrongValue));
/// Source `types.ErrIllegalValueForType`.
pub static ERR_ILLEGAL_VALUE_FOR_TYPE: LazyLock<TerrorError> =
    LazyLock::new(|| types_error(errcode::ErrIllegalValueForType));
