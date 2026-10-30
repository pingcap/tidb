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

//! The constructors the generated error prototype catalogs of the parser
//! module use: Go `terror.ErrClass.NewStd` / `NewStdErr`, whose standard
//! messages are `parser/mysql`'s (`mysql.MySQLErrName`). The main module's
//! catalogs use `tidb_dbterror::catalog_support`, Go `dbterror.ErrClass`
//! over `pkg/errno`'s messages.

use crate::{TerrorClass, TerrorCode, TerrorError};
use tidb_mysql::errors::ErrMessage;

/// `ErrClass`, as the generated prototype catalogs spell it.
pub type Class = TerrorClass;

/// Go `terror.ErrClass.NewStd(code)`: the `parser/mysql` catalog message
/// under the same code.
pub fn std_err(class: Class, code: u16) -> TerrorError {
    TerrorError::registered_from_catalog(class, TerrorCode::new(code as isize))
}

/// Go `ErrClass.NewStdErr(code, mysql.Message(raw, nil))`.
pub fn plain_err(class: Class, code: u16, raw: &'static str) -> TerrorError {
    TerrorError::registered_standard(
        class,
        TerrorCode::new(code as isize),
        ErrMessage {
            raw,
            redact_arg_pos: &[],
        },
    )
}

/// Go `ErrClass.NewStdErr(code, Message(mysql.MySQLErrName[message_code].Raw
/// + suffix, ...RedactArgPos))`.
pub fn std_err_msg(
    class: Class,
    code: u16,
    message_code: u16,
    suffix: &'static str,
) -> TerrorError {
    let catalog = tidb_mysql::errors::message_by_code(message_code)
        .copied()
        .expect("NewStdErr message code must exist in the parser/mysql catalog");
    let raw: &'static str = if suffix.is_empty() {
        catalog.raw
    } else {
        // One allocation per prototype, made once behind its `LazyLock`.
        Box::leak(format!("{}{suffix}", catalog.raw).into_boxed_str())
    };
    TerrorError::registered_standard(
        class,
        TerrorCode::new(code as isize),
        ErrMessage {
            raw,
            redact_arg_pos: catalog.redact_arg_pos,
        },
    )
}
