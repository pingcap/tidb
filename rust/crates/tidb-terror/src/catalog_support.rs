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

//! The constructors the generated per-package error prototype catalogs use.

use crate::{TerrorClass, TerrorCode, TerrorError};
use tidb_mysql::errors::ErrMessage;

/// Alias keeping the generated tables short.
/// `ErrClass`, as the generated prototype catalogs spell it.
pub type Class = TerrorClass;

/// `Class.NewStd(code)`: the catalog message under the same code.
/// Go `ErrClass.NewStd(code)`: the catalog message under the same code.
pub fn std_err(class: Class, code: u16) -> TerrorError {
    TerrorError::registered_std(class, TerrorCode::new(code as isize))
}

/// `Class.NewStdErr(code, mysql.Message(raw, nil))`.
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

/// `Class.NewStdErr(code, Message(MySQLErrName[message_code].Raw+suffix,
/// MySQLErrName[message_code].RedactArgPos))`.
/// Go `ErrClass.NewStdErr(code, Message(MySQLErrName[message_code].Raw+suffix, ...))`.
pub fn std_err_msg(
    class: Class,
    code: u16,
    message_code: u16,
    suffix: &'static str,
) -> TerrorError {
    let catalog = tidb_errno::message_by_code(message_code)
        .or_else(|| tidb_mysql::errors::message_by_code(message_code))
        .copied()
        .expect("NewStdErr message code must exist in the MySQL or TiDB catalog");
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
