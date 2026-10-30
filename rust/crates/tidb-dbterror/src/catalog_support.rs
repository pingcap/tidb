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

//! The constructors the generated per-package error prototype catalogs of
//! the main module use: Go `dbterror.ErrClass.NewStd` / `NewStdErr`, whose
//! standard messages are `pkg/errno`'s (`errno.MySQLErrName`). The parser
//! module's catalogs use `tidb_terror::catalog_support` instead, Go
//! `terror.ErrClass.NewStd` over `parser/mysql`'s messages.

use tidb_mysql::errors::ErrMessage;
use tidb_terror::TerrorError;

use crate::ErrClass;

/// `ErrClass`, as the generated prototype catalogs spell it.
pub type Class = tidb_terror::TerrorClass;

/// Go `dbterror.ErrClass.NewStd(code)`: the errno catalog message under the
/// same code.
pub fn std_err(class: Class, code: u16) -> TerrorError {
    ErrClass(class).new_std(code)
}

/// Go `ErrClass.NewStdErr(code, mysql.Message(raw, nil))`.
pub fn plain_err(class: Class, code: u16, raw: &'static str) -> TerrorError {
    ErrClass(class).new_std_err(
        code,
        ErrMessage {
            raw,
            redact_arg_pos: &[],
        },
    )
}

/// Go `ErrClass.NewStdErr(code, Message(errno.MySQLErrName[message_code].Raw
/// + suffix, ...RedactArgPos))`.
pub fn std_err_msg(class: Class, code: u16, message_code: u16, suffix: &'static str) -> TerrorError {
    let catalog = crate::catalog_message(message_code);
    let raw: &'static str = if suffix.is_empty() {
        catalog.raw
    } else {
        // One allocation per prototype, made once behind its `LazyLock`.
        Box::leak(format!("{}{suffix}", catalog.raw).into_boxed_str())
    };
    ErrClass(class).new_std_err(
        code,
        ErrMessage {
            raw,
            redact_arg_pos: catalog.redact_arg_pos,
        },
    )
}
