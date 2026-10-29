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

//! Go error prototypes (`ErrFoo = ClassBar.NewStd(...)`) of every package
//! outside the three that already own a catalog
//! (`pkg/util/dbterror`, `.../plannererrors`, `.../exeerrors`).
//!
//! Go identity travels with the error value: a `*terror.Error` knows its class
//! and code, and `Error()` prints `[<class>:<code>]<message>`. Codes are not
//! unique across classes (`1146` is `infoschema.ErrTableNotExists` in class
//! `schema` and `plannererrors.ErrNoSuchTable` in class `planner`), so the class
//! can only come from the Go definition a raise site names -- these statics are
//! those definitions. Module = Go package, constant = the Go name in
//! SCREAMING_SNAKE.

use tidb_terror::{TerrorClass, TerrorCode, TerrorError};
use tidb_mysql::errors::ErrMessage;

/// Alias keeping the generated tables short.
pub(crate) type Class = TerrorClass;

/// `Class.NewStd(code)`: the catalog message under the same code.
pub(crate) fn std_err(class: Class, code: u16) -> TerrorError {
    TerrorError::registered_std(class, TerrorCode::new(code as isize))
}

/// `Class.NewStdErr(code, mysql.Message(raw, nil))`.
pub(crate) fn plain_err(class: Class, code: u16, raw: &'static str) -> TerrorError {
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
pub(crate) fn std_err_msg(
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

pub mod admin;
pub mod ast;
pub mod autoid;
pub mod charset;
pub mod collate;
pub mod consistency;
pub mod domain;
pub mod executor;
pub mod expression;
pub mod hint;
pub mod infoschema;
pub mod infosync;
pub mod kv;
pub mod memory;
pub mod meta;
pub mod param;
pub mod parser;
pub mod parser_types;
pub mod plugin;
pub mod privileges;
pub mod server;
pub mod servererr;
pub mod session;
pub mod sessionstates;
pub mod statistics;
pub mod structure;
pub mod table;
pub mod tablecodec;
pub mod tables;
pub mod tiflashcompute;
pub mod tikv;
pub mod timeutil;
pub mod types;
pub mod variable;
pub mod workloadrepo;
