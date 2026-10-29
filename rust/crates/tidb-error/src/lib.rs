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

//! Shared error-number, message, redaction, SQLSTATE, `SQLError`, and TiDB
//! class/code/RFC identity authority.
//!
//! `pkg/errno/{errcode,errname}.go`, Go's `parser/terror`, `errctx`,
//! `plannererrors`, `server/err` and every package's error prototypes
//! (`catalog`); `pkg/parser/mysql`'s errors are tidb-mysql's (`errors`). Protocol, execution, and transactional
//! crates depend on this leaf instead of copying numeric registries or local
//! terror-class markers.

pub mod catalog;
pub mod errctx;
mod mysql_error;
pub mod plannererrors;
pub mod server_errors;
pub mod terror;
pub mod tidb;

pub use mysql_error::MysqlError;

