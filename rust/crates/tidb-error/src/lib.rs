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
//! The two catalogs are direct checked-in translations of
//! `pkg/parser/mysql/{errcode,errname,state,error}.go` and
//! `pkg/errno/{errcode,errname}.go`. Protocol, execution, and transactional
//! crates depend on this leaf instead of copying numeric registries or local
//! terror-class markers.

pub mod mysql;
pub mod plannererrors;
pub mod terror;
pub mod tidb;

/// A source error message template and its zero-based sensitive arguments.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErrMessage {
    /// Go `fmt` template stored by the source catalog.
    pub raw: &'static str,
    /// Argument positions passed to `errors.RedactErrorArg`.
    pub redact_arg_pos: &'static [usize],
}

/// One named entry in a checked-in source error catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogEntry {
    /// Source Go identifier.
    pub name: &'static str,
    /// MySQL protocol error number.
    pub code: u16,
    /// Default message template and redaction metadata.
    pub message: ErrMessage,
}
