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

//! `ALTER TABLE ... ALTER [COLUMN] name ... DEFAULT` payload.
//!
//! Go's `HandParser.parseAlterAlter` stores this as an ALTER-column spec,
//! with an optional default-value option. Keep it separate from MODIFY/CHANGE
//! column definitions: this operation owns only an existing column name and
//! either a default expression or a drop-default marker.

use crate::Expr;

/// A default-value change for an existing column.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterColumnDefault {
    /// Existing column name.
    pub name: String,
    /// New default expression, or `None` for `DROP DEFAULT`.
    pub default_value: Option<Expr>,
}
