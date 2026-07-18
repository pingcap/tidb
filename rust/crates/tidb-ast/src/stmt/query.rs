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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Query statements and their shared restore boundary.

use crate::{SelectStmt, SetOprStmt};

/// A statement that produces a result set.
///
/// Keeping the two query shapes behind this envelope makes every query-only
/// position (`WITH`, derived tables, `IN` subqueries, and `INSERT ... SELECT`)
/// statically unable to contain DDL, DML, or session commands.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryStmt {
    /// A plain `SELECT` statement.
    Select(Box<SelectStmt>),
    /// A set operation (`UNION` / `EXCEPT` / `INTERSECT`).
    SetOpr(Box<SetOprStmt>),
}

impl QueryStmt {
    /// Appends the query's canonical SQL to `out`.
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Select(select) => select.restore_into(out),
            Self::SetOpr(setopr) => setopr.restore_into(out),
        }
    }
}
