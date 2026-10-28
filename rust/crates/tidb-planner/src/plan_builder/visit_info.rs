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

//! Go's `visitInfo`: the privileges a statement needs, collected while it is
//! PLANNED rather than from its syntax.
//!
//! Go's builder appends one entry per table it resolves (`buildDataSource`),
//! per locked table of a locking read, and per view it expands, then
//! `CheckPrivilege` verifies the list after planning. Collecting at plan time
//! is what makes the answer follow name resolution (a CTE or derived table
//! needs nothing, a view's body is verified against its definer) instead of a
//! second, syntactic reading of the statement.

use tidb_mysql::PrivilegeType;

/// The error Go attaches to a [`VisitInfo`] (`authErr`), reported when the
/// check fails. Without one a failed check is `ErrPrivilegeCheckFail` (8121).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VisitAuthError {
    /// No `authErr`.
    None,
    /// `plannererrors.ErrTableaccessDenied` (1142): `"<command> command denied
    /// to user ... for table '<table>'"`. The user is the session's, which the
    /// verifier owns.
    TableAccessDenied {
        /// The command the message names (`SELECT`, `SELECT with locking clause`).
        command: &'static str,
        /// The table name as Go prints it (`TableInfo.Name.L`).
        table: String,
    },
    /// `plannererrors.ErrViewNoExplain`.
    ViewNoExplain,
    /// `plannererrors.ErrDBaccessDenied` (1044) for the database named.
    DbAccessDenied {
        /// The database as Go prints it.
        database: String,
    },
    /// `plannererrors.ErrSpecificAccessDenied` (1227) naming the privilege.
    SpecificAccessDenied {
        /// The privilege text the message names (`CONFIG`, `SUPER or
        /// BACKUP_ADMIN`).
        privilege: &'static str,
    },
    /// `plannererrors.ErrPrivilegeCheckFail`.
    PrivilegeCheckFail,
}

/// Go `visitInfo`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VisitInfo {
    /// The privilege (a mask, as Go's `mysql.PrivilegeType`: a locking read
    /// asks for `DELETE|UPDATE|LOCK TABLES` and is satisfied by any one).
    pub privilege: PrivilegeType,
    /// Lowercased schema.
    pub database: String,
    /// Lowercased table.
    pub table: String,
    /// Column, empty for the table scope.
    pub column: String,
    /// Go's `authErr`.
    pub error: VisitAuthError,
    /// Go `dynamicPrivs`: any one of them passes the check.
    pub dynamic_privs: Vec<String>,
    /// Go `dynamicWithGrant`.
    pub dynamic_with_grant: bool,
}

pub use tidb_infoschema::table_source::PrivilegeVerifier;
