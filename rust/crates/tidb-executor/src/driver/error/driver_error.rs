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

use std::borrow::Cow;

use tidb_error::terror::TerrorError;

use crate::ExecError;

/// A failure while running a SQL string through the driver.
#[derive(Debug, Clone)]
pub enum DriverError {
    /// An internal storage service's already-classified MySQL failure.
    Mysql(super::MysqlError),
    /// The general refusal: this tier does not implement what the statement
    /// asked for, and the carried text says which part.
    ///
    /// It is raised from every layer -- an `EXPLAIN` format name, a catalog
    /// object kind, an unported `ALTER TABLE` action, an operator with no
    /// executor -- and is by far the most-constructed variant here. The
    /// doc it replaces said "not a supported `FROM`-less `SELECT`", which
    /// described the driver when a `FROM` clause was the frontier; it has not
    /// been true for any of the hundreds of sites since.
    ///
    /// Everything it refuses reaches the client as Go's catch-all 1105, so
    /// the carried text IS the whole diagnostic. Say what was refused, not
    /// that something was.
    ///
    /// [`Cow`] rather than `&'static str` because a refusal that names the
    /// AST kind it saw builds its text per call. That used to be a SECOND
    /// variant, `UnsupportedKind(String)`, which rendered byte for byte
    /// identically on adjacent lines of `to_mysql_error` -- nothing on the
    /// wire could tell the two apart, and the only difference was who owned
    /// the bytes. Build one with [`DriverError::unsupported`], which takes
    /// either.
    Unsupported(Cow<'static, str>),
    /// Rewriting an expression or executing failed.
    Exec(ExecError),
    /// The shared catalog is unusable because a statement panicked while
    /// holding it, so its schema state may be half-written.
    CatalogPoisoned,
    /// A memory-table scan needs the session to retrieve its rows
    /// (`StmtContext::mem_table_retrieval`). Internal: the session answers
    /// the request and rebuilds, so a client sees it only when a statement
    /// path forgot to.
    MemTableRetrievalNeeded,
}

impl DriverError {
    /// [`DriverError::Unsupported`] from either a fixed reason or one built
    /// per call.
    ///
    /// Both spellings render identically on the wire, so this is the one
    /// constructor; the borrow stays borrowed and the owned string is moved.
    #[must_use]
    pub fn unsupported(reason: impl Into<Cow<'static, str>>) -> Self {
        DriverError::Unsupported(reason.into())
    }

    /// Re-raises this failure as another Go definition with the same wire
    /// code: one `DriverError` variant serves raise sites whose Go errors
    /// differ only by class (`infoschema.ErrKeyNotExists` from DDL,
    /// `plannererrors.ErrKeyDoesNotExist` from the planner), and the site
    /// knows which Go function it transcribes.
    #[must_use]
    pub fn in_class(self, proto: &'static TerrorError) -> Self {
        let error = self.to_mysql_error();
        debug_assert_eq!(
            error.code,
            proto.to_sql_error().code,
            "a class change keeps the code"
        );
        DriverError::Mysql(error.with_identity(proto.identity()))
    }

    /// Go `ErrX.Equal(err)` / `terror.ErrorEqual(err, ErrX)`: whether this
    /// failure was raised as Go's `proto` (same class and code).
    #[must_use]
    pub fn equal(&self, proto: &TerrorError) -> bool {
        match self {
            DriverError::Mysql(error) => error.identity == Some(proto.identity()),
            other => other.clone().to_mysql_error().identity == Some(proto.identity()),
        }
    }

    /// Go `err.Error()` for this failure: `[<class>:<code>]<message>` for a
    /// `*terror.Error`, the bare message for a plain error. What
    /// `@@tidb_last_query_info`, the logs and every embedded `err.Error()`
    /// print; [`Self::to_mysql_error`]'s message alone is the wire text.
    #[must_use]
    pub fn error_string(&self) -> String {
        self.clone().to_mysql_error().error_string()
    }
}
