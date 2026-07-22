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

//! The source-owned `CREATE VIEW` AST and canonical restore boundary.

use crate::util::{back_quote, push_name_path};
use crate::{QueryStmt, ViewAlgorithm, ViewCheckOption, ViewSecurity};

/// A `CREATE [OR REPLACE] [ALGORITHM = ...] VIEW` definition.
///
/// The grammar carries Go's default `CURRENT_USER` definer and `DEFINER`
/// security mode, and retains explicit alternatives with the shared typed
/// user-identity representation. View execution is deliberately rejected by
/// `tidb-exec` before it changes catalog or transaction state.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStmt {
    /// Whether the source wrote `OR REPLACE`.
    pub or_replace: bool,
    /// The view algorithm, including Go's always-restored default.
    pub algorithm: ViewAlgorithm,
    /// The view definer, including Go's always-restored `CURRENT_USER`
    /// default. This is syntactic metadata only until view execution has a
    /// privilege-aware catalog.
    pub definer: crate::UserSpec,
    /// The view security mode, including Go's always-restored `DEFINER`
    /// default. This is syntactic metadata only until view execution has a
    /// privilege-aware catalog.
    pub security: ViewSecurity,
    /// The view name path.
    pub name: Vec<String>,
    /// Optional output column names, in written order.
    pub columns: Vec<String>,
    /// The view query. This uses the typed query envelope so a view cannot
    /// accidentally contain a DDL/DML/session statement.
    pub query: crate::NodeBox<QueryStmt>,
    /// Whether the whole query was enclosed in the `AS (...)` form.
    pub query_parenthesized: bool,
    /// The optional view check option.
    pub check_option: ViewCheckOption,
}

impl CreateViewStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE ");
        if self.or_replace {
            out.push_str("OR REPLACE ");
        }
        out.push_str("ALGORITHM = ");
        out.push_str(self.algorithm.sql());
        out.push_str(" DEFINER = ");
        // `CreateViewStmt.Restore` deliberately differs from
        // `auth.UserIdentity.Restore`: a view's explicitly empty hostname
        // (`DEFINER=``@```) is omitted, while ordinary user statements still
        // restore `@``. Keep that Go-specific behavior at this AST boundary.
        if self.definer.current_user {
            out.push_str("CURRENT_USER");
        } else {
            out.push_str(&back_quote(&self.definer.user));
            if !self.definer.host.is_empty() {
                out.push('@');
                out.push_str(&back_quote(&self.definer.host));
            }
        }
        out.push_str(" SQL SECURITY ");
        out.push_str(self.security.sql());
        out.push_str(" VIEW ");
        push_name_path(out, &self.name);
        if !self.columns.is_empty() {
            out.push_str(" (");
            for (index, column) in self.columns.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&back_quote(column));
            }
            out.push(')');
        }
        out.push_str(" AS ");
        if self.query_parenthesized {
            out.push('(');
        }
        self.query.restore_into(out);
        if self.query_parenthesized {
            out.push(')');
        }
        if self.check_option == ViewCheckOption::Local {
            out.push_str(" WITH LOCAL CHECK OPTION");
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for CreateViewStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            or_replace,
            algorithm,
            definer,
            security,
            name,
            columns,
            query,
            query_parenthesized,
            check_option,
        } = self;
        if !crate::Visitable::accept(algorithm, visitor) {
            return false;
        }
        if !crate::Visitable::accept(definer, visitor) {
            return false;
        }
        if !crate::Visitable::accept(security, visitor) {
            return false;
        }
        if !crate::Visitable::accept(query.as_mut(), visitor) {
            return false;
        }
        if !crate::Visitable::accept(check_option, visitor) {
            return false;
        }
        let _ = or_replace;
        let _ = algorithm;
        let _ = definer;
        let _ = security;
        let _ = name;
        let _ = columns;
        let _ = query;
        let _ = query_parenthesized;
        let _ = check_option;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
