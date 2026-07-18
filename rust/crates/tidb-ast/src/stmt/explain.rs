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

//! Describe, statistics-lock, and explain statement payloads.

use crate::util::{back_quote, escape_string_literal, push_name_path};
use crate::Stmt;

/// TiDB's shared describe-table normal form: standalone `DESC`/`DESCRIBE`
/// and `EXPLAIN <table>` all reach Go's `ShowColumns` branch and therefore
/// restore as `DESC <table> [column]`. This is distinct from an `EXPLAIN`
/// wrapper around a query or DML statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeTableStmt {
    /// The described table's dotted name path.
    pub table: Vec<String>,
    /// An optional dotted column-name path filter.
    pub column: Option<Vec<String>>,
}

impl DescribeTableStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DESC ");
        push_name_path(out, &self.table);
        if let Some(column) = &self.column {
            out.push(' ');
            push_name_path(out, column);
        }
    }
}

/// One table target of `LOCK STATS` or `UNLOCK STATS`. Go attaches an
/// optional partition list only to the final table in the target list.
#[derive(Debug, Clone, PartialEq)]
pub struct StatsLockTable {
    /// Target table's dotted name path.
    pub name: Vec<String>,
    /// Optional partition names attached to this target.
    pub partitions: Vec<String>,
}

/// Shared payload of Go's separate `LockStatsStmt` and `UnlockStatsStmt`
/// nodes. The enclosing [`AdminStmt`] variant retains the operation because
/// locking and unlocking have opposite metadata effects.
#[derive(Debug, Clone, PartialEq)]
pub struct StatsLockStmt {
    /// Statistics targets in source order.
    pub tables: Vec<StatsLockTable>,
}

impl StatsLockStmt {
    pub(crate) fn restore_into(&self, out: &mut String, locked: bool) {
        out.push_str(if locked {
            "LOCK STATS "
        } else {
            "UNLOCK STATS "
        });
        for (index, table) in self.tables.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            push_name_path(out, &table.name);
            if !table.partitions.is_empty() {
                out.push_str(" PARTITION(");
                for (partition_index, partition) in table.partitions.iter().enumerate() {
                    if partition_index > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(&back_quote(partition));
                }
                out.push(')');
            }
        }
    }
}

/// The supported `ast.ExplainStmt` shape: an optional `ANALYZE` flag, the
/// Go-format string, and an already parsed explainable inner statement.
///
/// This intentionally excludes Go's `FOR CONNECTION`, plan-digest, and
/// `EXPLORE` branches: each carries a distinct AST contract that this seed
/// AST does not model, so the parser rejects them instead of collapsing them
/// into an ordinary inner statement. The shared `DESC table` normal form is
/// instead represented faithfully by [`DescribeTableStmt`].
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStmt {
    /// Whether the wrapper requested runtime analysis.
    pub analyze: bool,
    /// Go defaults this to `row`; its spelling is preserved for non-default
    /// values because restore prints the original payload case.
    pub format: String,
    /// The DML/query/ALTER statement being explained.
    pub statement: Box<Stmt>,
}

impl ExplainStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("EXPLAIN ");
        if self.analyze {
            out.push_str("ANALYZE ");
        }
        // Go's ast.ExplainStmt suppresses only the default ROW format after
        // ANALYZE. Plain EXPLAIN always writes FORMAT = 'row'.
        if !self.analyze || !self.format.eq_ignore_ascii_case("row") {
            out.push_str("FORMAT = '");
            out.push_str(&escape_string_literal(&self.format));
            out.push_str("' ");
        }
        self.statement.restore_into(out);
    }
}
