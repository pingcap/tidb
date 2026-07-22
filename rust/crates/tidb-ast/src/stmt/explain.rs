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

/// The mutually exclusive targets of Go's `ast.ExplainStmt`.
#[derive(Debug, Clone, PartialEq)]
pub enum ExplainTarget {
    /// An ordinary query, DML, or ALTER TABLE statement.
    Statement(Box<Stmt>),
    /// A plan digest supplied as a string literal.
    PlanDigest(String),
    /// `EXPLORE 'sql-or-digest'`.
    ExploreDigest(String),
    /// `EXPLORE REPLAYER 'file'`.
    ExploreReplayer(String),
    /// `EXPLORE` applied to a parsed statement.
    ExploreStatement(Box<Stmt>),
}

/// Complete `ast.ExplainStmt` payload. `FOR CONNECTION` remains the separate
/// [`crate::ExplainForStmt`] node used by Go.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStmt {
    /// Whether the wrapper requested runtime analysis.
    pub analyze: bool,
    /// Go defaults this to `row`; its spelling is preserved for non-default
    /// values because restore prints the original payload case.
    pub format: String,
    /// Explain target.
    pub target: ExplainTarget,
}

impl ExplainStmt {
    /// Returns the parsed statement target when this EXPLAIN owns one.
    pub fn statement(&self) -> Option<&Stmt> {
        match &self.target {
            ExplainTarget::Statement(statement) | ExplainTarget::ExploreStatement(statement) => {
                Some(statement)
            }
            _ => None,
        }
    }

    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("EXPLAIN ");
        if self.analyze {
            out.push_str("ANALYZE ");
        }
        match &self.target {
            ExplainTarget::ExploreDigest(value) => {
                out.push_str("EXPLORE '");
                out.push_str(&escape_string_literal(value));
                out.push('\'');
            }
            ExplainTarget::ExploreReplayer(file) => {
                out.push_str("EXPLORE REPLAYER '");
                out.push_str(&escape_string_literal(file));
                out.push('\'');
            }
            ExplainTarget::ExploreStatement(statement) => {
                out.push_str("EXPLORE ");
                statement.restore_into(out);
            }
            ExplainTarget::PlanDigest(digest) => {
                if !self.analyze || !self.format.eq_ignore_ascii_case("row") {
                    restore_format(out, &self.format);
                }
                out.push('\'');
                out.push_str(&escape_string_literal(digest));
                out.push('\'');
            }
            ExplainTarget::Statement(statement) => {
                if !self.analyze || !self.format.eq_ignore_ascii_case("row") {
                    restore_format(out, &self.format);
                }
                statement.restore_into(out);
            }
        }
    }
}

fn restore_format(out: &mut String, format: &str) {
    out.push_str("FORMAT = '");
    out.push_str(&escape_string_literal(format));
    out.push_str("' ");
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for DescribeTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table, column } = self;
        let _ = table;
        let _ = column;
        visitor.leave(self)
    }
}

impl crate::Visitable for StatsLockTable {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, partitions } = self;
        let _ = name;
        let _ = partitions;
        visitor.leave(self)
    }
}

impl crate::Visitable for StatsLockStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { tables } = self;
        for value in tables.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = tables;
        visitor.leave(self)
    }
}

impl crate::Visitable for ExplainTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Statement(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::PlanDigest(field_0) => {
                let _ = field_0;
            }
            Self::ExploreDigest(field_0) => {
                let _ = field_0;
            }
            Self::ExploreReplayer(field_0) => {
                let _ = field_0;
            }
            Self::ExploreStatement(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ExplainStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            analyze,
            format,
            target,
        } = self;
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        let _ = analyze;
        let _ = format;
        let _ = target;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
