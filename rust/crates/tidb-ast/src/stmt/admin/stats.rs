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

//! `LOAD STATS` and `DROP STATS`, mirroring Go's `LoadStatsStmt` and
//! `DropStatsStmt` in `pkg/parser/ast/misc.go`.

use super::*;

/// TiDB's `LOAD STATS 'path'` parser/restore envelope.
///
/// Applying a statistics artifact needs TiDB's statistics handle, infoschema
/// versioning, and session domain. The seed executor rejects this before it
/// changes transaction or catalog state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadStatsStmt {
    /// Decoded statistics artifact path.
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// TiDB's statistics-deletion command and its optional deprecated scopes.
pub struct DropStatsStmt {
    /// Target tables in source order.
    pub tables: Vec<Vec<String>>,
    /// Whether the deprecated `GLOBAL` scope was specified.
    pub global: bool,
    /// Optional deprecated partition names.
    pub partitions: Vec<String>,
}

impl DropStatsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP STATS ");
        for (index, table) in self.tables.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            push_name_path(out, table);
        }
        if self.global {
            out.push_str(" GLOBAL");
        }
        if !self.partitions.is_empty() {
            out.push_str(" PARTITION ");
            for (index, partition) in self.partitions.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&crate::util::back_quote(partition));
            }
        }
    }
}

impl LoadStatsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("LOAD STATS ");
        // Go's `LoadStatsStmt.Restore` uses `WriteString`, unlike ordinary
        // scalar string expressions which use `_UTF8MB4` under default flags.
        out.push('\'');
        out.push_str(&escape_string_literal(&self.path));
        out.push('\'');
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for LoadStatsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { path } = self;
        let _ = path;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropStatsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            tables,
            global,
            partitions,
        } = self;
        let _ = tables;
        let _ = global;
        let _ = partitions;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
