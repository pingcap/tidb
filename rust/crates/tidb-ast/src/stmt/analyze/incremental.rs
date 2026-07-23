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

//! Typed `ANALYZE INCREMENTAL TABLE` payloads from
//! `pkg/parser/ddl_drop_parser.go`.

use super::restore_analyze_body;
use crate::{AnalyzeOption, AnalyzeTarget};

/// The source-backed incremental-analysis form. It deliberately shares the
/// complete post-`TABLE` payload with ordinary [`crate::AnalyzeTableStmt`]
/// because Go parses both through the same production after setting one flag.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeIncrementalStmt {
    /// Whether `NO_WRITE_TO_BINLOG` or `LOCAL` preceded `INCREMENTAL`.
    pub no_write_to_binlog: bool,
    /// One or more source table names.
    pub tables: Vec<Vec<String>>,
    /// Optional partition-name restriction.
    pub partitions: Vec<String>,
    /// The complete selector shared with ordinary ANALYZE TABLE.
    pub target: AnalyzeTarget,
    /// Ordered `WITH` options shared with ordinary ANALYZE TABLE.
    pub options: Vec<AnalyzeOption>,
}

impl AnalyzeIncrementalStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ANALYZE ");
        if self.no_write_to_binlog {
            out.push_str("NO_WRITE_TO_BINLOG ");
        }
        out.push_str("INCREMENTAL TABLE ");
        restore_analyze_body(
            out,
            &self.tables,
            &self.partitions,
            &self.target,
            &self.options,
        );
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AnalyzeIncrementalStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            no_write_to_binlog,
            tables,
            partitions,
            target,
            options,
        } = self;
        let _ = no_write_to_binlog;
        let _ = tables;
        let _ = partitions;
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        let _ = target;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = options;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
