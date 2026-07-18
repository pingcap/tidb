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

//! `ANALYZE TABLE` payloads and restore translated from
//! `pkg/parser/ast/stats.go`.

use crate::util::{back_quote, push_name_path};

#[path = "analyze/incremental.rs"]
mod incremental;

pub use incremental::{AnalyzeIncrementalStmt, AnalyzeIncrementalTarget};

/// The current source-backed subset of Go's `ANALYZE TABLE` AST.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeTableStmt {
    /// One or more dotted table-name paths, in source order.
    pub tables: Vec<Vec<String>>,
    /// Optional simple partition names.
    pub partitions: Vec<String>,
    /// The one optional analysis target selector.
    pub target: AnalyzeTarget,
    /// Ordered `WITH` options in the accepted current subset.
    pub options: Vec<AnalyzeOption>,
}

/// Mutually exclusive analysis target grammar after the table/partition list.
#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzeTarget {
    /// No explicit selector: Go's default analysis behavior.
    Default,
    /// `INDEX` with its optional list of names (an empty list is valid).
    Index(Vec<String>),
    /// `ALL COLUMNS`.
    AllColumns,
    /// `COLUMNS name [, ...]`.
    Columns(Vec<String>),
}

/// A source-backed `ANALYZE TABLE ... WITH` option.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeOption {
    /// The exact accepted numeric literal spelling.
    pub value: String,
    /// Whether this requests `TOPN` or `BUCKETS`.
    pub kind: AnalyzeOptionKind,
}

/// `WITH` option kinds present in the current integration parser inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzeOptionKind {
    /// `TOPN`.
    TopN,
    /// `BUCKETS`.
    Buckets,
}

impl AnalyzeTableStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ANALYZE TABLE ");
        for (i, table) in self.tables.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            push_name_path(out, table);
        }
        if !self.partitions.is_empty() {
            out.push_str(" PARTITION ");
            for (i, partition) in self.partitions.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&back_quote(partition));
            }
        }
        match &self.target {
            AnalyzeTarget::Default => {}
            AnalyzeTarget::Index(indexes) => {
                out.push_str(" INDEX");
                if !indexes.is_empty() {
                    out.push(' ');
                    for (i, index) in indexes.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        out.push_str(&back_quote(index));
                    }
                }
            }
            AnalyzeTarget::AllColumns => out.push_str(" ALL COLUMNS"),
            AnalyzeTarget::Columns(columns) => {
                out.push_str(" COLUMNS ");
                for (i, column) in columns.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&back_quote(column));
                }
            }
        }
        if !self.options.is_empty() {
            out.push_str(" WITH ");
            for (i, option) in self.options.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&option.value);
                out.push(' ');
                out.push_str(match option.kind {
                    AnalyzeOptionKind::TopN => "TOPN",
                    AnalyzeOptionKind::Buckets => "BUCKETS",
                });
            }
        }
    }
}
