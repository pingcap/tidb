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

pub use incremental::AnalyzeIncrementalStmt;

/// Go's `ANALYZE TABLE` AST payload.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeTableStmt {
    /// One or more dotted table-name paths, in source order.
    pub tables: Vec<Vec<String>>,
    /// Optional simple partition names.
    pub partitions: Vec<String>,
    /// Whether `NO_WRITE_TO_BINLOG` or its `LOCAL` alias was specified.
    pub no_write_to_binlog: bool,
    /// The one optional analysis target selector.
    pub target: AnalyzeTarget,
    /// Ordered `WITH` options.
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
    /// `PREDICATE COLUMNS`.
    PredicateColumns,
    /// `COLUMNS name [, ...]`.
    Columns(Vec<String>),
    /// `UPDATE|DROP HISTOGRAM ON name [, ...]`.
    Histogram {
        /// Whether histogram data is updated or removed.
        operation: HistogramOperation,
        /// Simple column names in source order.
        columns: Vec<String>,
    },
}

/// The histogram operation selected by `ANALYZE TABLE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramOperation {
    /// `UPDATE HISTOGRAM`.
    Update,
    /// `DROP HISTOGRAM`.
    Drop,
}

/// A source-backed `ANALYZE TABLE ... WITH` option.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeOption {
    /// The exact accepted numeric literal spelling.
    pub value: String,
    /// The requested statistics option.
    pub kind: AnalyzeOptionKind,
}

/// `WITH` option kinds present in the current integration parser inventory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalyzeOptionKind {
    /// `TOPN`.
    TopN,
    /// `BUCKETS`.
    Buckets,
    /// `CMSKETCH DEPTH`.
    CmSketchDepth,
    /// `CMSKETCH WIDTH`.
    CmSketchWidth,
    /// `SAMPLES`.
    Samples,
    /// `SAMPLERATE`.
    SampleRate,
    /// `NDVRATE`.
    NdvRate,
}

impl AnalyzeOption {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(&self.value);
        out.push(' ');
        out.push_str(match self.kind {
            AnalyzeOptionKind::TopN => "TOPN",
            AnalyzeOptionKind::Buckets => "BUCKETS",
            AnalyzeOptionKind::CmSketchDepth => "CMSKETCH DEPTH",
            AnalyzeOptionKind::CmSketchWidth => "CMSKETCH WIDTH",
            AnalyzeOptionKind::Samples => "SAMPLES",
            AnalyzeOptionKind::SampleRate => "SAMPLERATE",
            AnalyzeOptionKind::NdvRate => "NDVRATE",
        });
    }
}

impl AnalyzeTableStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ANALYZE ");
        if self.no_write_to_binlog {
            out.push_str("NO_WRITE_TO_BINLOG ");
        }
        out.push_str("TABLE ");
        restore_analyze_body(
            out,
            &self.tables,
            &self.partitions,
            &self.target,
            &self.options,
        );
    }
}

pub(crate) fn restore_analyze_body(
    out: &mut String,
    tables: &[Vec<String>],
    partitions: &[String],
    target: &AnalyzeTarget,
    options: &[AnalyzeOption],
) {
    for (i, table) in tables.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_name_path(out, table);
    }
    if !partitions.is_empty() {
        out.push_str(" PARTITION ");
        for (i, partition) in partitions.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&back_quote(partition));
        }
    }
    match target {
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
        AnalyzeTarget::PredicateColumns => out.push_str(" PREDICATE COLUMNS"),
        AnalyzeTarget::Columns(columns) => {
            out.push_str(" COLUMNS ");
            for (i, column) in columns.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&back_quote(column));
            }
        }
        AnalyzeTarget::Histogram { operation, columns } => {
            out.push_str(match operation {
                HistogramOperation::Update => " UPDATE HISTOGRAM ON ",
                HistogramOperation::Drop => " DROP HISTOGRAM ON ",
            });
            for (i, column) in columns.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&back_quote(column));
            }
        }
    }
    if !options.is_empty() {
        out.push_str(" WITH ");
        for (i, option) in options.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            option.restore_into(out);
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AnalyzeTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            tables,
            partitions,
            no_write_to_binlog,
            target,
            options,
        } = self;
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = tables;
        let _ = partitions;
        let _ = no_write_to_binlog;
        let _ = target;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for AnalyzeTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::Index(field_0) => {
                let _ = field_0;
            }
            Self::AllColumns => {}
            Self::PredicateColumns => {}
            Self::Columns(field_0) => {
                let _ = field_0;
            }
            Self::Histogram { operation, columns } => {
                if !crate::Visitable::accept(operation, visitor) {
                    return false;
                }
                let _ = operation;
                let _ = columns;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for HistogramOperation {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Update => {}
            Self::Drop => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AnalyzeOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { value, kind } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        let _ = value;
        let _ = kind;
        visitor.leave(self)
    }
}

impl crate::Visitable for AnalyzeOptionKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::TopN => {}
            Self::Buckets => {}
            Self::CmSketchDepth => {}
            Self::CmSketchWidth => {}
            Self::Samples => {}
            Self::SampleRate => {}
            Self::NdvRate => {}
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
