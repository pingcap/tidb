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

use crate::util::{back_quote, push_name_path};

/// `ANALYZE INCREMENTAL TABLE` preserves whether analysis is directed at all
/// table statistics or an explicit partition set.
#[derive(Debug, Clone, PartialEq)]
pub enum AnalyzeIncrementalTarget {
    /// `TABLE table [, table ...]` without a partition restriction.
    Tables(Vec<Vec<String>>),
    /// `TABLE table [, table ...] PARTITION partition [, partition ...]`.
    Partitions {
        /// Table-name paths in source order.
        tables: Vec<Vec<String>>,
        /// Simple partition names in source order.
        partitions: Vec<String>,
    },
}

/// The source-backed incremental-analysis subset.
///
/// `indexes` is `None` when no `INDEX` clause was written, and `Some(vec![])`
/// for the valid, explicitly written empty `INDEX` selector.
#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeIncrementalStmt {
    /// The table or partition statistics target.
    pub target: AnalyzeIncrementalTarget,
    /// Optional `INDEX [name [, name ...]]` selector.
    pub indexes: Option<Vec<String>>,
}

impl AnalyzeIncrementalStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ANALYZE INCREMENTAL TABLE ");
        let tables = match &self.target {
            AnalyzeIncrementalTarget::Tables(tables) => tables,
            AnalyzeIncrementalTarget::Partitions { tables, .. } => tables,
        };
        for (i, table) in tables.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            push_name_path(out, table);
        }
        if let AnalyzeIncrementalTarget::Partitions { partitions, .. } = &self.target {
            out.push_str(" PARTITION ");
            for (i, partition) in partitions.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&back_quote(partition));
            }
        }
        if let Some(indexes) = &self.indexes {
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
    }
}
