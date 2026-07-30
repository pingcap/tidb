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

//! `ADMIN CHECK` / `CHECKSUM` / `RECOVER INDEX` / `CLEANUP TABLE LOCK`: the
//! consistency-repair payloads, mirroring Go's `CleanupTableLockStmt` and the
//! admin check statements in `pkg/parser/ast/misc.go`.

use super::*;

/// The two `ADMIN CHECK` forms that share a prefix but have different
/// physical-index contracts in TiDB.
#[derive(Debug, Clone, PartialEq)]
pub enum AdminCheckStmt {
    /// `ADMIN CHECK TABLE table [, table ...]`.
    ///
    /// Go's parser permits a list even though its planner later rejects more
    /// than one table for the actual consistency-check operation.
    Table {
        /// Table-name paths in source order.
        tables: Vec<Vec<String>>,
    },
    /// `ADMIN CHECK INDEX table index [(begin, end), ...]`.
    Index {
        /// Checked table's dotted name path.
        table: Vec<String>,
        /// Parsed index identifier, restored as a bare name by Go.
        index: String,
        /// Optional half-open handle intervals.
        handle_ranges: Vec<AdminCheckHandleRange>,
    },
}

/// One half-open handle range attached to [`AdminCheckStmt::Index`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminCheckHandleRange {
    /// Inclusive lower handle bound.
    pub begin: i64,
    /// Exclusive upper handle bound.
    pub end: i64,
}

impl AdminCheckStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Table { tables } => {
                out.push_str("ADMIN CHECK TABLE ");
                for (i, table) in tables.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    push_name_path(out, table);
                }
            }
            Self::Index {
                table,
                index,
                handle_ranges,
            } => {
                out.push_str("ADMIN CHECK INDEX ");
                push_name_path(out, table);
                // Go restores AdminStmt.Index as a bare identifier even when
                // the input index name used backticks.
                out.push(' ');
                out.push_str(index);
                for (i, range) in handle_ranges.iter().enumerate() {
                    if i == 0 {
                        out.push(' ');
                    } else {
                        out.push_str(", ");
                    }
                    out.push('(');
                    out.push_str(&range.begin.to_string());
                    out.push(',');
                    out.push_str(&range.end.to_string());
                    out.push(')');
                }
            }
        }
    }
}

/// Go's `AdminChecksumTable` payload. This is deliberately separate from
/// [`AdminCheckStmt::Table`]: a checksum scans TiKV key ranges and returns
/// aggregate CRC/KV/byte rows, while an admin check validates index records.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminChecksumStmt {
    /// Table-name paths in source order.
    pub tables: Vec<Vec<String>>,
}

impl AdminChecksumStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN CHECKSUM TABLE ");
        for (index, table) in self.tables.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            push_name_path(out, table);
        }
    }
}

/// Go's `AdminRecoverIndex` payload. Recovery is separate from `ADMIN CHECK
/// INDEX`: it backfills a corrupted secondary index and returns recovery
/// counts instead of validating existing key/value records.
#[derive(Debug, Clone, PartialEq)]
pub struct AdminRecoverIndexStmt {
    /// Recovered table's dotted name path.
    pub table: Vec<String>,
    /// Index identifier, restored bare by Go's AST.
    pub index: String,
}

/// Go's `CleanupTableLockStmt` payload for stale table-lock cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminCleanupTableLockStmt {
    /// Table-name paths in source order.
    pub tables: Vec<Vec<String>>,
}

impl AdminCleanupTableLockStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN CLEANUP TABLE LOCK ");
        for (index, table) in self.tables.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            push_name_path(out, table);
        }
    }
}

impl AdminRecoverIndexStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN RECOVER INDEX ");
        push_name_path(out, &self.table);
        out.push(' ');
        out.push_str(&self.index);
    }

    pub(crate) fn restore_cleanup_into(&self, out: &mut String) {
        out.push_str("ADMIN CLEANUP INDEX ");
        push_name_path(out, &self.table);
        out.push(' ');
        out.push_str(&self.index);
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AdminCheckStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Table { tables } => {
                let _ = tables;
            }
            Self::Index {
                table,
                index,
                handle_ranges,
            } => {
                for value in handle_ranges.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = table;
                let _ = index;
                let _ = handle_ranges;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminCheckHandleRange {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { begin, end } = self;
        let _ = begin;
        let _ = end;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminChecksumStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { tables } = self;
        let _ = tables;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminRecoverIndexStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { table, index } = self;
        let _ = table;
        let _ = index;
        visitor.leave(self)
    }
}

impl crate::Visitable for AdminCleanupTableLockStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { tables } = self;
        let _ = tables;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
