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

//! Complete standalone `FLUSH` payload translated from `pkg/parser/ast/misc.go`.

use crate::util::push_name_path;
use crate::StatsObject;

/// A standalone `FLUSH` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct FlushStmt {
    /// `NO_WRITE_TO_BINLOG`; `LOCAL` parses to the same state.
    pub no_write_to_binlog: bool,
    /// Flush target and its target-specific payload.
    pub target: FlushTarget,
}

/// Complete Go `FlushStmtType` target set.
#[derive(Debug, Clone, PartialEq)]
pub enum FlushTarget {
    /// `STATUS`.
    Status,
    /// `PRIVILEGES`.
    Privileges,
    /// `TABLE[S] [table [, ...]] [WITH READ LOCK]`.
    Tables {
        /// Empty means every table.
        tables: Vec<Vec<String>>,
        /// Whether `WITH READ LOCK` was present.
        read_lock: bool,
    },
    /// `TIDB PLUGINS plugin [, ...]`.
    TiDbPlugins(Vec<String>),
    /// `HOSTS`.
    Hosts,
    /// One of the log flush forms.
    Logs(FlushLogType),
    /// `CLIENT_ERRORS_SUMMARY`.
    ClientErrorsSummary,
    /// `STATS_DELTA object [, ...] [CLUSTER]`.
    StatsDelta {
        /// Table, database, or global statistics targets.
        objects: Vec<StatsObject>,
        /// Whether every TiDB node must flush its local deltas.
        cluster: bool,
    },
}

/// Go's complete `LogType` set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushLogType {
    /// `LOGS`.
    Default,
    /// `BINARY LOGS`.
    Binary,
    /// `ENGINE LOGS`.
    Engine,
    /// `ERROR LOGS`.
    Error,
    /// `GENERAL LOGS`.
    General,
    /// `SLOW LOGS`.
    Slow,
}

impl FlushStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("FLUSH ");
        if self.no_write_to_binlog {
            out.push_str("NO_WRITE_TO_BINLOG ");
        }
        match &self.target {
            FlushTarget::Status => out.push_str("STATUS"),
            FlushTarget::Privileges => out.push_str("PRIVILEGES"),
            FlushTarget::Tables { tables, read_lock } => {
                out.push_str("TABLES");
                for (index, table) in tables.iter().enumerate() {
                    out.push_str(if index == 0 { " " } else { ", " });
                    push_name_path(out, table);
                }
                if *read_lock {
                    out.push_str(" WITH READ LOCK");
                }
            }
            FlushTarget::TiDbPlugins(plugins) => {
                out.push_str("TIDB PLUGINS");
                for (index, plugin) in plugins.iter().enumerate() {
                    out.push_str(if index == 0 { " " } else { ", " });
                    out.push_str(plugin);
                }
            }
            FlushTarget::Hosts => out.push_str("HOSTS"),
            FlushTarget::Logs(log_type) => out.push_str(match log_type {
                FlushLogType::Default => "LOGS",
                FlushLogType::Binary => "BINARY LOGS",
                FlushLogType::Engine => "ENGINE LOGS",
                FlushLogType::Error => "ERROR LOGS",
                FlushLogType::General => "GENERAL LOGS",
                FlushLogType::Slow => "SLOW LOGS",
            }),
            FlushTarget::ClientErrorsSummary => out.push_str("CLIENT_ERRORS_SUMMARY"),
            FlushTarget::StatsDelta { objects, cluster } => {
                out.push_str("STATS_DELTA ");
                for (index, object) in objects.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    object.restore_into(out);
                }
                if *cluster {
                    out.push_str(" CLUSTER");
                }
            }
        }
    }

    /// Applies Go's `FlushStmt.DedupFlushObjects` shadowing rules.
    pub fn dedup_stats_objects(&mut self) {
        if let FlushTarget::StatsDelta { objects, .. } = &mut self.target {
            crate::traffic::dedup_stats_objects(objects);
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for FlushStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            no_write_to_binlog,
            target,
        } = self;
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        let _ = no_write_to_binlog;
        let _ = target;
        visitor.leave(self)
    }
}

impl crate::Visitable for FlushTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Status => {}
            Self::Privileges => {}
            Self::Tables { tables, read_lock } => {
                let _ = tables;
                let _ = read_lock;
            }
            Self::TiDbPlugins(field_0) => {
                let _ = field_0;
            }
            Self::Hosts => {}
            Self::Logs(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ClientErrorsSummary => {}
            Self::StatsDelta { objects, cluster } => {
                for value in objects.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = objects;
                let _ = cluster;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for FlushLogType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::Binary => {}
            Self::Engine => {}
            Self::Error => {}
            Self::General => {}
            Self::Slow => {}
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
