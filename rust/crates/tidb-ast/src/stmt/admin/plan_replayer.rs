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

//! `PLAN REPLAYER`, mirroring Go's `PlanReplayerStmt` in
//! `pkg/parser/ast/misc.go`.

use super::*;

/// Complete parser-visible `PLAN REPLAYER` operation.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanReplayerStmt {
    /// `PLAN REPLAYER LOAD 'file'`.
    Load(String),
    /// `PLAN REPLAYER CAPTURE [REMOVE] 'sql-digest' 'plan-digest'`.
    Capture {
        /// Whether the capture is removed rather than added.
        remove: bool,
        /// SQL digest.
        sql_digest: String,
        /// Plan digest.
        plan_digest: String,
    },
    /// `PLAN REPLAYER [DUMP] [WITH STATS ...] EXPLAIN [ANALYZE] target`.
    Dump {
        /// Optional historical-statistics timestamp expression.
        historical_stats: Option<Box<Expr>>,
        /// Whether the target is executed while collecting the replay.
        analyze: bool,
        /// Dump target.
        target: Box<PlanReplayerTarget>,
    },
}

/// Target carried by a Plan Replayer dump.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanReplayerTarget {
    /// An ordinary parsed statement.
    Statement(Box<Stmt>),
    /// A file containing SQL.
    File(String),
    /// One or more literal SQL statements.
    Statements(Vec<String>),
    /// The special slow-query selector.
    SlowQuery {
        /// Optional predicate.
        where_clause: Option<Box<Expr>>,
        /// Optional ordering.
        order_by: Vec<crate::OrderItem>,
        /// Optional row limit.
        limit: Option<Box<crate::Limit>>,
    },
}

impl PlanReplayerStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Load(file) => {
                out.push_str("PLAN REPLAYER LOAD '");
                out.push_str(&escape_string_literal(file));
                out.push('\'');
            }
            Self::Capture {
                remove,
                sql_digest,
                plan_digest,
            } => {
                out.push_str("PLAN REPLAYER CAPTURE ");
                if *remove {
                    out.push_str("REMOVE ");
                }
                out.push('\'');
                out.push_str(&escape_string_literal(sql_digest));
                out.push_str("' '");
                out.push_str(&escape_string_literal(plan_digest));
                out.push('\'');
            }
            Self::Dump {
                historical_stats,
                analyze,
                target,
            } => {
                out.push_str("PLAN REPLAYER DUMP ");
                if let Some(timestamp) = historical_stats {
                    out.push_str("WITH STATS AS OF TIMESTAMP ");
                    timestamp.restore_into(out);
                    out.push(' ');
                }
                out.push_str(if *analyze {
                    "EXPLAIN ANALYZE "
                } else {
                    "EXPLAIN "
                });
                target.restore_into(out);
            }
        }
    }
}

impl PlanReplayerTarget {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Statement(statement) => statement.restore_into(out),
            Self::File(file) => {
                out.push('\'');
                out.push_str(&escape_string_literal(file));
                out.push('\'');
            }
            Self::Statements(statements) => {
                out.push('(');
                for (index, statement) in statements.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    out.push('\'');
                    out.push_str(&escape_string_literal(statement));
                    out.push('\'');
                }
                out.push(')');
            }
            Self::SlowQuery {
                where_clause,
                order_by,
                limit,
            } => {
                out.push_str("SLOW QUERY");
                if let Some(where_clause) = where_clause {
                    out.push_str(" WHERE ");
                    where_clause.restore_into(out);
                }
                if !order_by.is_empty() {
                    out.push_str(" ORDER BY ");
                    for (index, item) in order_by.iter().enumerate() {
                        if index > 0 {
                            out.push(',');
                        }
                        item.restore_into(out);
                    }
                }
                if let Some(limit) = limit {
                    out.push_str(" LIMIT ");
                    if let Some(offset) = &limit.offset {
                        offset.restore_into(out);
                        out.push(',');
                    }
                    limit.count.restore_into(out);
                }
            }
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for PlanReplayerStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Load(field_0) => {
                let _ = field_0;
            }
            Self::Capture {
                remove,
                sql_digest,
                plan_digest,
            } => {
                let _ = remove;
                let _ = sql_digest;
                let _ = plan_digest;
            }
            Self::Dump {
                historical_stats,
                analyze,
                target,
            } => {
                if let Some(value) = historical_stats.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                if !crate::Visitable::accept(target.as_mut(), visitor) {
                    return false;
                }
                let _ = historical_stats;
                let _ = analyze;
                let _ = target;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for PlanReplayerTarget {
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
            Self::File(field_0) => {
                let _ = field_0;
            }
            Self::Statements(field_0) => {
                let _ = field_0;
            }
            Self::SlowQuery {
                where_clause,
                order_by,
                limit,
            } => {
                if let Some(value) = where_clause.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                for value in order_by.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                if let Some(value) = limit.as_mut() {
                    if !crate::Visitable::accept(value.as_mut(), visitor) {
                        return false;
                    }
                }
                let _ = where_clause;
                let _ = order_by;
                let _ = limit;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
