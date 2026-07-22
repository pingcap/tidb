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

//! Remaining statement families transcreated from `pkg/parser/ast/misc.go`.

#![allow(missing_docs)]

use crate::util::{back_quote, escape_string_literal, push_name_path};
use crate::{Expr, Stmt};

/// `TRACE [FORMAT = ... | PLAN [TARGET = ...]] statement`.
#[derive(Debug, Clone, PartialEq)]
pub struct TraceStmt {
    pub format: String,
    pub trace_plan: bool,
    pub trace_plan_target: String,
    pub statement: Box<Stmt>,
}

/// `EXPLAIN FORMAT = ... FOR CONNECTION id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainForStmt {
    pub format: String,
    pub connection_id: u64,
}

/// Internal `BINLOG 'payload'` command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinlogStmt {
    pub value: String,
}

/// `KILL [TIDB] [QUERY|CONNECTION] target`.
#[derive(Debug, Clone, PartialEq)]
pub struct KillStmt {
    pub query: bool,
    pub tidb_extension: bool,
    pub target: KillTarget,
}

/// KILL target representation.
#[derive(Debug, Clone, PartialEq)]
pub enum KillTarget {
    ConnectionId(u64),
    Expr(Expr),
}

/// `SET CONFIG type-or-instance name = value`.
#[derive(Debug, Clone, PartialEq)]
pub struct SetConfigStmt {
    pub target: SetConfigTarget,
    pub name: String,
    pub value: Expr,
}

/// Cluster component or instance selected by `SET CONFIG`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetConfigTarget {
    Component(String),
    Instance(String),
}

/// One recommendation option.
#[derive(Debug, Clone, PartialEq)]
pub struct RecommendIndexOption {
    pub name: String,
    pub value: Expr,
}

/// `RECOMMEND INDEX` action.
#[derive(Debug, Clone, PartialEq)]
pub enum RecommendIndexStmt {
    Run {
        sql: Option<String>,
        options: Vec<RecommendIndexOption>,
    },
    ShowOption,
    Apply(i64),
    Ignore(i64),
    Set(Vec<RecommendIndexOption>),
    Status,
    Cancel,
}

/// Extended-statistics kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtendedStatsType {
    Cardinality,
    Dependency,
    Correlation,
}

/// `CREATE STATISTICS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStatisticsStmt {
    pub if_not_exists: bool,
    pub name: String,
    pub stats_type: ExtendedStatsType,
    pub table: Vec<String>,
    pub columns: Vec<Vec<String>>,
}

/// Server-control and help statements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerControlStmt {
    Shutdown,
    Restart,
    Help(String),
}

/// Static calibration workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CalibrateWorkload {
    Tpcc,
    OltpReadWrite,
    OltpReadOnly,
    OltpWriteOnly,
    Tpch10,
}

/// One dynamic `CALIBRATE RESOURCE` option.
#[derive(Debug, Clone, PartialEq)]
pub enum CalibrateResourceOption {
    StartTime(Expr),
    EndTime(Expr),
    DurationString(String),
    DurationInterval { value: Expr, unit: String },
    Duration(Expr),
}

/// `CALIBRATE RESOURCE` payload.
#[derive(Debug, Clone, PartialEq)]
pub struct CalibrateResourceStmt {
    pub workload: Option<CalibrateWorkload>,
    pub options: Vec<CalibrateResourceOption>,
}

impl TraceStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("TRACE ");
        if self.trace_plan {
            out.push_str("PLAN ");
            if !self.trace_plan_target.is_empty() {
                out.push_str("TARGET = '");
                out.push_str(&escape_string_literal(&self.trace_plan_target));
                out.push_str("' ");
            }
        } else if self.format != "row" {
            out.push_str("FORMAT = '");
            out.push_str(&escape_string_literal(&self.format));
            out.push_str("' ");
        }
        self.statement.restore_into(out);
    }
}

impl ExplainForStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("EXPLAIN FORMAT = '");
        out.push_str(&escape_string_literal(&self.format));
        out.push_str("' FOR CONNECTION ");
        out.push_str(&self.connection_id.to_string());
    }
}

impl BinlogStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("BINLOG '");
        out.push_str(&escape_string_literal(&self.value));
        out.push('\'');
    }
}

impl KillStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("KILL");
        if self.tidb_extension {
            out.push_str(" TIDB");
        }
        if self.query {
            out.push_str(" QUERY");
        }
        out.push(' ');
        match &self.target {
            KillTarget::ConnectionId(id) => out.push_str(&id.to_string()),
            KillTarget::Expr(expr) => expr.restore_into(out),
        }
    }
}

impl SetConfigStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SET CONFIG ");
        match &self.target {
            SetConfigTarget::Component(component) => out.push_str(&component.to_ascii_uppercase()),
            SetConfigTarget::Instance(instance) => {
                out.push('\'');
                out.push_str(&escape_string_literal(instance));
                out.push('\'');
            }
        }
        out.push(' ');
        out.push_str(&self.name.to_ascii_uppercase());
        out.push_str(" = ");
        self.value.restore_into(out);
    }
}

impl RecommendIndexOption {
    fn restore_into(&self, out: &mut String) {
        out.push_str(&self.name.to_ascii_uppercase());
        out.push_str(" = ");
        self.value.restore_into(out);
    }
}

impl RecommendIndexStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("RECOMMEND INDEX");
        match self {
            Self::Run { sql, options } => {
                out.push_str(" RUN");
                if let Some(sql) = sql {
                    out.push_str(" FOR '");
                    out.push_str(&escape_string_literal(sql));
                    out.push('\'');
                }
                restore_recommend_options(out, " WITH ", options);
            }
            Self::ShowOption => out.push_str(" SHOW OPTION"),
            Self::Apply(id) => out.push_str(&format!(" APPLY {id}")),
            Self::Ignore(id) => out.push_str(&format!(" IGNORE {id}")),
            Self::Set(options) => restore_recommend_options(out, " SET ", options),
            Self::Status => out.push_str(" STATUS"),
            Self::Cancel => out.push_str(" CANCEL"),
        }
    }
}

fn restore_recommend_options(out: &mut String, prefix: &str, options: &[RecommendIndexOption]) {
    if options.is_empty() {
        return;
    }
    out.push_str(prefix);
    for (index, option) in options.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        option.restore_into(out);
    }
}

impl CreateStatisticsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE STATISTICS ");
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
        out.push_str(match self.stats_type {
            ExtendedStatsType::Cardinality => " (CARDINALITY) ON ",
            ExtendedStatsType::Dependency => " (DEPENDENCY) ON ",
            ExtendedStatsType::Correlation => " (CORRELATION) ON ",
        });
        push_name_path(out, &self.table);
        out.push('(');
        for (index, column) in self.columns.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            push_name_path(out, column);
        }
        out.push(')');
    }
}

impl ServerControlStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Shutdown => out.push_str("SHUTDOWN"),
            Self::Restart => out.push_str("RESTART"),
            Self::Help(topic) => {
                out.push_str("HELP '");
                out.push_str(&escape_string_literal(topic));
                out.push('\'');
            }
        }
    }
}

impl CalibrateResourceStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CALIBRATE RESOURCE");
        if let Some(workload) = self.workload {
            out.push_str(" WORKLOAD ");
            out.push_str(match workload {
                CalibrateWorkload::Tpcc => "TPCC",
                CalibrateWorkload::OltpReadWrite => "OLTP_READ_WRITE",
                CalibrateWorkload::OltpReadOnly => "OLTP_READ_ONLY",
                CalibrateWorkload::OltpWriteOnly => "OLTP_WRITE_ONLY",
                CalibrateWorkload::Tpch10 => "TPCH_10",
            });
        }
        for option in &self.options {
            out.push(' ');
            match option {
                CalibrateResourceOption::StartTime(expr) => {
                    out.push_str("START_TIME ");
                    expr.restore_into(out);
                }
                CalibrateResourceOption::EndTime(expr) => {
                    out.push_str("END_TIME ");
                    expr.restore_into(out);
                }
                CalibrateResourceOption::DurationString(value) => {
                    out.push_str("DURATION '");
                    out.push_str(&escape_string_literal(value));
                    out.push('\'');
                }
                CalibrateResourceOption::DurationInterval { value, unit } => {
                    out.push_str("DURATION INTERVAL ");
                    value.restore_into(out);
                    out.push(' ');
                    out.push_str(&unit.to_ascii_uppercase());
                }
                CalibrateResourceOption::Duration(expr) => {
                    out.push_str("DURATION ");
                    expr.restore_into(out);
                }
            }
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for TraceStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            format,
            trace_plan,
            trace_plan_target,
            statement,
        } = self;
        if !crate::Visitable::accept(statement.as_mut(), visitor) {
            return false;
        }
        let _ = format;
        let _ = trace_plan;
        let _ = trace_plan_target;
        let _ = statement;
        visitor.leave(self)
    }
}

impl crate::Visitable for ExplainForStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            format,
            connection_id,
        } = self;
        let _ = format;
        let _ = connection_id;
        visitor.leave(self)
    }
}

impl crate::Visitable for BinlogStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { value } = self;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for KillStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            query,
            tidb_extension,
            target,
        } = self;
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        let _ = query;
        let _ = tidb_extension;
        let _ = target;
        visitor.leave(self)
    }
}

impl crate::Visitable for KillTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::ConnectionId(field_0) => {
                let _ = field_0;
            }
            Self::Expr(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SetConfigStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            target,
            name,
            value,
        } = self;
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        if !crate::Visitable::accept(value, visitor) {
            return false;
        }
        let _ = target;
        let _ = name;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for SetConfigTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Component(field_0) => {
                let _ = field_0;
            }
            Self::Instance(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for RecommendIndexOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, value } = self;
        if !crate::Visitable::accept(value, visitor) {
            return false;
        }
        let _ = name;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for RecommendIndexStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Run { sql, options } => {
                for value in options.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = sql;
                let _ = options;
            }
            Self::ShowOption => {}
            Self::Apply(field_0) => {
                let _ = field_0;
            }
            Self::Ignore(field_0) => {
                let _ = field_0;
            }
            Self::Set(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
            Self::Status => {}
            Self::Cancel => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ExtendedStatsType {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Cardinality => {}
            Self::Dependency => {}
            Self::Correlation => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateStatisticsStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            if_not_exists,
            name,
            stats_type,
            table,
            columns,
        } = self;
        if !crate::Visitable::accept(stats_type, visitor) {
            return false;
        }
        let _ = if_not_exists;
        let _ = name;
        let _ = stats_type;
        let _ = table;
        let _ = columns;
        visitor.leave(self)
    }
}

impl crate::Visitable for ServerControlStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Shutdown => {}
            Self::Restart => {}
            Self::Help(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CalibrateWorkload {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Tpcc => {}
            Self::OltpReadWrite => {}
            Self::OltpReadOnly => {}
            Self::OltpWriteOnly => {}
            Self::Tpch10 => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CalibrateResourceOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::StartTime(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::EndTime(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DurationString(field_0) => {
                let _ = field_0;
            }
            Self::DurationInterval { value, unit } => {
                if !crate::Visitable::accept(value, visitor) {
                    return false;
                }
                let _ = value;
                let _ = unit;
            }
            Self::Duration(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CalibrateResourceStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { workload, options } = self;
        if let Some(value) = workload.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = workload;
        let _ = options;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
