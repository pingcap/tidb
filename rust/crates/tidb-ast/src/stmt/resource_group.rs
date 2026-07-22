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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Typed payload and canonical restore for `CREATE`, `ALTER`, and `DROP
//! RESOURCE GROUP`, owned by Go's `ddl_resource_group_parser.go` source
//! domain.

use crate::util::{back_quote, escape_string_literal};
use crate::Expr;

/// A `CREATE RESOURCE GROUP` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateResourceGroupStmt {
    /// Whether duplicate-group errors are suppressed.
    pub if_not_exists: bool,
    /// The resource-group name.
    pub name: String,
    /// Source-ordered admission-control options.
    pub options: Vec<ResourceGroupOption>,
}

/// An `ALTER RESOURCE GROUP` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterResourceGroupStmt {
    /// Whether missing-group errors are suppressed.
    pub if_exists: bool,
    /// The resource-group name.
    pub name: String,
    /// Source-ordered replacement admission-control options.
    pub options: Vec<ResourceGroupOption>,
}

/// A `DROP RESOURCE GROUP` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropResourceGroupStmt {
    /// Whether missing-group errors are suppressed.
    pub if_exists: bool,
    /// The resource-group name.
    pub name: String,
}

/// One source-visible resource-group option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceGroupOption {
    /// `RU_PER_SEC = n` or `RU_PER_SEC = UNLIMITED`.
    RuPerSec(ResourceGroupRate),
    /// `PRIORITY = LOW|MEDIUM|HIGH`.
    Priority(ResourceGroupPriority),
    /// `BURSTABLE` or `BURSTABLE = UNLIMITED|MODERATED|OFF`.
    Burstable(ResourceGroupBurstable),
    /// `QUERY_LIMIT = NULL|(<runaway option> ...)`.
    QueryLimit(Vec<ResourceGroupRunawayOption>),
    /// `BACKGROUND = NULL|(<background option>, ...)`.
    Background(Vec<ResourceGroupBackgroundOption>),
}

/// The rate limit in a resource-group `RU_PER_SEC` option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceGroupRate {
    /// A finite request-unit rate.
    Limited(u64),
    /// TiDB's no-limit sentinel.
    Unlimited,
}

/// The scheduling priority in a resource-group option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceGroupPriority {
    /// Low priority.
    Low,
    /// Medium priority.
    Medium,
    /// High priority.
    High,
}

/// The burst policy in a resource-group option.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceGroupBurstable {
    /// Additional RU is not bounded.
    Unlimited,
    /// The bare `BURSTABLE` default and explicit moderated policy.
    Moderated,
    /// Bursting is disabled.
    Off,
}

/// One `QUERY_LIMIT` sub-option. Multiple rule variants may coexist, while
/// the grammar permits at most one action and one watch option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceGroupRunawayOption {
    /// A threshold that identifies runaway queries.
    Rule(ResourceGroupRunawayRule),
    /// The action taken after a threshold matches.
    Action(ResourceGroupRunawayAction),
    /// The query-watch matching mode and lifetime.
    Watch(ResourceGroupRunawayWatch),
}

/// A runaway-query threshold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceGroupRunawayRule {
    /// `EXEC_ELAPSED = '<duration>'`.
    ExecElapsed(String),
    /// `PROCESSED_KEYS = n`.
    ProcessedKeys(i64),
    /// `RU = n`.
    RequestUnit(i64),
}

/// The action applied to a runaway query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceGroupRunawayAction {
    /// Record the runaway query without terminating it.
    DryRun,
    /// Throttle the runaway query.
    Cooldown,
    /// Terminate the runaway query.
    Kill,
    /// Move the query into another resource group.
    SwitchGroup(String),
}

/// A `WATCH` option. `None` is Go's empty duration sentinel and restores as
/// `UNLIMITED`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceGroupRunawayWatch {
    /// The matching mode.
    pub watch_type: crate::RunawayWatchType,
    /// A finite duration, or `None` for unlimited.
    pub duration: Option<String>,
}

/// `QUERY WATCH ADD` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct AddQueryWatchStmt {
    /// Options in source order. Go rejects duplicate option families.
    pub options: Vec<QueryWatchOption>,
}

/// `QUERY WATCH REMOVE` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropQueryWatchStmt {
    /// Numeric watch ID or resource-group target.
    pub target: QueryWatchRemoveTarget,
}

/// Target accepted by `QUERY WATCH REMOVE`.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryWatchRemoveTarget {
    /// Numeric watch ID.
    Id(i64),
    /// Static resource-group name.
    ResourceGroup(String),
    /// Resource-group expression, such as a user variable.
    ResourceGroupExpr(Expr),
}

/// One option of `QUERY WATCH ADD`.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryWatchOption {
    /// Static resource-group name.
    ResourceGroup(String),
    /// Resource-group expression, such as a user variable.
    ResourceGroupExpr(Expr),
    /// Action to apply when the watch matches.
    Action(ResourceGroupRunawayAction),
    /// SQL text or digest matching rule.
    Text(QueryWatchTextOption),
}

/// SQL text or digest matching rule for `QUERY WATCH ADD`.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryWatchTextOption {
    /// Exact, similar, or plan matching.
    pub watch_type: crate::RunawayWatchType,
    /// Pattern expression.
    pub pattern: Expr,
    /// Whether source used `SQL TEXT <type> TO` instead of a digest form.
    pub type_specified: bool,
}

/// One `BACKGROUND` sub-option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceGroupBackgroundOption {
    /// `TASK_TYPES = '<comma-separated task names>'`.
    TaskTypes(String),
    /// `UTILIZATION_LIMIT = n`.
    UtilizationLimit(u64),
}

impl CreateResourceGroupStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE RESOURCE GROUP ");
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
        restore_options(out, &self.options);
    }
}

impl AlterResourceGroupStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER RESOURCE GROUP ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
        restore_options(out, &self.options);
    }
}

impl DropResourceGroupStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP RESOURCE GROUP ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
    }
}

fn restore_options(out: &mut String, options: &[ResourceGroupOption]) {
    for (index, option) in options.iter().enumerate() {
        out.push_str(if index == 0 { " " } else { ", " });
        option.restore_into(out);
    }
}

impl ResourceGroupOption {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::RuPerSec(ResourceGroupRate::Limited(rate)) => {
                out.push_str("RU_PER_SEC = ");
                out.push_str(&rate.to_string());
            }
            Self::RuPerSec(ResourceGroupRate::Unlimited) => out.push_str("RU_PER_SEC = UNLIMITED"),
            Self::Priority(priority) => {
                out.push_str("PRIORITY = ");
                out.push_str(match priority {
                    ResourceGroupPriority::Low => "LOW",
                    ResourceGroupPriority::Medium => "MEDIUM",
                    ResourceGroupPriority::High => "HIGH",
                });
            }
            Self::Burstable(policy) => {
                out.push_str("BURSTABLE = ");
                out.push_str(match policy {
                    ResourceGroupBurstable::Unlimited => "UNLIMITED",
                    ResourceGroupBurstable::Moderated => "MODERATED",
                    ResourceGroupBurstable::Off => "OFF",
                });
            }
            Self::QueryLimit(options) => {
                out.push_str("QUERY_LIMIT = ");
                if options.is_empty() {
                    out.push_str("NULL");
                } else {
                    out.push('(');
                    for (index, option) in options.iter().enumerate() {
                        if index > 0 {
                            out.push(' ');
                        }
                        option.restore_into(out);
                    }
                    out.push(')');
                }
            }
            Self::Background(options) => {
                out.push_str("BACKGROUND = ");
                if options.is_empty() {
                    out.push_str("NULL");
                } else {
                    out.push('(');
                    for (index, option) in options.iter().enumerate() {
                        if index > 0 {
                            out.push_str(", ");
                        }
                        option.restore_into(out);
                    }
                    out.push(')');
                }
            }
        }
    }
}

impl ResourceGroupRunawayOption {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Rule(rule) => rule.restore_into(out),
            Self::Action(action) => action.restore_into(out),
            Self::Watch(watch) => watch.restore_into(out),
        }
    }
}

impl ResourceGroupRunawayRule {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::ExecElapsed(duration) => {
                out.push_str("EXEC_ELAPSED = '");
                out.push_str(&escape_string_literal(duration));
                out.push('\'');
            }
            Self::ProcessedKeys(keys) => {
                out.push_str("PROCESSED_KEYS = ");
                out.push_str(&keys.to_string());
            }
            Self::RequestUnit(request_units) => {
                out.push_str("RU = ");
                out.push_str(&request_units.to_string());
            }
        }
    }
}

impl ResourceGroupRunawayAction {
    fn restore_into(&self, out: &mut String) {
        out.push_str("ACTION = ");
        match self {
            Self::DryRun => out.push_str("DRYRUN"),
            Self::Cooldown => out.push_str("COOLDOWN"),
            Self::Kill => out.push_str("KILL"),
            Self::SwitchGroup(name) => {
                out.push_str("SWITCH_GROUP(");
                out.push_str(&back_quote(name));
                out.push(')');
            }
        }
    }
}

impl ResourceGroupRunawayWatch {
    fn restore_into(&self, out: &mut String) {
        out.push_str("WATCH = ");
        out.push_str(self.watch_type.sql());
        out.push_str(" DURATION = ");
        if let Some(duration) = &self.duration {
            out.push('\'');
            out.push_str(&escape_string_literal(duration));
            out.push('\'');
        } else {
            out.push_str("UNLIMITED");
        }
    }
}

impl AddQueryWatchStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("QUERY WATCH ADD");
        for option in &self.options {
            out.push(' ');
            option.restore_into(out);
        }
    }
}

impl DropQueryWatchStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("QUERY WATCH REMOVE ");
        match &self.target {
            QueryWatchRemoveTarget::Id(id) => out.push_str(&id.to_string()),
            QueryWatchRemoveTarget::ResourceGroup(name) => {
                out.push_str("RESOURCE GROUP ");
                out.push_str(&back_quote(name));
            }
            QueryWatchRemoveTarget::ResourceGroupExpr(expr) => {
                out.push_str("RESOURCE GROUP ");
                expr.restore_into(out);
            }
        }
    }
}

impl QueryWatchOption {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::ResourceGroup(name) => {
                out.push_str("RESOURCE GROUP ");
                out.push_str(&back_quote(name));
            }
            Self::ResourceGroupExpr(expr) => {
                out.push_str("RESOURCE GROUP ");
                expr.restore_into(out);
            }
            Self::Action(action) => action.restore_into(out),
            Self::Text(text) => text.restore_into(out),
        }
    }
}

impl QueryWatchTextOption {
    fn restore_into(&self, out: &mut String) {
        if self.type_specified {
            out.push_str("SQL TEXT ");
            out.push_str(self.watch_type.sql());
            out.push_str(" TO ");
        } else {
            out.push_str(match self.watch_type {
                crate::RunawayWatchType::Similar => "SQL DIGEST ",
                crate::RunawayWatchType::Plan => "PLAN DIGEST ",
                _ => "",
            });
        }
        self.pattern.restore_into(out);
    }
}

impl ResourceGroupBackgroundOption {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::TaskTypes(tasks) => {
                out.push_str("TASK_TYPES = '");
                out.push_str(&escape_string_literal(tasks));
                out.push('\'');
            }
            Self::UtilizationLimit(limit) => {
                out.push_str("UTILIZATION_LIMIT = ");
                out.push_str(&limit.to_string());
            }
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for CreateResourceGroupStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            if_not_exists,
            name,
            options,
        } = self;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = if_not_exists;
        let _ = name;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterResourceGroupStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            if_exists,
            name,
            options,
        } = self;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = if_exists;
        let _ = name;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropResourceGroupStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { if_exists, name } = self;
        let _ = if_exists;
        let _ = name;
        visitor.leave(self)
    }
}

impl crate::Visitable for ResourceGroupOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::RuPerSec(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Priority(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Burstable(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::QueryLimit(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
            Self::Background(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ResourceGroupRate {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Limited(field_0) => {
                let _ = field_0;
            }
            Self::Unlimited => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ResourceGroupPriority {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Low => {}
            Self::Medium => {}
            Self::High => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ResourceGroupBurstable {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Unlimited => {}
            Self::Moderated => {}
            Self::Off => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ResourceGroupRunawayOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Rule(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Action(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Watch(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ResourceGroupRunawayRule {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::ExecElapsed(field_0) => {
                let _ = field_0;
            }
            Self::ProcessedKeys(field_0) => {
                let _ = field_0;
            }
            Self::RequestUnit(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ResourceGroupRunawayAction {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::DryRun => {}
            Self::Cooldown => {}
            Self::Kill => {}
            Self::SwitchGroup(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ResourceGroupRunawayWatch {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            watch_type,
            duration,
        } = self;
        if !crate::Visitable::accept(watch_type, visitor) {
            return false;
        }
        let _ = watch_type;
        let _ = duration;
        visitor.leave(self)
    }
}

impl crate::Visitable for AddQueryWatchStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { options } = self;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropQueryWatchStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { target } = self;
        if !crate::Visitable::accept(target, visitor) {
            return false;
        }
        let _ = target;
        visitor.leave(self)
    }
}

impl crate::Visitable for QueryWatchRemoveTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Id(field_0) => {
                let _ = field_0;
            }
            Self::ResourceGroup(field_0) => {
                let _ = field_0;
            }
            Self::ResourceGroupExpr(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for QueryWatchOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::ResourceGroup(field_0) => {
                let _ = field_0;
            }
            Self::ResourceGroupExpr(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Action(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Text(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for QueryWatchTextOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            watch_type,
            pattern,
            type_specified,
        } = self;
        if !crate::Visitable::accept(watch_type, visitor) {
            return false;
        }
        if !crate::Visitable::accept(pattern, visitor) {
            return false;
        }
        let _ = watch_type;
        let _ = pattern;
        let _ = type_specified;
        visitor.leave(self)
    }
}

impl crate::Visitable for ResourceGroupBackgroundOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::TaskTypes(field_0) => {
                let _ = field_0;
            }
            Self::UtilizationLimit(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
