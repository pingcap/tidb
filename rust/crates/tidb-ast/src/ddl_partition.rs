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

//! Partition payloads shared by `CREATE TABLE` and `ALTER TABLE`.

use crate::{NodeBox, PartitionType};

use super::{back_quote, push_name_path, Expr, TableOption};

/// Every `ALTER TABLE` partition action belongs to this envelope. The outer
/// DDL action therefore has one stable ownership boundary while the exact Go
/// payload remains typed here.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub enum AlterPartitionAction {
    /// `PARTITION BY ...` replaces the table's partition definition. This is
    /// terminal in Go's ALTER TABLE grammar and reuses the same typed
    /// partition payload as CREATE TABLE without claiming execution support.
    Repartition(Box<TablePartitioning>),
    /// `PARTITION name ATTRIBUTES [=] {DEFAULT | 'attributes'}`. Go assigns
    /// this a dedicated `AlterTablePartitionAttributes` specification; it is
    /// intentionally not folded into the broad partition-option surface.
    SetAttributes {
        /// The partition whose attributes change.
        partition: String,
        /// Attribute text, or `None` for `ATTRIBUTES=DEFAULT`.
        attributes: Option<String>,
    },
    /// `PARTITION name table_option...`. Go stores the shared table-option
    /// list in an `AlterTablePartitionOptions` spec alongside its target.
    SetOptions {
        partition: String,
        options: Vec<TableOption>,
    },
    /// `ADD PARTITION [IF NOT EXISTS] [NO_WRITE_TO_BINLOG]` plus its payload.
    Add {
        /// Optional source guard.
        if_not_exists: bool,
        /// Legacy binlog suppression marker.
        no_write_to_binlog: bool,
        /// Count or definitions.
        spec: AddPartitionSpec,
    },
    /// `EXCHANGE PARTITION name WITH TABLE name`.
    Exchange {
        partition: String,
        table: Vec<String>,
        with_validation: bool,
    },
    /// `DROP PARTITION [IF EXISTS] names`.
    Drop { if_exists: bool, names: Vec<String> },
    /// `REORGANIZE PARTITION` with typed replacement definitions.
    Reorganize {
        no_write_to_binlog: bool,
        names: Vec<String>,
        definitions: Vec<PartitionDefinition>,
    },
    /// `COALESCE PARTITION count`.
    Coalesce {
        no_write_to_binlog: bool,
        count: u64,
    },
    /// `TRUNCATE PARTITION ALL | names`.
    Truncate { all: bool, names: Vec<String> },
    /// `CHECK PARTITION ALL | names`.
    Check { all: bool, names: Vec<String> },
    /// `IMPORT PARTITION ALL | names TABLESPACE`.
    ImportTablespace { all: bool, names: Vec<String> },
    /// `DISCARD PARTITION ALL | names TABLESPACE`.
    DiscardTablespace { all: bool, names: Vec<String> },
    /// `REMOVE PARTITIONING`.
    RemovePartitioning,
    /// `FIRST PARTITION LESS THAN (expr)`. Go stores this as the
    /// `AlterTableDropFirstPartition` specification; `IF EXISTS` is retained
    /// as execution metadata but omitted by Go's canonical restore.
    FirstPartitionLessThan {
        /// The exclusive upper bound for the first interval partition.
        expr: NodeBox<Expr>,
        /// Whether the source requested `IF EXISTS`.
        if_exists: bool,
    },
    /// `LAST PARTITION LESS THAN (expr)`. Go stores this as the
    /// `AlterTableAddLastPartition` specification and restores the optional
    /// `NO_WRITE_TO_BINLOG` marker after the bound.
    LastPartitionLessThan {
        /// The exclusive upper bound for the last interval partition.
        expr: NodeBox<Expr>,
        /// Whether the source requested `NO_WRITE_TO_BINLOG`/`LOCAL`.
        no_write_to_binlog: bool,
    },
    /// `SPLIT MAXVALUE PARTITION LESS THAN (expr)`. Go stores this as the
    /// `AlterTableReorganizeLastPartition` specification.
    SplitMaxValuePartition {
        /// The exclusive upper bound for the new partition.
        expr: NodeBox<Expr>,
    },
    /// `MERGE FIRST PARTITION LESS THAN (expr)`. Go stores this as the
    /// `AlterTableReorganizeFirstPartition` specification.
    MergeFirstPartitionLessThan {
        /// The exclusive upper bound for the merged first partition.
        expr: NodeBox<Expr>,
    },
    /// `REBUILD|OPTIMIZE|REPAIR PARTITION`.
    Maintain {
        operation: PartitionMaintenanceOp,
        no_write_to_binlog: bool,
        all: bool,
        names: Vec<String>,
    },
}

/// Restores the partition envelope with Go's canonical bytes.
pub(super) fn restore_alter_action(
    out: &mut String,
    action: &AlterPartitionAction,
    context: &crate::RestoreContext,
) {
    match action {
        AlterPartitionAction::Repartition(partitioning) => {
            partitioning.restore_after_alter_table(out, context);
        }
        AlterPartitionAction::SetAttributes {
            partition,
            attributes,
        } => {
            out.push_str("PARTITION ");
            out.push_str(&back_quote(partition));
            out.push_str(" ATTRIBUTES=");
            match attributes {
                Some(attributes) => {
                    out.push('\'');
                    out.push_str(&super::escape_string_literal(attributes));
                    out.push('\'');
                }
                None => out.push_str("DEFAULT"),
            }
        }
        AlterPartitionAction::SetOptions { partition, options } => {
            let plain_context = context.without_flags(crate::RestoreFlags::TIDB_SPECIAL_COMMENT);
            context.write_with_tidb_special_comment(out, "placement", |out| {
                out.push_str("PARTITION ");
                out.push_str(&back_quote(partition));
                for option in options {
                    out.push(' ');
                    option.restore_into_with_context(out, &plain_context);
                }
            });
        }
        AlterPartitionAction::Add {
            if_not_exists,
            no_write_to_binlog,
            spec,
        } => {
            out.push_str("ADD PARTITION");
            if *if_not_exists {
                context.write_with_tidb_special_comment(out, "", |out| {
                    out.push_str(" IF NOT EXISTS");
                });
            }
            if *no_write_to_binlog {
                out.push_str(" NO_WRITE_TO_BINLOG");
            }
            match spec {
                AddPartitionSpec::Count(count) if *count != 0 => {
                    out.push_str(" PARTITIONS ");
                    out.push_str(&count.to_string());
                }
                AddPartitionSpec::Count(_) => {}
                AddPartitionSpec::Definitions(definitions) => {
                    out.push_str(" (");
                    for (index, definition) in definitions.iter().enumerate() {
                        if index > 0 {
                            out.push_str(", ");
                        }
                        definition.restore_into_with_context(out, context);
                    }
                    out.push(')');
                }
            }
        }
        AlterPartitionAction::Exchange {
            partition,
            table,
            with_validation,
        } => {
            out.push_str("EXCHANGE PARTITION ");
            out.push_str(&back_quote(partition));
            out.push_str(" WITH TABLE ");
            push_name_path(out, table);
            if !with_validation {
                out.push_str(" WITHOUT VALIDATION");
            }
        }
        AlterPartitionAction::Drop { if_exists, names } => {
            out.push_str("DROP PARTITION ");
            if *if_exists {
                context.write_with_tidb_special_comment(out, "", |out| {
                    out.push_str("IF EXISTS ");
                });
            }
            push_partition_names(out, names);
        }
        AlterPartitionAction::Reorganize {
            no_write_to_binlog,
            names,
            definitions,
        } => {
            out.push_str("REORGANIZE PARTITION");
            if *no_write_to_binlog {
                out.push_str(" NO_WRITE_TO_BINLOG");
            }
            if !names.is_empty() {
                out.push(' ');
                push_partition_names(out, names);
                out.push_str(" INTO (");
                for (index, definition) in definitions.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    definition.restore_into_with_context(out, context);
                }
                out.push(')');
            }
        }
        AlterPartitionAction::Coalesce {
            no_write_to_binlog,
            count,
        } => {
            out.push_str("COALESCE PARTITION ");
            if *no_write_to_binlog {
                out.push_str("NO_WRITE_TO_BINLOG ");
            }
            out.push_str(&count.to_string());
        }
        AlterPartitionAction::Truncate { all, names } => {
            out.push_str("TRUNCATE PARTITION ");
            if *all {
                out.push_str("ALL");
            } else {
                push_partition_names(out, names);
            }
        }
        AlterPartitionAction::Check { all, names } => {
            out.push_str("CHECK PARTITION ");
            if *all {
                out.push_str("ALL");
            } else {
                push_partition_names(out, names);
            }
        }
        AlterPartitionAction::ImportTablespace { all, names } => {
            out.push_str("IMPORT PARTITION ");
            if *all {
                out.push_str("ALL");
            } else {
                push_partition_names(out, names);
            }
            out.push_str(" TABLESPACE");
        }
        AlterPartitionAction::DiscardTablespace { all, names } => {
            out.push_str("DISCARD PARTITION ");
            if *all {
                out.push_str("ALL");
            } else {
                push_partition_names(out, names);
            }
            out.push_str(" TABLESPACE");
        }
        AlterPartitionAction::RemovePartitioning => out.push_str("REMOVE PARTITIONING"),
        AlterPartitionAction::FirstPartitionLessThan { expr, .. } => {
            out.push_str("FIRST PARTITION LESS THAN (");
            expr.restore_into(out);
            out.push(')');
        }
        AlterPartitionAction::LastPartitionLessThan {
            expr,
            no_write_to_binlog,
        } => {
            out.push_str("LAST PARTITION LESS THAN (");
            expr.restore_into(out);
            out.push(')');
            if *no_write_to_binlog {
                out.push_str(" NO_WRITE_TO_BINLOG");
            }
        }
        AlterPartitionAction::SplitMaxValuePartition { expr } => {
            out.push_str("SPLIT MAXVALUE PARTITION LESS THAN (");
            expr.restore_into(out);
            out.push(')');
        }
        AlterPartitionAction::MergeFirstPartitionLessThan { expr } => {
            out.push_str("MERGE FIRST PARTITION LESS THAN (");
            expr.restore_into(out);
            out.push(')');
        }
        AlterPartitionAction::Maintain {
            operation,
            no_write_to_binlog,
            all,
            names,
        } => {
            out.push_str(operation.keyword());
            out.push_str(" PARTITION ");
            if *no_write_to_binlog {
                out.push_str("NO_WRITE_TO_BINLOG ");
            }
            if *all {
                out.push_str("ALL");
            } else {
                push_partition_names(out, names);
            }
        }
    }
}

/// The maintenance operations which share Go's partition-list AST fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionMaintenanceOp {
    /// `REBUILD PARTITION`.
    Rebuild,
    /// `OPTIMIZE PARTITION`.
    Optimize,
    /// `REPAIR PARTITION`.
    Repair,
}

impl PartitionMaintenanceOp {
    pub(super) fn keyword(self) -> &'static str {
        match self {
            Self::Rebuild => "REBUILD",
            Self::Optimize => "OPTIMIZE",
            Self::Repair => "REPAIR",
        }
    }
}

pub(super) fn push_partition_names(out: &mut String, names: &[String]) {
    for (index, name) in names.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&back_quote(name));
    }
}

/// The two payload forms carried by Go's `AlterTableAddPartitions` AST.
#[derive(Debug, Clone, PartialEq)]
pub enum AddPartitionSpec {
    /// `PARTITIONS n`.
    Count(u64),
    /// Parenthesized partition definitions, in written order.
    Definitions(Vec<PartitionDefinition>),
}

/// The partition method carried by `CREATE TABLE ... PARTITION BY` and its
/// optional `SUBPARTITION BY` child.  This is deliberately independent from
/// [`AlterPartitionAction`]: creation has method, count, interval and index
/// locality state which an ALTER action does not own.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionMethod {
    /// HASH, KEY, RANGE, LIST, or SYSTEM_TIME.
    pub kind: PartitionType,
    /// `LINEAR`, valid for HASH and KEY methods.
    pub linear: bool,
    /// HASH/RANGE/LIST expression, when this is expression partitioning.
    pub expr: Option<Expr>,
    /// KEY columns or RANGE/LIST `COLUMNS` name paths.
    pub columns: Vec<Vec<String>>,
    /// Optional `ALGORITHM = n` for KEY methods.
    pub key_algorithm: Option<u64>,
    /// SYSTEM_TIME `INTERVAL expr <unit>` unit, retained separately so the
    /// expression does not lose its source role.
    pub unit: Option<String>,
    /// SYSTEM_TIME `LIMIT n`.
    pub limit: u64,
    /// The requested partition/subpartition count.
    pub count: u64,
    /// RANGE interval partitioning extension.
    pub interval: Option<crate::NodeBox<PartitionInterval>>,
}

/// `RANGE ... INTERVAL (...)` creation-only syntax.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionInterval {
    /// The interval magnitude.
    pub expr: Expr,
    /// Optional time unit.
    pub unit: Option<String>,
    /// `FIRST PARTITION LESS THAN (expr)`.
    pub first_range_end: Option<Expr>,
    /// `LAST PARTITION LESS THAN (expr)`.
    pub last_range_end: Option<Expr>,
    /// `NULL PARTITION`.
    pub null_partition: bool,
    /// `MAXVALUE PARTITION`.
    pub maxvalue_partition: bool,
}

/// Full creation-side partitioning payload.  Go's `PartitionOptions` has this
/// exact ownership: a main method, optional submethod, definitions and index
/// locality changes.  Keeping it here prevents a creation parser from
/// pretending an ALTER action is an equivalent structure.
#[derive(Debug, Clone, PartialEq)]
pub struct TablePartitioning {
    /// The top-level partition method.
    pub method: PartitionMethod,
    /// Optional HASH/KEY subpartition method.
    pub subpartition: Option<PartitionMethod>,
    /// Explicit partition definitions in source order.
    pub definitions: Vec<PartitionDefinition>,
    /// `UPDATE INDEXES (idx GLOBAL|LOCAL, ...)` entries.
    pub update_indexes: Vec<PartitionIndexUpdate>,
}

/// One `UPDATE INDEXES` locality assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionIndexUpdate {
    /// Index name.
    pub name: String,
    /// `true` for GLOBAL; `false` for LOCAL.
    pub global: bool,
}

/// One source-level partition definition in `ALTER TABLE ... ADD PARTITION`.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionDefinition {
    /// The partition identifier.
    pub name: String,
    /// The typed boundary/list/default payload.
    pub clause: PartitionDefinitionClause,
    /// Source-supported per-partition options.
    pub options: Vec<TableOption>,
    /// Optional inline `(SUBPARTITION ..., ...)` definitions.  ALTER ADD
    /// shares the definition type but leaves this empty.
    pub sub_partitions: Vec<SubPartitionDefinition>,
}

/// One inline `SUBPARTITION name [table options]` definition.
#[derive(Debug, Clone, PartialEq)]
pub struct SubPartitionDefinition {
    /// Subpartition identifier.
    pub name: String,
    /// Source-order options.
    pub options: Vec<TableOption>,
}

impl PartitionDefinition {
    fn restore_into_with_context(&self, out: &mut String, context: &crate::RestoreContext) {
        out.push_str("PARTITION ");
        out.push_str(&back_quote(&self.name));
        self.clause.restore_into(out);
        for option in &self.options {
            out.push(' ');
            option.restore_into_with_context(out, context);
        }
        if !self.sub_partitions.is_empty() {
            out.push_str(" (");
            for (index, definition) in self.sub_partitions.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                definition.restore_into_with_context(out, context);
            }
            out.push(')');
        }
    }
}

impl SubPartitionDefinition {
    fn restore_into_with_context(&self, out: &mut String, context: &crate::RestoreContext) {
        out.push_str("SUBPARTITION ");
        out.push_str(&back_quote(&self.name));
        for option in &self.options {
            out.push(' ');
            option.restore_into_with_context(out, context);
        }
    }
}

impl TablePartitioning {
    pub(crate) fn restore_into_with_context(
        &self,
        out: &mut String,
        context: &crate::RestoreContext,
    ) {
        self.restore_with_prefix(out, " PARTITION BY ", context);
    }

    /// Restore this shared payload as a terminal `ALTER TABLE` action.
    fn restore_after_alter_table(&self, out: &mut String, context: &crate::RestoreContext) {
        self.restore_with_prefix(out, "PARTITION BY ", context);
    }

    fn restore_with_prefix(&self, out: &mut String, prefix: &str, context: &crate::RestoreContext) {
        out.push_str(prefix);
        self.method.restore_into(out);
        if self.method.count > 0 && self.definitions.is_empty() {
            out.push_str(" PARTITIONS ");
            out.push_str(&self.method.count.to_string());
        }
        if let Some(subpartition) = &self.subpartition {
            out.push_str(" SUBPARTITION BY ");
            subpartition.restore_into(out);
            if subpartition.count > 0 {
                out.push_str(" SUBPARTITIONS ");
                out.push_str(&subpartition.count.to_string());
            }
        }
        if !self.definitions.is_empty() {
            out.push_str(" (");
            for (index, definition) in self.definitions.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                definition.restore_into_with_context(out, context);
            }
            out.push(')');
        }
        if !self.update_indexes.is_empty() {
            out.push_str(" UPDATE INDEXES (");
            for (index, update) in self.update_indexes.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&back_quote(&update.name));
                out.push_str(if update.global { " GLOBAL" } else { " LOCAL" });
            }
            out.push(')');
        }
    }
}

impl PartitionMethod {
    fn restore_into(&self, out: &mut String) {
        if self.linear {
            out.push_str("LINEAR ");
        }
        out.push_str(self.kind.sql());
        if let Some(algorithm) = self.key_algorithm {
            out.push_str(" ALGORITHM = ");
            out.push_str(&algorithm.to_string());
        }
        if self.kind == PartitionType::SystemTime {
            if let (Some(expr), Some(unit)) = (&self.expr, &self.unit) {
                out.push_str(" INTERVAL ");
                expr.restore_into(out);
                out.push(' ');
                out.push_str(unit);
            }
            if self.limit > 0 {
                out.push_str(" LIMIT ");
                out.push_str(&self.limit.to_string());
            }
            return;
        }
        if let Some(expr) = &self.expr {
            out.push_str(" (");
            expr.restore_into(out);
            out.push(')');
        } else {
            if matches!(self.kind, PartitionType::Range | PartitionType::List) {
                out.push_str(" COLUMNS");
            }
            out.push_str(" (");
            for (index, column) in self.columns.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_name_path(out, column);
            }
            out.push(')');
        }
        if let Some(interval) = &self.interval {
            out.push_str(" INTERVAL (");
            interval.expr.restore_into(out);
            if let Some(unit) = &interval.unit {
                out.push(' ');
                out.push_str(unit);
            }
            out.push(')');
            if let Some(expr) = &interval.first_range_end {
                out.push_str(" FIRST PARTITION LESS THAN (");
                expr.restore_into(out);
                out.push(')');
            }
            if let Some(expr) = &interval.last_range_end {
                out.push_str(" LAST PARTITION LESS THAN (");
                expr.restore_into(out);
                out.push(')');
            }
            if interval.null_partition {
                out.push_str(" NULL PARTITION");
            }
            if interval.maxvalue_partition {
                out.push_str(" MAXVALUE PARTITION");
            }
        }
    }
}

/// The partition-boundary forms that Go's parser puts in one interface.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionDefinitionClause {
    /// No explicit value clause.
    None,
    /// `VALUES LESS THAN (value, ...)`.
    LessThan(Vec<PartitionValue>),
    /// `VALUES IN (value | (value, ...), ...)`.
    In(Vec<PartitionValue>),
    /// The canonical `DEFAULT` form.
    Default,
    /// System-versioning marker.
    History {
        /// `true` for `CURRENT`, `false` for `HISTORY`.
        current: bool,
    },
}

impl PartitionDefinitionClause {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::None => {}
            Self::LessThan(values) => {
                out.push_str(" VALUES LESS THAN (");
                restore_partition_values(out, values);
                out.push(')');
            }
            Self::In(values) => {
                out.push_str(" VALUES IN (");
                restore_partition_values(out, values);
                out.push(')');
            }
            Self::Default => out.push_str(" DEFAULT"),
            Self::History { current } => {
                out.push_str(if *current { " CURRENT" } else { " HISTORY" })
            }
        }
    }
}

/// A scalar, tuple, or special marker in a partition value list.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionValue {
    /// A normal scalar expression.
    Expr(Expr),
    /// A parenthesized value tuple.
    Tuple(Vec<Expr>),
    /// The LIST-partition-only `DEFAULT` marker.
    Default,
    /// The RANGE-partition-only `MAXVALUE` marker.
    MaxValue,
}

fn restore_partition_values(out: &mut String, values: &[PartitionValue]) {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        match value {
            PartitionValue::Expr(expr) => expr.restore_into(out),
            PartitionValue::Tuple(exprs) => {
                out.push('(');
                for (index, expr) in exprs.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    expr.restore_into(out);
                }
                out.push(')');
            }
            PartitionValue::Default => out.push_str("DEFAULT"),
            PartitionValue::MaxValue => out.push_str("MAXVALUE"),
        }
    }
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for AlterPartitionAction {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Repartition(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SetAttributes {
                partition,
                attributes,
            } => {
                let _ = partition;
                let _ = attributes;
            }
            Self::SetOptions { partition, options } => {
                for option in options.iter_mut() {
                    if !crate::Visitable::accept(option, visitor) {
                        return false;
                    }
                }
                let _ = partition;
                let _ = options;
            }
            Self::Add {
                if_not_exists,
                no_write_to_binlog,
                spec,
            } => {
                if !crate::Visitable::accept(spec, visitor) {
                    return false;
                }
                let _ = if_not_exists;
                let _ = no_write_to_binlog;
                let _ = spec;
            }
            Self::Exchange {
                partition,
                table,
                with_validation,
            } => {
                let _ = partition;
                let _ = table;
                let _ = with_validation;
            }
            Self::Drop { if_exists, names } => {
                let _ = if_exists;
                let _ = names;
            }
            Self::Reorganize {
                no_write_to_binlog,
                names,
                definitions,
            } => {
                for value in definitions.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = no_write_to_binlog;
                let _ = names;
                let _ = definitions;
            }
            Self::Coalesce {
                no_write_to_binlog,
                count,
            } => {
                let _ = no_write_to_binlog;
                let _ = count;
            }
            Self::Truncate { all, names } => {
                let _ = all;
                let _ = names;
            }
            Self::Check { all, names } => {
                let _ = all;
                let _ = names;
            }
            Self::ImportTablespace { all, names } => {
                let _ = all;
                let _ = names;
            }
            Self::DiscardTablespace { all, names } => {
                let _ = all;
                let _ = names;
            }
            Self::RemovePartitioning => {}
            Self::FirstPartitionLessThan { expr, if_exists } => {
                if !crate::Visitable::accept(expr, visitor) {
                    return false;
                }
                let _ = expr;
                let _ = if_exists;
            }
            Self::LastPartitionLessThan {
                expr,
                no_write_to_binlog,
            } => {
                if !crate::Visitable::accept(expr, visitor) {
                    return false;
                }
                let _ = expr;
                let _ = no_write_to_binlog;
            }
            Self::SplitMaxValuePartition { expr } => {
                if !crate::Visitable::accept(expr, visitor) {
                    return false;
                }
                let _ = expr;
            }
            Self::MergeFirstPartitionLessThan { expr } => {
                if !crate::Visitable::accept(expr, visitor) {
                    return false;
                }
                let _ = expr;
            }
            Self::Maintain {
                operation,
                no_write_to_binlog,
                all,
                names,
            } => {
                if !crate::Visitable::accept(operation, visitor) {
                    return false;
                }
                let _ = operation;
                let _ = no_write_to_binlog;
                let _ = all;
                let _ = names;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for PartitionMaintenanceOp {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Rebuild => {}
            Self::Optimize => {}
            Self::Repair => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AddPartitionSpec {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Count(field_0) => {
                let _ = field_0;
            }
            Self::Definitions(field_0) => {
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

impl crate::Visitable for PartitionMethod {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            kind,
            linear,
            expr,
            columns,
            key_algorithm,
            unit,
            limit,
            count,
            interval,
        } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        if let Some(value) = expr.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = interval.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = kind;
        let _ = linear;
        let _ = expr;
        let _ = columns;
        let _ = key_algorithm;
        let _ = unit;
        let _ = limit;
        let _ = count;
        let _ = interval;
        visitor.leave(self)
    }
}

impl crate::Visitable for PartitionInterval {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            expr,
            unit,
            first_range_end,
            last_range_end,
            null_partition,
            maxvalue_partition,
        } = self;
        if !crate::Visitable::accept(expr, visitor) {
            return false;
        }
        if let Some(value) = first_range_end.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = last_range_end.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = expr;
        let _ = unit;
        let _ = first_range_end;
        let _ = last_range_end;
        let _ = null_partition;
        let _ = maxvalue_partition;
        visitor.leave(self)
    }
}

impl crate::Visitable for TablePartitioning {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            method,
            subpartition,
            definitions,
            update_indexes,
        } = self;
        if !crate::Visitable::accept(method, visitor) {
            return false;
        }
        if let Some(value) = subpartition.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in definitions.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in update_indexes.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = method;
        let _ = subpartition;
        let _ = definitions;
        let _ = update_indexes;
        visitor.leave(self)
    }
}

impl crate::Visitable for PartitionIndexUpdate {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, global } = self;
        let _ = name;
        let _ = global;
        visitor.leave(self)
    }
}

impl crate::Visitable for PartitionDefinition {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            name,
            clause,
            options,
            sub_partitions,
        } = self;
        if !crate::Visitable::accept(clause, visitor) {
            return false;
        }
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        for value in sub_partitions.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = name;
        let _ = clause;
        let _ = options;
        let _ = sub_partitions;
        visitor.leave(self)
    }
}

impl crate::Visitable for SubPartitionDefinition {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, options } = self;
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = name;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for PartitionDefinitionClause {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::LessThan(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
            Self::In(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
            Self::Default => {}
            Self::History { current } => {
                let _ = current;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for PartitionValue {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Expr(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Tuple(field_0) => {
                for value in field_0.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = field_0;
            }
            Self::Default => {}
            Self::MaxValue => {}
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
