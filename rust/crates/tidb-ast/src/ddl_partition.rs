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
    /// `PARTITION name PLACEMENT [POLICY] (SET DEFAULT | [=]
    /// (DEFAULT|StringName))`. Go stores this as an
    /// `AlterTablePartitionOptions` spec rather than a root table option, so
    /// retain the partition target alongside its policy payload.
    SetPlacementPolicy {
        /// The partition whose policy changes.
        partition: String,
        /// Placement-policy name, or the canonical `DEFAULT` reset marker.
        policy: String,
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
        expr: Expr,
        /// Whether the source requested `IF EXISTS`.
        if_exists: bool,
    },
    /// `LAST PARTITION LESS THAN (expr)`. Go stores this as the
    /// `AlterTableAddLastPartition` specification and restores the optional
    /// `NO_WRITE_TO_BINLOG` marker after the bound.
    LastPartitionLessThan {
        /// The exclusive upper bound for the last interval partition.
        expr: Expr,
        /// Whether the source requested `NO_WRITE_TO_BINLOG`/`LOCAL`.
        no_write_to_binlog: bool,
    },
    /// `SPLIT MAXVALUE PARTITION LESS THAN (expr)`. Go stores this as the
    /// `AlterTableReorganizeLastPartition` specification.
    SplitMaxValuePartition {
        /// The exclusive upper bound for the new partition.
        expr: Expr,
    },
    /// `MERGE FIRST PARTITION LESS THAN (expr)`. Go stores this as the
    /// `AlterTableReorganizeFirstPartition` specification.
    MergeFirstPartitionLessThan {
        /// The exclusive upper bound for the merged first partition.
        expr: Expr,
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
pub(super) fn restore_alter_action(out: &mut String, action: &AlterPartitionAction) {
    match action {
        AlterPartitionAction::Repartition(partitioning) => {
            partitioning.restore_after_alter_table(out);
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
        AlterPartitionAction::SetPlacementPolicy { partition, policy } => {
            out.push_str("PARTITION ");
            out.push_str(&back_quote(partition));
            out.push_str(" PLACEMENT POLICY = ");
            out.push_str(&back_quote(policy));
        }
        AlterPartitionAction::Add {
            if_not_exists,
            no_write_to_binlog,
            spec,
        } => {
            out.push_str("ADD PARTITION");
            if *if_not_exists {
                out.push_str(" IF NOT EXISTS");
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
                        definition.restore_into(out);
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
                out.push_str("IF EXISTS ");
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
                    definition.restore_into(out);
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
    /// KEY columns or RANGE/LIST `COLUMNS` names.
    pub columns: Vec<String>,
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
    pub interval: Option<PartitionInterval>,
}

/// The supported `PARTITION BY` method classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionType {
    /// `HASH (expr)`.
    Hash,
    /// `KEY [ALGORITHM = n] (columns)`.
    Key,
    /// `RANGE (expr)` or `RANGE COLUMNS (columns)`.
    Range,
    /// `LIST (expr)` or `LIST COLUMNS (columns)`.
    List,
    /// `SYSTEM_TIME [INTERVAL expr unit | LIMIT n]`.
    SystemTime,
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
    pub(super) fn restore_into(&self, out: &mut String) {
        out.push_str("PARTITION ");
        out.push_str(&back_quote(&self.name));
        self.clause.restore_into(out);
        for option in &self.options {
            out.push(' ');
            option.restore_into(out);
        }
        if !self.sub_partitions.is_empty() {
            out.push_str(" (");
            for (index, definition) in self.sub_partitions.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                definition.restore_into(out);
            }
            out.push(')');
        }
    }
}

impl SubPartitionDefinition {
    fn restore_into(&self, out: &mut String) {
        out.push_str("SUBPARTITION ");
        out.push_str(&back_quote(&self.name));
        for option in &self.options {
            out.push(' ');
            option.restore_into(out);
        }
    }
}

impl TablePartitioning {
    /// Restores Go's `PartitionOptions` canonical order.
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_with_prefix(out, " PARTITION BY ");
    }

    /// Restore this shared payload as a terminal `ALTER TABLE` action.
    fn restore_after_alter_table(&self, out: &mut String) {
        self.restore_with_prefix(out, "PARTITION BY ");
    }

    fn restore_with_prefix(&self, out: &mut String, prefix: &str) {
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
                definition.restore_into(out);
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
        out.push_str(match self.kind {
            PartitionType::Hash => "HASH",
            PartitionType::Key => "KEY",
            PartitionType::Range => "RANGE",
            PartitionType::List => "LIST",
            PartitionType::SystemTime => "SYSTEM_TIME",
        });
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
                out.push_str(&back_quote(column));
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
