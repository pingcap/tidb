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

//! `CREATE`/`ALTER`/`RENAME`/`DROP TABLE` statements and their restore.

use crate::util::{back_quote, escape_string_literal, push_name_path};
use crate::{Expr, RestoreContext};

mod alter;
pub use alter::cache::AlterTableCacheMode;
pub use alter::check::AlterCheck;
pub use alter::column_default::AlterColumnDefault;
pub use alter::drop_check::DropCheck;
pub use alter::drop_foreign_key::DropForeignKey;
pub use alter::drop_primary_key::DropPrimaryKey;
pub use alter::index_visibility::AlterIndexVisibility;
pub use alter::lock::{AlterTableLock, AlterTableLockMode};
pub use alter::rename_column::RenameColumn;
pub use alter::rename_index::RenameIndex;
pub use alter::ttl::AlterTableRemoveTtl;

#[path = "ddl/field_type.rs"]
mod field_type;
pub use field_type::{ColumnType, ColumnTypeArg};

// A column definition is shared by CREATE TABLE and every column-bearing
// ALTER TABLE action. Its AST/restore behavior therefore owns a leaf of its
// own instead of coupling either statement envelope to the other.
#[path = "ddl/column.rs"]
mod column;
pub use column::{
    AutoRandomOption, ColumnDef, ColumnFormat, ColumnOption, ColumnStorage, InlineKeyKind,
    InlineKeyOption,
};

// CHECK constraints cross CREATE TABLE, ALTER TABLE, and column options, so
// their source-shaped payload and restore contract live at their own shared
// boundary rather than inside any statement or column envelope.
#[path = "ddl/check.rs"]
mod check;
pub use check::CheckConstraintDefinition;

// Table options form their own stable AST/restore domain shared by CREATE
// TABLE and ALTER TABLE. Keep that vocabulary out of this statement-family
// root so option work can proceed independently from the table envelopes.
#[path = "ddl/table_option.rs"]
mod table_option;
pub use table_option::TableOption;

// Creation-side SPLIT owns a different envelope from ALTER TABLE SPLIT: the
// table name is already carried by CREATE TABLE, and Go stores a list of
// these options directly on CreateTableStmt. Keep that boundary physical so
// future work on one form cannot accidentally reuse the other's target type.
#[path = "ddl/create_split.rs"]
mod create_split;
pub use create_split::{CreateTableSplit, CreateTableSplitTarget};

// The CREATE TABLE envelope owns the statement-level ordering contract while
// its elements remain in their own shared leaves.
#[path = "ddl/create.rs"]
mod create;
pub use create::{
    CreateTableAsQuery, CreateTableOnDuplicate, CreateTableStmt, CreateTableTemporary,
};

// Partition payloads are shared by CREATE/ALTER table grammar, but their
// representation and restoration form one stable subdomain of DDL rather
// than a growing tail in this statement-family module.
#[path = "ddl_partition.rs"]
mod partition;
pub use partition::{
    AddPartitionSpec, AlterPartitionAction, PartitionDefinition, PartitionDefinitionClause,
    PartitionIndexUpdate, PartitionInterval, PartitionMaintenanceOp, PartitionMethod,
    PartitionValue, SubPartitionDefinition, TablePartitioning,
};

// Index syntax is shared by standalone CREATE/DROP, CREATE TABLE constraints,
// and ALTER TABLE.  Keep its lossless source vocabulary in one leaf before
// those statement envelopes migrate to it.
#[path = "ddl_index.rs"]
mod index;
pub use index::{
    ForeignKeyConstraintDefinition, ForeignKeyMatch, ForeignKeyReference, IndexAlgorithm,
    IndexConstraintDefinition, IndexConstraintKind, IndexKind, IndexLock, IndexOnlineDdl,
    IndexOptions, IndexPreSplitRegions, IndexVisibility,
};

// `CREATE VIEW` has a distinct grammar envelope from table creation. Keep its
// algorithm/definer/security/query payload physically isolated so the view
// source family can evolve without reopening this shared DDL root.
#[path = "ddl/create_view.rs"]
mod create_view;
pub use create_view::CreateViewStmt;

/// The non-default `ALGORITHM` characteristic of a standalone `DROP INDEX`.
///
/// Go stores `DEFAULT` too, but its restore omits it. Keeping only visible
/// alternatives makes that canonicalization structural rather than a
/// conditional at every restore site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropIndexAlgorithm {
    /// `ALGORITHM = INPLACE`.
    Inplace,
    /// `ALGORITHM = COPY`.
    Copy,
    /// `ALGORITHM = INSTANT`.
    Instant,
}

impl DropIndexAlgorithm {
    fn restore(self) -> &'static str {
        match self {
            Self::Inplace => "INPLACE",
            Self::Copy => "COPY",
            Self::Instant => "INSTANT",
        }
    }
}

/// The non-default `LOCK` characteristic of a standalone `DROP INDEX`.
///
/// As with [`DropIndexAlgorithm`], Go's `DEFAULT` value is deliberately
/// represented by absence because `ast.DropIndexStmt.Restore` omits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropIndexLock {
    /// `LOCK = NONE`.
    None,
    /// `LOCK = SHARED`.
    Shared,
    /// `LOCK = EXCLUSIVE`.
    Exclusive,
}

impl DropIndexLock {
    fn restore(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Shared => "SHARED",
            Self::Exclusive => "EXCLUSIVE",
        }
    }
}

/// A standalone `DROP INDEX [IF EXISTS] name ON table` statement.
///
/// This mirrors Go's `ast.DropIndexStmt` / `IndexLockAndAlgorithm` boundary:
/// `DEFAULT` algorithm and lock clauses are accepted but absent from the
/// canonical SQL, while non-default algorithm is restored before lock even if
/// source order was reversed. Execution remains deliberately unsupported
/// until the catalog can model index ownership and missing-index semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropIndexStmt {
    /// Whether Go parsed the `HYPO` modifier. Go deliberately omits this
    /// execution-only flag from `DropIndexStmt.Restore`, so canonical SQL is
    /// still `DROP INDEX ...`.
    pub is_hypo: bool,
    /// Whether a missing index is ignored.
    pub if_exists: bool,
    /// The local index name.
    pub name: String,
    /// The table that owns the index.
    pub table: Vec<String>,
    /// The optional non-default online DDL algorithm.
    pub algorithm: Option<DropIndexAlgorithm>,
    /// The optional non-default online DDL lock mode.
    pub lock: Option<DropIndexLock>,
}

impl DropIndexStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("DROP INDEX ");
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
        out.push_str(" ON ");
        push_name_path(out, &self.table);
        if let Some(algorithm) = self.algorithm {
            out.push_str(" ALGORITHM = ");
            out.push_str(algorithm.restore());
        }
        if let Some(lock) = self.lock {
            out.push_str(" LOCK = ");
            out.push_str(lock.restore());
        }
    }

    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        out.push_str("DROP INDEX ");
        if self.if_exists {
            context.write_with_tidb_special_comment(out, "", |out| {
                out.push_str("IF EXISTS ");
            });
        }
        out.push_str(&back_quote(&self.name));
        out.push_str(" ON ");
        push_name_path(out, &self.table);
        if let Some(algorithm) = self.algorithm {
            out.push_str(" ALGORITHM = ");
            out.push_str(algorithm.restore());
        }
        if let Some(lock) = self.lock {
            out.push_str(" LOCK = ");
            out.push_str(lock.restore());
        }
    }
}

/// A `RENAME TABLE old1 TO new1 [, old2 TO new2 ...]` statement — a
/// different top-level statement kind from `ALTER TABLE ... RENAME`, though
/// both rename a table. Pairs apply in written order (verified against real
/// TiDB with a 3-way swap: `a TO c, b TO a, c TO b` correctly swaps `a` and
/// `b`'s contents, matching straightforward sequential processing).
#[derive(Debug, Clone, PartialEq)]
pub struct RenameTableStmt {
    /// Each `(old_name, new_name)` pair, in written order.
    pub pairs: Vec<(Vec<String>, Vec<String>)>,
}

impl RenameTableStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("RENAME TABLE ");
        for (i, (old, new)) in self.pairs.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            push_name_path(out, old);
            out.push_str(" TO ");
            push_name_path(out, new);
        }
    }
}

/// A `DROP [GLOBAL] [TEMPORARY] TABLE [IF EXISTS] name [, ...] [RESTRICT |
/// CASCADE]` statement. `RESTRICT`/`CASCADE` parse but restore to nothing
/// (confirmed via `godump restore`) — real MySQL/TiDB enforce referential
/// integrity unconditionally regardless of which is written, so neither
/// changes behavior; they exist only for cross-database portability.
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStmt {
    /// The `TEMPORARY` / `GLOBAL TEMPORARY` modifier (restored before
    /// `TABLE`). This executor never models temporary tables, so a
    /// temporary drop is `Unsupported` at execution — parse+restore only.
    pub temporary: DropTemporary,
    /// Suppresses the "table doesn't exist" error for any name in `names`
    /// that isn't in the catalog — each name is still checked
    /// independently, so a mix of existing and missing names still drops
    /// the existing ones (see `Database::drop_table`'s own doc for the
    /// exact per-name semantics, confirmed via `gorun`).
    pub if_exists: bool,
    /// Each table's name path, in written order.
    pub names: Vec<Vec<String>>,
}

/// The temporary-ness of a `DROP ... TABLE` (`DROP TABLE` vs `DROP
/// TEMPORARY TABLE` vs `DROP GLOBAL TEMPORARY TABLE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DropTemporary {
    /// A plain `DROP TABLE`.
    #[default]
    None,
    /// `DROP TEMPORARY TABLE`.
    Local,
    /// `DROP GLOBAL TEMPORARY TABLE`.
    Global,
}

impl DropTableStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(match self.temporary {
            DropTemporary::None => "DROP TABLE ",
            DropTemporary::Local => "DROP TEMPORARY TABLE ",
            DropTemporary::Global => "DROP GLOBAL TEMPORARY TABLE ",
        });
        if self.if_exists {
            out.push_str("IF EXISTS ");
        }
        for (i, name) in self.names.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            push_name_path(out, name);
        }
    }
}

/// One ordinary secondary-index key part, shared by standalone `CREATE
/// INDEX` and `ALTER TABLE ... ADD INDEX`.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexPart {
    /// A named column with an optional prefix length and descending order.
    Column {
        /// Column identifier.
        name: String,
        /// Parsed prefix length. Go restores zero as absent.
        prefix_len: Option<i64>,
        /// Descending sort direction.
        desc: bool,
    },
    /// A parenthesized functional or multi-valued expression.
    Expr {
        /// Expression payload.
        expr: Expr,
        /// Descending sort direction.
        desc: bool,
    },
}

impl IndexPart {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Column {
                name,
                prefix_len,
                desc,
            } => {
                out.push_str(&back_quote(name));
                if let Some(prefix_len) = prefix_len.filter(|prefix_len| *prefix_len > 0) {
                    out.push('(');
                    out.push_str(&prefix_len.to_string());
                    out.push(')');
                }
                if *desc {
                    out.push_str(" DESC");
                }
            }
            Self::Expr { expr, desc } => {
                out.push('(');
                expr.restore_into(out);
                out.push(')');
                if *desc {
                    out.push_str(" DESC");
                }
            }
        }
    }
}

/// Typed standalone `CREATE ... INDEX` payload.
///
/// The index class, options, and online DDL modifiers intentionally use the
/// same source-shaped vocabulary as the Go AST.  Keeping the envelope whole
/// avoids the old reduced booleans silently discarding valid `CREATE INDEX`
/// forms before the executor sees them.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStmt {
    /// Source index class.
    pub kind: IndexKind,
    /// `IF NOT EXISTS` modifier.
    pub if_not_exists: bool,
    /// Index identifier.
    pub name: String,
    /// Target table path.
    pub table: Vec<String>,
    /// Ordered key parts.
    pub parts: Vec<IndexPart>,
    /// Source-visible index options.
    pub options: IndexOptions,
    /// Source-visible online DDL modifiers.
    pub online: IndexOnlineDdl,
}

impl CreateIndexStmt {
    /// Appends the ordinary canonical SQL used by default restoration.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("CREATE ");
        out.push_str(self.kind.sql());
        out.push(' ');
        if self.if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        out.push_str(&back_quote(&self.name));
        out.push_str(" ON ");
        push_name_path(out, &self.table);
        out.push(' ');
        push_index_parts(out, &self.parts);
        let options = self.options.restore();
        if !self.options.is_empty_for_statement_restore() {
            out.push(' ');
            out.push_str(&options);
        }
        let online = self.online.restore();
        if !online.is_empty() {
            out.push(' ');
            out.push_str(&online);
        }
    }

    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        out.push_str("CREATE ");
        out.push_str(self.kind.sql());
        out.push(' ');
        if self.if_not_exists {
            context.write_with_tidb_special_comment(out, "", |out| {
                out.push_str("IF NOT EXISTS ");
            });
        }
        out.push_str(&back_quote(&self.name));
        out.push_str(" ON ");
        push_name_path(out, &self.table);
        out.push(' ');
        push_index_parts(out, &self.parts);
        let options = self.options.restore_with_context(context);
        if !self.options.is_empty_for_statement_restore() {
            out.push(' ');
            out.push_str(&options);
        }
        let online = self.online.restore();
        if !online.is_empty() {
            out.push(' ');
            out.push_str(&online);
        }
    }
}

fn push_index_parts(out: &mut String, parts: &[IndexPart]) {
    out.push('(');
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            out.push_str(", ");
        }
        part.restore_into(out);
    }
    out.push(')');
}

/// An `ALTER TABLE` statement: the table name and its ordered alteration
/// specifications. This is Go's `AlterTableStmt.Specs` ownership boundary;
/// each typed action documents its own execution capability.
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStmt {
    /// The table name path.
    pub name: Vec<String>,
    /// Alteration actions in source order.
    pub actions: Vec<AlterTableAction>,
}

/// A source-backed split boundary specification shared by standalone
/// `SPLIT ... TABLE` and `ALTER TABLE ... SPLIT ...` forms.
///
/// `BY` retains each supplied key tuple, whereas `BETWEEN` retains both
/// bounds and the requested number of regions.  This is intentionally syntax
/// only: turning expressions into encoded TiKV keys belongs to a real table
/// codec and placement-aware storage layer, not this AST.
#[derive(Debug, Clone, PartialEq)]
pub enum SplitOption {
    /// `BY (value [, value ...]) [, ...]`.
    By(Vec<Vec<Expr>>),
    /// `BETWEEN (lower...) AND (upper...) REGIONS count`.
    Between {
        /// The inclusive lower split-key tuple; Go permits an empty tuple.
        lower: Vec<Expr>,
        /// The inclusive upper split-key tuple; Go permits an empty tuple.
        upper: Vec<Expr>,
        /// Number of requested regions, parsed from an unsigned integer token.
        regions: i64,
    },
}

impl SplitOption {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::By(points) => {
                out.push_str("BY ");
                for (point_index, point) in points.iter().enumerate() {
                    if point_index != 0 {
                        out.push(',');
                    }
                    push_split_tuple(out, point);
                }
            }
            Self::Between {
                lower,
                upper,
                regions,
            } => {
                out.push_str("BETWEEN ");
                push_split_tuple(out, lower);
                out.push_str(" AND ");
                push_split_tuple(out, upper);
                out.push_str(" REGIONS ");
                out.push_str(&regions.to_string());
            }
        }
    }
}

/// The object whose keyspace an `ALTER TABLE ... SPLIT` operation addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitTarget {
    /// The altered table's record keyspace. Go accepts both `SPLIT TABLE`
    /// and bare `SPLIT`, but its ALTER restore omits `TABLE`.
    Table,
    /// The altered table's primary-index keyspace (`SPLIT PRIMARY KEY`).
    PrimaryKey,
    /// A named secondary index (`SPLIT INDEX name`).
    Index(String),
}

impl SplitTarget {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Table => {}
            Self::PrimaryKey => out.push_str("PRIMARY KEY "),
            Self::Index(name) => {
                out.push_str("INDEX ");
                out.push_str(&back_quote(name));
                out.push(' ');
            }
        }
    }
}

/// The standalone `SPLIT [REGION FOR] [PARTITION] TABLE` statement.
///
/// It lives in the administrative envelope because Go's `SplitRegionStmt`
/// causes region-management work rather than catalog DDL.  The payload is
/// nevertheless typed so parsing never erases table/index/partition intent.
#[derive(Debug, Clone, PartialEq)]
pub struct SplitRegionStmt {
    /// Whether the input used the `REGION FOR` prefix.
    pub region_for: bool,
    /// Whether the input used the `PARTITION` prefix before `TABLE`.
    pub partition_syntax: bool,
    /// The target table name path.
    pub table: Vec<String>,
    /// Optional partition names after the table name.
    pub partitions: Vec<String>,
    /// Optional secondary-index name.
    pub index: Option<String>,
    /// Either explicit points or an interpolated range.
    pub option: SplitOption,
}

impl SplitRegionStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("SPLIT ");
        if self.region_for {
            out.push_str("REGION FOR ");
        }
        if self.partition_syntax {
            out.push_str("PARTITION ");
        }
        out.push_str("TABLE ");
        push_name_path(out, &self.table);
        if !self.partitions.is_empty() {
            out.push_str(" PARTITION(");
            for (index, partition) in self.partitions.iter().enumerate() {
                if index != 0 {
                    out.push_str(", ");
                }
                out.push_str(&back_quote(partition));
            }
            out.push(')');
        }
        if let Some(index) = &self.index {
            out.push_str(" INDEX ");
            out.push_str(&back_quote(index));
        }
        out.push(' ');
        self.option.restore_into(out);
    }
}

/// Restores Go's `SplitOption` tuple form.  These are deliberately not
/// [`Expr::Row`]: Go restores split tuples with ordinary parentheses, not
/// `ROW(...)`, even when a tuple contains multiple values.
fn push_split_tuple(out: &mut String, values: &[Expr]) {
    out.push('(');
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            out.push(',');
        }
        value.restore_into(out);
    }
    out.push(')');
}

impl AlterTableStmt {
    /// Appends the ordinary canonical SQL used by default restoration.
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ALTER TABLE ");
        push_name_path(out, &self.name);
        for (index, action) in self.actions.iter().enumerate() {
            if index == 0 || action.uses_space_separator() {
                out.push(' ');
            } else {
                out.push_str(", ");
            }
            action.restore_into(out);
        }
    }

    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        if context.flags().has_skip_placement_rule_for_restore()
            && !self.actions.is_empty()
            && self.actions.iter().all(|action| {
                matches!(
                    action,
                    AlterTableAction::Partition(AlterPartitionAction::SetOptions { options, .. })
                        if options.iter().all(|option| matches!(option, TableOption::PlacementPolicy(_)))
                )
            })
        {
            return;
        }
        out.push_str("ALTER TABLE ");
        push_name_path(out, &self.name);
        let mut restored = 0;
        for action in &self.actions {
            if action.is_suppressed(context) {
                continue;
            }
            if restored == 0 || action.uses_space_separator() {
                out.push(' ');
            } else {
                out.push_str(", ");
            }
            action.restore_into_with_context(out, context);
            restored += 1;
        }
    }
}

/// `ATTRIBUTES [=] {DEFAULT | 'attributes'}` payload owned by one ALTER TABLE
/// specification. `None` is Go's explicit DEFAULT state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributesSpec {
    /// Attribute text, or `None` for `DEFAULT`.
    pub attributes: Option<String>,
}

impl AttributesSpec {
    fn restore_into(&self, out: &mut String) {
        out.push_str("ATTRIBUTES=");
        match &self.attributes {
            Some(attributes) => {
                out.push('\'');
                out.push_str(&escape_string_literal(attributes));
                out.push('\'');
            }
            None => out.push_str("DEFAULT"),
        }
    }
}

/// `STATS_OPTIONS [=] {DEFAULT | 'options'}` payload owned by one ALTER
/// TABLE specification. It is distinct from CREATE TABLE's individual
/// `STATS_*` table options, matching Go's AST and visitor boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsOptionsSpec {
    /// Statistics option text, or `None` for `DEFAULT`.
    pub options: Option<String>,
}

/// Online DDL algorithm selected by `ALTER TABLE ... ALGORITHM`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlterTableAlgorithm {
    /// Let TiDB select the algorithm.
    Default,
    /// Copy the table.
    Copy,
    /// Perform the change in place.
    Inplace,
    /// Metadata-only instant change.
    Instant,
}

impl AlterTableAlgorithm {
    fn sql(self) -> &'static str {
        match self {
            Self::Default => "DEFAULT",
            Self::Copy => "COPY",
            Self::Inplace => "INPLACE",
            Self::Instant => "INSTANT",
        }
    }
}

impl StatsOptionsSpec {
    fn restore_into(&self, out: &mut String) {
        out.push_str("STATS_OPTIONS=");
        match &self.options {
            Some(options) => {
                out.push('\'');
                out.push_str(&escape_string_literal(options));
                out.push('\'');
            }
            None => out.push_str("DEFAULT"),
        }
    }
}

impl AlterTableAction {
    fn is_suppressed(&self, context: &RestoreContext) -> bool {
        (context.flags().has_with_ttl_enable_off()
            && matches!(
                self,
                Self::SetTableOptions { options }
                    if options.iter().all(|option| matches!(option, TableOption::TtlEnable(_)))
            ))
            || (context.flags().has_skip_placement_rule_for_restore()
                && matches!(
                    self,
                    Self::SetTableOptions { options }
                        if options.iter().all(|option| matches!(option, TableOption::PlacementPolicy(_)))
                ))
            || (context.flags().has_skip_placement_rule_for_restore()
                && matches!(
                    self,
                    Self::Partition(AlterPartitionAction::SetOptions { options, .. })
                        if options.iter().all(|option| matches!(option, TableOption::PlacementPolicy(_)))
                ))
    }

    /// Go joins terminal partition replacement/removal specs with whitespace
    /// rather than the ordinary comma separator.
    fn uses_space_separator(&self) -> bool {
        matches!(
            self,
            Self::Partition(
                AlterPartitionAction::RemovePartitioning | AlterPartitionAction::Repartition(_)
            )
        )
    }

    fn restore_into(&self, out: &mut String) {
        match self {
            AlterTableAction::AddIndexConstraint(constraint) => {
                out.push_str("ADD ");
                constraint.restore_into(out);
            }
            AlterTableAction::AddForeignKey(constraint) => {
                out.push_str("ADD ");
                constraint.restore_into(out);
            }
            _ => self.restore_into_with_context(out, &RestoreContext::default()),
        }
    }

    fn restore_into_with_context(&self, out: &mut String, context: &RestoreContext) {
        match self {
            AlterTableAction::Partition(action) => {
                partition::restore_alter_action(out, action, context)
            }
            AlterTableAction::SetKeysEnabled(enabled) => {
                out.push_str(if *enabled {
                    "ENABLE KEYS"
                } else {
                    "DISABLE KEYS"
                });
            }
            AlterTableAction::AddColumn {
                if_not_exists,
                column,
                position,
            } => {
                out.push_str("ADD COLUMN ");
                if *if_not_exists {
                    context.write_with_tidb_special_comment(out, "", |out| {
                        out.push_str("IF NOT EXISTS ");
                    });
                }
                column.restore_into_with_context(out, context);
                push_column_position(out, position);
            }
            AlterTableAction::AddColumns {
                if_not_exists,
                columns,
                constraints,
            } => {
                out.push_str("ADD COLUMN ");
                if *if_not_exists {
                    // Go keeps this TiDB-only spelling in a special comment
                    // when restoring an ALTER TABLE specification.
                    context.write_with_tidb_special_comment(out, "", |out| {
                        out.push_str("IF NOT EXISTS ");
                    });
                }
                out.push('(');
                let mut first = true;
                for column in columns {
                    if !first {
                        out.push_str(", ");
                    }
                    first = false;
                    column.restore_into_with_context(out, context);
                }
                // Go's AlterTableSpec has separate NewColumns and
                // NewConstraints slices and restores all columns before all
                // constraints, even when the source interleaves them.
                for constraint in constraints {
                    if !first {
                        out.push_str(", ");
                    }
                    first = false;
                    match constraint {
                        TableConstraint::Index(index) => {
                            index.restore_into_with_context(out, context)
                        }
                        TableConstraint::Check(check) => {
                            check.restore_into_with_context(out, context)
                        }
                        TableConstraint::ForeignKey(foreign_key) => {
                            foreign_key.restore_into_with_context(out, context)
                        }
                    }
                }
                out.push(')');
            }
            AlterTableAction::DropColumn { if_exists, name } => {
                out.push_str("DROP COLUMN ");
                if *if_exists {
                    context.write_with_tidb_special_comment(out, "", |out| {
                        out.push_str("IF EXISTS ");
                    });
                }
                out.push_str(&back_quote(name));
            }
            AlterTableAction::DropPrimaryKey(action) => action.restore_into(out),
            AlterTableAction::DropIndex { name, if_exists } => {
                out.push_str("DROP INDEX ");
                if *if_exists {
                    context.write_with_tidb_special_comment(out, "", |out| {
                        out.push_str("IF EXISTS ");
                    });
                }
                out.push_str(&back_quote(name));
            }
            AlterTableAction::DropForeignKey(action) => {
                out.push_str("DROP FOREIGN KEY ");
                out.push_str(&back_quote(&action.name));
            }
            AlterTableAction::DropCheck(action) => {
                out.push_str("DROP CHECK ");
                out.push_str(&back_quote(&action.name));
            }
            AlterTableAction::Lock(action) => {
                out.push_str("LOCK = ");
                out.push_str(action.mode.sql());
            }
            AlterTableAction::AlterIndexVisibility(action) => {
                out.push_str("ALTER INDEX ");
                out.push_str(&back_quote(&action.name));
                out.push(' ');
                out.push_str(action.visibility.sql());
            }
            AlterTableAction::AlterCheck(action) => {
                out.push_str("ALTER CHECK ");
                out.push_str(&back_quote(&action.name));
                out.push_str(if action.enforced {
                    " ENFORCED"
                } else {
                    " NOT ENFORCED"
                });
            }
            AlterTableAction::AlterColumnDefault(action) => {
                out.push_str("ALTER COLUMN ");
                push_name_path(out, &action.name);
                match &action.default_value {
                    Some(default_value) => {
                        out.push_str(" SET DEFAULT ");
                        default_value.restore_into(out);
                    }
                    None => out.push_str(" DROP DEFAULT"),
                }
            }
            AlterTableAction::RenameIndex(action) => {
                out.push_str("RENAME INDEX ");
                out.push_str(&back_quote(&action.from));
                out.push_str(" TO ");
                out.push_str(&back_quote(&action.to));
            }
            AlterTableAction::RenameColumn(action) => {
                out.push_str("RENAME COLUMN ");
                out.push_str(&back_quote(&action.from));
                out.push_str(" TO ");
                out.push_str(&back_quote(&action.to));
            }
            AlterTableAction::ModifyColumn {
                if_exists,
                column,
                position,
            } => {
                out.push_str("MODIFY COLUMN ");
                if *if_exists {
                    context.write_with_tidb_special_comment(out, "", |out| {
                        out.push_str("IF EXISTS ");
                    });
                }
                column.restore_into_with_context(out, context);
                push_column_position(out, position);
            }
            AlterTableAction::ChangeColumn {
                if_exists,
                old_name,
                column,
                position,
            } => {
                out.push_str("CHANGE COLUMN ");
                if *if_exists {
                    context.write_with_tidb_special_comment(out, "", |out| {
                        out.push_str("IF EXISTS ");
                    });
                }
                push_name_path(out, old_name);
                out.push(' ');
                column.restore_into_with_context(out, context);
                push_column_position(out, position);
            }
            AlterTableAction::OrderByColumns { items } => {
                out.push_str("ORDER BY ");
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    item.restore_into(out);
                }
            }
            AlterTableAction::RenameTable { new_name } => {
                out.push_str("RENAME AS ");
                push_name_path(out, new_name);
            }
            AlterTableAction::SetTableOptions { options } => {
                // `AlterTableSpec.Restore` special-cases exactly the
                // charset+collation pair: unlike either `TableOption`
                // alone, it drops both `DEFAULT` prefixes and equals signs.
                // A longer sequence uses `TableOption.Restore` for every
                // item, so preserve the length-sensitive Go branch instead
                // of normalizing all option lists through one spelling.
                if let [TableOption::CharacterSet(charset), TableOption::Collate(collation)] =
                    options.as_slice()
                {
                    out.push_str("CHARACTER SET ");
                    out.push_str(charset);
                    out.push_str(" COLLATE ");
                    out.push_str(collation);
                    return;
                }
                let mut restored = 0;
                let mut has_ttl_definition = false;
                for option in options {
                    if context.flags().has_skip_placement_rule_for_restore()
                        && matches!(option, TableOption::PlacementPolicy(_))
                    {
                        continue;
                    }
                    if context.flags().has_with_ttl_enable_off()
                        && matches!(option, TableOption::TtlEnable(_))
                    {
                        continue;
                    }
                    if restored > 0 {
                        out.push(' ');
                    }
                    // Go's `parseAlterTableOptions` keeps adjacent options
                    // in one spec; its restore separates them with spaces,
                    // never commas.
                    option.restore_into_with_context(out, context);
                    restored += 1;
                    has_ttl_definition |= matches!(option, TableOption::Ttl { .. });
                }
                if context.flags().has_with_ttl_enable_off() && has_ttl_definition {
                    if restored > 0 {
                        out.push(' ');
                    }
                    TableOption::TtlEnable(false).restore_into_with_context(out, context);
                }
            }
            AlterTableAction::ConvertCharacterSet { charset, collation } => {
                out.push_str("CONVERT TO CHARACTER SET ");
                out.push_str(charset.as_deref().unwrap_or("DEFAULT"));
                if let Some(collation) = collation {
                    out.push_str(" COLLATE ");
                    out.push_str(collation);
                }
            }
            AlterTableAction::Cache(mode) => out.push_str(mode.sql()),
            AlterTableAction::RemoveTtl(_) => {
                context.write_with_tidb_special_comment(out, "ttl", |out| {
                    out.push_str("REMOVE TTL");
                });
            }
            AlterTableAction::SetAttributes(spec) => spec.restore_into(out),
            AlterTableAction::SetStatsOptions(spec) => spec.restore_into(out),
            AlterTableAction::Algorithm(algorithm) => {
                out.push_str("ALGORITHM = ");
                out.push_str(algorithm.sql());
            }
            AlterTableAction::ReadOnly(read_only) => {
                out.push_str(if *read_only {
                    "READ ONLY"
                } else {
                    "READ WRITE"
                });
            }
            AlterTableAction::Force => {
                out.push_str("FORCE /* AlterTableForce is not supported */ ")
            }
            AlterTableAction::SecondaryLoad(load) => {
                out.push_str(if *load {
                    "SECONDARY_LOAD"
                } else {
                    "SECONDARY_UNLOAD"
                });
            }
            AlterTableAction::TablespaceImport(import) => {
                out.push_str(if *import {
                    "IMPORT TABLESPACE"
                } else {
                    "DISCARD TABLESPACE"
                });
            }
            AlterTableAction::AddStatistics {
                if_not_exists,
                name,
                stats_type,
                columns,
            } => {
                out.push_str("ADD STATS_EXTENDED ");
                if *if_not_exists {
                    out.push_str("IF NOT EXISTS ");
                }
                out.push_str(&back_quote(name));
                out.push(' ');
                out.push_str(match stats_type {
                    crate::ExtendedStatsType::Cardinality => "CARDINALITY(",
                    crate::ExtendedStatsType::Dependency => "DEPENDENCY(",
                    crate::ExtendedStatsType::Correlation => "CORRELATION(",
                });
                for (index, column) in columns.iter().enumerate() {
                    if index > 0 {
                        out.push_str(", ");
                    }
                    out.push_str(&back_quote(column));
                }
                out.push(')');
            }
            AlterTableAction::DropStatistics { if_exists, name } => {
                out.push_str("DROP STATS_EXTENDED ");
                if *if_exists {
                    out.push_str("IF EXISTS ");
                }
                out.push_str(&back_quote(name));
            }
            AlterTableAction::WithValidation => out.push_str("WITH VALIDATION"),
            AlterTableAction::WithoutValidation => out.push_str("WITHOUT VALIDATION"),
            AlterTableAction::AddIndexConstraint(constraint) => {
                out.push_str("ADD ");
                constraint.restore_into_with_context(out, context);
            }
            AlterTableAction::AddForeignKey(constraint) => {
                out.push_str("ADD ");
                constraint.restore_into_with_context(out, context);
            }
            AlterTableAction::AddCheck(check) => {
                out.push_str("ADD ");
                check.restore_into_with_context(out, context);
            }
            AlterTableAction::SetTiFlashReplica { count, labels, .. } => {
                // Go's `TiFlashReplicaSpec.Hypo` is deliberately an
                // execution/planner-only marker: `AlterTableSpec.Restore`
                // canonicalizes both `SET TIFLASH` and `SET HYPO TIFLASH`
                // to this spelling.
                out.push_str("SET TIFLASH REPLICA ");
                out.push_str(&count.to_string());
                if !labels.is_empty() {
                    out.push_str(" LOCATION LABELS ");
                    for (index, label) in labels.iter().enumerate() {
                        if index > 0 {
                            out.push_str(", ");
                        }
                        out.push('\'');
                        out.push_str(&escape_string_literal(label));
                        out.push('\'');
                    }
                }
            }
            AlterTableAction::Compact {
                partitions,
                replica_kind,
            } => {
                out.push_str("COMPACT");
                if !partitions.is_empty() {
                    out.push_str(" PARTITION ");
                    for (index, partition) in partitions.iter().enumerate() {
                        if index > 0 {
                            out.push(',');
                        }
                        out.push_str(&back_quote(partition));
                    }
                }
                match replica_kind {
                    CompactReplicaKind::All => {}
                    CompactReplicaKind::TiFlash => out.push_str(" TIFLASH REPLICA"),
                    CompactReplicaKind::TiKv => out.push_str(" TIKV REPLICA"),
                }
            }
            AlterTableAction::SplitRegion { target, option } => {
                out.push_str("SPLIT ");
                target.restore_into(out);
                option.restore_into(out);
            }
            AlterTableAction::MaskingPolicy(action) => action.restore_into(out),
        }
    }
}

/// One `ALTER TABLE` action.
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    /// `REMOVE TTL`, represented separately from partition removal so the
    /// parser cannot consume it as `REMOVE PARTITIONING`.
    RemoveTtl(AlterTableRemoveTtl),
    /// `CACHE` or `NOCACHE`. Go stores these as dedicated ALTER TABLE
    /// specifications rather than table options, so retain the two-state
    /// syntax boundary without conflating it with SELECT SQL_CACHE.
    Cache(AlterTableCacheMode),
    /// Adjacent generic charset/collation table options, such as `CHARSET
    /// utf8mb4 COLLATE utf8mb4_bin`. Go's `parseAlterTableOptions` stores
    /// all of these in one `AlterTableOption` spec and restores them in
    /// written order without comma separators. This seed preserves that
    /// parser/restore contract but does not yet model table-level charset
    /// metadata in the executor.
    SetTableOptions {
        /// The source-order option sequence.
        options: Vec<TableOption>,
    },
    /// `CONVERT TO { CHARACTER SET | CHARSET | CHAR SET } charset [COLLATE
    /// collation]`. Unlike [`Self::SetTableOptions`], Go gives this a
    /// distinct `CONVERT TO` restore prefix and omits the equals sign.
    ConvertCharacterSet {
        /// `None` represents Go's `CHARACTER SET DEFAULT` form.
        charset: Option<String>,
        /// Optional target collation.
        collation: Option<String>,
    },
    /// `ATTRIBUTES [=] {DEFAULT | 'attributes'}`. This is a distinct Go
    /// `AlterTableAttributes` specification, rather than a generic table
    /// option; `None` retains Go's `DEFAULT` payload exactly.
    SetAttributes(AttributesSpec),
    /// `STATS_OPTIONS [=] {DEFAULT | 'options'}` with its distinct Go AST
    /// node and visitor boundary.
    SetStatsOptions(StatsOptionsSpec),
    /// `ALGORITHM [=] {DEFAULT|COPY|INPLACE|INSTANT}`.
    Algorithm(AlterTableAlgorithm),
    /// `READ ONLY` when true and `READ WRITE` when false.
    ReadOnly(bool),
    /// Bare `FORCE`; FORCE auto-ID options remain typed table options.
    Force,
    /// `SECONDARY_LOAD` when true and `SECONDARY_UNLOAD` when false.
    SecondaryLoad(bool),
    /// Table `IMPORT TABLESPACE` when true and `DISCARD TABLESPACE` when false.
    TablespaceImport(bool),
    /// `ADD STATS_EXTENDED [IF NOT EXISTS] name kind(columns...)`.
    AddStatistics {
        /// Source guard.
        if_not_exists: bool,
        /// Extended-statistics name.
        name: String,
        /// Cardinality, dependency, or correlation.
        stats_type: crate::ExtendedStatsType,
        /// Unqualified column names in source order.
        columns: Vec<String>,
    },
    /// `DROP STATS_EXTENDED [IF EXISTS] name`.
    DropStatistics {
        /// Source guard.
        if_exists: bool,
        /// Extended-statistics name.
        name: String,
    },
    /// `WITH VALIDATION`. Go represents this as a standalone ALTER TABLE
    /// specification that may be ordered with other specifications; the
    /// seed executor keeps it typed but rejects it before transaction
    /// mutation because generated-column validation is not modelled.
    WithValidation,
    /// `WITHOUT VALIDATION`. This is the explicit opt-out counterpart to
    /// [`Self::WithValidation`], preserved as a distinct Go specification so
    /// restore cannot lose the source choice.
    WithoutValidation,
    /// `ENABLE KEYS` or `DISABLE KEYS`. Go retains these as distinct
    /// payload-free ALTER specifications; one boolean keeps the same closed
    /// state space without two empty Rust variants.
    SetKeysEnabled(bool),
    /// `ADD [COLUMN] col type [options...] [FIRST | AFTER col]`. `ADD`
    /// alone (without `COLUMN`) restores identically, matching the Go AST's
    /// normalization.
    AddColumn {
        /// Whether Go parsed `IF NOT EXISTS` for this individual column spec.
        if_not_exists: bool,
        /// The new column's definition.
        column: ColumnDef,
        /// Where to insert it; `Default` (the end) if unwritten.
        position: ColumnPosition,
    },
    /// `ADD [COLUMN] (table_element [, table_element ...])`. Go keeps this
    /// grouped form distinct from a single column with an optional position:
    /// its restore contract retains the surrounding parentheses and stores
    /// columns separately from table-level constraints.
    AddColumns {
        /// `IF NOT EXISTS`, retained for grouped ADD COLUMN lists.
        if_not_exists: bool,
        /// The grouped column definitions in source order.
        columns: Vec<ColumnDef>,
        /// Table-level index/check/foreign-key constraints from the same
        /// grouped source list. Go stores these separately from columns and
        /// restores them after every column definition.
        constraints: Vec<TableConstraint>,
    },
    /// `DROP [COLUMN] name`. `DROP` alone restores identically, matching the
    /// Go AST's normalization.
    DropColumn {
        /// Whether Go parsed `IF EXISTS` for this individual column spec.
        if_exists: bool,
        /// The column to remove.
        name: String,
    },
    /// `DROP PRIMARY KEY`. This remains a separate payload-free Go action,
    /// not a synthetic secondary-index removal.
    DropPrimaryKey(DropPrimaryKey),
    /// `DROP {INDEX|KEY} [IF EXISTS] name`. `KEY` restores as `INDEX`,
    /// matching Go's AST. This action has parser/restore fidelity only: the
    /// seed executor rejects it because it does not retain index names or
    /// model TiDB's missing-index and index-ownership behavior.
    DropIndex {
        /// Whether `IF EXISTS` was written.
        if_exists: bool,
        /// The secondary-index name.
        name: String,
    },
    /// `DROP FOREIGN KEY name`. Go's parser records only the existing
    /// foreign-key name; unlike `DROP INDEX`, its hand-written parser does
    /// not accept the AST's MariaDB-only `IF EXISTS` form.
    DropForeignKey(DropForeignKey),
    /// `DROP {CHECK|CONSTRAINT} name`. Go canonicalizes the latter spelling
    /// to `DROP CHECK` and stores only the existing constraint name.
    DropCheck(DropCheck),
    /// `LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE}`. Go restores every input
    /// spelling as `LOCK = mode` and permits it as one ordinary ALTER action.
    Lock(AlterTableLock),
    /// `ALTER INDEX name {VISIBLE|INVISIBLE}`. This is a distinct index
    /// metadata mutation in TiDB, so the parser retains its visibility
    /// target instead of reducing it to an untyped table option. The seed
    /// executor rejects it before mutation until its catalog stores index
    /// visibility.
    AlterIndexVisibility(AlterIndexVisibility),
    /// `ALTER {CHECK|CONSTRAINT} name {ENFORCED|NOT ENFORCED}`. Go restores
    /// both introducers as `ALTER CHECK`; this typed action deliberately
    /// models only enforcement-state changes, not CHECK declarations or
    /// column-default syntax.
    AlterCheck(AlterCheck),
    /// `ALTER [COLUMN] name {SET DEFAULT value|DROP DEFAULT}`. Unlike
    /// MODIFY/CHANGE, Go stores only the existing column name and its
    /// optional default expression; `None` represents `DROP DEFAULT`.
    AlterColumnDefault(AlterColumnDefault),
    /// `RENAME {KEY|INDEX} old TO new`. Go canonicalizes `KEY` to `INDEX`.
    RenameIndex(RenameIndex),
    /// `RENAME COLUMN old TO new`. Go preserves this as a dedicated
    /// payload-free column-name operation, separate from `CHANGE COLUMN`.
    RenameColumn(RenameColumn),
    /// `MODIFY [COLUMN] col type [options...] [FIRST | AFTER col]` — changes
    /// an existing column's type/options in place (`col` names the SAME
    /// column, never a rename); `MODIFY` alone restores identically to the
    /// `COLUMN`-qualified form.
    ModifyColumn {
        /// Whether a missing source column is ignored.
        if_exists: bool,
        /// The column's (unchanged) name and its new type/options.
        column: ColumnDef,
        /// Where to move it; `Default` leaves it at its current position.
        position: ColumnPosition,
    },
    /// `CHANGE [COLUMN] old_name col type [options...] [FIRST | AFTER col]`
    /// — like `MODIFY`, but also renames the column from `old_name` to
    /// `col`'s own name; `CHANGE` alone restores identically to the
    /// `COLUMN`-qualified form.
    ChangeColumn {
        /// Whether a missing source column is ignored.
        if_exists: bool,
        /// The column's current name.
        old_name: Vec<String>,
        /// The new name (in `column.name`) and new type/options.
        column: ColumnDef,
        /// Where to move it; `Default` leaves it at its current position.
        position: ColumnPosition,
    },
    /// `ORDER BY col [, col ...]` — TiDB's table-reorganization ordering
    /// clause. The seed retains the source payload but rejects execution
    /// because it has no physical row-rewrite/storage-engine path.
    OrderByColumns {
        /// Ordered column names and optional descending directions.
        items: Vec<AlterOrderItem>,
    },
    /// `RENAME [TO | AS] name` — renames the table. All three forms (`TO`,
    /// `AS`, or neither) restore identically as `RENAME AS`, matching the
    /// Go AST's normalization.
    RenameTable {
        /// The table's new name path.
        new_name: Vec<String>,
    },
    /// `ADD` followed by any Go index-bearing constraint class: primary,
    /// unique, ordinary, fulltext, vector, or columnar. All options remain
    /// structural rather than being silently reduced before execution.
    AddIndexConstraint(IndexConstraintDefinition),
    /// `ADD [CONSTRAINT name] FOREIGN KEY ... REFERENCES ...`, including
    /// prefix/expression key parts, `MATCH`, and `IF NOT EXISTS`.
    AddForeignKey(ForeignKeyConstraintDefinition),
    /// `ADD [CONSTRAINT [name]] CHECK (expr) [[NOT] ENFORCED]`. The
    /// constraint's parser/restore contract is represented faithfully, but
    /// execution rejects it before the DDL implicit-commit boundary: the
    /// seed catalog has neither constraint metadata nor write-time check
    /// enforcement.
    AddCheck(CheckConstraintDefinition),
    /// Partition grammar is owned by one typed subdomain rather than flat
    /// variants in the table-alter action list.
    Partition(AlterPartitionAction),
    /// `SET [HYPO] TIFLASH REPLICA count [LOCATION LABELS 'label', ...]`.
    /// Go retains `hypo` for planner semantics but intentionally omits it
    /// from AST restore; preserve that provenance without inventing replica
    /// metadata in the seed executor. Execution is rejected before the DDL
    /// implicit-commit boundary.
    SetTiFlashReplica {
        /// Whether the input used the planner-only `HYPO` form.
        hypo: bool,
        /// Requested TiFlash replica count.
        count: u64,
        /// Optional placement labels, in written order.
        labels: Vec<String>,
    },
    /// Go's separate `CompactTableStmt`, represented here as a typed ALTER
    /// action because this Rust AST has one `ALTER TABLE` envelope. The
    /// distinction is structural only: restore exactly follows Go's compact
    /// statement contract. Execution is rejected before mutation because
    /// compaction requires real TiKV/TiFlash storage engines.
    Compact {
        /// Optional partition names to compact.
        partitions: Vec<String>,
        /// Which replica family Go selected (or all when omitted).
        replica_kind: CompactReplicaKind,
    },
    /// `SPLIT {TABLE | PRIMARY KEY | INDEX name} {BY ... | BETWEEN ...}`.
    ///
    /// This preserves Go's parser/restore contract only.  Actual region
    /// splitting requires TiKV key encoding, placement, scheduling, and
    /// scatter/wait behavior, so the executor rejects this action before any
    /// DDL transaction or catalog mutation.
    SplitRegion {
        /// The record or index keyspace selected by the statement.
        target: SplitTarget,
        /// The split points or range.
        option: SplitOption,
    },
    /// A masking-policy change owned by `ddl_masking_parser.go` rather than
    /// the generic table-DDL grammar.
    MaskingPolicy(Box<crate::AlterMaskingPolicyAction>),
}

/// One source-owned column item in `ALTER TABLE ... ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterOrderItem {
    /// Column path accepted by Go's `parseColumnName`.
    pub column: Vec<String>,
    /// Whether the source requested descending order.
    pub desc: bool,
}

impl AlterOrderItem {
    fn restore_into(&self, out: &mut String) {
        push_name_path(out, &self.column);
        if self.desc {
            out.push_str(" DESC");
        }
    }
}

/// Go's `CompactReplicaKind` enum used by `ALTER TABLE ... COMPACT`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactReplicaKind {
    /// Compact both replica families (the omitted default).
    All,
    /// Compact only TiFlash replicas.
    TiFlash,
    /// Compact only TiKV replicas.
    TiKv,
}

/// Where an `ADD COLUMN` inserts the new column.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnPosition {
    /// No `FIRST`/`AFTER`: appended at the end.
    Default,
    /// `FIRST`: inserted as the new first column.
    First,
    /// `AFTER col`: inserted immediately after the named column.
    After(String),
}

/// Restores an `ALTER TABLE` column position suffix: nothing for `Default`,
/// ` FIRST`, or ` AFTER \`col\``.
fn push_column_position(out: &mut String, position: &ColumnPosition) {
    match position {
        ColumnPosition::Default => {}
        ColumnPosition::First => out.push_str(" FIRST"),
        ColumnPosition::After(col) => {
            out.push_str(" AFTER ");
            out.push_str(&back_quote(col));
        }
    }
}

/// One table-level constraint declared inside a `CREATE TABLE`'s column-list
/// parens.
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    /// Every Go non-foreign-key index constraint, with class/options/key
    /// parts retained as one source-shaped payload.
    Index(IndexConstraintDefinition),
    /// `[CONSTRAINT [name]] CHECK (expr) [[NOT] ENFORCED]` — `enforced`
    /// defaults to `true` when neither keyword is written, matching the Go
    /// AST's restore (confirmed via `godump restore`, not assumed). Not
    /// enforced by this executor even when `enforced` is `true`: real
    /// TiDB itself only enforces `CHECK` when the GLOBAL-scope
    /// `tidb_enable_check_constraint` system variable is on, which
    /// defaults OFF — this seed executor has no notion of session/global
    /// variables at all, so "parsed and restored, never enforced" already
    /// matches TiDB's own out-of-the-box default behavior.
    Check(CheckConstraintDefinition),
    /// Full Go `FOREIGN KEY` constraint payload, including the asymmetric
    /// leading `CONSTRAINT` keyword even when unnamed.
    ForeignKey(ForeignKeyConstraintDefinition),
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for DropIndexAlgorithm {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Inplace => {}
            Self::Copy => {}
            Self::Instant => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for DropIndexLock {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::Shared => {}
            Self::Exclusive => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for DropIndexStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            is_hypo,
            if_exists,
            name,
            table,
            algorithm,
            lock,
        } = self;
        if let Some(value) = algorithm.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if let Some(value) = lock.as_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = is_hypo;
        let _ = if_exists;
        let _ = name;
        let _ = table;
        let _ = algorithm;
        let _ = lock;
        visitor.leave(self)
    }
}

impl crate::Visitable for RenameTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { pairs } = self;
        let _ = pairs;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            temporary,
            if_exists,
            names,
        } = self;
        if !crate::Visitable::accept(temporary, visitor) {
            return false;
        }
        let _ = temporary;
        let _ = if_exists;
        let _ = names;
        visitor.leave(self)
    }
}

impl crate::Visitable for DropTemporary {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::None => {}
            Self::Local => {}
            Self::Global => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for IndexPart {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Column {
                name,
                prefix_len,
                desc,
            } => {
                let _ = name;
                let _ = prefix_len;
                let _ = desc;
            }
            Self::Expr { expr, desc } => {
                if !crate::Visitable::accept(expr, visitor) {
                    return false;
                }
                let _ = expr;
                let _ = desc;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for CreateIndexStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            kind,
            if_not_exists,
            name,
            table,
            parts,
            options,
            online,
        } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        for value in parts.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        if !crate::Visitable::accept(options, visitor) {
            return false;
        }
        if !crate::Visitable::accept(online, visitor) {
            return false;
        }
        let _ = kind;
        let _ = if_not_exists;
        let _ = name;
        let _ = table;
        let _ = parts;
        let _ = options;
        let _ = online;
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterTableStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, actions } = self;
        for value in actions.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = name;
        let _ = actions;
        visitor.leave(self)
    }
}

impl crate::Visitable for SplitOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::By(field_0) => {
                for value in field_0.iter_mut() {
                    for value in value.iter_mut() {
                        if !crate::Visitable::accept(value, visitor) {
                            return false;
                        }
                    }
                }
                let _ = field_0;
            }
            Self::Between {
                lower,
                upper,
                regions,
            } => {
                for value in lower.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                for value in upper.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = lower;
                let _ = upper;
                let _ = regions;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SplitTarget {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Table => {}
            Self::PrimaryKey => {}
            Self::Index(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for SplitRegionStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            region_for,
            partition_syntax,
            table,
            partitions,
            index,
            option,
        } = self;
        if !crate::Visitable::accept(option, visitor) {
            return false;
        }
        let _ = region_for;
        let _ = partition_syntax;
        let _ = table;
        let _ = partitions;
        let _ = index;
        let _ = option;
        visitor.leave(self)
    }
}

impl crate::Visitable for AttributesSpec {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { attributes } = self;
        let _ = attributes;
        visitor.leave(self)
    }
}

impl crate::Visitable for StatsOptionsSpec {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { options } = self;
        let _ = options;
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterTableAlgorithm {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterTableAction {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::RemoveTtl(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Cache(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SetTableOptions { options } => {
                for value in options.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = options;
            }
            Self::ConvertCharacterSet { charset, collation } => {
                let _ = charset;
                let _ = collation;
            }
            Self::SetAttributes(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SetStatsOptions(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Algorithm(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ReadOnly(field_0)
            | Self::SecondaryLoad(field_0)
            | Self::TablespaceImport(field_0) => {
                let _ = field_0;
            }
            Self::Force => {}
            Self::AddStatistics {
                if_not_exists,
                name,
                stats_type,
                columns,
            } => {
                if !crate::Visitable::accept(stats_type, visitor) {
                    return false;
                }
                let _ = if_not_exists;
                let _ = name;
                let _ = stats_type;
                let _ = columns;
            }
            Self::DropStatistics { if_exists, name } => {
                let _ = if_exists;
                let _ = name;
            }
            Self::WithValidation => {}
            Self::WithoutValidation => {}
            Self::SetKeysEnabled(field_0) => {
                let _ = field_0;
            }
            Self::AddColumn {
                if_not_exists,
                column,
                position,
            } => {
                if !crate::Visitable::accept(column, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(position, visitor) {
                    return false;
                }
                let _ = if_not_exists;
                let _ = column;
                let _ = position;
            }
            Self::AddColumns {
                if_not_exists,
                columns,
                constraints,
            } => {
                for value in columns.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                for value in constraints.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = if_not_exists;
                let _ = columns;
                let _ = constraints;
            }
            Self::DropColumn { if_exists, name } => {
                let _ = if_exists;
                let _ = name;
            }
            Self::DropPrimaryKey(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropIndex { if_exists, name } => {
                let _ = if_exists;
                let _ = name;
            }
            Self::DropForeignKey(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::DropCheck(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Lock(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterIndexVisibility(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterCheck(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AlterColumnDefault(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RenameIndex(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RenameColumn(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ModifyColumn {
                if_exists,
                column,
                position,
            } => {
                if !crate::Visitable::accept(column, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(position, visitor) {
                    return false;
                }
                let _ = if_exists;
                let _ = column;
                let _ = position;
            }
            Self::ChangeColumn {
                if_exists,
                old_name,
                column,
                position,
            } => {
                if !crate::Visitable::accept(column, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(position, visitor) {
                    return false;
                }
                let _ = if_exists;
                let _ = old_name;
                let _ = column;
                let _ = position;
            }
            Self::OrderByColumns { items } => {
                for value in items.iter_mut() {
                    if !crate::Visitable::accept(value, visitor) {
                        return false;
                    }
                }
                let _ = items;
            }
            Self::RenameTable { new_name } => {
                let _ = new_name;
            }
            Self::AddIndexConstraint(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AddForeignKey(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::AddCheck(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Partition(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::SetTiFlashReplica {
                hypo,
                count,
                labels,
            } => {
                let _ = hypo;
                let _ = count;
                let _ = labels;
            }
            Self::Compact {
                partitions,
                replica_kind,
            } => {
                if !crate::Visitable::accept(replica_kind, visitor) {
                    return false;
                }
                let _ = partitions;
                let _ = replica_kind;
            }
            Self::SplitRegion { target, option } => {
                if !crate::Visitable::accept(target, visitor) {
                    return false;
                }
                if !crate::Visitable::accept(option, visitor) {
                    return false;
                }
                let _ = target;
                let _ = option;
            }
            Self::MaskingPolicy(field_0) => {
                if !crate::Visitable::accept(field_0.as_mut(), visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for AlterOrderItem {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { column, desc } = self;
        let _ = column;
        let _ = desc;
        visitor.leave(self)
    }
}

impl crate::Visitable for CompactReplicaKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::All => {}
            Self::TiFlash => {}
            Self::TiKv => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for ColumnPosition {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Default => {}
            Self::First => {}
            Self::After(field_0) => {
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for TableConstraint {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Index(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::Check(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::ForeignKey(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
        }
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS

#[cfg(test)]
mod tests {
    use super::{DropIndexAlgorithm, DropIndexLock, DropIndexStmt};

    fn restore(statement: DropIndexStmt) -> String {
        let mut sql = String::new();
        statement.restore_into(&mut sql);
        sql
    }

    #[test]
    fn drop_index_restore_matches_go_canonical_option_order() {
        assert_eq!(
            restore(DropIndexStmt {
                is_hypo: false,
                if_exists: true,
                name: "idx`name".to_owned(),
                table: vec!["app".to_owned(), "orders".to_owned()],
                algorithm: Some(DropIndexAlgorithm::Inplace),
                lock: Some(DropIndexLock::Exclusive),
            }),
            "DROP INDEX IF EXISTS `idx``name` ON `app`.`orders` ALGORITHM = INPLACE LOCK = EXCLUSIVE"
        );
        assert_eq!(
            restore(DropIndexStmt {
                is_hypo: false,
                if_exists: false,
                name: "idx".to_owned(),
                table: vec!["t".to_owned()],
                algorithm: None,
                lock: Some(DropIndexLock::Shared),
            }),
            "DROP INDEX `idx` ON `t` LOCK = SHARED"
        );
    }
}
