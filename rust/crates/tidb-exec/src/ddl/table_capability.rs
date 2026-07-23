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

//! Static `CREATE TABLE`/`ALTER TABLE` capability classification.
//!
//! This leaf deliberately owns only checks that must reject before the DDL
//! implicit-commit boundary. Catalog lookup, constraint-name resolution, and
//! physical mutation remain in the coordinator and source-shaped catalog
//! leaves because their source-visible ordering is observable.

use tidb_ast::{
    AlterTableAction, AlterTableStmt, ColumnOption, CreateTableStmt,
    ForeignKeyConstraintDefinition, ForeignKeyMatch, IndexConstraintDefinition,
    IndexConstraintKind, IndexOptions, IndexPart, TableConstraint, TableOption,
};

use super::affinity;
use crate::ExecError;

pub(super) fn has_unimplemented_binary_type(type_name: &str) -> bool {
    type_name.eq_ignore_ascii_case("BINARY") || type_name.eq_ignore_ascii_case("VARBINARY")
}

pub(super) fn has_unimplemented_blob_type(type_name: &str) -> bool {
    matches!(
        type_name.to_ascii_uppercase().as_str(),
        "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB"
    )
}

pub(super) fn has_unimplemented_vector_type(type_name: &str) -> bool {
    type_name.eq_ignore_ascii_case("VECTOR")
}

fn unimplemented_column_option(options: &[ColumnOption]) -> Option<&'static str> {
    options.iter().find_map(|option| match option {
        ColumnOption::ColumnFormat(_) => Some("COLUMN_FORMAT column option"),
        ColumnOption::AutoRandom(_) => Some("AUTO_RANDOM column option"),
        ColumnOption::SecondaryEngineAttribute(_) => {
            Some("SECONDARY_ENGINE_ATTRIBUTE column option")
        }
        ColumnOption::MariaDbRowStart | ColumnOption::MariaDbRowEnd => {
            Some("MariaDB system-versioned column")
        }
        // Go emits a warning that STORAGE is ignored by every storage
        // engine. Keep the typed AST/restoration contract without creating a
        // Rust-only execution failure for an option with no source effect.
        ColumnOption::Storage(_) => None,
        _ => None,
    })
}

fn has_unimplemented_unique_key_parts(parts: &[IndexPart]) -> bool {
    parts.iter().any(|part| {
        !matches!(
            part,
            IndexPart::Column {
                prefix_len: None,
                desc: false,
                ..
            }
        )
    })
}

fn is_primary_key(kind: IndexConstraintKind) -> bool {
    kind == IndexConstraintKind::PrimaryKey
}

fn is_catalog_index_kind(kind: IndexConstraintKind) -> bool {
    matches!(
        kind,
        IndexConstraintKind::PrimaryKey
            | IndexConstraintKind::Key
            | IndexConstraintKind::Index
            | IndexConstraintKind::Unique
            | IndexConstraintKind::UniqueKey
            | IndexConstraintKind::UniqueIndex
    )
}

fn validate_index_constraint(constraint: &IndexConstraintDefinition) -> Result<(), ExecError> {
    if !is_catalog_index_kind(constraint.kind) {
        return Err(ExecError::Unsupported(match constraint.kind {
            IndexConstraintKind::Fulltext => "FULLTEXT INDEX",
            IndexConstraintKind::Vector => "VECTOR INDEX",
            IndexConstraintKind::Columnar => "COLUMNAR INDEX",
            _ => unreachable!("catalog kind handled above"),
        }));
    }
    let mut unsupported_options = constraint.options.clone();
    // The seed catalog has always treated the physical primary-key layout as
    // storage-neutral: it preserves logical primary-key uniqueness and table
    // scans without claiming a clustered TiKV layout. Keep that established
    // capability while every other source option remains explicit metadata
    // the catalog cannot own yet.
    if is_primary_key(constraint.kind) {
        unsupported_options.primary_key_storage = None;
    }
    if unsupported_options != IndexOptions::default() {
        return Err(ExecError::Unsupported("advanced index options"));
    }
    if matches!(
        constraint.kind,
        IndexConstraintKind::PrimaryKey
            | IndexConstraintKind::Unique
            | IndexConstraintKind::UniqueKey
            | IndexConstraintKind::UniqueIndex
    ) && has_unimplemented_unique_key_parts(&constraint.parts)
    {
        return Err(ExecError::Unsupported(
            "PRIMARY/UNIQUE key prefix or direction",
        ));
    }
    Ok(())
}

fn plain_column_names(parts: &[IndexPart], feature: &'static str) -> Result<(), ExecError> {
    for part in parts {
        match part {
            IndexPart::Column {
                prefix_len: None,
                desc: false,
                ..
            } => {}
            IndexPart::Column { .. } | IndexPart::Expr { .. } => {
                return Err(ExecError::Unsupported(feature));
            }
        }
    }
    Ok(())
}

fn validate_foreign_key_constraint(
    foreign_key: &ForeignKeyConstraintDefinition,
) -> Result<(), ExecError> {
    let Some(reference_parts) = foreign_key.reference.parts.as_deref() else {
        return Err(ExecError::Unsupported("advanced FOREIGN KEY"));
    };
    if foreign_key.if_not_exists
        || foreign_key.reference.match_type != ForeignKeyMatch::None
        || foreign_key.reference.table.is_none()
    {
        return Err(ExecError::Unsupported("advanced FOREIGN KEY"));
    }
    plain_column_names(&foreign_key.parts, "advanced FOREIGN KEY")?;
    plain_column_names(reference_parts, "advanced FOREIGN KEY")
}

/// Classifies static `CREATE TABLE` gaps before the DDL implicit commit.
/// Source-ordered name resolution and construction checks remain in
/// `ddl::create_table::prepare`, after the coordinator commits.
pub(super) fn preflight_create_table(table: &CreateTableStmt) -> Result<(), ExecError> {
    if !table.splits.is_empty() {
        // CREATE TABLE SPLIT affects physical key ranges and region placement.
        // This catalog has neither, so reject the complete typed payload
        // before the DDL implicit commit instead of publishing a table whose
        // source-requested split topology was silently erased.
        return Err(ExecError::Unsupported("CREATE TABLE SPLIT"));
    }
    if table.like_table.is_some() {
        return Err(ExecError::Unsupported("CREATE TABLE LIKE"));
    }
    if table.ctas.is_some() {
        // TiDB carries CTAS as one DDL statement with a ResultSetNode and a
        // duplicate-key policy. This seed catalog cannot atomically derive
        // the output schema and materialize that source, so reject the whole
        // typed payload before the implicit DDL commit rather than pretending
        // it were a CREATE followed by a separate INSERT.
        return Err(ExecError::Unsupported("CREATE TABLE AS SELECT"));
    }
    if table.columns.is_empty() {
        // The Go parser permits a bare `CREATE TABLE name`, but no catalog
        // table can be constructed from it without inventing columns. Keep
        // parser compatibility and make the executor boundary explicit.
        return Err(ExecError::Unsupported("CREATE TABLE without columns"));
    }
    if table
        .columns
        .iter()
        .any(|column| !column.qualifier.is_empty())
    {
        // Go accepts qualified names in a CREATE TABLE column definition so
        // the parser can report the executor's invalid-name error later. The
        // compact catalog has only one unqualified column-name slot; reject
        // before the DDL implicit commit instead of silently dropping the
        // schema/table path and publishing a different table contract.
        return Err(ExecError::Unsupported("qualified CREATE TABLE column name"));
    }
    if table.temporary != tidb_ast::CreateTableTemporary::None {
        return Err(ExecError::Unsupported("CREATE TEMPORARY TABLE"));
    }
    if table.partitioning.is_some() {
        // Partitioning changes physical table routing, key layout, catalog
        // metadata and every DML access path. The parser preserves the full
        // source shape, but this single capability boundary rejects it before
        // the DDL implicit commit so the seed catalog can never manufacture a
        // silently non-partitioned table.
        return Err(ExecError::Unsupported("CREATE TABLE PARTITION BY"));
    }
    if let Some(level) = table.table_options.iter().find_map(|option| match option {
        TableOption::Affinity(level) => Some(level.as_str()),
        _ => None,
    }) {
        // AFFINITY changes physical placement and table/partition metadata.
        // The seed catalog owns neither, so reject the typed source payload
        // before the implicit DDL commit rather than creating an ordinary
        // table that silently drops its placement contract.
        let feature = if affinity::normalize_level(level).is_err() {
            "invalid CREATE TABLE AFFINITY"
        } else {
            "CREATE TABLE AFFINITY"
        };
        return Err(ExecError::Unsupported(feature));
    }
    if table.table_options.iter().any(|option| {
        matches!(
            option,
            TableOption::AutoextendSize(_)
                | TableOption::PageChecksum(_)
                | TableOption::PageCompressed(_)
                | TableOption::PageCompressionLevel(_)
                | TableOption::Transactional(_)
                | TableOption::IetfQuotes(_)
                | TableOption::Sequence(_)
                | TableOption::Union(_)
        )
    }) {
        // These MySQL/MariaDB compatibility options are parser-visible and
        // Go emits an ignored-storage-engine warning, but the seed catalog
        // has no metadata or physical-engine model for them. Reject before
        // the DDL implicit commit instead of silently dropping the contract.
        return Err(ExecError::Unsupported(
            "CREATE TABLE compatibility/MERGE options",
        ));
    }
    for constraint in &table.table_constraints {
        match constraint {
            TableConstraint::Index(index) => validate_index_constraint(index)?,
            TableConstraint::ForeignKey(foreign_key) => {
                validate_foreign_key_constraint(foreign_key)?
            }
            TableConstraint::Check(_) => {}
        }
    }
    if table.columns.iter().any(|column| {
        column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::Reference(_)))
    }) {
        // Go's parser retains the shared ReferenceDef on the column AST.
        // This seed catalog has no column-reference owner/name model, so
        // reject before DDL commit rather than accepting then dropping its
        // semantics.
        return Err(ExecError::Unsupported("column-level REFERENCES"));
    }
    if table.columns.iter().any(|column| {
        column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::Generated { .. }))
    }) {
        // A generated column needs catalog expression metadata plus write and
        // backfill evaluation. Reject before the DDL implicit commit rather
        // than creating an ordinary nullable column and silently losing that
        // contract.
        return Err(ExecError::Unsupported("generated columns"));
    }
    if table.columns.iter().any(|column| {
        column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::OnUpdate(_)))
    }) {
        return Err(ExecError::Unsupported("ON UPDATE columns"));
    }
    if table.columns.iter().any(|column| {
        column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::Check(_)))
    }) {
        // CHECK is structural DDL metadata plus write-time validation. The
        // seed catalog owns neither, so preserving the parser payload is not
        // enough to make execution safe. Refuse before the DDL commit rather
        // than creating a table whose column contract was silently erased.
        return Err(ExecError::Unsupported("column-level CHECK"));
    }
    if let Some(feature) = table
        .columns
        .iter()
        .find_map(|column| unimplemented_column_option(&column.options))
    {
        return Err(ExecError::Unsupported(feature));
    }
    if table.columns.iter().any(|column| column.ty.name == "JSON") {
        return Err(ExecError::Unsupported("JSON column type"));
    }
    if table
        .columns
        .iter()
        .any(|column| has_unimplemented_vector_type(&column.ty.name))
    {
        return Err(ExecError::Unsupported("VECTOR column type"));
    }
    if table
        .columns
        .iter()
        .any(|column| has_unimplemented_binary_type(&column.ty.name))
    {
        return Err(ExecError::Unsupported("BINARY/VARBINARY column type"));
    }
    if table
        .columns
        .iter()
        .any(|column| has_unimplemented_blob_type(&column.ty.name))
    {
        return Err(ExecError::Unsupported("BLOB column type"));
    }
    Ok(())
}

/// Selects the only `ALTER TABLE` action this seed may execute and rejects
/// every static capability gap before catalog or transaction mutation.
/// `pkg/ddl/executor.go:AlterTable` resolves and validates the complete spec
/// list before applying its jobs; this typed boundary makes partial-prefix
/// execution equally impossible here.
pub(super) fn executable_alter_action(
    statement: &AlterTableStmt,
) -> Result<&AlterTableAction, ExecError> {
    let action = match statement.actions.as_slice() {
        [action] => action,
        [] => return Err(ExecError::Unsupported("ALTER TABLE without action")),
        _ => return Err(ExecError::Unsupported("ALTER TABLE multiple actions")),
    };

    if let AlterTableAction::AddColumn { column, .. }
    | AlterTableAction::ModifyColumn { column, .. }
    | AlterTableAction::ChangeColumn { column, .. } = action
    {
        if column.ty.name == "JSON" {
            return Err(ExecError::Unsupported("JSON column type"));
        }
        if has_unimplemented_vector_type(&column.ty.name) {
            return Err(ExecError::Unsupported("VECTOR column type"));
        }
        if has_unimplemented_binary_type(&column.ty.name) {
            return Err(ExecError::Unsupported("BINARY/VARBINARY column type"));
        }
        if has_unimplemented_blob_type(&column.ty.name) {
            return Err(ExecError::Unsupported("BLOB column type"));
        }
        if column.options.contains(&ColumnOption::AutoIncrement) {
            return Err(ExecError::Unsupported("ALTER TABLE AUTO_INCREMENT"));
        }
        if column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::Reference(_)))
        {
            return Err(ExecError::Unsupported("column-level REFERENCES"));
        }
        if column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::Generated { .. }))
        {
            return Err(ExecError::Unsupported("generated columns"));
        }
        if column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::OnUpdate(_)))
        {
            return Err(ExecError::Unsupported("ON UPDATE columns"));
        }
        if column
            .options
            .iter()
            .any(|option| matches!(option, ColumnOption::Check(_)))
        {
            // The catalog has no CHECK metadata or write-time evaluator. A
            // column CHECK cannot be accepted then discarded: it changes
            // later write validity and SHOW CREATE output. Reject before the
            // ALTER implicit-commit edge.
            return Err(ExecError::Unsupported("column-level CHECK"));
        }
        if let Some(feature) = unimplemented_column_option(&column.options) {
            return Err(ExecError::Unsupported(feature));
        }
    }

    let unsupported = match action {
        AlterTableAction::ModifyColumn { column, .. } if !column.qualifier.is_empty() => {
            Some("ALTER TABLE qualified MODIFY COLUMN")
        }
        AlterTableAction::ModifyColumn { column, .. }
        | AlterTableAction::ChangeColumn { column, .. }
            if matches!(column.ty.name.to_ascii_uppercase().as_str(), "ENUM" | "SET") =>
        {
            // The compact catalog stores text-like values only; it does not
            // enforce ENUM ordinals/SET membership or conversion errors.
            // Reject the typed ALTER before the implicit-commit boundary
            // rather than mutating a column while silently dropping those
            // source semantics.
            Some("ALTER TABLE ENUM/SET column type")
        }
        AlterTableAction::OrderByColumns { .. } => Some("ALTER TABLE ORDER BY"),
        AlterTableAction::AddColumns { .. } => Some("ALTER TABLE ADD COLUMN list"),
        AlterTableAction::DropPrimaryKey(_) => Some("ALTER TABLE DROP PRIMARY KEY"),
        AlterTableAction::DropIndex { .. } => Some("ALTER TABLE DROP INDEX"),
        AlterTableAction::DropForeignKey(_) => Some("ALTER TABLE DROP FOREIGN KEY"),
        AlterTableAction::DropCheck(_) => Some("ALTER TABLE DROP CHECK"),
        AlterTableAction::Lock(_) => Some("ALTER TABLE LOCK"),
        AlterTableAction::Cache(_) => Some("ALTER TABLE CACHE/NOCACHE"),
        AlterTableAction::RemoveTtl(_) => Some("ALTER TABLE REMOVE TTL"),
        AlterTableAction::AlterIndexVisibility(_) => Some("ALTER TABLE ALTER INDEX VISIBILITY"),
        AlterTableAction::AlterCheck(_) => Some("ALTER TABLE ALTER CHECK"),
        AlterTableAction::AlterColumnDefault(_) => Some("ALTER TABLE ALTER COLUMN DEFAULT"),
        AlterTableAction::RenameColumn(_) => Some("ALTER TABLE RENAME COLUMN"),
        AlterTableAction::RenameIndex(_) => Some("ALTER TABLE RENAME INDEX"),
        AlterTableAction::AddCheck(_) => Some("ALTER TABLE ADD CHECK"),
        AlterTableAction::SplitRegion { .. } => Some("ALTER TABLE SPLIT"),
        AlterTableAction::SetTiFlashReplica { .. } => Some("ALTER TABLE SET TIFLASH REPLICA"),
        AlterTableAction::Compact { .. } => Some("ALTER TABLE COMPACT"),
        AlterTableAction::MaskingPolicy(_) => Some("ALTER TABLE MASKING POLICY"),
        AlterTableAction::WithValidation | AlterTableAction::WithoutValidation => {
            Some("ALTER TABLE VALIDATION")
        }
        AlterTableAction::SetTableOptions { options }
            if matches!(options.as_slice(), [TableOption::Comment(_)]) =>
        {
            Some("ALTER TABLE COMMENT")
        }
        AlterTableAction::SetTableOptions { options }
            if options
                .iter()
                .all(|option| matches!(option, TableOption::EngineAttribute(_))) =>
        {
            // The catalog has no engine-attribute metadata. Reject the full
            // option list before ALTER's implicit-commit boundary rather than
            // mutating a table while silently dropping SHOW CREATE state.
            Some("ALTER TABLE ENGINE_ATTRIBUTE")
        }
        AlterTableAction::SetTableOptions { options }
            if matches!(options.as_slice(), [TableOption::AutoIncrement(_)]) =>
        {
            Some("ALTER TABLE AUTO_INCREMENT")
        }
        AlterTableAction::SetTableOptions { options }
            if matches!(
                options.as_slice(),
                [TableOption::AutoIdCache(_)
                    | TableOption::AutoRandomBase(_)
                    | TableOption::ForceAutoRandomBase(_)]
            ) =>
        {
            Some("ALTER TABLE AUTO_ID_CACHE/AUTO_RANDOM_BASE")
        }
        AlterTableAction::SetTableOptions { options }
            if matches!(options.as_slice(), [TableOption::ShardRowIdBits(_)]) =>
        {
            Some("ALTER TABLE SHARD_ROW_ID_BITS")
        }
        AlterTableAction::SetTableOptions { options }
            if matches!(options.as_slice(), [TableOption::PlacementPolicy(_)]) =>
        {
            Some("ALTER TABLE PLACEMENT POLICY")
        }
        AlterTableAction::SetTableOptions { options }
            if matches!(options.as_slice(), [TableOption::Affinity(_)]) =>
        {
            Some("ALTER TABLE AFFINITY")
        }
        AlterTableAction::SetTableOptions { options }
            if options.iter().all(|option| {
                matches!(
                    option,
                    TableOption::Ttl { .. }
                        | TableOption::TtlEnable(_)
                        | TableOption::TtlJobInterval(_)
                )
            }) =>
        {
            Some("ALTER TABLE TTL options")
        }
        AlterTableAction::SetTableOptions { .. } | AlterTableAction::ConvertCharacterSet { .. } => {
            Some("ALTER TABLE table options")
        }
        action => crate::partition::unsupported_alter_action(action),
    };
    if let Some(feature) = unsupported {
        return Err(ExecError::Unsupported(feature));
    }
    Ok(action)
}
