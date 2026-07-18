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

//! DDL index metadata construction.
//!
//! The source owner is `pkg/ddl/index.go`: `checkAndBuildIndexInfo` resolves
//! index parts, rejects duplicate names, and publishes one catalog entry only
//! after validation. This in-memory executor keeps that same validation-before-
//! mutation boundary for the subset its table-scan engine can represent.

use tidb_ast::{IndexConstraintDefinition, IndexConstraintKind, IndexOptions, IndexPart};

use crate::catalog::{IndexColumn, IndexMetadata};
use crate::{ExecError, Table};

/// Resolves a source-shaped table/ALTER index definition into the limited
/// logical catalog metadata the seed executor can faithfully own.
///
/// Parser and AST retain every Go option.  Execution deliberately accepts
/// only ordinary/unique/primary named-column definitions with no extra index
/// metadata, rather than dropping unsupported schema behavior after an
/// implicit commit.
pub(super) fn prepare_index_definition(
    table: &Table,
    definition: &IndexConstraintDefinition,
) -> Result<IndexMetadata, ExecError> {
    let unique = match definition.kind {
        IndexConstraintKind::Key | IndexConstraintKind::Index => false,
        IndexConstraintKind::PrimaryKey
        | IndexConstraintKind::Unique
        | IndexConstraintKind::UniqueKey
        | IndexConstraintKind::UniqueIndex => true,
        IndexConstraintKind::Fulltext => return Err(ExecError::Unsupported("FULLTEXT INDEX")),
        IndexConstraintKind::Vector => return Err(ExecError::Unsupported("VECTOR INDEX")),
        IndexConstraintKind::Columnar => return Err(ExecError::Unsupported("COLUMNAR INDEX")),
    };
    let mut unsupported_options = definition.options.clone();
    // Existing execution treats PRIMARY KEY's clustered/nonclustered physical
    // layout as neutral, while retaining its logical uniqueness contract.
    if definition.kind == IndexConstraintKind::PrimaryKey {
        unsupported_options.primary_key_storage = None;
    }
    if unsupported_options != IndexOptions::default() {
        return Err(ExecError::Unsupported("advanced index options"));
    }
    if unique
        && definition.parts.iter().any(|part| {
            !matches!(
                part,
                IndexPart::Column {
                    prefix_len: None,
                    desc: false,
                    ..
                }
            )
        })
    {
        return Err(ExecError::Unsupported(
            "PRIMARY/UNIQUE key prefix or direction",
        ));
    }

    let columns = definition
        .parts
        .iter()
        .map(|part| match part {
            IndexPart::Column {
                name,
                prefix_len,
                desc,
            } => table
                .cols
                .iter()
                .position(|column| column.eq_ignore_ascii_case(name))
                .map(|column| IndexColumn {
                    column,
                    prefix_len: *prefix_len,
                    desc: *desc,
                })
                .ok_or_else(|| ExecError::UnknownColumn(name.clone())),
            IndexPart::Expr { .. } => Err(ExecError::Unsupported("functional index")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let name = effective_index_name(
        &table.indexes,
        definition.name.as_deref(),
        &columns,
        &table.cols,
    );
    if table
        .indexes
        .iter()
        .any(|existing| existing.name.eq_ignore_ascii_case(&name))
    {
        return Err(ExecError::DuplicateIndex(name));
    }
    Ok(IndexMetadata {
        name,
        columns,
        unique,
    })
}

/// Whether this source-shaped constraint participates in the seed catalog's
/// duplicate-key groups after metadata construction.
pub(super) fn index_requires_unique_key_group(definition: &IndexConstraintDefinition) -> bool {
    matches!(
        definition.kind,
        IndexConstraintKind::PrimaryKey
            | IndexConstraintKind::Unique
            | IndexConstraintKind::UniqueKey
            | IndexConstraintKind::UniqueIndex
    )
}

/// Resolves the plain column positions used by the seed's uniqueness checks.
/// Callers use it only after [`prepare_index_definition`] has accepted a
/// unique/primary constraint, so prefix, direction, and expression parts are
/// already outside the capability boundary.
pub(super) fn index_key_group(
    table: &Table,
    definition: &IndexConstraintDefinition,
) -> Result<Vec<usize>, ExecError> {
    definition
        .parts
        .iter()
        .map(|part| match part {
            IndexPart::Column {
                name,
                prefix_len: None,
                desc: false,
            } => table
                .cols
                .iter()
                .position(|column| column.eq_ignore_ascii_case(name))
                .ok_or_else(|| ExecError::UnknownColumn(name.clone())),
            _ => Err(ExecError::Unsupported(
                "PRIMARY/UNIQUE key prefix or direction",
            )),
        })
        .collect()
}

pub(super) fn add_index_metadata(
    indexes: &mut Vec<IndexMetadata>,
    requested_name: Option<&str>,
    columns: Vec<IndexColumn>,
    unique: bool,
    column_names: &[String],
) -> Result<(), ExecError> {
    let name = effective_index_name(indexes, requested_name, &columns, column_names);
    if indexes
        .iter()
        .any(|existing| existing.name.eq_ignore_ascii_case(&name))
    {
        return Err(ExecError::DuplicateIndex(name));
    }
    indexes.push(IndexMetadata {
        name,
        columns,
        unique,
    });
    Ok(())
}

fn effective_index_name(
    indexes: &[IndexMetadata],
    requested_name: Option<&str>,
    columns: &[IndexColumn],
    column_names: &[String],
) -> String {
    if let Some(name) = requested_name {
        return name.to_string();
    }
    let mut base = columns
        .first()
        .and_then(|part| column_names.get(part.column))
        .cloned()
        .unwrap_or_else(|| "expression_index".to_string());
    let mut suffix = 2;
    if base.eq_ignore_ascii_case("PRIMARY") {
        base = format!("{base}_{suffix}");
        suffix += 1;
    }
    while indexes
        .iter()
        .any(|existing| existing.name.eq_ignore_ascii_case(&base))
    {
        let root = columns
            .first()
            .and_then(|part| column_names.get(part.column))
            .cloned()
            .unwrap_or_else(|| "expression_index".to_string());
        base = format!("{root}_{suffix}");
        suffix += 1;
    }
    base
}
