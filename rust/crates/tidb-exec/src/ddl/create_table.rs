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

//! `CREATE TABLE` validation and catalog construction.
//!
//! This is the in-memory counterpart of `pkg/ddl/executor.go:CreateTable` and
//! `pkg/ddl/create_table.go:BuildTableInfoWithStmt`/
//! `checkTableInfoValidWithStmt`: preflight static capability gaps before the
//! DDL implicit commit, preserve source-ordered catalog resolution afterward,
//! then publish the completed table atomically.

use tidb_ast::{
    ColumnOption, CreateTableStmt, IndexConstraintKind, IndexPart, TableConstraint, TableOption,
};

use super::index::add_index_metadata;
use super::table_capability::preflight_create_table;
use crate::catalog::{table_key, AutoIncrementColumn, ForeignKey, IndexColumn};
use crate::{Database, ExecError, Table};

pub(super) struct PreparedTable {
    name: String,
    table: Table,
    auto_increment_start: Option<u64>,
}

pub(super) struct CreateTablePreflight {
    auto_increment: Option<(AutoIncrementColumn, u64)>,
}

fn is_unique_index(kind: IndexConstraintKind) -> bool {
    matches!(
        kind,
        IndexConstraintKind::PrimaryKey
            | IndexConstraintKind::Unique
            | IndexConstraintKind::UniqueKey
            | IndexConstraintKind::UniqueIndex
    )
}

fn is_primary_key(kind: IndexConstraintKind) -> bool {
    kind == IndexConstraintKind::PrimaryKey
}

fn plain_column_names(
    parts: &[IndexPart],
    feature: &'static str,
) -> Result<Vec<String>, ExecError> {
    parts
        .iter()
        .map(|part| match part {
            IndexPart::Column {
                name,
                prefix_len: None,
                desc: false,
            } => Ok(name.clone()),
            IndexPart::Column { .. } | IndexPart::Expr { .. } => {
                Err(ExecError::Unsupported(feature))
            }
        })
        .collect()
}

fn auto_increment_schema(
    table: &CreateTableStmt,
) -> Result<Option<(AutoIncrementColumn, u64)>, ExecError> {
    let columns: Vec<usize> = table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            column
                .options
                .contains(&ColumnOption::AutoIncrement)
                .then_some(index)
        })
        .collect();
    if columns.len() > 1 {
        return Err(ExecError::Unsupported("multiple AUTO_INCREMENT columns"));
    }
    let Some(column) = columns.first().copied() else {
        return Ok(None);
    };
    let ty = &table.columns[column].ty;
    if !matches!(ty.name.as_str(), "INT" | "INTEGER" | "BIGINT") {
        return Err(ExecError::Unsupported("AUTO_INCREMENT column type"));
    }
    let start = table
        .table_options
        .iter()
        .find_map(|option| match option {
            TableOption::AutoIncrement(value) => Some(value),
            _ => None,
        })
        .map(|value| {
            value
                .parse::<u64>()
                // `pkg/ddl/create_table.go` stores this AST UInt through the
                // legacy signed `TableInfo.AutoIncID`. Values above MaxInt64
                // become non-positive, so allocator creation starts at 1.
                .map(|value| {
                    if value == 0 || value > i64::MAX as u64 {
                        1
                    } else {
                        value
                    }
                })
                .map_err(|_| ExecError::Unsupported("AUTO_INCREMENT start value"))
        })
        .transpose()?
        .unwrap_or(1);
    Ok(Some((AutoIncrementColumn { column }, start)))
}

/// Performs exactly the static capability checks that TiDB reaches before
/// this executor's DDL implicit-commit boundary. Name resolution and index
/// publication checks intentionally stay in [`prepare`]: the prior executor
/// performed those after commit, and transaction/error ordering is observable.
pub(super) fn preflight(table: &CreateTableStmt) -> Result<CreateTablePreflight, ExecError> {
    preflight_create_table(table)?;

    Ok(CreateTablePreflight {
        auto_increment: auto_increment_schema(table)?,
    })
}

/// Builds the physical catalog value after the DDL coordinator crosses the
/// implicit-commit boundary. Publication remains a separate infallible step,
/// so a build error cannot leave a half-created table.
pub(super) fn prepare(
    table: &CreateTableStmt,
    preflight: CreateTablePreflight,
) -> Result<PreparedTable, ExecError> {
    let name = table_key(&table.name);
    let auto_increment = preflight.auto_increment;
    let cols: Vec<String> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let resolve = |names: &[String]| -> Result<Vec<usize>, ExecError> {
        names
            .iter()
            .map(|name| {
                cols.iter()
                    .position(|column| column.eq_ignore_ascii_case(name))
                    .ok_or_else(|| ExecError::UnknownColumn(name.clone()))
            })
            .collect()
    };
    let resolve_key_parts = |parts: &[IndexPart]| -> Result<Vec<usize>, ExecError> {
        parts
            .iter()
            .map(|part| match part {
                IndexPart::Column {
                    name,
                    prefix_len: None,
                    desc: false,
                } => cols
                    .iter()
                    .position(|column| column.eq_ignore_ascii_case(name))
                    .ok_or_else(|| ExecError::UnknownColumn(name.clone())),
                IndexPart::Column { .. } => Err(ExecError::Unsupported(
                    "PRIMARY/UNIQUE key prefix or direction",
                )),
                IndexPart::Expr { .. } => {
                    Err(ExecError::Unsupported("functional PRIMARY/UNIQUE key"))
                }
            })
            .collect()
    };

    let mut key_groups: Vec<Vec<usize>> = Vec::new();
    let table_primary_key =
        table
            .table_constraints
            .iter()
            .find_map(|constraint| match constraint {
                TableConstraint::Index(index) if is_primary_key(index.kind) => Some(index),
                _ => None,
            });
    if let Some(primary_key) = table_primary_key {
        key_groups.push(resolve_key_parts(&primary_key.parts)?);
    } else if let Some(index) = table.columns.iter().position(|column| {
        column
            .options
            .iter()
            .any(ColumnOption::is_inline_primary_key)
    }) {
        key_groups.push(vec![index]);
    }
    for (index, column) in table.columns.iter().enumerate() {
        if column
            .options
            .iter()
            .any(ColumnOption::is_inline_unique_key)
        {
            key_groups.push(vec![index]);
        }
    }
    for constraint in &table.table_constraints {
        if let TableConstraint::Index(index) = constraint {
            if is_unique_index(index.kind) && !is_primary_key(index.kind) {
                key_groups.push(resolve_key_parts(&index.parts)?);
            }
        }
    }

    let foreign_keys: Vec<ForeignKey> = table
        .table_constraints
        .iter()
        .filter_map(|constraint| match constraint {
            TableConstraint::ForeignKey(foreign_key) => Some(foreign_key),
            _ => None,
        })
        .map(|foreign_key| {
            let table = foreign_key
                .reference
                .table
                .as_ref()
                .expect("preflight requires a foreign-key reference table");
            let reference_parts = foreign_key
                .reference
                .parts
                .as_deref()
                .expect("preflight requires foreign-key reference parts");
            Ok(ForeignKey {
                local_cols: resolve(&plain_column_names(
                    &foreign_key.parts,
                    "advanced FOREIGN KEY",
                )?)?,
                ref_table: table_key(table),
                ref_cols: plain_column_names(reference_parts, "advanced FOREIGN KEY")?,
                on_delete: foreign_key.reference.on_delete,
                on_update: foreign_key.reference.on_update,
            })
        })
        .collect::<Result<_, ExecError>>()?;

    let mut indexes = Vec::new();
    let mut add_named_key = |requested_name: Option<&str>, parts: &[IndexPart], unique: bool| {
        let columns = parts
            .iter()
            .map(|part| match part {
                IndexPart::Column {
                    name,
                    prefix_len,
                    desc,
                } => cols
                    .iter()
                    .position(|column| column.eq_ignore_ascii_case(name))
                    .map(|column| IndexColumn {
                        column,
                        prefix_len: *prefix_len,
                        desc: *desc,
                    })
                    .ok_or_else(|| ExecError::UnknownColumn(name.clone())),
                IndexPart::Expr { .. } => {
                    Err(ExecError::Unsupported("functional PRIMARY/UNIQUE key"))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        add_index_metadata(&mut indexes, requested_name, columns, unique, &cols)
    };
    if let Some(primary_key) = table_primary_key {
        add_named_key(Some("PRIMARY"), &primary_key.parts, true)?;
    } else if let Some(column) = table.columns.iter().find(|column| {
        column
            .options
            .iter()
            .any(ColumnOption::is_inline_primary_key)
    }) {
        add_named_key(
            Some("PRIMARY"),
            &[IndexPart::Column {
                name: column.name.clone(),
                prefix_len: None,
                desc: false,
            }],
            true,
        )?;
    }
    for column in &table.columns {
        if column
            .options
            .iter()
            .any(ColumnOption::is_inline_unique_key)
        {
            add_named_key(
                None,
                &[IndexPart::Column {
                    name: column.name.clone(),
                    prefix_len: None,
                    desc: false,
                }],
                true,
            )?;
        }
    }
    for constraint in &table.table_constraints {
        if let TableConstraint::Index(index) = constraint {
            if is_primary_key(index.kind) {
                continue;
            }
            add_named_key(
                index.name.as_deref(),
                &index.parts,
                is_unique_index(index.kind),
            )?;
        }
    }

    let col_types = table
        .columns
        .iter()
        .map(|column| column.ty.clone())
        .collect();
    let col_defaults = table
        .columns
        .iter()
        .map(|column| {
            column.options.iter().find_map(|option| match option {
                ColumnOption::Default(expr) => Some(expr.clone()),
                _ => None,
            })
        })
        .collect();
    let auto_increment_start = auto_increment.map(|(_, start)| start);
    let table = Table {
        cols,
        col_types,
        col_defaults,
        auto_increment: auto_increment.map(|(column, _)| column),
        rows: Vec::new(),
        key_groups,
        indexes,
        foreign_keys,
    };
    Ok(PreparedTable {
        name,
        table,
        auto_increment_start,
    })
}

impl Database {
    /// Publishes an already-validated table. This step is infallible, so no
    /// invalid declaration can half-mutate the catalog after implicit commit.
    pub(super) fn publish_table(&mut self, prepared: PreparedTable) {
        let PreparedTable {
            name,
            table,
            auto_increment_start,
        } = prepared;
        self.tables.insert(name.clone(), table);
        if let Some(start) = auto_increment_start {
            self.auto_increment_next
                .borrow_mut()
                .insert(name, Some(start));
        }
    }
}
