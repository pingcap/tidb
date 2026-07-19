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

//! Immutable configured-table catalog for the bounded read-only SQL node.
//!
//! This is deliberately not a dynamic TiDB `InfoSchema`. It owns no schema
//! lease, version, DDL visibility, temporary tables, partitions, or views.

use std::{collections::HashMap, error::Error, fmt};

use crate::read_only_scan::{fold_identifier, ConfiguredTable, ReadOnlyScanError};

/// A configured catalog cannot be constructed without one unambiguous stable
/// identity for every table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredCatalogError {
    /// One table violates the shared single-table admission contract.
    InvalidTable {
        /// Source-order position of the rejected table.
        index: usize,
        /// Exact shared validation failure.
        error: ReadOnlyScanError,
    },
    /// Two tables have the same case-insensitive schema and table name.
    DuplicateTableName {
        /// Original schema name from the later descriptor.
        schema: String,
        /// Original table name from the later descriptor.
        table: String,
    },
    /// Two tables have the same physical TiDB table ID.
    DuplicateTableId(i64),
}

impl fmt::Display for ConfiguredCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTable { index, error } => {
                write!(
                    formatter,
                    "invalid configured table at index {index}: {error}"
                )
            }
            Self::DuplicateTableName { schema, table } => write!(
                formatter,
                "duplicate configured table name: {schema}.{table}"
            ),
            Self::DuplicateTableId(id) => write!(formatter, "duplicate configured table ID: {id}"),
        }
    }
}

impl Error for ConfiguredCatalogError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidTable { error, .. } => Some(error),
            Self::DuplicateTableName { .. } | Self::DuplicateTableId(_) => None,
        }
    }
}

/// Failure to resolve a configured table name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfiguredTableLookupError {
    /// No configured table has the requested visible name.
    UnknownTable(String),
    /// An unqualified name exists in more than one configured schema.
    AmbiguousTable(String),
}

impl fmt::Display for ConfiguredTableLookupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTable(name) => write!(formatter, "unknown configured table: {name}"),
            Self::AmbiguousTable(name) => write!(formatter, "ambiguous configured table: {name}"),
        }
    }
}

impl Error for ConfiguredTableLookupError {}

/// An immutable, source-ordered set of validated configured tables.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredCatalog {
    tables: Vec<ConfiguredTable>,
    by_name: HashMap<(String, String), usize>,
    by_id: HashMap<i64, usize>,
    by_unqualified_name: HashMap<String, Vec<usize>>,
}

impl ConfiguredCatalog {
    /// Validates and indexes configured tables without changing source order.
    pub fn new(
        tables: impl IntoIterator<Item = ConfiguredTable>,
    ) -> Result<Self, ConfiguredCatalogError> {
        let tables = tables.into_iter().collect::<Vec<_>>();
        let mut by_name = HashMap::with_capacity(tables.len());
        let mut by_id = HashMap::with_capacity(tables.len());
        let mut by_unqualified_name: HashMap<String, Vec<usize>> = HashMap::new();

        for (index, table) in tables.iter().enumerate() {
            table
                .validate()
                .map_err(|error| ConfiguredCatalogError::InvalidTable { index, error })?;
            let folded_schema = fold_identifier(table.schema());
            let folded_table = fold_identifier(table.table());
            if by_name
                .insert((folded_schema, folded_table.clone()), index)
                .is_some()
            {
                return Err(ConfiguredCatalogError::DuplicateTableName {
                    schema: table.schema().to_owned(),
                    table: table.table().to_owned(),
                });
            }
            if by_id.insert(table.table_id(), index).is_some() {
                return Err(ConfiguredCatalogError::DuplicateTableId(table.table_id()));
            }
            by_unqualified_name
                .entry(folded_table)
                .or_default()
                .push(index);
        }

        Ok(Self {
            tables,
            by_name,
            by_id,
            by_unqualified_name,
        })
    }

    /// Returns all configured tables in startup/source order.
    #[must_use]
    pub fn tables(&self) -> &[ConfiguredTable] {
        &self.tables
    }

    /// Looks up one table by its stable TiDB table ID.
    #[must_use]
    pub fn table_by_id(&self, table_id: i64) -> Option<&ConfiguredTable> {
        self.by_id.get(&table_id).map(|index| &self.tables[*index])
    }

    /// Looks up one fully qualified table name case-insensitively.
    #[must_use]
    pub fn table_by_name(&self, schema: &str, table: &str) -> Option<&ConfiguredTable> {
        self.by_name
            .get(&(fold_identifier(schema), fold_identifier(table)))
            .map(|index| &self.tables[*index])
    }

    /// Resolves a qualified or unqualified configured table name.
    pub fn resolve_table(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<&ConfiguredTable, ConfiguredTableLookupError> {
        if let Some(schema) = schema {
            return self.table_by_name(schema, table).ok_or_else(|| {
                ConfiguredTableLookupError::UnknownTable(format!("{schema}.{table}"))
            });
        }
        let Some(indices) = self.by_unqualified_name.get(&fold_identifier(table)) else {
            return Err(ConfiguredTableLookupError::UnknownTable(table.to_owned()));
        };
        match indices.as_slice() {
            [index] => Ok(&self.tables[*index]),
            _ => Err(ConfiguredTableLookupError::AmbiguousTable(table.to_owned())),
        }
    }
}
