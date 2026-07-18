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

//! Index-advisor column/index identity from `pkg/planner/indexadvisor/model.go`.
//!
//! This leaf ports the normalized identity constructors, key formatting, and
//! index-prefix relation over owned strings. Catalog lookup and optimizer
//! statistics remain external planner boundaries.

/// A normalized table column identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Column {
    /// Lowercase schema name.
    pub schema_name: String,
    /// Lowercase table name.
    pub table_name: String,
    /// Lowercase column name.
    pub column_name: String,
}

impl Column {
    /// Creates a normalized column identity.
    #[must_use]
    pub fn new(schema_name: &str, table_name: &str, column_name: &str) -> Self {
        Self {
            schema_name: schema_name.to_lowercase(),
            table_name: table_name.to_lowercase(),
            column_name: column_name.to_lowercase(),
        }
    }

    /// Returns the source `schema.table.column` key.
    #[must_use]
    pub fn key(&self) -> String {
        format!(
            "{}.{}.{}",
            self.schema_name, self.table_name, self.column_name
        )
    }
}

/// An index identity and ordered column list.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Index {
    /// Lowercase schema name.
    pub schema_name: String,
    /// Lowercase table name.
    pub table_name: String,
    /// Lowercase index name.
    pub index_name: String,
    /// Ordered columns in the index.
    pub columns: Vec<Column>,
}

impl Index {
    /// Creates an index from schema/table/index names and column names.
    #[must_use]
    pub fn new(schema_name: &str, table_name: &str, index_name: &str, columns: &[&str]) -> Self {
        Self {
            schema_name: schema_name.to_lowercase(),
            table_name: table_name.to_lowercase(),
            index_name: index_name.to_lowercase(),
            columns: columns
                .iter()
                .map(|column| Column::new(schema_name, table_name, column))
                .collect(),
        }
    }

    /// Returns the source `schema.table(col1,col2)` key.
    #[must_use]
    pub fn key(&self) -> String {
        let names = self
            .columns
            .iter()
            .map(|column| column.column_name.as_str())
            .collect::<Vec<_>>();
        format!(
            "{}.{}({})",
            self.schema_name,
            self.table_name,
            names.join(",")
        )
    }

    /// Reports whether `other` is a column-prefix of this index.
    #[must_use]
    pub fn prefix_contains(&self, other: &Self) -> bool {
        self.schema_name == other.schema_name
            && self.table_name == other.table_name
            && self.columns.len() >= other.columns.len()
            && self
                .columns
                .iter()
                .zip(&other.columns)
                .all(|(left, right)| left.column_name == right.column_name)
    }
}
