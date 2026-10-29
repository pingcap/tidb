// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/planner/indexadvisor/model.go`.

use crate::keyset::Keyed;

use tidb_hack::GoToLower;

/// Go `Query`: a statement of the workload.
#[derive(Clone, Debug)]
pub struct Query {
    /// Go `SchemaName`.
    pub schema_name: String,
    /// Go `Text`.
    pub text: String,
    /// Go `Frequency`.
    pub frequency: i64,
}

impl Keyed for Query {
    fn key(&self) -> String {
        self.text.clone()
    }
}

/// Go `Column`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    /// Go `SchemaName`.
    pub schema_name: String,
    /// Go `TableName`.
    pub table_name: String,
    /// Go `ColumnName`.
    pub column_name: String,
}

impl Column {
    /// Go `NewColumn`: every part lower-cased.
    pub(crate) fn new(schema_name: &str, table_name: &str, column_name: &str) -> Self {
        Self {
            schema_name: schema_name.go_to_lower(),
            table_name: table_name.go_to_lower(),
            column_name: column_name.go_to_lower(),
        }
    }
}

impl Keyed for Column {
    fn key(&self) -> String {
        format!(
            "{}.{}.{}",
            self.schema_name, self.table_name, self.column_name
        )
    }
}

/// Go `Index`.
#[derive(Clone, Debug)]
pub struct Index {
    /// Go `SchemaName`.
    pub schema_name: String,
    /// Go `TableName`.
    pub table_name: String,
    /// Go `IndexName`.
    pub index_name: String,
    /// Go `Columns`.
    pub columns: Vec<Column>,
}

impl Index {
    /// Go `NewIndex`.
    pub(crate) fn new(
        schema_name: &str,
        table_name: &str,
        index_name: &str,
        columns: &[&str],
    ) -> Self {
        Self {
            schema_name: schema_name.go_to_lower(),
            table_name: table_name.go_to_lower(),
            index_name: index_name.go_to_lower(),
            columns: columns
                .iter()
                .map(|column| Column::new(schema_name, table_name, column))
                .collect(),
        }
    }

    /// Go `NewIndexWithColumns`.
    pub(crate) fn with_columns(index_name: &str, columns: &[Column]) -> Self {
        let names: Vec<&str> = columns
            .iter()
            .map(|column| column.column_name.as_str())
            .collect();
        Self::new(
            &columns[0].schema_name,
            &columns[0].table_name,
            index_name,
            &names,
        )
    }

    /// Go `PrefixContain`: whether `other` is a prefix of this index.
    pub(crate) fn prefix_contains(&self, other: &Self) -> bool {
        if self.schema_name != other.schema_name
            || self.table_name != other.table_name
            || self.columns.len() < other.columns.len()
        {
            return false;
        }
        other
            .columns
            .iter()
            .zip(&self.columns)
            .all(|(wanted, own)| own.column_name == wanted.column_name)
    }
}

impl Keyed for Index {
    fn key(&self) -> String {
        let names: Vec<&str> = self
            .columns
            .iter()
            .map(|column| column.column_name.as_str())
            .collect();
        format!(
            "{}.{}({})",
            self.schema_name,
            self.table_name,
            names.join(",")
        )
    }
}

/// Go `IndexSetCost`: the cost of an index configuration.
#[derive(Clone, Debug, Default)]
pub(crate) struct IndexSetCost {
    pub(crate) total_workload_query_cost: f64,
    pub(crate) total_number_of_index_columns: usize,
    pub(crate) index_keys: String,
}

impl IndexSetCost {
    /// Go `Less`.
    pub(crate) fn less(&self, other: &Self) -> bool {
        if self.total_workload_query_cost == 0.0 {
            // Not initialized.
            return false;
        }
        if other.total_workload_query_cost == 0.0 {
            return true;
        }
        let (cost, other_cost) = (
            self.total_workload_query_cost,
            other.total_workload_query_cost,
        );
        if (cost - other_cost).abs() > 10.0
            && (cost - other_cost).abs() / cost.max(other_cost) > 0.001
        {
            // Their costs are very different: the lower, the better.
            return cost < other_cost;
        }
        if self.total_number_of_index_columns != other.total_number_of_index_columns {
            // The same cost: the fewer columns, the better.
            return self.total_number_of_index_columns < other.total_number_of_index_columns;
        }
        // To make the result stable.
        self.index_keys < other.index_keys
    }
}

/// Go `ImpactedQuery`.
#[derive(Clone, Debug)]
pub struct ImpactedQuery {
    /// Go `Query`.
    pub query: String,
    /// Go `Improvement`.
    pub improvement: f64,
}

/// Go `Recommendation`.
#[derive(Clone, Debug)]
pub struct Recommendation {
    /// Go `Database`.
    pub database: String,
    /// Go `Table`.
    pub table: String,
    /// Go `IndexName`.
    pub index_name: String,
    /// Go `IndexColumns`.
    pub index_columns: Vec<String>,
    /// Go `Reason`.
    pub reason: String,
    /// Go `IndexSize`.
    pub index_size: u64,
    /// Go `WorkloadImprovement`.
    pub workload_improvement: f64,
    /// Go `TopImpactedQueries`.
    pub top_impacted_queries: Vec<ImpactedQuery>,
}
