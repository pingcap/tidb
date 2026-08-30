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

//! Pinned Go `pkg/planner/util/tablesampler/sample.go`.

use std::mem::size_of;

use tidb_ast::TableSample;
use tidb_expr::schema::Schema;

/// Go `tablesampler.TableSampleInfo`: metadata retained for a physical table
/// sample and its row decoder.
#[derive(Clone, Debug)]
pub struct TableSampleInfo {
    /// Go `AstNode`.
    pub ast_node: TableSample,
    /// Go `FullSchema`, cloned when this value is constructed.
    pub full_schema: Schema,
    /// Physical partition IDs selected for this logical table.
    ///
    /// Go retains `[]table.PartitionedTable`; the Rust catalog already maps
    /// every selected partition to its physical ID, the fact range splitting
    /// ultimately consumes.
    pub partition_ids: Vec<i64>,
}

impl TableSampleInfo {
    /// Go `(*TableSampleInfo).MemoryUsage`, expressed over Rust-owned values.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        let column_bytes = self
            .full_schema
            .columns
            .capacity()
            .saturating_mul(size_of::<tidb_expr::column::Column>());
        let key_bytes = self
            .full_schema
            .pk_or_uk
            .iter()
            .chain(&self.full_schema.nullable_uk)
            .map(|key| {
                key.capacity()
                    .saturating_mul(size_of::<tidb_expr::column::Column>())
            })
            .sum::<usize>();
        let partition_bytes = self
            .partition_ids
            .capacity()
            .saturating_mul(size_of::<i64>());
        i64::try_from(
            size_of::<Self>()
                .saturating_add(column_bytes)
                .saturating_add(key_bytes)
                .saturating_add(partition_bytes),
        )
        .unwrap_or(i64::MAX)
    }
}

/// Go `NewTableSampleInfo`.
#[must_use]
pub fn new_table_sample_info(
    node: Option<&TableSample>,
    full_schema: &Schema,
    partition_ids: Vec<i64>,
) -> Option<TableSampleInfo> {
    node.map(|ast_node| TableSampleInfo {
        ast_node: ast_node.clone(),
        full_schema: full_schema.clone(),
        partition_ids,
    })
}

#[cfg(test)]
mod tests {
    use super::new_table_sample_info;
    use tidb_ast::{SampleMethod, TableSample};
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::{column::Column, schema::Schema};

    #[test]
    fn constructor_matches_go_nil_and_clone_contract() {
        let mut schema = Schema::new(vec![Column::new(
            7,
            FieldType::new(FieldTypeCode::LongLong),
        )]);
        assert!(new_table_sample_info(None, &schema, vec![11]).is_none());

        let node = TableSample {
            method: Some(SampleMethod::Region),
            expr: None,
            unit: None,
            repeatable: None,
        };
        let info = new_table_sample_info(Some(&node), &schema, vec![11, 12])
            .expect("a non-nil AST node creates sample metadata");
        schema.columns[0].unique_id = 99;

        assert_eq!(info.full_schema.columns[0].unique_id, 7);
        assert_eq!(info.partition_ids, [11, 12]);
        assert!(info.memory_usage() >= i64::try_from(std::mem::size_of_val(&info)).unwrap());
    }
}
