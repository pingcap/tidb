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

use std::fmt::Write;

use serde::{Deserialize, Serialize};
use tidb_ast::CiString;
use tidb_model::column::ColumnInfo;
use tidb_model::index::IndexInfo;
use tidb_model::partition::PartitionInfo;
use tidb_model::{ActionType, DBInfo, TableInfo};

/// Go `notifier.SchemaChangeEvent`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaChangeEvent {
    inner: JsonSchemaChangeEvent,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct JsonSchemaChangeEvent {
    #[serde(
        rename = "mini_db_info",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    mini_db_info: Option<MiniDbInfoForSchemaEvent>,
    #[serde(
        rename = "table_info",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    table_info: Option<Box<TableInfo>>,
    #[serde(
        rename = "old_table_info",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    old_table_info: Option<Box<TableInfo>>,
    #[serde(
        rename = "added_partition_info",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    added_partition_info: Option<Box<PartitionInfo>>,
    #[serde(
        rename = "dropped_partition_info",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    dropped_partition_info: Option<Box<PartitionInfo>>,
    #[serde(rename = "columns", default, skip_serializing_if = "Vec::is_empty")]
    columns: Vec<ColumnInfo>,
    #[serde(rename = "indexes", default, skip_serializing_if = "Vec::is_empty")]
    indexes: Vec<IndexInfo>,
    // The capitalized wire name is intentional and matches pinned Go.
    #[serde(rename = "Analyzed", default, skip_serializing_if = "is_false")]
    analyzed: bool,
    #[serde(
        rename = "old_table_id_for_partition",
        default,
        skip_serializing_if = "is_zero"
    )]
    old_table_id_for_partition: i64,
    #[serde(rename = "type", default, skip_serializing_if = "is_none_action")]
    action_type: ActionType,
}

const fn is_false(value: &bool) -> bool {
    !*value
}

const fn is_zero(value: &i64) -> bool {
    *value == 0
}

const fn is_none_action(value: &ActionType) -> bool {
    value.0 == ActionType::ACTION_NONE.0
}

impl std::fmt::Display for SchemaChangeEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = format!("(Event Type: {}", self.inner.action_type);
        if let Some(table) = self.inner.table_info.as_deref() {
            let _ = write!(out, ", Table ID: {}, Table Name: {}", table.id, table.name);
        }
        if let Some(table) = self.inner.old_table_info.as_deref() {
            let _ = write!(
                out,
                ", Old Table ID: {}, Old Table Name: {}",
                table.id, table.name
            );
        }
        if self.inner.old_table_id_for_partition != 0 {
            let _ = write!(
                out,
                ", Old Table ID for Partition: {}",
                self.inner.old_table_id_for_partition
            );
        }
        if let Some(partitions) = self.inner.added_partition_info.as_deref() {
            for partition in partitions.definitions.snapshot() {
                if !partition.name.lowercase().is_empty() {
                    let _ = write!(out, ", Partition Name: {}", partition.name);
                }
                let _ = write!(out, ", Partition ID: {}", partition.id);
            }
        }
        if let Some(partitions) = self.inner.dropped_partition_info.as_deref() {
            for partition in partitions.definitions.snapshot() {
                if !partition.name.lowercase().is_empty() {
                    let _ = write!(out, ", Dropped Partition Name: {}", partition.name);
                }
                let _ = write!(out, ", Dropped Partition ID: {}", partition.id);
            }
        }
        for column in &self.inner.columns {
            let _ = write!(
                out,
                ", Column ID: {}, Column Name: {}",
                column.id, column.name
            );
        }
        for index in &self.inner.indexes {
            let _ = write!(out, ", Index ID: {}, Index Name: {}", index.id, index.name);
        }
        out.push(')');
        formatter.write_str(&out)
    }
}

impl SchemaChangeEvent {
    /// Go `GetType`.
    #[must_use]
    pub const fn action_type(&self) -> ActionType {
        self.inner.action_type
    }

    /// Go `NewCreateTableEvent`.
    #[must_use]
    pub fn create_table(table: TableInfo) -> Self {
        Self::with_table(ActionType::ACTION_CREATE_TABLE, table)
    }

    /// Go `GetCreateTableInfo`.
    #[must_use]
    pub fn create_table_info(&self) -> &TableInfo {
        self.assert_type(ActionType::ACTION_CREATE_TABLE);
        self.table_info()
    }

    /// Go `NewTruncateTableEvent`.
    #[must_use]
    pub fn truncate_table(table: TableInfo, old_table: TableInfo) -> Self {
        Self {
            inner: JsonSchemaChangeEvent {
                action_type: ActionType::ACTION_TRUNCATE_TABLE,
                table_info: Some(Box::new(table)),
                old_table_info: Some(Box::new(old_table)),
                ..JsonSchemaChangeEvent::default()
            },
        }
    }

    /// Go `GetTruncateTableInfo`.
    #[must_use]
    pub fn truncate_table_info(&self) -> (&TableInfo, &TableInfo) {
        self.assert_type(ActionType::ACTION_TRUNCATE_TABLE);
        (self.table_info(), self.old_table_info())
    }

    /// Go `NewDropTableEvent`.
    #[must_use]
    pub fn drop_table(old_table: TableInfo) -> Self {
        Self {
            inner: JsonSchemaChangeEvent {
                action_type: ActionType::ACTION_DROP_TABLE,
                old_table_info: Some(Box::new(old_table)),
                ..JsonSchemaChangeEvent::default()
            },
        }
    }

    /// Go `GetDropTableInfo`.
    #[must_use]
    pub fn drop_table_info(&self) -> &TableInfo {
        self.assert_type(ActionType::ACTION_DROP_TABLE);
        self.old_table_info()
    }

    /// Go `NewAddColumnEvent`.
    #[must_use]
    pub fn add_columns(table: TableInfo, columns: Vec<ColumnInfo>) -> Self {
        Self::with_columns(ActionType::ACTION_ADD_COLUMN, table, columns, false)
    }

    /// Go `GetAddColumnInfo`.
    #[must_use]
    pub fn add_column_info(&self) -> (&TableInfo, &[ColumnInfo]) {
        self.assert_type(ActionType::ACTION_ADD_COLUMN);
        (self.table_info(), &self.inner.columns)
    }

    /// Go `NewModifyColumnEvent`.
    #[must_use]
    pub fn modify_columns(table: TableInfo, columns: Vec<ColumnInfo>, analyzed: bool) -> Self {
        Self::with_columns(ActionType::ACTION_MODIFY_COLUMN, table, columns, analyzed)
    }

    /// Go `GetModifyColumnInfo`.
    #[must_use]
    pub fn modify_column_info(&self) -> (&TableInfo, &[ColumnInfo], bool) {
        self.assert_type(ActionType::ACTION_MODIFY_COLUMN);
        (self.table_info(), &self.inner.columns, self.inner.analyzed)
    }

    /// Go `NewAddPartitionEvent`.
    #[must_use]
    pub fn add_partitions(table: TableInfo, added: PartitionInfo) -> Self {
        Self::with_partitions(
            ActionType::ACTION_ADD_TABLE_PARTITION,
            table,
            Some(added),
            None,
        )
    }

    /// Go `GetAddPartitionInfo`.
    #[must_use]
    pub fn add_partition_info(&self) -> (&TableInfo, &PartitionInfo) {
        self.assert_type(ActionType::ACTION_ADD_TABLE_PARTITION);
        (self.table_info(), self.added_partition_info())
    }

    /// Go `NewTruncatePartitionEvent`.
    #[must_use]
    pub fn truncate_partitions(
        table: TableInfo,
        added: PartitionInfo,
        dropped: PartitionInfo,
    ) -> Self {
        Self::with_partitions(
            ActionType::ACTION_TRUNCATE_TABLE_PARTITION,
            table,
            Some(added),
            Some(dropped),
        )
    }

    /// Go `GetTruncatePartitionInfo`.
    #[must_use]
    pub fn truncate_partition_info(&self) -> (&TableInfo, &PartitionInfo, &PartitionInfo) {
        self.assert_type(ActionType::ACTION_TRUNCATE_TABLE_PARTITION);
        (
            self.table_info(),
            self.added_partition_info(),
            self.dropped_partition_info(),
        )
    }

    /// Go `NewDropPartitionEvent`.
    #[must_use]
    pub fn drop_partitions(table: TableInfo, dropped: PartitionInfo) -> Self {
        Self::with_partitions(
            ActionType::ACTION_DROP_TABLE_PARTITION,
            table,
            None,
            Some(dropped),
        )
    }

    /// Go `GetDropPartitionInfo`.
    #[must_use]
    pub fn drop_partition_info(&self) -> (&TableInfo, &PartitionInfo) {
        self.assert_type(ActionType::ACTION_DROP_TABLE_PARTITION);
        (self.table_info(), self.dropped_partition_info())
    }

    /// Go `NewExchangePartitionEvent`.
    #[must_use]
    pub fn exchange_partition(
        table: TableInfo,
        partition: PartitionInfo,
        old_table: TableInfo,
    ) -> Self {
        let mut event = Self::with_partitions(
            ActionType::ACTION_EXCHANGE_TABLE_PARTITION,
            table,
            Some(partition),
            None,
        );
        event.inner.old_table_info = Some(Box::new(old_table));
        event
    }

    /// Go `GetExchangePartitionInfo`.
    #[must_use]
    pub fn exchange_partition_info(&self) -> (&TableInfo, &PartitionInfo, &TableInfo) {
        self.assert_type(ActionType::ACTION_EXCHANGE_TABLE_PARTITION);
        (
            self.table_info(),
            self.added_partition_info(),
            self.old_table_info(),
        )
    }

    /// Go `NewReorganizePartitionEvent`.
    #[must_use]
    pub fn reorganize_partitions(
        table: TableInfo,
        added: PartitionInfo,
        dropped: PartitionInfo,
    ) -> Self {
        Self::with_partitions(
            ActionType::ACTION_REORGANIZE_PARTITION,
            table,
            Some(added),
            Some(dropped),
        )
    }

    /// Go `GetReorganizePartitionInfo`.
    #[must_use]
    pub fn reorganize_partition_info(&self) -> (&TableInfo, &PartitionInfo, &PartitionInfo) {
        self.assert_type(ActionType::ACTION_REORGANIZE_PARTITION);
        (
            self.table_info(),
            self.added_partition_info(),
            self.dropped_partition_info(),
        )
    }

    /// Go `NewAddPartitioningEvent`.
    #[must_use]
    pub fn add_partitioning(old_table_id: i64, table: TableInfo, added: PartitionInfo) -> Self {
        let mut event = Self::with_partitions(
            ActionType::ACTION_ALTER_TABLE_PARTITIONING,
            table,
            Some(added),
            None,
        );
        event.inner.old_table_id_for_partition = old_table_id;
        event
    }

    /// Go `GetAddPartitioningInfo`.
    #[must_use]
    pub fn add_partitioning_info(&self) -> (i64, &TableInfo, &PartitionInfo) {
        self.assert_type(ActionType::ACTION_ALTER_TABLE_PARTITIONING);
        (
            self.inner.old_table_id_for_partition,
            self.table_info(),
            self.added_partition_info(),
        )
    }

    /// Go `NewRemovePartitioningEvent`.
    #[must_use]
    pub fn remove_partitioning(
        old_table_id: i64,
        table: TableInfo,
        dropped: PartitionInfo,
    ) -> Self {
        let mut event = Self::with_partitions(
            ActionType::ACTION_REMOVE_PARTITIONING,
            table,
            None,
            Some(dropped),
        );
        event.inner.old_table_id_for_partition = old_table_id;
        event
    }

    /// Go `GetRemovePartitioningInfo`.
    #[must_use]
    pub fn remove_partitioning_info(&self) -> (i64, &TableInfo, &PartitionInfo) {
        self.assert_type(ActionType::ACTION_REMOVE_PARTITIONING);
        (
            self.inner.old_table_id_for_partition,
            self.table_info(),
            self.dropped_partition_info(),
        )
    }

    /// Go `NewAddIndexEvent`.
    #[must_use]
    pub fn add_indexes(table: TableInfo, indexes: Vec<IndexInfo>, analyzed: bool) -> Self {
        Self {
            inner: JsonSchemaChangeEvent {
                action_type: ActionType::ACTION_ADD_INDEX,
                table_info: Some(Box::new(table)),
                indexes,
                analyzed,
                ..JsonSchemaChangeEvent::default()
            },
        }
    }

    /// Go `GetAddIndexInfo`.
    #[must_use]
    pub fn add_index_info(&self) -> (&TableInfo, &[IndexInfo], bool) {
        self.assert_type(ActionType::ACTION_ADD_INDEX);
        (self.table_info(), &self.inner.indexes, self.inner.analyzed)
    }

    /// Go `NewFlashbackClusterEvent`.
    #[must_use]
    pub fn flashback_cluster() -> Self {
        Self {
            inner: JsonSchemaChangeEvent {
                action_type: ActionType::ACTION_FLASHBACK_CLUSTER,
                ..JsonSchemaChangeEvent::default()
            },
        }
    }

    /// Go `NewDropSchemaEvent`, including its deliberately small payload.
    #[must_use]
    pub fn drop_schema(database: &DBInfo, tables: &[TableInfo]) -> Self {
        let tables = tables
            .iter()
            .map(|table| MiniTableInfoForSchemaEvent {
                id: table.id,
                name: table.name.clone(),
                partitions: table
                    .partition
                    .as_ref()
                    .map(|partition| {
                        partition
                            .read()
                            .definitions
                            .snapshot()
                            .into_iter()
                            .map(|partition| MiniPartitionInfoForSchemaEvent {
                                id: partition.id,
                                name: partition.name.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .collect();
        Self {
            inner: JsonSchemaChangeEvent {
                action_type: ActionType::ACTION_DROP_SCHEMA,
                mini_db_info: Some(MiniDbInfoForSchemaEvent {
                    id: database.id,
                    name: database.name.clone(),
                    tables,
                }),
                ..JsonSchemaChangeEvent::default()
            },
        }
    }

    /// Go `GetDropSchemaInfo`.
    #[must_use]
    pub fn drop_schema_info(&self) -> &MiniDbInfoForSchemaEvent {
        self.assert_type(ActionType::ACTION_DROP_SCHEMA);
        self.inner
            .mini_db_info
            .as_ref()
            .expect("drop-schema event has mini database info")
    }

    fn with_table(action_type: ActionType, table: TableInfo) -> Self {
        Self {
            inner: JsonSchemaChangeEvent {
                action_type,
                table_info: Some(Box::new(table)),
                ..JsonSchemaChangeEvent::default()
            },
        }
    }

    fn with_columns(
        action_type: ActionType,
        table: TableInfo,
        columns: Vec<ColumnInfo>,
        analyzed: bool,
    ) -> Self {
        Self {
            inner: JsonSchemaChangeEvent {
                action_type,
                table_info: Some(Box::new(table)),
                columns,
                analyzed,
                ..JsonSchemaChangeEvent::default()
            },
        }
    }

    fn with_partitions(
        action_type: ActionType,
        table: TableInfo,
        added: Option<PartitionInfo>,
        dropped: Option<PartitionInfo>,
    ) -> Self {
        Self {
            inner: JsonSchemaChangeEvent {
                action_type,
                table_info: Some(Box::new(table)),
                added_partition_info: added.map(Box::new),
                dropped_partition_info: dropped.map(Box::new),
                ..JsonSchemaChangeEvent::default()
            },
        }
    }

    fn assert_type(&self, expected: ActionType) {
        assert_eq!(self.inner.action_type, expected);
    }

    fn table_info(&self) -> &TableInfo {
        self.inner
            .table_info
            .as_deref()
            .expect("event has table info")
    }

    fn old_table_info(&self) -> &TableInfo {
        self.inner
            .old_table_info
            .as_deref()
            .expect("event has old table info")
    }

    fn added_partition_info(&self) -> &PartitionInfo {
        self.inner
            .added_partition_info
            .as_deref()
            .expect("event has added partition info")
    }

    fn dropped_partition_info(&self) -> &PartitionInfo {
        self.inner
            .dropped_partition_info
            .as_deref()
            .expect("event has dropped partition info")
    }
}

/// Go `MiniDBInfoForSchemaEvent`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MiniDbInfoForSchemaEvent {
    /// Database ID.
    pub id: i64,
    /// Database name.
    pub name: CiString,
    /// Tables dropped with the database.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<MiniTableInfoForSchemaEvent>,
}

/// Go `MiniTableInfoForSchemaEvent`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MiniTableInfoForSchemaEvent {
    /// Table ID.
    pub id: i64,
    /// Table name.
    pub name: CiString,
    /// Physical partitions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<MiniPartitionInfoForSchemaEvent>,
}

/// Go `MiniPartitionInfoForSchemaEvent`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MiniPartitionInfoForSchemaEvent {
    /// Partition ID.
    pub id: i64,
    /// Partition name.
    pub name: CiString,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_model::go_runtime::GoSharedSlice;
    use tidb_model::partition::PartitionDefinition;

    #[test]
    fn event_string_matches_go() {
        let partitions = |ids: &[i64]| PartitionInfo {
            definitions: GoSharedSlice::from_vec(
                ids.iter()
                    .map(|id| PartitionDefinition {
                        id: *id,
                        ..Default::default()
                    })
                    .collect::<Vec<_>>(),
            ),
            ..PartitionInfo::default()
        };
        let mut event = SchemaChangeEvent::add_columns(
            TableInfo {
                id: 1,
                name: CiString::new("Table1"),
                ..Default::default()
            },
            vec![
                ColumnInfo {
                    id: 7,
                    name: CiString::new("Column1"),
                    ..Default::default()
                },
                ColumnInfo {
                    id: 8,
                    name: CiString::new("Column2"),
                    ..Default::default()
                },
            ],
        );
        event.inner.old_table_info = Some(Box::new(TableInfo {
            id: 4,
            name: CiString::new("Table2"),
            ..Default::default()
        }));
        event.inner.added_partition_info = Some(Box::new(partitions(&[2, 3])));
        event.inner.dropped_partition_info = Some(Box::new(partitions(&[5, 6])));
        event.inner.indexes = vec![
            IndexInfo {
                id: 9,
                name: CiString::new("Index1"),
                ..Default::default()
            },
            IndexInfo {
                id: 10,
                name: CiString::new("Index2"),
                ..Default::default()
            },
        ];
        assert_eq!(
            event.to_string(),
            "(Event Type: add column, Table ID: 1, Table Name: Table1, Old Table ID: 4, Old Table Name: Table2, Partition ID: 2, Partition ID: 3, Dropped Partition ID: 5, Dropped Partition ID: 6, Column ID: 7, Column Name: Column1, Column ID: 8, Column Name: Column2, Index ID: 9, Index Name: Index1, Index ID: 10, Index Name: Index2)"
        );
    }

    #[test]
    fn event_json_round_trips_go_shape() {
        let event = SchemaChangeEvent::create_table(TableInfo {
            id: 1000,
            name: CiString::new("t1"),
            ..Default::default()
        });
        let bytes = serde_json::to_vec(&event).unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("\"type\":3"));
        let decoded: SchemaChangeEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.create_table_info().id, 1000);
        assert_eq!(decoded.create_table_info().name.original(), "t1");
    }
}
