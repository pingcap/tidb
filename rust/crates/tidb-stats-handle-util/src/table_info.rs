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

use std::collections::HashMap;
use std::sync::Mutex;

use tidb_ast::CiString;
use tidb_model::{GoShared, TableInfo};

/// Go `infoschema.TableItem`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TableItem {
    /// Go `DBName`.
    pub db_name: CiString,
    /// Go `TableName`.
    pub table_name: CiString,
}

/// The complete `infoschema.InfoSchema` surface consumed by this package.
pub trait InfoSchema: Send + Sync {
    /// Go `table.Table`.
    type Table: Clone;

    /// Go `infoschema.IsV2`.
    fn is_v2(&self) -> bool;
    /// Go `SchemaMetaVersion`.
    fn schema_meta_version(&self) -> i64;
    /// Go `TableByID`.
    fn table_by_id(&self, id: i64) -> Option<Self::Table>;
    /// Go `FindTableByPartitionID`'s table result.
    fn find_table_by_partition_id(&self, id: i64) -> Option<Self::Table>;
    /// Go `TableItemByID`.
    fn table_item_by_id(&self, id: i64) -> Option<TableItem>;
    /// Go `TableItemByPartitionID`.
    fn table_item_by_partition_id(&self, id: i64) -> Option<TableItem>;
    /// Tables returned by
    /// `ListTablesWithSpecialAttribute(PartitionAttribute)`.
    fn partitioned_table_infos(&self) -> Vec<GoShared<TableInfo>>;
}

#[derive(Default)]
struct PartitionCache {
    partition_to_table: HashMap<i64, i64>,
    schema_version: i64,
}

/// Go's private `tableInfoGetterImpl`.
#[derive(Default)]
pub struct TableInfoGetter {
    for_init_stats_and_info_schema_v1_only: Mutex<PartitionCache>,
}

impl TableInfoGetter {
    /// Go `NewTableInfoGetter`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `TableInfoByID`.
    pub fn table_info_by_id<S: InfoSchema>(
        &self,
        info_schema: &S,
        physical_id: i64,
    ) -> Option<S::Table> {
        info_schema
            .table_by_id(physical_id)
            .or_else(|| info_schema.find_table_by_partition_id(physical_id))
    }

    /// Go `TableInfoByIDForInitStats`.
    pub fn table_info_by_id_for_init_stats<S: InfoSchema>(
        &self,
        info_schema: &S,
        physical_id: i64,
    ) -> Option<S::Table> {
        if info_schema.is_v2() {
            return self.table_info_by_id(info_schema, physical_id);
        }
        if let Some(table) = info_schema.table_by_id(physical_id) {
            return Some(table);
        }
        self.partition_id_to_table_id_for_init_stats(info_schema, physical_id)
            .and_then(|id| info_schema.table_by_id(id))
    }

    /// Go `TableItemByIDForInitStats`.
    pub fn table_item_by_id_for_init_stats<S: InfoSchema>(
        &self,
        info_schema: &S,
        physical_id: i64,
    ) -> Option<TableItem> {
        if info_schema.is_v2() {
            return self.table_item_by_id(info_schema, physical_id);
        }
        if let Some(item) = info_schema.table_item_by_id(physical_id) {
            return Some(item);
        }
        self.partition_id_to_table_id_for_init_stats(info_schema, physical_id)
            .and_then(|id| info_schema.table_item_by_id(id))
    }

    /// Go `TableItemByID`.
    pub fn table_item_by_id<S: InfoSchema>(
        &self,
        info_schema: &S,
        physical_id: i64,
    ) -> Option<TableItem> {
        info_schema
            .table_item_by_id(physical_id)
            .or_else(|| info_schema.table_item_by_partition_id(physical_id))
    }

    fn partition_id_to_table_id_for_init_stats<S: InfoSchema>(
        &self,
        info_schema: &S,
        partition_id: i64,
    ) -> Option<i64> {
        let mut cache = self
            .for_init_stats_and_info_schema_v1_only
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let version = info_schema.schema_meta_version();
        if version != cache.schema_version {
            cache.schema_version = version;
            cache.partition_to_table = build_partition_id_to_table_id(info_schema);
        }
        cache.partition_to_table.get(&partition_id).copied()
    }
}

fn build_partition_id_to_table_id<S: InfoSchema>(info_schema: &S) -> HashMap<i64, i64> {
    let mut mapper = HashMap::new();
    for table in info_schema.partitioned_table_infos() {
        let table = table.read();
        let partition = table
            .partition
            .as_ref()
            .expect("partition-attributed table has partition info")
            .read();
        for definition in partition.definitions.snapshot() {
            mapper.insert(definition.id, table.id);
        }
    }
    mapper
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tidb_model::{GoShared, GoSharedSlice, PartitionDefinition, PartitionInfo, TableInfo};

    use super::{InfoSchema, TableInfoGetter, TableItem};

    struct MockInfoSchema {
        v2: bool,
        version: i64,
        tables: HashMap<i64, i64>,
        partition_parents: HashMap<i64, i64>,
        table_infos: Vec<GoShared<TableInfo>>,
        partition_item_scans: AtomicUsize,
    }

    impl MockInfoSchema {
        fn fixture(v2: bool) -> Self {
            let partition = PartitionInfo {
                definitions: GoSharedSlice::from_vec(vec![PartitionDefinition {
                    id: 20,
                    ..PartitionDefinition::default()
                }]),
                ..PartitionInfo::default()
            };
            let table = GoShared::new(TableInfo {
                id: 2,
                name: tidb_ast::CiString::new("partitioned"),
                partition: Some(GoShared::new(partition)),
                ..TableInfo::default()
            });
            Self {
                v2,
                version: 1,
                tables: HashMap::from([(1, 1), (2, 2)]),
                partition_parents: HashMap::from([(20, 2)]),
                table_infos: vec![table],
                partition_item_scans: AtomicUsize::new(0),
            }
        }

        fn item(id: i64) -> TableItem {
            TableItem {
                db_name: tidb_ast::CiString::new("test"),
                table_name: tidb_ast::CiString::new(if id == 1 { "normal" } else { "partitioned" }),
            }
        }
    }

    impl InfoSchema for MockInfoSchema {
        type Table = i64;

        fn is_v2(&self) -> bool {
            self.v2
        }

        fn schema_meta_version(&self) -> i64 {
            self.version
        }

        fn table_by_id(&self, id: i64) -> Option<Self::Table> {
            self.tables.get(&id).copied()
        }

        fn find_table_by_partition_id(&self, id: i64) -> Option<Self::Table> {
            self.partition_parents.get(&id).copied()
        }

        fn table_item_by_id(&self, id: i64) -> Option<TableItem> {
            self.tables.contains_key(&id).then(|| Self::item(id))
        }

        fn table_item_by_partition_id(&self, id: i64) -> Option<TableItem> {
            self.partition_item_scans.fetch_add(1, Ordering::SeqCst);
            self.partition_parents.get(&id).copied().map(Self::item)
        }

        fn partitioned_table_infos(&self) -> Vec<GoShared<TableInfo>> {
            self.table_infos.clone()
        }
    }

    #[deny(unused_must_use)]
    #[test]
    fn source_return_values_may_be_ignored_like_go() {
        TableInfoGetter::new();
    }

    #[test]
    fn ordinary_lookup_falls_back_to_partition_search() {
        let info_schema = MockInfoSchema::fixture(false);
        let getter = TableInfoGetter::new();
        assert_eq!(getter.table_info_by_id(&info_schema, 1), Some(1));
        assert_eq!(getter.table_info_by_id(&info_schema, 20), Some(2));
        assert_eq!(getter.table_info_by_id(&info_schema, 99), None);
    }

    #[test]
    fn v1_init_stats_item_lookup_uses_partition_cache_not_scan() {
        let info_schema = MockInfoSchema::fixture(false);
        let getter = TableInfoGetter::new();
        assert_eq!(
            getter
                .table_item_by_id_for_init_stats(&info_schema, 20)
                .unwrap()
                .table_name
                .lowercase(),
            "partitioned"
        );
        assert_eq!(info_schema.partition_item_scans.load(Ordering::SeqCst), 0);
        assert!(getter
            .table_item_by_id_for_init_stats(&info_schema, 1 << 60)
            .is_none());
    }

    #[test]
    fn v2_uses_indexed_partition_lookups() {
        let info_schema = MockInfoSchema::fixture(true);
        let getter = TableInfoGetter::new();
        assert_eq!(
            getter.table_info_by_id_for_init_stats(&info_schema, 20),
            Some(2)
        );
        assert_eq!(
            getter
                .table_item_by_id_for_init_stats(&info_schema, 20)
                .unwrap()
                .table_name
                .lowercase(),
            "partitioned"
        );
        assert_eq!(info_schema.partition_item_scans.load(Ordering::SeqCst), 1);
    }
}
