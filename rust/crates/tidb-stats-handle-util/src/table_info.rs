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
use std::sync::OnceLock;

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

    /// Go `infoschema.IsV2` of the schema a `SessionExtendedInfoSchema`
    /// wraps: Go's `NewSnapshotTableInfoGetter` unwraps the session wrapper
    /// first, since `IsV2` does not see through it.
    fn is_v2(&self) -> bool;
    /// Go `TableByID`.
    fn table_by_id(&self, id: i64) -> Option<Self::Table>;
    /// Go `FindTableByPartitionID`'s table result.
    fn find_table_by_partition_id(&self, id: i64) -> Option<Self::Table>;
    /// Go `TableItemByID`.
    fn table_item_by_id(&self, id: i64) -> Option<TableItem>;
    /// Go `TableIDByPartitionID`.
    fn table_id_by_partition_id(&self, id: i64) -> Option<i64>;
    /// Tables returned by
    /// `ListTablesWithSpecialAttribute(PartitionAttribute)`.
    fn partitioned_table_infos(&self) -> Vec<GoShared<TableInfo>>;
}

/// Go's private `tableInfoGetterImpl`.
#[derive(Default)]
pub struct TableInfoGetter;

impl TableInfoGetter {
    /// Go `NewTableInfoGetter`.
    pub fn new() -> Self {
        Self
    }

    /// Go `TableInfoByID`: the table of the physical ID; a partition's is its
    /// parent table.
    pub fn table_info_by_id<S: InfoSchema>(
        &self,
        info_schema: &S,
        physical_id: i64,
    ) -> Option<S::Table> {
        info_schema
            .table_by_id(physical_id)
            .or_else(|| info_schema.find_table_by_partition_id(physical_id))
    }
}

/// Go `buildPartitionID2TableID`.
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

/// Go `SnapshotTableInfoGetter`: the table of a physical ID in one
/// InfoSchema snapshot. On InfoSchema V1 each partition lookup scans all
/// tables, so it builds a partition map on the first `parent_table_id` call.
/// It is safe for concurrent use. Create one per snapshot instead of sharing
/// a cache, because callers on different schema versions would rebuild a
/// shared map.
pub struct SnapshotTableInfoGetter<'a, S: InfoSchema> {
    is: &'a S,
    /// Go `pid2tid` + `pid2tidOnce`: unset until the first V1 partition
    /// lookup builds it; read-only after that.
    pid2tid: OnceLock<HashMap<i64, i64>>,
    is_v2: bool,
}

impl<'a, S: InfoSchema> SnapshotTableInfoGetter<'a, S> {
    /// Go `NewSnapshotTableInfoGetter`.
    pub fn new(is: &'a S) -> Self {
        Self {
            is,
            pid2tid: OnceLock::new(),
            is_v2: is.is_v2(),
        }
    }

    /// Go `ParentTableID`: the parent table ID when the physical ID is a
    /// partition.
    pub fn parent_table_id(&self, physical_id: i64) -> Option<i64> {
        if self.is_v2 {
            return self.is.table_id_by_partition_id(physical_id);
        }
        // Not built in the constructor: table_info_by_id and table_item_by_id
        // call this only when the ID is not a table ID, so a caller that
        // gives them only table IDs skips the scan.
        self.pid2tid
            .get_or_init(|| build_partition_id_to_table_id(self.is))
            .get(&physical_id)
            .copied()
    }

    /// Go `TableInfoByID`: the table of the physical ID; a partition's is its
    /// parent table.
    pub fn table_info_by_id(&self, physical_id: i64) -> Option<S::Table> {
        if let Some(table) = self.is.table_by_id(physical_id) {
            return Some(table);
        }
        self.parent_table_id(physical_id)
            .and_then(|table_id| self.is.table_by_id(table_id))
    }

    /// Go `TableItemByID`: the table item of the physical ID; a partition's
    /// is its parent table's item.
    pub fn table_item_by_id(&self, physical_id: i64) -> Option<TableItem> {
        if let Some(item) = self.is.table_item_by_id(physical_id) {
            return Some(item);
        }
        self.parent_table_id(physical_id)
            .and_then(|table_id| self.is.table_item_by_id(table_id))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tidb_goruntime::go_runtime::GoSharedSlice;
    use tidb_model::{GoShared, PartitionDefinition, PartitionInfo, TableInfo};

    use super::{InfoSchema, SnapshotTableInfoGetter, TableInfoGetter, TableItem};

    const NORMAL_ID: i64 = 1;
    const PARTITIONED_ID: i64 = 2;
    const PARTITION_ID: i64 = 20;

    /// Go `partitionItemLookupForbiddenInfoSchema` over
    /// `MockInfoSchemaWithSchemaVer`: a V1 schema whose partition lookups
    /// fail the test, counting the all-table scans that build the partition
    /// map.
    struct PartitionItemLookupForbiddenInfoSchema {
        v2: bool,
        tables: HashMap<i64, &'static str>,
        partition_parents: HashMap<i64, i64>,
        table_infos: Vec<GoShared<TableInfo>>,
        partition_map_builds: AtomicUsize,
        forbid_partition_lookups: bool,
    }

    impl PartitionItemLookupForbiddenInfoSchema {
        fn fixture(v2: bool) -> Self {
            let partition = PartitionInfo {
                definitions: GoSharedSlice::from_vec(vec![
                    PartitionDefinition {
                        id: PARTITION_ID,
                        ..PartitionDefinition::default()
                    },
                    PartitionDefinition {
                        id: PARTITION_ID + 1,
                        ..PartitionDefinition::default()
                    },
                ]),
                ..PartitionInfo::default()
            };
            let table = GoShared::new(TableInfo {
                id: PARTITIONED_ID,
                name: tidb_ast::CiString::new("partitioned"),
                partition: Some(GoShared::new(partition)),
                ..TableInfo::default()
            });
            Self {
                v2,
                tables: HashMap::from([(NORMAL_ID, "normal"), (PARTITIONED_ID, "partitioned")]),
                partition_parents: HashMap::from([
                    (PARTITION_ID, PARTITIONED_ID),
                    (PARTITION_ID + 1, PARTITIONED_ID),
                ]),
                table_infos: vec![table],
                partition_map_builds: AtomicUsize::new(0),
                forbid_partition_lookups: !v2,
            }
        }
    }

    impl InfoSchema for PartitionItemLookupForbiddenInfoSchema {
        type Table = i64;

        fn is_v2(&self) -> bool {
            self.v2
        }

        fn table_by_id(&self, id: i64) -> Option<Self::Table> {
            self.tables.contains_key(&id).then_some(id)
        }

        fn find_table_by_partition_id(&self, id: i64) -> Option<Self::Table> {
            self.partition_parents.get(&id).copied()
        }

        fn table_item_by_id(&self, id: i64) -> Option<TableItem> {
            self.tables.get(&id).map(|name| TableItem {
                db_name: tidb_ast::CiString::new("test"),
                table_name: tidb_ast::CiString::new(*name),
            })
        }

        fn table_id_by_partition_id(&self, id: i64) -> Option<i64> {
            assert!(
                !self.forbid_partition_lookups,
                "TableIDByPartitionID should not be called for partition ID {id}"
            );
            self.partition_parents.get(&id).copied()
        }

        fn partitioned_table_infos(&self) -> Vec<GoShared<TableInfo>> {
            self.partition_map_builds.fetch_add(1, Ordering::SeqCst);
            self.table_infos.clone()
        }
    }

    #[test]
    fn ordinary_lookup_falls_back_to_partition_search() {
        let info_schema = PartitionItemLookupForbiddenInfoSchema::fixture(false);
        let getter = TableInfoGetter::new();
        assert_eq!(getter.table_info_by_id(&info_schema, NORMAL_ID), Some(NORMAL_ID));
        assert_eq!(
            getter.table_info_by_id(&info_schema, PARTITION_ID),
            Some(PARTITIONED_ID)
        );
        assert_eq!(getter.table_info_by_id(&info_schema, 99), None);
    }

    /// Go `TestSnapshotTableInfoGetterAvoidsV1PartitionScan`.
    #[test]
    fn test_snapshot_table_info_getter_avoids_v1_partition_scan() {
        let is = PartitionItemLookupForbiddenInfoSchema::fixture(false);
        let snapshot_getter = SnapshotTableInfoGetter::new(&is);
        assert_eq!(snapshot_getter.table_info_by_id(NORMAL_ID), Some(NORMAL_ID));
        let item = snapshot_getter.table_item_by_id(NORMAL_ID).unwrap();
        assert_eq!(item.table_name.lowercase(), "normal");
        // A table ID lookup does not need the partition map, so the map must
        // not exist yet.
        assert_eq!(is.partition_map_builds.load(Ordering::SeqCst), 0);
        assert_eq!(
            snapshot_getter.table_info_by_id(PARTITION_ID),
            Some(PARTITIONED_ID)
        );
        assert_eq!(snapshot_getter.table_info_by_id(1 << 60), None);
        let item = snapshot_getter.table_item_by_id(PARTITION_ID).unwrap();
        assert_eq!(item.table_name.lowercase(), "partitioned");
        assert!(snapshot_getter.table_item_by_id(1 << 60).is_none());
        assert_eq!(snapshot_getter.parent_table_id(NORMAL_ID), None);
        assert_eq!(
            snapshot_getter.parent_table_id(PARTITION_ID),
            Some(PARTITIONED_ID)
        );
        assert_eq!(snapshot_getter.parent_table_id(1 << 60), None);
        // The partition ID and unknown ID lookups above all use one map.
        assert_eq!(is.partition_map_builds.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn snapshot_getter_on_v2_uses_the_schema_partition_index() {
        let is = PartitionItemLookupForbiddenInfoSchema::fixture(true);
        let snapshot_getter = SnapshotTableInfoGetter::new(&is);
        assert_eq!(
            snapshot_getter.parent_table_id(PARTITION_ID + 1),
            Some(PARTITIONED_ID)
        );
        assert_eq!(
            snapshot_getter
                .table_item_by_id(PARTITION_ID)
                .unwrap()
                .table_name
                .lowercase(),
            "partitioned"
        );
        assert_eq!(is.partition_map_builds.load(Ordering::SeqCst), 0);
    }
}
