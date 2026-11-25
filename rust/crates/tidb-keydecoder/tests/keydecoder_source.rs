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

//! Go `pkg/util/keydecoder/keydecoder_test.go::TestDecodeKey`.

use tidb_ast::CiString;
use tidb_tablecodec::table_key::{
    encode_index_seek_key, encode_row_key_with_handle, RecordHandle,
};
use tidb_codec::encode_key;
use tidb_datatype::Datum;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_keydecoder::{decode_key, HandleType, KeyInfoCatalog, KeyInfoTableLookup};
use tidb_model::db::DBInfo;
use tidb_model::go_runtime::GoShared;
use tidb_model::index::IndexInfo;
use tidb_model::partition::{PartitionDefinition, PartitionInfo};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_table::tables::{Index, TableCommon};

fn index(id: i64, name: &str) -> IndexInfo {
    IndexInfo {
        id,
        name: CiString::new(name),
        state: SchemaState::PUBLIC,
        ..IndexInfo::default()
    }
}

/// Go `infoschema.MockInfoSchema(tblInfos)`: the schema `test` with each
/// table built over its `TableInfo` (`tables.MockTableFromMeta`).
fn catalog() -> SessionExtendedInfoSchema {
    let database = GoShared::new(DBInfo {
            id: 1,
            name: CiString::new("test"),
            deprecated_tables: vec![
                TableInfo {
                    id: 1,
                    name: CiString::new("table1"),
                    indices: vec![index(1, "index1")].into(),
                    ..TableInfo::default()
                },
                TableInfo {
                    id: 2,
                    name: CiString::new("table2"),
                    ..TableInfo::default()
                },
                TableInfo {
                    id: 3,
                    name: CiString::new("table3"),
                    indices: vec![index(4, "index4")].into(),
                    partition: Some(GoShared::new(PartitionInfo {
                        enable: true,
                        definitions: vec![
                            PartitionDefinition {
                                id: 5,
                                name: CiString::new("p0"),
                                ..PartitionDefinition::default()
                            },
                            PartitionDefinition {
                                id: 6,
                                name: CiString::new("p1"),
                                ..PartitionDefinition::default()
                            },
                        ]
                        .into(),
                        ..PartitionInfo::default()
                    })),
                    ..TableInfo::default()
                },
            ]
            .into(),
            ..DBInfo::default()
        });
    let mut catalog =
        SessionExtendedInfoSchema::init_with_db_infos(vec![database.clone()], Vec::new(), Vec::new(), 1);
    for table in database.read().deprecated_tables.iter_deref() {
        // Go `MockTableFromMeta`: the table over its `TableInfo`, with an
        // index per `IndexInfo` (these tables have no columns).
        let table = table.read().clone();
        let name = table.name.original().to_owned();
        let indices = table.indices.iter_deref().collect::<Vec<_>>();
        let mut built = TableCommon::loaded(table, Vec::new());
        for index in indices {
            built.add_index(Index::new(index.read().clone(), Vec::new()), false);
        }
        catalog.register_kv_in("test", &name, built).unwrap();
    }
    catalog
}

fn record_key(table_id: i64, handle: RecordHandle) -> Vec<u8> {
    encode_row_key_with_handle(table_id, &handle)
}

fn index_key(table_id: i64, index_id: i64, values: &[Datum]) -> Vec<u8> {
    encode_index_seek_key(table_id, index_id, &encode_key(values).unwrap())
}

#[test]
fn physical_lookup_selects_only_the_requested_index() {
    let catalog = catalog();
    let metadata = |index| {
        let Some(KeyInfoTableLookup::Resolved(table)) =
            catalog.resolve_physical_table_for_key(5, index)
        else {
            panic!("partition should resolve");
        };
        table
    };
    assert!(metadata(None).indexes.is_empty());
    assert!(metadata(Some(99)).indexes.is_empty());
    let table = metadata(Some(4));
    assert_eq!(table.partition_id, 5);
    assert_eq!(table.partition_name, "p0");
    assert_eq!(table.indexes.len(), 1);
    assert_eq!(table.indexes[0].name, "index4");
}

#[test]
fn v2_key_lookup_loads_only_its_owning_table() {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tidb_infoschema::cluster_catalog::ClusterCatalogError;
    use tidb_infoschema::infoschema_v2::{CachedTable, Data, TableLoader};
    use tidb_infoschema::TableEntry;

    struct Loader;
    impl TableLoader for Loader {
        fn load_table(
            &self,
            db_id: i64,
            table_id: i64,
            _: u64,
        ) -> Result<CachedTable, ClusterCatalogError> {
            assert_eq!((db_id, table_id), (1, 20));
            let table = TableCommon::loaded(
                TableInfo {
                    db_id,
                    id: table_id,
                    name: CiString::new("A_Target"),
                    state: SchemaState::PUBLIC,
                    ..TableInfo::default()
                },
                Vec::new(),
            );
            Ok(CachedTable {
                info: table.meta(),
                table: Arc::new(TableEntry::Kv(Arc::new(table))),
                size: 1024,
            })
        }

        fn list_tables(
            &self,
            _: i64,
            _: u64,
        ) -> Result<Vec<GoShared<TableInfo>>, ClusterCatalogError> {
            panic!("key decoding should not list the database");
        }
    }

    let database = GoShared::new(DBInfo {
        id: 1,
        name: CiString::new("MiXeD"),
        table_name2id: Some(GoShared::new(BTreeMap::from([
            ("z_unused".to_owned(), 10),
            ("a_target".to_owned(), 20),
        ]))),
        ..DBInfo::default()
    });
    let mut catalog =
        SessionExtendedInfoSchema::init_with_db_infos(vec![database], Vec::new(), Vec::new(), 1);
    catalog.init_with_db_infos_v2(Arc::new(Data::new()), Arc::new(Loader), 1 << 20);
    let decoded = decode_key(&record_key(20, RecordHandle::Int(42)), &catalog).unwrap();
    assert_eq!(decoded.db_name, "MiXeD");
    assert_eq!(decoded.table_name, "A_Target");
    assert_eq!(decoded.handle_value, "42");
}

#[test]
fn test_decode_key() {
    let catalog = catalog();

    let decoded = decode_key(&record_key(1, RecordHandle::Int(1)), &catalog).unwrap();
    assert_eq!(decoded.db_id, 1);
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_id, 1);
    assert_eq!(decoded.table_name, "table1");
    assert_eq!(decoded.partition_id, 0);
    assert_eq!(decoded.partition_name, "");
    assert_eq!(decoded.handle_type, HandleType::Int);
    assert!(!decoded.is_partition_handle);
    assert_eq!(decoded.handle_value, "1");
    assert_eq!(decoded.index_id, 0);
    assert_eq!(decoded.index_name, "");
    assert_eq!(decoded.index_values, None);

    let common =
        RecordHandle::Common(encode_key(&[Datum::Int(100), Datum::new_string_datum("abc")]).unwrap());
    let decoded = decode_key(&record_key(2, common), &catalog).unwrap();
    assert_eq!(decoded.db_id, 1);
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_id, 2);
    assert_eq!(decoded.table_name, "table2");
    assert_eq!(decoded.partition_id, 0);
    assert_eq!(decoded.partition_name, "");
    assert_eq!(decoded.handle_type, HandleType::Common);
    assert!(!decoded.is_partition_handle);
    assert_eq!(decoded.handle_value, "{100, abc}");
    assert_eq!(decoded.index_id, 0);
    assert_eq!(decoded.index_name, "");
    assert_eq!(decoded.index_values, None);

    let decoded = decode_key(
        &index_key(1, 1, &[Datum::new_string_datum("abc"), Datum::Int(1)]),
        &catalog,
    )
    .unwrap();
    assert_eq!(decoded.db_id, 1);
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_id, 1);
    assert_eq!(decoded.table_name, "table1");
    assert_eq!(decoded.partition_id, 0);
    assert_eq!(decoded.partition_name, "");
    assert_eq!(decoded.index_id, 1);
    assert_eq!(decoded.index_name, "index1");
    assert_eq!(decoded.index_values, Some(vec!["abc".into(), "1".into()]));
    assert_eq!(decoded.handle_type, HandleType::None);
    assert_eq!(decoded.handle_value, "");
    assert!(!decoded.is_partition_handle);

    let decoded = decode_key(&record_key(5, RecordHandle::Int(10)), &catalog).unwrap();
    assert_eq!(decoded.db_id, 1);
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_id, 3);
    assert_eq!(decoded.table_name, "table3");
    assert_eq!(decoded.partition_id, 5);
    assert_eq!(decoded.partition_name, "p0");
    assert_eq!(decoded.handle_type, HandleType::Int);
    assert_eq!(decoded.handle_value, "10");
    assert_eq!(decoded.index_id, 0);
    assert_eq!(decoded.index_name, "");
    assert_eq!(decoded.index_values, None);
    assert!(!decoded.is_partition_handle);

    let decoded = decode_key(
        &index_key(6, 4, &[Datum::new_string_datum("abcde"), Datum::Int(2)]),
        &catalog,
    )
    .unwrap();
    assert_eq!(decoded.db_id, 1);
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_id, 3);
    assert_eq!(decoded.table_name, "table3");
    assert_eq!(decoded.partition_id, 6);
    assert_eq!(decoded.partition_name, "p1");
    assert_eq!(decoded.index_id, 4);
    assert_eq!(decoded.index_name, "index4");
    assert_eq!(
        decoded.index_values,
        Some(vec!["abcde".into(), "2".into()])
    );
    assert_eq!(decoded.handle_type, HandleType::None);
    assert_eq!(decoded.handle_value, "");
    assert!(!decoded.is_partition_handle);

    assert!(decode_key(b"this-is-a-totally-invalidkey", &catalog).is_err());

    let mut partly_invalid = vec![b't', 0x80, 0, 0, 0, 0, 0, 0, 1];
    partly_invalid.extend_from_slice(b"rest-part-is-invalid");
    assert!(decode_key(&partly_invalid, &catalog).is_err());

    let decoded = decode_key(&record_key(4, RecordHandle::Int(1)), &catalog).unwrap();
    assert_eq!(decoded.table_id, 4);
    assert_eq!(decoded.handle_type, HandleType::Int);
    assert_eq!(decoded.handle_value, "1");
    assert_eq!(decoded.db_id, 0);
    assert_eq!(decoded.db_name, "");
    assert_eq!(decoded.table_name, "");
    assert_eq!(decoded.partition_id, 0);
    assert_eq!(decoded.partition_name, "");
    assert_eq!(decoded.index_id, 0);
    assert_eq!(decoded.index_name, "");
    assert!(!decoded.is_partition_handle);
    assert_eq!(decoded.index_values, None);
}

// Go's test binary links `planner/core`, whose `init` sets `expression.BuildSimpleExpr`.
tidb_planner_init::link!();
