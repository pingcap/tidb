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
use tidb_codec::table_key::{
    encode_index_seek_key, encode_row_key_with_handle, RecordHandle,
};
use tidb_codec::encode_key;
use tidb_datatype::Datum;
use tidb_exec::cluster_catalog::{ClusterCatalog, LoadedDatabase};
use tidb_exec::keydecoder::{decode_key, HandleType};
use tidb_model::db::DBInfo;
use tidb_model::go_runtime::GoShared;
use tidb_model::index::IndexInfo;
use tidb_model::partition::{PartitionDefinition, PartitionInfo};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;

fn index(id: i64, name: &str) -> IndexInfo {
    IndexInfo {
        id,
        name: CiString::new(name),
        state: SchemaState::PUBLIC,
        ..IndexInfo::default()
    }
}

fn catalog() -> ClusterCatalog {
    ClusterCatalog {
        schema_version: 1,
        databases: vec![LoadedDatabase {
            info: DBInfo {
                id: 1,
                name: CiString::new("test"),
                ..DBInfo::default()
            },
            tables: vec![
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
            ],
        }],
    }
}

fn record_key(table_id: i64, handle: RecordHandle) -> Vec<u8> {
    encode_row_key_with_handle(table_id, &handle)
}

fn index_key(table_id: i64, index_id: i64, values: &[Datum]) -> Vec<u8> {
    encode_index_seek_key(table_id, index_id, &encode_key(values).unwrap())
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
        RecordHandle::Common(encode_key(&[Datum::Int(100), Datum::new_string("abc")]).unwrap());
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
        &index_key(1, 1, &[Datum::new_string("abc"), Datum::Int(1)]),
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
        &index_key(6, 4, &[Datum::new_string("abcde"), Datum::Int(2)]),
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
