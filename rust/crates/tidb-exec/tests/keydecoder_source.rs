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

//! Public source contract for Go `pkg/util/keydecoder`.

use tidb_ast::CiString;
use tidb_codec::table_key::{
    encode_index_seek_key, encode_row_key_with_handle, RecordHandle, TableKeyError,
};
use tidb_codec::{encode_key, gen_table_record_prefix};
use tidb_datatype::Datum;
use tidb_exec::cluster_catalog::{ClusterCatalog, LoadedDatabase};
use tidb_exec::keydecoder::{
    decode_key, DecodedKey, HandleType, KeyDecoderError, KeyInfoCatalog, KeyInfoTableLookup,
};
use tidb_executor::{Catalog, KvTable};
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

fn partitioned_table() -> TableInfo {
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
                partitioned_table(),
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

fn assert_json(decoded: &DecodedKey, expected: &str) {
    assert_eq!(serde_json::to_string(decoded).unwrap(), expected);
}

struct LogicalTableWithoutSchema;

impl KeyInfoCatalog for LogicalTableWithoutSchema {
    fn resolve_physical_table(&self, physical_id: i64) -> Option<KeyInfoTableLookup> {
        Some(KeyInfoTableLookup::TableWithoutSchema {
            table_name: "orphaned_table".to_owned(),
            table_id: physical_id,
        })
    }
}

#[test]
fn logical_table_without_schema_stops_before_payload_decode() {
    let catalog = LogicalTableWithoutSchema;

    let mut malformed_record = gen_table_record_prefix(9);
    malformed_record.push(0xff);
    let decoded = decode_key(&malformed_record, &catalog).unwrap();
    assert_json(&decoded, r#"{"table_name":"orphaned_table","table_id":9}"#);

    let decoded = decode_key(
        &index_key(9, 7, &[Datum::new_string("must-not-be-decoded")]),
        &catalog,
    )
    .unwrap();
    assert_json(&decoded, r#"{"table_name":"orphaned_table","table_id":9}"#);
}

#[test]
fn integer_and_common_record_handles_match_go() {
    let catalog = catalog();
    let decoded = decode_key(&record_key(1, RecordHandle::Int(1)), &catalog).unwrap();
    assert_eq!(decoded.db_id, 1);
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_id, 1);
    assert_eq!(decoded.table_name, "table1");
    assert_eq!(decoded.partition_id, 0);
    assert_eq!(decoded.partition_name, "");
    assert_eq!(decoded.handle_type, HandleType::Int);
    assert_eq!(decoded.handle_value, "1");
    assert!(!decoded.is_partition_handle);
    assert_eq!(decoded.index_id, 0);
    assert_eq!(decoded.index_name, "");
    assert_eq!(decoded.index_values, None);
    assert_json(
        &decoded,
        r#"{"db_name":"test","table_name":"table1","handle_type":"int","handle_value":"1","db_id":1,"table_id":1}"#,
    );

    let common =
        RecordHandle::Common(encode_key(&[Datum::Int(100), Datum::new_string("abc")]).unwrap());
    let decoded = decode_key(&record_key(2, common), &catalog).unwrap();
    assert_eq!(decoded.db_id, 1);
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_id, 2);
    assert_eq!(decoded.table_name, "table2");
    assert_eq!(decoded.handle_type, HandleType::Common);
    assert_eq!(decoded.handle_value, "{100, abc}");
    assert!(!decoded.is_partition_handle);
    assert_json(
        &decoded,
        r#"{"db_name":"test","table_name":"table2","handle_type":"common","handle_value":"{100, abc}","db_id":1,"table_id":2}"#,
    );
}

#[test]
fn executor_catalog_retains_the_source_database_id() {
    let mut catalog = Catalog::default();
    catalog.register_database_with_id("test", 42);
    let mut table = KvTable::new(7, Vec::new());
    table.set_name("table7");
    catalog.register_kv_in("test", "table7", table).unwrap();

    let decoded = decode_key(&record_key(7, RecordHandle::Int(9)), &catalog).unwrap();
    assert_json(
        &decoded,
        r#"{"db_name":"test","table_name":"table7","handle_type":"int","handle_value":"9","db_id":42,"table_id":7}"#,
    );
}

#[test]
fn index_and_partition_metadata_match_go() {
    let catalog = catalog();
    let decoded = decode_key(
        &index_key(1, 1, &[Datum::new_string("abc"), Datum::Int(1)]),
        &catalog,
    )
    .unwrap();
    assert_eq!(decoded.db_id, 1);
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_id, 1);
    assert_eq!(decoded.table_name, "table1");
    assert_eq!(decoded.index_id, 1);
    assert_eq!(decoded.index_name, "index1");
    assert_eq!(decoded.index_values, Some(vec!["abc".into(), "1".into()]));
    assert_eq!(decoded.handle_type, HandleType::None);
    assert!(!decoded.is_partition_handle);
    assert_json(
        &decoded,
        r#"{"db_name":"test","table_name":"table1","index_name":"index1","index_values":["abc","1"],"db_id":1,"table_id":1,"index_id":1}"#,
    );

    let decoded = decode_key(&record_key(5, RecordHandle::Int(10)), &catalog).unwrap();
    assert_eq!(decoded.db_id, 1);
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_id, 3);
    assert_eq!(decoded.table_name, "table3");
    assert_eq!(decoded.partition_id, 5);
    assert_eq!(decoded.partition_name, "p0");
    assert_eq!(decoded.handle_type, HandleType::Int);
    assert_eq!(decoded.handle_value, "10");
    assert!(!decoded.is_partition_handle);
    assert_json(
        &decoded,
        r#"{"db_name":"test","table_name":"table3","partition_name":"p0","handle_type":"int","handle_value":"10","db_id":1,"table_id":3,"partition_id":5}"#,
    );

    let decoded = decode_key(
        &index_key(6, 4, &[Datum::new_string("abcde"), Datum::Int(2)]),
        &catalog,
    )
    .unwrap();
    assert_eq!(decoded.table_id, 3);
    assert_eq!(decoded.table_name, "table3");
    assert_eq!(decoded.partition_id, 6);
    assert_eq!(decoded.partition_name, "p1");
    assert_eq!(decoded.index_id, 4);
    assert_eq!(decoded.index_name, "index4");
    assert_eq!(decoded.index_values, Some(vec!["abcde".into(), "2".into()]));
    assert_json(
        &decoded,
        r#"{"db_name":"test","table_name":"table3","partition_name":"p1","index_name":"index4","index_values":["abcde","2"],"db_id":1,"table_id":3,"partition_id":6,"index_id":4}"#,
    );
}

#[test]
fn missing_and_invalid_keys_preserve_go_results() {
    let catalog = catalog();
    assert_json(&DecodedKey::default(), r#"{"table_id":0}"#);

    let decoded = decode_key(&record_key(4, RecordHandle::Int(1)), &catalog).unwrap();
    assert_eq!(decoded.table_id, 4);
    assert_eq!(decoded.handle_type, HandleType::Int);
    assert_eq!(decoded.handle_value, "1");
    assert_eq!(decoded.db_id, 0);
    assert_eq!(decoded.table_name, "");
    assert_json(
        &decoded,
        r#"{"handle_type":"int","handle_value":"1","table_id":4}"#,
    );

    let decoded = decode_key(&index_key(1, 2, &[Datum::new_string("abc")]), &catalog).unwrap();
    assert_eq!(decoded.index_id, 2);
    assert_eq!(decoded.index_name, "");
    assert_eq!(decoded.index_values, None);
    assert_json(
        &decoded,
        r#"{"db_name":"test","table_name":"table1","db_id":1,"table_id":1,"index_id":2}"#,
    );

    let decoded = decode_key(&index_key(4, 2, &[Datum::new_string("abc")]), &catalog).unwrap();
    assert_eq!(decoded.table_id, 4);
    assert_eq!(decoded.index_id, 2);
    assert_eq!(decoded.index_name, "");
    assert_eq!(decoded.index_values, None);
    assert_json(&decoded, r#"{"table_id":4,"index_id":2}"#);

    let empty_index = decode_key(&index_key(1, 1, &[]), &catalog).unwrap();
    assert_eq!(empty_index.index_name, "index1");
    assert_eq!(empty_index.index_values, None);

    let mut broken_index = index_key(1, 1, &[]);
    broken_index.push(0xff);
    let decoded = decode_key(&broken_index, &catalog).unwrap();
    assert_eq!(decoded.db_name, "test");
    assert_eq!(decoded.table_name, "table1");
    assert_eq!(decoded.index_id, 0);
    assert_eq!(decoded.index_values, None);

    let mut broken_record = gen_table_record_prefix(1);
    broken_record.push(0xff);
    let failure = decode_key(&broken_record, &catalog).unwrap_err();
    assert_eq!(failure.decoded.db_id, 1);
    assert_eq!(failure.decoded.table_name, "table1");
    assert_eq!(failure.decoded.handle_type, HandleType::None);
    assert_eq!(
        failure.error,
        KeyDecoderError::InvalidRecord { table_id: 1 }
    );
    assert_eq!(failure.to_string(), "cannot decode record key of table 1");

    let mut broken_partition_record = gen_table_record_prefix(5);
    broken_partition_record.push(0xff);
    let failure = decode_key(&broken_partition_record, &catalog).unwrap_err();
    assert_eq!(failure.decoded.table_id, 3);
    assert_eq!(failure.decoded.partition_id, 5);
    assert_eq!(
        failure.error,
        KeyDecoderError::InvalidRecord { table_id: 5 }
    );
    assert_eq!(failure.to_string(), "cannot decode record key of table 5");

    let failure = decode_key(b"this-is-a-totally-invalidkey", &catalog).unwrap_err();
    assert_eq!(failure.decoded.as_ref(), &DecodedKey::default());
    assert!(matches!(failure.error, KeyDecoderError::UnknownKey(_)));

    let failure = decode_key(b"invalid", &catalog).unwrap_err();
    assert_eq!(
        failure.to_string(),
        "Unknown key type for key [105 110 118 97 108 105 100]"
    );

    let mut invalid_head = index_key(1, 1, &[]);
    invalid_head[9] = b'x';
    let failure = decode_key(&invalid_head, &catalog).unwrap_err();
    assert_eq!(failure.decoded.as_ref(), &DecodedKey::default());
    assert_eq!(
        failure.error,
        KeyDecoderError::InvalidHead(TableKeyError::InvalidKey)
    );
}
