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

use tidb_executor::TableEntry;
use tidb_tablecodec::table_key::{decode_record_key, encode_table_prefix};

use crate::tests_support::{row_text, warnings_of};
use crate::Session;

fn one(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))[0][0].clone()
}

fn uppercase_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn wrapped_hex(key: &[u8]) -> String {
    let mut encoded = Vec::new();
    for group in key.chunks(8) {
        encoded.extend_from_slice(group);
        let padding = 8 - group.len();
        encoded.resize(encoded.len() + padding, 0);
        encoded.push(0xff - padding as u8);
    }
    if key.len().is_multiple_of(8) {
        encoded.extend_from_slice(&[0; 8]);
        encoded.push(0xf7);
    }
    uppercase_hex(&encoded)
}

fn table_ids(session: &Session, name: &str) -> (i64, Vec<i64>) {
    let catalog = session.shared_catalog();
    let catalog = catalog.lock().unwrap();
    let TableEntry::Kv(table) = catalog.table_in("test", name).unwrap() else {
        panic!("{name} must be a KV table");
    };
    (
        table.table_id,
        table.indexes().iter().map(|index| index.id).collect(),
    )
}

#[test]
fn tidb_decode_key_matches_go_generic_and_warning_vectors() {
    let mut session = Session::new();
    assert_eq!(
        one(
            &mut session,
            "SELECT TIDB_DECODE_KEY('74800000000000002B5F72800000000000A5D3')",
        ),
        r#"{"_tidb_rowid":42451,"table_id":"43"}"#
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT TIDB_DECODE_KEY('74800000000000ffff5f7205bff199999999999a013131000000000000f9')",
        ),
        r#"{"handle":"{1.1, 11}","table_id":65535}"#
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT TIDB_DECODE_KEY('74800000000000019B5F698000000000000001015257303100000000FB013736383232313130FF3900000000000000F8010000000000000000F7')",
        ),
        r#"{"index_id":1,"index_vals":"RW01, 768221109, ","table_id":411}"#
    );
    assert_eq!(
        one(
            &mut session,
            "SELECT TIDB_DECODE_KEY('7480000000000000FF4700000000000000F8')",
        ),
        r#"{"table_id":71}"#
    );
    assert_eq!(one(&mut session, "SELECT TIDB_DECODE_KEY(123)"), "123");
    assert_eq!(
        warnings_of(&session),
        vec![(1105, "invalid key: 12".to_owned())]
    );

    let invalid = "7480000000000000FF2E5F728000000011FFE1A3000000000000";
    assert_eq!(
        one(
            &mut session,
            &format!("SELECT TIDB_DECODE_KEY('{invalid}')"),
        ),
        invalid
    );
    assert_eq!(
        warnings_of(&session),
        vec![(1105, format!("invalid key: {invalid}"))]
    );
    assert_eq!(one(&mut session, "SELECT TIDB_DECODE_KEY(NULL)"), "NULL");
}

#[test]
fn tidb_decode_key_uses_table_column_and_index_metadata() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a VARCHAR(255), b INT, c DATETIME, PRIMARY KEY (a,b,c))")
        .unwrap();
    let (table_id, _) = table_ids(&session, "t");
    session
        .run("INSERT INTO t VALUES ('bbbb', 10, '2020-01-01 00:00:00')")
        .unwrap();
    let key = {
        let catalog = session.shared_catalog();
        let mut catalog = catalog.lock().unwrap();
        let TableEntry::Kv(table) = catalog.table_mut_in("test", "t").unwrap() else {
            panic!("t must be a KV table");
        };
        table
            .stored_keys()
            .unwrap()
            .into_iter()
            .find(|key| tidb_tablecodec::is_record_key(key))
            .unwrap()
    };
    assert_eq!(
        one(
            &mut session,
            &format!("SELECT TIDB_DECODE_KEY('{}')", wrapped_hex(&key)),
        ),
        format!(
            r#"{{"handle":{{"a":"bbbb","b":"10","c":"2020-01-01 00:00:00"}},"table_id":{table_id}}}"#
        )
    );
    let (_, handle) = decode_record_key(&key).unwrap();
    let mut columns = handle.encoded_columns().unwrap();
    columns[2] = vec![0];
    let mut null_key = encode_table_prefix(table_id);
    null_key.extend_from_slice(b"_r");
    for column in columns {
        null_key.extend_from_slice(&column);
    }
    let null_hex = wrapped_hex(&null_key);
    assert_eq!(
        one(
            &mut session,
            &format!("SELECT TIDB_DECODE_KEY('{null_hex}')"),
        ),
        null_hex
    );

    session.run("DROP TABLE t").unwrap();
    session
        .run("CREATE TABLE t (a VARCHAR(255), b INT, c DATETIME, INDEX idx(a,b,c))")
        .unwrap();
    let (table_id, _) = table_ids(&session, "t");
    session
        .run("INSERT INTO t VALUES ('aaaaa', 100, '2000-01-01 00:00:00')")
        .unwrap();
    let key = {
        let catalog = session.shared_catalog();
        let mut catalog = catalog.lock().unwrap();
        let TableEntry::Kv(table) = catalog.table_mut_in("test", "t").unwrap() else {
            panic!("t must be a KV table");
        };
        table
            .stored_keys()
            .unwrap()
            .into_iter()
            .find(|key| tidb_tablecodec::is_index_key(key))
            .unwrap()
    };
    assert_eq!(
        one(
            &mut session,
            &format!("SELECT TIDB_DECODE_KEY('{}')", wrapped_hex(&key)),
        ),
        format!(
            r#"{{"index_id":1,"index_vals":{{"a":"aaaaa","b":"100","c":"2000-01-01 00:00:00"}},"table_id":{table_id}}}"#
        )
    );
}

#[test]
fn tidb_decode_key_distinguishes_clustered_and_partition_handles() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY CLUSTERED, b INT, KEY bk(b)) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))")
        .unwrap();
    session.run("INSERT INTO t VALUES (7, 100)").unwrap();
    let (table_id, indexes, partition_id) = {
        let catalog = session.shared_catalog();
        let catalog = catalog.lock().unwrap();
        let TableEntry::Kv(table) = catalog.table_in("test", "t").unwrap() else {
            panic!("t must be a KV table");
        };
        (
            table.table_id,
            table
                .indexes()
                .iter()
                .map(|index| index.id)
                .collect::<Vec<_>>(),
            table.partition().unwrap().definitions[0].id,
        )
    };
    let (record, index) = {
        let catalog = session.shared_catalog();
        let mut catalog = catalog.lock().unwrap();
        let TableEntry::Kv(table) = catalog.table_mut_in("test", "t").unwrap() else {
            panic!("t must be a KV table");
        };
        let keys = table.stored_keys().unwrap();
        (
            keys.iter()
                .find(|key| tidb_tablecodec::is_record_key(key))
                .unwrap()
                .clone(),
            keys.iter()
                .find(|key| tidb_tablecodec::is_index_key(key))
                .unwrap()
                .clone(),
        )
    };
    assert_eq!(
        one(
            &mut session,
            &format!("SELECT TIDB_DECODE_KEY('{}')", wrapped_hex(&record)),
        ),
        format!(r#"{{"a":7,"partition_id":{partition_id},"table_id":"{table_id}"}}"#)
    );
    assert_eq!(
        one(
            &mut session,
            &format!(
                "SELECT TIDB_DECODE_KEY('{}')",
                uppercase_hex(&encode_table_prefix(partition_id))
            ),
        ),
        format!(r#"{{"partition_id":{partition_id},"table_id":{table_id}}}"#)
    );
    assert_eq!(indexes[0], 1);
    assert_eq!(
        one(
            &mut session,
            &format!("SELECT TIDB_DECODE_KEY('{}')", wrapped_hex(&index)),
        ),
        format!(
            r#"{{"index_id":1,"index_vals":{{"b":"100"}},"partition_id":{partition_id},"table_id":{table_id}}}"#
        )
    );
}
