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

//! Source-backed tests for the cluster catalog loader.
//!
//! The stored bytes are the same Go-produced `DBInfo`/`TableInfo` JSON shapes
//! `tidb-meta`'s `go_vectors` tests pin, laid under keys built by the same
//! `tidb-meta` key codec a live cluster writes. Go source of truth:
//! `pkg/meta/meta.go` `ListDatabases`/`ListTables`/`GetMetasByDBID`.

use std::collections::BTreeMap;

use tidb_exec::cluster_catalog::{
    configure_loaded_table, load_cluster_catalog, prefix_scan_end, ClusterCatalogError, MetaPairs,
    MetaSnapshot,
};
use tidb_meta::{key, value};

/// One immutable snapshot of stored meta bytes.
#[derive(Default)]
struct RecordedSnapshot {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl RecordedSnapshot {
    fn put(&mut self, raw_key: Vec<u8>, raw_value: impl Into<Vec<u8>>) {
        self.pairs.insert(raw_key, raw_value.into());
    }
}

impl MetaSnapshot for RecordedSnapshot {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(raw_key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        let end = prefix_scan_end(prefix).expect("finite scan end");
        Ok(self
            .pairs
            .range(prefix.to_vec()..end)
            .map(|(stored_key, stored_value)| (stored_key.clone(), stored_value.clone()))
            .collect())
    }
}

const GO_DBINFO: &str = r#"{"id":3,"db_name":{"O":"Campaign","L":"campaign"},"charset":"utf8mb4","collate":"utf8mb4_bin","Deprecated":{},"state":5,"policy_ref_info":null}"#;

/// A base table whose every column is inside the widened read domain:
/// signed `BIGINT` handle, `BIGINT`, `BIGINT UNSIGNED`, `DOUBLE`, `CHAR(16)`.
const GO_SUPPORTED_TABLE: &str = r#"{"id":77,"name":{"O":"Rows","L":"rows"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":1,"name":{"O":"id","L":"id"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":2,"name":{"O":"balance","L":"balance"},"offset":1,"type":{"Tp":8,"Flag":1,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":3,"name":{"O":"counter","L":"counter"},"offset":2,"type":{"Tp":8,"Flag":33,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":4,"name":{"O":"score","L":"score"},"offset":3,"type":{"Tp":5,"Flag":1,"Flen":22,"Decimal":-1,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":5,"name":{"O":"label","L":"label"},"offset":4,"type":{"Tp":254,"Flag":1,"Flen":16,"Decimal":0,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":5,"version":5}"#;

/// The same shape with one nullable `VARCHAR(64)` column, which the read path
/// cannot decode yet.
const GO_UNSUPPORTED_TABLE: &str = r#"{"id":78,"name":{"O":"Notes","L":"notes"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":1,"name":{"O":"id","L":"id"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":2,"name":{"O":"note","L":"note"},"offset":1,"type":{"Tp":15,"Flag":0,"Flen":64,"Decimal":-1,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":2,"version":5}"#;

/// A base table whose only unsupported column is `NOT NULL`, so the refusal
/// has to name the stored type rather than nullability.
const GO_UNSUPPORTED_TYPE_TABLE: &str = r#"{"id":79,"name":{"O":"Tags","L":"tags"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":1,"name":{"O":"id","L":"id"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":2,"name":{"O":"tag","L":"tag"},"offset":1,"type":{"Tp":15,"Flag":1,"Flen":64,"Decimal":-1,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":2,"version":5}"#;

fn recorded_cluster() -> RecordedSnapshot {
    let mut snapshot = RecordedSnapshot::default();
    snapshot.put(key::schema_version_kv_key(), value::encode_int_value(412));
    snapshot.put(key::database_kv_key(3), GO_DBINFO);
    snapshot.put(key::table_kv_key(3, 77), GO_SUPPORTED_TABLE);
    snapshot.put(key::table_kv_key(3, 78), GO_UNSUPPORTED_TABLE);
    snapshot.put(key::table_kv_key(3, 79), GO_UNSUPPORTED_TYPE_TABLE);
    // The same database hash also holds allocator fields, which are not tables.
    snapshot.put(
        key::auto_table_id_kv_key(3, 77),
        value::encode_int_value(9000),
    );
    snapshot.put(
        key::auto_increment_id_kv_key(3, 77),
        value::encode_int_value(120),
    );
    snapshot
}

#[test]
fn loads_databases_tables_and_schema_version_from_stored_bytes() {
    let mut snapshot = recorded_cluster();
    let catalog = load_cluster_catalog(&mut snapshot).expect("catalog loads");

    assert_eq!(catalog.schema_version, 412);
    assert_eq!(catalog.databases.len(), 1);
    let database = &catalog.databases[0];
    assert_eq!(database.info.id, 3);
    assert_eq!(database.info.name.original(), "Campaign");
    // Allocator fields in the same hash must not be mistaken for tables.
    assert_eq!(database.tables.len(), 3);
    assert_eq!(database.tables[0].id, 77);
    assert_eq!(database.tables[0].db_id, 3);
    assert_eq!(database.tables[1].id, 78);
}

#[test]
fn absent_schema_version_reads_as_go_zero() {
    let mut snapshot = RecordedSnapshot::default();
    snapshot.put(key::database_kv_key(3), GO_DBINFO);
    let catalog = load_cluster_catalog(&mut snapshot).expect("catalog loads");
    assert_eq!(catalog.schema_version, 0);
    assert!(catalog.databases[0].tables.is_empty());
}

#[test]
fn supported_loaded_table_becomes_a_configured_table() {
    let mut snapshot = recorded_cluster();
    let catalog = load_cluster_catalog(&mut snapshot).expect("catalog loads");
    let (database, table) = catalog.find_table("campaign", "rows").expect("table found");

    let configured = configure_loaded_table(database.name.original(), table).expect("table admitted");
    assert_eq!(configured.table_id(), 77);
    assert_eq!(configured.schema(), "Campaign");
    assert_eq!(configured.table(), "Rows");
    let names: Vec<_> = configured
        .columns()
        .iter()
        .map(|column| (column.name().to_owned(), column.id()))
        .collect();
    assert_eq!(
        names,
        vec![
            ("id".to_owned(), 1),
            ("balance".to_owned(), 2),
            ("counter".to_owned(), 3),
            ("score".to_owned(), 4),
            ("label".to_owned(), 5),
        ]
    );
    configured.validate().expect("configured table is valid");
}

#[test]
fn unsupported_column_is_refused_by_name_and_type() {
    let mut snapshot = recorded_cluster();
    let catalog = load_cluster_catalog(&mut snapshot).expect("catalog loads");
    let (database, table) = catalog.find_table("campaign", "notes").expect("table found");

    let refusal =
        configure_loaded_table(database.name.original(), table).expect_err("table must be refused");
    assert_eq!(refusal.name, "Campaign.Notes");
    assert!(
        refusal.reason.contains("`note`"),
        "refusal must name the column: {}",
        refusal.reason
    );
    assert!(
        refusal.reason.contains("nullable"),
        "refusal must name the reason: {}",
        refusal.reason
    );
}

#[test]
fn unsupported_not_null_column_is_refused_by_its_stored_type() {
    let mut snapshot = recorded_cluster();
    let catalog = load_cluster_catalog(&mut snapshot).expect("catalog loads");
    let (database, table) = catalog.find_table("campaign", "tags").expect("table found");

    let refusal =
        configure_loaded_table(database.name.original(), table).expect_err("table must be refused");
    assert_eq!(
        refusal.reason,
        "column `tag` has type VARCHAR(64), which this node cannot decode yet"
    );
    assert_eq!(
        refusal.to_string(),
        "table Campaign.Tags is present in the cluster catalog but cannot be read by this node: \
         column `tag` has type VARCHAR(64), which this node cannot decode yet"
    );
}

#[test]
fn prefix_scan_end_increments_the_last_byte_below_ff() {
    assert_eq!(prefix_scan_end(b"ab"), Some(b"ac".to_vec()));
    assert_eq!(prefix_scan_end(&[0x01, 0xFF]), Some(vec![0x02]));
    assert_eq!(prefix_scan_end(&[0xFF, 0xFF]), None);
    assert_eq!(prefix_scan_end(b""), None);
}
