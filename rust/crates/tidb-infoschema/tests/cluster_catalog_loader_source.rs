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

use tidb_infoschema::cluster_catalog::{
    load_executor_info_schema, load_info_schema, ClusterCatalogError, MetaPairs, MetaSnapshot,
};


use tidb_kv::key::prefix_scan_end;
use tidb_meta::{key, value};

#[test]
fn batched_schema_uses_current_runtime_entries_and_keeps_stored_metadata() {
    use std::sync::Arc;
    use tidb_ast::CiString;
    use tidb_infoschema::{SessionExtendedInfoSchema, TableEntry};
    use tidb_model::{db::DBInfo, SchemaState, TableInfo};
    use tidb_table::tables::TableCommon;

    let stored = |id, name, state| TableInfo {
        id,
        name: CiString::new(name),
        state,
        ..Default::default()
    };
    let db = tidb_model::GoShared::new(DBInfo {
        id: 501,
        name: CiString::new("ApP"),
        deprecated_tables: vec![
            stored(71, "Mixed", SchemaState::PUBLIC),
            stored(72, "MIXED", SchemaState::PUBLIC),
            stored(73, "Changing", SchemaState::DELETE_ONLY),
        ]
        .into(),
        ..Default::default()
    });
    let mut catalog = SessionExtendedInfoSchema::init_with_db_infos(
        Vec::new(), Vec::new(), Vec::new(), 1,
    );
    catalog.load_stored_schema_tables_from_handles(db, None, |table| {
        let table = table.read();
        (table.state == SchemaState::PUBLIC)
            .then(|| TableEntry::Kv(Arc::new(TableCommon::new(table.id, Vec::new()))))
    });
    catalog.prepare_v1_table_id_lookup(None);

    assert!(catalog.kv_table_by_id(71).is_none());
    assert!(catalog.kv_table_by_id(73).is_none());
    assert_eq!(catalog.kv_table_by_id(72).unwrap().name(), "MIXED");
    for id in 71..=73 {
        assert_eq!(catalog.table_info_by_id(id).unwrap().read().id, id);
        assert_eq!(catalog.schema_name_by_table_id(id).as_deref(), Some("ApP"));
    }

    let mut overlay = catalog.clone();
    overlay.adjust_table_row_count("app", "mixed", 5);
    assert!(Arc::ptr_eq(&overlay.table_id_names, &catalog.table_id_names));
    assert_eq!(overlay.kv_table_by_id(72).unwrap().storage_statistics().0, 5);
    assert_eq!(catalog.kv_table_by_id(72).unwrap().storage_statistics().0, 0);
}

#[test]
fn published_v1_runtime_ids_follow_schema_positions_after_a_reload() {
    use tidb_ast::CiString;
    use tidb_infoschema::SessionExtendedInfoSchema;
    use tidb_model::{db::DBInfo, GoShared};
    use tidb_table::tables::TableCommon;

    let db = |id, name| GoShared::new(DBInfo {
        id,
        name: CiString::new(name),
        ..Default::default()
    });
    let first = db(501, "First");
    let second = db(502, "Second");
    let mut previous = SessionExtendedInfoSchema::init_with_db_infos(
        vec![first.clone(), second.clone()], Vec::new(), Vec::new(), 1,
    );
    previous.register_kv_in("first", "alpha", TableCommon::new(71, Vec::new())).unwrap();
    previous.register_kv_in("second", "beta", TableCommon::new(72, Vec::new())).unwrap();
    previous.prepare_v1_table_id_lookup(None);

    let mut reloaded = SessionExtendedInfoSchema::init_with_db_infos(
        Vec::new(), Vec::new(), Vec::new(), 2,
    );
    assert!(reloaded.adopt_built_database(&previous, &second));
    assert!(reloaded.adopt_built_database(&previous, &first));
    reloaded.prepare_v1_table_id_lookup(Some(&previous));

    for catalog in [previous, reloaded] {
        let (schema, table) = catalog.table_by_id(71).unwrap();
        assert_eq!((schema, table.name().as_str()), ("first", "alpha"));
        let (schema, table) = catalog.table_by_id(72).unwrap();
        assert_eq!((schema, table.name().as_str()), ("second", "beta"));
    }
}

#[test]
fn published_v1_metadata_tracks_schema_replacement() {
    use tidb_ast::CiString;
    use tidb_infoschema::SessionExtendedInfoSchema;
    use tidb_model::{GoShared, GoSharedPointerSlice, SchemaState, TableInfo, db::DBInfo};

    // A metadata-only table in an intermediate DDL state has no runtime entry.
    let old = GoShared::new(TableInfo {
        id: 71,
        name: CiString::new("Changing"),
        state: SchemaState::DELETE_ONLY,
        ..Default::default()
    });
    let db = GoShared::new(DBInfo {
        id: 501,
        name: CiString::new("ApP"),
        deprecated_tables: GoSharedPointerSlice::from_handles(vec![Some(old.clone())]),
        ..Default::default()
    });
    let mut catalog = SessionExtendedInfoSchema::init_with_db_infos(vec![db], Vec::new(), Vec::new(), 1);
    catalog.prepare_v1_table_id_lookup(None);
    let previous = catalog.clone();
    assert!(previous.kv_table_by_id(71).is_none());
    assert!(previous.table_info_by_id(71).unwrap().ptr_eq(&old));
    assert_eq!(previous.schema_name_by_table_id(71).as_deref(), Some("ApP"));

    let replacement = GoShared::new(TableInfo {
        id: 72,
        name: CiString::new("Changing"),
        ..Default::default()
    });
    assert!(catalog.edit_schema_by_id(501, |db| {
        db.deprecated_tables = GoSharedPointerSlice::from_handles(vec![Some(replacement.clone())]);
    }));
    for prepare in [false, true] {
        if prepare {
            catalog.prepare_v1_table_id_lookup(Some(&previous));
        }
        assert!(catalog.table_info_by_id(71).is_none());
        assert!(catalog.schema_name_by_table_id(71).is_none());
        assert!(catalog.table_info_by_id(72).unwrap().ptr_eq(&replacement));
        assert_eq!(catalog.schema_name_by_table_id(72).as_deref(), Some("ApP"));
    }
    assert!(previous.table_info_by_id(71).unwrap().ptr_eq(&old));
    assert!(previous.table_info_by_id(72).is_none());
}

/// One immutable snapshot of stored meta bytes.
#[derive(Default)]
struct RecordedSnapshot {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
    /// The node's latest image the snapshot carries, if any.
    latest: Option<std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema>>,
    /// How many prefix scans the snapshot served.
    scans: usize,
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

    fn latest_info_schema(&self) -> Option<&std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema>> {
        self.latest.as_ref()
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        self.scans += 1;
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
/// admits and decodes as a `NULL`-able column.
const GO_NULLABLE_TABLE: &str = r#"{"id":78,"name":{"O":"Notes","L":"notes"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":1,"name":{"O":"id","L":"id"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":2,"name":{"O":"note","L":"note"},"offset":1,"type":{"Tp":15,"Flag":0,"Flen":64,"Decimal":-1,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":2,"version":5}"#;

/// A base table whose only unsupported column is `NOT NULL`, so the refusal
/// has to name the stored type rather than nullability.
const GO_UNSUPPORTED_TYPE_TABLE: &str = r#"{"id":79,"name":{"O":"Tags","L":"tags"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":1,"name":{"O":"id","L":"id"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":2,"name":{"O":"tag","L":"tag"},"offset":1,"type":{"Tp":245,"Flag":1,"Flen":-1,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":2,"version":5}"#;

/// A base table with a signed `DECIMAL(10,2) NOT NULL` column, widening the
/// admitted read domain beyond `GO_SUPPORTED_TABLE`'s original set.
const GO_DECIMAL_TABLE: &str = r#"{"id":80,"name":{"O":"Ledger","L":"ledger"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":1,"name":{"O":"id","L":"id"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":2,"name":{"O":"amount","L":"amount"},"offset":1,"type":{"Tp":246,"Flag":1,"Flen":10,"Decimal":2,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":2,"version":5}"#;

/// A base table with a `VARCHAR(64) NOT NULL` column and a
/// `VARBINARY(32) NOT NULL` column, exercising both charsets this node
/// admits for the `VARCHAR`/`VARSTRING` type code.
const GO_VARCHAR_TABLE: &str = r#"{"id":81,"name":{"O":"Tags2","L":"tags2"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":1,"name":{"O":"id","L":"id"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":2,"name":{"O":"tag","L":"tag"},"offset":1,"type":{"Tp":15,"Flag":1,"Flen":64,"Decimal":-1,"Charset":"utf8mb4","Collate":"utf8mb4_bin","Elems":null,"Array":false},"state":5,"version":2},
{"id":3,"name":{"O":"raw","L":"raw"},"offset":2,"type":{"Tp":15,"Flag":1,"Flen":32,"Decimal":-1,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":3,"version":5}"#;

/// A base table with a `VARCHAR(64) NOT NULL COLLATE utf8mb4_general_ci`
/// column. Its charset is `utf8mb4`, so a charset-only check admits it, but
/// the node's string comparators are hardcoded to `utf8mb4_bin`: admitting it
/// would order and group `'B'` before `'a'` where Go orders `'a'` before
/// `'B'`.
const GO_CI_COLLATION_TABLE: &str = r#"{"id":82,"name":{"O":"Labels","L":"labels"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":1,"name":{"O":"id","L":"id"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":2,"name":{"O":"name","L":"name"},"offset":1,"type":{"Tp":15,"Flag":1,"Flen":64,"Decimal":-1,"Charset":"utf8mb4","Collate":"utf8mb4_general_ci","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":2,"version":5}"#;

/// A base table with a `BINARY(16) NOT NULL` column: the `String` type code at
/// the `binary` charset, which `ConfiguredScalarType::Char` cannot express.
const GO_BINARY_CHAR_TABLE: &str = r#"{"id":83,"name":{"O":"Blobs","L":"blobs"},"charset":"utf8mb4","collate":"utf8mb4_bin","cols":[
{"id":1,"name":{"O":"id","L":"id"},"offset":0,"type":{"Tp":8,"Flag":3,"Flen":20,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2},
{"id":2,"name":{"O":"digest","L":"digest"},"offset":1,"type":{"Tp":254,"Flag":1,"Flen":16,"Decimal":0,"Charset":"binary","Collate":"binary","Elems":null,"Array":false},"state":5,"version":2}
],"index_info":null,"state":5,"pk_is_handle":true,"is_common_handle":false,"max_col_id":2,"version":5}"#;

fn recorded_cluster() -> RecordedSnapshot {
    let mut snapshot = RecordedSnapshot::default();
    snapshot.put(key::schema_version_kv_key(), value::encode_int_value(412));
    snapshot.put(key::database_kv_key(3), GO_DBINFO);
    snapshot.put(key::table_kv_key(3, 77), GO_SUPPORTED_TABLE);
    snapshot.put(key::table_kv_key(3, 78), GO_NULLABLE_TABLE);
    snapshot.put(key::table_kv_key(3, 79), GO_UNSUPPORTED_TYPE_TABLE);
    snapshot.put(key::table_kv_key(3, 80), GO_DECIMAL_TABLE);
    snapshot.put(key::table_kv_key(3, 81), GO_VARCHAR_TABLE);
    snapshot.put(key::table_kv_key(3, 82), GO_CI_COLLATION_TABLE);
    snapshot.put(key::table_kv_key(3, 83), GO_BINARY_CHAR_TABLE);
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
    let catalog = load_info_schema(&mut snapshot).expect("catalog loads");

    assert_eq!(catalog.schema_meta_version(), 412);
    // Only the stored databases: `METRICS_SCHEMA` is one of Go's virtual
    // schemas (`RegisterVirtualTable`), never stored.
    assert_eq!(catalog.stored_schemas().len(), 1);
    let database = &catalog.stored_schemas()[0];
    assert_eq!(database.read().id, 3);
    assert_eq!(database.read().name.original(), "Campaign");
    // Allocator fields in the same hash must not be mistaken for tables.
    let tables = database
        .read().deprecated_tables
        .iter_deref()
        .map(|table| table.read().clone())
        .collect::<Vec<_>>();
    assert_eq!(tables.len(), 7);
    assert_eq!(tables[0].id, 77);
    assert_eq!(tables[0].db_id, 3);
    assert_eq!(tables[1].id, 78);
}

#[test]
fn absent_schema_version_reads_as_go_zero() {
    let mut snapshot = RecordedSnapshot::default();
    snapshot.put(key::database_kv_key(3), GO_DBINFO);
    let catalog = load_info_schema(&mut snapshot).expect("catalog loads");
    assert_eq!(catalog.schema_meta_version(), 0);
    assert!(catalog.stored_schemas()[0].read().deprecated_tables.iter_deref().next().is_none());
}

#[test]
fn prefix_scan_end_increments_the_last_byte_below_ff() {
    assert_eq!(prefix_scan_end(b"ab"), Some(b"ac".to_vec()));
    assert_eq!(prefix_scan_end(&[0x01, 0xFF]), Some(vec![0x02]));
    assert_eq!(prefix_scan_end(&[0xFF, 0xFF]), None);
    assert_eq!(prefix_scan_end(b""), None);
}

/// Go `initVirtualTables` adds each memory schema's driver `DBInfo`, whose
/// `Deprecated.Tables` lists its tables, so a loaded infoschema's
/// `TableInfoByName` answers a memory table as it answers a stored one.
#[test]
fn a_loaded_image_answers_memory_tables_by_name() {
    let mut snapshot = RecordedSnapshot::default();
    let image = load_info_schema(&mut snapshot).unwrap();
    let (schema, table) = image
        .table_info_by_name("INFORMATION_SCHEMA", "Tables")
        .expect("a memory table");
    assert_eq!(schema.read().id, (1 << 62) | 1);
    assert_eq!(table.read().name.lowercase(), "tables");
    assert!(image.stored_schemas().is_empty());
}

/// Go `Loader.fetchSchemasWithTables`: V2 keeps names but decodes only
/// tables selected by `IsTableInfoMustLoad`; V1 still loads every table.
#[test]
fn v2_full_load_retains_only_must_loaded_metadata() {
    let mut snapshot = recorded_cluster();
    // Current Go TableInfo JSON includes these null/default attributes in
    // this order. Older JSON without them is deliberately must-load.
    for id in 77..=83 {
        let raw = snapshot.pairs.get_mut(&key::table_kv_key(3, id)).unwrap();
        raw.pop();
        raw.extend_from_slice(br#","fk_info":null,"partition":null,"Lock":null,"tiflash_replica":"#);
        raw.extend_from_slice(if id == 78 { br#"{"count":1}"# } else { b"null" });
        raw.extend_from_slice(br#","temp_table_type":0,"policy_ref_info":null,"ttl_info":null}"#);
    }
    let v2 = tidb_infoschema::cluster_catalog::load_info_schema_for_mode(&mut snapshot, &mut |_, _| true, true).unwrap();
    let db = v2.stored_schemas().into_iter().find(|db| db.read().id == 3).unwrap();
    assert_eq!(db.read().table_name2id.as_ref().unwrap().read().len(), 7);
    let residents: Vec<_> = db.read().deprecated_tables.iter_deref().map(|t| t.read().id).collect();
    assert_eq!(residents, vec![78]);
    let v1 = load_info_schema(&mut snapshot).unwrap();
    let db = v1.stored_schemas().into_iter().find(|db| db.read().id == 3).unwrap();
    assert_eq!(db.read().deprecated_tables.len(), 7);
}

/// A DDL executor statement reads the node's latest image when it is at the
/// snapshot's schema version (Go `Loader`'s version cache hit) without
/// scanning meta, and loads the snapshot's image otherwise.
#[test]
fn executor_schema_is_the_latest_image_only_at_the_snapshot_version() {
    let mut fresh = recorded_cluster();
    let latest = std::sync::Arc::new(load_info_schema(&mut fresh).expect("catalog loads"));

    let mut hit = recorded_cluster();
    hit.latest = Some(std::sync::Arc::clone(&latest));
    let image = load_executor_info_schema(&mut hit).expect("catalog loads");
    assert!(std::sync::Arc::ptr_eq(&image, &latest));
    assert_eq!(hit.scans, 0);

    // The node is behind the snapshot: its image is not the snapshot's.
    let mut behind = recorded_cluster();
    behind.put(key::schema_version_kv_key(), value::encode_int_value(413));
    behind.latest = Some(std::sync::Arc::clone(&latest));
    let image = load_executor_info_schema(&mut behind).expect("catalog loads");
    assert!(!std::sync::Arc::ptr_eq(&image, &latest));
    assert_eq!(image.schema_meta_version(), 413);
    assert!(behind.scans > 0);

    let mut none = recorded_cluster();
    let image = load_executor_info_schema(&mut none).expect("catalog loads");
    assert_eq!(image.schema_meta_version(), 412);
}

// Go's test binary links `planner/core`, whose `init` sets `expression.BuildSimpleExpr`.
tidb_planner_init::link!();
