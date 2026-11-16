// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
use super::*;
use tidb_model::table::FKInfo;
use tidb_model::SchemaState;

#[derive(Default)]
struct Store {
    tables: Mutex<HashMap<(i64, i64), CachedTable>>,
    reads: Mutex<Vec<(i64, i64, u64)>>,
}
impl TableLoader for Store {
    fn load_table(&self, db: i64, id: i64, ts: u64) -> Result<CachedTable, ClusterCatalogError> {
        self.reads.lock().unwrap().push((db, id, ts));
        self.tables
            .lock()
            .unwrap()
            .get(&(db, id))
            .cloned()
            .ok_or_else(|| not_exists("db", "table"))
    }
    fn list_tables(
        &self,
        db: i64,
        _ts: u64,
    ) -> Result<Vec<GoShared<TableInfo>>, ClusterCatalogError> {
        Ok(self
            .tables
            .lock()
            .unwrap()
            .iter()
            .filter(|((d, _), _)| *d == db)
            .map(|(_, t)| t.info.clone())
            .collect())
    }
}
fn database(id: i64, name: &str) -> DBInfo {
    DBInfo {
        id,
        name: CiString::new(name),
        state: SchemaState::PUBLIC,
        ..Default::default()
    }
}
fn table(id: i64, name: &str) -> CachedTable {
    let mut info = TableInfo {
        id,
        db_id: 1,
        name: CiString::new(name),
        state: SchemaState::PUBLIC,
        ..Default::default()
    };
    info.columns = vec![tidb_model::column::ColumnInfo {
        id: 1,
        name: CiString::new("a"),
        state: SchemaState::PUBLIC,
        field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ..Default::default()
    }]
    .into();
    let ids = tidb_table::table_from_meta::LocalTableAutoIds::default();
    let entry = tidb_table::table_from_meta::cluster_table(
        &info,
        &tidb_table::table_from_meta::AutoIdSource::In {
            db_id: 1,
            ids: &ids,
        },
    )
    .unwrap();
    let info = entry.meta();
    let table = Arc::new(TableEntry::Kv(Arc::new(entry)));
    let size = table_cache_entry_size(&info.read(), &table);
    CachedTable { info, table, size }
}
fn item(db: &str, table: &CachedTable, version: i64) -> TableItem {
    let info = table.info.read();
    TableItem {
        db_name: CiString::new(db),
        db_id: info.db_id,
        table_name: info.name.clone(),
        table_id: info.id,
        schema_version: version,
    }
}
fn fixture() -> (InfoSchemaV2, Arc<Store>) {
    let data = Arc::new(Data::new());
    data.add_db(1, database(1, "testDB"));
    let store = Arc::new(Store::default());
    (InfoSchemaV2::new(data, store.clone(), 2, 42), store)
}
/// Go `TestV2Basic`, pkg/infoschema/infoschema_v2_test.go.
#[test]
fn test_v2_basic() {
    let (mut is, store) = fixture();
    let table = table(2, "test");
    store.tables.lock().unwrap().insert((1, 2), table.clone());
    is.data.add(item("testDB", &table, 2), table.clone());
    is.schema_meta_version = 1;
    assert_eq!(is.all_schemas().len(), 1);
    assert!(is.table_by_id(2, None).is_none());
    is.schema_meta_version = 2;
    assert_eq!(is.all_schemas().len(), 1);
    assert_eq!(is.schema_by_name("TESTdb").unwrap().read().id, 1);
    assert_eq!(is.schema_by_id(1).unwrap().read().name.original(), "testDB");
    assert_eq!(
        is.table_by_name("testDB", "test", None)
            .unwrap()
            .info
            .read()
            .id,
        2
    );
    assert!(is.table_by_name("testDB", "notexist", None).is_err());
    assert!(is.table_by_id(-1, None).is_none());
    assert!(is.table_by_id(1234567, None).is_none());
    assert!(is.schema_by_id(-1).is_none());
    assert_eq!(is.schema_table_infos("testDB").unwrap().len(), 1);
    assert!(is.schema_table_infos("notexist").unwrap().is_empty());
    assert_eq!(is.table_item_by_id(2).unwrap().db_name.original(), "testDB");
    assert!(is.table_item_by_id(3).is_none());
    is.evict_table("testDB", "test");
    assert!(!is.table_is_cached(2));
    assert_eq!(is.table_by_id(2, None).unwrap().info.read().id, 2);
    assert!(!is.table_is_cached(2)); // Go TableByID does not refill by default.
    assert_eq!(store.reads.lock().unwrap().as_slice(), &[(1, 2, 42)]);
    is.table_by_name("testdb", "test", Some(false)).unwrap();
    assert!(!is.table_is_cached(2));
    is.table_by_name("testdb", "test", None).unwrap();
    assert!(is.table_is_cached(2));
    let cached = is.table_by_id(2, None).unwrap();
    is.schema_meta_version = 8;
    assert!(Arc::ptr_eq(
        &cached,
        &is.table_by_name("testdb", "test", None).unwrap()
    ));
    assert!(is.data.table_cache.contains(&TableCacheKey {
        table_id: 2,
        schema_version: 2
    }));
}
/// Go `TestInfoschemaCache`, pkg/infoschema/infoschemav2_cache_test.go.
/// Capacity follows Go's three sizing-fixture entries; longer names push it over.
#[test]
fn test_infoschema_cache() {
    for by_name in [false, true] {
        let (mut is, store) = fixture();
        is.schema_meta_version = 4;
        is.data.set_cache_capacity(3 * table(1, "t").size);
        for id in 1..=4 {
            let table = table(id, &format!("t{id}"));
            store.tables.lock().unwrap().insert((1, id), table.clone());
            is.data.add(item("testDB", &table, id), table);
        }
        assert_eq!(is.data.table_cache.status().evictions, 1);
        for id in 1..=4 {
            let before = is.data.table_cache.status();
            if by_name {
                is.table_by_name("testDB", &format!("t{id}"), Some(true))
                    .unwrap();
            } else {
                is.table_by_id(id, Some(true)).unwrap();
            }
            let after = is.data.table_cache.status();
            assert_eq!(after.misses, before.misses + 1);
            assert_eq!(after.evictions, before.evictions + 1);
        }
        for id in 2..=4 {
            let before = is.data.table_cache.status();
            if by_name {
                is.table_by_name("testDB", &format!("t{id}"), Some(true))
                    .unwrap();
            } else {
                is.table_by_id(id, Some(true)).unwrap();
            }
            assert_eq!(is.data.table_cache.status().hits, before.hits + 1);
        }
        is.data.set_cache_capacity(0);
        assert_eq!(is.data.table_cache.len(), 0);
        assert_eq!(is.data.table_cache.size(), 0);
    }
}
fn fk(name: &str, schema: &str, table: &str) -> FKInfo {
    FKInfo {
        name: CiString::new(name),
        ref_schema: CiString::new(schema),
        ref_table: CiString::new(table),
        version: 1,
        ..Default::default()
    }
}
/// Go `TestReferredForeignKeys`.
#[test]
fn test_referred_foreign_keys() {
    let data = Data::new();
    let t1 = TableInfo {
        name: CiString::new("table1"),
        foreign_keys: vec![fk("fk1", "db1", "table1"), fk("fk2", "db1", "table1")].into(),
        ..Default::default()
    };
    data.add_referred_foreign_keys(&CiString::new("db1"), &t1, 1);
    let expected1 = data.get_table_referred_foreign_keys("db1", "table1", 1);
    assert_eq!(expected1.len(), 2);
    assert_eq!(expected1[0].child_fk_name.original(), "fk1");
    assert_eq!(expected1[1].child_fk_name.original(), "fk2");
    assert!(data
        .get_table_referred_foreign_keys("db1", "table2", 1)
        .is_empty());
    let ta = TableInfo {
        name: CiString::new("tableA"),
        foreign_keys: vec![fk("fkA", "db2", "tableA")].into(),
        ..Default::default()
    };
    data.add_referred_foreign_keys(&CiString::new("db2"), &ta, 2);
    let expected2 = data.get_table_referred_foreign_keys("db2", "tablea", 2);
    assert_eq!(expected2.len(), 1);
    assert_eq!(expected2[0].child_fk_name.original(), "fkA");
    assert!(data
        .get_table_referred_foreign_keys("db1", "tablea", 2)
        .is_empty());
    assert!(data
        .get_table_referred_foreign_keys("db2", "table1", 2)
        .is_empty());
    data.delete_referred_foreign_keys(&CiString::new("db1"), &t1, 3);
    assert!(data
        .get_table_referred_foreign_keys("db1", "table1", 3)
        .is_empty());
    assert_eq!(
        data.get_table_referred_foreign_keys("db2", "tablea", 3),
        expected2
    );
    data.delete_referred_foreign_keys(&CiString::new("db2"), &ta, 4);
    assert!(data
        .get_table_referred_foreign_keys("db2", "tablea", 4)
        .is_empty());
    assert_eq!(
        data.get_table_referred_foreign_keys("db1", "table1", 2),
        expected1
    );
    assert_eq!(
        data.get_table_referred_foreign_keys("db2", "tablea", 2),
        expected2
    );
}
/// Go `TestGCOldFKVersion`.
#[test]
fn test_gc_old_fk_version() {
    let data = Data::new();
    let reference = ReferredFKInfo {
        child_schema: CiString::new("s"),
        child_table: CiString::new("t"),
        child_fk_name: CiString::new("fk"),
        ..Default::default()
    };
    {
        let mut idx = data.indexes.write().unwrap();
        for ver in 1..=5 {
            idx.referred_foreign_keys.set(
                ("db1".into(), "table1".into()),
                ver,
                (ver % 2 != 0).then(|| vec![reference.clone()]),
            );
        }
        idx.referred_foreign_keys.set(
            ("db2".into(), "table2".into()),
            1,
            Some(vec![reference.clone()]),
        );
    }
    assert_eq!(data.gc_old_fk_version(4), 2);
    let vers: Vec<_> = data
        .indexes
        .read()
        .unwrap()
        .referred_foreign_keys
        .0
        .keys()
        .map(|(_, v)| *v)
        .collect();
    assert_eq!(vers, [3, 4, 5, 1]);
    assert_eq!(
        data.get_table_referred_foreign_keys("db1", "table1", 3),
        [reference.clone()]
    );
    assert!(data
        .get_table_referred_foreign_keys("db1", "table1", 4)
        .is_empty());
    assert_eq!(
        data.get_table_referred_foreign_keys("db1", "table1", 5),
        [reference]
    );
}
/// Go `keepAlive` / `GetAndResetRecentInfoSchemaTS` contract from cache.go.
#[test]
fn keep_alive_lowers_shared_recent_min_ts() {
    let (is, store) = fixture();
    let table = table(2, "test");
    store.tables.lock().unwrap().insert((1, 2), table.clone());
    is.data.add(item("testDB", &table, 2), table);
    assert_eq!(is.data.recent_min_ts.swap(100, Ordering::AcqRel), 0);
    is.clone_and_update_ts(60)
        .table_by_name("testDB", "test", None)
        .unwrap();
    is.table_by_id(2, None).unwrap();
    assert_eq!(is.data.recent_min_ts.swap(200, Ordering::AcqRel), 42);
    is.clone_and_update_ts(70)
        .get_table_referred_foreign_keys("testDB", "test");
    assert_eq!(is.data.recent_min_ts.load(Ordering::Acquire), 70);
    // Go builder probe: zzprobe/i04 TestKeepAlive (recorded in the ExecPlan).
    is.data.recent_min_ts.store(100, Ordering::Release);
    assert!(is.table_by_name("missing", "missing", None).is_err());
    assert_eq!(is.data.recent_min_ts.swap(100, Ordering::AcqRel), 42);
    assert!(is.table_by_id(-1, None).is_none());
    assert_eq!(is.data.recent_min_ts.swap(100, Ordering::AcqRel), 100);
    assert!(is.table_by_id(1234567, None).is_none());
    assert_eq!(is.data.recent_min_ts.swap(100, Ordering::AcqRel), 42);
}

/// Go `TestDataStructFieldsCorrectnessInSchemaChange`.
#[test]
fn test_data_struct_fields_correctness_in_schema_change() {
    let (mut is, store) = fixture();
    let mut t = table(10, "testTable");
    is.data.add(item("testDB", &t, 2), t.clone());
    assert_eq!(
        is.data.indexes.read().unwrap().schema_id2name.get(&1, 1),
        Some("testdb".into())
    );
    assert_eq!(
        is.data
            .indexes
            .read()
            .unwrap()
            .by_name
            .get(&("testdb".into(), "testtable".into()), 2)
            .unwrap()
            .table_id,
        10
    );
    assert_eq!(is.table_item_by_id(10).unwrap().db_id, 1);
    assert!(is.table_is_cached(10));
    let mut meta = t.info.read().clone();
    meta.partition = Some(GoShared::new(tidb_model::partition::PartitionInfo {
        enable: true,
        definitions: vec![
            tidb_model::partition::PartitionDefinition {
                id: 1,
                name: CiString::new("p1"),
                ..Default::default()
            },
            tidb_model::partition::PartitionDefinition {
                id: 2,
                name: CiString::new("p2"),
                ..Default::default()
            },
        ]
        .into(),
        ..Default::default()
    }));
    t.info = GoShared::new(meta.clone());
    is.data.remove(&item("testDB", &t, 2), 3);
    is.data.add(item("testDB", &t, 3), t.clone());
    is.schema_meta_version = 3;
    assert_eq!(is.table_id_by_partition_id(2), Some(10));
    assert_eq!(is.data.indexes.read().unwrap().pid2tid.0.len(), 2);
    let mut pi = meta.partition.as_ref().unwrap().read().clone();
    pi.definitions = vec![pi.definitions.map_visible(Clone::clone)[0].clone()].into();
    meta.partition = Some(GoShared::new(pi));
    is.data.remove(&item("testDB", &t, 3), 4);
    t.info = GoShared::new(meta);
    is.data.add(item("testDB", &t, 4), t.clone());
    is.schema_meta_version = 4;
    assert_eq!(is.table_id_by_partition_id(1), Some(10));
    assert_eq!(is.table_id_by_partition_id(2), None);
    assert_eq!(is.data.indexes.read().unwrap().pid2tid.0.len(), 4);
    store.tables.lock().unwrap().insert((1, 10), t.clone());
    is.data.remove(&item("testDB", &t, 4), 5);
    is.data.add(item("testDB", &t, 5), t.clone()); // DROP intermediate state still has meta.
    is.schema_meta_version = 5;
    assert!(is.table_is_cached(10));
    assert_eq!(is.table_id_by_partition_id(1), Some(10));
    is.data.remove(&item("testDB", &t, 5), 5); // final metadata removal.
    assert!(!is.table_is_cached(10));
    assert!(is.table_item_by_id(10).is_none());
    assert_eq!(is.table_id_by_partition_id(1), None);
    is.schema_meta_version = 4;
    assert_eq!(is.table_id_by_partition_id(1), Some(10));
    assert!(is.table_item_by_id(10).is_some());
    is.data.delete_db(&database(1, "testDB"), 6);
    is.schema_meta_version = 6;
    assert!(is.schema_by_id(1).is_none());
    assert!(is.schema_by_name("testDB").is_none());
}

/// Go `TestSpecialAttributeCorrectnessInSchemaChange`: resident attributes
/// survive cache eviction, and remove/add at one version replaces the tomb.
#[test]
fn test_special_attribute_correctness_in_schema_change() {
    let (mut is, _) = fixture();
    let mut t = table(10, "testTable");
    is.data.add(item("testDB", &t, 2), t.clone());
    assert!(is
        .list_tables_with_special_attribute(has_special_attributes)
        .is_empty());
    for version in [3, 4] {
        is.data.remove(&item("testDB", &t, version - 1), version);
        let mut meta = t.info.read().clone();
        meta.partition = Some(GoShared::new(tidb_model::PartitionInfo {
            expr: "aa+1".into(),
            columns: vec![CiString::new("aa")].into(),
            enable: true,
            definitions: (1..=if version == 3 { 2 } else { 1 })
                .map(|id| tidb_model::PartitionDefinition {
                    id,
                    name: CiString::new(format!("p{id}")),
                    ..Default::default()
                })
                .collect::<Vec<_>>()
                .into(),
            ..Default::default()
        }));
        t.info = GoShared::new(meta);
        is.data.add(item("testDB", &t, version), t.clone());
        is.schema_meta_version = version;
        is.evict_table("testDB", "testTable");
        let resident = is.list_tables_with_special_attribute(|t| t.partition.is_some());
        assert_eq!(
            resident[0].1[0]
                .read()
                .partition
                .as_ref()
                .unwrap()
                .read()
                .definitions
                .len(),
            if version == 3 { 2 } else { 1 }
        );
        assert_eq!(
            is.table_id_by_partition_id(2),
            if version == 3 { Some(10) } else { None }
        );
    }
    for (version, add) in [(5, true), (6, false)] {
        is.data.remove(&item("testDB", &t, version - 1), version);
        let mut meta = t.info.read().clone();
        meta.placement_policy_ref = add.then(|| {
            GoShared::new(tidb_model::PolicyRefInfo {
                id: 1,
                name: CiString::new("p3"),
            })
        });
        t.info = GoShared::new(meta);
        is.data.add(item("testDB", &t, version), t.clone());
        is.schema_meta_version = version;
        is.evict_table("testDB", "testTable");
        let found = is.list_tables_with_special_attribute(|t| t.placement_policy_ref.is_some());
        assert_eq!(found.len(), usize::from(add));
    }
    for (version, add) in [(7, true), (8, false)] {
        is.data.remove(&item("testDB", &t, version - 1), version);
        let mut meta = t.info.read().clone();
        meta.tiflash_replica = add.then(|| {
            GoShared::new(tidb_model::table::TiFlashReplicaInfo {
                count: 1,
                available: true,
                ..Default::default()
            })
        });
        t.info = GoShared::new(meta);
        is.data.add(item("testDB", &t, version), t.clone());
        is.schema_meta_version = version;
        assert_eq!(
            is.list_tables_with_special_attribute(|t| t.tiflash_replica.is_some())
                .len(),
            usize::from(add)
        );
    }
    for (version, add) in [(9, true), (10, false)] {
        is.data.remove(&item("testDB", &t, version - 1), version);
        let mut meta = t.info.read().clone();
        meta.lock = add.then(|| {
            GoShared::new(tidb_model::table::TableLockInfo {
                ts: 1,
                ..Default::default()
            })
        });
        t.info = GoShared::new(meta);
        is.data.add(item("testDB", &t, version), t.clone());
        is.schema_meta_version = version;
        is.evict_table("testDB", "testTable");
        assert_eq!(
            is.list_tables_with_special_attribute(|t| t.lock.is_some())
                .len(),
            usize::from(add)
        );
    }
}

/// Go resetBeforeFullLoad: a full load tombstones every previously known
/// identity, retaining historical lookups and replacing same-version tombs.
#[test]
fn reset_before_full_load_preserves_history() {
    let (mut is, _) = fixture();
    let t = table(2, "old");
    is.data.add(item("testDB", &t, 2), t);
    is.data.reset_before_full_load(10);
    is.data.add_db(10, database(1, "testDB"));
    let new = table(3, "new");
    is.data.add(item("testDB", &new, 10), new);
    assert!(is.table_item_by_id(2).is_some());
    is.schema_meta_version = 10;
    assert!(is.table_item_by_id(2).is_none());
    assert!(is.table_item_by_id(3).is_some());
    assert_eq!(is.schema_simple_table_infos("testDB").len(), 1);
}

fn build_catalog(
    data: Arc<Data>,
    store: Arc<Store>,
    version: i64,
    tables: Vec<GoShared<TableInfo>>,
    policies: Vec<tidb_model::PolicyInfo>,
) -> crate::SessionExtendedInfoSchema {
    let mut db = database(1, "testDB");
    db.deprecated_tables =
        tidb_model::GoSharedPointerSlice::from_handles(tables.into_iter().map(Some).collect());
    let mut schema = crate::SessionExtendedInfoSchema::init_with_db_infos(
        vec![GoShared::new(db)],
        policies,
        vec![],
        version,
    );
    schema.set_schema_ts(42);
    schema.init_with_db_infos_v2(data, store, 1024 * 1024);
    schema
}

/// Go `TestMisc`: policy/resource group create, alter and drop in the V2 image.
#[test]
fn test_misc() {
    let mut schema = build_catalog(
        Arc::new(Data::new()),
        Arc::new(Store::default()),
        1,
        vec![],
        vec![],
    );
    assert!(schema.is_v2());
    assert_eq!(schema.resource_groups().count(), 0);
    for id in 1..=2 {
        schema.set_resource_group(tidb_model::ResourceGroupInfo {
            id,
            name: CiString::new(if id == 1 { "test" } else { "test2" }),
            ..Default::default()
        });
        assert_eq!(schema.resource_groups().count(), id as usize);
    }
    let mut group = schema.resource_group("test").unwrap().clone();
    group.state = SchemaState::PUBLIC;
    schema.set_resource_group(group);
    assert_eq!(schema.resource_groups().count(), 2);
    assert_eq!(
        schema.resource_group("TEST").unwrap().state,
        SchemaState::PUBLIC
    );
    schema.delete_resource_group_by_id(1);
    assert!(schema.resource_group("test").is_none());
    assert_eq!(schema.resource_group("test2").unwrap().id, 2);
    assert_eq!(schema.resource_groups().count(), 1);
    for id in 1..=2 {
        schema.set_policy(tidb_model::PolicyInfo {
            id,
            name: CiString::new(if id == 1 { "test" } else { "test2" }),
            ..Default::default()
        });
        assert_eq!(schema.policies().count(), id as usize);
    }
    let mut policy = schema.policy("test").unwrap().clone();
    policy.state = SchemaState::PUBLIC;
    schema.set_policy(policy);
    assert_eq!(schema.policy("TEST").unwrap().state, SchemaState::PUBLIC);
    assert_eq!(schema.policies().count(), 2);
    schema.delete_policy_by_id(1);
    assert!(schema.policy("test").is_none());
    assert_eq!(schema.policy("test2").unwrap().id, 2);
    assert_eq!(schema.policies().count(), 1);
}

/// Go `TestBundles`: table and partition rules track policy alterations.
#[test]
fn test_bundles() {
    let data = Arc::new(Data::new());
    let store = Arc::new(Store::default());
    let mut info = table(10, "test").info.read().clone();
    info.partition = Some(GoShared::new(tidb_model::PartitionInfo {
        enable: true,
        definitions: vec![
            tidb_model::PartitionDefinition {
                id: 1,
                ..Default::default()
            },
            tidb_model::PartitionDefinition {
                id: 2,
                ..Default::default()
            },
        ]
        .into(),
        ..Default::default()
    }));
    let policy = |region: &str| tidb_model::PolicyInfo {
        id: 1,
        name: CiString::new("test"),
        state: SchemaState::PUBLIC,
        placement_settings: Some(GoShared::new(tidb_model::PlacementSettings {
            primary_region: region.into(),
            regions: "r1,r2".into(),
            ..Default::default()
        })),
    };
    let mut schema = build_catalog(
        data.clone(),
        store.clone(),
        3,
        vec![GoShared::new(info.clone())],
        vec![policy("r1")],
    );
    assert_eq!(schema.all_placement_bundles().count(), 0);
    let reference = GoShared::new(tidb_model::PolicyRefInfo {
        id: 1,
        name: CiString::new("test"),
    });
    info.placement_policy_ref = Some(reference.clone());
    schema = build_catalog(
        data.clone(),
        store.clone(),
        4,
        vec![GoShared::new(info.clone())],
        vec![policy("r1")],
    );
    assert_eq!(
        schema
            .placement_bundle_by_physical_table_id(10)
            .unwrap()
            .rules[0]
            .label_constraints[0]
            .values[0],
        "r1"
    );
    schema.set_policy(policy("r2"));
    assert_eq!(
        schema
            .placement_bundle_by_physical_table_id(10)
            .unwrap()
            .rules[0]
            .label_constraints[0]
            .values[0],
        "r2"
    );
    let mut partition = info.partition.as_ref().unwrap().read().clone();
    let mut definitions = partition.definitions.snapshot();
    definitions[0].placement_policy_ref = Some(reference);
    partition.definitions = definitions.into();
    info.partition = Some(GoShared::new(partition));
    schema = build_catalog(
        data,
        store.clone(),
        6,
        vec![GoShared::new(info)],
        vec![policy("r2")],
    );
    assert_eq!(
        schema
            .placement_bundle_by_physical_table_id(1)
            .unwrap()
            .rules[0]
            .label_constraints[0]
            .values[0],
        "r2"
    );
    schema.set_policy(policy("r1"));
    assert_eq!(
        schema
            .placement_bundle_by_physical_table_id(1)
            .unwrap()
            .rules[0]
            .label_constraints[0]
            .values[0],
        "r1"
    );
    assert!(store.reads.lock().unwrap().is_empty());
}

/// Go `TestReferredFKInfo`: each DDL version retains its own parent references.
#[test]
fn test_referred_fk_info() {
    let (mut is, _) = fixture();
    let mut info = table(10, "testTable").info.read().clone();
    info.foreign_keys = vec![fk("fk_1", "t1", "parent")].into();
    let add = |is: &InfoSchemaV2, info: &TableInfo, version| {
        is.data.add_table_info(
            TableItem {
                db_name: CiString::new("testDB"),
                db_id: 1,
                table_name: info.name.clone(),
                table_id: info.id,
                schema_version: version,
            },
            GoShared::new(info.clone()),
        );
    };
    add(&is, &info, 2);
    assert_eq!(
        is.data
            .indexes
            .read()
            .unwrap()
            .referred_foreign_keys
            .0
            .len(),
        1
    );
    assert_eq!(
        is.get_table_referred_foreign_keys("t1", "parent")[0]
            .child_fk_name
            .original(),
        "fk_1"
    );
    is.data
        .delete_referred_foreign_keys(&CiString::new("testDB"), &info, 3);
    info.foreign_keys = vec![fk("fk_1", "t1", "parent"), fk("fk_2", "t1", "parent")].into();
    add(&is, &info, 3);
    is.schema_meta_version = 3;
    assert_eq!(
        is.data
            .indexes
            .read()
            .unwrap()
            .referred_foreign_keys
            .0
            .len(),
        2
    );
    assert_eq!(
        is.get_table_referred_foreign_keys("t1", "parent")[1]
            .child_fk_name
            .original(),
        "fk_2"
    );
    is.data
        .delete_referred_foreign_keys(&CiString::new("testDB"), &info, 4);
    info.foreign_keys = vec![fk("fk_1", "t1", "parent")].into();
    add(&is, &info, 4);
    is.schema_meta_version = 4;
    assert_eq!(
        is.data
            .indexes
            .read()
            .unwrap()
            .referred_foreign_keys
            .0
            .len(),
        3
    );
    assert_eq!(is.get_table_referred_foreign_keys("t1", "parent").len(), 1);
    is.data
        .delete_referred_foreign_keys(&CiString::new("testDB"), &info, 5);
    is.data.remove(&is.table_item_by_id(10).unwrap(), 5);
    is.schema_meta_version = 5;
    assert_eq!(
        is.data
            .indexes
            .read()
            .unwrap()
            .referred_foreign_keys
            .0
            .len(),
        4
    );
    assert!(is
        .get_table_referred_foreign_keys("t1", "parent")
        .is_empty());
    is.schema_meta_version = 3;
    assert_eq!(is.get_table_referred_foreign_keys("t1", "parent").len(), 2);
}

/// Go `Data.GCOldVersion`: by-name and by-ID tomb records are removed together.
#[test]
fn gc_old_version_removes_matching_tombstones() {
    let (mut is, _) = fixture();
    let table = table(10, "test");
    is.data.add(item("testDB", &table, 2), table.clone());
    is.data.remove(&item("testDB", &table, 2), 3);
    is.data.add(item("testDB", &table, 4), table.clone());
    is.data.remove(&item("testDB", &table, 4), 5);
    assert_eq!(is.data.gc_old_version(5), (2, 2));
    let idx = is.data.indexes.read().unwrap();
    assert_eq!(idx.by_name.0.len(), 2);
    assert_eq!(idx.by_id.0.len(), 2);
    drop(idx);
    is.schema_meta_version = 4;
    assert!(is.table_item_by_id(10).is_some());
    is.schema_meta_version = 5;
    assert!(is.table_item_by_id(10).is_none());
}

/// Go `TestV2Basic` / `TestInfoschemaCache`, exercised through the catalog and
/// planner interfaces used by SQL, with ordinary full-load metadata absent.
#[test]
fn v2_catalog_reader_pins_only_accessed_tables_and_preserves_load_errors() {
    use crate::table_source::TableSource;
    let data = Arc::new(Data::new());
    let store = Arc::new(Store::default());
    let mut db = database(1, "testDB");
    db.table_name2id = Some(GoShared::new(BTreeMap::from([
        ("test".to_owned(), 10),
        ("untouched".to_owned(), 11),
    ])));
    store
        .tables
        .lock()
        .unwrap()
        .insert((1, 10), table(10, "test"));
    store
        .tables
        .lock()
        .unwrap()
        .insert((1, 11), table(11, "untouched"));
    let mut schema = crate::SessionExtendedInfoSchema::init_with_db_infos(
        vec![GoShared::new(db)],
        vec![],
        vec![],
        2,
    );
    schema.set_schema_ts(42);
    schema.init_with_db_infos_v2(data.clone(), store.clone(), 1024);
    assert!(schema.stored_schemas()[0]
        .read()
        .deprecated_tables
        .is_empty());
    assert!(data.table_cache.is_empty());
    assert_eq!(
        schema.table_names("testDB").unwrap(),
        vec!["test", "untouched"]
    );
    assert!(store.reads.lock().unwrap().is_empty());
    data.recent_min_ts.store(100, Ordering::Release);
    assert!(schema
        .try_table_info_by_name("missing", "missing")
        .unwrap()
        .is_none());
    assert_eq!(data.recent_min_ts.swap(100, Ordering::AcqRel), 42);
    assert!(schema.table_info_by_name("missing", "missing").is_none());
    assert_eq!(data.recent_min_ts.swap(100, Ordering::AcqRel), 42);
    assert!(schema.try_get_in("testDB", "missing").unwrap().is_none());
    assert_eq!(data.recent_min_ts.load(Ordering::Acquire), 42);
    data.recent_min_ts.store(100, Ordering::Release);
    assert!(schema.try_get_in("testDB", "test").unwrap().is_some());
    assert_eq!(data.recent_min_ts.load(Ordering::Acquire), 42);
    data.table_cache.purge();
    assert!(schema.try_get_in("testDB", "test").unwrap().is_some());
    assert_eq!(store.reads.lock().unwrap().len(), 1); // retained by this reader
    let next = schema.clone();
    assert!(next.try_get_in("testDB", "test").unwrap().is_some());
    assert_eq!(
        store.reads.lock().unwrap().as_slice(),
        &[(1, 10, 42), (1, 10, 42)]
    );
    assert_eq!(next.statistics_schema(10).unwrap().info.read().id, 10);
    assert!(next.latest_index_schema().indexes_for_table(10).is_some());
    let planner = next.planner_catalog("testDB", None);
    assert_eq!(
        planner
            .try_find_table("testDB", "test")
            .unwrap()
            .unwrap()
            .table_id(),
        10
    );
    assert!(!data.table_cache.contains(&TableCacheKey {
        table_id: 11,
        schema_version: 2
    }));
    store.tables.lock().unwrap().remove(&(1, 10));
    data.table_cache.purge();
    let failed = schema.clone();
    assert!(failed.try_get_in("testDB", "test").is_err());
    assert!(failed.try_table_info_by_name("testDB", "test").is_err());
    assert!(failed
        .planner_catalog("testDB", None)
        .try_find_table("testDB", "test")
        .is_err());
}

/// Go `Builder.ApplyDiff`: a batch's empty versions cannot retag an unchanged
/// table, and intermediate table identities remain readable at their versions.
#[test]
fn v2_diff_images_keep_each_table_change_version() {
    let data = Arc::new(Data::new());
    let store = Arc::new(Store::default());
    let old = table(10, "old");
    let untouched = table(11, "untouched");
    store.tables.lock().unwrap().insert((1, 10), old.clone());
    store
        .tables
        .lock()
        .unwrap()
        .insert((1, 11), untouched.clone());
    let original = build_catalog(
        data.clone(),
        store.clone(),
        2,
        vec![old.info.clone(), untouched.info.clone()],
        vec![],
    );
    let mut staged = original
        .metadata_copy_for_diff(Some((&[10].into_iter().collect(), &Default::default())))
        .unwrap();
    assert_eq!(store.reads.lock().unwrap().as_slice(), &[(1, 10, 42)]);
    let mut renamed = old.info.read().clone();
    renamed.name = CiString::new("renamed");
    staged.edit_schema_by_id(1, |db| {
        db.deprecated_tables = vec![renamed].into();
    });
    staged.record_v2_diff_image(3);
    staged.set_schema_meta_version(5); // versions 4 and 5 did not change this table
    staged.init_with_db_infos_v2(data.clone(), store.clone(), 1024);
    let reader = staged.v2().unwrap();
    assert_eq!(
        reader
            .table_item_by_name("testDB", "renamed")
            .unwrap()
            .schema_version,
        3
    );
    assert_eq!(reader.table_item_by_id(11).unwrap().schema_version, 2);
    assert!(reader.table_item_by_name("testDB", "old").is_none());
    let mut historical = reader.clone();
    historical.schema_meta_version = 2;
    assert!(historical.table_item_by_name("testDB", "old").is_some());
    assert!(historical.table_item_by_name("testDB", "renamed").is_none());
    historical.schema_meta_version = 3;
    assert!(historical.table_item_by_name("testDB", "renamed").is_some());
}

/// Go `internal.Sizeof`: unused slice capacity and shared pointee identity
/// affect entry charges; serialized field names do not describe allocations.
#[test]
fn cache_size_charges_capacity_and_deduplicates_shared_metadata() {
    let t = table(10, "t");
    let mut info = t.info.read().clone();
    let base = table_cache_entry_size(&info, &t.table);
    info.comment.reserve(4096);
    assert!(table_cache_entry_size(&info, &t.table) >= base + 4096);
    let column = info.columns.get(0).unwrap();
    info.columns = tidb_model::GoSharedPointerSlice::from_handles_with_capacity(
        vec![Some(column.clone())],
        100,
    );
    let one = table_cache_entry_size(&info, &t.table);
    info.columns = tidb_model::GoSharedPointerSlice::from_handles_with_capacity(
        vec![Some(column.clone()), Some(column)],
        100,
    );
    assert_eq!(table_cache_entry_size(&info, &t.table), one);
}

/// Go TableByName also resolves sequence objects after a cache miss; the
/// statement's SequenceOperatorProvider pins the same metadata reader.
#[test]
fn v2_sequence_snapshot_loads_sequences_lazily() {
    use tidb_util::misc::SequenceSchema;
    let data = Arc::new(Data::new());
    let store = Arc::new(Store::default());
    let mut info = TableInfo {
        id: 10,
        db_id: 1,
        name: CiString::new("seq"),
        state: SchemaState::PUBLIC,
        ..Default::default()
    };
    let spec = tidb_model::table::SequenceInfo {
        start: 1,
        increment: 1,
        min_value: 1,
        max_value: 100,
        cache_value: 10,
        ..Default::default()
    };
    info.sequence = Some(GoShared::new(spec.clone()));
    store.tables.lock().unwrap().insert(
        (1, 10),
        CachedTable {
            info: GoShared::new(info.clone()),
            size: 100,
            table: Arc::new(TableEntry::Sequence(tidb_table::sequence::SequenceDef {
                id: 10,
                name: "seq".into(),
                comment: String::new(),
                update_ts: 0,
                allocator: tidb_table::sequence::SequenceAllocator::new(spec),
            })),
        },
    );
    let schema = build_catalog(data, store.clone(), 2, vec![GoShared::new(info)], vec![]);
    let snapshot = crate::sequence::SequenceSnapshot::new(&schema, "testDB");
    assert!(store.reads.lock().unwrap().is_empty());
    assert_eq!(
        snapshot
            .get_sequence_by_name("testDB", "seq")
            .unwrap()
            .get_sequence_id(),
        10
    );
    assert_eq!(store.reads.lock().unwrap().len(), 1);
    assert_eq!(
        snapshot
            .get_sequence_by_name("testDB", "seq")
            .unwrap()
            .get_sequence_id(),
        10
    );
    assert_eq!(store.reads.lock().unwrap().len(), 1);
}

/// Go Builder's globalTemporaryTableIDs and resident lock index survive
/// table eviction without eagerly materializing unrelated table objects.
#[test]
fn v2_temporary_and_lock_enumeration_remains_lazy() {
    let data = Arc::new(Data::new());
    let store = Arc::new(Store::default());
    let mut temporary = table(10, "temp").info.read().clone();
    temporary.temp_table_type = tidb_model::TempTableType::GLOBAL;
    let plain = table(11, "plain").info;
    let schema = build_catalog(
        data.clone(),
        store.clone(),
        2,
        vec![GoShared::new(temporary), plain],
        vec![],
    );
    data.table_cache.purge();
    assert_eq!(
        schema.global_temporary_table_ids(),
        vec![("testdb".into(), "temp".into())]
    );
    assert_eq!(schema.locked_tables().count(), 0);
    assert!(store.reads.lock().unwrap().is_empty());
    let item = schema.v2().unwrap().table_item_by_id(10).unwrap();
    data.remove(&item, 3);
    let mut newer = schema.v2().unwrap().clone();
    newer.schema_meta_version = 3;
    assert!(newer.global_temporary_table_names().is_empty());
    assert_eq!(schema.global_temporary_table_ids().len(), 1);
    // Go base-map history is released with old schemas. The shared native
    // identity index retains its cutoff pivot and removes older versions.
    data.gc_old_version(4);
    assert!(newer.global_temporary_table_names().is_empty());
    assert!(data
        .indexes
        .read()
        .unwrap()
        .global_temporary_tables
        .0
        .keys()
        .all(|(_, version)| *version >= 3));
}

/// Go `loadTableInfo`: flashback errors retry at the same snapshot, ordinary
/// failures propagate, and failed loads never poison subsequent lookups.
#[test]
fn v2_load_table_info_retries_flashback_and_retains_errors() {
    struct Retrying {
        calls: std::sync::atomic::AtomicUsize,
    }
    impl TableLoader for Retrying {
        fn load_table(&self, _: i64, _: i64, ts: u64) -> Result<CachedTable, ClusterCatalogError> {
            assert_eq!(ts, 42);
            match self.calls.fetch_add(1, Ordering::Relaxed) {
                0 => Err(ClusterCatalogError::Snapshot(
                    "in flashback progress".into(),
                )),
                1 => Err(ClusterCatalogError::Snapshot("read failed".into())),
                _ => Ok(table(10, "test")),
            }
        }
        fn list_tables(
            &self,
            _: i64,
            _: u64,
        ) -> Result<Vec<GoShared<TableInfo>>, ClusterCatalogError> {
            unreachable!()
        }
    }
    let data = Arc::new(Data::new());
    data.add_db(1, database(1, "testDB"));
    data.add_table_item(item("testDB", &table(10, "test"), 2));
    let loader = Arc::new(Retrying {
        calls: Default::default(),
    });
    let schema = InfoSchemaV2::new(data, loader.clone(), 2, 42);
    assert!(schema
        .table_by_name("testDB", "test", None)
        .unwrap_err()
        .to_string()
        .contains("read failed"));
    assert_eq!(
        schema
            .table_by_name("testDB", "test", None)
            .unwrap()
            .info
            .read()
            .id,
        10
    );
    assert_eq!(loader.calls.load(Ordering::Relaxed), 3);
}

/// Go `internal.Sizeof` visits dynamic default-value maps recursively and
/// stops at a previously visited pointer, including a map referring to itself.
#[test]
fn cache_size_counts_dynamic_defaults_and_handles_cycles() {
    use tidb_model::{ColumnDefaultValue, GoAny, GoAnyMap};
    let t = table(10, "t");
    let info = t.info.read().clone();
    let before = table_cache_entry_size(&info, &t.table);
    let mut map = GoAnyMap::allocated([(
        "payload",
        GoAny::new(ColumnDefaultValue::Str("x".repeat(4096).into())),
    )]);
    map.insert("self", GoAny::new(ColumnDefaultValue::Map(map.clone())));
    info.columns.get(0).unwrap().write().default_value = GoAny::new(ColumnDefaultValue::Map(map));
    assert!(table_cache_entry_size(&info, &t.table) >= before + 4096);
}

/// Go `infoschemaV2.loadTableInfo` and SIEVE `Get` failpoint sites: forced
/// cache misses use the loader, TableByName returns its error, and TableByID
/// returns false. Disabling the failpoints restores the cached table.
#[cfg(feature = "failpoints")]
#[test]
fn load_table_info_failpoints_preserve_name_error_and_id_absence() {
    let (is, store) = fixture();
    let table = table(2, "test");
    store.tables.lock().unwrap().insert((1, 2), table.clone());
    is.data.add(item("testDB", &table, 2), table);
    assert!(is.table_by_name("testDB", "test", None).is_ok());
    let skip = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/infoschema/skipGet",
        "return()",
    );
    let error = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/infoschema/mockLoadTableInfoError",
        "return(true)",
    );
    assert!(matches!(
        is.table_by_name("testDB", "test", None).unwrap_err(),
        ClusterCatalogError::Snapshot(message) if message == "mockLoadTableInfoError"
    ));
    assert!(is.table_by_id(2, None).is_none());
    assert!(store.reads.lock().unwrap().is_empty());
    drop(error);
    assert!(is.table_by_name("testDB", "test", None).is_ok());
    assert_eq!(store.reads.lock().unwrap().len(), 1);
    drop(skip);
    assert!(is.table_by_name("testDB", "test", None).is_ok());
    assert_eq!(store.reads.lock().unwrap().len(), 1);
}

/// Go `TestTableFromMeta` and `tryLockMDLAndUpdateSchemaIfNecessary`.
/// The throwaway `zzprobe/i04.TestTableState` confirms all non-NONE states
/// construct a table; only PUBLIC is visible to SQL planning.
#[test]
fn v2_loads_intermediate_table_states_but_plans_only_public_tables() {
    use crate::table_source::TableSource;
    let base = table(10, "test").info.read().clone();
    let ids = tidb_table::table_from_meta::LocalTableAutoIds::default();
    let auto = tidb_table::table_from_meta::AutoIdSource::In {
        db_id: 1,
        ids: &ids,
    };
    for state in [
        SchemaState::NONE,
        SchemaState::DELETE_ONLY,
        SchemaState::WRITE_ONLY,
        SchemaState::WRITE_REORGANIZATION,
        SchemaState::DELETE_REORGANIZATION,
        SchemaState::PUBLIC,
    ] {
        let mut info = base.clone();
        info.state = state;
        let built = tidb_table::table_from_meta::table_from_meta(&info, &auto);
        if state == SchemaState::NONE {
            assert_eq!(
                built.unwrap_err(),
                "[table:8042]table test can't be in none state"
            );
            continue;
        }
        let built = built.unwrap();
        let info = built.meta();
        let entry = Arc::new(TableEntry::Kv(Arc::new(built)));
        let size = table_cache_entry_size(&info.read(), &entry);
        let store = Arc::new(Store::default());
        store.tables.lock().unwrap().insert(
            (1, 10),
            CachedTable {
                info,
                table: entry,
                size,
            },
        );
        let mut db = database(1, "testDB");
        db.table_name2id = Some(GoShared::new(BTreeMap::from([("test".to_owned(), 10)])));
        let mut schema = crate::SessionExtendedInfoSchema::init_with_db_infos(
            vec![GoShared::new(db)],
            vec![],
            vec![],
            2,
        );
        schema.set_schema_ts(42);
        schema.init_with_db_infos_v2(Arc::new(Data::new()), store, 1 << 20);
        assert_eq!(
            schema
                .table_info_by_name("testDB", "test")
                .unwrap()
                .1
                .read()
                .state,
            state
        );
        assert_eq!(
            schema
                .planner_catalog("testDB", None)
                .try_find_table("testDB", "test")
                .unwrap()
                .is_some(),
            state == SchemaState::PUBLIC
        );
    }
}

/// A V2 image builds its planner image (memory schemas, local temporary
/// tables) once and shares it across statements, as Go plans every statement
/// against the one immutable `InfoSchema` of a version; only the lazily
/// converted stored tables are per statement. Rebuilding the image per
/// statement made every internal statement slow enough to miss Go's 100 ms
/// stats sync-load wait.
#[test]
fn v2_statements_share_the_planner_image_and_resolve_stored_tables_lazily() {
    use crate::table_source::TableSource;
    let store = Arc::new(Store::default());
    store
        .tables
        .lock()
        .unwrap()
        .insert((1, 10), table(10, "test"));
    let mut db = database(1, "testDB");
    db.table_name2id = Some(GoShared::new(BTreeMap::from([("test".to_owned(), 10)])));
    let mut schema = crate::SessionExtendedInfoSchema::init_with_db_infos(
        vec![GoShared::new(db)],
        vec![],
        vec![],
        2,
    );
    schema.set_schema_ts(42);
    schema.init_with_db_infos_v2(Arc::new(Data::new()), store, 1 << 20);
    let first = schema.planner_schema_view();
    let second = schema.planner_schema_view();
    assert!(first.shares_image_with(&second));
    assert!(!Arc::ptr_eq(&first, &second));
    for _ in 0..2 {
        assert!(schema
            .planner_catalog("testDB", None)
            .try_find_table("testDB", "test")
            .unwrap()
            .is_some());
    }
}

/// Go hands every reader the same `InfoSchema`: a reader's handle shares the
/// image (its own table pins aside), and a mutation of one handle copies the
/// image first, leaving the others as they were.
#[test]
fn readers_share_the_schema_image_and_a_mutation_copies_it() {
    let store = Arc::new(Store::default());
    store
        .tables
        .lock()
        .unwrap()
        .insert((1, 10), table(10, "test"));
    let mut db = database(1, "testDB");
    db.table_name2id = Some(GoShared::new(BTreeMap::from([("test".to_owned(), 10)])));
    let mut published = crate::SessionExtendedInfoSchema::init_with_db_infos(
        vec![GoShared::new(db)],
        vec![],
        vec![],
        2,
    );
    published.set_schema_ts(42);
    published.init_with_db_infos_v2(Arc::new(Data::new()), store, 1 << 20);
    let reader = published.clone();
    assert!(reader.shares_schema_image_with(&published));
    let mut writer = published.clone();
    writer.set_schema_ts(43);
    assert!(!writer.shares_schema_image_with(&published));
    assert_eq!(published.schema_ts(), 42);
    assert_eq!(reader.schema_ts(), 42);
    assert!(reader.table_info_by_name("testDB", "test").is_some());
}

/// Go `SessionExtendedInfoSchema.UpdateTableInfo` (`MdlTables`): the
/// transaction's image reads the table it is given from then on, while its
/// schema version -- the one the transaction pinned -- stays.
#[test]
fn update_table_info_swaps_one_table_and_keeps_the_pinned_version() {
    let revised = |revision: u64| {
        let cached = table(10, "t");
        let mut info = cached.info.read().clone_like_go();
        info.revision = revision;
        let ids = tidb_table::table_from_meta::LocalTableAutoIds::default();
        let built = tidb_table::table_from_meta::cluster_table(
            &info,
            &tidb_table::table_from_meta::AutoIdSource::In { db_id: 1, ids: &ids },
        )
        .unwrap();
        Arc::new(TableEntry::Kv(Arc::new(built)))
    };
    let mut schema = crate::SessionExtendedInfoSchema::init_with_db_infos(
        vec![GoShared::new(database(1, "testDB"))],
        vec![],
        vec![],
        5,
    );
    assert!(schema.update_table_info("testdb", "T", revised(1)));
    let (version, metadata) = (schema.schema_meta_version(), schema.schema_version());
    let revision = |schema: &crate::SessionExtendedInfoSchema| match schema.get_in("testDB", "t") {
        Some(TableEntry::Kv(table)) => table.meta().read().revision,
        _ => panic!("the table is in the image"),
    };
    assert_eq!(revision(&schema), 1);
    assert!(schema.update_table_info("testDB", "t", revised(2)));
    assert_eq!(revision(&schema), 2, "the swapped table is what the image reads");
    assert_eq!(schema.schema_meta_version(), version, "the pinned schema version stays");
    assert_eq!(schema.schema_version(), metadata, "the plan cache's image identity stays");
    assert!(!schema.update_table_info("missing", "t", revised(3)), "no such database");
}
