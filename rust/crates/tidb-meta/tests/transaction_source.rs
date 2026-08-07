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

//! Boundary receipts for the transaction-backed part of Go `TestMeta`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use tidb_ast::CiString;
use tidb_meta::transaction::{
    extract_schema_and_table_name_from_job, fast_unmarshal_table_name_info, iter_all_tables,
    job_matches, oldest_schema_version, split_range_int64_max, table_info_must_load, unescape_name,
    unescape_name_bytes, AutoIdGroup, DailyRuStats, DdlJobCodec, DdlTableVersion, GroupRuStats,
    MemoryTransaction, MetaSnapshotStore, MustLoadFilterAttr, Mutator, MutatorOption, MvccInfo,
    MvccReader, MvccWrite, NextGenBootTableVersion, RawTransaction, ResourceGroupCodec,
    RuConsumption, RuStats, TtlTuneFactors, NAME_EXTRACT_REGEXP,
};
use tidb_meta::{key, structure, value, MetaError, Result};
use tidb_model::placement::PlacementSettings;
use tidb_model::{
    ActionType, DBInfo, MaskingPolicyInfo, MaskingPolicyStatus, MaskingPolicyType, PolicyInfo,
    SchemaDiff, TableInfo,
};

#[test]
fn global_ids_are_atomic_contiguous_and_source_limited() {
    let meta = Mutator::new(MemoryTransaction::at_start_ts(42));
    assert_eq!(meta.start_ts(), 42);
    assert!(meta
        .inspect(MemoryTransaction::configured_for_meta)
        .unwrap());
    assert_eq!(meta.gen_global_id().unwrap(), 1);
    assert_eq!(meta.global_id().unwrap(), 1);

    let first = meta.clone();
    let second = meta.clone();
    let a = thread::spawn(move || first.gen_global_ids(3).unwrap());
    let b = thread::spawn(move || second.gen_global_ids(4).unwrap());
    let (a, b) = (a.join().unwrap(), b.join().unwrap());
    assert!((a == [2, 3, 4] && b == [5, 6, 7, 8]) || (a == [6, 7, 8] && b == [2, 3, 4, 5]));
    assert_eq!(meta.global_id().unwrap(), 8);
    assert_eq!(meta.global_id_key(), key::next_global_id_kv_key());

    assert_eq!(meta.advance_global_ids(2).unwrap(), 8);
    assert_eq!(meta.global_id().unwrap(), 10);
}

#[test]
fn new_mutator_configures_transaction_and_runs_options_in_source_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let first = Arc::clone(&order);
    let second = Arc::clone(&order);
    let mut options: Vec<MutatorOption<MemoryTransaction>> = vec![
        Box::new(move |meta| {
            assert_eq!(meta.start_ts(), 55);
            first.lock().unwrap().push(1);
        }),
        Box::new(move |_| second.lock().unwrap().push(2)),
    ];
    let meta = Mutator::new_with_options(MemoryTransaction::at_start_ts(55), &mut options);
    assert_eq!(*order.lock().unwrap(), [1, 2]);
    assert!(meta
        .inspect(MemoryTransaction::configured_for_meta)
        .unwrap());
}

#[test]
fn negative_global_id_batch_mutates_before_go_make_panic() {
    let meta = Mutator::new(MemoryTransaction::default());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        meta.gen_global_ids(-1).unwrap();
    }));
    assert!(panic.is_err());
    assert_eq!(meta.global_id().unwrap(), -1);
    assert_eq!(
        meta.gen_global_id().unwrap(),
        0,
        "Go's deferred unlock leaves allocation usable after the panic"
    );
}

#[test]
fn global_id_zero_limit_error_and_signed_wrap_match_go_mutation_order() {
    let meta = Mutator::new(MemoryTransaction::default());
    assert!(meta.gen_global_ids(0).unwrap().is_empty());
    assert_eq!(meta.global_id().unwrap(), 0);
    assert_eq!(meta.advance_global_ids(0).unwrap(), 0);

    let limit = tidb_metadef::system::MAX_USER_GLOBAL_ID;
    let mut transaction = MemoryTransaction::default();
    transaction
        .set(
            key::next_global_id_kv_key(),
            value::encode_int_value(limit - 1),
        )
        .unwrap();
    let meta = Mutator::new(transaction);
    assert_eq!(meta.gen_global_id().unwrap(), limit);
    assert_eq!(
        meta.gen_global_id(),
        Err(MetaError::GlobalIdExceedsLimit {
            generated: limit + 1,
            limit,
        })
    );
    assert_eq!(
        meta.global_id().unwrap(),
        limit + 1,
        "Go checks the limit after persisting the increment"
    );

    let mut transaction = MemoryTransaction::default();
    transaction
        .set(
            key::next_global_id_kv_key(),
            value::encode_int_value(i64::MAX),
        )
        .unwrap();
    let meta = Mutator::new(transaction);
    assert_eq!(meta.gen_global_id().unwrap(), i64::MIN);

    let mut transaction = MemoryTransaction::default();
    transaction
        .set(
            key::next_global_id_kv_key(),
            value::encode_int_value(limit - 1),
        )
        .unwrap();
    let meta = Mutator::new(transaction);
    assert_eq!(meta.advance_global_ids(1).unwrap(), limit - 1);
    assert!(matches!(
        meta.advance_global_ids(1),
        Err(MetaError::GlobalIdExceedsLimit { generated, limit: observed })
            if generated == limit + 1 && observed == limit
    ));
    assert_eq!(meta.global_id().unwrap(), limit + 1);

    let mut transaction = MemoryTransaction::default();
    transaction
        .set(
            key::next_global_id_kv_key(),
            value::encode_int_value(limit - 1),
        )
        .unwrap();
    let meta = Mutator::new(transaction);
    assert_eq!(meta.gen_global_ids(1).unwrap(), [limit]);
    assert!(matches!(
        meta.gen_global_ids(1),
        Err(MetaError::GlobalIdExceedsLimit { generated, limit: observed })
            if generated == limit + 1 && observed == limit
    ));
    assert_eq!(meta.global_id().unwrap(), limit + 1);

    let mut transaction = MemoryTransaction::default();
    transaction
        .set(
            key::next_global_id_kv_key(),
            value::encode_int_value(i64::MAX),
        )
        .unwrap();
    assert_eq!(
        Mutator::new(transaction).advance_global_ids(1).unwrap(),
        i64::MAX
    );

    assert_eq!(meta.gen_placement_policy_id().unwrap(), 1);
    assert_eq!(meta.policy_id().unwrap(), 1);
    assert_eq!(meta.gen_masking_policy_id().unwrap(), 1);
    assert_eq!(meta.masking_policy_id().unwrap(), 1);
}

#[test]
fn database_and_table_lifecycle_preserves_go_order_and_partial_mutation() {
    let meta = Mutator::new(MemoryTransaction::default());
    assert_eq!(meta.schema_version().unwrap(), 0);
    assert_eq!(meta.gen_schema_version().unwrap(), 1);
    assert_eq!(meta.gen_schema_versions(2).unwrap(), 3);

    let mut database = DBInfo {
        id: 1,
        name: CiString::new("a"),
        ..Default::default()
    };
    meta.create_database(&database).unwrap();
    assert_eq!(
        meta.create_database(&database),
        Err(MetaError::DatabaseExists)
    );
    assert_eq!(meta.database(1).unwrap().unwrap().name.original(), "a");

    database.name = CiString::new("aa");
    meta.update_database(&database).unwrap();
    assert_eq!(meta.databases().unwrap()[0].name.original(), "aa");
    assert!(meta.database(999).unwrap().is_none());
    assert_eq!(
        meta.update_database(&DBInfo {
            id: 999,
            ..Default::default()
        }),
        Err(MetaError::DatabaseNotExists)
    );

    let mut table = TableInfo {
        id: 1,
        name: CiString::new("t"),
        db_id: 1,
        ..Default::default()
    };
    meta.create_table_or_view(1, &table).unwrap();
    assert_eq!(
        meta.create_table_or_view(1, &table),
        Err(MetaError::TableExists)
    );
    assert_eq!(
        meta.create_table_or_view(2, &table),
        Err(MetaError::DatabaseNotExists)
    );

    table.name = CiString::new("tt");
    meta.update_table(1, &mut table).unwrap();
    assert_eq!(table.revision, 1, "Go increments the caller before marshal");
    let stored = meta.table(1, 1).unwrap().unwrap();
    assert_eq!(stored.name.original(), "tt");
    assert_eq!(stored.db_id, 1, "DBID is restored after JSON decode");
    assert!(meta.table_exists(1, 1).unwrap());
    assert!(!meta.table_exists(1, 2).unwrap());
    assert!(meta.table(1, 999).unwrap().is_none());
    assert!(matches!(
        meta.table(999, 1),
        Err(MetaError::DatabaseNotExists)
    ));
    assert_eq!(meta.table_exists(999, 1), Err(MetaError::DatabaseNotExists));
    let mut missing_table = TableInfo {
        id: 999,
        revision: 10,
        ..Default::default()
    };
    assert_eq!(
        meta.update_table(1, &mut missing_table),
        Err(MetaError::TableNotExists)
    );
    assert_eq!(missing_table.revision, 10);
    assert_eq!(
        meta.update_table(999, &mut missing_table),
        Err(MetaError::DatabaseNotExists)
    );
    assert_eq!(missing_table.revision, 10);

    let table_two = TableInfo {
        id: 2,
        name: CiString::new("bb"),
        ..Default::default()
    };
    meta.create_table_or_view(1, &table_two).unwrap();
    let listed = meta.tables(1).unwrap();
    assert_eq!(
        listed.iter().map(|table| table.id).collect::<Vec<_>>(),
        [1, 2]
    );
    let simple = meta.simple_tables(1).unwrap();
    assert_eq!(
        simple
            .iter()
            .map(|table| (table.id, table.name.original()))
            .collect::<Vec<_>>(),
        [(1, "tt"), (2, "bb")]
    );
    let mut iterated = Vec::new();
    meta.iter_tables(1, |table| {
        iterated.push(table.id);
        Ok(())
    })
    .unwrap();
    assert_eq!(iterated, [1, 2]);
    let mut callbacks = 0;
    assert_eq!(
        meta.iter_tables(1, |_| {
            callbacks += 1;
            Err(MetaError::Storage("sentinel".to_owned()))
        }),
        Err(MetaError::Storage("sentinel".to_owned()))
    );
    assert_eq!(callbacks, 1);

    assert_eq!(meta.increment_row_id(1, 2, 10).unwrap(), 10);
    assert_eq!(meta.row_id(1, 2).unwrap(), 10);
    meta.drop_table_or_view(1, 2).unwrap();
    assert_eq!(
        meta.drop_table_or_view(1, 2),
        Err(MetaError::TableNotExists)
    );
    assert_eq!(
        meta.drop_table_or_view(999, 2),
        Err(MetaError::DatabaseNotExists)
    );
    assert_eq!(
        meta.tables(1)
            .unwrap()
            .into_iter()
            .map(|table| table.id)
            .collect::<Vec<_>>(),
        [1]
    );
    assert_eq!(
        meta.simple_tables(1)
            .unwrap()
            .into_iter()
            .map(|table| table.id)
            .collect::<Vec<_>>(),
        [1]
    );
    // Go DropTableOrView deliberately leaves the allocator behind.
    assert_eq!(meta.row_id(1, 2).unwrap(), 10);
    meta.delete_row_id(1, 2).unwrap();
    assert_eq!(meta.row_id(1, 2).unwrap(), 0);

    meta.drop_database(1).unwrap();
    meta.drop_database(1234).unwrap();
    assert!(meta.databases().unwrap().is_empty());
    assert!(matches!(meta.tables(1), Err(MetaError::DatabaseNotExists)));
    assert_eq!(meta.row_id(1, 1).unwrap(), 0);
}

#[test]
fn raw_storage_failures_propagate_and_keep_go_partial_mutation_order() {
    let source = Mutator::new(MemoryTransaction::default());
    source
        .create_database(&DBInfo {
            id: 1,
            name: CiString::new("db"),
            ..Default::default()
        })
        .unwrap();
    source
        .create_table_or_view(
            1,
            &TableInfo {
                id: 1,
                name: CiString::new("t1"),
                ..Default::default()
            },
        )
        .unwrap();
    source
        .create_policy(&PolicyInfo {
            id: 1,
            name: CiString::new("p1"),
            ..Default::default()
        })
        .unwrap();
    let base = source.inspect(Clone::clone).unwrap();

    let read_error = Mutator::new(base.clone().with_get_error(0, "point read"));
    assert_eq!(
        read_error.database_exists(1),
        Err(MetaError::Storage("point read".to_owned()))
    );
    let read_error = Mutator::new(base.clone().with_get_error(1, "second table read"));
    assert_eq!(
        read_error.create_table_or_view(
            1,
            &TableInfo {
                id: 2,
                ..Default::default()
            }
        ),
        Err(MetaError::Storage("second table read".to_owned()))
    );

    let scan_error = Mutator::new(base.clone().with_scan_error(0, "hash scan"));
    assert!(matches!(
        scan_error.databases(),
        Err(MetaError::Storage(message)) if message == "hash scan"
    ));
    let scan_error = Mutator::new(base.clone().with_scan_error(0, "database hash scan"));
    assert!(matches!(
        scan_error.metas_by_database_id(1),
        Err(MetaError::Storage(message)) if message == "database hash scan"
    ));

    let write_error = Mutator::new(base.clone().with_set_error(0, "table write"));
    assert_eq!(
        write_error.create_table_or_view(
            1,
            &TableInfo {
                id: 2,
                ..Default::default()
            }
        ),
        Err(MetaError::Storage("table write".to_owned()))
    );
    assert!(write_error.table(1, 2).unwrap().is_none());

    let update_write = Mutator::new(base.clone().with_set_error(0, "table update write"));
    let mut updated = update_write.table(1, 1).unwrap().unwrap();
    updated.name = CiString::new("caller-mutated");
    assert_eq!(
        update_write.update_table(1, &mut updated),
        Err(MetaError::Storage("table update write".to_owned()))
    );
    assert_eq!(
        updated.revision, 1,
        "Go increments the caller before the failing marshal/write"
    );
    let still_stored = update_write.table(1, 1).unwrap().unwrap();
    assert_eq!(still_stored.revision, 0);
    assert_eq!(still_stored.name.original(), "t1");

    let partial = Mutator::new(base.clone().with_set_error(1, "row allocator write"));
    assert_eq!(
        partial.create_table_and_set_auto_id(
            1,
            &TableInfo {
                id: 3,
                name: CiString::new("partial"),
                ..Default::default()
            },
            AutoIdGroup {
                row_id: 99,
                ..Default::default()
            }
        ),
        Err(MetaError::Storage("row allocator write".to_owned()))
    );
    assert!(partial.table(1, 3).unwrap().is_some());
    assert_eq!(partial.auto_ids(1, 3).row_id().get().unwrap(), 0);

    let policy_write = Mutator::new(base.clone().with_set_error(0, "policy write"));
    assert_eq!(
        policy_write.create_policy(&PolicyInfo {
            id: 2,
            ..Default::default()
        }),
        Err(MetaError::Storage("policy write".to_owned()))
    );
    assert!(matches!(
        policy_write.policy(2),
        Err(MetaError::PolicyIdNotExists(2))
    ));

    let scalar_write = Mutator::new(base.clone().with_set_error(0, "scalar write"));
    assert_eq!(
        scalar_write.set_metadata_lock(true),
        Err(MetaError::Storage("scalar write".to_owned()))
    );
    assert_eq!(scalar_write.metadata_lock().unwrap(), None);

    let table_delete = Mutator::new(base.clone().with_delete_error(0, "table delete"));
    assert_eq!(
        table_delete.drop_table_or_view(1, 1),
        Err(MetaError::Storage("table delete".to_owned()))
    );
    assert!(table_delete.table(1, 1).unwrap().is_some());

    let policy_delete = Mutator::new(base.clone().with_delete_error(0, "policy delete"));
    assert_eq!(
        policy_delete.drop_policy(1),
        Err(MetaError::Storage("policy delete".to_owned()))
    );
    assert_eq!(policy_delete.policy(1).unwrap().id, 1);

    let database_delete = Mutator::new(base.with_delete_error(0, "database clear"));
    assert_eq!(
        database_delete.drop_database(1),
        Err(MetaError::Storage("database clear".to_owned()))
    );
    assert!(database_delete.database_exists(1).unwrap());
}

#[test]
fn catalog_fast_paths_cover_metas_names_attributes_cancellation_and_corruption() {
    let meta = Mutator::new(MemoryTransaction::default());
    meta.create_database(&DBInfo {
        id: 1,
        name: CiString::new("db"),
        ..Default::default()
    })
    .unwrap();
    for table in [
        TableInfo {
            id: 10,
            name: CiString::new("ordinary"),
            ..Default::default()
        },
        TableInfo {
            id: 11,
            name: CiString::new("Special"),
            affinity: Some(Box::new(tidb_model::table::TableAffinityInfo {
                level: "s".to_owned(),
            })),
            ..Default::default()
        },
    ] {
        meta.create_table_or_view(1, &table).unwrap();
    }
    meta.auto_ids(1, 10).row_id().put(7).unwrap();

    let metas = meta.metas_by_database_id(1).unwrap();
    assert_eq!(metas.len(), 3);
    assert!(metas
        .iter()
        .any(|pair| pair.field == key::auto_table_id_key(10) && pair.value == b"7"));

    let (names, must_load) = meta.all_name_to_id_and_must_loaded_table_info(1).unwrap();
    assert_eq!(names.get(b"ordinary".as_slice()), Some(&10));
    assert_eq!(names.get(b"Special".as_slice()), Some(&11));
    assert_eq!(
        must_load.iter().map(|table| table.id).collect::<Vec<_>>(),
        [11]
    );

    let selected = meta
        .table_info_with_attributes(
            1,
            &[MustLoadFilterAttr {
                attr: br#""affinity":{"#,
                load_if_missing: false,
            }],
        )
        .unwrap();
    assert_eq!(
        selected.iter().map(|table| table.id).collect::<Vec<_>>(),
        [11]
    );

    let mut cancellation_checks = 0;
    assert!(matches!(
        meta.tables_with_cancel(1, || {
            cancellation_checks += 1;
            true
        }),
        Err(MetaError::Cancelled)
    ));
    assert_eq!(
        cancellation_checks, 1,
        "Go checks cancellation only after filtering to a table field"
    );

    assert_eq!(
        meta.metas_by_database_id(99),
        Err(MetaError::DatabaseNotExists)
    );

    let mut corrupted = meta.inspect(Clone::clone).unwrap();
    corrupted
        .set(key::table_kv_key(1, 99), br#"{"id":99}"#.to_vec())
        .unwrap();
    let corrupted = Mutator::new(corrupted);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        corrupted
            .all_name_to_id_and_must_loaded_table_info(1)
            .unwrap();
    }))
    .is_err());
    assert!(corrupted.simple_tables(1).is_err());
}

#[test]
fn system_database_creation_and_iteration_cover_classic_and_nextgen_rules() {
    let meta = Mutator::new(MemoryTransaction::default());
    assert!(!meta.database_exists(42).unwrap());
    meta.create_sys_database_by_id_if_not_exists("mysql", 42)
        .unwrap();
    meta.create_sys_database_by_id_if_not_exists("ignored", 42)
        .unwrap();
    assert!(meta.database_exists(42).unwrap());
    assert_eq!(meta.system_database_id().unwrap(), 42);
    assert_eq!(meta.database(42).unwrap().unwrap().name.original(), "mysql");

    let mut names = Vec::new();
    meta.iter_databases(|database| {
        names.push(database.name.original().to_owned());
        Ok(())
    })
    .unwrap();
    assert_eq!(names, ["mysql"]);
    let mut callbacks = 0;
    assert_eq!(
        meta.iter_databases(|_| {
            callbacks += 1;
            Err(MetaError::Storage("stop databases".to_owned()))
        }),
        Err(MetaError::Storage("stop databases".to_owned()))
    );
    assert_eq!(callbacks, 1);

    let fresh = Mutator::new(MemoryTransaction::default());
    let id = fresh.create_mysql_database_if_not_exists().unwrap();
    if tidb_config::kerneltype::is_next_gen() {
        assert_eq!(id, tidb_metadef::system::SYSTEM_DATABASE_ID);
        assert_eq!(fresh.global_id().unwrap(), 0);
    } else {
        assert_eq!(id, 1);
        assert_eq!(fresh.global_id().unwrap(), 1);
    }
    assert_eq!(fresh.create_mysql_database_if_not_exists().unwrap(), id);
    assert_eq!(fresh.system_database_id().unwrap(), id);
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RecordedResourceGroup {
    id: i64,
    name: String,
}

impl ResourceGroupCodec for RecordedResourceGroup {
    fn id(&self) -> i64 {
        self.id
    }

    fn lower_name(&self) -> &str {
        &self.name
    }

    fn default_group() -> Self {
        Self {
            id: 1,
            name: "default".to_owned(),
        }
    }

    fn encode_json(&self) -> Result<Vec<u8>> {
        tidb_model::serde_helpers::to_go_json(self)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))
    }

    fn decode_json(encoded: &[u8]) -> Result<Self> {
        serde_json::from_slice(encoded).map_err(|error| MetaError::InvalidJson(error.to_string()))
    }
}

#[test]
fn policies_masking_policies_and_resource_groups_preserve_source_lifecycle() {
    let meta = Mutator::new(MemoryTransaction::default());

    let zero_policy = PolicyInfo::default();
    assert_eq!(
        meta.create_policy(&zero_policy),
        Err(MetaError::InvalidObjectId("policy"))
    );
    let mut policy = PolicyInfo {
        id: 7,
        name: CiString::new("p"),
        placement_settings: Some(Box::new(PlacementSettings {
            primary_region: "my primary".to_owned(),
            regions: "my regions".to_owned(),
            learners: 1,
            followers: 2,
            voters: 3,
            schedule: "even".to_owned(),
            constraints: "+disk=ssd".to_owned(),
            learner_constraints: "+zone=shanghai".to_owned(),
            ..PlacementSettings::default()
        })),
        ..PolicyInfo::default()
    };
    meta.create_policy(&policy).unwrap();
    assert_eq!(meta.create_policy(&policy), Err(MetaError::PolicyExists));
    let read_policy = meta.policy(7).unwrap();
    assert_eq!(read_policy.name.original(), "p");
    assert_eq!(
        read_policy
            .placement_settings
            .as_ref()
            .unwrap()
            .primary_region,
        "my primary"
    );
    assert_eq!(
        read_policy
            .placement_settings
            .as_ref()
            .unwrap()
            .learner_constraints,
        "+zone=shanghai"
    );
    policy.name = CiString::new("p2");
    policy
        .placement_settings
        .as_mut()
        .unwrap()
        .learner_constraints = "+zone=nanjing".to_owned();
    meta.update_policy(&policy).unwrap();
    let listed = meta.policies().unwrap();
    assert_eq!(listed[0].name.original(), "p2");
    assert_eq!(
        listed[0]
            .placement_settings
            .as_ref()
            .unwrap()
            .learner_constraints,
        "+zone=nanjing"
    );
    let policy_field = structure::encode_hash_data_key(key::POLICIES, b"Policy:7");
    let raw_policy = meta
        .inspect(|transaction| transaction.entries()[&policy_field].clone())
        .unwrap();
    assert_eq!(raw_policy[0], value::CURRENT_MAGIC_BYTE_VER);
    assert_eq!(
        std::str::from_utf8(&raw_policy[1..]).unwrap(),
        r#"{"primary_region":"my primary","regions":"my regions","learners":1,"followers":2,"voters":3,"schedule":"even","constraints":"+disk=ssd","leader_constraints":"","learner_constraints":"+zone=nanjing","follower_constraints":"","voter_constraints":"","survival_preferences":"","id":7,"name":{"O":"p2","L":"p2"},"state":0}"#
    );
    // Reopening a new mutator over the stored transaction is the in-memory
    // equivalent of Go's post-commit reread.
    let reopened = Mutator::new(meta.inspect(Clone::clone).unwrap());
    assert_eq!(
        reopened
            .policy(7)
            .unwrap()
            .placement_settings
            .unwrap()
            .learner_constraints,
        "+zone=nanjing"
    );
    assert_eq!(
        meta.update_policy(&PolicyInfo {
            id: 8,
            ..Default::default()
        }),
        Err(MetaError::PolicyNotExists)
    );
    meta.drop_policy(7).unwrap();
    meta.drop_policy(7).unwrap();
    assert!(matches!(
        meta.policy(7),
        Err(MetaError::PolicyIdNotExists(7))
    ));

    let zero_masking = MaskingPolicyInfo::default();
    assert_eq!(
        meta.create_masking_policy(&zero_masking),
        Err(MetaError::InvalidObjectId("masking policy"))
    );
    let mut masking = MaskingPolicyInfo {
        id: 9,
        name: CiString::new("m"),
        table_id: i64::MAX,
        column_id: -7,
        expression: "mask(c < 1)".to_owned(),
        status: MaskingPolicyStatus::ENABLED,
        masking_type: MaskingPolicyType::MASK_FULL,
        restrict_ops: tidb_ast::MaskingPolicyRestrictOps::from_bits(u64::MAX),
        created_by: "root@%".to_owned(),
        updated_by: "root@%".to_owned(),
        state: tidb_model::SchemaState::PUBLIC,
        ..MaskingPolicyInfo::default()
    };
    meta.create_masking_policy(&masking).unwrap();
    assert_eq!(
        meta.create_masking_policy(&masking),
        Err(MetaError::MaskingPolicyIdExists(9))
    );
    masking.name = CiString::new("m2");
    meta.update_masking_policy(&masking).unwrap();
    assert_eq!(meta.masking_policy(9).unwrap(), masking);
    assert_eq!(meta.masking_policies().unwrap(), [masking.clone()]);
    let reopened = Mutator::new(meta.inspect(Clone::clone).unwrap());
    assert_eq!(reopened.masking_policy(9).unwrap(), masking);
    assert_eq!(
        meta.update_masking_policy(&MaskingPolicyInfo {
            id: 10,
            name: CiString::new("missing"),
            ..MaskingPolicyInfo::default()
        }),
        Err(MetaError::MaskingPolicyIdNotExists(10))
    );
    meta.drop_masking_policy(9).unwrap();
    meta.drop_masking_policy(9).unwrap();

    assert_eq!(
        meta.resource_group::<RecordedResourceGroup>(1).unwrap(),
        RecordedResourceGroup::default_group()
    );
    assert_eq!(
        meta.resource_groups::<RecordedResourceGroup>().unwrap(),
        [RecordedResourceGroup::default_group()]
    );
    assert_eq!(
        meta.add_resource_group(&RecordedResourceGroup {
            id: 0,
            name: "zero".to_owned(),
        }),
        Err(MetaError::InvalidObjectId("group"))
    );
    let group = RecordedResourceGroup {
        id: 2,
        name: "analytics".to_owned(),
    };
    meta.add_resource_group(&group).unwrap();
    assert_eq!(
        meta.add_resource_group(&group),
        Err(MetaError::ResourceGroupExists)
    );
    assert_eq!(
        meta.resource_group::<RecordedResourceGroup>(2).unwrap(),
        group
    );
    assert_eq!(
        meta.update_resource_group(&RecordedResourceGroup {
            id: 3,
            name: "missing".to_owned(),
        }),
        Err(MetaError::ResourceGroupNotExists)
    );
    // Go allows the implicit default group to be updated before persistence.
    meta.update_resource_group(&RecordedResourceGroup {
        id: 1,
        name: "default".to_owned(),
    })
    .unwrap();
    assert_eq!(
        meta.resource_groups::<RecordedResourceGroup>()
            .unwrap()
            .len(),
        2
    );
    meta.drop_resource_group(2).unwrap();
    meta.drop_resource_group(2).unwrap();
    assert_eq!(
        meta.resource_group::<RecordedResourceGroup>(2),
        Err(MetaError::ResourceGroupIdNotExists(2))
    );
}

#[test]
fn policy_masking_and_resource_reads_preserve_magic_json_and_empty_panics() {
    let with_hash_value = |hash: &[u8], field: &[u8], stored: &[u8]| {
        let mut transaction = MemoryTransaction::default();
        transaction
            .set(
                structure::encode_hash_data_key(hash, field),
                stored.to_vec(),
            )
            .unwrap();
        Mutator::new(transaction)
    };

    assert!(matches!(
        with_hash_value(key::POLICIES, &key::policy_key(7), b"\x01{}").policy(7),
        Err(MetaError::IncompatibleMagicType)
    ));
    assert!(matches!(
        with_hash_value(key::POLICIES, &key::policy_key(7), b"\x00{").policies(),
        Err(MetaError::InvalidJson(_))
    ));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = with_hash_value(key::POLICIES, &key::policy_key(7), b"").policy(7);
    }))
    .is_err());

    assert_eq!(
        with_hash_value(
            key::MASKING_POLICIES,
            &key::masking_policy_key(8),
            b"\x40{}"
        )
        .masking_policy(8),
        Err(MetaError::UnknownMagicType)
    );
    assert!(matches!(
        with_hash_value(key::MASKING_POLICIES, &key::masking_policy_key(8), b"\x00{")
            .masking_policies(),
        Err(MetaError::InvalidJson(_))
    ));

    assert_eq!(
        with_hash_value(key::RESOURCE_GROUPS, &key::resource_group_key(9), b"\x3f{}")
            .resource_group::<RecordedResourceGroup>(9),
        Err(MetaError::IncompatibleMagicType)
    );
    assert!(matches!(
        with_hash_value(key::RESOURCE_GROUPS, &key::resource_group_key(9), b"\x00{")
            .resource_groups::<RecordedResourceGroup>(),
        Err(MetaError::InvalidJson(_))
    ));
}

#[test]
fn declared_meta_errors_keep_source_codes_and_context_messages() {
    let rows = [
        (
            MetaError::DatabaseExists,
            1007,
            "[meta:1007]database already exists",
        ),
        (
            MetaError::DatabaseNotExists,
            1049,
            "[meta:1049]database doesn't exist",
        ),
        (
            MetaError::TableExists,
            1050,
            "[meta:1050]table already exists",
        ),
        (
            MetaError::TableNotExists,
            1146,
            "[meta:1146]table doesn't exist",
        ),
        (
            MetaError::DdlReorgElementNotExist,
            8235,
            "[meta:8235]DDL reorg element does not exist",
        ),
        (
            MetaError::PolicyExists,
            8238,
            "[meta:8238]policy already exists",
        ),
        (
            MetaError::PolicyIdNotExists(8),
            8239,
            "[meta:8239]policy id : 8 doesn't exist",
        ),
        (
            MetaError::ResourceGroupExists,
            8248,
            "[meta:8248]group already exists",
        ),
        (
            MetaError::ResourceGroupIdNotExists(12),
            8249,
            "[meta:8249]resource group id : 12 doesn't exist",
        ),
        (
            MetaError::MaskingPolicyIdExists(9),
            8268,
            "masking policy id : 9 already exists: [meta:8268]masking policy already exists",
        ),
        (
            MetaError::MaskingPolicyIdNotExists(10),
            8269,
            "masking policy id : 10 doesn't exist: [meta:8269]masking policy doesn't exist",
        ),
        (
            MetaError::MaskingPolicyExpressionInvalidColumn,
            8275,
            "[meta:8275]masking policy expression can only reference the target column '%-.64s'",
        ),
    ];
    for (error, code, message) in rows {
        assert_eq!(error.code(), Some(code));
        assert_eq!(error.to_string(), message);
    }
}

#[test]
fn bootstrap_schema_diff_and_raw_bdr_role_round_trip() {
    let meta = Mutator::new(MemoryTransaction::default());
    assert_eq!(meta.bootstrap_version().unwrap(), 0);
    meta.finish_bootstrap(1).unwrap();
    assert_eq!(meta.bootstrap_version().unwrap(), 1);
    meta.finish_bootstrap(10).unwrap();
    assert_eq!(meta.bootstrap_version().unwrap(), 10);

    let diff = SchemaDiff {
        version: 100,
        action_type: ActionType::ACTION_TRUNCATE_TABLE,
        schema_id: 1,
        table_id: 2,
        old_table_id: 3,
        ..Default::default()
    };
    meta.set_schema_diff(&diff).unwrap();
    assert_eq!(meta.schema_diff(100).unwrap(), Some(diff));
    assert_eq!(meta.schema_diff(101).unwrap(), None);

    assert!(meta.bdr_role().unwrap().is_empty());
    meta.set_bdr_role(&[0xff, b'p']).unwrap();
    assert_eq!(meta.bdr_role().unwrap(), [0xff, b'p']);
    meta.clear_bdr_role().unwrap();
    assert!(meta.bdr_role().unwrap().is_empty());
}

#[test]
fn global_and_auto_id_key_bytes_remain_the_go_vectors() {
    let meta = Mutator::new(MemoryTransaction::default());
    assert_eq!(
        meta.global_id_key(),
        vec![
            0x6d, 0x4e, 0x65, 0x78, 0x74, 0x47, 0x6c, 0x6f, 0x62, 0xff, 0x61, 0x6c, 0x49, 0x44,
            0x00, 0x00, 0x00, 0x00, 0xfb, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x73,
        ]
    );
    assert_eq!(
        key::auto_table_id_kv_key(1, 3),
        vec![
            0x6d, 0x44, 0x42, 0x3a, 0x31, 0x00, 0x00, 0x00, 0x00, 0xfb, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x33, 0x00, 0x00, 0x00, 0xfc,
        ]
    );
}

#[test]
fn auto_id_accessors_keep_old_versions_shared_and_copy_zero_is_a_noop() {
    let meta = Mutator::new(MemoryTransaction::default());
    let ids = meta.auto_ids(1, 2);
    ids.put(AutoIdGroup {
        row_id: 3,
        increment_id: 4,
        random_id: 5,
    })
    .unwrap();
    assert_eq!(
        ids.get().unwrap(),
        AutoIdGroup {
            row_id: 3,
            increment_id: 4,
            random_id: 5,
        }
    );
    assert_eq!(ids.increment_id(4).get().unwrap(), 3);
    ids.row_id().copy_to(9, 8).unwrap();
    assert_eq!(meta.auto_ids(9, 8).row_id().get().unwrap(), 3);
    meta.auto_ids(1, 99).row_id().copy_to(9, 7).unwrap();
    assert_eq!(meta.auto_ids(9, 7).row_id().get().unwrap(), 0);
    ids.delete().unwrap();
    assert_eq!(ids.get().unwrap(), AutoIdGroup::default());
}

#[test]
fn composed_auto_id_sequence_and_schema_diff_methods_preserve_source_order() {
    let meta = Mutator::new(MemoryTransaction::default());
    meta.create_database(&DBInfo {
        id: 1,
        name: CiString::new("db"),
        ..Default::default()
    })
    .unwrap();

    let table = TableInfo {
        id: 3,
        name: CiString::new("t"),
        ..Default::default()
    };
    meta.create_table_and_set_auto_id(
        1,
        &table,
        AutoIdGroup {
            row_id: 123,
            increment_id: 0,
            random_id: 0,
        },
    )
    .unwrap();
    assert_eq!(meta.auto_ids(1, 3).row_id().get().unwrap(), 123);
    assert_eq!(
        meta.auto_table_id_key_value(1, 3, 1234),
        (key::auto_table_id_kv_key(1, 3), b"1234".to_vec())
    );

    // Go intentionally permits allocator fields for nonexistent DB/table IDs.
    assert_eq!(meta.auto_ids(1, 999).row_id().increment(10).unwrap(), 10);
    assert_eq!(meta.auto_ids(999, 3).row_id().increment(11).unwrap(), 11);

    let random_table = TableInfo {
        id: 5,
        name: CiString::new("random"),
        auto_random_bits: 5,
        ..Default::default()
    };
    meta.create_table_and_set_auto_id(
        1,
        &random_table,
        AutoIdGroup {
            random_id: 77,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(meta.auto_ids(1, 5).random_id().get().unwrap(), 77);

    let mut auto_increment_column = tidb_model::column::ColumnInfo::new_extra_handle_col_info();
    auto_increment_column.set_flag(1 << 9);
    let increment_table = TableInfo {
        id: 6,
        name: CiString::new("increment"),
        version: tidb_model::table_info::TABLE_INFO_VERSION5,
        auto_id_cache: 1,
        columns: vec![auto_increment_column],
        ..Default::default()
    };
    meta.create_table_and_set_auto_id(
        1,
        &increment_table,
        AutoIdGroup {
            increment_id: 88,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(meta.auto_ids(1, 6).increment_id(5).get().unwrap(), 88);

    let sequence = TableInfo {
        id: 4,
        name: CiString::new("seq"),
        ..Default::default()
    };
    meta.create_sequence_and_set_value(1, &sequence, 50)
        .unwrap();
    assert_eq!(meta.auto_ids(1, 4).sequence_value().get().unwrap(), 50);
    meta.restart_sequence_value(1, &sequence, -9).unwrap();
    assert_eq!(meta.auto_ids(1, 4).sequence_value().get().unwrap(), -9);
    meta.drop_sequence(1, 4).unwrap();
    assert_eq!(meta.auto_ids(1, 4).sequence_value().get().unwrap(), 0);
    assert_eq!(
        meta.restart_sequence_value(1, &sequence, 1),
        Err(MetaError::TableNotExists)
    );
    assert_eq!(
        meta.restart_sequence_value(2, &sequence, 1),
        Err(MetaError::DatabaseNotExists)
    );

    assert_eq!(meta.schema_version_with_non_empty_diff().unwrap(), 0);
    assert_eq!(meta.gen_schema_version().unwrap(), 1);
    assert_eq!(meta.schema_version_with_non_empty_diff().unwrap(), 0);
    let diff = SchemaDiff {
        version: 1,
        action_type: ActionType::ACTION_CREATE_TABLE,
        ..Default::default()
    };
    meta.set_schema_diff(&diff).unwrap();
    assert_eq!(meta.schema_version_with_non_empty_diff().unwrap(), 1);
    assert_eq!(
        meta.encoded_schema_diff_key(1),
        structure::encode_string_data_key(b"Diff:1")
    );
}

#[test]
fn scalar_settings_preserve_absence_formatting_and_non_boolean_lock_bytes() {
    let meta = Mutator::new(MemoryTransaction::default());
    assert_eq!(meta.ddl_table_version().unwrap(), DdlTableVersion::INIT);
    assert_eq!(
        meta.next_gen_boot_table_version().unwrap(),
        NextGenBootTableVersion::INIT
    );
    meta.set_ddl_table_version(DdlTableVersion::DDL_NOTIFIER)
        .unwrap();
    meta.set_next_gen_boot_table_version(NextGenBootTableVersion::MASKING_POLICY)
        .unwrap();
    assert_eq!(
        meta.ddl_table_version().unwrap(),
        DdlTableVersion::DDL_NOTIFIER
    );
    assert_eq!(
        meta.next_gen_boot_table_version().unwrap(),
        NextGenBootTableVersion::MASKING_POLICY
    );

    assert_eq!(meta.metadata_lock().unwrap(), None);
    meta.set_metadata_lock(true).unwrap();
    assert_eq!(meta.metadata_lock().unwrap(), Some(true));
    meta.set_metadata_lock(false).unwrap();
    assert_eq!(meta.metadata_lock().unwrap(), Some(false));

    assert_eq!(meta.schema_cache_size().unwrap(), None);
    meta.set_schema_cache_size(u64::MAX).unwrap();
    assert_eq!(meta.schema_cache_size().unwrap(), Some(u64::MAX));

    assert_eq!(meta.ingest_max_batch_split_ranges().unwrap(), None);
    meta.set_ingest_max_batch_split_ranges(-7).unwrap();
    assert_eq!(meta.ingest_max_batch_split_ranges().unwrap(), Some(-7));
    meta.set_ingest_max_inflight(i64::MAX).unwrap();
    assert_eq!(meta.ingest_max_inflight().unwrap(), Some(i64::MAX));

    meta.set_ingest_max_per_sec(1.005).unwrap();
    assert_eq!(meta.ingest_max_per_sec().unwrap(), Some(1.0));
    meta.set_ingest_max_split_ranges_per_sec(f64::INFINITY)
        .unwrap();
    assert_eq!(
        meta.ingest_max_split_ranges_per_sec().unwrap(),
        Some(f64::INFINITY)
    );
    for (input, expected) in [
        (f64::NAN, "NaN"),
        (f64::NEG_INFINITY, "-Inf"),
        (-0.0, "-0.00"),
        (f64::from_bits(1), "0.00"),
        (2.675, "2.67"),
    ] {
        meta.set_ingest_max_per_sec(input).unwrap();
        let encoded = structure::encode_string_data_key(key::INGEST_MAX_PER_SEC);
        assert_eq!(
            meta.inspect(|transaction| transaction.entries()[&encoded].clone())
                .unwrap(),
            expected.as_bytes()
        );
        let got = meta.ingest_max_per_sec().unwrap().unwrap();
        if input.is_nan() {
            assert!(got.is_nan());
        } else {
            assert_eq!(got.to_bits(), expected.parse::<f64>().unwrap().to_bits());
        }
    }
}

#[test]
fn malformed_scalar_storage_returns_the_source_parse_error_class() {
    let with_string_value = |logical_key: &[u8], stored: &[u8]| {
        let mut transaction = MemoryTransaction::default();
        transaction
            .set(
                structure::encode_string_data_key(logical_key),
                stored.to_vec(),
            )
            .unwrap();
        Mutator::new(transaction)
    };

    for (logical_key, getter) in [
        (
            key::NEXT_GLOBAL_ID,
            Mutator::<MemoryTransaction>::global_id
                as fn(&Mutator<MemoryTransaction>) -> Result<i64>,
        ),
        (key::POLICY_GLOBAL_ID, Mutator::policy_id),
        (key::MASKING_POLICY_GLOBAL_ID, Mutator::masking_policy_id),
        (key::SCHEMA_VERSION, Mutator::schema_version),
        (key::BOOTSTRAP, Mutator::bootstrap_version),
    ] {
        assert_eq!(
            getter(&with_string_value(logical_key, b"x")),
            Err(MetaError::InvalidIntValue)
        );
    }

    assert_eq!(
        with_string_value(key::DDL_TABLE_VERSION, b"x").ddl_table_version(),
        Err(MetaError::InvalidIntValue)
    );
    assert_eq!(
        with_string_value(key::BOOT_TABLE_VERSION, b"x").next_gen_boot_table_version(),
        Err(MetaError::InvalidIntValue)
    );
    assert_eq!(
        with_string_value(key::SCHEMA_CACHE_SIZE, b"-1").schema_cache_size(),
        Err(MetaError::InvalidUnsignedIntValue)
    );
    assert_eq!(
        with_string_value(key::SCHEMA_CACHE_SIZE, b"").schema_cache_size(),
        Ok(None)
    );

    for (logical_key, getter) in [
        (
            key::INGEST_MAX_BATCH_SPLIT_RANGES,
            Mutator::<MemoryTransaction>::ingest_max_batch_split_ranges
                as fn(&Mutator<MemoryTransaction>) -> Result<Option<i64>>,
        ),
        (key::INGEST_MAX_INFLIGHT, Mutator::ingest_max_inflight),
    ] {
        assert_eq!(
            getter(&with_string_value(logical_key, b"one")),
            Err(MetaError::InvalidIntValue)
        );
        assert_eq!(
            getter(&with_string_value(logical_key, b"")),
            Err(MetaError::InvalidIntValue),
            "Go distinguishes an empty stored value from a missing key"
        );
    }

    for (logical_key, getter) in [
        (
            key::INGEST_MAX_SPLIT_RANGES_PER_SEC,
            Mutator::<MemoryTransaction>::ingest_max_split_ranges_per_sec
                as fn(&Mutator<MemoryTransaction>) -> Result<Option<f64>>,
        ),
        (key::INGEST_MAX_PER_SEC, Mutator::ingest_max_per_sec),
    ] {
        for malformed in [b"one".as_slice(), b"", &[0xff]] {
            assert_eq!(
                getter(&with_string_value(logical_key, malformed)),
                Err(MetaError::InvalidFloatValue)
            );
        }
    }

    assert_eq!(
        with_string_value(key::METADATA_LOCK, b"not-one").metadata_lock(),
        Ok(Some(false))
    );
}

#[test]
fn source_range_partial_json_and_filter_boundaries() {
    assert_eq!(
        split_range_int64_max(1),
        [("0".to_owned(), "9999999999999999999".to_owned())]
    );
    let ranges = split_range_int64_max(15);
    assert_eq!(ranges.len(), 15);
    assert_eq!(ranges[0].0, "0");
    assert_eq!(ranges.last().unwrap().1.len(), 19);
    assert!(std::panic::catch_unwind(|| split_range_int64_max(0)).is_err());

    let compact = br#"{"id":7,"name":{"O":"t","L":"t"}} trailing garbage"#;
    let table = fast_unmarshal_table_name_info(compact).unwrap();
    assert_eq!(table.id, 7);
    assert_eq!(table.name.original(), "t");

    assert_eq!(unescape_name(r#"a\"b\\c"#), "a\"b\\c");
    assert_eq!(unescape_name_bytes(b"a\\\"b\\\\c\xff"), b"a\"b\\c\xff");

    // Go takes the first value token and only checks that the name object has
    // exactly two scalar fields; it does not require the keys to be O/L.
    let odd_keys = br#"{"id":8,"name":{"x":"Original","y":false}}"#;
    let table = fast_unmarshal_table_name_info(odd_keys).unwrap();
    assert_eq!(table.name.original(), "Original");
    assert!(fast_unmarshal_table_name_info(br#"{"id":8,"name":{"x":"Original","y":{}}}"#).is_err());
    assert!(table_info_must_load(br#"{"fk_info":null}"#));
    let ordinary = br#"{"fk_info":null,"partition":null,"Lock":null,"tiflash_replica":null,"temp_table_type":0,"policy_ref_info":null,"ttl_info":null,"affinity":null}"#;
    assert!(!table_info_must_load(ordinary));
    assert!(!table_info_must_load(
        br#"{"fk_info":[],"partition":null,"Lock":null,"tiflash_replica":null,"temp_table_type":0,"policy_ref_info":null,"ttl_info":null}"#
    ));
    for special in [
        br#"{"fk_info":[{}],"partition":null,"Lock":null,"tiflash_replica":null,"temp_table_type":0,"policy_ref_info":null,"ttl_info":null}"#.as_slice(),
        br#"{"fk_info":null,"partition":{},"Lock":null,"tiflash_replica":null,"temp_table_type":0,"policy_ref_info":null,"ttl_info":null}"#,
        br#"{"fk_info":null,"partition":null,"Lock":{},"tiflash_replica":null,"temp_table_type":0,"policy_ref_info":null,"ttl_info":null}"#,
        br#"{"fk_info":null,"partition":null,"Lock":null,"tiflash_replica":{},"temp_table_type":0,"policy_ref_info":null,"ttl_info":null}"#,
        br#"{"fk_info":null,"partition":null,"Lock":null,"tiflash_replica":null,"temp_table_type":1,"policy_ref_info":null,"ttl_info":null}"#,
        br#"{"fk_info":null,"partition":null,"Lock":null,"tiflash_replica":null,"temp_table_type":0,"policy_ref_info":{},"ttl_info":null}"#,
        br#"{"fk_info":null,"partition":null,"Lock":null,"tiflash_replica":null,"temp_table_type":0,"policy_ref_info":null,"ttl_info":{}}"#,
        br#"{"fk_info":null,"partition":null,"Lock":null,"tiflash_replica":null,"temp_table_type":0,"policy_ref_info":null,"ttl_info":null,"affinity":{"level":"s"}}"#,
    ] {
        assert!(table_info_must_load(special), "special={:?}", special);
    }
    assert!(table_info_must_load(
        br#"{"fk_info":[],"partition":null,"Lock":null,"tiflash_replica":null,"temp_table_type":0,"policy_ref_info":null,"ttl_info":null,"affinity":{}}"#
    ));

    let ordered = value::serialize_table_info(&TableInfo {
        affinity: Some(Box::new(tidb_model::table::TableAffinityInfo {
            level: "s".to_owned(),
        })),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        std::str::from_utf8(&ordered).unwrap(),
        r#"{"id":0,"name":{"O":"","L":""},"charset":"","collate":"","cols":null,"index_info":null,"constraint_info":null,"fk_info":null,"state":0,"pk_is_handle":false,"is_common_handle":false,"common_handle_version":0,"comment":"","auto_inc_id":0,"auto_id_cache":0,"auto_rand_id":0,"max_col_id":0,"max_idx_id":0,"max_fk_id":0,"max_cst_id":0,"update_timestamp":0,"ShardRowIDBits":0,"max_shard_row_id_bits":0,"auto_random_bits":0,"auto_random_range_bits":0,"pre_split_regions":0,"partition":null,"compression":"","view":null,"sequence":null,"Lock":null,"version":0,"tiflash_replica":null,"is_columnar":false,"temp_table_type":0,"cache_table_status":0,"policy_ref_info":null,"stats_options":null,"exchange_partition_info":null,"ttl_info":null,"affinity":{"level":"s"},"revision":0}"#
    );
    assert!(table_info_must_load(&ordered));

    let source_cases = [
        TableInfo {
            ttl_info: Some(Box::new(tidb_model::table::TTLInfo {
                interval_expr_str: "1".to_owned(),
                interval_time_unit: 3,
                job_interval: "1h".to_owned(),
                ..Default::default()
            })),
            ..Default::default()
        },
        TableInfo {
            affinity: Some(Box::new(tidb_model::table::TableAffinityInfo {
                level: "s".to_owned(),
            })),
            ..Default::default()
        },
        TableInfo {
            tiflash_replica: Some(Box::new(tidb_model::table::TiFlashReplicaInfo {
                count: 1,
                ..Default::default()
            })),
            ..Default::default()
        },
        TableInfo {
            placement_policy_ref: Some(tidb_model::placement::PolicyRefInfo {
                id: 1,
                ..Default::default()
            }),
            ..Default::default()
        },
        TableInfo {
            partition: Some(Box::new(tidb_model::partition::PartitionInfo {
                expr: "a".to_owned(),
                ..Default::default()
            })),
            ..Default::default()
        },
        TableInfo {
            lock: Some(Box::new(tidb_model::table::TableLockInfo {
                tp: tidb_ast::TableLockType::NONE,
                sessions: Vec::new(),
                state: tidb_model::table::TableLockState::PRE_LOCK,
                ts: 0,
            })),
            ..Default::default()
        },
        TableInfo {
            foreign_keys: vec![tidb_model::table::FKInfo {
                id: 1,
                ..Default::default()
            }],
            ..Default::default()
        },
        TableInfo {
            temp_table_type: tidb_model::table::TempTableType::GLOBAL,
            ..Default::default()
        },
    ];
    for source_case in source_cases {
        assert!(table_info_must_load(
            &value::serialize_table_info(&source_case).unwrap()
        ));
    }
    for ordinary in [
        TableInfo::default(),
        TableInfo {
            foreign_keys: Vec::new(),
            ..Default::default()
        },
        TableInfo {
            id: 123,
            ..Default::default()
        },
    ] {
        assert!(!table_info_must_load(
            &value::serialize_table_info(&ordinary).unwrap()
        ));
    }
}

#[test]
fn table_name_regex_and_fast_decoder_cover_go_escape_vectors() {
    let regex = regex::bytes::Regex::new(NAME_EXTRACT_REGEXP).unwrap();
    for name in ["a", "\"a\"", "\"\"a\"", "\"\\\"a\"", "\"\\\"啊\""] {
        let encoded = value::serialize_table_info(&TableInfo {
            name: CiString::new(name),
            ..Default::default()
        })
        .unwrap();
        let captures = regex.captures(&encoded).unwrap();
        assert_eq!(unescape_name_bytes(&captures[1]), name.as_bytes());
        let simple = fast_unmarshal_table_name_info(&encoded).unwrap();
        assert_eq!(simple.name.original(), name);
    }

    for malformed in [
        br#"{"name":{"O":"t","L":"t"}}"#.as_slice(),
        br#"{"id":1.5,"name":{"O":"t","L":"t"}}"#,
        br#"{"id":9223372036854775808,"name":{"O":"t","L":"t"}}"#,
        br#"{"id":1,"name":{"O":"t"}}"#,
        br#"{"id":1,"name":{"O":1,"L":"t"}}"#,
        br#"{"id":1,"name":{"O":"t","L":"t","x":0}}"#,
    ] {
        assert!(fast_unmarshal_table_name_info(malformed).is_err());
    }
}

struct MemoryStore {
    snapshot: MemoryTransaction,
    timestamps: Mutex<Vec<u64>>,
}

impl MetaSnapshotStore for MemoryStore {
    type Snapshot = MemoryTransaction;

    fn snapshot(&self, start_ts: u64) -> Self::Snapshot {
        self.timestamps.lock().unwrap().push(start_ts);
        self.snapshot.clone()
    }
}

struct VersionedMemoryStore {
    versions: BTreeMap<u64, MemoryTransaction>,
}

impl MetaSnapshotStore for VersionedMemoryStore {
    type Snapshot = MemoryTransaction;

    fn snapshot(&self, start_ts: u64) -> Self::Snapshot {
        self.versions[&start_ts].clone()
    }
}

#[test]
fn historical_snapshot_keeps_the_global_id_visible_at_its_start_ts() {
    let live = Mutator::new(MemoryTransaction::default());
    assert_eq!(live.gen_global_id().unwrap(), 1);
    let version_one = live.inspect(Clone::clone).unwrap();
    assert_eq!(live.gen_global_id().unwrap(), 2);
    let version_two = live.inspect(Clone::clone).unwrap();

    let store = VersionedMemoryStore {
        versions: BTreeMap::from([(11, version_one), (22, version_two)]),
    };
    let historical = Mutator::new(store.snapshot(11));
    let current = Mutator::new(store.snapshot(22));
    assert_eq!(historical.global_id().unwrap(), 1);
    assert_eq!(current.global_id().unwrap(), 2);
}

#[test]
fn iter_all_tables_clamps_workers_streams_ranges_and_serializes_callbacks() {
    let meta = Mutator::new(MemoryTransaction::default());
    for (database_id, table_id) in [(1, 11), (6_000_000_000_000_000_000, 22)] {
        meta.create_database(&DBInfo {
            id: database_id,
            name: CiString::new(format!("db{database_id}")),
            ..Default::default()
        })
        .unwrap();
        meta.create_table_or_view(
            database_id,
            &TableInfo {
                id: table_id,
                name: CiString::new(format!("t{table_id}")),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let mut snapshot = meta.inspect(Clone::clone).unwrap();
    // This encoded key is within the DB range but is not hash data. Go's
    // IterateHashWithBoundedKey skips it instead of failing the scan.
    snapshot
        .set(
            structure::encode_string_data_key(b"DB:2"),
            b"not table metadata".to_vec(),
        )
        .unwrap();
    let marks = Arc::new(AtomicUsize::new(0));
    let store = MemoryStore {
        snapshot: snapshot.with_internal_meta_mark_counter(Arc::clone(&marks)),
        timestamps: Mutex::new(Vec::new()),
    };
    let in_callback = AtomicUsize::new(0);
    let max_in_callback = AtomicUsize::new(0);
    let mut seen = Vec::new();
    iter_all_tables(&store, 77, 99, &|| false, |table| {
        let previous = in_callback.fetch_add(1, Ordering::SeqCst);
        max_in_callback.fetch_max(previous + 1, Ordering::SeqCst);
        seen.push((table.db_id, table.id));
        in_callback.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    })
    .unwrap();
    seen.sort_unstable();
    assert_eq!(seen, [(1, 11), (6_000_000_000_000_000_000, 22)]);
    assert_eq!(max_in_callback.load(Ordering::SeqCst), 1);
    assert_eq!(*store.timestamps.lock().unwrap(), vec![77; 15]);
    assert_eq!(marks.load(Ordering::SeqCst), 15);

    let cancelled_store = MemoryStore {
        snapshot: store.snapshot.clone(),
        timestamps: Mutex::new(Vec::new()),
    };
    assert_eq!(
        iter_all_tables(&cancelled_store, 88, 0, &|| true, |_| Ok(())),
        Err(MetaError::Cancelled)
    );
    assert_eq!(*cancelled_store.timestamps.lock().unwrap(), [88]);

    let callback_error_store = MemoryStore {
        snapshot: store.snapshot.clone(),
        timestamps: Mutex::new(Vec::new()),
    };
    assert_eq!(
        iter_all_tables(&callback_error_store, 99, 1, &|| false, |_| {
            Err(MetaError::Storage("callback stopped".to_owned()))
        }),
        Err(MetaError::Storage("callback stopped".to_owned()))
    );

    let range_error_store = MemoryStore {
        snapshot: store
            .snapshot
            .clone()
            .with_iteration_error("snapshot range failed"),
        timestamps: Mutex::new(Vec::new()),
    };
    assert_eq!(
        iter_all_tables(&range_error_store, 100, 1, &|| false, |_| Ok(())),
        Err(MetaError::Storage("snapshot range failed".to_owned()))
    );

    let panic_store = MemoryStore {
        snapshot: store.snapshot.clone(),
        timestamps: Mutex::new(Vec::new()),
    };
    assert_eq!(
        iter_all_tables(&panic_store, 101, 1, &|| false, |_| -> Result<()> {
            panic!("callback panic")
        }),
        Err(MetaError::Storage(
            "panic recovered in IterAllTables worker".to_owned()
        ))
    );

    let mut malformed = store.snapshot.clone();
    malformed
        .set(key::table_kv_key(1, 999), b"{".to_vec())
        .unwrap();
    let malformed_store = MemoryStore {
        snapshot: malformed,
        timestamps: Mutex::new(Vec::new()),
    };
    assert!(matches!(
        iter_all_tables(&malformed_store, 102, 1, &|| false, |_| Ok(())),
        Err(MetaError::InvalidJson(_))
    ));
}

#[test]
fn job_name_filter_keeps_go_operator_precedence() {
    let job = br#"{"schema_name":"s","table_name":"t","ignored":1}"#;
    assert_eq!(
        extract_schema_and_table_name_from_job(job).unwrap(),
        ("s".to_owned(), "t".to_owned())
    );
    let empty = BTreeSet::new();
    assert!(job_matches(b"not json", &empty, &empty).unwrap());

    let schemas = BTreeSet::from(["other".to_owned()]);
    let tables = BTreeSet::from(["t".to_owned()]);
    assert!(job_matches(job, &schemas, &tables).unwrap());

    let schemas = BTreeSet::from(["s".to_owned()]);
    let tables = BTreeSet::from(["other".to_owned()]);
    assert!(!job_matches(job, &schemas, &tables).unwrap());
    assert!(job_matches(job, &schemas, &empty).unwrap());

    for (schema, table) in [
        ("", ""),
        ("schema", "table"),
        ("\"schema\"", "\\table"),
        ("库", "表"),
        ("s,!@#$%^&*()", "t[]{};:'"),
        ("s\\\"混合", "t\nline"),
    ] {
        let encoded = tidb_model::serde_helpers::to_go_json(&serde_json::json!({
            "err": {"schema_name": "wrong", "table_name": "wrong"},
            "query": "schema_name/table_name distractor",
            "schema_name": schema,
            "table_name": table,
            "warning": {"schema_name": "also wrong"}
        }))
        .unwrap();
        assert_eq!(
            extract_schema_and_table_name_from_job(&encoded).unwrap(),
            (schema.to_owned(), table.to_owned())
        );
    }
    for malformed in [
        br#"{"schema_name":"s"}"#.as_slice(),
        br#"{"schema_name":1,"table_name":"t"}"#,
        br#"{"schema_name":"s","table_name":false}"#,
    ] {
        assert!(extract_schema_and_table_name_from_job(malformed).is_err());
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RecordedJob {
    id: i64,
    schema_name: String,
    table_name: String,
    #[serde(skip)]
    update_raw_args_seen: bool,
}

impl DdlJobCodec for RecordedJob {
    fn id(&self) -> i64 {
        self.id
    }

    fn encode(&mut self, update_raw_args: bool) -> Result<Vec<u8>> {
        self.update_raw_args_seen = update_raw_args;
        tidb_model::serde_helpers::to_go_json(self)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))
    }

    fn decode(encoded: &[u8]) -> Result<Self> {
        serde_json::from_slice(encoded).map_err(|error| MetaError::InvalidJson(error.to_string()))
    }
}

fn job(id: i64, schema: &str, table: &str) -> RecordedJob {
    RecordedJob {
        id,
        schema_name: schema.to_owned(),
        table_name: table.to_owned(),
        update_raw_args_seen: false,
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PoisonJob {
    id: i64,
    schema_name: String,
    table_name: String,
    poison: bool,
}

impl DdlJobCodec for PoisonJob {
    fn id(&self) -> i64 {
        self.id
    }

    fn encode(&mut self, _update_raw_args: bool) -> Result<Vec<u8>> {
        tidb_model::serde_helpers::to_go_json(self)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))
    }

    fn decode(encoded: &[u8]) -> Result<Self> {
        let decoded: Self = serde_json::from_slice(encoded)
            .map_err(|error| MetaError::InvalidJson(error.to_string()))?;
        if decoded.poison {
            return Err(MetaError::InvalidJson("poison job".to_owned()));
        }
        Ok(decoded)
    }
}

#[test]
fn ddl_history_is_big_endian_reverse_inclusive_and_filtered_before_decode() {
    let meta = Mutator::new(MemoryTransaction::default());
    let mut one = job(1, "s1", "t1");
    let mut two = job(2, "s2", "t2");
    let mut three = job(3, "s3", "t3");
    meta.add_history_ddl_job(&mut one, true).unwrap();
    meta.add_history_ddl_job(&mut three, false).unwrap();
    meta.add_history_ddl_job(&mut two, false).unwrap();
    assert!(one.update_raw_args_seen);
    assert_eq!(meta.history_ddl_count().unwrap(), 3);
    let job_two_key =
        structure::encode_hash_data_key(key::DDL_JOB_HISTORY, &key::ddl_job_id_key(2));
    assert!(
        meta.inspect(|transaction| transaction.entries().contains_key(&job_two_key))
            .unwrap(),
        "the inclusive start boundary must be the exact stored key"
    );
    assert_eq!(
        meta.ddl_job_history_key(888),
        vec![
            0x6d, 0x44, 0x44, 0x4c, 0x4a, 0x6f, 0x62, 0x48, 0x69, 0xff, 0x73, 0x74, 0x6f, 0x72,
            0x79, 0x00, 0x00, 0x00, 0xfc, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x68, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x78, 0xff, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0xf7,
        ]
    );

    assert_eq!(
        meta.history_ddl_job::<RecordedJob>(2).unwrap().unwrap().id,
        2
    );
    assert!(meta.history_ddl_job::<RecordedJob>(9).unwrap().is_none());

    let mut all = meta.last_history_ddl_jobs::<RecordedJob>().unwrap();
    let mut jobs = Vec::new();
    all.get_last_jobs(2, &mut jobs).unwrap();
    assert_eq!(jobs.iter().map(|job| job.id).collect::<Vec<_>>(), [3, 2]);
    all.get_last_jobs(2, &mut jobs).unwrap();
    assert_eq!(jobs.iter().map(|job| job.id).collect::<Vec<_>>(), [1]);

    let mut from_two = meta.history_ddl_jobs::<RecordedJob>(2).unwrap();
    from_two.get_last_jobs(9, &mut jobs).unwrap();
    assert_eq!(jobs.iter().map(|job| job.id).collect::<Vec<_>>(), [2, 1]);

    let mut filtered = meta
        .last_history_ddl_jobs_with_filter::<RecordedJob>(
            BTreeSet::from(["does-not-match".to_owned()]),
            BTreeSet::from(["t2".to_owned()]),
        )
        .unwrap();
    filtered.get_last_jobs(3, &mut jobs).unwrap();
    assert_eq!(jobs.iter().map(|job| job.id).collect::<Vec<_>>(), [2]);
}

#[test]
fn ddl_history_count_buffer_filter_and_decode_error_edges_match_go() {
    let meta = Mutator::new(MemoryTransaction::default());
    for mut job in [
        PoisonJob {
            id: 1,
            schema_name: "s".to_owned(),
            table_name: "ok".to_owned(),
            poison: false,
        },
        PoisonJob {
            id: 2,
            schema_name: "s".to_owned(),
            table_name: "poison".to_owned(),
            poison: true,
        },
    ] {
        meta.add_history_ddl_job(&mut job, false).unwrap();
    }

    let mut iterator = meta.last_history_ddl_jobs::<PoisonJob>().unwrap();
    let mut jobs = vec![PoisonJob {
        id: 99,
        schema_name: String::new(),
        table_name: String::new(),
        poison: false,
    }];
    iterator.get_last_jobs(0, &mut jobs).unwrap();
    assert!(jobs.is_empty());
    jobs.push(PoisonJob {
        id: 98,
        schema_name: String::new(),
        table_name: String::new(),
        poison: false,
    });
    iterator.get_last_jobs(-1, &mut jobs).unwrap();
    assert!(jobs.is_empty());

    assert_eq!(
        iterator.get_last_jobs(2, &mut jobs),
        Err(MetaError::InvalidJson("poison job".to_owned()))
    );
    assert!(jobs.is_empty(), "Go returns a nil result slice on error");
    assert_eq!(
        iterator.get_last_jobs(2, &mut jobs),
        Err(MetaError::InvalidJson("poison job".to_owned())),
        "Go does not advance after a decode error"
    );

    let mut filtered = meta
        .last_history_ddl_jobs_with_filter::<PoisonJob>(
            BTreeSet::new(),
            BTreeSet::from(["ok".to_owned()]),
        )
        .unwrap();
    filtered.get_last_jobs(2, &mut jobs).unwrap();
    assert_eq!(jobs.iter().map(|job| job.id).collect::<Vec<_>>(), [1]);
}

#[test]
fn ddl_history_preserves_constructor_and_both_next_error_boundaries() {
    let source = Mutator::new(MemoryTransaction::default());
    for mut job in [job(1, "s1", "t1"), job(2, "s2", "t2")] {
        source.add_history_ddl_job(&mut job, false).unwrap();
    }

    let constructor_error = Mutator::new(
        source
            .inspect(Clone::clone)
            .unwrap()
            .with_iteration_error("new reverse iterator"),
    );
    assert!(matches!(
        constructor_error.last_history_ddl_jobs::<RecordedJob>(),
        Err(MetaError::Storage(message)) if message == "new reverse iterator"
    ));

    let matching_next_error = Mutator::new(
        source
            .inspect(Clone::clone)
            .unwrap()
            .with_reverse_next_error(0, "next matching job"),
    );
    let mut iterator = matching_next_error
        .last_history_ddl_jobs::<RecordedJob>()
        .unwrap();
    let mut jobs = vec![job(99, "old", "old")];
    assert_eq!(
        iterator.get_last_jobs(2, &mut jobs),
        Err(MetaError::Storage("next matching job".to_owned()))
    );
    assert!(jobs.is_empty(), "Go returns nil instead of partial jobs");
    assert_eq!(
        iterator.get_last_jobs(2, &mut jobs),
        Err(MetaError::Storage("next matching job".to_owned())),
        "a failed Next leaves the cursor on the same entry"
    );

    let filtered_next_error = Mutator::new(
        source
            .inspect(Clone::clone)
            .unwrap()
            .with_reverse_next_error(0, "next filtered job"),
    );
    let mut iterator = filtered_next_error
        .last_history_ddl_jobs_with_filter::<RecordedJob>(
            BTreeSet::from(["does-not-match".to_owned()]),
            BTreeSet::new(),
        )
        .unwrap();
    jobs.push(job(98, "old", "old"));
    assert_eq!(
        iterator.get_last_jobs(2, &mut jobs),
        Err(MetaError::Storage("next filtered job".to_owned()))
    );
    assert!(jobs.is_empty());
}

#[test]
fn dxf_and_ru_stats_match_go_json_shapes_including_null() {
    let meta = Mutator::new(MemoryTransaction::default());
    let factors = TtlTuneFactors::default();
    meta.set_dxf_schedule_tune_factors("ks", &factors).unwrap();
    let dxf_key = structure::encode_hash_data_key(key::DXF_SCHEDULE_TUNE, b"ks");
    let dxf = meta
        .inspect(|transaction| transaction.entries()[&dxf_key].clone())
        .unwrap();
    assert_eq!(dxf, br#"{"expire_time":"0001-01-01T00:00:00Z"}"#);
    assert_eq!(meta.dxf_schedule_tune_factors("ks").unwrap(), Some(factors));
    assert_eq!(meta.dxf_schedule_tune_factors("missing").unwrap(), None);

    let factors = TtlTuneFactors {
        ttl_nanoseconds: 3_600_000_000_000,
        amplify_factor: 1.5,
        ..Default::default()
    };
    meta.set_dxf_schedule_tune_factors("ks2", &factors).unwrap();
    let dxf_key = structure::encode_hash_data_key(key::DXF_SCHEDULE_TUNE, b"ks2");
    assert_eq!(
        meta.inspect(|transaction| transaction.entries()[&dxf_key].clone())
            .unwrap(),
        br#"{"ttl":3600000000000,"expire_time":"0001-01-01T00:00:00Z","amplify_factor":1.5}"#
    );
    assert_eq!(
        meta.dxf_schedule_tune_factors("ks2").unwrap(),
        Some(factors)
    );

    meta.set_ru_stats(None).unwrap();
    assert_eq!(meta.ru_stats().unwrap(), None);
    let ru_key = structure::encode_string_data_key(key::REQUEST_UNIT_STATS);
    assert_eq!(
        meta.inspect(|transaction| transaction.entries()[&ru_key].clone())
            .unwrap(),
        b"null"
    );

    let stats = RuStats {
        latest: Some(Box::new(DailyRuStats {
            end_time: Utc.with_ymd_and_hms(2026, 8, 7, 1, 2, 3).unwrap(),
            stats: Some(vec![GroupRuStats {
                id: 7,
                name: "<rg>".to_owned(),
                ru_consumption: Some(RuConsumption {
                    read_request_units: 1.25,
                    ..Default::default()
                }),
            }]),
        })),
        previous: None,
    };
    meta.set_ru_stats(Some(&stats)).unwrap();
    assert_eq!(meta.ru_stats().unwrap(), Some(stats));
    let stored = meta
        .inspect(|transaction| transaction.entries()[&ru_key].clone())
        .unwrap();
    assert!(String::from_utf8(stored)
        .unwrap()
        .contains("\\u003crg\\u003e"));

    let mut corrupt_dxf = meta.inspect(Clone::clone).unwrap();
    corrupt_dxf
        .set(
            structure::encode_hash_data_key(key::DXF_SCHEDULE_TUNE, b"bad"),
            b"{".to_vec(),
        )
        .unwrap();
    assert!(matches!(
        Mutator::new(corrupt_dxf).dxf_schedule_tune_factors("bad"),
        Err(MetaError::InvalidJson(_))
    ));

    let mut corrupt_ru = meta.inspect(Clone::clone).unwrap();
    corrupt_ru
        .set(
            structure::encode_string_data_key(key::REQUEST_UNIT_STATS),
            b"{".to_vec(),
        )
        .unwrap();
    assert!(matches!(
        Mutator::new(corrupt_ru).ru_stats(),
        Err(MetaError::InvalidJson(_))
    ));
}

#[derive(Default)]
struct RecordedMvcc {
    expected_key: Vec<u8>,
    requested_timestamp: Option<u64>,
    result: Option<MvccInfo>,
    error: Option<MetaError>,
}

impl MvccReader for RecordedMvcc {
    fn mvcc_by_encoded_key(&mut self, key: &[u8], timestamp: u64) -> Result<Option<MvccInfo>> {
        assert_eq!(key, self.expected_key);
        self.requested_timestamp = Some(timestamp);
        if let Some(error) = self.error.clone() {
            return Err(error);
        }
        Ok(self.result.clone())
    }
}

#[test]
fn oldest_schema_version_reads_the_last_mvcc_short_value_at_max_ts() {
    let mut reader = RecordedMvcc {
        expected_key: key::schema_version_kv_key(),
        result: Some(MvccInfo {
            writes: vec![
                MvccWrite {
                    short_value: b"9".to_vec(),
                },
                MvccWrite {
                    short_value: b"10".to_vec(),
                },
            ],
        }),
        ..Default::default()
    };
    assert_eq!(oldest_schema_version(&mut reader).unwrap(), 10);
    assert_eq!(reader.requested_timestamp, Some(u64::MAX));

    reader.result = Some(MvccInfo::default());
    assert_eq!(
        oldest_schema_version(&mut reader),
        Err(MetaError::NoSchemaVersionWrite)
    );
    reader.result = None;
    assert_eq!(
        oldest_schema_version(&mut reader),
        Err(MetaError::NoSchemaVersionWrite)
    );

    reader.result = Some(MvccInfo {
        writes: vec![MvccWrite {
            short_value: b"not-an-int".to_vec(),
        }],
    });
    assert_eq!(
        oldest_schema_version(&mut reader),
        Err(MetaError::InvalidIntValue)
    );

    reader.error = Some(MetaError::Storage("mvcc unavailable".to_owned()));
    assert_eq!(
        oldest_schema_version(&mut reader),
        Err(MetaError::Storage("mvcc unavailable".to_owned()))
    );
}
