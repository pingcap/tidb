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

//! The cluster adapter for Go `pkg/statistics/handle/lockstats`: one table
//! target persists its logical and partition rows, duplicate operations warn,
//! and one unlock retracts the same stored image.

use std::collections::BTreeMap;

use tidb_ast::CiString;
use tidb_datatype::{Time, TimeType};
use tidb_exec::cluster_catalog::{
    load_cluster_catalog, ClusterCatalogError, LoadedDatabase, MetaPairs, MetaSnapshot,
};
use tidb_exec::cluster_stats_lock::{
    plan_cluster_stats_lock, ClusterStatsLockStatement, StatsLockTarget,
};
use tidb_exec::cluster_stats_load::ClusterStatsLoader;
use tidb_exec::cluster_stats_write::{
    plan_loaded_stats_meta_write, plan_stats_delta_statement, stats_delta_statements,
};
use tidb_exec::mysql_bootstrap::{plan_mysql_bootstrap, BootstrapEnvironment};
use tidb_model::db::DBInfo;
use tidb_model::go_runtime::GoShared;
use tidb_model::partition::{PartitionDefinition, PartitionInfo};
use tidb_model::table_info::TableInfo;
use tidb_stats_handle_usage::{DeltaUpdate, TableDelta};
use tidb_txnkv::transaction::{OptimisticMutation, OptimisticMutationKind};

#[derive(Default)]
struct MetaStore {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MetaSnapshot for MetaStore {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(raw_key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        Ok(self
            .pairs
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

impl MetaStore {
    fn apply(&mut self, mutations: &[OptimisticMutation]) {
        for mutation in mutations {
            let present = self.pairs.contains_key(mutation.key());
            match mutation.kind() {
                OptimisticMutationKind::Insert => {
                    assert!(!present, "insert overwrote a persisted lock row");
                    self.pairs
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
                OptimisticMutationKind::PutExisting => {
                    assert!(present, "update named a missing persisted row");
                    self.pairs
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
                OptimisticMutationKind::Delete => {
                    assert!(present, "delete named a missing persisted lock row");
                    self.pairs.remove(mutation.key());
                }
                OptimisticMutationKind::IndexDelete
                | OptimisticMutationKind::MetaDelete
                | OptimisticMutationKind::SystemRowDelete => {
                    self.pairs.remove(mutation.key());
                }
                OptimisticMutationKind::IndexPut
                | OptimisticMutationKind::UniqueIndexInsert
                | OptimisticMutationKind::MetaPut
                | OptimisticMutationKind::SystemRowPut => {
                    self.pairs
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
                OptimisticMutationKind::LockOnly => {}
            }
        }
    }
}

fn timestamp() -> Time {
    Time::from_date_checked(2026, 8, 29, 12, 0, 0, 0, TimeType::Timestamp, 0)
        .expect("fixed timestamp")
}

fn bootstrapped() -> MetaStore {
    let mut store = MetaStore::default();
    let write = plan_mysql_bootstrap(
        &mut store,
        467_996_279_696_261_139,
        &BootstrapEnvironment {
            system_tz: "UTC".to_owned(),
            new_collation_enabled: true,
            cluster_id: 7,
            current_timestamp: timestamp(),
            ddl_table_version: 0,
        },
    )
    .expect("fresh keyspace bootstrap");
    store.apply(&write.mutations);
    store
}

fn add_partitioned_table(catalog: &mut tidb_exec::cluster_catalog::ClusterCatalog) {
    catalog.databases.push(LoadedDatabase {
        info: DBInfo {
            id: 900,
            name: CiString::new("app"),
            ..DBInfo::default()
        },
        tables: vec![TableInfo {
            id: 4242,
            name: CiString::new("t"),
            partition: Some(GoShared::new(PartitionInfo {
                enable: true,
                definitions: vec![
                    PartitionDefinition {
                        id: 4243,
                        name: CiString::new("p0"),
                        ..PartitionDefinition::default()
                    },
                    PartitionDefinition {
                        id: 4244,
                        name: CiString::new("p1"),
                        ..PartitionDefinition::default()
                    },
                ]
                .into(),
                num: 2,
                ..PartitionInfo::default()
            })),
            ..TableInfo::default()
        }],
    });
}

fn statement(lock: bool) -> ClusterStatsLockStatement {
    ClusterStatsLockStatement {
        lock,
        targets: vec![StatsLockTarget {
            schema: "app".to_owned(),
            table: "t".to_owned(),
            partitions: Vec::new(),
        }],
    }
}

#[test]
fn table_lock_round_trips_through_the_persisted_cluster_rows() {
    let mut store = bootstrapped();
    let mut catalog = load_cluster_catalog(&mut store).expect("bootstrap catalog");
    add_partitioned_table(&mut catalog);

    let first = plan_cluster_stats_lock(&mut store, &catalog, &statement(true), 100, timestamp())
        .expect("first lock plans");
    assert!(first.warning.is_empty());
    assert_eq!(
        first.mutations.len(),
        3,
        "logical table plus two partitions"
    );
    store.apply(&first.mutations);

    let duplicate =
        plan_cluster_stats_lock(&mut store, &catalog, &statement(true), 101, timestamp())
            .expect("duplicate lock plans");
    assert!(duplicate.mutations.is_empty());
    assert_eq!(duplicate.warning, "skip locking locked table: app.t");

    let unlock = plan_cluster_stats_lock(&mut store, &catalog, &statement(false), 102, timestamp())
        .expect("unlock plans");
    assert!(unlock.warning.is_empty());
    assert_eq!(
        unlock.mutations.len(),
        3,
        "the same three lock rows are removed"
    );
    store.apply(&unlock.mutations);

    let duplicate =
        plan_cluster_stats_lock(&mut store, &catalog, &statement(false), 103, timestamp())
            .expect("duplicate unlock plans");
    assert!(duplicate.mutations.is_empty());
    assert_eq!(duplicate.warning, "skip unlocking unlocked table: app.t");
}

#[test]
fn table_unlock_merges_negative_partition_delta_into_partition_and_global_meta() {
    let mut store = bootstrapped();
    let mut catalog = load_cluster_catalog(&mut store).expect("bootstrap catalog");
    add_partitioned_table(&mut catalog);

    for table_id in [4242, 4243, 4244] {
        let initial = plan_loaded_stats_meta_write(
            &mut store,
            &catalog,
            table_id,
            2,
            0,
            90,
            timestamp(),
        )
        .expect("initial stats meta plans");
        store.apply(&initial.mutations);
    }

    let lock = plan_cluster_stats_lock(&mut store, &catalog, &statement(true), 100, timestamp())
        .expect("whole table lock plans");
    store.apply(&lock.mutations);
    for statement in stats_delta_statements(&[DeltaUpdate {
        table_id: 4243,
        delta: TableDelta {
            delta: -2,
            count: 2,
            init_time: None,
        },
        is_locked: true,
    }]) {
        let plan = plan_stats_delta_statement(
            &mut store,
            &catalog,
            &statement,
            101,
            timestamp(),
        )
        .expect("locked negative delta plans");
        store.apply(&plan.mutations);
    }

    let unlock = plan_cluster_stats_lock(&mut store, &catalog, &statement(false), 102, timestamp())
        .expect("whole table unlock plans");
    store.apply(&unlock.mutations);
    assert!(!unlock.mutations.is_empty());

    let loader = ClusterStatsLoader::locate(&catalog).expect("statistics tables locate");
    assert_eq!(
        loader.load_meta(&mut store, 4242).expect("global meta loads"),
        Some((102, 0, 2, 0, 90))
    );
    assert_eq!(
        loader.load_meta(&mut store, 4243).expect("partition meta loads"),
        Some((102, 0, 2, 0, 90))
    );
    assert_eq!(
        loader.load_meta(&mut store, 4244).expect("sibling meta loads"),
        Some((102, 0, 0, 2, 90))
    );

    let duplicate =
        plan_cluster_stats_lock(&mut store, &catalog, &statement(false), 103, timestamp())
            .expect("all lock rows were deleted");
    assert!(duplicate.mutations.is_empty());
    assert_eq!(duplicate.warning, "skip unlocking unlocked table: app.t");
}
