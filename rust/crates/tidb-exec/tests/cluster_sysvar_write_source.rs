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

//! A `SET GLOBAL` write is only real if this node's OWN loader
//! ([`cluster_sysvar_load`]) reads it back -- the same bar
//! `cluster_account_write_source.rs` holds the account writer to.

use std::collections::BTreeMap;

use tidb_datatype::{Time, TimeType};
use tidb_exec::cluster_catalog::{
    load_cluster_catalog, ClusterCatalog, ClusterCatalogError, MetaPairs, MetaSnapshot,
};
use tidb_exec::cluster_sysvar_load::load_cluster_sysvars;
use tidb_exec::cluster_sysvar_write::plan_sysvar_write;
use tidb_exec::mysql_bootstrap::{plan_mysql_bootstrap, BootstrapEnvironment};
use tidb_txnkv::transaction::{OptimisticMutation, OptimisticMutationKind};

fn timestamp() -> Time {
    Time::from_date_checked(2026, 7, 29, 6, 12, 55, 0, TimeType::Timestamp, 0)
        .expect("a fixed calendar date is a valid timestamp")
}

fn environment() -> BootstrapEnvironment {
    BootstrapEnvironment {
        system_tz: "Asia/Shanghai".to_owned(),
        new_collation_enabled: true,
        cluster_id: 7_667_705_271_188_879_689,
        current_timestamp: timestamp(),
        ddl_table_version: 0,
    }
}

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
                    assert!(!present, "an Insert would have overwritten a stored row");
                    self.pairs
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
                OptimisticMutationKind::PutExisting => {
                    assert!(present, "a PutExisting named a row that is not stored");
                    self.pairs
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
                OptimisticMutationKind::Delete => {
                    assert!(present, "a Delete named a row that is not stored");
                    self.pairs.remove(mutation.key());
                }
                OptimisticMutationKind::IndexDelete | OptimisticMutationKind::MetaDelete => {
                    self.pairs.remove(mutation.key());
                }
                OptimisticMutationKind::IndexPut | OptimisticMutationKind::MetaPut => {
                    self.pairs
                        .insert(mutation.key().to_vec(), mutation.value().to_vec());
                }
            }
        }
    }

    fn catalog(&mut self) -> ClusterCatalog {
        load_cluster_catalog(self).expect("the bootstrapped catalog loads")
    }

    fn sysvars(&mut self) -> Vec<(String, String)> {
        let catalog = self.catalog();
        load_cluster_sysvars(self, &catalog).expect("the sysvars load")
    }

    /// Reads the cluster's current image, applies `mutate` (a `SET GLOBAL`'s
    /// own effect on a scratch table seeded from that same read -- exactly
    /// what [`crate::cluster_sysvar_seam::RealPendingSysvarChange::commit`]
    /// does with a real [`tidb_session::vars::GlobalSysvars`] scratch copy),
    /// plans and applies the resulting diff, and answers what the loader
    /// reads back afterwards.
    fn write(&mut self, mutate: impl FnOnce(&mut BTreeMap<String, String>)) -> Vec<(String, String)> {
        let catalog = self.catalog();
        let mut desired: BTreeMap<String, String> = self.sysvars().into_iter().collect();
        mutate(&mut desired);
        let plan = plan_sysvar_write(self, &catalog, &desired, timestamp())
            .expect("the sysvar image is writable");
        self.apply(&plan.mutations);
        self.sysvars()
    }
}

fn bootstrapped() -> MetaStore {
    let mut store = MetaStore::default();
    let write = plan_mysql_bootstrap(&mut store, 467_996_279_696_261_139, &environment())
        .expect("a fresh keyspace bootstraps");
    let mutations = write.mutations;
    store.apply(&mutations);
    store
}

/// `mysql_bootstrap` seeds one row per registry variable at its build
/// default (Go's own bootstrap writes `mysql.global_variables` the same
/// way), so the fixture never starts from an empty table; every assertion
/// here looks up the one row a statement actually touched rather than
/// comparing the whole table.
fn value_of<'a>(rows: &'a [(String, String)], name: &str) -> Option<&'a str> {
    rows.iter()
        .find(|(row_name, _)| row_name == name)
        .map(|(_, value)| value.as_str())
}

#[test]
fn a_set_global_of_a_variable_is_a_row_this_nodes_own_loader_reads_back() {
    let mut store = bootstrapped();
    let read_back = store.write(|desired| {
        desired.insert("max_connections".to_owned(), "500".to_owned());
    });
    assert_eq!(value_of(&read_back, "max_connections"), Some("500"));
}

#[test]
fn a_second_set_global_of_the_same_variable_updates_the_row_in_place() {
    let mut store = bootstrapped();
    store.write(|desired| {
        desired.insert("max_connections".to_owned(), "500".to_owned());
    });
    let read_back = store.write(|desired| {
        desired.insert("max_connections".to_owned(), "900".to_owned());
    });
    assert_eq!(value_of(&read_back, "max_connections"), Some("900"));
    // Only one row exists for the variable: an update in place, not a second
    // insert alongside the first.
    assert_eq!(
        read_back
            .iter()
            .filter(|(name, _)| name == "max_connections")
            .count(),
        1
    );
}

#[test]
fn set_global_default_deletes_the_stored_row() {
    let mut store = bootstrapped();
    store.write(|desired| {
        desired.insert("max_connections".to_owned(), "500".to_owned());
    });
    let read_back = store.write(|desired| {
        desired.remove("max_connections");
    });
    assert_eq!(
        value_of(&read_back, "max_connections"),
        None,
        "resetting to DEFAULT should remove the stored row, not zero its value"
    );
}

#[test]
fn an_untouched_variable_survives_someone_elses_set_global() {
    // The whole-image diff must never revert a variable this statement did
    // not name, which is the property that makes it safe against a
    // concurrent `SET GLOBAL` on a different variable.
    let mut store = bootstrapped();
    store.write(|desired| {
        desired.insert("max_connections".to_owned(), "500".to_owned());
    });
    let read_back = store.write(|desired| {
        desired.insert("max_allowed_packet".to_owned(), "67108864".to_owned());
    });
    assert_eq!(value_of(&read_back, "max_connections"), Some("500"));
    assert_eq!(value_of(&read_back, "max_allowed_packet"), Some("67108864"));
}

#[test]
fn a_no_op_write_plans_nothing() {
    let mut store = bootstrapped();
    let catalog = store.catalog();
    let desired: BTreeMap<String, String> = store.sysvars().into_iter().collect();
    let plan = plan_sysvar_write(&mut store, &catalog, &desired, timestamp())
        .expect("a matching image plans cleanly");
    assert!(
        plan.is_empty(),
        "an image identical to what is already stored should plan no mutations"
    );
    assert!(plan.changed.is_empty());
}
