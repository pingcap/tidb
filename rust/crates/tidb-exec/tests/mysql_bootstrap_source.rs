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

//! A bootstrap is only real if this node's OWN readers can read it back.
//!
//! Every assertion here goes through the code paths that serve a live cluster —
//! [`tidb_exec::cluster_catalog::load_cluster_catalog`],
//! [`tidb_exec::cluster_privilege_load::read_bootstrap_state`], and
//! [`tidb_exec::cluster_privilege_load::load_cluster_privileges`] — rather than
//! inspecting the mutation list. Writing bytes only this test understands would
//! prove nothing.

use std::collections::BTreeMap;

use tidb_exec::cluster_catalog::{load_cluster_catalog, ClusterCatalogError, MetaPairs, MetaSnapshot};
use tidb_exec::cluster_privilege_load::{
    load_cluster_privileges, read_bootstrap_state, ClusterBootstrapState,
};
use tidb_exec::mysql_bootstrap::{plan_mysql_bootstrap, BootstrapError, BootstrapWrite};
use tidb_meta::key;
use tidb_metadef::BOOTSTRAP_TABLES;
use tidb_txnkv::transaction::OptimisticMutationKind;

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

/// Applies one planned bootstrap, the way a committed transaction would.
fn apply(store: &mut MetaStore, write: &BootstrapWrite) {
    for mutation in &write.mutations {
        match mutation.kind() {
            OptimisticMutationKind::MetaDelete
            | OptimisticMutationKind::Delete
            | OptimisticMutationKind::IndexDelete => {
                store.pairs.remove(mutation.key());
            }
            _ => {
                store
                    .pairs
                    .insert(mutation.key().to_vec(), mutation.value().to_vec());
            }
        }
    }
}

fn bootstrapped() -> MetaStore {
    let mut store = MetaStore::default();
    let write = plan_mysql_bootstrap(&mut store, 467_996_279_696_261_139).expect("a fresh keyspace bootstraps");
    apply(&mut store, &write);
    store
}

#[test]
fn a_bootstrapped_keyspace_loads_back_as_a_catalog_with_every_mysql_table() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let database = catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "mysql")
        .expect("the bootstrap created the mysql database");
    assert_eq!(database.tables.len(), BOOTSTRAP_TABLES.len());
    for table in BOOTSTRAP_TABLES {
        assert!(
            database
                .tables
                .iter()
                .any(|stored| stored.name.lowercase() == table.name && stored.id == table.id),
            "mysql.{} is missing from the loaded catalog",
            table.name
        );
    }
}

#[test]
fn the_bootstrap_marker_is_the_one_the_detector_reads() {
    // This is the whole point of writing `mysql.tidb`: the detector that
    // decides whether to bootstrap at all must see it as bootstrapped.
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the catalog loads");
    let state = read_bootstrap_state(&mut store, &catalog).expect("the marker reads back");
    assert!(state.already_bootstrapped(), "{state}");
    assert!(matches!(
        state,
        ClusterBootstrapState::Bootstrapped { version: Some(_) }
    ));
}

#[test]
fn the_seeded_root_account_loads_back_as_an_unlocked_superuser() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the catalog loads");
    let privileges = load_cluster_privileges(&mut store, &catalog).expect("the seed row reads back");
    let [root] = privileges.users.as_slice() else {
        panic!("a bootstrap seeds exactly one account, got {:?}", privileges.users.len());
    };
    assert_eq!(root.host, "%");
    assert_eq!(root.user, "root");
    // An EMPTY password, not a missing column: Go's own bootstrap stores the
    // empty string, and that is what makes a fresh cluster reachable.
    assert_eq!(root.authentication_string, "");
    assert_eq!(root.plugin, "mysql_native_password");
    assert!(!root.account_locked);
}

#[test]
fn a_second_bootstrap_is_refused_rather_than_layered_on_top() {
    let mut store = bootstrapped();
    let error = plan_mysql_bootstrap(&mut store, 1).expect_err("a bootstrapped keyspace is refused");
    assert!(
        matches!(error, BootstrapError::AlreadyPresent { .. }),
        "{error}"
    );
    // A keyspace where only the DATABASE survived — a bootstrap that died
    // halfway — must be refused too, or the retry writes a second copy.
    let mut half = MetaStore::default();
    half.pairs.insert(
        key::table_kv_key(
            tidb_metadef::system::SYSTEM_DATABASE_ID,
            BOOTSTRAP_TABLES[0].id,
        ),
        b"{}".to_vec(),
    );
    let error = plan_mysql_bootstrap(&mut half, 1).expect_err("a half-bootstrapped keyspace is refused");
    assert!(
        matches!(error, BootstrapError::AlreadyPresent { .. }),
        "{error}"
    );
}

#[test]
fn a_bootstrap_spends_exactly_one_schema_version_and_describes_it() {
    let mut store = MetaStore::default();
    store
        .pairs
        .insert(key::schema_version_kv_key(), b"60".to_vec());
    let write = plan_mysql_bootstrap(&mut store, 7).expect("the keyspace bootstraps");
    assert_eq!(write.schema_version, 61);
    assert_eq!(write.diff.version, 61);
    assert_eq!(
        write.diff.action_type,
        tidb_model::action_type::ActionType::ACTION_CREATE_TABLES
    );
    // Every created table is named in the diff, which is what makes a real
    // TiDB's own reloader pick them all up at this one version.
    assert_eq!(write.diff.affected_options.len(), BOOTSTRAP_TABLES.len());
    apply(&mut store, &write);
    assert_eq!(store.pairs[&key::schema_version_kv_key()], b"61");
}

#[test]
fn every_seeded_row_carries_the_index_entries_its_table_declares() {
    // `mysql.user` and `mysql.tidb` are both NON-clustered, so a row with no
    // index entry is a row half of TiDB cannot find. Nothing backfills them
    // later, so the bootstrap that writes the row must write them.
    let write = plan_mysql_bootstrap(&mut MetaStore::default(), 7).expect("the keyspace bootstraps");
    let index_entries = write
        .mutations
        .iter()
        .filter(|mutation| mutation.kind() == OptimisticMutationKind::IndexPut)
        .count();
    let records = write
        .mutations
        .iter()
        .filter(|mutation| mutation.kind() == OptimisticMutationKind::Insert)
        .count();
    // One `mysql.user` row (PRIMARY + i_user) and two `mysql.tidb` rows
    // (PRIMARY each).
    assert_eq!(records, 3);
    assert_eq!(index_entries, 4);
}
