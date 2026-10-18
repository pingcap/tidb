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

//! An in-memory meta key-value store for the cluster DDL tier.
//!
//! Go's unit tests run every session on a mock store (`testkit.CreateMockStore`)
//! that is bootstrapped and carries a domain, so DDL has one implementation.
//! This module is the first piece of giving a bare Rust session the same shape
//! (ExecPlan milestone 3): a [`MetaSnapshot`] over a plain ordered map that a
//! planned change's mutations can be applied to, after which the catalog is
//! loaded back from it exactly as a cluster node loads it from TiKV.

use std::collections::BTreeMap;

use tidb_txnkv::transaction::{BufferMutation, BufferMutationOp};

use crate::cluster_catalog::{ClusterCatalogError, MetaPairs, MetaSnapshot};

/// An ordered in-memory image of the meta keys.
#[derive(Clone, Debug, Default)]
pub struct InMemoryMeta {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl InMemoryMeta {
    /// An empty store: a keyspace no TiDB ever touched.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one planned transaction's mutations, in order: a set stores its
    /// value, a delete removes the key, a lock changes nothing.
    pub fn apply(&mut self, mutations: Vec<BufferMutation>) {
        for mutation in mutations {
            let (op, key, value, _, _) = mutation.into_parts();
            match op {
                BufferMutationOp::Set => {
                    self.pairs.insert(key, value);
                }
                BufferMutationOp::Delete => {
                    self.pairs.remove(&key);
                }
                BufferMutationOp::Lock => {}
            }
        }
    }

    /// How many keys the store holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    /// Whether the store holds no key.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }
}

impl MetaSnapshot for InMemoryMeta {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        Ok(self
            .pairs
            .range(prefix.to_vec()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use tidb_datatype::{Time, TimeType};

    use super::*;
    use crate::cluster_catalog::load_cluster_catalog;
    use crate::cluster_ddl::{plan_ddl, DdlPlan, DdlStatement};
    use crate::mysql_bootstrap::{plan_mysql_bootstrap, BootstrapEnvironment};

    fn bootstrapped() -> InMemoryMeta {
        let mut meta = InMemoryMeta::new();
        let environment = BootstrapEnvironment {
            system_tz: "UTC".to_owned(),
            new_collation_enabled: true,
            cluster_id: 1,
            current_timestamp: Time::from_date_checked(
                2026,
                10,
                17,
                0,
                0,
                0,
                0,
                TimeType::Timestamp,
                0,
            )
            .unwrap(),
            ddl_table_version: 0,
        };
        let write = plan_mysql_bootstrap(&mut meta, 1, &environment).unwrap();
        meta.apply(write.mutations);
        meta
    }

    /// A DDL planned by the production planner over the in-memory store, applied
    /// to it and loaded back: the cycle a store-backed bare session would run.
    #[test]
    fn a_planned_create_database_applies_to_the_store_and_loads_back() {
        let mut meta = bootstrapped();
        let before = load_cluster_catalog(&mut meta).unwrap();
        assert!(before
            .databases
            .iter()
            .any(|db| db.info.name.original() == "test"));
        assert!(before
            .databases
            .iter()
            .all(|db| db.info.name.original() != "spike"));
        let statement = DdlStatement::CreateDatabase {
            name: "spike".to_owned(),
            if_not_exists: false,
            charset: "utf8mb4".to_owned(),
            collate: "utf8mb4_bin".to_owned(),
            placement_policy: None,
        };
        let DdlPlan::Write(write) = plan_ddl(&mut meta, &statement, 2).unwrap() else {
            panic!("CREATE DATABASE of a new name must produce a write");
        };
        let version = write.schema_version;
        meta.apply(write.mutations);
        let after = load_cluster_catalog(&mut meta).unwrap();
        assert_eq!(after.schema_version, version);
        assert!(after
            .databases
            .iter()
            .any(|db| db.info.name.original() == "spike"));
        // The same statement again is now a duplicate, from the store's state alone.
        assert!(plan_ddl(&mut meta, &statement, 3).is_err());
    }
}
