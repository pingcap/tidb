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

use tidb_cluster::cluster_catalog::{ClusterCatalogError, MetaPairs, MetaSnapshot};

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
