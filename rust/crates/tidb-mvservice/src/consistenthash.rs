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

//! Go master `pkg/mvservice/consistenthash.go`.

use std::collections::HashMap;

use crate::server_maintainer::ServerInfo;

#[derive(Clone, Debug)]
pub(crate) struct VirtualNode {
    pub(crate) hash: u32,
    pub(crate) node: String,
}

/// Go `ConsistentHash`: a consistent hashing ring. Not thread-safe; callers
/// synchronize.
pub struct ConsistentHash {
    pub(crate) ring: Vec<VirtualNode>,
    data: HashMap<String, usize>,
    hash_func: Box<dyn Fn(&[u8]) -> u32 + Send + Sync>,
    replicas: usize,
}

impl ConsistentHash {
    /// Go `NewConsistentHash`: `replicas` virtual nodes per real node,
    /// clamped to [1, 200].
    #[must_use]
    pub fn new(replicas: i64) -> Self {
        let replicas = replicas.clamp(1, 200) as usize;
        Self { ring: Vec::new(), data: HashMap::new(), hash_func: Box::new(tidb_murmur3::sum32), replicas }
    }

    /// Go `Rebuild`: rebuilds the ring with the given nodes.
    pub fn rebuild(&mut self, nodes: &HashMap<String, ServerInfo>) {
        self.ring = Vec::with_capacity(nodes.len() * self.replicas);
        self.data = HashMap::with_capacity(nodes.len());
        for node in nodes.keys() {
            self.do_insert(node);
        }
        self.do_resort();
    }

    fn do_resort(&mut self) {
        // Keep the ring sorted for binary search.
        self.ring.sort_by(|a, b| a.hash.cmp(&b.hash).then_with(|| a.node.cmp(&b.node)));
    }

    fn do_insert(&mut self, node: &str) -> bool {
        if node.is_empty() || self.data.contains_key(node) {
            return false;
        }
        for i in 0..self.replicas {
            // Create a virtual node identifier, e.g. "node1#0", "node1#1".
            let virtual_key = format!("{node}#{i}");
            let hash = (self.hash_func)(virtual_key.as_bytes());
            self.ring.push(VirtualNode { hash, node: node.to_owned() });
        }
        self.data.insert(node.to_owned(), self.replicas);
        true
    }

    /// Go `AddNode`: adds a real node and its virtual nodes.
    pub fn add_node(&mut self, node: &str) -> bool {
        if self.do_insert(node) {
            self.do_resort();
            return true;
        }
        false
    }

    /// Go `RemoveNode`: removes a real node and all its virtual nodes.
    pub fn remove_node(&mut self, node: &str) {
        if self.data.remove(node).is_none() {
            return;
        }
        self.ring.retain(|vnode| vnode.node != node);
    }

    /// Go `GetNode`: the real node owning `key`.
    #[must_use]
    pub fn get_node(&self, key: &[u8]) -> &str {
        if self.ring.is_empty() {
            return "";
        }
        let hash = (self.hash_func)(key);
        // Binary search for the first node clockwise.
        let mut idx = self.ring.partition_point(|vnode| vnode.hash < hash);
        // Wrap to the start of the ring if out of range.
        if idx == self.ring.len() {
            idx = 0;
        }
        &self.ring[idx].node
    }

    /// Go `GetNodes`: every real node, sorted.
    #[must_use]
    pub fn get_nodes(&self) -> Vec<String> {
        let mut nodes: Vec<String> = self.data.keys().cloned().collect();
        nodes.sort();
        nodes
    }

    /// Go `NodeCount`.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.data.len()
    }

    #[cfg(test)]
    pub(crate) fn get_virtual_nodes(&self, node: &str) -> usize {
        self.data.get(node).copied().unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn set_hash_func(&mut self, hash_func: impl Fn(&[u8]) -> u32 + Send + Sync + 'static) {
        self.hash_func = Box::new(hash_func);
    }
}
