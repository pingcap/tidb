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

//! Statement-planning overlay. The transaction's client MemDB applies writes
//! in statement order; this scratch plan does not coalesce transaction state.
use super::OptimisticMutation;
use std::collections::BTreeMap;

/// Scratch operations used while planning one SQL statement.
#[derive(Debug, Default)]
pub struct MutationPlan {
    mutations: Vec<OptimisticMutation>,
    latest: BTreeMap<Vec<u8>, usize>,
}
impl MutationPlan {
    /// Whether the statement planned any mutations.
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
    /// Creates an empty statement plan.
    pub fn new() -> Self {
        Self::default()
    }
    /// Appends a statement operation and updates its planning overlay.
    pub fn stage(&mut self, mutation: OptimisticMutation) {
        self.latest
            .insert(mutation.key().to_vec(), self.mutations.len());
        self.mutations.push(mutation);
    }
    /// The most recent planned operation for a key.
    pub fn staged(&self, key: &[u8]) -> Option<&OptimisticMutation> {
        self.latest.get(key).map(|index| &self.mutations[*index])
    }
    /// Returns operations in statement order for the client MemDB.
    pub fn into_mutations(self) -> Vec<OptimisticMutation> {
        self.mutations
    }
}
