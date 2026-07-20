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

use std::collections::BTreeMap;

use tidb_proto::{KvrpcContext, KvrpcPeer, KvrpcRegionEpoch, KvrpcRequestOrigin};

use crate::region::{
    BackgroundRegionCacheError, RegionAttempt, RegionLoader, RegionLocation, RegionVerId,
};
use crate::SharedReadRuntime;

use super::mutation::OptimisticMutation;

const TXN_COMMIT_BATCH_BYTES: usize = 16 * 1024;

/// One deterministically ordered batch routed to one exact region epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionMutationBatch {
    region: RegionVerId,
    address: String,
    context: KvrpcContext,
    attempt: RegionAttempt,
    mutations: Vec<OptimisticMutation>,
}

impl RegionMutationBatch {
    /// Exact region epoch used for this attempt.
    #[must_use]
    pub const fn region(&self) -> RegionVerId {
        self.region
    }

    /// Physical TiKV leader address resolved by the shared RegionCache.
    #[must_use]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Immutable mutations in encoded-key order.
    #[must_use]
    pub fn mutations(&self) -> &[OptimisticMutation] {
        &self.mutations
    }

    pub(super) fn context(&self) -> &KvrpcContext {
        &self.context
    }

    pub(super) fn attempt(&self) -> &RegionAttempt {
        &self.attempt
    }

    pub(super) fn keys(&self) -> Vec<Vec<u8>> {
        self.mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .collect()
    }
}

/// One deterministically ordered key-only batch used by Commit and rollback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionKeyBatch {
    region: RegionVerId,
    address: String,
    context: KvrpcContext,
    attempt: RegionAttempt,
    keys: Vec<Vec<u8>>,
}

impl RegionKeyBatch {
    #[must_use]
    pub(super) const fn region(&self) -> RegionVerId {
        self.region
    }

    #[must_use]
    pub(super) fn address(&self) -> &str {
        &self.address
    }

    #[must_use]
    pub(super) fn context(&self) -> &KvrpcContext {
        &self.context
    }

    pub(super) fn attempt(&self) -> &RegionAttempt {
        &self.attempt
    }

    #[must_use]
    pub(super) fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RegionBatchError {
    Cache(String),
    Route(String),
    MissingLeader(RegionVerId),
    MissingLeaderPeer(RegionVerId, u64),
    MissingStore(RegionVerId, u64),
    EmptyAddress(RegionVerId, u64),
}

impl std::fmt::Display for RegionBatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

pub(super) fn group_mutations<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    mutations: &[OptimisticMutation],
) -> Result<Vec<RegionMutationBatch>, RegionBatchError>
where
    L: RegionLoader,
{
    let mut grouped: BTreeMap<RegionVerId, (Route, Vec<OptimisticMutation>)> = BTreeMap::new();
    for mutation in mutations {
        let route = locate_route(runtime, mutation.key())?;
        grouped
            .entry(route.region)
            .or_insert_with(|| (route.clone(), Vec::new()))
            .1
            .push(mutation.clone());
    }
    let mut batches = Vec::new();
    for (region, (route, mutations)) in grouped {
        let chunks = split_mutations(mutations);
        for mutations in chunks {
            batches.push(RegionMutationBatch {
                region,
                address: route.address.clone(),
                context: route.context.clone(),
                attempt: route.attempt.clone(),
                mutations,
            });
        }
    }
    Ok(batches)
}

pub(super) fn group_keys<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    keys: &[Vec<u8>],
) -> Result<Vec<RegionKeyBatch>, RegionBatchError>
where
    L: RegionLoader,
{
    let mut sorted = keys.to_vec();
    sorted.sort();
    sorted.dedup();
    let mut grouped: BTreeMap<RegionVerId, (Route, Vec<Vec<u8>>)> = BTreeMap::new();
    for key in sorted {
        let route = locate_route(runtime, &key)?;
        grouped
            .entry(route.region)
            .or_insert_with(|| (route.clone(), Vec::new()))
            .1
            .push(key);
    }
    let mut batches = Vec::new();
    for (region, (route, keys)) in grouped {
        for keys in split_keys(keys) {
            batches.push(RegionKeyBatch {
                region,
                address: route.address.clone(),
                context: route.context.clone(),
                attempt: route.attempt.clone(),
                keys,
            });
        }
    }
    Ok(batches)
}

fn split_mutations(mutations: Vec<OptimisticMutation>) -> Vec<Vec<OptimisticMutation>> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for mutation in mutations {
        let mutation_bytes = mutation.key().len().saturating_add(mutation.value().len());
        if !current.is_empty()
            && current_bytes.saturating_add(mutation_bytes) > TXN_COMMIT_BATCH_BYTES
        {
            batches.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(mutation_bytes);
        current.push(mutation);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

fn split_keys(keys: Vec<Vec<u8>>) -> Vec<Vec<Vec<u8>>> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for key in keys {
        if !current.is_empty() && current_bytes.saturating_add(key.len()) > TXN_COMMIT_BATCH_BYTES {
            batches.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(key.len());
        current.push(key);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

pub(super) fn point_route<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    key: &[u8],
) -> Result<RegionKeyBatch, RegionBatchError>
where
    L: RegionLoader,
{
    let route = locate_route(runtime, key)?;
    Ok(RegionKeyBatch {
        region: route.region,
        address: route.address,
        context: route.context,
        attempt: route.attempt,
        keys: vec![key.to_vec()],
    })
}

#[derive(Clone)]
struct Route {
    region: RegionVerId,
    address: String,
    context: KvrpcContext,
    attempt: RegionAttempt,
}

fn locate_route<C, L>(
    runtime: &SharedReadRuntime<C, L>,
    key: &[u8],
) -> Result<Route, RegionBatchError>
where
    L: RegionLoader,
{
    let location = runtime
        .locate_key(key)
        .map_err(cache_error)?
        .map_err(|error| RegionBatchError::Route(error.to_string()))?;
    route_from_location(location, runtime.cluster_id())
}

fn route_from_location(
    location: RegionLocation,
    cluster_id: u64,
) -> Result<Route, RegionBatchError> {
    let leader_id = location
        .leader_peer_id
        .ok_or(RegionBatchError::MissingLeader(location.region))?;
    let leader = location
        .peers
        .iter()
        .find(|peer| peer.id == leader_id)
        .ok_or(RegionBatchError::MissingLeaderPeer(
            location.region,
            leader_id,
        ))?;
    let store = location
        .stores
        .iter()
        .find(|store| store.id == leader.store_id)
        .ok_or(RegionBatchError::MissingStore(
            location.region,
            leader.store_id,
        ))?;
    if store.address.is_empty() {
        return Err(RegionBatchError::EmptyAddress(
            location.region,
            leader.store_id,
        ));
    }
    Ok(Route {
        region: location.region,
        address: store.address.clone(),
        context: KvrpcContext {
            region_id: location.region.id,
            region_epoch: Some(KvrpcRegionEpoch {
                conf_ver: location.region.epoch.conf_ver,
                version: location.region.epoch.version,
            }),
            peer: Some(KvrpcPeer {
                id: leader.id,
                store_id: leader.store_id,
                role: leader.role.as_i32(),
                is_witness: leader.is_witness,
            }),
            request_source: "tidb_rust_normal_optimistic_2pc".to_owned(),
            request_origin: KvrpcRequestOrigin::TiDb as i32,
            cluster_id,
            ..KvrpcContext::default()
        },
        attempt: RegionAttempt {
            region: location.region,
            peer_id: leader.id,
            store_id: leader.store_id,
            address: store.address.clone(),
            store_epoch: leader.store_epoch,
        },
    })
}

fn cache_error(error: BackgroundRegionCacheError) -> RegionBatchError {
    RegionBatchError::Cache(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_region_batches_split_at_exact_checked_byte_boundary() {
        let exact = vec![
            OptimisticMutation::insert(b"a".to_vec(), vec![1; 8 * 1024 - 1]).unwrap(),
            OptimisticMutation::insert(b"b".to_vec(), vec![2; 8 * 1024 - 1]).unwrap(),
        ];
        let exact_batches = split_mutations(exact);
        assert_eq!(exact_batches.len(), 1);
        assert_eq!(exact_batches[0][0].key(), b"a");
        assert_eq!(exact_batches[0][1].key(), b"b");

        let plus_one = vec![
            OptimisticMutation::insert(b"a".to_vec(), vec![1; 8 * 1024]).unwrap(),
            OptimisticMutation::insert(b"b".to_vec(), vec![2; 8 * 1024 - 1]).unwrap(),
        ];
        let split = split_mutations(plus_one);
        assert_eq!(split.len(), 2);
        assert!(split.iter().all(|batch| !batch.is_empty()));
        assert_eq!(split[0][0].key(), b"a");
        assert_eq!(split[1][0].key(), b"b");
    }
}
