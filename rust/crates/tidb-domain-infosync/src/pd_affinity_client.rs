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

//! The PD HTTP calls behind [`tidb_domain_affinity::PdAffinityClient`]:
//! `CreateAffinityGroups`, `GetAffinityGroups`, `GetAllAffinityGroups` and
//! `BatchDeleteAffinityGroups` on the info syncer's derivation of the store's
//! PD HTTP client, which Go hands to `affinity.InitManager(is.pdHTTPCli)`.

use std::collections::HashMap;

use tidb_domain_affinity::{
    AffinityGroup, AffinityGroupState, AffinityGroupStates, AffinityGroups, PdAffinityClient,
    PdHttpError,
};
use tikv_client::pd_http;

/// Go `pdhttp.Client` as the affinity manager calls it.
pub struct PdHttpAffinityClient {
    pd_http_client: pd_http::Client,
}

impl PdHttpAffinityClient {
    /// The client `affinity.InitManager(is.pdHTTPCli)` gets: the info
    /// syncer's derivation of the store's PD HTTP client.
    #[must_use]
    pub fn new(pd_http_client: &pd_http::Client) -> Self {
        Self {
            pd_http_client: crate::pd_http::info_syncer_pd_http_client(pd_http_client),
        }
    }

    // One request on the store runtime; the info syncer's `ErrHTTPServiceError`
    // stays a coded error, as Go's `*errors.Error` does.
    fn request<T: Send>(
        request: impl std::future::Future<Output = tikv_client::Result<T>> + Send,
    ) -> Result<T, PdHttpError> {
        tidb_store_driver_txn::cluster_http::block_on_pd_http(request).map_err(|message| {
            if crate::pd_http::is_http_service_error(&message) {
                PdHttpError::Coded {
                    code: tidb_errno::errcode::ErrHTTPServiceError,
                    message,
                }
            } else {
                PdHttpError::Message(message)
            }
        })
    }
}

fn affinity_group_states(
    states: HashMap<String, pd_http::AffinityGroupState>,
) -> AffinityGroupStates {
    states
        .into_iter()
        .map(|(id, state)| {
            let group = state.affinity_group;
            let state = AffinityGroupState {
                group: AffinityGroup {
                    id: group.id,
                    create_timestamp: group.create_timestamp,
                    leader_store_id: group.leader_store_id,
                    voter_store_ids: group.voter_store_ids,
                },
                phase: state.phase,
                range_count: state.range_count,
                region_count: state.region_count,
                affinity_region_count: state.affinity_region_count,
            };
            (id, state)
        })
        .collect()
}

impl PdAffinityClient for PdHttpAffinityClient {
    fn create_affinity_groups(
        &self,
        groups: &AffinityGroups,
        skip_exist_check: bool,
    ) -> Result<AffinityGroupStates, PdHttpError> {
        let affinity_groups = groups
            .iter()
            .map(|(id, ranges)| {
                let ranges = ranges
                    .iter()
                    .map(|range| pd_http::AffinityGroupKeyRange {
                        start_key: range.start_key.clone(),
                        end_key: range.end_key.clone(),
                    })
                    .collect();
                (id.clone(), ranges)
            })
            .collect::<HashMap<_, Vec<_>>>();
        let opts = if skip_exist_check {
            vec![pd_http::with_skip_exist_check()]
        } else {
            Vec::new()
        };
        Self::request(
            self.pd_http_client
                .create_affinity_groups(&affinity_groups, &opts),
        )
        .map(affinity_group_states)
    }

    /// pd: with no ids this is `GetAllAffinityGroups`.
    fn get_affinity_groups(&self, ids: &[String]) -> Result<AffinityGroupStates, PdHttpError> {
        Self::request(self.pd_http_client.get_affinity_groups(ids)).map(affinity_group_states)
    }

    fn get_all_affinity_groups(&self) -> Result<AffinityGroupStates, PdHttpError> {
        Self::request(self.pd_http_client.get_all_affinity_groups()).map(affinity_group_states)
    }

    fn batch_delete_affinity_groups(&self, ids: &[String], force: bool) -> Result<(), PdHttpError> {
        Self::request(self.pd_http_client.batch_delete_affinity_groups(ids, force))
    }
}
