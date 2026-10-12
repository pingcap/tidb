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

//! The PD HTTP calls behind [`tidb_domain::affinity::PdAffinityClient`]:
//! `CreateAffinityGroups`, `GetAffinityGroups`, `GetAllAffinityGroups` and
//! `BatchDeleteAffinityGroups` as `tikv/pd/client/http` makes them, over the
//! node's cluster HTTP client (the CA and client identity PD discovery uses).

use std::collections::BTreeMap;
use std::time::Duration;

use tidb_domain::affinity::{
    query_escape, AffinityGroupKeyRange, AffinityGroupStates, AffinityGroups, PdAffinityClient,
    PdHttpError,
};

/// pd `AffinityGroups`.
const AFFINITY_GROUPS: &str = "/pd/api/v2/affinity-groups";
/// The request timeout the node's other PD HTTP calls use.
const PD_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// pd `CreateAffinityGroupInput`.
#[derive(serde::Serialize)]
struct CreateAffinityGroupInput<'a> {
    ranges: &'a [AffinityGroupKeyRange],
}

/// pd `CreateAffinityGroupsRequest`.
#[derive(serde::Serialize)]
struct CreateAffinityGroupsRequest<'a> {
    affinity_groups: BTreeMap<&'a str, CreateAffinityGroupInput<'a>>,
}

/// pd `AffinityGroupsResponse`.
#[derive(serde::Deserialize)]
struct AffinityGroupsResponse {
    #[serde(default)]
    affinity_groups: Option<AffinityGroupStates>,
}

/// pd `BatchDeleteAffinityGroupsRequest`.
#[derive(serde::Serialize)]
struct BatchDeleteAffinityGroupsRequest<'a> {
    ids: &'a [String],
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    force: bool,
}

/// PD's affinity-group HTTP API at one endpoint.
pub struct PdHttpAffinityClient {
    endpoint: String,
    http: reqwest::blocking::Client,
}

impl PdHttpAffinityClient {
    /// The client for `pd_http` (`http://host:port`), switched to HTTPS and
    /// given the cluster identity when `security` enables TLS.
    pub fn with_security(
        pd_http: &str,
        security: &tidb_pd_client::ClusterSecurity,
    ) -> Result<Self, String> {
        let endpoint = if security.is_tls_enabled() {
            pd_http.replacen("http://", "https://", 1)
        } else {
            pd_http.to_owned()
        };
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            http: crate::cluster_http::cluster_http_client(security)?,
        })
    }

    /// pd `client.request`: a non-200 answer is the error
    /// `request pd http api failed with status: '<status>', body: '<body>'`.
    fn request(
        &self,
        method: reqwest::Method,
        uri: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, PdHttpError> {
        let mut request = self
            .http
            .request(method, format!("{}{uri}", self.endpoint))
            .timeout(PD_HTTP_TIMEOUT);
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let response = request
            .send()
            .map_err(|error| PdHttpError::Message(error.to_string()))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .map_err(|error| PdHttpError::Message(error.to_string()))?
            .to_vec();
        if status != reqwest::StatusCode::OK {
            return Err(PdHttpError::Message(format!(
                "request pd http api failed with status: '{status}', body: '{}'",
                String::from_utf8_lossy(&bytes)
            )));
        }
        Ok(bytes)
    }

    fn groups_response(bytes: &[u8]) -> Result<AffinityGroupStates, PdHttpError> {
        let response: AffinityGroupsResponse = serde_json::from_slice(bytes)
            .map_err(|error| PdHttpError::Message(error.to_string()))?;
        Ok(response.affinity_groups.unwrap_or_default())
    }
}

impl PdAffinityClient for PdHttpAffinityClient {
    fn create_affinity_groups(
        &self,
        groups: &AffinityGroups,
        skip_exist_check: bool,
    ) -> Result<AffinityGroupStates, PdHttpError> {
        let request = CreateAffinityGroupsRequest {
            affinity_groups: groups
                .iter()
                .map(|(id, ranges)| (id.as_str(), CreateAffinityGroupInput { ranges }))
                .collect(),
        };
        let body = serde_json::to_vec(&request)
            .map_err(|error| PdHttpError::Message(error.to_string()))?;
        let uri = if skip_exist_check {
            format!("{AFFINITY_GROUPS}?skip_exist_check=true")
        } else {
            AFFINITY_GROUPS.to_owned()
        };
        let bytes = self.request(reqwest::Method::POST, &uri, Some(body))?;
        Self::groups_response(&bytes)
    }

    /// pd: with no ids this is `GetAllAffinityGroups`.
    fn get_affinity_groups(&self, ids: &[String]) -> Result<AffinityGroupStates, PdHttpError> {
        if ids.is_empty() {
            return self.get_all_affinity_groups();
        }
        let query = ids
            .iter()
            .map(|id| format!("ids={}", query_escape(id)))
            .collect::<Vec<_>>()
            .join("&");
        let bytes = self.request(
            reqwest::Method::GET,
            &format!("{AFFINITY_GROUPS}?{query}"),
            None,
        )?;
        Self::groups_response(&bytes)
    }

    fn get_all_affinity_groups(&self) -> Result<AffinityGroupStates, PdHttpError> {
        let bytes = self.request(reqwest::Method::GET, AFFINITY_GROUPS, None)?;
        Self::groups_response(&bytes)
    }

    /// pd: `POST` with the `?delete` query.
    fn batch_delete_affinity_groups(&self, ids: &[String], force: bool) -> Result<(), PdHttpError> {
        let body = serde_json::to_vec(&BatchDeleteAffinityGroupsRequest { ids, force })
            .map_err(|error| PdHttpError::Message(error.to_string()))?;
        self.request(
            reqwest::Method::POST,
            &format!("{AFFINITY_GROUPS}?delete"),
            Some(body),
        )
        .map(|_| ())
    }
}
