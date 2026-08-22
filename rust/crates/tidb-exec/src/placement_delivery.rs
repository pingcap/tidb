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

//! Sending placement rule bundles to PD.
//!
//! boundary: PD's HTTP API, which is where placement rules live. Go reaches
//! it through `github.com/tikv/pd/client/http` --
//! `PDPlacementManager.PutRuleBundles` calls `SetPlacementRuleBundles`
//! (`pkg/domain/infosync/placement_manager.go:66`) -- and nothing in this
//! tree's gRPC PD client speaks it. `tikv/client-rust` does not either: it
//! exports raw and transactional KV over gRPC and no PD administrative API,
//! so it is not the analogue of the package Go uses here.
//!
//! The contract is read from PD's own client rather than guessed:
//! `client/http/api.go` defines
//! `PlacementRuleBundle = "/pd/api/v1/config/placement-rule"` and
//! `PlacementRuleBundleWithPartialParameter(partial)` appending
//! `?partial=%t`; `client/http/interface.go` shows `SetPlacementRuleBundles`
//! marshalling `[]*GroupBundle` and POSTing it. TiDB passes `partial=true`.
//!
//! The bundle JSON needs no wire type of its own: [`tidb_placement::Bundle`]
//! already serialises to PD's four fields -- `group_id`, `group_index`,
//! `group_override`, `rules`.

use std::time::Duration;

use tidb_placement::Bundle;

/// Why a bundle delivery did not happen.
#[derive(Debug)]
pub enum PlacementDeliveryError {
    /// The bundles could not be encoded.
    Encode(String),
    /// The request could not be built or sent.
    Transport(String),
    /// PD answered, and not with success. Carries the status and body, which
    /// is what PD puts its own diagnostic in.
    Rejected {
        /// The HTTP status PD returned.
        status: u16,
        /// PD's response body.
        detail: String,
    },
    /// A runtime could not be started to drive the request.
    Runtime(String),
}

impl std::fmt::Display for PlacementDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(detail) => write!(formatter, "encoding placement bundles: {detail}"),
            Self::Transport(detail) => write!(formatter, "sending placement bundles: {detail}"),
            Self::Rejected { status, detail } => {
                write!(formatter, "PD rejected the placement bundles ({status}): {detail}")
            }
            Self::Runtime(detail) => write!(formatter, "placement delivery runtime: {detail}"),
        }
    }
}

/// Go `PDPlacementManager.PutRuleBundles`: POSTs every bundle to PD.
///
/// `endpoint` is PD's client address, the same one the gRPC client connects
/// to -- PD serves its HTTP API there as well.
///
/// Go sends `partial=true`, which means the named groups are replaced and
/// groups this request does not mention are left alone. `partial=false` would
/// drop every rule PD holds that is not in this payload, which for a single
/// table's change would erase the placement of every OTHER object.
///
/// An empty bundle list is not sent at all, matching Go's early return.
///
/// # Errors
///
/// Returns [`PlacementDeliveryError`] when the bundles cannot be encoded, the
/// request cannot be sent, or PD answers with a non-success status.
pub fn put_rule_bundles(
    endpoint: &str,
    bundles: &[Bundle],
    timeout: Duration,
) -> Result<(), PlacementDeliveryError> {
    if bundles.is_empty() {
        return Ok(());
    }
    let body = serde_json::to_vec(bundles)
        .map_err(|error| PlacementDeliveryError::Encode(error.to_string()))?;
    let url = format!(
        "{}/pd/api/v1/config/placement-rule?partial=true",
        endpoint.trim_end_matches('/')
    );
    // The DDL path is synchronous, so the request is driven on a
    // current-thread runtime of its own -- the same shape `PdClient` uses to
    // present a synchronous API over tonic.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| PlacementDeliveryError::Runtime(error.to_string()))?;
    runtime.block_on(async move {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| PlacementDeliveryError::Transport(error.to_string()))?;
        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| PlacementDeliveryError::Transport(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let detail = response.text().await.unwrap_or_default();
        Err(PlacementDeliveryError::Rejected {
            status: status.as_u16(),
            detail,
        })
    })
}
