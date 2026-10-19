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
                write!(
                    formatter,
                    "PD rejected the placement bundles ({status}): {detail}"
                )
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

/// Go `placement.TiFlashRuleGroupID`.
const TIFLASH_RULE_GROUP_ID: &str = "tiflash";
/// Go `placement.RuleIndexTiFlash`.
const RULE_INDEX_TIFLASH: i64 = 120;

/// Go `pdhttp`'s `rawKeyToKeyHexStr`: the memcomparable-encoded key in hex.
fn raw_key_to_key_hex_str(key: &[u8]) -> String {
    if key.is_empty() {
        return String::new();
    }
    let mut encoded = Vec::new();
    tidb_codec::encode_bytes(&mut encoded, key);
    encoded.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Go `infosync.MakeNewRule(id, 0, nil)` as the delete operation
/// `SetPlacementRuleBatch` sends for a zero count (`pd.RuleOp` JSON).
fn tiflash_rule_delete_op(physical_table_id: i64) -> serde_json::Value {
    serde_json::json!({
        "group_id": TIFLASH_RULE_GROUP_ID,
        "id": format!("table-{physical_table_id}-r"),
        "index": RULE_INDEX_TIFLASH,
        "start_key": raw_key_to_key_hex_str(
            &tidb_codec::table_key::gen_table_record_prefix(physical_table_id),
        ),
        "end_key": raw_key_to_key_hex_str(
            &tidb_codec::table_key::encode_table_prefix(physical_table_id + 1),
        ),
        "role": "learner",
        "is_witness": false,
        "count": 0,
        "label_constraints": [{"key": "engine", "op": "in", "values": ["tiflash"]}],
        "action": "del",
        "delete_by_id_prefix": false,
    })
}

/// Go `infosync.DeleteTiFlashPlacementRules`: removes the TiFlash replica
/// rule of every physical table, through the TiFlash replica manager's
/// `SetPlacementRuleBatch` (the `tiflash` rule group configured first, as
/// `SetTiFlashGroupConfig` does, then one delete operation per table).
///
/// # Errors
///
/// Returns [`PlacementDeliveryError`] when a request cannot be sent or PD
/// answers with a non-success status.
pub fn delete_tiflash_placement_rules(
    endpoint: &str,
    physical_table_ids: &[i64],
    timeout: Duration,
) -> Result<(), PlacementDeliveryError> {
    let endpoint = endpoint.trim_end_matches('/').to_owned();
    let operations: Vec<_> = physical_table_ids
        .iter()
        .map(|&id| tiflash_rule_delete_op(id))
        .collect();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| PlacementDeliveryError::Runtime(error.to_string()))?;
    runtime.block_on(async move {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| PlacementDeliveryError::Transport(error.to_string()))?;
        let rejected =
            |status: reqwest::StatusCode, detail: String| PlacementDeliveryError::Rejected {
                status: status.as_u16(),
                detail,
            };
        // Go `SetTiFlashGroupConfig`: keep a group already at the TiFlash
        // index without override; otherwise (re)write it.
        let response = client
            .get(format!(
                "{endpoint}/pd/api/v1/config/rule_group/{TIFLASH_RULE_GROUP_ID}"
            ))
            .send()
            .await
            .map_err(|error| PlacementDeliveryError::Transport(error.to_string()))?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(rejected(status, body));
        }
        let group: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
        let configured = group.get("index").and_then(serde_json::Value::as_i64)
            == Some(RULE_INDEX_TIFLASH)
            && !group
                .get("override")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
        if !configured {
            let response = client
                .post(format!("{endpoint}/pd/api/v1/config/rule_group"))
                .header("Content-Type", "application/json")
                .body(
                    serde_json::json!({"id": TIFLASH_RULE_GROUP_ID, "index": RULE_INDEX_TIFLASH})
                        .to_string(),
                )
                .send()
                .await
                .map_err(|error| PlacementDeliveryError::Transport(error.to_string()))?;
            let status = response.status();
            if !status.is_success() {
                return Err(rejected(status, response.text().await.unwrap_or_default()));
            }
        }
        let response = client
            .post(format!("{endpoint}/pd/api/v1/config/rules/batch"))
            .header("Content-Type", "application/json")
            .body(serde_json::Value::Array(operations).to_string())
            .send()
            .await
            .map_err(|error| PlacementDeliveryError::Transport(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        Err(rejected(status, response.text().await.unwrap_or_default()))
    })
}

#[cfg(test)]
mod tiflash_rule_tests {
    use super::*;

    #[test]
    fn a_tiflash_rule_delete_is_go_make_new_rule_with_a_zero_count() {
        let op = tiflash_rule_delete_op(100);
        assert_eq!("tiflash", op["group_id"]);
        assert_eq!("table-100-r", op["id"]);
        assert_eq!(120, op["index"]);
        assert_eq!(0, op["count"]);
        assert_eq!("del", op["action"]);
        // `t\x80\0\0\0\0\0\0\x64_r` and `t\x80\0\0\0\0\0\0\x65`, memcomparable.
        assert_eq!("7480000000000000ff645f720000000000fa", op["start_key"]);
        assert_eq!("7480000000000000ff6500000000000000f8", op["end_key"]);
    }
}
