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
//! (`pkg/domain/infosync/placement_manager.go:66`) on the info syncer's
//! derivation of the store's PD HTTP client -- and this module calls the
//! same client-rust port of it (`tikv_client::pd_http`).
//!
//! The bundle JSON needs no wire type of its own: [`tidb_placement::Bundle`]
//! already serialises to PD's four fields -- `group_id`, `group_index`,
//! `group_override`, `rules` -- which is why Go can cast it to
//! `*pd.GroupBundle`; the cast here goes through that JSON.

use std::time::Duration;

use tidb_placement::Bundle;
use tikv_client::pd_http;

/// Why a bundle delivery did not happen.
#[derive(Debug)]
pub enum PlacementDeliveryError {
    /// The bundles could not be encoded.
    Encode(String),
    /// The PD HTTP request failed: it could not be sent, PD answered with a
    /// status other than success (the error carries PD's body, which is what
    /// PD puts its own diagnostic in), or it ran past its deadline.
    Request(String),
    /// A runtime could not be started to drive the request.
    Runtime(String),
}

impl std::fmt::Display for PlacementDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(detail) => write!(formatter, "encoding placement bundles: {detail}"),
            Self::Request(detail) => write!(formatter, "sending placement bundles: {detail}"),
            Self::Runtime(detail) => write!(formatter, "placement delivery runtime: {detail}"),
        }
    }
}

// One request on the store runtime, bounded by the caller's deadline (the
// timer is created inside the runtime).
fn request_pd<T: Send>(
    timeout: Duration,
    request: impl std::future::Future<Output = tikv_client::Result<T>> + Send,
) -> Result<T, PlacementDeliveryError> {
    tidb_txnkv::driver::tikv_store::block_on_store_runtime(async move {
        tokio::time::timeout(timeout, request).await
    })
    .map_err(PlacementDeliveryError::Runtime)?
    .map_err(|_| PlacementDeliveryError::Request("PD HTTP request timed out".to_owned()))?
    .map_err(|error| PlacementDeliveryError::Request(error.to_string()))
}

/// Go `PDPlacementManager.PutRuleBundles`: POSTs every bundle to PD through
/// `SetPlacementRuleBundles(ctx, bundles, true)`, on the info syncer's
/// derivation of the store's PD HTTP client `pd_http_client`.
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
    pd_http_client: &pd_http::Client,
    bundles: &[Bundle],
    timeout: Duration,
) -> Result<(), PlacementDeliveryError> {
    if bundles.is_empty() {
        return Ok(());
    }
    // Go's `(*pd.GroupBundle)(bundle)` cast.
    let rule_bundles = bundles
        .iter()
        .map(|bundle| serde_json::to_value(bundle).and_then(serde_json::from_value))
        .collect::<Result<Vec<pd_http::GroupBundle>, _>>()
        .map_err(|error| PlacementDeliveryError::Encode(error.to_string()))?;
    let pd_http_client = crate::cluster_http::info_syncer_pd_http_client(pd_http_client);
    request_pd(
        timeout,
        pd_http_client.set_placement_rule_bundles(&rule_bundles, true),
    )
}

/// Go `infosync.DeleteTiFlashPlacementRules`: removes the TiFlash replica
/// rule of every physical table, through the TiFlash replica manager's
/// `SetPlacementRuleBatch` -- `SetTiFlashGroupConfig` first, then one
/// `RuleOpDel` per table's `MakeNewRule(id, 0, nil)` -- on the info syncer's
/// derivation of the store's PD HTTP client `pd_http_client`.
///
/// # Errors
///
/// Returns [`PlacementDeliveryError`] when a request cannot be sent or PD
/// answers with a non-success status.
pub fn delete_tiflash_placement_rules(
    pd_http_client: &pd_http::Client,
    physical_table_ids: &[i64],
    timeout: Duration,
) -> Result<(), PlacementDeliveryError> {
    let pd_http_client = crate::cluster_http::info_syncer_pd_http_client(pd_http_client);
    let rule_ops: Vec<_> = physical_table_ids
        .iter()
        .map(|&id| tiflash_rule_delete_op(id))
        .collect();
    // Go `SetTiFlashGroupConfig`: keep a group already at the TiFlash index
    // without override; otherwise (re)write it.
    let group_config = request_pd(
        timeout,
        pd_http_client.get_placement_rule_group_by_id(tidb_placement::TIFLASH_RULE_GROUP_ID),
    )?;
    if group_config.index != tidb_placement::RULE_INDEX_TIFLASH || group_config.r#override {
        request_pd(
            timeout,
            pd_http_client.set_placement_rule_group(&pd_http::RuleGroup {
                id: tidb_placement::TIFLASH_RULE_GROUP_ID.to_owned(),
                index: tidb_placement::RULE_INDEX_TIFLASH,
                r#override: false,
            }),
        )?;
    }
    request_pd(
        timeout,
        pd_http_client.set_placement_rule_in_batch(&rule_ops),
    )
}

/// Go `doSetPlacementRuleBatch`'s operation for a zero-count
/// `infosync.MakeNewRule(id, 0, nil)`: a `RuleOpDel`.
fn tiflash_rule_delete_op(physical_table_id: i64) -> pd_http::RuleOp {
    pd_http::RuleOp {
        rule: crate::tiflash_replica_manager::make_new_rule(physical_table_id, 0, &[]),
        action: pd_http::RULE_OP_DEL,
        delete_by_id_prefix: false,
    }
}

#[cfg(test)]
mod tiflash_rule_tests {
    use super::*;

    #[test]
    fn a_tiflash_rule_delete_is_go_make_new_rule_with_a_zero_count() {
        let op = serde_json::to_value(tiflash_rule_delete_op(100)).unwrap();
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
