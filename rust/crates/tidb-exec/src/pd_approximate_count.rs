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

//! The PD half of Go `pkg/executor/internal/pdhelper/pd.go`: the record-key
//! region statistics its loader asks PD for. The approximate-count cache
//! itself lives in `tidb_executor::pd_helper`, which the in-process catalog
//! shares, and is re-exported here.

use std::time::Duration;

/// The two PD region statistics consumed by Go's approximate-count helper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegionCountStats {
    /// Regions intersecting the table record-key range.
    pub count: usize,
    /// PD's approximate number of keys in that range.
    pub storage_keys: i64,
}

/// Go `Helper.GetPDRegionStats(..., noIndexStats=true)`: PD's statistics for
/// the memcomparable-encoded record-key range of one physical table, through
/// the store's PD HTTP client (`TryGetPDHTTPClient`, caller ID
/// `tidb-store-helper`) and `GetRegionStatusByKeyRange(.., false)`. The
/// request is bounded by `timeout`, the caller's statement budget.
pub async fn load_record_region_stats(
    pd_http_client: &tikv_client::pd_http::Client,
    table_id: i64,
    timeout: Duration,
) -> Result<RegionCountStats, String> {
    let start = tidb_codec::gen_table_record_prefix(table_id);
    let end = tidb_txnkv::Key::from_bytes(start.clone()).prefix_next();
    let mut encoded_start = Vec::new();
    let mut encoded_end = Vec::new();
    tidb_codec::encode_bytes(&mut encoded_start, &start);
    tidb_codec::encode_bytes(&mut encoded_end, end.as_bytes());
    let pd_cli = pd_http_client.with_caller_id("tidb-store-helper");
    let key_range = tikv_client::pd_http::new_key_range(encoded_start, encoded_end);
    let request = pd_cli.get_region_status_by_key_range(&key_range, false);
    let stats = tokio::time::timeout(timeout, request)
        .await
        .map_err(|_| "PD region statistics request timed out".to_owned())?
        .map_err(|error| error.to_string())?;
    let count = usize::try_from(stats.count).map_err(|error| error.to_string())?;
    Ok(RegionCountStats {
        count,
        storage_keys: stats.storage_keys,
    })
}

pub use tidb_executor::pd_helper::{approximate_table_count_key, ApproximateTableCountCache};
