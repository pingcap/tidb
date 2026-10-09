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

/// Go `Helper.GetPDRegionStats(..., noIndexStats=true)`: query PD's HTTP API
/// for the memcomparable-encoded record-key range of one physical table.
pub fn load_record_region_stats(
    endpoint: &str,
    table_id: i64,
    timeout: Duration,
) -> Result<RegionCountStats, String> {
    let start = tidb_codec::gen_table_record_prefix(table_id);
    let end = tidb_txnkv::Key::from_bytes(start.clone()).prefix_next();
    let mut encoded_start = Vec::new();
    let mut encoded_end = Vec::new();
    tidb_codec::encode_bytes(&mut encoded_start, &start);
    tidb_codec::encode_bytes(&mut encoded_end, end.as_bytes());
    let url = format!(
        "{}/pd/api/v1/stats/region?start_key={}&end_key={}",
        endpoint.trim_end_matches('/'),
        go_query_escape(&encoded_start),
        go_query_escape(&encoded_end)
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async move {
        let response = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| error.to_string())?
            .get(url)
            .send()
            .await
            .map_err(|error| error.to_string())?
            .error_for_status()
            .map_err(|error| error.to_string())?;
        let value: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
        let count = value
            .get("count")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| "PD region statistics omitted count".to_owned())?;
        let storage_keys = value
            .get("storage_keys")
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| "PD region statistics omitted storage_keys".to_owned())?;
        Ok(RegionCountStats {
            count,
            storage_keys,
        })
    })
}

fn go_query_escape(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut escaped = String::with_capacity(bytes.len() * 3);
    for byte in bytes.iter().copied() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            escaped.push(char::from(byte));
        } else if byte == b' ' {
            escaped.push('+');
        } else {
            escaped.push('%');
            escaped.push(char::from(HEX[usize::from(byte >> 4)]));
            escaped.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    escaped
}

pub use tidb_executor::pd_helper::{approximate_table_count_key, ApproximateTableCountCache};
