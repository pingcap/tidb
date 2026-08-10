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

//! Region-management seam for the convergence node.
//!
//! The session resolves SQL objects into raw TiKV split keys. This seam owns
//! the separate control-plane effect: memcomparable-encoding those keys for
//! PD, then asking the process authority's existing client to split and
//! scatter them. Keeping the effect behind a trait makes the SQL routing and
//! result shape testable without a cluster.

use std::time::{Duration, Instant};

use tidb_executor::SplitRegionPlan;
use tidb_pd_client::PdClient;

const SPLIT_REGION_TIMEOUT: Duration = Duration::from_secs(300);
const INITIAL_SCATTER_POLL_DELAY: Duration = Duration::from_millis(2);
const MAX_SCATTER_POLL_DELAY: Duration = Duration::from_millis(500);
const MAX_OPERATOR_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Client-visible outcome of one `SPLIT TABLE` execution.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SplitRegionOutcome {
    /// Number of newly split regions reported by PD.
    pub total_split_regions: u64,
    /// Fraction of the requested scatter work PD reports complete.
    pub scatter_finish_ratio: f64,
}

/// Process-level capability used by `SPLIT TABLE` statements.
pub trait ClusterRegionAdmin: Send + Sync {
    /// Splits and scatters all raw keys in one resolved plan.
    fn split_and_scatter(&self, plan: &SplitRegionPlan) -> Result<SplitRegionOutcome, String>;
}

/// Production region-management capability over the node's shared PD worker.
pub struct RealClusterRegionAdmin {
    pd: PdClient,
}

impl RealClusterRegionAdmin {
    /// Binds region management to an already-running PD client handle.
    #[must_use]
    pub const fn new(pd: PdClient) -> Self {
        Self { pd }
    }
}

impl ClusterRegionAdmin for RealClusterRegionAdmin {
    fn split_and_scatter(&self, plan: &SplitRegionPlan) -> Result<SplitRegionOutcome, String> {
        // Region metadata carries memcomparable-encoded boundaries. Go's
        // TiKV split path performs that conversion before PD observes the
        // regions; the aggregate PD RPC requires it from its caller. It also
        // expects the equivalent ordered key set.
        let split_keys = pd_split_keys(plan);
        let deadline = Instant::now() + SPLIT_REGION_TIMEOUT;
        let response = self
            .pd
            .split_and_scatter_regions_with_timeout(
                &split_keys,
                &plan.table_id.to_string(),
                // Go's default `tidb_wait_split_region_timeout`.
                SPLIT_REGION_TIMEOUT,
            )
            .map_err(|error| error.to_string())?;
        let total_split_regions = response.region_ids.len();
        let finished_scatter_regions = if response.scatter_finished_percentage >= 100 {
            total_split_regions
        } else {
            self.wait_scatter_region_finish(&response.region_ids, deadline)
        };
        Ok(SplitRegionOutcome {
            total_split_regions: total_split_regions as u64,
            scatter_finish_ratio: if total_split_regions == 0 {
                0.0
            } else {
                finished_scatter_regions as f64 / total_split_regions as f64
            },
        })
    }
}

impl RealClusterRegionAdmin {
    /// Go `waitScatterRegionFinish`: poll each returned region's PD operator
    /// until its `scatter-region` operator stops running, sharing the same
    /// statement-wide deadline used for the split itself. A transient PD
    /// error takes the same bounded retry path as a still-running operator;
    /// only regions observed complete contribute to the returned ratio.
    fn wait_scatter_region_finish(&self, region_ids: &[u64], deadline: Instant) -> usize {
        let mut finished = 0;
        for region_id in region_ids {
            let mut delay = INITIAL_SCATTER_POLL_DELAY;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let probe_timeout = remaining.min(MAX_OPERATOR_RPC_TIMEOUT);
                match self
                    .pd
                    .is_region_scattering_with_timeout(*region_id, probe_timeout)
                {
                    Ok(false) => {
                        finished += 1;
                        break;
                    }
                    Ok(true) | Err(_) => {}
                }

                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                std::thread::sleep(delay.min(remaining));
                delay = delay.saturating_mul(2).min(MAX_SCATTER_POLL_DELAY);
            }
        }
        finished
    }
}

fn pd_split_keys(plan: &SplitRegionPlan) -> Vec<Vec<u8>> {
    let mut split_keys = plan
        .split_keys
        .iter()
        .map(|key| {
            let mut encoded = Vec::with_capacity(tidb_codec::encoded_bytes_len(key.len()));
            tidb_codec::encode_bytes(&mut encoded, key);
            encoded
        })
        .collect::<Vec<_>>();
    split_keys.sort_unstable();
    split_keys.dedup();
    split_keys
}

pub(super) struct UnsupportedClusterRegionAdmin;

impl ClusterRegionAdmin for UnsupportedClusterRegionAdmin {
    fn split_and_scatter(&self, _plan: &SplitRegionPlan) -> Result<SplitRegionOutcome, String> {
        Err("this server has no region-management authority".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::pd_split_keys;
    use tidb_executor::SplitRegionPlan;

    fn pd_key(raw: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        tidb_codec::encode_bytes(&mut encoded, raw);
        encoded
    }

    #[test]
    fn pd_receives_encoded_deduplicated_split_keys_in_key_order() {
        let plan = SplitRegionPlan {
            table_id: 42,
            split_keys: vec![b"end".to_vec(), b"a".to_vec(), b"end".to_vec()],
        };
        assert_eq!(pd_split_keys(&plan), [pd_key(b"a"), pd_key(b"end")]);
    }
}
