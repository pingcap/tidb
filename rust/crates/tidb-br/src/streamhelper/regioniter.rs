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


//! Go `streamhelper/regioniter.go`: paging through the regions of a range.

use std::time::Duration;

use tidb_gocontext::Context;
use tidb_proto::metapb;

use crate::errors::{self, Result, ERR_PD_BATCH_SCAN_REGION};
use crate::utils::backoff::RetryState;

/// Go `defaultPageSize`.
const DEFAULT_PAGE_SIZE: usize = 2048;

/// Go `RegionWithLeader`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RegionWithLeader {
    /// Go `Region`.
    pub region: metapb::Region,
    /// Go `Leader`.
    pub leader: metapb::Peer,
}

/// Go `Store`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Store {
    /// Go `ID`.
    pub id: u64,
    /// Go `BootAt`.
    pub boot_at: u64,
}

/// Go `TiKVClusterMeta`.
pub trait TiKVClusterMeta: Send + Sync {
    /// Go `RegionScan`: at most `limit` regions from the one holding `key`.
    ///
    /// # Errors
    /// PD's error.
    fn region_scan(&self, ctx: &Context, key: &[u8], end_key: &[u8], limit: usize) -> Result<Vec<RegionWithLeader>>;
    /// Go `Stores`.
    ///
    /// # Errors
    /// PD's error.
    fn stores(&self, ctx: &Context) -> Result<Vec<Store>>;
    /// Go `BlockGCUntil`: the service safe point at `at`; returns it.
    ///
    /// # Errors
    /// PD's error, or another service's safe point is above `at`.
    fn block_gc_until(&self, ctx: &Context, at: u64) -> Result<u64>;
    /// Go `UnblockGC`.
    ///
    /// # Errors
    /// PD's error.
    fn unblock_gc(&self, ctx: &Context) -> Result<()>;
    /// Go `FetchCurrentTS`.
    ///
    /// # Errors
    /// PD's error.
    fn fetch_current_ts(&self, ctx: &Context) -> Result<u64>;
}

fn redact(key: &[u8]) -> String {
    tidb_redact::key(key)
}

/// Go `kv.Key.Next`.
fn key_next(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}

/// Go `locateKeyOfRegion`.
///
/// # Errors
/// PD's error, or no region holds the key.
pub fn locate_key_of_region(ctx: &Context, cli: &dyn TiKVClusterMeta, key: &[u8]) -> Result<RegionWithLeader> {
    let regions = cli.region_scan(ctx, key, &key_next(key), 1)?;
    regions.into_iter().next().ok_or_else(|| {
        errors::annotatef(&ERR_PD_BATCH_SCAN_REGION, format!("scanning the key {} returns empty region", redact(key)))
    })
}

/// Go `CheckRegionConsistency`.
///
/// # Errors
/// [`ERR_PD_BATCH_SCAN_REGION`] for a hole or an empty result.
pub fn check_region_consistency(start_key: &[u8], end_key: &[u8], regions: &[RegionWithLeader]) -> Result<()> {
    let (Some(first), Some(last)) = (regions.first(), regions.last()) else {
        return Err(errors::annotatef(
            &ERR_PD_BATCH_SCAN_REGION,
            format!("scan region return empty result, startKey: {}, endKey: {}", redact(start_key), redact(end_key)),
        ));
    };
    if first.region.start_key.as_slice() > start_key {
        return Err(errors::annotatef(
            &ERR_PD_BATCH_SCAN_REGION,
            format!(
                "first region's startKey > startKey, startKey: {}, regionStartKey: {}",
                redact(start_key),
                redact(&first.region.start_key)
            ),
        ));
    }
    if !last.region.end_key.is_empty() && last.region.end_key.as_slice() < end_key {
        return Err(errors::annotatef(
            &ERR_PD_BATCH_SCAN_REGION,
            format!(
                "last region's endKey < endKey, endKey: {}, regionEndKey: {}",
                redact(end_key),
                redact(&last.region.end_key)
            ),
        ));
    }
    for w in regions.windows(2) {
        if w[0].region.end_key != w[1].region.start_key {
            return Err(errors::annotatef(
                &ERR_PD_BATCH_SCAN_REGION,
                format!(
                    "region endKey not equal to next region startKey, endKey: {}, startKey: {}",
                    redact(&w[0].region.end_key),
                    redact(&w[1].region.start_key)
                ),
            ));
        }
    }
    Ok(())
}

/// Go `RegionIter`.
pub struct RegionIter<'a> {
    cli: &'a dyn TiKVClusterMeta,
    end_key: Vec<u8>,
    current_start_key: Vec<u8>,
    /// With an empty end key, whether the last region was reached.
    inf_scan_finished: bool,
    /// Go `PageSize`.
    pub page_size: usize,
}

/// Go `IterateRegion`.
#[must_use]
pub fn iterate_region<'a>(cli: &'a dyn TiKVClusterMeta, start_key: &[u8], end_key: &[u8]) -> RegionIter<'a> {
    RegionIter {
        cli,
        end_key: end_key.to_vec(),
        current_start_key: start_key.to_vec(),
        inf_scan_finished: false,
        page_size: DEFAULT_PAGE_SIZE,
    }
}

impl RegionIter<'_> {
    /// Go `Next`: the next page, retried 8 times 500ms apart.
    ///
    /// # Errors
    /// PD's error or an inconsistent page, after the retries.
    pub fn next(&mut self, ctx: &Context) -> Result<Vec<RegionWithLeader>> {
        let mut rs = Vec::new();
        let mut state = RetryState::new(8, Duration::from_millis(500), Duration::from_millis(500));
        crate::utils::retry::with_retry(
            ctx,
            || {
                let res = (|| {
                    let regions = self.cli.region_scan(ctx, &self.current_start_key, &self.end_key, self.page_size)?;
                    if let Some(last) = regions.last() {
                        check_region_consistency(&self.current_start_key, &last.region.end_key.clone(), &regions)?;
                        rs = regions;
                        return Ok(());
                    }
                    check_region_consistency(&self.current_start_key, &self.end_key, &regions)
                })();
                if let Err(err) = &res {
                    tidb_log::warn(
                        "failed with trying to scan regions",
                        &[
                            tidb_log::Field::new("error", tidb_log::Value::Str(err.to_string())),
                            tidb_log::Field::new("start", tidb_log::Value::Str(redact(&self.current_start_key))),
                            tidb_log::Field::new("end", tidb_log::Value::Str(redact(&self.end_key))),
                        ],
                    );
                }
                res
            },
            &mut state,
        )?;
        let end_key = rs.last().map(|r| r.region.end_key.clone()).unwrap_or_default();
        if end_key.is_empty() {
            self.inf_scan_finished = true;
        }
        self.current_start_key = end_key;
        Ok(rs)
    }

    /// Go `Done`.
    #[must_use]
    pub fn done(&self) -> bool {
        if self.end_key.is_empty() {
            return self.inf_scan_finished;
        }
        self.inf_scan_finished || self.current_start_key >= self.end_key
    }
}
