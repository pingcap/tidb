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

//! Go `br/pkg/restore/split/split.go`: the region splitter and the region
//! scans with their consistency checks.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use tidb_gocontext::Context;
use tidb_log::{Field, Value};

use super::client::SplitClient;
use super::region::RegionInfo;
use crate::errors::{self, Error, Result};
use crate::utils::backoff::{BackoffStrategy, RetryState};

/// Go `WaitRegionOnlineAttemptTimes` (a package var tests lower), initially
/// lightning's `config.DefaultRegionCheckBackoffLimit`.
pub static WAIT_REGION_ONLINE_ATTEMPT_TIMES: AtomicI32 = AtomicI32::new(1800);
/// Go `SplitRetryTimes` (a package var tests lower).
pub static SPLIT_RETRY_TIMES: AtomicI32 = AtomicI32::new(150);
/// Go `SplitRetryInterval`.
pub const SPLIT_RETRY_INTERVAL: Duration = Duration::from_millis(50);
/// Go `SplitMaxRetryInterval`.
pub const SPLIT_MAX_RETRY_INTERVAL: Duration = Duration::from_secs(4);
/// Go `ScatterWaitUpperInterval`.
pub const SCATTER_WAIT_UPPER_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// Go `ScanRegionPaginationLimit`.
pub const SCAN_REGION_PAGINATION_LIMIT: usize = 128;
/// Go `DefaultRegionIndexStep`.
pub const DEFAULT_REGION_INDEX_STEP: usize = 128;

/// Go `ErrBackoff`.
pub static ERR_BACKOFF: LazyLock<Error> = LazyLock::new(|| errors::errorf("found backoff error"));
/// Go `ErrBackoffAndDontCount`.
pub static ERR_BACKOFF_AND_DONT_COUNT: LazyLock<Error> =
    LazyLock::new(|| errors::errorf("found backoff error but don't count"));

/// Go `NormalizeRegionIndexStep`.
#[must_use]
pub fn normalize_region_index_step(step: usize) -> usize {
    if step == 0 {
        DEFAULT_REGION_INDEX_STEP
    } else {
        step
    }
}

/// Go `RegionSplitter`.
pub struct RegionSplitter<C: SplitClient> {
    client: C,
    region_index_step: usize,
    coarse_scatter: bool,
}

impl<C: SplitClient> RegionSplitter<C> {
    /// Go `NewRegionSplitterWithRegionIndexStep`.
    #[must_use]
    pub fn new(client: C, region_index_step: usize) -> Self {
        Self {
            client,
            region_index_step: normalize_region_index_step(region_index_step),
            coarse_scatter: false,
        }
    }

    /// Go `SetCoarseScatter`.
    pub fn set_coarse_scatter(&mut self, coarse_scatter: bool) {
        self.coarse_scatter = coarse_scatter;
    }

    /// The split client.
    #[must_use]
    pub fn client(&self) -> &C {
        &self.client
    }

    /// Go `ExecuteSortedKeys`: splits at each key, roughly first (every
    /// `regionIndexStep`th key, scattered), then at all of them.
    ///
    /// # Errors
    /// The split's error.
    pub fn execute_sorted_keys(&self, ctx: &Context, sorted_split_keys: &[Vec<u8>]) -> Result<()> {
        if sorted_split_keys.is_empty() {
            tidb_log::info("skip split regions, no split keys", &[]);
            return Ok(());
        }
        tidb_log::info(
            "execute split sorted keys",
            &[Field::new(
                "keys count",
                Value::I64(sorted_split_keys.len() as i64),
            )],
        );
        self.execute_split_by_ranges(ctx, sorted_split_keys)
    }

    /// Go `executeSplitByRanges`.
    fn execute_split_by_ranges(&self, ctx: &Context, sorted_keys: &[Vec<u8>]) -> Result<()> {
        let start = Instant::now();
        let mut rough = Vec::new();
        if self.region_index_step < sorted_keys.len() {
            let mut i = self.region_index_step;
            while i < sorted_keys.len() {
                rough.push(sorted_keys[i].clone());
                i += self.region_index_step;
            }
        }
        if !rough.is_empty() {
            self.execute_split_by_keys(ctx, &rough, true)?;
        }
        tidb_log::info(
            "finish spliting regions roughly",
            &[Field::new(
                "take",
                Value::Duration(start.elapsed().as_nanos() as i64),
            )],
        );
        self.execute_split_by_keys(ctx, sorted_keys, !self.coarse_scatter)?;
        tidb_log::info(
            "finish spliting and scattering regions",
            &[Field::new(
                "take",
                Value::Duration(start.elapsed().as_nanos() as i64),
            )],
        );
        Ok(())
    }

    /// Go `executeSplitByKeys`.
    fn execute_split_by_keys(
        &self,
        ctx: &Context,
        sorted_keys: &[Vec<u8>],
        scatter: bool,
    ) -> Result<()> {
        let start = Instant::now();
        let scatter_regions = if scatter {
            self.client.split_keys_and_scatter(ctx, sorted_keys)?
        } else {
            self.client.split_keys(ctx, sorted_keys)?
        };
        if scatter_regions.is_empty() {
            tidb_log::info(
                "finish splitting regions.",
                &[Field::new(
                    "take",
                    Value::Duration(start.elapsed().as_nanos() as i64),
                )],
            );
        } else {
            tidb_log::info(
                "finish splitting and scattering regions. and starts to wait",
                &[
                    Field::new("regions", Value::I64(scatter_regions.len() as i64)),
                    Field::new("take", Value::Duration(start.elapsed().as_nanos() as i64)),
                ],
            );
            self.wait_regions_scattered(ctx, &scatter_regions, SCATTER_WAIT_UPPER_INTERVAL);
        }
        Ok(())
    }

    /// Go `waitRegionsScattered`.
    fn wait_regions_scattered(&self, ctx: &Context, regions: &[RegionInfo], timeout: Duration) {
        tidb_log::info(
            "start to wait for scattering regions",
            &[Field::new("regions", Value::I64(regions.len() as i64))],
        );
        let start = Instant::now();
        let left = self.wait_for_scatter_regions_timeout(ctx, regions, timeout);
        let fields = |extra: Vec<Field>| {
            let mut f = extra;
            f.push(Field::new("regions", Value::I64(regions.len() as i64)));
            f.push(Field::new(
                "take",
                Value::Duration(start.elapsed().as_nanos() as i64),
            ));
            f
        };
        if left == 0 {
            tidb_log::info("waiting for scattering regions done", &fields(vec![]));
        } else {
            tidb_log::warn(
                "waiting for scattering regions timeout",
                &fields(vec![Field::new(
                    "not scattered Count",
                    Value::I64(left as i64),
                )]),
            );
        }
    }

    /// Go `WaitForScatterRegionsTimeout`: how many regions are left.
    pub fn wait_for_scatter_regions_timeout(
        &self,
        ctx: &Context,
        regions: &[RegionInfo],
        timeout: Duration,
    ) -> usize {
        let (ctx2, cancel) = Context::with_timeout(ctx, timeout);
        let (left, _) = self.client.wait_regions_scattered(&ctx2, regions);
        cancel.cancel();
        left
    }
}

fn key(k: &[u8]) -> String {
    tidb_redact::key(k)
}

fn epoch_text(r: &RegionInfo) -> String {
    let e = r.region.region_epoch.unwrap_or_default();
    format!("conf_ver:{} version:{} ", e.conf_ver, e.version)
}

fn batch_scan_err(msg: String) -> Error {
    errors::annotatef(&errors::ERR_PD_BATCH_SCAN_REGION, msg)
}

/// Go `checkRegionConsistency`: the regions cover `[start, end)` (only the
/// start when `limited`), each with a leader, without gaps.
///
/// # Errors
/// [`errors::ERR_PD_BATCH_SCAN_REGION`] for the first violation.
pub fn check_region_consistency(
    start_key: &[u8],
    end_key: &[u8],
    regions: &[RegionInfo],
    limited: bool,
) -> Result<()> {
    let Some(first) = regions.first() else {
        return Err(batch_scan_err(format!(
            "scan region return empty result, startKey: {}, endKey: {}",
            key(start_key),
            key(end_key)
        )));
    };
    let last = regions.last().unwrap();
    if first.region.start_key.as_slice() > start_key {
        return Err(batch_scan_err(format!(
            "first region {}'s startKey({}) > startKey({}), region epoch: {}",
            first.region.id,
            key(&first.region.start_key),
            key(start_key),
            epoch_text(first)
        )));
    } else if !limited
        && !last.region.end_key.is_empty()
        && last.region.end_key.as_slice() < end_key
    {
        return Err(batch_scan_err(format!(
            "last region {}'s endKey({}) < endKey({}), region epoch: {}",
            last.region.id,
            key(&last.region.end_key),
            key(end_key),
            epoch_text(last)
        )));
    }
    let leader_ok = |r: &RegionInfo| -> Result<()> {
        match &r.leader {
            None => Err(batch_scan_err(format!(
                "region {}'s leader is nil",
                r.region.id
            ))),
            Some(l) if l.store_id == 0 => Err(batch_scan_err(format!(
                "region {}'s leader's store id is 0",
                r.region.id
            ))),
            Some(_) => Ok(()),
        }
    };
    leader_ok(first)?;
    let mut cur = first;
    for r in &regions[1..] {
        leader_ok(r)?;
        if cur.region.end_key != r.region.start_key {
            return Err(batch_scan_err(format!(
                "region {}'s endKey not equal to next region {}'s startKey, endKey: {}, startKey: {}, region epoch: {} {}",
                cur.region.id,
                r.region.id,
                key(&cur.region.end_key),
                key(&r.region.start_key),
                epoch_text(cur),
                epoch_text(r)
            )));
        }
        cur = r;
    }
    Ok(())
}

/// Go `utils.WithRetry` over a backoffer the attempts also touch.
fn retry_with<B: BackoffStrategy>(
    ctx: &Context,
    bo: &mut B,
    mut f: impl FnMut(&mut B) -> Result<()>,
) {
    while bo.remaining_attempts() > 0 {
        let Err(err) = f(bo) else { return };
        let backoff = bo.next_backoff(&err);
        if ctx.sleep(backoff) {
            return;
        }
    }
}

/// Go `scanRegionsLimitWithRetry`: the last attempt's batch and error. PD
/// answers from its leader here, so Go's follower-handle option has
/// nothing to choose.
fn scan_regions_limit_with_retry(
    ctx: &Context,
    client: &dyn SplitClient,
    start_key: &[u8],
    end_key: &[u8],
    limit: usize,
) -> Result<Vec<RegionInfo>> {
    let mut batch = Vec::new();
    let mut last_err: Option<Error> = None;
    retry_with(ctx, &mut WaitRegionOnlineBackoffer::new(), |_| {
        let res = client
            .scan_regions(ctx, start_key, end_key, limit)
            .and_then(|b| {
                batch = b;
                check_region_consistency(start_key, end_key, &batch, true).inspect_err(|err| {
                    tidb_log::warn(
                        "failed to scan region, retrying",
                        &[
                            Field::new("error", Value::Str(err.to_string())),
                            Field::new("regionLength", Value::I64(batch.len() as i64)),
                        ],
                    );
                })
            });
        last_err = res.as_ref().err().cloned();
        res
    });
    last_err.map_or(Ok(batch), Err)
}

/// Go `PaginateScanRegion`: the regions of `[start, end)` in pages of
/// `limit`, retried until they are consistent.
///
/// # Errors
/// [`errors::ERR_INVALID_RANGE`], or the last attempt's error.
pub fn paginate_scan_region(
    ctx: &Context,
    client: &dyn SplitClient,
    start_key: &[u8],
    end_key: &[u8],
    limit: usize,
) -> Result<Vec<RegionInfo>> {
    if !end_key.is_empty() && start_key > end_key {
        return Err(errors::annotatef(
            &errors::ERR_INVALID_RANGE,
            format!(
                "startKey > endKey, startKey: {}, endkey: {}",
                key(start_key),
                key(end_key)
            ),
        ));
    }
    let mut last_regions: Vec<RegionInfo> = Vec::new();
    let mut last_err: Option<Error> = None;
    retry_with(ctx, &mut WaitRegionOnlineBackoffer::new(), |backoffer| {
        let mut regions = Vec::with_capacity(16);
        let mut scan_start = start_key.to_vec();
        loop {
            let batch =
                match scan_regions_limit_with_retry(ctx, client, &scan_start, end_key, limit) {
                    Ok(b) => b,
                    Err(err) => {
                        let e = errors::annotatef(
                            &errors::ERR_PD_BATCH_SCAN_REGION,
                            format!(
                                "scan regions from start-key:{}, err: {err}",
                                key(&scan_start)
                            ),
                        );
                        last_err = Some(Arc::clone(&e));
                        return Err(e);
                    }
                };
            let n = batch.len();
            regions.extend(batch);
            if n < limit {
                break;
            }
            scan_start = regions.last().unwrap().region.end_key.clone();
            if scan_start.is_empty() || (!end_key.is_empty() && scan_start.as_slice() >= end_key) {
                break;
            }
        }
        // A changed region count means TiKV made progress: the attempt is
        // not counted.
        if regions.len() != last_regions.len() {
            backoffer.stat.reduce_retry();
        }
        last_regions = regions;
        let res =
            check_region_consistency(start_key, end_key, &last_regions, false).inspect_err(|err| {
                tidb_log::warn(
                    "failed to scan region, retrying",
                    &[
                        Field::new("error", Value::Str(err.to_string())),
                        Field::new("regionLength", Value::I64(last_regions.len() as i64)),
                    ],
                );
            });
        last_err = res.as_ref().err().cloned();
        res
    });
    last_err.map_or(Ok(last_regions), Err)
}

/// Go `WaitRegionOnlineBackoffer`: backs off on region scan errors only.
pub struct WaitRegionOnlineBackoffer {
    /// Go `Stat`.
    pub stat: RetryState,
}

impl WaitRegionOnlineBackoffer {
    /// Go `NewWaitRegionOnlineBackoffer`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stat: RetryState::new(
                WAIT_REGION_ONLINE_ATTEMPT_TIMES.load(Ordering::Relaxed),
                Duration::from_millis(10),
                Duration::from_secs(2),
            ),
        }
    }
}

impl Default for WaitRegionOnlineBackoffer {
    fn default() -> Self {
        Self::new()
    }
}

impl BackoffStrategy for WaitRegionOnlineBackoffer {
    fn next_backoff(&mut self, err: &Error) -> Duration {
        if errors::is(err.as_ref(), &errors::ERR_PD_BATCH_SCAN_REGION) {
            let mut delay = self.stat.exponential_backoff();
            if tidb_failpoint::eval(
                "github.com/pingcap/tidb/br/pkg/restore/split/hint-scan-region-backoff",
            )
            .is_some_and(|v| v.bool())
            {
                delay = Duration::from_micros(1);
            }
            return delay;
        }
        self.stat.give_up();
        Duration::ZERO
    }

    fn remaining_attempts(&self) -> i32 {
        BackoffStrategy::remaining_attempts(&self.stat)
    }
}

/// Go `BackoffMayNotCountBackoffer`: [`ERR_BACKOFF`] counts an attempt,
/// [`ERR_BACKOFF_AND_DONT_COUNT`] does not, anything else gives up.
pub struct BackoffMayNotCountBackoffer {
    state: RetryState,
}

impl BackoffMayNotCountBackoffer {
    /// Go `NewBackoffMayNotCountBackoffer`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: RetryState::new(
                WAIT_REGION_ONLINE_ATTEMPT_TIMES.load(Ordering::Relaxed),
                Duration::from_millis(10),
                Duration::from_secs(2),
            ),
        }
    }
}

impl Default for BackoffMayNotCountBackoffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Go `errors.ErrorEqual(err, sentinel)` after `errors.Annotate`: the
/// causes' texts are equal.
fn cause_is(err: &Error, sentinel: &Error) -> bool {
    errors::cause(err.as_ref()).to_string() == sentinel.to_string()
}

impl BackoffStrategy for BackoffMayNotCountBackoffer {
    fn next_backoff(&mut self, err: &Error) -> Duration {
        if cause_is(err, &ERR_BACKOFF) {
            return self.state.exponential_backoff();
        }
        if cause_is(err, &ERR_BACKOFF_AND_DONT_COUNT) {
            let delay = self.state.exponential_backoff();
            self.state.reduce_retry();
            return delay;
        }
        self.state.give_up();
        Duration::ZERO
    }

    fn remaining_attempts(&self) -> i32 {
        BackoffStrategy::remaining_attempts(&self.state)
    }
}

/// Go `getSplitKeysOfRegions`: each key in the region it falls inside (not
/// at a region's start). Keys are memcomparable-encoded to compare with
/// region bounds.
#[must_use]
pub fn get_split_keys_of_regions(
    sorted_keys: &[Vec<u8>],
    sorted_regions: &[RegionInfo],
) -> Vec<(RegionInfo, Vec<Vec<u8>>)> {
    let enc = |k: &[u8]| {
        let mut b = Vec::new();
        tidb_codec::encode_bytes(&mut b, k);
        b
    };
    let mut out: Vec<(RegionInfo, Vec<Vec<u8>>)> = Vec::new();
    if sorted_keys.is_empty() {
        return out;
    }
    let mut cur = 0;
    let mut split_key = enc(&sorted_keys[cur]);
    'regions: for region in sorted_regions {
        loop {
            let skip = sorted_keys[cur].is_empty() || split_key == region.region.start_key;
            if !skip {
                if !region.contains_interior(&split_key) {
                    break;
                }
                match out.last_mut() {
                    Some((r, keys)) if r.region.id == region.region.id => {
                        keys.push(sorted_keys[cur].clone())
                    }
                    _ => out.push((region.clone(), vec![sorted_keys[cur].clone()])),
                }
            }
            cur += 1;
            if cur >= sorted_keys.len() {
                break 'regions;
            }
            split_key = enc(&sorted_keys[cur]);
        }
    }
    if cur < sorted_keys.len() {
        let last_key = sorted_keys.last().unwrap();
        let end = sorted_regions
            .last()
            .map(|r| r.region.end_key.clone())
            .unwrap_or_default();
        if *last_key != end {
            tidb_log::error(
                "in getSplitKeysOfRegions, regions don't cover all keys",
                &[
                    Field::new("firstKey", Value::Str(key(&sorted_keys[0]))),
                    Field::new("lastKey", Value::Str(key(last_key))),
                    Field::new(
                        "firstRegionStartKey",
                        Value::Str(key(sorted_regions
                            .first()
                            .map_or(&[][..], |r| r.region.start_key.as_slice()))),
                    ),
                    Field::new("lastRegionEndKey", Value::Str(key(&end))),
                ],
            );
        }
    }
    out
}
