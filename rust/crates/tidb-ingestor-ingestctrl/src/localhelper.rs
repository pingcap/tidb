// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/localhelper.go`.
use crate::{Error, Result, rate_limiter::IngestLimiter};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tidb_gocontext::Context;
use tidb_proto::{import_sstpb as sst, metapb};

/// Go `coarseGrainedSplitKeysThreshold`.
pub const COARSE_GRAINED_SPLIT_KEYS_THRESHOLD: usize = 64;
/// Go `split.SplitClient` operations used by `Backend`.
pub trait SplitClient: Send + Sync {
    /// Go `SplitClient.SplitKeysAndScatter`.
    fn split_keys_and_scatter(&self, ctx: &Context, keys: &[Vec<u8>]) -> Result<Vec<u64>>;
    /// Go `SplitClient.WaitRegionsScattered`.
    fn wait_regions_scattered(&self, ctx: &Context, regions: &[u64]) -> Result<usize>;
}
/// Go `getCoarseGrainedSplitKeys`.
pub fn get_coarse_grained_split_keys(keys: &[Vec<u8>]) -> Vec<Vec<u8>> {
    if keys.is_empty() {
        return Vec::new();
    }
    let stride = (keys.len() as f64).sqrt() as usize;
    let mut result: Vec<_> = keys.iter().step_by(stride).cloned().collect();
    if (keys.len() - 1) % stride != 0 {
        result.push(keys.last().unwrap().clone());
    }
    result
}
/// Go `Backend.splitAndScatterRegionInBatches`.
pub fn split_and_scatter_region_in_batches(
    ctx: &Context,
    client: &dyn SplitClient,
    keys: &[Vec<u8>],
    mut batch_count: usize,
    max_per_sec: f64,
) -> Result<()> {
    if batch_count == 0 {
        return Err(Error::Other("split batch count must be positive".into()));
    }
    let limiter = IngestLimiter::new(ctx.clone(), 0, max_per_sec);
    if max_per_sec > 0.0 {
        batch_count = batch_count.min(limiter.burst());
    }
    if keys.len() > COARSE_GRAINED_SPLIT_KEYS_THRESHOLD {
        split_and_scatter_region_in_batches_with_limiter(
            ctx,
            client,
            &get_coarse_grained_split_keys(keys),
            batch_count,
            &limiter,
        )?;
    }
    split_and_scatter_region_in_batches_with_limiter(ctx, client, keys, batch_count, &limiter)
}
/// Go `Backend.splitAndScatterRegionInBatchesWithLimiter`.
fn split_and_scatter_region_in_batches_with_limiter(
    ctx: &Context,
    client: &dyn SplitClient,
    keys: &[Vec<u8>],
    batch_count: usize,
    limiter: &IngestLimiter,
) -> Result<()> {
    for batch in keys.chunks(batch_count) {
        limiter
            .acquire(0, batch.len())
            .map_err(|e| Error::Other(e.to_string()))?;
        split_and_scatter_region_by_ranges(ctx, client, batch)?;
    }
    Ok(())
}
/// Go `Backend.splitAndScatterRegionByRanges`: split errors abort; waiting for
/// scatter is best effort, as regions can be imported before scattering ends.
pub fn split_and_scatter_region_by_ranges(
    ctx: &Context,
    client: &dyn SplitClient,
    keys: &[Vec<u8>],
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let regions = client.split_keys_and_scatter(ctx, keys)?;
    let _ = client.wait_regions_scattered(ctx, &regions);
    Ok(())
}
/// Go `beforeEnd`.
pub fn before_end(key: &[u8], end: &[u8]) -> bool {
    end.is_empty() || key < end
}
/// Go `keyInsideRegion`.
pub fn key_inside_region(region: &metapb::Region, key: &[u8]) -> bool {
    key >= region.start_key.as_slice() && before_end(key, &region.end_key)
}
/// Go `insideRegion` (SST bounds are inclusive).
pub fn inside_region(region: &metapb::Region, metas: &[sst::SstMeta]) -> bool {
    metas.iter().all(|meta| {
        let range = meta.range.clone().unwrap_or_default();
        key_inside_region(region, &range.start) && key_inside_region(region, &range.end)
    })
}
/// Go `largerStartKey`.
pub fn larger_start_key<'a>(a: &'a [u8], b: &'a [u8]) -> &'a [u8] {
    a.max(b)
}

struct Bucket {
    tokens: f64,
    last: Instant,
}
struct WriteLimitState {
    limit: i64,
    burst: i64,
    stores: HashMap<u64, Bucket>,
}
/// Go `storeWriteLimiter`: independent token buckets, updated in place.
pub struct StoreWriteLimiter {
    state: Mutex<WriteLimitState>,
}
/// Go `calculateLimitAndBurst`.
pub fn calculate_limit_and_burst(limit: i64) -> (i64, i64) {
    if limit <= 0 {
        (0, 0)
    } else {
        (limit, limit.saturating_add(limit / 5))
    }
}
impl StoreWriteLimiter {
    /// Go `newStoreWriteLimiter`.
    pub fn new(limit: i64) -> Self {
        let (limit, burst) = calculate_limit_and_burst(limit);
        Self {
            state: Mutex::new(WriteLimitState {
                limit,
                burst,
                stores: HashMap::new(),
            }),
        }
    }
    /// Go `storeWriteLimiter.Limit`.
    pub fn limit(&self) -> i64 {
        self.state.lock().unwrap().limit
    }
    /// Go `storeWriteLimiter.UpdateLimit`.
    pub fn update_limit(&self, new_limit: i64) {
        let (limit, burst) = calculate_limit_and_burst(new_limit);
        let mut state = self.state.lock().unwrap();
        if state.limit == limit {
            return;
        }
        let now = Instant::now();
        let old_limit = state.limit as f64;
        let old_burst = state.burst as f64;
        for bucket in state.stores.values_mut() {
            bucket.tokens = (bucket.tokens
                + now.duration_since(bucket.last).as_secs_f64() * old_limit)
                .min(old_burst);
            bucket.last = now;
        }
        state.limit = limit;
        state.burst = burst;
        if limit == 0 {
            state.stores.clear();
        }
    }
    /// Go `storeWriteLimiter.WaitN`: requests above burst are split into reservations.
    pub fn wait_n(&self, ctx: &Context, store_id: u64, mut n: usize) -> Result<()> {
        loop {
            let (delay, weight) = {
                let mut state = self.state.lock().unwrap();
                if state.limit == 0 {
                    return Ok(());
                }
                if let Some(e) = ctx.err() {
                    return Err(Error::Other(e.to_string()));
                }
                let limit = state.limit as f64;
                let burst = state.burst as f64;
                let weight = n.min(state.burst as usize);
                let now = Instant::now();
                let bucket = state.stores.entry(store_id).or_insert(Bucket {
                    tokens: burst,
                    last: now,
                });
                bucket.tokens = (bucket.tokens
                    + now.duration_since(bucket.last).as_secs_f64() * limit)
                    .min(burst)
                    - weight as f64;
                bucket.last = now;
                (
                    Duration::from_secs_f64((-bucket.tokens / limit).max(0.0)),
                    weight,
                )
            };
            if !delay.is_zero() && ctx.sleep(delay) {
                let mut state = self.state.lock().unwrap();
                let limit = state.limit as f64;
                let burst = state.burst as f64;
                if let Some(bucket) = state.stores.get_mut(&store_id) {
                    let now = Instant::now();
                    bucket.tokens = (bucket.tokens
                        + now.duration_since(bucket.last).as_secs_f64() * limit
                        + weight as f64)
                        .min(burst);
                    bucket.last = now;
                }
                return Err(Error::Other(ctx.err().unwrap().to_string()));
            }
            n -= weight;
            if n == 0 {
                return Ok(());
            }
        }
    }
}
/// Go `EstimateCompactionThreshold2`.
pub fn estimate_compaction_threshold2(total_raw_file_size: i64) -> i64 {
    let threshold = (total_raw_file_size.max(0) as u64 / 512).next_power_of_two();
    threshold.clamp(512 << 20, 32 << 30) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Split {
        calls: AtomicUsize,
        fail: bool,
    }
    impl SplitClient for Split {
        /// Go `SplitClient.SplitKeysAndScatter`.
        fn split_keys_and_scatter(&self, _: &Context, _: &[Vec<u8>]) -> Result<Vec<u64>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(Error::Other("mock split error".into()))
            } else {
                Ok(vec![1])
            }
        }
        /// Go `SplitClient.WaitRegionsScattered`.
        fn wait_regions_scattered(&self, _: &Context, _: &[u64]) -> Result<usize> {
            Ok(0)
        }
    }
    /// Go `TestGetCoarseGrainedSplitKeys key fixture`.
    fn keys(n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| (i as u16).to_be_bytes().to_vec()).collect()
    }
    /// Go `TestSplitAndScatterRegionInBatchesTwoLevel`, all four subcases.
    #[test]
    fn test_split_and_scatter_region_in_batches_two_level() {
        for (n, fail, rate, expected) in [
            (121, false, 0.0, 4),
            (64, false, 0.0, 2),
            (121, true, 0.0, 1),
            (121, false, 0.5, 1),
        ] {
            let client = Split {
                calls: AtomicUsize::new(0),
                fail,
            };
            let (ctx, _cancel) =
                Context::with_timeout(&Context::background(), Duration::from_millis(100));
            let result = split_and_scatter_region_in_batches(&ctx, &client, &keys(n), 50, rate);
            assert_eq!(result.is_err(), fail || rate > 0.0);
            if fail {
                assert!(result.unwrap_err().to_string().contains("mock split error"));
            } else if rate > 0.0 {
                assert!(result.unwrap_err().to_string().contains("context deadline"));
            }
            assert_eq!(client.calls.load(Ordering::SeqCst), expected);
        }
    }
    /// Go `TestGetCoarseGrainedSplitKeys`.
    #[test]
    fn test_get_coarse_grained_split_keys() {
        let keys = keys(122);
        assert_eq!(
            get_coarse_grained_split_keys(&keys)
                .iter()
                .filter(|k| **k == keys[121])
                .count(),
            1
        );
    }
    /// Go `TestStoreWriteLimiter`: saturating burst, above-burst requests, independent stores.
    #[test]
    fn test_store_write_limiter() {
        let ctx = Context::background();
        StoreWriteLimiter::new(i64::MAX)
            .wait_n(&ctx, 1, 1024)
            .unwrap();
        assert_eq!(calculate_limit_and_burst(i64::MAX), (i64::MAX, i64::MAX));
        let limiter = StoreWriteLimiter::new(100);
        let start = Instant::now();
        limiter.wait_n(&ctx, 1, 240).unwrap();
        assert!(start.elapsed() > Duration::from_secs(1));
        check_store_rates(&StoreWriteLimiter::new(100), 100);
    }
    /// Go `TestStoreWriteLimiter rate assertions`.
    fn check_store_rates(limiter: &StoreWriteLimiter, limit: usize) {
        std::thread::scope(|scope| {
            for store in 0..10 {
                scope.spawn(move || {
                    let (ctx, _cancel) =
                        Context::with_timeout(&Context::background(), Duration::from_millis(200));
                    let start = Instant::now();
                    let mut tokens = 0;
                    while limiter.wait_n(&ctx, store, 13).is_ok() {
                        tokens += 13;
                    }
                    assert!(
                        tokens
                            <= limit
                                + limit / 5
                                + (start.elapsed().as_secs_f64() * limit as f64) as usize
                                + 1
                    );
                });
            }
        });
    }
    /// Go `TestTuneStoreWriteLimiter`: updates existing store buckets.
    #[test]
    fn test_tune_store_write_limiter() {
        let limiter = StoreWriteLimiter::new(100);
        check_store_rates(&limiter, 100);
        limiter.update_limit(200);
        assert_eq!(limiter.limit(), 200);
        check_store_rates(&limiter, 200);
        limiter.update_limit(0);
        limiter
            .wait_n(&Context::background(), 0, usize::MAX)
            .unwrap();
        limiter.update_limit(100);
        limiter.wait_n(&Context::background(), 0, 120).unwrap();
    }
}
