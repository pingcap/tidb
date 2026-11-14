// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/rate_limiter_param.go`.
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use tidb_meta::transaction::{MetaTransaction, Mutator};

/// Go `defaultMaxBatchSplitRanges`.
pub const DEFAULT_MAX_BATCH_SPLIT_RANGES: i64 = 2048;
/// Go `CurrentMaxBatchSplitRanges`.
pub static CURRENT_MAX_BATCH_SPLIT_RANGES: AtomicI64 = AtomicI64::new(0);
/// Go `CurrentMaxSplitRangesPerSec`, represented by its IEEE-754 bits.
pub static CURRENT_MAX_SPLIT_RANGES_PER_SEC: AtomicU64 = AtomicU64::new(0);
/// Go `CurrentMaxIngestInflight`.
pub static CURRENT_MAX_INGEST_INFLIGHT: AtomicI64 = AtomicI64::new(0);
/// Go `CurrentMaxIngestPerSec`, represented by its IEEE-754 bits.
pub static CURRENT_MAX_INGEST_PER_SEC: AtomicU64 = AtomicU64::new(0);

/// Go `GetMaxBatchSplitRanges`.
pub fn get_max_batch_split_ranges() -> i64 {
    match CURRENT_MAX_BATCH_SPLIT_RANGES.load(Ordering::SeqCst) {
        0 => DEFAULT_MAX_BATCH_SPLIT_RANGES,
        n => n,
    }
}
/// Go `GetMaxSplitRangePerSec`.
pub fn get_max_split_range_per_sec() -> f64 {
    f64::from_bits(CURRENT_MAX_SPLIT_RANGES_PER_SEC.load(Ordering::SeqCst))
}
/// Go `GetMaxIngestConcurrency`.
pub fn get_max_ingest_concurrency() -> i64 {
    CURRENT_MAX_INGEST_INFLIGHT.load(Ordering::SeqCst)
}
/// Go `GetMaxIngestPerSec`.
pub fn get_max_ingest_per_sec() -> f64 {
    f64::from_bits(CURRENT_MAX_INGEST_PER_SEC.load(Ordering::SeqCst))
}

/// Go `IngestParam`, shared with the status handler selecting these parameters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestParam {
    /// Go `IngestParamMaxBatchSplitRanges`.
    MaxBatchSplitRanges,
    /// Go `IngestParamMaxSplitRangesPerSec`.
    MaxSplitRangesPerSec,
    /// Go `IngestParamMaxInflight`.
    MaxInflight,
    /// Go `IngestParamMaxPerSecond`.
    MaxPerSecond,
}
impl IngestParam {
    /// Go `IngestConcurrencyHandler.ServeHTTP`'s metadata getter.
    pub fn get<T: MetaTransaction>(&self, m: &Mutator<T>) -> tidb_meta::error::Result<Option<f64>> {
        match self {
            Self::MaxBatchSplitRanges => m
                .get_ingest_max_batch_split_ranges()
                .map(|n| n.map(|n| n as f64)),
            Self::MaxSplitRangesPerSec => m.get_ingest_max_split_ranges_per_sec(),
            Self::MaxInflight => m.get_ingest_max_inflight().map(|n| n.map(|n| n as f64)),
            Self::MaxPerSecond => m.get_ingest_max_per_sec(),
        }
    }
    /// Go `IngestConcurrencyHandler.ServeHTTP`'s metadata setter.
    pub fn set<T: MetaTransaction>(
        &self,
        m: &Mutator<T>,
        value: f64,
    ) -> tidb_meta::error::Result<()> {
        match self {
            Self::MaxBatchSplitRanges => m.set_ingest_max_batch_split_ranges(value as i64),
            Self::MaxSplitRangesPerSec => m.set_ingest_max_split_ranges_per_sec(value),
            Self::MaxInflight => m.set_ingest_max_inflight(value as i64),
            Self::MaxPerSecond => m.set_ingest_max_per_sec(value),
        }
    }
    /// Go `IngestConcurrencyHandler.ServeHTTP.updateGlobal`, after commit.
    pub fn update_global(&self, value: f64) -> f64 {
        match self {
            Self::MaxBatchSplitRanges => {
                CURRENT_MAX_BATCH_SPLIT_RANGES.swap(value as i64, Ordering::SeqCst) as f64
            }
            Self::MaxSplitRangesPerSec => f64::from_bits(
                CURRENT_MAX_SPLIT_RANGES_PER_SEC.swap(value.to_bits(), Ordering::SeqCst),
            ),
            Self::MaxInflight => {
                CURRENT_MAX_INGEST_INFLIGHT.swap(value as i64, Ordering::SeqCst) as f64
            }
            Self::MaxPerSecond => {
                f64::from_bits(CURRENT_MAX_INGEST_PER_SEC.swap(value.to_bits(), Ordering::SeqCst))
            }
        }
    }
}
/// Go `InitializeRateLimiterParam` and `initializeVariables`: zero batch size
/// falls back locally without overwriting a persisted zero.
pub fn initialize_rate_limiter_param<T: MetaTransaction>(
    m: &Mutator<T>,
) -> tidb_meta::error::Result<()> {
    for (param, default) in [
        (IngestParam::MaxBatchSplitRanges, 2048.0),
        (IngestParam::MaxSplitRangesPerSec, 0.0),
        (IngestParam::MaxInflight, 0.0),
        (IngestParam::MaxPerSecond, 0.0),
    ] {
        let value = match param.get(m)? {
            None => {
                param.set(m, default)?;
                default
            }
            Some(0.0) => default,
            Some(v) => v,
        };
        param.update_global(value);
    }
    Ok(())
}
