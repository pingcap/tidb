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

//! Per-statement value cache from `stmtctx.go`.
//!
//! Source: `pkg/sessionctx/stmtctx/stmtctx.go:211-222` (the `stmtCache`
//! holder and its `reset`) and `:812-866` (`StmtCacheKey`,
//! `GetOrStoreStmtCache`, `GetOrEvaluateStmtCache`, `ResetInStmtCache`,
//! `ResetStmtCache`).
//!
//! NARROWING: Go stores `map[StmtCacheKey]any` and every reader immediately
//! type-asserts. The `any` becomes the typed [`StmtCacheValue`] enum over the
//! three value types the source actually stores:
//!
//! * `StmtNowTsCacheKey` -> `time.Time` ([`SystemTime`] here), stored by
//!   `pkg/sessionctx/variable/sysvar.go:139` (`timestamp` sysvar reads
//!   `GetOrStoreStmtCache(..., time.Now()).(time.Time)`).
//! * `StmtSafeTSCacheKey` -> `uint64`, stored by
//!   `pkg/expression/builtin_time.go:7165` (`tidb_bounded_staleness` caches
//!   `minSafeTS` and asserts `.(uint64)`).
//! * `StmtExternalTSCacheKey` -> `uint64`, evaluated by
//!   `pkg/sessiontxn/staleread/util.go:156-163` (`GetExternalTimestamp`
//!   asserts `.(uint64)`).
//!
//! This leaf owns only the keyed first-value-wins cache; the statement
//! context that embeds it, the sysvar/expression/stale-read callers, and the
//! statement reset cycle stay outside.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

/// Go `StmtCacheKey`: the key type in the statement cache.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StmtCacheKey {
    /// Go `StmtNowTsCacheKey`: now/current_timestamp calculation of one stmt.
    NowTs,
    /// Go `StmtSafeTSCacheKey`: safeTS calculation of one stmt.
    SafeTs,
    /// Go `StmtExternalTSCacheKey`: externalTS calculation of one stmt.
    ExternalTs,
}

/// The typed replacement for Go's `any` cache values (see the module doc for
/// the per-key source call sites this enum was narrowed from).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum StmtCacheValue {
    /// The `time.Now()` cached under `StmtNowTsCacheKey`.
    NowTs(SystemTime),
    /// The min safe TS cached under `StmtSafeTSCacheKey`.
    SafeTs(u64),
    /// The external timestamp cached under `StmtExternalTSCacheKey`.
    ExternalTs(u64),
}

impl StmtCacheValue {
    /// The reader-side narrowing of Go's `.(time.Time)` assertion.
    #[must_use]
    pub const fn as_now_ts(self) -> Option<SystemTime> {
        match self {
            Self::NowTs(ts) => Some(ts),
            Self::SafeTs(_) | Self::ExternalTs(_) => None,
        }
    }

    /// The reader-side narrowing of Go's `.(uint64)` assertion on the safe
    /// TS.
    #[must_use]
    pub const fn as_safe_ts(self) -> Option<u64> {
        match self {
            Self::SafeTs(ts) => Some(ts),
            Self::NowTs(_) | Self::ExternalTs(_) => None,
        }
    }

    /// The reader-side narrowing of Go's `.(uint64)` assertion on the
    /// external TS.
    #[must_use]
    pub const fn as_external_ts(self) -> Option<u64> {
        match self {
            Self::ExternalTs(ts) => Some(ts),
            Self::NowTs(_) | Self::SafeTs(_) => None,
        }
    }
}

/// Go `stmtCache`: a mutex-guarded keyed cache where the first stored value
/// wins for the rest of the statement.
///
/// Go's lazily allocated nil map and the empty map made by `ResetStmtCache`
/// are observationally identical, so both collapse to one always-present
/// [`HashMap`] here.
#[derive(Debug, Default)]
pub struct StmtCache {
    data: Mutex<HashMap<StmtCacheKey, StmtCacheValue>>,
}

impl StmtCache {
    /// Creates an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `GetOrStoreStmtCache`: returns the cached value for `key` if it
    /// exists, otherwise stores and returns `value`.
    pub fn get_or_store(&self, key: StmtCacheKey, value: StmtCacheValue) -> StmtCacheValue {
        let mut data = self.data.lock().expect("stmt cache poisoned");
        *data.entry(key).or_insert(value)
    }

    /// Go `GetOrEvaluateStmtCache`: returns the cached value for `key` if it
    /// exists, otherwise calculates it with `evaluator`.
    ///
    /// As in the source, an evaluator error is returned without caching
    /// anything, so a later call re-evaluates.
    pub fn get_or_evaluate<E>(
        &self,
        key: StmtCacheKey,
        evaluator: impl FnOnce() -> Result<StmtCacheValue, E>,
    ) -> Result<StmtCacheValue, E> {
        let mut data = self.data.lock().expect("stmt cache poisoned");
        if let Some(value) = data.get(&key) {
            return Ok(*value);
        }
        let value = evaluator()?;
        data.insert(key, value);
        Ok(value)
    }

    /// Go `ResetInStmtCache`: resets the cache of the given key.
    pub fn reset_in(&self, key: StmtCacheKey) {
        let mut data = self.data.lock().expect("stmt cache poisoned");
        data.remove(&key);
    }

    /// Go `ResetStmtCache` (and the `stmtCache.reset` used between
    /// statements): resets all cached values.
    pub fn reset(&self) {
        let mut data = self.data.lock().expect("stmt cache poisoned");
        data.clear();
    }
}
