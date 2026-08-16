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

//! Go `rustats.go`: the RU data types used by Top-RU.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use super::ru_details::RuDetails;
use super::ruv2_metrics::{RuV2Metrics, RuV2Weights};
use super::stmtstats::BinaryDigest;

/// boundary: Go `rmclient.RUVersion` from the PD client's
/// `resource_group/controller`, an integer enum whose zero value means
/// "unspecified".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct RuVersion(pub i32);

impl RuVersion {
    /// The Go zero value, which [`normalize_ru_version`] maps to the default.
    pub const UNSPECIFIED: Self = Self(0);
    /// Go `rmclient.RUVersionV1`.
    pub const V1: Self = Self(1);
    /// Go `rmclient.RUVersionV2`.
    pub const V2: Self = Self(2);
}

/// Go `RUVersionProvider`: returns the current RU version used by Top-RU
/// accounting.
pub trait RuVersionProvider: Send + Sync {
    /// Go `RUVersionProvider.GetRUVersion`.
    fn get_ru_version(&self) -> RuVersion;
}

/// Go `DefaultRUVersion`: the RU version used when no provider is bound.
///
/// boundary: `rmclient.DefaultRUVersion` is v1 accounting.
#[must_use]
pub fn default_ru_version() -> RuVersion {
    RuVersion::V1
}

/// Go `NormalizeRUVersion`: converts zero-value or unknown versions to the
/// default.
#[must_use]
pub fn normalize_ru_version(version: RuVersion) -> RuVersion {
    if version == RuVersion::UNSPECIFIED {
        default_ru_version()
    } else {
        version
    }
}

/// Go `RUKey`: identifies an RU aggregation key by user, SQL digest, and plan
/// digest.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct RuKey {
    /// Go `RUKey.User`.
    pub user: String,
    /// Go `RUKey.SQLDigest`.
    pub sql_digest: BinaryDigest,
    /// Go `RUKey.PlanDigest`.
    pub plan_digest: BinaryDigest,
}

/// Go `ExecutionContext`: the RU sampling state for one active SQL execution.
///
/// Go's `*util.RUDetails` and `*execdetails.RUV2Metrics` pointers are shared
/// with the executing statement, so both are `Arc` here; Go's nil is `None`.
#[derive(Debug, Default)]
pub struct ExecutionContext {
    /// Go `ExecutionContext.RUDetails`, cached at begin time to avoid
    /// per-tick `context.Value()` lookups.
    pub ru_details: Option<Arc<RuDetails>>,
    /// Go `ExecutionContext.RUV2Metrics`.
    pub ruv2_metrics: Option<Arc<RuV2Metrics>>,
    /// Go `ExecutionContext.Key`.
    pub key: RuKey,
    /// Go `ExecutionContext.RUV2Weights`.
    pub ruv2_weights: RuV2Weights,
    /// Go `ExecutionContext.LastRUTotal`.
    pub last_ru_total: f64,
    /// Go `ExecutionContext.RUVersion`.
    pub ru_version: RuVersion,
}

/// Go `RUIncrement`: a delta RU consumption for a specific [`RuKey`].
///
/// This is the unit of data produced by `StatementStats::merge_ru_into` and
/// consumed by `RuCollector::collect_ru_increments`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RuIncrement {
    /// Go `RUIncrement.TotalRU`: the delta RU consumption (RRU + WRU).
    pub total_ru: f64,

    /// Go `RUIncrement.ExecCount`: the number of SQL executions included in
    /// this increment. Begin-based semantics: each execution contributes at
    /// most one count on its first positive RU delta (tick or finish); later
    /// deltas carry count=0.
    pub exec_count: u64,

    /// Go `RUIncrement.ExecDuration`: the cumulative execution time in
    /// nanoseconds.
    pub exec_duration: u64,
}

impl RuIncrement {
    /// Go `RUIncrement.Merge`.
    pub fn merge(&mut self, other: &RuIncrement) {
        self.total_ru += other.total_ru;
        self.exec_count += other.exec_count;
        self.exec_duration += other.exec_duration;
    }
}

/// Go `RUIncrementMap`: maps [`RuKey`] to aggregated RU increments.
///
/// Go's map of `*RUIncrement` pointers is a map of owned values here; the
/// aliasing Go's `Merge` doc warns about cannot arise.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuIncrementMap(pub HashMap<RuKey, RuIncrement>);

impl RuIncrementMap {
    /// An empty map, Go's `RUIncrementMap{}`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `make(RUIncrementMap, size)`.
    #[must_use]
    pub fn with_capacity(size: usize) -> Self {
        Self(HashMap::with_capacity(size))
    }

    /// Go's `incr, ok := m[key]; if !ok { incr = &RUIncrement{}; m[key] = incr }`.
    pub fn get_or_create(&mut self, key: RuKey) -> &mut RuIncrement {
        self.0.entry(key).or_default()
    }
}

impl Deref for RuIncrementMap {
    type Target = HashMap<RuKey, RuIncrement>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RuIncrementMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
impl RuIncrementMap {
    /// Go's test-only `RUIncrementMap.Merge`, kept out of the production API
    /// surface exactly as `stmtstats_test.go` does.
    pub(super) fn merge(&mut self, other: &RuIncrementMap) {
        for (key, other_incr) in other.iter() {
            match self.0.get_mut(key) {
                None => {
                    self.0.insert(key.clone(), *other_incr);
                }
                Some(incr) => incr.merge(other_incr),
            }
        }
    }
}
