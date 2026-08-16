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

//! SEED of Go `pkg/sessionctx/variable`, covering `session.go`'s
//! dependency-closed value objects: [`Concurrency`] with its
//! deprecated-variable fallback, [`MemQuota`], [`BatchSize`],
//! [`PipelinedDmlConfig`], [`PartitionPruneMode`] with its out-of-date-mode
//! upgrade, and the runtime-filter type/mode enums. Each `default()` is the
//! corresponding initializer inside Go `NewSessionVars`.
//!
//! The ~500-field `SessionVars` struct itself is NOT here: this workspace's
//! live session state is `crate::vars`' override-map design, and these
//! objects are the pieces of Go's struct that carry behavior of their own.
//! The `SetSystemVar` paths that write into them are the sysvar-closure
//! batch; until then callers use the setters directly, exactly as Go's
//! closures do.

use tidb_vardef::defaults;

/// Go `vardef.ConcurrencyUnset`: the sentinel meaning "fall back to
/// `ExecutorConcurrency`".
pub const CONCURRENCY_UNSET: i64 = -1;

/// Go `Concurrency`: the per-operator worker counts.
///
/// The deprecated per-operator fields keep Go's privacy: they are written
/// through setters, and each deprecated getter falls back to
/// [`Concurrency::executor_concurrency`] while the field holds
/// [`CONCURRENCY_UNSET`]. `distSQLScanConcurrency`,
/// `analyzeDistSQLScanConcurrency`, and `mergeJoinConcurrency`/
/// `streamAggConcurrency` never fall back in the same way — the last two
/// default to real values, the first two are not deprecated.
///
/// Go's `SourceAddr net.TCPAddr` (coprocessor-only) is carried as an optional
/// address string; nothing reads it in this tier yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Concurrency {
    index_lookup_concurrency: i64,
    index_lookup_join_concurrency: i64,
    dist_sql_scan_concurrency: i64,
    analyze_dist_sql_scan_concurrency: i64,
    hash_join_concurrency: i64,
    projection_concurrency: i64,
    hash_agg_partial_concurrency: i64,
    hash_agg_final_concurrency: i64,
    window_concurrency: i64,
    merge_join_concurrency: i64,
    stream_agg_concurrency: i64,
    index_merge_intersection_concurrency: i64,
    /// Go `ExecutorConcurrency`, the shared fallback.
    pub executor_concurrency: i64,
    /// Go `SourceAddr`, available in coprocessor only.
    pub source_addr: Option<String>,
    /// Go `IdleTransactionTimeout` in seconds.
    pub idle_transaction_timeout: i64,
}

impl Default for Concurrency {
    /// Go `NewSessionVars`' `Concurrency{...}` initializer.
    fn default() -> Self {
        Self {
            index_lookup_concurrency: defaults::DEF_INDEX_LOOKUP_CONCURRENCY,
            index_lookup_join_concurrency: defaults::DEF_INDEX_LOOKUP_JOIN_CONCURRENCY,
            dist_sql_scan_concurrency: defaults::DEF_DIST_SQL_SCAN_CONCURRENCY,
            analyze_dist_sql_scan_concurrency: defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY,
            hash_join_concurrency: defaults::DEF_TIDB_HASH_JOIN_CONCURRENCY,
            projection_concurrency: defaults::DEF_TIDB_PROJECTION_CONCURRENCY,
            hash_agg_partial_concurrency: defaults::DEF_TIDB_HASH_AGG_PARTIAL_CONCURRENCY,
            hash_agg_final_concurrency: defaults::DEF_TIDB_HASH_AGG_FINAL_CONCURRENCY,
            window_concurrency: defaults::DEF_TIDB_WINDOW_CONCURRENCY,
            merge_join_concurrency: defaults::DEF_TIDB_MERGE_JOIN_CONCURRENCY,
            stream_agg_concurrency: defaults::DEF_TIDB_STREAM_AGG_CONCURRENCY,
            index_merge_intersection_concurrency:
                defaults::DEF_TIDB_INDEX_MERGE_INTERSECTION_CONCURRENCY,
            executor_concurrency: defaults::DEF_EXECUTOR_CONCURRENCY,
            source_addr: None,
            idle_transaction_timeout: 0,
        }
    }
}

macro_rules! concurrency_with_fallback {
    ($(#[$doc:meta])* $getter:ident, $(#[$set_doc:meta])* $setter:ident, $field:ident) => {
        $(#[$doc])*
        #[must_use]
        pub fn $getter(&self) -> i64 {
            if self.$field != CONCURRENCY_UNSET {
                self.$field
            } else {
                self.executor_concurrency
            }
        }

        $(#[$set_doc])*
        pub fn $setter(&mut self, n: i64) {
            self.$field = n;
        }
    };
}

impl Concurrency {
    concurrency_with_fallback!(
        /// Go `IndexLookupConcurrency`.
        index_lookup_concurrency,
        /// Go `SetIndexLookupConcurrency`.
        set_index_lookup_concurrency,
        index_lookup_concurrency
    );
    concurrency_with_fallback!(
        /// Go `IndexLookupJoinConcurrency`.
        index_lookup_join_concurrency,
        /// Go `SetIndexLookupJoinConcurrency`.
        set_index_lookup_join_concurrency,
        index_lookup_join_concurrency
    );
    concurrency_with_fallback!(
        /// Go `HashJoinConcurrency`.
        hash_join_concurrency,
        /// Go `SetHashJoinConcurrency`.
        set_hash_join_concurrency,
        hash_join_concurrency
    );
    concurrency_with_fallback!(
        /// Go `ProjectionConcurrency`.
        projection_concurrency,
        /// Go `SetProjectionConcurrency`.
        set_projection_concurrency,
        projection_concurrency
    );
    concurrency_with_fallback!(
        /// Go `HashAggPartialConcurrency`.
        hash_agg_partial_concurrency,
        /// Go `SetHashAggPartialConcurrency`.
        set_hash_agg_partial_concurrency,
        hash_agg_partial_concurrency
    );
    concurrency_with_fallback!(
        /// Go `HashAggFinalConcurrency`.
        hash_agg_final_concurrency,
        /// Go `SetHashAggFinalConcurrency`.
        set_hash_agg_final_concurrency,
        hash_agg_final_concurrency
    );
    concurrency_with_fallback!(
        /// Go `WindowConcurrency`.
        window_concurrency,
        /// Go `SetWindowConcurrency`.
        set_window_concurrency,
        window_concurrency
    );
    concurrency_with_fallback!(
        /// Go `MergeJoinConcurrency`. The default is a real value, so the
        /// fallback only fires if a caller explicitly stores the sentinel.
        merge_join_concurrency,
        /// Go `SetMergeJoinConcurrency`.
        set_merge_join_concurrency,
        merge_join_concurrency
    );
    concurrency_with_fallback!(
        /// Go `StreamAggConcurrency`; like merge join, defaulted to a real
        /// value.
        stream_agg_concurrency,
        /// Go `SetStreamAggConcurrency`.
        set_stream_agg_concurrency,
        stream_agg_concurrency
    );
    concurrency_with_fallback!(
        /// Go `IndexMergeIntersectionConcurrency`.
        index_merge_intersection_concurrency,
        /// Go `SetIndexMergeIntersectionConcurrency`.
        set_index_merge_intersection_concurrency,
        index_merge_intersection_concurrency
    );

    /// Go `DistSQLScanConcurrency`: no fallback.
    #[must_use]
    pub const fn dist_sql_scan_concurrency(&self) -> i64 {
        self.dist_sql_scan_concurrency
    }

    /// Go `SetDistSQLScanConcurrency`.
    pub fn set_dist_sql_scan_concurrency(&mut self, n: i64) {
        self.dist_sql_scan_concurrency = n;
    }

    /// Go `AnalyzeDistSQLScanConcurrency`: no fallback.
    #[must_use]
    pub const fn analyze_dist_sql_scan_concurrency(&self) -> i64 {
        self.analyze_dist_sql_scan_concurrency
    }

    /// Go `SetAnalyzeDistSQLScanConcurrency`.
    pub fn set_analyze_dist_sql_scan_concurrency(&mut self, n: i64) {
        self.analyze_dist_sql_scan_concurrency = n;
    }

    /// Go `UnionConcurrency`: always the executor concurrency.
    #[must_use]
    pub const fn union_concurrency(&self) -> i64 {
        self.executor_concurrency
    }
}

/// Go `MemQuota`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemQuota {
    /// Go `MemQuotaQuery`: the memory quota for a query.
    pub mem_quota_query: i64,
    /// Go `MemQuotaApplyCache`: the capacity for the apply cache.
    pub mem_quota_apply_cache: i64,
}

impl Default for MemQuota {
    /// Go `NewSessionVars`' `MemQuota{...}` initializer.
    fn default() -> Self {
        Self {
            mem_quota_query: defaults::DEF_TIDB_MEM_QUOTA_QUERY,
            mem_quota_apply_cache: defaults::DEF_TIDB_MEM_QUOTA_APPLY_CACHE,
        }
    }
}

/// Go `BatchSize`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchSize {
    /// Go `IndexJoinBatchSize`.
    pub index_join_batch_size: i64,
    /// Go `IndexLookupSize`.
    pub index_lookup_size: i64,
    /// Go `InitChunkSize`.
    pub init_chunk_size: i64,
    /// Go `MaxChunkSize`.
    pub max_chunk_size: i64,
    /// Go `MinPagingSize`.
    pub min_paging_size: i64,
    /// Go `MaxPagingSize`.
    pub max_paging_size: i64,
}

impl Default for BatchSize {
    /// Go `NewSessionVars`' `BatchSize{...}` initializer.
    fn default() -> Self {
        Self {
            index_join_batch_size: defaults::DEF_INDEX_JOIN_BATCH_SIZE,
            index_lookup_size: defaults::DEF_INDEX_LOOKUP_SIZE,
            init_chunk_size: defaults::DEF_INIT_CHUNK_SIZE,
            max_chunk_size: defaults::DEF_MAX_CHUNK_SIZE,
            min_paging_size: defaults::DEF_MIN_PAGING_SIZE,
            max_paging_size: defaults::DEF_MAX_PAGING_SIZE,
        }
    }
}

/// Go `PipelinedDMLConfig`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct PipelinedDmlConfig {
    /// Go `PipelinedFlushConcurrency`.
    pub pipelined_flush_concurrency: i64,
    /// Go `PipelinedResolveLockConcurrency`.
    pub pipelined_resolve_lock_concurrency: i64,
    /// Go `PipelinedWriteThrottleRatio`, `T_sleep / (T_sleep + T_flush)`.
    pub pipelined_write_throttle_ratio: f64,
}

/// Go `PartitionPruneMode`: a string-typed mode with three out-of-date
/// spellings kept for upgrades.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionPruneMode {
    /// Go `Static`: prune at plan phase only.
    Static,
    /// Go `Dynamic`: prune at execute phase only.
    Dynamic,
    /// Go `StaticOnly`, out-of-date.
    StaticOnly,
    /// Go `DynamicOnly`, out-of-date.
    DynamicOnly,
    /// Go `StaticButPrepareDynamic` (`static-collect-dynamic`), out-of-date.
    StaticButPrepareDynamic,
    /// Any other spelling, which Go keeps as the raw string; it is invalid
    /// and survives `Update` unchanged.
    Other,
}

impl PartitionPruneMode {
    /// The Go string constant behind each mode. [`Self::Other`] has no fixed
    /// spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Static => "static",
            Self::Dynamic => "dynamic",
            Self::StaticOnly => "static-only",
            Self::DynamicOnly => "dynamic-only",
            Self::StaticButPrepareDynamic => "static-collect-dynamic",
            Self::Other => "",
        }
    }

    /// The mode a stored string denotes.
    #[must_use]
    pub fn from_str_value(value: &str) -> Self {
        match value {
            "static" => Self::Static,
            "dynamic" => Self::Dynamic,
            "static-only" => Self::StaticOnly,
            "dynamic-only" => Self::DynamicOnly,
            "static-collect-dynamic" => Self::StaticButPrepareDynamic,
            _ => Self::Other,
        }
    }

    /// Go `Valid`. Note the source's own asymmetry: `static-collect-dynamic`
    /// is upgradable by `Update` but is NOT valid.
    #[must_use]
    pub const fn valid(self) -> bool {
        matches!(
            self,
            Self::Static | Self::Dynamic | Self::StaticOnly | Self::DynamicOnly
        )
    }

    /// Go `Update`: maps the out-of-date modes onto their replacements.
    #[must_use]
    pub const fn update(self) -> Self {
        match self {
            Self::StaticOnly | Self::StaticButPrepareDynamic => Self::Static,
            Self::DynamicOnly => Self::Dynamic,
            other => other,
        }
    }
}

/// Go `RuntimeFilterType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RuntimeFilterType {
    /// Go `In`: `t.k1 in (?)`.
    In,
    /// Go `MinMax`: `t.k1 < ? and t.k1 > ?`.
    MinMax,
}

impl RuntimeFilterType {
    /// Go `String`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::In => "IN",
            Self::MinMax => "MIN_MAX",
        }
    }

    /// Go `RuntimeFilterTypeStringToType`: exact uppercase names only.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "IN" => Some(Self::In),
            "MIN_MAX" => Some(Self::MinMax),
            _ => None,
        }
    }
}

/// Go `ToRuntimeFilterType`: a comma-separated session value becomes a
/// deduplicated type list, case-folded per element; any illegal element
/// rejects the whole value.
#[must_use]
pub fn to_runtime_filter_type(session_var_value: &str) -> Option<Vec<RuntimeFilterType>> {
    let mut types = Vec::new();
    for type_name in session_var_value.split(',') {
        let rf_type = RuntimeFilterType::from_name(&type_name.to_uppercase())?;
        if !types.contains(&rf_type) {
            types.push(rf_type);
        }
    }
    Some(types)
}

/// Go `RuntimeFilterMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeFilterMode {
    /// Go `RFOff`.
    Off,
    /// Go `RFLocal`.
    Local,
    /// Go `RFGlobal`.
    Global,
}

impl RuntimeFilterMode {
    /// Go `String`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::Local => "LOCAL",
            Self::Global => "GLOBAL",
        }
    }

    /// Go `RuntimeFilterModeStringToMode`: only `OFF` and `LOCAL` parse;
    /// `GLOBAL` exists as a mode but is not yet accepted from a string.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "OFF" => Some(Self::Off),
            "LOCAL" => Some(Self::Local),
            _ => None,
        }
    }
}

use crate::vars::SessionVars;
use crate::varsutil::{tidb_opt_int64, tidb_opt_positive_int32};
use tidb_vardef::tidb_vars as names;

/// The stored text a Go `SetSession` closure would receive: the session's
/// value where one is set, else the registry default. Every default here
/// round-trips through its closure to the `NewSessionVars` field default, so
/// deriving from stored-or-default text reproduces the field lifecycle.
fn stored(vars: &SessionVars, name: &str) -> String {
    vars.get_system(name).unwrap_or_default()
}

impl Concurrency {
    /// Derives the typed concurrency state from a session's stored
    /// variables, applying each Go `SetSession` closure body
    /// (`newExecConcurrencySysVar`'s `tidbOptPositiveInt32(val,
    /// ConcurrencyUnset)` for the deprecated per-operator variables, and the
    /// dedicated closures for the executor/dist-SQL ones) to the stored
    /// text. This is the observable contract of Go `SetSystemVar` followed
    /// by the getters; the closure runs at set time there and at read time
    /// here, over the same normalized text.
    #[must_use]
    pub fn from_vars(vars: &SessionVars) -> Self {
        let unset = |name: &str| tidb_opt_positive_int32(&stored(vars, name), CONCURRENCY_UNSET);
        Self {
            index_lookup_concurrency: unset(names::TIDB_INDEX_LOOKUP_CONCURRENCY),
            index_lookup_join_concurrency: unset(names::TIDB_INDEX_LOOKUP_JOIN_CONCURRENCY),
            dist_sql_scan_concurrency: tidb_opt_positive_int32(
                &stored(vars, names::TIDB_DIST_SQL_SCAN_CONCURRENCY),
                defaults::DEF_DIST_SQL_SCAN_CONCURRENCY,
            ),
            analyze_dist_sql_scan_concurrency: tidb_opt_positive_int32(
                &stored(vars, names::TIDB_ANALYZE_DIST_SQL_SCAN_CONCURRENCY),
                defaults::DEF_ANALYZE_DIST_SQL_SCAN_CONCURRENCY,
            ),
            hash_join_concurrency: unset(names::TIDB_HASH_JOIN_CONCURRENCY),
            projection_concurrency: unset(names::TIDB_PROJECTION_CONCURRENCY),
            hash_agg_partial_concurrency: unset(names::TIDB_HASH_AGG_PARTIAL_CONCURRENCY),
            hash_agg_final_concurrency: unset(names::TIDB_HASH_AGG_FINAL_CONCURRENCY),
            window_concurrency: unset(names::TIDB_WINDOW_CONCURRENCY),
            merge_join_concurrency: unset(names::TIDB_MERGE_JOIN_CONCURRENCY),
            stream_agg_concurrency: unset(names::TIDB_STREAM_AGG_CONCURRENCY),
            index_merge_intersection_concurrency: unset(
                names::TIDB_INDEX_MERGE_INTERSECTION_CONCURRENCY,
            ),
            executor_concurrency: tidb_opt_positive_int32(
                &stored(vars, names::TIDB_EXECUTOR_CONCURRENCY),
                defaults::DEF_EXECUTOR_CONCURRENCY,
            ),
            source_addr: None,
            idle_transaction_timeout: 0,
        }
    }
}

impl MemQuota {
    /// Derives the memory quotas: Go's two `TidbOptInt64` closures over the
    /// stored text.
    #[must_use]
    pub fn from_vars(vars: &SessionVars) -> Self {
        Self {
            mem_quota_query: tidb_opt_int64(
                &stored(vars, names::TIDB_MEM_QUOTA_QUERY),
                defaults::DEF_TIDB_MEM_QUOTA_QUERY,
            ),
            mem_quota_apply_cache: tidb_opt_int64(
                &stored(vars, names::TIDB_MEM_QUOTA_APPLY_CACHE),
                defaults::DEF_TIDB_MEM_QUOTA_APPLY_CACHE,
            ),
        }
    }
}

impl BatchSize {
    /// Derives the batch sizes: each field's Go closure is
    /// `tidbOptPositiveInt32(val, its default)`.
    #[must_use]
    pub fn from_vars(vars: &SessionVars) -> Self {
        let positive =
            |name: &str, default: i64| tidb_opt_positive_int32(&stored(vars, name), default);
        Self {
            index_join_batch_size: positive(
                names::TIDB_INDEX_JOIN_BATCH_SIZE,
                defaults::DEF_INDEX_JOIN_BATCH_SIZE,
            ),
            index_lookup_size: positive(
                names::TIDB_INDEX_LOOKUP_SIZE,
                defaults::DEF_INDEX_LOOKUP_SIZE,
            ),
            init_chunk_size: positive(names::TIDB_INIT_CHUNK_SIZE, defaults::DEF_INIT_CHUNK_SIZE),
            max_chunk_size: positive(names::TIDB_MAX_CHUNK_SIZE, defaults::DEF_MAX_CHUNK_SIZE),
            min_paging_size: positive(names::TIDB_MIN_PAGING_SIZE, defaults::DEF_MIN_PAGING_SIZE),
            max_paging_size: positive(names::TIDB_MAX_PAGING_SIZE, defaults::DEF_MAX_PAGING_SIZE),
        }
    }
}

#[cfg(test)]
mod closure_tests {
    use super::*;

    // Go `TestConcurrencyVariables`, now through the real SET path: setting
    // the variable changes the derived field; unset ones follow the raised
    // executor concurrency.
    #[test]
    fn set_system_var_drives_the_derived_concurrency() {
        let mut vars = SessionVars::new();

        // Untouched session derives exactly NewSessionVars' defaults.
        assert_eq!(Concurrency::from_vars(&vars), Concurrency::default());
        assert_eq!(MemQuota::from_vars(&vars), MemQuota::default());
        assert_eq!(BatchSize::from_vars(&vars), BatchSize::default());

        vars.set_system(names::TIDB_WINDOW_CONCURRENCY, "2".to_owned())
            .unwrap();
        vars.set_system(names::TIDB_MERGE_JOIN_CONCURRENCY, "2".to_owned())
            .unwrap();
        vars.set_system(names::TIDB_STREAM_AGG_CONCURRENCY, "2".to_owned())
            .unwrap();
        let concurrency = Concurrency::from_vars(&vars);
        assert_eq!(concurrency.window_concurrency(), 2);
        assert_eq!(concurrency.merge_join_concurrency(), 2);
        assert_eq!(concurrency.stream_agg_concurrency(), 2);

        // Raising the executor concurrency moves the still-unset getters.
        let raised = defaults::DEF_EXECUTOR_CONCURRENCY + 1;
        vars.set_system(names::TIDB_EXECUTOR_CONCURRENCY, raised.to_string())
            .unwrap();
        let concurrency = Concurrency::from_vars(&vars);
        assert_eq!(concurrency.executor_concurrency, raised);
        assert_eq!(concurrency.index_lookup_concurrency(), raised);
        assert_eq!(concurrency.index_lookup_join_concurrency(), raised);
        assert_eq!(concurrency.hash_join_concurrency(), raised);
        assert_eq!(concurrency.window_concurrency(), 2);
        assert_eq!(concurrency.merge_join_concurrency(), 2);
    }

    // The mem-quota and batch-size closures parse the stored text with their
    // Go defaults as the fallback.
    #[test]
    fn set_system_var_drives_quotas_and_batch_sizes() {
        let mut vars = SessionVars::new();
        vars.set_system(names::TIDB_MEM_QUOTA_QUERY, "2097152".to_owned())
            .unwrap();
        vars.set_system(names::TIDB_INDEX_JOIN_BATCH_SIZE, "123".to_owned())
            .unwrap();
        vars.set_system(names::TIDB_MAX_CHUNK_SIZE, "2048".to_owned())
            .unwrap();

        assert_eq!(MemQuota::from_vars(&vars).mem_quota_query, 2_097_152);
        let batch = BatchSize::from_vars(&vars);
        assert_eq!(batch.index_join_batch_size, 123);
        assert_eq!(batch.max_chunk_size, 2048);
        // Untouched fields keep their defaults.
        assert_eq!(batch.index_lookup_size, defaults::DEF_INDEX_LOOKUP_SIZE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go `TestNewSessionVars`' concurrency/batch-size expectations, applied
    // to the initializers these defaults come from.
    #[test]
    fn defaults_match_new_session_vars() {
        let concurrency = Concurrency::default();
        assert_eq!(
            concurrency.executor_concurrency,
            defaults::DEF_EXECUTOR_CONCURRENCY
        );
        // The deprecated fields start unset and fall back.
        assert_eq!(
            concurrency.index_lookup_concurrency(),
            defaults::DEF_EXECUTOR_CONCURRENCY
        );
        assert_eq!(
            concurrency.index_lookup_join_concurrency(),
            defaults::DEF_EXECUTOR_CONCURRENCY
        );
        assert_eq!(
            concurrency.hash_join_concurrency(),
            defaults::DEF_EXECUTOR_CONCURRENCY
        );
        assert_eq!(
            concurrency.projection_concurrency(),
            defaults::DEF_EXECUTOR_CONCURRENCY
        );
        assert_eq!(
            concurrency.hash_agg_partial_concurrency(),
            defaults::DEF_EXECUTOR_CONCURRENCY
        );
        assert_eq!(
            concurrency.hash_agg_final_concurrency(),
            defaults::DEF_EXECUTOR_CONCURRENCY
        );
        assert_eq!(
            concurrency.window_concurrency(),
            defaults::DEF_EXECUTOR_CONCURRENCY
        );
        // These two default to real values, not the sentinel.
        assert_eq!(
            concurrency.merge_join_concurrency(),
            defaults::DEF_TIDB_MERGE_JOIN_CONCURRENCY
        );
        assert_eq!(
            concurrency.stream_agg_concurrency(),
            defaults::DEF_TIDB_STREAM_AGG_CONCURRENCY
        );
        assert_eq!(
            concurrency.dist_sql_scan_concurrency(),
            defaults::DEF_DIST_SQL_SCAN_CONCURRENCY
        );
        assert_eq!(concurrency.union_concurrency(), 5);

        let batch = BatchSize::default();
        assert_eq!(
            batch.index_join_batch_size,
            defaults::DEF_INDEX_JOIN_BATCH_SIZE
        );
        assert_eq!(batch.index_lookup_size, defaults::DEF_INDEX_LOOKUP_SIZE);
        assert_eq!(batch.init_chunk_size, 32);
        assert_eq!(batch.max_chunk_size, 1024);

        let quota = MemQuota::default();
        assert_eq!(quota.mem_quota_query, defaults::DEF_TIDB_MEM_QUOTA_QUERY);
        assert_eq!(
            quota.mem_quota_apply_cache,
            defaults::DEF_TIDB_MEM_QUOTA_APPLY_CACHE
        );
    }

    // Go `TestConcurrencyVariables`: a set value overrides the fallback, and
    // raising ExecutorConcurrency moves every still-unset getter with it.
    #[test]
    fn set_values_override_the_executor_fallback() {
        let mut concurrency = Concurrency::default();

        concurrency.set_window_concurrency(2);
        assert_eq!(concurrency.window_concurrency(), 2);

        concurrency.set_merge_join_concurrency(2);
        assert_eq!(concurrency.merge_join_concurrency(), 2);

        concurrency.set_stream_agg_concurrency(2);
        assert_eq!(concurrency.stream_agg_concurrency(), 2);

        // index lookup remains unset, so raising the executor concurrency
        // raises it too — but not the explicitly set ones.
        let raised = defaults::DEF_EXECUTOR_CONCURRENCY + 1;
        concurrency.executor_concurrency = raised;
        assert_eq!(concurrency.index_lookup_concurrency(), raised);
        assert_eq!(concurrency.index_lookup_join_concurrency(), raised);
        assert_eq!(concurrency.window_concurrency(), 2);
        assert_eq!(concurrency.merge_join_concurrency(), 2);
        assert_eq!(concurrency.stream_agg_concurrency(), 2);
        assert_eq!(concurrency.union_concurrency(), raised);
    }

    // Go `PartitionPruneMode.Valid`/`Update`, including the asymmetry where
    // static-collect-dynamic upgrades but is not valid.
    #[test]
    fn partition_prune_modes_validate_and_upgrade() {
        for (spelling, valid, updated) in [
            ("static", true, PartitionPruneMode::Static),
            ("dynamic", true, PartitionPruneMode::Dynamic),
            ("static-only", true, PartitionPruneMode::Static),
            ("dynamic-only", true, PartitionPruneMode::Dynamic),
            ("static-collect-dynamic", false, PartitionPruneMode::Static),
            ("bogus", false, PartitionPruneMode::Other),
        ] {
            let mode = PartitionPruneMode::from_str_value(spelling);
            assert_eq!(mode.valid(), valid, "{spelling}");
            assert_eq!(mode.update(), updated, "{spelling}");
        }
        assert_eq!(PartitionPruneMode::Static.as_str(), "static");
        assert_eq!(
            PartitionPruneMode::StaticButPrepareDynamic.as_str(),
            "static-collect-dynamic"
        );
    }

    // Go `ToRuntimeFilterType`: case-folded, deduplicated, all-or-nothing.
    #[test]
    fn runtime_filter_values_parse_as_lists() {
        assert_eq!(
            to_runtime_filter_type("IN"),
            Some(vec![RuntimeFilterType::In])
        );
        assert_eq!(
            to_runtime_filter_type("in,min_max"),
            Some(vec![RuntimeFilterType::In, RuntimeFilterType::MinMax])
        );
        // Duplicates collapse.
        assert_eq!(
            to_runtime_filter_type("IN,IN"),
            Some(vec![RuntimeFilterType::In])
        );
        // One illegal element rejects the whole value.
        assert_eq!(to_runtime_filter_type("IN,BLOOM"), None);
        assert_eq!(to_runtime_filter_type(""), None);

        assert_eq!(RuntimeFilterType::In.as_str(), "IN");
        assert_eq!(RuntimeFilterType::MinMax.as_str(), "MIN_MAX");
    }

    // Go `RuntimeFilterModeStringToMode`: GLOBAL exists but does not parse.
    #[test]
    fn runtime_filter_modes_parse_off_and_local_only() {
        assert_eq!(
            RuntimeFilterMode::from_name("OFF"),
            Some(RuntimeFilterMode::Off)
        );
        assert_eq!(
            RuntimeFilterMode::from_name("LOCAL"),
            Some(RuntimeFilterMode::Local)
        );
        assert_eq!(RuntimeFilterMode::from_name("GLOBAL"), None);
        assert_eq!(RuntimeFilterMode::Global.as_str(), "GLOBAL");
    }
}
