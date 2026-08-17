// Copyright 2025 PingCAP, Inc.
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

//! Go `pkg/domain/domain_sysvars.go` — PARTIAL. One production symbol is
//! deliberately absent; everything else is here.
//!
//! Present: `setStatsCacheCapacity` ([`set_stats_cache_capacity`]),
//! `setPDClientDynamicOption` ([`set_pd_client_dynamic_option`]),
//! `setGlobalResourceControl` ([`set_global_resource_control`]),
//! `setLowResolutionTSOUpdateInterval`
//! ([`set_low_resolution_tso_update_interval`]), `updatePDClient`
//! ([`update_pd_client`]), `setExternalTimestamp`
//! ([`set_external_timestamp`]), `getExternalTimestamp`
//! ([`get_external_timestamp`]), and
//! `changePDMetadataCircuitBreakerErrorRateThresholdRatio`
//! ([`change_pd_metadata_circuit_breaker_error_rate_threshold_ratio`]).
//!
//! ABSENT: `initDomainSysVars`. Its entire body assigns Go closures into
//! process-global function-pointer slots in `pkg/sessionctx/variable`
//! (`variable.SetStatsCacheCapacity`, `variable.SetPDClientDynamicOption`,
//! `variable.SetExternalTimestamp`, `variable.GetExternalTimestamp`,
//! `variable.SetGlobalResourceControl`,
//! `variable.SetLowResolutionTSOUpdateInterval`,
//! `variable.ChangeSchemaCacheSize`,
//! `variable.ChangePDMetadataCircuitBreakerErrorRateThresholdRatio`), plus
//! one that reaches through `Domain.isSyncer`. Those slots have no Rust
//! home — `tidb_session::sysvar` carries no equivalent registry of mutable
//! hooks — and inventing one here would be `pkg/sessionctx/variable`'s
//! design decision, not `pkg/domain`'s. In Rust the wiring is instead the
//! act of implementing [`DomainSysVarEnv`], so there is nothing left for
//! `initDomainSysVars` to do once the `domain.go` batch supplies that impl.
//! Blocking Go symbols, precisely: `variable.SetStatsCacheCapacity` (an
//! `atomic.Pointer[func(int64)]`) and `Domain.isSyncer.ChangeSchemaCacheSize`.
//!
//! The substance of the file is [`set_pd_client_dynamic_option`]: the map
//! from a session-variable name and its string value to a PD client dynamic
//! option. Four Go behaviors there are load-bearing and reproduced:
//!
//! 1. An unrecognized `name` falls off the end of the switch and returns
//!    `nil` — no error, no PD call. A caller cannot distinguish "applied"
//!    from "ignored".
//! 2. The `vardef` mirror (`MaxTSOBatchWaitInterval.Store` and friends) is
//!    written only *after* the PD update succeeds, so a failed PD call
//!    leaves the mirror at its old value.
//! 3. `TiDBTSOClientRPCMode` is the one case with no mirror write at all;
//!    only the PD option is set.
//! 4. Only `TiDBTSOClientRPCMode` validates its value. The three boolean
//!    cases run through `variable.TiDBOptOn`, which maps everything that is
//!    not `"ON"`/`"on"`/`"1"` to `false` without complaint.
//!
//! Narrowings, all named:
//!
//! - `// boundary:` Go `pkg/domain.Domain` — every function here is a
//!   `*Domain` method whose body immediately delegates to one collaborator.
//!   Those collaborators are [`DomainSysVarEnv`], one method per Go symbol.
//! - `// boundary:` Go `Domain.store.GetOracle()` (`kv.Storage` ->
//!   `oracle.Oracle`) — [`DomainSysVarEnv::oracle_set_low_resolution_timestamp_update_interval`],
//!   `oracle_set_external_timestamp`, `oracle_get_external_timestamp`.
//! - `// boundary:` Go `Domain.store.(interface{ GetPDClient() pd.Client })`
//!   and `pd.Client.UpdateOption` — [`DomainSysVarEnv::pd_update_option`].
//!   Go's two nil escapes (the store does not implement the interface; the
//!   client is nil) both return `nil` *without* applying anything;
//!   [`update_pd_client`] reproduces that through
//!   [`DomainSysVarEnv::pd_client_present`], so a TiDB on a store with no
//!   PD silently accepts the SET.
//! - `// boundary:` Go `github.com/tikv/pd/client/opt.DynamicOption` —
//!   [`PdDynamicOption`], carrying only the five options this file sets.
//! - `// boundary:` Go `Domain.StatsHandle()` and
//!   `statistics/handle.Handle.SetStatsCacheCapacity` —
//!   [`DomainSysVarEnv::stats_handle_set_cache_capacity`], which returns
//!   `false` for Go's `statsHandle == nil` "from test" escape.
//! - `// boundary:` Go `variable.EnableGlobalResourceControlFunc` /
//!   `DisableGlobalResourceControlFunc` —
//!   [`DomainSysVarEnv::enable_global_resource_control`] /
//!   `disable_global_resource_control`.
//! - `// boundary:` Go `tikv.ChangePDRegionMetaCircuitBreakerSettings` with
//!   `circuitbreaker.Settings.ErrorRateThresholdPct` —
//!   [`DomainSysVarEnv::set_pd_region_meta_circuit_breaker_error_rate_threshold_pct`].
//!   Go passes a mutator closure over the whole `Settings`; it only ever
//!   writes that one field, so the narrowing is to that field.
//! - `// boundary:` Go `vardef.MaxTSOBatchWaitInterval`,
//!   `vardef.EnableTSOFollowerProxy`, `vardef.EnablePDFollowerHandleRegion`,
//!   `vardef.EnableBatchQueryRegion` — process-global atomics in
//!   `pkg/sessionctx/vardef` with no Rust counterpart yet, so they are
//!   [`DomainSysVarEnv`] `store_*` methods rather than statics owned here.
//! - `// boundary:` Go `variable.TiDBOptOn` — already ported as
//!   `tidb_exec::option_values::tidb_opt_on`; re-stated here as
//!   [`tidb_opt_on`] because `tidb-exec` pulls in `tidb-planner` and
//!   `tidb-executor`, which `pkg/domain` must stay below.
//! - `// boundary:` Go `variable.ErrWrongValueForVar` —
//!   [`DomainSysVarError::WrongValueForVar`], MySQL code 1231.
//! - `// boundary:` Go `strconv.ParseFloat(sVal, 64)` — Rust's `f64` parser
//!   is used. It rejects a few strings Go accepts (hex-float literals such
//!   as `0x1p3`, and digit separators). That is the safe direction: those
//!   inputs error out here instead of silently setting a different wait
//!   interval. Every value the sysvar's own validation admits is a plain
//!   decimal, which both parsers read identically.
//! - `// boundary:` Go `time.Duration(float64(time.Millisecond)*val)` — Go
//!   truncates the product toward zero into a signed 64-bit nanosecond
//!   count, and a negative `val` yields a negative Duration. Rust's
//!   `Duration` cannot be negative, so [`PdDynamicOption`] carries signed
//!   nanoseconds instead of a `Duration`, preserving the value rather than
//!   clamping it.

use tidb_error::tidb::errcode;
use tidb_vardef::tidb_vars;

/// Go `variable.TiDBOptOn` (`varsutil.go:184`).
///
/// boundary: Go `pkg/sessionctx/variable.TiDBOptOn`.
#[must_use]
pub fn tidb_opt_on(opt: &str) -> bool {
    opt.eq_ignore_ascii_case("ON") || opt == "1"
}

/// The PD client dynamic options `domain_sysvars.go` sets.
///
/// boundary: Go `github.com/tikv/pd/client/opt.DynamicOption`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PdDynamicOption {
    /// Go `opt.MaxTSOBatchWaitInterval`, in signed nanoseconds — see the
    /// module doc for why this is not a `Duration`.
    MaxTsoBatchWaitInterval(i64),
    /// Go `opt.EnableTSOFollowerProxy`.
    EnableTsoFollowerProxy(bool),
    /// Go `opt.EnableFollowerHandle`. Note: only used for the region API
    /// today; if PD grows more follower-served APIs this option may change.
    EnableFollowerHandle(bool),
    /// Go `opt.TSOClientRPCConcurrency`.
    TsoClientRpcConcurrency(i32),
    /// Go `opt.EnableRouterClient`.
    EnableRouterClient(bool),
}

/// Errors `domain_sysvars.go` can return.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DomainSysVarError {
    /// Go `variable.ErrWrongValueForVar.GenWithStackByArgs(name, sVal)` (1231).
    WrongValueForVar {
        /// The variable name.
        name: String,
        /// The rejected value.
        value: String,
    },
    /// Go's `strconv.ParseFloat` failure, propagated as-is.
    ParseFloat(String),
    /// Whatever PD, the oracle, or the stats handle reported.
    Backend(String),
}

impl DomainSysVarError {
    /// The MySQL error code, where there is one.
    #[must_use]
    pub fn code(&self) -> Option<u16> {
        match self {
            Self::WrongValueForVar { .. } => Some(errcode::ErrWrongValueForVar),
            Self::ParseFloat(_) | Self::Backend(_) => None,
        }
    }
}

impl std::fmt::Display for DomainSysVarError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongValueForVar { name, value } => {
                write!(f, "Incorrect value for variable '{name}': '{value}'")
            }
            Self::ParseFloat(msg) | Self::Backend(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for DomainSysVarError {}

/// Everything `domain_sysvars.go` reaches through its `*Domain` receiver.
///
/// boundary: Go `pkg/domain.Domain` — see the module doc for the Go symbol
/// behind each method.
pub trait DomainSysVarEnv {
    /// boundary: Go `Domain.store.(interface{ GetPDClient() pd.Client })`
    /// succeeding *and* the returned client being non-nil. False means Go's
    /// `updatePDClient` returns `nil` without applying anything.
    fn pd_client_present(&self) -> bool;

    /// boundary: Go `pd.Client.UpdateOption(option, val)`.
    ///
    /// # Errors
    /// Whatever PD reports.
    fn pd_update_option(&self, option: PdDynamicOption) -> Result<(), DomainSysVarError>;

    /// boundary: Go `vardef.MaxTSOBatchWaitInterval.Store(val)`.
    fn store_max_tso_batch_wait_interval(&self, val: f64);

    /// boundary: Go `vardef.EnableTSOFollowerProxy.Store(val)`.
    fn store_enable_tso_follower_proxy(&self, val: bool);

    /// boundary: Go `vardef.EnablePDFollowerHandleRegion.Store(val)`.
    fn store_enable_pd_follower_handle_region(&self, val: bool);

    /// boundary: Go `vardef.EnableBatchQueryRegion.Store(val)`.
    fn store_enable_batch_query_region(&self, val: bool);

    /// boundary: Go `Domain.StatsHandle().SetStatsCacheCapacity(c)`.
    /// Returns false for Go's `statsHandle == nil` escape, which the comment
    /// there marks as reachable only "from test".
    fn stats_handle_set_cache_capacity(&self, capacity: i64) -> bool;

    /// boundary: Go `variable.EnableGlobalResourceControlFunc()`.
    fn enable_global_resource_control(&self);

    /// boundary: Go `variable.DisableGlobalResourceControlFunc()`.
    fn disable_global_resource_control(&self);

    /// boundary: Go
    /// `Domain.store.GetOracle().SetLowResolutionTimestampUpdateInterval(interval)`.
    ///
    /// # Errors
    /// Whatever the oracle reports.
    fn oracle_set_low_resolution_timestamp_update_interval(
        &self,
        interval_nanos: i64,
    ) -> Result<(), DomainSysVarError>;

    /// boundary: Go `Domain.store.GetOracle().SetExternalTimestamp(ctx, ts)`.
    ///
    /// # Errors
    /// Whatever the oracle reports.
    fn oracle_set_external_timestamp(&self, ts: u64) -> Result<(), DomainSysVarError>;

    /// boundary: Go `Domain.store.GetOracle().GetExternalTimestamp(ctx)`.
    ///
    /// # Errors
    /// Whatever the oracle reports.
    fn oracle_get_external_timestamp(&self) -> Result<u64, DomainSysVarError>;

    /// boundary: Go `tikv.ChangePDRegionMetaCircuitBreakerSettings(func(c
    /// *circuitbreaker.Settings) { c.ErrorRateThresholdPct = ratio })`.
    fn set_pd_region_meta_circuit_breaker_error_rate_threshold_pct(&self, pct: u32);
}

/// Go `Domain.setStatsCacheCapacity`.
///
/// Go calls `do.StatsHandle()` twice — once for the nil check and once for
/// the call — which is a redundant re-read, not observable behavior; the
/// single [`DomainSysVarEnv::stats_handle_set_cache_capacity`] covers both.
pub fn set_stats_cache_capacity<E: DomainSysVarEnv + ?Sized>(env: &E, capacity: i64) {
    // A nil stats handle (from test) is silently ignored.
    let _applied = env.stats_handle_set_cache_capacity(capacity);
}

/// Go `Domain.updatePDClient`.
///
/// # Errors
/// Whatever PD reports. A missing PD client is not an error.
pub fn update_pd_client<E: DomainSysVarEnv + ?Sized>(
    env: &E,
    option: PdDynamicOption,
) -> Result<(), DomainSysVarError> {
    if !env.pd_client_present() {
        return Ok(());
    }
    env.pd_update_option(option)
}

/// Go `Domain.setPDClientDynamicOption`.
///
/// # Errors
/// [`DomainSysVarError::ParseFloat`] for an unparseable
/// `tidb_tso_client_batch_max_wait_time`,
/// [`DomainSysVarError::WrongValueForVar`] for an unrecognized
/// `tidb_tso_client_rpc_mode`, or whatever PD reports.
pub fn set_pd_client_dynamic_option<E: DomainSysVarEnv + ?Sized>(
    env: &E,
    name: &str,
    s_val: &str,
) -> Result<(), DomainSysVarError> {
    match name {
        tidb_vars::TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME => {
            let val: f64 = s_val
                .parse()
                .map_err(|e: std::num::ParseFloatError| e.to_string())
                .map_err(DomainSysVarError::ParseFloat)?;
            // Go: time.Duration(float64(time.Millisecond) * val), i.e.
            // truncate toward zero into signed nanoseconds.
            let nanos = (val * 1_000_000.0) as i64;
            update_pd_client(env, PdDynamicOption::MaxTsoBatchWaitInterval(nanos))?;
            env.store_max_tso_batch_wait_interval(val);
        }
        tidb_vars::TIDB_ENABLE_TSO_FOLLOWER_PROXY => {
            let val = tidb_opt_on(s_val);
            update_pd_client(env, PdDynamicOption::EnableTsoFollowerProxy(val))?;
            env.store_enable_tso_follower_proxy(val);
        }
        tidb_vars::PD_ENABLE_FOLLOWER_HANDLE_REGION => {
            let val = tidb_opt_on(s_val);
            update_pd_client(env, PdDynamicOption::EnableFollowerHandle(val))?;
            env.store_enable_pd_follower_handle_region(val);
        }
        tidb_vars::TIDB_TSO_CLIENT_RPC_MODE => {
            let concurrency = match s_val {
                tidb_vars::TSO_CLIENT_RPC_MODE_DEFAULT => 1,
                tidb_vars::TSO_CLIENT_RPC_MODE_PARALLEL => 2,
                tidb_vars::TSO_CLIENT_RPC_MODE_PARALLEL_FAST => 4,
                _ => {
                    return Err(DomainSysVarError::WrongValueForVar {
                        name: name.to_owned(),
                        value: s_val.to_owned(),
                    })
                }
            };
            // No vardef mirror for this one — see the module doc.
            update_pd_client(env, PdDynamicOption::TsoClientRpcConcurrency(concurrency))?;
        }
        tidb_vars::TIDB_ENABLE_BATCH_QUERY_REGION => {
            let val = tidb_opt_on(s_val);
            update_pd_client(env, PdDynamicOption::EnableRouterClient(val))?;
            env.store_enable_batch_query_region(val);
        }
        // Go's switch has no default: an unknown name is silently accepted.
        _ => {}
    }
    Ok(())
}

/// Go `Domain.setGlobalResourceControl`.
pub fn set_global_resource_control<E: DomainSysVarEnv + ?Sized>(env: &E, enable: bool) {
    if enable {
        env.enable_global_resource_control();
    } else {
        env.disable_global_resource_control();
    }
}

/// Go `Domain.setLowResolutionTSOUpdateInterval`.
///
/// # Errors
/// Whatever the oracle reports.
pub fn set_low_resolution_tso_update_interval<E: DomainSysVarEnv + ?Sized>(
    env: &E,
    interval_nanos: i64,
) -> Result<(), DomainSysVarError> {
    env.oracle_set_low_resolution_timestamp_update_interval(interval_nanos)
}

/// Go `Domain.setExternalTimestamp`.
///
/// # Errors
/// Whatever the oracle reports.
pub fn set_external_timestamp<E: DomainSysVarEnv + ?Sized>(
    env: &E,
    ts: u64,
) -> Result<(), DomainSysVarError> {
    env.oracle_set_external_timestamp(ts)
}

/// Go `Domain.getExternalTimestamp`.
///
/// # Errors
/// Whatever the oracle reports.
pub fn get_external_timestamp<E: DomainSysVarEnv + ?Sized>(
    env: &E,
) -> Result<u64, DomainSysVarError> {
    env.oracle_get_external_timestamp()
}

/// Go `changePDMetadataCircuitBreakerErrorRateThresholdRatio`.
pub fn change_pd_metadata_circuit_breaker_error_rate_threshold_ratio<
    E: DomainSysVarEnv + ?Sized,
>(
    env: &E,
    error_rate_ratio: u32,
) {
    env.set_pd_region_meta_circuit_breaker_error_rate_threshold_pct(error_rate_ratio);
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    // `pkg/domain` has no upstream test naming any symbol in
    // `domain_sysvars.go` (grepped across all of `pkg/domain/*_test.go`);
    // the behavior is exercised only through testkit `SET GLOBAL` cases
    // elsewhere in the tree. These tests are therefore written, not
    // transcreated.

    #[derive(Default)]
    struct MockEnv {
        pd_present: bool,
        pd_err: Option<String>,
        pd_calls: RefCell<Vec<PdDynamicOption>>,
        stores: RefCell<Vec<String>>,
        stats_handle: bool,
        stats_calls: RefCell<Vec<i64>>,
        rc_calls: RefCell<Vec<bool>>,
        oracle_calls: RefCell<Vec<String>>,
        breaker_calls: RefCell<Vec<u32>>,
    }

    impl MockEnv {
        fn with_pd() -> Self {
            Self {
                pd_present: true,
                stats_handle: true,
                ..Self::default()
            }
        }
    }

    impl DomainSysVarEnv for MockEnv {
        fn pd_client_present(&self) -> bool {
            self.pd_present
        }
        fn pd_update_option(&self, option: PdDynamicOption) -> Result<(), DomainSysVarError> {
            self.pd_calls.borrow_mut().push(option);
            match &self.pd_err {
                Some(e) => Err(DomainSysVarError::Backend(e.clone())),
                None => Ok(()),
            }
        }
        fn store_max_tso_batch_wait_interval(&self, val: f64) {
            self.stores.borrow_mut().push(format!("wait={val}"));
        }
        fn store_enable_tso_follower_proxy(&self, val: bool) {
            self.stores.borrow_mut().push(format!("proxy={val}"));
        }
        fn store_enable_pd_follower_handle_region(&self, val: bool) {
            self.stores.borrow_mut().push(format!("follower={val}"));
        }
        fn store_enable_batch_query_region(&self, val: bool) {
            self.stores.borrow_mut().push(format!("batch={val}"));
        }
        fn stats_handle_set_cache_capacity(&self, capacity: i64) -> bool {
            if self.stats_handle {
                self.stats_calls.borrow_mut().push(capacity);
            }
            self.stats_handle
        }
        fn enable_global_resource_control(&self) {
            self.rc_calls.borrow_mut().push(true);
        }
        fn disable_global_resource_control(&self) {
            self.rc_calls.borrow_mut().push(false);
        }
        fn oracle_set_low_resolution_timestamp_update_interval(
            &self,
            interval_nanos: i64,
        ) -> Result<(), DomainSysVarError> {
            self.oracle_calls
                .borrow_mut()
                .push(format!("lowres={interval_nanos}"));
            Ok(())
        }
        fn oracle_set_external_timestamp(&self, ts: u64) -> Result<(), DomainSysVarError> {
            self.oracle_calls.borrow_mut().push(format!("set={ts}"));
            Ok(())
        }
        fn oracle_get_external_timestamp(&self) -> Result<u64, DomainSysVarError> {
            self.oracle_calls.borrow_mut().push("get".to_owned());
            Ok(42)
        }
        fn set_pd_region_meta_circuit_breaker_error_rate_threshold_pct(&self, pct: u32) {
            self.breaker_calls.borrow_mut().push(pct);
        }
    }

    #[test]
    fn tidb_opt_on_matches_go() {
        assert!(tidb_opt_on("ON"));
        assert!(tidb_opt_on("on"));
        assert!(tidb_opt_on("On"));
        assert!(tidb_opt_on("1"));
        assert!(!tidb_opt_on("OFF"));
        assert!(!tidb_opt_on("true"));
        // Go compares "1" byte-for-byte, so a padded or alternate spelling
        // of one is off.
        assert!(!tidb_opt_on(" 1"));
        assert!(!tidb_opt_on("01"));
    }

    #[test]
    fn batch_max_wait_time_converts_millis_to_nanos_and_mirrors_after_pd() {
        let env = MockEnv::with_pd();
        set_pd_client_dynamic_option(&env, tidb_vars::TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME, "0.5")
            .unwrap();
        assert_eq!(
            *env.pd_calls.borrow(),
            vec![PdDynamicOption::MaxTsoBatchWaitInterval(500_000)]
        );
        assert_eq!(*env.stores.borrow(), vec!["wait=0.5"]);
    }

    #[test]
    fn batch_max_wait_time_rejects_a_non_number_before_touching_pd() {
        let env = MockEnv::with_pd();
        let err = set_pd_client_dynamic_option(
            &env,
            tidb_vars::TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME,
            "abc",
        )
        .unwrap_err();
        assert!(matches!(err, DomainSysVarError::ParseFloat(_)));
        assert!(env.pd_calls.borrow().is_empty());
        assert!(env.stores.borrow().is_empty());
    }

    #[test]
    fn a_pd_failure_leaves_the_vardef_mirror_untouched() {
        let env = MockEnv {
            pd_present: true,
            pd_err: Some("pd down".to_owned()),
            ..MockEnv::default()
        };
        let err =
            set_pd_client_dynamic_option(&env, tidb_vars::TIDB_ENABLE_TSO_FOLLOWER_PROXY, "ON")
                .unwrap_err();
        assert_eq!(err, DomainSysVarError::Backend("pd down".to_owned()));
        assert_eq!(
            *env.pd_calls.borrow(),
            vec![PdDynamicOption::EnableTsoFollowerProxy(true)]
        );
        assert!(env.stores.borrow().is_empty());
    }

    #[test]
    fn a_missing_pd_client_still_mirrors_and_reports_success() {
        // Go's updatePDClient returns nil when the store has no PD client,
        // so the SET appears to succeed and vardef is updated anyway.
        let env = MockEnv::default();
        set_pd_client_dynamic_option(&env, tidb_vars::PD_ENABLE_FOLLOWER_HANDLE_REGION, "1")
            .unwrap();
        assert!(env.pd_calls.borrow().is_empty());
        assert_eq!(*env.stores.borrow(), vec!["follower=true"]);
    }

    #[test]
    fn rpc_mode_maps_to_concurrency_and_writes_no_mirror() {
        for (val, want) in [
            (tidb_vars::TSO_CLIENT_RPC_MODE_DEFAULT, 1),
            (tidb_vars::TSO_CLIENT_RPC_MODE_PARALLEL, 2),
            (tidb_vars::TSO_CLIENT_RPC_MODE_PARALLEL_FAST, 4),
        ] {
            let env = MockEnv::with_pd();
            set_pd_client_dynamic_option(&env, tidb_vars::TIDB_TSO_CLIENT_RPC_MODE, val).unwrap();
            assert_eq!(
                *env.pd_calls.borrow(),
                vec![PdDynamicOption::TsoClientRpcConcurrency(want)]
            );
            assert!(env.stores.borrow().is_empty());
        }
    }

    #[test]
    fn rpc_mode_is_the_only_case_that_validates() {
        let env = MockEnv::with_pd();
        let err =
            set_pd_client_dynamic_option(&env, tidb_vars::TIDB_TSO_CLIENT_RPC_MODE, "parallel")
                .unwrap_err();
        assert_eq!(
            err,
            DomainSysVarError::WrongValueForVar {
                name: tidb_vars::TIDB_TSO_CLIENT_RPC_MODE.to_owned(),
                value: "parallel".to_owned(),
            }
        );
        assert_eq!(err.code(), Some(1231));
        assert!(env.pd_calls.borrow().is_empty());

        // Meanwhile the boolean cases accept anything, silently as false.
        let env2 = MockEnv::with_pd();
        set_pd_client_dynamic_option(&env2, tidb_vars::TIDB_ENABLE_BATCH_QUERY_REGION, "banana")
            .unwrap();
        assert_eq!(
            *env2.pd_calls.borrow(),
            vec![PdDynamicOption::EnableRouterClient(false)]
        );
        assert_eq!(*env2.stores.borrow(), vec!["batch=false"]);
    }

    #[test]
    fn an_unknown_name_is_silently_accepted() {
        let env = MockEnv::with_pd();
        set_pd_client_dynamic_option(&env, "tidb_not_a_pd_option", "whatever").unwrap();
        assert!(env.pd_calls.borrow().is_empty());
        assert!(env.stores.borrow().is_empty());
    }

    #[test]
    fn stats_cache_capacity_tolerates_a_nil_handle() {
        let env = MockEnv::with_pd();
        set_stats_cache_capacity(&env, 1024);
        assert_eq!(*env.stats_calls.borrow(), vec![1024]);

        let no_handle = MockEnv::default();
        set_stats_cache_capacity(&no_handle, 1024);
        assert!(no_handle.stats_calls.borrow().is_empty());
    }

    #[test]
    fn resource_control_dispatches_both_ways() {
        let env = MockEnv::with_pd();
        set_global_resource_control(&env, true);
        set_global_resource_control(&env, false);
        assert_eq!(*env.rc_calls.borrow(), vec![true, false]);
    }

    #[test]
    fn oracle_delegations_pass_through() {
        let env = MockEnv::with_pd();
        set_low_resolution_tso_update_interval(&env, 2_000_000).unwrap();
        set_external_timestamp(&env, 99).unwrap();
        assert_eq!(get_external_timestamp(&env).unwrap(), 42);
        assert_eq!(
            *env.oracle_calls.borrow(),
            vec!["lowres=2000000", "set=99", "get"]
        );
    }

    #[test]
    fn circuit_breaker_ratio_reaches_the_settings_field() {
        let env = MockEnv::with_pd();
        change_pd_metadata_circuit_breaker_error_rate_threshold_ratio(&env, 70);
        assert_eq!(*env.breaker_calls.borrow(), vec![70]);
    }

    #[test]
    fn update_pd_client_short_circuits_without_a_client() {
        let env = MockEnv::default();
        assert_eq!(
            update_pd_client(&env, PdDynamicOption::EnableFollowerHandle(true)),
            Ok(())
        );
        assert!(env.pd_calls.borrow().is_empty());
    }
}
