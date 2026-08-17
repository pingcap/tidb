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

//! Go `pkg/domain/sysvar_cache.go` lands complete.
//!
//! Every production symbol of that file is here: `sysVarCache`
//! ([`SysVarCache`]), `rebuildSysVarCacheIfNeeded`
//! ([`SysVarCache::rebuild_if_needed`]), `GetSessionCache`
//! ([`SysVarCache::get_session_cache`]), `GetGlobalVar`
//! ([`SysVarCache::get_global_var`]), `fetchTableValues`
//! ([`fetch_table_values`]), `overrideSysVarWithConfig`
//! ([`override_sysvar_with_config`]), and `rebuildSysVarCache`
//! ([`SysVarCache::rebuild`]).
//!
//! The cache replaces the old `GlobalVariableCache`: it holds every system
//! variable twice, once under the session view that a new session copies
//! wholesale into its `systems[]` map, and once under the global view that
//! `GetGlobalVar` reads. Both are rebuilt together, and only together — a
//! half-populated cache is treated as empty and rebuilt on the next read.
//!
//! Two Go behaviors here are load-bearing and are reproduced rather than
//! tidied, because both are visible through the package's own API:
//!
//! 1. `rebuildSysVarCache` writes `newGlobalCache[sv.Name] = sVal` *before*
//!    running `sVal = sv.ValidateWithRelaxedValidation(...)`. The value the
//!    cache serves is therefore the raw table value, while the value handed
//!    to `SetGlobal` is the validated one. When relaxed validation clamps or
//!    normalizes, `GetGlobalVar` returns the unclamped string.
//!    [`SysVarCache::rebuild`] does the same, in the same order.
//! 2. A `SetGlobal` failure is logged and then dropped: `err` is
//!    reassigned inside the loop and the function still returns `nil`. So a
//!    variable whose side effect failed to apply is nonetheless cached as if
//!    it had. Reproduced exactly — see
//!    [`SysVarCacheDeps::set_global`]'s doc.
//!
//! Narrowings, all named:
//!
//! - `// boundary:` Go `pkg/domain.Domain` — the file's functions are all
//!   `*Domain` methods, but the only `Domain` state they touch is
//!   `do.sysVarCache`, `do.sysSessionPool`, and `do.infoCache`. The cache is
//!   the receiver here; the other two become [`SysVarCacheDeps`] methods.
//! - `// boundary:` Go `pkg/sessionctx.Context` and
//!   `Domain.sysSessionPool` — `rebuildSysVarCache(nil)` borrows a session
//!   from the pool purely to get a restricted SQL executor and a
//!   `*variable.SessionVars` for validation. Both roles are folded into
//!   [`SysVarCacheDeps`], so the pool checkout/return (Go's
//!   `Get`/`defer Put`) is the implementor's business, not this module's.
//! - `// boundary:` Go
//!   `sessionctx.Context.GetRestrictedSQLExecutor().ExecRestrictedSQL` with
//!   `kv.WithInternalSourceType(ctx, kv.InternalTxnSysVar)` — narrowed to
//!   [`RestrictedSqlExecutor::exec_restricted_sysvar_sql`], which returns
//!   just the two string columns this one query selects. The internal source
//!   type is a tracing/throttling tag with no effect on the rows returned,
//!   so it is the implementor's to set; it is named in that method's doc.
//! - `// boundary:` Go `pkg/sessionctx/variable.GetSysVars()` and
//!   `variable.SysVar` — the registry is a process-global map in Go. Here
//!   [`SysVarCacheDeps::sys_vars`] supplies it and [`SysVarSpec`] carries
//!   the six fields this file reads. `SysVarSpec::skip_init` and
//!   `skip_sysvar_cache` are the *results* of Go's `SysVar.SkipInit()` and
//!   `SysVar.SkipSysvarCache()` (`variable.go:475` and `:489`), not the raw
//!   `skipInit` field. The already-ported registry is
//!   `tidb_session::sysvar`; it is deliberately not depended on, because
//!   `tidb-session` re-exports `tidb-planner` and `tidb-executor` and
//!   `pkg/domain` must stay below them.
//! - `// boundary:` Go `variable.SysVar.ValidateWithRelaxedValidation` and
//!   `variable.SysVar.SetGlobal` — [`SysVarCacheDeps`] methods. Go passes
//!   `ctx.GetSessionVars()` and `vardef.ScopeGlobal` to the first and
//!   `context.Background()` to the second; neither varies at this call site,
//!   so neither is a parameter here.
//! - `// boundary:` Go `pkg/config/deploymode.IsStarter()` and
//!   `pkg/config.GetMaxAllowedPacket()` — [`SysVarCacheDeps::is_starter`]
//!   and [`SysVarCacheDeps::max_allowed_packet`]. `deploymode::is_starter`
//!   exists in Rust as `tidb_config::deploymode::is_starter`, and is left to
//!   the implementor rather than called here so that `tidb-domain` keeps no
//!   dependency on the config tree for one boolean.
//! - `// boundary:` Go `Domain.infoCache.ReSize(vardef.SchemaVersionCacheLimit)`
//!   — `infoCache` is a `domain.go` field with no Rust home yet, so the
//!   resize is [`SysVarCacheDeps::resize_info_cache`], which the `domain.go`
//!   batch must implement. It is *not* dropped, because skipping it would
//!   silently pin the infoschema cache at its startup size.
//! - `// boundary:` Go `pkg/util/logutil` — every `BgLogger()` call is
//!   dropped; none changes a result.
//! - `// boundary:` Go `variable.ErrUnknownSystemVar` — reproduced as
//!   [`SysVarCacheError::UnknownSystemVar`], carrying MySQL code 1193 from
//!   `tidb_error`.
//!
//! Where this port was deliberately conservative: nothing in
//! [`SysVarCache::rebuild`] invents a value. If the table query fails the
//! rebuild fails and the old cache is left untouched (as in Go, which
//! returns before taking the write lock); if a variable is absent from
//! `mysql.global_variables` its compiled-in `SysVar.Value` is used, which is
//! exactly what Go does.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};

use tidb_error::tidb::errcode;

/// The one query `fetchTableValues` runs.
pub const SYSVAR_TABLE_QUERY: &str =
    "SELECT variable_name, variable_value FROM mysql.global_variables";

/// Go `vardef.MaxAllowedPacket` (`pkg/sessionctx/vardef/sysvar.go:300`).
pub const MAX_ALLOWED_PACKET: &str = "max_allowed_packet";

/// Errors this module can produce.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SysVarCacheError {
    /// Go `variable.ErrUnknownSystemVar.GenWithStackByArgs(name)` (1193).
    UnknownSystemVar(String),
    /// Any error surfaced by the restricted SQL executor. Go propagates the
    /// executor's own `error` untouched; the text is carried through here.
    Exec(String),
}

impl SysVarCacheError {
    /// The MySQL error code, where there is one.
    #[must_use]
    pub fn code(&self) -> Option<u16> {
        match self {
            Self::UnknownSystemVar(_) => Some(errcode::ErrUnknownSystemVariable),
            Self::Exec(_) => None,
        }
    }
}

impl std::fmt::Display for SysVarCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownSystemVar(name) => write!(f, "Unknown system variable '{name}'"),
            Self::Exec(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for SysVarCacheError {}

/// The fields of Go `variable.SysVar` that `sysvar_cache.go` reads.
///
/// boundary: Go `pkg/sessionctx/variable.SysVar`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SysVarSpec {
    /// Go `SysVar.Name`.
    pub name: String,
    /// Go `SysVar.Value`: the compiled-in default, used when the variable is
    /// absent from `mysql.global_variables`.
    pub value: String,
    /// Go `SysVar.IsInitedFromConfig` (`variable.go:97`). When true the
    /// table row is ignored and the instance's own value wins.
    pub is_inited_from_config: bool,
    /// The result of Go `SysVar.SkipInit()` (`variable.go:475`), i.e.
    /// `sv.skipInit || sv.IsNoop || !sv.HasSessionScope()`.
    pub skip_init: bool,
    /// Go `SysVar.HasGlobalScope()` (`variable.go:209`).
    pub has_global_scope: bool,
    /// The result of Go `SysVar.SkipSysvarCache()` (`variable.go:489`), true
    /// only for the GC variables and `tidb_external_ts`.
    pub skip_sysvar_cache: bool,
    /// Whether Go's `SysVar.SetGlobal` func pointer is non-nil. Go guards
    /// the call with `sv.SetGlobal != nil`, so the nil-ness is itself part
    /// of the variable's definition.
    pub has_set_global: bool,
}

/// The restricted SQL path `fetchTableValues` uses.
///
/// boundary: Go `sessionctx.Context.GetRestrictedSQLExecutor()` plus
/// `RestrictedSQLExecutor.ExecRestrictedSQL(ctx, nil, sql)`, called under
/// `kv.WithInternalSourceType(context.Background(), kv.InternalTxnSysVar)`.
/// The internal source type is a tracing tag that does not change the rows,
/// so implementors set it; this module does not model it.
pub trait RestrictedSqlExecutor {
    /// Run [`SYSVAR_TABLE_QUERY`] and return `(variable_name,
    /// variable_value)` per row, in row order.
    ///
    /// Go reads the columns with `row.GetString(0)` / `row.GetString(1)`;
    /// the narrowing to two strings is exactly what that call site consumes.
    ///
    /// # Errors
    /// Whatever the executor reports.
    fn exec_restricted_sysvar_sql(
        &self,
        sql: &str,
    ) -> Result<Vec<(String, String)>, SysVarCacheError>;
}

/// Everything `sysvar_cache.go` reaches for through its `*Domain` receiver.
///
/// boundary: Go `pkg/domain.Domain` — see the module doc for the per-method
/// Go symbol each of these stands for.
pub trait SysVarCacheDeps: RestrictedSqlExecutor {
    /// boundary: Go `variable.GetSysVars()`. Go iterates a map, so the
    /// order is unspecified; anything order-dependent here would be a bug in
    /// Go too.
    fn sys_vars(&self) -> Vec<SysVarSpec>;

    /// boundary: Go `pkg/config/deploymode.IsStarter()`.
    fn is_starter(&self) -> bool;

    /// boundary: Go `pkg/config.GetMaxAllowedPacket()`.
    fn max_allowed_packet(&self) -> u64;

    /// boundary: Go `SysVar.ValidateWithRelaxedValidation(ctx.GetSessionVars(),
    /// value, vardef.ScopeGlobal)`. Relaxed validation never errors; it
    /// returns the closest acceptable string, so the return type is a plain
    /// `String`.
    fn validate_with_relaxed_validation(&self, sv: &SysVarSpec, value: &str) -> String;

    /// boundary: Go `SysVar.SetGlobal(context.Background(),
    /// ctx.GetSessionVars(), value)`.
    ///
    /// Go logs a failure and carries on — the rebuild still succeeds and the
    /// variable is still cached. [`SysVarCache::rebuild`] does the same, so
    /// returning `Err` here only suppresses the side effect; it does not
    /// fail the rebuild.
    ///
    /// # Errors
    /// Whatever applying the global value reports.
    fn set_global(&self, sv: &SysVarSpec, value: &str) -> Result<(), String>;

    /// boundary: Go `Domain.infoCache.ReSize(int(vardef.SchemaVersionCacheLimit.Load()))`.
    fn resize_info_cache(&self);
}

/// Go `fetchTableValues`: read `mysql.global_variables` into a map.
///
/// Go builds a `map[string]string` by assignment, so a duplicate
/// `variable_name` in the table keeps the *last* row. Preserved.
///
/// # Errors
/// Propagates the executor's error, exactly as Go returns `nil, err`.
pub fn fetch_table_values<E: RestrictedSqlExecutor + ?Sized>(
    exec: &E,
) -> Result<HashMap<String, String>, SysVarCacheError> {
    let mut table_contents = HashMap::new();
    let rows = exec.exec_restricted_sysvar_sql(SYSVAR_TABLE_QUERY)?;
    for (name, val) in rows {
        table_contents.insert(name, val);
    }
    Ok(table_contents)
}

/// Go `overrideSysVarWithConfig`.
///
/// Only overrides when the key is already present — a
/// `mysql.global_variables` row for `max_allowed_packet` is required before
/// the config value can win. An absent key stays absent, so the compiled-in
/// `SysVar.Value` is used later instead.
pub fn override_sysvar_with_config(
    table_content: &mut HashMap<String, String>,
    max_allowed_packet: u64,
) {
    if let Some(slot) = table_content.get_mut(MAX_ALLOWED_PACKET) {
        // Go: strconv.FormatUint(config.GetMaxAllowedPacket(), 10).
        *slot = max_allowed_packet.to_string();
    }
}

/// Go `sysVarCache`: system variables split into session and global views.
#[derive(Debug, Default)]
pub struct SysVarCache {
    /// Go's `syncutil.RWMutex` guarding `global` and `session`, which are
    /// only ever read or swapped as a pair.
    maps: RwLock<CacheMaps>,
    /// Go `rebuildLock`: only one rebuild at a time, so an earlier
    /// `fetchTableValues` that finishes last cannot clobber a later one.
    rebuild_lock: Mutex<()>,
}

#[derive(Debug, Default)]
struct CacheMaps {
    /// Go `global`.
    global: HashMap<String, String>,
    /// Go `session`.
    session: HashMap<String, String>,
}

impl SysVarCache {
    /// An empty cache, as Go's zero-value `sysVarCache`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `rebuildSysVarCacheIfNeeded`.
    ///
    /// Either map being empty counts as "needs rebuild" — not both — so a
    /// build that produced only globals is discarded and redone.
    ///
    /// Note the Go control flow this keeps: the rebuild's error is logged
    /// *and* returned, so the caller sees it.
    ///
    /// # Errors
    /// Whatever [`SysVarCache::rebuild`] reports.
    pub fn rebuild_if_needed<D: SysVarCacheDeps + ?Sized>(
        &self,
        deps: &D,
    ) -> Result<(), SysVarCacheError> {
        let cache_needs_rebuild = {
            let maps = self.read_maps();
            maps.session.is_empty() || maps.global.is_empty()
        };
        if cache_needs_rebuild {
            // boundary: Go `logutil.BgLogger().Warn("sysvar cache is empty,
            // triggering rebuild")`.
            self.rebuild(deps)?;
        }
        Ok(())
    }

    /// Go `Domain.GetSessionCache`: a copy of the session view, intended to
    /// be assigned straight into a new session's `systems[]` map.
    ///
    /// Go calls this a deep copy; `maps.Clone` is in fact shallow, which for
    /// `map[string]string` is the same thing.
    ///
    /// # Errors
    /// Whatever the rebuild reports.
    pub fn get_session_cache<D: SysVarCacheDeps + ?Sized>(
        &self,
        deps: &D,
    ) -> Result<HashMap<String, String>, SysVarCacheError> {
        self.rebuild_if_needed(deps)?;
        Ok(self.read_maps().session.clone())
    }

    /// Go `Domain.GetGlobalVar`: one global value out of the cache.
    ///
    /// # Errors
    /// [`SysVarCacheError::UnknownSystemVar`] when the name is not in the
    /// global view — including for a variable that exists but has no global
    /// scope, which is what Go reports too.
    pub fn get_global_var<D: SysVarCacheDeps + ?Sized>(
        &self,
        deps: &D,
        name: &str,
    ) -> Result<String, SysVarCacheError> {
        self.rebuild_if_needed(deps)?;
        let maps = self.read_maps();
        match maps.global.get(name) {
            Some(val) => Ok(val.clone()),
            // boundary: Go `logutil.BgLogger().Warn("could not find key in
            // global cache")`.
            None => Err(SysVarCacheError::UnknownSystemVar(name.to_owned())),
        }
    }

    /// Go `rebuildSysVarCache`: rebuild both views from
    /// `mysql.global_variables` plus the compiled-in registry.
    ///
    /// # Errors
    /// Only [`fetch_table_values`] can fail the rebuild. A `SetGlobal`
    /// failure is swallowed, exactly as in Go.
    pub fn rebuild<D: SysVarCacheDeps + ?Sized>(&self, deps: &D) -> Result<(), SysVarCacheError> {
        let mut new_session_cache: HashMap<String, String> = HashMap::new();
        let mut new_global_cache: HashMap<String, String> = HashMap::new();

        // Only one rebuild can be in progress at a time; this prevents a
        // lost update race where an earlier fetch finishes last.
        let _rebuild = self
            .rebuild_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let mut table_contents = fetch_table_values(deps)?;

        if deps.is_starter() {
            override_sysvar_with_config(&mut table_contents, deps.max_allowed_packet());
        }

        for sv in deps.sys_vars() {
            let mut s_val = sv.value.clone();
            // NOTE: instance variables use the value stored in this instance.
            if let Some(from_table) = table_contents.get(&sv.name) {
                if !sv.is_inited_from_config {
                    s_val.clone_from(from_table);
                }
            }
            // The session cache stores non-skippable variables, which
            // essentially means session scope. For historical reasons some
            // globals are in here too.
            if !sv.skip_init {
                new_session_cache.insert(sv.name.clone(), s_val.clone());
            }
            if sv.has_global_scope {
                // Deliberately the *unvalidated* value — see the module doc.
                new_global_cache.insert(sv.name.clone(), s_val.clone());

                // SET GLOBAL only calls SetGlobal on the calling instance,
                // so running it here is what makes it take effect on every
                // TiDB server. INSTANCE-scoped vars never get here.
                if sv.has_set_global && !sv.skip_sysvar_cache {
                    s_val = deps.validate_with_relaxed_validation(&sv, &s_val);
                    // boundary: Go logs the error and moves on; the rebuild
                    // still returns nil and the variable stays cached.
                    let _ = deps.set_global(&sv, &s_val);
                }
            }
        }

        {
            let mut maps = self
                .maps
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            maps.session = new_session_cache;
            maps.global = new_global_cache;
        }
        deps.resize_info_cache();
        Ok(())
    }

    /// Test/`domain.go` hook matching Go's direct
    /// `dom.sysVarCache.Lock(); dom.sysVarCache.global = ...` in
    /// `domain_test.go`.
    pub fn set_global_for_test(&self, global: HashMap<String, String>) {
        self.maps
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .global = global;
    }

    fn read_maps(&self) -> std::sync::RwLockReadGuard<'_, CacheMaps> {
        self.maps
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    /// A `SysVarCacheDeps` whose every collaborator is scriptable.
    ///
    /// `pkg/domain`'s own tests for this file are testkit-bound — they need
    /// a bootstrapped store with a real `mysql.global_variables` table — so
    /// nothing here is transcreated from an upstream case. The one direct
    /// upstream touch, `TestClosestReplicaReadChecker`
    /// (`domain_test.go:268-272`), only pokes `sysVarCache.global` directly;
    /// that access shape is preserved as
    /// [`SysVarCache::set_global_for_test`] and exercised below.
    struct MockDeps {
        rows: RefCell<Vec<(String, String)>>,
        exec_err: Option<String>,
        vars: Vec<SysVarSpec>,
        starter: bool,
        packet: u64,
        /// Names for which relaxed validation rewrites the value.
        clamp_to: Option<String>,
        set_global_err: bool,
        set_global_calls: RefCell<Vec<(String, String)>>,
        resize_calls: RefCell<usize>,
        exec_calls: RefCell<usize>,
    }

    impl Default for MockDeps {
        fn default() -> Self {
            Self {
                rows: RefCell::new(Vec::new()),
                exec_err: None,
                vars: Vec::new(),
                starter: false,
                packet: 67_108_864,
                clamp_to: None,
                set_global_err: false,
                set_global_calls: RefCell::new(Vec::new()),
                resize_calls: RefCell::new(0),
                exec_calls: RefCell::new(0),
            }
        }
    }

    impl RestrictedSqlExecutor for MockDeps {
        fn exec_restricted_sysvar_sql(
            &self,
            sql: &str,
        ) -> Result<Vec<(String, String)>, SysVarCacheError> {
            assert_eq!(sql, SYSVAR_TABLE_QUERY);
            *self.exec_calls.borrow_mut() += 1;
            match &self.exec_err {
                Some(e) => Err(SysVarCacheError::Exec(e.clone())),
                None => Ok(self.rows.borrow().clone()),
            }
        }
    }

    impl SysVarCacheDeps for MockDeps {
        fn sys_vars(&self) -> Vec<SysVarSpec> {
            self.vars.clone()
        }
        fn is_starter(&self) -> bool {
            self.starter
        }
        fn max_allowed_packet(&self) -> u64 {
            self.packet
        }
        fn validate_with_relaxed_validation(&self, _sv: &SysVarSpec, value: &str) -> String {
            self.clamp_to.clone().unwrap_or_else(|| value.to_owned())
        }
        fn set_global(&self, sv: &SysVarSpec, value: &str) -> Result<(), String> {
            self.set_global_calls
                .borrow_mut()
                .push((sv.name.clone(), value.to_owned()));
            if self.set_global_err {
                Err("boom".to_owned())
            } else {
                Ok(())
            }
        }
        fn resize_info_cache(&self) {
            *self.resize_calls.borrow_mut() += 1;
        }
    }

    fn sv(name: &str, value: &str) -> SysVarSpec {
        SysVarSpec {
            name: name.to_owned(),
            value: value.to_owned(),
            has_global_scope: true,
            ..SysVarSpec::default()
        }
    }

    #[test]
    fn fetch_table_values_keeps_the_last_duplicate_row() {
        let deps = MockDeps {
            rows: RefCell::new(vec![
                ("a".to_owned(), "1".to_owned()),
                ("a".to_owned(), "2".to_owned()),
                ("b".to_owned(), "3".to_owned()),
            ]),
            ..MockDeps::default()
        };
        let got = fetch_table_values(&deps).unwrap();
        assert_eq!(got.get("a").map(String::as_str), Some("2"));
        assert_eq!(got.get("b").map(String::as_str), Some("3"));
    }

    #[test]
    fn fetch_table_values_propagates_the_executor_error() {
        let deps = MockDeps {
            exec_err: Some("nope".to_owned()),
            ..MockDeps::default()
        };
        assert_eq!(
            fetch_table_values(&deps),
            Err(SysVarCacheError::Exec("nope".to_owned()))
        );
    }

    #[test]
    fn override_only_fires_when_the_row_exists() {
        let mut absent = HashMap::new();
        override_sysvar_with_config(&mut absent, 4096);
        assert!(absent.is_empty());

        let mut present: HashMap<String, String> =
            [(MAX_ALLOWED_PACKET.to_owned(), "1024".to_owned())]
                .into_iter()
                .collect();
        override_sysvar_with_config(&mut present, 4096);
        assert_eq!(present[MAX_ALLOWED_PACKET], "4096");
    }

    #[test]
    fn override_is_skipped_outside_starter_mode() {
        let deps = MockDeps {
            rows: RefCell::new(vec![(MAX_ALLOWED_PACKET.to_owned(), "1024".to_owned())]),
            vars: vec![sv(MAX_ALLOWED_PACKET, "67108864")],
            starter: false,
            packet: 4096,
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        cache.rebuild(&deps).unwrap();
        assert_eq!(
            cache.get_global_var(&deps, MAX_ALLOWED_PACKET).unwrap(),
            "1024"
        );

        let starter = MockDeps {
            rows: RefCell::new(vec![(MAX_ALLOWED_PACKET.to_owned(), "1024".to_owned())]),
            vars: vec![sv(MAX_ALLOWED_PACKET, "67108864")],
            starter: true,
            packet: 4096,
            ..MockDeps::default()
        };
        let cache2 = SysVarCache::new();
        cache2.rebuild(&starter).unwrap();
        assert_eq!(
            cache2.get_global_var(&starter, MAX_ALLOWED_PACKET).unwrap(),
            "4096"
        );
    }

    #[test]
    fn table_value_beats_the_compiled_default_unless_inited_from_config() {
        let deps = MockDeps {
            rows: RefCell::new(vec![
                ("plain".to_owned(), "from_table".to_owned()),
                ("from_config".to_owned(), "from_table".to_owned()),
            ]),
            vars: vec![
                sv("plain", "compiled"),
                SysVarSpec {
                    is_inited_from_config: true,
                    ..sv("from_config", "compiled")
                },
                sv("absent", "compiled"),
            ],
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        cache.rebuild(&deps).unwrap();
        assert_eq!(cache.get_global_var(&deps, "plain").unwrap(), "from_table");
        assert_eq!(
            cache.get_global_var(&deps, "from_config").unwrap(),
            "compiled"
        );
        assert_eq!(cache.get_global_var(&deps, "absent").unwrap(), "compiled");
    }

    #[test]
    fn skip_init_keeps_a_var_out_of_the_session_view_only() {
        let deps = MockDeps {
            vars: vec![
                SysVarSpec {
                    skip_init: true,
                    ..sv("global_only", "g")
                },
                sv("both", "b"),
                SysVarSpec {
                    has_global_scope: false,
                    ..sv("session_only", "s")
                },
            ],
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        cache.rebuild(&deps).unwrap();
        let session = cache.get_session_cache(&deps).unwrap();
        assert!(!session.contains_key("global_only"));
        assert_eq!(session["both"], "b");
        assert_eq!(session["session_only"], "s");

        assert!(cache.get_global_var(&deps, "global_only").is_ok());
        assert_eq!(
            cache.get_global_var(&deps, "session_only"),
            Err(SysVarCacheError::UnknownSystemVar(
                "session_only".to_owned()
            ))
        );
    }

    #[test]
    fn set_global_runs_only_for_global_scope_with_a_hook_and_no_skip() {
        let deps = MockDeps {
            vars: vec![
                SysVarSpec {
                    has_set_global: true,
                    ..sv("with_hook", "1")
                },
                sv("no_hook", "2"),
                SysVarSpec {
                    has_set_global: true,
                    skip_sysvar_cache: true,
                    ..sv("gc_var", "3")
                },
                SysVarSpec {
                    has_set_global: true,
                    has_global_scope: false,
                    ..sv("instance_var", "4")
                },
            ],
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        cache.rebuild(&deps).unwrap();
        assert_eq!(
            *deps.set_global_calls.borrow(),
            vec![("with_hook".to_owned(), "1".to_owned())]
        );
    }

    #[test]
    fn the_cache_keeps_the_unvalidated_value_while_set_global_gets_the_validated_one() {
        // This is Go quirk (1) from the module doc: the assignment into
        // newGlobalCache happens before ValidateWithRelaxedValidation runs.
        let deps = MockDeps {
            rows: RefCell::new(vec![("v".to_owned(), "999999".to_owned())]),
            vars: vec![SysVarSpec {
                has_set_global: true,
                ..sv("v", "1")
            }],
            clamp_to: Some("100".to_owned()),
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        cache.rebuild(&deps).unwrap();
        assert_eq!(cache.get_global_var(&deps, "v").unwrap(), "999999");
        assert_eq!(
            *deps.set_global_calls.borrow(),
            vec![("v".to_owned(), "100".to_owned())]
        );
    }

    #[test]
    fn a_failing_set_global_still_leaves_the_rebuild_successful_and_the_var_cached() {
        // Go quirk (2): the error is logged into `err` and then discarded.
        let deps = MockDeps {
            vars: vec![SysVarSpec {
                has_set_global: true,
                ..sv("v", "7")
            }],
            set_global_err: true,
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        assert_eq!(cache.rebuild(&deps), Ok(()));
        assert_eq!(cache.get_global_var(&deps, "v").unwrap(), "7");
    }

    #[test]
    fn a_failed_fetch_leaves_the_previous_cache_intact() {
        let good = MockDeps {
            vars: vec![sv("v", "old")],
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        cache.rebuild(&good).unwrap();

        let bad = MockDeps {
            exec_err: Some("down".to_owned()),
            vars: vec![sv("v", "new")],
            ..MockDeps::default()
        };
        assert_eq!(
            cache.rebuild(&bad),
            Err(SysVarCacheError::Exec("down".to_owned()))
        );
        assert_eq!(cache.get_global_var(&good, "v").unwrap(), "old");
        // The failed rebuild never reached the resize.
        assert_eq!(*bad.resize_calls.borrow(), 0);
    }

    #[test]
    fn reads_rebuild_an_empty_cache_exactly_once() {
        let deps = MockDeps {
            vars: vec![sv("v", "1")],
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        assert_eq!(cache.get_global_var(&deps, "v").unwrap(), "1");
        assert_eq!(*deps.exec_calls.borrow(), 1);
        // Now populated: the second read does not rebuild.
        assert_eq!(cache.get_global_var(&deps, "v").unwrap(), "1");
        assert_eq!(*deps.exec_calls.borrow(), 1);
        assert_eq!(*deps.resize_calls.borrow(), 1);
    }

    #[test]
    fn a_half_populated_cache_counts_as_empty() {
        // Only globals: `session` is empty, so every read rebuilds. This is
        // the state `set_global_for_test` leaves behind, mirroring
        // domain_test.go's direct write to `sysVarCache.global`.
        let deps = MockDeps {
            vars: vec![SysVarSpec {
                skip_init: true,
                ..sv("g", "1")
            }],
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        cache.rebuild(&deps).unwrap();
        assert!(cache.get_session_cache(&deps).unwrap().is_empty());
        assert_eq!(*deps.exec_calls.borrow(), 2);
        assert_eq!(cache.get_global_var(&deps, "g").unwrap(), "1");
        assert_eq!(*deps.exec_calls.borrow(), 3);
    }

    #[test]
    fn set_global_for_test_matches_domain_tests_direct_write() {
        let cache = SysVarCache::new();
        cache.set_global_for_test(
            [(
                "tidb_replica_read".to_owned(),
                "closest-adaptive".to_owned(),
            )]
            .into_iter()
            .collect(),
        );
        let deps = MockDeps {
            vars: vec![SysVarSpec {
                skip_init: true,
                ..sv("tidb_replica_read", "leader")
            }],
            ..MockDeps::default()
        };
        // Session is still empty, so the read rebuilds and the injected
        // value is replaced — the same thing that happens in Go, which is
        // why `TestClosestReplicaReadChecker` never reads it back through
        // `GetGlobalVar`.
        assert_eq!(
            cache.get_global_var(&deps, "tidb_replica_read").unwrap(),
            "leader"
        );
    }

    #[test]
    fn unknown_var_reports_1193() {
        let deps = MockDeps {
            vars: vec![sv("v", "1")],
            ..MockDeps::default()
        };
        let cache = SysVarCache::new();
        let err = cache.get_global_var(&deps, "nope").unwrap_err();
        assert_eq!(err, SysVarCacheError::UnknownSystemVar("nope".to_owned()));
        assert_eq!(err.code(), Some(1193));
    }
}
