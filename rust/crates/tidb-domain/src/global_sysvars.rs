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

//! The node's `variable.GlobalVarAccessor`: what Go's session answers
//! through the domain (`session.GetGlobalSysVar` -> `Domain.GetGlobalVar`,
//! `SetGlobalSysVar` -> `mysql.global_variables` + `Domain.NotifyUpdateSysVarCache`),
//! plus the process-local INSTANCE tier Go keeps in `vardef` atomics.
//!
//! GLOBAL scope: [`GlobalSysvars`] is the shared, `Arc`-backed accessor
//! every session a factory opens is given -- over the domain's sysvar cache
//! ([`crate::sysvar_cache::SysVarCache`]), rebuilt from
//! `mysql.global_variables`. `SET GLOBAL x = v` writes the row and rebuilds
//! the cache; the setting session's own `@@x` is untouched (MySQL's rule: a
//! live session keeps whatever it already has). A brand new session copies
//! the cache's session view once, at connect
//! (`SessionVars::seed_from_globals`, Go `loadCommonGlobalVariablesIfNeeded`)
//! -- so a session opened AFTER the `SET GLOBAL` sees the new value as its
//! session default, while sessions already open do not. A `SkipInit`
//! variable is not copied: the session reads its GLOBAL value at first use
//! and keeps it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use tidb_variable::sysvar::{alias_of, get_sys_var, SysVarDef, SCOPE_GLOBAL, SCOPE_INSTANCE};
use tidb_util::fixcontrol::OptimizerFixControl;
use tidb_variable::vars::{
    global_get_hook, parse_read_timestamp, validation_var_error,
    ExternalTimestampProvider, PdRegionPolicy, ResolvedGlobals, SET_GLOBAL_RESOURCE_CONTROL,
};
use tidb_variable::VarError;

/// The node's global variable accessor every session of one
/// `tidb_variable::Session` factory holds a clone of: Go's `GlobalVarAccessor`
/// over the domain's `sysVarCache`, plus the process-local INSTANCE tier.
///
/// The GLOBAL tier is [`crate::sysvar_cache::SysVarCache`], rebuilt
/// from `mysql.global_variables` as Go's `rebuildSysVarCache` does:
/// `@@global.x` reads its global view (`Domain.GetGlobalVar`) and a new
/// session copies its session view (`Domain.GetSessionCache`). A cluster
/// node persists `SET GLOBAL` through its sysvar seam and rebuilds from the
/// cluster's table (`LoadSysVarCacheLoop` and the `/tidb/sysvars` etcd
/// notify).
#[derive(Clone, Debug)]
pub struct GlobalSysvars {
    pd_region_policy: Arc<RwLock<Option<Arc<dyn PdRegionPolicy>>>>,
    external_timestamp: Arc<RwLock<Option<Arc<dyn ExternalTimestampProvider>>>>,
    /// Go `Domain.sysVarCache`: the global and session views every read and
    /// every new session is answered from. Shared by every clone.
    cache: Arc<crate::sysvar_cache::SysVarCache>,
    /// The `mysql.global_variables` rows the cache was last rebuilt from (Go
    /// `fetchTableValues`). For a store-backed table (an in-process catalog,
    /// a cluster) every rebuild replaces them from the store's own table; for
    /// a cluster scratch copy and a storeless factory these rows ARE the
    /// table.
    table: Arc<Mutex<HashMap<String, String>>>,
    /// Go `variable.SetSysVar` at startup: a config-derived value replacing a
    /// GLOBAL variable's registry `Value`, which the cache serves for a
    /// variable with no row.
    startup: Arc<Mutex<HashMap<String, String>>>,
    /// Go `SysVar.IsInitedFromConfig`: the variables `setInstanceVar` gave
    /// a config value, which the cache keeps over any table row.
    inited_from_config: Arc<Mutex<std::collections::HashSet<String>>>,
    /// The INSTANCE tier: Go `vardef.ScopeInstance`. A per-node value, held
    /// beside the global one rather than in it because the two tiers differ
    /// in exactly one way that matters -- Go persists `ScopeGlobal` to
    /// `mysql.GLOBAL_VARIABLES` and holds `ScopeInstance` in this process's
    /// own memory (`SetInstanceSysVar` writes an `atomic` in `vardef`, never
    /// a row). Keeping them in one map would make [`Self::overrides`] offer
    /// a node-local value to the cluster writer as if it were cluster state.
    ///
    /// 28 entries have this scope alone; the six that carry
    /// `ScopeGlobal|ScopeInstance` live in `values`, matching Go's
    /// `setSysVariable`, which sends anything `IsGlobal` to
    /// `SetGlobalSysVar`.
    instances: Arc<Mutex<HashMap<String, String>>>,
    /// Ordered INSTANCE-only writes made against a cluster transaction's
    /// scratch table. Cluster rows never contain this tier, so the writer must
    /// replay these mutations into the live node only after the statement's
    /// durable GLOBAL half has committed.
    instance_mutations: Option<Arc<Mutex<Vec<InstanceMutation>>>>,
    /// Scratch registries validate cluster writes before commit. They must not
    /// publish process-wide runtime settings until [`Self::replace_from`]
    /// makes the committed state live.
    publishes_runtime_settings: bool,
    /// The read-mostly image described on [`ResolvedGlobals`]. Writers swap it
    /// wholesale; readers clone the `Arc` under a read lock.
    resolved: Arc<RwLock<Arc<ResolvedGlobals>>>,
    /// Serializes rebuilds of `resolved`; see `refresh_resolved`.
    refresh_lock: Arc<Mutex<()>>,
}

#[derive(Clone, Debug)]
enum InstanceMutation {
    Set(String, String),
    Reset(String),
}


// Go Domain converts fractional milliseconds to integer nanoseconds.
fn tso_batch_wait(value: &str) -> std::time::Duration {
    let milliseconds = value.parse::<f64>().expect("validated TSO wait is numeric");
    std::time::Duration::from_nanos((milliseconds * 1_000_000.0) as u64)
}

/// Go's `TiDBTSOClientRPCMode` choice-to-concurrency mapping
/// (`pkg/domain/domain_sysvars.go:92-106`): `DEFAULT` sends one batch at a
/// time, `PARALLEL` allows two and `PARALLEL-FAST` four.
///
/// Go reaches its `ErrWrongValueForVar` arm only for a value the enum
/// validator already rejects, so an unknown spelling here means the catalog
/// and this mapping disagree; it falls back to Go's own default of 1 rather
/// than failing a publication that the validator already accepted.
fn tso_rpc_concurrency(value: &str) -> isize {
    if value.eq_ignore_ascii_case("PARALLEL") {
        2
    } else if value.eq_ignore_ascii_case("PARALLEL-FAST") {
        4
    } else {
        1
    }
}

impl Default for GlobalSysvars {
    fn default() -> Self {
        Self {
            external_timestamp: Arc::default(),
            pd_region_policy: Arc::default(),
            cache: Arc::default(),
            table: Arc::default(),
            startup: Arc::default(),
            inited_from_config: Arc::default(),
            instances: Arc::default(),
            instance_mutations: None,
            publishes_runtime_settings: true,
            resolved: Arc::new(RwLock::new(EMPTY_RESOLVED.with(|it| it.clone()))),
            refresh_lock: Arc::default(),
        }
    }
}

/// Go `session`'s `GlobalVarAccessor` implementation over the domain.
impl tidb_variable::GlobalVarAccessor for GlobalSysvars {
    fn get(&self, name: &str) -> Result<String, VarError> {
        GlobalSysvars::get(self, name)
    }

    fn get_for_global_scope(&self, name: &str) -> Result<String, VarError> {
        GlobalSysvars::get_for_global_scope(self, name)
    }

    fn get_by_registry_index(&self, index: usize) -> Result<String, VarError> {
        GlobalSysvars::get_by_registry_index(self, index)
    }

    fn set_with_time_zone(
        &self,
        name: &str,
        value: String,
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<bool, VarError> {
        GlobalSysvars::set_with_time_zone(self, name, value, zone)
    }

    fn set_instance(&self, name: &str, value: String) -> Result<bool, VarError> {
        GlobalSysvars::set_instance(self, name, value)
    }

    fn reset(&self, name: &str) -> Result<(), VarError> {
        GlobalSysvars::reset(self, name)
    }

    fn reset_instance(&self, name: &str) -> Result<(), VarError> {
        GlobalSysvars::reset_instance(self, name)
    }

    fn session_cache(&self) -> HashMap<String, String> {
        GlobalSysvars::session_cache(self)
    }

    fn overrides(&self) -> HashMap<String, String> {
        GlobalSysvars::overrides(self)
    }

    fn statement_memory_policy(&self) -> (tidb_vardef::OomAction, bool) {
        GlobalSysvars::statement_memory_policy(self)
    }

    fn check_mb4_value_in_utf8(&self) -> bool {
        GlobalSysvars::check_mb4_value_in_utf8(self)
    }

    fn enable_check_constraint(&self) -> bool {
        GlobalSysvars::enable_check_constraint(self)
    }

    fn resource_control_enabled(&self) -> bool {
        GlobalSysvars::resource_control_enabled(self)
    }

    fn resource_control_strict_mode(&self) -> bool {
        GlobalSysvars::resource_control_strict_mode(self)
    }

    fn external_timestamp_provider(&self) -> Option<Arc<dyn ExternalTimestampProvider>> {
        GlobalSysvars::external_timestamp_provider(self)
    }

    fn set_external_timestamp_provider(&self, provider: Arc<dyn ExternalTimestampProvider>) {
        GlobalSysvars::set_external_timestamp_provider(self, provider);
    }
}

impl tidb_variable::GlobalSysvarAccessor for GlobalSysvars {
    fn get_global_sysvar(&self, name: &str) -> Option<String> {
        self.get(name).ok()
    }
}

thread_local! {
    /// The empty image `Default` starts from; `SYS_VARS`' length is not yet
    /// readable in a `const` context, so the first real build sizes the table.
    static EMPTY_RESOLVED: Arc<ResolvedGlobals> = Arc::default();
}

/// Reads the live process/config products behind Go's instance-scoped
/// `GetSessionOrGlobalSystemVar` hooks. Explicit `SET INSTANCE` values remain
/// authoritative in the registry; otherwise the current process config and
/// vardef atomics are exposed instead of the catalog's bootstrap spelling.
fn runtime_instance_value(globals: &GlobalSysvars, def: &'static SysVarDef) -> Option<String> {
    if !def.has_instance_scope() || def.has_global_scope() {
        return None;
    }
    let lowered = tidb_variable::sysvar::lowered_if_needed(def.name);
    if let Some(value) = globals
        .store(def)
        .lock()
        .expect("instance sysvar lock poisoned")
        .get(lowered.as_ref())
    {
        return Some(value.clone());
    }

    let config = tidb_config::config_tree::config::get_global_config();
    let instance = &config.instance;
    match def.name {
        "tidb_stmt_summary_enable_persistent" => Some(if instance.stmt_summary_enable_persistent {
            "ON".to_owned()
        } else {
            "OFF".to_owned()
        }),
        "tidb_stmt_summary_filename" => Some(instance.stmt_summary_filename.clone()),
        "tidb_stmt_summary_file_max_days" => Some(instance.stmt_summary_file_max_days.to_string()),
        "tidb_stmt_summary_file_max_size" => Some(instance.stmt_summary_file_max_size.to_string()),
        "tidb_stmt_summary_file_max_backups" => {
            Some(instance.stmt_summary_file_max_backups.to_string())
        }
        "tidb_general_log" => Some(if instance.tidb_general_log {
            "ON".to_owned()
        } else {
            "OFF".to_owned()
        }),
        "tidb_pprof_sql_cpu" => Some(if instance.enable_pprof_sql_cpu {
            "1".to_owned()
        } else {
            "0".to_owned()
        }),
        "ddl_slow_threshold" => Some(instance.ddl_slow_opr_threshold.to_string()),
        "tidb_expensive_query_time_threshold" => {
            Some(instance.expensive_query_time_threshold.to_string())
        }
        "tidb_expensive_txn_time_threshold" => {
            Some(instance.expensive_txn_time_threshold.to_string())
        }
        "tidb_enable_slow_log" => Some(if instance.enable_slow_log.load() {
            "ON".to_owned()
        } else {
            "OFF".to_owned()
        }),
        "tidb_slow_log_threshold" => Some(instance.slow_threshold.to_string()),
        "tidb_record_plan_in_slow_log" => Some(
            if instance.record_plan_in_slow_log != 0 {
                "ON"
            } else {
                "OFF"
            }
            .to_owned(),
        ),
        "tidb_check_mb4_value_in_utf8" => Some(if instance.check_mb4_value_in_utf8.load() {
            "ON".to_owned()
        } else {
            "OFF".to_owned()
        }),
        // Go `GetGlobal`: `Priority2Str[ForcePriority]`.
        "tidb_force_priority" => Some(
            tidb_mysql::Priority::from_enum(
                tidb_vardef::FORCE_PRIORITY.load(std::sync::atomic::Ordering::SeqCst),
            )
            .as_name()
            .to_owned(),
        ),
        "tidb_memory_usage_alarm_ratio" => {
            Some(tidb_vardef::memory_usage_alarm_ratio().to_string())
        }
        "tidb_memory_usage_alarm_keep_record_num" => Some(
            tidb_vardef::MEMORY_USAGE_ALARM_KEEP_RECORD_NUM
                .load(std::sync::atomic::Ordering::SeqCst)
                .to_string(),
        ),
        "plugin_dir" => Some(instance.plugin_dir.clone()),
        "plugin_load" => Some(instance.plugin_load.clone()),
        "tidb_config" => Some(config.get_json_config().unwrap_or_default()),
        "tidb_log_file_max_days" => Some(config.log.file.max_days.to_string()),
        "tidb_enable_collect_execution_info" => {
            Some(if instance.enable_collect_execution_info.load() {
                "ON".to_owned()
            } else {
                "OFF".to_owned()
            })
        }
        "tidb_rc_read_check_ts" => Some(if instance.tidb_rc_read_check_ts {
            "ON".to_owned()
        } else {
            "OFF".to_owned()
        }),
        _ => None,
    }
}

/// Reads the process-wide products Go exposes through the
/// GLOBAL getter hooks. The registry still owns validation and persistence;
/// these atomics are the live consumer-facing authority.
fn runtime_global_value(name: &str) -> Option<String> {
    match name {
        tidb_vardef::tidb_vars::TIDB_ENABLE_FOREIGN_KEY => Some(
            if tidb_vardef::ENABLE_FOREIGN_KEY.load(std::sync::atomic::Ordering::SeqCst) {
                "ON".to_owned()
            } else {
                "OFF".to_owned()
            },
        ),
        tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE => Some(
            if tidb_vardef::RUN_AUTO_ANALYZE.load(std::sync::atomic::Ordering::SeqCst) {
                "ON".to_owned()
            } else {
                "OFF".to_owned()
            },
        ),
        tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE => Some(
            if tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                "ON".to_owned()
            } else {
                "OFF".to_owned()
            },
        ),
        tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY => Some(
            tidb_vardef::AUTO_ANALYZE_CONCURRENCY
                .load(std::sync::atomic::Ordering::SeqCst)
                .to_string(),
        ),
        tidb_vardef::tidb_vars::TIDB_CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO => {
            Some(tidb_vardef::circuit_breaker_pd_metadata_error_rate_threshold_ratio().to_string())
        }
        tidb_vardef::tidb_vars::TIDB_ENABLE_RESOURCE_CONTROL => Some(
            if tidb_vardef::ENABLE_RESOURCE_CONTROL.load(std::sync::atomic::Ordering::SeqCst) {
                "ON".to_owned()
            } else {
                "OFF".to_owned()
            },
        ),
        tidb_vardef::tidb_vars::TIDB_RESOURCE_CONTROL_STRICT_MODE => Some(
            if tidb_vardef::ENABLE_RESOURCE_CONTROL_STRICT_MODE
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                "ON".to_owned()
            } else {
                "OFF".to_owned()
            },
        ),
        _ => None,
    }
}

fn is_auto_analyze_setting(name: &str) -> bool {
    matches!(
        name,
        tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE
            | tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE
            | tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY
    )
}

fn is_resource_control_setting(name: &str) -> bool {
    matches!(
        name,
        tidb_vardef::tidb_vars::TIDB_ENABLE_RESOURCE_CONTROL
            | tidb_vardef::tidb_vars::TIDB_RESOURCE_CONTROL_STRICT_MODE
    )
}

fn is_stmt_summary_setting(name: &str) -> bool {
    matches!(
        name,
        "tidb_enable_stmt_summary"
            | "tidb_stmt_summary_internal_query"
            | "tidb_stmt_summary_refresh_interval"
            | "tidb_stmt_summary_history_size"
            | "tidb_stmt_summary_max_stmt_count"
            | "tidb_stmt_summary_max_sql_length"
            | "tidb_stmt_summary_persist_evicted"
            | "tidb_stmt_summary_group_by_user"
    )
}

impl GlobalSysvars {
    /// A fresh registry with every variable at its default (Go's state on a
    /// cluster nobody has ever run `SET GLOBAL` against).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Where a variable's node-wide value is written: a variable that has
    /// GLOBAL scope at all is a `mysql.global_variables` row (the cache is
    /// rebuilt from it) even when it ALSO has instance scope; only the
    /// instance-only ones are node-local.
    ///
    /// Every write goes through here, so a value can never be written to one
    /// tier and looked for in the other -- the failure mode that makes a
    /// `SET` a silent no-op.
    fn store(&self, def: &'static SysVarDef) -> &Arc<Mutex<HashMap<String, String>>> {
        if def.has_global_scope() {
            &self.table
        } else {
            &self.instances
        }
    }

    /// Go `Domain.GetGlobalVar` behind `session.GetGlobalSysVar`: the global
    /// view of the cache, rebuilt from the table first if it is empty. A
    /// variable the cache does not hold falls back to its registry value, as
    /// Go's accessor does once its table probe also misses.
    fn cached_global(&self, def: &'static SysVarDef) -> String {
        self.cache
            .get_global_var(&SysVarCacheRebuild(self), def.name)
            .unwrap_or_else(|_| self.registry_value(def))
    }

    /// Go `SysVar.Value` as this process has it: the registry default, or the
    /// startup value `variable.SetSysVar` replaced it with.
    fn registry_value(&self, def: &'static SysVarDef) -> String {
        self.startup
            .lock()
            .expect("startup sysvar lock poisoned")
            .get(def.name)
            .cloned()
            .unwrap_or_else(|| tidb_variable::sysvar::effective_default(def))
    }

    /// Go `rebuildSysVarCache`: rebuilds both cache views from the table
    /// rows, running every GLOBAL variable's SetGlobal hook with its value
    /// (which is how a peer's `SET GLOBAL` takes effect on this node), then
    /// republishes the read image derived from the cache.
    fn rebuild_sys_var_cache(&self) {
        // The table read cannot fail here: the rows are already in hand.
        let _ = self.cache.rebuild(&SysVarCacheRebuild(self));
        self.refresh_resolved();
    }

    /// Installs the node's PD policy and applies the current persisted/default value.
    pub fn set_pd_region_policy(&self, policy: Arc<dyn PdRegionPolicy>) {
        // Bind and initialize under the image lock so a concurrent publication
        // cannot be overwritten by an older value read before binding.
        let resolved = self
            .resolved
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = |name| {
            let index = tidb_variable::sysvar::sys_var_index_lookup(name).expect("PD policy is registered");
            resolved
                .values
                .get(index)
                .and_then(|slot| slot.as_deref())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    tidb_variable::sysvar::effective_default(&tidb_variable::sysvar::SYS_VARS[index])
                })
        };
        let enabled = |name| {
            let value = value(name);
            value.eq_ignore_ascii_case("ON") || value == "1"
        };
        *self
            .pd_region_policy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(policy.clone());
        if self.publishes_runtime_settings {
            policy.set_follower_handle(enabled(
                tidb_vardef::tidb_vars::PD_ENABLE_FOLLOWER_HANDLE_REGION,
            ));
            policy.set_tso_follower_proxy(enabled(
                tidb_vardef::tidb_vars::TIDB_ENABLE_TSO_FOLLOWER_PROXY,
            ));
            policy.set_tso_batch_wait(tso_batch_wait(&value(
                tidb_vardef::tidb_vars::TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME,
            )));
            policy.set_tso_rpc_concurrency(tso_rpc_concurrency(&value(
                tidb_vardef::tidb_vars::TIDB_TSO_CLIENT_RPC_MODE,
            )));
        }
        drop(resolved);
    }

    /// Installs the live storage oracle used by external timestamp callbacks.
    pub fn set_external_timestamp_provider(&self, provider: Arc<dyn ExternalTimestampProvider>) {
        *self
            .external_timestamp
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(provider);
    }

    fn external_timestamp_provider(&self) -> Option<Arc<dyn ExternalTimestampProvider>> {
        self.external_timestamp
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Reads a node-wide value; oracle-backed variables bypass the cached image.
    pub fn get(&self, name: &str) -> Result<String, VarError> {
        if name.eq_ignore_ascii_case("tidb_external_ts") {
            if let Some(provider) = self.external_timestamp_provider() {
                return provider
                    .get()
                    .map(|ts| ts.to_string())
                    .map_err(VarError::ValidationRefused);
            }
        }
        let Some(index) = tidb_variable::sysvar::sys_var_index_lookup(name) else {
            return Err(VarError::UnknownSystemVariable(name.to_ascii_lowercase()));
        };
        let def = &tidb_variable::sysvar::SYS_VARS[index];
        // Go's retired partition-statistics concurrency variable has fixed
        // getters so upgraded persisted values cannot leak a stale setting.
        if let Some(value) = self.instance_plan_cache_limit_getter(def) {
            return Ok(value);
        }
        if def.name == tidb_vardef::tidb_vars::TIDB_MERGE_PARTITION_STATS_CONCURRENCY {
            return Ok("1".to_owned());
        }
        if self.publishes_runtime_settings
            && def.name == tidb_vardef::tidb_vars::REQUIRE_SECURE_TRANSPORT
        {
            if tidb_config::deploymode::is_starter() {
                return Ok("ON".to_owned());
            }
            return Ok(if tidb_util::tls::REQUIRE_SECURE_TRANSPORT
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                "ON"
            } else {
                "OFF"
            }
            .to_owned());
        }
        if self.publishes_runtime_settings
            && def.name == tidb_vardef::tidb_vars::TIDB_TTL_JOB_ENABLE
        {
            return Ok(
                if tidb_vardef::ENABLE_TTL_JOB.load(std::sync::atomic::Ordering::SeqCst) {
                    "ON"
                } else {
                    "OFF"
                }
                .to_owned(),
            );
        }
        if self.publishes_runtime_settings {
            if let Some(value) = runtime_global_value(def.name) {
                return Ok(value);
            }
        }
        if self.publishes_runtime_settings {
            if let Some(value) = runtime_instance_value(self, def) {
                return Ok(value);
            }
        }
        let snapshot = Arc::clone(
            &*self
                .resolved
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        if let Some(value) = snapshot.values.get(index).and_then(|slot| slot.as_ref()) {
            return Ok(value.to_string());
        }
        if snapshot.values.len() == tidb_variable::sysvar::SYS_VARS.len() {
            // The image is current (a mutation republishes it before releasing
            // its writer), so an empty slot means "at its registry default".
            return Ok(tidb_variable::sysvar::effective_default(def));
        }
        // A pre-image from `Default`: the cache has not been built yet.
        Ok(self.unimaged_value(def))
    }

    /// A node-wide value before the read image exists (a fresh `Default`
    /// table nobody has rebuilt): the GLOBAL tier reads the cache, which
    /// builds itself on first read as Go's `rebuildSysVarCacheIfNeeded`
    /// does; the INSTANCE tier reads its own map. The image is published
    /// only by writers, so a reader can never install a stale one.
    fn unimaged_value(&self, def: &'static SysVarDef) -> String {
        if def.has_global_scope() {
            return self.cached_global(def);
        }
        self.instances
            .lock()
            .expect("instance sysvar lock poisoned")
            .get(def.name)
            .cloned()
            .unwrap_or_else(|| tidb_variable::sysvar::effective_default(def))
    }

    /// The global-form read used by both SQL variables and expression
    /// accessors. INSTANCE and NONE scopes have one node value; only a
    /// SESSION-only variable has no global copy (Go error 1238).
    pub fn get_for_global_scope(&self, name: &str) -> Result<String, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if !def.has_global_scope() && !def.has_instance_scope() && def.has_session_scope() {
            return Err(VarError::NoGlobalCopy(name.to_ascii_lowercase()));
        }
        self.get(name).map(|value| global_get_hook(def.name, value))
    }

    /// Rebuilds the read-mostly image from the cache's global view and the
    /// INSTANCE map. Every mutating entry point calls this after the cache
    /// rebuild (or the instance write) it follows; readers swap in the new
    /// `Arc` wholesale. The image is derived state, never written by itself.
    fn refresh_resolved(&self) {
        // Refreshes run one at a time: a refresh that started before a
        // concurrent rebuild swapped the cache publishes before that
        // rebuild's own refresh, which starts after the swap. The image lock
        // itself is not held while computing, because a SetGlobal hook a
        // first-read rebuild runs may read this table.
        let _refresh = self
            .refresh_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut slots: Vec<Option<Arc<str>>> =
            std::vec::Vec::with_capacity(tidb_variable::sysvar::SYS_VARS.len());
        slots.resize_with(tidb_variable::sysvar::SYS_VARS.len(), || None);
        for (index, def) in tidb_variable::sysvar::SYS_VARS.iter().enumerate() {
            if def.has_global_scope() {
                slots[index] = Some(Arc::from(self.cached_global(def).as_str()));
            }
        }
        for (name, value) in self
            .instances
            .lock()
            .expect("instance sysvar lock poisoned")
            .iter()
        {
            if let Some(index) = tidb_variable::sysvar::sys_var_index_lookup(name) {
                if !tidb_variable::sysvar::SYS_VARS[index].has_global_scope() {
                    slots[index] = Some(Arc::from(value.as_str()));
                }
            }
        }
        // A node-local variable without a value of its own serves the
        // registry value `variable.SetSysVar` replaced at startup.
        for (name, value) in self
            .startup
            .lock()
            .expect("startup sysvar lock poisoned")
            .iter()
        {
            if let Some(index) = tidb_variable::sysvar::sys_var_index_lookup(name) {
                if slots[index].is_none() && !tidb_variable::sysvar::SYS_VARS[index].has_global_scope() {
                    slots[index] = Some(Arc::from(value.as_str()));
                }
            }
        }
        let effective = |name: &str| {
            let index = tidb_variable::sysvar::sys_var_index_lookup(name)
                .expect("typed global policy names are registered");
            slots[index]
                .as_deref()
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    tidb_variable::sysvar::effective_default(&tidb_variable::sysvar::SYS_VARS[index])
                })
        };
        let oom_action = tidb_vardef::OomAction::parse(&effective(
            tidb_vardef::tidb_vars::TIDB_MEM_OOM_ACTION,
        ));
        let tmp_storage = effective(tidb_vardef::tidb_vars::TIDB_ENABLE_TMP_STORAGE_ON_OOM);
        let tmp_storage_on_oom = !(tmp_storage.eq_ignore_ascii_case("off") || tmp_storage == "0");
        let check_mb4 = effective(tidb_vardef::tidb_vars::TIDB_CHECK_MB4_VALUE_IN_UTF8);
        let check_mb4_value_in_utf8 = check_mb4.eq_ignore_ascii_case("on") || check_mb4 == "1";
        let check_constraint = effective(tidb_vardef::tidb_vars::TIDB_ENABLE_CHECK_CONSTRAINT);
        let enable_check_constraint =
            check_constraint.eq_ignore_ascii_case("on") || check_constraint == "1";
        *self
            .resolved
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(ResolvedGlobals {
            values: slots.into(),
            oom_action,
            tmp_storage_on_oom,
            check_mb4_value_in_utf8,
            enable_check_constraint,
        });
    }

    /// The typed process-wide statement-memory policy Go exposes through
    /// `vardef.OOMAction` and `vardef.EnableTmpStorageOnOOM` atomics.
    ///
    /// It is parsed when a GLOBAL mutation publishes the resolved image, not
    /// when each statement starts.
    pub fn statement_memory_policy(&self) -> (tidb_vardef::OomAction, bool) {
        let snapshot = Arc::clone(
            &*self
                .resolved
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        (snapshot.oom_action, snapshot.tmp_storage_on_oom)
    }

    /// Go's process-wide typed `vardef.CheckMb4ValueInUTF8` atomic, parsed
    /// when a GLOBAL mutation publishes the resolved image rather than by
    /// every statement context.
    pub fn check_mb4_value_in_utf8(&self) -> bool {
        self.resolved
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .check_mb4_value_in_utf8
    }

    /// Go's process-wide typed `vardef.EnableCheckConstraint` atomic, parsed
    /// once per GLOBAL publication.
    pub fn enable_check_constraint(&self) -> bool {
        self.resolved
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .enable_check_constraint
    }

    /// Reads the current GLOBAL resource-control enable value for statement
    /// admission. The table is authoritative for this registry (and for a
    /// cluster scratch image); the process atomic is published alongside it
    /// for Go-compatible callbacks.
    pub fn resource_control_enabled(&self) -> bool {
        self.global_bool_value(
            tidb_vardef::tidb_vars::TIDB_ENABLE_RESOURCE_CONTROL,
            tidb_vardef::defaults::DEF_TIDB_ENABLE_RESOURCE_CONTROL,
        )
    }

    /// Reads the current GLOBAL resource-control strict-mode value.
    pub fn resource_control_strict_mode(&self) -> bool {
        self.global_bool_value(
            tidb_vardef::tidb_vars::TIDB_RESOURCE_CONTROL_STRICT_MODE,
            tidb_vardef::defaults::DEF_TIDB_RESOURCE_CONTROL_STRICT_MODE,
        )
    }

    /// Reads one variable by its registry position, skipping the name probe
    /// callers already paid (`SessionVars::get_system` resolves the index
    /// once for both tiers). Same image-then-authoritative fallback as
    /// [`Self::get`].
    pub fn get_by_registry_index(&self, index: usize) -> Result<String, VarError> {
        let def = &tidb_variable::sysvar::SYS_VARS[index];
        if def.name == "tidb_external_ts" {
            return self.get(def.name);
        }
        if let Some(value) = self.instance_plan_cache_limit_getter(def) {
            return Ok(value);
        }
        if def.name == tidb_vardef::tidb_vars::TIDB_MERGE_PARTITION_STATS_CONCURRENCY {
            return Ok("1".to_owned());
        }
        if self.publishes_runtime_settings
            && def.name == tidb_vardef::tidb_vars::REQUIRE_SECURE_TRANSPORT
        {
            return self.get(def.name);
        }
        if self.publishes_runtime_settings {
            if let Some(value) = runtime_instance_value(self, def) {
                return Ok(value);
            }
            if let Some(value) = runtime_global_value(def.name) {
                return Ok(value);
            }
        }
        let snapshot = Arc::clone(
            &*self
                .resolved
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        if let Some(value) = snapshot.values.get(index).and_then(|slot| slot.as_ref()) {
            return Ok(value.to_string());
        }
        if snapshot.values.len() == tidb_variable::sysvar::SYS_VARS.len() {
            return Ok(tidb_variable::sysvar::effective_default(def));
        }
        Ok(self.unimaged_value(def))
    }

    /// Validates and writes a global value, visible to every session that
    /// reads `@@global.name` or opens AFTER this call.
    ///
    /// Answers whether Go's validation clamped the value, which the caller
    /// turns into `ErrTruncatedWrongValue` (1292) on the statement.
    pub fn set(&self, name: &str, value: String) -> Result<bool, VarError> {
        // Go `validateScope` (`variable.go:265`): `SET GLOBAL` is admitted by
        // `sv.HasGlobalScope() || sv.HasInstanceScope()`, so `SET GLOBAL
        // tidb_general_log = 1` is legal and lands in the instance tier.
        self.set_with_time_zone(name, value, &tidb_datatype::SessionTimeZone::utc())
    }

    /// `SET GLOBAL` with the issuing session's time zone.
    ///
    /// Go's `ValidateFromType(TypeTime)` parses a short `HH:MM` value in the
    /// session's `Location()` before the variable's GLOBAL hook stores it.
    /// Most callers use [`Self::set`] (which has no session and therefore uses
    /// UTC); SQL execution calls this variant so TTL schedule-window values
    /// retain the issuer's numeric offset, matching `TestSetJobScheduleWindow`.
    pub fn set_with_time_zone(
        &self,
        name: &str,
        value: String,
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<bool, VarError> {
        if name.eq_ignore_ascii_case("tidb_external_ts") {
            if let Some(provider) = self.external_timestamp_provider() {
                if value.is_empty() {
                    return Err(VarError::SqlError(tidb_error::mysql::SqlError::new_f(
                        tidb_error::mysql::errcode::ErrTruncatedWrongValue,
                        "Incorrect datetime value: '%s'",
                        &[],
                        &[tidb_error::mysql::FormatArg::from(value.as_str())],
                    )));
                }
                provider
                    .set(parse_read_timestamp(&value, zone, true)?)
                    .map_err(VarError::ValidationRefused)?;
            }
        }
        let value = match get_sys_var(name) {
            Some(def) if def.var_type == tidb_variable::sysvar::VarType::Time => {
                tidb_variable::sysvar::normalize_time_value(&value, zone)
                    .map_err(|error| validation_var_error(name, &value, error))?
            }
            _ => value,
        };
        self.write(name, value, SCOPE_GLOBAL)
    }

    /// `SET INSTANCE name = value`, and the destination Go's legacy routing
    /// sends an unqualified `SET` on an instance-scoped variable to.
    ///
    /// Go `validateScope`: `scope == ScopeInstance && !sv.HasInstanceScope()`
    /// is `errLocalVariable` (1228), the same error a `SET GLOBAL` on a
    /// session-only variable gets.
    pub fn set_instance(&self, name: &str, value: String) -> Result<bool, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if !def.is_read_only() && !def.has_instance_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        self.write(name, value, SCOPE_INSTANCE)
    }

    /// Go `variable.SetSysVar`: the STARTUP write `setGlobalVars`
    /// (`cmd/tidb-server/main.go:1105`) pushes config-derived values with —
    /// it replaces the registry `Value` directly, with no scope or read-only
    /// validation, which is how read-only NONE-scope variables like `port`,
    /// `socket` and `hostname` get their per-process values. The sysvar cache
    /// serves the replaced value for a variable with no
    /// `mysql.global_variables` row, and new sessions copy it from the
    /// cache's session view. Runtime `SET` statements never come through
    /// here.
    pub fn set_startup(&self, name: &str, value: String) {
        let Some(def) = get_sys_var(name) else {
            return;
        };
        self.startup
            .lock()
            .expect("startup sysvar lock poisoned")
            .insert(def.name.to_owned(), value.clone());
        if def.has_global_scope() {
            self.rebuild_sys_var_cache();
        } else {
            // A node-local variable's one value is this process's own: the
            // INSTANCE/NONE read path serves it from here.
            self.instances
                .lock()
                .expect("instance sysvar lock poisoned")
                .insert(def.name.to_owned(), value.clone());
            self.apply_set_global_hook(def.name, &value);
            self.refresh_resolved();
        }
    }

    /// Go `setGlobalVars`' `setInstanceVar`: a non-empty, non-`0` value from
    /// the `[instance]` config section replaces the registry value and marks
    /// the variable `IsInitedFromConfig`, so this instance keeps it over the
    /// `mysql.global_variables` row.
    pub fn set_instance_var(&self, name: &str, value: String) {
        if value.is_empty() || value == "0" {
            return;
        }
        let Some(def) = get_sys_var(name) else {
            return;
        };
        self.inited_from_config
            .lock()
            .expect("inited-from-config lock poisoned")
            .insert(def.name.to_owned());
        self.set_startup(name, value);
    }

    fn is_inited_from_config(&self, name: &str) -> bool {
        self.inited_from_config
            .lock()
            .expect("inited-from-config lock poisoned")
            .contains(name)
    }

    // Go GetGlobal exposes the typed byte count, not the original unit spelling.
    fn instance_plan_cache_limit_getter(&self, def: &'static SysVarDef) -> Option<String> {
        if def.name != tidb_vardef::tidb_vars::TIDB_INSTANCE_PLAN_CACHE_MAX_MEM_SIZE {
            return None;
        }
        let value = self.cached_global(def);
        tidb_variable::varsutil::parse_byte_size(&value)
            .map(|(bytes, _)| bytes)
            .or_else(|| value.parse::<u64>().ok())
            .map(|bytes| (bytes as i64).to_string())
    }

    /// Whether [`Self::apply_set_global_hook`] has an arm for `name`: Go's
    /// `SysVar.SetGlobal != nil` for the variables whose process-wide effect
    /// this node models. The sysvar cache rebuild calls the hook only for
    /// these.
    fn has_set_global_hook(name: &str) -> bool {
        use tidb_vardef::tidb_vars::*;
        matches!(
            name,
            REQUIRE_SECURE_TRANSPORT
                | TIDB_TTL_JOB_ENABLE
                | TIDB_ENABLE_FOREIGN_KEY
                | TIDB_ENABLE_CHECK_CONSTRAINT
                | TIDB_DDL_ENABLE_FAST_REORG
                | TIDB_ENABLE_MDL
                | TIDB_SCHEMA_CACHE_SIZE
                | TIDB_DDL_ERROR_COUNT_LIMIT
                | TIDB_CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO
                | TIDB_COMMITTER_CONCURRENCY
                | TIDB_REDACT_LOG
                | TIDB_ENABLE_INSTANCE_PLAN_CACHE
                | TIDB_INSTANCE_PLAN_CACHE_MAX_MEM_SIZE
                | TIDB_INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE
                | TIDB_MEM_OOM_ACTION
                | TIDB_MEMORY_USAGE_ALARM_RATIO
                | TIDB_MEMORY_USAGE_ALARM_KEEP_RECORD_NUM
                | TIDB_ANALYZE_DEFAULT_NUM_BUCKETS
                | TIDB_ANALYZE_DEFAULT_NUM_TOP_N
                | TIDB_STATS_CACHE_MEM_QUOTA
                | TIDB_MAX_AUTO_ANALYZE_TIME
                | PD_ENABLE_FOLLOWER_HANDLE_REGION
                | TIDB_ENABLE_TSO_FOLLOWER_PROXY
                | TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME
                | TIDB_TSO_CLIENT_RPC_MODE
                | TIDB_CHECK_MB4_VALUE_IN_UTF8
        ) || is_auto_analyze_setting(name)
            || is_resource_control_setting(name)
            || is_stmt_summary_setting(name)
            || Self::is_memory_arbitration_setting(name)
    }

    /// Go `SysVar.SetGlobal` for the variables whose process-wide product
    /// this node publishes: called with the variable's value by every cache
    /// rebuild (Go `rebuildSysVarCache`, which is how a peer's `SET GLOBAL`
    /// takes effect here) and, for an INSTANCE-scoped variable, by the write
    /// itself (Go `SetInstanceSysVar` -> `SetGlobalFromHook`).
    ///
    /// The value is handed in, never read back from the cache: a rebuild
    /// runs the hooks before it publishes its new views. A scratch table
    /// (a cluster `SET GLOBAL` validated before its commit) publishes
    /// nothing; its committed image does, once it becomes the live table.
    fn apply_set_global_hook(&self, name: &str, value: &str) {
        if !self.publishes_runtime_settings {
            return;
        }
        use std::sync::atomic::Ordering::SeqCst;
        use tidb_vardef::tidb_vars::*;
        let on = value.eq_ignore_ascii_case("ON") || value == "1";
        match name {
            REQUIRE_SECURE_TRANSPORT => tidb_util::tls::REQUIRE_SECURE_TRANSPORT
                .store(!tidb_config::deploymode::is_starter() && on, SeqCst),
            TIDB_TTL_JOB_ENABLE => tidb_vardef::ENABLE_TTL_JOB.store(on, SeqCst),
            // Go `SetGlobal`: `ForcePriority = Str2Priority(val)`.
            TIDB_FORCE_PRIORITY => tidb_vardef::FORCE_PRIORITY
                .store(tidb_mysql::priority_from_str(value).as_enum(), SeqCst),
            TIDB_ENABLE_FOREIGN_KEY => tidb_vardef::ENABLE_FOREIGN_KEY.store(on, SeqCst),
            TIDB_ENABLE_CHECK_CONSTRAINT => tidb_vardef::ENABLE_CHECK_CONSTRAINT.store(on, SeqCst),
            TIDB_DDL_ENABLE_FAST_REORG => tidb_vardef::ENABLE_FAST_REORG.store(on, SeqCst),
            // Go's `TiDBEnableMDL` hook runs `SwitchMDL` -> `SetEnableMDL`. A
            // node that leaves the process flag at its `false` default
            // acknowledges DDL schema versions on the classic per-node key
            // while a Go owner with metadata locking on waits for the
            // per-job key, so every Go-issued DDL on the cluster blocks until
            // the node is gone. Following the value the cluster holds is not
            // a switch, so Go's running-job guard (enforced by the cluster
            // writer for a user's `SET GLOBAL`) does not apply here.
            TIDB_ENABLE_MDL => tidb_vardef::set_enable_mdl(on),
            TIDB_SCHEMA_CACHE_SIZE => {
                let bytes = tidb_variable::varsutil::parse_byte_size(value)
                    .map(|(bytes, _)| bytes)
                    .or_else(|| value.parse::<u64>().ok())
                    .unwrap_or(tidb_vardef::defaults::DEF_TIDB_SCHEMA_CACHE_SIZE as u64);
                tidb_vardef::SCHEMA_CACHE_SIZE.store(bytes, SeqCst);
            }
            TIDB_ENABLE_AUTO_ANALYZE => tidb_vardef::RUN_AUTO_ANALYZE.store(on, SeqCst),
            TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE => {
                tidb_vardef::ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE.store(on, SeqCst)
            }
            TIDB_AUTO_ANALYZE_CONCURRENCY => tidb_vardef::AUTO_ANALYZE_CONCURRENCY.store(
                value
                    .parse::<i64>()
                    .unwrap_or(tidb_vardef::defaults::DEF_TIDB_AUTO_ANALYZE_CONCURRENCY),
                SeqCst,
            ),
            TIDB_DDL_ERROR_COUNT_LIMIT => tidb_vardef::set_ddl_error_count_limit(
                value
                    .parse::<i64>()
                    .unwrap_or(tidb_vardef::defaults::DEF_TIDB_DDL_ERROR_COUNT_LIMIT),
            ),
            TIDB_CIRCUIT_BREAKER_PD_METADATA_ERROR_RATE_THRESHOLD_RATIO => {
                tidb_vardef::set_circuit_breaker_pd_metadata_error_rate_threshold_ratio(
                    value.parse::<f64>().unwrap_or(
                        tidb_vardef::defaults::DEF_TIDB_CIRCUIT_BREAKER_PD_META_ERROR_RATE_RATIO,
                    ),
                )
            }
            TIDB_ENABLE_RESOURCE_CONTROL => {
                // Go's SetGlobal: only a change reaches the domain's hook.
                let previous = tidb_vardef::ENABLE_RESOURCE_CONTROL.swap(on, SeqCst);
                if previous != on {
                    let hook = SET_GLOBAL_RESOURCE_CONTROL
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone();
                    if let Some(hook) = hook {
                        hook(on);
                    }
                }
            }
            TIDB_RESOURCE_CONTROL_STRICT_MODE => {
                tidb_vardef::ENABLE_RESOURCE_CONTROL_STRICT_MODE.store(on, SeqCst)
            }
            TIDB_COMMITTER_CONCURRENCY => {
                if let Ok(value) = value.parse::<i32>() {
                    tidb_tikvutil::COMMITTER_CONCURRENCY.store(value, SeqCst);
                }
            }
            TIDB_REDACT_LOG => tidb_error::mysql::set_redaction_mode(match value {
                "ON" => tidb_error::mysql::RedactionMode::Enabled,
                "MARKER" => tidb_error::mysql::RedactionMode::Marker,
                _ => tidb_error::mysql::RedactionMode::Disabled,
            }),
            TIDB_ENABLE_INSTANCE_PLAN_CACHE => {
                tidb_vardef::ENABLE_INSTANCE_PLAN_CACHE.store(on, SeqCst)
            }
            TIDB_INSTANCE_PLAN_CACHE_MAX_MEM_SIZE => {
                if let Some(bytes) = tidb_variable::varsutil::parse_byte_size(value)
                    .map(|(bytes, _)| bytes)
                    .or_else(|| value.parse().ok())
                {
                    tidb_vardef::INSTANCE_PLAN_CACHE_MAX_MEM_SIZE
                        .store(bytes.min(i64::MAX as u64) as i64, SeqCst);
                }
            }
            TIDB_INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE => {
                if let Ok(value) = value.parse::<f64>() {
                    tidb_vardef::INSTANCE_PLAN_CACHE_RESERVED_PERCENTAGE
                        .store(value.to_bits(), SeqCst);
                }
            }
            TIDB_MEM_OOM_ACTION => tidb_vardef::set_oom_action(value),
            TIDB_MEMORY_USAGE_ALARM_RATIO => {
                if let Ok(value) = value.parse::<f64>() {
                    tidb_vardef::set_memory_usage_alarm_ratio(value);
                }
            }
            TIDB_MEMORY_USAGE_ALARM_KEEP_RECORD_NUM => {
                if let Ok(value) = value.parse::<i64>() {
                    tidb_vardef::MEMORY_USAGE_ALARM_KEEP_RECORD_NUM.store(value, SeqCst);
                }
            }
            TIDB_ANALYZE_DEFAULT_NUM_BUCKETS => {
                if let Ok(value) = value.parse::<u64>() {
                    tidb_vardef::ANALYZE_DEFAULT_NUM_BUCKETS.store(value, SeqCst);
                }
            }
            TIDB_ANALYZE_DEFAULT_NUM_TOP_N => {
                if let Ok(value) = value.parse::<u64>() {
                    tidb_vardef::ANALYZE_DEFAULT_NUM_TOP_N.store(value, SeqCst);
                }
            }
            TIDB_STATS_CACHE_MEM_QUOTA => {
                if let Ok(value) = value.parse::<i64>() {
                    tidb_vardef::STATS_CACHE_MEM_QUOTA.store(value, SeqCst);
                }
            }
            TIDB_MAX_AUTO_ANALYZE_TIME => {
                if let Ok(value) = value.parse::<i64>() {
                    tidb_vardef::MAX_AUTO_ANALYZE_TIME.store(value, SeqCst);
                }
            }
            PD_ENABLE_FOLLOWER_HANDLE_REGION
            | TIDB_ENABLE_TSO_FOLLOWER_PROXY
            | TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME
            | TIDB_TSO_CLIENT_RPC_MODE => {
                // Go `setPDClientDynamicOption`: the store's PD client takes
                // the new option in place.
                let policy = self
                    .pd_region_policy
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                if let Some(policy) = policy {
                    match name {
                        PD_ENABLE_FOLLOWER_HANDLE_REGION => policy.set_follower_handle(on),
                        TIDB_ENABLE_TSO_FOLLOWER_PROXY => policy.set_tso_follower_proxy(on),
                        TIDB_TSO_CLIENT_BATCH_MAX_WAIT_TIME => {
                            policy.set_tso_batch_wait(tso_batch_wait(value))
                        }
                        _ => policy.set_tso_rpc_concurrency(tso_rpc_concurrency(value)),
                    }
                }
            }
            // Go's instance hook updates
            // `config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8`.
            TIDB_CHECK_MB4_VALUE_IN_UTF8 => {
                tidb_config::config_tree::config::update_global(|config| {
                    config.instance.check_mb4_value_in_utf8 =
                        tidb_config::config_tree::marshal::AtomicBool::new(value == "ON");
                });
            }
            _ if is_stmt_summary_setting(name) => self.publish_stmt_summary_setting(name, value),
            _ if Self::is_memory_arbitration_setting(name) => {
                let _ = tidb_util::memory::apply_process_memory_setting(name, value);
            }
            _ => {}
        }
    }

    fn write(&self, name: &str, value: String, scope: u8) -> Result<bool, VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_global_scope() && !def.has_instance_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        let lookup = |sibling: &str| self.get(sibling).ok();
        let validated = def
            .validate_in_scope_with_lookup(&value, scope, Some(&lookup))
            .map_err(|error| validation_var_error(name, &value, error))?;
        let key = name.to_ascii_lowercase();
        if key == tidb_vardef::tidb_vars::TIDB_INSTANCE_PLAN_CACHE_MAX_MEM_SIZE {
            let bytes = tidb_variable::varsutil::parse_byte_size(&validated.value)
                .map(|(bytes, _)| bytes)
                .or_else(|| validated.value.parse::<u64>().ok());
            let Some(bytes) = bytes else {
                return Err(VarError::ValidationRefused(format!(
                    "invalid tidb_instance_plan_cache_max_mem_size value {value}"
                )));
            };
            if bytes < 100 * 1024 * 1024 {
                return Err(VarError::ValidationRefused(
                    "tidb_instance_plan_cache_max_mem_size should be at least 100MiB".to_owned(),
                ));
            }
        }
        // Go's `tidb_auto_analyze_concurrency` Validation observes the two
        // current GLOBAL switches. Check the table being written (rather
        // than process atomics) so a cluster scratch image validates a
        // statement against its own pending rows and unrelated registries in
        // parallel tests cannot change the answer mid-write.
        if key == tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY {
            let run_auto_analyze = self.global_bool_value(
                tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE,
                tidb_vardef::defaults::DEF_TIDB_ENABLE_AUTO_ANALYZE,
            );
            let enable_auto_analyze_priority_queue = self.global_bool_value(
                tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE,
                tidb_vardef::defaults::DEF_TIDB_ENABLE_AUTO_ANALYZE_PRIORITY_QUEUE,
            );
            if !run_auto_analyze || !enable_auto_analyze_priority_queue {
                return Err(VarError::ValidationRefused(format!(
                    "cannot set {}: requires both tidb_enable_auto_analyze and tidb_enable_auto_analyze_priority_queue to be true. Current values: tidb_enable_auto_analyze={}, tidb_enable_auto_analyze_priority_queue={}",
                    tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY,
                    run_auto_analyze,
                    enable_auto_analyze_priority_queue
                )));
            }
        }
        // Go's plan-replayer continuous-capture Validation refuses ON until
        // the GLOBAL historical-stats switch is enabled. The closure reads
        // the pending shared image, so validate against this table rather
        // than a process singleton.
        if key == "tidb_enable_plan_replayer_continuous_capture"
            && validated.value == "ON"
            && !self.global_bool_value("tidb_enable_historical_stats", false)
        {
            return Err(VarError::ValidationRefused(
                "tidb_enable_historical_stats should be enabled before enabling tidb_enable_plan_replayer_continuous_capture"
                    .to_owned(),
            ));
        }
        if key == tidb_vardef::tidb_vars::TIDB_OPT_FIX_CONTROL {
            OptimizerFixControl::parse(&validated.value)
                .map_err(|error| VarError::ValidationRefused(error.to_string()))?;
        }
        let stored_value = validated.value;
        // Go's GOGC tuner bounds are cross-validated against the current
        // process values: max must stay strictly above min, and min strictly
        // below max. The Rust registry has no separate tuner singleton, so
        // use the authoritative GLOBAL image that owns the SQL-visible
        // values; this also keeps scratch cluster tables self-contained.
        if key == tidb_vardef::tidb_vars::TIDB_GOGC_TUNER_MAX_VALUE {
            let min_value = self
                .get(tidb_vardef::tidb_vars::TIDB_GOGC_TUNER_MIN_VALUE)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_GOGC_MIN_VALUE);
            if stored_value.parse::<i64>().unwrap_or_default() <= min_value {
                return Err(VarError::ValidationRefused(
                    "tidb_gogc_tuner_max_value should be more than tidb_gogc_tuner_min_value"
                        .to_owned(),
                ));
            }
        }
        if key == tidb_vardef::tidb_vars::TIDB_GOGC_TUNER_MIN_VALUE {
            let max_value = self
                .get(tidb_vardef::tidb_vars::TIDB_GOGC_TUNER_MAX_VALUE)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_GOGC_MAX_VALUE);
            if stored_value.parse::<i64>().unwrap_or_default() >= max_value {
                return Err(VarError::ValidationRefused(
                    "tidb_gogc_tuner_min_value should be less than tidb_gogc_tuner_max_value"
                        .to_owned(),
                ));
            }
        }
        // Go's `tidb_super_read_only` Validation (`sysvar.go:999`): turning
        // the flag OFF through a user SET is refused while
        // `tidb_restricted_read_only` is ON.
        if key == "tidb_super_read_only"
            && stored_value == "OFF"
            && self.get("tidb_restricted_read_only").ok().as_deref() == Some("ON")
        {
            return Err(VarError::ValidationRefused(
                "can't turn off tidb_super_read_only when tidb_restricted_read_only is on"
                    .to_owned(),
            ));
        }
        if self.publishes_runtime_settings && Self::is_memory_arbitration_setting(&key) {
            tidb_util::memory::validate_process_memory_setting(&key, &stored_value)
                .map_err(VarError::ValidationRefused)?;
        }
        // Go's `tidb_trace_event` GLOBAL hook owns the process-wide flight
        // recorder. A JSON configuration starts/replaces the recorder, while
        // an empty assignment closes it. Keep this publication after all
        // registry validation but before storing the SQL-facing value so a
        // malformed trigger cannot leave a half-applied setting behind.
        if self.publishes_runtime_settings && key == "tidb_trace_event" {
            if stored_value.is_empty() {
                if let Some(recorder) = tidb_util::traceevent::get_flight_recorder() {
                    recorder.close();
                }
            } else {
                let config = serde_json::from_str::<tidb_util::traceevent::FlightRecorderConfig>(
                    &stored_value,
                )
                .map_err(|error| VarError::ValidationRefused(error.to_string()))?;
                tidb_util::traceevent::start_log_flight_recorder(config)
                    .map_err(VarError::ValidationRefused)?;
            }
        }
        // Go's `validate_password.*` Validation closures (`sysvar.go:717-790`)
        // keep the five settings coupled: a count raise lifts the sibling
        // `length` to `number + special + 2 * mixed_case`, and a `length` set
        // below that floor is adjusted up instead of stored.
        let stored_value = if key == "validate_password.length" {
            let floor = self.validate_password_length_floor(stored_value.parse::<i64>().ok());
            match floor {
                Some(floor) => floor.to_string(),
                None => stored_value,
            }
        } else {
            stored_value
        };
        {
            let mut values = self.store(def).lock().expect("global sysvar lock poisoned");
            if let Some(other) = alias_of(&key) {
                values.insert(other.to_owned(), stored_value.clone());
            }
            values.insert(key.clone(), stored_value.clone());
        }
        if !def.has_global_scope() {
            // Go `SetInstanceSysVar`: the hook is the whole write; the
            // INSTANCE tier has no row and no cache.
            self.apply_set_global_hook(&key, &stored_value);
            self.refresh_resolved();
            self.record_instance_mutation(InstanceMutation::Set(key.clone(), stored_value));
            return Ok(validated.truncated);
        }
        // Go's `tidb_restricted_read_only` SetGlobal hook promotes
        // `tidb_super_read_only` whenever restricted mode is enabled
        // (`SetGlobalSysVarOnly`, a row of its own). The promotion is
        // one-way: clearing restricted mode leaves super-read-only enabled
        // until it is explicitly cleared by a later SET.
        if key == "tidb_restricted_read_only" && stored_value == "ON" {
            self.table
                .lock()
                .expect("global sysvar lock poisoned")
                .insert("tidb_super_read_only".to_owned(), "ON".to_owned());
        }
        // Go `replaceGlobalVariablesTableValue` -> `NotifyUpdateSysVarCache`:
        // the cache is rebuilt from the table the write just changed.
        self.rebuild_sys_var_cache();
        if matches!(
            key.as_str(),
            "validate_password.mixed_case_count"
                | "validate_password.number_count"
                | "validate_password.special_char_count"
        ) {
            // Setting a count raises the stored `length` to the new minimum
            // when the current length falls short (`updatePasswordValidationLength`,
            // `varsutil.go:446`): a plain store of its own row, no further
            // validation, read against the counts just written.
            if let Some(required) = self.validate_password_required_length() {
                let current: i64 = self
                    .get("validate_password.length")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(8);
                if current < required {
                    self.table
                        .lock()
                        .expect("global sysvar lock poisoned")
                        .insert("validate_password.length".to_owned(), required.to_string());
                    self.rebuild_sys_var_cache();
                }
            }
        }
        Ok(validated.truncated)
    }

    /// Publishes Go's GLOBAL statement-summary setters into the live v2
    /// proxy. The SQL table remains the source of truth for reads, while the
    /// summary map is the process-wide consumer Go's `SetGlobal` callbacks
    /// update immediately. Scratch cluster images deliberately skip this
    /// hook and publish only after their committed image becomes live.
    fn publish_stmt_summary_setting(&self, name: &str, value: &str) {
        if !self.publishes_runtime_settings || !is_stmt_summary_setting(name) {
            return;
        }
        let bool_value = || value.eq_ignore_ascii_case("ON") || value == "1";
        match name {
            "tidb_enable_stmt_summary" => {
                tidb_stmtsummary::v2::stmtsummary::set_enabled(bool_value())
            }
            "tidb_stmt_summary_internal_query" => {
                tidb_stmtsummary::v2::stmtsummary::set_enable_internal_query(bool_value())
            }
            "tidb_stmt_summary_refresh_interval" => {
                if let Ok(value) = value.parse::<i64>() {
                    tidb_stmtsummary::v2::stmtsummary::set_refresh_interval(value);
                }
            }
            "tidb_stmt_summary_history_size" => {
                if let Ok(value) = value.parse::<i32>() {
                    tidb_stmtsummary::v2::stmtsummary::set_history_size(value);
                }
            }
            "tidb_stmt_summary_max_stmt_count" => {
                if let Ok(value) = value.parse::<i64>() {
                    tidb_stmtsummary::v2::stmtsummary::set_max_stmt_count(value);
                }
            }
            "tidb_stmt_summary_max_sql_length" => {
                if let Ok(value) = value.parse::<i32>() {
                    tidb_stmtsummary::v2::stmtsummary::set_max_sql_length(value);
                }
            }
            "tidb_stmt_summary_persist_evicted" => {
                tidb_stmtsummary::v2::stmtsummary::set_persist_evicted(bool_value())
            }
            "tidb_stmt_summary_group_by_user" => {
                tidb_stmtsummary::v2::stmtsummary::set_group_by_user(bool_value())
            }
            _ => {}
        }
    }

    /// The length floor the `validate_password` coupling requires right now:
    /// `number_count + special_char_count + 2 * mixed_case_count`
    /// (`sysvar.go:717`'s Validation), read from the current global values
    /// with the registry defaults (8/1/1/1) standing in for unset entries.
    /// `None` when the stored `length` is not an integer and no floor applies.
    fn validate_password_length_floor(&self, length: Option<i64>) -> Option<i64> {
        let number = self.validate_password_global("validate_password.number_count", 1);
        let special = self.validate_password_global("validate_password.special_char_count", 1);
        let mixed = self.validate_password_global("validate_password.mixed_case_count", 1);
        let floor = Some(number + special + 2 * mixed);
        match length {
            None => None,
            Some(length) => floor.filter(|floor| length < *floor),
        }
    }

    /// The same floor for a count raise: applies whenever the stored length
    /// falls short of it.
    fn validate_password_required_length(&self) -> Option<i64> {
        self.validate_password_length_floor(self.get("validate_password.length").ok()?.parse().ok())
    }

    /// Reads one `validate_password` global, falling back to Go's default.
    fn validate_password_global(&self, name: &str, default: i64) -> i64 {
        self.get(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn global_bool_value(&self, name: &str, default: bool) -> bool {
        self.get(name).map_or(default, |value| {
            value.eq_ignore_ascii_case("ON") || value == "1"
        })
    }

    /// Restores the registry default (`SET GLOBAL x = DEFAULT` against a
    /// table whose row the default replaces).
    pub fn reset(&self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_global_scope() && !def.has_instance_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_global_scope() {
            return self.reset_instance(name);
        }
        let key = name.to_ascii_lowercase();
        self.table
            .lock()
            .expect("global sysvar lock poisoned")
            .remove(&key);
        // The rebuild serves the registry value again and runs its hook.
        self.rebuild_sys_var_cache();
        Ok(())
    }

    /// Restores an INSTANCE-scoped value to its registry default, after the
    /// same scope validation as [`Self::set_instance`].
    pub fn reset_instance(&self, name: &str) -> Result<(), VarError> {
        let def = get_sys_var(name)
            .ok_or_else(|| VarError::UnknownSystemVariable(name.to_ascii_lowercase()))?;
        if def.is_read_only() {
            return Err(VarError::ReadOnlyVariable(name.to_ascii_lowercase()));
        }
        if !def.has_instance_scope() {
            return Err(VarError::SessionOnlyVariable(name.to_ascii_lowercase()));
        }
        if def.has_global_scope() {
            // A `ScopeGlobal|ScopeInstance` variable's one value is its row.
            return self.reset(name);
        }
        let key = name.to_ascii_lowercase();
        self.instances
            .lock()
            .expect("instance sysvar lock poisoned")
            .remove(&key);
        self.apply_set_global_hook(def.name, &self.registry_value(def));
        self.refresh_resolved();
        self.record_instance_mutation(InstanceMutation::Reset(key));
        Ok(())
    }

    /// Go `rebuildSysVarCache` over rows a caller already read from a
    /// cluster's (or an in-process store's) `mysql.global_variables` (Go
    /// `fetchTableValues`): the rows replace this table wholesale, the cache
    /// is rebuilt from them, and every GLOBAL variable's SetGlobal hook runs
    /// with its value.
    ///
    /// Unlike [`Self::set`], this does not validate: a stored row already
    /// passed that validation when some earlier `SET GLOBAL` (Go's or this
    /// node's own) wrote it. A name this registry does not recognize is
    /// skipped rather than refused, the same forward/backward-compatibility
    /// stance `tidb_cluster::cluster_privilege_load` takes on a column or
    /// privilege name the running version does not know (Go's rebuild walks
    /// only `GetSysVars()`, so such a row never reaches its cache either).
    pub fn load_from_cluster<I: IntoIterator<Item = (String, String)>>(&self, rows: I) {
        let rows: HashMap<String, String> = rows
            .into_iter()
            .filter_map(|(name, value)| {
                let key = name.to_ascii_lowercase();
                get_sys_var(&key).is_some().then_some((key, value))
            })
            .collect();
        *self.table.lock().expect("global sysvar lock poisoned") = rows;
        self.rebuild_sys_var_cache();
    }

    /// The `mysql.global_variables` rows this table holds (see the `table`
    /// field): what a cluster scratch copy's statement leaves to persist,
    /// and what an in-process write diffs to find the rows it changed.
    pub fn overrides(&self) -> HashMap<String, String> {
        self.table
            .lock()
            .expect("global sysvar lock poisoned")
            .clone()
    }

    /// Go `Domain.GetSessionCache`: a copy of the cache's session view, the
    /// values a new session copies into its own variables
    /// (`loadCommonGlobalVariablesIfNeeded`).
    fn session_cache(&self) -> HashMap<String, String> {
        self.cache
            .get_session_cache(&SysVarCacheRebuild(self))
            .unwrap_or_default()
    }

    /// A fresh table seeded from a cluster's stored
    /// `mysql.global_variables` rows -- the scratch copy a convergence node
    /// validates one `SET GLOBAL` against before persisting it, the same
    /// shape [`tidb_variable::privilege::PrivilegeRegistry`]'s cluster scratch table
    /// takes for an account statement.
    #[must_use]
    pub fn from_cluster_rows<I: IntoIterator<Item = (String, String)>>(rows: I) -> Self {
        let table = Self {
            instance_mutations: Some(Arc::default()),
            publishes_runtime_settings: false,
            ..Self::default()
        };
        table.load_from_cluster(rows);
        table
    }

    /// Publishes `fresh`'s rows into every existing clone of this table and
    /// rebuilds its cache from them, retaining this node's INSTANCE-only
    /// overrides.
    ///
    /// Mirrors [`tidb_variable::privilege::PrivilegeRegistry::replace_from`]: this
    /// `mysql.global_variables` never contains the process-local INSTANCE
    /// tier. A startup or periodic cluster rebuild must therefore replace the
    /// persisted GLOBAL image without clearing settings written through
    /// `SET INSTANCE` on this node.
    pub fn replace_from(&self, fresh: &Self) {
        let rows = std::mem::take(&mut *fresh.table.lock().expect("global sysvar lock poisoned"));
        *self.table.lock().expect("global sysvar lock poisoned") = rows;
        self.rebuild_sys_var_cache();
    }

    /// Publishes only the named GLOBAL variables from `fresh`.
    ///
    /// The cluster writer uses this as its post-commit fallback when the
    /// durable whole-image reread fails. Applying only the statement's own
    /// changed names cannot erase a disjoint concurrent `SET GLOBAL`, while
    /// `set`/`reset` retain validation aliases and runtime-setting hooks.
    pub fn publish_global_changes_from(&self, fresh: &Self, changed: &[String]) {
        let desired = fresh.overrides();
        for name in changed {
            if get_sys_var(name).is_none() {
                // A newer peer may have stored a sysvar this binary does not
                // know. The cluster loader deliberately skips such rows; a
                // post-commit fallback must do the same instead of turning a
                // confirmed durable change into a process panic.
                continue;
            }
            match desired.get(name).cloned() {
                Some(value) => {
                    self.set(name, value)
                        .expect("the committed scratch table contains validated sysvars");
                }
                None => {
                    self.reset(name)
                        .expect("the committed plan contains registered sysvars");
                }
            }
        }
    }

    /// Replays the INSTANCE-only part of one committed cluster statement into
    /// this node's live table. Mutations retain source order so repeated
    /// assignments to one name have the same last-assignment-wins behavior as
    /// the statement that produced the scratch table.
    pub fn publish_instance_changes_from_if(
        &self,
        fresh: &Self,
        mut should_publish: impl FnMut(&str) -> bool,
    ) -> Vec<String> {
        let Some(mutations) = &fresh.instance_mutations else {
            return Vec::new();
        };
        let mutations = std::mem::take(
            &mut *mutations
                .lock()
                .expect("instance sysvar mutation lock poisoned"),
        );
        let mut published = Vec::new();
        for mutation in mutations {
            let name = match &mutation {
                InstanceMutation::Set(name, _) | InstanceMutation::Reset(name) => name,
            };
            if !should_publish(name) {
                continue;
            }
            match mutation {
                InstanceMutation::Set(name, value) => {
                    self.set_instance(&name, value)
                        .expect("the committed scratch table contains validated instance sysvars");
                    published.push(name);
                }
                InstanceMutation::Reset(name) => {
                    self.reset_instance(&name)
                        .expect("the committed scratch table contains registered instance sysvars");
                    published.push(name);
                }
            }
        }
        published
    }

    fn record_instance_mutation(&self, mutation: InstanceMutation) {
        if let Some(mutations) = &self.instance_mutations {
            mutations
                .lock()
                .expect("instance sysvar mutation lock poisoned")
                .push(mutation);
        }
    }

    fn is_memory_arbitration_setting(name: &str) -> bool {
        matches!(
            name,
            tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT
                | tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT_SESS_MIN_SIZE
                | tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE
                | tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_SOFT_LIMIT
        )
    }
}

/// What Go's `rebuildSysVarCache` reaches through its `*Domain` receiver and
/// the system session it borrows, for one [`GlobalSysvars`].
///
/// boundary: Go `Domain.sysSessionPool` + `ExecRestrictedSQL(SELECT
/// variable_name, variable_value FROM mysql.global_variables)` -- the rows
/// are the table the owner last read from its store (an in-process catalog
/// or the cluster), or the rows a storeless or scratch table holds; see
/// [`GlobalSysvars`]'s `table`.
struct SysVarCacheRebuild<'a>(&'a GlobalSysvars);

impl crate::sysvar_cache::RestrictedSqlExecutor for SysVarCacheRebuild<'_> {
    fn exec_restricted_sysvar_sql(
        &self,
        _sql: &str,
    ) -> Result<Vec<(String, String)>, crate::sysvar_cache::SysVarCacheError> {
        Ok(self
            .0
            .table
            .lock()
            .expect("global sysvar lock poisoned")
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect())
    }
}

impl crate::sysvar_cache::SysVarCacheDeps for SysVarCacheRebuild<'_> {
    fn sys_vars(&self) -> Vec<crate::sysvar_cache::SysVarSpec> {
        tidb_variable::sysvar::SYS_VARS
            .iter()
            .map(|def| crate::sysvar_cache::SysVarSpec {
                name: def.name.to_owned(),
                value: self.0.registry_value(def),
                is_inited_from_config: self.0.is_inited_from_config(def.name),
                skip_init: def.skip_init(),
                has_global_scope: def.has_global_scope(),
                skip_sysvar_cache: def.skip_sysvar_cache(),
                has_set_global: GlobalSysvars::has_set_global_hook(def.name),
            })
            .collect()
    }

    fn is_starter(&self) -> bool {
        tidb_config::deploymode::is_starter()
    }

    fn max_allowed_packet(&self) -> u64 {
        tidb_config::config_tree::config::get_max_allowed_packet()
    }

    fn validate_with_relaxed_validation(
        &self,
        sv: &crate::sysvar_cache::SysVarSpec,
        value: &str,
    ) -> String {
        // Go `ValidateWithRelaxedValidation`: the normalized value, or the
        // value as given when validation refuses it.
        get_sys_var(&sv.name)
            .and_then(|def| def.validate_in_scope(value, SCOPE_GLOBAL).ok())
            .map_or_else(|| value.to_owned(), |validated| validated.value)
    }

    fn set_global(
        &self,
        sv: &crate::sysvar_cache::SysVarSpec,
        value: &str,
    ) -> Result<(), String> {
        self.0.apply_set_global_hook(&sv.name, value);
        Ok(())
    }

    // boundary: Go `Domain.infoCache.ReSize(SchemaVersionCacheLimit)` -- this
    // node has no infoschema version cache to resize; the cluster node reads
    // `tidb_schema_version_cache_limit` where it retains schema versions.
    fn resize_info_cache(&self) {}
}

#[cfg(test)]
mod tests;
